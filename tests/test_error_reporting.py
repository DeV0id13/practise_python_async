import asyncio
import json
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from async_crawler.crawler import AsyncCrawler
from async_crawler.exceptions import NetworkError, ParseError, PermanentError, TransientError
from async_crawler.retry import RetryStrategy


@pytest.mark.parametrize("name", ["connect_timeout", "read_timeout", "total_timeout", "timeout_multiplier"])
@pytest.mark.parametrize("value", [0, -1, True, float("inf"), float("nan"), "1"])
def test_invalid_timeouts(name, value):
    with pytest.raises(ValueError, match=name):
        AsyncCrawler(**{name: value})


def test_multiplier_must_not_reduce_timeout():
    with pytest.raises(ValueError, match="timeout_multiplier"):
        AsyncCrawler(timeout_multiplier=0.5)


async def test_timeouts_increase_per_retry_and_reset_for_new_url(monkeypatch):
    crawler = AsyncCrawler(respect_robots=False, max_retries=2, base_backoff=0,
                           connect_timeout=2, read_timeout=3, total_timeout=4, timeout_multiplier=2)
    calls = []
    response = MagicMock(status=200)
    response.text = AsyncMock(return_value="ok")

    @asynccontextmanager
    async def request(url, **kwargs):
        calls.append((url, kwargs))
        if url.endswith("/first") and len(calls) < 3:
            raise asyncio.TimeoutError()
        yield response

    crawler.session = MagicMock(closed=False)
    crawler.session.get.side_effect = request
    monkeypatch.setattr(crawler.rate_limiter, "acquire", AsyncMock())
    assert await crawler.fetch_url("https://example.com/first") == "ok"
    assert await crawler.fetch_url("https://example.com/second") == "ok"
    assert "timeout" not in calls[0][1] and "timeout" not in calls[3][1]
    assert (crawler.timeout.connect, crawler.timeout.sock_read, crawler.timeout.total) == (2, 3, 4)
    assert [(call[1]["timeout"].connect, call[1]["timeout"].sock_read, call[1]["timeout"].total)
            for call in calls[1:3]] == [(4, 6, 8), (8, 12, 16)]


@pytest.mark.parametrize(("multiplier", "expected"), [(2, [2, 4, 4, 4]), (1e200, [4, 4, 4, 4])])
async def test_retry_timeout_multiplier_is_capped(monkeypatch, multiplier, expected):
    crawler = AsyncCrawler(respect_robots=False, max_retries=4, base_backoff=0, timeout_multiplier=multiplier)
    calls = []
    response = MagicMock(status=200)
    response.text = AsyncMock(return_value="ok")

    @asynccontextmanager
    async def request(url, **kwargs):
        calls.append(kwargs)
        if len(calls) <= 4:
            raise asyncio.TimeoutError()
        yield response

    crawler.session = MagicMock(closed=False)
    crawler.session.get.side_effect = request
    monkeypatch.setattr(crawler.rate_limiter, "acquire", AsyncMock())
    assert await crawler.fetch_url("https://example.com/") == "ok"
    assert [call["timeout"].total for call in calls[1:]] == [30 * factor for factor in expected]


async def test_concurrent_urls_have_independent_timeout_attempts(monkeypatch):
    crawler = AsyncCrawler(respect_robots=False, max_retries=1, base_backoff=0,
                           max_per_domain=2, total_timeout=4)
    calls = []
    response = MagicMock(status=200)
    response.text = AsyncMock(return_value="ok")

    @asynccontextmanager
    async def request(url, **kwargs):
        calls.append((url, kwargs))
        await asyncio.sleep(0)
        if url.endswith("/retry") and sum(item[0] == url for item in calls) == 1:
            raise asyncio.TimeoutError()
        yield response

    crawler.session = MagicMock(closed=False)
    crawler.session.get.side_effect = request
    monkeypatch.setattr(crawler.rate_limiter, "acquire", AsyncMock())
    assert await asyncio.gather(crawler.fetch_url("https://example.com/retry"),
                                crawler.fetch_url("https://example.com/other")) == ["ok", "ok"]
    assert ["timeout" in options for url, options in calls if url.endswith("/retry")] == [False, True]
    assert ["timeout" in options for url, options in calls if url.endswith("/other")] == [False]


async def test_retry_statistics_and_logging(monkeypatch, caplog):
    times = iter([1, 2, 4])
    monkeypatch.setattr("async_crawler.retry.perf_counter", lambda: next(times))
    monkeypatch.setattr("async_crawler.retry.asyncio.sleep", AsyncMock())
    strategy = RetryStrategy(max_retries=1, base_backoff=1)
    operation = AsyncMock(side_effect=[TransientError("busy", url="https://example.com"), "ok"])
    with caplog.at_level(logging.INFO):
        assert await strategy.execute_with_retry(operation) == "ok"
    assert strategy.get_stats() == {
        "errors_by_type": {"TransientError": 1}, "successful_retries": 1, "average_retry_time": 2,
    }
    assert "TransientError" in caplog.text and "https://example.com" in caplog.text
    assert "попытка=1" in caplog.text and "1.00 с" in caplog.text and "Повтор успешен" in caplog.text


async def test_permanent_http_and_parse_errors_are_recorded_without_retry(monkeypatch):
    crawler = AsyncCrawler(respect_robots=False, max_retries=2, base_backoff=0)
    fetch = AsyncMock(side_effect=PermanentError("not found", url="https://example.com/missing", status=404))
    monkeypatch.setattr(crawler, "_fetch_once", fetch)
    with pytest.raises(PermanentError):
        await crawler.fetch_url("https://example.com/missing")
    assert fetch.await_count == 1

    monkeypatch.setattr(crawler, "fetch_url", AsyncMock(return_value="html"))
    parse = AsyncMock(side_effect=ValueError("broken parser"))
    monkeypatch.setattr(crawler.html_parser, "parse_html", parse)
    with pytest.raises(ParseError) as error:
        await crawler.fetch_and_parse("https://example.com/bad")
    assert isinstance(error.value.__cause__, ValueError)
    assert parse.await_count == 1
    assert crawler.get_error_stats() == {
        "errors_by_type": {"PermanentError": 1, "ParseError": 1}, "successful_retries": 0,
        "average_retry_time": 0.0,
        "permanent_error_urls": ["https://example.com/bad", "https://example.com/missing"],
    }


async def test_error_stats_reset_for_new_crawl():
    crawler = AsyncCrawler()
    crawler.retry_strategy.record_error(NetworkError("offline"))
    crawler.permanent_error_urls.add("https://example.com")
    await crawler.crawl([])
    assert crawler.get_error_stats() == {
        "errors_by_type": {}, "successful_retries": 0, "average_retry_time": 0.0, "permanent_error_urls": [],
    }


async def test_demo_prints_saved_report(monkeypatch, tmp_path, capsys):
    from async_crawler import demo_day5

    runner = MagicMock(addresses=[("127.0.0.1", 1234)], setup=AsyncMock(), cleanup=AsyncMock())
    site = MagicMock(start=AsyncMock())
    crawler = MagicMock(crawl=AsyncMock(), close=AsyncMock(), failed_urls={"/page": "error"})
    crawler.get_error_stats.return_value = {"errors_by_type": {"PermanentError": 1}}
    monkeypatch.setattr(demo_day5.web, "AppRunner", lambda app: runner)
    monkeypatch.setattr(demo_day5.web, "TCPSite", lambda *args: site)
    monkeypatch.setattr(demo_day5, "AsyncCrawler", lambda **kwargs: crawler)
    monkeypatch.chdir(tmp_path)
    await demo_day5.main()
    output = capsys.readouterr().out.split("\nОтчёт:")[0]
    assert json.loads(output) == json.loads((tmp_path / "error_report.json").read_text(encoding="utf-8"))
