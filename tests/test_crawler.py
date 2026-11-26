import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aiohttp import web
from aiointercept import aiointercept
from async_crawler.crawler import AsyncCrawler
from async_crawler.exceptions import PermanentError, TransientError
from async_crawler.queue import CrawlerQueue


@pytest.mark.asyncio
async def test_fetch_and_parse(monkeypatch):
    crawler = AsyncCrawler()
    url = "https://example.com/page"
    html = "<title>Example</title><a href='/about'>About</a>"
    fetch = AsyncMock(return_value=html)
    monkeypatch.setattr(crawler, "fetch_url", fetch)

    result = await crawler.fetch_and_parse(url)

    fetch.assert_awaited_once_with(url)
    assert result["url"] == url
    assert result["title"] == "Example"
    assert result["links"] == ["https://example.com/about"]


@pytest.mark.asyncio
async def test_fetch_and_parse_propagates_fetch_error(monkeypatch):
    crawler = AsyncCrawler()
    monkeypatch.setattr(crawler, "fetch_url", AsyncMock(side_effect=asyncio.TimeoutError))
    parse = AsyncMock()
    monkeypatch.setattr(crawler.html_parser, "parse_html", parse)

    with pytest.raises(asyncio.TimeoutError):
        await crawler.fetch_and_parse("https://example.com/")

    parse.assert_not_awaited()


@pytest.mark.asyncio
async def test_fetch_url_success():
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000)
    expected_html = "<html>Test page</html>"

    async with aiointercept() as mocked:
        url = f"{mocked.server_url}/page"
        mocked.get(url, status=200, body=expected_html)

        try:
            result = await crawler.fetch_url(url)
            assert result == expected_html
        finally:
            await crawler.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "error_type"), [(404, PermanentError), (500, TransientError)])
async def test_fetch_url_http_error(status: int, error_type: type[Exception]) -> None:
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_retries=0)
    async with aiointercept() as mocked:
        url = f"{mocked.server_url}/error"
        mocked.get(url, status=status)
        try:
            with pytest.raises(error_type) as error:
                await crawler.fetch_url(url)
            assert error.value.status == status
            assert error.value.url == url
            assert isinstance(error.value.__cause__, aiohttp.ClientResponseError)
            assert error.value.__cause__.status == status
        finally:
            await crawler.close()


@pytest.mark.asyncio
async def test_fetch_url_timeout() -> None:
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_retries=0)
    try:
        with patch.object(aiohttp.ClientSession, "get", side_effect=asyncio.TimeoutError):
            with pytest.raises(TransientError) as error:
                await crawler.fetch_url("https://example.com/timeout")
            assert error.value.url == "https://example.com/timeout"
            assert isinstance(error.value.__cause__, asyncio.TimeoutError)
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_fetch_urls_partial_failure() -> None:
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_retries=0)
    async with aiointercept() as mocked:
        success_url = f"{mocked.server_url}/success"
        error_url = f"{mocked.server_url}/error"
        mocked.get(success_url, status=200, body="<html>Success</html>")
        mocked.get(error_url, status=500)
        try:
            results = await crawler.fetch_urls([success_url, error_url])
            assert results == {success_url: "<html>Success</html>"}
        finally:
            await crawler.close()


@pytest.mark.asyncio
async def test_close() -> None:
    crawler = AsyncCrawler()
    await crawler.close()
    session = aiohttp.ClientSession()
    crawler.session = session

    await crawler.close()

    assert session.closed
    assert crawler.session is None
    await crawler.close()


@pytest.mark.asyncio
async def test_concurrency_limit() -> None:
    active = 0
    max_active = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        try:
            await asyncio.sleep(0.1)
            return web.Response(text=request.match_info["number"])
        finally:
            active -= 1

    app = web.Application()
    app.router.add_get("/{number}", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    crawler = AsyncCrawler(max_concurrent=3, respect_robots=False, requests_per_second=1000)
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        urls = [f"http://127.0.0.1:{port}/{i}" for i in range(10)]

        results = await crawler.fetch_urls(urls)

        assert results == {url: str(i) for i, url in enumerate(urls)}
        assert max_active == 2
    finally:
        await crawler.close()
        await runner.cleanup()


async def test_crawl_follows_links_once_and_respects_depth_and_filters(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=2, max_depth=1)
    pages = {
        "https://example.com/": """<title>Start</title>
            <a href='/keep/one#part'>one</a><a href='/keep/one'>duplicate</a>
            <a href='/keep/skip'>excluded</a><a href='/other'>not included</a>
            <a href='https://elsewhere.com/keep/two'>external</a>""",
        "https://example.com/keep/one": "<title>One</title><a href='/keep/deep'>deep</a>",
    }
    fetch = AsyncMock(side_effect=lambda url: pages[url])
    monkeypatch.setattr(crawler, "fetch_url", fetch)

    results = await crawler.crawl(
        ["https://example.com/#top", "https://example.com/"],
        include_patterns=[r"/keep/"], exclude_patterns=[r"/skip$"],
    )

    assert set(results) == set(pages)
    assert results["https://example.com/"]["title"] == "Start"
    assert results["https://example.com/keep/one"]["title"] == "One"
    assert fetch.await_count == 2


async def test_crawl_multiple_seed_hosts_and_external_links(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=1, max_depth=1)
    urls = ["https://a.example/", "https://b.example/", "https://c.example/"]
    fetch = AsyncMock(side_effect=lambda url: {
        urls[0]: f'<a href="{urls[1]}">b</a><a href="{urls[2]}">c</a>',
        urls[1]: "",
        urls[2]: "",
    }[url])
    monkeypatch.setattr(crawler, "fetch_url", fetch)

    restricted = await crawler.crawl(urls[:2])
    unrestricted = await crawler.crawl(urls[:1], same_domain_only=False)
    assert set(restricted) == set(urls[:2])
    assert set(unrestricted) == set(urls)


async def test_crawl_counts_failed_pages_towards_max_pages(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=2, max_depth=2)
    root = "https://example.com/"
    failed = "https://example.com/fail"
    skipped = "https://example.com/skipped"

    async def fetch(url):
        if url == root:
            return f'<a href="{failed}">fail</a><a href="{skipped}">skip</a>'
        raise RuntimeError("download failed")

    mock = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(crawler, "fetch_url", mock)
    results = await asyncio.wait_for(crawler.crawl([root], max_pages=2), 1)
    assert set(results) == {root}
    assert {call.args[0] for call in mock.await_args_list} == {root, failed}


@pytest.mark.parametrize("max_pages", [0, -1, True, 1.5])
async def test_crawl_rejects_invalid_max_pages(max_pages):
    with pytest.raises(ValueError, match="max_pages"):
        await AsyncCrawler().crawl([], max_pages=max_pages)


@pytest.mark.parametrize("max_depth", [-1, True, 1.5])
def test_crawler_rejects_invalid_max_depth(max_depth):
    with pytest.raises(ValueError, match="max_depth"):
        AsyncCrawler(max_depth=max_depth)


async def test_crawl_depth_zero_and_invalid_seed(monkeypatch):
    crawler = AsyncCrawler(max_depth=0)
    fetch = AsyncMock(return_value='<a href="/next">next</a>')
    monkeypatch.setattr(crawler, "fetch_url", fetch)
    assert set(await crawler.crawl(["https://example.com/"])) == {"https://example.com/"}
    assert fetch.await_count == 1
    with pytest.raises(ValueError, match="start URL"):
        await crawler.crawl(["/relative"])
    assert await crawler.crawl([]) == {}


async def test_crawl_cancellation_stops_workers(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=2)
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def fetch(url):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(crawler, "fetch_url", fetch)
    task = asyncio.create_task(crawler.crawl(["https://example.com/"]))
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()


async def test_crawl_runs_workers_concurrently(monkeypatch):
    crawler = AsyncCrawler(max_concurrent=2, max_depth=0)
    both_started = asyncio.Event()
    release = asyncio.Event()
    active = 0

    async def fetch(url):
        nonlocal active
        active += 1
        if active == 2:
            both_started.set()
        await release.wait()
        return "<title>Page</title>"

    monkeypatch.setattr(crawler, "fetch_url", fetch)
    task = asyncio.create_task(crawler.crawl(["https://a.example/", "https://b.example/"]))
    try:
        await asyncio.wait_for(both_started.wait(), 1)
    finally:
        release.set()
    assert len(await asyncio.wait_for(task, 1)) == 2


async def test_crawl_with_http_session():
    crawler = AsyncCrawler(max_concurrent=2, max_per_domain=1, max_depth=1, respect_robots=False, requests_per_second=1000)
    async with aiointercept() as mocked:
        root = f"{mocked.server_url}/"
        child = f"{mocked.server_url}/child"
        mocked.get(root, status=200, body='<title>Root</title><a href="/child">child</a>')
        mocked.get(child, status=200, body="<title>Child</title>")
        try:
            results = await asyncio.wait_for(crawler.crawl([root]), 2)
            assert {url: page["title"] for url, page in results.items()} == {
                root: "Root", child: "Child",
            }
            assert crawler.semaphore_manager.get_stats()["active"] == 0
        finally:
            await crawler.close()


async def test_log_progress_uses_queue_stats_and_elapsed_time(monkeypatch, caplog):
    crawler = AsyncCrawler()
    queue = CrawlerQueue()
    for url in ("success", "failure", "active", "pending"):
        queue.add_url(url)
    queue.mark_processed(await queue.get_next())
    queue.mark_failed(await queue.get_next(), "error")
    await queue.get_next()
    monkeypatch.setattr("async_crawler.crawler.perf_counter", lambda: 14.0)

    with caplog.at_level(logging.INFO, logger="async_crawler.crawler"):
        crawler._log_progress(queue, start_time=10.0)

    assert "ожидают=1, обрабатываются=1, успешно=1, ошибок=1" in caplog.text
    assert "завершено=2, время=4.00 с, скорость=0.50 страниц/с" in caplog.text


def test_log_progress_zero_elapsed(monkeypatch, caplog):
    monkeypatch.setattr("async_crawler.crawler.perf_counter", lambda: 10.0)
    with caplog.at_level(logging.INFO, logger="async_crawler.crawler"):
        AsyncCrawler()._log_progress(CrawlerQueue(), start_time=10.0)
    assert "время=0.00 с, скорость=0.00 страниц/с" in caplog.text


async def test_crawl_logs_success_and_failure(monkeypatch, caplog):
    crawler = AsyncCrawler(max_concurrent=1, max_depth=0)
    monkeypatch.setattr(
        crawler, "fetch_url", AsyncMock(side_effect=["<title>OK</title>", RuntimeError("error")]),
    )
    with caplog.at_level(logging.INFO, logger="async_crawler.crawler"):
        await crawler.crawl(["https://example.com/ok", "https://example.com/fail"])

    progress = [record.getMessage() for record in caplog.records if record.getMessage().startswith("Прогресс:")]
    assert len(progress) == 2
    assert "успешно=1, ошибок=0, завершено=1" in progress[0]
    assert "успешно=1, ошибок=1, завершено=2" in progress[1]


async def test_request_uses_rate_limit_and_releases_response_and_semaphores(monkeypatch):
    crawler = AsyncCrawler(user_agent="ExampleBot/1.0")
    response = MagicMock(status=403)
    response_context = MagicMock()
    response_context.__aenter__ = AsyncMock(return_value=response)
    response_context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock(closed=False)
    session.get.return_value = response_context
    crawler.session = session
    delay = MagicMock(return_value=4.0)
    monkeypatch.setattr(crawler.robots_parser, "get_crawl_delay", delay)
    acquire = AsyncMock()
    monkeypatch.setattr(crawler.rate_limiter, "acquire", acquire)
    url = "https://EXAMPLE.com:443/robots.txt"

    with pytest.raises(RuntimeError, match="inside"):
        async with crawler._request(url) as result:
            assert result is response
            assert crawler.semaphore_manager.get_stats()["active"] == 1
            raise RuntimeError("inside")

    delay.assert_called_once_with("ExampleBot/1.0", base_url=url)
    acquire.assert_awaited_once_with("example.com", crawl_delay=4.0)
    session.get.assert_called_once_with(url, allow_redirects=False)
    response_context.__aexit__.assert_awaited_once()
    assert crawler.semaphore_manager.get_stats()["active"] == 0


async def test_request_missing_robots_delay_and_cancellation(monkeypatch):
    crawler = AsyncCrawler()
    session = MagicMock(closed=False)
    crawler.session = session
    entered = asyncio.Event()

    async def acquire(domain, *, crawl_delay):
        assert domain == "example.com"
        assert crawl_delay == 0.0
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(crawler.rate_limiter, "acquire", acquire)

    async def request():
        async with crawler._request("https://example.com/"):
            pytest.fail("cancelled request must not start")

    task = asyncio.create_task(request())
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    session.get.assert_not_called()
    assert crawler.semaphore_manager.get_stats()["active"] == 0


async def test_request_without_respecting_robots(monkeypatch):
    crawler = AsyncCrawler(respect_robots=False)
    session = MagicMock(closed=False)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=MagicMock())
    context.__aexit__ = AsyncMock(return_value=False)
    session.get.return_value = context
    crawler.session = session
    delay = MagicMock(side_effect=AssertionError("unexpected robots lookup"))
    monkeypatch.setattr(crawler.robots_parser, "get_crawl_delay", delay)
    acquire = AsyncMock()
    monkeypatch.setattr(crawler.rate_limiter, "acquire", acquire)
    async with crawler._request("https://example.com/"):
        pass
    acquire.assert_awaited_once_with("example.com", crawl_delay=0.0)
    delay.assert_not_called()


async def test_request_http_response_and_user_agent():
    crawler = AsyncCrawler(user_agent="ExampleBot/1.0")
    async with aiointercept() as mocked:
        url = f"{mocked.server_url}/missing"
        mocked.get(url, status=404, body="not found")
        try:
            async with crawler._request(url) as response:
                assert response.status == 404
                assert await response.text() == "not found"
                assert crawler.session.headers["User-Agent"] == "ExampleBot/1.0"
        finally:
            await crawler.close()


async def test_request_rejects_invalid_url():
    crawler = AsyncCrawler()
    with pytest.raises(ValueError, match="URL"):
        async with crawler._request("/relative"):
            pytest.fail("invalid URL must not be requested")
    assert crawler.session is None
