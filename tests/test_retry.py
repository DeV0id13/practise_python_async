import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from async_crawler.crawler import AsyncCrawler


def response(status, text="ok"):
    result = MagicMock(status=status)
    result.text = AsyncMock(return_value=text)
    if status >= 400:
        result.raise_for_status.side_effect = aiohttp.ClientResponseError(
            MagicMock(), (), status=status, message="error",
        )
    return result


async def test_retries_use_capped_exponential_backoff_and_release_response(monkeypatch):
    crawler = AsyncCrawler(max_retries=3, base_backoff=0.25, max_backoff=0.6)
    responses = iter([response(503), response(429), response(500), response(200)])
    calls = []
    sleeps = []
    active = False

    @asynccontextmanager
    async def request(url):
        nonlocal active
        calls.append(url)
        active = True
        try:
            yield next(responses)
        finally:
            active = False

    async def sleep(delay):
        assert not active
        sleeps.append(delay)

    monkeypatch.setattr(crawler, "_request", request)
    monkeypatch.setattr("async_crawler.crawler.asyncio.sleep", sleep)
    assert await crawler._fetch_with_retry("https://example.com/") == (False, "ok")
    assert len(calls) == 4
    assert sleeps == pytest.approx([0.25, 0.5, 0.6])


@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_non_retryable_http_error_is_raised_immediately(monkeypatch, status):
    crawler = AsyncCrawler(max_retries=3)
    calls = []

    @asynccontextmanager
    async def request(url):
        calls.append(url)
        yield response(status)

    monkeypatch.setattr(crawler, "_request", request)
    with pytest.raises(aiohttp.ClientResponseError) as error:
        await crawler._fetch_with_retry("https://example.com/")
    assert error.value.status == status
    assert len(calls) == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_retryable_http_error_stops_after_max_retries(monkeypatch, status):
    crawler = AsyncCrawler(max_retries=2, base_backoff=0)
    attempts = []

    @asynccontextmanager
    async def request(url):
        attempts.append(url)
        yield response(status)

    monkeypatch.setattr(crawler, "_request", request)
    with pytest.raises(aiohttp.ClientResponseError) as error:
        await crawler._fetch_with_retry("https://example.com/")
    assert error.value.status == status
    assert len(attempts) == 3


@pytest.mark.parametrize("failure", [asyncio.TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientPayloadError])
async def test_transient_exceptions_retry_until_limit(monkeypatch, failure):
    crawler = AsyncCrawler(max_retries=2, base_backoff=0)
    attempts = []

    @asynccontextmanager
    async def request(url):
        attempts.append(url)
        raise failure("temporary")
        yield

    monkeypatch.setattr(crawler, "_request", request)
    with pytest.raises(failure):
        await crawler._fetch_with_retry("https://example.com/")
    assert len(attempts) == 3


async def test_cancellation_during_backoff_stops_retries(monkeypatch):
    crawler = AsyncCrawler(max_retries=3, base_backoff=10)
    sleeping = asyncio.Event()
    attempts = []

    @asynccontextmanager
    async def request(url):
        attempts.append(url)
        yield response(503)

    async def sleep(delay):
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(crawler, "_request", request)
    monkeypatch.setattr("async_crawler.crawler.asyncio.sleep", sleep)
    task = asyncio.create_task(crawler._fetch_with_retry("https://example.com/"))
    await asyncio.wait_for(sleeping.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(attempts) == 1


@pytest.mark.parametrize("value", [-1, True, 1.5, "1", None])
def test_invalid_max_retries(value):
    with pytest.raises(ValueError, match="max_retries"):
        AsyncCrawler(max_retries=value)


@pytest.mark.parametrize("name", ["base_backoff", "max_backoff"])
@pytest.mark.parametrize("value", [-1, True, float("inf"), float("nan"), "1", None, 10**400])
def test_invalid_backoff(name, value):
    with pytest.raises(ValueError, match=name):
        AsyncCrawler(**{name: value})
