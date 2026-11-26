import asyncio
from contextlib import asynccontextmanager
from time import monotonic
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web

from async_crawler.crawler import AsyncCrawler, RobotsDisallowedError
from async_crawler.exceptions import PermanentError
from async_crawler.robots import RobotsParser, RobotsUnavailableError


@pytest.fixture
async def serve():
    runners = []

    async def start(handler):
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        runners.append(runner)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        return f"http://127.0.0.1:{runner.addresses[0][1]}"

    yield start
    for runner in runners:
        await runner.cleanup()


async def test_robots_and_page_use_same_session_user_agent_and_crawl_delay(serve):
    requests = []

    async def handler(request):
        requests.append((request.path, monotonic(), request.headers.get("User-Agent"), request.transport))
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: TestBot\nCrawl-delay: 1\n")
        return web.Response(text="page")

    root = await serve(handler)
    crawler = AsyncCrawler(user_agent="TestBot", requests_per_second=1000)
    try:
        assert await crawler.fetch_url(f"{root}/page") == "page"
        assert [record[0] for record in requests] == ["/robots.txt", "/page"]
        assert [record[2] for record in requests] == ["TestBot", "TestBot"]
        assert requests[1][1] - requests[0][1] >= 0.9
        assert requests[0][3] is requests[1][3]
    finally:
        await crawler.close()


async def test_rate_limiter_spaces_concurrent_page_requests(serve):
    times = []

    async def handler(request):
        times.append(monotonic())
        return web.Response(text=request.path)

    root = await serve(handler)
    crawler = AsyncCrawler(respect_robots=False, max_concurrent=3, max_per_domain=3, requests_per_second=20)
    try:
        results = await asyncio.gather(*(crawler.fetch_url(f"{root}/{i}") for i in range(3)))
        assert results == ["/0", "/1", "/2"]
        assert len(times) == 3
        assert all(b - a >= 0.04 for a, b in zip(times, times[1:]))
    finally:
        await crawler.close()


async def test_page_retries_use_rate_limiter(serve, monkeypatch):
    attempts = []

    async def handler(request):
        attempts.append(request.path)
        return web.Response(status=503 if len(attempts) < 3 else 200, text="ready")

    root = await serve(handler)
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, max_retries=2, base_backoff=0)
    acquire = AsyncMock(wraps=crawler.rate_limiter.acquire)
    monkeypatch.setattr(crawler.rate_limiter, "acquire", acquire)
    try:
        assert await crawler.fetch_url(f"{root}/page") == "ready"
        assert attempts == ["/page", "/page", "/page"]
        assert acquire.await_count == 3
    finally:
        await crawler.close()


async def test_redirect_checks_destination_robots_and_resets_blocked(serve, caplog):
    target_paths = []

    async def target(request):
        target_paths.append(request.path)
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: *\nDisallow: /private\n")
        return web.Response(text="must not be requested")

    destination = await serve(target)

    async def source(request):
        if request.path == "/robots.txt":
            return web.Response(text="User-agent: *\nDisallow:\n")
        raise web.HTTPFound(location=f"{destination}/private")

    root = await serve(source)
    crawler = AsyncCrawler(requests_per_second=1000)
    try:
        with caplog.at_level("INFO"), pytest.raises(RobotsDisallowedError):
            await crawler.fetch_url(f"{root}/redirect")
        assert target_paths == ["/robots.txt"]
        assert crawler.blocked_urls == {f"{destination}/private"}
        assert "URL запрещён robots.txt" in caplog.text
        await crawler.crawl([])
        assert not crawler.blocked_urls
    finally:
        await crawler.close()


async def test_redirects_and_robots_redirect_use_limited_requests(serve):
    paths = []

    async def handler(request):
        paths.append(request.path)
        if request.path == "/robots.txt":
            raise web.HTTPFound(location="/rules")
        if request.path == "/rules":
            return web.Response(text="User-agent: *\nDisallow:\n")
        if request.path == "/start":
            raise web.HTTPMovedPermanently(location="/page")
        return web.Response(text="redirected")

    root = await serve(handler)
    crawler = AsyncCrawler(requests_per_second=1000)
    try:
        assert await crawler.fetch_url(f"{root}/start") == "redirected"
        assert paths == ["/robots.txt", "/rules", "/start", "/page"]
    finally:
        await crawler.close()


async def test_redirected_page_uses_final_url_as_base_for_links(serve):
    async def handler(request):
        if request.path == "/start":
            raise web.HTTPFound(location="/section/page")
        return web.Response(text='<a href="child">child</a>')

    root = await serve(handler)
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000)
    try:
        result = await crawler.fetch_and_parse(f"{root}/start")
        assert result["url"] == f"{root}/section/page"
        assert result["links"] == [f"{root}/section/child"]
    finally:
        await crawler.close()


async def test_concurrent_redirects_keep_separate_base_urls(serve):
    async def handler(request):
        if request.path.startswith("/start/"):
            name = request.path.rsplit("/", 1)[-1]
            raise web.HTTPFound(location=f"/{name}/page")
        return web.Response(text='<a href="child">child</a>')

    root = await serve(handler)
    crawler = AsyncCrawler(respect_robots=False, max_per_domain=2, requests_per_second=1000)
    try:
        results = await asyncio.gather(*(crawler.fetch_and_parse(f"{root}/start/{name}") for name in ("a", "b")))
        assert [result["links"] for result in results] == [[f"{root}/a/child"], [f"{root}/b/child"]]
    finally:
        await crawler.close()


async def test_missing_robots_allows_page(serve):
    paths = []

    async def handler(request):
        paths.append(request.path)

        if request.path == "/robots.txt":
            return web.Response(status=404)

        return web.Response(status=200, text="page")

    root = await serve(handler)
    crawler = AsyncCrawler(requests_per_second=1000)

    try:
        result = await crawler.fetch_url(f"{root}/page")

        assert result == "page"
        assert paths == ["/robots.txt", "/page"]
        assert not crawler.blocked_urls
    finally:
        await crawler.close()


@pytest.mark.parametrize(
    ("kind", "error_type", "status"),
    [("loop", RuntimeError, None), ("missing", PermanentError, 302), ("unhandled", PermanentError, 304)],
)
async def test_invalid_page_redirects_are_rejected(serve, kind, error_type, status):
    async def handler(request):
        if kind == "loop":
            return web.Response(status=302, headers={"Location": "/page"})
        if kind == "missing":
            return web.Response(status=302)
        return web.Response(status=304)

    root = await serve(handler)
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000)
    try:
        with pytest.raises(error_type) as error:
            await crawler.fetch_url(f"{root}/page")
        if status is not None:
            assert error.value.status == status
            assert error.value.url == f"{root}/page"
            assert isinstance(error.value.__cause__, aiohttp.ClientResponseError)
    finally:
        await crawler.close()


async def test_server_failure_blocks_pages_and_is_retried_after_cooldown(serve, monkeypatch):
    paths = []
    clock = 10.0

    async def handler(request):
        paths.append(request.path)
        if request.path == "/robots.txt":
            if paths.count("/robots.txt") == 1:
                return web.Response(status=503)
            return web.Response(text="User-agent: *\nDisallow:\n")
        return web.Response(text="page")

    monkeypatch.setattr("async_crawler.robots.monotonic", lambda: clock)
    root = await serve(handler)
    crawler = AsyncCrawler(requests_per_second=1000)
    try:
        results = await asyncio.gather(
            *(crawler.fetch_url(f"{root}/{i}") for i in range(6)), return_exceptions=True,
        )
        assert all(isinstance(result, RobotsUnavailableError) for result in results)
        assert paths == ["/robots.txt"]
        clock += crawler.robots_parser.FAILURE_COOLDOWN + 1
        assert await crawler.fetch_url(f"{root}/recovered") == "page"
        assert paths == ["/robots.txt", "/robots.txt", "/recovered"]
    finally:
        await crawler.close()


async def test_network_failure_cooldown_and_recovery(monkeypatch):
    attempts = []
    clock = 0.0

    @asynccontextmanager
    async def request(url):
        attempts.append(url)
        raise aiohttp.ClientConnectionError("offline")
        yield

    monkeypatch.setattr("async_crawler.robots.monotonic", lambda: clock)
    parser = RobotsParser(request=request)
    for _ in range(3):
        with pytest.raises(RobotsUnavailableError, match="offline"):
            await parser.fetch_robots("https://example.com")
    assert len(attempts) == 1
    assert not parser.can_fetch("https://example.com/page")
    clock += parser.FAILURE_COOLDOWN + 1
    with pytest.raises(RobotsUnavailableError):
        await parser.fetch_robots("https://example.com")
    assert len(attempts) == 2
