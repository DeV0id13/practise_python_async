import asyncio
from unittest.mock import patch

import aiohttp
import pytest
from aiohttp import web
from aiointercept import aiointercept
from async_crawler.crawler import AsyncCrawler


@pytest.mark.asyncio
async def test_fetch_url_success():
    crawler = AsyncCrawler()
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
@pytest.mark.parametrize("status", [404, 500])
async def test_fetch_url_http_error(status: int) -> None:
    crawler = AsyncCrawler()
    async with aiointercept() as mocked:
        url = f"{mocked.server_url}/error"
        mocked.get(url, status=status)
        try:
            with pytest.raises(aiohttp.ClientResponseError) as error:
                await crawler.fetch_url(url)
            assert error.value.status == status
        finally:
            await crawler.close()


@pytest.mark.asyncio
async def test_fetch_url_timeout() -> None:
    crawler = AsyncCrawler()
    try:
        with patch.object(aiohttp.ClientSession, "get", side_effect=asyncio.TimeoutError):
            with pytest.raises(asyncio.TimeoutError):
                await crawler.fetch_url("https://example.com/timeout")
    finally:
        await crawler.close()


@pytest.mark.asyncio
async def test_fetch_urls_partial_failure() -> None:
    crawler = AsyncCrawler()
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
    crawler = AsyncCrawler(max_concurrent=3)
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        urls = [f"http://127.0.0.1:{port}/{i}" for i in range(10)]

        results = await crawler.fetch_urls(urls)

        assert results == {url: str(i) for i, url in enumerate(urls)}
        assert max_active == 3
    finally:
        await crawler.close()
        await runner.cleanup()
