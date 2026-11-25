import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiointercept import aiointercept

from async_crawler.robots import RobotsParser, RobotsUnavailableError


async def test_fetch_rules_and_crawl_delay():
    parser = RobotsParser()
    async with aiointercept() as mocked:
        root = mocked.server_url
        mocked.get(
            f"{root}/robots.txt", status=200,
            body="User-agent: *\nDisallow: /private\nCrawl-delay: 3\n",
        )
        assert not parser.can_fetch(f"{root}/public")
        result = await parser.fetch_robots(f"{root}/some/page?query=1")
        assert result == {"origin": root, "robots_url": f"{root}/robots.txt", "status": 200}
        assert parser.can_fetch(f"{root}/public")
        assert not parser.can_fetch(f"{root}/private/page")
        assert parser.get_crawl_delay() == 3.0
        assert parser.get_crawl_delay("OtherBot", base_url=root) == 3.0
        result["status"] = 999
        assert (await parser.fetch_robots(root))["status"] == 200


@pytest.mark.parametrize(
    "status,allowed",
    [
        (401, False),
        (403, False),
        (404, True),
        (410, True),
    ],
)
async def test_http_status_policies(status, allowed):
    parser = RobotsParser()
    async with aiointercept() as mocked:
        root = mocked.server_url
        mocked.get(f"{root}/robots.txt", status=status, body="")
        assert (await parser.fetch_robots(root))["status"] == status
        assert parser.can_fetch(f"{root}/page") is allowed
        assert parser.get_crawl_delay() == 0.0


async def test_separate_origins_and_explicit_delay_selection():
    parser = RobotsParser()
    async with aiointercept() as first, aiointercept() as second:
        first.get(f"{first.server_url}/robots.txt", status=200, body="User-agent: *\nDisallow: /one\nCrawl-delay: 1\n")
        second.get(f"{second.server_url}/robots.txt", status=200, body="User-agent: *\nDisallow: /two\nCrawl-delay: 2\n")
        await asyncio.gather(parser.fetch_robots(first.server_url), parser.fetch_robots(second.server_url))
        assert not parser.can_fetch(f"{first.server_url}/one")
        assert parser.can_fetch(f"{first.server_url}/two")
        assert not parser.can_fetch(f"{second.server_url}/two")
        assert parser.get_crawl_delay(base_url=first.server_url) == 1
        assert parser.get_crawl_delay(base_url=second.server_url) == 2
        with pytest.raises(ValueError, match="base_url"):
            parser.get_crawl_delay()


async def test_concurrent_fetches_share_one_request(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    response = MagicMock(status=200)

    async def read():
        entered.set()
        await release.wait()
        return b"User-agent: *\nDisallow: /hidden\n"

    response.read = AsyncMock(side_effect=read)
    response_context = MagicMock()
    response_context.__aenter__ = AsyncMock(return_value=response)
    response_context.__aexit__ = AsyncMock(return_value=False)
    session = MagicMock()
    session.get.return_value = response_context
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=False)
    factory = MagicMock(return_value=session_context)
    monkeypatch.setattr("async_crawler.robots.aiohttp.ClientSession", factory)

    parser = RobotsParser()
    first = asyncio.create_task(parser.fetch_robots("https://EXAMPLE.com:443/a"))
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(parser.fetch_robots("https://example.com/b"))
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.wait_for(asyncio.gather(first, second), 1)
        assert results[0] == results[1]
        factory.assert_called_once()
        session.get.assert_called_once_with("https://example.com/robots.txt", allow_redirects=False)
        assert not parser.can_fetch("https://example.com/hidden")
    finally:
        release.set()
        await asyncio.gather(*(t for t in (first, second) if t is not None), return_exceptions=True)


@pytest.mark.parametrize("status", [429, 500, 503])
async def test_temporary_http_error_is_cached_for_cooldown(status):
    parser = RobotsParser()
    async with aiointercept() as mocked:
        root = mocked.server_url
        mocked.get(f"{root}/robots.txt", status=status, body="", repeat=2)
        for _ in range(2):
            with pytest.raises(RobotsUnavailableError) as exc:
                await parser.fetch_robots(root)
            assert str(status) in str(exc.value)
            assert not parser.can_fetch(f"{root}/page")
        with pytest.raises(LookupError):
            parser.get_crawl_delay()


async def test_user_agent_specific_rules_and_delay():
    parser = RobotsParser()
    async with aiointercept() as mocked:
        root = mocked.server_url
        mocked.get(
            f"{root}/robots.txt", status=200,
            body="User-agent: SpecialBot\nDisallow: /special\nCrawl-delay: 5\n"
                 "User-agent: *\nDisallow: /general\nCrawl-delay: 2\n",
        )
        await parser.fetch_robots(root)
        assert not parser.can_fetch(f"{root}/special", "SpecialBot")
        assert parser.can_fetch(f"{root}/general", "SpecialBot")
        assert not parser.can_fetch(f"{root}/general", "OtherBot")
        assert parser.get_crawl_delay("SpecialBot") == 5.0
        assert parser.get_crawl_delay("OtherBot") == 2.0


def test_unloaded_and_invalid_origin():
    parser = RobotsParser()
    assert not parser.can_fetch("https://example.com/")
    with pytest.raises(LookupError):
        parser.get_crawl_delay()
    with pytest.raises(ValueError):
        parser.can_fetch("/relative")


def test_origin_normalizes_scheme_host_and_default_port():
    assert RobotsParser._origin("HTTPS://EXAMPLE.COM:443/a") == "https://example.com"
    assert RobotsParser._origin("http://[::1]:8080/path") == "http://[::1]:8080"
