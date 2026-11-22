import asyncio

import pytest

from async_crawler.semaphore import SemaphoreManager


@pytest.mark.parametrize("name", ["max_concurrent", "max_per_domain"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, "2", None])
def test_limits_must_be_positive_integers(name, value):
    with pytest.raises(ValueError, match=name):
        SemaphoreManager(**{name: value})


async def test_per_domain_waiter_does_not_consume_global_slot():
    manager = SemaphoreManager(max_concurrent=2, max_per_domain=1)
    started = {name: asyncio.Event() for name in ("first", "same", "other")}
    release = asyncio.Event()

    async def request(name, url):
        async with manager.limit(url):
            started[name].set()
            await release.wait()

    first = asyncio.create_task(request("first", "https://EXAMPLE.com:443/a"))
    same = other = None
    try:
        await asyncio.wait_for(started["first"].wait(), 1)
        same = asyncio.create_task(request("same", "http://example.com:8080/b"))
        await asyncio.sleep(0)
        other = asyncio.create_task(request("other", "https://other.example/"))
        await asyncio.wait_for(started["other"].wait(), 1)
        assert not started["same"].is_set()
        assert manager.get_stats() == {"active": 2, "by_domain": {"example.com": 1, "other.example": 1}}

        snapshot = manager.get_stats()
        snapshot["by_domain"].clear()
        assert manager.get_stats()["by_domain"] == {"example.com": 1, "other.example": 1}
    finally:
        release.set()
        await asyncio.gather(*(task for task in (first, same, other) if task is not None))
    assert started["same"].is_set()
    assert manager.get_stats() == {"active": 0, "by_domain": {}}


async def test_global_limit_across_domains():
    manager = SemaphoreManager(max_concurrent=1, max_per_domain=2)
    async with manager.limit("https://a.example"):
        entered = asyncio.Event()

        async def second_request():
            async with manager.limit("https://b.example"):
                entered.set()

        task = asyncio.create_task(second_request())
        await asyncio.sleep(0)
        assert not entered.is_set()
        assert manager.get_stats() == {"active": 1, "by_domain": {"a.example": 1}}
    await asyncio.wait_for(task, 1)
    assert entered.is_set()
    assert manager.get_stats() == {"active": 0, "by_domain": {}}


@pytest.mark.parametrize("waiting_url", ["https://a.example/other", "https://b.example/"])
async def test_cancelled_waiter_does_not_leak_permits(waiting_url):
    manager = SemaphoreManager(max_concurrent=1, max_per_domain=1)
    async with manager.limit("https://a.example/"):
        task = asyncio.create_task(manager_wait(manager, waiting_url))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert manager.get_stats() == {"active": 1, "by_domain": {"a.example": 1}}
    await asyncio.wait_for(manager_wait(manager, waiting_url), 1)
    assert manager.get_stats() == {"active": 0, "by_domain": {}}


async def manager_wait(manager, url):
    async with manager.limit(url):
        pass


async def test_exception_and_cancellation_release_active_permits():
    manager = SemaphoreManager(max_concurrent=1, max_per_domain=1)
    with pytest.raises(RuntimeError):
        async with manager.limit("https://example.com"):
            raise RuntimeError("request failed")
    assert manager.get_stats() == {"active": 0, "by_domain": {}}

    entered = asyncio.Event()

    async def request():
        async with manager.limit("https://example.com"):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(request())
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert manager.get_stats() == {"active": 0, "by_domain": {}}
    await asyncio.wait_for(manager_wait(manager, "https://example.com"), 1)


async def test_url_without_hostname_is_rejected():
    manager = SemaphoreManager()
    with pytest.raises(ValueError, match="hostname"):
        async with manager.limit("/relative/path"):
            pytest.fail("context must not be entered")
    assert manager.get_stats() == {"active": 0, "by_domain": {}}
