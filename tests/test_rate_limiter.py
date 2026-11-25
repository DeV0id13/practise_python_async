import asyncio
import math

import pytest

from async_crawler.rate_limiter import RateLimiter


@pytest.mark.parametrize("rate", [0, -1, True, False, float("nan"), float("inf"), -float("inf"), "1", None, 5e-324, 10**400])
def test_invalid_rates(rate):
    with pytest.raises(ValueError, match="requests_per_second"):
        RateLimiter(rate)


@pytest.mark.parametrize("rate", [1, 0.5, 2.5])
def test_positive_integer_and_float_rates(rate):
    RateLimiter(rate)


def test_invalid_per_domain():
    with pytest.raises(ValueError, match="per_domain"):
        RateLimiter(per_domain=1)


async def test_same_domain_waits_and_domains_are_independent(monkeypatch):
    now = 10.0
    delays = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        now += delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter(requests_per_second=2)

    await limiter.acquire("EXAMPLE.COM")
    await limiter.acquire("other.example")
    assert delays == []
    await limiter.acquire("example.com")
    assert delays == [0.5]
    await limiter.acquire("example.com")
    assert delays == [0.5, 0.5]


async def test_global_mode_and_none_key(monkeypatch):
    now = 5.0
    delays = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        now += delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter(requests_per_second=0.5, per_domain=False)
    await limiter.acquire("one.example")
    await limiter.acquire("two.example")
    await limiter.acquire()
    assert delays == [2.0, 2.0]

    delays.clear()
    limiter = RateLimiter()
    await limiter.acquire()
    await limiter.acquire("one.example")
    assert delays == []
    await limiter.acquire()
    assert delays == [1.0]


async def test_concurrent_waiters_and_cancellation(monkeypatch):
    now = 0.0
    sleeping = asyncio.Event()
    release = asyncio.Event()
    real_sleep = asyncio.sleep

    async def sleep(delay):
        nonlocal now
        sleeping.set()
        await release.wait()
        now += delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter()
    await limiter.acquire("a.example")
    first = asyncio.create_task(limiter.acquire("a.example"))
    await asyncio.wait_for(sleeping.wait(), 1)
    second = asyncio.create_task(limiter.acquire("a.example"))
    other = asyncio.create_task(limiter.acquire("b.example"))
    try:
        await asyncio.wait_for(other, 1)
        assert not first.done()
        assert not second.done()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        await asyncio.wait_for(second, 1)
        assert math.isclose(now, 1.0)
    finally:
        release.set()
        for task in (first, second, other):
            if not task.done():
                task.cancel()
        await asyncio.gather(first, second, other, return_exceptions=True)
        await real_sleep(0)


async def test_rechecks_time_after_early_wakeup(monkeypatch):
    now = 0.0
    delays = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        now += delay / 2 if len(delays) == 1 else delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter()
    await limiter.acquire("example.com")
    await limiter.acquire("example.com")
    assert delays == [1.0, 0.5]


async def test_concurrent_requests_observe_interval(monkeypatch):
    now = 0.0
    real_sleep = asyncio.sleep
    granted = []

    async def sleep(delay):
        nonlocal now
        await real_sleep(0)
        now += delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter(requests_per_second=2)

    async def request():
        await limiter.acquire("example.com")
        granted.append(now)

    await asyncio.gather(*(request() for _ in range(4)))
    assert granted == [0.0, 0.5, 1.0, 1.5]


@pytest.mark.parametrize("domain", ["", 123, False])
async def test_invalid_domain(domain):
    with pytest.raises(ValueError, match="domain"):
        await RateLimiter().acquire(domain)


@pytest.mark.parametrize("name", ["min_delay", "jitter"])
@pytest.mark.parametrize("value", [-1, True, float("nan"), float("inf"), "1", None, 10**400])
def test_invalid_delay_configuration(name, value):
    with pytest.raises(ValueError, match=name):
        RateLimiter(**{name: value})


@pytest.mark.parametrize("value", [-1, True, float("nan"), float("inf"), "1", None, 10**400])
async def test_invalid_crawl_delay(value):
    limiter = RateLimiter()
    with pytest.raises(ValueError, match="crawl_delay"):
        await limiter.acquire("example.com", crawl_delay=value)


async def test_maximum_delay_and_dynamic_crawl_delay(monkeypatch):
    now = 10.0
    delays = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        now += delay

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    limiter = RateLimiter(requests_per_second=2, min_delay=1.5)
    await limiter.acquire("example.com", crawl_delay=3.0)
    assert delays == []
    await limiter.acquire("example.com", crawl_delay=2.0)
    await limiter.acquire("example.com", crawl_delay=0.25)
    assert delays == [2.0, 1.5]


async def test_jitter_is_sampled_once_even_after_early_wakeup(monkeypatch):
    now = 0.0
    delays = []
    samples = []

    async def sleep(delay):
        nonlocal now
        delays.append(delay)
        now += delay / 2 if len(delays) == 1 else delay

    def uniform(lower, upper):
        samples.append((lower, upper))
        return 0.4

    monkeypatch.setattr("async_crawler.rate_limiter.monotonic", lambda: now)
    monkeypatch.setattr("async_crawler.rate_limiter.asyncio.sleep", sleep)
    monkeypatch.setattr("async_crawler.rate_limiter.random.uniform", uniform)
    limiter = RateLimiter(requests_per_second=1, jitter=0.5)
    await limiter.acquire("example.com")
    await limiter.acquire("example.com")
    assert samples == [(0.0, 0.5)]
    assert delays == pytest.approx([1.4, 0.7])
