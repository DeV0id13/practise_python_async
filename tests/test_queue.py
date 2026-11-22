import asyncio

import pytest

from async_crawler.queue import CrawlerQueue


async def test_priority_fifo_and_duplicate_depth():
    queue = CrawlerQueue()
    assert queue.add_url("later", priority=2, depth=3)
    assert queue.add_url("first", priority=1, depth=1)
    assert queue.add_url("second", priority=1, depth=2)
    assert not queue.add_url("first", priority=-1, depth=99)
    assert queue.depths == {"later": 3, "first": 1, "second": 2}
    assert [await queue.get_next() for _ in range(3)] == ["first", "second", "later"]
    assert not queue.add_url("first")


async def test_stats_and_completion():
    queue = CrawlerQueue()
    queue.add_url("success")
    queue.add_url("failure")
    assert queue.get_stats() == {"pending": 2, "processing": 0, "processed": 0, "failed": 0}

    assert await queue.get_next() == "success"
    assert queue.get_stats() == {"pending": 1, "processing": 1, "processed": 0, "failed": 0}
    queue.mark_processed("success")
    assert await queue.get_next() == "failure"
    queue.mark_failed("failure", "timeout")
    assert queue.failed == {"failure": "timeout"}
    assert queue.get_stats() == {"pending": 0, "processing": 0, "processed": 1, "failed": 1}
    assert not queue.add_url("success")
    assert not queue.add_url("failure")

    with pytest.raises(ValueError):
        queue.mark_failed("success", "late error")
    with pytest.raises(ValueError):
        queue.mark_processed("unknown")
    assert queue.get_stats() == {"pending": 0, "processing": 0, "processed": 1, "failed": 1}


async def test_get_next_waits_for_url():
    queue = CrawlerQueue()
    waiter = asyncio.create_task(queue.get_next())
    await asyncio.sleep(0)
    assert not waiter.done()
    queue.add_url("new", depth=4)
    assert await asyncio.wait_for(waiter, timeout=1) == "new"
    assert queue.depths["new"] == 4


@pytest.mark.asyncio
async def test_duplicate_urls():
    queue = CrawlerQueue()
    assert queue.add_url("https://example.com", depth=2) is True
    assert queue.add_url("https://example.com", depth=5) is False
    assert queue.get_stats() == {"pending": 1, "processing": 0, "processed": 0, "failed": 0}
    assert queue.depths["https://example.com"] == 2


@pytest.mark.asyncio
async def test_mark_failed():
    queue = CrawlerQueue()
    queue.add_url("https://example.com", depth=3)
    url = await queue.get_next()
    queue.mark_failed(url, "connection error")

    assert queue.failed == {url: "connection error"}
    assert queue.depths[url] == 3
    assert queue.get_stats() == {"pending": 0, "processing": 0, "processed": 0, "failed": 1}


@pytest.mark.asyncio
async def test_queue_join():
    queue = CrawlerQueue()
    queue.add_url("success")
    queue.add_url("failure")
    success = await queue.get_next()
    failure = await queue.get_next()

    # CrawlerQueue пока не предоставляет публичный join().
    joiner = asyncio.create_task(queue._queue.join())
    await asyncio.sleep(0)
    assert not joiner.done()
    queue.mark_processed(success)
    await asyncio.sleep(0)
    assert not joiner.done()
    queue.mark_failed(failure, "error")
    await asyncio.wait_for(joiner, timeout=1)


async def test_multiple_waiters_receive_distinct_urls():
    queue = CrawlerQueue()
    waiters = [asyncio.create_task(queue.get_next()) for _ in range(2)]
    await asyncio.sleep(0)
    assert all(not waiter.done() for waiter in waiters)

    queue.add_url("one", depth=1)
    queue.add_url("two", depth=2)
    urls = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1)
    assert set(urls) == {"one", "two"}
    assert queue.get_stats() == {"pending": 0, "processing": 2, "processed": 0, "failed": 0}


@pytest.mark.parametrize("method", ["mark_processed", "mark_failed"])
async def test_cannot_finish_pending_url(method):
    queue = CrawlerQueue()
    queue.add_url("pending")
    args = ("pending", "error") if method == "mark_failed" else ("pending",)
    with pytest.raises(ValueError):
        getattr(queue, method)(*args)
    assert queue.get_stats() == {"pending": 1, "processing": 0, "processed": 0, "failed": 0}
    assert await queue.get_next() == "pending"
