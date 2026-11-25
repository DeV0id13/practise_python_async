from async_crawler.crawler import AsyncCrawler
import pytest


def test_request_stats(monkeypatch):
    now = 100.0

    monkeypatch.setattr(
        "async_crawler.crawler.perf_counter",
        lambda: now,
    )

    crawler = AsyncCrawler()

    crawler._record_request("example.com")

    now = 101.0
    crawler._record_request("python.org")

    now = 101.5
    crawler._record_request("example.com")

    stats = crawler.get_request_stats()

    assert stats["total_requests"] == 3
    assert stats["current_rps"] == 2
    assert stats["average_rps"] == pytest.approx(2.0)
    assert stats["average_delay"] == pytest.approx(1.5)
    assert stats["blocked_urls"] == 0