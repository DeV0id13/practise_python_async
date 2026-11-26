"""Локальный пример: PYTHONPATH=src poetry run python -m async_crawler.demo_day5."""
import asyncio
import json
import logging
from pathlib import Path

from aiohttp import web

from async_crawler.crawler import AsyncCrawler


async def main() -> None:
    attempts = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal attempts
        if request.path == "/":
            return web.Response(text='<a href="/flaky">flaky</a><a href="/missing">missing</a>')
        if request.path == "/flaky":
            attempts += 1
            return web.Response(status=503 if attempts == 1 else 200, text="recovered")
        return web.Response(status=404)

    app = web.Application()
    app.router.add_get("/{path:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    crawler = AsyncCrawler(respect_robots=False, requests_per_second=1000, base_backoff=0.01)
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        await crawler.crawl([f"http://127.0.0.1:{runner.addresses[0][1]}/"], max_pages=3)
        report = {**crawler.get_error_stats(), "failed_urls": crawler.failed_urls}
        Path("error_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        print("Отчёт: error_report.json")
    finally:
        await crawler.close()
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
