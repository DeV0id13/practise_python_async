import asyncio
import logging
from time import perf_counter

from aiohttp import web
from async_crawler.crawler import AsyncCrawler


async def handler(request: web.Request) -> web.Response:
    if request.path == "/robots.txt":
        return web.Response(
            text=(
                "User-agent: *\n"
                "Disallow: /private\n"
                "Crawl-delay: 1\n"
            )
        )

    if request.path == "/":
        return web.Response(
            text="""
                <html>
                    <title>Main page</title>
                    <a href="/public">Public page</a>
                    <a href="/private">Private page</a>
                </html>
            """,
            content_type="text/html",
        )

    return web.Response(
        text="<html><title>Public page</title></html>",
        content_type="text/html",
    )


async def main() -> None:
    app = web.Application()
    app.router.add_get("/{path:.*}", handler)

    runner = web.AppRunner(app)
    await runner.setup()

    crawler = AsyncCrawler(
        max_concurrent=5,
        max_per_domain=2,
        max_depth=2,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        jitter=0.2,
        user_agent="MyBot/1.0",
        max_retries=3,
    )

    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()

        port = runner.addresses[0][1]
        url = f"http://127.0.0.1:{port}/"

        start = perf_counter()

        results = await crawler.crawl(
            start_urls=[url],
            max_pages=10,
            same_domain_only=True,
        )

        elapsed = perf_counter() - start

        print("\n--- Результаты обхода ---")
        print(f"Время: {elapsed:.2f} с")
        print(f"Обработано страниц: {len(results)}")
        print(f"Посещено URL: {len(crawler.visited_urls)}")
        print(f"Заблокировано: {len(crawler.blocked_urls)}")

        print("\n--- Запрещённые URL ---")
        for blocked_url in crawler.blocked_urls:
            print(blocked_url)

        print("\n--- HTTP-статистика ---")
        for name, value in crawler.get_request_stats().items():
            print(f"{name}: {value:.2f}")

    finally:
        await crawler.close()
        await runner.cleanup()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
