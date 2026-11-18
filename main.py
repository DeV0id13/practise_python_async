"""Демонстрация работы краулера: poetry run python main.py."""

import asyncio
import logging
from time import perf_counter
import aiohttp
from async_crawler.crawler import AsyncCrawler

URLS = [
    "https://example.com",
    "https://httpbin.org/get",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
    "https://python.org",
    "https://youtube.com",
    "https://github.com",
    "https://stackoverflow.com",
    "https://wikipedia.org",
    "https://google.com",
    "https://yandex.ru",
    "https://vk.com",
    "https://twitter.com",
    "https://facebook.com",
    "https://instagram.com",
    "https://linkedin.com",
    "https://reddit.com",
    "https://medium.com",
    "https://news.ycombinator.com",
    "https://cnn.com",
    "https://bbc.com",
    "https://nytimes.com",
    "https://forbes.com",
    "https://theverge.com",
    "https://techcrunch.com",
    "https://wired.com",
    "https://arstechnica.com",
    "https://engadget.com",
    "https://mashable.com",
    "https://gizmodo.com",
    "https://lifehacker.com",
    "https://slate.com",
    "https://vox.com",
    "https://theatlantic.com",
    "https://theguardian.com",
    "https://aljazeera.com",
    "https://reuters.com",
    "https://apnews.com",
]


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # 1. Последовательная загрузка
    crawler = AsyncCrawler(max_concurrent=20)
    sequential_results = {}

    try:
        start = perf_counter()

        for url in URLS:
            try:
                html = await crawler.fetch_url(url)
                sequential_results[url] = html

            except (aiohttp.ClientError, asyncio.TimeoutError):
                continue

        sequential_time = perf_counter() - start

    finally:
        await crawler.close()

    # 2. Конкурентная загрузка
    crawler = AsyncCrawler(max_concurrent=20)

    try:
        start = perf_counter()
        concurrent_results = await crawler.fetch_urls(URLS)
        concurrent_time = perf_counter() - start

    finally:
        await crawler.close()

    # 3. Вывод результатов
    print("\n--- РЕЗУЛЬТАТЫ ---")

    print(f"Последовательно: {sequential_time:.2f} с")
    print(f"Конкурентно: {concurrent_time:.2f} с")

    print(
        f"Ускорение: "
        f"{sequential_time / concurrent_time:.2f}x"
    )

    print(
        f"Успешно (последовательно): "
        f"{len(sequential_results)}/{len(URLS)}"
    )

    print(
        f"Успешно (конкурентно): "
        f"{len(concurrent_results)}/{len(URLS)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
