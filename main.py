"""Демонстрация работы краулера: poetry run python main.py."""

import asyncio
import json
import logging
from pathlib import Path
from time import perf_counter
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

    crawler = AsyncCrawler(
        max_concurrent=100,
        max_per_domain=50,
        max_depth=10,
    )

    start = perf_counter()

    try:
        results = await crawler.crawl(
            start_urls=URLS,
            max_pages=100,
            same_domain_only=False,
        )

        elapsed = perf_counter() - start

        print("\n--- РЕЗУЛЬТАТЫ ОБХОДА ---")

        for url, data in results.items():
            print(f"URL: {url}")
            print(f"Title: {data['title']}")
            print(f"Links: {len(data['links'])}")
            print(f"Text length: {len(data['text'])}")
            print("-" * 50)

        print("\n--- СТАТИСТИКА ---")
        print(f"Посещено: {len(crawler.visited_urls)}")
        print(f"Успешно: {len(crawler.processed_urls)}")
        print(f"Ошибок: {len(crawler.failed_urls)}")
        print(f"Время: {elapsed:.2f} с")

        output = {
            "results": results,
            "failed_urls": crawler.failed_urls,
            "visited_urls": sorted(crawler.visited_urls),
        }

        path = Path("crawl_results.json")

        with path.open("w", encoding="utf-8") as file:
            json.dump(output, file, ensure_ascii=False, indent=2)

        print(f"\nДанные сохранены в {path}")

    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
