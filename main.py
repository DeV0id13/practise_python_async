"""Демонстрация работы краулера: poetry run python main.py."""

import asyncio
import logging
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

    crawler = AsyncCrawler(max_concurrent=25)

    start = perf_counter()

    try:
        results = await asyncio.gather(
            *(crawler.fetch_and_parse(url) for url in URLS),
            return_exceptions=True,
        )
    finally:
        await crawler.close()

    elapsed = perf_counter() - start
    successful = 0

    for url, result in zip(URLS, results):
        print(f"\n{'=' * 60}")
        print(f"URL: {url}")

        if isinstance(result, BaseException):
            print(f"ERROR: {type(result).__name__}: {result}")
            continue

        successful += 1

        summary = {
            "url": result["url"],
            "title": result["title"],
            "text_length": len(result["text"]),
            "links_count": len(result["links"]),
            "links": result["links"][:5],
            "images_count": len(result["images"]),
            "headings": result["headings"],
            "metadata": result["metadata"],
        }

        print(summary)

    print("\n--- ИТОГОВАЯ СТАТИСТИКА ---")
    print(f"Всего страниц: {len(URLS)}")
    print(f"Успешно обработано: {successful}")
    print(f"Ошибок: {len(URLS) - successful}")
    print(f"Время выполнения: {elapsed:.2f} с")


if __name__ == "__main__":
    asyncio.run(main())
