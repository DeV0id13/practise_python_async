"""HTTP-клиент. Step 1"""
from async_crawler.parser import HTMLParser
import asyncio
import logging
import re
import aiohttp
from time import perf_counter
from typing import Any
from urllib.parse import urldefrag, urlsplit
from async_crawler.queue import CrawlerQueue
from async_crawler.semaphore import SemaphoreManager

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(
            self,
            max_concurrent: int = 10,
            max_per_domain: int = 2,
            max_depth: int = 2,
    ) -> None:
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.semaphore_manager = SemaphoreManager(max_concurrent, max_per_domain)
        self.session: aiohttp.ClientSession | None = None
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)
        self.html_parser = HTMLParser()
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, Any] = {}

    async def crawl(
            self,
            start_urls: list[str],
            max_pages: int = 100,
            same_domain_only: bool = True,
            include_patterns: list[str] | None = None,
            exclude_patterns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Вернуть результаты успешных страниц, индексированные по URL.
        max_pages ограничивает число уникальных URL, включая неудачные.
        Шаблоны — регулярные выражения для найденных ссылок; стартовые URL
        добавляются независимо от фильтров. Глубина стартовых URL равна нулю.
        """
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")

        includes = [re.compile(pattern) for pattern in include_patterns or []]
        excludes = [re.compile(pattern) for pattern in exclude_patterns or []]
        seeds = []
        domains = set()
        for url in start_urls:
            url, _ = urldefrag(url)
            parsed = urlsplit(url)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ValueError(f"start URL must be an absolute HTTP(S) URL: {url!r}")
            seeds.append(url)
            domains.add(parsed.hostname)

        queue = CrawlerQueue()
        start_time = perf_counter()
        admitted = 0
        processed_urls: dict[str, Any] = {}

        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()

        def enqueue(url: str, depth: int) -> None:
            nonlocal admitted
            if admitted < max_pages and queue.add_url(url, priority=depth, depth=depth):
                admitted += 1

        for url in seeds:
            enqueue(url, 0)

        async def worker() -> None:
            while True:
                url = await queue.get_next()
                if url is None:
                    return
                self.visited_urls.add(url)
                try:
                    result = await self.fetch_and_parse(url)
                    depth = queue.depths[url]
                    if depth < self.max_depth:
                        for link in result["links"]:
                            try:
                                link, _ = urldefrag(link)
                                parsed = urlsplit(link)
                                host = parsed.hostname
                            except ValueError:
                                continue
                            if parsed.scheme not in ("http", "https") or not host:
                                continue
                            if same_domain_only and host not in domains:
                                continue
                            if includes and not any(pattern.search(link) for pattern in includes):
                                continue
                            if any(pattern.search(link) for pattern in excludes):
                                continue
                            enqueue(link, depth + 1)
                    self.processed_urls[url] = result
                except asyncio.CancelledError:
                    self.failed_urls[url] = "crawl cancelled"
                    queue.mark_failed(url, "crawl cancelled")
                    raise

                except Exception as error:
                    self.failed_urls[url] = str(error)
                    queue.mark_failed(url, str(error))
                    logger.exception("Ошибка обработки %s", url)

                else:
                    queue.mark_processed(url)

                finally:
                    self._log_progress(queue, start_time)

        workers = [asyncio.create_task(worker()) for _ in range(min(self.max_concurrent, max_pages))]
        try:
            await queue.join()
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

        if admitted == 0:
            self._log_progress(queue, start_time)
        return self.processed_urls.copy()

    def _log_progress(
            self,
            queue: CrawlerQueue,
            start_time: float,
    ) -> None:
        """Записать статистику очереди и среднюю скорость с начала обхода."""

        stats = queue.get_stats()
        completed = stats["processed"] + stats["failed"]
        elapsed = max(perf_counter() - start_time, 0.0)
        rate = completed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Прогресс: ожидают=%d, обрабатываются=%d, успешно=%d, ошибок=%d, "
            "завершено=%d, время=%.2f с, скорость=%.2f страниц/с",
            stats["pending"], stats["processing"], stats["processed"], stats["failed"],
            completed, elapsed, rate,
        )

    async def fetch_url(self, url: str) -> str:
        """Загрузить HTML одной страницы."""
        async with self.semaphore_manager.limit(url):
            logger.info("Начало загрузки %s", url)
            try:
                if self.session is None or self.session.closed:
                    self.session = aiohttp.ClientSession(timeout=self.timeout)
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    html = await response.text()
                    logger.info("Загрузка %s успешно завершена", url)
                    return html
            except asyncio.TimeoutError:
                logger.error("Таймаут при загрузке %s", url)
                raise
            except aiohttp.ClientError as e:
                logger.error("Ошибка при загрузке %s: %s", url, e)
                raise
            except Exception as e:
                logger.error("Неожиданная ошибка при загрузке %s: %s", url, e)
                raise

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Конкурентно загрузить несколько страниц."""
        coroutines = [self.fetch_url(url) for url in urls]
        results = await asyncio.gather(
            *coroutines,
            return_exceptions=True,
        )
        return {
            url: result
            for url, result in zip(urls, results)
            if isinstance(result, str)
        }

    async def fetch_and_parse(self, url: str) -> dict[str, Any]:
        """Загрузить страницу и извлечь структурированные данные."""
        html = await self.fetch_url(url)
        return await self.html_parser.parse_html(html, url)

    async def close(self) -> None:
        """Закрыть HTTP-сессию."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
