"""HTTP-клиент. Step 1"""
from async_crawler.parser import HTMLParser
import asyncio
import logging
import math
import re
import aiohttp
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from time import perf_counter
from typing import Any
from urllib.parse import urldefrag, urlsplit
from async_crawler.http_utils import MAX_REDIRECTS, redirect_target
from async_crawler.queue import CrawlerQueue
from async_crawler.rate_limiter import RateLimiter
from async_crawler.robots import RobotsParser
from async_crawler.semaphore import SemaphoreManager
from async_crawler.http_utils import retry_after_seconds
from collections import deque

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_final_url: ContextVar[str | None] = ContextVar("crawler_final_url", default=None)


class RobotsDisallowedError(Exception):
    """URL запрещён правилами robots.txt."""


class AsyncCrawler:
    def __init__(
            self,
            max_concurrent: int = 10,
            max_per_domain: int = 2,
            max_depth: int = 2,
            requests_per_second: float = 1.0,
            respect_robots: bool = True,
            min_delay: float = 0.0,
            jitter: float = 0.0,
            user_agent: str = "MyBot/1.0",
            max_retries: int = 3,
            base_backoff: float = 1.0,
            max_backoff: float = 30.0,
    ) -> None:
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth must be a non-negative integer")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")
        for name, value in (("base_backoff", base_backoff), ("max_backoff", max_backoff)):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a non-negative finite number")
            try:
                numeric_value = float(value)
            except OverflowError as error:
                raise ValueError(f"{name} must be a non-negative finite number") from error
            if not math.isfinite(numeric_value) or numeric_value < 0:
                raise ValueError(f"{name} must be a non-negative finite number")
        self.max_retries = max_retries
        self.base_backoff = float(base_backoff)
        self.max_backoff = float(max_backoff)
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.semaphore_manager = SemaphoreManager(max_concurrent, max_per_domain)
        self.session: aiohttp.ClientSession | None = None
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)
        self.html_parser = HTMLParser()
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, Any] = {}
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True,
            min_delay=min_delay,
            jitter=jitter,
        )

        self.respect_robots = respect_robots
        self.user_agent = user_agent
        self.blocked_urls: set[str] = set()
        self.robots_parser = RobotsParser(request=self._request)
        self._allowed_hosts: set[str] | None = None
        self._reset_request_stats()

    def _reset_request_stats(self) -> None:
        self._stats_started = perf_counter()
        self._request_count = 0
        self._recent_requests: deque[float] = deque()
        self._last_request_times: dict[str, float] = {}
        self._total_interval = 0.0
        self._interval_count = 0

    def _record_request(self, domain: str) -> None:
        now = perf_counter()

        self._request_count += 1
        self._recent_requests.append(now)

        while (
                self._recent_requests
                and now - self._recent_requests[0] >= 1.0
        ):
            self._recent_requests.popleft()

        last = self._last_request_times.get(domain)

        if last is not None:
            self._total_interval += now - last
            self._interval_count += 1

        self._last_request_times[domain] = now

    @asynccontextmanager
    async def _request(
            self,
            url: str,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Выполнить запрос под ограничениями и передать открытый ответ."""
        parsed = urlsplit(url)
        domain = parsed.hostname
        if parsed.scheme not in ("http", "https") or not domain:
            raise ValueError(f"URL must be an absolute HTTP(S) URL: {url!r}")

        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout, headers={"User-Agent": self.user_agent},
            )

        crawl_delay = 0.0
        if self.respect_robots:
            try:
                crawl_delay = self.robots_parser.get_crawl_delay(self.user_agent, base_url=url)
            except LookupError:
                pass

        async with self.semaphore_manager.limit(url):
            await self.rate_limiter.acquire(domain, crawl_delay=crawl_delay)
            self._record_request(domain)
            async with self.session.get(url, allow_redirects=False) as response:
                yield response

    def get_request_stats(self) -> dict[str, int | float]:
        now = perf_counter()
        elapsed = now - self._stats_started

        while (
                self._recent_requests
                and now - self._recent_requests[0] >= 1.0
        ):
            self._recent_requests.popleft()

        return {
            "total_requests": self._request_count,

            "current_rps": len(self._recent_requests),

            "average_rps": (
                self._request_count / elapsed
                if elapsed > 0 else 0.0
            ),

            "average_delay": (
                self._total_interval / self._interval_count
                if self._interval_count else 0.0
            ),

            "blocked_urls": len(self.blocked_urls),
        }

    async def _fetch_with_retry(self, url: str) -> tuple[bool, str]:
        """Вернуть (перенаправление, адрес или HTML), повторяя временные ошибки."""
        delay = min(self.base_backoff, self.max_backoff)
        for attempt in range(self.max_retries + 1):
            wait = delay
            try:
                async with self._request(url) as response:
                    if 300 <= response.status < 400:
                        return True, redirect_target(response, url)
                    response.raise_for_status()
                    return False, await response.text()
            except aiohttp.ClientResponseError as error:
                if (
                        error.status not in RETRYABLE_STATUSES
                        or attempt == self.max_retries
                ):
                    raise

                if error.headers:
                    wait = max(
                        delay,
                        retry_after_seconds(
                            error.headers.get("Retry-After")
                        ),
                    )
            except (aiohttp.ClientSSLError, aiohttp.ServerFingerprintMismatch):
                raise
            except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError):
                if attempt == self.max_retries:
                    raise

            logger.warning(
                "Повторный запрос %s через %.2f с (%d/%d)",
                url,
                wait,
                attempt + 1,
                self.max_retries,
            )

            await asyncio.sleep(wait)

            delay = min(self.max_backoff, delay * 2)

        raise AssertionError("unreachable")

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
        self.visited_urls.clear()
        self.failed_urls.clear()
        self.processed_urls.clear()
        self.blocked_urls.clear()
        self._reset_request_stats()

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

        self._allowed_hosts = domains if same_domain_only else None
        workers = [asyncio.create_task(worker()) for _ in range(min(self.max_concurrent, max_pages))]
        try:
            await queue.join()
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self._allowed_hosts = None

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
        request_stats = self.get_request_stats()
        logger.info(
            "HTTP: запросов=%d, текущий RPS=%d, "
            "средний RPS=%.2f, средний интервал=%.2f с, "
            "заблокировано robots.txt=%d",
            request_stats["total_requests"],
            request_stats["current_rps"],
            request_stats["average_rps"],
            request_stats["average_delay"],
            request_stats["blocked_urls"],
        )

    async def fetch_url(self, url: str) -> str:
        """Загрузить HTML с проверкой robots.txt на каждом перенаправлении."""
        current, _ = urldefrag(url)
        seen = set()
        logger.info("Начало загрузки %s", url)
        try:
            for _ in range(MAX_REDIRECTS + 1):
                if current in seen:
                    raise RuntimeError(f"Redirect loop for {current}")
                seen.add(current)
                host = urlsplit(current).hostname
                if self._allowed_hosts is not None:
                    if host not in self._allowed_hosts:
                        raise RuntimeError(
                            f"URL is outside crawl scope: {current}"
                        )
                if self.respect_robots:
                    await self.robots_parser.fetch_robots(current)
                    if not self.robots_parser.can_fetch(current, self.user_agent):
                        self.blocked_urls.add(current)
                        logger.info("URL запрещён robots.txt: %s", current)
                        raise RobotsDisallowedError(f"URL disallowed by robots.txt: {current}")

                redirected, result = await self._fetch_with_retry(current)
                if redirected:
                    current = result
                    continue
                _final_url.set(current)
                logger.info("Загрузка %s успешно завершена", url)
                return result
            raise RuntimeError(f"Too many redirects for {url}")
        except asyncio.TimeoutError:
            logger.error("Таймаут при загрузке %s", url)
            raise
        except aiohttp.ClientError as e:
            logger.error("Ошибка при загрузке %s: %s", url, e)
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
        token = _final_url.set(None)
        try:
            html = await self.fetch_url(url)
            return await self.html_parser.parse_html(html, _final_url.get() or url)
        finally:
            _final_url.reset(token)

    async def close(self) -> None:
        """Закрыть HTTP-сессию."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
