"""HTTP-клиент. Step 1"""
from async_crawler.parser import HTMLParser
import asyncio
import logging
import aiohttp
from typing import Any

logger = logging.getLogger(__name__)


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10) -> None:
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: aiohttp.ClientSession | None = None
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)
        self.html_parser = HTMLParser()

    async def fetch_url(self, url: str) -> str:
        """Загрузить HTML одной страницы."""
        async with self.semaphore:
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
