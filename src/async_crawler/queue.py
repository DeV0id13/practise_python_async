import asyncio
from itertools import count


class CrawlerQueue:
    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue[tuple[int, int, str]] = asyncio.PriorityQueue()
        self._added: set[str] = set()
        self.depths: dict[str, int] = {}
        self._processing: set[str] = set()
        self._processed: set[str] = set()
        self.failed: dict[str, str] = {}
        self._counter = count()

    async def join(self) -> None:
        await self._queue.join()

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """Добавить URL, если он ещё никогда не добавлялся."""
        if url in self._added:
            return False
        self._queue.put_nowait((priority, next(self._counter), url))
        self._added.add(url)
        self.depths[url] = depth
        return True

    async def get_next(self) -> str | None:
        """
        Дождаться следующего URL и отметить его как обрабатываемый.
        None зарезервирован для будущего сигнала завершения worker.
        """
        _, _, url = await self._queue.get()
        self._processing.add(url)
        return url

    def mark_processed(self, url: str) -> None:
        """Отметить обрабатываемый URL как успешно завершённый."""
        self._finish(url)
        self._processed.add(url)

    def mark_failed(self, url: str, error: str) -> None:
        """Отметить обрабатываемый URL как завершённый с ошибкой."""
        self._finish(url)
        self.failed[url] = error

    def _finish(self, url: str) -> None:
        if url not in self._processing:
            raise ValueError(f"URL is not being processed: {url}")
        self._processing.remove(url)
        self._queue.task_done()

    def get_stats(self) -> dict[str, int]:
        """Получить количество URL в каждом состоянии."""
        return {
            "pending": self._queue.qsize(),
            "processing": len(self._processing),
            "processed": len(self._processed),
            "failed": len(self.failed),
        }
