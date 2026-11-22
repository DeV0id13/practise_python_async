import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit


class SemaphoreManager:
    def __init__(
        self,
        max_concurrent: int = 10,
        max_per_domain: int = 2,
    ) -> None:
        for name, value in (("max_concurrent", max_concurrent), ("max_per_domain", max_per_domain)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

        self._global_semaphore = asyncio.Semaphore(max_concurrent)
        self._max_per_domain = max_per_domain
        self._domain_semaphores: dict[str, asyncio.Semaphore] = {}
        self._active = 0
        self._active_by_domain: dict[str, int] = {}

    @asynccontextmanager
    async def limit(self, url: str) -> AsyncIterator[None]:
        """Занять места для хоста и общего лимита на время блока."""
        domain = urlsplit(url).hostname
        if domain is None:
            raise ValueError(f"URL must contain a hostname: {url!r}")

        domain_semaphore = self._domain_semaphores.get(domain)
        if domain_semaphore is None:
            domain_semaphore = asyncio.Semaphore(self._max_per_domain)
            self._domain_semaphores[domain] = domain_semaphore

        # Ожидание домена не должно занимать место в общем лимите.
        async with domain_semaphore:
            async with self._global_semaphore:
                self._active += 1
                self._active_by_domain[domain] = self._active_by_domain.get(domain, 0) + 1
                try:
                    yield
                finally:
                    self._active -= 1
                    self._active_by_domain[domain] -= 1
                    if self._active_by_domain[domain] == 0:
                        del self._active_by_domain[domain]

    def get_stats(self) -> dict[str, int | dict[str, int]]:
        """Вернуть снимок количества активных запросов и их хостов."""
        return {"active": self._active, "by_domain": self._active_by_domain.copy()}
