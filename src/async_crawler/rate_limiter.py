import asyncio
import math
import random
from time import monotonic


def _non_negative_finite(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a non-negative finite number")
    try:
        result = float(value)
    except OverflowError as error:
        raise ValueError(f"{name} must be a non-negative finite number") from error
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{name} must be a non-negative finite number")
    return result


class RateLimiter:
    def __init__(
            self,
            requests_per_second: float = 1.0,
            per_domain: bool = True,
            min_delay: float = 0.0,
            jitter: float = 0.0,
    ) -> None:
        if isinstance(requests_per_second, bool) or not isinstance(requests_per_second, (int, float)):
            raise ValueError("requests_per_second must be a positive finite number")
        try:
            rate = float(requests_per_second)
        except OverflowError as error:
            raise ValueError("requests_per_second must be a positive finite number") from error
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("requests_per_second must be a positive finite number")
        interval = 1.0 / rate
        if not math.isfinite(interval):
            raise ValueError("requests_per_second is too small")
        if not isinstance(per_domain, bool):
            raise ValueError("per_domain must be a boolean")

        self._interval = interval
        self._min_delay = _non_negative_finite(min_delay, "min_delay")
        self._jitter = _non_negative_finite(jitter, "jitter")
        self._per_domain = per_domain
        self._last_request: dict[str | None, float] = {}
        self._locks: dict[str | None, asyncio.Lock] = {}

    async def acquire(
            self,
            domain: str | None = None,
            *,
            crawl_delay: float = 0.0,
    ) -> None:
        """Дождаться разрешения с учётом ограничений и случайной задержки.

        None использует отдельный ключ; при per_domain=False ключ общий.
        """
        crawl_delay = _non_negative_finite(crawl_delay, "crawl_delay")
        if self._per_domain and domain is not None:
            if not isinstance(domain, str) or not domain:
                raise ValueError("domain must be a non-empty string or None")
            key = domain.lower()
        else:
            key = None

        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock

        async with lock:
            interval = max(self._interval, self._min_delay, crawl_delay)
            if key in self._last_request and self._jitter:
                interval += random.uniform(0.0, self._jitter)
                if not math.isfinite(interval):
                    raise ValueError("combined interval and jitter must be finite")

            while True:
                now = monotonic()
                last = self._last_request.get(key)
                remaining = interval - (now - last) if last is not None else 0.0
                if remaining <= 0:
                    self._last_request[key] = now
                    return
                await asyncio.sleep(remaining)
