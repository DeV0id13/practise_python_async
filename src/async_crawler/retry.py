import asyncio
import logging
import math
from collections import Counter
from collections.abc import Awaitable, Callable
from time import perf_counter
from typing import TypeVar, ParamSpec

from async_crawler.exceptions import TransientError, NetworkError

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = ParamSpec("P")


class RetryStrategy:
    def __init__(
            self,
            max_retries: int = 3,
            backoff_factor: float = 2.0,
            retry_on: list[type[Exception]] | None = None,
            base_backoff: float = 1.0,
            max_backoff: float = 30.0,
            retries_by_error: dict[type[Exception], int] | None = None,
            backoff_by_error: dict[type[Exception], float] | None = None,
    ) -> None:
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries must be a non-negative integer")

        values = {}
        for name, value, minimum in (
                ("backoff_factor", backoff_factor, 1.0),
                ("base_backoff", base_backoff, 0.0),
                ("max_backoff", max_backoff, 0.0),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a finite number >= {minimum:g}")
            try:
                number = float(value)
            except OverflowError as error:
                raise ValueError(f"{name} must be a finite number >= {minimum:g}") from error
            if not math.isfinite(number) or number < minimum:
                raise ValueError(f"{name} must be a finite number >= {minimum:g}")
            values[name] = number

        if retry_on is not None and (
                not isinstance(retry_on, list)
                or any(not isinstance(item, type) or not issubclass(item, Exception) for item in retry_on)
        ):
            raise ValueError("retry_on must be None or a list of Exception subclasses")

        for name, mapping in (("retries_by_error", retries_by_error), ("backoff_by_error", backoff_by_error)):
            if mapping is not None and not isinstance(mapping, dict):
                raise ValueError(f"{name} must be a dictionary")
            for error_type, value in (mapping or {}).items():
                if not isinstance(error_type, type) or not issubclass(error_type, Exception):
                    raise ValueError(f"{name} keys must be Exception subclasses")
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise ValueError(f"{name} values must be non-negative numbers")
                if name == "retries_by_error":
                    if not isinstance(value, int) or value < 0:
                        raise ValueError("retries_by_error values must be non-negative integers")
                else:
                    try:
                        valid = math.isfinite(float(value)) and value >= 0
                    except OverflowError:
                        valid = False
                    if not valid:
                        raise ValueError("backoff_by_error values must be non-negative finite numbers")

        self.max_retries = max_retries
        self.backoff_factor = values["backoff_factor"]
        self.base_backoff = values["base_backoff"]
        self.max_backoff = values["max_backoff"]
        self.retries_by_error = dict(retries_by_error or {})
        self.backoff_by_error = dict(backoff_by_error or {})

        self.retry_on = (
            tuple(retry_on)
            if retry_on is not None
            else (TransientError, NetworkError)
        )
        self.reset_stats()

    def reset_stats(self) -> None:
        self.errors = Counter()
        self.successful_retries = 0
        self._retry_duration = 0.0
        self._retry_attempts = 0

    def record_error(self, error: Exception) -> None:
        self.errors[type(error).__name__] += 1

    def get_stats(self) -> dict:
        """Среднее время учитывает выполнение повторных попыток без задержки перед ними."""
        return {
            "errors_by_type": dict(self.errors),
            "successful_retries": self.successful_retries,
            "average_retry_time": self._retry_duration / self._retry_attempts if self._retry_attempts else 0.0,
        }

    async def execute_with_retry(
            self,
            coro: Callable[P, Awaitable[T]],
            *args: P.args,
            **kwargs: P.kwargs,
    ) -> T:

        retries_by_type: dict[type[Exception], int] = {}
        delays_by_type: dict[type[Exception], float] = {}
        last_error = None

        for attempt in range(self.max_retries + 1):
            started = perf_counter()
            try:
                try:
                    result = await coro(*args, **kwargs)
                finally:
                    if attempt:
                        self._retry_duration += perf_counter() - started
                        self._retry_attempts += 1
                if attempt:
                    self.successful_retries += 1
                    logger.info("Повтор успешен: URL=%s, попытка=%d", getattr(last_error, "url", None), attempt + 1)
                return result

            except Exception as error:
                self.record_error(error)
                last_error = error
                error_type = type(error)

                retries = retries_by_type.get(error_type, 0)

                limit = self.retries_by_error.get(
                    error_type,
                    self.max_retries,
                )

                if not isinstance(error, self.retry_on) or attempt == self.max_retries or retries >= limit:
                    logger.error("Ошибка %s: URL=%s, попытка=%d, итог=ошибка", error_type.__name__, getattr(error, "url", None), attempt + 1)
                    raise

                delay = delays_by_type.get(
                    error_type,
                    min(
                        self.backoff_by_error.get(
                            error_type,
                            self.base_backoff,
                        ),
                        self.max_backoff,
                    ),
                )

                wait = delay

                if isinstance(error, TransientError):
                    wait = max(wait, error.retry_after)

                logger.warning(
                    "%s: URL=%s, попытка=%d, повтор %d/%d через %.2f с",
                    error_type.__name__,
                    getattr(error, "url", None),
                    attempt + 1,
                    retries + 1,
                    limit,
                    wait,
                )

                await asyncio.sleep(wait)

                retries_by_type[error_type] = retries + 1

                delays_by_type[error_type] = min(
                    self.max_backoff,
                    delay * self.backoff_factor,
                )

        raise AssertionError("unreachable")
