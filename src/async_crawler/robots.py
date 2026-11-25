import asyncio
import aiohttp
from time import monotonic
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from collections.abc import AsyncIterator
from async_crawler.http_utils import retry_after_seconds
from async_crawler.http_utils import MAX_REDIRECTS, redirect_target


class RobotsUnavailableError(Exception):
    """Не удалось получить robots.txt, поэтому обход origin недоступен."""

class RobotsParser:
    FAILURE_COOLDOWN = 30.0

    def __init__(
        self,
        request: Callable[
            [str],
            AbstractAsyncContextManager[aiohttp.ClientResponse],
        ] | None = None,
    ) -> None:
        self._request = request
        self._rules: dict[str, RobotFileParser] = {}
        self._cache: dict[str, dict[str, str | int]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._failures: dict[str, tuple[float, str]] = {}

    @asynccontextmanager
    async def _get_response(self, url: str) -> AsyncIterator[aiohttp.ClientResponse]:
        if self._request is not None:
            async with self._request(url) as response:
                yield response
        else:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, allow_redirects=False) as response:
                    yield response

    @staticmethod
    def _origin(url: str) -> str:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port
        if parts.scheme not in ("http", "https") or not host:
            raise ValueError(f"URL must be an absolute HTTP(S) URL: {url!r}")
        host = host.encode("idna").decode("ascii")
        if ":" in host:
            host = f"[{host}]"
        default_port = 80 if parts.scheme == "http" else 443
        authority = f"{host}:{port}" if port is not None and port != default_port else host
        return f"{parts.scheme}://{authority}"

    async def fetch_robots(
            self,
            base_url: str,
    ) -> dict[str, str | int]:
        """
        Загрузить правила origin; вернуть адрес robots.txt и HTTP-статус.

        Ответы 4xx запрещают обход; при 429, сетевых и серверных ошибках
        повторная попытка разрешена после временного интервала.
        """
        origin = self._origin(base_url)
        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            cached = self._cache.get(origin)
            if cached is not None:
                return cached.copy()

            failure = self._failures.get(origin)
            if failure is not None:
                retry_at, message = failure
                if monotonic() < retry_at:
                    raise RobotsUnavailableError(message)
                del self._failures[origin]

            robots_url = f"{origin}/robots.txt"
            current = robots_url
            seen = set()
            parser = RobotFileParser(robots_url)
            try:
                for _ in range(MAX_REDIRECTS + 1):
                    if current in seen:
                        raise RobotsUnavailableError(f"Redirect loop for {robots_url}")
                    seen.add(current)
                    async with self._get_response(current) as response:
                        status = response.status
                        if 300 <= status < 400:
                            current = redirect_target(response, current)
                            continue
                        if 200 <= status < 300:
                            content = await response.read()
                            parser.parse(content.decode("utf-8", errors="replace").splitlines())
                        elif status in (401, 403):
                            parser.disallow_all = True
                        elif 400 <= status < 500 and status != 429:
                            parser.allow_all = True
                        else:
                            response.raise_for_status()
                            raise RobotsUnavailableError(f"Unexpected robots.txt status {status}")
                        break
                else:
                    raise RobotsUnavailableError(f"Too many redirects for {robots_url}")
            except (aiohttp.ClientError, asyncio.TimeoutError, RobotsUnavailableError) as error:
                message = f"robots.txt unavailable for {origin}: {error}"
                cooldown = self.FAILURE_COOLDOWN

                if isinstance(error, aiohttp.ClientResponseError):
                    if error.headers:
                        cooldown = max(
                            cooldown,
                            retry_after_seconds(
                                error.headers.get("Retry-After")
                            ),
                        )

                self._failures[origin] = (
                    monotonic() + cooldown,
                    message,
                )
                raise RobotsUnavailableError(message) from error

            result: dict[str, str | int] = {"origin": origin, "robots_url": robots_url, "status": status}
            self._rules[origin] = parser
            self._cache[origin] = result
            return result.copy()

    def can_fetch(
            self,
            url: str,
            user_agent: str = "*",
    ) -> bool:
        """Проверить правила origin; до их загрузки доступ запрещён."""
        parser = self._rules.get(self._origin(url))
        return parser.can_fetch(user_agent, url) if parser is not None else False

    def get_crawl_delay(
            self,
            user_agent: str = "*",
            *,
            base_url: str | None = None,
    ) -> float:
        """Получить Crawl-delay; если директивы нет, вернуть 0.0.

        При нескольких загруженных origin необходимо указать base_url.
        """
        if base_url is not None:
            origin = self._origin(base_url)
        elif len(self._rules) == 1:
            origin = next(iter(self._rules))
        elif not self._rules:
            raise LookupError("robots.txt has not been loaded")
        else:
            raise ValueError("base_url is required when multiple origins are loaded")

        parser = self._rules.get(origin)
        if parser is None:
            raise LookupError(f"robots.txt has not been loaded for {origin}")
        delay = parser.crawl_delay(user_agent)
        return float(delay) if delay is not None else 0.0
