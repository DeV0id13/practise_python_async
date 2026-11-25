from urllib.parse import urldefrag, urljoin, urlsplit
from email.utils import parsedate_to_datetime
from time import time
import aiohttp

MAX_REDIRECTS = 5
REDIRECT_STATUSES = {301, 302, 303, 307, 308}


def redirect_target(response: aiohttp.ClientResponse, url: str) -> str:
    """Проверить HTTP-перенаправление и вернуть абсолютный URL."""
    location = response.headers.get("Location")
    if response.status not in REDIRECT_STATUSES or not location:
        raise aiohttp.ClientResponseError(
            response.request_info, response.history, status=response.status,
            message="Unsupported redirect or missing Location",
        )
    try:
        target, _ = urldefrag(urljoin(url, location))
        parsed = urlsplit(target)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("invalid redirect target")
        _ = parsed.port
    except ValueError as error:
        raise aiohttp.ClientResponseError(
            response.request_info, response.history, status=response.status,
            message="Invalid redirect target",
        ) from error
    return target


def retry_after_seconds(value: str | None) -> float:
    """Преобразовать Retry-After в секунды."""
    if not value:
        return 0.0

    value = value.strip()

    try:
        if value.isascii() and value.isdigit():
            return float(int(value))

        retry_date = parsedate_to_datetime(value)
        return max(0.0, retry_date.timestamp() - time())

    except (ValueError, TypeError, OverflowError, OSError):
        return 0.0
