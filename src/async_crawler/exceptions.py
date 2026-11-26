class CrawlerError(Exception):
    """Базовое исключение краулера."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status = status


class TransientError(CrawlerError):
    """Временная ошибка, допускающая повтор."""

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status: int | None = None,
        retry_after: float = 0.0,
    ) -> None:
        super().__init__(
            message,
            url=url,
            status=status,
        )
        self.retry_after = retry_after


class PermanentError(CrawlerError):
    """Постоянная ошибка, не допускающая повтор."""


class NetworkError(CrawlerError):
    """Ошибка сетевого соединения."""


class ParseError(CrawlerError):
    """Ошибка парсинга документа."""
