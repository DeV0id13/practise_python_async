import pytest
from unittest.mock import AsyncMock
import asyncio
from async_crawler.retry import RetryStrategy
from async_crawler.exceptions import NetworkError, ParseError, PermanentError, TransientError


async def test_retry_until_success():
    strategy = RetryStrategy(
        max_retries=3,
        base_backoff=0,
    )

    operation = AsyncMock(
        side_effect=[
            TransientError("Temporary failure"),
            TransientError("Temporary failure"),
            "success",
        ]
    )

    result = await strategy.execute_with_retry(operation)

    assert result == "success"
    assert operation.await_count == 3


@pytest.mark.parametrize("value", [-1, True, False, 1.0, "3", None])
def test_invalid_max_retries(value):
    with pytest.raises(ValueError, match="max_retries"):
        RetryStrategy(max_retries=value)


@pytest.mark.parametrize("value", [0, 0.99, -1, True, False, float("nan"), float("inf"), -float("inf"), "2", None, 10**400])
def test_invalid_backoff_factor(value):
    with pytest.raises(ValueError, match="backoff_factor"):
        RetryStrategy(backoff_factor=value)


@pytest.mark.parametrize("name", ["base_backoff", "max_backoff"])
@pytest.mark.parametrize("value", [-1, True, False, float("nan"), float("inf"), -float("inf"), "1", None, 10**400])
def test_invalid_delays(name, value):
    with pytest.raises(ValueError, match=name):
        RetryStrategy(**{name: value})


@pytest.mark.parametrize("value", [
    (), (ValueError,), ValueError, "ValueError", [ValueError()], [BaseException],
    [KeyboardInterrupt], [ValueError, "error"], [object], [123],
])
def test_invalid_retry_on(value):
    with pytest.raises(ValueError, match="retry_on"):
        RetryStrategy(retry_on=value)


def test_valid_boundaries_and_exception_classes():
    strategy = RetryStrategy(
        max_retries=0, backoff_factor=1, base_backoff=0, max_backoff=0,
        retry_on=[ValueError, TransientError],
    )
    assert strategy.max_retries == 0
    assert strategy.backoff_factor == 1.0
    assert strategy.base_backoff == strategy.max_backoff == 0.0
    assert strategy.retry_on == (ValueError, TransientError)
    assert RetryStrategy(retry_on=[]).retry_on == ()


@pytest.mark.parametrize("name", ["retries_by_error", "backoff_by_error"])
@pytest.mark.parametrize("value", [[], (), False, "invalid"])
def test_individual_settings_require_dict(name, value):
    with pytest.raises(ValueError, match=name):
        RetryStrategy(**{name: value})


@pytest.mark.parametrize("name", ["retries_by_error", "backoff_by_error"])
@pytest.mark.parametrize("key", ["error", ValueError(), BaseException, KeyboardInterrupt, object])
def test_individual_settings_require_exception_classes(name, key):
    with pytest.raises(ValueError, match=name):
        RetryStrategy(**{name: {key: 1}})


@pytest.mark.parametrize("value", [-1, 0.5, True, False, float("inf"), "1", None])
def test_invalid_individual_retries(value):
    with pytest.raises(ValueError, match="retries_by_error"):
        RetryStrategy(retries_by_error={NetworkError: value})


@pytest.mark.parametrize("value", [-1, True, False, float("nan"), float("inf"), -float("inf"), "1", None, 10**400])
def test_invalid_individual_backoff(value):
    with pytest.raises(ValueError, match="backoff_by_error"):
        RetryStrategy(backoff_by_error={NetworkError: value})


def test_individual_settings_accept_zero_and_copy():
    retries = {NetworkError: 0}
    backoff = {NetworkError: 0.5}
    strategy = RetryStrategy(retries_by_error=retries, backoff_by_error=backoff)
    retries[NetworkError] = 1
    backoff[NetworkError] = 1
    assert strategy.retries_by_error == {NetworkError: 0}
    assert strategy.backoff_by_error == {NetworkError: 0.5}


async def test_custom_retry_exceptions():
    strategy = RetryStrategy(max_retries=1, base_backoff=0, retry_on=[ValueError])
    operation = AsyncMock(side_effect=[ValueError("temporary"), "ok"])
    assert await strategy.execute_with_retry(operation) == "ok"
    assert operation.await_count == 2


@pytest.fixture
def sleep_mock(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr("async_crawler.retry.asyncio.sleep", sleep)
    return sleep


async def test_permanent_error_no_retry(sleep_mock):
    error = PermanentError("permanent")
    operation = AsyncMock(side_effect=error)
    with pytest.raises(PermanentError) as result:
        await RetryStrategy().execute_with_retry(operation)
    assert result.value is error
    assert operation.await_count == 1
    sleep_mock.assert_not_awaited()


async def test_parse_error_no_retry(sleep_mock):
    error = ParseError("invalid document")
    operation = AsyncMock(side_effect=error)
    with pytest.raises(ParseError) as result:
        await RetryStrategy().execute_with_retry(operation)
    assert result.value is error
    assert operation.await_count == 1
    sleep_mock.assert_not_awaited()


async def test_max_retries(sleep_mock):
    operation = AsyncMock(side_effect=NetworkError("offline"))
    strategy = RetryStrategy(max_retries=2, base_backoff=1)
    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(operation)
    assert operation.await_count == 3
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1, 2]


@pytest.mark.parametrize(("retry_after", "expected"), [(5, 5), (0.5, 1)])
async def test_retry_after(sleep_mock, retry_after, expected):
    operation = AsyncMock(side_effect=[TransientError("busy", retry_after=retry_after), "ok"])
    assert await RetryStrategy(max_retries=1, base_backoff=1).execute_with_retry(operation) == "ok"
    sleep_mock.assert_awaited_once_with(expected)


async def test_exponential_backoff(sleep_mock):
    operation = AsyncMock(side_effect=[NetworkError("offline") for _ in range(3)] + ["ok"])
    strategy = RetryStrategy(max_retries=3, base_backoff=1, backoff_factor=2)
    assert await strategy.execute_with_retry(operation) == "ok"
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1, 2, 4]


@pytest.mark.parametrize("limit", [0, 1])
async def test_retry_limit_by_error(sleep_mock, limit):
    strategy = RetryStrategy(max_retries=5, retries_by_error={NetworkError: limit}, base_backoff=1)
    operation = AsyncMock(side_effect=NetworkError("offline"))
    with pytest.raises(NetworkError):
        await strategy.execute_with_retry(operation)
    assert operation.await_count == limit + 1
    assert sleep_mock.await_count == limit


async def test_backoff_by_error(sleep_mock):
    strategy = RetryStrategy(
        max_retries=2, base_backoff=1, max_backoff=5, backoff_by_error={NetworkError: 3},
    )
    operation = AsyncMock(side_effect=[NetworkError("offline"), NetworkError("offline"), "ok"])
    assert await strategy.execute_with_retry(operation) == "ok"
    assert [call.args[0] for call in sleep_mock.await_args_list] == [3, 5]


async def test_mixed_errors(sleep_mock):
    errors = [TransientError("busy"), NetworkError("offline"), TransientError("busy"), NetworkError("offline")]
    strategy = RetryStrategy(
        max_retries=4, retries_by_error={TransientError: 2, NetworkError: 2},
        backoff_by_error={TransientError: 1, NetworkError: 3},
    )
    operation = AsyncMock(side_effect=errors + ["ok"])
    assert await strategy.execute_with_retry(operation) == "ok"
    assert operation.await_count == 5
    assert [call.args[0] for call in sleep_mock.await_args_list] == [1, 3, 2, 6]

    sleep_mock.reset_mock()
    operation = AsyncMock(side_effect=errors + ["unreachable"])
    with pytest.raises(NetworkError):
        await RetryStrategy(max_retries=3, retries_by_error={TransientError: 2, NetworkError: 2}).execute_with_retry(operation)
    assert operation.await_count == 4
    assert sleep_mock.await_count == 3


async def test_empty_retry_on(sleep_mock):
    operation = AsyncMock(side_effect=TransientError("busy"))
    with pytest.raises(TransientError):
        await RetryStrategy(retry_on=[]).execute_with_retry(operation)
    assert operation.await_count == 1
    sleep_mock.assert_not_awaited()


async def test_pass_arguments(sleep_mock):
    operation = AsyncMock(side_effect=[NetworkError("offline"), "ok"])
    result = await RetryStrategy(max_retries=1).execute_with_retry(operation, "url", 42, timeout=3)
    assert result == "ok"
    assert operation.await_count == 2
    assert all(call.args == ("url", 42) and call.kwargs == {"timeout": 3} for call in operation.await_args_list)

async def test_cancelled_error_no_retry(sleep_mock):
    operation = AsyncMock(
        side_effect=asyncio.CancelledError()
    )

    with pytest.raises(asyncio.CancelledError):
        await RetryStrategy().execute_with_retry(operation)

    assert operation.await_count == 1
    sleep_mock.assert_not_awaited()
