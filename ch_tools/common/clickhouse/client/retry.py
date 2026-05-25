from typing import Any, Tuple, Type, Union

import requests
import tenacity

from ch_tools.common import logging

from .error import ClickhouseError


def is_transient_error(exc: BaseException) -> bool:
    """
    Determine if an error is transient and can be retried.

    Retryable errors:
    - requests.exceptions.ConnectionError (network issues, DNS, connection reset)
    - requests.exceptions.Timeout, ReadTimeout (transient network issues)
    - requests.exceptions.ChunkedEncodingError (transient network)
    - ClickhouseError with HTTP status codes from proxy/load balancer:
      - 429: Too Many Requests
      - 502: Bad Gateway
      - 503: Service Unavailable
      - 504: Gateway Timeout

    Non-retryable errors:
    - HTTP 500: real ClickHouse DB errors (not idempotent to retry)
    - HTTP 4xx: client errors (syntax, permissions, unknown tables)
    - All other exceptions
    """
    # Network-related errors are retryable
    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True

    # ClickHouse HTTP errors - check status code. Do not rely on
    # requests.Response truthiness: 4xx/5xx responses are falsy.
    if isinstance(exc, ClickhouseError):
        retryable_status_codes = {429, 502, 503, 504}
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code in retryable_status_codes

    return False


def retry(
    exception_types: Union[Type[BaseException], Tuple[Type[BaseException], ...]],
    max_attempts: int = 5,
    max_interval: int = 5,
) -> Any:
    """
    Function decorator that retries wrapped function on failures.
    """
    return tenacity.retry(
        retry=tenacity.retry_if_exception_type(exception_types),
        wait=tenacity.wait_random_exponential(multiplier=0.5, max=max_interval),
        stop=tenacity.stop_after_attempt(max_attempts),
        reraise=True,
    )


def _log_retry_attempt(retry_state: tenacity.RetryCallState) -> None:
    """Log a warning before each retry sleep."""
    if retry_state.outcome is None or retry_state.next_action is None:
        return
    exc = retry_state.outcome.exception()
    logging.warning(
        f"Query failed (attempt {retry_state.attempt_number}): {exc!r}. "
        f"Retrying in {retry_state.next_action.sleep:.2f}s..."
    )
