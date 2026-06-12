from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    reraise: bool = True,
) -> Callable[[F], F]:
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_attempts: Total number of attempts (including the first).
        delay:        Initial wait in seconds before the first retry.
        backoff:      Multiplier applied to delay after each failure.
                      delay=1.0, backoff=2.0 -> waits 1s, 2s, 4s ...
        exceptions:   Tuple of exception types to catch and retry on.
                      Only these types trigger a retry; others propagate
                      immediately.
        reraise:      If True (default), re-raise the last exception after
                      all attempts are exhausted. If False, return None.

    Usage:
        @retry(max_attempts=5, delay=0.5, backoff=2.0, exceptions=(KafkaException,))
        def publish_batch(self, messages):
            ...

        @retry(max_attempts=3, exceptions=(ClickHouseError,))
        def insert(self, rows):
            ...
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as exc:

                    if attempt == max_attempts:
                        logger.error(
                            "All %d attempts failed for %s: %s",
                            max_attempts,
                            func.__qualname__,
                            exc,
                            extra={
                                "function": func.__qualname__,
                                "attempts": max_attempts,
                                "error": str(exc),
                                "error_type": type(exc).__name__,
                            },
                        )
                        if reraise:
                            raise
                        return None

                    logger.warning(
                        "Attempt %d/%d failed for %s, retrying in %.1fs: %s",
                        attempt,
                        max_attempts,
                        func.__qualname__,
                        current_delay,
                        exc,
                        extra={
                            "function": func.__qualname__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "retry_delay": current_delay,
                            "error_type": type(exc).__name__,
                        },
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff

            return None

        return wrapper  # type: ignore[return-value]

    return decorator  # type: ignore[return-value]


class RetryConfig:
    """
    Named retry configurations for common infrastructure operations.

    Import and apply these instead of specifying raw numbers at call sites
    so retry policies are consistent and easy to tune from one place.
    """

    KAFKA_PUBLISH = dict(max_attempts=5, delay=0.5, backoff=2.0)
    CLICKHOUSE_INSERT = dict(max_attempts=3, delay=1.0, backoff=2.0)
    ZONE_CSV_LOAD = dict(max_attempts=3, delay=0.5, backoff=1.5)
    HEALTH_CHECK = dict(max_attempts=5, delay=2.0, backoff=1.0)
