from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar, Token

# Thread-safe and async-safe context variable.
# Each batch assigns a new UUID here at entry (ParquetReader / Kafka consumer).
# Every log line in that batch carries the same ID so a full trace can be
# reconstructed with a single grep.
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def new_correlation_id() -> str:
    """Generate a new random UUID string for a batch."""
    return str(uuid.uuid4())


def set_correlation_id(correlation_id: str) -> Token[str]:
    """
    Set the correlation ID for the current context.

    Returns the Token needed to reset to the previous value,
    which is useful in tests and when using bind_correlation_id().
    """
    return _correlation_id.set(correlation_id)


def get_correlation_id() -> str:
    """Return the correlation ID set for the current context, or empty string."""
    return _correlation_id.get()


def reset_correlation_id(token: Token[str]) -> None:
    """Reset the correlation ID to its value before a set_correlation_id() call."""
    _correlation_id.reset(token)


class CorrelationFilter(logging.Filter):
    """
    Logging filter that injects the current correlation ID into every log record.

    Added to handlers in setup_logging() so correlation_id appears in every
    JSON log line without requiring callers to pass it as an extra= kwarg.

    If no correlation ID is set for the current context the field is
    omitted (empty string is filtered out) to keep logs clean.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        cid = get_correlation_id()
        if cid:
            record.correlation_id = cid  # type: ignore[attr-defined]
        return True


class BatchCorrelationContext:
    """
    Context manager that sets and clears a correlation ID for a batch.

    Usage:
        with BatchCorrelationContext(batch_id) as ctx:
            process_batch(...)
            logger.info("Done", extra={"rows": n})
            # log line will include correlation_id=batch_id automatically

    The correlation ID is restored to its previous value on exit,
    which is important in multi-threaded or async contexts.
    """

    def __init__(self, correlation_id: str | None = None) -> None:
        self.correlation_id = correlation_id or new_correlation_id()
        self._token: Token[str] | None = None

    def __enter__(self) -> BatchCorrelationContext:
        self._token = set_correlation_id(self.correlation_id)
        return self

    def __exit__(self, *_: object) -> None:
        if self._token is not None:
            reset_correlation_id(self._token)
