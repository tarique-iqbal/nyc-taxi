from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime
from typing import Any

from etl.utils.json import dumps

# Standard LogRecord attributes that should not be duplicated
# in the JSON output as extra fields.
_STANDARD_LOG_FIELDS: frozenset[str] = frozenset(
    {
        "args",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class JsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.

    Every log line produced by this formatter is machine-readable and
    can be ingested directly by Loki, ELK, or Datadog without a parsing
    pipeline. Extra fields passed via extra={} appear as top-level JSON
    keys alongside the standard fields.

    Standard output shape:
    {
        "timestamp": "2024-01-15T10:30:00.123456+00:00",
        "level": "INFO",
        "logger": "etl.infrastructure.kafka.consumer",
        "message": "Batch flushed",
        "batch_id": "abc-123",
        "valid": 498,
        "invalid": 2
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        if record.stack_info:
            log_entry["stack_info"] = self.formatStack(record.stack_info)

        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_FIELDS and not key.startswith("_"):
                log_entry[key] = value

        return dumps(log_entry)


def setup_logging(level: str = "INFO") -> None:
    """
    Configure the root logger to emit structured JSON to stdout.

    Call once at application startup in lifecycle.py before any
    other code runs. Subsequent getLogger() calls will inherit
    the JSON formatter.

    Args:
        level: Logging level string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Silence overly verbose third-party loggers.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("clickhouse_driver").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger.

    Thin wrapper so modules import from observability rather than
    logging directly, keeping the dependency consistent.
    """
    return logging.getLogger(name)
