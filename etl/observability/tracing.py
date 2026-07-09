from __future__ import annotations

import functools
import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

# OpenTelemetry is an optional dependency. If it is not installed the
# tracing functions degrade gracefully to no-ops so the rest of the
# pipeline is unaffected.
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    logger.debug("opentelemetry not installed -- tracing disabled")

_tracer: Any = None

F = TypeVar("F", bound=Callable[..., Any])


def setup_tracing(service_name: str = "nyc-taxi-etl") -> None:
    """
    Initialise the OpenTelemetry tracer.

    Call once at startup in lifecycle.py after setup_logging().
    If opentelemetry is not installed this is a no-op.

    The OTLP exporter endpoint is read from the standard
    OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
    """
    global _tracer

    if not _OTEL_AVAILABLE:
        return

    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer(service_name)
        logger.info("OpenTelemetry tracing initialised", extra={"service": service_name})
    except Exception as exc:
        logger.warning("Failed to initialise tracing: %s", exc)
        _tracer = None


def get_tracer() -> Any:
    """Return the tracer, or None if tracing is not configured."""
    return _tracer


@contextmanager
def span(name: str, **attributes: Any) -> Generator[Any, None, None]:
    """
    Context manager that wraps a block of code in a trace span.

    Degrades to a no-op context manager if tracing is not configured.
    Attributes are set as span attributes for filtering in the trace UI.

    Usage:
        with span("batch.insert", batch_id=batch_id, rows=len(rows)):
            inserter.insert(rows)
    """
    tracer = get_tracer()
    if tracer is None or not _OTEL_AVAILABLE:
        yield None
        return

    with tracer.start_as_current_span(name) as current_span:
        for key, value in attributes.items():
            current_span.set_attribute(key, str(value))
        try:
            yield current_span
        except Exception as exc:
            current_span.record_exception(exc)
            current_span.set_status(trace.status.Status(trace.status.StatusCode.ERROR, str(exc)))
            raise


def traced(span_name: str | None = None, **span_attrs: Any) -> Callable[[F], F]:
    """
    Decorator that wraps a function in a trace span.

    The span name defaults to the function's qualified name.

    Usage:
        @traced("kafka.publish_batch", topic="nyc-taxi-trips")
        def publish_batch(self, messages):
            ...
    """

    def decorator(func: F) -> F:
        name = span_name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with span(name, **span_attrs):
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
