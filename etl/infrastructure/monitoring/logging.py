from __future__ import annotations

# This module is a thin re-export shim.
#
# The structured JSON logging implementation lives in
# etl/observability/structured_logging.py, which is the correct
# architectural home (observability is a cross-cutting concern,
# not an infrastructure concern).
#
# infrastructure/monitoring/logging.py exists to satisfy the project
# structure and to give monitoring-layer callers a stable import path
# that will not break if the observability internals are reorganised.
from etl.observability.correlation import (
    BatchCorrelationContext,
    CorrelationFilter,
    get_correlation_id,
    set_correlation_id,
)
from etl.observability.structured_logging import (
    get_logger,
    setup_logging,
)

__all__ = [
    "get_logger",
    "setup_logging",
    "BatchCorrelationContext",
    "CorrelationFilter",
    "get_correlation_id",
    "set_correlation_id",
]
