"""
Tests for the monitoring logging re-export shim.

Ensures the shim exposes the same public API and objects as the
underlying observability modules.
"""

from __future__ import annotations

import logging


# Import surface
def test_setup_logging_importable():
    from etl.infrastructure.monitoring.logging import setup_logging

    assert callable(setup_logging)


def test_get_logger_importable():
    from etl.infrastructure.monitoring.logging import get_logger

    assert callable(get_logger)


def test_batch_correlation_context_importable():
    from etl.infrastructure.monitoring.logging import BatchCorrelationContext

    assert BatchCorrelationContext is not None


def test_correlation_filter_importable():
    from etl.infrastructure.monitoring.logging import CorrelationFilter

    assert CorrelationFilter is not None


def test_get_correlation_id_importable():
    from etl.infrastructure.monitoring.logging import get_correlation_id

    assert callable(get_correlation_id)


def test_set_correlation_id_importable():
    from etl.infrastructure.monitoring.logging import set_correlation_id

    assert callable(set_correlation_id)


def test_all_exports_declared():
    import etl.infrastructure.monitoring.logging as shim

    for name in shim.__all__:
        assert hasattr(shim, name), f"__all__ declares '{name}' but it is not present"


# Same objects as originals
def test_setup_logging_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import setup_logging as shim_fn
    from etl.observability.structured_logging import setup_logging as original

    assert shim_fn is original


def test_get_logger_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import get_logger as shim_fn
    from etl.observability.structured_logging import get_logger as original

    assert shim_fn is original


def test_batch_correlation_context_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import BatchCorrelationContext as shim_cls
    from etl.observability.correlation import BatchCorrelationContext as original

    assert shim_cls is original


def test_correlation_filter_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import CorrelationFilter as shim_cls
    from etl.observability.correlation import CorrelationFilter as original

    assert shim_cls is original


def test_get_correlation_id_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import get_correlation_id as shim_fn
    from etl.observability.correlation import get_correlation_id as original

    assert shim_fn is original


def test_set_correlation_id_is_same_object_as_original():
    from etl.infrastructure.monitoring.logging import set_correlation_id as shim_fn
    from etl.observability.correlation import set_correlation_id as original

    assert shim_fn is original


# Functional: get_logger
def test_get_logger_via_shim_returns_logger():
    from etl.infrastructure.monitoring.logging import get_logger

    logger = get_logger("etl.infrastructure.monitoring.test")
    assert isinstance(logger, logging.Logger)


def test_get_logger_via_shim_name_matches():
    from etl.infrastructure.monitoring.logging import get_logger

    logger = get_logger("etl.monitoring.shim.test")
    assert logger.name == "etl.monitoring.shim.test"


def test_get_logger_same_name_via_shim_and_original_return_same_instance():
    from etl.infrastructure.monitoring.logging import get_logger as shim_logger
    from etl.observability.structured_logging import get_logger as orig_logger

    assert shim_logger("etl.same.logger") is orig_logger("etl.same.logger")


# Functional: correlation ID via shim
def test_set_and_get_correlation_id_via_shim():
    from etl.infrastructure.monitoring.logging import (
        get_correlation_id,
        set_correlation_id,
    )

    token = set_correlation_id("shim-test-id")
    try:
        assert get_correlation_id() == "shim-test-id"
    finally:
        from etl.observability.correlation import reset_correlation_id

        reset_correlation_id(token)


def test_batch_correlation_context_via_shim_sets_id():
    from etl.infrastructure.monitoring.logging import (
        BatchCorrelationContext,
        get_correlation_id,
    )

    with BatchCorrelationContext("monitor-shim-batch"):
        assert get_correlation_id() == "monitor-shim-batch"
    assert get_correlation_id() != "monitor-shim-batch"


def test_correlation_id_cleared_after_context_exits():
    from etl.infrastructure.monitoring.logging import (
        BatchCorrelationContext,
        get_correlation_id,
    )

    with BatchCorrelationContext("temp-id"):
        pass
    assert get_correlation_id() != "temp-id"


# Functional: setup_logging via shim
def test_setup_logging_via_shim_sets_level():
    from etl.infrastructure.monitoring.logging import setup_logging

    setup_logging("WARNING")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    setup_logging("INFO")  # restore


def test_setup_logging_via_shim_attaches_json_formatter():
    from etl.infrastructure.monitoring.logging import setup_logging
    from etl.observability.structured_logging import JsonFormatter

    setup_logging("INFO")
    root = logging.getLogger()
    assert any(isinstance(h.formatter, JsonFormatter) for h in root.handlers)
