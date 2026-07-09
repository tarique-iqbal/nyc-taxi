from __future__ import annotations

import json
import logging
import sys

from etl.observability.structured_logging import JsonFormatter, get_logger, setup_logging


def _make_record(
    message: str = "test message",
    level: int = logging.INFO,
    name: str = "etl.test",
    **extra: object,
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="test.py",
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def _format(record: logging.LogRecord) -> dict:
    formatter = JsonFormatter()
    raw = formatter.format(record)
    return json.loads(raw)


# JsonFormatter: required fields
def test_format_returns_valid_json():
    formatter = JsonFormatter()
    raw = formatter.format(_make_record())
    parsed = json.loads(raw)
    assert isinstance(parsed, dict)


def test_format_includes_timestamp():
    d = _format(_make_record())
    assert "timestamp" in d
    assert "T" in d["timestamp"]  # ISO 8601 format


def test_format_timestamp_is_utc():
    d = _format(_make_record())
    assert "+00:00" in d["timestamp"]


def test_format_includes_level():
    d = _format(_make_record(level=logging.WARNING))
    assert d["level"] == "WARNING"


def test_format_includes_logger_name():
    d = _format(_make_record(name="etl.infrastructure.kafka"))
    assert d["logger"] == "etl.infrastructure.kafka"


def test_format_includes_message():
    d = _format(_make_record(message="Batch persisted"))
    assert d["message"] == "Batch persisted"


def test_format_level_names():
    for level, name in [
        (logging.DEBUG, "DEBUG"),
        (logging.INFO, "INFO"),
        (logging.WARNING, "WARNING"),
        (logging.ERROR, "ERROR"),
        (logging.CRITICAL, "CRITICAL"),
    ]:
        d = _format(_make_record(level=level))
        assert d["level"] == name


# JsonFormatter: extra fields
def test_format_extra_fields_appear_as_top_level_keys():
    record = _make_record()
    record.batch_id = "abc-123"
    record.size = 498
    d = _format(record)
    assert d["batch_id"] == "abc-123"
    assert d["size"] == 498


def test_format_extra_correlation_id_included():
    record = _make_record()
    record.correlation_id = "uuid-xyz"
    d = _format(record)
    assert d["correlation_id"] == "uuid-xyz"


def test_format_extra_nested_dict_included():
    record = _make_record()
    record.context = {"topic": "nyc-taxi-trips", "partition": 0}
    d = _format(record)
    assert d["context"]["topic"] == "nyc-taxi-trips"


# JsonFormatter: standard fields not duplicated
def test_format_does_not_include_internal_lineno_as_extra():
    record = _make_record()
    d = _format(record)
    # lineno is a standard LogRecord field — should not appear as extra key
    # (it's not in the standard output fields either)
    assert "lineno" not in d


def test_format_does_not_include_args_as_extra():
    record = _make_record()
    d = _format(record)
    assert "args" not in d


def test_format_does_not_include_private_attributes():
    record = _make_record()
    record._private = "should be hidden"
    d = _format(record)
    assert "_private" not in d


# JsonFormatter: exception info
def test_format_includes_exception_when_exc_info_set():
    try:
        raise ValueError("test error")
    except ValueError:
        record = logging.LogRecord(
            name="etl.test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Something failed",
            args=(),
            exc_info=sys.exc_info(),
        )
    formatter = JsonFormatter()
    raw = formatter.format(record)
    d = json.loads(raw)
    assert "exception" in d
    assert "ValueError" in d["exception"]
    assert "test error" in d["exception"]


def test_format_no_exception_field_when_no_exc_info():
    d = _format(_make_record())
    assert "exception" not in d


# JsonFormatter: output is single line
def test_format_output_is_single_line():
    d_raw = JsonFormatter().format(_make_record(message="hello world"))
    assert "\n" not in d_raw


# setup_logging
def test_setup_logging_sets_info_level_by_default():
    setup_logging("INFO")
    root = logging.getLogger()
    assert root.level == logging.INFO


def test_setup_logging_sets_debug_level():
    setup_logging("DEBUG")
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    setup_logging("INFO")  # restore


def test_setup_logging_sets_warning_level():
    setup_logging("WARNING")
    root = logging.getLogger()
    assert root.level == logging.WARNING
    setup_logging("INFO")  # restore


def test_setup_logging_attaches_json_formatter():
    setup_logging("INFO")
    root = logging.getLogger()
    assert len(root.handlers) > 0
    handler = root.handlers[0]
    assert isinstance(handler.formatter, JsonFormatter)


def test_setup_logging_handler_writes_to_stdout():
    setup_logging("INFO")
    root = logging.getLogger()
    handler = root.handlers[0]
    assert handler.stream is sys.stdout


def test_setup_logging_silences_confluent_kafka():
    setup_logging("DEBUG")
    kafka_logger = logging.getLogger("confluent_kafka")
    assert kafka_logger.level == logging.WARNING
    setup_logging("INFO")


def test_setup_logging_silences_clickhouse_driver():
    setup_logging("DEBUG")
    ch_logger = logging.getLogger("clickhouse_driver")
    assert ch_logger.level == logging.WARNING
    setup_logging("INFO")


def test_setup_logging_clears_existing_handlers():
    root = logging.getLogger()
    root.addHandler(logging.StreamHandler())
    root.addHandler(logging.StreamHandler())
    assert len(root.handlers) > 1
    setup_logging("INFO")
    assert len(root.handlers) == 1


# get_logger
def test_get_logger_returns_logger_instance():
    logger = get_logger("etl.test.module")
    assert isinstance(logger, logging.Logger)


def test_get_logger_has_correct_name():
    logger = get_logger("etl.infrastructure.kafka")
    assert logger.name == "etl.infrastructure.kafka"


def test_get_logger_same_name_returns_same_instance():
    logger_a = get_logger("etl.same")
    logger_b = get_logger("etl.same")
    assert logger_a is logger_b


# Integration: end-to-end log capture
def test_log_message_captured_as_json(capsys):
    setup_logging("INFO")
    logger = get_logger("etl.test.capture")
    logger.info("Pipeline started", extra={"batch_id": "test-batch"})
    captured = capsys.readouterr()
    lines = [line for line in captured.out.strip().split("\n") if line]
    assert len(lines) >= 1
    last_line = lines[-1]
    d = json.loads(last_line)
    assert d["message"] == "Pipeline started"
    assert d["batch_id"] == "test-batch"
    assert d["level"] == "INFO"
