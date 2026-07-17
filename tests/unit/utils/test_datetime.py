from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest

from etl.utils.datetime import (
    format_for_clickhouse,
    parse_timestamp,
    parse_timestamp_strict,
    to_utc,
    truncate_to_day,
    truncate_to_hour,
)


# to_utc
def test_to_utc_naive_datetime_gets_utc_timezone():
    dt = datetime(2024, 1, 15, 10, 30, 0)
    result = to_utc(dt)
    assert result.tzinfo == UTC


def test_to_utc_naive_datetime_preserves_values():
    dt = datetime(2024, 1, 15, 10, 30, 45)
    result = to_utc(dt)
    assert result.year == 2024
    assert result.hour == 10
    assert result.second == 45


def test_to_utc_already_utc_unchanged():
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    result = to_utc(dt)
    assert result == dt
    assert result.tzinfo == UTC


def test_to_utc_offset_aware_converted_to_utc():
    est = timezone(timedelta(hours=-5))
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=est)
    result = to_utc(dt)
    assert result.tzinfo == UTC
    assert result.hour == 15  # 10am EST = 15:00 UTC


def test_to_utc_returns_new_object_for_naive():
    dt = datetime(2024, 1, 15, 10, 0, 0)
    result = to_utc(dt)
    assert result is not dt


# parse_timestamp
def test_parse_timestamp_none_returns_none():
    assert parse_timestamp(None) is None


def test_parse_timestamp_aware_datetime_returned():
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    result = parse_timestamp(dt)
    assert result == dt
    assert result.tzinfo == UTC


def test_parse_timestamp_naive_datetime_made_utc():
    dt = datetime(2024, 1, 15, 10, 0, 0)
    result = parse_timestamp(dt)
    assert result is not None
    assert result.tzinfo == UTC
    assert result.hour == 10


def test_parse_timestamp_pandas_timestamp():
    ts = pd.Timestamp("2024-01-15 10:30:00")

    result = parse_timestamp(ts)

    assert result is not None
    assert result.year == 2024
    assert result.hour == 10
    assert result.minute == 30


def test_parse_timestamp_pandas_nat_returns_none():
    result = parse_timestamp(pd.NaT)

    assert result is None


def test_parse_timestamp_invalid_value_returns_none():
    result = parse_timestamp("not-a-date")
    assert result is None


def test_parse_timestamp_unparseable_object_returns_none():
    result = parse_timestamp({"key": "value"})
    assert result is None


def test_parse_timestamp_integer_as_nanoseconds():
    # pandas can interpret integers as nanosecond timestamps
    ts = pd.Timestamp(0)

    result = parse_timestamp(ts)

    assert result is not None


# parse_timestamp_strict
def test_parse_timestamp_strict_valid_datetime_returned():
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    result = parse_timestamp_strict(dt)
    assert result == dt


def test_parse_timestamp_strict_none_raises_value_error():
    with pytest.raises(ValueError):
        parse_timestamp_strict(None)


def test_parse_timestamp_strict_invalid_raises_value_error():
    with pytest.raises(ValueError):
        parse_timestamp_strict("not-a-date")


def test_parse_timestamp_strict_error_message_includes_field_name():
    with pytest.raises(ValueError, match="pickup_datetime"):
        parse_timestamp_strict(None, field_name="pickup_datetime")


def test_parse_timestamp_strict_naive_datetime_made_utc():
    dt = datetime(2024, 1, 15, 10, 0, 0)
    result = parse_timestamp_strict(dt)
    assert result.tzinfo == UTC


# format_for_clickhouse
def test_format_for_clickhouse_utc_datetime():
    dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
    result = format_for_clickhouse(dt)
    assert result == "2024-01-15 10:30:45"


def test_format_for_clickhouse_no_timezone_suffix():
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    result = format_for_clickhouse(dt)
    assert "+" not in result
    assert "Z" not in result
    assert "UTC" not in result


def test_format_for_clickhouse_converts_offset_to_utc():
    est = timezone(timedelta(hours=-5))
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=est)
    result = format_for_clickhouse(dt)
    assert result == "2024-01-15 15:00:00"


def test_format_for_clickhouse_naive_assumed_utc():
    dt = datetime(2024, 1, 15, 10, 30, 0)
    result = format_for_clickhouse(dt)
    assert result == "2024-01-15 10:30:00"


def test_format_for_clickhouse_midnight():
    dt = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)
    assert format_for_clickhouse(dt) == "2024-01-15 00:00:00"


def test_format_for_clickhouse_end_of_day():
    dt = datetime(2024, 1, 15, 23, 59, 59, tzinfo=UTC)
    assert format_for_clickhouse(dt) == "2024-01-15 23:59:59"


# truncate_to_hour
def test_truncate_to_hour_clears_minutes_and_seconds():
    dt = datetime(2024, 1, 15, 10, 47, 33, tzinfo=UTC)
    result = truncate_to_hour(dt)
    assert result.hour == 10
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_truncate_to_hour_preserves_date_and_hour():
    dt = datetime(2024, 6, 20, 14, 59, 59, tzinfo=UTC)
    result = truncate_to_hour(dt)
    assert result.year == 2024
    assert result.month == 6
    assert result.day == 20
    assert result.hour == 14


def test_truncate_to_hour_already_on_hour_unchanged():
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    result = truncate_to_hour(dt)
    assert result == dt


def test_truncate_to_hour_result_is_utc():
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    result = truncate_to_hour(dt)
    assert result.tzinfo == UTC


# truncate_to_day
def test_truncate_to_day_clears_time_components():
    dt = datetime(2024, 1, 15, 14, 30, 45, 123456, tzinfo=UTC)
    result = truncate_to_day(dt)
    assert result.hour == 0
    assert result.minute == 0
    assert result.second == 0
    assert result.microsecond == 0


def test_truncate_to_day_preserves_date():
    dt = datetime(2024, 6, 20, 23, 59, 59, tzinfo=UTC)
    result = truncate_to_day(dt)
    assert result.year == 2024
    assert result.month == 6
    assert result.day == 20


def test_truncate_to_day_already_midnight_unchanged():
    dt = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)
    result = truncate_to_day(dt)
    assert result == dt


def test_truncate_to_day_result_is_utc():
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
    result = truncate_to_day(dt)
    assert result.tzinfo == UTC


def test_truncate_to_day_offset_datetime_converts_to_utc_first():
    est = timezone(timedelta(hours=-5))
    dt = datetime(2024, 1, 15, 2, 0, 0, tzinfo=est)  # 02:00 EST = 07:00 UTC same day
    result = truncate_to_day(dt)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15
    assert result.hour == 0
