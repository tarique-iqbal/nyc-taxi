from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def to_utc(dt: datetime) -> datetime:
    """
    Return a UTC-aware datetime from a naive or offset-aware datetime.

    Naive datetimes are assumed to be UTC, which matches the TLC dataset
    convention. Offset-aware datetimes are converted to UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_timestamp(value: Any) -> datetime | None:
    """
    Coerce a raw Parquet timestamp value to a UTC-aware datetime.

    PyArrow yields Parquet timestamp columns as one of:
      - datetime objects (standard Python)
      - pandas Timestamp objects
      - numpy datetime64 scalars

    Returns None if the value is None or cannot be parsed, so callers
    can decide whether a missing timestamp is a parse error or a default.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return to_utc(value)

    try:
        import pandas as pd

        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return None
        dt = ts.to_pydatetime()
        return to_utc(dt)
    except Exception:
        return None


def parse_timestamp_strict(value: Any, field_name: str = "timestamp") -> datetime:
    """
    Coerce a raw Parquet timestamp value to a UTC-aware datetime.

    Raises ValueError if the value cannot be parsed, unlike the lenient
    parse_timestamp() above. Used in contexts where a missing datetime
    is a hard failure (pickup_datetime, dropoff_datetime).
    """
    result = parse_timestamp(value)
    if result is None:
        raise ValueError(
            f"Cannot parse '{field_name}' value '{value}' as a datetime."
        )
    return result


def format_for_clickhouse(dt: datetime) -> str:
    """
    Format a datetime for ClickHouse DateTime columns.

    ClickHouse expects 'YYYY-MM-DD HH:MM:SS' without timezone suffix
    when inserting via the native protocol. The datetime should already
    be in UTC before calling this.
    """
    utc = to_utc(dt)
    return utc.strftime("%Y-%m-%d %H:%M:%S")


def truncate_to_hour(dt: datetime) -> datetime:
    """Return the datetime truncated to the start of its hour (UTC)."""
    utc = to_utc(dt)
    return utc.replace(minute=0, second=0, microsecond=0)


def truncate_to_day(dt: datetime) -> datetime:
    """Return the datetime truncated to midnight UTC."""
    utc = to_utc(dt)
    return utc.replace(hour=0, minute=0, second=0, microsecond=0)
