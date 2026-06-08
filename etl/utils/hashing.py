from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any


def hash_fields(*values: Any) -> str:
    """
    Generate a deterministic SHA-256 hex digest from any sequence of values.

    Values are joined with '|' as separator, which is unlikely to appear
    in TLC field values. Each value is cast to str so callers do not need
    to pre-convert types.

    Used directly by hash_trip(). Also available for ad-hoc hashing
    elsewhere in the pipeline (e.g. batch ID generation).
    """
    key = "|".join(str(v) for v in values)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def hash_trip(
    vendor_id: str,
    pickup_datetime: datetime,
    dropoff_datetime: datetime,
    pickup_location_id: int,
) -> str:
    """
    Generate the canonical trip_id from the natural key fields.

    This is the single source of truth for trip_id generation.
    domain/trip/deduplicator.py delegates to this function so the
    hashing logic lives in one place.

    The same four inputs always produce the same SHA-256 hex digest,
    making trip_id stable across Kafka replays. ClickHouse
    ReplacingMergeTree uses trip_id to deduplicate replayed messages.

    ISO format is used for datetimes to avoid locale-dependent output.
    """
    return hash_fields(
        vendor_id,
        pickup_datetime.isoformat(),
        dropoff_datetime.isoformat(),
        pickup_location_id,
    )


def hash_batch(batch_id: str, source_file: str, sequence: int) -> str:
    """
    Generate a short identifier for a specific batch within a source file.

    Not used for deduplication -- only for log correlation and tracing.
    """
    return hash_fields(batch_id, source_file, sequence)[:16]
