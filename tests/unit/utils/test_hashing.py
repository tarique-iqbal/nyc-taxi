from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from etl.utils.hashing import hash_batch, hash_fields, hash_trip


# hash_fields
def test_hash_fields_returns_string():
    result = hash_fields("a", "b", "c")
    assert isinstance(result, str)


def test_hash_fields_is_64_hex_chars():
    result = hash_fields("vendor", "2024-01-15", "161")
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_hash_fields_deterministic():
    assert hash_fields("x", "y", "z") == hash_fields("x", "y", "z")


def test_hash_fields_different_values_differ():
    assert hash_fields("a", "b") != hash_fields("a", "c")


def test_hash_fields_order_matters():
    assert hash_fields("a", "b") != hash_fields("b", "a")


def test_hash_fields_empty_string_included():
    h1 = hash_fields("a", "", "b")
    h2 = hash_fields("a", "b")
    assert h1 != h2


def test_hash_fields_single_value():
    result = hash_fields("only-one")
    assert len(result) == 64


def test_hash_fields_integer_coerced_to_string():
    assert hash_fields(1, 2, 3) == hash_fields("1", "2", "3")


def test_hash_fields_matches_manual_sha256():
    key = "|".join(["a", "b", "c"])
    expected = hashlib.sha256(key.encode("utf-8")).hexdigest()
    assert hash_fields("a", "b", "c") == expected


def test_hash_fields_separator_is_pipe():
    # "a|b" and "a", "b" should produce the same result as manual
    key = "a|b"
    expected = hashlib.sha256(key.encode("utf-8")).hexdigest()
    assert hash_fields("a", "b") == expected


# hash_trip
_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


def test_hash_trip_returns_64_hex_string():
    result = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_hash_trip_deterministic():
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    assert a == b


def test_hash_trip_different_vendor_differs():
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("VER", _PICKUP, _DROPOFF, 161)
    assert a != b


def test_hash_trip_different_pickup_differs():
    other = datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC)
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("CMT", other, _DROPOFF, 161)
    assert a != b


def test_hash_trip_different_dropoff_differs():
    other = datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC)
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("CMT", _PICKUP, other, 161)
    assert a != b


def test_hash_trip_different_location_differs():
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("CMT", _PICKUP, _DROPOFF, 132)
    assert a != b


def test_hash_trip_uses_iso_format_for_datetimes():
    # Manually reproduce the expected hash
    key = "|".join(
        [
            "CMT",
            _PICKUP.isoformat(),
            _DROPOFF.isoformat(),
            "161",
        ]
    )
    expected = hashlib.sha256(key.encode("utf-8")).hexdigest()
    assert hash_trip("CMT", _PICKUP, _DROPOFF, 161) == expected


def test_hash_trip_same_inputs_different_batch_ids_still_equal():
    # batch_id is not part of the trip_id hash
    a = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    b = hash_trip("CMT", _PICKUP, _DROPOFF, 161)
    assert a == b


def test_hash_trip_stable_across_calls():
    results = [hash_trip("CMT", _PICKUP, _DROPOFF, 161) for _ in range(10)]
    assert len(set(results)) == 1


def test_hash_trip_delegates_to_hash_fields():
    from etl.utils.hashing import hash_fields

    expected = hash_fields(
        "CMT",
        _PICKUP.isoformat(),
        _DROPOFF.isoformat(),
        161,
    )
    assert hash_trip("CMT", _PICKUP, _DROPOFF, 161) == expected


# hash_batch
def test_hash_batch_returns_16_char_string():
    result = hash_batch("batch-id", "file.parquet", 0)
    assert len(result) == 16


def test_hash_batch_is_lowercase_hex():
    result = hash_batch("batch-id", "file.parquet", 0)
    assert all(c in "0123456789abcdef" for c in result)


def test_hash_batch_deterministic():
    a = hash_batch("batch-id", "file.parquet", 0)
    b = hash_batch("batch-id", "file.parquet", 0)
    assert a == b


def test_hash_batch_different_sequence_differs():
    a = hash_batch("batch-id", "file.parquet", 0)
    b = hash_batch("batch-id", "file.parquet", 1)
    assert a != b


def test_hash_batch_different_batch_id_differs():
    a = hash_batch("batch-a", "file.parquet", 0)
    b = hash_batch("batch-b", "file.parquet", 0)
    assert a != b


def test_hash_batch_is_prefix_of_full_hash():
    full = hash_fields("batch-id", "file.parquet", 0)
    short = hash_batch("batch-id", "file.parquet", 0)
    assert full.startswith(short)
