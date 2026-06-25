from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from etl.infrastructure.kafka.serializer import KafkaSerializer


@pytest.fixture
def serializer() -> KafkaSerializer:
    return KafkaSerializer()


# serialize
def test_serialize_returns_bytes(serializer):
    result = serializer.serialize({"key": "value"})
    assert isinstance(result, bytes)


def test_serialize_produces_valid_json(serializer):
    result = serializer.serialize({"trip_id": "abc", "fare": 12.5})
    parsed = json.loads(result.decode("utf-8"))
    assert parsed["trip_id"] == "abc"
    assert parsed["fare"] == 12.5


def test_serialize_datetime_to_iso_string(serializer):
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    result = serializer.serialize({"pickup_datetime": dt})
    parsed = json.loads(result.decode("utf-8"))
    assert "2024-01-15" in parsed["pickup_datetime"]
    assert "10:30:00" in parsed["pickup_datetime"]


def test_serialize_decimal_to_float(serializer):
    result = serializer.serialize({"fare_amount": Decimal("12.50")})
    parsed = json.loads(result.decode("utf-8"))
    assert parsed["fare_amount"] == 12.5
    assert isinstance(parsed["fare_amount"], float)


def test_serialize_uuid_to_string(serializer):
    uid = UUID("12345678-1234-5678-1234-567812345678")
    result = serializer.serialize({"batch_id": uid})
    parsed = json.loads(result.decode("utf-8"))
    assert parsed["batch_id"] == "12345678-1234-5678-1234-567812345678"


def test_serialize_empty_dict(serializer):
    result = serializer.serialize({})
    assert result == b"{}"


def test_serialize_nested_dict(serializer):
    result = serializer.serialize({"a": {"b": "c"}})
    parsed = json.loads(result.decode("utf-8"))
    assert parsed["a"]["b"] == "c"


def test_serialize_none_value(serializer):
    result = serializer.serialize({"key": None})
    parsed = json.loads(result.decode("utf-8"))
    assert parsed["key"] is None


# deserialize
def test_deserialize_returns_dict(serializer):
    data = b'{"trip_id": "abc", "fare": 12.5}'
    result = serializer.deserialize(data)
    assert isinstance(result, dict)
    assert result["trip_id"] == "abc"


def test_deserialize_none_returns_none(serializer):
    assert serializer.deserialize(None) is None


def test_deserialize_empty_bytes_returns_none(serializer):
    assert serializer.deserialize(b"") is None


def test_deserialize_malformed_json_returns_none(serializer):
    result = serializer.deserialize(b"not valid json {{{")
    assert result is None


def test_deserialize_non_utf8_returns_none(serializer):
    result = serializer.deserialize(b"\xff\xfe invalid")
    assert result is None


# round-trip
def test_round_trip_plain_dict(serializer):
    original = {"trip_id": "abc123", "vendor_id": "CMT", "total_amount": 19.8}
    serialized = serializer.serialize(original)
    restored = serializer.deserialize(serialized)
    assert restored == original


def test_round_trip_preserves_string_values(serializer):
    original = {"vendor_id": "Creative Mobile Technologies", "zone": "Midtown Center"}
    restored = serializer.deserialize(serializer.serialize(original))
    assert restored["vendor_id"] == "Creative Mobile Technologies"
    assert restored["zone"] == "Midtown Center"


def test_round_trip_float_amounts(serializer):
    original = {"fare_amount": 12.5, "tip_amount": 3.0, "total_amount": 19.8}
    restored = serializer.deserialize(serializer.serialize(original))
    assert abs(restored["fare_amount"] - 12.5) < 1e-9
    assert abs(restored["total_amount"] - 19.8) < 1e-9


# serialize_batch
def test_serialize_batch_returns_list_of_bytes(serializer):
    messages = [{"id": 1}, {"id": 2}, {"id": 3}]
    result = serializer.serialize_batch(messages)
    assert isinstance(result, list)
    assert all(isinstance(b, bytes) for b in result)
    assert len(result) == 3


def test_serialize_batch_preserves_order(serializer):
    messages = [{"id": i} for i in range(5)]
    result = serializer.serialize_batch(messages)
    for i, payload in enumerate(result):
        parsed = json.loads(payload.decode("utf-8"))
        assert parsed["id"] == i


def test_serialize_batch_empty_list(serializer):
    result = serializer.serialize_batch([])
    assert result == []
