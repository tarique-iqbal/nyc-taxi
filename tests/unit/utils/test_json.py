from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from etl.utils.json import ETLJSONEncoder, dumps, dumps_lines, loads


# ETLJSONEncoder: datetime
def test_encoder_datetime_to_iso():
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    result = json.dumps({"dt": dt}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert "2024-01-15" in parsed["dt"]
    assert "10:30:00" in parsed["dt"]


def test_encoder_date_to_iso():
    d = date(2024, 1, 15)
    result = json.dumps({"d": d}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["d"] == "2024-01-15"


def test_encoder_naive_datetime_serialised():
    naive = datetime(2024, 1, 15, 10, 0, 0)
    result = json.dumps({"dt": naive}, cls=ETLJSONEncoder)
    assert "2024-01-15" in result


# ETLJSONEncoder: Decimal
def test_encoder_decimal_to_float():
    result = json.dumps({"amount": Decimal("12.50")}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert isinstance(parsed["amount"], float)
    assert abs(parsed["amount"] - 12.5) < 1e-9


def test_encoder_decimal_zero():
    result = json.dumps({"amount": Decimal("0")}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["amount"] == 0.0


def test_encoder_large_decimal():
    result = json.dumps({"amount": Decimal("9999999.99")}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert abs(parsed["amount"] - 9999999.99) < 0.01


# ETLJSONEncoder: UUID
def test_encoder_uuid_to_string():
    uid = UUID("12345678-1234-5678-1234-567812345678")
    result = json.dumps({"id": uid}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["id"] == "12345678-1234-5678-1234-567812345678"


def test_encoder_uuid_is_string_type():
    uid = UUID("12345678-1234-5678-1234-567812345678")
    result = json.dumps({"id": uid}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert isinstance(parsed["id"], str)


# ETLJSONEncoder: bytes
def test_encoder_bytes_to_hex():
    result = json.dumps({"data": b"\x01\x02\x03\x04"}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["data"] == "01020304"


def test_encoder_empty_bytes():
    result = json.dumps({"data": b""}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["data"] == ""


# ETLJSONEncoder: to_dict()
def test_encoder_object_with_to_dict():
    class MyObj:
        def to_dict(self):
            return {"key": "value", "number": 42}

    result = json.dumps({"obj": MyObj()}, cls=ETLJSONEncoder)
    parsed = json.loads(result)
    assert parsed["obj"]["key"] == "value"
    assert parsed["obj"]["number"] == 42


def test_encoder_unhandled_type_raises():
    class Unknown:
        pass

    with pytest.raises(TypeError):
        json.dumps({"x": Unknown()}, cls=ETLJSONEncoder)


# dumps
def test_dumps_returns_string():
    result = dumps({"key": "value"})
    assert isinstance(result, str)


def test_dumps_plain_dict():
    result = dumps({"a": 1, "b": "two"})
    parsed = json.loads(result)
    assert parsed == {"a": 1, "b": "two"}


def test_dumps_decimal():
    result = dumps({"fare": Decimal("12.50")})
    parsed = json.loads(result)
    assert abs(parsed["fare"] - 12.5) < 1e-9


def test_dumps_datetime():
    dt = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    result = dumps({"pickup": dt})
    assert "2024-01-15" in result


def test_dumps_kwargs_passed_through():
    result = dumps({"key": "value"}, indent=2)
    assert "\n" in result


def test_dumps_empty_dict():
    assert dumps({}) == "{}"


def test_dumps_nested_dict():
    result = dumps({"outer": {"inner": "value"}})
    parsed = json.loads(result)
    assert parsed["outer"]["inner"] == "value"


def test_dumps_none_value():
    result = dumps({"key": None})
    parsed = json.loads(result)
    assert parsed["key"] is None


# loads
def test_loads_string_returns_dict():
    result = loads('{"key": "value"}')
    assert result == {"key": "value"}


def test_loads_bytes_returns_dict():
    result = loads(b'{"key": "value"}')
    assert result == {"key": "value"}


def test_loads_nested_dict():
    result = loads('{"outer": {"inner": 42}}')
    assert result["outer"]["inner"] == 42


def test_loads_json_array_raises_type_error():
    with pytest.raises(TypeError, match="list"):
        loads("[1, 2, 3]")


def test_loads_json_string_raises_type_error():
    with pytest.raises(TypeError, match="str"):
        loads('"just a string"')


def test_loads_json_number_raises_type_error():
    with pytest.raises(TypeError, match="int"):
        loads("42")


def test_loads_json_null_raises_type_error():
    with pytest.raises(TypeError):
        loads("null")


def test_loads_empty_object_returns_empty_dict():
    result = loads("{}")
    assert result == {}


def test_loads_malformed_json_raises():
    with pytest.raises((json.JSONDecodeError, ValueError)):
        loads("{not valid json")


# dumps_lines
def test_dumps_lines_returns_string():
    result = dumps_lines([{"a": 1}, {"b": 2}])
    assert isinstance(result, str)


def test_dumps_lines_one_record_per_line():
    records = [{"id": 1}, {"id": 2}, {"id": 3}]
    result = dumps_lines(records)
    lines = result.strip().split("\n")
    assert len(lines) == 3


def test_dumps_lines_each_line_is_valid_json():
    records = [{"id": i, "val": f"v{i}"} for i in range(5)]
    result = dumps_lines(records)
    for line in result.split("\n"):
        parsed = json.loads(line)
        assert isinstance(parsed, dict)


def test_dumps_lines_preserves_values():
    records = [{"trip_id": "abc", "fare": 12.5}, {"trip_id": "def", "fare": 19.8}]
    result = dumps_lines(records)
    lines = result.split("\n")
    first = json.loads(lines[0])
    assert first["trip_id"] == "abc"
    assert abs(first["fare"] - 12.5) < 1e-9


def test_dumps_lines_handles_decimal():
    records = [{"amount": Decimal("99.99")}]
    result = dumps_lines(records)
    parsed = json.loads(result.strip())
    assert abs(parsed["amount"] - 99.99) < 0.001


def test_dumps_lines_empty_list():
    result = dumps_lines([])
    assert result == ""


def test_dumps_lines_single_record_no_trailing_newline():
    result = dumps_lines([{"x": 1}])
    assert not result.endswith("\n")


# round-trip
def test_round_trip_plain_dict():
    original = {"trip_id": "abc", "vendor": "CMT", "amount": 19.8}
    assert loads(dumps(original)) == original


def test_round_trip_preserves_numeric_types():
    original = {"int_val": 42, "float_val": 3.14}
    result = loads(dumps(original))
    assert result["int_val"] == 42
    assert abs(result["float_val"] - 3.14) < 1e-9


def test_round_trip_decimal_becomes_float():
    original = {"fare": Decimal("12.50")}
    result = loads(dumps(original))
    assert isinstance(result["fare"], float)
    assert abs(result["fare"] - 12.5) < 1e-9
