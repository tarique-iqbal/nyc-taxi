from __future__ import annotations

import pytest

from etl.domain.trip.normalizers import (
    DEFAULT_PASSENGER_COUNT,
    UNKNOWN_LABEL,
    TripNormalizer,
)


# Vendor
def test_vendor_1_mapped():
    result = TripNormalizer.normalize({"vendor_id": 1})
    assert result["vendor_id"] == "Creative Mobile Technologies"


def test_vendor_2_mapped():
    result = TripNormalizer.normalize({"vendor_id": 2})
    assert result["vendor_id"] == "VeriFone Inc."


def test_vendor_unknown_code_returns_unknown():
    result = TripNormalizer.normalize({"vendor_id": 99})
    assert result["vendor_id"] == UNKNOWN_LABEL


def test_vendor_none_returns_unknown():
    result = TripNormalizer.normalize({"vendor_id": None})
    assert result["vendor_id"] == UNKNOWN_LABEL


def test_vendor_string_none_returns_unknown():
    result = TripNormalizer.normalize({"vendor_id": "abc"})
    assert result["vendor_id"] == UNKNOWN_LABEL


# Payment type
@pytest.mark.parametrize(
    "code,expected",
    [
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ],
)
def test_payment_type_codes_mapped(code: int, expected: str):
    result = TripNormalizer.normalize({"payment_type": code})
    assert result["payment_type"] == expected


def test_payment_type_unknown_code():
    result = TripNormalizer.normalize({"payment_type": 99})
    assert result["payment_type"] == UNKNOWN_LABEL


def test_payment_type_none():
    result = TripNormalizer.normalize({"payment_type": None})
    assert result["payment_type"] == UNKNOWN_LABEL


# Rate code
@pytest.mark.parametrize(
    "code,expected",
    [
        (1, "Standard"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride"),
    ],
)
def test_rate_code_mapped(code: int, expected: str):
    result = TripNormalizer.normalize({"rate_code_id": code})
    assert result["rate_code"] == expected


def test_rate_code_as_float_string():
    # Parquet may yield rate_code as 1.0
    result = TripNormalizer.normalize({"rate_code_id": 1.0})
    assert result["rate_code"] == "Standard"


def test_rate_code_unknown():
    result = TripNormalizer.normalize({"rate_code_id": 99})
    assert result["rate_code"] == UNKNOWN_LABEL


def test_rate_code_none():
    result = TripNormalizer.normalize({"rate_code_id": None})
    assert result["rate_code"] == UNKNOWN_LABEL


# Passenger count
def test_passenger_count_none_defaults_to_one():
    result = TripNormalizer.normalize({"passenger_count": None})
    assert result["passenger_count"] == DEFAULT_PASSENGER_COUNT


def test_passenger_count_zero_defaults_to_one():
    result = TripNormalizer.normalize({"passenger_count": 0})
    assert result["passenger_count"] == DEFAULT_PASSENGER_COUNT


def test_passenger_count_preserved():
    result = TripNormalizer.normalize({"passenger_count": 3})
    assert result["passenger_count"] == 3


def test_passenger_count_float_coerced():
    result = TripNormalizer.normalize({"passenger_count": 2.0})
    assert result["passenger_count"] == 2


# Store and forward flag
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Y", "Yes"),
        ("N", "No"),
        ("y", "Yes"),
        ("n", "No"),
    ],
)
def test_store_and_fwd_flag_mapped(raw: str, expected: str):
    result = TripNormalizer.normalize({"store_and_fwd_flag": raw})
    assert result["store_and_fwd_flag"] == expected


def test_store_and_fwd_flag_none_returns_unknown():
    result = TripNormalizer.normalize({"store_and_fwd_flag": None})
    assert result["store_and_fwd_flag"] == UNKNOWN_LABEL


def test_store_and_fwd_flag_unexpected_value_returns_unknown():
    result = TripNormalizer.normalize({"store_and_fwd_flag": "X"})
    assert result["store_and_fwd_flag"] == UNKNOWN_LABEL


# Input dict is not mutated
def test_normalize_does_not_mutate_input():
    raw = {"vendor_id": 1, "payment_type": 2, "passenger_count": None}
    original = dict(raw)
    TripNormalizer.normalize(raw)
    assert raw == original


# Full normalize output has expected keys
def test_normalize_returns_all_normalised_keys():
    raw = {
        "vendor_id": 1,
        "payment_type": 1,
        "rate_code_id": 1,
        "passenger_count": 2,
        "store_and_fwd_flag": "N",
    }
    result = TripNormalizer.normalize(raw)
    assert "vendor_id" in result
    assert "payment_type" in result
    assert "rate_code" in result
    assert "passenger_count" in result
    assert "store_and_fwd_flag" in result
