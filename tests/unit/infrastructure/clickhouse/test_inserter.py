from __future__ import annotations

from datetime import UTC, datetime

from etl.infrastructure.clickhouse.inserter import _coerce_row

_BASE_ROW = {
    "trip_id": "abc123",
    "vendor_id": "Creative Mobile Technologies",
    "pickup_datetime": datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
    "dropoff_datetime": datetime(2024, 1, 15, 10, 30, tzinfo=UTC),
    "passenger_count": 2,
    "pickup_zone": "Times Sq",
    "dropoff_zone": "JFK",
    "pickup_borough": "Manhattan",
    "dropoff_borough": "Queens",
    "fare_amount": 10.0,
    "extra": 0.0,
    "mta_tax": 0.5,
    "tip_amount": 2.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.3,
    "congestion_surcharge": 2.5,
    "airport_fee": 0.0,
    "total_amount": 15.3,
    "payment_type": "Credit card",
    "rate_code": "Standard",
    "store_and_fwd_flag": "No",
    "batch_id": "b1",
    "source_file": "test.parquet",
}


# Existing behaviour (money / passenger_count / datetime) must not regress.
def test_none_money_fields_default_to_zero():
    row = dict(_BASE_ROW, fare_amount=None, tip_amount=None)
    coerced = _coerce_row(row)
    assert coerced["fare_amount"] == 0.0
    assert coerced["tip_amount"] == 0.0


def test_none_passenger_count_defaults_to_zero():
    row = dict(_BASE_ROW, passenger_count=None)
    coerced = _coerce_row(row)
    assert coerced["passenger_count"] == 0


def test_iso_string_datetime_is_parsed():
    row = dict(_BASE_ROW, pickup_datetime="2024-01-15T10:00:00+00:00")
    coerced = _coerce_row(row)
    assert isinstance(coerced["pickup_datetime"], datetime)
    assert coerced["pickup_datetime"].tzinfo is not None


def test_naive_datetime_gets_utc_attached():
    row = dict(_BASE_ROW, pickup_datetime=datetime(2024, 1, 15, 10, 0))
    coerced = _coerce_row(row)
    assert coerced["pickup_datetime"].tzinfo == UTC
