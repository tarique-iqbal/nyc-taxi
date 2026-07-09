from __future__ import annotations

from datetime import UTC, datetime

from etl.application.services.validation_service import ValidationService

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


def _valid_row(**overrides: object) -> dict:
    base = {
        "pickup_datetime": _PICKUP,
        "dropoff_datetime": _DROPOFF,
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
    }
    base.update(overrides)
    return base


# validate_raw: valid
def test_valid_row_passes():
    svc = ValidationService()
    ok, error = svc.validate_raw(_valid_row())
    assert ok is True
    assert error is None


def test_optional_fields_absent_passes():
    svc = ValidationService()
    row = {
        "pickup_datetime": _PICKUP,
        "dropoff_datetime": _DROPOFF,
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
    }
    ok, error = svc.validate_raw(row)
    assert ok is True


def test_extra_columns_ignored():
    svc = ValidationService()
    row = _valid_row(unknown_column="xyz", another_extra=999)
    ok, error = svc.validate_raw(row)
    assert ok is True
    assert error is None


def test_optional_fields_present_passes():
    svc = ValidationService()
    row = _valid_row(
        vendor_id=1,
        passenger_count=2.0,
        trip_distance=3.5,
        rate_code_id=1,
        store_and_fwd_flag="N",
        payment_type=1,
        fare_amount=12.5,
        extra=1.0,
        mta_tax=0.5,
        tip_amount=3.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        congestion_surcharge=2.5,
        airport_fee=0.0,
        total_amount=19.8,
    )
    ok, error = svc.validate_raw(row)
    assert ok is True


# validate_raw: missing required fields
def test_missing_pickup_datetime_fails():
    svc = ValidationService()
    row = {
        "dropoff_datetime": _DROPOFF,
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
    }
    ok, error = svc.validate_raw(row)
    assert ok is False
    assert error is not None
    assert "pickup_datetime" in error


def test_missing_dropoff_datetime_fails():
    svc = ValidationService()
    row = {
        "pickup_datetime": _PICKUP,
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
    }
    ok, error = svc.validate_raw(row)
    assert ok is False
    assert "dropoff_datetime" in error


def test_missing_pickup_location_id_fails():
    svc = ValidationService()
    row = {
        "pickup_datetime": _PICKUP,
        "dropoff_datetime": _DROPOFF,
        "dropoff_location_id": 236,
    }
    ok, error = svc.validate_raw(row)
    assert ok is False
    assert "pickup_location_id" in error


def test_missing_dropoff_location_id_fails():
    svc = ValidationService()
    row = {
        "pickup_datetime": _PICKUP,
        "dropoff_datetime": _DROPOFF,
        "pickup_location_id": 161,
    }
    ok, error = svc.validate_raw(row)
    assert ok is False
    assert "dropoff_location_id" in error


# validate_raw: cross-field validator
def test_dropoff_equal_to_pickup_fails():
    svc = ValidationService()
    ok, error = svc.validate_raw(_valid_row(dropoff_datetime=_PICKUP))
    assert ok is False
    assert error is not None


def test_dropoff_before_pickup_fails():
    svc = ValidationService()
    before = datetime(2024, 1, 15, 9, 0, 0, tzinfo=UTC)
    ok, error = svc.validate_raw(_valid_row(dropoff_datetime=before))
    assert ok is False
    assert error is not None


def test_dropoff_one_second_after_pickup_passes():
    svc = ValidationService()
    from datetime import timedelta

    one_sec_later = _PICKUP + timedelta(seconds=1)
    ok, error = svc.validate_raw(_valid_row(dropoff_datetime=one_sec_later))
    assert ok is True


# validate_raw: location_id validator
def test_zero_pickup_location_id_fails():
    svc = ValidationService()
    ok, error = svc.validate_raw(_valid_row(pickup_location_id=0))
    assert ok is False
    assert "pickup_location_id" in error


def test_negative_dropoff_location_id_fails():
    svc = ValidationService()
    ok, error = svc.validate_raw(_valid_row(dropoff_location_id=-1))
    assert ok is False
    assert "dropoff_location_id" in error


def test_positive_location_ids_pass():
    svc = ValidationService()
    ok, _ = svc.validate_raw(_valid_row(pickup_location_id=1, dropoff_location_id=265))
    assert ok is True


# validate_batch
def test_validate_batch_all_valid():
    svc = ValidationService()
    rows = [_valid_row(), _valid_row(), _valid_row()]
    valid, invalid = svc.validate_batch(rows)
    assert len(valid) == 3
    assert len(invalid) == 0


def test_validate_batch_all_invalid():
    svc = ValidationService()
    bad_row = {"vendor_id": 1}  # missing all required fields
    rows = [bad_row, bad_row, bad_row]
    valid, invalid = svc.validate_batch(rows)
    assert len(valid) == 0
    assert len(invalid) == 3


def test_validate_batch_mixed():
    svc = ValidationService()
    rows = [
        _valid_row(),
        {"vendor_id": 1},  # invalid: missing required fields
        _valid_row(),
        _valid_row(dropoff_datetime=_PICKUP),  # invalid: dropoff == pickup
    ]
    valid, invalid = svc.validate_batch(rows)
    assert len(valid) == 2
    assert len(invalid) == 2


def test_validate_batch_invalid_tuple_has_error_message():
    svc = ValidationService()
    bad_row = {"vendor_id": 1}
    _, invalid = svc.validate_batch([bad_row])
    row, error_msg = invalid[0]
    assert row is bad_row
    assert isinstance(error_msg, str)
    assert len(error_msg) > 0


def test_validate_batch_preserves_order_of_valid_rows():
    svc = ValidationService()
    rows = [_valid_row(pickup_location_id=i + 1, dropoff_location_id=i + 2) for i in range(5)]
    valid, _ = svc.validate_batch(rows)
    assert [r["pickup_location_id"] for r in valid] == [1, 2, 3, 4, 5]


def test_validate_batch_empty_input():
    svc = ValidationService()
    valid, invalid = svc.validate_batch([])
    assert valid == []
    assert invalid == []
