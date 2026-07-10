from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from etl.domain.trip.events import ProcessingStage
from etl.domain.trip.models import Trip, Zone
from etl.domain.trip.services import TripDomainService

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


def _make_zone_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_by_id.return_value = Zone(
        location_id=161,
        borough="Manhattan",
        zone="Midtown Center",
        service_zone="Yellow Zone",
    )
    return repo


def _valid_raw_row(**overrides: object) -> dict:
    """
    Raw row as yielded by ParquetReader (post column rename, pre normalisation).
    Keys match what TripNormalizer.normalize() expects.
    """
    row = {
        "vendor_id": 1,
        "pickup_datetime": _PICKUP,
        "dropoff_datetime": _DROPOFF,
        "passenger_count": 2.0,
        "trip_distance": 3.5,
        "rate_code_id": 1.0,
        "store_and_fwd_flag": "N",
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
        "payment_type": 1,
        "fare_amount": 12.5,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
        "total_amount": 19.8,
    }
    row.update(overrides)
    return row


def _make_service() -> TripDomainService:
    return TripDomainService(zone_repository=_make_zone_repo())


# Empty batch
def test_empty_batch_returns_empty_lists():
    svc = _make_service()
    valid, invalid = svc.process_batch([], "batch-1", "test.parquet")
    assert valid == []
    assert invalid == []


# Valid rows
def test_valid_row_produces_one_trip():
    svc = _make_service()
    valid, invalid = svc.process_batch([_valid_raw_row()], "b1", "test.parquet")
    assert len(valid) == 1
    assert len(invalid) == 0


def test_valid_row_produces_trip_instance():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row()], "b1", "test.parquet")
    assert isinstance(valid[0], Trip)


def test_batch_id_propagated_to_trip():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row()], "my-batch-id", "test.parquet")
    assert valid[0].batch_id == "my-batch-id"


def test_source_file_propagated_to_trip():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row()], "b1", "yellow_tripdata_2024-01.parquet")
    assert valid[0].source_file == "yellow_tripdata_2024-01.parquet"


def test_vendor_id_normalised_to_string():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row(vendor_id=1)], "b1", "test.parquet")
    assert valid[0].vendor_id == "Creative Mobile Technologies"


def test_payment_type_normalised_to_string():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row(payment_type=1)], "b1", "test.parquet")
    assert valid[0].payment.payment_type == "Credit card"


def test_rate_code_normalised_to_string():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row(rate_code_id=2.0)], "b1", "test.parquet")
    assert valid[0].rate_code == "JFK"


def test_null_passenger_count_normalised_to_one():
    svc = _make_service()
    valid, _ = svc.process_batch([_valid_raw_row(passenger_count=None)], "b1", "test.parquet")
    assert len(valid) == 1
    assert valid[0].passenger_count == 1


def test_zone_enrichment_applied():
    repo = _make_zone_repo()
    repo.get_by_id.return_value = Zone(
        location_id=161,
        borough="Manhattan",
        zone="Midtown Center",
        service_zone="Yellow Zone",
    )
    svc = TripDomainService(zone_repository=repo)
    valid, _ = svc.process_batch([_valid_raw_row()], "b1", "test.parquet")
    assert valid[0].pickup_zone == "Midtown Center"
    assert valid[0].pickup_borough == "Manhattan"


def test_trip_id_is_deterministic_for_same_row():
    svc = _make_service()
    row = _valid_raw_row()
    valid_a, _ = svc.process_batch([row], "b1", "test.parquet")
    valid_b, _ = svc.process_batch([row], "b2", "test.parquet")
    assert valid_a[0].trip_id == valid_b[0].trip_id


def test_multiple_valid_rows():
    svc = _make_service()
    rows = [
        _valid_raw_row(
            pickup_datetime=datetime(2024, 1, 15, h, 0, 0, tzinfo=UTC),
            dropoff_datetime=datetime(2024, 1, 15, h, 30, 0, tzinfo=UTC),
        )
        for h in range(10, 15)
    ]
    valid, invalid = svc.process_batch(rows, "b1", "test.parquet")
    assert len(valid) == 5
    assert len(invalid) == 0


# Invalid rows
def test_zero_duration_produces_invalid_event():
    svc = _make_service()
    row = _valid_raw_row(dropoff_datetime=_PICKUP)  # dropoff == pickup
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert len(invalid) == 1
    assert invalid[0].error_type == "InvalidTripDurationError"


def test_invalid_passenger_count_produces_invalid_event():
    svc = _make_service()
    row = _valid_raw_row(passenger_count=10.0)
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert len(invalid) == 1
    assert invalid[0].error_type == "InvalidPassengerCountError"


def test_negative_fare_produces_invalid_event():
    svc = _make_service()
    row = _valid_raw_row(fare_amount=-5.0)
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert len(invalid) == 1
    assert invalid[0].error_type == "NegativeMoneyError"


def test_pre_tlc_pickup_date_produces_invalid_event():
    svc = _make_service()
    before_tlc = datetime(2008, 6, 15, 10, 0, 0, tzinfo=UTC)
    row = _valid_raw_row(
        pickup_datetime=before_tlc,
        dropoff_datetime=datetime(2008, 6, 15, 10, 30, 0, tzinfo=UTC),
    )
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert len(invalid) == 1
    assert invalid[0].error_type == "InvalidPickupDatetimeError"


def test_invalid_event_carries_original_record():
    svc = _make_service()
    row = _valid_raw_row(passenger_count=10.0)
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert invalid[0].original_record == row


def test_invalid_event_carries_batch_id():
    svc = _make_service()
    row = _valid_raw_row(passenger_count=10.0)
    _, invalid = svc.process_batch([row], "b1-specific", "test.parquet")
    assert invalid[0].batch_id == "b1-specific"


def test_invalid_event_carries_source_file():
    svc = _make_service()
    row = _valid_raw_row(passenger_count=10.0)
    _, invalid = svc.process_batch([row], "b1", "source.parquet")
    assert invalid[0].source_file == "source.parquet"


def test_invalid_event_stage_is_validation_for_business_rule():
    svc = _make_service()
    row = _valid_raw_row(passenger_count=10.0)
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert invalid[0].stage == ProcessingStage.VALIDATION


def test_parse_error_stage_is_parsing():
    svc = _make_service()
    # Missing pickup_datetime causes TripParseError
    row = _valid_raw_row()
    row.pop("pickup_datetime")
    _, invalid = svc.process_batch([row], "b1", "test.parquet")
    assert len(invalid) == 1
    assert invalid[0].stage == ProcessingStage.PARSING


# Mixed batch
def test_mixed_batch_splits_correctly():
    svc = _make_service()
    rows = [
        _valid_raw_row(),
        _valid_raw_row(passenger_count=10.0),  # invalid
        _valid_raw_row(fare_amount=-5.0),  # invalid
        _valid_raw_row(
            pickup_datetime=datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC),
            dropoff_datetime=datetime(2024, 1, 15, 11, 30, 0, tzinfo=UTC),
        ),
    ]
    valid, invalid = svc.process_batch(rows, "b1", "test.parquet")
    assert len(valid) == 2
    assert len(invalid) == 2


def test_one_bad_row_does_not_abort_batch():
    svc = _make_service()
    rows = [
        _valid_raw_row(passenger_count=10.0),  # bad
        _valid_raw_row(  # good
            pickup_datetime=datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC),
            dropoff_datetime=datetime(2024, 1, 15, 11, 30, 0, tzinfo=UTC),
        ),
    ]
    valid, invalid = svc.process_batch(rows, "b1", "test.parquet")
    assert len(valid) == 1
    assert len(invalid) == 1


# Within-batch deduplication
def test_duplicate_row_in_batch_deduplicated():
    svc = _make_service()
    row = _valid_raw_row()
    valid, invalid = svc.process_batch([row, row], "b1", "test.parquet")
    assert len(valid) == 1
    assert len(invalid) == 1


def test_duplicate_event_stage_is_deduplication():
    svc = _make_service()
    row = _valid_raw_row()
    _, invalid = svc.process_batch([row, row], "b1", "test.parquet")
    assert invalid[0].stage == ProcessingStage.DEDUPLICATION


def test_unique_rows_not_deduplicated():
    svc = _make_service()
    rows = [
        _valid_raw_row(
            pickup_datetime=datetime(2024, 1, 15, h, 0, 0, tzinfo=UTC),
            dropoff_datetime=datetime(2024, 1, 15, h, 30, 0, tzinfo=UTC),
        )
        for h in range(10, 13)
    ]
    valid, invalid = svc.process_batch(rows, "b1", "test.parquet")
    assert len(valid) == 3
    assert len(invalid) == 0


def test_triplicate_row_keeps_one_rejects_two():
    svc = _make_service()
    row = _valid_raw_row()
    valid, invalid = svc.process_batch([row, row, row], "b1", "test.parquet")
    assert len(valid) == 1
    assert len(invalid) == 2


# Unexpected exception handling
def test_unexpected_exception_wrapped_as_invalid_event():
    repo = MagicMock()
    repo.get_by_id.side_effect = Exception("unexpected zone failure")
    svc = TripDomainService(zone_repository=repo)

    valid, invalid = svc.process_batch([_valid_raw_row()], "b1", "test.parquet")

    assert len(valid) == 0
    assert len(invalid) == 1


def test_unexpected_exception_does_not_propagate():
    repo = MagicMock()
    repo.get_by_id.side_effect = RuntimeError("catastrophic failure")
    svc = TripDomainService(zone_repository=repo)

    valid, invalid = svc.process_batch(
        [
            _valid_raw_row(),
            _valid_raw_row(
                pickup_datetime=datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC),
                dropoff_datetime=datetime(2024, 1, 15, 11, 30, 0, tzinfo=UTC),
            ),
        ],
        "b1",
        "test.parquet",
    )
    # Pipeline must not crash -- both rows wrapped as invalid
    assert len(valid) == 0
    assert len(invalid) == 2
