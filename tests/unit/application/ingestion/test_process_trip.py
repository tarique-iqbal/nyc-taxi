from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from etl.application.ingestion.process_trip import (
    ProcessTripCommand,
    ProcessTripResult,
    ProcessTripUseCase,
)
from etl.domain.trip.events import InvalidTripDetected, ProcessingStage
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

_RAW_ROW = {
    "vendor_id": "Creative Mobile Technologies",
    "pickup_datetime": _PICKUP,
    "dropoff_datetime": _DROPOFF,
    "passenger_count": 2,
    "trip_distance": 3.5,
    "rate_code": "Standard",
    "store_and_fwd_flag": "No",
    "pickup_location_id": 161,
    "dropoff_location_id": 236,
    "payment_type": "Credit card",
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


def _make_trip(trip_id: str = "abc123") -> Trip:
    return Trip(
        trip_id=trip_id,
        vendor_id="Creative Mobile Technologies",
        pickup_datetime=_PICKUP,
        dropoff_datetime=_DROPOFF,
        passenger_count=2,
        distance=Distance.of(3.5),
        duration=Duration.between(_PICKUP, _DROPOFF),
        pickup_location_id=161,
        dropoff_location_id=236,
        payment=Payment(
            payment_type="Credit card",
            fare_amount=Money.of(12.5, "fare_amount"),
            extra=Money.of(1.0, "extra"),
            mta_tax=Money.of(0.5, "mta_tax"),
            tip_amount=Money.of(3.0, "tip_amount"),
            tolls_amount=Money.zero(),
            improvement_surcharge=Money.of(0.3, "improvement_surcharge"),
            congestion_surcharge=Money.of(2.5, "congestion_surcharge"),
            airport_fee=Money.zero(),
            total_amount=Money.of(19.8, "total_amount"),
        ),
        rate_code="Standard",
        store_and_fwd_flag="No",
        batch_id="batch-1",
        source_file="test.parquet",
    )


def _make_invalid_event(trip_id: str | None = None) -> InvalidTripDetected:
    return InvalidTripDetected(
        stage=ProcessingStage.VALIDATION,
        error_message="Duration 0s is invalid",
        error_type="InvalidTripDurationError",
        original_record=_RAW_ROW,
        batch_id="batch-1",
        source_file="test.parquet",
        trip_id=trip_id,
    )


def _make_use_case(
    valid_trips: list[Trip] | None = None,
    invalid_events: list[InvalidTripDetected] | None = None,
) -> tuple[ProcessTripUseCase, MagicMock]:
    domain_service = MagicMock()
    domain_service.process_batch.return_value = (
        valid_trips or [],
        invalid_events or [],
    )
    return ProcessTripUseCase(domain_service=domain_service), domain_service


# ProcessTripCommand
def test_command_auto_generates_batch_id():
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")
    assert cmd.batch_id != ""
    assert len(cmd.batch_id) == 36  # UUID4


def test_command_two_instances_have_different_batch_ids():
    cmd_a = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")
    cmd_b = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")
    assert cmd_a.batch_id != cmd_b.batch_id


def test_command_explicit_batch_id_preserved():
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet", batch_id="fixed-id")
    assert cmd.batch_id == "fixed-id"


def test_command_is_immutable():
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")
    with pytest.raises((AttributeError, TypeError)):
        cmd.batch_id = "new-id"  # type: ignore[misc]


# ProcessTripResult
def test_result_valid_factory_sets_success_true():
    trip = _make_trip()
    result = ProcessTripResult.valid(trip)
    assert result.success is True
    assert result.trip is trip
    assert result.invalid_event is None


def test_result_invalid_factory_sets_success_false():
    event = _make_invalid_event()
    result = ProcessTripResult.invalid(event)
    assert result.success is False
    assert result.invalid_event is event
    assert result.trip is None


def test_result_valid_and_invalid_are_mutually_exclusive():
    trip = _make_trip()
    result = ProcessTripResult.valid(trip)
    assert result.invalid_event is None

    event = _make_invalid_event()
    result2 = ProcessTripResult.invalid(event)
    assert result2.trip is None


# ProcessTripUseCase.handle: valid
def test_handle_returns_valid_result_for_valid_row():
    trip = _make_trip()
    use_case, domain_service = _make_use_case(valid_trips=[trip])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.success is True
    assert result.trip is trip


def test_handle_delegates_to_domain_service():
    use_case, domain_service = _make_use_case(valid_trips=[_make_trip()])
    cmd = ProcessTripCommand(
        raw_row=_RAW_ROW,
        source_file="test.parquet",
        batch_id="my-batch",
    )

    use_case.handle(cmd)

    domain_service.process_batch.assert_called_once()
    call_kwargs = domain_service.process_batch.call_args
    assert call_kwargs.kwargs.get("batch_id") == "my-batch" or "my-batch" in str(call_kwargs)


def test_handle_passes_single_row_list_to_domain():
    use_case, domain_service = _make_use_case(valid_trips=[_make_trip()])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    use_case.handle(cmd)

    raw_rows = (
        domain_service.process_batch.call_args.kwargs.get("raw_rows")
        or domain_service.process_batch.call_args.args[0]
    )
    assert raw_rows == [_RAW_ROW]


def test_handle_passes_source_file_to_domain():
    use_case, domain_service = _make_use_case(valid_trips=[_make_trip()])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="yellow_tripdata_2024-01.parquet")

    use_case.handle(cmd)

    call_args = domain_service.process_batch.call_args
    source = call_args.kwargs.get("source_file") or call_args.args[2]
    assert source == "yellow_tripdata_2024-01.parquet"


# ProcessTripUseCase.handle: invalid
def test_handle_returns_invalid_result_for_invalid_row():
    event = _make_invalid_event("bad-trip")
    use_case, _ = _make_use_case(invalid_events=[event])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.success is False
    assert result.invalid_event is event


def test_handle_returns_invalid_when_domain_returns_no_trips():
    use_case, _ = _make_use_case(valid_trips=[], invalid_events=[])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.success is False
    assert result.invalid_event is not None
    assert result.invalid_event.error_type == "DomainError"


def test_handle_never_raises_on_domain_error():
    domain_service = MagicMock()
    domain_service.process_batch.return_value = ([], [_make_invalid_event()])
    use_case = ProcessTripUseCase(domain_service=domain_service)
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    result = use_case.handle(cmd)  # must not raise
    assert result.success is False


def test_handle_invalid_event_preserves_error_type():
    event = InvalidTripDetected(
        stage=ProcessingStage.VALIDATION,
        error_message="Passenger count 10 is invalid",
        error_type="InvalidPassengerCountError",
        original_record=_RAW_ROW,
        batch_id="b1",
        source_file="test.parquet",
        trip_id="trip-1",
    )
    use_case, _ = _make_use_case(invalid_events=[event])
    cmd = ProcessTripCommand(raw_row=_RAW_ROW, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.invalid_event.error_type == "InvalidPassengerCountError"
    assert result.invalid_event.stage == ProcessingStage.VALIDATION
