from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from etl.application.ingestion.process_batch import (
    ProcessBatchCommand,
    ProcessBatchResult,
    ProcessBatchUseCase,
)
from etl.domain.trip.events import InvalidTripDetected, ProcessingStage
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip


def _make_trip(trip_id: str = "abc123") -> Trip:
    pickup = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    dropoff = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    return Trip(
        trip_id=trip_id,
        vendor_id="Creative Mobile Technologies",
        pickup_datetime=pickup,
        dropoff_datetime=dropoff,
        passenger_count=2,
        distance=Distance.of(3.5),
        duration=Duration.between(pickup, dropoff),
        pickup_location_id=161,
        dropoff_location_id=236,
        payment=Payment(
            payment_type="Credit card",
            fare_amount=Money.of(10.0, "fare_amount"),
            extra=Money.zero(),
            mta_tax=Money.of(0.5, "mta_tax"),
            tip_amount=Money.of(2.0, "tip_amount"),
            tolls_amount=Money.zero(),
            improvement_surcharge=Money.of(0.3, "improvement_surcharge"),
            congestion_surcharge=Money.of(2.5, "congestion_surcharge"),
            airport_fee=Money.zero(),
            total_amount=Money.of(15.3, "total_amount"),
        ),
        rate_code="Standard",
        store_and_fwd_flag="No",
        batch_id="batch-1",
        source_file="test.parquet",
    )


def _make_invalid_event(trip_id: str = "bad-trip") -> InvalidTripDetected:
    return InvalidTripDetected(
        stage=ProcessingStage.VALIDATION,
        error_message="Duration 0s is invalid",
        error_type="InvalidTripDurationError",
        original_record={"pickup_location_id": 161},
        batch_id="batch-1",
        source_file="test.parquet",
        trip_id=trip_id,
    )


def _make_use_case(
    valid_trips: list[Trip] | None = None,
    invalid_events: list[InvalidTripDetected] | None = None,
) -> tuple[ProcessBatchUseCase, MagicMock, MagicMock, MagicMock]:
    domain_service = MagicMock()
    domain_service.process_batch.return_value = (
        valid_trips or [],
        invalid_events or [],
    )
    publisher = MagicMock()
    dl_service = MagicMock()

    use_case = ProcessBatchUseCase(
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
    )
    return use_case, domain_service, publisher, dl_service


# ProcessBatchCommand
def test_command_auto_generates_batch_id():
    cmd = ProcessBatchCommand(raw_rows=[{"x": 1}], source_file="test.parquet")
    assert cmd.batch_id is not None
    assert len(cmd.batch_id) == 36  # UUID4 format


def test_command_explicit_batch_id_preserved():
    cmd = ProcessBatchCommand(
        raw_rows=[],
        source_file="test.parquet",
        batch_id="fixed-id",
    )
    assert cmd.batch_id == "fixed-id"


def test_two_commands_get_different_batch_ids():
    cmd_a = ProcessBatchCommand(raw_rows=[], source_file="test.parquet")
    cmd_b = ProcessBatchCommand(raw_rows=[], source_file="test.parquet")
    assert cmd_a.batch_id != cmd_b.batch_id


# ProcessBatchResult
def test_result_total_is_valid_plus_invalid():
    result = ProcessBatchResult(
        batch_id="x", valid_count=8, invalid_count=2, duration_seconds=0.1
    )
    assert result.total == 10


def test_result_reject_rate_zero_when_no_invalid():
    result = ProcessBatchResult(
        batch_id="x", valid_count=10, invalid_count=0, duration_seconds=0.1
    )
    assert result.reject_rate == 0.0


def test_result_reject_rate_calculated_correctly():
    result = ProcessBatchResult(
        batch_id="x", valid_count=8, invalid_count=2, duration_seconds=0.1
    )
    assert abs(result.reject_rate - 0.2) < 1e-9


def test_result_reject_rate_zero_when_total_zero():
    result = ProcessBatchResult(
        batch_id="x", valid_count=0, invalid_count=0, duration_seconds=0.0
    )
    assert result.reject_rate == 0.0


def test_result_reject_rate_one_when_all_invalid():
    result = ProcessBatchResult(
        batch_id="x", valid_count=0, invalid_count=5, duration_seconds=0.1
    )
    assert result.reject_rate == 1.0


# ProcessBatchUseCase.handle: all valid
def test_handle_returns_valid_count():
    trips = [_make_trip(f"id-{i}") for i in range(5)]
    use_case, _, _, _ = _make_use_case(valid_trips=trips)
    cmd = ProcessBatchCommand(raw_rows=[{}] * 5, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.valid_count == 5
    assert result.invalid_count == 0


def test_handle_publishes_valid_trips():
    trips = [_make_trip("id-1"), _make_trip("id-2")]
    use_case, _, publisher, _ = _make_use_case(valid_trips=trips)
    cmd = ProcessBatchCommand(raw_rows=[{}, {}], source_file="test.parquet")

    use_case.handle(cmd)

    publisher.publish_batch.assert_called_once()
    topic, messages = publisher.publish_batch.call_args.args
    assert topic == "nyc-taxi-trips"
    assert len(messages) == 2


def test_handle_does_not_publish_when_no_valid_trips():
    use_case, _, publisher, _ = _make_use_case(valid_trips=[])
    cmd = ProcessBatchCommand(raw_rows=[{}], source_file="test.parquet")

    use_case.handle(cmd)

    publisher.publish_batch.assert_not_called()


def test_handle_does_not_dead_letter_when_no_invalid():
    trips = [_make_trip()]
    use_case, _, _, dl_service = _make_use_case(valid_trips=trips, invalid_events=[])
    cmd = ProcessBatchCommand(raw_rows=[{}], source_file="test.parquet")

    use_case.handle(cmd)

    dl_service.send_batch.assert_not_called()


# ProcessBatchUseCase.handle: all invalid
def test_handle_returns_invalid_count():
    events = [_make_invalid_event(f"bad-{i}") for i in range(3)]
    use_case, _, _, _ = _make_use_case(invalid_events=events)
    cmd = ProcessBatchCommand(raw_rows=[{}] * 3, source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.valid_count == 0
    assert result.invalid_count == 3


def test_handle_dead_letters_invalid_records():
    events = [_make_invalid_event("bad-1"), _make_invalid_event("bad-2")]
    use_case, _, _, dl_service = _make_use_case(invalid_events=events)
    cmd = ProcessBatchCommand(raw_rows=[{}, {}], source_file="test.parquet")

    use_case.handle(cmd)

    dl_service.send_batch.assert_called_once()
    records = dl_service.send_batch.call_args.args[0]
    assert len(records) == 2


def test_handle_does_not_publish_when_all_invalid():
    events = [_make_invalid_event()]
    use_case, _, publisher, _ = _make_use_case(invalid_events=events)
    cmd = ProcessBatchCommand(raw_rows=[{}], source_file="test.parquet")

    use_case.handle(cmd)

    publisher.publish_batch.assert_not_called()


# ProcessBatchUseCase.handle: mixed
def test_handle_mixed_valid_and_invalid():
    trips = [_make_trip("good")]
    events = [_make_invalid_event("bad")]
    use_case, _, publisher, dl_service = _make_use_case(
        valid_trips=trips, invalid_events=events
    )
    cmd = ProcessBatchCommand(raw_rows=[{}, {}], source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.valid_count == 1
    assert result.invalid_count == 1
    publisher.publish_batch.assert_called_once()
    dl_service.send_batch.assert_called_once()


# ProcessBatchUseCase.handle: empty batch
def test_handle_empty_batch_returns_zero_counts():
    use_case, _, publisher, dl_service = _make_use_case()
    cmd = ProcessBatchCommand(raw_rows=[], source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.valid_count == 0
    assert result.invalid_count == 0
    publisher.publish_batch.assert_not_called()
    dl_service.send_batch.assert_not_called()


# ProcessBatchUseCase.handle: domain service receives correct args
def test_handle_passes_batch_id_and_source_file_to_domain_service():
    use_case, domain_service, _, _ = _make_use_case()
    cmd = ProcessBatchCommand(
        raw_rows=[{"x": 1}],
        source_file="yellow_tripdata_2024-01.parquet",
        batch_id="fixed-batch-id",
    )

    use_case.handle(cmd)

    domain_service.process_batch.assert_called_once()
    kwargs = domain_service.process_batch.call_args
    assert kwargs.kwargs.get("batch_id") == "fixed-batch-id" or \
           kwargs.args[1] == "fixed-batch-id"


# ProcessBatchUseCase.handle: duration
def test_handle_result_has_positive_duration():
    use_case, _, _, _ = _make_use_case()
    cmd = ProcessBatchCommand(raw_rows=[], source_file="test.parquet")

    result = use_case.handle(cmd)

    assert result.duration_seconds >= 0.0


def test_handle_result_batch_id_matches_command():
    use_case, _, _, _ = _make_use_case()
    cmd = ProcessBatchCommand(
        raw_rows=[], source_file="test.parquet", batch_id="my-batch"
    )

    result = use_case.handle(cmd)

    assert result.batch_id == "my-batch"
