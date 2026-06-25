from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from etl.application.services.ingestion_service import IngestionService, IngestionSummary
from etl.domain.trip.events import InvalidTripDetected, ProcessingStage
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip
from etl.runtime.shutdown import ShutdownHandler

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


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
        batch_id="b1",
        source_file="test.parquet",
    )


def _make_invalid_event(trip_id: str = "bad") -> InvalidTripDetected:
    return InvalidTripDetected(
        stage=ProcessingStage.VALIDATION,
        error_message="Duration 0s is invalid",
        error_type="InvalidTripDurationError",
        original_record={"pickup_location_id": 161},
        batch_id="b1",
        source_file="test.parquet",
        trip_id=trip_id,
    )


def _make_reader(batches: list[list[dict]]) -> MagicMock:
    reader = MagicMock()
    reader.source_file = "test.parquet"
    reader.iter_batches.return_value = iter(batches)
    return reader


def _make_domain_service(
    valid_trips: list[Trip] | None = None,
    invalid_events: list[InvalidTripDetected] | None = None,
) -> MagicMock:
    svc = MagicMock()
    svc.process_batch.return_value = (valid_trips or [], invalid_events or [])
    return svc


def _make_service(
    batches: list[list[dict]] | None = None,
    valid_trips: list[Trip] | None = None,
    invalid_events: list[InvalidTripDetected] | None = None,
    shutdown_requested: bool = False,
    schema_valid: list[dict] | None = None,
    schema_invalid: list[tuple] | None = None,
) -> tuple[IngestionService, MagicMock, MagicMock, MagicMock]:
    reader = _make_reader(batches or [[{"row": 1}]])
    domain_service = _make_domain_service(valid_trips, invalid_events)
    publisher = MagicMock()
    dl_service = MagicMock()

    handler = MagicMock(spec=ShutdownHandler)
    handler.is_shutdown_requested = shutdown_requested

    validation_service = MagicMock()
    validation_service.validate_batch.return_value = (
        schema_valid if schema_valid is not None else [{"row": 1}],
        schema_invalid or [],
    )

    service = IngestionService(
        reader=reader,
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=validation_service,
    )
    return service, publisher, dl_service, domain_service


# IngestionSummary
def test_summary_reject_rate_zero_when_no_rows():
    s = IngestionSummary(source_file="test.parquet")
    assert s.reject_rate == 0.0


def test_summary_reject_rate_includes_schema_rejected():
    s = IngestionSummary(source_file="test.parquet")
    s.total_rows = 10
    s.total_invalid = 2
    s.total_schema_rejected = 3
    assert abs(s.reject_rate - 0.5) < 1e-9


def test_summary_reject_rate_all_rejected():
    s = IngestionSummary(source_file="test.parquet")
    s.total_rows = 5
    s.total_invalid = 5
    assert s.reject_rate == 1.0


def test_summary_complete_sets_finished_at():
    s = IngestionSummary(source_file="test.parquet")
    assert s.finished_at is None
    s.complete()
    assert s.finished_at is not None


def test_summary_as_dict_keys():
    s = IngestionSummary(source_file="test.parquet")
    s.complete()
    d = s.as_dict()
    assert "source_file" in d
    assert "total_batches" in d
    assert "total_valid" in d
    assert "total_invalid" in d
    assert "total_schema_rejected" in d
    assert "reject_rate" in d
    assert "finished_at" in d


# IngestionService.run: basic flow
def test_run_returns_summary():
    service, _, _, _ = _make_service()
    summary = service.run()
    assert isinstance(summary, IngestionSummary)


def test_run_sets_finished_at_on_summary():
    service, _, _, _ = _make_service()
    summary = service.run()
    assert summary.finished_at is not None


def test_run_calls_publisher_flush():
    service, publisher, _, _ = _make_service()
    service.run()
    publisher.flush.assert_called_once()


def test_run_counts_batches():
    service, _, _, _ = _make_service(batches=[[{"r": 1}], [{"r": 2}], [{"r": 3}]])
    summary = service.run()
    assert summary.total_batches == 3


def test_run_counts_total_rows():
    service, _, _, _ = _make_service(
        batches=[[{"r": 1}, {"r": 2}], [{"r": 3}]],
        schema_valid=[{"r": 1}, {"r": 2}],
    )
    summary = service.run()
    assert summary.total_rows == 3


# IngestionService.run: valid trips
def test_run_publishes_valid_trips():
    trips = [_make_trip("id-1"), _make_trip("id-2")]
    service, publisher, _, _ = _make_service(valid_trips=trips)
    service.run()
    publisher.publish_batch.assert_called_once()


def test_run_counts_valid_trips_in_summary():
    trips = [_make_trip("id-1"), _make_trip("id-2")]
    service, _, _, _ = _make_service(valid_trips=trips)
    summary = service.run()
    assert summary.total_valid == 2


def test_run_does_not_publish_when_no_valid_trips():
    service, publisher, _, _ = _make_service(valid_trips=[], invalid_events=[])
    service.run()
    publisher.publish_batch.assert_not_called()


# IngestionService.run: invalid records
def test_run_dead_letters_invalid_domain_records():
    events = [_make_invalid_event("bad-1"), _make_invalid_event("bad-2")]
    service, _, dl_service, _ = _make_service(invalid_events=events)
    service.run()
    dl_service.send_batch.assert_called_once()


def test_run_counts_invalid_in_summary():
    events = [_make_invalid_event()]
    service, _, _, _ = _make_service(invalid_events=events)
    summary = service.run()
    assert summary.total_invalid == 1


def test_run_does_not_dead_letter_when_no_invalid():
    trips = [_make_trip()]
    service, _, dl_service, _ = _make_service(valid_trips=trips, invalid_events=[])
    service.run()
    dl_service.send_batch.assert_not_called()


# IngestionService.run: schema validation
def test_run_dead_letters_schema_rejected_rows():
    bad_row = {"vendor_id": 1}
    service, _, dl_service, _ = _make_service(
        schema_valid=[],
        schema_invalid=[(bad_row, "missing pickup_datetime")],
    )
    service.run()
    dl_service.send.assert_called_once()


def test_run_counts_schema_rejected_in_summary():
    bad_row = {"vendor_id": 1}
    service, _, _, _ = _make_service(
        schema_valid=[],
        schema_invalid=[(bad_row, "missing required field")],
    )
    summary = service.run()
    assert summary.total_schema_rejected == 1


def test_run_schema_rejected_not_passed_to_domain_service():
    bad_row = {"vendor_id": 1}
    service, _, _, domain_service = _make_service(
        schema_valid=[],
        schema_invalid=[(bad_row, "error")],
    )
    service.run()
    domain_service.process_batch.assert_called_once()
    called_rows = domain_service.process_batch.call_args.args[0]
    assert called_rows == []


# IngestionService.run: shutdown
def test_run_stops_when_shutdown_requested_before_batch():
    reader = _make_reader([[{"r": 1}], [{"r": 2}], [{"r": 3}]])
    domain_service = _make_domain_service()
    publisher = MagicMock()
    dl_service = MagicMock()
    validation_service = MagicMock()
    validation_service.validate_batch.return_value = ([], [])

    handler = MagicMock(spec=ShutdownHandler)
    handler.is_shutdown_requested = True  # shutdown already set

    service = IngestionService(
        reader=reader,
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=validation_service,
    )
    summary = service.run()

    assert summary.total_batches == 0
    domain_service.process_batch.assert_not_called()


def test_run_flush_called_even_after_shutdown():
    handler = MagicMock(spec=ShutdownHandler)
    handler.is_shutdown_requested = True

    reader = _make_reader([[]])
    publisher = MagicMock()

    service = IngestionService(
        reader=reader,
        domain_service=_make_domain_service(),
        publisher=publisher,
        dead_letter_service=MagicMock(),
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=MagicMock(
            validate_batch=MagicMock(return_value=([], []))
        ),
    )
    service.run()
    publisher.flush.assert_called_once()


# IngestionService.run: multiple batches accumulate correctly
def test_run_accumulates_counts_across_batches():
    reader = _make_reader([[{"r": 1}, {"r": 2}], [{"r": 3}, {"r": 4}, {"r": 5}]])
    trips_b1 = [_make_trip("t1")]
    trips_b2 = [_make_trip("t2"), _make_trip("t3")]
    invalid_b1 = [_make_invalid_event("bad1")]

    domain_service = MagicMock()
    domain_service.process_batch.side_effect = [
        (trips_b1, invalid_b1),
        (trips_b2, []),
    ]

    validation_service = MagicMock()
    validation_service.validate_batch.side_effect = [
        ([{"r": 1}, {"r": 2}], []),
        ([{"r": 3}, {"r": 4}, {"r": 5}], []),
    ]

    handler = MagicMock(spec=ShutdownHandler)
    handler.is_shutdown_requested = False

    service = IngestionService(
        reader=reader,
        domain_service=domain_service,
        publisher=MagicMock(),
        dead_letter_service=MagicMock(),
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=validation_service,
    )
    summary = service.run()

    assert summary.total_batches == 2
    assert summary.total_rows == 5
    assert summary.total_valid == 3   # 1 + 2
    assert summary.total_invalid == 1
