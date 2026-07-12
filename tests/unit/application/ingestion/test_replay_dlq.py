from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from etl.application.ingestion.replay_dlq import (
    ReplayDlqCommand,
    ReplayDlqResult,
    ReplayDlqUseCase,
    ReplaySource,
)
from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
from etl.domain.trip.events import InvalidTripDetected, ProcessingStage
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip
from etl.utils.compression import write_jsonl_gz

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

_VALID_RAW = {
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

_INVALID_RAW = {**_VALID_RAW, "passenger_count": 10.0}  # fails validation


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
        batch_id="b1",
        source_file="test.parquet",
    )


def _make_invalid_event(trip_id: str = "bad") -> InvalidTripDetected:
    return InvalidTripDetected(
        stage=ProcessingStage.VALIDATION,
        error_message="Passenger count 10 is invalid",
        error_type="InvalidPassengerCountError",
        original_record=_INVALID_RAW,
        batch_id="b1",
        source_file="test.parquet",
        trip_id=trip_id,
    )


def _dl_record(
    raw: dict | None = None,
    batch_id: str = "batch-1",
    retry_count: int = 0,
    trip_id: str | None = None,
) -> DeadLetterRecord:
    return DeadLetterRecord(
        original_record=raw or _VALID_RAW,
        error_message="test error",
        error_type="InvalidTripDurationError",
        stage=DeadLetterStage.VALIDATION,
        batch_id=batch_id,
        source_file="test.parquet",
        trip_id=trip_id,
        retry_count=retry_count,
    )


def _write_records(path: Path, records: list[DeadLetterRecord]) -> None:
    write_jsonl_gz([r.as_dict() for r in records], path)


def _make_use_case(
    valid_trips: list | None = None,
    invalid_events: list | None = None,
) -> tuple[ReplayDlqUseCase, MagicMock, MagicMock, MagicMock]:
    domain_service = MagicMock()
    domain_service.process_batch.return_value = (
        valid_trips or [],
        invalid_events or [],
    )
    publisher = MagicMock()
    dl_service = MagicMock()
    use_case = ReplayDlqUseCase(
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
    )
    return use_case, domain_service, publisher, dl_service


# ReplayDlqCommand
def test_command_defaults_to_disk_source():
    cmd = ReplayDlqCommand()
    assert cmd.source == ReplaySource.DISK


def test_command_defaults_to_no_batch_id_filter():
    cmd = ReplayDlqCommand()
    assert cmd.batch_id is None


def test_command_default_rejected_dir():
    cmd = ReplayDlqCommand()
    assert "rejected" in str(cmd.rejected_dir)


def test_command_custom_batch_id_filter():
    cmd = ReplayDlqCommand(batch_id="abc-123")
    assert cmd.batch_id == "abc-123"


# ReplayDlqResult
def test_result_recovery_rate_zero_when_nothing_replayed():
    result = ReplayDlqResult()
    assert result.recovery_rate == 0.0


def test_result_recovery_rate_one_when_all_recovered():
    result = ReplayDlqResult(total_replayed=5, recovered=5, still_invalid=0)
    assert result.recovery_rate == 1.0


def test_result_recovery_rate_zero_when_none_recovered():
    result = ReplayDlqResult(total_replayed=5, recovered=0, still_invalid=5)
    assert result.recovery_rate == 0.0


def test_result_recovery_rate_partial():
    result = ReplayDlqResult(total_replayed=10, recovered=7, still_invalid=3)
    assert abs(result.recovery_rate - 0.7) < 1e-9


# ReplaySource.KAFKA raises NotImplementedError
def test_kafka_source_raises_not_implemented(tmp_path):
    use_case, _, _, _ = _make_use_case()
    cmd = ReplayDlqCommand(source=ReplaySource.KAFKA, rejected_dir=tmp_path)
    with pytest.raises(NotImplementedError):
        use_case.handle(cmd)


# handle: empty rejected directory
def test_handle_empty_dir_returns_zero_counts(tmp_path):
    use_case, _, _, _ = _make_use_case()
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)
    assert result.total_replayed == 0
    assert result.recovered == 0
    assert result.still_invalid == 0


def test_handle_empty_dir_does_not_publish(tmp_path):
    use_case, _, publisher, _ = _make_use_case()
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    use_case.handle(cmd)
    publisher.publish_batch.assert_not_called()


def test_handle_missing_dir_does_not_raise(tmp_path):
    use_case, _, _, _ = _make_use_case()
    missing = tmp_path / "does_not_exist"
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=missing)
    result = use_case.handle(cmd)
    assert result.total_replayed == 0


# handle: all records recovered
def test_handle_recovered_record_published_to_main_topic(tmp_path):
    trip = _make_trip()
    use_case, _, publisher, _ = _make_use_case(valid_trips=[trip])

    record = _dl_record(raw=_VALID_RAW, batch_id="b1")
    _write_records(tmp_path / "b1.jsonl.gz", [record])

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)

    assert result.recovered == 1
    assert result.still_invalid == 0
    publisher.publish_batch.assert_called_once()


def test_handle_recovered_count_matches_records(tmp_path):
    use_case, _, _, _ = _make_use_case(
        valid_trips=[_make_trip("t1"), _make_trip("t2"), _make_trip("t3")]
    )
    records = [_dl_record(batch_id="b1"), _dl_record(batch_id="b1"), _dl_record(batch_id="b1")]
    _write_records(tmp_path / "b1.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)

    assert result.recovered == 3
    assert result.total_replayed == 3


# handle: all records still invalid
def test_handle_still_invalid_sent_to_dlq(tmp_path):
    invalid_event = _make_invalid_event()
    use_case, _, _, dl_service = _make_use_case(valid_trips=[], invalid_events=[invalid_event])

    record = _dl_record(raw=_INVALID_RAW, batch_id="b1")
    _write_records(tmp_path / "b1.jsonl.gz", [record])

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)

    assert result.still_invalid == 1
    assert result.recovered == 0
    dl_service.send.assert_called_once()


def test_handle_still_invalid_increments_retry_count(tmp_path):
    invalid_event = _make_invalid_event()
    use_case, _, _, dl_service = _make_use_case(valid_trips=[], invalid_events=[invalid_event])

    record = _dl_record(raw=_INVALID_RAW, batch_id="b1", retry_count=2)
    _write_records(tmp_path / "b1.jsonl.gz", [record])

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    use_case.handle(cmd)

    sent_record = dl_service.send.call_args.args[0]
    assert sent_record.retry_count == 3


def test_handle_still_invalid_preserves_original_record(tmp_path):
    invalid_event = _make_invalid_event()
    use_case, _, _, dl_service = _make_use_case(
        valid_trips=[],
        invalid_events=[invalid_event],
    )

    record = _dl_record(raw=_INVALID_RAW, batch_id="b1")
    _write_records(tmp_path / "b1.jsonl.gz", [record])

    cmd = ReplayDlqCommand(
        source=ReplaySource.DISK,
        rejected_dir=tmp_path,
    )
    use_case.handle(cmd)

    sent_record = dl_service.send.call_args.args[0]

    expected_original = {
        **_INVALID_RAW,
        "pickup_datetime": _INVALID_RAW["pickup_datetime"].isoformat(),
        "dropoff_datetime": _INVALID_RAW["dropoff_datetime"].isoformat(),
    }

    assert sent_record.original_record == expected_original


# handle: mixed
def test_handle_mixed_recovery(tmp_path):
    domain_service = MagicMock()
    domain_service.process_batch.side_effect = [
        ([_make_trip("t1")], []),  # first record: recovered
        ([], [_make_invalid_event()]),  # second record: still invalid
    ]
    publisher = MagicMock()
    dl_service = MagicMock()
    use_case = ReplayDlqUseCase(
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
    )

    records = [
        _dl_record(raw=_VALID_RAW, batch_id="b1"),
        _dl_record(raw=_INVALID_RAW, batch_id="b1"),
    ]
    _write_records(tmp_path / "b1.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)

    assert result.total_replayed == 2
    assert result.recovered == 1
    assert result.still_invalid == 1
    assert abs(result.recovery_rate - 0.5) < 1e-9


# handle: batch_id filter
def test_handle_batch_id_filter_reads_only_matching_file(tmp_path):
    use_case, domain_service, _, _ = _make_use_case(valid_trips=[_make_trip()])

    _write_records(tmp_path / "batch-A.jsonl.gz", [_dl_record(batch_id="batch-A")])
    _write_records(tmp_path / "batch-B.jsonl.gz", [_dl_record(batch_id="batch-B")])

    cmd = ReplayDlqCommand(
        source=ReplaySource.DISK,
        batch_id="batch-A",
        rejected_dir=tmp_path,
    )
    result = use_case.handle(cmd)

    assert result.total_replayed == 1
    domain_service.process_batch.assert_called_once()


def test_handle_batch_id_filter_missing_file_returns_zero(tmp_path):
    use_case, _, _, _ = _make_use_case()
    cmd = ReplayDlqCommand(
        source=ReplaySource.DISK,
        batch_id="nonexistent-batch",
        rejected_dir=tmp_path,
    )
    result = use_case.handle(cmd)
    assert result.total_replayed == 0


# handle: multiple files
def test_handle_replays_all_files_when_no_filter(tmp_path):
    use_case, domain_service, _, _ = _make_use_case(valid_trips=[_make_trip()])

    _write_records(tmp_path / "b1.jsonl.gz", [_dl_record(batch_id="b1")])
    _write_records(tmp_path / "b2.jsonl.gz", [_dl_record(batch_id="b2")])
    _write_records(tmp_path / "b3.jsonl.gz", [_dl_record(batch_id="b3")])

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    result = use_case.handle(cmd)

    assert result.total_replayed == 3
    assert domain_service.process_batch.call_count == 3


# handle: flush called
def test_handle_calls_dead_letter_flush_at_end(tmp_path):
    use_case, _, _, dl_service = _make_use_case()
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    use_case.handle(cmd)
    dl_service.flush.assert_called_once()


def test_handle_calls_publisher_flush_for_recovered_records(tmp_path):
    use_case, _, publisher, _ = _make_use_case(valid_trips=[_make_trip()])
    _write_records(tmp_path / "b1.jsonl.gz", [_dl_record(batch_id="b1")])
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_path)
    use_case.handle(cmd)
    publisher.publish_batch.assert_called_once()
