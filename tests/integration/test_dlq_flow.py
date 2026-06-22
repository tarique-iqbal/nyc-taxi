"""
Integration test: DLQ flow for invalid trip records.

Requires running Kafka (make up). ClickHouse is not required -- the DLQ
flow writes to Kafka DLQ topic and disk regardless of ClickHouse state.

invalid_trips.parquet contains 5 rows, each with a distinct domain fault:
  Row 0: zero duration    (dropoff == pickup)          -> InvalidTripDurationError
  Row 1: passenger_count=11 (above maximum)            -> InvalidPassengerCountError
  Row 2: fare_amount=-5.0  (negative money)            -> NegativeMoneyError
  Row 3: pickup 2008-06-15 (before TLC min date 2009)  -> InvalidPickupDatetimeError
  Row 4: passenger_count=10 (above maximum)            -> InvalidPassengerCountError

sample_trips.parquet contains 5 rows that all pass domain validation.
A mixed batch test uses both to verify valid trips are published to the
main topic while invalid ones are routed to the DLQ.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    INVALID_PARQUET,
    SAMPLE_PARQUET,
    requires_kafka,
)

pytestmark = [requires_kafka, pytest.mark.integration]

EXPECTED_INVALID_COUNT = 5
EXPECTED_VALID_COUNT = 5


# Helpers

def _read_parquet_rows(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader
    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def _read_rejected_files(rejected_dir: Path) -> list[dict]:
    from etl.utils.compression import list_rejected_files, read_jsonl_gz
    records = []
    for f in list_rejected_files(rejected_dir):
        records.extend(read_jsonl_gz(f))
    return records


# All invalid rows rejected by domain pipeline

def test_all_invalid_rows_rejected(domain_service, batch_id):
    raw_rows = _read_parquet_rows(INVALID_PARQUET)

    valid_trips, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    assert len(valid_trips) == 0
    assert len(invalid_events) == EXPECTED_INVALID_COUNT


# Error types match the known faults in the fixture

def test_invalid_event_error_types(domain_service, batch_id):
    raw_rows = _read_parquet_rows(INVALID_PARQUET)

    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    error_types = [e.error_type for e in invalid_events]

    # Row 0: zero duration
    assert "InvalidTripDurationError" in error_types
    # Row 1 and 4: passenger count out of range
    assert error_types.count("InvalidPassengerCountError") == 2
    # Row 2: negative fare_amount raises at Money construction
    assert "NegativeMoneyError" in error_types
    # Row 3: pickup before TLC min date
    assert "InvalidPickupDatetimeError" in error_types


# Invalid events carry stage information

def test_invalid_events_carry_validation_stage(domain_service, batch_id):
    from etl.domain.trip.events import ProcessingStage

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    stages = {e.stage for e in invalid_events}
    assert ProcessingStage.VALIDATION in stages


# Invalid events preserve the original raw record

def test_invalid_events_preserve_original_record(domain_service, batch_id):
    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    for event in invalid_events:
        assert isinstance(event.original_record, dict)
        assert len(event.original_record) > 0
        # Raw record must contain pickup_datetime so replay is possible
        assert "pickup_datetime" in event.original_record


# DLQ records written to disk

def test_invalid_records_written_to_rejected_dir(
    domain_service,
    dead_letter_service,
    tmp_rejected_dir,
    batch_id,
):
    from etl.domain.dead_letter.models import DeadLetterRecord

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    on_disk = _read_rejected_files(tmp_rejected_dir)
    assert len(on_disk) == EXPECTED_INVALID_COUNT


# Disk records are complete and deserialise correctly

def test_rejected_records_have_required_fields(
    domain_service,
    dead_letter_service,
    tmp_rejected_dir,
    batch_id,
):
    from etl.domain.dead_letter.models import DeadLetterRecord

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    on_disk = _read_rejected_files(tmp_rejected_dir)
    for record in on_disk:
        assert "original_record" in record
        assert "error_message" in record
        assert "error_type" in record
        assert "stage" in record
        assert "batch_id" in record
        assert "source_file" in record
        assert "failed_at" in record
        assert record["batch_id"] == batch_id
        assert record["source_file"] == "invalid_trips.parquet"


# One rejected file per batch_id on disk

def test_rejected_file_named_by_batch_id(
    domain_service,
    dead_letter_service,
    tmp_rejected_dir,
    batch_id,
):
    from etl.domain.dead_letter.models import DeadLetterRecord
    from etl.utils.compression import list_rejected_files

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    files = list_rejected_files(tmp_rejected_dir)
    assert len(files) == 1
    assert batch_id in files[0].name


# Mixed batch: valid trips to Kafka, invalid to DLQ

def test_mixed_batch_routes_correctly(
    domain_service,
    kafka_producer,
    dead_letter_service,
    tmp_rejected_dir,
    settings,
    batch_id,
):
    """
    Combine sample_trips (5 valid) and invalid_trips (5 invalid) into
    one batch. Verify valid trips published to main topic and invalid
    records written to rejected dir.
    """
    from etl.domain.dead_letter.models import DeadLetterRecord

    valid_raw = _read_parquet_rows(SAMPLE_PARQUET)
    invalid_raw = _read_parquet_rows(INVALID_PARQUET)
    combined = valid_raw + invalid_raw

    valid_trips, invalid_events = domain_service.process_batch(
        raw_rows=combined,
        batch_id=batch_id,
        source_file="mixed_batch.parquet",
    )

    assert len(valid_trips) == EXPECTED_VALID_COUNT
    assert len(invalid_events) == EXPECTED_INVALID_COUNT

    # Publish valid to main topic
    messages = [t.to_dict() for t in valid_trips]
    kafka_producer.publish_batch(settings.kafka.topic, messages)
    kafka_producer.flush()

    # Route invalid to DLQ
    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    on_disk = _read_rejected_files(tmp_rejected_dir)
    assert len(on_disk) == EXPECTED_INVALID_COUNT


# retry_count starts at zero on first failure

def test_initial_retry_count_is_zero(
    domain_service,
    dead_letter_service,
    tmp_rejected_dir,
    batch_id,
):
    from etl.domain.dead_letter.models import DeadLetterRecord

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    on_disk = _read_rejected_files(tmp_rejected_dir)
    for record in on_disk:
        assert record["retry_count"] == 0


# Each error type present in disk records

def test_error_types_present_in_rejected_records(
    domain_service,
    dead_letter_service,
    tmp_rejected_dir,
    batch_id,
):
    from etl.domain.dead_letter.models import DeadLetterRecord

    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    _, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="invalid_trips.parquet",
    )

    dl_records = [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
    dead_letter_service.send_batch(dl_records)
    dead_letter_service.flush()

    on_disk = _read_rejected_files(tmp_rejected_dir)
    error_types = {r["error_type"] for r in on_disk}

    assert "InvalidTripDurationError" in error_types
    assert "InvalidPassengerCountError" in error_types
    assert "NegativeMoneyError" in error_types
    assert "InvalidPickupDatetimeError" in error_types
