"""
Integration: invalid trips are routed to the DLQ and never reach ClickHouse.

invalid_trips.parquet contains 5 rows each with a distinct fault:
  Row 0: zero duration          → InvalidTripDurationError
  Row 1: passenger_count=10     → InvalidPassengerCountError
  Row 2: fare_amount=-5.0       → NegativeMoneyError
  Row 3: pickup 2008 (pre-TLC)  → InvalidPickupDatetimeError
  Row 4: passenger_count=10     → InvalidPassengerCountError

Expected outcome:
  - 0 messages published to main Kafka topic
  - 5 records written to data/rejected/ (disk)
  - 0 rows inserted into ClickHouse
  - Each DeadLetterRecord preserves original_record for replay

Requires: Kafka + ClickHouse (make up && make schema && make topics).
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.integration.conftest import (
    INVALID_PARQUET,
    requires_clickhouse,
    requires_kafka,
)

pytestmark = [requires_clickhouse, requires_kafka, pytest.mark.integration]

EXPECTED_INVALID = 5


def _read_parquet(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader

    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def _run_pipeline(
    raw_rows,
    domain_service,
    publisher,
    dead_letter_service,
    topic,
    batch_id,
    source_file,
) -> tuple[int, int]:
    from etl.application.services.validation_service import ValidationService
    from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage

    schema_valid, schema_invalid = ValidationService().validate_batch(raw_rows)
    for row, err in schema_invalid:
        dead_letter_service.send(
            DeadLetterRecord(
                original_record=row,
                error_message=err,
                error_type="SchemaValidationError",
                stage=DeadLetterStage.VALIDATION,
                batch_id=batch_id,
                source_file=source_file,
            )
        )

    valid_trips, invalid_events = domain_service.process_batch(schema_valid, batch_id, source_file)

    if valid_trips:
        publisher.publish_batch(topic, [t.to_dict() for t in valid_trips])

    if invalid_events:
        dead_letter_service.send_batch(
            [DeadLetterRecord.from_invalid_event(e) for e in invalid_events]
        )

    publisher.flush()
    dead_letter_service.flush()
    return len(valid_trips), len(invalid_events) + len(schema_invalid)


def _read_rejected(rejected_dir: Path) -> list[dict]:
    from etl.utils.compression import list_rejected_files, read_jsonl_gz

    records = []
    for f in list_rejected_files(rejected_dir):
        records.extend(read_jsonl_gz(f))
    return records


def test_no_valid_trips_published(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    valid_count, _ = _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )
    assert valid_count == 0


def test_all_invalid_rows_dead_lettered_to_disk(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    _, invalid_count = _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    assert invalid_count == EXPECTED_INVALID

    on_disk = _read_rejected(tmp_rejected_dir)
    assert len(on_disk) == EXPECTED_INVALID


def test_zero_rows_in_clickhouse_for_invalid_batch(
    domain_service,
    kafka_producer,
    dead_letter_service,
    ch_client,
    settings,
    cleanup_batch,
    tmp_rejected_dir,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet(INVALID_PARQUET)
    _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    # Give async insert time to flush if anything was accidentally inserted
    time.sleep(1)

    result = ch_client.execute(
        "SELECT count() FROM taxi.trips FINAL WHERE batch_id = %(bid)s",
        {"bid": batch_id},
    )
    assert int(result[0][0]) == 0


def test_rejected_records_contain_all_error_types(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    on_disk = _read_rejected(tmp_rejected_dir)
    error_types = {r["error_type"] for r in on_disk}

    assert "InvalidTripDurationError" in error_types
    assert "InvalidPassengerCountError" in error_types
    assert "NegativeMoneyError" in error_types
    assert "InvalidPickupDatetimeError" in error_types


def test_rejected_records_preserve_original_row(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    on_disk = _read_rejected(tmp_rejected_dir)
    for record in on_disk:
        assert "original_record" in record
        assert isinstance(record["original_record"], dict)
        assert len(record["original_record"]) > 0
        assert "pickup_datetime" in record["original_record"]


def test_rejected_records_have_correct_source_file(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    on_disk = _read_rejected(tmp_rejected_dir)
    for record in on_disk:
        assert record["source_file"] == "invalid_trips.parquet"
        assert record["batch_id"] == batch_id


def test_rejected_records_initial_retry_count_zero(
    domain_service,
    kafka_producer,
    dead_letter_service,
    settings,
    batch_id,
    tmp_rejected_dir,
):
    raw_rows = _read_parquet(INVALID_PARQUET)
    _run_pipeline(
        raw_rows,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "invalid_trips.parquet",
    )

    on_disk = _read_rejected(tmp_rejected_dir)
    for record in on_disk:
        assert record["retry_count"] == 0


def test_mixed_batch_splits_at_pipeline_boundary(
    domain_service,
    kafka_producer,
    kafka_consumer,
    trip_repository,
    ch_client,
    dead_letter_service,
    settings,
    cleanup_batch,
    tmp_rejected_dir,
):
    """
    Combining valid (5) and invalid (5) rows in one batch:
    valid trips reach ClickHouse, invalid reach rejected dir.
    """
    from etl.infrastructure.storage.parquet_reader import ParquetReader
    from etl.runtime.shutdown import ShutdownHandler
    from tests.integration.conftest import SAMPLE_PARQUET

    batch_id = cleanup_batch

    valid_rows = []
    for batch in ParquetReader(path=SAMPLE_PARQUET, batch_size=100).iter_batches():
        valid_rows.extend(batch)

    invalid_rows = _read_parquet(INVALID_PARQUET)
    combined = valid_rows + invalid_rows

    valid_count, invalid_count = _run_pipeline(
        combined,
        domain_service,
        kafka_producer,
        dead_letter_service,
        settings.kafka.topic,
        batch_id,
        "mixed.parquet",
    )

    assert valid_count == 5
    assert invalid_count == 5

    handler = ShutdownHandler()
    start = time.monotonic()

    inserted_ids = set()
    expected_count = 5

    for batch in kafka_consumer.consume_batches(handler):
        # Only process records produced by this test batch
        rows = [row for row in batch.rows if row.get("batch_id") == batch_id]

        if not rows:
            if time.monotonic() - start > 15:
                handler.request_shutdown()
                break
            continue

        trip_repository.save_batch_from_dicts(rows)
        kafka_consumer.commit()

        inserted_ids.update(row["trip_id"] for row in rows)

        if len(inserted_ids) >= expected_count:
            handler.request_shutdown()
            break

        if time.monotonic() - start > 15:
            handler.request_shutdown()
            break

    time.sleep(1)

    result = ch_client.execute(
        "SELECT count() FROM taxi.trips FINAL WHERE batch_id = %(bid)s",
        {"bid": batch_id},
    )

    assert int(result[0][0]) == 5

    on_disk = _read_rejected(tmp_rejected_dir)
    assert len(on_disk) == 5
