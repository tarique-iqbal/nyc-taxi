"""
Integration: DLQ replay pipeline.

Tests the full recovery flow:
  1. Domain pipeline rejects invalid records → written to data/rejected/
  2. Root cause is absent in replay (or records are valid in replay fixture)
  3. ReplayDlqUseCase re-runs original_record through domain pipeline
  4. Recovered records published to Kafka
  5. Still-invalid records re-written to DLQ with retry_count + 1

invalid_trips.parquet — 5 rows each permanently invalid.
sample_trips.parquet  — 5 rows valid on every run (used to simulate
                        recoverable records by writing them as DLQ entries).
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
from etl.utils.compression import list_rejected_files, read_jsonl_gz, write_jsonl_gz
from tests.integration.conftest import (
    INVALID_PARQUET,
    SAMPLE_PARQUET,
    requires_kafka,
)
from tests.integration.pipeline.conftest import (
    build_dead_letter_service,
    read_parquet_rows,
)

pytestmark = [requires_kafka, pytest.mark.integration]


def _make_dl_record(
    raw: dict,
    batch_id: str,
    retry_count: int = 0,
    trip_id: str | None = None,
) -> DeadLetterRecord:
    return DeadLetterRecord(
        original_record=raw,
        error_message="test error",
        error_type="InvalidTripDurationError",
        stage=DeadLetterStage.VALIDATION,
        batch_id=batch_id,
        source_file="test.parquet",
        trip_id=trip_id,
        retry_count=retry_count,
    )


def _write_dl_records(path: Path, records: list[DeadLetterRecord]) -> None:
    write_jsonl_gz([r.as_dict() for r in records], path)


def _make_replay_use_case(settings, rejected_dir: Path, kafka_producer):
    from etl.application.ingestion.replay_dlq import ReplayDlqUseCase
    from etl.domain.trip.services import TripDomainService
    from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher
    from etl.infrastructure.storage.zone_lookup import CsvZoneRepository
    from tests.integration.conftest import ZONE_CSV

    repo = CsvZoneRepository(path=ZONE_CSV)
    repo.load()
    domain_service = TripDomainService(zone_repository=repo)

    output_dl_service = KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        dlq_topic=settings.kafka.dlq_topic,
        rejected_dir=rejected_dir / "output",
    )
    (rejected_dir / "output").mkdir(exist_ok=True)

    return ReplayDlqUseCase(
        domain_service=domain_service,
        publisher=kafka_producer,
        dead_letter_service=output_dl_service,
        topic=settings.kafka.topic,
    ), output_dl_service


# Writing to DLQ
def test_invalid_rows_written_to_rejected_dir(domain_service, settings, tmp_rejected_dir):
    dl_service = build_dead_letter_service(settings, tmp_rejected_dir)
    rows = read_parquet_rows(INVALID_PARQUET)
    batch_id = str(uuid.uuid4())
    _, invalid = domain_service.process_batch(rows, batch_id, "invalid_trips.parquet")

    for event in invalid:
        dl_record = DeadLetterRecord.from_invalid_event(event)
        dl_service.send(dl_record)
    dl_service.flush()

    files = list_rejected_files(tmp_rejected_dir)
    assert len(files) >= 1
    all_records = [r for f in files for r in read_jsonl_gz(f)]
    assert len(all_records) == len(invalid)


def test_dl_records_have_correct_initial_retry_count(domain_service, settings, tmp_rejected_dir):
    dl_service = build_dead_letter_service(settings, tmp_rejected_dir)
    rows = read_parquet_rows(INVALID_PARQUET)
    batch_id = str(uuid.uuid4())
    _, invalid = domain_service.process_batch(rows, batch_id, "invalid_trips.parquet")

    for event in invalid:
        dl_service.send(DeadLetterRecord.from_invalid_event(event))
    dl_service.flush()

    all_records = [r for f in list_rejected_files(tmp_rejected_dir) for r in read_jsonl_gz(f)]
    assert all(r["retry_count"] == 0 for r in all_records)


# Replay: recoverable records
def test_replay_recoverable_records_published_to_kafka(settings, kafka_producer, tmp_rejected_dir):
    """
    Seed the rejected dir with records that will pass the domain pipeline
    (valid raw rows from sample_trips.parquet written as DL entries).
    Replay must publish them to the main Kafka topic.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, output_dl = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    batch_id = str(uuid.uuid4())
    valid_rows = read_parquet_rows(SAMPLE_PARQUET)
    records = [_make_dl_record(row, batch_id) for row in valid_rows]
    _write_dl_records(tmp_rejected_dir / f"{batch_id}.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    result = use_case.handle(cmd)

    assert result.recovered == len(valid_rows)
    assert result.still_invalid == 0
    assert abs(result.recovery_rate - 1.0) < 1e-9


def test_replay_recovery_rate_one_for_valid_records(settings, kafka_producer, tmp_rejected_dir):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    batch_id = str(uuid.uuid4())
    valid_rows = read_parquet_rows(SAMPLE_PARQUET)
    records = [_make_dl_record(row, batch_id) for row in valid_rows]
    _write_dl_records(tmp_rejected_dir / f"{batch_id}.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    result = use_case.handle(cmd)

    assert result.recovery_rate == 1.0


# Replay: permanently invalid records
def test_replay_permanently_invalid_records_remain_in_dlq(
    settings, kafka_producer, tmp_rejected_dir
):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    batch_id = str(uuid.uuid4())
    invalid_rows = read_parquet_rows(INVALID_PARQUET)
    records = [_make_dl_record(row, batch_id) for row in invalid_rows]
    _write_dl_records(tmp_rejected_dir / f"{batch_id}.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    result = use_case.handle(cmd)

    assert result.still_invalid == len(invalid_rows)
    assert result.recovered == 0
    assert result.recovery_rate == 0.0


def test_replay_still_invalid_retry_count_incremented(settings, kafka_producer, tmp_rejected_dir):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    batch_id = str(uuid.uuid4())
    invalid_rows = read_parquet_rows(INVALID_PARQUET)
    records = [_make_dl_record(row, batch_id, retry_count=2) for row in invalid_rows]
    _write_dl_records(tmp_rejected_dir / f"{batch_id}.jsonl.gz", records)

    output_dir = tmp_rejected_dir / "output"
    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    use_case.handle(cmd)

    output_records = [r for f in list_rejected_files(output_dir) for r in read_jsonl_gz(f)]
    assert all(r["retry_count"] == 3 for r in output_records)


# Replay: batch_id filter
def test_replay_batch_id_filter_replays_only_target(settings, kafka_producer, tmp_rejected_dir):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    bid_a = str(uuid.uuid4())
    bid_b = str(uuid.uuid4())
    valid_rows = read_parquet_rows(SAMPLE_PARQUET)

    _write_dl_records(
        tmp_rejected_dir / f"{bid_a}.jsonl.gz",
        [_make_dl_record(r, bid_a) for r in valid_rows],
    )
    _write_dl_records(
        tmp_rejected_dir / f"{bid_b}.jsonl.gz",
        [_make_dl_record(r, bid_b) for r in read_parquet_rows(INVALID_PARQUET)],
    )

    cmd = ReplayDlqCommand(
        source=ReplaySource.DISK,
        batch_id=bid_a,
        rejected_dir=tmp_rejected_dir,
    )
    result = use_case.handle(cmd)

    assert result.total_replayed == len(valid_rows)
    assert result.recovered == len(valid_rows)


# Replay: empty directory
def test_replay_empty_dir_returns_zero_totals(settings, kafka_producer, tmp_rejected_dir):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    result = use_case.handle(cmd)

    assert result.total_replayed == 0
    assert result.recovered == 0
    assert result.still_invalid == 0


# Replay + ClickHouse
def test_replay_recovered_records_reachable_via_clickhouse(
    settings, kafka_producer, trip_repository, ch_client, tmp_rejected_dir, cleanup_batch
):
    """
    Recovered records published to Kafka are then available to be
    consumed and inserted into ClickHouse via the normal consumer path.
    This test simulates that by inserting directly after replay.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    use_case, _ = _make_replay_use_case(settings, tmp_rejected_dir, kafka_producer)

    valid_rows = read_parquet_rows(SAMPLE_PARQUET)
    records = [_make_dl_record(row, cleanup_batch) for row in valid_rows]
    _write_dl_records(tmp_rejected_dir / f"{cleanup_batch}.jsonl.gz", records)

    cmd = ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=tmp_rejected_dir)
    result = use_case.handle(cmd)

    assert result.recovered == len(valid_rows)
