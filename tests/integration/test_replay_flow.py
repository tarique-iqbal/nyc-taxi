"""
Integration test: DLQ replay flow via ReplayDlqUseCase.

Requires running Kafka (make up). ClickHouse is not required.

Strategy:
  - Recoverable records: raw rows from sample_trips.parquet wrapped as
    DeadLetterRecord with stage=PERSIST (simulating a ClickHouse insert
    failure after successful domain processing). On replay the domain
    pipeline succeeds and they are published to the main Kafka topic.

  - Unrecoverable records: raw rows from invalid_trips.parquet wrapped as
    DeadLetterRecord. On replay the domain pipeline rejects them again.
    They are re-sent to the DLQ with retry_count incremented.

Each test uses two isolated tmp subdirectories:
  input_dir  -- holds the pre-populated rejected files to replay from.
  output_dir -- holds records that are still invalid after replay.

This separation ensures re-failed output does not mix with the original
input files being replayed.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from tests.integration.conftest import (
    INVALID_PARQUET,
    SAMPLE_PARQUET,
    requires_kafka,
)

pytestmark = [requires_kafka, pytest.mark.integration]

VALID_COUNT = 5
INVALID_COUNT = 5


def _read_parquet_rows(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader
    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def _make_dead_letter_records(
    raw_rows: list[dict],
    batch_id: str,
    source_file: str,
    stage: str = "persist",
    retry_count: int = 0,
) -> list:
    from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
    return [
        DeadLetterRecord(
            original_record=row,
            error_message="Simulated failure for replay test",
            error_type="SimulatedError",
            stage=DeadLetterStage(stage),
            batch_id=batch_id,
            source_file=source_file,
            retry_count=retry_count,
        )
        for row in raw_rows
    ]


def _write_to_dir(records: list, rejected_dir: Path, batch_id: str) -> None:
    from etl.utils.compression import rejected_file_path, write_jsonl_gz
    path = rejected_file_path(rejected_dir, batch_id)
    write_jsonl_gz([r.as_dict() for r in records], path)


def _read_from_dir(rejected_dir: Path) -> list[dict]:
    from etl.utils.compression import list_rejected_files, read_jsonl_gz
    records = []
    for f in list_rejected_files(rejected_dir):
        records.extend(read_jsonl_gz(f))
    return records


def _build_replay_use_case(domain_service, kafka_producer, settings, output_dir: Path):
    from etl.application.ingestion.replay_dlq import ReplayDlqUseCase
    from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher

    dl_service = KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        dlq_topic=settings.kafka.dlq_topic,
        rejected_dir=output_dir,
    )
    return ReplayDlqUseCase(
        domain_service=domain_service,
        publisher=kafka_producer,
        dead_letter_service=dl_service,
        topic=settings.kafka.topic,
    )


# Recoverable records
def test_valid_records_recovered_on_replay(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """
    Records that failed at the persist stage (ClickHouse down) but passed
    domain validation are recovered on replay. Full recovery rate expected.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid = str(uuid.uuid4())
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)
    records = _make_dead_letter_records(raw_rows, bid, "sample_trips.parquet", stage="persist")
    _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir)
    )

    assert result.total_replayed == VALID_COUNT
    assert result.recovered == VALID_COUNT
    assert result.still_invalid == 0
    assert result.recovery_rate == 1.0


def test_recovered_records_not_re_written_to_output(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """Recovered records must not appear in the output dir."""
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid = str(uuid.uuid4())
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)
    records = _make_dead_letter_records(raw_rows, bid, "sample_trips.parquet", stage="persist")
    _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    use_case.handle(ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir))

    on_disk = _read_from_dir(output_dir)
    assert len(on_disk) == 0


# Unrecoverable records
def test_invalid_records_remain_in_dlq_after_replay(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """
    Records whose original_record data is inherently invalid remain invalid
    on replay. They are re-sent to the DLQ via the output dead_letter_service.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid = str(uuid.uuid4())
    raw_rows = _read_parquet_rows(INVALID_PARQUET)
    records = _make_dead_letter_records(raw_rows, bid, "invalid_trips.parquet")
    _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir)
    )

    assert result.total_replayed == INVALID_COUNT
    assert result.recovered == 0
    assert result.still_invalid == INVALID_COUNT
    assert result.recovery_rate == 0.0


def test_retry_count_incremented_on_replay(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """retry_count must be incremented for each failed replay attempt."""
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid = str(uuid.uuid4())
    raw_rows = _read_parquet_rows(INVALID_PARQUET)[:1]
    records = _make_dead_letter_records(
        raw_rows, bid, "invalid_trips.parquet", retry_count=2
    )
    _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    use_case.handle(ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir))

    on_disk = _read_from_dir(output_dir)
    assert len(on_disk) == 1
    assert on_disk[0]["retry_count"] == 3


# Mixed recovery
def test_mixed_recovery_rate(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """
    Batch contains valid and invalid records. Recovery rate reflects the split.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid = str(uuid.uuid4())
    valid_rows = _read_parquet_rows(SAMPLE_PARQUET)[:3]
    invalid_rows = _read_parquet_rows(INVALID_PARQUET)[:2]

    records = (
        _make_dead_letter_records(valid_rows, bid, "sample.parquet", stage="persist")
        + _make_dead_letter_records(invalid_rows, bid, "invalid.parquet")
    )
    _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir)
    )

    assert result.total_replayed == 5
    assert result.recovered == 3
    assert result.still_invalid == 2
    assert abs(result.recovery_rate - 0.6) < 0.01


# Batch ID filter
def test_replay_filters_by_batch_id(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """
    ReplayDlqCommand.batch_id restricts replay to one file.
    Records from other batch files are untouched.
    """
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    bid_a = str(uuid.uuid4())
    bid_b = str(uuid.uuid4())

    valid_rows = _read_parquet_rows(SAMPLE_PARQUET)
    invalid_rows = _read_parquet_rows(INVALID_PARQUET)

    _write_to_dir(
        _make_dead_letter_records(valid_rows, bid_a, "sample.parquet", stage="persist"),
        input_dir,
        bid_a,
    )
    _write_to_dir(
        _make_dead_letter_records(invalid_rows, bid_b, "invalid.parquet"),
        input_dir,
        bid_b,
    )

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(
            source=ReplaySource.DISK,
            rejected_dir=input_dir,
            batch_id=bid_a,
        )
    )

    assert result.total_replayed == VALID_COUNT
    assert result.recovered == VALID_COUNT
    assert result.still_invalid == 0


# Edge cases
def test_empty_rejected_dir_completes_with_zero_totals(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir)
    )

    assert result.total_replayed == 0
    assert result.recovered == 0
    assert result.still_invalid == 0
    assert result.recovery_rate == 0.0


def test_replay_kafka_source_raises_not_implemented(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    output_dir = tmp_path / "output"
    output_dir.mkdir()

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)

    with pytest.raises(NotImplementedError):
        use_case.handle(ReplayDlqCommand(source=ReplaySource.KAFKA))


def test_multiple_batches_all_replayed_when_no_filter(
    domain_service,
    kafka_producer,
    settings,
    tmp_path,
):
    """Without a batch_id filter all files in the rejected dir are replayed."""
    from etl.application.ingestion.replay_dlq import ReplayDlqCommand, ReplaySource

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    for _ in range(3):
        bid = str(uuid.uuid4())
        rows = _read_parquet_rows(SAMPLE_PARQUET)[:2]
        records = _make_dead_letter_records(rows, bid, "sample.parquet", stage="persist")
        _write_to_dir(records, input_dir, bid)

    use_case = _build_replay_use_case(domain_service, kafka_producer, settings, output_dir)
    result = use_case.handle(
        ReplayDlqCommand(source=ReplaySource.DISK, rejected_dir=input_dir)
    )

    assert result.total_replayed == 6
    assert result.recovered == 6
