from __future__ import annotations

from unittest.mock import patch

import pytest

from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher


def _make_record(stage: DeadLetterStage = DeadLetterStage.VALIDATION) -> DeadLetterRecord:
    return DeadLetterRecord(
        original_record={"vendor_id": "1"},
        error_message="boom",
        error_type="ValueError",
        stage=stage,
        batch_id="batch-1",
        source_file="trips.parquet",
        trip_id="trip-1",
    )


@pytest.fixture
def publisher(tmp_path):
    with patch("etl.infrastructure.kafka.dead_letter_publisher.Producer"):
        return KafkaDeadLetterPublisher(
            bootstrap_servers="localhost:9092",
            dlq_topic="nyc-taxi-trips-dlq",
            rejected_dir=tmp_path,
        )


@patch("etl.infrastructure.kafka.dead_letter_publisher.write_jsonl_gz")
@patch("etl.infrastructure.kafka.dead_letter_publisher.dlq_records_total")
def test_send_increments_counter_with_stage_label(mock_counter, mock_write, publisher):
    record = _make_record(DeadLetterStage.ENRICHMENT)

    publisher.send(record)

    mock_counter.labels.assert_called_once_with(stage="enrichment")
    mock_counter.labels.return_value.inc.assert_called_once_with()


@patch("etl.infrastructure.kafka.dead_letter_publisher.write_jsonl_gz")
@patch("etl.infrastructure.kafka.dead_letter_publisher.dlq_records_total")
def test_send_batch_increments_counter_once_per_record(mock_counter, mock_write, publisher):
    records = [
        _make_record(DeadLetterStage.PARSING),
        _make_record(DeadLetterStage.PERSIST),
    ]

    publisher.send_batch(records)

    assert mock_counter.labels.call_count == 2
    mock_counter.labels.assert_any_call(stage="parsing")
    mock_counter.labels.assert_any_call(stage="persist")
    assert mock_counter.labels.return_value.inc.call_count == 2


@patch("etl.infrastructure.kafka.dead_letter_publisher.write_jsonl_gz")
@patch("etl.infrastructure.kafka.dead_letter_publisher.dlq_records_total")
def test_send_increments_counter_even_when_disk_write_fails(mock_counter, mock_write, publisher):
    mock_write.side_effect = OSError("disk full")

    publisher.send(_make_record())

    mock_counter.labels.return_value.inc.assert_called_once_with()


@patch("etl.infrastructure.kafka.dead_letter_publisher.write_jsonl_gz")
def test_send_writes_to_kafka_and_disk(mock_write, publisher):
    record = _make_record()

    publisher.send(record)

    publisher._producer.produce.assert_called_once()
    mock_write.assert_called_once()
