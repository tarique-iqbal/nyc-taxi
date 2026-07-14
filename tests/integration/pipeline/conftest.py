from __future__ import annotations

import time
from pathlib import Path

import pytest

requires_full_stack = pytest.mark.skipif(
    False,  # evaluated by combined marks on each test module
    reason="Requires Kafka and ClickHouse",
)


def read_parquet_rows(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader

    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def query_batch(ch_client, batch_id: str) -> list[tuple]:
    return ch_client.execute(
        "SELECT trip_id, vendor_id, pickup_zone, dropoff_zone, "
        "       pickup_borough, total_amount, payment_type "
        "FROM taxi.trips FINAL "
        "WHERE batch_id = %(batch_id)s "
        "ORDER BY pickup_datetime",
        {"batch_id": batch_id},
    )


def wait_for_rows(ch_client, batch_id: str, expected: int, timeout: int = 15) -> list[tuple]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = query_batch(ch_client, batch_id)
        if len(rows) >= expected:
            return rows
        time.sleep(0.5)
    return query_batch(ch_client, batch_id)


def build_dead_letter_service(settings, rejected_dir: Path):
    from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher

    return KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        dlq_topic=settings.kafka.dlq_topic,
        rejected_dir=rejected_dir,
    )
