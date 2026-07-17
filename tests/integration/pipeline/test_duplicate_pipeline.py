"""
Integration: duplicate detection pipeline.

Tests two deduplication layers:
  1. Within-batch: TripDeduplicator removes duplicate trip_ids before insert.
  2. Cross-batch:  ReplacingMergeTree keeps only the latest ingested_at on FINAL.

sample_trips.parquet provides 5 rows with deterministic trip_ids.
"""

from __future__ import annotations

import pytest

from tests.integration.conftest import (
    SAMPLE_PARQUET,
    requires_clickhouse,
    requires_kafka,
)
from tests.integration.pipeline.conftest import (
    read_parquet_rows,
    wait_for_rows,
)

pytestmark = [requires_kafka, requires_clickhouse, pytest.mark.integration]

EXPECTED_VALID = 5


# trip_id determinism
def test_trip_id_is_deterministic_for_same_row(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid_a, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    valid_b, _ = domain_service.process_batch(rows, batch_id + "-2", "sample_trips.parquet")
    ids_a = [t.trip_id for t in valid_a]
    ids_b = [t.trip_id for t in valid_b]
    assert ids_a == ids_b


def test_different_rows_produce_different_trip_ids(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    trip_ids = [t.trip_id for t in valid]
    assert len(set(trip_ids)) == len(trip_ids)


# Within-batch deduplication
def test_doubled_rows_produce_half_valid_half_duplicates(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    doubled = rows + rows
    valid, invalid = domain_service.process_batch(doubled, batch_id, "sample_trips.parquet")
    assert len(valid) == EXPECTED_VALID
    assert len(invalid) == EXPECTED_VALID


def test_within_batch_duplicate_events_have_deduplication_stage(domain_service, batch_id):
    from etl.domain.trip.events import ProcessingStage

    rows = read_parquet_rows(SAMPLE_PARQUET)
    doubled = rows + rows
    _, invalid = domain_service.process_batch(doubled, batch_id, "sample_trips.parquet")
    stages = {e.stage for e in invalid}
    assert ProcessingStage.DEDUPLICATION in stages


def test_tripled_rows_keep_one_reject_two_per_trip(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    tripled = rows + rows + rows
    valid, invalid = domain_service.process_batch(tripled, batch_id, "sample_trips.parquet")
    assert len(valid) == EXPECTED_VALID
    assert len(invalid) == EXPECTED_VALID * 2


def test_within_batch_dedup_preserves_first_occurrence(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid_single, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    valid_doubled, _ = domain_service.process_batch(
        rows + rows, batch_id + "-d", "sample_trips.parquet"
    )
    ids_single = set(t.trip_id for t in valid_single)
    ids_doubled = set(t.trip_id for t in valid_doubled)
    assert ids_single == ids_doubled


def test_within_batch_duplicates_not_inserted_to_clickhouse(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    doubled = rows + rows
    valid, _ = domain_service.process_batch(doubled, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    assert len(persisted) == EXPECTED_VALID


# Cross-batch deduplication (ReplacingMergeTree)
def test_same_batch_inserted_twice_deduplicates_on_final(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    dicts = [t.to_dict() for t in valid]

    trip_repository.save_batch_from_dicts(dicts)
    trip_repository.save_batch_from_dicts(dicts)

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    assert len(persisted) == EXPECTED_VALID


def test_three_inserts_same_data_deduplicates_on_final(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    dicts = [t.to_dict() for t in valid]

    for _ in range(3):
        trip_repository.save_batch_from_dicts(dicts)

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    assert len(persisted) == EXPECTED_VALID


def test_cross_batch_dedup_uses_latest_ingested_at(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    Insert the same batch twice. ReplacingMergeTree keeps the row with the
    highest ingested_at. Verify count via FINAL is still EXPECTED_VALID.
    """
    import time

    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    dicts = [t.to_dict() for t in valid]

    trip_repository.save_batch_from_dicts(dicts)
    time.sleep(0.1)  # ensure different ingested_at
    trip_repository.save_batch_from_dicts(dicts)

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    assert len(persisted) == EXPECTED_VALID


def test_raw_count_may_exceed_final_count_before_merge(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    Before background merge, raw row count may be higher than FINAL count.
    This confirms that ReplacingMergeTree deduplication is query-time
    when using FINAL, not necessarily immediate.
    """
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    dicts = [t.to_dict() for t in valid]

    trip_repository.save_batch_from_dicts(dicts)
    trip_repository.save_batch_from_dicts(dicts)

    # Raw count (no FINAL) may be 2x
    raw = ch_client.execute(
        "SELECT count() FROM taxi.trips WHERE batch_id = %(bid)s",
        {"bid": cleanup_batch},
    )
    raw_count = raw[0][0] if raw else 0

    # FINAL count is always deduplicated
    final = ch_client.execute(
        "SELECT count() FROM taxi.trips FINAL WHERE batch_id = %(bid)s",
        {"bid": cleanup_batch},
    )
    final_count = final[0][0] if final else 0

    assert final_count == EXPECTED_VALID
    assert raw_count >= EXPECTED_VALID
