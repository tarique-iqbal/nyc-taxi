"""
Integration test: full pipeline from Parquet -> Kafka -> ClickHouse.

Requires running Kafka and ClickHouse (make up).
Skip if infrastructure is unavailable.

Fixture data (sample_trips.parquet) contains 5 valid trips:
  Row 0: VendorID=1, PU=161 (Midtown Center),  DO=236 (Upper East Side North)
  Row 1: VendorID=2, PU=162 (Midtown East),    DO=186 (Penn Station)
  Row 2: VendorID=1, PU=230 (Times Sq),        DO=161 (Midtown Center)
  Row 3: VendorID=2, PU=132 (JFK Airport),     DO=163 (Midtown North), rate=JFK
  Row 4: VendorID=1, PU=239 (Upper West Side), DO=237 (Upper East Side South)

All rows pass domain validation. All pickup/dropoff location IDs exist in
tests/fixtures/taxi_zone_lookup.csv so enrichment populates real zone names.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from tests.integration.conftest import (
    SAMPLE_PARQUET,
    requires_clickhouse,
    requires_kafka,
)

pytestmark = [requires_clickhouse, requires_kafka, pytest.mark.integration]

EXPECTED_VALID_COUNT = 5


def _read_parquet_rows(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader

    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def _query_batch(ch_client, batch_id: str) -> list[tuple]:
    return ch_client.execute(
        "SELECT trip_id, vendor_id, pickup_zone, dropoff_zone, "
        "       pickup_borough, total_amount, payment_type, rate_code "
        "FROM taxi.trips FINAL "
        "WHERE batch_id = %(batch_id)s "
        "ORDER BY pickup_datetime",
        {"batch_id": batch_id},
    )


def _wait_for_rows(ch_client, batch_id: str, expected: int, timeout: int = 15) -> list[tuple]:
    """Poll ClickHouse until expected rows appear or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = _query_batch(ch_client, batch_id)
        if len(rows) >= expected:
            return rows
        time.sleep(0.5)
    return _query_batch(ch_client, batch_id)


def test_producer_publishes_valid_trips(
    domain_service,
    kafka_producer,
    settings,
    cleanup_batch,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )

    assert len(valid_trips) == EXPECTED_VALID_COUNT, (
        f"Expected {EXPECTED_VALID_COUNT} valid trips, got {len(valid_trips)}. "
        f"Invalid events: {[e.error_message for e in invalid_events]}"
    )
    assert len(invalid_events) == 0

    messages = [t.to_dict() for t in valid_trips]
    kafka_producer.publish_batch(settings.kafka.topic, messages)
    kafka_producer.flush()


def test_consumer_inserts_trips_into_clickhouse(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )

    # Simulate the consumer insert path (dicts from Kafka, not Trip entities).
    rows = [t.to_dict() for t in valid_trips]
    trip_repository.save_batch_from_dicts(rows)

    persisted = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)
    assert len(persisted) == EXPECTED_VALID_COUNT


def test_zone_enrichment_populated_in_clickhouse(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])

    rows = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)
    assert len(rows) == EXPECTED_VALID_COUNT

    # rows columns: trip_id, vendor_id, pickup_zone, dropoff_zone,
    #               pickup_borough, total_amount, payment_type, rate_code
    zones_by_pickup_zone = {row[2]: row for row in rows}

    # Row 0: PU=161 (Midtown Center), DO=236 (Upper East Side North)
    midtown = zones_by_pickup_zone.get("Midtown Center")
    assert midtown is not None, f"Midtown Center not found. Zones: {list(zones_by_pickup_zone)}"
    assert midtown[3] == "Upper East Side North"
    assert midtown[4] == "Manhattan"

    # Row 3: PU=132 (JFK Airport) -- rate_code should be JFK
    jfk = zones_by_pickup_zone.get("JFK Airport")
    assert jfk is not None, "JFK Airport pickup zone not found"
    assert jfk[7] == "JFK"


def test_normalised_fields_in_clickhouse(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])

    rows = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)

    # vendor_id should be readable strings, not raw ints
    vendor_ids = {row[1] for row in rows}
    assert "Creative Mobile Technologies" in vendor_ids
    assert "VeriFone Inc." in vendor_ids
    assert 1 not in vendor_ids
    assert 2 not in vendor_ids

    # payment_type should be readable strings
    payment_types = {row[6] for row in rows}
    assert "Credit card" in payment_types
    assert "Cash" in payment_types
    assert 1 not in payment_types


def test_fare_amounts_stored_correctly(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])

    rows = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)

    # total_amount is column index 5 in the query
    total_amounts = sorted(float(row[5]) for row in rows)

    # Expected totals from sample_trips.parquet (sorted):
    # 13.30, 18.30, 19.80, 25.80, 66.05
    assert len(total_amounts) == 5
    assert abs(total_amounts[0] - 13.30) < 0.01
    assert abs(total_amounts[4] - 66.05) < 0.01


def test_duplicate_insert_deduplicated_by_replacing_merge_tree(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    """
    Insert the same batch twice. ReplacingMergeTree on trip_id ensures
    FINAL queries return exactly EXPECTED_VALID_COUNT rows, not double.

    Note: ClickHouse deduplication is eventual. FINAL forces a merge
    read that resolves duplicates at query time, which is accurate even
    before background merge runs.
    """
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )
    rows = [t.to_dict() for t in valid_trips]

    # Insert twice
    trip_repository.save_batch_from_dicts(rows)
    trip_repository.save_batch_from_dicts(rows)

    persisted = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)
    assert len(persisted) == EXPECTED_VALID_COUNT, (
        f"Expected {EXPECTED_VALID_COUNT} after dedup, got {len(persisted)}"
    )


def test_within_batch_duplicates_removed_before_clickhouse(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    """
    Feed the same raw rows twice in one batch.
    TripDeduplicator should remove them before they reach ClickHouse.
    """
    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    # Double the raw rows -- same trips appear twice in one batch
    doubled = raw_rows + raw_rows

    valid_trips, invalid_events = domain_service.process_batch(
        raw_rows=doubled,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )

    # Domain layer should have caught within-batch duplicates
    assert len(valid_trips) == EXPECTED_VALID_COUNT

    # Duplicates should appear as InvalidTripDetected with deduplication stage
    from etl.domain.trip.events import ProcessingStage

    dup_events = [e for e in invalid_events if e.stage == ProcessingStage.DEDUPLICATION]
    assert len(dup_events) == EXPECTED_VALID_COUNT

    # Insert the deduplicated batch
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])
    persisted = _wait_for_rows(ch_client, batch_id, EXPECTED_VALID_COUNT)
    assert len(persisted) == EXPECTED_VALID_COUNT


def test_etl_metadata_persisted(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    batch_id = cleanup_batch
    source_file = "sample_trips.parquet"
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file=source_file,
    )
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])

    result = ch_client.execute(
        "SELECT DISTINCT batch_id, source_file FROM taxi.trips FINAL WHERE batch_id = %(batch_id)s",
        {"batch_id": batch_id},
    )

    assert len(result) == 1
    assert result[0][0] == batch_id
    assert result[0][1] == source_file


def test_kafka_json_roundtrip_preserves_timestamp_fields(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    """
    Regression test for Kafka JSON serialization.

    Datetime fields are serialized to ISO8601 strings when sent
    through Kafka. The consumer/repository layer must convert them
    back into Python datetimes before constructing Arrow tables.
    """
    from etl.infrastructure.kafka.serializer import KafkaSerializer

    serializer = KafkaSerializer()

    batch_id = cleanup_batch

    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, invalid_events = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )

    assert len(valid_trips) == EXPECTED_VALID_COUNT
    assert len(invalid_events) == 0

    kafka_messages: list[dict] = []

    for trip in valid_trips:
        original = trip.to_dict()

        payload = serializer.serialize(original)

        restored = serializer.deserialize(payload)

        assert restored is not None

        # Verify Kafka converted datetimes into strings.
        # This is the exact situation the consumer receives.
        assert isinstance(
            restored["pickup_datetime"],
            str,
        )

        assert isinstance(
            restored["dropoff_datetime"],
            str,
        )

        kafka_messages.append(restored)

    # save_batch_from_dicts must accept Kafka-restored payloads.
    trip_repository.save_batch_from_dicts(kafka_messages)

    persisted = _wait_for_rows(
        ch_client,
        batch_id,
        EXPECTED_VALID_COUNT,
    )

    assert len(persisted) == EXPECTED_VALID_COUNT


def test_repository_accepts_iso8601_timestamp_strings(
    domain_service,
    trip_repository,
    ch_client,
    cleanup_batch,
):
    """
    Repository must accept Kafka-style ISO8601 datetime strings.

    Shifts pickup/dropoff off SAMPLE_PARQUET's original date: the _mv
    tables' countState() etc. accumulate per physical insert regardless
    of trip_id dedup on the raw table, so an extra insert on that date
    would permanently skew test_materialized_views.py's exact-count
    assertions with no way to clean it back up.
    """
    from datetime import timedelta

    from etl.utils.hashing import hash_trip

    batch_id = cleanup_batch
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)

    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )

    trip = valid_trips[0]
    shifted_pickup = trip.pickup_datetime + timedelta(days=365)
    shifted_dropoff = trip.dropoff_datetime + timedelta(days=365)

    row = trip.to_dict()
    row["pickup_datetime"] = shifted_pickup.isoformat()
    row["dropoff_datetime"] = shifted_dropoff.isoformat()
    row["trip_id"] = hash_trip(
        trip.vendor_id,
        shifted_pickup,
        shifted_dropoff,
        trip.pickup_location_id,
    )

    assert isinstance(row["pickup_datetime"], str)
    assert isinstance(row["dropoff_datetime"], str)

    # Verify repository handles ISO8601 timestamp strings from Kafka payloads.
    trip_repository.save_batch_from_dicts([row])

    # Confirm ClickHouse accepted the row.
    persisted = _wait_for_rows(
        ch_client,
        batch_id,
        expected=1,
    )

    assert len(persisted) == 1
