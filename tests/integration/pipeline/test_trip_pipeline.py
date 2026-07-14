"""
Integration: happy-path pipeline.

Parquet → domain pipeline → Kafka → consumer → ClickHouse.

sample_trips.parquet — 5 valid rows, all on 2024-01-15:
  Row 0: VendorID=1, PU=161 (Midtown Center),  DO=236, payment=Credit card
  Row 1: VendorID=2, PU=162 (Midtown East),    DO=186, payment=Cash
  Row 2: VendorID=1, PU=230 (Times Sq),        DO=161, payment=Credit card
  Row 3: VendorID=2, PU=132 (JFK Airport),     DO=163, payment=Credit card, rate=JFK
  Row 4: VendorID=1, PU=239 (Upper West Side), DO=237, payment=Cash
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


# Domain pipeline
def test_all_valid_rows_pass_domain_pipeline(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, invalid = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    assert len(valid) == EXPECTED_VALID
    assert len(invalid) == 0


def test_domain_pipeline_generates_unique_trip_ids(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    trip_ids = [t.trip_id for t in valid]
    assert len(set(trip_ids)) == EXPECTED_VALID


def test_domain_pipeline_normalises_vendor_ids(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    vendor_ids = {t.vendor_id for t in valid}
    assert "Creative Mobile Technologies" in vendor_ids
    assert "VeriFone Inc." in vendor_ids
    assert 1 not in vendor_ids


def test_domain_pipeline_enriches_zones(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    zones = {t.pickup_zone for t in valid}
    assert "Midtown Center" in zones
    assert "JFK Airport" in zones
    assert "Unknown" not in zones


def test_domain_pipeline_normalises_payment_type(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    payment_types = {t.payment.payment_type for t in valid}
    assert "Credit card" in payment_types
    assert "Cash" in payment_types


def test_domain_pipeline_applies_jfk_rate_code(domain_service, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    rate_codes = {t.rate_code for t in valid}
    assert "Standard" in rate_codes
    assert "JFK" in rate_codes


# Publish to Kafka
def test_valid_trips_published_without_error(domain_service, kafka_producer, settings, batch_id):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, batch_id, "sample_trips.parquet")
    kafka_producer.publish_batch(settings.kafka.topic, [t.to_dict() for t in valid])
    remaining = kafka_producer.flush()
    assert remaining == 0


# Persist to ClickHouse
def test_trips_persisted_to_clickhouse(domain_service, trip_repository, ch_client, cleanup_batch):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    assert len(persisted) == EXPECTED_VALID


def test_clickhouse_vendor_id_is_string(domain_service, trip_repository, ch_client, cleanup_batch):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    vendor_ids = {row[1] for row in persisted}
    assert "Creative Mobile Technologies" in vendor_ids
    assert 1 not in vendor_ids


def test_clickhouse_pickup_zone_enriched(domain_service, trip_repository, ch_client, cleanup_batch):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    pickup_zones = {row[2] for row in persisted}
    assert "Midtown Center" in pickup_zones
    assert "JFK Airport" in pickup_zones


def test_clickhouse_batch_metadata_stored(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    result = ch_client.execute(
        "SELECT DISTINCT batch_id, source_file FROM taxi.trips FINAL WHERE batch_id = %(bid)s",
        {"bid": cleanup_batch},
    )
    assert len(result) == 1
    assert result[0][0] == cleanup_batch
    assert result[0][1] == "sample_trips.parquet"


def test_clickhouse_total_amounts_correct(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    rows = read_parquet_rows(SAMPLE_PARQUET)
    valid, _ = domain_service.process_batch(rows, cleanup_batch, "sample_trips.parquet")
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid])

    persisted = wait_for_rows(ch_client, cleanup_batch, EXPECTED_VALID)
    total_amounts = sorted(float(row[5]) for row in persisted)
    assert abs(total_amounts[0] - 13.30) < 0.01
    assert abs(total_amounts[-1] - 66.05) < 0.01
