"""
Integration test: ClickHouse materialized views are populated on insert.

Requires running ClickHouse with the schema and MVs applied (make up + make schema).
Kafka is not required -- trips are inserted directly via ClickHouseTripRepository.

Fixture data (sample_trips.parquet) -- all on 2024-01-15:
  Row 0: VendorID=1, PU=161 (Midtown Center,  Manhattan),    DO=236, payment=Credit card, total=19.80
  Row 1: VendorID=2, PU=162 (Midtown East,    Manhattan),    DO=186, payment=Cash,        total=25.80
  Row 2: VendorID=1, PU=230 (Times Sq,        Manhattan),    DO=161, payment=Credit card, total=13.30
  Row 3: VendorID=2, PU=132 (JFK Airport,     Queens),       DO=163, payment=Credit card, total=66.05
  Row 4: VendorID=1, PU=239 (Upper West Side, Manhattan),    DO=237, payment=Cash,        total=18.30

Expected aggregates from fixture:
  Total trips:      5
  Total fare sum:   143.25
  Vendor 1 trips:   3  (rows 0, 2, 4)
  Vendor 2 trips:   2  (rows 1, 3)
  Credit card:      3  (rows 0, 2, 3)
  Cash:             2  (rows 1, 4)
  Pickup boroughs:  Manhattan (4 trips), Queens (1 trip -- JFK)
  Dropoff boroughs: Manhattan (5 trips)

Strategy for isolation:
  ClickHouse AggregatingMergeTree MVs cannot have rows deleted mid-test.
  Tests therefore compare MV results against raw trips table counts for
  the same time window, ensuring MV == raw regardless of pre-existing
  fixture data from prior test runs. Hard counts (>= N) guard the minimum.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    SAMPLE_PARQUET,
    requires_clickhouse,
)

pytestmark = [requires_clickhouse, pytest.mark.integration]

FIXTURE_DATE = "2024-01-15"
FIXTURE_TOTAL_FARE = 143.25
FIXTURE_TRIP_COUNT = 5
FIXTURE_VENDOR1_COUNT = 3
FIXTURE_VENDOR2_COUNT = 2
FIXTURE_CREDIT_CARD_COUNT = 3
FIXTURE_CASH_COUNT = 2


def _read_parquet_rows(path: Path) -> list[dict]:
    from etl.infrastructure.storage.parquet_reader import ParquetReader

    rows = []
    for batch in ParquetReader(path=path, batch_size=100).iter_batches():
        rows.extend(batch)
    return rows


def _raw_count_for_date(ch_client, date: str) -> int:
    result = ch_client.execute(
        "SELECT count() FROM taxi.trips FINAL WHERE toDate(pickup_datetime) = %(date)s",
        {"date": date},
    )
    return int(result[0][0]) if result else 0


def _raw_fare_sum_for_date(ch_client, date: str) -> float:
    result = ch_client.execute(
        "SELECT sum(total_amount) FROM taxi.trips FINAL WHERE toDate(pickup_datetime) = %(date)s",
        {"date": date},
    )
    return float(result[0][0]) if result else 0.0


def _insert_fixture(domain_service, trip_repository, batch_id: str) -> None:
    raw_rows = _read_parquet_rows(SAMPLE_PARQUET)
    valid_trips, _ = domain_service.process_batch(
        raw_rows=raw_rows,
        batch_id=batch_id,
        source_file="sample_trips.parquet",
    )
    trip_repository.save_batch_from_dicts([t.to_dict() for t in valid_trips])


# trips_hourly_mv
def test_hourly_mv_count_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    """
    countMerge from trips_hourly_mv for 2024-01-15 must equal count
    from raw trips table for the same date.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_count = _raw_count_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT countMerge(trip_count) FROM taxi.trips_hourly_mv WHERE toDate(hour) = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_count = int(mv_result[0][0]) if mv_result and mv_result[0][0] else 0

    assert mv_count == raw_count
    assert mv_count >= FIXTURE_TRIP_COUNT


def test_hourly_mv_five_distinct_hour_buckets(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    Each fixture trip falls in a different hour (10-14).
    trips_hourly_mv should have at least 5 rows for 2024-01-15.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    result = ch_client.execute(
        "SELECT count(DISTINCT hour) FROM taxi.trips_hourly_mv WHERE toDate(hour) = %(date)s",
        {"date": FIXTURE_DATE},
    )
    distinct_hours = int(result[0][0]) if result else 0
    assert distinct_hours >= 5


def test_hourly_mv_vendor_split(domain_service, trip_repository, ch_client, cleanup_batch):
    """
    Vendor-level trip counts from hourly MV match raw table for 2024-01-15.
    Vendor 1: 3 trips (rows 0, 2, 4), Vendor 2: 2 trips (rows 1, 3).
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    mv_result = ch_client.execute(
        "SELECT vendor_id, countMerge(trip_count) "
        "FROM taxi.trips_hourly_mv "
        "WHERE toDate(hour) = %(date)s "
        "GROUP BY vendor_id "
        "ORDER BY vendor_id",
        {"date": FIXTURE_DATE},
    )
    raw_result = ch_client.execute(
        "SELECT vendor_id, count() FROM taxi.trips FINAL "
        "WHERE toDate(pickup_datetime) = %(date)s "
        "GROUP BY vendor_id ORDER BY vendor_id",
        {"date": FIXTURE_DATE},
    )

    mv_by_vendor = {row[0]: int(row[1]) for row in mv_result}
    raw_by_vendor = {row[0]: int(row[1]) for row in raw_result}

    assert mv_by_vendor == raw_by_vendor

    assert mv_by_vendor.get("Creative Mobile Technologies", 0) >= FIXTURE_VENDOR1_COUNT
    assert mv_by_vendor.get("VeriFone Inc.", 0) >= FIXTURE_VENDOR2_COUNT


# trips_daily_mv
def test_daily_mv_count_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_count = _raw_count_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT countMerge(trip_count) FROM taxi.trips_daily_mv WHERE day = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_count = int(mv_result[0][0]) if mv_result and mv_result[0][0] else 0

    assert mv_count == raw_count
    assert mv_count >= FIXTURE_TRIP_COUNT


def test_daily_mv_fare_sum_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    """
    sumMerge(total_fare) from trips_daily_mv matches sum from raw trips.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_sum = _raw_fare_sum_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT sumMerge(total_fare) FROM taxi.trips_daily_mv WHERE day = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_sum = float(mv_result[0][0]) if mv_result and mv_result[0][0] else 0.0

    assert abs(mv_sum - raw_sum) < 0.01
    assert mv_sum >= FIXTURE_TOTAL_FARE


def test_daily_mv_payment_type_split(domain_service, trip_repository, ch_client, cleanup_batch):
    """
    Payment type split in daily MV matches raw table.
    Credit card: 3 trips, Cash: 2 trips.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    mv_result = ch_client.execute(
        "SELECT payment_type, countMerge(trip_count) "
        "FROM taxi.trips_daily_mv "
        "WHERE day = %(date)s "
        "GROUP BY payment_type "
        "ORDER BY payment_type",
        {"date": FIXTURE_DATE},
    )
    raw_result = ch_client.execute(
        "SELECT payment_type, count() FROM taxi.trips FINAL "
        "WHERE toDate(pickup_datetime) = %(date)s "
        "GROUP BY payment_type ORDER BY payment_type",
        {"date": FIXTURE_DATE},
    )

    mv_by_payment = {row[0]: int(row[1]) for row in mv_result}
    raw_by_payment = {row[0]: int(row[1]) for row in raw_result}

    assert mv_by_payment == raw_by_payment

    assert mv_by_payment.get("Credit card", 0) >= FIXTURE_CREDIT_CARD_COUNT
    assert mv_by_payment.get("Cash", 0) >= FIXTURE_CASH_COUNT


# trips_by_borough_mv
def test_borough_mv_count_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_count = _raw_count_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT countMerge(trip_count) FROM taxi.trips_by_borough_mv WHERE day = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_count = int(mv_result[0][0]) if mv_result and mv_result[0][0] else 0

    assert mv_count == raw_count
    assert mv_count >= FIXTURE_TRIP_COUNT


def test_borough_mv_manhattan_and_queens_present(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    4 trips originate from Manhattan, 1 from Queens (JFK).
    Both boroughs must appear as pickup_borough in trips_by_borough_mv.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    result = ch_client.execute(
        "SELECT pickup_borough, countMerge(trip_count) "
        "FROM taxi.trips_by_borough_mv "
        "WHERE day = %(date)s "
        "GROUP BY pickup_borough",
        {"date": FIXTURE_DATE},
    )
    by_borough = {row[0]: int(row[1]) for row in result}

    assert "Manhattan" in by_borough
    assert "Queens" in by_borough
    assert by_borough["Manhattan"] >= 4
    assert by_borough["Queens"] >= 1


def test_borough_mv_all_dropoffs_in_manhattan(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """All 5 fixture trips drop off in Manhattan."""
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    result = ch_client.execute(
        "SELECT dropoff_borough, countMerge(trip_count) "
        "FROM taxi.trips_by_borough_mv "
        "WHERE day = %(date)s "
        "GROUP BY dropoff_borough",
        {"date": FIXTURE_DATE},
    )
    by_dropoff = {row[0]: int(row[1]) for row in result}

    assert "Manhattan" in by_dropoff
    assert by_dropoff["Manhattan"] >= FIXTURE_TRIP_COUNT


# trips_by_payment_mv
def test_payment_mv_count_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_count = _raw_count_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT countMerge(trip_count) FROM taxi.trips_by_payment_mv WHERE day = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_count = int(mv_result[0][0]) if mv_result and mv_result[0][0] else 0

    assert mv_count == raw_count
    assert mv_count >= FIXTURE_TRIP_COUNT


def test_payment_mv_credit_card_avg_tip(domain_service, trip_repository, ch_client, cleanup_batch):
    """
    avgMerge(avg_tip) from payment MV matches avg from raw table for credit card.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_result = ch_client.execute(
        "SELECT avg(tip_amount) FROM taxi.trips FINAL "
        "WHERE toDate(pickup_datetime) = %(date)s AND payment_type = 'Credit card'",
        {"date": FIXTURE_DATE},
    )
    raw_avg_tip = float(raw_result[0][0]) if raw_result and raw_result[0][0] else 0.0

    mv_result = ch_client.execute(
        "SELECT avgMerge(avg_tip) "
        "FROM taxi.trips_by_payment_mv "
        "WHERE day = %(date)s AND payment_type = 'Credit card'",
        {"date": FIXTURE_DATE},
    )
    mv_avg_tip = float(mv_result[0][0]) if mv_result and mv_result[0][0] else 0.0

    assert abs(mv_avg_tip - raw_avg_tip) < 0.01


# trips_by_zone_mv
def test_zone_mv_count_matches_raw(domain_service, trip_repository, ch_client, cleanup_batch):
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    raw_count = _raw_count_for_date(ch_client, FIXTURE_DATE)

    mv_result = ch_client.execute(
        "SELECT countMerge(trip_count) FROM taxi.trips_by_zone_mv WHERE day = %(date)s",
        {"date": FIXTURE_DATE},
    )
    mv_count = int(mv_result[0][0]) if mv_result and mv_result[0][0] else 0

    assert mv_count == raw_count
    assert mv_count >= FIXTURE_TRIP_COUNT


def test_zone_mv_known_pickup_zones_present(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    Specific pickup zones from the fixture must appear in trips_by_zone_mv.
    PU=161 -> Midtown Center, PU=132 -> JFK Airport.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    result = ch_client.execute(
        "SELECT pickup_zone FROM taxi.trips_by_zone_mv WHERE day = %(date)s GROUP BY pickup_zone",
        {"date": FIXTURE_DATE},
    )
    zones = {row[0] for row in result}

    assert "Midtown Center" in zones
    assert "JFK Airport" in zones
    assert "Times Sq/Theatre District" in zones


def test_zone_mv_avg_fare_for_jfk_exceeds_standard(
    domain_service, trip_repository, ch_client, cleanup_batch
):
    """
    JFK Airport trips carry airport fees and tolls.
    avgMerge(avg_fare) for JFK must exceed avgMerge for a standard zone.
    """
    _insert_fixture(domain_service, trip_repository, cleanup_batch)

    result = ch_client.execute(
        "SELECT pickup_zone, avgMerge(avg_fare) "
        "FROM taxi.trips_by_zone_mv "
        "WHERE day = %(date)s AND pickup_zone IN ('JFK Airport', 'Midtown Center') "
        "GROUP BY pickup_zone",
        {"date": FIXTURE_DATE},
    )
    avg_by_zone = {row[0]: float(row[1]) for row in result}

    assert "JFK Airport" in avg_by_zone
    assert "Midtown Center" in avg_by_zone
    assert avg_by_zone["JFK Airport"] > avg_by_zone["Midtown Center"]
