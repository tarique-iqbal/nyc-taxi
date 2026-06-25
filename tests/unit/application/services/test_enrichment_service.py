from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from etl.application.services.enrichment_service import EnrichmentService
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip, Zone


def _make_zone_repository(is_loaded: bool = True, zone_count: int = 265) -> MagicMock:
    repo = MagicMock()
    repo.is_loaded = is_loaded
    repo.zone_count = zone_count
    repo.get_by_id.return_value = Zone(
        location_id=161,
        borough="Manhattan",
        zone="Midtown Center",
        service_zone="Yellow Zone",
    )
    return repo


def _make_trip(trip_id: str = "abc123") -> Trip:
    pickup = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    dropoff = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    payment = Payment(
        payment_type="Credit card",
        fare_amount=Money.of(10.0, "fare_amount"),
        extra=Money.zero(),
        mta_tax=Money.of(0.5, "mta_tax"),
        tip_amount=Money.of(2.0, "tip_amount"),
        tolls_amount=Money.zero(),
        improvement_surcharge=Money.of(0.3, "improvement_surcharge"),
        congestion_surcharge=Money.of(2.5, "congestion_surcharge"),
        airport_fee=Money.zero(),
        total_amount=Money.of(15.3, "total_amount"),
    )
    return Trip(
        trip_id=trip_id,
        vendor_id="Creative Mobile Technologies",
        pickup_datetime=pickup,
        dropoff_datetime=dropoff,
        passenger_count=2,
        distance=Distance.of(3.5),
        duration=Duration.between(pickup, dropoff),
        pickup_location_id=161,
        dropoff_location_id=236,
        payment=payment,
        rate_code="Standard",
        store_and_fwd_flag="No",
        batch_id="batch-1",
        source_file="test.parquet",
    )


# ensure_loaded
def test_ensure_loaded_passes_when_repo_is_loaded():
    repo = _make_zone_repository(is_loaded=True)
    svc = EnrichmentService(zone_repository=repo)
    svc.ensure_loaded()  # should not raise


def test_ensure_loaded_raises_when_repo_not_loaded():
    repo = _make_zone_repository(is_loaded=False)
    svc = EnrichmentService(zone_repository=repo)
    with pytest.raises(RuntimeError, match="ZoneRepository is not loaded"):
        svc.ensure_loaded()


def test_ensure_loaded_passes_when_is_loaded_attribute_absent():
    repo = MagicMock(spec=[])  # no is_loaded attribute
    svc = EnrichmentService(zone_repository=repo)
    svc.ensure_loaded()  # getattr defaults to True when attribute missing


# enrich_batch
def test_enrich_batch_populates_zone_fields():
    repo = _make_zone_repository()
    repo.get_by_id.side_effect = lambda loc_id: Zone(
        location_id=loc_id,
        borough="Manhattan",
        zone="Test Zone",
        service_zone="Yellow Zone",
    )
    svc = EnrichmentService(zone_repository=repo)
    trip = _make_trip()

    svc.enrich_batch([trip])

    assert trip.pickup_zone == "Test Zone"
    assert trip.pickup_borough == "Manhattan"
    assert trip.dropoff_zone == "Test Zone"
    assert trip.dropoff_borough == "Manhattan"


def test_enrich_batch_calls_enricher_for_each_trip():
    repo = _make_zone_repository()
    svc = EnrichmentService(zone_repository=repo)
    trips = [_make_trip(f"id-{i}") for i in range(3)]

    svc.enrich_batch(trips)

    # get_by_id called twice per trip (pickup + dropoff)
    assert repo.get_by_id.call_count == 6


def test_enrich_batch_empty_list_is_safe():
    repo = _make_zone_repository()
    svc = EnrichmentService(zone_repository=repo)
    svc.enrich_batch([])
    repo.get_by_id.assert_not_called()


def test_enrich_batch_unknown_zone_does_not_raise():
    repo = _make_zone_repository()
    repo.get_by_id.return_value = Zone.unknown(999)
    svc = EnrichmentService(zone_repository=repo)
    trip = _make_trip()

    svc.enrich_batch([trip])  # should not raise

    assert trip.pickup_zone == "Unknown"
    assert trip.pickup_borough == "Unknown"


# reload
def test_reload_calls_load_on_repository():
    repo = _make_zone_repository()
    svc = EnrichmentService(zone_repository=repo)
    svc.reload()
    repo.load.assert_called_once()


def test_reload_does_nothing_if_repo_has_no_load_method():
    repo = MagicMock(spec=["get_by_id", "load_all", "is_loaded", "zone_count"])
    del repo.load  # ensure load attribute doesn't exist
    repo_no_load = MagicMock()
    del repo_no_load.load
    # Use spec to create a mock without load
    repo2 = MagicMock(spec=["get_by_id", "load_all"])
    svc = EnrichmentService(zone_repository=repo2)
    svc.reload()  # should not raise


# zone_count
def test_zone_count_returns_repository_count():
    repo = _make_zone_repository(zone_count=265)
    svc = EnrichmentService(zone_repository=repo)
    assert svc.zone_count == 265


def test_zone_count_returns_zero_when_attribute_missing():
    repo = MagicMock(spec=["get_by_id", "load_all"])
    svc = EnrichmentService(zone_repository=repo)
    assert svc.zone_count == 0
