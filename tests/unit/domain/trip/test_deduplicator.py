from __future__ import annotations

from datetime import UTC, datetime

from etl.domain.trip.deduplicator import TripDeduplicator, generate_trip_id
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip

_PICKUP = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
_DROPOFF = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)


def _make_trip(
    trip_id: str,
    vendor_id: str = "Creative Mobile Technologies",
) -> Trip:
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
        vendor_id=vendor_id,
        pickup_datetime=_PICKUP,
        dropoff_datetime=_DROPOFF,
        passenger_count=1,
        distance=Distance.of(3.5),
        duration=Duration.between(_PICKUP, _DROPOFF),
        pickup_location_id=132,
        dropoff_location_id=161,
        payment=payment,
        rate_code="Standard",
        store_and_fwd_flag="No",
        batch_id="batch-1",
        source_file="test.parquet",
    )


# generate_trip_id
def test_generate_trip_id_is_deterministic():
    id_a = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    id_b = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    assert id_a == id_b


def test_generate_trip_id_different_vendor_differs():
    id_a = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    id_b = generate_trip_id("VER", _PICKUP, _DROPOFF, 132)
    assert id_a != id_b


def test_generate_trip_id_different_pickup_differs():
    other_pickup = datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC)
    id_a = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    id_b = generate_trip_id("CMT", other_pickup, _DROPOFF, 132)
    assert id_a != id_b


def test_generate_trip_id_different_dropoff_differs():
    other_dropoff = datetime(2024, 1, 15, 11, 30, 0, tzinfo=UTC)
    id_a = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    id_b = generate_trip_id("CMT", _PICKUP, other_dropoff, 132)
    assert id_a != id_b


def test_generate_trip_id_different_location_differs():
    id_a = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    id_b = generate_trip_id("CMT", _PICKUP, _DROPOFF, 161)
    assert id_a != id_b


def test_generate_trip_id_returns_sha256_hex():
    trip_id = generate_trip_id("CMT", _PICKUP, _DROPOFF, 132)
    assert len(trip_id) == 64
    assert all(c in "0123456789abcdef" for c in trip_id)


# TripDeduplicator
def test_no_duplicates_returns_all_trips():
    dedup = TripDeduplicator()
    trips = [_make_trip("id-1"), _make_trip("id-2"), _make_trip("id-3")]
    unique, duplicates = dedup.deduplicate(trips)
    assert len(unique) == 3
    assert len(duplicates) == 0


def test_single_duplicate_kept_first_removed_second():
    dedup = TripDeduplicator()
    trip_a = _make_trip("same-id")
    trip_b = _make_trip("same-id")
    unique, duplicates = dedup.deduplicate([trip_a, trip_b])
    assert len(unique) == 1
    assert unique[0] is trip_a
    assert len(duplicates) == 1
    assert duplicates[0] is trip_b


def test_multiple_duplicates_of_same_id():
    dedup = TripDeduplicator()
    trips = [_make_trip("dup") for _ in range(4)]
    unique, duplicates = dedup.deduplicate(trips)
    assert len(unique) == 1
    assert len(duplicates) == 3


def test_mixed_unique_and_duplicates():
    dedup = TripDeduplicator()
    trips = [
        _make_trip("id-1"),
        _make_trip("id-2"),
        _make_trip("id-1"),  # duplicate
        _make_trip("id-3"),
        _make_trip("id-2"),  # duplicate
    ]
    unique, duplicates = dedup.deduplicate(trips)
    assert len(unique) == 3
    assert len(duplicates) == 2
    unique_ids = [t.trip_id for t in unique]
    assert unique_ids == ["id-1", "id-2", "id-3"]


def test_empty_list_returns_empty():
    dedup = TripDeduplicator()
    unique, duplicates = dedup.deduplicate([])
    assert unique == []
    assert duplicates == []


def test_single_trip_returns_unique():
    dedup = TripDeduplicator()
    trip = _make_trip("only-one")
    unique, duplicates = dedup.deduplicate([trip])
    assert len(unique) == 1
    assert len(duplicates) == 0


def test_order_preserved_in_unique():
    dedup = TripDeduplicator()
    trips = [_make_trip(f"id-{i}") for i in range(5)]
    unique, _ = dedup.deduplicate(trips)
    assert [t.trip_id for t in unique] == [f"id-{i}" for i in range(5)]
