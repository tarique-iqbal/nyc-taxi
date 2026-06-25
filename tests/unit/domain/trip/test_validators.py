from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from etl.domain.trip.exceptions import (
    InvalidPassengerCountError,
    InvalidPickupDatetimeError,
    InvalidTripDurationError,
    NegativeMoneyError,
)
from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip
from etl.domain.trip.validators import MAX_TRIP_DURATION_SECONDS, TLC_MIN_DATE, TripValidator


def _payment(**overrides: object) -> Payment:
    defaults = dict(
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
    defaults.update(overrides)
    return Payment(**defaults)


def _trip(**overrides: object) -> Trip:
    pickup = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    dropoff = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    defaults = dict(
        trip_id="abc123",
        vendor_id="Creative Mobile Technologies",
        pickup_datetime=pickup,
        dropoff_datetime=dropoff,
        passenger_count=2,
        distance=Distance.of(5.0),
        duration=Duration.between(pickup, dropoff),
        pickup_location_id=132,
        dropoff_location_id=161,
        payment=_payment(),
        rate_code="Standard",
        store_and_fwd_flag="No",
        batch_id="batch-1",
        source_file="test.parquet",
    )
    defaults.update(overrides)
    return Trip(**defaults)


# Valid trip
def test_valid_trip_passes_all_rules():
    TripValidator.validate(_trip())


# Duration
def test_zero_duration_rejected():
    pickup = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    trip = _trip(
        pickup_datetime=pickup,
        dropoff_datetime=pickup,
        duration=Duration(seconds=0),
    )
    with pytest.raises(InvalidTripDurationError) as exc_info:
        TripValidator.validate(trip)
    assert exc_info.value.seconds == 0


def test_negative_duration_rejected():
    trip = _trip(duration=Duration(seconds=-60))
    with pytest.raises(InvalidTripDurationError):
        TripValidator.validate(trip)


def test_duration_exactly_24h_rejected():
    trip = _trip(duration=Duration(seconds=MAX_TRIP_DURATION_SECONDS))
    with pytest.raises(InvalidTripDurationError) as exc_info:
        TripValidator.validate(trip)
    assert exc_info.value.seconds == MAX_TRIP_DURATION_SECONDS


def test_duration_just_under_24h_passes():
    trip = _trip(duration=Duration(seconds=MAX_TRIP_DURATION_SECONDS - 1))
    TripValidator.validate(trip)


# Passenger count
def test_zero_passengers_rejected():
    trip = _trip(passenger_count=0)
    with pytest.raises(InvalidPassengerCountError) as exc_info:
        TripValidator.validate(trip)
    assert exc_info.value.count == 0


def test_ten_passengers_rejected():
    trip = _trip(passenger_count=10)
    with pytest.raises(InvalidPassengerCountError) as exc_info:
        TripValidator.validate(trip)
    assert exc_info.value.count == 10


def test_one_passenger_passes():
    TripValidator.validate(_trip(passenger_count=1))


def test_nine_passengers_passes():
    TripValidator.validate(_trip(passenger_count=9))


# Pickup datetime
def test_pickup_before_tlc_min_date_rejected():
    before_tlc = datetime(2008, 12, 31, 23, 59, 59, tzinfo=UTC)
    pickup = before_tlc
    dropoff = datetime(2008, 12, 31, 23, 45, 0, tzinfo=UTC)
    trip = _trip(
        pickup_datetime=pickup,
        dropoff_datetime=dropoff,
        duration=Duration(seconds=900),
    )
    with pytest.raises(InvalidPickupDatetimeError):
        TripValidator.validate(trip)


def test_pickup_exactly_at_tlc_min_date_passes():
    pickup = TLC_MIN_DATE
    dropoff = datetime(2009, 1, 1, 0, 30, 0, tzinfo=UTC)
    trip = _trip(
        pickup_datetime=pickup,
        dropoff_datetime=dropoff,
        duration=Duration.between(pickup, dropoff),
    )
    TripValidator.validate(trip)


def test_future_pickup_rejected():
    from datetime import timedelta
    far_future = datetime.now(UTC) + timedelta(days=365)
    trip = _trip(
        pickup_datetime=far_future,
        dropoff_datetime=far_future + timedelta(minutes=30),
        duration=Duration(seconds=1800),
    )
    with pytest.raises(InvalidPickupDatetimeError):
        TripValidator.validate(trip)


# Money
def test_negative_fare_amount_raises_at_construction():
    with pytest.raises(NegativeMoneyError) as exc_info:
        Money.of(-1.0, "fare_amount")
    assert exc_info.value.field == "fare_amount"
    assert exc_info.value.amount == -1.0


def test_negative_tip_raises_at_construction():
    with pytest.raises(NegativeMoneyError) as exc_info:
        Money.of(-0.01, "tip_amount")
    assert "tip_amount" in str(exc_info.value)


def test_zero_fare_passes():
    payment = _payment(fare_amount=Money.of(0.0, "fare_amount"))
    TripValidator.validate(_trip(payment=payment))


def test_money_addition():
    a = Money.of(5.0)
    b = Money.of(3.0)
    result = a + b
    assert result.amount == Decimal("8.0")
