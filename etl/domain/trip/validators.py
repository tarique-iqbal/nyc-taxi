from __future__ import annotations

from datetime import UTC, datetime

from etl.domain.trip.exceptions import (
    InvalidPassengerCountError,
    InvalidPickupDatetimeError,
    InvalidTripDurationError,
    NegativeMoneyError,
)
from etl.domain.trip.models import Trip

# TLC trips before Jan 2009 are invalid regardless of source.
TLC_MIN_DATE: datetime = datetime(2009, 1, 1, tzinfo=UTC)

# Maximum valid trip duration: 24 hours in seconds.
MAX_TRIP_DURATION_SECONDS: int = 86_400

MIN_PASSENGER_COUNT: int = 1
MAX_PASSENGER_COUNT: int = 9

# Monetary fields checked for negative values.
MONEY_FIELDS: list[str] = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
]


class TripValidator:
    """
    Applies business rule validation to a Trip entity.

    Called after normalisation and enrichment. Each check raises a
    DomainError subclass on failure -- never returns False. The domain
    service catches these and wraps them in InvalidTripDetected events.

    All methods are static -- TripValidator holds no state.
    """

    @staticmethod
    def validate(trip: Trip) -> None:
        """
        Run all validation rules against the given Trip.

        Raises the first DomainError encountered. Rules are ordered from
        cheapest to most specific.
        """
        TripValidator._validate_duration(trip)
        TripValidator._validate_passenger_count(trip)
        TripValidator._validate_pickup_datetime(trip)
        TripValidator._validate_money(trip)

    @staticmethod
    def _validate_duration(trip: Trip) -> None:
        seconds = trip.duration.seconds
        if seconds <= 0 or seconds >= MAX_TRIP_DURATION_SECONDS:
            raise InvalidTripDurationError(seconds)

    @staticmethod
    def _validate_passenger_count(trip: Trip) -> None:
        count = trip.passenger_count
        if count < MIN_PASSENGER_COUNT or count > MAX_PASSENGER_COUNT:
            raise InvalidPassengerCountError(count)

    @staticmethod
    def _validate_pickup_datetime(trip: Trip) -> None:
        pickup = trip.pickup_datetime

        # Make offset-aware for comparison if needed.
        if pickup.tzinfo is None:
            pickup = pickup.replace(tzinfo=UTC)

        if pickup < TLC_MIN_DATE:
            raise InvalidPickupDatetimeError(
                trip.pickup_datetime,
                f"before TLC minimum date {TLC_MIN_DATE.date()}",
            )

        now = datetime.now(UTC)
        # Allow 1 day of tolerance for timezone edge cases.
        tolerance_seconds = 86_400
        if (pickup - now).total_seconds() > tolerance_seconds:
            raise InvalidPickupDatetimeError(
                trip.pickup_datetime,
                "is in the future",
            )

    @staticmethod
    def _validate_money(trip: Trip) -> None:
        p = trip.payment
        money_components = {
            "fare_amount": p.fare_amount,
            "extra": p.extra,
            "mta_tax": p.mta_tax,
            "tip_amount": p.tip_amount,
            "tolls_amount": p.tolls_amount,
            "improvement_surcharge": p.improvement_surcharge,
            "congestion_surcharge": p.congestion_surcharge,
            "airport_fee": p.airport_fee,
            "total_amount": p.total_amount,
        }
        for name, money in money_components.items():
            # Money.__post_init__ already guards against negatives at
            # construction time. This re-check catches any Money objects
            # constructed without going through Money.of().
            if money.amount < 0:
                raise NegativeMoneyError(name, float(money.amount))
