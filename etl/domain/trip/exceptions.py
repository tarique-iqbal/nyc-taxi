from __future__ import annotations


class DomainError(Exception):
    """
    Base class for all domain exceptions.

    Domain exceptions represent business rule violations -- the data
    itself is wrong. They are distinct from infrastructure exceptions
    (network errors, timeouts) which represent system failures.

    The domain service catches all DomainError subclasses and wraps
    them in InvalidTripDetected events rather than letting them propagate.
    """


class InvalidTripDurationError(DomainError):
    """
    Raised when trip duration is outside the acceptable range.

    Rules:
      - Duration must be positive (dropoff after pickup).
      - Duration must be under 24 hours (86400 seconds).
        Trips longer than 24 hours are data errors, not real trips.
    """

    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        super().__init__(
            f"Trip duration {seconds}s is invalid. Must be > 0 and < 86400 (24 hours)."
        )


class NegativeMoneyError(DomainError):
    """
    Raised when a fare component is negative.

    Fare amounts, tips, tolls, and surcharges must all be >= 0.
    Negative values indicate upstream data corruption or entry errors.
    """

    def __init__(self, field: str, amount: float) -> None:
        self.field = field
        self.amount = amount
        super().__init__(
            f"Fare field '{field}' has negative value {amount}. All monetary amounts must be >= 0."
        )


class InvalidPassengerCountError(DomainError):
    """
    Raised when passenger count is outside the valid TLC range.

    Rules:
      - Minimum: 1 (null values are normalised to 1 before validation).
      - Maximum: 9 (largest yellow cab capacity per TLC rules).
    """

    def __init__(self, count: int) -> None:
        self.count = count
        super().__init__(f"Passenger count {count} is invalid. Must be between 1 and 9.")


class InvalidPickupDatetimeError(DomainError):
    """
    Raised when pickup datetime is outside the valid TLC operating range.

    Rules:
      - Must be >= TLC_MIN_DATE (2009-01-01, first year of TLC data).
      - Must not be in the future (allows 1 day tolerance for timezone skew).
    """

    def __init__(self, pickup_datetime: object, reason: str) -> None:
        self.pickup_datetime = pickup_datetime
        super().__init__(f"Pickup datetime '{pickup_datetime}' is invalid: {reason}")


class ZoneNotFoundError(DomainError):
    """
    Raised when a location ID cannot be resolved to a zone.

    In practice the enricher falls back to Location.unknown() instead
    of raising this exception, so it only surfaces if a caller explicitly
    requires a resolved zone and receives an unknown record.
    """

    def __init__(self, location_id: int) -> None:
        self.location_id = location_id
        super().__init__(f"Zone not found for location_id={location_id}.")


class TripParseError(DomainError):
    """
    Raised when a raw row cannot be parsed into a Trip at all.

    This is a lower-level failure than validation -- the row is
    missing required fields or has wrong types before business
    rules can even be checked.
    """

    def __init__(self, field: str, raw_value: object, reason: str) -> None:
        self.field = field
        self.raw_value = raw_value
        super().__init__(f"Cannot parse field '{field}' with value '{raw_value}': {reason}")
