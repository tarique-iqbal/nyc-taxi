from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TypeAlias

from etl.domain.trip.exceptions import NegativeMoneyError, TripParseError
from etl.utils.json import JSONDict

RawValue: TypeAlias = str | int | float | Decimal | None

# Value objects
#
# Value objects are immutable (frozen=True) and validated on creation.
# Identity is by value, not reference (e.g., Money, Duration).


@dataclass(frozen=True)
class Money:
    """
    Represents a non-negative monetary amount in USD.

    Stored as Decimal to avoid floating-point rounding errors that
    accumulate across fare components.
    """

    amount: Decimal
    field_name: str = field(default="amount", compare=False)

    def __post_init__(self) -> None:
        if self.amount < Decimal("0"):
            raise NegativeMoneyError(self.field_name, float(self.amount))

    @classmethod
    def of(cls, value: float | str | Decimal, field_name: str = "amount") -> Money:
        try:
            decimal_value = Decimal(str(value)) if not isinstance(value, Decimal) else value
        except InvalidOperation as exc:
            raise TripParseError(field_name, value, "not a valid decimal") from exc
        return cls(amount=decimal_value, field_name=field_name)

    @classmethod
    def zero(cls) -> Money:
        return cls(amount=Decimal("0"))

    def __add__(self, other: Money) -> Money:
        return Money(amount=self.amount + other.amount)

    def __float__(self) -> float:
        return float(self.amount)


@dataclass(frozen=True)
class Distance:
    """Trip distance in miles. Zero is valid (e.g. cancelled trips)."""

    miles: float

    def __post_init__(self) -> None:
        if self.miles < 0:
            raise ValueError(f"Distance cannot be negative, got {self.miles}")

    @classmethod
    def of(cls, value: float | None) -> Distance:
        if value is None:
            return cls(miles=0.0)
        return cls(miles=float(value))


@dataclass(frozen=True)
class Duration:
    """Trip duration in seconds derived from pickup and dropoff datetimes."""

    seconds: int

    @property
    def minutes(self) -> float:
        return self.seconds / 60

    @property
    def hours(self) -> float:
        return self.seconds / 3600

    @classmethod
    def between(cls, pickup: datetime, dropoff: datetime) -> Duration:
        delta = dropoff - pickup
        return cls(seconds=int(delta.total_seconds()))


@dataclass(frozen=True)
class Location:
    """
    Enriched location -- a zone ID resolved to human-readable names.

    Before enrichment only location_id is known. After enrichment
    zone and borough are populated. unknown() is the safe fallback
    when a location_id cannot be resolved.
    """

    location_id: int
    zone: str
    borough: str

    @classmethod
    def unknown(cls, location_id: int) -> Location:
        return cls(location_id=location_id, zone="Unknown", borough="Unknown")

    @property
    def is_known(self) -> bool:
        return self.zone != "Unknown"


@dataclass(frozen=True)
class Payment:
    """
    All monetary components of a single trip, grouped into one value object.

    Each component is a Money instance and individually non-negative.
    total_amount is stored as-provided by the source data and is not
    recomputed from components to avoid masking upstream rounding.
    """

    payment_type: str
    fare_amount: Money
    extra: Money
    mta_tax: Money
    tip_amount: Money
    tolls_amount: Money
    improvement_surcharge: Money
    congestion_surcharge: Money
    airport_fee: Money
    total_amount: Money

    @classmethod
    def from_raw(cls, raw: dict[str, RawValue]) -> Payment:
        def _money(key: str) -> Money:
            return Money.of(raw.get(key) or 0, field_name=key)

        return cls(
            payment_type=str(raw.get("payment_type", "Unknown")),
            fare_amount=_money("fare_amount"),
            extra=_money("extra"),
            mta_tax=_money("mta_tax"),
            tip_amount=_money("tip_amount"),
            tolls_amount=_money("tolls_amount"),
            improvement_surcharge=_money("improvement_surcharge"),
            congestion_surcharge=_money("congestion_surcharge"),
            airport_fee=_money("airport_fee"),
            total_amount=_money("total_amount"),
        )


# Entities
#
# Entities have identity (trip_id) and mutable state. Trip is the
# aggregate root -- all state changes go through its methods.


@dataclass
class Zone:
    """
    Reference data for a TLC taxi zone.

    Loaded once at startup from data/reference/taxi_zone_lookup.csv
    via CsvZoneRepository. Used by the enricher to resolve location IDs.
    """

    location_id: int
    borough: str
    zone: str
    service_zone: str

    @classmethod
    def unknown(cls, location_id: int) -> Zone:
        return cls(
            location_id=location_id,
            borough="Unknown",
            zone="Unknown",
            service_zone="Unknown",
        )


@dataclass
class Trip:
    """
    Aggregate root for the trip domain.

    Lifecycle:
      1. Built from normalised raw row fields (vendor_id, datetimes, etc.)
      2. Enriched via enrich_zones() -- populates pickup/dropoff zone names
      3. Validated via validate() -- raises DomainError on rule violations

    trip_id is a deterministic SHA-256 hash generated by the deduplicator
    before Trip construction. The same source row always produces the same
    trip_id, making inserts idempotent on Kafka replay.
    """

    trip_id: str
    vendor_id: str
    pickup_datetime: datetime
    dropoff_datetime: datetime
    passenger_count: int
    distance: Distance
    duration: Duration
    pickup_location_id: int
    dropoff_location_id: int
    payment: Payment
    rate_code: str
    store_and_fwd_flag: str
    batch_id: str
    source_file: str

    pickup_zone: str = "Unknown"
    dropoff_zone: str = "Unknown"
    pickup_borough: str = "Unknown"
    dropoff_borough: str = "Unknown"

    ingested_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def enrich_zones(self, pickup: Zone, dropoff: Zone) -> None:
        """
        Populate zone names and boroughs from resolved Zone entities.

        Called by the enricher after zone lookup. Safe to call with
        Zone.unknown() -- the trip will have 'Unknown' labels but
        will not be rejected.
        """
        self.pickup_zone = pickup.zone
        self.pickup_borough = pickup.borough
        self.dropoff_zone = dropoff.zone
        self.dropoff_borough = dropoff.borough

    def to_dict(self) -> JSONDict:
        """
        Serialise to a flat dict for Kafka publishing and ClickHouse insertion.

        Decimal amounts are converted to float for JSON compatibility.
        Datetimes are converted to ISO strings for JSON compatibility.
        """
        p = self.payment
        return {
            "trip_id": self.trip_id,
            "vendor_id": self.vendor_id,
            "pickup_datetime": self.pickup_datetime.isoformat(),
            "dropoff_datetime": self.dropoff_datetime.isoformat(),
            "trip_duration_seconds": self.duration.seconds,
            "passenger_count": self.passenger_count,
            "trip_distance": self.distance.miles,
            "pickup_location_id": self.pickup_location_id,
            "dropoff_location_id": self.dropoff_location_id,
            "pickup_zone": self.pickup_zone,
            "dropoff_zone": self.dropoff_zone,
            "pickup_borough": self.pickup_borough,
            "dropoff_borough": self.dropoff_borough,
            "fare_amount": float(p.fare_amount),
            "extra": float(p.extra),
            "mta_tax": float(p.mta_tax),
            "tip_amount": float(p.tip_amount),
            "tolls_amount": float(p.tolls_amount),
            "improvement_surcharge": float(p.improvement_surcharge),
            "congestion_surcharge": float(p.congestion_surcharge),
            "airport_fee": float(p.airport_fee),
            "total_amount": float(p.total_amount),
            "payment_type": p.payment_type,
            "rate_code": self.rate_code,
            "store_and_fwd_flag": self.store_and_fwd_flag,
            "ingested_at": self.ingested_at.isoformat(),
            "batch_id": self.batch_id,
            "source_file": self.source_file,
        }


@dataclass
class TripAggregate:
    """
    Batch-level aggregate that holds the result of processing a raw batch.

    After process_batch() completes, valid_trips and invalid_records
    are populated and can be handed to the application layer for
    publishing and dead-lettering respectively.
    """

    batch_id: str
    source_file: str
    raw_row_count: int
    valid_trips: list[Trip] = field(default_factory=list)
    invalid_records: list[dict[str, object]] = field(default_factory=list)

    @property
    def valid_count(self) -> int:
        return len(self.valid_trips)

    @property
    def invalid_count(self) -> int:
        return len(self.invalid_records)

    @property
    def reject_rate(self) -> float:
        if self.raw_row_count == 0:
            return 0.0
        return self.invalid_count / self.raw_row_count
