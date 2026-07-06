from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from etl.utils.json import JSONDict


class ProcessingStage(StrEnum):
    """The pipeline stage at which a trip event was raised."""

    PARSING = "parsing"
    NORMALIZATION = "normalization"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    DEDUPLICATION = "deduplication"
    PERSIST = "persist"


@dataclass(frozen=True)
class TripCreated:
    """
    Raised when a raw row is successfully parsed into a Trip.

    This is the earliest lifecycle event -- the trip has been constructed
    from raw Parquet data but has not yet been normalised, enriched, or
    validated.
    """

    trip_id: str
    vendor_id: str
    batch_id: str
    source_file: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class TripEnriched:
    """
    Raised when zone names are successfully resolved for a trip.

    pickup_zone and dropoff_zone may still be 'Unknown' if the location
    ID was not in the zone lookup -- the enricher never rejects a trip
    for a missing zone.
    """

    trip_id: str
    batch_id: str
    pickup_zone: str
    dropoff_zone: str
    pickup_borough: str
    dropoff_borough: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class TripValidated:
    """
    Raised when a trip passes all business rule checks.

    A validated trip is ready to be published to Kafka.
    """

    trip_id: str
    batch_id: str
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True)
class InvalidTripDetected:
    """
    Raised when a trip fails at any stage of the domain pipeline.

    Carries the original raw record so the dead letter service can
    preserve it for inspection and replay. The stage field identifies
    exactly where in the pipeline the failure occurred.

    error_type is the exception class name for grouping in dashboards.
    """

    stage: ProcessingStage
    error_message: str
    error_type: str
    original_record: JSONDict
    batch_id: str
    source_file: str
    trip_id: str | None = None
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        stage: ProcessingStage,
        original_record: JSONDict,
        batch_id: str,
        source_file: str,
        trip_id: str | None = None,
    ) -> InvalidTripDetected:
        return cls(
            stage=stage,
            error_message=str(exc),
            error_type=type(exc).__name__,
            original_record=original_record,
            batch_id=batch_id,
            source_file=source_file,
            trip_id=trip_id,
        )
