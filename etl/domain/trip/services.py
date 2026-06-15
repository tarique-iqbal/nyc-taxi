from __future__ import annotations

import logging
from datetime import UTC, datetime

from etl.domain.trip.deduplicator import TripDeduplicator, generate_trip_id
from etl.domain.trip.enrichers import TripEnricher
from etl.domain.trip.events import InvalidTripDetected, ProcessingStage
from etl.domain.trip.exceptions import DomainError, TripParseError
from etl.domain.trip.models import Distance, Duration, Payment, Trip
from etl.domain.trip.normalizers import TripNormalizer
from etl.domain.trip.repositories import ZoneRepository
from etl.domain.trip.validators import TripValidator

logger = logging.getLogger(__name__)


def _parse_datetime(value: object, field_name: str) -> datetime:
    """
    Coerce a raw value to a UTC-aware datetime.

    PyArrow yields Parquet timestamp columns as pandas Timestamps or
    Python datetimes. Both cases are handled here.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            import pandas as pd

            dt = pd.Timestamp(value).to_pydatetime()
        except Exception as exc:
            raise TripParseError(field_name, value, f"cannot convert to datetime: {exc}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _build_trip(
    normalised: dict[str, object],
    batch_id: str,
    source_file: str,
) -> Trip:
    """
    Construct a Trip entity from a normalised raw row.

    Raises TripParseError if required fields are missing or unparseable.
    """
    pickup_dt = _parse_datetime(normalised.get("pickup_datetime"), "pickup_datetime")
    dropoff_dt = _parse_datetime(normalised.get("dropoff_datetime"), "dropoff_datetime")

    try:
        pickup_location_id = int(normalised["pickup_location_id"])  # type: ignore[arg-type]
        dropoff_location_id = int(normalised["dropoff_location_id"])  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError) as exc:
        raise TripParseError("location_id", normalised.get("pickup_location_id"), str(exc)) from exc

    trip_id = generate_trip_id(
        vendor_id=str(normalised.get("vendor_id", "")),
        pickup_datetime=pickup_dt,
        dropoff_datetime=dropoff_dt,
        pickup_location_id=pickup_location_id,
    )

    return Trip(
        trip_id=trip_id,
        vendor_id=str(normalised.get("vendor_id", "Unknown")),
        pickup_datetime=pickup_dt,
        dropoff_datetime=dropoff_dt,
        passenger_count=int(normalised.get("passenger_count", 1)),  # type: ignore[arg-type]
        distance=Distance.of(normalised.get("trip_distance")),
        duration=Duration.between(pickup_dt, dropoff_dt),
        pickup_location_id=pickup_location_id,
        dropoff_location_id=dropoff_location_id,
        payment=Payment.from_raw(normalised),
        rate_code=str(normalised.get("rate_code", "Unknown")),
        store_and_fwd_flag=str(normalised.get("store_and_fwd_flag", "Unknown")),
        batch_id=batch_id,
        source_file=source_file,
    )


class TripDomainService:
    """
    Orchestrates the full domain pipeline for a batch of raw rows.

    Pipeline per row:
      1. Normalise  -- maps int codes to strings, fills nulls
      2. Parse      -- constructs Trip entity
      3. Enrich     -- resolves location IDs to zone names
      4. Validate   -- applies business rules
      5. Deduplicate -- removes within-batch duplicates

    Any DomainError at steps 1-4 wraps the row in InvalidTripDetected
    and continues to the next row. The pipeline never raises -- it
    accumulates failures as events.

    Returns:
        (valid_trips, invalid_events)
    """

    def __init__(
        self,
        zone_repository: ZoneRepository,
        deduplicator: TripDeduplicator | None = None,
    ) -> None:
        self._enricher = TripEnricher(zone_repository)
        self._deduplicator = deduplicator or TripDeduplicator()

    def process_batch(
        self,
        raw_rows: list[dict[str, object]],
        batch_id: str,
        source_file: str,
    ) -> tuple[list[Trip], list[InvalidTripDetected]]:
        """
        Process a list of raw Parquet rows through the domain pipeline.

        Args:
            raw_rows:    List of raw dicts from ParquetReader.
            batch_id:    Correlation ID for this batch (UUID from application layer).
            source_file: Name of the source Parquet file for provenance.

        Returns:
            Tuple of (valid_trips, invalid_events).
        """
        valid_trips: list[Trip] = []
        invalid_events: list[InvalidTripDetected] = []

        for raw in raw_rows:
            trip: Trip | None = None
            try:
                normalised = TripNormalizer.normalize(raw)
                trip = _build_trip(normalised, batch_id, source_file)
                self._enricher.enrich(trip)
                TripValidator.validate(trip)
                valid_trips.append(trip)

            except DomainError as exc:
                stage = _stage_from_exception(exc)
                event = InvalidTripDetected.from_exception(
                    exc=exc,
                    stage=stage,
                    original_record=raw,
                    batch_id=batch_id,
                    source_file=source_file,
                    trip_id=trip.trip_id if trip else None,
                )
                invalid_events.append(event)
                logger.debug(
                    "Trip rejected at %s stage: %s",
                    stage.value,
                    str(exc),
                    extra={"batch_id": batch_id, "error_type": type(exc).__name__},
                )

            except Exception as exc:
                # Unexpected errors (e.g. pandas missing) are wrapped so
                # one bad row never aborts the whole batch.
                event = InvalidTripDetected.from_exception(
                    exc=exc,
                    stage=ProcessingStage.PARSING,
                    original_record=raw,
                    batch_id=batch_id,
                    source_file=source_file,
                )
                invalid_events.append(event)
                logger.warning(
                    "Unexpected error processing row: %s",
                    str(exc),
                    exc_info=True,
                    extra={"batch_id": batch_id},
                )

        unique_trips, duplicates = self._deduplicator.deduplicate(valid_trips)

        for dup in duplicates:
            invalid_events.append(
                InvalidTripDetected(
                    stage=ProcessingStage.DEDUPLICATION,
                    error_message=f"Duplicate trip_id within batch: {dup.trip_id}",
                    error_type="WithinBatchDuplicate",
                    original_record=dup.to_dict(),
                    batch_id=batch_id,
                    source_file=source_file,
                    trip_id=dup.trip_id,
                )
            )

        logger.info(
            "Batch processed",
            extra={
                "batch_id": batch_id,
                "total_rows": len(raw_rows),
                "valid": len(unique_trips),
                "invalid": len(invalid_events),
                "duplicates": len(duplicates),
            },
        )

        return unique_trips, invalid_events


def _stage_from_exception(exc: DomainError) -> ProcessingStage:
    """Map a DomainError subclass to the pipeline stage where it originated."""
    from etl.domain.trip.exceptions import (  # noqa: PLC0415
        InvalidPassengerCountError,
        InvalidPickupDatetimeError,
        InvalidTripDurationError,
        NegativeMoneyError,
        TripParseError,
        ZoneNotFoundError,
    )

    if isinstance(exc, TripParseError):
        return ProcessingStage.PARSING
    if isinstance(exc, ZoneNotFoundError):
        return ProcessingStage.ENRICHMENT
    if isinstance(
        exc,
        (
            InvalidTripDurationError,
            InvalidPassengerCountError,
            InvalidPickupDatetimeError,
            NegativeMoneyError,
        ),
    ):
        return ProcessingStage.VALIDATION
    return ProcessingStage.PARSING
