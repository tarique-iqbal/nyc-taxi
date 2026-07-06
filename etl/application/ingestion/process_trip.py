from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from etl.domain.trip.events import InvalidTripDetected
from etl.domain.trip.models import Trip
from etl.domain.trip.services import TripDomainService


@dataclass(frozen=True)
class ProcessTripCommand:
    """Command carrying a single raw row for use-case processing."""

    raw_row: dict[str, Any]
    source_file: str
    batch_id: str = ""

    def __post_init__(self) -> None:
        if not self.batch_id:
            object.__setattr__(self, "batch_id", str(uuid.uuid4()))


@dataclass(frozen=True)
class ProcessTripResult:
    """
    Result of processing a single trip through the domain pipeline.

    Exactly one of trip or invalid_event will be set.
    """

    trip: Trip | None
    invalid_event: InvalidTripDetected | None
    success: bool

    @classmethod
    def valid(cls, trip: Trip) -> ProcessTripResult:
        return cls(trip=trip, invalid_event=None, success=True)

    @classmethod
    def invalid(cls, event: InvalidTripDetected) -> ProcessTripResult:
        return cls(trip=None, invalid_event=event, success=False)


class ProcessTripUseCase:
    """
    Use case for processing a single raw trip row through the domain pipeline.

    Delegates to TripDomainService.process_batch() with a single-element list
    rather than duplicating pipeline logic. Primarily used in:
      - Unit tests that need fine-grained control over one trip at a time.
      - Edge-case tooling (e.g. manually re-running one rejected record).
      - Integration tests verifying the full domain pipeline end-to-end.

    For production throughput use IngestionService, which operates
    on full Parquet batches.

    Usage:
        use_case = ProcessTripUseCase(domain_service=service)
        result = use_case.handle(ProcessTripCommand(raw_row=row, source_file="file.parquet"))
        if result.success:
            publish(result.trip)
        else:
            dead_letter(result.invalid_event)
    """

    def __init__(self, domain_service: TripDomainService) -> None:
        self._domain_service = domain_service

    def handle(self, command: ProcessTripCommand) -> ProcessTripResult:
        """
        Process a single raw row and return the outcome.

        Never raises -- domain exceptions are captured as InvalidTripDetected
        events inside TripDomainService.process_batch().
        """
        valid_trips, invalid_events = self._domain_service.process_batch(
            raw_rows=[command.raw_row],
            batch_id=command.batch_id,
            source_file=command.source_file,
        )

        if valid_trips:
            return ProcessTripResult.valid(valid_trips[0])

        event = invalid_events[0] if invalid_events else None
        if event is None:
            # Defensive: domain service returned neither valid nor invalid.
            # Should never happen in practice.
            from etl.domain.trip.events import ProcessingStage
            from etl.domain.trip.exceptions import DomainError
            event = InvalidTripDetected.from_exception(
                exc=DomainError("No result returned from domain service"),
                stage=ProcessingStage.PARSING,
                original_record=command.raw_row,
                batch_id=command.batch_id,
                source_file=command.source_file,
            )

        return ProcessTripResult.invalid(event)
