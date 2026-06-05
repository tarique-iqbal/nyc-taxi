from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum


class DeadLetterStage(str, Enum):
    """
    The pipeline stage that produced a dead letter record.

    Mirrors ProcessingStage from domain/trip/events.py but is
    kept separate so dead_letter models have no import dependency
    on the trip subdomain.
    """

    PARSING = "parsing"
    NORMALIZATION = "normalization"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    DEDUPLICATION = "deduplication"
    PERSIST = "persist"


@dataclass
class DeadLetterRecord:
    """
    Wraps a failed record with all context needed for inspection and replay.

    Written to two destinations by DeadLetterService:
      1. Kafka DLQ topic (nyc-taxi-trips-dlq) -- for automated replay.
      2. data/rejected/ on disk -- for human inspection without Kafka access.

    Both writes happen atomically -- if either fails the record is not
    acknowledged as handled.

    trip_id may be None if the failure occurred before trip_id could be
    generated (e.g. a parse error on the datetime fields).
    """

    original_record: dict[str, object]
    error_message: str
    error_type: str
    stage: DeadLetterStage
    batch_id: str
    source_file: str
    trip_id: str | None = None
    failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    retry_count: int = 0

    @classmethod
    def from_invalid_event(
        cls,
        event: object,
        stage: DeadLetterStage | None = None,
    ) -> DeadLetterRecord:
        """
        Construct a DeadLetterRecord from an InvalidTripDetected event.

        Accepts any object with the expected attributes so the dead_letter
        subdomain does not import from the trip subdomain directly.
        """
        return cls(
            original_record=getattr(event, "original_record", {}),
            error_message=getattr(event, "error_message", ""),
            error_type=getattr(event, "error_type", "Unknown"),
            stage=stage or DeadLetterStage(getattr(event, "stage", DeadLetterStage.PARSING).value),
            batch_id=getattr(event, "batch_id", ""),
            source_file=getattr(event, "source_file", ""),
            trip_id=getattr(event, "trip_id", None),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "trip_id": self.trip_id,
            "original_record": self.original_record,
            "error_message": self.error_message,
            "error_type": self.error_type,
            "stage": self.stage.value,
            "batch_id": self.batch_id,
            "source_file": self.source_file,
            "failed_at": self.failed_at.isoformat(),
            "retry_count": self.retry_count,
        }
