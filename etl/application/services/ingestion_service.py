from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from etl.application import EventPublisher
from etl.application.services.validation_service import ValidationService
from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
from etl.domain.dead_letter.services import DeadLetterService
from etl.domain.trip.services import TripDomainService
from etl.infrastructure.storage.parquet_reader import ParquetReader
from etl.observability.correlation import BatchCorrelationContext
from etl.runtime.shutdown import ShutdownHandler

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class IngestionSummary:
    """Aggregated stats for a completed ingestion run."""

    source_file: str
    total_batches: int = 0
    total_rows: int = 0
    total_valid: int = 0
    total_invalid: int = 0
    total_schema_rejected: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    @property
    def reject_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.total_invalid + self.total_schema_rejected) / self.total_rows

    def complete(self) -> None:
        self.finished_at = datetime.now(UTC)

    def as_dict(self) -> dict[str, object]:
        return {
            "source_file": self.source_file,
            "total_batches": self.total_batches,
            "total_rows": self.total_rows,
            "total_valid": self.total_valid,
            "total_invalid": self.total_invalid,
            "total_schema_rejected": self.total_schema_rejected,
            "reject_rate": round(self.reject_rate, 4),
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


class IngestionService:
    """
    Coordinates the full producer pipeline loop.

    Sequence per Parquet batch:
      1. Schema validate (Pydantic) -- reject completely malformed rows early
      2. Domain process (normalise + parse + enrich + validate + deduplicate)
      3. Publish valid trips to Kafka
      4. Dead-letter invalid records to DLQ topic and data/rejected/

    Respects the ShutdownHandler flag -- finishes the current batch and
    then exits cleanly rather than stopping mid-batch.

    Usage:
        service = IngestionService(reader, domain_service, publisher,
                                   dead_letter_service, topic, shutdown_handler)
        summary = service.run()
    """

    def __init__(
        self,
        reader: ParquetReader,
        domain_service: TripDomainService,
        publisher: EventPublisher,
        dead_letter_service: DeadLetterService,
        topic: str,
        shutdown_handler: ShutdownHandler | None = None,
        validation_service: ValidationService | None = None,
    ) -> None:
        self._reader = reader
        self._domain_service = domain_service
        self._publisher = publisher
        self._dead_letter_service = dead_letter_service
        self._topic = topic
        self._shutdown_handler = shutdown_handler or ShutdownHandler()
        self._validation_service = validation_service or ValidationService()

    def run(self) -> IngestionSummary:
        """
        Iterate all Parquet batches and run the full ingestion pipeline.

        Returns IngestionSummary with counts for the completed run.
        Exits early (but cleanly) if shutdown is requested mid-file.
        """
        summary = IngestionSummary(source_file=self._reader.source_file)

        logger.info(
            "Ingestion started",
            extra={"source_file": self._reader.source_file, "topic": self._topic},
        )

        for raw_batch in self._reader.iter_batches():
            if self._shutdown_handler.is_shutdown_requested:
                logger.info("Shutdown requested -- stopping after current batch")
                break

            with BatchCorrelationContext() as ctx:
                self._process_batch(raw_batch, ctx.correlation_id, summary)

        summary.complete()
        self._publisher.flush()

        logger.info("Ingestion complete", extra=summary.as_dict())
        return summary

    def _process_batch(
        self,
        raw_batch: list[dict[str, Any]],
        batch_id: str,
        summary: IngestionSummary,
    ) -> None:
        summary.total_batches += 1
        summary.total_rows += len(raw_batch)

        # Step 1: application-level schema validation
        schema_valid, schema_invalid = self._validation_service.validate_batch(raw_batch)
        summary.total_schema_rejected += len(schema_invalid)

        for row, error_msg in schema_invalid:
            record = DeadLetterRecord(
                original_record=row,
                error_message=error_msg,
                error_type="SchemaValidationError",
                stage=DeadLetterStage.VALIDATION,
                batch_id=batch_id,
                source_file=self._reader.source_file,
            )
            self._dead_letter_service.send(record)

        # Step 2: domain pipeline (normalise, enrich, validate, deduplicate)
        valid_trips, invalid_events = self._domain_service.process_batch(
            schema_valid, batch_id, self._reader.source_file
        )

        summary.total_valid += len(valid_trips)
        summary.total_invalid += len(invalid_events)

        # Step 3: publish valid trips to Kafka
        if valid_trips:
            messages = [t.to_dict() for t in valid_trips]
            self._publisher.publish_batch(self._topic, messages)

        # Step 4: dead-letter invalid domain records
        if invalid_events:
            dl_records = [
                DeadLetterRecord.from_invalid_event(event)
                for event in invalid_events
            ]
            self._dead_letter_service.send_batch(dl_records)

        logger.info(
            "Batch ingested",
            extra={
                "batch_id": batch_id,
                "raw": len(raw_batch),
                "schema_rejected": len(schema_invalid),
                "valid": len(valid_trips),
                "invalid": len(invalid_events),
            },
        )
