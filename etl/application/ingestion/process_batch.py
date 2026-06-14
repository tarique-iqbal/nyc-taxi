from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

from etl.application import EventPublisher
from etl.domain.dead_letter.models import DeadLetterRecord
from etl.domain.dead_letter.services import DeadLetterService
from etl.domain.trip.services import TripDomainService


@dataclass(frozen=True)
class ProcessBatchCommand:
    """
    Command carrying a single Parquet batch for pipeline processing.

    batch_id is the correlation ID that threads through every log line
    and every ClickHouse row produced from this batch.
    """

    raw_rows: list[dict]
    source_file: str
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True)
class ProcessBatchResult:
    """
    Result of running one batch through the domain pipeline and publishing to Kafka.

    valid_count + invalid_count may not equal len(raw_rows) if schema
    validation (ValidationService) rejected some rows before the domain
    pipeline was reached.
    """

    batch_id: str
    valid_count: int
    invalid_count: int
    duration_seconds: float

    @property
    def total(self) -> int:
        return self.valid_count + self.invalid_count

    @property
    def reject_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.invalid_count / self.total


class ProcessBatchUseCase:
    """
    Use case: process one batch of raw Parquet rows end-to-end.

    Steps:
      1. Domain pipeline -- normalise, enrich, validate, deduplicate.
      2. Publish valid trips to the main Kafka topic.
      3. Dead-letter invalid records.
      4. Return ProcessBatchResult with counts and wall-clock duration.

    This use case operates on a pre-validated batch -- schema validation
    (Pydantic) is handled upstream by ValidationService before this is
    called. IngestionService composes both.

    Using this use case directly (bypassing IngestionService) is useful
    in tests and in CLI tooling where a single batch needs to be
    reprocessed without opening a ParquetReader.
    """

    def __init__(
        self,
        domain_service: TripDomainService,
        publisher: EventPublisher,
        dead_letter_service: DeadLetterService,
        topic: str,
    ) -> None:
        self._domain_service = domain_service
        self._publisher = publisher
        self._dead_letter_service = dead_letter_service
        self._topic = topic

    def handle(self, command: ProcessBatchCommand) -> ProcessBatchResult:
        """
        Run the domain pipeline on command.raw_rows and publish results.

        Never raises on domain failures -- invalid rows are dead-lettered
        and counted. Raises only on infrastructure failures (Kafka,
        dead letter service) after retries are exhausted.
        """
        started_at = time.monotonic()

        valid_trips, invalid_events = self._domain_service.process_batch(
            raw_rows=command.raw_rows,
            batch_id=command.batch_id,
            source_file=command.source_file,
        )

        if valid_trips:
            messages = [trip.to_dict() for trip in valid_trips]
            self._publisher.publish_batch(self._topic, messages)

        if invalid_events:
            dl_records = [
                DeadLetterRecord.from_invalid_event(event)
                for event in invalid_events
            ]
            self._dead_letter_service.send_batch(dl_records)

        return ProcessBatchResult(
            batch_id=command.batch_id,
            valid_count=len(valid_trips),
            invalid_count=len(invalid_events),
            duration_seconds=time.monotonic() - started_at,
        )
