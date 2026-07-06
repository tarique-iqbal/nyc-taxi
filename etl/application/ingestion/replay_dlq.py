from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import cast

from etl.application import EventPublisher
from etl.domain.dead_letter.models import DeadLetterRecord, DeadLetterStage
from etl.domain.dead_letter.services import DeadLetterService
from etl.domain.trip.services import TripDomainService
from etl.utils.compression import iter_jsonl_gz, list_rejected_files
from etl.utils.json import JSONDict

logger = logging.getLogger(__name__)


class ReplaySource(StrEnum):
    DISK = "disk"
    KAFKA = "kafka"


@dataclass(frozen=True)
class ReplayDlqCommand:
    """
    Command to replay dead-letter records through the domain pipeline.

    source=DISK reads from data/rejected/*.jsonl.gz.
    source=KAFKA reads from the DLQ Kafka topic (requires a separate
    consumer group and is not yet implemented -- use DISK for now).

    batch_id filters to a single batch file. If None, all files are replayed.
    """

    source: ReplaySource = ReplaySource.DISK
    batch_id: str | None = None
    rejected_dir: Path = field(default_factory=lambda: Path("data/rejected"))


@dataclass
class ReplayDlqResult:
    """Aggregated outcome of a DLQ replay run."""

    total_replayed: int = 0
    recovered: int = 0
    still_invalid: int = 0

    @property
    def recovery_rate(self) -> float:
        if self.total_replayed == 0:
            return 0.0
        return self.recovered / self.total_replayed


class ReplayDlqUseCase:
    """
    Use case: replay failed records from the DLQ through the domain pipeline.

    Re-runs each dead-letter record through the same TripDomainService
    pipeline that originally rejected it. The intent is to recover records
    that were rejected due to transient issues (missing zone data, temporary
    enrichment failure, temporarily broken validator rules) after the root
    cause has been fixed.

    Outcomes per record:
      - Recovered:      domain pipeline succeeds -> published to main Kafka topic.
      - Still invalid:  domain pipeline fails again -> re-sent to DLQ with
                        incremented retry_count and updated error_message.

    The retry_count on DeadLetterRecord is preserved across replays so
    operators can identify records that have failed multiple times.

    Usage:
        use_case = ReplayDlqUseCase(domain_service, publisher, dead_letter_service, topic)
        result = use_case.handle(ReplayDlqCommand(source=ReplaySource.DISK))
        print(f"Recovered {result.recovered}/{result.total_replayed}")
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

    def handle(self, command: ReplayDlqCommand) -> ReplayDlqResult:
        """
        Replay all matching DLQ records and return the outcome summary.
        """
        if command.source == ReplaySource.KAFKA:
            raise NotImplementedError(
                "Kafka DLQ replay requires a dedicated consumer group. "
                "Use ReplaySource.DISK and replay from data/rejected/ files."
            )

        result = ReplayDlqResult()

        for record in self._read_from_disk(command):
            result.total_replayed += 1
            recovered = self._replay_record(record)
            if recovered:
                result.recovered += 1
            else:
                result.still_invalid += 1

        logger.info(
            "DLQ replay complete",
            extra={
                "source": command.source.value,
                "total": result.total_replayed,
                "recovered": result.recovered,
                "still_invalid": result.still_invalid,
                "recovery_rate": round(result.recovery_rate, 4),
            },
        )
        self._dead_letter_service.flush()
        return result

    def _replay_record(self, record: DeadLetterRecord) -> bool:
        """
        Re-run one dead-letter record through the domain pipeline.

        Returns True if the record was recovered (domain pipeline succeeded).
        """
        replay_batch_id = str(uuid.uuid4())

        valid_trips, invalid_events = self._domain_service.process_batch(
            raw_rows=[record.original_record],
            batch_id=replay_batch_id,
            source_file=record.source_file,
        )

        if valid_trips:
            messages = [t.to_dict() for t in valid_trips]
            self._publisher.publish_batch(self._topic, messages)
            logger.debug(
                "DLQ record recovered",
                extra={
                    "original_batch_id": record.batch_id,
                    "trip_id": record.trip_id,
                    "original_stage": record.stage.value,
                },
            )
            return True

        if invalid_events:
            event = invalid_events[0]
            updated = DeadLetterRecord(
                original_record=record.original_record,
                error_message=event.error_message,
                error_type=event.error_type,
                stage=DeadLetterStage(event.stage.value),
                batch_id=record.batch_id,
                source_file=record.source_file,
                trip_id=record.trip_id,
                retry_count=record.retry_count + 1,
            )
            self._dead_letter_service.send(updated)
            logger.debug(
                "DLQ record still invalid after replay",
                extra={
                    "batch_id": record.batch_id,
                    "trip_id": record.trip_id,
                    "retry_count": updated.retry_count,
                    "error_type": event.error_type,
                },
            )

        return False

    def _read_from_disk(
        self, command: ReplayDlqCommand
    ) -> Iterator[DeadLetterRecord]:
        """
        Yield DeadLetterRecord objects from gzip JSON lines files on disk.

        If batch_id is set, reads only the matching file.
        Otherwise reads all .jsonl.gz files in rejected_dir.
        """
        if command.batch_id:
            target = command.rejected_dir / f"{command.batch_id}.jsonl.gz"
            files = [target] if target.exists() else []
            if not files:
                logger.warning(
                    "No rejected file found for batch_id=%s", command.batch_id
                )
        else:
            files = list_rejected_files(command.rejected_dir)
            logger.info(
                "Found rejected files for replay",
                extra={"count": len(files), "dir": str(command.rejected_dir)},
            )

        for file_path in files:
            logger.debug("Replaying from file", extra={"path": str(file_path)})

            try:
                for raw in iter_jsonl_gz(file_path):
                    value = raw.get("original_record", {})
                    original_record: JSONDict = value if isinstance(value, dict) else {}
                    error_message = cast(str, raw.get("error_message", ""))
                    error_type = cast(str, raw.get("error_type", "Unknown"))
                    stage = DeadLetterStage(
                        cast(str, raw.get("stage", DeadLetterStage.PARSING.value))
                    )
                    batch_id = cast(str, raw.get("batch_id", ""))
                    source_file = cast(str, raw.get("source_file", ""))
                    trip_id = cast(str | None, raw.get("trip_id"))
                    retry_count = int(cast(int | str, raw.get("retry_count", 0)))

                    yield DeadLetterRecord(
                        original_record=original_record,
                        error_message=error_message,
                        error_type=error_type,
                        stage=stage,
                        batch_id=batch_id,
                        source_file=source_file,
                        trip_id=trip_id,
                        retry_count=retry_count,
                    )

            except Exception as exc:
                logger.error(
                    "Failed to read rejected file %s: %s", file_path, exc
                )
