from __future__ import annotations

from abc import ABC, abstractmethod

from etl.domain.dead_letter.models import DeadLetterRecord


class DeadLetterService(ABC):
    """
    Abstract interface for routing failed records to dead letter storage.

    The domain defines the contract. Infrastructure (Kafka DLQ publisher
    + disk writer) provides the concrete implementation wired in the
    DI container.

    Implementations must write to both destinations atomically:
      - Kafka DLQ topic (nyc-taxi-trips-dlq)
      - data/rejected/ on disk (JSON lines file)

    Writing both ensures:
      - Kafka DLQ is available for automated replay via replay_dlq.sh.
      - Disk copy is available for human inspection without Kafka access.
    """

    @abstractmethod
    def send(self, record: DeadLetterRecord) -> None:
        """
        Route a single dead letter record to both destinations.

        Must not raise. If either destination is unavailable the
        implementation should log the failure and continue -- dropping
        a DLQ record is preferable to crashing the pipeline.
        """

    @abstractmethod
    def send_batch(self, records: list[DeadLetterRecord]) -> None:
        """
        Route a list of dead letter records.

        Implementations may send records individually (as per
        dead_letter_publisher.py) to keep each DLQ message inspectable
        without parsing a batch envelope.
        """

    @abstractmethod
    def flush(self) -> None:
        """
        Flush any buffered records to their destinations.

        Called during graceful shutdown after the current batch completes.
        """
