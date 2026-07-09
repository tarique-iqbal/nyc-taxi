from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class AccumulatedBatch:
    """
    A batch of rows accumulated from Kafka messages, ready for insertion.

    batch_id is the correlation ID that threads through all log lines
    and the ClickHouse ingested_at metadata for this batch.
    """

    rows: list[dict[str, Any]]
    batch_id: str
    source: str
    accumulated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def size(self) -> int:
        return len(self.rows)

    def is_empty(self) -> bool:
        return len(self.rows) == 0


class BatchAccumulator:
    """
    Accumulates rows across multiple Kafka messages and decides when to flush.

    Decoupled from the Kafka consumer -- it only handles the batching
    logic so it can be tested without any Kafka infrastructure.

    Flush triggers (whichever comes first):
      1. Size threshold: accumulated rows >= max_size
      2. Time timeout: time since last flush >= max_wait_seconds

    Usage in consumer:
        accumulator = BatchAccumulator(max_size=500, max_wait_seconds=10)
        for message in kafka_consumer:
            row = deserialise(message)
            accumulator.add(row)
            if accumulator.should_flush():
                batch = accumulator.flush()
                insert_to_clickhouse(batch)
                kafka_consumer.commit()
    """

    def __init__(
        self,
        max_size: int,
        max_wait_seconds: int,
        source: str = "kafka",
    ) -> None:
        self._max_size = max_size
        self._max_wait_seconds = max_wait_seconds
        self._source = source
        self._rows: list[dict[str, Any]] = []
        self._last_flush_at: float = time.monotonic()
        self._current_batch_id: str = self._new_batch_id()

    def add(self, row: dict[str, Any]) -> None:
        """Add a single row to the accumulator."""
        self._rows.append(row)

    def add_many(self, rows: list[dict[str, Any]]) -> None:
        """Add multiple rows to the accumulator."""
        self._rows.extend(rows)

    def should_flush(self) -> bool:
        """
        Return True if the accumulator should be flushed now.

        Checked after every add() call in the consumer loop.
        """
        return self._size_threshold_reached() or self._timeout_reached()

    def flush(self) -> AccumulatedBatch:
        """
        Return the accumulated rows as a batch and reset the accumulator.

        The batch carries the current batch_id so all downstream log lines
        and ClickHouse rows share the same correlation ID.

        Calling flush() on an empty accumulator is safe -- it returns an
        empty batch and resets the timeout.
        """
        batch = AccumulatedBatch(
            rows=list(self._rows),
            batch_id=self._current_batch_id,
            source=self._source,
        )
        self._rows.clear()
        self._last_flush_at = time.monotonic()
        self._current_batch_id = self._new_batch_id()
        return batch

    def pending_count(self) -> int:
        """Number of rows waiting to be flushed."""
        return len(self._rows)

    def seconds_since_last_flush(self) -> float:
        return time.monotonic() - self._last_flush_at

    def _size_threshold_reached(self) -> bool:
        return len(self._rows) >= self._max_size

    def _timeout_reached(self) -> bool:
        return self.seconds_since_last_flush() >= self._max_wait_seconds

    @staticmethod
    def _new_batch_id() -> str:
        return str(uuid.uuid4())
