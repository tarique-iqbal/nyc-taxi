from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer

from etl.domain.dead_letter.models import DeadLetterRecord
from etl.domain.dead_letter.services import DeadLetterService
from etl.infrastructure.monitoring.metrics import dlq_records_total
from etl.utils.compression import rejected_file_path, write_jsonl_gz
from etl.utils.json import dumps

logger = logging.getLogger(__name__)


class KafkaDeadLetterPublisher(DeadLetterService):
    """
    Concrete implementation of DeadLetterService.

    Writes each failed record to two destinations atomically:
      1. Kafka DLQ topic (nyc-taxi-trips-dlq) -- for automated replay
         via replay_dlq.sh / ReplayDlqUseCase.
      2. data/rejected/<batch_id>.jsonl.gz on disk -- for human
         inspection without needing Kafka access.

    Records are published individually (not batched) so each DLQ
    message is self-contained and inspectable without parsing a
    batch envelope.

    Failure policy:
      - If the Kafka write fails, the error is logged and the disk
        write still proceeds.
      - If the disk write fails, the error is logged and the Kafka
        write still proceeds.
      - Neither failure raises -- losing a DLQ record is preferable
        to crashing the pipeline. Operators are alerted via log errors.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        dlq_topic: str,
        rejected_dir: Path | str,
    ) -> None:
        self._dlq_topic = dlq_topic
        self._rejected_dir = Path(rejected_dir)
        self._rejected_dir.mkdir(parents=True, exist_ok=True)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "retries": 3,
                "linger.ms": 0,  # publish DLQ records immediately, no batching delay
            }
        )

    def send(self, record: DeadLetterRecord) -> None:
        """
        Write a single DeadLetterRecord to Kafka DLQ and disk.

        Never raises. Errors in either destination are logged and
        the other destination write is still attempted.
        """
        self._write_to_kafka(record)
        self._write_to_disk(record)
        dlq_records_total.labels(stage=record.stage.value).inc()

    def send_batch(self, records: list[DeadLetterRecord]) -> None:
        """
        Write a list of DeadLetterRecords to both destinations.

        Each record is published individually so DLQ consumers can
        process one failure at a time without parsing batch envelopes.
        """
        for record in records:
            self.send(record)

    def flush(self) -> None:
        """
        Flush the Kafka producer buffer.

        Called during graceful shutdown so buffered DLQ messages
        are not lost when the process exits.
        """
        try:
            remaining = self._producer.flush(timeout=15.0)
            if remaining > 0:
                logger.warning(
                    "DLQ producer flushed with %d undelivered messages",
                    remaining,
                )
        except KafkaException as exc:
            logger.error("Error flushing DLQ producer: %s", exc)

    def _write_to_kafka(self, record: DeadLetterRecord) -> None:
        try:
            payload = dumps(record.as_dict()).encode("utf-8")
            key = (record.trip_id or record.batch_id).encode("utf-8")
            self._producer.produce(
                topic=self._dlq_topic,
                key=key,
                value=payload,
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)
        except (KafkaException, BufferError) as exc:
            logger.error(
                "Failed to publish record to DLQ topic: %s",
                exc,
                extra={
                    "dlq_topic": self._dlq_topic,
                    "batch_id": record.batch_id,
                    "trip_id": record.trip_id,
                    "stage": record.stage.value,
                },
            )

    def _write_to_disk(self, record: DeadLetterRecord) -> None:
        try:
            path = rejected_file_path(self._rejected_dir, record.batch_id)
            write_jsonl_gz([record.as_dict()], path)
        except Exception as exc:
            logger.error(
                "Failed to write rejected record to disk: %s",
                exc,
                extra={
                    "rejected_dir": str(self._rejected_dir),
                    "batch_id": record.batch_id,
                    "trip_id": record.trip_id,
                },
            )

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error(
                "DLQ delivery failed",
                extra={
                    "topic": self._dlq_topic,
                    "error": str(err),
                },
            )
        else:
            logger.debug(
                "DLQ record delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
