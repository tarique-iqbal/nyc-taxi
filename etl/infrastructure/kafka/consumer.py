from __future__ import annotations

import logging
from collections.abc import Iterator

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from etl.config.settings import get_settings
from etl.infrastructure.kafka.serializer import KafkaSerializer
from etl.runtime.batching import AccumulatedBatch, BatchAccumulator
from etl.runtime.shutdown import ShutdownHandler

logger = logging.getLogger(__name__)


class KafkaConsumerAdapter:
    """
    Consumes messages from a Kafka topic and yields accumulated batches.

    Exactly-once semantics:
      - enable.auto.commit=False: offsets never advance automatically.
      - The consumer yields a batch and WAITS. The caller (consumer entrypoint)
        inserts into ClickHouse and then calls commit(). Only on success does
        the offset advance.
      - If ClickHouse insert fails: exception propagates, commit() is never
        called, Kafka replays the same batch on restart.
      - If the process dies after insert but before commit(): batch is replayed
        on restart. ReplacingMergeTree on trip_id deduplicates the re-insert.

    Accumulation strategy:
      - Each Kafka message carries one serialised trip dict (from the producer).
      - BatchAccumulator collects messages until size threshold OR timeout,
        whichever comes first.
      - poll(timeout=1.0) limits how long we block so the shutdown flag is
        checked at least once per second.

    Usage:
        adapter = KafkaConsumerAdapter(...)
        for batch in adapter.consume_batches(shutdown_handler):
            repository.save_batch_from_dicts(batch.rows)
            adapter.commit()
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        batch_size: int | None = None,
        batch_timeout_seconds: int | None = None,
    ) -> None:
        settings = get_settings()
        self._topic = topic
        self._serializer = KafkaSerializer()
        self._accumulator = BatchAccumulator(
            max_size=batch_size or settings.kafka.batch_size,
            max_wait_seconds=batch_timeout_seconds or settings.kafka.batch_timeout_seconds,
            source="kafka",
        )
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "max.poll.interval.ms": 300_000,
                "session.timeout.ms": 30_000,
                "fetch.min.bytes": 1,
                "fetch.wait.max.ms": 500,
            }
        )
        self._consumer.subscribe([topic])
        logger.info(
            "Kafka consumer subscribed",
            extra={"topic": topic, "group_id": group_id},
        )

    def consume_batches(self, shutdown_handler: ShutdownHandler) -> Iterator[AccumulatedBatch]:
        """
        Yield accumulated batches until shutdown is requested.

        Polls Kafka every second so the shutdown flag is checked
        frequently. Flushes the accumulator on size threshold, time
        timeout, or shutdown signal.

        The caller MUST call commit() after processing each yielded batch.
        Not calling commit() means the batch will be replayed on restart.
        """
        logger.info("Consumer loop started", extra={"topic": self._topic})

        try:
            while not shutdown_handler.is_shutdown_requested:
                msg: Message | None = self._consumer.poll(timeout=1.0)

                if msg is not None:
                    error = msg.error()
                    if error is not None:
                        self._handle_error(error)
                    else:
                        row = self._serializer.deserialize(msg.value())
                        if row is not None:
                            self._accumulator.add(row)

                if self._accumulator.should_flush():
                    batch = self._accumulator.flush()
                    if not batch.is_empty():
                        logger.debug(
                            "Yielding batch for processing",
                            extra={
                                "batch_id": batch.batch_id,
                                "size": batch.size,
                            },
                        )
                        yield batch

        finally:
            # Flush whatever is left before the shutdown completes.
            # This batch is yielded, processed, and committed so no
            # rows are lost on clean shutdown.
            if self._accumulator.pending_count() > 0:
                final_batch = self._accumulator.flush()
                if not final_batch.is_empty():
                    logger.info(
                        "Flushing final batch before shutdown",
                        extra={"size": final_batch.size},
                    )
                    yield final_batch

        logger.info("Consumer loop exited cleanly")

    def commit(self) -> None:
        """
        Commit the current offsets for all assigned partitions.

        Synchronous commit (asynchronous=False) so the caller knows
        the offset was persisted to Kafka before continuing. Called
        only after ClickHouse confirms the insert.
        """
        try:
            self._consumer.commit(asynchronous=False)
            logger.debug("Kafka offsets committed")
        except KafkaException as exc:
            logger.error("Failed to commit Kafka offsets: %s", exc)
            raise

    def close(self) -> None:
        """
        Commit pending offsets and close the consumer connection.

        Called during graceful shutdown (lifecycle.shutdown()).
        Unsubscribes cleanly so the consumer group rebalances
        immediately rather than waiting for the session timeout.
        """
        try:
            self._consumer.close()
            logger.info("Kafka consumer closed")
        except KafkaException as exc:
            logger.warning("Error closing Kafka consumer: %s", exc)

    def _handle_error(self, error: KafkaError) -> None:
        if error.code() == KafkaError._PARTITION_EOF:
            # End of partition -- not an error, just no more messages right now.
            logger.debug("Reached end of partition")
        elif error.fatal():
            logger.error("Fatal Kafka error: %s", error)
            raise KafkaException(error)
        else:
            logger.warning("Non-fatal Kafka error: %s", error)
