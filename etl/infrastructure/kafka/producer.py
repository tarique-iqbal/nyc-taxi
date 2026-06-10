from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaException, Producer

from etl.infrastructure.kafka.serializer import KafkaSerializer
from etl.runtime.retry import RetryConfig, retry

logger = logging.getLogger(__name__)


class KafkaEventPublisher:
    """
    Implements the EventPublisher port defined in the application layer.

    Publishes individual trip records to a Kafka topic. Each dict in
    the messages list is published as a separate Kafka message so the
    consumer can accumulate them independently via BatchAccumulator.

    Reliability guarantees:
      - acks="all": waits for leader + all in-sync replicas to acknowledge
        before considering a message delivered.
      - enable.idempotence=True: prevents duplicate messages on producer
        retry (requires acks="all" and retries > 0).
      - Exponential backoff retry via @retry decorator on publish_batch.

    Usage:
        publisher = KafkaEventPublisher(bootstrap_servers="localhost:9092")
        publisher.publish_batch("nyc-taxi-trips", [trip.to_dict(), ...])
        publisher.flush()
    """

    def __init__(
        self,
        bootstrap_servers: str,
        acks: str = "all",
        retries: int = 5,
    ) -> None:
        self._topic_default: str | None = None
        self._serializer = KafkaSerializer()
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": acks,
                "retries": retries,
                "enable.idempotence": "true",
                "max.in.flight.requests.per.connection": 5,
                "linger.ms": 5,            # small batching window at the network level
                "batch.size": 65536,       # 64 KB per broker batch
                "compression.type": "lz4",
            }
        )

    @retry(**RetryConfig.KAFKA_PUBLISH, exceptions=(KafkaException, BufferError))
    def publish_batch(self, topic: str, messages: list[dict[str, Any]]) -> None:
        """
        Publish a list of dicts to a Kafka topic.

        Each dict is serialised to JSON bytes and produced as a
        separate Kafka message. trip_id is used as the message key
        so partitioning is deterministic -- messages for the same
        trip consistently land on the same partition.

        poll(0) is called after each produce() to serve delivery
        report callbacks without blocking. A final flush() at the
        end of the producer process drains the internal queue.

        Args:
            topic:    Target Kafka topic name.
            messages: List of trip dicts (from Trip.to_dict()).
        """
        delivered = 0
        errors = 0

        for message in messages:
            key = str(message.get("trip_id", "")).encode("utf-8") or None
            value = self._serializer.serialize(message)

            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)

        self._producer.poll(0)

        logger.debug(
            "Published messages to Kafka",
            extra={"topic": topic, "count": len(messages)},
        )

    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush all buffered messages and wait for delivery confirmations.

        Called:
          1. At the end of each Parquet file to ensure all trips are
             delivered before the producer process exits.
          2. During graceful shutdown (lifecycle.shutdown()).

        Returns the number of messages still in the queue after timeout.
        A non-zero return means some messages were not delivered.
        """
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(
                "Kafka producer flush timed out with %d messages undelivered",
                remaining,
            )
        return remaining

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err:
            logger.error(
                "Kafka delivery failed",
                extra={
                    "topic": msg.topic() if msg else "unknown",
                    "error": str(err),
                },
            )
        else:
            logger.debug(
                "Kafka message delivered",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )
