from __future__ import annotations

import logging
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from etl.infrastructure.monitoring.metrics import kafka_consumer_lag

logger = logging.getLogger(__name__)


@dataclass
class PartitionLag:
    topic: str
    partition: int
    current_offset: int
    committed_offset: int

    @property
    def lag(self) -> int:
        if self.committed_offset < 0:
            # -1001 means no committed offset yet (consumer has never committed).
            # Treat the full partition as lag.
            return self.current_offset
        return max(0, self.current_offset - self.committed_offset)


class KafkaLagMonitor:
    """
    Polls Kafka for consumer group lag and updates the Prometheus gauge.

    Queries:
      1. list_consumer_group_offsets()  -- committed offsets per partition
      2. get_watermark_offsets()        -- high watermark (latest offset)
      Lag = high watermark - committed offset per partition, summed across
      all partitions.

    The kafka_consumer_lag Prometheus gauge is updated on each poll() call.
    Grafana and Alertmanager read this gauge to detect a stalled consumer.

    Usage (in a background thread or scheduled call):
        monitor = KafkaLagMonitor(
            bootstrap_servers="localhost:9092",
            group_id="nyc-taxi-etl-consumer",
            topic="nyc-taxi-trips",
        )
        monitor.poll()          # call periodically
        total_lag = monitor.total_lag()
    """

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        lag_alert_threshold: int = 10_000,
    ) -> None:
        self._group_id = group_id
        self._topic = topic
        self._lag_alert_threshold = lag_alert_threshold
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": "false",
            }
        )
        self._partition_lags: list[PartitionLag] = []

    def poll(self) -> int:
        """
        Query Kafka for current lag and update the Prometheus gauge.

        Returns the total lag across all partitions.
        Logs a warning if lag exceeds the alert threshold.
        """
        try:
            self._partition_lags = self._fetch_partition_lags()
            total = sum(p.lag for p in self._partition_lags)
            kafka_consumer_lag.set(total)

            if total > self._lag_alert_threshold:
                logger.warning(
                    "Kafka consumer lag above threshold",
                    extra={
                        "lag": total,
                        "threshold": self._lag_alert_threshold,
                        "group_id": self._group_id,
                        "topic": self._topic,
                    },
                )
            else:
                logger.debug(
                    "Kafka consumer lag",
                    extra={"lag": total, "topic": self._topic},
                )

            return total

        except KafkaException as exc:
            logger.error("Failed to poll Kafka lag: %s", exc)
            return -1

    def total_lag(self) -> int:
        """Return the last known total lag without querying Kafka."""
        return sum(p.lag for p in self._partition_lags)

    def partition_lags(self) -> list[PartitionLag]:
        """Return per-partition lag from the last poll."""
        return list(self._partition_lags)

    def _fetch_partition_lags(self) -> list[PartitionLag]:
        metadata = self._admin.list_topics(topic=self._topic, timeout=10)
        topic_metadata = metadata.topics.get(self._topic)
        if topic_metadata is None:
            logger.warning("Topic not found in metadata", extra={"topic": self._topic})
            return []

        partitions = [
            TopicPartition(self._topic, pid)
            for pid in topic_metadata.partitions
        ]

        committed = self._consumer.committed(partitions, timeout=10)

        lags = []
        for tp in committed:
            low, high = self._consumer.get_watermark_offsets(tp, timeout=5)
            lags.append(
                PartitionLag(
                    topic=self._topic,
                    partition=tp.partition,
                    current_offset=high,
                    committed_offset=tp.offset,
                )
            )

        return lags

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as exc:
            logger.warning("Error closing lag monitor consumer: %s", exc)
