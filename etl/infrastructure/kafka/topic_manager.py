from __future__ import annotations

import logging
from dataclasses import dataclass

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    """Configuration for a single Kafka topic."""

    name: str
    num_partitions: int = 4
    replication_factor: int = 1
    retention_ms: int = 604_800_000  # 7 days
    cleanup_policy: str = "delete"
    compression_type: str = "lz4"
    max_message_bytes: int = 10_485_760  # 10 MB


# Default topic configurations matching kafka-topics.sh.
# DLQ gets 1 partition (low volume) and 14-day retention for
# operator inspection time before automated replay.
DEFAULT_TOPICS: list[TopicConfig] = [
    TopicConfig(
        name="nyc-taxi-trips",
        num_partitions=4,
        retention_ms=604_800_000,
    ),
    TopicConfig(
        name="nyc-taxi-trips-dlq",
        num_partitions=1,
        retention_ms=1_209_600_000,  # 14 days
    ),
]


class TopicManager:
    """
    Creates and inspects Kafka topics programmatically.

    Called during application startup (lifecycle.py) if topics do not
    already exist. Idempotent -- creates only topics that are absent.

    Also used by scripts/create_topics.sh via:
        python -c "from etl.infrastructure.kafka.topic_manager import ..."
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self._admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def ensure_topics_exist(
        self,
        topics: list[TopicConfig] | None = None,
    ) -> None:
        """
        Create any topics that do not already exist.

        Existing topics are left unchanged -- partitions and retention
        settings are not updated if the topic is already present.

        Args:
            topics: List of TopicConfig to create. Defaults to DEFAULT_TOPICS.
        """
        configs = topics or DEFAULT_TOPICS
        existing = self._list_existing_topics()

        to_create = [t for t in configs if t.name not in existing]
        if not to_create:
            logger.info(
                "All required topics already exist",
                extra={"topics": [t.name for t in configs]},
            )
            return

        new_topics = [
            NewTopic(
                topic=t.name,
                num_partitions=t.num_partitions,
                replication_factor=t.replication_factor,
                config={
                    "retention.ms": str(t.retention_ms),
                    "cleanup.policy": t.cleanup_policy,
                    "compression.type": t.compression_type,
                    "max.message.bytes": str(t.max_message_bytes),
                },
            )
            for t in to_create
        ]

        futures = self._admin.create_topics(new_topics)

        for topic_name, future in futures.items():
            try:
                future.result()
                logger.info("Created Kafka topic", extra={"topic": topic_name})
            except KafkaException as exc:
                # TOPIC_ALREADY_EXISTS (error code 36) is safe to ignore.
                # Another instance may have created it between list and create.
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    logger.debug(
                        "Topic already exists (race condition)", extra={"topic": topic_name}
                    )
                else:
                    logger.error("Failed to create topic %s: %s", topic_name, exc)
                    raise

    def list_topics(self) -> list[str]:
        """Return names of all topics on the broker."""
        return sorted(self._list_existing_topics())

    def topic_exists(self, name: str) -> bool:
        return name in self._list_existing_topics()

    def _list_existing_topics(self) -> set[str]:
        metadata = self._admin.list_topics(timeout=10)
        return set(metadata.topics.keys())

    def close(self) -> None:
        """No explicit cleanup is required for AdminClient."""
        return
