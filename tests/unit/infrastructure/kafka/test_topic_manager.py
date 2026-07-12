from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from etl.infrastructure.kafka.topic_manager import (
    DEFAULT_TOPICS,
    TopicConfig,
    TopicManager,
)


# TopicConfig
def test_topic_config_defaults():
    config = TopicConfig(name="my-topic")
    assert config.num_partitions == 4
    assert config.replication_factor == 1
    assert config.retention_ms == 604_800_000  # 7 days
    assert config.cleanup_policy == "delete"
    assert config.compression_type == "lz4"
    assert config.max_message_bytes == 10_485_760


def test_topic_config_custom_values():
    config = TopicConfig(
        name="custom-topic",
        num_partitions=8,
        replication_factor=3,
        retention_ms=1_209_600_000,
    )
    assert config.name == "custom-topic"
    assert config.num_partitions == 8
    assert config.replication_factor == 3
    assert config.retention_ms == 1_209_600_000


def test_topic_config_name_is_required():
    with pytest.raises(TypeError):
        TopicConfig()  # type: ignore[call-arg]


# DEFAULT_TOPICS
def test_default_topics_has_two_entries():
    assert len(DEFAULT_TOPICS) == 2


def test_default_topics_has_main_topic():
    names = [t.name for t in DEFAULT_TOPICS]
    assert "nyc-taxi-trips" in names


def test_default_topics_has_dlq_topic():
    names = [t.name for t in DEFAULT_TOPICS]
    assert "nyc-taxi-trips-dlq" in names


def test_main_topic_has_four_partitions():
    main = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips")
    assert main.num_partitions == 4


def test_dlq_topic_has_one_partition():
    dlq = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips-dlq")
    assert dlq.num_partitions == 1


def test_dlq_topic_has_longer_retention_than_main():
    main = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips")
    dlq = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips-dlq")
    assert dlq.retention_ms > main.retention_ms


def test_dlq_retention_is_fourteen_days():
    dlq = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips-dlq")
    assert dlq.retention_ms == 1_209_600_000  # 14 days


# TopicManager: list_topics / topic_exists
@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_topic_exists_returns_true_for_existing(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {"nyc-taxi-trips": MagicMock()}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    assert manager.topic_exists("nyc-taxi-trips") is True


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_topic_exists_returns_false_for_missing(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    assert manager.topic_exists("nonexistent-topic") is False


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_list_topics_returns_topic_names(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {
        "nyc-taxi-trips": MagicMock(),
        "nyc-taxi-trips-dlq": MagicMock(),
    }
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    topics = manager.list_topics()

    assert "nyc-taxi-trips" in topics
    assert "nyc-taxi-trips-dlq" in topics


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_list_topics_returns_list(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    assert isinstance(manager.list_topics(), list)


# TopicManager: ensure_topics_exist
@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_skips_existing_topics(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {
        "nyc-taxi-trips": MagicMock(),
        "nyc-taxi-trips-dlq": MagicMock(),
    }
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    manager.ensure_topics_exist()

    instance.create_topics.assert_not_called()


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_creates_missing_topics(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {}  # none exist

    future = MagicMock()
    future.result.return_value = None
    instance.create_topics.return_value = {
        "nyc-taxi-trips": future,
        "nyc-taxi-trips-dlq": future,
    }
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    manager.ensure_topics_exist()

    instance.create_topics.assert_called_once()


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_creates_only_missing_topics(MockAdmin):
    instance = MagicMock()
    # main topic already exists, DLQ is missing
    instance.list_topics.return_value.topics = {"nyc-taxi-trips": MagicMock()}

    future = MagicMock()
    future.result.return_value = None
    instance.create_topics.return_value = {"nyc-taxi-trips-dlq": future}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    manager.ensure_topics_exist()

    instance.create_topics.assert_called_once()
    created_names = [t.topic for t in instance.create_topics.call_args.args[0]]
    assert "nyc-taxi-trips-dlq" in created_names
    assert "nyc-taxi-trips" not in created_names


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_passes_partition_count(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {}

    future = MagicMock()
    future.result.return_value = None
    instance.create_topics.return_value = {
        "nyc-taxi-trips": future,
        "nyc-taxi-trips-dlq": future,
    }
    MockAdmin.return_value = instance

    custom_topic = TopicConfig(name="custom", num_partitions=8)
    manager = TopicManager("localhost:9092")
    manager.ensure_topics_exist(topics=[custom_topic])

    new_topics = instance.create_topics.call_args.args[0]
    assert new_topics[0].num_partitions == 8


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_handles_topic_already_exists_race(MockAdmin):
    from confluent_kafka import KafkaException

    instance = MagicMock()
    instance.list_topics.return_value.topics = {}

    future = MagicMock()
    future.result.side_effect = KafkaException("TOPIC_ALREADY_EXISTS")
    instance.create_topics.return_value = {"nyc-taxi-trips": future}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    # TOPIC_ALREADY_EXISTS should be silently ignored
    manager.ensure_topics_exist(topics=[TopicConfig(name="nyc-taxi-trips")])


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_raises_on_other_kafka_exception(MockAdmin):
    from confluent_kafka import KafkaException

    instance = MagicMock()
    instance.list_topics.return_value.topics = {}

    future = MagicMock()
    future.result.side_effect = KafkaException("BROKER_NOT_AVAILABLE")
    instance.create_topics.return_value = {"nyc-taxi-trips": future}
    MockAdmin.return_value = instance

    manager = TopicManager("localhost:9092")
    with pytest.raises(KafkaException):
        manager.ensure_topics_exist(topics=[TopicConfig(name="nyc-taxi-trips")])


@patch("etl.infrastructure.kafka.topic_manager.AdminClient")
def test_ensure_topics_with_custom_topic_list(MockAdmin):
    instance = MagicMock()
    instance.list_topics.return_value.topics = {}

    future = MagicMock()
    future.result.return_value = None
    instance.create_topics.return_value = {"my-topic": future}
    MockAdmin.return_value = instance

    custom = [TopicConfig(name="my-topic", num_partitions=2)]
    manager = TopicManager("localhost:9092")
    manager.ensure_topics_exist(topics=custom)

    created = [t.topic for t in instance.create_topics.call_args.args[0]]
    assert created == ["my-topic"]
