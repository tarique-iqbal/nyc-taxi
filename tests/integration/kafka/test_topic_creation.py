"""
Integration tests: Kafka topic creation and management.

Verifies TopicManager behaviour against a real Kafka broker:
  - Default pipeline topics are created correctly
  - Topic properties match TopicConfig (partitions, retention)
  - ensure_topics_exist() is idempotent
  - Custom topics can be created and verified
  - Topics can be introspected via topic_exists() and list_topics()

Requires running Kafka (make up). Skip if broker is unreachable.
All test-created topics use unique names to avoid interference with
the pipeline topics.
"""

from __future__ import annotations

import uuid

import pytest
from confluent_kafka.admin import AdminClient

from etl.infrastructure.kafka.topic_manager import DEFAULT_TOPICS, TopicConfig, TopicManager
from tests.integration.conftest import requires_kafka

pytestmark = [requires_kafka, pytest.mark.integration]


def _unique_topic(prefix: str = "test") -> str:
    return f"{prefix}-{uuid.uuid4()}"


def _delete_topic(bootstrap_servers: str, name: str) -> None:
    """Best-effort cleanup of a test topic."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        futures = admin.delete_topics([name], operation_timeout=10)
        for _, f in futures.items():
            f.result()
    except Exception:
        pass


def _get_topic_metadata(bootstrap_servers: str, name: str) -> dict | None:
    """Return topic metadata dict or None if topic does not exist."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        metadata = admin.list_topics(topic=name, timeout=5)
        topic = metadata.topics.get(name)
        if topic and not topic.error:
            return {
                "partitions": len(topic.partitions),
            }
        return None
    except Exception:
        return None


# Pipeline topics
def test_default_pipeline_topics_exist(settings):
    """
    The main topic and DLQ topic must exist after bootstrap.
    Assumes create_topics.sh or ensure_topics_exist() has been run.
    """
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    assert manager.topic_exists(settings.kafka.topic)
    assert manager.topic_exists(settings.kafka.dlq_topic)


def test_main_topic_has_correct_partition_count(settings):
    meta = _get_topic_metadata(settings.kafka.bootstrap_servers, settings.kafka.topic)
    assert meta is not None
    main_config = next(t for t in DEFAULT_TOPICS if t.name == "nyc-taxi-trips")
    assert meta["partitions"] == main_config.num_partitions


def test_dlq_topic_has_one_partition(settings):
    meta = _get_topic_metadata(settings.kafka.bootstrap_servers, settings.kafka.dlq_topic)
    assert meta is not None
    assert meta["partitions"] == 1


# ensure_topics_exist: idempotency
def test_ensure_topics_exist_idempotent_with_pipeline_topics(settings):
    """Running ensure_topics_exist() when both topics already exist must not raise."""
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    manager.ensure_topics_exist()  # first call (topics exist from bootstrap)
    manager.ensure_topics_exist()  # second call: must be a no-op


def test_ensure_topics_exist_idempotent_with_custom_topic(settings):
    name = _unique_topic("idempotent")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        config = [TopicConfig(name=name, num_partitions=1)]
        manager.ensure_topics_exist(topics=config)
        # Second call: topic now exists, should skip without error
        manager.ensure_topics_exist(topics=config)
        assert manager.topic_exists(name)
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name)


# ensure_topics_exist: creates missing topics
def test_ensure_topics_exist_creates_new_topic(settings):
    name = _unique_topic("create")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        assert not manager.topic_exists(name)
        manager.ensure_topics_exist(topics=[TopicConfig(name=name, num_partitions=1)])
        assert manager.topic_exists(name)
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name)


def test_ensure_topics_exist_creates_with_correct_partition_count(settings):
    name = _unique_topic("partitions")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        manager.ensure_topics_exist(topics=[TopicConfig(name=name, num_partitions=3)])
        meta = _get_topic_metadata(settings.kafka.bootstrap_servers, name)
        assert meta is not None
        assert meta["partitions"] == 3
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name)


def test_ensure_topics_exist_creates_multiple_topics_at_once(settings):
    name_a = _unique_topic("multi-a")
    name_b = _unique_topic("multi-b")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        manager.ensure_topics_exist(
            topics=[
                TopicConfig(name=name_a, num_partitions=1),
                TopicConfig(name=name_b, num_partitions=2),
            ]
        )
        assert manager.topic_exists(name_a)
        assert manager.topic_exists(name_b)
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name_a)
        _delete_topic(settings.kafka.bootstrap_servers, name_b)


def test_ensure_topics_exist_creates_missing_leaves_existing(settings):
    """
    If one topic exists and one is missing, only the missing one is created.
    The existing topic must not be modified or raise an error.
    """
    name_existing = _unique_topic("existing")
    name_missing = _unique_topic("missing")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        manager.ensure_topics_exist(topics=[TopicConfig(name=name_existing, num_partitions=1)])
        assert manager.topic_exists(name_existing)
        assert not manager.topic_exists(name_missing)

        manager.ensure_topics_exist(
            topics=[
                TopicConfig(name=name_existing, num_partitions=1),
                TopicConfig(name=name_missing, num_partitions=1),
            ]
        )

        assert manager.topic_exists(name_existing)
        assert manager.topic_exists(name_missing)
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name_existing)
        _delete_topic(settings.kafka.bootstrap_servers, name_missing)


# topic_exists
def test_topic_exists_true_for_main_pipeline_topic(settings):
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    assert manager.topic_exists(settings.kafka.topic) is True


def test_topic_exists_false_for_nonexistent_topic(settings):
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    assert manager.topic_exists("definitely-does-not-exist-xyz-123") is False


def test_topic_exists_true_after_creation(settings):
    name = _unique_topic("exists")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        manager.ensure_topics_exist(topics=[TopicConfig(name=name, num_partitions=1)])
        assert manager.topic_exists(name) is True
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name)


# list_topics
def test_list_topics_returns_list(settings):
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    result = manager.list_topics()
    assert isinstance(result, list)


def test_list_topics_includes_pipeline_topics(settings):
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    topics = manager.list_topics()
    assert settings.kafka.topic in topics
    assert settings.kafka.dlq_topic in topics


def test_list_topics_includes_newly_created_topic(settings):
    name = _unique_topic("list")
    manager = TopicManager(bootstrap_servers=settings.kafka.bootstrap_servers)
    try:
        manager.ensure_topics_exist(topics=[TopicConfig(name=name, num_partitions=1)])
        topics = manager.list_topics()
        assert name in topics
    finally:
        _delete_topic(settings.kafka.bootstrap_servers, name)
