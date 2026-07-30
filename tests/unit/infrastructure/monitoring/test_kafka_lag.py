from __future__ import annotations

from unittest.mock import MagicMock, patch

from etl.infrastructure.monitoring.kafka_lag import KafkaLagMonitor, PartitionLag


# PartitionLag: lag property edge cases
def test_lag_positive_when_consumer_behind():
    p = PartitionLag(topic="t", partition=0, current_offset=200, committed_offset=150)
    assert p.lag == 50


def test_lag_zero_when_consumer_caught_up():
    p = PartitionLag(topic="t", partition=0, current_offset=100, committed_offset=100)
    assert p.lag == 0


def test_lag_zero_when_committed_exceeds_current():
    p = PartitionLag(topic="t", partition=0, current_offset=50, committed_offset=75)
    assert p.lag == 0


def test_lag_equals_current_when_never_committed():
    # -1001 is the confluent-kafka sentinel for "no committed offset"
    p = PartitionLag(topic="t", partition=0, current_offset=300, committed_offset=-1001)
    assert p.lag == 300


def test_lag_zero_when_never_committed_and_empty_partition():
    p = PartitionLag(topic="t", partition=0, current_offset=0, committed_offset=-1001)
    assert p.lag == 0


def test_lag_large_offset_difference():
    p = PartitionLag(topic="t", partition=0, current_offset=10_000_000, committed_offset=9_500_000)
    assert p.lag == 500_000


# PartitionLag: dataclass fields
def test_partition_lag_stores_all_fields():
    p = PartitionLag(topic="nyc-taxi-trips", partition=2, current_offset=1000, committed_offset=900)
    assert p.topic == "nyc-taxi-trips"
    assert p.partition == 2
    assert p.current_offset == 1000
    assert p.committed_offset == 900


# Multi-partition aggregation
def test_total_lag_across_partitions():
    partitions = [
        PartitionLag("t", 0, current_offset=100, committed_offset=80),  # lag=20
        PartitionLag("t", 1, current_offset=200, committed_offset=200),  # lag=0
        PartitionLag("t", 2, current_offset=300, committed_offset=250),  # lag=50
        PartitionLag("t", 3, current_offset=400, committed_offset=360),  # lag=40
    ]
    total = sum(p.lag for p in partitions)
    assert total == 110


def test_total_lag_all_caught_up():
    partitions = [PartitionLag("t", i, current_offset=100, committed_offset=100) for i in range(4)]
    assert sum(p.lag for p in partitions) == 0


def test_total_lag_mix_of_never_committed_and_caught_up():
    partitions = [
        PartitionLag("t", 0, current_offset=500, committed_offset=-1001),  # lag=500
        PartitionLag("t", 1, current_offset=300, committed_offset=300),  # lag=0
    ]
    assert sum(p.lag for p in partitions) == 500


# KafkaLagMonitor: partition_lags returns copy
@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_partition_lags_returns_defensive_copy(MockConsumer, MockAdmin):
    MockAdmin.return_value = MagicMock()
    MockConsumer.return_value = MagicMock()
    monitor = KafkaLagMonitor("localhost:9092", "group", "topic")
    monitor._partition_lags = [
        PartitionLag("topic", 0, 100, 80),
        PartitionLag("topic", 1, 200, 150),
    ]

    first = monitor.partition_lags()
    second = monitor.partition_lags()

    first.clear()
    assert len(second) == 2  # mutation of first did not affect second
    assert len(monitor._partition_lags) == 2  # internal state unchanged


# KafkaLagMonitor: total_lag with pre-populated data
@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_total_lag_sums_all_partition_lags(MockConsumer, MockAdmin):
    MockAdmin.return_value = MagicMock()
    MockConsumer.return_value = MagicMock()
    monitor = KafkaLagMonitor("localhost:9092", "group", "topic")
    monitor._partition_lags = [
        PartitionLag("topic", 0, current_offset=100, committed_offset=60),
        PartitionLag("topic", 1, current_offset=200, committed_offset=175),
        PartitionLag("topic", 2, current_offset=50, committed_offset=50),
    ]
    # 40 + 25 + 0 = 65
    assert monitor.total_lag() == 65


@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_total_lag_zero_before_first_poll(MockConsumer, MockAdmin):
    MockAdmin.return_value = MagicMock()
    MockConsumer.return_value = MagicMock()
    monitor = KafkaLagMonitor("localhost:9092", "group", "topic")
    assert monitor.total_lag() == 0


# KafkaLagMonitor: alert threshold
@patch("etl.infrastructure.monitoring.kafka_lag.kafka_consumer_lag")
@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_poll_does_not_warn_below_threshold(MockConsumer, MockAdmin, mock_gauge, caplog):
    import logging

    mock_admin = MagicMock()
    mock_consumer = MagicMock()
    MockAdmin.return_value = mock_admin
    MockConsumer.return_value = mock_consumer

    partition_meta = MagicMock()
    partition_meta.partitions = {0: MagicMock()}
    mock_admin.list_topics.return_value.topics = {"topic": partition_meta}

    tp = MagicMock()
    tp.partition = 0
    tp.offset = 90
    mock_future = MagicMock()
    mock_future.result.return_value.topic_partitions = [tp]
    mock_admin.list_consumer_group_offsets.return_value = {"group": mock_future}
    mock_consumer.get_watermark_offsets.return_value = (0, 100)  # lag=10

    monitor = KafkaLagMonitor("localhost:9092", "group", "topic", lag_alert_threshold=10_000)
    with caplog.at_level(logging.WARNING):
        monitor.poll()

    warning_records = [
        r
        for r in caplog.records
        if r.levelno >= logging.WARNING and "threshold" in r.message.lower()
    ]
    assert len(warning_records) == 0


@patch("etl.infrastructure.monitoring.kafka_lag.kafka_consumer_lag")
@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_poll_warns_above_threshold(MockConsumer, MockAdmin, mock_gauge, caplog):
    import logging

    mock_admin = MagicMock()
    mock_consumer = MagicMock()
    MockAdmin.return_value = mock_admin
    MockConsumer.return_value = mock_consumer

    partition_meta = MagicMock()
    partition_meta.partitions = {0: MagicMock()}
    mock_admin.list_topics.return_value.topics = {"topic": partition_meta}

    tp = MagicMock()
    tp.partition = 0
    tp.offset = 0
    mock_future = MagicMock()
    mock_future.result.return_value.topic_partitions = [tp]
    mock_admin.list_consumer_group_offsets.return_value = {"group": mock_future}
    mock_consumer.get_watermark_offsets.return_value = (0, 100_000)  # lag=100_000

    monitor = KafkaLagMonitor("localhost:9092", "group", "topic", lag_alert_threshold=10_000)
    with caplog.at_level(logging.WARNING):
        monitor.poll()

    assert any(r.levelno >= logging.WARNING for r in caplog.records)


# KafkaLagMonitor: fetch uses admin API, not Consumer.committed()
@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_fetch_uses_admin_list_consumer_group_offsets_not_consumer_committed(
    MockConsumer, MockAdmin
):
    """
    Regression test: the internal watermark-only Consumer must never be asked
    for committed offsets on the real group. Calling Consumer.committed()
    with the real group.id risks Kafka handing this Consumer partitions
    during a rebalance that it never polls or commits, silently orphaning
    them from the real consumer -- this happened in production.
    """
    mock_admin = MagicMock()
    mock_consumer = MagicMock()
    MockAdmin.return_value = mock_admin
    MockConsumer.return_value = mock_consumer

    partition_meta = MagicMock()
    partition_meta.partitions = {0: MagicMock()}
    mock_admin.list_topics.return_value.topics = {"topic": partition_meta}

    tp = MagicMock()
    tp.partition = 0
    tp.offset = 0
    mock_future = MagicMock()
    mock_future.result.return_value.topic_partitions = [tp]
    mock_admin.list_consumer_group_offsets.return_value = {"group": mock_future}
    mock_consumer.get_watermark_offsets.return_value = (0, 0)

    monitor = KafkaLagMonitor("localhost:9092", "group", "topic")
    monitor.poll()

    mock_admin.list_consumer_group_offsets.assert_called_once()
    mock_consumer.committed.assert_not_called()


@patch("etl.infrastructure.monitoring.kafka_lag.AdminClient")
@patch("etl.infrastructure.monitoring.kafka_lag.Consumer")
def test_internal_consumer_uses_a_private_group_id(MockConsumer, MockAdmin):
    """
    Regression test: the watermark-only Consumer must be constructed with a
    group.id distinct from the real consumer group, so it can never become
    an actual member of that group and steal partitions during a rebalance.
    """
    MockAdmin.return_value = MagicMock()
    MockConsumer.return_value = MagicMock()

    KafkaLagMonitor("localhost:9092", "nyc-taxi-etl-consumer", "topic")

    config = MockConsumer.call_args.args[0]
    assert config["group.id"] != "nyc-taxi-etl-consumer"
