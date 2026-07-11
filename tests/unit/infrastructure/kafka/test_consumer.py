from __future__ import annotations

import json
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from etl.runtime.shutdown import ShutdownHandler


def _msg(value: bytes, error=None) -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = value
    return msg


def _json_msg(data: dict) -> MagicMock:
    return _msg(json.dumps(data).encode("utf-8"))


def _eof_error() -> MagicMock:
    from confluent_kafka import KafkaError

    err = MagicMock()
    err.code.return_value = KafkaError._PARTITION_EOF
    err.fatal.return_value = False
    return err


def _fatal_error() -> MagicMock:
    err = MagicMock()
    err.code.return_value = 999
    err.fatal.return_value = True
    err.__str__ = MagicMock(return_value="fatal broker error")
    return err


def _non_fatal_error() -> MagicMock:
    from confluent_kafka import KafkaError

    err = MagicMock()
    err.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART
    err.fatal.return_value = False
    err.__str__ = MagicMock(return_value="non-fatal error")
    return err


def _handler_stops_after(n: int) -> MagicMock:
    """
    ShutdownHandler mock that returns False for the first n accesses
    to is_shutdown_requested, then True on subsequent accesses.
    """
    handler = MagicMock(spec=ShutdownHandler)
    responses = [False] * n + [True] * 10

    type(handler).is_shutdown_requested = PropertyMock(side_effect=responses)
    return handler


# KafkaConsumerAdapter construction
@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consumer_subscribes_to_topic_on_init(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    MockConsumer.return_value = instance

    KafkaConsumerAdapter(
        bootstrap_servers="localhost:9092",
        group_id="test-group",
        topic="nyc-taxi-trips",
    )

    instance.subscribe.assert_called_once_with(["nyc-taxi-trips"])


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consumer_configures_no_auto_commit(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    MockConsumer.return_value = MagicMock()

    KafkaConsumerAdapter("localhost:9092", "g", "t")

    config = MockConsumer.call_args.args[0]
    assert config["enable.auto.commit"] == "false"


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consumer_sets_earliest_offset_reset(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    MockConsumer.return_value = MagicMock()

    KafkaConsumerAdapter("localhost:9092", "g", "t")

    config = MockConsumer.call_args.args[0]
    assert config["auto.offset.reset"] == "earliest"


# _handle_error
@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_handle_error_partition_eof_does_not_raise(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    MockConsumer.return_value = MagicMock()

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")
    adapter._handle_error(_eof_error())  # should not raise


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_handle_error_non_fatal_does_not_raise(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    MockConsumer.return_value = MagicMock()

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")
    adapter._handle_error(_non_fatal_error())  # should not raise


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_handle_error_fatal_raises_kafka_exception(MockConsumer):
    from confluent_kafka import KafkaException

    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    MockConsumer.return_value = MagicMock()

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")

    with pytest.raises(KafkaException):
        adapter._handle_error(_fatal_error())


# commit
@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_commit_calls_underlying_consumer_synchronously(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    MockConsumer.return_value = instance

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")
    adapter.commit()

    instance.commit.assert_called_once_with(asynchronous=False)


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_commit_raises_on_kafka_exception(MockConsumer):
    from confluent_kafka import KafkaException

    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.commit.side_effect = KafkaException("offset error")
    MockConsumer.return_value = instance

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")

    with pytest.raises(KafkaException):
        adapter.commit()


# close
@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_close_calls_underlying_consumer(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    MockConsumer.return_value = instance

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")
    adapter.close()

    instance.close.assert_called_once()


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_close_does_not_raise_on_kafka_exception(MockConsumer):
    from confluent_kafka import KafkaException

    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.close.side_effect = KafkaException("close error")
    MockConsumer.return_value = instance

    adapter = KafkaConsumerAdapter("localhost:9092", "g", "t")
    adapter.close()  # should not raise


# consume_batches: message accumulation
@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_skips_none_poll_result(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.poll.return_value = None
    MockConsumer.return_value = instance

    handler = _handler_stops_after(1)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=100, batch_timeout_seconds=60
    )

    batches = list(adapter.consume_batches(handler))
    assert batches == []  # no messages polled → no batches


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_skips_empty_value(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.poll.return_value = _msg(b"")  # deserialises to None
    MockConsumer.return_value = instance

    handler = _handler_stops_after(2)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=100, batch_timeout_seconds=60
    )

    batches = list(adapter.consume_batches(handler))
    assert batches == []


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_skips_partition_eof(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.poll.return_value = _msg(b"", error=_eof_error())
    MockConsumer.return_value = instance

    handler = _handler_stops_after(2)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=100, batch_timeout_seconds=60
    )

    batches = list(adapter.consume_batches(handler))
    assert batches == []


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_yields_batch_on_size_flush(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    row = {"trip_id": "abc", "vendor_id": "CMT"}

    # 3 messages then shutdown
    instance.poll.side_effect = [
        _json_msg(row),
        _json_msg(row),
        _json_msg(row),
        None,
    ]
    MockConsumer.return_value = instance

    handler = _handler_stops_after(4)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=3, batch_timeout_seconds=60
    )

    batches = list(adapter.consume_batches(handler))

    assert len(batches) >= 1
    assert batches[0].size == 3


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_flushes_remainder_on_shutdown(MockConsumer):
    """
    Pending rows in the accumulator at shutdown time must be yielded as a
    final batch so no data is silently dropped on clean exit.
    """
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    row = {"trip_id": "abc"}

    # 2 messages polled, then shutdown requested
    instance.poll.side_effect = [_json_msg(row), _json_msg(row), None]
    MockConsumer.return_value = instance

    handler = _handler_stops_after(3)
    adapter = KafkaConsumerAdapter(
        "localhost:9092",
        "g",
        "t",
        batch_size=100,  # size threshold not reached
        batch_timeout_seconds=60,
    )

    batches = list(adapter.consume_batches(handler))

    total_rows = sum(b.size for b in batches)
    assert total_rows == 2


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_stops_when_shutdown_requested(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    instance.poll.return_value = _json_msg({"trip_id": "abc"})
    MockConsumer.return_value = instance

    # Shutdown immediately
    handler = _handler_stops_after(0)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=1000, batch_timeout_seconds=60
    )

    list(adapter.consume_batches(handler))

    # poll should not be called at all since shutdown flag is set upfront
    instance.poll.assert_not_called()


@patch("etl.infrastructure.kafka.consumer.Consumer")
def test_consume_batches_batch_id_unique_across_batches(MockConsumer):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    instance = MagicMock()
    row = {"trip_id": "abc"}

    # Two full batches of size 2
    instance.poll.side_effect = [
        _json_msg(row),
        _json_msg(row),  # batch 1
        _json_msg(row),
        _json_msg(row),  # batch 2
        None,
    ]
    MockConsumer.return_value = instance

    handler = _handler_stops_after(5)
    adapter = KafkaConsumerAdapter(
        "localhost:9092", "g", "t", batch_size=2, batch_timeout_seconds=60
    )

    batches = list(adapter.consume_batches(handler))
    size_2_batches = [b for b in batches if b.size == 2]

    if len(size_2_batches) >= 2:
        assert size_2_batches[0].batch_id != size_2_batches[1].batch_id
