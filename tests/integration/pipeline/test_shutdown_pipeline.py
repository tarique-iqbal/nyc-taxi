"""
Integration: graceful shutdown pipeline.

Verifies that ShutdownHandler interacts correctly with IngestionService
and the consumer loop to ensure:
  - Shutdown flag stops the producer loop after the current batch
  - No data is lost when shutdown is triggered mid-file
  - The Kafka producer is flushed before exit
  - A partial consumer batch is yielded as the final flush on shutdown
  - Callbacks registered on ShutdownHandler are called

These tests use mocked Kafka/ClickHouse where possible to keep them
fast and deterministic. Where real infrastructure is used the
pytestmark declares the requirement explicitly.
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from etl.runtime.shutdown import ShutdownHandler

pytestmark = [pytest.mark.integration]


def _make_raw_row(n: int = 0) -> dict:
    pickup = datetime(2024, 1, 15, 10 + n % 12, 0, 0, tzinfo=UTC)
    dropoff = datetime(2024, 1, 15, 10 + n % 12, 30, 0, tzinfo=UTC)
    return {
        "vendor_id": 1,
        "pickup_datetime": pickup,
        "dropoff_datetime": dropoff,
        "passenger_count": 2.0,
        "trip_distance": 3.5,
        "rate_code_id": 1.0,
        "store_and_fwd_flag": "N",
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
        "payment_type": 1,
        "fare_amount": 12.5,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
        "total_amount": 19.8,
    }


def _make_ingestion_service(
    batches: list[list[dict]],
    shutdown_handler: ShutdownHandler,
    valid_trips_per_batch: int = 1,
) -> tuple:
    """
    Build an IngestionService with mocked Kafka, DLQ, and domain service.
    """
    from etl.application.services.ingestion_service import IngestionService
    from etl.domain.trip.models import Distance, Duration, Money, Payment, Trip

    def _make_trip(n: int) -> Trip:
        pickup = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
        dropoff = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        return Trip(
            trip_id=str(uuid.uuid4()),
            vendor_id="CMT",
            pickup_datetime=pickup,
            dropoff_datetime=dropoff,
            passenger_count=1,
            distance=Distance.of(3.0),
            duration=Duration.between(pickup, dropoff),
            pickup_location_id=161,
            dropoff_location_id=236,
            payment=Payment(
                payment_type="Credit card",
                fare_amount=Money.of(10.0, "fare_amount"),
                extra=Money.zero(),
                mta_tax=Money.of(0.5, "mta_tax"),
                tip_amount=Money.zero(),
                tolls_amount=Money.zero(),
                improvement_surcharge=Money.of(0.3, "improvement_surcharge"),
                congestion_surcharge=Money.of(2.5, "congestion_surcharge"),
                airport_fee=Money.zero(),
                total_amount=Money.of(13.3, "total_amount"),
            ),
            rate_code="Standard",
            store_and_fwd_flag="No",
            batch_id="b1",
            source_file="test.parquet",
        )

    reader = MagicMock()
    reader.source_file = "test.parquet"
    reader.iter_batches.return_value = iter(batches)

    domain_service = MagicMock()
    domain_service.process_batch.return_value = (
        [_make_trip(i) for i in range(valid_trips_per_batch)],
        [],
    )

    publisher = MagicMock()
    dl_service = MagicMock()

    validation_service = MagicMock()
    validation_service.validate_batch.side_effect = lambda rows: (rows, [])

    service = IngestionService(
        reader=reader,
        domain_service=domain_service,
        publisher=publisher,
        dead_letter_service=dl_service,
        topic="nyc-taxi-trips",
        shutdown_handler=shutdown_handler,
        validation_service=validation_service,
    )
    return service, publisher, dl_service, domain_service


# Shutdown flag stops the loop
def test_shutdown_before_start_processes_no_batches():
    handler = ShutdownHandler()
    handler.request_shutdown()

    batches = [[_make_raw_row(i) for i in range(5)]] * 3
    service, _, _, domain_service = _make_ingestion_service(batches, handler)

    summary = service.run()

    assert summary.total_batches == 0
    domain_service.process_batch.assert_not_called()


def test_shutdown_after_first_batch_stops_remaining():
    call_count = {"n": 0}
    handler = ShutdownHandler()

    reader = MagicMock()
    reader.source_file = "test.parquet"

    def _batches():
        for i in range(5):
            call_count["n"] += 1
            if call_count["n"] == 1:
                handler.request_shutdown()
            yield [_make_raw_row(i)]

    reader.iter_batches.return_value = _batches()

    from etl.application.services.ingestion_service import IngestionService

    domain_service = MagicMock()
    domain_service.process_batch.return_value = ([], [])
    validation_service = MagicMock()
    validation_service.validate_batch.side_effect = lambda r: (r, [])

    service = IngestionService(
        reader=reader,
        domain_service=domain_service,
        publisher=MagicMock(),
        dead_letter_service=MagicMock(),
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=validation_service,
    )
    service.run()

    # After shutdown is set during batch 1, the loop checks the flag
    # before starting batch 2 and stops.
    assert domain_service.process_batch.call_count <= 2


def test_shutdown_does_not_skip_current_batch():
    """
    Shutdown requested mid-loop should complete the current batch
    before stopping, not abort it.
    """
    handler = ShutdownHandler()
    batches = [
        [_make_raw_row(0), _make_raw_row(1)],  # batch 1 -- must complete
        [_make_raw_row(2), _make_raw_row(3)],  # batch 2 -- may be skipped
    ]
    service, publisher, _, _ = _make_ingestion_service(batches, handler, valid_trips_per_batch=2)

    # Trigger shutdown just after first iteration begins
    def _trigger():
        time.sleep(0.01)
        handler.request_shutdown()

    t = threading.Thread(target=_trigger, daemon=True)
    t.start()
    summary = service.run()
    t.join()

    # At least one batch must have been processed completely
    assert summary.total_batches >= 1


# Kafka flush on shutdown
def test_publisher_flush_called_after_shutdown():
    handler = ShutdownHandler()
    handler.request_shutdown()

    service, publisher, _, _ = _make_ingestion_service([[_make_raw_row()]], handler)
    service.run()

    publisher.flush.assert_called_once()


def test_publisher_flush_called_even_with_empty_file():
    handler = ShutdownHandler()
    service, publisher, _, _ = _make_ingestion_service([], handler)
    service.run()
    publisher.flush.assert_called_once()


def test_publisher_flush_called_after_exception_in_batch():
    handler = ShutdownHandler()
    reader = MagicMock()
    reader.source_file = "test.parquet"
    reader.iter_batches.side_effect = RuntimeError("read error")

    publisher = MagicMock()
    from etl.application.services.ingestion_service import IngestionService

    service = IngestionService(
        reader=reader,
        domain_service=MagicMock(),
        publisher=publisher,
        dead_letter_service=MagicMock(),
        topic="nyc-taxi-trips",
        shutdown_handler=handler,
        validation_service=MagicMock(validate_batch=MagicMock(side_effect=lambda r: (r, []))),
    )

    with pytest.raises(RuntimeError):
        service.run()

    publisher.flush.assert_called_once()


# ShutdownHandler callbacks
def test_shutdown_callbacks_fired_on_request_shutdown():
    handler = ShutdownHandler()
    fired = []
    handler.register_callback(lambda: fired.append("cb1"))
    handler.register_callback(lambda: fired.append("cb2"))
    handler.request_shutdown()
    assert fired == ["cb1", "cb2"]


def test_shutdown_callbacks_fired_in_registration_order():
    handler = ShutdownHandler()
    order = []
    for i in range(5):
        n = i
        handler.register_callback(lambda x=n: order.append(x))
    handler.request_shutdown()
    assert order == [0, 1, 2, 3, 4]


def test_shutdown_callback_exception_does_not_prevent_flag():
    handler = ShutdownHandler()
    handler.register_callback(lambda: (_ for _ in ()).throw(RuntimeError("bad cb")))
    handler.request_shutdown()
    assert handler.is_shutdown_requested is True


# Consumer loop shutdown with real Kafka
def test_consumer_shutdown_flushes_consumed_partial_batch(settings, kafka_producer):
    """
    Publish messages to Kafka with a consumer whose batch_size is larger
    than the number of messages. Trigger shutdown and verify that any
    messages already consumed into the accumulator are flushed before exit.
    """
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

    batch_id = str(uuid.uuid4())
    msgs = [{"trip_id": f"sd-{batch_id}-{i}"} for i in range(3)]
    kafka_producer.publish_batch(settings.kafka.topic, msgs)
    kafka_producer.flush()

    handler = ShutdownHandler()
    consumer = KafkaConsumerAdapter(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=f"test-shutdown-{uuid.uuid4()}",
        topic=settings.kafka.topic,
        batch_size=100,
        batch_timeout_seconds=30,
    )

    all_rows: list[dict] = []

    def _trigger():
        time.sleep(5)
        handler.request_shutdown()

    t = threading.Thread(target=_trigger, daemon=True)
    t.start()

    try:
        for batch in consumer.consume_batches(handler):
            all_rows.extend(batch.rows)
    finally:
        consumer.close()

    t.join()

    tagged = [r for r in all_rows if batch_id in r.get("trip_id", "")]

    # Shutdown should flush any records already accumulated before exit.
    # It is not expected to drain Kafka after shutdown is requested.
    assert tagged


# Summary completeness
def test_summary_finished_at_set_after_run():
    handler = ShutdownHandler()
    handler.request_shutdown()
    service, _, _, _ = _make_ingestion_service([], handler)
    summary = service.run()
    assert summary.finished_at is not None


def test_summary_source_file_matches_reader(tmp_path):
    handler = ShutdownHandler()
    handler.request_shutdown()
    service, _, _, _ = _make_ingestion_service([], handler)
    service._reader.source_file = "yellow_tripdata_2024-01.parquet"
    summary = service.run()
    assert summary.source_file == "yellow_tripdata_2024-01.parquet"
