"""
Consumer entrypoint.

Reads accumulated trip batches from the Kafka topic, inserts them
into ClickHouse, and commits offsets only after a confirmed insert.

Exactly-once semantics:
  - Kafka offset advances only after ClickHouse insert succeeds.
  - If insert fails: no commit, same batch replayed on restart.
  - If process dies after insert but before commit: batch replayed,
    ReplacingMergeTree on trip_id deduplicates the re-insert.

Startup sequence:
  1. lifecycle.startup("consumer") -- logging, tracing, ClickHouse, zones, Kafka consumer
  2. Wire ClickHouseTripRepository with initialised client
  3. Run consume loop until EOF or SIGTERM
  4. lifecycle.shutdown() -- commit final offsets, close connections

Run:
    python -m etl.entrypoints.consumer
"""

from __future__ import annotations

import logging
import sys

from etl.config.settings import get_settings
from etl.domain.trip.repositories import TripRepository
from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter
from etl.runtime.lifecycle import AppComponents, shutdown, startup
from etl.runtime.shutdown import ShutdownHandler

logger = logging.getLogger(__name__)


def _build_trip_repository(components: AppComponents) -> TripRepository:
    """
    Wire ClickHouseTripRepository with the initialised ClickHouseClient.

    Import is deferred so this module is importable before
    infrastructure/clickhouse/ is generated.
    """
    from etl.infrastructure.clickhouse.repository import ClickHouseTripRepository

    if components.clickhouse_client is None:
        raise RuntimeError("startup() did not initialize ClickHouse client")

    return ClickHouseTripRepository(client=components.clickhouse_client)


def main() -> None:
    settings = get_settings()
    components: AppComponents | None = None
    consumer_adapter: KafkaConsumerAdapter | None = None
    handler = ShutdownHandler()
    handler.register()

    try:
        components = startup(mode="consumer")

        trip_repository = _build_trip_repository(components)

        consumer_adapter = KafkaConsumerAdapter(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            group_id=settings.kafka.consumer_group_id,
            topic=settings.kafka.topic,
            batch_size=settings.kafka.batch_size,
            batch_timeout_seconds=settings.kafka.batch_timeout_seconds,
        )

        # Register consumer close as a shutdown callback so that
        # if SIGTERM fires mid-poll, the consumer exits the
        # consume_batches() loop cleanly via the shutdown flag.
        handler.register_callback(lambda: logger.info("Shutdown flag set, finishing current batch"))

        total_batches = 0
        total_rows = 0

        for batch in consumer_adapter.consume_batches(handler):
            try:
                trip_repository.save_batch_from_dicts(
                    batch.rows
                )

                # Offset advances only here -- after confirmed insert.
                consumer_adapter.commit()

                total_batches += 1
                total_rows += batch.size

                logger.info(
                    "Batch persisted",
                    extra={
                        "batch_id": batch.batch_id,
                        "size": batch.size,
                        "total_batches": total_batches,
                        "total_rows": total_rows,
                    },
                )

            except Exception as exc:
                # Insert failed. Do NOT commit. Log and let Kafka replay
                # the batch on restart. If this is a persistent failure
                # (e.g. schema mismatch), it will loop until fixed.
                logger.error(
                    "Failed to persist batch -- offset not committed, will replay: %s",
                    exc,
                    exc_info=True,
                    extra={"batch_id": batch.batch_id, "size": batch.size},
                )
                # Signal shutdown so the process restarts cleanly
                # rather than looping on a broken insert indefinitely.
                handler.request_shutdown()

        logger.info(
            "Consumer finished",
            extra={"total_batches": total_batches, "total_rows": total_rows},
        )

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")

    except Exception as exc:
        logger.critical(
            "Consumer failed with unhandled exception: %s",
            exc,
            exc_info=True,
        )
        sys.exit(1)

    finally:
        if consumer_adapter is not None:
            consumer_adapter.close()
        if components is not None:
            shutdown(components)


if __name__ == "__main__":
    main()
