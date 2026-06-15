"""
Producer entrypoint.

Reads NYC Yellow Taxi trip data from a Parquet file in batches,
runs each batch through the domain pipeline (validate, enrich,
normalise, deduplicate), and publishes valid trips to the Kafka
topic configured in .env.

Startup sequence:
  1. lifecycle.startup("producer") -- logging, tracing, zones, Kafka producer
  2. Wire application services with initialised components
  3. Run IngestionService until EOF or SIGTERM
  4. lifecycle.shutdown() -- flush producer, close connections

Run:
    python -m etl.entrypoints.producer
"""

from __future__ import annotations

import logging
import sys

from etl.application.services.ingestion_service import IngestionService
from etl.application.services.validation_service import ValidationService
from etl.config.settings import get_settings
from etl.domain.dead_letter.services import DeadLetterService
from etl.domain.trip.services import TripDomainService
from etl.infrastructure.kafka.topic_manager import TopicManager
from etl.infrastructure.storage.parquet_reader import ParquetReader
from etl.runtime.lifecycle import AppComponents, shutdown, startup
from etl.runtime.shutdown import ShutdownHandler

logger = logging.getLogger(__name__)


def _build_dead_letter_service(components: AppComponents) -> DeadLetterService:
    """
    Wire the concrete DeadLetterService implementation.

    Imports are deferred so this module is importable before
    infrastructure/kafka/dead_letter_publisher.py is generated.
    The producer process cannot start without it, but tests can
    import this module for inspection without triggering the import.
    """
    from etl.infrastructure.kafka.dead_letter_publisher import (
        KafkaDeadLetterPublisher,
    )
    settings = components.settings
    return KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        dlq_topic=settings.kafka.dlq_topic,
        rejected_dir=settings.etl.rejected_dir,
    )


def main() -> None:
    settings = get_settings()
    components: AppComponents | None = None
    handler = ShutdownHandler()
    handler.register()

    try:
        components = startup(mode="producer")

        # Ensure Kafka topics exist before publishing.
        topic_manager = TopicManager(
            bootstrap_servers=settings.kafka.bootstrap_servers
        )
        topic_manager.ensure_topics_exist()

        domain_service = TripDomainService(
            zone_repository=components.zone_repository
        )
        dead_letter_service = _build_dead_letter_service(components)

        reader = ParquetReader(
            path=settings.etl.parquet_file_path,
            batch_size=settings.etl.parquet_batch_size,
        )

        service = IngestionService(
            reader=reader,
            domain_service=domain_service,
            publisher=components.kafka_producer,
            dead_letter_service=dead_letter_service,
            topic=settings.kafka.topic,
            shutdown_handler=handler,
            validation_service=ValidationService(),
        )

        summary = service.run()

        logger.info("Producer finished", extra=summary.as_dict())

        if summary.reject_rate > 0.1:
            logger.warning(
                "Reject rate above 10%%",
                extra={"reject_rate": summary.reject_rate},
            )

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")

    except Exception as exc:
        logger.critical(
            "Producer failed with unhandled exception: %s",
            exc,
            exc_info=True,
        )
        sys.exit(1)

    finally:
        if components is not None:
            shutdown(components)


if __name__ == "__main__":
    main()
