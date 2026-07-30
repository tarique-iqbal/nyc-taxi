from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

from etl.config.settings import Settings, get_settings
from etl.observability.structured_logging import setup_logging
from etl.observability.tracing import setup_tracing

if TYPE_CHECKING:
    from wsgiref.simple_server import WSGIServer

    from etl.infrastructure.clickhouse.client import ClickHouseClient
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter
    from etl.infrastructure.kafka.producer import KafkaEventPublisher
    from etl.infrastructure.storage.zone_lookup import CsvZoneRepository

logger = logging.getLogger(__name__)


class Shutdownable(Protocol):
    def shutdown(self) -> None: ...


class _MetricsHttpServer:
    """
    Wraps the (httpd, thread) pair returned by prometheus_client.start_http_server
    so it satisfies the Shutdownable protocol used by AppComponents/shutdown().
    """

    def __init__(self, httpd: WSGIServer, thread: threading.Thread) -> None:
        self._httpd = httpd
        self._thread = thread

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5.0)


@dataclass
class AppComponents:
    """
    Holds all initialised infrastructure components.

    Built by startup() and torn down by shutdown() in the correct order.
    Passed to entrypoints so they do not need to construct their own
    infrastructure instances.
    """

    settings: Settings
    zone_repository: CsvZoneRepository | None = field(default=None)
    clickhouse_client: ClickHouseClient | None = field(default=None)
    kafka_producer: KafkaEventPublisher | None = field(default=None)
    kafka_consumer: KafkaConsumerAdapter | None = field(default=None)
    metrics_server: Shutdownable | None = field(default=None)
    health_server: Shutdownable | None = field(default=None)


def startup(mode: str = "producer") -> AppComponents:
    """
    Initialise all application components in dependency order.

    Startup sequence (order is mandatory):
      1. Logging          -- must be first so all subsequent steps are logged
      2. Tracing          -- must follow logging
      3. ClickHouse       -- zone load needs a healthy DB connection
      4. Zone repository  -- load CSV into memory before domain can enrich
      5. Kafka            -- connect after local resources are ready
      6. Metrics server   -- expose /metrics endpoint

    Args:
        mode: "producer", "consumer", or "both". Controls which
              infrastructure components are initialised.

    Returns:
        AppComponents with all resources ready.

    Raises:
        Any infrastructure exception on connection failure.
        The process should exit if startup() raises.
    """
    settings = get_settings()

    # Step 1: logging
    setup_logging(level=settings.monitoring.log_level)
    logger.info("Starting NYC Taxi ETL", extra={"mode": mode})

    # Step 2: tracing (optional, no-op if opentelemetry not installed)
    setup_tracing(service_name="nyc-taxi-etl")

    components = AppComponents(settings=settings)

    # Step 3: ClickHouse
    if mode in ("consumer", "both"):
        logger.info("Connecting to ClickHouse")
        from etl.infrastructure.clickhouse.client import ClickHouseClient

        components.clickhouse_client = ClickHouseClient(
            host=settings.clickhouse.host,
            port=settings.clickhouse.port,
            database=settings.clickhouse.database,
            user=settings.clickhouse.user,
            password=settings.clickhouse.password,
        )
        components.clickhouse_client.ping()
        logger.info("ClickHouse connection established")

    # Step 4: Zone repository
    logger.info("Loading zone lookup", extra={"path": str(settings.etl.zone_lookup_path)})
    from etl.infrastructure.storage.zone_lookup import CsvZoneRepository

    repo = CsvZoneRepository(path=settings.etl.zone_lookup_path)
    repo.load()
    components.zone_repository = repo
    logger.info("Zone lookup loaded", extra={"zones": repo.zone_count})

    # Step 5: Kafka
    if mode in ("producer", "both"):
        logger.info("Creating Kafka producer")
        from etl.infrastructure.kafka.producer import KafkaEventPublisher

        components.kafka_producer = KafkaEventPublisher(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            acks=settings.kafka.producer_acks,
            retries=settings.kafka.producer_retries,
        )

    if mode in ("consumer", "both"):
        logger.info("Creating Kafka consumer")
        from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter

        components.kafka_consumer = KafkaConsumerAdapter(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            group_id=settings.kafka.consumer_group_id,
            topic=settings.kafka.topic,
        )

    # Step 6: Metrics server
    logger.info("Starting Prometheus metrics server")
    from prometheus_client import start_http_server

    port = (
        settings.monitoring.prometheus_port_consumer
        if mode == "consumer"
        else settings.monitoring.prometheus_port_producer
    )
    httpd, thread = start_http_server(port)
    components.metrics_server = _MetricsHttpServer(httpd, thread)
    logger.info("Prometheus metrics server started", extra={"port": port, "mode": mode})

    logger.info("Startup complete", extra={"mode": mode})
    return components


def shutdown(components: AppComponents) -> None:
    """
    Tear down all infrastructure components in reverse startup order.

    Shutdown sequence:
      1. Flush Kafka producer buffer
      2. Commit final Kafka consumer offsets
      3. Close ClickHouse connection
      4. Stop metrics and health servers

    Each step is wrapped in try/except so a failure in one step does
    not prevent the remaining steps from running.
    """
    logger.info("Initiating graceful shutdown")

    # Step 1: Kafka producer flush
    if components.kafka_producer is not None:
        try:
            logger.info("Flushing Kafka producer")
            components.kafka_producer.flush()
        except Exception as exc:
            logger.error("Error flushing Kafka producer: %s", exc)

    # Step 2: Kafka consumer commit
    if components.kafka_consumer is not None:
        try:
            logger.info("Committing final Kafka consumer offsets")
            components.kafka_consumer.close()
        except Exception as exc:
            logger.error("Error closing Kafka consumer: %s", exc)

    # Step 3: ClickHouse
    if components.clickhouse_client is not None:
        try:
            logger.info("Closing ClickHouse connection")
            components.clickhouse_client.close()
        except Exception as exc:
            logger.error("Error closing ClickHouse: %s", exc)

    # Step 4: Metrics / health servers
    for name, server in [
        ("metrics", components.metrics_server),
        ("health", components.health_server),
    ]:
        if server is not None:
            try:
                server.shutdown()
            except Exception as exc:
                logger.error("Error stopping %s server: %s", name, exc)

    logger.info("Shutdown complete")
