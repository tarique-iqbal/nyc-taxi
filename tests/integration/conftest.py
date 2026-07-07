from __future__ import annotations

import contextlib
import uuid
from pathlib import Path

import pytest

from etl.config.settings import get_settings

FIXTURES_DIR = Path(__file__).parents[1] / "fixtures"
SAMPLE_PARQUET = FIXTURES_DIR / "sample_trips.parquet"
INVALID_PARQUET = FIXTURES_DIR / "invalid_trips.parquet"
ZONE_CSV = FIXTURES_DIR / "taxi_zone_lookup.csv"


def _clickhouse_available() -> bool:
    try:
        from etl.infrastructure.clickhouse.client import ClickHouseClient
        settings = get_settings()
        client = ClickHouseClient(
            host=settings.clickhouse.host,
            port=settings.clickhouse.port,
            database=settings.clickhouse.database,
            user=settings.clickhouse.user,
            password=settings.clickhouse.password,
        )
        client.ping()
        client.close()
        return True
    except Exception:
        return False


def _kafka_available() -> bool:
    try:
        from confluent_kafka.admin import AdminClient
        settings = get_settings()
        admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
        admin.list_topics(timeout=3)
        return True
    except Exception:
        return False


requires_clickhouse = pytest.mark.skipif(
    not _clickhouse_available(),
    reason="ClickHouse not reachable -- run: make up",
)

requires_kafka = pytest.mark.skipif(
    not _kafka_available(),
    reason="Kafka not reachable -- run: make up",
)


@pytest.fixture(scope="session")
def settings():
    get_settings.cache_clear()
    return get_settings()


@pytest.fixture(scope="session")
def zone_repository():
    from etl.infrastructure.storage.zone_lookup import CsvZoneRepository
    repo = CsvZoneRepository(path=ZONE_CSV)
    repo.load()
    return repo


@pytest.fixture(scope="session")
def domain_service(zone_repository):
    from etl.domain.trip.services import TripDomainService
    return TripDomainService(zone_repository=zone_repository)


@pytest.fixture(scope="session")
def ch_client(settings):
    from etl.infrastructure.clickhouse.client import ClickHouseClient
    client = ClickHouseClient(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
        user=settings.clickhouse.user,
        password=settings.clickhouse.password,
    )
    yield client
    client.close()

@pytest.fixture(scope="session")
def apply_clickhouse_schema(ch_client):
    from etl.infrastructure.clickhouse.schema_manager import SchemaManager
    SchemaManager(ch_client).apply_all()
    yield

@pytest.fixture(scope="session")
def trip_repository(ch_client, apply_clickhouse_schema):
    from etl.infrastructure.clickhouse.repository import ClickHouseTripRepository
    return ClickHouseTripRepository(client=ch_client)


@pytest.fixture(scope="function")
def batch_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture(scope="function")
def cleanup_batch(ch_client, batch_id):
    """Delete rows inserted by this batch after the test completes."""
    yield batch_id
    with contextlib.suppress(Exception):
        ch_client.execute(
            "ALTER TABLE taxi.trips DELETE WHERE batch_id = %(batch_id)s",
            {"batch_id": batch_id},
        )


@pytest.fixture(scope="session")
def kafka_producer(settings):
    from etl.infrastructure.kafka.producer import KafkaEventPublisher
    producer = KafkaEventPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        acks=settings.kafka.producer_acks,
        retries=settings.kafka.producer_retries,
    )
    yield producer
    producer.flush()


@pytest.fixture(scope="function")
def kafka_consumer(settings):
    from etl.infrastructure.kafka.consumer import KafkaConsumerAdapter
    group_id = f"test-group-{uuid.uuid4()}"
    consumer = KafkaConsumerAdapter(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=group_id,
        topic=settings.kafka.topic,
        batch_size=100,
        batch_timeout_seconds=5,
    )
    yield consumer
    consumer.close()


@pytest.fixture(scope="function")
def tmp_rejected_dir(tmp_path):
    rejected = tmp_path / "rejected"
    rejected.mkdir()
    return rejected


@pytest.fixture(scope="function")
def dead_letter_service(settings, tmp_rejected_dir):
    from etl.infrastructure.kafka.dead_letter_publisher import KafkaDeadLetterPublisher
    return KafkaDeadLetterPublisher(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        dlq_topic=settings.kafka.dlq_topic,
        rejected_dir=tmp_rejected_dir,
    )
