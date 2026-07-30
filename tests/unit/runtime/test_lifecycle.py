from __future__ import annotations

from dataclasses import fields
from unittest.mock import MagicMock, patch

from etl.config.settings import Settings
from etl.runtime.lifecycle import AppComponents, shutdown


def _make_settings() -> Settings:
    return Settings()


def test_app_components_requires_settings():
    settings = _make_settings()
    components = AppComponents(settings=settings)
    assert components.settings is settings


def test_app_components_all_optional_fields_default_to_none():
    components = AppComponents(settings=_make_settings())
    assert components.zone_repository is None
    assert components.clickhouse_client is None
    assert components.kafka_producer is None
    assert components.kafka_consumer is None
    assert components.metrics_server is None
    assert components.health_server is None


def test_app_components_fields_can_be_set():
    components = AppComponents(settings=_make_settings())
    mock_client = MagicMock()
    components.clickhouse_client = mock_client
    assert components.clickhouse_client is mock_client


def test_app_components_is_dataclass():
    import dataclasses

    assert dataclasses.is_dataclass(AppComponents)


def test_app_components_field_names():
    field_names = {f.name for f in fields(AppComponents)}
    assert "settings" in field_names
    assert "zone_repository" in field_names
    assert "clickhouse_client" in field_names
    assert "kafka_producer" in field_names
    assert "kafka_consumer" in field_names
    assert "metrics_server" in field_names
    assert "health_server" in field_names


# shutdown: kafka producer
def test_shutdown_flushes_kafka_producer():
    components = AppComponents(settings=_make_settings())
    components.kafka_producer = MagicMock()
    shutdown(components)
    components.kafka_producer.flush.assert_called_once()


def test_shutdown_skips_kafka_producer_when_none():
    components = AppComponents(settings=_make_settings())
    # Should not raise
    shutdown(components)


def test_shutdown_continues_after_kafka_producer_flush_raises():
    components = AppComponents(settings=_make_settings())
    components.kafka_producer = MagicMock()
    components.kafka_producer.flush.side_effect = RuntimeError("broker down")
    components.clickhouse_client = MagicMock()

    shutdown(components)

    # ClickHouse should still be closed despite Kafka flush failure
    components.clickhouse_client.close.assert_called_once()


# shutdown: kafka consumer
def test_shutdown_closes_kafka_consumer():
    components = AppComponents(settings=_make_settings())
    components.kafka_consumer = MagicMock()
    shutdown(components)
    components.kafka_consumer.close.assert_called_once()


def test_shutdown_skips_kafka_consumer_when_none():
    components = AppComponents(settings=_make_settings())
    shutdown(components)  # should not raise


def test_shutdown_continues_after_kafka_consumer_close_raises():
    components = AppComponents(settings=_make_settings())
    components.kafka_consumer = MagicMock()
    components.kafka_consumer.close.side_effect = Exception("consumer error")
    components.clickhouse_client = MagicMock()

    shutdown(components)

    components.clickhouse_client.close.assert_called_once()


# shutdown: clickhouse
def test_shutdown_closes_clickhouse_client():
    components = AppComponents(settings=_make_settings())
    components.clickhouse_client = MagicMock()
    shutdown(components)
    components.clickhouse_client.close.assert_called_once()


def test_shutdown_skips_clickhouse_when_none():
    components = AppComponents(settings=_make_settings())
    shutdown(components)  # should not raise


def test_shutdown_continues_after_clickhouse_close_raises():
    components = AppComponents(settings=_make_settings())
    components.clickhouse_client = MagicMock()
    components.clickhouse_client.close.side_effect = Exception("CH error")
    components.metrics_server = MagicMock()

    shutdown(components)

    components.metrics_server.shutdown.assert_called_once()


# shutdown: servers
def test_shutdown_stops_metrics_server():
    components = AppComponents(settings=_make_settings())
    components.metrics_server = MagicMock()
    shutdown(components)
    components.metrics_server.shutdown.assert_called_once()


def test_shutdown_stops_health_server():
    components = AppComponents(settings=_make_settings())
    components.health_server = MagicMock()
    shutdown(components)
    components.health_server.shutdown.assert_called_once()


def test_shutdown_skips_servers_when_none():
    components = AppComponents(settings=_make_settings())
    shutdown(components)  # should not raise


def test_shutdown_continues_after_metrics_server_raises():
    components = AppComponents(settings=_make_settings())
    components.metrics_server = MagicMock()
    components.metrics_server.shutdown.side_effect = Exception("server error")
    components.health_server = MagicMock()

    shutdown(components)

    components.health_server.shutdown.assert_called_once()


# shutdown: order guarantee
def test_shutdown_flushes_producer_before_closing_clickhouse():
    """
    Kafka producer must be flushed before ClickHouse closes.
    Ensures buffered messages are delivered before connections drop.
    """
    call_order = []
    components = AppComponents(settings=_make_settings())
    components.kafka_producer = MagicMock()
    components.kafka_producer.flush.side_effect = lambda: call_order.append("kafka_flush")
    components.clickhouse_client = MagicMock()
    components.clickhouse_client.close.side_effect = lambda: call_order.append("ch_close")

    shutdown(components)

    assert call_order.index("kafka_flush") < call_order.index("ch_close")


def test_shutdown_closes_consumer_before_clickhouse():
    """
    Kafka consumer must be closed (committing final offsets) before
    ClickHouse connection is dropped.
    """
    call_order = []
    components = AppComponents(settings=_make_settings())
    components.kafka_consumer = MagicMock()
    components.kafka_consumer.close.side_effect = lambda: call_order.append("kafka_close")
    components.clickhouse_client = MagicMock()
    components.clickhouse_client.close.side_effect = lambda: call_order.append("ch_close")

    shutdown(components)

    assert call_order.index("kafka_close") < call_order.index("ch_close")


# shutdown: fully populated components
def test_shutdown_with_all_components_set():
    components = AppComponents(settings=_make_settings())
    components.kafka_producer = MagicMock()
    components.kafka_consumer = MagicMock()
    components.clickhouse_client = MagicMock()
    components.metrics_server = MagicMock()
    components.health_server = MagicMock()

    shutdown(components)

    components.kafka_producer.flush.assert_called_once()
    components.kafka_consumer.close.assert_called_once()
    components.clickhouse_client.close.assert_called_once()
    components.metrics_server.shutdown.assert_called_once()
    components.health_server.shutdown.assert_called_once()


def test_shutdown_with_no_components_set():
    components = AppComponents(settings=_make_settings())
    shutdown(components)  # should not raise at all


# startup: mode parameter (mocked)
@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
@patch("etl.infrastructure.kafka.producer.KafkaEventPublisher")
def test_startup_producer_mode_creates_kafka_producer(
    MockPublisher, MockZoneRepo, mock_start_http_server
):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    components = startup(mode="producer")

    MockPublisher.assert_called_once()
    assert components.kafka_consumer is None


@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
@patch("etl.infrastructure.kafka.consumer.KafkaConsumerAdapter")
@patch("etl.infrastructure.clickhouse.client.ClickHouseClient")
def test_startup_consumer_mode_creates_clickhouse_and_consumer(
    MockCHClient, MockConsumer, MockZoneRepo, mock_start_http_server
):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    mock_ch_instance = MagicMock()
    MockCHClient.return_value = mock_ch_instance

    components = startup(mode="consumer")

    MockCHClient.assert_called_once()
    MockConsumer.assert_called_once()
    assert components.kafka_producer is None


@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
def test_startup_always_loads_zone_repository(MockZoneRepo, mock_start_http_server):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    startup(mode="producer")

    mock_repo_instance.load.assert_called_once()


@patch("prometheus_client.start_http_server")
@patch("etl.runtime.lifecycle.setup_logging")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
def test_startup_calls_setup_logging_first(MockZoneRepo, mock_logging, mock_start_http_server):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    call_order = []
    mock_logging.side_effect = lambda **kw: call_order.append("logging")

    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance
    MockZoneRepo.side_effect = lambda **kw: (call_order.append("zone_repo"), mock_repo_instance)[1]

    startup(mode="producer")

    assert call_order[0] == "logging"


@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
def test_startup_returns_app_components(MockZoneRepo, mock_start_http_server):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    result = startup(mode="producer")

    assert isinstance(result, AppComponents)
    assert result.settings is not None


# startup: metrics server wiring
@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
@patch("etl.infrastructure.kafka.producer.KafkaEventPublisher")
def test_startup_producer_mode_starts_metrics_server_on_producer_port(
    MockPublisher, MockZoneRepo, mock_start_http_server
):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    components = startup(mode="producer")

    mock_start_http_server.assert_called_once_with(
        components.settings.monitoring.prometheus_port_producer
    )


@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
@patch("etl.infrastructure.kafka.consumer.KafkaConsumerAdapter")
@patch("etl.infrastructure.clickhouse.client.ClickHouseClient")
def test_startup_consumer_mode_starts_metrics_server_on_consumer_port(
    MockCHClient, MockConsumer, MockZoneRepo, mock_start_http_server
):
    from etl.runtime.lifecycle import startup

    mock_start_http_server.return_value = (MagicMock(), MagicMock())
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance
    MockCHClient.return_value = MagicMock()

    components = startup(mode="consumer")

    mock_start_http_server.assert_called_once_with(
        components.settings.monitoring.prometheus_port_consumer
    )


@patch("prometheus_client.start_http_server")
@patch("etl.infrastructure.storage.zone_lookup.CsvZoneRepository")
@patch("etl.infrastructure.kafka.producer.KafkaEventPublisher")
def test_startup_sets_metrics_server_component(MockPublisher, MockZoneRepo, mock_start_http_server):
    from etl.runtime.lifecycle import startup

    mock_httpd = MagicMock()
    mock_thread = MagicMock()
    mock_start_http_server.return_value = (mock_httpd, mock_thread)
    mock_repo_instance = MagicMock()
    mock_repo_instance.zone_count = 265
    MockZoneRepo.return_value = mock_repo_instance

    components = startup(mode="producer")

    assert components.metrics_server is not None
    components.metrics_server.shutdown()
    mock_httpd.shutdown.assert_called_once()
    mock_httpd.server_close.assert_called_once()
    mock_thread.join.assert_called_once()
