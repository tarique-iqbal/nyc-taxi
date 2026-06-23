from __future__ import annotations

from unittest.mock import MagicMock, patch

from etl.infrastructure.monitoring.health import (
    ComponentHealth,
    HealthChecker,
    HealthReport,
    HealthStatus,
)

# ComponentHealth

def test_component_health_defaults():
    c = ComponentHealth(name="kafka", status=HealthStatus.OK)
    assert c.detail == ""


def test_component_health_with_detail():
    c = ComponentHealth(name="clickhouse", status=HealthStatus.DOWN, detail="Connection refused")
    assert c.detail == "Connection refused"


# HealthReport.overall

def test_overall_ok_when_all_ok():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.OK),
        ComponentHealth("clickhouse", HealthStatus.OK),
    ])
    assert report.overall == HealthStatus.OK


def test_overall_down_when_any_down():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.OK),
        ComponentHealth("clickhouse", HealthStatus.DOWN),
    ])
    assert report.overall == HealthStatus.DOWN


def test_overall_degraded_when_any_degraded_none_down():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.OK),
        ComponentHealth("clickhouse", HealthStatus.DEGRADED),
    ])
    assert report.overall == HealthStatus.DEGRADED


def test_overall_down_beats_degraded():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.DEGRADED),
        ComponentHealth("clickhouse", HealthStatus.DOWN),
    ])
    assert report.overall == HealthStatus.DOWN


def test_overall_ok_for_empty_components():
    report = HealthReport(components=[])
    assert report.overall == HealthStatus.OK


def test_overall_all_down():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.DOWN),
        ComponentHealth("clickhouse", HealthStatus.DOWN),
    ])
    assert report.overall == HealthStatus.DOWN


# HealthReport.as_dict

def test_as_dict_shape():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.OK),
        ComponentHealth("clickhouse", HealthStatus.OK),
    ])
    d = report.as_dict()
    assert "status" in d
    assert "components" in d
    assert d["status"] == "ok"
    assert "kafka" in d["components"]
    assert "clickhouse" in d["components"]


def test_as_dict_component_has_status_and_detail():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.DOWN, detail="timeout"),
    ])
    d = report.as_dict()
    kafka = d["components"]["kafka"]
    assert kafka["status"] == "down"
    assert kafka["detail"] == "timeout"


def test_as_dict_overall_status_matches_overall_property():
    report = HealthReport(components=[
        ComponentHealth("kafka", HealthStatus.DEGRADED),
        ComponentHealth("clickhouse", HealthStatus.OK),
    ])
    d = report.as_dict()
    assert d["status"] == report.overall.value


# HealthStatus is str enum

def test_health_status_values():
    assert HealthStatus.OK == "ok"
    assert HealthStatus.DEGRADED == "degraded"
    assert HealthStatus.DOWN == "down"


def test_health_status_serialises_as_string():
    import json
    report = HealthReport(components=[ComponentHealth("x", HealthStatus.OK)])
    d = report.as_dict()
    serialised = json.dumps(d)
    assert '"ok"' in serialised


# HealthChecker._check_clickhouse

def test_check_clickhouse_ok_when_ping_succeeds():
    client = MagicMock()
    client.ping.return_value = True
    checker = HealthChecker(clickhouse_client=client)
    component = checker._check_clickhouse()
    assert component.status == HealthStatus.OK
    assert component.name == "clickhouse"


def test_check_clickhouse_down_when_ping_raises():
    client = MagicMock()
    client.ping.side_effect = ConnectionError("refused")
    checker = HealthChecker(clickhouse_client=client)
    component = checker._check_clickhouse()
    assert component.status == HealthStatus.DOWN
    assert "refused" in component.detail


def test_check_clickhouse_degraded_when_client_is_none():
    checker = HealthChecker(clickhouse_client=None)
    component = checker._check_clickhouse()
    assert component.status == HealthStatus.DEGRADED
    assert "not configured" in component.detail


# HealthChecker._check_kafka

def test_check_kafka_degraded_when_no_servers():
    checker = HealthChecker(kafka_bootstrap_servers=None)
    component = checker._check_kafka()
    assert component.status == HealthStatus.DEGRADED
    assert "not configured" in component.detail


def test_check_kafka_degraded_when_empty_servers():
    checker = HealthChecker(kafka_bootstrap_servers="")
    component = checker._check_kafka()
    assert component.status == HealthStatus.DEGRADED


@patch("confluent_kafka.admin.AdminClient")
def test_check_kafka_ok_when_admin_client_lists_topics(MockAdminClient):
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value.topics = {"topic-a": None, "topic-b": None}
    MockAdminClient.return_value = mock_admin

    checker = HealthChecker(kafka_bootstrap_servers="localhost:9092")
    component = checker._check_kafka()

    assert component.status == HealthStatus.OK
    assert component.name == "kafka"
    assert "2 topics" in component.detail


@patch("confluent_kafka.admin.AdminClient")
def test_check_kafka_down_when_admin_client_raises(MockAdminClient):
    MockAdminClient.side_effect = Exception("broker unreachable")

    checker = HealthChecker(kafka_bootstrap_servers="localhost:9092")
    component = checker._check_kafka()

    assert component.status == HealthStatus.DOWN
    assert "broker unreachable" in component.detail


# HealthChecker.check

@patch("confluent_kafka.admin.AdminClient")
def test_check_returns_report_with_two_components(MockAdminClient):
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value.topics = {}
    MockAdminClient.return_value = mock_admin

    client = MagicMock()
    client.ping.return_value = True
    checker = HealthChecker(
        clickhouse_client=client,
        kafka_bootstrap_servers="localhost:9092",
    )
    report = checker.check()

    assert len(report.components) == 2
    names = {c.name for c in report.components}
    assert names == {"clickhouse", "kafka"}


def test_check_overall_down_when_clickhouse_fails():
    client = MagicMock()
    client.ping.side_effect = Exception("CH down")
    checker = HealthChecker(clickhouse_client=client, kafka_bootstrap_servers=None)
    report = checker.check()
    assert report.overall == HealthStatus.DOWN
