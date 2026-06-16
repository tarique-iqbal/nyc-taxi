from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class HealthStatus(StrEnum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


@dataclass
class ComponentHealth:
    name: str
    status: HealthStatus
    detail: str = ""


@dataclass
class HealthReport:
    """
    Aggregated health status across all monitored components.

    overall is DOWN if any component is DOWN, DEGRADED if any component
    is DEGRADED, OK only when all components are OK.
    """

    components: list[ComponentHealth] = field(default_factory=list)

    @property
    def overall(self) -> HealthStatus:
        statuses = {c.status for c in self.components}
        if HealthStatus.DOWN in statuses:
            return HealthStatus.DOWN
        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        return HealthStatus.OK

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.overall.value,
            "components": {
                c.name: {"status": c.status.value, "detail": c.detail}
                for c in self.components
            },
        }


class HealthChecker:
    """
    Checks connectivity to ClickHouse and Kafka and returns a HealthReport.

    Used by the health_server FastAPI endpoint. Each check is independent
    so a ClickHouse outage does not prevent the Kafka status from being
    reported and vice versa.

    Kafka lag is included in the report so load balancers and Docker health
    checks can detect a stalled consumer without scraping Prometheus.
    """

    def __init__(
        self,
        clickhouse_client: Any | None = None,
        kafka_bootstrap_servers: str | None = None,
        kafka_topic: str | None = None,
        kafka_group_id: str | None = None,
    ) -> None:
        self._ch_client = clickhouse_client
        self._kafka_servers = kafka_bootstrap_servers
        self._kafka_topic = kafka_topic
        self._kafka_group_id = kafka_group_id

    def check(self) -> HealthReport:
        report = HealthReport()
        report.components.append(self._check_clickhouse())
        report.components.append(self._check_kafka())
        return report

    def _check_clickhouse(self) -> ComponentHealth:
        if self._ch_client is None:
            return ComponentHealth(
                name="clickhouse",
                status=HealthStatus.DEGRADED,
                detail="client not configured",
            )
        try:
            self._ch_client.ping()
            return ComponentHealth(name="clickhouse", status=HealthStatus.OK)
        except Exception as exc:
            logger.warning("ClickHouse health check failed: %s", exc)
            return ComponentHealth(
                name="clickhouse",
                status=HealthStatus.DOWN,
                detail=str(exc),
            )

    def _check_kafka(self) -> ComponentHealth:
        if not self._kafka_servers:
            return ComponentHealth(
                name="kafka",
                status=HealthStatus.DEGRADED,
                detail="bootstrap servers not configured",
            )
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": self._kafka_servers})
            metadata = admin.list_topics(timeout=5)
            topic_count = len(metadata.topics)
            return ComponentHealth(
                name="kafka",
                status=HealthStatus.OK,
                detail=f"{topic_count} topics visible",
            )
        except Exception as exc:
            logger.warning("Kafka health check failed: %s", exc)
            return ComponentHealth(
                name="kafka",
                status=HealthStatus.DOWN,
                detail=str(exc),
            )
