"""
Health check server entrypoint.

Exposes GET /health on HEALTH_PORT (default 8000).
Used by Docker health checks and load balancers to verify
ClickHouse and Kafka connectivity.

Response shape:
    {
        "status": "ok" | "degraded" | "down",
        "components": {
            "clickhouse": {"status": "ok", "detail": ""},
            "kafka":      {"status": "ok", "detail": "3 topics visible"}
        }
    }

HTTP status codes:
    200 -- status is "ok"
    207 -- status is "degraded" (partial availability)
    503 -- status is "down"

Run:
    python -m etl.entrypoints.health_server
"""

from __future__ import annotations

import logging
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from etl.config.settings import get_settings
from etl.infrastructure.monitoring.health import HealthChecker, HealthStatus
from etl.observability.structured_logging import setup_logging

logger = logging.getLogger(__name__)

app = FastAPI(title="NYC Taxi ETL Health", docs_url=None, redoc_url=None)

_checker: HealthChecker | None = None


def _get_checker() -> HealthChecker:
    global _checker
    if _checker is None:
        settings = get_settings()
        try:
            from etl.infrastructure.clickhouse.client import ClickHouseClient

            ch_client = ClickHouseClient(
                host=settings.clickhouse.host,
                port=settings.clickhouse.port,
                database=settings.clickhouse.database,
                user=settings.clickhouse.user,
                password=settings.clickhouse.password,
            )
        except Exception:
            ch_client = None

        _checker = HealthChecker(
            clickhouse_client=ch_client,
            kafka_bootstrap_servers=settings.kafka.bootstrap_servers,
            kafka_topic=settings.kafka.topic,
            kafka_group_id=settings.kafka.consumer_group_id,
        )
    return _checker


@app.get("/health")
def health() -> JSONResponse:
    checker = _get_checker()
    report = checker.check()
    body = report.as_dict()

    status_code_map = {
        HealthStatus.OK: 200,
        HealthStatus.DEGRADED: 207,
        HealthStatus.DOWN: 503,
    }
    status_code = status_code_map.get(report.overall, 503)

    if report.overall != HealthStatus.OK:
        logger.warning("Health check degraded", extra=body)

    return JSONResponse(content=body, status_code=status_code)


@app.get("/ready")
def ready() -> JSONResponse:
    """
    Readiness probe for Kubernetes / Docker.

    Returns 200 when the server is up, regardless of downstream health.
    Distinct from /health so orchestrators can tell the difference between
    "process is alive but dependencies are down" and "process is not running".
    """
    return JSONResponse(content={"status": "ready"}, status_code=200)


def main() -> None:
    settings = get_settings()
    setup_logging(level=settings.monitoring.log_level)

    port = settings.monitoring.health_port

    logger.info("Starting health server", extra={"port": port})

    try:
        uvicorn.run(
            "etl.entrypoints.health_server:app",
            host="0.0.0.0",
            port=port,
            log_config=None,  # suppress uvicorn default logging; use our JSON formatter
            access_log=False,
        )
    except Exception as exc:
        logger.critical("Health server failed to start: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
