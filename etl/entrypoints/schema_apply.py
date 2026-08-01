"""
ClickHouse schema-apply entrypoint.

Applies all pending migrations from clickhouse/migrations/ via SchemaManager.
Safe to run multiple times -- already-applied migrations are skipped.

Equivalent to scripts/apply_schema.sh, but runnable without docker compose
(e.g. as a Kubernetes Job) since it talks to ClickHouse directly over the
network instead of via `docker compose exec`.

Run:
    python -m etl.entrypoints.schema_apply
"""

from __future__ import annotations

import logging
import sys

from etl.config.settings import get_settings
from etl.infrastructure.clickhouse.client import ClickHouseClient
from etl.infrastructure.clickhouse.schema_manager import SchemaManager
from etl.observability.structured_logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    setup_logging(level=settings.monitoring.log_level)

    client = ClickHouseClient(
        host=settings.clickhouse.host,
        port=settings.clickhouse.port,
        database=settings.clickhouse.database,
        user=settings.clickhouse.user,
        password=settings.clickhouse.password,
    )

    try:
        client.ping()
        manager = SchemaManager(client)
        manager.apply_all()
        logger.info("Applied migrations", extra={"versions": manager.applied_versions()})
    except Exception as exc:
        logger.critical("Schema application failed: %s", exc)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
