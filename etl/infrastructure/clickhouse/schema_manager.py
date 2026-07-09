from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from clickhouse_driver.errors import Error as ClickHouseError

from etl.infrastructure.clickhouse.client import ClickHouseClient

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parents[3] / "clickhouse" / "migrations"

MIGRATION_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS taxi.schema_migrations
(
    version     String,
    applied_at  DateTime DEFAULT now(),
    checksum    String
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version
"""


class SchemaManager:
    """
    Applies ClickHouse SQL migrations in order and tracks applied versions.

    Migration files live in clickhouse/migrations/ and are named with a
    numeric prefix: 001_initial_schema.sql, 002_materialized_views.sql.
    They are applied in ascending filename order.

    Each applied migration is recorded in taxi.schema_migrations with its
    SHA-256 checksum. On subsequent runs, already-applied migrations are
    skipped. If a migration file's content changes after it has been applied,
    a warning is logged (the migration is not re-applied).

    All migration SQL uses IF NOT EXISTS so running the same migration
    twice is safe even if the tracking table is lost.

    Usage:
        manager = SchemaManager(client)
        manager.apply_all()
    """

    def __init__(
        self,
        client: ClickHouseClient,
        migrations_dir: Path | None = None,
    ) -> None:
        self._client = client
        self._migrations_dir = migrations_dir or MIGRATIONS_DIR

    def apply_all(self) -> None:
        """
        Ensure the tracking table exists, then apply all pending migrations.
        """
        self._ensure_tracking_table()
        applied = self._applied_versions()
        migration_files = self._discover_migrations()

        if not migration_files:
            logger.warning(
                "No migration files found",
                extra={"dir": str(self._migrations_dir)},
            )
            return

        for path in migration_files:
            version = path.stem
            sql = path.read_text(encoding="utf-8")
            checksum = self._checksum(sql)

            if version in applied:
                if applied[version] != checksum:
                    logger.warning(
                        "Migration checksum mismatch - file changed after apply",
                        extra={"version": version, "path": str(path)},
                    )
                else:
                    logger.debug("Migration already applied", extra={"version": version})
                continue

            self._apply_migration(version, sql, checksum, path)

        logger.info(
            "Schema migrations complete",
            extra={"total": len(migration_files)},
        )

    def applied_versions(self) -> list[str]:
        """Return a sorted list of applied migration versions."""
        self._ensure_tracking_table()
        return sorted(self._applied_versions().keys())

    def _ensure_tracking_table(self) -> None:
        try:
            self._client.execute(MIGRATION_TRACKING_TABLE)
        except ClickHouseError as exc:
            logger.error("Failed to create migration tracking table: %s", exc)
            raise

    def _applied_versions(self) -> dict[str, str]:
        """Return {version: checksum} for all applied migrations."""
        rows = self._client.execute("SELECT version, checksum FROM taxi.schema_migrations FINAL")
        return {row[0]: row[1] for row in rows}

    def _discover_migrations(self) -> list[Path]:
        """
        Return migration files sorted by filename.

        Filename sort ensures 001_ is applied before 002_ regardless
        of filesystem ordering.
        """
        if not self._migrations_dir.exists():
            logger.warning(
                "Migrations directory not found",
                extra={"dir": str(self._migrations_dir)},
            )
            return []

        files = sorted(self._migrations_dir.glob("*.sql"))
        logger.debug(
            "Discovered migration files",
            extra={"count": len(files), "dir": str(self._migrations_dir)},
        )
        return files

    def _apply_migration(
        self,
        version: str,
        sql: str,
        checksum: str,
        path: Path,
    ) -> None:
        """
        Execute a migration SQL file and record it in the tracking table.

        Splits on semicolons to handle multi-statement migration files.
        Empty statements (from trailing semicolons) are skipped.
        """
        logger.info("Applying migration", extra={"version": version, "path": str(path)})

        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for statement in statements:
            try:
                self._client.execute(statement)
            except ClickHouseError as exc:
                logger.error(
                    "Migration statement failed",
                    extra={"version": version, "error": str(exc)},
                )
                raise

        self._client.execute(
            "INSERT INTO taxi.schema_migrations (version, checksum) VALUES",
            [{"version": version, "checksum": checksum}],
        )

        logger.info("Migration applied", extra={"version": version})

    @staticmethod
    def _checksum(sql: str) -> str:
        return hashlib.sha256(sql.encode("utf-8")).hexdigest()
