from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from etl.runtime.retry import RetryConfig, retry

logger = logging.getLogger(__name__)


class ClickHouseClient:
    """
    Connection wrapper around clickhouse-driver.

    Provides a retry-decorated interface for queries and inserts.
    All ClickHouseTripRepository and SchemaManager operations go
    through this client so retry logic lives in one place.

    async_insert=1 tells ClickHouse to buffer small inserts internally
    and flush them in larger merged writes. Combined with the ETL's own
    BatchAccumulator batching, this prevents insert amplification from
    many small columnar writes.

    wait_for_async_insert=1 blocks until ClickHouse confirms the buffer
    was flushed, giving us a synchronous durability guarantee despite
    the async buffer.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        async_insert: int = 1,
        wait_for_async_insert: int = 1,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._settings = {
            "async_insert": async_insert,
            "wait_for_async_insert": wait_for_async_insert,
        }
        self._client = Client(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            settings=self._settings,
            connect_timeout=10,
            send_receive_timeout=60,
            sync_request_timeout=5,
        )

    @retry(**RetryConfig.HEALTH_CHECK, exceptions=(ClickHouseError, OSError))
    def ping(self) -> bool:
        """
        Verify the ClickHouse connection is alive.

        Called during startup (lifecycle.startup) before the pipeline
        starts consuming. Raises on connection failure after retries.
        """
        self._client.execute("SELECT 1")
        logger.debug("ClickHouse ping OK", extra={"host": self._host, "port": self._port})
        return True

    @retry(**RetryConfig.CLICKHOUSE_INSERT, exceptions=(ClickHouseError, OSError))
    def execute(self, query: str, params: Any = None) -> list[Any]:
        """
        Execute a query and return rows.

        Used for DDL (CREATE TABLE, migrations) and SELECT queries.
        Not used for bulk inserts -- use insert_dataframe() for those.
        """
        return self._client.execute(query, params or {})

    @retry(**RetryConfig.CLICKHOUSE_INSERT, exceptions=(ClickHouseError, OSError))
    def execute_many(self, query: str, rows: Sequence[Any]) -> None:
        """
        Execute an INSERT with a sequence of row tuples.

        Fallback for cases where columnar insert is not suitable.
        For production inserts use insert_dataframe() via ColumnarInserter.
        """
        self._client.execute(query, rows)

    @retry(**RetryConfig.CLICKHOUSE_INSERT, exceptions=(ClickHouseError, OSError))
    def insert_dataframe(
        self,
        table: str,
        dataframe: Any,
        column_names: list[str] | None = None,
    ) -> None:
        """
        Insert a pandas DataFrame using ClickHouse's columnar protocol.

        Columnar format (Arrow -> DataFrame) matches ClickHouse's internal
        storage layout so data travels over the wire in the same format it
        will be stored. 10-50x faster than row-based INSERT for bulk loads.

        column_names is inferred from DataFrame.columns if not provided.
        """
        self._client.insert_dataframe(
            f"INSERT INTO {table} VALUES",
            dataframe,
            settings=self._settings,
        )
        logger.debug(
            "DataFrame inserted",
            extra={"table": table, "rows": len(dataframe)},
        )

    def close(self) -> None:
        """Disconnect from ClickHouse. Called during lifecycle.shutdown()."""
        try:
            self._client.disconnect()
            logger.debug("ClickHouse connection closed")
        except Exception as exc:
            logger.warning("Error closing ClickHouse connection: %s", exc)

    @property
    def database(self) -> str:
        return self._database
