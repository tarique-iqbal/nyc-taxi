from __future__ import annotations

import logging
from typing import Any

from etl.domain.trip.models import Trip
from etl.domain.trip.repositories import TripRepository
from etl.infrastructure.clickhouse.client import ClickHouseClient
from etl.infrastructure.clickhouse.inserter import ColumnarInserter
from etl.infrastructure.monitoring.metrics import batch_insert_duration_seconds

logger = logging.getLogger(__name__)


class ClickHouseTripRepository(TripRepository):
    """
    Implements TripRepository using ClickHouse as the backing store.

    Two insert paths:
      1. save_batch(trips)           -- producer path. Accepts Trip entities,
                                        calls to_dict(), then columnar insert.
      2. save_batch_from_dicts(rows) -- consumer path. Accepts raw dicts from
                                        Kafka messages, goes straight to
                                        columnar insert without entity construction.
                                        Avoids rebuilding domain objects from
                                        already-serialised data.

    Both paths use ColumnarInserter for the actual write. The insert is
    idempotent: ReplacingMergeTree on trip_id means re-inserting the same
    row on Kafka replay keeps only the latest version (by ingested_at).

    count() uses FINAL to return the deduplicated row count. FINAL forces
    ClickHouse to merge parts before counting, which is accurate but slow
    on large tables. Do not call count() in hot paths.
    """

    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client
        self._inserter = ColumnarInserter(client)

    def save_batch(self, trips: list[Trip]) -> None:
        """
        Persist a list of validated Trip entities to ClickHouse.

        Converts each Trip to a flat dict via to_dict() before the
        columnar insert. Called from the producer path when Trip
        entities are available (e.g. in tests or single-process mode).
        """
        if not trips:
            return

        rows = [trip.to_dict() for trip in trips]
        with batch_insert_duration_seconds.time():
            self._inserter.insert(rows)

        logger.info(
            "Trip batch saved",
            extra={"count": len(trips)},
        )

    def save_batch_from_dicts(self, rows: list[dict[str, Any]]) -> None:
        """
        Persist a batch of trip dicts directly to ClickHouse.

        Used by the consumer entrypoint after deserialising Kafka messages.
        The rows are already in the correct flat dict format produced by
        Trip.to_dict() on the producer side, so entity reconstruction is
        unnecessary.
        """
        if not rows:
            return

        with batch_insert_duration_seconds.time():
            self._inserter.insert(rows)

        logger.info(
            "Trip batch saved from dicts",
            extra={"count": len(rows)},
        )

    def count(self) -> int:
        """
        Return the deduplicated trip count.

        Uses FINAL to force merge resolution of ReplacingMergeTree.
        Slow on large tables -- intended for operational checks only,
        not for dashboard queries (use materialized views instead).
        """
        result = self._client.execute("SELECT count() FROM taxi.trips FINAL")
        return int(result[0][0]) if result else 0
