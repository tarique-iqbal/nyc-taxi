from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from etl.infrastructure.clickhouse.client import ClickHouseClient

logger = logging.getLogger(__name__)

# Explicit Arrow schema for the trips table.
# Defining it here rather than inferring from the dict avoids type
# mismatches where Python's type inference disagrees with the
# ClickHouse column types (e.g. Decimal vs float, UInt8 vs int64).
TRIPS_ARROW_SCHEMA = pa.schema([
    pa.field("trip_id",                 pa.string()),
    pa.field("vendor_id",               pa.string()),
    pa.field("pickup_datetime",         pa.timestamp("us", tz="UTC")),
    pa.field("dropoff_datetime",        pa.timestamp("us", tz="UTC")),
    pa.field("trip_duration_seconds",   pa.uint32()),
    pa.field("passenger_count",         pa.uint8()),
    pa.field("trip_distance",           pa.float32()),
    pa.field("pickup_location_id",      pa.uint16()),
    pa.field("dropoff_location_id",     pa.uint16()),
    pa.field("pickup_zone",             pa.string()),
    pa.field("dropoff_zone",            pa.string()),
    pa.field("pickup_borough",          pa.string()),
    pa.field("dropoff_borough",         pa.string()),
    pa.field("fare_amount",             pa.float64()),
    pa.field("extra",                   pa.float64()),
    pa.field("mta_tax",                 pa.float64()),
    pa.field("tip_amount",              pa.float64()),
    pa.field("tolls_amount",            pa.float64()),
    pa.field("improvement_surcharge",   pa.float64()),
    pa.field("congestion_surcharge",    pa.float64()),
    pa.field("airport_fee",             pa.float64()),
    pa.field("total_amount",            pa.float64()),
    pa.field("payment_type",            pa.string()),
    pa.field("rate_code",               pa.string()),
    pa.field("store_and_fwd_flag",      pa.string()),
    pa.field("ingested_at",             pa.timestamp("us", tz="UTC")),
    pa.field("batch_id",                pa.string()),
    pa.field("source_file",             pa.string()),
])


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce types in a trip dict to match the Arrow schema.

    Decimal amounts are cast to float (acceptable precision for
    fare values in transit). datetime without tzinfo is assumed UTC.
    None values for numeric fields default to 0 to prevent Arrow
    null handling from widening the column type.
    """
    coerced = dict(row)

    for money_field in (
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee", "total_amount",
    ):
        v = coerced.get(money_field)
        coerced[money_field] = float(v) if v is not None else 0.0

    for dt_field in ("pickup_datetime", "dropoff_datetime", "ingested_at"):
        v = coerced.get(dt_field)
        if isinstance(v, datetime) and v.tzinfo is None:
            coerced[dt_field] = v.replace(tzinfo=UTC)

    for uint_field in ("passenger_count",):
        v = coerced.get(uint_field)
        coerced[uint_field] = int(v) if v is not None else 0

    return coerced


class ColumnarInserter:
    """
    Converts a list of trip dicts to a columnar DataFrame and inserts
    into ClickHouse using the native columnar protocol.

    Pipeline:
      list[dict] -> PyArrow Table (with explicit schema) -> pandas DataFrame
                 -> ClickHouseClient.insert_dataframe()

    Why columnar is 10-50x faster than row-based:
      ClickHouse stores data column by column on disk. Sending data in
      columnar format means it arrives in the exact layout ClickHouse
      needs to write -- no transposition on the server side.
      Row-based INSERT requires ClickHouse to transpose every batch.

    The explicit Arrow schema (TRIPS_ARROW_SCHEMA) prevents type widening
    where Python's type inference picks int64 for a UInt8 column, which
    causes clickhouse-driver to reject the insert.
    """

    TABLE = "taxi.trips"

    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    def insert(self, rows: list[dict[str, Any]]) -> None:
        """
        Convert rows to columnar format and insert into taxi.trips.

        Empty list is a no-op -- no round-trip to ClickHouse.
        """
        if not rows:
            return

        coerced = [_coerce_row(row) for row in rows]
        table = pa.Table.from_pylist(coerced, schema=TRIPS_ARROW_SCHEMA)
        dataframe = table.to_pandas()

        self._client.insert_dataframe(
            table=self.TABLE,
            dataframe=dataframe,
        )

        logger.debug(
            "Columnar insert complete",
            extra={"table": self.TABLE, "rows": len(rows)},
        )
