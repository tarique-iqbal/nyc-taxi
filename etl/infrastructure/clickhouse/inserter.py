from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from etl.infrastructure.clickhouse.client import ClickHouseClient
from etl.utils.datetime import parse_timestamp

logger = logging.getLogger(__name__)

# Explicit Arrow schema for the trips table.
# Defining it here rather than inferring from the dict avoids type
# mismatches where Python's type inference disagrees with the
# ClickHouse column types (e.g. Decimal vs float, UInt8 vs int64).
TRIPS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("trip_id", pa.string()),
        pa.field("vendor_id", pa.string()),
        pa.field("pickup_datetime", pa.timestamp("us", tz="UTC")),
        pa.field("dropoff_datetime", pa.timestamp("us", tz="UTC")),
        pa.field("trip_duration_seconds", pa.uint32()),
        pa.field("passenger_count", pa.uint8()),
        pa.field("trip_distance", pa.float32()),
        pa.field("pickup_location_id", pa.uint16()),
        pa.field("dropoff_location_id", pa.uint16()),
        pa.field("pickup_zone", pa.string()),
        pa.field("dropoff_zone", pa.string()),
        pa.field("pickup_borough", pa.string()),
        pa.field("dropoff_borough", pa.string()),
        pa.field("fare_amount", pa.float64()),
        pa.field("extra", pa.float64()),
        pa.field("mta_tax", pa.float64()),
        pa.field("tip_amount", pa.float64()),
        pa.field("tolls_amount", pa.float64()),
        pa.field("improvement_surcharge", pa.float64()),
        pa.field("congestion_surcharge", pa.float64()),
        pa.field("airport_fee", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("payment_type", pa.string()),
        pa.field("rate_code", pa.string()),
        pa.field("store_and_fwd_flag", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("batch_id", pa.string()),
        pa.field("source_file", pa.string()),
    ]
)


def _coerce_row(row: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce types in a trip dict to match the Arrow schema.

    Handles both Parquet-sourced dicts (datetime objects) and
    Kafka-sourced dicts (ISO 8601 strings) for datetime fields.
    Decimal amounts cast to float. None numeric values default to 0.
    """
    coerced = dict(row)

    for money_field in (
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
    ):
        v = coerced.get(money_field)
        coerced[money_field] = float(v) if v is not None else 0.0

    for dt_field in ("pickup_datetime", "dropoff_datetime", "ingested_at"):
        v = coerced.get(dt_field)
        if isinstance(v, str):
            v = parse_timestamp(v)
        if isinstance(v, datetime) and v.tzinfo is None:
            v = v.replace(tzinfo=UTC)
        coerced[dt_field] = v

    for uint_field in ("passenger_count",):
        v = coerced.get(uint_field)
        coerced[uint_field] = int(v) if v is not None else 0

    return coerced


class ColumnarInserter:
    """
    Converts a list of trip dicts to a columnar Arrow Table and inserts
    into ClickHouse using the native columnar protocol.

    Pipeline:
      list[dict] -> PyArrow Table (with explicit schema)
                 -> columnar Python lists via to_pylist()
                 -> ClickHouseClient.execute(columnar=True)

    Why bypass pandas:
      clickhouse_driver's insert_dataframe calls .values on each DataFrame
      column expecting a numpy ndarray. pandas 2.0+ maps pa.string() to
      ArrowStringArray (dtype='str'), whose .values is an ArrowExtensionArray,
      not an ndarray -- causing a TypeError at insert time.

      Extracting columnar data via PyArrow's to_pylist() produces plain
      Python lists that clickhouse_driver accepts natively, with no pandas
      dtype negotiation involved.

    The explicit Arrow schema (TRIPS_ARROW_SCHEMA) is still used to enforce
    types during table construction before extraction.
    """

    TABLE = "taxi.trips"

    def __init__(self, client: ClickHouseClient) -> None:
        self._client = client

    def insert(self, rows: list[dict[str, Any]]) -> None:
        """
        Coerce and insert trip dicts into taxi.trips using the columnar protocol.

        Pipeline:
        list[dict] -> coerce types -> PyArrow Table -> per-column Python lists
                    -> clickhouse_driver execute(columnar=True)

        Bypasses pandas entirely: to_pylist() produces plain Python lists which
        clickhouse_driver's columnar path requires. The pandas path fails because
        clickhouse_driver's column_chunks() rejects both ArrowStringArray and
        numpy ndarray -- only list and tuple are accepted.

        Empty list is a no-op -- no round-trip to ClickHouse.
        """
        if not rows:
            return

        coerced = [_coerce_row(row) for row in rows]
        table = pa.Table.from_pylist(coerced, schema=TRIPS_ARROW_SCHEMA)
        columnar_data = [table[name].to_pylist() for name in table.column_names]

        self._client.execute(
            f"INSERT INTO {self.TABLE} VALUES",
            columnar_data,
            columnar=True,
        )

        logger.debug(
            "Columnar insert complete",
            extra={"table": self.TABLE, "rows": len(rows)},
        )
