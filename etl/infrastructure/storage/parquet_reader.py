from __future__ import annotations

import logging
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from etl.config.settings import get_settings

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
]

COLUMN_RENAME_MAP: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
}


class ParquetReader:
    """
    Reads NYC Yellow Taxi Parquet files in memory-efficient chunks.

    Uses PyArrow iter_batches() so the full file is never loaded into
    memory at once. Only the columns required by the domain pipeline
    are read -- PyArrow skips unneeded column groups at the I/O level,
    significantly reducing read time and memory pressure.

    Yields batches of raw dicts to the application layer. Column names
    are normalised to snake_case before yielding so the domain layer
    never sees raw TLC field names like "VendorID" or "PULocationID".

    Usage:
        reader = ParquetReader(path=Path("data/raw/yellow_tripdata_2024-01.parquet"))
        for batch in reader.iter_batches():
            process(batch)
    """

    def __init__(
        self,
        path: Path | None = None,
        batch_size: int | None = None,
        source_file: str | None = None,
    ) -> None:
        settings = get_settings()
        self._path = path or settings.etl.parquet_file_path
        self._batch_size = batch_size or settings.etl.parquet_batch_size
        self._source_file = source_file or self._path.name
        self._validate_path()

    def _validate_path(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self._path}")
        if not self._path.suffix == ".parquet":
            raise ValueError(f"Expected .parquet file, got: {self._path.suffix}")

    def _available_columns(self, schema: pa.Schema) -> list[str]:
        """
        Return the intersection of REQUIRED_COLUMNS and columns that
        actually exist in this file. Logs a warning for any missing ones
        so schema drift is visible without crashing the pipeline.
        """
        file_columns = set(schema.names)
        available = []
        for col in REQUIRED_COLUMNS:
            if col in file_columns:
                available.append(col)
            else:
                logger.warning(
                    "Required column missing from Parquet file",
                    extra={"column": col, "file": str(self._path)},
                )
        return available

    def _rename_columns(self, row: dict[str, object]) -> dict[str, object]:
        return {COLUMN_RENAME_MAP.get(k, k): v for k, v in row.items()}

    def _batch_to_dicts(self, batch: pa.RecordBatch) -> list[dict[str, object]]:
        """
        Convert a PyArrow RecordBatch to a list of plain Python dicts.

        pa.RecordBatch.to_pydict() returns {col: [values]} (columnar).
        We transpose to [{col: value}, ...] (row-oriented) because the
        domain pipeline operates record by record.
        """
        columnar = batch.to_pydict()
        num_rows = batch.num_rows
        rows = []
        for i in range(num_rows):
            row = {col: columnar[col][i] for col in columnar}
            rows.append(self._rename_columns(row))
        return rows

    def iter_batches(self) -> Generator[list[dict[str, object]], None, None]:
        """
        Yield successive batches of raw dicts from the Parquet file.

        Each yielded list contains at most batch_size dicts. The final
        batch may be smaller.

        Raises:
            FileNotFoundError: if the Parquet file does not exist.
            pa.lib.ArrowInvalid: if the file is corrupt or not valid Parquet.
        """
        parquet_file: Any = pq.ParquetFile(self._path)  # type: ignore[no-untyped-call]
        columns = self._available_columns(parquet_file.schema_arrow)

        logger.info(
            "Starting Parquet read",
            extra={
                "file": self._source_file,
                "batch_size": self._batch_size,
                "columns": len(columns),
                "total_row_groups": parquet_file.metadata.num_row_groups,
            },
        )

        total_rows = 0
        batch_num = 0

        for record_batch in parquet_file.iter_batches(
            batch_size=self._batch_size,
            columns=columns,
        ):
            rows = self._batch_to_dicts(record_batch)
            total_rows += len(rows)
            batch_num += 1

            logger.debug(
                "Parquet batch ready",
                extra={
                    "batch_num": batch_num,
                    "batch_size": len(rows),
                    "total_rows_so_far": total_rows,
                    "source_file": self._source_file,
                },
            )

            yield rows

        logger.info(
            "Parquet read complete",
            extra={
                "file": self._source_file,
                "total_rows": total_rows,
                "total_batches": batch_num,
            },
        )

    def __iter__(self) -> Iterator[list[dict[str, object]]]:
        return self.iter_batches()

    @property
    def source_file(self) -> str:
        return self._source_file

    @property
    def path(self) -> Path:
        return self._path
