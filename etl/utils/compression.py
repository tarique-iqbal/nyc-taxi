from __future__ import annotations

import gzip
import logging
from collections.abc import Iterator
from pathlib import Path

from etl.utils.json import dumps, loads

logger = logging.getLogger(__name__)

JSONL_GZ_SUFFIX = ".jsonl.gz"


def write_jsonl_gz(records: list[dict], path: Path) -> None:
    """
    Append records to a gzip-compressed JSON Lines file.

    Opens in append mode ('at') so multiple batches can accumulate
    in the same file without truncating earlier entries.

    Creates the parent directory if it does not exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "at", encoding="utf-8") as f:
        for record in records:
            f.write(dumps(record) + "\n")

    logger.debug(
        "Wrote records to compressed file",
        extra={"path": str(path), "count": len(records)},
    )


def read_jsonl_gz(path: Path) -> list[dict]:
    """
    Read all records from a gzip-compressed JSON Lines file.

    Skips blank lines silently. Raises on malformed JSON so callers
    are aware of corruption rather than silently dropping records.
    """
    if not path.exists():
        raise FileNotFoundError(f"Compressed file not found: {path}")

    records = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(loads(line))
            except Exception as exc:
                raise ValueError(
                    f"Malformed JSON on line {line_num} of {path}: {exc}"
                ) from exc

    return records


def iter_jsonl_gz(path: Path) -> Iterator[dict]:
    """
    Lazily iterate over records in a gzip-compressed JSON Lines file.

    Yields one record at a time without loading the whole file into memory.
    Useful for large rejected files during DLQ replay.
    """
    if not path.exists():
        raise FileNotFoundError(f"Compressed file not found: {path}")

    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield loads(line)


def rejected_file_path(rejected_dir: Path, batch_id: str) -> Path:
    """
    Build the standard file path for a batch's rejected records.

    Convention: <rejected_dir>/<batch_id>.jsonl.gz
    One file per batch keeps rejected records grouped by their
    originating correlation ID.
    """
    return rejected_dir / f"{batch_id}{JSONL_GZ_SUFFIX}"


def list_rejected_files(rejected_dir: Path) -> list[Path]:
    """
    Return all .jsonl.gz files in the rejected directory, sorted by name.

    Used by the DLQ replay service to discover files for reprocessing.
    """
    if not rejected_dir.exists():
        return []
    return sorted(rejected_dir.glob(f"*{JSONL_GZ_SUFFIX}"))
