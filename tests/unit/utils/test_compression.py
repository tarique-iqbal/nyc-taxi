from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from etl.utils.compression import (
    iter_jsonl_gz,
    list_rejected_files,
    read_jsonl_gz,
    rejected_file_path,
    write_jsonl_gz,
)


def _records(n: int = 3) -> list[dict]:
    return [{"trip_id": f"trip-{i}", "fare": 10.0 + i} for i in range(n)]


# write_jsonl_gz
def test_write_creates_file(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(), path)
    assert path.exists()


def test_write_creates_parent_dirs(tmp_path):
    path = tmp_path / "nested" / "dir" / "batch.jsonl.gz"
    write_jsonl_gz(_records(), path)
    assert path.exists()


def test_write_file_is_gzip(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(), path)
    with gzip.open(path, "rt") as f:
        content = f.read()
    assert len(content) > 0


def test_write_produces_valid_json_lines(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(3), path)
    with gzip.open(path, "rt") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 3
    for line in lines:
        parsed = json.loads(line)
        assert isinstance(parsed, dict)


def test_write_appends_on_second_call(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(2), path)
    write_jsonl_gz(_records(3), path)
    with gzip.open(path, "rt") as f:
        lines = [line for line in f if line.strip()]
    assert len(lines) == 5


def test_write_empty_list_creates_empty_file(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz([], path)
    assert path.exists()
    with gzip.open(path, "rt") as f:
        content = f.read()
    assert content == ""


def test_write_single_record(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz([{"key": "value"}], path)
    with gzip.open(path, "rt") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"key": "value"}


def test_write_preserves_field_values(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    records = [{"trip_id": "abc123", "total_amount": 19.8, "zone": "Midtown Center"}]
    write_jsonl_gz(records, path)
    with gzip.open(path, "rt") as f:
        parsed = json.loads(f.read().strip())
    assert parsed["trip_id"] == "abc123"
    assert abs(parsed["total_amount"] - 19.8) < 1e-9
    assert parsed["zone"] == "Midtown Center"


# read_jsonl_gz
def test_read_returns_list_of_dicts(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(3), path)
    result = read_jsonl_gz(path)
    assert isinstance(result, list)
    assert all(isinstance(r, dict) for r in result)


def test_read_count_matches_written(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(5), path)
    result = read_jsonl_gz(path)
    assert len(result) == 5


def test_read_values_match_written(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    records = [{"trip_id": f"t{i}", "fare": float(i)} for i in range(3)]
    write_jsonl_gz(records, path)
    result = read_jsonl_gz(path)
    assert result[0]["trip_id"] == "t0"
    assert result[2]["trip_id"] == "t2"


def test_read_missing_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_jsonl_gz(tmp_path / "nonexistent.jsonl.gz")


def test_read_empty_file_returns_empty_list(tmp_path):
    path = tmp_path / "empty.jsonl.gz"
    write_jsonl_gz([], path)
    result = read_jsonl_gz(path)
    assert result == []


def test_read_skips_blank_lines(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    with gzip.open(path, "wt") as f:
        f.write('{"a": 1}\n\n{"b": 2}\n\n')
    result = read_jsonl_gz(path)
    assert len(result) == 2


def test_read_malformed_json_raises(tmp_path):
    path = tmp_path / "bad.jsonl.gz"
    with gzip.open(path, "wt") as f:
        f.write('{"valid": 1}\n{not valid json\n')
    with pytest.raises((ValueError, json.JSONDecodeError)):
        read_jsonl_gz(path)


def test_read_write_round_trip(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    original = [{"id": i, "val": f"v{i}"} for i in range(10)]
    write_jsonl_gz(original, path)
    result = read_jsonl_gz(path)
    assert result == original


# iter_jsonl_gz
def test_iter_returns_dicts(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(3), path)
    for record in iter_jsonl_gz(path):
        assert isinstance(record, dict)


def test_iter_yields_correct_count(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(5), path)
    count = sum(1 for _ in iter_jsonl_gz(path))
    assert count == 5


def test_iter_is_lazy(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    write_jsonl_gz(_records(100), path)
    gen = iter_jsonl_gz(path)
    first = next(gen)
    assert isinstance(first, dict)


def test_iter_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(iter_jsonl_gz(tmp_path / "missing.jsonl.gz"))


def test_iter_empty_file_yields_nothing(tmp_path):
    path = tmp_path / "empty.jsonl.gz"
    write_jsonl_gz([], path)
    assert list(iter_jsonl_gz(path)) == []


def test_iter_values_match_written(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    records = [{"n": i} for i in range(5)]
    write_jsonl_gz(records, path)
    result = list(iter_jsonl_gz(path))
    assert [r["n"] for r in result] == list(range(5))


def test_iter_skips_blank_lines(tmp_path):
    path = tmp_path / "batch.jsonl.gz"
    with gzip.open(path, "wt") as f:
        f.write('{"a": 1}\n\n{"b": 2}\n')
    result = list(iter_jsonl_gz(path))
    assert len(result) == 2


# rejected_file_path
def test_rejected_file_path_ends_with_jsonl_gz(tmp_path):
    path = rejected_file_path(tmp_path, "my-batch")
    assert path.suffix == ".gz"
    assert path.name.endswith(".jsonl.gz")


def test_rejected_file_path_contains_batch_id(tmp_path):
    batch_id = "abc-123-def"
    path = rejected_file_path(tmp_path, batch_id)
    assert batch_id in path.name


def test_rejected_file_path_parent_is_rejected_dir(tmp_path):
    path = rejected_file_path(tmp_path, "batch-1")
    assert path.parent == tmp_path


def test_rejected_file_path_is_path_object(tmp_path):
    path = rejected_file_path(tmp_path, "batch-1")
    assert isinstance(path, Path)


def test_rejected_file_path_different_batches_differ(tmp_path):
    a = rejected_file_path(tmp_path, "batch-a")
    b = rejected_file_path(tmp_path, "batch-b")
    assert a != b


# list_rejected_files
def test_list_rejected_files_empty_dir(tmp_path):
    result = list_rejected_files(tmp_path)
    assert result == []


def test_list_rejected_files_missing_dir_returns_empty(tmp_path):
    result = list_rejected_files(tmp_path / "nonexistent")
    assert result == []


def test_list_rejected_files_finds_jsonl_gz(tmp_path):
    (tmp_path / "batch-1.jsonl.gz").touch()
    (tmp_path / "batch-2.jsonl.gz").touch()
    result = list_rejected_files(tmp_path)
    assert len(result) == 2


def test_list_rejected_files_ignores_other_extensions(tmp_path):
    (tmp_path / "batch-1.jsonl.gz").touch()
    (tmp_path / "batch-2.json").touch()
    (tmp_path / "batch-3.txt").touch()
    result = list_rejected_files(tmp_path)
    assert len(result) == 1
    assert result[0].name == "batch-1.jsonl.gz"


def test_list_rejected_files_sorted_by_name(tmp_path):
    (tmp_path / "c.jsonl.gz").touch()
    (tmp_path / "a.jsonl.gz").touch()
    (tmp_path / "b.jsonl.gz").touch()
    result = list_rejected_files(tmp_path)
    names = [f.name for f in result]
    assert names == sorted(names)


def test_list_rejected_files_returns_path_objects(tmp_path):
    (tmp_path / "batch.jsonl.gz").touch()
    result = list_rejected_files(tmp_path)
    assert all(isinstance(f, Path) for f in result)


def test_list_rejected_files_with_actual_content(tmp_path):
    path = rejected_file_path(tmp_path, "batch-abc")
    write_jsonl_gz(_records(2), path)
    result = list_rejected_files(tmp_path)
    assert len(result) == 1
    assert "batch-abc" in result[0].name
