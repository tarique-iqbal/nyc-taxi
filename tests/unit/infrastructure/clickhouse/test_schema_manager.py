from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

from etl.infrastructure.clickhouse.schema_manager import SchemaManager

# Helpers

def _sha256(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def _write_migration(dir_: Path, filename: str, sql: str) -> Path:
    path = dir_ / filename
    path.write_text(sql, encoding="utf-8")
    return path


# _checksum

def test_checksum_is_deterministic():
    sql = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory"
    c1 = SchemaManager._checksum(sql)
    c2 = SchemaManager._checksum(sql)
    assert c1 == c2


def test_checksum_differs_for_different_sql():
    c1 = SchemaManager._checksum("SELECT 1")
    c2 = SchemaManager._checksum("SELECT 2")
    assert c1 != c2


def test_checksum_is_64_hex_chars():
    checksum = SchemaManager._checksum("CREATE TABLE foo (x Int32) ENGINE = Memory")
    assert len(checksum) == 64
    assert all(c in "0123456789abcdef" for c in checksum)


def test_checksum_matches_sha256():
    sql = "SELECT 1"
    expected = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    assert SchemaManager._checksum(sql) == expected


# _discover_migrations

def test_discover_migrations_sorted_by_filename(tmp_path):
    _write_migration(tmp_path, "003_extra.sql", "SELECT 3")
    _write_migration(tmp_path, "001_initial.sql", "SELECT 1")
    _write_migration(tmp_path, "002_views.sql", "SELECT 2")

    client = MagicMock()
    manager = SchemaManager(client, migrations_dir=tmp_path)
    files = manager._discover_migrations()

    names = [f.name for f in files]
    assert names == ["001_initial.sql", "002_views.sql", "003_extra.sql"]


def test_discover_migrations_returns_empty_for_missing_dir():
    client = MagicMock()
    manager = SchemaManager(client, migrations_dir=Path("/nonexistent/path"))
    files = manager._discover_migrations()
    assert files == []


def test_discover_migrations_ignores_non_sql_files(tmp_path):
    _write_migration(tmp_path, "001_initial.sql", "SELECT 1")
    (tmp_path / "README.md").write_text("docs")
    (tmp_path / "backup.txt").write_text("backup")

    client = MagicMock()
    manager = SchemaManager(client, migrations_dir=tmp_path)
    files = manager._discover_migrations()

    assert len(files) == 1
    assert files[0].name == "001_initial.sql"


def test_discover_migrations_empty_directory_returns_empty(tmp_path):
    client = MagicMock()
    manager = SchemaManager(client, migrations_dir=tmp_path)
    assert manager._discover_migrations() == []


# apply_all: fresh run

def test_apply_all_creates_tracking_table(tmp_path):
    sql = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql)

    client = MagicMock()
    client.execute.side_effect = [None, [], None, None]  # create_table, select, execute sql, insert
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    first_call_sql = client.execute.call_args_list[0].args[0]
    assert "CREATE TABLE IF NOT EXISTS" in first_call_sql
    assert "schema_migrations" in first_call_sql


def test_apply_all_applies_pending_migration(tmp_path):
    sql = "CREATE TABLE IF NOT EXISTS taxi.zones (id Int32) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql)

    client = MagicMock()
    # create_table, select (empty = nothing applied), execute migration, insert tracking row
    client.execute.side_effect = [None, [], None, None]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    executed_sqls = [c.args[0] for c in client.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS taxi.zones" in s for s in executed_sqls)


def test_apply_all_records_migration_in_tracking_table(tmp_path):
    sql = "SELECT 1;"
    _write_migration(tmp_path, "001_test.sql", sql)

    client = MagicMock()
    client.execute.side_effect = [None, [], None, None]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    # Last execute call should be the INSERT into schema_migrations
    last_call = client.execute.call_args_list[-1]
    last_sql = last_call.args[0]
    assert "INSERT INTO taxi.schema_migrations" in last_sql


def test_apply_all_multi_statement_sql_split_on_semicolons(tmp_path):
    sql = "CREATE TABLE a (x Int32) ENGINE = Memory;\nCREATE TABLE b (y Int32) ENGINE = Memory;"
    _write_migration(tmp_path, "001_multi.sql", sql)

    client = MagicMock()
    # create tracking, select, execute stmt 1, execute stmt 2, insert tracking
    client.execute.side_effect = [None, [], None, None, None]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    executed = [c.args[0] for c in client.execute.call_args_list]
    assert any("CREATE TABLE a" in s for s in executed)
    assert any("CREATE TABLE b" in s for s in executed)


# apply_all: already applied

def test_apply_all_skips_already_applied_migration(tmp_path):
    sql = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql)
    checksum = _sha256(sql)

    client = MagicMock()
    # create tracking, select returns already-applied version
    client.execute.side_effect = [None, [("001_initial", checksum)]]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    # Only 2 execute calls: create_tracking_table + select
    # No migration SQL or INSERT executed
    assert client.execute.call_count == 2


def test_apply_all_applies_only_new_migrations(tmp_path):
    sql_001 = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory;"
    sql_002 = "CREATE TABLE IF NOT EXISTS taxi.zones (id Int32) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql_001)
    _write_migration(tmp_path, "002_zones.sql", sql_002)

    checksum_001 = _sha256(sql_001)
    client = MagicMock()
    # 001 already applied, 002 is new
    client.execute.side_effect = [None, [("001_initial", checksum_001)], None, None]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    executed = [c.args[0] for c in client.execute.call_args_list]
    assert not any("CREATE TABLE IF NOT EXISTS taxi.trips" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS taxi.zones" in s for s in executed)


# apply_all: checksum mismatch

def test_apply_all_logs_warning_on_checksum_mismatch(tmp_path, caplog):
    import logging
    sql = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql)

    wrong_checksum = "aaaa" * 16

    client = MagicMock()
    client.execute.side_effect = [None, [("001_initial", wrong_checksum)]]
    manager = SchemaManager(client, migrations_dir=tmp_path)

    with caplog.at_level(logging.WARNING):
        manager.apply_all()

    assert any("checksum" in r.message.lower() for r in caplog.records)


def test_apply_all_does_not_reapply_on_checksum_mismatch(tmp_path):
    sql = "CREATE TABLE IF NOT EXISTS taxi.trips (id String) ENGINE = Memory;"
    _write_migration(tmp_path, "001_initial.sql", sql)
    wrong_checksum = "aaaa" * 16

    client = MagicMock()
    client.execute.side_effect = [None, [("001_initial", wrong_checksum)]]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()

    # Only create_tracking + select called; migration not re-executed
    assert client.execute.call_count == 2


# applied_versions

def test_applied_versions_returns_sorted_list(tmp_path):
    client = MagicMock()
    client.execute.side_effect = [
        None,  # ensure tracking table
        [("002_views", "ccc"), ("001_initial", "bbb")],  # SELECT result (unsorted)
    ]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    versions = manager.applied_versions()
    assert versions == ["001_initial", "002_views"]


def test_applied_versions_empty_when_none_applied(tmp_path):
    client = MagicMock()
    client.execute.side_effect = [None, []]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    assert manager.applied_versions() == []


# apply_all: no migrations

def test_apply_all_with_empty_migrations_dir_does_not_raise(tmp_path):
    client = MagicMock()
    client.execute.side_effect = [None, []]
    manager = SchemaManager(client, migrations_dir=tmp_path)
    manager.apply_all()  # should not raise


def test_apply_all_with_missing_migrations_dir_does_not_raise():
    client = MagicMock()
    client.execute.side_effect = [None, []]
    manager = SchemaManager(client, migrations_dir=Path("/does/not/exist"))
    manager.apply_all()  # should not raise
