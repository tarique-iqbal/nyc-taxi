from __future__ import annotations

from unittest.mock import MagicMock, patch

from etl.infrastructure.clickhouse.repository import ClickHouseTripRepository


def _make_trip(trip_id: str = "abc123") -> MagicMock:
    trip = MagicMock()
    trip.to_dict.return_value = {"trip_id": trip_id}
    return trip


@patch("etl.infrastructure.clickhouse.repository.ColumnarInserter")
def _make_repository(MockInserter) -> tuple[ClickHouseTripRepository, MagicMock]:
    client = MagicMock()
    inserter = MagicMock()
    MockInserter.return_value = inserter
    repo = ClickHouseTripRepository(client=client)
    return repo, inserter


# save_batch (producer path: Trip entities)
def test_save_batch_empty_list_is_noop():
    repo, inserter = _make_repository()
    repo.save_batch([])
    inserter.insert.assert_not_called()


def test_save_batch_converts_trips_and_calls_inserter():
    repo, inserter = _make_repository()
    trips = [_make_trip("t1"), _make_trip("t2")]

    repo.save_batch(trips)

    inserter.insert.assert_called_once_with([{"trip_id": "t1"}, {"trip_id": "t2"}])


@patch("etl.infrastructure.clickhouse.repository.batch_insert_duration_seconds")
def test_save_batch_times_the_insert(mock_histogram):
    repo, inserter = _make_repository()
    mock_histogram.time.return_value.__enter__ = MagicMock()
    mock_histogram.time.return_value.__exit__ = MagicMock(return_value=False)

    repo.save_batch([_make_trip()])

    mock_histogram.time.assert_called_once()
    inserter.insert.assert_called_once()


@patch("etl.infrastructure.clickhouse.repository.batch_insert_duration_seconds")
def test_save_batch_does_not_time_when_empty(mock_histogram):
    repo, _ = _make_repository()
    repo.save_batch([])
    mock_histogram.time.assert_not_called()


# save_batch_from_dicts (consumer path: raw dicts from Kafka)
def test_save_batch_from_dicts_empty_list_is_noop():
    repo, inserter = _make_repository()
    repo.save_batch_from_dicts([])
    inserter.insert.assert_not_called()


def test_save_batch_from_dicts_calls_inserter_directly():
    repo, inserter = _make_repository()
    rows = [{"trip_id": "t1"}, {"trip_id": "t2"}]

    repo.save_batch_from_dicts(rows)

    inserter.insert.assert_called_once_with(rows)


@patch("etl.infrastructure.clickhouse.repository.batch_insert_duration_seconds")
def test_save_batch_from_dicts_times_the_insert(mock_histogram):
    repo, inserter = _make_repository()
    mock_histogram.time.return_value.__enter__ = MagicMock()
    mock_histogram.time.return_value.__exit__ = MagicMock(return_value=False)

    repo.save_batch_from_dicts([{"trip_id": "t1"}])

    mock_histogram.time.assert_called_once()
    inserter.insert.assert_called_once()


@patch("etl.infrastructure.clickhouse.repository.batch_insert_duration_seconds")
def test_save_batch_from_dicts_does_not_time_when_empty(mock_histogram):
    repo, _ = _make_repository()
    repo.save_batch_from_dicts([])
    mock_histogram.time.assert_not_called()


# count
@patch("etl.infrastructure.clickhouse.repository.ColumnarInserter")
def test_count_returns_int_from_client_execute(MockInserter):
    client = MagicMock()
    client.execute.return_value = [(42,)]
    repo = ClickHouseTripRepository(client=client)

    assert repo.count() == 42
    client.execute.assert_called_once_with("SELECT count() FROM taxi.trips FINAL")


@patch("etl.infrastructure.clickhouse.repository.ColumnarInserter")
def test_count_returns_zero_when_no_result_rows(MockInserter):
    client = MagicMock()
    client.execute.return_value = []
    repo = ClickHouseTripRepository(client=client)

    assert repo.count() == 0
