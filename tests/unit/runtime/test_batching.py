from __future__ import annotations

import time

from etl.runtime.batching import AccumulatedBatch, BatchAccumulator


def _row(n: int = 1) -> dict:
    return {"trip_id": f"trip-{n}", "vendor_id": "Creative Mobile Technologies"}


# should_flush: size threshold

def test_should_flush_false_when_below_threshold():
    acc = BatchAccumulator(max_size=5, max_wait_seconds=60)
    acc.add(_row())
    assert acc.should_flush() is False


def test_should_flush_true_at_threshold():
    acc = BatchAccumulator(max_size=3, max_wait_seconds=60)
    acc.add(_row(1))
    acc.add(_row(2))
    acc.add(_row(3))
    assert acc.should_flush() is True


def test_should_flush_true_above_threshold():
    acc = BatchAccumulator(max_size=2, max_wait_seconds=60)
    for i in range(5):
        acc.add(_row(i))
    assert acc.should_flush() is True


# should_flush: time timeout

def test_should_flush_true_after_timeout():
    acc = BatchAccumulator(max_size=1000, max_wait_seconds=0)
    acc.add(_row())
    # max_wait_seconds=0 means immediately elapsed
    time.sleep(0.01)
    assert acc.should_flush() is True


def test_should_flush_false_before_timeout():
    acc = BatchAccumulator(max_size=1000, max_wait_seconds=60)
    acc.add(_row())
    assert acc.should_flush() is False


# flush

def test_flush_returns_accumulated_rows():
    acc = BatchAccumulator(max_size=10, max_wait_seconds=60)
    acc.add(_row(1))
    acc.add(_row(2))
    batch = acc.flush()
    assert batch.size == 2
    assert batch.rows[0]["trip_id"] == "trip-1"
    assert batch.rows[1]["trip_id"] == "trip-2"


def test_flush_resets_accumulator():
    acc = BatchAccumulator(max_size=10, max_wait_seconds=60)
    acc.add(_row(1))
    acc.flush()
    assert acc.pending_count() == 0


def test_flush_returns_new_batch_id_each_time():
    acc = BatchAccumulator(max_size=10, max_wait_seconds=60)
    acc.add(_row(1))
    batch_a = acc.flush()
    acc.add(_row(2))
    batch_b = acc.flush()
    assert batch_a.batch_id != batch_b.batch_id


def test_flush_resets_timeout():
    acc = BatchAccumulator(max_size=1000, max_wait_seconds=60)
    acc.flush()
    # Immediately after flush the timeout should not be reached
    assert acc.should_flush() is False


def test_flush_on_empty_is_safe():
    acc = BatchAccumulator(max_size=10, max_wait_seconds=60)
    batch = acc.flush()
    assert batch.is_empty()
    assert batch.size == 0


# add_many

def test_add_many_increases_pending_count():
    acc = BatchAccumulator(max_size=100, max_wait_seconds=60)
    acc.add_many([_row(i) for i in range(5)])
    assert acc.pending_count() == 5


# AccumulatedBatch

def test_accumulated_batch_is_empty_true_for_empty():
    batch = AccumulatedBatch(rows=[], batch_id="x", source="kafka")
    assert batch.is_empty() is True


def test_accumulated_batch_is_empty_false_for_non_empty():
    batch = AccumulatedBatch(rows=[_row()], batch_id="x", source="kafka")
    assert batch.is_empty() is False


def test_accumulated_batch_size():
    rows = [_row(i) for i in range(7)]
    batch = AccumulatedBatch(rows=rows, batch_id="x", source="kafka")
    assert batch.size == 7


# seconds_since_last_flush

def test_seconds_since_last_flush_increases_over_time():
    acc = BatchAccumulator(max_size=10, max_wait_seconds=60)
    time.sleep(0.05)
    assert acc.seconds_since_last_flush() >= 0.05
