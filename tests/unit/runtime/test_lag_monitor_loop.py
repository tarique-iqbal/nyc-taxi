from __future__ import annotations

import time
from unittest.mock import MagicMock

from etl.runtime.lag_monitor_loop import LagMonitorLoop


def _make_monitor() -> MagicMock:
    return MagicMock()


def test_start_spawns_a_background_thread():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.start()
    try:
        assert loop._thread is not None
        assert loop._thread.is_alive()
        assert loop._thread.daemon is True
    finally:
        loop.shutdown()


def test_loop_polls_the_monitor_periodically():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.start()
    try:
        deadline = time.monotonic() + 2.0
        while monitor.poll.call_count < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert monitor.poll.call_count >= 2
    finally:
        loop.shutdown()


def test_shutdown_stops_the_thread():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.start()
    loop.shutdown()

    assert loop._thread is not None
    assert not loop._thread.is_alive()


def test_shutdown_closes_the_monitor():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.start()
    loop.shutdown()

    monitor.close.assert_called_once()


def test_shutdown_without_start_does_not_raise():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.shutdown()  # should not raise even though start() was never called

    monitor.close.assert_called_once()


def test_shutdown_stops_polling():
    monitor = _make_monitor()
    loop = LagMonitorLoop(monitor, interval_seconds=0.01)

    loop.start()
    time.sleep(0.05)
    loop.shutdown()

    count_after_shutdown = monitor.poll.call_count
    time.sleep(0.05)

    assert monitor.poll.call_count == count_after_shutdown
