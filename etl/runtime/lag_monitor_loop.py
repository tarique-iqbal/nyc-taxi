from __future__ import annotations

import logging
import threading

from etl.infrastructure.monitoring.kafka_lag import KafkaLagMonitor

logger = logging.getLogger(__name__)


class LagMonitorLoop:
    """
    Runs KafkaLagMonitor.poll() on a fixed interval in a managed background thread.

    Decouples "when to poll" from KafkaLagMonitor itself, which stays a
    simple, synchronously-testable poll() call. Started once at consumer
    startup and stopped during graceful shutdown so the thread does not
    outlive the process.

    Usage:
        loop = LagMonitorLoop(monitor, interval_seconds=15.0)
        loop.start()
        ...
        loop.shutdown()
    """

    def __init__(self, monitor: KafkaLagMonitor, interval_seconds: float = 15.0) -> None:
        self._monitor = monitor
        self._interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start polling in a daemon background thread."""
        self._thread = threading.Thread(
            target=self._run,
            name="kafka-lag-monitor",
            daemon=True,
        )
        self._thread.start()

    def shutdown(self) -> None:
        """Stop the background thread and close the underlying monitor's consumer."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_seconds + 5.0)
        self._monitor.close()

    def _run(self) -> None:
        # wait() returns True as soon as stop() is set, False on each timeout --
        # this polls every interval_seconds without busy-waiting.
        while not self._stop_event.wait(self._interval_seconds):
            self._monitor.poll()
