from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)


class ShutdownHandler:
    """
    Handles SIGTERM and SIGINT for graceful pipeline shutdown.

    On receiving either signal, sets an internal threading.Event.
    The consumer and producer loops check is_shutdown_requested()
    after each batch and exit cleanly after finishing their current work.

    Without graceful shutdown, Docker's `docker stop` sends SIGTERM and
    then SIGKILL after a timeout. If the pipeline is mid-insert when
    SIGKILL lands, the batch is lost and Kafka offsets are not committed,
    causing the batch to be replayed on restart (which is safe with
    ReplacingMergeTree but wastes work).

    With graceful shutdown:
      1. SIGTERM sets the shutdown flag.
      2. Consumer finishes the current batch.
      3. ClickHouse insert completes.
      4. Kafka offset is committed.
      5. Producer flushes its buffer.
      6. Connections are closed in lifecycle.shutdown().
      7. Process exits cleanly with code 0.

    Usage:
        handler = ShutdownHandler()
        handler.register()
        while not handler.is_shutdown_requested:
            batch = accumulator.flush()
            process(batch)
    """

    def __init__(self) -> None:
        self._event = threading.Event()
        self._callbacks: list[Callable[[], None]] = []

    def register(self) -> None:
        """Register signal handlers for SIGTERM and SIGINT."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        logger.debug("Graceful shutdown handler registered for SIGTERM and SIGINT")

    def register_callback(self, callback: Callable[[], None]) -> None:
        """
        Register a callback to run when shutdown is triggered.

        Callbacks are called in registration order after the shutdown
        flag is set. Useful for setting flags in long-running loops
        that cannot poll is_shutdown_requested directly.
        """
        self._callbacks.append(callback)

    @property
    def is_shutdown_requested(self) -> bool:
        """True if a shutdown signal has been received."""
        return self._event.is_set()

    def request_shutdown(self) -> None:
        """
        Trigger shutdown programmatically (without a signal).

        Used in tests and for clean exits after the producer has
        finished reading all Parquet rows.
        """
        self._event.set()
        self._run_callbacks()

    def wait(self, timeout: float | None = None) -> bool:
        """
        Block until shutdown is requested or the timeout expires.

        Returns True if shutdown was requested, False on timeout.
        """
        return self._event.wait(timeout=timeout)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        signal_name = signal.Signals(signum).name
        logger.info(
            "Shutdown signal received, finishing current batch before exit",
            extra={"signal": signal_name},
        )
        self._event.set()
        self._run_callbacks()

    def _run_callbacks(self) -> None:
        for callback in self._callbacks:
            try:
                callback()
            except Exception as exc:
                logger.warning(
                    "Shutdown callback failed: %s", exc, exc_info=True
                )
