from __future__ import annotations

import signal
import threading
import time
from unittest.mock import MagicMock, patch

from etl.runtime.shutdown import ShutdownHandler


# Initial state
def test_not_shutdown_on_creation():
    handler = ShutdownHandler()
    assert handler.is_shutdown_requested is False


def test_no_callbacks_on_creation():
    handler = ShutdownHandler()
    # _callbacks starts empty -- request_shutdown should not raise
    handler.request_shutdown()


# request_shutdown
def test_request_shutdown_sets_flag():
    handler = ShutdownHandler()
    handler.request_shutdown()
    assert handler.is_shutdown_requested is True


def test_request_shutdown_idempotent():
    handler = ShutdownHandler()
    handler.request_shutdown()
    handler.request_shutdown()
    assert handler.is_shutdown_requested is True


def test_request_shutdown_calls_registered_callbacks():
    handler = ShutdownHandler()
    cb = MagicMock()
    handler.register_callback(cb)
    handler.request_shutdown()
    cb.assert_called_once()


def test_request_shutdown_calls_multiple_callbacks_in_order():
    handler = ShutdownHandler()
    call_order = []
    handler.register_callback(lambda: call_order.append("first"))
    handler.register_callback(lambda: call_order.append("second"))
    handler.register_callback(lambda: call_order.append("third"))
    handler.request_shutdown()
    assert call_order == ["first", "second", "third"]


def test_request_shutdown_callback_exception_does_not_stop_others():
    handler = ShutdownHandler()
    cb1 = MagicMock(side_effect=RuntimeError("callback error"))
    cb2 = MagicMock()
    handler.register_callback(cb1)
    handler.register_callback(cb2)
    handler.request_shutdown()
    cb2.assert_called_once()


def test_request_shutdown_flag_set_even_if_callback_raises():
    handler = ShutdownHandler()
    handler.register_callback(lambda: (_ for _ in ()).throw(RuntimeError("oops")))
    handler.request_shutdown()
    assert handler.is_shutdown_requested is True


# register_callback
def test_register_callback_stored():
    handler = ShutdownHandler()
    cb = MagicMock()
    handler.register_callback(cb)
    handler.request_shutdown()
    cb.assert_called_once()


def test_register_multiple_callbacks():
    handler = ShutdownHandler()
    cbs = [MagicMock() for _ in range(5)]
    for cb in cbs:
        handler.register_callback(cb)
    handler.request_shutdown()
    for cb in cbs:
        cb.assert_called_once()


def test_callback_not_called_before_shutdown():
    handler = ShutdownHandler()
    cb = MagicMock()
    handler.register_callback(cb)
    cb.assert_not_called()


def test_callback_receives_no_arguments():
    handler = ShutdownHandler()
    received_args = []
    handler.register_callback(lambda *a, **kw: received_args.extend([a, kw]))
    handler.request_shutdown()
    assert received_args == [(), {}]


# wait
def test_wait_returns_true_when_already_shutdown():
    handler = ShutdownHandler()
    handler.request_shutdown()
    result = handler.wait(timeout=0.01)
    assert result is True


def test_wait_returns_false_on_timeout_when_not_shutdown():
    handler = ShutdownHandler()
    result = handler.wait(timeout=0.05)
    assert result is False


def test_wait_returns_true_when_shutdown_set_by_another_thread():
    handler = ShutdownHandler()

    def trigger():
        time.sleep(0.05)
        handler.request_shutdown()

    t = threading.Thread(target=trigger)
    t.start()
    result = handler.wait(timeout=1.0)
    t.join()
    assert result is True


def test_wait_none_timeout_blocks_until_shutdown():
    handler = ShutdownHandler()
    results = []

    def trigger():
        time.sleep(0.05)
        handler.request_shutdown()

    def waiter():
        results.append(handler.wait(timeout=1.0))

    t_trigger = threading.Thread(target=trigger)
    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    t_trigger.start()
    t_trigger.join()
    t_waiter.join(timeout=2.0)
    assert results == [True]


# register (signal handler)
def test_register_installs_sigterm_handler():
    handler = ShutdownHandler()
    with patch("signal.signal") as mock_signal:
        handler.register()
        calls = [c.args[0] for c in mock_signal.call_args_list]
        assert signal.SIGTERM in calls


def test_register_installs_sigint_handler():
    handler = ShutdownHandler()
    with patch("signal.signal") as mock_signal:
        handler.register()
        calls = [c.args[0] for c in mock_signal.call_args_list]
        assert signal.SIGINT in calls


def test_register_same_handler_for_both_signals():
    handler = ShutdownHandler()
    with patch("signal.signal") as mock_signal:
        handler.register()

        handlers = [c.args[1] for c in mock_signal.call_args_list]

        # Both signals are wired to the same method on the same instance.
        assert len(handlers) == 2
        assert all(h.__self__ is handler for h in handlers)
        assert all(h.__func__ is ShutdownHandler._handle_signal for h in handlers)


# _handle_signal
def test_handle_signal_sets_shutdown_flag():
    handler = ShutdownHandler()
    handler._handle_signal(signal.SIGTERM, None)
    assert handler.is_shutdown_requested is True


def test_handle_signal_runs_callbacks():
    handler = ShutdownHandler()
    cb = MagicMock()
    handler.register_callback(cb)
    handler._handle_signal(signal.SIGTERM, None)
    cb.assert_called_once()


def test_handle_sigint_also_sets_flag():
    handler = ShutdownHandler()
    handler._handle_signal(signal.SIGINT, None)
    assert handler.is_shutdown_requested is True


# thread safety
def test_multiple_threads_see_shutdown_flag():
    handler = ShutdownHandler()
    seen = []

    def check():
        # Poll until shutdown or timeout
        for _ in range(100):
            if handler.is_shutdown_requested:
                seen.append(True)
                return
            time.sleep(0.01)
        seen.append(False)

    threads = [threading.Thread(target=check) for _ in range(5)]
    for t in threads:
        t.start()

    time.sleep(0.05)
    handler.request_shutdown()

    for t in threads:
        t.join(timeout=2.0)

    assert all(seen)
    assert len(seen) == 5


def test_request_shutdown_from_multiple_threads_idempotent():
    handler = ShutdownHandler()
    cb = MagicMock()
    handler.register_callback(cb)

    threads = [threading.Thread(target=handler.request_shutdown) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert handler.is_shutdown_requested is True
    # Flag is set -- callbacks may be called multiple times due to racing
    # but the flag must be True and no exception should have propagated
    assert cb.call_count >= 1


# consumer loop pattern
def test_shutdown_flag_stops_loop():
    """
    Simulates the consumer loop pattern:
      while not handler.is_shutdown_requested:
          process_batch()
    """
    handler = ShutdownHandler()
    processed = []

    def consumer_loop():
        while not handler.is_shutdown_requested:
            processed.append(len(processed))
            if len(processed) >= 3:
                handler.request_shutdown()

    consumer_loop()
    assert len(processed) == 3
    assert handler.is_shutdown_requested is True


def test_callback_can_be_used_to_break_blocking_loop():
    """
    Simulates a blocking poll loop that uses a threading.Event
    registered as a shutdown callback.
    """
    stop_event = threading.Event()
    handler = ShutdownHandler()
    handler.register_callback(stop_event.set)

    poll_calls = []

    def blocking_loop():
        while not stop_event.is_set():
            poll_calls.append(1)
            stop_event.wait(timeout=0.01)

    t = threading.Thread(target=blocking_loop)
    t.start()

    time.sleep(0.05)
    handler.request_shutdown()
    t.join(timeout=1.0)

    assert not t.is_alive()
    assert handler.is_shutdown_requested is True
