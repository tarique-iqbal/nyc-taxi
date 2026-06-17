from __future__ import annotations

from unittest.mock import patch

import pytest

from etl.runtime.retry import RetryConfig, retry

# Succeeds immediately

def test_succeeds_on_first_attempt():
    calls = []

    @retry(max_attempts=3, delay=0, exceptions=(ValueError,))
    def fn():
        calls.append(1)
        return "ok"

    result = fn()
    assert result == "ok"
    assert len(calls) == 1


# Retries then succeeds

def test_retries_and_succeeds_on_third_attempt():
    counter = {"n": 0}

    @retry(max_attempts=3, delay=0, exceptions=(ValueError,))
    def fn():
        counter["n"] += 1
        if counter["n"] < 3:
            raise ValueError("not yet")
        return "success"

    with patch("time.sleep"):
        result = fn()

    assert result == "success"
    assert counter["n"] == 3


# Exhausts attempts and re-raises

def test_reraises_after_max_attempts():
    @retry(max_attempts=3, delay=0, exceptions=(RuntimeError,))
    def fn():
        raise RuntimeError("always fails")

    with patch("time.sleep"), pytest.raises(RuntimeError, match="always fails"):
        fn()


def test_attempt_count_matches_max_attempts():
    counter = {"n": 0}

    @retry(max_attempts=4, delay=0, exceptions=(OSError,))
    def fn():
        counter["n"] += 1
        raise OSError("fail")

    with patch("time.sleep"), pytest.raises(OSError):
        fn()

    assert counter["n"] == 4


# reraise=False

def test_reraise_false_returns_none_after_exhaustion():
    @retry(max_attempts=2, delay=0, exceptions=(ValueError,), reraise=False)
    def fn():
        raise ValueError("fail")

    with patch("time.sleep"):
        result = fn()

    assert result is None


# Only catches specified exceptions

def test_uncaught_exception_propagates_immediately():
    counter = {"n": 0}

    @retry(max_attempts=5, delay=0, exceptions=(ValueError,))
    def fn():
        counter["n"] += 1
        raise TypeError("wrong type -- should not retry")

    with pytest.raises(TypeError, match="wrong type"):
        fn()

    assert counter["n"] == 1  # no retry occurred


# Exponential backoff

def test_exponential_backoff_delays():
    @retry(max_attempts=3, delay=1.0, backoff=2.0, exceptions=(ValueError,))
    def fn():
        raise ValueError("fail")

    with patch("time.sleep") as mock_sleep, pytest.raises(ValueError):
        fn()

    sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
    assert sleep_calls == [1.0, 2.0]


def test_no_sleep_on_last_attempt():
    @retry(max_attempts=2, delay=1.0, exceptions=(ValueError,))
    def fn():
        raise ValueError("fail")

    with patch("time.sleep") as mock_sleep, pytest.raises(ValueError):
        fn()

    # Two attempts means one retry sleep before attempt 2, none after.
    assert mock_sleep.call_count == 1


# Return value preserved

def test_return_value_passed_through():
    @retry(max_attempts=3, delay=0, exceptions=(ValueError,))
    def fn():
        return {"rows": 42}

    assert fn() == {"rows": 42}


# RetryConfig constants

def test_retry_config_kafka_publish_has_expected_keys():
    assert "max_attempts" in RetryConfig.KAFKA_PUBLISH
    assert "delay" in RetryConfig.KAFKA_PUBLISH
    assert "backoff" in RetryConfig.KAFKA_PUBLISH


def test_retry_config_clickhouse_insert_has_expected_keys():
    assert "max_attempts" in RetryConfig.CLICKHOUSE_INSERT
    assert RetryConfig.CLICKHOUSE_INSERT["max_attempts"] >= 1


# Wraps preserves function metadata

def test_retry_preserves_function_name():
    @retry(max_attempts=2, delay=0, exceptions=(ValueError,))
    def my_function():
        pass

    assert my_function.__name__ == "my_function"


def test_retry_preserves_docstring():
    @retry(max_attempts=2, delay=0, exceptions=(ValueError,))
    def my_function():
        """My docstring."""

    assert my_function.__doc__ == "My docstring."
