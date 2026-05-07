from __future__ import annotations

import threading

import pytest

from asset_allocation_runtime_common.shared_core.timeout_circuit_breaker import (
    TimeoutCircuitBreaker,
    TimeoutCircuitBreakerConfig,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += float(seconds)


def test_timeout_circuit_opens_half_opens_and_closes_on_success() -> None:
    clock = FakeClock()
    circuit = TimeoutCircuitBreaker(provider="test", scope_key="provider-a", monotonic=clock)
    config = TimeoutCircuitBreakerConfig(failure_threshold=2, open_seconds=10.0)

    assert circuit.before_call(config) is None
    assert circuit.record_timeout(config, reason="timeout", path="/first") is None
    opened = circuit.record_timeout(config, reason="timeout", path="/second")

    assert opened is not None
    assert opened.retry_after_seconds == 10.0
    assert opened.timeout_count == 2

    blocked = circuit.before_call(config)
    assert blocked is not None
    assert blocked.retry_after_seconds == 10.0

    clock.advance(10.0)
    assert circuit.before_call(config) is None
    blocked_probe = circuit.before_call(config)
    assert blocked_probe is not None
    assert blocked_probe.half_open_probe_in_progress is True

    circuit.record_success()
    assert circuit.before_call(config) is None


def test_half_open_timeout_reopens_full_cooldown() -> None:
    clock = FakeClock()
    circuit = TimeoutCircuitBreaker(provider="test", scope_key="provider-a", monotonic=clock)
    config = TimeoutCircuitBreakerConfig(failure_threshold=1, open_seconds=15.0)

    circuit.record_timeout(config, reason="timeout", path="/first")
    clock.advance(15.0)

    assert circuit.before_call(config) is None
    reopened = circuit.record_timeout(config, reason="timeout", path="/probe")

    assert reopened is not None
    assert reopened.retry_after_seconds == 15.0
    assert reopened.timeout_count == 1


def test_only_one_half_open_probe_is_permitted_concurrently() -> None:
    clock = FakeClock()
    circuit = TimeoutCircuitBreaker(provider="test", scope_key="provider-a", monotonic=clock)
    config = TimeoutCircuitBreakerConfig(failure_threshold=1, open_seconds=5.0)

    circuit.record_timeout(config, reason="timeout", path="/first")
    clock.advance(5.0)

    barrier = threading.Barrier(5)
    outcomes: list[bool] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait(timeout=5.0)
        allowed = circuit.before_call(config) is None
        with lock:
            outcomes.append(allowed)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5.0)

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 4


def test_open_seconds_zero_disables_timeout_circuit() -> None:
    circuit = TimeoutCircuitBreaker(provider="test", scope_key="provider-a")
    config = TimeoutCircuitBreakerConfig(failure_threshold=1, open_seconds=0.0)

    assert circuit.record_timeout(config, reason="timeout", path="/first") is None
    assert circuit.before_call(config) is None


def test_failure_threshold_must_be_positive() -> None:
    circuit = TimeoutCircuitBreaker(provider="test", scope_key="provider-a")
    config = TimeoutCircuitBreakerConfig(failure_threshold=0, open_seconds=10.0)

    with pytest.raises(ValueError, match="failure threshold must be >= 1"):
        circuit.before_call(config)
