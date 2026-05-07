from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeoutCircuitBreakerConfig:
    failure_threshold: int = 3
    open_seconds: float = 300.0

    def normalized(self) -> "TimeoutCircuitBreakerConfig":
        threshold = int(self.failure_threshold)
        if threshold < 1:
            raise ValueError("Timeout circuit breaker failure threshold must be >= 1.")
        return TimeoutCircuitBreakerConfig(
            failure_threshold=threshold,
            open_seconds=max(0.0, float(self.open_seconds)),
        )

    @property
    def enabled(self) -> bool:
        return self.normalized().open_seconds > 0.0


@dataclass(frozen=True)
class TimeoutCircuitOpenState:
    provider: str
    scope_key: str
    retry_after_seconds: float
    failure_threshold: int
    open_seconds: float
    timeout_count: int
    last_reason: Optional[str]
    half_open_probe_in_progress: bool = False


class TimeoutCircuitBreaker:
    def __init__(
        self,
        *,
        provider: str,
        scope_key: str,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.provider = str(provider)
        self.scope_key = str(scope_key)
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._timeout_count = 0
        self._open_until_monotonic = 0.0
        self._last_reason: Optional[str] = None
        self._half_open_probe_active = False

    def before_call(self, config: TimeoutCircuitBreakerConfig) -> Optional[TimeoutCircuitOpenState]:
        resolved = config.normalized()
        if not resolved.enabled:
            self.close()
            return None

        now = self._monotonic()
        with self._lock:
            if self._open_until_monotonic <= 0.0:
                return None

            if now < self._open_until_monotonic:
                return self._open_state(resolved, self._open_until_monotonic - now)

            if not self._half_open_probe_active:
                self._half_open_probe_active = True
                return None

            return self._open_state(resolved, 0.0, half_open_probe_in_progress=True)

    def record_success(self) -> None:
        self.close()

    def record_non_timeout_response(self) -> None:
        self.close()

    def record_timeout(
        self,
        config: TimeoutCircuitBreakerConfig,
        *,
        reason: str,
        path: Optional[str] = None,
    ) -> Optional[TimeoutCircuitOpenState]:
        resolved = config.normalized()
        if not resolved.enabled:
            self.close()
            return None

        now = self._monotonic()
        with self._lock:
            self._last_reason = str(reason)
            if self._half_open_probe_active or (
                self._open_until_monotonic > 0.0 and now >= self._open_until_monotonic
            ):
                self._timeout_count = resolved.failure_threshold
            else:
                self._timeout_count += 1

            self._half_open_probe_active = False
            if self._timeout_count < resolved.failure_threshold:
                return None

            self._open_until_monotonic = now + resolved.open_seconds
            state = self._open_state(resolved, resolved.open_seconds)

        logger.warning(
            "Timeout circuit opened (provider=%s, scope=%s, path=%s, reason=%s, failures=%s, open_seconds=%.1f).",
            self.provider,
            self.scope_key,
            path or "n/a",
            reason,
            state.timeout_count,
            state.open_seconds,
        )
        return state

    def close(self) -> None:
        with self._lock:
            self._timeout_count = 0
            self._open_until_monotonic = 0.0
            self._last_reason = None
            self._half_open_probe_active = False

    def _open_state(
        self,
        config: TimeoutCircuitBreakerConfig,
        retry_after_seconds: float,
        *,
        half_open_probe_in_progress: bool = False,
    ) -> TimeoutCircuitOpenState:
        return TimeoutCircuitOpenState(
            provider=self.provider,
            scope_key=self.scope_key,
            retry_after_seconds=max(0.0, float(retry_after_seconds)),
            failure_threshold=int(config.failure_threshold),
            open_seconds=float(config.open_seconds),
            timeout_count=int(self._timeout_count),
            last_reason=self._last_reason,
            half_open_probe_in_progress=half_open_probe_in_progress,
        )


_REGISTRY_LOCK = threading.Lock()
_REGISTRY: dict[str, TimeoutCircuitBreaker] = {}


def get_timeout_circuit(
    *,
    provider: str,
    scope_key: str,
) -> TimeoutCircuitBreaker:
    key = f"{provider}:{scope_key}"
    with _REGISTRY_LOCK:
        circuit = _REGISTRY.get(key)
        if circuit is None:
            circuit = TimeoutCircuitBreaker(provider=provider, scope_key=scope_key)
            _REGISTRY[key] = circuit
        return circuit


def reset_timeout_circuit_registry() -> None:
    with _REGISTRY_LOCK:
        _REGISTRY.clear()
