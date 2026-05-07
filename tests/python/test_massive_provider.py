from __future__ import annotations

from typing import Any, Optional

import pytest
import requests

from asset_allocation_runtime_common.shared_core.massive_provider import (
    MassiveProvider,
    MassiveProviderCircuitOpenError,
    MassiveProviderConfig,
    MassiveProviderError,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: Optional[dict[str, Any]] = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {"results": []}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeSession:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def get(self, *_args: object, **_kwargs: object) -> FakeResponse:
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome  # type: ignore[return-value]

    def close(self) -> None:
        pass


def _build_provider(session: FakeSession) -> MassiveProvider:
    return MassiveProvider(
        MassiveProviderConfig(
            api_key="provider-secret",
            base_url="https://api.massive.test",
            timeout_seconds=1.0,
            circuit_breaker_failure_threshold=1,
            circuit_breaker_open_seconds=60.0,
        ),
        session=session,  # type: ignore[arg-type]
    )


def test_massive_provider_timeout_circuit_fails_fast_after_timeout() -> None:
    session = FakeSession([requests.Timeout("timeout apiKey=provider-secret")])
    provider = _build_provider(session)

    with pytest.raises(MassiveProviderError):
        provider.list_tickers()
    with pytest.raises(MassiveProviderCircuitOpenError) as exc_info:
        provider.list_tickers()

    assert session.calls == 1
    assert exc_info.value.retry_after_seconds > 0
    assert "provider-secret" not in str(exc_info.value)


def test_massive_provider_504_opens_timeout_circuit() -> None:
    session = FakeSession([FakeResponse(504)])
    provider = _build_provider(session)

    with pytest.raises(MassiveProviderError):
        provider.list_tickers()
    with pytest.raises(MassiveProviderCircuitOpenError):
        provider.list_tickers()

    assert session.calls == 1


def test_massive_provider_ordinary_5xx_does_not_open_timeout_circuit() -> None:
    session = FakeSession([FakeResponse(503), FakeResponse(503)])
    provider = _build_provider(session)

    with pytest.raises(MassiveProviderError):
        provider.list_tickers()
    with pytest.raises(MassiveProviderError):
        provider.list_tickers()

    assert session.calls == 2
