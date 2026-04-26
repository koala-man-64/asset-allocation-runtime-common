from __future__ import annotations

import httpx
import pytest

import asset_allocation_runtime_common.shared_core.massive_gateway_client as massive_module
from asset_allocation_runtime_common.shared_core.massive_gateway_client import (
    MassiveGatewayAuthError,
    MassiveGatewayClient,
    MassiveGatewayClientConfig,
    MassiveGatewayUnavailableError,
)


def _build_client(*, transport: httpx.BaseTransport, retry_attempts: int = 1) -> MassiveGatewayClient:
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    return MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
            request_retry_attempts=retry_attempts,
            request_retry_base_delay_seconds=1.0,
            request_retry_max_delay_seconds=10.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )


def test_massive_retries_transient_status_before_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"detail": "warming"})
        return httpx.Response(200, json={"results": [{"ticker": "AAPL"}]})

    monkeypatch.setattr(massive_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(massive_module.random, "uniform", lambda _start, _end: 0.0)

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=2)
    try:
        payload = client.get_tickers()
    finally:
        client.close()

    assert payload == [{"ticker": "AAPL"}]
    assert calls == 2


def test_massive_auth_failure_is_not_retried() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, json={"detail": "unauthorized"})

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=3)
    try:
        with pytest.raises(MassiveGatewayAuthError):
            client.get_tickers()
    finally:
        client.close()

    assert calls == 1


def test_massive_unavailable_detail_and_payload_are_redacted() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "gateway unavailable apiKey=provider-secret"})

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=1)
    try:
        with pytest.raises(MassiveGatewayUnavailableError) as exc_info:
            client.get_tickers()
    finally:
        client.close()

    assert "provider-secret" not in str(exc_info.value)
    assert "provider-secret" not in str(exc_info.value.detail)
    assert "provider-secret" not in str(exc_info.value.payload)
