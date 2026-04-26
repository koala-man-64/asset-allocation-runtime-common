from __future__ import annotations

import traceback

import httpx
import pytest

import asset_allocation_runtime_common.shared_core.alpha_vantage_gateway_client as alpha_module
from asset_allocation_runtime_common.shared_core.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayClientConfig,
    AlphaVantageGatewayThrottleError,
    AlphaVantageGatewayUnavailableError,
)


def _build_client(
    *,
    transport: httpx.BaseTransport,
    retry_attempts: int = 1,
    circuit_failure_threshold: int = 3,
    circuit_open_seconds: float = 60.0,
) -> AlphaVantageGatewayClient:
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    return AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
            warmup_enabled=False,
            readiness_enabled=False,
            request_retry_attempts=retry_attempts,
            request_retry_base_delay_seconds=1.0,
            request_retry_max_delay_seconds=10.0,
            circuit_breaker_failure_threshold=circuit_failure_threshold,
            circuit_breaker_open_seconds=circuit_open_seconds,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )


def test_retries_transient_status_with_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                503,
                json={"detail": "Alpha Vantage upstream unavailable apiKey=provider-secret"},
                headers={"Retry-After": "2"},
            )
        return httpx.Response(200, text="symbol,name\nAAPL,Apple Inc.\n")

    monkeypatch.setattr(alpha_module.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(alpha_module.random, "uniform", lambda _start, _end: 0.0)

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=2)
    try:
        payload = client.get_listing_status_csv()
    finally:
        client.close()

    assert payload == "symbol,name\nAAPL,Apple Inc.\n"
    assert calls == 2
    assert sleeps == [2.0]


def test_429_is_throttle_without_retry() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={"detail": "provider throttle apiKey=provider-secret"})

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=3)
    try:
        with pytest.raises(AlphaVantageGatewayThrottleError) as exc_info:
            client.get_listing_status_csv()
    finally:
        client.close()

    assert calls == 1
    assert exc_info.value.status_code == 429
    assert "provider-secret" not in str(exc_info.value)
    assert "provider-secret" not in str(exc_info.value.payload)


def test_timeout_is_classified_unavailable() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout apiKey=provider-secret")

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=1)
    try:
        with pytest.raises(AlphaVantageGatewayUnavailableError) as exc_info:
            client.get_listing_status_csv()
    finally:
        client.close()

    assert exc_info.value.status_code == 504
    assert "provider-secret" not in str(exc_info.value)
    assert "provider-secret" not in str(exc_info.value.payload)
    assert "provider-secret" not in "".join(traceback.format_exception(exc_info.value))


def test_transient_error_detail_and_payload_are_redacted() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, json={"detail": "bad gateway apiKey=provider-secret"})

    client = _build_client(transport=httpx.MockTransport(handler), retry_attempts=1)
    try:
        with pytest.raises(AlphaVantageGatewayUnavailableError) as exc_info:
            client.get_listing_status_csv()
    finally:
        client.close()

    assert exc_info.value.status_code == 502
    assert "provider-secret" not in str(exc_info.value)
    assert "provider-secret" not in str(exc_info.value.detail)
    assert "provider-secret" not in str(exc_info.value.payload)


def test_circuit_breaker_fails_fast_after_unavailable_error() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"detail": "gateway warming"})

    client = _build_client(
        transport=httpx.MockTransport(handler),
        retry_attempts=1,
        circuit_failure_threshold=1,
        circuit_open_seconds=60.0,
    )
    try:
        with pytest.raises(AlphaVantageGatewayUnavailableError):
            client.get_listing_status_csv()
        with pytest.raises(AlphaVantageGatewayUnavailableError) as exc_info:
            client.get_listing_status_csv()
    finally:
        client.close()

    assert calls == 1
    assert "circuit breaker is open" in str(exc_info.value)
