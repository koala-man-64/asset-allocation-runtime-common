from __future__ import annotations

import httpx
import pytest

import asset_allocation_runtime_common.shared_core.quiver_gateway_client as quiver_gateway_client_module
from asset_allocation_runtime_common.shared_core.quiver_gateway_client import (
    QuiverGatewayClient,
    QuiverGatewayClientConfig,
    QuiverGatewayUnavailableError,
)


def test_build_headers_include_bearer_token_and_caller_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-quiver-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-quiver-job-123")

    client = QuiverGatewayClient(
        QuiverGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
        ),
        access_token_provider=lambda: "oidc-token",
    )

    headers = client._build_headers()
    assert headers["Authorization"] == "Bearer oidc-token"
    assert headers["X-Caller-Job"] == "bronze-quiver-job"
    assert headers["X-Caller-Execution"] == "bronze-quiver-job-123"


def test_from_env_enforces_timeout_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation/.default")
    monkeypatch.setenv("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", "5")

    client = QuiverGatewayClient.from_env()
    try:
        assert client.config.timeout_seconds >= 60.0
    finally:
        client.close()


def test_warmup_probe_retries_before_first_request(monkeypatch: pytest.MonkeyPatch) -> None:
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            if counters["warmup"] < 3:
                return httpx.Response(503, text="warming")
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/quiver/live/congress-holdings":
            counters["data"] += 1
            return httpx.Response(200, json=[{"Politician": "Test User"}])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(quiver_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = QuiverGatewayClient(
        QuiverGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=True,
            warmup_max_attempts=3,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        payload = client.get_live_congress_holdings()
    finally:
        http_client.close()

    assert payload == [{"Politician": "Test User"}]
    assert counters["warmup"] == 3
    assert counters["data"] == 1


def test_request_fails_fast_when_readiness_never_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(503, text="warming")
        if request.url.path == "/api/providers/quiver/live/congress-holdings":
            counters["data"] += 1
            return httpx.Response(200, json=[{"Politician": "Test User"}])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(quiver_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = QuiverGatewayClient(
        QuiverGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=True,
            warmup_max_attempts=1,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
            readiness_enabled=True,
            readiness_max_attempts=2,
            readiness_sleep_seconds=0.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        with pytest.raises(QuiverGatewayUnavailableError):
            client.get_live_congress_holdings()
    finally:
        http_client.close()

    assert counters["warmup"] == 2
    assert counters["data"] == 0
