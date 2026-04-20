from __future__ import annotations

import httpx
import pytest

import asset_allocation_runtime_common.shared_core.quiver_gateway_client as quiver_gateway_client_module
from asset_allocation_runtime_common.shared_core.quiver_gateway_client import (
    QuiverGatewayClient,
    QuiverGatewayClientConfig,
    QuiverGatewayUnavailableError,
)


def _build_client(*, transport: httpx.BaseTransport | None = None) -> QuiverGatewayClient:
    http_client = None
    if transport is not None:
        http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    return QuiverGatewayClient(
        QuiverGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )


_METHOD_CASES = [
    pytest.param(
        "get_live_congress_trading",
        {"normalized": True, "representative": "Pelosi"},
        "/api/providers/quiver/live/congress-trading",
        {"normalized": "true", "representative": "Pelosi"},
        id="live-congress-trading",
    ),
    pytest.param(
        "get_historical_congress_trading",
        {"ticker": "AAPL", "analyst": "Jane Doe"},
        "/api/providers/quiver/historical/congress-trading/AAPL",
        {"analyst": "Jane Doe"},
        id="historical-congress-trading",
    ),
    pytest.param(
        "get_live_senate_trading",
        {"name": "Smith", "options": False},
        "/api/providers/quiver/live/senate-trading",
        {"name": "Smith", "options": "false"},
        id="live-senate-trading",
    ),
    pytest.param(
        "get_historical_senate_trading",
        {"ticker": "MSFT"},
        "/api/providers/quiver/historical/senate-trading/MSFT",
        {},
        id="historical-senate-trading",
    ),
    pytest.param(
        "get_live_house_trading",
        {"name": "Pelosi", "options": True},
        "/api/providers/quiver/live/house-trading",
        {"name": "Pelosi", "options": "true"},
        id="live-house-trading",
    ),
    pytest.param(
        "get_historical_house_trading",
        {"ticker": "NVDA"},
        "/api/providers/quiver/historical/house-trading/NVDA",
        {},
        id="historical-house-trading",
    ),
    pytest.param(
        "get_live_gov_contracts",
        {},
        "/api/providers/quiver/live/gov-contracts",
        {},
        id="live-gov-contracts",
    ),
    pytest.param(
        "get_historical_gov_contracts",
        {"ticker": "PLTR"},
        "/api/providers/quiver/historical/gov-contracts/PLTR",
        {},
        id="historical-gov-contracts",
    ),
    pytest.param(
        "get_live_gov_contracts_all",
        {"date": "2026-03-31", "page": 2, "page_size": 50},
        "/api/providers/quiver/live/gov-contracts-all",
        {"date": "2026-03-31", "page": "2", "page_size": "50"},
        id="live-gov-contracts-all",
    ),
    pytest.param(
        "get_historical_gov_contracts_all",
        {"ticker": "LMT"},
        "/api/providers/quiver/historical/gov-contracts-all/LMT",
        {},
        id="historical-gov-contracts-all",
    ),
    pytest.param(
        "get_live_insiders",
        {
            "ticker": "AAPL",
            "date": "2026-03-31",
            "uploaded": "2026-04-01",
            "limit_codes": True,
            "page": 3,
            "page_size": 25,
        },
        "/api/providers/quiver/live/insiders",
        {
            "ticker": "AAPL",
            "date": "2026-03-31",
            "uploaded": "2026-04-01",
            "limit_codes": "true",
            "page": "3",
            "page_size": "25",
        },
        id="live-insiders",
    ),
    pytest.param(
        "get_live_sec13f",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": False,
            "page": 2,
            "page_size": 100,
        },
        "/api/providers/quiver/live/sec13f",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": "false",
            "page": "2",
            "page_size": "100",
        },
        id="live-sec13f",
    ),
    pytest.param(
        "get_live_sec13f_changes",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": False,
            "most_recent": True,
            "show_new_funds": False,
            "mobile": True,
            "page": 2,
            "page_size": 100,
        },
        "/api/providers/quiver/live/sec13f-changes",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": "false",
            "most_recent": "true",
            "show_new_funds": "false",
            "mobile": "true",
            "page": "2",
            "page_size": "100",
        },
        id="live-sec13f-changes",
    ),
    pytest.param(
        "get_live_lobbying",
        {"all_records": True, "date_from": "2026-01-01", "date_to": "2026-03-31", "page": 2, "page_size": 30},
        "/api/providers/quiver/live/lobbying",
        {
            "all": "true",
            "date_from": "2026-01-01",
            "date_to": "2026-03-31",
            "page": "2",
            "page_size": "30",
        },
        id="live-lobbying",
    ),
    pytest.param(
        "get_historical_lobbying",
        {"ticker": "AAPL", "page": 2, "page_size": 25, "query": "chips", "query_ticker": "MSFT"},
        "/api/providers/quiver/historical/lobbying/AAPL",
        {"page": "2", "page_size": "25", "query": "chips", "queryTicker": "MSFT"},
        id="historical-lobbying",
    ),
    pytest.param(
        "get_live_etf_holdings",
        {"etf": "SPY", "ticker": "NVDA"},
        "/api/providers/quiver/live/etf-holdings",
        {"etf": "SPY", "ticker": "NVDA"},
        id="live-etf-holdings",
    ),
    pytest.param(
        "get_live_congress_holdings",
        {},
        "/api/providers/quiver/live/congress-holdings",
        {},
        id="live-congress-holdings",
    ),
]

_INVALID_ENV_CASES = [
    pytest.param("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", "not-a-number", "ASSET_ALLOCATION_API_TIMEOUT_SECONDS must be a number.", id="timeout"),
    pytest.param("QUIVER_TIMEOUT_SECONDS", "not-a-number", "QUIVER_TIMEOUT_SECONDS must be a number.", id="fallback-timeout"),
    pytest.param("ASSET_ALLOCATION_API_WARMUP_ATTEMPTS", "bad", "ASSET_ALLOCATION_API_WARMUP_ATTEMPTS must be an integer.", id="warmup-attempts"),
    pytest.param("ASSET_ALLOCATION_API_WARMUP_BASE_SECONDS", "bad", "ASSET_ALLOCATION_API_WARMUP_BASE_SECONDS must be a number.", id="warmup-base"),
    pytest.param("ASSET_ALLOCATION_API_WARMUP_MAX_SECONDS", "bad", "ASSET_ALLOCATION_API_WARMUP_MAX_SECONDS must be a number.", id="warmup-max"),
    pytest.param(
        "ASSET_ALLOCATION_API_WARMUP_PROBE_TIMEOUT_SECONDS",
        "bad",
        "ASSET_ALLOCATION_API_WARMUP_PROBE_TIMEOUT_SECONDS must be a number.",
        id="warmup-probe-timeout",
    ),
    pytest.param("ASSET_ALLOCATION_API_READINESS_ATTEMPTS", "bad", "ASSET_ALLOCATION_API_READINESS_ATTEMPTS must be an integer.", id="readiness-attempts"),
    pytest.param(
        "ASSET_ALLOCATION_API_READINESS_SLEEP_SECONDS",
        "bad",
        "ASSET_ALLOCATION_API_READINESS_SLEEP_SECONDS must be a number.",
        id="readiness-sleep",
    ),
]


def test_build_headers_include_bearer_token_and_caller_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-quiver-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-quiver-job-123")

    client = _build_client()

    headers = client._build_headers()
    assert headers["Authorization"] == "Bearer oidc-token"
    assert headers["X-Caller-Job"] == "bronze-quiver-job"
    assert headers["X-Caller-Execution"] == "bronze-quiver-job-123"


@pytest.mark.parametrize(("method_name", "kwargs", "expected_path", "expected_params"), _METHOD_CASES)
def test_gateway_client_methods_match_control_plane_routes(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    kwargs: dict[str, object],
    expected_path: str,
    expected_params: dict[str, str],
) -> None:
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-quiver-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-quiver-job-123")
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["path"] = request.url.path
        observed["params"] = dict(request.url.params)
        observed["headers"] = dict(request.headers)
        return httpx.Response(200, json=[{"ok": True}])

    client = _build_client(transport=httpx.MockTransport(handler))
    try:
        payload = getattr(client, method_name)(**kwargs)
    finally:
        client.close()

    assert payload == [{"ok": True}]
    assert observed["path"] == expected_path
    assert observed["params"] == expected_params
    headers = observed["headers"]
    assert headers["authorization"] == "Bearer oidc-token"
    assert headers["x-caller-job"] == "bronze-quiver-job"
    assert headers["x-caller-execution"] == "bronze-quiver-job-123"


@pytest.mark.parametrize(("env_name", "env_value", "expected_message"), _INVALID_ENV_CASES)
def test_from_env_rejects_invalid_numeric_values(
    monkeypatch: pytest.MonkeyPatch,
    env_name: str,
    env_value: str,
    expected_message: str,
) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation/.default")
    monkeypatch.setenv(env_name, env_value)

    with pytest.raises(ValueError, match=expected_message):
        QuiverGatewayClient.from_env()


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
