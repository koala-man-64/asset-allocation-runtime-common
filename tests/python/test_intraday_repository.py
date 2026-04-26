from __future__ import annotations

import json

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.intraday_repository import IntradayRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def _watchlist_payload() -> dict[str, object]:
    return {
        "watchlistId": "watch-1",
        "name": "Tech Core",
        "enabled": True,
        "symbolCount": 2,
        "symbols": ["AAPL", "MSFT"],
    }


def _run_payload() -> dict[str, object]:
    return {
        "runId": "run-1",
        "watchlistId": "watch-1",
        "triggerKind": "manual",
        "status": "queued",
        "symbolCount": 2,
    }


def test_intraday_worker_lifecycle_uses_contract_payloads() -> None:
    calls: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.content.decode("utf-8") or None))
        if request.url.path.endswith("/claim"):
            return httpx.Response(
                200,
                json={
                    "run": _run_payload(),
                    "watchlist": _watchlist_payload(),
                    "currentSymbolStatuses": [],
                    "claimToken": "claim-1",
                },
            )
        return httpx.Response(200, json=_run_payload())

    transport = _build_transport(handler)
    try:
        repo = IntradayRepository(transport=transport)
        claim = repo.claim_monitor_run(execution_name="intraday-monitor-job-1")
        completed = repo.complete_monitor_run(
            "run-1",
            claim_token="claim-1",
            refresh_symbols=["msft", "MSFT"],
        )
        failed = repo.fail_monitor_run("run-1", claim_token="claim-1", error="snapshot failed")
    finally:
        transport.close()

    assert claim.claimToken == "claim-1"
    assert completed.runId == "run-1"
    assert failed.runId == "run-1"
    assert calls == [
        ("POST", "/api/internal/intraday-monitor/claim", '{"executionName":"intraday-monitor-job-1"}'),
        (
            "POST",
            "/api/internal/intraday-monitor/runs/run-1/complete",
            '{"claimToken":"claim-1","symbolStatuses":[],"events":[],"refreshSymbols":["MSFT"]}',
        ),
        (
            "POST",
            "/api/internal/intraday-monitor/runs/run-1/fail",
            '{"claimToken":"claim-1","error":"snapshot failed"}',
        ),
    ]


def test_intraday_refresh_lifecycle_uses_internal_routes() -> None:
    calls: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.content.decode("utf-8") or None))
        if request.url.path.endswith("/claim"):
            return httpx.Response(
                200,
                json={
                    "batch": {
                        "batchId": "batch-1",
                        "runId": "run-1",
                        "watchlistId": "watch-1",
                        "domain": "market",
                        "bucketLetter": "A",
                        "status": "claimed",
                        "symbols": ["AAPL"],
                        "symbolCount": 1,
                    },
                    "claimToken": "claim-1",
                },
            )
        return httpx.Response(
            200,
            json={
                "batchId": "batch-1",
                "runId": "run-1",
                "watchlistId": "watch-1",
                "domain": "market",
                "bucketLetter": "A",
                "status": "completed",
                "symbols": ["AAPL"],
                "symbolCount": 1,
            },
        )

    transport = _build_transport(handler)
    try:
        repo = IntradayRepository(transport=transport)
        claim = repo.claim_refresh_batch(execution_name="intraday-refresh-job-1")
        completed = repo.complete_refresh_batch("batch-1", claim_token="claim-1")
        failed = repo.fail_refresh_batch("batch-1", claim_token="claim-1", error="refresh failed")
    finally:
        transport.close()

    assert claim.claimToken == "claim-1"
    assert completed.batchId == "batch-1"
    assert failed.batchId == "batch-1"
    assert calls == [
        ("POST", "/api/internal/intraday-refresh/claim", '{"executionName":"intraday-refresh-job-1"}'),
        ("POST", "/api/internal/intraday-refresh/batches/batch-1/complete", '{"claimToken":"claim-1"}'),
        (
            "POST",
            "/api/internal/intraday-refresh/batches/batch-1/fail",
            '{"claimToken":"claim-1","error":"refresh failed"}',
        ),
    ]


def test_intraday_watchlist_append_uses_new_3_7_contract_surface() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/intraday/watchlists/watch-1/symbols"
        assert json.loads(request.content.decode("utf-8")) == {
            "symbols": ["AAPL", "MSFT"],
            "queueRun": False,
            "reason": "operator add",
        }
        return httpx.Response(
            200,
            json={
                "watchlist": {
                    **_watchlist_payload(),
                    "symbolCount": 3,
                    "symbols": ["AAPL", "MSFT", "NVDA"],
                },
                "addedSymbols": ["msft"],
                "alreadyPresentSymbols": ["aapl"],
                "queuedRun": None,
                "runSkippedReason": "queue_run_disabled",
            },
        )

    transport = _build_transport(handler)
    try:
        repo = IntradayRepository(transport=transport)
        result = repo.append_watchlist_symbols(
            "watch-1",
            symbols=[" aapl ", "MSFT", "AAPL"],
            queue_run=False,
            reason="  operator add  ",
        )
    finally:
        transport.close()

    assert result.addedSymbols == ["MSFT"]
    assert result.alreadyPresentSymbols == ["AAPL"]
    assert result.runSkippedReason == "queue_run_disabled"
