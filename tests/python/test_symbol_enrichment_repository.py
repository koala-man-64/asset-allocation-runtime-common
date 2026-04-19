from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.symbol_enrichment_repository import SymbolEnrichmentRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_claim_work_unwraps_typed_work_item() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/internal/symbol-cleanup/claim"
        assert request.content.decode("utf-8") == '{"executionName":"symbol-cleanup-job"}'
        return httpx.Response(
            200,
            json={
                "work": {
                    "workId": "work-1",
                    "runId": "run-1",
                    "symbol": "AAPL",
                    "status": "queued",
                    "requestedFields": ["sector_norm", "industry_norm"],
                    "attemptCount": 0,
                }
            },
        )

    transport = _build_transport(handler)
    try:
        repo = SymbolEnrichmentRepository(transport=transport)
        result = repo.claim_work(execution_name="symbol-cleanup-job")
    finally:
        transport.close()

    assert result is not None
    assert result.symbol == "AAPL"
    assert result.requestedFields == ["sector_norm", "industry_norm"]


def test_complete_fail_and_resolve_call_expected_paths() -> None:
    calls: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = request.content.decode("utf-8") or None
        calls.append((request.method, request.url.path, payload))
        if request.url.path == "/api/internal/symbol-enrichment/resolve":
            return httpx.Response(
                200,
                json={
                    "symbol": "AAPL",
                    "profile": {
                        "sector_norm": "Technology",
                        "industry_norm": "Technology Hardware, Storage & Peripherals",
                    },
                    "model": "gpt-5.4",
                    "confidence": 0.91,
                    "warnings": [],
                },
            )
        return httpx.Response(204, content=b"")

    transport = _build_transport(handler)
    try:
        repo = SymbolEnrichmentRepository(transport=transport)
        repo.complete_work(
            "work-1",
            result={
                "symbol": "AAPL",
                "profile": {"sector_norm": "Technology"},
                "warnings": [],
            },
        )
        repo.fail_work("work-1", error="boom")
        resolved = repo.resolve_symbol_profile(
            {
                "symbol": "AAPL",
                "overwriteMode": "fill_missing",
                "requestedFields": ["sector_norm", "industry_norm"],
                "providerFacts": {"symbol": "AAPL"},
            }
        )
    finally:
        transport.close()

    assert calls == [
        (
            "POST",
            "/api/internal/symbol-cleanup/work-1/complete",
            '{"result":{"symbol":"AAPL","profile":{"sector_norm":"Technology"},"warnings":[]}}',
        ),
        ("POST", "/api/internal/symbol-cleanup/work-1/fail", '{"error":"boom"}'),
        (
            "POST",
            "/api/internal/symbol-enrichment/resolve",
            '{"symbol":"AAPL","overwriteMode":"fill_missing","requestedFields":["sector_norm","industry_norm"],"providerFacts":{"symbol":"AAPL"}}',
        ),
    ]
    assert resolved.confidence == 0.91
    assert resolved.profile.sector_norm == "Technology"


def test_get_run_returns_typed_summary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/internal/symbol-cleanup/runs/run-1"
        return httpx.Response(
            200,
            json={
                "runId": "run-1",
                "status": "running",
                "mode": "full_reconcile",
                "queuedCount": 10,
                "claimedCount": 1,
                "completedCount": 3,
                "failedCount": 0,
                "acceptedUpdateCount": 2,
                "rejectedUpdateCount": 1,
                "lockedSkipCount": 0,
                "overwriteCount": 2,
            },
        )

    transport = _build_transport(handler)
    try:
        repo = SymbolEnrichmentRepository(transport=transport)
        result = repo.get_run("run-1")
    finally:
        transport.close()

    assert result is not None
    assert result.status == "running"
    assert result.mode == "full_reconcile"
