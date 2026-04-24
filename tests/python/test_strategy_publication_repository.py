from __future__ import annotations

import json

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.strategy_publication_repository import StrategyPublicationRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_strategy_publication_repository_posts_reconcile_signal() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/internal/strategy-publications/reconcile-signal"
        assert request.headers["authorization"] == "Bearer test-token"
        assert json.loads(request.content.decode("utf-8")) == {
            "jobKey": "regime",
            "sourceFingerprint": "abc123",
            "publishedAt": None,
            "metadata": {
                "publishedAsOfDate": "2026-04-23",
                "inputAsOfDate": None,
                "historyRows": 10,
                "latestRows": 1,
                "transitionRows": 0,
                "activeModels": [],
                "domainArtifactPath": None,
                "producerJobName": "gold-regime-job",
            },
        }
        return httpx.Response(
            200,
            json={
                "jobKey": "regime",
                "sourceFingerprint": "abc123",
                "status": "pending",
                "created": True,
                "createdAt": "2026-04-23T21:00:00Z",
                "updatedAt": "2026-04-23T21:00:00Z",
                "processedAt": None,
                "error": None,
            },
        )

    transport = _build_transport(handler)
    try:
        repo = StrategyPublicationRepository(transport=transport)
        result = repo.record_reconcile_signal(
            job_key="regime",
            source_fingerprint="abc123",
            metadata={
                "publishedAsOfDate": "2026-04-23",
                "historyRows": 10,
                "latestRows": 1,
                "transitionRows": 0,
            },
        )
    finally:
        transport.close()

    assert result.jobKey == "regime"
    assert result.sourceFingerprint == "abc123"
    assert result.created is True
