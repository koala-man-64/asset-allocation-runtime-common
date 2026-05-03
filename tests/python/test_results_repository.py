from __future__ import annotations

import pytest
import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.results_repository import ResultsRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_results_reconcile_posts_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/internal/results/reconcile"
        assert request.content.decode("utf-8") == '{"dryRun":true}'
        return httpx.Response(
            200,
            json={
                "dryRun": True,
                "rankingDirtyCount": 2,
                "rankingNoopCount": 0,
                "canonicalEnqueuedCount": 1,
                "canonicalUpToDateCount": 3,
                "canonicalSkippedCount": 4,
                "publicationSignalsProcessedCount": 5,
                "publicationSignalsErrorCount": 0,
                "errorCount": 0,
                "errors": [],
            },
        )

    transport = _build_transport(handler)
    try:
        repo = ResultsRepository(transport=transport)
        result = repo.reconcile(dry_run=True)
    finally:
        transport.close()

    assert result.dryRun is True
    assert result.rankingDirtyCount == 2
    assert result.publicationSignalsProcessedCount == 5


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {"dryRun": True, "rankingDirtyCount": 2},
        {
            "dryRun": True,
            "rankingDirtyCount": "2",
            "rankingNoopCount": 0,
            "canonicalEnqueuedCount": 0,
            "canonicalUpToDateCount": 0,
            "canonicalSkippedCount": 0,
            "publicationSignalsProcessedCount": 0,
            "publicationSignalsErrorCount": 0,
            "errorCount": 0,
            "errors": [],
        },
    ],
)
def test_results_reconcile_rejects_malformed_success_payloads(payload: object) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = _build_transport(handler)
    try:
        repo = ResultsRepository(transport=transport)
        with pytest.raises(ValueError):
            repo.reconcile(dry_run=False)
    finally:
        transport.close()


def test_results_reconcile_preserves_typed_error_counters() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "dryRun": False,
                "rankingDirtyCount": 0,
                "rankingNoopCount": 0,
                "canonicalEnqueuedCount": 0,
                "canonicalUpToDateCount": 0,
                "canonicalSkippedCount": 0,
                "publicationSignalsProcessedCount": 1,
                "publicationSignalsErrorCount": 1,
                "errorCount": 1,
                "errors": ["publication:regime:boom"],
            },
        )

    transport = _build_transport(handler)
    try:
        repo = ResultsRepository(transport=transport)
        result = repo.reconcile(dry_run=False)
    finally:
        transport.close()

    assert result.errorCount == 1
    assert result.errors == ["publication:regime:boom"]
