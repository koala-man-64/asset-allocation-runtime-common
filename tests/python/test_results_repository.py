from __future__ import annotations

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
        return httpx.Response(200, json={"dryRun": True, "rankingDirtyCount": 2})

    transport = _build_transport(handler)
    try:
        repo = ResultsRepository(transport=transport)
        result = repo.reconcile(dry_run=True)
    finally:
        transport.close()

    assert result == {"dryRun": True, "rankingDirtyCount": 2}
