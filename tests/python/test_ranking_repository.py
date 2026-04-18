from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.ranking_repository import RankingRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_list_ranking_schemas_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/rankings"
        return httpx.Response(200, json=[{"name": "quality-momentum", "version": 2}])

    transport = _build_transport(handler)
    try:
        repo = RankingRepository(transport=transport)
        result = repo.list_ranking_schemas()
    finally:
        transport.close()

    assert result == [{"name": "quality-momentum", "version": 2}]


def test_get_ranking_schema_revision_passes_version_query_param() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/rankings/quality-momentum/revision"
        assert request.url.params["version"] == "3"
        return httpx.Response(200, json={"name": "quality-momentum", "version": 3})

    transport = _build_transport(handler)
    try:
        repo = RankingRepository(transport=transport)
        result = repo.get_ranking_schema_revision("quality-momentum", version=3)
    finally:
        transport.close()

    assert result == {"name": "quality-momentum", "version": 3}


def test_ranking_mutations_are_blocked() -> None:
    transport = _build_transport(lambda request: httpx.Response(200, json={}))
    try:
        repo = RankingRepository(transport=transport)
        for method in (repo.save_ranking_schema, repo.delete_ranking_schema):
            try:
                method("quality-momentum")
            except NotImplementedError as exc:
                assert "does not mutate" in str(exc)
            else:
                raise AssertionError("Expected NotImplementedError for jobs-side mutation method.")
    finally:
        transport.close()


def test_ranking_refresh_lifecycle_calls_expected_internal_paths() -> None:
    calls: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.content.decode("utf-8") or None))
        if request.url.path.endswith("/claim"):
            return httpx.Response(200, json={"work": {"strategyName": "alpha", "claimToken": "claim-1"}})
        return httpx.Response(200, json={"status": "ok"})

    transport = _build_transport(handler)
    try:
        repo = RankingRepository(transport=transport)
        claim = repo.claim_next_refresh(execution_name="job-1")
        repo.complete_refresh(
            "alpha",
            claim_token="claim-1",
            run_id="run-1",
            dependency_fingerprint="fp-1",
            dependency_state={"domains": {}},
        )
        repo.fail_refresh("alpha", claim_token="claim-1", error="boom")
    finally:
        transport.close()

    assert claim == {"strategyName": "alpha", "claimToken": "claim-1"}
    assert calls == [
        ("POST", "/api/internal/rankings/refresh/claim", '{"executionName":"job-1"}'),
        (
            "POST",
            "/api/internal/rankings/refresh/alpha/complete",
            '{"claimToken":"claim-1","runId":"run-1","dependencyFingerprint":"fp-1","dependencyState":{"domains":{}}}',
        ),
        ("POST", "/api/internal/rankings/refresh/alpha/fail", '{"claimToken":"claim-1","error":"boom"}'),
    ]
