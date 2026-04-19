from __future__ import annotations

from typing import Any

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.regime_repository import RegimeRepository


class _FakeCursor:
    def __init__(self, *, fetchone_rows=None, fetchall_rows=None) -> None:
        self.fetchone_rows = list(fetchone_rows or [])
        self.fetchall_rows = list(fetchall_rows or [])
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))

    def fetchone(self):
        if not self.fetchone_rows:
            return None
        return self.fetchone_rows.pop(0)

    def fetchall(self):
        return list(self.fetchall_rows)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_get_active_regime_model_revision_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/regimes/models/default-regime/active"
        return httpx.Response(200, json={"name": "default-regime", "version": 2})

    transport = _build_transport(handler)
    try:
        repo = RegimeRepository(transport=transport)
        result = repo.get_active_regime_model_revision("default-regime")
    finally:
        transport.close()

    assert result == {"name": "default-regime", "version": 2}


def test_list_active_regime_model_revisions_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/regimes/models/active"
        return httpx.Response(200, json=[{"name": "default-regime", "version": 2}])

    transport = _build_transport(handler)
    try:
        repo = RegimeRepository(transport=transport)
        result = repo.list_active_regime_model_revisions()
    finally:
        transport.close()

    assert result == [{"name": "default-regime", "version": 2}]


def test_list_active_regime_model_revisions_retries_transient_control_plane_failures() -> None:
    request_count = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        request_count["count"] += 1
        if request_count["count"] < 3:
            return httpx.Response(502, json={"detail": "bad gateway"})
        return httpx.Response(200, json=[{"name": "default-regime", "version": 2}])

    transport = _build_transport(handler)
    try:
        repo = RegimeRepository(transport=transport)
        result = repo.list_active_regime_model_revisions()
    finally:
        transport.close()

    assert request_count["count"] == 3
    assert result == [{"name": "default-regime", "version": 2}]


def test_list_active_regime_model_revisions_falls_back_to_postgres_when_transport_unavailable(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchall_rows=[
            (
                "default-regime",
                2,
                "Canonical regime v2",
                {"highVolEnterThreshold": 28.0, "highVolExitThreshold": 28.0},
                "published",
                "cfg-hash",
                "2026-04-01T00:00:00Z",
                "2026-04-01T00:00:00Z",
                "2026-04-01T01:00:00Z",
                "tester",
            )
        ]
    )
    monkeypatch.setattr(
        "asset_allocation_runtime_common.regime_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = RegimeRepository(dsn="postgresql://test", transport=None)
    monkeypatch.setattr(repo, "_get_transport", lambda: None)

    result = repo.list_active_regime_model_revisions()

    assert result == [
        {
            "name": "default-regime",
            "version": 2,
            "description": "Canonical regime v2",
            "config": {"highVolEnterThreshold": 28.0, "highVolExitThreshold": 28.0},
            "status": "published",
            "config_hash": "cfg-hash",
            "published_at": "2026-04-01T00:00:00Z",
            "created_at": "2026-04-01T00:00:00Z",
            "activated_at": "2026-04-01T01:00:00Z",
            "activated_by": "tester",
        }
    ]
    assert any("FROM core.regime_model_revisions AS r" in sql for sql, _params in cursor.executed)


def test_get_active_regime_model_revision_falls_back_to_postgres_on_control_plane_error(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[
            (
                "default-regime",
                2,
                "Canonical regime v2",
                {"highVolEnterThreshold": 28.0, "highVolExitThreshold": 28.0},
                "published",
                "cfg-hash",
                "2026-04-01T00:00:00Z",
                "2026-04-01T00:00:00Z",
                "2026-04-01T01:00:00Z",
                "tester",
            )
        ]
    )
    monkeypatch.setattr(
        "asset_allocation_runtime_common.regime_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    class _BoomTransport:
        def request_json(self, method: str, path: str, *, params: dict[str, Any] | None = None):
            raise httpx.ReadTimeout("timeout")

    repo = RegimeRepository(dsn="postgresql://test", transport=_BoomTransport())
    monkeypatch.setattr(repo, "_request_retry_config", lambda: (1, 0.0))

    result = repo.get_active_regime_model_revision("default-regime")

    assert result is not None
    assert result["version"] == 2
    assert result["config"]["highVolExitThreshold"] == 28.0
