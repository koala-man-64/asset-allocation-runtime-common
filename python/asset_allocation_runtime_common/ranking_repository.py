from __future__ import annotations

from typing import Any

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class RankingRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def get_ranking_schema(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/rankings/{name}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def list_ranking_schemas(self) -> list[dict[str, Any]]:
        payload = self.transport.request_json("GET", "/api/internal/rankings")
        return payload if isinstance(payload, list) else []

    def get_ranking_schema_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self.transport.request_json(
                "GET",
                f"/api/internal/rankings/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return payload if isinstance(payload, dict) else None

    def claim_next_refresh(self, *, execution_name: str | None = None) -> dict[str, Any] | None:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/rankings/refresh/claim",
            json_body={"executionName": execution_name},
        )
        if isinstance(payload, dict):
            work = payload.get("work")
            if isinstance(work, dict):
                return work
        return None

    def complete_refresh(
        self,
        strategy_name: str,
        *,
        claim_token: str,
        run_id: str | None = None,
        dependency_fingerprint: str | None = None,
        dependency_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self.transport.request_json(
            "POST",
            f"/api/internal/rankings/refresh/{strategy_name}/complete",
            json_body={
                "claimToken": claim_token,
                "runId": run_id,
                "dependencyFingerprint": dependency_fingerprint,
                "dependencyState": dependency_state,
            },
        )
        return payload if isinstance(payload, dict) else {}

    def fail_refresh(self, strategy_name: str, *, claim_token: str, error: str) -> dict[str, Any]:
        payload = self.transport.request_json(
            "POST",
            f"/api/internal/rankings/refresh/{strategy_name}/fail",
            json_body={
                "claimToken": claim_token,
                "error": error,
            },
        )
        return payload if isinstance(payload, dict) else {}

    def save_ranking_schema(self, *args, **kwargs) -> None:
        raise NotImplementedError("Jobs repo does not mutate ranking control-plane state directly.")

    def delete_ranking_schema(self, *args, **kwargs) -> bool:
        raise NotImplementedError("Jobs repo does not mutate ranking control-plane state directly.")
