from __future__ import annotations

from typing import Any

try:
    from asset_allocation_contracts.symbol_enrichment import (
        SymbolCleanupRunSummary,
        SymbolCleanupWorkItem,
        SymbolEnrichmentResolveRequest,
        SymbolEnrichmentResolveResponse,
    )
except ModuleNotFoundError as exc:
    if exc.name != "asset_allocation_contracts.symbol_enrichment":
        raise
    from asset_allocation_runtime_common._symbol_enrichment_contract_compat import (
        SymbolCleanupRunSummary,
        SymbolCleanupWorkItem,
        SymbolEnrichmentResolveRequest,
        SymbolEnrichmentResolveResponse,
    )

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


class SymbolEnrichmentRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()
        self.dsn = dsn

    def claim_work(self, *, execution_name: str | None = None) -> SymbolCleanupWorkItem | None:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/symbol-cleanup/claim",
            json_body={"executionName": execution_name},
        )
        if not isinstance(payload, dict):
            return None
        work = payload.get("work")
        if not isinstance(work, dict):
            return None
        return SymbolCleanupWorkItem.model_validate(work)

    def complete_work(
        self,
        work_id: str,
        *,
        result: SymbolEnrichmentResolveResponse | dict[str, Any] | None = None,
    ) -> None:
        json_body: dict[str, Any] = {}
        if result is not None:
            if isinstance(result, SymbolEnrichmentResolveResponse):
                json_body["result"] = result.model_dump(mode="json")
            else:
                json_body["result"] = result
        self.transport.request_json(
            "POST",
            f"/api/internal/symbol-cleanup/{work_id}/complete",
            json_body=json_body,
        )

    def fail_work(self, work_id: str, *, error: str) -> None:
        self.transport.request_json(
            "POST",
            f"/api/internal/symbol-cleanup/{work_id}/fail",
            json_body={"error": error},
        )

    def resolve_symbol_profile(
        self,
        payload: SymbolEnrichmentResolveRequest | dict[str, Any],
    ) -> SymbolEnrichmentResolveResponse:
        json_body = payload.model_dump(mode="json") if isinstance(payload, SymbolEnrichmentResolveRequest) else payload
        response = self.transport.request_json(
            "POST",
            "/api/internal/symbol-enrichment/resolve",
            json_body=json_body,
        )
        if not isinstance(response, dict):
            raise ValueError("Symbol enrichment resolve response was not a JSON object.")
        return SymbolEnrichmentResolveResponse.model_validate(response)

    def get_run(self, run_id: str) -> SymbolCleanupRunSummary | None:
        try:
            payload = self.transport.request_json("GET", f"/api/internal/symbol-cleanup/runs/{run_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        if not isinstance(payload, dict):
            return None
        return SymbolCleanupRunSummary.model_validate(payload)
