from __future__ import annotations

from asset_allocation_contracts.results import ResultsReconcileResponse
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport


class ResultsRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def reconcile(self, *, dry_run: bool = False) -> ResultsReconcileResponse:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/results/reconcile",
            json_body={"dryRun": dry_run},
        )
        if not isinstance(payload, dict):
            raise ValueError("Results reconcile response was not a JSON object.")
        return ResultsReconcileResponse.model_validate(payload)
