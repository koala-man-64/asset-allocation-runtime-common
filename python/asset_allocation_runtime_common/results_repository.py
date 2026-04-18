from __future__ import annotations

from typing import Any

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport


class ResultsRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def reconcile(self, *, dry_run: bool = False) -> dict[str, Any]:
        payload = self.transport.request_json(
            "POST",
            "/api/internal/results/reconcile",
            json_body={"dryRun": dry_run},
        )
        return payload if isinstance(payload, dict) else {}
