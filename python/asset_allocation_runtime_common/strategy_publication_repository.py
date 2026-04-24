from __future__ import annotations

import os
import time
from typing import Any

from asset_allocation_contracts.strategy_publication import (
    RegimePublicationReconcileMetadata,
    StrategyPublicationReconcileSignalRequest,
    StrategyPublicationReconcileSignalResponse,
)
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport


def _bounded_attempts() -> int:
    raw = str(os.environ.get("STRATEGY_PUBLICATION_SIGNAL_ATTEMPTS") or "3").strip()
    try:
        parsed = int(raw)
    except ValueError:
        parsed = 3
    return min(max(parsed, 1), 5)


class StrategyPublicationRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def record_reconcile_signal(
        self,
        *,
        job_key: str,
        source_fingerprint: str,
        metadata: dict[str, Any] | None = None,
    ) -> StrategyPublicationReconcileSignalResponse:
        request = StrategyPublicationReconcileSignalRequest(
            jobKey=job_key,
            sourceFingerprint=source_fingerprint,
            metadata=RegimePublicationReconcileMetadata.model_validate(metadata or {}),
        )
        attempts = _bounded_attempts()
        last_error: Exception | None = None
        payload: Any = None
        for attempt in range(1, attempts + 1):
            try:
                payload = self.transport.request_json(
                    "POST",
                    "/api/internal/strategy-publications/reconcile-signal",
                    json_body=request.model_dump(mode="json", by_alias=True),
                )
                last_error = None
                break
            except ControlPlaneRequestError as exc:
                last_error = exc
                if exc.status_code is not None and 400 <= exc.status_code < 500:
                    break
            except Exception as exc:
                last_error = exc
            if attempt < attempts:
                time.sleep(min(0.5 * attempt, 2.0))
        if last_error is not None:
            raise last_error
        return StrategyPublicationReconcileSignalResponse.model_validate(payload)
