from __future__ import annotations

from typing import Any

from asset_allocation_contracts.stock_screener import StockScreenerRequest, StockScreenerResponse

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport


def _serialize_screener_params(request: StockScreenerRequest) -> dict[str, Any]:
    params = request.model_dump(mode="json", exclude_none=True)
    for key in ("sectors", "industries", "countries"):
        value = params.get(key)
        if isinstance(value, list):
            joined = ",".join(str(item).strip() for item in value if str(item).strip())
            if joined:
                params[key] = joined
            else:
                params.pop(key, None)
    return params


class StockScreenerRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def get_stock_screener(
        self,
        request: StockScreenerRequest | dict[str, Any] | None = None,
    ) -> StockScreenerResponse:
        validated_request = request if isinstance(request, StockScreenerRequest) else StockScreenerRequest.model_validate(request or {})
        payload = self.transport.request_json(
            "GET",
            "/api/data/screener",
            params=_serialize_screener_params(validated_request),
        )
        if not isinstance(payload, dict):
            raise ValueError("Control-plane stock screener response was not a JSON object.")
        return StockScreenerResponse.model_validate(payload)
