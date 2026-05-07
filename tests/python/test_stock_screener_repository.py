from __future__ import annotations

from datetime import date

import httpx

from asset_allocation_contracts.stock_screener import StockScreenerRequest
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.stock_screener_repository import StockScreenerRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_get_stock_screener_serializes_contract_params_and_validates_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/data/screener"
        assert request.url.params["q"] == "AAPL"
        assert request.url.params["as_of"] == "2025-01-02"
        assert request.url.params["limit"] == "25"
        assert request.url.params["sort"] == "return_5d"
        assert request.url.params["sectors"] == "Technology,Healthcare"
        assert request.url.params["has_gold"] == "true"
        return httpx.Response(
            200,
            json={
                "asOf": "2025-01-02",
                "total": 1,
                "limit": 25,
                "offset": 0,
                "rows": [{"symbol": "aapl", "close": 189.0, "hasSilver": 1, "hasGold": 1}],
                "summary": {
                    "universeCount": 10,
                    "totalResultCount": 1,
                    "returnedCount": 1,
                    "coverage": {
                        "total": 1,
                        "withSilver": 1,
                        "withGold": 1,
                        "missingSilver": 0,
                        "missingGold": 0,
                    },
                },
                "facets": {
                    "sectors": [{"value": "Technology", "count": 1}],
                    "industries": [],
                    "countries": [{"value": "US", "count": 1}],
                },
            },
        )

    transport = _build_transport(handler)
    try:
        repo = StockScreenerRepository(transport=transport)
        result = repo.get_stock_screener(
            StockScreenerRequest(
                q=" AAPL ",
                as_of=date(2025, 1, 2),
                limit=25,
                sort="return_5d",
                sectors=["Technology", "Healthcare"],
                has_gold=True,
            )
        )
    finally:
        transport.close()

    assert result.asOf == date(2025, 1, 2)
    assert result.rows[0].symbol == "AAPL"
    assert result.summary is not None
    assert result.summary.coverage.withGold == 1


def test_get_stock_screener_rejects_non_object_response() -> None:
    transport = _build_transport(lambda _request: httpx.Response(200, json=[]))
    try:
        repo = StockScreenerRepository(transport=transport)
        try:
            repo.get_stock_screener()
        except ValueError as exc:
            assert "not a JSON object" in str(exc)
        else:
            raise AssertionError("Expected non-object stock screener responses to fail validation.")
    finally:
        transport.close()
