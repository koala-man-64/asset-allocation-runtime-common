from __future__ import annotations

from typing import Any, TypeVar

from asset_allocation_contracts.intraday import (
    IntradayMonitorClaimRequest,
    IntradayMonitorClaimResponse,
    IntradayMonitorCompleteRequest,
    IntradayMonitorEvent,
    IntradayMonitorFailRequest,
    IntradayMonitorRunSummary,
    IntradayRefreshBatchSummary,
    IntradayRefreshClaimRequest,
    IntradayRefreshClaimResponse,
    IntradayRefreshCompleteRequest,
    IntradayRefreshFailRequest,
    IntradaySymbolStatus,
    IntradayWatchlistDetail,
    IntradayWatchlistSymbolAppendRequest,
    IntradayWatchlistSymbolAppendResponse,
    IntradayWatchlistSummary,
    IntradayWatchlistUpsertRequest,
)

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport

_ResponseT = TypeVar("_ResponseT")


def _validate_object_response(payload: Any, response_type: type[_ResponseT]) -> _ResponseT:
    if not isinstance(payload, dict):
        raise ValueError("Control-plane response was not a JSON object.")
    return response_type.model_validate(payload)


class IntradayRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def ready(self) -> None:
        self.transport.probe("/api/internal/intraday/ready")

    def claim_monitor_run(self, *, execution_name: str | None = None) -> IntradayMonitorClaimResponse:
        response = self.transport.request_json(
            "POST",
            "/api/internal/intraday-monitor/claim",
            json_body=IntradayMonitorClaimRequest(executionName=execution_name).model_dump(
                mode="json",
                exclude_none=True,
            ),
        )
        return _validate_object_response(response, IntradayMonitorClaimResponse)

    def complete_monitor_run(
        self,
        run_id: str,
        *,
        claim_token: str,
        symbol_statuses: list[IntradaySymbolStatus] | None = None,
        events: list[IntradayMonitorEvent] | None = None,
        refresh_symbols: list[str] | None = None,
    ) -> IntradayMonitorRunSummary:
        request = IntradayMonitorCompleteRequest(
            claimToken=claim_token,
            symbolStatuses=symbol_statuses or [],
            events=events or [],
            refreshSymbols=refresh_symbols or [],
        )
        response = self.transport.request_json(
            "POST",
            f"/api/internal/intraday-monitor/runs/{run_id}/complete",
            json_body=request.model_dump(mode="json", exclude_none=True),
        )
        return _validate_object_response(response, IntradayMonitorRunSummary)

    def fail_monitor_run(self, run_id: str, *, claim_token: str, error: str) -> IntradayMonitorRunSummary:
        response = self.transport.request_json(
            "POST",
            f"/api/internal/intraday-monitor/runs/{run_id}/fail",
            json_body=IntradayMonitorFailRequest(claimToken=claim_token, error=error).model_dump(
                mode="json",
                exclude_none=True,
            ),
        )
        return _validate_object_response(response, IntradayMonitorRunSummary)

    def claim_refresh_batch(self, *, execution_name: str | None = None) -> IntradayRefreshClaimResponse:
        response = self.transport.request_json(
            "POST",
            "/api/internal/intraday-refresh/claim",
            json_body=IntradayRefreshClaimRequest(executionName=execution_name).model_dump(
                mode="json",
                exclude_none=True,
            ),
        )
        return _validate_object_response(response, IntradayRefreshClaimResponse)

    def complete_refresh_batch(self, batch_id: str, *, claim_token: str) -> IntradayRefreshBatchSummary:
        response = self.transport.request_json(
            "POST",
            f"/api/internal/intraday-refresh/batches/{batch_id}/complete",
            json_body=IntradayRefreshCompleteRequest(claimToken=claim_token).model_dump(
                mode="json",
                exclude_none=True,
            ),
        )
        return _validate_object_response(response, IntradayRefreshBatchSummary)

    def fail_refresh_batch(self, batch_id: str, *, claim_token: str, error: str) -> IntradayRefreshBatchSummary:
        response = self.transport.request_json(
            "POST",
            f"/api/internal/intraday-refresh/batches/{batch_id}/fail",
            json_body=IntradayRefreshFailRequest(claimToken=claim_token, error=error).model_dump(
                mode="json",
                exclude_none=True,
            ),
        )
        return _validate_object_response(response, IntradayRefreshBatchSummary)

    def list_watchlists(self) -> list[IntradayWatchlistSummary]:
        response = self.transport.request_json("GET", "/api/intraday/watchlists")
        if not isinstance(response, list):
            return []
        return [IntradayWatchlistSummary.model_validate(item) for item in response if isinstance(item, dict)]

    def get_watchlist(self, watchlist_id: str) -> IntradayWatchlistDetail | None:
        try:
            response = self.transport.request_json("GET", f"/api/intraday/watchlists/{watchlist_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return _validate_object_response(response, IntradayWatchlistDetail)

    def create_watchlist(
        self,
        payload: IntradayWatchlistUpsertRequest | dict[str, Any],
    ) -> IntradayWatchlistDetail:
        request = (
            payload
            if isinstance(payload, IntradayWatchlistUpsertRequest)
            else IntradayWatchlistUpsertRequest.model_validate(payload)
        )
        response = self.transport.request_json(
            "POST",
            "/api/intraday/watchlists",
            json_body=request.model_dump(mode="json", exclude_none=True),
        )
        return _validate_object_response(response, IntradayWatchlistDetail)

    def update_watchlist(
        self,
        watchlist_id: str,
        payload: IntradayWatchlistUpsertRequest | dict[str, Any],
    ) -> IntradayWatchlistDetail:
        request = (
            payload
            if isinstance(payload, IntradayWatchlistUpsertRequest)
            else IntradayWatchlistUpsertRequest.model_validate(payload)
        )
        response = self.transport.request_json(
            "PUT",
            f"/api/intraday/watchlists/{watchlist_id}",
            json_body=request.model_dump(mode="json", exclude_none=True),
        )
        return _validate_object_response(response, IntradayWatchlistDetail)

    def delete_watchlist(self, watchlist_id: str) -> bool:
        try:
            self.transport.request_json("DELETE", f"/api/intraday/watchlists/{watchlist_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return False
            raise
        return True

    def enqueue_watchlist_run(self, watchlist_id: str) -> IntradayMonitorRunSummary:
        response = self.transport.request_json("POST", f"/api/intraday/watchlists/{watchlist_id}/run")
        return _validate_object_response(response, IntradayMonitorRunSummary)

    def append_watchlist_symbols(
        self,
        watchlist_id: str,
        payload: IntradayWatchlistSymbolAppendRequest | dict[str, Any] | None = None,
        *,
        symbols: list[str] | None = None,
        queue_run: bool = True,
        reason: str | None = None,
    ) -> IntradayWatchlistSymbolAppendResponse:
        if payload is None:
            payload = IntradayWatchlistSymbolAppendRequest(
                symbols=symbols or [],
                queueRun=queue_run,
                reason=reason,
            )
        request = (
            payload
            if isinstance(payload, IntradayWatchlistSymbolAppendRequest)
            else IntradayWatchlistSymbolAppendRequest.model_validate(payload)
        )
        response = self.transport.request_json(
            "POST",
            f"/api/intraday/watchlists/{watchlist_id}/symbols",
            json_body=request.model_dump(mode="json", exclude_none=True),
        )
        return _validate_object_response(response, IntradayWatchlistSymbolAppendResponse)
