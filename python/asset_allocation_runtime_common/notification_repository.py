from __future__ import annotations

from typing import Any, Literal, TypeVar

from asset_allocation_contracts.notifications import (
    CreateNotificationRequest,
    NotificationActionDetailResponse,
    NotificationDecisionRequest,
    NotificationStatusResponse,
)

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport

_ResponseT = TypeVar("_ResponseT")


def _dump_model(payload: Any) -> dict[str, Any]:
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json", exclude_none=True)
    if isinstance(payload, dict):
        return payload
    raise TypeError(f"Expected a pydantic model or dict payload, got {type(payload).__name__}.")


def _validate_object_response(payload: Any, response_type: type[_ResponseT]) -> _ResponseT:
    if not isinstance(payload, dict):
        raise ValueError("Control-plane response was not a JSON object.")
    return response_type.model_validate(payload)


class NotificationRepository:
    def __init__(self, *, transport: ControlPlaneTransport | None = None):
        self.transport = transport or ControlPlaneTransport.from_env()

    def create_notification(
        self,
        payload: CreateNotificationRequest | dict[str, Any],
    ) -> NotificationStatusResponse:
        request = (
            payload
            if isinstance(payload, CreateNotificationRequest)
            else CreateNotificationRequest.model_validate(payload)
        )
        response = self.transport.request_json(
            "POST",
            "/api/notifications",
            json_body=request.model_dump(mode="json", exclude_none=True),
        )
        return _validate_object_response(response, NotificationStatusResponse)

    def get_status(self, request_id: str) -> NotificationStatusResponse | None:
        try:
            response = self.transport.request_json("GET", f"/api/notifications/{request_id}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return _validate_object_response(response, NotificationStatusResponse)

    def get_action(self, token: str) -> NotificationActionDetailResponse | None:
        try:
            response = self.transport.request_json("GET", f"/api/notifications/actions/{token}")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return None
            raise
        return _validate_object_response(response, NotificationActionDetailResponse)

    def approve_action(self, token: str, *, reason: str = "") -> NotificationStatusResponse:
        return self._submit_decision(token, "approve", reason=reason)

    def deny_action(self, token: str, *, reason: str = "") -> NotificationStatusResponse:
        return self._submit_decision(token, "deny", reason=reason)

    def _submit_decision(
        self,
        token: str,
        decision: Literal["approve", "deny"],
        *,
        reason: str = "",
    ) -> NotificationStatusResponse:
        request = NotificationDecisionRequest(decision=decision, reason=reason)
        response = self.transport.request_json(
            "POST",
            f"/api/notifications/actions/{token}/{decision}",
            json_body=_dump_model(request),
        )
        return _validate_object_response(response, NotificationStatusResponse)
