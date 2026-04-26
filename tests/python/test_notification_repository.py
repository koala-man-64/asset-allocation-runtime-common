from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx

from asset_allocation_contracts.notifications import NotificationRecipient
from asset_allocation_contracts.trade_desk import TradeOrderPreviewRequest
from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.notification_repository import NotificationRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def _status_payload() -> dict[str, object]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return {
        "requestId": "notification-1",
        "kind": "trade_approval",
        "status": "pending",
        "sourceRepo": "asset-allocation-jobs",
        "clientRequestId": "client-1",
        "title": "Approve MSFT buy",
        "description": "Approve or deny the proposed order.",
        "createdAt": now,
        "updatedAt": now,
        "decisionStatus": "pending",
        "executionStatus": "pending_approval",
    }


def test_create_notification_posts_released_contract_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/notifications"
        assert json.loads(request.content.decode("utf-8")) == {
            "sourceRepo": "asset-allocation-jobs",
            "clientRequestId": "client-1",
            "idempotencyKey": "notification-idem-0001",
            "kind": "message",
            "title": "Backtest complete",
            "description": "Backtest run finished.",
            "recipients": [
                {
                    "recipientId": "operator",
                    "email": "ops@example.com",
                    "channels": ["email"],
                }
            ],
            "metadata": {},
        }
        return httpx.Response(200, json={**_status_payload(), "kind": "message", "title": "Backtest complete"})

    transport = _build_transport(handler)
    try:
        repo = NotificationRepository(transport=transport)
        result = repo.create_notification(
            {
                "sourceRepo": "asset-allocation-jobs",
                "clientRequestId": "client-1",
                "idempotencyKey": "notification-idem-0001",
                "kind": "message",
                "title": "Backtest complete",
                "description": "Backtest run finished.",
                "recipients": [NotificationRecipient(recipientId="operator", email="ops@example.com", channels=["email"])],
            }
        )
    finally:
        transport.close()

    assert result.requestId == "notification-1"
    assert result.kind == "message"


def test_create_trade_approval_notification_reuses_trade_order_contract() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        assert request.url.path == "/api/notifications"
        assert body["kind"] == "trade_approval"
        assert body["tradeApproval"]["order"]["symbol"] == "MSFT"
        assert body["tradeApproval"]["accountId"] == body["tradeApproval"]["order"]["accountId"]
        return httpx.Response(200, json=_status_payload())

    transport = _build_transport(handler)
    try:
        repo = NotificationRepository(transport=transport)
        result = repo.create_notification(
            {
                "sourceRepo": "asset-allocation-jobs",
                "clientRequestId": "client-approval-1",
                "idempotencyKey": "notification-idem-0002",
                "kind": "trade_approval",
                "title": "Approve MSFT buy",
                "description": "Approve or deny the proposed order.",
                "recipients": [
                    {
                        "recipientId": "operator",
                        "phoneNumber": "+15555550100",
                        "channels": ["sms"],
                    }
                ],
                "tradeApproval": {
                    "accountId": "acct-paper",
                    "previewId": "preview-1",
                    "orderHash": "hash-1",
                    "placeIdempotencyKey": "place-idem-0000001",
                    "order": TradeOrderPreviewRequest(
                        accountId="acct-paper",
                        environment="paper",
                        clientRequestId="trade-client-1",
                        symbol="msft",
                        side="buy",
                        orderType="limit",
                        quantity=10,
                        limitPrice=100,
                    ),
                },
            }
        )
    finally:
        transport.close()

    assert result.decisionStatus == "pending"
    assert result.executionStatus == "pending_approval"


def test_notification_status_action_and_decision_paths_are_typed() -> None:
    calls: list[tuple[str, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.content.decode("utf-8") or None))
        if request.url.path == "/api/notifications/actions/token-1":
            now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            return httpx.Response(
                200,
                json={
                    "requestId": "notification-1",
                    "tokenId": "token-1",
                    "kind": "trade_approval",
                    "title": "Approve MSFT buy",
                    "description": "Approve or deny the proposed order.",
                    "createdAt": now,
                    "decisionStatus": "pending",
                    "executionStatus": "pending_approval",
                },
            )
        return httpx.Response(200, json=_status_payload())

    transport = _build_transport(handler)
    try:
        repo = NotificationRepository(transport=transport)
        status = repo.get_status("notification-1")
        action = repo.get_action("token-1")
        approved = repo.approve_action("token-1", reason="Risk approved.")
        denied = repo.deny_action("token-2")
    finally:
        transport.close()

    assert status is not None
    assert status.requestId == "notification-1"
    assert action is not None
    assert action.tokenId == "token-1"
    assert approved.requestId == "notification-1"
    assert denied.requestId == "notification-1"
    assert calls == [
        ("GET", "/api/notifications/notification-1", None),
        ("GET", "/api/notifications/actions/token-1", None),
        ("POST", "/api/notifications/actions/token-1/approve", '{"decision":"approve","reason":"Risk approved."}'),
        ("POST", "/api/notifications/actions/token-2/deny", '{"decision":"deny","reason":""}'),
    ]
