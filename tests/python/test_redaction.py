from __future__ import annotations

import json
import logging
import sys

from asset_allocation_runtime_common.shared_core.logging_config import JsonFormatter
from asset_allocation_runtime_common.shared_core.redaction import REDACTED, redact_secrets, redact_text


def test_redact_secrets_handles_sensitive_keys_urls_and_bearer_tokens() -> None:
    payload = {
        "apiKey": "alpha-secret",
        "safe": "AAPL",
        "nested": {
            "url": "postgres://user:db-password@example.test/db?apiKey=query-secret",
            "header": "Authorization: Bearer bearer-secret",
        },
    }

    redacted = redact_secrets(payload)

    assert redacted["apiKey"] == REDACTED
    assert redacted["safe"] == "AAPL"
    serialized = json.dumps(redacted)
    assert "alpha-secret" not in serialized
    assert "db-password" not in serialized
    assert "query-secret" not in serialized
    assert "bearer-secret" not in serialized
    assert serialized.count(REDACTED) >= 4


def test_json_formatter_redacts_message_context_and_traceback() -> None:
    formatter = JsonFormatter()

    try:
        raise RuntimeError("provider failed with apiKey=traceback-secret")
    except RuntimeError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="request failed token=%s",
        args=("message-secret",),
        exc_info=exc_info,
        func="test",
    )
    record.context = {"Authorization": "Bearer context-secret", "symbol": "AAPL"}  # type: ignore[attr-defined]

    output = formatter.format(record)
    parsed = json.loads(output)

    assert parsed["symbol"] == "AAPL"
    assert "message-secret" not in output
    assert "context-secret" not in output
    assert "traceback-secret" not in output
    assert "token=" + REDACTED in parsed["message"]


def test_redact_text_keeps_non_secret_text_readable() -> None:
    assert redact_text("symbol=AAPL status=503") == "symbol=AAPL status=503"
