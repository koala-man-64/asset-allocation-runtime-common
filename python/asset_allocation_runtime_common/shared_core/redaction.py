from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

REDACTED = "[REDACTED]"

_SENSITIVE_KEY_MARKERS = (
    "authorization",
    "accesstoken",
    "refreshtoken",
    "bearertoken",
    "apikey",
    "accountkey",
    "clientsecret",
    "connectionstring",
    "credential",
    "password",
    "passwd",
    "postgresdsn",
    "sas",
    "secret",
    "signature",
    "subscriptionkey",
    "token",
)

_KEY_VALUE_RE = re.compile(
    r"(?P<prefix>[\"']?"
    r"(?:authorization|access[_-]?token|refresh[_-]?token|bearer[_-]?token|api[_-]?key|apikey|"
    r"account[_-]?key|client[_-]?secret|connection[_-]?string|credential|password|passwd|pwd|"
    r"postgres[_-]?dsn|sas[_-]?token|secret|signature|subscription[_-]?key|token)"
    r"[\"']?\s*[:=]\s*)"
    r"(?P<quote>[\"']?)"
    r"(?P<value>[^\"'\s,;&}]+)"
    r"(?P=quote)",
    re.IGNORECASE,
)
_AUTH_HEADER_RE = re.compile(r"\b(?P<scheme>Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_URL_USERINFO_RE = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<userinfo>[^/@\s]+:[^/@\s]+)@")


def is_sensitive_key(key: object) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key or "").lower())
    return any(marker in normalized for marker in _SENSITIVE_KEY_MARKERS)


def redact_text(value: object) -> str:
    text = str(value or "")
    if not text:
        return text

    text = _AUTH_HEADER_RE.sub(lambda match: f"{match.group('scheme')} {REDACTED}", text)
    text = _URL_USERINFO_RE.sub(lambda match: f"{match.group('scheme')}{REDACTED}@", text)

    def replace_secret(match: re.Match[str]) -> str:
        quote = match.group("quote") or ""
        return f"{match.group('prefix')}{quote}{REDACTED}{quote}"

    return _KEY_VALUE_RE.sub(replace_secret, text)


def redact_secrets(value: Any, *, _depth: int = 0, _max_depth: int = 8) -> Any:
    if _depth >= _max_depth:
        return REDACTED
    if value is None:
        return None
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, bytes):
        try:
            return redact_text(value.decode("utf-8", errors="replace"))
        except Exception:
            return REDACTED
    if isinstance(value, Mapping):
        return {
            key: REDACTED if is_sensitive_key(key) else redact_secrets(item, _depth=_depth + 1, _max_depth=_max_depth)
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_secrets(item, _depth=_depth + 1, _max_depth=_max_depth) for item in value)
    if isinstance(value, list):
        return [redact_secrets(item, _depth=_depth + 1, _max_depth=_max_depth) for item in value]
    if isinstance(value, set):
        return {redact_secrets(item, _depth=_depth + 1, _max_depth=_max_depth) for item in value}
    return value


def redact_exception_cause(exc: BaseException) -> RuntimeError:
    return RuntimeError(redact_text(f"{type(exc).__name__}: {exc}"))
