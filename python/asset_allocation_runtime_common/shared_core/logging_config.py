import json
import logging
import os
import sys
from copy import copy
from datetime import datetime, timezone
from typing import Any, Dict

from asset_allocation_runtime_common.shared_core.redaction import redact_secrets, redact_text

# Standard Python Log Levels
# CRITICAL = 50
# ERROR = 40
# WARNING = 30
# INFO = 20
# DEBUG = 10
# NOTSET = 0

class SecretRedactingFormatter(logging.Formatter):
    """Formatter base class that redacts secrets from messages and tracebacks."""

    def _redacted_record(self, record: logging.LogRecord) -> logging.LogRecord:
        redacted = copy(record)
        redacted.exc_text = None
        return redacted

    def formatException(self, ei) -> str:  # noqa: N802
        return redact_text(super().formatException(ei))

    def format(self, record: logging.LogRecord) -> str:
        return redact_text(super().format(self._redacted_record(record)))


class JsonFormatter(SecretRedactingFormatter):
    """
    Formatter that outputs JSON strings for structured logging.
    """
    def format(self, record: logging.LogRecord) -> str:
        redacted_record = self._redacted_record(record)
        log_record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": redacted_record.levelname,
            "message": redact_text(redacted_record.getMessage()),
            "module": redacted_record.module,
            "funcName": redacted_record.funcName,
            "line": redacted_record.lineno,
            "logger": redacted_record.name
        }
        
        # Merge extra field if available
        if hasattr(record, 'context') and isinstance(record.context, dict): # type: ignore
            log_record.update(redact_secrets(record.context)) # type: ignore
            
        # Exception handling
        if record.exc_info:
            log_record["exception"] = redact_text(self.formatException(record.exc_info))
            
        return json.dumps(redact_secrets(log_record), default=str)

def configure_logging() -> logging.Logger:
    """
    Configures the root logger based on environment variables.
    ENV: LOG_FORMAT (JSON | TEXT) - Required
    ENV: LOG_LEVEL (DEBUG | INFO | WARNING | ERROR) - Required
    """
    logger = logging.getLogger()
    
    # idempotent configuration
    if logger.handlers:
        return logger
        
    log_format_raw = os.environ.get("LOG_FORMAT", "TEXT")
    log_format = log_format_raw.strip().upper() or "TEXT"

    log_level_raw = os.environ.get("LOG_LEVEL", "INFO")
    log_level_str = log_level_raw.strip().upper() or "INFO"

    if log_format not in {"JSON", "TEXT"}:
        raise ValueError(f"Invalid LOG_FORMAT={log_format_raw!r} (expected JSON or TEXT).")

    try:
        level = getattr(logging, log_level_str)
    except AttributeError as exc:
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level_raw!r} (expected DEBUG|INFO|WARNING|ERROR|CRITICAL)."
        ) from exc
    if not isinstance(level, int):
        raise ValueError(
            f"Invalid LOG_LEVEL={log_level_raw!r} (expected DEBUG|INFO|WARNING|ERROR|CRITICAL)."
        )
    logger.setLevel(level)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    
    if log_format == "JSON":
        handler.setFormatter(JsonFormatter())
    else:
        # Standard readable format
        formatter = SecretRedactingFormatter(
            '%(asctime)s [%(levelname)s] %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
    logger.addHandler(handler)
    
    # Silence Azure SDK spam
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # Silence http client request logs (can include query-string secrets like apikey=...).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    
    return logger
