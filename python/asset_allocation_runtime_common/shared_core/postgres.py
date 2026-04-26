from __future__ import annotations

from datetime import date, datetime
import math
import os
from typing import Any, Iterable, Optional, Sequence

import pandas as pd


class PostgresError(RuntimeError):
    pass


def get_dsn(env_var: str) -> Optional[str]:
    raw = os.environ.get(env_var)
    if not raw:
        return None
    value = str(raw).strip()
    return value or None


def _import_psycopg():
    try:
        import psycopg  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise PostgresError(
            "psycopg is required for Postgres features. Install dependencies from requirements.txt."
        ) from exc
    return psycopg


def connect(dsn: str):
    psycopg = _import_psycopg()
    return psycopg.connect(dsn)


def require_columns(df: pd.DataFrame, required: Sequence[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime()
        except Exception:
            pass
    if hasattr(value, "item") and not isinstance(value, (datetime, date, bytes, bytearray, memoryview, str)):
        try:
            return normalize_scalar(value.item())
        except Exception:
            pass
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def copy_rows(
    cursor: Any,
    *,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> None:
    cols = ", ".join(columns)
    statement = f"COPY {table} ({cols}) FROM STDIN"
    with cursor.copy(statement) as copy:
        for row in rows:
            copy.write_row([normalize_scalar(value) for value in row])

