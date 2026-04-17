from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any, Iterable, Sequence


class PostgresError(RuntimeError):
    pass


def _import_psycopg():
    try:
        import psycopg  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise PostgresError(
            "psycopg is required for Postgres features. Install dependencies from pyproject.toml."
        ) from exc
    return psycopg


def connect(dsn: str):
    psycopg = _import_psycopg()
    return psycopg.connect(dsn)


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
