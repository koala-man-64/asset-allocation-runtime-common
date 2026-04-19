from __future__ import annotations

import logging
import os
import time
from typing import Any

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneRequestError, ControlPlaneTransport
from asset_allocation_runtime_common.foundation.postgres import connect

logger = logging.getLogger(__name__)
_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class RegimeRepository:
    def __init__(self, dsn: str | None = None, *, transport: ControlPlaneTransport | None = None):
        self._transport = transport
        self.dsn = dsn or str(os.environ.get("POSTGRES_DSN") or "").strip() or None

    def _get_transport(self) -> ControlPlaneTransport | None:
        if self._transport is not None:
            return self._transport
        try:
            self._transport = ControlPlaneTransport.from_env()
        except Exception as exc:
            logger.warning("Control-plane transport unavailable for regime repository: %s", exc)
            return None
        return self._transport

    def _request_retry_config(self) -> tuple[int, float]:
        raw_attempts = str(os.environ.get("ASSET_ALLOCATION_API_READ_RETRY_ATTEMPTS") or "").strip()
        raw_base_seconds = str(os.environ.get("ASSET_ALLOCATION_API_READ_RETRY_BASE_SECONDS") or "").strip()
        try:
            attempts = int(raw_attempts) if raw_attempts else 3
        except ValueError:
            attempts = 3
        try:
            base_seconds = float(raw_base_seconds) if raw_base_seconds else 0.5
        except ValueError:
            base_seconds = 0.5
        return max(1, attempts), max(0.0, base_seconds)

    def _request_json_with_retry(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        transport = self._get_transport()
        if transport is None:
            raise RuntimeError("Control-plane transport is unavailable.")

        attempts, base_seconds = self._request_retry_config()
        for attempt in range(1, attempts + 1):
            try:
                return transport.request_json(method, path, params=params)
            except ControlPlaneRequestError as exc:
                retryable = exc.status_code in _RETRYABLE_STATUS_CODES or exc.status_code is None
                if exc.status_code == 404 or not retryable or attempt >= attempts:
                    raise
                logger.warning(
                    "Retrying control-plane regime read after %s %s failed (attempt %s/%s, status=%s): %s",
                    method.upper(),
                    path,
                    attempt,
                    attempts,
                    exc.status_code,
                    exc,
                )
            except Exception as exc:
                if attempt >= attempts:
                    raise
                logger.warning(
                    "Retrying control-plane regime read after %s %s failed (attempt %s/%s): %s",
                    method.upper(),
                    path,
                    attempt,
                    attempts,
                    exc,
                )
            time.sleep(base_seconds * (2 ** (attempt - 1)))
        raise RuntimeError(f"Unreachable retry loop for {method.upper()} {path}.")

    def _fetchone_postgres(self, sql: str, params: tuple[Any, ...], columns: list[str]) -> dict[str, Any] | None:
        if not self.dsn:
            return None
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        if not row:
            return None
        return dict(zip(columns, row))

    def _fetchall_postgres(self, sql: str, params: tuple[Any, ...], columns: list[str]) -> list[dict[str, Any]]:
        if not self.dsn:
            return []
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def _get_regime_model_revision_from_postgres(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        columns = [
            "name",
            "version",
            "description",
            "config",
            "status",
            "config_hash",
            "published_at",
            "created_at",
        ]
        if version is None:
            return self._fetchone_postgres(
                """
                SELECT
                    model_name,
                    version,
                    description,
                    config,
                    status,
                    config_hash,
                    published_at,
                    created_at
                FROM core.regime_model_revisions
                WHERE model_name = %s
                ORDER BY version DESC
                LIMIT 1
                """,
                (name,),
                columns,
            )
        return self._fetchone_postgres(
            """
            SELECT
                model_name,
                version,
                description,
                config,
                status,
                config_hash,
                published_at,
                created_at
            FROM core.regime_model_revisions
            WHERE model_name = %s AND version = %s
            """,
            (name, int(version)),
            columns,
        )

    def _get_active_regime_model_revision_from_postgres(self, name: str) -> dict[str, Any] | None:
        return self._fetchone_postgres(
            """
            WITH latest_activation AS (
                SELECT model_name, model_version, activated_at, activated_by
                FROM core.regime_model_activations
                WHERE model_name = %s
                ORDER BY activated_at DESC, activation_id DESC
                LIMIT 1
            )
            SELECT
                r.model_name,
                r.version,
                r.description,
                r.config,
                r.status,
                r.config_hash,
                r.published_at,
                r.created_at,
                a.activated_at,
                a.activated_by
            FROM core.regime_model_revisions AS r
            JOIN latest_activation AS a
              ON a.model_name = r.model_name
             AND a.model_version = r.version
            """,
            (name,),
            [
                "name",
                "version",
                "description",
                "config",
                "status",
                "config_hash",
                "published_at",
                "created_at",
                "activated_at",
                "activated_by",
            ],
        )

    def _list_active_regime_model_revisions_from_postgres(self) -> list[dict[str, Any]]:
        return self._fetchall_postgres(
            """
            WITH latest_activations AS (
                SELECT DISTINCT ON (model_name)
                    model_name,
                    model_version,
                    activated_at,
                    activated_by
                FROM core.regime_model_activations
                ORDER BY model_name, activated_at DESC, activation_id DESC
            )
            SELECT
                r.model_name,
                r.version,
                r.description,
                r.config,
                r.status,
                r.config_hash,
                r.published_at,
                r.created_at,
                a.activated_at,
                a.activated_by
            FROM core.regime_model_revisions AS r
            JOIN latest_activations AS a
              ON a.model_name = r.model_name
             AND a.model_version = r.version
            ORDER BY r.model_name
            """,
            (),
            [
                "name",
                "version",
                "description",
                "config",
                "status",
                "config_hash",
                "published_at",
                "created_at",
                "activated_at",
                "activated_by",
            ],
        )

    def _get_regime_latest_from_postgres(
        self,
        *,
        model_name: str,
        model_version: int | None = None,
    ) -> dict[str, Any] | None:
        resolved_version = int(model_version) if model_version is not None else None
        if resolved_version is None:
            active = self._get_active_regime_model_revision_from_postgres(model_name)
            if not active:
                return None
            resolved_version = int(active["version"])
        return self._fetchone_postgres(
            """
            SELECT
                as_of_date,
                effective_from_date,
                model_name,
                model_version,
                regime_code,
                regime_status,
                matched_rule_id,
                halt_flag,
                halt_reason,
                spy_return_20d,
                rvol_10d_ann,
                vix_spot_close,
                vix3m_close,
                vix_slope,
                trend_state,
                curve_state,
                vix_gt_32_streak,
                computed_at
            FROM gold.regime_latest
            WHERE model_name = %s AND model_version = %s
            """,
            (model_name, resolved_version),
            [
                "as_of_date",
                "effective_from_date",
                "model_name",
                "model_version",
                "regime_code",
                "regime_status",
                "matched_rule_id",
                "halt_flag",
                "halt_reason",
                "spy_return_20d",
                "rvol_10d_ann",
                "vix_spot_close",
                "vix3m_close",
                "vix_slope",
                "trend_state",
                "curve_state",
                "vix_gt_32_streak",
                "computed_at",
            ],
        )

    def get_regime_model_revision(self, name: str, version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self._request_json_with_retry(
                "GET",
                f"/api/internal/regimes/models/{name}/revision",
                params={"version": version} if version is not None else None,
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return self._get_regime_model_revision_from_postgres(name, version=version)
            logger.warning("Falling back to Postgres for regime revision lookup: %s", exc)
            return self._get_regime_model_revision_from_postgres(name, version=version)
        except Exception as exc:
            logger.warning("Falling back to Postgres for regime revision lookup: %s", exc)
            return self._get_regime_model_revision_from_postgres(name, version=version)
        return payload if isinstance(payload, dict) else None

    def get_active_regime_model_revision(self, name: str) -> dict[str, Any] | None:
        try:
            payload = self._request_json_with_retry("GET", f"/api/internal/regimes/models/{name}/active")
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return self._get_active_regime_model_revision_from_postgres(name)
            logger.warning("Falling back to Postgres for active regime model lookup: %s", exc)
            return self._get_active_regime_model_revision_from_postgres(name)
        except Exception as exc:
            logger.warning("Falling back to Postgres for active regime model lookup: %s", exc)
            return self._get_active_regime_model_revision_from_postgres(name)
        return payload if isinstance(payload, dict) else None

    def list_active_regime_model_revisions(self) -> list[dict[str, Any]]:
        try:
            payload = self._request_json_with_retry("GET", "/api/internal/regimes/models/active")
        except Exception as exc:
            logger.warning("Falling back to Postgres for active regime model list: %s", exc)
            return self._list_active_regime_model_revisions_from_postgres()
        return payload if isinstance(payload, list) else []

    def get_regime_latest(self, *, model_name: str, model_version: int | None = None) -> dict[str, Any] | None:
        try:
            payload = self._request_json_with_retry(
                "GET",
                "/api/internal/regimes/current",
                params={
                    "modelName": model_name,
                    "modelVersion": model_version,
                },
            )
        except ControlPlaneRequestError as exc:
            if exc.status_code == 404:
                return self._get_regime_latest_from_postgres(model_name=model_name, model_version=model_version)
            logger.warning("Falling back to Postgres for current regime lookup: %s", exc)
            return self._get_regime_latest_from_postgres(model_name=model_name, model_version=model_version)
        except Exception as exc:
            logger.warning("Falling back to Postgres for current regime lookup: %s", exc)
            return self._get_regime_latest_from_postgres(model_name=model_name, model_version=model_version)
        return payload if isinstance(payload, dict) else None

    def save_regime_model(self, *args, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("Jobs repo does not mutate regime control-plane state directly.")

