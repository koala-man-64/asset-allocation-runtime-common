from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Optional

import httpx

from asset_allocation_runtime_common.api_gateway_auth import build_access_token_provider
from asset_allocation_runtime_common.shared_core.redaction import (
    redact_exception_cause,
    redact_secrets,
    redact_text,
)

logger = logging.getLogger(__name__)
_MIN_API_GATEWAY_TIMEOUT_SECONDS = 60.0
_DEFAULT_API_WARMUP_ENABLED = True
_DEFAULT_API_WARMUP_MAX_ATTEMPTS = 3
_DEFAULT_API_WARMUP_BASE_DELAY_SECONDS = 1.0
_DEFAULT_API_WARMUP_MAX_DELAY_SECONDS = 8.0
_DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS = 5.0
_DEFAULT_API_READINESS_ENABLED = True
_DEFAULT_API_READINESS_MAX_ATTEMPTS = 6
_DEFAULT_API_READINESS_SLEEP_SECONDS = 10.0
_DEFAULT_REQUEST_RETRY_ATTEMPTS = 3
_DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS = 1.0
_DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS = 8.0
_API_WARMUP_PROBE_PATH = "/healthz"
_RETRYABLE_WARMUP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_REQUEST_STATUS_CODES = {408, 425, 500, 502, 503, 504}


class QuiverGatewayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        detail: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(redact_text(message))
        self.status_code = status_code
        self.detail = redact_text(detail) if detail is not None else None
        self.payload = redact_secrets(payload) if payload is not None else None


class QuiverGatewayAuthError(QuiverGatewayError):
    pass


class QuiverGatewayRateLimitError(QuiverGatewayError):
    pass


class QuiverGatewayNotFoundError(QuiverGatewayError):
    pass


class QuiverGatewayProtocolError(QuiverGatewayError):
    pass


class QuiverGatewayUnavailableError(QuiverGatewayError):
    pass


class QuiverGatewayDisabledError(QuiverGatewayUnavailableError):
    pass


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be a number.") from exc


def _env_int(name: str, default: int) -> int:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return int(default)
    try:
        return int(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be an integer.") from exc


@dataclass(frozen=True)
class QuiverGatewayClientConfig:
    base_url: str
    api_scope: Optional[str]
    timeout_seconds: float
    warmup_enabled: bool = _DEFAULT_API_WARMUP_ENABLED
    warmup_max_attempts: int = _DEFAULT_API_WARMUP_MAX_ATTEMPTS
    warmup_base_delay_seconds: float = _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS
    warmup_max_delay_seconds: float = _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS
    warmup_probe_timeout_seconds: float = _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS
    readiness_enabled: bool = _DEFAULT_API_READINESS_ENABLED
    readiness_max_attempts: int = _DEFAULT_API_READINESS_MAX_ATTEMPTS
    readiness_sleep_seconds: float = _DEFAULT_API_READINESS_SLEEP_SECONDS
    request_retry_attempts: int = _DEFAULT_REQUEST_RETRY_ATTEMPTS
    request_retry_base_delay_seconds: float = _DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS
    request_retry_max_delay_seconds: float = _DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS


class QuiverGatewayClient:
    def __init__(
        self,
        config: QuiverGatewayClientConfig,
        *,
        http_client: Optional[httpx.Client] = None,
        access_token_provider: Optional[Callable[[], str]] = None,
    ) -> None:
        self.config = config
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(config.timeout_seconds), trust_env=False)
        self._access_token_provider = access_token_provider
        self._warmup_lock = threading.Lock()
        self._warmup_attempted = False
        self._warmup_succeeded = not config.warmup_enabled
        self._readiness_lock = threading.Lock()
        self._readiness_attempted = False
        self._readiness_succeeded = not config.readiness_enabled

    @staticmethod
    def from_env() -> "QuiverGatewayClient":
        base_url = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_BASE_URL"))
        if not base_url:
            raise ValueError("ASSET_ALLOCATION_API_BASE_URL is required for Quiver ETL via API gateway.")

        api_scope = _strip_or_none(os.environ.get("ASSET_ALLOCATION_API_SCOPE"))
        if not api_scope:
            raise ValueError("ASSET_ALLOCATION_API_SCOPE is required for Quiver ETL via the Asset Allocation API gateway.")

        timeout_seconds = _env_float(
            "ASSET_ALLOCATION_API_TIMEOUT_SECONDS",
            _env_float("QUIVER_TIMEOUT_SECONDS", _MIN_API_GATEWAY_TIMEOUT_SECONDS),
        )
        if timeout_seconds < _MIN_API_GATEWAY_TIMEOUT_SECONDS:
            logger.warning(
                "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=%s is too low for Quiver requests; using %s.",
                timeout_seconds,
                _MIN_API_GATEWAY_TIMEOUT_SECONDS,
            )
            timeout_seconds = _MIN_API_GATEWAY_TIMEOUT_SECONDS
        request_retry_attempts = max(
            1,
            _env_int("QUIVER_GATEWAY_RETRY_ATTEMPTS", _DEFAULT_REQUEST_RETRY_ATTEMPTS),
        )
        request_retry_base_delay_seconds = max(
            0.0,
            _env_float("QUIVER_GATEWAY_RETRY_BASE_SECONDS", _DEFAULT_REQUEST_RETRY_BASE_DELAY_SECONDS),
        )
        request_retry_max_delay_seconds = max(
            request_retry_base_delay_seconds,
            _env_float("QUIVER_GATEWAY_RETRY_MAX_SECONDS", _DEFAULT_REQUEST_RETRY_MAX_DELAY_SECONDS),
        )

        return QuiverGatewayClient(
            QuiverGatewayClientConfig(
                base_url=str(base_url).rstrip("/"),
                api_scope=api_scope,
                timeout_seconds=float(timeout_seconds),
                warmup_enabled=True,
                warmup_max_attempts=max(
                    1,
                    _env_int("ASSET_ALLOCATION_API_WARMUP_ATTEMPTS", _DEFAULT_API_WARMUP_MAX_ATTEMPTS),
                ),
                warmup_base_delay_seconds=max(
                    0.0,
                    _env_float("ASSET_ALLOCATION_API_WARMUP_BASE_SECONDS", _DEFAULT_API_WARMUP_BASE_DELAY_SECONDS),
                ),
                warmup_max_delay_seconds=max(
                    0.0,
                    _env_float("ASSET_ALLOCATION_API_WARMUP_MAX_SECONDS", _DEFAULT_API_WARMUP_MAX_DELAY_SECONDS),
                ),
                warmup_probe_timeout_seconds=max(
                    0.1,
                    _env_float(
                        "ASSET_ALLOCATION_API_WARMUP_PROBE_TIMEOUT_SECONDS",
                        _DEFAULT_API_WARMUP_PROBE_TIMEOUT_SECONDS,
                    ),
                ),
                readiness_enabled=True,
                readiness_max_attempts=max(
                    1,
                    _env_int("ASSET_ALLOCATION_API_READINESS_ATTEMPTS", _DEFAULT_API_READINESS_MAX_ATTEMPTS),
                ),
                readiness_sleep_seconds=max(
                    0.0,
                    _env_float("ASSET_ALLOCATION_API_READINESS_SLEEP_SECONDS", _DEFAULT_API_READINESS_SLEEP_SECONDS),
                ),
                request_retry_attempts=request_retry_attempts,
                request_retry_base_delay_seconds=request_retry_base_delay_seconds,
                request_retry_max_delay_seconds=request_retry_max_delay_seconds,
            )
        )

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "QuiverGatewayClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _build_headers(self) -> dict[str, str]:
        if self._access_token_provider is None:
            if not self.config.api_scope:
                raise QuiverGatewayAuthError(
                    "ASSET_ALLOCATION_API_SCOPE is required for bearer-token access to the Asset Allocation API gateway."
                )
            self._access_token_provider = build_access_token_provider(self.config.api_scope)

        try:
            token = self._access_token_provider()
        except Exception as exc:
            raise QuiverGatewayAuthError(
                "Failed to acquire bearer token for the Asset Allocation API gateway."
            ) from redact_exception_cause(exc)

        headers = {"Authorization": f"Bearer {token}"}
        caller_job = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_NAME"))
        caller_execution = _strip_or_none(os.environ.get("CONTAINER_APP_JOB_EXECUTION_NAME"))
        if caller_job:
            headers["X-Caller-Job"] = str(caller_job)
        if caller_execution:
            headers["X-Caller-Execution"] = str(caller_execution)
        return headers

    def _extract_detail(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except Exception:
            text = (response.text or "").strip()
            return redact_text(text or response.reason_phrase)
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str) and detail.strip():
                return redact_text(detail.strip())
            return redact_text(json.dumps(redact_secrets(payload), ensure_ascii=False))
        if isinstance(payload, list):
            return f"Unexpected list response ({len(payload)} items)."
        if isinstance(payload, str) and payload.strip():
            return redact_text(payload.strip())
        return redact_text(response.reason_phrase)

    def _warmup_probe_url(self) -> str:
        return f"{self.config.base_url}{_API_WARMUP_PROBE_PATH}"

    def _warm_up_gateway(self) -> bool:
        if not self.config.warmup_enabled:
            return True
        if self._warmup_attempted:
            return self._warmup_succeeded

        with self._warmup_lock:
            if self._warmup_attempted:
                return self._warmup_succeeded

            attempts = max(1, int(self.config.warmup_max_attempts))
            delay_seconds = max(0.0, float(self.config.warmup_base_delay_seconds))
            max_delay_seconds = max(delay_seconds, float(self.config.warmup_max_delay_seconds))
            timeout = httpx.Timeout(min(self.config.timeout_seconds, self.config.warmup_probe_timeout_seconds))
            succeeded = False
            try:
                for attempt in range(1, attempts + 1):
                    try:
                        response = self._http.get(self._warmup_probe_url(), headers=self._build_headers(), timeout=timeout)
                        if response.status_code < 400:
                            succeeded = True
                            return True
                        if response.status_code not in _RETRYABLE_WARMUP_STATUS_CODES or attempt >= attempts:
                            return False
                    except Exception:
                        if attempt >= attempts:
                            return False
                    if delay_seconds > 0.0:
                        time.sleep(delay_seconds)
                    delay_seconds = min(max_delay_seconds, max(0.1, delay_seconds * 2.0))
                return succeeded
            finally:
                self._warmup_attempted = True
                self._warmup_succeeded = succeeded

    def warm_up_gateway(self, *, force: bool = False) -> bool:
        if force:
            with self._warmup_lock:
                self._warmup_attempted = False
                self._warmup_succeeded = not self.config.warmup_enabled
        return self._warm_up_gateway()

    def _ensure_gateway_ready(self) -> bool:
        if not self.config.readiness_enabled:
            return self._warm_up_gateway()
        if self._readiness_attempted:
            return self._readiness_succeeded

        with self._readiness_lock:
            if self._readiness_attempted:
                return self._readiness_succeeded

            attempts = max(1, int(self.config.readiness_max_attempts))
            pause = max(0.0, float(self.config.readiness_sleep_seconds))
            ready = False
            for attempt in range(1, attempts + 1):
                ready = self.warm_up_gateway(force=attempt > 1)
                if ready:
                    break
                if attempt < attempts and pause > 0.0:
                    time.sleep(pause)

            self._readiness_attempted = True
            self._readiness_succeeded = ready
            return ready

    def _retry_request_delay(self, delay_seconds: float) -> float:
        current = max(0.0, float(delay_seconds))
        if current <= 0.0:
            return 0.0
        return min(float(self.config.request_retry_max_delay_seconds), max(current * 2.0, 1.0))

    def _retry_after_delay_seconds(self, response: httpx.Response) -> Optional[float]:
        raw = _strip_or_none(response.headers.get("Retry-After"))
        if raw is None:
            return None
        try:
            return max(0.0, float(raw))
        except Exception:
            pass
        try:
            parsed = parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return max(0.0, parsed.timestamp() - time.time())
        except Exception:
            return None

    def _retry_sleep_seconds(self, delay_seconds: float, *, response: Optional[httpx.Response] = None) -> float:
        max_delay = max(0.0, float(self.config.request_retry_max_delay_seconds))
        retry_after = self._retry_after_delay_seconds(response) if response is not None else None
        if retry_after is not None:
            return min(max_delay, retry_after)

        current = min(max_delay, max(0.0, float(delay_seconds)))
        if current <= 0.0:
            return 0.0
        jitter = random.uniform(0.0, min(current * 0.25, max_delay))
        return min(max_delay, current + jitter)

    def _reset_gateway_state(self) -> None:
        with self._warmup_lock:
            self._warmup_attempted = False
            self._warmup_succeeded = not self.config.warmup_enabled
        with self._readiness_lock:
            self._readiness_attempted = False
            self._readiness_succeeded = not self.config.readiness_enabled

    def _is_disabled_detail(self, detail: str) -> bool:
        lowered = detail.lower()
        return "disabled" in lowered and ("provider" in lowered or "quiver" in lowered or "gateway" in lowered)

    def _request_json(self, path: str, *, params: Optional[dict[str, Any]] = None) -> Any:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        request_params = {k: v for k, v in dict(params or {}).items() if v is not None}
        attempts = max(1, int(self.config.request_retry_attempts))
        delay_seconds = max(0.0, float(self.config.request_retry_base_delay_seconds))

        for attempt in range(1, attempts + 1):
            if not self._ensure_gateway_ready():
                raise QuiverGatewayUnavailableError(
                    "Asset Allocation API gateway for Quiver is not ready.",
                    detail="Warm-up/readiness probe failed.",
                    payload={"path": path, "probe_path": _API_WARMUP_PROBE_PATH},
                )

            try:
                response = self._http.get(url, headers=self._build_headers(), params=request_params)
            except httpx.TimeoutException as exc:
                if attempt < attempts:
                    sleep_seconds = self._retry_sleep_seconds(delay_seconds)
                    logger.warning(
                        "Quiver gateway request timed out (path=%s, attempt=%s/%s, sleep=%.1fs).",
                        path,
                        attempt,
                        attempts,
                        sleep_seconds,
                    )
                    if sleep_seconds > 0.0:
                        time.sleep(sleep_seconds)
                    delay_seconds = self._retry_request_delay(delay_seconds)
                    self._reset_gateway_state()
                    continue
                raise QuiverGatewayUnavailableError(
                    "Timed out calling the Asset Allocation Quiver gateway.",
                    status_code=504,
                    detail="Gateway request timed out.",
                    payload={"path": path},
                ) from redact_exception_cause(exc)
            except httpx.TransportError as exc:
                if attempt < attempts:
                    sleep_seconds = self._retry_sleep_seconds(delay_seconds)
                    logger.warning(
                        "Quiver gateway transport error retrying (path=%s, attempt=%s/%s, sleep=%.1fs, error=%s).",
                        path,
                        attempt,
                        attempts,
                        sleep_seconds,
                        redact_text(exc),
                    )
                    if sleep_seconds > 0.0:
                        time.sleep(sleep_seconds)
                    delay_seconds = self._retry_request_delay(delay_seconds)
                    self._reset_gateway_state()
                    continue
                raise QuiverGatewayUnavailableError(
                    f"Asset Allocation Quiver gateway transport failure: {type(exc).__name__}: {exc}",
                    payload={"path": path},
                ) from redact_exception_cause(exc)
            except Exception as exc:
                raise QuiverGatewayUnavailableError(
                    f"Asset Allocation Quiver gateway request failed: {type(exc).__name__}: {exc}",
                    payload={"path": path},
                ) from redact_exception_cause(exc)

            if response.status_code < 400:
                try:
                    return response.json()
                except Exception as exc:
                    raise QuiverGatewayProtocolError(
                        "Asset Allocation Quiver gateway returned non-JSON content.",
                        status_code=response.status_code,
                        detail=redact_text((response.text or "").strip()[:240]) or None,
                        payload={"path": path},
                    ) from exc

            detail = self._extract_detail(response)
            payload = {"path": path, "status_code": int(response.status_code), "detail": detail}
            if self._is_disabled_detail(detail):
                raise QuiverGatewayDisabledError(
                    detail or "Quiver provider is disabled.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            if response.status_code in _RETRYABLE_REQUEST_STATUS_CODES and attempt < attempts:
                sleep_seconds = self._retry_sleep_seconds(delay_seconds, response=response)
                logger.warning(
                    "Quiver gateway request retrying after status=%s (path=%s, attempt=%s/%s, sleep=%.1fs, detail=%s).",
                    response.status_code,
                    path,
                    attempt,
                    attempts,
                    sleep_seconds,
                    detail[:220],
                )
                if sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)
                delay_seconds = self._retry_request_delay(delay_seconds)
                self._reset_gateway_state()
                continue

            if response.status_code in {401, 403}:
                raise QuiverGatewayAuthError(
                    detail or "Unauthorized.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            if response.status_code == 404:
                raise QuiverGatewayNotFoundError(
                    detail or "Not found.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            if response.status_code == 429:
                raise QuiverGatewayRateLimitError(
                    detail or "Rate limited.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            if response.status_code in _RETRYABLE_REQUEST_STATUS_CODES:
                raise QuiverGatewayUnavailableError(
                    detail or "Asset Allocation Quiver gateway request failed.",
                    status_code=response.status_code,
                    detail=detail,
                    payload=payload,
                )
            raise QuiverGatewayError(
                detail or f"Asset Allocation Quiver gateway returned status {response.status_code}.",
                status_code=response.status_code,
                detail=detail,
                payload=payload,
            )

        raise QuiverGatewayUnavailableError(
            f"Asset Allocation Quiver gateway retry budget exhausted calling {path}",
            payload={"path": path},
        )

    def get_live_congress_trading(self, *, normalized: bool | None = None, representative: str | None = None) -> Any:
        return self._request_json("/api/providers/quiver/live/congress-trading", params={"normalized": normalized, "representative": representative})

    def get_historical_congress_trading(self, *, ticker: str, analyst: str | None = None) -> Any:
        return self._request_json(f"/api/providers/quiver/historical/congress-trading/{ticker}", params={"analyst": analyst})

    def get_live_senate_trading(self, *, name: str | None = None, options: bool | None = None) -> Any:
        return self._request_json("/api/providers/quiver/live/senate-trading", params={"name": name, "options": str(options).lower() if options is not None else None})

    def get_historical_senate_trading(self, *, ticker: str) -> Any:
        return self._request_json(f"/api/providers/quiver/historical/senate-trading/{ticker}")

    def get_live_house_trading(self, *, name: str | None = None, options: bool | None = None) -> Any:
        return self._request_json("/api/providers/quiver/live/house-trading", params={"name": name, "options": str(options).lower() if options is not None else None})

    def get_historical_house_trading(self, *, ticker: str) -> Any:
        return self._request_json(f"/api/providers/quiver/historical/house-trading/{ticker}")

    def get_live_gov_contracts(self) -> Any:
        return self._request_json("/api/providers/quiver/live/gov-contracts")

    def get_historical_gov_contracts(self, *, ticker: str) -> Any:
        return self._request_json(f"/api/providers/quiver/historical/gov-contracts/{ticker}")

    def get_live_gov_contracts_all(self, *, date: str | None = None, page: int | None = None, page_size: int | None = None) -> Any:
        return self._request_json("/api/providers/quiver/live/gov-contracts-all", params={"date": date, "page": page, "page_size": page_size})

    def get_historical_gov_contracts_all(self, *, ticker: str) -> Any:
        return self._request_json(f"/api/providers/quiver/historical/gov-contracts-all/{ticker}")

    def get_live_insiders(
        self,
        *,
        ticker: str | None = None,
        date: str | None = None,
        uploaded: str | None = None,
        limit_codes: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._request_json(
            "/api/providers/quiver/live/insiders",
            params={
                "ticker": ticker,
                "date": date,
                "uploaded": uploaded,
                "limit_codes": str(limit_codes).lower() if limit_codes is not None else None,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_sec13f(
        self,
        *,
        ticker: str | None = None,
        owner: str | None = None,
        date: str | None = None,
        period: str | None = None,
        today: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._request_json(
            "/api/providers/quiver/live/sec13f",
            params={
                "ticker": ticker,
                "owner": owner,
                "date": date,
                "period": period,
                "today": str(today).lower() if today is not None else None,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_sec13f_changes(
        self,
        *,
        ticker: str | None = None,
        owner: str | None = None,
        date: str | None = None,
        period: str | None = None,
        today: bool | None = None,
        most_recent: bool | None = None,
        show_new_funds: bool | None = None,
        mobile: bool | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._request_json(
            "/api/providers/quiver/live/sec13f-changes",
            params={
                "ticker": ticker,
                "owner": owner,
                "date": date,
                "period": period,
                "today": str(today).lower() if today is not None else None,
                "most_recent": str(most_recent).lower() if most_recent is not None else None,
                "show_new_funds": str(show_new_funds).lower() if show_new_funds is not None else None,
                "mobile": str(mobile).lower() if mobile is not None else None,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_live_lobbying(
        self,
        *,
        all_records: bool | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int | None = None,
        page_size: int | None = None,
    ) -> Any:
        return self._request_json(
            "/api/providers/quiver/live/lobbying",
            params={
                "all": str(all_records).lower() if all_records is not None else None,
                "date_from": date_from,
                "date_to": date_to,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_historical_lobbying(
        self,
        *,
        ticker: str,
        page: int | None = None,
        page_size: int | None = None,
        query: str | None = None,
        query_ticker: str | None = None,
    ) -> Any:
        return self._request_json(
            f"/api/providers/quiver/historical/lobbying/{ticker}",
            params={"page": page, "page_size": page_size, "query": query, "queryTicker": query_ticker},
        )

    def get_live_etf_holdings(self, *, etf: str | None = None, ticker: str | None = None) -> Any:
        return self._request_json("/api/providers/quiver/live/etf-holdings", params={"etf": etf, "ticker": ticker})

    def get_live_congress_holdings(self) -> Any:
        return self._request_json("/api/providers/quiver/live/congress-holdings")
