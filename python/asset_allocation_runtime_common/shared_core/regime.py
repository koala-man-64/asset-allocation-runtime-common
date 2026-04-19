from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from asset_allocation_contracts import regime as contracts_regime

DEFAULT_HALT_REASON = contracts_regime.DEFAULT_HALT_REASON
DEFAULT_REGIME_MODEL_NAME = contracts_regime.DEFAULT_REGIME_MODEL_NAME
CurveState = contracts_regime.CurveState
RegimeBlockedAction = contracts_regime.RegimeBlockedAction
RegimeCode = contracts_regime.RegimeCode
RegimeModelConfig = contracts_regime.RegimeModelConfig
RegimePolicy = contracts_regime.RegimePolicy
RegimeStatus = contracts_regime.RegimeStatus
TargetGrossExposureByRegime = contracts_regime.TargetGrossExposureByRegime
TrendState = contracts_regime.TrendState
CANONICAL_DEFAULT_REGIME_VERSION = getattr(contracts_regime, "CANONICAL_DEFAULT_REGIME_VERSION", 2)

_contracts_canonical_default_regime_config_errors = getattr(
    contracts_regime,
    "canonical_default_regime_config_errors",
    None,
)
_contracts_canonical_default_regime_model_config = getattr(
    contracts_regime,
    "canonical_default_regime_model_config",
    None,
)
_contracts_default_regime_model_config = contracts_regime.default_regime_model_config

__all__ = [
    "DEFAULT_HALT_REASON",
    "DEFAULT_REGIME_MODEL_NAME",
    "CurveState",
    "RegimeBlockedAction",
    "RegimeCode",
    "CANONICAL_DEFAULT_REGIME_VERSION",
    "RegimeModelConfig",
    "RegimePolicy",
    "RegimeStatus",
    "TargetGrossExposureByRegime",
    "TrendState",
    "build_regime_outputs",
    "classify_regime_row",
    "compute_curve_state",
    "compute_trend_state",
    "canonical_default_regime_config_errors",
    "canonical_default_regime_model_config",
    "default_regime_model_config",
    "next_business_session",
]


def canonical_default_regime_model_config() -> dict[str, Any]:
    if callable(_contracts_canonical_default_regime_model_config):
        raw = _contracts_canonical_default_regime_model_config()
    else:
        raw = _contracts_default_regime_model_config()
        raw = {
            **raw,
            "highVolExitThreshold": raw.get("highVolEnterThreshold", 28.0),
        }
    return RegimeModelConfig.model_validate(raw).model_dump(mode="json")


def canonical_default_regime_config_errors(
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
) -> list[str]:
    if callable(_contracts_canonical_default_regime_config_errors):
        return list(_contracts_canonical_default_regime_config_errors(config))

    expected = canonical_default_regime_model_config()
    actual = _resolve_regime_config(config).model_dump(mode="json")
    return [
        f"{key} must be {expected[key]!r} (got {actual[key]!r})"
        for key in expected
        if actual.get(key) != expected.get(key)
    ]


def default_regime_model_config() -> dict[str, Any]:
    return canonical_default_regime_model_config()


def _resolve_regime_config(config: RegimeModelConfig | Mapping[str, Any] | None = None) -> RegimeModelConfig:
    if isinstance(config, RegimeModelConfig):
        return config

    merged = canonical_default_regime_model_config()
    if config:
        merged.update(dict(config))
    return RegimeModelConfig.model_validate(merged)


def _calculate_easter_sunday(year: int) -> date:
    # Anonymous Gregorian algorithm.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    current += timedelta(days=7 * (occurrence - 1))
    return current


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _is_us_equity_market_holiday(value: date) -> bool:
    year = value.year
    holidays = {
        _observed_fixed_holiday(year, 1, 1),
        _nth_weekday_of_month(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday_of_month(year, 2, 0, 3),  # Presidents Day
        _calculate_easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday_of_month(year, 5, 0),  # Memorial Day
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday_of_month(year, 9, 0, 1),  # Labor Day
        _nth_weekday_of_month(year, 11, 3, 4),  # Thanksgiving
        _observed_fixed_holiday(year, 12, 25),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(year, 6, 19))
    return value in holidays


def next_business_session(as_of_date: date) -> date:
    current = as_of_date + timedelta(days=1)
    while current.weekday() >= 5 or _is_us_equity_market_holiday(current):
        current += timedelta(days=1)
    return current


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        out = int(value)
    except (TypeError, ValueError):
        return None
    return out


def compute_trend_state(return_20d: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> TrendState:
    cfg = _resolve_regime_config(config)
    value = _safe_float(return_20d)
    if value is None:
        return "near_zero"
    if value > cfg.trendPositiveThreshold:
        return "positive"
    if value < cfg.trendNegativeThreshold:
        return "negative"
    return "near_zero"


def compute_curve_state(vix_slope: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> CurveState:
    cfg = _resolve_regime_config(config)
    value = _safe_float(vix_slope)
    if value is None:
        return "flat"
    if value >= cfg.curveContangoThreshold:
        return "contango"
    if value <= cfg.curveInvertedThreshold:
        return "inverted"
    return "flat"


def _uses_high_vol_transition_band(config: RegimeModelConfig) -> bool:
    return float(config.highVolExitThreshold) < float(config.highVolEnterThreshold)


def classify_regime_row(
    row: Mapping[str, Any],
    *,
    prev_confirmed_regime: RegimeCode | None = None,
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _resolve_regime_config(config)
    inputs_complete = bool(row.get("inputs_complete_flag"))
    if not inputs_complete:
        return {
            "regime_code": "unclassified",
            "regime_status": "unclassified",
            "matched_rule_id": None,
            "halt_flag": False,
            "halt_reason": None,
            "trend_state": compute_trend_state(row.get("return_20d"), config=cfg),
            "curve_state": compute_curve_state(row.get("vix_slope"), config=cfg),
        }

    trend_state = compute_trend_state(row.get("return_20d"), config=cfg)
    curve_state = compute_curve_state(row.get("vix_slope"), config=cfg)
    rvol_10d_ann = _safe_float(row.get("rvol_10d_ann"))
    vix_spot_close = _safe_float(row.get("vix_spot_close"))
    vix_gt_32_streak = _safe_int(row.get("vix_gt_32_streak")) or 0

    halt_flag = bool(
        vix_spot_close is not None
        and vix_spot_close > cfg.haltVixThreshold
        and vix_gt_32_streak >= cfg.haltVixStreakDays
    )

    regime_code: RegimeCode
    regime_status: RegimeStatus
    matched_rule_id: str | None

    if rvol_10d_ann is not None and rvol_10d_ann > cfg.highVolEnterThreshold and curve_state == "inverted":
        regime_code = "high_vol"
        regime_status = "confirmed"
        matched_rule_id = "high_vol"
    elif (
        rvol_10d_ann is not None
        and _uses_high_vol_transition_band(cfg)
        and cfg.highVolExitThreshold <= rvol_10d_ann < cfg.highVolEnterThreshold
    ):
        regime_code = prev_confirmed_regime or "unclassified"
        regime_status = "transition" if prev_confirmed_regime else "unclassified"
        matched_rule_id = "transition_band"
    elif (
        rvol_10d_ann is not None
        and cfg.bearVolMin <= rvol_10d_ann < cfg.bearVolMaxExclusive
        and trend_state == "negative"
        and curve_state in {"flat", "inverted"}
    ):
        regime_code = "trending_bear"
        regime_status = "confirmed"
        matched_rule_id = "trending_bear"
    elif (
        rvol_10d_ann is not None
        and rvol_10d_ann < cfg.bullVolMaxExclusive
        and trend_state == "positive"
        and curve_state == "contango"
    ):
        regime_code = "trending_bull"
        regime_status = "confirmed"
        matched_rule_id = "trending_bull"
    elif (
        rvol_10d_ann is not None
        and cfg.choppyVolMin <= rvol_10d_ann < cfg.choppyVolMaxExclusive
        and trend_state == "near_zero"
        and curve_state == "contango"
    ):
        regime_code = "choppy_mean_reversion"
        regime_status = "confirmed"
        matched_rule_id = "choppy_mean_reversion"
    else:
        regime_code = "unclassified"
        regime_status = "unclassified"
        matched_rule_id = None

    return {
        "regime_code": regime_code,
        "regime_status": regime_status,
        "matched_rule_id": matched_rule_id,
        "halt_flag": halt_flag,
        "halt_reason": DEFAULT_HALT_REASON if halt_flag else None,
        "trend_state": trend_state,
        "curve_state": curve_state,
    }


def build_regime_outputs(
    inputs: pd.DataFrame,
    *,
    model_name: str,
    model_version: int,
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
    computed_at: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cfg = _resolve_regime_config(config)
    if inputs.empty:
        empty_history = pd.DataFrame(
            columns=[
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
            ]
        )
        empty_latest = pd.DataFrame(columns=empty_history.columns)
        empty_transitions = pd.DataFrame(
            columns=[
                "model_name",
                "model_version",
                "effective_from_date",
                "prior_regime_code",
                "new_regime_code",
                "trigger_rule_id",
                "computed_at",
            ]
        )
        return empty_history, empty_latest, empty_transitions

    computed = pd.Timestamp(computed_at or datetime.now(timezone.utc))
    frame = inputs.copy()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["as_of_date"]).sort_values("as_of_date").reset_index(drop=True)

    frame["effective_from_date"] = [next_business_session(as_of_date) for as_of_date in frame["as_of_date"].tolist()]

    history_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    prev_confirmed_regime: RegimeCode | None = None
    last_recorded_confirmed: RegimeCode | None = None

    for row in frame.to_dict("records"):
        classification = classify_regime_row(
            row,
            prev_confirmed_regime=prev_confirmed_regime,
            config=cfg,
        )
        regime_code = classification["regime_code"]
        regime_status = classification["regime_status"]
        matched_rule_id = classification["matched_rule_id"]

        history_rows.append(
            {
                "as_of_date": row["as_of_date"],
                "effective_from_date": row["effective_from_date"],
                "model_name": model_name,
                "model_version": int(model_version),
                "regime_code": regime_code,
                "regime_status": regime_status,
                "matched_rule_id": matched_rule_id,
                "halt_flag": bool(classification["halt_flag"]),
                "halt_reason": classification["halt_reason"],
                "spy_return_20d": _safe_float(row.get("return_20d")),
                "rvol_10d_ann": _safe_float(row.get("rvol_10d_ann")),
                "vix_spot_close": _safe_float(row.get("vix_spot_close")),
                "vix3m_close": _safe_float(row.get("vix3m_close")),
                "vix_slope": _safe_float(row.get("vix_slope")),
                "trend_state": classification["trend_state"],
                "curve_state": classification["curve_state"],
                "vix_gt_32_streak": _safe_int(row.get("vix_gt_32_streak")),
                "computed_at": computed,
            }
        )

        if regime_status == "confirmed":
            prev_confirmed_regime = regime_code
            if last_recorded_confirmed != regime_code:
                transition_rows.append(
                    {
                        "model_name": model_name,
                        "model_version": int(model_version),
                        "effective_from_date": row["effective_from_date"],
                        "prior_regime_code": last_recorded_confirmed,
                        "new_regime_code": regime_code,
                        "trigger_rule_id": matched_rule_id,
                        "computed_at": computed,
                    }
                )
                last_recorded_confirmed = regime_code

    history = pd.DataFrame(history_rows)
    latest = history.tail(1).reset_index(drop=True)
    transitions = pd.DataFrame(transition_rows)
    return history, latest, transitions
