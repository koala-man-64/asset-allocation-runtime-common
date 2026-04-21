from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from asset_allocation_contracts import regime as contracts_regime

DEFAULT_HALT_REASON = contracts_regime.DEFAULT_HALT_REASON
DEFAULT_REGIME_MODEL_NAME = contracts_regime.DEFAULT_REGIME_MODEL_NAME
CurveState = contracts_regime.CurveState
RegimeCode = contracts_regime.RegimeCode
RegimeModelConfig = contracts_regime.RegimeModelConfig
RegimePolicy = contracts_regime.RegimePolicy
RegimeSignal = contracts_regime.RegimeSignal
RegimeSignalConfig = contracts_regime.RegimeSignalConfig
RegimeSignalState = contracts_regime.RegimeSignalState
RegimeTransitionType = contracts_regime.RegimeTransitionType
TrendState = contracts_regime.TrendState
CANONICAL_DEFAULT_REGIME_VERSION = getattr(contracts_regime, "CANONICAL_DEFAULT_REGIME_VERSION", 3)

_TREND_POSITIVE_THRESHOLD = 0.02
_TREND_NEGATIVE_THRESHOLD = -0.02
_CURVE_CONTANGO_THRESHOLD = 0.50
_CURVE_INVERTED_THRESHOLD = -0.50
_REGIME_ORDER: tuple[RegimeCode, ...] = tuple(contracts_regime.canonical_default_regime_signal_configs().keys())

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
    "RegimeCode",
    "CANONICAL_DEFAULT_REGIME_VERSION",
    "RegimeModelConfig",
    "RegimePolicy",
    "RegimeSignal",
    "RegimeSignalConfig",
    "RegimeSignalState",
    "RegimeTransitionType",
    "TrendState",
    "build_regime_outputs",
    "canonical_default_regime_config_errors",
    "canonical_default_regime_model_config",
    "classify_regime_row",
    "compute_curve_state",
    "compute_trend_state",
    "default_regime_model_config",
    "next_business_session",
]


def canonical_default_regime_model_config() -> dict[str, Any]:
    if callable(_contracts_canonical_default_regime_model_config):
        raw = _contracts_canonical_default_regime_model_config()
    else:
        raw = _contracts_default_regime_model_config()
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
        _nth_weekday_of_month(year, 1, 0, 3),
        _nth_weekday_of_month(year, 2, 0, 3),
        _calculate_easter_sunday(year) - timedelta(days=2),
        _last_weekday_of_month(year, 5, 0),
        _observed_fixed_holiday(year, 7, 4),
        _nth_weekday_of_month(year, 9, 0, 1),
        _nth_weekday_of_month(year, 11, 3, 4),
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


def _safe_bool(value: Any) -> bool | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


def compute_trend_state(return_20d: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> TrendState:
    _ = config
    value = _safe_float(return_20d)
    if value is None:
        return "near_zero"
    if value > _TREND_POSITIVE_THRESHOLD:
        return "positive"
    if value < _TREND_NEGATIVE_THRESHOLD:
        return "negative"
    return "near_zero"


def compute_curve_state(vix_slope: Any, *, config: RegimeModelConfig | Mapping[str, Any] | None = None) -> CurveState:
    _ = config
    value = _safe_float(vix_slope)
    if value is None:
        return "flat"
    if value >= _CURVE_CONTANGO_THRESHOLD:
        return "contango"
    if value <= _CURVE_INVERTED_THRESHOLD:
        return "inverted"
    return "flat"


def _same_sign(left: float | None, right: float | None) -> bool | None:
    if left is None or right is None:
        return None
    if left == 0.0 or right == 0.0:
        return False
    return (left > 0.0 and right > 0.0) or (left < 0.0 and right < 0.0)


def _build_metric_view(row: Mapping[str, Any]) -> dict[str, Any]:
    metrics = dict(row)

    spy_close = _safe_float(row.get("spy_close"))
    qqq_close = _safe_float(row.get("qqq_close"))
    spy_sma_200d = _safe_float(row.get("spy_sma_200d") or row.get("sma_200d"))
    qqq_sma_200d = _safe_float(row.get("qqq_sma_200d"))
    spy_return_20d = _safe_float(row.get("return_20d") or row.get("spy_return_20d"))
    qqq_return_20d = _safe_float(row.get("qqq_return_20d"))
    acwi_return_20d = _safe_float(row.get("acwi_return_20d"))
    atr_14d = _safe_float(row.get("atr_14d"))
    vix_spot_close = _safe_float(row.get("vix_spot_close"))
    hy_oas_z_20d = _safe_float(row.get("hy_oas_z_20d"))
    rates_event_flag = _safe_bool(row.get("rates_event_flag"))

    metrics["spy_return_20d"] = spy_return_20d
    metrics["qqq_return_20d"] = qqq_return_20d
    metrics["acwi_return_20d"] = acwi_return_20d
    metrics["abs_spy_return_20d"] = abs(spy_return_20d) if spy_return_20d is not None else None
    metrics["atr_14d_pct_of_close"] = (
        float(atr_14d / spy_close)
        if atr_14d is not None and spy_close is not None and spy_close != 0.0
        else None
    )
    metrics["spy_above_sma_200"] = (
        bool(spy_close > spy_sma_200d)
        if spy_close is not None and spy_sma_200d is not None
        else None
    )
    metrics["spy_below_sma_200"] = (
        bool(spy_close < spy_sma_200d)
        if spy_close is not None and spy_sma_200d is not None
        else None
    )
    metrics["qqq_above_sma_200"] = (
        bool(qqq_close > qqq_sma_200d)
        if qqq_close is not None and qqq_sma_200d is not None
        else None
    )
    metrics["qqq_below_sma_200"] = (
        bool(qqq_close < qqq_sma_200d)
        if qqq_close is not None and qqq_sma_200d is not None
        else None
    )
    metrics["global_equity_alignment"] = _same_sign(spy_return_20d, acwi_return_20d)
    metrics["rates_event_flag"] = rates_event_flag
    metrics["cross_asset_stress_alignment"] = (
        bool(vix_spot_close >= 20.0 and hy_oas_z_20d >= 1.0)
        if vix_spot_close is not None and hy_oas_z_20d is not None
        else None
    )
    return metrics


def _metric_is_missing(metric_value: Any) -> bool:
    if metric_value is None:
        return True
    if isinstance(metric_value, float) and pd.isna(metric_value):
        return True
    return False


def _evaluate_rule(metric_value: Any, comparison: str, *, lower: float | None, upper: float | None) -> float | None:
    if _metric_is_missing(metric_value):
        return None

    if comparison == "bool_true":
        normalized = _safe_bool(metric_value)
        return 1.0 if normalized else 0.0

    value = _safe_float(metric_value)
    if value is None:
        return None
    if comparison == "gte":
        return 1.0 if value >= float(lower or 0.0) else 0.0
    if comparison == "lte":
        return 1.0 if value <= float(lower or 0.0) else 0.0
    if comparison == "between":
        return 1.0 if float(lower or 0.0) <= value <= float(upper or 0.0) else 0.0
    raise ValueError(f"Unsupported regime metric comparison '{comparison}'.")


def _signal_from_config(
    regime_code: RegimeCode,
    *,
    signal_config: RegimeSignalConfig,
    metrics: Mapping[str, Any],
    activation_threshold: float,
) -> dict[str, Any]:
    missing_required = [metric for metric in signal_config.requiredMetrics if _metric_is_missing(metrics.get(metric))]
    evidence: dict[str, Any] = {}

    if missing_required:
        evidence["missing_metrics"] = missing_required
        return {
            "regime_code": regime_code,
            "display_name": signal_config.displayName,
            "signal_state": "insufficient_data",
            "score": 0.0,
            "activation_threshold": activation_threshold,
            "is_active": False,
            "matched_rule_id": None,
            "evidence": evidence,
        }

    rule_scores: list[float] = []
    matched_rules: list[str] = []
    for rule in signal_config.rules:
        metric_value = metrics.get(rule.metric)
        evidence[rule.metric] = metric_value
        rule_score = _evaluate_rule(
            metric_value,
            rule.comparison,
            lower=rule.lower,
            upper=rule.upper,
        )
        if rule_score is None:
            evidence.setdefault("missing_metrics", []).append(rule.metric)
            return {
                "regime_code": regime_code,
                "display_name": signal_config.displayName,
                "signal_state": "insufficient_data",
                "score": 0.0,
                "activation_threshold": activation_threshold,
                "is_active": False,
                "matched_rule_id": None,
                "evidence": evidence,
            }
        evidence[f"{rule.metric}__score"] = rule_score
        rule_scores.append(float(rule_score))
        if rule_score >= 1.0:
            matched_rules.append(rule.metric)

    score = float(sum(rule_scores) / len(rule_scores)) if rule_scores else 0.0
    is_active = score >= float(activation_threshold)
    signal_state: RegimeSignalState = "active" if is_active else "inactive"
    if matched_rules:
        evidence["matched_metrics"] = matched_rules
    return {
        "regime_code": regime_code,
        "display_name": signal_config.displayName,
        "signal_state": signal_state,
        "score": score,
        "activation_threshold": float(activation_threshold),
        "is_active": is_active,
        "matched_rule_id": regime_code if is_active else None,
        "evidence": evidence,
    }


def classify_regime_row(
    row: Mapping[str, Any],
    *,
    prev_confirmed_regime: RegimeCode | None = None,
    config: RegimeModelConfig | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _ = prev_confirmed_regime
    cfg = _resolve_regime_config(config)
    metrics = _build_metric_view(row)
    trend_state = compute_trend_state(metrics.get("spy_return_20d"), config=cfg)
    curve_state = compute_curve_state(row.get("vix_slope"), config=cfg)
    inputs_complete = bool(row.get("inputs_complete_flag"))
    vix_spot_close = _safe_float(row.get("vix_spot_close"))
    vix_gt_32_streak = _safe_int(row.get("vix_gt_32_streak")) or 0

    halt_flag = bool(
        vix_spot_close is not None
        and vix_spot_close > float(cfg.haltVixThreshold)
        and vix_gt_32_streak >= int(cfg.haltVixStreakDays)
    )

    signals: list[dict[str, Any]] = []
    active_regimes: list[RegimeCode] = []
    for regime_code in _REGIME_ORDER:
        if regime_code == "unclassified":
            continue
        signal = _signal_from_config(
            regime_code,
            signal_config=cfg.signalConfigs[regime_code],
            metrics=metrics,
            activation_threshold=float(cfg.activationThreshold),
        )
        if signal["is_active"]:
            active_regimes.append(regime_code)
        signals.append(signal)

    unclassified_active = (not inputs_complete) or len(active_regimes) == 0
    unclassified_state: RegimeSignalState = "active" if unclassified_active else "inactive"
    if unclassified_active:
        active_regimes = ["unclassified"] if not active_regimes else active_regimes

    unclassified_evidence = {
        "inputs_complete_flag": inputs_complete,
        "active_regime_count": len([regime for regime in active_regimes if regime != "unclassified"]),
    }
    if not inputs_complete:
        unclassified_evidence["reason"] = "insufficient_data"
    elif len([regime for regime in active_regimes if regime != "unclassified"]) == 0:
        unclassified_evidence["reason"] = "no_active_regimes"

    signals.append(
        {
            "regime_code": "unclassified",
            "display_name": cfg.signalConfigs["unclassified"].displayName,
            "signal_state": unclassified_state,
            "score": 1.0 if unclassified_active else 0.0,
            "activation_threshold": float(cfg.activationThreshold),
            "is_active": unclassified_active,
            "matched_rule_id": "unclassified" if unclassified_active else None,
            "evidence": unclassified_evidence,
        }
    )
    if unclassified_active and "unclassified" not in active_regimes:
        active_regimes.append("unclassified")

    if inputs_complete:
        active_regimes = [regime for regime in active_regimes if regime != "unclassified"] or ["unclassified"]

    return {
        "signals": signals,
        "active_regimes": active_regimes,
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
    history_columns = [
        "as_of_date",
        "effective_from_date",
        "model_name",
        "model_version",
        "regime_code",
        "display_name",
        "signal_state",
        "score",
        "activation_threshold",
        "is_active",
        "matched_rule_id",
        "halt_flag",
        "halt_reason",
        "evidence_json",
        "computed_at",
    ]
    transition_columns = [
        "model_name",
        "model_version",
        "effective_from_date",
        "regime_code",
        "transition_type",
        "prior_score",
        "new_score",
        "activation_threshold",
        "trigger_rule_id",
        "computed_at",
    ]
    if inputs.empty:
        empty_history = pd.DataFrame(columns=history_columns)
        empty_latest = pd.DataFrame(columns=history_columns)
        empty_transitions = pd.DataFrame(columns=transition_columns)
        return empty_history, empty_latest, empty_transitions

    computed = pd.Timestamp(computed_at or datetime.now(timezone.utc))
    frame = inputs.copy()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date
    frame = frame.dropna(subset=["as_of_date"]).sort_values("as_of_date").reset_index(drop=True)
    frame["effective_from_date"] = [next_business_session(as_of_date) for as_of_date in frame["as_of_date"].tolist()]

    history_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    previous_signal_state: dict[RegimeCode, dict[str, Any]] = {}

    for row in frame.to_dict("records"):
        classification = classify_regime_row(row, config=cfg)
        for signal in classification["signals"]:
            regime_code = signal["regime_code"]
            signal_row = {
                "as_of_date": row["as_of_date"],
                "effective_from_date": row["effective_from_date"],
                "model_name": model_name,
                "model_version": int(model_version),
                "regime_code": regime_code,
                "display_name": signal["display_name"],
                "signal_state": signal["signal_state"],
                "score": float(signal["score"]),
                "activation_threshold": float(signal["activation_threshold"]),
                "is_active": bool(signal["is_active"]),
                "matched_rule_id": signal["matched_rule_id"],
                "halt_flag": bool(classification["halt_flag"]),
                "halt_reason": classification["halt_reason"],
                "evidence_json": json.dumps(signal["evidence"], sort_keys=True),
                "computed_at": computed,
            }
            history_rows.append(signal_row)

            prior = previous_signal_state.get(regime_code)
            prior_active = bool(prior["is_active"]) if prior is not None else False
            current_active = bool(signal["is_active"])
            if prior is None:
                if current_active:
                    transition_rows.append(
                        {
                            "model_name": model_name,
                            "model_version": int(model_version),
                            "effective_from_date": row["effective_from_date"],
                            "regime_code": regime_code,
                            "transition_type": "entered",
                            "prior_score": None,
                            "new_score": float(signal["score"]),
                            "activation_threshold": float(signal["activation_threshold"]),
                            "trigger_rule_id": signal["matched_rule_id"],
                            "computed_at": computed,
                        }
                    )
            elif prior_active != current_active:
                transition_rows.append(
                    {
                        "model_name": model_name,
                        "model_version": int(model_version),
                        "effective_from_date": row["effective_from_date"],
                        "regime_code": regime_code,
                        "transition_type": "entered" if current_active else "exited",
                        "prior_score": float(prior["score"]),
                        "new_score": float(signal["score"]),
                        "activation_threshold": float(signal["activation_threshold"]),
                        "trigger_rule_id": signal["matched_rule_id"] if current_active else prior.get("matched_rule_id"),
                        "computed_at": computed,
                    }
                )
            previous_signal_state[regime_code] = {
                "is_active": current_active,
                "score": float(signal["score"]),
                "matched_rule_id": signal["matched_rule_id"],
            }

    history = pd.DataFrame(history_rows, columns=history_columns)
    latest_as_of_date = history["as_of_date"].max() if not history.empty else None
    latest = (
        history.loc[history["as_of_date"] == latest_as_of_date].reset_index(drop=True)
        if latest_as_of_date is not None
        else pd.DataFrame(columns=history_columns)
    )
    transitions = pd.DataFrame(transition_rows, columns=transition_columns)
    return history, latest, transitions
