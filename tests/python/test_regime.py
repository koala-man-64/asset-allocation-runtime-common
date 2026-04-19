from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.domain.regime import (
    build_regime_outputs,
    classify_regime_row,
    compute_curve_state,
    compute_trend_state,
    next_business_session,
)


def test_compute_states_use_canonical_boundaries() -> None:
    assert compute_trend_state(0.02) == "near_zero"
    assert compute_trend_state(0.0201) == "positive"
    assert compute_trend_state(-0.02) == "near_zero"
    assert compute_trend_state(-0.0201) == "negative"

    assert compute_curve_state(0.50) == "contango"
    assert compute_curve_state(0.49) == "flat"
    assert compute_curve_state(-0.50) == "inverted"
    assert compute_curve_state(-0.49) == "flat"


def test_classify_regime_row_uses_strict_high_vol_entry_and_disables_canonical_transition_band() -> None:
    canonical_at_boundary = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": -0.05,
            "vix_slope": -1.0,
            "rvol_10d_ann": 28.0,
            "vix_spot_close": 32.01,
            "vix_gt_32_streak": 2,
        }
    )
    canonical_above_boundary = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": -0.05,
            "vix_slope": -1.0,
            "rvol_10d_ann": 28.01,
            "vix_spot_close": 32.01,
            "vix_gt_32_streak": 2,
        }
    )
    canonical_mid_band = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": 0.0,
            "vix_slope": 0.1,
            "rvol_10d_ann": 26.0,
            "vix_spot_close": 24.0,
            "vix_gt_32_streak": 0,
        },
        prev_confirmed_regime="trending_bear",
    )
    legacy_mid_band = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": 0.0,
            "vix_slope": 0.1,
            "rvol_10d_ann": 26.0,
            "vix_spot_close": 24.0,
            "vix_gt_32_streak": 0,
        },
        prev_confirmed_regime="trending_bear",
        config={"highVolExitThreshold": 25.0},
    )

    assert canonical_at_boundary["regime_code"] == "unclassified"
    assert canonical_at_boundary["halt_flag"] is True
    assert canonical_above_boundary["regime_code"] == "high_vol"
    assert canonical_mid_band["regime_status"] == "unclassified"
    assert canonical_mid_band["matched_rule_id"] is None
    assert legacy_mid_band["regime_code"] == "trending_bear"
    assert legacy_mid_band["regime_status"] == "transition"
    assert legacy_mid_band["matched_rule_id"] == "transition_band"


def test_next_business_session_skips_weekends_and_market_holidays() -> None:
    assert next_business_session(pd.Timestamp("2026-03-06").date()).isoformat() == "2026-03-09"
    assert next_business_session(pd.Timestamp("2026-04-02").date()).isoformat() == "2026-04-06"


def test_build_regime_outputs_uses_deterministic_next_business_session_dates() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-02",
                "return_1d": 0.01,
                "return_20d": 0.04,
                "rvol_10d_ann": 12.0,
                "vix_spot_close": 18.0,
                "vix3m_close": 18.7,
                "vix_slope": 0.7,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-04",
                "return_1d": -0.02,
                "return_20d": -0.05,
                "rvol_10d_ann": 18.0,
                "vix_spot_close": 24.0,
                "vix3m_close": 23.2,
                "vix_slope": -0.8,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-04-02",
                "return_1d": 0.01,
                "return_20d": 0.03,
                "rvol_10d_ann": 14.0,
                "vix_spot_close": 19.0,
                "vix3m_close": 19.8,
                "vix_slope": 0.8,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
        ]
    )

    history, latest, transitions = build_regime_outputs(
        inputs,
        model_name="default-regime",
        model_version=2,
        computed_at=datetime(2026, 4, 7, tzinfo=timezone.utc),
    )

    assert history["effective_from_date"].tolist()[0].isoformat() == "2026-03-03"
    assert history["effective_from_date"].tolist()[1].isoformat() == "2026-03-05"
    assert history["effective_from_date"].tolist()[2].isoformat() == "2026-04-06"
    assert latest.iloc[0]["as_of_date"].isoformat() == "2026-04-02"
    assert transitions["new_regime_code"].tolist() == ["trending_bull", "trending_bear", "trending_bull"]
