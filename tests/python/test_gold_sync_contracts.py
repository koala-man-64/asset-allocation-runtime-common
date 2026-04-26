from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from asset_allocation_runtime_common.market_data import gold_column_lookup_catalog as lookup_catalog
from asset_allocation_runtime_common.shared_core import gold_sync_contracts as sync


def test_market_sync_config_includes_corporate_action_columns() -> None:
    config = sync.get_sync_config("market")
    prepared = sync._prepare_frame(  # type: ignore[attr-defined]
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-02-28")],
                "symbol": ["aapl"],
                "dividend_amount": [0.25],
                "split_coefficient": [2.0],
                "is_dividend_day": [1],
                "is_split_day": [1],
            }
        ),
        config=config,
    )

    row = prepared.iloc[0]
    assert row["date"] == date(2026, 2, 28)
    assert row["symbol"] == "AAPL"
    assert "dividend_amount" in config.columns
    assert "split_coefficient" in config.columns
    assert "is_dividend_day" in config.integer_columns
    assert "is_split_day" in config.integer_columns
    assert float(row["dividend_amount"]) == 0.25
    assert float(row["split_coefficient"]) == 2.0
    assert int(row["is_dividend_day"]) == 1
    assert int(row["is_split_day"]) == 1


def test_market_lookup_catalog_covers_corporate_action_columns() -> None:
    market_columns = lookup_catalog.expected_gold_lookup_columns()["market_data"]

    assert "dividend_amount" in market_columns
    assert "split_coefficient" in market_columns
    assert "is_dividend_day" in market_columns
    assert "is_split_day" in market_columns


def test_market_sync_config_includes_daily_liquidity_columns() -> None:
    config = sync.get_sync_config("market")
    prepared = sync._prepare_frame(  # type: ignore[attr-defined]
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-02-28")],
                "symbol": ["aapl"],
                "dist_prev_week_high_atr": [0.75],
                "position_in_20d_range": [0.65],
                "swept_sr_resistance_1": [1],
                "bars_since_bearish_sweep": [2],
                "bearish_confirm_after_sweep": [1],
                "amihud_20d": [0.0000123],
                "dollar_volume_20d": [1_500_000.0],
                "liquidity_stress_score": [1.25],
            }
        ),
        config=config,
    )

    row = prepared.iloc[0]
    assert "dist_prev_week_high_atr" in config.columns
    assert "position_in_20d_range" in config.columns
    assert "swept_sr_resistance_1" in config.integer_columns
    assert "bars_since_bearish_sweep" in config.integer_columns
    assert "bearish_confirm_after_sweep" in config.integer_columns
    assert int(row["swept_sr_resistance_1"]) == 1
    assert int(row["bars_since_bearish_sweep"]) == 2
    assert int(row["bearish_confirm_after_sweep"]) == 1
    assert row["amihud_20d"] == pytest.approx(0.0000123)
    assert row["dollar_volume_20d"] == pytest.approx(1_500_000.0)


def test_market_lookup_catalog_covers_daily_liquidity_columns() -> None:
    market_columns = lookup_catalog.expected_gold_lookup_columns()["market_data"]

    assert "dist_prev_week_high_atr" in market_columns
    assert "position_in_20d_range" in market_columns
    assert "swept_sr_resistance_1" in market_columns
    assert "bars_since_bearish_sweep" in market_columns
    assert "amihud_20d" in market_columns
    assert "liquidity_stress_score" in market_columns
