from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from asset_allocation_runtime_common.backtesting.runtime_engine import (
    RebalanceTarget,
    ResolvedBacktestDefinition,
    _build_snapshot_symbol_index,
    _score_snapshot,
    _target_quantities_for_pending_targets,
    validate_backtest_submission,
)
from asset_allocation_runtime_common.ranking_engine.contracts import RankingSchemaConfig
from asset_allocation_runtime_common.strategy_engine.contracts import StrategyConfig


def _sample_universe() -> SimpleNamespace:
    return SimpleNamespace(
        source="postgres_gold",
        root={
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 1,
                }
            ],
        },
    )


def _definition_with_position_policy(
    position_policy: dict[str, object],
    *,
    top_n: int = 3,
) -> ResolvedBacktestDefinition:
    universe = _sample_universe()
    raw_config = {
        "universeConfigName": "large-cap-quality",
        "rebalance": "weekly",
        "longOnly": True,
        "topN": top_n,
        "lookbackWindow": 20,
        "holdingPeriod": 5,
        "costModel": "default",
        "rankingSchemaName": "quality",
        "intrabarConflictPolicy": "stop_first",
        "positionPolicy": position_policy,
        "exits": [],
    }
    return ResolvedBacktestDefinition(
        strategy_name="mom-spy-res",
        strategy_version=3,
        strategy_config=StrategyConfig.model_validate(raw_config),
        strategy_config_raw=raw_config,
        strategy_universe=universe,
        ranking_schema_name="quality",
        ranking_schema_version=7,
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "f1",
                                "table": "market_data",
                                "column": "return_20d",
                                "weight": 1,
                                "direction": "desc",
                                "missingValuePolicy": "exclude",
                                "transforms": [],
                            }
                        ],
                        "transforms": [],
                    }
                ],
                "overallTransforms": [],
            }
        ),
        ranking_universe_name="large-cap-quality",
        ranking_universe_version=5,
        ranking_universe=universe,
    )


def _ranking_snapshot() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-03T14:30:00Z")] * 4,
            "symbol": ["MSFT", "AAPL", "NVDA", "GOOG"],
            "market_data__close": [10.0, 10.0, 10.0, 10.0],
            "market_data__return_20d": [0.5, 0.6, 0.2, 0.4],
        }
    )


def test_score_snapshot_applies_position_policy_limits() -> None:
    ranked = _score_snapshot(
        _ranking_snapshot(),
        definition=_definition_with_position_policy(
            {
                "targetPositionSize": {"mode": "pct_of_allocatable_capital", "value": 30},
                "maxPositionSize": {"mode": "pct_of_allocatable_capital", "value": 20},
                "maxOpenPositions": 2,
            }
        ),
        rebalance_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
    )

    selected = ranked[ranked["selected"]]
    assert selected["symbol"].tolist() == ["AAPL", "MSFT"]
    assert selected["target_weight"].tolist() == pytest.approx([0.2, 0.2])


def test_target_quantities_reject_long_only_overallocation() -> None:
    snapshot_index = _build_snapshot_symbol_index(
        pd.DataFrame(
            [
                {"symbol": "AAPL", "market_data__open": 100.0},
                {"symbol": "MSFT", "market_data__open": 100.0},
            ]
        )
    )

    with pytest.raises(ValueError, match="exceeds available strategy capital"):
        _target_quantities_for_pending_targets(
            {
                "AAPL": RebalanceTarget(target_weight=0.0, target_notional=60_000.0),
                "MSFT": RebalanceTarget(target_weight=0.0, target_notional=50_000.0),
            },
            snapshot_index=snapshot_index,
            market_equity_open=100_000.0,
            definition=_definition_with_position_policy(
                {"targetPositionSize": {"mode": "notional_base_ccy", "value": 60_000}},
                top_n=2,
            ),
        )


def test_validate_backtest_submission_rejects_option_only_policy() -> None:
    with pytest.raises(ValueError, match="equity execution only"):
        validate_backtest_submission(
            "postgresql://test:test@localhost:5432/asset_allocation",
            definition=_definition_with_position_policy({"allowedAssetClasses": ["option"]}),
            start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
            bar_size="5m",
        )
