from __future__ import annotations

import json
from typing import Any, Callable, Iterable, Sequence

from asset_allocation_runtime_common.postgres import connect, copy_rows


BACKTEST_RESULTS_SCHEMA_VERSION = 5

_SUMMARY_COLUMNS = [
    "run_id",
    "total_return",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "max_drawdown",
    "trades",
    "initial_cash",
    "final_equity",
    "gross_total_return",
    "gross_annualized_return",
    "total_commission",
    "total_slippage_cost",
    "total_transaction_cost",
    "cost_drag_bps",
    "avg_gross_exposure",
    "avg_net_exposure",
    "sortino_ratio",
    "calmar_ratio",
    "closed_positions",
    "winning_positions",
    "losing_positions",
    "hit_rate",
    "avg_win_pnl",
    "avg_loss_pnl",
    "avg_win_return",
    "avg_loss_return",
    "payoff_ratio",
    "profit_factor",
    "expectancy_pnl",
    "expectancy_return",
]
_TIMESERIES_COLUMNS = [
    "run_id",
    "bar_ts",
    "portfolio_value",
    "drawdown",
    "daily_return",
    "period_return",
    "cumulative_return",
    "cash",
    "gross_exposure",
    "net_exposure",
    "turnover",
    "commission",
    "slippage_cost",
    "trade_count",
]
_ROLLING_COLUMNS = [
    "run_id",
    "bar_ts",
    "window_days",
    "window_periods",
    "rolling_return",
    "rolling_volatility",
    "rolling_sharpe",
    "rolling_max_drawdown",
    "turnover_sum",
    "commission_sum",
    "slippage_cost_sum",
    "n_trades_sum",
    "gross_exposure_avg",
    "net_exposure_avg",
]
_TRADE_COLUMNS = [
    "run_id",
    "trade_seq",
    "execution_ts",
    "symbol",
    "quantity",
    "price",
    "notional",
    "commission",
    "slippage_cost",
    "cash_after",
    "position_id",
    "trade_role",
]
_CLOSED_POSITION_COLUMNS = [
    "run_id",
    "position_id",
    "symbol",
    "opened_at",
    "closed_at",
    "holding_period_bars",
    "average_cost",
    "exit_price",
    "max_quantity",
    "resize_count",
    "realized_pnl",
    "realized_return",
    "total_commission",
    "total_slippage_cost",
    "total_transaction_cost",
    "exit_reason",
    "exit_rule_id",
]
_SELECTION_TRACE_COLUMNS = [
    "run_id",
    "rebalance_ts",
    "ordinal",
    "symbol",
    "score",
    "selected",
    "target_weight",
]
_REGIME_TRACE_COLUMNS = [
    "run_id",
    "bar_ts",
    "session_date",
    "model_name",
    "model_version",
    "as_of_date",
    "effective_from_date",
    "primary_regime_code",
    "halt_flag",
    "halt_reason",
    "active_regimes_json",
    "signals_json",
]


def _coalesce_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _build_summary_row(run_id: str, summary: dict[str, Any]) -> list[Any]:
    return [
        run_id,
        summary.get("total_return"),
        summary.get("annualized_return"),
        summary.get("annualized_volatility"),
        summary.get("sharpe_ratio"),
        summary.get("max_drawdown"),
        summary.get("trades"),
        summary.get("initial_cash"),
        summary.get("final_equity"),
        summary.get("gross_total_return"),
        summary.get("gross_annualized_return"),
        summary.get("total_commission"),
        summary.get("total_slippage_cost"),
        summary.get("total_transaction_cost"),
        summary.get("cost_drag_bps"),
        summary.get("avg_gross_exposure"),
        summary.get("avg_net_exposure"),
        summary.get("sortino_ratio"),
        summary.get("calmar_ratio"),
        summary.get("closed_positions"),
        summary.get("winning_positions"),
        summary.get("losing_positions"),
        summary.get("hit_rate"),
        summary.get("avg_win_pnl"),
        summary.get("avg_loss_pnl"),
        summary.get("avg_win_return"),
        summary.get("avg_loss_return"),
        summary.get("payoff_ratio"),
        summary.get("profit_factor"),
        summary.get("expectancy_pnl"),
        summary.get("expectancy_return"),
    ]


def _build_timeseries_row(run_id: str, row: dict[str, Any], _index: int) -> list[Any]:
    daily_return = _coalesce_value(row, "daily_return", "period_return")
    period_return = _coalesce_value(row, "period_return", "daily_return")
    return [
        run_id,
        row.get("date"),
        row.get("portfolio_value"),
        row.get("drawdown"),
        daily_return,
        period_return,
        row.get("cumulative_return"),
        row.get("cash"),
        row.get("gross_exposure"),
        row.get("net_exposure"),
        row.get("turnover"),
        row.get("commission"),
        row.get("slippage_cost"),
        row.get("trade_count"),
    ]


def _build_rolling_row(run_id: str, row: dict[str, Any], _index: int) -> list[Any]:
    window_days = _coalesce_value(row, "window_days", "window_periods")
    window_periods = _coalesce_value(row, "window_periods", "window_days")
    return [
        run_id,
        row.get("date"),
        window_days,
        window_periods,
        row.get("rolling_return"),
        row.get("rolling_volatility"),
        row.get("rolling_sharpe"),
        row.get("rolling_max_drawdown"),
        row.get("turnover_sum"),
        row.get("commission_sum"),
        row.get("slippage_cost_sum"),
        row.get("n_trades_sum"),
        row.get("gross_exposure_avg"),
        row.get("net_exposure_avg"),
    ]


def _build_trade_row(run_id: str, row: dict[str, Any], index: int) -> list[Any]:
    return [
        run_id,
        index,
        row.get("execution_date"),
        row.get("symbol"),
        row.get("quantity"),
        row.get("price"),
        row.get("notional"),
        row.get("commission"),
        row.get("slippage_cost"),
        row.get("cash_after"),
        row.get("position_id"),
        row.get("trade_role"),
    ]


def _build_closed_position_row(run_id: str, row: dict[str, Any], _index: int) -> list[Any]:
    return [
        run_id,
        row.get("position_id"),
        row.get("symbol"),
        row.get("opened_at"),
        row.get("closed_at"),
        row.get("holding_period_bars"),
        row.get("average_cost"),
        row.get("exit_price"),
        row.get("max_quantity"),
        row.get("resize_count"),
        row.get("realized_pnl"),
        row.get("realized_return"),
        row.get("total_commission"),
        row.get("total_slippage_cost"),
        row.get("total_transaction_cost"),
        row.get("exit_reason"),
        row.get("exit_rule_id"),
    ]


def _build_selection_trace_row(run_id: str, row: dict[str, Any], _index: int) -> list[Any]:
    return [
        run_id,
        row.get("rebalance_ts"),
        row.get("ordinal"),
        row.get("symbol"),
        row.get("score"),
        row.get("selected"),
        row.get("target_weight"),
    ]


def _build_regime_trace_row(run_id: str, row: dict[str, Any], _index: int) -> list[Any]:
    return [
        run_id,
        row.get("date"),
        row.get("session_date"),
        row.get("model_name"),
        row.get("model_version"),
        row.get("as_of_date"),
        row.get("effective_from_date"),
        row.get("primary_regime_code"),
        row.get("halt_flag"),
        row.get("halt_reason"),
        json.dumps(list(row.get("active_regimes") or []), sort_keys=False),
        json.dumps(list(row.get("signals") or []), sort_keys=True),
    ]


def _delete_existing_result_rows(cur: Any, run_id: str) -> None:
    for table_name in (
        "core.backtest_regime_trace",
        "core.backtest_selection_trace",
        "core.backtest_trades",
        "core.backtest_closed_positions",
        "core.backtest_rolling_metrics",
        "core.backtest_timeseries",
        "core.backtest_run_summary",
    ):
        cur.execute(f"DELETE FROM {table_name} WHERE run_id = %s", (run_id,))


def _validate_run_exists(cur: Any, run_id: str) -> None:
    cur.execute("SELECT 1 FROM core.runs WHERE run_id = %s FOR UPDATE", (run_id,))
    if cur.fetchone() is None:
        raise ValueError(f"Run '{run_id}' not found.")


def _copy_dataset(
    cur: Any,
    *,
    table: str,
    columns: Sequence[str],
    rows: Iterable[dict[str, Any]] | None,
    row_builder: Callable[[dict[str, Any], int], Sequence[Any]],
    run_id: str,
) -> None:
    row_count = 0

    def built_rows() -> Iterable[Sequence[Any]]:
        nonlocal row_count
        source = () if rows is None else rows
        for index, row in enumerate(source, start=1):
            row_count = index
            yield row_builder(row, index)

    copy_rows(cur, table=table, columns=columns, rows=built_rows())
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
    persisted = int(cur.fetchone()[0] or 0)
    if persisted != row_count:
        raise ValueError(
            f"Persisted row-count mismatch for table '{table}' run_id='{run_id}': expected {row_count} got {persisted}."
        )


def persist_backtest_results(
    dsn: str,
    *,
    run_id: str,
    summary: dict[str, Any],
    timeseries_rows: Iterable[dict[str, Any]] | None = None,
    rolling_metric_rows: Iterable[dict[str, Any]] | None = None,
    trade_rows: Iterable[dict[str, Any]] | None = None,
    closed_position_rows: Iterable[dict[str, Any]] | None = None,
    selection_trace_rows: Iterable[dict[str, Any]] | None = None,
    regime_trace_rows: Iterable[dict[str, Any]] | None = None,
    results_schema_version: int = BACKTEST_RESULTS_SCHEMA_VERSION,
) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            _validate_run_exists(cur, run_id)
            _delete_existing_result_rows(cur, run_id)
            cur.execute(
                """
                INSERT INTO core.backtest_run_summary (
                    run_id,
                    total_return,
                    annualized_return,
                    annualized_volatility,
                    sharpe_ratio,
                    max_drawdown,
                    trades,
                    initial_cash,
                    final_equity,
                    gross_total_return,
                    gross_annualized_return,
                    total_commission,
                    total_slippage_cost,
                    total_transaction_cost,
                    cost_drag_bps,
                    avg_gross_exposure,
                    avg_net_exposure,
                    sortino_ratio,
                    calmar_ratio,
                    closed_positions,
                    winning_positions,
                    losing_positions,
                    hit_rate,
                    avg_win_pnl,
                    avg_loss_pnl,
                    avg_win_return,
                    avg_loss_return,
                    payoff_ratio,
                    profit_factor,
                    expectancy_pnl,
                    expectancy_return
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                _build_summary_row(run_id, summary),
            )
            _copy_dataset(
                cur,
                table="core.backtest_timeseries",
                columns=_TIMESERIES_COLUMNS,
                rows=timeseries_rows,
                row_builder=lambda row, index: _build_timeseries_row(run_id, row, index),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_rolling_metrics",
                columns=_ROLLING_COLUMNS,
                rows=rolling_metric_rows,
                row_builder=lambda row, index: _build_rolling_row(run_id, row, index),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_trades",
                columns=_TRADE_COLUMNS,
                rows=trade_rows,
                row_builder=lambda row, index: _build_trade_row(run_id, row, index),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_closed_positions",
                columns=_CLOSED_POSITION_COLUMNS,
                rows=closed_position_rows,
                row_builder=lambda row, index: _build_closed_position_row(run_id, row, index),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_selection_trace",
                columns=_SELECTION_TRACE_COLUMNS,
                rows=selection_trace_rows,
                row_builder=lambda row, index: _build_selection_trace_row(run_id, row, index),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_regime_trace",
                columns=_REGIME_TRACE_COLUMNS,
                rows=regime_trace_rows,
                row_builder=lambda row, index: _build_regime_trace_row(run_id, row, index),
                run_id=run_id,
            )
            cur.execute(
                """
                UPDATE core.runs
                SET
                    results_ready_at = NOW(),
                    results_schema_version = %s,
                    heartbeat_at = NOW()
                WHERE run_id = %s
                """,
                (int(results_schema_version), run_id),
            )
