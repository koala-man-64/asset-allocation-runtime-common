from __future__ import annotations

from typing import Any, Iterable, Sequence

from asset_allocation_runtime_common.postgres import connect, copy_rows


BACKTEST_RESULTS_SCHEMA_VERSION = 1

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
]
_TIMESERIES_COLUMNS = [
    "run_id",
    "bar_ts",
    "portfolio_value",
    "drawdown",
    "daily_return",
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
    "regime_code",
    "regime_status",
    "matched_rule_id",
    "halt_flag",
    "halt_reason",
    "blocked",
    "blocked_reason",
    "blocked_action",
    "exposure_multiplier",
]


def _coerce_records(rows: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not rows:
        return []
    return [dict(row) for row in rows]


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
    ]


def _build_timeseries_rows(run_id: str, rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            run_id,
            row.get("date"),
            row.get("portfolio_value"),
            row.get("drawdown"),
            row.get("daily_return"),
            row.get("cumulative_return"),
            row.get("cash"),
            row.get("gross_exposure"),
            row.get("net_exposure"),
            row.get("turnover"),
            row.get("commission"),
            row.get("slippage_cost"),
            row.get("trade_count"),
        ]
        for row in rows
    ]


def _build_rolling_rows(run_id: str, rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            run_id,
            row.get("date"),
            row.get("window_days"),
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
        for row in rows
    ]


def _build_trade_rows(run_id: str, rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    built: list[list[Any]] = []
    for index, row in enumerate(rows, start=1):
        built.append(
            [
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
            ]
        )
    return built


def _build_selection_trace_rows(run_id: str, rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            run_id,
            row.get("rebalance_ts"),
            row.get("ordinal"),
            row.get("symbol"),
            row.get("score"),
            row.get("selected"),
            row.get("target_weight"),
        ]
        for row in rows
    ]


def _build_regime_trace_rows(run_id: str, rows: Sequence[dict[str, Any]]) -> list[list[Any]]:
    return [
        [
            run_id,
            row.get("date"),
            row.get("session_date"),
            row.get("model_name"),
            row.get("model_version"),
            row.get("as_of_date"),
            row.get("effective_from_date"),
            row.get("regime_code"),
            row.get("regime_status"),
            row.get("matched_rule_id"),
            row.get("halt_flag"),
            row.get("halt_reason"),
            row.get("blocked"),
            row.get("blocked_reason"),
            row.get("blocked_action"),
            row.get("exposure_multiplier"),
        ]
        for row in rows
    ]


def _delete_existing_result_rows(cur: Any, run_id: str) -> None:
    for table_name in (
        "core.backtest_regime_trace",
        "core.backtest_selection_trace",
        "core.backtest_trades",
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
    rows: Sequence[Sequence[Any]],
    run_id: str,
) -> None:
    if not rows:
        return
    copy_rows(cur, table=table, columns=columns, rows=rows)
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (run_id,))
    persisted = int(cur.fetchone()[0] or 0)
    if persisted != len(rows):
        raise ValueError(
            f"Persisted row-count mismatch for table '{table}' run_id='{run_id}': expected {len(rows)} got {persisted}."
        )


def persist_backtest_results(
    dsn: str,
    *,
    run_id: str,
    summary: dict[str, Any],
    timeseries_rows: Iterable[dict[str, Any]] | None = None,
    rolling_metric_rows: Iterable[dict[str, Any]] | None = None,
    trade_rows: Iterable[dict[str, Any]] | None = None,
    selection_trace_rows: Iterable[dict[str, Any]] | None = None,
    regime_trace_rows: Iterable[dict[str, Any]] | None = None,
    results_schema_version: int = BACKTEST_RESULTS_SCHEMA_VERSION,
) -> None:
    timeseries_records = _coerce_records(timeseries_rows)
    rolling_records = _coerce_records(rolling_metric_rows)
    trade_records = _coerce_records(trade_rows)
    selection_records = _coerce_records(selection_trace_rows)
    regime_records = _coerce_records(regime_trace_rows)

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
                    final_equity
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                _build_summary_row(run_id, summary),
            )
            _copy_dataset(
                cur,
                table="core.backtest_timeseries",
                columns=_TIMESERIES_COLUMNS,
                rows=_build_timeseries_rows(run_id, timeseries_records),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_rolling_metrics",
                columns=_ROLLING_COLUMNS,
                rows=_build_rolling_rows(run_id, rolling_records),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_trades",
                columns=_TRADE_COLUMNS,
                rows=_build_trade_rows(run_id, trade_records),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_selection_trace",
                columns=_SELECTION_TRACE_COLUMNS,
                rows=_build_selection_trace_rows(run_id, selection_records),
                run_id=run_id,
            )
            _copy_dataset(
                cur,
                table="core.backtest_regime_trace",
                columns=_REGIME_TRACE_COLUMNS,
                rows=_build_regime_trace_rows(run_id, regime_records),
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
