from __future__ import annotations

from asset_allocation_runtime_common import backtest_results


class _RowSource:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        yield from self.rows


class _FakeCopy:
    def __init__(self, cursor: "_FakeCursor", statement: str) -> None:
        self.cursor = cursor
        self.statement = statement
        self.rows: list[list[object]] = []

    def __enter__(self) -> "_FakeCopy":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        table = self.statement.split(" ", 2)[1]
        self.cursor.copied_tables[table] = list(self.rows)

    def write_row(self, row: list[object]) -> None:
        self.rows.append(list(row))


class _FakeCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self.copied_tables: dict[str, list[list[object]]] = {}
        self._fetchone_value: tuple[object, ...] | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT 1 FROM core.runs"):
            self._fetchone_value = (1,)
        elif normalized.startswith("SELECT COUNT(*) FROM"):
            table = normalized.split("FROM ", 1)[1].split(" ", 1)[0]
            self._fetchone_value = (len(self.copied_tables.get(table, [])),)
        else:
            self._fetchone_value = None

    def fetchone(self) -> tuple[object, ...] | None:
        return self._fetchone_value

    def copy(self, statement: str) -> _FakeCopy:
        return _FakeCopy(self, statement)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_persist_backtest_results_writes_all_tables_and_marks_run_ready(monkeypatch) -> None:
    cursor = _FakeCursor()
    monkeypatch.setattr(backtest_results, "connect", lambda _dsn: _FakeConnection(cursor))

    timeseries_rows = _RowSource(
        [
            {
                "date": "2026-03-03T14:30:00+00:00",
                "portfolio_value": 100000.0,
                "drawdown": 0.0,
                "period_return": 0.01,
                "cumulative_return": 0.01,
                "cash": 100000.0,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "turnover": 0.0,
                "commission": 0.0,
                "slippage_cost": 0.0,
                "trade_count": 0,
            },
            {
                "date": "2026-03-03T14:35:00+00:00",
                "portfolio_value": 101000.0,
                "drawdown": -0.002,
                "daily_return": 0.0095,
                "period_return": 0.0095,
                "cumulative_return": 0.02,
                "cash": 99000.0,
                "gross_exposure": 0.01,
                "net_exposure": 0.01,
                "turnover": 0.01,
                "commission": 1.0,
                "slippage_cost": 0.5,
                "trade_count": 1,
            },
        ]
    )
    rolling_rows = _RowSource(
        [
            {
                "date": "2026-03-03T14:30:00+00:00",
                "window_periods": 63,
                "rolling_return": 0.0,
                "rolling_volatility": 0.0,
                "rolling_sharpe": 0.0,
                "rolling_max_drawdown": 0.0,
                "turnover_sum": 0.0,
                "commission_sum": 0.0,
                "slippage_cost_sum": 0.0,
                "n_trades_sum": 0.0,
                "gross_exposure_avg": 0.0,
                "net_exposure_avg": 0.0,
            }
        ]
    )
    trade_rows = _RowSource(
        [
            {
                "execution_date": "2026-03-03T14:35:00+00:00",
                "symbol": "MSFT",
                "quantity": 10.0,
                "price": 100.0,
                "notional": 1000.0,
                "commission": 1.0,
                "slippage_cost": 0.5,
                "cash_after": 98998.5,
                "position_id": "pos-1",
                "trade_role": "entry",
            }
        ]
    )
    closed_position_rows = _RowSource(
        [
            {
                "position_id": "pos-1",
                "symbol": "MSFT",
                "opened_at": "2026-03-03T14:35:00+00:00",
                "closed_at": "2026-03-05T14:35:00+00:00",
                "holding_period_bars": 3,
                "average_cost": 100.0,
                "exit_price": 105.0,
                "max_quantity": 10.0,
                "resize_count": 1,
                "realized_pnl": 45.0,
                "realized_return": 0.045,
                "total_commission": 2.0,
                "total_slippage_cost": 1.0,
                "total_transaction_cost": 3.0,
                "exit_reason": "rebalance_exit",
                "exit_rule_id": None,
            }
        ]
    )
    selection_rows = _RowSource(
        [
            {
                "rebalance_ts": "2026-03-03T14:30:00+00:00",
                "ordinal": 1,
                "symbol": "MSFT",
                "score": 0.9,
                "selected": True,
                "target_weight": 0.5,
            }
        ]
    )
    regime_rows = _RowSource(
        [
            {
                "date": "2026-03-03T14:30:00+00:00",
                "session_date": "2026-03-03",
                "model_name": "default-regime",
                "model_version": 1,
                "as_of_date": "2026-03-03",
                "effective_from_date": "2026-03-03",
                "regime_code": "trending_bull",
                "regime_status": "confirmed",
                "matched_rule_id": "bull",
                "halt_flag": False,
                "halt_reason": None,
                "blocked": False,
                "blocked_reason": None,
                "blocked_action": None,
                "exposure_multiplier": 1.0,
            }
        ]
    )

    backtest_results.persist_backtest_results(
        "postgresql://test",
        run_id="run-123",
        summary={
            "total_return": 0.12,
            "annualized_return": 0.3,
            "annualized_volatility": 0.2,
            "sharpe_ratio": 1.5,
            "max_drawdown": -0.1,
            "trades": 2,
            "initial_cash": 100000.0,
            "final_equity": 112000.0,
            "gross_total_return": 0.121,
            "gross_annualized_return": 0.301,
            "total_commission": 2.0,
            "total_slippage_cost": 1.0,
            "total_transaction_cost": 3.0,
            "cost_drag_bps": 0.3,
            "avg_gross_exposure": 0.01,
            "avg_net_exposure": 0.01,
            "sortino_ratio": 1.8,
            "calmar_ratio": 3.0,
            "closed_positions": 1,
            "winning_positions": 1,
            "losing_positions": 0,
            "hit_rate": 1.0,
            "avg_win_pnl": 45.0,
            "avg_loss_pnl": 0.0,
            "avg_win_return": 0.045,
            "avg_loss_return": 0.0,
            "payoff_ratio": 0.0,
            "profit_factor": 0.0,
            "expectancy_pnl": 45.0,
            "expectancy_return": 0.045,
        },
        timeseries_rows=timeseries_rows,
        rolling_metric_rows=rolling_rows,
        trade_rows=trade_rows,
        closed_position_rows=closed_position_rows,
        selection_trace_rows=selection_rows,
        regime_trace_rows=regime_rows,
    )

    assert backtest_results.BACKTEST_RESULTS_SCHEMA_VERSION == 4
    assert timeseries_rows.iterations == 1
    assert rolling_rows.iterations == 1
    assert trade_rows.iterations == 1
    assert closed_position_rows.iterations == 1
    assert selection_rows.iterations == 1
    assert regime_rows.iterations == 1
    assert len(cursor.copied_tables["core.backtest_timeseries"]) == 2
    assert len(cursor.copied_tables["core.backtest_rolling_metrics"]) == 1
    assert len(cursor.copied_tables["core.backtest_trades"]) == 1
    assert len(cursor.copied_tables["core.backtest_closed_positions"]) == 1
    assert len(cursor.copied_tables["core.backtest_selection_trace"]) == 1
    assert len(cursor.copied_tables["core.backtest_regime_trace"]) == 1
    assert cursor.copied_tables["core.backtest_timeseries"][0][4] == 0.01
    assert cursor.copied_tables["core.backtest_timeseries"][0][5] == 0.01
    assert cursor.copied_tables["core.backtest_timeseries"][1][4] == 0.0095
    assert cursor.copied_tables["core.backtest_timeseries"][1][5] == 0.0095
    assert cursor.copied_tables["core.backtest_rolling_metrics"][0][2] == 63
    assert cursor.copied_tables["core.backtest_rolling_metrics"][0][3] == 63
    assert cursor.copied_tables["core.backtest_trades"][0][-2:] == ["pos-1", "entry"]
    assert cursor.copied_tables["core.backtest_closed_positions"][0][1] == "pos-1"
    assert any("UPDATE core.runs" in sql for sql, _ in cursor.executed)
