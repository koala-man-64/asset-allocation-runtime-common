from __future__ import annotations

from asset_allocation_runtime_common import backtest_results


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
        },
        timeseries_rows=[
            {
                "date": "2026-03-03T14:30:00+00:00",
                "portfolio_value": 100000.0,
                "drawdown": 0.0,
                "daily_return": 0.0,
                "cumulative_return": 0.0,
                "cash": 100000.0,
                "gross_exposure": 0.0,
                "net_exposure": 0.0,
                "turnover": 0.0,
                "commission": 0.0,
                "slippage_cost": 0.0,
                "trade_count": 0,
            }
        ],
        rolling_metric_rows=[
            {
                "date": "2026-03-03T14:30:00+00:00",
                "window_days": 63,
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
        ],
        trade_rows=[
            {
                "execution_date": "2026-03-03T14:35:00+00:00",
                "symbol": "MSFT",
                "quantity": 10.0,
                "price": 100.0,
                "notional": 1000.0,
                "commission": 1.0,
                "slippage_cost": 0.5,
                "cash_after": 98998.5,
            }
        ],
        selection_trace_rows=[
            {
                "rebalance_ts": "2026-03-03T14:30:00+00:00",
                "ordinal": 1,
                "symbol": "MSFT",
                "score": 0.9,
                "selected": True,
                "target_weight": 0.5,
            }
        ],
        regime_trace_rows=[
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
        ],
    )

    assert len(cursor.copied_tables["core.backtest_timeseries"]) == 1
    assert len(cursor.copied_tables["core.backtest_rolling_metrics"]) == 1
    assert len(cursor.copied_tables["core.backtest_trades"]) == 1
    assert len(cursor.copied_tables["core.backtest_selection_trace"]) == 1
    assert len(cursor.copied_tables["core.backtest_regime_trace"]) == 1
    assert any("UPDATE core.runs" in sql for sql, _ in cursor.executed)
