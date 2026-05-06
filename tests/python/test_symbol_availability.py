from __future__ import annotations

import pandas as pd
import pytest

import asset_allocation_runtime_common.shared_core.symbol_availability as symbol_availability
from asset_allocation_runtime_common.market_data.symbol_identity import UnsupportedProviderSymbolError
from asset_allocation_runtime_common.shared_core.symbol_availability import EmptyProviderSymbolSetError


def test_sync_domain_availability_rejects_empty_provider_set_before_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:password@localhost/db")
    monkeypatch.setattr(
        symbol_availability,
        "_fetch_provider_symbols_df",
        lambda _provider: pd.DataFrame(columns=["Symbol"]),
    )

    def fail_connect(_dsn: str):
        raise AssertionError("Postgres should not be opened for an empty provider symbol set.")

    monkeypatch.setattr(symbol_availability, "connect", fail_connect)

    with pytest.raises(EmptyProviderSymbolSetError) as exc_info:
        symbol_availability.sync_domain_availability("market")

    assert exc_info.value.provider == "massive"
    assert exc_info.value.source_column == "source_massive"
    assert exc_info.value.domain == "market"


def test_normalize_massive_records_canonicalizes_vix_aliases_and_counts_resolutions() -> None:
    out = symbol_availability._normalize_massive_records(
        [
            {"ticker": "AAPL", "type": "CS"},
            {"ticker": "I:VIX", "type": "INDEX"},
            {"ticker": "I:VIX3M", "type": "INDEX"},
        ]
    )

    assert out["Symbol"].tolist() == ["AAPL", "^VIX", "^VIX3M"]
    assert out.attrs["alias_resolution_count"] == 2
    assert out.attrs["alias_resolution_failure_count"] == 0


def test_normalize_massive_records_skips_invalid_provider_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(symbol_availability.mdc, "write_warning", lambda message: warnings.append(str(message)))

    out = symbol_availability._normalize_massive_records(
        [
            {"ticker": "AAPL", "type": "CS"},
            {"ticker": "N/A", "type": "CS"},
            {"ticker": "UNKNOWN", "type": "CS"},
            {"ticker": "MSFT", "type": "CS"},
        ]
    )

    assert out["Symbol"].tolist() == ["AAPL", "MSFT"]
    assert out.attrs["alias_resolution_count"] == 0
    assert out.attrs["alias_resolution_failure_count"] == 2
    assert any("Massive ticker sync skipped invalid symbol records: count=2" in message for message in warnings)


def test_normalize_massive_records_tracks_failures_for_all_invalid_provider_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(symbol_availability.mdc, "write_warning", lambda message: warnings.append(str(message)))

    out = symbol_availability._normalize_massive_records(
        [
            {"ticker": "N/A", "type": "CS"},
            {"ticker": "NA", "type": "ETF"},
        ]
    )

    assert out.empty
    assert out.attrs["alias_resolution_count"] == 0
    assert out.attrs["alias_resolution_failure_count"] == 2
    assert any("count=2" in message for message in warnings)


def test_normalize_massive_records_rejects_unsupported_bare_vix_alias() -> None:
    with pytest.raises(UnsupportedProviderSymbolError):
        symbol_availability._normalize_massive_records([{"ticker": "VIX", "type": "INDEX"}])
