from __future__ import annotations

import pytest

from asset_allocation_runtime_common.market_data.symbol_identity import (
    InvalidSymbolInputError,
    UnsupportedProviderSymbolError,
    canonicalize_provider_symbol,
    provider_symbol_for_query,
)


def test_canonicalize_provider_symbol_resolves_massive_market_vix_aliases() -> None:
    assert canonicalize_provider_symbol("massive", "market", "I:VIX") == "^VIX"
    assert canonicalize_provider_symbol("massive", "market", "I:VIX3M") == "^VIX3M"
    assert canonicalize_provider_symbol("massive", "market", "aapl") == "AAPL"
    assert canonicalize_provider_symbol("massive", "market", "test_mkt_1") == "TEST_MKT_1"


def test_canonicalize_provider_symbol_rejects_bare_massive_market_vix_aliases() -> None:
    with pytest.raises(UnsupportedProviderSymbolError) as exc_info:
        canonicalize_provider_symbol("massive", "market", "VIX")

    assert exc_info.value.result.status == "unsupported"
    assert exc_info.value.result.error is not None
    assert exc_info.value.result.error.code == "unsupported"


def test_symbol_alias_resolution_is_provider_domain_scoped() -> None:
    with pytest.raises(UnsupportedProviderSymbolError):
        canonicalize_provider_symbol("massive", "finance", "I:VIX")
    assert provider_symbol_for_query("massive", "finance", "^VIX") == "^VIX"


def test_canonicalize_provider_symbol_rejects_unknown_provider_aliases() -> None:
    with pytest.raises(UnsupportedProviderSymbolError) as exc_info:
        canonicalize_provider_symbol("massive", "market", "I:UNKNOWN")

    assert exc_info.value.result.error is not None
    assert "Unknown provider alias" in exc_info.value.result.error.message


def test_provider_symbol_for_query_resolves_massive_market_vix_aliases() -> None:
    assert provider_symbol_for_query("massive", "market", "^VIX") == "I:VIX"
    assert provider_symbol_for_query("massive", "market", "^VIX3M") == "I:VIX3M"
    assert provider_symbol_for_query("massive", "market", "SPY") == "SPY"


@pytest.mark.parametrize("raw_symbol", ["", "   ", "-", "N/A", "bad symbol", "BAD/SYMBOL"])
def test_symbol_resolution_rejects_blank_placeholder_and_malformed_symbols(raw_symbol: str) -> None:
    with pytest.raises(InvalidSymbolInputError):
        canonicalize_provider_symbol("massive", "market", raw_symbol)
