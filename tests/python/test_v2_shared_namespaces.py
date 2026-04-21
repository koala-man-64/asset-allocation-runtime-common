from __future__ import annotations

import importlib
import logging
import sys

from asset_allocation_contracts.paths import DataPaths as ContractDataPaths
from asset_allocation_runtime_common import backtesting, domain, foundation, market_data, providers
from asset_allocation_runtime_common.shared_core.logging_config import configure_logging


def test_v2_namespaces_expose_shared_backend_modules() -> None:
    assert callable(foundation.connect)
    assert callable(foundation.create_bronze_alpha26_manifest)
    assert callable(providers.get_complete_ticker_list)
    assert callable(market_data.load_domain_artifact)
    assert callable(backtesting.persist_backtest_results)
    assert callable(domain.build_regime_outputs)


def test_market_data_namespace_reexports_contract_datapaths() -> None:
    assert market_data.DataPaths is ContractDataPaths


def test_domain_namespace_exposes_broader_regime_surface() -> None:
    config = domain.default_regime_model_config()
    canonical_config = domain.canonical_default_regime_model_config()

    assert domain.DEFAULT_REGIME_MODEL_NAME
    assert domain.CANONICAL_DEFAULT_REGIME_VERSION == 3
    assert config == canonical_config
    assert config["activationThreshold"] == 0.6
    assert config["signalConfigs"]["trending_up"]["displayName"] == "Trending (Up)"
    assert domain.canonical_default_regime_config_errors(canonical_config) == []


def test_top_level_package_keeps_symbol_enrichment_repository_lazy() -> None:
    sys.modules.pop("asset_allocation_runtime_common.symbol_enrichment_repository", None)
    package = importlib.import_module("asset_allocation_runtime_common")

    assert "asset_allocation_runtime_common.symbol_enrichment_repository" not in sys.modules
    assert package.RegimeRepository.__name__ == "RegimeRepository"

    _ = package.SymbolEnrichmentRepository
    assert "asset_allocation_runtime_common.symbol_enrichment_repository" in sys.modules


def test_configure_logging_defaults_when_env_is_unset(monkeypatch) -> None:
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level

    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)

        logger = configure_logging()

        assert logger is root
        assert root.level == logging.INFO
        assert len(root.handlers) == 1
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
        root.setLevel(original_level)
        for handler in original_handlers:
            root.addHandler(handler)
