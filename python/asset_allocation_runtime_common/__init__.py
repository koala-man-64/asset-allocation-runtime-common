from asset_allocation_runtime_common.api_gateway_auth import build_access_token_provider
from asset_allocation_runtime_common.backtest_results import (
    BACKTEST_RESULTS_SCHEMA_VERSION,
    persist_backtest_results,
)
from asset_allocation_runtime_common.backtest_repository import BacktestRepository
from asset_allocation_runtime_common.control_plane_transport import (
    ControlPlaneRequestError,
    ControlPlaneTransport,
    ControlPlaneTransportConfig,
)
from asset_allocation_runtime_common.job_metadata import (
    JOB_METADATA_TAGS,
    JobMetadataResolution,
    catalog_job_names,
    expected_job_metadata,
    resolve_job_metadata,
    validate_job_metadata_tags,
)
from asset_allocation_runtime_common.strategy_publication_repository import StrategyPublicationRepository
from asset_allocation_runtime_common.ranking_repository import RankingRepository
from asset_allocation_runtime_common.regime_repository import RegimeRepository
from asset_allocation_runtime_common.results_repository import ResultsRepository
from asset_allocation_runtime_common.strategy_repository import (
    StrategyRepository,
    normalize_strategy_config_document,
)
from asset_allocation_runtime_common.universe_repository import UniverseRepository

__all__ = [
    "BacktestRepository",
    "BACKTEST_RESULTS_SCHEMA_VERSION",
    "ControlPlaneRequestError",
    "ControlPlaneTransport",
    "ControlPlaneTransportConfig",
    "JOB_METADATA_TAGS",
    "JobMetadataResolution",
    "RankingRepository",
    "RegimeRepository",
    "ResultsRepository",
    "StrategyRepository",
    "SymbolEnrichmentRepository",
    "UniverseRepository",
    "build_access_token_provider",
    "catalog_job_names",
    "expected_job_metadata",
    "normalize_strategy_config_document",
    "persist_backtest_results",
    "resolve_job_metadata",
    "validate_job_metadata_tags",
]


def __getattr__(name: str):
    if name == "SymbolEnrichmentRepository":
        from asset_allocation_runtime_common.symbol_enrichment_repository import SymbolEnrichmentRepository

        return SymbolEnrichmentRepository
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
