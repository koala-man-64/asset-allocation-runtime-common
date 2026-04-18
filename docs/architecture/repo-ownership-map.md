# Repo Ownership Map

## Canonical Owners

| Repo | Owns | Explicitly Does Not Own |
| --- | --- | --- |
| `asset-allocation-contracts` | Python/TypeScript contracts, schemas, compatibility fixtures | Runtime IO, storage, deploy logic |
| `asset-allocation-runtime-common` | Shared backend foundations, provider adapters, market-data helpers, extracted backtesting helpers, control-plane HTTP transport, auth helpers, shared runtime repositories, pure transforms | Control-plane API routes, control-plane authority over DB-backed state, monitoring ownership, jobs scheduling entrypoints |
| `asset-allocation-control-plane` | FastAPI API, control-plane monitoring, Postgres/operator state, deploy manifests, shared ops scripts | Jobs orchestration, shared backend runtime foundations |
| `asset-allocation-jobs` | ETL, jobs orchestration, job-specific monitoring, repo-local runtime entrypoints | Control-plane-owned Postgres state, shared backend runtime foundations, deploy substrate |
| `asset-allocation-ui` | Standalone UI, UI env bootstrap, UI deploy surface | Backend runtime helpers and control-plane internals |

## Temporary Mirrors Approved For One Release Cycle

| Repo | Path | Canonical Owner | Retirement Date | Removal Issue |
| --- | --- | --- | --- | --- |
| `asset-allocation-jobs` | `core/api_gateway_auth.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/control_plane_transport.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/backtest_repository.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/ranking_repository.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/strategy_repository.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/universe_repository.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-jobs` | `core/regime_repository.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-jobs-runtime-common-shims` |
| `asset-allocation-control-plane` | `core/api_gateway_auth.py` | `asset-allocation-runtime-common` | 2026-06-01 | `remove-control-plane-runtime-common-shims` |
