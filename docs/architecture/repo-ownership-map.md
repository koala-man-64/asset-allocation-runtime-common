# Repo Ownership Map

## Canonical Owners

| Repo | Owns | Explicitly Does Not Own |
| --- | --- | --- |
| `asset-allocation-contracts` | Python/TypeScript contracts, schemas, compatibility fixtures | Runtime IO, storage, deploy logic |
| `asset-allocation-runtime-common` | Control-plane HTTP transport, auth helpers, read-only runtime client repositories, pure transforms | Postgres repositories, provider adapters, monitoring, orchestration |
| `asset-allocation-control-plane` | FastAPI API, control-plane monitoring, Postgres/operator state, deploy manifests, shared ops scripts | Jobs orchestration, provider adapters as long-term shared assets |
| `asset-allocation-jobs` | ETL, provider adapters, jobs orchestration, job-specific monitoring | Control-plane-owned Postgres state and deploy substrate |
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

