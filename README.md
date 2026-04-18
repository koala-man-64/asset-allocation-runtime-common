# Asset Allocation Runtime Common

Shared backend package for the split Asset Allocation system.

This repository owns the shared backend foundations that both Python runtimes consume:
- storage and runtime foundation modules
- provider gateway adapters and shared provider helpers
- market-data and domain helper modules
- extracted backtesting engine helpers
- managed-identity bearer token acquisition
- control-plane HTTP transport
- control-plane client repositories for shared runtime state
- pure transforms and normalization helpers used by those flows

It does not own control-plane authority, API routes, monitoring ownership, deploy manifests, or jobs scheduling entrypoints.

## Quickstart

```powershell
python -m pip install -e .\python[test]
python -m pytest .\tests\python -q
```

## Operations

- `release.yml` dispatches `runtime_common_released` only to `asset-allocation-control-plane` and `asset-allocation-jobs`.
- `asset-allocation-ui` is intentionally excluded because it consumes published contracts, not `asset-allocation-runtime-common`.

## Docs

- [Architecture Contract](docs/architecture/architecture-contract.md)
- [Boundary ADR](docs/architecture/adr-001-multirepo-boundaries.md)
- [Ownership Map](docs/architecture/repo-ownership-map.md)
- [Migration Ledger](docs/architecture/migration-ledger.md)
- [Repo Handbook](docs/architecture/repo-handbook.md)
- [CI and Release Runbook](docs/ops/ci-release-runbook.md)
- [Cutover and Rollback Runbook](docs/ops/cutover-rollback-runbook.md)
- [Cutover Release Notes](docs/releases/cutover-release-notes.md)
