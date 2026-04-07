# Asset Allocation Runtime Common

Shared backend package for the split Asset Allocation system.

This repository owns transport-neutral runtime helpers that both Python runtimes consume:
- managed-identity bearer token acquisition
- control-plane HTTP transport
- control-plane client repositories for read-only runtime state
- pure config normalization helpers used by those clients

It does not own Postgres repositories, provider adapters, monitoring collectors, deploy manifests, or job orchestration.

## Quickstart

```powershell
python -m pip install -e .\python[test]
python -m pytest .\tests\python -q
```

## Docs

- [Architecture Contract](docs/architecture/architecture-contract.md)
- [Boundary ADR](docs/architecture/adr-001-multirepo-boundaries.md)
- [Ownership Map](docs/architecture/repo-ownership-map.md)
- [Migration Ledger](docs/architecture/migration-ledger.md)
- [Repo Handbook](docs/architecture/repo-handbook.md)
- [CI and Release Runbook](docs/ops/ci-release-runbook.md)
- [Cutover and Rollback Runbook](docs/ops/cutover-rollback-runbook.md)
- [Cutover Release Notes](docs/releases/cutover-release-notes.md)
