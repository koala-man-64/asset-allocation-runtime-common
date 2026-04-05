# ADR-001: Multirepo Boundaries and Shared Backend Package

## Status

Accepted

## Decision

The split runtime system keeps four product repos and one narrow shared backend package:

- `asset-allocation-contracts`
- `asset-allocation-runtime-common`
- `asset-allocation-control-plane`
- `asset-allocation-jobs`
- `asset-allocation-ui`

`asset-allocation-runtime-common` is the only approved shared backend code package. It owns transport-neutral helpers, read-only control-plane HTTP client repositories, serializers, validators, and pure transforms. It must stay free of Postgres repositories, provider adapters, monitoring collectors, deploy manifests, and orchestration logic.

## Why

Control-plane and jobs had already diverged while still carrying duplicate copies of the same backend client logic. Keeping that code duplicated would make the four-repo split cosmetic instead of real. Pulling the shared read-only client layer into a versioned package reduces drift without pushing mutable domain ownership into a generic shared library.

## Consequences

- Contracts become published Python and TypeScript artifacts.
- Runtime-common becomes a published Python artifact.
- Control-plane remains the only writer for operator and Postgres-owned state.
- Jobs reads control-plane-owned state through HTTP clients from runtime-common.
- UI remains standalone and consumes published TypeScript contracts only.

## Guardrails

- No direct database access from jobs for control-plane-owned domains.
- No provider adapters or monitoring collectors inside runtime-common.
- Any compatibility shim left in a runtime repo must have an owner, retirement date, and removal issue.
- Release manifests must record dependency versions, not sibling checkout refs.

