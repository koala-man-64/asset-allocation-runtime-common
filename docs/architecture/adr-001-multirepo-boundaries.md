# ADR-001: Multirepo Boundaries and Shared Backend Package

## Status

Accepted

## Decision

The split runtime system keeps four product repos and one shared backend package:

- `asset-allocation-contracts`
- `asset-allocation-runtime-common`
- `asset-allocation-control-plane`
- `asset-allocation-jobs`
- `asset-allocation-ui`

`asset-allocation-runtime-common` is the only approved shared backend code package. It owns shared backend foundations, provider adapters, market-data helpers, extracted backtesting helpers, control-plane HTTP client repositories, serializers, validators, and pure transforms. It must not own control-plane API routes, control-plane authority over DB-backed state, monitoring ownership, deploy manifests, or jobs scheduling entrypoints.

## Why

Control-plane and jobs had already diverged while still carrying duplicate copies of the same backend runtime internals. Keeping that code duplicated would make the split cosmetic instead of real. Pulling the shared backend foundations into a versioned package reduces drift while still keeping repo-local authority and entrypoints in the owning repos.

## Consequences

- Contracts become published Python and TypeScript artifacts.
- Runtime-common becomes a published Python artifact.
- Control-plane remains the only authority for operator and DB-backed control-plane state.
- Jobs consumes shared backend foundations from runtime-common while keeping orchestration entrypoints local.
- UI remains standalone and consumes published TypeScript contracts only.

## Guardrails

- No direct database ownership transfer away from control-plane for control-plane-owned domains.
- No control-plane API routes, monitoring ownership, or jobs scheduling entrypoints inside runtime-common.
- Any compatibility shim left in a runtime repo must have an owner, retirement date, and removal issue.
- Release manifests must record dependency versions, not sibling checkout refs.
