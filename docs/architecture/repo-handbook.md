# Split Repo Handbook

## Canonical Architecture Contract

`docs/architecture/architecture-contract.md` is the canonical living contract for this repository. Use it as the primary reference for repo purpose, package boundaries, public runtime behavior, and future-agent guardrails.

The other architecture docs in this folder remain authoritative as supporting evidence for specific decisions, ownership boundaries, and migration history. They do not replace the contract as the single source of truth for intended repository design.

## Local Development

1. Publish or locally install `asset-allocation-contracts` and `asset-allocation-runtime-common`.
2. Work inside one runtime repo at a time.
3. Prefer `asset_allocation_runtime_common.foundation`, `providers`, `market_data`, `backtesting`, and `domain` imports over recreating shared backend modules in consumer repos.
4. Use compatibility workflows for cross-repo changes instead of sibling source checkouts.

## Versioning Rules

- `asset-allocation-contracts` and `asset-allocation-runtime-common` are semver artifacts.
- `asset-allocation-runtime-common` currently declares `asset-allocation-contracts==3.11.0` as its exact shared-package dependency, and CI verifies that the pin resolves from the configured package index and propagates into built distribution metadata.
- Changing the contracts pin is an intentional source change and must line up with a published `asset-allocation-contracts` release.
- The `Runtime Common Release` workflow owns the tracked `asset-allocation-runtime-common` version bump and release tag creation from `main`; the operator selects the semver increment when dispatching the workflow.
- Control-plane, jobs, and UI pin exact dependency versions in manifests.
- Release manifests record at least the current repo version plus shared dependency versions.

## Release Topology

- Build once in each repo.
- Push immutable artifacts.
- Promote by digest or version only.
- Record the deployed version matrix in each repo's `release-manifest.json`.

## Stop-Ship Conditions

- Any `file:` dependency to a sibling repo.
- Any Dockerfile that copies a sibling repo as build input.
- Any workflow that checks out a sibling repo for normal CI or release.
- Any control-plane-owned domain accessed from jobs storage code.
- Any tracked generated output or scratch artifact.
