# Split Repo Handbook

## Local Development

1. Publish or locally install `asset-allocation-contracts` and `asset-allocation-runtime-common`.
2. Work inside one runtime repo at a time.
3. Use compatibility workflows for cross-repo changes instead of sibling source checkouts.

## Versioning Rules

- `asset-allocation-contracts` and `asset-allocation-runtime-common` are semver artifacts.
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

