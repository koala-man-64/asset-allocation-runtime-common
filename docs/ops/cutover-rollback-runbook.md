# Cutover and Rollback Runbook

## Cutover

1. Release `asset-allocation-contracts`.
2. Release `asset-allocation-runtime-common`.
3. Update control-plane and jobs pins if needed.
4. Run control-plane contracts compatibility and runtime-common compatibility.
5. Run jobs control-plane compatibility and runtime-common compatibility.
6. Release control-plane, jobs, and UI with pinned package versions.

## Rollback

- Roll back consumer repos by redeploying the previous image digest.
- If the issue is package-level, repin to the previous `asset-allocation-runtime-common` version and rerun CI.
- Do not hotfix by reintroducing sibling checkout or source-copy behavior.

## Verification

- API health and readiness endpoints succeed.
- One representative job can read control-plane state and complete.
- UI serves `/` and `/config.js`.
- Release manifests show the intended contracts and runtime-common versions.

