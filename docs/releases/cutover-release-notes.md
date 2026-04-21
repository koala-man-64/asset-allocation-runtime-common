# Cutover Release Notes

## Delivered

- Introduced `asset-allocation-runtime-common` as the canonical shared backend package.
- Moved read-only control-plane HTTP client repositories and auth helper logic into that package.
- Replaced sibling-repo dependency assumptions in consumer manifests, Dockerfiles, and CI/release workflows.
- Pinned the declared `asset-allocation-contracts` runtime-common dependency to the exact `3.0.0` release.
- Added ownership, migration, and cutover documentation for the split-repo topology.

## Operational Notes

- Temporary compatibility shims remain in control-plane and jobs for one release cycle only.
- Release manifests now record shared dependency versions instead of sibling repo refs.
- Any remaining sibling checkout in normal CI or release is a regression and should fail review.
