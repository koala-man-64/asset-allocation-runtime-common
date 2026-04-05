# CI and Release Runbook

## CI

- `ci.yml` is the required gate for PRs and `main`.
- CI must install the package from the repo itself and run tests without any sibling checkout.
- CI also verifies the package can build a wheel and sdist.

## Release

- `release.yml` publishes the Python package and writes `artifacts/release-manifest.json`.
- The release workflow dispatches `runtime_common_released` to control-plane and jobs.
- Consumer repos should validate the new version through their compatibility workflows before production rollout.

## Consumer Expectations

- Control-plane and jobs install `asset-allocation-runtime-common==<version>`.
- Consumer release manifests must include the runtime-common version they built against.
- No consumer should fetch runtime-common source by path during CI or release.

