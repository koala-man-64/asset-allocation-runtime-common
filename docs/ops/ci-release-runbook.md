# CI and Release Runbook

## CI

- `ci.yml` is the required gate for PRs and `main`.
- CI must install the package from the repo itself and run tests without any sibling checkout.
- CI also verifies the package can build a wheel and sdist.

## Release

- `release.yml` publishes the Python package and writes `artifacts/release-manifest.json`.
- The release workflow dispatches `runtime_common_released` to control-plane and jobs.
- Consumer repos should validate the new version through their compatibility workflows before production rollout.

### GitHub configuration bootstrap

The release workflow requires these repository-scoped GitHub settings:

- Variables: `CONTROL_PLANE_REPOSITORY`, `JOBS_REPOSITORY`, `DISPATCH_APP_ID`, `PYTHON_PUBLISH_REPOSITORY_URL`
- Secrets: `DISPATCH_APP_PRIVATE_KEY`, `PYTHON_PUBLISH_USERNAME`, `PYTHON_PUBLISH_PASSWORD`

This repo tracks the expected configuration in `docs/ops/env-contract.csv` and `.env.template`.

#### Prerequisites

- GitHub CLI `gh` installed
- `gh auth login` completed for the target GitHub account
- A PEM file available for `DISPATCH_APP_PRIVATE_KEY`

#### Local setup flow

Preview the generated GitHub env file:

```powershell
.\scripts\setup-env.ps1 -DryRun
```

Write `.env.github` interactively:

```powershell
.\scripts\setup-env.ps1 -DispatchAppPrivateKeyPath C:\path\to\dispatch-app.pem
```

Sync the documented variables and secrets to the current repository:

```powershell
.\scripts\sync-all-to-github.ps1
```

Use `-DryRun` on either script to preview actions without writing the local file or changing GitHub settings.

### Release prep flow

When PyPI already has the current package version, bump the tracked release version references in this repo before rerunning `release.yml`.

Preview the next version without writing files:

```powershell
.\scripts\prepare-release.ps1 -Version 0.1.1 -DryRun
```

Apply the version bump:

```powershell
.\scripts\prepare-release.ps1 -Version 0.1.1
```

The script updates only these tracked files:

- `python/pyproject.toml`
- `docs/architecture/architecture-contract.md`

It does not create commits, create tags, dispatch workflows, touch generated packaging output, or edit downstream repos.

After the script runs:

1. Commit the version change.
2. Rerun the `Runtime Common Release` workflow.
3. Confirm the workflow publishes the new package version instead of retrying the old one.

## Consumer Expectations

- Control-plane and jobs install `asset-allocation-runtime-common==<version>`.
- Consumer release manifests must include the runtime-common version they built against.
- No consumer should fetch runtime-common source by path during CI or release.
