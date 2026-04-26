# CI and Release Runbook

## CI

- `ci.yml` is the required gate for PRs and `main`.
- CI must install the package from the repo itself and run tests without any sibling checkout.
- CI also verifies the package can build a wheel and sdist.
- CI verifies that `python/pyproject.toml` pins `asset-allocation-contracts` exactly, that the pinned version resolves from the configured package index, and that the built wheel and sdist metadata declare the same exact pin.

- `security.yml` also verifies that the declared exact contracts pin in `python/pyproject.toml` resolves before running `pip-audit`.

## Release

- `release.yml` is a manually dispatched workflow that releases from `main`.
- It reuses an already-prepared release version when possible; otherwise it bumps the requested semver segment, updates the tracked release files, commits the bump, creates the release tag, publishes the Python package, and writes `artifacts/release-manifest.json`.
- The release workflow dispatches `runtime_common_released` to jobs.
- Consumer repos should validate any repinned runtime-common version through their normal CI and release checks before production rollout.

### GitHub configuration bootstrap

The release workflow requires these repository-scoped GitHub settings:

- Variables: `JOBS_REPOSITORY`, `DISPATCH_APP_ID`, `PYTHON_PUBLISH_REPOSITORY_URL`
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

### Release flow

Dispatch `Runtime Common Release` from `main` and choose the semver increment the workflow should apply if it needs to create a new version:

- `patch` for internal fixes with no consumer-visible contract change
- `minor` for additive compatible surface changes
- `major` for breaking contract changes

If the tracked version in `python/pyproject.toml` is already ahead of PyPI, or if `main` is already tagged for that exact version, the workflow reuses it instead of bumping again. Uploads use `twine --skip-existing`, so reruns can recover from partial failures without tripping on duplicate-file errors.

When the workflow needs a new release version, it updates only these tracked files before tagging and publishing:

- `python/pyproject.toml`
- `docs/architecture/architecture-contract.md`

### Local release metadata preview

`scripts/prepare-release.ps1` remains the tracked-file updater used by the workflow. Use it locally only to preview or debug the release metadata change.

Preview the next version locally without writing files:

```powershell
.\scripts\prepare-release.ps1 -Version 0.1.1 -DryRun
```

Apply the version bump locally:

```powershell
.\scripts\prepare-release.ps1 -Version 0.1.1
```

It does not create commits, create tags, dispatch workflows, touch generated packaging output, or edit downstream repos.

After a local dry-run or debug run, discard or keep the file changes intentionally before running `Runtime Common Release`. The normal operator flow is to let the workflow own the bump, commit, tag, publish, and downstream dispatch.

## Consumer Expectations

- Control-plane and jobs install `asset-allocation-runtime-common==<version>`.
- Consumer release manifests must include the runtime-common version they built against.
- No consumer should fetch runtime-common source by path during CI or release.
