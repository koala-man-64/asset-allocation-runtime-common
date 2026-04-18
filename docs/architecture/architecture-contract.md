# Architecture Contract

## Document Control

| Field | Value |
| --- | --- |
| Status | Active |
| Applies To | `asset-allocation-runtime-common` repository and published Python package |
| Canonical Baseline | `main` branch, package version `2.0.3` in `python/pyproject.toml` |
| Owner | Repository code owner `@rdprokes` from `.github/CODEOWNERS` |
| Last Reviewed | 2026-04-18 |
| Change Class | Decision Capture |

Evidence status used in this document:
- `Verified`: directly supported by tracked code, tests, workflows, or committed architecture docs in this repo.
- `Inferred`: reasonable conclusion from tracked evidence, but not stated directly in this repo.
- `Unverified / Needs confirmation`: important unknown that should not be treated as a settled invariant.

## 1. Purpose and Scope

This document is the authoritative architecture and functionality contract for `asset-allocation-runtime-common`. It defines what this repository exists to do, what it must keep doing, what it must not absorb, and how future changes are expected to preserve package behavior for its consumers.

This repository is a versioned Python package. It is not a deployable service, a control-plane API repo, a jobs scheduler, or an infrastructure repo. Its scope is the shared backend package consumed by the split Asset Allocation Python runtimes: foundation modules, provider adapters, market-data helpers, extracted backtesting helpers, shared runtime repositories, transport/auth, and pure transforms. Supporting docs such as the boundary ADR, ownership map, migration ledger, runbooks, and release notes remain valid, but they are supporting evidence rather than peer authorities.

## 2. Why This Repo Exists

The split Asset Allocation system uses five repositories:
- `asset-allocation-contracts`
- `asset-allocation-runtime-common`
- `asset-allocation-control-plane`
- `asset-allocation-jobs`
- `asset-allocation-ui`

`asset-allocation-runtime-common` exists because `asset-allocation-control-plane` and `asset-allocation-jobs` had already diverged while still carrying duplicate copies of backend runtime logic. The intended design is to centralize the real shared backend layer here: managed-identity token acquisition, control-plane HTTP transport, shared runtime repositories, storage/runtime foundations, provider adapters, market-data helpers, extracted backtesting helpers, and pure normalization helpers. This reduces drift without transferring control-plane authority or jobs entrypoints into a generic shared service.

## 3. System Context and External Boundaries

This package sits between the two Python runtime repos and the control-plane HTTP surface:

1. Consumer code in `asset-allocation-control-plane` or `asset-allocation-jobs` installs a pinned version of this package.
2. Consumer code instantiates transport and repository objects from this package.
3. This package acquires a managed-identity bearer token, constructs HTTP requests to the control-plane internal API, and returns JSON payloads or raises a transport error.
4. This package may contain shared persistence and provider helpers, but it does not own control-plane API routes, repo-local authority over DB-backed state, or jobs scheduling entrypoints.

Boundary summary:

| Boundary | Relationship |
| --- | --- |
| `asset-allocation-contracts` | Peer shared package for cross-language contracts; not owned here |
| `asset-allocation-control-plane` | Downstream consumer and canonical owner of control-plane state and internal API behavior |
| `asset-allocation-jobs` | Downstream consumer that installs this package for shared backend foundations and runtime state access |
| `asset-allocation-ui` | Separate repo; not a consumer of this Python package |
| Control-plane internal HTTP API | Trust boundary crossed by this package via `httpx` and bearer authentication |

This repository explicitly does not own control-plane API routes, control-plane authority over DB-backed state, monitoring ownership, deploy manifests, or jobs scheduling entrypoints.

## 4. Architectural Principles and Invariants

### Shared Backend Boundary
**Contract**

This repository must remain the shared backend package for Python consumers. Code added here must preserve a real shared backend invariant across at least two Python repos. It must not become a general-purpose helper bucket or a shadow control-plane/jobs runtime.

**Why**

The package exists to remove duplicated backend foundations while preserving clear ownership boundaries across the split repos. The risk is no longer sharing too little; it is centralizing the wrong authority here.

**Evidence**

- `Verified`: `README.md`
- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`

**Change Impact**

Any change that adds a new top-level module, new export, or new dependency must be reviewed against this boundary before merge.

### Control-Plane Ownership Must Stay Intact
**Contract**

This package must preserve control-plane ownership of runtime state. Ranking, regime, strategy, and universe repositories must remain control-plane HTTP clients rather than storage owners or alternate writers.

**Why**

The split-repo design keeps operator and control-plane-owned state in `asset-allocation-control-plane` and forces jobs to consume that state over HTTP rather than through direct storage access.

**Evidence**

- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`
- `Verified`: `python/asset_allocation_runtime_common/ranking_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/regime_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/universe_repository.py`

**Change Impact**

If any repository begins writing control-plane-owned state or bypassing the HTTP boundary, the ownership map, ADR, tests, and downstream compatibility expectations must all be revisited.

### Read-Mostly Repositories With One Explicit Exception
**Contract**

Regime, strategy, and universe repositories are read-only facades over control-plane HTTP. Ranking schema mutations remain blocked, but `RankingRepository` now also carries the ranking-refresh claim/complete/fail workflow. `BacktestRepository` continues to own backtest run lifecycle calls, and `ResultsRepository` exposes the semantic reconcile trigger for ranking freshness and canonical backtest freshness.

**Why**

The code and tests enforce blocked mutations for most domain repositories, while backtest run control is currently implemented as an operational workflow over the same transport layer.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/backtest_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/ranking_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/regime_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/universe_repository.py`
- `Verified`: `tests/python/test_backtest_repository.py`
- `Verified`: `tests/python/test_ranking_repository.py`
- `Verified`: `tests/python/test_strategy_repository.py`
- `Verified`: `tests/python/test_universe_repository.py`

**Change Impact**

If the backtest write exception is removed, expanded, or reclassified, this document, downstream consumers, and the ownership docs must be updated together.

### No Repo-Local Authority Or EntryPoints
**Contract**

This repository may own shared storage/runtime/provider helpers, but it must not absorb control-plane API routes, control-plane authority over DB-backed owner state, monitoring ownership, deploy logic, or jobs scheduling/orchestration entrypoints.

**Why**

Those concerns still have clear homes in the other split repos. Moving authority or entrypoints here would reintroduce hidden coupling and blur release ownership even if the underlying helper logic is shared.

**Evidence**

- `Verified`: `README.md`
- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`

**Change Impact**

Any proposed addition in these categories is a boundary change, not a routine refactor, and requires a decision update before implementation.

### Semver-Governed Public Surface
**Contract**

The published package surface is semver-governed. Public exports, consumer-visible request behavior, normalization behavior, error semantics, supported Python version, and runtime dependencies are part of the compatibility contract.

**Why**

This package is released independently and consumed by version-pinned downstream repos. Compatibility must be assessed from the consumer point of view, not only from internal implementation shape.

**Evidence**

- `Verified`: `docs/architecture/repo-handbook.md`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `.github/workflows/release.yml`
- `Verified`: `python/asset_allocation_runtime_common/__init__.py`
- `Verified`: `python/pyproject.toml`

**Change Impact**

Any change to the public surface or consumer-visible behavior requires version-impact review, test review, and release-note review.

## 5. Canonical Component Model

### Public Component Inventory
**Contract**

The package currently exposes the following public surface and responsibilities:

| Public Component | Responsibility |
| --- | --- |
| `build_access_token_provider` | Acquire and cache managed-identity bearer tokens for the control-plane API |
| `ControlPlaneTransportConfig` | Hold base URL, API scope, and timeout settings for transport creation |
| `ControlPlaneTransport` | Build authenticated HTTP requests, normalize environment-derived config, and return JSON payloads |
| `ControlPlaneRequestError` | Represent non-404 transport failures with status code and detail |
| `foundation.*` | Shared storage/runtime foundations including blob, config, runtime-config, logging, manifests, and Postgres helpers |
| `providers.*` | Shared Alpha Vantage and Massive gateway/provider adapters |
| `market_data.*` | Shared market-data layout, artifact, reconciliation, and pipeline helpers |
| `domain.regime` | Shared regime domain model and transforms |
| `backtesting.*` | Shared backtest result persistence and extracted runtime engine helpers |
| `BacktestRepository` | Read and manage backtest run lifecycle over control-plane HTTP |
| `RankingRepository` | Read ranking schema state and manage ranking-refresh claims over control-plane HTTP |
| `ResultsRepository` | Trigger semantic results freshness reconcile over control-plane HTTP |
| `RegimeRepository` | Read regime model and current regime state over control-plane HTTP |
| `StrategyRepository` | Read strategy state over control-plane HTTP and normalize strategy config documents |
| `UniverseRepository` | Read universe configuration state over control-plane HTTP |
| `normalize_strategy_config_document` | Remove disabled strategy substructures from returned config payloads |

**Why**

Future agents need one stable inventory of what the package publicly offers before deciding whether a change is internal, additive, or breaking.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/__init__.py`
- `Verified`: `python/asset_allocation_runtime_common/api_gateway_auth.py`
- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: `python/asset_allocation_runtime_common/backtest_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/ranking_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/regime_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/universe_repository.py`

**Change Impact**

Adding, removing, or repurposing any listed component changes the public package contract and requires updates to Sections 7, 8, 9, 11, and 13.

### Auth and Transport Form the Shared Runtime Spine
**Contract**

`build_access_token_provider`, `ControlPlaneTransportConfig`, `ControlPlaneTransport`, and `ControlPlaneRequestError` are the shared runtime spine of this package. Repository classes must rely on this transport layer rather than reimplementing auth, base URL normalization, or HTTP failure semantics independently.

**Why**

Shared transport behavior is the common invariant this package was created to centralize.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/api_gateway_auth.py`
- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: repository modules in `python/asset_allocation_runtime_common/*.py`
- `Verified`: `tests/python/test_control_plane_transport.py`

**Change Impact**

Changes here affect every repository class and should be treated as high-blast-radius package changes.

### Strategy Normalization Is a Deliberate Transform
**Contract**

`normalize_strategy_config_document` is a deliberate behavior of the package rather than incidental cleanup. Strategy reads must continue to return configs with disabled `regimePolicy` and disabled exit rules removed, and retained rules must not carry an `enabled` flag.

**Why**

This is the only explicit pure transform implemented in the package today, and tests assert the normalized output shape directly.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `tests/python/test_strategy_repository.py`

**Change Impact**

Any change to this transform is a consumer-visible behavior change and must be versioned and retested accordingly.

## 6. Runtime Flows

### Consumer Installation and Contracts Version Floor
**Contract**

Consumer repos must consume this package as a versioned dependency rather than through sibling source checkout or vendoring. Within this repo, `asset-allocation-contracts>=2.1.0` is declared as the current minimum stable version in `python/pyproject.toml`, and CI plus security verification only require that the declared requirement resolves from the configured package index.

**Why**

The split-repo operating model depends on versioned artifacts and release manifests, not cross-repo source coupling.

**Evidence**

- `Verified`: `docs/architecture/repo-handbook.md`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `docs/ops/cutover-rollback-runbook.md`
- `Verified`: `docs/releases/cutover-release-notes.md`

**Change Impact**

Any change that assumes sibling checkout or source-copy behavior is a stop-ship regression against the current release model.

### Environment Bootstrap and Transport Construction
**Contract**

`ControlPlaneTransport.from_env()` is the standard package bootstrap path. It requires `ASSET_ALLOCATION_API_BASE_URL` and `ASSET_ALLOCATION_API_SCOPE`, strips a trailing `/api` from the base URL, and uses `ASSET_ALLOCATION_API_TIMEOUT_SECONDS` with a floor of five seconds and a fallback of thirty seconds.

**Why**

Tests assert the normalized base URL behavior, and the code defines the package's environment-based transport contract.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: `tests/python/test_control_plane_transport.py`

**Change Impact**

Changing env var names, base URL normalization, or timeout behavior affects every consumer bootstrap path and requires compatibility review.

### Managed Identity Token Acquisition and Caching
**Contract**

Auth uses `DefaultAzureCredential` with interactive browser credential disabled. Access tokens are cached and refreshed when less than sixty seconds from expiry.

**Why**

This is the current package auth behavior and the reason consumers do not own token handling themselves.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/api_gateway_auth.py`
- `Unverified / Needs confirmation`: there are no direct tests for token caching, refresh skew, empty-token handling, or concurrency behavior

**Change Impact**

Any change to credential type, token caching behavior, or failure semantics requires consumer review and new direct tests.

### Request Header Construction and Transport Failure Handling
**Contract**

Each HTTP request must include `Authorization: Bearer <token>` and `Accept: application/json`. When present in the environment, `CONTAINER_APP_JOB_NAME` and `CONTAINER_APP_JOB_EXECUTION_NAME` must be forwarded as `X-Caller-Job` and `X-Caller-Execution`. Non-404 HTTP failures must surface as `ControlPlaneRequestError` with extracted detail; empty response bodies must return `None`.

**Why**

These behaviors are part of the observable transport contract and are asserted by tests.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: `tests/python/test_control_plane_transport.py`

**Change Impact**

Header, error, or empty-body changes alter downstream observability and failure handling expectations and require test updates.

### Read-Only Repository Access Flow
**Contract**

Repository reads follow this flow: repository method -> authenticated transport call to `/api/internal/...` -> JSON payload -> optional package-level normalization -> dictionary, list, or `None` returned to the caller. For the read paths that implement it, HTTP 404 is modeled as a not-found result rather than a hard failure.

**Why**

This is the core consumer flow that the package centralizes across domains.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/backtest_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/ranking_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/regime_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/universe_repository.py`
- `Verified`: tests under `tests/python/`

**Change Impact**

Any change to path construction, request methods, `None` semantics, or payload passthrough behavior is a package contract change.

### Strategy Config Normalization Flow
**Contract**

Strategy reads must normalize `config` payloads before returning them to consumers by removing disabled regime policies, removing disabled exit rules, and stripping `enabled` from retained structures.

**Why**

The package intentionally returns a normalized config view rather than the raw control-plane payload.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `tests/python/test_strategy_repository.py`

**Change Impact**

Changing normalization output changes consumer-visible strategy behavior and must be versioned, documented, and retested.

### Backtest Lifecycle Exception Flow
**Contract**

`BacktestRepository` currently performs both reads and operational writes over control-plane HTTP: claim next run, start run, heartbeat, complete run, and fail run. `RankingRepository` now also performs operational ranking-refresh claim/complete/fail calls, and `ResultsRepository` triggers the control-plane semantic reconcile pass.

**Why**

This behavior is present in code and tests even though the broader package pattern is read-mostly. It is therefore an explicit exception that future changes must either preserve or consciously retire.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/backtest_repository.py`
- `Verified`: `tests/python/test_backtest_repository.py`
- `Inferred`: the `dsn` constructor parameter retained across repository classes appears to be a migration-era compatibility shape rather than an active database dependency

**Change Impact**

If backtest operations are moved, expanded, or reclassified, downstream workflow assumptions and the ownership contract need an explicit decision update.

## 7. Data, State, and Interface Contracts

### Public Export Surface
**Contract**

The public package surface is the export list in `python/asset_allocation_runtime_common/__init__.py`. Consumers should treat anything not exported there as internal unless a later contract update states otherwise.

Current exports:

| Export | Category |
| --- | --- |
| `BacktestRepository` | Repository |
| `ControlPlaneRequestError` | Error type |
| `ControlPlaneTransport` | Transport |
| `ControlPlaneTransportConfig` | Transport config |
| `RankingRepository` | Repository |
| `RegimeRepository` | Repository |
| `ResultsRepository` | Repository |
| `StrategyRepository` | Repository |
| `UniverseRepository` | Repository |
| `build_access_token_provider` | Auth helper |
| `normalize_strategy_config_document` | Pure transform |

**Why**

This is the narrowest reliable inventory of what the package publishes today.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/__init__.py`

**Change Impact**

Export changes are public API changes and require versioning, release notes, and contract updates.

### Environment and Header Contract
**Contract**

The transport contract currently uses these environment variables:

| Environment Variable | Behavior |
| --- | --- |
| `ASSET_ALLOCATION_API_BASE_URL` | Required; base URL with optional trailing `/api` stripped |
| `ASSET_ALLOCATION_API_SCOPE` | Required; bearer token scope |
| `ASSET_ALLOCATION_API_TIMEOUT_SECONDS` | Optional; timeout floor of five seconds, fallback of thirty seconds |
| `CONTAINER_APP_JOB_NAME` | Optional; forwarded as `X-Caller-Job` |
| `CONTAINER_APP_JOB_EXECUTION_NAME` | Optional; forwarded as `X-Caller-Execution` |

**Why**

These names and behaviors are consumer-visible and partly asserted by tests.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: `tests/python/test_control_plane_transport.py`

**Change Impact**

Env var or header changes require downstream rollout coordination because they affect deployment-time configuration and request tracing.

### Endpoint Families and Request Shapes
**Contract**

The package currently targets internal control-plane endpoint families under `/api/internal/...`:

| Domain | Endpoint Family |
| --- | --- |
| Backtests | `/api/internal/backtests/runs/...` |
| Rankings | `/api/internal/rankings...` |
| Regimes | `/api/internal/regimes/...` |
| Results | `/api/internal/results/...` |
| Strategies | `/api/internal/strategies...` |
| Universes | `/api/internal/universes...` |

Tests directly assert selected HTTP methods, query params, and request bodies, including:
- `POST /api/internal/backtests/runs/claim` with `{"executionName": ...}`
- `POST /api/internal/backtests/runs/{run_id}/start`
- `POST /api/internal/backtests/runs/{run_id}/heartbeat`
- `POST /api/internal/backtests/runs/{run_id}/complete`
- `POST /api/internal/backtests/runs/{run_id}/fail`
- `GET /api/internal/rankings/{name}/revision?version=...`
- `GET /api/internal/strategies/{name}/revision?version=...`

**Why**

These path and payload details are part of the behavioral contract because the test suite encodes them explicitly.

**Evidence**

- `Verified`: repository modules in `python/asset_allocation_runtime_common/`
- `Verified`: `tests/python/test_backtest_repository.py`
- `Verified`: `tests/python/test_ranking_repository.py`
- `Verified`: `tests/python/test_strategy_repository.py`

**Change Impact**

Endpoint or payload changes require compatibility validation against the control-plane API and downstream consumers.

### Not-Found and Error Semantics
**Contract**

For repository read paths that explicitly implement it, 404 responses return `None`. Other HTTP failures raise `ControlPlaneRequestError` with a status code and extracted detail string.

**Why**

This package currently distinguishes not-found from transport failure on behalf of consumers.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: repository modules in `python/asset_allocation_runtime_common/`
- `Verified`: `tests/python/test_control_plane_transport.py`
- `Verified`: `tests/python/test_strategy_repository.py`
- `Verified`: `tests/python/test_universe_repository.py`

**Change Impact**

Any change to `None` vs exception behavior is breaking for consumers that branch on these outcomes.

### Mutation Blocking and Current Write Exception
**Contract**

Mutation methods that would let jobs write control-plane-owned ranking schema, regime, strategy, or universe definitions must remain blocked via `NotImplementedError` unless ownership changes are explicitly approved. Current operational exceptions are limited to `BacktestRepository` run lifecycle calls, `RankingRepository` ranking-refresh claim/complete/fail calls, and `ResultsRepository` semantic reconcile triggers.

**Why**

This is the code-level enforcement of the split-repo ownership model.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/ranking_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/regime_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/strategy_repository.py`
- `Verified`: `python/asset_allocation_runtime_common/universe_repository.py`
- `Verified`: `tests/python/test_ranking_repository.py`
- `Verified`: `tests/python/test_strategy_repository.py`
- `Verified`: `tests/python/test_universe_repository.py`
- `Unverified / Needs confirmation`: whether the backtest write path is a permanent design choice or a migration-era exception

**Change Impact**

Unblocking writes is an ownership decision and requires aligned updates to architecture docs, tests, and consumer expectations.

## 8. Operational Qualities

### Python, Runtime, and Dependency Constraints
**Contract**

This package currently targets Python `>=3.14,<3.15` and runtime dependencies `azure-identity==1.25.2` and `httpx==0.28.1`. The repo also declares `asset-allocation-contracts>=2.1.0` as the current minimum stable version, and CI plus security verification only require that the declared requirement resolves. Test-only dependencies are declared separately.

**Why**

The published artifact contract includes supported Python and dependency versions.

**Evidence**

- `Verified`: `python/pyproject.toml`

**Change Impact**

Runtime or dependency changes require compatibility review for both consumer repos and may require coordinated release planning.

### CI, Build, and Local Validation Responsibilities
**Contract**

CI must install the package from this repo, run `ruff`, run `pytest tests/python -q`, and verify wheel and sdist buildability without sibling checkout.

**Why**

This is the current isolated validation baseline for the package and part of the split-repo stop-ship model.

**Evidence**

- `Verified`: `.github/workflows/ci.yml`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `docs/architecture/repo-handbook.md`

**Change Impact**

Reducing this validation weakens confidence in the published artifact and must be treated as a release-quality decision.

### Release and Downstream Dispatch Behavior
**Contract**

The manually dispatched release workflow runs from `main`, determines whether the tracked package version can be reused or must be bumped according to the requested semver increment, updates the tracked release baseline when needed, creates the release tag, builds and publishes the Python package, writes `artifacts/release-manifest.json`, and dispatches `runtime_common_released` to jobs. Consumer repos are expected to pin exact versions and record them in their release manifests.

**Why**

The package is meant to participate in a versioned release matrix rather than ad hoc source sharing.

**Evidence**

- `Verified`: `.github/workflows/release.yml`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `docs/architecture/repo-handbook.md`
- `Verified`: `docs/ops/cutover-rollback-runbook.md`

**Change Impact**

Release-contract changes affect downstream upgrade flow, rollback flow, and artifact traceability.

### Security and Dependency Audit Posture
**Contract**

The repo currently runs a separate dependency audit workflow using `pip-audit` on pull requests, pushes to `main`, manual dispatch, and a weekly schedule. CI and the security workflow both verify that the declared `asset-allocation-contracts` dependency requirement resolves before install or audit. Raising the minimum version floor remains an intentional source change, but installers may select newer matching published versions without changing this repo.

**Why**

This is the only dedicated security workflow defined in this repo today.

**Evidence**

- `Verified`: `.github/workflows/security.yml`

**Change Impact**

Changing or removing this workflow changes the repo's minimum dependency-risk posture and should be documented.

### Current Reliability and Observability Behavior
**Contract**

The package currently provides deterministic request construction, bearer auth, explicit exception surfacing, and caller identity header forwarding. It does not currently define metrics, tracing, or structured logging contracts beyond the token refresh log entry in `api_gateway_auth.py`.

**Why**

Future agents should not assume broader observability guarantees than the package actually provides today.

**Evidence**

- `Verified`: `python/asset_allocation_runtime_common/api_gateway_auth.py`
- `Verified`: `python/asset_allocation_runtime_common/control_plane_transport.py`
- `Verified`: `tests/python/test_control_plane_transport.py`

**Change Impact**

Adding retries, fallback logic, richer logging, or tracing changes runtime behavior and should not be treated as incidental refactoring.

### Known Validation Gap: No Cross-Repo Consumer Compatibility Gate Yet
**Contract**

This repo currently validates itself in isolation. It does not, by itself, prove that a candidate wheel installs into `asset-allocation-jobs` and passes its compatibility suite, and control-plane adoption is coordinated through pinned published versions rather than a downstream dispatch receiver.

**Why**

The current CI is mocked-HTTP contract coverage for this package, not end-to-end consumer compatibility coverage.

**Evidence**

- `Verified`: `.github/workflows/ci.yml`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `docs/ops/cutover-rollback-runbook.md`
- `Verified`: `docs/architecture/migration-ledger.md`

**Change Impact**

Future release process changes should prefer adding this gate rather than weakening the existing isolated test/build contract.

## 9. Change Guardrails for Future Agents

### Inclusion Test for New Code
**Contract**

New code belongs in this repo only if it satisfies all of the following:
1. It encodes a stable runtime invariant shared by at least two real Python consumers.
2. It does not take ownership of control-plane state, provider logic, storage logic, monitoring, deployment, or orchestration.
3. It can be validated as part of this package's semver-governed public or internal runtime contract.

If those conditions are not met, the code belongs elsewhere even if duplication remains.

**Why**

This package is safer as a small compatibility boundary than as a convenience layer.

**Evidence**

- `Verified`: `README.md`
- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`
- `Verified`: `docs/architecture/migration-ledger.md`

**Change Impact**

Borderline additions should be rejected or escalated into a decision update before implementation.

### Anti-Goals
**Contract**

Future agents must not:
- create a generic `utils` bucket
- add control-plane API routes
- add control-plane authority over DB-backed owner state
- add monitoring ownership
- add jobs scheduling entrypoints
- add deploy manifests or runtime bootstrap logic
- add sibling-repo source coupling back into CI or release

**Why**

These are the fastest ways to erase the split-repo boundary the package was introduced to protect.

**Evidence**

- `Verified`: `README.md`
- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`
- `Verified`: `docs/architecture/repo-handbook.md`
- `Verified`: `docs/ops/ci-release-runbook.md`

**Change Impact**

Any proposal in these categories is a design-boundary review, not routine implementation work.

### Semver and Deprecation Rules
**Contract**

Use these default release-impact rules unless a later decision register entry replaces them:
- Patch: internal refactor or bug fix with no public export change and no consumer-visible behavior change
- Minor: additive compatible API or behavior that does not invalidate existing consumer usage
- Major: removed or renamed exports, changed endpoint expectations, changed error or `None` semantics, changed normalization output, changed supported Python/runtime dependency contract, or any other consumer-visible break

Prefer deprecate-then-remove for supported public surface. If a breaking removal is unavoidable, record it in the decision register and release notes before release.

**Why**

This package is explicitly a semver artifact, and consumer safety depends on version numbers matching actual compatibility impact.

**Evidence**

- `Verified`: `docs/architecture/repo-handbook.md`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `.github/workflows/release.yml`
- `Verified`: `python/asset_allocation_runtime_common/__init__.py`
- `Verified`: tests under `tests/python/`

**Change Impact**

Versioning mistakes create hidden downstream breakage and complicate rollback.

### Documentation, Tests, and Release Notes Must Move Together
**Contract**

When public behavior changes, the same change set must update:
- this contract
- affected tests in `tests/python/`
- release notes or release-facing summary
- versioning decisions when compatibility impact changes

Documentation-only changes may omit test changes, but behavior changes must not land without contract and validation updates.

**Why**

The value of this document depends on it staying synchronized with the package and release process.

**Evidence**

- `Verified`: `.github/workflows/ci.yml`
- `Verified`: `.github/workflows/release.yml`
- `Verified`: `docs/ops/ci-release-runbook.md`
- `Verified`: `docs/releases/cutover-release-notes.md`

**Change Impact**

Changes that skip one of these surfaces create drift between package behavior, tests, and operating guidance.

### Prefer Duplication Over the Wrong Abstraction
**Contract**

When a shared abstraction is still speculative, prefer temporary duplication in consumer repos over moving unstable or consumer-specific behavior into this package. Promote logic here only after the shared invariant is real; extract logic back out if it starts needing consumer flags, environment branching, or unrelated dependencies.

**Why**

The repository's long-term risk is overreach, not under-sharing.

**Evidence**

- `Verified`: `docs/architecture/adr-001-multirepo-boundaries.md`
- `Verified`: `docs/architecture/migration-ledger.md`
- `Verified`: `docs/architecture/repo-ownership-map.md`

**Change Impact**

Future cleanup work should use this rule to decide whether to share more code or retire a bad abstraction.

## 10. Known Gaps and Open Questions

| Topic | Status | Evidence |
| --- | --- | --- |
| Exact hosting/runtime platform for `asset-allocation-control-plane` consumers of this package | `Unverified / Needs confirmation` | This repo documents the package boundary but does not define the full control-plane deployment runtime |
| Exact hosting/runtime platform for `asset-allocation-jobs` | `Inferred` | `CONTAINER_APP_JOB_NAME` and `CONTAINER_APP_JOB_EXECUTION_NAME` suggest Azure Container Apps Jobs, but this repo does not confirm that directly |
| Exact Python package registry used by release | `Unverified / Needs confirmation` | `.github/workflows/release.yml` uses repo variables and secrets but does not hard-code the target registry |
| Remaining shim status in consumer repos | `Unverified / Needs confirmation` | `docs/architecture/repo-ownership-map.md` and `docs/architecture/migration-ledger.md` record temporary mirrors, but this repo cannot prove their current removal state in sibling repos |
| Whether backtest write operations are permanent or transitional | `Unverified / Needs confirmation` | Code and tests prove they exist; repo docs do not yet classify them as permanent target-state behavior |
| Direct test coverage for `build_access_token_provider` | `Verified` | `python/asset_allocation_runtime_common/api_gateway_auth.py` is public, but there are no direct tests covering scope validation, caching, refresh skew, empty-token failure, or concurrency |
| Direct contract coverage for several repository methods | `Verified` | Existing tests do not directly cover every public method such as `get_run`, `get_ranking_schema`, `get_regime_model_revision`, `get_regime_latest`, `list_strategies`, or `get_strategy_config` |
| Cross-repo installed-wheel compatibility gate | `Verified` | This repo's CI validates lint, tests, and build locally, but no local workflow here installs the candidate package into consumer repos |

## 11. Decision Register

| Date | Decision | Impacted Sections | Review Status |
| --- | --- | --- | --- |
| 2026-04-18 | Raise the `asset-allocation-contracts` minimum stable source dependency floor to `2.1.0` so runtime-common adoption requires the published `2.1.0` contracts release or newer compatible stable versions. | 6, 8, 11, 13 | Active |
| 2026-04-17 | Widen runtime-common into the shared backend package for storage/runtime foundations, provider adapters, market-data helpers, extracted backtesting helpers, and shared runtime repositories; treat the resulting package contract as semver-major `2.0.0`. | 1, 2, 3, 4, 5, 6, 8, 9 | Active |
| 2026-04-18 | Declare `asset-allocation-contracts` as a minimum stable version floor (`>=`) in source; CI and security verify that the declared requirement resolves, and installers may consume newer matching published versions without repo changes. | 6, 8, 9, 13 | Active |
| 2026-04-06 | Adopt `docs/architecture/architecture-contract.md` as the authoritative living contract for this repo. Existing ADRs, ownership docs, migration notes, and runbooks become supporting evidence rather than peer architecture authorities. | All | Active |
| 2026-04-06 | Treat the public export list in `python/asset_allocation_runtime_common/__init__.py` as the default published API boundary for this package. | 5, 7, 9 | Active |
| 2026-04-06 | Treat ranking, regime, strategy, and universe repositories as read-only package boundaries, with explicit operational exceptions limited to backtest lifecycle, ranking-refresh lifecycle, and results freshness reconcile calls. | 4, 6, 7, 10 | Active |
| 2026-04-17 | Extend the runtime-common public surface with ranking-refresh lifecycle methods and a results freshness reconcile client to support delta-driven platinum ranking and canonical backtest freshness. | 5, 6, 7, 9 | Active |
| 2026-04-06 | Require this contract to be updated in the same change set as any public behavior or boundary change. | 9, 13 | Active |
| 2026-04-17 | Keep `asset-allocation-contracts` exact-pinned in source but always advance `main` to the latest stable published release through repo automation; fail CI when runtime-common falls behind instead of tolerating version lag. | 6, 8, 9, 13 | Superseded |

## 12. Evidence Ledger

Architecture and boundary docs:
- `README.md`
- `docs/architecture/adr-001-multirepo-boundaries.md`
- `docs/architecture/repo-ownership-map.md`
- `docs/architecture/migration-ledger.md`
- `docs/architecture/repo-handbook.md`
- `docs/ops/ci-release-runbook.md`
- `docs/ops/cutover-rollback-runbook.md`
- `docs/releases/cutover-release-notes.md`

Published package and implementation evidence:
- `python/pyproject.toml`
- `python/asset_allocation_runtime_common/__init__.py`
- `python/asset_allocation_runtime_common/api_gateway_auth.py`
- `python/asset_allocation_runtime_common/control_plane_transport.py`
- `python/asset_allocation_runtime_common/backtest_repository.py`
- `python/asset_allocation_runtime_common/ranking_repository.py`
- `python/asset_allocation_runtime_common/regime_repository.py`
- `python/asset_allocation_runtime_common/strategy_repository.py`
- `python/asset_allocation_runtime_common/universe_repository.py`

Test evidence:
- `tests/python/test_control_plane_transport.py`
- `tests/python/test_backtest_repository.py`
- `tests/python/test_ranking_repository.py`
- `tests/python/test_regime_repository.py`
- `tests/python/test_results_repository.py`
- `tests/python/test_strategy_repository.py`
- `tests/python/test_universe_repository.py`

Workflow evidence:
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/security.yml`
- `.github/CODEOWNERS`

## 13. Update Protocol

1. Update this document in the same PR whenever a change affects package boundaries, public exports, request semantics, error semantics, normalization rules, supported runtime/dependency versions, or release behavior.
2. Keep evidence labels honest:
   - use `Verified` when the repo proves the claim directly
   - use `Inferred` when the claim is a strong conclusion from repo evidence
   - use `Unverified / Needs confirmation` when the repo cannot prove the claim
3. Do not promote an `Inferred` or `Unverified / Needs confirmation` item into an invariant without new tracked evidence.
4. When contract-affecting behavior changes, update the decision register entry or add a new one with the date, impacted sections, and review status.
5. Review this contract at every tagged release and whenever a PR changes a top-level module, exported symbol, runtime dependency, or repo boundary document.
6. During review, remove stale evidence links, move resolved unknowns out of Section 10, and confirm that Sections 4 through 9 still match the published package and test suite.
