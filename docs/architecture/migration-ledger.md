# Migration Ledger

## Boundary Freeze Baseline

| Item | Classification | Owner | Status |
| --- | --- | --- | --- |
| Jobs-side shared backend foundations, control-plane transport, and shared repositories | canonical | `asset-allocation-runtime-common` | migrated |
| Control-plane shared backend foundations and provider/runtime deduplication | canonical | `asset-allocation-runtime-common` | migrated in v2 |
| Contracts consumption in control-plane and jobs Python packages | canonical | `asset-allocation-contracts` | version-pinned |
| Contracts consumption in UI package and Docker image | canonical | `asset-allocation-contracts` | migrated to package dependency |
| Control-plane API gateway auth helper | temporary mirror | `asset-allocation-runtime-common` | shim retained |
| Jobs API gateway auth helper | temporary mirror | `asset-allocation-runtime-common` | shim retained |
| Control-plane Docker build vendoring sibling repos | retire-next | `asset-allocation-control-plane` | replaced by package install |
| Jobs Docker build vendoring sibling repos | retire-next | `asset-allocation-jobs` | replaced by package install |
| UI Docker build vendoring sibling repos | retire-next | `asset-allocation-ui` | replaced by package install |
| Generated output tracked in split repos | explicit exception | owning repo | currently only UI bytecode remained at cutover start |

## Open Follow-Up

- Remove any remaining runtime-common compatibility wrappers after consumer import migration fully settles.
- Add a full release-matrix publisher if a dedicated platform repo is introduced.
