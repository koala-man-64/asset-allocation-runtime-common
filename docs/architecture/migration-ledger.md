# Migration Ledger

## Boundary Freeze Baseline

| Item | Classification | Owner | Status |
| --- | --- | --- | --- |
| Jobs-side control-plane transport and read-only repositories | canonical | `asset-allocation-runtime-common` | migrated |
| Contracts consumption in control-plane and jobs Python packages | canonical | `asset-allocation-contracts` | version-pinned |
| Contracts consumption in UI package and Docker image | canonical | `asset-allocation-contracts` | migrated to package dependency |
| Control-plane API gateway auth helper | temporary mirror | `asset-allocation-runtime-common` | shim retained |
| Jobs API gateway auth helper | temporary mirror | `asset-allocation-runtime-common` | shim retained |
| Control-plane Docker build vendoring sibling repos | retire-next | `asset-allocation-control-plane` | replaced by package install |
| Jobs Docker build vendoring sibling repos | retire-next | `asset-allocation-jobs` | replaced by package install |
| UI Docker build vendoring sibling repos | retire-next | `asset-allocation-ui` | replaced by package install |
| Generated output tracked in split repos | explicit exception | owning repo | currently only UI bytecode remained at cutover start |

## Open Follow-Up

- Move any remaining duplicated pure helpers from control-plane and jobs into runtime-common only when they have no storage or provider side effects.
- Remove compatibility shims after one release cycle.
- Add a full release-matrix publisher if a dedicated platform repo is introduced.

