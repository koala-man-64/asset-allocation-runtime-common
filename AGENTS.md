# AGENTS.md instructions for C:\Users\rdpro\Projects\asset-allocation-runtime-common

## Source of Truth

Repo-local agents live under `.codex/skills`. Use that directory as the authoritative repo-local agent inventory for this repository.

## Cross-Repo Contract Routing

When planning or reviewing work in this repo, treat shared cross-repo data contract changes as owned by the sibling repo `asset-allocation-contracts`.

A change counts as a shared data contract change when it adds, removes, renames, retypes, or changes validation, serialization, or schema semantics for:

- shared API request and response payloads
- serialization keys or schema-backed shapes consumed across repos
- types mirrored from `asset-allocation-contracts` or `@asset-allocation/contracts`

This rule does not apply to:

- local DB schema
- internal-only DTOs
- repo-private view models or helpers not exported across repos

Required routing:

- Route shared contract shape changes through `asset-allocation-contracts` first.
- Use `asset-allocation-contracts/docs/architecture/master-design.md` as the contract process reference.
- Plan this repo only for adoption, adapters, migration, or version-bump work until an `asset-allocation-contracts` work item exists or is explicitly included as a prerequisite.

Required planning decision:

- State either `This is a contracts-repo-first change.` or `This is local-only and does not require contracts repo routing.`

Evidence to check when unsure:

- package dependency on `asset-allocation-contracts` or `@asset-allocation/contracts`
- repo docs or tests that describe `asset-allocation-contracts` as the owner of shared contracts

Default:

- If the shape may be shared and ownership is still unclear after checking local evidence, assume it is shared and route through `asset-allocation-contracts`.

## How to Use Agents

- Discovery: repo-local agents live under `.codex/skills`.
- Trigger rules: if the user names an agent or the task clearly matches one, use the minimal set that covers the request.
- Coordination: for planning and workflow decisions, treat `delivery-orchestrator-agent` and `project-workflow-enforcer-agent` as the authoritative routing surfaces.
- Safety: do not plan consumer-repo-only implementation for shared contract field changes unless the corresponding `asset-allocation-contracts` work is already included.
