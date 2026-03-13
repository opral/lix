# Query Planner Drift Reduction Plan

## Objective

Reduce planner drift by moving state-backed public-surface semantics into one canonical layer above the internal vtable rewrite path.

The goal is not "everything should live in `vtable_read.rs` / `vtable_write.rs`".

The goal is:

- all state-backed public surfaces share one semantic model
- reads and writes use the same surface metadata and canonical rules
- the vtable layer stays generic and execution-oriented
- surface-specific behavior stops being reimplemented in `canonicalize`, `write_resolver`, `effective_state_resolver`, and `lowerer`

## Core Principle

`Public-surface semantics should be defined once in a canonical state layer, then lowered to the vtable/internal tables.`

Not:

- entity logic in one branch
- filesystem logic in another
- scope/overlay logic in a third
- and generic internal-state rewrite doing a fourth partial interpretation

## Current Drift Sources

The current planner spreads the same semantic concerns across several modules:

- [packages/engine/src/sql/public/planner/canonicalize.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/public/planner/canonicalize.rs)
- [packages/engine/src/sql/public/planner/semantics/write_analysis.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/public/planner/semantics/write_analysis.rs)
- [packages/engine/src/sql/public/planner/semantics/write_resolver.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/public/planner/semantics/write_resolver.rs)
- [packages/engine/src/sql/public/planner/semantics/effective_state_resolver.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/public/planner/semantics/effective_state_resolver.rs)
- [packages/engine/src/sql/public/planner/backend/lowerer.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/public/planner/backend/lowerer.rs)
- [packages/engine/src/state/internal/vtable_read.rs](/Users/samuel/git-repos/lix/packages/engine/src/state/internal/vtable_read.rs)
- [packages/engine/src/state/internal/vtable_write.rs](/Users/samuel/git-repos/lix/packages/engine/src/state/internal/vtable_write.rs)

Examples:

- entity identity derivation and patch semantics live in `write_resolver`
- filesystem path semantics live in `write_resolver`
- overlay and effective-row loading logic live in `effective_state_resolver`
- pushdown logic lives in `lowerer`
- generic `schema_key`-scoped rewrite rules live in `vtable_write`

This creates two failure modes:

- semantic drift between surface families
- duplicated fixes when a rule changes

## Non-Goals

This plan does not try to:

- move all public semantics directly into the raw vtable rewrite layer
- force non-state-backed surfaces into a fake state abstraction
- preserve old `sql2`/`day-1` terminology

## Target Architecture

## 1. Introduce `SurfaceSemantics`

Add a new module, for example:

- `packages/engine/src/sql/public/planner/semantics/surface_semantics.rs`

This becomes the single source of truth for state-backed surface behavior.

For each state-backed surface, define:

- storage model
- canonical columns
- exposed columns
- identity strategy
- scope strategy
- overlay strategy
- write capabilities
- property projection strategy

Suggested shape:

```rust
struct SurfaceSemantics {
    storage_kind: SurfaceStorageKind,
    column_map: ColumnMap,
    identity: IdentitySemantics,
    scope: ScopeSemantics,
    overlay: OverlaySemantics,
    write: WriteSemantics,
    projection: ProjectionSemantics,
}
```

## 2. Introduce Canonical State IR

Add a canonical state-focused IR that both reads and writes lower into.

Suggested types:

- `CanonicalStateRead`
- `CanonicalStateSelector`
- `CanonicalStateWrite`
- `CanonicalStatePatch`
- `CanonicalStateRowKey`

This IR should carry:

- resolved canonical columns
- normalized predicates
- scope/overlay requirements
- identity information
- requested projections

This IR sits above the vtable layer and below public-surface parsing/binding.

## 3. Classify Surfaces By Storage Model

Split surfaces into:

- state-backed
- non-state-backed

State-backed surfaces:

- `lix_state`
- dynamic entity surfaces
- most filesystem metadata surfaces

Non-state-backed surfaces:

- change/history/working-changes style surfaces
- admin surfaces that are not really state-row overlays
- payload/blob adjunct behavior

The drift-reduction work should start with the state-backed set.

## 4. Move Shared Semantics Out Of `write_resolver`

`write_resolver.rs` should stop hand-owning:

- entity PK-derived identity logic
- `lixcol_*` mapping logic
- filesystem exact-filter matching logic
- ad hoc scope/global/untracked interpretation

Instead it should:

1. resolve target surface semantics
2. build a canonical selector
3. load canonical exact rows
4. apply a canonical patch
5. emit canonical state writes

That reduces it from a product-specific rules file to a generic state-write coordinator.

## 5. Move Shared Semantics Out Of `effective_state_resolver`

`effective_state_resolver.rs` should stop being a second source of truth for:

- overlay semantics
- exact-filter column mapping
- version/global/untracked projection rules

It should consume `SurfaceSemantics + CanonicalStateRead` instead.

That way read-side exact-row loading and write-side exact-row loading share the same rules.

## 6. Keep `vtable_*` Generic

`vtable_read.rs` and `vtable_write.rs` should remain generic internal-state machinery.

They should own:

- internal table rewrites
- `schema_key`-scoped state routing
- materialized/untracked table routing
- generic snapshot persistence rules

They should not own:

- entity PK semantics
- filesystem path semantics
- public overlay defaults
- public surface capability rules

## 7. Replace String-Based Predicate Reinterpretation

Where planner internals still degrade predicates into strings early, replace that with normalized expression/state IR until final lowering.

This matters because string-based reinterpretation creates hidden drift between:

- canonicalization
- effective-state planning
- pushdown splitting
- final lowering

Keep final SQL string generation at the backend boundary only.

## Refactor Phases

## Phase 1: Add `SurfaceSemantics`

Create the module and move into it:

- canonical/public column mapping
- scope defaults
- overlay defaults
- write-capability metadata

Initial consumers:

- `write_resolver`
- `effective_state_resolver`

Validation:

- no direct per-family column-map tables remain in `write_resolver`
- no direct per-family exact-filter mapping remains in `effective_state_resolver`

## Phase 2: Add Canonical State Selector + Row Key

Introduce:

- `CanonicalStateSelector`
- `CanonicalStateRowKey`

Use them for:

- exact-row loading
- selector reads for writes
- effective-row matching

Validation:

- selector exactness and row-key derivation are shared between read and write paths

## Phase 3: Add Canonical State Patch

Introduce:

- `CanonicalStatePatch`

This should represent:

- property updates
- canonical state column updates
- identity-preservation rules

Entity updates should derive through this, not via handwritten merge logic in `write_resolver`.

Validation:

- entity update logic no longer needs bespoke PK/identity patch code inline

## Phase 4: Move Filesystem Metadata Semantics Into Shared Layer

Extract filesystem-specific normalization and matching from `write_resolver` into surface semantics + canonical state patching.

Validation:

- path normalization and row matching are not duplicated across read/write branches

## Phase 5: Make `effective_state_resolver` Consume Canonical IR

Change effective-state planning to operate on:

- `SurfaceSemantics`
- `CanonicalStateRead`
- `CanonicalStateSelector`

not on ad hoc mappings and string predicates.

Validation:

- overlay/global/untracked behavior is shared with write-side canonical selectors

## Phase 6: Reduce `write_resolver` To Coordination

After the above extractions, `write_resolver` should mostly:

- choose the right canonical flow
- orchestrate selector -> exact rows -> patch -> write plan

Validation:

- large family-specific helper clusters are deleted or moved out

## Phase 7: Guardrails

Add tests that specifically prove the same semantics are shared:

- entity/state reads and writes agree on identity and scope rules
- filesystem reads and writes agree on path normalization rules
- canonical selector logic is shared by public reads and public writes

Guardrail examples:

- grep-based or structural tests that prevent new per-surface column maps from being reintroduced into `write_resolver`
- tests that compare read-side and write-side exact-row targeting on the same surface

## Suggested Order Of Implementation

1. `SurfaceSemantics`
2. canonical selector/row-key
3. shared exact-row loading
4. canonical patch
5. entity migration
6. filesystem migration
7. effective-state migration
8. guardrails

## Success Criteria

This plan is complete when:

- state-backed public surfaces share one canonical semantic layer
- `write_resolver` no longer contains most surface-specific business rules
- `effective_state_resolver` no longer remaps columns/scope independently
- `vtable_*` remains generic internal-state machinery
- changing a scope/identity/overlay rule requires touching one semantic module, not several planner branches

## Progress Log

- 2026-03-12: Created plan for reducing query-planner drift by introducing a shared canonical semantics layer above the vtable path instead of pushing all public-surface logic into raw vtable rewrite code.
- 2026-03-12: Implemented the first extraction slice with `surface_semantics.rs`, centralizing shared state-backed selector column mapping and overlay-lane defaults that had been duplicated across `write_resolver` and `effective_state_resolver`.
- 2026-03-12: Implemented the canonical selector/row-key slice by adding `CanonicalStateSelector` and `CanonicalStateRowKey`, moving exact effective-row loading onto typed row keys, and making state/entity selector resolution share the same selector and row-key contract.
- 2026-03-12: Implemented the canonical assignments slice by renaming the shared mutation model from `patch` to `assignments`, extracting state/entity update application into `state_assignments.rs`, and moving state/entity insert-row/default assembly there so `write_resolver` no longer owns those mutation rules directly.
- 2026-03-12: Implemented the filesystem assignments slice by extracting typed file/directory update assignment parsing and normalization into `filesystem_assignments.rs`, then rewiring filesystem update resolution to consume those typed assignments instead of reopening raw payload maps in multiple branches.
- 2026-03-12: Implemented the filesystem insert slice by extracting typed file/directory insert parsing into `filesystem_assignments.rs`, rewiring filesystem insert planning to consume those typed assignments, deleting the remaining raw filesystem payload helpers from `write_resolver`, and adding small guardrail tests that keep insert/update path parsing aligned.
- 2026-03-12: Extracted filesystem insert batch and ancestor/path coordination into `filesystem_planning.rs`, so `write_resolver` now delegates file/directory insert planning to shared `plan_*_insert_batch` functions instead of owning `PendingFilesystemInsertBatch`, target resolution, and insert finalization directly.
- 2026-03-12: Added a structural SQL guardrail that requires `write_resolver` to delegate filesystem insert planning through `plan_directory_insert_batch` / `plan_file_insert_batch` and forbids the extracted insert batch/ancestor helper cluster from reappearing there.
- 2026-03-12: Moved the remaining generic filesystem lookup/load helpers into `filesystem_queries.rs`, rewired both `write_resolver` and `filesystem_planning` to use that shared query seam, and added a guardrail that forbids those query/helper definitions from drifting back into `write_resolver`.
- 2026-03-12: Extracted the top-level filesystem write coordinator into `write_resolver/filesystem_writes.rs`, leaving `write_resolver.rs` with the lower-level filesystem helpers for now and adding a guardrail that keeps the high-level file/directory insert/update/delete entrypoints out of the main resolver file.
- 2026-03-12: Moved the remaining filesystem helper cluster out of `write_resolver.rs` and into `write_resolver/filesystem_writes.rs`, including path/cycle resolution, selector row loading, descriptor/blob row construction, and auto-id helpers, then added a guardrail that keeps those lower-level filesystem helpers out of the parent resolver file too.
- 2026-03-12: Moved effective-state pushdown policy into `surface_semantics.rs` and stopped degrading read predicates to strings inside `effective_state_resolver.rs`; effective-state plans now carry structured `Expr` predicates until backend lowering, and SQLite entity/state-by-version suites passed against the new shared path.
- 2026-03-12: Added structural planner guardrails that keep effective-state pushdown policy in `surface_semantics.rs`, keep shared state assignment semantics in `state_assignments.rs`, keep exact-row targeting shared through `CanonicalStateRowKey` and `resolve_exact_effective_state_row`, and keep filesystem path normalization centralized in `filesystem_assignments.rs`.
