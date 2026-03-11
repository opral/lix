# SQL2 Completion Plan

## Objective

Finish the migration by making `packages/engine/src/sql2/**` the only owner of public-surface semantics and deleting the remaining legacy/internal bridge paths.

This plan is not about adding isolated planner features. It is about completing one clean ownership cut:

- all public-surface reads route through sql2
- all public-surface writes route through sql2
- `query_runtime` becomes execution plumbing, not a semantic router
- `internal_state` becomes internal storage/bootstrap machinery only, then shrinks further once the last callers are gone

## Why A New Plan

`plan.md` established the semantic architecture and migration rules. The remaining problem is not direction; it is incomplete cutover.

Today the main live split is:

- `query_runtime/shared_path.rs` still decides between sql2 and fallback execution
- `internal_state/mod.rs` still contains a public-read lowering bridge for nested/public read cases
- some public surfaces are fully owned by sql2 while others still depend on bridge logic or fallback behavior

That split is the source of the remaining complexity. The next phase should remove it.

## Hard Rules

1. No new public-surface behavior may be implemented in `internal_state/**`.
2. No new public-surface behavior may depend on exact SQL-shape rewrites outside `sql2/**`.
3. If sql2 lacks a capability, add it to sql2 directly instead of adding another bridge.
4. `query_runtime/**` may orchestrate execution, but it must not decide public-surface semantics.
5. A partially migrated public surface is not an acceptable end state. Each surface family must have one owner.

## End State

The engine should look like this:

1. Parse and bind SQL.
2. Ask sql2 whether the statement batch is public-surface work.
3. If yes, sql2 prepares the full read or write program, including nested public subqueries.
4. `query_runtime` executes the prepared program and generic runtime effects.
5. `internal_state` is used only for internal tables, bootstrap/materialization helpers that are not public-surface semantics, or is deleted where no longer needed.

Concretely:

- `prepare_execution_with_backend()` should not probe sql2 and then build a second semantic plan for the same public statement batch.
- `internal_state::lower_public_read_query_with_sql2_backend()` and related public-surface detection helpers should disappear.
- nested public reads should lower inside sql2 itself, with the same registry/proof/effective-state machinery as top-level reads.

## Phase 1: Make Sql2 Own Public Dispatch

Goal:
Move public-surface dispatch out of `query_runtime/shared_path.rs` and `internal_state/mod.rs` into one sql2-owned preparation boundary.

Work:

- add one sql2 entrypoint that classifies and prepares public statement batches for execution
- make `query_runtime/shared_path.rs` call that entrypoint first and treat the result as authoritative
- keep fallback only for statement batches that do not reference a public surface at all
- reject mixed batches that combine public-surface semantics with unsupported internal-only statements instead of partially falling through
- remove the current pattern where reads can lower through sql2 but execution planning still re-enters the generic semantic planner

Primary files:

- `packages/engine/src/query_runtime/shared_path.rs`
- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/internal_state/mod.rs`

Exit criteria:

- any top-level statement batch that references a public surface either prepares fully in sql2 or fails with a sql2 error
- `shared_path` no longer performs semantic fallback for public-surface batches
- the `public write target ... must route through sql2` guard becomes unnecessary because routing is structural

## Phase 2: Move Nested Public Reads Fully Into Sql2

Goal:
Delete the public-read rewrite bridge in `internal_state/mod.rs` by teaching sql2 to lower nested public reads directly.

Work:

- support nested public-surface scans in subqueries, predicates, CTEs, and mutation selectors within sql2 lowering
- make the lowerer recurse through nested relations with the live `SurfaceRegistry`, including dynamic surfaces
- keep canonicalization/proofs/effective-state logic identical between top-level and nested reads
- remove `statement_references_public_sql2_surface*`, `rewrite_public_read_query_to_lowered_sql()`, and `lower_public_read_query_with_sql2_backend()`
- rehome any remaining generic SQL helpers those bridge functions still own into `sql2/core/**` or neutral shared modules

Primary files:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`
- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/internal_state/mod.rs`
- `packages/engine/src/internal_state/materialize.rs`

Exit criteria:

- public read subqueries no longer call into `internal_state` for lowering
- entity/state/filesystem/admin selectors that contain public subqueries all execute through sql2 alone
- deleting the public-read bridge does not regress the integration suites that currently rely on it

## Phase 3: Finish Surface-Family Migration

Goal:
Remove the remaining semantic gaps so every public family is fully owned by sql2.

Surface families to close:

- state
- entity
- filesystem
- stored schema
- version/admin
- change/history/public projections that are still special-cased elsewhere

Work:

- inventory every public relation in `SurfaceRegistry` and mark each as:
  - fully owned by sql2
  - read-only in sql2
  - write-blocked in sql2
  - still bridged/fallback
- eliminate the last `returns_none`, “must route through sql2”, or bridge-only special cases for public surfaces
- model remaining admin/version semantics explicitly in sql2 instead of keeping them on frozen legacy behavior
- move any lingering public-surface materialization or followup assumptions into sql2 planned artifacts where they belong

Primary files:

- `packages/engine/src/sql2/catalog/mod.rs`
- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/sql2/planner/semantics/**`
- `packages/engine/src/query_runtime/shared_path.rs`

Exit criteria:

- every public surface in the registry has a single sql2 execution owner
- no public-surface test requires bridge code in `internal_state`
- no production code path intentionally returns `None` from sql2 preparation for a supported public surface

## Phase 4: Collapse Public Write Runtime Onto One Sql2 Artifact

Goal:
Finish the runtime cut so public tracked and untracked writes execute from one sql2-prepared artifact, not a mix of sql2 planning plus generic followup semantics.

Work:

- keep transaction-scoped append sessions as the tracked write boundary for sql2 writes
- ensure domain changes, commit preconditions, invariants, materialization, and runtime effects all derive from the sql2 prepared write artifact
- remove any public-write-only dependence on `internal_state::PostprocessPlan`, fallback mutation validation, or exact internal rewrite shapes
- separate true generic runtime services from public-surface semantic services

Primary files:

- `packages/engine/src/query_runtime/shared_path.rs`
- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/internal_state/followup.rs`
- `packages/engine/src/query_runtime/invariants.rs`

Exit criteria:

- public writes do not require internal-state postprocess planning
- public tracked writes append commits from sql2 domain-change artifacts only
- public untracked writes still share the sql2 semantic resolver and diverge only at execution mode

## Phase 5: Delete The Remaining Legacy Public Bridge

Goal:
Remove the modules and invariants that only exist to preserve the migration split.

Delete or reduce:

- public-surface bridge helpers in `packages/engine/src/internal_state/mod.rs`
- any public-read/public-write rewrite helpers in `packages/engine/src/internal_state/**`
- public-surface fallback branches in `packages/engine/src/query_runtime/shared_path.rs`
- tests that only pin transitional bridge behavior instead of semantic behavior

Keep only if still needed for non-public internal execution:

- bootstrap helpers
- internal-table rewrite/materialization helpers
- internal storage execution contracts that are not public semantics

Exit criteria:

- `internal_state` contains no public-surface routing or lowering logic
- `query_runtime` no longer has a public-surface fallback boundary
- the remaining non-sql2 code is explicitly internal-only and named that way

## Phase 6: Decompose Sql2 After Cutover

Goal:
Once sql2 is the sole owner, break up the large modules so the architecture is not correct-but-concentrated.

Mandatory splits:

- split `write_resolver.rs` by surface family and shared helpers
- split `lowerer.rs` by semantic kernel / lowering family
- split `runtime/mod.rs` into read preparation, write preparation, error mapping, and execution contracts

Possible structure:

- `sql2/planner/semantics/write_resolver/`
- `sql2/planner/backend/lowerer/`
- `sql2/runtime/read.rs`
- `sql2/runtime/write.rs`
- `sql2/runtime/errors.rs`

Exit criteria:

- no single sql2 implementation module should remain the default dumping ground for unrelated planner concerns
- the cutover should be preserved while complexity is reduced, not reintroduced through new bridges

## Acceptance Gates

Each phase should land only when the relevant semantic suites pass on sql2-owned paths.

Required gates:

- public read/write integration suites for state/entity/filesystem/admin/version surfaces
- transaction behavior, especially explicit `BEGIN/COMMIT`, `EngineTransaction`, and `ON CONFLICT`
- materialization/rebuild suites
- dynamic surface catalog invalidation tests
- explain/observation/dependency metadata tests for migrated paths

Additional structural gates:

- no new dependency from `sql2/**` to frozen legacy modules
- no public-surface code path in `internal_state/**`
- no public-surface fallback branch in `query_runtime/shared_path.rs`

## Suggested Execution Order

1. Phase 1: make sql2 own public dispatch
2. Phase 2: move nested public reads fully into sql2
3. Phase 3: finish the remaining public families
4. Phase 4: collapse public write runtime onto one sql2 artifact
5. Phase 5: delete the remaining legacy public bridge
6. Phase 6: decompose sql2 after cutover

This order matters. The correct next move is to finish ownership first, then delete the bridge, then decompose. Doing decomposition before the cut only spreads the split architecture into more files.

## Immediate Next Slice

The first concrete slice from this plan should be:

1. make `sql2/runtime` the sole classifier for public statement batches
2. remove public-surface fallback in `query_runtime/shared_path.rs`
3. migrate nested public read lowering out of `internal_state/mod.rs`

If those three steps land, the engine stops having two live semantic owners for the same public contract.

## Progress Log

- 2026-03-11 15:05 PDT - drafted `plan4.md` as the completion plan for the sql2 cutover: one public-semantics owner, no new bridge work, explicit deletion of the remaining fallback boundary, and a decomposition pass only after the cutover is structurally complete
- 2026-03-11 15:29 PDT - completed Phase 1 dispatch cut in the live path: `query_runtime/shared_path.rs` now asks sql2 to classify and prepare public statement batches through one `prepare_sql2_public_execution(...)` boundary, and fallback planning is now reserved for non-public/internal batches instead of probing public reads and writes independently
- 2026-03-11 16:02 PDT - completed the Phase 2 ownership move for the public read bridge: public read detection and lowering helpers now live under `sql2/runtime`, `internal_state/mod.rs` no longer owns those bridge functions, and internal-state insert-select materialization now calls the sql2-owned lowering helper directly
- 2026-03-11 16:24 PDT - closed the remaining known public state-family fallback in tracked filesystem payload persistence: synthetic `INSERT INTO lix_state_by_version ...` side-effect writes now prepare and execute through `query_runtime/shared_path` + sql2 instead of `preprocess_sql_to_plan()`/legacy backend rewrite, which fixed the `plugin_install` upsert regression across sqlite, postgres, and materialization
- 2026-03-11 18:05 PDT - implemented resolver-driven execution-mode selection for sql2 state/entity writes: `canonicalize` now records a requested mode instead of an authoritative tracked/untracked mode, `ResolvedWritePlan` now carries the resolved `execution_mode`, state/entity existing-row writes pick tracked vs untracked from the effective-state winner lane, and tracked commit artifacts/runtime gating now consume the resolved execution mode instead of the canonical request
- 2026-03-11 20:11 PDT - completed the remaining Phase 3 public state-family cut: top-level public reads now use a strict sql2 read-preparation path instead of falling through to generic planning, unbounded `lix_state` reads are schema-bounded inside sql2 using state-backed registered schemas, wrapped `lix_state` version-id exposure failures are normalized at the sql2 read entrypoint, and `_by_version` state read/write paths now return the established public error contracts (`LIX_ERROR_SCHEMA_NOT_REGISTERED`, explicit `version_id` requirement messages) rather than raw planner/proof text
