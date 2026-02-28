# Lix Rewrite Pipeline Strictness Plan

## Goal
Make SQL rewrite behavior fail fast on unknown/unsupported columns so pipeline bugs surface as explicit engine errors, not late database errors or silent no-ops.

## Rollout policy
- Use `warn -> error` rollout per phase.
- Add runtime strictness mode:
  - `off`: current behavior.
  - `warn`: emit structured warning with statement + phase + column.
  - `error`: return `LixError`.
- Keep default at `warn` until phase test gates are green in CI for all simulation backends.

## Scope
- Write pipeline first (`INSERT`, `UPDATE`, `DELETE`).
- Then statement routing strictness.
- Then read pushdown strictness (optional, last).

## Phase 0: Baseline + observability
### Changes
- Add a small strictness utility shared by rewrite modules:
  - classify column as `known_property`, `known_lixcol`, `known_facade`, `unknown`.
  - format deterministic error/warn messages.
- Add structured warn hook in rewrite context.
- Add repro tests for known failure patterns:
  - `DELETE FROM lix_file WHERE path = ...` currently failing.
  - unknown `UPDATE` column on entity view currently passes through.
  - unknown `INSERT` column on entity view currently ignored.

### Exit criteria
- No behavior change in `off`.
- New tests demonstrate current non-strict behavior and are pinned.

## Phase 1: Entity-view write strictness
### Changes
- In entity view write rewrite:
  - Reject unknown `UPDATE` assignment columns.
  - Reject unknown `WHERE` column references in `UPDATE`/`DELETE`.
  - Reject unknown `INSERT` columns (do not silently drop).
- Keep existing explicit rejects (`schema_key`, direct `snapshot_content`, etc.).

### Files
- `lix/packages/engine/src/sql/planning/rewrite_engine/entity_views/write.rs`

### Tests
- Add matrix tests (`sqlite`, `postgres`, `materialization`):
  - unknown `INSERT` column -> error.
  - unknown `UPDATE` assignment -> error.
  - unknown `WHERE` column -> error.
  - known property/lixcol predicates still pass.

### Exit criteria
- Entity-view unknown column usage always fails in `error` mode with clear message.

## Phase 2: Filesystem facade strictness + path-safe writes
### Changes
- Define explicit allowed facade columns per filesystem view (`lix_file*`, `lix_directory*`).
- Reject unknown assignments/predicates for filesystem writes.
- Fix file `DELETE/UPDATE ... WHERE path ...` by rewriting predicate scope to ID/version scope (similar to directory flow), not by letting `path` reach descriptor/vtable layers.
- Preserve `data` fast-path behavior, but validate predicate columns strictly.

### Files
- `lix/packages/engine/src/filesystem/mutation_rewrite.rs`

### Tests
- `DELETE FROM lix_file WHERE path = ...` succeeds and deletes expected row.
- `UPDATE lix_file SET metadata = ... WHERE path = ...` works.
- unknown filesystem write column -> explicit error.
- add regression for qualified column refs (`alias.path`) handling.

### Exit criteria
- No filesystem facade write can leak unresolved facade columns downstream.

## Phase 3: Lower-layer write strictness
### Changes
- Enforce assignment allowlists for:
  - `lix_state_by_version_view_write`
  - `lix_state_view_write`
  - `vtable_write` update/delete paths
- Reject unknown write-target columns instead of filtering/ignoring.
- Keep internal auto-managed columns (`updated_at`, etc.) controlled by rewrite layer only.

### Files
- `lix/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_by_version_view_write.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_write.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/steps/vtable_write.rs`

### Tests
- Unknown assignment at each layer errors before SQL execution.
- Existing valid mutation plans unchanged.

### Exit criteria
- No lower write layer silently drops user-provided unknown columns.

## Phase 4: Statement routing strictness
### Changes
- Replace blanket statement passthrough for write-like statements with strict routing:
  - If statement is write and no canonical rule claims it, error.
  - Keep passthrough only for explicitly allowed statement classes (e.g. pure reads if intended).
- Add diagnostics indicating which rule chain declined rewrite.

### Files
- `lix/packages/engine/src/sql/planning/rewrite_engine/pipeline/registry.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/statement/mod.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/statement/passthrough.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/pipeline/statement_pipeline.rs`

### Tests
- Unsupported write statement -> deterministic engine error.
- Supported write statements still route correctly.

### Exit criteria
- No accidental write passthrough.

## Phase 5: Read-side pushdown strictness (optional, after write hardening)
### Changes
- Keep rewrite semantics safe but add strict validation mode for read pushdown extraction:
  - unknown pushdown columns remain in `remaining` (semantic safety),
  - optionally warn/error when unknown columns appear in pushdown-eligible contexts.
- This is diagnostic strictness, not functional rewrite blocking by default.

### Files
- `lix/packages/engine/src/sql/planning/rewrite_engine/steps/state_pushdown.rs`
- `lix/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_history_view_read.rs`

### Exit criteria
- Unknown read pushdown columns are visible to developers via warnings/errors in strict mode.

## Cross-phase test strategy
- Every phase must run:
  - targeted unit tests,
  - affected integration/simulation tests across `sqlite`, `postgres`, `materialization`.
- Add one `strict_mode_compat` test module to assert behavior differences:
  - `off`: legacy allowed behavior.
  - `warn`: succeeds + warning emitted.
  - `error`: fails with explicit `LixError`.

## Error message contract
- Message format:
  - `strict rewrite violation: <phase>: unknown column '<name>' in <context>; allowed: <list>`
- Must include:
  - phase/module,
  - operation (`insert/update/delete/select`),
  - relation/view name if known.

## Execution order
1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5 (optional)

## Definition of done
- Write pipeline has no silent unknown-column behavior.
- Known facade columns are rewritten before descriptor/vtable layers.
- Unsupported write statements fail at routing layer with explicit errors.
- CI green on all simulation backends with strict mode enabled for write pipeline.
