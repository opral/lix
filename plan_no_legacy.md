# Plan: Remove All Remaining Legacy Bridging

Date: 2026-02-24  
Scope: `packages/engine/src` (including modules outside `sql2`)

## Goal

Eliminate all runtime bridging from `sql2` to `crate::sql`, so dependency direction is:

`engine + filesystem + plugin runtime -> sql2 -> backend`

No `sql2` runtime path should call legacy `sql/*`.

## Invariants

1. Runtime flow remains:
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`.
2. One conceptual change per phase, one commit per phase.
3. Integration behavior must stay unchanged.

## Remaining Bridge Seams

1. `sql_preprocess_runtime.rs` is the only non-`sql/*` runtime module still importing `crate::sql::*`.
2. `packages/engine/src/sql/*` still owns preprocess internals (`pipeline`, `types`, `row_resolution`, `steps`, `rewrite`) that block physical deletion.
3. Legacy `sql/*` tests still hold behavioral coverage that must be migrated or replaced before deletion.

## Phase NL1: Remove Legacy Detected-Change Types

1. Switch `filesystem/mutation_rewrite.rs` to use `sql2::contracts::effects::DetectedFileDomainChange`.
2. Update side-effect structs and constructors to emit `sql2` type directly.
3. Remove conversion calls in `engine.rs` (`from_legacy_detected_file_domain_changes`).
4. Delete `sql2/contracts/legacy_sql/effects.rs` and its exports from `mod.rs`.

Verify:

1. `cargo test -p lix_engine --test transaction_execution --test file_materialization -- --test-threads=1`

Exit:

1. Filesystem/engine no longer use legacy detected-change types.

## Phase NL2: Remove Legacy Lowering from Filesystem Rewrite

1. Introduce `sql2` lowering entrypoint (or move lowering ownership to `sql2`).
2. Replace filesystem callsite using `legacy_sql::lower_statement` with `sql2` lowering.
3. Remove `use crate::sql as legacy_sql` from filesystem rewrite path.

Verify:

1. `cargo test -p lix_engine --test filesystem_view --test file_history_view -- --test-threads=1`

Exit:

1. Filesystem rewrite path does not call legacy lowering.

## Phase NL3: Move Postprocess Followup Builders into `sql2`

1. Move update/delete followup builders from `sql/steps/vtable_write.rs` into `sql2/execution`.
2. Keep temporary wrappers in legacy `sql/*` if needed, but direction must be `sql/* -> sql2`, never `sql2 -> sql/*`.
3. Update `sql2/execution/postprocess.rs` to use native `sql2` followup builder logic and contracts.
4. Remove legacy plan/type conversions used only for followup execution.

Verify:

1. `cargo test -p lix_engine --test transaction_execution --test schema_provider -- --test-threads=1`

Exit:

1. `sql2/execution/postprocess.rs` has zero legacy `sql/*` imports.

## Phase NL4: Move Read-Rewrite Session Ownership into `sql2`

1. Move `ReadRewriteSession` and `rewrite_read_query_with_backend_and_params_in_session` ownership to `sql2/history/rewrite`.
2. Update filesystem/history callers to use `sql2` implementation directly.
3. If needed, keep `sql/read_pipeline.rs` as wrapper calling into `sql2` temporarily.

Verify:

1. `cargo test -p lix_engine --test file_history_view --test state_history_view -- --test-threads=1`

Exit:

1. `sql2/history/rewrite` no longer aliases legacy session/rewrite API.

## Phase NL5: Move Preprocess Ownership into `sql2/planning`

1. Port preprocess pipeline ownership (`parse/materialize defaults/rewrite/bind/lower/render`) into `sql2/planning`.
2. Remove calls from `sql2/planning/preprocess.rs` to legacy `preprocess_*`.
3. Keep legacy `sql/pipeline.rs` as optional wrapper to `sql2` during migration.
4. Move plan fingerprint ownership to `sql2` contracts/planning.

Verify:

1. `cargo test -p lix_engine --test execute --test commit --test state_commit_stream -- --test-threads=1`

Exit:

1. `sql2/planning/preprocess.rs` has zero `crate::sql` dependency.

## Phase NL6: Delete `sql2/contracts/legacy_sql/*`

1. Remove `sql2/contracts/legacy_sql/mod.rs`.
2. Remove `sql2/contracts/legacy_sql/preprocess.rs`.
3. Remove all imports from `engine.rs`, `sql2/planning/*`, `sql2/execution/*`.

Verify:

1. `rg -n "contracts::legacy_sql|legacy_sql::|crate::sql as legacy_sql" packages/engine/src`
2. `cargo test -p lix_engine --tests`

Exit:

1. `sql2/contracts/legacy_sql` is gone.
2. Runtime path has no legacy bridge modules.

## Phase NL7: Final Guardrails and Legacy Module Shrink

1. Strengthen `tests/sql2_guardrails.rs`:
   - forbid `crate::sql::` imports under `sql2`, filesystem runtime rewrite paths, and engine runtime section.
   - forbid `legacy_sql` adapter module reintroduction.
2. Optionally convert remaining `sql/*` code to wrapper-only (or begin deletion plan if no remaining callers).

Verify:

1. `cargo test -p lix_engine --test sql2_guardrails`
2. `cargo test -p lix_engine`

Exit:

1. Guardrails enforce no return to legacy bridge patterns.

## Phase NL8: Preprocess Contract Internalization (`sql2` owns output types)

1. Replace `sql_preprocess_runtime` conversions from `crate::sql::PreprocessOutput` to `sql2` contracts with native `sql2` output construction.
2. Move/introduce preprocess contract builders under `sql2/planning` + `sql2/contracts` so runtime no longer depends on `sql/types.rs`.
3. Keep `sql/*` wrappers only as temporary call-through where needed.

Verify:

1. `cargo test -p lix_engine --test execute --test commit --test transaction_execution -- --test-threads=1`

Exit:

1. `sql_preprocess_runtime` no longer depends on `crate::sql::{PreprocessOutput,PostprocessPlan,MutationRow,...}` data types.

## Phase NL9: Port Preprocess Pipeline Core to `sql2/planning`

1. Move preprocess pipeline orchestration (`materialize insert-select sources`, `defaults`, `statement rewrite`, `render`) from `sql/pipeline.rs` to `sql2/planning`.
2. Keep `sql/pipeline.rs` as a wrapper to `sql2` during this phase.
3. Ensure placeholder bind-once semantics and postprocess single-statement invariants remain unchanged.

Verify:

1. `cargo test -p lix_engine --test transaction_execution --test schema_provider --test deterministic_mode -- --test-threads=1`

Exit:

1. `sql_preprocess_runtime` calls `sql2/planning` directly, not `crate::sql::preprocess_*`.

## Phase NL10: Port Shared SQL Utilities Needed by Preprocess

1. Move remaining preprocess dependencies from `sql/*` into `sql2` ownership:
   - binding/placeholder helpers,
   - row/source resolution helpers,
   - rewrite helpers used during preprocess.
2. Keep compatibility wrappers in `sql/*` only if still referenced by in-tree `sql/*` tests.

Verify:

1. `cargo test -p lix_engine --test filesystem_view --test file_history_view --test transaction_execution -- --test-threads=1`

Exit:

1. `rg -n "\\bcrate::sql::" packages/engine/src --glob '!packages/engine/src/sql/**'` returns no matches.

## Phase NL11: Migrate/Retire Legacy `sql/*` Unit Coverage

1. Identify behaviorally important `sql/*` unit tests and move them under `sql2/*` test modules.
2. Drop obsolete tests that only validate removed legacy implementation details.
3. Ensure remaining coverage is integration-first and `sql2`-owned.

Verify:

1. `cargo test -p lix_engine --lib`
2. `cargo test -p lix_engine --tests -- --test-threads=1`

Exit:

1. No required correctness coverage depends on compiling `packages/engine/src/sql/*`.

## Phase NL12: Convert `sql/*` to Wrapper-Only (Deletion Readiness Gate)

1. Reduce `sql/mod.rs` and submodules to minimal wrappers forwarding to `sql2` (or no-op stubs if unused).
2. Remove dead exports and dead imports from `sql/mod.rs`.
3. Add a strict guardrail asserting no runtime modules import `crate::sql::*`.

Verify:

1. `cargo test -p lix_engine --test sql2_guardrails`
2. `cargo test -p lix_engine --test execute --test transaction_execution -- --test-threads=1`

Exit:

1. Runtime behavior does not require any `sql/*` implementation code paths.

## Phase NL13: Delete `packages/engine/src/sql/*`

1. Remove `mod sql;` from `packages/engine/src/lib.rs`.
2. Delete `packages/engine/src/sql/**`.
3. Remove leftover imports/re-exports and dead code references.
4. Add file-existence guardrail to prevent reintroduction.

Verify:

1. `cargo test -p lix_engine --tests -- --test-threads=1`
2. `rg -n "\\bcrate::sql::|\\bsql::" packages/engine/src` (allow only SQL parser crate paths like `sqlparser::`).

Exit:

1. `packages/engine/src/sql` is physically removed.
2. Full `lix_engine` integration suite stays green.

## Definition of Done

1. No `sql2` runtime module imports `crate::sql::*`.
2. No filesystem/engine runtime path depends on legacy adapter conversions.
3. `sql2/contracts/legacy_sql/*` is deleted.
4. `packages/engine/src/sql/*` is deleted.
5. Full `lix_engine` integration suite passes.
