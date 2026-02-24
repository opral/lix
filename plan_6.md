# Plan 6: SQL2 Cleanup and Ownership Tightening

## Goal

Finalize SQL2 ownership so runtime flow stays clean and centralized:

`parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`

## Phase 6.1: Remove dead bridge/shim modules

1. Delete `packages/engine/src/sql2/type_bridge.rs`.
2. Remove `pub(crate) mod type_bridge;` from `packages/engine/src/sql2/mod.rs`.
3. Delete `packages/engine/src/sql2/execution/postprocess.rs` (re-export shim).
4. Update callsites to import followup builders directly from `sql2/execution/followup.rs`.

Exit criteria:

1. No `type_bridge` module remains in SQL2.
2. No `execution/postprocess.rs` re-export layer remains.
3. `cargo test -p lix_engine --test sql2_guardrails -- --test-threads=1` passes.

## Phase 6.2: Make fallback logic explicitly test-only

1. Restrict `sql2/fallback.rs` exposure to test builds only.
2. Remove dead-code allowances that only exist to support non-test fallback wiring.
3. Keep tests that validate sequentialization behavior, but avoid runtime-facing fallback API.

Exit criteria:

1. Fallback helpers are not part of non-test runtime wiring.
2. `cargo test -p lix_engine --test execute -- --test-threads=1` passes.

## Phase 6.3: Consolidate duplicated SQL2 orchestration

1. Extract shared orchestration between:
   - `packages/engine/src/sql2/api.rs`
   - `packages/engine/src/sql2/in_transaction.rs`
2. Ensure both transaction and non-transaction entrypoints use the same stage ordering and shared helpers.
3. Keep behavior identical; no semantic rewrite changes in this phase.

Exit criteria:

1. Parse/plan/execute/effects flow is defined once, not duplicated.
2. `cargo test -p lix_engine --test transaction_execution -- --test-threads=1` passes.

## Phase 6.4: Move remaining SQL2 runtime helpers out of engine root

1. Move SQL2-specific helpers from `packages/engine/src/engine.rs` into SQL2-owned modules:
   - placeholder-state advancement helpers
   - filesystem update domain-change collection helpers
2. Keep `engine.rs` focused on engine shell/entrypoints and shared non-SQL2 concerns.
3. Update tests/imports accordingly.

Exit criteria:

1. SQL2 runtime mechanics are owned under `packages/engine/src/sql2/**`.
2. `cargo test -p lix_engine --test execute --test transaction_execution -- --test-threads=1` passes.

## Phase 6.5: Naming and dead-code cleanup

1. Rename legacy terms still present in SQL2 internals (for example, helpers named `*_legacy_*`).
2. Remove broad `#![allow(dead_code)]`/`#[allow(dead_code)]` where no longer needed.
3. Keep only narrowly scoped allowances with explicit rationale if required.

Exit criteria:

1. No stale legacy naming remains in SQL2 runtime modules.
2. Dead-code suppressions are minimized and justified.
3. `cargo test -p lix_engine --test sql2_guardrails --test execute --test transaction_execution -- --test-threads=1` passes.

## Phase 6.6: Final verification and branch hygiene

1. Run the integration-focused engine test set repeatedly until stable:
   - `cargo test -p lix_engine --test execute -- --test-threads=1`
   - `cargo test -p lix_engine --test transaction_execution -- --test-threads=1`
   - `cargo test -p lix_engine --test sql2_guardrails -- --test-threads=1`
2. Ensure no references to removed shim modules remain.
3. Keep only intentional planning docs in repo root.

Exit criteria:

1. Integration suite is green for SQL2 path.
2. No dead shims/bridges remain.
3. Branch is ready for merge with clean SQL2 ownership.
