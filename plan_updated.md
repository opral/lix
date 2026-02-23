# Query Planner Refactor Plan (Simplified, No Backward Compatibility)

Reference: `planner_review_updated.md`  
Date: 2026-02-23

## Goal

Implement one runtime flow only:

`parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`

New implementation lives under `packages/engine/src/sql2`.

Postprocess order is fixed:
`postprocess_sql (in-tx) -> apply_effects_tx (in-tx SQL-backed records) -> commit boundary -> apply_effects_post_commit (non-SQL runtime effects)`.

For explicit user transactions, `apply_effects_post_commit` is queued and flushed only on outer `commit()`. Rollback drops queued effects.

## Acceptance Boundary

Only one compatibility requirement is retained:

1. Full `lix_engine` integration tests must pass.

Non-goals:

1. No runtime backward compatibility guarantees outside what integration tests enforce.
2. No long-lived dual-path execution.
3. No migration gate modes (`legacy`, `shadow_compare`) in final implementation.

## Hard Constraints (This Refactor)

1. Every executed statement comes from one typed `ExecutionPlan`.
2. Bind placeholders exactly once; rebinding is a hard error.
3. Planner is side-effect free.
4. Execution consumes planner-emitted typed requirements/effects only.
5. Fallback/branching uses typed errors only; no string-matched control flow.
6. Non-SQL runtime effects never run before successful commit.
7. Legacy planner/executor paths and compatibility shims are deleted at cutover.

## Target Structure

Model the implementation under `packages/engine/src/sql2` as:

```text
packages/engine/src/
  sql2/
    mod.rs
    README.md
    api.rs

    planning/
      mod.rs
      parse.rs
      bind_once.rs
      plan.rs
      derive_requirements.rs
      derive_effects.rs
      lower_sql.rs
      invariants.rs
      trace.rs

    execution/
      mod.rs
      run.rs
      transaction.rs
      materialize.rs
      execute_prepared.rs
      postprocess.rs
      apply_effects_tx.rs
      apply_effects_post_commit.rs
      deferred_effects.rs

    contracts/
      mod.rs
      execution_plan.rs
      planned_statement.rs
      prepared_statement.rs
      requirements.rs
      effects.rs
      postprocess_actions.rs
      planner_error.rs
      executor_error.rs

    surfaces/
      mod.rs
      registry.rs
      lix_state/{mod.rs, planner.rs, lower.rs}
      lix_state_by_version/{mod.rs, planner.rs, lower.rs}
      lix_state_history/{mod.rs, planner.rs, lower.rs}
      filesystem/{mod.rs, planner.rs, lower.rs}
      entity/{mod.rs, planner.rs, lower.rs}

    semantics/
      mod.rs
      state_resolution/{mod.rs, canonical.rs, optimize.rs, requirements.rs, effects.rs}

    vtable/
      mod.rs
      registry.rs
      internal_state_vtable/{mod.rs, capabilities.rs, lower_read.rs, lower_write.rs, predicates.rs}

    storage/
      mod.rs
      tables/{mod.rs, state.rs, history.rs, commits.rs, filesystem.rs, entity.rs}
      queries/{mod.rs, state.rs, history.rs, commits.rs, filesystem.rs, entity.rs}

    history/
      mod.rs
      rewrite/{mod.rs, state_history.rs, file_history.rs, directory_history.rs, predicates.rs}
      requests.rs
      maintenance.rs
      projections.rs
      plugin_inputs.rs

    ast/
      mod.rs
      nodes.rs
      utils.rs
      walk.rs
```

## Run-Sized Phase Plan (One Agent Run Per Phase)

From this point forward, each phase below must be sized to fit a single agent context window and
end in one commit.

Per-phase execution rules:

1. One conceptual change only.
2. Prefer touching <= 10 files (mechanical move/delete phases can exceed this).
3. Run the phase-specific verification command before commit.
4. If verification fails, fix only regressions introduced by that phase.

### Phase 1: Lock Acceptance and Safety Tests

1. Lock integration acceptance boundary (`lix_engine` full suite).
2. Add focused tests for highest-risk semantics:
   - placeholders (`?`, `?N`, `$N`) across single statements and scripts.
   - transaction script behavior and single-bind guarantees.
   - postprocess ordering and commit-gated non-SQL effects.
   - explicit transaction `commit()` flushes queued post-commit effects exactly once.
   - explicit transaction rollback drops queued post-commit effects and applies none.
   - post-commit effect failure behavior: retry policy, idempotency guarantees, and surfaced failure reporting.
3. Add deterministic plan fingerprint tests for stable planner output.

Exit criteria:

1. Full `lix_engine` integration suite is green on baseline branch.
2. Focused risk tests exist and fail on regressions.

### Phase 2: Complete `sql2` Structure and Logic First (No Engine Cutover Yet)

This phase must fully complete the target `sql2` tree and logic before any `engine.rs` entrypoint
switching. `sql(1)` stays as-is during this phase.

#### Phase 2.1: `sql2/contracts` Completion

1. Finalize `contracts/*` including `planned_statement.rs`.
2. Remove `contracts/* -> crate::sql::*` type dependencies.
3. Verification: `cargo test -p lix_engine sql2::planning::trace::tests`.

#### Phase 2.2: `sql2/ast` Completion

1. Add `ast/{mod.rs,nodes.rs,utils.rs,walk.rs}`.
2. Move AST traversal helpers used by `planning`/`surfaces` to `sql2/ast`.
3. Verification: `cargo test -p lix_engine --lib sql2::`.

#### Phase 2.3: `sql2/surfaces` Completion

1. Add `surfaces/mod.rs` and `surfaces/registry.rs`.
2. Add `lix_state`, `lix_state_by_version`, `lix_state_history`, `filesystem`, `entity`
   submodules with `mod.rs`, `planner.rs`, `lower.rs`.
3. Port read/write rewrite logic into surfaces-owned planners/lowerers.
4. Verification: `cargo test -p lix_engine --test execute -- --test-threads=1`.

#### Phase 2.4: `sql2/semantics` Completion

1. Add `semantics/state_resolution/{canonical,optimize,requirements,effects}`.
2. Move state-resolution decisions out of ad-hoc planner logic into this layer.
3. Verification: `cargo test -p lix_engine --test state_commit_stream -- --test-threads=1`.

#### Phase 2.5: `sql2/vtable` Completion

1. Add `vtable/registry.rs` and `internal_state_vtable/*`.
2. Move vtable-specific capability and predicate logic into `sql2/vtable`.
3. Verification: `cargo test -p lix_engine --test schema_provider -- --test-threads=1`.

#### Phase 2.6: `sql2/storage` Completion

1. Add `storage/tables/*` and `storage/queries/*`.
2. Centralize SQL table/query builders used by execution and history rewrite.
3. Verification: `cargo test -p lix_engine --test file_materialization -- --test-threads=1`.

#### Phase 2.7: `sql2/history` Completion

1. Add `history/rewrite/*`, `requests.rs`, `maintenance.rs`, `projections.rs`, `plugin_inputs.rs`.
2. Move history/file-history rewrite and projection logic under `sql2/history`.
3. Verification: `cargo test -p lix_engine --test file_history_view -- --test-threads=1`.

#### Phase 2.8: `sql2/planning` Full Internalization

1. Ensure parse/bind_once/plan/derive/lower pipeline is fully `sql2`-native.
2. Remove remaining `sql2/planning/* -> crate::sql::*` dependencies.
3. Keep release invariants active in `planning/invariants.rs`.
4. Verification: `cargo test -p lix_engine --test transaction_execution -- --test-threads=1`.

#### Phase 2.9: `sql2/execution` Full Internalization

1. Finalize `execution/{run,transaction,materialize,postprocess,apply_effects_*}`.
2. Remove remaining `sql2/execution/* -> crate::sql::*` dependencies.
3. Define typed postprocess and post-commit failure/retry/idempotency behavior.
4. Verification: `cargo test -p lix_engine --test deterministic_mode -- --test-threads=1`.

#### Phase 2.10: Pre-Cutover Gate

1. Confirm `sql2` tree exists and is wired internally:
   - `api`, `planning`, `execution`, `contracts`, `surfaces`, `semantics`, `vtable`, `storage`, `history`, `ast`.
2. Confirm runtime flow exists in `sql2`:
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`.
3. Verification: `cargo test -p lix_engine`.

Phase 2 exit criteria:

1. `sql2` structure and logic are complete across the full target tree.
2. `sql2` internals do not rely on `crate::sql::*` for planner/executor runtime behavior.
3. Full `lix_engine` integration suite is green before engine entrypoint cutover.

### Phase 3: Engine Cutover and Legacy Runtime Deletion (After Phase 2 Completes)

#### Phase 3.1: Engine API Cutover

1. Route `Engine::execute*` entrypoints through `sql2/api.rs`.
2. Keep `sql(1)` sources available temporarily as non-runtime fallback only if still required.
3. Verification: `cargo test -p lix_engine --test execute -- --test-threads=1`.

#### Phase 3.2: Remove Compatibility Shims (Part 1)

1. Delete `PreprocessOutput.params` flattening compatibility behavior.
2. Remove legacy placeholder advancement helpers superseded by `bind_once`.
3. Verification: `cargo test -p lix_engine --test transaction_execution -- --test-threads=1`.

#### Phase 3.3: Remove Compatibility Shims (Part 2)

1. Replace string-based fallback checks with typed planner/executor errors.
2. Remove helpers in the style of `is_postprocess_multi_statement_error`.
3. Verification: `cargo test -p lix_engine --test sql2_guardrails -- --test-threads=1`.

#### Phase 3.4: Remove Legacy Build-Graph Wiring

1. Remove legacy planner/executor imports and wiring from `packages/engine/src/engine.rs`.
2. Keep `sql(1)` sources only if still needed by non-runtime utilities/tests; otherwise mark for deletion in next phase.
3. Verification: `cargo test -p lix_engine --test state_commit_stream -- --test-threads=1`.

#### Phase 3.5: Delete Legacy Runtime Stack

1. Delete `packages/engine/src/execute/*` (if present) and any old planner/pipeline orchestration paths replaced by `sql2`.
2. Ensure removed modules are out of build graph and no lingering imports remain.
3. Verification: `cargo test -p lix_engine --tests --no-run`.

Phase 3 exit criteria:

1. Runtime uses only `sql2` path.
2. Legacy planner/executor runtime modules are removed from build graph.
3. Full `lix_engine` integration suite is green after cutover.

### Phase 4: Stabilize, Harden, and Guardrail (Microphases)

#### Phase 4.1: Regression Burn-Down

1. Fix cutover regressions until targeted suites are green.
2. Verification: rerun failing suites first, then `cargo test -p lix_engine --tests --no-run`.

#### Phase 4.2: Full Integration Validation

1. Run full integration acceptance suite.
2. Verification: `cargo test -p lix_engine`.

#### Phase 4.3: Add Guardrails

1. Add CI checks to prevent legacy runtime path reintroduction.
2. Add symbol/import guardrails for forbidden `crate::sql::*` runtime dependencies.
3. Verification: guardrail tests plus CI-local check command.

#### Phase 4.4: Codebase Hygiene

1. Split oversized modules and enforce ownership boundaries.
2. Keep public behavior unchanged; only structural cleanup.
3. Verification: `cargo test -p lix_engine`.

Phase 4 exit criteria:

1. Required CI runs are green on `sql2` only.
2. Guardrails prevent legacy-path reintroduction.
3. Final runtime flow matches:
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`.

## Definition of Done

1. Engine executes through one planner API and one executor pipeline (`sql2` only).
2. Placeholder rebinding loops are eliminated.
3. String-based fallback control flow is eliminated.
4. Planner is pure; side effects are driven by typed contracts.
5. Postprocess ordering is enforced and test-covered:
   `postprocess_sql -> apply_effects_tx -> commit boundary -> apply_effects_post_commit`.
6. Full `lix_engine` integration tests pass.
