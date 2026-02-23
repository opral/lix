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

## Simplified 4-Phase Plan

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

### Phase 2: Implement `sql2` End-to-End (No Legacy Routing)

1. Implement full `sql2` lifecycle:
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`.
2. Implement typed planner/executor contracts and typed errors from day one.
3. Implement release-mode invariants in `sql2/planning/invariants.rs`.
4. Implement unified runner for direct execute, scripts, and transaction API.
5. Implement explicit post-commit effect semantics:
   - define idempotency contract for non-SQL effects.
   - define retry policy and failure reporting for `apply_effects_post_commit`.
   - if durable retry is required, persist pending post-commit effects before applying.

Exit criteria:

1. `sql2` is functionally complete and used by a callable API (`sql2/api.rs`).
2. No string-matched fallback logic exists in `sql2`.
3. Invariants run in release builds.
4. Pre-cutover gate: full `lix_engine` integration suite is green with engine entrypoints routed through `sql2` (test/branch configuration), before legacy deletion.

### Phase 3: Single Cutover and Legacy Deletion

1. Switch engine entrypoints to `sql2/api.rs`.
2. Delete legacy execution stack under `packages/engine/src/execute/*`.
3. Delete old planner/pipeline orchestration paths replaced by `sql2`.
4. Remove compatibility shims:
   - `PreprocessOutput.params` flattening behavior.
   - legacy placeholder advancement helpers.
   - string-based fallback helpers (`is_postprocess_multi_statement_error` style checks).
5. Remove legacy module wiring/imports from `packages/engine/src/engine.rs`.

Exit criteria:

1. Runtime uses only `sql2` path.
2. Legacy planner/executor modules are removed from build graph.
3. Full `lix_engine` integration suite is green after cutover.

### Phase 4: Stabilize, Harden, and Guardrail

1. Fix all regressions until integration suite is consistently green.
2. Run and harden placeholder/property/determinism suites.
3. Add CI guardrails (`rg` checks) preventing reintroduction of legacy symbols and legacy execute calls.
4. Enforce file-size and ownership hygiene while splitting oversized modules.

Exit criteria:

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
