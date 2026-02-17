# Plan: Introduce `vtable_read` Planner

## Goal
Move read semantics from ad-hoc SQL string assembly into a planner pipeline in `vtable_read`, while keeping emitted output as standard SQL AST/query text.

## Why
Current read behavior is spread across multiple rewrites and view-specific query builders:
1. `vtable_read` does schema expansion + some pushdown.
2. `lix_state_by_version_view_read` and `lix_state_view_read` still own semantic planning.
3. `lix_state_history_view_read` has its own planning logic.

This creates duplicate optimization work, inconsistent pushdown behavior, and higher regression risk.

## Architecture

### Core Principle
Planner output remains a normal SQL AST (`sqlparser::ast::Query`) that we serialize and execute through the current pipeline.

### Pipeline
1. `AST -> VtableReadOp` (normalize intent)
2. `VtableReadOp -> VtableReadPlan` (semantic planning)
3. `VtableReadPlan -> Query AST` (emit canonical query)
4. existing lowering/binding/backend execute

## Scope Of This Plan
1. Build `vtable_read` planner foundations.
2. Migrate state read semantics to planner-driven flow.
3. Keep behavior parity and placeholder safety.

## Non-Goals (for this phase)
1. Changing write-path semantics (`vtable_write`).
2. Changing filesystem/file view semantics yet.
3. Introducing custom runtime executors.

## Semantic Invariants
1. `lix_state_by_version` remains effective-state semantics by default.
2. direct-only behavior via `inherited_from_version_id IS NULL` remains valid.
3. `lix_state` remains active-version scoped effective-state view.
4. commit_id exposure remains optional and demand-driven.
5. Placeholder binding semantics (`?`, `?1`, `$1`) remain stable.

## Planner Components

### `VtableReadOp`
Normalized intent fields:
1. mode: `raw`, `effective_by_version`, `effective_active`, `history`
2. projection class: `count_only`, `light`, `full`
3. required semantic columns: `needs_commit_info`, `needs_inherited_marker`, etc.
4. filter set: `schema_key`, `entity_id`, `file_id`, `version_id`, `root_commit_id`, `depth`, generic predicates
5. order/limit requirements

### `VtableReadPlan`
Chosen plan shape fields:
1. scope seed strategy (`active pointer`, explicit versions, subquery versions)
2. chain/winner strategy
3. pushdown placement decisions by CTE stage
4. fast-path choice (`count fast path`, `single-version fast path`, etc.)
5. placeholder safety mode

### Emission
1. Produce canonical CTE/query AST from `VtableReadPlan`.
2. Ensure deterministic SQL shape to make EXPLAIN/bench comparisons stable.

## Migration Phases

## Phase 0: Guardrails + Baseline
1. Lock baseline numbers for:
   - `lix_state`
   - `lix_state_by_version`
   - `lix_state_history`
2. Keep existing tests as semantic guardrails.

Exit criteria:
1. Baselines recorded.
2. All existing tests green before migration.

## Phase 1: Planner Skeleton
1. Add planner module namespace and data models (`VtableReadOp`, `VtableReadPlan`).
2. Build AST-to-op normalization with zero behavior change.
3. Add plan debug snapshots/tests.

Exit criteria:
1. planner can represent current query classes.
2. no behavior change yet.

## Phase 2: Planner Emission Parity
1. Emit current-equivalent SQL through planner for selected read classes.
2. route existing `vtable_read` rewrite through planner emitter.
3. keep fallback path for unmigrated shapes.

Exit criteria:
1. parity tests pass for migrated classes.
2. explain shape stable for baseline queries.

## Phase 3: Migrate State View Rewrites
1. make `lix_state_by_version_view_read` shape-only normalizer.
2. make `lix_state_view_read` shape-only normalizer.
3. keep semantic logic centralized in planner.

Exit criteria:
1. duplicated semantic planning removed from these steps.
2. `state_by_version_view` and `state_view` suites stay green.

## Phase 4: Migrate History Read Semantics
1. move remaining `lix_state_history` planning decisions behind planner.
2. preserve count/timeline fast paths.

Exit criteria:
1. `state_history_view` suite green.
2. no regressions in history benchmarks.

## Phase 5: Optimize + Harden
1. tighten pushdown ordering and fast-path heuristics.
2. reduce temp-btree/row_number pressure where safe.
3. add planner-specific placeholder-order regression tests.

Exit criteria:
1. measurable improvements vs baseline.
2. no semantic regressions.

## Success Metrics
1. one semantic read planner for state/state_by_version/state_history flows.
2. no duplicated optimization logic across these view rewrites.
3. improved latency on active-scope and history-heavy queries.

## Implementation Status (2026-02-17)
1. Phase 1 done: planner module + `VtableReadOp`/`VtableReadPlan` scaffolding.
2. Phase 2 done: `vtable_read` rewrite now delegates through planner inference/plan/emission.
3. Phase 3 (partial) done: effective-state SQL generation moved to planner (`sql/planner/effective_state_read.rs`) and `lix_state`/`lix_state_by_version` rewrites now call planner directly.
4. Phase 4 done: `lix_state_history` semantic planning moved to `sql/planner/state_history_read.rs`; `lix_state_history_view_read` is now a shape-only wrapper.
5. Phase 5 pending: no additional pushdown/row-number heuristic changes yet.

## Benchmark Baseline (2026-02-16)

Commands run:
1. `cargo bench -p lix_engine --bench lix_state -- --noplot --quick`
2. `cargo bench -p lix_engine --bench lix_state_by_version -- --noplot --quick`
3. `cargo bench -p lix_engine --bench lix_state_history -- --noplot --quick`

### `lix_state` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_state_count_no_inheritance` | 12.160 ms | 10.847 ms |
| `lix_state_count_inherited` | 12.376 ms | 12.156 ms |

### `lix_state_by_version` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_state_by_version_count_eq_version` | 4.6327 ms | 2.7656 ms |
| `lix_state_by_version_count_in_version_list` | 5.3414 ms | 2.6909 ms |
| `lix_state_by_version_count_active_scope_subquery` | 118.44 ms | 140.65 ms |

### `lix_state_history` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_state_history_count_by_root_commit` | 16.995 ms | 17.118 ms |
| `lix_state_history_entity_timeline_scan` | 14.918 ms | 14.920 ms |
