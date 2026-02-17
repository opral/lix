# Plan: Unify Read Semantics In `vtable_read.rs`

## Goal
Unify effective-state read planning in `packages/engine/src/sql/steps/vtable_read.rs` so `lix_state`, `lix_state_by_version`, and entity base reads stop duplicating inheritance/ranking/count/commit optimization logic.

## Why
Current read performance and regressions come from split logic:
1. `vtable_read` handles routed raw state union and per-version winner selection.
2. `lix_state_by_version_view_read` adds inheritance recursion and a second winner pass.
3. `lix_state_view_read` duplicates that with active-version seeding.
4. `entity_views/read` has a third embedded inheritance query path for base entity views.

This duplicates optimization work and makes pushdown correctness fragile.

## Architectural Direction
Keep views as shape + naming layers, not semantic planners.

Semantics move to one planner in `vtable_read`:
1. infer scope from query AST (no user-facing flags required),
2. build one canonical effective-state query shape,
3. apply pushdown once,
4. emit backend-specific SQL from one code path.

## Semantic Invariants (must not change)
1. For `lix_state_by_version`, `version_id = X` means effective visible state at `X` with inheritance.
2. Direct-only reads remain expressible via `version_id = X AND inherited_from_version_id IS NULL`.
3. `lix_state` remains active-version scoped view semantics.
4. Untracked-over-materialized winner behavior remains unchanged.
5. Commit mapping appears only when query references commit columns.

## Out Of Scope
1. Filesystem table rewrites in `filesystem_step`.
2. Write path changes (`vtable_write` and state writes).
3. Entity view target/schema resolution in `entity_views/target.rs`.

## Migration Plan

## Phase 0: Baseline + Guardrails
1. Freeze baseline benches:
   - `cargo bench -p lix_engine --bench lix_state_by_version -- --noplot --quick`
   - `cargo bench -p lix_engine --bench lix_file -- --noplot --quick`
2. Add dedicated explain/plan snapshots for:
   - `eq version`,
   - `IN version list`,
   - active scope subquery.
3. Add regression tests for:
   - inheritance correctness,
   - bind placeholder ordering,
   - `inherited_from_version_id IS NULL` direct-read behavior.

Exit criteria:
1. Baseline numbers and plans are captured in benchmark notes.
2. New guards fail on known previous regressions.

## Phase 1: Add Canonical Read Planner In `vtable_read`
1. Introduce internal query classification in `vtable_read`:
   - raw state read,
   - effective-by-version read,
   - active-effective read.
2. AST-driven inference only:
   - infer active scope from `lix_state` source shape or active-version subquery pattern,
   - infer target versions from `=`, `IN (...)`, `IN (subquery)`,
   - detect direct-only via `inherited_from_version_id IS NULL`,
   - detect commit dependency via projection/filters/order/group references.
3. Build canonical CTE generator for effective reads:
   - `target_versions`,
   - `version_chain`,
   - winner selection once,
   - optional commit mapping CTEs only when needed.

Exit criteria:
1. Canonical planner can generate current-equivalent SQL for all three scopes.
2. No behavior changes in existing read tests.

## Phase 2: Make `lix_state_by_version_view_read` Shape-Only
1. Remove recursion/ranking/commit SQL generation from
   `packages/engine/src/sql/steps/lix_state_by_version_view_read.rs`.
2. Rewrite to canonical source that `vtable_read` classifies as effective-by-version.
3. Keep only alias handling and minimal filter pass-through.

Exit criteria:
1. `state_by_version_view` test suite passes.
2. Existing `pushes_version_id_*` rewrite tests pass after expected SQL-shape updates.
3. Active-scope benchmark is no worse than current best.

## Phase 3: Make `lix_state_view_read` Shape-Only
1. Remove active-version recursion/ranking/commit SQL generation from
   `packages/engine/src/sql/steps/lix_state_view_read.rs`.
2. Route `lix_state` to canonical active-effective source shape.
3. Let `vtable_read` infer and plan active-version seed centrally.

Exit criteria:
1. `lix_state` read tests pass.
2. No fallback to duplicated recursion SQL.

## Phase 4: Remove Entity Base Embedded Inheritance SQL
1. Delete `base_state_source_sql` heavy query assembly in
   `packages/engine/src/sql/entity_views/read.rs`.
2. Entity base views should source from `lix_state` or canonical equivalent only.
3. Keep only schema-driven projection and override predicates.

Exit criteria:
1. Entity view parity tests pass.
2. No third inheritance planner remains in entity view read path.

## Phase 5: Consolidate Pushdown In One Place
1. Move source/ranked pushdown semantics into `vtable_read` planner.
2. Keep parameter-aware narrowing:
   - `schema_key = ?`,
   - `version_id = ?`,
   - `version_id IN (?, ...)`,
   - `version_id IN (subquery)`.
3. Eliminate duplicated pushdown logic from view-specific read rewrites.

Exit criteria:
1. Placeholder ordering tests pass.
2. Explain plans show reduced repeated broad unions.

## Phase 6: Cleanup + Hardening
1. Remove dead helpers no longer used by thin view rewrites.
2. Document canonical read planner contract in code comments.
3. Add perf regression benches focused on:
   - active scope subquery,
   - direct-only reads,
   - commit_id projected vs not projected.

Exit criteria:
1. Clean compile with no dead-path warnings from removed planner fragments.
2. Bench thresholds are enforced in CI notes.

## Query Planning Rules To Enforce
1. Build `target_versions` directly from resolved constraints; avoid broad `all_target_versions` then filtering.
2. Evaluate active scope once and reuse.
3. Never materialize commit/change CTEs when commit columns are not referenced.
4. Apply schema narrowing before table-union expansion.
5. Preserve semantic correctness before micro-optimization.

## Test Matrix
1. Correctness:
   - `state_by_version_view`,
   - `state_view`,
   - `entity_view`,
   - inheritance-specific tests.
2. Performance:
   - `lix_state_by_version` bench suite,
   - `lix_file` bench suite.
3. Planner behavior:
   - explain plan snapshots for top 3 hot query shapes.

## Success Metrics
1. No duplicated recursion/winner planner remains outside `vtable_read`.
2. `lix_state_by_version_count_active_scope_subquery` remains sub-100ms target.
3. No regression in inheritance correctness tests.
4. Simplified maintenance: one planner to optimize for all view entry points.

## Implementation Order Recommendation
1. Phase 1 and 2 first.
2. Phase 3 and 4 second.
3. Phase 5 and 6 third.

This order minimizes blast radius while quickly removing the heaviest duplicate planner (`lix_state_by_version_view_read`).

## Benchmark Baseline (2026-02-16 13:21:13-13:29:00 PST)

Commands run:
1. `cargo bench -p lix_engine --bench lix_state -- --noplot --quick`
2. `cargo bench -p lix_engine --bench lix_state_by_version -- --noplot --quick`
3. `cargo bench -p lix_engine --bench lix_file -- --noplot --quick`

Notes:
1. Numbers below are Criterion quick-mode estimates.
2. `Baseline` is from before read-unification work.
3. `After` is from the latest implementation pass.

### `lix_state` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_state_count_no_inheritance` | 11.897 ms | 11.699 ms |
| `lix_state_count_inherited` | 11.595 ms | 11.905 ms |

### `lix_state_by_version` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_state_by_version_count_eq_version` | 3.959 ms | 4.225 ms |
| `lix_state_by_version_count_in_version_list` | 5.137 ms | 3.770 ms |
| `lix_state_by_version_count_active_scope_subquery` | 296.50 ms | 133.14 ms |

### `lix_file` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_file_insert_no_plugin` | 2.149 ms | 1.881 ms |
| `lix_file_insert_plugin_json` | 6.592 ms | 8.247 ms |
| `lix_file_exact_delete_missing_ids` | 636.06 µs | 594.46 µs |
| `lix_file_exact_update_missing_id` | 2.846 s | 2.726 s |
| `lix_file_read_scan_path_data_no_plugin` | 595.76 ms | 576.81 ms |
| `lix_file_read_scan_path_data_plugin_json` | 873.23 ms | 477.95 ms |
| `lix_file_read_point_path_data_no_plugin` | 755.69 ms | 687.17 ms |
| `lix_file_read_point_path_data_plugin_json` | 1.324 s | 614.20 ms |
