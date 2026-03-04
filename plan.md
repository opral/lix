# Revised Plan: Canonical State Wrappers + Entity View Wrapper Collapse

## Summary
`lix_state` will become a strict convenience wrapper for active-version access only.  
All `lix_state` reads and writes route through `lix_state_by_version` semantics, with active version injected by the wrapper.  
`version_id` is treated as non-existent on `lix_state` across all statement types.

Entity views follow the same wrapper model and keep no independent read semantics:
- `entity_x` reads from `lix_state`
- `entity_x_by_version` reads from `lix_state_by_version`
- `entity_x_history` reads from `lix_state_history`

Entity-view read rewrite should be wrapper-first:
- projection + alias mapping (`lix_json_extract(...)` + `lixcol_*` aliases) is the primary behavior.
- keep only required semantics that wrappers still need:
  - schema-declared `x-lix-override-lixcols` predicates, and
  - history root/version predicate pushdown needed to scope timeline queries correctly.

## Important API / Interface Changes
1. `lix_state` no longer exposes `version_id` as an accessible column in practice or schema metadata.
2. Any `lix_state` reference to `version_id` or `lixcol_version_id` is rejected for:
3. `SELECT` projection and predicates.
4. `UPDATE` predicates and assignments.
5. `DELETE` predicates.
6. `INSERT` column list.
7. `lix_state` keeps active-version injection behavior and delegates execution semantics to `lix_state_by_version`.
8. Internal schema metadata for `lix_state` is updated to remove `version_id`.
9. Entity-view reads become thin wrappers over the corresponding state view family:
10. `<schema>` -> `lix_state`
11. `<schema>_by_version` -> `lix_state_by_version`
12. `<schema>_history` -> `lix_state_history`
13. Entity-view read rewrite retains only:
14. `schema_key` scoping predicate.
15. property extraction from `snapshot_content`.
16. `lixcol_*` alias projection for the variant.
17. schema-declared override predicates (where present).
18. minimal history root/version pushdown required by `lix_state_history` semantics.
19. Any additional entity-view-specific branching beyond those constraints is removed.

## Implementation Plan

1. Rework `lix_state` read rewrite into wrapper routing in [lix_state_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_read.rs).
2. Replace active-state bespoke lowering with wrapper lowering to a derived query over `lix_state_by_version`.
3. Derived query must project only `lix_state` columns (exclude `version_id` and `lixcol_version_id`).
4. Derived query must include active-version predicate equivalent to `version_id = <active_version_id>`.
5. Add explicit rewrite-time validation that rejects `version_id`/`lixcol_version_id` references targeting `lix_state`.

6. Keep query canonical ordering (`lix_state` rule before `lix_state_by_version`) and rely on existing second rule to lower the wrapper subquery in [logical_views.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/query/canonical/logical_views.rs).
7. Add a code comment in `logical_views` documenting this ordering dependency.

8. Rework `lix_state` write rewrite into wrapper-only behavior in [lix_state_view_write.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_write.rs).
9. Preserve explicit `version_id` rejection for `INSERT` and `UPDATE` assignments.
10. Add rejection for `version_id`/`lixcol_version_id` in `UPDATE/DELETE` predicates on `lix_state`.
11. Remove wrapper-only extras you chose to drop: untracked auto-probe injection and inherited-filter special stripping.
12. Canonical output from wrapper should target `lix_state_by_version` form with active-version predicate injected.

13. Reorder statement canonical routing so `lix_state` wrapper runs before by-version canonicalization in [canonical mod.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/mod.rs).
14. Keep single semantic core in by-version rewrite and vtable write path.
15. Remove no-longer-needed inherited-selection compatibility hook in [canonical vtable_write.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/vtable_write.rs) if unused after cutover.

16. Update internal schema whitelist for `lix_state` in [schema provider](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/schema/provider.rs) to remove `version_id` from `properties` and `required`.

17. Update architecture doc text in [architecture-current-state.md](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/docs/architecture-current-state.md) to state that `lix_state` is read/write wrapper over by-version semantics with active-version injection.

18. Simplify entity-view read rewrite in [entity_views/read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/entity_views/read.rs):
19. Keep history pushdown limited to root/version scoping predicates; remove unrelated branching.
20. Build wrapper query directly from the variant-selected source view (`lix_state`, `lix_state_by_version`, `lix_state_history`) plus `schema_key` scoping.
21. Keep only field extraction and `lixcol_*` projection mapping in entity-view rewrite.

22. Keep entity-view target resolution in [entity_views/target.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/entity_views/target.rs) focused on view-to-variant/schema mapping and projection metadata.
23. Remove read-only target metadata branches no longer needed once entity-view read logic is wrapper-only.

24. Update architecture doc text in [architecture-current-state.md](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/docs/architecture-current-state.md) to describe entity views as thin wrappers over state views (no separate read semantics).

## Test Cases and Scenarios

1. Add read rejection tests in [state_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/state_view.rs):
2. `SELECT version_id FROM lix_state` fails with deterministic error.
3. `SELECT lixcol_version_id FROM lix_state` fails.
4. `SELECT * FROM lix_state` remains valid.
5. `SELECT 1 FROM (SELECT * FROM lix_state) s WHERE s.version_id IS NOT NULL` fails.

6. Add predicate rejection tests in `state_view.rs`:
7. `UPDATE lix_state ... WHERE version_id = ...` fails.
8. `DELETE FROM lix_state WHERE version_id = ...` fails.

9. Keep and verify assignment/insert rejection tests in `state_view.rs`:
10. `INSERT ... version_id` rejected.
11. `UPDATE ... SET version_id = ...` rejected.

12. Update inheritance tests in [state_inheritance.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/state_inheritance.rs) to stop projecting `version_id` from `lix_state`; assert version via `lix_state_by_version` or internal rows where needed.

13. Add read-wrapper routing unit tests in [lix_state_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_read.rs):
14. Rewritten SQL includes `lix_state_by_version` source and active-version predicate.
15. Rewritten projection excludes `version_id`.

16. Update any query-analysis tests that currently encode `version_id` on `lix_state` as valid, including [dependency_spec.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/dependency_spec.rs), to align with the new contract.

17. Run focused suites:
18. `cargo test -p lix_engine --test state_view`
19. `cargo test -p lix_engine --test state_inheritance`
20. `cargo test -p lix_engine --test state_by_version_view`
21. `cargo test -p lix_engine --test on_conflict_views`
22. `cargo test -p lix_engine`

23. Add/adjust entity-view read tests in [entity_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/entity_view.rs) and [entity_history_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/entity_history_view.rs):
24. base entity views read through `lix_state` wrapper semantics.
25. by-version entity views read through `lix_state_by_version`.
26. history entity views read through `lix_state_history`.
27. no behavior divergence from the underlying state view beyond property/alias projection.

## Assumptions and Defaults
1. Single cutover rollout with no compatibility flag.
2. Error code class remains existing rewrite error class (`LIX_ERROR_UNKNOWN`) unless a project-wide typed error migration is done separately.
3. `lix_state` remains writable/readable, but only as active-version wrapper semantics.
4. Any existing caller depending on `version_id` through `lix_state` must migrate to `lix_state_by_version`.
5. This plan change targets entity-view read architecture; entity-view write-path simplification is out of scope unless requested separately.


## progress log

- 2026-03-04: Started implementation loop; verified current repo state and loaded all target files from the plan.
- 2026-03-04: Reworked `lix_state` write rewrite to wrapper mode in `lix_state_view_write.rs`:
  - rewrites now target `lix_state_by_version` with active `version_id` injection.
  - explicit `version_id` assignment/insert still rejected.
  - predicate references to `version_id` on `lix_state` now rejected for `UPDATE`/`DELETE`.
  - removed wrapper-only untracked auto-probe and inherited predicate stripping logic.
- 2026-03-04: Reworked `lix_state` read rewrite to wrapper mode in `lix_state_view_read.rs`:
  - reads route through `lix_state_by_version` with active-version filter.
  - wrapper projection excludes `version_id`.
  - explicit `version_id`/`lixcol_version_id` references are now rejected in `lix_state` read expressions.
- 2026-03-04: Updated canonical ordering comments and backend statement routing so `lix_state` wrapper runs before `lix_state_by_version` canonicalization.
- 2026-03-04: Removed obsolete inherited-selection compatibility helper from canonical `vtable_write` rule.
- 2026-03-04: Updated internal `lix_state` schema whitelist in `schema/provider.rs` to remove `version_id`.
- 2026-03-04: Updated architecture doc text to reflect wrapper-based `lix_state` semantics.
- 2026-03-04: Updated dependent tests and fixtures (`engine.rs`, `state_inheritance.rs`, `dependency_spec.rs`, and `state_view.rs`) for the new contract; test execution is next.
- 2026-03-04: Updated plan to collapse entity-view read architecture to thin wrappers over `lix_state` / `lix_state_by_version` / `lix_state_history`, keeping only projection + `lixcol_*` mapping logic.
- 2026-03-04: Implemented entity-view read wrapper collapse in `entity_views/read.rs`:
  - base/by-version/history views route to `lix_state` / `lix_state_by_version` / `lix_state_history`.
  - projection remains JSON extraction + `lixcol_*` alias mapping.
  - retained schema override predicates and required history root/version pushdown for correctness.
- 2026-03-04: Fixed checkpoint regression by adding scoped predicate pushdown in `lix_state_view_read.rs` for simple single-relation `lix_state` queries so wrapper filters reach underlying by-version rewrite without changing join-heavy behavior.
- 2026-03-04: Added/updated wrapper read tests in `lix_state_view_read.rs` and validated focused suites:
  - `checkpoint`, `state_view`, `entity_view`, `entity_history_view`, `state_inheritance`, `state_by_version_view`, `on_conflict_views`, `file_history_view`, `file_materialization` all passed.
- 2026-03-04: Full `cargo test -p lix_engine` run is partially blocked by local environment limits for embedded Postgres (`FATAL: could not create shared memory segment: No space left on device`) after repeated full runs; logic-level regressions introduced by this change set were not observed in targeted suites.
- 2026-03-04: Added regression test `vtable_update_without_untracked_predicate_updates_effective_untracked_row` in `packages/engine/tests/vtable_write.rs` to encode desired effective-row semantics for vtable updates without explicit `untracked` predicate. Verified red-state failure on SQLite/materialization (visible row stayed `{\"key\":\"untracked-initial\"}` instead of `{\"key\":\"effective-updated\"}`); Postgres variant remains environment-blocked by shared-memory limits.
- 2026-03-04: Investigated embedded Postgres startup failures (`could not create shared memory segment`) and confirmed macOS SysV shared-memory ID exhaustion (`kern.sysv.shmmni=32` with all 32 IDs occupied by stale `NATTCH=0` segments). Added automatic stale SysV IPC cleanup to `packages/engine/tests/support/simulations/postgres.rs` before simulation startup. Re-ran the regression test and verified Postgres boots; test now fails consistently on intended semantic assertion across sqlite/materialization/postgres.
- 2026-03-04: Implemented vtable effective-update semantics in `packages/engine/src/sql/planning/rewrite_engine/steps/vtable_write.rs`:
  - updates without explicit `untracked` predicate now execute in effective scope by emitting a pre-update to `lix_internal_state_untracked` and a tracked update guarded by `NOT EXISTS` shadow check against untracked keys.
  - explicit `untracked = true` and `untracked = false` routing behavior remains supported and unchanged.
- 2026-03-04: Updated canonical vtable update output to carry multi-statement effective updates plus `VtableUpdate` postprocess and multiple update validations in `packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/statement/canonical/vtable_write.rs`.
- 2026-03-04: Fixed planner invariant/validation guard regression where `None` postprocess incorrectly required a single rewritten statement. Updated helpers in:
  - `packages/engine/src/sql/planning/preprocess.rs`
  - `packages/engine/src/sql/planning/invariants.rs`
  - `packages/engine/src/sql/planning/rewrite_engine/pipeline/validator.rs`
  - `packages/engine/src/sql/planning/rewrite_engine/pipeline.rs` (test helper)
- 2026-03-04: Fixed correlated shadow predicate generation to reference the materialized target table directly (instead of synthetic alias `__lix_mat`) so SQLite/Postgres execute consistently.
- 2026-03-04: Re-ran targeted regression:
  - `cargo test -p lix_engine vtable_update_without_untracked_predicate_updates_effective_untracked_row -- --nocapture`
  - result: passed on sqlite, materialization, and postgres backends.
- 2026-03-04: Ran full vtable write suite after semantic and planner-guard changes:
  - `cargo test -p lix_engine --test vtable_write`
  - result: `53 passed; 0 failed`.
- 2026-03-04: Added additional unsoundness regressions before fixes:
  - alias-qualified effective update (`UPDATE ... AS s`) must work.
  - implicit effective update with key mutation must not mutate shadowed tracked rows.
  - implicit delete must target effective row (delete untracked shadow first-class).
  - unsupported `untracked` predicate shapes (`OR`) must be rejected instead of misrouted.
- 2026-03-04: Implemented vtable soundness fixes in `steps/vtable_write.rs`:
  - strict `untracked` predicate stripping now rejects non-conjunctive shapes that cannot be safely stripped.
  - update/delete rewrite now strips target-table alias qualifiers from predicates/assignments and removes table alias at rewrite time to avoid backend alias rendering mismatches.
  - effective update shadow predicate now compares untracked keys against post-update key expressions (assignment-aware), fixing key-mutation shadow leaks.
  - implicit delete now uses effective scope by default (`NOT EXISTS` shadow guard on tracked tombstone update).
- 2026-03-04: Implemented effective-delete untracked cleanup in `sql/execution/followup.rs`:
  - when effective-scope selection SQL is present, followup now executes `DELETE FROM lix_internal_state_untracked WHERE <selection>`.
  - keeps tracked tombstone statement as the postprocess row source while ensuring effective delete semantics.
- 2026-03-04: Verified new regressions and core suites:
  - `cargo test -p lix_engine --test vtable_write` -> `62 passed; 0 failed`.
  - `cargo test -p lix_engine --test state_view` -> `69 passed; 0 failed`.
  - `cargo test -p lix_engine --test state_by_version_view` -> `54 passed; 0 failed`.
- 2026-03-04: Added explicit wrapper-level regression for the reported `lix_state` behavior:
  - `lix_state_update_without_untracked_predicate_updates_effective_untracked_row` in `packages/engine/tests/state_view.rs`.
  - verifies `UPDATE lix_state` without `untracked` updates the effective untracked row and leaves tracked materialized row unchanged.
  - targeted run: `cargo test -p lix_engine --test state_view lix_state_update_without_untracked_predicate_updates_effective_untracked_row -- --nocapture` -> passed on sqlite/materialization/postgres.
