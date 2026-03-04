# Single-Cutover Read Simplification: AST Pushdown + Unified `by_version` Core

## Summary

This plan performs a single cutover (no feature flag) to simplify read architecture around one semantic core:

1. `lix_state_by_version` is the only state-read semantic core.
2. `lix_state` is only an adapter: inject active `version_id`, hide `version_id`.
3. Entity views are pure projection adapters over `lix_state` / `lix_state_by_version` / `lix_state_history`.
4. Predicate pushdown becomes AST-based (`Expr`), not SQL-string based.
5. Wrapper projections are column-pruned so `commit_id` is only projected when required.

Chosen strategy and defaults:

- Rollout: **Single Cutover**
- Pushdown scope: **Conservative** (`AND` conjunctions of safe forms only)

## Decisions Locked

1. No public SQL contract change for visible columns beyond existing `lix_state` rule (`version_id` remains hidden/rejected).
2. `SELECT *` and wildcard queries keep current behavior; conservative fallback may keep full projection.
3. Commit mapping should run only when query shape actually requires `commit_id` (or fallback complexity policy says “safe to keep on”).
4. No partial migration: old string pushdown path is removed in same cutover.

## Important Interface / Type Changes

1. Replace string pushdown carrier:

- From: `StatePushdown { source_predicates: Vec<String>, ranked_predicates: Vec<String> }`
- To: AST carrier with explicit bucketed predicates (for example `Vec<Expr>` per bucket plus metadata).

2. Add column-usage analysis API for relation-local pruning:

- New helper that returns required columns for a target relation from a `Select`.
- Includes projection, filters, grouping, ordering, having/qualify, and wildcard fallback.

3. Wrapper-query builders switch to AST construction:

- `lix_state` wrapper query builder in [lix_state_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_read.rs)
- Entity wrapper query builder in [entity_views/read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/entity_views/read.rs)

4. Remove dead active-read constructor from [vtable_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/vtable_read.rs):

- `build_effective_state_active_query` (unused) and related dead code.

## Implementation Plan

1. Introduce AST pushdown primitives in [state_pushdown.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/state_pushdown.rs).

- Keep current conservative extraction policy (`Eq`, `InList`, `InSubquery`, `IsNull/IsNotNull` under `AND`).
- Return extracted predicates as `Expr` plus bucket (`Source` vs `Ranked`), not rendered SQL strings.
- Keep non-extracted predicates in outer `selection` unchanged.
- Add AST qualifier rewrite helper to retarget extracted expressions to aliases (`s` or `ranked`) without string replacement.

2. Add relation column-usage analysis helper (new module under rewrite engine, e.g. `steps/column_usage.rs`).

- Inputs: `Select`, relation alias/name, `allow_unqualified`.
- Output: required canonical state columns.
- Conservative fallback to `AllColumns` when any of the following hold:
  - wildcard projection,
  - complex shapes where reliable column inference is unsafe,
  - ambiguous multi-relation references.
- Ensure this helper is reusable by both `lix_state` and entity wrappers.

3. Rewrite `lix_state` read wrapper to AST builder in [lix_state_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_read.rs).

- Remove `build_lix_state_wrapper_query(extra_predicates: &[String])`.
- Build `Query` AST directly:
  - `FROM lix_state_by_version AS s`
  - `WHERE s.version_id = <active-version-subquery>`
  - plus extracted source/ranked-safe pushdowns re-targeted for wrapper scope.
- Apply column pruning:
  - project only required visible `lix_state` columns for this relation.
  - include `commit_id` only when required.
  - never include `version_id`/`lixcol_version_id`.
- Preserve existing explicit rejection of `version_id`/`lixcol_version_id` references.

4. Rewrite entity adapters to AST in [entity_views/read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/entity_views/read.rs).

- Remove SQL string assembly/parsing for wrapper query generation.
- Build AST projections:
  - property extraction via `lix_json_extract(snapshot_content, ...) AS <prop>`
  - `lixcol_*` aliases per variant.
- Source mapping remains strict:
  - base -> `lix_state` (or pinned override route via `lix_state_by_version`)
  - by_version -> `lix_state_by_version`
  - history -> `lix_state_history`
- Preserve only allowed extra logic:
  - schema_key scoping,
  - schema override predicates,
  - minimal history root/version pushdown.
- Convert history pushdown representation to AST predicates (no string serialization during extraction/routing).

5. Keep `lix_state_by_version` as core lowering entry in [lix_state_by_version_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_by_version_view_read.rs).

- Continue computing `include_commit_mapping` from query shape, but now wrapper input should omit `commit_id` unless needed.
- Maintain conservative behavior for complex query shapes (fallback can still enable commit mapping).
- Replace any remaining pushdown string assumptions with AST bucket consumption.

6. Remove dead active-path builder in [vtable_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/vtable_read.rs).

- Delete `build_effective_state_active_query` and any dead helpers only tied to active-path semantics.
- Confirm all active-state reads come through `lix_state -> lix_state_by_version`.

7. Update architecture/documentation in [architecture-current-state.md](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/docs/architecture-current-state.md).

- Explicitly document:
  - one state-read core,
  - adapter roles,
  - AST pushdown model,
  - projection pruning behavior and commit-mapping trigger rules.

8. Keep pipeline ordering as-is but codify dependency in comments/tests:

- [logical_views.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/pipeline/rules/query/canonical/logical_views.rs)
- `lix_state` rewrite must run before `lix_state_by_version` rewrite.

## Test Cases and Scenarios

1. Pushdown AST unit tests in [state_pushdown.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/state_pushdown.rs).

- Extract `AND` conjunction into correct buckets with preserved AST.
- Leave unsupported predicate shapes in outer selection unchanged.
- Verify alias requalification is structurally correct.

2. `lix_state` wrapper rewrite tests in [lix_state_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_view_read.rs).

- `SELECT entity_id FROM lix_state` does not project `commit_id`.
- `SELECT commit_id FROM lix_state` does project `commit_id`.
- `SELECT * FROM lix_state` keeps full visible projection.
- `version_id` and `lixcol_version_id` read rejection remains unchanged.

3. `lix_state_by_version` commit-mapping rewrite tests in [lix_state_by_version_view_read.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/src/sql/planning/rewrite_engine/steps/lix_state_by_version_view_read.rs).

- No-commit projection omits commit CTEs.
- Commit projection includes commit CTEs.
- Complex fallback cases still enable mapping when required.

4. Integration regression tests in [state_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/state_view.rs).

- Existing `commit_id` correctness when selected.
- No behavior regression for active-version routing and `version_id` rejection.
- Validate query results unaffected for non-commit projections.

5. Entity wrapper regressions in [entity_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/entity_view.rs) and [entity_history_view.rs](/Users/samuel/.codex/worktrees/3daf/lix-codex-app/packages/engine/tests/entity_history_view.rs).

- Base/by-version/history routing preserved.
- Override predicates still honored.
- History root/version pushdown preserved.

6. Full targeted suites to run:

- `cargo test -p lix_engine --test state_view`
- `cargo test -p lix_engine --test state_by_version_view`
- `cargo test -p lix_engine --test entity_view`
- `cargo test -p lix_engine --test entity_history_view`
- `cargo test -p lix_engine --test vtable_write`
- `cargo test -p lix_engine`

## Acceptance Criteria

1. All state/entity read paths lower through `lix_state_by_version` semantics (directly or via adapter).
2. No pushdown route relies on SQL string construction for predicate transport/classification.
3. `lix_state` no longer forces `commit_id` projection for queries that do not reference it.
4. Commit-mapping CTEs are absent in rewritten SQL when `commit_id` is not required.
5. Existing behavior and tests for `version_id` rejection and effective-state semantics remain green.

## Assumptions and Defaults

1. Single cutover is acceptable without feature-flag fallback.
2. Conservative pushdown is sufficient for this cutover; broader operator support is explicitly deferred.
3. External user-facing SQL contracts remain stable except performance improvements and internal simplification.
4. In ambiguous/complex query shapes, correctness is prioritized over aggressive pruning/pushdown.

## Progress report 

- 2026-03-04: Implemented AST pushdown carrier in `state_pushdown.rs` (`StatePushdown` now stores `Expr`), added alias-retarget helper, and added regression/unit coverage for extraction + `IS NULL` + alias remap behavior.
- 2026-03-04: Reworked `lix_state_view_read.rs` wrapper rewrite to AST construction:
  - derived wrapper query is built as AST over `lix_state_by_version AS s`.
  - active-version predicate is injected as AST (`s.version_id = (<active-version subquery>)`).
  - pushdown predicates are now AST (including ranked->source alias retargeting without string replacement).
  - wrapper projection is commit-aware via `column_usage` helper; `commit_id` is omitted unless referenced.
- 2026-03-04: Added `steps/column_usage.rs` and wired it into `lix_state` read rewrite for projection pruning policy.
- 2026-03-04: Reworked `entity_views/read.rs` query construction to AST (no dynamic SQL assembly for wrapper SELECT composition), including history pushdown transport as AST expressions.
- 2026-03-04: Removed dead active-read constructor path from `vtable_read.rs` (`build_effective_state_active_*`) to align with single `by_version` read core.
- 2026-03-04: Fixed effective-scope delete regression for `inherited_from_version_id` predicates:
  - added split plan fields in `VtableDeletePlan` for materialized effective-scope SQL vs untracked-safe SQL.
  - added `rewrite_delete_effective_scope_maps_inherited_from_version_predicate_for_untracked_cleanup` unit coverage in `vtable_write.rs`.
  - wired followup untracked cleanup to use the untracked-safe predicate channel.
- 2026-03-04: Fixed untracked update routing regression for `writer_key`:
  - untracked update paths now strip `writer_key` assignments (tracked paths keep `writer_key`).
  - validated with `writer_key` integration suite.
- 2026-03-04: Validation checkpoint complete:
  - `cargo test -p lix_engine --test vtable_write` passed.
  - `cargo test -p lix_engine --test state_inheritance` passed.
  - `cargo test -p lix_engine --test writer_key` passed.
  - full `cargo test -p lix_engine` passed.
- 2026-03-04: Added `writer_key` parity for untracked state:
  - schema/init now ensures `lix_internal_state_untracked.writer_key` exists (new column + migration guard).
  - untracked insert/upsert paths in `vtable_write` now persist/update `writer_key`.
  - untracked read projection in `vtable_read` now returns stored `writer_key` instead of `NULL`.
  - added `untracked_writer_key_matches_materialized_writer_key` integration coverage in `writer_key.rs` (SQLite + Postgres).
  - validation checkpoint: `cargo test -p lix_engine --test writer_key`, `cargo test -p lix_engine --test state_view`, and `cargo test -p lix_engine --test vtable_write` all passed.
- 2026-03-04: Centralized `lix_state` column contracts in `steps/state_columns.rs`:
  - moved visible-column contract to `LIX_STATE_VISIBLE_COLUMNS` and reused it from `lix_state_view_read`.
  - removed duplicate non-commit projection list in `column_usage` by deriving from shared visible columns.
  - moved update-allowed column contracts for `lix_state` and `lix_state_by_version` into shared constants and reused in both write validators.
  - validation checkpoint: `cargo test -p lix_engine --test state_view` and `cargo test -p lix_engine --test state_by_version_view` passed.
