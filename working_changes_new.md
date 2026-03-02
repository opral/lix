# Working Changes Redesign Plan (No Working Artifacts)

## Goal

Implement working diff without any working-specific commit artifacts.

Target outcome:

- No `working_commit_id` in version pointer model.
- No working commit.
- No working change set.
- No working change set elements.
- No synthetic `working_projection:*` rows.
- `lix_working_changes` is purely derived from:
  - current `version.commit_id`
  - per-version last checkpoint commit baseline

This is a clean cut. No backward compatibility layer.

## New Semantic Model

For each version `V`:

- `version.commit_id`: current head commit.
- `checkpoint_commit_id`: commit used as working-diff baseline.

`lix_working_changes(V) := diff(version.commit_id(V), checkpoint_commit_id(V))`

Checkpoint:

- label current `version.commit_id` with `checkpoint`
- set `checkpoint_commit_id = version.commit_id`
- do not rotate or create any working commit

## Storage Decision

Use an internal, per-version baseline table (authoritative engine metadata).

Recommended internal table:

- `lix_internal_last_checkpoint(version_id PRIMARY KEY, checkpoint_commit_id NOT NULL)`

This table is not user-facing schema/state. It is a materialized index for fast baseline lookup.

Rationale:

- deterministic semantics in merge DAGs
- durable across restart/sync/replay
- O(1) lookup for `lix_working_changes`
- avoids hidden internal-only state drift

### Deterministic Rebuild (Materialization Logic)

At boot, rebuild `lix_internal_last_checkpoint` from durable state:

1. For each version row, start from `version.commit_id`.
2. Walk ancestor commits that carry `checkpoint` label.
3. Pick winner deterministically:
   1. smallest ancestry depth
   2. newest commit `created_at`
   3. highest commit id (stable tie-break)
4. If no checkpoint ancestor exists, use `version.commit_id` itself.
5. Upsert rebuilt rows into `lix_internal_last_checkpoint`.

This rebuild is materialization logic. Runtime writes (e.g. checkpoint creation) do incremental UPSERTs, and boot rebuild is recovery/consistency verification.

## Invariants

1. Every version has exactly one baseline pointer row.
2. `checkpoint_commit_id` references an existing commit.
3. `checkpoint_commit_id` is ancestor-or-equal of `version.commit_id` for each version.
4. `lix_working_changes` never reads synthetic projection rows (because they do not exist).
5. `lix_working_changes` returns changed rows only (`added`, `modified`, `removed`).

## Implementation Phases

## Phase 1: Schema and Seed

1. Remove `working_commit_id` from version pointer schema and related typed structs.
2. Introduce internal table `lix_internal_last_checkpoint(version_id, checkpoint_commit_id)`.
3. Init seed:
- for each seeded version, set `checkpoint_commit_id = version.commit_id`.
4. Add deterministic materialization rebuild for `lix_internal_last_checkpoint`.

Primary touchpoints:

- `packages/engine/src/version/*`
- `packages/engine/src/init/seed.rs`
- `packages/engine/src/materialization/*`

## Phase 2: Version + Commit Runtime

1. Remove all `working_commit_id` assumptions from runtime loaders and commit preparation.
2. Build `VersionInfo` parent linkage from tip commit only.
3. Keep regular commit graph behavior unchanged for writes.

Primary touchpoints:

- `packages/engine/src/sql/history/commit_runtime.rs`
- `packages/engine/src/sql/execution/followup.rs`
- `packages/engine/src/commit/generate_commit.rs`

## Phase 3: Checkpoint Rewrite

Replace checkpoint implementation with pointer update semantics.

Old behavior to delete:

- promote working commit
- create new working commit/change set
- rewrite commit graph for rotation

New behavior:

1. Read active version + current `version.commit_id`.
2. Ensure checkpoint label on `version.commit_id`.
3. `UPSERT lix_internal_last_checkpoint(version_id, checkpoint_commit_id=version.commit_id)`.
4. Return `{ id: version.commit_id, change_set_id: commit.change_set_id }`.

Primary touchpoint:

- `packages/engine/src/checkpoint/create_checkpoint.rs`

## Phase 4: Remove Working Projection Subsystem

Delete read-time synthetic projection pipeline.

Remove:

- refresh-on-read hook
- synthetic projection writes
- `WORKING_PROJECTION_METADATA`
- source-change-id metadata bridge

Primary touchpoints:

- `packages/engine/src/sql/execution/shared_path.rs`
- `packages/engine/src/sql/side_effects.rs`
- `packages/engine/src/sql/history/projections.rs`
- `packages/engine/src/working_projection.rs`

## Phase 5: Rebuild `lix_working_changes` as Pure Diff

Rewrite `lix_working_changes` SQL lowering to use real state only.

Inputs:

- active version id
- current `version.commit_id`
- `checkpoint_commit_id` from `lix_internal_last_checkpoint`
- real `lix_change_set_element` and `lix_change`

Output columns stay:

- `entity_id`, `schema_key`, `file_id`
- `before_change_id`, `after_change_id`
- `before_commit_id`, `after_commit_id`
- `status`

No special handling for `working_projection:*` IDs.

Primary touchpoint:

- `packages/engine/src/sql/planning/rewrite_engine/steps/lix_working_changes_view_read.rs`

## Phase 6: API and Surface Cleanup

1. Remove `working_commit_id` from public SQL surfaces and internal structs where exposed.
2. Update validators, planner expectations, and registry docs.
3. Keep user-facing API simple: `lix_version` exposes tip commit only (or keep legacy column removed entirely as clean cut).

Primary touchpoints:

- `packages/engine/src/sql/planning/rewrite_engine/steps/lix_version_view_read.rs`
- `packages/engine/src/sql/planning/rewrite_engine/steps/lix_version_view_write.rs`
- `packages/engine/tests/version_*`

## Phase 7: Test Rewrite

Replace working-commit-centric tests with baseline-pointer semantics.

Add/adjust tests:

1. Init seeds `lix_internal_last_checkpoint` per version.
2. Tracked writes appear in `lix_working_changes` against baseline.
3. Checkpoint advances baseline and clears working diff.
4. No synthetic projection IDs appear anywhere.
5. Multi-version isolation: baseline pointer is per version.
6. Merge DAG determinism: rebuild picks deterministic checkpoint winner.

Primary touchpoints:

- `packages/engine/tests/working_changes_view.rs`
- `packages/engine/tests/working_change_set.rs` (rewrite heavily or retire)
- `packages/engine/tests/checkpoint.rs`
- `packages/engine/tests/init.rs`

## Removal Checklist

- [ ] Remove `working_commit_id` from builtin version pointer schema.
- [ ] Remove all code paths that create/rotate working commits.
- [ ] Remove working projection refresh on read.
- [ ] Remove `working_projection` metadata/constants/module.
- [ ] Remove synthetic change-id translation logic.
- [ ] Remove tests asserting working commit rotation.

## Migration Strategy

Because this is a clean cut and not deployed:

1. Apply schema and runtime changes directly.
2. Update tests in same branch.
3. Do not keep fallback paths.

## Performance Notes

Expected impact:

- Lower write/read complexity by deleting projection write-on-read path.
- `lix_working_changes` may do heavier pure SQL diff in worst-case large histories.

Mitigations (only if needed after benchmarks):

1. index tuning on `lix_change_set_element(change_set_id, entity_id, schema_key, file_id)`
2. selective CTE pushdown for schema/file predicates
3. optional internal cache table later (non-authoritative)

## Risks

1. Missing baseline pointer updates in checkpoint path can silently break diff semantics.
2. Removing `working_commit_id` touches many tests and view rewrite contracts.
3. DAG edge cases need explicit tests (merge parents, multiple checkpoints).

## Done Definition

1. No `working_projection:*` IDs are ever produced.
2. No synthetic working rows in untracked state.
3. `lix_working_changes` derives from tip vs baseline only.
4. Checkpoint only updates label + `lix_internal_last_checkpoint`.
5. Full engine test suite passes under new semantics.

## Progress report

### Milestone 1 - Schema and seed refactor

- Removed `working_commit_id` from version pointer schema and Rust types.
- Added internal baseline table:
  - `lix_internal_last_checkpoint(version_id PRIMARY KEY, checkpoint_commit_id NOT NULL)`.
- Updated init/seed so global + main versions seed baseline rows.
- Added deterministic rebuild path for `lix_internal_last_checkpoint` during bootstrap.

### Milestone 2 - Version + commit runtime cleanup

- Removed runtime assumptions about working commit rotation from commit runtime and commit generation.
- Version pointer snapshot content now stores only `{ id, commit_id }`.
- Updated materialization planning and version creation paths to use tip commit only.

### Milestone 3 - Checkpoint rewrite

- Replaced checkpoint rotation logic with pointer semantics:
  - ensure checkpoint label on current `version.commit_id`
  - upsert `lix_internal_last_checkpoint(version_id, checkpoint_commit_id)`
  - return current commit + change set.
- No working commit, no working change set creation during checkpoint.

### Milestone 4 - Remove working projection subsystem

- Deleted:
  - `packages/engine/src/working_projection.rs`
  - `packages/engine/src/sql/history/projections.rs`
- Removed projection refresh hooks from SQL execution side effects and shared execution path.

### Milestone 5 - Rebuild `lix_working_changes` as pure diff

- Rewrote `lix_working_changes` lowering to diff:
  - tip commit from `lix_version_pointer.commit_id`
  - baseline from `lix_internal_last_checkpoint`.
- Removed any `working_projection:*` dependencies.
- View now returns changed rows only with status-aware before/after commit/change IDs.

### Milestone 6 - API and surface cleanup

- Removed `working_commit_id` from `lix_version` read/write rewrite surfaces.
- Updated insert/update validation and rewrite behavior for commit-only pointer semantics.
- Updated all affected integration tests to the new contract.

### Milestone 7 - Test rewrite + verification

- Reworked checkpoint and working-diff tests to baseline-pointer semantics.
- Updated version, init, entity/filesystem/state test fixtures to remove `working_commit_id`.
- Replaced `working_change_set` tests to validate `lix_working_changes` and checkpoint materialization behavior.
- Full verification:
  - `cargo test -p lix_engine` passes.
