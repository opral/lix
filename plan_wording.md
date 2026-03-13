# Wording Plan

## Goal

Remove the terminology collisions around "materialized", "live", and "projection".

The target invariant is:

- `lix_internal_live_v1_*` is the authoritative committed per-schema live state.

The rebuild subsystem should be named as a rebuild/repair mechanism, not as the primary model for how live state exists.

## Core Naming Decision

Use these concepts consistently:

- `live state`: authoritative persisted committed state
- `rebuild`: recomputes live state from commit history
- `projection`: a derived query surface, never the source of truth

Do not use `materialized` for the authoritative live tables.

## Table Renames

Rename:

- `lix_internal_state_materialized_v1_<schema>`
- to `lix_internal_live_v1_<schema>`
- `lix_internal_live_untracked_v1`
  -> `lix_internal_live_untracked_v1`

Examples:

- `lix_internal_live_v1_lix_file_descriptor`
  -> `lix_internal_live_v1_lix_file_descriptor`
- `lix_internal_live_v1_lix_directory_descriptor`
  -> `lix_internal_live_v1_lix_directory_descriptor`
- `lix_internal_live_v1_lix_binary_blob_ref`
  -> `lix_internal_live_v1_lix_binary_blob_ref`
- `lix_internal_live_v1_lix_commit`
  -> `lix_internal_live_v1_lix_commit`
- `lix_internal_live_v1_lix_version_pointer`
  -> `lix_internal_live_v1_lix_version_pointer`
- `lix_internal_live_v1_lix_version_descriptor`
  -> `lix_internal_live_v1_lix_version_descriptor`
- `lix_internal_live_untracked_v1`
  -> `lix_internal_live_untracked_v1`

Keep `v1`.

Reason:

- the tables are versioned storage layout
- the suffix keeps future storage evolution possible

## Rebuild API Renames

Do not use `materialize()` for the rebuild path.

Recommended names:

- `materialization_plan()` -> `live_state_rebuild_plan()`
- `apply_materialization_plan()` -> `apply_live_state_rebuild_plan()`
- `materialize()` -> `rebuild_live_state()`

Recommended type renames:

- `MaterializationPlan` -> `LiveStateRebuildPlan`
- `MaterializationWrite` -> `LiveStateWrite`
- `MaterializationReport` -> `LiveStateRebuildReport`
- `MaterializationApplyReport` -> `LiveStateApplyReport`
- `MaterializationScope` -> `LiveStateRebuildScope`
- `MaterializationRequest` -> `LiveStateRebuildRequest`
- `MaterializationWarning` -> `LiveStateRebuildWarning`
- `MaterializationDebugTrace` -> `LiveStateRebuildDebugTrace`
- `MaterializationDebugMode` -> `LiveStateRebuildDebugMode`

Reason:

- this code path reconstructs persisted live state
- it is not the normal meaning of "how live state exists"
- naming should make it clear this is a rebuild/repair operation

## Internal Symbol Renames

Rename constants and local fields so code follows the same model.

Examples:

- `MATERIALIZED_PREFIX` -> `LIVE_STATE_PREFIX`
- `materialized_table_name()` -> `live_state_table_name()`
- `materialized_state` -> `live_state_rows`
- `commit_result.materialized_state` -> `commit_result.live_state_rows`
- `materialized_row_values_parameterized()` -> `live_state_row_values_parameterized()`
- `build_materialized_on_conflict()` -> `build_live_state_on_conflict()`

## Projection Terminology Rule

Reserve `projection` for derived views only.

Allowed:

- filesystem live projection
- public state projection
- query projection

Not allowed:

- referring to authoritative persisted tables as projections

Reason:

- projections can be recomputed
- live state is authoritative persisted truth

## Documentation Invariant

Document this invariant explicitly:

- `lix_internal_live_v1_*` stores the authoritative committed current row for a schema key, scoped by `version_id`
- normal reads should prefer direct lookup from these tables where possible
- rebuild APIs exist to reconstruct these tables from commit history if needed

Document this separately:

- `lix_internal_live_v1_*` stores committed live state
- `lix_internal_live_untracked_v1` stores untracked live state
- query surfaces may expose effective live state by overlaying untracked live state on committed live state
- projections are derived only when they compute that overlayed/effective view

## Non-Goals

This wording plan does not yet decide:

- whether `lix_file` should remain a derived projection over committed live schema rows
- whether filesystem CRUD needs dedicated live indexes beyond per-schema live tables
- whether copy-on-write roots should be added for filesystem state

Those are design questions to revisit after terminology is fixed.

## Why This Matters

Today the same word family is doing too much:

- persisted live tables
- rebuild-from-history subsystem
- in-memory commit outputs
- query projections

That makes it difficult to reason about bugs.

The desired mental model is:

- commit path writes `live state`
- reads use `live state`
- rebuild path repairs `live state`
- projections derive views from `live state`

## Recommended Order

1. Rename tables and SQL constants from `state_materialized` to `live`.
2. Rename rebuild APIs from `materialize` to `rebuild_live_state`.
3. Rename in-memory types and fields from `materialized_*` to `live_state_*`.
4. Update docs to state the invariant plainly.
5. Revisit whether any code still recomputes exact committed state instead of reading live tables directly.
