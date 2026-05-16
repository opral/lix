# SQL2 Cleanup Inventory

Current date: 2026-05-16

This inventory tracks the remaining SQL2 cleanup work after the P1 live-state
exact-load fix.

## Green Gates

The cleanup is not complete until these gates pass from a clean worktree:

- `cargo fmt --check`
- `cargo check -p lix_engine`
- `cargo test -p lix_engine`
- `cargo test -p lix_engine --test branching`
- `cargo test -p lix_engine --test transaction`
- `cargo test -p lix_engine --test sql`
- `cargo test -p lix_engine --test code_structure`

The ignored-test audit below decides which ignored tests should be enabled,
deleted as obsolete raw-DML coverage, or kept ignored because they are
manual/diagnostic/stress tests.

## Ignored-Test Buckets

| Bucket | Criteria | Action | Owner phase |
| --- | --- | --- |
| Reactivate | Functional product behavior reachable through public APIs, disabled only because Phase 1 cut off SQL writes. | Remove `#[ignore]`, update fixtures only when semantics are unchanged, and include in gates. | Bound public write execution |
| Delete or replace | Tests that exercise raw DataFusion provider DML as an internal execution boundary that should stay fail-closed. | Replace with bound-pipeline coverage and fail-closed/code-structure guards, then delete obsolete raw-DML tests. | Provider DML deletion |
| Keep ignored | Manual metrics, storage accounting printouts, large stress tests, or diagnostics not suitable as default CI gates. | Keep `#[ignore]` with a specific reason. | Manual diagnostics |

## Current Ignored Tests

### Reactivate

- `packages/engine/tests/sql.rs:26`
  - All `simulation_test!` generated SQL integration cases.
  - Reason: public SQL integration harness was disabled for Phase 1 writes.
  - Unblock: all public write targets used by the SQL harness have bound executors.

### Delete Or Replace

- `packages/engine/src/sql2/providers/lix_state.rs:2122`
  - `insert_into_requires_write_transaction`
- `packages/engine/src/sql2/providers/lix_state.rs:2142`
  - `update_requires_write_transaction`
- `packages/engine/src/sql2/providers/lix_state.rs:2165`
  - `delete_requires_write_transaction`
- `packages/engine/src/sql2/providers/lix_state.rs:2184`
  - `delete_returns_lix_state_delete_exec_with_write_ctx`
- `packages/engine/src/sql2/providers/lix_state.rs:2200`
  - `update_rejects_read_only_lix_state_columns`
- `packages/engine/src/sql2/providers/lix_state.rs:2223`
  - `update_returns_lix_state_update_exec_with_write_ctx`
- `packages/engine/src/sql2/providers/lix_state.rs:2243`
  - `insert_into_returns_data_sink_exec_with_write_ctx`
- `packages/engine/src/sql2/providers/lix_state.rs:2346`
  - `insert_plan_returns_datafusion_count_uint64`
- `packages/engine/src/sql2/providers/lix_state.rs:2380`
  - `update_plan_evaluates_filters_assignments_and_stages_rows`
- `packages/engine/src/sql2/providers/lix_state.rs:2456`
  - `delete_plan_with_empty_filters_stages_all_visible_rows`

These tests target raw provider DML execution. The replacement coverage should
prove public/bound write behavior and fail-closed raw `TableProvider` DML.

- `packages/engine/src/sql2/exec/datafusion.rs:1775`
- `packages/engine/src/sql2/exec/datafusion.rs:1883`
- `packages/engine/src/sql2/exec/datafusion.rs:1919`
- `packages/engine/src/sql2/exec/datafusion.rs:1976`
- `packages/engine/src/sql2/exec/datafusion.rs:2023`
- `packages/engine/src/sql2/exec/datafusion.rs:2063`
- `packages/engine/src/sql2/exec/datafusion.rs:2101`
- `packages/engine/src/sql2/exec/datafusion.rs:2164`
- `packages/engine/src/sql2/exec/datafusion.rs:2228`
- `packages/engine/src/sql2/exec/datafusion.rs:2469`
- `packages/engine/src/sql2/exec/datafusion.rs:2520`
- `packages/engine/src/sql2/exec/datafusion.rs:2570`
- `packages/engine/src/sql2/exec/datafusion.rs:2619`
- `packages/engine/src/sql2/exec/datafusion.rs:2663`
- `packages/engine/src/sql2/exec/datafusion.rs:2702`
- `packages/engine/src/sql2/exec/datafusion.rs:2753`
- `packages/engine/src/sql2/exec/datafusion.rs:2794`
- `packages/engine/src/sql2/exec/datafusion.rs:2838`
- `packages/engine/src/sql2/exec/datafusion.rs:2885`
- `packages/engine/src/sql2/exec/datafusion.rs:2924`
- `packages/engine/src/sql2/exec/datafusion.rs:2974`
- `packages/engine/src/sql2/exec/datafusion.rs:3041`
- `packages/engine/src/sql2/exec/datafusion.rs:3095`
- `packages/engine/src/sql2/exec/datafusion.rs:3145`
- `packages/engine/src/sql2/exec/datafusion.rs:3203`
- `packages/engine/src/sql2/exec/datafusion.rs:3260`

These tests target old raw DataFusion write execution for entity/file/directory
surfaces. Delete or rewrite them after equivalent bound-pipeline tests cover the
same public behavior.

### Keep Ignored

- `packages/engine/tests/json_pointer_crud_storage.rs:36`
  - Prints JSON pointer CRUD storage-size reference rows.
- `packages/engine/tests/tmp_lix_key_value_amplification.rs:1181`
  - Prints read/write amplification north-star metrics for key-value inserts.
- `packages/engine/tests/tmp_lix_key_value_amplification.rs:1191`
  - Large `lix_file.data` stress test; defaults to 100 MiB.
- `packages/engine/tests/tmp_lix_key_value_amplification.rs:1203`
  - Prints branching amplification canaries for key-value writes.
- `packages/engine/tests/tmp_lix_key_value_amplification.rs:1225`
  - Prints branching amplification canaries for file writes.
- `packages/engine/tests/storage_accounting.rs:166`
  - Prints deterministic storage accounting table.
- `packages/engine/tests/storage_accounting.rs:233`
  - Prints deterministic json_store storage accounting table.
- `packages/engine/tests/storage_accounting.rs:276`
  - Prints deterministic changelog storage accounting table.
- `packages/engine/tests/storage_accounting.rs:318`
  - Prints deterministic untracked_state storage accounting table.

## Reactivated In Bound Entity Phase

- `packages/engine/tests/transaction.rs`
  - `read_sql_rolls_back_read_transaction_when_pre_plan_setup_fails`
  - `write_transaction_open_rolls_back_when_active_version_resolution_fails`
  - `active_transaction_blocks_session_read_and_allows_transaction_read`
  - `begin_transaction_cannot_race_with_opening_session_write`
  - Verified with `cargo test -p lix_engine --test transaction -- --nocapture`.

The bound entity write executor now handles public entity `INSERT`, `UPDATE`,
and `DELETE` for the functional targets exercised by branching, transaction,
engine, and the targeted ignored SQL cases. Raw DataFusion provider DML remains
in the delete-or-replace bucket until those provider paths are removed or
guarded by code-structure tests.

## Current Behavioral Blocker

The remaining broad blocker is the disabled SQL integration harness at
`packages/engine/tests/sql.rs:26`. Reactivation should proceed in target-driven
batches after running the full `cargo test -p lix_engine --test sql` gate and
triaging any residual public target gaps separately from obsolete raw provider
DML coverage.
