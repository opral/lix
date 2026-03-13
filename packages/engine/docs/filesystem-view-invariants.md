# Filesystem View Invariants

This document captures the architectural guardrails for `lix_file*` surfaces and the derived binary payload index.

## Recurring Failure Pattern

The same class of bugs kept recurring for three reasons:

1. View drift: active, by-version, and history file projections evolved independently.
2. Derived index drift: `lix_internal_binary_file_version_ref` was treated as if it were authoritative.
3. Split write paths: side effects differed between API execute and explicit transaction execute.

When these drifted, writes appeared to succeed while read paths disagreed.

## Source Of Truth

For tracked file payload pointers, the authoritative state is:

- `lix_internal_live_v1_lix_binary_blob_ref`

Everything else is derived:

- `lix_internal_binary_file_version_ref` is a cache/index for read performance.
- `lix_internal_file_data_cache` and `lix_internal_binary_blob_store` are byte stores.

## Invariants

1. `lix_file` must be implemented as a filter over `lix_file_by_version`, not as a separate independent projection.
2. `lix_file_history` and `lix_file_history_by_version` must resolve bytes with the same fallback policy as live views:
   - prefer history cache bytes when present
   - otherwise resolve from blob store through historical `lix_binary_blob_ref`
3. Binary ref index sync must run in both execution modes:
   - non-transaction API execute path
   - explicit transaction execute path
4. Binary ref index sync must enforce integrity:
   - reject `lix_binary_blob_ref` snapshots that reference a missing blob hash
5. Sync target discovery and snapshot loading must read from tracked materialized state, not untracked state.
6. Missing materialized relation during early init must be handled gracefully (no hard failure).

## Regression Coverage

The following tests lock these invariants:

- `rewrites_file_history_reads_with_binary_blob_fallback`
- `direct_binary_blob_ref_write_rejects_missing_blob_hash`
- `direct_binary_blob_ref_write_syncs_internal_binary_ref_index`
- `file_update_data_by_path_updates_builtin_binary_blob_ref`

These should remain green before landing any change that touches file projections, state rewrites, or side effects.
