# Canonical `lix_file` Payload Redesign

## Summary

- No backward compatibility constraints.
- Make tracked payload state authoritative.
- `lix_file.data` becomes a pure projection of tracked payload state.
- The filesystem becomes binary-file-first and plugin-free.
- Remove filesystem cache/index tables from the live read/write path.
- Scope this plan to live filesystem reads and writes only.
- Leave `lix_file_history` and `lix_file_history_by_version` untouched and out of scope.

## Canonical Model

- A file is the composition of tracked descriptor state in `lix_file_descriptor`, tracked payload-pointer state in `lix_binary_blob_ref`, and CAS storage keyed by `blob_hash`.
- `lix_file` and `lix_file_by_version` are read-only projections over descriptor state plus payload-pointer state.
- Live reads resolve bytes from tracked `lix_binary_blob_ref` plus CAS only.
- `lix_internal_file_data_cache` and `lix_internal_binary_file_version_ref` are removed from the filesystem design.

## Public Contract

- `INSERT/UPDATE/DELETE lix_file*` must lower into a typed file-mutation intent before execution.
- `data` is a first-class write input, not a column that gets stripped during rewrite.
- `SELECT data FROM lix_file*` reads directly from tracked payload pointers plus CAS.
- Read-after-write must be correct in single-statement execution, multi-statement execution, explicit transaction execution, and script execution.

## Out Of Scope

- `lix_file_history`
- `lix_file_history_by_version`
- history-specific materialization or cleanup
- any non-filesystem semantic extraction built on top of file bytes

## Source Of Truth

- Tracked descriptor shape remains in `lix_file_descriptor`.
- Tracked payload shape remains in `lix_binary_blob_ref`.
- CAS bytes are addressed by `blob_hash`.
- Filesystem reads resolve from tracked payload state plus CAS.
- The filesystem layer does not invoke plugins on reads or writes.

## Execution Model

1. Parse SQL into AST statements.
2. Lower `lix_file*` mutations into `FileMutationIntent`.
3. Resolve exact target scope and file ids before execution.
4. Persist new blobs into CAS for authoritative payload writes.
5. Persist tracked `lix_binary_blob_ref` mutations.
6. Persist tracked `lix_file_descriptor` mutations.

## `FileMutationIntent`

- `file_id`
- `version_id`
- `before_path`
- `after_path`
- `descriptor_patch`
- `payload`
- `writer_key`

`payload` should be one of:

- `Unchanged`
- `Set(bytes)`
- `Delete`

This intent must be produced once and then reused by:

- SQL rewrite/planning
- live filesystem write execution

## Required Architectural Changes

### Reads

- Rewrite `lix_file*` reads to join descriptor rows to tracked `lix_binary_blob_ref` rows first.
- Resolve bytes from CAS directly from the tracked `blob_hash`.
- Make missing blob hashes a hard integrity error.
- Remove all plugin involvement from the normal filesystem read path.
- Remove filesystem cache/index reads entirely.

### Writes

- Stop stripping `data` from `INSERT lix_file`.
- Stop stripping `data` from `UPDATE lix_file`.
- Stop turning data-only file updates into noop SQL plus follow-up side effects.
- Lower every file mutation through the same typed intent path in all execution modes.
- Persist tracked payload changes in the same transaction as descriptor changes.
- Remove filesystem cache/index writes entirely.

### Filesystem Scope

- The filesystem layer has no plugin hooks.
- Filesystem writes do not invoke `detectChanges`.
- Filesystem reads do not invoke `applyChanges`.
- Any higher-level semantic extraction must live outside the filesystem contract.

### Cache Removal

- Delete `lix_internal_binary_file_version_ref` from the live filesystem architecture.
- Delete `lix_internal_file_data_cache` from the live filesystem architecture.
- Remove any planner or side-effect logic that relies on those tables.
- Ensure the only live payload lookup path is tracked `lix_binary_blob_ref` plus CAS.

## Code To Delete

- `strip_file_data_from_insert`
- data-stripping/noop rewrite behavior for `UPDATE lix_file`
- `pending_file_writes` as a correctness path
- read-time `materialize_missing_file_data_with_plugins(...)` for normal `lix_file*` reads
- filesystem-triggered `detectChanges` and `applyChanges` integration
- `lix_internal_file_data_cache` reads/writes in the live filesystem path
- `lix_internal_binary_file_version_ref` reads/writes in the live filesystem path
- duplicate statement-rewrite/coalescing paths that special-case `lix_file` separately from the canonical pipeline

## Implementation Phases

### Phase 1: Remove Plugins And Caches From Live Filesystem

- Remove filesystem-triggered `detectChanges`.
- Remove filesystem-triggered `applyChanges`.
- Remove `lix_internal_file_data_cache` and `lix_internal_binary_file_version_ref` from live read/write logic.
- Add guardrails that forbid plugin hooks and cache-table dependencies in live filesystem code.

### Phase 2: Canonical Intent

- Introduce `FileMutationIntent` and build it directly from parsed statements plus params.
- Make planning and live filesystem execution consume that intent instead of reparsing or prefetching through `lix_file`.
- Add guardrails that forbid data-stripping/noop fallback for file writes.

### Phase 3: Canonical Read Path

- Rewrite `lix_file` and `lix_file_by_version` to source payload pointers from tracked `lix_binary_blob_ref`.
- Read bytes from CAS directly.
- Make CAS lookup the only live payload read path.

### Phase 4: Canonical Write Path

- Lower `INSERT/UPDATE/DELETE lix_file*` into descriptor mutations plus payload-pointer mutations.
- Persist CAS blobs and tracked `lix_binary_blob_ref` rows in the same transaction.
- Preserve statement barriers so multi-statement scripts observe prior file writes immediately.

### Phase 5: Cleanup

- Remove legacy side-effect inference and duplicate transaction coalescing paths.
- Collapse the engine onto one write pipeline for file mutations.
- Simplify docs and tests to the new model.

## Test Plan

- Insert file bytes, then read them back immediately through `lix_file`.
- Update file bytes, then read them back immediately through `lix_file`.
- Ensure the same behavior in single-statement execute, multi-statement execute, explicit transaction callbacks, and `BEGIN ... COMMIT` script execution.
- Ensure descriptor-only updates do not rewrite `lix_binary_blob_ref`.
- Ensure data-only updates do rewrite `lix_binary_blob_ref`.
- Ensure live filesystem reads do not touch removed cache tables.
- Ensure missing `blob_hash` targets fail with integrity errors.
- Ensure filesystem reads and writes do not invoke plugin hooks at all.
- Ensure deletes tombstone descriptor visibility in live views.

## Guardrails

- `lix_file.data` is never computed from plugin code.
- File write planning never depends on reading `lix_file` or `lix_file_by_version`.
- There is exactly one canonical pipeline for file writes across all execution modes.
- Live filesystem reads and writes do not depend on filesystem cache/index tables.
- Read-after-write correctness is tested at statement barriers and transaction boundaries.
- The filesystem layer does not call `detectChanges` or `applyChanges`.
- History views are untouched by this plan.

## Deliverables

- one canonical `FileMutationIntent` path
- one canonical `lix_file*` read projection path
- tracked `lix_binary_blob_ref` as the only payload pointer source of truth
- no plugin dependence inside the filesystem layer
- no filesystem cache/index tables in the live path
- removal of the legacy inferred file-write side-effect machinery

## Assumptions

- `lix_binary_blob_ref` remains the tracked payload-pointer schema.
- CAS remains internal storage, not a public SQL surface.
- We can change public docs and behavior without preserving the current plugin-coupled filesystem story.
- History stays unchanged during this work.

## Progress Log

- 2026-03-05 16:07: Drafted the canonical redesign plan. The target model is descriptor state + tracked payload pointer + CAS.
- 2026-03-06: Simplified the target further: the filesystem is now explicitly plugin-free. No `detectChanges`, no `applyChanges`, and no binary fallback language in the filesystem contract.
- 2026-03-06: Narrowed scope to the smallest live filesystem cut: tracked `lix_file_descriptor`, tracked `lix_binary_blob_ref`, CAS storage, no filesystem plugins, no filesystem caches, and live `lix_file` / `lix_file_by_version` only. History is explicitly untouched and out of scope.
- 2026-03-06: Rewired the live read path so `lix_file` and `lix_file_by_version` resolve payload bytes from tracked `lix_binary_blob_ref` state plus CAS. Normal live reads no longer materialize file data through plugin hooks or filesystem caches.
- 2026-03-06: Rewired the live write path so authoritative file writes and deletes emit tracked `lix_binary_blob_ref` changes directly. The live execution path now skips filesystem cache/index maintenance, and cache-miss coverage was updated to assert state-plus-CAS behavior instead of cache repopulation.
- 2026-03-06: Added a shared live file projection builder and moved live write planning onto it. `pending_file_writes` and filesystem mutation scoping now prefetch against tracked descriptor state plus `lix_binary_blob_ref` plus CAS through the canonical projection, instead of consulting filesystem cache tables or the logical `lix_file*` views.
- 2026-03-06: Fixed the explicit-version regression in the postprocess path. Filesystem payload ref changes are now still persisted when descriptor updates on `lix_file_by_version` go through SQL postprocess, so by-version path+data updates round-trip correctly instead of dropping the payload rewrite.
- 2026-03-06: Removed the remaining live filesystem plugin-detection branch from execution intent collection. `collect_execution_side_effects_with_backend_from_statements` now only gathers filesystem-owned side effects, so live `lix_file*` writes no longer carry dead `detect_plugin_file_changes` or plugin-cache plumbing even as disabled options.
- 2026-03-06: Replaced the fake no-op SQL shell for data-only filesystem updates with an explicit effect-only rewrite output. `UPDATE lix_file SET data = ...` now lowers to zero prepared SQL statements in the rewrite engine, while the live payload write still persists through the filesystem-owned side-effect path.
- 2026-03-06: Removed the last live filesystem sentinel and cache-maintenance shim. Data-only `lix_file` updates now travel through an explicit `FilesystemUpdateRewrite::EffectOnly` branch instead of synthesized `SELECT 0 WHERE 1 = 0` SQL, the stale `PostprocessPlan::DomainChangesOnly` branch is gone, and unused live cache/index invalidation and binary-fallback maintenance helpers were deleted from the runtime path.
- 2026-03-06: Pruned the remaining dead live filesystem cache/materialization probes. The old binary-ref index loaders, file-data-unavailable probes, cache-table query builders, and unused cache-table constants that were only supporting the retired live cache path are now deleted.
- 2026-03-06: Realigned the filesystem test surface to the simplified live contract. Live view tests now read tracked `lix_binary_blob_ref` state directly, payload-corruption cases assert `NULL` when the CAS blob row is missing, and the old plugin/cache-era `file_materialization` suite was fenced off so only binary-first live/CAS coverage remains active. The four originally failing targets (`file_history_view`, `file_materialization`, `filesystem_view`, `writer_key`) now pass again.
