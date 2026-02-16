# Optimization Notebook v2

## Benchmark Target

- Scenario: `packages/nextjs-replay-bench` (`BENCH_REPLAY_COMMITS=500`, `BENCH_REPLAY_WARMUP_COMMITS=5`, `BENCH_REPLAY_FIRST_PARENT=0`)
- Goal: remove multi-second replay stalls while preserving engine semantics.

## Baseline (before optimize 1)

| Metric                     | Value                                                                                              |
| -------------------------- | -------------------------------------------------------------------------------------------------- |
| Replay duration (measured) | `49,545.83ms`                                                                                      |
| Commit throughput          | `9.47 commits/s`                                                                                   |
| Execute phase              | `42,331.42ms` (`85.4%`)                                                                            |
| Max statement latency      | `5,246.03ms`                                                                                       |
| Dominant outlier           | `DELETE FROM lix_file WHERE id IN (...)` triggering `pending.collect_delete_targets.id_projection` |

## Optimize 1: exact delete-target predicate pushdown

**Hypothesis**  
Simple id-scoped deletes should not prefetch via full `lix_state_by_version` expansion.  
If we resolve exact `(file_id, version_id)` targets from the SQL predicate itself, we can skip the pathological scan.

**Implementation**

- Added exact delete target extraction in `pending_file_writes` for:
  - `id = ...`
  - `id IN (...)`
  - optional `version_id` / `lixcol_version_id` exact constraints
  - conjunctions via `AND`
- Applied as an early fast path in `collect_delete_targets(...)`.
- Kept fallback to the existing generic prefetch path when predicates are not exact.
- Added unit tests for active-version, explicit-version, and non-exact predicate cases.

**Result**

| Metric                     | Before           | After             | Delta     |
| -------------------------- | ---------------- | ----------------- | --------- |
| Replay duration (measured) | `49,545.83ms`    | `19,784.81ms`     | `-60.1%`  |
| Commit throughput          | `9.47 commits/s` | `23.71 commits/s` | `+150.4%` |
| Execute phase              | `42,331.42ms`    | `12,586.99ms`     | `-70.3%`  |
| Max statement latency      | `5,246.03ms`     | `310.45ms`        | `-94.1%`  |

**Validation**

- `cargo test -p lix_engine pending_file_writes`
- `cargo test -p lix_engine --test commit`

## Optimize 2: exact current-version lookup for update rewrite prefetch

**Hypothesis**  
`mutation.file_ids_matching_update.fast_id` still queried `lix_state_by_version` for exact `(version_id, file_id)` update predicates, paying unnecessary planner/view expansion cost per update.

**Implementation**

- Added a direct exact lookup path in `mutation_rewrite`:
  - first checks `lix_internal_state_untracked` for `(schema_key='lix_file_descriptor', version_id, entity_id)`
  - then checks `lix_internal_state_materialized_v1_lix_file_descriptor` for the same key (non-tombstone)
- Falls back to the previous `lix_state_by_version` fast-id query only when exact current-version lookup misses.
- Keeps rewrite semantics intact for inherited/version-chain cases via fallback.

**Result**

| Metric                     | Before            | After             | Delta    |
| -------------------------- | ----------------- | ----------------- | -------- |
| Replay duration (measured) | `19,784.81ms`     | `14,191.52ms`     | `-28.3%` |
| Commit throughput          | `23.71 commits/s` | `33.05 commits/s` | `+39.4%` |
| Execute phase              | `12,586.99ms`     | `7,074.07ms`      | `-43.8%` |
| Max statement latency      | `59.73ms`         | `52.72ms`         | `-11.7%` |

**Validation**

- `cargo test -p lix_engine mutation_rewrite`
- `cargo test -p lix_engine --test commit`

## Current frontier

- After optimize 2, replay is split roughly 50/50:
  - `executeStatementsMs`: `7,074.07ms` (49.8%)
  - `readPatchSetMs`: `7,073.03ms` (49.8%)
- Slowest SQL statements are now ~`50ms` large write payloads, with previous multi-second outliers eliminated.
- Profiling indicates no remaining single engine rewrite hotspot large enough to plausibly yield another >10% overall reduction without changing higher-level replay strategy (for example batching mode or git patch-read pipeline changes).

## Optimize 3 (attempt): temp-table set-based cache flush/invalidation

**Hypothesis**  
Replacing per-row cache upserts/deletes with temp-table bulk load + set-based `INSERT ... SELECT` / `DELETE ... EXISTS` should reduce transaction overhead in replay commits.

**Implementation**  
- Added SQLite transaction-path temp tables for:
  - pending file data cache updates
  - pending file path cache updates
  - file cache invalidation targets
- Replaced row-by-row operations with set-based operations when batch size crosses a threshold.

**Benchmark comparison (same config as above)**

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| Replay duration (measured) | `14,165.04ms` | `14,438.95ms` (run 1) | `+1.9%` |
| Replay duration (measured) | `14,165.04ms` | `14,112.03ms` (run 2) | `-0.4%` |
| Commit throughput | `33.11 commits/s` | `32.48` to `33.23 commits/s` | noise |

**Result**  
- No reliable >10% improvement on this replay path.
- The run remains dominated by a near 50/50 split between execute and patch-read, and this cache flush path does not move total runtime materially in current execution mode.
