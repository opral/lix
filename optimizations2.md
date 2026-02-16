# Optimization Notebook v2

## Benchmark Target

- Scenario: `packages/nextjs-replay-bench` (`BENCH_REPLAY_COMMITS=500`, `BENCH_REPLAY_WARMUP_COMMITS=5`, `BENCH_REPLAY_FIRST_PARENT=0`)
- Goal: remove multi-second replay stalls while preserving engine semantics.

## Baseline (before optimize 1)

| Metric | Value |
| --- | --- |
| Replay duration (measured) | `49,545.83ms` |
| Commit throughput | `9.47 commits/s` |
| Execute phase | `42,331.42ms` (`85.4%`) |
| Max statement latency | `5,246.03ms` |
| Dominant outlier | `DELETE FROM lix_file WHERE id IN (...)` triggering `pending.collect_delete_targets.id_projection` |

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

| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| Replay duration (measured) | `49,545.83ms` | `19,784.81ms` | `-60.1%` |
| Commit throughput | `9.47 commits/s` | `23.71 commits/s` | `+150.4%` |
| Execute phase | `42,331.42ms` | `12,586.99ms` | `-70.3%` |
| Max statement latency | `5,246.03ms` | `310.45ms` | `-94.1%` |

**Validation**
- `cargo test -p lix_engine pending_file_writes`
- `cargo test -p lix_engine --test commit`

