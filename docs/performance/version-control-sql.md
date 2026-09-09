# Version-control SQL complexity and profiling

The SQL consolidation uses one first-parent iterator for log and history, and
the same endpoint comparison for history and diff. Checkpoint membership is
immutable canonical commit metadata; a derived ordered inventory supports
paged synchronization and retention without separate logical marker writes.

## Complexity contract

Let V be mainline commits visited before a requested page is satisfied, P be
selected checkpoint commits, D be endpoint differences examined, F be file
descriptors, and A be ancestor directory descriptors needed to resolve paths.

- Log pages perform O(V) graph-node reads and stream rows. Checkpoint filtering
  can visit unmarked commits between returned checkpoints; it does not promise
  O(P) when arbitrarily many automatic commits intervene.
- Paged history applies destination metadata restrictions before diffs. Its work
  is O(V + sum of selected endpoint comparison costs), not all historical
  snapshot sizes. Exact selected destination IDs stop traversal after the last
  requested ID is found. IDs outside the mainline can require walking to root.
- The supported left-join page shape uses the existing probe-key join optimizer
  to pass page IDs into history before opening endpoint diffs. The equivalent
  two-query IN form has the same destination pruning.
- Global checkpoint counts and daily activity metrics over `lix_commit WHERE
  is_checkpoint` scan retained canonical commit metadata, O(C) for C retained
  commits. They do not load historical row snapshots. Ordering the full result
  costs O(C log C); a bounded top-P sort can reduce sorting work to O(C log P),
  but the count still visits the complete inventory. These global metrics do
  not inherit the recent mainline page guarantee.
- Full checkpoint publication performs constant logical work with respect to
  the number of working changes: capture/alias a state root, publish immutable
  metadata, advance control and stage the inventory index. Physical index writes
  retain the storage backend's lookup/update costs; reclamation is asynchronous.
- File-ID restricted ancestor-move comparisons use descriptor point reads and
  cached ancestor resolution. Unfiltered ancestor-change discovery currently
  scans descriptor identities of the requested relation, O(F + A) descriptor work
  at each endpoint (use directory count for F when querying directories).
  It never decodes file bytes. A reverse directory-to-descendant index would be
  needed to bound discovery to affected descendants alone.
- Sparse bootstrap pages checkpoint headers/flags, while certifying state only
  for serving heads and working baselines. Off-branch historical state hydrates
  on demand; bootstrap must not materialize every checkpoint state.
- Migration rewrites canonical metadata and traverses deduplicated manifest
  dependencies. It must not enumerate every row of every checkpoint merely to
  backfill a boolean.

## Reproduction

The memory correctness/scaling test asserts work counters as well as results:

```sh
cargo nextest run -p lix -E 'test(mainline_page_work)' --success-output immediate
```

The standalone public-API RocksDB profile separates setup from query timings:

```sh
cargo run --manifest-path tooling/Cargo.toml -p lix_e2e --release \
  --example version_control_sql_profile -- 100 1000 5000
```

Use `LIX_PROFILE_MEMORY=1` for a memory control. Each query reports the median
and p95 after three warmups and 21 measured repetitions. Full capture reports
its actual operation after a nonempty working interval, not empty follow-ups.

## Development evidence (2026-09-09)

Initial in-memory test-profile run, five commits per page:

| Retained checkpoints | Log graph reads | Joined preview graph reads | Endpoint diffs | Combined query time |
| --- | ---: | ---: | ---: | ---: |
| 10 | 5 | 10 | 5 | 10.149 ms |
| 30 | 5 | 10 | 5 | 7.877 ms |
| 60 | 5 | 10 | 5 | 8.170 ms |

These are development-build observations, not a release latency claim. The
work-counter assertions are the regression gate. `perf stat` over five whole
fixture/test runs reported 0.22587 ± 0.00455 seconds wall time, approximately
1.59 billion cycles and 1.12 billion instructions per run. Those counters include
fixture creation, checkpoints, and all tested queries; they do not isolate the
SQL query alone. Optimized backend results should be recorded with their build
revision when the implementation is finalized.

## Optimized RocksDB measurements

The implementation working tree based on `f6e712855e22b2204736e5c181a372f1c8696ed7`
was profiled with the release harness on 2026-09-09. Timings exclude setup;
concurrent validation builds were running, so these are observations rather than
an isolated-machine benchmark or a latency guarantee.

| Retained checkpoints | Log median / p95 | Joined history median / p95 | Nonempty full checkpoint |
| ---: | ---: | ---: | ---: |
| 100 | 0.619 / 1.038 ms | 4.144 / 6.271 ms | 0.231 ms |
| 1,000 | 0.383 / 0.454 ms | 3.658 / 3.764 ms | 0.231 ms |
| 5,000 | 0.399 / 0.500 ms | 5.073 / 5.226 ms | 0.287 ms |

Each history page contains 20 destination commits and one changed row per
commit. Checkpoint captures contain 100, 1,000, and 5,000 distinct working rows,
respectively. The results agree with the deterministic graph/diff work counters:
recent-page traversal does not grow with older history, and full capture does
not enumerate working rows. Selected checkpoint partitioning, arbitrary old
anchors, and ancestor-directory discovery have different costs and are not
covered by this timing claim.

`perf stat` over three full release-harness runs reported 8.137 ± 0.169 seconds
wall time, 72.6 billion cycles, 144.5 billion instructions, and 1.48 billion
cache misses per run. These counters include creating all 5,000 retained
checkpoints, all working writes, queries, and closing RocksDB; they are whole
workload evidence, not per-query costs.
