# Version-control SQL complexity and profiling

The SQL consolidation uses one first-parent iterator for log and history, and the same endpoint comparison for history and diff. Checkpoint status is evaluated by the anchored log surface, while the commit inventory remains available for synchronization and DAG inspection.

## Complexity contract

Let V be mainline commits visited before a requested page is satisfied, P be selected checkpoint commits, D be endpoint differences examined, F be file descriptors, and A be ancestor directory descriptors needed to resolve paths.

- Log pages perform O(V) graph-node reads and stream rows. Checkpoint filtering
  can visit unmarked commits between returned checkpoints; it does not promise O(P) when arbitrarily many automatic commits intervene.
- Paged history applies destination metadata restrictions before diffs. Its work
  is O(V + sum of selected endpoint comparison costs), not all historical snapshot sizes. Exact selected destination IDs stop traversal after the last requested ID is found. IDs outside the mainline can require walking to root.
- The supported left-join page shape uses the existing probe-key join optimizer
  to pass page IDs into history before opening endpoint diffs. The equivalent two-query IN form has the same destination pruning.
  Keep the page relation on the left and the history relation on the right;
  the physical rewrite is intentionally limited to joins whose build side is
  collected before the history scan is opened. Inner-join timings are useful
  diagnostics but do not replace the left join's empty-checkpoint semantics.
- Full checkpoint publication performs constant logical work with respect to
  the number of working changes: capture/alias a state root, publish immutable metadata, advance control and stage the inventory index. Physical index writes retain the storage backend's lookup/update costs; reclamation is asynchronous.
- File-ID restricted ancestor-move comparisons use descriptor point reads and
  cached ancestor resolution. Unfiltered ancestor-change discovery currently scans descriptor identities of the requested relation, O(F + A) descriptor work at each endpoint (use directory count for F when querying directories). It never decodes file bytes. A reverse directory-to-descendant index would be needed to bound discovery to affected descendants alone.
- Sparse bootstrap pages checkpoint headers/flags, while certifying state only
  for serving heads and working baselines. Off-branch historical state hydrates on demand; bootstrap must not materialize every checkpoint state.
- Snapshot header validation uses a topological traversal with O((H + E) log H)
  time and O(H + E) memory for H headers and E known parent edges. Inventory-only jump boundaries stay deferred; validation does not fetch checkpoint states.
- Migration rewrites canonical metadata and traverses deduplicated manifest
  dependencies. It must not enumerate every row of every checkpoint merely to backfill a boolean.

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

Use `LIX_PROFILE_MEMORY=1` for a memory control. Each query reports the median and p95 after three warmups and 21 measured repetitions. Full capture reports its actual operation after a nonempty working interval, not empty follow-ups.

The storage-backed checkpoint surface harness exercises the same anchored log
page and selected-destination left joins for key-value and file history:

```sh
cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench checkpoint_history_scale --features storage-benches,slatedb -- \
  setup-query rocksdb /tmp/checkpoint-query-rocks 10000 1 1000
LIX_CHECKPOINT_HISTORY_QUERY_CHECKPOINTS=1000 \
cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench checkpoint_history_scale --features storage-benches,slatedb -- \
  measure-query rocksdb /tmp/checkpoint-query-rocks 10000 1 7 3
```

The ignored partial-replica profile adds the same three queries to its cold
open/first-operation scorecard. It includes sync bootstrap traffic and is a
diagnostic cold-replica measurement rather than a storage-only latency result:

```sh
LIX_PARTIAL_REPLICA_PROFILE_OUTPUT=/tmp/lix-partial-profile.json \
cargo test --manifest-path packages/e2e/Cargo.toml \
  --features sdk-tests,server-protocol --test sync_mode \
  partial_replica_open_profile -- --ignored --nocapture
```

One current-tree run on 2026-09-20 produced the following cold scorecard.
Times are milliseconds; request counts include background sync traffic. The
authority was in-process memory storage and the replica used native filesystem
storage, so these values are diagnostic rather than a remote-service SLA:

| Case | Open | Log page | Key history page | File history page | File-page requests |
| --- | ---: | ---: | ---: | ---: | ---: |
| base (16 rows) | 11.31 | 1.13 | 20.08 | 42.94 | 11 |
| rows (1,600 rows) | 9.76 | 1.20 | 22.13 | 687.94 | 113 |
| branches (16) | 9.55 | 1.12 | 24.55 | 45.13 | 11 |
| history (200 updates) | 9.81 | 1.06 | 20.03 | 42.69 | 11 |
| content (1 MiB) | 9.51 | 1.18 | 20.22 | 44.28 | 11 |

The 1,600-row case is a material cold-replica diagnostic: the file-history
page took 688 ms and emitted 610 KB (596 KiB) across 113 requests while the
key-history page remained 22 ms. The result includes substantial
hydration/background traffic at that width and should remain visible in future
profile runs rather than being attributed to the SQL join alone.

## Development evidence (2026-09-09)

Initial in-memory test-profile run, five commits per page:

| Retained checkpoints | Log graph reads | Joined preview graph reads | Endpoint diffs | Combined query time |
| --- | ---: | ---: | ---: | ---: |
| 10 | 5 | 10 | 5 | 10.149 ms |
| 30 | 5 | 10 | 5 | 7.877 ms |
| 60 | 5 | 10 | 5 | 8.170 ms |

These are development-build observations, not a release latency claim. The work-counter assertions are the regression gate. `perf stat` over five whole fixture/test runs reported 0.22587 ± 0.00455 seconds wall time, approximately 1.59 billion cycles and 1.12 billion instructions per run. Those counters include fixture creation, checkpoints, and all tested queries; they do not isolate the SQL query alone. Optimized backend results should be recorded with their build revision when the implementation is finalized.

## Optimized RocksDB measurements

### Checkpoint-log cleanup profile (2026-09-20)

A release run on the current working tree used the RocksDB
`version_control_sql_profile` with 100, 500, and 1,000 retained checkpoints.
Each page has 20 rows, and every measured query discarded three warmups before
21 samples. The fixture writes both a key-value row and a file row before each
checkpoint, so the file-history and inner-join timings below exercise the same
destination set as the left-join queries:

The current and matched baseline runs were each single runs on this host while
validation builds were concurrent; they are concrete regression evidence, not a
statistical speedup claim.

| Retained | Log page | Key history left / inner | File history left / inner | Full checkpoint |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.484 / 0.526 ms | 2.821 / 2.846; 2.834 / 2.846 ms | 3.402 / 3.424; 3.401 / 3.413 ms | 0.380 ms |
| 500 | 0.601 / 0.696 ms | 3.257 / 3.335; 3.256 / 3.279 ms | 3.849 / 3.894; 3.856 / 3.871 ms | 0.351 ms |
| 1,000 | 0.718 / 0.732 ms | 4.329 / 4.512; 4.237 / 4.350 ms | 4.803 / 4.820; 4.823 / 4.844 ms | 0.369 ms |

Each query pair is median / p95; within the key and file columns, the first
pair is the left join and the second is the inner join. The corresponding
matched pre-cleanup run used the same fixture and query shapes on the
`c9aa3d8ac` worktree. Its log page predicates included the old
`is_checkpoint_active` column so this comparison includes active-status
evaluation:

| Retained | Log page | Key history left / inner | File history left / inner | Full checkpoint |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.517 / 0.563 ms | 2.868 / 2.890; 2.887 / 2.906 ms | 3.457 / 3.532; 3.465 / 3.497 ms | 0.354 ms |
| 500 | 0.629 / 0.667 ms | 3.245 / 3.993; 3.406 / 3.883 ms | 3.836 / 3.865; 3.860 / 3.895 ms | 0.350 ms |
| 1,000 | 0.756 / 1.044 ms | 4.372 / 4.891; 4.241 / 4.330 ms | 4.787 / 4.812; 4.794 / 4.816 ms | 0.367 ms |

On this matched run, current log p50 is 5% to 6% lower, key-history left join
is within 2%, and file-history left join is within 2% of the active-status
baseline. Full-capture timings differ by at most 7% in this single run. The
current inner joins track their left-join counterparts closely, which is
expected when all selected checkpoints have matching history; the left join
remains the semantic contract for empty checkpoint history.

The implementation working tree based on `f6e712855e22b2204736e5c181a372f1c8696ed7` was profiled with the release harness on 2026-09-09. Timings exclude setup; concurrent validation builds were running, so these are observations rather than an isolated-machine benchmark or a latency guarantee.

| Retained checkpoints | Log median / p95 | Joined history median / p95 | Nonempty full checkpoint |
| ---: | ---: | ---: | ---: |
| 100 | 0.619 / 1.038 ms | 4.144 / 6.271 ms | 0.231 ms |
| 1,000 | 0.383 / 0.454 ms | 3.658 / 3.764 ms | 0.231 ms |
| 5,000 | 0.399 / 0.500 ms | 5.073 / 5.226 ms | 0.287 ms |

Each history page contains 20 destination commits and one changed row per commit. Checkpoint captures contain 100, 1,000, and 5,000 distinct working rows, respectively. The results agree with the deterministic graph/diff work counters: recent-page traversal does not grow with older history, and full capture does not enumerate working rows. Selected checkpoint partitioning, arbitrary old anchors, and ancestor-directory discovery have different costs and are not covered by this timing claim.

`perf stat` over three full release-harness runs reported 8.137 ± 0.169 seconds wall time, 72.6 billion cycles, 144.5 billion instructions, and 1.48 billion cache misses per run. These counters include creating all 5,000 retained checkpoints, all working writes, queries, and closing RocksDB; they are whole workload evidence, not per-query costs.
