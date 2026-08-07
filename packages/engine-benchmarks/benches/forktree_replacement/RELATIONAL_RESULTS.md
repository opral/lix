# ForkTree relational-mutation and three-way-merge evidence

Date: 2026-08-07.

Frozen predecessor: `7f5f9be4190924ededd9cb880c76e0c80e3606f2`, tree
`e72a41b777742ba6e3495b5931d3e853ea780ca0`. The predecessor evidence is
unchanged:

- `README.md`: SHA-256
  `1562c9d299ea506307c9a64bd6bd612e1c35724ee9f1f6b7e2af97cd76ffbc30`
- `RESULTS.md`: SHA-256
  `983a26129f55021c825e6739607ef262d7f0487b7324c97043d46aa5c5f5f7b2`

Exact current-main comparator: `bc7e31158588e5722422b0f73ea95cebdb1ebe96`,
tree `5cb459d218bf071f89bfa08de2649bf67595507d`. The complete benchmark diff was
applied to an isolated detached worktree at that SHA, so both `current_lix` and
`forktree` below use the same current Lix, RocksDB, and SlateDB packages. No
production serving code is wired to ForkTree.

## Decision

**GO on the bounded relational architecture gate; NO-GO on production wiring
until the small-row physical-footprint tradeoff has an owner-safe design.**

The same blocked persistent tree now owns insert, update, delete, and mixed
transactions plus general three-way merge. Every measured CRUD cell is faster
and allocates less than exact current main on both adapters. Conflict detection,
NULL semantics, identity uniqueness, deterministic ordering, recovery,
retention, reclamation, blob sharing, corruption, partial-publication crash,
and selector/GC races pass on RocksDB and SlateDB.

The unresolved cost is physical: a 50K fixture with intentionally repetitive
tiny values occupies 2.5x--4.6x current post-flush space. Content hashes and
per-object adapter framing do not compress like current's repetitive keys. This
is a real replacement-layout tradeoff, not a compaction attribution. A future
packing design must retain one authenticated object space and cannot add a
lookup authority, side index, or alternate persisted format.

## Authority and complexity

The prototype retains exactly two physical planes:

1. `forktree_objects`, one immutable authenticated space for tree nodes, value
   packs, commit objects, semantic change objects, blob manifests, and chunks.
2. `forktree_refs`, one tiny mutable plane for branch/checkpoint/redo selectors
   and one publication/reclamation epoch.

There is one authoritative state root per commit. Transaction-local node maps
are authenticated working memory derived from that root and are discarded at
publication; they are not a durable index or byte authority. Publication puts
new immutable objects, one selector, and one epoch in one adapter commit.

Let `N` be live identities, `U` be changed identities, `F=8` internal fanout,
and `Z` be copied or split blocks.

- Current common-shape tracked-state materialization has the previously
  measured `O(N + D log_F N)` term; general changed-state construction is
  `O(M log_F N)` for materialized changes.
- ForkTree publication is `O(U log_F N + Z)` reads/writes, one adapter commit,
  and `O(U + Z)` working memory. Every tree level is fetched in one authenticated
  batch. Inserts split only a full block; deletes remove an empty block;
  nonempty underfull immutable blocks remain valid. There is no sibling-body
  repack in ordinary CRUD.
- Diff and merge are `O(changed paths + output + conflicts)` through hash
  pruning. A 10% uniformly distributed change naturally reaches most leaves,
  but no operation unconditionally rebuilds or scans the full tree.
- Branch, checkpoint, undo, and redo selector changes are `O(1)` and rotate the
  epoch in the same commit.
- Reclamation is the predecessor's one-universe authenticated mark/sweep. Its
  CPU is `O(selectors + reachable objects + scanned objects)`; page memory is
  bounded, while the benchmark mark set remains `O(reachable objects)`.

No tested relational feature requires a rootless state authority. Audit and
plugin intent can remain immutable change objects referenced by a commit whose
state root is unchanged; those objects explain the transition but do not serve
rows. That claim does not attempt an archive-derived-WASM redesign.

## Fixtures and measurement

- Focused gate: 1,000 live rows, one atomic K=32 mixed transaction.
- Scale gate: 50,000 live rows; K=32, K=500 (1%), and K=5,000 (10%) mixed
  transactions.
- Isolated operations: K=500 insert-only, delete-only, and update-only.
- History: 1,000 live rows and 1,000 K=1 commits, then point/range read, diff,
  branch, merge, undo, redo, checkpoint, retention, flush/drop/reopen, and GC.
- Relational oracle: disjoint merge plus overlapping update/update,
  delete/update, and insert/insert conflicts. Identical concurrent values merge
  without conflict. Every ordered row and nullable value is compared with one
  `BTreeMap` oracle.
- Multimedia smoke: an 8 MiB deterministic segmented source, 4 KiB localized
  edit, shared chunks, diff, branch, merge, range/full read, cold reopen,
  retained-root sweep, and final-reference reclamation. The predecessor's
  64/512 MiB and 74/75-reuse evidence remains the large-payload qualification.

Each timing sample creates and flushes a fresh database, excludes setup, and
times one public atomic transaction. Current Lix uses prepared public DML pages
for inserts/updates. Its public prepared-DML surface does not support DELETE,
so deletes use cached ordinary DELETE statements inside the same explicit
transaction; no benchmark adapter was added. Process CPU uses
`CLOCK_PROCESS_CPUTIME_ID`; allocations use the existing mimalloc wrapper. Disk
is post-flush. All cells completed far below 20 minutes.

Representative commands:

```text
FORKTREE_RELATIONAL_KIND=mixed cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> <layout> 50000 32 5 1 1
FORKTREE_RELATIONAL_KIND=mixed cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> <layout> 50000 500 3 1 1
FORKTREE_RELATIONAL_KIND=mixed cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> <layout> 50000 5000 3 1 1
FORKTREE_RELATIONAL_KIND=<insert|delete|update> cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> <layout> 50000 500 3 1 1
FORKTREE_RELATIONAL_ORACLE=1 FORKTREE_RELATIONAL_KIND=mixed cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- relational <backend> forktree 1000 32 1 0 1
cargo bench -q -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb -- history <backend> <layout> 1000 1 1 0 1000
```

## Focused gate and perfect-elimination ceiling

Medians; MB is decimal. Lower is better.

| Rows/K | Adapter | Current wall | ForkTree wall | Change | Current CPU | ForkTree CPU | Current alloc | ForkTree alloc | Current disk | ForkTree disk |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1K/32 | Rocks | 4.787 ms | 0.503 ms | -89.5% | 2.407 ms | 0.752 ms | 7.00 MB | 0.296 MB | 0.149 MB | 0.142 MB |
| 1K/32 | Slate | 5.457 ms | 1.273 ms | -76.7% | 3.421 ms | 1.276 ms | 10.57 MB | 1.592 MB | 0.121 MB | 0.084 MB |
| 50K/32 | Rocks | 29.955 ms | 0.980 ms | -96.73% | 27.791 ms | 1.829 ms | 96.46 MB | 0.594 MB | 0.662 MB | 2.235 MB |
| 50K/32 | Slate | 31.519 ms | 2.420 ms | -92.32% | 29.407 ms | 3.746 ms | 103.79 MB | 3.922 MB | 0.634 MB | 2.902 MB |

At 50K/K=32, CPU and allocation dominate current wall: process CPU is
27.8/29.4 ms and allocation is 96.5/103.8 MB on Rocks/Slate. Eliminating the
current materialization path has a 100% theoretical ceiling; the measured
ForkTree residual demonstrates a 96.73%/92.32% latency ceiling and
99.38%/96.22% allocation reduction. This is the dominant causal seam.

## Mixed churn

| K | Adapter | Current wall | ForkTree wall | Wall change | Current CPU | ForkTree CPU | CPU change | Current alloc | ForkTree alloc | Alloc change |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 32 | Rocks | 29.955 ms | 0.980 ms | -96.73% | 27.791 ms | 1.829 ms | -93.42% | 96.46 MB | 0.594 MB | -99.38% |
| 32 | Slate | 31.519 ms | 2.420 ms | -92.32% | 29.407 ms | 3.746 ms | -87.26% | 103.79 MB | 3.922 MB | -96.22% |
| 500 | Rocks | 68.654 ms | 6.844 ms | -90.03% | 66.805 ms | 9.553 ms | -85.70% | 263.60 MB | 5.967 MB | -97.74% |
| 500 | Slate | 71.494 ms | 21.845 ms | -69.44% | 70.177 ms | 22.908 ms | -67.36% | 285.45 MB | 46.87 MB | -83.58% |
| 5,000 | Rocks | 245.452 ms | 40.947 ms | -83.32% | 243.622 ms | 43.533 ms | -82.13% | 931.13 MB | 35.35 MB | -96.20% |
| 5,000 | Slate | 286.649 ms | 121.055 ms | -57.77% | 293.496 ms | 122.653 ms | -58.21% | 1,104.10 MB | 271.15 MB | -75.44% |

One-sample phase RSS deltas at K=500 are about 10.0/16.8 MB current versus
3.4/0.9 MB ForkTree on Rocks/Slate. At K=5,000 they are 24.7/54.6 MB current
versus 14.0/9.5 MB ForkTree. Warm allocator retention makes later-sample RSS
mostly flat, so these are phase deltas rather than medians.

### Backend and tree work

Logical counters are identical on both adapters. Slate physical counters are
shown because Rocks does not expose equivalent per-object byte counters here.

| K | Layout | Get calls / keys / value bytes | Puts / logical write bytes | Slate physical reads / bytes | Slate physical writes / bytes |
|---:|---|---:|---:|---:|---:|
| 32 | Current | 359 / 739 / 2.703 MB | 98 / 18.2 KB | 246 / 2.707 MB | 5 / 7.6 KB |
| 32 | ForkTree | 9 / 274 / 32.4 KB | 162 / 42.4 KB | 7 / 34.1 KB | 1 / 39.8 KB |
| 500 | Current | 1,381 / 3,103 / 8.071 MB | 729 / 467--468 KB | 652 / 8.081 MB | 7 / 338.6 KB |
| 500 | ForkTree | 9 / 2,679 / 281.4 KB | 1,564 / 389.1 KB | 7 / 299.2 KB | 1 / 364.1 KB |
| 5,000 | Current | 10,381 / 24,103 / 44.980 MB | 5,265 / 1.792 MB | 3,652 / 45.007 MB | 7 / 542.1 KB |
| 5,000 | ForkTree | 9 / 14,358 / 933.0 KB | 8,461 / 1.660 MB | 7 / 1.027 MB | 1 / 1.525 MB |

The initial state has exactly 13,396 authoritative tree/value-pack objects:
6,250 leaves, 6,250 packs, and 896 internal nodes. Derived from the exact
authenticated-node loads, unchanged state-object sharing is 13,282/13,396
(99.15%) at K=32, 12,279/13,396 (91.66%) at K=500, and 7,497/13,396
(55.96%) at K=5,000. The drop at 10% is expected for uniformly distributed
changes, not a full-tree fallback.

Object bytes per logical mutation byte are 47.2x, 27.5x, and 11.3x at K=32,
500, and 5,000. Node-only amplification is 46.1x, 26.7x, and 10.5x. Small
batches pay ancestor-hash publication; batching amortizes it.

## Isolated CRUD at 1% churn

| Operation | Rocks current | Rocks ForkTree | Change | Slate current | Slate ForkTree | Change |
|---|---:|---:|---:|---:|---:|---:|
| Insert 500 | 50.169 ms | 7.637 ms | -84.78% | 59.694 ms | 25.591 ms | -57.13% |
| Delete 500 | 86.074 ms | 6.059 ms | -92.96% | 89.155 ms | 18.879 ms | -78.82% |
| Update 500 | 41.909 ms | 5.996 ms | -85.69% | 41.530 ms | 19.020 ms | -54.20% |

Allocation falls 96.49%/75.19% for insert, 98.50%/89.58% for delete, and
96.80%/77.59% for update on Rocks/Slate. Every operation publishes one commit
and exactly matches the current-Lix row oracle, including NULL values.

## History, merge, and selectors

Exact-main 1K-history results:

| Phase | Rocks current | Rocks ForkTree | Change | Slate current | Slate ForkTree | Change |
|---|---:|---:|---:|---:|---:|---:|
| Point | 707.6 us | 53.3 us | -92.46% | 786.9 us | 90.2 us | -88.54% |
| Range 32 | 1,281.1 us | 151.4 us | -88.18% | 1,376.1 us | 317.5 us | -76.92% |
| K=1 update | 2,254.3 us | 40.3 us | -98.21% | 2,575.6 us | 117.2 us | -95.45% |
| Hash-pruned diff | 2,067.6 us | 55.4 us | -97.32% | 2,193.0 us | 165.4 us | -92.46% |
| Branch root | 1,871.6 us | 21.7 us | -98.84% | 2,032.6 us | 25.0 us | -98.77% |
| Disjoint merge | 163.835 ms | 136.7 us | -99.92% | 172.412 ms | 309.5 us | -99.82% |
| Undo | 395.5 us | 12.9 us | -96.74% | 1,143.7 us | 21.6 us | -98.11% |
| Redo | 337.4 us | 9.3 us | -97.25% | 668.9 us | 9.3 us | -98.61% |
| Checkpoint | 10.218 ms | 8.1 us | -99.92% | 12.150 ms | 15.9 us | -99.87% |
| Cold reopen/recovery | 9.502 ms | 0.418 ms | -95.60% | 11.554 ms | 1.029 ms | -91.09% |

The general relational oracle independently measures a three-row disjoint
merge at 201 us Rocks / 501 us Slate. An overlapping four-row merge reports
three exact conflicts in 109/374 us and performs zero writes: divergent
update/update, delete/update, and same-PK insert/insert. The identical NULL edit
is not a conflict. The target selector remains byte-identical after conflicts
and invalid insert/update/delete attempts.

The generalized loader raises predecessor K=1 update latency from 30.3 to
40.3 us Rocks and 106.2 to 117.2 us Slate. This is an explicit unification
tradeoff: one insert/delete/update path replaces the former update-only path.
It remains 98.21%/95.45% faster than exact current main and does not add an
authority.

History GC preserves all 7,296 objects while three roots are retained. After
release it reclaims 7,021 objects / 2.107 MB. Final history disk is 3.091 MB
versus current 2.945 MB on Rocks (+4.96%) and 3.881 MB versus 5.752 MB on Slate
(-32.5%). Slate final reclamation costs 80.3 ms and 233.4 MB allocated; GC is
offline work and remains a production optimization target.

## Causal iterations and rejected paths

1. The first correct general path copied and repacked every sibling body at
   each touched parent. At 1% mixed churn it still won Rocks but took 113.0 ms
   on Slate versus 71.5 ms current. It performed 4,899 read snapshots, loaded
   6,982 values, and wrote 4,386 nodes.
2. One operation-local level-batched loader cut read snapshots from 4,899 to
   nine, but Slate only improved to 102.6 ms. This proved N+1 request overhead
   was removable but not dominant enough; the remaining term was sibling-body
   decode/allocation and rewrite.
3. The accepted path copies parent references directly, splits only a full
   changed block, and permits valid nonempty underfull blocks after deletion.
   At 1% it loads 1,117 values and writes 1,559 nodes. Exact-main Slate falls to
   21.85 ms, 69.44% faster than current. The fanout-body repacker was deleted.

A separate update-only fast path was not restored: it would duplicate mutation
orchestration and obscure whether general semantics truly use one tree. A
durable occupancy index, row overlay, alternate canonical tree, full-tree
rebuild, and compatibility codec were never added.

## Crash, corruption, reopen, and reclamation oracle

Both adapters pass these deterministic cases:

- a transaction writes a nullable pack and changed tree path, then publishes
  commit/ref/epoch atomically;
- a staged authenticated pack/leaf/delta without a selector simulates a crash
  before root publication and is reclaimed as unreachable;
- publication-first rotates the epoch and rejects stale GC;
- GC-first rotates the epoch and rejects stale root-only publication; retry
  rereads the root/epoch and succeeds;
- partial tree-node and blob-chunk corruption fail closed at immutable-adapter
  insertion or owner-side hash/domain validation;
- close/drop/reopen reconstructs the exact ordered relational state and edited
  blob with cold runtime caches;
- a checkpoint and old-blob branch preserve old roots; release plus sweep
  reclaims old objects while shared chunks and the live root survive; deleting
  the final branch leaves object inventory exactly `(0, 0)`.

## Physical footprint and deletion potential

Post-flush 50K mixed footprint:

| K | Rocks current | Rocks ForkTree | Ratio | Slate current | Slate ForkTree | Ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 32 | 0.662 MB | 2.235 MB | 3.37x | 0.634 MB | 2.902 MB | 4.58x |
| 500 | 1.028 MB | 2.602 MB | 2.53x | 1.030 MB | 3.347 MB | 3.25x |
| 5,000 | 1.490 MB | 3.967 MB | 2.66x | 1.684 MB | 5.080 MB | 3.02x |

These are post-flush LSM files without controlled compaction. No claim is made
about SST amplification attribution. The stable both-adapter regression is
large enough that production wiring is blocked even though all latency,
allocation, read-byte, and semantic gates pass.

At exact `bc7e311`, the gross source envelope of durable-layout subsystems that
this model could collapse is 75,199 Rust lines including inline tests:

- `tracked_state`: 53,171 lines;
- `changelog`: 3,423 lines;
- `commit_graph`: 3,121 lines;
- `branch`: 1,088 lines;
- selected merge/checkpoint/undo/redo/GC orchestration files: 9,668 lines;
- `binary_cas`: 4,728 lines.

This is an exact deletion *upper envelope*, not a promise to erase every line.
Public SQL/session APIs, CDC, runtime validation, and graph/query facades remain
necessary, but their duplicate durable trees, change materialization, branch
root tables, and separate CAS key-space authority could be replaced by the one
object graph plus selector/epoch plane. A production plan must enumerate files
line by line before claiming realized LOC deletion.

## Validation

- `cargo fmt --check`: pass.
- `git diff --check`: pass.
- `cargo clippy -p lix_benchmarks --bench forktree_replacement --features
  'storage-benches slatedb' -- -D warnings`: pass.
- RocksDB and SlateDB focused, churn, CRUD, history, merge, reopen, GC,
  corruption, and race oracles: pass.
- No production file, PR, or serving registration was added.
