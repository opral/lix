# Checkpoint scale profile

SQLite exploration was measured on 2026-07-23 and 2026-07-24 against the
unshipped checkpoint implementation at `23be0ad5`, then against the
checkpoint-only changes in this worktree. Acceptance runs are now explicitly
RocksDB and SlateDB; the old SQLite data is retained below as diagnostic
history, not acceptance evidence.

## Acceptance workload

- RocksDB and local-object-store SlateDB, each in its own explicit directory
  on `tmpfs`.
- 10,000 files with deterministic 256-byte payloads.
- Seeded in 100 auto-commits, followed by one initial checkpoint.
- 1,000 measured checkpoints.
- Each measured interval changes 10 files across 5 atomic auto-commits.
- 5,000 auto-commits and 10,000 file versions are compacted in total.
- The run checks 10,000 live files and their deterministic payloads after
  checkpointing, zero working changes, 1,002 visible checkpoints, and the same
  history after reopen.

The harness detects checkpoint GC from the engine's post-collection tracing
event rather than backend internals, latency, or a modeled schedule. Each
`observed_background_gc` sample includes exact swept commit, change, and
tracked-root counts. Classification therefore remains backend-neutral when
logical maintenance is triggered by age and collectible-interval debt.

This is the chosen sparse happy-path model: one branch, no merges or sync, and
0.1% of files changed per checkpoint. The payload is deliberately small so
graph/tree and inline-LSM overhead remain visible instead of benchmarking blob
throughput. In particular, 256 bytes is below RocksDB's 32 KiB blob threshold;
storage reclamation claims for large payloads require a separate >=32 KiB
sentinel. A width sentinel changes all 10,000 files.

`create_checkpoint` measures backend acceptance latency. At milestones the
harness separately times the adapter's explicit flush and recursively samples
directory bytes/files. SlateDB flush is a WAL flush; dropping its final handle
also closes the database and flushes memtables to L0. The summary therefore
reports both after-flush and after-close physical size. RocksDB true reopen is
also tested only after every shared adapter handle has been dropped. Physical
bytes are not treated as immediately reclaimed: RocksDB deletes leave
tombstones until compaction, while SlateDB compaction and object GC are
asynchronous.

Machine:

- AMD EPYC-Genoa, 16 vCPUs (the harness uses one current-thread runtime).
- Linux 7.0.0-15-generic.
- Rust `1.97.0-nightly (b954122bb 2026-05-20)`.
- `tmpfs`, so latency results primarily measure engine/backend CPU work.
  Recursive directory bytes and file counts remain physical storage
  measurements.

Acceptance results are populated only after independent RocksDB and SlateDB
fixtures complete the same workload. SQLite is not acceptance evidence for
this implementation.

## Current backend results — 2026-07-24

The current hot path was measured against the 10,000-file SlateDB fixture
after 1,172 visible checkpoints (1,202 retained commits), and the equivalent
RocksDB fixture after 1,002 checkpoints. The harness alternates seven warmed
repetitions of each query and reports the median.

| Backend | Checkpoints | `LIMIT 20` | `LIMIT 128` direct | `LIMIT 128` after cut | Full rows | `COUNT(*)` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SlateDB | 1,172 | 55.867 ms | 198.379 ms | 43.805 ms | 43.091 ms | 44.863 ms |
| RocksDB | 1,002 | 0.523 ms | 0.823 ms | 1.082 ms | 1.366 ms | 1.437 ms |

Before the checkpoint-record scan, the same SlateDB fixture took 2,011.487 ms
for `SELECT count(*) FROM lix_checkpoint`: one serial point read per
checkpoint. The paged record-map path brings the same query to roughly
43--45 ms (about 45x). The full-history path now makes two 1,024-record
range pages at this depth instead of roughly 1,172 random reads.

The different backends justify a conservative hybrid cutoff: small UI pages
remain page-bounded on the direct chain walk, while unbounded queries and
`LIMIT >= 64` scan records once and walk the checkpoint chain in memory. At
`LIMIT 128`, this avoids SlateDB's 198 ms serial-read cost at the price of
0.259 ms on local RocksDB. On this local SlateDB fixture the direct `LIMIT 20`
was slower than the full map path, so this is an asymptotic/cross-backend
policy—not a claim that it is SlateDB-optimal. The harness prints all three
query shapes so the cutoff can be revisited with an S3-backed SlateDB fixture.

Logical GC was separately re-profiled on the SlateDB 10,000-file fixture at
1,167 visible checkpoints: 320.792 ms total, of which 314.790 ms was the
linear changelog scan. It is now scheduled after durable checkpoint publication
and is excluded from foreground checkpoint latency. The previous physical
tree/CAS traversal took 39,792.387 ms in a nearby deep fixture. Collection now
removes only logical changelog and tracked-root metadata; immutable tree/CAS
orphan repair is an offline maintenance concern, so physical bytes can lag
logical collection.

The asynchronous path was then exercised on the same 10,000-file SlateDB
fixture from 1,292 through 1,332 visible checkpoints (40 intervals × 10 files
× 5 auto-commits). The one due background sweep completed during the bounded
idle settle window:

| Metric | Result |
| --- | ---: |
| Foreground `create_checkpoint` p50 / p95 / p99 / max | 86.960 / 94.253 / 96.050 / 96.050 ms |
| Background logical GC | 349.587 ms |
| GC changelog scan | 343.285 ms |
| GC physical tree/CAS scan | not part of checkpoint GC |
| Sweep | 825 commits, 1,614 changes, 825 tracked roots |
| Final visible checkpoints / live commits | 1,332 / 1,337 |

That run preserves the important separation: a ~350 ms history-sized sweep is
observable maintenance work, not a checkpoint-latency outlier. The harness
waits at most five seconds after its timed workload so it can verify idle
collection and reopen correctness without folding that wait into checkpoint
throughput.

## Historical SQLite result

The baseline is one full run from the v1 baseline fixture. Optimized values
below are from the median wall time of three independent copies of one
final-format fixture. The fixtures were prepared independently because the
intentional pre-ship recovery-ref v2 cut cannot open a v1 recovery ref.

| Metric | Baseline | Optimized | Change |
| --- | ---: | ---: | ---: |
| Total measured run | 335.728 s | 45.216 s | 7.42x faster |
| Throughput | 2.979 checkpoints/s | 22.116 checkpoints/s | 7.42x |
| `create_checkpoint` mean | 293.861 ms | 6.611 ms | 44.5x faster |
| `create_checkpoint` p50 | 286.846 ms | 3.812 ms | 75.2x faster |
| `create_checkpoint` p95 | 418.685 ms | 4.738 ms | 88.4x faster |
| `create_checkpoint` p99 | 442.808 ms | 158.557 ms | 2.79x faster |
| First 100 mean | 182.155 ms | 4.145 ms | 43.9x faster |
| Last 100 mean | 418.514 ms | 7.100 ms | 58.9x faster |
| Initial 10k-file checkpoint | 316.553 ms | 154.341 ms | 2.05x faster |
| Final SQLite file | 54,022,144 B | 57,929,728 B | +7.23% |
| Final live commits | 1,007 | 1,212 | +205 deferred auto-commits |

The three final-format optimized runs took 44.224 s, 45.216 s, and 45.522 s.
Across them, checkpoint p50 was 3.667-3.879 ms, p95 was 4.655-4.738 ms, and
p99 was 151.616-158.741 ms.

| Run | Wall time | Mean | p50 | p95 | p99 | Max | Final file |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline | 335.728 s | 293.861 ms | 286.846 ms | 418.685 ms | 442.808 ms | 514.121 ms | 54,022,144 B |
| Optimized 1 (reported median) | 45.216 s | 6.611 ms | 3.812 ms | 4.738 ms | 158.557 ms | 276.248 ms | 57,929,728 B |
| Optimized 2 | 44.224 s | 6.486 ms | 3.667 ms | 4.655 ms | 151.616 ms | 281.403 ms | 57,987,072 B |
| Optimized 3 | 45.522 s | 6.732 ms | 3.879 ms | 4.721 ms | 158.741 ms | 298.979 ms | 57,921,536 B |

Wall time includes the same seven milestone storage flush/samples in baseline
and optimized runs. The `create_checkpoint` distributions stop their timers
before that instrumentation and are the primary comparison.

There are 15 scheduled GC checkpoints among the 1,000 measurements. They are
1.5% of samples, which is why normal publication remains below 5 ms at p95
while p99 includes maintenance. Within each 100-checkpoint depth band,
optimized p95 ranges from 3.43 ms to 5.31 ms. Baseline checkpoint latency grew
from a 182.155 ms first-100 mean to a 418.514 ms last-100 mean.

At depth 1,000, repeated optimized runs measured:

- `lix_working_change`: 2.567-3.273 ms.
- Full `lix_checkpoint` history: 3.504-4.923 ms.
- Reopen plus full checkpoint history: 3.887-4.182 ms.
- Peak RSS in the median wall-time run: 290,700 KiB.
- An additional instrumented run observed 132,311,376 B peak combined
  database, WAL, and SHM bytes. After the final forced WAL truncation it used
  57,946,112 B combined.

## Historical SQLite scaling matrix

The following fixed-64-policy baseline was measured on 2026-07-24 before the
adaptive repository-global policy. Every case starts from the same
final-format 10,000-file seed. Each checkpoint changes one 256-byte file in
one auto-commit. Scheduled sweep samples are reported separately from
ordinary checkpoint publication. The current acceptance harness instead uses
the backend-neutral tracing observation described above.

| Measured checkpoint depth | Ordinary mean | Ordinary p95 | Scheduled GC samples | Scheduled GC mean | Scheduled GC max | Main database |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 2.389 ms | 2.604 ms | 1 | 106.381 ms | 106.381 ms | 18,259,968 B |
| 500 | 3.163 ms | 4.339 ms | 7 | 132.877 ms | 159.729 ms | 25,067,520 B |
| 1,000 | 2.905 ms | 3.477 ms | 15 | 140.226 ms | 183.060 ms | 37,421,056 B |
| 2,000 | 3.409 ms | 4.263 ms | 31 | 188.058 ms | 276.418 ms | 62,214,144 B |
| 2,001-5,000 segment | 4.157 ms | 6.250 ms | 47 | 387.068 ms | 538.950 ms | 117,366,784 B at depth 5,000 |

The 5,000-depth fixture extends the measured 2,000-depth fixture by 3,000
checkpoints. The two segments took 21.428 s and 48.334 s respectively. Across
the cumulative 5,000 checkpoints, scheduled GC consumed approximately
24.022 s, or 34.4% of their combined 69.762 s wall time.

SQLite `dbstat` attributes 96,866,304 B of the 101,662,720-byte main-database
growth from the seed to depth 5,000 (95.28%) to tracked-state tree chunks.
Tracked chunks grew from 5,128,192 B to 101,994,496 B. This makes persisted
tracked-tree chunk bytes a materially better storage-debt signal than raw
checkpoint count.

Selected sweep samples expose an approximately linear cost in retained
checkpoint history:

| Checkpoint sequence | Scheduled GC |
| ---: | ---: |
| 64 | 103.695 ms |
| 512 | 140.389 ms |
| 1,024 | 183.381 ms |
| 2,048 | 258.319 ms |
| 3,072 | 360.638 ms |
| 4,096 | 417.493 ms |
| 4,928 | 538.950 ms |

Running this repository-wide O(K) sweep every fixed 64 checkpoints therefore
makes cumulative maintenance O(K² / 64). Ordinary publication remains in the
3-6 ms range through depth 5,000, so it is not the next scaling bottleneck.

### Checkpoint history `LIMIT`

The surface harness warms both queries and reports the median of seven
alternating measurements:

| Checkpoint depth | `LIMIT 20` | Full history |
| ---: | ---: | ---: |
| 100 | 0.720 ms | 0.681 ms |
| 500 | 1.767 ms | 1.777 ms |
| 1,000 | 2.952 ms | 3.019 ms |
| 2,000 | 5.329 ms | 5.389 ms |
| 5,000 | 13.320 ms | 13.629 ms |

`LIMIT 20` has the same linear curve as full history because the provider
walks the entire checkpoint chain and truncates afterward. Bounding the
record-only chain walk by the pushed-down limit changes the common history
view from O(K) to O(20).

### One-generation churn sentinels

These cases all trigger their first scheduled sweep at sequence 64. General
file-update time is excluded; the table isolates ordinary checkpoint
publication, the scheduled sweep, and physical checkpoint-related storage.
Each row is one run, so small differences between the two 4,032-change
layouts should not be over-interpreted.

| Files per checkpoint | Auto-commits per checkpoint | Changed rows in generation | Ordinary mean | GC at sequence 64 | Final main database | Peak database + WAL + SHM |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 63 | 2.590 ms | 121.095 ms | 18,280,448 B | 29,771,256 B |
| 64 | 1 | 4,032 | 9.327 ms | 183.299 ms | 46,563,328 B | 188,091,872 B |
| 64 | 8 | 4,032 | 4.822 ms | 165.268 ms | 41,197,568 B | 200,561,400 B |
| 512 | 8 | 32,256 | 16.692 ms | 516.380 ms | 164,220,928 B | 1,063,548,912 B |

The 32,256-row generation reaches more than 1 GiB before its scheduled sweep
despite the final main database being 164 MB. A checkpoint-count trigger
cannot bound this width-dependent storage spike. Changed-key and tracked-tree
chunk-byte debt are substantially better scheduling signals; commit count is
still useful for deep intervals that repeatedly edit the same keys.

### 10k-wide sentinel

After the 1,000-checkpoint run, changing all 10,000 files in one atomic
auto-commit took 23.168 s through the normal SQL write path. Creating the
checkpoint itself took 115.745 ms. The write path is outside checkpointing and
was deliberately not optimized.

Advancing 22 small checkpoints to the next scheduled sweep produced one
348.649 ms maintenance checkpoint. Live commits fell from 1,214 to 1,026 and
SQLite exposed 1,115 reusable pages. This bounds the happy-path wide
checkpoint, but it also shows why the count-based maintenance cadence should
eventually become a byte/debt budget if dense intervals are common.

## Storage profile

After forced WAL truncation, SQLite `dbstat` attributes 3,731,456 bytes of the
3,907,584-byte optimized main-database increase (95.5%) to tracked-state tree
chunks. These are the final partial GC generation. Binary CAS page counts and
changelog change-record page counts are identical before and after. The peak
combined database/WAL/SHM result above is the disk-space safety number; the
table below explains retained main-database pages.

| Storage space | Baseline | Optimized |
| --- | ---: | ---: |
| Tracked tree chunks (`0x0004_0001`) | 35,692,544 B | 39,424,000 B |
| Tracked commit roots (`0x0004_0004`) | 167,936 B | 204,800 B |
| Binary CAS manifests (`0x0005_0001`) | 1,736,704 B | 1,736,704 B |
| Binary CAS chunks (`0x0005_0003`) | 7,286,784 B | 7,286,784 B |
| Changelog commits (`0x0006_0001`) | 98,304 B | 118,784 B |
| Changelog changes (`0x0006_0002`) | 7,512,064 B | 7,512,064 B |
| Changelog refs (`0x0006_0003`) | 569,344 B | 602,112 B |

The optimized live-commit count is bounded by retained checkpoints plus less
than one 64-checkpoint auto-commit generation. In this run the final sweep was
at sequence 960, leaving 41 intervals x 5 auto-commits = 205 additional live
commits.

## Profile and cuts

The baseline had two cumulative hot paths:

1. Each checkpoint discovered the latest checkpoint through general change
   history. That loaded full reachable commits, all member refs, and all member
   changes before filtering checkpoint markers.
2. Every checkpoint synchronously ran repository-wide mark/sweep. It scanned
   live state, changelog, tracked roots/chunks, and binary CAS, and planned
   changelog collection twice.

The hard cuts are:

- Resolve the branch checkpoint marker from tracked state by key. Its tracked
  value already carries the commit that last changed it.
- For full history and larger pushed-down limits, scan record-only commits in
  fixed pages and follow the direct checkpoint-parent chain in a hash map.
  Member ref chunks are irrelevant. Small UI pages retain the point walk.
- Keep recovery-ref rotation atomic on every checkpoint. GC state is
  repository-global, so concurrent branches do not schedule redundant sweeps.
  Its grace interval is at least 64 checkpoints and grows with mature history,
  making total checkpoint-GC scan work linear rather than fixed-cadence
  quadratic.
- During background logical GC, hydrate flat payloads only for branch refs and plan/
  stage changelog collection once. Delete dead tracked-root metadata directly
  from swept commit ids; defer immutable tracked-tree and Binary CAS orphan
  repair to an explicit offline maintenance path.

The persistent hash-guided tracked-state diff and structural sharing remain
unchanged.

The benchmark remains deliberately scoped to the 90% path: one branch, no
merges, sync, or concurrent writers. It measures logical reachability and
serving latency separately from eventual physical-object reclamation. The
historical SQLite sections below are retained only as diagnosis history; they
are not used to accept or size the SlateDB/RocksDB implementation.

## External design evidence

- As a maintenance-cadence analogy, [DuckDB checkpoints automatically by WAL-size threshold](https://duckdb.org/docs/current/sql/statements/checkpoint);
  the default threshold is documented as 16 MiB in
  [DuckDB configuration](https://duckdb.org/docs/stable/configuration/overview).
- Also as a WAL-maintenance analogy, [Turso recommends periodic or WAL-size-triggered checkpointing](https://docs.turso.tech/sync/checkpoint),
  rather than doing full compaction per logical write.
- [Jujutsu separates operation abandonment from later garbage collection](https://docs.jj-vcs.dev/latest/cli-reference/)
  and publishes lightweight operation heads over immutable content-addressed
  objects in its [concurrency design](https://docs.jj-vcs.dev/latest/technical/concurrency/).
- [Sapling MetaLog](https://sapling-scm.com/docs/dev/internals/metalog/)
  publishes its lightweight references after files, trees, and commits, so
  interrupted heavyweight writes remain invisible.
- Dolt uses [generational/online GC](https://www.dolthub.com/blog/2023-01-25-online-gc/)
  and now offers [incremental GC](https://www.dolthub.com/blog/2026-04-28-introducing-incremental-garbage-collection/).
  Its [Prolly-tree design](https://www.dolthub.com/docs/architecture/storage-engine/prolly-tree/)
  validates retaining Lix's existing hash-guided diff and structural sharing.

## Reproduce

```sh
cargo check -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends

# RocksDB: prepare one closed seed, copy it, then run acceptance.
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  setup rocksdb /tmp/checkpoint-rocks-seed 10000
cp -a /tmp/checkpoint-rocks-seed /tmp/checkpoint-rocks-run
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  run rocksdb /tmp/checkpoint-rocks-run 1000 10 5
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  surfaces rocksdb /tmp/checkpoint-rocks-run
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  stats rocksdb /tmp/checkpoint-rocks-run

# SlateDB: prepare an independent closed seed; fixtures are not portable
# between storage backends.
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  setup slatedb /tmp/checkpoint-slate-seed 10000
cp -a /tmp/checkpoint-slate-seed /tmp/checkpoint-slate-run
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  run slatedb /tmp/checkpoint-slate-run 1000 10 5
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  surfaces slatedb /tmp/checkpoint-slate-run
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  stats slatedb /tmp/checkpoint-slate-run

# Width sentinels from the completed backend-specific fixtures.
cp -a /tmp/checkpoint-rocks-run /tmp/checkpoint-rocks-width
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  run rocksdb /tmp/checkpoint-rocks-width 1 10000 1
cp -a /tmp/checkpoint-slate-run /tmp/checkpoint-slate-width
cargo bench -p lix_sdk --bench profile_checkpoint_scale \
  --no-default-features --features checkpoint_backends -- \
  run slatedb /tmp/checkpoint-slate-width 1 10000 1
```

All target directories must be absent before `setup` or `cp -a`. Historical
SQLite baseline numbers require the earlier SQLite harness at `23be0ad5`; its
fixtures are intentionally neither format-compatible nor acceptance inputs
for these backends.
