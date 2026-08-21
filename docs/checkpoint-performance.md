---
description: Reproduce and qualify constant-time foreground checkpoint publication.
---

# Checkpoint foreground performance

Checkpoint publication is a metadata operation. Its foreground work is bounded
independently of live rows, changed rows, commits since the prior checkpoint,
retained checkpoint depth, branch count, and checkpoint-GC debt. Root diffing,
history reconstruction, tombstone retirement, and repository GC are not part of
the publication timer.

This report records the qualification performed on Linux on 2026-08-21 at
`05e610afb` plus the checkpoint O(1) changes. Times are wall-clock measurements
from an optimized build on the same host. Storage operation/payload ceilings
are portable regression gates; allocator counts cover the checkpoint's engine
task and deliberately exclude adapter worker tasks.

## Reproduction

Build the two-adapter harness:

```sh
cargo build --release --manifest-path packages/e2e/Cargo.toml \
  --bench profile_checkpoint_scale --features storage-benches,slatedb
```

Cargo prints the executable under `target/release/deps`. The equivalent Cargo
Bench commands below prepare and run a fixture:

```sh
cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench profile_checkpoint_scale --features storage-benches,slatedb -- \
  setup rocksdb /tmp/checkpoint-rocks 10000 0

cargo bench --manifest-path packages/e2e/Cargo.toml \
  --bench profile_checkpoint_scale --features storage-benches,slatedb -- \
  run rocksdb /tmp/checkpoint-rocks 1000 1 1
```

The setup arguments are `files branches`. The run arguments are
`checkpoints changed-files-per-checkpoint auto-commits-per-checkpoint`. Copy a
closed setup directory before each run to compare shapes from the same seed.
Use `slatedb` in place of `rocksdb` for the other shipping adapter.

The harness asserts every checkpoint against these foreground ceilings:

| Resource | Hard maximum |
| :-- | --: |
| Storage read views | 8 |
| Point-read batches / keys | 80 / 80 |
| Scan starts / pages / rows | 4 / 4 / 48 |
| Write transactions / adapter calls | 1 / 20 |
| Written records / bytes | 32 / 65,536 |
| Allocations / allocated bytes | 24,576 / 3 MiB |
| Latency p99 / max | 25 ms / 100 ms |
| First-to-last 100-checkpoint p95 drift | at most 2x + 2 ms |
| Interval-write p99 / max | 100 ms / 500 ms |

Storage counters are Tokio task-local. Allocator attribution consults that same
task-local scope, so SlateDB compaction on the same executor thread is not
misclassified as foreground allocation. Backend worker allocations are outside
that scope, so the allocation ceiling is an engine-task regression gate rather
than a claim about whole-process allocation. After every measured checkpoint,
the harness yields once and immediately times a second no-op checkpoint before
any ordinary write can absorb maintenance contention. Both latency
distributions must pass the same ceiling. GC has separate trace fields for root
discovery, changelog work, tracked-root staging, swept records, and total time.

## Before and after

Before the hard cut, the initial RocksDB checkpoint materialized the whole
working diff and scaled with repository width:

| Live files | Before | After |
| --: | --: | --: |
| 1,000 | 11.046 ms | 1.206 ms |
| 5,000 | 59.966 ms | bounded by the same census |
| 10,000 | 131.749 ms | 1.092 ms |
| 20,000 | 314.429 ms | 1.092 ms |

The after-census at both 1,000 and 20,000 RocksDB files was exactly 5 read
views, 43 point batches, 46 keys, one scan page visiting 14 fixed metadata
rows, one write transaction, and 25 written records. SlateDB produced the same
storage census; its 1,000/10,000/20,000-file initial checkpoints were
1.603/1.359/1.256 ms.

Sustained high-cadence publication previously inherited repository-GC gate
holds. A 10,000-file RocksDB run reached 540.230 ms maximum checkpoint latency
while a root scan took 537.688 ms. After decoupling:

| Backend and shape | Main p50 | Main p95 | Main p99 / max | Post-yield p99 / max | Write p99 / max |
| :-- | --: | --: | --: | --: | --: |
| RocksDB, 10k files, 1k checkpoints | 0.472 ms | 0.563 ms | 0.671 / 1.482 ms | 0.577 / 0.726 ms | 0.808 / 2.188 ms |
| SlateDB, 10k files, 1k checkpoints | 0.632 ms | 0.923 ms | 1.058 / 1.674 ms | 1.129 / 1.593 ms | 1.142 / 3.068 ms |

The first/last 100-checkpoint p95 values were 0.564/0.628 ms on RocksDB and
0.728/0.712 ms on SlateDB. The RocksDB run observed a 364.920 ms background
root-discovery pass while main checkpoint p99/max stayed at 0.671/1.482 ms and
the direct post-yield probe stayed at 0.577/0.726 ms. The GC scans are
intentionally allowed to scale; they no longer extend the foreground
distribution or monopolize writer admission.

## Independent scale dimensions

One 1,000-file seed was copied for three RocksDB and three SlateDB runs:

| Changed rows | Commits since checkpoint | RocksDB checkpoint | SlateDB checkpoint | Foreground storage census |
| --: | --: | --: | --: | :-- |
| 1 | 1 | 1.282 ms | 1.473 ms | 46 batches, 50 keys, 1 page/14 rows, 25 records |
| 1,000 | 1 | 1.922 ms | 1.793 ms | identical |
| 1,000 | 1,000 | 1.107 ms | 1.332 ms | identical |

Branch fixtures plateau rather than scale. Moving from 1 to 1,000 retained
branches kept the branch-lifecycle path at four pages / 44 rows and one write;
point keys changed from 71 to 75 and written records from 25 to 26 because the
fixture crosses fixed empty/non-empty topology cases, not because it visits
branches.

A 5,000-checkpoint RocksDB run exercised retained history and accumulated GC
debt: p99 was 0.682 ms, max was 1.338 ms, and first/last p95 was 0.464/0.603 ms.
The maximum foreground census was 52 point batches, 58 keys, one page/14 rows,
28 written records, and 23,414 bytes. The publication payload is bounded by the
fixed packed-segment targets; the regression ceiling is 64 KiB.

## Profile

The CPU profile can be reproduced with:

```sh
perf record -g -o /tmp/checkpoint.data -- \
  target/release/deps/profile_checkpoint_scale-* \
  run rocksdb /tmp/checkpoint-rocks 1000 1 1
perf report --stdio --no-children -i /tmp/checkpoint.data
```

The sampled hot functions were BLAKE3 compression, tracked-state tree diffing,
and RocksDB `MultiGet`; those samples belonged to the `lix-checkpoint-gc`
background worker. `perf stat` for the full run, including the fixed five-second
maintenance settle, reported 2.592 CPU-seconds, 14.125 billion cycles, 29.698
billion instructions, and 675 context switches. Foreground attribution is the
per-checkpoint census above rather than this process-wide profile.

## Architectural hard cuts

- A checkpoint commit publishes a complete-state fence that structurally
  aliases the captured head root. Its semantic first parent is the previous
  checkpoint; its physical source is the captured head. Publication never
  materializes or copies the interval diff.
- Ordinary writes always publish persistent root authority. Checkpointing can
  therefore never inherit deferred root-rebuild debt from a wide direct or
  replacement write.
- Packed current-state bases retain their owning checkpoint. Epoch rotation is
  metadata-only; stale bases are interpreted against their owner rather than
  scanned or retired during publication.
- History and GC synthesize a complete-state fence delta lazily by diffing the
  prior checkpoint root and captured root. Sync transfers the same state as a
  bounded authenticated alias containing the physical source commit and exact
  root ID; it does not expand an interval into one oversized wire item. GC
  retains the physical change owners needed for lazy reconstruction.
- Checkpoint publication only records durable GC debt and coalesces a background
  task. Repository-scale planning happens without the foreground session gate;
  scale-dependent adapter batch construction happens before the serialized
  writer lane, foreground publications have writer-admission priority, and the
  final maintenance write uses storage preconditions. Conflict replanning is
  capped at three exponentially delayed attempts so sustained writes cannot
  create an unbounded full-repository retry loop.
- Tombstone compaction is background repository maintenance. The foreground
  path rotates a bounded epoch and does not enumerate tombstones. Maintenance
  prefilters compactable tombstones and admits at most 32 rows per pass, so a
  permanently ineligible prefix cannot starve later candidates or build an
  oversized put batch.
- Repository retirement publications admit at most 4,000 mutations and 3 MiB
  of values. Wide commits first drain owner-local locators, then persist a
  restartable child-delete intent inventory while their trees are intact.
  Later passes revalidate shared content hashes against the current live
  closure, drain target/intent pairs under the cap, and delete manifest
  authority last.

These cuts preserve the checkpoint as the working-diff and undo floor while
keeping history, branch, sync, crash/reopen, and adapter behavior intact.
