# ForkTree Stage 2 GC/publication acceptance oracle

Status: test-only, frozen against non-runnable milestone 2. This package does not edit production, ForkTree, storage, cursor, SQL, adapter, or GC source.

## Provenance

- Stage 2 milestone input: `cbe48835f6f07a21e0babf1ba16652a0c6b8a214`, tree `36ffe0ff867cd31bf52263675de2d16fc54e9b4f`.
- Authority-deletion oracle: `1dbbf3d206540d36f5912eab8372a42819778b47`.
- Bounded-GC owner contract: `73f191fbb960bdb9bb647f63dc909fba606a5c40`.
- One-view publication correction: `ee402a098a991f7e91eb9c62e2cefe960f8e547e`.
- Read-return boundary discriminator: `89c73a24b97ce8dedee5e6c9a85e67c481b29090`.
- Public version-control control: `ae3b9bf13676a79e01b25e5d1a2cc624517326e9`.
- Landed streaming cursor contract: `StorageRead::begin_scan -> ScanCursor::next_page`; fresh views restart only at `Excluded(last_authenticated_key)`.

## Sealed facade contract

The test imports exactly one absent milestone-2 seam:

```text
lix::storage_bench::stage2_gc_publication_acceptance
```

The first runnable Stage 2 owner supplies this module only under `storage-benches`. It contains `AcceptancePlan`, `AcceptanceCheck`, opaque equality-only authority tokens, `AcceptanceReport`, and generic `run_acceptance(storage, reopen, plan)`. It must remain a typed test facade over the real production owner. It may not expose or accept `StorageSpace`, selector keys, codec bytes, raw ObjectIds, maintenance roots, or direct mutation callbacks.

`AcceptancePlan::focused()` creates the smallest deterministic graph that:

- places more than 64 pending traversal entries in the persisted queue;
- exceeds one persisted mark-pack bound, proving a split and bounded packs;
- includes live shared references, an open upload, dead staged/unpublished objects, and a final releasable reference;
- advances through at least two committed GC pages and two cold reopens.

The external test receives semantic checks, opaque authority snapshots, and counts only. Equality of `pinned` and `prepared` proves exact byte transport from one retained `StorageRead` without revealing those bytes. The facade must count publisher `begin_read` calls at the real acquisition boundary; the required count is exactly one.

## Required lifecycle

One RocksDB or SlateDB cell performs, in this order:

1. Seed two owners sharing one reachable subtree, one open upload, and deterministic dead staged objects.
2. Open one pinned read and prepare a publication from its exact global epoch, GC progress value/absence, owner selector, and full next-root trace.
3. Start bounded GC, persist mark/queue packs, commit a deletion page, flush/drop/cold reopen, and resume from authenticated progress.
4. Prove the held cursor is poisoned after a real page/decode/backend error. A fresh read restarts exclusively after the last authenticated delivered key; the historical view is not promised after read drop.
5. Race publication first against GC and GC first against publication. The loser must fail its exact epoch/progress/selector CAS. A same-owner stale writer must fail; there is no retry hidden by this oracle.
6. Prove the open upload and shared subtree survive partial release. Delete the final semantic reference and reclaim exactly the remaining dead objects.
7. Reclaim abandoned unpublished objects without deleting live data.
8. Cold reopen and fail closed for malformed selector, missing graph, malformed mark pack, and malformed queue pack. Corrupt maintenance state disables deletion; it never authorizes reclamation or becomes absence.

## Required check set

The result must contain exactly 20 checks; extra unreviewed behavior is not silently accepted:

```text
OneStorageReadPin
CursorErrorPoisonsView
FreshExclusiveRestart
ExactGlobalEpochCas
ExactProgressCas
ExactOwnerSelectorCas
PersistedMarkPacksBounded
QueueExceedsSixtyFour
CrashReopenResumes
AbandonedUnpublishedObjectsReclaimed
OpenUploadRetained
SharedReferenceRetained
FinalReferenceReclaimed
MalformedSelectorFailsClosed
MissingGraphFailsClosed
MalformedMarkPackFailsClosed
MalformedQueuePackFailsClosed
PublicationFirstFencesGc
GcFirstFencesPublication
SameOwnerStaleWriterRejected
```

## Authority and compatibility guards

- One immutable object space and one selector plane remain authoritative.
- GC progress and bounded mark/queue packs are disposable maintenance state; they cannot authorize reads, writes, roots, or deletion.
- Every deletion page exact-CASes the raw global epoch plus exact progress bytes captured from one coherent view. Owner publication additionally exact-CASes its selected owner bytes.
- No persisted reader lease, second read, retry, compatibility decoder, legacy space, migration, raw-space hook, ObjectId escape, or dual path is permitted.
- Missing/malformed selectors, graph edges, packs, cursors, domains, hashes, lengths, chronology, or progress fences fail closed.
- The harness intentionally does not reproduce ForkTree encodings or GC algorithms. All fault placement is selected by typed semantic `AcceptanceCheck` scenarios inside the sealed facade.

## Compile-red boundary

Milestone cbe is intentionally non-runnable and has no sealed acceptance module. This harness references no deleted owner or private ForkTree symbol; its sole new Lix API dependency is the module above. Therefore the harness remains red until the real owner exposes the cfg-only facade and runtime. Restoring a legacy module to make it compile is a rejection.

## Focused commands

Use a distinct target per immutable head. Each cell is capped at 20 minutes.

```sh
timeout 20m env CARGO_TARGET_DIR=<target> CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,slatedb --no-run

timeout 20m env FORKTREE_STAGE2_BACKEND=rocksdb \
  CARGO_TARGET_DIR=<target> CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,slatedb \
  forktree_stage2_gc_publication_acceptance -- --exact --nocapture --test-threads=1

timeout 20m env FORKTREE_STAGE2_BACKEND=slatedb \
  CARGO_TARGET_DIR=<target> CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks \
  --test forktree_stage2_gc_publication_acceptance \
  --features storage-benches,slatedb \
  forktree_stage2_gc_publication_acceptance -- --exact --nocapture --test-threads=1
```

Admission order is build, RocksDB, then SlateDB. Do not run SlateDB or any wider matrix until the focused RocksDB cell is green.

## Frozen pre-commit identities

- Oracle source SHA-256: `a43980d3de613d5800478e6c7e8a12c73a4d1833f53ec213f3fb26f317aec1c7`; Git blob `2beae2395ae59c061276c2a2b5f6932a50e3975a`.
- Cargo manifest SHA-256: `852637a2cfba650a88feaa2d184e62ff032c84f3fe940b045080ea2c224ca73d`; Git blob `922b8fc464bb5528e9c9870e082717facb5e4e67`.
- `cargo fmt --all -- --check`: pass.
- `git diff --check`: pass.
- Forbidden source import scan (`lix::forktree`, `StorageSpace`, `ObjectId`, raw object/selector space, `ScanPlan`, `ScanOptions`): zero.
