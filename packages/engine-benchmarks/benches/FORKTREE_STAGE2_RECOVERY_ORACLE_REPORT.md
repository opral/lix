# ForkTree Stage-2 crash/recovery acceptance oracle

Verdict: **GREEN for the test/model contract below.** This package adds no
production, Stage-2, adapter, persisted-format, or PR code.

## Immutable provenance

- Approved Stage-1: `138b55e1de90806c380ad27b2b349f4c66a1387f`, tree
  `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`.
- Accepted reader-lease/GC contract: `ac23754c8ba4a943e69da1304e371d8416456f1b`,
  tree `156656c53b0193f6090e62bf652454f80fe461ac`.
- Final public cursor/view contract: `e0c5378d7db69117866b8d7c260258d761e2b15c`,
  tree `36e74c821ba1ece4b515e7972a3354a41d265767`.

The oracle is a child of the accepted lease commit. It reuses that test-only
storage bridge and defines a separate authenticated state machine; it does not
wire or emulate a compatibility codec.

## Required authority and crash contract

1. Immutable typed objects are content-addressed and authenticated. Catalog,
   state, blob, receipt, and edge identity includes kind, generation, complete
   typed child edges, payload, and checksum.
2. One authenticated global authority is the sole selected root, commit epoch,
   selected generation, lease-generation allocator, and GC watermark.
   Recovery/checkpoint/child/upload selectors and reader leases are roots, not
   alternate epochs or publication authorities.
3. Staging objects does not publish them. Publication atomically CASes the exact
   raw global authority, prior selected root, and exact present/absent GC
   progress. It advances the global epoch and selected generation, installs the
   new root, and records the displaced root as recovery authority.
4. A crash before staging changes nothing. A crash after staging leaves only
   reclaimable unpublished objects. A crash after selector CAS exposes the
   complete new graph and exact recovery root after cold reopen.
5. Checkpoint/recovery restore validates the entire typed graph from one
   coherent read before CASing the global authority. Missing, malformed,
   mistyped, non-decreasing, or content-mismatched edges reject.
6. Cursor resume authenticates exact lease ID/generation, root, view ID, and
   position. Renewal rotates generation/view and invalidates the old cursor;
   release makes the current cursor fail with `ReadExpired`.
7. GC start CASes the raw global and persists only a rebuildable closure proof:
   fenced raw global, exact canonical root count/digest, minimum live lease
   generation, and exact live-lease count/digest. A cold reopen recomputes and
   matches this closure before continuing.
8. Publication-first makes stale GC start fail. GC-first permits publication
   only by consuming the exact progress row in the same authority CAS, making
   stale sweep fail. Complete graph validation precedes the first deletion.
9. Shared roots survive until the last recovery, checkpoint, child, upload, or
   lease reference is removed. Only then may a fenced cycle reclaim them.

There is no wall-clock/grace deletion rule. Lease expiry is exact logical epoch:
a lease is retained iff `valid_through_global_epoch >= gc_start_epoch`.
Expired rows are removed atomically at GC start under raw-global and exact-row
preconditions. Quiescent repositories may retain an expired row until another
publication or GC start; safety does not depend on clock or process survival.

## Oracle coverage

Both RocksDB and SlateDB run the same deterministic sequence:

- crash before immutable staging, after staging/before publication, after
  selector CAS, and after GC start/before completion;
- cold flush/drop/reopen at every crash boundary;
- recovery selector and checkpoint restore;
- reader lease acquisition, renewal, old-cursor rejection, release, and
  released-cursor rejection;
- publication-first/GC-first races and same-root two-writer CAS contention;
- open multipart receipt root, child branch, checkpoint, recovery root, and
  final-reference reclamation;
- abandoned unpublished and losing-writer object reclamation;
- torn/missing graph, typed-kind substitution, non-authoritative staging,
  malformed recovery selector, and independently corrupt catalog/state/blob/
  edge/receipt objects;
- corruption rejection before any sweep delete.

## Complexity

For `N` newly staged objects/bytes, `R` live reader leases, `K` other roots,
`V` reachable objects/edges, and `O` unreachable objects:

- immutable staging: `O(N)` writes and bytes;
- publication, selector mutation, lease mutation: `O(1)` coherent points plus
  one atomic commit;
- selector recovery/checkpoint restore validation: `O(V)` reads before an
  `O(1)` publication;
- GC/recovery: `O(R + K + V + O)` reads/work and `O(O)` deletion work;
- cursor authority validation: `O(1)` before output-sensitive traversal.

The executable oracle retains an `O(V)` exact visited set so it can compare and
diagnose the whole tiny deterministic graph. That is observer state, not an
accepted production implementation. Stage-2 remains bound by the accepted
lease contract: persisted rebuildable marks and paged inventory give transient
`O(R + K + frontier + page)` memory without a second root/index authority.

The perfect-elimination ceiling for safety work is zero: eliminating the raw
authority/progress CAS, complete root closure, or typed graph authentication
would make one of the covered crash/race/corruption cases unsafe.

## Exact dual-adapter evidence

All cells were below 20 minutes. The one-time release build completed in 16m16s
while actively compiling native RocksDB; the warm correction build completed in
16.15s. The oracle cells themselves completed in less than 30ms each.

| Backend | Wall | CPU | Allocated | Peak RSS | Logical gets / scans / commits | Disk | Physical object I/O |
|---|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 21.041ms | 15.875ms | 820,312 B | 22,112 KiB | 569 / 70 / 75 | 366,309 B | not exposed |
| SlateDB | 23.651ms | 11.945ms | 69,191,312 B | 84,800 KiB | 569 / 70 / 75 | 52,444 B | 795 reads / 301,891 B; 89 writes / 61,710 B; 5 deletes |

Both records report every named oracle as `pass`. RocksDB wrote 1,480 filesystem
blocks; SlateDB wrote 680. No final-reference or orphan leak remained.

Source gates: `cargo fmt --all -- --check` PASS; `git diff --check` PASS;
warnings-denied focused Clippy PASS in 4m19s.

## Runnable acceptance command

```text
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo bench -p lix_benchmarks --bench forktree_reader_lease_gc \
  --features storage-benches,slatedb --no-run

<isolated-target>/release/deps/forktree_reader_lease_gc-<hash> \
  stage2-recovery rocksdb
<isolated-target>/release/deps/forktree_reader_lease_gc-<hash> \
  stage2-recovery slatedb

cargo fmt --all -- --check
git diff --check ac23754c8ba4a943e69da1304e371d8416456f1b..HEAD
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  cargo clippy -p lix_benchmarks --bench forktree_reader_lease_gc \
  --features storage-benches,slatedb -- -D warnings
```

Locally accepted release binary before commit freeze:
`39cbb14abc0ea4a02928117f74aac89c35add04f9f94920c83b7b9c0268bc8e2`.
