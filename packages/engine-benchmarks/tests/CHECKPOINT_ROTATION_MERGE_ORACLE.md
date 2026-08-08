# Checkpoint rotation merge acceptance oracle

Status: test-only freeze on exact failing base. The local no-run cell reached
the 20-minute host boundary while compiling native dependencies, before the
test target was linked or executed. Do not count that boundary as a candidate
failure. Run the unchanged test on the correction head.

## Provenance

- Base: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`
- Base tree: `9a705d36392e88d8f5f363b2b23d373deec3321d`
- Base parents: `803d19ec0b67fb4b759aceab7ceb74650d9d894f` and
  `d005a4ac2f2d62322bb477c958092d76efc45c9f`
- Production changes: none

## Executable acceptance surface

`checkpoint_rotation_merge_oracle.rs` defines identical RocksDB and SlateDB
cases for:

1. Three rows, one ordinary history commit `H`, one compacting checkpoint `C`,
   a source branch created explicitly from pre-checkpoint `H`, and disjoint
   source/target edits. The authenticated merge base and merge receipt must be
   `H`; merge must succeed without `added/added` conflicts.
2. The resulting merge has exact target/source parent ordering, remains
   readable through undo, redo, 64 checkpoint/GC rotations, close, flush, and
   cold reopen, including retained history and the historical source branch.
3. A same-identity source/target edit still returns one
   `sameEntityChanged` conflict and moves neither branch.
4. Deleting the authenticated `H` commit record makes merge-base traversal
   fail closed after durable reopen.

Exact failing-base command:

```text
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks --test checkpoint_rotation_merge_oracle --features storage-benches,slatedb -- --nocapture --test-threads=1
```

Smallest tests may be run first with either exact filter:

```text
recovered_head_disjoint_merge_rocksdb
recovered_head_disjoint_merge_slatedb
```

## Required correction-head extension contract

The production correction's typed owner/test hook must additionally make the
following queue and bounded-retention cases executable without exposing raw
queue mutation APIs:

- The branch-side bridge binds old `H` to the semantically equivalent
  checkpoint `C`, never to a later diverged target `T`. A checkpoint must not
  acquire a permanent graph parent edge to `H`.
- After the historical branch is deleted and checkpoint/GC advances, `H` and
  its compacted interval become reclaimable while the surviving branch and
  checkpoint remain readable.
- With an authenticated pending queue batch, branch-first publication CASes
  the exact raw queue, publishes the bridge/root, and makes stale GC fail.
- GC-first consumption/reclamation makes stale branch publication fail closed
  with no branch. A retry may only succeed from a newly authenticated mapping.
- Malformed, mismatched, duplicate, or stale `H -> C` recovery mappings fail
  closed. Merge readers consume only the published graph bridge/root and never
  read recovery-ref or pending-queue maintenance state.
- Deleting the historical branch followed by final GC releases the final
  reference and reclaims `H`.

On exact `a12`, the one branch-keyed recovery-ref row is unconditionally
replaced by each checkpoint. The first checkpoint records `H -> C1`; after 64
empty rotations only `C63 -> C64` remains. Therefore the original relation
cannot be reconstructed from the current recovery-ref row and must be consumed
and authenticated before overwrite by the correction's bounded bridge owner.
