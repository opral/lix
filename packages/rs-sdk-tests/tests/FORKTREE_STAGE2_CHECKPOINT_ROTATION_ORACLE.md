# ForkTree Stage2 checkpoint-rotation acceptance oracle

Status: frozen test-only successor. No production source is changed. The source
is intentionally compile-red on `a12` and the current pre-Stage2 branch solely
because these accepted SPI symbols do not exist yet:

```text
lix::integration::AcceptancePhysicalLayout
OpenLixBuilder::with_acceptance_physical_layout
```

The closed acceptance selector is the same one used by the frozen SQL and
version-control oracles. It must select exactly one physical owner before open;
it is not a compatibility reader, migration, fallback, or dual writer.

## Public-only contract

The oracle imports only public Lix/session/SQL APIs plus RocksDB and SlateDB.
It does not import or construct a storage space, queue key, codec, ObjectId,
raw mutation, storage adapter, GC plan, recovery-ref record, or ForkTree owner
type. Stage2 therefore satisfies the same semantics through its sole
selector/object authority.

Each adapter runs both closed layouts and requires identical observable output:

1. Exactly three tracked rows, one ordinary history commit `H`, one compacting
   checkpoint `C`, and exactly 64 checkpoint/recovery/GC rotations.
2. Create a historical source branch from pre-checkpoint `H`; make disjoint
   source/target edits; preview and merge with exact base `H` and no
   `added/added` conflict.
3. Authenticate source, target, and two-parent merge chronology. Checkpoint `C`
   must not permanently parent `H`.
4. Undo/redo, exact historical reads, flush, cold reopen, and retained source
   branch remain byte-equivalent.
5. A true same-identity source/target edit still previews and returns one
   `sameEntityChanged` conflict.
6. Creating a branch from a nonexistent parent fails closed and publishes no
   branch.
7. In a separate final-reference fixture, branch `H` after 64 rotations while
   main has already diverged to `T`, then delete the historical branch and
   advance another 64 rotations. `H` and its superseded interval must be
   reclaimed; checkpoint `C`, current branch state, and cold reopen remain
   readable. This rejects a permanent `C -> H` ancestry edge and a bridge to
   later `T`.

Queue publication/GC races remain owner-local invariant tests. This public
oracle deliberately cannot read the pending queue or recovery mapping: merge
and history readers may consume only published graph/selector objects.

## Exact commands on the first runnable Stage2 head

```text
RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
cargo test -p lix_tests --test forktree_stage2_checkpoint_rotation \
  forktree_stage2_checkpoint_rotation_rocksdb -- --exact --nocapture --test-threads=1

RUST_MIN_STACK=8388608 CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
cargo test -p lix_tests --test forktree_stage2_checkpoint_rotation \
  forktree_stage2_checkpoint_rotation_slatedb -- --exact --nocapture --test-threads=1
```

No local build was run for this successor. Acceptance begins with fmt,
`git diff --check`, and source-residue checks; compilation is deferred to the
immutable Stage2 head that provides the closed SPI.
