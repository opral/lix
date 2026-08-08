# ForkTree W5/R7 GC + reachability oracle

This is a test/report-only package anchored at corrected d6b lineage. It contains
no production implementation, no adapter behavior, no current-main benchmark,
and no PR mutation.

## Immutable lineage

    corrected base: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
    base parent:    1f742a382c755399b8a49ab536c4f6dc55fffdd8
    base tree:      641654079f60fcd1c9ff9ccbbd06d3edcabe4096

The d6b correction is the fail-closed missing-commit-record change. The 1f742
commit is retained as the red semantic-topology control. The static verifier
must be calibrated against both without modifying either tree.

## Artifacts

* forktree_w5_r7_gc_reachability_oracle.rs is a pure model test artifact.
* forktree_w5_r7_residue_verify.mjs scans production source only.
* This report is the command/coverage manifest.
* The test is intentionally independent of Lix production APIs so runtime
  qualification remains dormant until the target compiles cleanly.

The model checks exact 65-entry processing (64 plus suffix), blocked-head
one-token debt with no retry spin and release/cadence drain, publication-first
and GC-first epoch fencing, one pinned coherent view with automatic error
poisoning and fresh restart, open-upload/shared/final references, malformed/
cycle/missing-root fail-closed, and cold-reopen authority/queue recovery.

## Static calibration

    node scripts/forktree_w5_r7_residue_verify.mjs --root <checkout>

Expected result for both 1f742a382c755399b8a49ab536c4f6dc55fffdd8 and
d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768 is RED. Any production residue is
a blocker for the future hard cut; the verifier is not permitted to accept
legacy readers/writers merely because tests pass.

Calibration evidence for this immutable package:

    d6b RED count: 168
    1f742 RED count: 168
    d6b output SHA-256:    af5cb87c2e9a7d3d144a50ba018f5d87c336458732dd6756d2d312b8eb71eec6
    1f742 output SHA-256:  af5cb87c2e9a7d3d144a50ba018f5d87c336458732dd6756d2d312b8eb71eec6

The exact Cargo no-run command reached the d6b production crate and was
blocked by inherited unresolved branch/live-state/changelog/storage symbols;
this is a baseline compile blocker, not an oracle diagnostic. No adapter or
Cargo runtime cell was run. The pure model source independently compiled with
rustc --edition=2021 --test -D warnings; executable SHA-256:

    2ad6a3ac8868486357b5bba10b16b0633c8238d89c19c4b1b5eed06abd0c1765

That standalone binary was not executed because the package-level no-run gate
was not green. Runtime remains dormant until a compile-green candidate exists.

## Compile/runtime order

First compile only:

    timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
      cargo test -p lix_benchmarks \
      --test forktree_w5_r7_gc_reachability_oracle --no-run

Only after this no-run is green may the pure model execute:

    timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
      cargo test -p lix_benchmarks \
      --test forktree_w5_r7_gc_reachability_oracle \
      -- --nocapture --test-threads=1

Adapter qualification is dormant until the first compile-green candidate
integrates the sealed typed facade. The exact future adapter command shape is:

    FORKTREE_W5_R7_BACKEND=memory|rocksdb|slatedb
    cargo test -p lix_benchmarks --test forktree_stage2_gc_publication_acceptance \
      --features storage-benches,rocksdb,slatedb \
      forktree_stage2_gc_publication_acceptance -- --exact --nocapture

That future run must use the same plan and assertions on Memory, RocksDB, and
SlateDB, including cold reopen, same-plan race ordering, reader pins, uploads,
shared/final roots, checkpoint bridge, corruption, and 65-entry debt behavior.

## Required residue and source gates

    cargo fmt --all -- --check
    git diff --check <exact-base>..<exact-head>
    node scripts/forktree_w5_r7_residue_verify.mjs --root <exact-root>
    cargo clippy -p lix_benchmarks \
      --test forktree_w5_r7_gc_reachability_oracle -- -D warnings

No old GC/reachability/tree-sweep space, namespace, codec, writer, reader,
fallback, migration, second authority, or raw StorageSpace forge may survive
in production. Explicit rejection fixtures may name legacy bytes only outside
the scanned production roots.

## Freeze rule

The final handoff records exact head/tree/parent/diff/patch hashes, source/test
hashes, static calibration logs, no-run and runtime logs, and this report hash.
A static RED control is expected for 1f742 and d6b. Runtime logs are not claimed
until the compile gate is green.
