# ForkTree W5/R7 GC + reachability oracle

This is a test/report-only correction successor to immutable oracle head
6487170dfa11b24411dbbd73e3c003439072df09. It contains no production
implementation, adapter behavior, current-main benchmark, or PR mutation.

## Immutable lineage

    correction parent: 6487170dfa11b24411dbbd73e3c003439072df09
    corrected base:    d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
    base parent:       1f742a382c755399b8a49ab536c4f6dc55fffdd8
    base tree:         641654079f60fcd1c9ff9ccbbd06d3edcabe4096

The d6b correction is the fail-closed missing-commit-record change. The 1f742
commit remains the red semantic-topology control. Neither tree is modified.

## Model authority and coverage

The model has exactly two logical planes: `OBJECT_SPACE` and
`SELECTOR_SPACE`. `RootGraph` owns the authenticated object map and all root
selectors; no queue, cache, lease, marker, or alternate selector authority is
introduced.

Every publication and GC plan captures an owner-bound fence containing the
owner identity, epoch, progress, and selector generation. Commit checks the
entire fence and owner before mutating the object or selector plane. The
publication-first and GC-first tests prepare both plans at the same fence and
then commit them in opposite orders, proving the first operation wins and the
stale second CAS fails.

The graph explicitly models H/S/C chronology and serving separation: history
objects have ordered first-parent generations, serving points to history, and
checkpoint points to serving. The selector plane also covers branch control,
upload, recovery alias, undo, plugin registry, and final-reference roots.
Shared branch/upload roots survive until both selectors release them; final
references survive independently until their selector is released.

The corruption suite performs concrete state mutations and requires failure
for missing objects, wrong kinds, empty payloads, duplicate edges, non-
chronological generations, cycles, and substituted object identities. Root
object IDs and authentication tags include kind, payload, ordered parents, and
generation.

The reader model captures one operation-owned coherent read containing the
exact fence and selector snapshot. Page failure poisons the view and releases
its pins. A stale cursor expires; a fresh view accepts only an authenticated
cursor whose restart is represented as `Excluded(last_authenticated_key)`.

Cold reopen serializes and strictly parses owner/fence, queue counters,
reclamation counters, object count/digest, and selector digest. Malformed,
truncated, and non-numeric persisted state fails closed.

The 65-entry queue test proves a 64-entry page followed by the suffix, while
the blocked-head test proves one debt token, no internal retry spin, and drain
only after explicit release.

## Static calibration

    node scripts/forktree_w5_r7_residue_verify.mjs --root <checkout>

Expected result for both 1f742a382c755399b8a49ab536c4f6dc55fffdd8 and
d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768 is RED. Any production residue is
a blocker for the future hard cut; the verifier is not permitted to accept
legacy readers or writers merely because tests pass.

The prior immutable calibration was RED count 168 for both controls, with
stdout SHA-256
`af5cb87c2e9a7d3d144a50ba018f5d87c336458732dd6756d2d312b8eb71eec6`.

The standalone model is source-checked with:

    rustc --edition=2021 --test -D warnings \
      packages/engine-benchmarks/tests/forktree_w5_r7_gc_reachability_oracle.rs \
      -o <isolated-model-binary>

No model binary or adapter runtime result is claimed by this package. The
package-level Cargo no-run gate and future adapter runs remain dormant until a
compile-green production candidate exists.

## Compile/runtime order

First compile only:

    timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
      cargo test -p lix_benchmarks \
      --test forktree_w5_r7_gc_reachability_oracle --no-run

Only after that gate is green may the model execute:

    timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
      cargo test -p lix_benchmarks \
      --test forktree_w5_r7_gc_reachability_oracle \
      -- --nocapture --test-threads=1

Adapter qualification must use the identical plan/assertions in this order:
Memory, RocksDB, then SlateDB. It must include cold reopen, both CAS race
orders, one coherent read, cursor poison/restart, all root classes, malformed
graph/persisted state, 65-entry drain, and shared/final reclamation.

    FORKTREE_W5_R7_BACKEND=memory|rocksdb|slatedb \
      cargo test -p lix_benchmarks \
      --test forktree_stage2_gc_publication_acceptance \
      --features storage-benches,rocksdb,slatedb \
      forktree_stage2_gc_publication_acceptance -- --exact --nocapture

## Required source gates

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

The final handoff records exact head/tree/parent/diff/patch hashes, changed-file
hashes, static calibration logs, and this report hash. Runtime logs are not
claimed until the compile gate is green.
