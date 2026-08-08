# ForkTree W5/R7 GC + reachability oracle

This is a new test/report-only direct successor to the blocked immutable
oracle head `409d14dbdc9e91b9cc6e2bd8c7bca4b487671113`. It contains no
production implementation, adapter behavior, current-main benchmark, or PR
mutation. The prior 409 ref is preserved unchanged; this package does not
rewrite or replace it.

## Immutable lineage

    correction parent/base: 409d14dbdc9e91b9cc6e2bd8c7bca4b487671113
    parent tree:            218700eecc1808611a08b55768b4ac31ba9f0c82
    parent parent:          b8098280eeb6c88820c8b3c2017d19caaff76480
    parent full-index diff: bdcf3cb567a8633f01b7a0252399926820019090a54ca9f3ff310d806cddbe00
    parent stable patch-id: 8799b698d7021b3f41b3638c13ec33deb5940efd
    prior direct lineage:   6487170dfa11b24411dbbd73e3c003439072df09
    prior parent tree:      94eefb7de3260a8c8a3217805a5372cb8670157c
    ancestry correction:    d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
    ancestry control:       1f742a382c755399b8a49ab536c4f6dc55fffdd8

The d6b correction is retained as ancestry evidence for the fail-closed
missing-commit-record change. The 1f742 commit remains the red
semantic-topology control. Neither tree is modified.

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
Reachability is the full authenticated transitive closure over *every live selector*:
`commit_gc`, queue-page processing, and direct removal all compute the full
closure before mutation. H remains retained through selected S/C ancestry,
including every ancestor in an H/S/C checkpoint chain, and an unselected
object is reclaimed without making the root graph appear fully reachable. The
explicit H/S/C test drops selectors in dependency order and checks each exact
release. A still-live checkpoint selector continues protecting its closure
after the originating view closes; only retirement of that selector permits
the old serving object to be collected.

The corruption suite performs concrete state mutations and requires failure
for missing objects, wrong kinds, empty payloads, duplicate edges, non-
chronological generations, cycles, and substituted object identities. Root
object IDs and authentication tags include kind, payload, ordered parents, and
generation.

The reader model authenticates the selected root's existence, kind, identity,
and reachable parent graph before returning one operation-owned coherent read
or recording a pin. It pins the complete authenticated closure, not only the
selected root. Pins are owner-scoped and view-scoped: each root/object pin is
keyed by the pair `(owner, view_id)`; poisoning or closing one view cannot
release another owner's root or an H/S/C ancestor. Cross-owner collision and
unpin controls are negative tests and
must fail without partial pin mutation. The publication-then-GC interleave
cannot retire the old serving object while an active checkpoint read pins it;
the still-live checkpoint selector also blocks retirement after view close.
Page failure poisons the view; a stale cursor expires; a fresh view accepts
only an authenticated cursor whose restart is represented as
`Excluded(last_authenticated_key)` and whose proof includes its owner/view
identity.

Cold reopen serializes and strictly parses owner/fence, queue counters,
reclamation counters, object count/digest, and selector digest. Malformed,
truncated, non-numeric, or graph/digest/state-tag mismatched persisted state
fails closed against the exact authenticated object and selector graph.

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

The legacy production-residue calibration remains RED 168 with the inherited
stdout SHA above; that RED is intentionally preserved as a hard-cut control.
The new package-local structural gate is separate and GREEN only when the
full-closure and owner/view-pinning obligations are present:

    node scripts/forktree_w5_r7_gc_structure_verify.mjs --root <checkout>

The captured structural-gate output SHA-256 is
`fe94eec334cb5255395b0a305add6912c82eca7e6a0d9e5d2e33ead6f8ff40f3`.
The captured legacy-residue output SHA-256 is the inherited
`af5cb87c2e9a7d3d144a50ba018f5d87c336458732dd6756d2d312b8eb71eec6` and exits
1 with RED count 168, as required for the pre-landing hard-cut control.

The bounded standalone model compile passed with `-D warnings` (compile log
SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
The model test binary SHA-256 is
`4837c40a89341bff40632aafaa0ac6cb67cef9b2be36c63c7b51afc6a55e5193`; its
serial test log SHA-256 is
`310c74b87e0cab8757d60952d248f031fa733ff3f13e8e51ce88fc3c301af9ac`.
It reports 13 passed, 0 failed, including the checkpoint-after-view-close
and cross-owner/view-ID collision controls. No adapter runtime or production
build was run.

The standalone model is source-checked with:

    rustc --edition=2021 --test -D warnings \
      packages/engine-benchmarks/tests/forktree_w5_r7_gc_reachability_oracle.rs \
      -o <isolated-model-binary>

The package-level Cargo no-run gate and future adapter runs remain dormant
until a compile-green production candidate exists.

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

For reproducible freeze identities, the ordinary patch is captured with
`git diff --binary <exact-base>..<exact-head>`; the lossless full-index patch
is captured with `git diff --full-index --binary <exact-base>..<exact-head>`.
