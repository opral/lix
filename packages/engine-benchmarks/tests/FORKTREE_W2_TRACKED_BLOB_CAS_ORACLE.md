# ForkTree W2 tracked-state + Blob/CAS adapter acceptance oracle

Status: immutable TEST/REPORT-ONLY contract. This package follows the accepted
current-state reader and Cut B filesystem/plugin reader contracts, and precedes
the writer/publication cuts. It does not change production code, W3-W5
implementation, storage format, or current-main qualification.

## Immutable prerequisites and anchor

    anchor: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
    anchor_tree: 641654079f60fcd1c9ff9ccbbd06d3edcabe4096
    anchor_parent/red control: 1f742a382c755399b8a49ab536c4f6dc55fffdd8
    current-state reader owner: 9f3c703e953440cde1d60b1511467c4337648c8f
    current-state reader acceptance: 8b0cf91387ffc86851b99029bdd8942938ba2be6
    Cut B reader acceptance: e92ea2e505ee3d96abbb529dbaedb23d4908ff42
    Cut B tree: 0d0797c024706beb1510cb2f0f88f8414a9a0c96

The eventual W2 candidate is acceptable only after the reader prerequisites
are source-qualified. This oracle does not make a compiler-red prerequisite
runnable and does not restore old readers.

## Sole authority and call graph

All W2 reads begin from one retained CoherentView owned by one operation. The
view authenticates the selected state root, branch/global overlay roots,
commit/diff roots and BlobId object root. An ObjectId is domain-tagged and a
BlobId is a distinct semantic identity; neither can be substituted for the
other. BlobRef is bound to the view id, view owner, BlobId, canonical manifest,
total length, ordered chunk identities, lengths and digests. No caller can
provide a storage space, raw key, manifest/chunk row, detached root, or second
read handle.

Canonical paths:

    state point:
      CoherentView -> state-root point lookup -> typed row decode -> overlay
      precedence -> one result/NULL/tombstone

    state range:
      CoherentView -> ordered leaf traversal -> branch/global merge -> bounds,
      tombstone filtering and deterministic key order -> materialized rows

    historical diff:
      CoherentView -> authenticated before/after roots and commit identities ->
      ordered identity comparison -> added/removed/modified rows

    materialization:
      CoherentView -> validated ordered rows -> filesystem/plugin semantic
      materializer; no write, cache, or alternate tree

    BlobRef full:
      CoherentView -> BlobId -> canonical manifest/chunk identity validation ->
      authenticated payload -> complete bytes

    BlobRef range:
      CoherentView -> same BlobId/ObjectId and metadata validation -> bounded
      intersecting chunk ranges; no full-payload read or copy

All six paths share the same view/object authority. Reads are side-effect free:
selector, epoch, receipt, progress, root and write-count digests are unchanged.

## Semantics and discriminators

The exact pure-model cases are:

    w2_point_range_null_tombstone_and_order_contract
    w2_diff_and_materialization_preserve_identity
    w2_65_rows_collapse_to_one_canonical_root
    w2_blobref_full_and_range_use_one_object_authority
    w2_same_size_manifest_substitution_fails_closed
    w2_corruption_cold_reopen_and_zero_writes
    w2_cross_view_object_pairing_rejected
    w2_persisted_flush_drop_reopen_reauthenticates_rows_manifest_and_chunks
    w2_corrupt_rows_reject_before_partial_materialization_and_reads_do_not_write

Required observations:

* branch rows shadow global rows; NULL is a value, tombstone is distinct, and
  ordered ranges preserve canonical key order with explicit tombstone mode;
* diff identity includes schema, file identity, EntityPk, ChangeId and CommitId;
  equal EntityPk values in different files remain distinct;
* 65 ordered rows collapse into one canonical root without duplicate or
  out-of-order keys;
* full and range BlobRef reads validate the same typed ObjectId, semantic BlobId,
  manifest, ordered chunk identities, lengths and digests;
* bounded ranges validate manifest/chunk identity before selecting bytes and
  count only intersecting payload bytes; a full payload read is never hidden;
* same-size manifest/chunk substitution, duplicate/reordered identities,
  missing/malformed/noncanonical/wrong-kind/missing-child/wrong-digest and
  invalid-range state/blob data fail closed;
* a second view cannot be paired with the first view's state/object authority,
  even when root and epoch are unchanged;
* persisted flush/drop/reopen reauthenticates state-row identity, manifest and
  every chunk before returning a root; malformed, missing and substituted data
  fail closed;
* point reads increment point counters but not scan counters; all read paths
  preserve durable write/commit counters and reject partial output.

No read may repair, rewrite, repack, update presence, mutate a selector, or
silently fall back to an old tracked-state or binary-CAS owner.

## Required hard cut

The production successor must delete, not wrap or dual-read, the old owners:

    tracked_state/codec.rs
    tracked_state/storage.rs
    tracked_state/tree.rs
    binary_cas/kv.rs
    binary_cas/codec.rs
    binary_cas/chunking.rs

It must remove every legacy reader/writer/codec/space/namespace for those
owners, including TrackedStateStoreReader, TrackedStateScanRequest and related
TrackedHead/BranchHead acquisition; BinaryCasContext, BinaryCasManifest,
BinaryCasChunkView, BINARY_CAS_* spaces, manifest/chunk/presence rows, and raw
StorageSpace forging. No migration, fallback, compatibility decoder, dual
writer, persisted cache/index, payload copy, or second ObjectId/BlobId authority
is allowed. Negative fixtures may mention old names only outside production
readers/writers and must prove rejection.

## Residue/source verifier

Run without compiling or mutating:

    node scripts/forktree_w2_tracked_blob_cas_residue_verify.mjs \
      --root <checkout> --base <exact-base> --target <exact-candidate>

    node scripts/forktree_w2_tracked_blob_cas_residue_verify.mjs \
      --root <checkout> --self-test

    python3 scripts/forktree_w2_compile_negative_fixtures.py --root <checkout>

The verifier checks exact ancestry, rejects production changes outside the W2
path allowlist, scans the complete `packages/lix/src` closure, rejects legacy
files/symbols/namespaces, and requires CoherentView, ObjectId, BlobId and
BlobRef owner symbols. Its structural fixtures prove a positive retained-reader
call and reject fresh views, raw stores and mismatched reader arguments. It is
expected RED on the d6b/e92 predecessor because the old owners remain. A
future candidate must pass with zero findings; a source-only pass never grants
runtime acceptance.

Calibration on the immutable e92/d6b production tree remains RED with 113
source-contract findings; the deterministic replay log SHA-256 for this
corrected verifier is recorded in the correction report. The corrected model is
warnings-denied and its standalone runtime passes all 9 tests. The package-level
Cargo target is intentionally not claimed until the prerequisite reader lineage
is compile-green.

## Exact compile and adapter commands

Compile the exact candidate first:

    timeout 20m env CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
      cargo test -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle --no-run

Then run the identical pure-model test on each adapter only after no-run is
green:

    timeout 20m env W2_BACKEND=memory CARGO_TARGET_DIR=<isolated-target> \
      CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle \
      -- --nocapture --test-threads=1

    timeout 20m env W2_BACKEND=rocksdb CARGO_TARGET_DIR=<isolated-target> \
      CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle \
      -- --nocapture --test-threads=1

    timeout 20m env W2_BACKEND=slatedb CARGO_TARGET_DIR=<isolated-target> \
      CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle \
      -- --nocapture --test-threads=1

The first compile-green W2 implementation must wire these case IDs through
real Memory, RocksDB and SlateDB CoherentView/ObjectId adapters, not replace
them with the pure model. Durable adapters must additionally flush/drop/reopen
before repeating point, range, diff, materialization, full/range BlobRef, and
corruption cases.

## Static and acceptance gates

    cargo fmt --all -- --check
    git diff --check <exact-base>..<exact-head>
    rustfmt --edition 2021 --check \
      packages/engine-benchmarks/tests/forktree_w2_tracked_blob_cas_oracle.rs
    rustc --edition=2021 --test -D warnings \
      packages/engine-benchmarks/tests/forktree_w2_tracked_blob_cas_oracle.rs
    python3 scripts/forktree_w2_compile_negative_fixtures.py --root <exact-root>
    node scripts/forktree_w2_tracked_blob_cas_residue_verify.mjs \
      --root <exact-root> --base <exact-base> --target <exact-head>
    cargo clippy -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle -- -D warnings

The final package records exact base/head/tree/parents, full-index diff,
ordinary diff, format-patch, patch ID, source/test/script/fixture/report
hashes, static red calibration logs, compile output, and per-adapter terminal
logs. The current package is test/report-only and makes no durable adapter
claim.

Acceptance is BLOCKED by any old owner residue, a second view/object authority,
a write during reads, NULL/tombstone/order mismatch, 65-row collapse failure,
same-size substitution acceptance, corruption acceptance, cold-reopen mismatch,
or any missing adapter case. No W3-W5 production or writer implementation is
part of this package.
