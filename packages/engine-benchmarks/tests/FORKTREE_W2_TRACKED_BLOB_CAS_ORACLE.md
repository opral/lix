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

All W2 reads begin from one retained CoherentView. The view authenticates the
selected state root, branch/global overlay roots, commit/diff roots and BlobId
object root. An opaque ObjectId is the only physical object identity accepted
by W2; BlobRef is a typed logical reference bound to that ObjectId and its
size/digest. No caller can provide a storage space, raw key, manifest/chunk
row, detached root, or second read handle.

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
      CoherentView -> ObjectId -> authenticated payload/manifest digest ->
      complete bytes

    BlobRef range:
      CoherentView -> same ObjectId and digest validation -> bounded byte range

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

Required observations:

* branch rows shadow global rows; NULL is a value, tombstone is distinct, and
  ordered ranges preserve canonical key order with explicit tombstone mode;
* diff identity includes schema, file identity, EntityPk, ChangeId and CommitId;
  equal EntityPk values in different files remain distinct;
* 65 ordered rows collapse into one canonical root without duplicate or
  out-of-order keys;
* full and range BlobRef reads validate the same ObjectId, size and digest;
* a same-size manifest/payload substitution fails by digest, not by length;
* missing, malformed, noncanonical, wrong-kind, missing-child, wrong-digest,
  and invalid-range state/blob data fail closed;
* a second view cannot be paired with the first view's state/object authority;
* cold reopen restores view/root/epoch state and read operations perform zero
  writes.

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

    node scripts/forktree_w2_tracked_blob_cas_residue_verify.mjs --root <checkout>

The verifier scans packages/lix/src, rejects legacy files/symbols/namespaces,
and requires CoherentView, ObjectId, BlobId and BlobRef owner symbols. It is
expected RED on the d6b/e92 predecessor because the old owners remain. A
future candidate must pass with zero findings; a source-only pass never grants
runtime acceptance.

Calibration on the immutable e92/d6b production tree is RED with 472 findings;
the deterministic output log SHA-256 is
92877f08d82db2154085e016b3053e7c52b20d2674b74f3781fd1aceb5cc3d08. The pure
model source compiled with rustc --edition=2021 --test -D warnings and its
standalone runtime passed all 7 tests; executable SHA-256 is
0aa20c32193460a180f6ddd4a6fb0c73c35883f3c866a0fb0eecfd34145f93c3. The
package-level Cargo target is intentionally not claimed until the prerequisite
reader lineage is compile-green.

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
    node scripts/forktree_w2_tracked_blob_cas_residue_verify.mjs --root <exact-root>
    cargo clippy -p lix_benchmarks \
      --test forktree_w2_tracked_blob_cas_oracle -- -D warnings

The final package records exact base/head/tree/parents, full-index diff,
ordinary diff, format-patch, patch ID, source/test/script/report hashes,
static red calibration logs, compile output, and per-adapter terminal logs.

Acceptance is BLOCKED by any old owner residue, a second view/object authority,
a write during reads, NULL/tombstone/order mismatch, 65-row collapse failure,
same-size substitution acceptance, corruption acceptance, cold-reopen mismatch,
or any missing adapter case. No W3-W5 production or writer implementation is
part of this package.
