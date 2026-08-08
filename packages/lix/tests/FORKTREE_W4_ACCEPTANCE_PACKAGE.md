# ForkTree W4 acceptance package

Status: immutable TEST/REPORT-ONLY package. This package adds no production
logic, no W5 logic, no storage format, and no runtime result. It is anchored
to the approved d6b Stage-2 lineage so a later W4 implementation can run the
same source and public-semantic gates.

## Immutable anchor

| item | value |
|---|---|
| anchor ref | `origin/codex/forktree-stage2-commit-catalog-failclosed-1f742` |
| anchor head | `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768` |
| anchor parent | `1f742a382c755399b8a49ab536c4f6dc55fffdd8` |
| anchor tree | `641654079f60fcd1c9ff9ccbbd06d3edcabe4096` |
| approved Stage-2 predecessor tree | `860a047b98eaa38368a3d889497628e244c2e0ec` |
| package scope | test/report files only |

The anchor commit is itself a one-file Stage-2 source correction. This package
does not assert that the anchor compiles or that any W4 runtime gate passes.
The expected pre-W4 source oracle is RED on the anchor because the legacy
media-upload writer still exists. That red result is a prerequisite signal,
not a reason to weaken the oracle or add a compatibility route.

Observed source-only result on the immutable anchor: exit `1`, log SHA-256
`e4f7516e30ceabfdbf64900fca845e11bbf09211b3ef49a78e78c743b9914365`. The log
identifies legacy upload spaces, old manifest/completion helpers, direct
prepared-CAS publication, old BinaryCas writer symbols, and the file-content
unlowered rejection. No compiler or adapter was invoked.

## W4 authority contract

The implementation under test must lower all file/blob/upload publication
through this one path:

```text
one CoherentView / StorageRead
  -> one PreparedPublication
  -> one StorageWriteSet + exact preconditions
  -> one transaction-owned prepare_write_set/commit
```

Completed file content has exactly one serving authority:

```text
semantic file state row
  -> BlobId and one authenticated BlobManifestV1 edge
  -> ordered BlobChunkV1 object IDs
```

An open upload has exactly one selected control:

```text
UploadSelectorV1
  -> UploadProgressV1
  -> ReceiptTreeRoot/pages
  -> UploadPartV1
  -> BlobChunkV1
```

`BlobManifestV1.canonical_blob_id` is an integrity assertion, not a second
serving owner. `UploadSelectorV1` is the open-upload selector; old JSON state
and manifest-leaf spaces cannot remain as shadow state.

## Source gate and red controls

Run only the source gate first:

```text
bash packages/lix/tests/forktree_w4_acceptance.sh
```

The gate must fail on a pre-W4 tree and pass only after the compiler-driven
writer deletion is complete. It checks the following without compiling:

* ForkTree owns `publish_new_upload`, receipt/part/progress staging,
  `publish_completed_upload`, and `into_storage_plan`.
* `UPLOAD_STATE_SPACE` and `UPLOAD_MANIFEST_LEAF_SPACE` are absent from
  production source.
* `UploadState`, `UploadManifestLeaf`, `UploadComplete` and old JSON
  publication helpers are absent from `session/media_upload.rs`.
* no file/upload call remains to `stage_atomic_cas_publication` or
  `execute_fast_lix_file_prepared_path_write`.
* no file/upload call remains to old BinaryCas writer methods or
  `binary_cas::kv` publication symbols.
* `transaction/commit.rs` no longer rejects file content as an unlowered
  cohort; checkpoint/W3 rejection may remain until W3 lands.
* the public API entry point remains present while its implementation route is
  transaction-owned.

The source gate is deliberately narrow. It cannot prove semantic atomicity;
the public tests below are mandatory for that.

## One-view/one-plan/one-commit tests

The implementation must add or identify exact public tests that exercise both
Memory and durable adapters. A focused test must:

1. begin one coherent read and bind selector, state root, upload receipt root
   and all validation to that view;
2. prepare one publication containing immutable objects, semantic state/root,
   selector puts/deletes, branch/global selector expectations and the existing
   epoch fence;
3. append ordinary transaction catalog, mutation-revision, actor and
   idempotency writes to the same storage plan;
4. call exactly one backend prepare/commit;
5. inject failure before commit, stale global CAS, stale upload-selector CAS,
   and backend failure, asserting old-or-new visibility and no selected
   partial closure.

Required positive controls:

* open-part publication selects progress and receipt closure atomically;
* completion removes the exact upload selector while installing the state
  manifest edge atomically;
* abort removes the exact selector and leaves shared chunks owned by other
  authenticated roots;
* identical part/completion replay is idempotent and does not add a second
  semantic history member;
* unrelated branch/path writers cannot overwrite this upload binding.

Required negative controls:

* missing or mismatched selector/progress/receipt/part object;
* wrong upload ID, path, declared size, part number, offset, domain or digest;
* duplicate/out-of-order receipt entry, non-contiguous completion, forged
  manifest edge or wrong state BlobId;
* publication from a stale CoherentView or stale selector/global epoch;
* a commit failure after planning leaves the prior state and selector intact.

## Multipart contract

Preserve the current public multipart behavior:

* part offsets are 16 MiB aligned;
* non-final parts are exactly 16 MiB;
* at most four parts may be outstanding out of order;
* identical replay succeeds without duplicate history; conflicting replay
  fails closed;
* completion requires exact declared size and a contiguous received prefix;
* open uploads remain selected and recoverable through the authenticated
  receipt closure; completion atomically moves the root to tracked state.

The tests should supersede, rather than continue reading the old spaces in,
the existing semantic fixtures:

* `sequential_parts_survive_a_new_session_and_publish_one_file`;
* `four_part_window_persists_one_leaf_per_completed_part`.

Those names are behavioral requirements only; the new fixtures must use
`UploadSelectorV1`, `UploadProgressV1`, `ReceiptTree`, and the transaction
publication owner.

## 64 MiB reuse contract

The focused reuse case is a 64 MiB payload represented as exactly 64 canonical
1 MiB chunks. It must:

1. publish and reopen the payload;
2. change one 1 MiB region through the public file path;
3. prove the unchanged 63 chunk ObjectIds and bytes are reused exactly;
4. prove the final BlobId, manifest order, range bytes and semantic file
   history are correct;
5. prove no 64 MiB chunk format, duplicate raw payload, or second manifest
   authority is introduced.

This is a correctness/resource counter, not a performance claim.

## Partial authenticated reads and reopen

Full and range reads must start from the semantic state row's authenticated
BlobId and one manifest edge under the same retained CoherentView. A caller
cannot supply a manifest/object ID, and the old CAS layout cannot be queried
as a fallback.

The tests must cover empty, first-byte, boundary, middle, final-byte and
multi-range reads before and after flush/drop/reopen. Selected manifests and
chunks must be checked for domain, object ID, declared length, content digest,
logical size and BlobId ownership. Missing, malformed, wrong-domain, forged,
wrong-owner and view-mismatched data must fail closed.

History-sensitive controls must include branch, small edit, diff, history,
undo, redo and checkpoint/reopen. File materialization remains the ordinary
semantic transaction transition; upload receipts cannot become a separate
history authority.

## W5 final-reference handoff

W4 publishes roots and selectors only. W5 owns reachability, queue/epoch
processing and physical reclamation. The test handoff is:

* open selector roots progress, receipt pages, parts and chunks;
* completed file state roots manifest and chunks;
* completion deletes the selector and installs the state edge in one fenced
  publication;
* abort deletes the selector, but shared chunks survive while another
  authenticated root remains;
* branch/file-root retirement lets W5 reclaim manifest/chunks only at the
  final reference;
* failed publications may leave unselected immutable orphans but never a
  selected partial state.

No test in this package implements W5, scans legacy receipt spaces, invents a
GC root, or treats cleanup debt as a publication authority.

## Required test ordering and evidence

The future implementation worker must run, in order:

```text
# source-only, no compiler/runtime dependency
bash packages/lix/tests/forktree_w4_acceptance.sh

# private Memory-focused ForkTree owner controls
cargo test -p lix --lib forktree::tests::upload_publication_and_sweep_are_epoch_fenced_in_both_orders -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::upload_abort_releases_receipt_closure_after_final_selector_move -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::upload_completion_moves_receipt_to_tracked_state_atomically -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::receipt_tree_is_path_copied_bounded_and_has_no_predecessor -- --exact --nocapture --test-threads=1
cargo test -p lix --lib forktree::tests::receipt_declared_size_digest_and_aggregate_corruption_fail_closed -- --exact --nocapture --test-threads=1

# then the identical public fixture on RocksDB and SlateDB
cargo test -p lix --test <exact-w4-public-adapter-target> -- <exact-test-filter> --nocapture --test-threads=1

# source/format qualification after the candidate exists
cargo fmt --all -- --check
cargo clippy -p lix --lib --tests --all-features -- -D warnings
```

The `<exact-w4-public-adapter-target>` placeholder is intentional: this
package does not invent a target that is absent from the anchor. The future
implementation must record the concrete target/filter and exact executable
SHA for Memory, RocksDB and SlateDB separately. Runtime must not be claimed on
the compiler-red d6b anchor.

## Package acceptance

Accept only a later immutable candidate that has:

* a passing source gate with zero legacy media-upload/CAS writer residue;
* one-view/one-plan/one-commit evidence and old-or-new crash behavior;
* multipart 16 MiB, completion, 64 MiB reuse, partial-auth corruption and
  cold-reopen correctness on Memory, RocksDB and SlateDB;
* exact W5 final-reference handoff evidence;
* no production changes outside the approved W4 lowering surface, no new
  durable authority/format/cache/compatibility reader, and no W5 logic.

This package is a frozen acceptance contract, not a production qualification
or merge approval.
