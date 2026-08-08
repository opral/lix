# W4 file/blob/upload publication readiness package for e1af

Status: TEST/REPORT-only readiness evidence. This package makes no production
edit, does not run Cargo or adapters, does no benchmark, and does not perform
W5 work. The source gate is intentionally calibrated **RED** on the exact
accepted e1af object because its W4 publication cut is not wired and forbidden
legacy authorities remain.

## Immutable source binding

```text
candidate=e1af471b9ab0f598dafa7c2ddec7867667c81740
candidate tree=bfa0d271a723da8250ab76ada16fda90926f1099
candidate parent=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
b484..e1af full-index binary SHA-256=9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c
b484..e1af stable patch ID=31cc575644bf17e65c59d558a03acffc848c2e20
e1af subject=fix(sql): close file history authority gaps
e1af production paths=sql2/providers/file_history.rs, sql2/providers/filesystem_working_diff.rs
```

The prior frozen W4 contracts remain bound as independent package evidence:

```text
W4 acceptance package=b1dd25ebc90e95304709fbbafcc662c144b0449c
W4 package tree=7632519278f18665bd1cd32590d031e817df0a65
W4 package diff SHA-256=e74e45c3210a4f254923c7e81ea38654aca07229e9de079550dca4a0aa60be44
W4 package patch ID=3e12a72f876a26e895121046014b167599e781e2
W4 review verdict=TEST/REPORT-ONLY BLOCKER
W4 review report SHA-256=8abc6846e92e47cb495608555d5dc332ca06ce992784d2b8943010377de7be30

file/blob landing package=a4c2bb7e64708142885ca26c12e6c62bec52420a
file/blob landing tree=4334430fcb70b3153841cfdeb78a85a7ce740e2f
file/blob landing review verdict=TEST/REPORT-ONLY BLOCKER
file/blob landing review SHA-256=9c7564ccd09c03a52d9568da063f0babde5ae3fb7a8785dd9d5ed19206d382e9
```

The W4 review blockers are preserved: the legacy-writer negative must be
global/path-aware, and the runtime package must include explicit 1% and 10%
reuse plus checkpoint-publication/cold-reopen controls.

## e1af source result: RED

The exact source gate is `source_gate_red.sh`. It is intentionally expected to
exit 1. The first discriminating blockers are:

| Finding | Exact source anchor | Meaning |
| --- | --- | --- |
| RED-01 | `transaction/commit.rs:1068,1182-1189` | nonempty `file_content_writes` is rejected before a ForkTree storage plan; normal file publication is not lowered |
| RED-02 | `transaction/context.rs:709` | `stage_atomic_cas_publication` remains callable as a legacy CAS publication seam |
| RED-03 | `sql2/providers/file.rs:2623`; `sql2/mod.rs:105`; `sql2/providers/mod.rs:48` | prepared-CAS file bridge remains exported and callable |
| RED-04 | `session/media_upload.rs:18-21,217,345,383-437` | old upload mutable spaces, direct per-part prepare/commit, and old completion bridge remain |
| RED-05 | `binary_cas/context.rs:159-261` | `ExistingChunkAwareBinaryCasWriter` and fixed-part/manifest/file-payload methods remain |
| RED-06 | `binary_cas/context.rs:92,135,153,188,204,217,231,244`; no `binary_cas/kv.rs` in e1af tree | stale legacy CAS references remain after the old KV owner was deleted; this is compiler-red, not a new implementation |
| RED-07 | `storage_bench.rs:1666-2440`; `media_upload.rs:884-1493` | old `BINARY_CAS_MANIFEST_SPACE`/`BINARY_CAS_CHUNK_SPACE` references remain |
| RED-08 | `session/media_upload.rs:19-21`; `gc.rs:45-74` | old raw mutable-space declarations remain despite the sealed W0 boundary |

These are source-derived findings, not compiler output. A build is forbidden by
this assignment; the package records the exact future compile commands but does
not claim diagnostics from an execution.

## Required one-authority publication contract

Every supported file-content and upload operation must use exactly:

```text
operation input
  -> one operation-owned CoherentView over the accepted read
  -> authenticate visible state, BlobId, manifest, chunks, selector/receipt closure
  -> one PreparedPublication
  -> one into_storage_plan
  -> one existing transaction prepare_write_set
  -> one prepared backend commit
```

No helper may acquire a second read, refresh a view, persist caller provenance,
or commit a CAS write independently. Unsupported cohorts fail closed before
plan creation. There is no compatibility reader, migration decoder, fallback
writer, mutable cache, or second physical authority.

### State authority

The visible `lix_binary_blob_ref` row from
`packages/lix/src/schema/builtin/lix_binary_blob_ref.json` remains the sole
semantic BlobId/size owner. `BlobManifestV1.canonical_blob_id` is an
authenticated integrity claim, not a second owner. Every manifest edge and
ordered `BlobChunkV1` object must be authenticated on the same `CoherentView`
before publication or output. Same-size manifest/content substitution, wrong
domain/object ID, missing/malformed chunk, wrong declared size/hash, cross-view
row, and partial closure must fail closed with zero visible partial writes.

### Normal file content

The live funnel is `sql2/providers/file.rs:2572,2597,3145,3163,3182,3373,
4154,4179,4215` into `TransactionFileContent` and
`transaction/staging.rs:file_content_writes`. The missing e1af lowerer must,
using the transaction's retained view, authenticate the named base when
reusing chunks, stage changed 1 MiB `BlobChunkV1` objects and a complete
`BlobManifestV1`, and add descriptor/blob-ref state mutations to the same
publication. Empty-file and tombstone transitions remain explicit valid
states; malformed live content-bearing rows fail closed.

### Upload parts and completion

`session/media_upload.rs:23` fixes public parts at 16 MiB and the existing
`binary_cas/types.rs` fixes internal chunks at 1 MiB. The replacement must use
`ForkTree` `OBJECT_SPACE` for authenticated chunks, parts, progress and
ReceiptTree objects and `SELECTOR_SPACE` for the upload selector/binding.

`forktree/publication.rs` already provides `publish_new_upload`, `abort_upload`,
and `publish_completed_upload`; `forktree/blob.rs:460` provides
`prepare_upload_completion`. These are currently type-probed/unwired, not a
second live writer. Each part must authenticate selector/progress/receipt
closure on one view, then publish one plan/commit. Completion must derive the
canonical BlobId from all ordered authenticated chunks, publish the final
manifest plus visible file/blob-ref mutation, and retire the selector in the
same plan. Abort deletes only the exact selector; W5 owns unreachable object
reclamation.

Required controls include exact 16 MiB alignment, final/non-final parts,
out-of-order bounded window, replay identity, path/size binding, stale CAS,
rollback, abort, malformed/missing/wrong-size/wrong-hash/substituted part,
cold reopen, and no partial publication.

## Legacy authority deletion order

Delete only after the last reader/writer moves:

1. Lower `file_content_writes` in `transaction/commit.rs`; preserve rollback,
   stale fences, metadata, empty/tombstone, branch/history and cold-reopen
   semantics. Then remove the file-content rejection and legacy
   `FileContent::PreparedCas`/`BlobWriteReceipt` bridge.
2. Move multipart state/leaf writes in `session/media_upload.rs:217-775` to
   ForkTree selector/object publication; move completion `:383-437` to
   `prepare_upload_completion` + `publish_completed_upload`; move abort and
   receipt scanning to W5-owned closure handling.
3. Route all file/plugin/archive reads through `CoherentView::bind_blob` and
   bounded full/range loaders. Delete globally:
   `ExistingChunkAwareBinaryCasWriter`, `stage_fixed_part`,
   `stage_fixed_manifest`, `stage_file_payload`, and `binary_cas::kv` calls.
4. Delete `transaction/context.rs:709` `stage_atomic_cas_publication`, SQL
   exports of `execute_fast_lix_file_prepared_path_write`, and all old
   prepared-CAS callers.
5. Delete old upload spaces `UPLOAD_STATE_SPACE` and
   `UPLOAD_MANIFEST_LEAF_SPACE`, old Binary CAS manifest/chunk registries,
   old module/reexports/metrics, and storage-bench scans only after W5 owns
   final-reference closure. Never recreate missing `binary_cas/kv.rs`.

The `lix_binary_blob_ref` schema and visible BlobId row remain; only the old
physical CAS/upload authorities and bridges are deleted.

## Static RED command and future adapter order

Static calibration, expected exit 1:

```bash
bash test-reports/forktree-w4-fileblob-upload-readiness-e1af/source_gate_red.sh \
  "$PWD" e1af471b9ab0f598dafa7c2ddec7867667c81740
```

Future compile gates are not run here and must use an isolated target:

```bash
CARGO_TARGET_DIR=/root/repos/target-w4-e1af-memory cargo fmt --all -- --check
CARGO_TARGET_DIR=/root/repos/target-w4-e1af-memory cargo check -p lix --lib --all-targets
CARGO_TARGET_DIR=/root/repos/target-w4-e1af-memory cargo clippy -p lix --lib --all-targets -- -D warnings
```

After compile-green, run the identical focused correctness package in order:

```text
Memory -> RocksDB -> SlateDB
```

RocksDB and SlateDB must record exact result digests, malformed/substituted
object failures, rollback/stale behavior, cold reopen, one-view/one-plan/one
commit counters, object/row reads and writes, bytes, allocations/RSS and
settled disk. Explicit 1% and 10% edits on 64 MiB content and checkpoint
publication followed by cold reopen are required. No adapter execution is
claimed by this package.
