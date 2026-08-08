# W4 file/blob/upload production implementation preflight

Status: `READ-ONLY PREFLIGHT / UNRUN`.

This package is a source map and implementation ordering contract. It is not a
production approval, runtime qualification, or W5 implementation. No
production source, adapter, PR, or owner branch was changed for this package.

## 1. Immutable binding

The production source baseline is exact e1af:

| item | value |
|---|---|
| baseline commit | `e1af471b9ab0f598dafa7c2ddec7867667c81740` |
| baseline tree | `bfa0d271a723da8250ab76ada16fda90926f1099` |
| parent | `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` |
| parent..baseline full-index binary diff | `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c` |
| parent..baseline ordinary binary diff | `459a58747fefbc2e6f4f55b2b0fb8d24993c396ef011d63d057bade61a01ed7d` |
| stable patch ID | `31cc575644bf17e65c59d558a03acffc848c2e20` |
| e1af production paths | `sql2/providers/file_history.rs`, `sql2/providers/filesystem_working_diff.rs` |

The frozen W4 v2 contract used by this preflight is the report-only package
`origin/codex/forktree-w4-fileblob-upload-readiness-e1af` at
`ff79e87fdc9cf8db7d1b47158cf9c8715b7471a9`, tree
`674ed66dd0bcc2ab0cd9bb7dee7d6e5fc8645d3a`, parent
`bd313e7e6880e4bd02fff51d7ed7d37d3dd9dcfb`. Its parent..head full-index
diff is `3141365b69c99e9aa21f3de11621d5638e993bd669a8a632ae61aadaba90e08b`,
stable patch ID `0d34c7b5dd0cf8d521177c7916bcf526db12ce68`, and its source RED
log is `834223a468cf787dad96030f924778dd0f07627ae15ebae3408c8d518091e26d`.
The v2 report is
`test-reports/forktree-w4-fileblob-upload-readiness-e1af/W4_READINESS_E1AF.md`,
SHA-256 `f2bd370af93df7e9267592cf4dde8692e20a4aae81420e08307a68d8564f37a5`.
Its nine RED findings remain the admission controls for the implementation
waves below.

## 2. Contract and authority boundary

Every supported W4 mutation must lower through exactly this ownership chain:

```text
one operation-owned CoherentView
  -> authenticated typed Blob/manifest/receipt validation
  -> one PreparedPublication
  -> one into_storage_plan()
  -> existing transaction prepare_write_set()
  -> existing prepared backend commit()
```

The ForkTree publication object is an in-memory plan, not a second commit
owner. `PreparedPublication::into_storage_plan` in
`packages/lix/src/forktree/publication.rs:1067-1130` emits writes and exact
preconditions only. The transaction remains the sole caller of
`prepare_write_set` and `prepared_commit.commit()` in
`packages/lix/src/transaction/context.rs:1655-1690`.

The visible `lix_binary_blob_ref` state row remains the semantic BlobId owner.
ForkTree's authenticated `BlobManifestV1`, `BlobChunkV1`, `UploadPartV1`,
`UploadProgressV1`, `ReceiptTree`, and selector objects are the authenticated
physical closure. They are not a replacement visible row, caller-supplied
identity, cache, or second mutable authority. W5 owns reachability and
reclamation after this closure is durably rooted.

The current source is intentionally not there yet:

* `transaction/commit.rs:1182-1193` rejects nonempty
  `PreparedWriteSet::file_content_writes` and checkpoint publications before
  a ForkTree plan can exist.
* `transaction/context.rs:709-733` still exposes
  `stage_atomic_cas_publication`, which is an independent prepared-CAS bridge.
* `sql2/providers/file.rs:2623-2635` still exposes
  `execute_fast_lix_file_prepared_path_write`; it is reexported by
  `sql2/mod.rs:100-106` and `sql2/providers/mod.rs:41-49`.
* `session/media_upload.rs` still writes mutable upload state and receipt-leaf
  spaces and performs direct per-part and completion commits.
* `binary_cas/context.rs:157-259` still owns
  `ExistingChunkAwareBinaryCasWriter` and fixed-part/manifest/file-payload
  staging. Its old KV owner is already absent from e1af; recreating it is
  forbidden.

## 3. Normal file-content publication: minimum production cut

### Existing caller path

The current semantic path is already concentrated enough for a compiler-driven
cut:

```text
SQL/native file mutation
  -> sql2/providers/file.rs SqlWrite staging
  -> stage_lix_file_content_insert_write/update_write (4154-4235)
  -> RowsWithFileContent / TransactionFileContent
  -> transaction/staging.rs PreparedWriteSet::file_content_writes
  -> Transaction::commit_prepared (1476-1690)
  -> commit::prepare_forktree_publication_with_parent_heads (1583-1603)
  -> PreparedForkTreePlan::into_storage_plan (1609-1610)
  -> metadata/idempotency/preconditions
  -> prepare_write_set
  -> commit_at_boundary / prepared_commit.commit
```

`stage_lix_file_content_blob_ref_write` at
`sql2/providers/file.rs:4215-4235` creates the visible blob-ref row. Empty
content and tombstone handling in the insert/update helpers must remain
semantic cases; removing the old CAS bridge must not turn an authenticated
empty file or deletion into a missing row.

### Required lowerer

The smallest production wave is a transaction-local lowerer in the existing
commit/publication closure. It must:

1. consume each `TransactionFileContent` exactly once after stale
   reconciliation and semantic validation;
2. use the already retained commit-time `CoherentView`/storage read to
   authenticate the named base row, manifest, chunk order, size, and BlobId;
3. stage only typed immutable objects and the visible state edit into the same
   `PreparedPublication` as the ordinary state/catalog mutation;
4. install exactly the expected branch/global selector fences already emitted
   by `PreparedPublication::from_branch_view` (or the corresponding existing
   global-epoch constructor), without opening a second view;
5. call `into_storage_plan` once, then let `commit_prepared` append catalog,
   tracked revision, runtime/idempotency, and filesystem metadata before the
   existing sole backend commit; and
6. reject unsupported cohorts before constructing a plan, preserving the
   current fail-closed behavior for multi-branch, selected-history,
   checkpoint, and other not-yet-lowered operations.

The lowerer may use `stage_blob_chunk`, `stage_blob_manifest`, and the
publication state-transition methods in
`packages/lix/src/forktree/publication.rs:194-250` and `:867-905`. It must not
call `PreparedPublication::commit` (there is no such accepted owner), create a
second `StorageWriteSet`, or invoke `stage_atomic_cas_publication`.

### Required file-content checks

Before any bytes become visible, the wave must cover full and ranged content,
inline/empty/tombstone rows, base reuse, malformed or missing manifest/chunk,
wrong size/hash, same-length substitution, stale visible BlobId, rollback,
reopen, and no-partial-publication. The visible row, authenticated manifest,
ordered chunk identities, and payload size must agree in the one view. An
unreachable immutable object after a failed CAS is allowed; a visible partial
state row or selector is not.

## 4. Multipart part/progress/receipt publication

### Current path and blockers

`session/media_upload.rs:217-381` (`upsert_file_content_part`) currently:

* opens a read and loads mutable `UPLOAD_STATE_SPACE` and
  `UPLOAD_MANIFEST_LEAF_SPACE`;
* calls the old Binary CAS writer's `stage_fixed_part`;
* stages old progress/leaf state and mutation-epoch data;
* calls `prepare_write_set` and `prepared.commit()` directly at
  `:340-358`; and
* opens another read after commit to decide completion.

`publish_completed_upload` at `:383-478` opens another read, calls
`stage_fixed_manifest`, writes the old complete-state encoding, then uses
`stage_atomic_cas_publication` and
`execute_fast_lix_file_prepared_path_write` in a separate session transaction.
This is the central W4 multipart violation: part publication and completion
publication are not one operation-owned ForkTree plan.

The public part contract remains 16 MiB (`UPLOAD_PART_BYTES`), with existing
alignment/final-part validation. It must not be changed to make the new owner
fit.

### Required part wave

For an open upload, the operation owns one `CoherentView` and one
`PreparedPublication`:

```text
UploadPart request
  -> read selector/progress/ReceiptTree in the retained view
  -> authenticate upload binding and existing part/chunk identities
  -> encode changed 1 MiB chunks and typed UploadPartV1
  -> stage_upload_part + ReceiptTreeEdit + UploadProgressV1
  -> put the exact UploadSelectorV1 with its selector precondition
  -> into_storage_plan once
  -> transaction prepare/commit once
```

`PreparedPublication::publish_new_upload` at
`forktree/publication.rs:254-304` already validates selector/progress
bindings, typed parts, progress object identity, and ReceiptTree closure. The
implementation should use its staging methods, not reintroduce the old
mutable upload spaces. A successful part operation may be retried through the
existing selector/precondition/idempotency semantics; it may not perform an
independent commit or use a caller-supplied receipt hash as authority.

### Required completion/abort wave

Completion must use the same retained view for the full authenticated receipt
closure:

```text
retained CoherentView
  -> prepare_upload_completion (forktree/blob.rs:460-627)
  -> validate selector, progress, ReceiptTree pages, part/chunk order,
     aggregate lengths, final digest, canonical manifest
  -> one BranchStateTransition adding the manifest's visible BlobId root
  -> PreparedPublication::publish_completed_upload (publication.rs:870-905)
  -> delete exact upload selector and stage manifest/state transition
  -> into_storage_plan once -> transaction commit once
```

`publish_completed_upload` rejects a completion derived from another view and
requires the manifest to be present in the state edit before staging. Abort
must delete only the authenticated selector through `abort_upload`; it must
not directly sweep parts or chunks. Failed completion/abort leaves the prior
visible state and selector intact, subject to the existing CAS fence.

W5, not this wave, owns eventual selector retirement, root closure traversal,
shared-chunk retention, final-reference reclamation, and recovery of abandoned
upload objects.

## 5. Read migration before deleting the old reader

The current binary-CAS reader surface has multiple consumers and must be
migrated as a closure, not deleted piecemeal:

| current seam | exact source | required W4 action |
|---|---|---|
| structured file read | `session/execute.rs:1137-1188` | retain its one `begin_read`; create one authenticated `CoherentView`; use `bind_blob`, `load_blob_bytes_many`, or `load_blob_ranges_many` |
| exact SQL file reads | `session/execute.rs:2336-2365`, `:2442-2459` | pass the same view-bound authenticated reader through exact-result hydration |
| SQL read abstraction | `sql2/context.rs:115-122`, `:359-361`, `:497-512` | replace the raw `BlobDataReader` ownership with a view-bound reader or make the trait a non-opening façade over the retained view |
| transaction staged reads | `transaction/context.rs:8242-8305` | preserve staged overlay, but use the transaction's retained coherent view; no fallback to a separately opened Binary CAS read |
| write execution helper | `transaction/context.rs:8735-8743` | remove its fresh `begin_read` and bind it to the transaction view |
| file provider ranges/full bytes | `sql2/providers/file.rs:5171-5269` | route through authenticated view methods; preserve range bounds and payload-size validation |
| filesystem index hydration | `filesystem/path_index.rs:572-650` | use the same authenticated view or delete this optional path; its current silent `Ok(self)` on load error is not an acceptable corruption fallback |
| e1af history/working-diff providers | `sql2/providers/file_history.rs`, `filesystem_working_diff.rs` | keep e1af's authority checks, but migrate their BlobDataReader transport without opening a second read |

`changelog/materialization.rs:204-224` is a separate JSONStore byte reader and
is not part of binary-CAS deletion. The migration must preserve that boundary.

The minimum accepted read property is one coherent snapshot from selector,
state row, manifest, chunk references, and payload bytes. No helper may call
`begin_read`, refresh a view, silently fall back to the old reader, or retain a
second mutable cache that can disagree with the authenticated row.

## 6. Dependency-ordered deletion plan

This is the smallest order that lets the compiler expose callers without
recreating missing legacy owners.

| wave | production closure | deletion gate |
|---|---|---|
| D0 | Add path/function-scoped negative checks and one-plan counters. Keep typed values (`BlobId`, `BlobPayload`, authenticated receipts) but do not add a compatibility layer. | Existing e1af RED remains reproducible; unsupported cohorts still fail before plan creation. |
| D1 | Lower `PreparedWriteSet::file_content_writes` from `transaction/commit.rs` using one commit read/view and one `PreparedPublication`; remove only the file-content rejection after tests pass. | `file_content_writes` no longer reaches an independent CAS writer; one prepare/commit and atomic rollback/stale/corruption tests. |
| D2 | Replace `upsert_file_content_part` old-space/CAS/direct-commit path with typed upload part/progress/ReceiptTree/selector staging in the transaction owner. | 16 MiB part semantics, selector/progress binding, retry/stale/no-partial tests. |
| D3 | Replace completion and abort with `prepare_upload_completion`, `publish_completed_upload`, and one state transition. | Full receipt/chunk authentication, final BlobId binding, range/full read, cold reopen, same-view completion, no old completion bridge. |
| D4 | Migrate all readers in section 5, including transaction and path-index helpers. | Memory then RocksDB then SlateDB read equality; malformed/missing/wrong-size/hash/substitution fail closed; no second view. |
| D5 | Delete `stage_atomic_cas_publication`, `execute_fast_lix_file_prepared_path_write`, `PreparedCas`, `ExistingChunkAwareBinaryCasWriter`, fixed-part/fixed-manifest/file-payload writer methods, and their reexports only after `rg`/compiler prove zero callers. | Full workspace source residue and negative compile checks; no `binary_cas/kv.rs` recreation. |
| D6/W5 handoff | Migrate authenticated root observation/reclamation first. Only then delete old upload receipt cleanup, Binary CAS GC/reclamation calls, old mutable upload spaces, and obsolete GC bridges. | W5 proves transitive root closure, selector owner/view fences, shared/final-reference retention, recovery/corruption fail-closed. |

The following must not be deleted in D1-D5 merely to make the compiler quiet:
`gc.rs` old reclamation ownership, `stage_reclaimable_upload_receipts`, or
any root/epoch operation whose replacement is not already owned by W5. A
source error at these boundaries is a dependency signal, not permission to
restore `binary_cas::kv` or add a fallback.

## 7. Forbidden widening and negative checks

The implementation must reject or fail the source gate if it introduces any of
the following:

* `PreparedPublication::commit`, a second `prepare_write_set`, or a direct
  backend commit from file/upload code;
* a second `begin_read`, `StorageAdapterRead` refresh, or a helper that hides a
  new read behind `load_bytes_many`;
* caller-supplied BlobId/manifest/receipt identity, a mutable side index, a
  cache used as authority, or a compatibility decoder/dual writer;
* restoration of `BINARY_CAS_*`, `UPLOAD_STATE_SPACE`,
  `UPLOAD_MANIFEST_LEAF_SPACE`, legacy CAS/upload/prepared-CAS reexports, or
  raw storage-space constructors;
* broadening ordinary W4 into checkpoint, selected-history, multi-branch, or
  W5 sweep publication before its own typed lowerer is ready;
* weakening SQL/file semantics, empty/tombstone behavior, size/hash checks,
  stale CAS fences, or corruption handling to fit the new owner;
* deleting authenticated immutable objects in W4 or treating an unreachable
  object as a visible failure. W5 owns reclamation.

Required source negatives are path-aware and must scan production, tests,
benchmarks, reexports, and generated/native call sites. Lexical matches in
unrelated JSON/changelog readers are not sufficient evidence; each match must
be classified by owner and call path.

## 8. W5 authenticated-root handoff

The handoff point is a successful single transaction containing both the
visible `lix_binary_blob_ref` state row and the complete authenticated
manifest/chunk closure. W5 must then:

1. observe the committed selector/state root from its own coherent read;
2. authenticate manifest, ordered chunk references, upload selector/progress,
   and branch/global root binding before considering any object reclaimable;
3. retain every transitive ancestor and shared chunk reachable from all live
   branch, checkpoint, history, upload, and serving roots;
4. fence publication-first versus GC-first races with the global selector/
   progress CAS; and
5. after final-reference retirement, sweep only objects proven unreachable.

Corrupt or missing named roots fail closed. Failed W4 publication may leave
unreachable immutable objects, but must never publish a partial state row,
selector, manifest, or receipt closure. W4 must not call W5 sweep/recovery
directly or invent a second root registry.

## 9. Post-freeze command plan (UNRUN)

The following is the command order for the first runnable successor. It is a
plan only; no command below was run by this preflight. Every cell has a hard
1200-second limit and stops the sequence on the first error or timeout.

```bash
set -Eeuo pipefail
BASE=e1af471b9ab0f598dafa7c2ddec7867667c81740
HEAD=<immutable-successor-sha>
TARGET=/root/repos/target-w4-fileblob-upload

git diff --check "$BASE..$HEAD"
git diff --binary --full-index --no-ext-diff "$BASE..$HEAD" | sha256sum
timeout --foreground --kill-after=5s 1200s cargo fmt --all -- --check
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo check -p lix
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo clippy -p lix --lib -- -D warnings
timeout --foreground --kill-after=5s 1200s \
  env CARGO_TARGET_DIR="$TARGET" cargo test -p lix --lib --no-run
```

The focused W4 oracle then runs exactly `Memory -> RocksDB -> SlateDB`:

```bash
run_cell() {
  local name="$1"; shift
  timeout --foreground --kill-after=5s 1200s "$@" || {
    echo "STOP_FIRST_BLOCKER cell=$name" >&2
    exit 1
  }
}
run_cell Memory   "$W4_ORACLE" --backend memory   --focused
run_cell RocksDB  "$W4_ORACLE" --backend rocksdb  --focused
run_cell SlateDB  "$W4_ORACLE" --backend slatedb  --focused
```

Each adapter cell must record exact result digests, selector/manifest/chunk
reads and writes, logical rows/bytes, object bytes, backend calls, allocation
and RSS counters, disk after flush/reopen, and no-partial-write outcomes. The
focused order covers ordinary file insert/update/delete, empty/tombstone,
16 MiB multipart parts, completion/abort, 1 MiB chunk reuse, authenticated
full/range reads, stale/rollback, corruption/substitution, branch/diff/merge,
checkpoint/reopen, shared roots, and final-reference handoff. No 50/500 MiB
matrix or Git/comparator workload is admitted in this preflight.

If Memory fails, RocksDB and SlateDB are not run. If RocksDB fails, SlateDB is
not run. A compile timeout is a host/build-boundary result and is not retried
under this package. Any later performance claim must use separate exact-SHA
targets/binaries and must not reuse contaminated preflight numbers.

## 10. Source evidence commands

The source map was produced read-only from the exact e1af worktree with:

```bash
git rev-parse HEAD HEAD^{tree} HEAD^
git diff --stat b484e20d845aee3f8137bfa3496f9b3cd0e8cd35..e1af471b9ab0f598dafa7c2ddec7867667c81740
rg -n 'reject_not_yet_lowered_cohorts|stage_atomic_cas_publication|commit_prepared|into_storage_plan|stage_lix_file_content_(insert|update|blob_ref)|execute_fast_lix_file_prepared_path_write|upsert_file_content_part|publish_completed_upload|prepare_upload_completion|BlobDataReader|stage_reclaimable_upload_receipts' packages/lix/src
git diff --check
```

No compiler, benchmark, adapter, PR, or production mutation was performed for
this report. The package is ready for R5 to bind to the first immutable W4
production successor after the source owner freezes it.
