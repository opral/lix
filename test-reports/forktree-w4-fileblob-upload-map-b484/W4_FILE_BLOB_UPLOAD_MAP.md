# W4 file/blob/upload publication implementability map

Status: TEST/REPORT-only design evidence. This package maps exact blocked
lineage `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`; it does not modify
production, compile, run an adapter, benchmark, push a production branch, or
approve a W4 implementation.

## Exact source and prior W4 anchors

```text
mapped commit=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
mapped tree=4477c83b246bddac09cd972564bd4ccd67f90f7b
mapped parent=fd2be256d763f17e9f127d4c984e36fba191cb82
fd2..b484 full-index binary SHA-256=d36495fc406cc213bb5729babae761916f97bd515221de14c1f3ae114ec22610
fd2..b484 stable patch ID=e90c9dd93db7c343f67887218049406640a77631
mapped production delta=sql2/providers/file_history.rs, sql2/providers/filesystem_working_diff.rs
```

Locally available prior W4 contracts are bound as review evidence, not as
accepted production heads:

```text
W4 acceptance package ref=origin/codex/forktree-w4-acceptance-d6b
W4 package head=b1dd25ebc90e95304709fbbafcc662c144b0449c
W4 package tree=7632519278f18665bd1cd32590d031e817df0a65
W4 anchor=d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
W4 package diff SHA-256=e74e45c3210a4f254923c7e81ea38654aca07229e9de079550dca4a0aa60be44
W4 package patch ID=3e12a72f876a26e895121046014b167599e781e2
W4 independent review=TEST/REPORT-ONLY BLOCKER
W4 review report SHA-256=8abc6846e92e47cb495608555d5dc332ca06ce992784d2b8943010377de7be30

file/blob landing oracle ref=origin/codex/file-blob-landing-acceptance-d6b
file/blob landing head=a4c2bb7e64708142885ca26c12e6c62bec52420a
file/blob landing tree=4334430fcb70b3153841cfdeb78a85a7ce740e2f
file/blob landing review=TEST/REPORT-ONLY BLOCKER
file/blob review report SHA-256=9c7564ccd09c03a52d9568da063f0babde5ae3fb7a8785dd9d5ed19206d382e9
```

The prior W4 review found the acceptance source gate was too narrow: it did
not globally forbid `ExistingChunkAwareBinaryCasWriter`, `stage_fixed_part`,
`stage_fixed_manifest`, `stage_atomic_cas_publication`, or
`execute_fast_lix_file_prepared_path_write`. The prior file/blob review also
required explicit 1% and 10% reuse cells and checkpoint publication/reopen.
Those requirements are mandatory in the future gate below.

## Current b484 authority map

### Public file content and SQL write funnel

The current SQL/provider path still creates a legacy `TransactionWrite::RowsWithFileContent`
cohort and hands it to the transaction staging layer:

| Path and symbol | Role | Current owner status |
| --- | --- | --- |
| `packages/lix/src/sql2/providers/file.rs:2207` `LixFileInsertSink::write_batches` | batches parsed SQL file inserts and stages descriptors/content | live public SQL writer; transient staging only until commit |
| `file.rs:2572` `execute_fast_lix_file_path_writes` | path-based fast insert/upsert entry | live writer entry |
| `file.rs:2597` `execute_fast_lix_file_id_path_writes` | id+path fast entry | live writer entry |
| `file.rs:2623` `execute_fast_lix_file_prepared_path_write` | consumes `BlobWriteReceipt` from old upload finalization | legacy bridge; must be deleted |
| `file.rs:3145,3163,3182` content update-by-id functions | update content and optional metadata; loads path index/blob-ref rows | live writer entry; must lower through the same ForkTree publication |
| `file.rs:3373` `stage_lix_file_fast_batch` | converts staged rows/content into `ctx.stage_write` | live transient staging funnel |
| `file.rs:4154` `stage_lix_file_content_insert_write` | creates `TransactionFileContent` and blob-ref row | transient intent; final BlobId row is durable authority |
| `file.rs:4179` `stage_lix_file_content_update_write` | creates replacement or blob-ref tombstone | transient intent; empty/tombstone semantics must remain exact |
| `file.rs:4215` `stage_lix_file_content_blob_ref_write` | appends `lix_binary_blob_ref` row with `blob_hash` and `size_bytes` | final visible BlobId owner; retain and authenticate |
| `packages/lix/src/session/execute.rs:1035,1088` | session file-content APIs call SQL fast paths | live public session caller |
| `packages/lix/src/transaction/staging.rs:65,82,966` | retains `file_content_writes` through statement/transaction rollback | transient transaction owner; must feed one publication |
| `packages/lix/src/transaction/commit.rs:1182` `reject_not_yet_lowered_cohorts` | rejects every nonempty `file_content_writes` cohort before publication | explicit compiler/runtime blocker, not a W4 writer |

`TransactionFileContent` in `transaction/types.rs:1808+` contains inline
`BlobPayload`, legacy `PreparedCas(BlobWriteReceipt)`, base/splice provenance,
auxiliary payloads, and plugin checkpoint data. These are transient operation
inputs. The only durable content authority must be the authenticated visible
`lix_binary_blob_ref` state row (`schema/builtin/lix_binary_blob_ref.json`),
whose `blob_hash` and `size_bytes` must exactly bind to the authenticated
ForkTree manifest and reconstructed bytes.

### Legacy Binary CAS / payload path

At b484, `packages/lix/src/binary_cas/` contains only `context.rs`, `metrics.rs`,
`mod.rs`, and `types.rs`; `binary_cas/kv.rs` is absent. Nevertheless the
remaining facade contains these live-looking owner APIs:

| Path/symbol | Current evidence | Classification |
| --- | --- | --- |
| `binary_cas/context.rs:74-115` `BinaryCasContext`, `reader`, `writer_skipping_existing_chunks` | storage reader/writer factory | stale facade until the missing KV owner is replaced; not a safe second writer |
| `context.rs:159-217` `ExistingChunkAwareBinaryCasWriter`, `stage_payload`, `stage_fixed_part`, `stage_fixed_manifest` | old chunk/manifest staging interface | legacy writer residue; must disappear globally, not only from `session/media_upload.rs` |
| `context.rs:224-261` `stage_file_payload` | flat-delta/same-length splice/full fallback dispatch | legacy writer seam; replace with authenticated ForkTree object staging |
| `context.rs:92,135,153,188,204,217,231,244` `binary_cas::kv::*` | references a path absent from the exact b484 tree | compiler fallout proving this lineage is not runnable |
| `binary_cas/mod.rs:1-12` module/reexports | exposes the facade and payload identity types internally | delete/rehome only after every reader/writer has moved; no compatibility reexport |
| `binary_cas/types.rs:39-76` `BlobId` and fixed-chunk identity; `:283` `BlobWriteReceipt` | identity type is needed by visible row/auth; receipt belongs to old prepared-CAS path | retain identity semantics, remove receipt/old writer after migration |
| `binary_cas/types.rs:103-151` splice hints | transient optimization provenance | may remain only as non-authoritative input to the new authenticated lowerer; no persisted/cache authority |

The old physical manifest/chunk symbols are also referenced after their
definitions were removed:

```text
BINARY_CAS_CHUNK_SPACE: session/media_upload.rs:884,1000,1493;
  storage_bench.rs:1806,2104,2241
BINARY_CAS_MANIFEST_SPACE: binary_cas/context.rs:92;
  storage_bench.rs:1666,1703,1803,2026,2238,2440
binary_cas::stage_mutation_epoch: gc.rs:583,2225;
  session/media_upload.rs:337,963,987
```

These are not an excuse to recreate old spaces. They are compiler-deletion
fallout or live callers awaiting the W4/W5 replacement.

### Existing multipart upload path

`packages/lix/src/session/media_upload.rs` is still the old durable owner:

| Path/symbol | Role | Durable/temporary classification |
| --- | --- | --- |
| `:18-24` `UPLOAD_STATE_SPACE`, `UPLOAD_MANIFEST_LEAF_SPACE`, `FILE_UPLOAD_PART_BYTES` | old receipt state/leaf spaces; public part size is 16 MiB | two legacy mutable spaces; 16 MiB contract must survive in new object model |
| `:34-112` `stage_reclaimable_upload_receipts` | scans open/completed state and leaves, returns live chunk hashes and stages deletion | old receipt/GC authority; replace with selector/object closure and W5 reachability |
| `:217` `upsert_file_content_part` | validates upload binding, writes chunk payloads and leaf/state, direct `prepare_write_set` | legacy per-part writer; must become one view→publication→transaction commit |
| `:263-267` `writer_skipping_existing_chunks().stage_fixed_part` | old 1 MiB payload chunking/reuse | legacy Binary CAS writer; replace with `BlobChunkV1` object IDs and authenticated receipt closure |
| `:304-337` leaf/state writes and mutation precondition | old space atomicity and stale CAS | preserve stale/replay behavior in selector expectation and global epoch CAS |
| `:345-355` `prepare_write_set`/`prepared.commit` | independent part commit | forbidden after W4 cut; must use transaction-owned publication |
| `:383` `publish_completed_upload` | finalizes leaves to one old manifest receipt and then file fast path | legacy finalization bridge; replace with `prepare_upload_completion` + `PreparedPublication::publish_completed_upload` |
| `:402-437` `stage_fixed_manifest`, `stage_atomic_cas_publication`, `execute_fast_lix_file_prepared_path_write` | old manifest/CAS/file publication | all three are deletion targets after the new completion cohort lands |
| `:551,605` `stage_upload_state`, `stage_upload_manifest_leaf` | old JSON/byte leaf encoders | delete after selector/progress/ReceiptTree migration |
| `:706-775` leaf scanning/loading | old completion/recovery reader | delete after all readers use authenticated upload selector/progress closure |
| `:915-1083` receipt publication/CAS sweep tests/helpers | old test/support path | rewrite as object/selector/W5 tests, then remove old names |

The public `FILE_UPLOAD_PART_BYTES` value is not an authority. The replacement
must preserve exactly 16 MiB non-final alignment, bounded four-part concurrency,
out-of-order window behavior, path/size binding, replay idempotency, abort,
and completion semantics.

### ForkTree object/blob/upload path already present but not wired

The new physical owner is present in b484 but operation wiring is incomplete:

| Path/symbol | Contract role | Current status |
| --- | --- | --- |
| `forktree/blob.rs:28` `CanonicalBlobIdBuilder` | 1 MiB chunk hashes and canonical fixed-manifest BlobId | authority-compatible integrity computation |
| `blob.rs:106` `bind_state_blob_ref`; `:152` `CoherentView::bind_blob` | bind visible BlobId/size/manifest edge to one coherent view | correct owner boundary |
| `blob.rs:163,178` full/range loaders | authenticate manifest/chunk object domain, size, digest, BlobId and view | correct bounded read seam |
| `blob.rs:460` `prepare_upload_completion` | authenticate selector/progress/ReceiptTree/parts/chunks and derive complete manifest | completion proof, currently not connected to public upload path |
| `forktree/publication.rs:194-221` stage chunk/manifest/upload-part/progress | encode authenticated objects into `OBJECT_SPACE` publication set | target writer, not a second writer |
| `publication.rs:257` `publish_new_upload` | validates and stages chunk/part/progress/ReceiptTree closure and absent selector | target open/resume publication |
| `publication.rs:306` `abort_upload` | validates raw selector against typed selector and deletes selector | target abort operation |
| `publication.rs:870` `publish_completed_upload` | requires manifest in state edit, stages manifest, deletes upload selector, publishes transition | target atomic completion operation |
| `publication.rs:960` `delete_upload_selector` | selector deletion with exact expected bytes | target stale-CAS fence |
| `forktree/mod.rs:19,91,150-157` exports/type probes | object and publication API is type-checked/probed | explicit unwired boundary; no runtime claim |
| `transaction/commit.rs:1182-1189` | rejects `file_content_writes` before plan creation | exact missing lowerer |

ForkTree `OBJECT_SPACE` must own immutable chunk, part, manifest, progress,
ReceiptTree and upload-related objects; `SELECTOR_SPACE` must own the upload
selector and its binding. The final visible file blob-reference state remains
the sole semantic BlobId owner. Upload progress/receipt is durable resumable
operation state, not a second published payload authority.

## Required W4 publication shape

Every supported operation must have this one shape, with no legacy branch:

```text
operation input
  -> one operation-owned CoherentView over the exact accepted read
  -> authenticate visible selector/state and all named object/manifest/chunk edges
  -> one PreparedPublication
       OBJECT_SPACE: immutable chunks/parts/manifests/receipt trees
       SELECTOR_SPACE: upload selector / progress binding / expected CAS bytes
       ForkTree state edit: descriptor + visible lix_binary_blob_ref BlobId/size
  -> PreparedPublication::into_storage_plan exactly once
  -> existing transaction prepare_write_set exactly once
  -> existing prepared backend commit exactly once
```

No helper may open a second read, refresh a view, or commit a prepared CAS
write independently. Unsupported layouts/cohorts must fail closed before plan
creation. The old `stage_atomic_cas_publication` hook is not a compatibility
fallback; it must be deleted when the final lowerer owns the entire cohort.

### Normal file content

`TransactionFileContent` is already the operation's transient payload carrier.
The smallest production cut is a transaction commit lowerer that consumes the
prepared file-content cohort using the transaction's retained opening read,
authenticates any existing `lix_binary_blob_ref` row and named base manifest,
creates only changed `BlobChunkV1` objects plus a complete authenticated
`BlobManifestV1`, and adds the descriptor/blob-ref `StateTreeMutation` to the
same publication. For an empty replacement it must produce the explicit
empty/tombstone state semantics without a payload object. It must never persist
splice provenance, caller BlobId, or a receipt as authority.

### Upload part/resume

Each 16 MiB part operation must authenticate the current upload selector,
progress object, binding digest and ReceiptTree on one view. It stages the
changed immutable 1 MiB `BlobChunkV1` objects, `UploadPartV1`, updated
`UploadProgressV1`/ReceiptTree objects, and selector expectation in one
publication. Replays with identical part identity are idempotent; changed
bytes, wrong offset, wrong size, wrong upload/path binding, missing/malformed
objects, and stale selector/epoch fail closed with no partial publication.

### Completion

Completion must call the authenticated `prepare_upload_completion` proof on the
same view, derive the canonical BlobId from all ordered authenticated chunks,
and append the final manifest plus visible file descriptor/blob-ref mutation to
one `PreparedPublication`. The upload selector is retired in that same plan;
ReceiptTree/part objects become W5-reclaimable only after the final root is
published. There is no intermediate prepared CAS receipt and no second file
writer.

### Abort and W5 handoff

Abort authenticates and deletes only the exact upload selector. It must not
delete shared chunk objects directly. W5 observes live final BlobId roots and
live upload selector closures, then reclaims unreachable object closure with
its own authenticated progress/owner fences. Shared chunks survive while any
selector or final file root retains them.

## Integrity and semantic obligations

The final W4 candidate must prove, before any payload write or visible-state
publication:

* `lix_binary_blob_ref` row key, file ID, declared size and visible BlobId bind
  to exactly one authenticated manifest edge;
* manifest object ID/domain, canonical BlobId, logical size, ordered chunk IDs,
  declared lengths and content digest authenticate before output/publication;
* every chunk object ID/domain and byte length/hash authenticate; missing,
  malformed, wrong-kind, cross-view, transplanted, same-size-substituted or
  wrong-domain objects fail closed;
* range reads load only intersecting authenticated 1 MiB chunks, but still
  validate manifest owner identity and range bounds on the same view;
* a valid same-length edit reuses unchanged chunks only after full named-base
  authentication; it hashes only changed chunks and never relies on a caller
  supplied BlobId or a cache;
* rollback, stale selector/CAS, concurrent GC, duplicate/replay, abort and
  completion failure leave zero partial visible publication and no orphaned
  authority; unreachable objects are W5 work, not ad-hoc delete fallback;
* 16 MiB public parts and 1 MiB internal chunks remain fixed and are checked
  across empty, exact-boundary, non-final, final, out-of-order and multi-part
  cases.

## Dependency-ordered production cut

### W4-A — finish the authenticated object lowerer

Use existing ForkTree object encoders/validators and the transaction's one
opening read. Lower normal `file_content_writes` before
`reject_not_yet_lowered_cohorts` returns the current error. Preserve state
descriptor/blob-ref/tombstone semantics and make the visible BlobId row the
sole state authority. This is the smallest cut that makes ordinary file writes
use the new owner.

Required callers: `file.rs:stage_lix_file_content_*`,
`transaction/staging.rs`, `transaction/commit.rs`, plugin auxiliary payload
lowering and `execute.rs` file APIs. Unsupported multi-branch/selected-history
cohorts remain typed fail-closed; no old file writer may serve them.

### W4-B — route multipart operations through selectors/objects

Replace `session/media_upload.rs:217` part staging, state/leaf encoders and
direct `prepare_write_set` with `publish_new_upload`/selector-progress
publication. Replace completion `:383-437` with the authenticated completion
proof and one final state transition. Implement abort as selector deletion and
leave physical object reclamation to W5.

### W4-C — replace reads and remove old CAS bridge

Route all file/plugin/archive/session reads through `CoherentView::bind_blob`
and its full/range loaders. Remove the old reader/writer bridge and all uses of
`BlobWriteReceipt`, `PreparedCas`, `stage_file_payload`, and
`execute_fast_lix_file_prepared_path_write`. Keep pure `BlobId`/`ChunkHash`
identity definitions only if they are rehomed under the sealed ForkTree blob
boundary; do not keep a compatibility `binary_cas` facade.

### W4-D — hand off reachability and delete legacy planes

After every reader/writer is migrated, remove the old upload spaces,
`binary_cas` module/reexports/metrics tied to the old plane, old storage-bench
scans, direct CAS publication hook, and SQL prepared-CAS export. Then hand the
OBJECT_SPACE/SELECTOR_SPACE root and selector closure to W5. No compatibility
reader, dual writer, migration path or alternate physical registry is allowed.

## Exact legacy deletion list and dependencies

| Delete/rehome only after | Exact paths/symbols | Dependency |
| --- | --- | --- |
| normal file lowerer lands | `transaction/commit.rs:reject_not_yet_lowered_cohorts` file-content rejection; `FileContent::PreparedCas`; `BlobWriteReceipt` | W4-A |
| all upload completion callers move | `session/media_upload.rs` old state/leaf constants and helpers; `stage_reclaimable_upload_receipts` | W4-B plus W5 |
| all readers/writers move | `binary_cas/context.rs` `ExistingChunkAwareBinaryCasWriter`, `stage_payload`, `stage_fixed_part`, `stage_fixed_manifest`, `stage_file_payload`; `binary_cas/mod.rs` reexports | W4-A/B/C |
| no upload/file path uses it | `transaction/context.rs:709` `stage_atomic_cas_publication` and all callers | W4-A/B; one transaction publication only |
| no SQL caller uses it | `sql2/providers/file.rs:2623` and `sql2/mod.rs:105`, `providers/mod.rs:48` exports | W4-B/C |
| W5 owns object accounting | `storage_bench.rs` direct `BINARY_CAS_*` scans and old CAS test helpers | W4-D/W5 |
| all old refs disappear | any absent `binary_cas/kv` references and `stage_mutation_epoch` calls | compiler-driven cleanup; never restore kv |

The `lix_binary_blob_ref` schema row itself is not deleted: its exact visible
BlobId/size semantics are the required state authority. Only the old physical
CAS writer/manifest registry and receipt bridge are deleted.

## Static compiler-deletion diagnostics (not run)

The assignment forbids a build, so no new compiler log is claimed. Exact
source inspection predicts these deterministic diagnostic classes on b484:

```text
E0433/E0583-style missing module/symbols:
  binary_cas/context.rs:92,135,153,188,204,217,231,244 -> binary_cas::kv
  session/media_upload.rs:337,963,987 -> binary_cas::stage_mutation_epoch
  session/media_upload.rs:884,1000,1493 -> BINARY_CAS_CHUNK_SPACE
  storage_bench.rs:1666,1703,1803,1806,2026,2104,2238,2241,2440
    -> BINARY_CAS_MANIFEST_SPACE/BINARY_CAS_CHUNK_SPACE

E0599-style constructor/associated-item residue:
  session/media_upload.rs:19-21 and gc.rs:45-74 -> removed StorageSpace::mutable
  related StorageSpaceId(...) calls -> private tuple constructor

Intentional runtime/compiler frontier:
  transaction/commit.rs:1182-1189 rejects nonempty file_content_writes before
  any ForkTree storage plan; this is the missing lowerer, not a fallback.
```

These are source-derived classifications, not execution evidence. The future
candidate must emit and attribute its exact compile frontier with isolated
targets before any adapter run.

## Future acceptance command order

The following commands are frozen but explicitly **UNRUN** in this package:

```bash
# source/provenance, candidate worktree only
git show -s --format='%H%n%T%n%P%n%s' b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
git diff --check
git grep -n -E \
  'ExistingChunkAwareBinaryCasWriter|stage_fixed_part|stage_fixed_manifest|\
   stage_atomic_cas_publication|execute_fast_lix_file_prepared_path_write|\
   UPLOAD_STATE_SPACE|UPLOAD_MANIFEST_LEAF_SPACE|BINARY_CAS_(CHUNK|MANIFEST)_SPACE' \
  -- packages/lix/src

# exact-SHA compile gates; use a separate target per candidate
CARGO_TARGET_DIR=/root/repos/target-w4-fileblob-memory \
  cargo check -p lix --lib --all-targets
CARGO_TARGET_DIR=/root/repos/target-w4-fileblob-memory \
  cargo test -p lix --lib --no-run
CARGO_TARGET_DIR=/root/repos/target-w4-fileblob-memory \
  cargo clippy -p lix --lib --all-targets -- -D warnings
```

Only after compile-green, run the same focused semantic package in this order:

```text
Memory -> RocksDB -> SlateDB
```

The first Memory cell must cover ordinary file insert/update/delete, explicit
empty and tombstone, 1 MiB boundaries, branch/diff/merge/history/checkpoint,
rollback, stale selector/CAS, cold reopen and malformed/missing/wrong-kind
manifest/chunk/BlobRef cases. The upload cells must cover 16 MiB public parts,
out-of-order four-part window, replay, abort, completion, missing/wrong-size/
wrong-hash/substituted part, and zero partial publication.

RocksDB then SlateDB must repeat the exact digest/corruption/reopen package and
record publication reads/writes/bytes, object counts, logical rows/bytes,
allocations/RSS and settled disk. Required accounting cells are explicit 1%
and 10% edits on 64 MiB content (plus 1 MiB boundaries), with checkpoint
publication followed by cold reopen. Valid reuse must show unchanged chunks
read/written only through authenticated object references; no payload value
reads for unchanged chunks. W5 separately owns shared/final-reference GC.

## Terminal classification

```text
b484 source: BLOCKED/compiler-red; no runnable W4 publication claim
new ForkTree blob/upload API: present but unwired to file_content_writes/media_upload
current live file/upload authority: legacy SQL + session/Binary CAS seams
smallest cut: W4-A normal file lowerer, then W4-B multipart/completion,
  W4-C reads/bridge deletion, W4-D legacy plane deletion and W5 handoff
runtime/build: UNRUN by instruction
```
