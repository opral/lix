# ForkTree Stage-2 compiler-hard-cut execution manifest

Status: **plan/test-only; production mutation is held**. This artifact does
not authorize an intermediate reader, writer, compatibility path, or compile.

## Frozen inputs and release conditions

- pinned current main: `e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7`
- pinned current-main tree: `ce241a0af016cadcb0c21d2d754eb3d4291cf79c`
- inventory predecessor: `b5e78190f49cab5de7bb19b6f967706c214363b6`
- approved unwired Stage 1: `138b55e1de90806c380ad27b2b349f4c66a1387f`
- approved Stage-1 tree: `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`
- corrected typed-owner app-oracle transport: `5a6a2cb037668c8dc6256d9b0975d0b39068f07a`
- app-oracle tree: `47169206cb7822937464a873f27c2bd41d8e98c2`
- independently built app-oracle binary SHA-256:
  `205ff27fbbc36acfff4ef2c02fe0ae6732687b1f3fa97eb7b09cd097664eb5a3`
- terminal app-oracle report SHA-256:
  `ffa367b9844051c952bfbee0d067cdde5cefe8a2125f9845152600d1694a70cc`
- frozen Stage-2 public-semantics acceptance harness:
  `cd15ffc725dfc2e65b7a6d3829e1fcb754894a79`, tree
  `ca878606f09401db77a4dd802057536a6f2dea42`; full-index diff SHA-256
  `85facdd7bf7bf9dbde674ae7fe06d7f8aa64836412f1fc4f5a40bfbab35f1b7f`,
  source SHA-256
  `d01b3727248e6d6d2c6fbaed80279b0e28a3d0b8c2fa56666b0221e0e98c9967`,
  binary SHA-256
  `f5c76345a1735aafee14f8055f8783f0dbfdc3e610c7651ea7290e8e9ab8798c`
- rejected-for-production ReaderLease research evidence (safe model, but no
  current public cross-handle identical-view requirement):
  `ac23754c8ba4a943e69da1304e371d8416456f1b`, tree
  `156656c53b0193f6090e62bf652454f80fe461ac`, branch
  `codex/forktree-reader-lease-gc-guardrail-138b`; canonical full-index diff
  SHA-256
  `204771e0aff92e15dad38d80391bbd072952c0dec9983139404f5487760355db`,
  report blob SHA-256
  `7e304627bd11a4cd3f41435f26a9d088bf36e0c3ccc65433b240652ca85d3745`
- common architecture base: `8e3ffe632bc27e1ab84fe9a6102b099ab2e9f441`
- read-only e871+138b conflicted merge index/tree:
  `f1fe3c03cbecaaa384611843d4727499f6500ed6`

The prospective e871+138b merge has seven real textual conflicts:
`binary_cas/kv.rs`, `branch/control.rs`, `init.rs`, `storage_bench.rs`,
`transaction/plugin_checkpoint.rs`, `rs-sdk-tests/tests/e2e.rs`, and
`server-protocol/src/lib.rs`. Its production changed-path intersections include
`binary_cas/kv.rs`, `branch/control.rs`, `gc.rs`,
`live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`,
`session/media_upload.rs`, `storage_bench.rs`,
`tracked_state/current_state_data_part.rs`, `tracked_state/storage.rs`, and
`transaction/commit.rs`, plus #1244's `init.rs` and
`transaction/plugin_checkpoint.rs`. The production wave must preserve
e871/#1258's
authenticated CAS/retention semantics while deleting its superseded physical
authority; preserve #1244's authenticated-control corruption semantics while
deleting that physical authority; and re-express the deterministic-sequence
untracked closure before deleting the hot/current path.

## Frozen concurrent-work conflict map

The following remote heads were fetched read-only while this artifact was
prepared. None is copied into this branch, and this plan does not decide whether
an unmerged PR lands. The production cut consumes only the then-current main in
its one authorized merge.

| Lane | Exact reviewed head/tree | Base/merge/path result | Stage-2 disposition |
|---|---|---|---|
| #1244 authenticated branch-control/plugin checkpoint | merged as current main `e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7`; second parent `aa9d1377ef8b6856362a26a96bc0d1d250899e4f`, tree `ce241a0af016cadcb0c21d2d754eb3d4291cf79c` | no durable-space-count change. Exact b5e..e871 adds branch-ID-bound `LBC1`/`branch.head_control.v10`, branch+file-bound `LPC3`/`plugin.current_checkpoint.v2`, repository protocol V61, and fail-closed public corruption propagation. Stage1 intersects `branch/control.rs`, `init.rs`, `storage_bench.rs`, `transaction/plugin_checkpoint.rs`, `rs-sdk-tests/tests/e2e.rs`, and `server-protocol/src/lib.rs`. | Delete `BRANCH_HEAD_CONTROL_SPACE`, `PLUGIN_CHECKPOINT_SPACE`, both files/codecs/magic/domain strings, and V61 old-layout marker before C1. Preserve semantics with authenticated BranchSelector/BranchSnapshot binding and a typed plugin-checkpoint object edge bound to branch/file/generation/blob/semantic root. A present corrupt object fails closed; only an authenticated owner/version mismatch is a cache miss. Branch retirement removes the edge and ordinary object GC handles final references. Public server protocol behavior remains unchanged. |
| #1258 binary-CAS GC | ancestor `b5e78190f49cab5de7bb19b6f967706c214363b6`; second parent `a6cb5d8c316d166b4a4eb5e5c3fc50d04d57774d`, tree `c913465505bc773d21a6e2804530287ee937a3f1` | the e871+Stage1 prospective merge still conflicts in `binary_cas/kv.rs` and `storage_bench.rs`; its other physical intersections remain semantic hot spots. | Preserve authenticated serving-closure role separation, declared-size/delta/base validation, full-queue roots, retained-history/final-reference semantics, true shared chunks, and both upload/publication race orders in the new object owner. Resolve `binary_cas/kv.rs` by moving callers to typed ForkTree manifests/chunks and deleting the file, not by retaining either physical implementation. Rewrite the bench/oracle to the new owner before deleting old accounting. |
| #1260 SQL write owner | `7061aad7f4b14e611b32bbe5493f39253b826378` / `d41598c18afae0b6a9c675fb8be3b263000da67a` | based directly on 476; e871 prospective merge is clean at tree `2ae6ffd8faef595ca9bf2e60447ef31a8922b92f`. Its only Stage1 intersection is `rs-sdk-tests/tests/e2e.rs`; production changes are confined to `sql2` plus that test. | Do not edit its SQL files. ForkTree exposes only typed transaction/publication/range-projection capabilities. The existing binder remains semantic plan authority; SQL integration is coordinated after its owner freezes. Test overlap is reconciled in qualification, not by changing #1260 here. |
| Storage streaming cursor | mutable review checkpoint `770d73c17afd4d3a569b31820696fe28b65e25d3` / `aa2de4a32d2d0bf33375e476d8c34c9dfd993eaf`; **not an acceptance head** | based on 476 tree `a2a261220fb08f88ac44ca7776b2bc7ba7d6441c`; exact library build and Stage-2 shape/lifetime/old-API probes pass. `git diff --binary` reproduces advertised `b815837f...`; canonical `--binary --full-index` is `d28aec542dccb919d1eb94d268c4e3e2e3f0358409982af8d3370f142629d190`. | Hetzner-IV exclusively owns traits, adapters, scan migration, and deletion of the old scan API. Stage 2 starts only after its immutable head is independently green and merged; the one main merge consumes the final names. This branch never edits those files or restores an old scan wrapper. |

The landed #1244 benchmark additions remain outside this artifact's source scope.
#1260 is textually clean only because its reviewed head is based on the 476
predecessor; that is not a promise about the eventual then-current main. Any
new overlap is resolved once, in the production worktree, by preserving the
semantic contracts above and never by retaining a legacy authority.

### One-merge conflict resolution order

| Current-main/Stage1 hot path | Resolution in the non-runnable wave |
|---|---|
| `binary_cas/kv.rs` | Do not choose either physical side. First move semantic blob callers to typed ForkTree manifest/chunk operations, preserve #1258 size/delta/base/final-reference validation, then delete the file before C1. |
| `storage_bench.rs` | Preserve public benchmark entry points only long enough to compile their callers; replace old-space counters with the accounting contract below, then remove every legacy space/layout symbol. |
| `branch/control.rs` | Preserve #1244's branch-ID-bound authenticated selector semantics and fail-closed corruption through BranchSelector/BranchSnapshot/typed roots, then delete `LBC1`, v10 control bytes, queue coupling and module. |
| `init.rs` | Retain a single hard-cut repository protocol marker for ForkTree only. Delete V61 old-layout value/decoder and initialize only the three accepted ForkTree spaces plus independent spaces. Never accept or migrate v61 bytes. |
| `transaction/plugin_checkpoint.rs` | Move the derived checkpoint into an authenticated typed object edge bound to branch/file/generation/blob/semantic root; preserve present-corrupt fail-closed and authenticated mismatch-as-cache-miss behavior, then delete `LPC3`, v2 space and module. |
| `server-protocol/src/lib.rs` | Preserve e871 public wire/session/error behavior and consume only unchanged public Lix APIs; no physical ForkTree or old checkpoint/control codec may enter the server. |
| `rs-sdk-tests/tests/e2e.rs` | Reconcile tests only after public paths use ForkTree. Retain cold-reopen branch-control and plugin-checkpoint corruption assertions against typed owner corruption, not old space names. |
| `gc.rs` | Keep only the public maintenance facade and status/error mapping. Move all root discovery, role validation, mark packs, cursor enumeration and delete authority behind ForkTree; delete #1258's now-superseded physical closure without weakening it. |
| `live_state/tracked_head.rs` and `tracked_head/hot.rs` | Re-express e871 deterministic untracked collection membership and current tracked/untracked precedence through typed owner calls, switch every reader, then delete both physical modules. |
| `session/media_upload.rs` | Replace old upload rows/leaves with UploadSelector/Progress/ReceiptTree/Part and the common publication fence; preserve completion/abort and both GC race orders. |
| `tracked_state/{current_state_data_part,storage}.rs` | Move retained-history/current readers and publication callers to typed roots/catalogs, then delete the files; no replay or rebuild compatibility survives. |
| `transaction/commit.rs` | Writer cut last: preserve validation/order/public CommitId/ChangeId consumption while replacing staged old spaces with one PreparedPublication and owner/global preconditions. |

The merge itself resolves only text needed to form the private worktree. These
semantic resolutions occur inside the single compiler wave; no merged legacy
implementation is compiled or used as an intermediate authority.

The current e871 + reviewed #1260 union is clean at prospective tree
`2ae6ffd8faef595ca9bf2e60447ef31a8922b92f`. This is evidence-only and no ref
is published. The final production worktree still consumes only then-current
main once, after the cursor merge.

The first event is now terminal GREEN: Memory, RocksDB and SlateDB passed all
five typed-owner hot/cold cases plus reopen, state/catalog, upload/GC,
shared/final references, both races and corruption fail-closed; both Clippy
invocations and fmt/diff also passed. Production release still requires both
events to be recorded in the final manifest:

1. **GREEN** -- Hetzner-III terminal dual-adapter qualification for exact oracle
   `5a6a2cb` (facts and hashes above).
2. Hetzner-IV's no-compatibility StorageRead streaming-cursor PR is
   independently approved and merged.

Ryzen-IV's public-resume audit is terminal: no JS/SDK/server/SQL token promises
the identical view after its originating handle/transaction is gone. The first
runnable source therefore uses only the native live-handle snapshot pin and
must contain no ReaderLease selector, codec, publication, GC root/accounting,
or lease-driven global rotation. `ac23754c` remains research evidence only for
a future public feature and is not a Stage2 production seam.

After both, create one production worktree from approved Stage 1 and merge the
then-current `origin/main` exactly once. Do not rebase or repeatedly merge.
That one merge must include the final cursor API. Run the owner seal and app
oracle before entering the deliberately non-runnable wave. If either fails,
stop; do not repair it with a compatibility layer.

## End-state authority

The first runnable source has exactly these ForkTree physical spaces:

- `0x0009_0001 forktree.object.v1`: authenticated immutable objects;
- `0x0009_0002 forktree.selector.v1`: authenticated mutable selectors and GC
  ordering state; and
- `0x0009_0003 forktree.untracked_row.v1`: semantically untracked rows only.

The first space owns tree nodes, leaves/value packs, Commit/Change/RefChange
objects, catalogs, branch/snapshot targets, blob manifests/chunks, upload
parts/ReceiptTrees, and GC maintenance objects. The selector plane owns one
global repository selector, branch/upload/checkpoint/recovery/undo/redo/GC
selectors and generations. It is not a row or payload authority. No reader
lease selector exists.

Object identity is the canonical object-domain hash and never includes an SST,
extent, pack, or physical placement. Any future locator is rebuildable and
cannot authorize reads, writes, roots, publication, or GC. Active
`StorageRead` snapshots are the process-local reader pin. Pagination remains
valid only while the originating coherent handle/session owns that exact view;
after drop or expiry it fails closed as `ReadExpired`/`InvalidCursor`. Logical
deletion commits under exact GC progress; physical reuse
waits for the adapter read low-watermark. No process pin registry, clock grace,
opaque persisted backend snapshot, or out-of-band object delete is allowed.

`ac23754c` is retained solely as proof that a durable design is possible if a
future public feature requires it. Reintroducing it requires a new architecture
decision; the current residue oracle rejects its selectors/codecs/accounting.

## Exact durable-space disposition on e871

There are 49 production space constants: 41 superseded authorities, six
semantically independent spaces, and two rebuildable revision spaces.

### Delete before first compile: 41 superseded spaces

| Family | Exact spaces |
|---|---|
| tracked/current (23) | `TRACKED_STATE_TREE_CHUNK_SPACE`, `TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE`, `TRACKED_STATE_CHANGE_LOCATOR_SPACE`, `TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE`, `TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE`, `MUTATION_DIRECTORY_NODE_SPACE`, `SCOPED_RANGE_NODE_SPACE`, `CURRENT_STATE_DATA_PART_SPACE`, `CURRENT_STATE_DATA_PART_REFS_SPACE`, `HOT_ROW_SPACE`, `HOT_FILE_SPACE`, `HOT_DIFF_SPACE`, `HOT_COLLECTION_CONTROL_SPACE`, `PACKED_CURRENT_BASE_SPACE`, `PACKED_CURRENT_BASE_CONTROL_SPACE`, `PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE`, `ROOT_CURRENT_BASE_SPACE`, `TRACKED_WORKING_DIFF_MARKER_SPACE`, `CERTIFIED_ENTITY_BATCH_SPACE`, `CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE`, `CERTIFIED_ENTITY_BATCH_PAGE_SPACE`, `ROW_GROUP_MANIFEST_SPACE`, `ROW_GROUP_COLUMN_SPACE` |
| changelog (3) | `COMMIT_SPACE`, `CHANGE_SPACE`, `COMMIT_CHANGE_ID_SPACE` |
| branch control (1) | `BRANCH_HEAD_CONTROL_SPACE` |
| binary CAS (4) | `BINARY_CAS_MANIFEST_SPACE`, `BINARY_CAS_MANIFEST_CHUNK_SPACE`, `BINARY_CAS_CHUNK_SPACE`, `BINARY_CAS_CHUNK_PRESENCE_SPACE` |
| multipart (2) | `UPLOAD_MANIFEST_LEAF_SPACE`, `UPLOAD_STATE_SPACE` |
| checkpoint/GC (7) | `CHECKPOINT_GC_STATE_SPACE`, `CHECKPOINT_RECOVERY_REF_SPACE`, `GC_REACHABILITY_DELTA_SPACE`, `GC_REACHABILITY_QUEUE_SPACE`, `GC_TREE_SWEEP_CURSOR_SPACE`, `GC_TREE_SWEEP_EPOCH_SPACE`, `GC_TREE_SWEEP_MARK_SPACE` |
| plugin checkpoint (1) | `PLUGIN_CHECKPOINT_SPACE` |

`HOT_ROW_SPACE` is deleted, not narrowed in place. Semantically untracked rows
move to the already sealed `UNTRACKED_ROW_SPACE`; tracked rows cannot enter it.

### Retain only independent semantics

- `JSON_SPACE`: out-of-line JSON bytes only.
- `UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE`: rebuildable untracked-payload
  ownership-loss hints only.
- `TRACKED_MUTATION_REVISION_SPACE`: invalidation signal, never state/root.
- `EXECUTE_IDEMPOTENCY_RECEIPT_SPACE`: execute idempotency only.
- `FILESYSTEM_PATH_REVISION_SPACE`: filesystem invalidation only.
- `REPOSITORY_PROTOCOL_SPACE`: hard-cut format gate only.
- `MUTATION_REVISION_SPACE` and `CATALOG_REVISION_SPACE`: rebuildable
  revisions; deleting/rebuilding them cannot change semantics.

## Direct space/path deletion ledger

Every listed production occurrence must disappear. Paths that remain as
public facades must no longer contain the token or decode the old bytes.

| Space | e871 production paths containing it |
|---|---|
| `BINARY_CAS_MANIFEST_SPACE` | `binary_cas/context.rs`, `binary_cas/kv.rs`, `binary_cas/mod.rs`, `binary_cas/stats.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `BINARY_CAS_MANIFEST_CHUNK_SPACE` | `binary_cas/kv.rs`, `binary_cas/mod.rs`, `binary_cas/stats.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `BINARY_CAS_CHUNK_SPACE` | `binary_cas/kv.rs`, `binary_cas/mod.rs`, `binary_cas/stats.rs`, `session/media_upload.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `BINARY_CAS_CHUNK_PRESENCE_SPACE` | `binary_cas/kv.rs`, `binary_cas/mod.rs`, `binary_cas/stats.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `BRANCH_HEAD_CONTROL_SPACE` | `branch/control.rs`, `branch/mod.rs`, `storage_bench.rs`, `tracked_state/storage.rs`, `transaction/commit.rs` |
| `CERTIFIED_ENTITY_BATCH_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs` |
| `CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE` | same four paths as `CERTIFIED_ENTITY_BATCH_SPACE` |
| `CERTIFIED_ENTITY_BATCH_PAGE_SPACE` | same four paths as `CERTIFIED_ENTITY_BATCH_SPACE` |
| `CHANGE_SPACE` | `changelog/context.rs`, `changelog/materialization.rs`, `changelog/mod.rs`, `changelog/store.rs`, `commit_graph/context.rs`, `gc.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `CHECKPOINT_GC_STATE_SPACE` | `gc.rs`, `tracked_state/storage.rs` |
| `CHECKPOINT_RECOVERY_REF_SPACE` | `gc.rs`, `tracked_state/storage.rs` |
| `COMMIT_CHANGE_ID_SPACE` | `changelog/context.rs`, `changelog/mod.rs`, `changelog/store.rs`, `gc.rs`, `sql2/providers/change.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `COMMIT_SPACE` | `changelog/context.rs`, `changelog/mod.rs`, `changelog/store.rs`, `commit_graph/context.rs`, `commit_graph/walker.rs`, `engine.rs`, `gc.rs`, `storage_bench.rs`, `tracked_state/context.rs`, `tracked_state/storage.rs` |
| `CURRENT_STATE_DATA_PART_SPACE` | `gc.rs`, `session/execute.rs`, `tracked_state/current_state_data_part.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs` |
| `CURRENT_STATE_DATA_PART_REFS_SPACE` | `gc.rs`, `tracked_state/current_state_data_part.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs` |
| `GC_REACHABILITY_DELTA_SPACE` | `gc.rs` |
| `GC_REACHABILITY_QUEUE_SPACE` | `gc.rs` |
| `GC_TREE_SWEEP_CURSOR_SPACE` | `gc.rs`, `storage_bench.rs` |
| `GC_TREE_SWEEP_EPOCH_SPACE` | `gc.rs`, `storage_bench.rs` |
| `GC_TREE_SWEEP_MARK_SPACE` | `gc.rs`, `storage_bench.rs` |
| `HOT_COLLECTION_CONTROL_SPACE` | `live_state/tracked_head/hot.rs` |
| `HOT_DIFF_SPACE` | `engine.rs`, `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `HOT_FILE_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs`, `tracked_state/storage.rs`, `transaction/commit.rs` |
| `HOT_ROW_SPACE` | `engine.rs`, `functions/state.rs`, `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs`, `tracked_state/storage.rs`, `transaction/commit.rs` |
| `MUTATION_DIRECTORY_NODE_SPACE` | `gc.rs`, `storage_bench.rs`, `tracked_state/mod.rs`, `tracked_state/mutation_directory.rs`, `tracked_state/storage.rs` |
| `PACKED_CURRENT_BASE_CONTROL_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs` |
| `PACKED_CURRENT_BASE_SPACE` | `engine.rs`, `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs`, `transaction/commit.rs` |
| `PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs` |
| `PLUGIN_CHECKPOINT_SPACE` | `storage_bench.rs`, `transaction/plugin_checkpoint.rs` |
| `ROOT_CURRENT_BASE_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs` |
| `ROW_GROUP_COLUMN_SPACE` | `columnar_row_group.rs`, `live_state/entity_decoded_column_cache.rs` |
| `ROW_GROUP_MANIFEST_SPACE` | `columnar_row_group.rs`, `session/execute.rs` |
| `SCOPED_RANGE_NODE_SPACE` | `gc.rs`, `tracked_state/mod.rs`, `tracked_state/scoped_range.rs`, `tracked_state/storage.rs` |
| `TRACKED_STATE_CHANGE_LOCATOR_SPACE` | `storage_bench.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs` |
| `TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE` | `commit_graph/context.rs`, `storage_bench.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs` |
| `TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE` | `storage_bench.rs`, `tracked_state/context.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs` |
| `TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE` | `commit_graph/context.rs`, `gc.rs`, `storage_bench.rs`, `tracked_state/context.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs`, `transaction/commit.rs` |
| `TRACKED_STATE_TREE_CHUNK_SPACE` | `gc.rs`, `storage_bench.rs`, `tracked_state/context.rs`, `tracked_state/mod.rs`, `tracked_state/storage.rs`, `tracked_state/tree.rs`, `transaction/commit.rs` |
| `TRACKED_WORKING_DIFF_MARKER_SPACE` | `live_state/mod.rs`, `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`, `storage_bench.rs`, `tracked_state/storage.rs` |
| `UPLOAD_MANIFEST_LEAF_SPACE` | `session/media_upload.rs` |
| `UPLOAD_STATE_SPACE` | `session/media_upload.rs` |

## Module and function disposition

### Delete physical modules

Before the first compile, delete:

```text
tracked_state/storage.rs
tracked_state/tree.rs
tracked_state/codec.rs
tracked_state/mutation_directory.rs
tracked_state/scoped_range.rs
tracked_state/scoped_current_state.rs
tracked_state/current_state_data_part.rs
tracked_state/current_state_envelope.rs
tracked_state/commit_root_rebuild.rs
tracked_state/replacement_part.rs
live_state/tracked_head.rs
live_state/tracked_head/hot.rs
columnar_row_group.rs
changelog/store.rs
changelog/codec.rs
branch/control.rs
commit_graph/walker.rs
binary_cas/kv.rs
binary_cas/codec.rs
binary_cas/chunking.rs
binary_cas/stats.rs
transaction/plugin_checkpoint.rs
storage_adapter/scan.rs                 # deleted by cursor PR, never restored
```

`tracked_state/{context,diff,merge,row_materialization,types,mod}.rs`,
`changelog/{context,materialization,types,mod}.rs`, `commit_graph/context.rs`,
`binary_cas/{context,types,mod}.rs`, `live_state/{context,reader,types,mod}.rs`,
`session/media_upload.rs`, and `gc.rs` may retain public semantic facades only
after their physical implementation is replaced. They may not decode or name
an old space.

### Tracked/current reader and helper closure

Delete the old implementations of these exported operations (private helpers
die with their modules):

- root/manifest: `load_root`, `load_snapshot_commit_root`,
  `load_commit_state_manifest`, `load_published_commit_state_manifest`,
  `load_published_commit_state_topology`, `load_commit_state_manifests`,
  `load_commit_state_authority_ids`, `load_commit_mutation_directory_roots`;
- delta/locator reads: `load_change_record_by_id`,
  `load_commit_delta_change_records`, `load_commit_delta_members_with_payloads`,
  `load_commit_delta_replay_metadata`, `load_owned_commit_delta_entries`,
  `scan_commit_delta_inventory`, `scan_commit_delta_values`,
  `scan_change_records_from_commit_deltas`;
- physical writes: `stage_commit_state_manifest*`,
  `stage_addressable_commit_deltas*`,
  `stage_ordered_addressable_commit_deltas`,
  `stage_ordered_addressable_replacement_parts*`,
  `stage_ordered_columnar_mutations`, `stage_change_locators`,
  `stage_current_state_scoped_ranges_*`,
  `stage_delete_commit_state_manifest_for_gc`;
- tree/directory/current part: `TrackedStateTree::{load_root,get,get_many,scan,diff,apply_mutations,merge_and_stage_ordered_parent_mutations}`, `build_mutation_directory`, `load_mutation_part_read_plan`, scoped-range routing/validation, current-state part/envelope codecs and row materialization from index entries;
- tracked hot/base/certified helpers: `TrackedHeadContext`,
  `HotStateStoreReader`, `HotStateWriter`, `HotTrackedSnapshot`,
  `stage_certified_entity_batches`, `scan_certified_history_rows`, packed/root
  current-base readers and writers, HOT file/row/diff/control scanners, and
  stale-generation collectors.

Their replacement is `CoherentView` plus `state_point`, `state_range`,
`edit_state_tree`, owner-authenticated object loading and batched ordered range
projection. Current points are `O(log_F N)`; ranges are
`O(log_F N + blocks + output)`, with local value > global, local tombstone
suppression, absence fallback, and NULL as a value.

The e871 deterministic-sequence closure must move from
`BranchHeadControlContext` + `HOT_ROW_SPACE` +
`validate_exact_collection_closure` to one authenticated untracked owner
operation. It must preserve canonical member identity, authenticated empty
collection, lifecycle generation, schema-presence validation, and fail-closed
same-count substitution. It cannot become a tracked root or an extra control.

### Persisted working diff

Move every reader first, then delete these exact owner symbols:

- `TrackedWorkingDiff`, `TrackedWorkingDiffEpoch`,
  `WorkingDiffIndexCoverage`;
- `working_diff_for_control`, `working_diff_epoch`,
  `hot_working_diff_entries*`, `choose_hot_or_packed_working_diff`;
- `stage_tracked_working_diff_epoch`,
  `stage_delete_tracked_working_diff_epoch`,
  `stage_active_working_diff_scopes`;
- `stage_commit_with_working_diff`,
  `stage_current_state_with_working_diff`,
  `stage_complete_current_state_with_working_diff`, and
  `stage_checkpoint_working_diff_epochs`.

Consumers to switch before writer deletion are `transaction/context.rs`,
branch switch/create/no-op, merge analysis/publication, checkpoint,
undo/redo/recovery, observe, `sql2/providers/{working_diff,filesystem_working_diff}.rs`,
catalog registration, and storage benches. Replacement is selected-root versus
transaction-local/root-to-root authenticated diff. The writer and marker space
are deleted only after every reader has moved.

### Changelog, graph, and branch control

Delete `ChangelogStoreReader`, `ChangelogStoreWriter`,
`ChangelogStorageRead`, `stage_transaction_append`, old `native_scan`,
commit/change/commit-change-ID keys and codecs, and
`BranchHeadControl{Context,Reader,Observation}` plus
`stage_branch_head_control`, `stage_delete_branch_head_control`, and
`branch_head_control_precondition`. Delete #1244's
`BRANCH_HEAD_CONTROL_NAMESPACE`, `LBC1` envelope, branch-ID keyed digest and
corruption codec as physical implementation, not as a compatibility decoder.

Switch exact and resumed reads in `changelog/context.rs`,
`commit_graph/context.rs`, `checkpoint.rs`, history/diff providers, branch
refs/lifecycle, merge-base/history, GC, observe, engine initialization and
transaction context to one `CommitCatalog` and one unified `ChangeCatalog`.
Exact and ordered lookup use the same raw UUID tree. Commit-member ownership is
only the catalog back-edge to Commit object+ordinal; standalone RefChange is in
the same ChangeCatalog. `BranchSelectorV1` carries no ChangeId.

Branch/current reads use exactly one `begin_read`, one same-handle selector
`get_many`, and same-handle object traversal. `view_id` hashes the exact raw
global+branch selector pair and binds transactions, pagination and observe.

### Binary CAS and multipart upload

Delete old `BinaryCasManifest`, manifest/chunk/presence codecs and KV helpers,
`ExistingChunkAwareBinaryCasWriter`, FastCDC/flat-delta persisted formats,
`stage_manifest`, `stage_manifest_chunk`, `stage_chunk`,
`scan_manifest_chunks`, old presence probes, and whole-payload assembly paths.
The e871/#1258 physical facade also disappears:
`BinaryCasGcSweep`, `load_mutation_epoch`, `stage_mutation_epoch`,
`stage_gc_reclamation`, and `stage_reclaim_unreachable_binary_cas`. Their
authenticated-size/delta/base and final-reference rules are implemented once
by typed object publication and reachability; the old CAS epoch is not retained
beside the admitted owner-local conflict/GC-generation plane.
Keep public `BlobDataReader`/file APIs only as facades over authenticated
ForkTree manifests/chunks and segmented outer-consumer materialization.

Delete multipart `UploadState`, `UploadManifestLeaf`,
`load/stage_upload_state`, `load/stage_upload_manifest_leaf`, cumulative leaf
scans, and their codecs. Rewrite `SessionContext::upsert_file_content_part`
and completion through
`UploadSelector -> UploadProgress -> ReceiptTree -> UploadPart -> chunks`.
Part publication is `O(part bytes + log_F P + new chunks)`; completion is
streaming `O(P + touched chunks)`. Completion atomically moves reachability
from receipt to file state. There is no predecessor, cumulative list, second
presence authority, alternate chunker, or contiguous internal fallback.

Retain the accepted fixed multimedia shape: fixed 1 MiB leaves, internal
fanout 64, authentication window 8, one immutable object authority. Preserve
the one-copy borrowed/sliced extent seam without adding a locator authority.

### Checkpoint, recovery, retention, plugin and GC

Delete the old structs and operations `CheckpointRecoveryRef`,
`CheckpointGcState`, `RootReachabilityDelta`, `TreeSweepEpochSession`,
`stage/load_checkpoint_gc_state`, `stage_recovery_ref_rotation`,
`stage_reachability_*`, `begin/open/stage_tree_sweep_epoch*`, old mark/queue
codecs, full-recovery global sets, and plugin checkpoint rows.

#1244's `plugin.current_checkpoint.v2`/`LPC3` bytes are also deleted. Preserve
their serving rule in one typed immutable checkpoint object edge: authenticate
branch/file owner, generation, blob hash, semantic root, lengths, runtime and
authority before use. Present malformed/substituted data fails closed; only an
authenticated expected-owner/version mismatch is a cache miss. Branch
retirement removes the selected edge and shared object bytes survive until the
last current/retained root releases them. The old V61 repository marker is
rejected by the new ForkTree protocol hard cut and is never migrated.

Current e871 additionally deletes the physical-retention implementation
`AuthenticatedServingDependencyClosure`,
`load_authenticated_serving_dependency_closure`,
`load_authenticated_repository_retention`,
`collect_active_point_replay_dependencies`, `fold_reachability_batches`,
`collect_all_reachability_checkpoint_roots`, `RetainedCommitSnapshot`,
`load_retained_commit_snapshots_for_schemas`,
`load_local_selected_change_owner_commit_ids`,
`collect_gc_binary_blob_roots`, `collect_gc_wasm_blob_roots`, and
`stage_reclaimable_upload_receipts`. These are not discarded semantically:
their role separation, full-queue checkpoint roots, current untracked/file and
plugin roots, upload receipts, historical replay roots, declared sizes, and
final-reference rules become typed edges in the one selected graph and bounded
V2 progress. `gc.rs` may consume only owner-produced typed pages/summaries.

Checkpoint/recovery/undo/redo become typed `SnapshotSelectorV1` roots.
Foreground readers retain their one coherent `StorageRead`; that handle's
native snapshot is the reader pin, and a token cannot resume after the handle
or transaction drops. ReaderLease is absent from the root universe.
Installed plugin WASM is an ordinary manifest edge from current and retained
registry objects. The owner traverses the complete selected graph; `gc.rs`
receives bounded typed progress/status only and never decodes owner internals.
Mark/queue/radix/live-branch packs remain rebuildable maintenance objects in
the same object space under `GcProgressSelectorV2`.

## Every old low-level scan consumer on e871

The cursor PR deletes the old `StorageRead::scan`, `ScanOptions`,
`StorageScanOptions`, adapter resume helpers, and all production or test resume
loops. New `ScanChunk` is the cursor page value. `ScanPlan`, `ScanPlanCursor`
and their module are deleted completely so no wrapper operation can recreate
an iterator. Stage 2 must never restore them.

| Disposition | Exact e871 functions |
|---|---|
| deleted with binary CAS | `binary_cas/kv.rs::{load_declared_manifest_chunks,load_declared_manifest_chunk_range,scan_all_values,scan_all_values_for_plan}`, `binary_cas/stats.rs::scan_space` |
| deleted with branch/changelog | `branch/control.rs::BranchHeadControlReader::scan`, `changelog/context.rs::native_scan`, `engine.rs::repository_has_changelog_commit` |
| deleted with old GC | `gc.rs::{scan_tree_sweep_marks,stage_tree_sweep_epoch_page,load_recovery_refs,stage_sweep_unreachable_content_nodes}` plus indirect branch-control scans in `load_tree_sweep_root_closure`, `audit_repository_gc_standalone_refs`, `stage_repository_gc_with_preconditions`, `stage_repository_gc_full_recovery` |
| deleted with old tracked/current | `tracked_state/storage.rs::{visit_change_records_from_commit_deltas,validate_no_orphan_commit_delta_segments,scan_full_space}`, `live_state/tracked_head.rs::stage_active_working_diff_scopes`, and `live_state/tracked_head/hot.rs::{stage_certified_entity_batches,scan_certified_entity_batch_rows,scan_certified_history_rows,packed_exclusive_schema_base_refs,packed_current_base_refs,stage_retire_packed_current_bases,scan_root_current_base_rows_for_merge,validate_exact_collection_closure,has_schema_rows,untracked_json_refs,hot_load_file_scope_identities,hot_working_diff_entries,hot_scan_entries,hot_scan_dense_encoded_key_range,scan_hot_file_entries,stage_collect_stale_hot_collection_controls,stage_collect_stale_hot_space,stage_collect_stale_hot_diff_records,stage_delete_hot_diff_scope}` |
| deleted with old upload/plugin | `session/media_upload.rs::{load_upload_progress,load_upload_manifest_leaves}`, `transaction/plugin_checkpoint.rs::stage_delete_branch_plugin_checkpoints` |
| H4 cursor-PR ownership | `storage/traits.rs::StorageRead::scan`, `storage_adapter/{context,read_scope,scan}.rs` wrappers, Memory/Rocks/Slate implementations, conformance/model/failure tests, all adapter resume state and bounds helpers. `ScanPlan`/`ScanPlanCursor` are deleted completely. |
| retained independent but migrated by H4 | test-only `json_store/context.rs::scan_untracked_reclaim_candidates`; no Stage-2 implementation may restore its old loop |

## Precise cursor consumption points after the cursor PR

The final API names are bound to Hetzner-IV's immutable cursor head before the
production merge. The mutable checkpoint compiles with:
`StorageRead::begin_scan(space, range, BeginScanOptions)`,
`ScanCursor::next_page(limit_rows)`, and ascending/descending `ScanOrder`.
All three adapters deterministically reject `Descending` as
`Unsupported(ReverseScan)`; the four owner drains require only ascending order.
The final cursor contract (being completed after the mutable checkpoint)
poisons `ScanCursor` before polling its source and clears poison only after a
validated page: cancellation during an await, backend failure, or malformed /
non-increasing page terminally returns `InvalidCursor`, while cancellation
before the first poll leaves it untouched. An explicit
`Bound::Excluded(last_authenticated_delivered_key)` on a fresh coherent view is
the only restart cursor. This plan freezes semantics and call sites; final implementation and
provenance still await the immutable cursor head.

Only these ForkTree owner functions consume the storage-space cursor:

| Owner function | Space/range | Cursor contract | Durable restart |
|---|---|---|---|
| `reachability::advance_selector_roots` | `SELECTOR_SPACE`, ascending typed-selector range | one cursor on the cycle's coherent `StorageRead`; authenticate each durable owner selector before yielding roots; repeatedly request bounded pages | `GcProgressV2.selector_resume_after`, used only as the next scan's exclusive lower bound after commit/crash |
| `reachability::advance_untracked_roots` | `UNTRACKED_ROW_SPACE`, ascending full/key projection required by root extraction | one cursor, validate branch/key/value ownership and manifest roots before marking | `untracked_resume_after` |
| `reachability::advance_sweep` | `OBJECT_SPACE`, ascending object IDs | sorted object/mark merge; authenticate object key/domain before at most 256 owner-produced deletes; repeat pages on one read instead of rebuilding an iterator | `object_resume_after` |
| `reachability::advance_cleanup` | `OBJECT_SPACE`, maintenance-domain range | delete only unreachable/superseded maintenance packs from the completed cycle; semantic corruption never authorizes deletion | `maintenance_resume_after` |

At every page checkpoint, retain only the last authenticated key in canonical
`GcProgressV2`, exact-CAS the cycle's unchanged fenced raw global plus old
GC-progress selector, and commit.
During a healthy run, keep that phase's original coherent `StorageRead` and
live cursor across those page commits so the native iterator advances without
LSM reconstruction. That enumeration view is immutable and is never asked to
read maintenance packs written after it opened. For each page commit, open a
separate short-lived coherent maintenance view to authenticate that the raw
global still equals the cycle fence plus the latest cycle-bound progress and
persisted mark/queue/radix packs, then exact-CAS those bytes. Any ordinary
publication that changes the fenced global invalidates the active GC progress;
maintenance aborts/restarts rather than adopting the new bytes. The
coordinator never refreshes or substitutes the cursor's
owner/object view. Drop the cursor/enumeration read only at phase completion,
cancellation, error, shutdown, or crash. A
resumed process opens one fresh enumeration view and cursor with the last
committed authenticated key as an exclusive lower bound, plus fresh bounded
maintenance views as pages commit. View expiry, missing/malformed rows,
non-increasing keys, wrong projection/domain, cancellation poison, or any
cursor error aborts maintenance fail-closed. No cursor/native snapshot is
persisted, and the maintenance view never becomes a second root authority.

Public state/catalog/history pagination binds its operation/root/resume
position to the originating coherent view. It may resume only while that
`StorageRead`/session remains live; after drop or expiry it returns
`ReadExpired`/`InvalidCursor`. GC's internal storage-space cursor remains owned
by the exact `GcProgressV2` fence and never forges or substitutes a public
reader pin.

Catalog pagination, state range/projection, cold diff, history, ReceiptTree
completion and blob range reads do **not** use this storage-space cursor. They
walk authenticated object trees using batched `get_many` on one coherent view.
This distinction prevents the cursor from becoming an index or authority.

## Ownership dependency DAG

```text
G0  H3 app oracle GREEN + H4 cursor merged
 |
 M0  one merge of then-current main into approved Stage1; owner seal green
 |
 R0  coherent CoherentView/session plumbing + protocol/bootstrap capability
 +--> R1  global/local/untracked point+ordered range+early projection readers
 +--> R2  CommitCatalog + unified ChangeCatalog exact/resume/graph/history
 +--> R3  branch/ref/current readers and RefChange materialization
 +--> R4  hash-pruned diff/merge + working-diff reader replacement
 +--> R5  blob full/range + plugin manifest + ReceiptTree/upload readers
 +--> R6  checkpoint/recovery/undo/redo/retention readers
 +--> R7  complete typed root traversal + cursor-backed bounded GC readers
 |
 W0  bootstrap absent selectors/empty authenticated roots
 +--> W1  sorted state/catalog/Commit/Change/RefChange path-copy publication
 +--> W2  branch/global/root-only/checkpoint/recovery/undo/redo selectors
 +--> W3  blob/chunk/upload part/completion/abort/plugin publication
 +--> W4  owner-local stale keys + global commit-version/GC-watermark retry fence
 +--> W5  persisted bounded mark/queue/continuation sweep
 |
 D0  remove working-diff writer
 +--> D1  delete 41 spaces + physical modules/codecs/helpers
 +--> D2  remove old exports, fixtures, benches, facade sentinel, scan residue
 |
 C0  residue/compile-fail gates
 |
 C1  FIRST RUNNABLE COMPILE
```

Reader nodes may proceed in source only after their typed owner capability
exists. No writer node starts until all R0--R7 consumers have moved. D0 follows
W1/W2 and R4. D1 follows every writer. C1 is forbidden until D2 and zero
residue.

### Exact compiler-wave file ownership

These are local edit groups inside the single non-runnable wave, not commits or
compilation boundaries. A path appears in the earliest group that must make it
authority-safe; later groups may finish its writer deletion.

| Group | Exact production paths/capability |
|---|---|
| R0 owner capability completion | `forktree/{serving,state,tree,view,blob,object,publication,reachability,gc_index,model,codec,mod}.rs`: bootstrap, coherent pair/session ownership, state point/range/projection, catalog exact/resume, history/diff streams, upload completion, bounded cursor enumeration and typed accounting. ReaderLease is forbidden. Still unwired. |
| R1 coherent session/transaction | `engine.rs`, `session/{context,transaction,observe,mod}.rs`, `transaction/{context,staging,schema_resolver,types,mod}.rs`: one `StorageRead`, one raw selector pair/view_id, no mid-traversal refresh; resume remains owned by that live handle and fails closed after drop. |
| R2 catalogs/graph/history | `changelog/{context,materialization,types,mod}.rs`, `commit_graph/{context,types,mod}.rs`, `checkpoint.rs`, `branch/refs.rs`, and non-#1260 history providers: CommitId/ChangeId exact+resume and graph traversal use the single authenticated catalogs. |
| R3 branch/root consumers | `branch/{context,lifecycle,refs,stage_rows,types,mod}.rs`, `session/{create_branch,switch_branch,checkpoint,undo_redo}.rs`, `session/merge/{analysis,branch,conflicts,mod}.rs`: selector/snapshot/ref-change edges and O(1) root movement. |
| R4 current/untracked/projection | `live_state/{context,reader,types,derived,visibility,entity_columnar,entity_columnar_cache,entity_decoded_column_cache,mod}.rs`, `functions/state.rs`, filesystem and JSON serving facades: canonical two-tree merge, tombstone/NULL semantics, early field projection, untracked owner. |
| R5 diff/working-diff | retained facade files `tracked_state/{context,diff,diff_id,merge,row_materialization,types,mod}.rs`, `session/merge/*`, observe/catalog consumers: state-root diff plus semantic Commit/Change chronology; transaction-local overlay replaces persisted working diff. |
| R6 blob/upload/plugin | retained facades `binary_cas/{context,types,metrics,mod}.rs`, filesystem blob readers, `session/media_upload.rs`, plugin registry/runtime callers: typed manifests/chunks/ReceiptTree and fixed 1 MiB/F64/Q8 reads. |
| R7 GC/retention | `gc.rs`, `session/gc.rs`, checkpoint/recovery/plugin root callers: selected typed graph, four live cursor drains, and adapter read-safe-point handling; no owner-internal decoding in `gc.rs`. |
| W1 state/catalog publication | `transaction/{commit,commit_coordinator,staging,normalization,validation,stale_commit}.rs` and session transaction close: sorted path-copy state plus CommitCatalog/ChangeCatalog in one prepared publication. |
| W2 selector/root publication | branch/session checkpoint/recovery/undo/redo/merge owner files above: exact owner selector stale keys plus global commit-version retry fence. No reader-lease mutation exists. |
| W3 blob/upload publication | `session/media_upload.rs`, filesystem mutation facade, binary-CAS semantic facade and plugin publication: immutable objects plus receipt/file selector transition in one commit. |
| W4 bounded GC publication | ForkTree reachability owner plus `gc.rs` facade: GC-start generation flip, page checkpoints fenced by unchanged raw global, owner-produced delete batches and adapter read safe point. |
| D0--D2 hard deletion | delete the 23 physical modules listed above, 41 spaces and codecs, all old exports/rebuilds/fixtures, persisted working-diff writer, and every old scan wrapper before C1. |

Reserved ownership is explicit: do not edit H4's Storage/adapter cursor files in
this wave; consume the landed API. Do not edit #1260's
`sql2/{exec/write.rs,exec/datafusion.rs,providers/{mod.rs,spec.rs,upsert.rs},session.rs}`.
Other SQL readers retain their public provider contract through the rewritten
changelog/live-state facades; any direct legacy-space import outside the
reserved set is removed in R2/R4. There is no temporary adapter between them.

## One non-runnable reader-first/writer-last wave

No numbered item is a runnable commit, feature flag, partial PR, or benchmark
head. Compiler errors are the work queue; the first compile is Step 12.

1. Merge then-current main once into approved Stage1. Resolve the cursor/seal
   integration and the e871 conflict/hot spots, then run only source seals and the
   Stage-1 owner/app oracles. Do not connect serving.
2. Add sealed facade capabilities missing for consumers: absent bootstrap,
   coherent session ownership, untracked point/range, authenticated blob
   full/range, hash-pruned diff/merge streams, lossless public Commit/Change
   adapters, protocol hard-cut rejection, live-handle-bound resume, and
   cursor-backed owner enumeration. Reject ReaderLease source residue.
3. Move session/transaction open to one `CoherentView`: one `begin_read`, one
   selector `get_many`, same-handle traversal, raw-pair `view_id` and exact raw
   publication preconditions. Traversal keeps that native `StorageRead` pin;
   resume after the handle/transaction drops fails closed.
4. Move live global/local/untracked point/range, filesystem/file/directory,
   JSON-pointer, functions, catalog, observe and projection readers. Re-express
   e871 deterministic-sequence exact closure in the untracked owner.
5. Move commit/change/ref exact lookup, ordered resume, graph, history and SQL
   history consumers to the single catalogs and authenticated objects.
6. Move branch create/switch/delete/rename, branch refs, checkpoint,
   recovery, undo/redo and retention consumers to selectors/root movement.
7. Move diff/merge/no-op/working-diff readers to authenticated root diff and
   transaction-local overlay. Prove zero marker readers before writer cut.
8. Move ordinary blob/plugin/file and multipart receipt readers to the object
   graph; completion streams ReceiptTree and has no reachability gap.
9. Move all GC/root readers to the complete typed universe and the four cursor
   points above. No in-memory global set or old queue/tree/CAS sweep remains.
10. Cut all writers last in one owner pass: sorted transaction state/catalog,
    semantic and RefChange, branch/global/root-only/snapshot, blob/upload/plugin,
    retention/catalog pruning and bounded sweep. Every
    publication retains one
    global commit-version/GC-watermark CAS as the sole ordering/fence plane, but
    exact owner-selector bytes determine semantic conflicts. A global-only
    mismatch internally rereads coherent raw global+owner selectors and retries
    with the already prepared immutable objects; an owner mismatch is a hard
    stale rejection. Same-owner stale writes and both GC race orders reject.
11. Delete the working-diff writer, all 41 spaces, listed physical modules,
    codecs, exports, old tests/benches and the unwired facade sentinel. Remove
    all old scan wrappers/aliases. Do not preserve a fallback to make compile
    errors smaller.
12. Run the standalone residue scanner. Require zero findings and both
    external compile probes to fail. Only then run the first all-feature
    compile.

SQL ownership boundary: this wave does not edit
`sql2/{exec/write.rs,exec/datafusion.rs,providers/{mod.rs,spec.rs,upsert.rs},session.rs}`
without explicit coordination with the SQL-write owner. ForkTree exposes the
minimal typed transaction/publication and projection capabilities; SQL keeps
the existing binder as semantic plan owner. Other SQL history/current/diff
consumers switch only within the coordinated non-runnable wave.

## Reconciled accepted contracts

- **Catalogs:** one raw-UUID CommitCatalog and one unified raw-UUID
  ChangeCatalog; exact and resume use the same tree; all owner back-edges fail
  closed; no order/ref directory.
- **Coherent pair/lazy open:** one `StorageRead`, one selector pair, root
  envelopes only at open. All visited object hashes/domains/IDs/generation/
  parent/owner edges validate before output. Unvisited corruption may remain
  latent. Resume binds view/repo/branch/root/operation/last key/integrity.
- **State:** local value > global; tombstone suppresses; absence falls through;
  NULL is a value; transaction overlay is non-durable.
- **Receipt tree:** bounded path-copy part directory; no predecessor or list;
  every deduplicated part advances its upload owner generation; completion
  atomically moves receipt to state.
- **Conflict plane:** branch/catalog UUID/upload/file owner revisions are the
  semantic conflict keys. One global commit-version/GC-watermark CAS remains
  the sole total-order and GC fence, but a global-only mismatch is an internal
  retry after one coherent reread of exact raw global+owner selectors, reusing
  already prepared immutable objects. Exact owner mismatch remains a hard stale
  rejection. There is no second authority/index/cache or O(branches) copy.
  Preparation improves from `Theta(N^2 P)` to `Theta(N P)` for `N` unrelated
  writers and preparation cost `P`; same-instant global CAS attempts remain
  `Theta(N^2)` and must be disclosed, especially on SlateDB. Frozen guardrail:
  `e4a6e6dd0ec308e19c34ae70c236e5372a206272`, tree
  `863fcb87c8b8e19b0e00dcf9a3605fea7e506fab`; report SHA-256
  `7360b463d9231c3383bc0738a90016c3a8e79126f09c4e20fbc28c6f5a6ca215`,
  evidence manifest SHA-256
  `980d8d15d72a80421486aab8b318371ae0998e68f3c544179110a3cf0ae9bd0f`.
- **GC:** persisted V2 radix mark/queue/live-branch packs and edge cursor;
  bounded executor memory; consumed queue packs retire; cursor is ephemeral;
  the live `StorageRead` snapshot and adapter low-watermark control physical
  reuse. Tokens fail `ReadExpired`/`InvalidCursor` after their originating
  handle drops. ReaderLease selectors/codecs/root accounting/global rotations
  are forbidden. The safe but unnecessary model is retained only as research:
  `ac23754c8ba4a943e69da1304e371d8416456f1b`, tree
  `156656c53b0193f6090e62bf652454f80fe461ac`.
- **Cold reopen:** batch/deduplicate known value-pack IDs on one coherent view;
  authenticate/decode each once; O(1) selector movement remains content-free.
- **Range/OLAP:** one authenticated get-many per object-tree level; deduplicate
  packs; project before full row allocation; no row-at-a-time fallback/cache/
  side index. The accepted Slate 50K model tradeoff is +1 object/query and +2
  per join, disclosed against dominant wall/CPU/allocation/byte wins. Frozen
  range/projection head `1047f895f7b48bf16b6114d68c112acab1988203`,
  evidence SHA-256
  `e3e9b2f9af4e05bbbd139547b120e3191dfc9186a9f7587e38edf9653898ab52`.
  The honest TableProvider successor is
  `2a0e8512bb37c9da2050c99c366e5ac05bb01553` against current comparator
  `c1ff6ffb28db7e2f3004f2e50f39c0f9e0ab5612`; its manifest SHA-256 is
  `6edb673f9b478cd651ea2079fa9d6aef490beb8bac67308c9d25a31f15f3e9f3`.
- **Cold diff:** breadth-first ordered unmatched forests, one authenticated
  sibling/child batch per level, value-pack dedupe once per call, bounded one
  level+output. No cache/locator. The model's residual Slate regression must be
  measured on production and is not silently waived. Frozen model head
  `0be9b69b63e78a52e458d8381cd29a00cc6153bb`, report SHA-256
  `4b735e2257c7e95423ee24d810a1b004cfe19e1ff705b5576668f685e650b6b8`;
  the subsequent same-snapshot extent attempt is terminal NO-CUT with final
  report SHA-256
  `62ae1e324d921bf032db135e8c3eca0d485c1855ec9c4b62576eca0e141bbb30`.
- **Semantic history diff:** equal ordered current rows, equal state content, or
  equal state roots are not sufficient to return an empty public `lix_diff`.
  Distinct selected Commit/Change identities and chronology remain authoritative
  in the single CommitCatalog/unified ChangeCatalog, and the ordered public
  change count/hash must be preserved even when materialized state is
  byte-identical. State-root hash skipping applies only to the state component;
  it never bypasses semantic history traversal.
- **Multimedia:** fixed 1 MiB/F64/Q8; segmented source and exact range bytes;
  unchanged leaves referenced; one-copy exact extent seam; no second CDC/rope
  format or locator authority.
- **History independence:** noncanonical path-copy roots are accepted. Ordinary
  one-row histories retain 99.6591--99.9528% bytes and sparse diff is 12--14
  gets. Adversarial independently reconstructed equal states may diff O(N+M).
  Sorted bulk ingest is the reproducible-root boundary. Preserve divergent-
  frontier/output-proportional diff and do not add online global canonical
  packing/balancing (measured 122x--9600x publication cost for 3.2% bulk-byte
  benefit). An optional future offline canonical snapshot requires measured
  product demand, explicit O(N) cost, and one atomic root move—never a second
  serving authority. Frozen model head
  `9ebdadcb38e9b831172fbb4b3033c064d0534e17`, tree
  `32ffdcfd43e5ccb5007e0f6767ddccc6378472e6`; report SHA-256
  `0d030076b7501779f665624764f676834a4490942edd87aef2f8c1386c939d68`.
- **Rocks history disk:** the frozen 1K post-flush +8.954% result is obsolete
  SST/tombstone retention, not live ForkTree geometry. Its perfect source-cut
  ceiling is only 8.218%, so no layout cut is admitted. Equivalent full
  compaction is 337,557 B versus current 2,998,289 B (-88.742%); the candidate
  retains only 39 live objects/145,282 accounted bytes after final GC. Report
  this flush/compaction distinction in production qualification and do not add
  a second physical authority to optimize transient LSM state. Frozen report
  SHA-256: `8a25f6c69a5b22eb0f681dc5067127272a5c141183f263832b4ebb52a6eed859`.

## Exact benchmark/accounting hooks for C1

The hard cut deletes old-layout accounting rather than relabeling it. The
first runnable source exposes one non-authoritative, `storage-benches`-only
accounting surface in `storage_bench.rs`; counters are derived from owner
operation results and adapter stats and never trigger an extra read, write,
flush, or scan.

| Hook boundary | Required measurements |
|---|---|
| coherent open / point / range | selector get calls/keys/bytes, object get-many calls/IDs/bytes, authenticated hash bytes, decoded nodes/leaves/value packs, projected/output bytes, tree levels, view-open wall/CPU/alloc/RSS |
| prepared publication | changed identities, copied nodes/value packs, new versus byte-identical objects, object/selector puts and bytes, owner preconditions, global-only retries, hard owner-stale rejects, backend commit calls/bytes, prepared-byte reuse |
| catalogs/history/diff/merge | catalog path copies, Commit/Change objects, hash-pruned nodes, unmatched forests, unique value packs, output changes/conflicts, semantic-history records even when state is equal |
| multimedia | manifest/internal/leaf objects, authenticated source/hash bytes, shared leaves/bytes, touched Q window, full/range output, contiguous outer materialization bytes only when requested |
| GC | phase, cursor opens/restarts/pages/rows, enumeration and maintenance reads separated, mark/queue/radix/live-pack IDs+bytes, active peak IDs/bytes, delete rows/logical bytes, stale/cancel/crash retries, live-handle expiry and reader-safe-point deferrals; no lease-row/minimum/digest accounting |
| settled layout | object counts/bytes by authenticated domain, selector/untracked rows, reachable/reclaimed objects+bytes, adapter logical/physical calls+bytes, post-close disk and explicit settled compaction |

The benchmark reset/take boundary is one serial fixture scope. Concurrent tests
use operation-returned stats rather than process-global deltas. Object-domain
layout accounting drains `OBJECT_SPACE` once through the new ascending cursor;
it is outside timed operations and cannot be used by serving or GC.

Compile these existing harnesses against C1 before running broad cells:

```bash
cargo bench -p lix_benchmarks --bench tracked_state_crud --no-run
cargo bench -p lix_benchmarks --bench diff_commands --no-run
cargo bench -p lix_benchmarks --bench checkpoint_history_scale --no-run
cargo bench -p lix_benchmarks --bench repository_gc_scale --no-run
cargo bench -p lix_benchmarks --bench large_blob_updates --no-run
cargo bench -p lix_benchmarks --bench tpch --no-run
cargo test -p lix_benchmarks --test tracked_state_crud_public_result --no-run
cargo test -p lix_benchmarks --test checkpoint_gc_replay_reopen --no-run
cargo test -p lix_benchmarks --test cas_gc_history_retention --no-run
```

`tracked_state_crud/{main,accounting,storage,transaction_api,sql_session}.rs`
owns CRUD/transaction measurements; `diff_commands.rs`,
`checkpoint_history_scale.rs`, `repository_gc_scale.rs`,
`large_blob_updates.rs` and `tpch/lix.rs` own their corresponding gates. The
accepted external DataFusion/TableProvider and public-semantics harnesses are
applied unchanged to the immutable C1 head; model binaries are never reused as
production evidence.

## First runnable gate

The first runnable candidate must satisfy, in order:

1. execution residue scanner: zero findings;
2. external equivalent-space and old-scan probes: both fail compilation;
3. repository sealed-owner and no-production-reference structure tests;
4. `cargo check -p lix --all-features` and canonical warnings-denied Clippy;
5. 24 owner tests, frozen blocker/history/bounded/cursor/app oracles, and
   deterministic crash/corruption/reader-pin tests;
6. exact initialization, hard-cut protocol rejection and Memory public
   semantics before any adapter/performance matrix.

Any failure here is repaired only inside the new owner or switched consumer.
Restoring an old path is an architecture failure.

### Exact first-runnable command sequence

Run from the candidate root with a fresh candidate-specific target. The full
commands, including expected-failure checks, are frozen in
`forktree_stage2_execution_oracle/INVOCATIONS.txt`; the controlling sequence is:

```sh
ORACLE=packages/lix/tests/forktree_stage2_execution_oracle
EVIDENCE=/tmp/forktree-stage2-first-runnable-evidence
CARGO_TARGET_DIR=/tmp/forktree-stage2-first-runnable-target
mkdir -p "$EVIDENCE"
rustc --edition=2024 -D warnings "$ORACLE/main.rs" -o "$EVIDENCE/residue-oracle"
"$EVIDENCE/residue-oracle" self-test
"$EVIDENCE/residue-oracle" audit "$PWD"
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo build -p lix --lib
LIX_RLIB=$(find "$CARGO_TARGET_DIR/debug/deps" -maxdepth 1 -name 'liblix-*.rlib' -printf '%T@ %p\n' | sort -nr | head -1 | cut -d' ' -f2-)
test -n "$LIX_RLIB"
if rustc --edition=2024 -D warnings -L "dependency=$CARGO_TARGET_DIR/debug/deps" "$ORACLE/space_forge_rejection.rs" --extern "lix=$LIX_RLIB" -o "$EVIDENCE/space-forge" 2>"$EVIDENCE/space-forge.stderr"; then exit 1; fi
rg 'E0423|E0624|private|cannot initialize|not found' "$EVIDENCE/space-forge.stderr"
if rustc --edition=2024 -D warnings -L "dependency=$CARGO_TARGET_DIR/debug/deps" "$ORACLE/old_scan_compile_rejection.rs" --extern "lix=$LIX_RLIB" -o "$EVIDENCE/old-scan" 2>"$EVIDENCE/old-scan.stderr"; then exit 1; fi
rg 'E0432|E0599|unresolved import|no method named' "$EVIDENCE/old-scan.stderr"
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo check -p lix --all-features
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo clippy -p lix --all-targets --all-features -- -D warnings
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo test -p lix forktree::tests --lib -j2 -- --test-threads=1
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo test -p lix --test integration sealed_owner_violations_are_empty -- --nocapture
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage1_application_oracle --no-run
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_rocksdb -- --exact --nocapture --test-threads=1
CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_slatedb -- --exact --nocapture --test-threads=1
cargo fmt --all -- --check
git diff --check
```

The candidate's commit/tree, Cargo.lock, cursor-PR head, built rlib and every
log are hashed before semantic tests. A compile failure before the residue and
negative API gates is expected work inside the non-runnable wave; a compile
success with residue is a failed gate, not progress.

The cursor head is intentionally not named here while Hetzner-IV still owns a
mutable production branch. At release, append its independently frozen exact
cursor conformance invocations and head/tree to this sequence before entering
the non-runnable wave. Do not guess names or preserve the deleted scan API to
make this list executable early.

## Exact both-adapter qualification order

Each cell is capped at 20 minutes. Gate broad work on the preceding focused
cell.

1. RocksDB then SlateDB typed app-oracle v2: all five lifecycle cases,
   flush/drop/reopen, corruption, upload/GC races and final references.
2. Public-semantics deterministic traces: 100x100 operations, fixed
   reader/cursor/child/upload/publication/GC/corruption stress, exact digest
   equality after reopen. Hold one native reader across publication/deletion,
   prove it remains coherent until drop, then prove old cursor resume fails
   `ReadExpired`/`InvalidCursor` and a fresh view starts from an authenticated
   exclusive key. Assert ReaderLease source residue is absent.
3. Smallest 1K relational gate: insert/update/delete/mixed K=1/32/1%,
   transaction open/publication, branch/global NULL/tombstone, exact/resume,
   latency/CPU/alloc/RSS/backend calls+bytes/writes and settled disk.
   Include N=1/10/100 unrelated branch/catalog/upload publishers: all owners
   eventually succeed through global-only internal retry, same-owner or
   overlapping catalog UUID writers reject stale, prepared immutable object
   bytes are reused, and global CAS attempts plus Slate physical work are
   reported honestly.
4. 10K branch/diff/merge: 100 branches/10 edited, hot reads, batched cold diff,
   merge publish, delete/final reclaim. A critical Slate cold-diff regression
   above 5% requires explicit manager disposition; model evidence alone is not
   production acceptance. Include two branches with byte-identical ordered
   current rows but distinct selected Change/Commit chronology and require the
   exact ordered public `lix_diff` count/hash rather than a state-root equality
   shortcut.
5. Honest DataFusion/TableProvider at 10K, then 50K only if 10K clears: exact
   plans/results, narrow/wide projections, aggregates/joins, cold reopen, zero
   query writes, early projection and object/byte accounting.
6. History H=100 then H=1K: checkpoint, reopen, point/range, hash-pruned history
   diff, merge, undo/redo, recovery, retained/final reclamation. Requalify Slate
   reopen object count/bytes and final-reclaim allocation/RSS blockers.
7. Multimedia 64 MiB then 512 MiB: fresh/repeat ingest, 4 KiB localized edit,
   full/range read, branch/diff/merge/checkpoint/reopen, shared/final GC,
   corruption, fixed 1 MiB/F64/Q8 reuse and bounded engine memory.
8. GC scale 50K then 500K: selector/untracked/traverse/sweep/cleanup crash at
   each checkpoint; cursor pages do not reconstruct scans; peak IDs <6K and
   metadata <512 KiB plus one window; complete work is
   O(S+U+R+E+O), maintenance memory is bounded by persisted packs plus one
   page/window; stale pages and both publication orders reject; a live native
   reader and adapter low-watermark defer physical reuse.
   Verify all four drains use ascending bounds, each exclusive restart begins
   strictly after the last authenticated delivered-and-committed key,
   cancel-before-poll is inert, cancellation during await/backend error/
   malformed page poisons the cursor across every adapter, and a fresh cursor
   on a new coherent view resumes exactly once without skip/duplicate.
9. Settled storage: explicit flush/settled compaction, live/reclaimed object
   counts, logical/physical bytes, LSM disclosure. No critical disk/resource
   regression >5% without explicit superior-overall-tradeoff acceptance.
10. Full fmt/diff, warnings-denied workspace Clippy/tests, filesystem fuzz,
    SQL/session/file/plugin/observe/accounting and independent qualification.

## Stop/go rule

GO from the first runnable head only if public semantics and authentication are
exact, residue is zero, both adapters are green, bounded GC/cursor complexity
holds, and no unaccepted critical regression exceeds 5%. Continue through an
initial performance regression only when a measured removable ceiling exceeds
10% or the hard cut deletes a major authority; report that ceiling before the
next iteration. STOP if correctness requires a second authority, compatibility
reader, full-tree rebuild, unbounded GC set, row-at-a-time OLAP fallback,
placement-derived identity, ReaderLease production authority, resumable cursor
after its originating view expires, or permanent global writer serialization.
