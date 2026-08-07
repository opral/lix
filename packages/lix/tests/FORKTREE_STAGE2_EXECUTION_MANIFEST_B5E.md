# ForkTree Stage-2 compiler-hard-cut execution manifest

Status: **plan/test-only; production mutation is held**. This artifact does
not authorize an intermediate reader, writer, compatibility path, or compile.

## Frozen inputs and release conditions

- pinned current main: `b5e78190f49cab5de7bb19b6f967706c214363b6`
- pinned current-main tree: `c913465505bc773d21a6e2804530287ee937a3f1`
- inventory predecessor: `4763408467d265b288a124e24b1d47be423f5d17`
- approved unwired Stage 1: `138b55e1de90806c380ad27b2b349f4c66a1387f`
- approved Stage-1 tree: `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`
- corrected typed-owner app-oracle transport: `5a6a2cb037668c8dc6256d9b0975d0b39068f07a`
- app-oracle tree: `47169206cb7822937464a873f27c2bd41d8e98c2`
- common architecture base: `8e3ffe632bc27e1ab84fe9a6102b099ab2e9f441`
- read-only b5e+138b conflicted merge index/tree:
  `7b24abfcd75f4227678fbd7da4590d55639c0b59`

The prospective b5e+138b merge has two real textual conflicts:
`binary_cas/kv.rs` and `storage_bench.rs`. Its ten production changed-path
intersections are `binary_cas/kv.rs`, `branch/control.rs`, `gc.rs`,
`live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`,
`session/media_upload.rs`, `storage_bench.rs`,
`tracked_state/current_state_data_part.rs`, `tracked_state/storage.rs`, and
`transaction/commit.rs`. The production wave must preserve b5e/#1258's
authenticated CAS/retention semantics while deleting its superseded physical
authority; it must also re-express the deterministic-sequence untracked closure
before deleting the hot/current path.

## Frozen concurrent-work conflict map

The following remote heads were fetched read-only while this artifact was
prepared. None is copied into this branch, and this plan does not decide whether
an unmerged PR lands. The production cut consumes only the then-current main in
its one authorized merge.

| Lane | Exact reviewed head/tree | Base/merge/path result | Stage-2 disposition |
|---|---|---|---|
| #1244 plugin-checkpoint corruption | `5d942436358f3ecd4a8df0ace22e9c191f9bcf05` / `c10d6bddb3657e04c80229e1f939a892c33cdeb5` | merge base `95478b1fa329f7608b2360554ca72fad782dfeb8`; prospective merge has one textual conflict in `packages/engine-benchmarks/Cargo.toml`. Stage-1 path intersections are `branch/control.rs`, `init.rs`, `storage_bench.rs`, `transaction/plugin_checkpoint.rs`, and `rs-sdk-tests/tests/e2e.rs`. | `PLUGIN_CHECKPOINT_SPACE`, `transaction/plugin_checkpoint.rs`, and old branch control are deleted. Preserve #1244's public fail-closed corruption behavior by authenticating plugin-registry/WASM edges in the selected object graph; do not port its physical checkpoint row or resolve its benchmark conflict in this plan branch. |
| #1258 binary-CAS GC | merged as current main `b5e78190f49cab5de7bb19b6f967706c214363b6`; second parent `a6cb5d8c316d166b4a4eb5e5c3fc50d04d57774d`, tree `c913465505bc773d21a6e2804530287ee937a3f1` | the b5e+Stage1 prospective merge has real conflicts in `binary_cas/kv.rs` and `storage_bench.rs`; eight other production intersections merge textually but remain semantic hot spots. | Preserve authenticated serving-closure role separation, declared-size/delta/base validation, full-queue roots, retained-history/final-reference semantics, true shared chunks, and both upload/publication race orders in the new object owner. Resolve `binary_cas/kv.rs` by moving callers to typed ForkTree manifests/chunks and deleting the file, not by retaining either physical implementation. Rewrite the bench/oracle to the new owner before deleting old accounting. |
| #1260 SQL write owner | `7061aad7f4b14e611b32bbe5493f39253b826378` / `d41598c18afae0b6a9c675fb8be3b263000da67a` | based directly on 476; clean prospective tree `d41598c18afae0b6a9c675fb8be3b263000da67a`. Its only Stage-1 intersection is `rs-sdk-tests/tests/e2e.rs`; production changes are confined to `sql2` plus that test. | Do not edit its SQL files. ForkTree exposes only typed transaction/publication/range-projection capabilities. The existing binder remains semantic plan authority; SQL integration is coordinated after its owner freezes. Test overlap is reconciled in qualification, not by changing #1260 here. |
| Storage streaming cursor | provisional branch `codex/storage-streaming-scan-cursor`, not advertised by the remote at freeze time | no immutable head/tree exists yet, so no source merge claim is made. Provisional API and hard deletion contract are recorded below. | Hetzner-IV exclusively owns traits, adapters, scan migration, and deletion of the old scan API. Stage 2 starts only after its immutable head is independently green and merged; the one main merge consumes the final names. This branch never edits those files or restores an old scan wrapper. |

The #1244 benchmark conflict is real but outside this artifact's source scope.
#1260 is textually clean only because its reviewed head is based on the 476
predecessor; that is not a promise about the eventual then-current main. Any
new overlap is resolved once, in the production worktree, by preserving the
semantic contracts above and never by retaining a legacy authority.

Production release requires both events:

1. Hetzner-III returns terminal dual-adapter GREEN for exact oracle `5a6a2cb`.
2. Hetzner-IV's no-compatibility StorageRead streaming-cursor PR is
   independently approved and merged.

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
selectors and generations. It is not a row or payload authority.

Object identity is the canonical object-domain hash and never includes an SST,
extent, pack, or physical placement. Any future locator is rebuildable and
cannot authorize reads, writes, roots, publication, or GC. Active
`StorageRead` snapshots are the process-local reader pin; durable pins are
authenticated selectors only. Logical deletion commits under an exact GC
fence; physical reuse waits for the adapter read low-watermark. No process pin
registry, clock grace, persisted snapshot token, or out-of-band object delete
is allowed.

## Exact durable-space disposition on b5e

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

| Space | b5e production paths containing it |
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

The b5e deterministic-sequence closure must move from
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
`branch_head_control_precondition`.

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
The b5e/#1258 physical facade also disappears:
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

Current b5e additionally deletes the physical-retention implementation
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
Installed plugin WASM is an ordinary manifest edge from current and retained
registry objects. The owner traverses the complete selected graph; `gc.rs`
receives bounded typed progress/status only and never decodes owner internals.
Mark/queue/radix/live-branch packs remain rebuildable maintenance objects in
the same object space under `GcProgressSelectorV2`.

## Every old low-level scan consumer on b5e

The cursor PR deletes the old `StorageRead::scan`, `ScanOptions`, `ScanChunk`,
`StorageScanOptions`, `ScanPlan`, adapter resume helpers, and all production or
test resume loops. Stage 2 must never restore them.

| Disposition | Exact b5e functions |
|---|---|
| deleted with binary CAS | `binary_cas/kv.rs::{load_declared_manifest_chunks,load_declared_manifest_chunk_range,scan_all_values,scan_all_values_for_plan}`, `binary_cas/stats.rs::scan_space` |
| deleted with branch/changelog | `branch/control.rs::BranchHeadControlReader::scan`, `changelog/context.rs::native_scan`, `engine.rs::repository_has_changelog_commit` |
| deleted with old GC | `gc.rs::{scan_tree_sweep_marks,stage_tree_sweep_epoch_page,load_recovery_refs,stage_sweep_unreachable_content_nodes}` plus indirect branch-control scans in `load_tree_sweep_root_closure`, `audit_repository_gc_standalone_refs`, `stage_repository_gc_with_preconditions`, `stage_repository_gc_full_recovery` |
| deleted with old tracked/current | `tracked_state/storage.rs::{visit_change_records_from_commit_deltas,validate_no_orphan_commit_delta_segments,scan_full_space}`, `live_state/tracked_head.rs::stage_active_working_diff_scopes`, and `live_state/tracked_head/hot.rs::{stage_certified_entity_batches,scan_certified_entity_batch_rows,scan_certified_history_rows,packed_exclusive_schema_base_refs,packed_current_base_refs,stage_retire_packed_current_bases,scan_root_current_base_rows_for_merge,validate_exact_collection_closure,has_schema_rows,untracked_json_refs,hot_load_file_scope_identities,hot_working_diff_entries,hot_scan_entries,hot_scan_dense_encoded_key_range,scan_hot_file_entries,stage_collect_stale_hot_collection_controls,stage_collect_stale_hot_space,stage_collect_stale_hot_diff_records,stage_delete_hot_diff_scope}` |
| deleted with old upload/plugin | `session/media_upload.rs::{load_upload_progress,load_upload_manifest_leaves}`, `transaction/plugin_checkpoint.rs::stage_delete_branch_plugin_checkpoints` |
| H4 cursor-PR ownership | `storage/traits.rs::StorageRead::scan`, `storage_adapter/{context,read_scope,scan}.rs` wrappers, Memory/Rocks/Slate implementations, conformance/model/failure tests, all adapter resume state and bounds helpers |
| retained independent but migrated by H4 | test-only `json_store/context.rs::scan_untracked_reclaim_candidates`; no Stage-2 implementation may restore its old loop |

## Precise cursor consumption points after the cursor PR

The final API names are bound to Hetzner-IV's immutable cursor head before the
production merge. The currently compiling names are provisional:
`StorageRead::begin_scan(space, range, BeginScanOptions)`,
`ScanCursor::next_page(limit_rows)`, and ascending/descending `ScanOrder`.
This plan freezes semantics and call sites, not those provisional spellings.

Only these ForkTree owner functions consume the storage-space cursor:

| Owner function | Space/range | Cursor contract | Durable restart |
|---|---|---|---|
| `reachability::advance_selector_roots` | `SELECTOR_SPACE`, ascending typed-selector range | one cursor on the cycle's coherent `StorageRead`; authenticate each selector before yielding roots; repeatedly request bounded pages | `GcProgressV2.selector_resume_after`, used only as the next scan's exclusive lower bound after commit/crash |
| `reachability::advance_untracked_roots` | `UNTRACKED_ROW_SPACE`, ascending full/key projection required by root extraction | one cursor, validate branch/key/value ownership and manifest roots before marking | `untracked_resume_after` |
| `reachability::advance_sweep` | `OBJECT_SPACE`, ascending object IDs | sorted object/mark merge; authenticate object key/domain before at most 256 owner-produced deletes; repeat pages on one read instead of rebuilding an iterator | `object_resume_after` |
| `reachability::advance_cleanup` | `OBJECT_SPACE`, maintenance-domain range | delete only unreachable/superseded maintenance packs from the completed cycle; semantic corruption never authorizes deletion | `maintenance_resume_after` |

At every checkpoint: retain only the last authenticated key in the canonical
`GcProgressV2` object, exact-CAS raw global selector plus old GC-progress
selector, rotate the GC generation as specified, commit, drop the cursor/read,
and reopen a fresh coherent read. Inside one checkpoint window, call
`next_page` repeatedly; never reconstruct a backend iterator. View expiry,
missing/malformed rows, non-increasing keys, wrong projection/domain, or cursor
error aborts maintenance fail-closed.

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
 +--> W4  owner-local conflict keys + read-only GC-generation fence
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

## One non-runnable reader-first/writer-last wave

No numbered item is a runnable commit, feature flag, partial PR, or benchmark
head. Compiler errors are the work queue; the first compile is Step 12.

1. Merge then-current main once into approved Stage1. Resolve the cursor/seal
   integration and the b5e conflict/hot spots, then run only source seals and the
   Stage-1 owner/app oracles. Do not connect serving.
2. Add sealed facade capabilities missing for consumers: absent bootstrap,
   coherent session ownership, untracked point/range, authenticated blob
   full/range, hash-pruned diff/merge streams, lossless public Commit/Change
   adapters, protocol hard-cut rejection, and cursor-backed owner enumeration.
3. Move session/transaction open to one `CoherentView`: one `begin_read`, one
   selector `get_many`, same-handle traversal, raw-pair `view_id` and exact raw
   publication preconditions.
4. Move live global/local/untracked point/range, filesystem/file/directory,
   JSON-pointer, functions, catalog, observe and projection readers. Re-express
   b5e deterministic-sequence exact closure in the untracked owner.
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
    retention/catalog pruning and bounded sweep. Ordinary publication uses
    owner-local conflict keys; global generation is the read-only GC fence and
    advances only at GC start. Same-owner stale writes and both GC race orders
    reject.
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
- **Conflict plane:** branch/catalog UUID/upload/file owner revisions are
  logical conflict keys. Repository GC generation is a publication fence,
  advances only at GC start, and is not a permanent all-writer conflict key.
- **GC:** persisted V2 radix mark/queue/live-branch packs and edge cursor;
  bounded executor memory; consumed queue packs retire; cursor is ephemeral;
  reader low-watermark controls physical reuse.
- **Cold reopen:** batch/deduplicate known value-pack IDs on one coherent view;
  authenticate/decode each once; O(1) selector movement remains content-free.
- **Range/OLAP:** one authenticated get-many per object-tree level; deduplicate
  packs; project before full row allocation; no row-at-a-time fallback/cache/
  side index. The accepted Slate 50K model tradeoff is +1 object/query and +2
  per join, disclosed against dominant wall/CPU/allocation/byte wins.
- **Cold diff:** breadth-first ordered unmatched forests, one authenticated
  sibling/child batch per level, value-pack dedupe once per call, bounded one
  level+output. No cache/locator. The model's residual Slate regression must be
  measured on production and is not silently waived.
- **Multimedia:** fixed 1 MiB/F64/Q8; segmented source and exact range bytes;
  unchanged leaves referenced; one-copy exact extent seam; no second CDC/rope
  format or locator authority.
- **History independence:** noncanonical path-copy roots are accepted. Ordinary
  one-row histories retain 99.6591--99.9528% bytes and sparse diff is 12--14
  gets. Adversarial independently reconstructed equal states may diff O(N+M).
  Preserve a future deterministic local-resync seam; do not eager-canonicalize
  (measured 122x--9600x publication cost for 3.2% bulk-byte benefit).
- **Rocks history disk:** the frozen 1K post-flush +8.954% result is obsolete
  SST/tombstone retention, not live ForkTree geometry. Its perfect source-cut
  ceiling is only 8.218%, so no layout cut is admitted. Equivalent full
  compaction is 337,557 B versus current 2,998,289 B (-88.742%); the candidate
  retains only 39 live objects/145,282 accounted bytes after final GC. Report
  this flush/compaction distinction in production qualification and do not add
  a second physical authority to optimize transient LSM state. Frozen report
  SHA-256: `8a25f6c69a5b22eb0f681dc5067127272a5c141183f263832b4ebb52a6eed859`.

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
cargo fmt --all -- --check
git diff --check
```

The candidate's commit/tree, Cargo.lock, cursor-PR head, built rlib and every
log are hashed before semantic tests. A compile failure before the residue and
negative API gates is expected work inside the non-runnable wave; a compile
success with residue is a failed gate, not progress.

## Exact both-adapter qualification order

Each cell is capped at 20 minutes. Gate broad work on the preceding focused
cell.

1. RocksDB then SlateDB typed app-oracle v2: all five lifecycle cases,
   flush/drop/reopen, corruption, upload/GC races and final references.
2. Public-semantics deterministic traces: 100x100 operations, fixed
   reader/cursor/child/upload/publication/GC/corruption stress, exact digest
   equality after reopen.
3. Smallest 1K relational gate: insert/update/delete/mixed K=1/32/1%,
   transaction open/publication, branch/global NULL/tombstone, exact/resume,
   latency/CPU/alloc/RSS/backend calls+bytes/writes and settled disk.
4. 10K branch/diff/merge: 100 branches/10 edited, hot reads, batched cold diff,
   merge publish, delete/final reclaim. A critical Slate cold-diff regression
   above 5% requires explicit manager disposition; model evidence alone is not
   production acceptance.
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
   metadata <512 KiB plus one window; work O(S+Q+R+E+O); stale pages and both
   publication orders reject; reader low-watermark defers physical reuse.
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
placement-derived identity, or permanent global writer serialization.
