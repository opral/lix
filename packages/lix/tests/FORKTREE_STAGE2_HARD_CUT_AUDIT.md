# ForkTree Stage-2 compiler hard-cut deletion audit

Status: frozen test/source-only implementer map. No production source was
edited, compiled, wired, or published by this audit.

## Immutable provenance

- latest-main comparator: `f77f5b9e2ff582f749d1c487d95e6c0e8e4d3662`
- latest-main tree: `597b98f80dad062b4c0b244f2e59fa489a9d4ce9`
- accepted architecture base: `8e3ffe632bc27e1ab84fe9a6102b099ab2e9f441`
- architecture-base tree: `8da56ca4e5d77aa25e57e611fbf4aaad4c01dd10`
- frozen unwired Stage-1 head: `4b7b3aa25ebed5f022ed258c172c27e4dc64753d`
- frozen Stage-1 tree: `5cafd24b60112220e86c5bccaf5fb382416f2666`

The Stage-1 head is not descended from f77. The production implementer must
integrate the two exact histories once before beginning the wave. This audit
does not synthesize or bless a merge tree.

Between the architecture base and f77, the only changed Lix production file is
`packages/lix/src/live_state/tracked_head/hot.rs` (`+304/-122`). It adds or
rewrites four current-serving helpers that must be consumed by the hard cut:

- `canonicalize_single_certified_batch`
- `exclude_ordered_live_batch_identities`
- `hot_filter_has_one_fixed_file_bucket`
- `canonicalize_hot_scan_rows`

The other two changed files are one CLI experiment and one SDK benchmark test.
There is no durable-space addition, deletion, or rename between the
architecture base and f77.

## Decision: one first-runnable wave

The refreshed requirement is stricter than the old document's conceptual
Stages 2, 3, and 4. A runnable checkpoint after only tracked serving/history
would still leave legacy CAS/upload and GC root authorities reachable. It is
therefore not an accepted first runnable state.

The first accepted compile must follow one local, non-runnable wave that moves
readers first, writers last, then physically deletes all 41 superseded durable
spaces and their owner implementations. CAS/upload and bounded GC remain
internally ordered subphases, not separately runnable or publishable stages.
If the wave cannot reach that boundary, abandon it; do not restore a bridge,
compatibility codec, fallback reader, dual writer, or feature flag.

## Reconciled durable-space inventory

The often cited 47-space authority map is correct only after classifying two
revision/materialization spaces as rebuildable. The source contains 49 actual
space constants:

| Class | Count | Exact spaces | First-runnable disposition |
|---|---:|---|---|
| Superseded tracked/current plane | 23 | `TRACKED_STATE_TREE_CHUNK_SPACE`, `TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE`, `TRACKED_STATE_CHANGE_LOCATOR_SPACE`, `TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE`, `TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE`, `MUTATION_DIRECTORY_NODE_SPACE`, `SCOPED_RANGE_NODE_SPACE`, `CURRENT_STATE_DATA_PART_SPACE`, `CURRENT_STATE_DATA_PART_REFS_SPACE`, tracked portion of `HOT_ROW_SPACE`, `HOT_FILE_SPACE`, `HOT_DIFF_SPACE`, `HOT_COLLECTION_CONTROL_SPACE`, `PACKED_CURRENT_BASE_SPACE`, `PACKED_CURRENT_BASE_CONTROL_SPACE`, `PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE`, `ROOT_CURRENT_BASE_SPACE`, `TRACKED_WORKING_DIFF_MARKER_SPACE`, `CERTIFIED_ENTITY_BATCH_SPACE`, `CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE`, `CERTIFIED_ENTITY_BATCH_PAGE_SPACE`, `ROW_GROUP_MANIFEST_SPACE`, `ROW_GROUP_COLUMN_SPACE` | Delete old encodings and all tracked readers/writers. A new untracked-only space may replace, not reinterpret, mixed `HOT_ROW_SPACE`. |
| Superseded changelog | 3 | `COMMIT_SPACE`, `CHANGE_SPACE`, `COMMIT_CHANGE_ID_SPACE` | Delete after all exact/ordered/history/graph consumers use one CommitCatalog and one unified ChangeCatalog. |
| Superseded branch control | 1 | `BRANCH_HEAD_CONTROL_SPACE` | Delete after coherent global+branch selector reads and typed selector publication replace it. |
| Superseded binary CAS | 4 | `BINARY_CAS_MANIFEST_SPACE`, `BINARY_CAS_MANIFEST_CHUNK_SPACE`, `BINARY_CAS_CHUNK_SPACE`, `BINARY_CAS_CHUNK_PRESENCE_SPACE` | Delete after manifest/chunk readers move and every publication uses the object owner and one epoch. |
| Superseded multipart upload | 2 | `UPLOAD_MANIFEST_LEAF_SPACE`, `UPLOAD_STATE_SPACE` | Delete in the same subphase as CAS. Open receipts become typed selector roots over bounded ReceiptTree objects. |
| Superseded GC/checkpoint | 7 | `CHECKPOINT_GC_STATE_SPACE`, `CHECKPOINT_RECOVERY_REF_SPACE`, `GC_REACHABILITY_DELTA_SPACE`, `GC_REACHABILITY_QUEUE_SPACE`, `GC_TREE_SWEEP_CURSOR_SPACE`, `GC_TREE_SWEEP_EPOCH_SPACE`, `GC_TREE_SWEEP_MARK_SPACE` | Delete after all roots and resumable maintenance state use typed selectors plus authenticated V2 mark/queue/radix packs. |
| Superseded plugin checkpoint | 1 | `PLUGIN_CHECKPOINT_SPACE` | Delete when plugin registry/WASM roots are ordinary authenticated graph edges. |
| Semantically independent | 6 | `JSON_SPACE`, `UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE`, `TRACKED_MUTATION_REVISION_SPACE`, `EXECUTE_IDEMPOTENCY_RECEIPT_SPACE`, `FILESYSTEM_PATH_REVISION_SPACE`, `REPOSITORY_PROTOCOL_SPACE` | Retain only their present independent semantics. They may not serve tracked rows, history, reachability, or upload progress. |
| Rebuildable materialization | 2 | `MUTATION_REVISION_SPACE`, `CATALOG_REVISION_SPACE` | Retain as invalidation/revision aids; deleting/rebuilding them must not change repository semantics. |

Total: `23 + 3 + 1 + 4 + 2 + 7 + 1 + 6 + 2 = 49` physical
spaces. The authority map is `41 superseded + 6 independent = 47`; the two
rebuildable revisions explain the apparent discrepancy.

Stage 1 introduces exactly three descriptors:

- `OBJECT_SPACE` (`0x0009_0001`): the only immutable authenticated tracked,
  history, blob, and maintenance-object plane;
- `SELECTOR_SPACE` (`0x0009_0002`): the only mutable selector/epoch plane; and
- `UNTRACKED_ROW_SPACE` (`0x0009_0003`): the explicitly untracked-only row
  authority.

The third is a semantic owner, not a second tracked root. No old space can be
reinterpreted under the new codec, and opening pre-cut bytes must fail the
repository protocol check.

## Production dependency and deletion map

The table lists direct durable owners plus their indirect public consumers.
Moving only the direct files is insufficient.

| Legacy owner/surface | Current readers and writers that form the dependency closure | Replacement owner | Files/codecs deleted before compile |
|---|---|---|---|
| Tracked commit roots, tree chunks, deltas, locators, inventories | `tracked_state/storage.rs` (`load_root`, snapshot/manifest/topology loads, delta scans, locator loads, `stage_*` publication and GC deletion); `tree.rs`; `context.rs`; `types.rs`; `commit_graph/context.rs`; `transaction/{context,commit}.rs`; `session/{execute,merge/*,undo_redo}.rs`; checkpoint, recovery, diff, history, SQL file-history and working-diff providers | RepositoryRoot, global/local state roots, immutable Commit/Change objects, CommitCatalog and unified ChangeCatalog | `tracked_state/storage.rs`, `tree.rs`, `codec.rs`, obsolete physical types and all manifest/delta/locator/inventory codecs |
| Mutation directory and scoped range coverage | `mutation_directory.rs` (`build_*`, `load_mutation_part_read_plan`, collectors); `scoped_range.rs` (`stage_*`, `route_*`, `scan_*`, `validate_*`); commit-root rebuild and replacement-part readers; transaction publication | Canonical sorted mutation source plus authenticated path-copy state tree | `mutation_directory.rs`, `scoped_range.rs`, `commit_root_rebuild.rs`, `replacement_part.rs` |
| Current data parts/envelopes | `current_state_data_part.rs`; `current_state_envelope.rs`; row materialization, diff, current state context/storage; GC and recovery readers | StateCell (`Value`, `Null`, `Tombstone`) in global/local ForkTree roots | Both modules, refs/manifests, specialized generation/rebuild paths |
| Tracked hot/base/current serving | `live_state/tracked_head.rs`; `hot.rs` including the four f77 canonical-order helpers; `live_state/mod.rs`; entity decoded column cache; engine; functions; catalog/bind/providers; filesystem and JSON-pointer APIs; observe; no-op detection; branch refs; transaction read-your-writes | One coherent `StorageRead`, raw global+branch selector pair/view_id, local tombstone precedence, transaction-local overlay, untracked-only owner | Tracked portions of `tracked_head.rs` and `hot.rs`; packed/root/certified/hot codecs; tracked row/file/diff/control spaces. Retain only public facade and untracked owner. |
| Columnar row groups | `columnar_row_group.rs`; hot/base readers; cache helpers; storage benchmark | Canonical ForkTree leaves/value packs and public projections | row-group manifests/columns and obsolete cache/base helpers |
| Persisted working diff | `TrackedWorkingDiff`; branch switch/create/no-op; merge analysis/branch; undo/redo; observe; recovery; `filesystem_working_diff` and working-diff SQL providers; old writer in transaction commit | selected-root versus transaction-local or authenticated root-to-root diff | Move every reader first; then delete old writer, marker codec, marker space, and acceptance fixtures |
| Changelog and graph | `changelog/{context,store,codec,materialization,mod}.rs`; `stage_transaction_append`; commit/change exact and scans; `commit_graph/{context,walker}.rs`; history SQL/providers; entity/file/directory history; observe; merge-base and reachable traversal | CommitCatalog and unified raw-UUID ChangeCatalog, immutable Commit/Change/RefChange objects with fail-closed owner back-edges | `changelog/store.rs`, `changelog/codec.rs`, old materialization, graph durable walker/storage and all three spaces |
| Branch control/ref serving | `branch/{control,mod,refs,lifecycle}.rs`; `BranchHeadControlReader`; load/scan/observed heads; stage/delete/precondition; create/switch/delete/rename; session create/switch; `lix_branch` and `lix_branch_ref` providers | coherent global+branch selector pair; BranchSnapshot edge to RefChange object; unified ChangeCatalog | `branch/control.rs`, physical branch-control implementations and `BRANCH_HEAD_CONTROL_SPACE`; no selector-owned `ref_change_id` |
| Binary CAS | `binary_cas/{context,kv,stats,mod}.rs`; `stage_manifest`, `stage_manifest_chunk`, `stage_chunk`, presence probes, manifest scans, full/range loads; filesystem/path index, handle, JSON store, changelog materialization, file/entity/directory/history SQL, transaction and session context | object-space authenticated chunks/manifests, typed segmented reads, owner-side presence validation | `binary_cas/kv.rs`, old CAS codecs and all four spaces; no whole-payload fallback or second presence map |
| Multipart upload | `session/media_upload.rs`; load/stage upload state and manifest leaves; completion; abort/expiry; storage bench | UploadSelector -> UploadProgress -> ReceiptTree -> UploadPart -> chunks, under the same global epoch | both upload spaces/codecs and predecessor/cumulative receipt shapes |
| Checkpoint/recovery/retention and GC | `gc.rs`; checkpoint/recovery state; reachability queue/delta; tree sweep cursor/epoch/mark; semantic deletion; session GC/execute; commit and plugin publication; storage bench | typed selector root universe; persisted `GcProgressSelectorV2`; authenticated radix mark packs, queue packs, live-branch packs and bounded `EdgePager`; exact raw epoch fence | seven old spaces, V1/in-memory discovery codecs, old queue/tree/CAS sweep paths. Keep a simplified coordinator that consumes owner summaries only. |
| Plugin checkpoint | `transaction/plugin_checkpoint.rs`; plugin registry/runtime; GC roots | ordinary tracked plugin registry plus authenticated WASM manifest edges reachable from retained roots | plugin checkpoint module/codec/space |
| Independent untracked/session authorities | JSON store, filesystem path index, idempotency, protocol init, observe revisions | Their existing narrow semantic owners | No deletion unless separately owner-switched; static gates prevent them from acquiring tracked/root semantics |

The gross direct implementation set is about 66K lines on f77. The accepted
45--52K net deletion remains realistic because public SQL/session/file/history
facades, transaction-local validation, untracked storage, and a bounded GC
coordinator remain. Only `git diff --numstat` and zero residue at the final
candidate can establish the actual deletion.

### Exact f77 path closure by durable family

These are the production files containing a direct space token or a direct
owner API invocation. Public callers named in the table above remain part of
the compiler closure even when they do not mention a physical token.

- tracked/current 23-space closure:
  `columnar_row_group.rs`, `commit_graph/context.rs`, `engine.rs`, `gc.rs`,
  `live_state/entity_decoded_column_cache.rs`, `live_state/mod.rs`,
  `live_state/tracked_head.rs`, `live_state/tracked_head/hot.rs`,
  `session/execute.rs`, `storage_bench.rs`, `tracked_state/context.rs`,
  `tracked_state/current_state_data_part.rs`, `tracked_state/mod.rs`,
  `tracked_state/mutation_directory.rs`, `tracked_state/scoped_range.rs`,
  `tracked_state/storage.rs`, `tracked_state/tree.rs`, and
  `transaction/commit.rs`;
- changelog/branch closure: `branch/control.rs`, `branch/mod.rs`,
  `changelog/context.rs`, `changelog/materialization.rs`, `changelog/mod.rs`,
  `changelog/store.rs`, `commit_graph/context.rs`, `commit_graph/walker.rs`,
  `engine.rs`, `gc.rs`, `sql2/providers/change.rs`, `storage_bench.rs`,
  `tracked_state/context.rs`, `tracked_state/storage.rs`, and
  `transaction/commit.rs`;
- CAS/upload closure: `binary_cas/context.rs`, `binary_cas/kv.rs`,
  `binary_cas/stats.rs`, `session/media_upload.rs`, `storage_bench.rs`, and
  `tracked_state/storage.rs`;
- GC/plugin closure: `gc.rs`, `storage_bench.rs`,
  `tracked_state/storage.rs`, and `transaction/plugin_checkpoint.rs`;
- retained-independent closure: `engine.rs`, `filesystem/path_index.rs`,
  `gc.rs`, `init.rs`, `json_store/context.rs`, `json_store/mod.rs`,
  `json_store/store.rs`, `live_state/tracked_head/hot.rs`,
  `session/idempotency.rs`, `session/mod.rs`, `storage_adapter/context.rs`,
  `storage_adapter/spaces.rs`, `storage_bench.rs`,
  `tracked_state/storage.rs`, and `transaction/context.rs`;
- rebuildable revision closure: `catalog/revision.rs`,
  `storage_adapter/context.rs`, and `storage_adapter/spaces.rs`.

The indirect API closure additionally includes `checkpoint.rs`, `init.rs`,
`session/checkpoint.rs`, `session/context.rs`, `session/create_branch.rs`,
`session/merge/analysis.rs`, `session/merge/branch.rs`,
`session/undo_redo.rs`, `sql2/providers/file_history.rs`,
`sql2/providers/filesystem_working_diff.rs`, tracked-state diff/current-state/
row-materialization modules, branch lifecycle/refs, functions context/state,
filesystem read/path APIs, JSON store callers, SQL entity/file/directory/
history providers, plugin registry/runtime, transaction context, and the
public session/engine facades. Compiler errors, followed by zero exact residue,
are the final authority on this indirect list.

## Compile-order cycles

1. **Serving/publication cycle.** Current readers require hot/current
   generations emitted by the old transaction writer. The new writer can only
   publish one authority after consumers accept a coherent ForkTree snapshot.
   Break it by moving readers first in deliberately non-compiling source, then
   replacing the writer; never run a new reader against old bytes.
2. **History/graph/publication cycle.** Commit publication currently appends
   changelog rows and tracked manifests that graph/history readers consume.
   Move exact lookup, scans, pagination, graph, and history to the two catalogs
   before writer cut; publish objects and catalog roots atomically afterward.
3. **Branch/ref/history cycle.** Branch control owns head state while
   `ref_change_id` is materialized through changelog/serving rows. Move branch
   reads and RefChange reads together, then atomically publish selector and
   ChangeCatalog edge; a selector must not duplicate the ID.
4. **Working-diff cycle.** Branch/no-op/merge/undo/recovery readers depend on a
   marker written by commit. Replace all readers with root-based diff before
   removing its writer. Reversing these two steps either loses semantics or
   creates a tempting compatibility bridge.
5. **Blob/upload/state cycle.** File rows name old CAS manifests; upload
   progress roots old chunks; completion moves reachability into tracked state.
   Move all manifest/chunk/receipt readers first, then atomically cut ordinary,
   upload, plugin, and completion writers. Delete the old presence authority in
   the same wave.
6. **Root/GC/publication cycle.** Old GC enumerates old tracked, branch,
   checkpoint, history, plugin, upload and CAS roots, while every new writer
   must rotate one epoch. Install typed root enumeration and V2 bounded
   maintenance readers before writer cut; then switch every logical writer and
   sweep together. A mixed epoch model is not runnable.
7. **Observe/catalog/filesystem cycle.** Invalidations, path materialization,
   catalog/schema and filesystem providers read current-serving outputs. Keep
   their public semantics, but bind them to `view_id`/selector changes before
   deleting hot generations. Rebuildable revisions cannot become row or root
   authority.

## Required non-runnable wave

No step below is an accepted commit, executable checkpoint, feature flag, or
partial PR. Source may be formatted mechanically, but the first production
compile occurs only after Step 12.

1. Integrate exact f77 and exact frozen Stage 1 once. Resolve only real source
   conflicts; re-run the static owner boundary before editing consumers.
2. Seal descriptors and plans first: `SpaceId` internals and `StorageSpace`
   fields/constructors become unavailable to external/other-owner code;
   ForkTree mutation is expressible only through typed staging,
   `PreparedPublication`, and private-field sweep plans.
3. Introduce transaction/session coherent view plumbing: one `begin_read`, one
   same-handle selector `get_many`, same-handle graph traversal, raw-pair
   `view_id`, and exact raw publication preconditions.
4. Move all current point/range/exact/file/directory/catalog/plugin/observe
   readers to typed global/local state plus transaction-local and
   untracked-only overlays. Preserve f77 canonical HOT ordering semantics in
   the new ordered merge, then make the old helpers unreachable.
5. Move commit/change/ref-change exact lookup, raw-UUID ordered resume,
   history, graph and observe readers to CommitCatalog/unified ChangeCatalog;
   validate every selected historical owner back-edge.
6. Move branch create/delete/rename/switch, diff/merge, checkpoint, undo/redo
   and recovery consumers to typed selector pairs and root operations.
7. Replace every persisted working-diff reader with authenticated selected-root
   versus transaction-local/root-to-root diff. Confirm no consumer remains
   before touching its writer.
8. Move blob/plugin/file full/range readers and open-upload traversal to the
   object graph. Receipt reads use the bounded ReceiptTree only.
9. Move GC discovery/resume readers to the complete selector root universe and
   persisted V2 radix mark/queue/continuation graph. Reader pins, open uploads,
   branches and retained history are safe-point roots; no in-memory global set
   is allowed.
10. Cut writers last in one atomic-owner pass: tracked transaction, metadata
    catalogs, standalone RefChange, branch/root-only, checkpoint/recovery/
    undo/redo, ordinary CAS, multipart part/completion/abort, plugin, retention
    and every sweep. Every logical root mutation and deduplicated publication
    rotates the same global epoch.
11. Remove the old working-diff writer, then delete all old writers/codecs and
    the 41 superseded spaces. Delete the listed modules and tracked portions;
    remove V1/unbounded GC discovery and any raw descriptor escape.
12. Run the residue oracle and external compile-fail probe. Only a zero-residue
    result and rejected forge probe permit the first all-feature compile.

## Complexity and resource risks

| Operation | Current f77 shape | Required ForkTree shape | Regression gate |
|---|---|---|---|
| tracked publication | Multiple hot/base/delta/index/materialization paths; measured work can approach changed identities times overlapping physical owners | `O(U log_F N + Z)`, one sorted mutation source and one atomic selector/epoch publication | backend calls/bytes, allocations and settled disk at 1K/50K with U=1/100/1K; no second mutation directory |
| point/range current read | Generation selection plus hot/base/current-part routing; successful point may be bounded but authority proof spans several planes | two authenticated trees: point `O(log_F N)`, range `O(log_F N + output)` plus overlay merge | global/local value/NULL/tombstone, f77 canonical order, no full collection scan on successful point |
| exact metadata/pagination | Separate commit/change/order/materialization paths | one CommitCatalog and one unified ChangeCatalog: `O(log_F M)` exact, `O(log_F M + page)` resume | raw UUID order, bounded start-after, view-bound token, corrupt owner back-edge fail-closed |
| working diff/diff/merge | persisted marker plus physical/current-state diffs | authenticated hash-pruned root diff: `O(changed paths + output + conflicts)` | no whole-tree comparison for no-op/small diff; deterministic NULL/conflict order |
| branch/checkpoint/undo/redo | branch controls and specialized checkpoint/recovery roots | `O(1)` selector/root movement plus catalog insertion when metadata is created | no `O(branches)` global update; coherent two-selector stale races |
| multipart part | upload state/leaf and CAS presence paths | `O(part bytes + log_F P + new chunks)`, `O(C + log_F P)` memory | duplicate/gap/out-of-order; no predecessor/list growth; completion `O(P)` streaming |
| GC | old queues/marks/cursors and broad root decoding; unbounded discovery is possible | `O(S + Q + R + O)` total, page/pack-bounded retained memory | 1K/50K roots/reachable/orphans, crash/reopen each phase, reader-pin low watermark, both epoch race orders |

The accepted density/physical-amplification evidence remains separate and
unchanged. This audit creates no new performance claim. The main Stage-2 risk
is accidentally preserving a current-serving accelerator as authority to avoid
path-copy cost; the residue and sealed API gates reject that shortcut.

## Conflict-sensitive first-compile gates

After zero residue, run focused gates before any broad workspace matrix:

1. descriptor/owner compile-fail probes, sealed-owner source conformance,
   canonical encoding and complete selected-graph corruption tests;
2. f77 canonical HOT ordering cases re-expressed through global/local ordered
   merge, including fixed file buckets, exclusions, duplicates, NULL and
   tombstones;
3. SQL/JSON-pointer CRUD, file/directory APIs, prepared/public DML,
   transaction-local visibility, observe invalidation and filesystem fuzz;
4. CommitCatalog/unified ChangeCatalog exact+resume, graph/history, standalone
   RefChange chronology and historical selected closure;
5. branch create/switch/delete/rename, global-first/branch-first stale writers,
   diff/apply/revert, semantic merge/fuzz, checkpoint/undo/redo/recovery and
   cold reopen;
6. multipart duplicate/gap/completion/abort/expiry, ordinary and upload
   publication-first/GC-first races, plugin WASM, shared/final chunk release,
   and 64/512 MiB segmented full/range controls;
7. V2 bounded GC crash at each phase, corrupt/missing/duplicate packs,
   >one-page root/edge queues, reader pins and safe-point advance, final
   reference, RocksDB and SlateDB reopen;
8. canonical `cargo fmt --all -- --check`, `git diff --check`, warnings-denied
   Clippy/all-feature check, then the complete accepted public-semantics oracle.

No performance gate may weaken fail-closed authentication, add a derived
serving directory, revive full replacement, or retain an old reader/writer.

## Frozen negative gate

`forktree_stage2_oracle/main.rs` is a standalone source scanner. It rejects:

- all 41 superseded space names;
- exact legacy owner APIs that survive a space rename;
- 14 modules whose physical implementation must disappear;
- V1/unbounded GC owner shapes;
- public raw `SpaceId` construction and public `StorageSpace` fields or
  constructors; and
- absence of the accepted object/selector, catalog, upload and bounded-GC
  owner types.

`space_forge_rejection.rs` is an external compile-fail probe. It reconstructs
the object-space numeric ID without importing `OBJECT_SPACE` and calls generic
`put_many`/`delete_many`. It compiles on f77 by design and must fail on the
first runnable candidate. This closes the equivalent-token forgery hole rather
than merely hiding one constant.

The scanner is intentionally exact-token based. It does not reject words such
as “legacy” or “fallback” in corruption tests, documentation, or public error
messages. Test fixtures containing old bytes may remain only to prove protocol
rejection; they cannot contain production readers or accepted decode paths.
