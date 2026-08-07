# Landed #1258 to ForkTree Stage-2 deletion map

Status: plan/test evidence only. Exact source range:
`4763408467d265b288a124e24b1d47be423f5d17..b5e78190f49cab5de7bb19b6f967706c214363b6`.
Current tree: `c913465505bc773d21a6e2804530287ee937a3f1`.

The merge introduces 21 `packages/lix/src` paths. Every path is classified
below. “Rewrite” means the public semantic facade may remain only after its
physical implementation calls the sealed ForkTree owner. “Delete” means the
path or named implementation is absent before the first runnable compile.

| # | Exact path | Stage-2 disposition |
|---:|---|---|
| 1 | `binary_cas/kv.rs` | Delete the file. Move authenticated manifest/chunk validation and final-reference semantics into typed ForkTree object loading/publication. This is one of two real b5e+138b textual conflicts. |
| 2 | `binary_cas/mod.rs` | Rewrite public blob facade; delete its KV GC facade and CAS mutation epoch. No old space or second presence authority remains. |
| 3 | `branch/control.rs` | Delete the file and `BRANCH_HEAD_CONTROL_SPACE`; branch/snapshot selectors own reachability. |
| 4 | `branch/mod.rs` | Rewrite exports/callers to typed branch selector/snapshot operations. |
| 5 | `filesystem/mod.rs` | Retain public filesystem semantics; remove CAS-root collector export. |
| 6 | `filesystem/read.rs` | Rewrite file reads/root discovery to selected typed state and authenticated blob-tree edges. |
| 7 | `gc.rs` | Rewrite as a thin consumer of owner-produced bounded pages/status; delete all old queue/control/tree/CAS/root decoders and global in-memory closure sets. |
| 8 | `live_state/tracked_head.rs` | Delete the file after every reader moves to coherent two-root state. |
| 9 | `live_state/tracked_head/hot.rs` | Delete the file; preserve serving/history role semantics through authenticated object edges. |
| 10 | `plugin/mod.rs` | Retain public plugin facade; remove current-layout GC collector exports. |
| 11 | `plugin/registry.rs` | Rewrite registry reads as typed state/object traversal; installed WASM remains an authenticated manifest edge for current and retained roots. |
| 12 | `session/media_upload.rs` | Rewrite to UploadSelector -> UploadProgress -> ReceiptTree -> UploadPart/chunks; delete old upload-space receipt and CAS sweep logic. |
| 13 | `session/merge/branch.rs` | Retain merge semantics; load plugin registry and blob/state edges through one coherent ForkTree view. |
| 14 | `session/mod.rs` | Remove old receipt-GC export; expose typed publication capability only. |
| 15 | `storage_bench.rs` | Rewrite focused oracles/accounting to the new owner, then remove old CAS/GC/space helpers. This is the other real b5e+138b textual conflict. |
| 16 | `tracked_state/current_state_data_part.rs` | Delete the file and its owner/codec. |
| 17 | `tracked_state/mod.rs` | Remove old physical exports; retain only public semantic adapters that delegate to ForkTree. |
| 18 | `tracked_state/storage.rs` | Delete the file after reader-first/writer-last cut. |
| 19 | `tracked_state/types.rs` | Retain only public semantic types; remove finite-selected physical-owner classification. |
| 20 | `transaction/bench_support.rs` | Rewrite test accounting to typed owner metrics; no production authority. |
| 21 | `transaction/commit.rs` | Writer cut last. Preserve both publication/GC race orders and retry semantics through owner-local revision plus GC-generation fences; delete old CAS epoch calls and legacy state publication. |

## Complete landed production-symbol disposition

### Delete with old CAS/upload physical ownership

- `binary_cas/kv.rs`: `load_mutation_epoch`, `stage_mutation_epoch`,
  `stage_reclaim_unreachable_binary_cas`, `mark_live_blob`,
  `validate_live_manifest_identity`, `mark_live_chunk_expectation`,
  `decode_manifest_chunk_key`, `verify_live_chunk_presence`.
- `binary_cas/mod.rs`: `BinaryCasGcSweep`, `stage_gc_reclamation`, and the
  exported `stage_mutation_epoch` facade.
- `session/media_upload.rs`: `stage_reclaimable_upload_receipts`,
  `decode_upload_manifest_leaf_upload_id`, `validate_upload_id_for_storage`,
  and `invalid_upload_storage`.
- `filesystem/read.rs`: `collect_gc_binary_blob_roots` and
  `blob_id_from_snapshot`.

The replacement validates the same public bytes, manifest domains, declared
single/chunk sizes, conflicting expectations, delta program/base layout,
shared chunks, receipt closure, and final-reference release. These rules exist
once behind the object owner; none of the listed functions becomes a wrapper
over old bytes.

### Delete with old retention/reachability ownership

- `branch/control.rs`: `BranchHeadTrackedReachability` and
  `tracked_reachability` plus the pre-existing control reader/writer.
- `gc.rs`: `AuthenticatedControlCommitReachability`,
  `authenticated_control_commit_reachability`, `fold_reachability_batches`,
  `decode_reachability_batch`, `collect_all_reachability_checkpoint_roots`,
  `collect_active_point_replay_dependencies`,
  `AuthenticatedServingDependencyClosure`,
  `load_authenticated_serving_dependency_closure`, and
  `load_authenticated_repository_retention`.
- `live_state/tracked_head/hot.rs`:
  `scan_packed_current_base_provenance_rows` and
  `tracked_serving_commit_dependencies`.
- `tracked_state/current_state_data_part.rs`:
  `decode_current_state_data_part_commit_ids`.
- `tracked_state/storage.rs`: `RetainedCommitSnapshot`,
  `load_local_selected_change_owner_commit_ids`,
  `validate_selected_owner_record`, and
  `load_retained_commit_snapshots_for_schemas`.
- `tracked_state/types.rs`: `may_contain_finite_selected_members`.

`stage_repository_gc` and `stage_repository_gc_with_preconditions` may retain
their public operation names only as a thin call into the sealed ForkTree owner.
Their b5e implementation body, old storage spaces, raw control/queue decoding,
and unbounded closure accumulation are deleted. The replacement root universe
is typed current global/local state (including untracked rows), unified
catalogs, retained commit/history/undo/replay roots, every full-queue
checkpoint/recovery alias, installed plugin manifests, and open ReceiptTrees.

### Rewrite semantic plugin/merge facades

- `plugin/registry.rs::load_plugin_registry_at_commit` may remain as a semantic
  reader over a supplied coherent view. `collect_gc_wasm_blob_roots` and
  `extend_registry_wasm_roots` disappear because manifest edges participate in
  the one object graph directly.
- `session/merge/branch.rs` keeps merge behavior but names no registry/CAS
  storage internals.

### Delete old benchmark bridge, retain its oracle

`RepositoryGcCommitBenchResult` and `collect_repository_gc_for_bench` are
deleted with old accounting. Their assertions are re-expressed against typed
owner metrics: logical/physical rows and bytes, bounded progress, complete root
roles, both race orders, cold reopen, shared/final references, and corruption
fail-closed.

## Required preserved #1258 invariants

1. An unconditional exact publication/GC fence guards every sweep page.
2. Physical serving dependencies validate graph/manifests but become semantic
   liveness only for an authenticated logical root/history/undo/replay role.
3. Every pending reachability-queue batch contributes checkpoint roots even
   when ordinary retirement consumes only its first bounded batch window.
4. Current untracked file rows, retained plugin registry generations, open
   uploads, and blocked history/replay roots are in the mark universe.
5. Delta manifests authenticate their program and persisted base layout before
   deletion; declared-size mismatches and conflicting expectations fail closed.
6. Shared chunks survive until the last reachable manifest/receipt edge is
   released.
7. Publication-first rejects stale GC; GC-first rejects stale publication and
   retry restages missing content. Root-only publication has the same ordering.
8. Reader snapshots remain valid until dropped; physical reuse waits for the
   adapter read low-watermark. No out-of-band object deletion is permitted.

The Stage-2 residue scanner names every physical symbol above that cannot be a
public semantic facade. Zero findings plus the external compile-fail probes are
required before the first runnable compile.
