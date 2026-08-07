#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 REPOSITORY_ROOT" >&2
  exit 2
fi

repo=$1
base=4763408467d265b288a124e24b1d47be423f5d17
head=b5e78190f49cab5de7bb19b6f967706c214363b6
oracle="$repo/packages/lix/tests/forktree_stage2_execution_oracle/main.rs"

test "$(git -C "$repo" rev-parse "$head^{tree}")" = c913465505bc773d21a6e2804530287ee937a3f1

diff -u \
  <(printf '%s\n' \
    packages/lix/src/binary_cas/kv.rs \
    packages/lix/src/binary_cas/mod.rs \
    packages/lix/src/branch/control.rs \
    packages/lix/src/branch/mod.rs \
    packages/lix/src/filesystem/mod.rs \
    packages/lix/src/filesystem/read.rs \
    packages/lix/src/gc.rs \
    packages/lix/src/live_state/tracked_head.rs \
    packages/lix/src/live_state/tracked_head/hot.rs \
    packages/lix/src/plugin/mod.rs \
    packages/lix/src/plugin/registry.rs \
    packages/lix/src/session/media_upload.rs \
    packages/lix/src/session/merge/branch.rs \
    packages/lix/src/session/mod.rs \
    packages/lix/src/storage_bench.rs \
    packages/lix/src/tracked_state/current_state_data_part.rs \
    packages/lix/src/tracked_state/mod.rs \
    packages/lix/src/tracked_state/storage.rs \
    packages/lix/src/tracked_state/types.rs \
    packages/lix/src/transaction/bench_support.rs \
    packages/lix/src/transaction/commit.rs) \
  <(git -C "$repo" diff --name-only "$base..$head" -- packages/lix/src | sort)

while IFS= read -r symbol; do
  rg -F -q "$symbol" "$repo/packages/lix/src"
  rg -F -q "\"$symbol\"" "$oracle"
done <<'SYMBOLS'
load_mutation_epoch
stage_mutation_epoch
stage_reclaim_unreachable_binary_cas
mark_live_blob
validate_live_manifest_identity
mark_live_chunk_expectation
decode_manifest_chunk_key
verify_live_chunk_presence
BinaryCasGcSweep
stage_gc_reclamation
stage_reclaimable_upload_receipts
decode_upload_manifest_leaf_upload_id
validate_upload_id_for_storage
invalid_upload_storage
collect_gc_binary_blob_roots
blob_id_from_snapshot
BranchHeadTrackedReachability
tracked_reachability
AuthenticatedControlCommitReachability
authenticated_control_commit_reachability
fold_reachability_batches
decode_reachability_batch
collect_all_reachability_checkpoint_roots
collect_active_point_replay_dependencies
AuthenticatedServingDependencyClosure
load_authenticated_serving_dependency_closure
load_authenticated_repository_retention
scan_packed_current_base_provenance_rows
tracked_serving_commit_dependencies
decode_current_state_data_part_commit_ids
RetainedCommitSnapshot
load_local_selected_change_owner_commit_ids
validate_selected_owner_record
load_retained_commit_snapshots_for_schemas
may_contain_finite_selected_members
collect_gc_wasm_blob_roots
extend_registry_wasm_roots
RepositoryGcCommitBenchResult
collect_repository_gc_for_bench
SYMBOLS

for facade in \
  stage_repository_gc \
  stage_repository_gc_with_preconditions \
  load_plugin_registry_at_commit; do
  rg -F -q "$facade" "$repo/packages/lix/src"
done

echo "landed-1258 path/symbol map PASS"
