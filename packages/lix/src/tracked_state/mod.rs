#![cfg_attr(not(feature = "storage-benches"), allow(dead_code, unused_imports))]

use std::collections::BTreeSet;

use crate::LixError;

#[cfg(feature = "storage-benches")]
mod bench_support;
mod codec;
#[cfg(test)]
pub(crate) use codec::tree_decode_row_pk_probe;
mod commit_root_rebuild;
mod context;
mod current_state_data_part;
pub(crate) mod current_state_envelope;
mod diff;
mod diff_id;
mod merge;
mod native_history_body;
pub(crate) mod mutation_directory;
pub(crate) mod replacement_part;
mod row_materialization;
mod scoped_current_state;
pub(crate) mod scoped_range;
mod storage;
mod tree;
mod types;

pub(crate) use codec::{encode_key_ref, encode_single_string_key_ref_into};
pub(crate) use storage::load_commit_state_authority_ids;
pub(crate) use commit_root_rebuild::{
    load_rebuild_plans_to_nearest_available_root, stage_rebuild_plan_with_writer,
    try_stage_collapsed_rebuild_plans_with_writer,
};
pub(crate) use context::{
    TrackedStateContext, TrackedStateStoreReader, descriptor_dependency_cascade_file_ids,
};
#[cfg(test)]
pub(crate) use context::DIFF_ROW_CREATED_AT_VALIDATIONS;
pub(crate) use current_state_data_part::{
    CURRENT_STATE_DATA_PART_REFS_SPACE, CURRENT_STATE_DATA_PART_SPACE,
    decode_current_state_data_part_commit_ids, decode_current_state_data_part_refs,
};
#[cfg(test)]
pub(crate) use current_state_data_part::{CurrentStateDataRow, encode_current_state_data_part};
pub(crate) use current_state_envelope::current_state_descriptor_from_scoped_range_part;
pub(crate) use diff::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStatePayloadBatch, TrackedStatePayloadRef,
};
pub(crate) use diff_id::decode_diff_id;
pub(crate) use merge::{
    TrackedStateMergeConflict, TrackedStateMergePick, TrackedStateMergePlan,
    merge_payload_fallback_ids, plan_merge,
};
pub(crate) use mutation_directory::{
    MUTATION_DIRECTORY_NODE_SPACE, collect_mutation_directory_node_ids,
};
pub(crate) use replacement_part::{EncodedReplacementPart, REPLACEMENT_PART_MAX_ROWS};
pub(crate) use row_materialization::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    MaterializedTrackedStateRowRef, materialize_batch_from_index_entries,
    materialize_batch_from_index_entry_refs,
};
#[cfg(test)]
pub(crate) use scoped_current_state::attest_scoped_range_root;
pub(crate) use scoped_current_state::incomplete_touched_scope_filter;
pub(crate) use scoped_range::{SCOPED_RANGE_NODE_SPACE, validate_scoped_range_trees};
pub(crate) use storage::TRACKED_STATE_TREE_CHUNK_SPACE;
#[cfg(feature = "storage-benches")]
pub(crate) use storage::decode_change_locator;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use storage::stage_commit_state_manifest;
#[cfg(test)]
pub(crate) use storage::stage_sweep_unreachable_content_nodes;
pub(crate) use storage::{
    CertifiedCommitStateTopologyParent, CommitDeltaChangeLocator, CommitDeltaLiveMembershipCursor,
    CommitDeltaMember, CommitDeltaPointReadCache, CommitDeltaReplacementGeneration,
    CommitDeltaReplacementScope, OrderedAddressableCommitDeltaStage, PublishedCommitStateTopology,
    StagedCommitStateManifest, commit_delta_contains_schema, commit_delta_member_scopes,
    direct_change_locator,
    load_change_record_by_id, load_commit_delta_change_records,
    load_commit_delta_members_for_schemas, load_commit_delta_members_with_payloads,
    load_commit_delta_members_with_payloads_for_schemas,
    load_commit_delta_replay_metadata, load_commit_delta_selection_certificate,
    load_commit_mutation_directory_roots, load_commit_state_manifest, load_commit_state_manifests,
    load_local_selected_change_owner_commit_ids, load_owned_commit_delta_entries,
    load_owned_commit_delta_entries_one_ordered_ref, load_published_commit_state_topology,
    load_retained_commit_snapshots_for_schemas, scan_change_records_from_commit_deltas,
    scan_change_records_from_commit_deltas_projected,
    scan_commit_delta_inventory, scan_commit_delta_values, scan_commit_state_manifest_commit_ids,
    selected_change_selection_fingerprint, stage_addressable_commit_deltas,
    stage_addressable_commit_deltas_with_selected_source,
    stage_certified_commit_state_manifest_with_handle, stage_change_locators,
    stage_commit_deltas_for_commit_state, stage_commit_state_manifest_with_handle,
    certify_authored_current_state_body,
    stage_current_state_scoped_ranges_from_published_topology_parent,
    stage_current_state_scoped_ranges_from_staged_parent,
    stage_current_state_scoped_ranges_from_topology, stage_ordered_addressable_commit_deltas,
    stage_ordered_addressable_replacement_parts, stage_ordered_columnar_mutations,
};
pub(crate) use storage::{
    RetainedPhysicalState, collect_current_state_part_json_refs,
    collect_local_commit_delta_json_refs, load_native_current_state_part_owners,
    stage_retire_commit_physical_state,
};
// Manufacturing a repository swept by the code that shipped before the
// history-retention fix is the only caller: `session::gc` needs to delete one
// commit's physical delta the way that sweep did. Deliberately test-gated
// rather than exported outright -- production reclaim reaches this through
// `stage_retire_commit_physical_state`, and a second, unconditional route to
// deleting a manifest is a footgun that would outlive the fixture it was added
// for. This attribute belongs to the `use` on the next line and nothing else;
// do not insert between them.
// The storage-space constants are what the space registry
// (`crate::storage_spaces`) and its layout invariants are built from. The
// registry is compiled in every configuration, so these are too.
pub(crate) use storage::{
    TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
#[cfg(test)]
pub(crate) use storage::{
    change_id_from_packed_address, commit_state_authority_key, load_commit_delta_change_ids,
    load_complete_current_state_values_from_scoped_root, load_snapshot_commit_root,
    scan_commit_delta_members, stage_resealed_commit_state_manifest_for_test,
};
#[cfg(test)]
pub(crate) use storage::{
    stage_delete_change_locators, stage_delete_commit_delta_inventory_entry,
    validate_current_state_scoped_range_serving_base_manifest,
};
#[cfg(test)]
pub(crate) use tree::test_gc_leaf_chunk;
pub(crate) use types::TrackedStateRootId;
pub(crate) use types::{COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH};
pub(crate) use types::{
    ColumnarMutationPartSet, CommitDeltaLifecycleSummary, CommitStateManifest,
    CommitStateMutationInventory, CommitStateReplayDebt, MaterializedTrackedStateRow,
    TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef, TrackedStateCommitRoot,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValue, TrackedStateReadColumns,
    RowPkRangeBound, row_pk_satisfies_bounds,
    TrackedStateRootMutationRef, TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
};
pub(crate) use types::CurrentStatePartSource;
#[cfg(test)]
pub(crate) use types::{
    CurrentStatePartDescriptor, ReplacementPartSource, TrackedStateCommitRootParent,
};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};

/// Builds an authenticated content-addressed closure for a set of retained
/// tracked-state roots. This is deliberately a read-only maintenance helper:
/// the returned hashes are a rebuildable sweep inventory, never serving
/// authority or a replacement for the immutable root metadata.
#[allow(dead_code)]
pub(crate) async fn collect_reachable_tree_chunk_hashes<S>(
    store: &S,
    roots: &[TrackedStateRootId],
) -> Result<BTreeSet<[u8; types::TRACKED_STATE_HASH_BYTES]>, LixError>
where
    S: crate::storage_adapter::StorageAdapterRead + ?Sized,
{
    let tree = tree::TrackedStateTree::new();
    let overlay = storage::TrackedStateChunkOverlay::new();
    let mut reachable = BTreeSet::new();
    for root in roots {
        reachable.extend(
            tree.reachable_chunk_hashes_with_overlay(store, &overlay, root)
                .await?,
        );
    }
    Ok(reachable)
}
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
