#[cfg(feature = "storage-benches")]
mod bench_support;
mod codec;
mod commit_root_rebuild;
mod context;
mod current_state_data_part;
pub(crate) mod current_state_envelope;
mod diff;
mod diff_id;
mod merge;
pub(crate) mod replacement_part;
mod row_materialization;
mod scoped_current_state;
pub(crate) mod scoped_range;
mod storage;
mod tree;
mod types;

pub(crate) use codec::{encode_key_ref, encode_single_string_key_ref_into};
pub(crate) use commit_root_rebuild::{
    load_rebuild_plans_to_nearest_available_root, stage_rebuild_plan_with_writer,
};
pub(crate) use context::{
    TrackedStateContext, TrackedStateStoreReader, descriptor_dependency_cascade_file_ids,
};
pub(crate) use current_state_data_part::{
    CURRENT_STATE_DATA_PART_REFS_SPACE, CURRENT_STATE_DATA_PART_SPACE,
    decode_current_state_data_part_refs,
};
pub(crate) use current_state_envelope::current_state_descriptor_from_scoped_range_part;
pub(crate) use diff::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStatePayloadBatch, TrackedStatePayloadRef,
};
pub(crate) use diff_id::{decode_diff_id, encode_diff_id};
pub(crate) use merge::{
    TrackedStateMergeConflict, TrackedStateMergePick, TrackedStateMergePlan,
    merge_payload_fallback_ids, plan_merge,
};
pub(crate) use row_materialization::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    MaterializedTrackedStateRowRef, materialize_batch_from_index_entries,
    materialize_batch_from_index_entry_refs,
};
pub(crate) use scoped_current_state::incomplete_touched_scope_filter;
pub(crate) use scoped_range::{SCOPED_RANGE_NODE_SPACE, validate_scoped_range_trees};
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use storage::stage_commit_state_manifest;
pub(crate) use storage::{
    CommitDeltaChangeLocator, CommitDeltaLiveMembershipCursor, CommitDeltaMember,
    CommitDeltaPointReadCache, CommitDeltaReplacementGeneration, CommitDeltaReplacementScope,
    OrderedAddressableCommitDeltaStage, commit_delta_contains_schema, direct_change_locator,
    load_change_record_by_id, load_commit_delta_change_records,
    load_commit_delta_members_with_payloads, load_commit_delta_members_with_payloads_for_schemas,
    load_commit_delta_replay_metadata, load_commit_delta_selection_certificate,
    load_commit_state_manifest, load_commit_state_manifests, load_owned_commit_delta_entries,
    load_owned_commit_delta_entries_one_ordered_ref, load_published_commit_state_manifest,
    scan_change_records_from_commit_deltas, scan_commit_delta_inventory, scan_commit_delta_values,
    selected_change_selection_fingerprint, stage_addressable_commit_deltas,
    stage_addressable_commit_deltas_with_selected_source, stage_certified_commit_state_manifest,
    stage_certified_commit_state_manifest_with_handle, stage_change_locators,
    stage_commit_deltas_for_commit_state, stage_commit_state_manifest_with_handle,
    stage_current_state_scoped_ranges_from_published_parent,
    stage_current_state_scoped_ranges_from_staged_parent, stage_delete_change_locators,
    stage_delete_commit_delta_inventory_entry, stage_ordered_addressable_commit_deltas,
    stage_ordered_addressable_replacement_parts, stage_ordered_columnar_mutations,
    validate_current_state_scoped_range_parent_manifest,
};
#[cfg(feature = "storage-benches")]
pub(crate) use storage::{
    TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, TRACKED_STATE_TREE_CHUNK_SPACE,
    decode_change_locator,
};
#[cfg(all(test, not(feature = "storage-benches")))]
pub(crate) use storage::{
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
#[cfg(test)]
pub(crate) use storage::{
    TRACKED_STATE_COMMIT_STATE_SEAL_SPACE, change_id_from_packed_address,
    load_authoritative_commit_root, load_commit_delta_change_ids,
    load_complete_current_state_values_from_scoped_root, scan_commit_delta_members,
    stage_resealed_commit_state_manifest_for_test,
};
pub(crate) use types::{COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH};
pub(crate) use types::{
    ColumnarMutationPartSet, CommitStateManifest, CommitStateMutationInventory,
    CommitStateReplayDebt, MaterializedTrackedStateRow, TrackedStateBaseCoordinate,
    TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateDeltaRef, TrackedStateFilter,
    TrackedStateIndexValue, TrackedStateReadColumns, TrackedStateRootMutationRef,
    TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
