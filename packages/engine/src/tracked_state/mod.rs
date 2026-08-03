#[cfg(feature = "storage-benches")]
mod bench_support;
mod codec;
mod commit_root_rebuild;
mod context;
mod current_state_part;
mod diff;
mod diff_id;
mod merge;
pub(crate) mod replacement_part;
mod row_materialization;
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
pub(crate) use current_state_part::{
    stage_complete_replacement_current_state_part_set, stage_delete_current_state_part_directory,
};
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
pub(crate) use storage::{
    CommitDeltaChangeLocator, CommitDeltaLiveMembershipCursor, CommitDeltaMember,
    CommitDeltaPointReadCache, CommitDeltaReplacementGeneration, CommitDeltaReplacementScope,
    OrderedAddressableCommitDeltaStage, commit_delta_contains_schema, direct_change_locator,
    load_change_record_by_id, load_commit_delta_change_records,
    load_commit_delta_members_with_payloads, load_commit_delta_members_with_payloads_for_schemas,
    load_commit_delta_replay_metadata, load_commit_delta_selection_certificate,
    load_commit_state_manifest, load_commit_state_manifests, load_owned_commit_delta_entries,
    load_owned_commit_delta_entries_one_ordered_ref, scan_change_records_from_commit_deltas,
    scan_commit_delta_inventory, scan_commit_delta_values, selected_change_selection_fingerprint,
    stage_addressable_commit_deltas, stage_addressable_commit_deltas_with_selected_source,
    stage_change_locators, stage_commit_deltas_for_commit_state, stage_commit_state_manifest,
    stage_delete_change_locators, stage_delete_commit_delta_inventory_entry,
    stage_ordered_addressable_commit_deltas, stage_ordered_addressable_replacement_parts,
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
    load_authoritative_commit_root, load_commit_delta_change_ids, scan_commit_delta_members,
};
pub(crate) use types::{COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH};
pub(crate) use types::{
    CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
    MaterializedTrackedStateRow, TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef,
    TrackedStateCommitRoot, TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValue,
    TrackedStateReadColumns, TrackedStateRootMutationRef, TrackedStateScanRequest,
    TrackedStateSingleStringReplacementRef,
};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
