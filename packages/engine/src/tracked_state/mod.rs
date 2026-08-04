#[cfg(feature = "storage-benches")]
mod bench_support;
mod codec;
mod context;
mod current_state_data_part;
mod current_state_part;
mod diff;
mod diff_id;
mod merge;
pub(crate) mod replacement_part;
mod row_materialization;
mod storage;
mod types;

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use codec::encode_key;
pub(crate) use codec::{decode_key_borrowed, encode_key_ref, encode_single_string_key_ref_into};
pub(crate) use context::{
    TrackedStateContext, TrackedStateStoreReader, descriptor_dependency_cascade_file_ids,
};
pub(crate) use current_state_data_part::{
    ArrowStateInputRowRef, CURRENT_STATE_DATA_PART_REFS_SPACE, CurrentStateDataRow,
    ENTITY_ARROW_STATE_COMMIT_ID_METADATA, ENTITY_ARROW_STATE_CREATED_AT_METADATA,
    ENTITY_ARROW_STATE_LAYOUT, ENTITY_ARROW_STATE_NAMESPACE,
    ENTITY_ARROW_STATE_SCHEMA_KEY_METADATA, ENTITY_ARROW_STATE_UPDATED_AT_METADATA,
    HydratedArrowStatePayload, decode_current_state_data_part, decode_current_state_data_part_refs,
    encode_authoritative_arrow_state_rows, stage_current_state_ref_summary,
};
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use current_state_part::empty_current_state_catalog_root;
pub(crate) use current_state_part::{
    CURRENT_STATE_CATALOG_SPACE, CURRENT_STATE_PART_DIRECTORY_SPACE,
    load_current_state_catalog_entry,
    load_current_state_catalog_reachability_many,
    load_current_state_part_directory_reachability_many,
    validate_current_state_catalog_transition_root,
};
pub(crate) use diff::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStatePayloadBatch, TrackedStatePayloadRef,
};
pub(crate) use diff_id::{decode_diff_id, encode_diff_id};
pub(crate) use merge::{
    TrackedStateMergeConflict, TrackedStateMergePick, TrackedStateMergePlan, plan_merge,
};
pub(crate) use row_materialization::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    MaterializedTrackedStateRowRef, materialize_batch_from_arrow_rows,
};
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use storage::stage_commit_state_manifest;
pub(crate) use storage::{
    CommitDeltaChangeLocator, CommitDeltaLifecycleSummary, CommitDeltaMember,
    CommitDeltaReplacementGeneration, CommitDeltaReplacementScope,
    OrderedAddressableCommitDeltaStage, addressable_change_id, direct_change_locator,
    finalize_commit_delta_event_coordinates, load_change_origin_keys_by_ids,
    load_change_record_by_id, load_commit_delta_members_with_payloads_for_history,
    load_commit_delta_selection_certificate, load_commit_state_manifest,
    load_commit_state_manifests, load_complete_current_state_coordinates_encoded,
    load_complete_current_state_rows_with_coordinates_encoded,
    load_complete_current_state_rows_with_coordinates_encoded_cached,
    load_current_state_payloads_at_coordinates, load_current_state_scope_descriptors,
    load_published_commit_state_manifest, scan_change_records_from_commit_deltas,
    scan_commit_delta_inventory, selected_change_selection_fingerprint,
    stage_addressable_commit_deltas, stage_addressable_commit_deltas_with_selected_source,
    stage_arrow_native_replacement_manifest, stage_certified_commit_state_manifest_with_handle,
    stage_change_locators, stage_commit_deltas_for_commit_state,
    stage_current_state_catalog_from_published_parent,
    stage_current_state_catalog_from_staged_parent, stage_delete_change_locators,
    stage_delete_commit_delta_inventory_entry, stage_ordered_addressable_commit_deltas,
    stage_ordered_addressable_replacement_parts, stage_ordered_arrow_native_commit_deltas,
    staged_commit_delta_members_for_write, validate_current_state_catalog_parent_manifest,
};
#[cfg(feature = "storage-benches")]
pub(crate) use storage::{
    TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, decode_change_locator,
};
#[cfg(all(test, not(feature = "storage-benches")))]
pub(crate) use storage::{
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
#[cfg(test)]
pub(crate) use storage::{
    TRACKED_STATE_COMMIT_STATE_SEAL_SPACE, load_commit_delta_change_ids, scan_commit_delta_members,
    stage_resealed_commit_state_manifest_for_test,
};
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use storage::{
    load_commit_delta_members_with_payloads, stage_current_state_catalog_from_test_parent,
    staged_commit_state_manifest_for_test,
};
#[cfg(test)]
pub(crate) use types::CurrentStateCatalogRoot;
pub(crate) use types::{
    CommitStateManifest, CommitStateMutationInventory, CurrentStatePartDescriptor,
    MaterializedTrackedStateRow, TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValue, TrackedStateIndexValueRef,
    TrackedStateReadColumns, TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
