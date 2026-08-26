#![cfg_attr(not(feature = "storage-benches"), allow(dead_code, unused_imports))]

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
#[cfg(test)]
pub(crate) use diff::{arm_diff_commits_test_probe, take_diff_commits_test_probe};
mod diff_id;
mod merge;
pub(crate) mod mutation_directory;
pub(crate) mod replacement_part;
mod row_pk_index;
mod row_materialization;
mod scoped_current_state;
pub(crate) mod scoped_range;
mod storage;
mod tree;
mod types;

pub(crate) use codec::{encode_key_ref, encode_single_string_key_ref_into};
pub(crate) use commit_root_rebuild::{
    load_rebuild_plans_to_nearest_available_root,
    load_rebuild_plans_to_nearest_available_root_bounded, stage_rebuild_plan_with_writer,
    try_stage_collapsed_rebuild_plans_with_writer,
};
#[cfg(test)]
pub(crate) use context::DIFF_ROW_CREATED_AT_VALIDATIONS;
pub(crate) use context::{
    TrackedStateContext, TrackedStateStoreReader, descriptor_dependency_cascade_file_ids,
};
pub(crate) use current_state_data_part::{
    CURRENT_STATE_DATA_PART_SPACE, decode_current_state_data_part_commit_ids,
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
    LAYOUT_BOUNDED_DIRECT, LAYOUT_BOUNDED_INDIRECT, LAYOUT_COMPACT_REPLACEMENT,
    LAYOUT_DIRECT_ROWS_ONLY, MUTATION_DIRECTORY_NODE_SPACE, MutationDirectoryRoot,
    collect_mutation_directory_node_ids,
};
pub(crate) use replacement_part::{
    EncodedReplacementPart, REPLACEMENT_PART_MAX_ROWS, REPLACEMENT_PART_TARGET_BYTES,
    ReplacementPartRowRef, encode_replacement_part_with_compressor,
};
pub(crate) use row_pk_index::{
    backfill_row_pk_index_for_commit, decode_row_pk_index_key, row_pk_index_scan_request,
    stage_row_pk_index_from_deltas, stage_row_pk_index_from_members,
    with_row_pk_index_mutations,
};
pub(crate) use codec::TrackedStateMutationBatchBuilder;
pub(crate) use tree::TrackedStateTree;
pub(crate) use row_materialization::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    MaterializedTrackedStateRowRef, materialize_batch_from_index_entries,
    materialize_batch_from_index_entry_refs,
};
#[cfg(test)]
pub(crate) use scoped_current_state::attest_scoped_range_root;
pub(crate) use scoped_current_state::incomplete_touched_scope_filter;
pub(crate) use scoped_range::{
    SCOPED_RANGE_NODE_SPACE, ScopedRangeRoot, validate_scoped_range_trees,
};
pub(crate) use storage::TRACKED_STATE_TREE_CHUNK_SPACE;
#[cfg(feature = "storage-benches")]
pub(crate) use storage::decode_change_locator;
pub(crate) use storage::load_commit_state_authority_ids;
pub(crate) use storage::stage_commit_state_manifest;
#[cfg(test)]
pub(crate) use storage::stage_sweep_unreachable_content_nodes;
#[cfg(test)]
pub(crate) use storage::{
    arm_point_replay_authority_batch_probe_for_test,
    reset_commit_delta_scan_probe_for_test, take_commit_delta_scan_probe_for_test,
    take_point_replay_authority_batch_probe_for_test,
};
pub(crate) use storage::{
    AuthoritativeLiveChangeRequest, CertifiedCommitStateTopologyParent,
    CommitDeltaChangeLocator, CommitDeltaLiveMembershipCursor,
    CommitDeltaMember, CommitDeltaPointReadCache, CommitDeltaReplacementGeneration,
    CommitDeltaReplacementScope, EnvelopeCertifiedNativeProjectionBatch,
    EnvelopeCertifiedNativeProjectionSegment, ExclusiveRowSnapshotBatch,
    OrderedAddressableCommitDeltaStage, PublishedCommitStateTopology, StagedCommitStateManifest,
    TrackedStateChunkOverlay,
    commit_delta_contains_schema, commit_delta_member_scopes, commit_history_is_deferred,
    deferred_commit_global_scope,
    complete_state_fence_change_owner_commit_ids, direct_change_locator,
    deferred_commit_history_ids,
    encode_commit_state_manifest_replacement_for_migration, load_authoritative_live_change_records,
    load_change_record_by_id,
    load_commit_delta_members_with_payloads,
    load_commit_delta_members_with_payloads_for_schemas, load_commit_delta_replay_metadata,
    load_commit_delta_selection_certificate, load_commit_history_members_with_payloads_for_schemas,
    has_deferred_commit_history, load_commit_mutation_directory_roots,
    load_commit_state_manifest, load_commit_state_manifests, load_exclusive_row_snapshots,
    load_local_commit_delta_members_with_payloads, load_local_selected_change_owner_commit_ids,
    load_owned_commit_delta_entries, load_owned_commit_delta_entries_one_ordered_ref,
    load_published_commit_state_topology, load_retained_commit_snapshots_for_schemas,
    scan_change_records_from_commit_deltas, scan_commit_delta_inventory, scan_commit_delta_members,
    scan_commit_delta_values, scan_commit_state_manifest_commit_ids,
    selected_change_selection_fingerprint,
    stage_addressable_commit_deltas, stage_addressable_commit_deltas_with_selected_source,
    stage_certified_commit_state_manifest_with_handle, stage_change_locators,
    stage_commit_deltas_for_commit_state, stage_commit_history_available,
    stage_commit_history_deferred, stage_commit_history_deferred_with_scope,
    stage_commit_state_manifest_with_handle,
    staged_commit_delta_members, staged_commit_delta_segment_bytes,
    stage_current_state_scoped_ranges_from_complete_state_source,
    sync_history_required_for_commits,
    stage_current_state_scoped_ranges_from_published_parent,
    stage_current_state_scoped_ranges_from_published_topology_parent,
    stage_current_state_scoped_ranges_from_staged_parent,
    stage_current_state_scoped_ranges_from_topology, stage_imported_addressable_commit_deltas,
    stage_imported_addressable_commit_deltas_with_selected_source,
    stage_ordered_addressable_commit_deltas, stage_ordered_addressable_replacement_parts,
    stage_ordered_columnar_mutations, stage_preencoded_ordered_addressable_replacement_parts,
    stage_prefixed_ordered_addressable_replacement_parts,
};
pub(crate) use storage::{
    RetainedPhysicalState, load_native_current_state_part_owners,
    stage_retire_commit_physical_state, stage_retire_commit_physical_state_bounded,
};
// Manufacturing a repository swept by the code that shipped before the
// history-retention fix is the only caller: `session::gc` needs to delete one
// commit's physical delta the way that sweep did. Deliberately test-gated
// rather than exported outright -- production reclaim reaches this through
// `stage_retire_commit_physical_state`, and a second, unconditional route to
// deleting a manifest is a footgun that would outlive the fixture it was added
// for. This attribute belongs to the `use` on the next line and nothing else;
// do not insert between them.
#[cfg(test)]
pub(crate) use storage::stage_delete_commit_state_manifest_for_gc;
// The storage-space constants are what the space registry
// (`crate::storage_spaces`) and its layout invariants are built from. The
// registry is compiled in every configuration, so these are too.
pub(crate) use storage::{
    TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
    TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE, TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE,
    TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
};
#[cfg(test)]
pub(crate) use storage::{
    commit_state_authority_key, load_commit_delta_change_ids, load_snapshot_commit_root,
    stage_resealed_commit_state_manifest_for_test,
};
#[cfg(test)]
pub(crate) use storage::{
    stage_delete_change_locators, stage_delete_commit_delta_inventory_entry,
    validate_current_state_scoped_range_serving_base_manifest,
};
#[cfg(test)]
pub(crate) use tree::test_gc_leaf_chunk;
pub(crate) use types::CurrentStatePartSource;
pub(crate) use types::TrackedStateRootId;
pub(crate) use types::{COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH};
pub(crate) use types::{
    ColumnarMutationPartSet, CommitDeltaLifecycleSummary, CommitStateManifest,
    CommitStateMutationInventory, CommitStateReplayDebt, CommitStateTouchedScopeFilter,
    MaterializedTrackedStateRow, RowPkRangeBound, TrackedStateBaseCoordinate,
    TrackedStateCommitDeltaRef, TrackedStateCommitRoot, TrackedStateCommitRootParent,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValue, TrackedStateIndexValueRef,
    TrackedStateReadColumns,
    TrackedStateRootMutationRef, TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
    row_pk_satisfies_bounds,
};
#[cfg(test)]
pub(crate) use types::{CurrentStatePartDescriptor, ReplacementPartSource};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};

#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
