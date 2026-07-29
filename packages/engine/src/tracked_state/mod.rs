#[cfg(feature = "storage-benches")]
mod bench_support;
mod codec;
mod commit_root_rebuild;
mod context;
mod diff;
mod merge;
mod row_materialization;
mod storage;
mod tree;
mod types;

pub(crate) use codec::encode_key_ref;
pub(crate) use context::{TrackedStateContext, TrackedStateStoreReader};
pub(crate) use diff::{
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStatePayloadBatch, TrackedStatePayloadRef,
};
pub(crate) use merge::{
    TrackedStateMergeConflict, TrackedStateMergePick, TrackedStateMergePlan,
    merge_payload_fallback_ids, plan_merge,
};
pub(crate) use row_materialization::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateExactBatch,
    MaterializedTrackedStateRowRef, materialize_batch_from_index_entries,
    materialize_batch_from_index_entry_refs,
};
#[cfg(test)]
pub(crate) use storage::load_commit_delta_change_ids;
pub(crate) use storage::{
    CommitDeltaChangeLocator, load_change_record_by_id, load_commit_delta_change_records,
    load_commit_delta_members_with_payloads, scan_change_records_from_commit_deltas,
    scan_commit_delta_inventory, stage_change_locators, stage_commit_deltas,
    stage_delete_change_locators, stage_delete_commit_delta_inventory_entry,
    stage_delete_commit_root,
};
#[cfg(feature = "storage-benches")]
pub(crate) use storage::{
    TRACKED_STATE_CHANGE_LOCATOR_SPACE, TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
    TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE, TRACKED_STATE_COMMIT_ROOT_SPACE,
    TRACKED_STATE_TREE_CHUNK_SPACE,
};
pub(crate) use types::{
    MaterializedTrackedStateRow, TrackedStateCommitDeltaRef, TrackedStateDeltaRef,
    TrackedStateFilter, TrackedStateReadColumns, TrackedStateRootMutationRef,
    TrackedStateScanRequest,
};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};
#[cfg(feature = "storage-benches")]
pub mod bench {
    pub use super::bench_support::*;
}
