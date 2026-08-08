#![cfg_attr(not(feature = "storage-benches"), allow(dead_code, unused_imports))]

mod context;
mod diff;
mod diff_id;
mod merge;
mod row_materialization;
mod types;

pub(crate) use context::descriptor_dependency_cascade_file_ids;
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
    MaterializedTrackedStateRowRef,
};
pub(crate) use types::TrackedStateRootId;
pub(crate) use types::{COMMIT_STATE_MAX_REPLAY_BYTES, COMMIT_STATE_MAX_REPLAY_DEPTH};
pub(crate) use types::{
    ColumnarMutationPartSet, CommitDeltaLifecycleSummary, CommitStateManifest,
    CommitStateMutationInventory, CommitStateReplayDebt, MaterializedTrackedStateRow,
    TrackedStateBaseCoordinate, TrackedStateCommitDeltaRef, TrackedStateCommitRoot,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateIndexValue, TrackedStateReadColumns,
    TrackedStateRootMutationRef, TrackedStateScanRequest, TrackedStateSingleStringReplacementRef,
};
#[cfg(test)]
pub(crate) use types::{CurrentStatePartDescriptor, TrackedStateCommitRootParent};
pub(crate) use types::{TrackedStateKey, TrackedStateKeyRef};
