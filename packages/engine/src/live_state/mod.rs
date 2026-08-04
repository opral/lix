mod context;
mod derived;
mod entity_columnar;
mod entity_columnar_cache;
mod entity_decoded_column_cache;
mod reader;
mod tracked_head;
mod types;
mod untracked_state;
pub(crate) mod visibility;

#[allow(unused_imports)]
pub(crate) use context::{BranchHeadControlCache, LiveStateContext, LiveStateStoreReader};
pub(crate) use entity_columnar::{
    ENTITY_COLUMNAR_ENTITY_PK_FIELD, ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY,
    EntityColumnarWriteSets, entity_identity_column_index, entity_row_group_set_id,
};
pub(crate) use entity_columnar_cache::{
    EntityColumnarArrayBudget, EntityColumnarShadowMaskCache, EntityColumnarShadowMaskKey,
};
pub(crate) use entity_decoded_column_cache::EntityDecodedColumnCache;
#[allow(unused_imports)]
pub(crate) use reader::LiveStateReader;
#[cfg(test)]
pub(crate) use reader::load_exact_batch_via_scan_for_test;
#[cfg(test)]
pub(crate) use tracked_head::TrackedHeadDeltaRef;
#[allow(unused_imports)]
pub(crate) use tracked_head::{
    CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE, CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
    CERTIFIED_ENTITY_BATCH_SPACE, CertifiedCurrentStatePredecessor,
    CertifiedCurrentStatePredecessorRef, CertifiedEntityBatchFileRef, ColumnarBaseCoordinate,
    CurrentStateDeltaRef, DeferredFreshHotPlan, DeferredFreshHotRowRef, DeferredFreshHotRows,
    EntityColumnarOverlayRow, HOT_DIFF_SPACE, HOT_FILE_SPACE, HOT_ROW_SPACE, HotTrackedSnapshot,
    PACKED_CURRENT_BASE_CONTROL_SPACE, PACKED_CURRENT_BASE_SPACE,
    PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE, PackedIdentityMembership, ROOT_CURRENT_BASE_SPACE,
    TRACKED_WORKING_DIFF_MARKER_SPACE, TrackedHeadContext, TrackedWorkingDiff,
    TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, materialize_certified_root_rows,
    scan_certified_history_rows, stage_certified_entity_batches,
    stage_collect_stale_working_diff_indexes, stage_delete_tracked_working_diff_epoch,
    stage_tracked_working_diff_epoch,
};
#[allow(unused_imports)]
pub(crate) use types::{
    Bound, LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter,
    LiveStateProjection, LiveStateRowFilter, LiveStateRowIdentityRef, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder,
    MaterializedLiveStateExactBatch, MaterializedLiveStateRow, MaterializedLiveStateRowRef,
    ScanConstraint, ScanField, ScanOperator,
};
#[allow(unused_imports)]
pub(crate) use untracked_state::{
    UNTRACKED_ROW_SPACE, load_untracked_exact_batch, scan_untracked_batch, stage_untracked_deltas,
    untracked_json_refs,
};
#[cfg(any(test, feature = "storage-benches"))]
#[allow(unused_imports)]
pub(crate) use untracked_state::{
    UntrackedFileCascadeReadProfile, take_untracked_file_cascade_read_profile,
};
#[allow(unused_imports)]
pub(crate) use visibility::{
    StagedLiveStateRows, VisibilityBranchScope, VisibilityRequest, expanded_branch_ids,
    overlay_load_exact_batch, overlay_scan_batch, overlay_scan_tracked_batch,
    resolve_visible_batch,
};
