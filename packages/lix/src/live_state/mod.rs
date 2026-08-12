mod context;
mod derived;
mod entity_columnar_cache;
mod entity_decoded_column_cache;
mod reader;
mod tracked_head;
mod types;
pub(crate) mod visibility;

#[allow(unused_imports)]
pub(crate) use context::{BranchHeadControlCache, LiveStateContext, LiveStateStoreReader};
/// Re-exported for the consumers that already spell these `crate::live_state::…`.
/// The definitions live in the top-level `entity_columnar` module, which sits
/// below both state planes; this facade only exists so the move did not have to
/// touch every call site at once and can be dropped as those are migrated.
pub(crate) use crate::entity_columnar::{
    ENTITY_COLUMNAR_ENTITY_PK_FIELD, ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY,
    EntityColumnarWriteSets, entity_row_group_set_id,
};
pub(crate) use entity_columnar_cache::{
    EntityColumnarArrayBudget, EntityColumnarShadowMaskCache, EntityColumnarShadowMaskKey,
};
pub(crate) use entity_decoded_column_cache::EntityDecodedColumnCache;
#[cfg(test)]
pub(crate) use reader::load_exact_batch_via_scan_for_test;
#[allow(unused_imports)]
pub(crate) use reader::{LiveStateReadDomain, LiveStateReader};
#[cfg(test)]
pub(crate) use tracked_head::TrackedHeadDeltaRef;
#[cfg(test)]
pub(crate) use tracked_head::WORKING_DIFF_PATH_HITS;
#[cfg(test)]
pub(crate) use tracked_head::hot_generation_scope_prefix;
#[cfg(test)]
pub(crate) use tracked_head::stage_collect_stale_working_diff_indexes;
pub(crate) use tracked_head::stage_retire_hot_generation;
#[allow(unused_imports)]
pub(crate) use tracked_head::{
    CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE, CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
    CERTIFIED_ENTITY_BATCH_SPACE, CertifiedCurrentStatePredecessor,
    CertifiedCurrentStatePredecessorRef, CertifiedEntityBatchFileRef, ColumnarBaseCoordinate,
    CurrentStateDeltaRef, DeferredFreshHotPlan, DeferredFreshHotRowRef, DeferredFreshHotRows,
    EntityColumnarOverlayRow, HOT_COLLECTION_CONTROL_SPACE, HOT_DIFF_SPACE, HOT_FILE_SPACE,
    HOT_INDEX_SPACE, HOT_ROW_SPACE, HotIndexEntry, HotIndexValue, HotTrackedSnapshot, PACKED_CURRENT_BASE_CONTROL_SPACE,
    PACKED_CURRENT_BASE_SPACE, PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
    PackedIdentityMembership, ROOT_CURRENT_BASE_SPACE, TRACKED_WORKING_DIFF_MARKER_SPACE,
    TrackedHeadContext, TrackedWorkingDiff, TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage,
    load_certified_rows_at_commit, materialize_certified_root_rows, scan_certified_history_rows,
    stage_certified_entity_batches, stage_delete_tracked_working_diff_epoch,
    stage_hot_index_entries,
    stage_tracked_working_diff_epoch,
};
#[allow(unused_imports)]
pub(crate) use types::{
    DeclaredColumnEq,
    Bound, LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter,
    LiveStateProjection, LiveStateRowFilter, LiveStateRowIdentityRef, LiveStateRowRequest,
    LiveStateScanRequest, MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder,
    MaterializedLiveStateExactBatch, MaterializedLiveStateRow, MaterializedLiveStateRowRef,
    ScanConstraint, ScanField, ScanOperator,
};
#[allow(unused_imports)]
pub(crate) use visibility::{
    StagedLiveStateRows, VisibilityBranchScope, VisibilityRequest, expanded_branch_ids,
    overlay_load_exact_batch, overlay_scan_batch, overlay_scan_tracked_batch,
    resolve_visible_batch,
};
