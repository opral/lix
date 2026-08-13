mod context;
mod derived;
mod entity_columnar_cache;
mod entity_decoded_column_cache;
mod reader;
mod tracked_head;
#[cfg(test)]
pub(crate) use tracked_head::{head_decode_entity_pk_probe, hot_decode_entity_pk_probe};
mod types;
pub(crate) mod visibility;

/// Re-exported for the consumers that already spell these `crate::hot_state::…`.
/// The definitions live in the top-level `entity_columnar` module, which sits
/// below both state planes; this facade only exists so the move did not have to
/// touch every call site at once and can be dropped as those are migrated.
pub(crate) use crate::entity_columnar::{
    ENTITY_COLUMNAR_ENTITY_PK_FIELD, ENTITY_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY,
    EntityColumnarWriteSets, entity_row_group_set_id,
};
#[allow(unused_imports)]
pub(crate) use context::{
    BranchHeadControlCache, GlobalKeyValueRowCache, HotStateContext, HotStateContextReader,
};
pub(crate) use entity_columnar_cache::{
    EntityColumnarArrayBudget, EntityColumnarShadowMaskCache, EntityColumnarShadowMaskKey,
};
pub(crate) use entity_decoded_column_cache::EntityDecodedColumnCache;
#[cfg(test)]
pub(crate) use reader::load_exact_batch_via_scan_for_test;
#[allow(unused_imports)]
pub(crate) use reader::{HotStateReadDomain, HotStateReader};
#[cfg(test)]
pub(crate) use tracked_head::TrackedHeadDeltaRef;
#[cfg(test)]
pub(crate) use tracked_head::WORKING_DIFF_PATH_HITS;
#[cfg(test)]
pub(crate) use tracked_head::hot_generation_scope_prefix;
#[cfg(test)]
pub(crate) use tracked_head::stage_collect_stale_working_diff_indexes;
pub(crate) use tracked_head::stage_retire_hot_generation;
// Read only by `#[cfg(test)]` probes, so a features-on *library* build compiles
// the counters and re-exports them without any in-crate reader. `cargo test`
// cannot see that — under `--profile test` the probe module exists and the
// imports are used — so this surfaces only as a `clippy --all-targets` failure
// on the non-test lib. Matches the `#[allow(unused_imports)]` on the re-export
// block directly below, which exists for the same reason.
#[cfg(any(test, feature = "storage-benches"))]
#[allow(unused_imports)]
pub(crate) use tracked_head::{
    BROAD_CANONICAL_CREATED_AT_HITS, BROAD_CANONICAL_CREATED_AT_KEYS,
    BROAD_CANONICAL_CREATED_AT_LOOKUPS, COMPACTED_TOMBSTONE_CANDIDATES,
    COMPACTED_TOMBSTONE_COMPACTED, COMPACTED_TOMBSTONE_OFFERED, COMPACTED_TOMBSTONE_ROUTES,
    HOT_SCAN_DECODED_ENTRIES, HOT_SCAN_MATCHED_ENTRIES, HOT_SCAN_TOMBSTONE_ENTRIES,
    INTERVAL_LOCAL_TOMBSTONE_CANDIDATES, INTERVAL_LOCAL_TOMBSTONE_ELIDED,
    INTERVAL_LOCAL_TOMBSTONE_OFFERED, INTERVAL_LOCAL_TOMBSTONE_ROUTES,
};
#[allow(unused_imports)]
pub(crate) use tracked_head::{
    CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE, CERTIFIED_ENTITY_BATCH_PAGE_SPACE,
    CERTIFIED_ENTITY_BATCH_SPACE, COLLECTION_CONTROL_SPACE, CertifiedCurrentStatePredecessor,
    CertifiedCurrentStatePredecessorRef, CertifiedEntityBatchFileRef, ColumnarBaseCoordinate,
    CurrentStateDeltaRef, DIFF_SPACE, DeferredFreshHotPlan, DeferredFreshHotRowRef,
    DeferredFreshHotRows, EntityColumnarOverlayRow, FILE_SPACE, HotIndexEntry, HotIndexValue,
    HotTrackedSnapshot, INDEX_SPACE, PACKED_CURRENT_BASE_CONTROL_SPACE, PACKED_CURRENT_BASE_SPACE,
    PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE, PackedIdentityMembership, ROOT_CURRENT_BASE_SPACE,
    ROW_SPACE, TRACKED_WORKING_DIFF_MARKER_SPACE, TrackedHeadContext, TrackedWorkingDiff,
    TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, load_certified_rows_at_commit,
    materialize_certified_root_rows, scan_certified_history_rows, stage_certified_entity_batches,
    stage_delete_tracked_working_diff_epoch, stage_hot_index_entries,
    stage_tracked_working_diff_epoch,
};
#[allow(unused_imports)]
pub(crate) use types::{
    Bound, DeclaredColumnEq, DeclaredColumnRange, HotStateExactBatchRequest,
    HotStateExactRowRequest, HotStateFilter,
    HotStateProjection, HotStateRowFilter, HotStateRowIdentityRef, HotStateRowRequest,
    HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateBatchBuilder,
    MaterializedHotStateExactBatch, MaterializedHotStateRow, MaterializedHotStateRowRef,
    ScanConstraint, ScanField, ScanOperator,
};
#[allow(unused_imports)]
pub(crate) use visibility::{
    StagedHotStateRows, VisibilityBranchScope, VisibilityRequest, expanded_branch_ids,
    overlay_load_exact_batch, overlay_scan_batch, overlay_scan_tracked_batch,
    resolve_visible_batch,
};
#[cfg(test)]
pub(crate) use visibility::{blob_ref_probe_stats, reset_blob_ref_probe_stats};
