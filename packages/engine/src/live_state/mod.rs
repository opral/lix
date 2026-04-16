//! Live-state query-serving subsystem boundary.
//!
//! `live_state` owns query-oriented state surfaces over tracked and untracked
//! rows. Use this module when the question is "what is visible for version V
//! right now?".
//!
//! `live_state` owns:
//! - lifecycle initialization and readiness checks for live-state serving
//! - projection replay/catch-up orchestration for live-state derived rows
//! - schema-scoped storage initialization
//! - stored, tracked, untracked, and effective row access
//! - rebuild planning and apply for visible-row materialization
//! - read-only passthrough query surfaces for canonical-owned facts when
//!   SQL/public reads need them
//!
//! `live_state` may serve canonical-owned entities such as `lix_commit`,
//! `lix_commit_edge`, `lix_change_set`, and `lix_change_set_element`, but those
//! mirrors do not transfer semantic ownership.
//!
//! `live_state` does not own DAG, root/head, or commit-addressed state
//! semantics. New history-semantic work should go through `canonical/*`, while
//! `sql/*` should keep reading query surfaces served here.
//!
//! Any replay cursor, watermark, or readiness state tracked here is local and
//! rebuildable execution machinery for derived projections. It is not canonical
//! semantics and must not become the source of truth for committed meaning.
//! Replay-specific implementation lives under `live_state::projection::*`.
//! Callers outside `live_state/*` should use the root-level entrypoints here.

mod bridge;
pub(crate) mod constraints;
pub(crate) mod effective;
mod frontier;
mod init;
pub(crate) mod lifecycle;
pub(crate) mod materialize;
pub(crate) mod naming;
mod plugin_archives;
#[allow(dead_code)]
pub(crate) mod projection;
mod projection_receipt;
#[cfg(test)]
mod read_context;
mod replay_cursor;
mod row_queries;
pub(crate) mod schema_access;
pub(crate) mod shared;
mod snapshot_queries;
pub(crate) mod storage;
mod storage_metadata;
pub(crate) mod stored_rows;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod tracked;
mod types;
pub(crate) mod untracked;
mod visible_rows;
pub(crate) mod writer_key;
use crate::catalog::SurfaceReadFreshness;
use crate::{LixBackend, LixBackendTransaction, LixError, Value};
use async_trait::async_trait;
use serde_json::Value as JsonValue;

pub(crate) use bridge::LiveStateTransactionBridge;
pub use constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
#[cfg(test)]
pub(crate) use effective::EffectiveRowsResolver;
pub use effective::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};
pub(crate) use frontier::{
    load_current_committed_version_frontier_with_backend,
    load_current_committed_version_frontier_with_executor,
    load_version_head_commit_id_with_executor, load_version_head_commit_map_with_executor,
};
pub use init::init;
pub use lifecycle::LiveStateReadiness;
pub use materialize::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
pub(crate) use plugin_archives::PluginArchiveRef;
pub use projection::{
    DerivedProjectionId, DerivedProjectionStatus, ProjectionReplayMode, ProjectionStatus,
};
pub(crate) use projection_receipt::CanonicalCommitProjectionReceipt;
#[cfg(test)]
pub(crate) use read_context::LiveReadContext;
pub use replay_cursor::ReplayCursor;
pub use row_queries::{
    decode_registered_schema_row, load_exact_live_row, scan_live_rows, write_live_rows,
    ExactLiveRowQuery, LiveRow, LiveRowQuery, LiveRowSource,
};
pub(crate) use schema_access::LiveRowShape;
pub(crate) use snapshot_queries::{LiveRowShapeContract, LiveStateQueryBackend};
#[cfg(test)]
pub(crate) use storage_metadata::{builtin_schema_storage_metadata, BuiltinSchemaStorageLane};
#[cfg(test)]
pub(crate) use storage_metadata::{key_value_file_id, key_value_plugin_key};
pub(crate) use storage_metadata::{key_value_schema_key, key_value_schema_version};
#[cfg(test)]
pub(crate) use testing::LIVE_STATE_SCHEMA_EPOCH;
pub(crate) use tracked::{
    load_exact_row_with_backend as load_exact_tracked_row_with_backend,
    scan_rows_with_backend as scan_tracked_rows_with_backend, TrackedRow, TrackedTombstoneMarker,
};
pub(crate) use tracked::{
    load_exact_tombstone_with_executor as load_exact_tracked_tombstone_with_executor,
    scan_tombstones_with_executor as scan_tracked_tombstones_with_executor,
};
#[cfg(test)]
pub(crate) use tracked::{TrackedReadView, TrackedTombstoneView};
#[cfg(test)]
pub(crate) use types::values_from_snapshot_content;
#[cfg(test)]
pub(crate) use types::{batch_row_constraints, BatchRowRequest};
pub(crate) use types::{
    exact_row_constraints, matches_constraints, ExactRowRequest, LiveWriteOperation, LiveWriteRow,
    RowIdentity, ScanRequest,
};
#[allow(unused_imports)]
pub(crate) use types::{
    LiveFilter, LiveFilterField, LiveFilterOp, LiveSnapshotRow, LiveSnapshotStorage,
    LiveStateProjectionStatus, SchemaRegistrationSet,
};
pub use types::{LiveStateMode, SchemaRegistration};
pub(crate) use untracked::load_exact_row_with_executor as load_exact_untracked_row_with_executor;
#[cfg(test)]
pub(crate) use untracked::UntrackedReadView;
#[allow(unused_imports)]
pub(crate) use untracked::{ExactUntrackedRowRequest, UntrackedRow};
pub(crate) use visible_rows::{
    scan_live_rows as scan_visible_live_rows, LiveReadRow, LiveStorageLane,
};
#[cfg(test)]
pub(crate) use writer_key::WriterKeyReadView;
pub(crate) use writer_key::WRITER_KEY_TABLE;
pub(crate) use writer_key::{
    apply_writer_key_annotations_with_executor, tracked_writer_key_annotations_from_changes,
};

pub(crate) const TRACKED_RELATION_PREFIX: &str = storage::sql::TRACKED_LIVE_TABLE_PREFIX;
pub(crate) use naming::{tracked_relation_name, INTERNAL_RELATION_PREFIX};
pub(crate) const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str =
    "lix_internal_registered_schema_bootstrap";
pub(crate) const FILE_DATA_CACHE_TABLE: &str = "lix_internal_file_data_cache";
pub(crate) const FILE_PATH_CACHE_TABLE: &str = "lix_internal_file_path_cache";
pub(crate) const FILE_LIXCOL_CACHE_TABLE: &str = "lix_internal_file_lixcol_cache";

pub(crate) fn internal_exact_relation_names() -> &'static [&'static str] {
    &[
        lifecycle::LIVE_STATE_STATUS_TABLE,
        REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
        FILE_DATA_CACHE_TABLE,
        FILE_PATH_CACHE_TABLE,
        FILE_LIXCOL_CACHE_TABLE,
    ]
}

pub async fn require_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::require_ready(backend).await
}

pub async fn projection_status(backend: &dyn LixBackend) -> Result<ProjectionStatus, LixError> {
    projection::projection_status(backend).await
}

pub(crate) async fn ensure_projection_read_freshness_with_backend(
    backend: &dyn LixBackend,
    freshness_contract: SurfaceReadFreshness,
    resolved_relations: &[String],
) -> Result<(), LixError> {
    if freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    let status =
        projection::status::load_live_state_projection_status_with_backend(backend).await?;
    if matches!(
        status.mode,
        LiveStateMode::Ready | LiveStateMode::Bootstrapping
    ) {
        return Ok(());
    }

    Err(projection_stale_error(resolved_relations, &status))
}

pub(crate) async fn ensure_projection_read_freshness_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    freshness_contract: SurfaceReadFreshness,
    resolved_relations: &[String],
) -> Result<(), LixError> {
    if freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    if require_ready_in_transaction(transaction).await.is_ok() {
        return Ok(());
    }

    let backend = crate::backend::transaction_backend_view(transaction);
    let status =
        projection::status::load_live_state_projection_status_with_backend(&backend).await?;
    if status.mode == LiveStateMode::Bootstrapping {
        return Ok(());
    }

    Err(projection_stale_error(resolved_relations, &status))
}

pub(crate) async fn list_installed_plugin_archive_refs(
    backend: &dyn LixBackend,
) -> Result<Vec<PluginArchiveRef>, LixError> {
    plugin_archives::list_installed_plugin_archive_refs(backend).await
}

pub(crate) async fn derive_read_time_surface_rows(
    backend: &dyn LixBackend,
    registry: &crate::catalog::CatalogProjectionRegistry,
    request: &crate::catalog::CatalogReadTimeProjectionRequest,
) -> Result<Vec<crate::catalog::CatalogDerivedRow>, LixError> {
    projection::dispatch::derive_read_time_projection_rows_with_backend(backend, registry, request)
        .await
}

pub async fn register_schema(
    backend: &dyn LixBackend,
    registration: impl Into<SchemaRegistration>,
) -> Result<(), LixError> {
    let registration = registration.into();
    storage::register_schema(backend, &registration).await
}

pub async fn finalize_live_state_after_commit_write(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    if lifecycle::require_ready_in_transaction(transaction)
        .await
        .is_ok()
    {
        lifecycle::mark_live_state_ready_at_latest_replay_cursor_in_transaction(transaction)
            .await?;
    }
    Ok(())
}

pub async fn rebuild_plan(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    materialize::rebuild_plan(backend, request).await
}

pub async fn apply_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    materialize::apply_rebuild_plan(backend, plan).await
}

pub async fn rebuild(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    materialize::rebuild(backend, request).await
}

pub(crate) async fn rebuild_projection(
    backend: &dyn LixBackend,
    plugin_materializer: &dyn crate::plugin::FilesystemPluginMaterializer,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    materialize::rebuild_projection(backend, plugin_materializer, request).await
}

pub(crate) async fn require_ready_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    lifecycle::require_ready_in_transaction(transaction).await
}

fn projection_stale_error(
    surface_names: &[String],
    status: &LiveStateProjectionStatus,
) -> LixError {
    let surfaces = if surface_names.is_empty() {
        "this public read".to_string()
    } else {
        format!("surface(s) {}", surface_names.join(", "))
    };
    let applied = format_optional_replay_cursor(status.applied_cursor.as_ref());
    let latest = format_optional_replay_cursor(status.latest_cursor.as_ref());
    let applied_frontier =
        format_optional_committed_frontier(status.applied_committed_frontier.as_ref());
    let current_frontier = format_committed_frontier(&status.current_committed_frontier);
    LixError::new(
        crate::common::ErrorCode::LiveStateNotReady.as_str(),
        format!(
            "Public read for {surfaces} requires fresh live-state projections, but live_state is {:?}. Applied committed frontier: {applied_frontier}. Current committed frontier: {current_frontier}. Applied replay cursor: {applied}. Latest replay cursor: {latest}. Canonical history/change reads may proceed while stale, but current-state projection reads must wait for replay or rebuild.",
            status.mode
        ),
    )
}

fn format_optional_replay_cursor(cursor: Option<&ReplayCursor>) -> String {
    cursor
        .map(|cursor| format!("{}@{}", cursor.change_id, cursor.created_at))
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_optional_committed_frontier(
    frontier: Option<&crate::CommittedVersionFrontier>,
) -> String {
    frontier
        .map(format_committed_frontier)
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_committed_frontier(frontier: &crate::CommittedVersionFrontier) -> String {
    frontier.describe()
}

pub(crate) async fn register_schema_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    registration: impl Into<SchemaRegistration>,
) -> Result<(), LixError> {
    let registration = registration.into();
    storage::register_schema_in_transaction(transaction, &registration).await
}

pub(crate) async fn mark_live_state_ready_at_latest_replay_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<ReplayCursor, LixError> {
    lifecycle::mark_live_state_ready_at_latest_replay_cursor_in_transaction(transaction).await
}

pub(crate) async fn load_latest_live_state_replay_cursor_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<ReplayCursor>, LixError> {
    projection::replay::load_latest_live_state_replay_cursor_with_backend(backend).await
}

pub(crate) async fn mark_live_state_projection_ready_with_backend(
    backend: &dyn LixBackend,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    projection::mark_live_state_projection_ready_with_backend(backend, cursor).await
}

pub(crate) async fn mark_live_state_projection_ready_without_replay_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    projection::replay::mark_live_state_projection_ready_without_replay_cursor_in_transaction(
        transaction,
    )
    .await
}

pub(crate) async fn load_mode_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateMode, LixError> {
    lifecycle::load_live_state_mode_with_backend(backend).await
}

pub(crate) async fn try_claim_bootstrap_with_backend(
    backend: &dyn LixBackend,
) -> Result<bool, LixError> {
    lifecycle::try_claim_live_state_bootstrap_with_backend(backend).await
}

pub(crate) async fn mark_mode_with_backend(
    backend: &dyn LixBackend,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_mode_with_backend(backend, mode).await
}

pub(crate) fn live_row_shape_from_definition(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<LiveRowShape, LixError> {
    schema_access::live_row_shape_from_definition(schema_key, schema_definition)
}

pub(crate) fn compile_live_row_shape_from_registered_snapshots(
    schema_key: &str,
    rows: Vec<Vec<Value>>,
) -> Result<LiveRowShape, LixError> {
    let layout = storage::compile_registered_live_layout(schema_key, rows)?;
    Ok(schema_access::live_row_shape_from_layout(layout))
}

pub(crate) fn payload_column_name_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    live_row_shape_from_definition(schema_key, schema_definition)?
        .payload_column_name(property_name)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "live schema '{}' does not include property '{}'",
                    schema_key, property_name
                ),
            )
        })
}

pub(crate) fn normalized_projection_sql_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(
        live_row_shape_from_definition(schema_key, schema_definition)?
            .normalized_projection_sql(table_alias),
    )
}

pub(crate) fn snapshot_select_expr_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    dialect: crate::SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(
        live_row_shape_from_definition(schema_key, schema_definition)?
            .snapshot_select_expr(dialect, table_alias),
    )
}

pub(crate) async fn load_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<Vec<u8>, LixError> {
    materialize::filesystem::load_file_payload_cache_data(backend, file_id, version_id).await
}

pub(crate) async fn upsert_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    materialize::filesystem::upsert_file_payload_cache_data(backend, file_id, version_id, data)
        .await
}

pub(crate) async fn delete_file_payload_cache_data(
    backend: &dyn LixBackend,
    file_id: &str,
    version_id: &str,
) -> Result<(), LixError> {
    materialize::filesystem::delete_file_payload_cache_data(backend, file_id, version_id).await
}

pub(crate) async fn rebuild_scope_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateApplyReport, LixError> {
    let plan = materialize::rebuild_plan_with_transaction(transaction, request).await?;
    let (rows_deleted, tables_touched) =
        materialize::apply_rebuild_scope_in_transaction(transaction, &plan).await?;
    Ok(LiveStateApplyReport {
        run_id: plan.run_id.clone(),
        rows_written: plan.writes.len(),
        rows_deleted,
        tables_touched: tables_touched.into_iter().collect(),
    })
}

#[async_trait(?Send)]
impl crate::live_state::LiveStateTransactionBridge for dyn LixBackendTransaction + '_ {
    async fn register_live_state_schema(
        &mut self,
        registration: &crate::live_state::SchemaRegistration,
    ) -> Result<(), LixError> {
        register_schema_in_transaction(self, registration.clone()).await
    }

    async fn advance_live_state_replay_boundary(
        &mut self,
        replay_cursor: &ReplayCursor,
    ) -> Result<(), LixError> {
        projection::replay::advance_live_state_projection_replay_boundary_to_cursor_in_transaction(
            self,
            replay_cursor,
        )
        .await
    }
}
