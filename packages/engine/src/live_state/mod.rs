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
//! - raw, tracked, untracked, and effective row access
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

pub(crate) mod constraints;
pub(crate) mod effective;
mod init;
pub(crate) mod lifecycle;
pub(crate) mod materialize;
#[allow(dead_code)]
pub(crate) mod projection;
mod query_contracts;
pub(crate) mod raw;
mod row_api;
pub(crate) mod schema_access;
pub(crate) mod shared;
pub(crate) mod storage;
mod storage_metadata;
#[cfg(test)]
pub(crate) mod testing;
pub(crate) mod tracked;
pub(crate) mod untracked;
mod visible_rows;
pub(crate) mod writer_key;
use crate::contracts::ReplayCursor;
use crate::{LixBackend, LixBackendTransaction, LixError, Value};
use async_trait::async_trait;
use serde_json::Value as JsonValue;

#[allow(unused_imports)]
pub(crate) use crate::contracts::artifacts::{
    LiveFilter, LiveFilterField, LiveFilterOp, LiveSnapshotRow, LiveSnapshotStorage,
    LiveStateProjectionStatus, SchemaRegistrationSet,
};
pub use crate::contracts::artifacts::{LiveStateMode, SchemaRegistration};
pub use constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
pub use effective::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
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
pub use projection::{
    DerivedProjectionId, DerivedProjectionStatus, ProjectionReplayMode, ProjectionStatus,
};
pub use row_api::{
    decode_registered_schema_row, load_exact_live_row, scan_live_rows, write_live_rows,
    ExactLiveRowQuery, LiveRow, LiveRowQuery, LiveRowSemantics, RowReadMode,
};
pub(crate) use schema_access::LiveReadContract;
pub use shared::identity::RowIdentity;
pub(crate) use storage_metadata::{
    builtin_schema_storage_metadata, key_value_file_id, key_value_plugin_key, key_value_schema_key,
    key_value_schema_version, BuiltinSchemaStorageLane, BuiltinSchemaStorageMetadata,
};
pub(crate) use tracked::{
    load_exact_row_with_backend as load_exact_tracked_row_with_backend,
    scan_rows_with_backend as scan_tracked_rows_with_backend, TrackedRow, TrackedTombstoneMarker,
};
pub(crate) use tracked::{
    load_exact_tombstone_with_executor as load_exact_tracked_tombstone_with_executor,
    scan_tombstones_with_executor as scan_tracked_tombstones_with_executor,
};
pub(crate) use untracked::load_exact_row_with_executor as load_exact_untracked_row_with_executor;
pub(crate) use untracked::{ExactUntrackedRowRequest, UntrackedRow};
pub(crate) use visible_rows::{
    scan_live_rows as scan_visible_live_rows, LiveReadRow, LiveStorageLane,
};

pub(crate) const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str =
    "lix_internal_registered_schema_bootstrap";
pub(crate) const FILE_DATA_CACHE_TABLE: &str = "lix_internal_file_data_cache";
pub(crate) const FILE_PATH_CACHE_TABLE: &str = "lix_internal_file_path_cache";
pub(crate) const FILE_LIXCOL_CACHE_TABLE: &str = "lix_internal_file_lixcol_cache";
pub(crate) const TRACKED_RELATION_PREFIX: &str = storage::sql::TRACKED_LIVE_TABLE_PREFIX;
pub(crate) use lifecycle::LIVE_STATE_STATUS_TABLE;

pub async fn require_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::require_ready(backend).await
}

pub async fn projection_status(backend: &dyn LixBackend) -> Result<ProjectionStatus, LixError> {
    projection::projection_status(backend).await
}

pub(crate) async fn derive_read_time_surface_rows(
    backend: &dyn LixBackend,
    registry: &crate::projections::ProjectionRegistry,
) -> Result<Vec<crate::projections::DerivedRow>, LixError> {
    projection::dispatch::derive_read_time_projection_rows_with_backend(backend, registry).await
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

pub(crate) async fn require_ready_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    lifecycle::require_ready_in_transaction(transaction).await
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

pub(crate) fn read_contract_from_definition(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<LiveReadContract, LixError> {
    schema_access::read_contract_from_definition(schema_key, schema_definition)
}

pub(crate) fn compile_live_read_contract_from_registered_snapshots(
    schema_key: &str,
    rows: Vec<Vec<Value>>,
) -> Result<LiveReadContract, LixError> {
    let layout = storage::compile_registered_live_layout(schema_key, rows)?;
    Ok(schema_access::live_read_contract_from_layout(layout))
}

pub(crate) fn payload_column_name_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    read_contract_from_definition(schema_key, schema_definition)?
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
        read_contract_from_definition(schema_key, schema_definition)?
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
        read_contract_from_definition(schema_key, schema_definition)?
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
impl crate::contracts::traits::LiveStateTransactionBridge for dyn LixBackendTransaction + '_ {
    async fn register_live_state_schema(
        &mut self,
        registration: &crate::contracts::artifacts::SchemaRegistration,
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
