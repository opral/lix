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

pub mod constraints;
pub mod effective;
mod init;
mod lifecycle;
pub(crate) mod materialize;
pub(crate) mod pending_reads;
#[allow(dead_code)]
pub(crate) mod projection;
mod query_contracts;
pub(crate) mod raw;
pub(crate) mod schema_access;
pub(crate) mod shared;
pub(crate) mod storage;
#[cfg(test)]
pub(crate) mod testing;
pub mod tracked;
pub mod untracked;
mod visible_rows;
use crate::contracts::traits::{TrackedWriteParticipant, UntrackedWriteParticipant};
use crate::{LixBackend, LixBackendTransaction, LixError};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str =
    "lix_internal_registered_schema_bootstrap";
pub(crate) const FILE_DATA_CACHE_TABLE: &str = "lix_internal_file_data_cache";
pub(crate) const FILE_PATH_CACHE_TABLE: &str = "lix_internal_file_path_cache";
pub(crate) const FILE_LIXCOL_CACHE_TABLE: &str = "lix_internal_file_lixcol_cache";

#[allow(unused_imports)]
pub(crate) use crate::contracts::artifacts::{
    ExactUntrackedLookupRequest, LiveFilter, LiveFilterField, LiveFilterOp, LiveSnapshotRow,
    LiveSnapshotStorage, LiveStateProjectionStatus, SchemaRegistrationSet,
    TrackedTombstoneLookupRequest,
};
pub use crate::contracts::artifacts::{LiveStateMode, SchemaRegistration};
use crate::contracts::ReplayCursor;
pub use constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
pub use effective::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};
pub use init::init;
pub use lifecycle::LiveStateReadiness;
pub(crate) use lifecycle::LIVE_STATE_STATUS_TABLE;
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
pub(crate) use schema_access::LiveReadContract;
pub use shared::identity::RowIdentity;
pub use tracked::{
    load_exact_row_with_backend as load_exact_tracked_row_with_backend,
    load_exact_rows_with_backend as load_exact_tracked_rows_with_backend,
    scan_rows_with_backend as scan_tracked_rows_with_backend, BatchTrackedRowRequest,
    ExactTrackedRowRequest, TrackedRow, TrackedScanRequest, TrackedTombstoneMarker,
    TrackedWriteBatch, TrackedWriteOperation, TrackedWriteRow,
};
pub(crate) use tracked::{
    load_exact_tombstone_with_executor as load_exact_tracked_tombstone_with_executor,
    scan_tombstones_with_executor as scan_tracked_tombstones_with_executor,
};
pub(crate) use untracked::load_exact_row_with_executor as load_exact_untracked_row_with_executor;
pub use untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend,
    load_exact_rows_with_backend as load_exact_untracked_rows_with_backend,
    scan_rows_with_backend as scan_untracked_rows_with_backend, BatchUntrackedRowRequest,
    ExactUntrackedRowRequest, UntrackedRow, UntrackedScanRequest, UntrackedWriteBatch,
    UntrackedWriteOperation, UntrackedWriteRow,
};
pub(crate) use visible_rows::{scan_live_rows, LiveReadRow, LiveStorageLane};

pub async fn require_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::require_ready(backend).await
}

pub async fn projection_status(backend: &dyn LixBackend) -> Result<ProjectionStatus, LixError> {
    projection::projection_status(backend).await
}

pub(crate) async fn load_live_state_projection_status_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateProjectionStatus, LixError> {
    projection::status::load_live_state_projection_status_with_backend(backend).await
}

pub async fn register_schema(
    backend: &dyn LixBackend,
    registration: impl Into<SchemaRegistration>,
) -> Result<(), LixError> {
    let registration = registration.into();
    storage::register_schema(backend, &registration).await
}

pub async fn finalize_commit(backend: &dyn LixBackend) -> Result<ReplayCursor, LixError> {
    lifecycle::finalize_commit(backend).await
}

pub async fn rebuild_plan(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    materialize::rebuild_plan(backend, request).await
}

pub async fn live_state_rebuild_plan(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildPlan, LixError> {
    rebuild_plan(backend, request).await
}

pub async fn apply_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    materialize::apply_rebuild_plan(backend, plan).await
}

pub async fn apply_live_state_rebuild_plan(
    backend: &dyn LixBackend,
    plan: &LiveStateRebuildPlan,
) -> Result<LiveStateApplyReport, LixError> {
    apply_rebuild_plan(backend, plan).await
}

pub async fn rebuild(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    materialize::rebuild(backend, request).await
}

pub async fn rebuild_live_state(
    backend: &dyn LixBackend,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateRebuildReport, LixError> {
    rebuild(backend, request).await
}

#[allow(dead_code)]
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

pub(crate) async fn finalize_commit_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<ReplayCursor, LixError> {
    lifecycle::finalize_commit_in_transaction(transaction).await
}

pub(crate) async fn load_latest_live_state_replay_cursor_with_backend(
    backend: &dyn LixBackend,
) -> Result<Option<ReplayCursor>, LixError> {
    projection::replay::load_latest_live_state_replay_cursor_with_backend(backend).await
}

pub(crate) async fn apply_commit_projections_best_effort_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &crate::contracts::artifacts::CanonicalCommitReceipt,
    tracked_writer_key_hints: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(), LixError> {
    projection::apply_commit_projections_best_effort_in_transaction(
        transaction,
        receipt,
        tracked_writer_key_hints,
    )
    .await
}

pub(crate) async fn mark_live_state_projection_ready_with_backend(
    backend: &dyn LixBackend,
    cursor: &ReplayCursor,
) -> Result<(), LixError> {
    projection::mark_live_state_projection_ready_with_backend(backend, cursor).await
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

pub(crate) async fn load_live_read_contract_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveReadContract, LixError> {
    schema_access::load_schema_read_contract_with_backend(backend, schema_key).await
}

pub(crate) async fn load_live_read_contract_for_table_name(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveReadContract>, LixError> {
    schema_access::load_schema_read_contract_for_table_name(backend, table_name).await
}

pub(crate) async fn live_storage_relation_exists_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<bool, LixError> {
    schema_access::live_storage_relation_exists_with_backend(backend, schema_key).await
}

pub(crate) async fn apply_tracked_write_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &[TrackedWriteRow],
) -> Result<(), LixError> {
    transaction.apply_tracked_write_batch(batch).await
}

pub(crate) async fn apply_untracked_write_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &[UntrackedWriteRow],
) -> Result<(), LixError> {
    transaction.apply_untracked_write_batch(batch).await
}

pub(crate) async fn upsert_bootstrap_tracked_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    entity_id: &str,
    schema_key: &str,
    schema_version: &str,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
    change_id: &str,
    snapshot_content: &str,
    timestamp: &str,
) -> Result<(), LixError> {
    let batch = [tracked::TrackedWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: file_id.to_string(),
        version_id: version_id.to_string(),
        global: version_id == crate::schema::builtin::GLOBAL_VERSION_ID,
        plugin_key: plugin_key.to_string(),
        metadata: None,
        change_id: change_id.to_string(),
        writer_key: None,
        snapshot_content: Some(snapshot_content.to_string()),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: tracked::TrackedWriteOperation::Upsert,
    }];
    apply_tracked_write_batch_in_transaction(transaction, &batch).await
}

pub(crate) async fn upsert_bootstrap_untracked_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    entity_id: &str,
    schema_key: &str,
    schema_version: &str,
    file_id: &str,
    version_id: &str,
    plugin_key: &str,
    snapshot_content: &str,
    timestamp: &str,
) -> Result<(), LixError> {
    let batch = [untracked::UntrackedWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: file_id.to_string(),
        version_id: version_id.to_string(),
        global: version_id == crate::schema::builtin::GLOBAL_VERSION_ID,
        plugin_key: plugin_key.to_string(),
        metadata: None,
        writer_key: None,
        snapshot_content: Some(snapshot_content.to_string()),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: untracked::UntrackedWriteOperation::Upsert,
    }];
    apply_untracked_write_batch_in_transaction(transaction, &batch).await
}

#[cfg(test)]
pub(crate) fn normalized_values_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, crate::Value>, LixError> {
    schema_access::normalized_values_for_schema(schema_key, schema_definition, snapshot_content)
}

pub(crate) async fn rebuild_scope_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    request: &LiveStateRebuildRequest,
) -> Result<LiveStateApplyReport, LixError> {
    rebuild_scope_with_writer_key_hints_in_transaction(transaction, request, &BTreeMap::new()).await
}

pub(crate) async fn rebuild_scope_with_writer_key_hints_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    request: &LiveStateRebuildRequest,
    writer_key_hints: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<LiveStateApplyReport, LixError> {
    // Rebuild semantic live state first, then optionally reapply workspace
    // annotation hints for read surfaces that still expose them.
    let plan = materialize::rebuild_plan_with_transaction(transaction, request).await?;
    let (rows_deleted, tables_touched) =
        materialize::apply_rebuild_scope_with_writer_key_hints_in_transaction(
            transaction,
            &plan,
            writer_key_hints,
        )
        .await?;
    Ok(LiveStateApplyReport {
        run_id: plan.run_id.clone(),
        rows_written: plan.writes.len(),
        rows_deleted,
        tables_touched: tables_touched.into_iter().collect(),
    })
}

fn snapshot_json_from_values(
    access: &storage::LiveRowAccess,
    schema_key: &str,
    values: &std::collections::BTreeMap<String, crate::Value>,
) -> Result<JsonValue, LixError> {
    raw::snapshot_json_from_values(access, schema_key, values)
}

fn snapshot_text_from_values(
    access: &storage::LiveRowAccess,
    schema_key: &str,
    values: &std::collections::BTreeMap<String, crate::Value>,
) -> Result<String, LixError> {
    serde_json::to_string(&snapshot_json_from_values(access, schema_key, values)?).map_err(
        |error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "failed to serialize live snapshot for schema '{}': {error}",
                    schema_key
                ),
            )
        },
    )
}

#[cfg(test)]
pub(crate) fn live_relation_name(schema_key: &str) -> String {
    schema_access::tracked_relation_name(schema_key)
}

#[cfg(test)]
pub(crate) fn live_schema_column_names(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<Vec<String>, LixError> {
    schema_access::schema_column_names(schema_key, schema_definition)
}

#[cfg(test)]
pub(crate) use lifecycle::LIVE_STATE_SCHEMA_EPOCH;
