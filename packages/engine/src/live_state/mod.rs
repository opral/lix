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
pub(crate) mod filesystem_projection;
pub(crate) mod filesystem_queries;
mod init;
pub(crate) mod key_value_queries;
mod lifecycle;
mod materialize;
pub(crate) mod pending_reads;
#[allow(dead_code)]
pub(crate) mod projection;
mod public_read_sql;
mod query_contracts;
pub(crate) mod raw;
pub(crate) mod schema_access;
pub(crate) mod shared;
mod storage;
#[cfg(test)]
pub(crate) mod testing;
pub mod tracked;
pub mod untracked;
mod visible_rows;
use crate::contracts::artifacts::SchemaLiveTableRequirement;
use crate::{LixBackend, LixBackendTransaction, LixError, ReplayCursor, SqlDialect};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub(crate) use constraints::matches_constraints;
pub use constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
pub(crate) use effective::resolve_effective_rows;
pub use effective::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};
pub(crate) use filesystem_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    resolve_file_id_by_path_in_version, FilesystemProjectionScope,
};
pub(crate) use filesystem_queries::{
    ensure_no_directory_at_file_path, ensure_no_file_at_directory_path,
    load_directory_descriptors_by_parent_name_pairs, load_directory_row_by_id,
    load_directory_row_by_id_with_pending_transaction_view, load_directory_row_by_path,
    load_directory_row_by_path_with_pending_transaction_view, load_directory_rows_under_path,
    load_file_descriptors_by_directory_name_extension_triplets, load_file_row_by_id,
    load_file_row_by_id_with_pending_transaction_view, load_file_row_by_id_without_path,
    load_file_row_by_id_without_path_with_pending_transaction_view, load_file_row_by_path,
    load_file_row_by_path_with_pending_transaction_view, load_file_rows_under_path,
    lookup_directory_id_by_path, lookup_directory_id_by_path_with_pending_transaction_view,
    lookup_directory_path_by_id, lookup_directory_path_by_id_with_pending_transaction_view,
    lookup_file_id_by_path, lookup_file_id_by_path_with_pending_transaction_view,
    DirectoryFilesystemRow, EffectiveDescriptorRow, FileFilesystemRow, FilesystemQueryError,
};
pub use init::init;
pub(crate) use key_value_queries::{
    build_ensure_runtime_sequence_row_sql, build_lock_runtime_sequence_row_sql,
    build_update_runtime_sequence_highest_sql, load_key_value_payloads,
};
pub(crate) use lifecycle::LiveStateProjectionStatus;
pub use lifecycle::{LiveStateMode, LiveStateReadiness};
pub use materialize::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionHeadDebugRow,
};
pub(crate) use pending_reads::{
    bootstrap_public_surface_registry_with_pending_transaction_view,
    execute_prepared_public_read_with_pending_transaction_view,
    execute_prepared_public_read_with_pending_transaction_view_in_transaction,
    public_read_execution_mode,
};
pub use projection::{
    DerivedProjectionId, DerivedProjectionStatus, ProjectionReplayMode, ProjectionStatus,
};
pub(crate) use query_contracts::{
    load_exact_untracked_effective_row_with_backend, load_live_read_shape_for_table_name,
    load_live_snapshot_rows_with_backend, normalize_live_snapshot_values_with_backend,
    tracked_tombstone_shadows_exact_row_with_backend, ExactUntrackedLookupRequest, LiveFilter,
    LiveFilterField, LiveFilterOp, LiveReadShape, LiveSnapshotRow, LiveSnapshotStorage,
    TrackedTombstoneLookupRequest,
};
pub(crate) use schema_access::LiveReadContract;
pub use shared::identity::RowIdentity;
pub(crate) use shared::query::entity_id_in_constraint;
pub(crate) use shared::snapshot::values_from_snapshot_content;
pub(crate) use shared::views::ReadViews as LiveReadViews;
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
    TrackedReadView as LiveTrackedReader, TrackedTombstoneView as LiveTrackedTombstoneReader,
    TrackedWriteParticipant as LiveTrackedWriter,
};
pub use untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend,
    load_exact_rows_with_backend as load_exact_untracked_rows_with_backend,
    scan_rows_with_backend as scan_untracked_rows_with_backend, BatchUntrackedRowRequest,
    ExactUntrackedRowRequest, UntrackedRow, UntrackedScanRequest, UntrackedWriteBatch,
    UntrackedWriteOperation, UntrackedWriteRow,
};
pub(crate) use untracked::{
    load_exact_row_with_executor as load_exact_untracked_row_with_executor,
    UntrackedReadView as LiveUntrackedReader, UntrackedWriteParticipant as LiveUntrackedWriter,
};
pub(crate) use visible_rows::{scan_live_rows, LiveReadRow, LiveStorageLane};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaRegistration {
    schema_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registered_snapshot: Option<JsonValue>,
    #[serde(skip, default)]
    source: SchemaRegistrationSource,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SchemaRegistrationSet {
    inner: BTreeMap<String, SchemaRegistration>,
}

impl SchemaRegistrationSet {
    pub(crate) fn insert(&mut self, registration: impl Into<SchemaRegistration>) {
        let registration = registration.into();
        self.inner
            .insert(registration.schema_key().to_string(), registration);
    }

    pub(crate) fn extend(&mut self, other: SchemaRegistrationSet) {
        self.inner.extend(other.inner);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &SchemaRegistration> {
        self.inner.values()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
enum SchemaRegistrationSource {
    #[default]
    StoredLayout,
    SchemaDefinition(JsonValue),
}

impl From<&str> for SchemaRegistration {
    fn from(schema_key: &str) -> Self {
        Self::new(schema_key)
    }
}

impl From<String> for SchemaRegistration {
    fn from(schema_key: String) -> Self {
        Self::new(schema_key)
    }
}

impl SchemaRegistration {
    pub fn new(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub fn with_registered_snapshot(
        schema_key: impl Into<String>,
        registered_snapshot: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: Some(registered_snapshot),
            source: SchemaRegistrationSource::StoredLayout,
        }
    }

    pub(crate) fn with_schema_definition(
        schema_key: impl Into<String>,
        schema_definition: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::SchemaDefinition(schema_definition),
        }
    }

    fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    fn schema_definition_override(&self) -> Option<&JsonValue> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::SchemaDefinition(schema_definition) => {
                Some(schema_definition)
            }
        }
    }
}

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

pub(crate) async fn apply_canonical_receipt_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &crate::commit::CanonicalCommitReceipt,
) -> Result<(), LixError> {
    projection::apply_canonical_receipt_in_transaction(transaction, receipt).await
}

pub(crate) async fn apply_commit_projections_best_effort_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    receipt: &crate::commit::CanonicalCommitReceipt,
    tracked_writer_key_hints: &BTreeMap<RowIdentity, Option<String>>,
) -> Result<(), LixError> {
    projection::apply_commit_projections_best_effort_in_transaction(
        transaction,
        receipt,
        tracked_writer_key_hints,
    )
    .await
}

pub(crate) async fn mark_live_state_projection_ready_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<ReplayCursor, LixError> {
    projection::mark_live_state_projection_ready_in_transaction(transaction).await
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

pub(crate) fn build_effective_public_read_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    effective_state_request: &crate::contracts::read::EffectiveStateRequest,
    surface_binding: &crate::contracts::surface::SurfaceBinding,
    pushdown_predicates: &[sqlparser::ast::Expr],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    include_snapshot_content: bool,
) -> Result<String, LixError> {
    public_read_sql::build_effective_public_read_source_sql(
        dialect,
        active_version_id,
        effective_state_request,
        surface_binding,
        pushdown_predicates,
        known_live_layouts,
        include_snapshot_content,
    )
}

pub(crate) fn build_working_changes_public_read_source_sql(
    dialect: SqlDialect,
    active_version_id: &str,
) -> String {
    public_read_sql::build_working_changes_public_read_source_sql(dialect, active_version_id)
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
    LiveTrackedWriter::apply_write_batch(transaction, batch).await
}

pub(crate) async fn apply_untracked_write_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &[UntrackedWriteRow],
) -> Result<(), LixError> {
    LiveUntrackedWriter::apply_write_batch(transaction, batch).await
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
        global: version_id == crate::version::GLOBAL_VERSION_ID,
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
        global: version_id == crate::version::GLOBAL_VERSION_ID,
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
pub(crate) fn is_untracked_live_table(table_name: &str) -> bool {
    storage::is_untracked_live_table(table_name)
}

pub(crate) fn coalesce_live_table_requirements(
    requirements: &[SchemaLiveTableRequirement],
) -> Vec<SchemaLiveTableRequirement> {
    let mut by_schema = std::collections::BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for requirement in requirements {
        by_schema
            .entry(requirement.schema_key.clone())
            .and_modify(|existing| {
                if existing.schema_definition.is_none() && requirement.schema_definition.is_some() {
                    existing.schema_definition = requirement.schema_definition.clone();
                }
            })
            .or_insert_with(|| requirement.clone());
    }
    by_schema.into_values().collect()
}
