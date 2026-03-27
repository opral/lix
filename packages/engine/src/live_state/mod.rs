//! Live-state subsystem boundary.
//!
//! `live_state` owns:
//! - lifecycle initialization and readiness checks
//! - schema-scoped storage initialization
//! - raw, session, roots, tracked, untracked, and effective row access
//! - rebuild planning and apply
//!
//! Future hook-in work should target the entrypoints exported here instead of
//! reaching into `storage/` or lifecycle internals directly.

pub mod constraints;
pub mod effective;
mod lifecycle;
mod materialize;
pub(crate) mod raw;
pub mod roots;
pub mod session;
pub(crate) mod shared;
mod storage;
pub mod tracked;
pub mod untracked;

use crate::backend::QueryExecutor;
use crate::schema::schema_from_registered_snapshot;
use crate::sql::execution::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::{LixBackend, LixBackendTransaction, LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;

pub use lifecycle::{CanonicalWatermark, LiveStateMode, LiveStateReadiness};
pub use materialize::{
    LatestVisibleWinnerDebugRow, LiveStateApplyReport, LiveStateRebuildDebugMode,
    LiveStateRebuildDebugTrace, LiveStateRebuildPlan, LiveStateRebuildReport,
    LiveStateRebuildRequest, LiveStateRebuildScope, LiveStateRebuildWarning, LiveStateWrite,
    LiveStateWriteOp, ScopeWinnerDebugRow, StageStat, TraversedCommitDebugRow,
    TraversedEdgeDebugRow, VersionAncestryDebugRow, VersionHeadDebugRow,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SchemaRegistration {
    schema_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    schema_definition: Option<JsonValue>,
}

impl From<&str> for SchemaRegistration {
    fn from(schema_key: &str) -> Self {
        Self::new(schema_key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveSchemaColumn {
    pub(crate) property_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveSchemaAccess {
    access: storage::LiveRowAccess,
    columns: Vec<LiveSchemaColumn>,
}

impl LiveSchemaAccess {
    pub(crate) fn raw_access(&self) -> &storage::LiveRowAccess {
        &self.access
    }

    pub(crate) fn columns(&self) -> &[LiveSchemaColumn] {
        &self.columns
    }

    pub(crate) fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        self.access.normalized_projection_sql(table_alias)
    }

    pub(crate) fn normalized_values(
        &self,
        snapshot_content: Option<&str>,
    ) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
        live_schema_normalized_values(self.access.layout().schema_key.as_str(), None, snapshot_content)
    }

    pub(crate) fn snapshot_json_from_values(
        &self,
        schema_key: &str,
        values: &std::collections::BTreeMap<String, Value>,
    ) -> Result<JsonValue, LixError> {
        snapshot_json_from_values(&self.access, schema_key, values)
    }

    pub(crate) fn snapshot_text_from_values(
        &self,
        schema_key: &str,
        values: &std::collections::BTreeMap<String, Value>,
    ) -> Result<String, LixError> {
        snapshot_text_from_values(&self.access, schema_key, values)
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
            schema_definition: None,
        }
    }

    pub fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub fn with_registered_snapshot(
        schema_key: impl Into<String>,
        registered_snapshot: JsonValue,
    ) -> Self {
        let schema_key = schema_key.into();
        let schema_definition = schema_from_registered_snapshot(&registered_snapshot)
            .ok()
            .map(|(_, schema)| schema);
        Self {
            schema_key,
            schema_definition,
        }
    }

    pub(crate) fn with_schema_definition(
        schema_key: impl Into<String>,
        schema_definition: JsonValue,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            schema_definition: Some(schema_definition),
        }
    }

    pub(crate) fn schema_definition(&self) -> Option<&JsonValue> {
        self.schema_definition.as_ref()
    }
}

pub async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::init(backend).await
}

pub async fn require_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::require_ready(backend).await
}

pub async fn register_schema(
    backend: &dyn LixBackend,
    registration: impl Into<SchemaRegistration>,
) -> Result<(), LixError> {
    let registration = registration.into();
    storage::register_schema(backend, &registration).await
}

pub async fn finalize_commit(backend: &dyn LixBackend) -> Result<CanonicalWatermark, LixError> {
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
) -> Result<CanonicalWatermark, LixError> {
    lifecycle::finalize_commit_in_transaction(transaction).await
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

pub(crate) async fn load_latest_canonical_watermark(
    backend: &dyn LixBackend,
) -> Result<Option<CanonicalWatermark>, LixError> {
    lifecycle::load_latest_canonical_watermark(backend).await
}

pub(crate) async fn mark_mode_with_backend(
    backend: &dyn LixBackend,
    mode: LiveStateMode,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_mode_with_backend(backend, mode).await
}

pub(crate) async fn mark_ready_with_backend(
    backend: &dyn LixBackend,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_ready_with_backend(backend, watermark).await
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

pub(crate) async fn version_exists_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<bool, LixError> {
    raw::load_exact_row_with_backend(
        backend,
        raw::RawStorage::Tracked,
        crate::version::version_descriptor_schema_key(),
        crate::version::version_descriptor_storage_version_id(),
        version_id,
        Some(crate::version::version_descriptor_file_id()),
    )
    .await
    .map(|row| {
        row.as_ref()
            .is_some_and(|row| row.plugin_key() == crate::version::version_descriptor_plugin_key())
    })
}

pub(crate) async fn version_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<bool, LixError> {
    raw::load_exact_row_with_executor(
        executor,
        raw::RawStorage::Tracked,
        crate::version::version_descriptor_schema_key(),
        crate::version::version_descriptor_storage_version_id(),
        version_id,
        Some(crate::version::version_descriptor_file_id()),
    )
    .await
    .map(|row| {
        row.as_ref()
            .is_some_and(|row| row.plugin_key() == crate::version::version_descriptor_plugin_key())
    })
}

pub(crate) async fn scan_tracked_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
    version_id: &str,
    constraints: &[constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<tracked::TrackedRow>, LixError> {
    tracked::scan_rows_with_executor(
        executor,
        &tracked::TrackedScanRequest {
            schema_key: schema_key.to_string(),
            version_id: version_id.to_string(),
            constraints: constraints.to_vec(),
            required_columns: required_columns.to_vec(),
        },
    )
    .await
}

pub(crate) async fn scan_untracked_rows_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
    version_id: &str,
    constraints: &[constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<untracked::UntrackedRow>, LixError> {
    untracked::scan_rows_with_executor(
        executor,
        &untracked::UntrackedScanRequest {
            schema_key: schema_key.to_string(),
            version_id: version_id.to_string(),
            constraints: constraints.to_vec(),
            required_columns: required_columns.to_vec(),
        },
    )
    .await
}

pub(crate) fn snapshot_json_from_values(
    access: &storage::LiveRowAccess,
    schema_key: &str,
    values: &std::collections::BTreeMap<String, crate::Value>,
) -> Result<JsonValue, LixError> {
    raw::snapshot_json_from_values(access, schema_key, values)
}

pub(crate) fn snapshot_text_from_values(
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

pub(crate) async fn load_live_schema_access_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveSchemaAccess, LixError> {
    storage::load_live_row_access_with_backend(backend, schema_key)
        .await
        .map(live_schema_access_from_storage)
}

pub(crate) async fn load_live_schema_access_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
) -> Result<LiveSchemaAccess, LixError> {
    storage::load_live_row_access_with_executor(executor, schema_key)
        .await
        .map(live_schema_access_from_storage)
}

pub(crate) async fn load_live_schema_access_for_table_name(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveSchemaAccess>, LixError> {
    storage::load_live_row_access_for_table_name(backend, table_name)
        .await
        .map(|access| access.map(live_schema_access_from_storage))
}

pub(crate) fn live_relation_name(schema_key: &str) -> String {
    storage::tracked_live_table_name(schema_key)
}

pub(crate) fn live_schema_payload_column_name(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    let layout = live_schema_layout(schema_key, schema_definition)?;
    storage::live_column_name_for_property(&layout, property_name)
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

pub(crate) fn live_schema_normalized_projection_sql(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(storage::LiveRowAccess::new(live_schema_layout(schema_key, schema_definition)?)
        .normalized_projection_sql(table_alias))
}

#[cfg(test)]
pub(crate) fn live_schema_column_names(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<Vec<String>, LixError> {
    Ok(live_schema_layout(schema_key, schema_definition)?
        .columns
        .into_iter()
        .map(|column| column.column_name)
        .collect())
}

pub(crate) fn live_schema_snapshot_select_expr(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(shared::snapshot_sql::live_snapshot_select_expr(
        &live_schema_layout(schema_key, schema_definition)?,
        dialect,
        table_alias,
    ))
}

pub(crate) fn live_schema_normalized_values(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    snapshot_content: Option<&str>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    storage::normalized_live_column_values(
        &live_schema_layout(schema_key, schema_definition)?,
        snapshot_content,
    )
}

#[cfg(test)]
pub(crate) fn live_schema_snapshot_text_from_values(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    values: &std::collections::BTreeMap<String, Value>,
) -> Result<String, LixError> {
    let access = storage::LiveRowAccess::new(live_schema_layout(schema_key, schema_definition)?);
    snapshot_text_from_values(&access, schema_key, values)
}

fn live_schema_layout(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<storage::LiveTableLayout, LixError> {
    if let Some(schema_definition) = schema_definition {
        return storage::live_table_layout_from_schema(schema_definition);
    }
    storage::builtin_live_table_layout(schema_key)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing live schema definition for '{}'", schema_key),
        )
    })
}

fn live_schema_access_from_storage(access: storage::LiveRowAccess) -> LiveSchemaAccess {
    LiveSchemaAccess {
        columns: access
            .columns()
            .iter()
            .map(|column| LiveSchemaColumn {
                property_name: column.property_name.clone(),
            })
            .collect(),
        access,
    }
}

#[cfg(test)]
pub(crate) use lifecycle::LIVE_STATE_SCHEMA_EPOCH;
pub(crate) use storage::{
    is_untracked_live_table, logical_snapshot_from_projected_row,
};

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
