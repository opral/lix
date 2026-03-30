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
pub(crate) mod raw;
pub(crate) mod schema_access;
pub(crate) mod shared;
mod storage;
pub mod tracked;
pub mod untracked;

pub use crate::canonical::CanonicalWatermark;
use crate::live_state::shared::identity::RowIdentity;
use crate::sql::executor::contracts::planned_statement::SchemaLiveTableRequirement;
use crate::{LixBackend, LixBackendTransaction, LixError};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub use init::init;
pub(crate) use lifecycle::LiveStateProjectionStatus;
pub use lifecycle::{LiveStateMode, LiveStateReadiness};
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
    Layout(storage::LiveTableLayout),
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
            source: SchemaRegistrationSource::Layout(
                storage::live_table_layout_from_schema(&schema_definition)
                    .expect("schema definition should compile to a live layout"),
            ),
        }
    }

    pub(crate) fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    pub(crate) fn layout_override(&self) -> Option<&storage::LiveTableLayout> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::Layout(layout) => Some(layout),
        }
    }
}

pub async fn require_ready(backend: &dyn LixBackend) -> Result<(), LixError> {
    lifecycle::require_ready(backend).await
}

pub async fn projection_status(backend: &dyn LixBackend) -> Result<ProjectionStatus, LixError> {
    projection::projection_status(backend).await
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

pub(crate) async fn load_projection_status_with_backend(
    backend: &dyn LixBackend,
) -> Result<LiveStateProjectionStatus, LixError> {
    lifecycle::load_projection_status_with_backend(backend).await
}

pub(crate) async fn mark_needs_rebuild_at_canonical_watermark_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    lifecycle::mark_needs_rebuild_at_canonical_watermark_in_transaction(transaction, watermark)
        .await
}

pub(crate) async fn advance_commit_replay_boundary_to_watermark_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    lifecycle::advance_commit_replay_boundary_to_watermark_in_transaction(transaction, watermark)
        .await
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

pub(crate) async fn mark_ready_at_canonical_watermark_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    lifecycle::mark_live_state_ready_at_canonical_watermark_in_transaction(transaction, watermark)
        .await
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
pub(crate) use storage::is_untracked_live_table;

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
