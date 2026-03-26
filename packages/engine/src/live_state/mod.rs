//! Live-state subsystem boundary.
//!
//! `live_state` owns:
//! - lifecycle initialization and readiness checks
//! - schema-scoped storage initialization
//! - raw, system, tracked, untracked, and effective row access
//! - rebuild planning and apply
//!
//! Future hook-in work should target the entrypoints exported here instead of
//! reaching into `storage/` or lifecycle internals directly.

pub mod constraints;
pub mod effective;
mod lifecycle;
mod materialize;
pub(crate) mod raw;
pub(crate) mod shared;
mod storage;
pub mod system;
pub mod tracked;
pub mod untracked;

use crate::{LixBackend, LixBackendTransaction, LixError};
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
    registered_snapshot: Option<JsonValue>,
    #[serde(skip, default)]
    source: SchemaRegistrationSource,
}

#[derive(Debug, Clone, PartialEq, Default)]
enum SchemaRegistrationSource {
    #[default]
    StoredLayout,
    LegacyLayout(storage::LiveTableLayout),
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

    pub(crate) fn with_legacy_layout(
        schema_key: impl Into<String>,
        layout: &crate::schema::live_layout::LiveTableLayout,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            registered_snapshot: None,
            source: SchemaRegistrationSource::LegacyLayout(storage_layout_from_legacy(layout)),
        }
    }

    pub(crate) fn registered_snapshot(&self) -> Option<&JsonValue> {
        self.registered_snapshot.as_ref()
    }

    pub(crate) fn layout_override(&self) -> Option<&storage::LiveTableLayout> {
        match &self.source {
            SchemaRegistrationSource::StoredLayout => None,
            SchemaRegistrationSource::LegacyLayout(layout) => Some(layout),
        }
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

#[cfg(test)]
pub(crate) use lifecycle::LIVE_STATE_SCHEMA_EPOCH;
pub(crate) use materialize::{
    apply_live_state_scope_in_transaction, live_state_rebuild_plan_with_executor,
};
pub(crate) use storage::{
    is_untracked_live_table, load_live_row_access_for_table_name,
    load_live_row_access_with_backend, load_live_row_access_with_executor,
    logical_snapshot_from_projected_row, normalized_live_column_values, LiveRowAccess,
};

fn storage_layout_from_legacy(
    layout: &crate::schema::live_layout::LiveTableLayout,
) -> storage::LiveTableLayout {
    storage::LiveTableLayout {
        schema_key: layout.schema_key.clone(),
        columns: layout
            .columns
            .iter()
            .map(|column| storage::LiveColumnSpec {
                property_name: column.property_name.clone(),
                column_name: column.column_name.clone(),
                required: column.required,
                nullable: column.nullable,
                kind: match column.kind {
                    crate::schema::live_layout::LiveColumnKind::String => {
                        storage::LiveColumnKind::String
                    }
                    crate::schema::live_layout::LiveColumnKind::Integer => {
                        storage::LiveColumnKind::Integer
                    }
                    crate::schema::live_layout::LiveColumnKind::Number => {
                        storage::LiveColumnKind::Number
                    }
                    crate::schema::live_layout::LiveColumnKind::Boolean => {
                        storage::LiveColumnKind::Boolean
                    }
                    crate::schema::live_layout::LiveColumnKind::JsonText => {
                        storage::LiveColumnKind::JsonText
                    }
                },
            })
            .collect(),
    }
}
