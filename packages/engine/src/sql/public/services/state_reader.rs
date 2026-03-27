use std::collections::BTreeMap;

use crate::canonical::readers::{
    load_committed_version_head_commit_id_from_live_state,
    load_exact_committed_state_row_from_live_state,
};
pub(crate) use crate::canonical::readers::{
    CommitQueryExecutor, ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::live_state::roots::load_version_ref_with_backend;
pub(crate) use crate::live_state::roots::VersionRefRow;
use crate::live_state::tracked::{
    load_exact_row_with_backend as load_exact_tracked_row_with_backend,
    scan_rows_with_backend as scan_tracked_rows_with_backend,
    scan_rows_with_executor as scan_tracked_rows_with_executor, TrackedRow,
};
use crate::live_state::tracked::{
    load_exact_tombstone_with_executor, scan_tombstones_with_executor,
};
pub(crate) use crate::live_state::tracked::{
    ExactTrackedRowRequest, TrackedScanRequest, TrackedTombstoneMarker,
};
use crate::live_state::untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend,
    scan_rows_with_backend as scan_untracked_rows_with_backend,
    scan_rows_with_executor as scan_untracked_rows_with_executor, UntrackedRow,
};
use crate::live_state::{
    is_untracked_live_table, live_relation_name, load_live_schema_access_for_table_name,
    load_live_schema_access_with_backend, logical_snapshot_from_projected_row, LiveSchemaAccess,
};
use crate::{LixBackend, LixError, SqlDialect, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStorageLane {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveReadRow {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    metadata: Option<String>,
    writer_key: Option<String>,
    change_id: Option<String>,
    values: BTreeMap<String, Value>,
}

impl LiveReadRow {
    pub(crate) fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub(crate) fn schema_version(&self) -> &str {
        &self.schema_version
    }

    pub(crate) fn file_id(&self) -> &str {
        &self.file_id
    }

    pub(crate) fn version_id(&self) -> &str {
        &self.version_id
    }

    pub(crate) fn plugin_key(&self) -> &str {
        &self.plugin_key
    }

    pub(crate) fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }

    pub(crate) fn change_id(&self) -> Option<&str> {
        self.change_id.as_deref()
    }

    pub(crate) fn values(&self) -> &BTreeMap<String, Value> {
        &self.values
    }

    pub(crate) fn property_text(&self, property_name: &str) -> Option<String> {
        match self.values.get(property_name) {
            Some(Value::Text(value)) => Some(value.clone()),
            Some(Value::Integer(value)) => Some(value.to_string()),
            _ => None,
        }
    }
}

impl From<TrackedRow> for LiveReadRow {
    fn from(row: TrackedRow) -> Self {
        Self {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            file_id: row.file_id,
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            writer_key: row.writer_key,
            change_id: row.change_id,
            values: row.values,
        }
    }
}

impl From<UntrackedRow> for LiveReadRow {
    fn from(row: UntrackedRow) -> Self {
        Self {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            file_id: row.file_id,
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            writer_key: row.writer_key,
            change_id: None,
            values: row.values,
        }
    }
}

pub(crate) async fn load_committed_version_head_commit_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    load_committed_version_head_commit_id_from_live_state(&mut executor, version_id).await
}

pub(crate) async fn load_exact_committed_state_row(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    load_exact_committed_state_row_from_live_state(backend, request).await
}

pub(crate) async fn scan_live_rows(
    backend: &dyn LixBackend,
    storage: LiveStorageLane,
    schema_key: &str,
    version_id: &str,
    constraints: &[crate::live_state::constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<LiveReadRow>, LixError> {
    match storage {
        LiveStorageLane::Tracked => scan_tracked_rows_with_backend(
            backend,
            &TrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                constraints: constraints.to_vec(),
                required_columns: required_columns.to_vec(),
            },
        )
        .await
        .map(|rows| rows.into_iter().map(LiveReadRow::from).collect()),
        LiveStorageLane::Untracked => scan_untracked_rows_with_backend(
            backend,
            &crate::live_state::untracked::UntrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                constraints: constraints.to_vec(),
                required_columns: required_columns.to_vec(),
            },
        )
        .await
        .map(|rows| rows.into_iter().map(LiveReadRow::from).collect()),
    }
}

pub(crate) async fn scan_live_rows_with_executor_ref(
    executor: &mut dyn CommitQueryExecutor,
    storage: LiveStorageLane,
    schema_key: &str,
    version_id: &str,
    constraints: &[crate::live_state::constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<LiveReadRow>, LixError> {
    match storage {
        LiveStorageLane::Tracked => scan_tracked_rows_with_executor(
            executor,
            &TrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                constraints: constraints.to_vec(),
                required_columns: required_columns.to_vec(),
            },
        )
        .await
        .map(|rows| rows.into_iter().map(LiveReadRow::from).collect()),
        LiveStorageLane::Untracked => scan_untracked_rows_with_executor(
            executor,
            &crate::live_state::untracked::UntrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                constraints: constraints.to_vec(),
                required_columns: required_columns.to_vec(),
            },
        )
        .await
        .map(|rows| rows.into_iter().map(LiveReadRow::from).collect()),
    }
}

pub(crate) async fn load_exact_live_row(
    backend: &dyn LixBackend,
    storage: LiveStorageLane,
    schema_key: &str,
    version_id: &str,
    entity_id: &str,
    file_id: Option<&str>,
) -> Result<Option<LiveReadRow>, LixError> {
    match storage {
        LiveStorageLane::Tracked => load_exact_tracked_row_with_backend(
            backend,
            &ExactTrackedRowRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                entity_id: entity_id.to_string(),
                file_id: file_id.map(ToOwned::to_owned),
            },
        )
        .await
        .map(|row| row.map(LiveReadRow::from)),
        LiveStorageLane::Untracked => load_exact_untracked_row_with_backend(
            backend,
            &crate::live_state::untracked::ExactUntrackedRowRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                entity_id: entity_id.to_string(),
                file_id: file_id.map(ToOwned::to_owned),
            },
        )
        .await
        .map(|row| row.map(LiveReadRow::from)),
    }
}

pub(crate) async fn load_version_ref(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    load_version_ref_with_backend(backend, version_id).await
}

pub(crate) async fn load_exact_tombstone(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactTrackedRowRequest,
) -> Result<Option<TrackedTombstoneMarker>, LixError> {
    load_exact_tombstone_with_executor(executor, request).await
}

pub(crate) async fn scan_tombstones(
    executor: &mut dyn CommitQueryExecutor,
    request: &TrackedScanRequest,
) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
    scan_tombstones_with_executor(executor, request).await
}

pub(crate) async fn load_live_row_access(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveSchemaAccess, LixError> {
    load_live_schema_access_with_backend(backend, schema_key).await
}

pub(crate) async fn load_live_row_access_for_table(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveSchemaAccess>, LixError> {
    load_live_schema_access_for_table_name(backend, table_name).await
}

pub(crate) fn normalized_values_from_snapshot(
    access: &LiveSchemaAccess,
    snapshot_content: Option<&str>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    access.normalized_values(snapshot_content)
}

pub(crate) fn projected_row_snapshot_json(
    access: Option<&LiveSchemaAccess>,
    schema_key: &str,
    row: &[Value],
    first_projected_column: usize,
    raw_snapshot_index: usize,
) -> Result<serde_json::Value, LixError> {
    logical_snapshot_from_projected_row(
        access.map(|access| access.raw_access()),
        schema_key,
        row,
        first_projected_column,
        raw_snapshot_index,
    )
    .and_then(|value| {
        value.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "projected row for schema '{}' did not contain a logical snapshot",
                    schema_key
                ),
            )
        })
    })
}

pub(crate) fn snapshot_json_from_row(
    access: &LiveSchemaAccess,
    row: &LiveReadRow,
) -> Result<serde_json::Value, LixError> {
    access.snapshot_json_from_values(row.schema_key(), row.values())
}

pub(crate) fn snapshot_text_from_row(
    access: &LiveSchemaAccess,
    row: &LiveReadRow,
) -> Result<String, LixError> {
    access.snapshot_text_from_values(row.schema_key(), row.values())
}

pub(crate) fn is_untracked_live_table_name(table_name: &str) -> bool {
    is_untracked_live_table(table_name)
}

pub(crate) async fn live_storage_relation_exists(
    backend: &dyn LixBackend,
    storage: LiveStorageLane,
    schema_key: &str,
) -> Result<bool, LixError> {
    let _ = storage;
    let relation_name = live_relation_name(schema_key);
    match backend.dialect() {
        SqlDialect::Sqlite => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM information_schema.tables \
                     WHERE table_name = $1 \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}
