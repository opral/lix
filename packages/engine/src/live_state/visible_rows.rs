use crate::live_state::store::{LiveStateBackendRef, LiveStateExecutorRef};
use crate::{LixError, Value};

use super::constraints::ScanConstraint;
use super::schema_access::LiveRowShape;
use super::tracked::{
    scan_rows_with_executor as scan_tracked_rows_with_executor, TrackedRow, TrackedScanRequest,
};
use super::untracked::{
    scan_rows_with_executor as scan_untracked_rows_with_executor, UntrackedRow,
    UntrackedScanRequest,
};

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
    file_id: Option<String>,
    version_id: String,
    plugin_key: Option<String>,
    metadata: Option<String>,
    change_id: Option<String>,
    values: std::collections::BTreeMap<String, Value>,
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

    pub(crate) fn file_id(&self) -> Option<&str> {
        self.file_id.as_deref()
    }

    pub(crate) fn version_id(&self) -> &str {
        &self.version_id
    }

    pub(crate) fn plugin_key(&self) -> Option<&str> {
        self.plugin_key.as_deref()
    }

    pub(crate) fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn change_id(&self) -> Option<&str> {
        self.change_id.as_deref()
    }

    pub(crate) fn values(&self) -> &std::collections::BTreeMap<String, Value> {
        &self.values
    }

    pub(crate) fn snapshot_text(&self, access: &LiveRowShape) -> Result<String, LixError> {
        access.snapshot_text_from_values(self.schema_key(), self.values())
    }

    pub(crate) fn snapshot_json(
        &self,
        access: &LiveRowShape,
    ) -> Result<serde_json::Value, LixError> {
        access.snapshot_json_from_values(self.schema_key(), self.values())
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
            change_id: Some(row.change_id),
            values: row.values,
        }
    }
}

pub(crate) async fn scan_live_rows(
    backend: LiveStateBackendRef<'_>,
    storage: LiveStorageLane,
    schema_key: &str,
    version_id: &str,
    constraints: &[ScanConstraint],
    required_columns: &[String],
    limit: Option<usize>,
) -> Result<Vec<LiveReadRow>, LixError> {
    let mut executor = backend;
    scan_live_rows_with_executor_ref(
        &mut executor,
        storage,
        schema_key,
        version_id,
        constraints,
        required_columns,
        limit,
    )
    .await
}

async fn scan_live_rows_with_executor_ref(
    executor: LiveStateExecutorRef<'_>,
    storage: LiveStorageLane,
    schema_key: &str,
    version_id: &str,
    constraints: &[ScanConstraint],
    required_columns: &[String],
    limit: Option<usize>,
) -> Result<Vec<LiveReadRow>, LixError> {
    match storage {
        LiveStorageLane::Tracked => {
            let rows = scan_tracked_rows_with_executor(
                executor,
                &TrackedScanRequest {
                    schema_key: schema_key.to_string(),
                    version_id: version_id.to_string(),
                    constraints: constraints.to_vec(),
                    required_columns: required_columns.to_vec(),
                },
            )
            .await?;
            Ok(rows.into_iter().map(LiveReadRow::from).collect())
        }
        LiveStorageLane::Untracked => {
            let request = UntrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                constraints: constraints.to_vec(),
                required_columns: required_columns.to_vec(),
            };
            let rows = match limit {
                Some(limit) => {
                    super::untracked::scan_rows_with_executor_limit(executor, &request, limit)
                        .await?
                }
                None => scan_untracked_rows_with_executor(executor, &request).await?,
            };
            Ok(rows.into_iter().map(LiveReadRow::from).collect())
        }
    }
}
