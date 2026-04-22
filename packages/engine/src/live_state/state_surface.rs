use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;

use crate::backend::TransactionBeginMode;
use crate::common::escape_sql_string;
use crate::{LixBackend, LixError, Value};

use super::commit_derived::{is_lazy_commit_derived_surface, scan_commit_derived_rows};
use super::schema_access::load_live_row_shape_with_backend;
use super::tracked::{
    scan_tombstones_with_backend as scan_tracked_tombstones_with_backend, TrackedScanRequest,
};
use super::untracked::{
    scan_rows_with_backend as scan_untracked_rows_with_backend, UntrackedScanRequest,
};
use super::{
    decode_registered_schema_row, scan_live_rows, scan_tracked_rows_with_backend,
    scan_untracked_rows_with_backend_limit, storage::no_live_columns, LiveRow, LiveRowQuery,
    LiveRowSource, ScanConstraint, ScanField, ScanOperator, TrackedRow, TrackedTombstoneMarker,
    UntrackedRow,
};
use crate::version::GLOBAL_VERSION_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSurfaceColumn {
    EntityId,
    SchemaKey,
    FileId,
    PluginKey,
    SnapshotContent,
    Metadata,
    SchemaVersion,
    CreatedAt,
    UpdatedAt,
    Global,
    ChangeId,
    CommitId,
    Untracked,
    VersionId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StateSurfaceFilter {
    Eq(StateSurfaceColumn, Value),
    In(StateSurfaceColumn, Vec<Value>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSurfaceRow {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub schema_version: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub global: bool,
    pub change_id: Option<String>,
    pub commit_id: Option<String>,
    pub untracked: bool,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateByVersionScanRequest {
    pub version_id: Option<String>,
    pub projection: Vec<StateSurfaceColumn>,
    pub filters: Vec<StateSurfaceFilter>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait StateByVersionSnapshot: Debug + Send + Sync {
    async fn scan_state_by_version_batches(
        &self,
        request: &StateByVersionScanRequest,
    ) -> Result<Vec<RecordBatch>, LixError>;
}

pub async fn open_state_by_version_snapshot(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Arc<dyn StateByVersionSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedStateByVersion::load_exact(backend, version_id).await?,
    ))
}

pub async fn open_visible_state_by_version_snapshot(
    backend: &dyn LixBackend,
) -> Result<Arc<dyn StateByVersionSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedStateByVersion::load_visible_versions(backend).await?,
    ))
}

pub fn open_state_by_version_reader_with_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Arc<dyn StateByVersionSnapshot> {
    Arc::new(BackendBackedStateByVersion { backend })
}

pub async fn open_state_by_version_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<Arc<dyn StateByVersionSnapshot>, LixError> {
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedStateByVersionCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("state-by-version-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("state_by_version runtime should build");
            let init = runtime
                .block_on(async { backend.begin_transaction(TransactionBeginMode::Read).await });

            let mut transaction = match init {
                Ok(transaction) => {
                    let _ = ready_tx.send(Ok(()));
                    transaction
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };

            while let Ok(command) = command_rx.recv() {
                match command {
                    TransactionBackedStateByVersionCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            let mut rows = load_state_by_version_rows(&backend, &request).await?;

                            if !request.filters.is_empty() {
                                rows.retain(|row| {
                                    request
                                        .filters
                                        .iter()
                                        .all(|filter| matches_filter(row, filter))
                                });
                            }

                            if let Some(limit) = request.limit {
                                rows.truncate(limit);
                            }

                            state_surface_batches_from_rows(&request.projection, &rows)
                        });
                        let _ = reply.send(result);
                    }
                }
            }

            let _ = runtime.block_on(transaction.rollback());
        })
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to spawn state_by_version snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state_by_version snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedStateByVersion {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedStateByVersion {
    pinned_version_id: Option<String>,
    rows: Vec<StateSurfaceRow>,
}

impl SnapshotBackedStateByVersion {
    async fn load_exact(backend: &dyn LixBackend, version_id: &str) -> Result<Self, LixError> {
        let rows = load_state_by_version_rows_for_version(
            backend,
            version_id,
            &StateByVersionScanRequest {
                version_id: Some(version_id.to_string()),
                projection: Vec::new(),
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
        Ok(Self {
            pinned_version_id: Some(version_id.to_string()),
            rows,
        })
    }

    async fn load_visible_versions(backend: &dyn LixBackend) -> Result<Self, LixError> {
        let rows = load_state_by_version_rows(
            backend,
            &StateByVersionScanRequest {
                version_id: None,
                projection: Vec::new(),
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
        Ok(Self {
            pinned_version_id: None,
            rows,
        })
    }
}

#[derive(Debug)]
enum TransactionBackedStateByVersionCommand {
    Scan {
        request: StateByVersionScanRequest,
        reply: oneshot::Sender<Result<Vec<RecordBatch>, LixError>>,
    },
}

struct TransactionBackedStateByVersion {
    commands: mpsc::Sender<TransactionBackedStateByVersionCommand>,
}

impl std::fmt::Debug for TransactionBackedStateByVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedStateByVersion").finish()
    }
}

#[async_trait(?Send)]
impl StateByVersionSnapshot for SnapshotBackedStateByVersion {
    async fn scan_state_by_version_batches(
        &self,
        request: &StateByVersionScanRequest,
    ) -> Result<Vec<RecordBatch>, LixError> {
        if let (Some(pinned_version_id), Some(requested_version_id)) = (
            self.pinned_version_id.as_deref(),
            request.version_id.as_deref(),
        ) {
            if requested_version_id != pinned_version_id {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "cached state_by_version snapshot was opened for version '{}' but query requested '{}'",
                        pinned_version_id, requested_version_id
                    ),
                ));
            }
        }
        let mut rows = self.rows.clone();

        if let Some(requested_version_id) = request.version_id.as_deref() {
            rows.retain(|row| row.version_id == requested_version_id);
        }

        if !request.filters.is_empty() {
            rows.retain(|row| {
                request
                    .filters
                    .iter()
                    .all(|filter| matches_filter(row, filter))
            });
        }

        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }

        state_surface_batches_from_rows(&request.projection, &rows)
    }
}

#[async_trait(?Send)]
impl StateByVersionSnapshot for TransactionBackedStateByVersion {
    async fn scan_state_by_version_batches(
        &self,
        request: &StateByVersionScanRequest,
    ) -> Result<Vec<RecordBatch>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedStateByVersionCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue state_by_version scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state_by_version snapshot worker dropped scan reply",
            )
        })?
    }
}

#[async_trait(?Send)]
impl StateByVersionSnapshot for BackendBackedStateByVersion {
    async fn scan_state_by_version_batches(
        &self,
        request: &StateByVersionScanRequest,
    ) -> Result<Vec<RecordBatch>, LixError> {
        let mut rows = load_state_by_version_rows(self.backend.as_ref(), request).await?;

        if !request.filters.is_empty() {
            rows.retain(|row| {
                request
                    .filters
                    .iter()
                    .all(|filter| matches_filter(row, filter))
            });
        }

        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }

        state_surface_batches_from_rows(&request.projection, &rows)
    }
}

fn state_surface_batches_from_rows(
    projection: &[StateSurfaceColumn],
    rows: &[StateSurfaceRow],
) -> Result<Vec<RecordBatch>, LixError> {
    Ok(vec![state_surface_record_batch(
        &resolved_projection(projection),
        rows,
    )?])
}

fn resolved_projection(projection: &[StateSurfaceColumn]) -> Vec<StateSurfaceColumn> {
    if projection.is_empty() {
        all_state_surface_columns()
    } else {
        projection.to_vec()
    }
}

fn all_state_surface_columns() -> Vec<StateSurfaceColumn> {
    vec![
        StateSurfaceColumn::EntityId,
        StateSurfaceColumn::SchemaKey,
        StateSurfaceColumn::FileId,
        StateSurfaceColumn::PluginKey,
        StateSurfaceColumn::SnapshotContent,
        StateSurfaceColumn::Metadata,
        StateSurfaceColumn::SchemaVersion,
        StateSurfaceColumn::CreatedAt,
        StateSurfaceColumn::UpdatedAt,
        StateSurfaceColumn::Global,
        StateSurfaceColumn::ChangeId,
        StateSurfaceColumn::CommitId,
        StateSurfaceColumn::Untracked,
        StateSurfaceColumn::VersionId,
    ]
}

fn state_surface_schema(columns: &[StateSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|column| match column {
                StateSurfaceColumn::EntityId => Field::new("entity_id", DataType::Utf8, false),
                StateSurfaceColumn::SchemaKey => Field::new("schema_key", DataType::Utf8, false),
                StateSurfaceColumn::FileId => Field::new("file_id", DataType::Utf8, true),
                StateSurfaceColumn::PluginKey => Field::new("plugin_key", DataType::Utf8, true),
                StateSurfaceColumn::SnapshotContent => {
                    Field::new("snapshot_content", DataType::Utf8, true)
                }
                StateSurfaceColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                StateSurfaceColumn::SchemaVersion => {
                    Field::new("schema_version", DataType::Utf8, true)
                }
                StateSurfaceColumn::CreatedAt => Field::new("created_at", DataType::Utf8, true),
                StateSurfaceColumn::UpdatedAt => Field::new("updated_at", DataType::Utf8, true),
                StateSurfaceColumn::Global => Field::new("global", DataType::Boolean, false),
                StateSurfaceColumn::ChangeId => Field::new("change_id", DataType::Utf8, true),
                StateSurfaceColumn::CommitId => Field::new("commit_id", DataType::Utf8, true),
                StateSurfaceColumn::Untracked => Field::new("untracked", DataType::Boolean, false),
                StateSurfaceColumn::VersionId => Field::new("version_id", DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ))
}

fn state_surface_record_batch(
    columns: &[StateSurfaceColumn],
    rows: &[StateSurfaceRow],
) -> Result<RecordBatch, LixError> {
    let arrays = columns
        .iter()
        .map(|column| match column {
            StateSurfaceColumn::EntityId => {
                string_array(rows.iter().map(|row| Some(row.entity_id.as_str())))
            }
            StateSurfaceColumn::SchemaKey => {
                string_array(rows.iter().map(|row| Some(row.schema_key.as_str())))
            }
            StateSurfaceColumn::FileId => {
                string_array(rows.iter().map(|row| row.file_id.as_deref()))
            }
            StateSurfaceColumn::PluginKey => {
                string_array(rows.iter().map(|row| row.plugin_key.as_deref()))
            }
            StateSurfaceColumn::SnapshotContent => {
                string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
            }
            StateSurfaceColumn::Metadata => {
                string_array(rows.iter().map(|row| row.metadata.as_deref()))
            }
            StateSurfaceColumn::SchemaVersion => {
                string_array(rows.iter().map(|row| row.schema_version.as_deref()))
            }
            StateSurfaceColumn::CreatedAt => {
                string_array(rows.iter().map(|row| row.created_at.as_deref()))
            }
            StateSurfaceColumn::UpdatedAt => {
                string_array(rows.iter().map(|row| row.updated_at.as_deref()))
            }
            StateSurfaceColumn::Global => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.global).collect::<Vec<_>>(),
            )) as ArrayRef,
            StateSurfaceColumn::ChangeId => {
                string_array(rows.iter().map(|row| row.change_id.as_deref()))
            }
            StateSurfaceColumn::CommitId => {
                string_array(rows.iter().map(|row| row.commit_id.as_deref()))
            }
            StateSurfaceColumn::Untracked => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.untracked).collect::<Vec<_>>(),
            )) as ArrayRef,
            StateSurfaceColumn::VersionId => {
                string_array(rows.iter().map(|row| Some(row.version_id.as_str())))
            }
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(state_surface_schema(columns), arrays).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("state_surface failed to build Arrow batch: {error}"),
        )
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

fn matches_filter(row: &StateSurfaceRow, filter: &StateSurfaceFilter) -> bool {
    match filter {
        StateSurfaceFilter::Eq(column, expected) => {
            state_surface_column_value(row, *column).is_some_and(|actual| actual == *expected)
        }
        StateSurfaceFilter::In(column, expected) => state_surface_column_value(row, *column)
            .is_some_and(|actual| expected.iter().any(|candidate| candidate == &actual)),
    }
}

fn state_surface_column_value(row: &StateSurfaceRow, column: StateSurfaceColumn) -> Option<Value> {
    match column {
        StateSurfaceColumn::EntityId => Some(Value::Text(row.entity_id.clone())),
        StateSurfaceColumn::SchemaKey => Some(Value::Text(row.schema_key.clone())),
        StateSurfaceColumn::FileId => {
            Some(row.file_id.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        StateSurfaceColumn::PluginKey => Some(
            row.plugin_key
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::SnapshotContent => Some(
            row.snapshot_content
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::Metadata => {
            Some(row.metadata.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        StateSurfaceColumn::SchemaVersion => Some(
            row.schema_version
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::CreatedAt => Some(
            row.created_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::UpdatedAt => Some(
            row.updated_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::Global => Some(Value::Boolean(row.global)),
        StateSurfaceColumn::ChangeId => Some(
            row.change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::CommitId => Some(
            row.commit_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        StateSurfaceColumn::Untracked => Some(Value::Boolean(row.untracked)),
        StateSurfaceColumn::VersionId => Some(Value::Text(row.version_id.clone())),
    }
}

async fn load_state_by_version_rows(
    backend: &dyn LixBackend,
    request: &StateByVersionScanRequest,
) -> Result<Vec<StateSurfaceRow>, LixError> {
    let target_version_ids = resolve_target_version_ids_for_request(backend, request).await?;
    let mut rows = Vec::new();
    for version_id in target_version_ids {
        rows.extend(load_state_by_version_rows_for_version(backend, &version_id, request).await?);
    }
    Ok(rows)
}

async fn load_state_by_version_rows_for_version(
    backend: &dyn LixBackend,
    version_id: &str,
    request: &StateByVersionScanRequest,
) -> Result<Vec<StateSurfaceRow>, LixError> {
    let route = StateSurfaceSourceRoute::from_request(version_id, request);
    if route.contradictory {
        return Ok(Vec::new());
    }

    let schema_keys = resolve_state_schema_keys(backend, version_id, &route).await?;
    let source_limit = source_limit_for_request(request, &route, schema_keys.len(), version_id);
    let mut rows = Vec::<LiveRow>::new();

    for schema_key in schema_keys {
        rows.extend(
            load_effective_rows_for_schema(
                backend,
                version_id,
                &schema_key,
                &route,
                request,
                source_limit,
            )
            .await?,
        );
    }

    let change_commit_ids = if request_needs_commit_id(request) {
        load_change_commit_ids_with_backend(backend, &rows).await?
    } else {
        BTreeMap::new()
    };
    Ok(rows
        .into_iter()
        .map(|row| StateSurfaceRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            plugin_key: row.plugin_key,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: Some(row.schema_version),
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            commit_id: row
                .change_id
                .as_ref()
                .and_then(|change_id| change_commit_ids.get(change_id).cloned()),
            change_id: row.change_id,
            untracked: row.untracked,
            // `state_by_version` exposes the effective row visible for the requested scope.
            // Global fallback stays visible via `global = true`, while the public version
            // column reflects the explicit version scope the query asked for.
            version_id: version_id.to_string(),
        })
        .collect())
}

async fn resolve_target_version_ids_for_request(
    backend: &dyn LixBackend,
    request: &StateByVersionScanRequest,
) -> Result<Vec<String>, LixError> {
    if let Some(version_id) = request.version_id.clone() {
        return Ok(vec![version_id]);
    }

    let mut version_ids =
        crate::live_state::load_current_committed_version_frontier_with_backend(backend)
            .await?
            .version_heads
            .into_keys()
            .collect::<Vec<_>>();
    version_ids.push(GLOBAL_VERSION_ID.to_string());
    version_ids.sort();
    version_ids.dedup();
    Ok(version_ids)
}

fn request_needs_commit_id(request: &StateByVersionScanRequest) -> bool {
    request.projection.is_empty()
        || request.projection.contains(&StateSurfaceColumn::CommitId)
        || request
            .filters
            .iter()
            .any(|filter| matches_filter_column(filter, StateSurfaceColumn::CommitId))
}

fn matches_filter_column(filter: &StateSurfaceFilter, column: StateSurfaceColumn) -> bool {
    match filter {
        StateSurfaceFilter::Eq(candidate, _) | StateSurfaceFilter::In(candidate, _) => {
            *candidate == column
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StateSurfaceLane {
    LocalTracked,
    LocalUntracked,
    GlobalTracked,
    GlobalUntracked,
}

impl StateSurfaceLane {
    fn source(self) -> LiveRowSource {
        match self {
            Self::LocalTracked | Self::GlobalTracked => LiveRowSource::Tracked,
            Self::LocalUntracked | Self::GlobalUntracked => LiveRowSource::Untracked,
        }
    }

    fn version_id(self, requested_version_id: &str) -> String {
        match self {
            Self::LocalTracked | Self::LocalUntracked => requested_version_id.to_string(),
            Self::GlobalTracked | Self::GlobalUntracked => GLOBAL_VERSION_ID.to_string(),
        }
    }

    fn is_global(self) -> bool {
        matches!(self, Self::GlobalTracked | Self::GlobalUntracked)
    }

    fn include_tombstones(self) -> bool {
        matches!(self, Self::LocalTracked | Self::GlobalTracked)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateSurfaceSourceRoute {
    schema_keys: Option<Vec<String>>,
    constraints: Vec<ScanConstraint>,
    global: Option<bool>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl StateSurfaceSourceRoute {
    fn from_request(version_id: &str, request: &StateByVersionScanRequest) -> Self {
        let mut route = Self {
            schema_keys: None,
            constraints: Vec::new(),
            global: None,
            untracked: None,
            contradictory: false,
        };

        for filter in &request.filters {
            match filter {
                StateSurfaceFilter::Eq(StateSurfaceColumn::SchemaKey, value) => {
                    constrain_string_set(
                        &mut route.schema_keys,
                        std::slice::from_ref(value),
                        &mut route.contradictory,
                    );
                }
                StateSurfaceFilter::In(StateSurfaceColumn::SchemaKey, values) => {
                    constrain_string_set(&mut route.schema_keys, values, &mut route.contradictory);
                }
                StateSurfaceFilter::Eq(StateSurfaceColumn::EntityId, value) => {
                    push_constraint(
                        &mut route.constraints,
                        ScanField::EntityId,
                        ScanOperator::Eq(value.clone()),
                    );
                }
                StateSurfaceFilter::In(StateSurfaceColumn::EntityId, values) => {
                    push_constraint(
                        &mut route.constraints,
                        ScanField::EntityId,
                        ScanOperator::In(values.clone()),
                    );
                }
                StateSurfaceFilter::Eq(StateSurfaceColumn::FileId, value) => {
                    push_constraint(
                        &mut route.constraints,
                        ScanField::FileId,
                        ScanOperator::Eq(value.clone()),
                    );
                }
                StateSurfaceFilter::In(StateSurfaceColumn::FileId, values) => {
                    push_constraint(
                        &mut route.constraints,
                        ScanField::FileId,
                        ScanOperator::In(values.clone()),
                    );
                }
                StateSurfaceFilter::Eq(StateSurfaceColumn::Global, Value::Boolean(value)) => {
                    constrain_boolean(&mut route.global, *value, &mut route.contradictory);
                }
                StateSurfaceFilter::In(StateSurfaceColumn::Global, values) => {
                    constrain_boolean_set(&mut route.global, values, &mut route.contradictory);
                }
                StateSurfaceFilter::Eq(StateSurfaceColumn::Untracked, Value::Boolean(value)) => {
                    constrain_boolean(&mut route.untracked, *value, &mut route.contradictory);
                }
                StateSurfaceFilter::In(StateSurfaceColumn::Untracked, values) => {
                    constrain_boolean_set(&mut route.untracked, values, &mut route.contradictory);
                }
                StateSurfaceFilter::Eq(StateSurfaceColumn::VersionId, Value::Text(filtered)) => {
                    if filtered != version_id {
                        route.contradictory = true;
                    }
                }
                StateSurfaceFilter::In(StateSurfaceColumn::VersionId, values) => {
                    let matches_version = values.iter().any(|value| match value {
                        Value::Text(filtered) => filtered == version_id,
                        _ => false,
                    });
                    if !matches_version {
                        route.contradictory = true;
                    }
                }
                _ => {}
            }
        }

        route
    }

    fn lanes(&self, version_id: &str) -> Vec<StateSurfaceLane> {
        if self.contradictory {
            return Vec::new();
        }

        if version_id == GLOBAL_VERSION_ID {
            return match self.untracked {
                Some(true) => vec![StateSurfaceLane::LocalUntracked],
                Some(false) => match self.global {
                    Some(false) => Vec::new(),
                    _ => vec![StateSurfaceLane::LocalTracked],
                },
                None => match self.global {
                    Some(false) => Vec::new(),
                    _ => vec![
                        StateSurfaceLane::LocalUntracked,
                        StateSurfaceLane::LocalTracked,
                    ],
                },
            };
        }

        match (self.global, self.untracked) {
            (Some(false), Some(false)) => vec![StateSurfaceLane::LocalTracked],
            (Some(false), Some(true)) => vec![StateSurfaceLane::LocalUntracked],
            (Some(true), Some(false)) => vec![StateSurfaceLane::GlobalTracked],
            (Some(true), Some(true)) => vec![StateSurfaceLane::GlobalUntracked],
            (Some(false), None) => {
                vec![
                    StateSurfaceLane::LocalUntracked,
                    StateSurfaceLane::LocalTracked,
                ]
            }
            (Some(true), None) => {
                vec![
                    StateSurfaceLane::GlobalUntracked,
                    StateSurfaceLane::GlobalTracked,
                ]
            }
            (None, Some(false)) => {
                vec![
                    StateSurfaceLane::LocalTracked,
                    StateSurfaceLane::GlobalTracked,
                ]
            }
            (None, Some(true)) => {
                vec![
                    StateSurfaceLane::LocalUntracked,
                    StateSurfaceLane::GlobalUntracked,
                ]
            }
            (None, None) => vec![
                StateSurfaceLane::LocalUntracked,
                StateSurfaceLane::LocalTracked,
                StateSurfaceLane::GlobalUntracked,
                StateSurfaceLane::GlobalTracked,
            ],
        }
    }
}

fn push_constraint(
    constraints: &mut Vec<ScanConstraint>,
    field: ScanField,
    operator: ScanOperator,
) {
    constraints.push(ScanConstraint { field, operator });
}

fn constrain_string_set(
    slot: &mut Option<Vec<String>>,
    values: &[Value],
    contradictory: &mut bool,
) {
    let next = values
        .iter()
        .filter_map(|value| match value {
            Value::Text(value) => Some(value.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();

    if next.is_empty() {
        *contradictory = true;
        return;
    }

    match slot {
        Some(existing) => {
            let existing_set = existing.iter().cloned().collect::<BTreeSet<_>>();
            let intersected = existing_set
                .intersection(&next)
                .cloned()
                .collect::<Vec<_>>();
            if intersected.is_empty() {
                *contradictory = true;
            } else {
                *existing = intersected;
            }
        }
        None => *slot = Some(next.into_iter().collect()),
    }
}

fn constrain_boolean(slot: &mut Option<bool>, value: bool, contradictory: &mut bool) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

fn constrain_boolean_set(slot: &mut Option<bool>, values: &[Value], contradictory: &mut bool) {
    let mut saw_true = false;
    let mut saw_false = false;
    for value in values {
        match value {
            Value::Boolean(true) => saw_true = true,
            Value::Boolean(false) => saw_false = true,
            _ => {}
        }
    }

    match (saw_true, saw_false) {
        (true, true) => {}
        (true, false) => constrain_boolean(slot, true, contradictory),
        (false, true) => constrain_boolean(slot, false, contradictory),
        (false, false) => *contradictory = true,
    }
}

async fn resolve_state_schema_keys(
    backend: &dyn LixBackend,
    version_id: &str,
    route: &StateSurfaceSourceRoute,
) -> Result<Vec<String>, LixError> {
    match &route.schema_keys {
        Some(schema_keys) => Ok(schema_keys.clone()),
        None => {
            let mut schema_keys = load_visible_state_schema_keys(backend, version_id).await?;
            schema_keys.extend(commit_family_state_schema_keys());
            schema_keys.sort();
            schema_keys.dedup();
            Ok(schema_keys)
        }
    }
}

fn commit_family_state_schema_keys() -> Vec<String> {
    vec![
        "lix_commit".to_string(),
        "lix_change_set".to_string(),
        "lix_change_set_element".to_string(),
        "lix_commit_edge".to_string(),
        "lix_change_author".to_string(),
    ]
}

async fn load_effective_rows_for_schema(
    backend: &dyn LixBackend,
    version_id: &str,
    schema_key: &str,
    route: &StateSurfaceSourceRoute,
    request: &StateByVersionScanRequest,
    source_limit: Option<usize>,
) -> Result<Vec<LiveRow>, LixError> {
    let mut resolved = BTreeMap::<(String, Option<String>), LiveRow>::new();
    let mut hidden = BTreeSet::<(String, Option<String>)>::new();
    let snapshot_shape = if request_needs_snapshot_content(request) {
        Some(load_live_row_shape_with_backend(backend, schema_key).await?)
    } else {
        None
    };
    let required_columns = required_columns_for_request(request);

    for lane in route.lanes(version_id) {
        let lane_rows = load_effective_rows_for_lane(
            backend,
            &snapshot_shape,
            schema_key,
            &required_columns,
            lane.is_global(),
            &LiveRowQuery {
                schema_key: schema_key.to_string(),
                version_id: lane.version_id(version_id),
                source: lane.source(),
                constraints: route.constraints.clone(),
                include_tombstones: lane.include_tombstones(),
            },
            source_limit.filter(|_| {
                matches!(
                    lane,
                    StateSurfaceLane::LocalUntracked | StateSurfaceLane::GlobalUntracked
                )
            }),
        )
        .await?;

        for lane_row in lane_rows {
            let key = (lane_row.row.entity_id.clone(), lane_row.row.file_id.clone());
            if resolved.contains_key(&key) || hidden.contains(&key) {
                continue;
            }

            if lane_row.is_tombstone {
                hidden.insert(key);
            } else {
                resolved.insert(key, lane_row.row);
            }
        }
    }

    Ok(resolved.into_values().collect())
}

#[derive(Debug)]
struct StateSurfaceLaneRow {
    row: LiveRow,
    is_tombstone: bool,
}

fn request_needs_snapshot_content(request: &StateByVersionScanRequest) -> bool {
    request.projection.is_empty()
        || request
            .projection
            .contains(&StateSurfaceColumn::SnapshotContent)
}

fn required_columns_for_request(request: &StateByVersionScanRequest) -> Vec<String> {
    if request_needs_snapshot_content(request) {
        Vec::new()
    } else {
        no_live_columns()
    }
}

fn source_limit_for_request(
    request: &StateByVersionScanRequest,
    route: &StateSurfaceSourceRoute,
    schema_key_count: usize,
    version_id: &str,
) -> Option<usize> {
    let limit = request.limit?;
    if schema_key_count != 1 {
        return None;
    }

    let lanes = route.lanes(version_id);
    if lanes.len() != 1 {
        return None;
    }

    match lanes[0] {
        StateSurfaceLane::LocalUntracked | StateSurfaceLane::GlobalUntracked => Some(limit),
        StateSurfaceLane::LocalTracked | StateSurfaceLane::GlobalTracked => None,
    }
}

async fn load_effective_rows_for_lane(
    backend: &dyn LixBackend,
    snapshot_shape: &Option<super::LiveRowShape>,
    schema_key: &str,
    required_columns: &[String],
    force_global: bool,
    query: &LiveRowQuery,
    source_limit: Option<usize>,
) -> Result<Vec<StateSurfaceLaneRow>, LixError> {
    if is_lazy_commit_derived_surface(schema_key) {
        return load_lazy_commit_derived_rows_for_lane(
            backend,
            force_global,
            query,
            snapshot_shape.is_some(),
        )
        .await;
    }

    match query.source {
        LiveRowSource::Tracked => {
            load_tracked_rows_for_lane(
                backend,
                snapshot_shape,
                schema_key,
                required_columns,
                force_global,
                query,
            )
            .await
        }
        LiveRowSource::Untracked => {
            load_untracked_rows_for_lane(
                backend,
                snapshot_shape,
                schema_key,
                required_columns,
                force_global,
                query,
                source_limit,
            )
            .await
        }
        LiveRowSource::Effective => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state_surface lane loading does not support nested effective queries",
        )),
    }
}

async fn load_lazy_commit_derived_rows_for_lane(
    backend: &dyn LixBackend,
    force_global: bool,
    query: &LiveRowQuery,
    include_snapshot_content: bool,
) -> Result<Vec<StateSurfaceLaneRow>, LixError> {
    let rows = scan_commit_derived_rows(backend, query, |backend, request| {
        let request = request.clone();
        Box::pin(async move { scan_live_rows(backend, &request).await })
    })
    .await?;

    Ok(rows
        .into_iter()
        .map(|mut row| {
            row.global = force_global || row.global;
            if !include_snapshot_content {
                row.snapshot_content = None;
            }
            StateSurfaceLaneRow {
                row,
                is_tombstone: false,
            }
        })
        .collect())
}

async fn load_tracked_rows_for_lane(
    backend: &dyn LixBackend,
    snapshot_shape: &Option<super::LiveRowShape>,
    schema_key: &str,
    required_columns: &[String],
    force_global: bool,
    query: &LiveRowQuery,
) -> Result<Vec<StateSurfaceLaneRow>, LixError> {
    let mut rows = scan_tracked_rows_with_backend(
        backend,
        &TrackedScanRequest {
            schema_key: schema_key.to_string(),
            version_id: query.version_id.clone(),
            constraints: query.constraints.clone(),
            required_columns: required_columns.to_vec(),
        },
    )
    .await?
    .into_iter()
    .map(|row| tracked_row_to_live_row(row, snapshot_shape, force_global))
    .collect::<Result<Vec<_>, _>>()?;

    if query.include_tombstones {
        let tombstones = scan_tracked_tombstones_with_backend(
            backend,
            &TrackedScanRequest {
                schema_key: schema_key.to_string(),
                version_id: query.version_id.clone(),
                constraints: query.constraints.clone(),
                required_columns: Vec::new(),
            },
        )
        .await?;
        rows.extend(
            tombstones
                .into_iter()
                .map(|row| tracked_tombstone_to_live_row(row, force_global)),
        );
    }

    Ok(rows)
}

async fn load_untracked_rows_for_lane(
    backend: &dyn LixBackend,
    snapshot_shape: &Option<super::LiveRowShape>,
    schema_key: &str,
    required_columns: &[String],
    force_global: bool,
    query: &LiveRowQuery,
    source_limit: Option<usize>,
) -> Result<Vec<StateSurfaceLaneRow>, LixError> {
    let request = UntrackedScanRequest {
        schema_key: schema_key.to_string(),
        version_id: query.version_id.clone(),
        constraints: query.constraints.clone(),
        required_columns: required_columns.to_vec(),
    };
    let rows = match source_limit {
        Some(limit) => scan_untracked_rows_with_backend_limit(backend, &request, limit).await?,
        None => scan_untracked_rows_with_backend(backend, &request).await?,
    };

    rows.into_iter()
        .map(|row| untracked_row_to_live_row(row, snapshot_shape, force_global))
        .collect()
}

fn tracked_row_to_live_row(
    row: TrackedRow,
    snapshot_shape: &Option<super::LiveRowShape>,
    force_global: bool,
) -> Result<StateSurfaceLaneRow, LixError> {
    Ok(StateSurfaceLaneRow {
        row: LiveRow {
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version: row.schema_version,
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            change_id: row.change_id,
            global: force_global || row.global,
            untracked: false,
            created_at: Some(row.created_at),
            updated_at: Some(row.updated_at),
            snapshot_content: snapshot_shape
                .as_ref()
                .map(|shape| shape.snapshot_text_from_values(&row.schema_key, &row.values))
                .transpose()?,
        },
        is_tombstone: false,
    })
}

fn untracked_row_to_live_row(
    row: UntrackedRow,
    snapshot_shape: &Option<super::LiveRowShape>,
    force_global: bool,
) -> Result<StateSurfaceLaneRow, LixError> {
    Ok(StateSurfaceLaneRow {
        row: LiveRow {
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version: row.schema_version,
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            change_id: Some(row.change_id),
            global: force_global || row.global,
            untracked: true,
            created_at: Some(row.created_at),
            updated_at: Some(row.updated_at),
            snapshot_content: snapshot_shape
                .as_ref()
                .map(|shape| shape.snapshot_text_from_values(&row.schema_key, &row.values))
                .transpose()?,
        },
        is_tombstone: false,
    })
}

fn tracked_tombstone_to_live_row(
    row: TrackedTombstoneMarker,
    force_global: bool,
) -> StateSurfaceLaneRow {
    StateSurfaceLaneRow {
        row: LiveRow {
            entity_id: row.entity_id,
            file_id: row.file_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version.unwrap_or_default(),
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            change_id: row.change_id,
            global: force_global || row.global,
            untracked: false,
            created_at: row.created_at,
            updated_at: row.updated_at,
            snapshot_content: None,
        },
        is_tombstone: true,
    }
}

async fn load_visible_state_schema_keys(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Vec<String>, LixError> {
    let mut schema_keys = load_live_storage_schema_keys(backend).await?;
    let rows = scan_live_rows(
        backend,
        &LiveRowQuery {
            schema_key: "lix_registered_schema".to_string(),
            version_id: version_id.to_string(),
            source: LiveRowSource::Effective,
            constraints: Vec::new(),
            include_tombstones: false,
        },
    )
    .await?;

    for row in &rows {
        let Some((schema_key, _)) = decode_registered_schema_row(row)? else {
            continue;
        };
        schema_keys.insert(schema_key.schema_key);
    }
    Ok(schema_keys.into_iter().collect())
}

async fn load_live_storage_schema_keys(
    backend: &dyn LixBackend,
) -> Result<BTreeSet<String>, LixError> {
    let rows = match backend.dialect() {
        crate::SqlDialect::Sqlite => {
            crate::live_state::store_sql::execute_query_with_backend(
                backend,
                "SELECT name \
                 FROM sqlite_master \
                 WHERE type IN ('table', 'view') \
                   AND name LIKE $1",
                &[Value::Text(format!("{}%", super::TRACKED_RELATION_PREFIX))],
            )
            .await?
            .rows
        }
        crate::SqlDialect::Postgres => {
            crate::live_state::store_sql::execute_query_with_backend(
                backend,
                "SELECT table_name \
                 FROM information_schema.tables \
                 WHERE table_name LIKE $1",
                &[Value::Text(format!("{}%", super::TRACKED_RELATION_PREFIX))],
            )
            .await?
            .rows
        }
    };

    let mut schema_keys = BTreeSet::new();
    for row in rows {
        let Some(Value::Text(table_name)) = row.first() else {
            continue;
        };
        let Some(schema_key) = table_name.strip_prefix(super::TRACKED_RELATION_PREFIX) else {
            continue;
        };
        if !schema_key.is_empty() {
            schema_keys.insert(schema_key.to_string());
        }
    }
    Ok(schema_keys)
}

async fn load_change_commit_ids_with_backend(
    backend: &dyn LixBackend,
    rows: &[LiveRow],
) -> Result<BTreeMap<String, String>, LixError> {
    let change_ids = rows
        .iter()
        .filter_map(|row| row.change_id.as_ref())
        .filter(|change_id| !change_id.trim().is_empty())
        .cloned()
        .collect::<BTreeSet<_>>();

    if change_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let in_list = change_ids
        .iter()
        .map(|change_id| format!("'{}'", escape_sql_string(change_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH {change_commit_cte} \
         SELECT change_id, commit_id \
         FROM change_commit_by_change_id \
         WHERE change_id IN ({in_list})",
        change_commit_cte =
            crate::canonical::build_lazy_change_commit_by_change_id_ctes_sql(backend.dialect(),),
        in_list = in_list,
    );
    let result =
        crate::live_state::store_sql::execute_query_with_backend(backend, &sql, &[]).await?;
    let mut rows = BTreeMap::new();
    for row in result.rows {
        let Some(Value::Text(change_id)) = row.first() else {
            continue;
        };
        let Some(Value::Text(commit_id)) = row.get(1) else {
            continue;
        };
        rows.insert(change_id.clone(), commit_id.clone());
    }
    Ok(rows)
}
#[derive(Clone)]
struct BackendBackedStateByVersion {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

impl std::fmt::Debug for BackendBackedStateByVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendBackedStateByVersion").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        open_state_by_version_snapshot, StateByVersionScanRequest, StateSurfaceColumn,
        StateSurfaceFilter,
    };
    use crate::live_state::{
        scan_live_rows, write_live_rows, LiveRow, LiveRowQuery, LiveRowSource,
    };
    use crate::schema::LixCommit;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, CanonicalChangeSeed, TestSqliteBackend,
    };
    use crate::Value;
    use datafusion::arrow::array::StringArray;

    fn commit_live_row(snapshot: &LixCommit, version_id: &str) -> LiveRow {
        LiveRow {
            entity_id: snapshot.id.clone(),
            file_id: None,
            schema_key: "lix_commit".to_string(),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            plugin_key: None,
            metadata: Some("{\"kind\":\"commit\"}".to_string()),
            change_id: Some(format!("change-{}", snapshot.id)),
            global: version_id == crate::version::GLOBAL_VERSION_ID,
            untracked: false,
            created_at: Some("2026-03-30T00:00:00Z".to_string()),
            updated_at: Some("2026-03-30T00:00:00Z".to_string()),
            snapshot_content: Some(
                serde_json::to_string(snapshot).expect("commit snapshot should serialize"),
            ),
        }
    }

    #[tokio::test]
    async fn state_by_version_snapshot_exposes_lazy_commit_derived_rows() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-1",
                entity_id: "entity-a",
                schema_key: "test_schema",
                schema_version: "1",
                file_id: Some("file-a"),
                plugin_key: None,
                snapshot_id: "snapshot-1",
                snapshot_content: Some(r#"{"key":"a"}"#),
                metadata: Some(r#"{"member":true}"#),
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await
        .expect("canonical member change should seed");

        let mut transaction = backend
            .begin_write_transaction()
            .await
            .expect("write transaction should open");
        write_live_rows(
            transaction.as_mut(),
            &[commit_live_row(
                &LixCommit {
                    id: "commit-1".to_string(),
                    change_set_id: Some("cs-1".to_string()),
                    change_ids: vec!["change-1".to_string()],
                    author_account_ids: vec![],
                    parent_commit_ids: vec![],
                },
                "main",
            )],
        )
        .await
        .expect("commit live row should write");
        transaction
            .commit()
            .await
            .expect("write transaction should commit");

        let direct_rows = scan_live_rows(
            &backend,
            &LiveRowQuery {
                schema_key: "lix_change_set_element".to_string(),
                version_id: "main".to_string(),
                source: LiveRowSource::Effective,
                constraints: vec![crate::live_state::ScanConstraint {
                    field: crate::live_state::ScanField::EntityId,
                    operator: crate::live_state::ScanOperator::Eq(Value::Text(
                        "cs-1~change-1".to_string(),
                    )),
                }],
                include_tombstones: false,
            },
        )
        .await
        .expect("direct lazy scan should succeed");
        assert_eq!(direct_rows.len(), 1);

        let snapshot = open_state_by_version_snapshot(&backend, "main")
            .await
            .expect("snapshot should open");
        let batches = snapshot
            .scan_state_by_version_batches(&StateByVersionScanRequest {
                version_id: Some("main".to_string()),
                projection: vec![
                    StateSurfaceColumn::EntityId,
                    StateSurfaceColumn::SchemaKey,
                    StateSurfaceColumn::SnapshotContent,
                ],
                filters: vec![
                    StateSurfaceFilter::Eq(
                        StateSurfaceColumn::SchemaKey,
                        Value::Text("lix_change_set_element".to_string()),
                    ),
                    StateSurfaceFilter::Eq(
                        StateSurfaceColumn::EntityId,
                        Value::Text("cs-1~change-1".to_string()),
                    ),
                ],
                limit: None,
            })
            .await
            .expect("state_by_version scan should succeed");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let entity_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("entity_id should be a string array");
        let schema_keys = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema_key should be a string array");
        let snapshot_contents = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("snapshot_content should be a string array");

        assert_eq!(entity_ids.value(0), "cs-1~change-1");
        assert_eq!(schema_keys.value(0), "lix_change_set_element");
        let snapshot: serde_json::Value =
            serde_json::from_str(snapshot_contents.value(0)).expect("valid derived snapshot JSON");
        assert_eq!(snapshot["change_set_id"], "cs-1");
        assert_eq!(snapshot["change_id"], "change-1");
    }
}
