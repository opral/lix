use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, FilesystemProjectionScope,
    FilesystemRelationKind,
};
use crate::common::escape_sql_string;
use crate::sql::lower_catalog_relation_binding_to_source_sql;
use crate::{LixBackend, LixError, Value};

use crate::backend::TransactionBeginMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileSurfaceColumn {
    Id,
    DirectoryId,
    Name,
    Extension,
    Path,
    Data,
    Metadata,
    Hidden,
    LixcolEntityId,
    LixcolSchemaKey,
    LixcolFileId,
    LixcolVersionId,
    LixcolPluginKey,
    LixcolSchemaVersion,
    LixcolGlobal,
    LixcolChangeId,
    LixcolCreatedAt,
    LixcolUpdatedAt,
    LixcolCommitId,
    LixcolUntracked,
    LixcolMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FileSurfaceFilter {
    Eq(FileSurfaceColumn, Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSurfaceRow {
    pub id: String,
    pub directory_id: Option<String>,
    pub name: String,
    pub extension: Option<String>,
    pub path: Option<String>,
    pub data: Option<Vec<u8>>,
    pub metadata: Option<String>,
    pub hidden: bool,
    pub lixcol_entity_id: String,
    pub lixcol_schema_key: String,
    pub lixcol_file_id: Option<String>,
    pub lixcol_version_id: String,
    pub lixcol_plugin_key: Option<String>,
    pub lixcol_schema_version: String,
    pub lixcol_global: bool,
    pub lixcol_change_id: Option<String>,
    pub lixcol_created_at: Option<String>,
    pub lixcol_updated_at: Option<String>,
    pub lixcol_commit_id: Option<String>,
    pub lixcol_untracked: bool,
    pub lixcol_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileSurfaceScanRequest {
    pub projection: Vec<FileSurfaceColumn>,
    pub filters: Vec<FileSurfaceFilter>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait FileSurfaceSnapshot: Debug + Send + Sync {
    async fn scan_files(
        &self,
        request: &FileSurfaceScanRequest,
    ) -> Result<Vec<FileSurfaceRow>, LixError>;
}

pub async fn open_file_surface_snapshot(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<Arc<dyn FileSurfaceSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedFileSurface::load(
            backend,
            FileSurfaceScope::ActiveVersion {
                active_version_id: active_version_id.to_string(),
            },
        )
        .await?,
    ))
}

pub async fn open_file_by_version_surface_snapshot(
    backend: &dyn LixBackend,
) -> Result<Arc<dyn FileSurfaceSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedFileSurface::load(backend, FileSurfaceScope::ExplicitVersion).await?,
    ))
}

pub async fn open_file_by_version_surface_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<Arc<dyn FileSurfaceSnapshot>, LixError> {
    open_file_surface_snapshot_with_shared_backend_and_scope(
        backend,
        FileSurfaceScope::ExplicitVersion,
    )
    .await
}

async fn open_file_surface_snapshot_with_shared_backend_and_scope(
    backend: Arc<dyn LixBackend + Send + Sync>,
    scope: FileSurfaceScope,
) -> Result<Arc<dyn FileSurfaceSnapshot>, LixError> {
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedFileSurfaceCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("file-surface-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("file surface runtime should build");
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
                    TransactionBackedFileSurfaceCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            load_file_surface_rows(&backend, &scope, &request).await
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
                format!("failed to spawn file surface snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "file surface snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedFileSurface {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedFileSurface {
    scope: FileSurfaceScope,
    rows: Vec<FileSurfaceRow>,
}

impl SnapshotBackedFileSurface {
    async fn load(backend: &dyn LixBackend, scope: FileSurfaceScope) -> Result<Self, LixError> {
        let rows = load_file_surface_rows(
            backend,
            &scope,
            &FileSurfaceScanRequest {
                projection: Vec::new(),
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
        Ok(Self { scope, rows })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileSurfaceScope {
    ActiveVersion { active_version_id: String },
    ExplicitVersion,
}

#[derive(Debug)]
enum TransactionBackedFileSurfaceCommand {
    Scan {
        request: FileSurfaceScanRequest,
        reply: oneshot::Sender<Result<Vec<FileSurfaceRow>, LixError>>,
    },
}

struct TransactionBackedFileSurface {
    commands: mpsc::Sender<TransactionBackedFileSurfaceCommand>,
}

impl std::fmt::Debug for TransactionBackedFileSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedFileSurface").finish()
    }
}

#[async_trait(?Send)]
impl FileSurfaceSnapshot for SnapshotBackedFileSurface {
    async fn scan_files(
        &self,
        request: &FileSurfaceScanRequest,
    ) -> Result<Vec<FileSurfaceRow>, LixError> {
        let _ = &self.scope;
        let mut rows = self.rows.clone();
        apply_file_surface_filters(&mut rows, &request.filters);
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl FileSurfaceSnapshot for TransactionBackedFileSurface {
    async fn scan_files(
        &self,
        request: &FileSurfaceScanRequest,
    ) -> Result<Vec<FileSurfaceRow>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedFileSurfaceCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue file surface scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "file surface snapshot worker dropped scan reply",
            )
        })?
    }
}

#[derive(Debug, Clone, Default)]
struct FileSurfaceRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl FileSurfaceRoute {
    fn from_filters(filters: &[FileSurfaceFilter]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            match filter {
                FileSurfaceFilter::Eq(FileSurfaceColumn::Id, Value::Text(value)) => {
                    assign_route_slot(&mut route.id, value.clone(), &mut route.contradictory);
                }
                FileSurfaceFilter::Eq(FileSurfaceColumn::Path, Value::Text(value)) => {
                    assign_route_slot(&mut route.path, value.clone(), &mut route.contradictory);
                }
                FileSurfaceFilter::Eq(FileSurfaceColumn::LixcolVersionId, Value::Text(value)) => {
                    assign_route_slot(
                        &mut route.lixcol_version_id,
                        value.clone(),
                        &mut route.contradictory,
                    );
                }
                FileSurfaceFilter::Eq(FileSurfaceColumn::Hidden, Value::Boolean(value)) => {
                    assign_route_slot(&mut route.hidden, *value, &mut route.contradictory);
                }
                FileSurfaceFilter::Eq(FileSurfaceColumn::LixcolGlobal, Value::Boolean(value)) => {
                    assign_route_slot(&mut route.lixcol_global, *value, &mut route.contradictory);
                }
                FileSurfaceFilter::Eq(
                    FileSurfaceColumn::LixcolUntracked,
                    Value::Boolean(value),
                ) => {
                    assign_route_slot(
                        &mut route.lixcol_untracked,
                        *value,
                        &mut route.contradictory,
                    );
                }
                _ => {}
            }
        }
        route
    }
}

fn assign_route_slot<T: PartialEq>(slot: &mut Option<T>, value: T, contradictory: &mut bool) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

async fn load_file_surface_rows(
    backend: &dyn LixBackend,
    scope: &FileSurfaceScope,
    request: &FileSurfaceScanRequest,
) -> Result<Vec<FileSurfaceRow>, LixError> {
    let route = FileSurfaceRoute::from_filters(&request.filters);
    if route.contradictory {
        return Ok(Vec::new());
    }

    let binding = builtin_catalog_compiler_facade().bind_filesystem_runtime_relation(
        FilesystemRelationKind::File,
        match scope {
            FileSurfaceScope::ActiveVersion { .. } => FilesystemProjectionScope::ActiveVersion,
            FileSurfaceScope::ExplicitVersion => FilesystemProjectionScope::ExplicitVersion,
        },
        match scope {
            FileSurfaceScope::ActiveVersion { active_version_id } => {
                Some(active_version_id.as_str())
            }
            FileSurfaceScope::ExplicitVersion => None,
        },
    )?;
    let projection_sql = lower_catalog_relation_binding_to_source_sql(backend.dialect(), &binding)?;
    let sql = build_file_surface_scan_sql(&projection_sql, &route, request.limit);
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = result
        .rows
        .iter()
        .map(|row| file_surface_row_from_values(row))
        .collect::<Result<Vec<_>, _>>()?;

    apply_file_surface_filters(&mut rows, &request.filters);
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn build_file_surface_scan_sql(
    projection_sql: &str,
    route: &FileSurfaceRoute,
    limit: Option<usize>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(id) = &route.id {
        predicates.push(format!("f.id = '{}'", escape_sql_string(id)));
    }
    if let Some(path) = &route.path {
        predicates.push(format!("f.path = '{}'", escape_sql_string(path)));
    }
    if let Some(version_id) = &route.lixcol_version_id {
        predicates.push(format!(
            "f.lixcol_version_id = '{}'",
            escape_sql_string(version_id)
        ));
    }
    if let Some(hidden) = route.hidden {
        predicates.push(format!(
            "f.hidden = {}",
            if hidden { "TRUE" } else { "FALSE" }
        ));
    }
    if let Some(global) = route.lixcol_global {
        predicates.push(format!(
            "f.lixcol_global = {}",
            if global { "TRUE" } else { "FALSE" }
        ));
    }
    if let Some(untracked) = route.lixcol_untracked {
        predicates.push(format!(
            "f.lixcol_untracked = {}",
            if untracked { "TRUE" } else { "FALSE" }
        ));
    }
    let where_sql = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    let limit_sql = limit
        .map(|limit| format!(" LIMIT {limit}"))
        .unwrap_or_default();

    format!(
        "SELECT \
            f.id, \
            f.directory_id, \
            f.name, \
            f.extension, \
            f.path, \
            f.data, \
            f.metadata, \
            f.hidden, \
            f.lixcol_entity_id, \
            f.lixcol_schema_key, \
            f.lixcol_file_id, \
            f.lixcol_version_id, \
            f.lixcol_plugin_key, \
            f.lixcol_schema_version, \
            f.lixcol_global, \
            f.lixcol_change_id, \
            f.lixcol_created_at, \
            f.lixcol_updated_at, \
            f.lixcol_commit_id, \
            f.lixcol_untracked, \
            f.lixcol_metadata \
         FROM ({projection_sql}) f \
         {where_sql} \
         ORDER BY f.path ASC, f.id ASC \
         {limit_sql}",
        projection_sql = projection_sql,
        where_sql = where_sql,
        limit_sql = limit_sql,
    )
}

fn apply_file_surface_filters(rows: &mut Vec<FileSurfaceRow>, filters: &[FileSurfaceFilter]) {
    if filters.is_empty() {
        return;
    }
    rows.retain(|row| {
        filters
            .iter()
            .all(|filter| matches_file_filter(row, filter))
    });
}

fn matches_file_filter(row: &FileSurfaceRow, filter: &FileSurfaceFilter) -> bool {
    match filter {
        FileSurfaceFilter::Eq(column, expected) => {
            file_surface_column_value(row, *column).is_some_and(|actual| actual == *expected)
        }
    }
}

fn file_surface_column_value(row: &FileSurfaceRow, column: FileSurfaceColumn) -> Option<Value> {
    match column {
        FileSurfaceColumn::Id => Some(Value::Text(row.id.clone())),
        FileSurfaceColumn::DirectoryId => Some(
            row.directory_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::Name => Some(Value::Text(row.name.clone())),
        FileSurfaceColumn::Extension => Some(
            row.extension
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::Path => Some(row.path.clone().map(Value::Text).unwrap_or(Value::Null)),
        FileSurfaceColumn::Data => Some(row.data.clone().map(Value::Blob).unwrap_or(Value::Null)),
        FileSurfaceColumn::Metadata => {
            Some(row.metadata.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        FileSurfaceColumn::Hidden => Some(Value::Boolean(row.hidden)),
        FileSurfaceColumn::LixcolEntityId => Some(Value::Text(row.lixcol_entity_id.clone())),
        FileSurfaceColumn::LixcolSchemaKey => Some(Value::Text(row.lixcol_schema_key.clone())),
        FileSurfaceColumn::LixcolFileId => Some(
            row.lixcol_file_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolVersionId => Some(Value::Text(row.lixcol_version_id.clone())),
        FileSurfaceColumn::LixcolPluginKey => Some(
            row.lixcol_plugin_key
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolSchemaVersion => {
            Some(Value::Text(row.lixcol_schema_version.clone()))
        }
        FileSurfaceColumn::LixcolGlobal => Some(Value::Boolean(row.lixcol_global)),
        FileSurfaceColumn::LixcolChangeId => Some(
            row.lixcol_change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolCreatedAt => Some(
            row.lixcol_created_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolUpdatedAt => Some(
            row.lixcol_updated_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolCommitId => Some(
            row.lixcol_commit_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        FileSurfaceColumn::LixcolUntracked => Some(Value::Boolean(row.lixcol_untracked)),
        FileSurfaceColumn::LixcolMetadata => Some(
            row.lixcol_metadata
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
    }
}

fn file_surface_row_from_values(row: &[Value]) -> Result<FileSurfaceRow, LixError> {
    Ok(FileSurfaceRow {
        id: required_text_value(row, 0, "id")?,
        directory_id: optional_text_value(row.get(1)),
        name: required_text_value(row, 2, "name")?,
        extension: optional_text_value(row.get(3)),
        path: optional_text_value(row.get(4)),
        data: optional_blob_value(row.get(5)),
        metadata: optional_text_value(row.get(6)),
        hidden: row.get(7).and_then(value_as_bool).unwrap_or(false),
        lixcol_entity_id: required_text_value(row, 8, "lixcol_entity_id")?,
        lixcol_schema_key: required_text_value(row, 9, "lixcol_schema_key")?,
        lixcol_file_id: optional_text_value(row.get(10)),
        lixcol_version_id: required_text_value(row, 11, "lixcol_version_id")?,
        lixcol_plugin_key: optional_text_value(row.get(12)),
        lixcol_schema_version: required_text_value(row, 13, "lixcol_schema_version")?,
        lixcol_global: row.get(14).and_then(value_as_bool).unwrap_or(false),
        lixcol_change_id: optional_text_value(row.get(15)),
        lixcol_created_at: optional_text_value(row.get(16)),
        lixcol_updated_at: optional_text_value(row.get(17)),
        lixcol_commit_id: optional_text_value(row.get(18)),
        lixcol_untracked: row.get(19).and_then(value_as_bool).unwrap_or(false),
        lixcol_metadata: optional_text_value(row.get(20)),
    })
}

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("file surface expected text {column}, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("file surface missing column {column}"),
        )),
    }
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(value)) => Some(value.clone()),
        _ => None,
    }
}

fn optional_blob_value(value: Option<&Value>) -> Option<Vec<u8>> {
    match value {
        Some(Value::Blob(value)) => Some(value.clone()),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        _ => None,
    }
}
