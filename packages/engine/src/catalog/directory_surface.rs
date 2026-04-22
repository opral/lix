use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::backend::TransactionBeginMode;
use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, FilesystemProjectionScope,
    FilesystemRelationKind,
};
use crate::common::escape_sql_string;
use crate::sql::lower_catalog_relation_binding_to_source_sql;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectorySurfaceColumn {
    Id,
    ParentId,
    Name,
    Path,
    Hidden,
    LixcolEntityId,
    LixcolSchemaKey,
    LixcolVersionId,
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
pub enum DirectorySurfaceFilter {
    Eq(DirectorySurfaceColumn, Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectorySurfaceRow {
    pub id: String,
    pub parent_id: Option<String>,
    pub name: String,
    pub path: Option<String>,
    pub hidden: bool,
    pub lixcol_entity_id: String,
    pub lixcol_schema_key: String,
    pub lixcol_version_id: String,
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
pub struct DirectorySurfaceScanRequest {
    pub projection: Vec<DirectorySurfaceColumn>,
    pub filters: Vec<DirectorySurfaceFilter>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait DirectorySurfaceSnapshot: Debug + Send + Sync {
    async fn scan_directories(
        &self,
        request: &DirectorySurfaceScanRequest,
    ) -> Result<Vec<DirectorySurfaceRow>, LixError>;
}

pub async fn open_directory_surface_snapshot(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<Arc<dyn DirectorySurfaceSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedDirectorySurface::load(
            backend,
            DirectorySurfaceScope::ActiveVersion {
                active_version_id: active_version_id.to_string(),
            },
        )
        .await?,
    ))
}

pub async fn open_directory_by_version_surface_snapshot(
    backend: &dyn LixBackend,
) -> Result<Arc<dyn DirectorySurfaceSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedDirectorySurface::load(backend, DirectorySurfaceScope::ExplicitVersion)
            .await?,
    ))
}

pub async fn open_directory_by_version_surface_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<Arc<dyn DirectorySurfaceSnapshot>, LixError> {
    open_directory_surface_snapshot_with_shared_backend_and_scope(
        backend,
        DirectorySurfaceScope::ExplicitVersion,
    )
    .await
}

async fn open_directory_surface_snapshot_with_shared_backend_and_scope(
    backend: Arc<dyn LixBackend + Send + Sync>,
    scope: DirectorySurfaceScope,
) -> Result<Arc<dyn DirectorySurfaceSnapshot>, LixError> {
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedDirectorySurfaceCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("directory-surface-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("directory surface runtime should build");
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
                    TransactionBackedDirectorySurfaceCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            load_directory_surface_rows(&backend, &scope, &request).await
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
                format!("failed to spawn directory surface snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "directory surface snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedDirectorySurface {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedDirectorySurface {
    scope: DirectorySurfaceScope,
    rows: Vec<DirectorySurfaceRow>,
}

impl SnapshotBackedDirectorySurface {
    async fn load(
        backend: &dyn LixBackend,
        scope: DirectorySurfaceScope,
    ) -> Result<Self, LixError> {
        let rows = load_directory_surface_rows(
            backend,
            &scope,
            &DirectorySurfaceScanRequest {
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
enum DirectorySurfaceScope {
    ActiveVersion { active_version_id: String },
    ExplicitVersion,
}

#[derive(Debug)]
enum TransactionBackedDirectorySurfaceCommand {
    Scan {
        request: DirectorySurfaceScanRequest,
        reply: oneshot::Sender<Result<Vec<DirectorySurfaceRow>, LixError>>,
    },
}

struct TransactionBackedDirectorySurface {
    commands: mpsc::Sender<TransactionBackedDirectorySurfaceCommand>,
}

impl std::fmt::Debug for TransactionBackedDirectorySurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedDirectorySurface").finish()
    }
}

#[async_trait(?Send)]
impl DirectorySurfaceSnapshot for SnapshotBackedDirectorySurface {
    async fn scan_directories(
        &self,
        request: &DirectorySurfaceScanRequest,
    ) -> Result<Vec<DirectorySurfaceRow>, LixError> {
        let _ = &self.scope;
        let mut rows = self.rows.clone();
        apply_directory_surface_filters(&mut rows, &request.filters);
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl DirectorySurfaceSnapshot for TransactionBackedDirectorySurface {
    async fn scan_directories(
        &self,
        request: &DirectorySurfaceScanRequest,
    ) -> Result<Vec<DirectorySurfaceRow>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedDirectorySurfaceCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue directory surface scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "directory surface snapshot worker dropped scan reply",
            )
        })?
    }
}

#[derive(Debug, Clone, Default)]
struct DirectorySurfaceRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl DirectorySurfaceRoute {
    fn from_filters(filters: &[DirectorySurfaceFilter]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            match filter {
                DirectorySurfaceFilter::Eq(DirectorySurfaceColumn::Id, Value::Text(value)) => {
                    assign_route_slot(&mut route.id, value.clone(), &mut route.contradictory);
                }
                DirectorySurfaceFilter::Eq(DirectorySurfaceColumn::Path, Value::Text(value)) => {
                    assign_route_slot(&mut route.path, value.clone(), &mut route.contradictory);
                }
                DirectorySurfaceFilter::Eq(
                    DirectorySurfaceColumn::LixcolVersionId,
                    Value::Text(value),
                ) => {
                    assign_route_slot(
                        &mut route.lixcol_version_id,
                        value.clone(),
                        &mut route.contradictory,
                    );
                }
                DirectorySurfaceFilter::Eq(
                    DirectorySurfaceColumn::Hidden,
                    Value::Boolean(value),
                ) => {
                    assign_route_slot(&mut route.hidden, *value, &mut route.contradictory);
                }
                DirectorySurfaceFilter::Eq(
                    DirectorySurfaceColumn::LixcolGlobal,
                    Value::Boolean(value),
                ) => {
                    assign_route_slot(&mut route.lixcol_global, *value, &mut route.contradictory);
                }
                DirectorySurfaceFilter::Eq(
                    DirectorySurfaceColumn::LixcolUntracked,
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

async fn load_directory_surface_rows(
    backend: &dyn LixBackend,
    scope: &DirectorySurfaceScope,
    request: &DirectorySurfaceScanRequest,
) -> Result<Vec<DirectorySurfaceRow>, LixError> {
    let route = DirectorySurfaceRoute::from_filters(&request.filters);
    if route.contradictory {
        return Ok(Vec::new());
    }

    let binding = builtin_catalog_compiler_facade().bind_filesystem_runtime_relation(
        FilesystemRelationKind::Directory,
        match scope {
            DirectorySurfaceScope::ActiveVersion { .. } => FilesystemProjectionScope::ActiveVersion,
            DirectorySurfaceScope::ExplicitVersion => FilesystemProjectionScope::ExplicitVersion,
        },
        match scope {
            DirectorySurfaceScope::ActiveVersion { active_version_id } => {
                Some(active_version_id.as_str())
            }
            DirectorySurfaceScope::ExplicitVersion => None,
        },
    )?;
    let projection_sql = lower_catalog_relation_binding_to_source_sql(backend.dialect(), &binding)?;
    let sql = build_directory_surface_scan_sql(&projection_sql, &route, request.limit);
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = result
        .rows
        .iter()
        .map(|row| directory_surface_row_from_values(row))
        .collect::<Result<Vec<_>, _>>()?;

    apply_directory_surface_filters(&mut rows, &request.filters);
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn build_directory_surface_scan_sql(
    projection_sql: &str,
    route: &DirectorySurfaceRoute,
    limit: Option<usize>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(id) = &route.id {
        predicates.push(format!("d.id = '{}'", escape_sql_string(id)));
    }
    if let Some(path) = &route.path {
        predicates.push(format!("d.path = '{}'", escape_sql_string(path)));
    }
    if let Some(version_id) = &route.lixcol_version_id {
        predicates.push(format!(
            "d.lixcol_version_id = '{}'",
            escape_sql_string(version_id)
        ));
    }
    if let Some(hidden) = route.hidden {
        predicates.push(format!(
            "d.hidden = {}",
            if hidden { "TRUE" } else { "FALSE" }
        ));
    }
    if let Some(global) = route.lixcol_global {
        predicates.push(format!(
            "d.lixcol_global = {}",
            if global { "TRUE" } else { "FALSE" }
        ));
    }
    if let Some(untracked) = route.lixcol_untracked {
        predicates.push(format!(
            "d.lixcol_untracked = {}",
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
            d.id, \
            d.parent_id, \
            d.name, \
            d.path, \
            d.hidden, \
            d.lixcol_entity_id, \
            d.lixcol_schema_key, \
            d.lixcol_version_id, \
            d.lixcol_schema_version, \
            d.lixcol_global, \
            d.lixcol_change_id, \
            d.lixcol_created_at, \
            d.lixcol_updated_at, \
            d.lixcol_commit_id, \
            d.lixcol_untracked, \
            d.lixcol_metadata \
         FROM ({projection_sql}) d \
         {where_sql} \
         ORDER BY d.path ASC, d.id ASC \
         {limit_sql}",
        projection_sql = projection_sql,
        where_sql = where_sql,
        limit_sql = limit_sql,
    )
}

fn apply_directory_surface_filters(
    rows: &mut Vec<DirectorySurfaceRow>,
    filters: &[DirectorySurfaceFilter],
) {
    if filters.is_empty() {
        return;
    }
    rows.retain(|row| {
        filters
            .iter()
            .all(|filter| matches_directory_filter(row, filter))
    });
}

fn matches_directory_filter(row: &DirectorySurfaceRow, filter: &DirectorySurfaceFilter) -> bool {
    match filter {
        DirectorySurfaceFilter::Eq(column, expected) => {
            directory_surface_column_value(row, *column).is_some_and(|actual| actual == *expected)
        }
    }
}

fn directory_surface_column_value(
    row: &DirectorySurfaceRow,
    column: DirectorySurfaceColumn,
) -> Option<Value> {
    match column {
        DirectorySurfaceColumn::Id => Some(Value::Text(row.id.clone())),
        DirectorySurfaceColumn::ParentId => Some(
            row.parent_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        DirectorySurfaceColumn::Name => Some(Value::Text(row.name.clone())),
        DirectorySurfaceColumn::Path => {
            Some(row.path.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        DirectorySurfaceColumn::Hidden => Some(Value::Boolean(row.hidden)),
        DirectorySurfaceColumn::LixcolEntityId => Some(Value::Text(row.lixcol_entity_id.clone())),
        DirectorySurfaceColumn::LixcolSchemaKey => Some(Value::Text(row.lixcol_schema_key.clone())),
        DirectorySurfaceColumn::LixcolVersionId => Some(Value::Text(row.lixcol_version_id.clone())),
        DirectorySurfaceColumn::LixcolSchemaVersion => {
            Some(Value::Text(row.lixcol_schema_version.clone()))
        }
        DirectorySurfaceColumn::LixcolGlobal => Some(Value::Boolean(row.lixcol_global)),
        DirectorySurfaceColumn::LixcolChangeId => Some(
            row.lixcol_change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        DirectorySurfaceColumn::LixcolCreatedAt => Some(
            row.lixcol_created_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        DirectorySurfaceColumn::LixcolUpdatedAt => Some(
            row.lixcol_updated_at
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        DirectorySurfaceColumn::LixcolCommitId => Some(
            row.lixcol_commit_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        DirectorySurfaceColumn::LixcolUntracked => Some(Value::Boolean(row.lixcol_untracked)),
        DirectorySurfaceColumn::LixcolMetadata => Some(
            row.lixcol_metadata
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
    }
}

fn directory_surface_row_from_values(row: &[Value]) -> Result<DirectorySurfaceRow, LixError> {
    Ok(DirectorySurfaceRow {
        id: required_text_value(row, 0, "id")?,
        parent_id: optional_text_value(row.get(1)),
        name: required_text_value(row, 2, "name")?,
        path: optional_text_value(row.get(3)),
        hidden: row.get(4).and_then(value_as_bool).unwrap_or(false),
        lixcol_entity_id: required_text_value(row, 5, "lixcol_entity_id")?,
        lixcol_schema_key: required_text_value(row, 6, "lixcol_schema_key")?,
        lixcol_version_id: required_text_value(row, 7, "lixcol_version_id")?,
        lixcol_schema_version: required_text_value(row, 8, "lixcol_schema_version")?,
        lixcol_global: row.get(9).and_then(value_as_bool).unwrap_or(false),
        lixcol_change_id: optional_text_value(row.get(10)),
        lixcol_created_at: optional_text_value(row.get(11)),
        lixcol_updated_at: optional_text_value(row.get(12)),
        lixcol_commit_id: optional_text_value(row.get(13)),
        lixcol_untracked: row.get(14).and_then(value_as_bool).unwrap_or(false),
        lixcol_metadata: optional_text_value(row.get(15)),
    })
}

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("directory surface expected text {column}, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("directory surface missing required {column}"),
        )),
    }
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Null) | None => None,
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        _ => None,
    }
}
