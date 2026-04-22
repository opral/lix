use std::fmt::Debug;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::backend::TransactionBeginMode;
use crate::common::escape_sql_string;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeSurfaceColumn {
    Id,
    EntityId,
    SchemaKey,
    SchemaVersion,
    FileId,
    PluginKey,
    Metadata,
    CreatedAt,
    Untracked,
    SnapshotContent,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeSurfaceFilter {
    Eq(ChangeSurfaceColumn, Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSurfaceRow {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub plugin_key: Option<String>,
    pub metadata: Option<String>,
    pub created_at: String,
    pub untracked: bool,
    pub snapshot_content: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSurfaceScanRequest {
    pub projection: Vec<ChangeSurfaceColumn>,
    pub filters: Vec<ChangeSurfaceFilter>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait ChangeSurfaceSnapshot: Debug + Send + Sync {
    async fn scan_changes(
        &self,
        request: &ChangeSurfaceScanRequest,
    ) -> Result<Vec<ChangeSurfaceRow>, LixError>;
}

pub async fn open_change_surface_snapshot(
    backend: &dyn LixBackend,
) -> Result<Arc<dyn ChangeSurfaceSnapshot>, LixError> {
    Ok(Arc::new(SnapshotBackedChangeSurface::load(backend).await?))
}

pub async fn open_change_surface_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
) -> Result<Arc<dyn ChangeSurfaceSnapshot>, LixError> {
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedChangeSurfaceCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("change-surface-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("change surface runtime should build");
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
                    TransactionBackedChangeSurfaceCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            load_change_surface_rows(&backend, &request).await
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
                format!("failed to spawn change surface snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "change surface snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedChangeSurface {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedChangeSurface {
    rows: Vec<ChangeSurfaceRow>,
}

impl SnapshotBackedChangeSurface {
    async fn load(backend: &dyn LixBackend) -> Result<Self, LixError> {
        let rows = load_change_surface_rows(
            backend,
            &ChangeSurfaceScanRequest {
                projection: Vec::new(),
                filters: Vec::new(),
                limit: None,
            },
        )
        .await?;
        Ok(Self { rows })
    }
}

#[derive(Debug)]
enum TransactionBackedChangeSurfaceCommand {
    Scan {
        request: ChangeSurfaceScanRequest,
        reply: oneshot::Sender<Result<Vec<ChangeSurfaceRow>, LixError>>,
    },
}

struct TransactionBackedChangeSurface {
    commands: mpsc::Sender<TransactionBackedChangeSurfaceCommand>,
}

impl std::fmt::Debug for TransactionBackedChangeSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedChangeSurface").finish()
    }
}

#[async_trait(?Send)]
impl ChangeSurfaceSnapshot for SnapshotBackedChangeSurface {
    async fn scan_changes(
        &self,
        request: &ChangeSurfaceScanRequest,
    ) -> Result<Vec<ChangeSurfaceRow>, LixError> {
        let mut rows = self.rows.clone();
        apply_change_surface_filters(&mut rows, &request.filters);
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl ChangeSurfaceSnapshot for TransactionBackedChangeSurface {
    async fn scan_changes(
        &self,
        request: &ChangeSurfaceScanRequest,
    ) -> Result<Vec<ChangeSurfaceRow>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedChangeSurfaceCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue change surface scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "change surface snapshot worker dropped scan reply",
            )
        })?
    }
}

#[derive(Debug, Clone, Default)]
struct ChangeSurfaceRoute {
    id: Option<String>,
    entity_id: Option<String>,
    schema_key: Option<String>,
    file_id: Option<String>,
    plugin_key: Option<String>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl ChangeSurfaceRoute {
    fn from_filters(filters: &[ChangeSurfaceFilter]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            match filter {
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::Id, Value::Text(value)) => {
                    assign_route_slot(&mut route.id, value.clone(), &mut route.contradictory);
                }
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::EntityId, Value::Text(value)) => {
                    assign_route_slot(
                        &mut route.entity_id,
                        value.clone(),
                        &mut route.contradictory,
                    );
                }
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::SchemaKey, Value::Text(value)) => {
                    assign_route_slot(
                        &mut route.schema_key,
                        value.clone(),
                        &mut route.contradictory,
                    );
                }
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::FileId, Value::Text(value)) => {
                    assign_route_slot(&mut route.file_id, value.clone(), &mut route.contradictory);
                }
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::PluginKey, Value::Text(value)) => {
                    assign_route_slot(
                        &mut route.plugin_key,
                        value.clone(),
                        &mut route.contradictory,
                    );
                }
                ChangeSurfaceFilter::Eq(ChangeSurfaceColumn::Untracked, Value::Boolean(value)) => {
                    assign_route_slot(&mut route.untracked, *value, &mut route.contradictory);
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

async fn load_change_surface_rows(
    backend: &dyn LixBackend,
    request: &ChangeSurfaceScanRequest,
) -> Result<Vec<ChangeSurfaceRow>, LixError> {
    let route = ChangeSurfaceRoute::from_filters(&request.filters);
    if route.contradictory {
        return Ok(Vec::new());
    }

    let sql = build_change_surface_scan_sql(&route, request.limit);
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = result
        .rows
        .iter()
        .map(|row| change_surface_row_from_values(row))
        .collect::<Result<Vec<_>, _>>()?;
    apply_change_surface_filters(&mut rows, &request.filters);
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn build_change_surface_scan_sql(route: &ChangeSurfaceRoute, limit: Option<usize>) -> String {
    let mut predicates = Vec::new();
    if let Some(id) = &route.id {
        predicates.push(format!("ch.id = '{}'", escape_sql_string(id)));
    }
    if let Some(entity_id) = &route.entity_id {
        predicates.push(format!("ch.entity_id = '{}'", escape_sql_string(entity_id)));
    }
    if let Some(schema_key) = &route.schema_key {
        predicates.push(format!(
            "ch.schema_key = '{}'",
            escape_sql_string(schema_key)
        ));
    }
    if let Some(file_id) = &route.file_id {
        predicates.push(format!("ch.file_id = '{}'", escape_sql_string(file_id)));
    }
    if let Some(plugin_key) = &route.plugin_key {
        predicates.push(format!(
            "ch.plugin_key = '{}'",
            escape_sql_string(plugin_key)
        ));
    }
    if let Some(untracked) = route.untracked {
        predicates.push(format!(
            "EXISTS (SELECT 1 FROM lix_internal_untracked_change_visibility uv WHERE uv.change_id = ch.id) = {}",
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
            ch.id, \
            ch.entity_id, \
            ch.schema_key, \
            ch.schema_version, \
            ch.file_id, \
            ch.plugin_key, \
            ch.metadata, \
            ch.created_at, \
            EXISTS ( \
                SELECT 1 \
                FROM lix_internal_untracked_change_visibility uv \
                WHERE uv.change_id = ch.id \
            ) AS untracked, \
            CASE \
                WHEN ch.snapshot_id = 'no-content' THEN NULL \
                ELSE s.content \
            END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
           ON s.id = ch.snapshot_id \
         {where_sql} \
         ORDER BY ch.created_at ASC, ch.id ASC \
         {limit_sql}",
        where_sql = where_sql,
        limit_sql = limit_sql,
    )
}

fn apply_change_surface_filters(rows: &mut Vec<ChangeSurfaceRow>, filters: &[ChangeSurfaceFilter]) {
    if filters.is_empty() {
        return;
    }
    rows.retain(|row| {
        filters
            .iter()
            .all(|filter| matches_change_surface_filter(row, filter))
    });
}

fn matches_change_surface_filter(row: &ChangeSurfaceRow, filter: &ChangeSurfaceFilter) -> bool {
    match filter {
        ChangeSurfaceFilter::Eq(column, expected) => {
            change_surface_column_value(row, *column).is_some_and(|actual| actual == *expected)
        }
    }
}

fn change_surface_column_value(
    row: &ChangeSurfaceRow,
    column: ChangeSurfaceColumn,
) -> Option<Value> {
    match column {
        ChangeSurfaceColumn::Id => Some(Value::Text(row.id.clone())),
        ChangeSurfaceColumn::EntityId => Some(Value::Text(row.entity_id.clone())),
        ChangeSurfaceColumn::SchemaKey => Some(Value::Text(row.schema_key.clone())),
        ChangeSurfaceColumn::SchemaVersion => Some(Value::Text(row.schema_version.clone())),
        ChangeSurfaceColumn::FileId => {
            Some(row.file_id.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        ChangeSurfaceColumn::PluginKey => Some(
            row.plugin_key
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        ChangeSurfaceColumn::Metadata => {
            Some(row.metadata.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        ChangeSurfaceColumn::CreatedAt => Some(Value::Text(row.created_at.clone())),
        ChangeSurfaceColumn::Untracked => Some(Value::Boolean(row.untracked)),
        ChangeSurfaceColumn::SnapshotContent => Some(
            row.snapshot_content
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
    }
}

fn change_surface_row_from_values(row: &[Value]) -> Result<ChangeSurfaceRow, LixError> {
    Ok(ChangeSurfaceRow {
        id: required_text_value(row, 0, "id")?,
        entity_id: required_text_value(row, 1, "entity_id")?,
        schema_key: required_text_value(row, 2, "schema_key")?,
        schema_version: required_text_value(row, 3, "schema_version")?,
        file_id: optional_text_value(row.get(4)),
        plugin_key: optional_text_value(row.get(5)),
        metadata: optional_text_value(row.get(6)),
        created_at: required_text_value(row, 7, "created_at")?,
        untracked: row.get(8).and_then(value_as_bool).unwrap_or(false),
        snapshot_content: optional_text_value(row.get(9)),
    })
}

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    row.get(index).and_then(value_as_text).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing change surface {column}"),
        )
    })
}

fn optional_text_value(value: Option<&Value>) -> Option<String> {
    value.and_then(value_as_text)
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Blob(bytes) => String::from_utf8(bytes.clone()).ok(),
        Value::Null => None,
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(boolean) => Some(*boolean),
        Value::Integer(integer) => Some(*integer != 0),
        Value::Real(number) => Some(*number != 0.0),
        Value::Text(text) if text.eq_ignore_ascii_case("true") => Some(true),
        Value::Text(text) if text.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}
