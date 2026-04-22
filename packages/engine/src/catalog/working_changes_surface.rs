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
pub enum WorkingChangesSurfaceColumn {
    EntityId,
    SchemaKey,
    FileId,
    LixcolGlobal,
    BeforeChangeId,
    AfterChangeId,
    BeforeCommitId,
    AfterCommitId,
    Status,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WorkingChangesSurfaceFilter {
    Eq(WorkingChangesSurfaceColumn, Value),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingChangesSurfaceRow {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub lixcol_global: bool,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
    pub before_commit_id: Option<String>,
    pub after_commit_id: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkingChangesSurfaceScanRequest {
    pub projection: Vec<WorkingChangesSurfaceColumn>,
    pub filters: Vec<WorkingChangesSurfaceFilter>,
    pub limit: Option<usize>,
}

#[async_trait(?Send)]
pub trait WorkingChangesSurfaceSnapshot: Debug + Send + Sync {
    async fn scan_working_changes(
        &self,
        request: &WorkingChangesSurfaceScanRequest,
    ) -> Result<Vec<WorkingChangesSurfaceRow>, LixError>;
}

pub async fn open_working_changes_surface_snapshot(
    backend: &dyn LixBackend,
    active_version_id: &str,
) -> Result<Arc<dyn WorkingChangesSurfaceSnapshot>, LixError> {
    Ok(Arc::new(
        SnapshotBackedWorkingChangesSurface::load(backend, active_version_id).await?,
    ))
}

pub async fn open_working_changes_surface_snapshot_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    active_version_id: &str,
) -> Result<Arc<dyn WorkingChangesSurfaceSnapshot>, LixError> {
    let active_version_id = active_version_id.to_string();
    let (command_tx, command_rx) = mpsc::channel::<TransactionBackedWorkingChangesSurfaceCommand>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), LixError>>();
    thread::Builder::new()
        .name("working-changes-surface-query-snapshot".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("working changes surface runtime should build");
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
                    TransactionBackedWorkingChangesSurfaceCommand::Scan { request, reply } => {
                        let result = runtime.block_on(async {
                            let backend =
                                crate::backend::transaction_backend_view(transaction.as_mut());
                            load_working_changes_surface_rows(
                                &backend,
                                &active_version_id,
                                &request,
                            )
                            .await
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
                format!("failed to spawn working changes surface snapshot worker: {error}"),
            )
        })?;

    ready_rx.await.map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "working changes surface snapshot worker dropped initialization reply",
        )
    })??;

    Ok(Arc::new(TransactionBackedWorkingChangesSurface {
        commands: command_tx,
    }))
}

#[derive(Debug, Clone)]
struct SnapshotBackedWorkingChangesSurface {
    rows: Vec<WorkingChangesSurfaceRow>,
}

impl SnapshotBackedWorkingChangesSurface {
    async fn load(backend: &dyn LixBackend, active_version_id: &str) -> Result<Self, LixError> {
        let rows = load_working_changes_surface_rows(
            backend,
            active_version_id,
            &WorkingChangesSurfaceScanRequest {
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
enum TransactionBackedWorkingChangesSurfaceCommand {
    Scan {
        request: WorkingChangesSurfaceScanRequest,
        reply: oneshot::Sender<Result<Vec<WorkingChangesSurfaceRow>, LixError>>,
    },
}

struct TransactionBackedWorkingChangesSurface {
    commands: mpsc::Sender<TransactionBackedWorkingChangesSurfaceCommand>,
}

impl std::fmt::Debug for TransactionBackedWorkingChangesSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBackedWorkingChangesSurface")
            .finish()
    }
}

#[async_trait(?Send)]
impl WorkingChangesSurfaceSnapshot for SnapshotBackedWorkingChangesSurface {
    async fn scan_working_changes(
        &self,
        request: &WorkingChangesSurfaceScanRequest,
    ) -> Result<Vec<WorkingChangesSurfaceRow>, LixError> {
        let mut rows = self.rows.clone();
        apply_working_changes_surface_filters(&mut rows, &request.filters);
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait(?Send)]
impl WorkingChangesSurfaceSnapshot for TransactionBackedWorkingChangesSurface {
    async fn scan_working_changes(
        &self,
        request: &WorkingChangesSurfaceScanRequest,
    ) -> Result<Vec<WorkingChangesSurfaceRow>, LixError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(TransactionBackedWorkingChangesSurfaceCommand::Scan {
                request: request.clone(),
                reply: reply_tx,
            })
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to enqueue working changes surface scan: {error}"),
                )
            })?;
        reply_rx.await.map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "working changes surface snapshot worker dropped scan reply",
            )
        })?
    }
}

#[derive(Debug, Clone, Default)]
struct WorkingChangesSurfaceRoute {
    entity_id: Option<String>,
    schema_key: Option<String>,
    file_id: Option<String>,
    status: Option<String>,
    contradictory: bool,
}

impl WorkingChangesSurfaceRoute {
    fn from_filters(filters: &[WorkingChangesSurfaceFilter]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            match filter {
                WorkingChangesSurfaceFilter::Eq(
                    WorkingChangesSurfaceColumn::EntityId,
                    Value::Text(value),
                ) => assign_route_slot(
                    &mut route.entity_id,
                    value.clone(),
                    &mut route.contradictory,
                ),
                WorkingChangesSurfaceFilter::Eq(
                    WorkingChangesSurfaceColumn::SchemaKey,
                    Value::Text(value),
                ) => assign_route_slot(
                    &mut route.schema_key,
                    value.clone(),
                    &mut route.contradictory,
                ),
                WorkingChangesSurfaceFilter::Eq(
                    WorkingChangesSurfaceColumn::FileId,
                    Value::Text(value),
                ) => assign_route_slot(&mut route.file_id, value.clone(), &mut route.contradictory),
                WorkingChangesSurfaceFilter::Eq(
                    WorkingChangesSurfaceColumn::Status,
                    Value::Text(value),
                ) => assign_route_slot(&mut route.status, value.clone(), &mut route.contradictory),
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

async fn load_working_changes_surface_rows(
    backend: &dyn LixBackend,
    active_version_id: &str,
    request: &WorkingChangesSurfaceScanRequest,
) -> Result<Vec<WorkingChangesSurfaceRow>, LixError> {
    let route = WorkingChangesSurfaceRoute::from_filters(&request.filters);
    if route.contradictory {
        return Ok(Vec::new());
    }

    let projection_sql =
        crate::sql::physical_plan::source_sql::build_working_changes_public_read_source_sql(
            backend.dialect(),
            active_version_id,
        );
    let sql = build_working_changes_surface_scan_sql(&projection_sql, &route, request.limit);
    let result = backend.execute(&sql, &[]).await?;
    let mut rows = result
        .rows
        .iter()
        .map(|row| working_changes_surface_row_from_values(row))
        .collect::<Result<Vec<_>, _>>()?;
    apply_working_changes_surface_filters(&mut rows, &request.filters);
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn build_working_changes_surface_scan_sql(
    projection_sql: &str,
    route: &WorkingChangesSurfaceRoute,
    limit: Option<usize>,
) -> String {
    let mut predicates = Vec::new();
    if let Some(entity_id) = &route.entity_id {
        predicates.push(format!("wc.entity_id = '{}'", escape_sql_string(entity_id)));
    }
    if let Some(schema_key) = &route.schema_key {
        predicates.push(format!(
            "wc.schema_key = '{}'",
            escape_sql_string(schema_key)
        ));
    }
    if let Some(file_id) = &route.file_id {
        predicates.push(format!("wc.file_id = '{}'", escape_sql_string(file_id)));
    }
    if let Some(status) = &route.status {
        predicates.push(format!("wc.status = '{}'", escape_sql_string(status)));
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
            wc.entity_id, \
            wc.schema_key, \
            wc.file_id, \
            wc.lixcol_global, \
            wc.before_change_id, \
            wc.after_change_id, \
            wc.before_commit_id, \
            wc.after_commit_id, \
            wc.status \
         FROM ({projection_sql}) wc \
         {where_sql} \
         ORDER BY wc.entity_id ASC, wc.schema_key ASC, wc.file_id ASC \
         {limit_sql}",
        projection_sql = projection_sql,
        where_sql = where_sql,
        limit_sql = limit_sql,
    )
}

fn apply_working_changes_surface_filters(
    rows: &mut Vec<WorkingChangesSurfaceRow>,
    filters: &[WorkingChangesSurfaceFilter],
) {
    if filters.is_empty() {
        return;
    }
    rows.retain(|row| {
        filters
            .iter()
            .all(|filter| matches_working_changes_surface_filter(row, filter))
    });
}

fn matches_working_changes_surface_filter(
    row: &WorkingChangesSurfaceRow,
    filter: &WorkingChangesSurfaceFilter,
) -> bool {
    match filter {
        WorkingChangesSurfaceFilter::Eq(column, expected) => {
            working_changes_surface_column_value(row, *column)
                .is_some_and(|actual| actual == *expected)
        }
    }
}

fn working_changes_surface_column_value(
    row: &WorkingChangesSurfaceRow,
    column: WorkingChangesSurfaceColumn,
) -> Option<Value> {
    match column {
        WorkingChangesSurfaceColumn::EntityId => Some(Value::Text(row.entity_id.clone())),
        WorkingChangesSurfaceColumn::SchemaKey => Some(Value::Text(row.schema_key.clone())),
        WorkingChangesSurfaceColumn::FileId => {
            Some(row.file_id.clone().map(Value::Text).unwrap_or(Value::Null))
        }
        WorkingChangesSurfaceColumn::LixcolGlobal => Some(Value::Boolean(row.lixcol_global)),
        WorkingChangesSurfaceColumn::BeforeChangeId => Some(
            row.before_change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        WorkingChangesSurfaceColumn::AfterChangeId => Some(
            row.after_change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        WorkingChangesSurfaceColumn::BeforeCommitId => Some(
            row.before_commit_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        WorkingChangesSurfaceColumn::AfterCommitId => Some(
            row.after_commit_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        WorkingChangesSurfaceColumn::Status => Some(Value::Text(row.status.clone())),
    }
}

fn working_changes_surface_row_from_values(
    row: &[Value],
) -> Result<WorkingChangesSurfaceRow, LixError> {
    Ok(WorkingChangesSurfaceRow {
        entity_id: required_text_value(row, 0, "entity_id")?,
        schema_key: required_text_value(row, 1, "schema_key")?,
        file_id: optional_text_value(row.get(2)),
        lixcol_global: row.get(3).and_then(value_as_bool).unwrap_or(false),
        before_change_id: optional_text_value(row.get(4)),
        after_change_id: optional_text_value(row.get(5)),
        before_commit_id: optional_text_value(row.get(6)),
        after_commit_id: optional_text_value(row.get(7)),
        status: required_text_value(row, 8, "status")?,
    })
}

fn required_text_value(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    row.get(index).and_then(value_as_text).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing working changes {column}"),
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
