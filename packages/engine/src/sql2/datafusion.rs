use std::any::Any;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::TableType, physical_plan::SendableRecordBatchStream};
use futures_util::{stream, TryStreamExt};
use tokio::sync::oneshot;

use crate::live_state::{
    open_state_by_version_snapshot, open_state_by_version_snapshot_with_shared_backend,
    StateByVersionScanRequest, StateByVersionSnapshot, StateSurfaceColumn, StateSurfaceFilter,
};
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedSql2ReadArtifact {
    pub(crate) sql: String,
    pub(crate) active_version_id: String,
    pub(crate) surface_names: Vec<String>,
}

pub(crate) async fn execute_read_with_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_borrowed_backend(backend, artifact).await?;
    collect_query_result_from_ctx(ctx, artifact).await
}

pub(crate) async fn execute_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_shared_backend(backend, artifact).await?;
    collect_query_result_from_ctx(ctx, artifact).await
}

async fn collect_query_result_from_ctx(
    ctx: SessionContext,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let dataframe = ctx
        .sql(&artifact.sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let result_columns = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let batches = dataframe
        .collect()
        .await
        .map_err(datafusion_error_to_lix_error)?;
    query_result_from_batches(&result_columns, &batches)
}

async fn build_session_for_read_with_borrowed_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                let snapshot =
                    open_state_by_version_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("sql2 phase-2 does not support surface '{other}' yet"),
                ));
            }
        }
    }
    Ok(ctx)
}

async fn build_session_for_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                let snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::clone(&backend))
                        .await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("sql2 phase-2 does not support surface '{other}' yet"),
                ));
            }
        }
    }
    Ok(ctx)
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 live_state error: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixStateSurfaceKind {
    State,
}

impl LixStateSurfaceKind {
    fn schema(self) -> SchemaRef {
        match self {
            Self::State => Arc::new(Schema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                Field::new("plugin_key", DataType::Utf8, true),
                Field::new("snapshot_content", DataType::Utf8, true),
                Field::new("metadata", DataType::Utf8, true),
                Field::new("schema_version", DataType::Utf8, true),
                Field::new("created_at", DataType::Utf8, true),
                Field::new("updated_at", DataType::Utf8, true),
                Field::new("global", DataType::Boolean, false),
                Field::new("change_id", DataType::Utf8, true),
                Field::new("commit_id", DataType::Utf8, true),
                Field::new("untracked", DataType::Boolean, false),
            ])),
        }
    }
}

#[derive(Debug, Clone)]
struct LixStateProvider {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn StateByVersionSnapshot>,
}

impl LixStateProvider {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: surface_kind.schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixStateProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_route_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        let route = LixStateRoute::from_filters(filters);
        Ok(Arc::new(LixStateScanExec::new(
            self.surface_kind,
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixStateScanExec {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn StateByVersionSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixStateRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixStateScanExec {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixStateRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            surface_kind,
            default_version_id,
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateScanExec(surface={:?}, version_id={}, limit={:?}, route={:?})",
                    self.surface_kind, self.default_version_id, self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateScanExec"),
        }
    }
}

impl ExecutionPlan for LixStateScanExec {
    fn name(&self) -> &str {
        "LixStateScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Execution(
                "LixStateScanExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "LixStateScanExec only exposes one partition, got {partition}"
            )));
        }

        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                enqueue_state_by_version_scan_batches(
                    snapshot,
                    state_by_version_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    ),
                )
                .await?
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct StateByVersionScanJob {
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<RecordBatch>, LixError>>,
}

fn state_by_version_scan_worker() -> &'static mpsc::Sender<StateByVersionScanJob> {
    static WORKER: OnceLock<mpsc::Sender<StateByVersionScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<StateByVersionScanJob>();
        thread::Builder::new()
            .name("sql2-live-state-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 live-state runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime.block_on(async move {
                        job.snapshot
                            .scan_state_by_version_batches(&job.request)
                            .await
                    });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 live-state worker thread should spawn");
        tx
    })
}

async fn enqueue_state_by_version_scan_batches(
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
) -> Result<Vec<RecordBatch>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state_by_version_scan_worker()
        .send(StateByVersionScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue live_state scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 live_state scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixStateRoute {
    schema_key: Option<String>,
    entity_id: Option<String>,
    file_id: Option<String>,
    global: Option<bool>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl LixStateRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Global => &mut route.global,
                        RouteBooleanField::Untracked => &mut route.untracked,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::SchemaKey => &mut route.schema_key,
                        RouteStringField::EntityId => &mut route.entity_id,
                        RouteStringField::FileId => &mut route.file_id,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn state_filters(&self) -> Vec<StateSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(schema_key) = &self.schema_key {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::SchemaKey,
                Value::Text(schema_key.clone()),
            ));
        }
        if let Some(entity_id) = &self.entity_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::EntityId,
                Value::Text(entity_id.clone()),
            ));
        }
        if let Some(file_id) = &self.file_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::FileId,
                Value::Text(file_id.clone()),
            ));
        }
        if let Some(global) = self.global {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Global,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.untracked {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Untracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

fn state_by_version_scan_request(
    surface_kind: LixStateSurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixStateRoute,
    limit: Option<usize>,
) -> StateByVersionScanRequest {
    StateByVersionScanRequest {
        version_id: match surface_kind {
            LixStateSurfaceKind::State => default_version_id.to_string(),
        },
        projection: state_projection_for_scan(surface_kind, projection),
        filters: route.state_filters(),
        limit,
    }
}

fn assign_route_slot<T: PartialEq>(slot: &mut Option<T>, value: T, contradictory: &mut bool) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RoutePredicate {
    Boolean {
        field: RouteBooleanField,
        value: bool,
    },
    String {
        field: RouteStringField,
        value: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteBooleanField {
    Global,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteStringField {
    SchemaKey,
    EntityId,
    FileId,
}

fn parse_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "schema_key" => parse_string_route(literal, RouteStringField::SchemaKey),
        "entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "file_id" => parse_string_route(literal, RouteStringField::FileId),
        "global" => parse_boolean_route(literal, RouteBooleanField::Global),
        "untracked" => parse_boolean_route(literal, RouteBooleanField::Untracked),
        _ => None,
    }
}

fn parse_string_route(literal: &ScalarValue, field: RouteStringField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(RoutePredicate::String {
            field,
            value: value.clone(),
        }),
        _ => None,
    }
}

fn parse_boolean_route(literal: &ScalarValue, field: RouteBooleanField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Boolean(Some(value)) => Some(RoutePredicate::Boolean {
            field,
            value: *value,
        }),
        _ => None,
    }
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };

    let projected = schema.project(projection).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to project lix_state schema: {error}"))
    })?;
    Ok(Arc::new(projected))
}

fn state_projection_for_scan(
    surface_kind: LixStateSurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<StateSurfaceColumn> {
    let all_columns = match surface_kind {
        LixStateSurfaceKind::State => vec![
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
        ],
    };
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn query_result_from_batches(
    result_columns: &[String],
    batches: &[RecordBatch],
) -> Result<QueryResult, LixError> {
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for array in batch.columns() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                row.push(scalar_value_to_lix_value(&scalar));
            }
            rows.push(row);
        }
    }

    Ok(QueryResult {
        rows,
        columns: result_columns.to_vec(),
    })
}

fn scalar_value_to_lix_value(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(value)) => Value::Boolean(*value),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(value)) => Value::Integer(*value),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Text(value.to_string()),
        },
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(value)) => Value::Real(f64::from(*value)),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(value)) => Value::Real(*value),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Value::Text(value.clone()),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Value::Null
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Value::Blob(value.clone())
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        build_session_for_read_with_borrowed_backend, build_session_for_read_with_shared_backend,
        execute_read_with_backend, execute_read_with_shared_backend, parse_route_filter,
        PreparedSql2ReadArtifact, RouteBooleanField, RoutePredicate, RouteStringField,
    };
    use crate::live_state::{
        open_state_by_version_snapshot_with_shared_backend, StateByVersionScanRequest,
        StateSurfaceColumn, StateSurfaceFilter,
    };
    use crate::session::AdditionalSessionOptions;
    use crate::test_support::{boot_test_engine, TestSqliteBackendEvent};
    use crate::{CreateVersionOptions, TransactionBeginMode, Value};
    use serde_json::json;

    async fn setup_sql2_state_fixture(
    ) -> Result<(crate::test_support::TestSqliteBackend, crate::Session), crate::LixError> {
        let (backend, _lix, session) = boot_test_engine().await?;
        session
            .register_schema(&json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;
        session
            .register_schema(&json!({
                "x-lix-key": "other_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;

        session
            .create_version(CreateVersionOptions {
                id: Some("version-a".to_string()),
                name: Some("version-a".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-b".to_string()),
                name: Some("version-b".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, 'version-a', NULL, '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, 'version-b', NULL, '{\"value\":\"B\"}', '1'\
                 )",
                &[],
            )
            .await?;

        let sql2_session = session
            .open_additional_session(AdditionalSessionOptions {
                active_version_id: Some("version-a".to_string()),
                origin_key: Some("engine:sql2".to_string()),
                ..AdditionalSessionOptions::default()
            })
            .await?;
        Ok((backend, sql2_session))
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-datafusion-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn parses_string_route_filters() {
        let filter =
            datafusion::logical_expr::col("schema_key").eq(datafusion::logical_expr::lit("demo"));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::SchemaKey,
                value: "demo".to_string(),
            })
        );
    }

    #[test]
    fn parses_boolean_route_filters() {
        let filter =
            datafusion::logical_expr::col("untracked").eq(datafusion::logical_expr::lit(true));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::Boolean {
                field: RouteBooleanField::Untracked,
                value: true,
            })
        );
    }

    #[test]
    fn builds_session_and_executes_lix_state_query() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("session should build");
                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let batches = dataframe.collect().await.expect("query should execute");
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
            })
        });
    }

    #[test]
    fn shared_backend_path_defers_state_reads_until_execution() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("lix_registered_schema")
                            && !sql.contains("change_commit_by_change_id")
                            && !sql.contains("lix_internal_live")),
                    "session setup should not query live_state on shared-backend path"
                );

                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let _batches = dataframe.collect().await.expect("query should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("test_state_schema")),
                    "execution should query live_state on shared-backend path"
                );
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("other_state_schema")),
                    "schema_key pushdown should avoid scanning unrelated state schemas"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_opens_read_transaction_for_query_snapshot() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let _ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");

                let begin_modes = backend
                    .recorded_events()
                    .into_iter()
                    .filter_map(|event| match event {
                        TestSqliteBackendEvent::BeginTransaction { mode } => Some(mode),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    begin_modes,
                    vec![TransactionBeginMode::Read],
                    "shared-backend sql2 path should open one read transaction as the query snapshot"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_entity_constraint_into_source_scan() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("\"entity_id\" = 'entity-a'")
                            || sql.contains("entity_id = 'entity-a'")),
                    "entity_id filter should be pushed into live_state source scans"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_derives_required_columns_from_projection() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");

                let state_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| sql.contains("lix_internal_live_v1_test_state_schema"))
                    .collect::<Vec<_>>();
                assert!(
                    !state_scan_sql.is_empty(),
                    "expected sql2 read to scan the test_state_schema live table"
                );
                assert!(
                    state_scan_sql.iter().all(|sql| !sql.contains("\"value\"")),
                    "entity-only projection should avoid loading dynamic state columns: {state_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_limit_only_for_safe_untracked_scans() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-a', 'test_state_schema', NULL, NULL, '{\"value\":\"UA\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("first untracked row should insert");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-b', 'test_state_schema', NULL, NULL, '{\"value\":\"UB\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("second untracked row should insert");

                let untracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = untracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(true),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("untracked state-surface read should execute");

                let untracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = true")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    untracked_scan_sql.iter().any(|sql| sql.contains("LIMIT 1")),
                    "single-lane untracked scan should receive the pushed limit: {untracked_scan_sql:?}"
                );
                drop(untracked_snapshot);

                let tracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = tracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: "version-a".to_string(),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(false),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("tracked state-surface read should execute");

                let tracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = false")
                            && sql.contains("is_tombstone = 0")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    tracked_scan_sql.iter().all(|sql| !sql.contains("LIMIT 1")),
                    "tracked scans should keep source-side limits disabled: {tracked_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn execute_read_uses_active_version_snapshot_for_lix_state() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.columns, vec!["entity_id", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-a".to_string()),
                        Value::Text("{\"value\":\"A\"}".to_string())
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_exposes_commit_id_for_tracked_lix_state_rows() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT commit_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.rows.len(), 1);
                match &result.rows[0][0] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected text commit_id, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_uses_execution_time_state_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                };

                backend.clear_query_log();
                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend read should execute");
                assert_eq!(result.rows.len(), 1);
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("lix_registered_schema")),
                    "shared-backend execution should query live_state at execution time"
                );
            })
        });
    }
}
