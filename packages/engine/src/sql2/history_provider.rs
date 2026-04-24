use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};

use crate::history::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRootScope,
    StateHistoryRow, StateHistoryVersionScope,
};
use crate::LixError;

use super::execute::HistoryContext;

pub(crate) async fn register_history_providers(
    session: &SessionContext,
    active_version_id: &str,
    history: Option<Arc<dyn HistoryContext>>,
) -> Result<Option<Arc<dyn TableProvider>>, LixError> {
    let Some(history) = history else {
        return Ok(None);
    };

    let provider: Arc<dyn TableProvider> =
        Arc::new(LixStateHistoryProvider::new(active_version_id, history));
    session
        .register_table("lix_state_history", Arc::clone(&provider))
        .map_err(datafusion_error_to_lix_error)?;
    Ok(Some(provider))
}

pub(crate) struct LixStateHistoryProvider {
    active_version_id: String,
    schema: SchemaRef,
    history: Arc<dyn HistoryContext>,
}

impl std::fmt::Debug for LixStateHistoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateHistoryProvider").finish()
    }
}

impl LixStateHistoryProvider {
    pub(crate) fn new(
        active_version_id: impl Into<String>,
        history: Arc<dyn HistoryContext>,
    ) -> Self {
        Self {
            active_version_id: active_version_id.into(),
            schema: lix_state_history_schema(),
            history,
        }
    }
}

#[async_trait]
impl TableProvider for LixStateHistoryProvider {
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
                if parse_state_history_filter(filter).is_some() {
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
        Ok(Arc::new(LixStateHistoryScanExec::new(
            self.active_version_id.clone(),
            Arc::clone(&self.history),
            projected_schema,
            projection.cloned(),
            StateHistoryRoute::from_filters(filters),
            limit,
        )))
    }
}

struct LixStateHistoryScanExec {
    active_version_id: String,
    history: Arc<dyn HistoryContext>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: StateHistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixStateHistoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateHistoryScanExec")
            .field("active_version_id", &self.active_version_id)
            .field("limit", &self.limit)
            .field("route", &self.route)
            .finish()
    }
}

impl LixStateHistoryScanExec {
    fn new(
        active_version_id: String,
        history: Arc<dyn HistoryContext>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: StateHistoryRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            active_version_id,
            history,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateHistoryScanExec(active_version_id={}, limit={:?}, route={:?})",
                    self.active_version_id, self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateHistoryScanExec"),
        }
    }
}

impl ExecutionPlan for LixStateHistoryScanExec {
    fn name(&self) -> &str {
        "LixStateHistoryScanExec"
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
                "LixStateHistoryScanExec does not accept children".to_string(),
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
                "LixStateHistoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let history = Arc::clone(&self.history);
        let request = state_history_request(&self.active_version_id, &self.route);
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let limit = self.limit;
        let zero_column_projection = self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.is_empty());

        let stream = stream::once(async move {
            let rows = if request_contradictory(&request) {
                Vec::new()
            } else {
                history
                    .scan_state_history(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let rows = if let Some(limit) = limit {
                rows.into_iter().take(limit).collect::<Vec<_>>()
            } else {
                rows
            };

            let batch = if zero_column_projection {
                let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
                RecordBatch::try_new_with_options(Arc::clone(&stream_schema), vec![], &options)
                    .map_err(|error| {
                        DataFusionError::Execution(format!(
                            "failed to build zero-column lix_state_history batch: {error}"
                        ))
                    })?
            } else {
                state_history_record_batch(Arc::clone(&stream_schema), &rows)?
            };
            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                batch,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn lix_state_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("change_id", DataType::Utf8, false),
        Field::new("commit_id", DataType::Utf8, false),
        Field::new("commit_created_at", DataType::Utf8, false),
        Field::new("root_commit_id", DataType::Utf8, false),
        Field::new("depth", DataType::Int64, false),
        Field::new("version_id", DataType::Utf8, false),
    ]))
}

fn projected_schema(base_schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let fields = match projection {
        Some(indices) => indices
            .iter()
            .map(|index| base_schema.field(*index).as_ref().clone())
            .collect::<Vec<_>>(),
        None => base_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>(),
    };
    Ok(Arc::new(Schema::new(fields)))
}

fn state_history_record_batch(schema: SchemaRef, rows: &[StateHistoryRow]) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(match field.name().as_str() {
                "entity_id" => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
                "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
                "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
                "plugin_key" => string_array(rows.iter().map(|row| row.plugin_key.as_deref())),
                "snapshot_content" => {
                    string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
                }
                "metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
                "schema_version" => {
                    string_array(rows.iter().map(|row| Some(row.schema_version.as_str())))
                }
                "change_id" => string_array(rows.iter().map(|row| Some(row.change_id.as_str()))),
                "commit_id" => string_array(rows.iter().map(|row| Some(row.commit_id.as_str()))),
                "commit_created_at" => {
                    string_array(rows.iter().map(|row| Some(row.commit_created_at.as_str())))
                }
                "root_commit_id" => {
                    string_array(rows.iter().map(|row| Some(row.root_commit_id.as_str())))
                }
                "depth" => Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.depth).collect::<Vec<_>>(),
                )) as ArrayRef,
                "version_id" => string_array(rows.iter().map(|row| Some(row.version_id.as_str()))),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "lix_state_history provider does not support projected column '{other}'"
                    )))
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

fn state_history_request(
    active_version_id: &str,
    route: &StateHistoryRoute,
) -> StateHistoryRequest {
    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        lineage_version_id: Some(active_version_id.to_string()),
        content_mode: StateHistoryContentMode::IncludeSnapshotContent,
        ..StateHistoryRequest::default()
    };

    if !route.root_commit_ids.is_empty() {
        request.root_scope = StateHistoryRootScope::RequestedRoots(route.root_commit_ids.clone());
    }
    if !route.entity_ids.is_empty() {
        request.entity_ids = route.entity_ids.clone();
    }
    if !route.schema_keys.is_empty() {
        request.schema_keys = route.schema_keys.clone();
    }
    if !route.version_ids.is_empty() {
        request.version_scope =
            StateHistoryVersionScope::RequestedVersions(route.version_ids.clone());
    }
    request.min_depth = route.min_depth;
    request.max_depth = route.max_depth;
    request
}

fn request_contradictory(request: &StateHistoryRequest) -> bool {
    request
        .min_depth
        .zip(request.max_depth)
        .is_some_and(|(min, max)| min > max)
        || matches!(request.root_scope, StateHistoryRootScope::RequestedRoots(ref roots) if roots.is_empty())
        || matches!(request.version_scope, StateHistoryVersionScope::RequestedVersions(ref versions) if versions.is_empty())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StateHistoryRoute {
    root_commit_ids: Vec<String>,
    entity_ids: Vec<String>,
    schema_keys: Vec<String>,
    version_ids: Vec<String>,
    min_depth: Option<i64>,
    max_depth: Option<i64>,
}

impl StateHistoryRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            apply_state_history_filter(filter, &mut route);
        }
        route
    }
}

fn canonical_state_history_column_name(name: &str) -> Option<&str> {
    match name {
        "root_commit_id" | "lixcol_root_commit_id" => Some("root_commit_id"),
        "entity_id" | "lixcol_entity_id" => Some("entity_id"),
        "schema_key" | "lixcol_schema_key" => Some("schema_key"),
        "version_id" | "lixcol_version_id" => Some("version_id"),
        "depth" | "lixcol_depth" => Some("depth"),
        _ => None,
    }
}

fn parse_state_history_filter(expr: &Expr) -> Option<()> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    match binary_expr.op {
        Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {}
        _ => return None,
    }

    let Expr::Column(column) = &*binary_expr.left else {
        return None;
    };
    let Expr::Literal(_, _) = &*binary_expr.right else {
        return None;
    };

    canonical_state_history_column_name(column.name.as_str()).and_then(|column_name| {
        match column_name {
            "root_commit_id" | "entity_id" | "schema_key" | "version_id" | "depth" => Some(()),
            _ => None,
        }
    })
}

fn apply_state_history_filter(expr: &Expr, route: &mut StateHistoryRoute) {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return;
    };
    let Expr::Column(column) = &*binary_expr.left else {
        return;
    };
    let Some(column_name) = canonical_state_history_column_name(column.name.as_str()) else {
        return;
    };
    let right = &*binary_expr.right;
    match (column_name, &binary_expr.op, right) {
        ("root_commit_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("entity_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("schema_key", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _))
        | ("version_id", Operator::Eq, Expr::Literal(ScalarValue::Utf8(Some(value)), _)) => {
            let bucket = match column_name {
                "root_commit_id" => &mut route.root_commit_ids,
                "entity_id" => &mut route.entity_ids,
                "schema_key" => &mut route.schema_keys,
                "version_id" => &mut route.version_ids,
                _ => unreachable!(),
            };
            if !bucket.contains(value) {
                bucket.push(value.clone());
            }
        }
        ("depth", Operator::Eq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(value);
                route.max_depth = Some(value);
            }
        }
        ("depth", Operator::Gt, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(
                    route
                        .min_depth
                        .map_or(value + 1, |current| current.max(value + 1)),
                );
            }
        }
        ("depth", Operator::GtEq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.min_depth = Some(route.min_depth.map_or(value, |current| current.max(value)));
            }
        }
        ("depth", Operator::Lt, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.max_depth = Some(
                    route
                        .max_depth
                        .map_or(value - 1, |current| current.min(value - 1)),
                );
            }
        }
        ("depth", Operator::LtEq, depth_expr) => {
            if let Some(value) = scalar_i64_literal(depth_expr) {
                route.max_depth = Some(route.max_depth.map_or(value, |current| current.min(value)));
            }
        }
        _ => {}
    }
}

fn scalar_i64_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::UInt8(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => i64::try_from(*value).ok(),
        _ => None,
    }
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 history provider error: {error}"))
}
