use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures_util::stream;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::commit_graph::CommitGraphReader;
use crate::commit_store::MaterializedChange;
use crate::serialize_row_metadata;
use crate::LixError;

use crate::sql2::catalog::{
    entity_surface_schema, EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec,
};
use crate::sql2::history_projection::{tombstone_identity_column_value, HistoryIdentityProjection};
use crate::sql2::history_route::{
    load_history_entries, parse_history_filter, HistoryColumnStyle, HistoryRoute,
    HistoryViewDescriptor, HISTORY_COL_START_COMMIT_ID,
};
use crate::sql2::providers::entity::{
    entity_f64_value, entity_i64_value, entity_json_text_value, parse_snapshot, string_array,
};
use crate::sql2::SqlCommitStoreQuerySource;

/// Schema-specific history surface backed directly by the commit graph.
///
/// The provider does not query `lix_state_history` through SQL. It uses the same
/// commit graph primitive as the generic history surface, then shapes canonical
/// changes into the typed entity columns for one registered schema.
pub(crate) struct EntityHistoryProvider {
    spec: Arc<EntitySurfaceSpec>,
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource,
}

impl std::fmt::Debug for EntityHistoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityHistoryProvider")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityHistoryProvider {
    pub(crate) fn new(
        spec: Arc<EntitySurfaceSpec>,
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlCommitStoreQuerySource,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntitySurfaceShape::History),
            spec,
            commit_graph,
            query_source,
        }
    }
}

#[async_trait]
impl TableProvider for EntityHistoryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_history_filter(filter, HistoryColumnStyle::Prefixed).is_some() {
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
        let route = HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed);
        let schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(EntityHistoryScanExec::new(
            Arc::clone(&self.spec),
            Arc::clone(&self.commit_graph),
            self.query_source.clone(),
            schema,
            route,
            limit,
        )))
    }
}

struct EntityHistoryScanExec {
    spec: Arc<EntitySurfaceSpec>,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource,
    schema: SchemaRef,
    route: HistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityHistoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityHistoryScanExec")
            .field("schema_key", &self.spec.schema_key)
            .field("route", &self.route)
            .field("limit", &self.limit)
            .finish()
    }
}

impl EntityHistoryScanExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlCommitStoreQuerySource,
        schema: SchemaRef,
        route: HistoryRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            spec,
            commit_graph,
            query_source,
            schema,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "EntityHistoryScanExec(schema_key={}, route={:?}, limit={:?})",
                self.spec.schema_key, self.route, self.limit
            ),
            DisplayFormatType::TreeRender => write!(f, "EntityHistoryScanExec"),
        }
    }
}

impl ExecutionPlan for EntityHistoryScanExec {
    fn name(&self) -> &str {
        "EntityHistoryScanExec"
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
            return Err(DataFusionError::Internal(
                "EntityHistoryScanExec does not accept children".to_string(),
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
                "EntityHistoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
        let commit_graph = Arc::clone(&self.commit_graph);
        let query_source = self.query_source.clone();
        let schema = Arc::clone(&self.schema);
        let route = self.route.clone();
        let limit = self.limit;
        let stream_schema = Arc::clone(&schema);
        let fut = async move {
            let rows = load_entity_history_rows(&spec, commit_graph, query_source, &route, limit)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            entity_history_record_batch(&stream_schema, &spec, &rows)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut),
        )))
    }
}

#[derive(Debug, Clone)]
struct EntityHistoryRow {
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: String,
    start_commit_id: String,
    depth: u32,
}

async fn load_entity_history_rows(
    spec: &EntitySurfaceSpec,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource,
    route: &HistoryRoute,
    limit: Option<usize>,
) -> Result<Vec<EntityHistoryRow>, LixError> {
    let history_view_name = format!("{}_history", spec.schema_key);
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: history_view_name.as_str(),
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        route,
        vec![spec.schema_key.clone()],
    )
    .await?;
    let mut rows = entries
        .into_iter()
        .map(|entry| EntityHistoryRow {
            change: entry.change,
            observed_commit_id: entry.observed_commit_id,
            commit_created_at: entry.commit_created_at,
            start_commit_id: entry.start_commit_id,
            depth: entry.depth,
        })
        .collect::<Vec<_>>();
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn entity_history_record_batch(
    schema: &SchemaRef,
    spec: &EntitySurfaceSpec,
    rows: &[EntityHistoryRow],
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| entity_history_column_array(field.name(), spec, rows))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )?)
}

fn entity_history_column_array(
    column_name: &str,
    spec: &EntitySurfaceSpec,
    rows: &[EntityHistoryRow],
) -> Result<ArrayRef> {
    if let Some(system_column) = column_name.strip_prefix("lixcol_") {
        return entity_history_system_column_array(system_column, rows);
    }

    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 entity history provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;
    let projected_values = rows
        .iter()
        .map(|row| entity_history_column_value(row, spec, column_name))
        .collect::<Result<Vec<_>>>()?;

    Ok(match column_type {
        EntityColumnType::String | EntityColumnType::Json => Arc::new(StringArray::from(
            projected_values
                .iter()
                .map(|snapshot| entity_json_text_value(snapshot.as_ref(), column_type))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Integer => Arc::new(Int64Array::from(
            projected_values
                .iter()
                .map(|snapshot| entity_i64_value(snapshot.as_ref()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Number => Arc::new(Float64Array::from(
            projected_values
                .iter()
                .map(|snapshot| entity_f64_value(snapshot.as_ref()))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Boolean => Arc::new(BooleanArray::from(
            projected_values
                .iter()
                .map(|snapshot| snapshot.as_ref().and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    })
}

fn entity_history_column_value(
    row: &EntityHistoryRow,
    spec: &EntitySurfaceSpec,
    column_name: &str,
) -> Result<Option<JsonValue>> {
    let snapshot = parse_snapshot(row.change.snapshot_content.as_deref())?;
    if let Some(snapshot) = snapshot {
        return Ok(snapshot.get(column_name).cloned());
    }

    let entity_id = row.change.entity_id.as_json_array_text().map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 entity history provider failed to project entity id: {error}"
        ))
    })?;
    tombstone_identity_column_value(
        column_name,
        &entity_id,
        HistoryIdentityProjection::PrimaryKeyPaths(&spec.primary_key_paths),
    )
    .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn entity_history_system_column_array(
    column_name: &str,
    rows: &[EntityHistoryRow],
) -> Result<ArrayRef> {
    Ok(match column_name {
        "entity_id" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    Some(
                        row.change
                            .entity_id
                            .as_json_array_text()
                            .expect("canonical change entity identity should project"),
                    )
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        "schema_key" => string_array(rows.iter().map(|row| Some(row.change.schema_key.as_str()))),
        "file_id" => string_array(rows.iter().map(|row| row.change.file_id.as_deref())),
        "snapshot_content" => string_array(
            rows.iter()
                .map(|row| row.change.snapshot_content.as_deref()),
        ),
        "metadata" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.change.metadata.as_ref().map(serialize_row_metadata))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        "change_id" => string_array(rows.iter().map(|row| Some(row.change.id.as_str()))),
        "observed_commit_id" => {
            string_array(rows.iter().map(|row| Some(row.observed_commit_id.as_str())))
        }
        "commit_created_at" => {
            string_array(rows.iter().map(|row| Some(row.commit_created_at.as_str())))
        }
        "start_commit_id" => {
            string_array(rows.iter().map(|row| Some(row.start_commit_id.as_str())))
        }
        "depth" => Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| i64::from(row.depth))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        other => {
            return Err(DataFusionError::Execution(format!(
                "sql2 entity history provider does not support system column 'lixcol_{other}'"
            )))
        }
    })
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };
    Ok(Arc::new(schema.project(projection)?))
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}
