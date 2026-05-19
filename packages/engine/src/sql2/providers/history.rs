use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};
use tokio::sync::Mutex;

use crate::commit_graph::CommitGraphReader;
use crate::{serialize_row_metadata, LixError};

use crate::sql2::history_route::{
    load_history_entries, parse_history_filter, HistoryColumnStyle, HistoryRoute,
    HistoryViewDescriptor,
};
use crate::sql2::result_metadata::json_field;
use crate::sql2::SqlHistoryQuerySource;
use crate::storage::StorageRead;

pub(super) async fn register_history_provider<S>(
    session: &SessionContext,
    surface_name: &str,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlHistoryQuerySource<S>,
) -> Result<Arc<dyn TableProvider>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let provider: Arc<dyn TableProvider> = Arc::new(LixStateHistoryProvider::new(
        Arc::new(Mutex::new(commit_graph)),
        query_source,
    ));
    session
        .register_table(surface_name, Arc::clone(&provider))
        .map_err(datafusion_error_to_lix_error)?;
    Ok(provider)
}

pub(crate) struct LixStateHistoryProvider<S> {
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
}

impl<S> std::fmt::Debug for LixStateHistoryProvider<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateHistoryProvider").finish()
    }
}

impl<S> LixStateHistoryProvider<S> {
    pub(crate) fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlHistoryQuerySource<S>,
    ) -> Self {
        Self {
            schema: lix_state_history_schema(),
            commit_graph,
            query_source,
        }
    }
}

#[async_trait]
impl<S> TableProvider for LixStateHistoryProvider<S>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
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
                if parse_history_filter(filter, HistoryColumnStyle::Bare).is_some() {
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
            Arc::clone(&self.commit_graph),
            self.query_source.clone(),
            projected_schema,
            projection.cloned(),
            HistoryRoute::from_filters(filters, HistoryColumnStyle::Bare),
            limit,
        )))
    }
}

struct LixStateHistoryScanExec<S> {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: HistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl<S> std::fmt::Debug for LixStateHistoryScanExec<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateHistoryScanExec")
            .field("limit", &self.limit)
            .field("route", &self.route)
            .finish()
    }
}

impl<S> LixStateHistoryScanExec<S> {
    fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlHistoryQuerySource<S>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
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
            commit_graph,
            query_source,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl<S> DisplayAs for LixStateHistoryScanExec<S> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateHistoryScanExec(limit={:?}, route={:?})",
                    self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateHistoryScanExec"),
        }
    }
}

impl<S> ExecutionPlan for LixStateHistoryScanExec<S>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
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

        let commit_graph = Arc::clone(&self.commit_graph);
        let query_source = self.query_source.clone();
        let route = self.route.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let limit = self.limit;
        let zero_column_projection = self
            .projection
            .as_ref()
            .is_some_and(|projection| projection.is_empty());

        let stream = stream::once(async move {
            let rows = if route.is_contradictory() {
                Vec::new()
            } else {
                load_state_history_rows(commit_graph, query_source, &route)
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

pub(super) fn lix_state_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        json_field("entity_id", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        json_field("snapshot_content", true),
        json_field("metadata", true),
        Field::new("change_id", DataType::Utf8, false),
        Field::new("observed_commit_id", DataType::Utf8, false),
        Field::new("commit_created_at", DataType::Utf8, false),
        Field::new("start_commit_id", DataType::Utf8, false),
        Field::new("depth", DataType::Int64, false),
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

#[derive(Debug, Clone)]
struct StateHistorySqlRow {
    entity_id: String,
    schema_key: String,
    file_id: Option<String>,
    snapshot_content: Option<String>,
    metadata: Option<String>,
    change_id: String,
    observed_commit_id: String,
    commit_created_at: String,
    start_commit_id: String,
    depth: i64,
}

fn state_history_record_batch(
    schema: SchemaRef,
    rows: &[StateHistorySqlRow],
) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(match field.name().as_str() {
                "entity_id" => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
                "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
                "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
                "snapshot_content" => {
                    string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
                }
                "metadata" => Arc::new(StringArray::from(
                    rows.iter()
                        .map(|row| row.metadata.as_ref().map(serialize_row_metadata))
                        .collect::<Vec<_>>(),
                )),
                "change_id" => string_array(rows.iter().map(|row| Some(row.change_id.as_str()))),
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
                    rows.iter().map(|row| row.depth).collect::<Vec<_>>(),
                )) as ArrayRef,
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

async fn load_state_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    route: &HistoryRoute,
) -> Result<Vec<StateHistorySqlRow>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_state_history",
            start_commit_column: "start_commit_id",
        },
        commit_graph,
        query_source.json_reader,
        route,
        Vec::new(),
    )
    .await?;
    let mut rows = entries
        .into_iter()
        .map(|entry| -> Result<StateHistorySqlRow, LixError> {
            Ok(StateHistorySqlRow {
                entity_id: entry.change.entity_id.as_json_array_text()?,
                schema_key: entry.change.schema_key,
                file_id: entry.change.file_id,
                snapshot_content: entry.change.snapshot_content,
                metadata: entry.change.metadata,
                change_id: entry.change.id,
                observed_commit_id: entry.observed_commit_id,
                commit_created_at: entry.commit_created_at,
                start_commit_id: entry.start_commit_id,
                depth: i64::from(entry.depth),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    rows.sort_by(|left, right| {
        left.entity_id
            .cmp(&right.entity_id)
            .then(left.file_id.cmp(&right.file_id))
            .then(left.schema_key.cmp(&right.schema_key))
            .then(left.depth.cmp(&right.depth))
            .then(left.change_id.cmp(&right.change_id))
    });
    Ok(rows)
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}
