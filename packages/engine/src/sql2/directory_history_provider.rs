use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
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
use futures_util::stream;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::commit_graph::CommitGraphReader;
use crate::serialize_row_metadata;
use crate::LixError;

use super::history_projection::{tombstone_identity_column_value, HistoryIdentityProjection};
use super::history_route::{
    history_descriptor_event_matches, load_history_entries, parse_history_filter,
    HistoryColumnStyle, HistoryEntry, HistoryRoute, HistoryViewDescriptor, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_ID, HISTORY_COL_FILE_ID,
    HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID, HISTORY_COL_SCHEMA_KEY,
    HISTORY_COL_SNAPSHOT_CONTENT, HISTORY_COL_START_COMMIT_ID,
};
use super::result_metadata::json_field;
use super::SqlCommitStoreQuerySource;
use crate::commit_store::MaterializedChange;
use crate::storage::StorageRead;

const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(crate) async fn register_lix_directory_history_provider<S>(
    session: &datafusion::prelude::SessionContext,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlCommitStoreQuerySource<S>,
) -> Result<(), LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    session
        .register_table(
            "lix_directory_history",
            Arc::new(LixDirectoryHistoryProvider::new(
                Arc::new(Mutex::new(commit_graph)),
                query_source,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

struct LixDirectoryHistoryProvider<S> {
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource<S>,
}

impl<S> std::fmt::Debug for LixDirectoryHistoryProvider<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryHistoryProvider").finish()
    }
}

impl<S> LixDirectoryHistoryProvider<S> {
    fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlCommitStoreQuerySource<S>,
    ) -> Self {
        Self {
            schema: lix_directory_history_schema(),
            commit_graph,
            query_source,
        }
    }
}

#[async_trait]
impl<S> TableProvider for LixDirectoryHistoryProvider<S>
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
        Ok(Arc::new(LixDirectoryHistoryScanExec::new(
            Arc::clone(&self.commit_graph),
            self.query_source.clone(),
            projected_schema(&self.schema, projection)?,
            HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed),
            limit,
        )))
    }
}

struct LixDirectoryHistoryScanExec<S> {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource<S>,
    schema: SchemaRef,
    route: HistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl<S> std::fmt::Debug for LixDirectoryHistoryScanExec<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryHistoryScanExec")
            .field("route", &self.route)
            .field("limit", &self.limit)
            .finish()
    }
}

impl<S> LixDirectoryHistoryScanExec<S> {
    fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlCommitStoreQuerySource<S>,
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
            commit_graph,
            query_source,
            schema,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl<S> DisplayAs for LixDirectoryHistoryScanExec<S> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "LixDirectoryHistoryScanExec(route={:?}, limit={:?})",
                self.route, self.limit
            ),
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryHistoryScanExec"),
        }
    }
}

impl<S> ExecutionPlan for LixDirectoryHistoryScanExec<S>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        "LixDirectoryHistoryScanExec"
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
                "LixDirectoryHistoryScanExec does not accept children".to_string(),
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
                "LixDirectoryHistoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let commit_graph = Arc::clone(&self.commit_graph);
        let query_source = self.query_source.clone();
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let route = self.route.clone();
        let limit = self.limit;
        let fut = async move {
            let mut rows = load_directory_history_rows(commit_graph, query_source, &route)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            directory_history_record_batch(&stream_schema, &rows)
                .map_err(lix_error_to_datafusion_error)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut),
        )))
    }
}

#[derive(Debug, Clone)]
struct DirectoryHistoryRecord {
    id: String,
    parent_id: Option<String>,
    name: Option<String>,
    hidden: Option<bool>,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct DirectoryHistoryOutputRow {
    entity_id: String,
    id: String,
    path: Option<String>,
    parent_id: Option<String>,
    name: Option<String>,
    hidden: Option<bool>,
    descriptor_change: MaterializedChange,
    event: DirectoryHistoryEvent,
}

#[derive(Debug, Clone)]
struct DirectoryHistoryEvent {
    directory_id: String,
    start_commit_id: String,
    depth: u32,
    change: MaterializedChange,
    observed_commit_id: String,
    commit_created_at: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
}

async fn load_directory_history_rows<S>(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource<S>,
    route: &HistoryRoute,
) -> Result<Vec<DirectoryHistoryOutputRow>, LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    let event_route = route.traversal_only();
    let event_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_directory_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.json_reader.clone(),
        &event_route,
        vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
    )
    .await?;
    let context_route = route.starts_only();
    let context_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_directory_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        &context_route,
        vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
    )
    .await?;
    let event_descriptors = parse_directory_history_records(&event_entries)?;
    let descriptors = parse_directory_history_records(&context_entries)?;
    let mut output = Vec::new();

    for descriptor in &event_descriptors {
        let event = directory_history_event_from_entry(&descriptor.id, &descriptor.entry);
        let Some(visible_descriptor) = nearest_directory_descriptor(&descriptors, &event) else {
            continue;
        };
        let path = if visible_descriptor.name.is_some() {
            resolve_directory_history_path(
                &visible_descriptor.id,
                &event.start_commit_id,
                event.depth,
                &descriptors,
                &mut BTreeMap::new(),
                &mut BTreeSet::new(),
            )
        } else {
            None
        };
        let id = tombstone_identity_column_value(
            "id",
            &visible_descriptor.id,
            HistoryIdentityProjection::SingleColumn { column: "id" },
        )?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| visible_descriptor.id.clone());
        output.push(DirectoryHistoryOutputRow {
            entity_id: visible_descriptor.id.clone(),
            id,
            path,
            parent_id: visible_descriptor.parent_id.clone(),
            name: visible_descriptor.name.clone(),
            hidden: visible_descriptor.hidden,
            descriptor_change: visible_descriptor.entry.change.clone(),
            event,
        });
    }
    output.retain(|row| {
        let entity_id = entity_id_json_array(&row.entity_id).ok();
        route.matches_surface_row(
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            entity_id.as_deref().unwrap_or(&row.entity_id),
            None,
            row.event.depth,
        )
    });

    output.sort_by(|left, right| {
        left.entity_id
            .cmp(&right.entity_id)
            .then(left.event.start_commit_id.cmp(&right.event.start_commit_id))
            .then(left.event.depth.cmp(&right.event.depth))
            .then(
                left.event
                    .observed_commit_id
                    .cmp(&right.event.observed_commit_id),
            )
            .then(left.event.change.id.cmp(&right.event.change.id))
    });
    Ok(output)
}

fn parse_directory_history_records(
    entries: &[HistoryEntry],
) -> Result<Vec<DirectoryHistoryRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(DirectoryHistoryRecord {
                    id: entry.change.entity_id.as_single_string_owned()?,
                    parent_id: None,
                    name: None,
                    hidden: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(DirectoryHistoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: Some(snapshot.name),
                hidden: Some(snapshot.hidden.unwrap_or(false)),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn directory_history_event_from_entry(
    directory_id: &str,
    entry: &HistoryEntry,
) -> DirectoryHistoryEvent {
    DirectoryHistoryEvent {
        directory_id: directory_id.to_string(),
        start_commit_id: entry.start_commit_id.clone(),
        depth: entry.depth,
        change: entry.change.clone(),
        observed_commit_id: entry.observed_commit_id.clone(),
        commit_created_at: entry.commit_created_at.clone(),
    }
}

fn nearest_directory_descriptor<'a>(
    descriptors: &'a [DirectoryHistoryRecord],
    event: &DirectoryHistoryEvent,
) -> Option<&'a DirectoryHistoryRecord> {
    descriptors
        .iter()
        .filter(|descriptor| {
            let exact_descriptor_event =
                history_descriptor_event_matches(&descriptor.entry, event.depth, &event.change.id);
            (exact_descriptor_event || descriptor.name.is_some())
                && descriptor.id == event.directory_id
                && descriptor.entry.start_commit_id == event.start_commit_id
                && descriptor.entry.depth >= event.depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })
}

fn resolve_directory_history_path(
    directory_id: &str,
    start_commit_id: &str,
    target_depth: u32,
    directories: &[DirectoryHistoryRecord],
    cache: &mut BTreeMap<String, Option<String>>,
    visiting: &mut BTreeSet<String>,
) -> Option<String> {
    if let Some(path) = cache.get(directory_id) {
        return path.clone();
    }
    if !visiting.insert(directory_id.to_string()) {
        cache.insert(directory_id.to_string(), None);
        return None;
    }
    let directory = directories
        .iter()
        .filter(|directory| {
            directory.name.is_some()
                && directory.id == directory_id
                && directory.entry.start_commit_id == start_commit_id
                && directory.entry.depth >= target_depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })?;
    let name = directory.name.as_ref()?;
    let path = match directory.parent_id.as_deref() {
        Some(parent_id) => {
            let parent_path = resolve_directory_history_path(
                parent_id,
                start_commit_id,
                target_depth,
                directories,
                cache,
                visiting,
            )?;
            format!("{parent_path}{name}/")
        }
        None => format!("/{name}/"),
    };
    visiting.remove(directory_id);
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Some(path)
}

fn directory_history_record_batch(
    schema: &SchemaRef,
    rows: &[DirectoryHistoryOutputRow],
) -> Result<RecordBatch, LixError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| directory_history_column_array(field.name(), rows))
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_directory_history record batch: {error}"),
        )
    })
}

fn directory_history_column_array(
    column_name: &str,
    rows: &[DirectoryHistoryOutputRow],
) -> Result<ArrayRef, LixError> {
    Ok(match column_name {
        "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
        "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
        "parent_id" => string_array(rows.iter().map(|row| row.parent_id.as_deref())),
        "name" => string_array(rows.iter().map(|row| row.name.as_deref())),
        "hidden" => Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
        )) as ArrayRef,
        HISTORY_COL_ENTITY_ID => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| entity_id_json_array(&row.entity_id).map(Some))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        HISTORY_COL_SCHEMA_KEY => {
            string_array(rows.iter().map(|_| Some(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)))
        }
        HISTORY_COL_FILE_ID => string_array(rows.iter().map(|_| None)),
        HISTORY_COL_CHANGE_ID => {
            string_array(rows.iter().map(|row| Some(row.event.change.id.as_str())))
        }
        HISTORY_COL_SNAPSHOT_CONTENT => string_array(
            rows.iter()
                .map(|row| row.descriptor_change.snapshot_content.as_deref()),
        ),
        HISTORY_COL_METADATA => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.descriptor_change
                        .metadata
                        .as_ref()
                        .map(serialize_row_metadata)
                })
                .collect::<Vec<_>>(),
        )),
        HISTORY_COL_OBSERVED_COMMIT_ID => string_array(
            rows.iter()
                .map(|row| Some(row.event.observed_commit_id.as_str())),
        ),
        HISTORY_COL_COMMIT_CREATED_AT => string_array(
            rows.iter()
                .map(|row| Some(row.event.commit_created_at.as_str())),
        ),
        HISTORY_COL_START_COMMIT_ID => string_array(
            rows.iter()
                .map(|row| Some(row.event.start_commit_id.as_str())),
        ),
        HISTORY_COL_DEPTH => Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| i64::from(row.event.depth))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        other => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                "sql2 lix_directory_history provider does not support projected column '{other}'"
            ),
            ))
        }
    })
}

fn lix_directory_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, true),
        json_field(HISTORY_COL_ENTITY_ID, false),
        Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
        Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
        json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
        Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
        json_field(HISTORY_COL_METADATA, true),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_START_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
    ]))
}

fn projected_schema(base_schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(base_schema));
    };
    Ok(Arc::new(base_schema.project(projection)?))
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    super::error::datafusion_error_to_lix_error(error)
}

fn entity_id_json_array(entity_id: &str) -> Result<String, LixError> {
    serde_json::to_string(&[entity_id]).map_err(|error| {
        LixError::unknown(format!(
            "failed to encode history entity id as JSON: {error}"
        ))
    })
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(error)
}
