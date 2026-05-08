use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BinaryArray, BooleanArray, Int64Array, StringArray};
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

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::changelog::MaterializedCanonicalChange;
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
use super::SqlChangelogQuerySource;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

pub(crate) async fn register_lix_file_history_provider(
    session: &datafusion::prelude::SessionContext,
    commit_graph: Box<dyn CommitGraphReader>,
    query_source: SqlChangelogQuerySource,
    blob_reader: Arc<dyn BlobDataReader>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_history",
            Arc::new(LixFileHistoryProvider::new(
                Arc::new(Mutex::new(commit_graph)),
                query_source,
                blob_reader,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

struct LixFileHistoryProvider {
    schema: SchemaRef,
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource,
    blob_reader: Arc<dyn BlobDataReader>,
}

impl std::fmt::Debug for LixFileHistoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileHistoryProvider").finish()
    }
}

impl LixFileHistoryProvider {
    fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlChangelogQuerySource,
        blob_reader: Arc<dyn BlobDataReader>,
    ) -> Self {
        Self {
            schema: lix_file_history_schema(),
            commit_graph,
            query_source,
            blob_reader,
        }
    }
}

#[async_trait]
impl TableProvider for LixFileHistoryProvider {
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
        let schema = projected_schema(&self.schema, projection)?;
        let needs_data = projection.is_none_or(|projection| {
            projection.iter().any(|index| {
                self.schema
                    .field(*index)
                    .name()
                    .as_str()
                    .eq_ignore_ascii_case("data")
            })
        });
        Ok(Arc::new(LixFileHistoryScanExec::new(
            Arc::clone(&self.commit_graph),
            self.query_source.clone(),
            Arc::clone(&self.blob_reader),
            schema,
            needs_data,
            HistoryRoute::from_filters(filters, HistoryColumnStyle::Prefixed),
            limit,
        )))
    }
}

struct LixFileHistoryScanExec {
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource,
    blob_reader: Arc<dyn BlobDataReader>,
    schema: SchemaRef,
    needs_data: bool,
    route: HistoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixFileHistoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileHistoryScanExec")
            .field("route", &self.route)
            .field("limit", &self.limit)
            .finish()
    }
}

impl LixFileHistoryScanExec {
    fn new(
        commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
        query_source: SqlChangelogQuerySource,
        blob_reader: Arc<dyn BlobDataReader>,
        schema: SchemaRef,
        needs_data: bool,
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
            blob_reader,
            schema,
            needs_data,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "LixFileHistoryScanExec(route={:?}, limit={:?})",
                self.route, self.limit
            ),
            DisplayFormatType::TreeRender => write!(f, "LixFileHistoryScanExec"),
        }
    }
}

impl ExecutionPlan for LixFileHistoryScanExec {
    fn name(&self) -> &str {
        "LixFileHistoryScanExec"
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
                "LixFileHistoryScanExec does not accept children".to_string(),
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
                "LixFileHistoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let commit_graph = Arc::clone(&self.commit_graph);
        let query_source = self.query_source.clone();
        let blob_reader = Arc::clone(&self.blob_reader);
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let route = self.route.clone();
        let limit = self.limit;
        let needs_data = self.needs_data;

        let fut = async move {
            let mut rows = load_file_history_rows(
                commit_graph,
                query_source,
                &blob_reader,
                &route,
                needs_data,
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            file_history_record_batch(&stream_schema, &rows).map_err(lix_error_to_datafusion_error)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut),
        )))
    }
}

#[derive(Debug, Clone)]
struct FileHistoryDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: Option<String>,
    hidden: Option<bool>,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryBlobRecord {
    file_id: String,
    blob_hash: Option<String>,
    entry: HistoryEntry,
}

#[derive(Debug, Clone)]
struct FileHistoryEvent {
    file_id: String,
    start_commit_id: String,
    depth: u32,
    priority: u8,
    change: MaterializedCanonicalChange,
    observed_commit_id: String,
    commit_created_at: String,
}

#[derive(Debug, Clone)]
struct FileHistoryOutputRow {
    entity_id: String,
    id: String,
    path: Option<String>,
    directory_id: Option<String>,
    name: Option<String>,
    hidden: Option<bool>,
    data: Option<Vec<u8>>,
    descriptor_change: MaterializedCanonicalChange,
    event: FileHistoryEvent,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
    hidden: bool,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
}

async fn load_file_history_rows(
    commit_graph: Arc<Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlChangelogQuerySource,
    blob_reader: &Arc<dyn BlobDataReader>,
    route: &HistoryRoute,
    needs_data: bool,
) -> Result<Vec<FileHistoryOutputRow>, LixError> {
    let event_route = route.traversal_only();
    let event_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        Arc::clone(&commit_graph),
        query_source.json_reader.clone(),
        &event_route,
        vec![
            FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            BLOB_REF_SCHEMA_KEY.to_string(),
        ],
    )
    .await?;
    let context_route = route.starts_only();
    let context_entries = load_history_entries(
        HistoryViewDescriptor {
            view_name: "lix_file_history",
            start_commit_column: HISTORY_COL_START_COMMIT_ID,
        },
        commit_graph,
        query_source.json_reader,
        &context_route,
        vec![
            FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            BLOB_REF_SCHEMA_KEY.to_string(),
        ],
    )
    .await?;

    let event_descriptors = parse_file_history_descriptors(&event_entries)?;
    let event_directories = parse_file_history_directories(&event_entries)?;
    let event_blobs = parse_file_history_blobs(&event_entries)?;
    let descriptors = parse_file_history_descriptors(&context_entries)?;
    let directories = parse_file_history_directories(&context_entries)?;
    let blobs = parse_file_history_blobs(&context_entries)?;
    let events = file_history_events(
        &event_descriptors,
        &event_directories,
        &event_blobs,
        &descriptors,
    );

    let mut output = Vec::new();
    for event in events {
        let Some(descriptor) = nearest_file_descriptor(&descriptors, &event) else {
            continue;
        };
        let blob = nearest_blob_ref(&blobs, &event);
        let data = if needs_data {
            match blob.and_then(|blob| blob.blob_hash.as_deref()) {
                Some(blob_hash) => load_single_blob_bytes(blob_reader, blob_hash).await?,
                None => None,
            }
        } else {
            None
        };
        let path = resolve_file_history_path(descriptor, &directories, event.depth);
        let id = tombstone_identity_column_value(
            "id",
            &descriptor.id,
            HistoryIdentityProjection::SingleColumn { column: "id" },
        )?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| descriptor.id.clone());

        output.push(FileHistoryOutputRow {
            entity_id: descriptor.id.clone(),
            id,
            path,
            directory_id: descriptor.directory_id.clone(),
            name: descriptor.name.clone(),
            hidden: descriptor.hidden,
            data,
            descriptor_change: descriptor.entry.change.clone(),
            event,
        });
    }
    output.retain(|row| {
        let entity_id = entity_id_json_array(&row.entity_id).ok();
        route.matches_surface_row(
            FILE_DESCRIPTOR_SCHEMA_KEY,
            entity_id.as_deref().unwrap_or(&row.entity_id),
            Some(&row.entity_id),
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

async fn load_single_blob_bytes(
    blob_reader: &Arc<dyn BlobDataReader>,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    let hash = BlobHash::from_hex(blob_hash)?;
    Ok(blob_reader
        .load_bytes_many(&[hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten())
}

fn file_history_events(
    event_descriptors: &[FileHistoryDescriptorRecord],
    event_directories: &[FileHistoryDirectoryRecord],
    event_blobs: &[FileHistoryBlobRecord],
    context_descriptors: &[FileHistoryDescriptorRecord],
) -> Vec<FileHistoryEvent> {
    let mut descriptor_ids_by_start = BTreeSet::<(String, String)>::new();
    let mut directory_ids_by_file_start = BTreeMap::<(String, String), BTreeSet<String>>::new();

    for descriptor in context_descriptors {
        let key = (
            descriptor.id.clone(),
            descriptor.entry.start_commit_id.clone(),
        );
        descriptor_ids_by_start.insert(key.clone());
        if let Some(directory_id) = &descriptor.directory_id {
            directory_ids_by_file_start
                .entry(key)
                .or_default()
                .insert(directory_id.clone());
        }
    }

    let mut candidates = Vec::new();
    for descriptor in event_descriptors {
        candidates.push(file_history_event_from_entry(
            descriptor.id.clone(),
            &descriptor.entry,
            1,
        ));
    }
    for directory in event_directories {
        for ((file_id, start_commit_id), directory_ids) in &directory_ids_by_file_start {
            if start_commit_id == &directory.entry.start_commit_id
                && directory_ids.contains(&directory.id)
            {
                candidates.push(file_history_event_from_entry(
                    file_id.clone(),
                    &directory.entry,
                    2,
                ));
            }
        }
    }
    for blob in event_blobs {
        if descriptor_ids_by_start
            .contains(&(blob.file_id.clone(), blob.entry.start_commit_id.clone()))
        {
            candidates.push(file_history_event_from_entry(
                blob.file_id.clone(),
                &blob.entry,
                3,
            ));
        }
    }

    candidates.sort_by(|left, right| {
        left.file_id
            .cmp(&right.file_id)
            .then(left.start_commit_id.cmp(&right.start_commit_id))
            .then(left.depth.cmp(&right.depth))
            .then(left.priority.cmp(&right.priority))
            .then(left.change.id.cmp(&right.change.id))
    });
    candidates.dedup_by(|left, right| {
        left.file_id == right.file_id
            && left.start_commit_id == right.start_commit_id
            && left.depth == right.depth
    });
    candidates
}

fn file_history_event_from_entry(
    file_id: String,
    entry: &HistoryEntry,
    priority: u8,
) -> FileHistoryEvent {
    FileHistoryEvent {
        file_id,
        start_commit_id: entry.start_commit_id.clone(),
        depth: entry.depth,
        priority,
        change: entry.change.clone(),
        observed_commit_id: entry.observed_commit_id.clone(),
        commit_created_at: entry.commit_created_at.clone(),
    }
}

fn parse_file_history_descriptors(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryDescriptorRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryDescriptorRecord {
                    id: entry.change.entity_id.as_single_string_owned()?,
                    directory_id: None,
                    name: None,
                    hidden: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: FileDescriptorSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDescriptorRecord {
                id: snapshot.id,
                directory_id: snapshot.directory_id,
                name: Some(snapshot.name),
                hidden: Some(snapshot.hidden),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_directories(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryDirectoryRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .filter_map(|entry| {
            let snapshot_content = entry.change.snapshot_content.clone()?;
            Some((entry, snapshot_content))
        })
        .map(|(entry, snapshot_content)| {
            let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(&snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_directory_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDirectoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: snapshot.name,
                entry: entry.clone(),
            })
        })
        .collect()
}

fn parse_file_history_blobs(
    entries: &[HistoryEntry],
) -> Result<Vec<FileHistoryBlobRecord>, LixError> {
    entries
        .iter()
        .filter(|entry| entry.change.schema_key == BLOB_REF_SCHEMA_KEY)
        .map(|entry| {
            let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else {
                return Ok(FileHistoryBlobRecord {
                    file_id: entry.change.file_id.clone().unwrap_or_else(|| {
                        entry
                            .change
                            .entity_id
                            .as_single_string_owned()
                            .expect("canonical change entity identity should project")
                    }),
                    blob_hash: None,
                    entry: entry.clone(),
                });
            };
            let snapshot: BlobRefSnapshot =
                serde_json::from_str(snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryBlobRecord {
                file_id: entry.change.file_id.clone().unwrap_or(snapshot.id),
                blob_hash: Some(snapshot.blob_hash),
                entry: entry.clone(),
            })
        })
        .collect()
}

fn nearest_file_descriptor<'a>(
    descriptors: &'a [FileHistoryDescriptorRecord],
    event: &FileHistoryEvent,
) -> Option<&'a FileHistoryDescriptorRecord> {
    descriptors
        .iter()
        .filter(|descriptor| {
            let exact_descriptor_event =
                history_descriptor_event_matches(&descriptor.entry, event.depth, &event.change.id);
            (exact_descriptor_event || descriptor.name.is_some())
                && descriptor.id == event.file_id
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

fn nearest_blob_ref<'a>(
    blobs: &'a [FileHistoryBlobRecord],
    event: &FileHistoryEvent,
) -> Option<&'a FileHistoryBlobRecord> {
    blobs
        .iter()
        .filter(|blob| {
            blob.file_id == event.file_id
                && blob.entry.start_commit_id == event.start_commit_id
                && blob.entry.depth >= event.depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })
}

fn resolve_file_history_path(
    descriptor: &FileHistoryDescriptorRecord,
    directories: &[FileHistoryDirectoryRecord],
    target_depth: u32,
) -> Option<String> {
    let name = descriptor.name.as_ref()?;
    let Some(directory_id) = descriptor.directory_id.as_deref() else {
        return Some(format!("/{name}"));
    };
    let directory_path = resolve_directory_history_path(
        directory_id,
        &descriptor.entry.start_commit_id,
        target_depth,
        directories,
        &mut BTreeMap::new(),
        &mut BTreeSet::new(),
    )?;
    Some(format!("{directory_path}{name}"))
}

fn resolve_directory_history_path(
    directory_id: &str,
    start_commit_id: &str,
    target_depth: u32,
    directories: &[FileHistoryDirectoryRecord],
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
            directory.id == directory_id
                && directory.entry.start_commit_id == start_commit_id
                && directory.entry.depth >= target_depth
        })
        .min_by(|left, right| {
            left.entry
                .depth
                .cmp(&right.entry.depth)
                .then(left.entry.change.id.cmp(&right.entry.change.id))
        })?;
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
            format!("{parent_path}{}/", directory.name)
        }
        None => format!("/{}/", directory.name),
    };
    visiting.remove(directory_id);
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Some(path)
}

fn file_history_record_batch(
    schema: &SchemaRef,
    rows: &[FileHistoryOutputRow],
) -> Result<RecordBatch, LixError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| file_history_column_array(field.name(), rows))
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_file_history record batch: {error}"),
        )
    })
}

fn file_history_column_array(
    column_name: &str,
    rows: &[FileHistoryOutputRow],
) -> Result<ArrayRef, LixError> {
    Ok(match column_name {
        "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
        "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
        "directory_id" => string_array(rows.iter().map(|row| row.directory_id.as_deref())),
        "name" => string_array(rows.iter().map(|row| row.name.as_deref())),
        "hidden" => Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
        )) as ArrayRef,
        "data" => Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.data.as_deref())
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        HISTORY_COL_ENTITY_ID => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| entity_id_json_array(&row.entity_id).map(Some))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )) as ArrayRef,
        HISTORY_COL_SCHEMA_KEY => {
            string_array(rows.iter().map(|_| Some(FILE_DESCRIPTOR_SCHEMA_KEY)))
        }
        HISTORY_COL_FILE_ID => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
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
                    "sql2 lix_file_history provider does not support projected column '{other}'"
                ),
            ))
        }
    })
}

fn lix_file_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, true),
        Field::new("data", DataType::Binary, true),
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
