use std::any::Any;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BinaryArray, BooleanArray, StringArray};
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
use datafusion::{datasource::TableType, physical_plan::SendableRecordBatchStream};
use futures_util::{stream, TryStreamExt};
use tokio::sync::oneshot;

use crate::catalog::{
    DirectorySurfaceColumn, DirectorySurfaceFilter, DirectorySurfaceRow,
    DirectorySurfaceScanRequest, DirectorySurfaceSnapshot, FileSurfaceColumn, FileSurfaceFilter,
    FileSurfaceRow, FileSurfaceScanRequest, FileSurfaceSnapshot,
};
#[cfg(test)]
use crate::history::{DirectoryHistoryRow, FileHistoryRow};
use crate::{LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LixFileSurfaceKind {
    File,
    FileByVersion,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LixFileHistorySurfaceKind {
    FileHistory,
    FileHistoryByVersion,
}

fn lix_file_schema(surface_kind: LixFileSurfaceKind) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("extension", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
    ];
    if surface_kind == LixFileSurfaceKind::FileByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields.extend([
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
fn lix_file_history_schema(_surface_kind: LixFileHistorySurfaceKind) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, true),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_version_id", DataType::Utf8, false),
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_change_id", DataType::Utf8, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, false),
        Field::new("lixcol_commit_created_at", DataType::Utf8, false),
        Field::new("lixcol_root_commit_id", DataType::Utf8, false),
        Field::new("lixcol_depth", DataType::Int64, false),
    ]))
}

#[derive(Debug, Clone)]
pub(crate) struct LixFileProvider {
    surface_kind: LixFileSurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn FileSurfaceSnapshot>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct LixFileHistoryProvider {
    surface_kind: LixFileHistorySurfaceKind,
    schema: SchemaRef,
    rows: Arc<Vec<FileHistoryRow>>,
}

#[cfg(test)]
impl LixFileHistoryProvider {
    pub(crate) fn new(surface_kind: LixFileHistorySurfaceKind, rows: Vec<FileHistoryRow>) -> Self {
        Self {
            surface_kind,
            schema: lix_file_history_schema(surface_kind),
            rows: Arc::new(rows),
        }
    }
}

impl LixFileProvider {
    pub(crate) fn new(
        surface_kind: LixFileSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn FileSurfaceSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: lix_file_schema(surface_kind),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixFileProvider {
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
                if parse_file_route_filter(filter).is_some() {
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
        let route = LixFileRoute::from_filters(filters);
        Ok(Arc::new(LixFileScanExec::new(
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

#[cfg(test)]
#[async_trait]
impl TableProvider for LixFileHistoryProvider {
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
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixFileHistoryScanExec::new(
            self.surface_kind,
            Arc::clone(&self.rows),
            projected_schema,
            projection.cloned(),
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixFileScanExec {
    surface_kind: LixFileSurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixFileRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixFileScanExec {
    fn new(
        surface_kind: LixFileSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn FileSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixFileRoute,
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

impl DisplayAs for LixFileScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixFileScanExec(limit={:?}, route={:?})",
                    self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileScanExec"),
        }
    }
}

impl ExecutionPlan for LixFileScanExec {
    fn name(&self) -> &str {
        "LixFileScanExec"
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
                "LixFileScanExec does not accept children".to_string(),
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
                "LixFileScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                let rows = enqueue_file_surface_scan(
                    snapshot,
                    file_surface_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    ),
                )
                .await?;
                file_surface_record_batches(
                    file_projection_for_scan(surface_kind, projection.as_ref()),
                    &rows,
                )?
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
struct FileSurfaceScanJob {
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    request: FileSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<FileSurfaceRow>, LixError>>,
}

fn file_surface_scan_worker() -> &'static mpsc::Sender<FileSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<FileSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<FileSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-file-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 file-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_files(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 file-surface worker thread should spawn");
        tx
    })
}

#[cfg(test)]
#[derive(Debug)]
struct LixFileHistoryScanExec {
    surface_kind: LixFileHistorySurfaceKind,
    rows: Arc<Vec<FileHistoryRow>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

#[cfg(test)]
impl LixFileHistoryScanExec {
    fn new(
        surface_kind: LixFileHistorySurfaceKind,
        rows: Arc<Vec<FileHistoryRow>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
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
            rows,
            schema,
            projection,
            limit,
            properties: Arc::new(properties),
        }
    }
}

#[cfg(test)]
impl DisplayAs for LixFileHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixFileHistoryScanExec(kind={:?}, limit={:?})",
                    self.surface_kind, self.limit
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileHistoryScanExec"),
        }
    }
}

#[cfg(test)]
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

        let projection = self.projection.clone();
        let limit = self.limit;
        let rows = Arc::clone(&self.rows);
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let truncated = limit
                .map(|limit| rows.iter().take(limit).cloned().collect::<Vec<_>>())
                .unwrap_or_else(|| rows.as_ref().clone());
            let batches = file_history_record_batches(
                file_history_projection_for_scan(projection.as_ref()),
                &truncated,
            )?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn enqueue_file_surface_scan(
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    request: FileSurfaceScanRequest,
) -> Result<Vec<FileSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    file_surface_scan_worker()
        .send(FileSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue file surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 file surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn file_surface_record_batches(
    projection: Vec<FileSurfaceColumn>,
    rows: &[FileSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![file_surface_record_batch(&projection, rows)?])
}

fn file_surface_record_batch(
    projection: &[FileSurfaceColumn],
    rows: &[FileSurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            FileSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            FileSurfaceColumn::DirectoryId => {
                string_array(rows.iter().map(|row| row.directory_id.as_deref()))
            }
            FileSurfaceColumn::Name => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
            FileSurfaceColumn::Extension => {
                string_array(rows.iter().map(|row| row.extension.as_deref()))
            }
            FileSurfaceColumn::Path => string_array(rows.iter().map(|row| row.path.as_deref())),
            FileSurfaceColumn::Data => binary_array(rows.iter().map(|row| row.data.as_deref())),
            FileSurfaceColumn::Metadata => {
                string_array(rows.iter().map(|row| row.metadata.as_deref()))
            }
            FileSurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolEntityId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
            }
            FileSurfaceColumn::LixcolSchemaKey => {
                string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
            }
            FileSurfaceColumn::LixcolFileId => {
                string_array(rows.iter().map(|row| row.lixcol_file_id.as_deref()))
            }
            FileSurfaceColumn::LixcolVersionId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
            }
            FileSurfaceColumn::LixcolPluginKey => {
                string_array(rows.iter().map(|row| row.lixcol_plugin_key.as_deref()))
            }
            FileSurfaceColumn::LixcolSchemaVersion => string_array(
                rows.iter()
                    .map(|row| Some(row.lixcol_schema_version.as_str())),
            ),
            FileSurfaceColumn::LixcolGlobal => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.lixcol_global).collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolChangeId => {
                string_array(rows.iter().map(|row| row.lixcol_change_id.as_deref()))
            }
            FileSurfaceColumn::LixcolCreatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_created_at.as_deref()))
            }
            FileSurfaceColumn::LixcolUpdatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_updated_at.as_deref()))
            }
            FileSurfaceColumn::LixcolCommitId => {
                string_array(rows.iter().map(|row| row.lixcol_commit_id.as_deref()))
            }
            FileSurfaceColumn::LixcolUntracked => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.lixcol_untracked)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolMetadata => {
                string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(file_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_file batch: {error}"))
    })
}

fn file_surface_schema(projection: &[FileSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                FileSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                FileSurfaceColumn::DirectoryId => Field::new("directory_id", DataType::Utf8, true),
                FileSurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                FileSurfaceColumn::Extension => Field::new("extension", DataType::Utf8, true),
                FileSurfaceColumn::Path => Field::new("path", DataType::Utf8, true),
                FileSurfaceColumn::Data => Field::new("data", DataType::Binary, true),
                FileSurfaceColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                FileSurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                FileSurfaceColumn::LixcolEntityId => {
                    Field::new("lixcol_entity_id", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolSchemaKey => {
                    Field::new("lixcol_schema_key", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolFileId => {
                    Field::new("lixcol_file_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolVersionId => {
                    Field::new("lixcol_version_id", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolPluginKey => {
                    Field::new("lixcol_plugin_key", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolSchemaVersion => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolGlobal => {
                    Field::new("lixcol_global", DataType::Boolean, false)
                }
                FileSurfaceColumn::LixcolChangeId => {
                    Field::new("lixcol_change_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolCreatedAt => {
                    Field::new("lixcol_created_at", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolUpdatedAt => {
                    Field::new("lixcol_updated_at", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolCommitId => {
                    Field::new("lixcol_commit_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolUntracked => {
                    Field::new("lixcol_untracked", DataType::Boolean, false)
                }
                FileSurfaceColumn::LixcolMetadata => {
                    Field::new("lixcol_metadata", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
fn file_history_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<String> {
    projection
        .map(|indices| {
            let schema = lix_file_history_schema(LixFileHistorySurfaceKind::FileHistory);
            indices
                .iter()
                .map(|index| schema.field(*index).name().to_string())
                .collect()
        })
        .unwrap_or_else(|| {
            lix_file_history_schema(LixFileHistorySurfaceKind::FileHistory)
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        })
}

#[cfg(test)]
fn file_history_record_batches(
    projection: Vec<String>,
    rows: &[FileHistoryRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![file_history_record_batch(&projection, rows)?])
}

#[cfg(test)]
fn file_history_record_batch(
    projection: &[String],
    rows: &[FileHistoryRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| {
            Ok(match column.as_str() {
                "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
                "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
                "data" => binary_array(rows.iter().map(|row| row.data.as_deref())),
                "metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
                "hidden" => Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
                )) as ArrayRef,
                "lixcol_entity_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
                }
                "lixcol_schema_key" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
                }
                "lixcol_file_id" => {
                    string_array(rows.iter().map(|row| row.lixcol_file_id.as_deref()))
                }
                "lixcol_version_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
                }
                "lixcol_plugin_key" => {
                    string_array(rows.iter().map(|row| row.lixcol_plugin_key.as_deref()))
                }
                "lixcol_schema_version" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_schema_version.as_str())),
                ),
                "lixcol_change_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_change_id.as_str())))
                }
                "lixcol_metadata" => {
                    string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
                }
                "lixcol_commit_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_commit_id.as_str())))
                }
                "lixcol_commit_created_at" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_commit_created_at.as_str())),
                ),
                "lixcol_root_commit_id" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_root_commit_id.as_str())),
                ),
                "lixcol_depth" => Arc::new(datafusion::arrow::array::Int64Array::from(
                    rows.iter().map(|row| row.lixcol_depth).collect::<Vec<_>>(),
                )) as ArrayRef,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "sql2 does not support lix_file_history column '{other}'"
                    )))
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(file_history_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 failed to build lix_file_history batch: {error}"
        ))
    })
}

#[cfg(test)]
fn file_history_schema(projection: &[String]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column.as_str() {
                "id" => Field::new("id", DataType::Utf8, false),
                "path" => Field::new("path", DataType::Utf8, true),
                "data" => Field::new("data", DataType::Binary, true),
                "metadata" => Field::new("metadata", DataType::Utf8, true),
                "hidden" => Field::new("hidden", DataType::Boolean, true),
                "lixcol_entity_id" => Field::new("lixcol_entity_id", DataType::Utf8, false),
                "lixcol_schema_key" => Field::new("lixcol_schema_key", DataType::Utf8, false),
                "lixcol_file_id" => Field::new("lixcol_file_id", DataType::Utf8, true),
                "lixcol_version_id" => Field::new("lixcol_version_id", DataType::Utf8, false),
                "lixcol_plugin_key" => Field::new("lixcol_plugin_key", DataType::Utf8, true),
                "lixcol_schema_version" => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                "lixcol_change_id" => Field::new("lixcol_change_id", DataType::Utf8, false),
                "lixcol_metadata" => Field::new("lixcol_metadata", DataType::Utf8, true),
                "lixcol_commit_id" => Field::new("lixcol_commit_id", DataType::Utf8, false),
                "lixcol_commit_created_at" => {
                    Field::new("lixcol_commit_created_at", DataType::Utf8, false)
                }
                "lixcol_root_commit_id" => {
                    Field::new("lixcol_root_commit_id", DataType::Utf8, false)
                }
                "lixcol_depth" => Field::new("lixcol_depth", DataType::Int64, false),
                other => panic!("unsupported lix_file_history schema column '{other}'"),
            })
            .collect::<Vec<_>>(),
    ))
}

fn file_projection_for_scan(
    surface_kind: LixFileSurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<FileSurfaceColumn> {
    let mut all_columns = vec![
        FileSurfaceColumn::Id,
        FileSurfaceColumn::DirectoryId,
        FileSurfaceColumn::Name,
        FileSurfaceColumn::Extension,
        FileSurfaceColumn::Path,
        FileSurfaceColumn::Data,
        FileSurfaceColumn::Metadata,
        FileSurfaceColumn::Hidden,
        FileSurfaceColumn::LixcolEntityId,
        FileSurfaceColumn::LixcolSchemaKey,
        FileSurfaceColumn::LixcolFileId,
    ];
    if surface_kind == LixFileSurfaceKind::FileByVersion {
        all_columns.push(FileSurfaceColumn::LixcolVersionId);
    }
    all_columns.extend([
        FileSurfaceColumn::LixcolPluginKey,
        FileSurfaceColumn::LixcolSchemaVersion,
        FileSurfaceColumn::LixcolGlobal,
        FileSurfaceColumn::LixcolChangeId,
        FileSurfaceColumn::LixcolCreatedAt,
        FileSurfaceColumn::LixcolUpdatedAt,
        FileSurfaceColumn::LixcolCommitId,
        FileSurfaceColumn::LixcolUntracked,
        FileSurfaceColumn::LixcolMetadata,
    ]);
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LixDirectorySurfaceKind {
    Directory,
    DirectoryByVersion,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LixDirectoryHistorySurfaceKind {
    DirectoryHistory,
}

fn lix_directory_schema(surface_kind: LixDirectorySurfaceKind) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
    ];
    if surface_kind == LixDirectorySurfaceKind::DirectoryByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields.extend([
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
fn lix_directory_history_schema(_surface_kind: LixDirectoryHistorySurfaceKind) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, true),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_version_id", DataType::Utf8, false),
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_change_id", DataType::Utf8, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, false),
        Field::new("lixcol_commit_created_at", DataType::Utf8, false),
        Field::new("lixcol_root_commit_id", DataType::Utf8, false),
        Field::new("lixcol_depth", DataType::Int64, false),
    ]))
}

#[derive(Debug, Clone)]
pub(crate) struct LixDirectoryProvider {
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct LixDirectoryHistoryProvider {
    surface_kind: LixDirectoryHistorySurfaceKind,
    schema: SchemaRef,
    rows: Arc<Vec<DirectoryHistoryRow>>,
}

#[cfg(test)]
impl LixDirectoryHistoryProvider {
    pub(crate) fn new(
        surface_kind: LixDirectoryHistorySurfaceKind,
        rows: Vec<DirectoryHistoryRow>,
    ) -> Self {
        Self {
            surface_kind,
            schema: lix_directory_history_schema(surface_kind),
            rows: Arc::new(rows),
        }
    }
}

impl LixDirectoryProvider {
    pub(crate) fn new(
        surface_kind: LixDirectorySurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: lix_directory_schema(surface_kind),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixDirectoryProvider {
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
                if parse_directory_route_filter(filter).is_some() {
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
        let route = LixDirectoryRoute::from_filters(filters);
        Ok(Arc::new(LixDirectoryScanExec::new(
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

#[cfg(test)]
#[async_trait]
impl TableProvider for LixDirectoryHistoryProvider {
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
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixDirectoryHistoryScanExec::new(
            self.surface_kind,
            Arc::clone(&self.rows),
            projected_schema,
            projection.cloned(),
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixDirectoryScanExec {
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixDirectoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixDirectoryScanExec {
    fn new(
        surface_kind: LixDirectorySurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn DirectorySurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixDirectoryRoute,
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

impl DisplayAs for LixDirectoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixDirectoryScanExec(limit={:?}, route={:?})",
                    self.limit, self.route
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryScanExec"),
        }
    }
}

impl ExecutionPlan for LixDirectoryScanExec {
    fn name(&self) -> &str {
        "LixDirectoryScanExec"
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
                "LixDirectoryScanExec does not accept children".to_string(),
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
                "LixDirectoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                let rows = enqueue_directory_surface_scan(
                    snapshot,
                    directory_surface_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    ),
                )
                .await?;
                directory_surface_record_batches(
                    directory_projection_for_scan(surface_kind, projection.as_ref()),
                    &rows,
                )?
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
struct DirectorySurfaceScanJob {
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    request: DirectorySurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<DirectorySurfaceRow>, LixError>>,
}

#[cfg(test)]
#[derive(Debug)]
struct LixDirectoryHistoryScanExec {
    surface_kind: LixDirectoryHistorySurfaceKind,
    rows: Arc<Vec<DirectoryHistoryRow>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

#[cfg(test)]
impl LixDirectoryHistoryScanExec {
    fn new(
        surface_kind: LixDirectoryHistorySurfaceKind,
        rows: Arc<Vec<DirectoryHistoryRow>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
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
            rows,
            schema,
            projection,
            limit,
            properties: Arc::new(properties),
        }
    }
}

#[cfg(test)]
impl DisplayAs for LixDirectoryHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixDirectoryHistoryScanExec(kind={:?}, limit={:?})",
                    self.surface_kind, self.limit
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryHistoryScanExec"),
        }
    }
}

#[cfg(test)]
impl ExecutionPlan for LixDirectoryHistoryScanExec {
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

        let projection = self.projection.clone();
        let limit = self.limit;
        let rows = Arc::clone(&self.rows);
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let truncated = limit
                .map(|limit| rows.iter().take(limit).cloned().collect::<Vec<_>>())
                .unwrap_or_else(|| rows.as_ref().clone());
            let batches = directory_history_record_batches(
                directory_history_projection_for_scan(projection.as_ref()),
                &truncated,
            )?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn directory_surface_scan_worker() -> &'static mpsc::Sender<DirectorySurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<DirectorySurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<DirectorySurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-directory-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 directory-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_directories(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 directory-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_directory_surface_scan(
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    request: DirectorySurfaceScanRequest,
) -> Result<Vec<DirectorySurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    directory_surface_scan_worker()
        .send(DirectorySurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue directory surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution(
                "sql2 directory surface scan worker dropped reply".to_string(),
            )
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn directory_surface_record_batches(
    projection: Vec<DirectorySurfaceColumn>,
    rows: &[DirectorySurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![directory_surface_record_batch(&projection, rows)?])
}

fn directory_surface_record_batch(
    projection: &[DirectorySurfaceColumn],
    rows: &[DirectorySurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            DirectorySurfaceColumn::Id => {
                string_array(rows.iter().map(|row| Some(row.id.as_str())))
            }
            DirectorySurfaceColumn::ParentId => {
                string_array(rows.iter().map(|row| row.parent_id.as_deref()))
            }
            DirectorySurfaceColumn::Name => {
                string_array(rows.iter().map(|row| Some(row.name.as_str())))
            }
            DirectorySurfaceColumn::Path => {
                string_array(rows.iter().map(|row| row.path.as_deref()))
            }
            DirectorySurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolEntityId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
            }
            DirectorySurfaceColumn::LixcolSchemaKey => {
                string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
            }
            DirectorySurfaceColumn::LixcolVersionId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
            }
            DirectorySurfaceColumn::LixcolSchemaVersion => string_array(
                rows.iter()
                    .map(|row| Some(row.lixcol_schema_version.as_str())),
            ),
            DirectorySurfaceColumn::LixcolGlobal => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.lixcol_global).collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolChangeId => {
                string_array(rows.iter().map(|row| row.lixcol_change_id.as_deref()))
            }
            DirectorySurfaceColumn::LixcolCreatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_created_at.as_deref()))
            }
            DirectorySurfaceColumn::LixcolUpdatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_updated_at.as_deref()))
            }
            DirectorySurfaceColumn::LixcolCommitId => {
                string_array(rows.iter().map(|row| row.lixcol_commit_id.as_deref()))
            }
            DirectorySurfaceColumn::LixcolUntracked => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.lixcol_untracked)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolMetadata => {
                string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(directory_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_directory batch: {error}"))
    })
}

fn directory_surface_schema(projection: &[DirectorySurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                DirectorySurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                DirectorySurfaceColumn::ParentId => Field::new("parent_id", DataType::Utf8, true),
                DirectorySurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                DirectorySurfaceColumn::Path => Field::new("path", DataType::Utf8, true),
                DirectorySurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                DirectorySurfaceColumn::LixcolEntityId => {
                    Field::new("lixcol_entity_id", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolSchemaKey => {
                    Field::new("lixcol_schema_key", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolVersionId => {
                    Field::new("lixcol_version_id", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolSchemaVersion => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolGlobal => {
                    Field::new("lixcol_global", DataType::Boolean, false)
                }
                DirectorySurfaceColumn::LixcolChangeId => {
                    Field::new("lixcol_change_id", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolCreatedAt => {
                    Field::new("lixcol_created_at", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolUpdatedAt => {
                    Field::new("lixcol_updated_at", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolCommitId => {
                    Field::new("lixcol_commit_id", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolUntracked => {
                    Field::new("lixcol_untracked", DataType::Boolean, false)
                }
                DirectorySurfaceColumn::LixcolMetadata => {
                    Field::new("lixcol_metadata", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
fn directory_history_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<String> {
    projection
        .map(|indices| {
            let schema =
                lix_directory_history_schema(LixDirectoryHistorySurfaceKind::DirectoryHistory);
            indices
                .iter()
                .map(|index| schema.field(*index).name().to_string())
                .collect()
        })
        .unwrap_or_else(|| {
            lix_directory_history_schema(LixDirectoryHistorySurfaceKind::DirectoryHistory)
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect()
        })
}

#[cfg(test)]
fn directory_history_record_batches(
    projection: Vec<String>,
    rows: &[DirectoryHistoryRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![directory_history_record_batch(&projection, rows)?])
}

#[cfg(test)]
fn directory_history_record_batch(
    projection: &[String],
    rows: &[DirectoryHistoryRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| {
            Ok(match column.as_str() {
                "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
                "parent_id" => string_array(rows.iter().map(|row| row.parent_id.as_deref())),
                "name" => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
                "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
                "hidden" => Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
                )) as ArrayRef,
                "lixcol_entity_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
                }
                "lixcol_schema_key" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
                }
                "lixcol_file_id" => {
                    string_array(rows.iter().map(|row| row.lixcol_file_id.as_deref()))
                }
                "lixcol_version_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
                }
                "lixcol_plugin_key" => {
                    string_array(rows.iter().map(|row| row.lixcol_plugin_key.as_deref()))
                }
                "lixcol_schema_version" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_schema_version.as_str())),
                ),
                "lixcol_change_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_change_id.as_str())))
                }
                "lixcol_metadata" => {
                    string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
                }
                "lixcol_commit_id" => {
                    string_array(rows.iter().map(|row| Some(row.lixcol_commit_id.as_str())))
                }
                "lixcol_commit_created_at" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_commit_created_at.as_str())),
                ),
                "lixcol_root_commit_id" => string_array(
                    rows.iter()
                        .map(|row| Some(row.lixcol_root_commit_id.as_str())),
                ),
                "lixcol_depth" => Arc::new(datafusion::arrow::array::Int64Array::from(
                    rows.iter().map(|row| row.lixcol_depth).collect::<Vec<_>>(),
                )) as ArrayRef,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "sql2 does not support lix_directory_history column '{other}'"
                    )))
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(directory_history_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!(
            "sql2 failed to build lix_directory_history batch: {error}"
        ))
    })
}

#[cfg(test)]
fn directory_history_schema(projection: &[String]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column.as_str() {
                "id" => Field::new("id", DataType::Utf8, false),
                "parent_id" => Field::new("parent_id", DataType::Utf8, true),
                "name" => Field::new("name", DataType::Utf8, false),
                "path" => Field::new("path", DataType::Utf8, true),
                "hidden" => Field::new("hidden", DataType::Boolean, true),
                "lixcol_entity_id" => Field::new("lixcol_entity_id", DataType::Utf8, false),
                "lixcol_schema_key" => Field::new("lixcol_schema_key", DataType::Utf8, false),
                "lixcol_file_id" => Field::new("lixcol_file_id", DataType::Utf8, true),
                "lixcol_version_id" => Field::new("lixcol_version_id", DataType::Utf8, false),
                "lixcol_plugin_key" => Field::new("lixcol_plugin_key", DataType::Utf8, true),
                "lixcol_schema_version" => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                "lixcol_change_id" => Field::new("lixcol_change_id", DataType::Utf8, false),
                "lixcol_metadata" => Field::new("lixcol_metadata", DataType::Utf8, true),
                "lixcol_commit_id" => Field::new("lixcol_commit_id", DataType::Utf8, false),
                "lixcol_commit_created_at" => {
                    Field::new("lixcol_commit_created_at", DataType::Utf8, false)
                }
                "lixcol_root_commit_id" => {
                    Field::new("lixcol_root_commit_id", DataType::Utf8, false)
                }
                "lixcol_depth" => Field::new("lixcol_depth", DataType::Int64, false),
                other => panic!("unsupported lix_directory_history schema column '{other}'"),
            })
            .collect::<Vec<_>>(),
    ))
}

fn directory_projection_for_scan(
    surface_kind: LixDirectorySurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<DirectorySurfaceColumn> {
    let mut all_columns = vec![
        DirectorySurfaceColumn::Id,
        DirectorySurfaceColumn::ParentId,
        DirectorySurfaceColumn::Name,
        DirectorySurfaceColumn::Path,
        DirectorySurfaceColumn::Hidden,
        DirectorySurfaceColumn::LixcolEntityId,
        DirectorySurfaceColumn::LixcolSchemaKey,
    ];
    if surface_kind == LixDirectorySurfaceKind::DirectoryByVersion {
        all_columns.push(DirectorySurfaceColumn::LixcolVersionId);
    }
    all_columns.extend([
        DirectorySurfaceColumn::LixcolSchemaVersion,
        DirectorySurfaceColumn::LixcolGlobal,
        DirectorySurfaceColumn::LixcolChangeId,
        DirectorySurfaceColumn::LixcolCreatedAt,
        DirectorySurfaceColumn::LixcolUpdatedAt,
        DirectorySurfaceColumn::LixcolCommitId,
        DirectorySurfaceColumn::LixcolUntracked,
        DirectorySurfaceColumn::LixcolMetadata,
    ]);
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixFileRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl LixFileRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_file_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Hidden => &mut route.hidden,
                        RouteBooleanField::LixcolGlobal => &mut route.lixcol_global,
                        RouteBooleanField::LixcolUntracked => &mut route.lixcol_untracked,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::Path => &mut route.path,
                        RouteStringField::LixcolVersionId => &mut route.lixcol_version_id,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn file_filters(&self) -> Vec<FileSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(id) = &self.id {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Id,
                Value::Text(id.clone()),
            ));
        }
        if let Some(path) = &self.path {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Path,
                Value::Text(path.clone()),
            ));
        }
        if let Some(version_id) = &self.lixcol_version_id {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolVersionId,
                Value::Text(version_id.clone()),
            ));
        }
        if let Some(hidden) = self.hidden {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Hidden,
                Value::Boolean(hidden),
            ));
        }
        if let Some(global) = self.lixcol_global {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolGlobal,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.lixcol_untracked {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolUntracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixDirectoryRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl LixDirectoryRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_directory_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Hidden => &mut route.hidden,
                        RouteBooleanField::LixcolGlobal => &mut route.lixcol_global,
                        RouteBooleanField::LixcolUntracked => &mut route.lixcol_untracked,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::Path => &mut route.path,
                        RouteStringField::LixcolVersionId => &mut route.lixcol_version_id,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn directory_filters(&self) -> Vec<DirectorySurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(id) = &self.id {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Id,
                Value::Text(id.clone()),
            ));
        }
        if let Some(path) = &self.path {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Path,
                Value::Text(path.clone()),
            ));
        }
        if let Some(version_id) = &self.lixcol_version_id {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolVersionId,
                Value::Text(version_id.clone()),
            ));
        }
        if let Some(hidden) = self.hidden {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Hidden,
                Value::Boolean(hidden),
            ));
        }
        if let Some(global) = self.lixcol_global {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolGlobal,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.lixcol_untracked {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolUntracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

fn file_surface_scan_request(
    surface_kind: LixFileSurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixFileRoute,
    limit: Option<usize>,
) -> FileSurfaceScanRequest {
    let mut filters = route.file_filters();
    if surface_kind == LixFileSurfaceKind::File {
        filters.push(FileSurfaceFilter::Eq(
            FileSurfaceColumn::LixcolVersionId,
            Value::Text(default_version_id.to_string()),
        ));
    }
    FileSurfaceScanRequest {
        projection: file_projection_for_scan(surface_kind, projection),
        filters,
        limit,
    }
}

fn directory_surface_scan_request(
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixDirectoryRoute,
    limit: Option<usize>,
) -> DirectorySurfaceScanRequest {
    let mut filters = route.directory_filters();
    if surface_kind == LixDirectorySurfaceKind::Directory {
        filters.push(DirectorySurfaceFilter::Eq(
            DirectorySurfaceColumn::LixcolVersionId,
            Value::Text(default_version_id.to_string()),
        ));
    }
    DirectorySurfaceScanRequest {
        projection: directory_projection_for_scan(surface_kind, projection),
        filters,
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
pub(crate) enum RoutePredicate {
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
pub(crate) enum RouteBooleanField {
    Hidden,
    LixcolGlobal,
    LixcolUntracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteStringField {
    LixcolVersionId,
    Id,
    Path,
}

pub(crate) fn parse_file_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    parse_file_route_predicate(expr)
}

pub(crate) fn parse_directory_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    parse_directory_route_predicate(expr)
}

fn parse_file_route_predicate(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_file_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_file_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_directory_route_predicate(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_directory_route_column_literal_filter(&binary_expr.left, &binary_expr.right).or_else(
        || parse_directory_route_column_literal_filter(&binary_expr.right, &binary_expr.left),
    )
}

fn parse_directory_route_column_literal_filter(
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
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_file_route_column_literal_filter(
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
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
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
        DataFusionError::Execution(format!("sql2 failed to project filesystem schema: {error}"))
    })?;
    Ok(Arc::new(projected))
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("{error:?}"))
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(
        values
            .map(|value| value.map(ToOwned::to_owned))
            .collect::<Vec<_>>(),
    ))
}

fn binary_array<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> ArrayRef {
    Arc::new(BinaryArray::from(values.collect::<Vec<_>>()))
}
