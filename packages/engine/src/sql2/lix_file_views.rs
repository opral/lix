use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, RecordBatchOptions, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
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
use serde::Deserialize;

use crate::binary_cas::BlobDataReader;
use crate::live_state::{
    LiveRow, LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateScanRequest,
};
use crate::LixError;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(crate) async fn register_lix_file_views(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_by_version",
            Arc::new(LixFileProvider::by_version(
                Arc::clone(&live_state),
                Arc::clone(&blob_reader),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_file",
            Arc::new(LixFileProvider::active_version(
                active_version_id,
                live_state,
                blob_reader,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixFileProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    default_version_id: Option<String>,
}

impl std::fmt::Debug for LixFileProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileProvider").finish()
    }
}

impl LixFileProvider {
    pub(crate) fn active_version(
        active_version_id: impl Into<String>,
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            blob_reader,
            default_version_id: Some(active_version_id.into()),
        }
    }

    pub(crate) fn by_version(
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
    ) -> Self {
        Self {
            schema: lix_file_by_version_schema(),
            live_state,
            blob_reader,
            default_version_id: None,
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
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    BLOB_REF_SCHEMA_KEY.to_string(),
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                ],
                version_ids: self
                    .default_version_id
                    .clone()
                    .into_iter()
                    .collect::<Vec<_>>(),
                ..LiveStateFilter::default()
            },
            projection: LiveStateProjection::default(),
            limit,
        };
        Ok(Arc::new(LixFileScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.blob_reader),
            projected_schema,
            request,
        )))
    }
}

struct LixFileScanExec {
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    schema: SchemaRef,
    request: LiveStateScanRequest,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixFileScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileScanExec").finish()
    }
}

impl LixFileScanExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        schema: SchemaRef,
        request: LiveStateScanRequest,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            blob_reader,
            schema,
            request,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixFileScanExec(limit={:?})", self.request.limit)
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
                "LixFileScanExec only supports partition 0, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let blob_reader = Arc::clone(&self.blob_reader);
        let request = self.request.clone();
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&schema);
        let fut = async move {
            let rows = live_state.scan(&request).await.map_err(|error| {
                DataFusionError::Execution(format!("sql2 lix_file scan failed: {error}"))
            })?;
            let batch = lix_file_record_batch(&batch_schema, &blob_reader, rows)
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!("sql2 lix_file batch build failed: {error}"))
                })?;
            Ok::<RecordBatch, DataFusionError>(batch)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut).map_ok(|batch| batch),
        )))
    }
}

#[derive(Debug, Clone)]
struct FileDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    live: LiveRow,
}

#[derive(Debug, Clone)]
struct BlobRefRecord {
    blob_hash: String,
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    version_id: String,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
}

async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    rows: Vec<LiveRow>,
) -> Result<RecordBatch, LixError> {
    let projected_columns = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    let needs_data = projected_columns
        .iter()
        .any(|column_name| *column_name == "data");

    let mut file_rows = BTreeMap::<(String, String), FileDescriptorRecord>::new();
    let mut blob_rows = BTreeMap::<(String, String), BlobRefRecord>::new();
    let mut directory_rows = Vec::<DirectoryDescriptorRecord>::new();

    for row in rows {
        match row.schema_key.as_str() {
            FILE_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                file_rows.insert(
                    (row.version_id.clone(), snapshot.id.clone()),
                    FileDescriptorRecord {
                        id: snapshot.id,
                        directory_id: snapshot.directory_id,
                        name: snapshot.name,
                        extension: snapshot.extension,
                        hidden: snapshot.hidden,
                        live: row,
                    },
                );
            }
            BLOB_REF_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: BlobRefSnapshot =
                    serde_json::from_str(snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                        )
                    })?;
                blob_rows.insert(
                    (row.version_id.clone(), snapshot.id.clone()),
                    BlobRefRecord {
                        blob_hash: snapshot.blob_hash,
                    },
                );
            }
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                    continue;
                };
                let snapshot: DirectoryDescriptorSnapshot = serde_json::from_str(snapshot_content)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                directory_rows.push(DirectoryDescriptorRecord {
                    id: snapshot.id,
                    parent_id: snapshot.parent_id,
                    name: snapshot.name,
                    version_id: row.version_id,
                });
            }
            _ => {}
        }
    }

    let directory_paths = derive_directory_paths(&directory_rows);
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut directory_ids = Vec::new();
    let mut names = Vec::new();
    let mut extensions = Vec::new();
    let mut hiddens = Vec::new();
    let mut data_values = Vec::new();
    let mut entity_ids = Vec::new();
    let mut schema_keys = Vec::new();
    let mut file_ids = Vec::new();
    let mut plugin_keys = Vec::new();
    let mut schema_versions = Vec::new();
    let mut globals = Vec::new();
    let mut change_ids = Vec::new();
    let mut created_ats = Vec::new();
    let mut updated_ats = Vec::new();
    let mut commit_ids = Vec::new();
    let mut untracked_values = Vec::new();
    let mut metadata_values = Vec::new();
    let mut version_ids = Vec::new();

    for ((version_id, _), file) in file_rows {
        let directory_path = file.directory_id.as_ref().and_then(|directory_id| {
            directory_paths
                .get(&(version_id.clone(), directory_id.clone()))
                .cloned()
        });
        let filename = match file.extension.as_deref() {
            Some(extension) if !extension.is_empty() => format!("{}.{}", file.name, extension),
            _ => file.name.clone(),
        };
        let path = match directory_path {
            Some(directory_path) => format!("{directory_path}{filename}"),
            None => format!("/{filename}"),
        };
        let data = if needs_data {
            match blob_rows.get(&(version_id.clone(), file.id.clone())) {
                Some(blob_ref) => {
                    blob_reader
                        .load_blob_data_by_hash(&blob_ref.blob_hash)
                        .await?
                }
                None => None,
            }
        } else {
            None
        };

        ids.push(Some(file.id));
        paths.push(Some(path));
        directory_ids.push(file.directory_id);
        names.push(Some(file.name));
        extensions.push(file.extension);
        hiddens.push(Some(file.hidden));
        data_values.push(data);
        entity_ids.push(Some(file.live.entity_id));
        schema_keys.push(Some(file.live.schema_key));
        file_ids.push(file.live.file_id);
        plugin_keys.push(file.live.plugin_key);
        schema_versions.push(Some(file.live.schema_version));
        globals.push(Some(file.live.global));
        change_ids.push(file.live.change_id);
        created_ats.push(file.live.created_at);
        updated_ats.push(file.live.updated_at);
        commit_ids.push(file.live.commit_id);
        untracked_values.push(Some(file.live.untracked));
        metadata_values.push(file.live.metadata);
        version_ids.push(Some(version_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "directory_id" => Arc::new(StringArray::from(directory_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "extension" => Arc::new(StringArray::from(extensions.clone())),
            "hidden" => Arc::new(BooleanArray::from(hiddens.clone())),
            "data" => Arc::new(BinaryArray::from(
                data_values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>(),
            )),
            "lixcol_entity_id" => Arc::new(StringArray::from(entity_ids.clone())),
            "lixcol_schema_key" => Arc::new(StringArray::from(schema_keys.clone())),
            "lixcol_file_id" => Arc::new(StringArray::from(file_ids.clone())),
            "lixcol_plugin_key" => Arc::new(StringArray::from(plugin_keys.clone())),
            "lixcol_schema_version" => Arc::new(StringArray::from(schema_versions.clone())),
            "lixcol_global" => Arc::new(BooleanArray::from(globals.clone())),
            "lixcol_change_id" => Arc::new(StringArray::from(change_ids.clone())),
            "lixcol_created_at" => Arc::new(StringArray::from(created_ats.clone())),
            "lixcol_updated_at" => Arc::new(StringArray::from(updated_ats.clone())),
            "lixcol_commit_id" => Arc::new(StringArray::from(commit_ids.clone())),
            "lixcol_untracked" => Arc::new(BooleanArray::from(untracked_values.clone())),
            "lixcol_metadata" => Arc::new(StringArray::from(metadata_values.clone())),
            "lixcol_version_id" => Arc::new(StringArray::from(version_ids.clone())),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("sql2 lix_file provider does not support projected column '{other}'"),
                ))
            }
        };
        columns.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_file record batch: {error}"),
        )
    })
}

fn derive_directory_paths(
    rows: &[DirectoryDescriptorRecord],
) -> BTreeMap<(String, String), String> {
    let mut by_version = BTreeMap::<String, BTreeMap<String, &DirectoryDescriptorRecord>>::new();
    for row in rows {
        by_version
            .entry(row.version_id.clone())
            .or_default()
            .insert(row.id.clone(), row);
    }

    let mut paths = BTreeMap::<(String, String), String>::new();
    for (version_id, records) in by_version {
        for directory_id in records.keys() {
            derive_directory_path_for(&version_id, directory_id, &records, &mut paths);
        }
    }
    paths
}

fn derive_directory_path_for(
    version_id: &str,
    directory_id: &str,
    records: &BTreeMap<String, &DirectoryDescriptorRecord>,
    paths: &mut BTreeMap<(String, String), String>,
) -> Option<String> {
    if let Some(path) = paths.get(&(version_id.to_string(), directory_id.to_string())) {
        return Some(path.clone());
    }
    let row = records.get(directory_id)?;
    let path = match row.parent_id.as_deref() {
        Some(parent_id) => {
            let parent_path = derive_directory_path_for(version_id, parent_id, records, paths)?;
            format!("{parent_path}{}/", row.name)
        }
        None => format!("/{}/", row.name),
    };
    paths.insert(
        (version_id.to_string(), directory_id.to_string()),
        path.clone(),
    );
    Some(path)
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

fn lix_file_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("extension", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("data", DataType::Binary, true),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
    ]))
}

fn lix_file_by_version_schema() -> SchemaRef {
    let mut fields = lix_file_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{derive_directory_path_for, DirectoryDescriptorRecord};
    use std::collections::BTreeMap;

    #[test]
    fn derives_nested_directory_paths() {
        let root = DirectoryDescriptorRecord {
            id: "dir-docs".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            version_id: "version-a".to_string(),
        };
        let child = DirectoryDescriptorRecord {
            id: "dir-guides".to_string(),
            parent_id: Some("dir-docs".to_string()),
            name: "guides".to_string(),
            version_id: "version-a".to_string(),
        };
        let mut records = BTreeMap::new();
        records.insert(root.id.clone(), &root);
        records.insert(child.id.clone(), &child);
        let mut paths = BTreeMap::new();

        assert_eq!(
            derive_directory_path_for("version-a", "dir-guides", &records, &mut paths),
            Some("/docs/guides/".to_string())
        );
    }
}
