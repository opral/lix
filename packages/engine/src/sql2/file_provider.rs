use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, RecordBatchOptions, StringArray, UInt64Array,
};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{create_physical_expr, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::Deserialize;

use crate::binary_cas::BlobDataReader;
use crate::engine2::functions::FunctionProviderHandle;
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::engine2::transaction::types::StageRow;
use crate::engine2::version_ref::VersionRefReader;
use crate::sql2::version_scope::resolve_provider_version_ids;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use super::filesystem_planner::{
    blob_ref_row, directory_path_resolvers_from_state_rows, file_descriptor_row, plan_file_delete,
    plan_file_path_update, BlobRefRowInput, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FilePathWriteInput, FilesystemDeletePlan, FilesystemRowContext,
};
use crate::engine2::transaction::types::{StageFileData, StageWrite, StageWriteStager};

pub(crate) async fn register_lix_file_providers(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Option<Arc<dyn StageWriteStager>>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_by_version",
            Arc::new(LixFileProvider::by_version(
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
                Arc::clone(&blob_reader),
                write_stager.as_ref().map(Arc::clone),
                functions.clone(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_file",
            Arc::new(LixFileProvider::active_version(
                active_version_id,
                live_state,
                version_ref,
                Arc::clone(&blob_reader),
                write_stager,
                functions,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixFileProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Option<Arc<dyn StageWriteStager>>,
    functions: FunctionProviderHandle,
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
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Option<Arc<dyn StageWriteStager>>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_stager,
            functions,
            default_version_id: Some(active_version_id.into()),
        }
    }

    pub(crate) fn by_version(
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Option<Arc<dyn StageWriteStager>>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_by_version_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_stager,
            functions,
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
        let mut request = lix_file_scan_request(self.default_version_id.as_deref(), limit);
        request.filter.version_ids = resolve_provider_version_ids(
            self.version_ref.as_ref(),
            self.default_version_id.as_deref(),
            request.filter.version_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        Ok(Arc::new(LixFileScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.blob_reader),
            projected_schema,
            request,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return not_impl_err!("{insert_op} not implemented for lix_file yet");
        }

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "INSERT into lix_file requires a write transaction".to_string(),
            ));
        };

        let sink = LixFileInsertSink::new(
            input.schema(),
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            self.functions.clone(),
            self.default_version_id.clone(),
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "DELETE FROM lix_file requires a write transaction".to_string(),
            ));
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let request = lix_file_scan_request(self.default_version_id.as_deref(), None);

        Ok(Arc::new(LixFileDeleteExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.blob_reader),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            self.default_version_id.clone(),
            request,
            physical_filters,
        )))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_file requires a write transaction".to_string(),
            ));
        };

        validate_lix_file_update_assignments(&self.schema, &assignments)?;

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_assignments = assignments
            .iter()
            .map(|(column_name, expr)| {
                Ok((
                    column_name.clone(),
                    create_physical_expr(expr, &df_schema, state.execution_props())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let request = lix_file_scan_request(self.default_version_id.as_deref(), None);

        Ok(Arc::new(LixFileUpdateExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.blob_reader),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            self.default_version_id.clone(),
            self.functions.clone(),
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixFileInsertSink {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    write_stager: Arc<dyn StageWriteStager>,
    functions: FunctionProviderHandle,
    default_version_id: Option<String>,
}

impl std::fmt::Debug for LixFileInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileInsertSink").finish()
    }
}

impl LixFileInsertSink {
    fn new(
        schema: SchemaRef,
        live_state: Arc<dyn LiveStateReader>,
        write_stager: Arc<dyn StageWriteStager>,
        functions: FunctionProviderHandle,
        default_version_id: Option<String>,
    ) -> Self {
        Self {
            schema,
            live_state,
            write_stager,
            functions,
            default_version_id,
        }
    }
}

impl DisplayAs for LixFileInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixFileInsertSink")
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileInsertSink"),
        }
    }
}

#[async_trait]
impl DataSink for LixFileInsertSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut staged = LixFileStagedBatch::default();
        let mut path_resolvers = None;
        while let Some(batch) = data.next().await.transpose()? {
            if record_batch_has_non_null_column(&batch, "path")? {
                if path_resolvers.is_none() {
                    // TODO(engine2): make transaction-scoped live-state reads
                    // use transaction-owned read services instead of requiring
                    // the live-state layer to open a separate backend read.
                    path_resolvers = Some(
                        file_path_resolvers_from_live_state(
                            Arc::clone(&self.live_state),
                            self.default_version_id.as_deref(),
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?,
                    );
                }
                staged.extend(lix_file_insert_stage_from_batch_with_path_resolvers(
                    &batch,
                    self.default_version_id.as_deref(),
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7(),
                )?);
            } else {
                staged.extend(lix_file_insert_stage_from_batch(
                    &batch,
                    self.default_version_id.as_deref(),
                )?);
            }
        }

        if !staged.state_rows.is_empty() || !staged.file_data_writes.is_empty() {
            let intent = if staged.file_data_writes.is_empty() {
                StageWrite::Rows {
                    rows: staged.state_rows,
                }
            } else {
                StageWrite::RowsWithFileData {
                    rows: staged.state_rows,
                    file_data: staged.file_data_writes,
                    count: staged.count,
                }
            };
            self.write_stager
                .stage_write(intent)
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }

        Ok(staged.count)
    }
}

struct LixFileDeleteExec {
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Arc<dyn StageWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixFileDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileDeleteExec").finish()
    }
}

impl LixFileDeleteExec {
    fn new(
        live_state: Arc<dyn LiveStateReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Arc<dyn StageWriteStager>,
        table_schema: SchemaRef,
        default_version_id: Option<String>,
        request: LiveStateScanRequest,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            blob_reader,
            write_stager,
            table_schema,
            default_version_id,
            request,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixFileDeleteExec(filters={})", self.filters.len())
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileDeleteExec"),
        }
    }
}

impl ExecutionPlan for LixFileDeleteExec {
    fn name(&self) -> &str {
        "LixFileDeleteExec"
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
                "LixFileDeleteExec does not accept children".to_string(),
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
                "LixFileDeleteExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let blob_reader = Arc::clone(&self.blob_reader);
        let write_stager = Arc::clone(&self.write_stager);
        let table_schema = Arc::clone(&self.table_schema);
        let default_version_id = self.default_version_id.clone();
        let request = self.request.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = live_state
                .scan_rows(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let blob_ref_file_ids =
                blob_ref_file_ids_from_live_rows(&rows).map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_file_record_batch(&table_schema, &blob_reader, rows)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_file_batch(source_batch, &filters)?;
            let staged = lix_file_delete_stage_from_batch(
                &matched_batch,
                default_version_id.as_deref(),
                &blob_ref_file_ids,
            )?;
            let count = staged.count;

            if count > 0 {
                write_stager
                    .stage_write(StageWrite::Rows {
                        rows: staged.state_rows,
                    })
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            }

            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                dml_count_batch(Arc::clone(&stream_schema), count)?,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

struct LixFileUpdateExec {
    live_state: Arc<dyn LiveStateReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Arc<dyn StageWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
    functions: FunctionProviderHandle,
    request: LiveStateScanRequest,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixFileUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileUpdateExec").finish()
    }
}

impl LixFileUpdateExec {
    fn new(
        live_state: Arc<dyn LiveStateReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Arc<dyn StageWriteStager>,
        table_schema: SchemaRef,
        default_version_id: Option<String>,
        functions: FunctionProviderHandle,
        request: LiveStateScanRequest,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            blob_reader,
            write_stager,
            table_schema,
            default_version_id,
            functions,
            request,
            assignments,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixFileUpdateExec(assignments={}, filters={})",
                    self.assignments.len(),
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixFileUpdateExec"),
        }
    }
}

impl ExecutionPlan for LixFileUpdateExec {
    fn name(&self) -> &str {
        "LixFileUpdateExec"
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
                "LixFileUpdateExec does not accept children".to_string(),
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
                "LixFileUpdateExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let blob_reader = Arc::clone(&self.blob_reader);
        let write_stager = Arc::clone(&self.write_stager);
        let table_schema = Arc::clone(&self.table_schema);
        let default_version_id = self.default_version_id.clone();
        let functions = self.functions.clone();
        let request = self.request.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = live_state
                .scan_rows(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_file_record_batch(&table_schema, &blob_reader, rows)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_file_batch(source_batch, &filters)?;
            let updated_batch =
                apply_lix_file_update_assignments(&table_schema, matched_batch, &assignments)?;
            let update_columns = LixFileUpdateColumns::from_assignments(&assignments);
            let mut path_resolvers = None;
            if update_columns.path {
                // TODO(engine2): make this resolver transaction-scoped so all
                // filesystem DML shares one directory identity cache.
                path_resolvers = Some(
                    file_path_resolvers_from_live_state(
                        Arc::clone(&live_state),
                        default_version_id.as_deref(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            let staged = lix_file_update_stage_from_batch(
                &updated_batch,
                default_version_id.as_deref(),
                update_columns,
                path_resolvers.as_mut(),
                &mut || functions.call_uuid_v7(),
            )?;
            let count = staged.count;

            if count > 0 {
                let intent = if staged.file_data_writes.is_empty() {
                    StageWrite::Rows {
                        rows: staged.state_rows,
                    }
                } else {
                    StageWrite::RowsWithFileData {
                        rows: staged.state_rows,
                        file_data: staged.file_data_writes,
                        count,
                    }
                };
                write_stager
                    .stage_write(intent)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            }

            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                dml_count_batch(Arc::clone(&stream_schema), count)?,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

struct LixFileScanExec {
    live_state: Arc<dyn LiveStateReader>,
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
        live_state: Arc<dyn LiveStateReader>,
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
            let rows = live_state.scan_rows(&request).await.map_err(|error| {
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
    live: LiveStateRow,
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

#[derive(Debug, Default)]
struct LixFileStagedBatch {
    state_rows: Vec<StageRow>,
    file_data_writes: Vec<StageFileData>,
    count: u64,
}

impl LixFileStagedBatch {
    fn extend(&mut self, other: LixFileStagedBatch) {
        self.state_rows.extend(other.state_rows);
        self.file_data_writes.extend(other.file_data_writes);
        self.count += other.count;
    }

    fn extend_filesystem_plan(&mut self, plan: super::filesystem_planner::FilesystemWritePlan) {
        self.state_rows.extend(plan.rows);
        self.file_data_writes.extend(plan.file_data);
        self.count += plan.count;
    }

    fn extend_filesystem_delete_plan(&mut self, plan: FilesystemDeletePlan) {
        self.state_rows.extend(plan.rows);
        self.count += plan.count;
    }
}

#[cfg(test)]
fn lix_file_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<StageRow>> {
    Ok(lix_file_insert_stage_from_batch(batch, default_version_id)?.state_rows)
}

fn lix_file_delete_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    blob_ref_file_ids: &BTreeSet<String>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let context = file_row_context_from_batch(batch, row_index, default_version_id)?;
        staged.extend_filesystem_delete_plan(plan_file_delete(FileDeleteInput {
            file_id: file_id.clone(),
            has_blob_ref: blob_ref_file_ids.contains(&file_id),
            context,
        }));
    }
    Ok(staged)
}

fn blob_ref_file_ids_from_live_rows(
    rows: &[LiveStateRow],
) -> std::result::Result<BTreeSet<String>, LixError> {
    let mut file_ids = BTreeSet::new();
    for row in rows {
        if row.schema_key != BLOB_REF_SCHEMA_KEY {
            continue;
        }
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
        file_ids.insert(snapshot.id);
    }
    Ok(file_ids)
}

fn lix_file_insert_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options(batch, default_version_id, true, true, true)
}

fn lix_file_insert_stage_from_batch_with_path_resolvers(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        default_version_id,
        true,
        true,
        true,
        Some(path_resolvers),
        Some(generate_directory_id),
    )
}

fn lix_file_existing_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?.unwrap_or(false);
        let context = file_row_context_from_batch(batch, row_index, default_version_id)?;

        if include_descriptor_writes {
            staged
                .state_rows
                .push(file_descriptor_row(FileDescriptorRowInput {
                    id: id.clone(),
                    directory_id: optional_string_value(batch, row_index, "directory_id")?,
                    name: required_string_value(batch, row_index, "name")?,
                    extension: optional_string_value(batch, row_index, "extension")?,
                    hidden,
                    context: context.clone(),
                }));
        }

        if include_data_writes {
            if let Some(data) = optional_binary_value(batch, row_index, "data")? {
                stage_lix_file_data_write(&mut staged, id, data, context)?;
            }
        }

        staged.count = staged
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("lix_file row count overflow".into()))?;
    }

    Ok(staged)
}

#[derive(Debug, Clone, Copy)]
struct LixFileUpdateColumns {
    path: bool,
    data: bool,
    descriptor: bool,
}

impl LixFileUpdateColumns {
    fn from_assignments(assignments: &[(String, Arc<dyn PhysicalExpr>)]) -> Self {
        let path = assignments
            .iter()
            .any(|(column_name, _)| column_name == "path");
        let data = assignments
            .iter()
            .any(|(column_name, _)| column_name == "data");
        let descriptor = assignments
            .iter()
            .any(|(column_name, _)| column_name != "path" && column_name != "data");
        Self {
            path,
            data,
            descriptor,
        }
    }
}

fn lix_file_update_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    update_columns: LixFileUpdateColumns,
    path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    if update_columns.path {
        let Some(path_resolvers) = path_resolvers else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_file with path requires directory path resolver".to_string(),
            ));
        };
        lix_file_path_update_stage_from_batch(
            batch,
            default_version_id,
            update_columns,
            path_resolvers,
            generate_directory_id,
        )
    } else {
        lix_file_existing_stage_from_batch(
            batch,
            default_version_id,
            update_columns.descriptor,
            update_columns.data,
        )
    }
}

fn lix_file_path_update_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    update_columns: LixFileUpdateColumns,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let path = required_string_value(batch, row_index, "path")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?.unwrap_or(false);
        let context = file_row_context_from_batch(batch, row_index, default_version_id)?;
        let existing_data = optional_binary_value(batch, row_index, "data")?;

        let resolver = path_resolvers
            .entry(file_path_resolver_key(&context))
            .or_insert_with(DirectoryPathResolver::default);
        let plan = plan_file_path_update(
            resolver,
            id.clone(),
            path,
            hidden,
            existing_data.clone(),
            context.clone(),
            generate_directory_id,
        )
        .map_err(lix_error_to_datafusion_error)?;
        staged.extend_filesystem_plan(plan);

        if update_columns.data {
            if let Some(data) = existing_data {
                stage_lix_file_data_write(&mut staged, id, data, context)?;
            }
        }
    }

    Ok(staged)
}

fn lix_file_stage_from_batch_with_options(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        default_version_id,
        reject_read_only_fields,
        include_descriptor_writes,
        include_data_writes,
        None,
        None,
    )
}

fn lix_file_stage_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
    mut path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    mut generate_directory_id: Option<&mut dyn FnMut() -> String>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_entity_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id = required_string_value(batch, row_index, "id")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?.unwrap_or(false);
        let context = file_row_context_from_batch(batch, row_index, default_version_id)?;
        let data = if include_data_writes {
            optional_binary_value(batch, row_index, "data")?
        } else {
            None
        };

        if let Some(path) = path {
            reject_read_only_lix_file_insert_field(batch, row_index, "directory_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "name")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "extension")?;

            let Some(path_resolvers) = path_resolvers.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_file with path requires directory path resolver".to_string(),
                ));
            };
            let resolver = path_resolvers
                .entry(file_path_resolver_key(&context))
                .or_insert_with(DirectoryPathResolver::default);
            let Some(generate_directory_id) = generate_directory_id.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_file with path requires directory id generator".to_string(),
                ));
            };
            let plan = super::filesystem_planner::plan_file_path_write(
                resolver,
                FilePathWriteInput {
                    id,
                    path,
                    data,
                    hidden,
                    context,
                },
                generate_directory_id,
            )
            .map_err(lix_error_to_datafusion_error)?;
            staged.extend_filesystem_plan(plan);
            continue;
        }

        let directory_id = optional_string_value(batch, row_index, "directory_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        let extension = optional_string_value(batch, row_index, "extension")?;

        if include_descriptor_writes {
            staged
                .state_rows
                .push(file_descriptor_row(FileDescriptorRowInput {
                    id: id.clone(),
                    directory_id: directory_id.clone(),
                    name: name.clone(),
                    extension: extension.clone(),
                    hidden,
                    context: context.clone(),
                }));
        }

        if let Some(data) = data {
            stage_lix_file_data_write(&mut staged, id, data, context)?;
        }
        staged.count = staged
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("lix_file row count overflow".into()))?;
    }

    Ok(staged)
}

fn stage_lix_file_data_write(
    staged: &mut LixFileStagedBatch,
    file_id: String,
    data: Vec<u8>,
    context: FilesystemRowContext,
) -> Result<()> {
    staged.state_rows.push(
        blob_ref_row(BlobRefRowInput {
            file_id: file_id.clone(),
            data: data.clone(),
            context: FilesystemRowContext {
                file_id: None,
                plugin_key: None,
                metadata: None,
                ..context.clone()
            },
        })
        .map_err(lix_error_to_datafusion_error)?,
    );
    staged.file_data_writes.push(StageFileData {
        file_id,
        version_id: context.version_id,
        untracked: context.untracked,
        data,
    });
    Ok(())
}

fn file_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    default_version_id: Option<&str>,
) -> Result<FilesystemRowContext> {
    let global = optional_bool_value(batch, row_index, "lixcol_global")?.unwrap_or(false);
    let version_id = if global {
        GLOBAL_VERSION_ID.to_string()
    } else {
        optional_string_value(batch, row_index, "lixcol_version_id")?
            .or_else(|| default_version_id.map(ToOwned::to_owned))
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "INSERT into lix_file_by_version requires lixcol_version_id".to_string(),
                )
            })?
    };

    Ok(FilesystemRowContext {
        version_id,
        global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        plugin_key: optional_string_value(batch, row_index, "lixcol_plugin_key")?,
        metadata: optional_string_value(batch, row_index, "lixcol_metadata")?,
    })
}

fn file_path_resolver_key(context: &FilesystemRowContext) -> String {
    // TODO(engine2): make this lane-aware if filesystem path uniqueness needs
    // to distinguish tracked/untracked/global rows inside the same version.
    context.version_id.clone()
}

async fn file_path_resolvers_from_live_state(
    live_state: Arc<dyn LiveStateReader>,
    default_version_id: Option<&str>,
) -> std::result::Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()],
                version_ids: default_version_id
                    .map(|version_id| vec![version_id.to_string()])
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
    if let Some(version_id) = default_version_id {
        resolvers
            .entry(version_id.to_string())
            .or_insert_with(DirectoryPathResolver::default);
    }
    Ok(resolvers)
}

async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    rows: Vec<LiveStateRow>,
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
        entity_ids.push(Some(file.live.entity_id.as_string()?));
        schema_keys.push(Some(file.live.schema_key));
        file_ids.push(file.live.file_id);
        plugin_keys.push(file.live.plugin_key);
        schema_versions.push(file.live.schema_version);
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

fn lix_file_scan_request(
    default_version_id: Option<&str>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            ],
            version_ids: default_version_id
                .map(|version_id| vec![version_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: LiveStateProjection::default(),
        limit,
    }
}

fn validate_lix_file_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_file failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "path" | "directory_id" | "name" | "extension" | "hidden" | "data" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_file cannot stage read-only column '{column_name}'"
            )));
        }
    }
    Ok(())
}

fn filter_lix_file_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_lix_file_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_lix_file_filters(
    batch: &RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut combined_mask: Option<BooleanArray> = None;
    for filter in filters {
        let result = filter.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("lix_file filter was not boolean".to_string())
            })?;
        let normalized = bool_array
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect::<BooleanArray>();
        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &normalized)?,
            None => normalized,
        });
    }
    Ok(combined_mask)
}

fn apply_lix_file_update_assignments(
    schema: &SchemaRef,
    batch: RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 || assignments.is_empty() {
        return Ok(batch);
    }

    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column_name = field.name();
        let original_column = batch.column_by_name(column_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "UPDATE lix_file source batch is missing column '{column_name}'"
            ))
        })?;
        let new_column = if let Some((_, assignment)) =
            assignments.iter().find(|(name, _)| name == column_name)
        {
            assignment.evaluate(&batch)?.into_array(batch.num_rows())?
        } else {
            Arc::clone(original_column)
        };
        columns.push(new_column);
    }

    RecordBatch::try_new(Arc::clone(schema), columns).map_err(DataFusionError::from)
}

fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn dml_count_batch(schema: SchemaRef, count: u64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
    )
    .map_err(DataFusionError::from)
}

fn record_batch_has_non_null_column(batch: &RecordBatch, column_name: &str) -> Result<bool> {
    for row_index in 0..batch.num_rows() {
        if optional_scalar_value(batch, row_index, column_name)?
            .is_some_and(|value| !value.is_null())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject_read_only_lix_file_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into lix_file cannot stage read-only column '{column_name}'"
        )));
    }
    Ok(())
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "INSERT into lix_file requires non-null text column '{column_name}'"
        ))
    })
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(ScalarValue::Null)
        | Some(ScalarValue::Utf8(None))
        | Some(ScalarValue::Utf8View(None))
        | Some(ScalarValue::LargeUtf8(None)) => Ok(None),
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_file expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None | Some(ScalarValue::Null) | Some(ScalarValue::Boolean(None)) => Ok(None),
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_file expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_binary_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<Vec<u8>>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(ScalarValue::Null)
        | Some(ScalarValue::Binary(None))
        | Some(ScalarValue::LargeBinary(None))
        | Some(ScalarValue::FixedSizeBinary(_, None)) => Ok(None),
        Some(ScalarValue::Binary(Some(value))) | Some(ScalarValue::LargeBinary(Some(value))) => {
            Ok(Some(value))
        }
        Some(ScalarValue::FixedSizeBinary(_, Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_file expected binary column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let column_index = match schema.index_of(column_name) {
        Ok(column_index) => column_index,
        Err(_) => return Ok(None),
    };
    if row_index >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "row index {row_index} out of bounds for lix_file batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_file column '{column_name}' at row {row_index}: {error}"
            ))
        })
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

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 lix_file provider error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BinaryArray, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::lit;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures_util::stream;
    use serde_json::Value as JsonValue;

    use crate::engine2::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::engine2::live_state::LiveStateRow;
    use crate::engine2::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};
    use crate::engine2::transaction::types::{StageWrite, StageWriteOutcome, StageWriteStager};
    use crate::LixError;

    use super::{
        derive_directory_path_for, lix_file_delete_stage_from_batch,
        lix_file_insert_stage_from_batch, lix_file_insert_stage_from_batch_with_path_resolvers,
        lix_file_write_rows_from_batch, DirectoryDescriptorRecord, LixFileInsertSink,
    };

    fn test_id_generator(ids: &'static [&'static str]) -> impl FnMut() -> String {
        let mut ids = ids.iter();
        move || ids.next().expect("test id should exist").to_string()
    }

    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct CapturingWriteStager {
        writes: std::sync::Mutex<Vec<StageWrite>>,
    }

    #[async_trait]
    impl StageWriteStager for CapturingWriteStager {
        async fn stage_write(&self, write: StageWrite) -> Result<StageWriteOutcome, LixError> {
            self.writes.lock().expect("writes lock").push(write);
            Ok(StageWriteOutcome { count: 0 })
        }
    }

    #[derive(Default)]
    struct RowsLiveStateReader {
        rows: Vec<LiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn live_directory_row(
        entity_id: &str,
        version_id: &str,
        snapshot_content: &str,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::engine2::entity_identity::EntityIdentity::from_string(entity_id)
                .expect("entity id should decode"),
            schema_key: super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn file_insert_batch(include_version: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("extension", DataType::Utf8, true),
            Field::new("hidden", DataType::Boolean, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let mut columns = vec![
            string_column(vec![Some("file-readme")]),
            string_column(vec![Some("dir-docs")]),
            string_column(vec![Some("readme")]),
            string_column(vec![Some("md")]),
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"file\"}")]),
        ];
        if include_version {
            fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("version-b")]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("file insert batch")
    }

    fn data_insert_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("directory_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("extension", DataType::Utf8, true),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_version_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("readme")]),
                string_column(vec![Some("md")]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("version-b")]),
            ],
        )
        .expect("file data batch")
    }

    fn path_data_insert_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_version_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("/docs/guides/readme.md")]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("version-b")]),
            ],
        )
        .expect("file path data batch")
    }

    fn path_update_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_version_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("/docs/renamed.md")]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                Arc::new(BinaryArray::from_vec(vec![b"hello"])) as ArrayRef,
                string_column(vec![Some("version-b")]),
            ],
        )
        .expect("file path update batch")
    }

    fn file_delete_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("lixcol_version_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("version-b")]),
            ],
        )
        .expect("file delete batch")
    }

    fn batch_stream(batch: RecordBatch) -> SendableRecordBatchStream {
        let schema = batch.schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        ))
    }

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

    #[test]
    fn decodes_file_insert_into_lix_state_write_row() {
        let batch = file_insert_batch(true, false);

        let rows = lix_file_write_rows_from_batch(&batch, None).expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("file-readme")));
        assert_eq!(rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(rows[0].version_id, "version-b");
        assert_eq!(rows[0].schema_version.as_str(), "1");
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"file\"}"));
        let snapshot: JsonValue =
            serde_json::from_str(rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme");
        assert_eq!(snapshot["extension"], "md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[test]
    fn active_file_insert_defaults_version_id() {
        let batch = file_insert_batch(false, false);

        let rows =
            lix_file_write_rows_from_batch(&batch, Some("version-a")).expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
    }

    #[test]
    fn by_version_file_insert_requires_version_id_for_non_global_rows() {
        let batch = file_insert_batch(false, false);

        let error =
            lix_file_write_rows_from_batch(&batch, None).expect_err("version id is required");

        assert!(
            error.to_string().contains("requires lixcol_version_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn file_update_accepts_path_assignment() {
        super::validate_lix_file_update_assignments(
            &super::lix_file_schema(),
            &[("path".to_string(), lit("/docs/renamed.md"))],
        )
        .expect("path should be writable for update");
    }

    #[test]
    fn file_path_update_stages_descriptor_from_new_path() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            "version-b".to_string(),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 0);
        assert_eq!(staged.state_rows.len(), 1);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue =
            serde_json::from_str(descriptor.snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed");
        assert_eq!(snapshot["extension"], "md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[test]
    fn file_path_update_preserves_existing_data_unless_data_is_assigned() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            "version-b".to_string(),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert!(
            staged.file_data_writes.is_empty(),
            "path-only update should not rewrite file data"
        );
        assert!(
            staged
                .state_rows
                .iter()
                .all(|row| row.schema_key != "lix_binary_blob_ref"),
            "path-only update should not rewrite the blob ref"
        );
    }

    #[tokio::test]
    async fn file_path_update_seeds_resolver_from_visible_directory_state() {
        let mut resolvers = super::file_path_resolvers_from_live_state(
            Arc::new(RowsLiveStateReader {
                rows: vec![live_directory_row(
                    "dir-docs",
                    "version-b",
                    "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
                )],
            }) as Arc<dyn LiveStateReader>,
            Some("version-b"),
        )
        .await
        .expect("directory state should seed path resolver");

        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert!(staged
            .state_rows
            .iter()
            .all(|row| row.schema_key != "lix_directory_descriptor"));

        let snapshot: JsonValue =
            serde_json::from_str(staged.state_rows[0].snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed");
        assert_eq!(snapshot["extension"], "md");
    }

    #[tokio::test]
    async fn file_path_update_stages_only_missing_parent_directories() {
        let mut resolvers = super::file_path_resolvers_from_live_state(
            Arc::new(RowsLiveStateReader::default()) as Arc<dyn LiveStateReader>,
            Some("version-b"),
        )
        .await
        .expect("empty directory state should seed path resolver");

        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: false,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["dir-generated-docs"]),
        )
        .expect("decode file path update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        assert_eq!(
            staged
                .state_rows
                .iter()
                .filter(|row| row.schema_key == "lix_directory_descriptor")
                .count(),
            1
        );

        let directory = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_directory_descriptor")
            .expect("missing /docs/ directory should be staged");
        assert_eq!(directory.entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("dir-generated-docs")));

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor should be staged");
        let snapshot: JsonValue =
            serde_json::from_str(descriptor.snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["directory_id"], "dir-generated-docs");
    }

    #[test]
    fn file_path_update_with_data_assignment_stages_blob_ref_and_payload() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            "version-b".to_string(),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: true,
                data: true,
                descriptor: false,
            },
            Some(&mut resolvers),
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path and data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].data, b"hello");
        assert!(staged
            .state_rows
            .iter()
            .any(|row| row.schema_key == "lix_file_descriptor"));
        assert!(staged
            .state_rows
            .iter()
            .any(|row| row.schema_key == "lix_binary_blob_ref"));
    }

    #[test]
    fn file_data_update_without_path_ignores_materialized_path_column() {
        let staged = super::lix_file_update_stage_from_batch(
            &path_update_batch(),
            None,
            super::LixFileUpdateColumns {
                path: false,
                data: true,
                descriptor: false,
            },
            None,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file data update");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_binary_blob_ref");
    }

    #[test]
    fn file_insert_stages_non_null_data() {
        let batch = data_insert_batch();

        let staged = lix_file_insert_stage_from_batch(&batch, None).expect("decode file data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        assert!(staged
            .state_rows
            .iter()
            .any(|row| row.schema_key == "lix_file_descriptor"));
        let blob_ref_row = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("data insert should stage blob ref row");
        assert_eq!(blob_ref_row.entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("file-readme")));
        assert_eq!(blob_ref_row.file_id.as_deref(), Some("file-readme"));
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].version_id, "version-b");
        assert_eq!(staged.file_data_writes[0].data, b"hello");
    }

    #[test]
    fn file_delete_with_blob_ref_stages_descriptor_and_blob_ref_tombstones() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(
            &batch,
            None,
            &BTreeSet::from(["file-readme".to_string()]),
        )
        .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 2);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor tombstone should be staged");
        assert_eq!(descriptor.entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("file-readme")));
        assert_eq!(descriptor.file_id, None);
        assert_eq!(descriptor.snapshot_content, None);

        let blob_ref = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be staged");
        assert_eq!(blob_ref.entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("file-readme")));
        assert_eq!(blob_ref.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref.snapshot_content, None);
    }

    #[test]
    fn file_delete_without_blob_ref_stages_only_descriptor_tombstone() {
        let batch = file_delete_batch();
        let staged = lix_file_delete_stage_from_batch(&batch, None, &BTreeSet::new())
            .expect("decode file delete");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.state_rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(
            staged.state_rows[0].entity_id.as_ref(),
            Some(&crate::engine2::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(staged.state_rows[0].snapshot_content, None);
    }

    #[test]
    fn file_path_insert_reuses_existing_parent_directory() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            "version-b".to_string(),
            super::DirectoryPathResolver::from_existing([
                ("/docs/".to_string(), "dir-docs".to_string()),
                ("/docs/guides/".to_string(), "dir-guides".to_string()),
            ])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("decode file path data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.state_rows.len(), 2);
        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue =
            serde_json::from_str(descriptor.snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-guides");
        assert_eq!(snapshot["name"], "readme");
        assert_eq!(snapshot["extension"], "md");
    }

    #[test]
    fn file_path_insert_stages_missing_parent_directories_once() {
        let mut resolvers = BTreeMap::new();

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            &mut resolvers,
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
        )
        .expect("decode file path data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 4);
        let directory_rows = staged
            .state_rows
            .iter()
            .filter(|row| row.schema_key == "lix_directory_descriptor")
            .collect::<Vec<_>>();
        assert_eq!(directory_rows.len(), 2);

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor row should be staged");
        let snapshot: JsonValue =
            serde_json::from_str(descriptor.snapshot_content.as_deref().unwrap())
                .expect("descriptor snapshot JSON");
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
    }

    #[tokio::test]
    async fn file_insert_sink_stages_decoded_lix_state_rows() {
        let batch = file_insert_batch(true, false);
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixFileInsertSink::new(
            batch.schema(),
            Arc::new(RowsLiveStateReader::default()) as Arc<dyn LiveStateReader>,
            Arc::clone(&stager) as Arc<dyn StageWriteStager>,
            test_functions(),
            None,
        );

        let count = sink
            .write_all(batch_stream(batch), &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage");

        assert_eq!(count, 1);
        let writes = stager.writes.lock().expect("writes lock");
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            StageWrite::Rows { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].entity_id.as_ref(), Some(&crate::engine2::entity_identity::EntityIdentity::single("file-readme")));
                assert_eq!(rows[0].schema_key, "lix_file_descriptor");
            }
            other => panic!("expected insert staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_stages_file_data_writes() {
        let batch = data_insert_batch();
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixFileInsertSink::new(
            batch.schema(),
            Arc::new(RowsLiveStateReader::default()) as Arc<dyn LiveStateReader>,
            Arc::clone(&stager) as Arc<dyn StageWriteStager>,
            test_functions(),
            None,
        );

        let count = sink
            .write_all(batch_stream(batch), &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage data");

        assert_eq!(count, 1);
        let writes = stager.writes.lock().expect("writes lock");
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            StageWrite::RowsWithFileData {
                rows,
                file_data,
                count,
            } => {
                assert_eq!(*count, 1);
                assert_eq!(rows.len(), 2);
                assert!(rows
                    .iter()
                    .any(|row| row.schema_key == "lix_file_descriptor"));
                assert!(rows
                    .iter()
                    .any(|row| row.schema_key == "lix_binary_blob_ref"));
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                assert_eq!(file_data[0].data, b"hello");
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_seeds_path_resolver_from_live_state() {
        let batch = path_data_insert_batch();
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixFileInsertSink::new(
            batch.schema(),
            Arc::new(RowsLiveStateReader {
                rows: vec![
                    live_directory_row(
                        "dir-docs",
                        "version-b",
                        "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\"}",
                    ),
                    live_directory_row(
                        "dir-guides",
                        "version-b",
                        "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\"}",
                    ),
                ],
            }) as Arc<dyn LiveStateReader>,
            Arc::clone(&stager) as Arc<dyn StageWriteStager>,
            test_functions(),
            None,
        );

        let count = sink
            .write_all(batch_stream(batch), &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage path data");

        assert_eq!(count, 1);
        let writes = stager.writes.lock().expect("writes lock");
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            StageWrite::RowsWithFileData {
                rows,
                file_data,
                count,
            } => {
                assert_eq!(*count, 1);
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                let descriptor = rows
                    .iter()
                    .find(|row| row.schema_key == "lix_file_descriptor")
                    .expect("file descriptor row should be staged");
                let snapshot: JsonValue =
                    serde_json::from_str(descriptor.snapshot_content.as_deref().unwrap())
                        .expect("descriptor snapshot JSON");
                assert_eq!(snapshot["directory_id"], "dir-guides");
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }
}
