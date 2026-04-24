use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatchOptions, StringArray, UInt64Array,
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
use serde_json::json;

use crate::binary_cas::BlobDataReader;
use crate::live_state::{
    LiveRow, LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateScanRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const FILE_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use crate::history::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRow,
};

use super::execute::{
    FileDataWrite, HistoryContext, LixStateWriteRow, SqlWriteIntent, SqlWriteStager,
};

pub(crate) async fn register_lix_file_providers(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    history: Option<Arc<dyn HistoryContext>>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_by_version",
            Arc::new(LixFileProvider::by_version(
                Arc::clone(&live_state),
                Arc::clone(&blob_reader),
                write_stager.as_ref().map(Arc::clone),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_file",
            Arc::new(LixFileProvider::active_version(
                active_version_id,
                live_state,
                Arc::clone(&blob_reader),
                write_stager,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    if let Some(history) = history {
        session
            .register_table(
                "lix_file_history",
                Arc::new(LixFileHistoryProvider::new(
                    active_version_id,
                    Arc::clone(&history),
                    Arc::clone(&blob_reader),
                    true,
                )),
            )
            .map_err(datafusion_error_to_lix_error)?;
        session
            .register_table(
                "lix_file_history_by_version",
                Arc::new(LixFileHistoryProvider::new(
                    active_version_id,
                    history,
                    blob_reader,
                    false,
                )),
            )
            .map_err(datafusion_error_to_lix_error)?;
    }
    Ok(())
}

pub(crate) struct LixFileProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
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
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            blob_reader,
            write_stager,
            default_version_id: Some(active_version_id.into()),
        }
    }

    pub(crate) fn by_version(
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_file_by_version_schema(),
            live_state,
            blob_reader,
            write_stager,
            default_version_id: None,
        }
    }
}

struct LixFileHistoryProvider {
    schema: SchemaRef,
    active_version_id: String,
    history: Arc<dyn HistoryContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    active_version_lineage: bool,
}

impl std::fmt::Debug for LixFileHistoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileHistoryProvider")
            .field("active_version_lineage", &self.active_version_lineage)
            .finish()
    }
}

impl LixFileHistoryProvider {
    fn new(
        active_version_id: impl Into<String>,
        history: Arc<dyn HistoryContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        active_version_lineage: bool,
    ) -> Self {
        Self {
            schema: lix_file_history_schema(),
            active_version_id: active_version_id.into(),
            history,
            blob_reader,
            active_version_lineage,
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
            self.active_version_id.clone(),
            Arc::clone(&self.history),
            Arc::clone(&self.blob_reader),
            projected_schema,
            self.active_version_lineage,
            limit,
        )))
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
        let request = lix_file_scan_request(self.default_version_id.as_deref(), limit);
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
            Arc::clone(write_stager),
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
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixFileInsertSink {
    schema: SchemaRef,
    write_stager: Arc<dyn SqlWriteStager>,
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
        write_stager: Arc<dyn SqlWriteStager>,
        default_version_id: Option<String>,
    ) -> Self {
        Self {
            schema,
            write_stager,
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
        while let Some(batch) = data.next().await.transpose()? {
            staged.extend(lix_file_insert_stage_from_batch(
                &batch,
                self.default_version_id.as_deref(),
            )?);
        }

        if !staged.state_rows.is_empty() || !staged.file_data_writes.is_empty() {
            let intent = if staged.file_data_writes.is_empty() {
                SqlWriteIntent::InsertLixState {
                    rows: staged.state_rows,
                }
            } else {
                SqlWriteIntent::InsertLixStateWithFileData {
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
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Arc<dyn SqlWriteStager>,
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
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Arc<dyn SqlWriteStager>,
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
                .scan(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_file_record_batch(&table_schema, &blob_reader, rows)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_file_batch(source_batch, &filters)?;
            let mut write_rows = lix_file_existing_write_rows_from_batch(
                &matched_batch,
                default_version_id.as_deref(),
            )?;
            for row in &mut write_rows {
                row.snapshot_content = None;
            }
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("lix_file DELETE row count overflow".into())
            })?;

            if count > 0 {
                write_stager
                    .stage_write(SqlWriteIntent::DeleteLixState { rows: write_rows })
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
    live_state: Arc<dyn LiveStateContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
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
        live_state: Arc<dyn LiveStateContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        write_stager: Arc<dyn SqlWriteStager>,
        table_schema: SchemaRef,
        default_version_id: Option<String>,
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
        let request = self.request.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = live_state
                .scan(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_file_record_batch(&table_schema, &blob_reader, rows)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_file_batch(source_batch, &filters)?;
            let updated_batch =
                apply_lix_file_update_assignments(&table_schema, matched_batch, &assignments)?;
            let include_data_writes = assignments
                .iter()
                .any(|(column_name, _)| column_name == "data");
            let include_descriptor_writes = assignments
                .iter()
                .any(|(column_name, _)| column_name != "data");
            let staged = lix_file_existing_stage_from_batch(
                &updated_batch,
                default_version_id.as_deref(),
                include_descriptor_writes,
                include_data_writes,
            )?;
            let count = staged.count;

            if count > 0 {
                let intent = if staged.file_data_writes.is_empty() {
                    SqlWriteIntent::InsertLixState {
                        rows: staged.state_rows,
                    }
                } else {
                    SqlWriteIntent::InsertLixStateWithFileData {
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

struct LixFileHistoryScanExec {
    active_version_id: String,
    history: Arc<dyn HistoryContext>,
    blob_reader: Arc<dyn BlobDataReader>,
    schema: SchemaRef,
    active_version_lineage: bool,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixFileHistoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileHistoryScanExec")
            .field("active_version_lineage", &self.active_version_lineage)
            .field("limit", &self.limit)
            .finish()
    }
}

impl LixFileHistoryScanExec {
    fn new(
        active_version_id: String,
        history: Arc<dyn HistoryContext>,
        blob_reader: Arc<dyn BlobDataReader>,
        schema: SchemaRef,
        active_version_lineage: bool,
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
            blob_reader,
            schema,
            active_version_lineage,
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
                "LixFileHistoryScanExec(active_version_lineage={}, limit={:?})",
                self.active_version_lineage, self.limit
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
                "LixFileHistoryScanExec only supports partition 0, got {partition}"
            )));
        }

        let active_version_id = self.active_version_id.clone();
        let history = Arc::clone(&self.history);
        let blob_reader = Arc::clone(&self.blob_reader);
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&schema);
        let active_version_lineage = self.active_version_lineage;
        let limit = self.limit;
        let fut = async move {
            let file_rows = history
                .scan_state_history(&file_history_request(
                    &active_version_id,
                    active_version_lineage,
                    FILE_DESCRIPTOR_SCHEMA_KEY,
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let directory_rows = history
                .scan_state_history(&file_history_request(
                    &active_version_id,
                    active_version_lineage,
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let blob_rows = history
                .scan_state_history(&file_history_request(
                    &active_version_id,
                    active_version_lineage,
                    BLOB_REF_SCHEMA_KEY,
                ))
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let mut rows =
                build_file_history_rows(file_rows, directory_rows, blob_rows, &blob_reader)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            file_history_record_batch(&batch_schema, &rows).map_err(lix_error_to_datafusion_error)
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

#[derive(Debug, Clone)]
struct FileHistoryDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
    row: StateHistoryRow,
}

#[derive(Debug, Clone)]
struct FileHistoryDirectoryRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    row: StateHistoryRow,
}

#[derive(Debug, Clone)]
struct FileHistoryBlobRecord {
    file_id: String,
    blob_hash: String,
    row: StateHistoryRow,
}

#[derive(Debug, Clone)]
struct FileHistoryEvent {
    file_id: String,
    root_commit_id: String,
    raw_depth: i64,
    change_id: String,
    commit_id: String,
    commit_created_at: String,
    priority: i64,
}

#[derive(Debug, Clone)]
struct FileHistoryOutputRow {
    id: String,
    path: Option<String>,
    data: Option<Vec<u8>>,
    hidden: bool,
    entity_id: String,
    schema_key: String,
    file_id: Option<String>,
    version_id: String,
    plugin_key: Option<String>,
    schema_version: String,
    change_id: String,
    metadata: Option<String>,
    commit_id: String,
    commit_created_at: String,
    root_commit_id: String,
    depth: i64,
}

#[derive(Debug, Default)]
struct LixFileStagedBatch {
    state_rows: Vec<LixStateWriteRow>,
    file_data_writes: Vec<FileDataWrite>,
    count: u64,
}

impl LixFileStagedBatch {
    fn extend(&mut self, other: LixFileStagedBatch) {
        self.state_rows.extend(other.state_rows);
        self.file_data_writes.extend(other.file_data_writes);
        self.count += other.count;
    }
}

fn file_history_request(
    active_version_id: &str,
    active_version_lineage: bool,
    schema_key: &str,
) -> StateHistoryRequest {
    let mut request = StateHistoryRequest {
        schema_keys: vec![schema_key.to_string()],
        content_mode: StateHistoryContentMode::IncludeSnapshotContent,
        ..StateHistoryRequest::default()
    };
    if active_version_lineage {
        request.lineage_scope = StateHistoryLineageScope::ActiveVersion;
        request.lineage_version_id = Some(active_version_id.to_string());
    }
    request
}

async fn build_file_history_rows(
    file_rows: Vec<StateHistoryRow>,
    directory_rows: Vec<StateHistoryRow>,
    blob_rows: Vec<StateHistoryRow>,
    blob_reader: &Arc<dyn BlobDataReader>,
) -> std::result::Result<Vec<FileHistoryOutputRow>, LixError> {
    let descriptors = parse_file_history_descriptors(file_rows)?;
    let directories = parse_file_history_directories(directory_rows)?;
    let blobs = parse_file_history_blobs(blob_rows)?;

    let mut descriptor_ids_by_root = BTreeSet::<(String, String)>::new();
    let mut directory_ids_by_file_root = BTreeMap::<(String, String), BTreeSet<String>>::new();
    let mut max_blob_depth_by_file_root = BTreeMap::<(String, String), i64>::new();
    for descriptor in &descriptors {
        let key = (descriptor.id.clone(), descriptor.row.root_commit_id.clone());
        descriptor_ids_by_root.insert(key.clone());
        if let Some(directory_id) = &descriptor.directory_id {
            directory_ids_by_file_root
                .entry(key)
                .or_default()
                .insert(directory_id.clone());
        }
    }
    for blob in &blobs {
        let key = (blob.file_id.clone(), blob.row.root_commit_id.clone());
        max_blob_depth_by_file_root
            .entry(key)
            .and_modify(|depth| *depth = (*depth).max(blob.row.depth))
            .or_insert(blob.row.depth);
    }

    let mut candidates = Vec::<FileHistoryEvent>::new();
    for descriptor in &descriptors {
        let key = (descriptor.id.clone(), descriptor.row.root_commit_id.clone());
        if max_blob_depth_by_file_root
            .get(&key)
            .is_none_or(|max_blob_depth| descriptor.row.depth <= *max_blob_depth)
        {
            candidates.push(file_history_event_from_row(
                descriptor.id.clone(),
                &descriptor.row,
                1,
            ));
        }
    }
    for directory in &directories {
        for ((file_id, root_commit_id), directory_ids) in &directory_ids_by_file_root {
            if root_commit_id == &directory.row.root_commit_id
                && directory_ids.contains(&directory.id)
                && max_blob_depth_by_file_root
                    .get(&(file_id.clone(), root_commit_id.clone()))
                    .is_none_or(|max_blob_depth| directory.row.depth <= *max_blob_depth)
            {
                candidates.push(file_history_event_from_row(
                    file_id.clone(),
                    &directory.row,
                    2,
                ));
            }
        }
    }
    for blob in &blobs {
        if descriptor_ids_by_root.contains(&(blob.file_id.clone(), blob.row.root_commit_id.clone()))
        {
            candidates.push(file_history_event_from_row(
                blob.file_id.clone(),
                &blob.row,
                3,
            ));
        }
    }
    candidates.sort_by(|left, right| {
        left.file_id
            .cmp(&right.file_id)
            .then(left.root_commit_id.cmp(&right.root_commit_id))
            .then(left.raw_depth.cmp(&right.raw_depth))
            .then(left.priority.cmp(&right.priority))
            .then(right.commit_created_at.cmp(&left.commit_created_at))
            .then(right.commit_id.cmp(&left.commit_id))
            .then(right.change_id.cmp(&left.change_id))
    });
    candidates.dedup_by(|left, right| {
        left.file_id == right.file_id
            && left.root_commit_id == right.root_commit_id
            && left.raw_depth == right.raw_depth
    });

    let mut public_depth_by_file_root = BTreeMap::<(String, String), i64>::new();
    let mut output = Vec::<FileHistoryOutputRow>::new();
    for event in candidates {
        let Some(descriptor) = nearest_file_descriptor(&descriptors, &event) else {
            continue;
        };
        let blob = nearest_blob_ref(&blobs, &event);
        let data = match blob {
            Some(blob) => blob_reader.load_blob_data_by_hash(&blob.blob_hash).await?,
            None => None,
        };
        let path = resolve_file_history_path(descriptor, &directories, event.raw_depth);
        let public_depth = public_depth_by_file_root
            .entry((event.file_id.clone(), event.root_commit_id.clone()))
            .and_modify(|depth| *depth += 1)
            .or_insert(0);

        output.push(FileHistoryOutputRow {
            id: descriptor.id.clone(),
            path,
            data,
            hidden: descriptor.hidden,
            entity_id: descriptor.row.entity_id.clone(),
            schema_key: descriptor.row.schema_key.clone(),
            file_id: descriptor.row.file_id.clone(),
            version_id: descriptor.row.version_id.clone(),
            plugin_key: descriptor.row.plugin_key.clone(),
            schema_version: descriptor.row.schema_version.clone(),
            change_id: event.change_id,
            metadata: descriptor.row.metadata.clone(),
            commit_id: event.commit_id,
            commit_created_at: event.commit_created_at,
            root_commit_id: event.root_commit_id,
            depth: *public_depth,
        });
    }
    Ok(output)
}

fn file_history_event_from_row(
    file_id: String,
    row: &StateHistoryRow,
    priority: i64,
) -> FileHistoryEvent {
    FileHistoryEvent {
        file_id,
        root_commit_id: row.root_commit_id.clone(),
        raw_depth: row.depth,
        change_id: row.change_id.clone(),
        commit_id: row.commit_id.clone(),
        commit_created_at: row.commit_created_at.clone(),
        priority,
    }
}

fn parse_file_history_descriptors(
    rows: Vec<StateHistoryRow>,
) -> std::result::Result<Vec<FileHistoryDescriptorRecord>, LixError> {
    rows.into_iter()
        .filter_map(|row| {
            let snapshot_content = row.snapshot_content.clone()?;
            Some((row, snapshot_content))
        })
        .map(|(row, snapshot_content)| {
            let snapshot: FileDescriptorSnapshot = serde_json::from_str(&snapshot_content)
                .map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_file_descriptor history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryDescriptorRecord {
                id: snapshot.id,
                directory_id: snapshot.directory_id,
                name: snapshot.name,
                extension: snapshot.extension,
                hidden: snapshot.hidden,
                row,
            })
        })
        .collect()
}

fn parse_file_history_directories(
    rows: Vec<StateHistoryRow>,
) -> std::result::Result<Vec<FileHistoryDirectoryRecord>, LixError> {
    rows.into_iter()
        .filter_map(|row| {
            let snapshot_content = row.snapshot_content.clone()?;
            Some((row, snapshot_content))
        })
        .map(|(row, snapshot_content)| {
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
                row,
            })
        })
        .collect()
}

fn parse_file_history_blobs(
    rows: Vec<StateHistoryRow>,
) -> std::result::Result<Vec<FileHistoryBlobRecord>, LixError> {
    rows.into_iter()
        .filter_map(|row| {
            let snapshot_content = row.snapshot_content.clone()?;
            Some((row, snapshot_content))
        })
        .map(|(row, snapshot_content)| {
            let snapshot: BlobRefSnapshot =
                serde_json::from_str(&snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid lix_binary_blob_ref history snapshot JSON: {error}"),
                    )
                })?;
            Ok(FileHistoryBlobRecord {
                file_id: row.file_id.clone().unwrap_or(snapshot.id),
                blob_hash: snapshot.blob_hash,
                row,
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
            descriptor.id == event.file_id
                && descriptor.row.root_commit_id == event.root_commit_id
                && descriptor.row.depth >= event.raw_depth
        })
        .min_by(|left, right| {
            left.row
                .depth
                .cmp(&right.row.depth)
                .then(right.row.commit_created_at.cmp(&left.row.commit_created_at))
                .then(right.row.commit_id.cmp(&left.row.commit_id))
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
                && blob.row.root_commit_id == event.root_commit_id
                && blob.row.depth >= event.raw_depth
        })
        .min_by(|left, right| {
            left.row
                .depth
                .cmp(&right.row.depth)
                .then(right.row.commit_created_at.cmp(&left.row.commit_created_at))
                .then(right.row.commit_id.cmp(&left.row.commit_id))
        })
}

fn resolve_file_history_path(
    descriptor: &FileHistoryDescriptorRecord,
    directories: &[FileHistoryDirectoryRecord],
    target_depth: i64,
) -> Option<String> {
    let filename = match descriptor.extension.as_deref() {
        Some(extension) if !extension.is_empty() => format!("{}.{}", descriptor.name, extension),
        _ => descriptor.name.clone(),
    };
    let Some(directory_id) = descriptor.directory_id.as_deref() else {
        return Some(format!("/{filename}"));
    };
    let directory_path = resolve_directory_history_path(
        directory_id,
        &descriptor.row.root_commit_id,
        target_depth,
        directories,
        &mut BTreeMap::new(),
    )?;
    Some(format!("{directory_path}{filename}"))
}

fn resolve_directory_history_path(
    directory_id: &str,
    root_commit_id: &str,
    target_depth: i64,
    directories: &[FileHistoryDirectoryRecord],
    cache: &mut BTreeMap<String, Option<String>>,
) -> Option<String> {
    if let Some(path) = cache.get(directory_id) {
        return path.clone();
    }
    let directory = directories
        .iter()
        .filter(|directory| {
            directory.id == directory_id
                && directory.row.root_commit_id == root_commit_id
                && directory.row.depth >= target_depth
        })
        .min_by(|left, right| {
            left.row
                .depth
                .cmp(&right.row.depth)
                .then(right.row.commit_created_at.cmp(&left.row.commit_created_at))
                .then(right.row.commit_id.cmp(&left.row.commit_id))
        })?;
    let path = match directory.parent_id.as_deref() {
        Some(parent_id) => {
            let parent_path = resolve_directory_history_path(
                parent_id,
                root_commit_id,
                target_depth,
                directories,
                cache,
            )?;
            format!("{parent_path}{}/", directory.name)
        }
        None => format!("/{}/", directory.name),
    };
    cache.insert(directory_id.to_string(), Some(path.clone()));
    Some(path)
}

fn file_history_record_batch(
    schema: &SchemaRef,
    rows: &[FileHistoryOutputRow],
) -> std::result::Result<RecordBatch, LixError> {
    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
            "data" => Arc::new(BinaryArray::from(
                rows.iter()
                    .map(|row| row.data.as_deref())
                    .collect::<Vec<_>>(),
            )),
            "hidden" => Arc::new(BooleanArray::from(
                rows.iter().map(|row| Some(row.hidden)).collect::<Vec<_>>(),
            )),
            "lixcol_entity_id" => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
            "lixcol_schema_key" => {
                string_array(rows.iter().map(|row| Some(row.schema_key.as_str())))
            }
            "lixcol_file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
            "lixcol_version_id" => {
                string_array(rows.iter().map(|row| Some(row.version_id.as_str())))
            }
            "lixcol_plugin_key" => string_array(rows.iter().map(|row| row.plugin_key.as_deref())),
            "lixcol_schema_version" => {
                string_array(rows.iter().map(|row| Some(row.schema_version.as_str())))
            }
            "lixcol_change_id" => string_array(rows.iter().map(|row| Some(row.change_id.as_str()))),
            "lixcol_metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
            "lixcol_commit_id" => string_array(rows.iter().map(|row| Some(row.commit_id.as_str()))),
            "lixcol_commit_created_at" => {
                string_array(rows.iter().map(|row| Some(row.commit_created_at.as_str())))
            }
            "lixcol_root_commit_id" => {
                string_array(rows.iter().map(|row| Some(row.root_commit_id.as_str())))
            }
            "lixcol_depth" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.depth).collect::<Vec<_>>(),
            )),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                    "sql2 lix_file_history provider does not support projected column '{other}'"
                ),
                ))
            }
        };
        columns.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_file_history record batch: {error}"),
        )
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

#[cfg(test)]
fn lix_file_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<LixStateWriteRow>> {
    Ok(lix_file_insert_stage_from_batch(batch, default_version_id)?.state_rows)
}

fn lix_file_existing_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<LixStateWriteRow>> {
    Ok(lix_file_existing_stage_from_batch(batch, default_version_id, true, false)?.state_rows)
}

fn lix_file_insert_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options(batch, default_version_id, true, true, true)
}

fn lix_file_existing_stage_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options(
        batch,
        default_version_id,
        false,
        include_descriptor_writes,
        include_data_writes,
    )
}

fn lix_file_stage_from_batch_with_options(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    let count = u64::try_from(batch.num_rows())
        .map_err(|_| DataFusionError::Execution("lix_file row count overflow".into()))?;
    let mut staged = LixFileStagedBatch {
        count,
        ..LixFileStagedBatch::default()
    };

    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_file_insert_field(batch, row_index, "path")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_entity_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let id = required_string_value(batch, row_index, "id")?;
        let directory_id = optional_string_value(batch, row_index, "directory_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        let extension = optional_string_value(batch, row_index, "extension")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?.unwrap_or(false);
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
        let untracked = optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false);

        if include_descriptor_writes {
            let snapshot_content = json!({
                "id": id.clone(),
                "directory_id": directory_id,
                "name": name,
                "extension": extension,
                "hidden": hidden,
            })
            .to_string();

            staged.state_rows.push(LixStateWriteRow {
                entity_id: id.clone(),
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                plugin_key: optional_string_value(batch, row_index, "lixcol_plugin_key")?,
                snapshot_content: Some(snapshot_content),
                metadata: optional_string_value(batch, row_index, "lixcol_metadata")?,
                schema_version: optional_string_value(batch, row_index, "lixcol_schema_version")?
                    .or_else(|| Some(FILE_DESCRIPTOR_SCHEMA_VERSION.to_string())),
                created_at: None,
                updated_at: None,
                global,
                change_id: None,
                commit_id: None,
                untracked,
                version_id: version_id.clone(),
            });
        }

        if include_data_writes {
            if let Some(data) = optional_binary_value(batch, row_index, "data")? {
                staged.file_data_writes.push(FileDataWrite {
                    file_id: id,
                    version_id,
                    untracked,
                    data,
                });
            }
        }
    }

    Ok(staged)
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
            "directory_id" | "name" | "extension" | "hidden" | "data" | "lixcol_metadata"
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

fn lix_file_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("hidden", DataType::Boolean, false),
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BinaryArray, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures_util::stream;
    use serde_json::Value as JsonValue;

    use crate::sql2::{SqlWriteIntent, SqlWriteOutcome, SqlWriteStager};
    use crate::LixError;

    use super::{
        derive_directory_path_for, lix_file_insert_stage_from_batch,
        lix_file_write_rows_from_batch, DirectoryDescriptorRecord, LixFileInsertSink,
    };

    #[derive(Default)]
    struct CapturingWriteStager {
        writes: std::sync::Mutex<Vec<SqlWriteIntent>>,
    }

    #[async_trait]
    impl SqlWriteStager for CapturingWriteStager {
        async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
            self.writes.lock().expect("writes lock").push(write);
            Ok(SqlWriteOutcome { count: 0 })
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
        assert_eq!(rows[0].entity_id, "file-readme");
        assert_eq!(rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(rows[0].version_id, "version-b");
        assert_eq!(rows[0].schema_version.as_deref(), Some("1"));
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
    fn file_insert_stages_non_null_data() {
        let batch = data_insert_batch();

        let staged = lix_file_insert_stage_from_batch(&batch, None).expect("decode file data");

        assert_eq!(staged.count, 1);
        assert_eq!(staged.state_rows.len(), 1);
        assert_eq!(staged.file_data_writes.len(), 1);
        assert_eq!(staged.file_data_writes[0].file_id, "file-readme");
        assert_eq!(staged.file_data_writes[0].version_id, "version-b");
        assert_eq!(staged.file_data_writes[0].data, b"hello");
    }

    #[tokio::test]
    async fn file_insert_sink_stages_decoded_lix_state_rows() {
        let batch = file_insert_batch(true, false);
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixFileInsertSink::new(
            batch.schema(),
            Arc::clone(&stager) as Arc<dyn SqlWriteStager>,
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
            SqlWriteIntent::InsertLixState { rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].entity_id, "file-readme");
                assert_eq!(rows[0].schema_key, "lix_file_descriptor");
            }
            other => panic!("expected insert write intent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_stages_file_data_writes() {
        let batch = data_insert_batch();
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixFileInsertSink::new(
            batch.schema(),
            Arc::clone(&stager) as Arc<dyn SqlWriteStager>,
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
            SqlWriteIntent::InsertLixStateWithFileData {
                rows,
                file_data,
                count,
            } => {
                assert_eq!(*count, 1);
                assert_eq!(rows.len(), 1);
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                assert_eq!(file_data[0].data, b"hello");
            }
            other => panic!("expected insert with file data write intent, got {other:?}"),
        }
    }
}
