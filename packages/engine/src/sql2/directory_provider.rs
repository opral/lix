use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatchOptions, StringArray, UInt64Array,
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

use crate::history::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryRequest, StateHistoryRow,
};
use crate::live_state::{
    LiveRow, LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateScanRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

use super::execute::{HistoryContext, LixStateWriteRow, SqlWriteIntent, SqlWriteStager};

const DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const DIRECTORY_SCHEMA_VERSION: &str = "1";

pub(crate) async fn register_lix_directory_providers(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    history: Option<Arc<dyn HistoryContext>>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_directory_by_version",
            Arc::new(LixDirectoryProvider::by_version(
                Arc::clone(&live_state),
                write_stager.as_ref().map(Arc::clone),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_directory",
            Arc::new(LixDirectoryProvider::active_version(
                active_version_id,
                live_state,
                write_stager,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    if let Some(history) = history {
        session
            .register_table(
                "lix_directory_history",
                Arc::new(LixDirectoryHistoryProvider::new(active_version_id, history)),
            )
            .map_err(datafusion_error_to_lix_error)?;
    }
    Ok(())
}

pub(crate) struct LixDirectoryProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    default_version_id: Option<String>,
}

impl std::fmt::Debug for LixDirectoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryProvider").finish()
    }
}

impl LixDirectoryProvider {
    fn active_version(
        active_version_id: impl Into<String>,
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_directory_schema(),
            live_state,
            write_stager,
            default_version_id: Some(active_version_id.into()),
        }
    }

    fn by_version(
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_directory_by_version_schema(),
            live_state,
            write_stager,
            default_version_id: None,
        }
    }
}

struct LixDirectoryHistoryProvider {
    schema: SchemaRef,
    active_version_id: String,
    history: Arc<dyn HistoryContext>,
}

impl std::fmt::Debug for LixDirectoryHistoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryHistoryProvider").finish()
    }
}

impl LixDirectoryHistoryProvider {
    fn new(active_version_id: impl Into<String>, history: Arc<dyn HistoryContext>) -> Self {
        Self {
            schema: lix_directory_history_schema(),
            active_version_id: active_version_id.into(),
            history,
        }
    }
}

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
            self.active_version_id.clone(),
            Arc::clone(&self.history),
            projected_schema,
            limit,
        )))
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
        let request = lix_directory_scan_request(self.default_version_id.as_deref(), limit);
        Ok(Arc::new(LixDirectoryScanExec::new(
            Arc::clone(&self.live_state),
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
            return not_impl_err!("{insert_op} not implemented for lix_directory yet");
        }

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "INSERT into lix_directory requires a write transaction".to_string(),
            ));
        };

        let sink = LixDirectoryInsertSink::new(
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
                "DELETE FROM lix_directory requires a write transaction".to_string(),
            ));
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let request = lix_directory_scan_request(self.default_version_id.as_deref(), None);

        Ok(Arc::new(LixDirectoryDeleteExec::new(
            Arc::clone(&self.live_state),
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
                "UPDATE lix_directory requires a write transaction".to_string(),
            ));
        };

        validate_lix_directory_update_assignments(&self.schema, &assignments)?;

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
        let request = lix_directory_scan_request(self.default_version_id.as_deref(), None);

        Ok(Arc::new(LixDirectoryUpdateExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            self.default_version_id.clone(),
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixDirectoryInsertSink {
    schema: SchemaRef,
    write_stager: Arc<dyn SqlWriteStager>,
    default_version_id: Option<String>,
}

impl std::fmt::Debug for LixDirectoryInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryInsertSink").finish()
    }
}

impl LixDirectoryInsertSink {
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

impl DisplayAs for LixDirectoryInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixDirectoryInsertSink")
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryInsertSink"),
        }
    }
}

#[async_trait]
impl DataSink for LixDirectoryInsertSink {
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
        let mut rows = Vec::new();
        while let Some(batch) = data.next().await.transpose()? {
            rows.extend(lix_directory_write_rows_from_batch(
                &batch,
                self.default_version_id.as_deref(),
            )?);
        }
        let count = u64::try_from(rows.len()).map_err(|_| {
            DataFusionError::Execution("lix_directory INSERT row count overflow".into())
        })?;

        self.write_stager
            .stage_write(SqlWriteIntent::InsertLixState { rows })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

struct LixDirectoryDeleteExec {
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixDirectoryDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryDeleteExec").finish()
    }
}

impl LixDirectoryDeleteExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
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

impl DisplayAs for LixDirectoryDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixDirectoryDeleteExec(filters={})", self.filters.len())
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryDeleteExec"),
        }
    }
}

impl ExecutionPlan for LixDirectoryDeleteExec {
    fn name(&self) -> &str {
        "LixDirectoryDeleteExec"
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
                "LixDirectoryDeleteExec does not accept children".to_string(),
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
                "LixDirectoryDeleteExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
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
            let source_batch = lix_directory_record_batch(&table_schema, rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_directory_batch(source_batch, &filters)?;
            let mut write_rows = lix_directory_existing_write_rows_from_batch(
                &matched_batch,
                default_version_id.as_deref(),
            )?;
            for row in &mut write_rows {
                row.snapshot_content = None;
            }
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("lix_directory DELETE row count overflow".into())
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

struct LixDirectoryUpdateExec {
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
    request: LiveStateScanRequest,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixDirectoryUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryUpdateExec").finish()
    }
}

impl LixDirectoryUpdateExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
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

impl DisplayAs for LixDirectoryUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixDirectoryUpdateExec(assignments={}, filters={})",
                    self.assignments.len(),
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryUpdateExec"),
        }
    }
}

impl ExecutionPlan for LixDirectoryUpdateExec {
    fn name(&self) -> &str {
        "LixDirectoryUpdateExec"
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
                "LixDirectoryUpdateExec does not accept children".to_string(),
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
                "LixDirectoryUpdateExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
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
            let source_batch = lix_directory_record_batch(&table_schema, rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_directory_batch(source_batch, &filters)?;
            let updated_batch =
                apply_lix_directory_update_assignments(&table_schema, matched_batch, &assignments)?;
            let write_rows = lix_directory_existing_write_rows_from_batch(
                &updated_batch,
                default_version_id.as_deref(),
            )?;
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("lix_directory UPDATE row count overflow".into())
            })?;

            if count > 0 {
                write_stager
                    .stage_write(SqlWriteIntent::InsertLixState { rows: write_rows })
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

struct LixDirectoryScanExec {
    live_state: Arc<dyn LiveStateContext>,
    schema: SchemaRef,
    request: LiveStateScanRequest,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixDirectoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryScanExec").finish()
    }
}

impl LixDirectoryScanExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
        schema: SchemaRef,
        request: LiveStateScanRequest,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            schema,
            request,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixDirectoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixDirectoryScanExec(limit={:?})", self.request.limit)
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
                "LixDirectoryScanExec only supports partition 0, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let request = self.request.clone();
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&schema);
        let fut = async move {
            let rows = live_state.scan(&request).await.map_err(|error| {
                DataFusionError::Execution(format!("sql2 lix_directory scan failed: {error}"))
            })?;
            let batch = lix_directory_record_batch(&batch_schema, rows).map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 lix_directory batch build failed: {error}"
                ))
            })?;
            Ok::<RecordBatch, DataFusionError>(batch)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut).map_ok(|batch| batch),
        )))
    }
}

struct LixDirectoryHistoryScanExec {
    active_version_id: String,
    history: Arc<dyn HistoryContext>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixDirectoryHistoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryHistoryScanExec")
            .field("limit", &self.limit)
            .finish()
    }
}

impl LixDirectoryHistoryScanExec {
    fn new(
        active_version_id: String,
        history: Arc<dyn HistoryContext>,
        schema: SchemaRef,
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
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixDirectoryHistoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixDirectoryHistoryScanExec(limit={:?})", self.limit)
            }
            DisplayFormatType::TreeRender => write!(f, "LixDirectoryHistoryScanExec"),
        }
    }
}

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
                "LixDirectoryHistoryScanExec only supports partition 0, got {partition}"
            )));
        }

        let active_version_id = self.active_version_id.clone();
        let history = Arc::clone(&self.history);
        let schema = Arc::clone(&self.schema);
        let batch_schema = Arc::clone(&schema);
        let limit = self.limit;
        let fut = async move {
            let request = StateHistoryRequest {
                lineage_scope: StateHistoryLineageScope::ActiveVersion,
                lineage_version_id: Some(active_version_id),
                schema_keys: vec![DIRECTORY_SCHEMA_KEY.to_string()],
                content_mode: StateHistoryContentMode::IncludeSnapshotContent,
                ..StateHistoryRequest::default()
            };
            let mut rows = build_directory_history_rows(
                history
                    .scan_state_history(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
            )
            .map_err(lix_error_to_datafusion_error)?;
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            directory_history_record_batch(&batch_schema, &rows)
                .map_err(lix_error_to_datafusion_error)
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(fut).map_ok(|batch| batch),
        )))
    }
}

#[derive(Debug, Clone)]
struct DirectoryDescriptorRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: bool,
    live: LiveRow,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
}

#[derive(Debug, Clone)]
struct DirectoryHistoryRecord {
    id: String,
    parent_id: Option<String>,
    name: String,
    path: Option<String>,
    hidden: bool,
    row: StateHistoryRow,
}

fn build_directory_history_rows(
    rows: Vec<StateHistoryRow>,
) -> std::result::Result<Vec<DirectoryHistoryRecord>, LixError> {
    let mut decoded = rows
        .into_iter()
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
            Ok(DirectoryHistoryRecord {
                id: snapshot.id,
                parent_id: snapshot.parent_id,
                name: snapshot.name,
                path: None,
                hidden: snapshot.hidden.unwrap_or(false),
                row,
            })
        })
        .collect::<std::result::Result<Vec<_>, LixError>>()?;

    let paths = derive_directory_history_paths(&decoded);
    for row in &mut decoded {
        row.path = paths
            .get(&(
                row.row.root_commit_id.clone(),
                row.row.depth,
                row.id.clone(),
            ))
            .cloned();
    }
    Ok(decoded)
}

fn derive_directory_history_paths(
    rows: &[DirectoryHistoryRecord],
) -> BTreeMap<(String, i64, String), String> {
    let mut paths = BTreeMap::<(String, i64, String), String>::new();
    for row in rows {
        derive_directory_history_path_for(
            row.row.root_commit_id.as_str(),
            row.row.depth,
            row.id.as_str(),
            rows,
            &mut paths,
        );
    }
    paths
}

fn derive_directory_history_path_for(
    root_commit_id: &str,
    target_depth: i64,
    directory_id: &str,
    rows: &[DirectoryHistoryRecord],
    paths: &mut BTreeMap<(String, i64, String), String>,
) -> Option<String> {
    let key = (
        root_commit_id.to_string(),
        target_depth,
        directory_id.to_string(),
    );
    if let Some(path) = paths.get(&key) {
        return Some(path.clone());
    }
    let row = rows
        .iter()
        .filter(|row| {
            row.id == directory_id
                && row.row.root_commit_id == root_commit_id
                && row.row.depth >= target_depth
        })
        .min_by(|left, right| {
            left.row
                .depth
                .cmp(&right.row.depth)
                .then(right.row.commit_created_at.cmp(&left.row.commit_created_at))
                .then(right.row.commit_id.cmp(&left.row.commit_id))
        })?;
    let path = match row.parent_id.as_deref() {
        Some(parent_id) => {
            let parent_path = derive_directory_history_path_for(
                root_commit_id,
                target_depth,
                parent_id,
                rows,
                paths,
            )?;
            format!("{parent_path}{}/", row.name)
        }
        None => format!("/{}/", row.name),
    };
    paths.insert(key, path.clone());
    Some(path)
}

fn directory_history_record_batch(
    schema: &SchemaRef,
    rows: &[DirectoryHistoryRecord],
) -> std::result::Result<RecordBatch, LixError> {
    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            "parent_id" => string_array(rows.iter().map(|row| row.parent_id.as_deref())),
            "name" => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
            "path" => string_array(rows.iter().map(|row| row.path.as_deref())),
            "hidden" => Arc::new(BooleanArray::from(
                rows.iter().map(|row| Some(row.hidden)).collect::<Vec<_>>(),
            )),
            "lixcol_entity_id" => string_array(rows.iter().map(|row| Some(row.row.entity_id.as_str()))),
            "lixcol_schema_key" => string_array(rows.iter().map(|row| Some(row.row.schema_key.as_str()))),
            "lixcol_file_id" => string_array(rows.iter().map(|row| row.row.file_id.as_deref())),
            "lixcol_version_id" => {
                string_array(rows.iter().map(|row| Some(row.row.version_id.as_str())))
            }
            "lixcol_plugin_key" => string_array(rows.iter().map(|row| row.row.plugin_key.as_deref())),
            "lixcol_schema_version" => {
                string_array(rows.iter().map(|row| Some(row.row.schema_version.as_str())))
            }
            "lixcol_change_id" => {
                string_array(rows.iter().map(|row| Some(row.row.change_id.as_str())))
            }
            "lixcol_metadata" => string_array(rows.iter().map(|row| row.row.metadata.as_deref())),
            "lixcol_commit_id" => {
                string_array(rows.iter().map(|row| Some(row.row.commit_id.as_str())))
            }
            "lixcol_commit_created_at" => {
                string_array(rows.iter().map(|row| Some(row.row.commit_created_at.as_str())))
            }
            "lixcol_root_commit_id" => {
                string_array(rows.iter().map(|row| Some(row.row.root_commit_id.as_str())))
            }
            "lixcol_depth" => Arc::new(Int64Array::from(
                rows.iter().map(|row| row.row.depth).collect::<Vec<_>>(),
            )),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "sql2 lix_directory_history provider does not support projected column '{other}'"
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
            format!("sql2 failed to build lix_directory_history record batch: {error}"),
        )
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

fn lix_directory_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<LixStateWriteRow>> {
    lix_directory_write_rows_from_batch_with_options(batch, default_version_id, true)
}

fn lix_directory_existing_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<LixStateWriteRow>> {
    lix_directory_write_rows_from_batch_with_options(batch, default_version_id, false)
}

fn lix_directory_write_rows_from_batch_with_options(
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    reject_read_only_fields: bool,
) -> Result<Vec<LixStateWriteRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            if reject_read_only_fields {
                reject_read_only_lix_directory_insert_field(batch, row_index, "path")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_entity_id")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_schema_key")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_change_id")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_created_at")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_updated_at")?;
                reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_commit_id")?;
            }

            let id = required_string_value(batch, row_index, "id")?;
            let parent_id = optional_string_value(batch, row_index, "parent_id")?;
            let name = required_string_value(batch, row_index, "name")?;
            let hidden = optional_bool_value(batch, row_index, "hidden")?.unwrap_or(false);
            let global = optional_bool_value(batch, row_index, "lixcol_global")?.unwrap_or(false);
            let version_id = if global {
                GLOBAL_VERSION_ID.to_string()
            } else {
                optional_string_value(batch, row_index, "lixcol_version_id")?
                    .or_else(|| default_version_id.map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "INSERT into lix_directory_by_version requires lixcol_version_id"
                                .to_string(),
                        )
                    })?
            };
            let snapshot_content = json!({
                "id": id,
                "parent_id": parent_id,
                "name": name,
                "hidden": hidden,
            })
            .to_string();

            Ok(LixStateWriteRow {
                entity_id: id,
                schema_key: DIRECTORY_SCHEMA_KEY.to_string(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                plugin_key: optional_string_value(batch, row_index, "lixcol_plugin_key")?,
                snapshot_content: Some(snapshot_content),
                metadata: optional_string_value(batch, row_index, "lixcol_metadata")?,
                schema_version: optional_string_value(batch, row_index, "lixcol_schema_version")?
                    .or_else(|| Some(DIRECTORY_SCHEMA_VERSION.to_string())),
                created_at: None,
                updated_at: None,
                global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?
                    .unwrap_or(false),
                version_id,
            })
        })
        .collect()
}

fn lix_directory_record_batch(
    schema: &SchemaRef,
    rows: Vec<LiveRow>,
) -> Result<RecordBatch, LixError> {
    let mut directory_rows = Vec::<DirectoryDescriptorRecord>::new();

    for row in rows {
        if row.schema_key != DIRECTORY_SCHEMA_KEY {
            continue;
        }
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
            hidden: snapshot.hidden.unwrap_or(false),
            live: row,
        });
    }

    let directory_paths = derive_directory_paths(&directory_rows);
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut parent_ids = Vec::new();
    let mut names = Vec::new();
    let mut hiddens = Vec::new();
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

    for directory in directory_rows {
        ids.push(Some(directory.id.clone()));
        paths.push(
            directory_paths
                .get(&(directory.live.version_id.clone(), directory.id.clone()))
                .cloned(),
        );
        parent_ids.push(directory.parent_id);
        names.push(Some(directory.name));
        hiddens.push(Some(directory.hidden));
        entity_ids.push(Some(directory.live.entity_id));
        schema_keys.push(Some(directory.live.schema_key));
        file_ids.push(directory.live.file_id);
        plugin_keys.push(directory.live.plugin_key);
        schema_versions.push(Some(directory.live.schema_version));
        globals.push(Some(directory.live.global));
        change_ids.push(directory.live.change_id);
        created_ats.push(directory.live.created_at);
        updated_ats.push(directory.live.updated_at);
        commit_ids.push(directory.live.commit_id);
        untracked_values.push(Some(directory.live.untracked));
        metadata_values.push(directory.live.metadata);
        version_ids.push(Some(directory.live.version_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "parent_id" => Arc::new(StringArray::from(parent_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "hidden" => Arc::new(BooleanArray::from(hiddens.clone())),
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
                    format!(
                        "sql2 lix_directory provider does not support projected column '{other}'"
                    ),
                ))
            }
        };
        columns.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_directory record batch: {error}"),
        )
    })
}

fn derive_directory_paths(
    rows: &[DirectoryDescriptorRecord],
) -> BTreeMap<(String, String), String> {
    let mut by_version = BTreeMap::<String, BTreeMap<String, &DirectoryDescriptorRecord>>::new();
    for row in rows {
        by_version
            .entry(row.live.version_id.clone())
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

fn lix_directory_scan_request(
    default_version_id: Option<&str>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![DIRECTORY_SCHEMA_KEY.to_string()],
            version_ids: default_version_id
                .map(|version_id| vec![version_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: LiveStateProjection::default(),
        limit,
    }
}

fn validate_lix_directory_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_directory failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "parent_id" | "name" | "hidden" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_directory cannot stage read-only column '{column_name}'"
            )));
        }
    }
    Ok(())
}

fn filter_lix_directory_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_lix_directory_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_lix_directory_filters(
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
                DataFusionError::Execution("lix_directory filter was not boolean".to_string())
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

fn apply_lix_directory_update_assignments(
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
                "UPDATE lix_directory source batch is missing column '{column_name}'"
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

fn reject_read_only_lix_directory_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into lix_directory cannot stage read-only column '{column_name}'"
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
            "INSERT into lix_directory requires non-null text column '{column_name}'"
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
            "INSERT into lix_directory expected text-compatible column '{column_name}', got {other:?}"
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
            "INSERT into lix_directory expected boolean column '{column_name}', got {other:?}"
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
            "row index {row_index} out of bounds for lix_directory batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_directory column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

fn lix_directory_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
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

fn lix_directory_by_version_schema() -> SchemaRef {
    let mut fields = lix_directory_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn lix_directory_history_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
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
    DataFusionError::Execution(format!("sql2 lix_directory provider error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures_util::stream;

    use crate::live_state::LiveRow;
    use crate::sql2::{LixStateWriteRow, SqlWriteIntent, SqlWriteOutcome, SqlWriteStager};
    use crate::LixError;

    use super::{
        derive_directory_path_for, lix_directory_by_version_schema, lix_directory_record_batch,
        lix_directory_write_rows_from_batch, DirectoryDescriptorRecord, LixDirectoryInsertSink,
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

    fn live_row(entity_id: &str, version_id: &str, snapshot_content: &str) -> LiveRow {
        LiveRow {
            entity_id: entity_id.to_string(),
            schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: Some("{\"source\":\"test\"}".to_string()),
            schema_version: "1".to_string(),
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn directory_insert_batch(include_version: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("hidden", DataType::Boolean, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let mut columns = vec![
            string_column(vec![Some("dir-docs")]),
            string_column(vec![None]),
            string_column(vec![Some("docs")]),
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            string_column(vec![Some("{\"source\":\"directory\"}")]),
        ];
        if include_version {
            fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("version-a")]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("directory insert batch should build")
    }

    #[test]
    fn derives_nested_directory_paths() {
        let root = DirectoryDescriptorRecord {
            id: "dir-docs".to_string(),
            parent_id: None,
            name: "docs".to_string(),
            hidden: false,
            live: live_row(
                "dir-docs",
                "version-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}",
            ),
        };
        let child = DirectoryDescriptorRecord {
            id: "dir-guides".to_string(),
            parent_id: Some("dir-docs".to_string()),
            name: "guides".to_string(),
            hidden: false,
            live: live_row(
                "dir-guides",
                "version-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\",\"hidden\":false}",
            ),
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
    fn record_batch_projects_directory_columns() {
        let rows = vec![
            live_row(
                "dir-docs",
                "version-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}",
            ),
            live_row(
                "dir-guides",
                "version-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\",\"hidden\":true}",
            ),
        ];

        let batch = lix_directory_record_batch(&lix_directory_by_version_schema(), rows)
            .expect("directory batch should build");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column_by_name("path")
                .expect("path column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("path is string")
                .value(1),
            "/docs/guides/"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_version_id")
                .expect("version column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("version is string")
                .value(1),
            "version-a"
        );
    }

    #[test]
    fn decodes_directory_insert_into_lix_state_write_row() {
        let rows = lix_directory_write_rows_from_batch(&directory_insert_batch(true, false), None)
            .expect("directory batch should decode");

        assert_eq!(
            rows,
            vec![LixStateWriteRow {
                entity_id: "dir-docs".to_string(),
                schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some(
                    "{\"hidden\":false,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}"
                        .to_string()
                ),
                metadata: Some("{\"source\":\"directory\"}".to_string()),
                schema_version: Some("1".to_string()),
                created_at: None,
                updated_at: None,
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                version_id: "version-a".to_string(),
            }]
        );
    }

    #[test]
    fn active_directory_insert_defaults_version_id() {
        let rows = lix_directory_write_rows_from_batch(
            &directory_insert_batch(false, false),
            Some("version-active"),
        )
        .expect("active directory batch should decode");

        assert_eq!(rows[0].version_id, "version-active");
    }

    #[test]
    fn by_version_directory_insert_requires_version_id_for_non_global_rows() {
        let error =
            lix_directory_write_rows_from_batch(&directory_insert_batch(false, false), None)
                .expect_err("by-version insert should require version id");

        assert!(
            error.to_string().contains("requires lixcol_version_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn directory_insert_rejects_non_null_path() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, true),
                Field::new("parent_id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("hidden", DataType::Boolean, false),
            ])),
            vec![
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("/docs/")]),
                string_column(vec![None]),
                string_column(vec![Some("docs")]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .expect("directory insert batch should build");

        let error = lix_directory_write_rows_from_batch(&batch, Some("version-a"))
            .expect_err("path is read-only");

        assert!(
            error.to_string().contains("read-only column 'path'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn directory_insert_sink_stages_decoded_lix_state_rows() {
        let stager = Arc::new(CapturingWriteStager::default());
        let batch = directory_insert_batch(true, false);
        let sink = LixDirectoryInsertSink::new(
            batch.schema(),
            Arc::clone(&stager) as Arc<dyn SqlWriteStager>,
            None,
        );
        let stream = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(sink.schema().clone(), stream));

        let count = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("directory sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            stager.writes.lock().expect("writes lock").as_slice(),
            &[SqlWriteIntent::InsertLixState {
                rows: vec![LixStateWriteRow {
                    entity_id: "dir-docs".to_string(),
                    schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                    file_id: None,
                    plugin_key: None,
                    snapshot_content: Some(
                        "{\"hidden\":false,\"id\":\"dir-docs\",\"name\":\"docs\",\"parent_id\":null}"
                            .to_string()
                    ),
                    metadata: Some("{\"source\":\"directory\"}".to_string()),
                    schema_version: Some("1".to_string()),
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    version_id: "version-a".to_string(),
                }]
            }]
        );
    }
}
