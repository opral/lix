use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, DFSchema, DataFusionError, Result, SchemaExt};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{create_physical_expr, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures_util::{stream, StreamExt, TryStreamExt};

use crate::engine2::live_state::{
    LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateScanRequest,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

use super::execute::{SqlWriteIntent, SqlWriteStager, StateRow};

pub(crate) async fn register_lix_state_providers(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_state_by_version",
            Arc::new(LixStateProvider::by_version(Arc::clone(&live_state), None)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_state",
            Arc::new(LixStateProvider::active_version(
                active_version_id,
                live_state,
                write_stager,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixStateProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    default_version_id: Option<String>,
}

impl std::fmt::Debug for LixStateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateProvider")
            .field("has_write_stager", &self.write_stager.is_some())
            .finish()
    }
}

impl LixStateProvider {
    pub(crate) fn active_version(
        active_version_id: impl Into<String>,
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_state_schema(),
            live_state,
            write_stager,
            default_version_id: Some(active_version_id.into()),
        }
    }

    pub(crate) fn by_version(
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: lix_state_by_version_schema(),
            live_state,
            write_stager,
            default_version_id: None,
        }
    }
}

#[async_trait]
impl TableProvider for LixStateProvider {
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
                if parse_lix_state_filter(filter).is_some() {
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
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let route = LixStateByVersionRoute::from_filters(filters);
        let projected_schema = projected_schema(&self.schema, projection)?;
        let request = lix_state_scan_request(
            &self.schema,
            self.default_version_id.as_deref(),
            projection,
            &route,
            limit,
        );
        Ok(Arc::new(LixStateScanExec::new(
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
            return not_impl_err!("{insert_op} not implemented for lix_state yet");
        }

        let Some(default_version_id) = &self.default_version_id else {
            return Err(DataFusionError::Execution(
                "INSERT is only supported for active lix_state".to_string(),
            ));
        };

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "INSERT into lix_state requires a write transaction".to_string(),
            ));
        };

        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;

        let sink = LixStateInsertSink::new(
            Arc::clone(&self.schema),
            Arc::clone(write_stager),
            default_version_id.clone(),
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(default_version_id) = &self.default_version_id else {
            return Err(DataFusionError::Execution(
                "DELETE is only supported for active lix_state".to_string(),
            ));
        };

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "DELETE FROM lix_state requires a write transaction".to_string(),
            ));
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;

        let route = LixStateByVersionRoute::from_filters(&filters);
        let request =
            lix_state_scan_request(&self.schema, Some(default_version_id), None, &route, None);

        Ok(Arc::new(LixStateDeleteExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            default_version_id.clone(),
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
        let Some(default_version_id) = &self.default_version_id else {
            return Err(DataFusionError::Execution(
                "UPDATE is only supported for active lix_state".to_string(),
            ));
        };

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_state requires a write transaction".to_string(),
            ));
        };

        validate_lix_state_update_assignments(&self.schema, &assignments)?;

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

        let route = LixStateByVersionRoute::from_filters(&filters);
        let request =
            lix_state_scan_request(&self.schema, Some(default_version_id), None, &route, None);

        Ok(Arc::new(LixStateUpdateExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            default_version_id.clone(),
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixStateInsertSink {
    schema: SchemaRef,
    write_stager: Arc<dyn SqlWriteStager>,
    default_version_id: String,
}

impl std::fmt::Debug for LixStateInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateInsertSink").finish()
    }
}

impl LixStateInsertSink {
    fn new(
        schema: SchemaRef,
        write_stager: Arc<dyn SqlWriteStager>,
        default_version_id: String,
    ) -> Self {
        Self {
            schema,
            write_stager,
            default_version_id,
        }
    }
}

impl DisplayAs for LixStateInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixStateInsertSink")
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateInsertSink"),
        }
    }
}

#[async_trait]
impl DataSink for LixStateInsertSink {
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
            rows.extend(lix_state_write_rows_from_batch(
                &batch,
                &self.default_version_id,
            )?);
        }
        let count = u64::try_from(rows.len())
            .map_err(|_| DataFusionError::Execution("INSERT row count overflow".into()))?;

        self.write_stager
            .stage_write(SqlWriteIntent::WriteRows { rows })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

struct LixStateDeleteExec {
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: String,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixStateDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateDeleteExec").finish()
    }
}

impl LixStateDeleteExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Arc<dyn SqlWriteStager>,
        table_schema: SchemaRef,
        default_version_id: String,
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

impl DisplayAs for LixStateDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixStateDeleteExec(filters={})", self.filters.len())
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateDeleteExec"),
        }
    }
}

impl ExecutionPlan for LixStateDeleteExec {
    fn name(&self) -> &str {
        "LixStateDeleteExec"
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
                "LixStateDeleteExec does not accept children".to_string(),
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
                "LixStateDeleteExec only exposes one partition, got {partition}"
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
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                live_state
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = lix_state_record_batch(Arc::clone(&table_schema), &rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_state_batch(source_batch, &filters)?;
            let write_rows =
                lix_state_deletable_write_rows_from_batch(&matched_batch, &default_version_id)?;
            let count = u64::try_from(write_rows.len())
                .map_err(|_| DataFusionError::Execution("DELETE row count overflow".to_string()))?;

            if count > 0 {
                write_stager
                    .stage_write(SqlWriteIntent::WriteRows { rows: write_rows })
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

struct LixStateUpdateExec {
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: String,
    request: LiveStateScanRequest,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixStateUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateUpdateExec").finish()
    }
}

impl LixStateUpdateExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Arc<dyn SqlWriteStager>,
        table_schema: SchemaRef,
        default_version_id: String,
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

impl DisplayAs for LixStateUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateUpdateExec(assignments={}, filters={})",
                    self.assignments.len(),
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateUpdateExec"),
        }
    }
}

impl ExecutionPlan for LixStateUpdateExec {
    fn name(&self) -> &str {
        "LixStateUpdateExec"
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
                "LixStateUpdateExec does not accept children".to_string(),
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
                "LixStateUpdateExec only exposes one partition, got {partition}"
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
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                live_state
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = lix_state_record_batch(Arc::clone(&table_schema), &rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_state_batch(source_batch, &filters)?;
            let updated_batch =
                apply_lix_state_update_assignments(&table_schema, matched_batch, &assignments)?;
            let write_rows =
                lix_state_stageable_write_rows_from_batch(&updated_batch, &default_version_id)?;
            let count = u64::try_from(write_rows.len())
                .map_err(|_| DataFusionError::Execution("UPDATE row count overflow".to_string()))?;

            if count > 0 {
                write_stager
                    .stage_write(SqlWriteIntent::WriteRows { rows: write_rows })
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

fn validate_lix_state_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_state failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(column_name.as_str(), "snapshot_content" | "metadata") {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_state cannot stage read-only column '{column_name}'"
            )));
        }
    }
    Ok(())
}

fn filter_lix_state_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_lix_state_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_lix_state_filters(
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
                DataFusionError::Execution("UPDATE lix_state filter was not boolean".to_string())
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

fn apply_lix_state_update_assignments(
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
                "UPDATE lix_state source batch is missing column '{column_name}'"
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

fn lix_state_stageable_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: &str,
) -> Result<Vec<StateRow>> {
    let mut rows = lix_state_write_rows_from_batch(batch, default_version_id)?;
    for row in &mut rows {
        row.created_at = None;
        row.updated_at = None;
        row.change_id = None;
        row.commit_id = None;
    }
    Ok(rows)
}

fn lix_state_deletable_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: &str,
) -> Result<Vec<StateRow>> {
    let mut rows = lix_state_stageable_write_rows_from_batch(batch, default_version_id)?;
    for row in &mut rows {
        row.snapshot_content = None;
    }
    Ok(rows)
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

fn lix_state_write_rows_from_batch(
    batch: &RecordBatch,
    default_version_id: &str,
) -> Result<Vec<StateRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let global = required_bool_value(batch, row_index, "global")?;
            let version_id =
                optional_string_value(batch, row_index, "version_id")?.unwrap_or_else(|| {
                    if global {
                        GLOBAL_VERSION_ID.to_string()
                    } else {
                        default_version_id.to_string()
                    }
                });

            Ok(StateRow {
                entity_id: required_string_value(batch, row_index, "entity_id")?,
                schema_key: required_string_value(batch, row_index, "schema_key")?,
                file_id: optional_string_value(batch, row_index, "file_id")?,
                plugin_key: optional_string_value(batch, row_index, "plugin_key")?,
                snapshot_content: optional_string_value(batch, row_index, "snapshot_content")?,
                metadata: optional_string_value(batch, row_index, "metadata")?,
                schema_version: optional_string_value(batch, row_index, "schema_version")?,
                created_at: optional_string_value(batch, row_index, "created_at")?,
                updated_at: optional_string_value(batch, row_index, "updated_at")?,
                global,
                change_id: optional_string_value(batch, row_index, "change_id")?,
                commit_id: optional_string_value(batch, row_index, "commit_id")?,
                untracked: required_bool_value(batch, row_index, "untracked")?,
                version_id,
            })
        })
        .collect()
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "INSERT into lix_state requires non-null text column '{column_name}'"
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
            "INSERT into lix_state expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn required_bool_value(batch: &RecordBatch, row_index: usize, column_name: &str) -> Result<bool> {
    match optional_scalar_value(batch, row_index, column_name)? {
        Some(ScalarValue::Boolean(Some(value))) => Ok(value),
        None | Some(ScalarValue::Null) | Some(ScalarValue::Boolean(None)) => {
            Err(DataFusionError::Execution(format!(
                "INSERT into lix_state requires non-null boolean column '{column_name}'"
            )))
        }
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into lix_state expected boolean column '{column_name}', got {other:?}"
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
            "row index {row_index} out of bounds for lix_state batch with {} rows",
            batch.num_rows()
        )));
    }

    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode lix_state column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

struct LixStateScanExec {
    live_state: Arc<dyn LiveStateContext>,
    schema: SchemaRef,
    request: LiveStateScanRequest,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixStateScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixStateScanExec").finish()
    }
}

impl LixStateScanExec {
    fn new(
        live_state: Arc<dyn LiveStateContext>,
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
            schema,
            request,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixStateScanExec(limit={:?})", self.request.limit)
            }
            DisplayFormatType::TreeRender => write!(f, "LixStateScanExec"),
        }
    }
}

impl ExecutionPlan for LixStateScanExec {
    fn name(&self) -> &str {
        "LixStateScanExec"
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
                "LixStateScanExec does not accept children".to_string(),
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
                "LixStateScanExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let schema = Arc::clone(&self.schema);
        let request = self.request.clone();
        let stream_schema = Arc::clone(&schema);
        let stream = stream::once(async move {
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                live_state
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let batch = lix_state_record_batch(Arc::clone(&stream_schema), &rows)
                .map_err(lix_error_to_datafusion_error)?;
            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                batch,
            )]))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn lix_state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, false),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, false),
    ]))
}

fn lix_state_by_version_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("snapshot_content", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("schema_version", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, false),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, false),
        Field::new("version_id", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixStateByVersionRoute {
    schema_keys: Option<BTreeSet<String>>,
    version_ids: Option<BTreeSet<String>>,
    entity_ids: Option<BTreeSet<String>>,
    file_id: Option<NullableKeyFilter<String>>,
    plugin_key: Option<NullableKeyFilter<String>>,
    contradictory: bool,
}

impl LixStateByVersionRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_lix_state_filter(filter) else {
                continue;
            };
            match predicate {
                LixStateFilterPredicate::SchemaKeys(values) => {
                    merge_string_route_slot(
                        &mut route.schema_keys,
                        values,
                        &mut route.contradictory,
                    );
                }
                LixStateFilterPredicate::VersionIds(values) => {
                    merge_string_route_slot(
                        &mut route.version_ids,
                        values,
                        &mut route.contradictory,
                    );
                }
                LixStateFilterPredicate::EntityIds(values) => {
                    merge_string_route_slot(
                        &mut route.entity_ids,
                        values,
                        &mut route.contradictory,
                    );
                }
                LixStateFilterPredicate::FileId(filter) => {
                    merge_nullable_key_route_slot(
                        &mut route.file_id,
                        filter,
                        &mut route.contradictory,
                    );
                }
                LixStateFilterPredicate::PluginKey(filter) => {
                    merge_nullable_key_route_slot(
                        &mut route.plugin_key,
                        filter,
                        &mut route.contradictory,
                    );
                }
            }
        }
        route
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LixStateFilterPredicate {
    SchemaKeys(BTreeSet<String>),
    VersionIds(BTreeSet<String>),
    EntityIds(BTreeSet<String>),
    FileId(NullableKeyFilter<String>),
    PluginKey(NullableKeyFilter<String>),
}

fn lix_state_scan_request(
    schema: &SchemaRef,
    default_version_id: Option<&str>,
    projection: Option<&Vec<usize>>,
    route: &LixStateByVersionRoute,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    let projection = LiveStateProjection {
        columns: projection_column_names(schema, projection),
    };
    let mut filter = LiveStateFilter {
        schema_keys: route
            .schema_keys
            .as_ref()
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default(),
        entity_ids: route
            .entity_ids
            .as_ref()
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default(),
        version_ids: default_version_id
            .map(|value| vec![value.to_string()])
            .or_else(|| {
                route
                    .version_ids
                    .as_ref()
                    .map(|values| values.iter().cloned().collect())
            })
            .unwrap_or_default(),
        ..LiveStateFilter::default()
    };
    if let Some(file_id) = route.file_id.clone() {
        filter.file_ids.push(file_id);
    }
    if let Some(plugin_key) = route.plugin_key.clone() {
        filter.plugin_keys.push(plugin_key);
    }

    LiveStateScanRequest {
        filter,
        projection,
        limit: route.contradictory.then_some(0).or(limit),
    }
}

fn projection_column_names(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Vec<String> {
    projection
        .map(|indices| {
            indices
                .iter()
                .filter_map(|index| schema.fields().get(*index))
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn merge_string_route_slot(
    slot: &mut Option<BTreeSet<String>>,
    values: BTreeSet<String>,
    contradictory: &mut bool,
) {
    if values.is_empty() {
        return;
    }

    match slot {
        Some(existing) => {
            existing.retain(|value| values.contains(value));
            if existing.is_empty() {
                *contradictory = true;
            }
        }
        None => *slot = Some(values),
    }
}

fn merge_nullable_key_route_slot(
    slot: &mut Option<NullableKeyFilter<String>>,
    value: NullableKeyFilter<String>,
    contradictory: &mut bool,
) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

fn parse_lix_state_filter(expr: &Expr) -> Option<LixStateFilterPredicate> {
    match expr {
        Expr::BinaryExpr(binary_expr) => parse_lix_state_binary_filter(binary_expr),
        Expr::InList(in_list) => parse_lix_state_in_list_filter(in_list),
        Expr::IsNull(expr) => parse_lix_state_null_filter(expr),
        _ => None,
    }
}

fn parse_lix_state_binary_filter(binary_expr: &BinaryExpr) -> Option<LixStateFilterPredicate> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_lix_state_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_lix_state_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_lix_state_in_list_filter(in_list: &InList) -> Option<LixStateFilterPredicate> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };

    let values = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }

    let values = values.into_iter().collect::<BTreeSet<_>>();
    match column.name.as_str() {
        "schema_key" => Some(LixStateFilterPredicate::SchemaKeys(values)),
        "version_id" => Some(LixStateFilterPredicate::VersionIds(values)),
        "entity_id" => Some(LixStateFilterPredicate::EntityIds(values)),
        _ => None,
    }
}

fn parse_lix_state_null_filter(expr: &Expr) -> Option<LixStateFilterPredicate> {
    let Expr::Column(column) = expr else {
        return None;
    };

    match column.name.as_str() {
        "file_id" => Some(LixStateFilterPredicate::FileId(NullableKeyFilter::Null)),
        "plugin_key" => Some(LixStateFilterPredicate::PluginKey(NullableKeyFilter::Null)),
        _ => None,
    }
}

fn parse_lix_state_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<LixStateFilterPredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };

    match column.name.as_str() {
        "schema_key" => string_expr_literal(literal_expr)
            .map(|value| LixStateFilterPredicate::SchemaKeys(BTreeSet::from([value]))),
        "version_id" => string_expr_literal(literal_expr)
            .map(|value| LixStateFilterPredicate::VersionIds(BTreeSet::from([value]))),
        "entity_id" => string_expr_literal(literal_expr)
            .map(|value| LixStateFilterPredicate::EntityIds(BTreeSet::from([value]))),
        "file_id" => nullable_key_literal(literal_expr).map(LixStateFilterPredicate::FileId),
        "plugin_key" => nullable_key_literal(literal_expr).map(LixStateFilterPredicate::PluginKey),
        _ => None,
    }
}

fn nullable_key_literal(expr: &Expr) -> Option<NullableKeyFilter<String>> {
    if is_null_literal(expr) {
        return Some(NullableKeyFilter::Null);
    }
    string_expr_literal(expr).map(NullableKeyFilter::Value)
}

fn string_expr_literal(expr: &Expr) -> Option<String> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Null, _))
}

fn lix_state_record_batch(schema: SchemaRef, rows: &[StateRow]) -> Result<RecordBatch, LixError> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sql2 failed to build zero-column lix_state batch: {error}"),
            )
        });
    }

    let columns = schema
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
                    string_array(rows.iter().map(|row| row.schema_version.as_deref()))
                }
                "created_at" => string_array(rows.iter().map(|row| row.created_at.as_deref())),
                "updated_at" => string_array(rows.iter().map(|row| row.updated_at.as_deref())),
                "global" => Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.global).collect::<Vec<_>>(),
                )) as ArrayRef,
                "change_id" => string_array(rows.iter().map(|row| row.change_id.as_deref())),
                "commit_id" => string_array(rows.iter().map(|row| row.commit_id.as_deref())),
                "untracked" => Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.untracked).collect::<Vec<_>>(),
                )) as ArrayRef,
                "version_id" => string_array(rows.iter().map(|row| Some(row.version_id.as_str()))),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("sql2 does not support lix_state column '{other}'"),
                    ))
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, columns).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("sql2 failed to build lix_state_by_version batch: {error}"),
        )
    })
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };

    let projected = schema.project(projection).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to project lix_state schema: {error}"))
    })?;
    Ok(Arc::new(projected))
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 live-state provider error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        lix_state_scan_request, lix_state_schema, lix_state_write_rows_from_batch,
        parse_lix_state_filter, register_lix_state_providers, LixStateByVersionRoute,
        LixStateDeleteExec, LixStateFilterPredicate, LixStateInsertSink, LixStateProvider,
        LixStateUpdateExec,
    };
    use crate::engine2::live_state::{LiveStateContext, LiveStateRowRequest, LiveStateScanRequest};
    use crate::sql2::StateRow;
    use crate::sql2::{SqlWriteIntent, SqlWriteOutcome, SqlWriteStager, StateRow};
    use crate::transaction::{PendingOverlay, PreparedWriteStatementStager, TransactionWriteDelta};
    use crate::{LixError, NullableKeyFilter};
    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Column, DataFusionError};
    use datafusion::datasource::sink::{DataSink, DataSinkExec};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    };
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use futures_util::stream;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    struct EmptyLiveStateContext;
    struct RowsLiveStateContext {
        rows: Vec<StateRow>,
    }
    struct DummyWriteStager;
    #[derive(Default)]
    struct CapturingWriteStager {
        writes: Mutex<Vec<SqlWriteIntent>>,
    }
    #[derive(Default)]
    struct CapturingBufferedWriteStager {
        deltas: Vec<TransactionWriteDelta>,
    }

    struct SingleBatchExec {
        batch: RecordBatch,
        properties: Arc<PlanProperties>,
    }

    impl std::fmt::Debug for SingleBatchExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SingleBatchExec").finish()
        }
    }

    impl SingleBatchExec {
        fn new(batch: RecordBatch) -> Self {
            let properties = PlanProperties::new(
                EquivalenceProperties::new(batch.schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                batch,
                properties: Arc::new(properties),
            }
        }
    }

    impl DisplayAs for SingleBatchExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "SingleBatchExec")
        }
    }

    impl ExecutionPlan for SingleBatchExec {
        fn name(&self) -> &str {
            "SingleBatchExec"
        }

        fn as_any(&self) -> &dyn std::any::Any {
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
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            if !children.is_empty() {
                return Err(DataFusionError::Execution(
                    "SingleBatchExec does not accept children".to_string(),
                ));
            }
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion::common::Result<SendableRecordBatchStream> {
            if partition != 0 {
                return Err(DataFusionError::Execution(format!(
                    "SingleBatchExec only exposes one partition, got {partition}"
                )));
            }

            let batch = self.batch.clone();
            let schema = batch.schema();
            let stream = stream::iter(vec![Ok(batch)]);
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    #[async_trait]
    impl LiveStateContext for EmptyLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<StateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_exact(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<StateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl LiveStateContext for RowsLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<StateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_exact(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<StateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl SqlWriteStager for DummyWriteStager {
        async fn stage_write(&self, _write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
            Ok(SqlWriteOutcome { count: 0 })
        }
    }

    #[async_trait]
    impl SqlWriteStager for CapturingWriteStager {
        async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
            self.writes.lock().expect("writes lock").push(write);
            Ok(SqlWriteOutcome { count: 0 })
        }
    }

    impl PreparedWriteStatementStager for CapturingBufferedWriteStager {
        fn mark_public_surface_registry_refresh_pending(&mut self) {}

        fn stage_transaction_write_delta(
            &mut self,
            delta: TransactionWriteDelta,
        ) -> Result<(), LixError> {
            self.deltas.push(delta);
            Ok(())
        }
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn str_lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn one_row_lix_state_batch(global: bool) -> RecordBatch {
        RecordBatch::try_new(
            lix_state_schema(),
            vec![
                string_column(vec![Some("entity-1")]),
                string_column(vec![Some("lix_key_value")]),
                string_column(vec![None]),
                string_column(vec![Some("plugin-a")]),
                string_column(vec![Some("{\"key\":\"hello\",\"value\":\"world\"}")]),
                string_column(vec![Some("{\"source\":\"test\"}")]),
                string_column(vec![Some("1")]),
                string_column(vec![Some("2026-04-23T00:00:00Z")]),
                string_column(vec![Some("2026-04-23T01:00:00Z")]),
                Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
                string_column(vec![Some("change-a")]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .expect("valid lix_state batch")
    }

    fn one_row_stageable_lix_state_batch() -> RecordBatch {
        RecordBatch::try_new(
            lix_state_schema(),
            vec![
                string_column(vec![Some("entity-1")]),
                string_column(vec![Some("lix_key_value")]),
                string_column(vec![None]),
                string_column(vec![None]),
                string_column(vec![Some("{\"key\":\"hello\",\"value\":\"world\"}")]),
                string_column(vec![None]),
                string_column(vec![Some("1")]),
                string_column(vec![None]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                string_column(vec![None]),
                string_column(vec![None]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ],
        )
        .expect("valid stageable lix_state batch")
    }

    fn live_row(entity_id: &str, metadata: Option<&str>) -> StateRow {
        StateRow {
            entity_id: entity_id.to_string(),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
            metadata: metadata.map(ToOwned::to_owned),
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: Some("2026-04-23T00:00:00Z".to_string()),
            updated_at: Some("2026-04-23T01:00:00Z".to_string()),
        }
    }

    #[test]
    fn parses_eq_filter_for_schema_key() {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("schema_key")),
            Operator::Eq,
            Box::new(str_lit("profile")),
        ));

        assert_eq!(
            parse_lix_state_filter(&expr),
            Some(LixStateFilterPredicate::SchemaKeys(BTreeSet::from([
                "profile".to_string(),
            ])))
        );
    }

    #[test]
    fn parses_in_list_filter_for_version_id() {
        let expr = Expr::InList(InList::new(
            Box::new(col("version_id")),
            vec![str_lit("a"), str_lit("b")],
            false,
        ));

        assert_eq!(
            parse_lix_state_filter(&expr),
            Some(LixStateFilterPredicate::VersionIds(BTreeSet::from([
                "a".to_string(),
                "b".to_string(),
            ])))
        );
    }

    #[test]
    fn builds_scan_request_from_route_and_projection() {
        let schema = super::lix_state_by_version_schema();
        let route = LixStateByVersionRoute::from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("profile")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("version_id")),
                Operator::Eq,
                Box::new(str_lit("v1")),
            )),
            Expr::IsNull(Box::new(col("file_id"))),
        ]);

        let request =
            lix_state_scan_request(&schema, None, Some(&vec![0, 1, 13]), &route, Some(10));

        assert_eq!(request.filter.schema_keys, vec!["profile".to_string()]);
        assert_eq!(request.filter.version_ids, vec!["v1".to_string()]);
        assert_eq!(request.filter.file_ids, vec![NullableKeyFilter::Null]);
        assert_eq!(
            request.projection.columns,
            vec![
                "entity_id".to_string(),
                "schema_key".to_string(),
                "version_id".to_string()
            ]
        );
        assert_eq!(request.limit, Some(10));
    }

    #[test]
    fn contradictory_filters_turn_into_zero_limit_request() {
        let schema = super::lix_state_by_version_schema();
        let route = LixStateByVersionRoute::from_filters(&[
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("a")),
            )),
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("schema_key")),
                Operator::Eq,
                Box::new(str_lit("b")),
            )),
        ]);

        let request = lix_state_scan_request(&schema, None, None, &route, None);

        assert_eq!(request.limit, Some(0));
        assert!(request.filter.schema_keys.is_empty());
    }

    #[test]
    fn active_version_view_pins_version_filter() {
        let schema = super::lix_state_schema();
        let route = LixStateByVersionRoute::from_filters(&[Expr::BinaryExpr(BinaryExpr::new(
            Box::new(col("schema_key")),
            Operator::Eq,
            Box::new(str_lit("profile")),
        ))]);

        let request = lix_state_scan_request(&schema, Some("version-a"), None, &route, None);

        assert_eq!(request.filter.schema_keys, vec!["profile".to_string()]);
        assert_eq!(request.filter.version_ids, vec!["version-a".to_string()]);
    }

    #[tokio::test]
    async fn registers_active_lix_state_with_write_context_only() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let write_stager = Arc::new(DummyWriteStager) as Arc<dyn SqlWriteStager>;

        register_lix_state_providers(
            &session,
            "version-a",
            live_state,
            Some(Arc::clone(&write_stager)),
        )
        .await
        .expect("lix_state providers should register");

        let lix_state = session
            .table_provider("lix_state")
            .await
            .expect("lix_state provider should exist");
        let lix_state = lix_state
            .as_any()
            .downcast_ref::<LixStateProvider>()
            .expect("lix_state should be a LixStateProvider");
        assert!(lix_state.write_stager.is_some());

        let by_version = session
            .table_provider("lix_state_by_version")
            .await
            .expect("lix_state_by_version provider should exist");
        let by_version = by_version
            .as_any()
            .downcast_ref::<LixStateProvider>()
            .expect("lix_state_by_version should be a LixStateProvider");
        assert!(by_version.write_stager.is_none());
    }

    #[tokio::test]
    async fn insert_into_requires_write_transaction() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let provider = LixStateProvider::active_version("version-a", live_state, None);
        let input = Arc::new(EmptyExec::new(provider.schema())) as Arc<dyn ExecutionPlan>;

        let error = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect_err("insert without a write stager should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn update_requires_write_transaction() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let provider = LixStateProvider::active_version("version-a", live_state, None);

        let error = provider
            .update(
                &session.state(),
                vec![("metadata".to_string(), str_lit("{\"source\":\"update\"}"))],
                vec![],
            )
            .await
            .expect_err("update without a write stager should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn delete_requires_write_transaction() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let provider = LixStateProvider::active_version("version-a", live_state, None);

        let error = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect_err("delete without a write stager should fail");

        assert!(
            error.to_string().contains("requires a write transaction"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn delete_returns_lix_state_delete_exec_with_write_stager() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let write_stager = Arc::new(DummyWriteStager) as Arc<dyn SqlWriteStager>;
        let provider =
            LixStateProvider::active_version("version-a", live_state, Some(write_stager));

        let plan = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect("delete should produce a write plan");

        assert!(plan.as_any().is::<LixStateDeleteExec>());
    }

    #[tokio::test]
    async fn update_rejects_read_only_lix_state_columns() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let write_stager = Arc::new(DummyWriteStager) as Arc<dyn SqlWriteStager>;
        let provider =
            LixStateProvider::active_version("version-a", live_state, Some(write_stager));

        let error = provider
            .update(
                &session.state(),
                vec![("entity_id".to_string(), str_lit("entity-2"))],
                vec![],
            )
            .await
            .expect_err("updating a read-only field should fail");

        assert!(
            error.to_string().contains("read-only column 'entity_id'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn update_returns_lix_state_update_exec_with_write_stager() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let write_stager = Arc::new(DummyWriteStager) as Arc<dyn SqlWriteStager>;
        let provider =
            LixStateProvider::active_version("version-a", live_state, Some(write_stager));

        let plan = provider
            .update(
                &session.state(),
                vec![("metadata".to_string(), str_lit("{\"source\":\"update\"}"))],
                vec![],
            )
            .await
            .expect("update should produce a write plan");

        assert!(plan.as_any().is::<LixStateUpdateExec>());
    }

    #[tokio::test]
    async fn insert_into_returns_data_sink_exec_with_write_stager() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let write_stager = Arc::new(DummyWriteStager) as Arc<dyn SqlWriteStager>;
        let provider =
            LixStateProvider::active_version("version-a", live_state, Some(write_stager));
        let input = Arc::new(EmptyExec::new(provider.schema())) as Arc<dyn ExecutionPlan>;

        let plan = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect("insert should produce a write plan");

        assert!(plan.as_any().is::<DataSinkExec>());
    }

    #[test]
    fn decodes_lix_state_batch_into_write_rows() {
        let rows = lix_state_write_rows_from_batch(&one_row_lix_state_batch(false), "version-a")
            .expect("batch should decode");

        assert_eq!(
            rows,
            vec![StateRow {
                entity_id: "entity-1".to_string(),
                schema_key: "lix_key_value".to_string(),
                file_id: None,
                plugin_key: Some("plugin-a".to_string()),
                snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
                metadata: Some("{\"source\":\"test\"}".to_string()),
                schema_version: Some("1".to_string()),
                created_at: Some("2026-04-23T00:00:00Z".to_string()),
                updated_at: Some("2026-04-23T01:00:00Z".to_string()),
                global: false,
                change_id: Some("change-a".to_string()),
                commit_id: None,
                untracked: false,
                version_id: "version-a".to_string(),
            }]
        );
    }

    #[test]
    fn decodes_global_lix_state_batch_into_global_version() {
        let rows = lix_state_write_rows_from_batch(&one_row_lix_state_batch(true), "version-a")
            .expect("batch should decode");

        assert_eq!(rows[0].version_id, "global");
        assert!(rows[0].global);
    }

    #[tokio::test]
    async fn insert_sink_stages_decoded_lix_state_rows() {
        let stager = Arc::new(CapturingWriteStager::default());
        let sink = LixStateInsertSink::new(
            lix_state_schema(),
            Arc::clone(&stager) as Arc<dyn SqlWriteStager>,
            "version-a".to_string(),
        );
        let batch = one_row_lix_state_batch(false);
        let stream = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(lix_state_schema(), stream));

        let count = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            stager.writes.lock().expect("writes lock").as_slice(),
            &[SqlWriteIntent::WriteRows {
                rows: vec![StateRow {
                    entity_id: "entity-1".to_string(),
                    schema_key: "lix_key_value".to_string(),
                    file_id: None,
                    plugin_key: Some("plugin-a".to_string()),
                    snapshot_content: Some("{\"key\":\"hello\",\"value\":\"world\"}".to_string()),
                    metadata: Some("{\"source\":\"test\"}".to_string()),
                    schema_version: Some("1".to_string()),
                    created_at: Some("2026-04-23T00:00:00Z".to_string()),
                    updated_at: Some("2026-04-23T01:00:00Z".to_string()),
                    global: false,
                    change_id: Some("change-a".to_string()),
                    commit_id: None,
                    untracked: false,
                    version_id: "version-a".to_string(),
                }]
            }]
        );
    }

    #[tokio::test]
    async fn insert_sink_stages_through_buffered_transaction_delta() {
        let stager = Arc::new(Mutex::new(CapturingBufferedWriteStager::default()));
        let sink = LixStateInsertSink::new(
            lix_state_schema(),
            Arc::clone(&stager) as Arc<dyn SqlWriteStager>,
            "version-a".to_string(),
        );
        let batch = one_row_stageable_lix_state_batch();
        let stream = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(lix_state_schema(), stream));

        let count = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("sink should stage through buffered path");

        assert_eq!(count, 1);
        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
        let overlay = stager.deltas[0]
            .pending_write_overlay()
            .expect("staged delta should expose pending overlay");
        let rows = overlay.visible_semantic_rows(false, "lix_key_value");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
    }

    #[tokio::test]
    async fn insert_plan_returns_datafusion_count_uint64() {
        let session = SessionContext::new();
        let live_state = Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>;
        let stager = Arc::new(Mutex::new(CapturingBufferedWriteStager::default()));
        let provider = LixStateProvider::active_version(
            "version-a",
            live_state,
            Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
        );
        let input = Arc::new(SingleBatchExec::new(one_row_stageable_lix_state_batch()))
            as Arc<dyn ExecutionPlan>;

        let plan = provider
            .insert_into(&session.state(), input, InsertOp::Append)
            .await
            .expect("insert should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("insert write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        assert!(!batches[0].schema().field(0).is_nullable());

        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 1);

        let stager = stager.lock().expect("stager lock");
        assert_eq!(stager.deltas.len(), 1);
    }

    #[tokio::test]
    async fn update_plan_evaluates_filters_assignments_and_stages_rows() {
        let session = SessionContext::new();
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_row("entity-1", Some("{\"source\":\"match\"}")),
                live_row("entity-2", Some("{\"source\":\"skip\"}")),
            ],
        }) as Arc<dyn LiveStateContext>;
        let stager = Arc::new(CapturingWriteStager::default());
        let provider = LixStateProvider::active_version(
            "version-a",
            live_state,
            Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
        );

        let plan = provider
            .update(
                &session.state(),
                vec![
                    (
                        "snapshot_content".to_string(),
                        str_lit("{\"key\":\"hello\",\"value\":\"updated\"}"),
                    ),
                    ("metadata".to_string(), col("schema_key")),
                ],
                vec![Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(col("metadata")),
                    Operator::Eq,
                    Box::new(str_lit("{\"source\":\"match\"}")),
                ))],
            )
            .await
            .expect("update should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("update write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 1);

        assert_eq!(
            stager.writes.lock().expect("writes lock").as_slice(),
            &[SqlWriteIntent::WriteRows {
                rows: vec![StateRow {
                    entity_id: "entity-1".to_string(),
                    schema_key: "lix_key_value".to_string(),
                    file_id: None,
                    plugin_key: None,
                    snapshot_content: Some("{\"key\":\"hello\",\"value\":\"updated\"}".to_string()),
                    metadata: Some("lix_key_value".to_string()),
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

    #[tokio::test]
    async fn delete_plan_with_empty_filters_stages_all_visible_rows() {
        let session = SessionContext::new();
        let live_state = Arc::new(RowsLiveStateContext {
            rows: vec![
                live_row("entity-1", Some("{\"source\":\"one\"}")),
                live_row("entity-2", Some("{\"source\":\"two\"}")),
            ],
        }) as Arc<dyn LiveStateContext>;
        let stager = Arc::new(CapturingWriteStager::default());
        let provider = LixStateProvider::active_version(
            "version-a",
            live_state,
            Some(Arc::clone(&stager) as Arc<dyn SqlWriteStager>),
        );

        let plan = provider
            .delete_from(&session.state(), vec![])
            .await
            .expect("delete should produce a write plan");
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect("delete write plan should execute");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "count");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::UInt64);
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("count should be UInt64");
        assert_eq!(count.value(0), 2);

        assert_eq!(
            stager.writes.lock().expect("writes lock").as_slice(),
            &[SqlWriteIntent::WriteRows {
                rows: vec![
                    StateRow {
                        entity_id: "entity-1".to_string(),
                        schema_key: "lix_key_value".to_string(),
                        file_id: None,
                        plugin_key: None,
                        snapshot_content: None,
                        metadata: Some("{\"source\":\"one\"}".to_string()),
                        schema_version: Some("1".to_string()),
                        created_at: None,
                        updated_at: None,
                        global: false,
                        change_id: None,
                        commit_id: None,
                        untracked: false,
                        version_id: "version-a".to_string(),
                    },
                    StateRow {
                        entity_id: "entity-2".to_string(),
                        schema_key: "lix_key_value".to_string(),
                        file_id: None,
                        plugin_key: None,
                        snapshot_content: None,
                        metadata: Some("{\"source\":\"two\"}".to_string()),
                        schema_version: Some("1".to_string()),
                        created_at: None,
                        updated_at: None,
                        global: false,
                        change_id: None,
                        commit_id: None,
                        untracked: false,
                        version_id: "version-a".to_string(),
                    },
                ]
            }]
        );
    }
}
