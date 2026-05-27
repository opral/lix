use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, DFSchema, DataFusionError, Result, ScalarValue, SchemaExt};
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
use futures_util::{stream, TryStreamExt};
use serde_json::Value as JsonValue;

use crate::branch::{
    branch_descriptor_stage_row, branch_descriptor_tombstone_row, branch_ref_stage_row,
    branch_ref_tombstone_row, BranchRefReader,
};
use crate::live_state::{
    LiveStateFilter, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::record_batch::record_batch_with_row_count;
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextBranchRefReader, WriteContextLiveStateReader,
};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionWrite, TransactionWriteMode, TransactionWriteOperation,
    TransactionWriteOrigin, TransactionWriteRow,
};
use crate::LixError;
use crate::GLOBAL_BRANCH_ID;

pub(super) async fn register_lix_branch_read_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixBranchProvider::new(live_state, branch_ref)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) async fn register_write_provider(
    session: &datafusion::prelude::SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixBranchProvider::with_write(write_ctx)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

struct LixBranchProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    write_access: WriteAccess,
}

impl std::fmt::Debug for LixBranchProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixBranchProvider").finish()
    }
}

impl LixBranchProvider {
    fn new(live_state: Arc<dyn LiveStateReader>, branch_ref: Arc<dyn BranchRefReader>) -> Self {
        Self {
            schema: lix_branch_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::read_only(),
        }
    }

    fn with_write(write_ctx: SqlWriteContext) -> Self {
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        Self {
            schema: lix_branch_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::write(write_ctx),
        }
    }
}

#[async_trait]
impl TableProvider for LixBranchProvider {
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
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LixBranchScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.branch_ref),
            projected_schema(&self.schema, projection),
            projection.cloned(),
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return not_impl_err!("{insert_op} not implemented for lix_branch yet");
        }
        let write_ctx = self.write_access.require_write("INSERT into lix_branch")?;
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let sink = LixBranchInsertSink::new(write_ctx);
        Ok(Arc::new(InsertExec::new(input, Arc::new(sink))))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self.write_access.require_write("DELETE FROM lix_branch")?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(LixBranchDeleteExec::new(
            write_ctx,
            Arc::clone(&self.live_state),
            Arc::clone(&self.branch_ref),
            Arc::clone(&self.schema),
            physical_filters,
        )))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self.write_access.require_write("UPDATE lix_branch")?;
        validate_lix_branch_update_assignments(&assignments)?;
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
        Ok(Arc::new(LixBranchUpdateExec::new(
            write_ctx,
            Arc::clone(&self.live_state),
            Arc::clone(&self.branch_ref),
            Arc::clone(&self.schema),
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixBranchInsertSink {
    write_ctx: SqlWriteContext,
}

impl std::fmt::Debug for LixBranchInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixBranchInsertSink").finish()
    }
}

impl LixBranchInsertSink {
    fn new(write_ctx: SqlWriteContext) -> Self {
        Self { write_ctx }
    }
}

impl DisplayAs for LixBranchInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixBranchInsertSink")
            }
            DisplayFormatType::TreeRender => write!(f, "LixBranchInsertSink"),
        }
    }
}

#[async_trait]
impl InsertSink for LixBranchInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let default_commit_id = self
            .write_ctx
            .load_branch_head(&self.write_ctx.active_branch_id())
            .await
            .map_err(lix_error_to_datafusion_error)?
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "INSERT into lix_branch could not resolve active branch head".to_string(),
                )
            })?;
        let mut rows = Vec::new();
        let mut count = 0u64;
        for batch in batches {
            let branch_rows = branch_insert_rows_from_batch(&batch, &default_commit_id)?;
            count = count
                .checked_add(u64::try_from(branch_rows.len()).map_err(|_| {
                    DataFusionError::Execution("INSERT row count overflow".to_string())
                })?)
                .ok_or_else(|| DataFusionError::Execution("INSERT row count overflow".into()))?;
            rows.extend(branch_rows.into_iter().flat_map(branch_insert_stage_rows));
        }

        if !rows.is_empty() {
            self.write_ctx
                .stage_write(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows,
                })
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }

        Ok(count)
    }
}

struct LixBranchDeleteExec {
    write_ctx: SqlWriteContext,
    active_branch_id: String,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    table_schema: SchemaRef,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixBranchDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixBranchDeleteExec").finish()
    }
}

impl LixBranchDeleteExec {
    fn new(
        write_ctx: SqlWriteContext,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        table_schema: SchemaRef,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = dml_plan_properties(Arc::clone(&result_schema));
        let active_branch_id = write_ctx.active_branch_id();
        Self {
            write_ctx,
            active_branch_id,
            live_state,
            branch_ref,
            table_schema,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixBranchDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixBranchDeleteExec(filters={})", self.filters.len())
            }
            DisplayFormatType::TreeRender => write!(f, "LixBranchDeleteExec"),
        }
    }
}

impl ExecutionPlan for LixBranchDeleteExec {
    fn name(&self) -> &str {
        "LixBranchDeleteExec"
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
                "LixBranchDeleteExec does not accept children".to_string(),
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
                "LixBranchDeleteExec only exposes one partition, got {partition}"
            )));
        }
        let write_ctx = self.write_ctx.clone();
        let active_branch_id = self.active_branch_id.clone();
        let live_state = Arc::clone(&self.live_state);
        let branch_ref = Arc::clone(&self.branch_ref);
        let filters = self.filters.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = load_branch_rows(live_state, branch_ref)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = branch_record_batch(&branch_projection_for_scan(None), &rows)?;
            let matched_batch = filter_branch_batch(source_batch, &filters)?;
            let branch_rows = branch_rows_from_batch(&matched_batch)?;
            reject_protected_branch_deletes(&branch_rows, &active_branch_id)?;
            let count = u64::try_from(branch_rows.len())
                .map_err(|_| DataFusionError::Execution("DELETE row count overflow".to_string()))?;
            let rows = branch_rows
                .into_iter()
                .flat_map(branch_tombstone_rows)
                .collect::<Vec<_>>();

            if !rows.is_empty() {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows,
                    })
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            }

            let _ = table_schema;
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

struct LixBranchUpdateExec {
    write_ctx: SqlWriteContext,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    table_schema: SchemaRef,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixBranchUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixBranchUpdateExec").finish()
    }
}

impl LixBranchUpdateExec {
    fn new(
        write_ctx: SqlWriteContext,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        table_schema: SchemaRef,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = dml_plan_properties(Arc::clone(&result_schema));
        Self {
            write_ctx,
            live_state,
            branch_ref,
            table_schema,
            assignments,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixBranchUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixBranchUpdateExec(assignments={}, filters={})",
                    self.assignments.len(),
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LixBranchUpdateExec"),
        }
    }
}

impl ExecutionPlan for LixBranchUpdateExec {
    fn name(&self) -> &str {
        "LixBranchUpdateExec"
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
                "LixBranchUpdateExec does not accept children".to_string(),
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
                "LixBranchUpdateExec only exposes one partition, got {partition}"
            )));
        }
        let write_ctx = self.write_ctx.clone();
        let live_state = Arc::clone(&self.live_state);
        let branch_ref = Arc::clone(&self.branch_ref);
        let table_schema = Arc::clone(&self.table_schema);
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = load_branch_rows(live_state, branch_ref)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = branch_record_batch(&branch_projection_for_scan(None), &rows)?;
            let matched_batch = filter_branch_batch(source_batch, &filters)?;
            let branch_rows =
                branch_update_rows_from_batch(&matched_batch, &assignments, &table_schema)?;
            reject_protected_branch_updates(&branch_rows)?;
            let count = u64::try_from(branch_rows.len())
                .map_err(|_| DataFusionError::Execution("UPDATE row count overflow".to_string()))?;
            let rows = branch_rows
                .into_iter()
                .flat_map(branch_update_stage_rows)
                .collect::<Vec<_>>();

            if !rows.is_empty() {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows,
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

struct LixBranchScanExec {
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixBranchScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixBranchScanExec").finish()
    }
}

impl LixBranchScanExec {
    fn new(
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            branch_ref,
            schema,
            projection,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixBranchScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixBranchScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixBranchScanExec"),
        }
    }
}

impl ExecutionPlan for LixBranchScanExec {
    fn name(&self) -> &str {
        "LixBranchScanExec"
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
                "LixBranchScanExec does not accept children".to_string(),
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
                "LixBranchScanExec only exposes one partition, got {partition}"
            )));
        }

        let live_state = Arc::clone(&self.live_state);
        let branch_ref = Arc::clone(&self.branch_ref);
        let projection = branch_projection_for_scan(self.projection.as_ref());
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let rows = load_branch_rows(live_state, branch_ref)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            branch_record_batch(&projection, &rows)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchRow {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
}

#[derive(Debug, Clone, Copy)]
enum BranchColumn {
    Id,
    Name,
    Hidden,
    CommitId,
}

async fn load_branch_rows(
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
) -> Result<Vec<BranchRow>, LixError> {
    let descriptor_rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["lix_branch_descriptor".to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string()],
                ..LiveStateFilter::default()
            },
            projection: Default::default(),
            limit: None,
        })
        .await?;

    let mut out = Vec::new();
    for descriptor_row in descriptor_rows {
        let descriptor = parse_descriptor(&descriptor_row)?;
        let Some(commit_id) = branch_ref.load_head_commit_id(&descriptor.id).await? else {
            continue;
        };
        out.push(BranchRow {
            commit_id,
            id: descriptor.id,
            name: descriptor.name,
            hidden: descriptor.hidden,
        });
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchDescriptor {
    id: String,
    name: String,
    hidden: bool,
}

fn parse_descriptor(row: &MaterializedLiveStateRow) -> Result<BranchDescriptor, LixError> {
    let snapshot = parse_snapshot(row, "lix_branch_descriptor")?;
    let id = snapshot
        .get("id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "lix_branch_descriptor is missing id"))?
        .to_string();
    let name = snapshot
        .get("name")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "lix_branch_descriptor is missing name"))?
        .to_string();
    let hidden = snapshot
        .get("hidden")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    Ok(BranchDescriptor { id, name, hidden })
}

fn parse_snapshot(row: &MaterializedLiveStateRow, schema_key: &str) -> Result<JsonValue, LixError> {
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{schema_key} row is missing snapshot_content"),
        )
    })?;
    serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{schema_key} snapshot_content is invalid JSON: {error}"),
        )
    })
}

fn validate_lix_branch_update_assignments(assignments: &[(String, Expr)]) -> Result<()> {
    for (column_name, _) in assignments {
        match column_name.as_str() {
            "name" | "hidden" | "commit_id" => {}
            "id" => {
                return Err(DataFusionError::Execution(
                    "UPDATE lix_branch cannot change immutable column 'id'".to_string(),
                ));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "UPDATE lix_branch failed: column '{other}' does not exist"
                )));
            }
        }
    }
    Ok(())
}

fn filter_branch_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_branch_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_branch_filters(
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
                DataFusionError::Execution("lix_branch filter was not boolean".to_string())
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

fn branch_insert_rows_from_batch(
    batch: &RecordBatch,
    default_commit_id: &str,
) -> Result<Vec<BranchRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let id = required_string_value(batch, row_index, "id", "INSERT")?;
            let name = required_string_value(batch, row_index, "name", "INSERT")?;
            let hidden =
                optional_bool_value(batch, row_index, "hidden", "INSERT")?.unwrap_or(false);
            let commit_id = optional_string_value(batch, row_index, "commit_id", "INSERT")?
                .unwrap_or_else(|| default_commit_id.to_string());
            Ok(BranchRow {
                id,
                name,
                hidden,
                commit_id,
            })
        })
        .collect()
}

fn branch_rows_from_batch(batch: &RecordBatch) -> Result<Vec<BranchRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            Ok(BranchRow {
                id: required_string_value(batch, row_index, "id", "DELETE")?,
                name: required_string_value(batch, row_index, "name", "DELETE")?,
                hidden: required_bool_value(batch, row_index, "hidden", "DELETE")?,
                commit_id: required_string_value(batch, row_index, "commit_id", "DELETE")?,
            })
        })
        .collect()
}

fn reject_protected_branch_deletes(rows: &[BranchRow], active_branch_id: &str) -> Result<()> {
    for row in rows {
        if row.id == GLOBAL_BRANCH_ID {
            return Err(DataFusionError::Execution(
                "DELETE FROM lix_branch cannot delete the global branch".to_string(),
            ));
        }
        if row.id == active_branch_id {
            return Err(DataFusionError::Execution(format!(
                "DELETE FROM lix_branch cannot delete active branch '{}'",
                row.id
            )));
        }
    }
    Ok(())
}

fn reject_protected_branch_updates(rows: &[BranchRow]) -> Result<()> {
    for row in rows {
        if row.id == GLOBAL_BRANCH_ID {
            return Err(DataFusionError::Execution(
                "UPDATE lix_branch cannot update the global branch".to_string(),
            ));
        }
    }
    Ok(())
}

fn branch_update_rows_from_batch(
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    table_schema: &SchemaRef,
) -> Result<Vec<BranchRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    (0..batch.num_rows())
        .map(|row_index| {
            Ok(BranchRow {
                id: required_string_value(batch, row_index, "id", "UPDATE")?,
                name: update_string_value(
                    batch,
                    &assignment_values,
                    table_schema,
                    row_index,
                    "name",
                )?,
                hidden: update_bool_value(
                    batch,
                    &assignment_values,
                    table_schema,
                    row_index,
                    "hidden",
                )?,
                commit_id: update_string_value(
                    batch,
                    &assignment_values,
                    table_schema,
                    row_index,
                    "commit_id",
                )?,
            })
        })
        .collect()
}

fn branch_stage_rows(
    row: BranchRow,
    origin: Option<TransactionWriteOrigin>,
) -> Vec<TransactionWriteRow> {
    vec![
        with_origin(
            branch_descriptor_stage_row(&row.id, &row.name, row.hidden),
            origin.clone(),
        ),
        with_origin(branch_ref_stage_row(&row.id, &row.commit_id), origin),
    ]
}

fn branch_tombstone_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = Some(lix_branch_origin(
        TransactionWriteOperation::Delete,
        &row.id,
    ));
    vec![
        with_origin(branch_descriptor_tombstone_row(&row.id), origin.clone()),
        with_origin(branch_ref_tombstone_row(&row.id), origin),
    ]
}

fn branch_insert_stage_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = lix_branch_origin(TransactionWriteOperation::Insert, &row.id);
    branch_stage_rows(row, Some(origin))
}

fn branch_update_stage_rows(row: BranchRow) -> Vec<TransactionWriteRow> {
    let origin = lix_branch_origin(TransactionWriteOperation::Update, &row.id);
    branch_stage_rows(row, Some(origin))
}

fn with_origin(
    mut row: TransactionWriteRow,
    origin: Option<TransactionWriteOrigin>,
) -> TransactionWriteRow {
    row.origin = origin;
    row
}

fn lix_branch_origin(action: TransactionWriteOperation, branch_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: "lix_branch".to_string(),
        operation: action,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![branch_id.to_string()],
        }),
    }
}

fn update_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    table_schema: &SchemaRef,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    let column_index = table_schema.index_of(column_name)?;
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted => required_string_value(batch, row_index, column_name, "UPDATE"),
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(value),
        InsertCell::Provided(SqlCell::Null) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch requires non-null text column '{column_name}'"
        ))),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
    .or_else(|error| {
        if batch.column(column_index).is_null(row_index) {
            Err(DataFusionError::Execution(format!(
                "UPDATE lix_branch requires non-null text column '{column_name}'"
            )))
        } else {
            Err(error)
        }
    })
}

fn update_bool_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    table_schema: &SchemaRef,
    row_index: usize,
    column_name: &str,
) -> Result<bool> {
    let column_index = table_schema.index_of(column_name)?;
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted => required_bool_value(batch, row_index, column_name, "UPDATE"),
        InsertCell::Provided(SqlCell::Value(ScalarValue::Boolean(Some(value)))) => Ok(value),
        InsertCell::Provided(SqlCell::Null) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch requires non-null boolean column '{column_name}'"
        ))),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_branch expected boolean column '{column_name}', got {other:?}"
        ))),
    }
    .or_else(|error| {
        if batch.column(column_index).is_null(row_index) {
            Err(DataFusionError::Execution(format!(
                "UPDATE lix_branch requires non-null boolean column '{column_name}'"
            )))
        } else {
            Err(error)
        }
    })
}

fn required_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    action: &str,
) -> Result<String> {
    optional_string_value(batch, row_index, column_name, action)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{action} lix_branch requires non-null text column '{column_name}'"
        ))
    })
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    action: &str,
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
            "{action} lix_branch expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn required_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    action: &str,
) -> Result<bool> {
    optional_bool_value(batch, row_index, column_name, action)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "{action} lix_branch requires non-null boolean column '{column_name}'"
        ))
    })
}

fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    action: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None | Some(ScalarValue::Null) | Some(ScalarValue::Boolean(None)) => Ok(None),
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "{action} lix_branch expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let Ok(column_index) = batch.schema().index_of(column_name) else {
        return Ok(None);
    };
    Ok(Some(ScalarValue::try_from_array(
        batch.column(column_index).as_ref(),
        row_index,
    )?))
}

fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn dml_plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

fn dml_count_batch(schema: SchemaRef, count: u64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
    )
    .map_err(DataFusionError::from)
}

pub(super) fn lix_branch_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

fn branch_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<BranchColumn> {
    let all_columns = vec![
        BranchColumn::Id,
        BranchColumn::Name,
        BranchColumn::Hidden,
        BranchColumn::CommitId,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> SchemaRef {
    match projection {
        Some(projection) => Arc::new(schema.project(projection).expect("projection is valid")),
        None => Arc::clone(schema),
    }
}

fn branch_record_batch(projection: &[BranchColumn], rows: &[BranchRow]) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            BranchColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            BranchColumn::Name => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
            BranchColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            BranchColumn::CommitId => {
                string_array(rows.iter().map(|row| Some(row.commit_id.as_str())))
            }
        })
        .collect::<Vec<_>>();
    record_batch_with_row_count(branch_schema(projection), arrays, rows.len()).map_err(|error| {
        DataFusionError::Execution(format!("failed to build lix_branch batch: {error}"))
    })
}

fn branch_schema(projection: &[BranchColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                BranchColumn::Id => Field::new("id", DataType::Utf8, false),
                BranchColumn::Name => Field::new("name", DataType::Utf8, false),
                BranchColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                BranchColumn::CommitId => Field::new("commit_id", DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ))
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}
