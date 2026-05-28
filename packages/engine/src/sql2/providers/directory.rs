use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, RecordBatchOptions, StringArray, UInt64Array,
};
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
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};
use serde::Deserialize;

use crate::branch::BranchRefReader;
use crate::functions::FunctionProviderHandle;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::sql2::branch_scope::{
    explicit_branch_ids_from_dml_filters, resolve_provider_branch_ids, resolve_write_branch_scope,
    BranchBinding,
};
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::filesystem_predicates::{
    canonicalize_filesystem_path_filters, FilesystemPathKind,
};
use crate::sql2::predicate_typecheck::{
    canonicalize_json_identity_text_filters, validate_json_predicate_filters,
};
use crate::sql2::write_normalization::{InsertCell, SqlCell, UpdateAssignmentValues};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionJson, TransactionWriteOperation, TransactionWriteOrigin,
    TransactionWriteRow,
};
use crate::{parse_row_metadata_value, serialize_row_metadata, LixError};

use crate::sql2::filesystem_planner::{
    directory_descriptor_write_row, directory_path_resolvers_from_state_rows,
    filesystem_storage_scope_key, plan_recursive_directory_delete, DirectoryDescriptorWriteIntent,
    DirectoryPathResolver, FilesystemDeletePlan, FilesystemRowContext,
};
use crate::sql2::filesystem_visibility::VisibleFilesystem;
use crate::sql2::result_metadata::json_field;
use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextBranchRefReader, WriteContextLiveStateReader,
};
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

const DIRECTORY_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

pub(super) async fn register_lix_directory_active_provider(
    session: &SessionContext,
    surface_name: &str,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixDirectoryProvider::active_branch(
                active_branch_id,
                live_state,
                branch_ref,
                functions,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) async fn register_lix_directory_by_branch_provider(
    session: &SessionContext,
    surface_name: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixDirectoryProvider::by_branch(
                live_state, branch_ref, functions,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) async fn register_by_branch_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixDirectoryProvider::by_branch_with_write(write_ctx)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(super) async fn register_active_write_provider(
    session: &SessionContext,
    surface_name: &str,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    session
        .register_table(
            surface_name,
            Arc::new(LixDirectoryProvider::active_branch_with_write(write_ctx)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixDirectoryProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    write_access: WriteAccess,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
}

impl std::fmt::Debug for LixDirectoryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryProvider").finish()
    }
}

impl LixDirectoryProvider {
    fn active_branch(
        active_branch_id: impl Into<String>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::read_only(),
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn active_branch_with_write(write_ctx: SqlWriteContext) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        Self {
            schema: lix_directory_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::write(write_ctx),
            functions,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn by_branch(
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_directory_by_branch_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::read_only(),
            functions,
            branch_binding: BranchBinding::explicit(),
        }
    }

    fn by_branch_with_write(write_ctx: SqlWriteContext) -> Self {
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        Self {
            schema: lix_directory_by_branch_schema(),
            live_state,
            branch_ref,
            write_access: WriteAccess::write(write_ctx),
            functions,
            branch_binding: BranchBinding::explicit(),
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
            .map(|_| TableProviderFilterPushDown::Exact)
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
        let scan_limit = if filters.is_empty() { limit } else { None };
        let mut request = lix_directory_scan_request(
            self.branch_binding.active_branch_id(),
            Some(projected_schema.as_ref()),
            scan_limit,
        );
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let filters = canonicalize_filesystem_path_filters(filters, FilesystemPathKind::Directory)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, _state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(LixDirectoryScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.schema),
            projected_schema,
            projection.cloned(),
            request,
            physical_filters,
            limit,
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
        let write_ctx = self
            .write_access
            .require_write("INSERT into lix_directory")?;
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;
        let sink = LixDirectoryInsertSink::new(
            write_ctx,
            self.functions.clone(),
            self.branch_binding.clone(),
        );
        Ok(Arc::new(InsertExec::new(input, Arc::new(sink))))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self
            .write_access
            .require_write("DELETE FROM lix_directory")?;
        let filters =
            canonicalize_filesystem_path_filters(&filters, FilesystemPathKind::Directory)?;
        let filters = canonicalize_json_identity_text_filters(self.schema.as_ref(), &filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let mut request =
            lix_directory_scan_request(self.branch_binding.active_branch_id(), None, None);
        request.filter.branch_ids = explicit_branch_ids_from_dml_filters(&filters);
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        Ok(Arc::new(LixDirectoryDeleteExec::new(
            write_ctx,
            Arc::clone(&self.schema),
            self.branch_binding.clone(),
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
        let write_ctx = self.write_access.require_write("UPDATE lix_directory")?;
        validate_lix_directory_update_assignments(&self.schema, &assignments)?;
        let filters =
            canonicalize_filesystem_path_filters(&filters, FilesystemPathKind::Directory)?;
        let filters = canonicalize_json_identity_text_filters(self.schema.as_ref(), &filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
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
        let mut request =
            lix_directory_scan_request(self.branch_binding.active_branch_id(), None, None);
        request.filter.branch_ids = explicit_branch_ids_from_dml_filters(&filters);
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        Ok(Arc::new(LixDirectoryUpdateExec::new(
            write_ctx,
            Arc::clone(&self.schema),
            self.branch_binding.clone(),
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct LixDirectoryInsertSink {
    write_ctx: SqlWriteContext,
    functions: FunctionProviderHandle,
    branch_binding: BranchBinding,
    surface_name: &'static str,
}

impl std::fmt::Debug for LixDirectoryInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryInsertSink").finish()
    }
}

impl LixDirectoryInsertSink {
    fn new(
        write_ctx: SqlWriteContext,
        functions: FunctionProviderHandle,
        branch_binding: BranchBinding,
    ) -> Self {
        let surface_name = lix_directory_surface_name(&branch_binding);
        Self {
            write_ctx,
            functions,
            branch_binding,
            surface_name,
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
impl InsertSink for LixDirectoryInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut path_resolvers = None;
        let mut rows = Vec::new();
        let mut count = 0_u64;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(
                    directory_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(self.write_ctx.clone())),
                        self.branch_binding.active_branch_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            count = count
                .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?)
                .ok_or_else(|| {
                    DataFusionError::Execution("lix_directory INSERT row count overflow".into())
                })?;
            if record_batch_has_non_null_column(&batch, "path")? {
                rows.extend(lix_directory_write_rows_from_batch_with_path_resolvers(
                    &batch,
                    self.branch_binding.active_branch_id(),
                    self.surface_name,
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7().to_string(),
                )?);
            } else {
                rows.extend(
                    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
                        &batch,
                        self.branch_binding.active_branch_id(),
                        self.surface_name,
                        true,
                        path_resolvers.as_mut(),
                        None,
                    )?,
                );
            }
        }

        self.write_ctx
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

fn lix_directory_surface_name(branch_binding: &BranchBinding) -> &'static str {
    match branch_binding {
        BranchBinding::Active { .. } => "lix_directory",
        BranchBinding::Explicit => "lix_directory_by_branch",
    }
}

struct LixDirectoryDeleteExec {
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    branch_binding: BranchBinding,
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
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        branch_binding: BranchBinding,
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
            write_ctx,
            table_schema,
            branch_binding,
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
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let branch_binding = self.branch_binding.clone();
        let request = self.request.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = write_ctx
                .scan_live_state(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_directory_record_batch(&table_schema, rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_directory_batch(source_batch, &filters)?;
            let branch_ids =
                directory_branch_ids_from_batch(&matched_batch, branch_binding.active_branch_id())?;
            let mut visible_filesystems = BTreeMap::new();
            for branch_id in branch_ids {
                visible_filesystems.insert(
                    branch_id.clone(),
                    VisibleFilesystem::load(
                        Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                        &branch_id,
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            let (write_rows, count) = lix_directory_recursive_delete_rows_from_batch(
                &matched_batch,
                branch_binding.active_branch_id(),
                &visible_filesystems,
            )?;

            if count > 0 {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: write_rows,
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

struct LixDirectoryUpdateExec {
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    branch_binding: BranchBinding,
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
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        branch_binding: BranchBinding,
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
            write_ctx,
            table_schema,
            branch_binding,
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
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let branch_binding = self.branch_binding.clone();
        let request = self.request.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = write_ctx
                .scan_live_state(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_directory_record_batch(&table_schema, rows)
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_directory_batch(source_batch, &filters)?;
            let mut path_resolvers = directory_path_resolvers_from_live_state(
                Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                branch_binding.active_branch_id(),
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
            let write_rows = lix_directory_update_write_rows_from_batch(
                &matched_batch,
                &assignments,
                branch_binding.active_branch_id(),
                &mut path_resolvers,
            )?;
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("lix_directory UPDATE row count overflow".into())
            })?;

            if count > 0 {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: write_rows,
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

struct LixDirectoryScanExec {
    live_state: Arc<dyn LiveStateReader>,
    batch_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for LixDirectoryScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixDirectoryScanExec").finish()
    }
}

impl LixDirectoryScanExec {
    fn new(
        live_state: Arc<dyn LiveStateReader>,
        batch_schema: SchemaRef,
        output_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        request: LiveStateScanRequest,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            batch_schema,
            output_schema,
            projection,
            request,
            filters,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixDirectoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixDirectoryScanExec(limit={:?})", self.limit)
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
        let filters = self.filters.clone();
        let limit = self.limit;
        let output_schema = Arc::clone(&self.output_schema);
        let batch_schema = Arc::clone(&self.batch_schema);
        let projection = self.projection.clone();
        let fut = async move {
            let rows = live_state.scan_rows(&request).await.map_err(|error| {
                DataFusionError::Execution(format!("sql2 lix_directory scan failed: {error}"))
            })?;
            let batch = lix_directory_record_batch(&batch_schema, rows).map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 lix_directory batch build failed: {error}"
                ))
            })?;
            let filtered = filter_lix_directory_batch(batch, &filters)?;
            let projected = match projection {
                Some(indices) => filtered.project(&indices).map_err(DataFusionError::from),
                None => Ok(filtered),
            }?;
            match limit {
                Some(limit) => Ok(projected.slice(0, limit.min(projected.num_rows()))),
                None => Ok(projected),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
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
    live: MaterializedLiveStateRow,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
}

#[cfg(test)]
fn lix_directory_write_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options(batch, branch_binding, "lix_directory", true)
}

fn lix_directory_write_rows_from_batch_with_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        true,
        Some(path_resolvers),
        Some(generate_directory_id),
    )
}

fn lix_directory_update_write_rows_from_batch(
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    branch_binding: Option<&str>,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
) -> Result<Vec<TransactionWriteRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    let mut rows = Vec::new();
    for row_index in 0..batch.num_rows() {
        let id = optional_string_value(batch, row_index, "id")?;
        let context = directory_row_context_from_update(
            batch,
            &assignment_values,
            row_index,
            branch_binding,
        )?;
        let parent_id =
            update_optional_string_value(batch, &assignment_values, row_index, "parent_id")?;
        let name = update_required_string_value(batch, &assignment_values, row_index, "name")?;
        if let Some(directory_id) = id.as_ref() {
            let resolver = path_resolvers
                .entry(directory_path_resolver_key(&context))
                .or_insert_with(DirectoryPathResolver::default);
            resolver
                .reserve_directory(parent_id.clone(), name.clone(), directory_id.clone())
                .map_err(lix_error_to_datafusion_error)?;
        }
        rows.push(directory_descriptor_write_row(
            DirectoryDescriptorWriteIntent {
                id,
                parent_id,
                name,
                hidden: update_optional_bool_value(batch, &assignment_values, row_index, "hidden")?,
                context,
            },
        ));
    }
    Ok(rows)
}

fn directory_branch_ids_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
) -> Result<BTreeSet<String>> {
    let mut branch_ids = BTreeSet::new();
    for row_index in 0..batch.num_rows() {
        branch_ids
            .insert(directory_row_context_from_batch(batch, row_index, branch_binding)?.branch_id);
    }
    Ok(branch_ids)
}

fn lix_directory_recursive_delete_rows_from_batch(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    visible_filesystems: &BTreeMap<String, VisibleFilesystem>,
) -> Result<(Vec<TransactionWriteRow>, u64)> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    let mut count = 0u64;
    for row_index in 0..batch.num_rows() {
        let directory_id = required_string_value(batch, row_index, "id")?;
        let context = directory_row_context_from_batch(batch, row_index, branch_binding)?;
        let visible_filesystem = visible_filesystems.get(&context.branch_id).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "DELETE FROM lix_directory missing visible filesystem for branch '{}'",
                context.branch_id
            ))
        })?;
        append_deduped_delete_plan(
            &mut rows,
            &mut seen,
            plan_recursive_directory_delete(&directory_id, visible_filesystem, context),
            &mut count,
        );
    }
    Ok((rows, count))
}

fn append_deduped_delete_plan(
    rows: &mut Vec<TransactionWriteRow>,
    seen: &mut BTreeSet<StateRowDedupeKey>,
    plan: FilesystemDeletePlan,
    count: &mut u64,
) {
    for row in plan.rows {
        if seen.insert(StateRowDedupeKey::from(&row)) {
            if is_user_visible_filesystem_delete_row(&row) {
                *count += 1;
            }
            rows.push(row);
        }
    }
}

fn is_user_visible_filesystem_delete_row(row: &TransactionWriteRow) -> bool {
    matches!(
        row.schema_key.as_str(),
        "lix_directory_descriptor" | "lix_file_descriptor"
    )
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StateRowDedupeKey {
    entity_pk: String,
    schema_key: String,
    file_id: Option<String>,
    branch_id: String,
    global: bool,
    untracked: bool,
}

impl From<&TransactionWriteRow> for StateRowDedupeKey {
    fn from(row: &TransactionWriteRow) -> Self {
        Self {
            entity_pk: row
                .entity_pk
                .as_ref()
                .expect("directory provider staged row should carry entity_pk")
                .as_single_string_owned()
                .expect("directory provider staged row entity primary key should project"),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            branch_id: row.branch_id.clone(),
            global: row.global,
            untracked: row.untracked,
        }
    }
}

#[cfg(test)]
fn lix_directory_write_rows_from_batch_with_options(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
) -> Result<Vec<TransactionWriteRow>> {
    lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
        batch,
        branch_binding,
        surface_name,
        reject_read_only_fields,
        None,
        None,
    )
}

fn lix_directory_write_rows_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    branch_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    mut path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    mut generate_directory_id: Option<&mut dyn FnMut() -> String>,
) -> Result<Vec<TransactionWriteRow>> {
    let mut rows = Vec::new();
    for row_index in 0..batch.num_rows() {
        if reject_read_only_fields {
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_entity_pk")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_schema_key")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_change_id")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_created_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_updated_at")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "lixcol_commit_id")?;
        }

        let path = optional_string_value(batch, row_index, "path")?;
        let id = optional_string_value(batch, row_index, "id")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?;
        let context = directory_row_context_from_batch(batch, row_index, branch_binding)?;

        if let Some(path) = path.filter(|_| reject_read_only_fields) {
            reject_read_only_lix_directory_insert_field(batch, row_index, "parent_id")?;
            reject_read_only_lix_directory_insert_field(batch, row_index, "name")?;

            let Some(path_resolvers) = path_resolvers.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_directory with path requires directory path resolver"
                        .to_string(),
                ));
            };
            let resolver = path_resolvers
                .entry(directory_path_resolver_key(&context))
                .or_insert_with(DirectoryPathResolver::default);
            let Some(generate_directory_id) = generate_directory_id.as_deref_mut() else {
                return Err(DataFusionError::Execution(
                    "INSERT into lix_directory with path requires directory id generator"
                        .to_string(),
                ));
            };
            let directory_id = id.unwrap_or_else(|| generate_directory_id());
            let mut planned_rows = resolver
                .create_directory_path_with_leaf_id(
                    &path,
                    Some(directory_id.clone()),
                    context,
                    hidden.unwrap_or(false),
                    generate_directory_id,
                )
                .map_err(lix_error_to_datafusion_error)?;
            attach_lix_directory_insert_origin(&mut planned_rows, surface_name, &directory_id);
            rows.extend(planned_rows);
            continue;
        }

        let parent_id = optional_string_value(batch, row_index, "parent_id")?;
        let name = required_string_value(batch, row_index, "name")?;
        if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
            if let Some(directory_id) = id.as_ref() {
                let resolver = path_resolvers
                    .entry(directory_path_resolver_key(&context))
                    .or_insert_with(DirectoryPathResolver::default);
                resolver
                    .reserve_directory(parent_id.clone(), name.clone(), directory_id.clone())
                    .map_err(lix_error_to_datafusion_error)?;
            }
        }
        let mut row = directory_descriptor_write_row(DirectoryDescriptorWriteIntent {
            id: id.clone(),
            parent_id,
            name,
            hidden,
            context,
        });
        if let Some(directory_id) = id.as_ref() {
            row.origin = Some(lix_directory_insert_origin(surface_name, directory_id));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn attach_lix_directory_insert_origin(
    rows: &mut [TransactionWriteRow],
    surface_name: &str,
    directory_id: &str,
) {
    let origin = lix_directory_insert_origin(surface_name, directory_id);
    for row in rows {
        if row.schema_key != DIRECTORY_SCHEMA_KEY {
            continue;
        }
        let Some(entity_pk) = row
            .entity_pk
            .as_ref()
            .and_then(|entity_pk| entity_pk.as_single_string_owned().ok())
        else {
            continue;
        };
        if entity_pk == directory_id {
            row.origin = Some(origin.clone());
        }
    }
}

fn lix_directory_insert_origin(surface_name: &str, directory_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: surface_name.to_string(),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![directory_id.to_string()],
        }),
    }
}

fn directory_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let scope = resolve_write_branch_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        optional_string_value(batch, row_index, "lixcol_branch_id")?,
        branch_binding,
        "INSERT into lix_directory_by_branch",
        "lix_directory",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: optional_metadata_value(batch, row_index, "lixcol_metadata", "lix_directory")?,
    })
}

fn directory_row_context_from_update(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    branch_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let scope = resolve_write_branch_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        optional_string_value(batch, row_index, "lixcol_branch_id")?,
        branch_binding,
        "UPDATE into lix_directory_by_branch",
        "lix_directory",
    )?;

    Ok(FilesystemRowContext {
        branch_id: scope.branch_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: update_optional_metadata_value(
            batch,
            assignment_values,
            row_index,
            "lixcol_metadata",
            "lix_directory",
        )?,
    })
}

fn directory_path_resolver_key(context: &FilesystemRowContext) -> String {
    filesystem_storage_scope_key(
        &context.branch_id,
        context.global,
        context.untracked,
        context.file_id.as_deref(),
    )
}

async fn directory_path_resolvers_from_live_state(
    live_state: Arc<dyn LiveStateReader>,
    branch_binding: Option<&str>,
) -> std::result::Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    DIRECTORY_SCHEMA_KEY.to_string(),
                    FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                ],
                branch_ids: branch_binding
                    .map(|branch_id| vec![branch_id.to_string()])
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
    if let Some(branch_id) = branch_binding {
        let key = filesystem_storage_scope_key(branch_id, false, false, None);
        resolvers
            .entry(key)
            .or_insert_with(DirectoryPathResolver::default);
    }
    Ok(resolvers)
}

fn lix_directory_record_batch(
    schema: &SchemaRef,
    rows: Vec<MaterializedLiveStateRow>,
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

    let directory_paths = derive_directory_paths(&directory_rows)?;
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut parent_ids = Vec::new();
    let mut names = Vec::new();
    let mut hiddens = Vec::new();
    let mut entity_pks = Vec::new();
    let mut schema_keys = Vec::new();
    let mut file_ids = Vec::new();
    let mut globals = Vec::new();
    let mut change_ids = Vec::new();
    let mut created_ats = Vec::new();
    let mut updated_ats = Vec::new();
    let mut commit_ids = Vec::new();
    let mut untracked_values = Vec::new();
    let mut metadata_values = Vec::new();
    let mut branch_ids = Vec::new();

    for directory in directory_rows {
        ids.push(Some(directory.id.clone()));
        paths.push(
            directory_paths
                .get(&(directory.live.branch_id.clone(), directory.id.clone()))
                .cloned(),
        );
        parent_ids.push(directory.parent_id);
        names.push(Some(directory.name));
        hiddens.push(Some(directory.hidden));
        entity_pks.push(Some(directory.live.entity_pk.as_json_array_text()?));
        schema_keys.push(Some(directory.live.schema_key));
        file_ids.push(directory.live.file_id);
        globals.push(Some(directory.live.global));
        change_ids.push(directory.live.change_id.map(|id| id.to_string()));
        created_ats.push(directory.live.created_at);
        updated_ats.push(directory.live.updated_at);
        commit_ids.push(directory.live.commit_id.map(|id| id.to_string()));
        untracked_values.push(Some(directory.live.untracked));
        metadata_values.push(directory.live.metadata.as_ref().map(serialize_row_metadata));
        branch_ids.push(Some(directory.live.branch_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "parent_id" => Arc::new(StringArray::from(parent_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
            "hidden" => Arc::new(BooleanArray::from(hiddens.clone())),
            "lixcol_entity_pk" => Arc::new(StringArray::from(entity_pks.clone())),
            "lixcol_schema_key" => Arc::new(StringArray::from(schema_keys.clone())),
            "lixcol_file_id" => Arc::new(StringArray::from(file_ids.clone())),
            "lixcol_global" => Arc::new(BooleanArray::from(globals.clone())),
            "lixcol_change_id" => Arc::new(StringArray::from(change_ids.clone())),
            "lixcol_created_at" => Arc::new(StringArray::from(created_ats.clone())),
            "lixcol_updated_at" => Arc::new(StringArray::from(updated_ats.clone())),
            "lixcol_commit_id" => Arc::new(StringArray::from(commit_ids.clone())),
            "lixcol_untracked" => Arc::new(BooleanArray::from(untracked_values.clone())),
            "lixcol_metadata" => Arc::new(StringArray::from(metadata_values.clone())),
            "lixcol_branch_id" => Arc::new(StringArray::from(branch_ids.clone())),
            other => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "sql2 lix_directory provider does not support projected column '{other}'"
                    ),
                ));
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
) -> std::result::Result<BTreeMap<(String, String), String>, LixError> {
    let mut by_branch = BTreeMap::<String, BTreeMap<String, &DirectoryDescriptorRecord>>::new();
    for row in rows {
        by_branch
            .entry(row.live.branch_id.clone())
            .or_default()
            .insert(row.id.clone(), row);
    }

    let mut paths = BTreeMap::<(String, String), String>::new();
    for (branch_id, records) in by_branch {
        for directory_id in records.keys() {
            derive_directory_path_for(
                &branch_id,
                directory_id,
                &records,
                &mut paths,
                &mut BTreeSet::new(),
            )?;
        }
    }
    Ok(paths)
}

fn derive_directory_path_for(
    branch_id: &str,
    directory_id: &str,
    records: &BTreeMap<String, &DirectoryDescriptorRecord>,
    paths: &mut BTreeMap<(String, String), String>,
    visiting: &mut BTreeSet<String>,
) -> std::result::Result<Option<String>, LixError> {
    if let Some(path) = paths.get(&(branch_id.to_string(), directory_id.to_string())) {
        return Ok(Some(path.clone()));
    }
    if !visiting.insert(directory_id.to_string()) {
        return Err(directory_parent_cycle_error(branch_id, directory_id));
    }
    let Some(row) = records.get(directory_id) else {
        visiting.remove(directory_id);
        return Ok(None);
    };
    let path = match row.parent_id.as_deref() {
        Some(parent_id) => {
            let Some(parent_path) =
                derive_directory_path_for(branch_id, parent_id, records, paths, visiting)?
            else {
                visiting.remove(directory_id);
                return Ok(None);
            };
            format!("{parent_path}{}/", row.name)
        }
        None => format!("/{}/", row.name),
    };
    visiting.remove(directory_id);
    paths.insert(
        (branch_id.to_string(), directory_id.to_string()),
        path.clone(),
    );
    Ok(Some(path))
}

fn directory_parent_cycle_error(branch_id: &str, directory_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id cycle in branch '{branch_id}' while resolving directory '{directory_id}'"
        ),
    )
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
    branch_binding: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![DIRECTORY_SCHEMA_KEY.to_string()],
            branch_ids: branch_binding
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: lix_directory_live_state_projection(projected_schema),
        limit,
    }
}

fn lix_directory_live_state_projection(projected_schema: Option<&Schema>) -> LiveStateProjection {
    let Some(schema) = projected_schema else {
        return LiveStateProjection::default();
    };
    let mut columns = Vec::new();
    let needs_snapshot = schema
        .fields()
        .iter()
        .any(|field| matches!(field.name().as_str(), "parent_id" | "name" | "hidden"));
    if needs_snapshot {
        columns.push("snapshot_content".to_string());
    }
    if schema
        .fields()
        .iter()
        .any(|field| field.name() == "lixcol_metadata")
    {
        columns.push("metadata".to_string());
    }
    LiveStateProjection { columns }
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

fn update_required_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?.ok_or_else(
        || {
            DataFusionError::Execution(format!(
                "UPDATE lix_directory requires non-null text column '{column_name}'"
            ))
        },
    )
}

fn update_optional_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => Ok(None),
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(Some(value)),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_directory expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn update_optional_metadata_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        })
        .transpose()
}

fn update_optional_bool_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<bool>> {
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => Ok(None),
        InsertCell::Provided(SqlCell::Value(ScalarValue::Boolean(Some(value)))) => Ok(Some(value)),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE lix_directory expected boolean column '{column_name}', got {other:?}"
        ))),
    }
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

fn optional_metadata_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    optional_string_value(batch, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(crate::sql2::error::lix_error_to_datafusion_error)
        })
        .transpose()
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

pub(super) fn lix_directory_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, true),
        json_field("lixcol_entity_pk", false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
        json_field("lixcol_metadata", true),
    ]))
}

pub(super) fn lix_directory_by_branch_schema() -> SchemaRef {
    let mut fields = lix_directory_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    crate::sql2::error::lix_error_to_datafusion_error(error)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use serde_json::json;

    use crate::binary_cas::BlobDataReader;
    use crate::changelog::{ChangeId, CommitId};
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::live_state::{LiveStateScanRequest, MaterializedLiveStateRow};
    use crate::sql2::dml::InsertSink;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
        TransactionWriteRow,
    };
    use crate::LixError;

    use super::{
        derive_directory_path_for, directory_path_resolvers_from_state_rows,
        lix_directory_by_branch_schema, lix_directory_insert_origin, lix_directory_record_batch,
        lix_directory_recursive_delete_rows_from_batch, lix_directory_write_rows_from_batch,
        lix_directory_write_rows_from_batch_with_path_resolvers, BranchBinding,
        DirectoryDescriptorRecord, LixDirectoryInsertSink,
    };
    use crate::sql2::filesystem_visibility::VisibleFilesystem;

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
    struct CapturingWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_branch_id(&self) -> &str {
            "branch-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            BlobDataReader::load_bytes_many(self, hashes).await
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<crate::changelog::CommitId>, LixError> {
            if branch_id == "ghost-branch" {
                return Ok(None);
            }
            Ok(Some(crate::changelog::CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    fn live_row(
        entity_pk: &str,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        live_filesystem_row(
            entity_pk,
            super::DIRECTORY_SCHEMA_KEY,
            None,
            branch_id,
            snapshot_content,
        )
    }

    fn live_filesystem_row(
        entity_pk: &str,
        schema_key: &str,
        file_id: Option<&str>,
        branch_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: schema_key.to_string(),
            file_id: file_id.map(ToOwned::to_owned),
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: Some(json!({"source": "test"}).to_string()),
            deleted: false,
            branch_id: branch_id.to_string(),
            change_id: Some(ChangeId::for_test_label(&format!("change-{entity_pk}"))),
            commit_id: Some(CommitId::for_test_label(&format!("commit-{entity_pk}"))),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn filesystem_rows() -> Vec<MaterializedLiveStateRow> {
        vec![
            live_filesystem_row(
                "dir-docs",
                "lix_directory_descriptor",
                None,
                "branch-a",
                r#"{"id":"dir-docs","parent_id":null,"name":"docs","hidden":false}"#,
            ),
            live_filesystem_row(
                "dir-guides",
                "lix_directory_descriptor",
                None,
                "branch-a",
                r#"{"id":"dir-guides","parent_id":"dir-docs","name":"guides","hidden":false}"#,
            ),
            live_filesystem_row(
                "file-index",
                "lix_file_descriptor",
                None,
                "branch-a",
                r#"{"id":"file-index","directory_id":"dir-docs","name":"index.md","hidden":false}"#,
            ),
            live_filesystem_row(
                "file-readme",
                "lix_file_descriptor",
                None,
                "branch-a",
                r#"{"id":"file-readme","directory_id":"dir-guides","name":"readme.md","hidden":false}"#,
            ),
            live_filesystem_row(
                "file-readme",
                "lix_binary_blob_ref",
                Some("file-readme"),
                "branch-a",
                r#"{"id":"file-readme","blob_hash":"abc123","size_bytes":5}"#,
            ),
        ]
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn directory_insert_batch(include_branch: bool, global: bool) -> RecordBatch {
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
        if include_branch {
            fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("branch-a")]));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("directory insert batch should build")
    }

    fn directory_path_insert_batch(path: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, true),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("dir-nested")]),
                string_column(vec![Some(path)]),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                string_column(vec![Some("branch-a")]),
            ],
        )
        .expect("directory path insert batch should build")
    }

    fn directory_delete_batch(ids: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("lixcol_branch_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(ids.iter().copied().map(Some).collect::<Vec<_>>()),
                string_column(vec![Some("branch-a"); ids.len()]),
            ],
        )
        .expect("directory delete batch should build")
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
                "branch-a",
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
                "branch-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\",\"hidden\":false}",
            ),
        };
        let mut records = BTreeMap::new();
        records.insert(root.id.clone(), &root);
        records.insert(child.id.clone(), &child);
        let mut paths = BTreeMap::new();

        assert_eq!(
            derive_directory_path_for(
                "branch-a",
                "dir-guides",
                &records,
                &mut paths,
                &mut BTreeSet::new()
            )
            .expect("path derivation should succeed"),
            Some("/docs/guides/".to_string())
        );
    }

    #[test]
    fn record_batch_projects_directory_columns() {
        let rows = vec![
            live_row(
                "dir-docs",
                "branch-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}",
            ),
            live_row(
                "dir-guides",
                "branch-a",
                "{\"id\":\"dir-guides\",\"parent_id\":\"dir-docs\",\"name\":\"guides\",\"hidden\":true}",
            ),
        ];

        let batch = lix_directory_record_batch(&lix_directory_by_branch_schema(), rows)
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
                .column_by_name("lixcol_branch_id")
                .expect("branch column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch is string")
                .value(1),
            "branch-a"
        );
    }

    #[test]
    fn decodes_directory_insert_into_lix_state_write_row() {
        let rows = lix_directory_write_rows_from_batch(&directory_insert_batch(true, false), None)
            .expect("directory batch should decode");

        assert_eq!(
            rows,
            vec![TransactionWriteRow {
                entity_pk: Some(crate::entity_pk::EntityPk::single("dir-docs")),
                schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                file_id: None,
                snapshot: Some(TransactionJson::from_value_for_test(
                    json!({"hidden":false,"id":"dir-docs","name":"docs","parent_id":null})
                )),
                metadata: Some(TransactionJson::from_value_for_test(
                    json!({"source": "directory"})
                )),
                origin: Some(lix_directory_insert_origin("lix_directory", "dir-docs")),
                created_at: None,
                updated_at: None,
                global: false,
                change_id: None,
                commit_id: None,
                untracked: false,
                branch_id: "branch-a".to_string(),
            }]
        );
    }

    #[test]
    fn active_directory_insert_defaults_branch_id() {
        let rows = lix_directory_write_rows_from_batch(
            &directory_insert_batch(false, false),
            Some("branch-active"),
        )
        .expect("active directory batch should decode");

        assert_eq!(rows[0].branch_id, "branch-active");
    }

    #[test]
    fn by_branch_directory_insert_requires_branch_id_for_non_global_rows() {
        let error =
            lix_directory_write_rows_from_batch(&directory_insert_batch(false, false), None)
                .expect_err("by-branch insert should require branch id");

        assert!(
            error.to_string().contains("requires lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn directory_insert_rejects_global_with_non_global_branch_id() {
        let error = lix_directory_write_rows_from_batch(&directory_insert_batch(true, true), None)
            .expect_err("global directory write should reject conflicting branch id");

        assert!(
            error
                .to_string()
                .contains("cannot set lixcol_global=true with non-global lixcol_branch_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn directory_path_insert_reuses_existing_parent_descriptor() {
        let existing_rows = vec![live_row(
            "dir-docs",
            "branch-a",
            "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}",
        )];
        let mut resolvers = directory_path_resolvers_from_state_rows(existing_rows)
            .expect("existing directory rows should seed paths");

        let rows = lix_directory_write_rows_from_batch_with_path_resolvers(
            &directory_path_insert_batch("/docs/nested/"),
            None,
            "lix_directory",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
        )
        .expect("directory path batch should decode");

        assert_eq!(rows.len(), 1);
        let snapshot = rows[0].snapshot.as_ref().unwrap();
        assert_eq!(snapshot["id"], "dir-nested");
        assert_eq!(snapshot["parent_id"], "dir-docs");
        assert_eq!(snapshot["name"], "nested");
    }

    #[test]
    fn recursive_directory_delete_deletes_nested_dirs_files_and_blob_refs() {
        let visible_filesystem = VisibleFilesystem::from_live_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert("branch-a".to_string(), visible_filesystem);

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&["dir-docs"]),
            None,
            &visible_filesystems,
        )
        .expect("recursive directory delete should plan");

        assert_eq!(count, 4);
        assert_eq!(
            rows.iter()
                .map(|row| {
                    (
                        row.schema_key.as_str(),
                        row.entity_pk
                            .as_ref()
                            .expect("planned delete row should carry entity_pk")
                            .as_single_string_owned()
                            .expect("planned delete row should project entity_pk"),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("lix_file_descriptor", "file-readme".to_string()),
                ("lix_binary_blob_ref", "file-readme".to_string()),
                ("lix_directory_descriptor", "dir-guides".to_string()),
                ("lix_file_descriptor", "file-index".to_string()),
                ("lix_directory_descriptor", "dir-docs".to_string()),
            ]
        );
        assert!(rows.iter().all(|row| row.snapshot.is_none()));
    }

    #[test]
    fn recursive_directory_delete_dedupes_overlapping_parent_and_child() {
        let visible_filesystem = VisibleFilesystem::from_live_rows(filesystem_rows())
            .expect("visible filesystem should build");
        let mut visible_filesystems = BTreeMap::new();
        visible_filesystems.insert("branch-a".to_string(), visible_filesystem);

        let (rows, count) = lix_directory_recursive_delete_rows_from_batch(
            &directory_delete_batch(&["dir-docs", "dir-guides"]),
            None,
            &visible_filesystems,
        )
        .expect("recursive directory delete should plan");

        assert_eq!(count, 4);
        let identities = rows
            .iter()
            .map(|row| {
                (
                    row.schema_key.clone(),
                    row.entity_pk.clone(),
                    row.file_id.clone(),
                    row.branch_id.clone(),
                )
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(identities.len(), rows.len());
        assert_eq!(rows.len(), 5);
    }

    #[tokio::test]
    async fn directory_insert_sink_stages_decoded_lix_state_rows() {
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = directory_insert_batch(true, false);
        let sink =
            LixDirectoryInsertSink::new(write_ctx, test_functions(), BranchBinding::explicit());
        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("directory sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: vec![TransactionWriteRow {
                    entity_pk: Some(crate::entity_pk::EntityPk::single("dir-docs")),
                    schema_key: super::DIRECTORY_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"hidden":false,"id":"dir-docs","name":"docs","parent_id":null})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"source": "directory"})
                    )),
                    origin: Some(lix_directory_insert_origin(
                        "lix_directory_by_branch",
                        "dir-docs"
                    )),
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    branch_id: "branch-a".to_string(),
                }]
            }]
        );
    }

    #[tokio::test]
    async fn directory_insert_sink_seeds_path_resolver_from_live_state() {
        let mut write_context = CapturingWriteContext {
            rows: vec![live_row(
                "dir-docs",
                "branch-a",
                "{\"id\":\"dir-docs\",\"parent_id\":null,\"name\":\"docs\",\"hidden\":false}",
            )],
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = directory_path_insert_batch("/docs/nested/");
        let sink =
            LixDirectoryInsertSink::new(write_ctx, test_functions(), BranchBinding::explicit());
        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("directory sink should stage path write");

        assert_eq!(count, 1);
        let [TransactionWrite::Rows { rows, .. }] = write_context.writes.as_slice() else {
            panic!("expected one directory staged write");
        };
        assert_eq!(rows.len(), 1);
        let snapshot = rows[0].snapshot.as_ref().unwrap();
        assert_eq!(snapshot["id"], "dir-nested");
        assert_eq!(snapshot["parent_id"], "dir-docs");
        assert_eq!(snapshot["name"], "nested");
    }
}
