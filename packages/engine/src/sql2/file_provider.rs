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
use futures_util::{stream, TryStreamExt};
use serde::Deserialize;

use crate::binary_cas::{BlobDataReader, BlobHash};
use crate::entity_identity::EntityIdentity;
use crate::functions::FunctionProviderHandle;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::filesystem_predicates::{
    canonicalize_filesystem_path_filters, FilesystemPathKind,
};
use crate::sql2::predicate_typecheck::validate_json_predicate_filters;
use crate::sql2::version_scope::{
    explicit_version_ids_from_dml_filters, resolve_provider_version_ids,
    resolve_write_version_scope, VersionBinding,
};
use crate::sql2::write_normalization::{
    is_binary_type, lix_file_data_type_error, lix_file_data_type_error_with_value,
    logical_expr_is_binary_or_null, reject_non_binary_casts_for_insert_column,
    scalar_is_binary_or_null, InsertCell, InsertColumnIntents, SqlCell, UpdateAssignmentValues,
    UpdateCell,
};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::version::VersionRefReader;
use crate::{parse_row_metadata_value, serialize_row_metadata, LixError};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

use super::filesystem_planner::{
    blob_ref_row, directory_path_resolvers_from_state_rows, file_descriptor_row,
    file_descriptor_write_row, filesystem_storage_scope_key, plan_file_delete,
    plan_file_path_update, BlobRefRowInput, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FileDescriptorWriteIntent, FilePathWriteInput, FilesystemDeletePlan,
    FilesystemRowContext,
};
use super::result_metadata::json_field;
use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextLiveStateReader, WriteContextVersionRefReader,
};
use crate::transaction::types::{
    LogicalPrimaryKey, TransactionFileData, TransactionWrite, TransactionWriteMode,
    TransactionWriteOperation, TransactionWriteOrigin,
};

pub(crate) async fn register_lix_file_providers(
    session: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    functions: FunctionProviderHandle,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_by_version",
            Arc::new(LixFileProvider::by_version(
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
                Arc::clone(&blob_reader),
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
                functions,
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) async fn register_lix_file_write_providers(
    session: &SessionContext,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    session
        .register_table(
            "lix_file_by_version",
            Arc::new(LixFileProvider::by_version_with_write(write_ctx.clone())),
        )
        .map_err(datafusion_error_to_lix_error)?;
    session
        .register_table(
            "lix_file",
            Arc::new(LixFileProvider::active_version_with_write(write_ctx)),
        )
        .map_err(datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) struct LixFileProvider {
    schema: SchemaRef,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    write_access: WriteAccess,
    functions: FunctionProviderHandle,
    version_binding: VersionBinding,
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
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_access: WriteAccess::read_only(),
            functions,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    pub(crate) fn active_version_with_write(write_ctx: SqlWriteContext) -> Self {
        let active_version_id = write_ctx.active_version_id();
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        let blob_reader = write_ctx.blob_reader();
        Self {
            schema: lix_file_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_access: WriteAccess::write(write_ctx),
            functions,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    pub(crate) fn by_version(
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
        blob_reader: Arc<dyn BlobDataReader>,
        functions: FunctionProviderHandle,
    ) -> Self {
        Self {
            schema: lix_file_by_version_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_access: WriteAccess::read_only(),
            functions,
            version_binding: VersionBinding::explicit(),
        }
    }

    pub(crate) fn by_version_with_write(write_ctx: SqlWriteContext) -> Self {
        let functions = write_ctx.functions();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        let blob_reader = write_ctx.blob_reader();
        Self {
            schema: lix_file_by_version_schema(),
            live_state,
            version_ref,
            blob_reader,
            write_access: WriteAccess::write(write_ctx),
            functions,
            version_binding: VersionBinding::explicit(),
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
        let analyzer = LixFileIdFilterAnalyzer;
        Ok(filters
            .iter()
            .map(|filter| {
                if ExactStringColumnFilterAnalyzer::new("lixcol_version_id").supports(filter)
                    || analyzer.supports(filter)
                    || contains_column(filter, "path")
                {
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
        let scan_limit = if filters.is_empty() { limit } else { None };
        let mut request =
            lix_file_scan_request(self.version_binding.active_version_id(), scan_limit);
        if self.write_access.is_write() && matches!(self.version_binding, VersionBinding::Explicit)
        {
            request.filter.version_ids = explicit_version_ids_from_dml_filters(filters);
            if request.filter.version_ids.is_empty() {
                return Err(DataFusionError::Plan(
                    "DELETE FROM lix_file_by_version requires an explicit lixcol_version_id predicate"
                        .to_string(),
                ));
            }
        }
        request.filter.version_ids = resolve_provider_version_ids(
            self.version_ref.as_ref(),
            &self.version_binding,
            request.filter.version_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        let filters = canonicalize_filesystem_path_filters(filters, FilesystemPathKind::File)?;
        let target_file_ids = file_id_constraint_from_filters(&filters)?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, _state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(LixFileScanExec::new(
            Arc::clone(&self.live_state),
            Arc::clone(&self.blob_reader),
            Arc::clone(&self.schema),
            projected_schema,
            projection.cloned(),
            request,
            target_file_ids,
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
            return not_impl_err!("{insert_op} not implemented for lix_file yet");
        }

        let write_ctx = self.write_access.require_write("INSERT into lix_file")?;
        let insert_column_intents = InsertColumnIntents::from_input(&input);
        let include_data_writes = insert_column_intents.includes_column("data");
        if include_data_writes {
            reject_non_binary_casts_for_insert_column(&input, "data", "INSERT into lix_file")?;
        }

        let sink = LixFileInsertSink::new(
            input.schema(),
            write_ctx.clone(),
            self.functions.clone(),
            self.version_binding.clone(),
            include_data_writes,
        );
        Ok(Arc::new(InsertExec::new(input, Arc::new(sink))))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self.write_access.require_write("DELETE FROM lix_file")?;

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let filters = canonicalize_filesystem_path_filters(&filters, FilesystemPathKind::File)?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let target_file_ids = file_id_constraint_from_filters(&filters)?;
        let mut request = lix_file_scan_request(self.version_binding.active_version_id(), None);
        if matches!(self.version_binding, VersionBinding::Explicit) {
            request.filter.version_ids = explicit_version_ids_from_dml_filters(&filters);
            if request.filter.version_ids.is_empty() {
                return Err(DataFusionError::Plan(
                    "DELETE FROM lix_file_by_version requires an explicit lixcol_version_id predicate"
                        .to_string(),
                ));
            }
        }

        Ok(Arc::new(LixFileDeleteExec::new(
            Arc::clone(&self.blob_reader),
            write_ctx.clone(),
            Arc::clone(&self.schema),
            self.version_binding.clone(),
            request,
            target_file_ids,
            physical_filters,
        )))
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self.write_access.require_write("UPDATE lix_file")?;

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
        let filters = canonicalize_filesystem_path_filters(&filters, FilesystemPathKind::File)?;
        let target_file_ids = file_id_constraint_from_filters(&filters)?;
        validate_json_predicate_filters(self.schema.as_ref(), &filters)?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let request = lix_file_scan_request(self.version_binding.active_version_id(), None);

        Ok(Arc::new(LixFileUpdateExec::new(
            Arc::clone(&self.blob_reader),
            write_ctx.clone(),
            Arc::clone(&self.schema),
            self.version_binding.clone(),
            self.functions.clone(),
            request,
            target_file_ids,
            physical_assignments,
            physical_filters,
        )))
    }
}

#[allow(dead_code)]
struct LixFileInsertSink {
    write_ctx: SqlWriteContext,
    functions: FunctionProviderHandle,
    version_binding: VersionBinding,
    surface_name: &'static str,
    include_data_writes: bool,
}

impl std::fmt::Debug for LixFileInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixFileInsertSink").finish()
    }
}

impl LixFileInsertSink {
    fn new(
        _schema: SchemaRef,
        write_ctx: SqlWriteContext,
        functions: FunctionProviderHandle,
        version_binding: VersionBinding,
        include_data_writes: bool,
    ) -> Self {
        let surface_name = lix_file_surface_name(&version_binding);
        Self {
            write_ctx,
            functions,
            version_binding,
            surface_name,
            include_data_writes,
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
impl InsertSink for LixFileInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut staged = LixFileStagedBatch::default();
        let mut path_resolvers = None;
        for batch in batches {
            if path_resolvers.is_none() {
                path_resolvers = Some(
                    file_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(self.write_ctx.clone())),
                        self.version_binding.active_version_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            if record_batch_has_non_null_column(&batch, "path")? {
                staged.extend(lix_file_insert_stage_from_batch_with_path_resolvers(
                    &batch,
                    self.version_binding.active_version_id(),
                    self.surface_name,
                    path_resolvers
                        .as_mut()
                        .expect("path resolver should be initialized"),
                    &mut || self.functions.call_uuid_v7(),
                    self.include_data_writes,
                )?);
            } else {
                staged.extend(
                    lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
                        &batch,
                        self.version_binding.active_version_id(),
                        self.surface_name,
                        path_resolvers
                            .as_mut()
                            .expect("path resolver should be initialized"),
                        &mut || self.functions.call_uuid_v7(),
                        self.include_data_writes,
                    )?,
                );
            }
        }

        if !staged.state_rows.is_empty() || !staged.file_data_writes.is_empty() {
            let intent = if staged.file_data_writes.is_empty() {
                TransactionWrite::Rows {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                }
            } else {
                TransactionWrite::RowsWithFileData {
                    mode: TransactionWriteMode::Insert,
                    rows: staged.state_rows,
                    file_data: staged.file_data_writes,
                    count: staged.count,
                }
            };
            self.write_ctx
                .stage_write(intent)
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }

        Ok(staged.count)
    }
}

fn lix_file_surface_name(version_binding: &VersionBinding) -> &'static str {
    match version_binding {
        VersionBinding::Active { .. } => "lix_file",
        VersionBinding::Explicit => "lix_file_by_version",
    }
}

#[allow(dead_code)]
struct LixFileDeleteExec {
    blob_reader: Arc<dyn BlobDataReader>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    request: LiveStateScanRequest,
    target_file_ids: FileIdConstraint,
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
        blob_reader: Arc<dyn BlobDataReader>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
        request: LiveStateScanRequest,
        target_file_ids: FileIdConstraint,
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
            blob_reader,
            write_ctx,
            table_schema,
            version_binding,
            request,
            target_file_ids,
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

        let blob_reader = Arc::clone(&self.blob_reader);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let request = self.request.clone();
        let target_file_ids = self.target_file_ids.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = scan_lix_file_live_rows(
                Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                &request,
                &target_file_ids,
            )
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
                version_binding.active_version_id(),
                &blob_ref_file_ids,
            )?;
            let count = staged.count;

            if count > 0 {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
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

#[allow(dead_code)]
struct LixFileUpdateExec {
    blob_reader: Arc<dyn BlobDataReader>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    functions: FunctionProviderHandle,
    request: LiveStateScanRequest,
    target_file_ids: FileIdConstraint,
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
        blob_reader: Arc<dyn BlobDataReader>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
        functions: FunctionProviderHandle,
        request: LiveStateScanRequest,
        target_file_ids: FileIdConstraint,
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
            blob_reader,
            write_ctx,
            table_schema,
            version_binding,
            functions,
            request,
            target_file_ids,
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

        let blob_reader = Arc::clone(&self.blob_reader);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let functions = self.functions.clone();
        let request = self.request.clone();
        let target_file_ids = self.target_file_ids.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = scan_lix_file_live_rows(
                Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                &request,
                &target_file_ids,
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
            let source_batch = lix_file_record_batch(&table_schema, &blob_reader, rows)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            let matched_batch = filter_lix_file_batch(source_batch, &filters)?;
            let assignment_values = UpdateAssignmentValues::evaluate(&matched_batch, &assignments)?;
            let update_columns = LixFileUpdateColumns::from_assignments(&assignments);
            let mut path_resolvers = None;
            if update_columns.path || update_columns.descriptor {
                path_resolvers = Some(
                    file_path_resolvers_from_live_state(
                        Arc::new(WriteContextLiveStateReader::new(write_ctx.clone())),
                        version_binding.active_version_id(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
                );
            }
            let staged = lix_file_update_stage_from_batch(
                &matched_batch,
                &assignment_values,
                version_binding.active_version_id(),
                update_columns,
                path_resolvers.as_mut(),
                &mut || functions.call_uuid_v7(),
            )?;
            let count = staged.count;

            if count > 0 {
                let intent = if staged.file_data_writes.is_empty() {
                    TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: staged.state_rows,
                    }
                } else {
                    TransactionWrite::RowsWithFileData {
                        mode: TransactionWriteMode::Replace,
                        rows: staged.state_rows,
                        file_data: staged.file_data_writes,
                        count,
                    }
                };
                write_ctx
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
    batch_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    request: LiveStateScanRequest,
    target_file_ids: FileIdConstraint,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
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
        batch_schema: SchemaRef,
        output_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        request: LiveStateScanRequest,
        target_file_ids: FileIdConstraint,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            live_state,
            blob_reader,
            batch_schema,
            output_schema,
            projection,
            request,
            target_file_ids,
            filters,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixFileScanExec(limit={:?})", self.limit)
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
        let target_file_ids = self.target_file_ids.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let output_schema = Arc::clone(&self.output_schema);
        let batch_schema = Arc::clone(&self.batch_schema);
        let projection = self.projection.clone();
        let fut = async move {
            let rows = scan_lix_file_live_rows(live_state, &request, &target_file_ids)
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!("sql2 lix_file scan failed: {error}"))
                })?;
            let batch = lix_file_record_batch(&batch_schema, &blob_reader, rows)
                .await
                .map_err(|error| {
                    DataFusionError::Execution(format!("sql2 lix_file batch build failed: {error}"))
                })?;
            let filtered = filter_lix_file_batch(batch, &filters)?;
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
struct FileDescriptorRecord {
    id: String,
    directory_id: Option<String>,
    name: String,
    hidden: bool,
    live: MaterializedLiveStateRow,
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
    state_rows: Vec<TransactionWriteRow>,
    file_data_writes: Vec<TransactionFileData>,
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
    version_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    Ok(lix_file_insert_stage_from_batch(batch, version_binding)?.state_rows)
}

fn lix_file_delete_stage_from_batch(
    batch: &RecordBatch,
    version_binding: Option<&str>,
    blob_ref_file_ids: &BTreeSet<String>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    for row_index in 0..batch.num_rows() {
        let file_id = required_string_value(batch, row_index, "id")?;
        let context = file_row_context_from_batch(batch, row_index, version_binding)?;
        staged.extend_filesystem_delete_plan(plan_file_delete(FileDeleteInput {
            file_id: file_id.clone(),
            has_blob_ref: blob_ref_file_ids.contains(&file_id),
            context,
        }));
    }
    Ok(staged)
}

fn blob_ref_file_ids_from_live_rows(
    rows: &[MaterializedLiveStateRow],
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

#[cfg(test)]
fn lix_file_insert_stage_from_batch(
    batch: &RecordBatch,
    version_binding: Option<&str>,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options(batch, version_binding, "lix_file", true, true, true)
}

fn lix_file_insert_stage_from_batch_with_id_generator_and_path_resolvers(
    batch: &RecordBatch,
    version_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_id: &mut dyn FnMut() -> String,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        version_binding,
        surface_name,
        true,
        true,
        include_data_writes,
        Some(path_resolvers),
        Some(generate_id),
    )
}

fn lix_file_insert_stage_from_batch_with_path_resolvers(
    batch: &RecordBatch,
    version_binding: Option<&str>,
    surface_name: &str,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        version_binding,
        surface_name,
        true,
        true,
        include_data_writes,
        Some(path_resolvers),
        Some(generate_directory_id),
    )
}

fn lix_file_existing_update_stage_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    version_binding: Option<&str>,
    include_descriptor_writes: bool,
    include_data_writes: bool,
    path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();
    let mut path_resolvers = path_resolvers;

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let hidden = update_optional_bool_value(batch, assignment_values, row_index, "hidden")?
            .unwrap_or(false);
        let context =
            file_row_context_from_update(batch, assignment_values, row_index, version_binding)?;

        if include_descriptor_writes {
            let directory_id =
                update_optional_string_value(batch, assignment_values, row_index, "directory_id")?;
            let name = update_required_string_value(batch, assignment_values, row_index, "name")?;
            if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
                let resolver = path_resolvers
                    .entry(file_path_resolver_key(&context))
                    .or_insert_with(DirectoryPathResolver::default);
                resolver
                    .reserve_file(directory_id.clone(), name.clone(), id.clone())
                    .map_err(lix_error_to_datafusion_error)?;
            }
            staged
                .state_rows
                .push(file_descriptor_row(FileDescriptorRowInput {
                    id: id.clone(),
                    directory_id,
                    name,
                    hidden,
                    context: context.clone(),
                }));
        }

        if include_data_writes {
            let data = update_required_binary_value(batch, assignment_values, row_index, "data")?;
            stage_lix_file_data_write(&mut staged, id, data, context, None)?;
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
    assignment_values: &UpdateAssignmentValues,
    version_binding: Option<&str>,
    update_columns: LixFileUpdateColumns,
    path_resolvers: Option<&mut BTreeMap<String, DirectoryPathResolver>>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    if update_columns.path || update_columns.descriptor {
        let Some(path_resolvers) = path_resolvers else {
            return Err(DataFusionError::Execution(
                "UPDATE lix_file requires filesystem path resolver".to_string(),
            ));
        };
        return if update_columns.path {
            lix_file_path_update_stage_from_batch(
                batch,
                assignment_values,
                version_binding,
                update_columns,
                path_resolvers,
                generate_directory_id,
            )
        } else {
            lix_file_existing_update_stage_from_batch(
                batch,
                assignment_values,
                version_binding,
                update_columns.descriptor,
                update_columns.data,
                Some(path_resolvers),
            )
        };
    }

    lix_file_existing_update_stage_from_batch(
        batch,
        assignment_values,
        version_binding,
        update_columns.descriptor,
        update_columns.data,
        None,
    )
}

fn lix_file_path_update_stage_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    version_binding: Option<&str>,
    update_columns: LixFileUpdateColumns,
    path_resolvers: &mut BTreeMap<String, DirectoryPathResolver>,
    generate_directory_id: &mut dyn FnMut() -> String,
) -> Result<LixFileStagedBatch> {
    let mut staged = LixFileStagedBatch::default();

    for row_index in 0..batch.num_rows() {
        let id = required_string_value(batch, row_index, "id")?;
        let path = update_required_string_value(batch, assignment_values, row_index, "path")?;
        let hidden = update_optional_bool_value(batch, assignment_values, row_index, "hidden")?
            .unwrap_or(false);
        let context =
            file_row_context_from_update(batch, assignment_values, row_index, version_binding)?;
        let assigned_data = if update_columns.data {
            Some(update_required_binary_value(
                batch,
                assignment_values,
                row_index,
                "data",
            )?)
        } else {
            None
        };

        let resolver = path_resolvers
            .entry(file_path_resolver_key(&context))
            .or_insert_with(DirectoryPathResolver::default);
        let plan = plan_file_path_update(
            resolver,
            id.clone(),
            path,
            hidden,
            None,
            context.clone(),
            generate_directory_id,
        )
        .map_err(lix_error_to_datafusion_error)?;
        staged.extend_filesystem_plan(plan);

        if let Some(data) = assigned_data {
            stage_lix_file_data_write(&mut staged, id, data, context, None)?;
        }
    }

    Ok(staged)
}

#[cfg(test)]
fn lix_file_stage_from_batch_with_options(
    batch: &RecordBatch,
    version_binding: Option<&str>,
    surface_name: &str,
    reject_read_only_fields: bool,
    include_descriptor_writes: bool,
    include_data_writes: bool,
) -> Result<LixFileStagedBatch> {
    lix_file_stage_from_batch_with_options_and_path_resolvers(
        batch,
        version_binding,
        surface_name,
        reject_read_only_fields,
        include_descriptor_writes,
        include_data_writes,
        None,
        None,
    )
}

fn lix_file_stage_from_batch_with_options_and_path_resolvers(
    batch: &RecordBatch,
    version_binding: Option<&str>,
    surface_name: &str,
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
        let id = optional_string_value(batch, row_index, "id")?;
        let hidden = optional_bool_value(batch, row_index, "hidden")?;
        let context = file_row_context_from_batch(batch, row_index, version_binding)?;
        let data = if include_data_writes {
            insert_optional_binary_value(batch, row_index, "data")?
        } else {
            None
        };

        if let Some(path) = path {
            reject_read_only_lix_file_insert_field(batch, row_index, "directory_id")?;
            reject_read_only_lix_file_insert_field(batch, row_index, "name")?;

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
            let file_id = id.unwrap_or_else(|| generate_directory_id());
            let mut plan = super::filesystem_planner::plan_file_path_write(
                resolver,
                FilePathWriteInput {
                    id: Some(file_id.clone()),
                    path,
                    data,
                    hidden,
                    context,
                },
                generate_directory_id,
            )
            .map_err(lix_error_to_datafusion_error)?;
            attach_lix_file_insert_origin(&mut plan.rows, surface_name, &file_id);
            staged.extend_filesystem_plan(plan);
            continue;
        }

        let directory_id = optional_string_value(batch, row_index, "directory_id")?;
        let name = required_string_value(batch, row_index, "name")?;

        let id = if data.is_some() {
            match id {
                Some(id) => Some(id),
                None => {
                    let Some(generate_id) = generate_directory_id.as_deref_mut() else {
                        return Err(DataFusionError::Execution(
                            "INSERT into lix_file with data requires id generator".to_string(),
                        ));
                    };
                    Some(generate_id())
                }
            }
        } else {
            id
        };

        if include_descriptor_writes {
            if let Some(path_resolvers) = path_resolvers.as_deref_mut() {
                if let Some(file_id) = id.as_ref() {
                    let resolver = path_resolvers
                        .entry(file_path_resolver_key(&context))
                        .or_insert_with(DirectoryPathResolver::default);
                    resolver
                        .reserve_file(directory_id.clone(), name.clone(), file_id.clone())
                        .map_err(lix_error_to_datafusion_error)?;
                }
            }
            let mut row = file_descriptor_write_row(FileDescriptorWriteIntent {
                id: id.clone(),
                directory_id: directory_id.clone(),
                name: name.clone(),
                hidden,
                context: context.clone(),
            });
            if let Some(file_id) = id.as_ref() {
                row.origin = Some(lix_file_insert_origin(surface_name, file_id));
            }
            staged.state_rows.push(row);
        }

        if let (Some(id), Some(data)) = (id, data) {
            let origin = Some(lix_file_insert_origin(surface_name, &id));
            stage_lix_file_data_write(&mut staged, id, data, context, origin)?;
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
    origin: Option<TransactionWriteOrigin>,
) -> Result<()> {
    let mut row = blob_ref_row(BlobRefRowInput {
        file_id: file_id.clone(),
        data: data.clone(),
        context: FilesystemRowContext {
            file_id: None,
            metadata: None,
            ..context.clone()
        },
    })
    .map_err(lix_error_to_datafusion_error)?;
    row.origin = origin;
    staged.state_rows.push(row);
    staged.file_data_writes.push(TransactionFileData {
        file_id,
        version_id: context.version_id,
        untracked: context.untracked,
        data,
    });
    Ok(())
}

fn attach_lix_file_insert_origin(
    rows: &mut [TransactionWriteRow],
    surface_name: &str,
    file_id: &str,
) {
    let origin = lix_file_insert_origin(surface_name, file_id);
    for row in rows {
        if row.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY || row.schema_key == BLOB_REF_SCHEMA_KEY {
            row.origin = Some(origin.clone());
        }
    }
}

fn lix_file_insert_origin(surface_name: &str, file_id: &str) -> TransactionWriteOrigin {
    TransactionWriteOrigin {
        surface: surface_name.to_string(),
        operation: TransactionWriteOperation::Insert,
        primary_key: Some(LogicalPrimaryKey {
            columns: vec!["id".to_string()],
            values: vec![file_id.to_string()],
        }),
    }
}

fn file_row_context_from_batch(
    batch: &RecordBatch,
    row_index: usize,
    version_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let explicit_version_id = optional_string_value(batch, row_index, "lixcol_version_id")?;
    let scope = resolve_write_version_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        explicit_version_id,
        version_binding,
        "INSERT into lix_file_by_version",
        "lix_file",
    )?;

    Ok(FilesystemRowContext {
        version_id: scope.version_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: optional_metadata_value(batch, row_index, "lixcol_metadata", "lix_file")?,
    })
}

fn file_row_context_from_update(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    version_binding: Option<&str>,
) -> Result<FilesystemRowContext> {
    let explicit_version_id = optional_string_value(batch, row_index, "lixcol_version_id")?;
    let scope = resolve_write_version_scope(
        optional_bool_value(batch, row_index, "lixcol_global")?,
        explicit_version_id,
        version_binding,
        "UPDATE into lix_file_by_version",
        "lix_file",
    )?;

    Ok(FilesystemRowContext {
        version_id: scope.version_id,
        global: scope.global,
        untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?.unwrap_or(false),
        file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
        metadata: update_optional_metadata_value(
            batch,
            assignment_values,
            row_index,
            "lixcol_metadata",
            "lix_file",
        )?,
    })
}

fn file_path_resolver_key(context: &FilesystemRowContext) -> String {
    filesystem_storage_scope_key(
        &context.version_id,
        context.global,
        context.untracked,
        context.file_id.as_deref(),
    )
}

async fn file_path_resolvers_from_live_state(
    live_state: Arc<dyn LiveStateReader>,
    version_binding: Option<&str>,
) -> std::result::Result<BTreeMap<String, DirectoryPathResolver>, LixError> {
    let rows = live_state
        .scan_rows(&LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                ],
                version_ids: version_binding
                    .map(|version_id| vec![version_id.to_string()])
                    .unwrap_or_default(),
                ..Default::default()
            },
            ..Default::default()
        })
        .await?;
    let mut resolvers = directory_path_resolvers_from_state_rows(rows)?;
    if let Some(version_id) = version_binding {
        let key = filesystem_storage_scope_key(version_id, false, false, None);
        resolvers
            .entry(key)
            .or_insert_with(DirectoryPathResolver::default);
    }
    Ok(resolvers)
}

async fn lix_file_record_batch(
    schema: &SchemaRef,
    blob_reader: &Arc<dyn BlobDataReader>,
    rows: Vec<MaterializedLiveStateRow>,
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

    let directory_paths = derive_directory_paths(&directory_rows)?;
    let mut ids = Vec::new();
    let mut paths = Vec::new();
    let mut directory_ids = Vec::new();
    let mut names = Vec::new();
    let mut hiddens = Vec::new();
    let mut data_values = Vec::new();
    let mut entity_ids = Vec::new();
    let mut schema_keys = Vec::new();
    let mut file_ids = Vec::new();
    let mut globals = Vec::new();
    let mut change_ids = Vec::new();
    let mut created_ats = Vec::new();
    let mut updated_ats = Vec::new();
    let mut commit_ids = Vec::new();
    let mut untracked_values = Vec::new();
    let mut metadata_values = Vec::new();
    let mut version_ids = Vec::new();

    for ((version_id, _), file) in file_rows {
        let directory_path = match file.directory_id.as_ref() {
            Some(directory_id) => {
                let key = (version_id.clone(), directory_id.clone());
                let Some(path) = directory_paths.get(&key).cloned() else {
                    return Err(LixError::new(
                        LixError::CODE_FOREIGN_KEY,
                        format!(
                            "lix_file_descriptor '{}' references missing directory_id '{}' in version '{}'",
                            file.id, directory_id, version_id
                        ),
                    ));
                };
                Some(path)
            }
            None => None,
        };
        let path = match directory_path {
            Some(directory_path) => format!("{directory_path}{}", file.name),
            None => format!("/{}", file.name),
        };
        let data = if needs_data {
            match blob_rows.get(&(version_id.clone(), file.id.clone())) {
                Some(blob_ref) => load_single_blob_bytes(blob_reader, &blob_ref.blob_hash).await?,
                None => None,
            }
        } else {
            None
        };

        ids.push(Some(file.id));
        paths.push(Some(path));
        directory_ids.push(file.directory_id);
        names.push(Some(file.name));
        hiddens.push(Some(file.hidden));
        data_values.push(data);
        entity_ids.push(Some(file.live.entity_id.as_json_array_text()?));
        schema_keys.push(Some(file.live.schema_key));
        file_ids.push(file.live.file_id);
        globals.push(Some(file.live.global));
        change_ids.push(file.live.change_id);
        created_ats.push(file.live.created_at);
        updated_ats.push(file.live.updated_at);
        commit_ids.push(file.live.commit_id);
        untracked_values.push(Some(file.live.untracked));
        metadata_values.push(file.live.metadata.as_ref().map(serialize_row_metadata));
        version_ids.push(Some(version_id));
    }

    let mut columns = Vec::<ArrayRef>::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.name().as_str() {
            "id" => Arc::new(StringArray::from(ids.clone())),
            "path" => Arc::new(StringArray::from(paths.clone())),
            "directory_id" => Arc::new(StringArray::from(directory_ids.clone())),
            "name" => Arc::new(StringArray::from(names.clone())),
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

fn derive_directory_paths(
    rows: &[DirectoryDescriptorRecord],
) -> Result<BTreeMap<(String, String), String>, LixError> {
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
            derive_directory_path_for(
                &version_id,
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
    version_id: &str,
    directory_id: &str,
    records: &BTreeMap<String, &DirectoryDescriptorRecord>,
    paths: &mut BTreeMap<(String, String), String>,
    visiting: &mut BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    if let Some(path) = paths.get(&(version_id.to_string(), directory_id.to_string())) {
        return Ok(Some(path.clone()));
    }
    if !visiting.insert(directory_id.to_string()) {
        return Err(directory_parent_cycle_error(version_id, directory_id));
    }
    let Some(row) = records.get(directory_id) else {
        visiting.remove(directory_id);
        return Ok(None);
    };
    let path = match row.parent_id.as_deref() {
        Some(parent_id) => {
            let Some(parent_path) =
                derive_directory_path_for(version_id, parent_id, records, paths, visiting)?
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
        (version_id.to_string(), directory_id.to_string()),
        path.clone(),
    );
    Ok(Some(path))
}

fn directory_parent_cycle_error(version_id: &str, directory_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_CONSTRAINT_VIOLATION,
        format!(
            "lix_directory_descriptor parent_id cycle in version '{version_id}' while resolving directory '{directory_id}'"
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

fn lix_file_scan_request(
    version_binding: Option<&str>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![
                FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                BLOB_REF_SCHEMA_KEY.to_string(),
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            ],
            version_ids: version_binding
                .map(|version_id| vec![version_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: LiveStateProjection::default(),
        limit,
    }
}

async fn scan_lix_file_live_rows(
    live_state: Arc<dyn LiveStateReader>,
    request: &LiveStateScanRequest,
    target_file_ids: &FileIdConstraint,
) -> std::result::Result<Vec<MaterializedLiveStateRow>, LixError> {
    let target_file_ids = match target_file_ids {
        FileIdConstraint::All => return live_state.scan_rows(request).await,
        FileIdConstraint::None => return Ok(Vec::new()),
        FileIdConstraint::Ids(target_file_ids) => target_file_ids,
    };

    let mut file_request = request.clone();
    file_request.filter.schema_keys = vec![
        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
        BLOB_REF_SCHEMA_KEY.to_string(),
    ];
    file_request.filter.entity_ids = target_file_ids
        .iter()
        .map(|file_id| EntityIdentity::single(file_id.clone()))
        .collect();

    let mut rows = live_state.scan_rows(&file_request).await?;

    let mut directory_request = request.clone();
    directory_request.filter.schema_keys = vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string()];
    directory_request.filter.entity_ids.clear();
    directory_request.limit = None;
    rows.extend(live_state.scan_rows(&directory_request).await?);

    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileIdConstraint {
    All,
    None,
    Ids(BTreeSet<String>),
}

impl FileIdConstraint {
    fn from_ids(ids: Vec<String>) -> Self {
        let ids = ids.into_iter().collect::<BTreeSet<_>>();
        if ids.is_empty() {
            Self::None
        } else {
            Self::Ids(ids)
        }
    }

    fn intersect(self, other: Self) -> Self {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Self::None,
            (Self::All, constraint) | (constraint, Self::All) => constraint,
            (Self::Ids(left), Self::Ids(right)) => {
                let ids = left.intersection(&right).cloned().collect::<BTreeSet<_>>();
                if ids.is_empty() {
                    Self::None
                } else {
                    Self::Ids(ids)
                }
            }
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::None, constraint) | (constraint, Self::None) => constraint,
            (Self::Ids(mut left), Self::Ids(right)) => {
                left.extend(right);
                Self::Ids(left)
            }
        }
    }
}

fn file_id_constraint_from_filters(filters: &[Expr]) -> Result<FileIdConstraint> {
    let analyzer = LixFileIdFilterAnalyzer;
    let mut constraint = FileIdConstraint::All;
    for filter in filters {
        if let Some(filter_constraint) = analyzer.analyze(filter)? {
            constraint = constraint.intersect(filter_constraint);
        }
    }
    Ok(constraint)
}

struct LixFileIdFilterAnalyzer;

impl LixFileIdFilterAnalyzer {
    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<FileIdConstraint>> {
        ExactStringColumnFilterAnalyzer::new("id").analyze(expr)
    }
}

struct ExactStringColumnFilterAnalyzer {
    column_name: &'static str,
}

impl ExactStringColumnFilterAnalyzer {
    fn new(column_name: &'static str) -> Self {
        Self { column_name }
    }

    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<FileIdConstraint>> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let Some(left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.intersect(right)))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let Some(left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.union(right)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(self
                .value_from_binary_filter(binary_expr)
                .map(|value| FileIdConstraint::Ids(BTreeSet::from([value])))),
            Expr::InList(in_list) => Ok(self
                .values_from_in_list_filter(in_list)
                .map(FileIdConstraint::from_ids)),
            _ => Ok(None),
        }
    }

    fn value_from_binary_filter(&self, binary_expr: &BinaryExpr) -> Option<String> {
        if binary_expr.op != Operator::Eq {
            return None;
        }
        self.value_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
            .or_else(|| {
                self.value_from_column_literal_filter(&binary_expr.right, &binary_expr.left)
            })
    }

    fn values_from_in_list_filter(&self, in_list: &InList) -> Option<Vec<String>> {
        if in_list.negated {
            return None;
        }
        let Expr::Column(column) = in_list.expr.as_ref() else {
            return None;
        };
        if column.name != self.column_name {
            return None;
        }
        let values = in_list
            .list
            .iter()
            .map(string_expr_literal)
            .collect::<Option<Vec<_>>>()?;
        Some(values)
    }

    fn value_from_column_literal_filter(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
    ) -> Option<String> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        if column.name != self.column_name {
            return None;
        }
        string_expr_literal(literal_expr)
    }
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

fn contains_column(expr: &Expr, column_name: &str) -> bool {
    match expr {
        Expr::Column(column) => column.name == column_name,
        Expr::BinaryExpr(binary_expr) => {
            contains_column(&binary_expr.left, column_name)
                || contains_column(&binary_expr.right, column_name)
        }
        Expr::InList(in_list) => {
            contains_column(&in_list.expr, column_name)
                || in_list
                    .list
                    .iter()
                    .any(|expr| contains_column(expr, column_name))
        }
        Expr::Between(between) => {
            contains_column(&between.expr, column_name)
                || contains_column(&between.low, column_name)
                || contains_column(&between.high, column_name)
        }
        Expr::Not(expr) | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            contains_column(expr, column_name)
        }
        Expr::Negative(expr) => contains_column(expr, column_name),
        _ => false,
    }
}

fn validate_lix_file_update_assignments(
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, expr) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE lix_file failed: column '{column_name}' does not exist"
            ))
        })?;
        if !matches!(
            column_name.as_str(),
            "path" | "directory_id" | "name" | "hidden" | "data" | "lixcol_metadata"
        ) {
            return Err(DataFusionError::Execution(format!(
                "UPDATE lix_file cannot stage read-only column '{column_name}'"
            )));
        }
        if column_name == "data" {
            reject_non_binary_lix_file_data_assignment(expr)?;
        }
    }
    Ok(())
}

fn reject_non_binary_lix_file_data_assignment(expr: &Expr) -> Result<()> {
    match expr {
        Expr::Literal(value, _) => {
            if !scalar_is_binary_or_null(value) {
                return Err(non_binary_lix_file_data_assignment_error());
            }
        }
        Expr::Cast(cast) if is_binary_type(&cast.data_type) => {
            if !logical_expr_is_binary_or_null(&cast.expr) {
                return Err(non_binary_lix_file_data_assignment_error());
            }
        }
        _ => {}
    }

    Ok(())
}

fn non_binary_lix_file_data_assignment_error() -> DataFusionError {
    lix_file_data_type_error(
        "UPDATE lix_file",
        "data",
        "use X'...' or a binary parameter for file contents",
    )
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

fn update_required_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<String> {
    update_optional_string_value(batch, assignment_values, row_index, column_name)?.ok_or_else(
        || {
            DataFusionError::Execution(format!(
                "UPDATE lix_file requires non-null text column '{column_name}'"
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
            "UPDATE lix_file expected text-compatible column '{column_name}', got {other:?}"
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
                .map_err(super::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(super::error::lix_error_to_datafusion_error)
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
            "UPDATE lix_file expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn update_required_binary_value(
    _batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Vec<u8>> {
    match assignment_values.assigned_cell(row_index, column_name)? {
        UpdateCell::Unassigned | UpdateCell::Assigned(SqlCell::Null) => {
            Err(lix_file_data_type_error(
                "UPDATE lix_file",
                column_name,
                "use X'' for an empty file or omit data to leave contents unchanged",
            ))
        }
        UpdateCell::Assigned(SqlCell::Value(ScalarValue::Binary(Some(value))))
        | UpdateCell::Assigned(SqlCell::Value(ScalarValue::LargeBinary(Some(value)))) => Ok(value),
        UpdateCell::Assigned(SqlCell::Value(ScalarValue::FixedSizeBinary(_, Some(value)))) => {
            Ok(value)
        }
        UpdateCell::Assigned(SqlCell::Value(other)) => Err(lix_file_data_type_error_with_value(
            "UPDATE lix_file",
            column_name,
            &other,
            "use X'...' or a binary parameter for file contents",
        )),
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
            "INSERT into lix_file expected text-compatible column '{column_name}', got {other:?}"
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
                .map_err(super::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(super::error::lix_error_to_datafusion_error)
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
            "INSERT into lix_file expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn insert_optional_binary_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<Vec<u8>>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None => Ok(None),
        Some(ScalarValue::Null)
        | Some(ScalarValue::Binary(None))
        | Some(ScalarValue::LargeBinary(None))
        | Some(ScalarValue::FixedSizeBinary(_, None)) => Err(lix_file_data_type_error(
            "INSERT into lix_file",
            column_name,
            "use X'' for an empty file or omit data to create a descriptor without contents",
        )),
        Some(ScalarValue::Binary(Some(value))) | Some(ScalarValue::LargeBinary(Some(value))) => {
            Ok(Some(value))
        }
        Some(ScalarValue::FixedSizeBinary(_, Some(value))) => Ok(Some(value)),
        Some(other) => Err(lix_file_data_type_error_with_value(
            "INSERT into lix_file",
            column_name,
            &other,
            "use X'...' or a binary parameter for file contents",
        )),
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
        Field::new("id", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, false),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, true),
        Field::new("data", DataType::Binary, true),
        json_field("lixcol_entity_id", false),
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
    super::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    super::error::lix_error_to_datafusion_error(error)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BinaryArray, BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::Value as JsonValue;

    use crate::binary_cas::BlobDataReader;
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::live_state::MaterializedLiveStateRow;
    use crate::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};
    use crate::sql2::dml::InsertSink;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
    };
    use crate::LixError;

    use super::{
        derive_directory_path_for, lix_file_delete_stage_from_batch,
        lix_file_insert_stage_from_batch, lix_file_insert_stage_from_batch_with_path_resolvers,
        lix_file_write_rows_from_batch, DirectoryDescriptorRecord, LixFileInsertSink,
        VersionBinding,
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

    fn string_literal(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    fn column(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    fn eq_filter(column_name: &str, value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column(column_name)),
            Operator::Eq,
            Box::new(string_literal(value)),
        ))
    }

    #[test]
    fn file_id_filters_support_string_id_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;
        let constraint = analyzer
            .analyze(&Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("file-b"), string_literal("file-a")],
                false,
            )))
            .unwrap()
            .unwrap();

        assert_eq!(
            constraint,
            super::FileIdConstraint::Ids(BTreeSet::from([
                "file-a".to_string(),
                "file-b".to_string()
            ]))
        );
        assert!(analyzer.supports(&eq_filter("id", "file-a")));
        assert!(analyzer.supports(&Expr::BinaryExpr(BinaryExpr::new(
            Box::new(string_literal("file-a")),
            Operator::Eq,
            Box::new(column("id")),
        ))));
    }

    #[test]
    fn file_id_filters_intersect_and_union_boolean_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;
        let left = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-a"), string_literal("file-b")],
            false,
        ));
        let right = Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-b"), string_literal("file-c")],
            false,
        ));

        let and_constraint = analyzer
            .analyze(&Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left.clone()),
                Operator::And,
                Box::new(right.clone()),
            )))
            .unwrap()
            .unwrap();
        assert_eq!(
            and_constraint,
            super::FileIdConstraint::Ids(BTreeSet::from(["file-b".to_string()]))
        );

        let or_constraint = analyzer
            .analyze(&Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                Operator::Or,
                Box::new(right),
            )))
            .unwrap()
            .unwrap();
        assert_eq!(
            or_constraint,
            super::FileIdConstraint::Ids(BTreeSet::from([
                "file-a".to_string(),
                "file-b".to_string(),
                "file-c".to_string()
            ]))
        );
    }

    #[test]
    fn file_id_filters_detect_contradictions() {
        let filters = vec![Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "file-a")),
            Operator::And,
            Box::new(eq_filter("id", "file-b")),
        ))];

        assert_eq!(
            super::file_id_constraint_from_filters(&filters).unwrap(),
            super::FileIdConstraint::None
        );
    }

    #[test]
    fn file_id_filters_ignore_non_id_and_negated_predicates() {
        let analyzer = super::LixFileIdFilterAnalyzer;

        assert!(!analyzer.supports(&eq_filter("name", "readme.md")));
        assert!(!analyzer.supports(&Expr::InList(InList::new(
            Box::new(column("id")),
            vec![string_literal("file-a")],
            true,
        ))));
    }

    fn lix_file_update_stage_from_batch_for_test(
        batch: &RecordBatch,
        version_binding: Option<&str>,
        update_columns: super::LixFileUpdateColumns,
        path_resolvers: Option<&mut BTreeMap<String, super::DirectoryPathResolver>>,
        generate_directory_id: &mut dyn FnMut() -> String,
    ) -> datafusion::common::Result<super::LixFileStagedBatch> {
        let mut columns = Vec::new();
        if update_columns.path {
            columns.extend(["path", "hidden"]);
        }
        if update_columns.data {
            columns.push("data");
        }
        if update_columns.descriptor {
            columns.extend(["directory_id", "name", "hidden"]);
        }
        let assignment_values = super::UpdateAssignmentValues::from_batch_columns(batch, &columns);
        super::lix_file_update_stage_from_batch(
            batch,
            &assignment_values,
            version_binding,
            update_columns,
            path_resolvers,
            generate_directory_id,
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
        fn active_version_id(&self) -> &str {
            "version-b"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<JsonValue>, LixError> {
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

        async fn load_version_head(
            &mut self,
            version_id: &str,
        ) -> Result<Option<String>, LixError> {
            if version_id == "ghost-version" {
                return Ok(None);
            }
            Ok(Some(format!("commit-{version_id}")))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    #[derive(Default)]
    struct RowsLiveStateReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for RowsLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn live_directory_row(
        entity_id: &str,
        version_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: super::DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            version_id: version_id.to_string(),
            change_id: Some(format!("change-{entity_id}")),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn live_file_row(
        entity_id: &str,
        version_id: &str,
        snapshot_content: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
            schema_key: super::FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
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
            Field::new("hidden", DataType::Boolean, false),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
        ];
        let mut columns = vec![
            string_column(vec![Some("file-readme")]),
            string_column(vec![Some("dir-docs")]),
            string_column(vec![Some("readme.md")]),
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
                Field::new("hidden", DataType::Boolean, false),
                Field::new("data", DataType::Binary, true),
                Field::new("lixcol_version_id", DataType::Utf8, false),
            ])),
            vec![
                string_column(vec![Some("file-readme")]),
                string_column(vec![Some("dir-docs")]),
                string_column(vec![Some("readme.md")]),
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
            derive_directory_path_for(
                "version-a",
                "dir-guides",
                &records,
                &mut paths,
                &mut BTreeSet::new()
            )
            .expect("path derivation should succeed"),
            Some("/docs/guides/".to_string())
        );
    }

    #[tokio::test]
    async fn file_projection_rejects_unresolved_non_root_directory_id() {
        let blob_reader = Arc::new(CapturingWriteContext::default()) as Arc<dyn BlobDataReader>;
        let error = super::lix_file_record_batch(
            &super::lix_file_schema(),
            &blob_reader,
            vec![live_file_row(
                "file-readme",
                "version-b",
                "{\"id\":\"file-readme\",\"directory_id\":\"missing-dir\",\"name\":\"readme.md\",\"hidden\":false}",
            )],
        )
        .await
        .expect_err("unresolved non-root directory_id should not project as root path");

        assert_eq!(error.code, LixError::CODE_FOREIGN_KEY);
        assert!(error.message.contains("missing-dir"));
    }

    #[test]
    fn decodes_file_insert_into_lix_state_write_row() {
        let batch = file_insert_batch(true, false);

        let rows = lix_file_write_rows_from_batch(&batch, None).expect("decode file insert");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(rows[0].schema_key, "lix_file_descriptor");
        assert_eq!(rows[0].version_id, "version-b");
        assert_eq!(
            rows[0].metadata.as_ref(),
            Some(&TransactionJson::from_value_for_test(
                serde_json::json!({"source": "file"})
            ))
        );
        let snapshot = rows[0].snapshot.as_ref().expect("descriptor snapshot JSON");
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "readme.md");
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
    fn file_insert_rejects_global_with_non_global_version_id() {
        let error = lix_file_write_rows_from_batch(&file_insert_batch(true, true), None)
            .expect_err("global file write should reject conflicting version id");

        assert!(
            error
                .to_string()
                .contains("cannot set lixcol_global=true with non-global lixcol_version_id"),
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
            super::filesystem_storage_scope_key("version-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
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
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
        assert_eq!(snapshot["hidden"], false);
    }

    #[test]
    fn file_path_update_preserves_existing_data_unless_data_is_assigned() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("version-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
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

        let staged = lix_file_update_stage_from_batch_for_test(
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

        let snapshot: JsonValue = staged.state_rows[0]
            .snapshot
            .as_ref()
            .unwrap()
            .value()
            .clone();
        assert_eq!(snapshot["directory_id"], "dir-docs");
        assert_eq!(snapshot["name"], "renamed.md");
    }

    #[tokio::test]
    async fn file_path_update_stages_only_missing_parent_directories() {
        let mut resolvers = super::file_path_resolvers_from_live_state(
            Arc::new(RowsLiveStateReader::default()) as Arc<dyn LiveStateReader>,
            Some("version-b"),
        )
        .await
        .expect("empty directory state should seed path resolver");

        let staged = lix_file_update_stage_from_batch_for_test(
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
        assert_eq!(
            directory.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "dir-generated-docs"
            ))
        );

        let descriptor = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_file_descriptor")
            .expect("file descriptor should be staged");
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-docs");
    }

    #[test]
    fn file_path_update_with_data_assignment_stages_blob_ref_and_payload() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("version-b", false, false, None),
            super::DirectoryPathResolver::from_existing([(
                "/docs/".to_string(),
                "dir-docs".to_string(),
            )])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_update_stage_from_batch_for_test(
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
        let staged = lix_file_update_stage_from_batch_for_test(
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
        assert_eq!(
            blob_ref_row.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
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
        assert_eq!(
            descriptor.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(descriptor.file_id, None);
        assert_eq!(descriptor.snapshot, None);

        let blob_ref = staged
            .state_rows
            .iter()
            .find(|row| row.schema_key == "lix_binary_blob_ref")
            .expect("blob ref tombstone should be staged");
        assert_eq!(
            blob_ref.entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(blob_ref.file_id.as_deref(), Some("file-readme"));
        assert_eq!(blob_ref.snapshot, None);
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
            Some(&crate::entity_identity::EntityIdentity::single(
                "file-readme"
            ))
        );
        assert_eq!(staged.state_rows[0].snapshot, None);
    }

    #[test]
    fn file_path_insert_reuses_existing_parent_directory() {
        let mut resolvers = BTreeMap::new();
        resolvers.insert(
            super::filesystem_storage_scope_key("version-b", false, false, None),
            super::DirectoryPathResolver::from_existing([
                ("/docs/".to_string(), "dir-docs".to_string()),
                ("/docs/guides/".to_string(), "dir-guides".to_string()),
            ])
            .expect("directory resolver should seed"),
        );

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["should-not-be-used"]),
            true,
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
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["id"], "file-readme");
        assert_eq!(snapshot["directory_id"], "dir-guides");
        assert_eq!(snapshot["name"], "readme.md");
    }

    #[test]
    fn file_path_insert_stages_missing_parent_directories_once() {
        let mut resolvers = BTreeMap::new();

        let staged = lix_file_insert_stage_from_batch_with_path_resolvers(
            &path_data_insert_batch(),
            None,
            "lix_file",
            &mut resolvers,
            &mut test_id_generator(&["dir-generated-docs", "dir-generated-guides"]),
            true,
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
        let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
        assert_eq!(snapshot["directory_id"], "dir-generated-guides");
    }

    #[tokio::test]
    async fn file_insert_sink_stages_decoded_lix_state_rows() {
        let batch = file_insert_batch(true, false);
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            batch.schema(),
            write_ctx,
            test_functions(),
            VersionBinding::explicit(),
            false,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::Rows { mode, rows } => {
                assert_eq!(*mode, TransactionWriteMode::Insert);
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].entity_id.as_ref(),
                    Some(&crate::entity_identity::EntityIdentity::single(
                        "file-readme"
                    ))
                );
                assert_eq!(rows[0].schema_key, "lix_file_descriptor");
            }
            other => panic!("expected insert staged write, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_insert_sink_stages_file_data_writes() {
        let batch = data_insert_batch();
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            batch.schema(),
            write_ctx,
            test_functions(),
            VersionBinding::explicit(),
            true,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileData {
                mode,
                rows,
                file_data,
                count,
                ..
            } => {
                assert_eq!(*mode, TransactionWriteMode::Insert);
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
        let mut write_context = CapturingWriteContext {
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
            writes: Vec::new(),
        };
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let sink = LixFileInsertSink::new(
            batch.schema(),
            write_ctx,
            test_functions(),
            VersionBinding::explicit(),
            true,
        );

        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("file insert sink should stage path data");

        assert_eq!(count, 1);
        let writes = &write_context.writes;
        assert_eq!(writes.len(), 1);
        match &writes[0] {
            TransactionWrite::RowsWithFileData {
                rows,
                file_data,
                count,
                ..
            } => {
                assert_eq!(*count, 1);
                assert_eq!(file_data.len(), 1);
                assert_eq!(file_data[0].file_id, "file-readme");
                let descriptor = rows
                    .iter()
                    .find(|row| row.schema_key == "lix_file_descriptor")
                    .expect("file descriptor row should be staged");
                let snapshot: JsonValue = descriptor.snapshot.as_ref().unwrap().value().clone();
                assert_eq!(snapshot["directory_id"], "dir-guides");
            }
            other => panic!("expected insert with file data staged write, got {other:?}"),
        }
    }
}
