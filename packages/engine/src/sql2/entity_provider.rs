use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
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
use serde_json::Value as JsonValue;

use crate::commit_graph::CommitGraphReader;
use crate::entity_identity::EntityIdentity;
use crate::live_state::LiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::sql2::version_scope::{
    explicit_version_ids_from_dml_filters, resolve_provider_version_ids, VersionBinding,
};
use crate::sql2::write_normalization::UpdateAssignmentValues;
use crate::transaction::types::StageRow;
use crate::version_ref::VersionRefReader;
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

use super::entity_history_provider::EntityHistoryProvider;
use super::history_route::{
    HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_COMMIT_ID, HISTORY_COL_DEPTH,
    HISTORY_COL_ENTITY_ID, HISTORY_COL_FILE_ID, HISTORY_COL_METADATA, HISTORY_COL_SCHEMA_KEY,
    HISTORY_COL_SCHEMA_VERSION, HISTORY_COL_SNAPSHOT_CONTENT, HISTORY_COL_START_COMMIT_ID,
};
use super::result_metadata::{json_field, mark_json_field};
use crate::sql2::{
    SqlWriteContext, WriteAccess, WriteContextLiveStateReader, WriteContextVersionRefReader,
};
use crate::transaction::types::{StageWrite, StageWriteMode};

pub(crate) async fn register_entity_providers(
    ctx: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    commit_graph: Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>,
    schema_definitions: &[JsonValue],
) -> Result<(), LixError> {
    for schema in schema_definitions {
        let spec = match derive_entity_surface_spec_from_schema(schema) {
            Ok(spec) => Arc::new(spec),
            Err(_) => continue,
        };

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            continue;
        }

        let by_version_name = format!("{}_by_version", spec.schema_key);
        ctx.register_table(
            &by_version_name,
            Arc::new(EntityProvider::by_version(
                Arc::clone(&spec),
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        ctx.register_table(
            &spec.schema_key,
            Arc::new(EntityProvider::active(
                Arc::clone(&spec),
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
                active_version_id.to_string(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        let history_name = format!("{}_history", spec.schema_key);
        ctx.register_table(
            &history_name,
            Arc::new(EntityHistoryProvider::new(
                Arc::clone(&spec),
                Arc::clone(&commit_graph),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    }

    Ok(())
}

pub(crate) async fn register_entity_write_providers(
    ctx: &SessionContext,
    write_ctx: SqlWriteContext,
    schema_definitions: &[JsonValue],
) -> Result<(), LixError> {
    for schema in schema_definitions {
        let spec = match derive_entity_surface_spec_from_schema(schema) {
            Ok(spec) => Arc::new(spec),
            Err(_) => continue,
        };

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            continue;
        }

        let by_version_name = format!("{}_by_version", spec.schema_key);
        ctx.register_table(
            &by_version_name,
            Arc::new(EntityProvider::by_version_with_write(
                Arc::clone(&spec),
                write_ctx.clone(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        ctx.register_table(
            &spec.schema_key,
            Arc::new(EntityProvider::active_with_write(
                Arc::clone(&spec),
                write_ctx.clone(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntityProviderVariant {
    Active,
    ByVersion,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntityColumnType {
    String,
    Json,
    Integer,
    Number,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntitySurfaceSpec {
    pub(super) schema_key: String,
    schema_version: Option<String>,
    pub(super) primary_key_paths: Vec<Vec<String>>,
    pub(super) visible_columns: Vec<String>,
    pub(super) column_types: BTreeMap<String, EntityColumnType>,
    defaulted_columns: BTreeSet<String>,
}

pub(crate) struct EntityProvider {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    write_access: WriteAccess,
    schema: SchemaRef,
    variant: EntityProviderVariant,
    version_binding: VersionBinding,
}

impl std::fmt::Debug for EntityProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityProvider")
            .field("schema_key", &self.spec.schema_key)
            .field("variant", &self.variant)
            .finish()
    }
}

impl EntityProvider {
    fn active(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
        active_version_id: String,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::Active),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::read_only(),
            variant: EntityProviderVariant::Active,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    fn active_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let active_version_id = write_ctx.active_version_id();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::Active),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::write(write_ctx),
            variant: EntityProviderVariant::Active,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    fn by_version(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::ByVersion),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::read_only(),
            variant: EntityProviderVariant::ByVersion,
            version_binding: VersionBinding::explicit(),
        }
    }

    fn by_version_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::ByVersion),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::write(write_ctx),
            variant: EntityProviderVariant::ByVersion,
            version_binding: VersionBinding::explicit(),
        }
    }
}

#[async_trait]
impl TableProvider for EntityProvider {
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
                if explicit_version_ids_from_dml_filters(&[(*filter).clone()]).is_empty() {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
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
        let mut request = entity_live_state_scan_request(
            &self.spec.schema_key,
            self.version_binding.active_version_id(),
            limit,
        );
        if self.write_access.is_write() && matches!(self.version_binding, VersionBinding::Explicit)
        {
            request.filter.version_ids = explicit_version_ids_from_dml_filters(filters);
            if request.filter.version_ids.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "DELETE FROM {}_by_version requires an explicit lixcol_version_id predicate",
                    self.spec.schema_key
                )));
            }
        }
        request.filter.version_ids = resolve_provider_version_ids(
            self.version_ref.as_ref(),
            &self.version_binding,
            request.filter.version_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;

        Ok(Arc::new(EntityScanExec::new(
            Arc::clone(&self.spec),
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
            return not_impl_err!("{insert_op} not implemented for entity surfaces yet");
        }

        let write_ctx = self.write_access.require_write(&format!(
            "INSERT into {} entity surface",
            self.spec.schema_key
        ))?;

        let insert_version_binding = match self.variant {
            EntityProviderVariant::Active => self.version_binding.clone(),
            EntityProviderVariant::ByVersion => VersionBinding::explicit(),
            EntityProviderVariant::History => {
                return not_impl_err!("INSERT is not implemented for entity history surfaces");
            }
        };

        let sink = EntityInsertSink::new(
            Arc::clone(&self.spec),
            input.schema(),
            write_ctx.clone(),
            insert_version_binding,
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let write_ctx = self.write_access.require_write(&format!(
            "DELETE FROM {} entity surface",
            self.spec.schema_key
        ))?;

        let version_binding = match self.variant {
            EntityProviderVariant::Active => self.version_binding.clone(),
            EntityProviderVariant::ByVersion => VersionBinding::explicit(),
            EntityProviderVariant::History => {
                return not_impl_err!("DELETE is not implemented for entity history surfaces");
            }
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let mut request = entity_live_state_scan_request(
            &self.spec.schema_key,
            version_binding.active_version_id(),
            None,
        );
        if matches!(version_binding, VersionBinding::Explicit) {
            request.filter.version_ids = explicit_version_ids_from_dml_filters(&filters);
            if request.filter.version_ids.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "DELETE FROM {}_by_version requires an explicit lixcol_version_id predicate",
                    self.spec.schema_key
                )));
            }
        }

        Ok(Arc::new(EntityDeleteExec::new(
            Arc::clone(&self.spec),
            write_ctx.clone(),
            Arc::clone(&self.schema),
            version_binding,
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
        let write_ctx = self
            .write_access
            .require_write(&format!("UPDATE {} entity surface", self.spec.schema_key))?;

        validate_entity_update_assignments(&self.spec, &self.schema, &assignments)?;

        let version_binding = match self.variant {
            EntityProviderVariant::Active => self.version_binding.clone(),
            EntityProviderVariant::ByVersion => VersionBinding::explicit(),
            EntityProviderVariant::History => {
                return not_impl_err!("UPDATE is not implemented for entity history surfaces");
            }
        };

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
        let request = entity_live_state_scan_request(
            &self.spec.schema_key,
            version_binding.active_version_id(),
            None,
        );

        Ok(Arc::new(EntityUpdateExec::new(
            Arc::clone(&self.spec),
            write_ctx.clone(),
            Arc::clone(&self.schema),
            version_binding,
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct EntityInsertSink {
    spec: Arc<EntitySurfaceSpec>,
    schema: SchemaRef,
    write_ctx: SqlWriteContext,
    version_binding: VersionBinding,
}

impl std::fmt::Debug for EntityInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityInsertSink")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityInsertSink {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        schema: SchemaRef,
        write_ctx: SqlWriteContext,
        version_binding: VersionBinding,
    ) -> Self {
        Self {
            spec,
            schema,
            write_ctx,
            version_binding,
        }
    }
}

impl DisplayAs for EntityInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EntityInsertSink(schema_key={})", self.spec.schema_key)
            }
            DisplayFormatType::TreeRender => write!(f, "EntityInsertSink"),
        }
    }
}

#[async_trait]
impl DataSink for EntityInsertSink {
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
            rows.extend(entity_lix_state_write_rows_from_batch(
                &self.spec,
                &batch,
                self.version_binding.active_version_id(),
            )?);
        }
        let count = u64::try_from(rows.len())
            .map_err(|_| DataFusionError::Execution("entity INSERT row count overflow".into()))?;

        self.write_ctx
            .stage_write(StageWrite::Rows {
                mode: StageWriteMode::Insert,
                rows,
            })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

#[allow(dead_code)]
struct EntityDeleteExec {
    spec: Arc<EntitySurfaceSpec>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityDeleteExec")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityDeleteExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
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
            spec,
            write_ctx,
            table_schema,
            version_binding,
            request,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "EntityDeleteExec(schema_key={}, filters={})",
                    self.spec.schema_key,
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "EntityDeleteExec"),
        }
    }
}

impl ExecutionPlan for EntityDeleteExec {
    fn name(&self) -> &str {
        "EntityDeleteExec"
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
                "EntityDeleteExec does not accept children".to_string(),
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
                "EntityDeleteExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let request = self.request.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                write_ctx
                    .scan_live_state(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let mut write_rows = entity_existing_lix_state_write_rows_from_batch(
                &spec,
                &matched_batch,
                version_binding.active_version_id(),
            )?;
            for row in &mut write_rows {
                row.snapshot_content = None;
            }
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity DELETE row count overflow".to_string())
            })?;

            if count > 0 {
                write_ctx
                    .stage_write(StageWrite::Rows {
                        mode: StageWriteMode::Replace,
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

#[allow(dead_code)]
struct EntityUpdateExec {
    spec: Arc<EntitySurfaceSpec>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    request: LiveStateScanRequest,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityUpdateExec")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityUpdateExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
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
            spec,
            write_ctx,
            table_schema,
            version_binding,
            request,
            assignments,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "EntityUpdateExec(schema_key={}, assignments={}, filters={})",
                    self.spec.schema_key,
                    self.assignments.len(),
                    self.filters.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "EntityUpdateExec"),
        }
    }
}

impl ExecutionPlan for EntityUpdateExec {
    fn name(&self) -> &str {
        "EntityUpdateExec"
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
                "EntityUpdateExec does not accept children".to_string(),
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
                "EntityUpdateExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let request = self.request.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                write_ctx
                    .scan_live_state(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let write_rows = entity_update_write_rows_from_batch(
                &spec,
                &matched_batch,
                &assignments,
                version_binding.active_version_id(),
            )?;
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity UPDATE row count overflow".to_string())
            })?;

            if count > 0 {
                write_ctx
                    .stage_write(StageWrite::Rows {
                        mode: StageWriteMode::Replace,
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

fn validate_entity_update_assignments(
    spec: &EntitySurfaceSpec,
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE entity surface '{}' failed: column '{column_name}' does not exist",
                spec.schema_key
            ))
        })?;
        if !spec.visible_columns.iter().any(|name| name == column_name)
            && column_name != "lixcol_metadata"
        {
            return Err(DataFusionError::Execution(format!(
                "UPDATE entity surface '{}' cannot stage read-only column '{column_name}'",
                spec.schema_key
            )));
        }
    }
    Ok(())
}

fn filter_entity_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_entity_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_entity_filters(
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
                DataFusionError::Execution("entity surface filter was not boolean".to_string())
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

fn entity_update_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    version_binding: Option<&str>,
) -> Result<Vec<StageRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    (0..batch.num_rows())
        .map(|row_index| {
            let explicit_global = optional_bool_value(batch, row_index, "lixcol_global")?;
            let version_id = if explicit_global == Some(true) {
                GLOBAL_VERSION_ID.to_string()
            } else {
                optional_string_value(batch, row_index, "lixcol_version_id")?
                    .or_else(|| version_binding.map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "UPDATE into {}_by_version requires lixcol_version_id",
                            spec.schema_key
                        ))
                    })?
            };
            let global = explicit_global.unwrap_or(version_id == GLOBAL_VERSION_ID);

            let schema_version = optional_string_value(batch, row_index, "lixcol_schema_version")?
                .or_else(|| spec.schema_version.clone())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "UPDATE entity surface '{}' requires lixcol_schema_version",
                        spec.schema_key
                    ))
                })?;

            Ok(StageRow {
                entity_id: optional_string_value(batch, row_index, "lixcol_entity_id")?
                    .map(|entity_id| {
                        EntityIdentity::from_string(&entity_id).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "UPDATE entity surface '{}' has invalid lixcol_entity_id: {error}",
                                spec.schema_key
                            ))
                        })
                    })
                    .transpose()?,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                snapshot_content: Some(entity_update_snapshot_content_from_batch(
                    spec,
                    batch,
                    &assignment_values,
                    row_index,
                )?),
                metadata: entity_update_optional_string_value(
                    batch,
                    &assignment_values,
                    row_index,
                    "lixcol_metadata",
                )?,
                schema_version,
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

fn entity_update_snapshot_content_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
) -> Result<String> {
    let mut object = serde_json::Map::new();
    for column_name in &spec.visible_columns {
        let column_type = spec.column_types.get(column_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "entity surface '{}' is missing type metadata for '{}'",
                spec.schema_key, column_name
            ))
        })?;
        let value = entity_update_scalar_value(batch, assignment_values, row_index, column_name)?;
        let value = entity_json_value_from_scalar(value, *column_type)?;
        if value.is_null() && spec.defaulted_columns.contains(column_name) {
            continue;
        }
        object.insert(column_name.clone(), value);
    }
    serde_json::to_string(&JsonValue::Object(object)).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to serialize entity surface '{}' snapshot_content: {error}",
            spec.schema_key
        ))
    })
}

fn entity_update_optional_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match entity_update_scalar_value(batch, assignment_values, row_index, column_name)? {
        None
        | Some(ScalarValue::Null)
        | Some(ScalarValue::Utf8(None))
        | Some(ScalarValue::Utf8View(None))
        | Some(ScalarValue::LargeUtf8(None)) => Ok(None),
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "UPDATE entity surface expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn entity_update_scalar_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    assignment_values.scalar_value(batch, row_index, column_name)
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

fn entity_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    version_binding: Option<&str>,
) -> Result<Vec<StageRow>> {
    entity_lix_state_write_rows_from_batch_with_options(spec, batch, version_binding, true)
}

fn entity_existing_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    version_binding: Option<&str>,
) -> Result<Vec<StageRow>> {
    entity_lix_state_write_rows_from_batch_with_options(spec, batch, version_binding, false)
}

fn entity_lix_state_write_rows_from_batch_with_options(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    version_binding: Option<&str>,
    reject_read_only_fields: bool,
) -> Result<Vec<StageRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let explicit_global = optional_bool_value(batch, row_index, "lixcol_global")?;
            let version_id = if explicit_global == Some(true) {
                GLOBAL_VERSION_ID.to_string()
            } else {
                optional_string_value(batch, row_index, "lixcol_version_id")?
                    .or_else(|| version_binding.map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "INSERT into {}_by_version requires lixcol_version_id",
                            spec.schema_key
                        ))
                    })?
            };
            let global = explicit_global.unwrap_or(version_id == GLOBAL_VERSION_ID);

            if let Some(schema_key) = optional_string_value(batch, row_index, "lixcol_schema_key")?
            {
                if schema_key != spec.schema_key {
                    return Err(DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' cannot set lixcol_schema_key to '{}'",
                        spec.schema_key, schema_key
                    )));
                }
            }

            if reject_read_only_fields {
                reject_present_entity_insert_field(batch, row_index, "lixcol_snapshot_content")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_created_at")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_updated_at")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_change_id")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_commit_id")?;
            }

            let schema_version = optional_string_value(batch, row_index, "lixcol_schema_version")?
                .or_else(|| spec.schema_version.clone())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' requires lixcol_schema_version",
                        spec.schema_key
                    ))
                })?;
            let snapshot_content = entity_snapshot_content_from_batch(spec, batch, row_index)?;
            let explicit_entity_id = optional_string_value(batch, row_index, "lixcol_entity_id")?;
            let entity_id = if spec.primary_key_paths.is_empty() {
                let entity_id = explicit_entity_id.ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' requires lixcol_entity_id because the schema has no x-lix-primary-key",
                        spec.schema_key
                    ))
                })?;
                Some(EntityIdentity::from_string(&entity_id).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' has invalid lixcol_entity_id: {error}",
                        spec.schema_key
                    ))
                })?)
            } else {
                explicit_entity_id
                    .map(|entity_id| {
                        EntityIdentity::from_string(&entity_id).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "INSERT into entity surface '{}' has invalid lixcol_entity_id: {error}",
                                spec.schema_key
                            ))
                        })
                    })
                    .transpose()?
            };

            Ok(StageRow {
                entity_id,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                snapshot_content: Some(snapshot_content),
                metadata: optional_string_value(batch, row_index, "lixcol_metadata")?,
                schema_version: schema_version,
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

fn entity_snapshot_content_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    row_index: usize,
) -> Result<String> {
    let mut object = serde_json::Map::new();
    for column_name in &spec.visible_columns {
        if !batch
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == column_name)
        {
            continue;
        }
        let column_type = spec.column_types.get(column_name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "entity surface '{}' is missing type metadata for '{}'",
                spec.schema_key, column_name
            ))
        })?;
        let value = optional_scalar_value(batch, row_index, column_name)?;
        let value = entity_json_value_from_scalar(value, *column_type)?;
        if value.is_null() && spec.defaulted_columns.contains(column_name) {
            continue;
        }
        object.insert(column_name.clone(), value);
    }
    serde_json::to_string(&JsonValue::Object(object)).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to serialize entity surface '{}' snapshot_content: {error}",
            spec.schema_key
        ))
    })
}

fn entity_json_value_from_scalar(
    value: Option<ScalarValue>,
    column_type: EntityColumnType,
) -> Result<JsonValue> {
    let Some(value) = value else {
        return Ok(JsonValue::Null);
    };
    match value {
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Int32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Float32(None) => Ok(JsonValue::Null),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => match column_type {
            EntityColumnType::Json => {
                // JSON surface columns accept SQL strings as JSON string values,
                // while still allowing callers to pass serialized JSON text for
                // objects, arrays, numbers, booleans, and null.
                Ok(serde_json::from_str(&value).unwrap_or(JsonValue::String(value)))
            }
            EntityColumnType::Integer => {
                value.parse::<i64>().map(JsonValue::from).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity integer column expected integer text, got error: {error}"
                    ))
                })
            }
            EntityColumnType::Number => value
                .parse::<f64>()
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity number column expected number text, got error: {error}"
                    ))
                })
                .and_then(json_number_from_f64),
            EntityColumnType::Boolean => {
                value.parse::<bool>().map(JsonValue::from).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity boolean column expected boolean text, got error: {error}"
                    ))
                })
            }
            EntityColumnType::String => Ok(JsonValue::String(value)),
        },
        ScalarValue::Boolean(Some(value)) => Ok(JsonValue::Bool(value)),
        ScalarValue::Int64(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::Int32(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::UInt64(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::UInt32(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::Float64(Some(value)) => json_number_from_f64(value),
        ScalarValue::Float32(Some(value)) => json_number_from_f64(value as f64),
        other => Err(DataFusionError::Execution(format!(
            "entity insert does not support scalar value {other:?}"
        ))),
    }
}

fn json_number_from_f64(value: f64) -> Result<JsonValue> {
    serde_json::Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("entity number column cannot store {value}"))
        })
}

fn reject_present_entity_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into entity surface cannot stage read-only column '{column_name}'"
        )));
    }
    Ok(())
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
            "INSERT into entity surface expected text-compatible column '{column_name}', got {other:?}"
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
            "INSERT into entity surface expected boolean column '{column_name}', got {other:?}"
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
            "row index {row_index} out of bounds for entity batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode entity column '{column_name}' at row {row_index}: {error}"
            ))
        })
}

struct EntityScanExec {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateReader>,
    schema: SchemaRef,
    request: LiveStateScanRequest,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityScanExec")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityScanExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
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
            spec,
            live_state,
            schema,
            request,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "EntityScanExec(schema_key={}, limit={:?})",
                    self.spec.schema_key, self.request.limit
                )
            }
            DisplayFormatType::TreeRender => write!(f, "EntityScanExec"),
        }
    }
}

impl ExecutionPlan for EntityScanExec {
    fn name(&self) -> &str {
        "EntityScanExec"
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
                "EntityScanExec does not accept children".to_string(),
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
                "EntityScanExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
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
            let batch = entity_record_batch(&spec, Arc::clone(&stream_schema), &rows)?;
            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                batch,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn entity_live_state_scan_request(
    schema_key: &str,
    active_version_id: Option<&str>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![schema_key.to_string()],
            version_ids: active_version_id
                .map(|version_id| vec![version_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: LiveStateProjection::default(),
        limit,
    }
}

fn entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &[LiveStateRow],
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }

    let snapshots = rows
        .iter()
        .map(|row| parse_snapshot(row.snapshot_content.as_deref()))
        .collect::<Result<Vec<_>>>()?;

    let columns = schema
        .fields()
        .iter()
        .map(|field| entity_column_array(spec, field.name(), rows, &snapshots))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn entity_column_array(
    spec: &EntitySurfaceSpec,
    column_name: &str,
    rows: &[LiveStateRow],
    snapshots: &[Option<JsonValue>],
) -> Result<ArrayRef> {
    if let Some(property_name) = column_name.strip_prefix("lixcol_") {
        return entity_system_column_array(property_name, rows);
    }

    let column_type = spec.column_types.get(column_name).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "sql2 entity provider '{}' does not expose column '{}'",
            spec.schema_key, column_name
        ))
    })?;

    let values = snapshots
        .iter()
        .map(|snapshot| snapshot.as_ref().and_then(|value| value.get(column_name)))
        .collect::<Vec<_>>();
    Ok(match column_type {
        EntityColumnType::String | EntityColumnType::Json => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| entity_json_text_value(*value, *column_type))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Integer => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| entity_i64_value(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Number => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| entity_f64_value(*value))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        EntityColumnType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    })
}

fn entity_system_column_array(column_name: &str, rows: &[LiveStateRow]) -> Result<ArrayRef> {
    Ok(match column_name {
        "entity_id" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.entity_id
                        .as_string()
                        .map(Some)
                        .map_err(lix_error_to_datafusion_error)
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
        "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
        "snapshot_content" => string_array(rows.iter().map(|row| row.snapshot_content.as_deref())),
        "metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
        "schema_version" => string_array(rows.iter().map(|row| Some(row.schema_version.as_str()))),
        "created_at" => string_array(rows.iter().map(|row| Some(row.created_at.as_str()))),
        "updated_at" => string_array(rows.iter().map(|row| Some(row.updated_at.as_str()))),
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
            return Err(DataFusionError::Execution(format!(
                "sql2 entity provider does not support system column 'lixcol_{other}'"
            )))
        }
    })
}

pub(super) fn parse_snapshot(snapshot_content: Option<&str>) -> Result<Option<JsonValue>> {
    snapshot_content
        .map(|snapshot| {
            serde_json::from_str::<JsonValue>(snapshot).map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 entity provider expected valid snapshot_content JSON: {error}"
                ))
            })
        })
        .transpose()
}

pub(super) fn entity_json_text_value(
    value: Option<&JsonValue>,
    column_type: EntityColumnType,
) -> Result<Option<String>> {
    Ok(match (column_type, value) {
        (_, None) | (_, Some(JsonValue::Null)) => None,
        (EntityColumnType::String, Some(JsonValue::Bool(value))) => Some(if *value {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        (EntityColumnType::String, Some(JsonValue::String(value))) => Some(value.clone()),
        (EntityColumnType::String, Some(other)) => Some(json_to_string(other)?),
        (EntityColumnType::Json, Some(other)) => Some(json_to_string(other)?),
        _ => None,
    })
}

pub(super) fn entity_i64_value(value: Option<&JsonValue>) -> Option<i64> {
    match value {
        Some(JsonValue::Number(number)) => number.as_i64(),
        Some(JsonValue::String(value)) => value.parse::<i64>().ok(),
        _ => None,
    }
}

pub(super) fn entity_f64_value(value: Option<&JsonValue>) -> Option<f64> {
    match value {
        Some(JsonValue::Number(number)) => number.as_f64(),
        Some(JsonValue::String(value)) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn json_to_string(value: &JsonValue) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!("failed to render JSON value: {error}"))
    })
}

pub(super) fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

pub(super) fn entity_surface_schema(
    spec: &EntitySurfaceSpec,
    variant: EntityProviderVariant,
) -> SchemaRef {
    let mut fields = spec
        .visible_columns
        .iter()
        .filter_map(|column_name| {
            let column_type = spec.column_types.get(column_name)?;
            let field = Field::new(
                column_name,
                arrow_data_type_for_entity_column_type(*column_type),
                true,
            );
            Some(if *column_type == EntityColumnType::Json {
                mark_json_field(field)
            } else {
                field
            })
        })
        .collect::<Vec<_>>();

    fields.extend(entity_system_fields(variant));
    Arc::new(Schema::new(fields))
}

fn arrow_data_type_for_entity_column_type(column_type: EntityColumnType) -> DataType {
    match column_type {
        EntityColumnType::String | EntityColumnType::Json => DataType::Utf8,
        EntityColumnType::Integer => DataType::Int64,
        EntityColumnType::Number => DataType::Float64,
        EntityColumnType::Boolean => DataType::Boolean,
    }
}

pub(super) fn entity_system_fields(variant: EntityProviderVariant) -> Vec<Field> {
    if variant == EntityProviderVariant::History {
        return vec![
            Field::new(HISTORY_COL_ENTITY_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
            Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
            json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
            json_field(HISTORY_COL_METADATA, true),
            Field::new(HISTORY_COL_SCHEMA_VERSION, DataType::Utf8, false),
            Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_START_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        ];
    }

    let mut fields = vec![
        Field::new("lixcol_entity_id", DataType::Utf8, true),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        json_field("lixcol_snapshot_content", true),
        json_field("lixcol_metadata", true),
        Field::new("lixcol_schema_version", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
    ];
    if variant == EntityProviderVariant::ByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };
    Ok(Arc::new(schema.project(projection)?))
}

fn derive_entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> std::result::Result<EntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "schema is missing string x-lix-key".to_string(),
            )
        })?;

    let schema_version = schema
        .get("x-lix-version")
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned);

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();

    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    entity_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let defaulted_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, property_schema)| {
                    !key.starts_with("lixcol_") && property_schema_has_default(property_schema)
                })
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let primary_key_paths = parse_primary_key_paths(schema)?;

    Ok(EntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        schema_version,
        primary_key_paths,
        visible_columns,
        column_types,
        defaulted_columns,
    })
}

fn property_schema_has_default(schema: &JsonValue) -> bool {
    schema.get("x-lix-default").is_some() || schema.get("default").is_some()
}

fn parse_primary_key_paths(schema: &JsonValue) -> std::result::Result<Vec<Vec<String>>, LixError> {
    let Some(primary_key) = schema.get("x-lix-primary-key") else {
        return Ok(Vec::new());
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "schema x-lix-primary-key must be an array of JSON Pointers".to_string(),
        )
    })?;

    primary_key
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("schema x-lix-primary-key entry at index {index} must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect()
}

// TODO(engine2): share JSON Pointer parsing with schema/canonical validation once
// those helpers have a clean module boundary for SQL providers.
fn parse_json_pointer(pointer: &str) -> std::result::Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> std::result::Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid JSON pointer segment '{segment}'"),
                    ))
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(
        schema_key,
        "lix_active_version"
            | "lix_active_account"
            | "lix_change"
            | "lix_commit"
            | "lix_commit_edge"
            | "lix_change_set"
            | "lix_change_set_element"
    )
}

fn entity_column_type_from_schema(schema: &JsonValue) -> Option<EntityColumnType> {
    let mut kinds = BTreeSet::new();
    collect_entity_type_kinds(schema, &mut kinds);
    kinds.remove("null");

    if kinds.is_empty() {
        return None;
    }

    if kinds.len() == 1 {
        return match kinds.into_iter().next() {
            Some("boolean") => Some(EntityColumnType::Boolean),
            Some("integer") => Some(EntityColumnType::Integer),
            Some("number") => Some(EntityColumnType::Number),
            Some("string") => Some(EntityColumnType::String),
            Some("object" | "array") => Some(EntityColumnType::Json),
            _ => None,
        };
    }

    Some(EntityColumnType::Json)
}

fn collect_entity_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => {
            out.insert(kind.as_str());
        }
        Some(JsonValue::Array(kinds)) => {
            for kind in kinds.iter().filter_map(JsonValue::as_str) {
                out.insert(kind);
            }
        }
        _ => {}
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(JsonValue::Array(branches)) = schema.get(keyword) {
            for branch in branches {
                collect_entity_type_kinds(branch, out);
            }
        }
    }
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    super::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::sink::DataSink;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures_util::stream;
    use serde_json::json;

    use super::{
        derive_entity_surface_spec_from_schema, entity_lix_state_write_rows_from_batch,
        entity_record_batch, entity_surface_schema, schema_exposed_as_entity_surface,
        EntityColumnType, EntityInsertSink, EntityProviderVariant,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::live_state::{
        LiveStateReader, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
    };
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{StageRow, StageWrite, StageWriteMode, StageWriteOutcome};
    use crate::version_ref::{VersionHead, VersionRefReader};
    use crate::LixError;

    struct EmptyLiveStateReader;
    struct EmptyVersionRefReader;
    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<LiveStateRow>,
        writes: Vec<StageWrite>,
    }

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl VersionRefReader for EmptyVersionRefReader {
        async fn load_head(&self, _version_id: &str) -> Result<Option<VersionHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
            Ok(Vec::new())
        }
    }

    fn empty_version_ref() -> Arc<dyn VersionRefReader> {
        Arc::new(EmptyVersionRefReader)
    }

    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
        )
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_blob_data_by_hash(
            &self,
            _blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_version_id(&self) -> &str {
            "version-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn blob_reader(&self) -> Arc<dyn BlobDataReader> {
            Arc::new(CapturingWriteContext::default())
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_version_head(
            &mut self,
            _version_id: &str,
        ) -> Result<Option<String>, LixError> {
            Ok(None)
        }

        async fn stage_write(&mut self, write: StageWrite) -> Result<StageWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(StageWriteOutcome { count: 0 })
        }
    }

    fn live_row() -> LiveStateRow {
        LiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "project_message".to_string(),
            file_id: None,
            snapshot_content: Some(
                "{\"body\":\"hello\",\"rating\":4.5,\"count\":7,\"enabled\":true,\"meta\":{\"x\":1}}"
                    .to_string(),
            ),
            metadata: Some("{\"source\":\"test\"}".to_string()),
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: Some("change-a".to_string()),
            commit_id: Some("commit-a".to_string()),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn entity_insert_spec() -> Arc<super::EntitySurfaceSpec> {
        Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "count": { "type": "integer" },
                    "enabled": { "type": "boolean" },
                    "meta": { "type": "object" },
                    "rating": { "type": "number" }
                }
            }))
            .expect("schema should derive entity surface spec"),
        )
    }

    fn entity_insert_spec_with_primary_key() -> Arc<super::EntitySurfaceSpec> {
        Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-version": "1",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "body": { "type": "string" }
                },
                "required": ["id", "body"]
            }))
            .expect("schema should derive entity surface spec"),
        )
    }

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn entity_insert_batch(include_version: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("enabled", DataType::Boolean, true),
            Field::new("meta", DataType::Utf8, true),
            Field::new("rating", DataType::Float64, true),
            Field::new("lixcol_entity_id", DataType::Utf8, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_untracked", DataType::Boolean, false),
        ];
        let mut columns = vec![
            string_column(vec![Some("hello")]),
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            string_column(vec![Some("{\"x\":1}")]),
            Arc::new(Float64Array::from(vec![4.5])) as ArrayRef,
            string_column(vec![Some("entity-1")]),
            string_column(vec![Some("{\"source\":\"entity\"}")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ];
        if include_version {
            fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("version-a")]));
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("entity insert batch should build")
    }

    fn primary_key_entity_insert_batch(include_entity_id: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("lixcol_version_id", DataType::Utf8, false),
        ];
        let mut columns = vec![
            string_column(vec![Some("message-1")]),
            string_column(vec![Some("hello")]),
            string_column(vec![Some("version-a")]),
        ];
        if include_entity_id {
            fields.push(Field::new("lixcol_entity_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("message-1")]));
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("primary-key entity insert batch should build")
    }

    #[test]
    fn excludes_non_entity_builtin_session_surfaces() {
        assert!(!schema_exposed_as_entity_surface("lix_active_version"));
        assert!(!schema_exposed_as_entity_surface("lix_active_account"));
        assert!(schema_exposed_as_entity_surface("project_message"));
    }

    #[test]
    fn derives_entity_surface_spec_from_schema_definition() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-version": "1",
            "type": "object",
            "properties": {
                "body": { "type": "string" },
                "rating": { "type": "number" },
                "meta": { "type": "object" },
                "lixcol_entity_id": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(spec.schema_version.as_deref(), Some("1"));
        assert_eq!(
            spec.visible_columns,
            vec!["body".to_string(), "meta".to_string(), "rating".to_string()]
        );
        assert_eq!(
            spec.column_types.get("body"),
            Some(&EntityColumnType::String)
        );
        assert_eq!(
            spec.column_types.get("rating"),
            Some(&EntityColumnType::Number)
        );
        assert_eq!(spec.column_types.get("meta"), Some(&EntityColumnType::Json));
        assert!(!spec.column_types.contains_key("lixcol_entity_id"));
    }

    #[test]
    fn by_version_schema_includes_version_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntityProviderVariant::ByVersion);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_id").is_ok());
        assert!(schema.field_with_name("lixcol_version_id").is_ok());
    }

    #[test]
    fn active_schema_excludes_version_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntityProviderVariant::Active);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_id").is_ok());
        assert!(schema.field_with_name("lixcol_version_id").is_err());
    }

    #[test]
    fn insert_schema_allows_defaulted_identity_columns_to_be_omitted() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntityProviderVariant::Active);
        assert!(
            schema
                .field_with_name("id")
                .expect("id field")
                .is_nullable(),
            "defaulted primary-key property should be nullable at SQL input"
        );
        assert!(
            schema
                .field_with_name("lixcol_entity_id")
                .expect("entity id field")
                .is_nullable(),
            "opaque identity projection should be nullable for normal primary-key inserts"
        );
    }

    #[test]
    fn record_batch_projects_payload_and_system_columns() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "rating": { "type": "number" },
                    "count": { "type": "integer" },
                    "enabled": { "type": "boolean" },
                    "meta": { "type": "object" }
                }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let schema = entity_surface_schema(&spec, EntityProviderVariant::ByVersion);

        let batch =
            entity_record_batch(&spec, schema, &[live_row()]).expect("entity batch should build");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("body is string")
                .value(0),
            "hello"
        );
        assert_eq!(
            batch
                .column_by_name("rating")
                .expect("rating column")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("rating is f64")
                .value(0),
            4.5
        );
        assert_eq!(
            batch
                .column_by_name("count")
                .expect("count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is i64")
                .value(0),
            7
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_entity_id")
                .expect("entity id column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("entity id is string")
                .value(0),
            "entity-1"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_version_id")
                .expect("version id column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("version id is string")
                .value(0),
            "version-a"
        );
    }

    #[tokio::test]
    async fn provider_registers_as_table_provider() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" }
                }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let provider = super::EntityProvider::by_version(
            spec,
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
            empty_version_ref(),
        );

        assert!(provider.schema.field_with_name("lixcol_version_id").is_ok());
    }

    #[test]
    fn decodes_by_version_entity_insert_into_lix_state_write_row() {
        let spec = entity_insert_spec();
        let rows =
            entity_lix_state_write_rows_from_batch(&spec, &entity_insert_batch(true, false), None)
                .expect("entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("entity-1"))
        );
        assert_eq!(rows[0].schema_key, "project_message");
        assert_eq!(rows[0].schema_version.as_str(), "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(rows[0].metadata.as_deref(), Some("{\"source\":\"entity\"}"));
        assert!(!rows[0].global);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                rows[0]
                    .snapshot_content
                    .as_deref()
                    .expect("snapshot_content")
            )
            .expect("snapshot_content JSON"),
            json!({
                "body": "hello",
                "count": 7,
                "enabled": true,
                "meta": {"x": 1},
                "rating": 4.5
            })
        );
    }

    #[test]
    fn primary_key_entity_insert_stages_partial_row_for_normalization() {
        let spec = entity_insert_spec_with_primary_key();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &primary_key_entity_insert_batch(false),
            None,
        )
        .expect("entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, None);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                rows[0]
                    .snapshot_content
                    .as_deref()
                    .expect("snapshot_content")
            )
            .expect("snapshot_content JSON"),
            json!({
                "body": "hello",
                "id": "message-1"
            })
        );
    }

    #[test]
    fn primary_key_entity_insert_preserves_explicit_opaque_projection_for_normalization() {
        let spec = entity_insert_spec_with_primary_key();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &primary_key_entity_insert_batch(true),
            None,
        )
        .expect("primary-key entity insert should stage explicit lixcol_entity_id");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("message-1"))
        );
    }

    #[test]
    fn active_entity_insert_defaults_version_id() {
        let spec = entity_insert_spec();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(false, false),
            Some("version-active"),
        )
        .expect("active entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-active");
        assert!(!rows[0].global);
    }

    #[test]
    fn by_version_entity_insert_requires_version_id_for_non_global_rows() {
        let spec = entity_insert_spec();
        let error =
            entity_lix_state_write_rows_from_batch(&spec, &entity_insert_batch(false, false), None)
                .expect_err("by-version entity insert should require version id");

        assert!(
            error.to_string().contains("requires lixcol_version_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn by_version_entity_insert_global_row_uses_global_version() {
        let spec = entity_insert_spec();
        let rows =
            entity_lix_state_write_rows_from_batch(&spec, &entity_insert_batch(false, true), None)
                .expect("global entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert!(rows[0].global);
        assert_eq!(rows[0].version_id, crate::GLOBAL_VERSION_ID);
    }

    #[tokio::test]
    async fn entity_insert_sink_stages_decoded_lix_state_rows() {
        let spec = entity_insert_spec();
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = entity_insert_batch(true, false);
        let sink = EntityInsertSink::new(
            Arc::clone(&spec),
            batch.schema(),
            write_ctx,
            super::VersionBinding::explicit(),
        );
        let stream = stream::iter(vec![Ok(batch)]);
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(sink.schema().clone(), stream));

        let count = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("entity sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            write_context.writes.as_slice(),
            &[StageWrite::Rows { mode: StageWriteMode::Insert, rows: vec![StageRow {
                    entity_id: Some(crate::entity_identity::EntityIdentity::single("entity-1")),
                    schema_key: "project_message".to_string(),
                    file_id: None,
                    snapshot_content: Some(
                        "{\"body\":\"hello\",\"count\":7,\"enabled\":true,\"meta\":{\"x\":1},\"rating\":4.5}"
                            .to_string()
                    ),
                    metadata: Some("{\"source\":\"entity\"}".to_string()),
                    schema_version: "1".to_string(),
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
