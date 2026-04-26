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
use datafusion::datasource::ViewTable;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr_fn::{col, try_cast};
use datafusion::logical_expr::{lit, Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{create_physical_expr, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::Value as JsonValue;

use crate::common::{derive_entity_id_from_json_paths, EntityIdDerivationError};
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{
    LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateScanRequest,
};
use crate::sql2::StateWriteRow;
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

use super::execute::{SqlWriteIntent, SqlWriteStager};
use super::udf::{
    lix_json_extract_boolean_expr, lix_json_extract_json_expr, lix_json_extract_text_expr,
};

pub(crate) async fn register_entity_providers(
    ctx: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    history_available: bool,
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
                write_stager.as_ref().map(Arc::clone),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        ctx.register_table(
            &spec.schema_key,
            Arc::new(EntityProvider::active(
                Arc::clone(&spec),
                Arc::clone(&live_state),
                write_stager.as_ref().map(Arc::clone),
                active_version_id.to_string(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        if history_available {
            let history_name = format!("{}_history", spec.schema_key);
            ctx.register_table(
                &history_name,
                entity_history_view_provider(ctx, &spec).await?,
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
    }

    Ok(())
}

async fn entity_history_view_provider(
    ctx: &SessionContext,
    spec: &EntitySurfaceSpec,
) -> Result<Arc<dyn TableProvider>, LixError> {
    let dataframe = ctx
        .sql("SELECT * FROM lix_state_history")
        .await
        .map_err(datafusion_error_to_lix_error)?
        .filter(col("schema_key").eq(lit(spec.schema_key.clone())))
        .map_err(datafusion_error_to_lix_error)?
        .select(entity_history_projection_exprs(spec).map_err(datafusion_error_to_lix_error)?)
        .map_err(datafusion_error_to_lix_error)?;

    Ok(Arc::new(ViewTable::new(
        dataframe.into_unoptimized_plan(),
        None,
    )))
}

fn entity_history_projection_exprs(spec: &EntitySurfaceSpec) -> Result<Vec<Expr>> {
    let mut projections = spec
        .visible_columns
        .iter()
        .map(|column_name| {
            let column_type = spec.column_types.get(column_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "sql2 entity provider '{}' does not expose column '{}'",
                    spec.schema_key, column_name
                ))
            })?;
            Ok(
                entity_history_payload_projection_expr(column_name, *column_type)
                    .alias(column_name.clone()),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    projections.extend(
        entity_system_fields(EntityProviderVariant::History)
            .into_iter()
            .filter_map(|field| {
                let public_name = field.name().strip_prefix("lixcol_")?;
                Some(col(public_name).alias(field.name().clone()))
            }),
    );
    Ok(projections)
}

fn entity_history_payload_projection_expr(
    property_name: &str,
    column_type: EntityColumnType,
) -> Expr {
    let snapshot_content = col("snapshot_content");
    match column_type {
        EntityColumnType::String => lix_json_extract_text_expr(snapshot_content, property_name),
        EntityColumnType::Json => lix_json_extract_json_expr(snapshot_content, property_name),
        EntityColumnType::Boolean => lix_json_extract_boolean_expr(snapshot_content, property_name),
        EntityColumnType::Integer => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Int64,
        ),
        EntityColumnType::Number => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Float64,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityProviderVariant {
    Active,
    ByVersion,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityColumnType {
    String,
    Json,
    Integer,
    Number,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntitySurfaceSpec {
    schema_key: String,
    schema_version: Option<String>,
    primary_key_paths: Vec<Vec<String>>,
    visible_columns: Vec<String>,
    column_types: BTreeMap<String, EntityColumnType>,
}

pub(crate) struct EntityProvider {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Option<Arc<dyn SqlWriteStager>>,
    schema: SchemaRef,
    variant: EntityProviderVariant,
    active_version_id: Option<String>,
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
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
        active_version_id: String,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::Active),
            spec,
            live_state,
            write_stager,
            variant: EntityProviderVariant::Active,
            active_version_id: Some(active_version_id),
        }
    }

    fn by_version(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateContext>,
        write_stager: Option<Arc<dyn SqlWriteStager>>,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::ByVersion),
            spec,
            live_state,
            write_stager,
            variant: EntityProviderVariant::ByVersion,
            active_version_id: None,
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
        let request = entity_live_state_scan_request(
            &self.spec.schema_key,
            self.active_version_id.as_deref(),
            limit,
        );

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

        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(format!(
                "INSERT into {} entity surface requires a write transaction",
                self.spec.schema_key
            )));
        };

        let insert_default_version = match self.variant {
            EntityProviderVariant::Active => self.active_version_id.clone(),
            EntityProviderVariant::ByVersion => None,
            EntityProviderVariant::History => {
                return not_impl_err!("INSERT is not implemented for entity history surfaces");
            }
        };

        let sink = EntityInsertSink::new(
            Arc::clone(&self.spec),
            input.schema(),
            Arc::clone(write_stager),
            insert_default_version,
        );
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(write_stager) = &self.write_stager else {
            return Err(DataFusionError::Execution(format!(
                "DELETE FROM {} entity surface requires a write transaction",
                self.spec.schema_key
            )));
        };

        let default_version_id = match self.variant {
            EntityProviderVariant::Active => self.active_version_id.clone(),
            EntityProviderVariant::ByVersion => None,
            EntityProviderVariant::History => {
                return not_impl_err!("DELETE is not implemented for entity history surfaces");
            }
        };

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let physical_filters = filters
            .iter()
            .map(|expr| create_physical_expr(expr, &df_schema, state.execution_props()))
            .collect::<Result<Vec<_>>>()?;
        let request = entity_live_state_scan_request(
            &self.spec.schema_key,
            default_version_id.as_deref(),
            None,
        );

        Ok(Arc::new(EntityDeleteExec::new(
            Arc::clone(&self.spec),
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            default_version_id,
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
            return Err(DataFusionError::Execution(format!(
                "UPDATE {} entity surface requires a write transaction",
                self.spec.schema_key
            )));
        };

        validate_entity_update_assignments(&self.spec, &self.schema, &assignments)?;

        let default_version_id = match self.variant {
            EntityProviderVariant::Active => self.active_version_id.clone(),
            EntityProviderVariant::ByVersion => None,
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
            default_version_id.as_deref(),
            None,
        );

        Ok(Arc::new(EntityUpdateExec::new(
            Arc::clone(&self.spec),
            Arc::clone(&self.live_state),
            Arc::clone(write_stager),
            Arc::clone(&self.schema),
            default_version_id,
            request,
            physical_assignments,
            physical_filters,
        )))
    }
}

struct EntityInsertSink {
    spec: Arc<EntitySurfaceSpec>,
    schema: SchemaRef,
    write_stager: Arc<dyn SqlWriteStager>,
    default_version_id: Option<String>,
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
        write_stager: Arc<dyn SqlWriteStager>,
        default_version_id: Option<String>,
    ) -> Self {
        Self {
            spec,
            schema,
            write_stager,
            default_version_id,
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
                self.default_version_id.as_deref(),
            )?);
        }
        let count = u64::try_from(rows.len())
            .map_err(|_| DataFusionError::Execution("entity INSERT row count overflow".into()))?;

        self.write_stager
            .stage_write(SqlWriteIntent::WriteRows { rows })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

struct EntityDeleteExec {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateContext>,
    write_stager: Arc<dyn SqlWriteStager>,
    table_schema: SchemaRef,
    default_version_id: Option<String>,
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
            spec,
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
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let mut write_rows = entity_existing_lix_state_write_rows_from_batch(
                &spec,
                &matched_batch,
                default_version_id.as_deref(),
            )?;
            for row in &mut write_rows {
                row.snapshot_content = None;
            }
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity DELETE row count overflow".to_string())
            })?;

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

struct EntityUpdateExec {
    spec: Arc<EntitySurfaceSpec>,
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
            spec,
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
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let updated_batch =
                apply_entity_update_assignments(&table_schema, matched_batch, &assignments)?;
            let write_rows = entity_existing_lix_state_write_rows_from_batch(
                &spec,
                &updated_batch,
                default_version_id.as_deref(),
            )?;
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity UPDATE row count overflow".to_string())
            })?;

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

fn apply_entity_update_assignments(
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
                "UPDATE entity surface source batch is missing column '{column_name}'"
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

fn entity_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<StateWriteRow>> {
    entity_lix_state_write_rows_from_batch_with_options(spec, batch, default_version_id, true)
}

fn entity_existing_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    default_version_id: Option<&str>,
) -> Result<Vec<StateWriteRow>> {
    entity_lix_state_write_rows_from_batch_with_options(spec, batch, default_version_id, false)
}

fn entity_lix_state_write_rows_from_batch_with_options(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    default_version_id: Option<&str>,
    reject_read_only_fields: bool,
) -> Result<Vec<StateWriteRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let explicit_global = optional_bool_value(batch, row_index, "lixcol_global")?;
            let version_id = if explicit_global == Some(true) {
                GLOBAL_VERSION_ID.to_string()
            } else {
                optional_string_value(batch, row_index, "lixcol_version_id")?
                    .or_else(|| default_version_id.map(ToOwned::to_owned))
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
            let snapshot = serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to decode entity surface '{}' snapshot_content for entity id derivation: {error}",
                    spec.schema_key
                ))
            })?;
            let entity_id = match optional_string_value(batch, row_index, "lixcol_entity_id")? {
                Some(entity_id) => entity_id,
                None => {
                    derive_entity_id_from_snapshot(spec, &snapshot)
                        .map_err(DataFusionError::Execution)?
                }
            };

            Ok(StateWriteRow {
                entity_id,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                plugin_key: optional_string_value(batch, row_index, "lixcol_plugin_key")?,
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
        object.insert(
            column_name.clone(),
            entity_json_value_from_scalar(value, *column_type)?,
        );
    }
    serde_json::to_string(&JsonValue::Object(object)).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to serialize entity surface '{}' snapshot_content: {error}",
            spec.schema_key
        ))
    })
}

fn derive_entity_id_from_snapshot(
    spec: &EntitySurfaceSpec,
    snapshot: &JsonValue,
) -> std::result::Result<String, String> {
    if spec.primary_key_paths.is_empty() {
        return Err(format!(
            "INSERT into entity surface '{}' requires lixcol_entity_id because the schema has no x-lix-primary-key",
            spec.schema_key
        ));
    }

    derive_entity_id_from_json_paths(snapshot, &spec.primary_key_paths)
        .map(|entity_id| entity_id.into_inner())
        .map_err(|error| entity_id_derivation_error_message(spec, error))
}

fn entity_id_derivation_error_message(
    spec: &EntitySurfaceSpec,
    error: EntityIdDerivationError,
) -> String {
    match error {
        EntityIdDerivationError::EmptyPrimaryKeyPath { index } => format!(
            "INSERT into entity surface '{}' has empty x-lix-primary-key pointer at index {index}",
            spec.schema_key
        ),
        EntityIdDerivationError::MissingPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&spec.primary_key_paths[index]);
            format!(
                "INSERT into entity surface '{}' requires value at primary-key pointer '{pointer}'",
                spec.schema_key
            )
        }
        EntityIdDerivationError::NullPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&spec.primary_key_paths[index]);
            format!(
                "INSERT into entity surface '{}' requires non-null value at primary-key pointer '{pointer}'",
                spec.schema_key
            )
        }
        EntityIdDerivationError::EmptyPrimaryKeyValue { index } => {
            let pointer = format_json_pointer(&spec.primary_key_paths[index]);
            format!(
                "INSERT into entity surface '{}' requires non-empty value at primary-key pointer '{pointer}'",
                spec.schema_key
            )
        }
    }
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
    live_state: Arc<dyn LiveStateContext>,
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
        "entity_id" => string_array(rows.iter().map(|row| Some(row.entity_id.as_str()))),
        "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
        "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
        "plugin_key" => string_array(rows.iter().map(|row| row.plugin_key.as_deref())),
        "snapshot_content" => string_array(rows.iter().map(|row| row.snapshot_content.as_deref())),
        "metadata" => string_array(rows.iter().map(|row| row.metadata.as_deref())),
        "schema_version" => string_array(rows.iter().map(|row| Some(row.schema_version.as_str()))),
        "created_at" => string_array(rows.iter().map(|row| Some(row.created_at.as_str()))),
        "updated_at" => string_array(rows.iter().map(|row| Some(row.updated_at.as_str()))),
        "global" => Arc::new(BooleanArray::from(
            rows.iter().map(|row| row.global).collect::<Vec<_>>(),
        )) as ArrayRef,
        "change_id" => string_array(rows.iter().map(|row| Some(row.change_id.as_str()))),
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

fn parse_snapshot(snapshot_content: Option<&str>) -> Result<Option<JsonValue>> {
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

fn entity_json_text_value(
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

fn entity_i64_value(value: Option<&JsonValue>) -> Option<i64> {
    match value {
        Some(JsonValue::Number(number)) => number.as_i64(),
        Some(JsonValue::String(value)) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn entity_f64_value(value: Option<&JsonValue>) -> Option<f64> {
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

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

fn entity_surface_schema(spec: &EntitySurfaceSpec, variant: EntityProviderVariant) -> SchemaRef {
    let mut fields = spec
        .visible_columns
        .iter()
        .filter_map(|column_name| {
            let column_type = spec.column_types.get(column_name)?;
            Some(Field::new(
                column_name,
                arrow_data_type_for_entity_column_type(*column_type),
                true,
            ))
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

fn entity_system_fields(variant: EntityProviderVariant) -> Vec<Field> {
    if variant == EntityProviderVariant::History {
        return vec![
            Field::new("lixcol_entity_id", DataType::Utf8, false),
            Field::new("lixcol_schema_key", DataType::Utf8, false),
            Field::new("lixcol_file_id", DataType::Utf8, true),
            Field::new("lixcol_plugin_key", DataType::Utf8, true),
            Field::new("lixcol_snapshot_content", DataType::Utf8, true),
            Field::new("lixcol_metadata", DataType::Utf8, true),
            Field::new("lixcol_schema_version", DataType::Utf8, false),
            Field::new("lixcol_change_id", DataType::Utf8, false),
            Field::new("lixcol_commit_id", DataType::Utf8, false),
            Field::new("lixcol_commit_created_at", DataType::Utf8, false),
            Field::new("lixcol_root_commit_id", DataType::Utf8, false),
            Field::new("lixcol_depth", DataType::Int64, false),
            Field::new("lixcol_version_id", DataType::Utf8, false),
        ];
    }

    let mut fields = vec![
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_snapshot_content", DataType::Utf8, true),
        Field::new("lixcol_metadata", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
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
    let primary_key_paths = parse_primary_key_paths(schema)?;

    Ok(EntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        schema_version,
        primary_key_paths,
        visible_columns,
        column_types,
    })
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

fn format_json_pointer(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }
    let encoded = segments
        .iter()
        .map(|segment| segment.replace('~', "~0").replace('/', "~1"))
        .collect::<Vec<_>>()
        .join("/");
    format!("/{encoded}")
}

fn schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_version" | "lix_active_account")
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
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 entity provider error: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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
    use crate::engine2::live_state::{
        LiveStateContext, LiveStateRow, LiveStateRowRequest, LiveStateScanRequest,
    };
    use crate::sql2::{SqlWriteIntent, SqlWriteOutcome, SqlWriteStager, StateWriteRow};
    use crate::LixError;

    struct EmptyLiveStateContext;
    #[derive(Default)]
    struct CapturingWriteStager {
        writes: Mutex<Vec<SqlWriteIntent>>,
    }

    #[async_trait]
    impl LiveStateContext for EmptyLiveStateContext {
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
    impl SqlWriteStager for CapturingWriteStager {
        async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
            self.writes.lock().expect("writes lock").push(write);
            Ok(SqlWriteOutcome { count: 0 })
        }
    }

    fn live_row() -> LiveStateRow {
        LiveStateRow {
            entity_id: "entity-1".to_string(),
            schema_key: "project_message".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                "{\"body\":\"hello\",\"rating\":4.5,\"count\":7,\"enabled\":true,\"meta\":{\"x\":1}}"
                    .to_string(),
            ),
            metadata: Some("{\"source\":\"test\"}".to_string()),
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: "change-a".to_string(),
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
            Arc::new(EmptyLiveStateContext) as Arc<dyn LiveStateContext>,
            None,
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
        assert_eq!(rows[0].entity_id, "entity-1");
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
        assert_eq!(rows[0].version_id, crate::version::GLOBAL_VERSION_ID);
    }

    #[tokio::test]
    async fn entity_insert_sink_stages_decoded_lix_state_rows() {
        let spec = entity_insert_spec();
        let stager = Arc::new(CapturingWriteStager::default());
        let batch = entity_insert_batch(true, false);
        let sink = EntityInsertSink::new(
            Arc::clone(&spec),
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
            .expect("entity sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            stager.writes.lock().expect("writes lock").as_slice(),
            &[SqlWriteIntent::WriteRows {
                rows: vec![StateWriteRow {
                    entity_id: "entity-1".to_string(),
                    schema_key: "project_message".to_string(),
                    file_id: None,
                    plugin_key: None,
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
