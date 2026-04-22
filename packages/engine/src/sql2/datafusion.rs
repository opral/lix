use std::any::Any;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::TableType, physical_plan::SendableRecordBatchStream};
use futures_util::{stream, TryStreamExt};
use tokio::sync::oneshot;

use crate::catalog::{
    open_change_surface_snapshot, open_change_surface_snapshot_with_shared_backend,
    open_directory_by_version_surface_snapshot,
    open_directory_by_version_surface_snapshot_with_shared_backend,
    open_directory_surface_snapshot, open_file_by_version_surface_snapshot,
    open_file_by_version_surface_snapshot_with_shared_backend, open_file_surface_snapshot,
    open_version_surface_snapshot, open_version_surface_snapshot_with_shared_backend,
    ChangeSurfaceColumn, ChangeSurfaceFilter, ChangeSurfaceRow, ChangeSurfaceScanRequest,
    ChangeSurfaceSnapshot, DirectorySurfaceColumn, DirectorySurfaceFilter, DirectorySurfaceRow,
    DirectorySurfaceScanRequest, DirectorySurfaceSnapshot, FileSurfaceColumn, FileSurfaceFilter,
    FileSurfaceRow, FileSurfaceScanRequest, FileSurfaceSnapshot, SurfaceColumnType, SurfaceFamily,
    SurfaceRegistry, SurfaceVariant, VersionSurfaceColumn, VersionSurfaceRow,
    VersionSurfaceScanRequest, VersionSurfaceSnapshot,
};
use crate::live_state::{
    open_state_by_version_snapshot, open_state_by_version_snapshot_with_shared_backend,
    StateByVersionScanRequest, StateByVersionSnapshot, StateSurfaceColumn, StateSurfaceFilter,
};
use crate::sql::diagnostics::sql_unknown_column_error;
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedSql2ReadArtifact {
    pub(crate) sql: String,
    pub(crate) bound_parameters: Vec<Value>,
    pub(crate) active_version_id: String,
    pub(crate) surface_names: Vec<String>,
    pub(crate) entity_surfaces: BTreeMap<String, PreparedSql2EntitySurfaceSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2EntitySurfaceSpec {
    pub(crate) public_name: String,
    pub(crate) schema_key: String,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) column_order: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
}

pub(crate) fn prepared_entity_surface_specs_for_registry(
    registry: &SurfaceRegistry,
    surface_names: &[String],
) -> BTreeMap<String, PreparedSql2EntitySurfaceSpec> {
    surface_names
        .iter()
        .filter_map(|surface_name| {
            let resolved = registry.bind_relation_name(surface_name)?;
            let schema_key = resolved.implicit_overrides.fixed_schema_key.clone()?;
            (resolved.descriptor.surface_family == SurfaceFamily::Entity).then(|| {
                (
                    surface_name.clone(),
                    PreparedSql2EntitySurfaceSpec {
                        public_name: resolved.descriptor.public_name.clone(),
                        schema_key,
                        surface_variant: resolved.descriptor.surface_variant,
                        column_order: resolved
                            .descriptor
                            .visible_columns
                            .iter()
                            .chain(resolved.descriptor.hidden_columns.iter())
                            .cloned()
                            .collect(),
                        column_types: resolved.column_types,
                    },
                )
            })
        })
        .collect()
}

pub(crate) async fn execute_read_with_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_borrowed_backend(backend, artifact).await?;
    collect_query_result_from_ctx(ctx, artifact).await
}

pub(crate) async fn execute_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let ctx = build_session_for_read_with_shared_backend(backend, artifact).await?;
    collect_query_result_from_ctx(ctx, artifact).await
}

async fn collect_query_result_from_ctx(
    ctx: SessionContext,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<QueryResult, LixError> {
    let mut dataframe = ctx
        .sql(&artifact.sql)
        .await
        .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    if !artifact.bound_parameters.is_empty() {
        dataframe = dataframe
            .with_param_values(
                artifact
                    .bound_parameters
                    .iter()
                    .map(scalar_value_from_lix_value)
                    .collect::<Vec<_>>(),
            )
            .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    }
    let result_columns = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let batches = dataframe
        .collect()
        .await
        .map_err(|error| datafusion_error_to_lix_error_with_artifact(error, artifact))?;
    query_result_from_batches(&result_columns, &batches)
}

async fn build_session_for_read_with_borrowed_backend(
    backend: &dyn LixBackend,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                let snapshot =
                    open_state_by_version_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_by_version" => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 phase-3 lix_state_by_version currently requires a shared backend execution host",
                ));
            }
            "lix_file" => {
                let snapshot =
                    open_file_surface_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixFileProvider::new(
                        LixFileSurfaceKind::File,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_file_by_version" => {
                let snapshot = open_file_by_version_surface_snapshot(backend).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixFileProvider::new(
                        LixFileSurfaceKind::FileByVersion,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_directory" => {
                let snapshot =
                    open_directory_surface_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixDirectoryProvider::new(
                        LixDirectorySurfaceKind::Directory,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_directory_by_version" => {
                let snapshot = open_directory_by_version_surface_snapshot(backend).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(LixDirectoryProvider::new(
                        LixDirectorySurfaceKind::DirectoryByVersion,
                        artifact.active_version_id.clone(),
                        snapshot,
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_version" => {
                let snapshot = open_version_surface_snapshot(backend).await?;
                ctx.register_table(surface_name, Arc::new(LixVersionProvider::new(snapshot)))
                    .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_change" => {
                let snapshot = open_change_surface_snapshot(backend).await?;
                ctx.register_table(surface_name, Arc::new(LixChangeProvider::new(snapshot)))
                    .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                let Some(spec) = artifact.entity_surfaces.get(other) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("sql2 phase-2 does not support surface '{other}' yet"),
                    ));
                };
                if spec.surface_variant == SurfaceVariant::ByVersion {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "sql2 entity by-version surface '{}' currently requires a shared backend execution host",
                            spec.public_name
                        ),
                    ));
                }
                let snapshot =
                    open_state_by_version_snapshot(backend, &artifact.active_version_id).await?;
                ctx.register_table(
                    surface_name,
                    Arc::new(
                        LixEntityProvider::new(
                            spec.clone(),
                            artifact.active_version_id.clone(),
                            snapshot,
                        )
                        .map_err(datafusion_error_to_lix_error)?,
                    ),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
        }
    }
    Ok(ctx)
}

async fn build_session_for_read_with_shared_backend(
    backend: Arc<dyn LixBackend + Send + Sync>,
    artifact: &PreparedSql2ReadArtifact,
) -> Result<SessionContext, LixError> {
    let ctx = SessionContext::new();
    let shared_state_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| matches!(surface.as_str(), "lix_state" | "lix_state_by_version"))
        || !artifact.entity_surfaces.is_empty()
    {
        Some(open_state_by_version_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_file_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| matches!(surface.as_str(), "lix_file" | "lix_file_by_version"))
    {
        Some(open_file_by_version_surface_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_directory_snapshot = if artifact.surface_names.iter().any(|surface| {
        matches!(
            surface.as_str(),
            "lix_directory" | "lix_directory_by_version"
        )
    }) {
        Some(
            open_directory_by_version_surface_snapshot_with_shared_backend(Arc::clone(&backend))
                .await?,
        )
    } else {
        None
    };
    let shared_version_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| surface.as_str() == "lix_version")
    {
        Some(open_version_surface_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    let shared_change_snapshot = if artifact
        .surface_names
        .iter()
        .any(|surface| surface.as_str() == "lix_change")
    {
        Some(open_change_surface_snapshot_with_shared_backend(Arc::clone(&backend)).await?)
    } else {
        None
    };
    for surface_name in &artifact.surface_names {
        match surface_name.as_str() {
            "lix_state" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::State,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_state_snapshot
                                .as_ref()
                                .expect("state surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_state_by_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixStateProvider::new(
                        LixStateSurfaceKind::StateByVersion,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_state_snapshot
                                .as_ref()
                                .expect("state surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_file" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixFileProvider::new(
                        LixFileSurfaceKind::File,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_file_snapshot
                                .as_ref()
                                .expect("file surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_file_by_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixFileProvider::new(
                        LixFileSurfaceKind::FileByVersion,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_file_snapshot
                                .as_ref()
                                .expect("file-by-version surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_directory" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixDirectoryProvider::new(
                        LixDirectorySurfaceKind::Directory,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_directory_snapshot
                                .as_ref()
                                .expect("directory surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_directory_by_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixDirectoryProvider::new(
                        LixDirectorySurfaceKind::DirectoryByVersion,
                        artifact.active_version_id.clone(),
                        Arc::clone(
                            shared_directory_snapshot
                                .as_ref()
                                .expect("directory surface snapshot should exist"),
                        ),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_version" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixVersionProvider::new(Arc::clone(
                        shared_version_snapshot
                            .as_ref()
                            .expect("version surface snapshot should exist"),
                    ))),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            "lix_change" => {
                ctx.register_table(
                    surface_name,
                    Arc::new(LixChangeProvider::new(Arc::clone(
                        shared_change_snapshot
                            .as_ref()
                            .expect("change surface snapshot should exist"),
                    ))),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            other => {
                let Some(spec) = artifact.entity_surfaces.get(other) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("sql2 phase-2 does not support surface '{other}' yet"),
                    ));
                };
                ctx.register_table(
                    surface_name,
                    Arc::new(
                        LixEntityProvider::new(
                            spec.clone(),
                            artifact.active_version_id.clone(),
                            Arc::clone(
                                shared_state_snapshot
                                    .as_ref()
                                    .expect("state snapshot should exist for entity surfaces"),
                            ),
                        )
                        .map_err(datafusion_error_to_lix_error)?,
                    ),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
        }
    }
    Ok(ctx)
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn datafusion_error_to_lix_error_with_artifact(
    error: DataFusionError,
    artifact: &PreparedSql2ReadArtifact,
) -> LixError {
    let error_text = error.to_string();
    if let Some(column_name) = parse_datafusion_unknown_column_name(&error_text) {
        let table_name = artifact.surface_names.first().map(String::as_str);
        let available_columns = artifact
            .surface_names
            .first()
            .and_then(|surface_name| artifact.entity_surfaces.get(surface_name))
            .map(|spec| {
                spec.column_order
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        return sql_unknown_column_error(&column_name, table_name, &available_columns, None);
    }

    datafusion_error_to_lix_error(error)
}

fn parse_datafusion_unknown_column_name(message: &str) -> Option<String> {
    for needle in [
        "No field named ",
        "field named ",
        "Column '",
        "column '",
        "column `",
    ] {
        let Some(start) = message.find(needle) else {
            continue;
        };
        let rest = &message[start + needle.len()..];
        let candidate = rest
            .trim_start_matches(['`', '\'', '"'])
            .split(|ch: char| {
                ch == '`'
                    || ch == '\''
                    || ch == '"'
                    || ch == '.'
                    || ch == ','
                    || ch == ' '
                    || ch == '\n'
                    || ch == '\r'
            })
            .next()
            .unwrap_or_default()
            .trim();
        if !candidate.is_empty() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)),
        Value::Real(value) => ScalarValue::Float64(Some(*value)),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    }
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::Execution(format!("sql2 live_state error: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixStateSurfaceKind {
    State,
    StateByVersion,
}

impl LixStateSurfaceKind {
    fn schema(self) -> SchemaRef {
        match self {
            Self::State => Arc::new(Schema::new(vec![
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
            ])),
            Self::StateByVersion => Arc::new(Schema::new(vec![
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
            ])),
        }
    }
}

#[derive(Debug, Clone)]
struct LixStateProvider {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn StateByVersionSnapshot>,
}

impl LixStateProvider {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: surface_kind.schema(),
            snapshot,
        }
    }
}

#[derive(Debug, Clone)]
struct LixEntityProvider {
    spec: PreparedSql2EntitySurfaceSpec,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn StateByVersionSnapshot>,
}

impl LixEntityProvider {
    fn new(
        spec: PreparedSql2EntitySurfaceSpec,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
    ) -> Result<Self> {
        let schema = entity_surface_schema(&spec, &spec.column_order)?;
        Ok(Self {
            spec,
            default_version_id,
            schema,
            snapshot,
        })
    }
}

#[async_trait]
impl TableProvider for LixEntityProvider {
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
                if parse_entity_route_filter(filter, self.spec.surface_variant).is_some() {
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
        let projected_columns = projected_entity_columns(&self.spec, projection);
        let projected_schema = entity_surface_schema(&self.spec, &projected_columns)?;
        let route = LixEntityRoute::from_filters(filters, self.spec.surface_variant);
        Ok(Arc::new(LixEntityScanExec::new(
            self.spec.clone(),
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projected_columns,
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixEntityScanExec {
    spec: PreparedSql2EntitySurfaceSpec,
    default_version_id: String,
    snapshot: Arc<dyn StateByVersionSnapshot>,
    schema: SchemaRef,
    projected_columns: Vec<String>,
    route: LixEntityRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixEntityScanExec {
    fn new(
        spec: PreparedSql2EntitySurfaceSpec,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
        schema: SchemaRef,
        projected_columns: Vec<String>,
        route: LixEntityRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            spec,
            default_version_id,
            snapshot,
            schema,
            projected_columns,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixEntityScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LixEntityScanExec({})", self.spec.public_name)
    }
}

impl ExecutionPlan for LixEntityScanExec {
    fn name(&self) -> &str {
        "LixEntityScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            return Err(DataFusionError::Internal(
                "LixEntityScanExec does not support children".to_string(),
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
            return Err(DataFusionError::Execution(
                "sql2 entity provider exposes exactly one partition".to_string(),
            ));
        }

        if self.route.contradictory {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::iter(Vec::<Result<RecordBatch>>::new()),
            )));
        }

        let scan_request = entity_surface_scan_request(
            &self.spec,
            &self.default_version_id,
            &self.projected_columns,
            &self.route,
            self.limit,
        )?;
        let snapshot = Arc::clone(&self.snapshot);
        let spec = self.spec.clone();
        let projected_columns = self.projected_columns.clone();
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let state_batches =
                enqueue_state_by_version_scan_batches(snapshot, scan_request).await?;
            let entity_batches = entity_surface_batches_from_state_batches(
                &spec,
                &projected_columns,
                &state_batches,
            )?;
            Ok::<_, DataFusionError>(stream::iter(
                entity_batches
                    .into_iter()
                    .map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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
                if parse_route_filter(filter).is_some() {
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
        let route = LixStateRoute::from_filters(filters);
        Ok(Arc::new(LixStateScanExec::new(
            self.surface_kind,
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixStateScanExec {
    surface_kind: LixStateSurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn StateByVersionSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixStateRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixStateScanExec {
    fn new(
        surface_kind: LixStateSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn StateByVersionSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixStateRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            surface_kind,
            default_version_id,
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixStateScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixStateScanExec(surface={:?}, version_id={}, limit={:?}, route={:?})",
                    self.surface_kind, self.default_version_id, self.limit, self.route
                )
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

        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                enqueue_state_by_version_scan_batches(
                    snapshot,
                    state_by_version_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    )?,
                )
                .await?
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct StateByVersionScanJob {
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<RecordBatch>, LixError>>,
}

fn state_by_version_scan_worker() -> &'static mpsc::Sender<StateByVersionScanJob> {
    static WORKER: OnceLock<mpsc::Sender<StateByVersionScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<StateByVersionScanJob>();
        thread::Builder::new()
            .name("sql2-live-state-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 live-state runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime.block_on(async move {
                        job.snapshot
                            .scan_state_by_version_batches(&job.request)
                            .await
                    });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 live-state worker thread should spawn");
        tx
    })
}

async fn enqueue_state_by_version_scan_batches(
    snapshot: Arc<dyn StateByVersionSnapshot>,
    request: StateByVersionScanRequest,
) -> Result<Vec<RecordBatch>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    state_by_version_scan_worker()
        .send(StateByVersionScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue live_state scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 live_state scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixFileSurfaceKind {
    File,
    FileByVersion,
}

fn lix_file_schema(surface_kind: LixFileSurfaceKind) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("directory_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("extension", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
    ];
    if surface_kind == LixFileSurfaceKind::FileByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields.extend([
        Field::new("lixcol_plugin_key", DataType::Utf8, true),
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

#[derive(Debug, Clone)]
struct LixFileProvider {
    surface_kind: LixFileSurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn FileSurfaceSnapshot>,
}

impl LixFileProvider {
    fn new(
        surface_kind: LixFileSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn FileSurfaceSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: lix_file_schema(surface_kind),
            snapshot,
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
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_file_route_filter(filter).is_some() {
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
        let route = LixFileRoute::from_filters(filters);
        Ok(Arc::new(LixFileScanExec::new(
            self.surface_kind,
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixFileScanExec {
    surface_kind: LixFileSurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixFileRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixFileScanExec {
    fn new(
        surface_kind: LixFileSurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn FileSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixFileRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            surface_kind,
            default_version_id,
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixFileScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixFileScanExec(limit={:?}, route={:?})",
                    self.limit, self.route
                )
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
                "LixFileScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                let rows = enqueue_file_surface_scan(
                    snapshot,
                    file_surface_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    ),
                )
                .await?;
                file_surface_record_batches(
                    file_projection_for_scan(surface_kind, projection.as_ref()),
                    &rows,
                )?
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct FileSurfaceScanJob {
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    request: FileSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<FileSurfaceRow>, LixError>>,
}

fn file_surface_scan_worker() -> &'static mpsc::Sender<FileSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<FileSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<FileSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-file-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 file-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_files(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 file-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_file_surface_scan(
    snapshot: Arc<dyn FileSurfaceSnapshot>,
    request: FileSurfaceScanRequest,
) -> Result<Vec<FileSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    file_surface_scan_worker()
        .send(FileSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue file surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 file surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn file_surface_record_batches(
    projection: Vec<FileSurfaceColumn>,
    rows: &[FileSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![file_surface_record_batch(&projection, rows)?])
}

fn file_surface_record_batch(
    projection: &[FileSurfaceColumn],
    rows: &[FileSurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            FileSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            FileSurfaceColumn::DirectoryId => {
                string_array(rows.iter().map(|row| row.directory_id.as_deref()))
            }
            FileSurfaceColumn::Name => string_array(rows.iter().map(|row| Some(row.name.as_str()))),
            FileSurfaceColumn::Extension => {
                string_array(rows.iter().map(|row| row.extension.as_deref()))
            }
            FileSurfaceColumn::Path => string_array(rows.iter().map(|row| row.path.as_deref())),
            FileSurfaceColumn::Data => binary_array(rows.iter().map(|row| row.data.as_deref())),
            FileSurfaceColumn::Metadata => {
                string_array(rows.iter().map(|row| row.metadata.as_deref()))
            }
            FileSurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolEntityId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
            }
            FileSurfaceColumn::LixcolSchemaKey => {
                string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
            }
            FileSurfaceColumn::LixcolFileId => {
                string_array(rows.iter().map(|row| row.lixcol_file_id.as_deref()))
            }
            FileSurfaceColumn::LixcolVersionId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
            }
            FileSurfaceColumn::LixcolPluginKey => {
                string_array(rows.iter().map(|row| row.lixcol_plugin_key.as_deref()))
            }
            FileSurfaceColumn::LixcolSchemaVersion => string_array(
                rows.iter()
                    .map(|row| Some(row.lixcol_schema_version.as_str())),
            ),
            FileSurfaceColumn::LixcolGlobal => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.lixcol_global).collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolChangeId => {
                string_array(rows.iter().map(|row| row.lixcol_change_id.as_deref()))
            }
            FileSurfaceColumn::LixcolCreatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_created_at.as_deref()))
            }
            FileSurfaceColumn::LixcolUpdatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_updated_at.as_deref()))
            }
            FileSurfaceColumn::LixcolCommitId => {
                string_array(rows.iter().map(|row| row.lixcol_commit_id.as_deref()))
            }
            FileSurfaceColumn::LixcolUntracked => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.lixcol_untracked)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            FileSurfaceColumn::LixcolMetadata => {
                string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(file_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_file batch: {error}"))
    })
}

fn file_surface_schema(projection: &[FileSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                FileSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                FileSurfaceColumn::DirectoryId => Field::new("directory_id", DataType::Utf8, true),
                FileSurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                FileSurfaceColumn::Extension => Field::new("extension", DataType::Utf8, true),
                FileSurfaceColumn::Path => Field::new("path", DataType::Utf8, true),
                FileSurfaceColumn::Data => Field::new("data", DataType::Binary, true),
                FileSurfaceColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                FileSurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                FileSurfaceColumn::LixcolEntityId => {
                    Field::new("lixcol_entity_id", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolSchemaKey => {
                    Field::new("lixcol_schema_key", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolFileId => {
                    Field::new("lixcol_file_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolVersionId => {
                    Field::new("lixcol_version_id", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolPluginKey => {
                    Field::new("lixcol_plugin_key", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolSchemaVersion => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                FileSurfaceColumn::LixcolGlobal => {
                    Field::new("lixcol_global", DataType::Boolean, false)
                }
                FileSurfaceColumn::LixcolChangeId => {
                    Field::new("lixcol_change_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolCreatedAt => {
                    Field::new("lixcol_created_at", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolUpdatedAt => {
                    Field::new("lixcol_updated_at", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolCommitId => {
                    Field::new("lixcol_commit_id", DataType::Utf8, true)
                }
                FileSurfaceColumn::LixcolUntracked => {
                    Field::new("lixcol_untracked", DataType::Boolean, false)
                }
                FileSurfaceColumn::LixcolMetadata => {
                    Field::new("lixcol_metadata", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn file_projection_for_scan(
    surface_kind: LixFileSurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<FileSurfaceColumn> {
    let mut all_columns = vec![
        FileSurfaceColumn::Id,
        FileSurfaceColumn::DirectoryId,
        FileSurfaceColumn::Name,
        FileSurfaceColumn::Extension,
        FileSurfaceColumn::Path,
        FileSurfaceColumn::Data,
        FileSurfaceColumn::Metadata,
        FileSurfaceColumn::Hidden,
        FileSurfaceColumn::LixcolEntityId,
        FileSurfaceColumn::LixcolSchemaKey,
        FileSurfaceColumn::LixcolFileId,
    ];
    if surface_kind == LixFileSurfaceKind::FileByVersion {
        all_columns.push(FileSurfaceColumn::LixcolVersionId);
    }
    all_columns.extend([
        FileSurfaceColumn::LixcolPluginKey,
        FileSurfaceColumn::LixcolSchemaVersion,
        FileSurfaceColumn::LixcolGlobal,
        FileSurfaceColumn::LixcolChangeId,
        FileSurfaceColumn::LixcolCreatedAt,
        FileSurfaceColumn::LixcolUpdatedAt,
        FileSurfaceColumn::LixcolCommitId,
        FileSurfaceColumn::LixcolUntracked,
        FileSurfaceColumn::LixcolMetadata,
    ]);
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LixDirectorySurfaceKind {
    Directory,
    DirectoryByVersion,
}

fn lix_directory_schema(surface_kind: LixDirectorySurfaceKind) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("parent_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, true),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("lixcol_entity_id", DataType::Utf8, false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
    ];
    if surface_kind == LixDirectorySurfaceKind::DirectoryByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields.extend([
        Field::new("lixcol_schema_version", DataType::Utf8, false),
        Field::new("lixcol_global", DataType::Boolean, false),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, false),
        Field::new("lixcol_metadata", DataType::Utf8, true),
    ]);
    Arc::new(Schema::new(fields))
}

#[derive(Debug, Clone)]
struct LixDirectoryProvider {
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: String,
    schema: SchemaRef,
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
}

impl LixDirectoryProvider {
    fn new(
        surface_kind: LixDirectorySurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    ) -> Self {
        Self {
            surface_kind,
            default_version_id,
            schema: lix_directory_schema(surface_kind),
            snapshot,
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
            .map(|filter| {
                if parse_directory_route_filter(filter).is_some() {
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
        let route = LixDirectoryRoute::from_filters(filters);
        Ok(Arc::new(LixDirectoryScanExec::new(
            self.surface_kind,
            self.default_version_id.clone(),
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixDirectoryScanExec {
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: String,
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixDirectoryRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixDirectoryScanExec {
    fn new(
        surface_kind: LixDirectorySurfaceKind,
        default_version_id: String,
        snapshot: Arc<dyn DirectorySurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixDirectoryRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            surface_kind,
            default_version_id,
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixDirectoryScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LixDirectoryScanExec(limit={:?}, route={:?})",
                    self.limit, self.route
                )
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
                "LixDirectoryScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let surface_kind = self.surface_kind;
        let default_version_id = self.default_version_id.clone();
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let batches = if route.contradictory {
                Vec::new()
            } else {
                let rows = enqueue_directory_surface_scan(
                    snapshot,
                    directory_surface_scan_request(
                        surface_kind,
                        &default_version_id,
                        projection.as_ref(),
                        &route,
                        limit,
                    ),
                )
                .await?;
                directory_surface_record_batches(
                    directory_projection_for_scan(surface_kind, projection.as_ref()),
                    &rows,
                )?
            };
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct DirectorySurfaceScanJob {
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    request: DirectorySurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<DirectorySurfaceRow>, LixError>>,
}

fn directory_surface_scan_worker() -> &'static mpsc::Sender<DirectorySurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<DirectorySurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<DirectorySurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-directory-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 directory-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_directories(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 directory-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_directory_surface_scan(
    snapshot: Arc<dyn DirectorySurfaceSnapshot>,
    request: DirectorySurfaceScanRequest,
) -> Result<Vec<DirectorySurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    directory_surface_scan_worker()
        .send(DirectorySurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue directory surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution(
                "sql2 directory surface scan worker dropped reply".to_string(),
            )
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn directory_surface_record_batches(
    projection: Vec<DirectorySurfaceColumn>,
    rows: &[DirectorySurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![directory_surface_record_batch(&projection, rows)?])
}

fn directory_surface_record_batch(
    projection: &[DirectorySurfaceColumn],
    rows: &[DirectorySurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            DirectorySurfaceColumn::Id => {
                string_array(rows.iter().map(|row| Some(row.id.as_str())))
            }
            DirectorySurfaceColumn::ParentId => {
                string_array(rows.iter().map(|row| row.parent_id.as_deref()))
            }
            DirectorySurfaceColumn::Name => {
                string_array(rows.iter().map(|row| Some(row.name.as_str())))
            }
            DirectorySurfaceColumn::Path => {
                string_array(rows.iter().map(|row| row.path.as_deref()))
            }
            DirectorySurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolEntityId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_entity_id.as_str())))
            }
            DirectorySurfaceColumn::LixcolSchemaKey => {
                string_array(rows.iter().map(|row| Some(row.lixcol_schema_key.as_str())))
            }
            DirectorySurfaceColumn::LixcolVersionId => {
                string_array(rows.iter().map(|row| Some(row.lixcol_version_id.as_str())))
            }
            DirectorySurfaceColumn::LixcolSchemaVersion => string_array(
                rows.iter()
                    .map(|row| Some(row.lixcol_schema_version.as_str())),
            ),
            DirectorySurfaceColumn::LixcolGlobal => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.lixcol_global).collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolChangeId => {
                string_array(rows.iter().map(|row| row.lixcol_change_id.as_deref()))
            }
            DirectorySurfaceColumn::LixcolCreatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_created_at.as_deref()))
            }
            DirectorySurfaceColumn::LixcolUpdatedAt => {
                string_array(rows.iter().map(|row| row.lixcol_updated_at.as_deref()))
            }
            DirectorySurfaceColumn::LixcolCommitId => {
                string_array(rows.iter().map(|row| row.lixcol_commit_id.as_deref()))
            }
            DirectorySurfaceColumn::LixcolUntracked => Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.lixcol_untracked)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            DirectorySurfaceColumn::LixcolMetadata => {
                string_array(rows.iter().map(|row| row.lixcol_metadata.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(directory_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_directory batch: {error}"))
    })
}

fn directory_surface_schema(projection: &[DirectorySurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                DirectorySurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                DirectorySurfaceColumn::ParentId => Field::new("parent_id", DataType::Utf8, true),
                DirectorySurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                DirectorySurfaceColumn::Path => Field::new("path", DataType::Utf8, true),
                DirectorySurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                DirectorySurfaceColumn::LixcolEntityId => {
                    Field::new("lixcol_entity_id", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolSchemaKey => {
                    Field::new("lixcol_schema_key", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolVersionId => {
                    Field::new("lixcol_version_id", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolSchemaVersion => {
                    Field::new("lixcol_schema_version", DataType::Utf8, false)
                }
                DirectorySurfaceColumn::LixcolGlobal => {
                    Field::new("lixcol_global", DataType::Boolean, false)
                }
                DirectorySurfaceColumn::LixcolChangeId => {
                    Field::new("lixcol_change_id", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolCreatedAt => {
                    Field::new("lixcol_created_at", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolUpdatedAt => {
                    Field::new("lixcol_updated_at", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolCommitId => {
                    Field::new("lixcol_commit_id", DataType::Utf8, true)
                }
                DirectorySurfaceColumn::LixcolUntracked => {
                    Field::new("lixcol_untracked", DataType::Boolean, false)
                }
                DirectorySurfaceColumn::LixcolMetadata => {
                    Field::new("lixcol_metadata", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn directory_projection_for_scan(
    surface_kind: LixDirectorySurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<DirectorySurfaceColumn> {
    let mut all_columns = vec![
        DirectorySurfaceColumn::Id,
        DirectorySurfaceColumn::ParentId,
        DirectorySurfaceColumn::Name,
        DirectorySurfaceColumn::Path,
        DirectorySurfaceColumn::Hidden,
        DirectorySurfaceColumn::LixcolEntityId,
        DirectorySurfaceColumn::LixcolSchemaKey,
    ];
    if surface_kind == LixDirectorySurfaceKind::DirectoryByVersion {
        all_columns.push(DirectorySurfaceColumn::LixcolVersionId);
    }
    all_columns.extend([
        DirectorySurfaceColumn::LixcolSchemaVersion,
        DirectorySurfaceColumn::LixcolGlobal,
        DirectorySurfaceColumn::LixcolChangeId,
        DirectorySurfaceColumn::LixcolCreatedAt,
        DirectorySurfaceColumn::LixcolUpdatedAt,
        DirectorySurfaceColumn::LixcolCommitId,
        DirectorySurfaceColumn::LixcolUntracked,
        DirectorySurfaceColumn::LixcolMetadata,
    ]);
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn lix_change_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("plugin_key", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("untracked", DataType::Boolean, false),
        Field::new("snapshot_content", DataType::Utf8, true),
    ]))
}

#[derive(Debug, Clone)]
struct LixChangeProvider {
    schema: SchemaRef,
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
}

impl LixChangeProvider {
    fn new(snapshot: Arc<dyn ChangeSurfaceSnapshot>) -> Self {
        Self {
            schema: lix_change_schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixChangeProvider {
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
                if parse_change_route_filter(filter).is_some() {
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
        let route = LixChangeRoute::from_filters(filters);
        Ok(Arc::new(LixChangeScanExec::new(
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
            route,
            limit,
        )))
    }
}

#[derive(Debug)]
struct LixChangeScanExec {
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    route: LixChangeRoute,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LixChangeScanExec {
    fn new(
        snapshot: Arc<dyn ChangeSurfaceSnapshot>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        route: LixChangeRoute,
        limit: Option<usize>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            snapshot,
            schema,
            projection,
            route,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixChangeScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixChangeScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixChangeScanExec"),
        }
    }
}

impl ExecutionPlan for LixChangeScanExec {
    fn name(&self) -> &str {
        "LixChangeScanExec"
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
                "LixChangeScanExec does not accept children".to_string(),
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
                "LixChangeScanExec only exposes one partition, got {partition}"
            )));
        }

        if self.route.contradictory {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::iter(Vec::<Result<RecordBatch>>::new()),
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let route = self.route.clone();
        let limit = self.limit;
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let scan_projection = change_projection_for_scan(projection.as_ref());
            let rows = enqueue_change_surface_scan(
                snapshot,
                ChangeSurfaceScanRequest {
                    projection: scan_projection.clone(),
                    filters: change_filters_for_route(&route),
                    limit,
                },
            )
            .await?;
            let batches = change_surface_record_batches(scan_projection, &rows)?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct ChangeSurfaceScanJob {
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    request: ChangeSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<ChangeSurfaceRow>, LixError>>,
}

fn change_surface_scan_worker() -> &'static mpsc::Sender<ChangeSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<ChangeSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<ChangeSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-change-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 change-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_changes(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 change-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_change_surface_scan(
    snapshot: Arc<dyn ChangeSurfaceSnapshot>,
    request: ChangeSurfaceScanRequest,
) -> Result<Vec<ChangeSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    change_surface_scan_worker()
        .send(ChangeSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue change surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 change surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn change_surface_record_batches(
    projection: Vec<ChangeSurfaceColumn>,
    rows: &[ChangeSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![change_surface_record_batch(&projection, rows)?])
}

fn change_surface_record_batch(
    projection: &[ChangeSurfaceColumn],
    rows: &[ChangeSurfaceRow],
) -> Result<RecordBatch> {
    if projection.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(
            change_surface_schema(projection),
            vec![],
            &options,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to build zero-column lix_change batch: {error}"
            ))
        });
    }

    let arrays = projection
        .iter()
        .map(|column| match column {
            ChangeSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            ChangeSurfaceColumn::EntityId => {
                string_array(rows.iter().map(|row| Some(row.entity_id.as_str())))
            }
            ChangeSurfaceColumn::SchemaKey => {
                string_array(rows.iter().map(|row| Some(row.schema_key.as_str())))
            }
            ChangeSurfaceColumn::SchemaVersion => {
                string_array(rows.iter().map(|row| Some(row.schema_version.as_str())))
            }
            ChangeSurfaceColumn::FileId => {
                string_array(rows.iter().map(|row| row.file_id.as_deref()))
            }
            ChangeSurfaceColumn::PluginKey => {
                string_array(rows.iter().map(|row| row.plugin_key.as_deref()))
            }
            ChangeSurfaceColumn::Metadata => {
                string_array(rows.iter().map(|row| row.metadata.as_deref()))
            }
            ChangeSurfaceColumn::CreatedAt => {
                string_array(rows.iter().map(|row| Some(row.created_at.as_str())))
            }
            ChangeSurfaceColumn::Untracked => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.untracked).collect::<Vec<_>>(),
            )) as ArrayRef,
            ChangeSurfaceColumn::SnapshotContent => {
                string_array(rows.iter().map(|row| row.snapshot_content.as_deref()))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(change_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_change batch: {error}"))
    })
}

fn change_surface_schema(projection: &[ChangeSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                ChangeSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                ChangeSurfaceColumn::EntityId => Field::new("entity_id", DataType::Utf8, false),
                ChangeSurfaceColumn::SchemaKey => Field::new("schema_key", DataType::Utf8, false),
                ChangeSurfaceColumn::SchemaVersion => {
                    Field::new("schema_version", DataType::Utf8, false)
                }
                ChangeSurfaceColumn::FileId => Field::new("file_id", DataType::Utf8, true),
                ChangeSurfaceColumn::PluginKey => Field::new("plugin_key", DataType::Utf8, true),
                ChangeSurfaceColumn::Metadata => Field::new("metadata", DataType::Utf8, true),
                ChangeSurfaceColumn::CreatedAt => Field::new("created_at", DataType::Utf8, false),
                ChangeSurfaceColumn::Untracked => Field::new("untracked", DataType::Boolean, false),
                ChangeSurfaceColumn::SnapshotContent => {
                    Field::new("snapshot_content", DataType::Utf8, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn change_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<ChangeSurfaceColumn> {
    let all_columns = vec![
        ChangeSurfaceColumn::Id,
        ChangeSurfaceColumn::EntityId,
        ChangeSurfaceColumn::SchemaKey,
        ChangeSurfaceColumn::SchemaVersion,
        ChangeSurfaceColumn::FileId,
        ChangeSurfaceColumn::PluginKey,
        ChangeSurfaceColumn::Metadata,
        ChangeSurfaceColumn::CreatedAt,
        ChangeSurfaceColumn::Untracked,
        ChangeSurfaceColumn::SnapshotContent,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn lix_version_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("hidden", DataType::Boolean, false),
        Field::new("commit_id", DataType::Utf8, false),
    ]))
}

#[derive(Debug, Clone)]
struct LixVersionProvider {
    schema: SchemaRef,
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
}

impl LixVersionProvider {
    fn new(snapshot: Arc<dyn VersionSurfaceSnapshot>) -> Self {
        Self {
            schema: lix_version_schema(),
            snapshot,
        }
    }
}

#[async_trait]
impl TableProvider for LixVersionProvider {
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
        let projected_schema = projected_schema(&self.schema, projection)?;
        Ok(Arc::new(LixVersionScanExec::new(
            Arc::clone(&self.snapshot),
            projected_schema,
            projection.cloned(),
        )))
    }
}

#[derive(Debug)]
struct LixVersionScanExec {
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl LixVersionScanExec {
    fn new(
        snapshot: Arc<dyn VersionSurfaceSnapshot>,
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
            snapshot,
            schema,
            projection,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for LixVersionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LixVersionScanExec")
            }
            DisplayFormatType::TreeRender => write!(f, "LixVersionScanExec"),
        }
    }
}

impl ExecutionPlan for LixVersionScanExec {
    fn name(&self) -> &str {
        "LixVersionScanExec"
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
                "LixVersionScanExec does not accept children".to_string(),
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
                "LixVersionScanExec only exposes one partition, got {partition}"
            )));
        }

        let snapshot = Arc::clone(&self.snapshot);
        let projection = self.projection.clone();
        let schema = Arc::clone(&self.schema);
        let stream = stream::once(async move {
            let rows = enqueue_version_surface_scan(
                snapshot,
                VersionSurfaceScanRequest {
                    projection: version_projection_for_scan(projection.as_ref()),
                    limit: None,
                },
            )
            .await?;
            let batches = version_surface_record_batches(
                version_projection_for_scan(projection.as_ref()),
                &rows,
            )?;
            Ok::<_, DataFusionError>(stream::iter(
                batches.into_iter().map(Ok::<RecordBatch, DataFusionError>),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Debug)]
struct VersionSurfaceScanJob {
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    request: VersionSurfaceScanRequest,
    reply: oneshot::Sender<std::result::Result<Vec<VersionSurfaceRow>, LixError>>,
}

fn version_surface_scan_worker() -> &'static mpsc::Sender<VersionSurfaceScanJob> {
    static WORKER: OnceLock<mpsc::Sender<VersionSurfaceScanJob>> = OnceLock::new();
    WORKER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<VersionSurfaceScanJob>();
        thread::Builder::new()
            .name("sql2-version-surface-scan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("sql2 version-surface runtime should build");
                while let Ok(job) = rx.recv() {
                    let result = runtime
                        .block_on(async move { job.snapshot.scan_versions(&job.request).await });
                    let _ = job.reply.send(result);
                }
            })
            .expect("sql2 version-surface worker thread should spawn");
        tx
    })
}

async fn enqueue_version_surface_scan(
    snapshot: Arc<dyn VersionSurfaceSnapshot>,
    request: VersionSurfaceScanRequest,
) -> Result<Vec<VersionSurfaceRow>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    version_surface_scan_worker()
        .send(VersionSurfaceScanJob {
            snapshot,
            request,
            reply: reply_tx,
        })
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "sql2 failed to enqueue version surface scan job: {error}"
            ))
        })?;
    reply_rx
        .await
        .map_err(|_| {
            DataFusionError::Execution("sql2 version surface scan worker dropped reply".to_string())
        })?
        .map_err(lix_error_to_datafusion_error)
}

fn version_surface_record_batches(
    projection: Vec<VersionSurfaceColumn>,
    rows: &[VersionSurfaceRow],
) -> Result<Vec<RecordBatch>> {
    Ok(vec![version_surface_record_batch(&projection, rows)?])
}

fn version_surface_record_batch(
    projection: &[VersionSurfaceColumn],
    rows: &[VersionSurfaceRow],
) -> Result<RecordBatch> {
    let arrays = projection
        .iter()
        .map(|column| match column {
            VersionSurfaceColumn::Id => string_array(rows.iter().map(|row| Some(row.id.as_str()))),
            VersionSurfaceColumn::Name => {
                string_array(rows.iter().map(|row| Some(row.name.as_str())))
            }
            VersionSurfaceColumn::Hidden => Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.hidden).collect::<Vec<_>>(),
            )) as ArrayRef,
            VersionSurfaceColumn::CommitId => {
                string_array(rows.iter().map(|row| Some(row.commit_id.as_str())))
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(version_surface_schema(projection), arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 failed to build lix_version batch: {error}"))
    })
}

fn version_surface_schema(projection: &[VersionSurfaceColumn]) -> SchemaRef {
    Arc::new(Schema::new(
        projection
            .iter()
            .map(|column| match column {
                VersionSurfaceColumn::Id => Field::new("id", DataType::Utf8, false),
                VersionSurfaceColumn::Name => Field::new("name", DataType::Utf8, false),
                VersionSurfaceColumn::Hidden => Field::new("hidden", DataType::Boolean, false),
                VersionSurfaceColumn::CommitId => Field::new("commit_id", DataType::Utf8, false),
            })
            .collect::<Vec<_>>(),
    ))
}

fn version_projection_for_scan(projection: Option<&Vec<usize>>) -> Vec<VersionSurfaceColumn> {
    let all_columns = vec![
        VersionSurfaceColumn::Id,
        VersionSurfaceColumn::Name,
        VersionSurfaceColumn::Hidden,
        VersionSurfaceColumn::CommitId,
    ];
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixChangeRoute {
    id: Option<String>,
    entity_id: Option<String>,
    schema_key: Option<String>,
    file_id: Option<String>,
    plugin_key: Option<String>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl LixChangeRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_change_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Untracked => &mut route.untracked,
                        RouteBooleanField::Global
                        | RouteBooleanField::Hidden
                        | RouteBooleanField::LixcolGlobal
                        | RouteBooleanField::LixcolUntracked => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::EntityId => &mut route.entity_id,
                        RouteStringField::SchemaKey => &mut route.schema_key,
                        RouteStringField::FileId => &mut route.file_id,
                        RouteStringField::PluginKey => &mut route.plugin_key,
                        RouteStringField::VersionId
                        | RouteStringField::LixcolVersionId
                        | RouteStringField::Path => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }
}

fn change_filters_for_route(route: &LixChangeRoute) -> Vec<ChangeSurfaceFilter> {
    let mut filters = Vec::new();
    if let Some(id) = &route.id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::Id,
            Value::Text(id.clone()),
        ));
    }
    if let Some(entity_id) = &route.entity_id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::EntityId,
            Value::Text(entity_id.clone()),
        ));
    }
    if let Some(schema_key) = &route.schema_key {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::SchemaKey,
            Value::Text(schema_key.clone()),
        ));
    }
    if let Some(file_id) = &route.file_id {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::FileId,
            Value::Text(file_id.clone()),
        ));
    }
    if let Some(plugin_key) = &route.plugin_key {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::PluginKey,
            Value::Text(plugin_key.clone()),
        ));
    }
    if let Some(untracked) = route.untracked {
        filters.push(ChangeSurfaceFilter::Eq(
            ChangeSurfaceColumn::Untracked,
            Value::Boolean(untracked),
        ));
    }
    filters
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixStateRoute {
    version_id: Option<String>,
    schema_key: Option<String>,
    entity_id: Option<String>,
    file_id: Option<String>,
    global: Option<bool>,
    untracked: Option<bool>,
    contradictory: bool,
}

impl LixStateRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Global => &mut route.global,
                        RouteBooleanField::Untracked => &mut route.untracked,
                        RouteBooleanField::Hidden
                        | RouteBooleanField::LixcolGlobal
                        | RouteBooleanField::LixcolUntracked => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::VersionId => &mut route.version_id,
                        RouteStringField::LixcolVersionId => continue,
                        RouteStringField::SchemaKey => &mut route.schema_key,
                        RouteStringField::EntityId => &mut route.entity_id,
                        RouteStringField::FileId => &mut route.file_id,
                        RouteStringField::PluginKey
                        | RouteStringField::Id
                        | RouteStringField::Path => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn state_filters(&self) -> Vec<StateSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(schema_key) = &self.schema_key {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::SchemaKey,
                Value::Text(schema_key.clone()),
            ));
        }
        if let Some(entity_id) = &self.entity_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::EntityId,
                Value::Text(entity_id.clone()),
            ));
        }
        if let Some(file_id) = &self.file_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::FileId,
                Value::Text(file_id.clone()),
            ));
        }
        if let Some(global) = self.global {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Global,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.untracked {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Untracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixFileRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl LixFileRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_file_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Hidden => &mut route.hidden,
                        RouteBooleanField::LixcolGlobal => &mut route.lixcol_global,
                        RouteBooleanField::LixcolUntracked => &mut route.lixcol_untracked,
                        RouteBooleanField::Global | RouteBooleanField::Untracked => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::Path => &mut route.path,
                        RouteStringField::LixcolVersionId => &mut route.lixcol_version_id,
                        _ => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn file_filters(&self) -> Vec<FileSurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(id) = &self.id {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Id,
                Value::Text(id.clone()),
            ));
        }
        if let Some(path) = &self.path {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Path,
                Value::Text(path.clone()),
            ));
        }
        if let Some(version_id) = &self.lixcol_version_id {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolVersionId,
                Value::Text(version_id.clone()),
            ));
        }
        if let Some(hidden) = self.hidden {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::Hidden,
                Value::Boolean(hidden),
            ));
        }
        if let Some(global) = self.lixcol_global {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolGlobal,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.lixcol_untracked {
            filters.push(FileSurfaceFilter::Eq(
                FileSurfaceColumn::LixcolUntracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixDirectoryRoute {
    id: Option<String>,
    path: Option<String>,
    lixcol_version_id: Option<String>,
    hidden: Option<bool>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl LixDirectoryRoute {
    fn from_filters(filters: &[Expr]) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_directory_route_filter(filter) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::Hidden => &mut route.hidden,
                        RouteBooleanField::LixcolGlobal => &mut route.lixcol_global,
                        RouteBooleanField::LixcolUntracked => &mut route.lixcol_untracked,
                        RouteBooleanField::Global | RouteBooleanField::Untracked => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::Id => &mut route.id,
                        RouteStringField::Path => &mut route.path,
                        RouteStringField::LixcolVersionId => &mut route.lixcol_version_id,
                        _ => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn directory_filters(&self) -> Vec<DirectorySurfaceFilter> {
        let mut filters = Vec::new();
        if let Some(id) = &self.id {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Id,
                Value::Text(id.clone()),
            ));
        }
        if let Some(path) = &self.path {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Path,
                Value::Text(path.clone()),
            ));
        }
        if let Some(version_id) = &self.lixcol_version_id {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolVersionId,
                Value::Text(version_id.clone()),
            ));
        }
        if let Some(hidden) = self.hidden {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::Hidden,
                Value::Boolean(hidden),
            ));
        }
        if let Some(global) = self.lixcol_global {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolGlobal,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.lixcol_untracked {
            filters.push(DirectorySurfaceFilter::Eq(
                DirectorySurfaceColumn::LixcolUntracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

fn state_by_version_scan_request(
    surface_kind: LixStateSurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixStateRoute,
    limit: Option<usize>,
) -> Result<StateByVersionScanRequest> {
    let version_id = match surface_kind {
        LixStateSurfaceKind::State => default_version_id.to_string(),
        LixStateSurfaceKind::StateByVersion => route
            .version_id
            .clone()
            .unwrap_or_else(|| default_version_id.to_string()),
    };
    Ok(StateByVersionScanRequest {
        version_id,
        projection: state_projection_for_scan(surface_kind, projection),
        filters: route.state_filters(),
        limit,
    })
}

fn file_surface_scan_request(
    surface_kind: LixFileSurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixFileRoute,
    limit: Option<usize>,
) -> FileSurfaceScanRequest {
    let mut filters = route.file_filters();
    if surface_kind == LixFileSurfaceKind::File {
        filters.push(FileSurfaceFilter::Eq(
            FileSurfaceColumn::LixcolVersionId,
            Value::Text(default_version_id.to_string()),
        ));
    }
    FileSurfaceScanRequest {
        projection: file_projection_for_scan(surface_kind, projection),
        filters,
        limit,
    }
}

fn directory_surface_scan_request(
    surface_kind: LixDirectorySurfaceKind,
    default_version_id: &str,
    projection: Option<&Vec<usize>>,
    route: &LixDirectoryRoute,
    limit: Option<usize>,
) -> DirectorySurfaceScanRequest {
    let mut filters = route.directory_filters();
    if surface_kind == LixDirectorySurfaceKind::Directory {
        filters.push(DirectorySurfaceFilter::Eq(
            DirectorySurfaceColumn::LixcolVersionId,
            Value::Text(default_version_id.to_string()),
        ));
    }
    DirectorySurfaceScanRequest {
        projection: directory_projection_for_scan(surface_kind, projection),
        filters,
        limit,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LixEntityRoute {
    lixcol_version_id: Option<String>,
    lixcol_entity_id: Option<String>,
    lixcol_file_id: Option<String>,
    lixcol_global: Option<bool>,
    lixcol_untracked: Option<bool>,
    contradictory: bool,
}

impl LixEntityRoute {
    fn from_filters(filters: &[Expr], surface_variant: SurfaceVariant) -> Self {
        let mut route = Self::default();
        for filter in filters {
            let Some(predicate) = parse_entity_route_filter(filter, surface_variant) else {
                continue;
            };

            match predicate {
                RoutePredicate::Boolean { field, value } => {
                    let slot = match field {
                        RouteBooleanField::LixcolGlobal => &mut route.lixcol_global,
                        RouteBooleanField::LixcolUntracked => &mut route.lixcol_untracked,
                        RouteBooleanField::Global
                        | RouteBooleanField::Untracked
                        | RouteBooleanField::Hidden => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
                RoutePredicate::String { field, value } => {
                    let slot = match field {
                        RouteStringField::LixcolVersionId => &mut route.lixcol_version_id,
                        RouteStringField::EntityId => &mut route.lixcol_entity_id,
                        RouteStringField::FileId => &mut route.lixcol_file_id,
                        RouteStringField::VersionId
                        | RouteStringField::SchemaKey
                        | RouteStringField::PluginKey
                        | RouteStringField::Id
                        | RouteStringField::Path => continue,
                    };
                    assign_route_slot(slot, value, &mut route.contradictory);
                }
            }
        }
        route
    }

    fn state_filters(&self, schema_key: &str) -> Vec<StateSurfaceFilter> {
        let mut filters = vec![StateSurfaceFilter::Eq(
            StateSurfaceColumn::SchemaKey,
            Value::Text(schema_key.to_string()),
        )];
        if let Some(entity_id) = &self.lixcol_entity_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::EntityId,
                Value::Text(entity_id.clone()),
            ));
        }
        if let Some(file_id) = &self.lixcol_file_id {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::FileId,
                Value::Text(file_id.clone()),
            ));
        }
        if let Some(global) = self.lixcol_global {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Global,
                Value::Boolean(global),
            ));
        }
        if let Some(untracked) = self.lixcol_untracked {
            filters.push(StateSurfaceFilter::Eq(
                StateSurfaceColumn::Untracked,
                Value::Boolean(untracked),
            ));
        }
        filters
    }
}

fn entity_surface_scan_request(
    spec: &PreparedSql2EntitySurfaceSpec,
    default_version_id: &str,
    projected_columns: &[String],
    route: &LixEntityRoute,
    limit: Option<usize>,
) -> Result<StateByVersionScanRequest> {
    let version_id = match spec.surface_variant {
        SurfaceVariant::Default => default_version_id.to_string(),
        SurfaceVariant::ByVersion => route
            .lixcol_version_id
            .clone()
            .unwrap_or_else(|| default_version_id.to_string()),
        other => {
            return Err(DataFusionError::Execution(format!(
                "sql2 does not support entity surface variant {:?} for {} yet",
                other, spec.public_name
            )));
        }
    };

    Ok(StateByVersionScanRequest {
        version_id,
        projection: entity_state_projection(projected_columns),
        filters: route.state_filters(&spec.schema_key),
        limit,
    })
}

fn assign_route_slot<T: PartialEq>(slot: &mut Option<T>, value: T, contradictory: &mut bool) {
    match slot {
        Some(existing) if *existing != value => *contradictory = true,
        Some(_) => {}
        None => *slot = Some(value),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RoutePredicate {
    Boolean {
        field: RouteBooleanField,
        value: bool,
    },
    String {
        field: RouteStringField,
        value: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteBooleanField {
    Global,
    Untracked,
    Hidden,
    LixcolGlobal,
    LixcolUntracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RouteStringField {
    VersionId,
    LixcolVersionId,
    SchemaKey,
    EntityId,
    FileId,
    PluginKey,
    Id,
    Path,
}

fn parse_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "version_id" => parse_string_route(literal, RouteStringField::VersionId),
        "schema_key" => parse_string_route(literal, RouteStringField::SchemaKey),
        "entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "file_id" => parse_string_route(literal, RouteStringField::FileId),
        "global" => parse_boolean_route(literal, RouteBooleanField::Global),
        "untracked" => parse_boolean_route(literal, RouteBooleanField::Untracked),
        _ => None,
    }
}

fn parse_file_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_file_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_file_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_directory_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_directory_route_column_literal_filter(&binary_expr.left, &binary_expr.right).or_else(
        || parse_directory_route_column_literal_filter(&binary_expr.right, &binary_expr.left),
    )
}

fn parse_change_route_filter(expr: &Expr) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_change_route_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| parse_change_route_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn parse_entity_route_filter(
    expr: &Expr,
    surface_variant: SurfaceVariant,
) -> Option<RoutePredicate> {
    let Expr::BinaryExpr(binary_expr) = expr else {
        return None;
    };
    if binary_expr.op != Operator::Eq {
        return None;
    }

    parse_entity_route_column_literal_filter(&binary_expr.left, &binary_expr.right, surface_variant)
        .or_else(|| {
            parse_entity_route_column_literal_filter(
                &binary_expr.right,
                &binary_expr.left,
                surface_variant,
            )
        })
}

fn parse_directory_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_change_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "schema_key" => parse_string_route(literal, RouteStringField::SchemaKey),
        "file_id" => parse_string_route(literal, RouteStringField::FileId),
        "plugin_key" => parse_string_route(literal, RouteStringField::PluginKey),
        "untracked" => parse_boolean_route(literal, RouteBooleanField::Untracked),
        _ => None,
    }
}

fn parse_entity_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    surface_variant: SurfaceVariant,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "lixcol_version_id" if surface_variant == SurfaceVariant::ByVersion => {
            parse_string_route(literal, RouteStringField::LixcolVersionId)
        }
        "lixcol_entity_id" => parse_string_route(literal, RouteStringField::EntityId),
        "lixcol_file_id" => parse_string_route(literal, RouteStringField::FileId),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_file_route_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<RoutePredicate> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let Expr::Literal(literal, _) = literal_expr else {
        return None;
    };

    match column.name.as_str() {
        "id" => parse_string_route(literal, RouteStringField::Id),
        "path" => parse_string_route(literal, RouteStringField::Path),
        "lixcol_version_id" => parse_string_route(literal, RouteStringField::LixcolVersionId),
        "hidden" => parse_boolean_route(literal, RouteBooleanField::Hidden),
        "lixcol_global" => parse_boolean_route(literal, RouteBooleanField::LixcolGlobal),
        "lixcol_untracked" => parse_boolean_route(literal, RouteBooleanField::LixcolUntracked),
        _ => None,
    }
}

fn parse_string_route(literal: &ScalarValue, field: RouteStringField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(RoutePredicate::String {
            field,
            value: value.clone(),
        }),
        _ => None,
    }
}

fn parse_boolean_route(literal: &ScalarValue, field: RouteBooleanField) -> Option<RoutePredicate> {
    match literal {
        ScalarValue::Boolean(Some(value)) => Some(RoutePredicate::Boolean {
            field,
            value: *value,
        }),
        _ => None,
    }
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

fn state_projection_for_scan(
    surface_kind: LixStateSurfaceKind,
    projection: Option<&Vec<usize>>,
) -> Vec<StateSurfaceColumn> {
    let all_columns = match surface_kind {
        LixStateSurfaceKind::State => vec![
            StateSurfaceColumn::EntityId,
            StateSurfaceColumn::SchemaKey,
            StateSurfaceColumn::FileId,
            StateSurfaceColumn::PluginKey,
            StateSurfaceColumn::SnapshotContent,
            StateSurfaceColumn::Metadata,
            StateSurfaceColumn::SchemaVersion,
            StateSurfaceColumn::CreatedAt,
            StateSurfaceColumn::UpdatedAt,
            StateSurfaceColumn::Global,
            StateSurfaceColumn::ChangeId,
            StateSurfaceColumn::CommitId,
            StateSurfaceColumn::Untracked,
        ],
        LixStateSurfaceKind::StateByVersion => vec![
            StateSurfaceColumn::EntityId,
            StateSurfaceColumn::SchemaKey,
            StateSurfaceColumn::FileId,
            StateSurfaceColumn::PluginKey,
            StateSurfaceColumn::SnapshotContent,
            StateSurfaceColumn::Metadata,
            StateSurfaceColumn::SchemaVersion,
            StateSurfaceColumn::CreatedAt,
            StateSurfaceColumn::UpdatedAt,
            StateSurfaceColumn::Global,
            StateSurfaceColumn::ChangeId,
            StateSurfaceColumn::CommitId,
            StateSurfaceColumn::Untracked,
            StateSurfaceColumn::VersionId,
        ],
    };
    projection.map_or(all_columns.clone(), |indices| {
        indices
            .iter()
            .filter_map(|index| all_columns.get(*index).copied())
            .collect()
    })
}

fn projected_entity_columns(
    spec: &PreparedSql2EntitySurfaceSpec,
    projection: Option<&Vec<usize>>,
) -> Vec<String> {
    projection.map_or_else(
        || spec.column_order.clone(),
        |indices| {
            indices
                .iter()
                .filter_map(|index| spec.column_order.get(*index).cloned())
                .collect()
        },
    )
}

fn entity_state_projection(projected_columns: &[String]) -> Vec<StateSurfaceColumn> {
    let mut projection = Vec::<StateSurfaceColumn>::new();
    let mut ensure = |column| {
        if !projection.contains(&column) {
            projection.push(column);
        }
    };

    for column in projected_columns {
        match column.as_str() {
            "lixcol_entity_id" => ensure(StateSurfaceColumn::EntityId),
            "lixcol_schema_key" => ensure(StateSurfaceColumn::SchemaKey),
            "lixcol_file_id" => ensure(StateSurfaceColumn::FileId),
            "lixcol_plugin_key" => ensure(StateSurfaceColumn::PluginKey),
            "lixcol_schema_version" => ensure(StateSurfaceColumn::SchemaVersion),
            "lixcol_version_id" => ensure(StateSurfaceColumn::VersionId),
            "lixcol_metadata" => ensure(StateSurfaceColumn::Metadata),
            "lixcol_global" => ensure(StateSurfaceColumn::Global),
            "lixcol_untracked" => ensure(StateSurfaceColumn::Untracked),
            _ => ensure(StateSurfaceColumn::SnapshotContent),
        }
    }

    if projection.is_empty() {
        projection.push(StateSurfaceColumn::SnapshotContent);
    }

    projection
}

fn entity_surface_schema(
    spec: &PreparedSql2EntitySurfaceSpec,
    column_names: &[String],
) -> Result<SchemaRef> {
    let fields = column_names
        .iter()
        .map(|column_name| {
            let Some(column_type) = spec.column_types.get(column_name) else {
                return Err(DataFusionError::Execution(format!(
                    "sql2 entity surface '{}' is missing type info for column '{}'",
                    spec.public_name, column_name
                )));
            };
            Ok(Field::new(
                column_name,
                surface_column_data_type(*column_type),
                true,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn surface_column_data_type(column_type: SurfaceColumnType) -> DataType {
    match column_type {
        SurfaceColumnType::String | SurfaceColumnType::Json => DataType::Utf8,
        SurfaceColumnType::Integer => DataType::Int64,
        SurfaceColumnType::Number => DataType::Float64,
        SurfaceColumnType::Boolean => DataType::Boolean,
    }
}

fn entity_surface_batches_from_state_batches(
    spec: &PreparedSql2EntitySurfaceSpec,
    projected_columns: &[String],
    state_batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>> {
    let mut column_values = projected_columns
        .iter()
        .map(|column| (column.clone(), Vec::<Value>::new()))
        .collect::<BTreeMap<_, _>>();

    for batch in state_batches {
        for row_index in 0..batch.num_rows() {
            let state_row = state_row_from_batch(batch, row_index)?;
            let entity_values = entity_row_values_from_state(spec, projected_columns, &state_row)?;
            for (column_name, value) in entity_values {
                column_values
                    .get_mut(&column_name)
                    .expect("entity column should exist in output map")
                    .push(value);
            }
        }
    }

    let arrays = projected_columns
        .iter()
        .map(|column_name| {
            let values = column_values
                .remove(column_name)
                .expect("entity output column should have collected values");
            let column_type = *spec
                .column_types
                .get(column_name)
                .expect("entity output column type should exist");
            lix_values_to_array(&values, column_type)
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = entity_surface_schema(spec, projected_columns)?;
    let batch = RecordBatch::try_new(schema, arrays).map_err(|error| {
        DataFusionError::Execution(format!("sql2 entity batch build failed: {error}"))
    })?;
    Ok(vec![batch])
}

fn state_row_from_batch(batch: &RecordBatch, row_index: usize) -> Result<BTreeMap<String, Value>> {
    let mut row = BTreeMap::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)?;
        row.insert(field.name().to_string(), scalar_value_to_lix_value(&scalar));
    }
    Ok(row)
}

fn entity_row_values_from_state(
    spec: &PreparedSql2EntitySurfaceSpec,
    projected_columns: &[String],
    state_row: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    let mut parsed_snapshot = None::<serde_json::Value>;
    let needs_snapshot = projected_columns
        .iter()
        .any(|column| !column.starts_with("lixcol_"));
    if needs_snapshot {
        parsed_snapshot = match state_row.get("snapshot_content") {
            Some(Value::Text(text)) => Some(serde_json::from_str(text).map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 entity surface '{}' received invalid snapshot_content JSON: {error}",
                    spec.public_name
                ))
            })?),
            Some(Value::Null) | None => None,
            Some(other) => {
                return Err(DataFusionError::Execution(format!(
                    "sql2 entity surface '{}' expected snapshot_content text, got {other:?}",
                    spec.public_name
                )))
            }
        };
    }

    let mut values = BTreeMap::new();
    for column_name in projected_columns {
        let value = match column_name.as_str() {
            "lixcol_entity_id" => state_row.get("entity_id").cloned().unwrap_or(Value::Null),
            "lixcol_schema_key" => state_row.get("schema_key").cloned().unwrap_or(Value::Null),
            "lixcol_file_id" => state_row.get("file_id").cloned().unwrap_or(Value::Null),
            "lixcol_plugin_key" => state_row.get("plugin_key").cloned().unwrap_or(Value::Null),
            "lixcol_schema_version" => state_row
                .get("schema_version")
                .cloned()
                .unwrap_or(Value::Null),
            "lixcol_version_id" => state_row.get("version_id").cloned().unwrap_or(Value::Null),
            "lixcol_metadata" => state_row.get("metadata").cloned().unwrap_or(Value::Null),
            "lixcol_global" => state_row.get("global").cloned().unwrap_or(Value::Null),
            "lixcol_untracked" => state_row.get("untracked").cloned().unwrap_or(Value::Null),
            property_name => parsed_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get(property_name))
                .map(json_value_to_lix_value)
                .unwrap_or(Value::Null),
        };
        values.insert(column_name.clone(), value);
    }

    Ok(values)
}

fn json_value_to_lix_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Boolean(*value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Value::Integer(integer)
            } else {
                Value::Real(value.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(value) => Value::Text(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(value.clone()),
    }
}

fn lix_values_to_array(values: &[Value], column_type: SurfaceColumnType) -> Result<ArrayRef> {
    match column_type {
        SurfaceColumnType::String => {
            let strings = values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Text(value) => Ok(Some(value.clone())),
                    other => Err(DataFusionError::Execution(format!(
                        "sql2 expected text value, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(strings)))
        }
        SurfaceColumnType::Json => {
            let strings = values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Text(value) => Ok(Some(value.clone())),
                    Value::Json(value) => Ok(Some(value.to_string())),
                    Value::Integer(value) => Ok(Some(value.to_string())),
                    Value::Real(value) => Ok(Some(value.to_string())),
                    Value::Boolean(value) => Ok(Some(value.to_string())),
                    other => Err(DataFusionError::Execution(format!(
                        "sql2 expected json-compatible value, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(strings)))
        }
        SurfaceColumnType::Integer => {
            let integers = values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Integer(value) => Ok(Some(*value)),
                    other => Err(DataFusionError::Execution(format!(
                        "sql2 expected integer value, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Int64Array::from(integers)))
        }
        SurfaceColumnType::Number => {
            let numbers = values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Integer(value) => Ok(Some(*value as f64)),
                    Value::Real(value) => Ok(Some(*value)),
                    other => Err(DataFusionError::Execution(format!(
                        "sql2 expected numeric value, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(numbers)))
        }
        SurfaceColumnType::Boolean => {
            let booleans = values
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Boolean(value) => Ok(Some(*value)),
                    Value::Integer(value) if *value == 0 || *value == 1 => Ok(Some(*value != 0)),
                    other => Err(DataFusionError::Execution(format!(
                        "sql2 expected boolean value, got {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(BooleanArray::from(booleans)))
        }
    }
}

fn query_result_from_batches(
    result_columns: &[String],
    batches: &[RecordBatch],
) -> Result<QueryResult, LixError> {
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for array in batch.columns() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                row.push(scalar_value_to_lix_value(&scalar));
            }
            rows.push(row);
        }
    }

    Ok(QueryResult {
        rows,
        columns: result_columns.to_vec(),
    })
}

fn scalar_value_to_lix_value(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(value)) => Value::Boolean(*value),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(value)) => Value::Integer(*value),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Text(value.to_string()),
        },
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(value)) => Value::Real(f64::from(*value)),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(value)) => Value::Real(*value),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Value::Text(value.clone()),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Value::Null
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Value::Blob(value.clone())
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
        other => Value::Text(other.to_string()),
    }
}

fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    let values = values
        .map(|value| value.map(ToOwned::to_owned))
        .collect::<Vec<_>>();
    Arc::new(StringArray::from(values)) as ArrayRef
}

fn binary_array<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> ArrayRef {
    let values = values.collect::<Vec<_>>();
    Arc::new(BinaryArray::from(values)) as ArrayRef
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::{
        build_session_for_read_with_borrowed_backend, build_session_for_read_with_shared_backend,
        execute_read_with_backend, execute_read_with_shared_backend, parse_directory_route_filter,
        parse_file_route_filter, parse_route_filter, PreparedSql2ReadArtifact, RouteBooleanField,
        RoutePredicate, RouteStringField,
    };
    use crate::live_state::{
        open_state_by_version_snapshot_with_shared_backend, StateByVersionScanRequest,
        StateSurfaceColumn, StateSurfaceFilter,
    };
    use crate::session::AdditionalSessionOptions;
    use crate::test_support::{boot_test_engine, TestSqliteBackendEvent};
    use crate::{CreateVersionOptions, LixBackend, TransactionBeginMode, Value};
    use serde_json::json;

    async fn setup_sql2_state_fixture(
    ) -> Result<(crate::test_support::TestSqliteBackend, crate::Session), crate::LixError> {
        let (backend, _lix, session) = boot_test_engine().await?;
        session
            .register_schema(&json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;
        session
            .register_schema(&json!({
                "x-lix-key": "other_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;

        session
            .create_version(CreateVersionOptions {
                id: Some("version-a".to_string()),
                name: Some("version-a".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-b".to_string()),
                name: Some("version-b".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, 'version-a', NULL, '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, 'version-b', NULL, '{\"value\":\"B\"}', '1'\
                 )",
                &[],
            )
            .await?;
        let sql2_session = session
            .open_additional_session(AdditionalSessionOptions {
                active_version_id: Some("version-a".to_string()),
                origin_key: Some("engine:sql2".to_string()),
                ..AdditionalSessionOptions::default()
            })
            .await?;
        sql2_session
            .execute(
                "INSERT INTO lix_file (id, path, data, metadata) VALUES ('file-a', '/hello.txt', X'68656C6C6F', '{\"kind\":\"text\"}')",
                &[],
            )
            .await?;
        sql2_session
            .execute(
                "INSERT INTO lix_directory (id, path, parent_id, name) VALUES ('dir-a', '/docs/', NULL, 'docs')",
                &[],
            )
            .await?;
        Ok((backend, sql2_session))
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-datafusion-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn parses_string_route_filters() {
        let filter =
            datafusion::logical_expr::col("schema_key").eq(datafusion::logical_expr::lit("demo"));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::SchemaKey,
                value: "demo".to_string(),
            })
        );
    }

    #[test]
    fn parses_version_id_route_filters() {
        let filter =
            datafusion::logical_expr::col("version_id").eq(datafusion::logical_expr::lit("v1"));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::VersionId,
                value: "v1".to_string(),
            })
        );
    }

    #[test]
    fn parses_boolean_route_filters() {
        let filter =
            datafusion::logical_expr::col("untracked").eq(datafusion::logical_expr::lit(true));

        assert_eq!(
            parse_route_filter(&filter),
            Some(RoutePredicate::Boolean {
                field: RouteBooleanField::Untracked,
                value: true,
            })
        );
    }

    #[test]
    fn parses_file_route_filters() {
        let filter =
            datafusion::logical_expr::col("path").eq(datafusion::logical_expr::lit("/hello.txt"));

        assert_eq!(
            parse_file_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::Path,
                value: "/hello.txt".to_string(),
            })
        );
    }

    #[test]
    fn parses_file_by_version_route_filters() {
        let filter = datafusion::logical_expr::col("lixcol_version_id")
            .eq(datafusion::logical_expr::lit("version-a"));

        assert_eq!(
            parse_file_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::LixcolVersionId,
                value: "version-a".to_string(),
            })
        );
    }

    #[test]
    fn parses_directory_route_filters() {
        let filter =
            datafusion::logical_expr::col("path").eq(datafusion::logical_expr::lit("/docs/"));

        assert_eq!(
            parse_directory_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::Path,
                value: "/docs/".to_string(),
            })
        );
    }

    #[test]
    fn parses_directory_by_version_route_filters() {
        let filter = datafusion::logical_expr::col("lixcol_version_id")
            .eq(datafusion::logical_expr::lit("version-a"));

        assert_eq!(
            parse_directory_route_filter(&filter),
            Some(RoutePredicate::String {
                field: RouteStringField::LixcolVersionId,
                value: "version-a".to_string(),
            })
        );
    }

    #[test]
    fn builds_session_and_executes_lix_state_query() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let ctx = build_session_for_read_with_borrowed_backend(&backend, &artifact)
                    .await
                    .expect("session should build");
                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let batches = dataframe.collect().await.expect("query should execute");
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
            })
        });
    }

    #[test]
    fn shared_backend_path_defers_state_reads_until_execution() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' ORDER BY entity_id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("lix_registered_schema")
                            && !sql.contains("change_commit_by_change_id")
                            && !sql.contains("lix_internal_live")),
                    "session setup should not query live_state on shared-backend path"
                );

                let dataframe = ctx.sql(&artifact.sql).await.expect("query should plan");
                let _batches = dataframe.collect().await.expect("query should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("test_state_schema")),
                    "execution should query live_state on shared-backend path"
                );
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .all(|sql| !sql.contains("other_state_schema")),
                    "schema_key pushdown should avoid scanning unrelated state schemas"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_opens_read_transaction_for_query_snapshot() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                backend.clear_query_log();
                let shared_backend: Arc<dyn crate::LixBackend + Send + Sync> =
                    Arc::new(backend.clone());
                let _ctx = build_session_for_read_with_shared_backend(shared_backend, &artifact)
                    .await
                    .expect("shared-backend session should build");

                let begin_modes = backend
                    .recorded_events()
                    .into_iter()
                    .filter_map(|event| match event {
                        TestSqliteBackendEvent::BeginTransaction { mode } => Some(mode),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    begin_modes,
                    vec![TransactionBeginMode::Read],
                    "shared-backend sql2 path should open one read transaction as the query snapshot"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_entity_constraint_into_source_scan() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("\"entity_id\" = 'entity-a'")
                            || sql.contains("entity_id = 'entity-a'")),
                    "entity_id filter should be pushed into live_state source scans"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_derives_required_columns_from_projection() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state WHERE schema_key = 'test_state_schema'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                backend.clear_query_log();
                let _result =
                    execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                        .await
                        .expect("sql2 shared-backend read should execute");

                let state_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| sql.contains("lix_internal_live_v1_test_state_schema"))
                    .collect::<Vec<_>>();
                assert!(
                    !state_scan_sql.is_empty(),
                    "expected sql2 read to scan the test_state_schema live table"
                );
                assert!(
                    state_scan_sql.iter().all(|sql| !sql.contains("\"value\"")),
                    "entity-only projection should avoid loading dynamic state columns: {state_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn shared_backend_path_pushes_limit_only_for_safe_untracked_scans() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-a', 'test_state_schema', NULL, NULL, '{\"value\":\"UA\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("first untracked row should insert");
                session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version\
                         ) VALUES (\
                         'entity-untracked-b', 'test_state_schema', NULL, NULL, '{\"value\":\"UB\"}', '1'\
                         )",
                        &[],
                    )
                    .await
                    .expect("second untracked row should insert");

                let untracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = untracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(true),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("untracked state-surface read should execute");

                let untracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = true")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    untracked_scan_sql.iter().any(|sql| sql.contains("LIMIT 1")),
                    "single-lane untracked scan should receive the pushed limit: {untracked_scan_sql:?}"
                );
                drop(untracked_snapshot);

                let tracked_snapshot =
                    open_state_by_version_snapshot_with_shared_backend(Arc::new(backend.clone()))
                        .await
                        .expect("shared-backend snapshot should open");

                backend.clear_query_log();
                let _batches = tracked_snapshot
                    .scan_state_by_version_batches(&StateByVersionScanRequest {
                        version_id: "version-a".to_string(),
                        projection: vec![StateSurfaceColumn::EntityId],
                        filters: vec![
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::SchemaKey,
                                Value::Text("test_state_schema".to_string()),
                            ),
                            StateSurfaceFilter::Eq(
                                StateSurfaceColumn::Untracked,
                                Value::Boolean(false),
                            ),
                        ],
                        limit: Some(1),
                    })
                    .await
                    .expect("tracked state-surface read should execute");

                let tracked_scan_sql = backend
                    .executed_sql()
                    .into_iter()
                    .filter(|sql| {
                        sql.contains("lix_internal_live_v1_test_state_schema")
                            && sql.contains("untracked = false")
                            && sql.contains("is_tombstone = 0")
                    })
                    .collect::<Vec<_>>();
                assert!(
                    tracked_scan_sql.iter().all(|sql| !sql.contains("LIMIT 1")),
                    "tracked scans should keep source-side limits disabled: {tracked_scan_sql:?}"
                );
            })
        });
    }

    #[test]
    fn execute_read_uses_active_version_snapshot_for_lix_state() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.columns, vec!["entity_id", "snapshot_content"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-a".to_string()),
                        Value::Text("{\"value\":\"A\"}".to_string())
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_exposes_commit_id_for_tracked_lix_state_rows() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT commit_id FROM lix_state WHERE schema_key = 'test_state_schema' AND entity_id = 'entity-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 read should execute");
                assert_eq!(result.rows.len(), 1);
                match &result.rows[0][0] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected text commit_id, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_uses_execution_time_state_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, snapshot_content FROM lix_state WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                backend.clear_query_log();
                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend read should execute");
                assert_eq!(result.rows.len(), 1);
                assert!(
                    backend
                        .executed_sql()
                        .into_iter()
                        .any(|sql| sql.contains("lix_registered_schema")),
                    "shared-backend execution should query live_state at execution time"
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id, version_id, snapshot_content FROM lix_state_by_version WHERE version_id = 'version-b' AND schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_by_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend by-version read should execute");
                assert_eq!(
                    result.columns,
                    vec!["entity_id", "version_id", "snapshot_content"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-b".to_string()),
                        Value::Text("version-b".to_string()),
                        Value::Text("{\"value\":\"B\"}".to_string()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_requires_exact_version_for_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT entity_id FROM lix_state_by_version WHERE schema_key = 'test_state_schema'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_state_by_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let error = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect_err("by-version read without version filter should fail");
                assert!(
                    error
                        .description
                        .contains("requires an exact version_id = ... predicate"),
                    "unexpected error: {error:?}"
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_file() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, data FROM lix_file WHERE path = '/hello.txt'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 file read should execute");
                assert_eq!(result.columns, vec!["id", "path", "data"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("file-a".to_string()),
                        Value::Text("/hello.txt".to_string()),
                        Value::Blob(b"hello".to_vec()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_file() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path FROM lix_file ORDER BY path".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend file read should execute");
                assert_eq!(result.columns, vec!["id", "path"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("/hello.txt".to_string()),
                        ]),
                    "expected inserted file in lix_file results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_file_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, lixcol_version_id \
                          FROM lix_file_by_version \
                         WHERE lixcol_version_id = 'version-a' \
                         ORDER BY path"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file_by_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend file-by-version read should execute");
                assert_eq!(result.columns, vec!["id", "path", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("/hello.txt".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected inserted file in lix_file_by_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_directory() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, name FROM lix_directory WHERE path = '/docs/'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 directory read should execute");
                assert_eq!(result.columns, vec!["id", "path", "name"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("dir-a".to_string()),
                        Value::Text("/docs/".to_string()),
                        Value::Text("docs".to_string()),
                    ]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_directory() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path FROM lix_directory ORDER BY path".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend directory read should execute");
                assert_eq!(result.columns, vec!["id", "path"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("dir-a".to_string()),
                            Value::Text("/docs/".to_string()),
                        ]),
                    "expected inserted directory in lix_directory results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_reads_lix_directory_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, path, lixcol_version_id \
                          FROM lix_directory_by_version \
                         WHERE lixcol_version_id = 'version-a' \
                         ORDER BY path"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_directory_by_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend directory-by-version read should execute");
                assert_eq!(result.columns, vec!["id", "path", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("dir-a".to_string()),
                            Value::Text("/docs/".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected inserted directory in lix_directory_by_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, name FROM lix_version WHERE id = 'version-a'".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 version read should execute");
                assert_eq!(result.columns, vec!["id", "name"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("version-a".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected version-a in lix_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_scans_lix_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT id, hidden FROM lix_version ORDER BY id".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend version read should execute");
                assert_eq!(result.columns, vec!["id", "hidden"]);
                assert!(
                    result
                        .rows
                        .iter()
                        .any(|row| row.first() == Some(&Value::Text("version-a".to_string()))),
                    "expected version-a in lix_version results: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_counts_lix_change() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT COUNT(*) AS c FROM lix_change".to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 change count should execute");
                assert_eq!(result.columns, vec!["c"]);
                let count = result.rows.first().and_then(|row| row.first()).cloned();
                assert!(
                    matches!(count, Some(Value::Integer(value)) if value > 0),
                    "expected positive lix_change count, got {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_supports_literal_only_lix_change_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT 'observe-shared-sentinel' AS marker FROM lix_change LIMIT 1"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 literal-only change read should execute");
                assert_eq!(result.columns, vec!["marker"]);
                assert_eq!(
                    result.rows,
                    vec![vec![Value::Text("observe-shared-sentinel".to_string())]]
                );
            })
        });
    }

    #[test]
    fn execute_read_with_backend_reads_lix_change_by_id() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let raw = backend
                    .execute(
                        "SELECT id FROM lix_internal_change WHERE entity_id = 'file-a' ORDER BY created_at DESC LIMIT 1",
                        &[],
                    )
                    .await
                    .expect("raw change lookup should execute");
                let change_id = match raw.rows.first().and_then(|row| row.first()) {
                    Some(Value::Text(value)) => value.clone(),
                    other => panic!("expected raw file-a change id, got {other:?}"),
                };
                let artifact = PreparedSql2ReadArtifact {
                    sql: format!(
                        "SELECT id, entity_id, schema_key FROM lix_change WHERE id = '{change_id}'"
                    ),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_change".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_backend(&backend, &artifact)
                    .await
                    .expect("sql2 change read should execute");
                assert_eq!(result.columns, vec!["id", "entity_id", "schema_key"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text(change_id));
                assert_eq!(result.rows[0][1], Value::Text("file-a".to_string()));
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_mixed_file_queries() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT f.id, fbv.lixcol_version_id \
                          FROM lix_file f \
                          JOIN lix_file_by_version fbv \
                            ON f.id = fbv.id \
                         WHERE fbv.lixcol_version_id = 'version-a'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec!["lix_file".to_string(), "lix_file_by_version".to_string()],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend mixed file read should execute");
                assert_eq!(result.columns, vec!["id", "lixcol_version_id"]);
                assert!(
                    result.rows.iter().any(|row| row
                        == &vec![
                            Value::Text("file-a".to_string()),
                            Value::Text("version-a".to_string()),
                        ]),
                    "expected joined active/by-version file row: {:?}",
                    result.rows
                );
            })
        });
    }

    #[test]
    fn execute_read_with_shared_backend_supports_mixed_state_queries() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let artifact = PreparedSql2ReadArtifact {
                    sql: "SELECT s.entity_id, sbv.version_id \
                          FROM lix_state s \
                          JOIN lix_state_by_version sbv \
                            ON s.entity_id = sbv.entity_id \
                         WHERE s.schema_key = 'test_state_schema' \
                           AND sbv.schema_key = 'test_state_schema' \
                           AND sbv.version_id = 'version-a'"
                        .to_string(),
                    bound_parameters: vec![],
                    active_version_id: "version-a".to_string(),
                    surface_names: vec![
                        "lix_state".to_string(),
                        "lix_state_by_version".to_string(),
                    ],
                    entity_surfaces: BTreeMap::new(),
                };

                let result = execute_read_with_shared_backend(Arc::new(backend.clone()), &artifact)
                    .await
                    .expect("sql2 shared-backend mixed read should execute");
                assert_eq!(result.columns, vec!["entity_id", "version_id"]);
                assert_eq!(result.rows.len(), 1);
                assert_eq!(
                    result.rows[0],
                    vec![
                        Value::Text("entity-a".to_string()),
                        Value::Text("version-a".to_string()),
                    ]
                );
            })
        });
    }
}
