use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};
use serde_json::Value as JsonValue;

use crate::branch::BranchRefReader;
use crate::commit_graph::CommitGraphReader;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateRowFilter, LiveStateScanRequest,
};
use crate::sql2::branch_scope::{resolve_provider_branch_ids, BranchBinding};
use crate::sql2::catalog::{
    entity_surface_schema, EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec, PublicCatalog,
    PublicSurfaceKind,
};
use crate::{serialize_row_metadata, LixError};

use crate::sql2::{
    SqlHistoryQuerySource, SqlWriteContext, WriteContextBranchRefReader,
    WriteContextLiveStateReader,
};

use super::entity_history::EntityHistoryProvider;
use crate::storage::StorageRead;

pub(crate) async fn register_entity_providers<S>(
    ctx: &SessionContext,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlHistoryQuerySource<S>,
    catalog: &PublicCatalog,
) -> Result<(), LixError>
where
    S: StorageRead + Clone + Send + Sync + 'static,
{
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                ctx.register_table(
                    &surface.name,
                    Arc::new(EntityProvider::active(
                        spec,
                        Arc::clone(&live_state),
                        Arc::clone(&branch_ref),
                        active_branch_id.to_string(),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                ctx.register_table(
                    &surface.name,
                    Arc::new(EntityProvider::by_branch(
                        spec,
                        Arc::clone(&live_state),
                        Arc::clone(&branch_ref),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            PublicSurfaceKind::EntityHistory { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                ctx.register_table(
                    &surface.name,
                    Arc::new(EntityHistoryProvider::new(
                        spec,
                        Arc::clone(&commit_graph),
                        query_source.clone(),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            _ => {}
        }
    }

    Ok(())
}

pub(crate) async fn register_entity_write_providers(
    ctx: &SessionContext,
    write_ctx: SqlWriteContext,
    catalog: &PublicCatalog,
) -> Result<(), LixError> {
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                ctx.register_table(
                    &surface.name,
                    Arc::new(EntityProvider::active_with_write(spec, write_ctx.clone())),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                ctx.register_table(
                    &surface.name,
                    Arc::new(EntityProvider::by_branch_with_write(
                        spec,
                        write_ctx.clone(),
                    )),
                )
                .map_err(datafusion_error_to_lix_error)?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn catalog_entity_spec(
    catalog: &PublicCatalog,
    schema_key: &str,
) -> Result<Arc<EntitySurfaceSpec>, LixError> {
    catalog
        .entity_spec(schema_key)
        .cloned()
        .map(Arc::new)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("catalog entity surface '{schema_key}' is missing its surface spec"),
            )
        })
}

pub(crate) struct EntityProvider {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    schema: SchemaRef,
    variant: EntitySurfaceShape,
    branch_binding: BranchBinding,
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
        branch_ref: Arc<dyn BranchRefReader>,
        active_branch_id: String,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntitySurfaceShape::Active),
            spec,
            live_state,
            branch_ref,
            variant: EntitySurfaceShape::Active,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn active_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntitySurfaceShape::Active),
            spec,
            live_state,
            branch_ref,
            variant: EntitySurfaceShape::Active,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn by_branch(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntitySurfaceShape::ByBranch),
            spec,
            live_state,
            branch_ref,
            variant: EntitySurfaceShape::ByBranch,
            branch_binding: BranchBinding::explicit(),
        }
    }

    fn by_branch_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let branch_ref = Arc::new(WriteContextBranchRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntitySurfaceShape::ByBranch),
            spec,
            live_state,
            branch_ref,
            variant: EntitySurfaceShape::ByBranch,
            branch_binding: BranchBinding::explicit(),
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
        let analyzer = EntityPrimaryKeyFilterAnalyzer::new(&self.spec);
        Ok(filters
            .iter()
            .map(|filter| {
                if ExactBranchIdFilterAnalyzer.supports(filter) || analyzer.supports(filter) {
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
        let mut request = entity_live_state_scan_request(
            &self.spec.schema_key,
            self.branch_binding.active_branch_id(),
            Some(projected_schema.as_ref()),
            limit,
        );
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        apply_exact_branch_id_filter(&mut request, exact_branch_ids_from_filters(filters)?);
        apply_exact_entity_pk_filters(&mut request, &self.spec, filters)?;

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
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("raw DataFusion INSERT is disabled; use the sql2 bound write pipeline")
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("raw DataFusion DELETE is disabled; use the sql2 bound write pipeline")
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("raw DataFusion UPDATE is disabled; use the sql2 bound write pipeline")
    }
}

fn entity_pks_from_primary_key_filters(
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<Option<Vec<EntityPk>>> {
    let analyzer = EntityPrimaryKeyFilterAnalyzer::new(spec);
    let mut entity_pks: Option<BTreeSet<EntityPk>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze(filter)? else {
            continue;
        };
        entity_pks = Some(match entity_pks {
            Some(existing_ids) => existing_ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }

    Ok(entity_pks.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_entity_pk_filters(
    request: &mut LiveStateScanRequest,
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<()> {
    if let Some(entity_pks) = entity_pks_from_primary_key_filters(spec, filters)? {
        if entity_pks.is_empty() {
            request.filter.rows = LiveStateRowFilter::None;
        }
        request.filter.entity_pks = entity_pks;
    }
    Ok(())
}

fn exact_branch_ids_from_filters(filters: &[Expr]) -> Result<Option<Vec<String>>> {
    let analyzer = ExactBranchIdFilterAnalyzer;
    let mut branch_ids: Option<BTreeSet<String>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze(filter)? else {
            continue;
        };
        branch_ids = Some(match branch_ids {
            Some(existing_ids) => existing_ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    Ok(branch_ids.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_branch_id_filter(
    request: &mut LiveStateScanRequest,
    branch_ids: Option<Vec<String>>,
) {
    if let Some(branch_ids) = branch_ids {
        if branch_ids.is_empty() {
            request.filter.rows = LiveStateRowFilter::None;
        }
        request.filter.branch_ids = branch_ids;
    }
}

struct EntityPrimaryKeyFilterAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
}

struct ExactBranchIdFilterAnalyzer;

impl ExactBranchIdFilterAnalyzer {
    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<String>>> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let Some(left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.intersection(&right).cloned().collect()))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let Some(mut left) = self.analyze(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze(&binary_expr.right)? else {
                    return Ok(None);
                };
                left.extend(right);
                Ok(Some(left))
            }
            Expr::BinaryExpr(binary_expr) => {
                Ok(branch_id_from_binary_filter(binary_expr).map(|value| BTreeSet::from([value])))
            }
            Expr::InList(in_list) => {
                Ok(branch_ids_from_in_list_filter(in_list)
                    .map(|values| values.into_iter().collect()))
            }
            _ => Ok(None),
        }
    }
}

fn branch_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    branch_id_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| branch_id_from_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn branch_ids_from_in_list_filter(in_list: &InList) -> Option<Vec<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "lixcol_branch_id" {
        return None;
    }

    let values = in_list
        .list
        .iter()
        .map(string_expr_literal)
        .collect::<Option<Vec<_>>>()?;
    if values.is_empty() {
        return None;
    }
    Some(values)
}

fn branch_id_from_column_literal_filter(column_expr: &Expr, literal_expr: &Expr) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "lixcol_branch_id" {
        return None;
    }
    string_expr_literal(literal_expr)
}

impl<'a> EntityPrimaryKeyFilterAnalyzer<'a> {
    fn new(spec: &'a EntitySurfaceSpec) -> Self {
        Self {
            primary_key_columns: string_primary_key_columns(spec),
        }
    }

    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<EntityPk>>> {
        if self.primary_key_columns.is_empty() {
            return Ok(None);
        };
        let Some(constraint) = self.analyze_constraint(expr)? else {
            return Ok(None);
        };
        Ok(constraint.into_entity_pks(&self.primary_key_columns))
    }

    fn analyze_constraint(&self, expr: &Expr) -> Result<Option<EntityPkConstraint>> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let Some(left) = self.analyze_constraint(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze_constraint(&binary_expr.right)? else {
                    return Ok(None);
                };
                Ok(Some(left.intersect(right, &self.primary_key_columns)))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let Some(left) = self.analyze_constraint(&binary_expr.left)? else {
                    return Ok(None);
                };
                let Some(right) = self.analyze_constraint(&binary_expr.right)? else {
                    return Ok(None);
                };
                let Some(left_ids) = left.into_entity_pks(&self.primary_key_columns) else {
                    return Ok(None);
                };
                let Some(mut right_ids) = right.into_entity_pks(&self.primary_key_columns) else {
                    return Ok(None);
                };
                right_ids.extend(left_ids);
                Ok(Some(EntityPkConstraint::Full(right_ids)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(entity_pk_constraint_from_binary_filter(
                binary_expr,
                &self.primary_key_columns,
            )),
            Expr::InList(in_list) => Ok(entity_pk_constraint_from_in_list_filter(
                in_list,
                &self.primary_key_columns,
            )),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntityPkConstraint {
    Full(BTreeSet<EntityPk>),
    Parts(BTreeMap<String, BTreeSet<String>>),
}

impl EntityPkConstraint {
    fn intersect(self, other: Self, primary_key_columns: &[&str]) -> Self {
        match (self, other) {
            (Self::Full(left), Self::Full(right)) => {
                Self::Full(left.intersection(&right).cloned().collect())
            }
            (Self::Full(ids), Self::Parts(parts)) | (Self::Parts(parts), Self::Full(ids)) => {
                Self::Full(
                    ids.into_iter()
                        .filter(|identity| {
                            identity_matches_parts(identity, primary_key_columns, &parts)
                        })
                        .collect(),
                )
            }
            (Self::Parts(mut left), Self::Parts(right)) => {
                for (column, right_values) in right {
                    left.entry(column)
                        .and_modify(|left_values| {
                            *left_values =
                                left_values.intersection(&right_values).cloned().collect();
                        })
                        .or_insert(right_values);
                }
                Self::Parts(left)
            }
        }
    }

    fn into_entity_pks(self, primary_key_columns: &[&str]) -> Option<BTreeSet<EntityPk>> {
        match self {
            Self::Full(ids) => Some(ids),
            Self::Parts(parts) => entity_pks_from_primary_key_parts(primary_key_columns, parts),
        }
    }
}

fn string_primary_key_columns(spec: &EntitySurfaceSpec) -> Vec<&str> {
    spec.primary_key_paths
        .iter()
        .map(|path| {
            let [column_name] = path.as_slice() else {
                return None;
            };
            let column = spec.visible_column(column_name)?;
            (column.column_type == EntityColumnType::String).then_some(column.name.as_str())
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

fn entity_pk_constraint_from_binary_filter(
    binary_expr: &BinaryExpr,
    primary_key_columns: &[&str],
) -> Option<EntityPkConstraint> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    entity_pk_constraint_from_column_literal_filter(
        &binary_expr.left,
        &binary_expr.right,
        primary_key_columns,
    )
    .or_else(|| {
        entity_pk_constraint_from_column_literal_filter(
            &binary_expr.right,
            &binary_expr.left,
            primary_key_columns,
        )
    })
}

fn entity_pk_constraint_from_in_list_filter(
    in_list: &InList,
    primary_key_columns: &[&str],
) -> Option<EntityPkConstraint> {
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
    match column.name.as_str() {
        "lixcol_entity_pk" => values
            .into_iter()
            .map(|value| EntityPk::from_json_array_text(&value).ok())
            .collect::<Option<BTreeSet<_>>>()
            .map(EntityPkConstraint::Full),
        column_name if primary_key_columns.contains(&column_name) => {
            Some(EntityPkConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                values.into_iter().collect(),
            )])))
        }
        _ => None,
    }
}

fn entity_pk_constraint_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    primary_key_columns: &[&str],
) -> Option<EntityPkConstraint> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let value = string_expr_literal(literal_expr)?;
    match column.name.as_str() {
        "lixcol_entity_pk" => EntityPk::from_json_array_text(&value)
            .ok()
            .map(|identity| EntityPkConstraint::Full(BTreeSet::from([identity]))),
        column_name if primary_key_columns.contains(&column_name) => {
            Some(EntityPkConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                BTreeSet::from([value]),
            )])))
        }
        _ => None,
    }
}

fn entity_pks_from_primary_key_parts(
    primary_key_columns: &[&str],
    parts: BTreeMap<String, BTreeSet<String>>,
) -> Option<BTreeSet<EntityPk>> {
    if primary_key_columns
        .iter()
        .any(|column| !parts.contains_key(*column))
    {
        return None;
    }

    let mut identities = BTreeSet::from([Vec::<String>::new()]);
    for column in primary_key_columns {
        let values = parts.get(*column)?;
        identities = identities
            .into_iter()
            .flat_map(|prefix| {
                values.iter().map(move |value| {
                    let mut parts = prefix.clone();
                    parts.push(value.clone());
                    parts
                })
            })
            .collect();
    }
    Some(
        identities
            .into_iter()
            .map(|parts| EntityPk { parts })
            .collect(),
    )
}

fn identity_matches_parts(
    identity: &EntityPk,
    primary_key_columns: &[&str],
    parts: &BTreeMap<String, BTreeSet<String>>,
) -> bool {
    let identity_parts = identity.parts.as_slice();
    primary_key_columns
        .iter()
        .zip(identity_parts.iter())
        .all(|(column, value)| {
            parts
                .get(*column)
                .is_none_or(|values| values.contains(value))
        })
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
            let rows = live_state
                .scan_rows(&request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
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
    active_branch_id: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![schema_key.to_string()],
            branch_ids: active_branch_id
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: entity_live_state_projection(projected_schema),
        limit,
    }
}

fn entity_live_state_projection(projected_schema: Option<&Schema>) -> LiveStateProjection {
    let Some(schema) = projected_schema else {
        return LiveStateProjection::default();
    };
    let mut columns = projection_column_names(schema);
    if schema
        .fields()
        .iter()
        .any(|field| !field.name().starts_with("lixcol_"))
        && !columns.iter().any(|column| column == "snapshot_content")
    {
        columns.push("snapshot_content".to_string());
    }
    LiveStateProjection { columns }
}

fn projection_column_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|field| field.name().strip_prefix("lixcol_"))
        .map(str::to_string)
        .collect()
}

fn entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &[MaterializedLiveStateRow],
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
    rows: &[MaterializedLiveStateRow],
    snapshots: &[Option<JsonValue>],
) -> Result<ArrayRef> {
    if let Some(property_name) = column_name.strip_prefix("lixcol_") {
        return entity_system_column_array(property_name, rows);
    }

    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 entity provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;

    let values = snapshots
        .iter()
        .map(|snapshot| snapshot.as_ref().and_then(|value| value.get(column_name)))
        .collect::<Vec<_>>();
    Ok(match column_type {
        EntityColumnType::String | EntityColumnType::Json => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| entity_json_text_value(*value, column_type))
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

fn entity_system_column_array(
    column_name: &str,
    rows: &[MaterializedLiveStateRow],
) -> Result<ArrayRef> {
    Ok(match column_name {
        "entity_pk" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.entity_pk
                        .as_json_array_text()
                        .map(Some)
                        .map_err(lix_error_to_datafusion_error)
                })
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        "schema_key" => string_array(rows.iter().map(|row| Some(row.schema_key.as_str()))),
        "file_id" => string_array(rows.iter().map(|row| row.file_id.as_deref())),
        "snapshot_content" => string_array(rows.iter().map(|row| row.snapshot_content.as_deref())),
        "metadata" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.metadata.as_ref().map(serialize_row_metadata))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
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
        "branch_id" => string_array(rows.iter().map(|row| Some(row.branch_id.as_str()))),
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

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };
    Ok(Arc::new(schema.project(projection)?))
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    crate::sql2::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    use super::entity_record_batch;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::catalog::{
        derive_entity_surface_spec_from_schema, entity_surface_schema,
        schema_exposed_as_entity_surface, EntityColumnType, EntitySurfaceShape,
    };
    use crate::LixError;

    struct EmptyLiveStateReader;
    struct EmptyBranchRefReader;

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(vec![])
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait]
    impl BranchRefReader for EmptyBranchRefReader {
        async fn load_head(&self, _branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new())
        }
    }

    fn empty_branch_ref() -> Arc<dyn BranchRefReader> {
        Arc::new(EmptyBranchRefReader)
    }

    fn live_row() -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("entity-1"),
            schema_key: "project_message".to_string(),
            file_id: None,
            snapshot_content: Some(
                "{\"body\":\"hello\",\"rating\":4.5,\"count\":7,\"enabled\":true,\"meta\":{\"x\":1}}"
                    .to_string(),
            ),
            metadata: Some(json!({"source": "test"}).to_string()),
            deleted: false,
            branch_id: "branch-a".to_string(),
            change_id: Some("change-a".to_string()),
            commit_id: Some("commit-a".to_string()),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn entity_insert_spec_with_primary_key() -> Arc<super::EntitySurfaceSpec> {
        Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
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
    fn excludes_non_entity_builtin_session_surfaces() {
        assert!(!schema_exposed_as_entity_surface("lix_active_account"));
        assert!(schema_exposed_as_entity_surface("project_message"));
    }

    #[test]
    fn derives_entity_surface_spec_from_schema_definition() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" },
                "rating": { "type": "number" },
                "meta": { "type": "object" },
                "lixcol_entity_pk": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_column_names().collect::<Vec<_>>(),
            vec!["body", "meta", "rating"]
        );
        assert_eq!(
            spec.visible_column("body").map(|column| column.column_type),
            Some(EntityColumnType::String)
        );
        assert_eq!(
            spec.visible_column("rating")
                .map(|column| column.column_type),
            Some(EntityColumnType::Number)
        );
        assert_eq!(
            spec.visible_column("meta").map(|column| column.column_type),
            Some(EntityColumnType::Json)
        );
        assert!(spec.visible_column("lixcol_entity_pk").is_none());
    }

    #[test]
    fn entity_surface_spec_rejects_properties_without_projection_type() {
        let error = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "kind": {}
            },
            "required": ["id", "kind"],
            "additionalProperties": false
        }))
        .expect_err("unprojectable property should be rejected");

        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(
            error.message.contains("property '/kind'"),
            "error should identify the property: {error:?}"
        );
    }

    #[test]
    fn by_branch_schema_includes_branch_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntitySurfaceShape::ByBranch);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_pk").is_ok());
        assert!(schema.field_with_name("lixcol_branch_id").is_ok());
    }

    #[test]
    fn active_schema_excludes_branch_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_pk").is_ok());
        assert!(schema.field_with_name("lixcol_branch_id").is_err());
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

        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        assert!(
            schema
                .field_with_name("id")
                .expect("id field")
                .is_nullable(),
            "defaulted primary-key property should be nullable at SQL input"
        );
        assert!(
            schema
                .field_with_name("lixcol_entity_pk")
                .expect("entity pk field")
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
        let schema = entity_surface_schema(&spec, EntitySurfaceShape::ByBranch);

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
                .column_by_name("lixcol_entity_pk")
                .expect("entity pk column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("entity pk is string")
                .value(0),
            "[\"entity-1\"]"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch id column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("branch id is string")
                .value(0),
            "branch-a"
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
        let provider = super::EntityProvider::by_branch(
            spec,
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
            empty_branch_ref(),
        );

        assert!(provider.schema.field_with_name("lixcol_branch_id").is_ok());
    }

    #[test]
    fn primary_key_filters_route_entity_pks_for_string_primary_key() {
        let spec = entity_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("id", "entity-a"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("entity-b"), string_literal("entity-a")],
                false,
            )),
        ];

        let entity_pks = super::entity_pks_from_primary_key_filters(&spec, &filters)
            .expect("primary-key filters should analyze")
            .expect("primary-key filters should produce a constraint");

        assert_eq!(
            entity_pks,
            vec![crate::entity_pk::EntityPk::single("entity-a")]
        );
    }

    #[test]
    fn primary_key_filter_analyzer_models_boolean_predicates() {
        let spec = entity_insert_spec_with_primary_key();
        let analyzer = super::EntityPrimaryKeyFilterAnalyzer::new(&spec);
        let disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "entity-a")),
            Operator::Or,
            Box::new(eq_filter("id", "entity-b")),
        ));
        let contradiction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "entity-a")),
            Operator::And,
            Box::new(eq_filter("id", "entity-b")),
        ));

        let disjunction_ids = analyzer
            .analyze(&disjunction)
            .expect("OR should analyze")
            .expect("OR should produce an entity-pk set");
        let contradiction_ids = analyzer
            .analyze(&contradiction)
            .expect("AND should analyze")
            .expect("AND should produce an entity-pk set");

        assert_eq!(
            disjunction_ids.into_iter().collect::<Vec<_>>(),
            vec![
                crate::entity_pk::EntityPk::single("entity-a"),
                crate::entity_pk::EntityPk::single("entity-b"),
            ]
        );
        assert!(contradiction_ids.is_empty());
    }

    #[test]
    fn primary_key_filters_ignore_non_key_and_negated_predicates() {
        let spec = entity_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("body", "hello"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("entity-a")],
                true,
            )),
        ];

        assert!(super::entity_pks_from_primary_key_filters(&spec, &filters)
            .expect("ignored filters should analyze")
            .unwrap_or_default()
            .is_empty());
    }
}
