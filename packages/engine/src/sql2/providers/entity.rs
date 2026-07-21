use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DataFusionError, Result, ScalarValue, not_impl_err};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use futures_util::FutureExt;
use serde_json::Value as JsonValue;

use crate::branch::BranchRefReader;
use crate::commit_graph::CommitGraphReader;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateRowFilter, LiveStateScanRequest,
};
use crate::sql2::branch_scope::{BranchBinding, resolve_provider_branch_ids};
use crate::sql2::catalog::{
    EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec, PublicCatalog, PublicSurfaceKind,
    entity_surface_schema,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::{GLOBAL_BRANCH_ID, LixError, parse_row_metadata_value};

use crate::sql2::{
    SqlHistoryQuerySource, SqlWriteContext, WriteAccess, WriteContextLiveStateReader,
};
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};

use super::entity_history::register_entity_history_surface;
use datafusion::physical_plan::ExecutionPlan;

use super::columns::{ColumnTableError, LIVE_STATE_COLS, build_array};
use super::spec::{
    InsertApply, PlannedDml, PlannedScan, TableSpec, projected_schema, register_spec_table,
    row_source,
};
use super::values::{
    optional_bool_value, optional_string_value, required_string_value, string_expr_literal,
};
use crate::storage_adapter::StorageAdapterRead;

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
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::active(
                        spec,
                        Arc::clone(&live_state),
                        Arc::clone(&branch_ref),
                        active_branch_id.to_string(),
                    )),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::by_branch(
                        spec,
                        Arc::clone(&live_state),
                        Arc::clone(&branch_ref),
                    )),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityHistory { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_entity_history_surface(
                    ctx,
                    &surface.name,
                    spec,
                    Arc::clone(&commit_graph),
                    query_source.clone(),
                )?;
            }
            _ => {}
        }
    }

    Ok(())
}

pub(crate) async fn register_entity_write_providers(
    ctx: &SessionContext,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
    catalog: &PublicCatalog,
) -> Result<(), LixError> {
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::active_with_write(
                        spec,
                        write_ctx.clone(),
                        Arc::clone(&branch_ref),
                    )),
                    WriteAccess::write(write_ctx.clone()),
                )?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::by_branch_with_write(
                        spec,
                        write_ctx.clone(),
                        Arc::clone(&branch_ref),
                    )),
                    WriteAccess::write(write_ctx.clone()),
                )?;
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

/// One spec type covers every registered entity schema: the runtime
/// [`EntitySurfaceSpec`] carries the per-schema column layout, and the
/// surface name follows the catalog naming for the base/by-branch shapes.
struct EntitySpec {
    surface_name: String,
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateReader>,
    branch_ref: Arc<dyn BranchRefReader>,
    schema: SchemaRef,
    branch_binding: BranchBinding,
}

impl EntitySpec {
    fn active(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        active_branch_id: String,
    ) -> Self {
        Self {
            surface_name: spec.schema_key.clone(),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::Active),
            spec,
            live_state,
            branch_ref,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn active_with_write(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx));
        Self::active(spec, live_state, branch_ref, active_branch_id)
    }

    fn by_branch(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::ByBranch),
            spec,
            live_state,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
        }
    }

    fn by_branch_with_write(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx));
        Self::by_branch(spec, live_state, branch_ref)
    }

    /// Plan-time scan derivation shared by `plan_scan` and the unit tests:
    /// the projected output schema, the live-state scan request (with branch
    /// routing resolved), and the residual snapshot row filters.
    async fn plan_scan_parts(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(SchemaRef, LiveStateScanRequest, Vec<EntityRowFilter>)> {
        let projected_schema = projected_schema(&self.schema, projection);
        let row_filters = EntityRowFilterAnalyzer::new(&self.spec).analyze_filters(filters)?;
        let mut request = entity_live_state_scan_request(
            &self.spec.schema_key,
            self.branch_binding.active_branch_id(),
            Some(projected_schema.as_ref()),
            if row_filters.is_empty() { limit } else { None },
            !row_filters.is_empty(),
        );
        let exact_branch_ids = exact_branch_ids_from_filters(filters)?;
        // Preserve an exact by-branch selector before resolving an explicit
        // provider scope. Resolving first would enumerate every branch even
        // when the DELETE has an exact `lixcol_branch_id = ...` predicate,
        // and write contexts intentionally only expose point branch-head
        // lookups. Active surfaces retain their existing post-resolution
        // filtering behavior so a branch overlay is still constructed first.
        if matches!(&self.branch_binding, BranchBinding::Explicit) {
            apply_exact_branch_id_filter(&mut request, exact_branch_ids.clone());
        }
        request.filter.branch_ids = resolve_provider_branch_ids(
            self.branch_ref.as_ref(),
            &self.branch_binding,
            request.filter.branch_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        apply_exact_branch_id_filter(&mut request, exact_branch_ids);
        apply_exact_entity_pk_filters(&mut request, &self.spec, filters)?;
        Ok((projected_schema, request, row_filters))
    }
}

#[async_trait]
impl TableSpec for EntitySpec {
    fn table_name(&self) -> &str {
        &self.surface_name
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        let primary_key_analyzer = EntityPrimaryKeyFilterAnalyzer::new(&self.spec);
        let row_filter_analyzer = EntityRowFilterAnalyzer::new(&self.spec);
        if ExactBranchIdFilterAnalyzer.supports(filter)
            || primary_key_analyzer.supports(filter)
            || row_filter_analyzer.supports(filter)
        {
            TableProviderFilterPushDown::Exact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let (schema, request, row_filters) =
            self.plan_scan_parts(projection, filters, limit).await?;
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            load: row_source(
                (
                    Arc::clone(&self.spec),
                    Arc::clone(&self.live_state),
                    schema,
                    request,
                    row_filters,
                ),
                |(spec, live_state, schema, request, row_filters)| async move {
                    let mut rows = live_state
                        .scan_rows(&request)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    apply_entity_row_filters(&mut rows, &row_filters)?;
                    entity_record_batch(&spec, schema, &rows)
                },
            ),
        })
    }

    // Rejects at plan time so validate-only
    // flows fail before the INSERT input plan executes; an exec-time rejection
    // in stage_insert would let empty-branch-scope statements short-circuit to
    // a silent 0-row success.
    async fn plan_insert(
        &self,
        _write_ctx: SqlWriteContext,
        _input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        not_impl_err!("raw DataFusion INSERT is disabled; use the sql2 bound write pipeline")
    }

    async fn plan_delete(
        &self,
        write_ctx: SqlWriteContext,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        reject_read_only_entity_surface(&self.spec.schema_key, "DELETE")?;
        if self.spec.schema_key == "lix_registered_schema" {
            return Err(lix_error_to_datafusion_error(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "delete lix_registered_schema is not supported",
            )));
        }
        if !filters.iter().any(contains_like_filter) {
            return not_impl_err!(
                "raw DataFusion DELETE is disabled; use the sql2 bound write pipeline"
            );
        }
        let (schema, request, row_filters) = self.plan_scan_parts(None, filters, None).await?;
        let source = row_source(
            (
                Arc::clone(&self.spec),
                Arc::clone(&self.live_state),
                schema,
                request,
                row_filters,
            ),
            |(spec, live_state, schema, request, row_filters)| async move {
                let mut rows = live_state
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                apply_entity_row_filters(&mut rows, &row_filters)?;
                entity_record_batch(&spec, schema, &rows)
            },
        );
        let spec = Arc::clone(&self.spec);
        let branch_binding = self.branch_binding.clone();
        Ok(PlannedDml {
            source,
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let spec = Arc::clone(&spec);
                let branch_binding = branch_binding.clone();
                async move {
                    let rows = entity_delete_stage_rows_from_batch(
                        &matched_batch,
                        spec.as_ref(),
                        &branch_binding,
                    )?;
                    let count = u64::try_from(rows.len()).map_err(|_| {
                        DataFusionError::Execution("DELETE row count overflow".to_string())
                    })?;
                    if count > 0 {
                        write_ctx
                            .stage_write(TransactionWrite::Rows {
                                mode: TransactionWriteMode::Replace,
                                rows,
                            })
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                    }
                    Ok(count)
                }
                .boxed()
            }),
        })
    }

    async fn plan_update(
        &self,
        _write_ctx: SqlWriteContext,
        _assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        _filters: &[Expr],
    ) -> Result<PlannedDml> {
        not_impl_err!("raw DataFusion UPDATE is disabled; use the sql2 bound write pipeline")
    }
}

fn contains_like_filter(expr: &Expr) -> bool {
    match expr {
        Expr::Like(_) => true,
        Expr::BinaryExpr(binary) => {
            contains_like_filter(&binary.left) || contains_like_filter(&binary.right)
        }
        _ => false,
    }
}

fn entity_delete_stage_rows_from_batch(
    batch: &RecordBatch,
    spec: &EntitySurfaceSpec,
    branch_binding: &BranchBinding,
) -> Result<Vec<TransactionWriteRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let global = optional_bool_value(
                batch,
                row_index,
                "lixcol_global",
                "DELETE FROM entity surface",
            )?
            .unwrap_or(false);
            let source_branch_id = optional_string_value(
                batch,
                row_index,
                "lixcol_branch_id",
                "DELETE FROM entity surface",
            )?;
            if matches!(branch_binding, BranchBinding::Explicit)
                && global
                && source_branch_id.as_deref() != Some(GLOBAL_BRANCH_ID)
            {
                return Err(DataFusionError::Execution(
                    "DELETE through an entity by-branch surface cannot mutate a projected global row"
                        .to_string(),
                ));
            }
            let branch_id = if global {
                GLOBAL_BRANCH_ID.to_string()
            } else {
                source_branch_id
                    .or_else(|| branch_binding.active_branch_id().map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "DELETE FROM entity by-branch requires lixcol_branch_id".to_string(),
                        )
                    })?
            };
            let entity_pk = EntityPk::from_json_array_text(&required_string_value(
                batch,
                row_index,
                "lixcol_entity_pk",
                "DELETE FROM entity surface",
            )?)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "DELETE FROM entity surface has invalid lixcol_entity_pk: {error}"
                ))
            })?;
            let metadata = optional_string_value(
                batch,
                row_index,
                "lixcol_metadata",
                "DELETE FROM entity surface",
            )?
            .map(|value| {
                let metadata =
                    parse_row_metadata_value(&value, &spec.schema_key).map_err(lix_error_to_datafusion_error)?;
                TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
                    .map_err(lix_error_to_datafusion_error)
            })
            .transpose()?;

            Ok(TransactionWriteRow {
                entity_pk: Some(entity_pk),
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(
                    batch,
                    row_index,
                    "lixcol_file_id",
                    "DELETE FROM entity surface",
                )?,
                snapshot: None,
                metadata,
                origin: None,
                created_at: None,
                updated_at: None,
                global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool_value(
                    batch,
                    row_index,
                    "lixcol_untracked",
                    "DELETE FROM entity surface",
                )?
                .unwrap_or(false),
                branch_id,
            })
        })
        .collect()
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

struct EntityRowFilterAnalyzer<'a> {
    spec: &'a EntitySurfaceSpec,
}

struct ExactBranchIdFilterAnalyzer;

impl ExactBranchIdFilterAnalyzer {
    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    #[expect(clippy::self_only_used_in_recursion)]
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
        }
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

impl<'a> EntityRowFilterAnalyzer<'a> {
    fn new(spec: &'a EntitySurfaceSpec) -> Self {
        Self { spec }
    }

    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr).is_some()
    }

    #[expect(clippy::unnecessary_wraps)]
    fn analyze_filters(&self, filters: &[Expr]) -> Result<Vec<EntityRowFilter>> {
        Ok(filters
            .iter()
            .filter_map(|filter| self.analyze(filter))
            .collect())
    }

    fn analyze(&self, expr: &Expr) -> Option<EntityRowFilter> {
        match expr {
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let left = self.analyze(&binary_expr.left)?;
                let right = self.analyze(&binary_expr.right)?;
                Some(EntityRowFilter::And(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let left = self.analyze(&binary_expr.left)?;
                let right = self.analyze(&binary_expr.right)?;
                Some(EntityRowFilter::Or(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary_expr) => self.analyze_binary(binary_expr),
            Expr::InList(in_list) => self.analyze_in_list(in_list),
            _ => None,
        }
    }

    fn analyze_binary(&self, binary_expr: &BinaryExpr) -> Option<EntityRowFilter> {
        if binary_expr.op != Operator::Eq {
            return None;
        }
        self.analyze_column_literal(&binary_expr.left, &binary_expr.right)
            .or_else(|| self.analyze_column_literal(&binary_expr.right, &binary_expr.left))
    }

    fn analyze_in_list(&self, in_list: &InList) -> Option<EntityRowFilter> {
        if in_list.negated {
            return None;
        }
        let Expr::Column(column) = in_list.expr.as_ref() else {
            return None;
        };
        let column_name = self.filterable_column_name(&column.name)?;
        let values = in_list
            .list
            .iter()
            .map(entity_filter_value_literal)
            .collect::<Option<Vec<_>>>()?;
        if values.is_empty() {
            return None;
        }
        Some(EntityRowFilter::ColumnIn {
            column: column_name.to_string(),
            column_type: self
                .spec
                .visible_column(column_name)
                .expect("filterable column should exist")
                .column_type,
            values,
        })
    }

    fn analyze_column_literal(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
    ) -> Option<EntityRowFilter> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        let column_name = self.filterable_column_name(&column.name)?;
        Some(EntityRowFilter::ColumnEq {
            column: column_name.to_string(),
            column_type: self
                .spec
                .visible_column(column_name)
                .expect("filterable column should exist")
                .column_type,
            value: entity_filter_value_literal(literal_expr)?,
        })
    }

    fn filterable_column_name(&self, column_name: &str) -> Option<&str> {
        let column = self.spec.visible_column(column_name)?;
        match column.column_type {
            EntityColumnType::String
            | EntityColumnType::Boolean
            | EntityColumnType::Integer
            | EntityColumnType::Number => Some(column.name.as_str()),
            EntityColumnType::Json => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum EntityFilterValue {
    Boolean(bool),
    Integer(i64),
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
enum EntityRowFilter {
    ColumnEq {
        column: String,
        column_type: EntityColumnType,
        value: EntityFilterValue,
    },
    ColumnIn {
        column: String,
        column_type: EntityColumnType,
        values: Vec<EntityFilterValue>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl EntityRowFilter {
    fn matches_snapshot(&self, snapshot: Option<&JsonValue>) -> bool {
        match self {
            Self::ColumnEq {
                column,
                column_type,
                value,
            } => entity_snapshot_value(snapshot, column, *column_type)
                .is_some_and(|actual| entity_filter_values_equal(&actual, value, *column_type)),
            Self::ColumnIn {
                column,
                column_type,
                values,
            } => entity_snapshot_value(snapshot, column, *column_type).is_some_and(|actual| {
                values
                    .iter()
                    .any(|expected| entity_filter_values_equal(&actual, expected, *column_type))
            }),
            Self::And(left, right) => {
                left.matches_snapshot(snapshot) && right.matches_snapshot(snapshot)
            }
            Self::Or(left, right) => {
                left.matches_snapshot(snapshot) || right.matches_snapshot(snapshot)
            }
        }
    }
}

fn entity_filter_value_literal(expr: &Expr) -> Option<EntityFilterValue> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Boolean(Some(value)) => Some(EntityFilterValue::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int64(Some(value)) => Some(EntityFilterValue::Integer(*value)),
        ScalarValue::UInt8(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Some(EntityFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt64(Some(value)) => {
            i64::try_from(*value).ok().map(EntityFilterValue::Integer)
        }
        ScalarValue::Float32(Some(value)) => Some(EntityFilterValue::Number(f64::from(*value))),
        ScalarValue::Float64(Some(value)) => Some(EntityFilterValue::Number(*value)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(EntityFilterValue::String(value.clone())),
        _ => None,
    }
}

fn entity_snapshot_value(
    snapshot: Option<&JsonValue>,
    column: &str,
    column_type: EntityColumnType,
) -> Option<EntityFilterValue> {
    let value = snapshot?.get(column)?;
    match column_type {
        EntityColumnType::String => match value {
            JsonValue::String(value) => Some(EntityFilterValue::String(value.clone())),
            _ => None,
        },
        EntityColumnType::Integer => entity_i64_value(Some(value)).map(EntityFilterValue::Integer),
        EntityColumnType::Number => entity_f64_value(Some(value)).map(EntityFilterValue::Number),
        EntityColumnType::Boolean => value.as_bool().map(EntityFilterValue::Boolean),
        EntityColumnType::Json => None,
    }
}

#[expect(clippy::cast_precision_loss, clippy::float_cmp)]
fn entity_filter_values_equal(
    actual: &EntityFilterValue,
    expected: &EntityFilterValue,
    column_type: EntityColumnType,
) -> bool {
    match (column_type, actual, expected) {
        (
            EntityColumnType::Number,
            EntityFilterValue::Number(actual),
            EntityFilterValue::Integer(expected),
        ) => *actual == *expected as f64,
        (
            EntityColumnType::Number,
            EntityFilterValue::Integer(actual),
            EntityFilterValue::Number(expected),
        ) => *actual as f64 == *expected,
        _ => actual == expected,
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

fn apply_entity_row_filters(
    rows: &mut Vec<MaterializedLiveStateRow>,
    filters: &[EntityRowFilter],
) -> Result<()> {
    if filters.is_empty() {
        return Ok(());
    }
    let mut filtered_rows = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
            DataFusionError::External(Box::new(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "entity scan filter could not parse snapshot_content for schema '{}' entity_pk '{:?}': {error}",
                    row.schema_key, row.entity_pk
                ),
            )))
        })?;
        if filters
            .iter()
            .all(|filter| filter.matches_snapshot(Some(&snapshot)))
        {
            filtered_rows.push(row);
        }
    }
    *rows = filtered_rows;
    Ok(())
}

fn entity_live_state_scan_request(
    schema_key: &str,
    active_branch_id: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
    force_snapshot_content: bool,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![schema_key.to_string()],
            branch_ids: active_branch_id
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..LiveStateFilter::default()
        },
        projection: entity_live_state_projection(projected_schema, force_snapshot_content),
        limit,
    }
}

fn entity_live_state_projection(
    projected_schema: Option<&Schema>,
    force_snapshot_content: bool,
) -> LiveStateProjection {
    let Some(schema) = projected_schema else {
        return LiveStateProjection::default();
    };
    let mut columns = projection_column_names(schema);
    if (force_snapshot_content
        || schema
            .fields()
            .iter()
            .any(|field| !field.name().starts_with("lixcol_")))
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

#[expect(trivial_casts)]
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

/// `lixcol_*` system columns share their accessors with the lix_state
/// surface: strip the prefix and materialize via [`LIVE_STATE_COLS`].
fn entity_system_column_array(
    column_name: &str,
    rows: &[MaterializedLiveStateRow],
) -> Result<ArrayRef> {
    let col = LIVE_STATE_COLS.col(column_name).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "sql2 entity provider does not support system column 'lixcol_{column_name}'"
        ))
    })?;
    build_array(col, rows).map_err(entity_system_column_error)
}

/// Map [`ColumnTableError`] onto entity's existing error surface. Only the
/// `Row` variant is reachable from [`entity_system_column_array`] (the column
/// lookup happens before the build); the rest are mapped for completeness.
fn entity_system_column_error(error: ColumnTableError) -> DataFusionError {
    match error {
        ColumnTableError::Row(error) => lix_error_to_datafusion_error(error),
        ColumnTableError::UnsupportedColumn(other) => DataFusionError::Execution(format!(
            "sql2 entity provider does not support system column 'lixcol_{other}'"
        )),
        ColumnTableError::Arrow(error) | ColumnTableError::ArrowZeroColumn(error) => {
            DataFusionError::from(error)
        }
    }
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
        (_, None | Some(JsonValue::Null)) => None,
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

#[cfg(test)]
#[expect(trivial_casts)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::catalog::TableProvider;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    use super::super::spec::SpecTableProvider;
    use super::entity_record_batch;
    use crate::LixError;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::WriteAccess;
    use crate::sql2::catalog::{
        EntityColumnType, EntitySurfaceShape, derive_entity_surface_spec_from_schema,
        entity_surface_schema, schema_exposed_as_entity_surface,
    };

    struct EmptyLiveStateReader;
    struct EmptyBranchRefReader;

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

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

    #[derive(Default)]
    struct DummyWriteContext;

    #[async_trait]
    impl crate::sql2::SqlWriteExecutionContext for DummyWriteContext {
        #[expect(clippy::unnecessary_literal_bound)]
        fn active_branch_id(&self) -> &str {
            "branch-a"
        }

        fn functions(&self) -> crate::functions::FunctionProviderHandle {
            crate::functions::FunctionProviderHandle::system()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(Vec::new())
        }

        async fn load_branch_head(
            &mut self,
            branch_id: &str,
        ) -> Result<Option<CommitId>, LixError> {
            Ok(Some(CommitId::for_test_label(&format!(
                "commit-{branch_id}"
            ))))
        }

        async fn stage_write(
            &mut self,
            _write: crate::transaction::types::TransactionWrite,
        ) -> Result<crate::transaction::types::TransactionWriteOutcome, LixError> {
            panic!("raw DataFusion entity INSERT must never stage writes");
        }
    }

    // Guards the plan-time phase of the entity INSERT rejection: validate-only
    // flows rely on `insert_into` failing before the input plan executes, and
    // an exec-time rejection would let empty-branch-scope statements
    // short-circuit into a silent 0-row success.
    #[tokio::test]
    async fn insert_into_rejects_raw_datafusion_inserts_at_plan_time() {
        let session = datafusion::prelude::SessionContext::new();
        let mut write_context = DummyWriteContext;
        let write_ctx = crate::sql2::SqlWriteContext::new(&mut write_context);
        let provider = SpecTableProvider::new(
            Arc::new(super::EntitySpec::active_with_write(
                entity_insert_spec_with_primary_key(),
                write_ctx.clone(),
                empty_branch_ref(),
            )),
            WriteAccess::write(write_ctx),
        );
        let input = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            provider.schema(),
        )) as Arc<dyn datafusion::physical_plan::ExecutionPlan>;

        let error = provider
            .insert_into(
                &session.state(),
                input,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect_err("raw DataFusion INSERT must be rejected at plan time");

        assert!(
            matches!(
                error,
                datafusion::common::DataFusionError::NotImplemented(_)
            ),
            "rejection should keep the NotImplemented error type: {error:?}"
        );
        assert!(
            error
                .to_string()
                .contains("raw DataFusion INSERT is disabled; use the sql2 bound write pipeline"),
            "unexpected error: {error}"
        );
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
            change_id: Some(ChangeId::for_test_label("change-a")),
            commit_id: Some(CommitId::for_test_label("commit-a")),
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

    fn filter_pushdown_spec() -> Arc<super::EntitySurfaceSpec> {
        Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "pushdown_note",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string" },
                    "score": { "type": "number" },
                    "meta": { "type": "object" }
                },
                "required": ["id", "kind", "score"]
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
    #[expect(clippy::float_cmp)]
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
        let provider = SpecTableProvider::new(
            Arc::new(super::EntitySpec::by_branch(
                spec,
                Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
                empty_branch_ref(),
            )),
            WriteAccess::read_only(),
        );

        assert!(
            provider
                .schema()
                .field_with_name("lixcol_branch_id")
                .is_ok()
        );
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

        assert!(
            super::entity_pks_from_primary_key_filters(&spec, &filters)
                .expect("ignored filters should analyze")
                .unwrap_or_default()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn payload_filter_scan_forces_snapshot_and_removes_pushed_limit() {
        let spec = filter_pushdown_spec();
        let provider = super::EntitySpec::by_branch(
            Arc::clone(&spec),
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
            empty_branch_ref(),
        );
        let entity_pk_index = provider
            .schema
            .index_of("lixcol_entity_pk")
            .expect("system entity-pk column should exist");
        let projection = vec![entity_pk_index];

        let (_schema, request, row_filters) = provider
            .plan_scan_parts(Some(&projection), &[eq_filter("kind", "todo")], Some(5))
            .await
            .expect("scan should plan");

        assert_eq!(request.limit, None);
        assert!(
            request
                .projection
                .columns
                .iter()
                .any(|column| column == "snapshot_content"),
            "filter-only payload column should force snapshot_content projection: {:?}",
            request.projection.columns
        );
        assert_eq!(
            row_filters,
            vec![super::EntityRowFilter::ColumnEq {
                column: "kind".to_string(),
                column_type: EntityColumnType::String,
                value: super::EntityFilterValue::String("todo".to_string()),
            }]
        );
    }

    #[tokio::test]
    async fn unsupported_payload_filter_keeps_limit_and_no_snapshot_projection() {
        let spec = filter_pushdown_spec();
        let provider = super::EntitySpec::by_branch(
            Arc::clone(&spec),
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
            empty_branch_ref(),
        );
        let entity_pk_index = provider
            .schema
            .index_of("lixcol_entity_pk")
            .expect("system entity-pk column should exist");
        let projection = vec![entity_pk_index];
        let range_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("score")),
            Operator::Gt,
            Box::new(Expr::Literal(ScalarValue::Float64(Some(5.0)), None)),
        ));

        let (_schema, request, row_filters) = provider
            .plan_scan_parts(Some(&projection), &[range_filter], Some(5))
            .await
            .expect("scan should plan");

        assert_eq!(request.limit, Some(5));
        assert!(
            !request
                .projection
                .columns
                .iter()
                .any(|column| column == "snapshot_content"),
            "unsupported payload filter should remain residual and not change projection: {:?}",
            request.projection.columns
        );
        assert!(row_filters.is_empty());
    }

    #[test]
    fn payload_row_filter_invalid_snapshot_errors() {
        let mut rows = vec![MaterializedLiveStateRow {
            snapshot_content: Some("{not-json".to_string()),
            ..live_row()
        }];
        let filters = vec![super::EntityRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: EntityColumnType::String,
            value: super::EntityFilterValue::String("hello".to_string()),
        }];

        let error = super::apply_entity_row_filters(&mut rows, &filters)
            .expect_err("invalid snapshot_content should surface as an error");

        assert!(
            error
                .to_string()
                .contains("could not parse snapshot_content"),
            "error should explain invalid snapshot_content: {error}"
        );
    }
}
