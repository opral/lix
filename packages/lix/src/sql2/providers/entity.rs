use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine as _;
#[cfg(test)]
use datafusion::arrow::array::Array;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, not_impl_err};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use futures_util::FutureExt;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::Value as JsonValue;

use crate::branch::BranchRefReader;
use crate::commit_graph::CommitGraphReader;
use crate::entity_pk::EntityPk;
use crate::hot_state::MaterializedHotStateBatch;
#[cfg(test)]
use crate::hot_state::MaterializedHotStateRow;
use crate::hot_state::{
    HotStateFilter, HotStateProjection, HotStateReader, HotStateRowFilter, HotStateScanRequest,
};
use crate::sql2::branch_scope::{BranchBinding, resolve_provider_branch_ids};
use crate::sql2::catalog::{
    EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec, PublicCatalog, PublicSurfaceKind,
    entity_surface_schema,
};
use crate::sql2::entity_projection::{
    EntityProjectionDecoder, entity_projection_error_to_datafusion_error,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};
use crate::sql2::write_normalization::{SqlCell, UpdateAssignmentValues, UpdateCell};
use crate::{GLOBAL_BRANCH_ID, LixError, parse_row_metadata_value};

use crate::sql2::{
    EntitySnapshotReader, SqlHistoryQuerySource, SqlWriteContext, WriteAccess,
    WriteContextHotStateReader,
};
use crate::transaction_types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::ProviderSelection;
use super::entity_history::register_entity_history_surface;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use futures_util::stream;

use super::spec::{
    DmlReturning, InsertApply, PlannedDml, PlannedScan, TableSpec,
    batch_stream_source_with_statistics_and_source, projected_schema, register_spec_table,
    row_source, scan_row_source, take_record_batch_rows,
};
use super::values::{
    optional_bool_value, optional_string_value, required_string_value, string_expr_literal,
};
use crate::storage_adapter::StorageAdapterRead;

pub(crate) async fn register_entity_providers<S>(
    ctx: &SessionContext,
    active_branch_id: &str,
    hot_state: Arc<dyn HotStateReader>,
    entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Option<Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>>,
    query_source: Option<SqlHistoryQuerySource<S>>,
    checkpoint_history_query_source: Option<SqlHistoryQuerySource<S>>,
    catalog: &PublicCatalog,
    include_write_surfaces: bool,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    for surface in catalog.surfaces() {
        if !selection.includes(surface) {
            continue;
        }
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } if include_write_surfaces => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::active(
                        spec,
                        Arc::clone(&hot_state),
                        Arc::clone(&branch_ref),
                        active_branch_id.to_string(),
                        entity_snapshot_reader.clone(),
                    )),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } if include_write_surfaces => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(EntitySpec::by_branch(
                        spec,
                        Arc::clone(&hot_state),
                        Arc::clone(&branch_ref),
                        entity_snapshot_reader.clone(),
                    )),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityHistory { schema_key } => {
                let selected_query_source =
                    if schema_key == crate::checkpoint::CHECKPOINT_SCHEMA_KEY {
                        checkpoint_history_query_source.as_ref()
                    } else {
                        query_source.as_ref()
                    };
                let (Some(commit_graph), Some(query_source)) =
                    (commit_graph.as_ref(), selected_query_source)
                else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "selected entity history provider is missing its history context",
                    ));
                };
                let spec = catalog_entity_spec(catalog, schema_key)?;
                register_entity_history_surface(
                    ctx,
                    &surface.name,
                    spec,
                    Arc::clone(commit_graph),
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
    selection: &ProviderSelection,
) -> Result<(), LixError> {
    for surface in catalog.surfaces() {
        if !selection.includes(surface) {
            continue;
        }
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
#[derive(Clone)]
struct EntitySpec {
    surface_name: String,
    spec: Arc<EntitySurfaceSpec>,
    hot_state: Arc<dyn HotStateReader>,
    entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    branch_ref: Arc<dyn BranchRefReader>,
    schema: SchemaRef,
    branch_binding: BranchBinding,
}

impl EntitySpec {
    fn active(
        spec: Arc<EntitySurfaceSpec>,
        hot_state: Arc<dyn HotStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        active_branch_id: String,
        entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    ) -> Self {
        Self {
            surface_name: spec.schema_key.clone(),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::Active),
            spec,
            hot_state,
            entity_snapshot_reader,
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
        let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx));
        Self::active(spec, hot_state, branch_ref, active_branch_id, None)
    }

    fn by_branch(
        spec: Arc<EntitySurfaceSpec>,
        hot_state: Arc<dyn HotStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::ByBranch),
            spec,
            hot_state,
            entity_snapshot_reader,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
        }
    }

    fn by_branch_with_write(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let hot_state = Arc::new(WriteContextHotStateReader::new(write_ctx));
        Self::by_branch(spec, hot_state, branch_ref, None)
    }

    /// Plan-time scan derivation shared by `plan_scan` and the unit tests:
    /// the projected output schema, the live-state scan request (with branch
    /// routing resolved), and the residual snapshot row filters.
    async fn plan_scan_parts(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(SchemaRef, HotStateScanRequest, Vec<EntityRowFilter>)> {
        let projected_schema = projected_schema(&self.schema, projection);
        // A predicate that resolves to a complete identity set is applied in
        // full by the `entity_pks` access path below, and `filter_pushdown`
        // already reports it as `Exact` for exactly that reason. Re-deriving a
        // residual row filter for it makes the provider evaluate the same
        // predicate twice and — because a non-empty `row_filters` disqualifies
        // every direct route — forces the point read onto the generic
        // visibility scan.
        let row_filters = EntityRowFilterAnalyzer::new(&self.spec).analyze_filters(
            &exact_identity_residual(&EntityPrimaryKeyFilterAnalyzer::new(&self.spec), filters),
        )?;
        let mut request = entity_hot_state_scan_request(
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
        apply_exact_file_id_filter(&mut request, exact_file_ids_from_filters(filters)?);
        request.filter.declared_column_eq = declared_column_eq(&self.spec, &row_filters);
        request.filter.declared_column_range = declared_column_range(&self.spec, &row_filters);
        Ok((projected_schema, request, row_filters))
    }

    fn returning_key_from_batch(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<EntityReturningKey> {
        let entity_pk = EntityPk::from_json_array_text(&required_string_value(
            batch,
            row_index,
            "lixcol_entity_pk",
            "UPDATE entity surface RETURNING",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE entity surface RETURNING has invalid lixcol_entity_pk: {error}"
            ))
        })?;
        let branch_id = match self.branch_binding {
            BranchBinding::Active { .. } => String::new(),
            BranchBinding::Explicit => required_string_value(
                batch,
                row_index,
                "lixcol_branch_id",
                "UPDATE entity surface RETURNING",
            )?,
        };
        Ok(EntityReturningKey {
            entity_pk,
            branch_id,
        })
    }

    async fn returning_post_image(
        &self,
        write_ctx: &SqlWriteContext,
        keys: &[EntityReturningKey],
    ) -> Result<RecordBatch> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let mut request = entity_hot_state_scan_request(
            &self.spec.schema_key,
            self.branch_binding.active_branch_id(),
            Some(self.schema.as_ref()),
            None,
            false,
        );
        request.filter.entity_pks = keys
            .iter()
            .map(|key| key.entity_pk.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if matches!(self.branch_binding, BranchBinding::Explicit) {
            request.filter.branch_ids = keys
                .iter()
                .map(|key| key.branch_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        }
        let rows = WriteContextHotStateReader::new(write_ctx.clone())
            .scan_batch(&request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let batch = entity_record_batch(
            &self.spec,
            Arc::clone(&self.schema),
            &rows,
            EntityBatchProjection::for_request(&request),
        )?;
        let mut post_rows = BTreeMap::new();
        for row_index in 0..batch.num_rows() {
            let key = self.returning_key_from_batch(&batch, row_index)?;
            let index = u32::try_from(row_index).map_err(|_| {
                DataFusionError::Execution("entity UPDATE RETURNING row index overflow".into())
            })?;
            if post_rows.insert(key.clone(), index).is_some() {
                return Err(DataFusionError::Execution(format!(
                    "entity UPDATE RETURNING post-image contains duplicate row for identity {:?}",
                    key.entity_pk
                )));
            }
        }
        let indices = keys
            .iter()
            .map(|key| {
                post_rows.get(key).copied().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "entity UPDATE RETURNING post-image is missing updated row {:?}",
                        key.entity_pk
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        take_record_batch_rows(&batch, &indices)
    }

    async fn plan_update_with_post_image(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: Option<DmlReturning>,
    ) -> Result<PlannedDml> {
        reject_read_only_entity_surface(&self.spec.schema_key, "UPDATE")?;
        let (schema, mut request, row_filters) = self.plan_scan_parts(None, filters, None).await?;
        // Schema-v1 UPDATE owns the authenticated scalar tuple. The public
        // table schema still contains the historical snapshot system column,
        // but asking current-state materialization for it would turn a native
        // row into an absent JSON slot and silently remove the update source.
        request
            .projection
            .columns
            .retain(|column| column != "snapshot_content");
        let batch_projection = EntityBatchProjection::for_request(&request);
        let update_snapshots = Arc::new(Mutex::new(BTreeMap::new()));
        let source = row_source(
            (
                Arc::clone(&self.spec),
                Arc::clone(&self.hot_state),
                schema,
                request,
                row_filters,
                batch_projection,
                Arc::clone(&update_snapshots),
            ),
            |(
                spec,
                hot_state,
                schema,
                request,
                row_filters,
                batch_projection,
                update_snapshots,
            )| async move {
                let rows = hot_state
                    .scan_batch(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let filtered = apply_entity_batch_filters(&spec, rows, &row_filters)?;
                capture_entity_update_snapshots(&spec, &filtered.rows, &update_snapshots)?;
                entity_record_batch(&spec, schema, &filtered.rows, batch_projection)
            },
        );
        let spec = Arc::clone(&self.spec);
        let branch_binding = self.branch_binding.clone();
        let returning_spec = self.clone();
        Ok(PlannedDml {
            source,
            apply: Arc::new(move |matched_batch| {
                let write_ctx = write_ctx.clone();
                let spec = Arc::clone(&spec);
                let branch_binding = branch_binding.clone();
                let assignments = assignments.clone();
                let returning = returning.clone();
                let returning_spec = returning_spec.clone();
                let update_snapshots = Arc::clone(&update_snapshots);
                async move {
                    let keys = returning
                        .as_ref()
                        .map(|_| {
                            (0..matched_batch.num_rows())
                                .map(|row_index| {
                                    returning_spec
                                        .returning_key_from_batch(&matched_batch, row_index)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()?;
                    let assignment_values =
                        UpdateAssignmentValues::evaluate(&matched_batch, &assignments)?;
                    let rows = entity_update_stage_rows_from_batch(
                        &matched_batch,
                        &assignment_values,
                        spec.as_ref(),
                        &branch_binding,
                        &update_snapshots,
                    )?;
                    let count = u64::try_from(rows.len()).map_err(|_| {
                        DataFusionError::Execution("UPDATE row count overflow".to_string())
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
                    if let (Some(returning), Some(keys)) = (returning, keys) {
                        let post_image = returning_spec
                            .returning_post_image(&write_ctx, &keys)
                            .await?;
                        returning.capture(returning.project(&post_image)?);
                    }
                    Ok(count)
                }
                .boxed()
            }),
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EntityReturningKey {
    entity_pk: EntityPk,
    branch_id: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EntityUpdateSnapshotKey {
    entity_pk: EntityPk,
    branch_id: String,
}

type EntityUpdateSnapshots = Arc<Mutex<BTreeMap<EntityUpdateSnapshotKey, JsonValue>>>;

fn capture_entity_update_snapshots(
    spec: &EntitySurfaceSpec,
    rows: &MaterializedHotStateBatch,
    snapshots: &EntityUpdateSnapshots,
) -> Result<()> {
    let mut captured = BTreeMap::new();
    for row in rows.iter() {
        let native = row.native_snapshot().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Schema v1 current-state row '{}' is missing its native scalar tuple",
                row.schema_key()
            ))
        })?;
        let snapshot = crate::native_row::logical_value(
            &spec.native_schema,
            row.entity_pk(),
            native,
        )
        .map_err(lix_error_to_datafusion_error)?;
        let key = EntityUpdateSnapshotKey {
            entity_pk: row.entity_pk().clone(),
            branch_id: if row.global() {
                GLOBAL_BRANCH_ID.to_string()
            } else {
                row.branch_id().to_string()
            },
        };
        if captured.insert(key, snapshot).is_some() {
            return Err(DataFusionError::Execution(
                "UPDATE entity surface source contains duplicate row identity".to_string(),
            ));
        }
    }
    *snapshots.lock().map_err(|_| {
        DataFusionError::Execution("UPDATE entity snapshot handoff is poisoned".to_string())
    })? = captured;
    Ok(())
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
            || ExactFileIdFilterAnalyzer.supports(filter)
            || primary_key_analyzer.supports(filter)
        {
            TableProviderFilterPushDown::Exact
        } else if row_filter_analyzer.supports(filter) {
            // Retain a DataFusion residual even when the row-shaped fallback
            // also evaluates the predicate. Immutable columnar layouts use
            // this residual for general typed filtering without a query-shape
            // recognizer; later row-group pruning remains an inexact subset.
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    /// Columns the hot index plane can serve that this scan is not already
    /// constrained on.
    ///
    /// A scan whose filters already resolve to an identity, or that already
    /// mentions the indexed column, is either a point lookup or about to be
    /// one. Offering such a column as a probe key would make a join collect
    /// its build side and replan a scan that was already as narrow as the
    /// index can make it — measurably slower for no rows saved.
    ///
    /// The two tests are ordered by cost. Collecting the filters' column names
    /// is cheap and already rejects the common case — a scan the planner has
    /// pushed an equality on this very column into — so the identity analysis,
    /// which parses primary-key constraints, only runs for the scans that are
    /// still candidates. This runs once per scan per execution, including on
    /// the warm plan-cache path that replans leaves, so it is on the point-read
    /// critical path.
    fn probe_key_columns(&self, filters: &[Expr]) -> Vec<String> {
        if self.spec.indexed_columns.is_empty() {
            return Vec::new();
        }
        let constrained = filters
            .iter()
            .flat_map(|filter| filter.column_refs())
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let columns = self
            .spec
            .indexed_columns
            .iter()
            .filter(|column| !constrained.contains(column.name.as_str()))
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if columns.is_empty()
            || entity_pks_from_primary_key_filters(&self.spec, filters)
                .ok()
                .flatten()
                .is_some()
        {
            return Vec::new();
        }
        columns
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
        let batch_projection = EntityBatchProjection::for_request(&request);
        let direct_entity_snapshot = direct_entity_batch_eligible(&schema, &request, &row_filters)
            .then(|| self.entity_snapshot_reader.clone())
            .flatten();
        let direct_primary_key_projection =
            direct_primary_key_projection_eligible(&self.spec, &schema, &request, &row_filters);
        let mut columnar_request = request.clone();
        // LIMIT is a relational operator, not a storage-layout capability.
        // Ask the reader whether the same filtered/projection scan has a
        // columnar layout; DataFusion retains the semantic LimitExec above it.
        columnar_request.limit = None;
        if let Some(reader) = self.entity_snapshot_reader.as_ref()
            && entity_columnar_projection_eligible(&schema)
            && let Some(layout) = reader
                .plan_entity_columnar_scan(columnar_request)
                .await
                .map_err(lix_error_to_datafusion_error)?
            && let Some(projection) =
                entity_columnar_projection(&layout.manifest, &schema, &self.spec)
        {
            let group_indices = entity_columnar_group_indices(&layout.manifest, &row_filters);
            return Ok(PlannedScan {
                schema: Arc::clone(&schema),
                ordering: None,
                source: Box::pin(entity_columnar_scan_source(
                    Arc::clone(reader),
                    layout,
                    projection,
                    group_indices,
                    schema,
                    Arc::clone(&self.spec),
                    row_filters,
                ))
                .await?,
            });
        }
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    Arc::clone(&self.spec),
                    Arc::clone(&self.hot_state),
                    schema,
                    request,
                    row_filters,
                    batch_projection,
                    direct_entity_snapshot,
                    direct_primary_key_projection,
                ),
                |(
                    spec,
                    hot_state,
                    schema,
                    request,
                    row_filters,
                    batch_projection,
                    direct_entity_snapshot,
                    direct_primary_key_projection,
                )| async move {
                    if direct_primary_key_projection
                        && let Some(direct_entity_snapshot) = direct_entity_snapshot.as_ref()
                        && let Some(entity_pks) = direct_entity_snapshot
                            .scan_entity_primary_keys(request.clone())
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                    {
                        record_rows_examined(entity_pks.len());
                        return entity_primary_key_record_batch(&spec, schema, entity_pks);
                    }
                    let rows = hot_state
                        .scan_batch(&request)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    // Before `row_filters` run: this is the row count a
                    // predicate without an indexed access path pays for.
                    record_rows_examined(rows.len());
                    let filtered = apply_entity_batch_filters(&spec, rows, &row_filters)?;
                    entity_record_batch(&spec, schema, &filtered.rows, batch_projection)
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
        let batch_projection = EntityBatchProjection::for_request(&request);
        let source = row_source(
            (
                Arc::clone(&self.spec),
                Arc::clone(&self.hot_state),
                schema,
                request,
                row_filters,
                batch_projection,
            ),
            |(spec, hot_state, schema, request, row_filters, batch_projection)| async move {
                let rows = hot_state
                    .scan_batch(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let filtered = apply_entity_batch_filters(&spec, rows, &row_filters)?;
                entity_record_batch(&spec, schema, &filtered.rows, batch_projection)
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
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        self.plan_update_with_post_image(write_ctx, assignments, filters, None)
            .await
    }

    async fn plan_update_with_returning(
        &self,
        write_ctx: SqlWriteContext,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: DmlReturning,
    ) -> Result<PlannedDml> {
        self.plan_update_with_post_image(write_ctx, assignments, filters, Some(returning))
            .await
    }
}

fn entity_columnar_projection_eligible(schema: &Schema) -> bool {
    !schema.fields().is_empty()
        && schema
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("lixcol_"))
}

fn entity_columnar_projection(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    schema: &Schema,
    spec: &EntitySurfaceSpec,
) -> Option<Vec<usize>> {
    let expected_fingerprint = spec.columnar_layout_fingerprint();
    if manifest
        .metadata
        .get(crate::sql2::ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY)
        != Some(&expected_fingerprint)
    {
        return None;
    }
    schema
        .fields()
        .iter()
        .map(|field| {
            spec.visible_column(field.name())?;
            manifest.fields.iter().position(|candidate| {
                candidate.name == *field.name()
                    && candidate.data_type.to_arrow() == *field.data_type()
            })
        })
        .collect()
}

async fn entity_columnar_scan_source(
    reader: Arc<dyn EntitySnapshotReader>,
    layout: Arc<crate::sql2::entity_batch::EntityColumnarScanLayout>,
    projection: Vec<usize>,
    group_indices: Vec<usize>,
    schema: SchemaRef,
    spec: Arc<EntitySurfaceSpec>,
    row_filters: Vec<EntityRowFilter>,
) -> Result<super::spec::ScanSource> {
    let identity_column = layout
        .manifest
        .fields
        .iter()
        .position(|field| {
            field.name == crate::sql2::ENTITY_COLUMNAR_ENTITY_PK_FIELD
                && field.data_type.to_arrow() == datafusion::arrow::datatypes::DataType::Utf8
        })
        .ok_or_else(|| {
            DataFusionError::Execution(
                "entity columnar sidecar is missing its hidden entity identity".to_owned(),
            )
        })?;
    let coordinate_shadow_masks = entity_columnar_coordinate_shadow_masks(&layout, &spec)?;
    let mut shadow_identities = if coordinate_shadow_masks.is_some() {
        Vec::new()
    } else {
        layout
            .overlay
            .iter()
            .map(|row| {
                row.entity_pk
                    .as_json_array_text()
                    .map_err(lix_error_to_datafusion_error)
            })
            .collect::<Result<Vec<_>>>()?
    };
    shadow_identities.sort_unstable();
    shadow_identities.dedup();
    let shadow_identity_digest = if coordinate_shadow_masks.is_some() {
        *blake3::hash(b"lix.entity_columnar.coordinate_masks.v1").as_bytes()
    } else {
        let mut hasher = blake3::Hasher::new();
        for identity in &shadow_identities {
            hasher.update(&(identity.len() as u64).to_be_bytes());
            hasher.update(identity.as_bytes());
        }
        *hasher.finalize().as_bytes()
    };
    let shadow_identities = Arc::new(
        shadow_identities
            .into_iter()
            .collect::<HashSet<_, ahash::RandomState>>(),
    );
    let mut overlay_cache_projection = projection.clone();
    overlay_cache_projection.push(usize::MAX);
    let filter_digest = blake3::hash(format!("{row_filters:?}").as_bytes());
    overlay_cache_projection.extend(
        filter_digest
            .as_bytes()
            .chunks_exact(4)
            .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()) as usize),
    );
    let overlay_batches = if layout.overlay.is_empty() {
        Vec::new()
    } else if let Some(batch) = reader
        .cached_entity_columnar_batch(
            &layout,
            usize::MAX,
            shadow_identity_digest,
            &overlay_cache_projection,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?
    {
        vec![batch.as_ref().clone()]
    } else {
        let batches = entity_columnar_overlay_batches(
            spec.as_ref(),
            Arc::clone(&schema),
            layout.overlay.as_ref(),
            &row_filters,
        )?;
        if let [batch] = batches.as_slice() {
            reader
                .cache_entity_columnar_batch(
                    &layout,
                    usize::MAX,
                    shadow_identity_digest,
                    overlay_cache_projection.clone(),
                    Arc::new(batch.clone()),
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
        }
        batches
    };
    let overlay_batches = Arc::new(overlay_batches);
    if group_indices.is_empty() && overlay_batches.is_empty() {
        let empty_schema = Arc::clone(&schema);
        let statistics =
            Statistics::new_unknown(schema.as_ref()).with_num_rows(Precision::Exact(0));
        return Ok(batch_stream_source_with_statistics_and_source(
            Arc::clone(&schema),
            vec![statistics.clone()],
            Some(statistics),
            move |_partition, _context| {
                let schema = Arc::clone(&empty_schema);
                let batch = RecordBatch::new_empty(Arc::clone(&schema));
                let batches = stream::once(async move { Ok(batch) });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
            },
        ));
    }
    let mut all_reconciled_statistics_cached = true;
    let mut base_statistics_cached = Vec::with_capacity(group_indices.len());
    let mut statistics = if layout.overlay.is_empty() {
        base_statistics_cached.resize(group_indices.len(), true);
        group_indices
            .iter()
            .map(|&group_index| {
                entity_columnar_group_statistics(
                    &layout.manifest.groups[group_index],
                    &projection,
                    schema.as_ref(),
                )
            })
            .collect::<Vec<_>>()
    } else {
        let mut cached = Vec::with_capacity(group_indices.len());
        for &group_index in &group_indices {
            if coordinate_shadow_masks
                .as_ref()
                .is_some_and(|masks| masks[group_index].is_none())
            {
                base_statistics_cached.push(true);
                cached.push(entity_columnar_group_statistics(
                    &layout.manifest.groups[group_index],
                    &projection,
                    schema.as_ref(),
                ));
                continue;
            }
            match reader
                .cached_entity_columnar_statistics(
                    &layout,
                    group_index,
                    shadow_identity_digest,
                    &projection,
                )
                .await
                .map_err(lix_error_to_datafusion_error)?
            {
                Some(statistics) => {
                    base_statistics_cached.push(true);
                    cached.push(statistics);
                }
                None => {
                    all_reconciled_statistics_cached = false;
                    base_statistics_cached.push(false);
                    cached.push(Statistics::new_unknown(schema.as_ref()));
                }
            }
        }
        cached
    };
    for batch in overlay_batches.iter() {
        statistics.push(entity_columnar_record_batch_statistics(batch)?);
    }
    let source_statistics = if all_reconciled_statistics_cached {
        Some(Statistics::try_merge_iter(
            statistics.iter(),
            schema.as_ref(),
        )?)
    } else if row_filters.is_empty() {
        let live_count = usize::try_from(layout.live_count).map_err(|_| {
            DataFusionError::Execution("entity collection cardinality exceeds usize".to_owned())
        })?;
        Some(Statistics::new_unknown(schema.as_ref()).with_num_rows(Precision::Exact(live_count)))
    } else {
        None
    };
    if statistics.is_empty() {
        statistics.push(Statistics::new_unknown(schema.as_ref()));
    }
    let partition_count = statistics.len();
    let base_partition_count = group_indices.len();
    // Overlay batches reach the partition closure already filtered, so the
    // examined count comes from the unfiltered overlay the layout carries.
    // One overlay partition records it; the rest record nothing.
    let overlay_rows_examined = layout.overlay.len();
    let stream_schema = Arc::clone(&schema);
    Ok(batch_stream_source_with_statistics_and_source(
        Arc::clone(&schema),
        statistics,
        source_statistics,
        move |partition, _context| {
            debug_assert!(partition < partition_count);
            if partition >= base_partition_count {
                if partition == base_partition_count {
                    record_rows_examined(overlay_rows_examined);
                }
                let schema = Arc::clone(&stream_schema);
                let batch = entity_columnar_overlay_partition(
                    overlay_batches.as_ref(),
                    base_partition_count,
                    partition,
                )
                .expect("statistics expose exactly one entry per overlay partition");
                let batches = stream::once(async move { Ok(batch) });
                return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)));
            }
            let reader = Arc::clone(&reader);
            let layout = layout.clone();
            let public_projection = projection.clone();
            let statistics_projection = projection.clone();
            let statistics_cached = base_statistics_cached[partition];
            let shadow_identities = Arc::clone(&shadow_identities);
            let coordinate_shadow_masks = coordinate_shadow_masks.clone();
            let group_index = group_indices[partition];
            let schema = Arc::clone(&stream_schema);
            let batch_schema = Arc::clone(&schema);
            let batches = stream::once(async move {
                let coordinate_keep = coordinate_shadow_masks
                    .as_ref()
                    .and_then(|masks| masks[group_index].as_ref())
                    .cloned();
                let coordinates_prove_unshadowed =
                    coordinate_shadow_masks.is_some() && coordinate_keep.is_none();
                let batch = cached_or_load_entity_columnar_batch(
                    &reader,
                    &layout,
                    group_index,
                    shadow_identity_digest,
                    public_projection.clone(),
                    async {
                        let batch = if (shadow_identities.is_empty()
                            && coordinate_shadow_masks.is_none())
                            || coordinates_prove_unshadowed
                        {
                            Arc::new(
                                reader
                                    .load_entity_columnar_group(
                                        layout.clone(),
                                        group_index,
                                        public_projection.clone(),
                                    )
                                    .await
                                    .map_err(lix_error_to_datafusion_error)?,
                            )
                        } else {
                            let keep = if let Some(keep) = coordinate_keep {
                                keep
                            } else {
                                reader
                                    .entity_columnar_shadow_mask(
                                        layout.clone(),
                                        group_index,
                                        identity_column,
                                        Arc::clone(&shadow_identities),
                                        shadow_identity_digest,
                                    )
                                    .await
                                    .map_err(lix_error_to_datafusion_error)?
                            };
                            let batch = reader
                                .load_entity_columnar_group(
                                    layout.clone(),
                                    group_index,
                                    public_projection.clone(),
                                )
                                .await
                                .map_err(lix_error_to_datafusion_error)?;
                            Arc::new(filter_record_batch(&batch, keep.as_ref())?)
                        };
                        Ok(batch)
                    },
                )
                .await?;
                if !shadow_identities.is_empty() && !statistics_cached {
                    let statistics = entity_columnar_record_batch_statistics(batch.as_ref())?;
                    reader
                        .cache_entity_columnar_statistics(
                            &layout,
                            group_index,
                            shadow_identity_digest,
                            statistics_projection,
                            statistics,
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                }
                RecordBatch::try_new(batch_schema, batch.columns().to_vec())
                    .map_err(DataFusionError::from)
            });
            // Rows of one row group that survived manifest pruning: group
            // pruning is the columnar route's access path, so a pruned group
            // never reaches here and never counts.
            //
            // Mapped over the stream rather than recorded inside the future
            // above, so nothing is added to that future's state machine. These
            // OLAP plans have very little stack headroom: before #1334 sized
            // these test threads, adding one `u64` to `SqlReadProfile` was
            // enough to overflow them. Keep additions off this future.
            let batches = futures_util::StreamExt::map(batches, |batch| {
                if let Ok(batch) = &batch {
                    record_rows_examined(batch.num_rows());
                }
                batch
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
        },
    ))
}

async fn cached_or_load_entity_columnar_batch(
    reader: &Arc<dyn EntitySnapshotReader>,
    layout: &Arc<crate::sql2::entity_batch::EntityColumnarScanLayout>,
    group_index: usize,
    shadow_identity_digest: [u8; 32],
    projection: Vec<usize>,
    load: impl Future<Output = Result<Arc<RecordBatch>>>,
) -> Result<Arc<RecordBatch>> {
    if let Some(batch) = reader
        .cached_entity_columnar_batch(layout, group_index, shadow_identity_digest, &projection)
        .await
        .map_err(lix_error_to_datafusion_error)?
    {
        return Ok(batch);
    }
    let batch = load.await?;
    reader
        .cache_entity_columnar_batch(
            layout,
            group_index,
            shadow_identity_digest,
            projection,
            batch,
        )
        .await
        .map_err(lix_error_to_datafusion_error)
}

fn entity_columnar_coordinate_shadow_masks(
    layout: &crate::sql2::entity_batch::EntityColumnarScanLayout,
    spec: &EntitySurfaceSpec,
) -> Result<Option<Arc<Vec<Option<Arc<BooleanArray>>>>>> {
    if layout
        .manifest
        .metadata
        .get(crate::sql2::ENTITY_COLUMNAR_BASE_COORDINATES_METADATA_KEY)
        .map(String::as_str)
        != Some("true")
    {
        return Ok(None);
    }
    let mut keep_rows = layout
        .manifest
        .groups
        .iter()
        .map(|_| None)
        .collect::<Vec<Option<Vec<bool>>>>();
    for row in layout.overlay.iter() {
        let Some(coordinate) = row.columnar_base_coordinate else {
            // This row was inserted after the immutable base and therefore
            // has no stale base member to suppress.
            continue;
        };
        let owner =
            crate::hot_state::entity_row_group_set_id(coordinate.base_commit_id, &spec.schema_key);
        if owner != layout.id {
            return exec_err!(
                "entity overlay columnar coordinate belongs to a different immutable base"
            );
        }
        let group_index = coordinate.group_index as usize;
        let group = layout.manifest.groups.get(group_index).ok_or_else(|| {
            DataFusionError::Execution(
                "entity overlay columnar coordinate has an invalid group index".to_owned(),
            )
        })?;
        if coordinate.row_index >= group.row_count {
            return exec_err!("entity overlay columnar coordinate has an invalid row index");
        }
        let keep =
            keep_rows[group_index].get_or_insert_with(|| vec![true; group.row_count as usize]);
        keep[coordinate.row_index as usize] = false;
    }
    Ok(Some(Arc::new(
        keep_rows
            .into_iter()
            .map(|keep| keep.map(|keep| Arc::new(BooleanArray::from(keep))))
            .collect(),
    )))
}

fn entity_columnar_overlay_partition(
    overlay_batches: &[RecordBatch],
    base_partition_count: usize,
    partition: usize,
) -> Option<RecordBatch> {
    overlay_batches
        .get(partition.checked_sub(base_partition_count)?)
        .cloned()
}

#[cfg(test)]
fn reconcile_entity_columnar_base_batch(
    batch: RecordBatch,
    public_schema: SchemaRef,
    shadow_entity_pks: &HashSet<String, ahash::RandomState>,
) -> Result<RecordBatch> {
    let identities = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution("entity columnar identity column is not Utf8".to_owned())
        })?;
    let keep = BooleanArray::from(
        (0..identities.len())
            .map(|index| !shadow_entity_pks.contains(identities.value(index)))
            .collect::<Vec<_>>(),
    );
    let batch = filter_record_batch(&batch, &keep)?;
    RecordBatch::try_new(public_schema, batch.columns()[1..].to_vec())
        .map_err(DataFusionError::from)
}

fn entity_columnar_overlay_batches(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &[crate::hot_state::EntityColumnarOverlayRow],
    row_filters: &[EntityRowFilter],
) -> Result<Vec<RecordBatch>> {
    let mut snapshots = Vec::new();
    for row in rows {
        if row.deleted {
            continue;
        }
        let snapshot = row.snapshot_content.as_deref().ok_or_else(|| {
            DataFusionError::Execution(
                "live entity columnar overlay row has no snapshot".to_owned(),
            )
        })?;
        if !row_filters.is_empty() {
            let parsed = parse_snapshot_value(std::str::from_utf8(snapshot).map_err(|error| {
                DataFusionError::Execution(format!(
                    "entity columnar overlay snapshot is not UTF-8: {error}"
                ))
            })?)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            if !row_filters.iter().try_fold(true, |matches, filter| {
                Ok::<_, DataFusionError>(
                    matches && filter.matches_snapshot(Some(&parsed), &spec.schema_key)?,
                )
            })? {
                continue;
            }
        }
        snapshots.push(Some(snapshot));
    }
    let decoder = EntityProjectionDecoder::new(
        spec,
        schema.fields().iter().map(|field| field.name().as_str()),
    )
    .map_err(entity_projection_error_to_datafusion_error)?;
    snapshots
        .chunks(crate::columnar_row_group::ROW_GROUP_MAX_ROWS)
        .map(|snapshots| {
            let columns = decoder
                .decode_arrow_columns(snapshots.iter().copied())
                .map_err(entity_projection_error_to_datafusion_error)?;
            RecordBatch::try_new(Arc::clone(&schema), columns).map_err(DataFusionError::from)
        })
        .collect()
}

/// Records rows a scan route looked at, before its own filtering.
///
/// `sql_profile` is gated behind `storage-benches`, so the call sites must not
/// name it directly: `--all-features` builds would compile and the default and
/// `wasm32` builds would not. Routing every site through this pair keeps the
/// diagnostic out of builds that do not have the module at all.
#[cfg(feature = "storage-benches")]
#[inline]
fn record_rows_examined(rows: usize) {
    crate::sql_profile::record_provider_rows_examined(rows);
}

#[cfg(not(feature = "storage-benches"))]
#[inline]
fn record_rows_examined(_rows: usize) {}

fn entity_columnar_group_indices(
    manifest: &crate::columnar_row_group::RowGroupManifest,
    row_filters: &[EntityRowFilter],
) -> Vec<usize> {
    let mut selected = Vec::new();
    for (group_index, group) in manifest.groups.iter().enumerate() {
        if row_filters
            .iter()
            .all(|filter| filter.may_match_group(manifest, group).unwrap_or(true))
        {
            selected.push(group_index);
        }
    }
    selected
}

fn entity_columnar_group_statistics(
    group: &crate::columnar_row_group::RowGroupStatistics,
    projection: &[usize],
    schema: &Schema,
) -> Statistics {
    let column_statistics = projection
        .iter()
        .map(|&index| {
            let source = &group.columns[index];
            ColumnStatistics::new_unknown()
                .with_null_count(Precision::Exact(source.null_count as usize))
                .with_min_value(entity_columnar_scalar_precision(source.min.as_ref()))
                .with_max_value(entity_columnar_scalar_precision(source.max.as_ref()))
                .with_sum_value(entity_columnar_scalar_precision(source.sum.as_ref()))
        })
        .collect();
    let mut statistics = Statistics::new_unknown(schema);
    statistics.num_rows = Precision::Exact(group.row_count as usize);
    statistics.column_statistics = column_statistics;
    statistics
}

fn entity_columnar_record_batch_statistics(batch: &RecordBatch) -> Result<Statistics> {
    let statistics = crate::columnar_row_group::exact_record_batch_statistics(batch)
        .map_err(lix_error_to_datafusion_error)?;
    let projection = (0..batch.num_columns()).collect::<Vec<_>>();
    Ok(entity_columnar_group_statistics(
        &statistics,
        &projection,
        batch.schema().as_ref(),
    ))
}

fn entity_columnar_scalar_precision(
    value: Option<&crate::columnar_row_group::RowGroupScalar>,
) -> Precision<ScalarValue> {
    let value = match value {
        Some(crate::columnar_row_group::RowGroupScalar::String(value)) => {
            ScalarValue::Utf8(Some(value.clone()))
        }
        Some(crate::columnar_row_group::RowGroupScalar::Int64(value)) => {
            ScalarValue::Int64(Some(*value))
        }
        Some(crate::columnar_row_group::RowGroupScalar::Float64(value)) => {
            ScalarValue::Float64(Some(*value))
        }
        Some(crate::columnar_row_group::RowGroupScalar::Boolean(value)) => {
            ScalarValue::Boolean(Some(*value))
        }
        None => return Precision::Absent,
    };
    Precision::Exact(value)
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
) -> Result<RawWriteBatch> {
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
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
            let metadata = parse_row_metadata_value(&value, &spec.schema_key)
                .map_err(lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
                .map_err(lix_error_to_datafusion_error)
        })
        .transpose()?;
        let file_id = optional_string_value(
            batch,
            row_index,
            "lixcol_file_id",
            "DELETE FROM entity surface",
        )?
        .map(Into::into);
        let untracked = optional_bool_value(
            batch,
            row_index,
            "lixcol_untracked",
            "DELETE FROM entity surface",
        )?
        .unwrap_or(false);
        rows.push_parts(
            Some(entity_pk),
            spec.schema_key.as_str().into(),
            file_id,
            None,
            metadata,
            None,
            None,
            None,
            global,
            None,
            None,
            untracked,
            branch_id.into(),
        );
    }
    Ok(rows)
}

fn entity_update_stage_rows_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    spec: &EntitySurfaceSpec,
    branch_binding: &BranchBinding,
    update_snapshots: &EntityUpdateSnapshots,
) -> Result<RawWriteBatch> {
    let update_snapshots = update_snapshots.lock().map_err(|_| {
        DataFusionError::Execution("UPDATE entity snapshot handoff is poisoned".to_string())
    })?;
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let global =
            optional_bool_value(batch, row_index, "lixcol_global", "UPDATE entity surface")?
                .unwrap_or(false);
        let source_branch_id = optional_string_value(
            batch,
            row_index,
            "lixcol_branch_id",
            "UPDATE entity surface",
        )?;
        if matches!(branch_binding, BranchBinding::Explicit)
            && global
            && source_branch_id.as_deref() != Some(GLOBAL_BRANCH_ID)
        {
            return Err(DataFusionError::Execution(
                "UPDATE through an entity by-branch surface cannot mutate a projected global row"
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
                        "UPDATE entity by-branch requires lixcol_branch_id".to_string(),
                    )
                })?
        };
        let entity_pk = EntityPk::from_json_array_text(&required_string_value(
            batch,
            row_index,
            "lixcol_entity_pk",
            "UPDATE entity surface",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE entity surface has invalid lixcol_entity_pk: {error}"
            ))
        })?;
        let snapshot_content = update_snapshots
            .get(&EntityUpdateSnapshotKey {
                entity_pk: entity_pk.clone(),
                branch_id: branch_id.clone(),
            })
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "UPDATE entity surface is missing its source snapshot for schema '{}'",
                    spec.schema_key
                ))
            })?;
        let mut snapshot = snapshot_content.clone();
        let object = snapshot.as_object_mut().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "UPDATE entity surface expected object snapshot for schema '{}'",
                spec.schema_key
            ))
        })?;
        for column in &spec.columns {
            let UpdateCell::Assigned(cell) =
                assignment_values.assigned_cell(row_index, &column.name)?
            else {
                continue;
            };
            object.insert(
                column.name.clone(),
                entity_update_json_value(cell, column.column_type, spec, &column.name)?,
            );
        }
        let metadata = match assignment_values.assigned_cell(row_index, "lixcol_metadata")? {
            UpdateCell::Unassigned => {
                optional_string_value(batch, row_index, "lixcol_metadata", "UPDATE entity surface")?
                    .map(|value| entity_update_metadata(&value, spec))
                    .transpose()?
            }
            UpdateCell::Assigned(SqlCell::Null) => None,
            UpdateCell::Assigned(SqlCell::Value(value)) => {
                let raw = scalar_utf8(value, "lixcol_metadata", spec)?;
                Some(entity_update_metadata(&raw, spec)?)
            }
        };
        let file_id =
            optional_string_value(batch, row_index, "lixcol_file_id", "UPDATE entity surface")?
                .map(Into::into);
        let untracked = optional_bool_value(
            batch,
            row_index,
            "lixcol_untracked",
            "UPDATE entity surface",
        )?
        .unwrap_or(false);
        rows.push_parts(
            Some(entity_pk),
            spec.schema_key.as_str().into(),
            file_id,
            Some(
                TransactionJson::from_value(
                    snapshot,
                    &format!("{} update snapshot_content", spec.schema_key),
                )
                .map_err(lix_error_to_datafusion_error)?,
            ),
            metadata,
            None,
            None,
            None,
            global,
            None,
            None,
            untracked,
            branch_id.into(),
        );
    }
    Ok(rows)
}

fn entity_update_json_value(
    cell: SqlCell,
    column_type: EntityColumnType,
    spec: &EntitySurfaceSpec,
    column_name: &str,
) -> Result<JsonValue> {
    let SqlCell::Value(value) = cell else {
        return Ok(JsonValue::Null);
    };
    match column_type {
        EntityColumnType::String => scalar_utf8(value, column_name, spec).map(JsonValue::String),
        EntityColumnType::Json => {
            let raw = scalar_utf8(value, column_name, spec)?;
            serde_json::from_str(&raw).map_err(|error| {
                DataFusionError::Execution(format!(
                    "UPDATE {} column '{column_name}' produced invalid JSON: {error}",
                    spec.schema_key
                ))
            })
        }
        EntityColumnType::Integer => match value {
            ScalarValue::Int64(Some(value)) => Ok(JsonValue::from(value)),
            other => Err(entity_update_type_error(
                spec,
                column_name,
                "BIGINT",
                &other,
            )),
        },
        EntityColumnType::Number => match value {
            ScalarValue::Float64(Some(value)) => serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "UPDATE {} column '{column_name}' produced non-finite DOUBLE PRECISION",
                        spec.schema_key
                    ))
                }),
            other => Err(entity_update_type_error(
                spec,
                column_name,
                "DOUBLE PRECISION",
                &other,
            )),
        },
        EntityColumnType::Boolean => match value {
            ScalarValue::Boolean(Some(value)) => Ok(JsonValue::Bool(value)),
            other => Err(entity_update_type_error(
                spec,
                column_name,
                "BOOLEAN",
                &other,
            )),
        },
        EntityColumnType::Timestamptz => match value {
            ScalarValue::TimestampMicrosecond(Some(value), _) => {
                chrono::DateTime::from_timestamp_micros(value)
                    .map(|timestamp| {
                        JsonValue::String(
                            timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                        )
                    })
                    .ok_or_else(|| {
                        entity_update_type_error(
                            spec,
                            column_name,
                            "TIMESTAMPTZ",
                            &ScalarValue::TimestampMicrosecond(Some(value), Some("UTC".into())),
                        )
                    })
            }
            other => Err(entity_update_type_error(
                spec,
                column_name,
                "TIMESTAMPTZ",
                &other,
            )),
        },
    }
}

fn scalar_utf8(value: ScalarValue, column_name: &str, spec: &EntitySurfaceSpec) -> Result<String> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(value),
        other => Err(entity_update_type_error(spec, column_name, "TEXT", &other)),
    }
}

fn entity_update_type_error(
    spec: &EntitySurfaceSpec,
    column_name: &str,
    expected: &str,
    actual: &ScalarValue,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "UPDATE {} column '{column_name}' expected {expected}, got {actual:?}",
        spec.schema_key
    ))
}

fn entity_update_metadata(raw: &str, spec: &EntitySurfaceSpec) -> Result<TransactionJson> {
    let metadata =
        parse_row_metadata_value(raw, &spec.schema_key).map_err(lix_error_to_datafusion_error)?;
    TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
        .map_err(lix_error_to_datafusion_error)
}

pub(super) fn entity_pks_from_primary_key_filters(
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<Option<Vec<EntityPk>>> {
    let analyzer = EntityPrimaryKeyFilterAnalyzer::new(spec);
    let mut constraint: Option<EntityPkConstraint> = None;
    for filter in filters {
        let Some(filter_constraint) = analyzer.analyze_conjunctive_constraint(filter)? else {
            continue;
        };
        constraint = Some(match constraint {
            Some(existing) => existing.intersect(filter_constraint, &analyzer.primary_key_columns),
            None => filter_constraint,
        });
    }

    Ok(constraint
        .and_then(|constraint| {
            constraint.into_entity_pks(
                &analyzer.primary_key_columns,
                &analyzer.primary_key_component_types,
            )
        })
        .map(|ids| ids.into_iter().collect()))
}

/// The first equality or `IN` list on a column this schema declares as unique
/// or as a foreign key, which the hot index plane can serve as an access path.
///
/// The matched filter is deliberately **left in `row_filters`**. Index entries
/// are never deleted when a value is superseded, so a lookup returns
/// candidates and this predicate is what rejects the stale ones. It is not a
/// redundant re-check over a handful of rows — it is the correctness half of
/// the access path, and removing it reintroduces rows that no longer match.
///
/// An `IN` list resolves the same way as an equality — one index bucket per
/// member, union of the candidates — which is what lets a join's runtime probe
/// keys reach the index at all: a probe carries the several build-side values
/// of one key column, never a single literal.
fn declared_column_eq(
    spec: &EntitySurfaceSpec,
    row_filters: &[EntityRowFilter],
) -> Option<crate::hot_state::DeclaredColumnEq> {
    row_filters.iter().find_map(|filter| {
        let (column, values) = declared_column_membership(filter)?;
        let indexed = spec
            .indexed_columns
            .iter()
            .find(|candidate| candidate.name == column)?;
        let values = values
            .into_iter()
            .map(|value| match value {
                EntityFilterValue::String(value) => {
                    Some(crate::hot_state::HotIndexValue::String(value.clone()))
                }
                EntityFilterValue::Integer(value) => {
                    Some(crate::hot_state::HotIndexValue::Integer(*value))
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some(crate::hot_state::DeclaredColumnEq {
            schema_key: spec.schema_key.clone(),
            ordinal: indexed.ordinal,
            values,
        })
    })
}

/// The interval an indexed column is constrained to, when a conjunction of
/// range predicates constrains one.
///
/// Only conjunctions contribute. A bound under a disjunction does not hold for
/// every row the filter admits, so descending into `Or` would produce an
/// interval that omits matching rows.
///
/// When several bounds constrain the same side, the first is taken rather than
/// the tightest -- so `x > 5 AND x > 100` seeks from 5, not from 100. That is
/// deliberate, not an oversight. Every conjunct must hold, so any one of them
/// is a valid bound; a looser one costs candidates, never correctness, and the
/// caller's residual rejects the surplus.
fn declared_column_range(
    spec: &EntitySurfaceSpec,
    row_filters: &[EntityRowFilter],
) -> Option<Box<crate::hot_state::DeclaredColumnRange>> {
    let mut bounds = Vec::new();
    for filter in row_filters {
        collect_conjunctive_ranges(filter, &mut bounds);
    }
    if bounds.is_empty() {
        return None;
    }
    for indexed in &spec.indexed_columns {
        let mut lower = None;
        let mut upper = None;
        for (column, op, value) in &bounds {
            if *column != indexed.name.as_str() {
                continue;
            }
            let Some(value) = hot_index_value_from_filter_value(value) else {
                continue;
            };
            match op {
                EntityRangeOp::Gt if lower.is_none() => lower = Some((value, false)),
                EntityRangeOp::GtEq if lower.is_none() => lower = Some((value, true)),
                EntityRangeOp::Lt if upper.is_none() => upper = Some((value, false)),
                EntityRangeOp::LtEq if upper.is_none() => upper = Some((value, true)),
                _ => {}
            }
        }
        if lower.is_some() || upper.is_some() {
            return Some(Box::new(crate::hot_state::DeclaredColumnRange {
                schema_key: spec.schema_key.clone(),
                ordinal: indexed.ordinal,
                lower,
                upper,
            }));
        }
    }
    None
}

/// Range bounds that hold for every row the filter admits.
fn collect_conjunctive_ranges<'a>(
    filter: &'a EntityRowFilter,
    out: &mut Vec<(&'a str, EntityRangeOp, &'a EntityFilterValue)>,
) {
    match filter {
        EntityRowFilter::ColumnRange {
            column, op, value, ..
        } => out.push((column.as_str(), *op, value)),
        EntityRowFilter::And(left, right) => {
            collect_conjunctive_ranges(left, out);
            collect_conjunctive_ranges(right, out);
        }
        _ => {}
    }
}

/// The index-plane representation of a filter literal, when one exists.
fn hot_index_value_from_filter_value(
    value: &EntityFilterValue,
) -> Option<crate::hot_state::HotIndexValue> {
    match value {
        EntityFilterValue::String(value) => {
            Some(crate::hot_state::HotIndexValue::String(value.clone()))
        }
        EntityFilterValue::Integer(value) => Some(crate::hot_state::HotIndexValue::Integer(*value)),
        _ => None,
    }
}

/// The column and the value set one row filter constrains it to, when the
/// filter is a membership test on a single column.
///
/// A disjunction counts: DataFusion's simplifier rewrites a short `IN` list
/// into `col = a OR col = b`, so the two spellings must reach the index the
/// same way. A conjunction does not, because either side alone would need to
/// be re-checked against the other, which is a wider contract than "the value
/// is one of these".
fn declared_column_membership(filter: &EntityRowFilter) -> Option<(&str, Vec<&EntityFilterValue>)> {
    match filter {
        EntityRowFilter::ColumnEq { column, value, .. } => Some((column, vec![value])),
        EntityRowFilter::ColumnIn { column, values, .. } => Some((column, values.iter().collect())),
        EntityRowFilter::Or(left, right) => {
            let (column, mut values) = declared_column_membership(left)?;
            let (right_column, right_values) = declared_column_membership(right)?;
            if column != right_column {
                return None;
            }
            values.extend(right_values);
            Some((column, values))
        }
        // A range names no finite value set, so it cannot become an
        // equality/IN probe. The index range seek is a separate access path.
        EntityRowFilter::ColumnRange { .. } | EntityRowFilter::And(..) => None,
    }
}

fn apply_exact_entity_pk_filters(
    request: &mut HotStateScanRequest,
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<()> {
    if let Some(entity_pks) = entity_pks_from_primary_key_filters(spec, filters)? {
        if entity_pks.is_empty() {
            request.filter.rows = HotStateRowFilter::None;
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
    request: &mut HotStateScanRequest,
    branch_ids: Option<Vec<String>>,
) {
    if let Some(branch_ids) = branch_ids {
        if branch_ids.is_empty() {
            request.filter.rows = HotStateRowFilter::None;
        }
        request.filter.branch_ids = branch_ids;
    }
}

/// Extracts the exact `lixcol_file_id` selector so a file-scoped entity read
/// becomes a physical seek instead of a schema-wide scan.
///
/// `HOT_ROW` is keyed `schema_key ++ file_id ++ entity_pk`, so a
/// `schema_key + file_id` filter is a contiguous prefix
/// (`hot_file_scan_prefixes`). The other three live-state authorities that can
/// hold rows with no branch-local `HOT_ROW` — the packed current base, the
/// certified entity batches, and the root current base — each already apply
/// `HotStateFilter::file_ids` (two of them with their own file-scoped seek),
/// so the merged answer stays complete. Without this analyzer the predicate
/// only ever survived as a DataFusion residual and every file-scoped read paid
/// O(rows in the branch).
fn exact_file_ids_from_filters(filters: &[Expr]) -> Result<Option<Vec<String>>> {
    let analyzer = ExactFileIdFilterAnalyzer;
    let mut file_ids: Option<BTreeSet<String>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze(filter)? else {
            continue;
        };
        file_ids = Some(match file_ids {
            Some(existing_ids) => existing_ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    Ok(file_ids.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_file_id_filter(request: &mut HotStateScanRequest, file_ids: Option<Vec<String>>) {
    if let Some(file_ids) = file_ids {
        if file_ids.is_empty() {
            request.filter.rows = HotStateRowFilter::None;
        }
        request.filter.file_ids = file_ids
            .into_iter()
            .map(crate::NullableKeyFilter::Value)
            .collect();
    }
}

struct ExactFileIdFilterAnalyzer;

impl ExactFileIdFilterAnalyzer {
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
                Ok(file_id_from_binary_filter(binary_expr).map(|value| BTreeSet::from([value])))
            }
            Expr::InList(in_list) => Ok(
                file_ids_from_in_list_filter(in_list).map(|values| values.into_iter().collect())
            ),
            _ => Ok(None),
        }
    }
}

fn file_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    file_id_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| file_id_from_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn file_ids_from_in_list_filter(in_list: &InList) -> Option<Vec<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "lixcol_file_id" {
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

fn file_id_from_column_literal_filter(column_expr: &Expr, literal_expr: &Expr) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "lixcol_file_id" {
        return None;
    }
    string_expr_literal(literal_expr)
}

pub(super) struct EntityPrimaryKeyFilterAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
    primary_key_component_types: Vec<crate::entity_pk::EntityPkComponentType>,
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
    pub(super) fn new(spec: &'a EntitySurfaceSpec) -> Self {
        Self {
            primary_key_columns: top_level_primary_key_columns(spec),
            primary_key_component_types: spec.primary_key_component_types.clone(),
        }
    }

    pub(super) fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    pub(super) fn contains_routable_conjunct(&self, expr: &Expr) -> bool {
        self.analyze_conjunctive_constraint(expr)
            .is_ok_and(|constraint| constraint.is_some())
    }

    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<EntityPk>>> {
        if self.primary_key_columns.is_empty() {
            return Ok(None);
        }
        let Some(constraint) = self.analyze_constraint(expr)? else {
            return Ok(None);
        };
        Ok(
            constraint
                .into_entity_pks(&self.primary_key_columns, &self.primary_key_component_types),
        )
    }

    /// Extracts identity constraints that are guaranteed conjuncts while
    /// refusing to partially route a disjunction. This lets DataFusion pass
    /// separately planned composite-key terms without turning a payload
    /// predicate into identity semantics.
    fn analyze_conjunctive_constraint(&self, expr: &Expr) -> Result<Option<EntityPkConstraint>> {
        if self.primary_key_columns.is_empty() {
            return Ok(None);
        }
        let Expr::BinaryExpr(binary_expr) = expr else {
            return self.analyze_constraint(expr);
        };
        if binary_expr.op != Operator::And {
            return self.analyze_constraint(expr);
        }

        let left = self.analyze_conjunctive_constraint(&binary_expr.left)?;
        let right = self.analyze_conjunctive_constraint(&binary_expr.right)?;
        Ok(match (left, right) {
            (Some(left), Some(right)) => Some(left.intersect(right, &self.primary_key_columns)),
            (Some(constraint), None) | (None, Some(constraint)) => Some(constraint),
            (None, None) => None,
        })
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
                let Some(left_ids) = left
                    .into_entity_pks(&self.primary_key_columns, &self.primary_key_component_types)
                else {
                    return Ok(None);
                };
                let Some(mut right_ids) = right
                    .into_entity_pks(&self.primary_key_columns, &self.primary_key_component_types)
                else {
                    return Ok(None);
                };
                right_ids.extend(left_ids);
                Ok(Some(EntityPkConstraint::Full(right_ids)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(entity_pk_constraint_from_binary_filter(
                binary_expr,
                &self.primary_key_columns,
                &self.primary_key_component_types,
            )),
            Expr::InList(in_list) => Ok(entity_pk_constraint_from_in_list_filter(
                in_list,
                &self.primary_key_columns,
                &self.primary_key_component_types,
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

    fn into_entity_pks(
        self,
        primary_key_columns: &[&str],
        component_types: &[crate::entity_pk::EntityPkComponentType],
    ) -> Option<BTreeSet<EntityPk>> {
        match self {
            Self::Full(ids) => Some(ids),
            Self::Parts(parts) => {
                entity_pks_from_primary_key_parts(primary_key_columns, component_types, parts)
            }
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
    fn analyze_filters(&self, filters: &[&Expr]) -> Result<Vec<EntityRowFilter>> {
        Ok(filters
            .iter()
            .filter_map(|filter| self.analyze(filter))
            .collect())
    }

    fn analyze(&self, expr: &Expr) -> Option<EntityRowFilter> {
        match expr {
            Expr::Column(column) => {
                let column_name = self.filterable_column_name(&column.name)?;
                let column = self.spec.visible_column(column_name)?;
                (column.column_type == EntityColumnType::Boolean).then(|| {
                    EntityRowFilter::ColumnEq {
                        column: column_name.to_string(),
                        column_type: EntityColumnType::Boolean,
                        value: EntityFilterValue::Boolean(true),
                    }
                })
            }
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
        if binary_expr.op == Operator::Eq {
            return self
                .analyze_column_literal(&binary_expr.left, &binary_expr.right)
                .or_else(|| self.analyze_column_literal(&binary_expr.right, &binary_expr.left));
        }
        let op = EntityRangeOp::from_operator(binary_expr.op)?;
        self.analyze_column_literal_range(&binary_expr.left, &binary_expr.right, op)
            .or_else(|| {
                self.analyze_column_literal_range(
                    &binary_expr.right,
                    &binary_expr.left,
                    op.reversed(),
                )
            })
    }

    /// A `column OP literal` range predicate, when the column can carry one.
    ///
    /// Restricted to `Integer` and `String`. Those are the types with a total
    /// order, and — not coincidentally — the two the hot index can encode
    /// order-preservingly, so this is the same admissible set a later index
    /// range seek needs. `Number` is refused because NaN makes the order
    /// partial; `Boolean` and `Json` have no useful range.
    fn analyze_column_literal_range(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
        op: EntityRangeOp,
    ) -> Option<EntityRowFilter> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        let column_name = self.filterable_column_name(&column.name)?;
        let column_type = self
            .spec
            .visible_column(column_name)
            .expect("filterable column should exist")
            .column_type;
        if !matches!(
            column_type,
            EntityColumnType::Integer | EntityColumnType::String
        ) {
            return None;
        }
        let value = entity_filter_value_literal(literal_expr, column_type)?;
        // A literal that widened into another representation (an integer
        // column compared against a float, say) has no total order against the
        // stored value, so it must not become a range.
        if !matches!(
            value,
            EntityFilterValue::Integer(_) | EntityFilterValue::String(_)
        ) {
            return None;
        }
        Some(EntityRowFilter::ColumnRange {
            column: column_name.to_string(),
            column_type,
            op,
            value,
        })
    }

    fn analyze_in_list(&self, in_list: &InList) -> Option<EntityRowFilter> {
        if in_list.negated {
            return None;
        }
        let Expr::Column(column) = in_list.expr.as_ref() else {
            return None;
        };
        let column_name = self.filterable_column_name(&column.name)?;
        let column_type = self
            .spec
            .visible_column(column_name)
            .expect("filterable column should exist")
            .column_type;
        let values = in_list
            .list
            .iter()
            .map(|expr| entity_filter_value_literal(expr, column_type))
            .collect::<Option<Vec<_>>>()?;
        if values.is_empty() {
            return None;
        }
        Some(EntityRowFilter::ColumnIn {
            column: column_name.to_string(),
            column_type,
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
        let column_type = self
            .spec
            .visible_column(column_name)
            .expect("filterable column should exist")
            .column_type;
        Some(EntityRowFilter::ColumnEq {
            column: column_name.to_string(),
            column_type,
            value: entity_filter_value_literal(literal_expr, column_type)?,
        })
    }

    fn filterable_column_name(&self, column_name: &str) -> Option<&str> {
        let column = self.spec.visible_column(column_name)?;
        match column.column_type {
            EntityColumnType::String
            | EntityColumnType::Boolean
            | EntityColumnType::Integer
            | EntityColumnType::Number => {
                #[cfg(any(test, feature = "storage-benches"))]
                record_filterable_column(&self.spec.schema_key, column_name, true);
                Some(column.name.as_str())
            }
            EntityColumnType::Json | EntityColumnType::Timestamptz => {
                #[cfg(any(test, feature = "storage-benches"))]
                record_filterable_column(&self.spec.schema_key, column_name, false);
                None
            }
        }
    }
}

/// Census of `filterable_column_name`, at the refusal itself.
///
/// Question being answered: how often does a real query shape ask to push a
/// predicate on a JSON column? A counter cannot say *which* schema and column,
/// and the shape is the whole point, so each decision is appended as one line
/// and aggregated afterwards.
///
/// Inert unless `LIX_FILTERABLE_CENSUS` names a file, so it costs nothing in
/// an ordinary test run and cannot perturb a timing measurement.
///
/// One `write_all` of a complete line, in append mode: short appends to the
/// same fd from several threads and processes do not interleave on Linux, and
/// the aggregate only needs line counts per key.
#[cfg(any(test, feature = "storage-benches"))]
fn record_filterable_column(schema_key: &str, column_name: &str, accepted: bool) {
    use std::io::Write as _;
    static PATH: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    let Some(path) = PATH
        .get_or_init(|| std::env::var("LIX_FILTERABLE_CENSUS").ok())
        .as_deref()
    else {
        return;
    };
    let verdict = if accepted { "accept" } else { "refuse_json" };
    let line = format!("{verdict}\t{schema_key}\t{column_name}\n");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = file.write_all(line.as_bytes());
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
    /// One half-bounded comparison against a literal.
    ///
    /// `BETWEEN` reaches the analyzer already desugared into `>= AND <=`, so a
    /// single bound per node composes into a closed interval through the
    /// existing [`EntityRowFilter::And`] arm. Carrying one bound rather than
    /// two keeps every consumer's arm total and means the interval logic lives
    /// in exactly one place.
    ColumnRange {
        column: String,
        column_type: EntityColumnType,
        op: EntityRangeOp,
        value: EntityFilterValue,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

/// The comparison a [`EntityRowFilter::ColumnRange`] applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntityRangeOp {
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl EntityRangeOp {
    fn from_operator(op: Operator) -> Option<Self> {
        match op {
            Operator::Lt => Some(Self::Lt),
            Operator::LtEq => Some(Self::LtEq),
            Operator::Gt => Some(Self::Gt),
            Operator::GtEq => Some(Self::GtEq),
            _ => None,
        }
    }

    /// The same predicate with the operands swapped.
    ///
    /// `5 < ordinal` and `ordinal < 5` are different predicates, so the
    /// literal-on-the-left spelling must reverse the comparison rather than
    /// reuse it. Reusing it silently returns the complement of the requested
    /// rows.
    fn reversed(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
        }
    }

    fn matches(self, ordering: std::cmp::Ordering) -> bool {
        match self {
            Self::Lt => ordering.is_lt(),
            Self::LtEq => ordering.is_le(),
            Self::Gt => ordering.is_gt(),
            Self::GtEq => ordering.is_ge(),
        }
    }
}

impl EntityRowFilter {
    fn may_match_group(
        &self,
        manifest: &crate::columnar_row_group::RowGroupManifest,
        group: &crate::columnar_row_group::RowGroupStatistics,
    ) -> Option<bool> {
        match self {
            Self::ColumnEq { column, value, .. } => {
                let index = manifest
                    .fields
                    .iter()
                    .position(|field| field.name == *column)?;
                entity_filter_value_in_statistics(value, &group.columns[index], group.row_count)
            }
            Self::ColumnIn { column, values, .. } => {
                let index = manifest
                    .fields
                    .iter()
                    .position(|field| field.name == *column)?;
                let statistics = &group.columns[index];
                let mut unknown = false;
                for value in values {
                    match entity_filter_value_in_statistics(value, statistics, group.row_count) {
                        Some(true) => return Some(true),
                        Some(false) => {}
                        None => unknown = true,
                    }
                }
                (!unknown).then_some(false)
            }
            Self::ColumnRange {
                column, op, value, ..
            } => {
                let index = manifest
                    .fields
                    .iter()
                    .position(|field| field.name == *column)?;
                entity_filter_range_in_statistics(
                    *op,
                    value,
                    &group.columns[index],
                    group.row_count,
                )
            }
            Self::And(left, right) => match (
                left.may_match_group(manifest, group),
                right.may_match_group(manifest, group),
            ) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Self::Or(left, right) => match (
                left.may_match_group(manifest, group),
                right.may_match_group(manifest, group),
            ) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }

    /// Names every top-level snapshot column this predicate reads.
    ///
    /// Row predicates are `ColumnEq` / `ColumnIn` over visible top-level
    /// columns, so the set is small and known before the scan decodes a
    /// single row.
    fn collect_filter_columns<'a>(&'a self, out: &mut BTreeSet<&'a str>) {
        match self {
            Self::ColumnEq { column, .. }
            | Self::ColumnIn { column, .. }
            | Self::ColumnRange { column, .. } => {
                out.insert(column.as_str());
            }
            Self::And(left, right) | Self::Or(left, right) => {
                left.collect_filter_columns(out);
                right.collect_filter_columns(out);
            }
        }
    }

    fn matches_snapshot(&self, snapshot: Option<&JsonValue>, schema_key: &str) -> Result<bool> {
        match self {
            Self::ColumnEq {
                column,
                column_type,
                value,
            } => Ok(
                entity_snapshot_value(snapshot, schema_key, column, *column_type)?
                    .is_some_and(|actual| entity_filter_values_equal(&actual, value, *column_type)),
            ),
            Self::ColumnIn {
                column,
                column_type,
                values,
            } => Ok(
                entity_snapshot_value(snapshot, schema_key, column, *column_type)?.is_some_and(
                    |actual| {
                        values.iter().any(|expected| {
                            entity_filter_values_equal(&actual, expected, *column_type)
                        })
                    },
                ),
            ),
            Self::ColumnRange {
                column,
                column_type,
                op,
                value,
            } => Ok(
                entity_snapshot_value(snapshot, schema_key, column, *column_type)?
                    .and_then(|actual| entity_filter_value_cmp(&actual, value))
                    .is_some_and(|ordering| op.matches(ordering)),
            ),
            Self::And(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                && right.matches_snapshot(snapshot, schema_key)?),
            Self::Or(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                || right.matches_snapshot(snapshot, schema_key)?),
        }
    }
}

fn entity_filter_value_in_statistics(
    value: &EntityFilterValue,
    statistics: &crate::columnar_row_group::RowGroupColumnStatistics,
    row_count: u32,
) -> Option<bool> {
    use crate::columnar_row_group::RowGroupScalar;
    if statistics.null_count == row_count && statistics.min.is_none() && statistics.max.is_none() {
        return Some(false);
    }
    match (value, statistics.min.as_ref()?, statistics.max.as_ref()?) {
        (
            EntityFilterValue::Boolean(value),
            RowGroupScalar::Boolean(min),
            RowGroupScalar::Boolean(max),
        ) => Some(min <= value && value <= max),
        (
            EntityFilterValue::Integer(value),
            RowGroupScalar::Int64(min),
            RowGroupScalar::Int64(max),
        ) => Some(min <= value && value <= max),
        (
            EntityFilterValue::Number(value),
            RowGroupScalar::Float64(min),
            RowGroupScalar::Float64(max),
        ) => (!value.is_nan() && !min.is_nan() && !max.is_nan())
            .then_some(min <= value && value <= max),
        (
            EntityFilterValue::String(value),
            RowGroupScalar::String(min),
            RowGroupScalar::String(max),
        ) => Some(min <= value && value <= max),
        _ => None,
    }
}

/// Whether any row of `statistics` can satisfy `op` against `value`.
///
/// A lower-bounded predicate can only be satisfied by a group whose **maximum**
/// clears the bound; an upper-bounded one by a group whose **minimum** does.
/// Comparing against the wrong end of the interval prunes groups that contain
/// matching rows, so the pairing here is the correctness core of range pruning.
///
/// `None` means "cannot tell" and the group is kept — this predicate is pushed
/// down as [`TableProviderFilterPushDown::Inexact`], so keeping too many groups
/// costs time while dropping too few is a wrong answer.
fn entity_filter_range_in_statistics(
    op: EntityRangeOp,
    value: &EntityFilterValue,
    statistics: &crate::columnar_row_group::RowGroupColumnStatistics,
    row_count: u32,
) -> Option<bool> {
    if statistics.null_count == row_count && statistics.min.is_none() && statistics.max.is_none() {
        return Some(false);
    }
    let bound = match op {
        EntityRangeOp::Gt | EntityRangeOp::GtEq => statistics.max.as_ref()?,
        EntityRangeOp::Lt | EntityRangeOp::LtEq => statistics.min.as_ref()?,
    };
    Some(op.matches(entity_scalar_value_cmp(bound, value)?))
}

/// Orders a row-group statistic against a filter literal.
///
/// Only the two types with a total order are comparable. `Float64` is
/// deliberately absent: NaN makes the order partial, and a partial order here
/// would prune a group that holds matching rows.
fn entity_scalar_value_cmp(
    scalar: &crate::columnar_row_group::RowGroupScalar,
    value: &EntityFilterValue,
) -> Option<std::cmp::Ordering> {
    use crate::columnar_row_group::RowGroupScalar;
    match (scalar, value) {
        (RowGroupScalar::Int64(scalar), EntityFilterValue::Integer(value)) => {
            Some(scalar.cmp(value))
        }
        (RowGroupScalar::String(scalar), EntityFilterValue::String(value)) => {
            Some(scalar.as_str().cmp(value.as_str()))
        }
        _ => None,
    }
}

/// Orders a decoded snapshot value against a filter literal.
///
/// `None` — a missing column, a type mismatch, or a value with no total order —
/// makes the comparison unsatisfied, matching SQL's treatment of a comparison
/// against NULL as unknown rather than true.
fn entity_filter_value_cmp(
    actual: &EntityFilterValue,
    expected: &EntityFilterValue,
) -> Option<std::cmp::Ordering> {
    match (actual, expected) {
        (EntityFilterValue::Integer(actual), EntityFilterValue::Integer(expected)) => {
            Some(actual.cmp(expected))
        }
        (EntityFilterValue::String(actual), EntityFilterValue::String(expected)) => {
            Some(actual.as_str().cmp(expected.as_str()))
        }
        _ => None,
    }
}

fn entity_filter_value_literal(
    expr: &Expr,
    column_type: EntityColumnType,
) -> Option<EntityFilterValue> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    let value = match literal {
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
    }?;
    match (&value, column_type) {
        (EntityFilterValue::Boolean(_), EntityColumnType::Boolean)
        | (EntityFilterValue::Integer(_), EntityColumnType::Integer)
        | (
            EntityFilterValue::Integer(_) | EntityFilterValue::Number(_),
            EntityColumnType::Number,
        )
        | (EntityFilterValue::String(_), EntityColumnType::String) => Some(value),
        _ => None,
    }
}

fn entity_snapshot_value(
    snapshot: Option<&JsonValue>,
    schema_key: &str,
    column: &str,
    column_type: EntityColumnType,
) -> Result<Option<EntityFilterValue>> {
    let Some(value) = snapshot.and_then(|snapshot| snapshot.get(column)) else {
        return Ok(None);
    };
    Ok(match column_type {
        EntityColumnType::String => match value {
            JsonValue::String(value) => Some(EntityFilterValue::String(value.clone())),
            _ => None,
        },
        EntityColumnType::Integer => {
            entity_i64_value(Some(value), schema_key, column)?.map(EntityFilterValue::Integer)
        }
        EntityColumnType::Number => {
            entity_f64_value(Some(value), schema_key, column)?.map(EntityFilterValue::Number)
        }
        EntityColumnType::Boolean => value.as_bool().map(EntityFilterValue::Boolean),
        EntityColumnType::Json => None,
        EntityColumnType::Timestamptz => value
            .as_str()
            .map(|value| EntityFilterValue::String(value.to_owned())),
    })
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

fn top_level_primary_key_columns(spec: &EntitySurfaceSpec) -> Vec<&str> {
    spec.primary_key_paths
        .iter()
        .map(|path| {
            let [column_name] = path.as_slice() else {
                return None;
            };
            spec.visible_column(column_name)
                .map(|column| column.name.as_str())
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

fn entity_pk_constraint_from_binary_filter(
    binary_expr: &BinaryExpr,
    primary_key_columns: &[&str],
    component_types: &[crate::entity_pk::EntityPkComponentType],
) -> Option<EntityPkConstraint> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    entity_pk_constraint_from_column_literal_filter(
        &binary_expr.left,
        &binary_expr.right,
        primary_key_columns,
        component_types,
    )
    .or_else(|| {
        entity_pk_constraint_from_column_literal_filter(
            &binary_expr.right,
            &binary_expr.left,
            primary_key_columns,
            component_types,
        )
    })
}

fn entity_pk_constraint_from_in_list_filter(
    in_list: &InList,
    primary_key_columns: &[&str],
    component_types: &[crate::entity_pk::EntityPkComponentType],
) -> Option<EntityPkConstraint> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if in_list.list.is_empty() {
        return None;
    }
    match column.name.as_str() {
        "lixcol_entity_pk" => in_list
            .list
            .iter()
            .map(string_expr_literal)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|value| {
                let parts = EntityPk::from_json_array_text(&value).ok()?.into_parts();
                EntityPk::from_external_parts(parts, component_types).ok()
            })
            .collect::<Option<BTreeSet<_>>>()
            .map(EntityPkConstraint::Full),
        column_name if primary_key_columns.contains(&column_name) => {
            let component_type =
                primary_key_component_type(column_name, primary_key_columns, component_types)?;
            let values = in_list
                .list
                .iter()
                .map(|expr| primary_key_expr_literal(expr, component_type))
                .collect::<Option<BTreeSet<_>>>()?;
            Some(EntityPkConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                values,
            )])))
        }
        _ => None,
    }
}

fn entity_pk_constraint_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    primary_key_columns: &[&str],
    component_types: &[crate::entity_pk::EntityPkComponentType],
) -> Option<EntityPkConstraint> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    match column.name.as_str() {
        "lixcol_entity_pk" => EntityPk::from_json_array_text(&string_expr_literal(literal_expr)?)
            .ok()
            .and_then(|identity| {
                EntityPk::from_external_parts(identity.into_parts(), component_types).ok()
            })
            .map(|identity| EntityPkConstraint::Full(BTreeSet::from([identity]))),
        column_name if primary_key_columns.contains(&column_name) => {
            let component_type =
                primary_key_component_type(column_name, primary_key_columns, component_types)?;
            let value = primary_key_expr_literal(literal_expr, component_type)?;
            Some(EntityPkConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                BTreeSet::from([value]),
            )])))
        }
        _ => None,
    }
}

fn primary_key_component_type(
    column_name: &str,
    primary_key_columns: &[&str],
    component_types: &[crate::entity_pk::EntityPkComponentType],
) -> Option<crate::entity_pk::EntityPkComponentType> {
    primary_key_columns
        .iter()
        .position(|candidate| *candidate == column_name)
        .and_then(|index| component_types.get(index))
        .copied()
}

fn primary_key_expr_literal(
    expr: &Expr,
    component_type: crate::entity_pk::EntityPkComponentType,
) -> Option<String> {
    use crate::entity_pk::EntityPkComponentType;

    if !matches!(component_type, EntityPkComponentType::Integer) {
        return string_expr_literal(expr);
    }
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    match literal {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::Int64(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value).to_string()),
        ScalarValue::UInt64(Some(value)) => {
            i64::try_from(*value).ok().map(|value| value.to_string())
        }
        _ => None,
    }
}

fn entity_pks_from_primary_key_parts(
    primary_key_columns: &[&str],
    component_types: &[crate::entity_pk::EntityPkComponentType],
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
    identities
        .into_iter()
        .map(|parts| EntityPk::from_external_parts(parts, component_types))
        .collect::<std::result::Result<BTreeSet<_>, _>>()
        .ok()
}

fn identity_matches_parts(
    identity: &EntityPk,
    primary_key_columns: &[&str],
    parts: &BTreeMap<String, BTreeSet<String>>,
) -> bool {
    if identity.components.len() != primary_key_columns.len() {
        return false;
    }
    primary_key_columns
        .iter()
        .zip(identity.components.iter())
        .all(|(column, component)| {
            parts
                .get(*column)
                .is_none_or(|values| values.contains(&component.external_string()))
        })
}

#[cfg(test)]
fn apply_entity_row_filters(
    rows: &mut Vec<MaterializedHotStateRow>,
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
        let mut matches = true;
        for filter in filters {
            if !filter.matches_snapshot(Some(&snapshot), &row.schema_key)? {
                matches = false;
                break;
            }
        }
        if matches {
            filtered_rows.push(row);
        }
    }
    *rows = filtered_rows;
    Ok(())
}

fn apply_entity_batch_filters(
    spec: &EntitySurfaceSpec,
    rows: MaterializedHotStateBatch,
    filters: &[EntityRowFilter],
) -> Result<FilteredEntityBatch> {
    if filters.is_empty() {
        return Ok(FilteredEntityBatch { rows });
    }
    // The batch is compacted in place. Rebuilding it row by row cloned every
    // surviving row's shared identity and snapshot buffers and then dropped
    // the originals, which is pure atomic refcount traffic over bytes that
    // never move.
    let mut failure: Option<DataFusionError> = None;
    let rows = rows.filter(
        |row| {
            if failure.is_some() {
                return false;
            }
            let native = match row.native_snapshot() {
                Some(native) => native,
                None => {
                    failure = Some(DataFusionError::Execution(format!(
                        "Schema v1 current-state row '{}' is missing its native scalar tuple",
                        row.schema_key()
                    )));
                    return false;
                }
            };
            let decoded = match crate::native_row::decode(&spec.native_schema, native) {
                Ok(decoded) => decoded,
                Err(error) => {
                    failure = Some(lix_error_to_datafusion_error(error));
                    return false;
                }
            };
            for filter in filters {
                match filter.matches_native(spec, row.entity_pk(), &decoded) {
                    Ok(true) => {}
                    Ok(false) => return false,
                    Err(error) => {
                        failure = Some(error);
                        return false;
                    }
                }
            }
            true
        },
        None,
    );
    if let Some(failure) = failure {
        return Err(failure);
    }
    Ok(FilteredEntityBatch { rows })
}

impl EntityRowFilter {
    fn matches_native(
        &self,
        spec: &EntitySurfaceSpec,
        entity_pk: &EntityPk,
        body: &[lix_schema::value_layout::BodyValue],
    ) -> Result<bool> {
        match self {
            Self::ColumnEq {
                column,
                column_type,
                value,
            } => Ok(entity_native_filter_value(spec, entity_pk, body, column)?
                .is_some_and(|actual| {
                    entity_filter_values_equal(&actual, value, *column_type)
                })),
            Self::ColumnIn {
                column,
                column_type,
                values,
            } => Ok(entity_native_filter_value(spec, entity_pk, body, column)?
                .is_some_and(|actual| {
                    values.iter().any(|expected| {
                        entity_filter_values_equal(&actual, expected, *column_type)
                    })
                })),
            Self::ColumnRange {
                column,
                op,
                value,
                ..
            } => Ok(entity_native_filter_value(spec, entity_pk, body, column)?
                .and_then(|actual| entity_filter_value_cmp(&actual, value))
                .is_some_and(|ordering| op.matches(ordering))),
            Self::And(left, right) => Ok(left.matches_native(spec, entity_pk, body)?
                && right.matches_native(spec, entity_pk, body)?),
            Self::Or(left, right) => Ok(left.matches_native(spec, entity_pk, body)?
                || right.matches_native(spec, entity_pk, body)?),
        }
    }
}

fn entity_native_filter_value(
    spec: &EntitySurfaceSpec,
    entity_pk: &EntityPk,
    body: &[lix_schema::value_layout::BodyValue],
    column_name: &str,
) -> Result<Option<EntityFilterValue>> {
    use crate::entity_pk::EntityPkComponent;
    use lix_schema::value_layout::BodyValue;

    if let Some(pk_ordinal) = spec
        .native_schema
        .primary_key
        .iter()
        .position(|name| name == column_name)
    {
        return Ok(match entity_pk.components.get(pk_ordinal) {
            Some(EntityPkComponent::Uuid(value)) => Some(EntityFilterValue::String(
                uuid::Uuid::from_bytes(*value).to_string(),
            )),
            Some(EntityPkComponent::Integer(value)) => Some(EntityFilterValue::Integer(*value)),
            Some(EntityPkComponent::String(value)) => {
                Some(EntityFilterValue::String(value.to_string()))
            }
            Some(EntityPkComponent::Bytes(value)) => Some(EntityFilterValue::String(
                base64::engine::general_purpose::STANDARD.encode(value),
            )),
            None => {
                return Err(DataFusionError::Execution(format!(
                    "native primary key arity mismatch for schema '{}'",
                    spec.schema_key
                )));
            }
        });
    }

    let value_ordinal = spec
        .native_schema
        .columns
        .iter()
        .filter(|column| !spec.native_schema.primary_key.contains(&column.name))
        .position(|column| column.name == column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "unknown native filter column '{}.{}'",
                spec.schema_key, column_name
            ))
        })?;
    Ok(match body.get(value_ordinal) {
        Some(BodyValue::Null) => None,
        Some(BodyValue::Text(value)) => Some(EntityFilterValue::String(value.clone())),
        Some(BodyValue::Uuid(value)) => {
            Some(EntityFilterValue::String(value.to_string()))
        }
        Some(BodyValue::Int8(value)) => Some(EntityFilterValue::Integer(*value)),
        Some(BodyValue::Float8(value)) => Some(EntityFilterValue::Number(*value)),
        Some(BodyValue::Boolean(value)) => Some(EntityFilterValue::Boolean(*value)),
        Some(BodyValue::Timestamptz(_)) | Some(BodyValue::Jsonb(_)) => None,
        None => {
            return Err(DataFusionError::Execution(format!(
                "native row body arity mismatch for schema '{}'",
                spec.schema_key
            )));
        }
    })
}
struct FilteredEntityBatch {
    rows: MaterializedHotStateBatch,
}

fn entity_hot_state_scan_request(
    schema_key: &str,
    active_branch_id: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
    force_snapshot_content: bool,
) -> HotStateScanRequest {
    HotStateScanRequest {
        filter: HotStateFilter {
            schema_keys: vec![schema_key.to_string()],
            branch_ids: active_branch_id
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..HotStateFilter::default()
        },
        projection: entity_hot_state_projection(projected_schema, force_snapshot_content),
        limit,
    }
}

fn entity_hot_state_projection(
    projected_schema: Option<&Schema>,
    force_snapshot_content: bool,
) -> HotStateProjection {
    let Some(schema) = projected_schema else {
        return HotStateProjection::default();
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
    HotStateProjection { columns }
}

fn projection_column_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|field| field.name().strip_prefix("lixcol_"))
        .map(str::to_string)
        .collect()
}

/// The filters that still need a row-shaped predicate after the exact
/// identity access path has consumed the ones it applies in full.
fn exact_identity_residual<'a>(
    analyzer: &EntityPrimaryKeyFilterAnalyzer<'_>,
    filters: &'a [Expr],
) -> Vec<&'a Expr> {
    filters
        .iter()
        .filter(|filter| !analyzer.supports(filter))
        .collect()
}

fn direct_entity_batch_eligible(
    schema: &Schema,
    request: &HotStateScanRequest,
    row_filters: &[EntityRowFilter],
) -> bool {
    // A range filter does not disqualify this route *when nothing better is
    // available*. Ranges are pushed down as `Inexact`, so DataFusion re-checks
    // them above the scan and this route may legitimately ignore them — which
    // keeps the pre-range fast path intact instead of demoting every range
    // query to the generic visibility scan. Equality and IN still disqualify
    // it, as they always have.
    //
    // But a range on an *indexed* column has something better: the index range
    // seek, which reaches this collection through resolved entity pks instead
    // of reading it end to end. That seek is resolved on the route this one
    // bypasses, so admitting the range here would silently disable it — the
    // fast full read would win the route and the seek would never run. When a
    // `declared_column_range` is present the range therefore disqualifies this
    // route, exactly as an equality on an indexed column already does.
    !schema.fields().is_empty()
        && matches!(request.filter.rows, HotStateRowFilter::All)
        && (row_filters.is_empty()
            || (request.filter.declared_column_range.is_none()
                && row_filters
                    .iter()
                    .all(|filter| matches!(filter, EntityRowFilter::ColumnRange { .. }))))
        && request.filter.file_ids.is_empty()
        && request.filter.constraints.is_empty()
        && schema
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("lixcol_"))
}

/// A provider-level physical projection: all requested columns are simple
/// string primary-key components stored verbatim in the current-state key.
/// DataFusion still plans and executes every relational operator above this
/// scan; the provider merely avoids JSON decoding to reproduce identity data.
fn direct_primary_key_projection_eligible(
    spec: &EntitySurfaceSpec,
    schema: &Schema,
    request: &HotStateScanRequest,
    row_filters: &[EntityRowFilter],
) -> bool {
    direct_entity_batch_eligible(schema, request, row_filters)
        // Exact identities retain the point-snapshot cache. This projection
        // capability is for ordered collection scans, not a replacement for
        // the row-addressable OLTP path.
        && request.filter.entity_pks.is_empty()
        && !schema.fields().is_empty()
        && schema
            .fields()
            .iter()
            .all(|field| simple_string_primary_key_index(spec, field.name()).is_some())
}

fn simple_string_primary_key_index(spec: &EntitySurfaceSpec, column_name: &str) -> Option<usize> {
    spec.primary_key_paths
        .iter()
        .position(|path| matches!(path.as_slice(), [name] if name == column_name))
        .filter(|index| {
            spec.primary_key_component_types.get(*index)
                == Some(&crate::entity_pk::EntityPkComponentType::String)
                && spec
                    .visible_column(column_name)
                    .is_some_and(|column| column.column_type == EntityColumnType::String)
        })
}

/// Selects the snapshot-to-Arrow implementation once per provider batch.
///
/// Exact primary-key scans retain their established serde-value fallback when
/// a tracked-only snapshot proof is unavailable. Broad scans are dominated by
/// decoding every snapshot, so their generic fallback uses the raw projection
/// decoder that visits only selected fields. This is a physical execution
/// choice; the SQL schema and result contract are the same in both cases.
#[derive(Clone, Copy)]
enum EntityBatchProjection {
    ParsedSnapshots,
    RawTrackedProjection,
}

impl EntityBatchProjection {
    fn for_request(request: &HotStateScanRequest) -> Self {
        if request.filter.entity_pks.is_empty() {
            Self::RawTrackedProjection
        } else {
            Self::ParsedSnapshots
        }
    }
}

fn entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedHotStateBatch,
    _projection: EntityBatchProjection,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }

    // Schema-v1 current-state rows have one durable representation. A missing
    // tuple is corruption, not permission to reinterpret a historical JSON
    // snapshot as current authority.
    entity_record_batch_from_native_rows(spec, schema, rows)
}

fn entity_record_batch_from_native_rows(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedHotStateBatch,
) -> Result<RecordBatch> {
    let expected_layout = lix_schema::value_layout::layout_id(&spec.native_schema)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let plan = lix_schema::value_layout::body_plan(&spec.native_schema);
    let decoded = rows
        .iter()
        .map(|row| {
            let native = row.native_snapshot().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Schema v1 current-state row '{}' is missing its native scalar tuple",
                    spec.schema_key
                ))
            })?;
            if native.layout_id != expected_layout {
                return Err(DataFusionError::Execution(format!(
                    "Schema v1 current-state row '{}' has a mismatched native layout",
                    spec.schema_key
                )));
            }
            lix_schema::value_layout::decode_body(&plan, &native.body)
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(system) = field.name().strip_prefix("lixcol_") {
                return entity_system_column_array(system, rows);
            }
            entity_native_column_array(spec, field.name(), rows, &decoded)
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

#[expect(trivial_casts)]
fn entity_native_column_array(
    spec: &EntitySurfaceSpec,
    column_name: &str,
    rows: &MaterializedHotStateBatch,
    decoded: &[Vec<lix_schema::value_layout::BodyValue>],
) -> Result<ArrayRef> {
    let schema = spec.native_schema.as_ref();
    let column = schema
        .columns
        .iter()
        .find(|column| column.name == column_name)
        .ok_or_else(|| DataFusionError::Execution(format!("unknown native column {column_name}")))?;
    let pk_ordinal = schema.primary_key.iter().position(|name| name == column_name);
    let value_ordinal = schema
        .columns
        .iter()
        .filter(|candidate| !schema.primary_key.contains(&candidate.name))
        .position(|candidate| candidate.name == column_name);
    let values = rows.iter().enumerate().map(|(row_index, row)| {
        if let Some(pk_ordinal) = pk_ordinal {
            return Ok(Some(NativeCellRef::Pk(
                row.entity_pk()
                    .components
                    .get(pk_ordinal)
                    .ok_or_else(|| DataFusionError::Execution("native primary key arity mismatch".into()))?,
            )));
        }
        let value_ordinal = value_ordinal.ok_or_else(|| {
            DataFusionError::Execution(format!("native value column '{column_name}' is not in layout"))
        })?;
        Ok(Some(NativeCellRef::Body(&decoded[row_index][value_ordinal])))
    }).collect::<Result<Vec<_>>>()?;
    use lix_schema::DataType;
    Ok(match column.data_type {
        DataType::Text | DataType::Uuid | DataType::Jsonb => Arc::new(StringArray::from(
            values.iter().map(|value| native_cell_text(value.as_ref().copied(), column.data_type)).collect::<Result<Vec<_>>>()?
        )) as ArrayRef,
        DataType::Int8 => Arc::new(Int64Array::from(values.iter().map(|value| match value.as_ref().copied() {
            Some(NativeCellRef::Pk(crate::entity_pk::EntityPkComponent::Integer(value))) => Ok(Some(*value)),
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Int8(value))) => Ok(Some(*value)),
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Null)) | None => Ok(None),
            _ => Err(DataFusionError::Execution("native int8 cell kind mismatch".into())),
        }).collect::<Result<Vec<_>>>()?)) as ArrayRef,
        DataType::Float8 => Arc::new(Float64Array::from(values.iter().map(|value| match value.as_ref().copied() {
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Float8(value))) => Ok(Some(*value)),
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Null)) | None => Ok(None),
            _ => Err(DataFusionError::Execution("native float8 cell kind mismatch".into())),
        }).collect::<Result<Vec<_>>>()?)) as ArrayRef,
        DataType::Boolean => Arc::new(BooleanArray::from(values.iter().map(|value| match value.as_ref().copied() {
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Boolean(value))) => Ok(Some(*value)),
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Null)) | None => Ok(None),
            _ => Err(DataFusionError::Execution("native boolean cell kind mismatch".into())),
        }).collect::<Result<Vec<_>>>()?)) as ArrayRef,
        DataType::Timestamptz => Arc::new(TimestampMicrosecondArray::from(values.iter().map(|value| match value.as_ref().copied() {
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Timestamptz(value))) => Ok(Some(*value)),
            Some(NativeCellRef::Body(lix_schema::value_layout::BodyValue::Null)) | None => Ok(None),
            _ => Err(DataFusionError::Execution("native timestamptz cell kind mismatch".into())),
        }).collect::<Result<Vec<_>>>()?).with_timezone("UTC")) as ArrayRef,
    })
}

#[derive(Clone, Copy)]
enum NativeCellRef<'a> {
    Pk(&'a crate::entity_pk::EntityPkComponent),
    Body(&'a lix_schema::value_layout::BodyValue),
}

fn native_cell_text(
    value: Option<NativeCellRef<'_>>,
    kind: lix_schema::DataType,
) -> Result<Option<String>> {
    use lix_schema::value_layout::BodyValue;
    Ok(match value {
        None | Some(NativeCellRef::Body(BodyValue::Null)) => None,
        Some(NativeCellRef::Pk(value)) => Some(value.external_string()),
        Some(NativeCellRef::Body(BodyValue::Text(value))) => Some(value.clone()),
        Some(NativeCellRef::Body(BodyValue::Uuid(value))) => Some(value.to_string()),
        Some(NativeCellRef::Body(BodyValue::Jsonb(value))) if kind == lix_schema::DataType::Jsonb => {
            Some(serde_json::to_string(value).map_err(|error| {
                DataFusionError::Execution(format!("native jsonb encoding failed: {error}"))
            })?)
        }
        _ => return Err(DataFusionError::Execution("native text cell kind mismatch".into())),
    })
}

fn entity_primary_key_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    entity_pks: Vec<EntityPk>,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let component_index = simple_string_primary_key_index(spec, field.name()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "entity primary-key projection cannot serve column '{}' for schema '{}'",
                    field.name(), spec.schema_key
                ))
            })?;
            let values = entity_pks
                .iter()
                .map(|entity_pk| match entity_pk.components.as_slice().get(component_index) {
                    Some(crate::entity_pk::EntityPkComponent::String(value)) => Ok(Some(value.as_ref())),
                    _ => Err(DataFusionError::Execution(format!(
                        "entity primary-key projection found an invalid key component for schema '{}' column '{}'",
                        spec.schema_key, field.name()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(StringArray::from(values));
            Ok(array)
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

/// Materialize `lixcol_*` system columns from borrowed batch rows.
///
/// Identity dictionaries and payload arenas remain owned by the live-state
/// batch until Arrow has copied the selected values into its output buffers;
/// no terminal row DTOs are manufactured on this path.
fn entity_system_column_array(
    column_name: &str,
    rows: &MaterializedHotStateBatch,
) -> Result<ArrayRef> {
    #[expect(trivial_casts)]
    let array = match column_name {
        "entity_pk" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.entity_pk().as_json_array_text().map(Some))
                .collect::<std::result::Result<Vec<_>, LixError>>()
                .map_err(lix_error_to_datafusion_error)?,
        )) as ArrayRef,
        "schema_key" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.schema_key())),
        )) as ArrayRef,
        "file_id" => {
            Arc::new(StringArray::from_iter(rows.iter().map(|row| row.file_id()))) as ArrayRef
        }
        "metadata" => Arc::new(StringArray::from_iter(rows.iter().map(|row| {
            row.metadata()
                .map(AsRef::<str>::as_ref)
                .map(crate::serialize_row_metadata)
        }))) as ArrayRef,
        "created_at" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.created_at().to_string())),
        )) as ArrayRef,
        "updated_at" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.updated_at().to_string())),
        )) as ArrayRef,
        "global" => Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.global())),
        )) as ArrayRef,
        "change_id" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.change_id().map(|id| id.to_string())),
        )) as ArrayRef,
        "commit_id" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.commit_id().map(|id| id.to_string())),
        )) as ArrayRef,
        "untracked" => Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.untracked())),
        )) as ArrayRef,
        "branch_id" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.branch_id())),
        )) as ArrayRef,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "sql2 entity provider does not support system column 'lixcol_{column_name}'"
            )));
        }
    };
    Ok(array)
}

pub(super) fn parse_snapshot(snapshot_content: Option<&str>) -> Result<Option<JsonValue>> {
    snapshot_content
        .map(|snapshot| {
            parse_snapshot_value(snapshot).map_err(|error| {
                DataFusionError::Execution(format!(
                    "sql2 entity provider expected valid snapshot_content JSON: {error}"
                ))
            })
        })
        .transpose()
}

fn parse_snapshot_value(snapshot: &str) -> serde_json::Result<JsonValue> {
    #[cfg(test)]
    ENTITY_SNAPSHOT_PARSE_COUNT.with(|count| count.set(count.get() + 1));
    serde_json::from_str(snapshot)
}

/// Materializes only the top-level fields named in `wanted`.
///
/// A row predicate reads a handful of named columns, so building the whole
/// snapshot map charges every *scanned* row for fields the predicate never
/// looks at. The returned value is a partial object that is only ever read
/// through `EntityRowFilter::matches_snapshot`; it must not reach projection.
///
/// Semantics are those of `serde_json::from_str::<JsonValue>` restricted to
/// `wanted`: the whole document is still validated (so malformed JSON and
/// trailing bytes are still rejected here), duplicate keys are still
/// last-wins, and a non-object snapshot still yields a value from which no
/// column can be read.
fn parse_snapshot_filter_columns(
    snapshot: &str,
    wanted: &BTreeSet<&str>,
) -> serde_json::Result<JsonValue> {
    #[cfg(test)]
    ENTITY_SNAPSHOT_FILTER_PARSE_COUNT.with(|count| count.set(count.get() + 1));
    let mut deserializer = serde_json::Deserializer::from_str(snapshot);
    let value = FilterColumnSeed { wanted }.deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

struct FilterColumnSeed<'a> {
    wanted: &'a BTreeSet<&'a str>,
}

impl<'de> DeserializeSeed<'de> for FilterColumnSeed<'_> {
    type Value = JsonValue;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(FilterColumnVisitor {
            wanted: self.wanted,
        })
    }
}

struct FilterColumnVisitor<'a> {
    wanted: &'a BTreeSet<&'a str>,
}

/// Every non-object snapshot collapses to `Null`, which is exactly what
/// `JsonValue::get` on the original non-object snapshot would have produced
/// for any column: `None`.
macro_rules! filter_column_scalar {
    ($name:ident, $ty:ty) => {
        fn $name<E>(self, _value: $ty) -> std::result::Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(JsonValue::Null)
        }
    };
}

impl<'de> Visitor<'de> for FilterColumnVisitor<'_> {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON entity snapshot")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut object = serde_json::Map::new();
        while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
            if self.wanted.contains(key.as_ref()) {
                let value = map.next_value::<JsonValue>()?;
                object.insert(key.into_owned(), value);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(JsonValue::Object(object))
    }

    fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(JsonValue::Null)
    }

    filter_column_scalar!(visit_bool, bool);
    filter_column_scalar!(visit_i64, i64);
    filter_column_scalar!(visit_u64, u64);
    filter_column_scalar!(visit_i128, i128);
    filter_column_scalar!(visit_u128, u128);
    filter_column_scalar!(visit_f64, f64);
    filter_column_scalar!(visit_str, &str);
    filter_column_scalar!(visit_borrowed_str, &'de str);
    filter_column_scalar!(visit_string, String);

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(JsonValue::Null)
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(JsonValue::Null)
    }
}

#[cfg(test)]
thread_local! {
    static ENTITY_SNAPSHOT_PARSE_COUNT: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

// Counted inside the predicate-column parser so a test can distinguish
// "the streaming filter ran" from "the filter route was never taken", which
// read identically when only full parses were counted.
#[cfg(test)]
thread_local! {
    static ENTITY_SNAPSHOT_FILTER_PARSE_COUNT: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn reset_entity_snapshot_filter_parse_count() {
    ENTITY_SNAPSHOT_FILTER_PARSE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn entity_snapshot_filter_parse_count() -> usize {
    ENTITY_SNAPSHOT_FILTER_PARSE_COUNT.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_entity_snapshot_parse_count() {
    ENTITY_SNAPSHOT_PARSE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn entity_snapshot_parse_count() -> usize {
    ENTITY_SNAPSHOT_PARSE_COUNT.with(std::cell::Cell::get)
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

pub(super) fn entity_i64_value(
    value: Option<&JsonValue>,
    schema_key: &str,
    column_name: &str,
) -> Result<Option<i64>> {
    json_bigint_value(value, schema_key, column_name).map_err(lix_error_to_datafusion_error)
}

pub(super) fn entity_f64_value(
    value: Option<&JsonValue>,
    schema_key: &str,
    column_name: &str,
) -> Result<Option<f64>> {
    json_double_value(value, schema_key, column_name).map_err(lix_error_to_datafusion_error)
}

fn json_to_string(value: &JsonValue) -> Result<String> {
    serde_json::to_string(value).map_err(|error| {
        DataFusionError::Execution(format!("failed to render JSON value: {error}"))
    })
}

#[cfg(test)]
#[expect(trivial_casts)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
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
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk as TestEntityPk;
    use crate::hot_state::{
        HotStateFilter, HotStateProjection, HotStateReader, HotStateRowFilter, HotStateScanRequest,
        MaterializedHotStateBatch, MaterializedHotStateBatchBuilder, MaterializedHotStateRow,
    };
    use crate::sql2::catalog::{
        EntityColumnType, EntitySurfaceShape, derive_entity_surface_spec_from_schema,
        entity_surface_schema, schema_exposed_as_entity_history_surface,
        schema_exposed_as_entity_surface,
    };

    struct EmptyHotStateReader;
    struct EmptyBranchRefReader;

    #[derive(Default)]
    struct TestCachingEntitySnapshotReader {
        batch: Mutex<Option<Arc<RecordBatch>>>,
    }

    #[async_trait]
    impl crate::sql2::EntitySnapshotReader for TestCachingEntitySnapshotReader {
        async fn scan_entity_snapshots(
            &self,
            _request: HotStateScanRequest,
        ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
            Ok(None)
        }

        async fn cached_entity_columnar_batch(
            &self,
            _layout: &crate::sql2::entity_batch::EntityColumnarScanLayout,
            _group_index: usize,
            _shadow_identity_digest: [u8; 32],
            _projection: &[usize],
        ) -> Result<Option<Arc<RecordBatch>>, LixError> {
            Ok(self.batch.lock().expect("test batch cache lock").clone())
        }

        async fn cache_entity_columnar_batch(
            &self,
            _layout: &crate::sql2::entity_batch::EntityColumnarScanLayout,
            _group_index: usize,
            _shadow_identity_digest: [u8; 32],
            _projection: Vec<usize>,
            batch: Arc<RecordBatch>,
        ) -> Result<Arc<RecordBatch>, LixError> {
            let mut resident = self.batch.lock().expect("test batch cache lock");
            Ok(Arc::clone(resident.get_or_insert(batch)))
        }
    }

    #[async_trait]
    impl HotStateReader for EmptyHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(vec![].into())
        }
    }

    #[async_trait]
    impl BranchRefReader for EmptyBranchRefReader {
        async fn load_head(&self, _branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new().into())
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
            "01920000-0000-7000-8000-0000000000a1"
        }

        fn functions(&self) -> crate::functions::FunctionProviderHandle {
            crate::functions::FunctionProviderHandle::system()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new().into())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobId],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }

        async fn scan_hot_state_batch(
            &mut self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::default())
        }

        async fn load_exact_hot_state_batch(
            &mut self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            Ok(crate::hot_state::MaterializedHotStateExactBatch::from_rows(
                vec![None; request.rows.len()],
            ))
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
            _write: crate::transaction_types::TransactionWrite,
        ) -> Result<crate::transaction_types::TransactionWriteOutcome, LixError> {
            panic!("raw DataFusion entity INSERT must never stage writes");
        }

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            _rows: crate::transaction_types::TypedMutationJournalBatch,
        ) -> Result<crate::transaction_types::TransactionWriteOutcome, LixError> {
            panic!("raw DataFusion entity INSERT must never stage transaction journals");
        }

        async fn can_stage_typed_mutation_journal_replace(
            &mut self,
            _schema_key: &str,
            _live_count: u64,
            _ordered_identity_digest: [u8; 32],
        ) -> Result<bool, LixError> {
            Ok(false)
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
        let provider = SpecTableProvider::new(Arc::new(super::EntitySpec::active_with_write(
            entity_insert_spec_with_primary_key(),
            write_ctx.clone(),
            empty_branch_ref(),
        )));
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
            error.to_string().contains("not implemented"),
            "unexpected error: {error}"
        );
    }

    fn live_row() -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("entity-1"),
            schema_key: "project_message".to_string(),
            file_id: None,
            snapshot_content: Some(
                "{\"body\":\"hello\",\"rating\":4.5,\"count\":7,\"enabled\":true,\"meta\":{\"x\":1}}"
                    .into(),
            ),
            metadata: Some(json!({"source": "test"}).to_string().into()),
            deleted: false,
            branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
            change_id: Some(ChangeId::for_test_label("change-a")),
            commit_id: Some(CommitId::for_test_label("commit-a")),
            global: false,
            untracked: false,
            created_at: LixTimestamp::expect_parse("test created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("test updated_at", "2026-04-23T01:00:00Z"),
        }
    }

    fn live_batch(rows: Vec<MaterializedHotStateRow>) -> MaterializedHotStateBatch {
        MaterializedHotStateBatch::from_rows(rows)
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

    #[test]
    fn direct_entity_batch_accepts_exact_payload_reads() {
        let payload_schema = Schema::new(vec![Field::new("body", DataType::Utf8, true)]);
        let system_schema = Schema::new(vec![Field::new("lixcol_entity_pk", DataType::Utf8, true)]);
        let mut request = HotStateScanRequest::default();
        assert!(super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));

        request
            .filter
            .entity_pks
            .push(crate::entity_pk::EntityPk::single("row"));
        assert!(super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.entity_pks.clear();

        request.filter.file_ids.push(crate::NullableKeyFilter::Null);
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.file_ids.clear();

        request
            .filter
            .constraints
            .push(crate::hot_state::ScanConstraint {
                field: crate::hot_state::ScanField::EntityPk,
                operator: crate::hot_state::ScanOperator::Eq(crate::Value::Text("row".to_string())),
            });
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.constraints.clear();

        request.filter.rows = HotStateRowFilter::None;
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.rows = HotStateRowFilter::All;

        assert!(!super::direct_entity_batch_eligible(
            &system_schema,
            &request,
            &[]
        ));
        assert!(!super::direct_entity_batch_eligible(
            &Schema::empty(),
            &request,
            &[]
        ));
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[super::EntityRowFilter::ColumnEq {
                column: "body".to_string(),
                column_type: EntityColumnType::String,
                value: super::EntityFilterValue::String("hello".to_string()),
            }]
        ));
    }

    #[test]
    fn direct_primary_key_projection_uses_identity_columns_without_snapshot_decode() {
        let spec = entity_insert_spec_with_primary_key();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let request = HotStateScanRequest::default();
        assert!(super::direct_primary_key_projection_eligible(
            &spec,
            schema.as_ref(),
            &request,
            &[]
        ));

        let batch = super::entity_primary_key_record_batch(
            &spec,
            Arc::clone(&schema),
            vec![crate::entity_pk::EntityPk::single("identity-1")],
        )
        .expect("identity projection should build an Arrow batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("identity column should be utf8");
        assert_eq!(values.value(0), "identity-1");

        let payload_schema = Schema::new(vec![Field::new("body", DataType::Utf8, true)]);
        assert!(!super::direct_primary_key_projection_eligible(
            &spec,
            &payload_schema,
            &request,
            &[]
        ));

        let mut exact_request = request.clone();
        exact_request
            .filter
            .entity_pks
            .push(crate::entity_pk::EntityPk::single("identity-1"));
        assert!(!super::direct_primary_key_projection_eligible(
            &spec,
            schema.as_ref(),
            &exact_request,
            &[]
        ));
    }

    #[test]
    fn zero_column_entity_batches_keep_row_count_on_the_generic_path() {
        let spec = entity_insert_spec_with_primary_key();
        let rows = live_batch(vec![live_row(), live_row()]);
        let batch = entity_record_batch(
            &spec,
            Arc::new(Schema::empty()),
            &rows,
            super::EntityBatchProjection::ParsedSnapshots,
        )
        .expect("generic zero-column entity batch should build");
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), rows.len());
    }

    #[test]
    fn filtered_entity_scan_reads_authenticated_native_cells_without_snapshot_dom() {
        let spec = Arc::new(
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
        );
        let mut winner = live_row();
        winner.untracked = true;
        let rejected = MaterializedHotStateRow {
            snapshot_content: Some(r#"{"body":"goodbye"}"#.into()),
            ..live_row()
        };
        let filter = super::EntityRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: EntityColumnType::String,
            value: super::EntityFilterValue::String("hello".to_string()),
        };

        super::reset_entity_snapshot_parse_count();
        super::reset_entity_snapshot_filter_parse_count();
        let rows = vec![winner, rejected];
        let native = rows
            .iter()
            .map(|row| {
                let snapshot = serde_json::from_str(
                    row.snapshot_content.as_deref().expect("test snapshot"),
                )
                .expect("test snapshot JSON");
                crate::native_row::encode(
                    &spec.native_schema,
                    &row.entity_pk,
                    row.branch_id.as_ref(),
                    row.file_id.as_deref(),
                    row.untracked,
                    &snapshot,
                )
                .expect("test native row")
            })
            .collect::<Vec<_>>();
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(rows.len());
        for (index, (row, native)) in rows.into_iter().zip(native).enumerate() {
            builder.push_owned(row);
            builder.set_native_snapshot(
                index,
                crate::hot_state::NativeRowSnapshot {
                    layout_id: native.layout_id,
                    owner_digest: native.owner_digest,
                    body: native.body,
                },
            );
        }
        let rows = builder.finish();
        let filtered = super::apply_entity_batch_filters(&spec, rows, &[filter])
        .expect("entity filter should select the matching row");
        assert_eq!(filtered.rows.len(), 1);
        assert_eq!(
            super::entity_snapshot_filter_parse_count(),
            0,
            "native scalar predicates must not inspect a JSON snapshot"
        );
        assert_eq!(
            super::entity_snapshot_parse_count(),
            0,
            "deciding a row predicate must not build a snapshot DOM"
        );

        let batch = entity_record_batch(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &filtered.rows,
            super::EntityBatchProjection::RawTrackedProjection,
        )
        .expect("mixed-retention projection should build the batch");
        assert_eq!(
            super::entity_snapshot_parse_count(),
            filtered.rows.len(),
            "projection parses the surviving rows only"
        );
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body is utf8")
                .value(0),
            "hello"
        );
    }

    /// The predicate parser sees a partial object. Anything it gets wrong is a
    /// wrong answer, not a slow one, so the rejection cases are asserted as
    /// hard as the acceptance cases.
    #[test]
    fn predicate_column_parse_matches_the_full_snapshot_parse() {
        fn wanted<'a>(names: &[&'a str]) -> std::collections::BTreeSet<&'a str> {
            names.iter().copied().collect()
        }

        for snapshot in [
            // Ordinary object: the wanted column sits after several others.
            r#"{"a":1,"body":"hello","c":[1,2,{"d":null}],"e":"tail"}"#,
            // Wanted column first, and absent columns around it.
            r#"{"body":"hello"}"#,
            // Duplicate keys: serde_json is last-wins, and so is the partial
            // parse. A first-wins reader would return a different row here.
            r#"{"body":"first","body":"hello"}"#,
            // The wanted column is absent entirely.
            r#"{"a":1,"z":2}"#,
            // Escapes and non-ASCII inside a value the predicate skips.
            r#"{"skipped":"é\n\"x\"","body":"hello"}"#,
            // An escaped predicate KEY. A key comparison taken straight
            // off the input bytes without unescaping would miss this
            // column and silently drop the row.
            r#"{"\u0062ody":"hello"}"#,
            // An escaped predicate VALUE, which the filter later compares
            // by equality against an unescaped literal.
            r#"{"body":"\u0068ello"}"#,
            // Nested objects that themselves contain the wanted key name.
            r#"{"nested":{"body":"decoy"},"body":"hello"}"#,
            // Empty object.
            r#"{}"#,
            // Non-object roots: no column is readable from any of them.
            r#"[1,2,3]"#,
            r#""hello""#,
            r#"12345678901234567890"#,
            r#"1.5"#,
            r#"true"#,
            r#"null"#,
        ] {
            let full = super::parse_snapshot_value(snapshot)
                .unwrap_or_else(|error| panic!("{snapshot} should parse: {error}"));
            let partial = super::parse_snapshot_filter_columns(snapshot, &wanted(&["body"]))
                .unwrap_or_else(|error| panic!("{snapshot} should parse partially: {error}"));
            assert_eq!(
                partial.get("body"),
                full.get("body"),
                "partial parse disagreed with the full parse on {snapshot}"
            );
        }

        // Multiple predicate columns are all recovered in one pass.
        let both = super::parse_snapshot_filter_columns(
            r#"{"body":"hello","skip":9,"lane":"L1"}"#,
            &wanted(&["body", "lane"]),
        )
        .expect("multi-column predicate parse");
        assert_eq!(both.get("body"), Some(&json!("hello")));
        assert_eq!(both.get("lane"), Some(&json!("L1")));
        assert_eq!(both.get("skip"), None, "unwanted columns stay unparsed");

        // Rejection: a malformed document must still fail here, even when the
        // damage is entirely inside a value the predicate skips over.
        for malformed in [
            r#"{"body":"hello","skip":}"#,
            r#"{"body":"hello""#,
            r#"{"skip":[1,2,,3],"body":"hello"}"#,
            r#"{"body":"hello"} trailing"#,
            r#""#,
        ] {
            assert!(
                super::parse_snapshot_value(malformed).is_err(),
                "fixture {malformed} must be malformed for the full parser"
            );
            assert!(
                super::parse_snapshot_filter_columns(malformed, &wanted(&["body"])).is_err(),
                "the predicate parser must reject {malformed} exactly as the full parser does"
            );
        }
    }

    #[test]
    fn collect_filter_columns_walks_the_whole_predicate_tree() {
        let leaf = |column: &str| super::EntityRowFilter::ColumnEq {
            column: column.to_string(),
            column_type: EntityColumnType::String,
            value: super::EntityFilterValue::String("x".to_string()),
        };
        let filter = super::EntityRowFilter::And(
            Box::new(super::EntityRowFilter::Or(
                Box::new(leaf("left")),
                Box::new(super::EntityRowFilter::ColumnIn {
                    column: "middle".to_string(),
                    column_type: EntityColumnType::String,
                    values: vec![super::EntityFilterValue::String("y".to_string())],
                }),
            )),
            Box::new(super::EntityRowFilter::And(
                Box::new(leaf("right")),
                // A range leaf. Its column drives the same partial parse, and a
                // range column missing from this set reads `None` from the partial
                // snapshot and drops every row it should have matched.
                Box::new(super::EntityRowFilter::ColumnRange {
                    column: "ranged".to_string(),
                    column_type: EntityColumnType::Integer,
                    op: super::EntityRangeOp::GtEq,
                    value: super::EntityFilterValue::Integer(1),
                }),
            )),
        );
        let mut columns = std::collections::BTreeSet::new();
        filter.collect_filter_columns(&mut columns);
        assert_eq!(
            columns.into_iter().collect::<Vec<_>>(),
            vec!["left", "middle", "ranged", "right"],
            "a column missed here silently drops the predicate that reads it"
        );
    }

    #[test]
    fn unfiltered_parsed_projection_parses_each_row_once() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": { "body": { "type": "string" } }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let rows = live_batch(vec![live_row(), live_row()]);
        super::reset_entity_snapshot_parse_count();
        entity_record_batch(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &rows,
            super::EntityBatchProjection::ParsedSnapshots,
        )
        .expect("parsed entity projection should build");
        assert_eq!(super::entity_snapshot_parse_count(), rows.len());
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
                    "count": { "type": "integer" },
                    "meta": { "type": "object" }
                },
                "required": ["id", "kind", "score", "count"]
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
        for schema_key in [
            "lix_binary_blob_ref",
            "lix_change",
            "lix_undo_redo_marker",
            "lix_collection_generation",
            "lix_directory_descriptor",
            "lix_file_descriptor",
        ] {
            assert!(!schema_exposed_as_entity_surface(schema_key));
            assert!(!schema_exposed_as_entity_history_surface(schema_key));
        }
        assert!(schema_exposed_as_entity_surface("project_message"));
        assert!(schema_exposed_as_entity_surface("lix_checkpoint"));
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
    fn read_schema_keeps_defaulted_required_identity_non_null() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "uuidv7()" },
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        assert!(
            !schema
                .field_with_name("id")
                .expect("id field")
                .is_nullable(),
            "read nullability must not encode that INSERT may omit a defaulted id"
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

        let batch = entity_record_batch(
            &spec,
            schema,
            &live_batch(vec![live_row()]),
            super::EntityBatchProjection::ParsedSnapshots,
        )
        .expect("entity batch should build");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body column")
                .as_any()
                .downcast_ref::<StringArray>()
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
                .downcast_ref::<StringArray>()
                .expect("entity pk is string")
                .value(0),
            "[\"entity-1\"]"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch id column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch id is string")
                .value(0),
            "01920000-0000-7000-8000-0000000000a1"
        );
    }

    #[test]
    fn exact_primary_key_batches_keep_the_existing_json_and_scalar_projection_contract() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/body"],
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
        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        // Deliberately noncanonical nested JSON distinguishes the established
        // serde-value projection from the broad tracked raw-byte path.
        let row = MaterializedHotStateRow {
            snapshot_content: Some(
                r#"{"body":"hello","rating":4.5,"count":7,"enabled":true,"meta":{"z":2,"a":1}}"#
                    .into(),
            ),
            ..live_row()
        };
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                entity_pks: vec![row.entity_pk.clone()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection::default(),
            limit: None,
        };
        let projection = super::EntityBatchProjection::for_request(&request);
        assert!(matches!(
            projection,
            super::EntityBatchProjection::ParsedSnapshots
        ));

        let batch = entity_record_batch(&spec, schema, &live_batch(vec![row]), projection)
            .expect("exact primary-key batch should build");
        assert_eq!(
            batch
                .column_by_name("meta")
                .expect("meta column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("meta is JSON text")
                .value(0),
            r#"{"a":1,"z":2}"#,
            "exact primary-key reads retain the old parse-and-render JSON semantics"
        );
        assert_eq!(
            batch
                .column_by_name("count")
                .expect("count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is i64")
                .value(0),
            7,
            "exact primary-key reads retain scalar projection semantics"
        );
    }

    #[test]
    fn untracked_broad_batches_keep_duplicate_key_last_wins_scalar_semantics() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "count": { "type": "integer" }
                }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let batch = entity_record_batch(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &live_batch(vec![MaterializedHotStateRow {
                snapshot_content: Some(r#"{"body":"sidecar","count":"bad","count":7}"#.into()),
                untracked: true,
                ..live_row()
            }]),
            super::EntityBatchProjection::RawTrackedProjection,
        )
        .expect("untracked broad batch must use the established parser path");

        assert_eq!(
            batch
                .column_by_name("count")
                .expect("count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count is i64")
                .value(0),
            7,
            "the later duplicate value must replace an earlier invalid value"
        );
    }

    #[test]
    fn canonical_tracked_raw_batch_matches_parsed_batch_for_all_system_columns() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/body"],
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
        let branch_snapshot = crate::transaction_types::TransactionJson::from_value(
            json!({
                "body": "branch-row",
                "rating": 4.5,
                "count": 7,
                "enabled": true,
                "meta": {"z": 2, "a": 1}
            }),
            "canonical branch projection test",
        )
        .expect("branch snapshot should normalize");
        let global_snapshot = crate::transaction_types::TransactionJson::from_value(
            json!({
                "body": "global-row",
                "rating": 1.5,
                "count": 9,
                "enabled": false,
                "meta": {"d": 4, "c": 3}
            }),
            "canonical global projection test",
        )
        .expect("global snapshot should normalize");
        let branch_row = MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("branch-row"),
            file_id: Some("file-branch".to_string()),
            snapshot_content: Some(branch_snapshot.normalized().into()),
            metadata: Some(r#"{"source":"branch"}"#.into()),
            ..live_row()
        };
        let global_row = MaterializedHotStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("global-row"),
            file_id: None,
            snapshot_content: Some(global_snapshot.normalized().into()),
            metadata: Some(r#"{"source":"global"}"#.into()),
            branch_id: "global".into(),
            global: true,
            change_id: Some(ChangeId::for_test_label("change-global")),
            commit_id: Some(CommitId::for_test_label("commit-global")),
            ..live_row()
        };
        let rows = live_batch(vec![branch_row, global_row]);
        let schema = entity_surface_schema(&spec, EntitySurfaceShape::ByBranch);
        let parsed = entity_record_batch(
            &spec,
            Arc::clone(&schema),
            &rows,
            super::EntityBatchProjection::ParsedSnapshots,
        )
        .expect("parsed batch should build");
        let raw = entity_record_batch(
            &spec,
            schema,
            &rows,
            super::EntityBatchProjection::RawTrackedProjection,
        )
        .expect("raw tracked batch should build");

        for field in [
            "lixcol_metadata",
            "lixcol_entity_pk",
            "lixcol_file_id",
            "lixcol_branch_id",
            "lixcol_global",
            "lixcol_untracked",
        ] {
            assert!(
                raw.schema().field_with_name(field).is_ok(),
                "missing {field}"
            );
        }
        assert_eq!(raw.schema(), parsed.schema());
        assert_eq!(record_batch_scalars(&raw), record_batch_scalars(&parsed));
    }

    fn record_batch_scalars(batch: &RecordBatch) -> Vec<Vec<ScalarValue>> {
        (0..batch.num_rows())
            .map(|row_index| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        ScalarValue::try_from_array(array.as_ref(), row_index)
                            .expect("test record batch value should materialize")
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn schema_v1_current_state_missing_native_tuple_fails_closed() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "project_message",
                "columns": [
                    {"name": "id", "type": "text", "nullable": false},
                    {"name": "body", "type": "text", "nullable": true}
                ],
                "primary_key": ["id"]
            }))
            .expect("schema should derive entity surface spec"),
        );
        let error = entity_record_batch(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &live_batch(vec![MaterializedHotStateRow {
                snapshot_content: Some("{not-json".into()),
                ..live_row()
            }]),
            super::EntityBatchProjection::RawTrackedProjection,
        )
        .expect_err("a JSON snapshot must not substitute for a missing native tuple");

        assert!(matches!(
            error,
            datafusion::common::DataFusionError::Execution(_)
        ));
        assert!(
            error
                .to_string()
                .contains("missing its native scalar tuple"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn bigint_projection_normalizes_integral_reals_and_rejects_invalid_values() {
        for (raw, expected) in [
            ("1.0", 1_i64),
            ("-0.0", 0_i64),
            ("9223372036854775807", i64::MAX),
            ("-9223372036854775808", i64::MIN),
        ] {
            let value =
                serde_json::from_str::<serde_json::Value>(raw).expect("test value should parse");
            assert_eq!(
                super::entity_i64_value(Some(&value), "integer_contract", "count")
                    .expect("in-range integral JSON number should project"),
                Some(expected),
                "{raw}"
            );
        }

        for raw in ["1.5", "9223372036854775808", "\"1\""] {
            let value =
                serde_json::from_str::<serde_json::Value>(raw).expect("test value should parse");
            let error = super::entity_i64_value(Some(&value), "integer_contract", "count")
                .expect_err("invalid BIGINT value should not project as NULL");
            let error = crate::sql2::error::datafusion_error_to_lix_error(error);
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{raw}");
            assert!(error.message.contains("integer_contract"), "{error:?}");
            assert!(error.message.contains("count"), "{error:?}");
            assert!(error.message.contains("BIGINT"), "{error:?}");
        }
    }

    #[test]
    fn double_projection_accepts_numbers_and_rejects_other_json_kinds() {
        for (raw, expected) in [("1", 1.0), ("1.5", 1.5)] {
            let value =
                serde_json::from_str::<serde_json::Value>(raw).expect("test value should parse");
            assert_eq!(
                super::entity_f64_value(Some(&value), "number_contract", "ratio")
                    .expect("JSON number should project"),
                Some(expected),
                "{raw}"
            );
        }

        for raw in ["\"1\"", "true"] {
            let value =
                serde_json::from_str::<serde_json::Value>(raw).expect("test value should parse");
            let error = super::entity_f64_value(Some(&value), "number_contract", "ratio")
                .expect_err("non-number JSON values should not project as DOUBLE PRECISION");
            let error = crate::sql2::error::datafusion_error_to_lix_error(error);
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{raw}");
            assert!(error.message.contains("number_contract"), "{error:?}");
            assert!(error.message.contains("ratio"), "{error:?}");
            assert!(error.message.contains("DOUBLE PRECISION"), "{error:?}");
        }
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
        let provider = SpecTableProvider::new(Arc::new(super::EntitySpec::by_branch(
            spec,
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
        )));

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

    #[tokio::test]
    async fn file_id_filter_pushes_an_exact_file_scope_into_scan() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "file_note",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "body": { "type": "string" }
                },
                "required": ["id", "body"]
            }))
            .expect("file-scoped schema should derive"),
        );
        let provider = super::EntitySpec::by_branch(
            Arc::clone(&spec),
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
        );

        let filter = eq_filter("lixcol_file_id", "file-a");
        assert_eq!(
            <super::EntitySpec as super::super::spec::TableSpec>::filter_pushdown(
                &provider, &filter
            ),
            datafusion::logical_expr::TableProviderFilterPushDown::Exact,
            "an exact file scope must not be left as a DataFusion residual"
        );
        let (_schema, request, row_filters) = provider
            .plan_scan_parts(None, &[filter], None)
            .await
            .expect("file-scoped scan should plan");
        assert_eq!(
            request.filter.file_ids,
            vec![crate::NullableKeyFilter::Value("file-a".to_string())]
        );
        assert!(
            row_filters.is_empty(),
            "lixcol_file_id is an identity column, not a payload row filter"
        );

        // An IN list is still one contiguous set of file prefixes.
        let in_list = Expr::InList(InList::new(
            Box::new(column("lixcol_file_id")),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("file-a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(Some("file-b".to_string())), None),
            ],
            false,
        ));
        let (_schema, request, _row_filters) = provider
            .plan_scan_parts(None, &[in_list], None)
            .await
            .expect("file-scoped IN scan should plan");
        assert_eq!(
            request.filter.file_ids,
            vec![
                crate::NullableKeyFilter::Value("file-a".to_string()),
                crate::NullableKeyFilter::Value("file-b".to_string()),
            ]
        );

        // Contradictory scopes select nothing rather than every file.
        let (_schema, request, _row_filters) = provider
            .plan_scan_parts(
                None,
                &[
                    eq_filter("lixcol_file_id", "file-a"),
                    eq_filter("lixcol_file_id", "file-b"),
                ],
                None,
            )
            .await
            .expect("contradictory file scopes should plan");
        assert!(request.filter.file_ids.is_empty());
        assert_eq!(request.filter.rows, HotStateRowFilter::None);

        // Shapes the seek cannot represent stay unsupported so DataFusion keeps
        // its residual instead of silently widening the scan.
        assert_eq!(
            <super::EntitySpec as super::super::spec::TableSpec>::filter_pushdown(
                &provider,
                &Expr::IsNull(Box::new(column("lixcol_file_id")))
            ),
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported
        );
        assert_eq!(
            <super::EntitySpec as super::super::spec::TableSpec>::filter_pushdown(
                &provider,
                &Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(column("lixcol_file_id")),
                    Operator::NotEq,
                    Box::new(Expr::Literal(
                        ScalarValue::Utf8(Some("file-a".to_string())),
                        None
                    )),
                ))
            ),
            datafusion::logical_expr::TableProviderFilterPushDown::Unsupported
        );
    }

    #[tokio::test]
    async fn integer_primary_key_filter_pushes_exact_identity_into_scan() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "integer_note",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "integer" },
                    "body": { "type": "string" }
                },
                "required": ["id", "body"]
            }))
            .expect("integer primary-key schema should derive"),
        );
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("id")),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(42)), None)),
        ));
        let expected = crate::entity_pk::EntityPk::from_external_parts(
            vec!["42".to_string()],
            &spec.primary_key_component_types,
        )
        .expect("integer identity should encode");
        let provider = super::EntitySpec::by_branch(
            Arc::clone(&spec),
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
        );

        assert_eq!(
            <super::EntitySpec as super::super::spec::TableSpec>::filter_pushdown(
                &provider, &filter
            ),
            datafusion::logical_expr::TableProviderFilterPushDown::Exact
        );
        let (_schema, request, _row_filters) = provider
            .plan_scan_parts(None, &[filter], None)
            .await
            .expect("integer point scan should plan");
        assert_eq!(request.filter.entity_pks, vec![expected]);
    }

    #[test]
    fn mixed_composite_primary_key_filters_use_typed_components() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "versioned_note",
            "x-lix-primary-key": ["/namespace", "/revision"],
            "type": "object",
            "properties": {
                "namespace": { "type": "string" },
                "revision": { "type": "integer" },
                "body": { "type": "string" }
            },
            "required": ["namespace", "revision", "body"]
        }))
        .expect("mixed primary-key schema should derive");
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("revision")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::UInt32(Some(7)), None)),
            )),
            eq_filter("namespace", "docs"),
        ];

        let actual = super::entity_pks_from_primary_key_filters(&spec, &filters)
            .expect("mixed primary-key filters should analyze")
            .expect("complete mixed primary key should route");
        let expected = crate::entity_pk::EntityPk::from_external_parts(
            vec!["docs".to_string(), "7".to_string()],
            &spec.primary_key_component_types,
        )
        .expect("mixed identity should encode");
        assert_eq!(actual, vec![expected]);
    }

    #[test]
    fn integer_primary_key_rejects_string_literal_pushdown() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "integer_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "integer" } },
            "required": ["id"]
        }))
        .expect("integer primary-key schema should derive");

        assert!(
            super::entity_pks_from_primary_key_filters(&spec, &[eq_filter("id", "42")])
                .expect("mismatched filter should be safely ignored")
                .is_none()
        );
    }

    #[test]
    fn split_composite_primary_key_filters_use_declared_path_order() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "localized_message",
            "x-lix-primary-key": ["/locale", "/key"],
            "type": "object",
            "properties": {
                "key": { "type": "string" },
                "locale": { "type": "string" },
                "body": { "type": "string" }
            },
            "required": ["key", "locale", "body"]
        }))
        .expect("schema should derive");
        // SQL predicate order is deliberately the reverse of the schema's
        // primary-key order.
        let filters = vec![eq_filter("key", "welcome"), eq_filter("locale", "en")];

        let entity_pks = super::entity_pks_from_primary_key_filters(&spec, &filters)
            .expect("composite primary-key filters should analyze")
            .expect("all composite parts should produce an exact identity");

        assert_eq!(
            entity_pks,
            vec![
                crate::entity_pk::EntityPk::tuple(vec!["en".to_string(), "welcome".to_string(),])
                    .expect("test identity should be valid")
            ]
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
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
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
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
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

    #[tokio::test]
    async fn integer_filter_does_not_claim_exact_pushdown_for_real_literal() {
        let spec = filter_pushdown_spec();
        let provider = super::EntitySpec::by_branch(
            Arc::clone(&spec),
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
        );
        let entity_pk_index = provider
            .schema
            .index_of("lixcol_entity_pk")
            .expect("system entity-pk column should exist");
        let projection = vec![entity_pk_index];
        let filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("count")),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Float64(Some(1.0)), None)),
        ));

        let (_schema, request, row_filters) = provider
            .plan_scan_parts(Some(&projection), &[filter], Some(5))
            .await
            .expect("scan should plan");

        assert_eq!(request.limit, Some(5));
        assert!(
            !request
                .projection
                .columns
                .iter()
                .any(|column| column == "snapshot_content"),
            "coercive integer comparisons must remain with DataFusion"
        );
        assert!(row_filters.is_empty());
    }

    #[test]
    fn payload_row_filter_invalid_snapshot_errors() {
        let mut rows = vec![MaterializedHotStateRow {
            snapshot_content: Some("{not-json".into()),
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

    #[test]
    fn payload_integer_filter_rejects_out_of_bigint_snapshot() {
        let mut rows = vec![MaterializedHotStateRow {
            snapshot_content: Some(r#"{"body":"hello","count":9223372036854775808}"#.into()),
            ..live_row()
        }];
        let filters = vec![super::EntityRowFilter::ColumnEq {
            column: "count".to_string(),
            column_type: EntityColumnType::Integer,
            value: super::EntityFilterValue::Integer(1),
        }];

        let error = super::apply_entity_row_filters(&mut rows, &filters)
            .expect_err("out-of-BIGINT values must not be silently filtered out");
        let error = crate::sql2::error::datafusion_error_to_lix_error(error);

        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error.message.contains("count"), "{error:?}");
        assert!(error.message.contains("BIGINT"), "{error:?}");
    }

    #[test]
    fn columnar_pruning_applies_boolean_conjunct_with_residual_filter() {
        let spec = derive_entity_surface_spec_from_schema(&serde_json::json!({
            "x-lix-key": "fixture",
            "type": "object",
            "properties": {
                "active": { "type": "boolean" },
                "lane": { "type": "string" }
            }
        }))
        .expect("schema");
        let snapshots = [
            serde_json::json!({"active": true, "lane": "a"}),
            serde_json::json!({"active": false, "lane": "a"}),
            serde_json::json!({"active": true, "lane": "b"}),
            serde_json::json!({"active": false, "lane": "b"}),
        ];
        let canonical = snapshots
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>();
        let identities = (0..snapshots.len())
            .map(|index| TestEntityPk::single(format!("entity-{index}")))
            .collect::<Vec<_>>();
        let encoded = crate::sql2::encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| crate::sql2::EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        let filters = vec![
            super::EntityRowFilter::ColumnEq {
                column: "active".to_string(),
                column_type: EntityColumnType::Boolean,
                value: super::EntityFilterValue::Boolean(true),
            },
            super::EntityRowFilter::ColumnIn {
                column: "lane".to_string(),
                column_type: EntityColumnType::String,
                values: vec![super::EntityFilterValue::String("a".to_string())],
            },
        ];

        let selected = super::entity_columnar_group_indices(&encoded.manifest, &filters);
        assert!(!selected.is_empty());
        let active_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == "active")
            .expect("active column");
        let lane_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == "lane")
            .expect("lane column");
        assert!(selected.into_iter().all(|group_index| {
            let group = &encoded.manifest.groups[group_index];
            matches!(
                group.columns[active_index].min,
                Some(crate::columnar_row_group::RowGroupScalar::Boolean(true))
            ) && matches!(
                (&group.columns[lane_index].min, &group.columns[lane_index].max),
                (
                    Some(crate::columnar_row_group::RowGroupScalar::String(min)),
                    Some(crate::columnar_row_group::RowGroupScalar::String(max))
                ) if min == "a" && max == "a"
            )
        }));
    }

    #[test]
    fn exact_boolean_pruning_excludes_all_null_groups_above_sidecar_threshold() {
        let spec = derive_entity_surface_spec_from_schema(&serde_json::json!({
            "x-lix-key": "nullable_boolean_fixture",
            "type": "object",
            "properties": {
                "active": { "type": ["boolean", "null"] }
            }
        }))
        .expect("schema");
        let snapshots = (0..1_025)
            .map(|index| match index % 3 {
                0 => serde_json::json!({"active": true}),
                1 => serde_json::json!({"active": false}),
                _ => serde_json::json!({"active": null}),
            })
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>();
        let identities = (0..snapshots.len())
            .map(|index| TestEntityPk::single(format!("entity-{index}")))
            .collect::<Vec<_>>();
        let encoded = crate::sql2::encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| crate::sql2::EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        let filters = vec![super::EntityRowFilter::ColumnEq {
            column: "active".to_string(),
            column_type: EntityColumnType::Boolean,
            value: super::EntityFilterValue::Boolean(true),
        }];

        let selected = super::entity_columnar_group_indices(&encoded.manifest, &filters);
        let active_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == "active")
            .expect("active column");
        assert!(!selected.is_empty());
        assert!(selected.into_iter().all(|group_index| {
            let group = &encoded.manifest.groups[group_index];
            group.columns[active_index].null_count < group.row_count
                && matches!(
                    (
                        &group.columns[active_index].min,
                        &group.columns[active_index].max
                    ),
                    (
                        Some(crate::columnar_row_group::RowGroupScalar::Boolean(true)),
                        Some(crate::columnar_row_group::RowGroupScalar::Boolean(true))
                    )
                )
        }));
    }

    #[test]
    fn boolean_beyond_clustering_budget_retains_datafusion_residual() {
        let mut properties = serde_json::Map::new();
        for index in 0..5 {
            properties.insert(
                format!("flag_{index}"),
                serde_json::json!({ "type": "boolean" }),
            );
        }
        let spec = derive_entity_surface_spec_from_schema(&serde_json::json!({
            "x-lix-key": "wide_boolean_fixture",
            "type": "object",
            "properties": properties
        }))
        .expect("schema");
        let snapshots = (0..1_025)
            .map(|row| {
                let mut snapshot = serde_json::Map::new();
                for index in 0..5 {
                    snapshot.insert(
                        format!("flag_{index}"),
                        serde_json::json!(((row >> index) & 1) == 1),
                    );
                }
                serde_json::Value::Object(snapshot)
            })
            .collect::<Vec<_>>();
        let canonical = snapshots
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>();
        let identities = (0..snapshots.len())
            .map(|index| TestEntityPk::single(format!("entity-{index}")))
            .collect::<Vec<_>>();
        let encoded = crate::sql2::encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| crate::sql2::EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        let row_filter = super::EntityRowFilter::ColumnEq {
            column: "flag_4".to_string(),
            column_type: EntityColumnType::Boolean,
            value: super::EntityFilterValue::Boolean(true),
        };
        let selected = super::entity_columnar_group_indices(
            &encoded.manifest,
            std::slice::from_ref(&row_filter),
        );
        let flag_index = encoded
            .manifest
            .fields
            .iter()
            .position(|field| field.name == "flag_4")
            .expect("flag column");
        assert!(selected.iter().any(|group_index| {
            let statistics = &encoded.manifest.groups[*group_index].columns[flag_index];
            matches!(
                (&statistics.min, &statistics.max),
                (
                    Some(crate::columnar_row_group::RowGroupScalar::Boolean(false)),
                    Some(crate::columnar_row_group::RowGroupScalar::Boolean(true))
                )
            )
        }));

        let provider = super::EntitySpec::by_branch(
            Arc::new(spec),
            Arc::new(EmptyHotStateReader) as Arc<dyn HotStateReader>,
            empty_branch_ref(),
            None,
        );
        let expression = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("flag_4")),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        ));
        assert_eq!(
            <super::EntitySpec as super::super::spec::TableSpec>::filter_pushdown(
                &provider,
                &expression
            ),
            datafusion::logical_expr::TableProviderFilterPushDown::Inexact
        );
    }

    #[test]
    fn columnar_projection_accepts_schema_bound_canonical_json_and_rejects_drift() {
        let spec = derive_entity_surface_spec_from_schema(&serde_json::json!({
            "x-lix-key": "json_payload",
            "type": "object",
            "properties": { "payload": { "type": ["string", "object", "null"] } }
        }))
        .expect("schema");
        let snapshot = serde_json::json!({"payload": {"z": 2, "a": 1}});
        let canonical = snapshot.to_string();
        let identity = TestEntityPk::single("entity-1");
        let encoded = crate::sql2::encode_registered_entity_row_groups(
            &spec,
            std::iter::once(crate::sql2::EntityColumnarRowRef {
                entity_pk: &identity,
                snapshot_bytes: canonical.as_bytes(),
                snapshot_value: &snapshot,
            }),
        )
        .expect("encode")
        .expect("registered layout");
        let full_schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        let payload_schema = Arc::new(Schema::new(vec![
            full_schema
                .field_with_name("payload")
                .expect("payload field")
                .clone(),
        ]));

        assert_eq!(
            super::entity_columnar_projection(&encoded.manifest, &payload_schema, &spec),
            Some(vec![0]),
            "schema-bound canonical JSON text is safe to scan directly"
        );

        let mut drifted = encoded.manifest.clone();
        drifted.metadata.insert(
            crate::sql2::ENTITY_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY.to_string(),
            "different registered schema".to_string(),
        );
        assert!(
            super::entity_columnar_projection(&drifted, &payload_schema, &spec).is_none(),
            "a String/Json-compatible Arrow type must not bypass schema binding"
        );
    }

    #[test]
    fn columnar_overlay_shadows_before_predicate_and_omits_tombstones() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "overlay_fixture",
            "type": "object",
            "properties": {
                "active": { "type": "boolean" },
                "lane": { "type": "string" }
            }
        }))
        .expect("schema");
        let public_schema = Arc::new(Schema::new(vec![
            Field::new("active", DataType::Boolean, true),
            Field::new("lane", DataType::Utf8, true),
        ]));
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::sql2::ENTITY_COLUMNAR_ENTITY_PK_FIELD,
                DataType::Utf8,
                false,
            ),
            Field::new("active", DataType::Boolean, true),
            Field::new("lane", DataType::Utf8, true),
        ]));
        let base = RecordBatch::try_new(
            physical_schema,
            vec![
                Arc::new(StringArray::from(vec![r#"["a"]"#, r#"["b"]"#, r#"["d"]"#])),
                Arc::new(BooleanArray::from(vec![true, true, true])),
                Arc::new(StringArray::from(vec!["old-a", "old-b", "base-d"])),
            ],
        )
        .expect("base batch");
        let shadows = [r#"["a"]"#.to_owned(), r#"["b"]"#.to_owned()]
            .into_iter()
            .collect::<HashSet<_, ahash::RandomState>>();
        let base =
            super::reconcile_entity_columnar_base_batch(base, Arc::clone(&public_schema), &shadows)
                .expect("reconcile base");
        assert_eq!(base.num_rows(), 1);
        assert_eq!(
            base.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("lane")
                .value(0),
            "base-d"
        );

        let overlays = vec![
            crate::hot_state::EntityColumnarOverlayRow {
                entity_pk: TestEntityPk::single("a"),
                snapshot_content: Some(Bytes::from_static(br#"{"active":false,"lane":"new-a"}"#)),
                deleted: false,
                columnar_base_coordinate: None,
            },
            crate::hot_state::EntityColumnarOverlayRow {
                entity_pk: TestEntityPk::single("b"),
                snapshot_content: None,
                deleted: true,
                columnar_base_coordinate: None,
            },
            crate::hot_state::EntityColumnarOverlayRow {
                entity_pk: TestEntityPk::single("c"),
                snapshot_content: Some(Bytes::from_static(br#"{"active":true,"lane":"insert-c"}"#)),
                deleted: false,
                columnar_base_coordinate: None,
            },
        ];
        let filters = [super::EntityRowFilter::ColumnEq {
            column: "active".to_owned(),
            column_type: EntityColumnType::Boolean,
            value: super::EntityFilterValue::Boolean(true),
        }];
        let overlay =
            super::entity_columnar_overlay_batches(&spec, public_schema, &overlays, &filters)
                .expect("typed overlay");
        let [overlay] = overlay.as_slice() else {
            panic!("expected one bounded overlay batch")
        };
        assert_eq!(overlay.num_rows(), 1);
        assert_eq!(
            overlay
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("lane")
                .value(0),
            "insert-c",
            "the updated row moved out of the predicate and its stale base was already shadowed"
        );
    }

    #[test]
    fn each_overlay_batch_maps_to_exactly_one_stream_partition() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = |value| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![value]))],
            )
            .expect("batch")
        };
        let overlays = [batch(10), batch(20)];

        assert!(super::entity_columnar_overlay_partition(&overlays, 3, 2).is_none());
        assert_eq!(
            super::entity_columnar_overlay_partition(&overlays, 3, 3)
                .expect("first overlay")
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            super::entity_columnar_overlay_partition(&overlays, 3, 4)
                .expect("second overlay")
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            20
        );
        assert!(super::entity_columnar_overlay_partition(&overlays, 3, 5).is_none());
    }

    #[test]
    fn columnar_coordinate_masks_touch_only_the_affected_physical_groups() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "coordinate_mask_fixture",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "active": { "type": "boolean" }
            },
            "required": ["id", "active"]
        }))
        .expect("schema");
        let snapshots = [
            json!({"id":"a","active":true}),
            json!({"id":"b","active":true}),
            json!({"id":"c","active":false}),
            json!({"id":"d","active":false}),
        ];
        let canonical = snapshots
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>();
        let identities = ["a", "b", "c", "d"].map(TestEntityPk::single);
        let encoded = crate::sql2::encode_registered_entity_row_groups(
            &spec,
            identities.iter().zip(&snapshots).zip(&canonical).map(
                |((entity_pk, snapshot), canonical)| crate::sql2::EntityColumnarRowRef {
                    entity_pk,
                    snapshot_bytes: canonical.as_bytes(),
                    snapshot_value: snapshot,
                },
            ),
        )
        .expect("encode")
        .expect("registered sidecar");
        let base_commit_id = CommitId::for_test_label("coordinate-base");
        let location = encoded
            .input_locations
            .location(0)
            .expect("encoded entity row has an input coordinate");
        let layout = crate::sql2::entity_batch::EntityColumnarScanLayout {
            id: crate::hot_state::entity_row_group_set_id(base_commit_id, &spec.schema_key),
            manifest: Arc::new(encoded.manifest.clone()),
            manifest_digest: encoded.manifest.content_digest().expect("manifest digest"),
            overlay: Arc::new(vec![
                crate::hot_state::EntityColumnarOverlayRow {
                    entity_pk: identities[0].clone(),
                    snapshot_content: Some(Bytes::from_static(br#"{"id":"a","active":false}"#)),
                    deleted: false,
                    columnar_base_coordinate: Some(crate::hot_state::ColumnarBaseCoordinate {
                        base_commit_id,
                        group_index: location.group_index,
                        row_index: location.row_index,
                    }),
                },
                // A post-base insert has no stale physical row to suppress.
                crate::hot_state::EntityColumnarOverlayRow {
                    entity_pk: TestEntityPk::single("inserted"),
                    snapshot_content: Some(Bytes::from_static(
                        br#"{"id":"inserted","active":true}"#,
                    )),
                    deleted: false,
                    columnar_base_coordinate: None,
                },
            ]),
            branch_id: Arc::from("main"),
            head_commit_id: base_commit_id,
            current_state_revision: 1,
            live_count: identities.len() as u64 + 1,
        };

        let masks = super::entity_columnar_coordinate_shadow_masks(&layout, &spec)
            .expect("coordinate masks")
            .expect("coordinate-capable layout");
        assert_eq!(masks.len(), encoded.manifest.groups.len());
        for (group_index, mask) in masks.iter().enumerate() {
            if group_index == location.group_index as usize {
                let mask = mask.as_ref().expect("affected group mask");
                assert!(!mask.value(location.row_index as usize));
                assert_eq!(mask.iter().filter(|value| *value == Some(false)).count(), 1);
            } else {
                assert!(mask.is_none(), "unaffected group must avoid mask work");
            }
        }
    }

    fn cached_batch_test_layout(
        branch_id: &'static str,
        revision: u64,
    ) -> Arc<crate::sql2::entity_batch::EntityColumnarScanLayout> {
        Arc::new(crate::sql2::entity_batch::EntityColumnarScanLayout {
            id: crate::columnar_row_group::RowGroupSetId::new([23; 16]),
            manifest: Arc::new(crate::columnar_row_group::RowGroupManifest {
                namespace: "cached_batch_test".to_owned(),
                metadata: HashMap::new(),
                fields: Vec::new(),
                groups: Vec::new(),
                encoded_digest: [0; 32],
            }),
            manifest_digest: [24; 32],
            overlay: Arc::new(Vec::new()),
            branch_id: Arc::from(branch_id),
            head_commit_id: CommitId::for_test_label("cached-batch-test-head"),
            current_state_revision: revision,
            live_count: 2,
        })
    }

    fn cached_batch_test_value(value: i64) -> Arc<RecordBatch> {
        Arc::new(
            RecordBatch::try_from_iter([(
                "value",
                Arc::new(Int64Array::from(vec![value, value])) as _,
            )])
            .expect("test batch"),
        )
    }

    #[tokio::test]
    async fn clean_columnar_batch_is_loaded_once() {
        let concrete = Arc::new(TestCachingEntitySnapshotReader::default());
        let reader: Arc<dyn crate::sql2::EntitySnapshotReader> = concrete;
        let loads = Arc::new(AtomicUsize::new(0));
        let digest = [31; 32];

        let layout = cached_batch_test_layout("main", 7);
        for _ in 0..2 {
            let loads = Arc::clone(&loads);
            let batch = cached_batch_test_value(7);
            let result = super::cached_or_load_entity_columnar_batch(
                &reader,
                &layout,
                0,
                digest,
                vec![2, 4],
                async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(batch)
                },
            )
            .await
            .expect("clean batch should load");
            assert_eq!(result.num_rows(), 2);
        }

        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_clean_columnar_load_is_not_cached() {
        let concrete = Arc::new(TestCachingEntitySnapshotReader::default());
        let reader: Arc<dyn crate::sql2::EntitySnapshotReader> = concrete;
        let layout = cached_batch_test_layout("main", 7);
        let loads = Arc::new(AtomicUsize::new(0));
        let first_loads = Arc::clone(&loads);
        let error = super::cached_or_load_entity_columnar_batch(
            &reader,
            &layout,
            0,
            [37; 32],
            vec![2],
            async move {
                first_loads.fetch_add(1, Ordering::SeqCst);
                Err(datafusion::common::DataFusionError::Execution(
                    "expected test failure".to_owned(),
                ))
            },
        )
        .await
        .expect_err("failed load must surface");
        assert!(error.to_string().contains("expected test failure"));

        let retry_loads = Arc::clone(&loads);
        let batch = cached_batch_test_value(9);
        super::cached_or_load_entity_columnar_batch(
            &reader,
            &layout,
            0,
            [37; 32],
            vec![2],
            async move {
                retry_loads.fetch_add(1, Ordering::SeqCst);
                Ok(batch)
            },
        )
        .await
        .expect("retry should populate cache");

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }
}
