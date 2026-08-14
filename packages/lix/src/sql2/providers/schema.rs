use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, not_impl_err};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::prelude::SessionContext;
use futures_util::FutureExt;
use serde_json::Value as JsonValue;

use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchRefReader};
use crate::changelog::CommitRecord;
use crate::commit_graph::CommitGraphReader;
use crate::common::LixTimestamp;
use crate::row_pk::{RowPk, RowPkComponent};
use crate::forktree::{StateCell, StateKeyRef, StateValue, decode_state_key, encode_state_key};
use crate::sql2::branch_scope::{BranchBinding, resolve_provider_branch_ids};
use crate::sql2::catalog::{
    SchemaColumnType, SchemaSurfaceShape, SchemaSurfaceSpec, PublicCatalog, PublicSurfaceKind,
    schema_surface_schema,
};
#[cfg(test)]
use crate::sql2::row_batch::row_snapshot;
use crate::sql2::row_batch::{
    RowExactBatchRequest, RowExactRowRequest, RowProjection, RowRowSelection,
    RowScanFilter, RowScanRequest, RowStateSlot, exact_forktree, exact_transaction,
    scan_slots_forktree, scan_slots_transaction,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::read_only::reject_read_only_schema_surface;
use crate::sql2::value_contract::{json_bigint_value, json_double_value};
use crate::sql2::write_normalization::{SqlCell, UpdateAssignmentValues, UpdateCell};
use crate::state::{ForkTreeStateView, StateRow, StateRowSource};
use crate::{GLOBAL_BRANCH_ID, LixError, NullableKeyFilter, parse_row_metadata_value};

use crate::sql2::{SqlChangelogQuerySource, SqlWriteContext, WriteAccess};
use crate::transaction::types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::ProviderSelection;
use super::schema_history::register_row_history_surface;
use datafusion::physical_plan::ExecutionPlan;

use super::spec::{
    DmlReturning, InsertApply, PlannedDml, PlannedScan, TableSpec, finish_scan_batch,
    projected_schema, register_spec_table, row_source, scan_row_source, take_record_batch_rows,
};
use super::values::{
    optional_bool_value, optional_string_value, required_string_value, string_expr_literal,
};
use crate::storage_adapter::StorageAdapterRead;

pub(crate) async fn register_row_providers<S>(
    ctx: &SessionContext,
    active_branch_id: &str,
    state_view: ForkTreeStateView<S>,
    branch_ref: Arc<dyn BranchRefReader>,
    commit_graph: Option<Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>>,
    query_source: Option<SqlChangelogQuerySource<S>>,
    default_as_of_commit_id: Option<String>,
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
            PublicSurfaceKind::SchemaBase { schema_key } if include_write_surfaces => {
                let spec = catalog_schema_spec(catalog, schema_key)?;
                let provider = if matches!(schema_key.as_str(), "lix_commit" | "lix_commit_edge") {
                    let commit_graph = commit_graph.as_ref().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "commit projection is missing its retained ForkTree graph",
                        )
                    })?;
                    RowSpec::active_commit(
                        spec,
                        Arc::clone(&branch_ref),
                        Arc::clone(commit_graph),
                    )
                } else {
                    RowSpec::active(
                        spec,
                        state_view.clone(),
                        Arc::clone(&branch_ref),
                        active_branch_id.to_string(),
                    )
                };
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(provider),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::SchemaByBranch { schema_key } if include_write_surfaces => {
                let spec = catalog_schema_spec(catalog, schema_key)?;
                let provider = if matches!(schema_key.as_str(), "lix_commit" | "lix_commit_edge") {
                    let commit_graph = commit_graph.as_ref().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "commit projection is missing its retained ForkTree graph",
                        )
                    })?;
                    RowSpec::by_branch_commit(
                        spec,
                        Arc::clone(&branch_ref),
                        Arc::clone(commit_graph),
                    )
                } else {
                    RowSpec::by_branch(spec, state_view.clone(), Arc::clone(&branch_ref))
                };
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(provider),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::SchemaHistory { schema_key } => {
                let (Some(commit_graph), Some(query_source)) =
                    (commit_graph.as_ref(), query_source.as_ref())
                else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "selected row history provider is missing its history context",
                    ));
                };
                let spec = catalog_schema_spec(catalog, schema_key)?;
                register_row_history_surface(
                    ctx,
                    &surface.name,
                    spec,
                    Arc::clone(commit_graph),
                    query_source.clone(),
                    default_as_of_commit_id.clone().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "selected row history provider is missing its pinned commit anchor",
                        )
                    })?,
                )?;
            }
            _ => {}
        }
    }

    Ok(())
}

pub(crate) async fn register_row_write_providers<R>(
    ctx: &SessionContext,
    write_ctx: SqlWriteContext<R>,
    branch_ref: Arc<dyn BranchRefReader>,
    catalog: &PublicCatalog,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    for surface in catalog.surfaces() {
        if !selection.includes(surface) {
            continue;
        }
        match &surface.kind {
            PublicSurfaceKind::SchemaBase { schema_key } => {
                let spec = catalog_schema_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(RowSpec::active_with_write(
                        spec,
                        write_ctx.clone(),
                        Arc::clone(&branch_ref),
                    )),
                    WriteAccess::write(write_ctx.clone()),
                )?;
            }
            PublicSurfaceKind::SchemaByBranch { schema_key } => {
                let spec = catalog_schema_spec(catalog, schema_key)?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(RowSpec::by_branch_with_write(
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

fn catalog_schema_spec(
    catalog: &PublicCatalog,
    schema_key: &str,
) -> Result<Arc<SchemaSurfaceSpec>, LixError> {
    catalog
        .schema_spec(schema_key)
        .cloned()
        .map(Arc::new)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("catalog row surface '{schema_key}' is missing its surface spec"),
            )
        })
}

/// One spec type covers every registered row schema: the runtime
/// [`SchemaSurfaceSpec`] carries the per-schema column layout, and the
/// surface name follows the catalog naming for the base/by-branch shapes.
#[derive(Clone)]
struct RowSpec<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    surface_name: String,
    spec: Arc<SchemaSurfaceSpec>,
    state_view: Option<ForkTreeStateView<R>>,
    write_ctx: Option<SqlWriteContext<R>>,
    branch_ref: Arc<dyn BranchRefReader>,
    schema: SchemaRef,
    branch_binding: BranchBinding,
    commit_graph: Option<super::SharedCommitGraph>,
}

impl<R> RowSpec<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn active(
        spec: Arc<SchemaSurfaceSpec>,
        state_view: ForkTreeStateView<R>,
        branch_ref: Arc<dyn BranchRefReader>,
        active_branch_id: String,
    ) -> Self {
        Self {
            surface_name: spec.schema_key.clone(),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            spec,
            state_view: Some(state_view),
            write_ctx: None,
            branch_ref,
            branch_binding: BranchBinding::active(active_branch_id),
            commit_graph: None,
        }
    }

    fn active_with_write(
        spec: Arc<SchemaSurfaceSpec>,
        write_ctx: SqlWriteContext<R>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        Self {
            surface_name: spec.schema_key.clone(),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            spec,
            state_view: None,
            write_ctx: Some(write_ctx),
            branch_ref,
            branch_binding: BranchBinding::active(active_branch_id),
            commit_graph: None,
        }
    }

    fn by_branch(
        spec: Arc<SchemaSurfaceSpec>,
        state_view: ForkTreeStateView<R>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            spec,
            state_view: Some(state_view),
            write_ctx: None,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
            commit_graph: None,
        }
    }

    fn by_branch_with_write(
        spec: Arc<SchemaSurfaceSpec>,
        write_ctx: SqlWriteContext<R>,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            spec,
            state_view: None,
            write_ctx: Some(write_ctx),
            branch_ref,
            branch_binding: BranchBinding::explicit(),
            commit_graph: None,
        }
    }

    fn active_commit(
        spec: Arc<SchemaSurfaceSpec>,
        branch_ref: Arc<dyn BranchRefReader>,
        commit_graph: super::SharedCommitGraph,
    ) -> Self {
        Self {
            surface_name: spec.schema_key.clone(),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            spec,
            state_view: None,
            write_ctx: None,
            branch_ref,
            branch_binding: BranchBinding::active(crate::GLOBAL_BRANCH_ID.to_string()),
            commit_graph: Some(commit_graph),
        }
    }

    fn by_branch_commit(
        spec: Arc<SchemaSurfaceSpec>,
        branch_ref: Arc<dyn BranchRefReader>,
        commit_graph: super::SharedCommitGraph,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            spec,
            state_view: None,
            write_ctx: None,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
            commit_graph: Some(commit_graph),
        }
    }

    async fn scan_rows(
        &self,
        request: &RowScanRequest,
    ) -> Result<Vec<RowStateSlot>, LixError> {
        if self.spec.schema_key == BRANCH_REF_SCHEMA_KEY {
            return self.scan_branch_ref_slots(request).await;
        }
        if let Some(view) = &self.state_view {
            scan_slots_forktree(view, request).await
        } else if let Some(write_ctx) = &self.write_ctx {
            scan_slots_transaction(write_ctx.state_view(), request).await
        } else {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "row provider has no retained native state view",
            ))
        }
    }

    async fn exact_slots(
        &self,
        request: &RowExactBatchRequest,
    ) -> Result<Vec<Option<RowStateSlot>>, LixError> {
        if self.spec.schema_key == BRANCH_REF_SCHEMA_KEY {
            return self.exact_branch_ref_slots(request).await;
        }
        if let Some(view) = &self.state_view {
            exact_forktree(view, request).await
        } else if let Some(write_ctx) = &self.write_ctx {
            exact_transaction(write_ctx.state_view(), request).await
        } else {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "row provider has no retained native state view",
            ))
        }
    }

    /// Branch refs are selector/control-plane facts, not tracked or untracked
    /// state rows. Project the authenticated selector + RefChange metadata
    /// through the native row surface instead of scanning a state root that
    /// cannot contain these rows. The synthetic StateRow is only an Arrow
    /// boundary carrier; its IDs, timestamps, and head are all sourced from
    /// the same retained BranchRefReader batch.
    async fn scan_branch_ref_slots(
        &self,
        request: &RowScanRequest,
    ) -> Result<Vec<RowStateSlot>, LixError> {
        let rows = if let Some(ids) = branch_ref_ids_from_request(request) {
            self.branch_ref.load_head_metadata_batch(&ids).await?
        } else {
            self.branch_ref.scan_head_metadata().await?
        };
        Ok(rows
            .into_iter()
            .map(|(head, metadata)| {
                branch_ref_state_slot(&head.branch_id, head.commit_id, metadata)
            })
            .collect())
    }

    async fn exact_branch_ref_slots(
        &self,
        request: &RowExactBatchRequest,
    ) -> Result<Vec<Option<RowStateSlot>>, LixError> {
        let ids = request
            .rows
            .iter()
            .map(|row| branch_ref_id_from_row_pk(&row.row_pk))
            .collect::<Vec<_>>();
        let requested_ids = ids.iter().flatten().cloned().collect::<Vec<_>>();
        let candidates = self
            .branch_ref
            .load_head_metadata_batch(&requested_ids)
            .await?
            .into_iter()
            .map(|(head, metadata)| {
                (
                    head.branch_id.clone(),
                    branch_ref_state_slot(&head.branch_id, head.commit_id, metadata),
                )
            })
            .collect::<BTreeMap<_, _>>();
        Ok(ids
            .into_iter()
            .map(|id| id.and_then(|id| candidates.get(&id).cloned()))
            .collect())
    }

    async fn commit_slots(&self) -> Result<Vec<RowStateSlot>, LixError> {
        let commit_graph = self.commit_graph.as_ref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "commit projection has no retained ForkTree graph",
            )
        })?;
        // The commit surfaces expose the authenticated logical topology, not
        // every object that happens to remain physically present while GC is
        // asynchronous. Branch selectors and snapshot selectors are the
        // roots; walking those roots also keeps expired intervals hidden once
        // their selectors are retired, without introducing a second retention
        // or reachability authority.
        let mut root_ids = self
            .branch_ref
            .scan_heads()
            .await?
            .into_iter()
            .map(|head| head.commit_id)
            .collect::<BTreeSet<_>>();
        {
            let mut graph = commit_graph.lock().await;
            root_ids.extend(
                graph
                    .snapshot_roots()
                    .await?
                    .into_iter()
                    .map(|(_, commit_id)| commit_id),
            );
        }
        let (nodes, records) = {
            let mut graph = commit_graph.lock().await;
            let mut nodes = BTreeMap::new();
            for root_id in root_ids {
                for reachable in graph.reachable_nodes(&root_id).await?.iter() {
                    nodes
                        .entry(reachable.commit.commit_id)
                        .or_insert_with(|| reachable.commit.clone());
                }
            }
            let nodes = nodes.into_values().collect::<Vec<_>>();
            let records = graph
                .load_commit_records(&nodes.iter().map(|node| node.commit_id).collect::<Vec<_>>())
                .await?;
            (nodes, records)
        };

        if nodes.len() != records.len() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree commit projection returned an incomplete commit-record batch",
            ));
        }

        let mut commits = Vec::with_capacity(nodes.len());
        for (node, record) in nodes.into_iter().zip(records) {
            let record = record.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "ForkTree commit projection is missing commit '{}'",
                        node.commit_id
                    ),
                )
            })?;
            if record.commit_id != node.commit_id
                || record.generation != node.generation
                || record.parent_commit_ids != node.parent_commit_ids
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "ForkTree commit projection has inconsistent topology for '{}'",
                        node.commit_id
                    ),
                ));
            }
            commits.push(record);
        }
        commits.sort_by_key(|record| record.commit_id);

        let by_branch = matches!(self.branch_binding, BranchBinding::Explicit);
        let branch_ids = if by_branch {
            let mut branch_ids = self
                .branch_ref
                .scan_heads()
                .await?
                .into_iter()
                .map(|head| head.branch_id)
                .collect::<Vec<_>>();
            branch_ids.sort();
            branch_ids.dedup();
            branch_ids
        } else {
            Vec::new()
        };

        let mut slots = Vec::new();
        for record in &commits {
            let commit_snapshot =
                crate::changelog::commit_row_snapshot_json(&record.commit_id.to_string())?;
            if self.spec.schema_key == "lix_commit" {
                let row_pk = RowPk::uuid_from_canonical(&record.commit_id.to_string())
                    .map_err(|error| {
                        LixError::new(LixError::CODE_STORAGE_ERROR, error.to_string())
                    })?;
                let row = commit_projection_row(
                    &self.spec.native_schema,
                    row_pk,
                    commit_snapshot,
                    record,
                )?;
                if by_branch {
                    for branch_id in &branch_ids {
                        slots.push(RowStateSlot::TrackedAt {
                            row: row.clone(),
                            branch_id: branch_id.clone(),
                        });
                    }
                } else {
                    slots.push(RowStateSlot::Tracked(row));
                }
                continue;
            }

            for (parent_order, parent_id) in record.parent_commit_ids.iter().enumerate() {
                let snapshot = serde_json::json!({
                    "parent_id": parent_id.to_string(),
                    "child_id": record.commit_id.to_string(),
                    "parent_order": parent_order as i64,
                });
                let row_pk = RowPk::from_primary_key_plan(
                    &snapshot,
                    &self.spec.primary_key_paths,
                    &self.spec.primary_key_component_types,
                )
                .map_err(|error| LixError::new(LixError::CODE_STORAGE_ERROR, error.to_string()))?;
                let row = commit_projection_row(
                    &self.spec.native_schema,
                    row_pk,
                    serde_json::to_string(&snapshot).map_err(|error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("commit edge snapshot serialization failed: {error}"),
                        )
                    })?,
                    record,
                )?;
                if by_branch {
                    for branch_id in &branch_ids {
                        slots.push(RowStateSlot::TrackedAt {
                            row: row.clone(),
                            branch_id: branch_id.clone(),
                        });
                    }
                } else {
                    slots.push(RowStateSlot::Tracked(row));
                }
            }
        }
        Ok(slots)
    }

    async fn plan_commit_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let source_schema = Arc::clone(&self.schema);
        let output_schema = projected_schema(&source_schema, projection);
        let df_schema = DFSchema::try_from(Arc::clone(&source_schema))?;
        let physical_filters = filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, props))
            .collect::<Result<Vec<_>>>()?;
        let projection = projection.cloned();
        let table_name = self.surface_name.clone();
        let provider = self.clone();
        Ok(PlannedScan {
            schema: Arc::clone(&output_schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&output_schema),
                (provider, source_schema, physical_filters, projection, limit),
                move |(provider, source_schema, physical_filters, projection, limit)| {
                    let table_name = table_name.clone();
                    async move {
                        let slots = provider
                            .commit_slots()
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        let batch = row_record_batch_from_slots(
                            &provider.spec,
                            source_schema,
                            &slots,
                            None,
                        )?;
                        finish_scan_batch(
                            batch,
                            &physical_filters,
                            projection.as_deref(),
                            limit,
                            &table_name,
                        )
                    }
                },
            ),
        })
    }

    /// Plan-time scan derivation shared by `plan_scan` and the unit tests:
    /// the projected output schema, the native scan request (with branch
    /// routing resolved), and the residual snapshot row filters.
    async fn plan_scan_parts(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(SchemaRef, RowScanRequest, Vec<RowRowFilter>)> {
        let projected_schema = projected_schema(&self.schema, projection);
        let row_filters = RowRowFilterAnalyzer::new(&self.spec).analyze_filters(filters)?;
        let mut request = row_state_scan_request(
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
        if self.spec.schema_key == crate::branch::BRANCH_REF_SCHEMA_KEY
            && matches!(self.branch_binding, BranchBinding::Active { .. })
        {
            // Branch-ref rows are authenticated in the global selector scope;
            // an active SQL branch is only the caller's session context and
            // must not be mistaken for the row's storage domain.
            request.filter.branch_ids = vec![GLOBAL_BRANCH_ID.to_string()];
        }
        apply_exact_branch_id_filter(&mut request, exact_branch_ids);
        apply_exact_row_pk_filters(&mut request, &self.spec, filters)?;
        apply_exact_file_id_filters(&mut request, filters)?;
        Ok((projected_schema, request, row_filters))
    }

    fn returning_key_from_batch(
        &self,
        batch: &RecordBatch,
        row_index: usize,
    ) -> Result<RowReturningKey> {
        let row_pk = RowPk::from_json_array_text(&required_string_value(
            batch,
            row_index,
            "lixcol_row_pk",
            "UPDATE row surface RETURNING",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE row surface RETURNING has invalid lixcol_row_pk: {error}"
            ))
        })?;
        let branch_id = match self.branch_binding {
            BranchBinding::Active { .. } => self
                .branch_binding
                .active_branch_id()
                .expect("active branch binding has an ID")
                .to_owned(),
            BranchBinding::Explicit => required_string_value(
                batch,
                row_index,
                "lixcol_branch_id",
                "UPDATE row surface RETURNING",
            )?,
        };
        Ok(RowReturningKey {
            row_pk,
            branch_id,
            file_id: optional_string_value(
                batch,
                row_index,
                "lixcol_file_id",
                "UPDATE row surface RETURNING",
            )?,
        })
    }

    async fn returning_post_image(
        &self,
        write_ctx: &SqlWriteContext<R>,
        keys: &[RowReturningKey],
    ) -> Result<RecordBatch> {
        if keys.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let request = row_state_scan_request(
            &self.spec.schema_key,
            self.branch_binding.active_branch_id(),
            Some(self.schema.as_ref()),
            None,
            false,
        );
        let exact_request = RowExactBatchRequest {
            rows: keys
                .iter()
                .map(|key| RowExactRowRequest {
                    schema_key: self.spec.schema_key.clone(),
                    branch_id: key.branch_id.clone(),
                    row_pk: key.row_pk.clone(),
                    file_id: key.file_id.clone(),
                })
                .collect(),
            projection: request.projection.clone(),
            untracked: None,
            include_tombstones: false,
        };
        let slots = exact_transaction(write_ctx.state_view(), &exact_request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let slots = slots
            .into_iter()
            .map(|slot| {
                slot.ok_or_else(|| {
                    DataFusionError::Execution(
                        "row UPDATE RETURNING post-image is missing an updated row".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = row_record_batch_from_slots(
            &self.spec,
            Arc::clone(&self.schema),
            &slots,
            exact_request.rows.first().map(|row| row.branch_id.as_str()),
        )?;
        let mut post_rows = BTreeMap::new();
        for row_index in 0..batch.num_rows() {
            let key = self.returning_key_from_batch(&batch, row_index)?;
            let index = u32::try_from(row_index).map_err(|_| {
                DataFusionError::Execution("row UPDATE RETURNING row index overflow".into())
            })?;
            if post_rows.insert(key.clone(), index).is_some() {
                return Err(DataFusionError::Execution(format!(
                    "row UPDATE RETURNING post-image contains duplicate row for identity {:?}",
                    key.row_pk
                )));
            }
        }
        let indices = keys
            .iter()
            .map(|key| {
                post_rows.get(key).copied().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "row UPDATE RETURNING post-image is missing updated row {:?}",
                        key.row_pk
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        take_record_batch_rows(&batch, &indices)
    }

    async fn plan_update_with_post_image(
        &self,
        write_ctx: SqlWriteContext<R>,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: Option<DmlReturning>,
    ) -> Result<PlannedDml> {
        reject_read_only_schema_surface(&self.spec.schema_key, "UPDATE")?;
        let (schema, request, row_filters) = self.plan_scan_parts(None, filters, None).await?;
        let exact_request = exact_row_batch_request(&request);
        let provider = self.clone();
        let source = row_source(
            (provider, schema, request, exact_request, row_filters),
            move |(provider, schema, request, exact_request, row_filters)| async move {
                if let Some(exact_request) = exact_request {
                    let exact_branch_id =
                        exact_request.rows.first().map(|row| row.branch_id.clone());
                    let slots = provider
                        .exact_slots(&exact_request)
                        .await
                        .map_err(lix_error_to_datafusion_error)?;
                    let slots = slots.into_iter().flatten().collect::<Vec<_>>();
                    let batch = row_record_batch_from_slots(
                        &provider.spec,
                        schema,
                        &slots,
                        exact_branch_id.as_deref(),
                    )?;
                    return Ok(batch);
                }
                let slots = provider
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let active_branch_id = provider.branch_binding.active_branch_id();
                let filtered = apply_row_state_slot_filters(
                    &provider.spec,
                    slots,
                    &row_filters,
                    active_branch_id,
                )?;
                row_record_batch_from_slots(
                    &provider.spec,
                    schema,
                    &filtered,
                    active_branch_id,
                )
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
                    let rows = row_update_stage_rows_from_batch(
                        &matched_batch,
                        &assignment_values,
                        spec.as_ref(),
                        &branch_binding,
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
struct RowReturningKey {
    row_pk: RowPk,
    branch_id: String,
    file_id: Option<String>,
}

fn commit_projection_row(
    schema: &lix_schema::Schema,
    row_pk: RowPk,
    snapshot: String,
    record: &CommitRecord,
) -> Result<StateRow, LixError> {
    let snapshot = serde_json::from_str::<serde_json::Value>(&snapshot).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("derived '{}' row is malformed: {error}", schema.key),
        )
    })?;
    let key = encode_state_key(StateKeyRef {
        schema_key: &schema.key,
        file_id: None,
        row_pk: &row_pk,
    });
    Ok(StateRow {
        key,
        value: StateValue {
            change_id: record.change_id,
            commit_id: record.commit_id,
            created_at: record.created_at,
            updated_at: record.created_at,
            cell: StateCell::NativeRow(crate::native_row::encode(
                schema,
                &row_pk,
                true,
                None,
                &snapshot,
            )?),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        },
        source: StateRowSource::Global,
    })
}

fn branch_ref_id_from_row_pk(row_pk: &RowPk) -> Option<String> {
    let id = row_pk.as_single_string_owned().ok()?;
    RowPk::uuid_from_canonical(&id).ok().map(|_| id)
}

fn branch_ref_ids_from_request(request: &RowScanRequest) -> Option<Vec<String>> {
    if request.filter.row_pks.is_empty() {
        return None;
    }
    Some(
        request
            .filter
            .row_pks
            .iter()
            .filter_map(branch_ref_id_from_row_pk)
            .collect(),
    )
}

fn branch_ref_state_slot(
    branch_id: &str,
    commit_id: crate::changelog::CommitId,
    metadata: crate::branch::BranchRefMetadata,
) -> RowStateSlot {
    let row_pk = RowPk::uuid_from_canonical(branch_id)
        .expect("authenticated branch ref IDs are canonical UUIDs");
    let key = encode_state_key(StateKeyRef {
        schema_key: BRANCH_REF_SCHEMA_KEY,
        file_id: None,
        row_pk: &row_pk,
    });
    let snapshot = serde_json::json!({
        "id": branch_id,
        "commit_id": commit_id.to_string(),
    });
    let schema = crate::native_row::seed_schema(BRANCH_REF_SCHEMA_KEY)
        .expect("built-in branch-ref native schema");
    RowStateSlot::Tracked(StateRow {
        key,
        value: StateValue {
            change_id: metadata.change_id,
            commit_id,
            // RefChange metadata does not expose an original creation time;
            // the public control row only promises a stable value, so use the
            // canonical zero timestamp while retaining authenticated updates.
            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
            updated_at: metadata.updated_at,
            cell: StateCell::NativeRow(
                crate::native_row::encode(
                    &schema,
                    &row_pk,
                    true,
                    None,
                    &snapshot,
                )
                .expect("derived branch-ref row matches its built-in schema"),
            ),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        },
        source: StateRowSource::Global,
    })
}

#[async_trait]
impl<R> TableSpec<R> for RowSpec<R>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        &self.surface_name
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if self.commit_graph.is_some() {
            return TableProviderFilterPushDown::Exact;
        }
        let primary_key_analyzer = RowPrimaryKeyFilterAnalyzer::new(&self.spec);
        let row_filter_analyzer = RowRowFilterAnalyzer::new(&self.spec);
        if ExactBranchIdFilterAnalyzer.supports(filter)
            || ExactFileIdFilterAnalyzer.supports(filter)
            || primary_key_analyzer.supports(filter)
        {
            TableProviderFilterPushDown::Exact
        } else if row_filter_analyzer.supports(filter) {
            // Retain a DataFusion residual while the native row projection
            // evaluates the predicate. Immutable columnar layouts use this
            // residual for general typed filtering without a query-shape
            // recognizer; later row-group pruning remains an inexact subset.
            TableProviderFilterPushDown::Inexact
        } else {
            TableProviderFilterPushDown::Unsupported
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        if self.commit_graph.is_some() && self.spec.schema_key != BRANCH_REF_SCHEMA_KEY {
            return self
                .plan_commit_scan(projection, filters, limit, props)
                .await;
        }
        let (schema, request, row_filters) =
            self.plan_scan_parts(projection, filters, limit).await?;
        let spec = Arc::clone(&self.spec);
        let provider = self.clone();
        let exact_request = exact_row_batch_request(&request);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (provider, spec, schema, request, exact_request, row_filters),
                move |(provider, spec, schema, request, exact_request, row_filters)| async move {
                    if let Some(exact_request) = exact_request {
                        let exact_branch_id =
                            exact_request.rows.first().map(|row| row.branch_id.clone());
                        let slots = visible_exact_slots(
                            provider
                                .exact_slots(&exact_request)
                                .await
                                .map_err(lix_error_to_datafusion_error)?,
                            request.limit,
                        );
                        let batch = row_record_batch_from_slots(
                            &spec,
                            schema,
                            &slots,
                            exact_branch_id.as_deref(),
                        )?;
                        return Ok(batch);
                    } else {
                        let slots = provider
                            .scan_rows(&request)
                            .await
                            .map_err(lix_error_to_datafusion_error)?;
                        let active_branch_id = provider.branch_binding.active_branch_id();
                        let filtered = apply_row_state_slot_filters(
                            &spec,
                            slots,
                            &row_filters,
                            active_branch_id,
                        )?;
                        row_record_batch_from_slots(
                            &spec,
                            schema,
                            &filtered,
                            active_branch_id,
                        )
                    }
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
        _write_ctx: SqlWriteContext<R>,
        _input: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<InsertApply>> {
        not_impl_err!("raw DataFusion INSERT is disabled; use the sql2 bound write pipeline")
    }

    async fn plan_delete(
        &self,
        write_ctx: SqlWriteContext<R>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        reject_read_only_schema_surface(&self.spec.schema_key, "DELETE")?;
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
        let provider = self.clone();
        let source = row_source(
            (provider, schema, request, row_filters),
            |(provider, schema, request, row_filters)| async move {
                let slots = provider
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let active_branch_id = provider.branch_binding.active_branch_id();
                let filtered = apply_row_state_slot_filters(
                    &provider.spec,
                    slots,
                    &row_filters,
                    active_branch_id,
                )?;
                row_record_batch_from_slots(
                    &provider.spec,
                    schema,
                    &filtered,
                    active_branch_id,
                )
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
                    let rows = row_delete_stage_rows_from_batch(
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
        write_ctx: SqlWriteContext<R>,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
    ) -> Result<PlannedDml> {
        self.plan_update_with_post_image(write_ctx, assignments, filters, None)
            .await
    }

    async fn plan_update_with_returning(
        &self,
        write_ctx: SqlWriteContext<R>,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: &[Expr],
        returning: DmlReturning,
    ) -> Result<PlannedDml> {
        self.plan_update_with_post_image(write_ctx, assignments, filters, Some(returning))
            .await
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

fn row_delete_stage_rows_from_batch(
    batch: &RecordBatch,
    spec: &SchemaSurfaceSpec,
    branch_binding: &BranchBinding,
) -> Result<RawWriteBatch> {
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let global = optional_bool_value(
            batch,
            row_index,
            "lixcol_global",
            "DELETE FROM row surface",
        )?
        .unwrap_or(false);
        let source_branch_id = optional_string_value(
            batch,
            row_index,
            "lixcol_branch_id",
            "DELETE FROM row surface",
        )?;
        if matches!(branch_binding, BranchBinding::Explicit)
            && global
            && source_branch_id.as_deref() != Some(GLOBAL_BRANCH_ID)
        {
            return Err(DataFusionError::Execution(
                "DELETE through an row by-branch surface cannot mutate a projected global row"
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
                        "DELETE FROM row by-branch requires lixcol_branch_id".to_string(),
                    )
                })?
        };
        let row_pk = RowPk::from_json_array_text(&required_string_value(
            batch,
            row_index,
            "lixcol_row_pk",
            "DELETE FROM row surface",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "DELETE FROM row surface has invalid lixcol_row_pk: {error}"
            ))
        })?;
        let metadata = optional_string_value(
            batch,
            row_index,
            "lixcol_metadata",
            "DELETE FROM row surface",
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
            "DELETE FROM row surface",
        )?
        .map(Into::into);
        rows.push_parts(
            Some(row_pk),
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
            false,
            branch_id.into(),
        );
    }
    Ok(rows)
}

fn row_update_stage_rows_from_batch(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    spec: &SchemaSurfaceSpec,
    branch_binding: &BranchBinding,
) -> Result<RawWriteBatch> {
    let mut rows = RawWriteBatch::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let global =
            optional_bool_value(batch, row_index, "lixcol_global", "UPDATE row surface")?
                .unwrap_or(false);
        let source_branch_id = optional_string_value(
            batch,
            row_index,
            "lixcol_branch_id",
            "UPDATE row surface",
        )?;
        if matches!(branch_binding, BranchBinding::Explicit)
            && global
            && source_branch_id.as_deref() != Some(GLOBAL_BRANCH_ID)
        {
            return Err(DataFusionError::Execution(
                "UPDATE through an row by-branch surface cannot mutate a projected global row"
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
                        "UPDATE row by-branch requires lixcol_branch_id".to_string(),
                    )
                })?
        };
        let row_pk = RowPk::from_json_array_text(&required_string_value(
            batch,
            row_index,
            "lixcol_row_pk",
            "UPDATE row surface",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE row surface has invalid lixcol_row_pk: {error}"
            ))
        })?;
        let mut snapshot = parse_snapshot_value(&required_string_value(
            batch,
            row_index,
            "lixcol_snapshot_content",
            "UPDATE row surface",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE row surface has invalid lixcol_snapshot_content: {error}"
            ))
        })?;
        let object = snapshot.as_object_mut().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "UPDATE row surface expected object snapshot for schema '{}'",
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
                row_update_json_value(cell, column.column_type, spec, &column.name)?,
            );
        }
        let metadata = match assignment_values.assigned_cell(row_index, "lixcol_metadata")? {
            UpdateCell::Unassigned => {
                optional_string_value(batch, row_index, "lixcol_metadata", "UPDATE row surface")?
                    .map(|value| row_update_metadata(&value, spec))
                    .transpose()?
            }
            UpdateCell::Assigned(SqlCell::Null) => None,
            UpdateCell::Assigned(SqlCell::Value(value)) => {
                let raw = scalar_utf8(value, "lixcol_metadata", spec)?;
                Some(row_update_metadata(&raw, spec)?)
            }
        };
        let file_id =
            optional_string_value(batch, row_index, "lixcol_file_id", "UPDATE row surface")?
                .map(Into::into);
        rows.push_parts(
            Some(row_pk),
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
            false,
            branch_id.into(),
        );
    }
    Ok(rows)
}

fn row_update_json_value(
    cell: SqlCell,
    column_type: SchemaColumnType,
    spec: &SchemaSurfaceSpec,
    column_name: &str,
) -> Result<JsonValue> {
    let SqlCell::Value(value) = cell else {
        return Ok(JsonValue::Null);
    };
    match column_type {
        SchemaColumnType::String => scalar_utf8(value, column_name, spec).map(JsonValue::String),
        SchemaColumnType::Json => {
            let raw = scalar_utf8(value, column_name, spec)?;
            serde_json::from_str(&raw).map_err(|error| {
                DataFusionError::Execution(format!(
                    "UPDATE {} column '{column_name}' produced invalid JSON: {error}",
                    spec.schema_key
                ))
            })
        }
        SchemaColumnType::Integer => match value {
            ScalarValue::Int64(Some(value)) => Ok(JsonValue::from(value)),
            other => Err(row_update_type_error(
                spec,
                column_name,
                "BIGINT",
                &other,
            )),
        },
        SchemaColumnType::Number => match value {
            ScalarValue::Float64(Some(value)) => serde_json::Number::from_f64(value)
                .map(JsonValue::Number)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "UPDATE {} column '{column_name}' produced non-finite DOUBLE PRECISION",
                        spec.schema_key
                    ))
                }),
            other => Err(row_update_type_error(
                spec,
                column_name,
                "DOUBLE PRECISION",
                &other,
            )),
        },
        SchemaColumnType::Boolean => match value {
            ScalarValue::Boolean(Some(value)) => Ok(JsonValue::Bool(value)),
            other => Err(row_update_type_error(
                spec,
                column_name,
                "BOOLEAN",
                &other,
            )),
        },
        SchemaColumnType::Timestamptz => match value {
            ScalarValue::TimestampMicrosecond(Some(value), _) => {
                chrono::DateTime::from_timestamp_micros(value)
                    .map(|timestamp| {
                        JsonValue::String(
                            timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                        )
                    })
                    .ok_or_else(|| {
                        row_update_type_error(
                            spec,
                            column_name,
                            "TIMESTAMPTZ",
                            &ScalarValue::TimestampMicrosecond(Some(value), Some("UTC".into())),
                        )
                    })
            }
            other => Err(row_update_type_error(
                spec,
                column_name,
                "TIMESTAMPTZ",
                &other,
            )),
        },
    }
}

fn scalar_utf8(value: ScalarValue, column_name: &str, spec: &SchemaSurfaceSpec) -> Result<String> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Ok(value),
        other => Err(row_update_type_error(spec, column_name, "TEXT", &other)),
    }
}

fn row_update_type_error(
    spec: &SchemaSurfaceSpec,
    column_name: &str,
    expected: &str,
    actual: &ScalarValue,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "UPDATE {} column '{column_name}' expected {expected}, got {actual:?}",
        spec.schema_key
    ))
}

fn row_update_metadata(raw: &str, spec: &SchemaSurfaceSpec) -> Result<TransactionJson> {
    let metadata =
        parse_row_metadata_value(raw, &spec.schema_key).map_err(lix_error_to_datafusion_error)?;
    TransactionJson::from_value(metadata, &format!("{} metadata", spec.schema_key))
        .map_err(lix_error_to_datafusion_error)
}

pub(super) fn row_pks_from_primary_key_filters(
    spec: &SchemaSurfaceSpec,
    filters: &[Expr],
) -> Result<Option<Vec<RowPk>>> {
    let analyzer = RowPrimaryKeyFilterAnalyzer::new(spec);
    let mut constraint: Option<RowPkConstraint> = None;
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
            constraint.into_row_pks(
                &analyzer.primary_key_columns,
                &analyzer.primary_key_component_types,
            )
        })
        .map(|ids| ids.into_iter().collect()))
}

fn apply_exact_row_pk_filters(
    request: &mut RowScanRequest,
    spec: &SchemaSurfaceSpec,
    filters: &[Expr],
) -> Result<()> {
    if let Some(row_pks) = row_pks_from_primary_key_filters(spec, filters)? {
        if row_pks.is_empty() {
            request.filter.rows = RowRowSelection::None;
        }
        request.filter.row_pks = row_pks;
    }
    Ok(())
}

fn exact_file_ids_from_filters(filters: &[Expr]) -> Result<Option<Vec<Option<String>>>> {
    let analyzer = ExactFileIdFilterAnalyzer;
    let mut file_ids: Option<BTreeSet<Option<String>>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze_conjunctive(filter)? else {
            continue;
        };
        file_ids = Some(match file_ids {
            Some(existing) => existing.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    Ok(file_ids.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_file_id_filters(request: &mut RowScanRequest, filters: &[Expr]) -> Result<()> {
    if let Some(file_ids) = exact_file_ids_from_filters(filters)? {
        if file_ids.is_empty() {
            request.filter.rows = RowRowSelection::None;
        }
        if let [file_id] = file_ids.as_slice() {
            request.filter.file_ids = vec![
                file_id
                    .clone()
                    .map_or(NullableKeyFilter::Null, NullableKeyFilter::Value),
            ];
        }
    }
    Ok(())
}

fn exact_row_batch_request(request: &RowScanRequest) -> Option<RowExactBatchRequest> {
    if !matches!(request.filter.rows, RowRowSelection::All)
        || !request.filter.constraints.is_empty()
        || request.filter.row_pks.is_empty()
    {
        return None;
    }
    let [schema_key] = request.filter.schema_keys.as_slice() else {
        return None;
    };
    let [branch_id] = request.filter.branch_ids.as_slice() else {
        return None;
    };
    let [file_id] = request.filter.file_ids.as_slice() else {
        return None;
    };
    let file_id = match file_id {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
        NullableKeyFilter::Any => return None,
    };
    let rows = request
        .filter
        .row_pks
        .iter()
        .cloned()
        .map(|row_pk| RowExactRowRequest {
            schema_key: schema_key.clone(),
            branch_id: branch_id.clone(),
            row_pk,
            file_id: file_id.clone(),
        })
        .collect::<Vec<_>>();
    Some(RowExactBatchRequest {
        rows,
        projection: request.projection.clone(),
        untracked: request.filter.untracked,
        include_tombstones: request.filter.include_tombstones,
    })
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

fn apply_exact_branch_id_filter(request: &mut RowScanRequest, branch_ids: Option<Vec<String>>) {
    if let Some(branch_ids) = branch_ids {
        if branch_ids.is_empty() {
            request.filter.rows = RowRowSelection::None;
        }
        request.filter.branch_ids = branch_ids;
    }
}

pub(super) struct RowPrimaryKeyFilterAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
    primary_key_component_types: Vec<crate::row_pk::RowPkComponentType>,
}

struct RowRowFilterAnalyzer<'a> {
    spec: &'a SchemaSurfaceSpec,
}

struct ExactBranchIdFilterAnalyzer;
struct ExactFileIdFilterAnalyzer;

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

impl ExactFileIdFilterAnalyzer {
    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr)
            .is_ok_and(|constraint| constraint.is_some_and(|ids| ids.len() == 1))
    }

    fn analyze_conjunctive(&self, expr: &Expr) -> Result<Option<BTreeSet<Option<String>>>> {
        let Expr::BinaryExpr(binary_expr) = expr else {
            return self.analyze(expr);
        };
        if binary_expr.op != Operator::And {
            return self.analyze(expr);
        }
        let left = self.analyze_conjunctive(&binary_expr.left)?;
        let right = self.analyze_conjunctive(&binary_expr.right)?;
        Ok(match (left, right) {
            (Some(left), Some(right)) => Some(left.intersection(&right).cloned().collect()),
            (Some(ids), None) | (None, Some(ids)) => Some(ids),
            (None, None) => None,
        })
    }

    #[expect(clippy::self_only_used_in_recursion)]
    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<Option<String>>>> {
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
                Ok(file_id_from_binary_filter(binary_expr)
                    .map(|value| BTreeSet::from([Some(value)])))
            }
            Expr::InList(in_list) => Ok(file_ids_from_in_list_filter(in_list)
                .map(|values| values.into_iter().map(Some).collect())),
            Expr::IsNull(inner) if matches!(inner.as_ref(), Expr::Column(column) if column.name == "lixcol_file_id") => {
                Ok(Some(BTreeSet::from([None])))
            }
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
    if column.name != "lixcol_file_id" || in_list.list.is_empty() {
        return None;
    }
    in_list.list.iter().map(string_expr_literal).collect()
}

fn file_id_from_column_literal_filter(column_expr: &Expr, literal_expr: &Expr) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    (column.name == "lixcol_file_id")
        .then(|| string_expr_literal(literal_expr))
        .flatten()
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

impl<'a> RowPrimaryKeyFilterAnalyzer<'a> {
    pub(super) fn new(spec: &'a SchemaSurfaceSpec) -> Self {
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

    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<RowPk>>> {
        if self.primary_key_columns.is_empty() {
            return Ok(None);
        }
        let Some(constraint) = self.analyze_constraint(expr)? else {
            return Ok(None);
        };
        Ok(
            constraint
                .into_row_pks(&self.primary_key_columns, &self.primary_key_component_types),
        )
    }

    /// Extracts identity constraints that are guaranteed conjuncts while
    /// refusing to partially route a disjunction. This lets DataFusion pass
    /// separately planned composite-key terms without turning a payload
    /// predicate into identity semantics.
    fn analyze_conjunctive_constraint(&self, expr: &Expr) -> Result<Option<RowPkConstraint>> {
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

    fn analyze_constraint(&self, expr: &Expr) -> Result<Option<RowPkConstraint>> {
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
                    .into_row_pks(&self.primary_key_columns, &self.primary_key_component_types)
                else {
                    return Ok(None);
                };
                let Some(mut right_ids) = right
                    .into_row_pks(&self.primary_key_columns, &self.primary_key_component_types)
                else {
                    return Ok(None);
                };
                right_ids.extend(left_ids);
                Ok(Some(RowPkConstraint::Full(right_ids)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(row_pk_constraint_from_binary_filter(
                binary_expr,
                &self.primary_key_columns,
                &self.primary_key_component_types,
            )),
            Expr::InList(in_list) => Ok(row_pk_constraint_from_in_list_filter(
                in_list,
                &self.primary_key_columns,
                &self.primary_key_component_types,
            )),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RowPkConstraint {
    Full(BTreeSet<RowPk>),
    Parts(BTreeMap<String, BTreeSet<String>>),
}

impl RowPkConstraint {
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

    fn into_row_pks(
        self,
        primary_key_columns: &[&str],
        component_types: &[crate::row_pk::RowPkComponentType],
    ) -> Option<BTreeSet<RowPk>> {
        match self {
            Self::Full(ids) => Some(ids),
            Self::Parts(parts) => {
                row_pks_from_primary_key_parts(primary_key_columns, component_types, parts)
            }
        }
    }
}

impl<'a> RowRowFilterAnalyzer<'a> {
    fn new(spec: &'a SchemaSurfaceSpec) -> Self {
        Self { spec }
    }

    fn supports(&self, expr: &Expr) -> bool {
        self.analyze(expr).is_some()
    }

    #[expect(clippy::unnecessary_wraps)]
    fn analyze_filters(&self, filters: &[Expr]) -> Result<Vec<RowRowFilter>> {
        Ok(filters
            .iter()
            .filter_map(|filter| self.analyze(filter))
            .collect())
    }

    fn analyze(&self, expr: &Expr) -> Option<RowRowFilter> {
        match expr {
            Expr::Column(column) => {
                let column_name = self.filterable_column_name(&column.name)?;
                let column = self.spec.visible_column(column_name)?;
                (column.column_type == SchemaColumnType::Boolean).then(|| {
                    RowRowFilter::ColumnEq {
                        column: column_name.to_string(),
                        column_type: SchemaColumnType::Boolean,
                        value: RowFilterValue::Boolean(true),
                    }
                })
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::And => {
                let left = self.analyze(&binary_expr.left)?;
                let right = self.analyze(&binary_expr.right)?;
                Some(RowRowFilter::And(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
                let left = self.analyze(&binary_expr.left)?;
                let right = self.analyze(&binary_expr.right)?;
                Some(RowRowFilter::Or(Box::new(left), Box::new(right)))
            }
            Expr::BinaryExpr(binary_expr) => self.analyze_binary(binary_expr),
            Expr::InList(in_list) => self.analyze_in_list(in_list),
            _ => None,
        }
    }

    fn analyze_binary(&self, binary_expr: &BinaryExpr) -> Option<RowRowFilter> {
        if binary_expr.op != Operator::Eq {
            return None;
        }
        self.analyze_column_literal(&binary_expr.left, &binary_expr.right)
            .or_else(|| self.analyze_column_literal(&binary_expr.right, &binary_expr.left))
    }

    fn analyze_in_list(&self, in_list: &InList) -> Option<RowRowFilter> {
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
            .map(|expr| row_filter_value_literal(expr, column_type))
            .collect::<Option<Vec<_>>>()?;
        if values.is_empty() {
            return None;
        }
        Some(RowRowFilter::ColumnIn {
            column: column_name.to_string(),
            column_type,
            values,
        })
    }

    fn analyze_column_literal(
        &self,
        column_expr: &Expr,
        literal_expr: &Expr,
    ) -> Option<RowRowFilter> {
        let Expr::Column(column) = column_expr else {
            return None;
        };
        let column_name = self.filterable_column_name(&column.name)?;
        let column_type = self
            .spec
            .visible_column(column_name)
            .expect("filterable column should exist")
            .column_type;
        Some(RowRowFilter::ColumnEq {
            column: column_name.to_string(),
            column_type,
            value: row_filter_value_literal(literal_expr, column_type)?,
        })
    }

    fn filterable_column_name(&self, column_name: &str) -> Option<&str> {
        let column = self.spec.visible_column(column_name)?;
        match column.column_type {
            SchemaColumnType::String
            | SchemaColumnType::Boolean
            | SchemaColumnType::Integer
            | SchemaColumnType::Number => Some(column.name.as_str()),
            SchemaColumnType::Json | SchemaColumnType::Timestamptz => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum RowFilterValue {
    Boolean(bool),
    Integer(i64),
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
enum RowRowFilter {
    ColumnEq {
        column: String,
        column_type: SchemaColumnType,
        value: RowFilterValue,
    },
    ColumnIn {
        column: String,
        column_type: SchemaColumnType,
        values: Vec<RowFilterValue>,
    },
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl RowRowFilter {
    fn matches_snapshot(&self, snapshot: Option<&JsonValue>, schema_key: &str) -> Result<bool> {
        match self {
            Self::ColumnEq {
                column,
                column_type,
                value,
            } => Ok(
                row_snapshot_value(snapshot, schema_key, column, *column_type)?
                    .is_some_and(|actual| row_filter_values_equal(&actual, value, *column_type)),
            ),
            Self::ColumnIn {
                column,
                column_type,
                values,
            } => Ok(
                row_snapshot_value(snapshot, schema_key, column, *column_type)?.is_some_and(
                    |actual| {
                        values.iter().any(|expected| {
                            row_filter_values_equal(&actual, expected, *column_type)
                        })
                    },
                ),
            ),
            Self::And(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                && right.matches_snapshot(snapshot, schema_key)?),
            Self::Or(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                || right.matches_snapshot(snapshot, schema_key)?),
        }
    }

    fn matches_native(
        &self,
        spec: &SchemaSurfaceSpec,
        row: Option<&DecodedNativeRowSlot>,
    ) -> Result<bool> {
        match self {
            Self::ColumnEq {
                column,
                column_type,
                value,
            } => Ok(native_row_filter_value(spec, row, column, *column_type)?
                .is_some_and(|actual| {
                    row_filter_values_equal(&actual, value, *column_type)
                })),
            Self::ColumnIn {
                column,
                column_type,
                values,
            } => Ok(native_row_filter_value(spec, row, column, *column_type)?
                .is_some_and(|actual| {
                    values.iter().any(|expected| {
                        row_filter_values_equal(&actual, expected, *column_type)
                    })
                })),
            Self::And(left, right) => Ok(left.matches_native(spec, row)?
                && right.matches_native(spec, row)?),
            Self::Or(left, right) => Ok(left.matches_native(spec, row)?
                || right.matches_native(spec, row)?),
        }
    }
}

fn row_filter_value_literal(
    expr: &Expr,
    column_type: SchemaColumnType,
) -> Option<RowFilterValue> {
    let Expr::Literal(literal, _) = expr else {
        return None;
    };
    let value = match literal {
        ScalarValue::Boolean(Some(value)) => Some(RowFilterValue::Boolean(*value)),
        ScalarValue::Int8(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::Int64(Some(value)) => Some(RowFilterValue::Integer(*value)),
        ScalarValue::UInt8(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt16(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt32(Some(value)) => Some(RowFilterValue::Integer(i64::from(*value))),
        ScalarValue::UInt64(Some(value)) => {
            i64::try_from(*value).ok().map(RowFilterValue::Integer)
        }
        ScalarValue::Float32(Some(value)) => Some(RowFilterValue::Number(f64::from(*value))),
        ScalarValue::Float64(Some(value)) => Some(RowFilterValue::Number(*value)),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(RowFilterValue::String(value.clone())),
        _ => None,
    }?;
    match (&value, column_type) {
        (RowFilterValue::Boolean(_), SchemaColumnType::Boolean)
        | (RowFilterValue::Integer(_), SchemaColumnType::Integer)
        | (
            RowFilterValue::Integer(_) | RowFilterValue::Number(_),
            SchemaColumnType::Number,
        )
        | (
            RowFilterValue::String(_),
            SchemaColumnType::String | SchemaColumnType::Timestamptz,
        ) => Some(value),
        _ => None,
    }
}

fn row_snapshot_value(
    snapshot: Option<&JsonValue>,
    schema_key: &str,
    column: &str,
    column_type: SchemaColumnType,
) -> Result<Option<RowFilterValue>> {
    let Some(value) = snapshot.and_then(|snapshot| snapshot.get(column)) else {
        return Ok(None);
    };
    Ok(match column_type {
        SchemaColumnType::String => match value {
            JsonValue::String(value) => Some(RowFilterValue::String(value.clone())),
            _ => None,
        },
        SchemaColumnType::Integer => {
            row_i64_value(Some(value), schema_key, column)?.map(RowFilterValue::Integer)
        }
        SchemaColumnType::Number => {
            row_f64_value(Some(value), schema_key, column)?.map(RowFilterValue::Number)
        }
        SchemaColumnType::Boolean => value.as_bool().map(RowFilterValue::Boolean),
        SchemaColumnType::Json => None,
        SchemaColumnType::Timestamptz => value
            .as_str()
            .map(|value| RowFilterValue::String(value.to_owned())),
    })
}

#[expect(clippy::cast_precision_loss, clippy::float_cmp)]
fn row_filter_values_equal(
    actual: &RowFilterValue,
    expected: &RowFilterValue,
    column_type: SchemaColumnType,
) -> bool {
    match (column_type, actual, expected) {
        (
            SchemaColumnType::Number,
            RowFilterValue::Number(actual),
            RowFilterValue::Integer(expected),
        ) => *actual == *expected as f64,
        (
            SchemaColumnType::Number,
            RowFilterValue::Integer(actual),
            RowFilterValue::Number(expected),
        ) => *actual as f64 == *expected,
        _ => actual == expected,
    }
}

fn top_level_primary_key_columns(spec: &SchemaSurfaceSpec) -> Vec<&str> {
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

fn row_pk_constraint_from_binary_filter(
    binary_expr: &BinaryExpr,
    primary_key_columns: &[&str],
    component_types: &[crate::row_pk::RowPkComponentType],
) -> Option<RowPkConstraint> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    row_pk_constraint_from_column_literal_filter(
        &binary_expr.left,
        &binary_expr.right,
        primary_key_columns,
        component_types,
    )
    .or_else(|| {
        row_pk_constraint_from_column_literal_filter(
            &binary_expr.right,
            &binary_expr.left,
            primary_key_columns,
            component_types,
        )
    })
}

fn row_pk_constraint_from_in_list_filter(
    in_list: &InList,
    primary_key_columns: &[&str],
    component_types: &[crate::row_pk::RowPkComponentType],
) -> Option<RowPkConstraint> {
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
        "lixcol_row_pk" => in_list
            .list
            .iter()
            .map(string_expr_literal)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .map(|value| {
                let parts = RowPk::from_json_array_text(&value).ok()?.into_parts();
                RowPk::from_external_parts(parts, component_types).ok()
            })
            .collect::<Option<BTreeSet<_>>>()
            .map(RowPkConstraint::Full),
        column_name if primary_key_columns.contains(&column_name) => {
            let component_type =
                primary_key_component_type(column_name, primary_key_columns, component_types)?;
            let values = in_list
                .list
                .iter()
                .map(|expr| primary_key_expr_literal(expr, component_type))
                .collect::<Option<BTreeSet<_>>>()?;
            Some(RowPkConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                values,
            )])))
        }
        _ => None,
    }
}

fn row_pk_constraint_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    primary_key_columns: &[&str],
    component_types: &[crate::row_pk::RowPkComponentType],
) -> Option<RowPkConstraint> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    match column.name.as_str() {
        "lixcol_row_pk" => RowPk::from_json_array_text(&string_expr_literal(literal_expr)?)
            .ok()
            .and_then(|identity| {
                RowPk::from_external_parts(identity.into_parts(), component_types).ok()
            })
            .map(|identity| RowPkConstraint::Full(BTreeSet::from([identity]))),
        column_name if primary_key_columns.contains(&column_name) => {
            let component_type =
                primary_key_component_type(column_name, primary_key_columns, component_types)?;
            let value = primary_key_expr_literal(literal_expr, component_type)?;
            Some(RowPkConstraint::Parts(BTreeMap::from([(
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
    component_types: &[crate::row_pk::RowPkComponentType],
) -> Option<crate::row_pk::RowPkComponentType> {
    primary_key_columns
        .iter()
        .position(|candidate| *candidate == column_name)
        .and_then(|index| component_types.get(index))
        .copied()
}

fn primary_key_expr_literal(
    expr: &Expr,
    component_type: crate::row_pk::RowPkComponentType,
) -> Option<String> {
    use crate::row_pk::RowPkComponentType;

    if !matches!(component_type, RowPkComponentType::Integer) {
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

fn row_pks_from_primary_key_parts(
    primary_key_columns: &[&str],
    component_types: &[crate::row_pk::RowPkComponentType],
    parts: BTreeMap<String, BTreeSet<String>>,
) -> Option<BTreeSet<RowPk>> {
    if primary_key_columns
        .iter()
        .any(|column| !parts.contains_key(*column))
    {
        return None;
    }

    let mut identitys = BTreeSet::from([Vec::<String>::new()]);
    for column in primary_key_columns {
        let values = parts.get(*column)?;
        identitys = identitys
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
    identitys
        .into_iter()
        .map(|parts| RowPk::from_external_parts(parts, component_types))
        .collect::<std::result::Result<BTreeSet<_>, _>>()
        .ok()
}

fn identity_matches_parts(
    identity: &RowPk,
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
fn apply_row_state_filters(
    rows: Vec<StateRow>,
    filters: &[RowRowFilter],
) -> Result<Vec<StateRow>> {
    if filters.is_empty() {
        return Ok(rows);
    }
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(snapshot_content) = row_snapshot(&row) else {
            continue;
        };
        let key = decode_state_key(&row.key).map_err(lix_error_to_datafusion_error)?;
        let snapshot = parse_snapshot_value(snapshot_content).map_err(|error| {
            DataFusionError::External(Box::new(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "row scan filter could not parse snapshot_content for schema '{}' row_pk '{:?}': {error}",
                    key.schema_key, key.row_pk
                ),
            )))
        })?;
        let matches = filters.iter().try_fold(true, |matches, filter| {
            if !matches {
                return Ok(false);
            }
            filter.matches_snapshot(Some(&snapshot), &key.schema_key)
        })?;
        if matches {
            filtered.push(row);
        }
    }
    Ok(filtered)
}

fn apply_row_state_slot_filters(
    spec: &SchemaSurfaceSpec,
    slots: Vec<RowStateSlot>,
    filters: &[RowRowFilter],
    branch_id: Option<&str>,
) -> Result<Vec<RowStateSlot>> {
    if filters.is_empty() {
        return Ok(slots);
    }
    let mut filtered = Vec::with_capacity(slots.len());
    for slot in slots {
        let decoded = decode_native_row_slot(spec, &slot, branch_id)?;
        let Some(decoded) = decoded.as_ref() else {
            continue;
        };
        let matches = filters.iter().try_fold(true, |matches, filter| {
            if !matches {
                return Ok(false);
            }
            filter.matches_native(spec, Some(decoded))
        })?;
        if matches {
            filtered.push(slot);
        }
    }
    Ok(filtered)
}

fn row_state_scan_request(
    schema_key: &str,
    active_branch_id: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
    force_snapshot_content: bool,
) -> RowScanRequest {
    RowScanRequest {
        filter: RowScanFilter {
            schema_keys: vec![schema_key.to_string()],
            branch_ids: active_branch_id
                .map(|branch_id| vec![branch_id.to_string()])
                .unwrap_or_default(),
            ..RowScanFilter::default()
        },
        projection: row_state_projection(projected_schema, force_snapshot_content),
        limit,
    }
}

fn row_state_projection(
    projected_schema: Option<&Schema>,
    force_snapshot_content: bool,
) -> RowProjection {
    let Some(schema) = projected_schema else {
        return RowProjection::default();
    };
    let mut columns = projection_column_names(schema);
    // Native row values are typed tuples. Ordinary SQL fields never need a
    // synthesized whole-row snapshot; only the explicit system column asks
    // for that API-boundary materialization.
    if force_snapshot_content && !columns.iter().any(|column| column == "snapshot_content") {
        columns.push("snapshot_content".to_string());
    }
    RowProjection { columns }
}

fn projection_column_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter_map(|field| field.name().strip_prefix("lixcol_"))
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
fn direct_row_batch_eligible(
    schema: &Schema,
    request: &RowScanRequest,
    row_filters: &[RowRowFilter],
) -> bool {
    !schema.fields().is_empty()
        && matches!(request.filter.rows, RowRowSelection::All)
        && !request.filter.include_tombstones
        && request.filter.file_ids.is_empty()
        && row_filters.is_empty()
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
#[cfg(test)]
fn direct_primary_key_projection_eligible(
    spec: &SchemaSurfaceSpec,
    schema: &Schema,
    request: &RowScanRequest,
    row_filters: &[RowRowFilter],
) -> bool {
    direct_row_batch_eligible(schema, request, row_filters)
        && request.filter.row_pks.is_empty()
        && !schema.fields().is_empty()
        && schema
            .fields()
            .iter()
            .all(|field| simple_string_primary_key_index(spec, field.name()).is_some())
}

/// Exact native reads preserve requested slot order and duplicates, but SQL
/// LIMIT counts visible slots only: absent rows and hidden tombstones do not
/// consume the limit.  This is deliberately a terminal projection step after
/// the authenticated point lookup.
fn visible_exact_slots(
    slots: Vec<Option<RowStateSlot>>,
    limit: Option<usize>,
) -> Vec<RowStateSlot> {
    slots
        .into_iter()
        .flatten()
        .take(limit.unwrap_or(usize::MAX))
        .collect()
}

#[cfg(test)]
fn simple_string_primary_key_index(spec: &SchemaSurfaceSpec, column_name: &str) -> Option<usize> {
    spec.primary_key_paths
        .iter()
        .position(|path| matches!(path.as_slice(), [name] if name == column_name))
        .filter(|index| {
            spec.primary_key_component_types.get(*index)
                == Some(&crate::row_pk::RowPkComponentType::String)
                && spec
                    .visible_column(column_name)
                    .is_some_and(|column| column.column_type == SchemaColumnType::String)
        })
}

#[cfg(test)]
fn row_record_batch_from_state_rows(
    spec: &SchemaSurfaceSpec,
    schema: SchemaRef,
    rows: &[StateRow],
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }
    let snapshots = rows
        .iter()
        .map(|row| parse_snapshot(row_snapshot(row)))
        .collect::<Result<Vec<_>>>()?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| row_column_array(spec, field.name(), rows, &snapshots))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn row_record_batch_from_slots(
    spec: &SchemaSurfaceSpec,
    schema: SchemaRef,
    slots: &[RowStateSlot],
    branch_id: Option<&str>,
) -> Result<RecordBatch> {
    // Authentication is independent of projection. COUNT(*) and other
    // zero-column plans must reject the same malformed/substituted row as a
    // visible scalar projection.
    let decoded = slots
        .iter()
        .map(|slot| decode_native_row_slot(spec, slot, branch_id))
        .collect::<Result<Vec<_>>>()?;
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(slots.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            native_row_slot_column_array(
                spec,
                field.name(),
                slots,
                &decoded,
                branch_id,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

struct DecodedNativeRowSlot {
    key: crate::forktree::StateKey,
    body: Vec<lix_schema::value_layout::BodyValue>,
}

fn native_body_column_index(spec: &SchemaSurfaceSpec, column_name: &str) -> Option<usize> {
    spec.native_schema
        .columns
        .iter()
        .filter(|column| !spec.native_schema.primary_key.contains(&column.name))
        .position(|column| column.name == column_name)
}

fn native_pk_column_index(spec: &SchemaSurfaceSpec, column_name: &str) -> Option<usize> {
    spec.native_schema
        .primary_key
        .iter()
        .position(|name| name == column_name)
}

fn native_projection_error(
    spec: &SchemaSurfaceSpec,
    column_name: &str,
    expected: &str,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "native row projection for '{}.{}' expected {expected}",
        spec.schema_key, column_name
    ))
}

fn native_row_filter_value(
    spec: &SchemaSurfaceSpec,
    row: Option<&DecodedNativeRowSlot>,
    column_name: &str,
    column_type: SchemaColumnType,
) -> Result<Option<RowFilterValue>> {
    let Some(row) = row else { return Ok(None) };
    if let Some(index) = native_pk_column_index(spec, column_name) {
        let component = row.key.row_pk.components.get(index).ok_or_else(|| {
            native_projection_error(spec, column_name, "a primary-key component")
        })?;
        return Ok(match (column_type, component) {
            (SchemaColumnType::String, RowPkComponent::Uuid(value)) => Some(
                RowFilterValue::String(uuid::Uuid::from_bytes(*value).to_string()),
            ),
            (SchemaColumnType::String, RowPkComponent::String(value)) => {
                Some(RowFilterValue::String(value.to_string()))
            }
            (SchemaColumnType::Integer, RowPkComponent::Integer(value)) => {
                Some(RowFilterValue::Integer(*value))
            }
            _ => return Err(native_projection_error(spec, column_name, "its declared PK type")),
        });
    }
    let Some(index) = native_body_column_index(spec, column_name) else {
        return Ok(None);
    };
    let value = row.body.get(index).ok_or_else(|| {
        native_projection_error(spec, column_name, "a native body slot")
    })?;
    Ok(match (column_type, value) {
        (_, lix_schema::value_layout::BodyValue::Null) => None,
        (SchemaColumnType::String, lix_schema::value_layout::BodyValue::Text(value)) => {
            Some(RowFilterValue::String(value.clone()))
        }
        (SchemaColumnType::String, lix_schema::value_layout::BodyValue::Uuid(value)) => {
            Some(RowFilterValue::String(value.to_string()))
        }
        (SchemaColumnType::Integer, lix_schema::value_layout::BodyValue::Int8(value)) => {
            Some(RowFilterValue::Integer(*value))
        }
        (SchemaColumnType::Number, lix_schema::value_layout::BodyValue::Float8(value)) => {
            Some(RowFilterValue::Number(*value))
        }
        (SchemaColumnType::Boolean, lix_schema::value_layout::BodyValue::Boolean(value)) => {
            Some(RowFilterValue::Boolean(*value))
        }
        (SchemaColumnType::Timestamptz, lix_schema::value_layout::BodyValue::Timestamptz(value)) => {
            Some(RowFilterValue::String(
                chrono::DateTime::from_timestamp_micros(*value)
                    .ok_or_else(|| native_projection_error(spec, column_name, "valid UTC microseconds"))?
                    .to_rfc3339(),
            ))
        }
        _ => return Err(native_projection_error(spec, column_name, "its declared scalar type")),
    })
}

fn native_row_slot_column_array(
    spec: &SchemaSurfaceSpec,
    column_name: &str,
    slots: &[RowStateSlot],
    decoded: &[Option<DecodedNativeRowSlot>],
    branch_id: Option<&str>,
) -> Result<ArrayRef> {
    if let Some(property_name) = column_name.strip_prefix("lixcol_") {
        return row_slot_system_column_array(spec, property_name, slots, branch_id);
    }
    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 row provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;
    let body_index = native_body_column_index(spec, column_name);
    let pk_index = native_pk_column_index(spec, column_name);
    if body_index.is_none() && pk_index.is_none() {
        return Err(native_projection_error(spec, column_name, "a declared native column"));
    }

    let array: ArrayRef = match column_type {
        SchemaColumnType::String => Arc::new(StringArray::from(
            decoded
                .iter()
                .map(|row| {
                    let Some(row) = row else { return Ok(None) };
                    if let Some(index) = pk_index {
                        return match row.key.row_pk.components.get(index) {
                            Some(RowPkComponent::Uuid(value)) => {
                                Ok(Some(uuid::Uuid::from_bytes(*value).to_string()))
                            }
                            Some(RowPkComponent::String(value)) => Ok(Some(value.to_string())),
                            _ => Err(native_projection_error(spec, column_name, "text or uuid PK")),
                        };
                    }
                    match row.body.get(body_index.expect("body column was established")) {
                        Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                        Some(lix_schema::value_layout::BodyValue::Text(value)) => {
                            Ok(Some(value.clone()))
                        }
                        Some(lix_schema::value_layout::BodyValue::Uuid(value)) => {
                            Ok(Some(value.to_string()))
                        }
                        _ => Err(native_projection_error(spec, column_name, "text or uuid")),
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        SchemaColumnType::Json => Arc::new(StringArray::from(
            decoded
                .iter()
                .map(|row| {
                    let Some(row) = row else { return Ok(None) };
                    match row.body.get(body_index.expect("jsonb cannot be a primary key")) {
                        Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                        Some(lix_schema::value_layout::BodyValue::Jsonb(value)) => {
                            serde_json::to_string(value).map(Some).map_err(|error| {
                                DataFusionError::Execution(format!(
                                    "failed to project native jsonb '{}.{}': {error}",
                                    spec.schema_key, column_name
                                ))
                            })
                        }
                        _ => Err(native_projection_error(spec, column_name, "jsonb")),
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        SchemaColumnType::Integer => Arc::new(Int64Array::from(
            decoded
                .iter()
                .map(|row| {
                    let Some(row) = row else { return Ok(None) };
                    if let Some(index) = pk_index {
                        return match row.key.row_pk.components.get(index) {
                            Some(RowPkComponent::Integer(value)) => Ok(Some(*value)),
                            _ => Err(native_projection_error(spec, column_name, "int8 PK")),
                        };
                    }
                    match row.body.get(body_index.expect("body column was established")) {
                        Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                        Some(lix_schema::value_layout::BodyValue::Int8(value)) => Ok(Some(*value)),
                        _ => Err(native_projection_error(spec, column_name, "int8")),
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        SchemaColumnType::Number => Arc::new(Float64Array::from(
            decoded
                .iter()
                .map(|row| {
                    let Some(row) = row else { return Ok(None) };
                    match row.body.get(body_index.expect("float8 cannot be a primary key")) {
                        Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                        Some(lix_schema::value_layout::BodyValue::Float8(value)) => Ok(Some(*value)),
                        _ => Err(native_projection_error(spec, column_name, "float8")),
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        SchemaColumnType::Boolean => Arc::new(BooleanArray::from(
            decoded
                .iter()
                .map(|row| {
                    let Some(row) = row else { return Ok(None) };
                    match row.body.get(body_index.expect("boolean cannot be a primary key")) {
                        Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                        Some(lix_schema::value_layout::BodyValue::Boolean(value)) => Ok(Some(*value)),
                        _ => Err(native_projection_error(spec, column_name, "boolean")),
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        SchemaColumnType::Timestamptz => Arc::new(
            TimestampMicrosecondArray::from(
                decoded
                    .iter()
                    .map(|row| {
                        let Some(row) = row else { return Ok(None) };
                        match row.body.get(body_index.expect("timestamptz cannot be a primary key")) {
                            Some(lix_schema::value_layout::BodyValue::Null) => Ok(None),
                            Some(lix_schema::value_layout::BodyValue::Timestamptz(value)) => Ok(Some(*value)),
                            _ => Err(native_projection_error(spec, column_name, "timestamptz")),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone("UTC"),
        ),
    };
    Ok(array)
}

fn decode_native_row_slot(
    spec: &SchemaSurfaceSpec,
    slot: &RowStateSlot,
    active_branch_id: Option<&str>,
) -> Result<Option<DecodedNativeRowSlot>> {
    let (row, explicit_branch_id) = match slot {
        RowStateSlot::Tracked(row) => (row, None),
        RowStateSlot::TrackedAt { row, branch_id } => (row, Some(branch_id.as_str())),
    };
    let key = decode_state_key(&row.key).map_err(lix_error_to_datafusion_error)?;
    if key.schema_key != spec.schema_key {
        return Err(lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "native row row schema '{}' does not match selected schema '{}'",
                key.schema_key, spec.schema_key
            ),
        )));
    }
    if key.row_pk.components.len() != spec.primary_key_component_types.len()
        || key
            .row_pk
            .components
            .iter()
            .zip(&spec.primary_key_component_types)
            .any(|(component, expected)| {
                !matches!(
                    (component, expected),
                    (RowPkComponent::Uuid(_), crate::row_pk::RowPkComponentType::Uuid)
                        | (
                            RowPkComponent::Integer(_),
                            crate::row_pk::RowPkComponentType::Integer
                        )
                        | (
                            RowPkComponent::String(_),
                            crate::row_pk::RowPkComponentType::String
                        )
                        | (
                            RowPkComponent::Bytes(_),
                            crate::row_pk::RowPkComponentType::Bytes
                        )
                )
            })
    {
        return Err(lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "native row row for schema '{}' has an invalid primary-key arity or scalar type",
                spec.schema_key
            ),
        )));
    }
    match &row.value.cell {
        StateCell::NativeRow(native) => {
            let global = row.source == StateRowSource::Global;
            if !global {
                explicit_branch_id.or(active_branch_id).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "row provider '{}' has no authenticated branch owner",
                        spec.schema_key
                    ))
                })?;
            }
            let body = crate::native_row::decode(
                spec.native_schema.as_ref(),
                &key.row_pk,
                global,
                key.file_id.as_deref(),
                native,
            )
            .map_err(lix_error_to_datafusion_error)?;
            Ok(Some(DecodedNativeRowSlot { key, body }))
        }
        StateCell::Value(_) => Err(lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "Schema-v1 row '{}' uses the removed JSON current-state representation",
                spec.schema_key
            ),
        ))),
        StateCell::Null | StateCell::Tombstone => Ok(None),
    }
}

fn slot_state_key(slot: &RowStateSlot) -> Result<crate::forktree::StateKey, LixError> {
    match slot {
        RowStateSlot::Tracked(row) | RowStateSlot::TrackedAt { row, .. } => {
            decode_state_key(&row.key)
        }
    }
}

fn row_slot_logical_value(
    spec: &SchemaSurfaceSpec,
    slot: &RowStateSlot,
    active_branch_id: Option<&str>,
) -> Result<Option<JsonValue>> {
    let (row, historical_branch_id) = match slot {
        RowStateSlot::Tracked(row) => (row, None),
        RowStateSlot::TrackedAt { row, branch_id } => (row, Some(branch_id.as_str())),
    };
    let key = decode_state_key(&row.key).map_err(lix_error_to_datafusion_error)?;
    match &row.value.cell {
        StateCell::NativeRow(native) => {
            let global = row.source == StateRowSource::Global;
            if !global {
                historical_branch_id.or(active_branch_id).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "row provider '{}' has no authenticated branch owner",
                        spec.schema_key
                    ))
                })?;
            }
            crate::native_row::logical_value(
                spec.native_schema.as_ref(),
                &key.row_pk,
                global,
                key.file_id.as_deref(),
                native,
            )
            .map(Some)
            .map_err(lix_error_to_datafusion_error)
        }
        StateCell::Value(_) => Err(lix_error_to_datafusion_error(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "Schema-v1 row '{}' uses the removed JSON current-state representation",
                spec.schema_key
            ),
        ))),
        StateCell::Null | StateCell::Tombstone => Ok(None),
    }
}

fn slot_value(
    slot: &RowStateSlot,
) -> Result<
    (
        &crate::forktree::StateCell,
        Option<&str>,
        Option<crate::changelog::ChangeId>,
        Option<crate::changelog::CommitId>,
        bool,
        Option<String>,
    ),
    LixError,
> {
    match slot {
        RowStateSlot::Tracked(row) | RowStateSlot::TrackedAt { row, .. } => Ok((
            &row.value.cell,
            row.value.metadata.as_deref(),
            Some(row.value.change_id),
            Some(row.value.commit_id),
            matches!(row.source, StateRowSource::Global),
            None,
        )),
    }
}

fn row_slot_system_column_array(
    spec: &SchemaSurfaceSpec,
    column_name: &str,
    slots: &[RowStateSlot],
    branch_id: Option<&str>,
) -> Result<ArrayRef> {
    let keys = slots
        .iter()
        .map(slot_state_key)
        .collect::<Result<Vec<_>, _>>()
        .map_err(lix_error_to_datafusion_error)?;
    let branch_ids = slots
        .iter()
        .map(|slot| {
            Ok(match slot {
                RowStateSlot::Tracked(row) => match row.source {
                    StateRowSource::Global => Some(GLOBAL_BRANCH_ID.to_string()),
                    StateRowSource::Branch | StateRowSource::StagedBranch => {
                        branch_id.map(str::to_string)
                    }
                    StateRowSource::StagedGlobal => Some(GLOBAL_BRANCH_ID.to_string()),
                },
                RowStateSlot::TrackedAt { branch_id, .. } => Some(branch_id.clone()),
            })
        })
        .collect::<Result<Vec<Option<String>>, LixError>>()
        .map_err(lix_error_to_datafusion_error)?;
    let array: ArrayRef = match column_name {
        "row_pk" => Arc::new(StringArray::from(
            keys.iter()
                .map(|key| key.row_pk.as_json_array_text().map(Some))
                .collect::<Result<Vec<_>, LixError>>()
                .map_err(lix_error_to_datafusion_error)?
                .into_iter()
                .collect::<Vec<_>>(),
        )),
        "schema_key" => Arc::new(StringArray::from_iter(
            keys.iter().map(|key| Some(key.schema_key.clone())),
        )),
        "file_id" => Arc::new(StringArray::from_iter(
            keys.iter().map(|key| key.file_id.clone()),
        )),
        "snapshot_content" => Arc::new(StringArray::from_iter(
            slots
                .iter()
                .map(|slot| {
                    row_slot_logical_value(spec, slot, branch_id).and_then(|value| {
                        value
                            .map(|value| {
                                serde_json::to_string(&value).map_err(|error| {
                                    DataFusionError::Execution(format!(
                                        "failed to materialize native row snapshot: {error}"
                                    ))
                                })
                            })
                            .transpose()
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        "metadata" => Arc::new(StringArray::from_iter(
            slots
                .iter()
                .map(|slot| {
                    slot_value(slot).map(|value| value.1.map(crate::serialize_row_metadata))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(lix_error_to_datafusion_error)?,
        )),
        "created_at" => Arc::new(StringArray::from_iter(slots.iter().map(
            |slot| match slot {
                RowStateSlot::Tracked(row) | RowStateSlot::TrackedAt { row, .. } => {
                    Some(row.value.created_at.to_string())
                }
            },
        ))),
        "updated_at" => Arc::new(StringArray::from_iter(slots.iter().map(
            |slot| match slot {
                RowStateSlot::Tracked(row) | RowStateSlot::TrackedAt { row, .. } => {
                    Some(row.value.updated_at.to_string())
                }
            },
        ))),
        "global" => Arc::new(BooleanArray::from_iter(
            slots
                .iter()
                .map(|slot| slot_value(slot).map(|value| Some(value.4)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(lix_error_to_datafusion_error)?,
        )),
        "change_id" => Arc::new(StringArray::from_iter(
            slots
                .iter()
                .map(|slot| match slot {
                    // Addressable transaction writes deliberately do not
                    // expose their unpublished change identity through the
                    // staged post-image. The commit identity remains visible
                    // and the change id becomes durable at publication.
                    RowStateSlot::Tracked(row)
                        if matches!(
                            row.source,
                            StateRowSource::StagedGlobal | StateRowSource::StagedBranch
                        ) =>
                    {
                        Ok(None)
                    }
                    RowStateSlot::TrackedAt { row, .. }
                        if matches!(
                            row.source,
                            StateRowSource::StagedGlobal | StateRowSource::StagedBranch
                        ) =>
                    {
                        Ok(None)
                    }
                    _ => slot_value(slot).map(|value| value.2.map(|id| id.to_string())),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(lix_error_to_datafusion_error)?,
        )),
        "commit_id" => Arc::new(StringArray::from_iter(
            slots
                .iter()
                .map(|slot| slot_value(slot).map(|value| value.3.map(|id| id.to_string())))
                .collect::<Result<Vec<_>, _>>()
                .map_err(lix_error_to_datafusion_error)?,
        )),
        "branch_id" => Arc::new(StringArray::from_iter(branch_ids)),
        _ => {
            return Err(DataFusionError::Execution(format!(
                "sql2 row provider does not support system column 'lixcol_{column_name}'"
            )));
        }
    };
    Ok(array)
}

#[cfg(test)]
fn row_primary_key_record_batch(
    spec: &SchemaSurfaceSpec,
    schema: SchemaRef,
    row_pks: Vec<RowPk>,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let component_index = simple_string_primary_key_index(spec, field.name()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "row primary-key projection cannot serve column '{}' for schema '{}'",
                    field.name(), spec.schema_key
                ))
            })?;
            let values = row_pks
                .iter()
                .map(|row_pk| match row_pk.components.as_slice().get(component_index) {
                    Some(crate::row_pk::RowPkComponent::String(value)) => Ok(Some(value.as_ref())),
                    _ => Err(DataFusionError::Execution(format!(
                        "row primary-key projection found an invalid key component for schema '{}' column '{}'",
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

#[cfg(test)]
#[expect(trivial_casts)]
fn row_column_array(
    spec: &SchemaSurfaceSpec,
    column_name: &str,
    rows: &[StateRow],
    snapshots: &[Option<JsonValue>],
) -> Result<ArrayRef> {
    if let Some(property_name) = column_name.strip_prefix("lixcol_") {
        return row_state_system_column_array(property_name, rows);
    }

    let column_type = spec
        .visible_column(column_name)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "sql2 row provider '{}' does not expose column '{}'",
                spec.schema_key, column_name
            ))
        })?
        .column_type;

    let values = snapshots
        .iter()
        .map(|snapshot| snapshot.as_ref().and_then(|value| value.get(column_name)))
        .collect::<Vec<_>>();
    Ok(match column_type {
        SchemaColumnType::String | SchemaColumnType::Json => Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| row_json_text_value(*value, column_type))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Integer => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| row_i64_value(*value, &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Number => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| row_f64_value(*value, &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        SchemaColumnType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
        SchemaColumnType::Timestamptz => Arc::new(
            TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|value| {
                        let Some(value) = value else {
                            return Ok(None);
                        };
                        if value.is_null() {
                            return Ok(None);
                        }
                        let text = value.as_str().ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "{}.{} expected timestamptz text",
                                spec.schema_key, column_name
                            ))
                        })?;
                        chrono::DateTime::parse_from_rfc3339(text)
                            .map(|timestamp| Some(timestamp.timestamp_micros()))
                            .map_err(|error| {
                                DataFusionError::Execution(format!(
                                    "{}.{} contains invalid timestamptz: {error}",
                                    spec.schema_key, column_name
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone("UTC"),
        ) as ArrayRef,
    })
}

/// Materialize `lixcol_*` only at the Arrow boundary from native rows.
#[cfg(test)]
fn row_state_system_column_array(column_name: &str, rows: &[StateRow]) -> Result<ArrayRef> {
    #[expect(trivial_casts)]
    let array = match column_name {
        "row_pk" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    decode_state_key(&row.key)
                        .and_then(|key| key.row_pk.as_json_array_text())
                        .map(Some)
                })
                .collect::<std::result::Result<Vec<_>, LixError>>()
                .map_err(lix_error_to_datafusion_error)?,
        )) as ArrayRef,
        "schema_key" => {
            Arc::new(StringArray::from_iter(rows.iter().map(|row| {
                decode_state_key(&row.key).ok().map(|key| key.schema_key)
            }))) as ArrayRef
        }
        "file_id" => {
            Arc::new(StringArray::from_iter(rows.iter().map(|row| {
                decode_state_key(&row.key).ok().and_then(|key| key.file_id)
            }))) as ArrayRef
        }
        "snapshot_content" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row_snapshot(row)),
        )) as ArrayRef,
        "metadata" => Arc::new(StringArray::from_iter(rows.iter().map(|row| {
            row.value
                .metadata
                .as_deref()
                .map(crate::serialize_row_metadata)
        }))) as ArrayRef,
        "created_at" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| Some(row.value.created_at.to_string())),
        )) as ArrayRef,
        "updated_at" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| Some(row.value.updated_at.to_string())),
        )) as ArrayRef,
        "global" => Arc::new(BooleanArray::from_iter(
            rows.iter()
                .map(|row| Some(matches!(row.source, StateRowSource::Global))),
        )) as ArrayRef,
        "change_id" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.value.change_id.to_string())),
        )) as ArrayRef,
        "commit_id" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.value.commit_id.to_string())),
        )) as ArrayRef,
        "untracked" => {
            Arc::new(BooleanArray::from_iter(rows.iter().map(|_| Some(false)))) as ArrayRef
        }
        "branch_id" => Arc::new(StringArray::from_iter(rows.iter().map(
            |row| match row.source {
                StateRowSource::Global => Some(GLOBAL_BRANCH_ID),
                StateRowSource::Branch | StateRowSource::StagedBranch => None,
                StateRowSource::StagedGlobal => Some(GLOBAL_BRANCH_ID),
            },
        ))) as ArrayRef,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "sql2 row provider does not support system column 'lixcol_{column_name}'"
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
                    "sql2 row provider expected valid snapshot_content JSON: {error}"
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

#[cfg(test)]
thread_local! {
    static ENTITY_SNAPSHOT_PARSE_COUNT: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[cfg(test)]
fn reset_row_snapshot_parse_count() {
    ENTITY_SNAPSHOT_PARSE_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
fn row_snapshot_parse_count() -> usize {
    ENTITY_SNAPSHOT_PARSE_COUNT.with(std::cell::Cell::get)
}

pub(super) fn row_json_text_value(
    value: Option<&JsonValue>,
    column_type: SchemaColumnType,
) -> Result<Option<String>> {
    Ok(match (column_type, value) {
        (_, None | Some(JsonValue::Null)) => None,
        (SchemaColumnType::String, Some(JsonValue::Bool(value))) => Some(if *value {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        (SchemaColumnType::String, Some(JsonValue::String(value))) => Some(value.clone()),
        (SchemaColumnType::String, Some(other)) => Some(json_to_string(other)?),
        (SchemaColumnType::Json, Some(other)) => Some(json_to_string(other)?),
        _ => None,
    })
}

pub(super) fn row_i64_value(
    value: Option<&JsonValue>,
    schema_key: &str,
    column_name: &str,
) -> Result<Option<i64>> {
    json_bigint_value(value, schema_key, column_name).map_err(lix_error_to_datafusion_error)
}

pub(super) fn row_f64_value(
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
mod tests {
    use super::*;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::{LixTimestamp, SharedStr};
    use crate::forktree::{StateCell, StateKeyRef, StateValue, decode_state_key, encode_state_key};
    use crate::sql2::row_batch::row_snapshot;
    use crate::state::{StagedStateRow, StateRow, StateRowSource, TransactionStateView};
    use crate::storage_adapter::{Memory, StorageAdapter};
    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Column, ScalarValue};
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    fn timestamp(value: i64) -> LixTimestamp {
        LixTimestamp::from_unix_millis_utc_lossy(value)
    }

    fn tracked_row(source: StateRowSource, schema_key: &str, row: &str) -> StateRow {
        let row_pk = RowPk::single(row);
        StateRow {
            key: encode_state_key(StateKeyRef {
                schema_key,
                file_id: None,
                row_pk: &row_pk,
            }),
            value: StateValue {
                change_id: ChangeId::for_test_label("row-change"),
                commit_id: CommitId::for_test_label("row-commit"),
                created_at: timestamp(1),
                updated_at: timestamp(2),
                cell: StateCell::Value(SharedStr::from(format!(r#"{{"body":"{row}"}}"#))),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source,
        }
    }

    fn tracked_row_with_file(
        source: StateRowSource,
        schema_key: &str,
        row: &str,
        file_id: Option<&str>,
        cell: StateCell,
    ) -> StateRow {
        let row_pk = RowPk::single(row);
        StateRow {
            key: encode_state_key(StateKeyRef {
                schema_key,
                file_id,
                row_pk: &row_pk,
            }),
            value: StateValue {
                change_id: ChangeId::for_test_label("row-file-change"),
                commit_id: CommitId::for_test_label("row-file-commit"),
                created_at: timestamp(1),
                updated_at: timestamp(2),
                cell,
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source,
        }
    }

    fn row_insert_spec_with_primary_key() -> Arc<SchemaSurfaceSpec> {
        Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "body": { "type": "string" }
                },
                "required": ["id", "body"]
            }))
            .expect("schema should derive row surface spec"),
        )
    }

    fn path_value_schema_spec() -> Arc<SchemaSurfaceSpec> {
        Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "json_pointer",
                "x-lix-primary-key": ["/path"],
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "value": {
                        "type": ["null", "string", "number", "integer", "boolean", "object", "array"]
                    }
                },
                "required": ["path", "value"],
                "additionalProperties": false
            }))
            .expect("path/value schema should derive"),
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

    struct NativeBranchRefReader;

    #[async_trait]
    impl BranchRefReader for NativeBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(Some(BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(branch_id),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new())
        }
    }

    async fn native_state_view() -> ForkTreeStateView<
        crate::storage_adapter::SharedStorageAdapterRead<crate::storage_adapter::MemoryRead>,
    > {
        let storage = StorageAdapter::new(Memory::new());
        crate::forktree::initialize_empty_repository(storage.clone())
            .await
            .expect("initialize canonical in-memory ForkTree");
        let read = storage
            .begin_read(Default::default())
            .await
            .expect("retain one in-memory read");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(read);
        ForkTreeStateView::from_facade(
            crate::forktree::ForkTreeReadFacade::new(read),
            GLOBAL_BRANCH_ID,
        )
        .await
        .expect("open canonical state view")
    }

    async fn native_schema_spec(
        spec: Arc<SchemaSurfaceSpec>,
    ) -> RowSpec<
        crate::storage_adapter::SharedStorageAdapterRead<crate::storage_adapter::MemoryRead>,
    > {
        RowSpec::by_branch(
            spec,
            native_state_view().await,
            Arc::new(NativeBranchRefReader),
        )
    }

    #[cfg(any())]
    #[test]
    fn exact_state_system_projection_preserves_native_identity_and_owner() {
        let spec = row_insert_spec_with_primary_key();
        let tracked = RowStateSlot::Tracked(tracked_row(
            StateRowSource::Branch,
            "app.message",
            "tracked",
        ));
        let untracked = untracked_row("app.message", "untracked");
        let slots = vec![tracked, untracked];

        let row_pk =
            row_slot_system_column_array(spec.as_ref(), "row_pk", &slots, Some("branch-a"))
                .expect("row pk projection");
        let untracked_flag =
            row_slot_system_column_array(spec.as_ref(), "untracked", &slots, Some("branch-a"))
                .expect("untracked projection");
        let branch_ids =
            row_slot_system_column_array(spec.as_ref(), "branch_id", &slots, Some("branch-a"))
                .expect("branch projection");
        assert_eq!(row_pk.len(), 2);
        assert_eq!(untracked_flag.len(), 2);
        assert_eq!(branch_ids.len(), 2);
        assert_eq!(
            branch_ids
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string branch ids")
                .value(0),
            "branch-a"
        );
        assert_eq!(
            untracked_flag
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean untracked flags")
                .value(1),
            true
        );
    }

    #[test]
    fn native_filter_propagates_malformed_snapshot_errors() {
        let row = StateRow {
            key: encode_state_key(StateKeyRef {
                schema_key: "app.message",
                file_id: None,
                row_pk: &RowPk::single("broken"),
            }),
            value: StateValue {
                change_id: ChangeId::for_test_label("malformed-change"),
                commit_id: CommitId::for_test_label("malformed-commit"),
                created_at: timestamp(1),
                updated_at: timestamp(1),
                cell: StateCell::Value(SharedStr::from("{not-json")),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source: StateRowSource::Branch,
        };
        let filter = RowRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: SchemaColumnType::String,
            value: RowFilterValue::String("ok".to_string()),
        };
        assert!(apply_row_state_filters(vec![row], &[filter]).is_err());
    }

    #[test]
    fn exact_request_requires_complete_file_owner_identity() {
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["app.message".to_string()],
                branch_ids: vec!["branch-a".to_string()],
                row_pks: vec![RowPk::single("one")],
                file_ids: vec![NullableKeyFilter::Null],
                ..RowScanFilter::default()
            },
            projection: RowProjection::default(),
            limit: Some(1),
        };
        let exact = exact_row_batch_request(&request).expect("complete owner is exact");
        assert_eq!(exact.rows.len(), 1);
        assert_eq!(exact.rows[0].file_id, None);

        let mut without_file = request;
        without_file.filter.file_ids.clear();
        assert!(exact_row_batch_request(&without_file).is_none());
    }

    #[test]
    fn bounded_scan_rejects_mixed_schema_or_branch_selectors() {
        let mixed_schema = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["a".to_string(), "b".to_string()],
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        assert!(crate::sql2::row_batch::schema_bounds(&mixed_schema).is_err());

        let mixed_branch = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["a".to_string()],
                branch_ids: vec!["one".to_string(), "two".to_string()],
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        // Branch selectors are now routed by the native grouped scan. The
        // schema bound itself remains valid; rejecting this shape here would
        // silently reintroduce the old active-branch-only behavior.
        assert!(crate::sql2::row_batch::schema_bounds(&mixed_branch).is_ok());
    }

    #[tokio::test]
    async fn native_row_fixtures_use_one_retained_memory_view() {
        let storage = StorageAdapter::new(Memory::new());
        crate::forktree::initialize_empty_repository(storage.clone())
            .await
            .expect("initialize canonical in-memory ForkTree");
        let read = storage
            .begin_read(Default::default())
            .await
            .expect("retain one in-memory read");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(read);
        let committed = ForkTreeStateView::from_facade(
            crate::forktree::ForkTreeReadFacade::new(read),
            GLOBAL_BRANCH_ID,
        )
        .await
        .expect("open canonical state view");

        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["app.message".to_string()],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        let committed_rows = scan_slots_forktree(&committed, &request)
            .await
            .expect("scan committed native view");
        assert!(committed_rows.is_empty());

        let staged = tracked_row(StateRowSource::StagedBranch, "app.message", "staged");
        let transaction = TransactionStateView::new(
            committed.clone(),
            vec![StagedStateRow::new(
                staged.key.clone(),
                staged.value.clone(),
                false,
            )],
        )
        .expect("construct ordered native transaction overlay");
        let staged_rows = scan_slots_transaction(&transaction, &request)
            .await
            .expect("scan staged native view");
        assert_eq!(
            staged_rows,
            vec![RowStateSlot::TrackedAt {
                row: staged,
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn native_branch_grouped_scan_routes_valid_alternate_and_dedupes_global() {
        let committed = native_state_view().await;
        let workspace_key = encode_state_key(StateKeyRef {
            schema_key: "lix_key_value",
            file_id: None,
            row_pk: &RowPk::single("lix_workspace_branch_id"),
        });
        let workspace_row = committed
            .points(&[workspace_key], true)
            .await
            .expect("workspace branch point")
            .into_iter()
            .next()
            .flatten()
            .expect("bootstrap workspace branch row");
        let main_branch_id = serde_json::from_str::<serde_json::Value>(
            row_snapshot(&workspace_row).expect("workspace row snapshot"),
        )
        .expect("workspace row JSON")["value"]
            .as_str()
            .expect("workspace branch UUID")
            .to_owned();
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["lix_key_value".to_string()],
                branch_ids: vec![GLOBAL_BRANCH_ID.to_string(), main_branch_id.clone()],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        let slots = scan_slots_forktree(&committed, &request)
            .await
            .expect("grouped native branch range");
        let tracked_branches = slots
            .iter()
            .filter_map(|slot| match slot {
                RowStateSlot::TrackedAt { branch_id, .. } => Some(branch_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(tracked_branches.contains(&GLOBAL_BRANCH_ID));
        assert!(tracked_branches.contains(&main_branch_id.as_str()));
        assert_eq!(
            tracked_branches
                .iter()
                .filter(|branch_id| **branch_id == GLOBAL_BRANCH_ID)
                .count(),
            3,
            "each of the three authenticated bootstrap keys is emitted once for the raw global branch, not once per selected branch"
        );

        let exact = crate::sql2::row_batch::exact_forktree(
            &committed,
            &RowExactBatchRequest {
                rows: vec![
                    RowExactRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        branch_id: GLOBAL_BRANCH_ID.to_string(),
                        row_pk: RowPk::single("lix_workspace_branch_id"),
                        file_id: None,
                    },
                    RowExactRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        branch_id: main_branch_id.clone(),
                        row_pk: RowPk::single("lix_workspace_branch_id"),
                        file_id: None,
                    },
                ],
                untracked: Some(false),
                ..RowExactBatchRequest::default()
            },
        )
        .await
        .expect("grouped native branch points");
        assert_eq!(exact.len(), 2);
        assert!(matches!(
            exact[0],
            Some(RowStateSlot::TrackedAt { ref branch_id, .. })
                if branch_id == GLOBAL_BRANCH_ID
        ));
        assert!(matches!(
            exact[1],
            Some(RowStateSlot::TrackedAt { ref branch_id, .. })
                if branch_id == &main_branch_id
        ));
    }

    #[tokio::test]
    async fn native_pk_only_scan_uses_bounded_row_prefix_for_both_views() {
        let committed = native_state_view().await;
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["app.message".to_string()],
                row_pks: vec![RowPk::single("pk-only")],
                file_ids: Vec::new(),
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        assert!(
            exact_row_batch_request(&request).is_none(),
            "PK-only requests must use the native range route rather than an incomplete exact request"
        );
        assert!(
            scan_slots_forktree(&committed, &request)
                .await
                .expect("bounded ForkTree PK range")
                .is_empty()
        );
        let transaction = TransactionStateView::new(committed, Vec::new())
            .expect("empty native transaction overlay");
        assert!(
            scan_slots_transaction(&transaction, &request)
                .await
                .expect("bounded transaction PK range")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn native_branch_grouped_scan_rejects_missing_branch_instead_of_using_active() {
        let committed = native_state_view().await;
        let foreign_branch = "11111111-1111-4111-8111-111111111111".to_string();
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["app.message".to_string()],
                branch_ids: vec![foreign_branch.clone()],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        assert!(
            scan_slots_forktree(&committed, &request).await.is_err(),
            "a missing explicit branch must fail instead of silently reading the active view"
        );
        let transaction = TransactionStateView::new(committed, Vec::new())
            .expect("empty native transaction overlay");
        assert!(
            scan_slots_transaction(&transaction, &request)
                .await
                .is_err(),
            "transaction scans must honor the same explicit branch identity"
        );
    }

    #[test]
    fn row_schema_contract_preserves_surface_and_identity_columns() {
        let spec = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "body": { "type": "string" },
                "rating": { "type": "number" },
                "meta": { "type": "object" },
                "lixcol_row_pk": { "type": "string" }
            }
        }))
        .expect("schema should derive");
        assert_eq!(
            spec.visible_column_names().collect::<Vec<_>>(),
            vec!["body", "id", "meta", "rating"]
        );
        assert!(spec.visible_column("lixcol_row_pk").is_none());
        let active = schema_surface_schema(&spec, SchemaSurfaceShape::Active);
        let by_branch = schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch);
        assert!(active.field_with_name("lixcol_row_pk").is_ok());
        assert!(active.field_with_name("lixcol_branch_id").is_err());
        assert!(by_branch.field_with_name("lixcol_branch_id").is_ok());
        assert!(
            !active
                .field_with_name("id")
                .expect("id field")
                .is_nullable()
        );
    }

    #[test]
    fn native_projection_preserves_payload_and_system_identity_columns() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "rating": { "type": "number" },
                    "count": { "type": "integer" }
                }
            }))
            .expect("schema should derive"),
        );
        let row = tracked_row(StateRowSource::Branch, "project_message", "row-1");
        let batch = row_record_batch_from_slots(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            &[RowStateSlot::Tracked(row)],
            Some("branch-a"),
        )
        .expect("native Arrow projection");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body string")
                .value(0),
            "row-1"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_row_pk")
                .expect("row pk")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("row pk string")
                .value(0),
            "[\"row-1\"]"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch id")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch id string")
                .value(0),
            "branch-a"
        );
    }

    #[test]
    fn borrowed_path_value_projection_matches_dom_null_duplicates_and_canonical_json() {
        let spec = path_value_schema_spec();
        assert!(spec.certifies_path_value_replacement);
        let slots = vec![
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "json_pointer",
                "/null",
                None,
                StateCell::Value(SharedStr::from(r#"{"path":"/null","value":null}"#)),
            )),
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "json_pointer",
                "/last",
                None,
                StateCell::Value(SharedStr::from(
                    r#"{"path":"bad","value":{"z":0},"path":"/last","value":{"z":2,"a":1}}"#,
                )),
            )),
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "json_pointer",
                "/fallback",
                None,
                StateCell::Value(SharedStr::from(r#"{"value":"text"}"#)),
            )),
        ];
        let schema = schema_surface_schema(&spec, SchemaSurfaceShape::Active);
        let batch = row_record_batch_from_slots(&spec, schema, &slots, Some("branch-a"))
            .expect("borrowed path/value projection");
        let paths = batch
            .column_by_name("path")
            .expect("path")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("path string");
        let values = batch
            .column_by_name("value")
            .expect("value")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value JSON text");
        assert_eq!(paths.value(0), "/null");
        assert!(values.is_null(0), "explicit JSON null maps to SQL NULL");
        assert_eq!(paths.value(1), "/last", "last duplicate field wins");
        assert_eq!(values.value(1), r#"{"a":1,"z":2}"#);
        assert_eq!(
            paths.value(2),
            "/fallback",
            "missing PK falls back to StateKey"
        );
        assert_eq!(values.value(2), r#""text""#);
    }

    #[test]
    fn borrowed_path_value_projection_retains_execution_error_for_malformed_or_trailing_json() {
        let spec = path_value_schema_spec();
        for snapshot in [
            r#"{"path":"/bad","value":}"#,
            r#"{"path":"/bad","value":1} trailing"#,
            r#"["/bad",1]"#,
        ] {
            let slot = RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "json_pointer",
                "/bad",
                None,
                StateCell::Value(SharedStr::from(snapshot)),
            ));
            let error = row_record_batch_from_slots(
                &spec,
                schema_surface_schema(&spec, SchemaSurfaceShape::Active),
                &[slot],
                Some("branch-a"),
            )
            .expect_err("malformed snapshot must fail closed");
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "malformed snapshot error class changed: {error:?}"
            );
        }
    }

    #[test]
    fn native_projection_keeps_zero_column_row_count_and_malformed_json_fail_closed() {
        let spec = row_insert_spec_with_primary_key();
        let row = tracked_row(StateRowSource::Branch, "project_message", "row-1");
        let empty = row_record_batch_from_state_rows(
            &spec,
            Schema::empty().into(),
            std::slice::from_ref(&row),
        )
        .expect("zero-column batch");
        assert_eq!(empty.num_columns(), 0);
        assert_eq!(empty.num_rows(), 1);

        let malformed = StateRow {
            value: StateValue {
                cell: StateCell::Value(SharedStr::from("{not-json")),
                ..row.value
            },
            ..row
        };
        let error = row_record_batch_from_state_rows(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            &[malformed],
        )
        .expect_err("malformed native snapshot must fail");
        assert!(
            error
                .to_string()
                .contains("expected valid snapshot_content JSON")
        );
    }

    #[test]
    fn native_primary_key_and_file_filters_preserve_exact_identity_rules() {
        let spec = row_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("id", "row-a"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("row-b"), string_literal("row-a")],
                false,
            )),
        ];
        let ids = row_pks_from_primary_key_filters(&spec, &filters)
            .expect("primary-key filters analyze")
            .expect("complete identity filter");
        assert_eq!(ids, vec![RowPk::single("row-a")]);
        assert_eq!(
            exact_file_ids_from_filters(&[Expr::IsNull(Box::new(column("lixcol_file_id")))])
                .expect("NULL file filter analyzes"),
            Some(vec![None])
        );
        assert_eq!(
            exact_file_ids_from_filters(&[eq_filter("lixcol_file_id", "file-a")])
                .expect("named file filter analyzes"),
            Some(vec![Some("file-a".to_string())])
        );
        let multi = Expr::InList(InList::new(
            Box::new(column("lixcol_file_id")),
            vec![string_literal("file-a"), string_literal("file-b")],
            false,
        ));
        assert!(!ExactFileIdFilterAnalyzer.supports(&multi));
    }

    #[test]
    fn native_primary_key_analyzer_preserves_typed_and_boolean_semantics() {
        let spec = row_insert_spec_with_primary_key();
        let analyzer = RowPrimaryKeyFilterAnalyzer::new(&spec);
        let disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "row-a")),
            Operator::Or,
            Box::new(eq_filter("id", "row-b")),
        ));
        let contradiction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "row-a")),
            Operator::And,
            Box::new(eq_filter("id", "row-b")),
        ));
        assert_eq!(
            analyzer
                .analyze(&disjunction)
                .expect("OR analyzes")
                .expect("OR identity set")
                .into_iter()
                .collect::<Vec<_>>(),
            vec![RowPk::single("row-a"), RowPk::single("row-b")]
        );
        assert!(
            analyzer
                .analyze(&contradiction)
                .expect("AND analyzes")
                .expect("AND identity set")
                .is_empty()
        );

        let integer = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
            "x-lix-key": "integer_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "integer" } }
        }))
        .expect("integer schema");
        assert!(
            row_pks_from_primary_key_filters(&integer, &[eq_filter("id", "42")])
                .expect("mismatched integer literal is ignored")
                .is_none()
        );
    }

    #[test]
    fn native_scalar_projection_keeps_type_contracts() {
        for (raw, expected) in [("1.0", 1_i64), ("-0.0", 0_i64)] {
            let value = serde_json::from_str::<serde_json::Value>(raw).expect("number");
            assert_eq!(
                row_i64_value(Some(&value), "integer_contract", "count")
                    .expect("integral BIGINT"),
                Some(expected)
            );
        }
        for raw in ["1.5", "9223372036854775808", "\"1\""] {
            let value = serde_json::from_str::<serde_json::Value>(raw).expect("value");
            let error = row_i64_value(Some(&value), "integer_contract", "count")
                .expect_err("invalid BIGINT must fail");
            let error = crate::sql2::error::datafusion_error_to_lix_error(error);
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        }
        for (raw, expected) in [("1", 1.0), ("1.5", 1.5)] {
            let value = serde_json::from_str::<serde_json::Value>(raw).expect("number");
            assert_eq!(
                row_f64_value(Some(&value), "number_contract", "ratio").expect("DOUBLE"),
                Some(expected)
            );
        }
        for raw in ["\"1\"", "true"] {
            let value = serde_json::from_str::<serde_json::Value>(raw).expect("value");
            assert!(row_f64_value(Some(&value), "number_contract", "ratio").is_err());
        }
    }

    #[test]
    fn native_exact_projection_preserves_order_duplicate_and_tombstone_slots() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": { "body": { "type": "string" } }
            }))
            .expect("schema should derive"),
        );
        let slots = vec![
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "project_message",
                "a",
                None,
                StateCell::Value(SharedStr::from(r#"{"body":"first"}"#)),
            )),
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "project_message",
                "b",
                Some("file-b"),
                StateCell::Tombstone,
            )),
            RowStateSlot::Tracked(tracked_row_with_file(
                StateRowSource::Branch,
                "project_message",
                "a",
                None,
                StateCell::Value(SharedStr::from(r#"{"body":"last"}"#)),
            )),
        ];
        let batch = row_record_batch_from_slots(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            &slots,
            Some("branch-a"),
        )
        .expect("native exact projection");
        assert_eq!(batch.num_rows(), 3);
        let body = batch
            .column_by_name("body")
            .expect("body")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body string");
        assert_eq!(body.value(0), "first");
        assert!(body.is_null(1), "tombstone remains a visible null slot");
        assert_eq!(body.value(2), "last");
        let file_ids = batch
            .column_by_name("lixcol_file_id")
            .expect("file ids")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file ids string");
        assert!(file_ids.is_null(0));
        assert_eq!(file_ids.value(1), "file-b");
        assert!(file_ids.is_null(2));
    }

    #[test]
    fn native_residual_filters_run_before_projection() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": { "body": { "type": "string" } }
            }))
            .expect("schema should derive"),
        );
        let rows = vec![
            tracked_row_with_file(
                StateRowSource::Branch,
                "project_message",
                "drop",
                None,
                StateCell::Value(SharedStr::from(r#"{"body":"drop"}"#)),
            ),
            tracked_row_with_file(
                StateRowSource::Global,
                "project_message",
                "keep",
                None,
                StateCell::Value(SharedStr::from(r#"{"body":"keep"}"#)),
            ),
        ];
        let filters = vec![RowRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: SchemaColumnType::String,
            value: RowFilterValue::String("keep".to_string()),
        }];
        let filtered = apply_row_state_filters(rows, &filters).expect("filter rows");
        let batch = row_record_batch_from_state_rows(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            &filtered,
        )
        .expect("project filtered native rows");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body string")
                .value(0),
            "keep"
        );
    }

    #[test]
    fn native_direct_row_batch_accepts_payload_and_rejects_unbounded_shapes() {
        let payload_schema = Schema::new(vec![Field::new("body", DataType::Utf8, true)]);
        let system_schema = Schema::new(vec![Field::new("lixcol_row_pk", DataType::Utf8, true)]);
        let mut request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["project_message".to_string()],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        assert!(direct_row_batch_eligible(&payload_schema, &request, &[]));
        request.filter.row_pks.push(RowPk::single("row"));
        assert!(direct_row_batch_eligible(&payload_schema, &request, &[]));
        request.filter.row_pks.clear();
        request.filter.file_ids.push(NullableKeyFilter::Null);
        assert!(!direct_row_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.file_ids.clear();
        request.filter.rows = RowRowSelection::None;
        assert!(!direct_row_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.rows = RowRowSelection::All;
        assert!(!direct_row_batch_eligible(&system_schema, &request, &[]));
        assert!(!direct_row_batch_eligible(
            &Schema::empty(),
            &request,
            &[]
        ));
        assert!(!direct_row_batch_eligible(
            &payload_schema,
            &request,
            &[RowRowFilter::ColumnEq {
                column: "body".to_string(),
                column_type: SchemaColumnType::String,
                value: RowFilterValue::String("hello".to_string()),
            }],
        ));
    }

    #[test]
    fn native_primary_key_projection_uses_identity_without_snapshot_decode() {
        let spec = row_insert_spec_with_primary_key();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["project_message".to_string()],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            ..RowScanRequest::default()
        };
        assert!(direct_primary_key_projection_eligible(
            &spec,
            schema.as_ref(),
            &request,
            &[]
        ));
        let batch = row_primary_key_record_batch(
            &spec,
            Arc::clone(&schema),
            vec![RowPk::single("identity-1")],
        )
        .expect("identity projection");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("identity string")
                .value(0),
            "identity-1"
        );
        let mut exact_request = request;
        exact_request
            .filter
            .row_pks
            .push(RowPk::single("identity-1"));
        assert!(!direct_primary_key_projection_eligible(
            &spec,
            schema.as_ref(),
            &exact_request,
            &[]
        ));
    }

    #[test]
    fn native_schema_surface_rejects_unprojectable_properties() {
        let error = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
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
        .expect_err("unprojectable property must fail closed");
        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
        assert!(error.message.contains("property '/kind'"));
    }

    #[test]
    fn native_schema_shapes_preserve_identity_and_branch_global_columns() {
        let spec = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string", "x-lix-default": "lix_uuid_v7()" },
                "body": { "type": "string" },
                "rating": { "type": "number" },
                "lixcol_row_pk": { "type": "string" }
            }
        }))
        .expect("schema should derive");
        assert_eq!(
            spec.visible_column_names().collect::<Vec<_>>(),
            vec!["body", "id", "rating"]
        );
        let active = schema_surface_schema(&spec, SchemaSurfaceShape::Active);
        let by_branch = schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch);
        assert!(active.field_with_name("lixcol_row_pk").is_ok());
        assert!(active.field_with_name("lixcol_branch_id").is_err());
        assert!(by_branch.field_with_name("lixcol_branch_id").is_ok());
        assert!(!active.field_with_name("id").expect("id").is_nullable());
        assert!(
            by_branch
                .field_with_name("lixcol_row_pk")
                .expect("pk")
                .is_nullable()
        );
    }

    #[test]
    fn native_record_batch_preserves_payload_and_system_identity() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
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
            .expect("schema should derive"),
        );
        let row = tracked_row_with_file(
            StateRowSource::Global,
            "project_message",
            "row-1",
            Some("file-1"),
            StateCell::Value(SharedStr::from(
                r#"{"body":"hello","rating":4.5,"count":7,"enabled":true,"meta":{"z":2,"a":1}}"#,
            )),
        );
        let batch = row_record_batch_from_slots(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::ByBranch),
            &[RowStateSlot::Tracked(row)],
            Some("branch-a"),
        )
        .expect("native record batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body string")
                .value(0),
            "hello"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_branch_id")
                .expect("branch")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("branch string")
                .value(0),
            GLOBAL_BRANCH_ID
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_file_id")
                .expect("file")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("file string")
                .value(0),
            "file-1"
        );
    }

    #[test]
    fn native_exact_primary_key_projection_keeps_json_and_scalar_contracts() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/body"],
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "rating": { "type": "number" },
                    "count": { "type": "integer" },
                    "meta": { "type": "object" }
                }
            }))
            .expect("schema should derive"),
        );
        let row = tracked_row_with_file(
            StateRowSource::Branch,
            "project_message",
            "hello",
            None,
            StateCell::Value(SharedStr::from(
                r#"{"body":"hello","rating":4.5,"count":7,"meta":{"z":2,"a":1}}"#,
            )),
        );
        let batch = row_record_batch_from_state_rows(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            &[row],
        )
        .expect("parsed native exact batch");
        assert_eq!(
            batch
                .column_by_name("meta")
                .expect("meta")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("meta string")
                .value(0),
            r#"{"a":1,"z":2}"#
        );
        assert_eq!(
            batch
                .column_by_name("count")
                .expect("count")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count i64")
                .value(0),
            7
        );
    }

    #[cfg(any())]
    #[test]
    fn native_untracked_projection_preserves_duplicate_json_last_value() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "body": { "type": "string" },
                    "count": { "type": "integer" }
                },
                "required": ["id", "body", "count"]
            }))
            .expect("schema should derive row surface spec"),
        );
        let tracked = tracked_row(StateRowSource::Branch, "project_message", "row-1");
        let key = decode_state_key(&tracked.key).expect("fixture key");
        let slot = RowStateSlot::Untracked(UntrackedStateRow {
            owner: CanonicalBranchId::from_bytes([7; 16]),
            key,
            value: UntrackedValue {
                created_at: timestamp(3),
                updated_at: timestamp(4),
                cell: StateCell::Value(SharedStr::from(
                    r#"{"id":"row-1","body":"ok","count":"bad","count":7}"#,
                )),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
        });
        let batch = row_record_batch_from_slots(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            &[slot],
            Some("branch-a"),
        )
        .expect("untracked native projection");
        assert_eq!(
            batch
                .column_by_name("count")
                .expect("count")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count i64")
                .value(0),
            7
        );
    }

    #[test]
    fn native_malformed_raw_projection_fails_closed() {
        let spec = row_insert_spec_with_primary_key();
        let row = tracked_row_with_file(
            StateRowSource::Branch,
            "project_message",
            "broken",
            None,
            StateCell::Value(SharedStr::from("{not-json")),
        );
        let error = row_record_batch_from_state_rows(
            &spec,
            schema_surface_schema(&spec, SchemaSurfaceShape::Active),
            &[row],
        )
        .expect_err("malformed native payload must fail");
        assert!(
            error
                .to_string()
                .contains("expected valid snapshot_content JSON")
        );
    }

    #[tokio::test]
    async fn native_provider_schema_is_backed_by_a_forktree_view() {
        let spec = row_insert_spec_with_primary_key();
        let provider = native_schema_spec(spec).await;
        assert!(
            provider
                .schema()
                .field_with_name("lixcol_row_pk")
                .is_ok()
        );
        let (_schema, request, filters) = provider
            .plan_scan_parts(None, &[], Some(5))
            .await
            .expect("native provider scan planning");
        assert_eq!(request.limit, Some(5));
        assert!(filters.is_empty());
    }

    #[test]
    fn native_exact_request_requires_and_preserves_correlated_file_owner() {
        let request = RowScanRequest {
            filter: RowScanFilter {
                schema_keys: vec!["row".to_string()],
                branch_ids: vec!["branch-a".to_string()],
                row_pks: vec![RowPk::single("b"), RowPk::single("a")],
                file_ids: vec![NullableKeyFilter::Null],
                untracked: Some(false),
                ..RowScanFilter::default()
            },
            limit: Some(1),
            ..RowScanRequest::default()
        };
        let exact = exact_row_batch_request(&request).expect("correlated exact request");
        assert_eq!(exact.rows[0].row_pk, RowPk::single("b"));
        assert_eq!(exact.rows[1].row_pk, RowPk::single("a"));
        assert!(exact.rows.iter().all(|row| row.file_id.is_none()));
        let mut without_file = request;
        without_file.filter.file_ids.clear();
        assert!(exact_row_batch_request(&without_file).is_none());
    }

    #[test]
    fn native_visible_exact_limit_skips_missing_and_keeps_duplicates() {
        let visible = tracked_row(StateRowSource::Branch, "project_message", "visible");
        let slots = vec![
            None,
            None,
            Some(RowStateSlot::Tracked(visible.clone())),
            Some(RowStateSlot::Tracked(visible)),
        ];
        let limited = visible_exact_slots(slots, Some(1));
        assert_eq!(limited.len(), 1);
        assert_eq!(
            decode_state_key(
                &crate::sql2::row_batch::tracked_slot(&limited[0])
                    .expect("visible slot is tracked")
                    .key,
            )
            .unwrap()
            .row_pk,
            RowPk::single("visible")
        );
        let duplicates = visible_exact_slots(
            vec![
                Some(RowStateSlot::Tracked(tracked_row(
                    StateRowSource::Branch,
                    "project_message",
                    "duplicate",
                ))),
                Some(RowStateSlot::Tracked(tracked_row(
                    StateRowSource::Branch,
                    "project_message",
                    "duplicate",
                ))),
            ],
            None,
        );
        assert_eq!(duplicates.len(), 2);
    }

    #[test]
    fn native_file_filter_distinguishes_null_named_and_multi_owner() {
        assert_eq!(
            exact_file_ids_from_filters(&[Expr::IsNull(Box::new(column("lixcol_file_id")))])
                .expect("NULL owner filter"),
            Some(vec![None])
        );
        assert_eq!(
            exact_file_ids_from_filters(&[eq_filter("lixcol_file_id", "file-a")])
                .expect("named owner filter"),
            Some(vec![Some("file-a".to_string())])
        );
        let multi = Expr::InList(InList::new(
            Box::new(column("lixcol_file_id")),
            vec![string_literal("file-a"), string_literal("file-b")],
            false,
        ));
        assert!(!ExactFileIdFilterAnalyzer.supports(&multi));
        let mut request = RowScanRequest::default();
        apply_exact_file_id_filters(&mut request, &[multi]).expect("residual owner filter");
        assert!(request.filter.file_ids.is_empty());
    }

    #[test]
    fn native_primary_key_filters_route_typed_identity_and_boolean_semantics() {
        let spec = row_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("id", "row-a"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("row-b"), string_literal("row-a")],
                false,
            )),
        ];
        assert_eq!(
            row_pks_from_primary_key_filters(&spec, &filters)
                .expect("identity filters")
                .expect("complete identity"),
            vec![RowPk::single("row-a")]
        );
        let analyzer = RowPrimaryKeyFilterAnalyzer::new(&spec);
        let disjunction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "row-a")),
            Operator::Or,
            Box::new(eq_filter("id", "row-b")),
        ));
        let contradiction = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(eq_filter("id", "row-a")),
            Operator::And,
            Box::new(eq_filter("id", "row-b")),
        ));
        assert_eq!(
            analyzer
                .analyze(&disjunction)
                .unwrap()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![RowPk::single("row-a"), RowPk::single("row-b")]
        );
        assert!(
            analyzer
                .analyze(&contradiction)
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn native_composite_primary_key_filters_use_declared_order_and_types() {
        let spec = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
            "x-lix-key": "localized_message",
            "x-lix-primary-key": ["/locale", "/revision"],
            "type": "object",
            "properties": {
                "locale": { "type": "string" },
                "revision": { "type": "integer" },
                "body": { "type": "string" }
            },
            "required": ["locale", "revision", "body"]
        }))
        .expect("composite schema");
        let filters = vec![
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(column("revision")),
                Operator::Eq,
                Box::new(Expr::Literal(ScalarValue::UInt32(Some(7)), None)),
            )),
            eq_filter("locale", "en"),
        ];
        let actual = row_pks_from_primary_key_filters(&spec, &filters)
            .expect("composite filters")
            .expect("complete composite identity");
        let expected = RowPk::from_external_parts(
            vec!["en".to_string(), "7".to_string()],
            &spec.primary_key_component_types,
        )
        .expect("composite identity");
        assert_eq!(actual, vec![expected]);

        assert!(
            row_pks_from_primary_key_filters(&spec, &[eq_filter("revision", "7")])
                .expect("incomplete composite filter")
                .is_none()
        );
    }

    #[test]
    fn native_primary_key_filters_ignore_non_key_and_negated_predicates() {
        let spec = row_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("body", "hello"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("row-a")],
                true,
            )),
        ];
        assert!(
            row_pks_from_primary_key_filters(&spec, &filters)
                .expect("ignored filters")
                .unwrap_or_default()
                .is_empty()
        );
        let integer = crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
            "x-lix-key": "integer_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "integer" } }
        }))
        .expect("integer schema");
        assert!(
            row_pks_from_primary_key_filters(&integer, &[eq_filter("id", "42")])
                .expect("mismatched integer literal")
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_payload_filter_planning_preserves_snapshot_and_limit_rules() {
        let spec = Arc::new(
            crate::sql2::catalog::derive_schema_surface_spec_from_schema(&json!({
                "x-lix-key": "pushdown_note",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string" },
                    "score": { "type": "number" },
                    "count": { "type": "integer" }
                },
                "required": ["id", "kind", "score", "count"]
            }))
            .expect("filter schema"),
        );
        let provider = native_schema_spec(spec).await;
        let row_pk_index = provider
            .schema
            .index_of("lixcol_row_pk")
            .expect("row pk column");
        let projection = vec![row_pk_index];
        let (_schema, request, filters) = provider
            .plan_scan_parts(Some(&projection), &[eq_filter("kind", "todo")], Some(5))
            .await
            .expect("payload filter planning");
        assert_eq!(request.limit, None);
        assert!(
            request
                .projection
                .columns
                .iter()
                .any(|column| column == "snapshot_content")
        );
        assert_eq!(filters.len(), 1);

        let range_filter = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(column("score")),
            Operator::Gt,
            Box::new(Expr::Literal(ScalarValue::Float64(Some(5.0)), None)),
        ));
        let (_schema, request, filters) = provider
            .plan_scan_parts(Some(&projection), &[range_filter], Some(5))
            .await
            .expect("unsupported payload filter planning");
        assert_eq!(request.limit, Some(5));
        assert!(
            !request
                .projection
                .columns
                .iter()
                .any(|column| column == "snapshot_content")
        );
        assert!(filters.is_empty());
    }

    #[test]
    fn native_payload_filters_fail_closed_on_malformed_and_out_of_range_values() {
        let malformed = tracked_row(StateRowSource::Branch, "project_message", "broken");
        let malformed = StateRow {
            value: StateValue {
                cell: StateCell::Value(SharedStr::from("{not-json")),
                ..malformed.value
            },
            ..malformed
        };
        let filter = RowRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: SchemaColumnType::String,
            value: RowFilterValue::String("hello".to_string()),
        };
        assert!(apply_row_state_filters(vec![malformed], &[filter]).is_err());

        let out_of_range = tracked_row_with_file(
            StateRowSource::Branch,
            "project_message",
            "wide",
            None,
            StateCell::Value(SharedStr::from(r#"{"count":9223372036854775808}"#)),
        );
        let filter = RowRowFilter::ColumnEq {
            column: "count".to_string(),
            column_type: SchemaColumnType::Integer,
            value: RowFilterValue::Integer(1),
        };
        let error = apply_row_state_filters(vec![out_of_range], &[filter])
            .expect_err("out-of-range BIGINT must fail closed");
        let error = crate::sql2::error::datafusion_error_to_lix_error(error);
        assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(error.message.contains("count"));
        assert!(error.message.contains("BIGINT"));
    }
}
