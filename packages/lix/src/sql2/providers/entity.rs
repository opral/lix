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
use crate::changelog::{ChangeId, CommitId};
use crate::commit_graph::CommitGraphReader;
use crate::common::{LixTimestamp, SharedStr};
use crate::entity_pk::EntityPk;
use crate::forktree::{
    CanonicalBranchId, ForkTreeReadFacade, StateCell, StateKeyRef, StateSource, decode_state_key,
    encode_state_key, state_points,
};
#[cfg(test)]
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateFilter, LiveStateProjection,
    LiveStateReader, LiveStateRowFilter, LiveStateScanRequest,
};
use crate::live_state::{MaterializedLiveStateBatch, MaterializedLiveStateBatchBuilder};
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
use crate::{GLOBAL_BRANCH_ID, LixError, NullableKeyFilter, parse_row_metadata_value};

use crate::sql2::{
    EntitySnapshotReader, SqlChangelogQuerySource, SqlWriteContext, WriteAccess,
    WriteContextLiveStateReader,
};
use crate::transaction::types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode,
};

use super::ProviderSelection;
use super::entity_history::register_entity_history_surface;
use datafusion::physical_plan::ExecutionPlan;

use super::spec::{
    DmlReturning, InsertApply, PlannedDml, PlannedScan, TableSpec, projected_schema,
    register_spec_table, row_source, scan_row_source, take_record_batch_rows,
};
use super::values::{
    optional_bool_value, optional_string_value, required_string_value, string_expr_literal,
};
use crate::storage_adapter::StorageAdapterRead;

#[derive(Clone, Debug)]
struct AuthenticatedEntityRow {
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    snapshot_content: Option<SharedStr>,
    metadata: Option<SharedStr>,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    change_id: Option<ChangeId>,
    commit_id: Option<CommitId>,
    untracked: bool,
    branch_id: SharedStr,
}

#[derive(Clone, Debug)]
struct AuthenticatedEntityExactRowRequest {
    entity_pk: EntityPk,
    file_id: Option<String>,
}

#[derive(Clone, Debug)]
struct AuthenticatedEntityExactBatchRequest {
    schema_key: String,
    branch_id: String,
    rows: Vec<AuthenticatedEntityExactRowRequest>,
    untracked: Option<bool>,
    include_tombstones: bool,
}

#[async_trait]
trait AuthenticatedEntityExactReader: Send + Sync {
    async fn load_exact(
        &self,
        request: AuthenticatedEntityExactBatchRequest,
    ) -> Result<Vec<Option<AuthenticatedEntityRow>>, LixError>;
}

struct ForkTreeAuthenticatedEntityExactReader<S> {
    facade: ForkTreeReadFacade<S>,
}

impl<S> ForkTreeAuthenticatedEntityExactReader<S> {
    fn new(facade: ForkTreeReadFacade<S>) -> Self {
        Self { facade }
    }
}

#[async_trait]
impl<S> AuthenticatedEntityExactReader for ForkTreeAuthenticatedEntityExactReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn load_exact(
        &self,
        request: AuthenticatedEntityExactBatchRequest,
    ) -> Result<Vec<Option<AuthenticatedEntityRow>>, LixError> {
        let view = self.facade.branch(&request.branch_id).await?;
        let encoded_keys = request
            .rows
            .iter()
            .map(|row| {
                encode_state_key(StateKeyRef {
                    schema_key: &request.schema_key,
                    file_id: row.file_id.as_deref(),
                    entity_pk: &row.entity_pk,
                })
            })
            .collect::<Vec<_>>();
        let tracked = if request.untracked == Some(true) {
            vec![None; encoded_keys.len()]
        } else {
            state_points(&view, &encoded_keys, true).await?
        };
        let untracked = if request.untracked == Some(false) {
            vec![None; encoded_keys.len()]
        } else {
            view.load_untracked_overlay_points(&encoded_keys).await?
        };
        if tracked.len() != request.rows.len() || untracked.len() != request.rows.len() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree exact entity lookup returned the wrong slot count",
            ));
        }

        let branch_id = SharedStr::from(request.branch_id);
        let mut output = Vec::with_capacity(request.rows.len());
        for ((requested, tracked), untracked) in
            request.rows.into_iter().zip(tracked).zip(untracked)
        {
            let row = if let Some((owner, key, value)) = untracked {
                if key.schema_key != request.schema_key
                    || key.entity_pk != requested.entity_pk
                    || key.file_id != requested.file_id
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "ForkTree exact entity untracked row identity does not match the request",
                    ));
                }
                if value.cell.deleted() && !request.include_tombstones {
                    None
                } else {
                    let deleted = value.cell.deleted();
                    Some(AuthenticatedEntityRow {
                        entity_pk: key.entity_pk,
                        schema_key: key.schema_key,
                        file_id: key.file_id,
                        snapshot_content: match value.cell {
                            StateCell::Value(value) => Some(value),
                            StateCell::Null | StateCell::Tombstone => None,
                        },
                        metadata: value.metadata,
                        deleted,
                        created_at: value.created_at,
                        updated_at: value.updated_at,
                        global: owner
                            == CanonicalBranchId::from_bytes(
                                *uuid::Uuid::parse_str(GLOBAL_BRANCH_ID)
                                    .expect("GLOBAL_BRANCH_ID must be a UUID")
                                    .as_bytes(),
                            ),
                        change_id: None,
                        commit_id: None,
                        untracked: true,
                        branch_id: SharedStr::from(
                            uuid::Uuid::from_bytes(*owner.as_bytes()).to_string(),
                        ),
                    })
                }
            } else if let Some(row) = tracked {
                let key = decode_state_key(&row.encoded_key)?;
                if key.schema_key != request.schema_key
                    || key.entity_pk != requested.entity_pk
                    || key.file_id != requested.file_id
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "ForkTree exact entity tracked row identity does not match the request",
                    ));
                }
                if row.value.cell.deleted() && !request.include_tombstones {
                    None
                } else {
                    let value = row.value;
                    let deleted = value.cell.deleted();
                    Some(AuthenticatedEntityRow {
                        entity_pk: key.entity_pk,
                        schema_key: key.schema_key,
                        file_id: key.file_id,
                        snapshot_content: match value.cell {
                            StateCell::Value(value) => Some(value),
                            StateCell::Null | StateCell::Tombstone => None,
                        },
                        metadata: value.metadata,
                        deleted,
                        created_at: value.created_at,
                        updated_at: value.updated_at,
                        global: matches!(row.source, StateSource::Global),
                        change_id: Some(value.change_id),
                        commit_id: Some(value.commit_id),
                        untracked: false,
                        branch_id: branch_id.clone(),
                    })
                }
            } else {
                None
            };
            output.push(row);
        }
        Ok(output)
    }
}

pub(crate) async fn register_entity_providers<S>(
    ctx: &SessionContext,
    active_branch_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
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
    let authenticated_exact_reader = query_source.as_ref().map(|source| {
        let reader: Arc<dyn AuthenticatedEntityExactReader> = Arc::new(
            ForkTreeAuthenticatedEntityExactReader::new(source.forktree_reader.clone()),
        );
        reader
    });
    for surface in catalog.surfaces() {
        if !selection.includes(surface) {
            continue;
        }
        match &surface.kind {
            PublicSurfaceKind::EntityBase { schema_key } if include_write_surfaces => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                let (surface_live_state, surface_snapshot_reader) = derived_surface_reader(
                    schema_key,
                    &live_state,
                    &entity_snapshot_reader,
                    &commit_graph,
                    &branch_ref,
                    true,
                    true,
                )?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(
                        EntitySpec::active(
                            spec,
                            surface_live_state,
                            Arc::clone(&branch_ref),
                            active_branch_id.to_string(),
                            surface_snapshot_reader,
                        )
                        .with_authenticated_exact_reader(
                            (!crate::live_state::is_derived_schema(schema_key))
                                .then(|| authenticated_exact_reader.as_ref().map(Arc::clone))
                                .flatten(),
                        ),
                    ),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityByBranch { schema_key } if include_write_surfaces => {
                let spec = catalog_entity_spec(catalog, schema_key)?;
                let (surface_live_state, surface_snapshot_reader) = derived_surface_reader(
                    schema_key,
                    &live_state,
                    &entity_snapshot_reader,
                    &commit_graph,
                    &branch_ref,
                    true,
                    false,
                )?;
                register_spec_table(
                    ctx,
                    &surface.name,
                    Arc::new(
                        EntitySpec::by_branch(
                            spec,
                            surface_live_state,
                            Arc::clone(&branch_ref),
                            surface_snapshot_reader,
                        )
                        .with_authenticated_exact_reader(
                            (!crate::live_state::is_derived_schema(schema_key))
                                .then(|| authenticated_exact_reader.as_ref().map(Arc::clone))
                                .flatten(),
                        ),
                    ),
                    WriteAccess::read_only(),
                )?;
            }
            PublicSurfaceKind::EntityHistory { schema_key } => {
                let (Some(commit_graph), Some(query_source)) =
                    (commit_graph.as_ref(), query_source.as_ref())
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
                    default_as_of_commit_id.clone().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "selected entity history provider is missing its pinned commit anchor",
                        )
                    })?,
                )?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn derived_surface_reader(
    schema_key: &str,
    live_state: &Arc<dyn LiveStateReader>,
    entity_snapshot_reader: &Option<Arc<dyn EntitySnapshotReader>>,
    commit_graph: &Option<Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>>,
    branch_ref: &Arc<dyn BranchRefReader>,
    include_recovery_roots: bool,
    include_retained_nodes: bool,
) -> Result<
    (
        Arc<dyn LiveStateReader>,
        Option<Arc<dyn EntitySnapshotReader>>,
    ),
    LixError,
> {
    if !matches!(
        schema_key,
        "lix_commit" | "lix_commit_edge" | crate::branch::BRANCH_REF_SCHEMA_KEY
    ) {
        return Ok((Arc::clone(live_state), entity_snapshot_reader.clone()));
    }
    let commit_graph = commit_graph.as_ref().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("derived surface '{schema_key}' is missing its ForkTree graph reader"),
        )
    })?;
    let derived_live_state: Arc<dyn LiveStateReader> =
        Arc::new(crate::commit_graph::CommitGraphLiveStateReader::new(
            schema_key,
            Arc::clone(commit_graph),
            Arc::clone(branch_ref),
            Some(Arc::clone(live_state)),
            include_recovery_roots,
            include_retained_nodes,
        ));
    let entity_snapshot_reader = Arc::new(crate::sql2::CanonicalEntitySnapshotProjection::new(
        Arc::clone(&derived_live_state),
    ));
    Ok((derived_live_state, Some(entity_snapshot_reader)))
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
    live_state: Arc<dyn LiveStateReader>,
    entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    authenticated_exact_reader: Option<Arc<dyn AuthenticatedEntityExactReader>>,
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
        entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    ) -> Self {
        Self {
            surface_name: spec.schema_key.clone(),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::Active),
            spec,
            live_state,
            entity_snapshot_reader,
            authenticated_exact_reader: None,
            branch_ref,
            branch_binding: BranchBinding::active(active_branch_id),
        }
    }

    fn with_authenticated_exact_reader(
        mut self,
        reader: Option<Arc<dyn AuthenticatedEntityExactReader>>,
    ) -> Self {
        self.authenticated_exact_reader = reader;
        self
    }

    fn active_with_write(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let active_branch_id = write_ctx.active_branch_id();
        let live_state: Arc<dyn LiveStateReader> =
            Arc::new(WriteContextLiveStateReader::new(write_ctx));
        let entity_snapshot_reader = Arc::new(crate::sql2::CanonicalEntitySnapshotProjection::new(
            Arc::clone(&live_state),
        ));
        Self::active(
            spec,
            live_state,
            branch_ref,
            active_branch_id,
            Some(entity_snapshot_reader),
        )
    }

    fn by_branch(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        branch_ref: Arc<dyn BranchRefReader>,
        entity_snapshot_reader: Option<Arc<dyn EntitySnapshotReader>>,
    ) -> Self {
        Self {
            surface_name: format!("{}_by_branch", spec.schema_key),
            schema: entity_surface_schema(&spec, EntitySurfaceShape::ByBranch),
            spec,
            live_state,
            entity_snapshot_reader,
            authenticated_exact_reader: None,
            branch_ref,
            branch_binding: BranchBinding::explicit(),
        }
    }

    fn by_branch_with_write(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        branch_ref: Arc<dyn BranchRefReader>,
    ) -> Self {
        let live_state: Arc<dyn LiveStateReader> =
            Arc::new(WriteContextLiveStateReader::new(write_ctx));
        let entity_snapshot_reader = Arc::new(crate::sql2::CanonicalEntitySnapshotProjection::new(
            Arc::clone(&live_state),
        ));
        Self::by_branch(spec, live_state, branch_ref, Some(entity_snapshot_reader))
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
        if self.spec.schema_key == crate::branch::BRANCH_REF_SCHEMA_KEY
            && matches!(self.branch_binding, BranchBinding::Active { .. })
        {
            // Branch-ref rows are authenticated in the global selector scope;
            // an active SQL branch is only the caller's session context and
            // must not be mistaken for the row's storage domain.
            request.filter.branch_ids = vec![GLOBAL_BRANCH_ID.to_string()];
        }
        apply_exact_branch_id_filter(&mut request, exact_branch_ids);
        apply_exact_entity_pk_filters(&mut request, &self.spec, filters)?;
        apply_exact_file_id_filters(&mut request, filters)?;
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
            BranchBinding::Active { .. } => self
                .branch_binding
                .active_branch_id()
                .expect("active branch binding has an ID")
                .to_owned(),
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
            file_id: optional_string_value(
                batch,
                row_index,
                "lixcol_file_id",
                "UPDATE entity surface RETURNING",
            )?,
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
        let request = entity_live_state_scan_request(
            &self.spec.schema_key,
            self.branch_binding.active_branch_id(),
            Some(self.schema.as_ref()),
            None,
            false,
        );
        let exact_request = LiveStateExactBatchRequest {
            rows: keys
                .iter()
                .map(|key| LiveStateExactRowRequest {
                    schema_key: self.spec.schema_key.clone(),
                    branch_id: key.branch_id.clone(),
                    entity_pk: key.entity_pk.clone(),
                    file_id: key.file_id.clone(),
                })
                .collect(),
            projection: request.projection.clone(),
            untracked: None,
            include_tombstones: false,
        };
        let rows = WriteContextLiveStateReader::new(write_ctx.clone())
            .load_exact_batch(&exact_request)
            .await
            .map_err(lix_error_to_datafusion_error)?
            .into_present_batch();
        let batch = entity_record_batch_with_parsed(
            &self.spec,
            Arc::clone(&self.schema),
            &rows,
            EntityBatchProjection::for_request(&request),
            None,
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
        let (schema, request, row_filters) = self.plan_scan_parts(None, filters, None).await?;
        let exact_request = exact_live_state_batch_request(&request);
        let batch_projection = EntityBatchProjection::for_request(&request);
        let authenticated_exact_reader = self.authenticated_exact_reader.clone();
        let direct_authenticated_exact = exact_request.is_some();
        let source = row_source(
            (
                Arc::clone(&self.spec),
                Arc::clone(&self.live_state),
                schema,
                request,
                exact_request,
                row_filters,
                batch_projection,
                authenticated_exact_reader,
            ),
            move |(
                spec,
                live_state,
                schema,
                request,
                exact_request,
                row_filters,
                batch_projection,
                authenticated_exact_reader,
            )| async move {
                if direct_authenticated_exact {
                    let exact_reader = authenticated_exact_reader.ok_or_else(|| {
                        DataFusionError::Execution(
                            "typed entity exact update has no retained ForkTree capability".into(),
                        )
                    })?;
                    let exact_request = exact_request.as_ref().ok_or_else(|| {
                        DataFusionError::Execution(
                            "typed entity exact update lost its exact request".into(),
                        )
                    })?;
                    return load_authenticated_entity_record_batch(
                        &spec,
                        schema,
                        exact_reader.as_ref(),
                        exact_request,
                        &row_filters,
                        None,
                    )
                    .await;
                }
                let rows = load_entity_live_state_batch(
                    live_state.as_ref(),
                    &request,
                    exact_request.as_ref(),
                )
                .await
                .map_err(lix_error_to_datafusion_error)?;
                let filtered = apply_entity_batch_filters(rows, &row_filters)?;
                entity_record_batch_with_parsed(
                    &spec,
                    schema,
                    &filtered.rows,
                    batch_projection,
                    filtered.parsed_snapshots.as_deref(),
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
                    let rows = entity_update_stage_rows_from_batch(
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
struct EntityReturningKey {
    entity_pk: EntityPk,
    branch_id: String,
    file_id: Option<String>,
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

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let (schema, request, row_filters) =
            self.plan_scan_parts(projection, filters, limit).await?;
        let entity_snapshot_reader = self.entity_snapshot_reader.clone();
        let authenticated_exact_reader = self.authenticated_exact_reader.clone();
        let live_state = Arc::clone(&self.live_state);
        let batch_projection = EntityBatchProjection::for_request(&request);
        let exact_request = exact_live_state_batch_request(&request);
        let direct_authenticated_exact = exact_request.is_some();
        let exact_limit_requires_materialization =
            exact_request.is_some() && request.limit.is_some();
        let direct_entity_snapshot = !exact_limit_requires_materialization
            && direct_entity_batch_eligible(&schema, &request, &row_filters);
        let direct_primary_key_projection = !exact_limit_requires_materialization
            && direct_primary_key_projection_eligible(&self.spec, &schema, &request, &row_filters);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    Arc::clone(&self.spec),
                    schema,
                    request,
                    exact_request,
                    row_filters,
                    batch_projection,
                    entity_snapshot_reader,
                    authenticated_exact_reader,
                    live_state,
                    direct_entity_snapshot,
                    direct_primary_key_projection,
                ),
                move |(
                    spec,
                    schema,
                    request,
                    exact_request,
                    row_filters,
                    batch_projection,
                    entity_snapshot_reader,
                    authenticated_exact_reader,
                    live_state,
                    direct_entity_snapshot,
                    direct_primary_key_projection,
                )| async move {
                    if direct_authenticated_exact {
                        let exact_reader = authenticated_exact_reader.ok_or_else(|| {
                            DataFusionError::Execution(
                                "typed entity exact projection has no retained ForkTree capability"
                                    .into(),
                            )
                        })?;
                        let exact_request = exact_request.as_ref().ok_or_else(|| {
                            DataFusionError::Execution(
                                "typed entity exact projection lost its exact request".into(),
                            )
                        })?;
                        return load_authenticated_entity_record_batch(
                            &spec,
                            schema,
                            exact_reader.as_ref(),
                            exact_request,
                            &row_filters,
                            request.limit,
                        )
                        .await;
                    }
                    let entity_snapshot_reader = entity_snapshot_reader.ok_or_else(|| {
                        DataFusionError::Execution(
                            "entity projection has no authenticated ForkTree capability".into(),
                        )
                    })?;
                    if direct_primary_key_projection {
                        let entity_pks = if let Some(exact_request) = &exact_request {
                            entity_snapshot_reader
                                .load_exact_entity_primary_keys(exact_request.clone())
                                .await
                        } else {
                            entity_snapshot_reader
                                .scan_entity_primary_keys(request.clone())
                                .await
                        }
                        .map_err(lix_error_to_datafusion_error)?;
                        if let Some(entity_pks) = entity_pks {
                            return entity_primary_key_record_batch(&spec, schema, entity_pks);
                        }
                    }
                    if direct_entity_snapshot {
                        let rows = if let Some(exact_request) = &exact_request {
                            entity_snapshot_reader
                                .load_exact_entity_snapshots(exact_request.clone())
                                .await
                        } else {
                            entity_snapshot_reader
                                .scan_entity_snapshots(request.clone())
                                .await
                        }
                            .map_err(lix_error_to_datafusion_error)?
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "ForkTree entity snapshot projection is unavailable for this request".into(),
                                )
                            })?;
                        let decoder = EntityProjectionDecoder::new(
                            &spec,
                            schema.fields().iter().map(|field| field.name().as_str()),
                        )
                        .map_err(entity_projection_error_to_datafusion_error)?;
                        let columns = decoder
                            .decode_arrow_columns(rows.iter().map(Option::as_deref))
                            .map_err(entity_projection_error_to_datafusion_error)?;
                        return RecordBatch::try_new(schema, columns)
                            .map_err(DataFusionError::from);
                    }

                    let rows = load_entity_live_state_batch(
                        live_state.as_ref(),
                        &request,
                        exact_request.as_ref(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                    let filtered = apply_entity_batch_filters(rows, &row_filters)?;
                    entity_record_batch_with_parsed(
                        &spec,
                        schema,
                        &filtered.rows,
                        batch_projection,
                        filtered.parsed_snapshots.as_deref(),
                    )
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
                Arc::clone(&self.live_state),
                schema,
                request,
                row_filters,
                batch_projection,
            ),
            |(spec, live_state, schema, request, row_filters, batch_projection)| async move {
                let rows = live_state
                    .scan_batch(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
                let filtered = apply_entity_batch_filters(rows, &row_filters)?;
                entity_record_batch_with_parsed(
                    &spec,
                    schema,
                    &filtered.rows,
                    batch_projection,
                    filtered.parsed_snapshots.as_deref(),
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
) -> Result<RawWriteBatch> {
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
        let mut snapshot = parse_snapshot_value(&required_string_value(
            batch,
            row_index,
            "lixcol_snapshot_content",
            "UPDATE entity surface",
        )?)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "UPDATE entity surface has invalid lixcol_snapshot_content: {error}"
            ))
        })?;
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

fn apply_exact_file_id_filters(request: &mut LiveStateScanRequest, filters: &[Expr]) -> Result<()> {
    if let Some(file_ids) = exact_file_ids_from_filters(filters)? {
        if file_ids.is_empty() {
            request.filter.rows = LiveStateRowFilter::None;
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

fn exact_live_state_batch_request(
    request: &LiveStateScanRequest,
) -> Option<LiveStateExactBatchRequest> {
    if !matches!(request.filter.rows, LiveStateRowFilter::All)
        || !request.filter.constraints.is_empty()
        || request.filter.entity_pks.is_empty()
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
        .entity_pks
        .iter()
        .cloned()
        .map(|entity_pk| LiveStateExactRowRequest {
            schema_key: schema_key.clone(),
            branch_id: branch_id.clone(),
            entity_pk,
            file_id: file_id.clone(),
        })
        .collect::<Vec<_>>();
    Some(LiveStateExactBatchRequest {
        rows,
        projection: request.projection.clone(),
        untracked: request.filter.untracked,
        include_tombstones: request.filter.include_tombstones,
    })
}

fn authenticated_entity_exact_request(
    request: &LiveStateExactBatchRequest,
) -> AuthenticatedEntityExactBatchRequest {
    let first = request
        .rows
        .first()
        .expect("an exact entity request must have at least one row");
    AuthenticatedEntityExactBatchRequest {
        schema_key: first.schema_key.clone(),
        branch_id: first.branch_id.clone(),
        rows: request
            .rows
            .iter()
            .map(|row| AuthenticatedEntityExactRowRequest {
                entity_pk: row.entity_pk.clone(),
                file_id: row.file_id.clone(),
            })
            .collect(),
        untracked: request.untracked,
        include_tombstones: request.include_tombstones,
    }
}

async fn load_authenticated_entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    reader: &dyn AuthenticatedEntityExactReader,
    request: &LiveStateExactBatchRequest,
    row_filters: &[EntityRowFilter],
    limit: Option<usize>,
) -> Result<RecordBatch> {
    let mut rows = reader
        .load_exact(authenticated_entity_exact_request(request))
        .await
        .map_err(lix_error_to_datafusion_error)?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    apply_authenticated_entity_filters(&mut rows, row_filters)?;
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    authenticated_entity_record_batch(spec, schema, &rows)
}

fn apply_authenticated_entity_filters(
    rows: &mut Vec<AuthenticatedEntityRow>,
    filters: &[EntityRowFilter],
) -> Result<()> {
    if filters.is_empty() {
        return Ok(());
    }
    let mut filtered = Vec::with_capacity(rows.len());
    for row in rows.drain(..) {
        let snapshot = row
            .snapshot_content
            .as_deref()
            .map(parse_snapshot_value)
            .transpose()
            .map_err(|error| {
                DataFusionError::External(Box::new(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "entity exact filter could not parse snapshot_content for schema '{}' entity_pk '{:?}': {error}",
                        row.schema_key, row.entity_pk
                    ),
                )))
            })?;
        let matches = filters.iter().try_fold(true, |matches, filter| {
            if !matches {
                return Ok(false);
            }
            filter.matches_snapshot(snapshot.as_ref(), &row.schema_key)
        })?;
        if matches {
            filtered.push(row);
        }
    }
    *rows = filtered;
    Ok(())
}

async fn load_entity_live_state_batch(
    reader: &dyn LiveStateReader,
    scan_request: &LiveStateScanRequest,
    exact_request: Option<&LiveStateExactBatchRequest>,
) -> std::result::Result<MaterializedLiveStateBatch, LixError> {
    if let Some(exact_request) = exact_request {
        let rows = reader
            .load_exact_batch(exact_request)
            .await?
            .into_present_batch();
        return Ok(rows.filter(|_| true, scan_request.limit));
    }
    reader.scan_batch(scan_request).await
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

pub(super) struct EntityPrimaryKeyFilterAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
    primary_key_component_types: Vec<crate::entity_pk::EntityPkComponentType>,
}

struct EntityRowFilterAnalyzer<'a> {
    spec: &'a EntitySurfaceSpec,
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
    fn analyze_filters(&self, filters: &[Expr]) -> Result<Vec<EntityRowFilter>> {
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
            Self::And(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                && right.matches_snapshot(snapshot, schema_key)?),
            Self::Or(left, right) => Ok(left.matches_snapshot(snapshot, schema_key)?
                || right.matches_snapshot(snapshot, schema_key)?),
        }
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
    rows: MaterializedLiveStateBatch,
    filters: &[EntityRowFilter],
) -> Result<FilteredEntityBatch> {
    if filters.is_empty() {
        return Ok(FilteredEntityBatch {
            rows,
            parsed_snapshots: None,
        });
    }
    let mut filtered = MaterializedLiveStateBatchBuilder::with_capacity(rows.len());
    let mut parsed_snapshots = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let Some(snapshot_content) = row.snapshot_content().map(AsRef::<str>::as_ref) else {
            continue;
        };
        let snapshot = parse_snapshot_value(snapshot_content).map_err(|error| {
            DataFusionError::External(Box::new(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "entity scan filter could not parse snapshot_content for schema '{}' entity_pk '{:?}': {error}",
                    row.schema_key(),
                    row.entity_pk()
                ),
            )))
        })?;
        let mut matches = true;
        for filter in filters {
            if !filter.matches_snapshot(Some(&snapshot), row.schema_key())? {
                matches = false;
                break;
            }
        }
        if matches {
            filtered.push_ref(row, None);
            parsed_snapshots.push(Some(snapshot));
        }
    }
    Ok(FilteredEntityBatch {
        rows: filtered.finish(),
        parsed_snapshots: Some(parsed_snapshots),
    })
}

struct FilteredEntityBatch {
    rows: MaterializedLiveStateBatch,
    /// Parsed snapshots retained only when row predicates already needed a
    /// DOM. Projection consumes this side column instead of parsing winners a
    /// second time.
    parsed_snapshots: Option<Vec<Option<JsonValue>>>,
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

fn direct_entity_batch_eligible(
    schema: &Schema,
    request: &LiveStateScanRequest,
    row_filters: &[EntityRowFilter],
) -> bool {
    !schema.fields().is_empty()
        && matches!(request.filter.rows, LiveStateRowFilter::All)
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
fn direct_primary_key_projection_eligible(
    spec: &EntitySurfaceSpec,
    schema: &Schema,
    request: &LiveStateScanRequest,
    row_filters: &[EntityRowFilter],
) -> bool {
    direct_entity_batch_eligible(schema, request, row_filters)
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
    fn for_request(request: &LiveStateScanRequest) -> Self {
        if request.filter.entity_pks.is_empty() {
            Self::RawTrackedProjection
        } else {
            Self::ParsedSnapshots
        }
    }
}

#[cfg(test)]
fn entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedLiveStateBatch,
    projection: EntityBatchProjection,
) -> Result<RecordBatch> {
    entity_record_batch_with_parsed(spec, schema, rows, projection, None)
}

fn entity_record_batch_with_parsed(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedLiveStateBatch,
    projection: EntityBatchProjection,
    parsed_snapshots: Option<&[Option<JsonValue>]>,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }
    if let Some(parsed_snapshots) = parsed_snapshots {
        debug_assert_eq!(parsed_snapshots.len(), rows.len());
        return entity_record_batch_from_parsed_snapshots(spec, schema, rows, parsed_snapshots);
    }

    match projection {
        EntityBatchProjection::ParsedSnapshots => {
            entity_record_batch_from_snapshots(spec, schema, rows)
        }
        EntityBatchProjection::RawTrackedProjection if rows.iter().all(|row| !row.untracked()) => {
            entity_record_batch_from_raw_projection(spec, schema, rows)
        }
        // Raw projection depends on the tracked write invariant: compact
        // TransactionJson bytes with no duplicate-key recovery semantics.
        // Keep every mixed-retention batch on the established parser path.
        EntityBatchProjection::RawTrackedProjection => {
            entity_record_batch_from_snapshots(spec, schema, rows)
        }
    }
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

fn entity_record_batch_from_snapshots(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedLiveStateBatch,
) -> Result<RecordBatch> {
    let snapshots = rows
        .iter()
        .map(|row| parse_snapshot(row.snapshot_content().map(AsRef::<str>::as_ref)))
        .collect::<Result<Vec<_>>>()?;

    entity_record_batch_from_parsed_snapshots(spec, schema, rows, &snapshots)
}

fn entity_record_batch_from_parsed_snapshots(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedLiveStateBatch,
    snapshots: &[Option<JsonValue>],
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| entity_column_array(spec, field.name(), rows, snapshots))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn entity_record_batch_from_raw_projection(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedLiveStateBatch,
) -> Result<RecordBatch> {
    let decoder = EntityProjectionDecoder::new(
        spec,
        schema.fields().iter().filter_map(|field| {
            (!field.name().starts_with("lixcol_")).then_some(field.name().as_str())
        }),
    )
    .map_err(entity_projection_error_to_datafusion_error)?;
    // The tracked write path persists TransactionJson-normalized bytes.
    // Visibility is resolved by the existing live-state reader first.
    let mut visible_columns = decoder
        .decode_arrow_columns(rows.iter().map(|row| {
            row.snapshot_content()
                .map(AsRef::<str>::as_ref)
                .map(str::as_bytes)
        }))
        .map_err(entity_projection_error_to_datafusion_error)?
        .into_iter();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            field.name().strip_prefix("lixcol_").map_or_else(
                || {
                    visible_columns.next().ok_or_else(|| {
                        DataFusionError::Execution(
                            "entity projection decoder did not return a visible column".to_string(),
                        )
                    })
                },
                |property_name| entity_system_column_array(property_name, rows),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn authenticated_entity_record_batch(
    spec: &EntitySurfaceSpec,
    schema: SchemaRef,
    rows: &[AuthenticatedEntityRow],
) -> Result<RecordBatch> {
    if rows
        .iter()
        .any(|row| row.deleted && row.snapshot_content.is_some())
    {
        return Err(DataFusionError::Execution(
            "authenticated entity tombstone carried payload bytes".into(),
        ));
    }
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        return RecordBatch::try_new_with_options(schema, vec![], &options)
            .map_err(DataFusionError::from);
    }
    let decoder = EntityProjectionDecoder::new(
        spec,
        schema.fields().iter().filter_map(|field| {
            (!field.name().starts_with("lixcol_")).then_some(field.name().as_str())
        }),
    )
    .map_err(entity_projection_error_to_datafusion_error)?;
    let mut visible_columns = decoder
        .decode_arrow_columns(
            rows.iter()
                .map(|row| row.snapshot_content.as_ref().map(AsRef::<[u8]>::as_ref)),
        )
        .map_err(entity_projection_error_to_datafusion_error)?
        .into_iter();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            field.name().strip_prefix("lixcol_").map_or_else(
                || {
                    visible_columns.next().ok_or_else(|| {
                        DataFusionError::Execution(
                            "authenticated entity projection decoder did not return a visible column"
                                .to_string(),
                        )
                    })
                },
                |property_name| authenticated_entity_system_column_array(property_name, rows),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

#[expect(trivial_casts)]
fn authenticated_entity_system_column_array(
    column_name: &str,
    rows: &[AuthenticatedEntityRow],
) -> Result<ArrayRef> {
    let array = match column_name {
        "entity_pk" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.entity_pk.as_json_array_text().map(Some))
                .collect::<std::result::Result<Vec<_>, LixError>>()
                .map_err(lix_error_to_datafusion_error)?,
        )) as ArrayRef,
        "schema_key" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.schema_key.as_str())),
        )) as ArrayRef,
        "file_id" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.file_id.as_deref()),
        )) as ArrayRef,
        "snapshot_content" => {
            Arc::new(StringArray::from_iter(rows.iter().map(|row| {
                row.snapshot_content.as_ref().map(AsRef::<str>::as_ref)
            }))) as ArrayRef
        }
        "metadata" => Arc::new(StringArray::from_iter(rows.iter().map(|row| {
            row.metadata
                .as_ref()
                .map(AsRef::<str>::as_ref)
                .map(crate::serialize_row_metadata)
        }))) as ArrayRef,
        "created_at" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.created_at.to_string())),
        )) as ArrayRef,
        "updated_at" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some(row.updated_at.to_string())),
        )) as ArrayRef,
        "global" => Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.global)),
        )) as ArrayRef,
        "change_id" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.change_id.map(|id| id.to_string())),
        )) as ArrayRef,
        "commit_id" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.commit_id.map(|id| id.to_string())),
        )) as ArrayRef,
        "untracked" => Arc::new(BooleanArray::from_iter(
            rows.iter().map(|row| Some(row.untracked)),
        )) as ArrayRef,
        "branch_id" => Arc::new(StringArray::from_iter(
            rows.iter().map(|row| Some::<&str>(row.branch_id.as_ref())),
        )) as ArrayRef,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "sql2 entity provider does not support system column 'lixcol_{column_name}'"
            )));
        }
    };
    Ok(array)
}

#[expect(trivial_casts)]
fn entity_column_array(
    spec: &EntitySurfaceSpec,
    column_name: &str,
    rows: &MaterializedLiveStateBatch,
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
                .map(|value| entity_i64_value(*value, &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Number => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| entity_f64_value(*value, &spec.schema_key, column_name))
                .collect::<Result<Vec<_>>>()?,
        )) as ArrayRef,
        EntityColumnType::Boolean => Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| value.and_then(JsonValue::as_bool))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    })
}

/// Materialize `lixcol_*` system columns from borrowed batch rows.
///
/// Identity dictionaries and payload arenas remain owned by the live-state
/// batch until Arrow has copied the selected values into its output buffers;
/// no terminal row DTOs are manufactured on this path.
fn entity_system_column_array(
    column_name: &str,
    rows: &MaterializedLiveStateBatch,
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
        "snapshot_content" => Arc::new(StringArray::from_iter(
            rows.iter()
                .map(|row| row.snapshot_content().map(AsRef::<str>::as_ref)),
        )) as ArrayRef,
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

#[cfg(test)]
thread_local! {
    static ENTITY_SNAPSHOT_PARSE_COUNT: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
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
    use crate::live_state::{
        LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateRowFilter,
        LiveStateScanRequest, MaterializedLiveStateBatch, MaterializedLiveStateRow,
    };
    use crate::sql2::catalog::{
        EntityColumnType, EntitySurfaceShape, derive_entity_surface_spec_from_schema,
        entity_surface_schema, schema_exposed_as_entity_history_surface,
        schema_exposed_as_entity_surface,
    };

    struct EmptyLiveStateReader;
    struct EmptyBranchRefReader;

    struct ExactOnlyLiveStateReader {
        result: crate::live_state::MaterializedLiveStateExactBatch,
        scans: AtomicUsize,
    }

    #[async_trait]
    impl LiveStateReader for ExactOnlyLiveStateReader {
        async fn load_exact_batch(
            &self,
            _request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
            Ok(self.result.clone())
        }

        async fn scan_batch(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<MaterializedLiveStateBatch, LixError> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            Ok(MaterializedLiveStateBatch::default())
        }
    }

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
            crate::live_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<MaterializedLiveStateBatch, LixError> {
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

        async fn scan_live_state_batch(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<MaterializedLiveStateBatch, LixError> {
            Ok(MaterializedLiveStateBatch::default())
        }

        async fn load_exact_live_state_batch(
            &mut self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
            Ok(
                crate::live_state::MaterializedLiveStateExactBatch::from_rows(vec![
                    None;
                    request
                        .rows
                        .len()
                ]),
            )
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

        async fn stage_typed_mutation_journal_replace(
            &mut self,
            _rows: crate::transaction::types::TypedMutationJournalBatch,
        ) -> Result<crate::transaction::types::TransactionWriteOutcome, LixError> {
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

    fn live_row() -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
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

    fn live_batch(rows: Vec<MaterializedLiveStateRow>) -> MaterializedLiveStateBatch {
        MaterializedLiveStateBatch::from_rows(rows)
    }

    struct ExactEntityReaderFixture {
        result: Vec<Option<super::AuthenticatedEntityRow>>,
        expected_file_ids: Vec<Option<String>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl super::AuthenticatedEntityExactReader for ExactEntityReaderFixture {
        async fn load_exact(
            &self,
            request: super::AuthenticatedEntityExactBatchRequest,
        ) -> Result<Vec<Option<super::AuthenticatedEntityRow>>, LixError> {
            assert_eq!(
                request
                    .rows
                    .iter()
                    .map(|row| row.file_id.clone())
                    .collect::<Vec<_>>(),
                self.expected_file_ids
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.result.clone())
        }
    }

    fn authenticated_row(
        entity_pk: &str,
        snapshot_content: Option<&str>,
        deleted: bool,
    ) -> super::AuthenticatedEntityRow {
        authenticated_row_with_file(entity_pk, None, snapshot_content, deleted)
    }

    fn authenticated_row_with_file(
        entity_pk: &str,
        file_id: Option<&str>,
        snapshot_content: Option<&str>,
        deleted: bool,
    ) -> super::AuthenticatedEntityRow {
        super::AuthenticatedEntityRow {
            entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
            schema_key: "project_message".to_string(),
            file_id: file_id.map(str::to_string),
            snapshot_content: snapshot_content.map(Into::into),
            metadata: None,
            deleted,
            created_at: LixTimestamp::expect_parse("created_at", "2026-04-23T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-04-23T01:00:00Z"),
            global: false,
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked: false,
            branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
        }
    }

    #[tokio::test]
    async fn authenticated_exact_projection_preserves_slots_order_and_tombstones() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" }
                },
                "required": []
            }))
            .expect("tombstone control schema should derive"),
        );
        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        let entity_a = crate::entity_pk::EntityPk::single("a");
        let entity_b = crate::entity_pk::EntityPk::single("b");
        let request = crate::live_state::LiveStateExactBatchRequest {
            rows: vec![
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "project_message".to_string(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    entity_pk: entity_a.clone(),
                    file_id: None,
                },
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "project_message".to_string(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    entity_pk: entity_b.clone(),
                    file_id: Some("file-b".to_string()),
                },
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "project_message".to_string(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    entity_pk: entity_a,
                    file_id: None,
                },
            ],
            include_tombstones: true,
            ..Default::default()
        };
        let reader = ExactEntityReaderFixture {
            result: vec![
                Some(authenticated_row(
                    "a",
                    Some(r#"{"id":"a","body":"first"}"#),
                    false,
                )),
                Some(authenticated_row_with_file("b", Some("file-b"), None, true)),
                Some(authenticated_row(
                    "a",
                    Some(r#"{"id":"a","body":"last"}"#),
                    false,
                )),
            ],
            expected_file_ids: vec![None, Some("file-b".to_string()), None],
            calls: AtomicUsize::new(0),
        };
        let batch = super::load_authenticated_entity_record_batch(
            &spec,
            schema,
            &reader,
            &request,
            &[],
            None,
        )
        .await
        .expect("authenticated exact projection");

        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(batch.num_rows(), 3);
        let body = batch
            .column_by_name("body")
            .expect("body column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("body is utf8");
        assert_eq!(body.value(0), "first");
        assert!(body.is_null(1), "tombstone stays a visible null row");
        assert_eq!(body.value(2), "last");
    }

    #[tokio::test]
    async fn authenticated_exact_projection_applies_residual_filters_before_projection() {
        let spec = entity_insert_spec_with_primary_key();
        let schema = entity_surface_schema(&spec, EntitySurfaceShape::Active);
        let request = crate::live_state::LiveStateExactBatchRequest {
            rows: vec![
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "project_message".to_string(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single("a"),
                    file_id: None,
                },
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "project_message".to_string(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single("b"),
                    file_id: Some("file-b".to_string()),
                },
            ],
            ..Default::default()
        };
        let reader = ExactEntityReaderFixture {
            result: vec![
                Some(authenticated_row(
                    "a",
                    Some(r#"{"id":"a","body":"drop"}"#),
                    false,
                )),
                Some(authenticated_row_with_file(
                    "b",
                    Some("file-b"),
                    Some(r#"{"id":"b","body":"keep"}"#),
                    false,
                )),
            ],
            expected_file_ids: vec![None, Some("file-b".to_string())],
            calls: AtomicUsize::new(0),
        };
        let filters = vec![super::EntityRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: EntityColumnType::String,
            value: super::EntityFilterValue::String("keep".to_string()),
        }];
        let batch = super::load_authenticated_entity_record_batch(
            &spec, schema, &reader, &request, &filters, None,
        )
        .await
        .expect("authenticated exact residual filter");
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column_by_name("body")
                .expect("body column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body is utf8")
                .value(0),
            "keep"
        );
    }

    #[test]
    fn exact_typed_scan_and_update_bypass_generic_materialization() {
        let source = include_str!("entity.rs");
        for owner_name in ["async fn plan_update_with_post_image", "async fn plan_scan"] {
            let start = source.find(owner_name).expect("typed owner exists");
            let owner = &source[start..];
            let direct = owner
                .find("if direct_authenticated_exact")
                .expect("exact dispatch guard");
            let typed = owner
                .find("load_authenticated_entity_record_batch")
                .expect("typed exact materialization");
            let generic = owner
                .find("load_entity_live_state_batch")
                .expect("generic path remains for non-exact scans");
            assert!(
                direct < typed && typed < generic,
                "{owner_name} re-entered generic materialization before typed exact dispatch"
            );
            let residual_gate = ["exact_request.is_some() &&", " row_filters.is_empty()"].concat();
            assert!(
                !owner.contains(&residual_gate),
                "{owner_name} must not gate authenticated exact reads on residual-filter absence"
            );
        }
    }

    #[test]
    fn exact_reader_overlay_order_and_cell_contract_is_explicit() {
        let source = include_str!("entity.rs");
        let start = source
            .find(
                "impl<S> AuthenticatedEntityExactReader for ForkTreeAuthenticatedEntityExactReader",
            )
            .expect("ForkTree exact reader owner");
        let end = source[start..]
            .find("pub(crate) async fn register_entity_providers")
            .map(|offset| start + offset)
            .expect("exact reader owner end");
        let owner = &source[start..end];
        assert!(owner.contains("state_points(&view, &encoded_keys, true)"));
        assert!(owner.contains("load_untracked_overlay_points(&encoded_keys)"));
        assert!(owner.contains("if let Some((owner, key, value)) = untracked"));
        assert!(owner.contains("row.value.cell.deleted() && !request.include_tombstones"));
        assert!(owner.contains("StateCell::Null | StateCell::Tombstone"));
        assert!(owner.contains("request.rows.into_iter().zip(tracked).zip(untracked)"));
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
        let mut request = LiveStateScanRequest::default();
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
            .push(crate::live_state::ScanConstraint {
                field: crate::live_state::ScanField::EntityPk,
                operator: crate::live_state::ScanOperator::Eq(crate::Value::Text(
                    "row".to_string(),
                )),
            });
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.constraints.clear();

        request.filter.rows = LiveStateRowFilter::None;
        assert!(!super::direct_entity_batch_eligible(
            &payload_schema,
            &request,
            &[]
        ));
        request.filter.rows = LiveStateRowFilter::All;

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
        let request = LiveStateScanRequest::default();
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
    fn filtered_entity_projection_reuses_each_candidate_parse_once() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": { "body": { "type": "string" } }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let mut winner = live_row();
        winner.untracked = true;
        let rejected = MaterializedLiveStateRow {
            snapshot_content: Some(r#"{"body":"goodbye"}"#.into()),
            ..live_row()
        };
        let tombstone = MaterializedLiveStateRow {
            snapshot_content: None,
            ..live_row()
        };
        let filter = super::EntityRowFilter::ColumnEq {
            column: "body".to_string(),
            column_type: EntityColumnType::String,
            value: super::EntityFilterValue::String("hello".to_string()),
        };

        super::reset_entity_snapshot_parse_count();
        let filtered = super::apply_entity_batch_filters(
            live_batch(vec![winner, rejected, tombstone]),
            &[filter],
        )
        .expect("entity filter should build a parsed side column");
        assert_eq!(filtered.rows.len(), 1);
        assert_eq!(
            filtered
                .parsed_snapshots
                .as_ref()
                .expect("filtered rows retain parsed snapshots")
                .len(),
            filtered.rows.len()
        );
        assert_eq!(super::entity_snapshot_parse_count(), 2);

        let batch = super::entity_record_batch_with_parsed(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &filtered.rows,
            super::EntityBatchProjection::RawTrackedProjection,
            filtered.parsed_snapshots.as_deref(),
        )
        .expect("mixed-retention projection should consume the parsed side column");
        assert_eq!(
            super::entity_snapshot_parse_count(),
            2,
            "Arrow projection must not parse a filtered winner again"
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
            "lix_checkpoint_marker",
            "lix_undo_redo_marker",
            "lix_collection_generation",
            "lix_directory_descriptor",
            "lix_file_descriptor",
        ] {
            assert!(!schema_exposed_as_entity_surface(schema_key));
            assert!(!schema_exposed_as_entity_history_surface(schema_key));
        }
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
    fn read_schema_keeps_defaulted_required_identity_non_null() {
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
        let row = MaterializedLiveStateRow {
            snapshot_content: Some(
                r#"{"body":"hello","rating":4.5,"count":7,"enabled":true,"meta":{"z":2,"a":1}}"#
                    .into(),
            ),
            ..live_row()
        };
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                entity_pks: vec![row.entity_pk.clone()],
                ..LiveStateFilter::default()
            },
            projection: LiveStateProjection::default(),
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
            &live_batch(vec![MaterializedLiveStateRow {
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
        let branch_snapshot = crate::transaction::types::TransactionJson::from_value(
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
        let global_snapshot = crate::transaction::types::TransactionJson::from_value(
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
        let branch_row = MaterializedLiveStateRow {
            entity_pk: crate::entity_pk::EntityPk::single("branch-row"),
            file_id: Some("file-branch".to_string()),
            snapshot_content: Some(branch_snapshot.normalized().into()),
            metadata: Some(r#"{"source":"branch"}"#.into()),
            ..live_row()
        };
        let global_row = MaterializedLiveStateRow {
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
    fn broad_raw_projection_keeps_invalid_snapshot_errors_on_the_execution_path() {
        let spec = Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": { "body": { "type": "string" } }
            }))
            .expect("schema should derive entity surface spec"),
        );
        let error = entity_record_batch(
            &spec,
            entity_surface_schema(&spec, EntitySurfaceShape::Active),
            &live_batch(vec![MaterializedLiveStateRow {
                snapshot_content: Some("{not-json".into()),
                ..live_row()
            }]),
            super::EntityBatchProjection::RawTrackedProjection,
        )
        .expect_err("malformed snapshot must fail");

        assert!(matches!(
            error,
            datafusion::common::DataFusionError::Execution(_)
        ));
        assert!(
            error
                .to_string()
                .contains("sql2 entity provider expected valid snapshot_content JSON"),
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
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
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

    #[test]
    fn exact_batch_requires_and_preserves_correlated_file_owner() {
        let mut request = LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec!["entity".into()],
                branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".into()],
                entity_pks: vec![
                    crate::entity_pk::EntityPk::single("b"),
                    crate::entity_pk::EntityPk::single("a"),
                ],
                file_ids: vec![crate::NullableKeyFilter::Null],
                ..Default::default()
            },
            limit: Some(1),
            ..Default::default()
        };
        let exact = super::exact_live_state_batch_request(&request)
            .expect("correlated identity should use exact batch");
        assert_eq!(exact.rows.len(), 2);
        assert_eq!(
            exact.rows[0].entity_pk,
            crate::entity_pk::EntityPk::single("b")
        );
        assert_eq!(exact.rows[0].file_id, None);
        assert_eq!(
            exact.rows[1].entity_pk,
            crate::entity_pk::EntityPk::single("a")
        );
        assert_eq!(exact.rows[1].file_id, None);

        request.filter.file_ids.clear();
        assert!(
            super::exact_live_state_batch_request(&request).is_none(),
            "PK-only identity must not guess a file owner"
        );
    }

    #[test]
    fn exact_file_filter_distinguishes_null_and_named_owner() {
        let null_filter = Expr::IsNull(Box::new(column("lixcol_file_id")));
        assert_eq!(
            super::exact_file_ids_from_filters(&[null_filter])
                .expect("NULL file filter should analyze"),
            Some(vec![None])
        );
        assert_eq!(
            super::exact_file_ids_from_filters(&[eq_filter("lixcol_file_id", "file-a")])
                .expect("named file filter should analyze"),
            Some(vec![Some("file-a".into())])
        );
    }

    #[tokio::test]
    async fn exact_limit_is_applied_after_missing_tombstone_and_duplicate_slots() {
        let request = LiveStateScanRequest {
            limit: Some(1),
            ..Default::default()
        };
        let exact_request = crate::live_state::LiveStateExactBatchRequest::default();
        let mut tracked = live_row();
        tracked.entity_pk = crate::entity_pk::EntityPk::single("visible-after-missing");
        let reader = ExactOnlyLiveStateReader {
            // The first two slots represent an absent row and a tombstone
            // omitted by visibility. LIMIT must select the later visible row.
            result: crate::live_state::MaterializedLiveStateExactBatch::from_rows(vec![
                None,
                None,
                Some(tracked),
            ]),
            scans: AtomicUsize::new(0),
        };
        let rows = super::load_entity_live_state_batch(&reader, &request, Some(&exact_request))
            .await
            .expect("exact visible batch");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.row(0).entity_pk(),
            &crate::entity_pk::EntityPk::single("visible-after-missing")
        );
        assert_eq!(reader.scans.load(Ordering::SeqCst), 0);

        let mut untracked = live_row();
        untracked.entity_pk = crate::entity_pk::EntityPk::single("duplicate");
        untracked.untracked = true;
        let reader = ExactOnlyLiveStateReader {
            result: crate::live_state::MaterializedLiveStateExactBatch::from_rows(vec![
                Some(untracked.clone()),
                Some(untracked),
            ]),
            scans: AtomicUsize::new(0),
        };
        let rows = super::load_entity_live_state_batch(&reader, &request, Some(&exact_request))
            .await
            .expect("duplicate exact slots");
        assert_eq!(rows.len(), 1);
        assert!(rows.row(0).untracked());
        assert_eq!(reader.scans.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn multi_value_file_filter_is_not_declared_or_applied_as_exact() {
        let filter = Expr::InList(InList::new(
            Box::new(column("lixcol_file_id")),
            vec![string_literal("file-a"), string_literal("file-b")],
            false,
        ));
        assert!(!super::ExactFileIdFilterAnalyzer.supports(&filter));
        let mut request = LiveStateScanRequest::default();
        super::apply_exact_file_id_filters(&mut request, &[filter])
            .expect("multi-owner predicate should remain residual");
        assert!(request.filter.file_ids.is_empty());
    }

    #[test]
    fn update_returning_post_image_uses_correlated_exact_identity() {
        let source = include_str!("entity.rs");
        let start = source
            .find("async fn returning_post_image")
            .expect("returning post-image owner");
        let end = source[start..]
            .find("async fn plan_update_with_post_image")
            .map(|offset| start + offset)
            .expect("next update owner");
        let owner = &source[start..end];
        assert!(owner.contains("LiveStateExactRowRequest"));
        assert!(owner.contains("file_id: key.file_id.clone()"));
        assert!(owner.contains(".load_exact_batch(&exact_request)"));
        assert!(!owner.contains(".scan_batch("));
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
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
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
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
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
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
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
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
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
        let mut rows = vec![MaterializedLiveStateRow {
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
        let mut rows = vec![MaterializedLiveStateRow {
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

    // Columnar read-planner tests were deleted with the superseded physical
    // reader; SQL scans now use the authenticated ForkTree row provider.
}
