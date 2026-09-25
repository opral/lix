use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};

use crate::binary_cas::BlobDataReader;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::common::LixTimestamp;
use crate::hot_state::{
    HotStateProjection, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateBatchBuilder, MaterializedHotStateRow, VisibilityBranchScope,
    VisibilityRequest, resolve_visible_batch,
};
use crate::plugin::runtime::PLUGIN_OWNER_KEY;
use crate::row_pk::{RowPk, RowPkComponentType};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind, SchemaSurfaceSpec};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::udfs::{ExecutionSlots, execution_slots};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    MaterializedTrackedStateBatch, MaterializedTrackedStateRow, TrackedStateContext,
    TrackedStateFilter, TrackedStateKey, TrackedStateReadColumns, TrackedStateScanRequest,
};

use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::schema::{
    RowBatchProjection, RowPrimaryKeyFilterAnalyzer, catalog_schema_spec,
    row_pks_from_primary_key_filters,
};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";
const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";

#[cfg(test)]
static STATE_AT_TRAVERSAL_PROBES: OnceLock<
    Mutex<std::collections::HashMap<(std::thread::ThreadId, String), Vec<(usize, usize)>>>,
> = OnceLock::new();

#[cfg(test)]
fn state_at_probe_key(commit_id: &str) -> (std::thread::ThreadId, String) {
    (std::thread::current().id(), commit_id.to_owned())
}

#[cfg(test)]
pub(crate) fn arm_state_at_traversal_probe(commit_id: &str) {
    STATE_AT_TRAVERSAL_PROBES
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("state-at traversal probe lock should remain available")
        .insert(state_at_probe_key(commit_id), Vec::new());
}

#[cfg(test)]
pub(crate) fn take_state_at_traversal_probe(commit_id: &str) -> Vec<(usize, usize)> {
    STATE_AT_TRAVERSAL_PROBES
        .get_or_init(|| Mutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("state-at traversal probe lock should remain available")
        .remove(&state_at_probe_key(commit_id))
        .expect("state-at traversal probe should be armed")
}

#[cfg(test)]
fn record_state_at_traversal_probe(commit_id: &str, request: &TrackedStateScanRequest) {
    if let Some(probes) = STATE_AT_TRAVERSAL_PROBES.get()
        && let Some(requests) = probes
            .lock()
            .expect("state-at traversal probe lock should remain available")
            .get_mut(&state_at_probe_key(commit_id))
    {
        requests.push((request.filter.row_pks.len(), request.filter.file_ids.len()));
    }
}

#[cfg(not(test))]
fn record_state_at_traversal_probe(_commit_id: &str, _request: &TrackedStateScanRequest) {}

#[cfg(test)]
fn record_state_at_point_probe(commit_id: &str, requested: usize, resolved: usize) {
    if let Some(probes) = STATE_AT_TRAVERSAL_PROBES.get()
        && let Some(requests) = probes
            .lock()
            .expect("state-at traversal probe lock should remain available")
            .get_mut(&state_at_probe_key(commit_id))
    {
        requests.push((requested, resolved));
    }
}

#[cfg(not(test))]
fn record_state_at_point_probe(_commit_id: &str, _requested: usize, _resolved: usize) {}

pub(super) fn register_state_at_function<S>(
    session: &datafusion::prelude::SessionContext,
    query_source: SqlChangelogQuerySource<S>,
    catalog: Arc<PublicCatalog>,
    active_branch_id: String,
    blob_reader: Arc<dyn BlobDataReader>,
) where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    session.register_udtf(
        "lix_as_of",
        Arc::new(StateAtFunction {
            store: query_source.store,
            catalog,
            slots: execution_slots(session),
            active_branch_id,
            blob_reader,
        }),
    );
}

struct StateAtFunction<S> {
    store: S,
    catalog: Arc<PublicCatalog>,
    slots: Arc<ExecutionSlots>,
    active_branch_id: String,
    blob_reader: Arc<dyn BlobDataReader>,
}

impl<S> fmt::Debug for StateAtFunction<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateAtFunction")
            .finish_non_exhaustive()
    }
}

impl<S> TableFunctionImpl for StateAtFunction<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let [relation, commit_id] = args else {
            return Err(DataFusionError::Plan(
                "lix_as_of requires a relation and exactly one commit ID argument".into(),
            ));
        };
        let relation_name = text_argument(relation, 1, "relation name", None)?;
        let commit_id = text_argument(commit_id, 2, "commit ID", Some(&self.slots))?;
        let surface = self.catalog.surface(&relation_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_as_of does not support relation '{relation_name}'"
            ))
        })?;
        let schema = relation_state_schema(&self.catalog, &relation_name)?;
        let kind = match &surface.kind {
            PublicSurfaceKind::SchemaBase { schema_key } => StateRelationKind::Schema {
                schema_key: schema_key.clone(),
                spec: catalog_schema_spec(&self.catalog, schema_key)
                    .map_err(lix_error_to_datafusion_error)?,
            },
            PublicSurfaceKind::File => StateRelationKind::File,
            PublicSurfaceKind::Directory => StateRelationKind::Directory,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "lix_as_of does not support relation '{relation_name}'"
                )));
            }
        };
        Ok(Arc::new(SpecTableProvider::new(Arc::new(StateAtSpec {
            store: self.store.clone(),
            relation_name,
            kind,
            schema,
            commit_id,
            root_commit_id: self.slots.root_commit_id(),
            active_branch_id: self.active_branch_id.clone(),
            blob_reader: Arc::clone(&self.blob_reader),
        }))))
    }
}

/// The shared discovery and execution schema for historical relation reads.
/// An endpoint can predate a required column added with a default, so custom
/// non-key columns can return NULL even when today's relation is non-nullable.
pub(crate) fn relation_state_schema(
    catalog: &PublicCatalog,
    relation_name: &str,
) -> Result<SchemaRef> {
    let surface = catalog.surface(relation_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "lix_as_of does not support relation '{relation_name}'"
        ))
    })?;
    let schema = catalog.surface_schema(relation_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "lix_as_of does not support relation '{relation_name}'"
        ))
    })?;
    let PublicSurfaceKind::SchemaBase { schema_key } = &surface.kind else {
        return match surface.kind {
            PublicSurfaceKind::File | PublicSurfaceKind::Directory => Ok(schema),
            _ => Err(DataFusionError::Plan(format!(
                "lix_as_of does not support relation '{relation_name}'"
            ))),
        };
    };
    if crate::catalog::CatalogSnapshot::builtin()
        .plan_for_key(schema_key)
        .is_some()
    {
        return Ok(schema);
    }
    let spec = catalog_schema_spec(catalog, schema_key).map_err(lix_error_to_datafusion_error)?;
    Ok(Arc::new(
        datafusion::arrow::datatypes::Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| {
                    if !field.name().starts_with("lixcol_")
                        && !spec
                            .primary_key_paths
                            .iter()
                            .any(|path| path.as_slice() == [field.name().clone()])
                    {
                        Arc::new(field.as_ref().clone().with_nullable(true))
                    } else {
                        Arc::clone(field)
                    }
                })
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ),
    ))
}

fn text_argument(
    argument: &Expr,
    position: usize,
    expected: &str,
    slots: Option<&ExecutionSlots>,
) -> Result<String> {
    if let (Expr::ScalarFunction(function), Some(slots)) = (argument, slots)
        && function.args.is_empty()
    {
        let value = match function.func.name() {
            "lix_root_commit_id" => slots.root_commit_id(),
            "lix_active_branch_commit_id" => slots.active_branch_commit_id(),
            _ => None,
        };
        if let Some(value) = value {
            return Ok(value);
        }
    }
    let Expr::Literal(value, _) = argument else {
        return Err(DataFusionError::Plan(format!(
            "lix_as_of argument {position} must be a {expected} literal or parameter"
        )));
    };
    value
        .try_as_str()
        .flatten()
        .map(str::to_owned)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_as_of argument {position} must be a non-null text {expected}"
            ))
        })
}

#[derive(Clone)]
enum StateRelationKind {
    Schema {
        schema_key: String,
        spec: Arc<SchemaSurfaceSpec>,
    },
    File,
    Directory,
}

struct StateAtSpec<S> {
    store: S,
    relation_name: String,
    kind: StateRelationKind,
    schema: SchemaRef,
    commit_id: String,
    root_commit_id: Option<String>,
    active_branch_id: String,
    blob_reader: Arc<dyn BlobDataReader>,
}

/// Materialize historical bytes only for the identities selected by a file diff.
/// Reuse the historical relation so plugin reconstruction and replica demands
/// have the same semantics as `lix_as_of`.
pub(super) async fn diff_file_content<S>(
    store: S,
    blob_reader: Arc<dyn BlobDataReader>,
    commit_id: &str,
    active_branch_id: &str,
    ids: &[String],
) -> Result<std::collections::HashMap<String, Vec<u8>>>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    use datafusion::arrow::array::{Array, LargeBinaryArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use futures_util::TryStreamExt;
    if ids.is_empty() {
        return Ok(Default::default());
    }
    let spec = StateAtSpec {
        store,
        relation_name: "lix_file".into(),
        kind: StateRelationKind::File,
        schema: Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("content", DataType::LargeBinary, true),
        ])),
        commit_id: commit_id.into(),
        root_commit_id: None,
        active_branch_id: active_branch_id.into(),
        blob_reader,
    };
    let filter = col("id").in_list(ids.iter().map(|id| lit(id.clone())).collect(), false);
    let plan = spec
        .plan_scan(None, &[filter], None, &ExecutionProps::new())
        .await?;
    let mut stream = plan
        .source
        .open(0, Arc::new(datafusion::execution::TaskContext::default()))?;
    let mut content = std::collections::HashMap::new();
    while let Some(batch) = stream.try_next().await? {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bytes = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if bytes.is_null(i) {
                return Err(DataFusionError::Execution(
                    "existing historical file has null content".into(),
                ));
            }
            content.insert(ids.value(i).into(), bytes.value(i).to_vec());
        }
    }
    Ok(content)
}

#[async_trait]
impl<S> TableSpec for StateAtSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        "lix_as_of"
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        match &self.kind {
            StateRelationKind::Schema { spec, .. } => {
                let analyzer = RowPrimaryKeyFilterAnalyzer::new(spec);
                if analyzer.supports(filter) {
                    TableProviderFilterPushDown::Exact
                } else if analyzer.contains_routable_conjunct(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            }
            StateRelationKind::File | StateRelationKind::Directory => {
                if exact_string_column_constraint_from_filters(std::slice::from_ref(filter), "id")
                    .is_ok_and(|constraint| !matches!(constraint, FileIdConstraint::All))
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            }
        }
    }

    async fn plan_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let output_schema = projected_schema(&self.schema, projection);
        let private_registry = matches!(
            &self.kind,
            StateRelationKind::Schema { schema_key, .. } if schema_key == "lix_registered_schema"
        );
        let scan_limit = (!private_registry && filters.is_empty()).then_some(limit).flatten();
        let row_pks = match &self.kind {
            StateRelationKind::Schema { spec, .. } => {
                row_pks_from_primary_key_filters(spec, filters)?
            }
            StateRelationKind::File | StateRelationKind::Directory => {
                match exact_string_column_constraint_from_filters(filters, "id")? {
                    FileIdConstraint::All => None,
                    FileIdConstraint::None => Some(Vec::new()),
                    FileIdConstraint::Ids(ids) => Some(
                        ids.iter()
                            .map(|id| uuid_row_pk(id))
                            .collect::<Result<Vec<_>>>()?,
                    ),
                }
            }
        };
        let contradictory = row_pks.as_ref().is_some_and(Vec::is_empty);
        let kind = self.kind.clone();
        let schema = Arc::clone(&output_schema);
        let store = self.store.clone();
        let commit_id = self.commit_id.clone();
        let root_commit_id = self.root_commit_id.clone();
        let active_branch_id = self.active_branch_id.clone();
        let blob_reader = Arc::clone(&self.blob_reader);
        let relation_name = self.relation_name.clone();
        Ok(PlannedScan {
            schema: Arc::clone(&output_schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&output_schema),
                (
                    store,
                    kind,
                    schema,
                    commit_id,
                    root_commit_id,
                    active_branch_id,
                    blob_reader,
                    row_pks,
                    contradictory,
                ),
                move |(
                    store,
                    kind,
                    schema,
                    commit_id,
                    root_commit_id,
                    active_branch_id,
                    blob_reader,
                    row_pks,
                    contradictory,
                )| async move {
                    if root_commit_id.as_deref() == Some(commit_id.as_str()) {
                        return Ok(RecordBatch::new_empty(schema));
                    }
                    let descriptor = commit_state_descriptor(store.clone(), &commit_id).await?;
                    // A composite limit applies after overlay resolution. If
                    // pushed independently into each root, local tombstones
                    // can consume the prefix and hide later visible base rows.
                    let root_scan_limit = descriptor
                        .base_commit_id
                        .is_none()
                        .then_some(scan_limit)
                        .flatten();
                    let mut tracked = TrackedStateContext::new().reader(store.clone());
                    if contradictory {
                        return Ok(RecordBatch::new_empty(schema));
                    }
                    let (local_batches, local_extra_rows) = load_relation_at_commit(
                        &mut tracked,
                        &kind,
                        &schema,
                        &commit_id,
                        row_pks.as_ref(),
                        root_scan_limit,
                        true,
                    )
                    .await?;
                    let (base_batches, base_extra_rows) =
                        if let Some(base_commit_id) = descriptor.base_commit_id {
                            load_relation_at_commit(
                                &mut tracked,
                                &kind,
                                &schema,
                                &base_commit_id.to_string(),
                                row_pks.as_ref(),
                                root_scan_limit,
                                false,
                            )
                            .await?
                        } else {
                            (Vec::new(), Vec::new())
                        };
                    let local_replacement_scopes = if descriptor.base_commit_id.is_some() {
                        load_local_replacement_scopes(&mut tracked, &commit_id).await?
                    } else {
                        std::collections::BTreeSet::new()
                    };
                    let (local_ancestors, base_ancestors) = if row_pks.is_some() {
                        let parent_field = match kind {
                            StateRelationKind::Directory => Some("parent_id"),
                            StateRelationKind::File => Some("directory_id"),
                            StateRelationKind::Schema { .. } => None,
                        };
                        if let Some(parent_field) = parent_field {
                            load_effective_ancestor_directories(
                                &mut tracked,
                                &commit_id,
                                descriptor.base_commit_id,
                                &local_batches,
                                &base_batches,
                                parent_field,
                            )
                            .await?
                        } else {
                            (Vec::new(), Vec::new())
                        }
                    } else {
                        (Vec::new(), Vec::new())
                    };
                    let mut local_extra_rows = local_extra_rows;
                    local_extra_rows.extend(local_ancestors);
                    let mut base_extra_rows = base_extra_rows;
                    base_extra_rows.extend(base_ancestors);
                    let hot = tracked_to_hot(
                        &local_batches,
                        local_extra_rows,
                        &base_batches,
                        base_extra_rows,
                        &active_branch_id,
                        descriptor.global_scope,
                        &local_replacement_scopes,
                    )?;
                    let hot = if private_registry {
                        hot.filter(
                            |row| {
                                !super::schema::hidden_registered_schema_row(
                                    "lix_registered_schema",
                                    row.row_pk(),
                                )
                            },
                            None,
                        )
                    } else {
                        hot
                    };
                    let mut result = match kind {
                        StateRelationKind::Schema { spec, .. } => {
                            let request = HotStateScanRequest {
                                projection: HotStateProjection::default(),
                                ..Default::default()
                            };
                            if crate::catalog::CatalogSnapshot::builtin()
                                .plan_for_key(&spec.schema_key)
                                .is_some()
                            {
                                super::schema::row_record_batch(
                                    &spec,
                                    schema,
                                    &hot,
                                    RowBatchProjection::for_request(&request),
                                )?
                            } else {
                                historical_schema_record_batch(&spec, schema, &hot)?
                            }
                        }
                        StateRelationKind::Directory => {
                            super::directory::lix_directory_record_batch(&schema, &hot)
                                .map_err(lix_error_to_datafusion_error)?
                        }
                        StateRelationKind::File => super::file::lix_file_state_record_batch(
                            &schema,
                            &blob_reader,
                            schema.index_of("content").is_ok(),
                            hot.into_rows(),
                        )
                        .await
                        .map_err(lix_error_to_datafusion_error)?,
                    };
                    if let Some(limit) = scan_limit {
                        result = result.slice(0, result.num_rows().min(limit));
                    }
                    let _ = relation_name;
                    Ok(result)
                },
            ),
        })
    }
}

async fn load_local_replacement_scopes<S: StorageAdapterRead + Clone>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    commit_id: &str,
) -> Result<std::collections::BTreeSet<(String, Option<String>)>> {
    let markers = tracked
        .scan_batch_at_commit(
            commit_id,
            &tracked_request(
                vec![crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned()],
                None,
                None,
                None,
            ),
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
    markers
        .iter()
        .filter(|row| !row.deleted())
        .map(|row| {
            crate::collection_generation::collection_scope_from_row_pk(row.row_pk())
                .map_err(lix_error_to_datafusion_error)
        })
        .collect()
}

async fn load_relation_at_commit<S: StorageAdapterRead + Clone>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    kind: &StateRelationKind,
    schema: &SchemaRef,
    commit_id: &str,
    row_pks: Option<&Vec<RowPk>>,
    scan_limit: Option<usize>,
    record_probe: bool,
) -> Result<(
    Vec<MaterializedTrackedStateBatch>,
    Vec<MaterializedTrackedStateRow>,
)> {
    let (batches, ancestors) = match kind {
        StateRelationKind::Schema { schema_key, .. } => {
            if let Some(row_pks) = row_pks {
                let rows =
                    load_schema_points_at_commit(tracked, commit_id, schema_key, row_pks).await?;
                (Vec::new(), rows)
            } else {
                let request = tracked_request(vec![schema_key.clone()], None, None, scan_limit);
                if record_probe {
                    record_state_at_traversal_probe(commit_id, &request);
                }
                (
                    vec![tracked.scan_batch_at_commit(commit_id, &request).await],
                    Vec::new(),
                )
            }
        }
        StateRelationKind::Directory => {
            let request = tracked_request(
                vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into()],
                row_pks.cloned(),
                row_pks.map(|_| vec![crate::NullableKeyFilter::Null]),
                None,
            );
            if record_probe {
                record_state_at_traversal_probe(commit_id, &request);
            }
            let batch = tracked.scan_batch_at_commit(commit_id, &request).await;
            (vec![batch], Vec::new())
        }
        StateRelationKind::File => {
            let file_ids = row_pks.map(|keys| {
                keys.iter()
                    .filter_map(single_row_pk_string)
                    .map(crate::NullableKeyFilter::Value)
                    .collect()
            });
            let owner_file_ids = file_ids.clone();
            let mut file_schema_keys = vec![FILE_DESCRIPTOR_SCHEMA_KEY.into()];
            if ["content", "lixcol_change_id", "lixcol_author_id", "lixcol_updated_at"]
                .iter()
                .any(|column| schema.index_of(column).is_ok())
            {
                file_schema_keys.push(BLOB_REF_SCHEMA_KEY.into());
            }
            let file_request = tracked_request(file_schema_keys, None, file_ids, None);
            if record_probe {
                record_state_at_traversal_probe(commit_id, &file_request);
            }
            let file_batch = tracked.scan_batch_at_commit(commit_id, &file_request).await;
            let owner_batch = if schema.index_of("content").is_ok() {
                let owner_request = tracked_request(
                    vec![KEY_VALUE_SCHEMA_KEY.into()],
                    Some(vec![RowPk::single(PLUGIN_OWNER_KEY)]),
                    owner_file_ids,
                    None,
                );
                if record_probe {
                    record_state_at_traversal_probe(commit_id, &owner_request);
                }
                Some(
                    tracked
                        .scan_batch_at_commit(commit_id, &owner_request)
                        .await,
                )
            } else {
                None
            };
            let mut file_batches = vec![file_batch];
            if let Some(owner_batch) = owner_batch {
                file_batches.push(owner_batch);
            }
            if row_pks.is_some() {
                (file_batches, Vec::new())
            } else {
                let directory_request = tracked_request(
                    vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into()],
                    None,
                    None,
                    None,
                );
                if record_probe {
                    record_state_at_traversal_probe(commit_id, &directory_request);
                }
                (
                    {
                        file_batches.push(
                            tracked
                                .scan_batch_at_commit(commit_id, &directory_request)
                                .await,
                        );
                        file_batches
                    },
                    Vec::new(),
                )
            }
        }
    };
    let batches = batches
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(lix_error_to_datafusion_error)?;
    Ok((batches, ancestors))
}

async fn load_schema_points_at_commit<S: StorageAdapterRead + Clone>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    commit_id: &str,
    schema_key: &str,
    row_pks: &[RowPk],
) -> Result<Vec<MaterializedTrackedStateRow>> {
    let commit_id_typed = CommitId::parse_lix(commit_id, "lix_as_of commit ID")
        .map_err(lix_error_to_datafusion_error)?;
    let keys = tracked
        .enumerate_schema_row_pk_keys_at_commit(commit_id_typed, schema_key, row_pks)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    record_state_at_point_probe(commit_id, row_pks.len(), keys.len());
    Ok(tracked
        .load_projected_batch_at_commit(commit_id, &keys, &ChangeRecordProjection::full())
        .await
        .map_err(lix_error_to_datafusion_error)?
        .into_rows()
        .into_iter()
        .flatten()
        .collect())
}

fn tracked_request(
    schema_keys: Vec<String>,
    row_pks: Option<Vec<RowPk>>,
    file_ids: Option<Vec<crate::NullableKeyFilter<String>>>,
    limit: Option<usize>,
) -> TrackedStateScanRequest {
    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys,
            row_pks: row_pks.unwrap_or_default(),
            file_ids: file_ids.unwrap_or_default(),
            include_tombstones: true,
            ..Default::default()
        },
        read_columns: TrackedStateReadColumns::default(),
        limit,
    }
}

struct CommitStateDescriptor {
    base_commit_id: Option<CommitId>,
    global_scope: bool,
}

async fn commit_state_descriptor<S: StorageAdapterRead + Clone>(
    store: S,
    commit_id: &str,
) -> Result<CommitStateDescriptor> {
    let commit_id = CommitId::parse_lix(commit_id, "lix_as_of commit ID")
        .map_err(lix_error_to_datafusion_error)?;
    let manifest = crate::tracked_state::load_published_commit_state_topology(&store, commit_id)
        .await
        .map_err(lix_error_to_datafusion_error)?
        .ok_or_else(|| {
            lix_error_to_datafusion_error(
                crate::tracked_state::NativeMetadataRef::CommitStateHeader(commit_id.to_string())
                    .annotate_missing(crate::tracked_state::sync_history_required_for_commits(&[
                        commit_id,
                    ])),
            )
        })?;
    let node = crate::commit_graph::CommitGraphContext::new()
        .reader(store)
        .load_node(&commit_id)
        .await
        .map_err(lix_error_to_datafusion_error)?
        .ok_or_else(|| {
            lix_error_to_datafusion_error(
                crate::tracked_state::NativeMetadataRef::CommitGraphRecord(commit_id.to_string())
                    .annotate_missing(crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        format!("commit '{commit_id}' does not exist"),
                    )),
            )
        })?;
    Ok(CommitStateDescriptor {
        base_commit_id: node.base_commit_id,
        global_scope: manifest.global_scope(),
    })
}

fn uuid_row_pk(id: &str) -> Result<RowPk> {
    RowPk::from_json_values(
        &[serde_json::Value::String(id.to_owned())],
        &[RowPkComponentType::Uuid],
    )
    .map_err(|error| DataFusionError::Plan(format!("invalid lix_as_of id: {error}")))
}

fn single_row_pk_string(row_pk: &RowPk) -> Option<String> {
    match row_pk.as_json_array_value().ok()? {
        serde_json::Value::Array(values) => match values.as_slice() {
            [serde_json::Value::String(value)] => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

async fn load_effective_ancestor_directories<S: StorageAdapterRead>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    local_commit_id: &str,
    base_commit_id: Option<CommitId>,
    local_batches: &[MaterializedTrackedStateBatch],
    base_batches: &[MaterializedTrackedStateBatch],
    parent_field: &str,
) -> Result<(
    Vec<MaterializedTrackedStateRow>,
    Vec<MaterializedTrackedStateRow>,
)> {
    let descriptor_schema = if parent_field == "directory_id" {
        FILE_DESCRIPTOR_SCHEMA_KEY
    } else {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY
    };
    let mut effective_initial = std::collections::BTreeMap::new();
    for (batches, local) in [(base_batches, false), (local_batches, true)] {
        for row in batches
            .iter()
            .flat_map(MaterializedTrackedStateBatch::iter)
            .filter(|row| row.schema_key() == descriptor_schema)
        {
            let key = (row.row_pk().clone(), row.file_id().map(str::to_owned));
            if local || !effective_initial.contains_key(&key) {
                effective_initial.insert(
                    key,
                    (!row.deleted()).then_some(snapshot_text(
                        row.decoded_snapshot(),
                        row.snapshot_content(),
                        parent_field,
                    )?),
                );
            }
        }
    }
    let mut pending = effective_initial
        .into_values()
        .flatten()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>();
    let mut seen = std::collections::BTreeSet::new();
    let mut local_ancestors = Vec::new();
    let mut base_ancestors = Vec::new();
    let load_budget =
        crate::filesystem::MAX_DIRECTORY_PARENT_DEPTH + usize::from(parent_field == "directory_id");
    for _ in 0..load_budget {
        let ids = pending.difference(&seen).cloned().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok((local_ancestors, base_ancestors));
        }
        seen.extend(ids.iter().cloned());
        let keys = ids
            .iter()
            .map(|id| {
                Ok(TrackedStateKey {
                    schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                    file_id: None,
                    row_pk: uuid_row_pk(id)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let local = tracked
            .load_projected_batch_at_commit(local_commit_id, &keys, &ChangeRecordProjection::full())
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let base = if let Some(base_commit_id) = base_commit_id {
            Some(
                tracked
                    .load_projected_batch_at_commit(
                        &base_commit_id.to_string(),
                        &keys,
                        &ChangeRecordProjection::full(),
                    )
                    .await
                    .map_err(lix_error_to_datafusion_error)?,
            )
        } else {
            None
        };
        pending.clear();
        for index in 0..keys.len() {
            let (row, global) = match local.row(index) {
                Some(row) => (Some(row), false),
                None => (base.as_ref().and_then(|rows| rows.row(index)), true),
            };
            let Some(row) = row.filter(|row| !row.deleted()) else {
                continue;
            };
            if let Some(parent) =
                snapshot_text(row.decoded_snapshot(), row.snapshot_content(), "parent_id")?
            {
                pending.insert(parent);
            }
            if global {
                base_ancestors.push(row.to_owned());
            } else {
                local_ancestors.push(row.to_owned());
            }
        }
    }
    if pending.difference(&seen).next().is_none() {
        Ok((local_ancestors, base_ancestors))
    } else {
        Err(DataFusionError::Execution(format!(
            "lix_as_of directory tree exceeds {} levels",
            crate::filesystem::MAX_DIRECTORY_PARENT_DEPTH
        )))
    }
}

fn snapshot_text(
    decoded: Option<&Arc<crate::row_payload::TypedRow>>,
    raw: Option<&crate::common::SharedStr>,
    field: &str,
) -> Result<Option<String>> {
    if let Some(decoded) = decoded {
        return match decoded.row.get(field) {
            None | Some(lix_schema::Value::Null) => Ok(None),
            Some(lix_schema::Value::Text(value)) => Ok(Some(value.clone())),
            Some(lix_schema::Value::Uuid(value)) => Ok(Some(value.to_string())),
            _ => Err(DataFusionError::Execution(format!(
                "lix_as_of field '{field}' is not text"
            ))),
        };
    }
    let Some(raw) = raw else { return Ok(None) };
    let value: serde_json::Value = serde_json::from_str(raw.as_str()).map_err(|error| {
        DataFusionError::Execution(format!("invalid historical filesystem descriptor: {error}"))
    })?;
    Ok(value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned))
}

fn tracked_to_hot(
    local_batches: &[MaterializedTrackedStateBatch],
    local_extra_rows: Vec<MaterializedTrackedStateRow>,
    base_batches: &[MaterializedTrackedStateBatch],
    base_extra_rows: Vec<MaterializedTrackedStateRow>,
    _session_branch_id: &str,
    commit_is_global: bool,
    local_replacement_scopes: &std::collections::BTreeSet<(String, Option<String>)>,
) -> Result<MaterializedHotStateBatch> {
    const HISTORICAL_LOCAL_BRANCH_ID: &str = "__lix_as_of_local_overlay__";
    let branch_id = if commit_is_global {
        crate::GLOBAL_BRANCH_ID
    } else {
        HISTORICAL_LOCAL_BRANCH_ID
    };
    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(
        local_batches
            .iter()
            .chain(base_batches)
            .map(MaterializedTrackedStateBatch::len)
            .sum::<usize>()
            + local_extra_rows.len()
            + base_extra_rows.len(),
    );
    for (batches, extra_rows, global) in [
        (local_batches, local_extra_rows, commit_is_global),
        (base_batches, base_extra_rows, true),
    ] {
        let storage_branch_id = if global {
            crate::GLOBAL_BRANCH_ID
        } else {
            branch_id
        };
        for row in batches.iter().flat_map(MaterializedTrackedStateBatch::iter) {
            if global
                && !commit_is_global
                && base_row_suppressed_by_local_replacement(
                    row.schema_key(),
                    row.file_id(),
                    local_replacement_scopes,
                )
            {
                continue;
            }
            let ordinal = builder.len();
            builder.push_owned(MaterializedHotStateRow {
                row_pk: row.row_pk().clone(),
                schema_key: row.schema_key().to_owned(),
                file_id: row.file_id().map(str::to_owned),
                snapshot_content: row.snapshot_content().cloned(),
                metadata: row.metadata().cloned(),
                deleted: row.deleted(),
                created_at: row.created_at(),
                updated_at: row.updated_at(),
                global,
                change_id: Some(row.change_id()),
                author_id: row.author_id().to_owned(),
                commit_id: Some(row.commit_id()),
                untracked: false,
                branch_id: Arc::from(storage_branch_id),
            });
            builder.set_decoded_snapshot(ordinal, row.decoded_snapshot().cloned());
        }
        for row in extra_rows {
            if global
                && !commit_is_global
                && base_row_suppressed_by_local_replacement(
                    &row.schema_key,
                    row.file_id.as_deref(),
                    local_replacement_scopes,
                )
            {
                continue;
            }
            let ordinal = builder.len();
            let created_at = LixTimestamp::parse(&row.created_at).map_err(|error| {
                DataFusionError::Execution(format!("invalid created_at: {error}"))
            })?;
            let updated_at = LixTimestamp::parse(&row.updated_at).map_err(|error| {
                DataFusionError::Execution(format!("invalid updated_at: {error}"))
            })?;
            let decoded_snapshot = row.decoded_snapshot.clone();
            builder.push_owned(MaterializedHotStateRow {
                row_pk: row.row_pk,
                global,
                schema_key: row.schema_key,
                file_id: row.file_id,
                snapshot_content: row.snapshot_content,
                metadata: row.metadata,
                deleted: row.deleted,
                created_at,
                updated_at,
                change_id: Some(row.change_id),
                author_id: row.author_id,
                commit_id: Some(row.commit_id),
                untracked: false,
                branch_id: Arc::from(storage_branch_id),
            });
            builder.set_decoded_snapshot(ordinal, decoded_snapshot);
        }
    }
    Ok(resolve_visible_batch(
        builder.finish(),
        MaterializedHotStateBatch::default(),
        &VisibilityRequest {
            branch_scope: VisibilityBranchScope::BranchIds {
                branch_ids: vec![branch_id.to_owned()],
            },
            include_tombstones: false,
            limit: None,
        },
    ))
}

/// Historical rows retain exactly their endpoint values. Registration can be
/// untracked, so an old schema document need not exist at the endpoint. The
/// amendment contract preserves every existing column's type and key, and
/// only permits added nullable/defaulted columns. Validate all present values
/// against that contract without applying any current defaults.
fn historical_schema_record_batch(
    spec: &SchemaSurfaceSpec,
    schema: SchemaRef,
    rows: &MaterializedHotStateBatch,
) -> Result<RecordBatch> {
    let definition = crate::schema::parse_lix_schema(&spec.schema_document)
        .map_err(lix_error_to_datafusion_error)?;
    let compiled = lix_schema::CompiledSchema::compile(&definition)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut typed_rows = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let typed = row
            .materialize_decoded_snapshot()
            .map_err(lix_error_to_datafusion_error)?
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "historical schema row is missing its typed payload".into(),
                )
            })?;
        validate_historical_schema_row(spec, &compiled, row.schema_key(), row.row_pk(), &typed)?;
        typed_rows.push(typed);
    }
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            vec![],
            &datafusion::arrow::record_batch::RecordBatchOptions::new()
                .with_row_count(Some(rows.len())),
        )
        .map_err(DataFusionError::from);
    }
    let decoder = crate::sql2::RowProjectionDecoder::new(
        spec,
        schema.fields().iter().filter_map(|field| {
            (!field.name().starts_with("lixcol_")).then_some(field.name().as_str())
        }),
    )
    .map_err(lix_error_to_datafusion_error)?;
    let mut visible = decoder
        .decode_mixed_arrow_columns(typed_rows.iter().map(|typed| (None, Some(&typed.row))))
        .map_err(lix_error_to_datafusion_error)?
        .into_iter();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(name) = field.name().strip_prefix("lixcol_") {
                super::schema::row_system_column_array(name, rows)
            } else {
                visible.next().ok_or_else(|| {
                    DataFusionError::Execution("historical projection omitted a column".into())
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

fn validate_historical_schema_row(
    spec: &SchemaSurfaceSpec,
    compiled: &lix_schema::CompiledSchema,
    stored_schema: &str,
    stored_pk: &RowPk,
    typed: &crate::row_payload::TypedRow,
) -> Result<()> {
    if stored_schema != spec.schema_key {
        return Err(DataFusionError::Execution(
            "historical row schema does not match the relation".into(),
        ));
    }
    typed
        .validate_durable_envelope(stored_schema, stored_pk)
        .map_err(lix_error_to_datafusion_error)?;
    if typed.schema_fingerprint == spec.schema_fingerprint {
        compiled.validate_complete_row(&typed.row)
    } else {
        compiled.validate_row(&typed.row)
    }
    .map_err(|error| DataFusionError::Execution(format!("invalid historical row: {error}")))?;
    let key_values = compiled
        .primary_key()
        .iter()
        .map(|column| {
            typed.row.get(column).cloned().ok_or_else(|| {
                DataFusionError::Execution("historical row is missing a primary-key field".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let field_pk = RowPk::from_schema_values(&key_values)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    if &field_pk != stored_pk {
        return Err(DataFusionError::Execution(
            "historical row primary-key fields do not match the stored identity".into(),
        ));
    }
    Ok(())
}

fn base_row_suppressed_by_local_replacement(
    schema_key: &str,
    file_id: Option<&str>,
    scopes: &std::collections::BTreeSet<(String, Option<String>)>,
) -> bool {
    scopes.contains(&(schema_key.to_owned(), None))
        || file_id.is_some_and(|file_id| {
            scopes.contains(&(schema_key.to_owned(), Some(file_id.to_owned())))
        })
}

#[cfg(test)]
mod historical_projection_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn historical_projection_validates_all_present_fields_and_identity() {
        let document = json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"historical_validation",
            "columns":[{"name":"id","type":"text","nullable":false},
                       {"name":"body","type":"text","nullable":false},
                       {"name":"later","type":"int8","nullable":false,"default_value":9}],
            "primary_key":["id"]
        });
        let spec = crate::sql2::derive_schema_surface_spec_from_schema(&document).unwrap();
        let parsed = crate::schema::parse_lix_schema(&document).unwrap();
        let compiled = lix_schema::CompiledSchema::compile(&parsed).unwrap();
        let valid = crate::row_payload::TypedRow {
            schema_fingerprint: [0; 32],
            row_pk: vec![lix_schema::Value::Text("row".into())].into(),
            row: lix_schema::Row::from([
                ("id".to_owned(), lix_schema::Value::Text("row".into())),
                ("body".to_owned(), lix_schema::Value::Text("before".into())),
            ]),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        };
        let pk = RowPk::single("row");
        validate_historical_schema_row(&spec, &compiled, &spec.schema_key, &pk, &valid).unwrap();
        assert!(
            !valid.row.contains_key("later"),
            "validation never fills defaults"
        );
        for corruption in [
            "type",
            "unknown",
            "missing",
            "null",
            "field_pk",
            "envelope_pk",
            "schema",
            "current_incomplete",
        ] {
            let mut row = valid.clone();
            let mut schema_key = spec.schema_key.as_str();
            match corruption {
                "type" => {
                    row.row.insert("body", lix_schema::Value::Int8(1));
                }
                "unknown" => {
                    row.row.insert("unknown", lix_schema::Value::Null);
                }
                "missing" => {
                    row.row.remove("body");
                }
                "null" => {
                    row.row.insert("body", lix_schema::Value::Null);
                }
                "field_pk" => {
                    row.row
                        .insert("id", lix_schema::Value::Text("other".into()));
                }
                "envelope_pk" => {
                    row.row_pk = vec![lix_schema::Value::Text("other".into())].into();
                }
                "schema" => {
                    schema_key = "other";
                }
                "current_incomplete" => {
                    row.schema_fingerprint = spec.schema_fingerprint;
                }
                _ => unreachable!(),
            }
            assert!(
                validate_historical_schema_row(&spec, &compiled, schema_key, &pk, &row).is_err(),
                "{corruption}"
            );
        }
    }
}

/// Build an endpoint path index from descriptor batches only. Overlay tombstones
/// and collection replacements use the same visibility rules as lix_as_of.
pub(super) async fn historical_path_index<S: StorageAdapterRead + Clone>(
    store: S,
    commit: &str,
    branch: &str,
) -> Result<Arc<crate::filesystem::FilesystemPathIndex>> {
    let descriptor = commit_state_descriptor(store.clone(), commit).await?;
    let mut tracked = TrackedStateContext::new().reader(store);
    let request = tracked_request(
        vec![
            FILE_DESCRIPTOR_SCHEMA_KEY.into(),
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into(),
        ],
        None,
        None,
        None,
    );
    let local = tracked
        .scan_batch_at_commit(commit, &request)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let base = if let Some(base) = descriptor.base_commit_id {
        vec![
            tracked
                .scan_batch_at_commit(&base.to_string(), &request)
                .await
                .map_err(lix_error_to_datafusion_error)?,
        ]
    } else {
        Vec::new()
    };
    let replacements = if descriptor.base_commit_id.is_some() {
        load_local_replacement_scopes(&mut tracked, commit).await?
    } else {
        Default::default()
    };
    let rows = tracked_to_hot(
        &[local],
        Vec::new(),
        &base,
        Vec::new(),
        branch,
        descriptor.global_scope,
        &replacements,
    )?;
    Ok(Arc::new(
        crate::filesystem::FilesystemPathIndex::from_live_batch(&rows)
            .map_err(lix_error_to_datafusion_error)?,
    ))
}
