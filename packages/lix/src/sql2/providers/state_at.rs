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
    MaterializedHotStateBatchBuilder, MaterializedHotStateRow,
};
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
    RowBatchProjection, RowPrimaryKeyFilterAnalyzer, catalog_schema_spec, row_pks_from_primary_key_filters,
};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[cfg(test)]
static STATE_AT_TRAVERSAL_PROBES: OnceLock<
    Mutex<
        std::collections::HashMap<(std::thread::ThreadId, String), Vec<(usize, usize)>>,
    >,
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
        "lix_state_at",
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
        formatter.debug_struct("StateAtFunction").finish_non_exhaustive()
    }
}

impl<S> TableFunctionImpl for StateAtFunction<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let [relation, commit_id] = args else {
            return Err(DataFusionError::Plan(
                "lix_state_at requires a relation and exactly one commit ID argument".into(),
            ));
        };
        let relation_name = text_argument(relation, 1, "relation name", None)?;
        let commit_id = text_argument(commit_id, 2, "commit ID", Some(&self.slots))?;
        let surface = self.catalog.surface(&relation_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_state_at does not support relation '{relation_name}'"
            ))
        })?;
        let schema = self.catalog.surface_schema(&relation_name).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_state_at does not support relation '{relation_name}'"
            ))
        })?;
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
                    "lix_state_at does not support relation '{relation_name}'"
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
            "lix_state_at argument {position} must be a {expected} literal or parameter"
        )));
    };
    value.try_as_str().flatten().map(str::to_owned).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "lix_state_at argument {position} must be a non-null text {expected}"
        ))
    })
}

#[derive(Clone)]
enum StateRelationKind {
    Schema { schema_key: String, spec: Arc<SchemaSurfaceSpec> },
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

#[async_trait]
impl<S> TableSpec for StateAtSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str { "lix_state_at" }
    fn schema(&self) -> SchemaRef { Arc::clone(&self.schema) }
    fn table_type(&self) -> TableType { TableType::View }

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
        let scan_limit = filters.is_empty().then_some(limit).flatten();
        let row_pks = match &self.kind {
            StateRelationKind::Schema { spec, .. } => row_pks_from_primary_key_filters(spec, filters)?,
            StateRelationKind::File | StateRelationKind::Directory => {
                match exact_string_column_constraint_from_filters(filters, "id")? {
                    FileIdConstraint::All => None,
                    FileIdConstraint::None => Some(Vec::new()),
                    FileIdConstraint::Ids(ids) => Some(ids.iter().map(|id| uuid_row_pk(id)).collect::<Result<Vec<_>>>()?),
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
                (store, kind, schema, commit_id, root_commit_id, active_branch_id, blob_reader, row_pks, contradictory),
                move |(store, kind, schema, commit_id, root_commit_id, active_branch_id, blob_reader, row_pks, contradictory)| async move {
                    if root_commit_id.as_deref() == Some(commit_id.as_str()) {
                        return Ok(RecordBatch::new_empty(schema));
                    }
                    let commit_is_global = commit_root_is_global(store.clone(), &commit_id).await?;
                    let mut tracked = TrackedStateContext::new().reader(store.clone());
                    if contradictory {
                        return Ok(RecordBatch::new_empty(schema));
                    }
                    let (batches, ancestor_rows) = match &kind {
                        StateRelationKind::Schema { schema_key, .. } => {
                            if let Some(row_pks) = row_pks.as_ref() {
                                let rows = load_schema_points_at_commit(
                                    &mut tracked,
                                    &commit_id,
                                    schema_key,
                                    row_pks,
                                )
                                .await?;
                                (Vec::new(), rows)
                            } else {
                                let request = tracked_request(
                                    vec![schema_key.clone()],
                                    None,
                                    None,
                                    scan_limit,
                                );
                                record_state_at_traversal_probe(&commit_id, &request);
                                (
                                    vec![tracked.scan_batch_at_commit(&commit_id, &request).await],
                                    Vec::new(),
                                )
                            }
                        }
                        StateRelationKind::Directory => {
                            let request = tracked_request(
                                vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into()],
                                row_pks.clone(),
                                row_pks
                                    .as_ref()
                                    .map(|_| vec![crate::NullableKeyFilter::Null]),
                                None,
                            );
                            record_state_at_traversal_probe(&commit_id, &request);
                            let batch = tracked.scan_batch_at_commit(&commit_id, &request).await;
                            let ancestors = if row_pks.is_some() {
                                match &batch {
                                    Ok(batch) => load_ancestor_directories(&mut tracked, &commit_id, batch, "parent_id").await,
                                    Err(_) => Ok(Vec::new()),
                                }
                            } else {
                                Ok(Vec::new())
                            }?;
                            (vec![batch], ancestors)
                        }
                        StateRelationKind::File => {
                            let file_ids = row_pks.as_ref().map(|keys| keys.iter().filter_map(single_row_pk_string).map(crate::NullableKeyFilter::Value).collect());
                            let mut file_schema_keys = vec![FILE_DESCRIPTOR_SCHEMA_KEY.into()];
                            if ["content", "lixcol_change_id", "lixcol_updated_at"]
                                .iter()
                                .any(|column| schema.index_of(column).is_ok())
                            {
                                file_schema_keys.push(BLOB_REF_SCHEMA_KEY.into());
                            }
                            let file_request = tracked_request(
                                file_schema_keys,
                                None,
                                file_ids,
                                None,
                            );
                            record_state_at_traversal_probe(&commit_id, &file_request);
                            let file_batch = tracked
                                .scan_batch_at_commit(&commit_id, &file_request)
                                .await;
                            if row_pks.is_some() {
                                let ancestors = match &file_batch {
                                    Ok(batch) => load_ancestor_directories(&mut tracked, &commit_id, batch, "directory_id").await,
                                    Err(_) => Ok(Vec::new()),
                                }?;
                                (vec![file_batch], ancestors)
                            } else {
                                let directory_request = tracked_request(
                                    vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.into()],
                                    None,
                                    None,
                                    None,
                                );
                                record_state_at_traversal_probe(&commit_id, &directory_request);
                                (vec![file_batch, tracked.scan_batch_at_commit(
                                    &commit_id,
                                    &directory_request,
                                ).await], Vec::new())
                            }
                        }
                    };
                    let batches = batches.into_iter().collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(lix_error_to_datafusion_error)?;
                    let hot = tracked_to_hot(
                        &batches,
                        ancestor_rows,
                        &active_branch_id,
                        commit_is_global,
                    )?;
                    let mut result = match kind {
                        StateRelationKind::Schema { spec, .. } => {
                            let request = HotStateScanRequest { projection: HotStateProjection::default(), ..Default::default() };
                            super::schema::row_record_batch(&spec, schema, &hot, RowBatchProjection::for_request(&request))?
                        }
                        StateRelationKind::Directory => super::directory::lix_directory_record_batch(&schema, &hot)
                            .map_err(lix_error_to_datafusion_error)?,
                        StateRelationKind::File => super::file::lix_file_state_record_batch(
                            &schema,
                            &blob_reader,
                            schema.index_of("content").is_ok(),
                            hot.into_rows(),
                        ).await.map_err(lix_error_to_datafusion_error)?,
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

async fn load_schema_points_at_commit<S: StorageAdapterRead + Clone>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    commit_id: &str,
    schema_key: &str,
    row_pks: &[RowPk],
) -> Result<Vec<MaterializedTrackedStateRow>> {
    let commit_id_typed = CommitId::parse_lix(commit_id, "lix_state_at commit ID")
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
            include_tombstones: false,
            ..Default::default()
        },
        read_columns: TrackedStateReadColumns::default(),
        limit,
    }
}

async fn commit_root_is_global<S: StorageAdapterRead + Clone>(store: S, commit_id: &str) -> Result<bool> {
    let commit_id = CommitId::parse_lix(commit_id, "lix_state_at commit ID")
        .map_err(lix_error_to_datafusion_error)?;
    let manifest = crate::tracked_state::load_published_commit_state_topology(&store, commit_id)
        .await
        .map_err(lix_error_to_datafusion_error)?
        .ok_or_else(|| DataFusionError::Execution(format!("commit '{commit_id}' has no tracked-state authority")))?;
    Ok(manifest.global_scope())
}

fn uuid_row_pk(id: &str) -> Result<RowPk> {
    RowPk::from_json_values(
        &[serde_json::Value::String(id.to_owned())],
        &[RowPkComponentType::Uuid],
    ).map_err(|error| DataFusionError::Plan(format!("invalid lix_state_at id: {error}")))
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

async fn load_ancestor_directories<S: StorageAdapterRead>(
    tracked: &mut crate::tracked_state::TrackedStateStoreReader<S>,
    commit_id: &str,
    initial: &MaterializedTrackedStateBatch,
    parent_field: &str,
) -> Result<Vec<MaterializedTrackedStateRow>> {
    let mut pending = std::collections::BTreeSet::new();
    for row in initial.iter() {
        if let Some(parent) =
            snapshot_text(row.decoded_snapshot(), row.snapshot_content(), parent_field)?
        {
            pending.insert(parent);
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut ancestors = Vec::new();
    let load_budget = crate::transaction::MAX_DIRECTORY_PARENT_DEPTH
        + usize::from(parent_field == "directory_id");
    for _ in 0..load_budget {
        let ids = pending.difference(&seen).cloned().collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(ancestors);
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
        let loaded = tracked
            .load_projected_batch_at_commit(commit_id, &keys, &ChangeRecordProjection::full())
            .await
            .map_err(lix_error_to_datafusion_error)?;
        pending.clear();
        for row in loaded.into_rows().into_iter().flatten().filter(|row| !row.deleted) {
            if let Some(parent) = snapshot_text(
                row.decoded_snapshot.as_ref(),
                row.snapshot_content.as_ref(),
                "parent_id",
            )? {
                pending.insert(parent);
            }
            ancestors.push(row);
        }
    }
    if pending.difference(&seen).next().is_none() {
        Ok(ancestors)
    } else {
        Err(DataFusionError::Execution(format!(
            "lix_state_at directory tree exceeds {} levels",
            crate::transaction::MAX_DIRECTORY_PARENT_DEPTH
        )))
    }
}

fn snapshot_text(
    decoded: Option<&Arc<crate::plugin::runtime::WasmTypedRow>>,
    raw: Option<&crate::common::SharedStr>,
    field: &str,
) -> Result<Option<String>> {
    if let Some(decoded) = decoded {
        return match decoded.row.get(field) {
            None | Some(lix_schema::Value::Null) => Ok(None),
            Some(lix_schema::Value::Text(value)) => Ok(Some(value.clone())),
            Some(lix_schema::Value::Uuid(value)) => Ok(Some(value.to_string())),
            _ => Err(DataFusionError::Execution(format!(
                "lix_state_at field '{field}' is not text"
            ))),
        };
    }
    let Some(raw) = raw else { return Ok(None) };
    let value: serde_json::Value = serde_json::from_str(raw.as_str()).map_err(|error| {
        DataFusionError::Execution(format!("invalid historical filesystem descriptor: {error}"))
    })?;
    Ok(value.get(field).and_then(serde_json::Value::as_str).map(str::to_owned))
}

fn tracked_to_hot(
    batches: &[MaterializedTrackedStateBatch],
    extra_rows: Vec<MaterializedTrackedStateRow>,
    branch_id: &str,
    commit_is_global: bool,
) -> Result<MaterializedHotStateBatch> {
    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(
        batches.iter().map(MaterializedTrackedStateBatch::len).sum::<usize>() + extra_rows.len(),
    );
    for row in batches.iter().flat_map(MaterializedTrackedStateBatch::iter).filter(|row| !row.deleted()) {
        let ordinal = builder.len();
        builder.push_owned(MaterializedHotStateRow {
            row_pk: row.row_pk().clone(),
            schema_key: row.schema_key().to_owned(),
            file_id: row.file_id().map(str::to_owned),
            snapshot_content: row.snapshot_content().cloned(),
            metadata: row.metadata().cloned(),
            deleted: false,
            created_at: row.created_at(),
            updated_at: row.updated_at(),
            global: commit_is_global,
            change_id: Some(row.change_id()),
            commit_id: Some(row.commit_id()),
            untracked: false,
            branch_id: Arc::from(branch_id),
        });
        builder.set_decoded_snapshot(ordinal, row.decoded_snapshot().cloned());
    }
    for row in extra_rows.into_iter().filter(|row| !row.deleted) {
        let ordinal = builder.len();
        let created_at = LixTimestamp::parse(&row.created_at)
            .map_err(|error| DataFusionError::Execution(format!("invalid created_at: {error}")))?;
        let updated_at = LixTimestamp::parse(&row.updated_at)
            .map_err(|error| DataFusionError::Execution(format!("invalid updated_at: {error}")))?;
        let decoded_snapshot = row.decoded_snapshot.clone();
        builder.push_owned(MaterializedHotStateRow {
            row_pk: row.row_pk,
            global: commit_is_global,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            deleted: false,
            created_at,
            updated_at,
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            branch_id: Arc::from(branch_id),
        });
        builder.set_decoded_snapshot(ordinal, decoded_snapshot);
    }
    Ok(builder.finish())
}
