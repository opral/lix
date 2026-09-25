mod interest;
mod paths;
pub(crate) use interest::prepare_native_diff_interest;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::create_physical_expr;
use serde_json::Value as JsonValue;

use crate::NullableKeyFilter;
use crate::branch::BranchHeadControlContext;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::hot_state::TrackedHeadContext;
use crate::row_payload::TypedRow as WasmTypedRow;
use crate::row_pk::{RowPk, RowPkComponent, RowPkComponentType};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::catalog::schema_surface::SchemaSurfaceSpec;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind};
use crate::sql2::error::{datafusion_error_to_lix_error, lix_error_to_datafusion_error};
#[cfg(test)]
use crate::sql2::result_metadata::field_is_row_ref;
use crate::sql2::result_metadata::{field_is_json, row_ref_field};
use crate::sql2::udfs::{ExecutionSlots, execution_slots};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    MaterializedTrackedStateExactBatch, MaterializedTrackedStateRowRef, TrackedStateContext,
    TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffIdentity, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStateFilter, TrackedStateKey,
    TrackedStatePayloadBatch, TrackedStateReadColumns, TrackedStateScanRequest,
    TrackedStateStoreReader,
};

use super::file::{FileIdConstraint, exact_string_column_constraint_from_filters};
use super::spec::{PlannedScan, SpecTableProvider, TableSpec, projected_schema, scan_row_source};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

pub(super) fn register_diff_function<S>(
    session: &datafusion::prelude::SessionContext,
    query_source: SqlChangelogQuerySource<S>,
    catalog: Arc<PublicCatalog>,
    read_interests: Option<Arc<crate::hot_state::ReadInterestRegistry>>,
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
    path_cache: Option<Arc<crate::filesystem::HistoricalPathIndexCache>>,
) where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    session.register_udtf(
        "lix_diff",
        Arc::new(DiffFunction {
            path_cache,
            store: query_source.store,
            read_interests,
            catalog,
            slots: execution_slots(session),
            blob_reader,
        }),
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DiffMode {
    General,
    WorkingHot,
}

struct DiffFunction<S> {
    path_cache: Option<Arc<crate::filesystem::HistoricalPathIndexCache>>,
    store: S,
    read_interests: Option<Arc<crate::hot_state::ReadInterestRegistry>>,
    catalog: Arc<PublicCatalog>,
    slots: Arc<ExecutionSlots>,
    blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
}

impl<S> fmt::Debug for DiffFunction<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiffFunction")
            .finish_non_exhaustive()
    }
}

impl<S> TableFunctionImpl for DiffFunction<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (mode, relation, from_commit_id, to_commit_id) = match args {
            [relation] => (
                DiffMode::WorkingHot,
                relation,
                self.slots
                    .working_diff_checkpoint_commit_id()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "lix_diff default range requires an active checkpoint".to_string(),
                        )
                    })?,
                self.slots.active_branch_commit_id().ok_or_else(|| {
                    DataFusionError::Plan(
                        "lix_diff default range requires an active branch head".to_string(),
                    )
                })?,
            ),
            [relation, from_commit_id, to_commit_id] => (
                DiffMode::General,
                relation,
                text_argument(from_commit_id, 2, "commit ID", Some(&self.slots))?,
                text_argument(to_commit_id, 3, "commit ID", Some(&self.slots))?,
            ),
            _ => {
                return Err(DataFusionError::Plan(
                    "lix_diff requires a relation and either zero or two commit ID arguments"
                        .to_string(),
                ));
            }
        };
        use crate::hot_state::DiffInterestEndpoint;
        let endpoint = |argument: &Expr, resolved: &String| {
            if let Expr::ScalarFunction(function) = argument
                && function.args.is_empty()
                && function.func.name() == "lix_active_branch_commit_id"
            {
                DiffInterestEndpoint::ActiveHead
            } else {
                DiffInterestEndpoint::Fixed(resolved.clone())
            }
        };
        let interest_endpoints = if mode == DiffMode::WorkingHot {
            (
                DiffInterestEndpoint::WorkingCheckpoint,
                DiffInterestEndpoint::ActiveHead,
            )
        } else {
            (
                endpoint(&args[1], &from_commit_id),
                endpoint(&args[2], &to_commit_id),
            )
        };
        let relation_name = text_argument(relation, 1, "relation name", None)?;
        let relation = DiffRelation::from_catalog(&self.catalog, &relation_name)?;
        Ok(Arc::new(SpecTableProvider::new(Arc::new(DiffSpec {
            path_cache: self.path_cache.clone(),
            blob_reader: Arc::clone(&self.blob_reader),
            store: self.store.clone(),
            read_interests: self.read_interests.clone(),
            interest_endpoints: Some(interest_endpoints),
            relation,
            from_commit_id,
            to_commit_id,
            active_branch_id: self.slots.active_branch_id(),
            mode,
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
            "lix_diff argument {position} must be a {expected} literal or parameter"
        )));
    };
    value
        .try_as_str()
        .flatten()
        .map(ToString::to_string)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "lix_diff argument {position} must be a non-null text {expected}"
            ))
        })
}

#[derive(Clone)]
pub(super) struct DiffRelation {
    name: String,
    kind: DiffRelationKind,
    pub(super) schema: SchemaRef,
    primary_key_columns: Vec<String>,
    schema_spec: Option<SchemaSurfaceSpec>,
}

#[derive(Clone, PartialEq, Eq)]
enum DiffRelationKind {
    Schema { schema_key: String },
    File,
    Directory,
}

impl DiffRelation {
    pub(super) fn from_catalog(catalog: &PublicCatalog, name: &str) -> Result<Self> {
        let surface = catalog.surface(name).ok_or_else(|| {
            DataFusionError::Plan(format!("lix_diff does not support relation '{name}'"))
        })?;
        let kind = match &surface.kind {
            PublicSurfaceKind::SchemaBase { schema_key } => DiffRelationKind::Schema {
                schema_key: schema_key.clone(),
            },
            PublicSurfaceKind::File => DiffRelationKind::File,
            PublicSurfaceKind::Directory => DiffRelationKind::Directory,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "lix_diff does not support relation '{name}'"
                )));
            }
        };
        let source_schema = catalog.surface_schema(name).ok_or_else(|| {
            DataFusionError::Plan(format!("lix_diff does not support relation '{name}'"))
        })?;
        let schema_spec = match &kind {
            DiffRelationKind::Schema { schema_key } => catalog.schema_spec(schema_key).cloned(),
            DiffRelationKind::File | DiffRelationKind::Directory => None,
        };
        let primary_key_columns = match &kind {
            DiffRelationKind::File | DiffRelationKind::Directory => vec!["id".to_owned()],
            DiffRelationKind::Schema { schema_key } => catalog
                .schema_spec(schema_key)
                .map(|spec| {
                    spec.primary_key_paths
                        .iter()
                        .map(|path| match path.as_slice() {
                            [column] => Ok(column.clone()),
                            _ => Err(DataFusionError::Plan(format!(
                                "lix_diff relation '{name}' has a non-column primary-key path"
                            ))),
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?
                .unwrap_or_default(),
        };
        let mut fields = vec![row_ref_field("row_ref", false)];
        for column in &primary_key_columns {
            let field = source_schema.field_with_name(column).map_err(|error| {
                DataFusionError::Plan(format!(
                    "lix_diff relation '{name}' is missing primary-key column '{column}': {error}"
                ))
            })?;
            fields.push(
                Field::new(column, field.data_type().clone(), false)
                    .with_metadata(field.metadata().clone()),
            );
        }
        fields.push(Field::new("diff_type", DataType::Utf8, false));
        for column in surface
            .columns
            .iter()
            .filter(|column| column.is_public() && !primary_key_columns.contains(&column.name))
        {
            let field = source_schema
                .field_with_name(&column.name)
                .map_err(|error| {
                    DataFusionError::Plan(format!(
                        "lix_diff relation '{name}' is missing column '{}': {error}",
                        column.name
                    ))
                })?;
            for side in ["from", "to"] {
                fields.push(
                    Field::new(
                        format!("{side}_{}", column.name),
                        field.data_type().clone(),
                        true,
                    )
                    .with_metadata(field.metadata().clone()),
                );
            }
        }
        fields.push(Field::new("lixcol_from_commit_id", DataType::Utf8, false));
        fields.push(Field::new("lixcol_to_commit_id", DataType::Utf8, false));
        Ok(Self {
            name: name.to_owned(),
            kind,
            schema: Arc::new(Schema::new(fields)),
            primary_key_columns,
            schema_spec,
        })
    }
}

pub(super) struct DiffSpec<S> {
    pub(super) path_cache: Option<Arc<crate::filesystem::HistoricalPathIndexCache>>,
    pub(super) blob_reader: Arc<dyn crate::binary_cas::BlobDataReader>,
    pub(super) store: S,
    pub(super) read_interests: Option<Arc<crate::hot_state::ReadInterestRegistry>>,
    pub(super) interest_endpoints: Option<(
        crate::hot_state::DiffInterestEndpoint,
        crate::hot_state::DiffInterestEndpoint,
    )>,
    pub(super) relation: DiffRelation,
    pub(super) from_commit_id: String,
    pub(super) to_commit_id: String,
    pub(super) active_branch_id: Option<String>,
    pub(super) mode: DiffMode,
}

fn diff_filter_schema(
    relation: &DiffRelation,
    schema: &SchemaRef,
    filters: &[Expr],
) -> Result<SchemaRef> {
    let mut fields = schema.fields().to_vec();
    for filter in filters {
        for column in filter.column_refs() {
            if !fields.iter().any(|field| field.name() == &column.name) {
                fields.push(Arc::new(
                    relation.schema.field_with_name(&column.name)?.clone(),
                ));
            }
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

impl<S: StorageAdapterRead + Clone + Send + Sync + 'static> DiffSpec<S> {
    /// Prepare only native inputs. Never evaluate SQL expressions or user functions
    /// while looking ahead beyond the first required history miss.
    pub(super) async fn prepare_history_inputs(
        &self,
        from: &str,
        to: &str,
        projection: &Vec<usize>,
        filters: &[Expr],
    ) -> Result<(), crate::LixError> {
        let schema = projected_schema(&self.relation.schema, Some(projection));
        let metadata_filters = if self.relation.kind == DiffRelationKind::File {
            filter_conjuncts(filters)
                .into_iter()
                .filter(|filter| file_metadata_filter(filter, &self.relation.schema))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let schema = diff_filter_schema(&self.relation, &schema, &metadata_filters)
            .map_err(datafusion_error_to_lix_error)?;
        let route = DiffRoute::from_filters(filters, &self.relation, &schema);
        if route.contradictory {
            return Ok(());
        }
        prepare_native_diff_interest(
            self.store.clone(),
            &self.relation.name,
            from,
            to,
            &route.request,
            &schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        )
        .await
    }
}

#[async_trait]
impl<S> TableSpec for DiffSpec<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    fn table_name(&self) -> &str {
        "lix_diff"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.relation.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn filter_pushdown(&self, filter: &Expr) -> TableProviderFilterPushDown {
        if self.relation.kind == DiffRelationKind::File
            && file_metadata_filter(filter, &self.relation.schema)
        {
            return TableProviderFilterPushDown::Inexact;
        }
        if filter.column_refs().iter().any(|column| {
            column.name == "row_ref"
                || self.relation.primary_key_columns.contains(&column.name)
                || matches!(
                    column.name.as_str(),
                    "from_lixcol_file_id" | "to_lixcol_file_id"
                )
        }) {
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
        let schema = projected_schema(&self.relation.schema, projection);
        // Evaluate pushed metadata predicates on descriptor rows before opening any
        // historical file bytes. Content predicates necessarily remain residual.
        let metadata_filters = if self.relation.kind == DiffRelationKind::File {
            filter_conjuncts(filters)
                .into_iter()
                .filter(|filter| file_metadata_filter(filter, &self.relation.schema))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let filter_schema = diff_filter_schema(&self.relation, &schema, &metadata_filters)?;
        let df_schema = DFSchema::try_from(filter_schema.as_ref().clone())?;
        let metadata_filters = metadata_filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, props, &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default()))
            .collect::<Result<Vec<_>>>()?;
        let route = DiffRoute::from_filters(filters, &self.relation, &filter_schema);
        let paths = (self.mode == DiffMode::General
            && self.relation.kind == DiffRelationKind::File
            && route.request.filter.file_ids.is_empty())
        .then(|| paths::PathRoute::from_filters(filters))
        .flatten();
        if let (Some(registry), Some((from, to))) = (&self.read_interests, &self.interest_endpoints)
        {
            registry
                .register(crate::hot_state::LogicalReadInterest::Diff {
                    branch_id: self.active_branch_id.clone(),
                    relation: self.relation.name.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    filter: route.request.filter.clone(),
                    retain_payloads: route.request.retain_payloads,
                    projected_columns: filter_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                    limit,
                })
                .map_err(lix_error_to_datafusion_error)?;
        }
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.store.clone(),
                    paths,
                    self.path_cache.clone(),
                    self.relation.clone(),
                    schema,
                    route,
                    self.from_commit_id.clone(),
                    self.to_commit_id.clone(),
                    self.active_branch_id.clone(),
                    self.mode,
                    self.blob_reader.clone(),
                    filter_schema,
                    metadata_filters,
                ),
                move |(
                    store,
                    paths,
                    path_cache,
                    relation,
                    schema,
                    mut route,
                    from_commit_id,
                    to_commit_id,
                    active_branch_id,
                    mode,
                    blob_reader,
                    filter_schema,
                    metadata_filters,
                )| async move {
                    if limit == Some(0) || route.contradictory || from_commit_id == to_commit_id {
                        return diff_record_batch(
                            schema,
                            &[],
                            &relation,
                            &from_commit_id,
                            &to_commit_id,
                        );
                    }
                    let mut tracked = TrackedStateContext::new().reader(store.clone());
                    // A pinned base that is identical at both endpoints cannot
                    // contribute any effective changes. In that common
                    // checkpoint-to-head case, retain HOT_DIFF as the sparse
                    // candidate index and still resolve the final winners
                    // through the composite overlay below.
                    let needs_global_provenance = filter_schema.fields().iter().any(|field| {
                        matches!(
                            field.name().as_str(),
                            "from_lixcol_global" | "to_lixcol_global"
                        )
                    });
                    let working_needs_endpoint_descriptors = match &relation.kind {
                        DiffRelationKind::File => filter_schema
                            .fields()
                            .iter()
                            .any(|field| side_column(field.name()).is_some()),
                        DiffRelationKind::Schema { .. } => true,
                        DiffRelationKind::Directory => true,
                    };
                    let generic_descriptors = if mode == DiffMode::General {
                        Some((
                            commit_state_descriptor(&store, &from_commit_id).await?,
                            commit_state_descriptor(&store, &to_commit_id).await?,
                        ))
                    } else {
                        None
                    };
                    if let Some(paths) = paths {
                        let ids = paths
                            .resolve(
                                store.clone(),
                                &from_commit_id,
                                &to_commit_id,
                                active_branch_id
                                    .as_deref()
                                    .unwrap_or(crate::GLOBAL_BRANCH_ID),
                                path_cache.as_deref(),
                            )
                            .await?;
                        let mut ids = ids.into_iter().collect::<Vec<_>>();
                        if !route.request.filter.file_ids.is_empty() {
                            ids.retain(|id| {
                                route
                                    .request
                                    .filter
                                    .file_ids
                                    .contains(&NullableKeyFilter::Value(id.clone()))
                            });
                        }
                        if ids.is_empty() {
                            return diff_record_batch(
                                schema,
                                &[],
                                &relation,
                                &from_commit_id,
                                &to_commit_id,
                            );
                        }
                        route.request.filter.file_ids =
                            ids.into_iter().map(NullableKeyFilter::Value).collect();
                        route.request.filter.file_ids.push(NullableKeyFilter::Null);
                    }
                    // Global provenance must come from the composite overlay
                    // resolution: the HOT epoch diff carries effective rows
                    // but not which side an inherited global row supplied, so
                    // provenance projections take the cold route.
                    let mut root_backed_working = false;
                    let direct_candidates = if mode == DiffMode::WorkingHot {
                        let branch_id = active_branch_id.as_deref().ok_or_else(|| {
                            hot_only_diff_error(DataFusionError::Execution(
                                "lix_diff default range requires an active branch".to_string(),
                            ))
                        })?;
                        let from_commit =
                            CommitId::parse_lix(&from_commit_id, "lix_diff checkpoint commit ID")
                                .map_err(lix_error_to_datafusion_error)?;
                        let to_commit =
                            CommitId::parse_lix(&to_commit_id, "lix_diff head commit ID")
                                .map_err(lix_error_to_datafusion_error)?;
                        let control = BranchHeadControlContext::new()
                            .reader(store.clone())
                            .load(branch_id)
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                            .ok_or_else(|| {
                                hot_only_diff_error(DataFusionError::Execution(format!(
                                    "lix_diff default range has no HOT control for branch '{branch_id}'"
                                )))
                            })?;
                        if control.head_commit_id != to_commit
                            || control.working_diff_checkpoint_commit_id != Some(from_commit)
                        {
                            return Err(hot_only_diff_error(DataFusionError::Execution(
                                "lix_diff default-range coordinates no longer match the certified HOT epoch"
                                    .to_string(),
                            )));
                        }
                        let tracked_head = TrackedHeadContext::new().reader(store.clone());
                        root_backed_working = tracked_head
                            .root_current_base_commit(branch_id, control.tracked_generation)
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                            .is_some();
                        if root_backed_working {
                            // The local index contains only overlay edits, not
                            // dirty rows inherited from the canonical base.
                            // Preserve the routed schema/file/primary-key scope
                            // and let native missing-input diagnostics hydrate it.
                            None
                        } else {
                            Some(
                                tracked_head
                                    .working_diff_for_control(branch_id, control, &route.request)
                                    .await
                                    .map_err(lix_error_to_datafusion_error)
                                    .map_err(hot_only_diff_error)?
                                    .ok_or_else(|| {
                                        hot_only_diff_error(DataFusionError::Execution(
                                            "lix_diff certified HOT index is unavailable"
                                                .to_string(),
                                        ))
                                    })?
                                    .diff,
                            )
                        }
                    } else if !route.request.retain_payloads
                        && !needs_global_provenance
                        && generic_descriptors.as_ref().is_some_and(
                            |(from_descriptor, to_descriptor)| {
                                from_descriptor.base_commit_id == to_descriptor.base_commit_id
                            },
                        )
                        && let Some(branch_id) = active_branch_id.as_deref()
                        && let (Ok(from_commit), Ok(to_commit)) = (
                            CommitId::parse(&from_commit_id),
                            CommitId::parse(&to_commit_id),
                        )
                    {
                        match BranchHeadControlContext::new()
                            .reader(store.clone())
                            .load(branch_id)
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                        {
                            Some(control)
                                if control.head_commit_id == to_commit
                                    && control.working_diff_checkpoint_commit_id
                                        == Some(from_commit) =>
                            {
                                TrackedHeadContext::new()
                                    .reader(store.clone())
                                    .working_diff_for_control(branch_id, control, &route.request)
                                    .await
                                    .map_err(lix_error_to_datafusion_error)?
                                    .map(|working| working.diff)
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    // Full replicas require the certified local HOT index.
                    // Root-backed replicas instead resolve canonical endpoint
                    // inputs within the original native request scope.
                    let (from_descriptor, to_descriptor) =
                        if let Some(descriptors) = generic_descriptors {
                            descriptors
                        } else if root_backed_working
                            || working_needs_endpoint_descriptors
                            || needs_global_provenance
                            || direct_candidates.as_ref().is_some_and(|diff| {
                                diff.entries.iter().any(|entry| {
                                    entry.identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                                })
                            })
                        {
                            let descriptor_result = async {
                                Ok::<_, DataFusionError>((
                                    commit_state_descriptor(&store, &from_commit_id).await?,
                                    commit_state_descriptor(&store, &to_commit_id).await?,
                                ))
                            }
                            .await;
                            if root_backed_working {
                                descriptor_result?
                            } else {
                                descriptor_result.map_err(hot_only_diff_error)?
                            }
                        } else {
                            (
                                CommitStateDescriptor::default(),
                                CommitStateDescriptor::default(),
                            )
                        };
                    let effective_result = effective_diff(
                        &mut tracked,
                        &from_commit_id,
                        &to_commit_id,
                        &from_descriptor,
                        &to_descriptor,
                        &route.request,
                        direct_candidates,
                        needs_global_provenance,
                    )
                    .await;
                    let (diff, from_global_rows, to_global_rows) =
                        if mode == DiffMode::WorkingHot && !root_backed_working {
                            effective_result.map_err(hot_only_diff_error)?
                        } else {
                            effective_result?
                        };
                    if route.request.retain_payloads {
                        diff.validate_live_payloads()
                            .map_err(lix_error_to_datafusion_error)?;
                    }
                    let (from_global_rows, to_global_rows) = if needs_global_provenance {
                        (from_global_rows, to_global_rows)
                    } else {
                        (HashSet::new(), HashSet::new())
                    };
                    let mut rows = match &relation.kind {
                        DiffRelationKind::Schema { schema_key } => schema_diff_rows(
                            diff,
                            schema_key,
                            &filter_schema,
                            &from_global_rows,
                            &to_global_rows,
                        )?,
                        DiffRelationKind::File => {
                            let result = file_diff_rows(
                                &mut tracked,
                                diff,
                                &route.request.filter.file_ids,
                                &filter_schema,
                                &from_commit_id,
                                &to_commit_id,
                                &from_descriptor,
                                &to_descriptor,
                            )
                            .await;
                            if mode == DiffMode::WorkingHot && !root_backed_working {
                                result.map_err(hot_only_diff_error)?
                            } else {
                                result?
                            }
                        }
                        DiffRelationKind::Directory => {
                            let result = directory_diff_rows(
                                &mut tracked,
                                diff,
                                &route.request.filter.row_pks,
                                &filter_schema,
                                &from_commit_id,
                                &to_commit_id,
                                &from_descriptor,
                                &to_descriptor,
                                &from_global_rows,
                                &to_global_rows,
                            )
                            .await;
                            if mode == DiffMode::WorkingHot && !root_backed_working {
                                result.map_err(hot_only_diff_error)?
                            } else {
                                result?
                            }
                        }
                    };
                    // Independent storage filters can overfetch a Cartesian
                    // superset. Preserve correlation before limits or output.
                    if let Some(refs) = &route.row_refs {
                        rows.retain(|row| refs.contains(&(row.file_id.clone(), row.row_pk.clone())));
                    }
                    if !metadata_filters.is_empty() {
                        let metadata = diff_record_batch(
                            filter_schema,
                            &rows,
                            &relation,
                            &from_commit_id,
                            &to_commit_id,
                        )?;
                        let mut selected = vec![true; rows.len()];
                        for filter in &metadata_filters {
                            let values = filter.evaluate(&metadata)?.into_array(rows.len())?;
                            let mask = values.as_any().downcast_ref::<BooleanArray>().ok_or_else(
                                || {
                                    DataFusionError::Internal(
                                        "file metadata filter must be boolean".into(),
                                    )
                                },
                            )?;
                            for (index, keep) in selected.iter_mut().enumerate() {
                                *keep &= !mask.is_null(index) && mask.value(index);
                            }
                        }
                        let mut index = 0;
                        rows.retain(|_| {
                            let keep = selected[index];
                            index += 1;
                            keep
                        });
                    }
                    if let Some(limit) = limit {
                        rows.truncate(limit);
                    }
                    let batch = diff_record_batch(
                        schema.clone(),
                        &rows,
                        &relation,
                        &from_commit_id,
                        &to_commit_id,
                    )?;
                    if relation.kind != DiffRelationKind::File
                        || !schema.fields().iter().any(|field| {
                            matches!(field.name().as_str(), "from_content" | "to_content")
                        })
                    {
                        return Ok(batch);
                    }
                    let mut columns = batch.columns().to_vec();
                    for (name, commit, after) in [
                        ("from_content", &from_commit_id, false),
                        ("to_content", &to_commit_id, true),
                    ] {
                        let Ok(index) = schema.index_of(name) else {
                            continue;
                        };
                        let ids = rows
                            .iter()
                            .filter_map(|row| {
                                let side = if after { &row.to } else { &row.from };
                                side.as_ref().and_then(|side| side.id.clone())
                            })
                            .collect::<Vec<_>>();
                        let content = super::state_at::diff_file_content(
                            store.clone(),
                            blob_reader.clone(),
                            commit,
                            active_branch_id
                                .as_deref()
                                .unwrap_or(crate::GLOBAL_BRANCH_ID),
                            &ids,
                        )
                        .await?;
                        let values = rows
                            .iter()
                            .map(|row| {
                                let side = if after { &row.to } else { &row.from };
                                let Some(side) = side else {
                                    return Ok(None);
                                };
                                let id = side.id.as_ref().ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "file diff side lacks identity".into(),
                                    )
                                })?;
                                content
                                    .get(id)
                                    .map(|bytes| Some(bytes.as_slice()))
                                    .ok_or_else(|| {
                                        DataFusionError::Execution(format!(
                                            "historical content missing for file {id}"
                                        ))
                                    })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        columns[index] = Arc::new(LargeBinaryArray::from(values));
                    }
                    Ok(RecordBatch::try_new(schema, columns)?)
                },
            ),
        })
    }
}

/// Only columns whose values are available without materializing file bytes.
fn file_metadata_filter(filter: &Expr, schema: &SchemaRef) -> bool {
    !filter.is_volatile()
        && filter.column_refs().iter().all(|column| {
            !matches!(column.name.as_str(), "from_content" | "to_content")
                && schema.index_of(&column.name).is_ok()
        })
}

fn filter_conjuncts(filters: &[Expr]) -> Vec<Expr> {
    fn append(expression: &Expr, conjuncts: &mut Vec<Expr>) {
        match expression {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                append(&binary.left, conjuncts);
                append(&binary.right, conjuncts);
            }
            _ => conjuncts.push(expression.clone()),
        }
    }

    let mut conjuncts = Vec::new();
    for filter in filters {
        append(filter, &mut conjuncts);
    }
    conjuncts
}

#[derive(Clone, Debug)]
struct DiffRoute {
    row_refs: Option<BTreeSet<(Option<String>, RowPk)>>,
    request: TrackedStateDiffRequest,
    contradictory: bool,
}

impl DiffRoute {
    fn from_filters(filters: &[Expr], relation: &DiffRelation, projection: &Schema) -> Self {
        let conjuncts = filter_conjuncts(filters);
        let row_ref_values = optional_values(&conjuncts, "row_ref");
        let id_values = optional_values(&conjuncts, "id");
        let typed_row_pks = relation.schema_spec.as_ref().and_then(|spec| {
            super::schema::row_pks_from_primary_key_filters(spec, filters)
                .ok()
                .flatten()
        });
        let explicit_row_filter = row_ref_values.is_some() || typed_row_pks.is_some();
        let mut contradictory = row_ref_values.as_ref().is_some_and(Vec::is_empty)
            || id_values.as_ref().is_some_and(Vec::is_empty)
            || typed_row_pks.as_ref().is_some_and(Vec::is_empty);
        let row_refs = row_ref_values.map(|values| {
            values
                .into_iter()
                .filter_map(|value| {
                    let resolved = crate::row_ref::decode_str(&value).ok()?;
                    if resolved.relation != relation.name {
                        return None;
                    }
                    // The public filesystem relations have a UUID primary
                    // key. The generic row-ref codec deliberately does not
                    // know the catalog, so reject a canonical-but-wrongly
                    // typed filesystem address before path expansion can
                    // feed it into uuid_row_pk and turn a no-match into an
                    // execution error.
                    if matches!(
                        &relation.kind,
                        DiffRelationKind::File | DiffRelationKind::Directory
                    ) && !matches!(
                        resolved.row_pk.components.as_slice(),
                        [RowPkComponent::Uuid(_)]
                    ) {
                        return None;
                    }
                    Some((resolved.file_id, resolved.row_pk))
                })
                .collect::<BTreeSet<_>>()
        });
        contradictory |= row_refs.as_ref().is_some_and(BTreeSet::is_empty);
        let row_ref_pks = row_refs
            .iter()
            .flatten()
            .map(|(_, pk)| pk.clone())
            .collect::<Vec<_>>();
        let mut row_pks = match typed_row_pks {
            Some(typed) if !row_ref_pks.is_empty() => typed
                .into_iter()
                .filter(|row_pk| row_ref_pks.contains(row_pk))
                .collect(),
            Some(typed) => typed,
            None => row_ref_pks,
        };
        contradictory |= explicit_row_filter && row_pks.is_empty();
        let mut schema_keys = Vec::new();
        let mut file_ids = Vec::new();
        match &relation.kind {
            DiffRelationKind::Schema { schema_key } => {
                schema_keys.push(schema_key.clone());
                if let Some(refs) = &row_refs {
                    file_ids.extend(refs.iter().map(|(file_id, _)| match file_id {
                        Some(id) => NullableKeyFilter::Value(id.clone()),
                        None => NullableKeyFilter::Null,
                    }));
                    file_ids.dedup();
                }
                if let Some(ids) = optional_values(&conjuncts, "from_lixcol_file_id")
                    .or_else(|| optional_values(&conjuncts, "to_lixcol_file_id"))
                {
                    file_ids.extend(ids.into_iter().map(NullableKeyFilter::Value));
                }
            }
            DiffRelationKind::File => {
                let mut ids = id_values.unwrap_or_default();
                for row_pk in row_pks.drain(..) {
                    match row_pk.as_single_string_owned() {
                        Ok(id) => ids.push(id),
                        Err(_) => contradictory = true,
                    }
                }
                file_ids.extend(ids.into_iter().map(NullableKeyFilter::Value));
                // Directory descriptors have a null file owner. Include their
                // sparse changes even for an exact file probe: ancestor moves
                // can change that file's path without touching its own rows.
                if !file_ids.is_empty() {
                    file_ids.push(NullableKeyFilter::Null);
                }
            }
            DiffRelationKind::Directory => {
                schema_keys.push(DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string());
                if let Some(ids) = id_values {
                    let ids = ids
                        .into_iter()
                        .filter_map(|id| uuid_row_pk(&id).ok())
                        .collect::<Vec<_>>();
                    if row_pks.is_empty() {
                        row_pks = ids;
                    } else {
                        row_pks.retain(|id| ids.contains(id));
                    }
                    contradictory |= row_pks.is_empty();
                }
            }
        }
        // File sides come from two batched descriptor point reads, never from
        // their potentially numerous changed content atoms. Keeping the tree
        // diff identity-only avoids hydrating every changed row merely to
        // render one file path or descriptor field.
        let retain_payloads = relation.kind != DiffRelationKind::File
            && projection
                .fields()
                .iter()
                .filter_map(|field| side_column(field.name()))
                .any(|(_, column)| {
                    !matches!(
                        column,
                        "id" | "lixcol_file_id"
                            | "lixcol_created_at"
                            | "lixcol_updated_at"
                            | "lixcol_change_id"
                            | "lixcol_author_id"
                            | "lixcol_commit_id"
                            | "lixcol_global"
                            | "lixcol_untracked"
                    )
                });
        Self {
            row_refs,
            request: TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys,
                    row_pks,
                    file_ids,
                    row_pk_lower: None,
                    row_pk_upper: None,
                    include_tombstones: true,
                },
                retain_payloads,
            },
            contradictory,
        }
    }
}

fn optional_values(conjuncts: &[Expr], column: &'static str) -> Option<Vec<String>> {
    match exact_string_column_constraint_from_filters(conjuncts, column) {
        Ok(FileIdConstraint::All) | Err(_) => None,
        Ok(FileIdConstraint::None) => Some(Vec::new()),
        Ok(FileIdConstraint::Ids(values)) => Some(values.into_iter().collect()),
    }
}

pub(crate) fn relation_diff_schema(catalog: &PublicCatalog, relation: &str) -> Result<SchemaRef> {
    DiffRelation::from_catalog(catalog, relation).map(|relation| relation.schema)
}

fn hot_only_diff_error(error: DataFusionError) -> DataFusionError {
    let error = datafusion_error_to_lix_error(error);
    // Pinned HOT file values may be represented by deferred binary-CAS chunks.
    // Fetching those exact content-addressed bytes is not history fallback, so
    // preserve the demand code for the replica retry loop. Coherent-read
    // expiration must also reach the bounded session retry unchanged.
    if matches!(
        error.code.as_str(),
        "LIX_SYNC_CHUNKS_REQUIRED"
            | crate::LixError::CODE_STORAGE_READ_EXPIRED
            | crate::LixError::CODE_TRANSACTION_CONFLICT
    ) {
        return lix_error_to_datafusion_error(error);
    }
    let message = if error.code == "LIX_SYNC_HISTORY_REQUIRED" {
        "lix_diff default range requires the certified local HOT checkpoint snapshot; canonical history fallback is forbidden"
            .to_string()
    } else {
        error.message
    };
    lix_error_to_datafusion_error(crate::LixError::new("LIX_DIFF_HOT_UNAVAILABLE", message))
}

#[derive(Clone)]
struct DiffSqlRow {
    row_pk: RowPk,
    file_id: Option<String>,
    diff_type: &'static str,

    from: Option<DiffSide>,
    to: Option<DiffSide>,
}

#[derive(Clone)]
struct DiffSide {
    id: Option<String>,
    global: bool,
    file_id: Option<String>,
    created_at: String,
    updated_at: String,
    change_id: String,
    author_id: String,
    commit_id: String,
    metadata: Option<JsonValue>,
    snapshot: Option<Arc<WasmTypedRow>>,
    path: Option<String>,
}

#[derive(Default)]
struct CommitStateDescriptor {
    base_commit_id: Option<CommitId>,
    global_scope: bool,
}

async fn commit_state_descriptor(
    store: &(impl StorageAdapterRead + Clone),
    commit_id: &str,
) -> Result<CommitStateDescriptor> {
    let commit_id = CommitId::parse_lix(commit_id, "lix_diff commit ID")
        .map_err(lix_error_to_datafusion_error)?;
    // Both immutable addresses are known before either read. Discover their
    // absence together so a partial replica can hydrate one bounded frontier.
    let mut graph = crate::commit_graph::CommitGraphContext::new().reader(store);
    let (topology, node) = futures_util::future::join(
        crate::tracked_state::load_published_commit_state_topology(store, commit_id),
        graph.load_node(&commit_id),
    )
    .await;
    // Corruption and other read failures remain errors, even if the other
    // input is absent. Missing data must never hide a damaged resident record.
    let topology = topology.map_err(lix_error_to_datafusion_error)?;
    let node = node.map_err(lix_error_to_datafusion_error)?;
    match (topology, node) {
        (Some(topology), Some(node)) => Ok(CommitStateDescriptor {
            base_commit_id: node.base_commit_id,
            global_scope: topology.global_scope(),
        }),
        (topology, node) => {
            use crate::tracked_state::NativeMetadataRef;
            let mut missing = Vec::with_capacity(2);
            if topology.is_none() {
                missing.push(NativeMetadataRef::CommitStateHeader(commit_id.to_string()));
            }
            if node.is_none() {
                missing.push(NativeMetadataRef::CommitGraphRecord(commit_id.to_string()));
            }
            Err(lix_error_to_datafusion_error(
                NativeMetadataRef::annotate_missing_batch(
                    missing,
                    crate::tracked_state::sync_history_required_for_commits(&[commit_id]),
                ),
            ))
        }
    }
}

async fn effective_diff<S: StorageAdapterRead>(
    tracked: &mut TrackedStateStoreReader<S>,
    from_commit_id: &str,
    to_commit_id: &str,
    from_descriptor: &CommitStateDescriptor,
    to_descriptor: &CommitStateDescriptor,
    request: &TrackedStateDiffRequest,
    local_candidates: Option<TrackedStateDiff>,
    needs_global_provenance: bool,
) -> Result<(
    TrackedStateDiff,
    HashSet<TrackedStateKey>,
    HashSet<TrackedStateKey>,
)> {
    let hot_candidates = local_candidates.is_some();
    let resolve_effective_winners = needs_global_provenance
        || from_descriptor.base_commit_id.is_some()
        || to_descriptor.base_commit_id.is_some();
    let local_candidates = match local_candidates {
        // HOT already owns both payloads when each side has a live local row.
        // A pinned base cannot override either winner. Keep these snapshot-local
        // payloads, including on sparse replicas whose authored owners are cold.
        Some(diff)
            if !resolve_effective_winners
                || (!needs_global_provenance
                    && diff.entries.iter().all(|entry| {
                        entry.before.as_ref().is_some_and(|row| !row.deleted)
                            && entry.after.as_ref().is_some_and(|row| !row.deleted)
                    })) =>
        {
            return Ok((diff, HashSet::new(), HashSet::new()));
        }
        Some(diff) => diff,
        None => tracked
            .diff_commits(from_commit_id, to_commit_id, request)
            .await
            .map_err(lix_error_to_datafusion_error)?,
    };
    let mut candidates = BTreeSet::new();
    extend_diff_keys(&mut candidates, &local_candidates);

    // HOT supplies the complete bounded candidate set. Resolve those exact
    // identities against endpoint overlays/bases below, without a history diff
    // or a scan of either base. General ranges also compare changing bases.
    let from_base = effective_base_source(from_commit_id, from_descriptor);
    let to_base = effective_base_source(to_commit_id, to_descriptor);
    if !hot_candidates
        && (from_descriptor.base_commit_id.is_some() || to_descriptor.base_commit_id.is_some())
        && from_base != to_base
    {
        let base_candidates = tracked
            .diff_commits(&from_base, &to_base, request)
            .await
            .map_err(lix_error_to_datafusion_error)?;
        extend_diff_keys(&mut candidates, &base_candidates);
    }
    if candidates.is_empty() {
        return Ok((TrackedStateDiff::default(), HashSet::new(), HashSet::new()));
    }

    let keys = candidates.into_iter().collect::<Vec<_>>();
    let projection = ChangeRecordProjection::full();
    let from_local = tracked
        .load_projected_batch_at_commit(from_commit_id, &keys, &projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let to_local = tracked
        .load_projected_batch_at_commit(to_commit_id, &keys, &projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let from_base_rows = load_base_rows(tracked, from_descriptor, &keys, &projection).await?;
    let to_base_rows = load_base_rows(tracked, to_descriptor, &keys, &projection).await?;
    let from_replacement_scopes =
        load_local_replacement_scopes_for_keys(tracked, from_commit_id, from_descriptor, &keys)
            .await?;
    let to_replacement_scopes =
        load_local_replacement_scopes_for_keys(tracked, to_commit_id, to_descriptor, &keys).await?;

    let identities = TrackedStateDiffIdentity::from_key_batch(keys.clone())
        .map_err(lix_error_to_datafusion_error)?;
    let mut entries = Vec::with_capacity(keys.len());
    let mut payloads = BTreeMap::new();
    let mut from_global_rows = HashSet::new();
    let mut to_global_rows = HashSet::new();
    for (index, (key, identity)) in keys.iter().zip(identities).enumerate() {
        let (before, before_global) = effective_row(
            from_local.row(index),
            from_base_rows.as_ref().and_then(|rows| rows.row(index)),
            from_descriptor.global_scope,
            base_key_suppressed(key, &from_replacement_scopes),
        );
        let (after, after_global) = effective_row(
            to_local.row(index),
            to_base_rows.as_ref().and_then(|rows| rows.row(index)),
            to_descriptor.global_scope,
            base_key_suppressed(key, &to_replacement_scopes),
        );
        let Some(kind) = classify_effective_rows(before, after) else {
            continue;
        };
        if before_global && before.is_some_and(|row| !row.deleted()) {
            from_global_rows.insert(key.clone());
        }
        if after_global && after.is_some_and(|row| !row.deleted()) {
            to_global_rows.insert(key.clone());
        }
        for row in [before, after].into_iter().flatten() {
            payloads.entry(row.change_id()).or_insert_with(|| {
                let snapshot = row
                    .decoded_snapshot()
                    .and_then(|snapshot| snapshot.durable_payload().ok())
                    .map(|payload| payload.to_vec());
                let metadata = row.metadata().and_then(|metadata| {
                    serde_json::from_str(metadata.as_str())
                        .ok()
                        .map(lix_schema::Jsonb::from_value)
                });
                (snapshot, metadata)
            });
        }
        entries.push(TrackedStateDiffEntry {
            identity: identity.clone(),
            kind,
            before: before.map(|row| diff_row(identity.clone(), row)),
            after: after.map(|row| diff_row(identity, row)),
        });
    }
    let payloads = TrackedStatePayloadBatch::from_payloads(
        payloads
            .into_iter()
            .map(|(change_id, (snapshot, metadata))| (change_id, snapshot, metadata)),
    )
    .map_err(lix_error_to_datafusion_error)?;
    Ok((
        TrackedStateDiff::from_entries_with_payloads(entries, payloads),
        from_global_rows,
        to_global_rows,
    ))
}

async fn load_local_replacement_scopes_for_keys<S: StorageAdapterRead>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    descriptor: &CommitStateDescriptor,
    keys: &[TrackedStateKey],
) -> Result<BTreeSet<(String, Option<String>)>> {
    if descriptor.base_commit_id.is_none() {
        return Ok(BTreeSet::new());
    }
    let scopes = keys
        .iter()
        .flat_map(|key| {
            std::iter::once((key.schema_key.clone(), None)).chain(
                key.file_id
                    .as_ref()
                    .map(|file_id| (key.schema_key.clone(), Some(file_id.clone()))),
            )
        })
        .collect::<BTreeSet<_>>();
    let marker_keys = scopes
        .iter()
        .map(|(schema_key, file_id)| TrackedStateKey {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            file_id: None,
            row_pk: RowPk::single(crate::collection_generation::collection_scope_key(
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )),
        })
        .collect::<Vec<_>>();
    let markers = tracked
        .load_projected_batch_at_commit(
            commit_id,
            &marker_keys,
            &ChangeRecordProjection::identity_only(),
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
    Ok(scopes
        .into_iter()
        .enumerate()
        .filter_map(|(index, scope)| {
            markers
                .row(index)
                .is_some_and(|row| !row.deleted())
                .then_some(scope)
        })
        .collect())
}

fn base_key_suppressed(key: &TrackedStateKey, scopes: &BTreeSet<(String, Option<String>)>) -> bool {
    scopes.contains(&(key.schema_key.clone(), None))
        || key.file_id.as_ref().is_some_and(|file_id| {
            scopes.contains(&(key.schema_key.clone(), Some(file_id.clone())))
        })
}

fn effective_base_source(commit_id: &str, descriptor: &CommitStateDescriptor) -> String {
    descriptor
        .base_commit_id
        .map_or_else(|| commit_id.to_owned(), |base| base.to_string())
}

fn extend_diff_keys(keys: &mut BTreeSet<TrackedStateKey>, diff: &TrackedStateDiff) {
    keys.extend(diff.entries.iter().map(|entry| TrackedStateKey {
        schema_key: entry.identity.schema_key().to_owned(),
        file_id: entry.identity.file_id().map(str::to_owned),
        row_pk: entry.identity.row_pk().clone(),
    }));
}

async fn load_base_rows<S: StorageAdapterRead>(
    tracked: &mut TrackedStateStoreReader<S>,
    descriptor: &CommitStateDescriptor,
    keys: &[TrackedStateKey],
    projection: &ChangeRecordProjection,
) -> Result<Option<MaterializedTrackedStateExactBatch>> {
    let Some(base_commit_id) = descriptor.base_commit_id else {
        return Ok(None);
    };
    debug_assert!(!descriptor.global_scope);
    tracked
        .load_projected_batch_at_commit(&base_commit_id.to_string(), keys, projection)
        .await
        .map(Some)
        .map_err(lix_error_to_datafusion_error)
}

fn effective_row<'a>(
    local: Option<MaterializedTrackedStateRowRef<'a>>,
    base: Option<MaterializedTrackedStateRowRef<'a>>,
    global_scope: bool,
    base_suppressed: bool,
) -> (Option<MaterializedTrackedStateRowRef<'a>>, bool) {
    match local {
        Some(row) => (Some(row), global_scope),
        None if !base_suppressed => (base, base.is_some()),
        None => (None, false),
    }
}

fn classify_effective_rows(
    before: Option<MaterializedTrackedStateRowRef<'_>>,
    after: Option<MaterializedTrackedStateRowRef<'_>>,
) -> Option<TrackedStateDiffKind> {
    let before_live = before.filter(|row| !row.deleted());
    let after_live = after.filter(|row| !row.deleted());
    match (before_live, after_live) {
        (None, None) => None,
        (None, Some(_)) => Some(TrackedStateDiffKind::Added),
        (Some(_), None) => Some(TrackedStateDiffKind::Removed),
        (Some(before), Some(after))
            if before.change_id() == after.change_id()
                || (effective_snapshot_eq(before, after)
                    && before.metadata() == after.metadata()) =>
        {
            None
        }
        (Some(_), Some(_)) => Some(TrackedStateDiffKind::Modified),
    }
}

fn effective_snapshot_eq(
    before: MaterializedTrackedStateRowRef<'_>,
    after: MaterializedTrackedStateRowRef<'_>,
) -> bool {
    match (before.decoded_snapshot(), after.decoded_snapshot()) {
        (Some(before), Some(after)) => match (before.durable_payload(), after.durable_payload()) {
            (Ok(before), Ok(after)) => before.as_ref() == after.as_ref(),
            _ => false,
        },
        _ => before.snapshot_content() == after.snapshot_content(),
    }
}

fn diff_row(
    identity: TrackedStateDiffIdentity,
    row: MaterializedTrackedStateRowRef<'_>,
) -> TrackedStateDiffRow {
    TrackedStateDiffRow {
        identity,
        deleted: row.deleted(),
        created_at: row.created_at(),
        updated_at: row.updated_at(),
        change_id: row.change_id(),
        commit_id: row.commit_id(),
        author_id: row.author_id().to_owned(),
    }
}

fn schema_diff_rows(
    diff: TrackedStateDiff,
    schema_key: &str,
    projection: &Schema,
    from_global_rows: &HashSet<TrackedStateKey>,
    to_global_rows: &HashSet<TrackedStateKey>,
) -> Result<Vec<DiffSqlRow>> {
    let needs_side = projection
        .fields()
        .iter()
        .any(|field| side_column(field.name()).is_some());
    diff.entries
        .iter()
        .filter(|entry| {
            !super::schema::hidden_registered_schema_row(schema_key, entry.identity.row_pk())
        })
        .map(|entry| {
            Ok(DiffSqlRow {
                row_pk: entry.identity.row_pk().clone(),
                file_id: entry.identity.file_id().map(str::to_owned),
                diff_type: diff_type(entry.kind),

                from: if needs_side {
                    diff_side(entry, entry.visible_before(), &diff, from_global_rows)?
                } else {
                    None
                },
                to: if needs_side {
                    diff_side(
                        entry,
                        entry.after.as_ref().filter(|row| !row.deleted),
                        &diff,
                        to_global_rows,
                    )?
                } else {
                    None
                },
            })
        })
        .collect()
}

fn diff_side(
    entry: &TrackedStateDiffEntry,
    row: Option<&TrackedStateDiffRow>,
    diff: &TrackedStateDiff,
    global_rows: &HashSet<TrackedStateKey>,
) -> Result<Option<DiffSide>> {
    let Some(row) = row else {
        return Ok(None);
    };
    let payload = diff.payloads().get(row.change_id);
    let snapshot = payload
        .and_then(|payload| payload.snapshot)
        .map(|bytes| {
            WasmTypedRow::decode_durable_payload(
                Arc::from(bytes),
                entry.identity.schema_key(),
                entry.identity.row_pk(),
            )
            .map(Arc::new)
            .map_err(lix_error_to_datafusion_error)
        })
        .transpose()?;
    let metadata =
        payload.and_then(|payload| payload.metadata.map(|value| value.as_value().clone()));
    Ok(Some(DiffSide {
        id: single_row_pk_string(entry.identity.row_pk()),
        global: global_rows.contains(&TrackedStateKey {
            schema_key: entry.identity.schema_key().to_owned(),
            file_id: entry.identity.file_id().map(str::to_owned),
            row_pk: entry.identity.row_pk().clone(),
        }),
        file_id: entry.identity.file_id().map(str::to_string),
        created_at: row.created_at.to_string(),
        updated_at: row.updated_at.to_string(),
        change_id: row.change_id.to_string(),
        author_id: row.author_id.clone(),
        commit_id: row.commit_id.to_string(),
        metadata,
        snapshot,
        path: None,
    }))
}

fn diff_type(kind: TrackedStateDiffKind) -> &'static str {
    match kind {
        TrackedStateDiffKind::Added => "added",
        TrackedStateDiffKind::Modified => "modified",
        TrackedStateDiffKind::Removed => "removed",
    }
}

fn single_row_pk_string(row_pk: &RowPk) -> Option<String> {
    match row_pk.as_json_array_value().ok()? {
        JsonValue::Array(values) => match values.as_slice() {
            [JsonValue::String(value)] => Some(value.clone()),
            _ => None,
        },
        _ => None,
    }
}

struct FileDiffGroup<'a> {
    descriptor: Option<&'a TrackedStateDiffEntry>,
}

#[cfg(test)]
thread_local! { static PATH_EXPANSIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

/// Find path-only rows independently of the requested output columns. Exact
/// filesystem identity predicates stay point reads; unfiltered directory changes
/// enumerate descriptor identities, never file contents or all tracked atoms.
async fn path_changed_descriptors<S: StorageAdapterRead>(
    tracked: &mut TrackedStateStoreReader<S>,
    exact_ids: &BTreeSet<String>,
    direct_ids: &BTreeSet<String>,
    directory: bool,
    from_commit_id: &str,
    to_commit_id: &str,
    from_descriptor: &CommitStateDescriptor,
    to_descriptor: &CommitStateDescriptor,
) -> Result<Vec<(String, DiffSide, DiffSide)>> {
    #[cfg(test)]
    PATH_EXPANSIONS.with(|n| n.set(n.get() + 1));
    let schema_key = if directory {
        DIRECTORY_DESCRIPTOR_SCHEMA_KEY
    } else {
        FILE_DESCRIPTOR_SCHEMA_KEY
    };
    let mut ids = exact_ids.clone();
    if ids.is_empty() {
        let mut sources = BTreeSet::from([from_commit_id.to_owned(), to_commit_id.to_owned()]);
        sources.extend(
            from_descriptor
                .base_commit_id
                .iter()
                .chain(to_descriptor.base_commit_id.iter())
                .map(ToString::to_string),
        );
        let request = TrackedStateScanRequest {
            filter: TrackedStateFilter {
                schema_keys: vec![schema_key.into()],
                include_tombstones: true,
                ..Default::default()
            },
            read_columns: TrackedStateReadColumns {
                columns: vec!["row_pk".into()],
            },
            limit: None,
        };
        for source in sources {
            let rows = tracked
                .scan_batch_at_commit(&source, &request)
                .await
                .map_err(lix_error_to_datafusion_error)?;
            for row in rows.iter() {
                if let Some(id) = single_row_pk_string(row.row_pk()) {
                    ids.insert(id);
                }
            }
        }
    }
    ids.retain(|id| !direct_ids.contains(id));
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = ids
        .iter()
        .map(|id| descriptor_key(schema_key, id, (!directory).then_some(id.as_str())))
        .collect::<Result<Vec<_>>>()?;
    let projection = ChangeRecordProjection {
        snapshot: true,
        snapshot_content: false,
        metadata: false,
        raw_snapshot: false,
    };
    let from_rows = tracked
        .load_projected_batch_at_commit(from_commit_id, &keys, &projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let to_rows = tracked
        .load_projected_batch_at_commit(to_commit_id, &keys, &projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let from_base = load_base_rows(tracked, from_descriptor, &keys, &projection).await?;
    let to_base = load_base_rows(tracked, to_descriptor, &keys, &projection).await?;
    let from_replacements =
        load_local_replacement_scopes_for_keys(tracked, from_commit_id, from_descriptor, &keys)
            .await?;
    let to_replacements =
        load_local_replacement_scopes_for_keys(tracked, to_commit_id, to_descriptor, &keys).await?;
    let mut from_cache = HashMap::new();
    let mut to_cache = HashMap::new();
    let mut changed = Vec::new();
    for (index, id) in ids.into_iter().enumerate() {
        let (from, from_global) = effective_row(
            from_rows.row(index),
            from_base.as_ref().and_then(|rows| rows.row(index)),
            from_descriptor.global_scope,
            base_key_suppressed(&keys[index], &from_replacements),
        );
        let (to, to_global) = effective_row(
            to_rows.row(index),
            to_base.as_ref().and_then(|rows| rows.row(index)),
            to_descriptor.global_scope,
            base_key_suppressed(&keys[index], &to_replacements),
        );
        let (Some(mut from), Some(mut to)) = (materialized_side(from)?, materialized_side(to)?)
        else {
            continue;
        };
        let from_path = filesystem_path(
            tracked,
            from_commit_id,
            from_descriptor,
            &from,
            directory,
            &mut from_cache,
        )
        .await?;
        let to_path = filesystem_path(
            tracked,
            to_commit_id,
            to_descriptor,
            &to,
            directory,
            &mut to_cache,
        )
        .await?;
        if from_path != to_path {
            from.global = from_global;
            to.global = to_global;
            from.path = Some(from_path);
            to.path = Some(to_path);
            changed.push((id, from, to));
        }
    }
    Ok(changed)
}

async fn file_diff_rows<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    diff: TrackedStateDiff,
    file_filter: &[NullableKeyFilter<String>],
    projection: &Schema,
    from_commit_id: &str,
    to_commit_id: &str,
    from_descriptor: &CommitStateDescriptor,
    to_descriptor: &CommitStateDescriptor,
) -> Result<Vec<DiffSqlRow>>
where
    S: StorageAdapterRead,
{
    let mut groups = BTreeMap::<String, FileDiffGroup<'_>>::new();
    for entry in &diff.entries {
        let file_id = if entry.identity.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
            entry
                .identity
                .file_id()
                .map(Cow::Borrowed)
                .or_else(|| single_row_pk_string(entry.identity.row_pk()).map(Cow::Owned))
        } else {
            entry.identity.file_id().map(Cow::Borrowed)
        };
        let Some(file_id) = file_id else {
            continue;
        };
        let group = if let Some(group) = groups.get_mut(file_id.as_ref()) {
            group
        } else {
            groups
                .entry(file_id.into_owned())
                .or_insert(FileDiffGroup { descriptor: None })
        };
        if entry.identity.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
            group.descriptor = Some(entry);
        }
    }

    // Adding a directory cannot change the path of a surviving, unchanged file.
    // New files and files reparented into it are already direct diff candidates.
    if diff.entries.iter().any(|entry| {
        entry.identity.schema_key() == DIRECTORY_DESCRIPTOR_SCHEMA_KEY
            && entry.kind != TrackedStateDiffKind::Added
    }) {
        let exact_ids = file_filter
            .iter()
            .filter_map(|filter| match filter {
                NullableKeyFilter::Value(id) => Some(id.clone()),
                NullableKeyFilter::Null | NullableKeyFilter::Any => None,
            })
            .collect();
        let path_changes = path_changed_descriptors(
            tracked,
            &exact_ids,
            &groups.keys().cloned().collect(),
            false,
            from_commit_id,
            to_commit_id,
            from_descriptor,
            to_descriptor,
        )
        .await?;
        for (id, _, _) in path_changes {
            groups
                .entry(id)
                .or_insert(FileDiffGroup { descriptor: None });
        }
    }

    let needs_side = projection
        .fields()
        .iter()
        .any(|field| side_column(field.name()).is_some());
    let descriptor_keys = if needs_side {
        groups
            .keys()
            .map(|file_id| descriptor_key(FILE_DESCRIPTOR_SCHEMA_KEY, file_id, Some(file_id)))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    let needs_snapshot = projection
        .fields()
        .iter()
        .filter_map(|field| side_column(field.name()))
        .any(|(_, column)| {
            !matches!(
                column,
                "id" | "lixcol_file_id"
                    | "lixcol_created_at"
                    | "lixcol_updated_at"
                    | "lixcol_change_id"
                    | "lixcol_author_id"
                    | "lixcol_commit_id"
                    | "lixcol_global"
                    | "lixcol_untracked"
                    | "lixcol_metadata"
            )
        });
    let needs_metadata = projection.fields().iter().any(|field| {
        matches!(
            field.name().as_str(),
            "from_lixcol_metadata" | "to_lixcol_metadata"
        )
    });
    let descriptor_projection = ChangeRecordProjection {
        snapshot_content: false,
        metadata: needs_metadata,
        snapshot: needs_snapshot,
        raw_snapshot: false,
    };
    let from_descriptors = tracked
        .load_projected_batch_at_commit(from_commit_id, &descriptor_keys, &descriptor_projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let to_descriptors = tracked
        .load_projected_batch_at_commit(to_commit_id, &descriptor_keys, &descriptor_projection)
        .await
        .map_err(lix_error_to_datafusion_error)?;
    let from_base_descriptors = load_base_rows(
        tracked,
        from_descriptor,
        &descriptor_keys,
        &descriptor_projection,
    )
    .await?;
    let to_base_descriptors = load_base_rows(
        tracked,
        to_descriptor,
        &descriptor_keys,
        &descriptor_projection,
    )
    .await?;
    let from_descriptor_replacements = load_local_replacement_scopes_for_keys(
        tracked,
        from_commit_id,
        from_descriptor,
        &descriptor_keys,
    )
    .await?;
    let to_descriptor_replacements = load_local_replacement_scopes_for_keys(
        tracked,
        to_commit_id,
        to_descriptor,
        &descriptor_keys,
    )
    .await?;
    let needs_paths = projection
        .fields()
        .iter()
        .any(|field| matches!(field.name().as_str(), "from_path" | "to_path"));
    let mut from_directory_cache = HashMap::new();
    let mut to_directory_cache = HashMap::new();

    let mut rows = Vec::with_capacity(groups.len());
    for (index, (file_id, group)) in groups.into_iter().enumerate() {
        let kind = group
            .descriptor
            .map_or(TrackedStateDiffKind::Modified, |entry| entry.kind);
        let row_pk = if let Some(descriptor) = group.descriptor {
            descriptor.identity.row_pk().clone()
        } else {
            uuid_row_pk(&file_id)?
        };
        let (from_descriptor_row, from_global) = effective_row(
            from_descriptors.row(index),
            from_base_descriptors
                .as_ref()
                .and_then(|rows| rows.row(index)),
            from_descriptor.global_scope,
            descriptor_keys
                .get(index)
                .is_some_and(|key| base_key_suppressed(key, &from_descriptor_replacements)),
        );
        let (to_descriptor_row, to_global) = effective_row(
            to_descriptors.row(index),
            to_base_descriptors
                .as_ref()
                .and_then(|rows| rows.row(index)),
            to_descriptor.global_scope,
            descriptor_keys
                .get(index)
                .is_some_and(|key| base_key_suppressed(key, &to_descriptor_replacements)),
        );
        let mut from = if needs_side {
            materialized_side(from_descriptor_row)?
        } else {
            None
        };
        let mut to = if needs_side {
            materialized_side(to_descriptor_row)?
        } else {
            None
        };
        if let Some(side) = from.as_mut() {
            side.global = from_global;
        }
        if let Some(side) = to.as_mut() {
            side.global = to_global;
        }
        if needs_paths {
            if let Some(side) = from.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        from_commit_id,
                        from_descriptor,
                        side,
                        false,
                        &mut from_directory_cache,
                    )
                    .await?,
                );
            }
            if let Some(side) = to.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        to_commit_id,
                        to_descriptor,
                        side,
                        false,
                        &mut to_directory_cache,
                    )
                    .await?,
                );
            }
        }
        rows.push(DiffSqlRow {
            row_pk,
            file_id: None,
            diff_type: diff_type(kind),
            from,
            to,
        });
    }
    Ok(rows)
}

async fn directory_diff_rows<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    diff: TrackedStateDiff,
    exact_row_pks: &[RowPk],
    projection: &Schema,
    from_commit_id: &str,
    to_commit_id: &str,
    from_descriptor: &CommitStateDescriptor,
    to_descriptor: &CommitStateDescriptor,
    from_global_rows: &HashSet<TrackedStateKey>,
    to_global_rows: &HashSet<TrackedStateKey>,
) -> Result<Vec<DiffSqlRow>>
where
    S: StorageAdapterRead,
{
    let needs_paths = projection
        .fields()
        .iter()
        .any(|field| matches!(field.name().as_str(), "from_path" | "to_path"));
    let should_expand = !diff.entries.is_empty() || !exact_row_pks.is_empty();
    let mut rows = schema_diff_rows(diff, "", projection, from_global_rows, to_global_rows)?;
    if should_expand {
        let exact_ids = exact_row_pks
            .iter()
            .filter_map(single_row_pk_string)
            .collect();
        let direct_ids = rows
            .iter()
            .filter_map(|row| single_row_pk_string(&row.row_pk))
            .collect();
        for (id, from, to) in path_changed_descriptors(
            tracked,
            &exact_ids,
            &direct_ids,
            true,
            from_commit_id,
            to_commit_id,
            from_descriptor,
            to_descriptor,
        )
        .await?
        {
            rows.push(DiffSqlRow {
                row_pk: uuid_row_pk(&id)?,
                file_id: None,
                diff_type: "modified",

                from: Some(from),
                to: Some(to),
            });
        }
    }
    if needs_paths {
        let mut from_directory_cache = HashMap::new();
        let mut to_directory_cache = HashMap::new();
        for row in &mut rows {
            if let Some(side) = row.from.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        from_commit_id,
                        from_descriptor,
                        side,
                        true,
                        &mut from_directory_cache,
                    )
                    .await?,
                );
            }
            if let Some(side) = row.to.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        to_commit_id,
                        to_descriptor,
                        side,
                        true,
                        &mut to_directory_cache,
                    )
                    .await?,
                );
            }
        }
    }
    Ok(rows)
}

fn descriptor_key(schema_key: &str, id: &str, file_id: Option<&str>) -> Result<TrackedStateKey> {
    Ok(TrackedStateKey {
        schema_key: schema_key.to_string(),
        file_id: file_id.map(str::to_string),
        row_pk: uuid_row_pk(id)?,
    })
}

fn uuid_row_pk(id: &str) -> Result<RowPk> {
    RowPk::from_json_values(
        &[JsonValue::String(id.to_string())],
        &[RowPkComponentType::Uuid],
    )
    .map_err(|error| {
        DataFusionError::Execution(format!("invalid filesystem row identity: {error}"))
    })
}

fn materialized_side(row: Option<MaterializedTrackedStateRowRef<'_>>) -> Result<Option<DiffSide>> {
    let Some(row) = row.filter(|row| !row.deleted()) else {
        return Ok(None);
    };
    let metadata = row
        .metadata()
        .map(|metadata| {
            serde_json::from_str(metadata.as_str()).map_err(|error| {
                DataFusionError::Execution(format!("invalid lix_diff row metadata: {error}"))
            })
        })
        .transpose()?;
    Ok(Some(DiffSide {
        id: single_row_pk_string(row.row_pk()),
        global: false, // Filled from effective endpoint overlay provenance by the caller.
        file_id: row.file_id().map(str::to_string),
        created_at: row.created_at().to_string(),
        updated_at: row.updated_at().to_string(),
        change_id: row.change_id().to_string(),
        author_id: row.author_id().to_string(),
        commit_id: row.commit_id().to_string(),
        metadata,
        snapshot: row.decoded_snapshot().cloned(),
        path: None,
    }))
}

async fn filesystem_path<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
    descriptor: &CommitStateDescriptor,
    side: &DiffSide,
    directory: bool,
    directory_cache: &mut HashMap<String, (String, Option<String>)>,
) -> Result<String>
where
    S: StorageAdapterRead,
{
    let snapshot = side.snapshot.as_ref().ok_or_else(|| {
        DataFusionError::Execution("filesystem descriptor is missing its snapshot".to_string())
    })?;
    let mut segments = vec![typed_string(snapshot, "name")?.ok_or_else(|| {
        DataFusionError::Execution("filesystem descriptor is missing its name".to_string())
    })?];
    let mut parent_id = typed_string(
        snapshot,
        if directory {
            "parent_id"
        } else {
            "directory_id"
        },
    )?;
    let mut depth = 0;
    while let Some(id) = parent_id {
        depth += 1;
        if depth > 128 {
            return Err(DataFusionError::NotImplemented(
                "lix_diff does not support directory trees deeper than 128 levels".to_string(),
            ));
        }
        if let Some((name, next_parent)) = directory_cache.get(&id) {
            segments.push(name.clone());
            parent_id = next_parent.clone();
            continue;
        }
        let key = descriptor_key(DIRECTORY_DESCRIPTOR_SCHEMA_KEY, &id, None)?;
        let local = tracked
            .load_projected_batch_at_commit(
                commit_id,
                std::slice::from_ref(&key),
                &ChangeRecordProjection::full(),
            )
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let base = load_base_rows(
            tracked,
            descriptor,
            std::slice::from_ref(&key),
            &ChangeRecordProjection::full(),
        )
        .await?;
        let replacements = load_local_replacement_scopes_for_keys(
            tracked,
            commit_id,
            descriptor,
            std::slice::from_ref(&key),
        )
        .await?;
        let (parent, _) = effective_row(
            local.row(0),
            base.as_ref().and_then(|rows| rows.row(0)),
            descriptor.global_scope,
            base_key_suppressed(&key, &replacements),
        );
        let parent = parent.filter(|row| !row.deleted()).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "filesystem descriptor references missing directory '{id}'"
            ))
        })?;
        let snapshot = parent.decoded_snapshot().ok_or_else(|| {
            DataFusionError::Execution("directory descriptor is missing its snapshot".to_string())
        })?;
        let name = typed_string(snapshot, "name")?.ok_or_else(|| {
            DataFusionError::Execution("directory descriptor is missing its name".to_string())
        })?;
        parent_id = typed_string(snapshot, "parent_id")?;
        directory_cache.insert(id, (name.clone(), parent_id.clone()));
        segments.push(name);
    }
    segments.reverse();
    Ok(format!("/{}", segments.join("/")))
}

fn typed_string(snapshot: &WasmTypedRow, name: &str) -> Result<Option<String>> {
    match snapshot.row.get(name) {
        None | Some(lix_schema::Value::Null) => Ok(None),
        Some(lix_schema::Value::Text(value)) => Ok(Some(value.clone())),
        Some(lix_schema::Value::Uuid(value)) => Ok(Some(value.to_string())),
        _ => Err(DataFusionError::Execution(format!(
            "lix_diff column '{name}' expected text"
        ))),
    }
}

fn side_column(name: &str) -> Option<(bool, &str)> {
    name.strip_prefix("from_")
        .map(|column| (false, column))
        .or_else(|| name.strip_prefix("to_").map(|column| (true, column)))
}

fn diff_record_batch(
    schema: SchemaRef,
    rows: &[DiffSqlRow],
    relation: &DiffRelation,
    from_commit_id: &str,
    to_commit_id: &str,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(rows.len())),
        )
        .map_err(DataFusionError::from);
    }
    let arrays = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef> {
            match field.name().as_str() {
                "lixcol_from_commit_id" => Ok(Arc::new(StringArray::from(vec![
                    from_commit_id;
                    rows.len()
                ]))),
                "lixcol_to_commit_id" => {
                    Ok(Arc::new(StringArray::from(vec![to_commit_id; rows.len()])))
                }
                _ => diff_column_array(field, rows, relation),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn diff_column_array(
    field: &Field,
    rows: &[DiffSqlRow],
    relation: &DiffRelation,
) -> Result<ArrayRef> {
    match field.name().as_str() {
        "row_ref" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    crate::row_ref::encode(&relation.name, row.file_id.as_deref(), &row.row_pk)
                        .map(|value| value.as_str().to_owned())
                        .map_err(lix_error_to_datafusion_error)
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        "diff_type" => Ok(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.diff_type),
        ))),
        name if relation
            .primary_key_columns
            .iter()
            .any(|column| column == name) =>
        {
            let component_index = relation
                .primary_key_columns
                .iter()
                .position(|column| column == name)
                .expect("the primary-key column guard found this column");
            primary_key_array(field, rows, component_index)
        }
        name => {
            let (after, column) = side_column(name).ok_or_else(|| {
                DataFusionError::Execution(format!("unsupported diff column '{name}'"))
            })?;
            let values = rows
                .iter()
                .map(|row| {
                    let side = if after {
                        row.to.as_ref()
                    } else {
                        row.from.as_ref()
                    };
                    side_value(side, column)
                })
                .collect::<Result<Vec<_>>>()?;
            values_array(field, &values)
        }
    }
}

/// Materializes a typed primary-key column from the stable diff identity.
///
/// A key is neither a before-side nor an after-side payload property: it is
/// the identity joining those sides. Reading it from a snapshot made projected
/// key-only diffs NULL for additions/removals because no side payload needed
/// to be hydrated. The canonical RowPk is always present for every diff kind.
fn primary_key_array(
    field: &Field,
    rows: &[DiffSqlRow],
    component_index: usize,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Utf8 => Ok(Arc::new(StringArray::from_iter_values(
            rows.iter()
                .map(
                    |row| match row.row_pk.components.as_slice().get(component_index) {
                        Some(RowPkComponent::Uuid(value)) => {
                            Ok(uuid::Uuid::from_bytes(*value).to_string())
                        }
                        Some(RowPkComponent::String(value)) => Ok(value.to_string()),
                        component => Err(primary_key_component_error(field, component)),
                    },
                )
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from_iter_values(
            rows.iter()
                .map(
                    |row| match row.row_pk.components.as_slice().get(component_index) {
                        Some(RowPkComponent::Integer(value)) => Ok(*value),
                        component => Err(primary_key_component_error(field, component)),
                    },
                )
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::LargeBinary => {
            let values = rows
                .iter()
                .map(
                    |row| match row.row_pk.components.as_slice().get(component_index) {
                        Some(RowPkComponent::Bytes(value)) => Ok(value.as_ref()),
                        component => Err(primary_key_component_error(field, component)),
                    },
                )
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(LargeBinaryArray::from_iter_values(values)))
        }
        data_type => Err(DataFusionError::Execution(format!(
            "lix_diff primary-key column '{}' has unsupported type {data_type}",
            field.name()
        ))),
    }
}

fn primary_key_component_error(
    field: &Field,
    component: Option<&RowPkComponent>,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "lix_diff primary-key column '{}' does not match canonical identity component {component:?}",
        field.name()
    ))
}

fn side_value(side: Option<&DiffSide>, column: &str) -> Result<Option<lix_schema::Value>> {
    let Some(side) = side else {
        return Ok(None);
    };
    Ok(match column {
        "id" => side.id.clone().map(lix_schema::Value::Text),
        "path" => side.path.clone().map(lix_schema::Value::Text),
        "lixcol_file_id" => side.file_id.clone().map(lix_schema::Value::Text),
        "lixcol_created_at" => Some(lix_schema::Value::Text(side.created_at.clone())),
        "lixcol_updated_at" => Some(lix_schema::Value::Text(side.updated_at.clone())),
        "lixcol_change_id" => Some(lix_schema::Value::Text(side.change_id.clone())),
        "lixcol_author_id" => Some(lix_schema::Value::Text(side.author_id.clone())),
        "lixcol_commit_id" => Some(lix_schema::Value::Text(side.commit_id.clone())),
        "lixcol_global" => Some(lix_schema::Value::Boolean(side.global)),
        "lixcol_untracked" => Some(lix_schema::Value::Boolean(false)),
        "lixcol_metadata" => side
            .metadata
            .clone()
            .map(|value| lix_schema::Value::Jsonb(value.into())),
        _ => side
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.row.get(column).cloned()),
    })
}

fn values_array(field: &Field, values: &[Option<lix_schema::Value>]) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            values
                .iter()
                .map(|value| match value {
                    None | Some(lix_schema::Value::Null) => Ok(None),
                    Some(lix_schema::Value::Text(value)) => Ok(Some(value.clone())),
                    Some(lix_schema::Value::Uuid(value)) => Ok(Some(value.to_string())),
                    Some(lix_schema::Value::Jsonb(value)) if field_is_json(field) => {
                        Ok(Some(value.as_value().to_string()))
                    }
                    _ => Err(DataFusionError::Execution(format!(
                        "lix_diff column '{}' expected text",
                        field.name()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values
                .iter()
                .map(|value| match value {
                    None | Some(lix_schema::Value::Null) => Ok(None),
                    Some(lix_schema::Value::Int8(value)) => Ok(Some(*value)),
                    _ => Err(DataFusionError::Execution(format!(
                        "lix_diff column '{}' expected INT8",
                        field.name()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            values
                .iter()
                .map(|value| match value {
                    None | Some(lix_schema::Value::Null) => Ok(None),
                    Some(lix_schema::Value::Float8(value)) => Ok(Some(*value)),
                    _ => Err(DataFusionError::Execution(format!(
                        "lix_diff column '{}' expected FLOAT8",
                        field.name()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            values
                .iter()
                .map(|value| match value {
                    None | Some(lix_schema::Value::Null) => Ok(None),
                    Some(lix_schema::Value::Boolean(value)) => Ok(Some(*value)),
                    _ => Err(DataFusionError::Execution(format!(
                        "lix_diff column '{}' expected BOOLEAN",
                        field.name()
                    ))),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Ok(Arc::new(
            TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|value| match value {
                        None | Some(lix_schema::Value::Null) => Ok(None),
                        Some(lix_schema::Value::Timestamptz(value)) => Ok(Some(*value)),
                        _ => Err(DataFusionError::Execution(format!(
                            "lix_diff column '{}' expected TIMESTAMPTZ",
                            field.name()
                        ))),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone_opt(timezone.clone()),
        )),
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from(vec![
            None::<&[u8]>;
            values.len()
        ]))),
        data_type => Err(DataFusionError::NotImplemented(format!(
            "lix_diff column '{}' does not support {data_type}",
            field.name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[tokio::test]
    async fn descriptor_reports_independent_missing_inputs_together() {
        use crate::storage_adapter::SharedStorageAdapterRead;
        use crate::tracked_state::NativeMetadataRef;
        let lix = crate::open_lix().await.unwrap();
        let store = SharedStorageAdapterRead::new(
            lix.storage_adapter()
                .begin_read(Default::default())
                .await
                .unwrap(),
        );
        let id = "0193182b-2a72-7ed5-9015-76bf271af333";
        let error = match commit_state_descriptor(&store, id).await {
            Ok(_) => panic!("unknown commit must need inputs"),
            Err(error) => datafusion_error_to_lix_error(error),
        };
        assert_eq!(
            NativeMetadataRef::batch_from_missing_error(&error).unwrap(),
            Some(vec![
                NativeMetadataRef::CommitStateHeader(id.into()),
                NativeMetadataRef::CommitGraphRecord(id.into()),
            ])
        );
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn descriptor_does_not_hide_corruption_behind_another_missing_input() {
        use crate::storage_adapter::{
            SharedStorageAdapterRead, StorageAdapter, StorageKey, StorageValue,
        };
        use crate::tracked_state::NativeMetadataRef;
        let id = CommitId::parse("0193182b-2a72-7ed5-9015-76bf271af333").unwrap();
        for corrupt_graph in [false, true] {
            let storage = StorageAdapter::new(crate::Memory::new());
            let (space, key) = if corrupt_graph {
                (
                    crate::changelog::COMMIT_SPACE,
                    StorageKey(bytes::Bytes::from(crate::changelog::commit_key(id))),
                )
            } else {
                (
                    crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                    crate::tracked_state::commit_state_authority_key(id),
                )
            };
            let mut writes = storage.new_write_set();
            writes.put(
                space,
                key,
                StorageValue {
                    bytes: bytes::Bytes::from_static(b"corrupt"),
                },
            );
            storage
                .commit_write_set(writes, Default::default())
                .await
                .unwrap();
            let read = SharedStorageAdapterRead::new(
                storage.begin_read(Default::default()).await.unwrap(),
            );
            let error = match commit_state_descriptor(&read, &id.to_string()).await {
                Ok(_) => panic!("resident corruption must fail"),
                Err(error) => datafusion_error_to_lix_error(error),
            };
            assert!(
                NativeMetadataRef::from_missing_error(&error)
                    .unwrap()
                    .is_none(),
                "{error}"
            );
            assert!(
                NativeMetadataRef::batch_from_missing_error(&error)
                    .unwrap()
                    .is_none(),
                "{error}"
            );
        }
    }

    #[derive(Clone)]
    struct ContentReadProbe<S> {
        inner: S,
        chunks: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    }

    impl<S: StorageAdapterRead> StorageAdapterRead for ContentReadProbe<S> {
        async fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> std::result::Result<crate::storage::GetManyResult, crate::storage::StorageError>
        {
            for request in requests {
                if request.space.name == "binary_cas.chunk" {
                    self.chunks
                        .lock()
                        .unwrap()
                        .extend(request.keys.iter().map(|key| key.0.to_vec()));
                }
            }
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: crate::storage::StorageSpace,
            range: crate::storage::KeyRange,
            opts: crate::storage::BeginScanOptions,
        ) -> std::result::Result<crate::storage::ScanCursor<'_>, crate::storage::StorageError>
        {
            assert_ne!(
                space.name, "binary_cas.chunk",
                "file content must use exact chunk reads"
            );
            self.inner.begin_scan(space, range, opts).await
        }
    }

    #[tokio::test]
    async fn file_content_filters_precede_storage_reads_for_diff_and_history() {
        use crate::storage_adapter::SharedStorageAdapterRead;
        use crate::{Value, open_lix};
        let lix = open_lix()
            .with_storage(crate::storage::Memory::new())
            .await
            .unwrap();
        let id = "0193182b-2a72-7ed5-9015-76bf271af333";
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/selected', $2)",
            &[
                Value::Text(id.into()),
                Value::Blob(b"before".to_vec().into()),
            ],
        )
        .await
        .unwrap();
        let before = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        // The compared range changes the small selected file and an unrelated
        // 40 MiB asset. The test records physical chunk requests, not elapsed time.
        let large = vec![0xab; 40 * 1024 * 1024];
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/unrelated.asset', $1)",
            &[Value::Blob(large.into())],
        )
        .await
        .unwrap();
        lix.execute(
            "UPDATE lix_file SET path = '/renamed', content = $1 WHERE id = $2",
            &[
                Value::Blob(b"after".to_vec().into()),
                Value::Text(id.into()),
            ],
        )
        .await
        .unwrap();
        let after = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let adapter = lix.storage_adapter();
        let chunks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let store = ContentReadProbe {
            inner: SharedStorageAdapterRead::new(
                adapter.begin_read(Default::default()).await.unwrap(),
            ),
            chunks: chunks.clone(),
        };
        let blob_reader =
            Arc::new(crate::binary_cas::BinaryCasContext::new().reader(store.clone()));
        let context = datafusion::prelude::SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .with_extension(Arc::new(ExecutionSlots::default())),
        );
        let relation =
            DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file").unwrap();
        context
            .register_table(
                "changes",
                Arc::new(SpecTableProvider::new(Arc::new(DiffSpec {
                    path_cache: None,
                    store: store.clone(),
                    blob_reader: blob_reader.clone(),
                    read_interests: None,
                    interest_endpoints: None,
                    relation,
                    from_commit_id: before,
                    to_commit_id: after.clone(),
                    active_branch_id: None,
                    mode: DiffMode::General,
                }))),
            )
            .unwrap();
        super::super::mainline::register_functions(
            &context,
            SqlChangelogQuerySource { store },
            Arc::new(PublicCatalog::fixed_system().clone()),
            blob_reader,
        );
        let allowed = [b"before".as_slice(), b"after".as_slice()].map(|bytes| {
            crate::binary_cas::ChunkHash::from_content(bytes)
                .into_bytes()
                .to_vec()
        });
        for source in [
            "changes".to_string(),
            format!("lix_history('lix_file', '{after}')"),
        ] {
            for predicate in [
                format!("id = '{id}'"),
                "from_path = '/selected'".into(),
                "to_path = '/renamed'".into(),
                "coalesce(to_path, from_path) = '/renamed'".into(),
                format!("id IN ('{id}') AND diff_type = 'modified'"),
            ] {
                chunks.lock().unwrap().clear();
                let batches = context
                    .sql(&format!(
                        "SELECT from_content, to_content FROM {source} WHERE {predicate}"
                    ))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= 1);
                let reads = chunks.lock().unwrap();
                assert!(
                    !reads.is_empty(),
                    "selected file content must reach storage"
                );
                assert!(
                    reads.iter().all(|key| allowed.contains(key)),
                    "unrelated asset chunk read for {predicate}: {reads:?}"
                );
            }
            chunks.lock().unwrap().clear();
            context
                .sql(&format!("SELECT id FROM {source}"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert!(
                chunks.lock().unwrap().is_empty(),
                "metadata projection must not open blob bytes"
            );
            context
                .sql(&format!(
                    "SELECT from_content, to_content FROM {source} WHERE to_path = '/missing'"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert!(
                chunks.lock().unwrap().is_empty(),
                "empty selection must not open blob bytes"
            );
        }
        chunks.lock().unwrap().clear();
        context
            .sql(&format!(
                "SELECT to_content FROM changes WHERE id = '{id}' LIMIT 1"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(
            chunks.lock().unwrap().iter().all(|key| key == &allowed[1]),
            "projecting only the after side must not open before bytes"
        );
        let mixed = context.sql(&format!(
            "SELECT id FROM changes WHERE id = '{id}' OR to_content = CAST('not a file' AS BYTEA)"
        )).await.unwrap().collect().await.unwrap();
        assert_eq!(
            mixed.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "mixed content OR must stay intact as a residual predicate"
        );
        chunks.lock().unwrap().clear();
        let volatile = context
            .sql(&format!(
                "SELECT to_content FROM changes WHERE id = '{id}' AND random() > 1 LIMIT 1"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(volatile.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert!(
            chunks.lock().unwrap().iter().all(|key| key == &allowed[1]),
            "metadata conjunct remains bounded beside a volatile residual"
        );
        let large = context
            .sql("SELECT to_content FROM changes WHERE to_path = '/unrelated.asset'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(large.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let bytes = large
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(bytes.value(0).len(), 40 * 1024 * 1024);
        assert!(bytes.value(0).iter().all(|byte| *byte == 0xab));
        lix.close().await.unwrap();
    }
    #[tokio::test]
    async fn ancestor_moves_are_file_and_directory_changes_for_working_history_and_filtered_counts()
    {
        use crate::{Value, open_lix};
        let lix = open_lix()
            .with_storage(crate::storage::Memory::new())
            .await
            .expect("open memory repository");
        let file_id = "0193182b-2a72-7ed5-9015-76bf271af333";
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/docs/nested/a.txt', $2)",
            &[
                Value::Text(file_id.into()),
                Value::Blob(b"same bytes".to_vec().into()),
            ],
        )
        .await
        .expect("create nested file");
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/other.txt', $1)",
            &[Value::Blob(b"unaffected".to_vec().into())],
        )
        .await
        .expect("create unrelated file");
        let nested = lix
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/docs/nested'",
                &[],
            )
            .await
            .unwrap();
        let directory_id = nested.rows()[0].get::<String>("id").unwrap();
        let checkpoint = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .expect("baseline checkpoint");
        let baseline = checkpoint.rows()[0].get::<String>("commit_id").unwrap();
        lix.execute(
            "UPDATE lix_directory SET path = '/renamed' WHERE path = '/docs'",
            &[],
        )
        .await
        .expect("move ancestor");
        for query in [
            "SELECT count(*) AS n FROM lix_diff('lix_file')".to_owned(),
            format!("SELECT count(*) AS n FROM lix_diff('lix_file') WHERE id = '{file_id}'"),
        ] {
            let result = lix.execute(&query, &[]).await.expect("path-only count");
            assert_eq!(result.rows()[0].get::<i64>("n").unwrap(), 1);
        }
        for (filter, expected_rows) in [
            (String::new(), 2),
            (format!(" WHERE id = '{directory_id}'"), 1),
        ] {
            let result = lix
                .execute(
                    &format!("SELECT count(*) AS n FROM lix_diff('lix_directory'){filter}"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(result.rows()[0].get::<i64>("n").unwrap(), expected_rows);
        }
        let target = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .expect("seal ancestor move");
        let target = target.rows()[0].get::<String>("commit_id").unwrap();
        for source in [
            format!("lix_diff('lix_file', '{baseline}', '{target}')"),
            format!("lix_history('lix_file', '{target}')"),
        ] {
            let count_query = format!(
                "SELECT count(*) AS n FROM {source} WHERE lixcol_to_commit_id = '{target}'"
            );
            let count = lix
                .execute(&count_query, &[])
                .await
                .expect("projection-independent historical count");
            assert_eq!(count.rows()[0].get::<i64>("n").unwrap(), 1);

            let query = format!(
                "SELECT id, diff_type, from_path, to_path FROM {source} WHERE id = '{file_id}' AND lixcol_to_commit_id = '{target}'"
            );
            let result = lix
                .execute(&query, &[])
                .await
                .expect("historical ancestor path difference");
            assert_eq!(result.rows().len(), 1);
            let rows = result.rows();
            assert_eq!(rows[0].get::<String>("diff_type").unwrap(), "modified");

            assert_eq!(
                rows[0].get::<String>("from_path").unwrap(),
                "/docs/nested/a.txt"
            );
            assert_eq!(
                rows[0].get::<String>("to_path").unwrap(),
                "/renamed/nested/a.txt"
            );
        }
        for source in [
            format!("lix_diff('lix_directory', '{baseline}', '{target}')"),
            format!("lix_history('lix_directory', '{target}')"),
        ] {
            let count = lix
                .execute(
                    &format!(
                        "SELECT count(*) AS n FROM {source} WHERE lixcol_to_commit_id = '{target}'"
                    ),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(count.rows()[0].get::<i64>("n").unwrap(), 2);

            let result = lix.execute(&format!("SELECT diff_type, from_path, to_path FROM {source} WHERE id = '{directory_id}' AND lixcol_to_commit_id = '{target}'"), &[]).await.unwrap();
            assert_eq!(result.rows().len(), 1);
            let rows = result.rows();
            assert_eq!(rows[0].get::<String>("diff_type").unwrap(), "modified");

            assert_eq!(rows[0].get::<String>("from_path").unwrap(), "/docs/nested");
            assert_eq!(rows[0].get::<String>("to_path").unwrap(), "/renamed/nested");
        }
        lix.close().await.expect("close repository");
    }

    #[tokio::test]
    async fn historical_path_pushdown_uses_both_endpoints_and_reuses_indexes() {
        use crate::{Value, open_lix};
        let lix = open_lix()
            .with_storage(crate::storage::Memory::new())
            .await
            .unwrap();
        for path in ["/old/a.txt", "/replace.txt", "/unrelated.txt"] {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(path.into()),
                    Value::Blob(b"original".to_vec().into()),
                ],
            )
            .await
            .unwrap();
        }
        let before = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        lix.execute(
            "UPDATE lix_directory SET path = '/new' WHERE path = '/old'",
            &[],
        )
        .await
        .unwrap();
        lix.execute("DELETE FROM lix_file WHERE path = '/replace.txt'", &[])
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/replace.txt', $1)",
            &[Value::Blob(b"replacement".to_vec().into())],
        )
        .await
        .unwrap();
        let after = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        // Make current paths disagree with both historical endpoints.
        lix.execute(
            "UPDATE lix_directory SET path = '/later' WHERE path = '/new'",
            &[],
        )
        .await
        .unwrap();
        lix.execute("DELETE FROM lix_file WHERE path = '/replace.txt'", &[])
            .await
            .unwrap();
        let source = format!("lix_diff('lix_file', '{before}', '{after}')");
        paths::INDEX_BUILDS.with(|n| n.set(0));
        for (predicate, expected) in [
            (
                "from_path IN ('/old/a.txt', '/replace.txt') OR to_path IN ('/new/a.txt', '/replace.txt')",
                3,
            ),
            ("from_path = '/old/a.txt' AND to_path = '/new/a.txt'", 1),
            ("from_path = '/old/a.txt' AND to_path = '/replace.txt'", 0),
            ("from_path = '/replace.txt' OR to_path = '/replace.txt'", 2),
            ("from_path = '/missing' OR to_path = '/missing'", 0),
            ("from_path = '/old/a.txt' OR diff_type = 'added'", 2),
            ("to_path IN ('/new/a.txt', NULL)", 1),
        ] {
            let result = lix.execute(&format!("SELECT id, from_path, to_path, from_content, to_content FROM {source} WHERE {predicate}"), &[]).await.unwrap();
            assert_eq!(result.rows().len(), expected, "{predicate}");
        }
        let parameterized = lix.execute(
            "SELECT id FROM lix_diff('lix_file', $1, $2) WHERE from_path IN ($3) OR to_path IN ($4)",
            &[Value::Text(before.clone()), Value::Text(after.clone()),
              Value::Text("/old/a.txt".into()), Value::Text("/replace.txt".into())],
        ).await.unwrap();
        assert_eq!(parameterized.rows().len(), 2);
        // Two indexes, independent of the number of predicates or projections.
        assert_eq!(paths::INDEX_BUILDS.with(|n| n.get()), 2);
        let result = lix
            .execute(
                &format!("SELECT from_path, to_path FROM {source} WHERE from_path = '/old/a.txt'"),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.rows()[0].get::<String>("to_path").unwrap(),
            "/new/a.txt"
        );
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn added_directory_does_not_expand_unchanged_file_paths() {
        use crate::{Value, open_lix};
        let lix = open_lix()
            .with_storage(crate::storage::Memory::new())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/existing/a.txt', $1)",
            &[Value::Blob(b"old".to_vec().into())],
        )
        .await
        .unwrap();
        let before = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/archive/b.txt', $1)",
            &[Value::Blob(b"new".to_vec().into())],
        )
        .await
        .unwrap();
        let after = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        PATH_EXPANSIONS.with(|n| n.set(0));
        let result = lix
            .execute(
                &format!("SELECT count(*) AS n FROM lix_diff('lix_file', '{before}', '{after}')"),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.rows()[0].get::<i64>("n").unwrap(), 1);
        assert_eq!(PATH_EXPANSIONS.with(|n| n.get()), 0);
        lix.close().await.unwrap();
    }

    #[test]
    fn relation_diff_schema_pairs_public_columns_and_retires_legacy_columns() {
        let relation = DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_key_value")
            .expect("key/value relation is registered");
        let names = relation
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            &names[..5],
            &["row_ref", "key", "diff_type", "from_value", "to_value"]
        );
        assert!(field_is_row_ref(
            relation.schema.field_with_name("row_ref").unwrap()
        ));
        assert!(!names.contains(&"from_key"));
        assert!(!names.contains(&"to_key"));
        assert_eq!(
            &names[names.len() - 2..],
            &["lixcol_from_commit_id", "lixcol_to_commit_id"]
        );
        assert!(!names.contains(&"lixcol_diff_type"));
        assert!(!names.contains(&"lixcol_row_count"));
        assert!(!names.contains(&"row_count"));
        assert!(!names.contains(&"diff_id"));
        assert!(!names.contains(&"before_change_id"));
        assert!(!names.contains(&"after_change_id"));
        assert!(
            relation
                .schema
                .field_with_name("from_value")
                .unwrap()
                .is_nullable()
        );
        assert!(field_is_json(
            relation.schema.field_with_name("from_value").unwrap()
        ));

        for name in names
            .into_iter()
            .filter(|name| name.starts_with("from_lixcol_") || name.starts_with("to_lixcol_"))
        {
            let system_name = name
                .strip_prefix("from_")
                .or_else(|| name.strip_prefix("to_"))
                .unwrap_or(name);
            assert!(
                crate::sql2::catalog::TRACKED_ROW_SYSTEM_COLUMN_NAMES.contains(&system_name),
                "diff exposes relation payload with a lixcol name: {name}"
            );
        }
    }

    #[test]
    fn default_range_diff_never_surfaces_a_history_demand() {
        let error = hot_only_diff_error(lix_error_to_datafusion_error(crate::LixError::new(
            "LIX_SYNC_HISTORY_REQUIRED",
            "history is deferred",
        )));
        let error = datafusion_error_to_lix_error(error);
        assert_eq!(error.code, "LIX_DIFF_HOT_UNAVAILABLE");
        assert!(!error.message.contains("history is deferred"));
    }

    #[test]
    fn default_range_diff_preserves_exact_hot_chunk_demand() {
        let error = hot_only_diff_error(lix_error_to_datafusion_error(
            crate::LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunks are deferred")
                .with_details(serde_json::json!({ "chunkIds": ["a".repeat(64)] })),
        ));
        let error = datafusion_error_to_lix_error(error);
        assert_eq!(error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        assert_eq!(
            error.details(),
            Some(&serde_json::json!({ "chunkIds": ["a".repeat(64)] }))
        );
    }

    #[test]
    fn default_range_diff_preserves_coherent_read_retry_errors() {
        for code in [
            crate::LixError::CODE_STORAGE_READ_EXPIRED,
            crate::LixError::CODE_TRANSACTION_CONFLICT,
        ] {
            let error = hot_only_diff_error(lix_error_to_datafusion_error(
                crate::LixError::new(code, "concurrent publication")
                    .with_details(serde_json::json!({ "retry": true })),
            ));
            let error = datafusion_error_to_lix_error(error);
            assert_eq!(error.code, code);
            assert_eq!(error.details(), Some(&serde_json::json!({ "retry": true })));
        }
    }

    #[test]
    fn relation_diff_rejects_non_relations() {
        let error = match DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_diff") {
            Ok(_) => panic!("table functions are not diffable relations"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not support relation"));
    }

    #[test]
    fn relation_diff_pushes_schema_identity_without_loading_payloads() {
        let relation = DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_key_value")
            .expect("key/value relation is registered");
        let projection = Schema::empty();
        let route = DiffRoute::from_filters(&[], &relation, &projection);

        assert_eq!(route.request.filter.schema_keys, vec!["lix_key_value"]);
        assert!(!route.request.retain_payloads);
        assert!(!route.contradictory);
    }

    #[test]
    fn relation_diff_pushes_typed_file_id_without_filtering_content_row_primary_keys() {
        let relation = DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file")
            .expect("file relation is registered");
        let projection = Schema::empty();
        let route = DiffRoute::from_filters(
            &[col("id").eq(lit("0193182b-2a72-7ed5-9015-76bf271af333"))],
            &relation,
            &projection,
        );

        assert_eq!(
            route.request.filter.file_ids,
            vec![
                NullableKeyFilter::Value("0193182b-2a72-7ed5-9015-76bf271af333".to_string()),
                NullableKeyFilter::Null
            ]
        );
        assert!(route.request.filter.row_pks.is_empty());
        assert!(!route.request.retain_payloads);
    }

    #[test]
    fn relation_diff_rejects_wrongly_typed_filesystem_row_refs() {
        for relation_name in ["lix_file", "lix_directory"] {
            let relation = DiffRelation::from_catalog(
                PublicCatalog::fixed_system(),
                relation_name,
            )
            .expect("filesystem relation is registered");
            let malformed = crate::row_ref::encode(
                relation_name,
                None,
                &RowPk::single("not-a-uuid"),
            )
            .expect("codec accepts catalog-independent canonical payloads");
            let route = DiffRoute::from_filters(
                &[col("row_ref").eq(lit(malformed.as_str()))],
                &relation,
                &Schema::empty(),
            );

            assert!(route.contradictory, "{relation_name} must be a no-match");
            assert!(route.request.filter.row_pks.is_empty());
        }
    }

    #[test]
    fn relation_diff_only_hydrates_projected_payload_columns() {
        let relation = DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file")
            .expect("file relation is registered");
        let identity = Schema::new(vec![Field::new("to_id", DataType::Utf8, true)]);
        let path = Schema::new(vec![Field::new("to_path", DataType::Utf8, true)]);

        assert!(
            !DiffRoute::from_filters(&[], &relation, &identity)
                .request
                .retain_payloads
        );
        assert!(
            !DiffRoute::from_filters(&[], &relation, &path)
                .request
                .retain_payloads
        );
    }
}
