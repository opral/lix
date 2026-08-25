use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Operator;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use serde_json::Value as JsonValue;

use crate::NullableKeyFilter;
use crate::branch::BranchHeadControlContext;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::hot_state::TrackedHeadContext;
use crate::plugin::runtime::WasmTypedRow;
use crate::row_pk::{RowPk, RowPkComponentType};
use crate::sql2::SqlChangelogQuerySource;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::sql2::result_metadata::{field_is_json, json_field};
use crate::sql2::udfs::{ExecutionSlots, execution_slots};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiff, TrackedStateDiffEntry, TrackedStateDiffKind,
    TrackedStateDiffRequest, TrackedStateDiffRow, TrackedStateFilter, TrackedStateKey,
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
) where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    session.register_udtf(
        "lix_diff",
        Arc::new(DiffFunction {
            store: query_source.store,
            catalog,
            slots: execution_slots(session),
        }),
    );
}

struct DiffFunction<S> {
    store: S,
    catalog: Arc<PublicCatalog>,
    slots: Arc<ExecutionSlots>,
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
        let [relation, from_commit_id, to_commit_id] = args else {
            return Err(DataFusionError::Plan(
                "lix_diff requires a relation and exactly two commit ID arguments".to_string(),
            ));
        };
        let relation_name = text_argument(relation, 1, "relation name", None)?;
        let from_commit_id = text_argument(from_commit_id, 2, "commit ID", Some(&self.slots))?;
        let to_commit_id = text_argument(to_commit_id, 3, "commit ID", Some(&self.slots))?;
        let relation = DiffRelation::from_catalog(&self.catalog, &relation_name)?;
        Ok(Arc::new(SpecTableProvider::new(Arc::new(DiffSpec {
            store: self.store.clone(),
            relation,
            from_commit_id,
            to_commit_id,
            active_branch_id: self.slots.active_branch_id(),
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
struct DiffRelation {
    kind: DiffRelationKind,
    schema: SchemaRef,
    primary_key_component_types: Vec<RowPkComponentType>,
}

#[derive(Clone, PartialEq, Eq)]
enum DiffRelationKind {
    Schema { schema_key: String },
    File,
    Directory,
}

impl DiffRelation {
    fn from_catalog(catalog: &PublicCatalog, name: &str) -> Result<Self> {
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
        let primary_key_component_types = match &kind {
            DiffRelationKind::Schema { schema_key } => catalog
                .schema_spec(schema_key)
                .map(|spec| spec.primary_key_component_types.clone())
                .unwrap_or_default(),
            DiffRelationKind::File | DiffRelationKind::Directory => {
                vec![RowPkComponentType::Uuid]
            }
        };
        let source_schema = catalog.surface_schema(name).ok_or_else(|| {
            DataFusionError::Plan(format!("lix_diff does not support relation '{name}'"))
        })?;
        let mut fields = vec![
            json_field("row_pk", false),
            Field::new("diff_type", DataType::Utf8, false),
        ];
        for column in surface.columns.iter().filter(|column| column.is_public()) {
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
        fields.push(Field::new("row_count", DataType::Int64, false));
        Ok(Self {
            kind,
            schema: Arc::new(Schema::new(fields)),
            primary_key_component_types,
        })
    }
}

struct DiffSpec<S> {
    store: S,
    relation: DiffRelation,
    from_commit_id: String,
    to_commit_id: String,
    active_branch_id: Option<String>,
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
        if filter.column_refs().iter().any(|column| {
            matches!(column.name.as_str(), "row_pk" | "from_id" | "to_id")
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
        _props: &ExecutionProps,
    ) -> Result<PlannedScan> {
        let schema = projected_schema(&self.relation.schema, projection);
        if self.relation.kind == DiffRelationKind::File
            && schema
                .fields()
                .iter()
                .any(|field| matches!(field.name().as_str(), "from_content" | "to_content"))
        {
            return Err(DataFusionError::NotImplemented(
                "lix_diff('lix_file', ...) does not support content projection; query lix_history('lix_file', commit_id) for file bytes"
                    .to_string(),
            ));
        }
        let route = DiffRoute::from_filters(filters, &self.relation, &schema);
        Ok(PlannedScan {
            schema: Arc::clone(&schema),
            ordering: None,
            source: scan_row_source(
                Arc::clone(&schema),
                (
                    self.store.clone(),
                    self.relation.clone(),
                    schema,
                    route,
                    self.from_commit_id.clone(),
                    self.to_commit_id.clone(),
                    self.active_branch_id.clone(),
                ),
                move |(
                    store,
                    relation,
                    schema,
                    route,
                    from_commit_id,
                    to_commit_id,
                    active_branch_id,
                )| async move {
                    if limit == Some(0) || route.contradictory || from_commit_id == to_commit_id {
                        return diff_record_batch(schema, &[]);
                    }
                    let mut tracked = TrackedStateContext::new().reader(store.clone());
                    let direct_diff = if !route.request.retain_payloads
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
                                    .reader(store)
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
                    let diff = if let Some(diff) = direct_diff {
                        diff
                    } else {
                        tracked
                            .diff_commits(&from_commit_id, &to_commit_id, &route.request)
                            .await
                            .map_err(lix_error_to_datafusion_error)?
                    };
                    let mut rows = match relation.kind {
                        DiffRelationKind::Schema { .. } => schema_diff_rows(diff, &schema)?,
                        DiffRelationKind::File => {
                            file_diff_rows(
                                &mut tracked,
                                diff,
                                &schema,
                                &from_commit_id,
                                &to_commit_id,
                            )
                            .await?
                        }
                        DiffRelationKind::Directory => {
                            directory_diff_rows(
                                &mut tracked,
                                diff,
                                &schema,
                                &from_commit_id,
                                &to_commit_id,
                            )
                            .await?
                        }
                    };
                    if let Some(limit) = limit {
                        rows.truncate(limit);
                    }
                    diff_record_batch(schema, &rows)
                },
            ),
        })
    }
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
    request: TrackedStateDiffRequest,
    contradictory: bool,
}

impl DiffRoute {
    fn from_filters(filters: &[Expr], relation: &DiffRelation, projection: &Schema) -> Self {
        let conjuncts = filter_conjuncts(filters);
        let row_pk_values = optional_values(&conjuncts, "row_pk");
        let extracted_row_pk_values = extracted_first_row_pk_values(&conjuncts);
        let id_values =
            optional_values(&conjuncts, "from_id").or_else(|| optional_values(&conjuncts, "to_id"));
        let explicit_row_filter = row_pk_values.is_some();
        let mut contradictory = row_pk_values.as_ref().is_some_and(Vec::is_empty)
            || id_values.as_ref().is_some_and(Vec::is_empty)
            || extracted_row_pk_values.as_ref().is_some_and(Vec::is_empty);
        let mut row_pks = row_pk_values
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| {
                let JsonValue::Array(values) = serde_json::from_str(&value).ok()? else {
                    return None;
                };
                RowPk::from_json_values(&values, &relation.primary_key_component_types).ok()
            })
            .collect::<Vec<_>>();
        contradictory |= explicit_row_filter && row_pks.is_empty();
        let mut schema_keys = Vec::new();
        let mut file_ids = Vec::new();
        match &relation.kind {
            DiffRelationKind::Schema { schema_key } => {
                schema_keys.push(schema_key.clone());
                if relation.primary_key_component_types.len() == 1
                    && let Some(values) = extracted_row_pk_values.as_ref()
                {
                    let extracted_keys = values
                        .iter()
                        .filter_map(|value| {
                            RowPk::from_json_values(
                                &[JsonValue::String(value.clone())],
                                &relation.primary_key_component_types,
                            )
                            .ok()
                        })
                        .collect::<Vec<_>>();
                    if row_pks.is_empty() {
                        row_pks = extracted_keys;
                    } else {
                        row_pks.retain(|row_pk| extracted_keys.contains(row_pk));
                    }
                    contradictory |= row_pks.is_empty();
                }
                if let Some(ids) = optional_values(&conjuncts, "from_lixcol_file_id")
                    .or_else(|| optional_values(&conjuncts, "to_lixcol_file_id"))
                {
                    file_ids.extend(ids.into_iter().map(NullableKeyFilter::Value));
                }
            }
            DiffRelationKind::File => {
                let mut ids = id_values.unwrap_or_default();
                if let Some(values) = extracted_row_pk_values {
                    ids.extend(values);
                }
                for row_pk in row_pks.drain(..) {
                    if let Ok(JsonValue::Array(values)) = row_pk.as_json_array_value()
                        && let [JsonValue::String(id)] = values.as_slice()
                    {
                        ids.push(id.clone());
                    } else {
                        contradictory = true;
                    }
                }
                file_ids.extend(ids.into_iter().map(NullableKeyFilter::Value));
            }
            DiffRelationKind::Directory => {
                schema_keys.push(DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string());
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
                        "id" | "lixcol_row_pk"
                            | "lixcol_schema_key"
                            | "lixcol_file_id"
                            | "lixcol_created_at"
                            | "lixcol_updated_at"
                            | "lixcol_change_id"
                            | "lixcol_commit_id"
                            | "lixcol_global"
                            | "lixcol_untracked"
                    )
                });
        Self {
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

fn extracted_first_row_pk_values(conjuncts: &[Expr]) -> Option<Vec<String>> {
    let mut values: Option<Vec<String>> = None;
    for conjunct in conjuncts {
        let Expr::BinaryExpr(binary) = conjunct else {
            continue;
        };
        if binary.op != Operator::Eq {
            continue;
        }
        let extracted = [
            (binary.left.as_ref(), binary.right.as_ref()),
            (binary.right.as_ref(), binary.left.as_ref()),
        ]
        .into_iter()
        .find_map(|(expression, expected)| {
            let Expr::ScalarFunction(function) = strip_cast(expression) else {
                return None;
            };
            if function.func.name() != "__lix_json_get_text" || function.args.len() != 2 {
                return None;
            }
            let Expr::Column(column) = strip_cast(&function.args[0]) else {
                return None;
            };
            if column.name != "row_pk" {
                return None;
            }
            let Expr::Literal(index, _) = strip_cast(&function.args[1]) else {
                return None;
            };
            if index.to_string() != "0" {
                return None;
            }
            let Expr::Literal(value, _) = strip_cast(expected) else {
                return None;
            };
            value.try_as_str().flatten().map(str::to_string)
        });
        if let Some(value) = extracted {
            match values.as_mut() {
                None => values = Some(vec![value]),
                Some(existing) => existing.retain(|existing| existing == &value),
            }
        }
    }
    values
}

fn strip_cast(mut expression: &Expr) -> &Expr {
    while let Expr::Cast(cast) = expression {
        expression = cast.expr.as_ref();
    }
    expression
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

#[derive(Clone)]
struct DiffSqlRow {
    row_pk: RowPk,
    diff_type: &'static str,
    row_count: i64,
    from: Option<DiffSide>,
    to: Option<DiffSide>,
}

#[derive(Clone)]
struct DiffSide {
    id: Option<String>,
    schema_key: String,
    global: bool,
    file_id: Option<String>,
    row_pk: RowPk,
    created_at: String,
    updated_at: String,
    change_id: String,
    commit_id: String,
    metadata: Option<JsonValue>,
    snapshot: Option<Arc<WasmTypedRow>>,
    path: Option<String>,
}

fn schema_diff_rows(diff: TrackedStateDiff, projection: &Schema) -> Result<Vec<DiffSqlRow>> {
    let needs_side = projection
        .fields()
        .iter()
        .any(|field| side_column(field.name()).is_some());
    diff.entries
        .iter()
        .map(|entry| {
            Ok(DiffSqlRow {
                row_pk: entry.identity.row_pk().clone(),
                diff_type: diff_type(entry.kind),
                row_count: 1,
                from: if needs_side {
                    diff_side(entry, entry.visible_before(), &diff)?
                } else {
                    None
                },
                to: if needs_side {
                    diff_side(
                        entry,
                        entry.after.as_ref().filter(|row| !row.deleted),
                        &diff,
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
        schema_key: entry.identity.schema_key().to_string(),
        global: entry.identity.schema_key() == crate::checkpoint::CHECKPOINT_SCHEMA_KEY,
        file_id: entry.identity.file_id().map(str::to_string),
        row_pk: entry.identity.row_pk().clone(),
        created_at: row.created_at.to_string(),
        updated_at: row.updated_at.to_string(),
        change_id: row.change_id.to_string(),
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
    row_count: usize,
    descriptor: Option<&'a TrackedStateDiffEntry>,
}

async fn file_diff_rows<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    diff: TrackedStateDiff,
    projection: &Schema,
    from_commit_id: &str,
    to_commit_id: &str,
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
            groups.entry(file_id.into_owned()).or_insert(FileDiffGroup {
                row_count: 0,
                descriptor: None,
            })
        };
        if entry.identity.schema_key() == FILE_DESCRIPTOR_SCHEMA_KEY {
            group.descriptor = Some(entry);
        }
        group.row_count += 1;
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
                "id" | "lixcol_row_pk"
                    | "lixcol_schema_key"
                    | "lixcol_file_id"
                    | "lixcol_created_at"
                    | "lixcol_updated_at"
                    | "lixcol_change_id"
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
        let mut from = if needs_side {
            materialized_side(from_descriptors.row(index))?
        } else {
            None
        };
        let mut to = if needs_side {
            materialized_side(to_descriptors.row(index))?
        } else {
            None
        };
        if needs_paths {
            if let Some(side) = from.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        from_commit_id,
                        side,
                        false,
                        &mut from_directory_cache,
                    )
                    .await?,
                );
            }
            if let Some(side) = to.as_mut() {
                side.path = Some(
                    filesystem_path(tracked, to_commit_id, side, false, &mut to_directory_cache)
                        .await?,
                );
            }
        }
        rows.push(DiffSqlRow {
            row_pk,
            diff_type: diff_type(kind),
            row_count: i64::try_from(group.row_count).map_err(|_| {
                DataFusionError::Execution("lix_diff row_count exceeds INT8".to_string())
            })?,
            from,
            to,
        });
    }
    Ok(rows)
}

async fn directory_diff_rows<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    diff: TrackedStateDiff,
    projection: &Schema,
    from_commit_id: &str,
    to_commit_id: &str,
) -> Result<Vec<DiffSqlRow>>
where
    S: StorageAdapterRead,
{
    let needs_paths = projection
        .fields()
        .iter()
        .any(|field| matches!(field.name().as_str(), "from_path" | "to_path"));
    let mut rows = schema_diff_rows(diff, projection)?;
    if needs_paths {
        let mut from_directory_cache = HashMap::new();
        let mut to_directory_cache = HashMap::new();
        for row in &mut rows {
            if let Some(side) = row.from.as_mut() {
                side.path = Some(
                    filesystem_path(
                        tracked,
                        from_commit_id,
                        side,
                        true,
                        &mut from_directory_cache,
                    )
                    .await?,
                );
            }
            if let Some(side) = row.to.as_mut() {
                side.path = Some(
                    filesystem_path(tracked, to_commit_id, side, true, &mut to_directory_cache)
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

fn materialized_side(
    row: Option<crate::tracked_state::MaterializedTrackedStateRowRef<'_>>,
) -> Result<Option<DiffSide>> {
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
        schema_key: row.schema_key().to_string(),
        global: row.schema_key() == crate::checkpoint::CHECKPOINT_SCHEMA_KEY,
        file_id: row.file_id().map(str::to_string),
        row_pk: row.row_pk().clone(),
        created_at: row.created_at().to_string(),
        updated_at: row.updated_at().to_string(),
        change_id: row.change_id().to_string(),
        commit_id: row.commit_id().to_string(),
        metadata,
        snapshot: row.decoded_snapshot().cloned(),
        path: None,
    }))
}

async fn filesystem_path<S>(
    tracked: &mut TrackedStateStoreReader<S>,
    commit_id: &str,
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
        let batch = tracked
            .load_projected_batch_at_commit(commit_id, &[key], &ChangeRecordProjection::full())
            .await
            .map_err(lix_error_to_datafusion_error)?;
        let parent = batch.row(0).filter(|row| !row.deleted()).ok_or_else(|| {
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

fn diff_record_batch(schema: SchemaRef, rows: &[DiffSqlRow]) -> Result<RecordBatch> {
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
        .map(|field| diff_column_array(field, rows))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)
}

fn diff_column_array(field: &Field, rows: &[DiffSqlRow]) -> Result<ArrayRef> {
    match field.name().as_str() {
        "row_pk" => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.row_pk
                        .as_json_array_text()
                        .map_err(lix_error_to_datafusion_error)
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        "diff_type" => Ok(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.diff_type),
        ))),
        "row_count" => Ok(Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.row_count),
        ))),
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

fn side_value(side: Option<&DiffSide>, column: &str) -> Result<Option<lix_schema::Value>> {
    let Some(side) = side else {
        return Ok(None);
    };
    Ok(match column {
        "id" => side.id.clone().map(lix_schema::Value::Text),
        "path" => side.path.clone().map(lix_schema::Value::Text),
        "lixcol_row_pk" => Some(lix_schema::Value::Jsonb(
            side.row_pk
                .as_json_array_value()
                .map_err(lix_error_to_datafusion_error)?
                .into(),
        )),
        "lixcol_schema_key" => Some(lix_schema::Value::Text(side.schema_key.clone())),
        "lixcol_file_id" => side.file_id.clone().map(lix_schema::Value::Text),
        "lixcol_created_at" => Some(lix_schema::Value::Text(side.created_at.clone())),
        "lixcol_updated_at" => Some(lix_schema::Value::Text(side.updated_at.clone())),
        "lixcol_change_id" => Some(lix_schema::Value::Text(side.change_id.clone())),
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
            &names[..6],
            &[
                "row_pk",
                "diff_type",
                "from_key",
                "to_key",
                "from_value",
                "to_value"
            ]
        );
        assert_eq!(names.last(), Some(&"row_count"));
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
        let projection = Schema::new(vec![Field::new("row_count", DataType::Int64, false)]);
        let route = DiffRoute::from_filters(&[], &relation, &projection);

        assert_eq!(route.request.filter.schema_keys, vec!["lix_key_value"]);
        assert!(!route.request.retain_payloads);
        assert!(!route.contradictory);
    }

    #[test]
    fn relation_diff_pushes_file_id_without_filtering_content_row_primary_keys() {
        let relation = DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file")
            .expect("file relation is registered");
        let projection = Schema::empty();
        let route = DiffRoute::from_filters(
            &[col("to_id").eq(lit("0193182b-2a72-7ed5-9015-76bf271af333"))],
            &relation,
            &projection,
        );

        assert_eq!(
            route.request.filter.file_ids,
            vec![NullableKeyFilter::Value(
                "0193182b-2a72-7ed5-9015-76bf271af333".to_string()
            )]
        );
        assert!(route.request.filter.row_pks.is_empty());
        assert!(!route.request.retain_payloads);
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
