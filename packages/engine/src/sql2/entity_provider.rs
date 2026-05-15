use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::{create_physical_expr, EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures_util::{stream, TryStreamExt};
use serde_json::Value as JsonValue;

use crate::commit_graph::CommitGraphReader;
use crate::entity_identity::EntityIdentity;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{
    LiveStateFilter, LiveStateProjection, LiveStateReader, LiveStateScanRequest,
};
use crate::sql2::dml::{InsertExec, InsertSink};
use crate::sql2::predicate_typecheck::validate_json_predicate_filters;
use crate::sql2::read_only::reject_read_only_entity_surface;
use crate::sql2::version_scope::{
    explicit_version_ids_from_dml_filters, resolve_provider_version_ids,
    resolve_write_version_scope, VersionBinding,
};
use crate::sql2::write_normalization::{
    InsertCell, InsertColumnIntents, SqlCell, UpdateAssignmentValues, UpdateCell,
};
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::version::VersionRefReader;
use crate::{parse_row_metadata_value, serialize_row_metadata, LixError};

use super::entity_history_provider::EntityHistoryProvider;
use super::history_route::{
    HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_ID,
    HISTORY_COL_FILE_ID, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT, HISTORY_COL_START_COMMIT_ID,
};
use super::result_metadata::{json_field, mark_json_field};
use crate::sql2::{
    SqlCommitStoreQuerySource, SqlWriteContext, WriteAccess, WriteContextLiveStateReader,
    WriteContextVersionRefReader,
};
use crate::transaction::types::{TransactionWrite, TransactionWriteMode};

pub(crate) async fn register_entity_providers(
    ctx: &SessionContext,
    active_version_id: &str,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    commit_graph: Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>,
    query_source: SqlCommitStoreQuerySource,
    schema_definitions: &[JsonValue],
) -> Result<(), LixError> {
    for schema in schema_definitions {
        let spec = match derive_entity_surface_spec_from_schema(schema) {
            Ok(spec) => Arc::new(spec),
            Err(_) => continue,
        };

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            continue;
        }

        let by_version_name = format!("{}_by_version", spec.schema_key);
        ctx.register_table(
            &by_version_name,
            Arc::new(EntityProvider::by_version(
                Arc::clone(&spec),
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        ctx.register_table(
            &spec.schema_key,
            Arc::new(EntityProvider::active(
                Arc::clone(&spec),
                Arc::clone(&live_state),
                Arc::clone(&version_ref),
                active_version_id.to_string(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        if schema_exposed_as_entity_history_surface(&spec.schema_key) {
            let history_name = format!("{}_history", spec.schema_key);
            ctx.register_table(
                &history_name,
                Arc::new(EntityHistoryProvider::new(
                    Arc::clone(&spec),
                    Arc::clone(&commit_graph),
                    query_source.clone(),
                )),
            )
            .map_err(datafusion_error_to_lix_error)?;
        }
    }

    Ok(())
}

pub(crate) async fn register_entity_write_providers(
    ctx: &SessionContext,
    write_ctx: SqlWriteContext,
    schema_definitions: &[JsonValue],
) -> Result<(), LixError> {
    for schema in schema_definitions {
        let spec = match derive_entity_surface_spec_from_schema(schema) {
            Ok(spec) => Arc::new(spec),
            Err(_) => continue,
        };

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            continue;
        }

        let by_version_name = format!("{}_by_version", spec.schema_key);
        ctx.register_table(
            &by_version_name,
            Arc::new(EntityProvider::by_version_with_write(
                Arc::clone(&spec),
                write_ctx.clone(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;

        ctx.register_table(
            &spec.schema_key,
            Arc::new(EntityProvider::active_with_write(
                Arc::clone(&spec),
                write_ctx.clone(),
            )),
        )
        .map_err(datafusion_error_to_lix_error)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntityProviderVariant {
    Active,
    ByVersion,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntityColumnType {
    String,
    Json,
    Integer,
    Number,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntitySurfaceColumn {
    pub(super) name: String,
    pub(super) column_type: EntityColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntitySurfaceSpec {
    pub(super) schema_key: String,
    pub(super) primary_key_paths: Vec<Vec<String>>,
    pub(super) columns: Vec<EntitySurfaceColumn>,
}

impl EntitySurfaceSpec {
    #[cfg(test)]
    fn visible_column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name.as_str())
    }

    pub(super) fn visible_column(&self, column_name: &str) -> Option<&EntitySurfaceColumn> {
        self.columns
            .iter()
            .find(|column| column.name == column_name)
    }

    fn is_visible_column(&self, column_name: &str) -> bool {
        self.visible_column(column_name).is_some()
    }
}

pub(crate) struct EntityProvider {
    spec: Arc<EntitySurfaceSpec>,
    live_state: Arc<dyn LiveStateReader>,
    version_ref: Arc<dyn VersionRefReader>,
    write_access: WriteAccess,
    schema: SchemaRef,
    variant: EntityProviderVariant,
    version_binding: VersionBinding,
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
        version_ref: Arc<dyn VersionRefReader>,
        active_version_id: String,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::Active),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::read_only(),
            variant: EntityProviderVariant::Active,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    fn active_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let active_version_id = write_ctx.active_version_id();
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::Active),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::write(write_ctx),
            variant: EntityProviderVariant::Active,
            version_binding: VersionBinding::active(active_version_id),
        }
    }

    fn by_version(
        spec: Arc<EntitySurfaceSpec>,
        live_state: Arc<dyn LiveStateReader>,
        version_ref: Arc<dyn VersionRefReader>,
    ) -> Self {
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::ByVersion),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::read_only(),
            variant: EntityProviderVariant::ByVersion,
            version_binding: VersionBinding::explicit(),
        }
    }

    fn by_version_with_write(spec: Arc<EntitySurfaceSpec>, write_ctx: SqlWriteContext) -> Self {
        let live_state = Arc::new(WriteContextLiveStateReader::new(write_ctx.clone()));
        let version_ref = Arc::new(WriteContextVersionRefReader::new(write_ctx.clone()));
        Self {
            schema: entity_surface_schema(&spec, EntityProviderVariant::ByVersion),
            spec,
            live_state,
            version_ref,
            write_access: WriteAccess::write(write_ctx),
            variant: EntityProviderVariant::ByVersion,
            version_binding: VersionBinding::explicit(),
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
                if ExactVersionIdFilterAnalyzer.supports(filter) || analyzer.supports(filter) {
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
            self.version_binding.active_version_id(),
            Some(projected_schema.as_ref()),
            limit,
        );
        request.filter.version_ids = resolve_provider_version_ids(
            self.version_ref.as_ref(),
            &self.version_binding,
            request.filter.version_ids,
        )
        .await
        .map_err(lix_error_to_datafusion_error)?;
        apply_exact_version_id_filter(&mut request, exact_version_ids_from_filters(filters)?);
        apply_exact_entity_id_filters(&mut request, &self.spec, filters)?;

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

fn entity_ids_from_primary_key_filters(
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<Option<Vec<EntityIdentity>>> {
    let analyzer = EntityPrimaryKeyFilterAnalyzer::new(spec);
    let mut entity_ids: Option<BTreeSet<EntityIdentity>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze(filter)? else {
            continue;
        };
        entity_ids = Some(match entity_ids {
            Some(existing_ids) => existing_ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }

    Ok(entity_ids.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_entity_id_filters(
    request: &mut LiveStateScanRequest,
    spec: &EntitySurfaceSpec,
    filters: &[Expr],
) -> Result<()> {
    if let Some(entity_ids) = entity_ids_from_primary_key_filters(spec, filters)? {
        if entity_ids.is_empty() {
            request.limit = Some(0);
        }
        request.filter.entity_ids = entity_ids;
    }
    Ok(())
}

fn exact_version_ids_from_filters(filters: &[Expr]) -> Result<Option<Vec<String>>> {
    let analyzer = ExactVersionIdFilterAnalyzer;
    let mut version_ids: Option<BTreeSet<String>> = None;
    for filter in filters {
        let Some(filter_ids) = analyzer.analyze(filter)? else {
            continue;
        };
        version_ids = Some(match version_ids {
            Some(existing_ids) => existing_ids.intersection(&filter_ids).cloned().collect(),
            None => filter_ids,
        });
    }
    Ok(version_ids.map(|ids| ids.into_iter().collect()))
}

fn apply_exact_version_id_filter(
    request: &mut LiveStateScanRequest,
    version_ids: Option<Vec<String>>,
) {
    if let Some(version_ids) = version_ids {
        if version_ids.is_empty() {
            request.limit = Some(0);
        }
        request.filter.version_ids = version_ids;
    }
}

struct EntityPrimaryKeyFilterAnalyzer<'a> {
    primary_key_columns: Vec<&'a str>,
}

struct ExactVersionIdFilterAnalyzer;

impl ExactVersionIdFilterAnalyzer {
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
                Ok(version_id_from_binary_filter(binary_expr).map(|value| BTreeSet::from([value])))
            }
            Expr::InList(in_list) => {
                Ok(version_ids_from_in_list_filter(in_list)
                    .map(|values| values.into_iter().collect()))
            }
            _ => Ok(None),
        }
    }
}

fn version_id_from_binary_filter(binary_expr: &BinaryExpr) -> Option<String> {
    if binary_expr.op != Operator::Eq {
        return None;
    }

    version_id_from_column_literal_filter(&binary_expr.left, &binary_expr.right)
        .or_else(|| version_id_from_column_literal_filter(&binary_expr.right, &binary_expr.left))
}

fn version_ids_from_in_list_filter(in_list: &InList) -> Option<Vec<String>> {
    if in_list.negated {
        return None;
    }
    let Expr::Column(column) = in_list.expr.as_ref() else {
        return None;
    };
    if column.name != "lixcol_version_id" {
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

fn version_id_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
) -> Option<String> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    if column.name != "lixcol_version_id" {
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

    fn analyze(&self, expr: &Expr) -> Result<Option<BTreeSet<EntityIdentity>>> {
        if self.primary_key_columns.is_empty() {
            return Ok(None);
        };
        let Some(constraint) = self.analyze_constraint(expr)? else {
            return Ok(None);
        };
        Ok(constraint.into_entity_ids(&self.primary_key_columns))
    }

    fn analyze_constraint(&self, expr: &Expr) -> Result<Option<EntityIdentityConstraint>> {
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
                let Some(left_ids) = left.into_entity_ids(&self.primary_key_columns) else {
                    return Ok(None);
                };
                let Some(mut right_ids) = right.into_entity_ids(&self.primary_key_columns) else {
                    return Ok(None);
                };
                right_ids.extend(left_ids);
                Ok(Some(EntityIdentityConstraint::Full(right_ids)))
            }
            Expr::BinaryExpr(binary_expr) => Ok(entity_identity_constraint_from_binary_filter(
                binary_expr,
                &self.primary_key_columns,
            )),
            Expr::InList(in_list) => Ok(entity_identity_constraint_from_in_list_filter(
                in_list,
                &self.primary_key_columns,
            )),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EntityIdentityConstraint {
    Full(BTreeSet<EntityIdentity>),
    Parts(BTreeMap<String, BTreeSet<String>>),
}

impl EntityIdentityConstraint {
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

    fn into_entity_ids(self, primary_key_columns: &[&str]) -> Option<BTreeSet<EntityIdentity>> {
        match self {
            Self::Full(ids) => Some(ids),
            Self::Parts(parts) => entity_ids_from_primary_key_parts(primary_key_columns, parts),
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

fn entity_identity_constraint_from_binary_filter(
    binary_expr: &BinaryExpr,
    primary_key_columns: &[&str],
) -> Option<EntityIdentityConstraint> {
    if binary_expr.op != Operator::Eq {
        return None;
    }
    entity_identity_constraint_from_column_literal_filter(
        &binary_expr.left,
        &binary_expr.right,
        primary_key_columns,
    )
    .or_else(|| {
        entity_identity_constraint_from_column_literal_filter(
            &binary_expr.right,
            &binary_expr.left,
            primary_key_columns,
        )
    })
}

fn entity_identity_constraint_from_in_list_filter(
    in_list: &InList,
    primary_key_columns: &[&str],
) -> Option<EntityIdentityConstraint> {
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
        "lixcol_entity_id" => values
            .into_iter()
            .map(|value| EntityIdentity::from_json_array_text(&value).ok())
            .collect::<Option<BTreeSet<_>>>()
            .map(EntityIdentityConstraint::Full),
        column_name if primary_key_columns.contains(&column_name) => {
            Some(EntityIdentityConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                values.into_iter().collect(),
            )])))
        }
        _ => None,
    }
}

fn entity_identity_constraint_from_column_literal_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    primary_key_columns: &[&str],
) -> Option<EntityIdentityConstraint> {
    let Expr::Column(column) = column_expr else {
        return None;
    };
    let value = string_expr_literal(literal_expr)?;
    match column.name.as_str() {
        "lixcol_entity_id" => EntityIdentity::from_json_array_text(&value)
            .ok()
            .map(|identity| EntityIdentityConstraint::Full(BTreeSet::from([identity]))),
        column_name if primary_key_columns.contains(&column_name) => {
            Some(EntityIdentityConstraint::Parts(BTreeMap::from([(
                column_name.to_string(),
                BTreeSet::from([value]),
            )])))
        }
        _ => None,
    }
}

fn entity_ids_from_primary_key_parts(
    primary_key_columns: &[&str],
    parts: BTreeMap<String, BTreeSet<String>>,
) -> Option<BTreeSet<EntityIdentity>> {
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
            .map(|parts| EntityIdentity { parts })
            .collect(),
    )
}

fn identity_matches_parts(
    identity: &EntityIdentity,
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

struct EntityInsertSink {
    spec: Arc<EntitySurfaceSpec>,
    insert_column_intents: InsertColumnIntents,
    write_ctx: SqlWriteContext,
    version_binding: VersionBinding,
}

impl std::fmt::Debug for EntityInsertSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityInsertSink")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityInsertSink {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        _schema: SchemaRef,
        insert_column_intents: InsertColumnIntents,
        write_ctx: SqlWriteContext,
        version_binding: VersionBinding,
    ) -> Self {
        Self {
            spec,
            insert_column_intents,
            write_ctx,
            version_binding,
        }
    }
}

impl DisplayAs for EntityInsertSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EntityInsertSink(schema_key={})", self.spec.schema_key)
            }
            DisplayFormatType::TreeRender => write!(f, "EntityInsertSink"),
        }
    }
}

#[async_trait]
impl InsertSink for EntityInsertSink {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(entity_lix_state_write_rows_from_batch(
                &self.spec,
                &batch,
                &self.insert_column_intents,
                self.version_binding.active_version_id(),
            )?);
        }
        let count = u64::try_from(rows.len())
            .map_err(|_| DataFusionError::Execution("entity INSERT row count overflow".into()))?;

        self.write_ctx
            .stage_write(TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows,
            })
            .await
            .map_err(lix_error_to_datafusion_error)?;

        Ok(count)
    }
}

#[allow(dead_code)]
struct EntityDeleteExec {
    spec: Arc<EntitySurfaceSpec>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    request: LiveStateScanRequest,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityDeleteExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityDeleteExec")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityDeleteExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
        request: LiveStateScanRequest,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            spec,
            write_ctx,
            table_schema,
            version_binding,
            request,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "EntityDeleteExec(schema_key={})", self.spec.schema_key)
            }
            DisplayFormatType::TreeRender => write!(f, "EntityDeleteExec"),
        }
    }
}

impl ExecutionPlan for EntityDeleteExec {
    fn name(&self) -> &str {
        "EntityDeleteExec"
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
                "EntityDeleteExec does not accept children".to_string(),
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
                "EntityDeleteExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let request = self.request.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                write_ctx
                    .scan_live_state(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let mut write_rows = entity_existing_lix_state_write_rows_from_batch(
                &spec,
                &matched_batch,
                version_binding.active_version_id(),
            )?;
            for row in &mut write_rows {
                row.snapshot = None;
            }
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity DELETE row count overflow".to_string())
            })?;

            if count > 0 {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: write_rows,
                    })
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            }

            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                dml_count_batch(Arc::clone(&stream_schema), count)?,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

#[allow(dead_code)]
struct EntityUpdateExec {
    spec: Arc<EntitySurfaceSpec>,
    write_ctx: SqlWriteContext,
    table_schema: SchemaRef,
    version_binding: VersionBinding,
    request: LiveStateScanRequest,
    assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for EntityUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityUpdateExec")
            .field("schema_key", &self.spec.schema_key)
            .finish()
    }
}

impl EntityUpdateExec {
    fn new(
        spec: Arc<EntitySurfaceSpec>,
        write_ctx: SqlWriteContext,
        table_schema: SchemaRef,
        version_binding: VersionBinding,
        request: LiveStateScanRequest,
        assignments: Vec<(String, Arc<dyn PhysicalExpr>)>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let result_schema = dml_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            spec,
            write_ctx,
            table_schema,
            version_binding,
            request,
            assignments,
            filters,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for EntityUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "EntityUpdateExec(schema_key={}, assignments={})",
                    self.spec.schema_key,
                    self.assignments.len()
                )
            }
            DisplayFormatType::TreeRender => write!(f, "EntityUpdateExec"),
        }
    }
}

impl ExecutionPlan for EntityUpdateExec {
    fn name(&self) -> &str {
        "EntityUpdateExec"
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
                "EntityUpdateExec does not accept children".to_string(),
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
                "EntityUpdateExec only exposes one partition, got {partition}"
            )));
        }

        let spec = Arc::clone(&self.spec);
        let write_ctx = self.write_ctx.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let version_binding = self.version_binding.clone();
        let request = self.request.clone();
        let assignments = self.assignments.clone();
        let filters = self.filters.clone();
        let result_schema = Arc::clone(&self.result_schema);
        let stream_schema = Arc::clone(&result_schema);

        let stream = stream::once(async move {
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                write_ctx
                    .scan_live_state(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
            let source_batch = entity_record_batch(&spec, Arc::clone(&table_schema), &rows)?;
            let matched_batch = filter_entity_batch(source_batch, &filters)?;
            let write_rows = entity_update_write_rows_from_batch(
                &spec,
                &matched_batch,
                &assignments,
                version_binding.active_version_id(),
            )?;
            let count = u64::try_from(write_rows.len()).map_err(|_| {
                DataFusionError::Execution("entity UPDATE row count overflow".to_string())
            })?;

            if count > 0 {
                write_ctx
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: write_rows,
                    })
                    .await
                    .map_err(lix_error_to_datafusion_error)?;
            }

            Ok::<_, DataFusionError>(stream::iter(vec![Ok::<RecordBatch, DataFusionError>(
                dml_count_batch(Arc::clone(&stream_schema), count)?,
            )]))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

fn validate_entity_update_assignments(
    spec: &EntitySurfaceSpec,
    schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<()> {
    for (column_name, _) in assignments {
        schema.field_with_name(column_name).map_err(|_| {
            DataFusionError::Plan(format!(
                "UPDATE entity surface '{}' failed: column '{column_name}' does not exist",
                spec.schema_key
            ))
        })?;
        if !spec.is_visible_column(column_name) && column_name != "lixcol_metadata" {
            return Err(DataFusionError::Execution(format!(
                "UPDATE entity surface '{}' cannot stage read-only column '{column_name}'",
                spec.schema_key
            )));
        }
    }
    Ok(())
}

fn filter_entity_batch(
    batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    let Some(mask) = evaluate_entity_filters(&batch, filters)? else {
        return Ok(batch);
    };
    Ok(filter_record_batch(&batch, &mask)?)
}

fn evaluate_entity_filters(
    batch: &RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut combined_mask: Option<BooleanArray> = None;
    for filter in filters {
        let result = filter.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution("entity surface filter was not boolean".to_string())
            })?;
        let normalized = bool_array
            .iter()
            .map(|value| Some(value == Some(true)))
            .collect::<BooleanArray>();
        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &normalized)?,
            None => normalized,
        });
    }
    Ok(combined_mask)
}

fn entity_update_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    version_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    let assignment_values = UpdateAssignmentValues::evaluate(batch, assignments)?;
    (0..batch.num_rows())
        .map(|row_index| {
            let scope = resolve_write_version_scope(
                optional_bool_value(batch, row_index, "lixcol_global")?,
                optional_string_value(batch, row_index, "lixcol_version_id")?,
                version_binding,
                &format!("UPDATE into {}_by_version", spec.schema_key),
                &spec.schema_key,
            )?;

            Ok(TransactionWriteRow {
                entity_id: optional_string_value(batch, row_index, "lixcol_entity_id")?
                    .map(|entity_id| {
                        EntityIdentity::from_json_array_text(&entity_id).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "UPDATE entity surface '{}' has invalid lixcol_entity_id: {error}",
                                spec.schema_key
                            ))
                        })
                    })
                    .transpose()?,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                snapshot: Some(
                    TransactionJson::from_value(
                        entity_update_snapshot_content_from_batch(
                            spec,
                            batch,
                            &assignment_values,
                            row_index,
                        )?,
                        &format!("{} update snapshot_content", spec.schema_key),
                    )
                    .map_err(super::error::lix_error_to_datafusion_error)?,
                ),
                metadata: entity_update_optional_metadata_value(
                    batch,
                    &assignment_values,
                    row_index,
                    "lixcol_metadata",
                    &spec.schema_key,
                )?,
                origin: None,
                created_at: None,
                updated_at: None,
                global: scope.global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?
                    .unwrap_or(false),
                version_id: scope.version_id,
            })
        })
        .collect()
}

fn entity_update_snapshot_content_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
) -> Result<JsonValue> {
    let snapshot_content = optional_string_value(batch, row_index, "lixcol_snapshot_content")?
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "UPDATE entity surface '{}' requires existing lixcol_snapshot_content",
                spec.schema_key
            ))
        })?;
    let mut object = match serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
        DataFusionError::Execution(format!(
            "UPDATE entity surface '{}' expected existing snapshot_content to be valid JSON: {error}",
            spec.schema_key
        ))
    })? {
        JsonValue::Object(object) => object,
        other => {
            return Err(DataFusionError::Execution(format!(
                "UPDATE entity surface '{}' expected existing snapshot_content to be a JSON object, got {other}",
                spec.schema_key
            )))
        }
    };

    for column in &spec.columns {
        let value = match entity_update_json_value(
            assignment_values,
            row_index,
            &column.name,
            column.column_type,
        )? {
            Some(value) => value,
            None => continue,
        };
        object.insert(column.name.clone(), value);
    }
    Ok(JsonValue::Object(object))
}

fn entity_update_optional_string_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match assignment_values.assigned_or_existing_cell(batch, row_index, column_name)? {
        InsertCell::Omitted | InsertCell::Provided(SqlCell::Null) => Ok(None),
        InsertCell::Provided(SqlCell::Value(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
            | ScalarValue::LargeUtf8(Some(value)),
        )) => Ok(Some(value)),
        InsertCell::Provided(SqlCell::Value(other)) => Err(DataFusionError::Execution(format!(
            "UPDATE entity surface expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn entity_update_optional_metadata_value(
    batch: &RecordBatch,
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    entity_update_optional_string_value(batch, assignment_values, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(super::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(super::error::lix_error_to_datafusion_error)
        })
        .transpose()
}

fn entity_update_json_value(
    assignment_values: &UpdateAssignmentValues,
    row_index: usize,
    column_name: &str,
    column_type: EntityColumnType,
) -> Result<Option<JsonValue>> {
    match assignment_values.assigned_cell(row_index, column_name)? {
        UpdateCell::Unassigned => Ok(None),
        UpdateCell::Assigned(SqlCell::Null) => Ok(Some(JsonValue::Null)),
        UpdateCell::Assigned(SqlCell::Value(value)) => {
            entity_json_value_from_scalar(Some(value), column_type).map(Some)
        }
    }
}

fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn dml_count_batch(schema: SchemaRef, count: u64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
    )
    .map_err(DataFusionError::from)
}

fn entity_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    insert_column_intents: &InsertColumnIntents,
    version_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    entity_lix_state_write_rows_from_batch_with_options(
        spec,
        batch,
        insert_column_intents,
        version_binding,
        true,
    )
}

fn entity_existing_lix_state_write_rows_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    version_binding: Option<&str>,
) -> Result<Vec<TransactionWriteRow>> {
    entity_lix_state_write_rows_from_batch_with_options(
        spec,
        batch,
        &InsertColumnIntents::all_explicit(),
        version_binding,
        false,
    )
}

fn entity_lix_state_write_rows_from_batch_with_options(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    insert_column_intents: &InsertColumnIntents,
    version_binding: Option<&str>,
    reject_read_only_fields: bool,
) -> Result<Vec<TransactionWriteRow>> {
    (0..batch.num_rows())
        .map(|row_index| {
            let scope = resolve_write_version_scope(
                optional_bool_value(batch, row_index, "lixcol_global")?,
                optional_string_value(batch, row_index, "lixcol_version_id")?,
                version_binding,
                &format!(
                    "INSERT into {}_by_version",
                    spec.schema_key
                ),
                &spec.schema_key,
            )?;

            if let Some(schema_key) = optional_string_value(batch, row_index, "lixcol_schema_key")?
            {
                if schema_key != spec.schema_key {
                    return Err(DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' cannot set lixcol_schema_key to '{}'",
                        spec.schema_key, schema_key
                    )));
                }
            }

            if reject_read_only_fields {
                reject_present_entity_insert_field(batch, row_index, "lixcol_snapshot_content")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_created_at")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_updated_at")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_change_id")?;
                reject_present_entity_insert_field(batch, row_index, "lixcol_commit_id")?;
            }

            let snapshot_content =
                entity_snapshot_content_from_batch(spec, batch, insert_column_intents, row_index)?;
            let explicit_entity_id = optional_string_value(batch, row_index, "lixcol_entity_id")?;
            let entity_id = if spec.primary_key_paths.is_empty() {
                let entity_id = explicit_entity_id.ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' requires lixcol_entity_id because the schema has no x-lix-primary-key",
                        spec.schema_key
                    ))
                })?;
                Some(EntityIdentity::from_json_array_text(&entity_id).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "INSERT into entity surface '{}' has invalid lixcol_entity_id: {error}",
                        spec.schema_key
                    ))
                })?)
            } else {
                explicit_entity_id
                    .map(|entity_id| {
                        EntityIdentity::from_json_array_text(&entity_id).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "INSERT into entity surface '{}' has invalid lixcol_entity_id: {error}",
                                spec.schema_key
                            ))
                        })
                    })
                    .transpose()?
            };

            Ok(TransactionWriteRow {
                entity_id,
                schema_key: spec.schema_key.clone(),
                file_id: optional_string_value(batch, row_index, "lixcol_file_id")?,
                snapshot: Some(TransactionJson::from_value(
                    snapshot_content,
                    &format!("{} insert snapshot_content", spec.schema_key),
                )
                .map_err(super::error::lix_error_to_datafusion_error)?),
                metadata: optional_metadata_value(
                    batch,
                    row_index,
                    "lixcol_metadata",
                    &spec.schema_key,
                )?,
                origin: None,
                created_at: None,
                updated_at: None,
                global: scope.global,
                change_id: None,
                commit_id: None,
                untracked: optional_bool_value(batch, row_index, "lixcol_untracked")?
                    .unwrap_or(false),
                version_id: scope.version_id,
            })
        })
        .collect()
}

fn entity_snapshot_content_from_batch(
    spec: &EntitySurfaceSpec,
    batch: &RecordBatch,
    insert_column_intents: &InsertColumnIntents,
    row_index: usize,
) -> Result<JsonValue> {
    let mut object = serde_json::Map::new();
    for column in &spec.columns {
        let value = match insert_column_intents.cell(batch, row_index, &column.name)? {
            InsertCell::Omitted => {
                continue;
            }
            InsertCell::Provided(SqlCell::Null) => JsonValue::Null,
            InsertCell::Provided(SqlCell::Value(value)) => {
                entity_json_value_from_scalar(Some(value), column.column_type)?
            }
        };
        object.insert(column.name.clone(), value);
    }
    Ok(JsonValue::Object(object))
}

fn entity_json_value_from_scalar(
    value: Option<ScalarValue>,
    column_type: EntityColumnType,
) -> Result<JsonValue> {
    let Some(value) = value else {
        return Ok(JsonValue::Null);
    };
    match value {
        ScalarValue::Null
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Int32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Float32(None) => Ok(JsonValue::Null),
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => match column_type {
            EntityColumnType::Json => {
                // JSON surface columns accept SQL strings as JSON string values,
                // while still allowing callers to pass serialized JSON text for
                // objects, arrays, numbers, booleans, and null.
                Ok(serde_json::from_str(&value).unwrap_or(JsonValue::String(value)))
            }
            EntityColumnType::Integer => {
                value.parse::<i64>().map(JsonValue::from).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity integer column expected integer text, got error: {error}"
                    ))
                })
            }
            EntityColumnType::Number => value
                .parse::<f64>()
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity number column expected number text, got error: {error}"
                    ))
                })
                .and_then(json_number_from_f64),
            EntityColumnType::Boolean => {
                value.parse::<bool>().map(JsonValue::from).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "entity boolean column expected boolean text, got error: {error}"
                    ))
                })
            }
            EntityColumnType::String => Ok(JsonValue::String(value)),
        },
        ScalarValue::Boolean(Some(value)) => Ok(JsonValue::Bool(value)),
        ScalarValue::Int64(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::Int32(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::UInt64(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::UInt32(Some(value)) => Ok(JsonValue::from(value)),
        ScalarValue::Float64(Some(value)) => json_number_from_f64(value),
        ScalarValue::Float32(Some(value)) => json_number_from_f64(value as f64),
        ScalarValue::Binary(Some(_))
        | ScalarValue::LargeBinary(Some(_))
        | ScalarValue::FixedSizeBinary(_, Some(_)) => Err(lix_error_to_datafusion_error(
            LixError::new(
                LixError::CODE_TYPE_MISMATCH,
                "entity JSON columns cannot store blob values directly",
            )
            .with_hint(
                "Encode bytes explicitly as JSON text/object, or store raw bytes in a blob-native surface such as lix_file.data.",
            ),
        )),
        ScalarValue::Binary(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::FixedSizeBinary(_, None) => Ok(JsonValue::Null),
        other => Err(DataFusionError::Execution(format!(
            "entity insert does not support scalar value {other:?}"
        ))),
    }
}

fn json_number_from_f64(value: f64) -> Result<JsonValue> {
    serde_json::Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| {
            DataFusionError::Execution(format!("entity number column cannot store {value}"))
        })
}

fn reject_present_entity_insert_field(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<()> {
    if optional_scalar_value(batch, row_index, column_name)?.is_some_and(|value| !value.is_null()) {
        return Err(DataFusionError::Execution(format!(
            "INSERT into entity surface cannot stage read-only column '{column_name}'"
        )));
    }
    Ok(())
}

fn optional_string_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<String>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None
        | Some(ScalarValue::Null)
        | Some(ScalarValue::Utf8(None))
        | Some(ScalarValue::Utf8View(None))
        | Some(ScalarValue::LargeUtf8(None)) => Ok(None),
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into entity surface expected text-compatible column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_metadata_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
    context: &str,
) -> Result<Option<TransactionJson>> {
    optional_string_value(batch, row_index, column_name)?
        .map(|value| {
            let metadata = parse_row_metadata_value(&value, context)
                .map_err(super::error::lix_error_to_datafusion_error)?;
            TransactionJson::from_value(metadata, &format!("{context} metadata"))
                .map_err(super::error::lix_error_to_datafusion_error)
        })
        .transpose()
}

fn optional_bool_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<bool>> {
    match optional_scalar_value(batch, row_index, column_name)? {
        None | Some(ScalarValue::Null) | Some(ScalarValue::Boolean(None)) => Ok(None),
        Some(ScalarValue::Boolean(Some(value))) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Execution(format!(
            "INSERT into entity surface expected boolean column '{column_name}', got {other:?}"
        ))),
    }
}

fn optional_scalar_value(
    batch: &RecordBatch,
    row_index: usize,
    column_name: &str,
) -> Result<Option<ScalarValue>> {
    let schema = batch.schema();
    let column_index = match schema.index_of(column_name) {
        Ok(column_index) => column_index,
        Err(_) => return Ok(None),
    };
    if row_index >= batch.num_rows() {
        return Err(DataFusionError::Execution(format!(
            "row index {row_index} out of bounds for entity batch with {} rows",
            batch.num_rows()
        )));
    }
    ScalarValue::try_from_array(batch.column(column_index).as_ref(), row_index)
        .map(Some)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "failed to decode entity column '{column_name}' at row {row_index}: {error}"
            ))
        })
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
            let rows = if request.limit == Some(0) {
                Vec::new()
            } else {
                live_state
                    .scan_rows(&request)
                    .await
                    .map_err(lix_error_to_datafusion_error)?
            };
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
    active_version_id: Option<&str>,
    projected_schema: Option<&Schema>,
    limit: Option<usize>,
) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![schema_key.to_string()],
            version_ids: active_version_id
                .map(|version_id| vec![version_id.to_string()])
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
        "entity_id" => Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.entity_id
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
        "version_id" => string_array(rows.iter().map(|row| Some(row.version_id.as_str()))),
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

pub(super) fn entity_surface_schema(
    spec: &EntitySurfaceSpec,
    variant: EntityProviderVariant,
) -> SchemaRef {
    let mut fields = spec
        .columns
        .iter()
        .map(|column| {
            let field = Field::new(
                &column.name,
                arrow_data_type_for_entity_column_type(column.column_type),
                true,
            );
            if column.column_type == EntityColumnType::Json {
                mark_json_field(field)
            } else {
                field
            }
        })
        .collect::<Vec<_>>();

    fields.extend(entity_system_fields(variant));
    Arc::new(Schema::new(fields))
}

fn arrow_data_type_for_entity_column_type(column_type: EntityColumnType) -> DataType {
    match column_type {
        EntityColumnType::String | EntityColumnType::Json => DataType::Utf8,
        EntityColumnType::Integer => DataType::Int64,
        EntityColumnType::Number => DataType::Float64,
        EntityColumnType::Boolean => DataType::Boolean,
    }
}

pub(super) fn entity_system_fields(variant: EntityProviderVariant) -> Vec<Field> {
    if variant == EntityProviderVariant::History {
        return vec![
            json_field(HISTORY_COL_ENTITY_ID, false),
            Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
            Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
            json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
            json_field(HISTORY_COL_METADATA, true),
            Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_START_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        ];
    }

    let mut fields = vec![
        json_field("lixcol_entity_id", true),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        json_field("lixcol_snapshot_content", true),
        json_field("lixcol_metadata", true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
    ];
    if variant == EntityProviderVariant::ByVersion {
        fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
    }
    fields
}

fn projected_schema(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
    let Some(projection) = projection else {
        return Ok(Arc::clone(schema));
    };
    Ok(Arc::new(schema.project(projection)?))
}

pub(super) fn derive_entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> std::result::Result<EntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "schema is missing string x-lix-key".to_string(),
            )
        })?;

    let properties = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("schema '{schema_key}' must define object properties"),
            )
        })?;

    let mut columns = properties
        .iter()
        .filter(|(key, _)| !key.starts_with("lixcol_"))
        .map(|(key, property_schema)| {
            let column_type = entity_column_type_from_schema(property_schema).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_SCHEMA_DEFINITION,
                    format!(
                        "schema '{schema_key}' property '/{key}' must declare a SQL-projectable JSON Schema type"
                    ),
                )
                .with_hint("Use an explicit type such as string, number, integer, boolean, object, array, or a supported union of those types.")
            })?;
            Ok(EntitySurfaceColumn {
                name: key.clone(),
                column_type,
            })
        })
        .collect::<std::result::Result<Vec<_>, LixError>>()?;
    columns.sort_by(|left, right| left.name.cmp(&right.name));

    let primary_key_paths = parse_primary_key_paths(schema)?;

    Ok(EntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        primary_key_paths,
        columns,
    })
}

fn parse_primary_key_paths(schema: &JsonValue) -> std::result::Result<Vec<Vec<String>>, LixError> {
    let Some(primary_key) = schema.get("x-lix-primary-key") else {
        return Ok(Vec::new());
    };
    let primary_key = primary_key.as_array().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "schema x-lix-primary-key must be an array of JSON Pointers".to_string(),
        )
    })?;

    primary_key
        .iter()
        .enumerate()
        .map(|(index, pointer)| {
            let pointer = pointer.as_str().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("schema x-lix-primary-key entry at index {index} must be a string"),
                )
            })?;
            parse_json_pointer(pointer)
        })
        .collect()
}

// TODO(engine): share JSON Pointer parsing with schema/canonical validation once
// those helpers have a clean module boundary for SQL providers.
fn parse_json_pointer(pointer: &str) -> std::result::Result<Vec<String>, LixError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("invalid JSON pointer '{pointer}'"),
        ));
    }
    pointer[1..]
        .split('/')
        .map(decode_json_pointer_segment)
        .collect()
}

fn decode_json_pointer_segment(segment: &str) -> std::result::Result<String, LixError> {
    let mut out = String::new();
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                _ => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid JSON pointer segment '{segment}'"),
                    ))
                }
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

fn schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_account" | "lix_change")
}

fn schema_exposed_as_entity_history_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_commit" | "lix_commit_edge")
}

fn entity_column_type_from_schema(schema: &JsonValue) -> Option<EntityColumnType> {
    let mut kinds = BTreeSet::new();
    collect_entity_type_kinds(schema, &mut kinds);
    kinds.remove("null");

    if kinds.is_empty() {
        return None;
    }

    if kinds.len() == 1 {
        return match kinds.into_iter().next() {
            Some("boolean") => Some(EntityColumnType::Boolean),
            Some("integer") => Some(EntityColumnType::Integer),
            Some("number") => Some(EntityColumnType::Number),
            Some("string") => Some(EntityColumnType::String),
            Some("object" | "array") => Some(EntityColumnType::Json),
            _ => None,
        };
    }

    Some(EntityColumnType::Json)
}

fn collect_entity_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => {
            out.insert(kind.as_str());
        }
        Some(JsonValue::Array(kinds)) => {
            for kind in kinds.iter().filter_map(JsonValue::as_str) {
                out.insert(kind);
            }
        }
        _ => {}
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(JsonValue::Array(branches)) = schema.get(keyword) {
            for branch in branches {
                collect_entity_type_kinds(branch, out);
            }
        }
    }
}

fn datafusion_error_to_lix_error(error: DataFusionError) -> LixError {
    super::error::datafusion_error_to_lix_error(error)
}

fn lix_error_to_datafusion_error(error: LixError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{Column, ScalarValue};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
    use serde_json::json;

    use super::{
        derive_entity_surface_spec_from_schema, entity_lix_state_write_rows_from_batch,
        entity_record_batch, entity_surface_schema, schema_exposed_as_entity_surface,
        EntityColumnType, EntityInsertSink, EntityProviderVariant,
    };
    use crate::binary_cas::BlobDataReader;
    use crate::functions::{
        FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
    };
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::dml::InsertSink;
    use crate::sql2::write_normalization::InsertColumnIntents;
    use crate::sql2::{SqlWriteContext, SqlWriteExecutionContext};
    use crate::transaction::types::{
        TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteOutcome,
        TransactionWriteRow,
    };
    use crate::version::{VersionHead, VersionRefReader};
    use crate::LixError;

    struct EmptyLiveStateReader;
    struct EmptyVersionRefReader;
    #[derive(Default)]
    struct CapturingWriteContext {
        rows: Vec<MaterializedLiveStateRow>,
        writes: Vec<TransactionWrite>,
    }

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
    impl VersionRefReader for EmptyVersionRefReader {
        async fn load_head(&self, _version_id: &str) -> Result<Option<VersionHead>, LixError> {
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
            Ok(Vec::new())
        }
    }

    fn empty_version_ref() -> Arc<dyn VersionRefReader> {
        Arc::new(EmptyVersionRefReader)
    }

    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
        )
    }

    #[async_trait]
    impl BlobDataReader for CapturingWriteContext {
        async fn load_bytes_many(
            &self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            Ok(crate::binary_cas::BlobBytesBatch::new(vec![
                None;
                hashes.len()
            ]))
        }
    }

    #[async_trait]
    impl SqlWriteExecutionContext for CapturingWriteContext {
        fn active_version_id(&self) -> &str {
            "version-a"
        }

        fn functions(&self) -> FunctionProviderHandle {
            test_functions()
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            Ok(Vec::new())
        }

        async fn load_bytes_many(
            &mut self,
            hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            BlobDataReader::load_bytes_many(self, hashes).await
        }

        async fn scan_live_state(
            &mut self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self.rows.clone())
        }

        async fn load_version_head(
            &mut self,
            version_id: &str,
        ) -> Result<Option<String>, LixError> {
            if version_id == "ghost-version" {
                return Ok(None);
            }
            Ok(Some(format!("commit-{version_id}")))
        }

        async fn stage_write(
            &mut self,
            write: TransactionWrite,
        ) -> Result<TransactionWriteOutcome, LixError> {
            self.writes.push(write);
            Ok(TransactionWriteOutcome { count: 0 })
        }
    }

    fn live_row() -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "project_message".to_string(),
            file_id: None,
            snapshot_content: Some(
                "{\"body\":\"hello\",\"rating\":4.5,\"count\":7,\"enabled\":true,\"meta\":{\"x\":1}}"
                    .to_string(),
            ),
            metadata: Some(json!({"source": "test"}).to_string()),
            deleted: false,
            version_id: "version-a".to_string(),
            change_id: Some("change-a".to_string()),
            commit_id: Some("commit-a".to_string()),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }

    fn entity_insert_spec() -> Arc<super::EntitySurfaceSpec> {
        Arc::new(
            derive_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "count": { "type": "integer" },
                    "enabled": { "type": "boolean" },
                    "meta": { "type": "object" },
                    "rating": { "type": "number" }
                }
            }))
            .expect("schema should derive entity surface spec"),
        )
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

    fn string_column(values: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(values)) as ArrayRef
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

    fn entity_insert_batch(include_version: bool, global: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("enabled", DataType::Boolean, true),
            Field::new("meta", DataType::Utf8, true),
            Field::new("rating", DataType::Float64, true),
            Field::new("lixcol_entity_id", DataType::Utf8, false),
            Field::new("lixcol_metadata", DataType::Utf8, true),
            Field::new("lixcol_global", DataType::Boolean, false),
            Field::new("lixcol_untracked", DataType::Boolean, false),
        ];
        let mut columns = vec![
            string_column(vec![Some("hello")]),
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            string_column(vec![Some("{\"x\":1}")]),
            Arc::new(Float64Array::from(vec![4.5])) as ArrayRef,
            string_column(vec![Some("[\"entity-1\"]")]),
            string_column(vec![Some("{\"source\":\"entity\"}")]),
            Arc::new(BooleanArray::from(vec![global])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ];
        if include_version {
            fields.push(Field::new("lixcol_version_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("version-a")]));
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("entity insert batch should build")
    }

    fn primary_key_entity_insert_batch(include_entity_id: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("lixcol_version_id", DataType::Utf8, false),
        ];
        let mut columns = vec![
            string_column(vec![Some("message-1")]),
            string_column(vec![Some("hello")]),
            string_column(vec![Some("version-a")]),
        ];
        if include_entity_id {
            fields.push(Field::new("lixcol_entity_id", DataType::Utf8, false));
            columns.push(string_column(vec![Some("[\"message-1\"]")]));
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("primary-key entity insert batch should build")
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
                "lixcol_entity_id": { "type": "string" }
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
        assert!(!spec.is_visible_column("lixcol_entity_id"));
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
    fn by_version_schema_includes_version_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntityProviderVariant::ByVersion);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_id").is_ok());
        assert!(schema.field_with_name("lixcol_version_id").is_ok());
    }

    #[test]
    fn active_schema_excludes_version_system_column() {
        let spec = derive_entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" }
            }
        }))
        .expect("schema should derive entity surface spec");

        let schema = entity_surface_schema(&spec, EntityProviderVariant::Active);
        assert!(schema.field_with_name("body").is_ok());
        assert!(schema.field_with_name("lixcol_entity_id").is_ok());
        assert!(schema.field_with_name("lixcol_version_id").is_err());
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

        let schema = entity_surface_schema(&spec, EntityProviderVariant::Active);
        assert!(
            schema
                .field_with_name("id")
                .expect("id field")
                .is_nullable(),
            "defaulted primary-key property should be nullable at SQL input"
        );
        assert!(
            schema
                .field_with_name("lixcol_entity_id")
                .expect("entity id field")
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
        let schema = entity_surface_schema(&spec, EntityProviderVariant::ByVersion);

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
                .column_by_name("lixcol_entity_id")
                .expect("entity id column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("entity id is string")
                .value(0),
            "[\"entity-1\"]"
        );
        assert_eq!(
            batch
                .column_by_name("lixcol_version_id")
                .expect("version id column")
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("version id is string")
                .value(0),
            "version-a"
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
        let provider = super::EntityProvider::by_version(
            spec,
            Arc::new(EmptyLiveStateReader) as Arc<dyn LiveStateReader>,
            empty_version_ref(),
        );

        assert!(provider.schema.field_with_name("lixcol_version_id").is_ok());
    }

    #[test]
    fn primary_key_filters_route_entity_ids_for_string_primary_key() {
        let spec = entity_insert_spec_with_primary_key();
        let filters = vec![
            eq_filter("id", "entity-a"),
            Expr::InList(InList::new(
                Box::new(column("id")),
                vec![string_literal("entity-b"), string_literal("entity-a")],
                false,
            )),
        ];

        let entity_ids = super::entity_ids_from_primary_key_filters(&spec, &filters)
            .expect("primary-key filters should analyze")
            .expect("primary-key filters should produce a constraint");

        assert_eq!(
            entity_ids,
            vec![crate::entity_identity::EntityIdentity::single("entity-a")]
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
            .expect("OR should produce an entity-id set");
        let contradiction_ids = analyzer
            .analyze(&contradiction)
            .expect("AND should analyze")
            .expect("AND should produce an entity-id set");

        assert_eq!(
            disjunction_ids.into_iter().collect::<Vec<_>>(),
            vec![
                crate::entity_identity::EntityIdentity::single("entity-a"),
                crate::entity_identity::EntityIdentity::single("entity-b"),
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

        assert!(super::entity_ids_from_primary_key_filters(&spec, &filters)
            .expect("ignored filters should analyze")
            .unwrap_or_default()
            .is_empty());
    }

    #[test]
    fn decodes_by_version_entity_insert_into_lix_state_write_row() {
        let spec = entity_insert_spec();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(true, false),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect("entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("entity-1"))
        );
        assert_eq!(rows[0].schema_key, "project_message");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].metadata.as_ref(),
            Some(&TransactionJson::from_value_for_test(
                json!({"source": "entity"})
            ))
        );
        assert!(!rows[0].global);
        assert_eq!(
            rows[0].snapshot.as_ref().expect("snapshot_content"),
            &json!({
                "body": "hello",
                "count": 7,
                "enabled": true,
                "meta": {"x": 1},
                "rating": 4.5
            })
        );
    }

    #[test]
    fn primary_key_entity_insert_stages_partial_row_for_normalization() {
        let spec = entity_insert_spec_with_primary_key();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &primary_key_entity_insert_batch(false),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect("entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, None);
        assert_eq!(
            rows[0].snapshot.as_ref().expect("snapshot_content"),
            &json!({
                "body": "hello",
                "id": "message-1"
            })
        );
    }

    #[test]
    fn primary_key_entity_insert_preserves_explicit_opaque_projection_for_normalization() {
        let spec = entity_insert_spec_with_primary_key();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &primary_key_entity_insert_batch(true),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect("primary-key entity insert should stage explicit lixcol_entity_id");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].entity_id.as_ref(),
            Some(&crate::entity_identity::EntityIdentity::single("message-1"))
        );
    }

    #[test]
    fn active_entity_insert_defaults_version_id() {
        let spec = entity_insert_spec();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(false, false),
            &InsertColumnIntents::all_explicit(),
            Some("version-active"),
        )
        .expect("active entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-active");
        assert!(!rows[0].global);
    }

    #[test]
    fn by_version_entity_insert_requires_version_id_for_non_global_rows() {
        let spec = entity_insert_spec();
        let error = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(false, false),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect_err("by-version entity insert should require version id");

        assert!(
            error.to_string().contains("requires lixcol_version_id"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn by_version_entity_insert_global_row_uses_global_version() {
        let spec = entity_insert_spec();
        let rows = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(false, true),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect("global entity batch should decode");

        assert_eq!(rows.len(), 1);
        assert!(rows[0].global);
        assert_eq!(rows[0].version_id, crate::GLOBAL_VERSION_ID);
    }

    #[test]
    fn entity_insert_rejects_global_with_non_global_version_id() {
        let spec = entity_insert_spec();
        let error = entity_lix_state_write_rows_from_batch(
            &spec,
            &entity_insert_batch(true, true),
            &InsertColumnIntents::all_explicit(),
            None,
        )
        .expect_err("global entity write should reject conflicting version id");

        assert!(
            error
                .to_string()
                .contains("cannot set lixcol_global=true with non-global lixcol_version_id"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn entity_insert_sink_stages_decoded_lix_state_rows() {
        let spec = entity_insert_spec();
        let mut write_context = CapturingWriteContext::default();
        let write_ctx = SqlWriteContext::new(&mut write_context);
        let batch = entity_insert_batch(true, false);
        let sink = EntityInsertSink::new(
            Arc::clone(&spec),
            batch.schema(),
            InsertColumnIntents::all_explicit(),
            write_ctx,
            super::VersionBinding::explicit(),
        );
        let count = sink
            .write_batches(vec![batch], &Arc::new(TaskContext::default()))
            .await
            .expect("entity sink should stage write");

        assert_eq!(count, 1);
        assert_eq!(
            write_context.writes.as_slice(),
            &[TransactionWrite::Rows {
                mode: TransactionWriteMode::Insert,
                rows: vec![TransactionWriteRow {
                    entity_id: Some(crate::entity_identity::EntityIdentity::single("entity-1")),
                    schema_key: "project_message".to_string(),
                    file_id: None,
                    snapshot: Some(TransactionJson::from_value_for_test(
                        json!({"body":"hello","count":7,"enabled":true,"meta":{"x":1},"rating":4.5})
                    )),
                    metadata: Some(TransactionJson::from_value_for_test(
                        json!({"source": "entity"})
                    )),
                    origin: None,
                    created_at: None,
                    updated_at: None,
                    global: false,
                    change_id: None,
                    commit_id: None,
                    untracked: false,
                    version_id: "version-a".to_string(),
                }]
            }]
        );
    }
}
