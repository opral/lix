use super::*;
use crate::canonical::refs::load_current_committed_version_frontier_with_backend;
use crate::errors::classification::sanitize_lowered_public_sql_error_description;
use crate::read::models::{
    load_directory_history_rows, load_file_history_rows, DirectoryHistoryRequest,
    DirectoryHistoryRow, FileHistoryContentMode, FileHistoryLineageScope, FileHistoryRequest,
    FileHistoryRootScope, FileHistoryRow, FileHistoryVersionScope,
};
use crate::read::models::{
    load_state_history_rows, StateHistoryContentMode, StateHistoryLineageScope,
    StateHistoryRequest, StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};
use crate::schema::{SchemaProvider, SqlRegisteredSchemaProvider};
use crate::sql::binder::{bind_public_read_statement, RuntimeBindingValues};
use crate::sql::catalog::{SurfaceBinding, SurfaceFamily, SurfaceReadFreshness, SurfaceRegistry};
use crate::sql::explain::{
    build_public_read_explain_artifacts, unwrap_explain_statement, ExplainStage,
    ExplainTimingCollector, PublicReadExplainBuildInput, PublicReadExplainRuntimeArtifacts,
};
use crate::sql::logical_plan::public_ir::BroadPublicReadStatement;
use crate::sql::logical_plan::{
    verify_logical_plan, DirectDirectoryHistoryField, DirectEntityHistoryField,
    DirectFileHistoryField, DirectPublicReadPlan, DirectStateHistoryField,
    DirectoryHistoryAggregate, DirectoryHistoryDirectReadPlan, DirectoryHistoryPredicate,
    DirectoryHistoryProjection, DirectoryHistorySortKey, EntityHistoryDirectReadPlan,
    EntityHistoryPredicate, EntityHistoryProjection, EntityHistorySortKey, FileHistoryAggregate,
    FileHistoryDirectReadPlan, FileHistoryPredicate, FileHistoryProjection, FileHistorySortKey,
    LogicalPlan, PublicReadLogicalPlan, StateHistoryAggregate, StateHistoryAggregatePredicate,
    StateHistoryDirectReadPlan, StateHistoryPredicate, StateHistoryProjection,
    StateHistoryProjectionValue, StateHistorySortKey, StateHistorySortValue,
};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::physical_plan::lowerer::{
    lower_broad_public_read_for_execution_with_layouts, lower_read_for_execution_with_layouts,
};
use crate::sql::physical_plan::{
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns, PreparedPublicReadExecution,
};
use crate::sql::routing::{
    route_broad_public_read_statement_with_known_live_layouts, route_public_read_execution_strategy,
};
use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_bound_public_surface_bindings;
use crate::sql::semantic_ir::{
    augment_dependency_spec_for_broad_public_read, prepare_structured_public_read_analysis,
    unknown_public_state_schema_error, PublicReadSemantics, StructuredPublicReadPreparation,
};
use crate::{LixBackendTransaction, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, Value as SqlValue,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

pub(crate) fn decode_public_read_result(
    result: QueryResult,
    lowered_read: &LoweredReadProgram,
) -> QueryResult {
    decode_public_read_result_columns(result, &lowered_read.result_columns)
}

pub(crate) fn finalize_prepared_public_read_result(
    result: QueryResult,
    prepared: &PreparedPublicRead,
) -> QueryResult {
    let result = if let Some(lowered) = prepared.lowered_read() {
        decode_public_read_result(result, lowered)
    } else {
        result
    };
    apply_public_output_columns(result, prepared.public_output_columns.as_deref())
}

pub(crate) fn decode_public_read_result_columns(
    mut result: QueryResult,
    result_columns: &LoweredResultColumns,
) -> QueryResult {
    let column_plan = match result_columns {
        LoweredResultColumns::Static(columns) => columns
            .iter()
            .copied()
            .chain(std::iter::repeat(LoweredResultColumn::Untyped))
            .take(result.columns.len())
            .collect::<Vec<_>>(),
        LoweredResultColumns::ByColumnName(columns_by_name) => result
            .columns
            .iter()
            .map(|column| {
                columns_by_name
                    .iter()
                    .find_map(|(candidate, kind)| {
                        candidate.eq_ignore_ascii_case(column).then_some(*kind)
                    })
                    .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect::<Vec<_>>(),
    };

    if !column_plan
        .iter()
        .any(|kind| *kind == LoweredResultColumn::Boolean)
    {
        return result;
    }

    for row in &mut result.rows {
        for (value, kind) in row.iter_mut().zip(column_plan.iter().copied()) {
            if kind == LoweredResultColumn::Boolean {
                if let Some(decoded) = decode_boolean_value(value) {
                    *value = decoded;
                }
            }
        }
    }

    result
}

pub(crate) async fn execute_prepared_public_read(
    backend: &dyn LixBackend,
    prepared: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    ensure_surface_read_freshness(
        backend,
        prepared.freshness_contract,
        prepared.surface_bindings(),
    )
    .await?;

    execute_prepared_public_read_unchecked(backend, prepared).await
}

pub(crate) async fn execute_prepared_public_read_without_freshness_check(
    backend: &dyn LixBackend,
    prepared: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    execute_prepared_public_read_unchecked(backend, prepared).await
}

pub(crate) async fn execute_prepared_public_read_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    prepared: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    ensure_surface_read_freshness_in_transaction(
        transaction,
        prepared.freshness_contract,
        prepared.surface_bindings(),
    )
    .await?;
    let backend = crate::engine::TransactionBackendAdapter::new(transaction);
    execute_prepared_public_read_unchecked(&backend, prepared).await
}

async fn execute_prepared_public_read_unchecked(
    backend: &dyn LixBackend,
    prepared: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    let result = match &prepared.execution {
        PreparedPublicReadExecution::LoweredSql(lowered) => {
            execute_lowered_public_read(
                backend,
                lowered,
                prepared.dependency_spec(),
                prepared.surface_bindings(),
                &prepared.bound_parameters,
                &prepared.runtime_bindings,
            )
            .await
        }
        PreparedPublicReadExecution::Direct(plan) => {
            execute_direct_public_read(backend, plan).await
        }
    }?;
    Ok(finalize_prepared_public_read_result(result, prepared))
}

async fn ensure_surface_read_freshness(
    backend: &dyn LixBackend,
    freshness_contract: SurfaceReadFreshness,
    surface_names: &[String],
) -> Result<(), LixError> {
    if freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    let status = crate::live_state::load_live_state_projection_status_with_backend(backend).await?;
    if matches!(
        status.mode,
        crate::live_state::LiveStateMode::Ready | crate::live_state::LiveStateMode::Bootstrapping
    ) {
        return Ok(());
    }

    Err(public_read_projection_stale_error(surface_names, &status))
}

async fn ensure_surface_read_freshness_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    freshness_contract: SurfaceReadFreshness,
    surface_names: &[String],
) -> Result<(), LixError> {
    if freshness_contract == SurfaceReadFreshness::AllowsStaleProjection {
        return Ok(());
    }

    if crate::live_state::require_ready_in_transaction(transaction)
        .await
        .is_ok()
    {
        return Ok(());
    }

    let backend = crate::engine::TransactionBackendAdapter::new(transaction);
    let status =
        crate::live_state::load_live_state_projection_status_with_backend(&backend).await?;
    if status.mode == crate::live_state::LiveStateMode::Bootstrapping {
        return Ok(());
    }
    Err(public_read_projection_stale_error(surface_names, &status))
}

fn public_read_projection_stale_error(
    surface_names: &[String],
    status: &crate::live_state::LiveStateProjectionStatus,
) -> LixError {
    let surfaces = if surface_names.is_empty() {
        "this public read".to_string()
    } else {
        format!("surface(s) {}", surface_names.join(", "))
    };
    let applied = format_optional_replay_cursor(status.applied_cursor.as_ref());
    let latest = format_optional_replay_cursor(status.latest_cursor.as_ref());
    let applied_frontier =
        format_optional_committed_frontier(status.applied_committed_frontier.as_ref());
    let current_frontier = format_committed_frontier(&status.current_committed_frontier);
    LixError::new(
        crate::errors::ErrorCode::LiveStateNotReady.as_str(),
        format!(
            "Public read for {surfaces} requires fresh live-state projections, but live_state is {:?}. Applied committed frontier: {applied_frontier}. Current committed frontier: {current_frontier}. Applied replay cursor: {applied}. Latest replay cursor: {latest}. Canonical history/change reads may proceed while stale, but current-state projection reads must wait for replay or rebuild.",
            status.mode
        ),
    )
}

fn format_optional_replay_cursor(cursor: Option<&crate::live_state::ReplayCursor>) -> String {
    cursor
        .map(|cursor| format!("{}@{}", cursor.change_id, cursor.created_at))
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_optional_committed_frontier(
    frontier: Option<&crate::CommittedVersionFrontier>,
) -> String {
    frontier
        .map(format_committed_frontier)
        .unwrap_or_else(|| "(none)".to_string())
}

fn format_committed_frontier(frontier: &crate::CommittedVersionFrontier) -> String {
    frontier.describe()
}

async fn execute_lowered_public_read(
    backend: &dyn LixBackend,
    lowered: &LoweredReadProgram,
    _dependency_spec: Option<&DependencySpec>,
    public_surfaces: &[String],
    params: &[Value],
    runtime_bindings: &RuntimeBindingValues,
) -> Result<QueryResult, LixError> {
    let mut result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &lowered.statements {
        let (sql, bound_params) =
            statement.bind_and_render_sql(params, runtime_bindings, backend.dialect())?;
        result = backend
            .execute(&sql, &bound_params)
            .await
            .map_err(|error| translate_lowered_public_read_error(error, public_surfaces))?;
    }
    Ok(decode_public_read_result_columns(
        result,
        &lowered.result_columns,
    ))
}

fn apply_public_output_columns(
    mut result: QueryResult,
    public_output_columns: Option<&[String]>,
) -> QueryResult {
    let Some(public_output_columns) = public_output_columns else {
        return result;
    };
    if !public_output_columns.is_empty() && public_output_columns.len() == result.columns.len() {
        result.columns = public_output_columns.to_vec();
    }
    result
}

fn translate_lowered_public_read_error(error: LixError, public_surfaces: &[String]) -> LixError {
    let description =
        sanitize_lowered_public_sql_error_description(&error.description, public_surfaces);
    LixError::new(&error.code, description)
}

fn runtime_binding_values_from_execution_context(
    execution_context: &ExecutionContext,
) -> Result<RuntimeBindingValues, LixError> {
    Ok(RuntimeBindingValues {
        active_version_id: execution_context
            .requested_version_id
            .clone()
            .unwrap_or_default(),
        active_account_ids_json: serde_json::to_string(&execution_context.active_account_ids)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("active account ids serialization failed: {error}"),
                )
            })?,
    })
}

fn render_lowered_read_sql(
    lowered: &LoweredReadProgram,
    params: &[Value],
    execution_context: &ExecutionContext,
    dialect: SqlDialect,
) -> Result<Vec<String>, LixError> {
    let runtime_bindings = runtime_binding_values_from_execution_context(execution_context)?;
    lowered
        .statements
        .iter()
        .map(|statement| {
            statement
                .bind_and_render_sql(params, &runtime_bindings, dialect)
                .map(|(sql, _)| sql)
        })
        .collect()
}

async fn execute_direct_public_read(
    backend: &dyn LixBackend,
    plan: &DirectPublicReadPlan,
) -> Result<QueryResult, LixError> {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => {
            execute_direct_state_history_read(backend, plan).await
        }
        DirectPublicReadPlan::EntityHistory(plan) => {
            execute_direct_entity_history_read(backend, plan).await
        }
        DirectPublicReadPlan::FileHistory(plan) => {
            execute_direct_file_history_read(backend, plan).await
        }
        DirectPublicReadPlan::DirectoryHistory(plan) => {
            execute_direct_directory_history_read(backend, plan).await
        }
    }
}

fn decode_boolean_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => Some(Value::Null),
        Value::Boolean(value) => Some(Value::Boolean(*value)),
        Value::Integer(0) => Some(Value::Boolean(false)),
        Value::Integer(1) => Some(Value::Boolean(true)),
        Value::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "0" | "false" => Some(Value::Boolean(false)),
            "1" | "true" => Some(Value::Boolean(true)),
            _ => None,
        },
        Value::Real(_) | Value::Json(_) | Value::Blob(_) => None,
        Value::Integer(_) => None,
    }
}

fn required_schema_keys_from_dependency_spec(
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    dependency_spec
        .map(|spec| spec.schema_keys.iter().cloned().collect())
        .unwrap_or_default()
}

fn public_output_columns_from_statement(statement: &Statement) -> Option<Vec<String>> {
    match statement {
        Statement::Query(query) => public_output_columns_from_query(query),
        _ => None,
    }
}

fn public_output_columns_from_query(query: &Query) -> Option<Vec<String>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    public_output_columns_from_select(select.as_ref())
}

fn public_output_columns_from_select(select: &Select) -> Option<Vec<String>> {
    let mut output = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => output.push(expr.to_string()),
            SelectItem::ExprWithAlias { alias, .. } => output.push(alias.value.clone()),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => return None,
        }
    }
    Some(output)
}

async fn load_known_live_layouts_for_dependency_spec(
    backend: &dyn LixBackend,
    dependency_spec: Option<&DependencySpec>,
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut schemas = BTreeMap::new();
    for schema_key in required_schema_keys_from_dependency_spec(dependency_spec) {
        schemas.insert(
            schema_key.clone(),
            provider.load_latest_schema(&schema_key).await?,
        );
    }
    Ok(schemas)
}

async fn load_known_live_layouts_for_public_read(
    backend: &dyn LixBackend,
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
    effective_state_request: Option<&EffectiveStateRequest>,
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut schemas = load_known_live_layouts_for_dependency_spec(backend, dependency_spec).await?;
    if let Some(request) = effective_state_request {
        if let Some(schema_key) = structured_read
            .surface_binding
            .implicit_overrides
            .fixed_schema_key
            .as_ref()
        {
            if !schemas.contains_key(schema_key) {
                schemas.insert(
                    schema_key.clone(),
                    provider.load_latest_schema(schema_key).await?,
                );
            }
        }
        for schema_key in &request.schema_set {
            if schemas.contains_key(schema_key) {
                continue;
            }
            schemas.insert(
                schema_key.clone(),
                provider.load_latest_schema(schema_key).await?,
            );
        }
    }
    Ok(schemas)
}

async fn load_known_live_layouts_for_broad_public_read(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    surface_bindings: &[SurfaceBinding],
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut schemas = BTreeMap::new();
    let mut required_schema_keys = surface_bindings
        .iter()
        .filter(|binding| {
            matches!(
                binding.descriptor.surface_family,
                SurfaceFamily::State | SurfaceFamily::Entity
            )
        })
        .filter_map(|binding| binding.implicit_overrides.fixed_schema_key.clone())
        .collect::<BTreeSet<_>>();
    if surface_bindings
        .iter()
        .any(|binding| binding.descriptor.surface_family == SurfaceFamily::State)
    {
        required_schema_keys.extend(registry.registered_state_surface_schema_keys());
    }
    for schema_key in required_schema_keys {
        schemas.insert(
            schema_key.clone(),
            provider.load_latest_schema(&schema_key).await?,
        );
    }
    Ok(schemas)
}

fn build_direct_state_history_plan(
    structured_read: &StructuredPublicRead,
) -> Result<Option<StateHistoryDirectReadPlan>, LixError> {
    if structured_read.surface_binding.descriptor.public_name != "lix_state_history" {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        active_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
        content_mode: StateHistoryContentMode::MetadataOnly,
        ..StateHistoryRequest::default()
    };
    let predicates = build_state_history_predicates_and_request(structured_read, &mut request)?;
    let group_by_fields = build_state_history_group_by_fields(structured_read)?;
    let having = build_state_history_having(structured_read)?;
    if state_history_query_needs_snapshot_content(structured_read, &predicates)? {
        request.content_mode = StateHistoryContentMode::IncludeSnapshotContent;
    }

    let (projections, wildcard_projection, wildcard_columns, projection_aliases) =
        build_state_history_projection_plan(structured_read, &group_by_fields)?;
    let sort_keys = build_state_history_sort_keys(structured_read, &projection_aliases)?;
    let (limit, offset) = direct_limit_values(
        structured_read.query.limit_clause.as_ref(),
        &structured_read.bound_parameters,
    )?;
    let result_columns = direct_state_history_result_columns(
        &structured_read.surface_binding,
        &projections,
        wildcard_projection,
    );

    Ok(Some(StateHistoryDirectReadPlan {
        request,
        predicates,
        projections,
        wildcard_projection,
        wildcard_columns,
        group_by_fields,
        having,
        sort_keys,
        limit,
        offset,
        result_columns,
    }))
}

fn build_direct_entity_history_plan(
    structured_read: &StructuredPublicRead,
) -> Result<Option<EntityHistoryDirectReadPlan>, LixError> {
    if structured_read.surface_binding.descriptor.surface_family != SurfaceFamily::Entity
        || structured_read.surface_binding.descriptor.surface_variant != SurfaceVariant::History
    {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let schema_key = structured_read
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct entity-history execution requires a fixed schema key",
            )
        })?;

    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        active_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
        content_mode: StateHistoryContentMode::IncludeSnapshotContent,
        schema_keys: vec![schema_key],
        ..StateHistoryRequest::default()
    };
    let predicates = build_entity_history_predicates_and_request(structured_read, &mut request)?;
    let (projections, wildcard_projection, wildcard_columns, projection_aliases) =
        build_entity_history_projection_plan(structured_read)?;
    let sort_keys = build_entity_history_sort_keys(structured_read, &projection_aliases)?;
    let (limit, offset) = direct_limit_values(
        structured_read.query.limit_clause.as_ref(),
        &structured_read.bound_parameters,
    )?;
    let result_columns = direct_entity_history_result_columns(
        &structured_read.surface_binding,
        &projections,
        wildcard_projection,
    );

    Ok(Some(EntityHistoryDirectReadPlan {
        surface_binding: structured_read.surface_binding.clone(),
        request,
        predicates,
        projections,
        wildcard_projection,
        wildcard_columns,
        sort_keys,
        limit,
        offset,
        result_columns,
    }))
}

fn build_state_history_group_by_fields(
    structured_read: &StructuredPublicRead,
) -> Result<Vec<DirectStateHistoryField>, LixError> {
    match &structured_read.query.group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            if !modifiers.is_empty() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support GROUP BY modifiers",
                ));
            }
            expressions
                .iter()
                .map(|expr| {
                    direct_state_history_field_from_expr(&structured_read.surface_binding, expr)?
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "direct state-history execution only supports grouping by state-history columns",
                            )
                        })
                })
                .collect()
        }
        GroupByExpr::All(_) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution does not support GROUP BY ALL",
        )),
    }
}

fn build_state_history_having(
    structured_read: &StructuredPublicRead,
) -> Result<Option<StateHistoryAggregatePredicate>, LixError> {
    let Some(having) = &structured_read.query.having else {
        return Ok(None);
    };
    parse_state_history_aggregate_predicate(
        having,
        &structured_read.bound_parameters,
        &mut PlaceholderState::new(),
    )?
    .ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution only supports HAVING predicates over COUNT(*)",
        )
    })
    .map(Some)
}

fn required_requested_version_id(structured_read: &StructuredPublicRead) -> Result<&str, LixError> {
    structured_read
        .requested_version_id
        .as_deref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public read '{}' requires a session-requested active version id",
                    structured_read.surface_binding.descriptor.public_name
                ),
            )
        })
}

fn build_entity_history_predicates_and_request(
    structured_read: &StructuredPublicRead,
    request: &mut StateHistoryRequest,
) -> Result<Vec<EntityHistoryPredicate>, LixError> {
    let mut predicates = Vec::new();
    let mut root_commit_ids = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut entity_ids = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut plugin_keys = BTreeSet::new();
    let mut min_depth = None;
    let mut max_depth = None;
    let mut placeholder_state = PlaceholderState::new();

    for predicate_expr in &structured_read.query.selection_predicates {
        let predicate = parse_entity_history_predicate(
            predicate_expr,
            &structured_read.surface_binding,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct entity-history execution does not support this predicate shape",
            )
        })?;
        apply_entity_history_pushdown(
            &predicate,
            &mut root_commit_ids,
            &mut version_ids,
            &mut entity_ids,
            &mut file_ids,
            &mut plugin_keys,
            &mut min_depth,
            &mut max_depth,
        );
        predicates.push(predicate);
    }

    if !root_commit_ids.is_empty() {
        request.root_scope =
            StateHistoryRootScope::RequestedRoots(root_commit_ids.into_iter().collect());
    }
    if !version_ids.is_empty() {
        request.version_scope =
            StateHistoryVersionScope::RequestedVersions(version_ids.into_iter().collect());
    }
    request.entity_ids = entity_ids.into_iter().collect();
    request.file_ids = file_ids.into_iter().collect();
    request.plugin_keys = plugin_keys.into_iter().collect();
    request.min_depth = min_depth;
    request.max_depth = max_depth;

    Ok(predicates)
}

fn apply_entity_history_pushdown(
    predicate: &EntityHistoryPredicate,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
    min_depth: &mut Option<i64>,
    max_depth: &mut Option<i64>,
) {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => {
            push_text_value_for_entity_history_field(
                field,
                value,
                root_commit_ids,
                version_ids,
                entity_ids,
                file_ids,
                plugin_keys,
            );
            if matches!(
                field,
                DirectEntityHistoryField::State(DirectStateHistoryField::Depth)
            ) {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth);
                    update_max_depth(max_depth, depth);
                }
            }
        }
        EntityHistoryPredicate::In(field, values) => {
            for value in values {
                push_text_value_for_entity_history_field(
                    field,
                    value,
                    root_commit_ids,
                    version_ids,
                    entity_ids,
                    file_ids,
                    plugin_keys,
                );
            }
        }
        EntityHistoryPredicate::Gt(field, value) => {
            if matches!(
                field,
                DirectEntityHistoryField::State(DirectStateHistoryField::Depth)
            ) {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth + 1);
                }
            }
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            if matches!(
                field,
                DirectEntityHistoryField::State(DirectStateHistoryField::Depth)
            ) {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth);
                }
            }
        }
        EntityHistoryPredicate::Lt(field, value) => {
            if matches!(
                field,
                DirectEntityHistoryField::State(DirectStateHistoryField::Depth)
            ) {
                if let Some(depth) = value_as_i64(value) {
                    update_max_depth(max_depth, depth - 1);
                }
            }
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            if matches!(
                field,
                DirectEntityHistoryField::State(DirectStateHistoryField::Depth)
            ) {
                if let Some(depth) = value_as_i64(value) {
                    update_max_depth(max_depth, depth);
                }
            }
        }
        EntityHistoryPredicate::NotEq(_, _)
        | EntityHistoryPredicate::IsNull(_)
        | EntityHistoryPredicate::IsNotNull(_) => {}
    }
}

fn push_text_value_for_entity_history_field(
    field: &DirectEntityHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectEntityHistoryField::State(DirectStateHistoryField::RootCommitId) => {
            root_commit_ids.insert(text.to_string());
        }
        DirectEntityHistoryField::State(DirectStateHistoryField::VersionId) => {
            version_ids.insert(text.to_string());
        }
        DirectEntityHistoryField::State(DirectStateHistoryField::EntityId) => {
            entity_ids.insert(text.to_string());
        }
        DirectEntityHistoryField::State(DirectStateHistoryField::FileId) => {
            file_ids.insert(text.to_string());
        }
        DirectEntityHistoryField::State(DirectStateHistoryField::PluginKey) => {
            plugin_keys.insert(text.to_string());
        }
        DirectEntityHistoryField::Property(_)
        | DirectEntityHistoryField::State(DirectStateHistoryField::SchemaKey)
        | DirectEntityHistoryField::State(DirectStateHistoryField::SnapshotContent)
        | DirectEntityHistoryField::State(DirectStateHistoryField::Metadata)
        | DirectEntityHistoryField::State(DirectStateHistoryField::SchemaVersion)
        | DirectEntityHistoryField::State(DirectStateHistoryField::ChangeId)
        | DirectEntityHistoryField::State(DirectStateHistoryField::CommitId)
        | DirectEntityHistoryField::State(DirectStateHistoryField::CommitCreatedAt)
        | DirectEntityHistoryField::State(DirectStateHistoryField::Depth) => {}
    }
}

fn build_entity_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<EntityHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, DirectEntityHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.surface_binding.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field =
            direct_entity_history_field_from_select_item(&structured_read.surface_binding, item)?;
        let output_name = direct_entity_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(EntityHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_entity_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, DirectEntityHistoryField>,
) -> Result<Vec<EntityHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct entity-history execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct entity-history execution does not support ORDER BY ... WITH FILL",
            ));
        }
        let output_name = direct_expr_output_name(&expr.expr);
        let field =
            direct_entity_history_field_from_expr(&structured_read.surface_binding, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct entity-history execution does not support this ORDER BY expression",
            ));
        };
        sort_keys.push(EntityHistorySortKey {
            output_name,
            field: Some(field),
            descending: matches!(expr.options.asc, Some(false)),
        });
    }
    Ok(sort_keys)
}

fn direct_entity_history_result_columns(
    surface_binding: &SurfaceBinding,
    projections: &[EntityHistoryProjection],
    wildcard_projection: bool,
) -> LoweredResultColumns {
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            surface_binding
                .column_types
                .iter()
                .map(|(name, column_type)| {
                    (
                        name.clone(),
                        direct_lowered_result_column_from_surface_type(*column_type),
                    )
                })
                .collect(),
        );
    }

    LoweredResultColumns::Static(
        projections
            .iter()
            .map(|projection| {
                direct_surface_column_type(
                    surface_binding,
                    direct_entity_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn build_direct_file_history_plan(
    structured_read: &StructuredPublicRead,
) -> Result<Option<FileHistoryDirectReadPlan>, LixError> {
    let public_name = structured_read
        .surface_binding
        .descriptor
        .public_name
        .as_str();
    if public_name != "lix_file_history" && public_name != "lix_file_history_by_version" {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = FileHistoryRequest {
        lineage_scope: if public_name == "lix_file_history" {
            FileHistoryLineageScope::ActiveVersion
        } else {
            FileHistoryLineageScope::Standard
        },
        active_version_id: (public_name == "lix_file_history")
            .then(|| required_requested_version_id(structured_read).map(str::to_string))
            .transpose()?,
        ..FileHistoryRequest::default()
    };
    let predicates = build_file_history_predicates_and_request(structured_read, &mut request)?;
    let aggregate = direct_file_history_aggregate(structured_read)?;
    if aggregate.is_none() && file_history_query_needs_data(structured_read, &predicates)? {
        request.content_mode = FileHistoryContentMode::IncludeData;
    }
    let aggregate_output_name = aggregate
        .as_ref()
        .map(|_| direct_file_history_aggregate_output_name(&structured_read.query.projection[0]));
    let (projections, wildcard_projection, wildcard_columns, projection_aliases) =
        if aggregate.is_some() {
            (Vec::new(), false, Vec::new(), BTreeMap::new())
        } else {
            build_file_history_projection_plan(structured_read)?
        };
    let sort_keys = build_file_history_sort_keys(structured_read, &projection_aliases)?;
    let (limit, offset) = direct_limit_values(
        structured_read.query.limit_clause.as_ref(),
        &structured_read.bound_parameters,
    )?;
    let result_columns = direct_file_history_result_columns(
        &structured_read.surface_binding,
        &projections,
        wildcard_projection,
        aggregate.as_ref(),
    );

    Ok(Some(FileHistoryDirectReadPlan {
        request,
        predicates,
        projections,
        wildcard_projection,
        wildcard_columns,
        sort_keys,
        limit,
        offset,
        aggregate,
        aggregate_output_name,
        result_columns,
    }))
}

fn build_direct_directory_history_plan(
    structured_read: &StructuredPublicRead,
) -> Result<Option<DirectoryHistoryDirectReadPlan>, LixError> {
    let public_name = structured_read
        .surface_binding
        .descriptor
        .public_name
        .as_str();
    if public_name != "lix_directory_history" {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = DirectoryHistoryRequest {
        lineage_scope: FileHistoryLineageScope::ActiveVersion,
        active_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
        ..DirectoryHistoryRequest::default()
    };
    let predicates = build_directory_history_predicates_and_request(structured_read, &mut request)?;
    let aggregate = direct_directory_history_aggregate(structured_read)?;
    let aggregate_output_name = aggregate.as_ref().map(|_| {
        direct_directory_history_aggregate_output_name(&structured_read.query.projection[0])
    });
    let (projections, wildcard_projection, wildcard_columns, projection_aliases) =
        if aggregate.is_some() {
            (Vec::new(), false, Vec::new(), BTreeMap::new())
        } else {
            build_directory_history_projection_plan(structured_read)?
        };
    let sort_keys = build_directory_history_sort_keys(structured_read, &projection_aliases)?;
    let (limit, offset) = direct_limit_values(
        structured_read.query.limit_clause.as_ref(),
        &structured_read.bound_parameters,
    )?;
    let result_columns = direct_directory_history_result_columns(
        &structured_read.surface_binding,
        &projections,
        wildcard_projection,
        aggregate.as_ref(),
    );

    Ok(Some(DirectoryHistoryDirectReadPlan {
        request,
        predicates,
        projections,
        wildcard_projection,
        wildcard_columns,
        sort_keys,
        limit,
        offset,
        aggregate,
        aggregate_output_name,
        result_columns,
    }))
}

fn direct_directory_history_aggregate(
    structured_read: &StructuredPublicRead,
) -> Result<Option<DirectoryHistoryAggregate>, LixError> {
    if structured_read.query.projection.len() != 1 {
        return Ok(None);
    }
    let item = &structured_read.query.projection[0];
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
    };
    let Expr::Function(function) = expr else {
        return Ok(direct_expr_is_count_star(expr).then_some(DirectoryHistoryAggregate::Count));
    };
    if function.over.is_some()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Ok(None);
    }
    if !function.name.to_string().eq_ignore_ascii_case("count") {
        return Ok(None);
    }
    let FunctionArguments::List(list) = &function.args else {
        return Ok(None);
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return Ok(None);
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            Ok(Some(DirectoryHistoryAggregate::Count))
        }
        _ => Ok(direct_expr_is_count_star(expr).then_some(DirectoryHistoryAggregate::Count)),
    }
}

fn direct_directory_history_aggregate_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(expr) => expr.to_string(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn build_directory_history_predicates_and_request(
    structured_read: &StructuredPublicRead,
    request: &mut DirectoryHistoryRequest,
) -> Result<Vec<DirectoryHistoryPredicate>, LixError> {
    let mut predicates = Vec::new();
    let mut root_commit_ids = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut directory_ids = BTreeSet::new();
    let mut placeholder_state = PlaceholderState::new();

    for predicate_expr in &structured_read.query.selection_predicates {
        let predicate = parse_directory_history_predicate(
            predicate_expr,
            &structured_read.surface_binding,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct directory-history execution does not support this predicate shape",
            )
        })?;
        apply_directory_history_pushdown(
            &predicate,
            &mut root_commit_ids,
            &mut version_ids,
            &mut directory_ids,
        );
        predicates.push(predicate);
    }

    if !root_commit_ids.is_empty() {
        request.root_scope =
            FileHistoryRootScope::RequestedRoots(root_commit_ids.into_iter().collect());
    }
    if !version_ids.is_empty() {
        request.version_scope =
            FileHistoryVersionScope::RequestedVersions(version_ids.into_iter().collect());
    }
    request.directory_ids = directory_ids.into_iter().collect();

    Ok(predicates)
}

fn apply_directory_history_pushdown(
    predicate: &DirectoryHistoryPredicate,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    directory_ids: &mut BTreeSet<String>,
) {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => {
            push_text_value_for_directory_history_field(
                field,
                value,
                root_commit_ids,
                version_ids,
                directory_ids,
            );
        }
        DirectoryHistoryPredicate::In(field, values) => {
            for value in values {
                push_text_value_for_directory_history_field(
                    field,
                    value,
                    root_commit_ids,
                    version_ids,
                    directory_ids,
                );
            }
        }
        DirectoryHistoryPredicate::NotEq(_, _)
        | DirectoryHistoryPredicate::Gt(_, _)
        | DirectoryHistoryPredicate::GtEq(_, _)
        | DirectoryHistoryPredicate::Lt(_, _)
        | DirectoryHistoryPredicate::LtEq(_, _)
        | DirectoryHistoryPredicate::IsNull(_)
        | DirectoryHistoryPredicate::IsNotNull(_) => {}
    }
}

fn push_text_value_for_directory_history_field(
    field: &DirectDirectoryHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    directory_ids: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectDirectoryHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        DirectDirectoryHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        DirectDirectoryHistoryField::Id | DirectDirectoryHistoryField::EntityId => {
            directory_ids.insert(text.to_string());
        }
        DirectDirectoryHistoryField::ParentId
        | DirectDirectoryHistoryField::Name
        | DirectDirectoryHistoryField::Path
        | DirectDirectoryHistoryField::Hidden
        | DirectDirectoryHistoryField::SchemaKey
        | DirectDirectoryHistoryField::FileId
        | DirectDirectoryHistoryField::PluginKey
        | DirectDirectoryHistoryField::SchemaVersion
        | DirectDirectoryHistoryField::ChangeId
        | DirectDirectoryHistoryField::LixcolMetadata
        | DirectDirectoryHistoryField::CommitId
        | DirectDirectoryHistoryField::CommitCreatedAt
        | DirectDirectoryHistoryField::Depth => {}
    }
}

fn build_directory_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<DirectoryHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, DirectDirectoryHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.surface_binding.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field = direct_directory_history_field_from_select_item(
            &structured_read.surface_binding,
            item,
        )?;
        let output_name = direct_directory_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(DirectoryHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_directory_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, DirectDirectoryHistoryField>,
) -> Result<Vec<DirectoryHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct directory-history execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct directory-history execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let field =
            direct_directory_history_field_from_expr(&structured_read.surface_binding, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct directory-history execution does not support this ORDER BY expression",
            ));
        };
        sort_keys.push(DirectoryHistorySortKey {
            output_name,
            field: Some(field),
            descending: matches!(expr.options.asc, Some(false)),
        });
    }
    Ok(sort_keys)
}

fn direct_directory_history_result_columns(
    surface_binding: &SurfaceBinding,
    projections: &[DirectoryHistoryProjection],
    wildcard_projection: bool,
    aggregate: Option<&DirectoryHistoryAggregate>,
) -> LoweredResultColumns {
    if aggregate.is_some() {
        return LoweredResultColumns::Static(vec![LoweredResultColumn::Untyped]);
    }
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            surface_binding
                .column_types
                .iter()
                .map(|(name, column_type)| {
                    (
                        name.clone(),
                        direct_lowered_result_column_from_surface_type(*column_type),
                    )
                })
                .collect(),
        );
    }

    LoweredResultColumns::Static(
        projections
            .iter()
            .map(|projection| {
                direct_surface_column_type(
                    surface_binding,
                    direct_directory_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn direct_directory_history_field_from_select_item(
    surface_binding: &SurfaceBinding,
    item: &SelectItem,
) -> Result<DirectDirectoryHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before direct directory-history field extraction",
            ))
        }
    };
    direct_directory_history_field_from_expr(surface_binding, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct directory-history execution does not support this projection expression",
        )
    })
}

fn direct_directory_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn direct_directory_history_field_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<DirectDirectoryHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            direct_directory_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            direct_directory_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::Nested(inner) => direct_directory_history_field_from_expr(surface_binding, inner),
        _ => Ok(None),
    }
}

fn direct_directory_history_field_from_column_name(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Result<DirectDirectoryHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Ok(DirectDirectoryHistoryField::Id),
        "parent_id" => Ok(DirectDirectoryHistoryField::ParentId),
        "name" => Ok(DirectDirectoryHistoryField::Name),
        "path" => Ok(DirectDirectoryHistoryField::Path),
        "hidden" => Ok(DirectDirectoryHistoryField::Hidden),
        "entity_id" | "lixcol_entity_id" => Ok(DirectDirectoryHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(DirectDirectoryHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(DirectDirectoryHistoryField::FileId),
        "version_id" | "lixcol_version_id" => Ok(DirectDirectoryHistoryField::VersionId),
        "plugin_key" | "lixcol_plugin_key" => Ok(DirectDirectoryHistoryField::PluginKey),
        "schema_version" | "lixcol_schema_version" => {
            Ok(DirectDirectoryHistoryField::SchemaVersion)
        }
        "change_id" | "lixcol_change_id" => Ok(DirectDirectoryHistoryField::ChangeId),
        "lixcol_metadata" => Ok(DirectDirectoryHistoryField::LixcolMetadata),
        "commit_id" | "lixcol_commit_id" => Ok(DirectDirectoryHistoryField::CommitId),
        "commit_created_at" | "lixcol_commit_created_at" => {
            Ok(DirectDirectoryHistoryField::CommitCreatedAt)
        }
        "root_commit_id" | "lixcol_root_commit_id" => Ok(DirectDirectoryHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(DirectDirectoryHistoryField::Depth),
        _ => Err(crate::errors::sql_unknown_column_error(
            column,
            Some(&surface_binding.descriptor.public_name),
            &surface_binding
                .exposed_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )),
    }
}

fn parse_directory_history_predicate(
    expr: &Expr,
    surface_binding: &SurfaceBinding,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<DirectoryHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_directory_history_predicate(inner, surface_binding, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = direct_directory_history_field_from_expr(surface_binding, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(directory_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = direct_directory_history_field_from_expr(surface_binding, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(directory_history_predicate_from_reversed_operator(
                        field, op, value,
                    ));
                }
            }
            Ok(None)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Ok(None);
            }
            let Some(field) = direct_directory_history_field_from_expr(surface_binding, expr)?
            else {
                return Ok(None);
            };
            let mut values = Vec::new();
            for item in list {
                let Some(value) = direct_value_from_expr(item, params, placeholder_state)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(DirectoryHistoryPredicate::In(field, values)))
        }
        Expr::IsNull(expr) => direct_directory_history_field_from_expr(surface_binding, expr)?
            .map(DirectoryHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct directory-history execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => direct_directory_history_field_from_expr(surface_binding, expr)?
            .map(DirectoryHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct directory-history execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn directory_history_predicate_from_operator(
    field: DirectDirectoryHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<DirectoryHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(DirectoryHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(DirectoryHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(DirectoryHistoryPredicate::Gt(field, value)),
        BinaryOperator::GtEq => Some(DirectoryHistoryPredicate::GtEq(field, value)),
        BinaryOperator::Lt => Some(DirectoryHistoryPredicate::Lt(field, value)),
        BinaryOperator::LtEq => Some(DirectoryHistoryPredicate::LtEq(field, value)),
        _ => None,
    }
}

fn directory_history_predicate_from_reversed_operator(
    field: DirectDirectoryHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<DirectoryHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(DirectoryHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(DirectoryHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(DirectoryHistoryPredicate::Lt(field, value)),
        BinaryOperator::GtEq => Some(DirectoryHistoryPredicate::LtEq(field, value)),
        BinaryOperator::Lt => Some(DirectoryHistoryPredicate::Gt(field, value)),
        BinaryOperator::LtEq => Some(DirectoryHistoryPredicate::GtEq(field, value)),
        _ => None,
    }
}

fn direct_file_history_aggregate(
    structured_read: &StructuredPublicRead,
) -> Result<Option<FileHistoryAggregate>, LixError> {
    if structured_read.query.projection.len() != 1 {
        return Ok(None);
    }
    let item = &structured_read.query.projection[0];
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => return Ok(None),
    };
    let Expr::Function(function) = expr else {
        return Ok(direct_expr_is_count_star(expr).then_some(FileHistoryAggregate::Count));
    };
    if function.over.is_some()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Ok(None);
    }
    if !function.name.to_string().eq_ignore_ascii_case("count") {
        return Ok(None);
    }
    let FunctionArguments::List(list) = &function.args else {
        return Ok(None);
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return Ok(None);
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Some(FileHistoryAggregate::Count)),
        _ => Ok(direct_expr_is_count_star(expr).then_some(FileHistoryAggregate::Count)),
    }
}

fn direct_expr_is_count_star(expr: &Expr) -> bool {
    expr.to_string()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .eq_ignore_ascii_case("count(*)")
}

fn direct_file_history_aggregate_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::UnnamedExpr(expr) => expr.to_string(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn build_file_history_predicates_and_request(
    structured_read: &StructuredPublicRead,
    request: &mut FileHistoryRequest,
) -> Result<Vec<FileHistoryPredicate>, LixError> {
    let mut predicates = Vec::new();
    let mut root_commit_ids = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut placeholder_state = PlaceholderState::new();

    for predicate_expr in &structured_read.query.selection_predicates {
        let predicate = parse_file_history_predicate(
            predicate_expr,
            &structured_read.surface_binding,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct file-history execution does not support this predicate shape",
            )
        })?;
        apply_file_history_pushdown(&predicate, &mut root_commit_ids, &mut version_ids);
        predicates.push(predicate);
    }

    if !root_commit_ids.is_empty() {
        request.root_scope =
            FileHistoryRootScope::RequestedRoots(root_commit_ids.into_iter().collect());
    }
    if !version_ids.is_empty() {
        request.version_scope =
            FileHistoryVersionScope::RequestedVersions(version_ids.into_iter().collect());
    }

    Ok(predicates)
}

fn apply_file_history_pushdown(
    predicate: &FileHistoryPredicate,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => {
            push_text_value_for_file_history_field(field, value, root_commit_ids, version_ids);
        }
        FileHistoryPredicate::In(field, values) => {
            for value in values {
                push_text_value_for_file_history_field(field, value, root_commit_ids, version_ids);
            }
        }
        FileHistoryPredicate::NotEq(_, _)
        | FileHistoryPredicate::Gt(_, _)
        | FileHistoryPredicate::GtEq(_, _)
        | FileHistoryPredicate::Lt(_, _)
        | FileHistoryPredicate::LtEq(_, _)
        | FileHistoryPredicate::IsNull(_)
        | FileHistoryPredicate::IsNotNull(_) => {}
    }
}

fn push_text_value_for_file_history_field(
    field: &DirectFileHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectFileHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        DirectFileHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        DirectFileHistoryField::Id
        | DirectFileHistoryField::EntityId
        | DirectFileHistoryField::FileId
        | DirectFileHistoryField::Path
        | DirectFileHistoryField::Data
        | DirectFileHistoryField::Metadata
        | DirectFileHistoryField::Hidden
        | DirectFileHistoryField::SchemaKey
        | DirectFileHistoryField::PluginKey
        | DirectFileHistoryField::SchemaVersion
        | DirectFileHistoryField::ChangeId
        | DirectFileHistoryField::LixcolMetadata
        | DirectFileHistoryField::CommitId
        | DirectFileHistoryField::CommitCreatedAt
        | DirectFileHistoryField::Depth => {}
    }
}

fn file_history_query_needs_data(
    structured_read: &StructuredPublicRead,
    predicates: &[FileHistoryPredicate],
) -> Result<bool, LixError> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok(true);
    }

    for projection in &structured_read.query.projection {
        if matches!(
            direct_file_history_field_from_select_item(
                &structured_read.surface_binding,
                projection
            )?,
            DirectFileHistoryField::Data
        ) {
            return Ok(true);
        }
    }
    for predicate in predicates {
        if file_history_predicate_field(predicate) == DirectFileHistoryField::Data {
            return Ok(true);
        }
    }
    if let Some(order_by) = &structured_read.query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(true);
        };
        for sort in expressions {
            if sort.with_fill.is_some() {
                return Ok(true);
            }
            let Some(field) =
                direct_file_history_field_from_expr(&structured_read.surface_binding, &sort.expr)?
            else {
                continue;
            };
            if field == DirectFileHistoryField::Data {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn build_file_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<FileHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, DirectFileHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.surface_binding.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field =
            direct_file_history_field_from_select_item(&structured_read.surface_binding, item)?;
        let output_name = direct_file_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(FileHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_file_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, DirectFileHistoryField>,
) -> Result<Vec<FileHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct file-history execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct file-history execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let field =
            direct_file_history_field_from_expr(&structured_read.surface_binding, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct file-history execution does not support this ORDER BY expression",
            ));
        };
        sort_keys.push(FileHistorySortKey {
            output_name,
            field: Some(field),
            descending: matches!(expr.options.asc, Some(false)),
        });
    }
    Ok(sort_keys)
}

fn direct_file_history_result_columns(
    surface_binding: &SurfaceBinding,
    projections: &[FileHistoryProjection],
    wildcard_projection: bool,
    aggregate: Option<&FileHistoryAggregate>,
) -> LoweredResultColumns {
    if aggregate.is_some() {
        return LoweredResultColumns::Static(vec![LoweredResultColumn::Untyped]);
    }
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            surface_binding
                .column_types
                .iter()
                .map(|(name, column_type)| {
                    (
                        name.clone(),
                        direct_lowered_result_column_from_surface_type(*column_type),
                    )
                })
                .collect(),
        );
    }

    LoweredResultColumns::Static(
        projections
            .iter()
            .map(|projection| {
                direct_surface_column_type(
                    surface_binding,
                    direct_file_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn direct_file_history_field_from_select_item(
    surface_binding: &SurfaceBinding,
    item: &SelectItem,
) -> Result<DirectFileHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before direct file-history field extraction",
            ))
        }
    };
    direct_file_history_field_from_expr(surface_binding, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct file-history execution does not support this projection expression",
        )
    })
}

fn direct_file_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn direct_file_history_field_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<DirectFileHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            direct_file_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            direct_file_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::Nested(inner) => direct_file_history_field_from_expr(surface_binding, inner),
        _ => Ok(None),
    }
}

fn direct_file_history_field_from_column_name(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Result<DirectFileHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Ok(DirectFileHistoryField::Id),
        "path" => Ok(DirectFileHistoryField::Path),
        "data" => Ok(DirectFileHistoryField::Data),
        "metadata" => Ok(DirectFileHistoryField::Metadata),
        "hidden" => Ok(DirectFileHistoryField::Hidden),
        "entity_id" | "lixcol_entity_id" => Ok(DirectFileHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(DirectFileHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(DirectFileHistoryField::FileId),
        "version_id" | "lixcol_version_id" => Ok(DirectFileHistoryField::VersionId),
        "plugin_key" | "lixcol_plugin_key" => Ok(DirectFileHistoryField::PluginKey),
        "schema_version" | "lixcol_schema_version" => Ok(DirectFileHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(DirectFileHistoryField::ChangeId),
        "lixcol_metadata" => Ok(DirectFileHistoryField::LixcolMetadata),
        "commit_id" | "lixcol_commit_id" => Ok(DirectFileHistoryField::CommitId),
        "commit_created_at" | "lixcol_commit_created_at" => {
            Ok(DirectFileHistoryField::CommitCreatedAt)
        }
        "root_commit_id" | "lixcol_root_commit_id" => Ok(DirectFileHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(DirectFileHistoryField::Depth),
        _ => Err(crate::errors::sql_unknown_column_error(
            column,
            Some(&surface_binding.descriptor.public_name),
            &surface_binding
                .exposed_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )),
    }
}

fn parse_file_history_predicate(
    expr: &Expr,
    surface_binding: &SurfaceBinding,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<FileHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_file_history_predicate(inner, surface_binding, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = direct_file_history_field_from_expr(surface_binding, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(file_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = direct_file_history_field_from_expr(surface_binding, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(file_history_predicate_from_reversed_operator(
                        field, op, value,
                    ));
                }
            }
            Ok(None)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Ok(None);
            }
            let Some(field) = direct_file_history_field_from_expr(surface_binding, expr)? else {
                return Ok(None);
            };
            let mut values = Vec::new();
            for item in list {
                let Some(value) = direct_value_from_expr(item, params, placeholder_state)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(FileHistoryPredicate::In(field, values)))
        }
        Expr::IsNull(expr) => direct_file_history_field_from_expr(surface_binding, expr)?
            .map(FileHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct file-history execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => direct_file_history_field_from_expr(surface_binding, expr)?
            .map(FileHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct file-history execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn file_history_predicate_from_operator(
    field: DirectFileHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<FileHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(FileHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(FileHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(FileHistoryPredicate::Gt(field, value)),
        BinaryOperator::GtEq => Some(FileHistoryPredicate::GtEq(field, value)),
        BinaryOperator::Lt => Some(FileHistoryPredicate::Lt(field, value)),
        BinaryOperator::LtEq => Some(FileHistoryPredicate::LtEq(field, value)),
        _ => None,
    }
}

fn file_history_predicate_from_reversed_operator(
    field: DirectFileHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<FileHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(FileHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(FileHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(FileHistoryPredicate::Lt(field, value)),
        BinaryOperator::GtEq => Some(FileHistoryPredicate::LtEq(field, value)),
        BinaryOperator::Lt => Some(FileHistoryPredicate::Gt(field, value)),
        BinaryOperator::LtEq => Some(FileHistoryPredicate::GtEq(field, value)),
        _ => None,
    }
}

fn build_state_history_predicates_and_request(
    structured_read: &StructuredPublicRead,
    request: &mut StateHistoryRequest,
) -> Result<Vec<StateHistoryPredicate>, LixError> {
    let mut predicates = Vec::new();
    let mut root_commit_ids = BTreeSet::new();
    let mut version_ids = BTreeSet::new();
    let mut entity_ids = BTreeSet::new();
    let mut file_ids = BTreeSet::new();
    let mut schema_keys = BTreeSet::new();
    let mut plugin_keys = BTreeSet::new();
    let mut min_depth = request.min_depth;
    let mut max_depth = request.max_depth;
    let mut placeholder_state = PlaceholderState::new();

    for predicate_expr in &structured_read.query.selection_predicates {
        let predicate = parse_state_history_predicate(
            predicate_expr,
            &structured_read.surface_binding,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support this predicate shape",
            )
        })?;
        apply_state_history_pushdown(
            &predicate,
            &mut root_commit_ids,
            &mut version_ids,
            &mut entity_ids,
            &mut file_ids,
            &mut schema_keys,
            &mut plugin_keys,
            &mut min_depth,
            &mut max_depth,
        );
        predicates.push(predicate);
    }

    if !root_commit_ids.is_empty() {
        request.root_scope =
            StateHistoryRootScope::RequestedRoots(root_commit_ids.into_iter().collect());
    }
    if !version_ids.is_empty() {
        request.version_scope =
            StateHistoryVersionScope::RequestedVersions(version_ids.into_iter().collect());
    }
    request.entity_ids = entity_ids.into_iter().collect();
    request.file_ids = file_ids.into_iter().collect();
    request.schema_keys = schema_keys.into_iter().collect();
    request.plugin_keys = plugin_keys.into_iter().collect();
    request.min_depth = min_depth;
    request.max_depth = max_depth;

    Ok(predicates)
}

fn apply_state_history_pushdown(
    predicate: &StateHistoryPredicate,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    schema_keys: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
    min_depth: &mut Option<i64>,
    max_depth: &mut Option<i64>,
) {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => {
            push_text_value_for_field(
                field,
                value,
                root_commit_ids,
                version_ids,
                entity_ids,
                file_ids,
                schema_keys,
                plugin_keys,
            );
            if *field == DirectStateHistoryField::Depth {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth);
                    update_max_depth(max_depth, depth);
                }
            }
        }
        StateHistoryPredicate::In(field, values) => {
            for value in values {
                push_text_value_for_field(
                    field,
                    value,
                    root_commit_ids,
                    version_ids,
                    entity_ids,
                    file_ids,
                    schema_keys,
                    plugin_keys,
                );
            }
        }
        StateHistoryPredicate::Gt(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth.saturating_add(1));
            }
        }
        StateHistoryPredicate::GtEq(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth);
            }
        }
        StateHistoryPredicate::Lt(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_max_depth(max_depth, depth.saturating_sub(1));
            }
        }
        StateHistoryPredicate::LtEq(field, value) if *field == DirectStateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_max_depth(max_depth, depth);
            }
        }
        StateHistoryPredicate::NotEq(_, _)
        | StateHistoryPredicate::Gt(_, _)
        | StateHistoryPredicate::GtEq(_, _)
        | StateHistoryPredicate::Lt(_, _)
        | StateHistoryPredicate::LtEq(_, _)
        | StateHistoryPredicate::IsNull(_)
        | StateHistoryPredicate::IsNotNull(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn push_text_value_for_field(
    field: &DirectStateHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    schema_keys: &mut BTreeSet<String>,
    plugin_keys: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectStateHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        DirectStateHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        DirectStateHistoryField::EntityId => {
            entity_ids.insert(text.to_string());
        }
        DirectStateHistoryField::FileId => {
            file_ids.insert(text.to_string());
        }
        DirectStateHistoryField::SchemaKey => {
            schema_keys.insert(text.to_string());
        }
        DirectStateHistoryField::PluginKey => {
            plugin_keys.insert(text.to_string());
        }
        DirectStateHistoryField::SnapshotContent
        | DirectStateHistoryField::Metadata
        | DirectStateHistoryField::SchemaVersion
        | DirectStateHistoryField::ChangeId
        | DirectStateHistoryField::CommitId
        | DirectStateHistoryField::CommitCreatedAt
        | DirectStateHistoryField::Depth => {}
    }
}

fn update_min_depth(min_depth: &mut Option<i64>, candidate: i64) {
    match min_depth {
        Some(current) => *current = (*current).max(candidate),
        None => *min_depth = Some(candidate),
    }
}

fn update_max_depth(max_depth: &mut Option<i64>, candidate: i64) {
    match max_depth {
        Some(current) => *current = (*current).min(candidate),
        None => *max_depth = Some(candidate),
    }
}

fn state_history_query_needs_snapshot_content(
    structured_read: &StructuredPublicRead,
    predicates: &[StateHistoryPredicate],
) -> Result<bool, LixError> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok(true);
    }

    for projection in &structured_read.query.projection {
        let value = direct_state_history_projection_value(
            &structured_read.surface_binding,
            projection,
            &[],
        )?;
        if let StateHistoryProjectionValue::Field(DirectStateHistoryField::SnapshotContent) = value
        {
            return Ok(true);
        }
    }
    for predicate in predicates {
        if state_history_predicate_field(predicate) == DirectStateHistoryField::SnapshotContent {
            return Ok(true);
        }
    }
    if let Some(order_by) = &structured_read.query.order_by {
        let OrderByKind::Expressions(expressions) = &order_by.kind else {
            return Ok(true);
        };
        for sort in expressions {
            if sort.with_fill.is_some() {
                return Ok(true);
            }
            let Some(field) =
                direct_state_history_field_from_expr(&structured_read.surface_binding, &sort.expr)?
            else {
                continue;
            };
            if field == DirectStateHistoryField::SnapshotContent {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn build_state_history_projection_plan(
    structured_read: &StructuredPublicRead,
    group_by_fields: &[DirectStateHistoryField],
) -> Result<
    (
        Vec<StateHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, StateHistoryProjectionValue>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        if !group_by_fields.is_empty() || structured_read.query.having.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support wildcard projection on grouped reads",
            ));
        }
        return Ok((
            Vec::new(),
            true,
            structured_read.surface_binding.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let value = direct_state_history_projection_value(
            &structured_read.surface_binding,
            item,
            group_by_fields,
        )?;
        let output_name = direct_state_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), value.clone());
        projections.push(StateHistoryProjection { output_name, value });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_state_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, StateHistoryProjectionValue>,
) -> Result<Vec<StateHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let value = direct_state_history_sort_value_from_expr(
            &structured_read.surface_binding,
            &expr.expr,
        )?
        .or_else(|| {
            projection_aliases
                .get(&output_name.to_ascii_lowercase())
                .cloned()
                .map(state_history_sort_value_from_projection_value)
        });
        let Some(value) = value else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "direct state-history execution does not support this ORDER BY expression",
            ));
        };
        sort_keys.push(StateHistorySortKey {
            output_name,
            value: Some(value),
            descending: matches!(expr.options.asc, Some(false)),
        });
    }
    Ok(sort_keys)
}

fn direct_state_history_result_columns(
    surface_binding: &SurfaceBinding,
    projections: &[StateHistoryProjection],
    wildcard_projection: bool,
) -> LoweredResultColumns {
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            surface_binding
                .column_types
                .iter()
                .map(
                    |(name, column_type): (&String, &crate::sql::catalog::SurfaceColumnType)| {
                        (
                            name.clone(),
                            direct_lowered_result_column_from_surface_type(*column_type),
                        )
                    },
                )
                .collect(),
        );
    }

    LoweredResultColumns::Static(
        projections
            .iter()
            .map(|projection| match &projection.value {
                StateHistoryProjectionValue::Field(field) => direct_surface_column_type(
                    surface_binding,
                    direct_state_history_field_name(field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped),
                StateHistoryProjectionValue::Aggregate(StateHistoryAggregate::Count) => {
                    LoweredResultColumn::Untyped
                }
            })
            .collect(),
    )
}

fn direct_lowered_result_column_from_surface_type(
    column_type: crate::sql::catalog::SurfaceColumnType,
) -> LoweredResultColumn {
    match column_type {
        crate::sql::catalog::SurfaceColumnType::Boolean => LoweredResultColumn::Boolean,
        crate::sql::catalog::SurfaceColumnType::String
        | crate::sql::catalog::SurfaceColumnType::Integer
        | crate::sql::catalog::SurfaceColumnType::Number
        | crate::sql::catalog::SurfaceColumnType::Json => LoweredResultColumn::Untyped,
    }
}

fn direct_surface_column_type(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<crate::sql::catalog::SurfaceColumnType> {
    surface_binding.column_types.iter().find_map(
        |(candidate, kind): (&String, &crate::sql::catalog::SurfaceColumnType)| {
            candidate.eq_ignore_ascii_case(column).then_some(*kind)
        },
    )
}

fn direct_entity_history_field_from_select_item(
    surface_binding: &SurfaceBinding,
    item: &SelectItem,
) -> Result<DirectEntityHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before direct entity-history field extraction",
            ))
        }
    };
    direct_entity_history_field_from_expr(surface_binding, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct entity-history execution does not support this projection expression",
        )
    })
}

fn direct_state_history_projection_value(
    surface_binding: &SurfaceBinding,
    item: &SelectItem,
    group_by_fields: &[DirectStateHistoryField],
) -> Result<StateHistoryProjectionValue, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before direct state-history projection parsing",
            ))
        }
    };
    if let Some(aggregate) = direct_state_history_aggregate_from_expr(expr)? {
        return Ok(StateHistoryProjectionValue::Aggregate(aggregate));
    }
    let field = direct_state_history_field_from_expr(surface_binding, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution does not support this projection expression",
        )
    })?;
    if !group_by_fields.is_empty() && !group_by_fields.contains(&field) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution only supports grouped projections over GROUP BY columns and COUNT(*)",
        ));
    }
    Ok(StateHistoryProjectionValue::Field(field))
}

fn direct_state_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn direct_entity_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn direct_state_history_aggregate_from_expr(
    expr: &Expr,
) -> Result<Option<StateHistoryAggregate>, LixError> {
    let Expr::Function(function) = expr else {
        return Ok(direct_expr_is_count_star(expr).then_some(StateHistoryAggregate::Count));
    };
    if function.over.is_some()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
    {
        return Ok(None);
    }
    if !function.name.to_string().eq_ignore_ascii_case("count") {
        return Ok(None);
    }
    let FunctionArguments::List(list) = &function.args else {
        return Ok(None);
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return Ok(None);
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(Some(StateHistoryAggregate::Count)),
        _ => Ok(direct_expr_is_count_star(expr).then_some(StateHistoryAggregate::Count)),
    }
}

fn direct_expr_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.clone())
            .unwrap_or_default(),
        _ => expr.to_string(),
    }
}

fn direct_state_history_field_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<DirectStateHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            direct_state_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            direct_state_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::Nested(inner) => direct_state_history_field_from_expr(surface_binding, inner),
        _ => Ok(None),
    }
}

fn direct_entity_history_field_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<DirectEntityHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            direct_entity_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            direct_entity_history_field_from_column_name(surface_binding, &ident.value).map(Some)
        }
        Expr::Nested(inner) => direct_entity_history_field_from_expr(surface_binding, inner),
        _ => Ok(None),
    }
}

fn direct_state_history_sort_value_from_expr(
    surface_binding: &SurfaceBinding,
    expr: &Expr,
) -> Result<Option<StateHistorySortValue>, LixError> {
    if let Some(field) = direct_state_history_field_from_expr(surface_binding, expr)? {
        return Ok(Some(StateHistorySortValue::Field(field)));
    }
    Ok(direct_state_history_aggregate_from_expr(expr)?.map(StateHistorySortValue::Aggregate))
}

fn state_history_sort_value_from_projection_value(
    value: StateHistoryProjectionValue,
) -> StateHistorySortValue {
    match value {
        StateHistoryProjectionValue::Field(field) => StateHistorySortValue::Field(field),
        StateHistoryProjectionValue::Aggregate(aggregate) => {
            StateHistorySortValue::Aggregate(aggregate)
        }
    }
}

fn direct_state_history_field_from_column_name(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Result<DirectStateHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Ok(DirectStateHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(DirectStateHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(DirectStateHistoryField::FileId),
        "plugin_key" | "lixcol_plugin_key" => Ok(DirectStateHistoryField::PluginKey),
        "snapshot_content" => Ok(DirectStateHistoryField::SnapshotContent),
        "metadata" | "lixcol_metadata" => Ok(DirectStateHistoryField::Metadata),
        "schema_version" | "lixcol_schema_version" => Ok(DirectStateHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(DirectStateHistoryField::ChangeId),
        "commit_id" | "lixcol_commit_id" => Ok(DirectStateHistoryField::CommitId),
        "commit_created_at" => Ok(DirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" | "lixcol_root_commit_id" => Ok(DirectStateHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(DirectStateHistoryField::Depth),
        "version_id" | "lixcol_version_id" => Ok(DirectStateHistoryField::VersionId),
        _ => Err(crate::errors::sql_unknown_column_error(
            column,
            Some(&surface_binding.descriptor.public_name),
            &surface_binding
                .exposed_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )),
    }
}

fn direct_entity_history_field_from_column_name(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Result<DirectEntityHistoryField, LixError> {
    let lowercase = column.to_ascii_lowercase();
    if let Ok(field) = direct_state_history_field_from_column_name(surface_binding, column) {
        return Ok(DirectEntityHistoryField::State(field));
    }
    if surface_binding
        .descriptor
        .visible_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        return Ok(DirectEntityHistoryField::Property(lowercase));
    }
    Err(crate::errors::sql_unknown_column_error(
        column,
        Some(&surface_binding.descriptor.public_name),
        &surface_binding
            .descriptor
            .visible_columns
            .iter()
            .chain(surface_binding.descriptor.hidden_columns.iter())
            .map(String::as_str)
            .collect::<Vec<_>>(),
        None,
    ))
}

fn parse_state_history_predicate(
    expr: &Expr,
    surface_binding: &SurfaceBinding,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<StateHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_state_history_predicate(inner, surface_binding, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = direct_state_history_field_from_expr(surface_binding, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(state_history_predicate_from_operator(field, op, value));
                }
                if let Expr::InList { .. } = right.as_ref() {
                    return Ok(None);
                }
            }
            if let Some(field) = direct_state_history_field_from_expr(surface_binding, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(state_history_predicate_from_reversed_operator(
                        field, op, value,
                    ));
                }
            }
            Ok(None)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Ok(None);
            }
            let Some(field) = direct_state_history_field_from_expr(surface_binding, expr)? else {
                return Ok(None);
            };
            let mut values = Vec::new();
            for item in list {
                let Some(value) = direct_value_from_expr(item, params, placeholder_state)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(StateHistoryPredicate::In(field, values)))
        }
        Expr::IsNull(expr) => direct_state_history_field_from_expr(surface_binding, expr)?
            .map(StateHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => direct_state_history_field_from_expr(surface_binding, expr)?
            .map(StateHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn parse_entity_history_predicate(
    expr: &Expr,
    surface_binding: &SurfaceBinding,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<EntityHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_entity_history_predicate(inner, surface_binding, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = direct_entity_history_field_from_expr(surface_binding, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(entity_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = direct_entity_history_field_from_expr(surface_binding, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(entity_history_predicate_from_reversed_operator(field, op, value));
                }
            }
            Ok(None)
        }
        Expr::InList { expr, list, negated } => {
            if *negated {
                return Ok(None);
            }
            let Some(field) = direct_entity_history_field_from_expr(surface_binding, expr)? else {
                return Ok(None);
            };
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let Some(value) = direct_value_from_expr(item, params, placeholder_state)? else {
                    return Ok(None);
                };
                values.push(value);
            }
            Ok(Some(EntityHistoryPredicate::In(field, values)))
        }
        Expr::IsNull(expr) => direct_entity_history_field_from_expr(surface_binding, expr)?
            .map(EntityHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct entity-history execution does not support IS NULL on this expression",
                )
            }),
        Expr::IsNotNull(expr) => direct_entity_history_field_from_expr(surface_binding, expr)?
            .map(EntityHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct entity-history execution does not support IS NOT NULL on this expression",
                )
            }),
        _ => Ok(None),
    }
}

fn entity_history_predicate_from_operator(
    field: DirectEntityHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<EntityHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(EntityHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(EntityHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(EntityHistoryPredicate::Gt(field, value)),
        BinaryOperator::GtEq => Some(EntityHistoryPredicate::GtEq(field, value)),
        BinaryOperator::Lt => Some(EntityHistoryPredicate::Lt(field, value)),
        BinaryOperator::LtEq => Some(EntityHistoryPredicate::LtEq(field, value)),
        _ => None,
    }
}

fn entity_history_predicate_from_reversed_operator(
    field: DirectEntityHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<EntityHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(EntityHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(EntityHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(EntityHistoryPredicate::Lt(field, value)),
        BinaryOperator::GtEq => Some(EntityHistoryPredicate::LtEq(field, value)),
        BinaryOperator::Lt => Some(EntityHistoryPredicate::Gt(field, value)),
        BinaryOperator::LtEq => Some(EntityHistoryPredicate::GtEq(field, value)),
        _ => None,
    }
}

fn parse_state_history_aggregate_predicate(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<StateHistoryAggregatePredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_state_history_aggregate_predicate(inner, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(aggregate) = direct_state_history_aggregate_from_expr(left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(state_history_aggregate_predicate_from_operator(
                        aggregate, op, value,
                    ));
                }
            }
            if let Some(aggregate) = direct_state_history_aggregate_from_expr(right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(state_history_aggregate_predicate_from_reversed_operator(
                        aggregate, op, value,
                    ));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn state_history_aggregate_predicate_from_operator(
    aggregate: StateHistoryAggregate,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryAggregatePredicate> {
    let count = value_as_i64(&value)?;
    match op {
        BinaryOperator::Eq => Some(StateHistoryAggregatePredicate::Eq(aggregate, count)),
        BinaryOperator::NotEq => Some(StateHistoryAggregatePredicate::NotEq(aggregate, count)),
        BinaryOperator::Gt => Some(StateHistoryAggregatePredicate::Gt(aggregate, count)),
        BinaryOperator::GtEq => Some(StateHistoryAggregatePredicate::GtEq(aggregate, count)),
        BinaryOperator::Lt => Some(StateHistoryAggregatePredicate::Lt(aggregate, count)),
        BinaryOperator::LtEq => Some(StateHistoryAggregatePredicate::LtEq(aggregate, count)),
        _ => None,
    }
}

fn state_history_aggregate_predicate_from_reversed_operator(
    aggregate: StateHistoryAggregate,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryAggregatePredicate> {
    let count = value_as_i64(&value)?;
    match op {
        BinaryOperator::Eq => Some(StateHistoryAggregatePredicate::Eq(aggregate, count)),
        BinaryOperator::NotEq => Some(StateHistoryAggregatePredicate::NotEq(aggregate, count)),
        BinaryOperator::Gt => Some(StateHistoryAggregatePredicate::Lt(aggregate, count)),
        BinaryOperator::GtEq => Some(StateHistoryAggregatePredicate::LtEq(aggregate, count)),
        BinaryOperator::Lt => Some(StateHistoryAggregatePredicate::Gt(aggregate, count)),
        BinaryOperator::LtEq => Some(StateHistoryAggregatePredicate::GtEq(aggregate, count)),
        _ => None,
    }
}

fn state_history_predicate_from_operator(
    field: DirectStateHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(StateHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(StateHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(StateHistoryPredicate::Gt(field, value)),
        BinaryOperator::GtEq => Some(StateHistoryPredicate::GtEq(field, value)),
        BinaryOperator::Lt => Some(StateHistoryPredicate::Lt(field, value)),
        BinaryOperator::LtEq => Some(StateHistoryPredicate::LtEq(field, value)),
        _ => None,
    }
}

fn state_history_predicate_from_reversed_operator(
    field: DirectStateHistoryField,
    op: &BinaryOperator,
    value: Value,
) -> Option<StateHistoryPredicate> {
    match op {
        BinaryOperator::Eq => Some(StateHistoryPredicate::Eq(field, value)),
        BinaryOperator::NotEq => Some(StateHistoryPredicate::NotEq(field, value)),
        BinaryOperator::Gt => Some(StateHistoryPredicate::Lt(field, value)),
        BinaryOperator::GtEq => Some(StateHistoryPredicate::LtEq(field, value)),
        BinaryOperator::Lt => Some(StateHistoryPredicate::Gt(field, value)),
        BinaryOperator::LtEq => Some(StateHistoryPredicate::GtEq(field, value)),
        _ => None,
    }
}

fn direct_value_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<Value>, LixError> {
    match expr {
        Expr::Nested(inner) => direct_value_from_expr(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } => {
            let Some(value) = direct_value_from_expr(expr, params, placeholder_state)? else {
                return Ok(None);
            };
            match (op, value) {
                (sqlparser::ast::UnaryOperator::Minus, Value::Integer(value)) => {
                    Ok(Some(Value::Integer(-value)))
                }
                (sqlparser::ast::UnaryOperator::Minus, Value::Real(value)) => {
                    Ok(Some(Value::Real(-value)))
                }
                (sqlparser::ast::UnaryOperator::Plus, value) => Ok(Some(value)),
                _ => Ok(None),
            }
        }
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(token) => {
                let index = resolve_placeholder_index(token, params.len(), placeholder_state)?;
                Ok(Some(params[index].clone()))
            }
            value => Ok(Some(sql_value_to_engine_value(value)?)),
        },
        _ => Ok(None),
    }
}

fn sql_value_to_engine_value(value: &SqlValue) -> Result<Value, LixError> {
    match value {
        SqlValue::Number(raw, _) => raw
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| raw.parse::<f64>().map(Value::Real))
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("could not parse numeric literal '{raw}'"),
                )
            }),
        SqlValue::SingleQuotedString(text)
        | SqlValue::DoubleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::TripleDoubleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::UnicodeStringLiteral(text)
        | SqlValue::NationalStringLiteral(text)
        | SqlValue::SingleQuotedRawStringLiteral(text)
        | SqlValue::DoubleQuotedRawStringLiteral(text)
        | SqlValue::TripleSingleQuotedRawStringLiteral(text)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(text)
        | SqlValue::SingleQuotedByteStringLiteral(text)
        | SqlValue::DoubleQuotedByteStringLiteral(text)
        | SqlValue::TripleSingleQuotedByteStringLiteral(text)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(text) => Ok(Value::Text(text.clone())),
        SqlValue::Boolean(value) => Ok(Value::Boolean(*value)),
        SqlValue::Null => Ok(Value::Null),
        SqlValue::DollarQuotedString(text) => Ok(Value::Text(text.value.clone())),
        SqlValue::HexStringLiteral(text) => {
            Ok(Value::Blob(decode_hex_literal(text).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("could not parse hex literal '{text}': {error}"),
                )
            })?))
        }
        SqlValue::Placeholder(_) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "unexpected placeholder literal during direct state-history preparation",
        )),
    }
}

fn direct_limit_values(
    limit_clause: Option<&LimitClause>,
    params: &[Value],
) -> Result<(Option<u64>, u64), LixError> {
    let Some(limit_clause) = limit_clause else {
        return Ok((None, 0));
    };

    let mut placeholder_state = PlaceholderState::new();
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "direct state-history execution does not support LIMIT BY",
                ));
            }
            let limit = limit
                .as_ref()
                .map(|expr| direct_u64_from_expr(expr, params, &mut placeholder_state))
                .transpose()?;
            let offset = offset
                .as_ref()
                .map(|offset| direct_u64_from_expr(&offset.value, params, &mut placeholder_state))
                .transpose()?
                .unwrap_or(0);
            Ok((limit, offset))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => Ok((
            Some(direct_u64_from_expr(limit, params, &mut placeholder_state)?),
            direct_u64_from_expr(offset, params, &mut placeholder_state)?,
        )),
    }
}

fn decode_hex_literal(text: &str) -> Result<Vec<u8>, &'static str> {
    if text.len() % 2 != 0 {
        return Err("hex literal must have even length");
    }

    let mut bytes = Vec::with_capacity(text.len() / 2);
    let mut chars = text.as_bytes().chunks_exact(2);
    for pair in &mut chars {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn decode_hex_nibble(byte: u8) -> Result<u8, &'static str> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("hex literal contains non-hex characters"),
    }
}

fn direct_u64_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<u64, LixError> {
    let Some(value) = direct_value_from_expr(expr, params, placeholder_state)? else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution requires literal LIMIT/OFFSET values",
        ));
    };
    match value {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        Value::Text(text) => text.parse::<u64>().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("could not parse LIMIT/OFFSET value '{text}'"),
            )
        }),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "direct state-history execution requires integer LIMIT/OFFSET values",
        )),
    }
}

async fn execute_direct_state_history_read(
    backend: &dyn LixBackend,
    plan: &StateHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_state_history_rows(backend, &plan.request).await?;
    rows.retain(|row| state_history_row_matches_predicates(row, &plan.predicates));

    if state_history_plan_uses_grouping(plan) {
        return execute_grouped_direct_state_history_read(rows, plan);
    }

    rows.sort_by(|left, right| compare_state_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_state_history_row(&row, plan))
        .collect();

    Ok(decode_public_read_result_columns(
        QueryResult { rows, columns },
        &plan.result_columns,
    ))
}

async fn execute_direct_entity_history_read(
    backend: &dyn LixBackend,
    plan: &EntityHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let rows = load_state_history_rows(backend, &plan.request).await?;
    let mut rows = rows
        .into_iter()
        .map(EntityHistoryRowView::try_from_state_row)
        .collect::<Result<Vec<_>, _>>()?;
    rows.retain(|row| entity_history_row_matches_predicates(row, &plan.predicates));
    rows.sort_by(|left, right| compare_entity_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_entity_history_row(&row, plan))
        .collect();

    Ok(decode_public_read_result_columns(
        QueryResult { rows, columns },
        &plan.result_columns,
    ))
}

struct EntityHistoryRowView {
    row: StateHistoryRow,
    snapshot: Option<JsonValue>,
}

impl EntityHistoryRowView {
    fn try_from_state_row(row: StateHistoryRow) -> Result<Self, LixError> {
        let snapshot = match row.snapshot_content.as_deref() {
            Some(snapshot) => Some(serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "direct entity-history execution could not parse snapshot_content: {error}"
                    ),
                )
            })?),
            None => None,
        };
        Ok(Self { row, snapshot })
    }
}

fn state_history_plan_uses_grouping(plan: &StateHistoryDirectReadPlan) -> bool {
    !plan.group_by_fields.is_empty()
        || plan.having.is_some()
        || plan
            .projections
            .iter()
            .any(|projection| matches!(projection.value, StateHistoryProjectionValue::Aggregate(_)))
}

fn execute_grouped_direct_state_history_read(
    rows: Vec<StateHistoryRow>,
    plan: &StateHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut groups = BTreeMap::<String, StateHistoryGroupAccumulator>::new();
    for row in rows {
        let group_values = plan
            .group_by_fields
            .iter()
            .map(|field| state_history_field_value(&row, field))
            .collect::<Vec<_>>();
        let key = state_history_group_key(&group_values)?;
        let entry = groups
            .entry(key)
            .or_insert_with(|| StateHistoryGroupAccumulator {
                group_values,
                count: 0,
            });
        entry.count += 1;
    }

    if groups.is_empty() && plan.group_by_fields.is_empty() {
        groups.insert(
            "__all__".to_string(),
            StateHistoryGroupAccumulator {
                group_values: Vec::new(),
                count: 0,
            },
        );
    }

    let mut grouped = groups
        .into_values()
        .filter(|group| state_history_group_matches_having(group, plan.having.as_ref()))
        .collect::<Vec<_>>();
    grouped.sort_by(|left, right| compare_state_history_groups(left, right, plan));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let grouped = grouped.into_iter().skip(offset);
    let grouped = if let Some(limit) = limit {
        grouped.take(limit).collect::<Vec<_>>()
    } else {
        grouped.collect::<Vec<_>>()
    };

    let columns = plan
        .projections
        .iter()
        .map(|projection| projection.output_name.clone())
        .collect();
    let rows = grouped
        .into_iter()
        .map(|group| project_state_history_group(&group, plan))
        .collect();

    Ok(QueryResult { rows, columns })
}

struct StateHistoryGroupAccumulator {
    group_values: Vec<Value>,
    count: i64,
}

fn state_history_group_key(values: &[Value]) -> Result<String, LixError> {
    serde_json::to_string(values).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("could not serialize state-history group key: {error}"),
        )
    })
}

fn state_history_group_matches_having(
    group: &StateHistoryGroupAccumulator,
    predicate: Option<&StateHistoryAggregatePredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    match predicate {
        StateHistoryAggregatePredicate::Eq(StateHistoryAggregate::Count, value) => {
            group.count == *value
        }
        StateHistoryAggregatePredicate::NotEq(StateHistoryAggregate::Count, value) => {
            group.count != *value
        }
        StateHistoryAggregatePredicate::Gt(StateHistoryAggregate::Count, value) => {
            group.count > *value
        }
        StateHistoryAggregatePredicate::GtEq(StateHistoryAggregate::Count, value) => {
            group.count >= *value
        }
        StateHistoryAggregatePredicate::Lt(StateHistoryAggregate::Count, value) => {
            group.count < *value
        }
        StateHistoryAggregatePredicate::LtEq(StateHistoryAggregate::Count, value) => {
            group.count <= *value
        }
    }
}

fn state_history_row_matches_predicates(
    row: &StateHistoryRow,
    predicates: &[StateHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| state_history_row_matches_predicate(row, predicate))
}

fn entity_history_row_matches_predicates(
    row: &EntityHistoryRowView,
    predicates: &[EntityHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| entity_history_row_matches_predicate(row, predicate))
}

fn state_history_row_matches_predicate(
    row: &StateHistoryRow,
    predicate: &StateHistoryPredicate,
) -> bool {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => state_history_field_value(row, field) == *value,
        StateHistoryPredicate::NotEq(field, value) => {
            state_history_field_value(row, field) != *value
        }
        StateHistoryPredicate::Gt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        StateHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        StateHistoryPredicate::Lt(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        StateHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&state_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        StateHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| state_history_field_value(row, field) == *value),
        StateHistoryPredicate::IsNull(field) => {
            matches!(state_history_field_value(row, field), Value::Null)
        }
        StateHistoryPredicate::IsNotNull(field) => {
            !matches!(state_history_field_value(row, field), Value::Null)
        }
    }
}

fn entity_history_row_matches_predicate(
    row: &EntityHistoryRowView,
    predicate: &EntityHistoryPredicate,
) -> bool {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => {
            entity_history_field_value(row, field) == *value
        }
        EntityHistoryPredicate::NotEq(field, value) => {
            entity_history_field_value(row, field) != *value
        }
        EntityHistoryPredicate::Gt(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        EntityHistoryPredicate::Lt(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&entity_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        EntityHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| entity_history_field_value(row, field) == *value),
        EntityHistoryPredicate::IsNull(field) => {
            matches!(entity_history_field_value(row, field), Value::Null)
        }
        EntityHistoryPredicate::IsNotNull(field) => {
            !matches!(entity_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_state_history_rows(
    left: &StateHistoryRow,
    right: &StateHistoryRow,
    sort_keys: &[StateHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in sort_keys {
        let Some(StateHistorySortValue::Field(field)) = &key.value else {
            continue;
        };
        let ordering = compare_public_values(
            &state_history_field_value(left, field),
            &state_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_entity_history_rows(
    left: &EntityHistoryRowView,
    right: &EntityHistoryRowView,
    sort_keys: &[EntityHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &entity_history_field_value(left, field),
            &entity_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_state_history_row(
    row: &StateHistoryRow,
    plan: &StateHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_state_history_field_from_column_name_for_projection(column)
                    .map(|field| state_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| match &projection.value {
            StateHistoryProjectionValue::Field(field) => state_history_field_value(row, field),
            StateHistoryProjectionValue::Aggregate(StateHistoryAggregate::Count) => {
                Value::Integer(1)
            }
        })
        .collect()
}

fn project_entity_history_row(
    row: &EntityHistoryRowView,
    plan: &EntityHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_entity_history_field_from_column_name(&plan.surface_binding, column)
                    .map(|field| entity_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| entity_history_field_value(row, &projection.field))
        .collect()
}

fn entity_history_field_value(
    row: &EntityHistoryRowView,
    field: &DirectEntityHistoryField,
) -> Value {
    match field {
        DirectEntityHistoryField::State(field) => state_history_field_value(&row.row, field),
        DirectEntityHistoryField::Property(property) => row
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get(property))
            .map(json_value_to_public_value)
            .unwrap_or(Value::Null),
    }
}

fn json_value_to_public_value(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = number.as_f64() {
                Value::Real(value)
            } else {
                Value::Text(number.to_string())
            }
        }
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Value::Json(value.clone()),
    }
}

fn compare_state_history_groups(
    left: &StateHistoryGroupAccumulator,
    right: &StateHistoryGroupAccumulator,
    plan: &StateHistoryDirectReadPlan,
) -> std::cmp::Ordering {
    if plan.sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }

    for key in &plan.sort_keys {
        let Some(value) = &key.value else {
            continue;
        };
        let ordering = match value {
            StateHistorySortValue::Field(field) => compare_public_values(
                &state_history_group_field_value(left, &plan.group_by_fields, field),
                &state_history_group_field_value(right, &plan.group_by_fields, field),
            ),
            StateHistorySortValue::Aggregate(StateHistoryAggregate::Count) => {
                compare_public_values(&Value::Integer(left.count), &Value::Integer(right.count))
            }
        }
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }

    std::cmp::Ordering::Equal
}

fn state_history_group_field_value(
    group: &StateHistoryGroupAccumulator,
    group_by_fields: &[DirectStateHistoryField],
    field: &DirectStateHistoryField,
) -> Value {
    group_by_fields
        .iter()
        .position(|candidate| candidate == field)
        .and_then(|index| group.group_values.get(index).cloned())
        .unwrap_or(Value::Null)
}

fn project_state_history_group(
    group: &StateHistoryGroupAccumulator,
    plan: &StateHistoryDirectReadPlan,
) -> Vec<Value> {
    plan.projections
        .iter()
        .map(|projection| match &projection.value {
            StateHistoryProjectionValue::Field(field) => {
                state_history_group_field_value(group, &plan.group_by_fields, field)
            }
            StateHistoryProjectionValue::Aggregate(StateHistoryAggregate::Count) => {
                Value::Integer(group.count)
            }
        })
        .collect()
}

fn direct_state_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<DirectStateHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" => Some(DirectStateHistoryField::EntityId),
        "schema_key" => Some(DirectStateHistoryField::SchemaKey),
        "file_id" => Some(DirectStateHistoryField::FileId),
        "plugin_key" => Some(DirectStateHistoryField::PluginKey),
        "snapshot_content" => Some(DirectStateHistoryField::SnapshotContent),
        "metadata" => Some(DirectStateHistoryField::Metadata),
        "schema_version" => Some(DirectStateHistoryField::SchemaVersion),
        "change_id" => Some(DirectStateHistoryField::ChangeId),
        "commit_id" => Some(DirectStateHistoryField::CommitId),
        "commit_created_at" => Some(DirectStateHistoryField::CommitCreatedAt),
        "root_commit_id" => Some(DirectStateHistoryField::RootCommitId),
        "depth" => Some(DirectStateHistoryField::Depth),
        "version_id" => Some(DirectStateHistoryField::VersionId),
        _ => None,
    }
}

fn state_history_field_value(row: &StateHistoryRow, field: &DirectStateHistoryField) -> Value {
    match field {
        DirectStateHistoryField::EntityId => Value::Text(row.entity_id.clone()),
        DirectStateHistoryField::SchemaKey => Value::Text(row.schema_key.clone()),
        DirectStateHistoryField::FileId => Value::Text(row.file_id.clone()),
        DirectStateHistoryField::PluginKey => Value::Text(row.plugin_key.clone()),
        DirectStateHistoryField::SnapshotContent => row
            .snapshot_content
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectStateHistoryField::Metadata => row
            .metadata
            .as_ref()
            .map(|value: &String| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectStateHistoryField::SchemaVersion => Value::Text(row.schema_version.clone()),
        DirectStateHistoryField::ChangeId => Value::Text(row.change_id.clone()),
        DirectStateHistoryField::CommitId => Value::Text(row.commit_id.clone()),
        DirectStateHistoryField::CommitCreatedAt => Value::Text(row.commit_created_at.clone()),
        DirectStateHistoryField::RootCommitId => Value::Text(row.root_commit_id.clone()),
        DirectStateHistoryField::Depth => Value::Integer(row.depth),
        DirectStateHistoryField::VersionId => Value::Text(row.version_id.clone()),
    }
}

async fn execute_direct_file_history_read(
    backend: &dyn LixBackend,
    plan: &FileHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_file_history_rows(backend, &plan.request).await?;
    rows.retain(|row| file_history_row_matches_predicates(row, &plan.predicates));

    if matches!(plan.aggregate, Some(FileHistoryAggregate::Count)) {
        let columns = vec![plan
            .aggregate_output_name
            .clone()
            .unwrap_or_else(|| "COUNT(*)".to_string())];
        return Ok(QueryResult {
            rows: vec![vec![Value::Integer(rows.len() as i64)]],
            columns,
        });
    }

    rows.sort_by(|left, right| compare_file_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_file_history_row(&row, plan))
        .collect();

    Ok(decode_public_read_result_columns(
        QueryResult { rows, columns },
        &plan.result_columns,
    ))
}

fn file_history_row_matches_predicates(
    row: &FileHistoryRow,
    predicates: &[FileHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| file_history_row_matches_predicate(row, predicate))
}

fn file_history_row_matches_predicate(
    row: &FileHistoryRow,
    predicate: &FileHistoryPredicate,
) -> bool {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => file_history_field_value(row, field) == *value,
        FileHistoryPredicate::NotEq(field, value) => file_history_field_value(row, field) != *value,
        FileHistoryPredicate::Gt(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        FileHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        FileHistoryPredicate::Lt(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        FileHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&file_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        FileHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| file_history_field_value(row, field) == *value),
        FileHistoryPredicate::IsNull(field) => {
            matches!(file_history_field_value(row, field), Value::Null)
        }
        FileHistoryPredicate::IsNotNull(field) => {
            !matches!(file_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_file_history_rows(
    left: &FileHistoryRow,
    right: &FileHistoryRow,
    sort_keys: &[FileHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }
    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &file_history_field_value(left, field),
            &file_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_file_history_row(row: &FileHistoryRow, plan: &FileHistoryDirectReadPlan) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_file_history_field_from_column_name_for_projection(column)
                    .map(|field| file_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| file_history_field_value(row, &projection.field))
        .collect()
}

fn direct_file_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<DirectFileHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(DirectFileHistoryField::Id),
        "path" => Some(DirectFileHistoryField::Path),
        "data" => Some(DirectFileHistoryField::Data),
        "metadata" => Some(DirectFileHistoryField::Metadata),
        "hidden" => Some(DirectFileHistoryField::Hidden),
        "lixcol_entity_id" => Some(DirectFileHistoryField::EntityId),
        "lixcol_schema_key" => Some(DirectFileHistoryField::SchemaKey),
        "lixcol_file_id" => Some(DirectFileHistoryField::FileId),
        "lixcol_version_id" => Some(DirectFileHistoryField::VersionId),
        "lixcol_plugin_key" => Some(DirectFileHistoryField::PluginKey),
        "lixcol_schema_version" => Some(DirectFileHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(DirectFileHistoryField::ChangeId),
        "lixcol_metadata" => Some(DirectFileHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(DirectFileHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(DirectFileHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(DirectFileHistoryField::RootCommitId),
        "lixcol_depth" => Some(DirectFileHistoryField::Depth),
        _ => None,
    }
}

fn file_history_field_value(row: &FileHistoryRow, field: &DirectFileHistoryField) -> Value {
    match field {
        DirectFileHistoryField::Id => Value::Text(row.id.clone()),
        DirectFileHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectFileHistoryField::Data => row
            .data
            .as_ref()
            .map(|value| Value::Blob(value.clone()))
            .unwrap_or(Value::Null),
        DirectFileHistoryField::Metadata => row
            .metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectFileHistoryField::Hidden => row.hidden.map(Value::Boolean).unwrap_or(Value::Null),
        DirectFileHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        DirectFileHistoryField::SchemaKey => Value::Text(row.lixcol_schema_key.clone()),
        DirectFileHistoryField::FileId => Value::Text(row.lixcol_file_id.clone()),
        DirectFileHistoryField::VersionId => Value::Text(row.lixcol_version_id.clone()),
        DirectFileHistoryField::PluginKey => Value::Text(row.lixcol_plugin_key.clone()),
        DirectFileHistoryField::SchemaVersion => Value::Text(row.lixcol_schema_version.clone()),
        DirectFileHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        DirectFileHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectFileHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        DirectFileHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        DirectFileHistoryField::RootCommitId => Value::Text(row.lixcol_root_commit_id.clone()),
        DirectFileHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

async fn execute_direct_directory_history_read(
    backend: &dyn LixBackend,
    plan: &DirectoryHistoryDirectReadPlan,
) -> Result<QueryResult, LixError> {
    let mut rows = load_directory_history_rows(backend, &plan.request).await?;
    rows.retain(|row| directory_history_row_matches_predicates(row, &plan.predicates));

    if matches!(plan.aggregate, Some(DirectoryHistoryAggregate::Count)) {
        let columns = vec![plan
            .aggregate_output_name
            .clone()
            .unwrap_or_else(|| "COUNT(*)".to_string())];
        return Ok(QueryResult {
            rows: vec![vec![Value::Integer(rows.len() as i64)]],
            columns,
        });
    }

    rows.sort_by(|left, right| compare_directory_history_rows(left, right, &plan.sort_keys));

    let offset = usize::try_from(plan.offset).unwrap_or(usize::MAX);
    let limit = plan.limit.and_then(|value| usize::try_from(value).ok());
    let rows = rows.into_iter().skip(offset);
    let rows = if let Some(limit) = limit {
        rows.take(limit).collect::<Vec<_>>()
    } else {
        rows.collect::<Vec<_>>()
    };

    let columns = if plan.wildcard_projection {
        plan.wildcard_columns.clone()
    } else {
        plan.projections
            .iter()
            .map(|projection| projection.output_name.clone())
            .collect()
    };
    let rows = rows
        .into_iter()
        .map(|row| project_directory_history_row(&row, plan))
        .collect();

    Ok(decode_public_read_result_columns(
        QueryResult { rows, columns },
        &plan.result_columns,
    ))
}

fn directory_history_row_matches_predicates(
    row: &DirectoryHistoryRow,
    predicates: &[DirectoryHistoryPredicate],
) -> bool {
    predicates
        .iter()
        .all(|predicate| directory_history_row_matches_predicate(row, predicate))
}

fn directory_history_row_matches_predicate(
    row: &DirectoryHistoryRow,
    predicate: &DirectoryHistoryPredicate,
) -> bool {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => {
            directory_history_field_value(row, field) == *value
        }
        DirectoryHistoryPredicate::NotEq(field, value) => {
            directory_history_field_value(row, field) != *value
        }
        DirectoryHistoryPredicate::Gt(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt())
        }
        DirectoryHistoryPredicate::GtEq(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq())
        }
        DirectoryHistoryPredicate::Lt(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt())
        }
        DirectoryHistoryPredicate::LtEq(field, value) => {
            compare_public_values(&directory_history_field_value(row, field), value)
                .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq())
        }
        DirectoryHistoryPredicate::In(field, values) => values
            .iter()
            .any(|value| directory_history_field_value(row, field) == *value),
        DirectoryHistoryPredicate::IsNull(field) => {
            matches!(directory_history_field_value(row, field), Value::Null)
        }
        DirectoryHistoryPredicate::IsNotNull(field) => {
            !matches!(directory_history_field_value(row, field), Value::Null)
        }
    }
}

fn compare_directory_history_rows(
    left: &DirectoryHistoryRow,
    right: &DirectoryHistoryRow,
    sort_keys: &[DirectoryHistorySortKey],
) -> std::cmp::Ordering {
    if sort_keys.is_empty() {
        return std::cmp::Ordering::Equal;
    }
    for key in sort_keys {
        let Some(field) = &key.field else {
            continue;
        };
        let ordering = compare_public_values(
            &directory_history_field_value(left, field),
            &directory_history_field_value(right, field),
        )
        .unwrap_or(std::cmp::Ordering::Equal);
        if ordering != std::cmp::Ordering::Equal {
            return if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    std::cmp::Ordering::Equal
}

fn project_directory_history_row(
    row: &DirectoryHistoryRow,
    plan: &DirectoryHistoryDirectReadPlan,
) -> Vec<Value> {
    if plan.wildcard_projection {
        return plan
            .wildcard_columns
            .iter()
            .map(|column| {
                direct_directory_history_field_from_column_name_for_projection(column)
                    .map(|field| directory_history_field_value(row, &field))
                    .unwrap_or(Value::Null)
            })
            .collect();
    }

    plan.projections
        .iter()
        .map(|projection| directory_history_field_value(row, &projection.field))
        .collect()
}

fn direct_directory_history_field_from_column_name_for_projection(
    column: &str,
) -> Option<DirectDirectoryHistoryField> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Some(DirectDirectoryHistoryField::Id),
        "parent_id" => Some(DirectDirectoryHistoryField::ParentId),
        "name" => Some(DirectDirectoryHistoryField::Name),
        "path" => Some(DirectDirectoryHistoryField::Path),
        "hidden" => Some(DirectDirectoryHistoryField::Hidden),
        "lixcol_entity_id" => Some(DirectDirectoryHistoryField::EntityId),
        "lixcol_schema_key" => Some(DirectDirectoryHistoryField::SchemaKey),
        "lixcol_file_id" => Some(DirectDirectoryHistoryField::FileId),
        "lixcol_version_id" => Some(DirectDirectoryHistoryField::VersionId),
        "lixcol_plugin_key" => Some(DirectDirectoryHistoryField::PluginKey),
        "lixcol_schema_version" => Some(DirectDirectoryHistoryField::SchemaVersion),
        "lixcol_change_id" => Some(DirectDirectoryHistoryField::ChangeId),
        "lixcol_metadata" => Some(DirectDirectoryHistoryField::LixcolMetadata),
        "lixcol_commit_id" => Some(DirectDirectoryHistoryField::CommitId),
        "lixcol_commit_created_at" => Some(DirectDirectoryHistoryField::CommitCreatedAt),
        "lixcol_root_commit_id" => Some(DirectDirectoryHistoryField::RootCommitId),
        "lixcol_depth" => Some(DirectDirectoryHistoryField::Depth),
        _ => None,
    }
}

fn directory_history_field_value(
    row: &DirectoryHistoryRow,
    field: &DirectDirectoryHistoryField,
) -> Value {
    match field {
        DirectDirectoryHistoryField::Id => Value::Text(row.id.clone()),
        DirectDirectoryHistoryField::ParentId => row
            .parent_id
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectDirectoryHistoryField::Name => Value::Text(row.name.clone()),
        DirectDirectoryHistoryField::Path => row
            .path
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectDirectoryHistoryField::Hidden => {
            row.hidden.map(Value::Boolean).unwrap_or(Value::Null)
        }
        DirectDirectoryHistoryField::EntityId => Value::Text(row.lixcol_entity_id.clone()),
        DirectDirectoryHistoryField::SchemaKey => Value::Text(row.lixcol_schema_key.clone()),
        DirectDirectoryHistoryField::FileId => Value::Text(row.lixcol_file_id.clone()),
        DirectDirectoryHistoryField::VersionId => Value::Text(row.lixcol_version_id.clone()),
        DirectDirectoryHistoryField::PluginKey => Value::Text(row.lixcol_plugin_key.clone()),
        DirectDirectoryHistoryField::SchemaVersion => {
            Value::Text(row.lixcol_schema_version.clone())
        }
        DirectDirectoryHistoryField::ChangeId => Value::Text(row.lixcol_change_id.clone()),
        DirectDirectoryHistoryField::LixcolMetadata => row
            .lixcol_metadata
            .as_ref()
            .map(|value| Value::Text(value.clone()))
            .unwrap_or(Value::Null),
        DirectDirectoryHistoryField::CommitId => Value::Text(row.lixcol_commit_id.clone()),
        DirectDirectoryHistoryField::CommitCreatedAt => {
            Value::Text(row.lixcol_commit_created_at.clone())
        }
        DirectDirectoryHistoryField::RootCommitId => Value::Text(row.lixcol_root_commit_id.clone()),
        DirectDirectoryHistoryField::Depth => Value::Integer(row.lixcol_depth),
    }
}

fn direct_file_history_field_name(field: &DirectFileHistoryField) -> &'static str {
    match field {
        DirectFileHistoryField::Id => "id",
        DirectFileHistoryField::Path => "path",
        DirectFileHistoryField::Data => "data",
        DirectFileHistoryField::Metadata => "metadata",
        DirectFileHistoryField::Hidden => "hidden",
        DirectFileHistoryField::EntityId => "lixcol_entity_id",
        DirectFileHistoryField::SchemaKey => "lixcol_schema_key",
        DirectFileHistoryField::FileId => "lixcol_file_id",
        DirectFileHistoryField::VersionId => "lixcol_version_id",
        DirectFileHistoryField::PluginKey => "lixcol_plugin_key",
        DirectFileHistoryField::SchemaVersion => "lixcol_schema_version",
        DirectFileHistoryField::ChangeId => "lixcol_change_id",
        DirectFileHistoryField::LixcolMetadata => "lixcol_metadata",
        DirectFileHistoryField::CommitId => "lixcol_commit_id",
        DirectFileHistoryField::CommitCreatedAt => "lixcol_commit_created_at",
        DirectFileHistoryField::RootCommitId => "lixcol_root_commit_id",
        DirectFileHistoryField::Depth => "lixcol_depth",
    }
}

fn direct_directory_history_field_name(field: &DirectDirectoryHistoryField) -> &'static str {
    match field {
        DirectDirectoryHistoryField::Id => "id",
        DirectDirectoryHistoryField::ParentId => "parent_id",
        DirectDirectoryHistoryField::Name => "name",
        DirectDirectoryHistoryField::Path => "path",
        DirectDirectoryHistoryField::Hidden => "hidden",
        DirectDirectoryHistoryField::EntityId => "lixcol_entity_id",
        DirectDirectoryHistoryField::SchemaKey => "lixcol_schema_key",
        DirectDirectoryHistoryField::FileId => "lixcol_file_id",
        DirectDirectoryHistoryField::VersionId => "lixcol_version_id",
        DirectDirectoryHistoryField::PluginKey => "lixcol_plugin_key",
        DirectDirectoryHistoryField::SchemaVersion => "lixcol_schema_version",
        DirectDirectoryHistoryField::ChangeId => "lixcol_change_id",
        DirectDirectoryHistoryField::LixcolMetadata => "lixcol_metadata",
        DirectDirectoryHistoryField::CommitId => "lixcol_commit_id",
        DirectDirectoryHistoryField::CommitCreatedAt => "lixcol_commit_created_at",
        DirectDirectoryHistoryField::RootCommitId => "lixcol_root_commit_id",
        DirectDirectoryHistoryField::Depth => "lixcol_depth",
    }
}

fn file_history_predicate_field(predicate: &FileHistoryPredicate) -> DirectFileHistoryField {
    match predicate {
        FileHistoryPredicate::Eq(field, _)
        | FileHistoryPredicate::NotEq(field, _)
        | FileHistoryPredicate::Gt(field, _)
        | FileHistoryPredicate::GtEq(field, _)
        | FileHistoryPredicate::Lt(field, _)
        | FileHistoryPredicate::LtEq(field, _)
        | FileHistoryPredicate::In(field, _)
        | FileHistoryPredicate::IsNull(field)
        | FileHistoryPredicate::IsNotNull(field) => field.clone(),
    }
}

fn state_history_predicate_field(predicate: &StateHistoryPredicate) -> DirectStateHistoryField {
    match predicate {
        StateHistoryPredicate::Eq(field, _)
        | StateHistoryPredicate::NotEq(field, _)
        | StateHistoryPredicate::Gt(field, _)
        | StateHistoryPredicate::GtEq(field, _)
        | StateHistoryPredicate::Lt(field, _)
        | StateHistoryPredicate::LtEq(field, _)
        | StateHistoryPredicate::In(field, _)
        | StateHistoryPredicate::IsNull(field)
        | StateHistoryPredicate::IsNotNull(field) => field.clone(),
    }
}

fn direct_state_history_field_name(field: &DirectStateHistoryField) -> &'static str {
    match field {
        DirectStateHistoryField::EntityId => "entity_id",
        DirectStateHistoryField::SchemaKey => "schema_key",
        DirectStateHistoryField::FileId => "file_id",
        DirectStateHistoryField::PluginKey => "plugin_key",
        DirectStateHistoryField::SnapshotContent => "snapshot_content",
        DirectStateHistoryField::Metadata => "metadata",
        DirectStateHistoryField::SchemaVersion => "schema_version",
        DirectStateHistoryField::ChangeId => "change_id",
        DirectStateHistoryField::CommitId => "commit_id",
        DirectStateHistoryField::CommitCreatedAt => "commit_created_at",
        DirectStateHistoryField::RootCommitId => "root_commit_id",
        DirectStateHistoryField::Depth => "depth",
        DirectStateHistoryField::VersionId => "version_id",
    }
}

fn direct_entity_history_field_name(field: &DirectEntityHistoryField) -> &str {
    match field {
        DirectEntityHistoryField::Property(property) => property.as_str(),
        DirectEntityHistoryField::State(field) => direct_state_history_field_name(field),
    }
}

fn identifier_expr(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}

fn hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn sql_value_expr(value: &Value) -> Expr {
    match value {
        Value::Null => Expr::Value(SqlValue::Null.into()),
        Value::Boolean(value) => Expr::Value(SqlValue::Boolean(*value).into()),
        Value::Integer(value) => Expr::Value(SqlValue::Number(value.to_string(), false).into()),
        Value::Real(value) => Expr::Value(SqlValue::Number(value.to_string(), false).into()),
        Value::Text(value) => Expr::Value(SqlValue::SingleQuotedString(value.clone()).into()),
        Value::Json(value) => Expr::Value(SqlValue::SingleQuotedString(value.to_string()).into()),
        Value::Blob(value) => Expr::Value(SqlValue::HexStringLiteral(hex_string(value)).into()),
    }
}

fn binary_predicate_expr(field_name: &str, op: BinaryOperator, value: &Value) -> Expr {
    Expr::BinaryOp {
        left: Box::new(identifier_expr(field_name)),
        op,
        right: Box::new(sql_value_expr(value)),
    }
}

fn in_list_predicate_expr(field_name: &str, values: &[Value]) -> Expr {
    Expr::InList {
        expr: Box::new(identifier_expr(field_name)),
        list: values.iter().map(sql_value_expr).collect(),
        negated: false,
    }
}

fn entity_history_predicate_expr(predicate: &EntityHistoryPredicate) -> Expr {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::Eq,
            value,
        ),
        EntityHistoryPredicate::NotEq(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::NotEq,
            value,
        ),
        EntityHistoryPredicate::Gt(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::Gt,
            value,
        ),
        EntityHistoryPredicate::GtEq(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::GtEq,
            value,
        ),
        EntityHistoryPredicate::Lt(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::Lt,
            value,
        ),
        EntityHistoryPredicate::LtEq(field, value) => binary_predicate_expr(
            direct_entity_history_field_name(field),
            BinaryOperator::LtEq,
            value,
        ),
        EntityHistoryPredicate::In(field, values) => {
            in_list_predicate_expr(direct_entity_history_field_name(field), values)
        }
        EntityHistoryPredicate::IsNull(field) => Expr::IsNull(Box::new(identifier_expr(
            direct_entity_history_field_name(field),
        ))),
        EntityHistoryPredicate::IsNotNull(field) => Expr::IsNotNull(Box::new(identifier_expr(
            direct_entity_history_field_name(field),
        ))),
    }
}

fn compare_public_values(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Integer(left), Value::Integer(right)) => Some(left.cmp(right)),
        (Value::Real(left), Value::Real(right)) => left.partial_cmp(right),
        (Value::Integer(left), Value::Real(right)) => (*left as f64).partial_cmp(right),
        (Value::Real(left), Value::Integer(right)) => left.partial_cmp(&(*right as f64)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Boolean(left), Value::Boolean(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(text) => Some(text.as_str()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        _ => None,
    }
}

enum SpecializedPublicReadPreparation {
    Prepared(PreparedPublicRead),
    Declined {
        reason: String,
        bound_statement: BoundStatement,
    },
}

fn parse_public_read_unknown_column_name(message: &str) -> Option<String> {
    let prefix = "strict rewrite violation: unknown column '";
    let start = message.find(prefix)? + prefix.len();
    let end = message[start..].find('\'')? + start;
    let column = &message[start..end];
    (!column.is_empty()).then(|| column.to_string())
}

fn public_read_preparation_error(bindings: &[SurfaceBinding], message: &str) -> Option<LixError> {
    let missing_column = parse_public_read_unknown_column_name(message)?;
    let binding = if bindings.len() == 1 {
        bindings.first()
    } else {
        bindings
            .iter()
            .find(|binding| message.contains(&format!("on '{}'", binding.descriptor.public_name)))
    }?;
    let available_columns = binding
        .descriptor
        .visible_columns
        .iter()
        .chain(binding.descriptor.hidden_columns.iter())
        .map(String::as_str)
        .collect::<Vec<_>>();
    Some(crate::errors::sql_unknown_column_error(
        &missing_column,
        Some(&binding.descriptor.public_name),
        available_columns.as_slice(),
        None,
    ))
}

async fn try_prepare_public_read_via_specialized_optimization(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    active_version_id: &str,
    explain_request: Option<&crate::sql::explain::ExplainRequest>,
    registry: &SurfaceRegistry,
    public_output_columns: Option<Vec<String>>,
    mut stage_timings: ExplainTimingCollector,
) -> Result<SpecializedPublicReadPreparation, LixError> {
    // Specialized public-read stage semantics:
    // - semantic_analysis: canonicalize the bound statement into a structured public read and
    //   derive dependency/effective-state semantics.
    // - logical_planning: construct the structured logical plan before any execution-strategy
    //   routing happens.
    // - routing: choose between direct-history execution and lowered-SQL execution.
    // - capability_resolution: load external schemas/layouts needed to lower specialized SQL
    //   execution once the strategy requires backend capability state.
    // - physical_planning: build the direct history plan or the lowered read program after any
    //   required capability resolution has completed.
    // - executor_preparation: render lowered backend SQL from a lowered read program. Direct
    //   history plans omit this stage because they do not prepare backend SQL text.
    let runtime_bindings =
        runtime_binding_values_from_execution_context(&bound_statement.execution_context)?;
    let bound_parameters = bound_statement.bound_parameters.clone();
    let semantic_started = Instant::now();
    let analysis = match prepare_structured_public_read_analysis(
        backend,
        bound_statement,
        active_version_id,
        registry,
    )
    .await?
    {
        StructuredPublicReadPreparation::Prepared(analysis) => analysis,
        StructuredPublicReadPreparation::Declined(bound_statement) => {
            return Ok(SpecializedPublicReadPreparation::Declined {
                reason: "specialized read optimization declined canonicalization".to_string(),
                bound_statement,
            });
        }
    };
    stage_timings.record(ExplainStage::SemanticAnalysis, semantic_started.elapsed());
    let structured_read = analysis.structured_read().clone();
    let surface_binding = structured_read.surface_binding.clone();
    let freshness_contract = surface_binding.read_freshness;
    let effective_state_request = analysis.semantics.effective_state_request.clone();
    let effective_state_plan = analysis.semantics.effective_state_plan.clone();
    let logical_started = Instant::now();
    let logical_plan = analysis.logical_plan();
    stage_timings.record(ExplainStage::LogicalPlanning, logical_started.elapsed());
    let routing_started = Instant::now();
    let strategy_decision = route_public_read_execution_strategy(&surface_binding);
    let direct_execution =
        strategy_decision.direct_execution && is_direct_only_history_surface(&surface_binding);
    stage_timings.record(ExplainStage::Routing, routing_started.elapsed());
    let routing_passes = strategy_decision.pass_traces;

    let physical_started = Instant::now();
    let (execution, pushdown_decision) = if direct_execution {
        match (
            surface_binding.descriptor.surface_family,
            surface_binding.descriptor.public_name.as_str(),
        ) {
            (SurfaceFamily::State, "lix_state_history") => {
                match build_direct_state_history_plan(&structured_read) {
                    Ok(Some(plan)) => {
                        let pushdown_decision = direct_state_history_pushdown_decision(&plan);
                        (
                            PreparedPublicReadExecution::Direct(
                                DirectPublicReadPlan::StateHistory(plan),
                            ),
                            pushdown_decision,
                        )
                    }
                    Ok(None) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: format!(
                                "specialized read optimization declined '{}'",
                                structured_read.surface_binding.descriptor.public_name
                            ),
                            bound_statement: analysis.bound_statement,
                        })
                    }
                    Err(error) if specialized_public_read_error_is_semantic(&error) => {
                        return Err(error)
                    }
                    Err(error) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: error.description,
                            bound_statement: analysis.bound_statement,
                        })
                    }
                }
            }
            (SurfaceFamily::Entity, _) => {
                match build_direct_entity_history_plan(&structured_read) {
                    Ok(Some(plan)) => {
                        let pushdown_decision = direct_entity_history_pushdown_decision(&plan);
                        (
                            PreparedPublicReadExecution::Direct(
                                DirectPublicReadPlan::EntityHistory(plan),
                            ),
                            pushdown_decision,
                        )
                    }
                    Ok(None) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: format!(
                                "specialized read optimization declined '{}'",
                                structured_read.surface_binding.descriptor.public_name
                            ),
                            bound_statement: analysis.bound_statement,
                        })
                    }
                    Err(error) if specialized_public_read_error_is_semantic(&error) => {
                        return Err(error)
                    }
                    Err(error) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: error.description,
                            bound_statement: analysis.bound_statement,
                        })
                    }
                }
            }
            (SurfaceFamily::Filesystem, "lix_directory_history") => {
                match build_direct_directory_history_plan(&structured_read) {
                    Ok(Some(plan)) => {
                        let pushdown_decision = direct_directory_history_pushdown_decision(&plan);
                        (
                            PreparedPublicReadExecution::Direct(
                                DirectPublicReadPlan::DirectoryHistory(plan),
                            ),
                            pushdown_decision,
                        )
                    }
                    Ok(None) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: format!(
                                "specialized read optimization declined '{}'",
                                structured_read.surface_binding.descriptor.public_name
                            ),
                            bound_statement: analysis.bound_statement,
                        })
                    }
                    Err(error) if specialized_public_read_error_is_semantic(&error) => {
                        return Err(error)
                    }
                    Err(error) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: error.description,
                            bound_statement: analysis.bound_statement,
                        })
                    }
                }
            }
            (SurfaceFamily::Filesystem, _) => {
                match build_direct_file_history_plan(&structured_read) {
                    Ok(Some(plan)) => {
                        let pushdown_decision = direct_file_history_pushdown_decision(&plan);
                        (
                            PreparedPublicReadExecution::Direct(DirectPublicReadPlan::FileHistory(
                                plan,
                            )),
                            pushdown_decision,
                        )
                    }
                    Ok(None) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: format!(
                                "specialized read optimization declined '{}'",
                                structured_read.surface_binding.descriptor.public_name
                            ),
                            bound_statement: analysis.bound_statement,
                        })
                    }
                    Err(error) if specialized_public_read_error_is_semantic(&error) => {
                        return Err(error)
                    }
                    Err(error) => {
                        return Ok(SpecializedPublicReadPreparation::Declined {
                            reason: error.description,
                            bound_statement: analysis.bound_statement,
                        })
                    }
                }
            }
            _ => {
                unreachable!("direct_execution already restricted to direct-only history surfaces")
            }
        }
    } else {
        let capability_started = Instant::now();
        let known_live_layouts = load_known_live_layouts_for_public_read(
            backend,
            &structured_read,
            analysis.dependency_spec.as_ref(),
            analysis.semantics.effective_state_request.as_ref(),
        )
        .await?;
        stage_timings.record(
            ExplainStage::CapabilityResolution,
            capability_started.elapsed(),
        );
        let current_version_heads =
            load_local_version_heads_for_surface(backend, &surface_binding).await?;
        let lowered_read = match lower_read_for_execution_with_layouts(
            backend.dialect(),
            &structured_read,
            analysis.semantics.effective_state_request.as_ref(),
            analysis.semantics.effective_state_plan.as_ref(),
            &known_live_layouts,
            &current_version_heads,
        ) {
            Ok(Some(program)) => Ok::<LoweredReadProgram, LixError>(program),
            Ok(None) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: format!(
                        "specialized read optimization declined '{}'",
                        structured_read.surface_binding.descriptor.public_name
                    ),
                    bound_statement: analysis.bound_statement,
                })
            }
            Err(error) if specialized_public_read_error_is_semantic(&error) => return Err(error),
            Err(error) => {
                return Ok(SpecializedPublicReadPreparation::Declined {
                    reason: error.description,
                    bound_statement: analysis.bound_statement,
                })
            }
        }?;
        let pushdown_decision = Some(lowered_read.pushdown_decision.clone());
        (
            PreparedPublicReadExecution::LoweredSql(lowered_read),
            pushdown_decision,
        )
    };
    stage_timings.record(ExplainStage::PhysicalPlanning, physical_started.elapsed());

    let lowered_sql = match &execution {
        PreparedPublicReadExecution::LoweredSql(lowered_read) => {
            let executor_started = Instant::now();
            let lowered_sql = render_lowered_read_sql(
                lowered_read,
                &analysis.bound_statement.bound_parameters,
                &analysis.bound_statement.execution_context,
                backend.dialect(),
            )?;
            stage_timings.record(
                ExplainStage::ExecutorPreparation,
                executor_started.elapsed(),
            );
            lowered_sql
        }
        PreparedPublicReadExecution::Direct(_) => Vec::new(),
    };

    let optimized_logical_plan = match &execution {
        PreparedPublicReadExecution::LoweredSql(_) => logical_plan.clone(),
        PreparedPublicReadExecution::Direct(direct_plan) => {
            analysis.logical_plan_with_direct_execution(direct_plan.clone())
        }
    };
    verify_logical_plan(&LogicalPlan::PublicRead(optimized_logical_plan.clone())).map_err(
        |error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public read logical plan verification failed: {}",
                    error.message
                ),
            )
        },
    )?;
    let explain = build_public_read_explain_artifacts(PublicReadExplainBuildInput {
        request: explain_request.cloned(),
        semantics: analysis.semantics.clone(),
        logical_plan: logical_plan.clone(),
        optimized_logical_plan: optimized_logical_plan.clone(),
        execution: execution.clone(),
        runtime_artifacts: PublicReadExplainRuntimeArtifacts {
            pushdown_decision: pushdown_decision.clone(),
            lowered_sql,
        },
        routing_passes: routing_passes.clone(),
        stage_timings: stage_timings.finish(),
    })?;

    Ok(SpecializedPublicReadPreparation::Prepared(
        PreparedPublicRead {
            optimization: Some(PublicReadOptimization {
                structured_read,
                effective_state_request: effective_state_request.clone(),
                effective_state_plan: effective_state_plan.clone(),
            }),
            freshness_contract,
            surface_bindings: analysis
                .semantics
                .surface_bindings
                .iter()
                .map(|binding| binding.descriptor.public_name.clone())
                .collect(),
            logical_plan: optimized_logical_plan,
            bound_parameters,
            runtime_bindings,
            public_output_columns,
            explain,
            execution,
        },
    ))
}

async fn load_local_version_heads_for_surface(
    backend: &dyn LixBackend,
    surface_binding: &SurfaceBinding,
) -> Result<BTreeMap<String, String>, LixError> {
    if surface_binding.descriptor.surface_family != SurfaceFamily::Admin
        || surface_binding.descriptor.public_name != "lix_version"
    {
        return Ok(BTreeMap::new());
    }

    Ok(
        load_current_committed_version_frontier_with_backend(backend)
            .await?
            .version_heads,
    )
}

fn direct_state_history_pushdown_decision(
    plan: &StateHistoryDirectReadPlan,
) -> Option<PushdownDecision> {
    let mut accepted_predicates = Vec::new();
    if let StateHistoryRootScope::RequestedRoots(root_commit_ids) = &plan.request.root_scope {
        for root_commit_id in root_commit_ids {
            accepted_predicates.push(binary_predicate_expr(
                "root_commit_id",
                BinaryOperator::Eq,
                &Value::Text(root_commit_id.clone()),
            ));
        }
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates: Vec::new(),
    })
}

fn direct_entity_history_pushdown_decision(
    plan: &EntityHistoryDirectReadPlan,
) -> Option<PushdownDecision> {
    let mut accepted_predicates = Vec::new();
    let mut residual_predicates = Vec::new();

    if let StateHistoryRootScope::RequestedRoots(root_commit_ids) = &plan.request.root_scope {
        for root_commit_id in root_commit_ids {
            accepted_predicates.push(binary_predicate_expr(
                "root_commit_id",
                BinaryOperator::Eq,
                &Value::Text(root_commit_id.clone()),
            ));
        }
    }
    if let StateHistoryVersionScope::RequestedVersions(version_ids) = &plan.request.version_scope {
        for version_id in version_ids {
            accepted_predicates.push(binary_predicate_expr(
                "version_id",
                BinaryOperator::Eq,
                &Value::Text(version_id.clone()),
            ));
        }
    }
    for entity_id in &plan.request.entity_ids {
        accepted_predicates.push(binary_predicate_expr(
            "entity_id",
            BinaryOperator::Eq,
            &Value::Text(entity_id.clone()),
        ));
    }
    for file_id in &plan.request.file_ids {
        accepted_predicates.push(binary_predicate_expr(
            "file_id",
            BinaryOperator::Eq,
            &Value::Text(file_id.clone()),
        ));
    }
    for plugin_key in &plan.request.plugin_keys {
        accepted_predicates.push(binary_predicate_expr(
            "plugin_key",
            BinaryOperator::Eq,
            &Value::Text(plugin_key.clone()),
        ));
    }
    if let Some(min_depth) = plan.request.min_depth {
        if plan.request.max_depth == Some(min_depth) {
            accepted_predicates.push(binary_predicate_expr(
                "depth",
                BinaryOperator::Eq,
                &Value::Integer(min_depth),
            ));
        } else {
            accepted_predicates.push(binary_predicate_expr(
                "depth",
                BinaryOperator::GtEq,
                &Value::Integer(min_depth),
            ));
        }
    }
    if let Some(max_depth) = plan.request.max_depth {
        if plan.request.min_depth != Some(max_depth) {
            accepted_predicates.push(binary_predicate_expr(
                "depth",
                BinaryOperator::LtEq,
                &Value::Integer(max_depth),
            ));
        }
    }

    for predicate in &plan.predicates {
        let field = match predicate {
            EntityHistoryPredicate::Eq(field, _)
            | EntityHistoryPredicate::NotEq(field, _)
            | EntityHistoryPredicate::Gt(field, _)
            | EntityHistoryPredicate::GtEq(field, _)
            | EntityHistoryPredicate::Lt(field, _)
            | EntityHistoryPredicate::LtEq(field, _)
            | EntityHistoryPredicate::In(field, _)
            | EntityHistoryPredicate::IsNull(field)
            | EntityHistoryPredicate::IsNotNull(field) => field,
        };
        if matches!(field, DirectEntityHistoryField::Property(_)) {
            residual_predicates.push(entity_history_predicate_expr(predicate));
        }
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates,
    })
}

fn direct_file_history_pushdown_decision(
    plan: &FileHistoryDirectReadPlan,
) -> Option<PushdownDecision> {
    let mut accepted_predicates = Vec::new();
    if let FileHistoryRootScope::RequestedRoots(root_commit_ids) = &plan.request.root_scope {
        for root_commit_id in root_commit_ids {
            accepted_predicates.push(binary_predicate_expr(
                "root_commit_id",
                BinaryOperator::Eq,
                &Value::Text(root_commit_id.clone()),
            ));
        }
    }
    if let FileHistoryVersionScope::RequestedVersions(version_ids) = &plan.request.version_scope {
        for version_id in version_ids {
            accepted_predicates.push(binary_predicate_expr(
                "version_id",
                BinaryOperator::Eq,
                &Value::Text(version_id.clone()),
            ));
        }
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates: Vec::new(),
    })
}

fn direct_directory_history_pushdown_decision(
    plan: &DirectoryHistoryDirectReadPlan,
) -> Option<PushdownDecision> {
    let mut accepted_predicates = Vec::new();
    if let FileHistoryRootScope::RequestedRoots(root_commit_ids) = &plan.request.root_scope {
        for root_commit_id in root_commit_ids {
            accepted_predicates.push(binary_predicate_expr(
                "root_commit_id",
                BinaryOperator::Eq,
                &Value::Text(root_commit_id.clone()),
            ));
        }
    }
    if let FileHistoryVersionScope::RequestedVersions(version_ids) = &plan.request.version_scope {
        for version_id in version_ids {
            accepted_predicates.push(binary_predicate_expr(
                "version_id",
                BinaryOperator::Eq,
                &Value::Text(version_id.clone()),
            ));
        }
    }
    for directory_id in &plan.request.directory_ids {
        accepted_predicates.push(binary_predicate_expr(
            "id",
            BinaryOperator::Eq,
            &Value::Text(directory_id.clone()),
        ));
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates: Vec::new(),
    })
}

fn specialized_public_read_error_is_semantic(error: &LixError) -> bool {
    error.code == "LIX_ERROR_SQL_UNKNOWN_COLUMN"
        || error
            .description
            .contains("lix_state does not expose version_id")
}

#[cfg(test)]
pub(super) async fn try_prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    try_prepare_public_read_with_registry_and_internal_access(
        backend,
        &registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
        None,
    )
    .await
}

pub(super) async fn try_prepare_public_read_with_registry_and_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    parse_duration: Option<Duration>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    try_prepare_public_read_with_internal_access(
        backend,
        registry,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        allow_internal_tables,
        parse_duration,
    )
    .await
}

async fn try_prepare_public_read_with_internal_access(
    backend: &dyn LixBackend,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    parse_duration: Option<Duration>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    // Public-read stage ownership starts here after `parse` has already
    // produced the SQL AST:
    // - bind: `bind_public_read_statement` performs generic statement binding.
    //   For broad public reads it also performs the broad front-end bind that
    //   produces typed broad IR, so later stages start from already-bound
    //   broad statements.
    if parsed_statements.len() != 1 {
        return Ok(None);
    }
    let explained = unwrap_explain_statement(&parsed_statements[0])?;
    let statement = explained.statement;
    let explain_request = explained.request;
    if let Some(error) = public_read_preflight_error(&registry, &statement) {
        return Err(error);
    }
    let public_output_columns = if explain_request.is_none() {
        public_output_columns_from_statement(&statement)
    } else {
        None
    };
    let read_summary = summarize_bound_public_read_statement(&registry, &statement);
    if read_summary.bound_surface_bindings.len() > 1
        && bound_summary_contains_direct_only_history_surface(&read_summary)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read preparation failed: direct-only history surfaces cannot participate in broad surface lowering",
        ));
    }
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }
    let bind_started = Instant::now();
    let bound_public_read = bind_public_read_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
            active_account_ids: Vec::new(),
        },
        &registry,
    )?;
    let bound_statement = bound_public_read.bound_statement;
    let broad_statement = bound_public_read.broad_statement;
    let mut stage_timings = ExplainTimingCollector::new(parse_duration);
    stage_timings.record(ExplainStage::Bind, bind_started.elapsed());
    let mut attempted_broad_lowering = false;
    if read_summary.bound_surface_bindings.len() > 1 {
        attempted_broad_lowering = true;
        if let Some(prepared) = prepare_public_read_via_surface_lowering(
            backend,
            bound_statement.clone(),
            broad_statement.clone(),
            explain_request.as_ref(),
            &registry,
            allow_internal_tables,
            public_output_columns.clone(),
            stage_timings.clone(),
        )
        .await?
        {
            return Ok(Some(prepared));
        }
    }
    let specialized = try_prepare_public_read_via_specialized_optimization(
        backend,
        bound_statement,
        active_version_id,
        explain_request.as_ref(),
        &registry,
        public_output_columns.clone(),
        stage_timings.clone(),
    )
    .await?;
    match specialized {
        SpecializedPublicReadPreparation::Prepared(prepared) => return Ok(Some(prepared)),
        SpecializedPublicReadPreparation::Declined {
            reason,
            bound_statement,
        } => {
            if !attempted_broad_lowering {
                if let Some(prepared) = prepare_public_read_via_surface_lowering(
                    backend,
                    bound_statement,
                    broad_statement,
                    explain_request.as_ref(),
                    &registry,
                    allow_internal_tables,
                    public_output_columns,
                    stage_timings,
                )
                .await?
                {
                    return Ok(Some(prepared));
                }
            }
            if let Some(error) =
                public_read_preparation_error(&read_summary.bound_surface_bindings, &reason)
            {
                return Err(error);
            }
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("public read preparation failed: {reason}"),
            ));
        }
    }
}

pub(super) async fn prepare_public_read_via_surface_lowering(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    broad_statement: Option<BroadPublicReadStatement>,
    explain_request: Option<&crate::sql::explain::ExplainRequest>,
    registry: &SurfaceRegistry,
    allow_internal_tables: bool,
    public_output_columns: Option<Vec<String>>,
    mut stage_timings: ExplainTimingCollector,
) -> Result<Option<PreparedPublicRead>, LixError> {
    // Broad public-read stage semantics:
    // - bind: completed by `try_prepare_public_read_with_internal_access`
    //   before this helper runs. This owns generic binding plus the broad
    //   front-end bind that produces typed broad IR.
    // - logical_planning: construct and verify the typed broad logical plan
    //   from already-bound broad IR.
    // - capability_resolution: load external schemas/layouts required before broad routing
    //   can choose stable lowered relations.
    // - routing: route typed broad public relations into lowerable broad relations.
    // - physical_planning: lower the optimized typed broad statement into the
    //   lowered read program.
    // - executor_preparation: render backend SQL from the lowered read program.
    // Broad lowering does not run structured semantic analysis, so semantic_analysis is omitted.
    let logical_started = Instant::now();
    let read_summary = summarize_bound_public_read_statement(registry, &bound_statement.statement);
    if bound_summary_contains_direct_only_history_surface(&read_summary) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public read preparation failed: direct-only history surfaces do not support broad surface lowering",
        ));
    }
    if read_summary.bound_surface_bindings.is_empty() {
        return Ok(None);
    }
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }
    let broad_statement = broad_statement.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "broad public read preparation failed: typed broad statement was unavailable after binding",
        )
    })?;

    let dependency_spec = augment_dependency_spec_for_broad_public_read(
        registry,
        derive_dependency_spec_from_bound_public_surface_bindings(
            &read_summary.bound_surface_bindings,
        ),
    );
    if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref()) {
        return Err(error);
    }

    let logical_plan = PublicReadLogicalPlan::Broad {
        broad_statement: Box::new(broad_statement.clone()),
        surface_bindings: read_summary.bound_surface_bindings.clone(),
        dependency_spec: dependency_spec.clone(),
    };
    verify_logical_plan(&LogicalPlan::PublicRead(logical_plan.clone())).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public read logical plan verification failed: {}",
                error.message
            ),
        )
    })?;
    stage_timings.record(ExplainStage::LogicalPlanning, logical_started.elapsed());

    let active_version_id = bound_statement
        .execution_context
        .requested_version_id
        .as_deref();
    let capability_started = Instant::now();
    let known_live_layouts = load_known_live_layouts_for_broad_public_read(
        backend,
        registry,
        &read_summary.bound_surface_bindings,
    )
    .await?;
    stage_timings.record(
        ExplainStage::CapabilityResolution,
        capability_started.elapsed(),
    );

    let routing_started = Instant::now();
    let routed_broad_read = match route_broad_public_read_statement_with_known_live_layouts(
        &broad_statement,
        registry,
        backend.dialect(),
        active_version_id,
        &known_live_layouts,
    ) {
        Ok(optimized) => optimized,
        Err(error) => {
            if let Some(mapped) = public_read_preparation_error(
                &read_summary.bound_surface_bindings,
                &error.description,
            ) {
                return Err(mapped);
            }
            return Err(error);
        }
    };
    let optimized_logical_plan = PublicReadLogicalPlan::Broad {
        broad_statement: Box::new(routed_broad_read.broad_statement.clone()),
        surface_bindings: read_summary.bound_surface_bindings.clone(),
        dependency_spec: dependency_spec.clone(),
    };
    verify_logical_plan(&LogicalPlan::PublicRead(optimized_logical_plan.clone())).map_err(
        |error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public read optimized logical plan verification failed: {}",
                    error.message
                ),
            )
        },
    )?;
    stage_timings.record(ExplainStage::Routing, routing_started.elapsed());

    let physical_started = Instant::now();
    let Some(lowered_read) = lower_broad_public_read_for_execution_with_layouts(
        &routed_broad_read.broad_statement,
        registry,
        backend.dialect(),
        bound_statement.bound_parameters.len(),
        active_version_id,
        &known_live_layouts,
    )?
    else {
        return Ok(None);
    };
    stage_timings.record(ExplainStage::PhysicalPlanning, physical_started.elapsed());
    let freshness_contract =
        super::bound_surface_freshness_contract(&read_summary.bound_surface_bindings)
            .expect("broad public read should bind at least one surface");

    let semantic_read = PublicReadSemantics {
        surface_bindings: read_summary.bound_surface_bindings.clone(),
        broad_statement: Some(Box::new(broad_statement)),
        structured_read: None,
        effective_state_request: None,
        effective_state_plan: None,
    };

    let executor_started = Instant::now();
    let lowered_sql = render_lowered_read_sql(
        &lowered_read,
        &bound_statement.bound_parameters,
        &bound_statement.execution_context,
        backend.dialect(),
    )?;
    stage_timings.record(
        ExplainStage::ExecutorPreparation,
        executor_started.elapsed(),
    );
    let surface_bindings = semantic_read
        .surface_bindings
        .iter()
        .map(|binding| binding.descriptor.public_name.clone())
        .collect();
    let explain = build_public_read_explain_artifacts(PublicReadExplainBuildInput {
        request: explain_request.cloned(),
        semantics: semantic_read,
        logical_plan: logical_plan.clone(),
        optimized_logical_plan: optimized_logical_plan.clone(),
        execution: PreparedPublicReadExecution::LoweredSql(lowered_read.clone()),
        runtime_artifacts: PublicReadExplainRuntimeArtifacts {
            pushdown_decision: Some(PushdownDecision::default()),
            lowered_sql,
        },
        routing_passes: routed_broad_read.pass_traces.clone(),
        stage_timings: stage_timings.finish(),
    })?;

    Ok(Some(PreparedPublicRead {
        optimization: None,
        freshness_contract,
        surface_bindings,
        logical_plan,
        bound_parameters: bound_statement.bound_parameters.clone(),
        runtime_bindings: runtime_binding_values_from_execution_context(
            &bound_statement.execution_context,
        )?,
        public_output_columns,
        explain,
        execution: PreparedPublicReadExecution::LoweredSql(lowered_read),
    }))
}

fn bound_summary_contains_direct_only_history_surface(
    read_summary: &BoundPublicReadSummary,
) -> bool {
    read_summary
        .bound_surface_bindings
        .iter()
        .any(is_direct_only_history_surface)
}

fn is_direct_only_history_surface(binding: &SurfaceBinding) -> bool {
    binding.descriptor.surface_variant == SurfaceVariant::History
        && matches!(
            binding.descriptor.surface_family,
            SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
        )
}

#[cfg(test)]
pub(super) async fn prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<PreparedPublicRead> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
pub(super) async fn prepare_public_read_strict(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<PreparedPublicRead>, LixError> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}
