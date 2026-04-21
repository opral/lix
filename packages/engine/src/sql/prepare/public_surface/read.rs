use super::*;
use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, CatalogHistoryReadSemantics,
    ResolvedRelation, SurfaceFamily, SurfaceRegistry,
};
use crate::history::{
    DirectoryHistoryRequest, FileHistoryContentMode, FileHistoryLineageScope, FileHistoryRequest,
    FileHistoryRootScope, FileHistoryVersionScope, StateHistoryContentMode,
    StateHistoryLineageScope, StateHistoryRequest, StateHistoryRootScope, StateHistoryVersionScope,
};
use crate::sql::binder::{bind_public_read_statement, RuntimeBindingValues};
use crate::sql::explain::{
    build_public_read_explain_artifacts, unwrap_explain_statement, ExplainStage,
    ExplainTimingCollector, PublicReadExplainBuildInput, PublicReadExplainCompiledArtifacts,
};
use crate::sql::logical_plan::public_ir::BroadPublicReadStatement;
use crate::sql::logical_plan::{
    verify_logical_plan, DirectoryHistoryAggregate, DirectoryHistoryField,
    DirectoryHistoryPredicate, DirectoryHistoryProjection, DirectoryHistoryReadPlan,
    DirectoryHistorySortKey, EntityHistoryField, EntityHistoryPredicate, EntityHistoryProjection,
    EntityHistoryReadPlan, EntityHistorySortKey, FileHistoryAggregate, FileHistoryField,
    FileHistoryPredicate, FileHistoryProjection, FileHistoryReadPlan, FileHistorySortKey,
    HistoryReadPlan, LogicalPlan, PublicReadLogicalPlan, StateHistoryAggregate,
    StateHistoryAggregatePredicate, StateHistoryField, StateHistoryPredicate,
    StateHistoryProjection, StateHistoryProjectionValue, StateHistoryReadPlan, StateHistorySortKey,
    StateHistorySortValue,
};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::sql::physical_plan::lowerer::lower_broad_public_read_for_execution_with_layouts;
use crate::sql::physical_plan::{
    compile_prepared_batch_public_read_execution, compile_public_rowset_query,
    compile_read_time_projection_execution, CompilerOwnedPublicReadExecutionSelection,
    LoweredReadBatch, LoweredResultColumn, LoweredResultColumns, PublicReadPhysicalPlan,
};
use crate::sql::prepare::public_surface::routing::{
    classify_public_read_plan_kind, route_broad_public_read_statement_with_known_live_layouts,
    PublicReadPlanKind,
};
use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_bound_public_surface_bindings;
use crate::sql::semantic_ir::{
    augment_dependency_spec_for_broad_public_read, prepare_structured_public_read_analysis,
    unknown_public_state_schema_error, PublicReadSemantics, StructuredPublicReadDecision,
};
use crate::sql::{
    PendingOverlayLane, PendingOverlayQuery, PreparedPublicReadContract, PublicReadResultColumn,
    PublicReadResultColumns,
};
use crate::SqlDialect;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr, Statement, Value as SqlValue,
};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

pub(crate) fn prepared_public_read_contract(
    prepared: &PublicReadPlan,
) -> PreparedPublicReadContract {
    PreparedPublicReadContract {
        committed_mode: committed_read_mode_from_prepared_public_read(prepared),
        pending_overlay_query: pending_overlay_query_from_prepared_public_read(prepared),
        result_columns: prepared
            .lowered_batch()
            .map(|lowered| public_read_result_columns_from_lowered(&lowered.result_columns)),
    }
}

pub(crate) fn committed_read_mode_from_prepared_public_read(
    public_read: &PublicReadPlan,
) -> CommittedReadMode {
    if public_read.effective_state_request().is_none()
        && public_read.effective_state_plan().is_none()
    {
        return CommittedReadMode::CommittedOnly;
    }

    CommittedReadMode::MaterializedState
}

fn pending_overlay_query_from_prepared_public_read(
    public_read: &PublicReadPlan,
) -> Option<PendingOverlayQuery> {
    let surface_read_plan = public_read.logical_plan.surface_read_plan()?;
    let structured_read = surface_read_plan.structured_read();
    if !matches!(
        structured_read.resolved_relation.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity
    ) {
        return None;
    }
    if matches!(
        structured_read.resolved_relation.descriptor.surface_variant,
        SurfaceVariant::History | SurfaceVariant::WorkingChanges
    ) {
        return None;
    }

    let compiled_query = compile_public_rowset_query(surface_read_plan)?;

    Some(PendingOverlayQuery {
        lane: PendingOverlayLane::Tracked,
        schema_key: structured_read
            .resolved_relation
            .implicit_overrides
            .fixed_schema_key
            .clone()
            .or_else(|| {
                let request = public_read.effective_state_request()?;
                (request.schema_set.len() == 1)
                    .then(|| request.schema_set.iter().next().cloned())
                    .flatten()
            })?,
        version_id: structured_read.requested_version_id.clone()?,
        projections: compiled_query.projections,
        filters: compiled_query.filters,
        order_by: compiled_query.order_by,
        limit: compiled_query.limit,
    })
}

fn public_read_result_columns_from_lowered(
    result_columns: &LoweredResultColumns,
) -> PublicReadResultColumns {
    match result_columns {
        LoweredResultColumns::Static(columns) => PublicReadResultColumns::Static(
            columns
                .iter()
                .copied()
                .map(public_read_result_column_from_lowered)
                .collect(),
        ),
        LoweredResultColumns::ByColumnName(columns_by_name) => {
            PublicReadResultColumns::ByColumnName(
                columns_by_name
                    .iter()
                    .map(|(name, kind)| {
                        (name.clone(), public_read_result_column_from_lowered(*kind))
                    })
                    .collect(),
            )
        }
    }
}

fn public_read_result_column_from_lowered(kind: LoweredResultColumn) -> PublicReadResultColumn {
    match kind {
        LoweredResultColumn::Untyped => PublicReadResultColumn::Untyped,
        LoweredResultColumn::Boolean => PublicReadResultColumn::Boolean,
    }
}

fn runtime_binding_values_from_statement_context(
    statement_context: &StatementContext,
) -> Result<RuntimeBindingValues, LixError> {
    Ok(RuntimeBindingValues {
        active_version_id: statement_context
            .requested_version_id
            .clone()
            .unwrap_or_default(),
        active_account_ids_json: serde_json::to_string(&statement_context.active_account_ids)
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("active account ids serialization failed: {error}"),
                )
            })?,
    })
}

fn render_lowered_read_batch_sql(
    lowered: &LoweredReadBatch,
    params: &[Value],
    statement_context: &StatementContext,
    dialect: SqlDialect,
) -> Result<Vec<String>, LixError> {
    let runtime_bindings = runtime_binding_values_from_statement_context(statement_context)?;
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

fn build_state_history_read_plan(
    structured_read: &StructuredPublicRead,
    history_read_semantics: CatalogHistoryReadSemantics,
) -> Result<Option<StateHistoryReadPlan>, LixError> {
    if !matches!(
        history_read_semantics,
        CatalogHistoryReadSemantics::StateHistoryActiveVersion
    ) {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        lineage_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
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
    let result_columns = state_history_result_columns(
        &structured_read.resolved_relation,
        &projections,
        wildcard_projection,
    );

    Ok(Some(StateHistoryReadPlan {
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

fn build_entity_history_read_plan(
    structured_read: &StructuredPublicRead,
    history_read_semantics: CatalogHistoryReadSemantics,
) -> Result<Option<EntityHistoryReadPlan>, LixError> {
    if !matches!(
        history_read_semantics,
        CatalogHistoryReadSemantics::EntityHistoryActiveVersion
    ) {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let schema_key = structured_read
        .resolved_relation
        .implicit_overrides
        .fixed_schema_key
        .clone()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "entity-history read execution requires a fixed schema key",
            )
        })?;

    let mut request = StateHistoryRequest {
        lineage_scope: StateHistoryLineageScope::ActiveVersion,
        lineage_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
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
    let result_columns = entity_history_result_columns(
        &structured_read.resolved_relation,
        &projections,
        wildcard_projection,
    );

    Ok(Some(EntityHistoryReadPlan {
        resolved_relation: structured_read.resolved_relation.clone(),
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
) -> Result<Vec<StateHistoryField>, LixError> {
    match &structured_read.query.group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            if !modifiers.is_empty() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "state-history read execution does not support GROUP BY modifiers",
                ));
            }
            expressions
                .iter()
                .map(|expr| {
                    state_history_field_from_expr(&structured_read.resolved_relation, expr)?
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "state-history read execution only supports grouping by state-history columns",
                            )
                        })
                })
                .collect()
        }
        GroupByExpr::All(_) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state-history read execution does not support GROUP BY ALL",
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
            "state-history read execution only supports HAVING predicates over COUNT(*)",
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
                    structured_read.resolved_relation.descriptor.public_name
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
            &structured_read.resolved_relation,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "entity-history read execution does not support this predicate shape",
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
            if matches!(field, EntityHistoryField::State(StateHistoryField::Depth)) {
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
            if matches!(field, EntityHistoryField::State(StateHistoryField::Depth)) {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth + 1);
                }
            }
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            if matches!(field, EntityHistoryField::State(StateHistoryField::Depth)) {
                if let Some(depth) = value_as_i64(value) {
                    update_min_depth(min_depth, depth);
                }
            }
        }
        EntityHistoryPredicate::Lt(field, value) => {
            if matches!(field, EntityHistoryField::State(StateHistoryField::Depth)) {
                if let Some(depth) = value_as_i64(value) {
                    update_max_depth(max_depth, depth - 1);
                }
            }
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            if matches!(field, EntityHistoryField::State(StateHistoryField::Depth)) {
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
    field: &EntityHistoryField,
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
        EntityHistoryField::State(StateHistoryField::RootCommitId) => {
            root_commit_ids.insert(text.to_string());
        }
        EntityHistoryField::State(StateHistoryField::VersionId) => {
            version_ids.insert(text.to_string());
        }
        EntityHistoryField::State(StateHistoryField::EntityId) => {
            entity_ids.insert(text.to_string());
        }
        EntityHistoryField::State(StateHistoryField::FileId) => {
            file_ids.insert(text.to_string());
        }
        EntityHistoryField::State(StateHistoryField::PluginKey) => {
            plugin_keys.insert(text.to_string());
        }
        EntityHistoryField::Property(_)
        | EntityHistoryField::State(StateHistoryField::SchemaKey)
        | EntityHistoryField::State(StateHistoryField::SnapshotContent)
        | EntityHistoryField::State(StateHistoryField::Metadata)
        | EntityHistoryField::State(StateHistoryField::SchemaVersion)
        | EntityHistoryField::State(StateHistoryField::ChangeId)
        | EntityHistoryField::State(StateHistoryField::CommitId)
        | EntityHistoryField::State(StateHistoryField::CommitCreatedAt)
        | EntityHistoryField::State(StateHistoryField::Depth) => {}
    }
}

fn build_entity_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<EntityHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, EntityHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.resolved_relation.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field =
            entity_history_field_from_select_item(&structured_read.resolved_relation, item)?;
        let output_name = entity_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(EntityHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_entity_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, EntityHistoryField>,
) -> Result<Vec<EntityHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "entity-history read execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "entity-history read execution does not support ORDER BY ... WITH FILL",
            ));
        }
        let output_name = direct_expr_output_name(&expr.expr);
        let field = entity_history_field_from_expr(&structured_read.resolved_relation, &expr.expr)?
            .or_else(|| {
                projection_aliases
                    .get(&output_name.to_ascii_lowercase())
                    .cloned()
            });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "entity-history read execution does not support this ORDER BY expression",
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

fn entity_history_result_columns(
    resolved_relation: &ResolvedRelation,
    projections: &[EntityHistoryProjection],
    wildcard_projection: bool,
) -> LoweredResultColumns {
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            resolved_relation
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
                    resolved_relation,
                    entity_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn build_file_history_read_plan(
    structured_read: &StructuredPublicRead,
    history_read_semantics: CatalogHistoryReadSemantics,
) -> Result<Option<FileHistoryReadPlan>, LixError> {
    let CatalogHistoryReadSemantics::FileHistory {
        active_version_lineage,
    } = history_read_semantics
    else {
        return Ok(None);
    };
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = FileHistoryRequest {
        lineage_scope: if active_version_lineage {
            FileHistoryLineageScope::ActiveVersion
        } else {
            FileHistoryLineageScope::Standard
        },
        lineage_version_id: active_version_lineage
            .then(|| required_requested_version_id(structured_read).map(str::to_string))
            .transpose()?,
        ..FileHistoryRequest::default()
    };
    let predicates = build_file_history_predicates_and_request(structured_read, &mut request)?;
    let aggregate = file_history_aggregate(structured_read)?;
    if aggregate.is_none() && file_history_query_needs_data(structured_read, &predicates)? {
        request.content_mode = FileHistoryContentMode::IncludeData;
    }
    let aggregate_output_name = aggregate
        .as_ref()
        .map(|_| file_history_aggregate_output_name(&structured_read.query.projection[0]));
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
    let result_columns = file_history_result_columns(
        &structured_read.resolved_relation,
        &projections,
        wildcard_projection,
        aggregate.as_ref(),
    );

    Ok(Some(FileHistoryReadPlan {
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

fn build_directory_history_read_plan(
    structured_read: &StructuredPublicRead,
    history_read_semantics: CatalogHistoryReadSemantics,
) -> Result<Option<DirectoryHistoryReadPlan>, LixError> {
    if !matches!(
        history_read_semantics,
        CatalogHistoryReadSemantics::DirectoryHistoryActiveVersion
    ) {
        return Ok(None);
    }
    if structured_read.query.uses_wildcard_projection()
        && structured_read.query.projection.len() != 1
    {
        return Ok(None);
    }

    let mut request = DirectoryHistoryRequest {
        lineage_scope: FileHistoryLineageScope::ActiveVersion,
        lineage_version_id: Some(required_requested_version_id(structured_read)?.to_string()),
        ..DirectoryHistoryRequest::default()
    };
    let predicates = build_directory_history_predicates_and_request(structured_read, &mut request)?;
    let aggregate = directory_history_aggregate(structured_read)?;
    let aggregate_output_name = aggregate
        .as_ref()
        .map(|_| directory_history_aggregate_output_name(&structured_read.query.projection[0]));
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
    let result_columns = directory_history_result_columns(
        &structured_read.resolved_relation,
        &projections,
        wildcard_projection,
        aggregate.as_ref(),
    );

    Ok(Some(DirectoryHistoryReadPlan {
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

fn directory_history_aggregate(
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

fn directory_history_aggregate_output_name(item: &SelectItem) -> String {
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
            &structured_read.resolved_relation,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "directory-history read execution does not support this predicate shape",
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
    field: &DirectoryHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
    directory_ids: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        DirectoryHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        DirectoryHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        DirectoryHistoryField::Id | DirectoryHistoryField::EntityId => {
            directory_ids.insert(text.to_string());
        }
        DirectoryHistoryField::ParentId
        | DirectoryHistoryField::Name
        | DirectoryHistoryField::Path
        | DirectoryHistoryField::Hidden
        | DirectoryHistoryField::SchemaKey
        | DirectoryHistoryField::FileId
        | DirectoryHistoryField::PluginKey
        | DirectoryHistoryField::SchemaVersion
        | DirectoryHistoryField::ChangeId
        | DirectoryHistoryField::LixcolMetadata
        | DirectoryHistoryField::CommitId
        | DirectoryHistoryField::CommitCreatedAt
        | DirectoryHistoryField::Depth => {}
    }
}

fn build_directory_history_projection_plan(
    structured_read: &StructuredPublicRead,
) -> Result<
    (
        Vec<DirectoryHistoryProjection>,
        bool,
        Vec<String>,
        BTreeMap<String, DirectoryHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.resolved_relation.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field =
            directory_history_field_from_select_item(&structured_read.resolved_relation, item)?;
        let output_name = directory_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(DirectoryHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_directory_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, DirectoryHistoryField>,
) -> Result<Vec<DirectoryHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "directory-history read execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "directory-history read execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let field =
            directory_history_field_from_expr(&structured_read.resolved_relation, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "directory-history read execution does not support this ORDER BY expression",
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

fn directory_history_result_columns(
    resolved_relation: &ResolvedRelation,
    projections: &[DirectoryHistoryProjection],
    wildcard_projection: bool,
    aggregate: Option<&DirectoryHistoryAggregate>,
) -> LoweredResultColumns {
    if aggregate.is_some() {
        return LoweredResultColumns::Static(vec![LoweredResultColumn::Untyped]);
    }
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            resolved_relation
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
                    resolved_relation,
                    directory_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn directory_history_field_from_select_item(
    resolved_relation: &ResolvedRelation,
    item: &SelectItem,
) -> Result<DirectoryHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before directory-history read field extraction",
            ));
        }
    };
    directory_history_field_from_expr(resolved_relation, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "directory-history read execution does not support this projection expression",
        )
    })
}

fn directory_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn directory_history_field_from_expr(
    resolved_relation: &ResolvedRelation,
    expr: &Expr,
) -> Result<Option<DirectoryHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            directory_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            directory_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::Nested(inner) => directory_history_field_from_expr(resolved_relation, inner),
        _ => Ok(None),
    }
}

fn directory_history_field_from_column_name(
    resolved_relation: &ResolvedRelation,
    column: &str,
) -> Result<DirectoryHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Ok(DirectoryHistoryField::Id),
        "parent_id" => Ok(DirectoryHistoryField::ParentId),
        "name" => Ok(DirectoryHistoryField::Name),
        "path" => Ok(DirectoryHistoryField::Path),
        "hidden" => Ok(DirectoryHistoryField::Hidden),
        "entity_id" | "lixcol_entity_id" => Ok(DirectoryHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(DirectoryHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(DirectoryHistoryField::FileId),
        "version_id" | "lixcol_version_id" => Ok(DirectoryHistoryField::VersionId),
        "plugin_key" | "lixcol_plugin_key" => Ok(DirectoryHistoryField::PluginKey),
        "schema_version" | "lixcol_schema_version" => Ok(DirectoryHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(DirectoryHistoryField::ChangeId),
        "lixcol_metadata" => Ok(DirectoryHistoryField::LixcolMetadata),
        "commit_id" | "lixcol_commit_id" => Ok(DirectoryHistoryField::CommitId),
        "commit_created_at" | "lixcol_commit_created_at" => {
            Ok(DirectoryHistoryField::CommitCreatedAt)
        }
        "root_commit_id" | "lixcol_root_commit_id" => Ok(DirectoryHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(DirectoryHistoryField::Depth),
        _ => Err(crate::sql::diagnostics::sql_unknown_column_error(
            column,
            Some(&resolved_relation.descriptor.public_name),
            &resolved_relation
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
    resolved_relation: &ResolvedRelation,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<DirectoryHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_directory_history_predicate(inner, resolved_relation, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = directory_history_field_from_expr(resolved_relation, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(directory_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = directory_history_field_from_expr(resolved_relation, right)? {
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
            let Some(field) = directory_history_field_from_expr(resolved_relation, expr)? else {
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
        Expr::IsNull(expr) => directory_history_field_from_expr(resolved_relation, expr)?
            .map(DirectoryHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "directory-history read execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => directory_history_field_from_expr(resolved_relation, expr)?
            .map(DirectoryHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "directory-history read execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn directory_history_predicate_from_operator(
    field: DirectoryHistoryField,
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
    field: DirectoryHistoryField,
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

fn file_history_aggregate(
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

fn file_history_aggregate_output_name(item: &SelectItem) -> String {
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
            &structured_read.resolved_relation,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "file-history read execution does not support this predicate shape",
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
    field: &FileHistoryField,
    value: &Value,
    root_commit_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) {
    let Some(text) = value_as_text(value) else {
        return;
    };
    match field {
        FileHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        FileHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        FileHistoryField::Id
        | FileHistoryField::EntityId
        | FileHistoryField::FileId
        | FileHistoryField::Path
        | FileHistoryField::Data
        | FileHistoryField::Metadata
        | FileHistoryField::Hidden
        | FileHistoryField::SchemaKey
        | FileHistoryField::PluginKey
        | FileHistoryField::SchemaVersion
        | FileHistoryField::ChangeId
        | FileHistoryField::LixcolMetadata
        | FileHistoryField::CommitId
        | FileHistoryField::CommitCreatedAt
        | FileHistoryField::Depth => {}
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
            file_history_field_from_select_item(&structured_read.resolved_relation, projection)?,
            FileHistoryField::Data
        ) {
            return Ok(true);
        }
    }
    for predicate in predicates {
        if file_history_predicate_field(predicate) == FileHistoryField::Data {
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
                file_history_field_from_expr(&structured_read.resolved_relation, &sort.expr)?
            else {
                continue;
            };
            if field == FileHistoryField::Data {
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
        BTreeMap<String, FileHistoryField>,
    ),
    LixError,
> {
    if structured_read.query.uses_wildcard_projection() {
        return Ok((
            Vec::new(),
            true,
            structured_read.resolved_relation.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let field = file_history_field_from_select_item(&structured_read.resolved_relation, item)?;
        let output_name = file_history_output_name(item);
        aliases.insert(output_name.to_ascii_lowercase(), field.clone());
        projections.push(FileHistoryProjection { output_name, field });
    }
    Ok((projections, false, Vec::new(), aliases))
}

fn build_file_history_sort_keys(
    structured_read: &StructuredPublicRead,
    projection_aliases: &BTreeMap<String, FileHistoryField>,
) -> Result<Vec<FileHistorySortKey>, LixError> {
    let Some(order_by) = &structured_read.query.order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "file-history read execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "file-history read execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let field = file_history_field_from_expr(&structured_read.resolved_relation, &expr.expr)?
            .or_else(|| {
                projection_aliases
                    .get(&output_name.to_ascii_lowercase())
                    .cloned()
            });
        let Some(field) = field else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "file-history read execution does not support this ORDER BY expression",
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

fn file_history_result_columns(
    resolved_relation: &ResolvedRelation,
    projections: &[FileHistoryProjection],
    wildcard_projection: bool,
    aggregate: Option<&FileHistoryAggregate>,
) -> LoweredResultColumns {
    if aggregate.is_some() {
        return LoweredResultColumns::Static(vec![LoweredResultColumn::Untyped]);
    }
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            resolved_relation
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
                    resolved_relation,
                    file_history_field_name(&projection.field),
                )
                .map(direct_lowered_result_column_from_surface_type)
                .unwrap_or(LoweredResultColumn::Untyped)
            })
            .collect(),
    )
}

fn file_history_field_from_select_item(
    resolved_relation: &ResolvedRelation,
    item: &SelectItem,
) -> Result<FileHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before file-history read field extraction",
            ));
        }
    };
    file_history_field_from_expr(resolved_relation, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "file-history read execution does not support this projection expression",
        )
    })
}

fn file_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn file_history_field_from_expr(
    resolved_relation: &ResolvedRelation,
    expr: &Expr,
) -> Result<Option<FileHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            file_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            file_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::Nested(inner) => file_history_field_from_expr(resolved_relation, inner),
        _ => Ok(None),
    }
}

fn file_history_field_from_column_name(
    resolved_relation: &ResolvedRelation,
    column: &str,
) -> Result<FileHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "id" => Ok(FileHistoryField::Id),
        "path" => Ok(FileHistoryField::Path),
        "data" => Ok(FileHistoryField::Data),
        "metadata" => Ok(FileHistoryField::Metadata),
        "hidden" => Ok(FileHistoryField::Hidden),
        "entity_id" | "lixcol_entity_id" => Ok(FileHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(FileHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(FileHistoryField::FileId),
        "version_id" | "lixcol_version_id" => Ok(FileHistoryField::VersionId),
        "plugin_key" | "lixcol_plugin_key" => Ok(FileHistoryField::PluginKey),
        "schema_version" | "lixcol_schema_version" => Ok(FileHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(FileHistoryField::ChangeId),
        "lixcol_metadata" => Ok(FileHistoryField::LixcolMetadata),
        "commit_id" | "lixcol_commit_id" => Ok(FileHistoryField::CommitId),
        "commit_created_at" | "lixcol_commit_created_at" => Ok(FileHistoryField::CommitCreatedAt),
        "root_commit_id" | "lixcol_root_commit_id" => Ok(FileHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(FileHistoryField::Depth),
        _ => Err(crate::sql::diagnostics::sql_unknown_column_error(
            column,
            Some(&resolved_relation.descriptor.public_name),
            &resolved_relation
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
    resolved_relation: &ResolvedRelation,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<FileHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_file_history_predicate(inner, resolved_relation, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = file_history_field_from_expr(resolved_relation, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(file_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = file_history_field_from_expr(resolved_relation, right)? {
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
            let Some(field) = file_history_field_from_expr(resolved_relation, expr)? else {
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
        Expr::IsNull(expr) => file_history_field_from_expr(resolved_relation, expr)?
            .map(FileHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "file-history read execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => file_history_field_from_expr(resolved_relation, expr)?
            .map(FileHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "file-history read execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn file_history_predicate_from_operator(
    field: FileHistoryField,
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
    field: FileHistoryField,
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
            &structured_read.resolved_relation,
            &structured_read.bound_parameters,
            &mut placeholder_state,
        )?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state-history read execution does not support this predicate shape",
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
            if *field == StateHistoryField::Depth {
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
        StateHistoryPredicate::Gt(field, value) if *field == StateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth.saturating_add(1));
            }
        }
        StateHistoryPredicate::GtEq(field, value) if *field == StateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_min_depth(min_depth, depth);
            }
        }
        StateHistoryPredicate::Lt(field, value) if *field == StateHistoryField::Depth => {
            if let Some(depth) = value_as_i64(value) {
                update_max_depth(max_depth, depth.saturating_sub(1));
            }
        }
        StateHistoryPredicate::LtEq(field, value) if *field == StateHistoryField::Depth => {
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
    field: &StateHistoryField,
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
        StateHistoryField::RootCommitId => {
            root_commit_ids.insert(text.to_string());
        }
        StateHistoryField::VersionId => {
            version_ids.insert(text.to_string());
        }
        StateHistoryField::EntityId => {
            entity_ids.insert(text.to_string());
        }
        StateHistoryField::FileId => {
            file_ids.insert(text.to_string());
        }
        StateHistoryField::SchemaKey => {
            schema_keys.insert(text.to_string());
        }
        StateHistoryField::PluginKey => {
            plugin_keys.insert(text.to_string());
        }
        StateHistoryField::SnapshotContent
        | StateHistoryField::Metadata
        | StateHistoryField::SchemaVersion
        | StateHistoryField::ChangeId
        | StateHistoryField::CommitId
        | StateHistoryField::CommitCreatedAt
        | StateHistoryField::Depth => {}
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
        let value =
            state_history_projection_value(&structured_read.resolved_relation, projection, &[])?;
        if let StateHistoryProjectionValue::Field(StateHistoryField::SnapshotContent) = value {
            return Ok(true);
        }
    }
    for predicate in predicates {
        if state_history_predicate_field(predicate) == StateHistoryField::SnapshotContent {
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
                state_history_field_from_expr(&structured_read.resolved_relation, &sort.expr)?
            else {
                continue;
            };
            if field == StateHistoryField::SnapshotContent {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn build_state_history_projection_plan(
    structured_read: &StructuredPublicRead,
    group_by_fields: &[StateHistoryField],
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
                "state-history read execution does not support wildcard projection on grouped reads",
            ));
        }
        return Ok((
            Vec::new(),
            true,
            structured_read.resolved_relation.exposed_columns.clone(),
            BTreeMap::new(),
        ));
    }

    let mut projections = Vec::new();
    let mut aliases = BTreeMap::new();
    for item in &structured_read.query.projection {
        let value = state_history_projection_value(
            &structured_read.resolved_relation,
            item,
            group_by_fields,
        )?;
        let output_name = state_history_output_name(item);
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
            "state-history read execution does not support ORDER BY ALL",
        ));
    };

    let mut sort_keys = Vec::new();
    for expr in expressions {
        if expr.with_fill.is_some() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state-history read execution does not support ORDER BY ... WITH FILL",
            ));
        }

        let output_name = direct_expr_output_name(&expr.expr);
        let value =
            state_history_sort_value_from_expr(&structured_read.resolved_relation, &expr.expr)?
                .or_else(|| {
                    projection_aliases
                        .get(&output_name.to_ascii_lowercase())
                        .cloned()
                        .map(state_history_sort_value_from_projection_value)
                });
        let Some(value) = value else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "state-history read execution does not support this ORDER BY expression",
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

fn state_history_result_columns(
    resolved_relation: &ResolvedRelation,
    projections: &[StateHistoryProjection],
    wildcard_projection: bool,
) -> LoweredResultColumns {
    if wildcard_projection {
        return LoweredResultColumns::ByColumnName(
            resolved_relation
                .column_types
                .iter()
                .map(
                    |(name, column_type): (&String, &crate::catalog::SurfaceColumnType)| {
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
                StateHistoryProjectionValue::Field(field) => {
                    direct_surface_column_type(resolved_relation, state_history_field_name(field))
                        .map(direct_lowered_result_column_from_surface_type)
                        .unwrap_or(LoweredResultColumn::Untyped)
                }
                StateHistoryProjectionValue::Aggregate(StateHistoryAggregate::Count) => {
                    LoweredResultColumn::Untyped
                }
            })
            .collect(),
    )
}

fn direct_lowered_result_column_from_surface_type(
    column_type: crate::catalog::SurfaceColumnType,
) -> LoweredResultColumn {
    match column_type {
        crate::catalog::SurfaceColumnType::Boolean => LoweredResultColumn::Boolean,
        crate::catalog::SurfaceColumnType::String
        | crate::catalog::SurfaceColumnType::Integer
        | crate::catalog::SurfaceColumnType::Number
        | crate::catalog::SurfaceColumnType::Json => LoweredResultColumn::Untyped,
    }
}

fn direct_surface_column_type(
    resolved_relation: &ResolvedRelation,
    column: &str,
) -> Option<crate::catalog::SurfaceColumnType> {
    resolved_relation.column_types.iter().find_map(
        |(candidate, kind): (&String, &crate::catalog::SurfaceColumnType)| {
            candidate.eq_ignore_ascii_case(column).then_some(*kind)
        },
    )
}

fn entity_history_field_from_select_item(
    resolved_relation: &ResolvedRelation,
    item: &SelectItem,
) -> Result<EntityHistoryField, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before entity-history read field extraction",
            ));
        }
    };
    entity_history_field_from_expr(resolved_relation, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "entity-history read execution does not support this projection expression",
        )
    })
}

fn state_history_projection_value(
    resolved_relation: &ResolvedRelation,
    item: &SelectItem,
    group_by_fields: &[StateHistoryField],
) -> Result<StateHistoryProjectionValue, LixError> {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "wildcard projection should be handled before state-history read projection parsing",
            ));
        }
    };
    if let Some(aggregate) = state_history_aggregate_from_expr(expr)? {
        return Ok(StateHistoryProjectionValue::Aggregate(aggregate));
    }
    let field = state_history_field_from_expr(resolved_relation, expr)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state-history read execution does not support this projection expression",
        )
    })?;
    if !group_by_fields.is_empty() && !group_by_fields.contains(&field) {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "state-history read execution only supports grouped projections over GROUP BY columns and COUNT(*)",
        ));
    }
    Ok(StateHistoryProjectionValue::Field(field))
}

fn state_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn entity_history_output_name(item: &SelectItem) -> String {
    match item {
        SelectItem::UnnamedExpr(expr) => direct_expr_output_name(expr),
        SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => "*".to_string(),
    }
}

fn state_history_aggregate_from_expr(
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

fn state_history_field_from_expr(
    resolved_relation: &ResolvedRelation,
    expr: &Expr,
) -> Result<Option<StateHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            state_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            state_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::Nested(inner) => state_history_field_from_expr(resolved_relation, inner),
        _ => Ok(None),
    }
}

fn entity_history_field_from_expr(
    resolved_relation: &ResolvedRelation,
    expr: &Expr,
) -> Result<Option<EntityHistoryField>, LixError> {
    match expr {
        Expr::Identifier(ident) => {
            entity_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(ident) = parts.last() else {
                return Ok(None);
            };
            entity_history_field_from_column_name(resolved_relation, &ident.value).map(Some)
        }
        Expr::Nested(inner) => entity_history_field_from_expr(resolved_relation, inner),
        _ => Ok(None),
    }
}

fn state_history_sort_value_from_expr(
    resolved_relation: &ResolvedRelation,
    expr: &Expr,
) -> Result<Option<StateHistorySortValue>, LixError> {
    if let Some(field) = state_history_field_from_expr(resolved_relation, expr)? {
        return Ok(Some(StateHistorySortValue::Field(field)));
    }
    Ok(state_history_aggregate_from_expr(expr)?.map(StateHistorySortValue::Aggregate))
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

fn state_history_field_from_column_name(
    resolved_relation: &ResolvedRelation,
    column: &str,
) -> Result<StateHistoryField, LixError> {
    match column.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Ok(StateHistoryField::EntityId),
        "schema_key" | "lixcol_schema_key" => Ok(StateHistoryField::SchemaKey),
        "file_id" | "lixcol_file_id" => Ok(StateHistoryField::FileId),
        "plugin_key" | "lixcol_plugin_key" => Ok(StateHistoryField::PluginKey),
        "snapshot_content" => Ok(StateHistoryField::SnapshotContent),
        "metadata" | "lixcol_metadata" => Ok(StateHistoryField::Metadata),
        "schema_version" | "lixcol_schema_version" => Ok(StateHistoryField::SchemaVersion),
        "change_id" | "lixcol_change_id" => Ok(StateHistoryField::ChangeId),
        "commit_id" | "lixcol_commit_id" => Ok(StateHistoryField::CommitId),
        "commit_created_at" => Ok(StateHistoryField::CommitCreatedAt),
        "root_commit_id" | "lixcol_root_commit_id" => Ok(StateHistoryField::RootCommitId),
        "depth" | "lixcol_depth" => Ok(StateHistoryField::Depth),
        "version_id" | "lixcol_version_id" => Ok(StateHistoryField::VersionId),
        _ => Err(crate::sql::diagnostics::sql_unknown_column_error(
            column,
            Some(&resolved_relation.descriptor.public_name),
            &resolved_relation
                .exposed_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            None,
        )),
    }
}

fn entity_history_field_from_column_name(
    resolved_relation: &ResolvedRelation,
    column: &str,
) -> Result<EntityHistoryField, LixError> {
    let lowercase = column.to_ascii_lowercase();
    if let Ok(field) = state_history_field_from_column_name(resolved_relation, column) {
        return Ok(EntityHistoryField::State(field));
    }
    if resolved_relation
        .descriptor
        .visible_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        return Ok(EntityHistoryField::Property(lowercase));
    }
    Err(crate::sql::diagnostics::sql_unknown_column_error(
        column,
        Some(&resolved_relation.descriptor.public_name),
        &resolved_relation
            .descriptor
            .visible_columns
            .iter()
            .chain(resolved_relation.descriptor.hidden_columns.iter())
            .map(String::as_str)
            .collect::<Vec<_>>(),
        None,
    ))
}

fn parse_state_history_predicate(
    expr: &Expr,
    resolved_relation: &ResolvedRelation,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<StateHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_state_history_predicate(inner, resolved_relation, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = state_history_field_from_expr(resolved_relation, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(state_history_predicate_from_operator(field, op, value));
                }
                if let Expr::InList { .. } = right.as_ref() {
                    return Ok(None);
                }
            }
            if let Some(field) = state_history_field_from_expr(resolved_relation, right)? {
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
            let Some(field) = state_history_field_from_expr(resolved_relation, expr)? else {
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
        Expr::IsNull(expr) => state_history_field_from_expr(resolved_relation, expr)?
            .map(StateHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "state-history read execution does not support this predicate shape",
                )
            }),
        Expr::IsNotNull(expr) => state_history_field_from_expr(resolved_relation, expr)?
            .map(StateHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "state-history read execution does not support this predicate shape",
                )
            }),
        _ => Ok(None),
    }
}

fn parse_entity_history_predicate(
    expr: &Expr,
    resolved_relation: &ResolvedRelation,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<Option<EntityHistoryPredicate>, LixError> {
    match expr {
        Expr::Nested(inner) => {
            parse_entity_history_predicate(inner, resolved_relation, params, placeholder_state)
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(field) = entity_history_field_from_expr(resolved_relation, left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(entity_history_predicate_from_operator(field, op, value));
                }
            }
            if let Some(field) = entity_history_field_from_expr(resolved_relation, right)? {
                if let Some(value) = direct_value_from_expr(left, params, placeholder_state)? {
                    return Ok(entity_history_predicate_from_reversed_operator(
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
            let Some(field) = entity_history_field_from_expr(resolved_relation, expr)? else {
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
        Expr::IsNull(expr) => entity_history_field_from_expr(resolved_relation, expr)?
            .map(EntityHistoryPredicate::IsNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "entity-history read execution does not support IS NULL on this expression",
                )
            }),
        Expr::IsNotNull(expr) => entity_history_field_from_expr(resolved_relation, expr)?
            .map(EntityHistoryPredicate::IsNotNull)
            .map(Some)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "entity-history read execution does not support IS NOT NULL on this expression",
                )
            }),
        _ => Ok(None),
    }
}

fn entity_history_predicate_from_operator(
    field: EntityHistoryField,
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
    field: EntityHistoryField,
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
            if let Some(aggregate) = state_history_aggregate_from_expr(left)? {
                if let Some(value) = direct_value_from_expr(right, params, placeholder_state)? {
                    return Ok(state_history_aggregate_predicate_from_operator(
                        aggregate, op, value,
                    ));
                }
            }
            if let Some(aggregate) = state_history_aggregate_from_expr(right)? {
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
    field: StateHistoryField,
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
    field: StateHistoryField,
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
            "unexpected placeholder literal during state-history read preparation",
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
                    "state-history read execution does not support LIMIT BY",
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
            "state-history read execution requires literal LIMIT/OFFSET values",
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
            "state-history read execution requires integer LIMIT/OFFSET values",
        )),
    }
}

fn file_history_field_name(field: &FileHistoryField) -> &'static str {
    match field {
        FileHistoryField::Id => "id",
        FileHistoryField::Path => "path",
        FileHistoryField::Data => "data",
        FileHistoryField::Metadata => "metadata",
        FileHistoryField::Hidden => "hidden",
        FileHistoryField::EntityId => "lixcol_entity_id",
        FileHistoryField::SchemaKey => "lixcol_schema_key",
        FileHistoryField::FileId => "lixcol_file_id",
        FileHistoryField::VersionId => "lixcol_version_id",
        FileHistoryField::PluginKey => "lixcol_plugin_key",
        FileHistoryField::SchemaVersion => "lixcol_schema_version",
        FileHistoryField::ChangeId => "lixcol_change_id",
        FileHistoryField::LixcolMetadata => "lixcol_metadata",
        FileHistoryField::CommitId => "lixcol_commit_id",
        FileHistoryField::CommitCreatedAt => "lixcol_commit_created_at",
        FileHistoryField::RootCommitId => "lixcol_root_commit_id",
        FileHistoryField::Depth => "lixcol_depth",
    }
}

fn directory_history_field_name(field: &DirectoryHistoryField) -> &'static str {
    match field {
        DirectoryHistoryField::Id => "id",
        DirectoryHistoryField::ParentId => "parent_id",
        DirectoryHistoryField::Name => "name",
        DirectoryHistoryField::Path => "path",
        DirectoryHistoryField::Hidden => "hidden",
        DirectoryHistoryField::EntityId => "lixcol_entity_id",
        DirectoryHistoryField::SchemaKey => "lixcol_schema_key",
        DirectoryHistoryField::FileId => "lixcol_file_id",
        DirectoryHistoryField::VersionId => "lixcol_version_id",
        DirectoryHistoryField::PluginKey => "lixcol_plugin_key",
        DirectoryHistoryField::SchemaVersion => "lixcol_schema_version",
        DirectoryHistoryField::ChangeId => "lixcol_change_id",
        DirectoryHistoryField::LixcolMetadata => "lixcol_metadata",
        DirectoryHistoryField::CommitId => "lixcol_commit_id",
        DirectoryHistoryField::CommitCreatedAt => "lixcol_commit_created_at",
        DirectoryHistoryField::RootCommitId => "lixcol_root_commit_id",
        DirectoryHistoryField::Depth => "lixcol_depth",
    }
}

fn file_history_predicate_field(predicate: &FileHistoryPredicate) -> FileHistoryField {
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

fn state_history_predicate_field(predicate: &StateHistoryPredicate) -> StateHistoryField {
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

fn state_history_field_name(field: &StateHistoryField) -> &'static str {
    match field {
        StateHistoryField::EntityId => "entity_id",
        StateHistoryField::SchemaKey => "schema_key",
        StateHistoryField::FileId => "file_id",
        StateHistoryField::PluginKey => "plugin_key",
        StateHistoryField::SnapshotContent => "snapshot_content",
        StateHistoryField::Metadata => "metadata",
        StateHistoryField::SchemaVersion => "schema_version",
        StateHistoryField::ChangeId => "change_id",
        StateHistoryField::CommitId => "commit_id",
        StateHistoryField::CommitCreatedAt => "commit_created_at",
        StateHistoryField::RootCommitId => "root_commit_id",
        StateHistoryField::Depth => "depth",
        StateHistoryField::VersionId => "version_id",
    }
}

fn entity_history_field_name(field: &EntityHistoryField) -> &str {
    match field {
        EntityHistoryField::Property(property) => property.as_str(),
        EntityHistoryField::State(field) => state_history_field_name(field),
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
        EntityHistoryPredicate::Eq(field, value) => {
            binary_predicate_expr(entity_history_field_name(field), BinaryOperator::Eq, value)
        }
        EntityHistoryPredicate::NotEq(field, value) => binary_predicate_expr(
            entity_history_field_name(field),
            BinaryOperator::NotEq,
            value,
        ),
        EntityHistoryPredicate::Gt(field, value) => {
            binary_predicate_expr(entity_history_field_name(field), BinaryOperator::Gt, value)
        }
        EntityHistoryPredicate::GtEq(field, value) => binary_predicate_expr(
            entity_history_field_name(field),
            BinaryOperator::GtEq,
            value,
        ),
        EntityHistoryPredicate::Lt(field, value) => {
            binary_predicate_expr(entity_history_field_name(field), BinaryOperator::Lt, value)
        }
        EntityHistoryPredicate::LtEq(field, value) => binary_predicate_expr(
            entity_history_field_name(field),
            BinaryOperator::LtEq,
            value,
        ),
        EntityHistoryPredicate::In(field, values) => {
            in_list_predicate_expr(entity_history_field_name(field), values)
        }
        EntityHistoryPredicate::IsNull(field) => {
            Expr::IsNull(Box::new(identifier_expr(entity_history_field_name(field))))
        }
        EntityHistoryPredicate::IsNotNull(field) => {
            Expr::IsNotNull(Box::new(identifier_expr(entity_history_field_name(field))))
        }
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
    Prepared(PublicReadPlan),
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

fn public_read_preparation_error(bindings: &[ResolvedRelation], message: &str) -> Option<LixError> {
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
    Some(crate::sql::diagnostics::sql_unknown_column_error(
        &missing_column,
        Some(&binding.descriptor.public_name),
        available_columns.as_slice(),
        None,
    ))
}

async fn try_prepare_public_read_via_specialized_optimization(
    dialect: SqlDialect,
    compiler_metadata: &super::super::SqlCompilerMetadata,
    bound_statement: BoundStatement,
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
    explain_request: Option<&crate::sql::explain::ExplainRequest>,
    registry: &SurfaceRegistry,
    public_output_columns: Option<Vec<String>>,
    mut stage_timings: ExplainTimingCollector,
) -> Result<SpecializedPublicReadPreparation, LixError> {
    // Specialized public-read stage semantics:
    // - semantic_analysis: canonicalize the bound statement into a structured public read and
    //   derive dependency/effective-state semantics.
    // - logical_planning: construct the structured logical plan before any public-read plan-kind
    //   classification happens.
    // - routing: classify the public read as history-read, read-time-projection, or
    //   prepared-batch.
    // - capability_resolution: load external schemas/layouts needed to lower specialized SQL
    //   execution once the chosen plan kind requires backend capability state.
    // - physical_planning: build the history plan or the lowered read batch after any
    //   required capability resolution has completed.
    // - artifact_preparation: render lowered backend SQL from a lowered read batch. Direct
    //   history plans omit this stage because they do not prepare backend SQL text.
    let runtime_bindings =
        runtime_binding_values_from_statement_context(&bound_statement.statement_context)?;
    let bound_parameters = bound_statement.bound_parameters.clone();
    let semantic_started = Instant::now();
    let analysis = match prepare_structured_public_read_analysis(
        bound_statement,
        registry,
        active_history_root_commit_id,
    )
    .await?
    {
        StructuredPublicReadDecision::Prepared(analysis) => analysis,
        StructuredPublicReadDecision::Declined(bound_statement) => {
            return Ok(SpecializedPublicReadPreparation::Declined {
                reason: "specialized read optimization declined canonicalization".to_string(),
                bound_statement,
            });
        }
    };
    stage_timings.record(ExplainStage::SemanticAnalysis, semantic_started.elapsed());
    let surface_read_plan = analysis.surface_read_plan().clone();
    let structured_read = surface_read_plan.structured_read().clone();
    let resolved_relation = structured_read.resolved_relation.clone();
    let freshness_contract = resolved_relation.read_freshness;
    let logical_started = Instant::now();
    let logical_plan = analysis.logical_plan();
    stage_timings.record(ExplainStage::LogicalPlanning, logical_started.elapsed());
    let routing_started = Instant::now();
    let plan_choice = classify_public_read_plan_kind(&surface_read_plan);
    stage_timings.record(ExplainStage::Routing, routing_started.elapsed());
    let routing_passes = plan_choice.pass_traces;
    let empty_current_version_heads = BTreeMap::new();
    let current_version_heads = if builtin_catalog_compiler_facade()
        .read_preparation_semantics(&resolved_relation)
        .requires_current_version_heads
    {
        compiler_metadata
            .current_version_heads
            .as_ref()
            .unwrap_or(&empty_current_version_heads)
    } else {
        &empty_current_version_heads
    };

    let physical_started = Instant::now();
    let (execution, pushdown_decision) = match plan_choice.kind {
        PublicReadPlanKind::HistoryRead => {
            match builtin_catalog_compiler_facade()
                .history_read_semantics(&structured_read.resolved_relation)
            {
                Some(CatalogHistoryReadSemantics::StateHistoryActiveVersion) => {
                    match build_state_history_read_plan(
                        &structured_read,
                        CatalogHistoryReadSemantics::StateHistoryActiveVersion,
                    ) {
                        Ok(Some(plan)) => {
                            let pushdown_decision = state_history_pushdown_decision(&plan);
                            (
                                PublicReadPhysicalPlan::HistoryRead(HistoryReadPlan::StateHistory(
                                    plan,
                                )),
                                pushdown_decision,
                            )
                        }
                        Ok(None) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: format!(
                                    "specialized read optimization declined '{}'",
                                    structured_read.resolved_relation.descriptor.public_name
                                ),
                                bound_statement: analysis.bound_statement,
                            });
                        }
                        Err(error) if specialized_public_read_error_is_semantic(&error) => {
                            return Err(error);
                        }
                        Err(error) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: error.description,
                                bound_statement: analysis.bound_statement,
                            });
                        }
                    }
                }
                Some(CatalogHistoryReadSemantics::EntityHistoryActiveVersion) => {
                    match build_entity_history_read_plan(
                        &structured_read,
                        CatalogHistoryReadSemantics::EntityHistoryActiveVersion,
                    ) {
                        Ok(Some(plan)) => {
                            let pushdown_decision = entity_history_pushdown_decision(&plan);
                            (
                                PublicReadPhysicalPlan::HistoryRead(
                                    HistoryReadPlan::EntityHistory(plan),
                                ),
                                pushdown_decision,
                            )
                        }
                        Ok(None) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: format!(
                                    "specialized read optimization declined '{}'",
                                    structured_read.resolved_relation.descriptor.public_name
                                ),
                                bound_statement: analysis.bound_statement,
                            });
                        }
                        Err(error) if specialized_public_read_error_is_semantic(&error) => {
                            return Err(error);
                        }
                        Err(error) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: error.description,
                                bound_statement: analysis.bound_statement,
                            });
                        }
                    }
                }
                Some(semantics @ CatalogHistoryReadSemantics::DirectoryHistoryActiveVersion) => {
                    match build_directory_history_read_plan(&structured_read, semantics) {
                        Ok(Some(plan)) => {
                            let pushdown_decision = directory_history_pushdown_decision(&plan);
                            (
                                PublicReadPhysicalPlan::HistoryRead(
                                    HistoryReadPlan::DirectoryHistory(plan),
                                ),
                                pushdown_decision,
                            )
                        }
                        Ok(None) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: format!(
                                    "specialized read optimization declined '{}'",
                                    structured_read.resolved_relation.descriptor.public_name
                                ),
                                bound_statement: analysis.bound_statement,
                            });
                        }
                        Err(error) if specialized_public_read_error_is_semantic(&error) => {
                            return Err(error);
                        }
                        Err(error) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: error.description,
                                bound_statement: analysis.bound_statement,
                            });
                        }
                    }
                }
                Some(semantics @ CatalogHistoryReadSemantics::FileHistory { .. }) => {
                    match build_file_history_read_plan(&structured_read, semantics) {
                        Ok(Some(plan)) => {
                            let pushdown_decision = file_history_pushdown_decision(&plan);
                            (
                                PublicReadPhysicalPlan::HistoryRead(HistoryReadPlan::FileHistory(
                                    plan,
                                )),
                                pushdown_decision,
                            )
                        }
                        Ok(None) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: format!(
                                    "specialized read optimization declined '{}'",
                                    structured_read.resolved_relation.descriptor.public_name
                                ),
                                bound_statement: analysis.bound_statement,
                            });
                        }
                        Err(error) if specialized_public_read_error_is_semantic(&error) => {
                            return Err(error);
                        }
                        Err(error) => {
                            return Ok(SpecializedPublicReadPreparation::Declined {
                                reason: error.description,
                                bound_statement: analysis.bound_statement,
                            });
                        }
                    }
                }
                _ => {
                    return Ok(SpecializedPublicReadPreparation::Declined {
                        reason: format!(
                            "specialized read optimization declined '{}'",
                            structured_read.resolved_relation.descriptor.public_name
                        ),
                        bound_statement: analysis.bound_statement,
                    });
                }
            }
        }
        PublicReadPlanKind::ReadTimeProjection(projection_read) => {
            let CompilerOwnedPublicReadExecutionSelection {
                execution,
                pushdown_decision,
            } = compile_read_time_projection_execution(&surface_read_plan, projection_read);
            (execution, Some(pushdown_decision))
        }
        PublicReadPlanKind::PreparedBatch => {
            match compile_prepared_batch_public_read_execution(
                dialect,
                &surface_read_plan,
                &compiler_metadata.known_live_schema_definitions,
                current_version_heads,
                &mut stage_timings,
            ) {
                Ok(Some(CompilerOwnedPublicReadExecutionSelection {
                    execution,
                    pushdown_decision,
                })) => (execution, Some(pushdown_decision)),
                Ok(None) => {
                    return Ok(SpecializedPublicReadPreparation::Declined {
                        reason: format!(
                            "specialized read optimization declined '{}'",
                            structured_read.resolved_relation.descriptor.public_name
                        ),
                        bound_statement: analysis.bound_statement,
                    });
                }
                Err(error) if specialized_public_read_error_is_semantic(&error) => {
                    return Err(error);
                }
                Err(error) => {
                    return Ok(SpecializedPublicReadPreparation::Declined {
                        reason: error.description,
                        bound_statement: analysis.bound_statement,
                    });
                }
            }
        }
    };
    stage_timings.record(ExplainStage::PhysicalPlanning, physical_started.elapsed());

    let lowered_sql = match &execution {
        PublicReadPhysicalPlan::LoweredSql(lowered_batch) => {
            let artifact_started = Instant::now();
            let lowered_sql = render_lowered_read_batch_sql(
                lowered_batch,
                &analysis.bound_statement.bound_parameters,
                &analysis.bound_statement.statement_context,
                dialect,
            )?;
            stage_timings.record(
                ExplainStage::ArtifactPreparation,
                artifact_started.elapsed(),
            );
            lowered_sql
        }
        PublicReadPhysicalPlan::ReadTimeProjection(_) => Vec::new(),
        PublicReadPhysicalPlan::HistoryRead(_) => Vec::new(),
    };

    let optimized_logical_plan = match &execution {
        PublicReadPhysicalPlan::LoweredSql(_) => logical_plan.clone(),
        PublicReadPhysicalPlan::ReadTimeProjection(_) => logical_plan.clone(),
        PublicReadPhysicalPlan::HistoryRead(history_read_plan) => {
            analysis.logical_plan_with_history_read(history_read_plan.clone())
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
        compiled_artifacts: PublicReadExplainCompiledArtifacts {
            pushdown_decision: pushdown_decision.clone(),
            lowered_sql,
        },
        routing_passes: routing_passes.clone(),
        stage_timings: stage_timings.finish(),
    })?;

    Ok(SpecializedPublicReadPreparation::Prepared(PublicReadPlan {
        freshness_contract,
        resolved_relations: analysis
            .semantics
            .resolved_relations
            .iter()
            .map(|binding| binding.descriptor.public_name.clone())
            .collect(),
        source_statement_sql: analysis.bound_statement.statement.to_string(),
        route_via_sql2: crate::sql2::should_route_selected_read(
            origin_key,
            &analysis
                .semantics
                .resolved_relations
                .iter()
                .map(|binding| binding.descriptor.public_name.clone())
                .collect::<Vec<_>>(),
        ),
        logical_plan: optimized_logical_plan,
        bound_parameters,
        runtime_bindings,
        public_output_columns,
        explain,
        execution,
    }))
}

fn state_history_pushdown_decision(plan: &StateHistoryReadPlan) -> Option<PushdownDecision> {
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

fn entity_history_pushdown_decision(plan: &EntityHistoryReadPlan) -> Option<PushdownDecision> {
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
        if matches!(field, EntityHistoryField::Property(_)) {
            residual_predicates.push(entity_history_predicate_expr(predicate));
        }
    }

    Some(PushdownDecision {
        accepted_predicates,
        rejected_predicates: Vec::new(),
        residual_predicates,
    })
}

fn file_history_pushdown_decision(plan: &FileHistoryReadPlan) -> Option<PushdownDecision> {
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

fn directory_history_pushdown_decision(
    plan: &DirectoryHistoryReadPlan,
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
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
) -> Result<Option<PublicReadPlan>, LixError> {
    let registry =
        crate::catalog::load_public_surface_registry_with_backend(backend, Some(active_version_id))
            .await
            .map_err(|error| LixError::new(error.code, error.description))?;
    let compiler_metadata =
        crate::sql::prepare::load_sql_compiler_metadata(backend, &registry).await?;
    try_prepare_public_read_with_registry_and_internal_access(
        backend.dialect(),
        &registry,
        &compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        origin_key,
        None,
    )
    .await
}

pub(super) async fn try_prepare_public_read_with_registry_and_internal_access(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    compiler_metadata: &super::super::SqlCompilerMetadata,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
    parse_duration: Option<Duration>,
) -> Result<Option<PublicReadPlan>, LixError> {
    if let Some(surface_name) = first_removed_builtin_surface_reference(parsed_statements) {
        return Err(removed_builtin_surface_unknown_table_error(&surface_name));
    }

    try_prepare_public_read_with_internal_access(
        dialect,
        registry,
        compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        origin_key,
        parse_duration,
    )
    .await
}

async fn try_prepare_public_read_with_internal_access(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    compiler_metadata: &super::super::SqlCompilerMetadata,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
    parse_duration: Option<Duration>,
) -> Result<Option<PublicReadPlan>, LixError> {
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
    let bind_started = Instant::now();
    let bound_public_read = bind_public_read_statement(
        statement,
        params.to_vec(),
        StatementContext {
            dialect: Some(dialect),
            origin_key: origin_key.map(ToString::to_string),
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
            dialect,
            compiler_metadata,
            bound_statement.clone(),
            broad_statement.clone(),
            origin_key,
            explain_request.as_ref(),
            &registry,
            public_output_columns.clone(),
            stage_timings.clone(),
        )
        .await?
        {
            return Ok(Some(prepared));
        }
    }
    let specialized = try_prepare_public_read_via_specialized_optimization(
        dialect,
        compiler_metadata,
        bound_statement,
        active_history_root_commit_id,
        origin_key,
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
                    dialect,
                    compiler_metadata,
                    bound_statement,
                    broad_statement,
                    origin_key,
                    explain_request.as_ref(),
                    &registry,
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
    dialect: SqlDialect,
    compiler_metadata: &super::super::SqlCompilerMetadata,
    bound_statement: BoundStatement,
    broad_statement: Option<BroadPublicReadStatement>,
    origin_key: Option<&str>,
    explain_request: Option<&crate::sql::explain::ExplainRequest>,
    registry: &SurfaceRegistry,
    public_output_columns: Option<Vec<String>>,
    mut stage_timings: ExplainTimingCollector,
) -> Result<Option<PublicReadPlan>, LixError> {
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
    //   lowered read batch.
    // - artifact_preparation: render backend SQL from the lowered read batch.
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
        resolved_relations: read_summary.bound_surface_bindings.clone(),
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
        .statement_context
        .requested_version_id
        .as_deref();
    let capability_started = Instant::now();
    stage_timings.record(
        ExplainStage::CapabilityResolution,
        capability_started.elapsed(),
    );

    let routing_started = Instant::now();
    let routed_broad_read = match route_broad_public_read_statement_with_known_live_layouts(
        &broad_statement,
        registry,
        dialect,
        active_version_id,
        &compiler_metadata.known_live_schema_definitions,
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
        resolved_relations: read_summary.bound_surface_bindings.clone(),
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
    let Some(lowered_batch) = lower_broad_public_read_for_execution_with_layouts(
        &routed_broad_read.broad_statement,
        registry,
        dialect,
        bound_statement.bound_parameters.len(),
        active_version_id,
        &compiler_metadata.known_live_schema_definitions,
    )?
    else {
        return Ok(None);
    };
    stage_timings.record(ExplainStage::PhysicalPlanning, physical_started.elapsed());
    let freshness_contract =
        super::bound_surface_freshness_contract(&read_summary.bound_surface_bindings)
            .expect("broad public read should bind at least one surface");

    let semantic_read = PublicReadSemantics {
        resolved_relations: read_summary.bound_surface_bindings.clone(),
        broad_statement: Some(Box::new(broad_statement)),
        surface_read_plan: None,
    };

    let artifact_started = Instant::now();
    let lowered_sql = render_lowered_read_batch_sql(
        &lowered_batch,
        &bound_statement.bound_parameters,
        &bound_statement.statement_context,
        dialect,
    )?;
    stage_timings.record(
        ExplainStage::ArtifactPreparation,
        artifact_started.elapsed(),
    );
    let resolved_relations: Vec<String> = semantic_read
        .resolved_relations
        .iter()
        .map(|binding| binding.descriptor.public_name.clone())
        .collect();
    let explain = build_public_read_explain_artifacts(PublicReadExplainBuildInput {
        request: explain_request.cloned(),
        semantics: semantic_read,
        logical_plan: logical_plan.clone(),
        optimized_logical_plan: optimized_logical_plan.clone(),
        execution: PublicReadPhysicalPlan::LoweredSql(lowered_batch.clone()),
        compiled_artifacts: PublicReadExplainCompiledArtifacts {
            pushdown_decision: Some(PushdownDecision::default()),
            lowered_sql,
        },
        routing_passes: routed_broad_read.pass_traces.clone(),
        stage_timings: stage_timings.finish(),
    })?;

    Ok(Some(PublicReadPlan {
        freshness_contract,
        resolved_relations: resolved_relations.clone(),
        source_statement_sql: bound_statement.statement.to_string(),
        route_via_sql2: crate::sql2::should_route_selected_read(origin_key, &resolved_relations),
        logical_plan,
        bound_parameters: bound_statement.bound_parameters.clone(),
        runtime_bindings: runtime_binding_values_from_statement_context(
            &bound_statement.statement_context,
        )?,
        public_output_columns,
        explain,
        execution: PublicReadPhysicalPlan::LoweredSql(lowered_batch),
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

fn is_direct_only_history_surface(binding: &ResolvedRelation) -> bool {
    builtin_catalog_compiler_facade()
        .history_read_semantics(binding)
        .is_some()
}

#[cfg(test)]
pub(super) async fn prepare_public_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
) -> Option<PublicReadPlan> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        origin_key,
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
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
) -> Result<Option<PublicReadPlan>, LixError> {
    try_prepare_public_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        origin_key,
    )
    .await
}
