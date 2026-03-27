use crate::errors::sql_unknown_column_error;
use crate::filesystem::live_projection::{
    build_filesystem_directory_projection_sql, build_filesystem_file_projection_sql,
    FilesystemProjectionScope,
};
use crate::live_state::schema_access::{
    normalized_projection_sql_for_schema, payload_column_name_for_schema,
    snapshot_select_expr_for_schema, tracked_relation_name,
};
use crate::sql::public::backend::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql::public::catalog::{
    SurfaceBinding, SurfaceColumnType, SurfaceFamily, SurfaceOverridePredicate,
    SurfaceOverrideValue, SurfaceRegistry, SurfaceVariant,
};
use crate::sql::public::core::parser::parse_sql_script;
use crate::sql::public::planner::ir::{
    CanonicalAdminKind, CanonicalAdminScan, CanonicalChangeScan, CanonicalStateScan,
    CanonicalWorkingChangesScan, FilesystemKind, ReadPlan, StructuredPublicRead, VersionScope,
};
use crate::sql::public::planner::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};
use crate::version::{version_descriptor_schema_key, version_ref_schema_key, GLOBAL_VERSION_ID};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, LimitClause, OrderBy, OrderByExpr, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadProgram {
    pub(crate) statements: Vec<Statement>,
    pub(crate) pushdown_decision: PushdownDecision,
    pub(crate) result_columns: LoweredResultColumns,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoweredResultColumn {
    Untyped,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LoweredResultColumns {
    Static(Vec<LoweredResultColumn>),
    ByColumnName(BTreeMap<String, LoweredResultColumn>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BroadPublicRelationSummary {
    pub(crate) public_relations: BTreeSet<String>,
    pub(crate) internal_relations: BTreeSet<String>,
    pub(crate) external_relations: BTreeSet<String>,
}

mod broad;

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement(
    statement: &mut Statement,
) -> Result<(), LixError> {
    broad::rewrite_supported_public_read_surfaces_in_statement(statement, SqlDialect::Sqlite)
}

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id(
    statement: &mut Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
    active_version_id: Option<&str>,
) -> Result<(), LixError> {
    broad::rewrite_supported_public_read_surfaces_in_statement_with_registry_and_active_version_id(
        statement,
        registry,
        dialect,
        active_version_id,
    )
}

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry(
    statement: &mut Statement,
    registry: &SurfaceRegistry,
) -> Result<(), LixError> {
    broad::rewrite_supported_public_read_surfaces_in_statement_with_registry(
        statement,
        registry,
        SqlDialect::Sqlite,
        None,
    )
}

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement_with_registry_and_dialect(
    statement: &mut Statement,
    registry: &SurfaceRegistry,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    broad::rewrite_supported_public_read_surfaces_in_statement_with_registry(
        statement, registry, dialect, None,
    )
}

pub(crate) fn summarize_bound_public_read_statement_with_registry(
    statement: &Statement,
    registry: &SurfaceRegistry,
) -> Result<Option<BroadPublicRelationSummary>, LixError> {
    broad::summarize_bound_public_read_statement_with_registry(statement, registry)
}

pub(crate) fn lower_read_for_execution(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    lower_read_for_execution_with_layouts(
        dialect,
        structured_read,
        effective_state_request,
        effective_state_plan,
        &BTreeMap::new(),
    )
}

pub(crate) fn lower_read_for_execution_with_layouts(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    let result_columns = lowered_result_columns(structured_read);
    match structured_read.surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_state_read_for_execution(
                dialect,
                structured_read,
                effective_state_request,
                effective_state_plan,
                known_live_layouts,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
                    result_columns: result_columns.clone(),
                })
            })
        }
        SurfaceFamily::Entity => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_entity_read_for_execution(
                dialect,
                structured_read,
                effective_state_request,
                effective_state_plan,
                known_live_layouts,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
                    result_columns: result_columns.clone(),
                })
            })
        }
        SurfaceFamily::Change => {
            lower_change_read_for_execution(dialect, structured_read).map(|statement| {
                let pushdown_decision =
                    if canonical_working_changes_scan(&structured_read.read_command.root).is_some()
                    {
                        working_changes_pushdown_decision(structured_read)
                    } else {
                        change_pushdown_decision(structured_read)
                    };
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision,
                    result_columns: result_columns.clone(),
                })
            })
        }
        SurfaceFamily::Admin => {
            lower_admin_read_for_execution(dialect, structured_read).map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: admin_pushdown_decision(structured_read),
                    result_columns: result_columns.clone(),
                })
            })
        }
        SurfaceFamily::Filesystem => lower_filesystem_read_for_execution(dialect, structured_read)
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: filesystem_pushdown_decision(structured_read),
                    result_columns: result_columns.clone(),
                })
            }),
    }
}

fn lower_state_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<Statement>, LixError> {
    if let Some(error) =
        state_read_exposed_column_error(&canonicalized.surface_binding, effective_state_request)
    {
        return Err(error);
    }

    let (pushdown_predicates, residual_selection) =
        if entity_surface_has_live_payload_collisions(&canonicalized.surface_binding) {
            (Vec::new(), canonicalized.query.selection.clone())
        } else {
            split_state_selection_for_pushdown(
                canonicalized.query.selection.as_ref(),
                effective_state_plan,
            )
        };
    let Some(derived_query) = build_state_source_query(
        dialect,
        canonicalized
            .bound_statement
            .execution_context
            .requested_version_id
            .as_deref(),
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
        known_live_layouts,
    )?
    else {
        return Ok(None);
    };
    let query =
        build_lowered_read_query(dialect, canonicalized, derived_query, residual_selection)?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn build_lowered_read_query(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    source_query: Query,
    selection: Option<Expr>,
) -> Result<Query, LixError> {
    let mut projection = structured_read.query.projection.clone();
    let active_version_id = structured_read
        .bound_statement
        .execution_context
        .requested_version_id
        .as_deref();
    rewrite_nested_filesystem_surfaces_in_select_items(
        dialect,
        active_version_id,
        &mut projection,
    )?;

    let mut selection = selection;
    if let Some(selection) = &mut selection {
        rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, selection)?;
    }

    let mut order_by = structured_read.query.order_by.clone();
    rewrite_nested_filesystem_surfaces_in_order_by(dialect, active_version_id, order_by.as_mut())?;

    let derived_alias = structured_read.query.source_alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&structured_read.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });

    Ok(Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            distinct: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(source_query),
                    alias: derived_alias,
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection,
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            connect_by: None,
            flavor: sqlparser::ast::SelectFlavor::Standard,
        }))),
        order_by,
        limit_clause: structured_read.query.limit_clause.clone(),
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    })
}

fn lowered_result_columns(structured_read: &StructuredPublicRead) -> LoweredResultColumns {
    if structured_read.query.uses_wildcard_projection() {
        let columns = structured_read
            .surface_binding
            .column_types
            .iter()
            .map(|(name, column_type)| {
                (
                    name.clone(),
                    lowered_result_column_from_surface_type(*column_type),
                )
            })
            .collect();
        return LoweredResultColumns::ByColumnName(columns);
    }

    LoweredResultColumns::Static(
        structured_read
            .query
            .projection
            .iter()
            .map(|item| {
                lowered_result_column_for_projection_item(item, &structured_read.surface_binding)
            })
            .collect(),
    )
}

fn lowered_result_column_for_projection_item(
    item: &SelectItem,
    surface_binding: &SurfaceBinding,
) -> LoweredResultColumn {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
            return LoweredResultColumn::Untyped;
        }
    };

    let column = match expr {
        Expr::Identifier(ident) => ident.value.as_str(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|part| part.value.as_str())
            .unwrap_or_default(),
        _ => return LoweredResultColumn::Untyped,
    };

    surface_column_type(surface_binding, column)
        .map(lowered_result_column_from_surface_type)
        .unwrap_or(LoweredResultColumn::Untyped)
}

fn lowered_result_column_from_surface_type(column_type: SurfaceColumnType) -> LoweredResultColumn {
    match column_type {
        SurfaceColumnType::Boolean => LoweredResultColumn::Boolean,
        SurfaceColumnType::String
        | SurfaceColumnType::Integer
        | SurfaceColumnType::Number
        | SurfaceColumnType::Json => LoweredResultColumn::Untyped,
    }
}

fn surface_column_type(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<SurfaceColumnType> {
    surface_binding
        .column_types
        .iter()
        .find_map(|(candidate, kind)| candidate.eq_ignore_ascii_case(column).then_some(*kind))
}

fn state_read_exposed_column_error(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Option<LixError> {
    let exposed = surface_binding
        .exposed_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    let missing = effective_state_request
        .required_columns
        .iter()
        .filter(|column| !exposed.contains(&column.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return None;
    }
    if surface_binding.descriptor.public_name == "lix_state"
        && missing
            .iter()
            .any(|column| matches!(column.as_str(), "version_id" | "lixcol_version_id"))
    {
        return Some(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "lix_state does not expose version_id; use lix_state_by_version for explicit version filters"
                    .to_string(),
        });
    }
    let column = missing[0].clone();
    let available = surface_binding
        .exposed_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    Some(sql_unknown_column_error(
        &column,
        Some(&surface_binding.descriptor.public_name),
        &available,
        None,
    ))
}

fn lower_entity_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<Statement>, LixError> {
    if canonicalized.query.uses_wildcard_projection() {
        return Ok(None);
    }

    let (pushdown_predicates, residual_selection) = split_state_selection_for_pushdown(
        canonicalized.query.selection.as_ref(),
        effective_state_plan,
    );
    let Some(derived_query) = build_entity_source_query(
        dialect,
        canonicalized
            .bound_statement
            .execution_context
            .requested_version_id
            .as_deref(),
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
        known_live_layouts,
    )?
    else {
        return Ok(None);
    };
    let query =
        build_lowered_read_query(dialect, canonicalized, derived_query, residual_selection)?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn lower_change_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<Statement>, LixError> {
    if canonical_working_changes_scan(&canonicalized.read_command.root).is_some() {
        return lower_working_changes_read_for_execution(dialect, canonicalized);
    }

    let derived_query = build_change_source_query()?;
    let query = build_lowered_read_query(
        dialect,
        canonicalized,
        derived_query,
        canonicalized.query.selection.clone(),
    )?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn lower_working_changes_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<Statement>, LixError> {
    let active_version_id = canonicalized
        .bound_statement
        .execution_context
        .requested_version_id
        .as_deref()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public read '{}' requires a session-requested active version id",
                    canonicalized.surface_binding.descriptor.public_name
                ),
            )
        })?;
    let derived_query = build_working_changes_source_query(active_version_id)?;
    let query = build_lowered_read_query(
        dialect,
        canonicalized,
        derived_query,
        canonicalized.query.selection.clone(),
    )?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn rewrite_nested_filesystem_surfaces_in_query(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    query: &mut Query,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_set_expr(
        dialect,
        active_version_id,
        query.body.as_mut(),
        top_level,
    )?;
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_set_expr(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    expr: &mut SetExpr,
    top_level: bool,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => rewrite_nested_filesystem_surfaces_in_select(
            dialect,
            active_version_id,
            select,
            top_level,
        ),
        SetExpr::Query(query) => {
            rewrite_nested_filesystem_surfaces_in_query(dialect, active_version_id, query, false)
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_set_expr(
                dialect,
                active_version_id,
                left.as_mut(),
                top_level,
            )?;
            rewrite_nested_filesystem_surfaces_in_set_expr(
                dialect,
                active_version_id,
                right.as_mut(),
                top_level,
            )
        }
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_select(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    select: &mut Select,
    top_level: bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_nested_filesystem_surfaces_in_table_with_joins(
            dialect,
            active_version_id,
            table,
            top_level,
        )?;
    }
    if let Some(selection) = &mut select.selection {
        rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, selection)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_select_items(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    projection: &mut [SelectItem],
) -> Result<(), LixError> {
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            }
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_order_by(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    order_by: Option<&mut OrderBy>,
) -> Result<(), LixError> {
    let Some(order_by) = order_by else {
        return Ok(());
    };
    let OrderByKind::Expressions(ordering) = &mut order_by.kind else {
        return Ok(());
    };
    for item in ordering {
        rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, &mut item.expr)?;
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_with_joins(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    table: &mut TableWithJoins,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_table_factor(
        dialect,
        active_version_id,
        &mut table.relation,
        top_level,
    )?;
    for join in &mut table.joins {
        rewrite_nested_filesystem_surfaces_in_table_factor(
            dialect,
            active_version_id,
            &mut join.relation,
            top_level,
        )?;
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_factor(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    relation: &mut TableFactor,
    top_level: bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(surface_name) = table_name_terminal(name) else {
                return Ok(());
            };
            if top_level || !is_rewriteable_filesystem_public_surface_name(surface_name) {
                return Ok(());
            }
            let Some(derived_query) =
                build_nested_filesystem_surface_query(dialect, active_version_id, surface_name)?
            else {
                return Ok(());
            };
            let derived_alias = alias.clone().or_else(|| {
                Some(TableAlias {
                    explicit: false,
                    name: Ident::new(surface_name),
                    columns: Vec::new(),
                })
            });
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_query(dialect, active_version_id, subquery, false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_nested_filesystem_surfaces_in_table_with_joins(
            dialect,
            active_version_id,
            table_with_joins,
            false,
        ),
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_expr(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    expr: &mut Expr,
) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, left)?;
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)
        }
        Expr::InList { expr, list, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            for item in list {
                rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, low)?;
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, pattern)
        }
        Expr::Subquery(query) => {
            rewrite_nested_filesystem_surfaces_in_query(dialect, active_version_id, query, false)
        }
        Expr::Exists { subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_query(dialect, active_version_id, subquery, false)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(dialect, active_version_id, expr)?;
            rewrite_nested_filesystem_surfaces_in_query(dialect, active_version_id, subquery, false)
        }
        _ => Ok(()),
    }
}

fn build_nested_filesystem_surface_query(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_name: &str,
) -> Result<Option<Query>, LixError> {
    let normalized = surface_name.to_ascii_lowercase();
    let query = match normalized.as_str() {
        "lix_file" => parse_single_query(&build_filesystem_file_projection_sql(
            FilesystemProjectionScope::ActiveVersion,
            active_version_id,
            false,
            dialect,
        )?)?,
        "lix_file_by_version" => parse_single_query(&build_filesystem_file_projection_sql(
            FilesystemProjectionScope::ExplicitVersion,
            None,
            false,
            dialect,
        )?)?,
        "lix_directory" => parse_single_query(&build_filesystem_directory_projection_sql(
            FilesystemProjectionScope::ActiveVersion,
            active_version_id,
            dialect,
        )?)?,
        "lix_directory_by_version" => {
            parse_single_query(&build_filesystem_directory_projection_sql(
                FilesystemProjectionScope::ExplicitVersion,
                None,
                dialect,
            )?)?
        }
        _ => return Ok(None),
    };
    Ok(Some(query))
}

fn query_contains_nested_filesystem_surface(query: &Query) -> bool {
    query_set_expr_contains_nested_filesystem_surface(query.body.as_ref(), true)
}

fn query_set_expr_contains_nested_filesystem_surface(expr: &SetExpr, top_level: bool) -> bool {
    match expr {
        SetExpr::Select(select) => select_contains_nested_filesystem_surface(select, top_level),
        SetExpr::Query(query) => query_contains_nested_filesystem_surface(query),
        SetExpr::SetOperation { left, right, .. } => {
            query_set_expr_contains_nested_filesystem_surface(left.as_ref(), top_level)
                || query_set_expr_contains_nested_filesystem_surface(right.as_ref(), top_level)
        }
        _ => false,
    }
}

fn select_contains_nested_filesystem_surface(select: &Select, top_level: bool) -> bool {
    select
        .from
        .iter()
        .any(|table| table_with_joins_contains_nested_filesystem_surface(table, top_level))
        || select
            .selection
            .as_ref()
            .is_some_and(expr_contains_nested_filesystem_surface)
        || select
            .projection
            .iter()
            .any(select_item_contains_nested_filesystem_surface)
}

fn table_with_joins_contains_nested_filesystem_surface(
    table: &TableWithJoins,
    top_level: bool,
) -> bool {
    table_factor_contains_nested_filesystem_surface(&table.relation, top_level)
        || table
            .joins
            .iter()
            .any(|join| table_factor_contains_nested_filesystem_surface(&join.relation, top_level))
}

fn table_factor_contains_nested_filesystem_surface(
    relation: &TableFactor,
    top_level: bool,
) -> bool {
    match relation {
        TableFactor::Table { name, .. } => {
            !top_level
                && table_name_terminal(name)
                    .is_some_and(is_rewriteable_filesystem_public_surface_name)
        }
        TableFactor::Derived { subquery, .. } => {
            query_set_expr_contains_nested_filesystem_surface(subquery.body.as_ref(), false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_with_joins_contains_nested_filesystem_surface(table_with_joins, false),
        _ => false,
    }
}

fn select_item_contains_nested_filesystem_surface(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => expr_contains_nested_filesystem_surface(expr),
        SelectItem::ExprWithAlias { expr, .. } => expr_contains_nested_filesystem_surface(expr),
        _ => false,
    }
}

fn expr_contains_nested_filesystem_surface(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_nested_filesystem_surface(left)
                || expr_contains_nested_filesystem_surface(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => expr_contains_nested_filesystem_surface(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_nested_filesystem_surface(expr)
                || list.iter().any(expr_contains_nested_filesystem_surface)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_nested_filesystem_surface(expr)
                || expr_contains_nested_filesystem_surface(low)
                || expr_contains_nested_filesystem_surface(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_contains_nested_filesystem_surface(expr)
                || expr_contains_nested_filesystem_surface(pattern)
        }
        Expr::Subquery(query) => query_contains_nested_filesystem_surface(query),
        Expr::Exists { subquery, .. } => query_contains_nested_filesystem_surface(subquery),
        Expr::InSubquery { expr, subquery, .. } => {
            expr_contains_nested_filesystem_surface(expr)
                || query_contains_nested_filesystem_surface(subquery)
        }
        _ => false,
    }
}

fn table_name_terminal(name: &sqlparser::ast::ObjectName) -> Option<&str> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.as_str())
}

fn is_rewriteable_filesystem_public_surface_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "lix_file" | "lix_file_by_version" | "lix_directory" | "lix_directory_by_version"
    )
}

fn lower_admin_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<Statement>, LixError> {
    let Some(admin_scan) = canonical_admin_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };

    let derived_query = build_admin_source_query(admin_scan.kind)?;
    let query = build_lowered_read_query(
        dialect,
        canonicalized,
        derived_query,
        canonicalized.query.selection.clone(),
    )?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn lower_filesystem_read_for_execution(
    dialect: SqlDialect,
    canonicalized: &StructuredPublicRead,
) -> Result<Option<Statement>, LixError> {
    let Some(filesystem_scan) = canonical_filesystem_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };
    let active_version_id = canonicalized
        .bound_statement
        .execution_context
        .requested_version_id
        .as_deref();

    let derived_query = match (filesystem_scan.kind, filesystem_scan.version_scope) {
        (FilesystemKind::File, VersionScope::ActiveVersion) => {
            parse_single_query(&build_filesystem_file_projection_sql(
                FilesystemProjectionScope::ActiveVersion,
                active_version_id,
                false,
                dialect,
            )?)?
        }
        (FilesystemKind::File, VersionScope::ExplicitVersion)
            if canonicalized.surface_binding.descriptor.public_name == "lix_file_by_version" =>
        {
            parse_single_query(&build_filesystem_file_projection_sql(
                FilesystemProjectionScope::ExplicitVersion,
                None,
                false,
                dialect,
            )?)?
        }
        (FilesystemKind::Directory, VersionScope::ActiveVersion) => {
            parse_single_query(&build_filesystem_directory_projection_sql(
                FilesystemProjectionScope::ActiveVersion,
                active_version_id,
                dialect,
            )?)?
        }
        (FilesystemKind::Directory, VersionScope::ExplicitVersion)
            if canonicalized.surface_binding.descriptor.public_name
                == "lix_directory_by_version" =>
        {
            parse_single_query(&build_filesystem_directory_projection_sql(
                FilesystemProjectionScope::ExplicitVersion,
                None,
                dialect,
            )?)?
        }
        _ => return Ok(None),
    };
    let query = build_lowered_read_query(
        dialect,
        canonicalized,
        derived_query,
        canonicalized.query.selection.clone(),
    )?;
    Ok(Some(Statement::Query(Box::new(query))))
}

fn build_state_source_query(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<Query>, LixError> {
    let sql = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => build_effective_state_source_sql(
            dialect,
            active_version_id,
            effective_state_request,
            surface_binding,
            pushdown_predicates,
            known_live_layouts,
        )?,
        SurfaceVariant::History => return Ok(None),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => return Ok(None),
    };
    parse_single_query(&sql).map(Some)
}

fn build_admin_source_query(kind: CanonicalAdminKind) -> Result<Query, LixError> {
    let version_descriptor_table = tracked_relation_name("lix_version_descriptor");
    let version_ref_table = tracked_relation_name("lix_version_ref");
    let version_descriptor_name_column = quote_ident(&builtin_payload_column_name(
        version_descriptor_schema_key(),
        "name",
    ));
    let version_descriptor_hidden_column = quote_ident(&builtin_payload_column_name(
        version_descriptor_schema_key(),
        "hidden",
    ));
    let version_ref_commit_id_column = quote_ident(&builtin_payload_column_name(
        version_ref_schema_key(),
        "commit_id",
    ));
    let sql = match kind {
        CanonicalAdminKind::Version => format!(
            "SELECT \
                d.entity_id AS id, \
                d.{version_descriptor_name_column} AS name, \
                COALESCE(d.{version_descriptor_hidden_column}, false) AS hidden, \
                t.commit_id AS commit_id \
             FROM {version_descriptor_table} d \
             LEFT JOIN ( \
               SELECT entity_id, {version_ref_commit_id_column} AS commit_id \
               FROM ( \
                 SELECT \
                   entity_id, \
                   {version_ref_commit_id_column}, \
                   ROW_NUMBER() OVER ( \
                     PARTITION BY entity_id \
                     ORDER BY updated_at DESC, \
                              created_at DESC \
                   ) AS rn \
                 FROM {version_ref_table} \
                 WHERE schema_key = 'lix_version_ref' \
                   AND untracked = true \
                   AND {version_ref_commit_id_column} IS NOT NULL \
               ) ranked_version_refs \
               WHERE rn = 1 \
             ) t \
               ON t.entity_id = d.entity_id \
             WHERE d.schema_key = '{descriptor_schema_key}' \
               AND d.version_id = '{global_version}' \
               AND d.is_tombstone = 0",
            version_descriptor_table = quote_ident(&version_descriptor_table),
            version_ref_table = quote_ident(&version_ref_table),
            version_descriptor_name_column = version_descriptor_name_column,
            version_descriptor_hidden_column = version_descriptor_hidden_column,
            version_ref_commit_id_column = version_ref_commit_id_column,
            descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        ),
    };

    parse_single_query(&sql)
}

fn build_entity_source_query(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<Option<Query>, LixError> {
    let projection = entity_projection_sql(surface_binding, effective_state_request);
    let projection = if projection.is_empty() {
        "entity_id AS lixcol_entity_id".to_string()
    } else {
        projection.join(", ")
    };

    let state_source_query = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => {
            Some(parse_single_query(&build_effective_live_source_sql(
                dialect,
                active_version_id,
                effective_state_request,
                surface_binding,
                pushdown_predicates,
                known_live_layouts,
                false,
            )?)?)
        }
        SurfaceVariant::History => None,
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => None,
    };
    let Some(state_source_query) = state_source_query else {
        return Ok(None);
    };
    let mut predicates = Vec::new();
    for predicate in &surface_binding.implicit_overrides.predicate_overrides {
        predicates.push(render_override_predicate(predicate));
    }

    let source_sql = state_source_query.to_string();
    let sql = if predicates.is_empty() {
        format!("SELECT {projection} FROM ({source_sql}) AS state_source")
    } else {
        format!(
            "SELECT {projection} FROM ({source_sql}) AS state_source WHERE {}",
            predicates.join(" AND ")
        )
    };
    parse_single_query(&sql).map(Some)
}

fn build_effective_state_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> Result<String, LixError> {
    build_effective_live_source_sql(
        dialect,
        active_version_id,
        effective_state_request,
        surface_binding,
        pushdown_predicates,
        known_live_layouts,
        true,
    )
}

fn build_effective_live_source_sql(
    dialect: SqlDialect,
    active_version_id: Option<&str>,
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
    include_snapshot_content: bool,
) -> Result<String, LixError> {
    let schema_keys = effective_state_request
        .schema_set
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    if schema_keys.is_empty() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "public state read lowerer requires a bounded schema set for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    }

    let (target_version_predicates, source_predicates) =
        split_effective_state_pushdown_predicates(pushdown_predicates);
    let commit_table = tracked_relation_name("lix_commit");
    let cse_table = tracked_relation_name("lix_change_set_element");
    let commit_change_set_id_column =
        quote_ident(&builtin_payload_column_name("lix_commit", "change_set_id"));
    let cse_change_set_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_set_id",
    ));
    let cse_change_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_id",
    ));
    let target_versions_cte = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => {
            active_target_versions_cte_sql(active_version_id.ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "public read '{}' requires a session-requested active version id",
                        surface_binding.descriptor.public_name
                    ),
                )
            })?)
        }
        SurfaceVariant::ByVersion => {
            explicit_target_versions_cte_sql(&schema_keys, &target_version_predicates)
        }
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "state lowerer expected default or by-version surface".to_string(),
            });
        }
    };
    let schema_winner_rows_sql = effective_state_schema_winner_rows_sql(
        dialect,
        surface_binding,
        &schema_keys,
        &source_predicates,
        effective_state_request,
        known_live_layouts,
        include_snapshot_content,
    );
    Ok(format!(
        "WITH \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               {commit_change_set_id_column} AS change_set_id \
             FROM {commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_column} AS change_set_id, \
               {cse_change_id_column} AS change_id \
             FROM {cse_table} \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           change_commit_by_change_id AS ( \
             SELECT \
               cse.change_id AS change_id, \
               MAX(cbv.commit_id) AS commit_id \
             FROM change_set_element_by_version cse \
             JOIN commit_by_version cbv \
               ON cbv.change_set_id = cse.change_set_id \
             WHERE cse.change_id IS NOT NULL \
             GROUP BY cse.change_id \
           ) \
         {schema_winner_rows_sql}",
        target_versions_cte = target_versions_cte,
        schema_winner_rows_sql = schema_winner_rows_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        commit_table = commit_table,
        cse_table = cse_table,
        commit_change_set_id_column = commit_change_set_id_column,
        cse_change_set_id_column = cse_change_set_id_column,
        cse_change_id_column = cse_change_id_column,
    ))
}

fn active_target_versions_cte_sql(active_version_id: &str) -> String {
    format!(
        "target_versions AS ( \
           SELECT '{active_version_id}' AS version_id \
         )",
        active_version_id = escape_sql_string(active_version_id),
    )
}

fn explicit_target_versions_cte_sql(
    schema_keys: &[String],
    target_version_predicates: &[String],
) -> String {
    let version_descriptor_table = tracked_relation_name("lix_version_descriptor");
    let version_descriptor_hidden_column = quote_ident(&builtin_payload_column_name(
        version_descriptor_schema_key(),
        "hidden",
    ));
    let hidden_global_requested = target_version_predicates
        .iter()
        .any(|predicate| predicate.contains(&format!("'{}'", GLOBAL_VERSION_ID)));
    let version_descriptor_predicates = vec![
        format!(
            "schema_key = '{}'",
            escape_sql_string(version_descriptor_schema_key())
        ),
        format!("version_id = '{}'", escape_sql_string(GLOBAL_VERSION_ID)),
        "is_tombstone = 0".to_string(),
        format!(
            "COALESCE({version_descriptor_hidden_column}, false) = false",
            version_descriptor_hidden_column = version_descriptor_hidden_column
        ),
    ];
    let schema_local_rows = schema_keys
        .iter()
        .map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE version_id <> '{global_version}' \
                   AND untracked = false",
                table_name = quote_ident(&tracked_relation_name(schema_key)),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        })
        .chain(schema_keys.iter().map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE version_id <> '{global_version}' \
                   AND untracked = true",
                table_name = quote_ident(&tracked_relation_name(schema_key)),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        }))
        .collect::<Vec<_>>();
    let all_target_versions = if schema_local_rows.is_empty() {
        if hidden_global_requested {
            format!(
                " UNION SELECT '{}' AS version_id",
                escape_sql_string(GLOBAL_VERSION_ID)
            )
        } else {
            String::new()
        }
    } else {
        let mut unions = Vec::new();
        unions.push(schema_local_rows.join(" UNION "));
        if hidden_global_requested {
            unions.push(format!(
                "SELECT '{}' AS version_id",
                escape_sql_string(GLOBAL_VERSION_ID)
            ));
        }
        format!(" UNION {}", unions.join(" UNION "))
    };
    let target_versions_where = render_where_clause_sql(target_version_predicates, " WHERE ");
    format!(
        "all_target_versions AS ( \
           SELECT DISTINCT entity_id AS version_id \
           FROM {version_descriptor_table} \
           WHERE {version_descriptor_predicates}\
           {all_target_versions} \
         ), \
         target_versions AS ( \
           SELECT version_id \
           FROM all_target_versions \
           {target_versions_where} \
         )",
        version_descriptor_table = version_descriptor_table,
        version_descriptor_predicates = version_descriptor_predicates.join(" AND "),
        all_target_versions = all_target_versions,
        target_versions_where = target_versions_where,
    )
}

fn effective_state_schema_winner_rows_sql(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    schema_keys: &[String],
    source_predicates: &[String],
    effective_state_request: &EffectiveStateRequest,
    known_live_layouts: &BTreeMap<String, JsonValue>,
    include_snapshot_content: bool,
) -> String {
    let payload_columns = effective_state_payload_columns(effective_state_request, surface_binding);
    schema_keys
        .iter()
        .map(|schema_key| {
            let table_name = quote_ident(&tracked_relation_name(schema_key));
            let untracked_table = quote_ident(&tracked_relation_name(schema_key));
            let tracked_full_projection = normalized_projection_sql_for_schema(
                schema_key,
                known_live_layouts.get(schema_key),
                Some("t"),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "live layout lookup for '{schema_key}' failed: {}",
                    error.description
                )
            });
            let untracked_full_projection = normalized_projection_sql_for_schema(
                schema_key,
                known_live_layouts.get(schema_key),
                Some("u"),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "live layout lookup for '{schema_key}' failed: {}",
                    error.description
                )
            });
            let ranked_payload_projection = render_state_payload_projection_list(
                dialect,
                surface_binding,
                schema_key,
                "ranked",
                &payload_columns,
                known_live_layouts,
            );
            let final_snapshot_projection = if include_snapshot_content {
                format!(
                    "{} AS snapshot_content, ",
                    snapshot_select_expr_for_schema(
                        schema_key,
                        known_live_layouts.get(schema_key),
                        dialect,
                        Some("ranked"),
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "live layout lookup for '{schema_key}' failed: {}",
                            error.description
                        )
                    })
                )
            } else {
                String::new()
            };
            let tracked_predicates = render_where_clause_sql(source_predicates, " AND ");
            let untracked_predicates = render_where_clause_sql(source_predicates, " AND ");
            format!(
                "SELECT \
                   ranked.effective_entity_id AS entity_id, \
                   ranked.effective_schema_key AS schema_key, \
                   ranked.effective_file_id AS file_id, \
                   ranked.effective_version_id AS version_id, \
                   ranked.effective_plugin_key AS plugin_key, \
                   {final_snapshot_projection} \
                   ranked.effective_schema_version AS schema_version, \
                   ranked.effective_created_at AS created_at, \
                   ranked.effective_updated_at AS updated_at, \
                   ranked.effective_global AS global, \
                   ranked.effective_change_id AS change_id, \
                   ranked.effective_commit_id AS commit_id, \
                   ranked.effective_untracked AS untracked, \
                   ranked.effective_writer_key AS writer_key, \
                   ranked.effective_metadata AS metadata{ranked_payload_projection} \
                 FROM ( \
                   SELECT \
                     c.effective_entity_id AS effective_entity_id, \
                     c.effective_schema_key AS effective_schema_key, \
                     c.effective_file_id AS effective_file_id, \
                     c.effective_version_id AS effective_version_id, \
                     c.effective_plugin_key AS effective_plugin_key, \
                     c.effective_schema_version AS effective_schema_version, \
                     c.effective_created_at AS effective_created_at, \
                     c.effective_updated_at AS effective_updated_at, \
                     c.effective_global AS effective_global, \
                     c.effective_change_id AS effective_change_id, \
                     c.effective_commit_id AS effective_commit_id, \
                     c.effective_untracked AS effective_untracked, \
                     c.effective_writer_key AS effective_writer_key, \
                     c.effective_metadata AS effective_metadata, \
                     c.is_tombstone AS is_tombstone{normalized_ranked_projection}, \
                     ROW_NUMBER() OVER ( \
                       PARTITION BY c.effective_version_id, c.effective_entity_id, c.effective_schema_key, c.effective_file_id \
                       ORDER BY \
                         c.precedence ASC, \
                         c.effective_updated_at DESC, \
                         c.effective_created_at DESC, \
                         COALESCE(c.effective_change_id, '') DESC \
                     ) AS rn \
                   FROM ( \
                     SELECT \
                       t.entity_id AS effective_entity_id, \
                       t.schema_key AS effective_schema_key, \
                       t.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       t.plugin_key AS effective_plugin_key, \
                       t.schema_version AS effective_schema_version, \
                       t.created_at AS effective_created_at, \
                       t.updated_at AS effective_updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS effective_global, \
                       t.change_id AS effective_change_id, \
                       cc.commit_id AS effective_commit_id, \
                       false AS effective_untracked, \
                       t.writer_key AS effective_writer_key, \
                       t.metadata AS effective_metadata, \
                       t.is_tombstone AS is_tombstone{tracked_full_projection}, \
                       2 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id = t.version_id \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.untracked = false{tracked_predicates} \
                     UNION ALL \
                     SELECT \
                       t.entity_id AS effective_entity_id, \
                       t.schema_key AS effective_schema_key, \
                       t.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       t.plugin_key AS effective_plugin_key, \
                       t.schema_version AS effective_schema_version, \
                       t.created_at AS effective_created_at, \
                       t.updated_at AS effective_updated_at, \
                       true AS effective_global, \
                       t.change_id AS effective_change_id, \
                       cc.commit_id AS effective_commit_id, \
                       false AS effective_untracked, \
                       t.writer_key AS effective_writer_key, \
                       t.metadata AS effective_metadata, \
                       t.is_tombstone AS is_tombstone{tracked_full_projection}, \
                       4 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND t.version_id = '{global_version}' \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.version_id = '{global_version}' \
                       AND t.untracked = false{tracked_predicates} \
                     UNION ALL \
                     SELECT \
                       u.entity_id AS effective_entity_id, \
                       u.schema_key AS effective_schema_key, \
                       u.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       u.plugin_key AS effective_plugin_key, \
                       u.schema_version AS effective_schema_version, \
                       u.created_at AS effective_created_at, \
                       u.updated_at AS effective_updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS effective_global, \
                       NULL AS effective_change_id, \
                       'untracked' AS effective_commit_id, \
                       true AS effective_untracked, \
                       u.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       1 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id = u.version_id \
                     WHERE u.untracked = true{untracked_predicates} \
                     UNION ALL \
                     SELECT \
                       u.entity_id AS effective_entity_id, \
                       u.schema_key AS effective_schema_key, \
                       u.file_id AS effective_file_id, \
                       tv.version_id AS effective_version_id, \
                       u.plugin_key AS effective_plugin_key, \
                       u.schema_version AS effective_schema_version, \
                       u.created_at AS effective_created_at, \
                       u.updated_at AS effective_updated_at, \
                       true AS effective_global, \
                       NULL AS effective_change_id, \
                       'untracked' AS effective_commit_id, \
                       true AS effective_untracked, \
                       u.writer_key AS effective_writer_key, \
                       u.metadata AS effective_metadata, \
                       0 AS is_tombstone{untracked_full_projection}, \
                       3 AS precedence \
                     FROM {untracked_table} u \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND u.version_id = '{global_version}' \
                     WHERE u.version_id = '{global_version}' \
                       AND u.untracked = true{untracked_predicates} \
                   ) AS c \
                 ) AS ranked \
                 WHERE ranked.rn = 1 \
                   AND ranked.is_tombstone = 0",
                final_snapshot_projection = final_snapshot_projection,
                ranked_payload_projection = ranked_payload_projection,
                normalized_ranked_projection = normalized_projection_sql_for_schema(
                    schema_key,
                    known_live_layouts.get(schema_key),
                    Some("c"),
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "live layout lookup for '{schema_key}' failed: {}",
                        error.description
                    )
                }),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
                tracked_full_projection = tracked_full_projection,
                tracked_predicates = tracked_predicates,
                untracked_full_projection = untracked_full_projection,
                untracked_predicates = untracked_predicates,
                table_name = table_name,
                untracked_table = untracked_table,
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn split_effective_state_pushdown_predicates(
    pushdown_predicates: &[String],
) -> (Vec<String>, Vec<String>) {
    let mut target_version_predicates = Vec::new();
    let mut source_predicates = Vec::new();
    for predicate in pushdown_predicates {
        if predicate.contains("version_id") && !predicate.contains("root_commit_id") {
            target_version_predicates.push(predicate.clone());
        } else {
            source_predicates.push(predicate.clone());
        }
    }
    (target_version_predicates, source_predicates)
}

fn render_where_clause_sql(predicates: &[String], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", predicates.join(" AND "))
    }
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn builtin_payload_column_name(schema_key: &str, property_name: &str) -> String {
    payload_column_name_for_schema(schema_key, None, property_name).unwrap_or_else(|error| {
        panic!(
            "builtin live schema '{schema_key}' must include '{property_name}': {}",
            error.description
        )
    })
}

fn effective_state_payload_columns(
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
) -> Vec<String> {
    effective_state_request
        .required_columns
        .iter()
        .filter(|column| {
            !is_live_state_envelope_column(column)
                || entity_surface_uses_payload_alias(surface_binding, column)
        })
        .cloned()
        .collect()
}

fn is_live_state_raw_envelope_column(column: &str) -> bool {
    matches!(
        column,
        "entity_id"
            | "schema_key"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "schema_version"
            | "metadata"
            | "created_at"
            | "updated_at"
            | "global"
            | "change_id"
            | "commit_id"
            | "untracked"
            | "writer_key"
            | "root_commit_id"
            | "depth"
    )
}

fn is_live_state_envelope_column(column: &str) -> bool {
    matches!(
        column,
        "entity_id"
            | "schema_key"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "schema_version"
            | "metadata"
            | "created_at"
            | "updated_at"
            | "global"
            | "change_id"
            | "commit_id"
            | "untracked"
            | "writer_key"
            | "lixcol_entity_id"
            | "lixcol_schema_key"
            | "lixcol_file_id"
            | "lixcol_version_id"
            | "lixcol_plugin_key"
            | "lixcol_schema_version"
            | "lixcol_change_id"
            | "lixcol_commit_id"
            | "lixcol_created_at"
            | "lixcol_updated_at"
            | "lixcol_global"
            | "lixcol_untracked"
            | "lixcol_writer_key"
            | "lixcol_metadata"
            | "snapshot_content"
            | "commit_created_at"
            | "root_commit_id"
            | "depth"
            | "lixcol_root_commit_id"
            | "lixcol_depth"
    )
}

fn entity_surface_has_live_payload_collisions(surface_binding: &SurfaceBinding) -> bool {
    surface_binding.descriptor.surface_family == SurfaceFamily::Entity
        && surface_binding.descriptor.surface_variant != SurfaceVariant::History
        && surface_binding
            .exposed_columns
            .iter()
            .any(|column| is_live_state_raw_envelope_column(column))
}

fn entity_surface_uses_payload_alias(surface_binding: &SurfaceBinding, column: &str) -> bool {
    entity_surface_has_live_payload_collisions(surface_binding)
        && surface_binding
            .exposed_columns
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(column))
        && is_live_state_raw_envelope_column(column)
}

fn entity_surface_payload_alias(column: &str) -> String {
    format!("payload__{column}")
}

fn render_state_payload_projection_list(
    dialect: SqlDialect,
    surface_binding: &SurfaceBinding,
    schema_key: &str,
    table_alias: &str,
    payload_columns: &[String],
    known_live_layouts: &BTreeMap<String, JsonValue>,
) -> String {
    if payload_columns.is_empty() {
        return String::new();
    }

    format!(
        ", {}",
        payload_columns
            .iter()
            .map(|column| {
                let expression = render_live_payload_column_expr(
                    dialect,
                    schema_key,
                    known_live_layouts.get(schema_key),
                    table_alias,
                    column,
                );
                let alias = if entity_surface_uses_payload_alias(surface_binding, column) {
                    entity_surface_payload_alias(column)
                } else {
                    column.clone()
                };
                format!("{expression} AS {}", render_identifier(&alias))
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_live_payload_column_expr(
    dialect: SqlDialect,
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    table_alias: &str,
    public_column: &str,
) -> String {
    let Ok(column_name) =
        payload_column_name_for_schema(schema_key, schema_definition, public_column)
    else {
        return "NULL".to_string();
    };
    let qualified = format!("{}.{}", quote_ident(table_alias), quote_ident(&column_name));
    match public_column {
        "metadata" => qualified,
        _ => match dialect {
            SqlDialect::Sqlite => format!(
                "CASE WHEN {qualified} IS NULL THEN NULL ELSE json_extract({qualified}, '$') || '' END"
            ),
            SqlDialect::Postgres => format!(
                "CASE WHEN {qualified} IS NULL THEN NULL ELSE (CAST({qualified} AS JSONB) #>> '{{}}') END"
            ),
        },
    }
}

fn build_change_source_query() -> Result<Query, LixError> {
    parse_single_query(
        "SELECT \
            ch.id AS id, \
            ch.entity_id AS entity_id, \
            ch.schema_key AS schema_key, \
            ch.schema_version AS schema_version, \
            ch.file_id AS file_id, \
            ch.plugin_key AS plugin_key, \
            ch.metadata AS metadata, \
            ch.created_at AS created_at, \
            CASE \
                WHEN ch.snapshot_id = 'no-content' THEN NULL \
                ELSE s.content \
            END AS snapshot_content \
         FROM lix_internal_change ch \
         LEFT JOIN lix_internal_snapshot s \
            ON s.id = ch.snapshot_id",
    )
}

fn build_working_changes_source_query(active_version_id: &str) -> Result<Query, LixError> {
    let version_ref_table = tracked_relation_name("lix_version_ref");
    let commit_tracked_table = tracked_relation_name("lix_commit");
    let cse_tracked_table = tracked_relation_name("lix_change_set_element");
    let commit_edge_table = tracked_relation_name("lix_commit_edge");
    let version_ref_commit_id_column = quote_ident(&builtin_payload_column_name(
        version_ref_schema_key(),
        "commit_id",
    ));
    let commit_change_set_id_column =
        quote_ident(&builtin_payload_column_name("lix_commit", "change_set_id"));
    let cse_change_set_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_set_id",
    ));
    let cse_change_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "change_id",
    ));
    let cse_entity_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "entity_id",
    ));
    let cse_schema_key_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "schema_key",
    ));
    let cse_file_id_column = quote_ident(&builtin_payload_column_name(
        "lix_change_set_element",
        "file_id",
    ));
    let commit_edge_child_id_column =
        quote_ident(&builtin_payload_column_name("lix_commit_edge", "child_id"));
    let commit_edge_parent_id_column =
        quote_ident(&builtin_payload_column_name("lix_commit_edge", "parent_id"));
    let active_version_cte = format!(
        "active_version AS ( \
            SELECT '{active_version_id}' AS version_id \
        )",
        active_version_id = escape_sql_string(active_version_id),
    );

    parse_single_query(&format!(
        "WITH RECURSIVE \
            {active_version_cte}, \
            scope_heads AS ( \
                SELECT \
                    'local' AS scope, \
                    (SELECT version_id FROM active_version) AS checkpoint_version_id, \
                    ( \
                        SELECT {version_ref_commit_id_column} \
                        FROM {version_ref_table} \
                        WHERE file_id = 'lix' \
                          AND entity_id = (SELECT version_id FROM active_version) \
                          AND version_id = 'global' \
                          AND {version_ref_commit_id_column} IS NOT NULL \
                        LIMIT 1 \
                    ) AS head_commit_id \
                UNION ALL \
                SELECT \
                    'global' AS scope, \
                    'global' AS checkpoint_version_id, \
                    ( \
                        SELECT {version_ref_commit_id_column} \
                        FROM {version_ref_table} \
                        WHERE file_id = 'lix' \
                          AND entity_id = 'global' \
                          AND version_id = 'global' \
                          AND {version_ref_commit_id_column} IS NOT NULL \
                        LIMIT 1 \
                    ) AS head_commit_id \
            ), \
            scope_baselines AS ( \
                SELECT \
                    scope, \
                    head_commit_id, \
                    COALESCE( \
                        ( \
                            SELECT checkpoint_commit_id \
                            FROM lix_internal_last_checkpoint \
                            WHERE version_id = checkpoint_version_id \
                            LIMIT 1 \
                        ), \
                        head_commit_id \
                    ) AS baseline_commit_id \
                FROM scope_heads \
            ), \
            commit_rows AS ( \
                SELECT \
                    entity_id AS id, \
                    {commit_change_set_id_column} AS change_set_id, \
                    created_at \
                FROM {commit_untracked_table} \
                WHERE file_id = 'lix' \
                  AND version_id = 'global' \
                  AND {commit_change_set_id_column} IS NOT NULL \
                UNION \
                SELECT \
                    entity_id AS id, \
                    {commit_change_set_id_column} AS change_set_id, \
                    created_at \
                FROM {commit_tracked_table} \
                WHERE file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND {commit_change_set_id_column} IS NOT NULL \
            ), \
            change_rows AS ( \
                SELECT \
                    ch.id AS change_id, \
                    snap.content AS row_snapshot \
                FROM lix_internal_change ch \
                LEFT JOIN lix_internal_snapshot snap \
                    ON snap.id = ch.snapshot_id \
            ), \
            change_set_element_rows AS ( \
                SELECT \
                    {cse_change_set_id_column} AS change_set_id, \
                    {cse_change_id_column} AS change_id, \
                    {cse_entity_id_column} AS entity_id, \
                    {cse_schema_key_column} AS schema_key, \
                    {cse_file_id_column} AS file_id \
                FROM {cse_untracked_table} \
                WHERE file_id = 'lix' \
                  AND version_id = 'global' \
                  AND {cse_change_set_id_column} IS NOT NULL \
                UNION \
                SELECT \
                    {cse_change_set_id_column} AS change_set_id, \
                    {cse_change_id_column} AS change_id, \
                    {cse_entity_id_column} AS entity_id, \
                    {cse_schema_key_column} AS schema_key, \
                    {cse_file_id_column} AS file_id \
                FROM {cse_tracked_table} \
                WHERE file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND {cse_change_set_id_column} IS NOT NULL \
            ), \
            tip_ancestry_walk AS ( \
                SELECT \
                    scope.scope AS scope, \
                    scope.head_commit_id AS commit_id, \
                    0 AS depth \
                FROM scope_baselines scope \
                UNION ALL \
                SELECT \
                    walk.scope AS scope, \
                    edge.{commit_edge_parent_id_column} AS commit_id, \
                    walk.depth + 1 AS depth \
                FROM tip_ancestry_walk walk \
                JOIN {commit_edge_table} edge \
                    ON edge.{commit_edge_child_id_column} = walk.commit_id \
                WHERE edge.version_id = 'global' \
                  AND edge.is_tombstone = 0 \
                  AND edge.{commit_edge_parent_id_column} IS NOT NULL \
                  AND walk.depth < 512 \
            ), \
            tip_ancestry AS ( \
                SELECT scope, commit_id, MIN(depth) AS depth \
                FROM tip_ancestry_walk \
                GROUP BY scope, commit_id \
            ), \
            baseline_ancestry_walk AS ( \
                SELECT \
                    scope.scope AS scope, \
                    scope.baseline_commit_id AS commit_id, \
                    0 AS depth \
                FROM scope_baselines scope \
                UNION ALL \
                SELECT \
                    walk.scope AS scope, \
                    edge.{commit_edge_parent_id_column} AS commit_id, \
                    walk.depth + 1 AS depth \
                FROM baseline_ancestry_walk walk \
                JOIN {commit_edge_table} edge \
                    ON edge.{commit_edge_child_id_column} = walk.commit_id \
                WHERE edge.version_id = 'global' \
                  AND edge.is_tombstone = 0 \
                  AND edge.{commit_edge_parent_id_column} IS NOT NULL \
                  AND walk.depth < 512 \
            ), \
            baseline_ancestry AS ( \
                SELECT scope, commit_id, MIN(depth) AS depth \
                FROM baseline_ancestry_walk \
                GROUP BY scope, commit_id \
            ), \
            tip_candidates AS ( \
                SELECT \
                    anc.scope AS scope, \
                    cse.entity_id, \
                    cse.schema_key, \
                    cse.file_id, \
                    cse.change_id, \
                    anc.depth, \
                    c.created_at AS commit_created_at \
                FROM tip_ancestry anc \
                JOIN commit_rows c \
                    ON c.id = anc.commit_id \
                JOIN change_set_element_rows cse \
                    ON cse.change_set_id = c.change_set_id \
            ), \
            tip_min_depth AS ( \
                SELECT \
                    scope, \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    MIN(depth) AS depth \
                FROM tip_candidates \
                GROUP BY scope, entity_id, schema_key, file_id \
            ), \
            tip_best_created_at AS ( \
                SELECT \
                    tc.scope, \
                    tc.entity_id, \
                    tc.schema_key, \
                    tc.file_id, \
                    MAX(tc.commit_created_at) AS commit_created_at \
                FROM tip_candidates tc \
                JOIN tip_min_depth d \
                    ON d.scope = tc.scope \
                   AND d.entity_id = tc.entity_id \
                   AND d.schema_key = tc.schema_key \
                   AND d.file_id = tc.file_id \
                   AND d.depth = tc.depth \
                GROUP BY tc.scope, tc.entity_id, tc.schema_key, tc.file_id \
            ), \
            tip_entries AS ( \
                SELECT \
                    tc.scope, \
                    tc.entity_id, \
                    tc.schema_key, \
                    tc.file_id, \
                    MAX(tc.change_id) AS change_id \
                FROM tip_candidates tc \
                JOIN tip_min_depth d \
                    ON d.scope = tc.scope \
                   AND d.entity_id = tc.entity_id \
                   AND d.schema_key = tc.schema_key \
                   AND d.file_id = tc.file_id \
                   AND d.depth = tc.depth \
                JOIN tip_best_created_at bc \
                    ON bc.scope = tc.scope \
                   AND bc.entity_id = tc.entity_id \
                   AND bc.schema_key = tc.schema_key \
                   AND bc.file_id = tc.file_id \
                   AND bc.commit_created_at = tc.commit_created_at \
                GROUP BY tc.scope, tc.entity_id, tc.schema_key, tc.file_id \
            ), \
            baseline_candidates AS ( \
                SELECT \
                    anc.scope AS scope, \
                    cse.entity_id, \
                    cse.schema_key, \
                    cse.file_id, \
                    cse.change_id, \
                    anc.depth, \
                    c.created_at AS commit_created_at \
                FROM baseline_ancestry anc \
                JOIN commit_rows c \
                    ON c.id = anc.commit_id \
                JOIN change_set_element_rows cse \
                    ON cse.change_set_id = c.change_set_id \
            ), \
            baseline_min_depth AS ( \
                SELECT \
                    scope, \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    MIN(depth) AS depth \
                FROM baseline_candidates \
                GROUP BY scope, entity_id, schema_key, file_id \
            ), \
            baseline_best_created_at AS ( \
                SELECT \
                    bc.scope, \
                    bc.entity_id, \
                    bc.schema_key, \
                    bc.file_id, \
                    MAX(bc.commit_created_at) AS commit_created_at \
                FROM baseline_candidates bc \
                JOIN baseline_min_depth d \
                    ON d.scope = bc.scope \
                   AND d.entity_id = bc.entity_id \
                   AND d.schema_key = bc.schema_key \
                   AND d.file_id = bc.file_id \
                   AND d.depth = bc.depth \
                GROUP BY bc.scope, bc.entity_id, bc.schema_key, bc.file_id \
            ), \
            baseline_entries AS ( \
                SELECT \
                    bc.scope, \
                    bc.entity_id, \
                    bc.schema_key, \
                    bc.file_id, \
                    MAX(bc.change_id) AS change_id \
                FROM baseline_candidates bc \
                JOIN baseline_min_depth d \
                    ON d.scope = bc.scope \
                   AND d.entity_id = bc.entity_id \
                   AND d.schema_key = bc.schema_key \
                   AND d.file_id = bc.file_id \
                   AND d.depth = bc.depth \
                JOIN baseline_best_created_at bca \
                    ON bca.scope = bc.scope \
                   AND bca.entity_id = bc.entity_id \
                   AND bca.schema_key = bc.schema_key \
                   AND bca.file_id = bc.file_id \
                   AND bca.commit_created_at = bc.commit_created_at \
                GROUP BY bc.scope, bc.entity_id, bc.schema_key, bc.file_id \
            ), \
            paired_entries AS ( \
                SELECT \
                    tip.scope AS scope, \
                    tip.entity_id AS entity_id, \
                    tip.schema_key AS schema_key, \
                    tip.file_id AS file_id, \
                    base.change_id AS before_change_id, \
                    tip.change_id AS after_change_id \
                FROM tip_entries tip \
                LEFT JOIN baseline_entries base \
                    ON base.scope = tip.scope \
                   AND base.entity_id = tip.entity_id \
                   AND base.schema_key = tip.schema_key \
                   AND base.file_id = tip.file_id \
                UNION ALL \
                SELECT \
                    base.scope AS scope, \
                    base.entity_id AS entity_id, \
                    base.schema_key AS schema_key, \
                    base.file_id AS file_id, \
                    base.change_id AS before_change_id, \
                    NULL AS after_change_id \
                FROM baseline_entries base \
                LEFT JOIN tip_entries tip \
                    ON tip.scope = base.scope \
                   AND tip.entity_id = base.entity_id \
                   AND tip.schema_key = base.schema_key \
                   AND tip.file_id = base.file_id \
                WHERE tip.entity_id IS NULL \
            ), \
            resolved_rows AS ( \
                SELECT \
                    pair.scope AS scope, \
                    pair.entity_id AS entity_id, \
                    pair.schema_key AS schema_key, \
                    pair.file_id AS file_id, \
                    pair.before_change_id AS before_change_id, \
                    pair.after_change_id AS after_change_id, \
                    before_change.row_snapshot AS before_row_snapshot, \
                    after_change.row_snapshot AS after_row_snapshot \
                FROM paired_entries pair \
                LEFT JOIN change_rows before_change \
                    ON before_change.change_id = pair.before_change_id \
                LEFT JOIN change_rows after_change \
                    ON after_change.change_id = pair.after_change_id \
            ) \
            SELECT * FROM ( \
                SELECT \
                    entity_id, \
                    schema_key, \
                    file_id, \
                    CASE WHEN scope = 'global' THEN true ELSE false END AS lixcol_global, \
                    CASE \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN NULL \
                        ELSE before_change_id \
                    END AS before_change_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN NULL \
                        ELSE after_change_id \
                    END AS after_change_id, \
                    CASE \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN NULL \
                        ELSE ( \
                            SELECT baseline_commit_id \
                            FROM scope_baselines scope \
                            WHERE scope.scope = resolved_rows.scope \
                            LIMIT 1 \
                        ) \
                    END AS before_commit_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN NULL \
                        ELSE ( \
                            SELECT head_commit_id \
                            FROM scope_baselines scope \
                            WHERE scope.scope = resolved_rows.scope \
                            LIMIT 1 \
                        ) \
                    END AS after_commit_id, \
                    CASE \
                        WHEN before_row_snapshot IS NOT NULL AND after_row_snapshot IS NULL THEN 'removed' \
                        WHEN before_row_snapshot IS NULL AND after_row_snapshot IS NOT NULL THEN 'added' \
                        WHEN before_row_snapshot IS NOT NULL \
                             AND after_row_snapshot IS NOT NULL \
                             AND before_change_id != after_change_id THEN 'modified' \
                    END AS status \
                FROM resolved_rows \
            ) AS working_changes \
            WHERE status IS NOT NULL",
        active_version_cte = active_version_cte,
        version_ref_table = version_ref_table,
        version_ref_commit_id_column = version_ref_commit_id_column,
        commit_change_set_id_column = commit_change_set_id_column,
        commit_tracked_table = commit_tracked_table,
        commit_untracked_table = quote_ident(&tracked_relation_name("lix_commit")),
        cse_change_set_id_column = cse_change_set_id_column,
        cse_change_id_column = cse_change_id_column,
        cse_entity_id_column = cse_entity_id_column,
        cse_schema_key_column = cse_schema_key_column,
        cse_file_id_column = cse_file_id_column,
        cse_tracked_table = cse_tracked_table,
        cse_untracked_table = quote_ident(&tracked_relation_name("lix_change_set_element")),
        commit_edge_table = commit_edge_table,
        commit_edge_parent_id_column = commit_edge_parent_id_column,
        commit_edge_child_id_column = commit_edge_child_id_column,
    ))
}

fn canonical_admin_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::public::planner::ir::CanonicalAdminScan> {
    match read_plan {
        ReadPlan::AdminScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_admin_scan(input),
    }
}

fn canonical_working_changes_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::public::planner::ir::CanonicalWorkingChangesScan> {
    match read_plan {
        ReadPlan::WorkingChangesScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_working_changes_scan(input),
    }
}

fn canonical_filesystem_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::public::planner::ir::CanonicalFilesystemScan> {
    match read_plan {
        ReadPlan::FilesystemScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_filesystem_scan(input),
    }
}

fn entity_projection_sql(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> Vec<String> {
    let mut projections = Vec::new();
    for column in &effective_state_request.required_columns {
        let Some(expression) = entity_projection_sql_for_column(surface_binding, column) else {
            continue;
        };
        if !projections.iter().any(|existing| existing == &expression) {
            projections.push(expression);
        }
    }
    projections
}

fn entity_projection_sql_for_column(
    surface_binding: &SurfaceBinding,
    column: &str,
) -> Option<String> {
    if let Some(source_column) =
        entity_hidden_alias_source_column(column, surface_binding.descriptor.surface_variant)
    {
        let alias = render_identifier(column);
        return Some(format!("{source_column} AS {alias}"));
    }

    if surface_binding
        .exposed_columns
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(column))
    {
        let alias = render_identifier(column);
        let expression = if entity_surface_uses_payload_alias(surface_binding, column) {
            render_identifier(&entity_surface_payload_alias(column))
        } else {
            render_identifier(column)
        };
        return Some(format!("{expression} AS {alias}"));
    }

    None
}

fn entity_hidden_alias_source_column(alias: &str, variant: SurfaceVariant) -> Option<&'static str> {
    match alias.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_change_id" => Some("change_id"),
        "lixcol_created_at" => Some("created_at"),
        "lixcol_updated_at" => Some("updated_at"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        "lixcol_version_id" if variant == SurfaceVariant::ByVersion => Some("version_id"),
        _ => None,
    }
}

fn render_override_predicate(predicate: &SurfaceOverridePredicate) -> String {
    match &predicate.value {
        SurfaceOverrideValue::Null => {
            format!("{} IS NULL", render_identifier(&predicate.column))
        }
        value => format!(
            "{} = {}",
            render_identifier(&predicate.column),
            render_override_value(value)
        ),
    }
}

fn render_override_value(value: &SurfaceOverrideValue) -> String {
    match value {
        SurfaceOverrideValue::Null => "NULL".to_string(),
        SurfaceOverrideValue::Boolean(value) => value.to_string(),
        SurfaceOverrideValue::Number(value) => value.clone(),
        SurfaceOverrideValue::String(value) => format!("'{}'", escape_sql_string(value)),
    }
}

fn build_pushdown_decision(effective_state_plan: &EffectiveStatePlan) -> PushdownDecision {
    PushdownDecision {
        accepted_predicates: effective_state_plan
            .pushdown_safe_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
        rejected_predicates: effective_state_plan
            .residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.to_string(),
                reason:
                    "day-1 public read lowering keeps this predicate above effective-state resolution"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates: effective_state_plan
            .residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

fn change_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "public change-scan lowering keeps change predicates above the derived change source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn working_changes_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "public working-changes lowering keeps predicates above the derived working-changes source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn admin_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "public admin-scan lowering keeps admin predicates above the derived admin source"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn filesystem_pushdown_decision(canonicalized: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "public filesystem lowering keeps filesystem predicates above the derived source"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn split_state_selection_for_pushdown(
    selection: Option<&Expr>,
    effective_state_plan: &EffectiveStatePlan,
) -> (Vec<String>, Option<Expr>) {
    let Some(selection) = selection else {
        return (Vec::new(), None);
    };

    let mut pushdown = Vec::new();
    let mut residual = Vec::new();
    for predicate in split_conjunctive_predicates(selection) {
        if effective_state_plan
            .pushdown_safe_predicates
            .iter()
            .any(|accepted| accepted == &predicate)
        {
            pushdown.push(predicate.to_string());
        } else {
            residual.push(predicate);
        }
    }

    (pushdown, combine_conjunctive_predicates(residual))
}

fn split_conjunctive_predicates(expr: &Expr) -> Vec<Expr> {
    let mut predicates = Vec::new();
    collect_conjunctive_predicates(expr, &mut predicates);
    predicates
}

fn collect_conjunctive_predicates(expr: &Expr, predicates: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_conjunctive_predicates(left, predicates);
            collect_conjunctive_predicates(right, predicates);
        }
        Expr::Nested(inner) => collect_conjunctive_predicates(inner, predicates),
        _ => predicates.push(expr.clone()),
    }
}

fn combine_conjunctive_predicates(predicates: Vec<Expr>) -> Option<Expr> {
    let mut predicates = predicates.into_iter();
    let first = predicates.next()?;
    Some(predicates.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }))
}

fn read_predicates_from_query(canonicalized: &StructuredPublicRead) -> Vec<String> {
    canonicalized
        .query
        .selection_predicates
        .iter()
        .cloned()
        .into_iter()
        .map(|predicate| predicate.to_string())
        .collect()
}

fn parse_single_query(sql: &str) -> Result<Query, LixError> {
    let mut statements = parse_sql_script(sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single lowered public read statement".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered public read to parse as a query".to_string(),
        });
    };
    Ok(*query)
}

fn render_identifier(value: &str) -> String {
    Ident::new(value).to_string()
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{
        lower_read_for_execution_with_layouts, rewrite_supported_public_read_surfaces_in_statement,
        LoweredReadProgram,
    };
    use crate::sql::public::catalog::SurfaceRegistry;
    use crate::sql::public::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql::public::planner::canonicalize::canonicalize_read;
    use crate::sql::public::planner::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
    use crate::sql::public::planner::semantics::effective_state_resolver::build_effective_state;
    use serde_json::{json, Value as JsonValue};
    use crate::{SqlDialect, Value};
    use std::collections::BTreeMap;

    fn lowered_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        lowered_program_with_layouts(registry, sql, &BTreeMap::new())
    }

    fn lowered_program_with_layouts(
        registry: &SurfaceRegistry,
        sql: &str,
        known_live_layouts: &BTreeMap<String, JsonValue>,
    ) -> Option<LoweredReadProgram> {
        let mut statements =
            crate::sql::public::core::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let mut execution_context = ExecutionContext::with_dialect(SqlDialect::Sqlite);
        execution_context.requested_version_id = Some("main".to_string());
        let bound =
            BoundStatement::from_statement(statement, Vec::<Value>::new(), execution_context);
        let structured_read = canonicalize_read(bound, registry)
            .expect("query should canonicalize")
            .into_structured_read();
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read);
        let effective_state = build_effective_state(&structured_read, dependency_spec.as_ref());
        lower_read_for_execution_with_layouts(
            SqlDialect::Sqlite,
            &structured_read,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
            known_live_layouts,
        )
        .expect("lowering should succeed")
    }

    #[test]
    fn lowers_builtin_entity_reads_through_state_surfaces() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
        )
        .expect("builtin entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("untracked = false"));
        assert!(lowered_sql.contains("untracked = true"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["key = 'hello'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn rewrites_joined_builtin_entity_surfaces_to_internal_queries() {
        let mut statements = crate::sql::public::core::parser::parse_sql_script(
            "SELECT COUNT(*) \
             FROM lix_entity_label el \
             JOIN lix_label l ON l.id = el.label_id \
             WHERE el.entity_id = 'commit-1' \
               AND el.schema_key = 'lix_commit' \
               AND el.file_id = 'lix' \
               AND l.name = 'checkpoint'",
        )
        .expect("SQL should parse");
        let mut statement = statements.pop().expect("single statement");

        rewrite_supported_public_read_surfaces_in_statement(&mut statement)
            .expect("joined entity surfaces should rewrite");
        let lowered_sql = statement.to_string();

        assert!(lowered_sql.contains("FROM lix_entity_label"));
        assert!(lowered_sql.contains("JOIN lix_label"));
        assert!(!lowered_sql.contains("lix_internal_live_v1_lix_entity_label"));
        assert!(!lowered_sql.contains("lix_internal_live_v1_lix_label"));
    }

    #[test]
    fn rewrites_cte_and_joined_state_surfaces_to_internal_queries() {
        let mut statements = crate::sql::public::core::parser::parse_sql_script(
            "WITH keyed AS ( \
               SELECT entity_id, schema_key \
               FROM lix_state_by_version \
               WHERE schema_key = 'lix_key_value' \
                 AND lixcol_version_id = 'main' \
             ) \
             SELECT keyed.schema_key, COUNT(*) \
             FROM keyed \
             JOIN lix_state_by_version sv \
               ON sv.entity_id = keyed.entity_id \
             WHERE sv.lixcol_version_id = 'main' \
             GROUP BY keyed.schema_key \
             ORDER BY keyed.schema_key",
        )
        .expect("SQL should parse");
        let mut statement = statements.pop().expect("single statement");

        rewrite_supported_public_read_surfaces_in_statement(&mut statement)
            .expect("cte and joined state surfaces should rewrite");
        let lowered_sql = statement.to_string();

        assert!(!lowered_sql.contains("FROM lix_state "));
        assert!(!lowered_sql.contains("JOIN lix_state_by_version"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(lowered_sql.contains("all_target_versions AS"));
    }

    #[test]
    fn does_not_rewrite_cte_names_that_shadow_public_surfaces() {
        let mut statements = crate::sql::public::core::parser::parse_sql_script(
            "WITH lix_state AS (SELECT 'shadow' AS entity_id) \
             SELECT entity_id FROM lix_state",
        )
        .expect("SQL should parse");
        let mut statement = statements.pop().expect("single statement");

        rewrite_supported_public_read_surfaces_in_statement(&mut statement)
            .expect("shadowing cte should remain untouched");
        let lowered_sql = statement.to_string();

        assert!(lowered_sql.contains("FROM lix_state"));
        assert!(!lowered_sql.contains("lix_internal_live_v1_"));
        assert!(!lowered_sql.contains("untracked = true"));
    }

    #[test]
    fn lowers_dynamic_entity_reads_with_scalar_override_predicates() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(
            crate::sql::public::catalog::DynamicEntitySurfaceSpec {
                schema_key: "message".to_string(),
                visible_columns: vec!["body".to_string(), "id".to_string()],
                column_types: BTreeMap::new(),
                predicate_overrides: vec![
                    crate::sql::public::catalog::SurfaceOverridePredicate {
                        column: "file_id".to_string(),
                        value: crate::sql::public::catalog::SurfaceOverrideValue::String(
                            "inlang".to_string(),
                        ),
                    },
                    crate::sql::public::catalog::SurfaceOverridePredicate {
                        column: "plugin_key".to_string(),
                        value: crate::sql::public::catalog::SurfaceOverrideValue::String(
                            "inlang_sdk".to_string(),
                        ),
                    },
                    crate::sql::public::catalog::SurfaceOverridePredicate {
                        column: "global".to_string(),
                        value: crate::sql::public::catalog::SurfaceOverrideValue::Boolean(true),
                    },
                ],
            },
        );

        let mut known_live_layouts = BTreeMap::new();
        known_live_layouts.insert(
            "message".to_string(),
            json!({
                "x-lix-key": "message",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "body": { "type": "string" },
                    "id": { "type": "string" }
                },
                "required": ["id"]
            }),
        );

        let lowered = lowered_program_with_layouts(
            &registry,
            "SELECT body, lixcol_global FROM message WHERE id = 'm1'",
            &known_live_layouts,
        )
        .expect("dynamic entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_live_v1_message"));
        assert!(lowered_sql.contains("file_id = 'inlang'"));
        assert!(lowered_sql.contains("plugin_key = 'inlang_sdk'"));
        assert!(lowered_sql.contains("global = true"));
    }

    #[test]
    fn rejects_entity_wildcard_reads_for_live_lowering() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        assert_eq!(
            lowered_program(&registry, "SELECT * FROM lix_key_value"),
            None
        );
    }

    #[test]
    fn lowers_state_reads_through_explicit_source_boundary() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
        )
        .expect("state read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_live_v1_lix_key_value"));
        assert!(!lowered_sql.contains("FROM lix_state"));
        assert!(!lowered_sql.contains(") WHERE schema_key = 'lix_key_value'"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            vec!["schema_key = 'lix_key_value'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            Vec::<String>::new()
        );
    }

    #[test]
    fn lowers_change_reads_through_internal_change_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT id, schema_key, snapshot_content FROM lix_change WHERE entity_id = 'entity-1'",
        )
        .expect("change read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM (SELECT ch.id AS id"));
        assert!(lowered_sql.contains("FROM lix_internal_change ch"));
        assert!(lowered_sql.contains("LEFT JOIN lix_internal_snapshot s"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["entity_id = 'entity-1'".to_string()]
        );
    }

    #[test]
    fn lowers_working_changes_reads_with_nested_filesystem_subqueries() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT COUNT(*) \
             FROM lix_working_changes wc \
             WHERE wc.file_id IN (SELECT f.id FROM lix_file f WHERE f.path = '/hello.txt')",
        )
        .expect("working changes read with nested filesystem subquery should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(
            !lowered_sql.contains("FROM lix_file"),
            "lowered sql still contains public lix_file: {lowered_sql}"
        );
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("FROM (WITH RECURSIVE"));
    }

    #[test]
    fn lowers_filesystem_current_and_versioned_reads_through_internal_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();

        let current = lowered_program(
            &registry,
            "SELECT id, path, data FROM lix_file WHERE id = 'file-1'",
        )
        .expect("filesystem current read should lower");
        let current_sql = current.statements[0].to_string();

        assert!(current_sql.contains("lix_internal_live_v1_lix_file_descriptor"));
        assert!(current_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(current_sql.contains("lix_internal_binary_blob_store"));
        assert!(!current_sql.contains("FROM lix_file_by_version"));
        assert_eq!(
            current.pushdown_decision.residual_predicates,
            vec!["id = 'file-1'".to_string()]
        );

        let by_version = lowered_program(
            &registry,
            "SELECT id, path FROM lix_directory_by_version \
             WHERE id = 'dir-1' AND lixcol_version_id = 'version-a'",
        )
        .expect("filesystem by-version read should lower");
        let by_version_sql = by_version.statements[0].to_string();

        assert!(by_version_sql.contains("lix_internal_live_v1_lix_directory_descriptor"));
        assert!(by_version_sql.contains("all_target_versions AS"));
        assert!(!by_version_sql.contains("FROM lix_directory_by_version"));
        assert_eq!(
            by_version.pushdown_decision.residual_predicates,
            vec![
                "id = 'dir-1'".to_string(),
                "lixcol_version_id = 'version-a'".to_string()
            ]
        );
    }

    #[test]
    fn rejects_removed_active_version_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        assert!(registry.bind_relation_name("lix_active_version").is_none());
    }

    #[test]
    fn rejects_removed_active_account_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        assert!(registry.bind_relation_name("lix_active_account").is_none());
    }

    #[test]
    fn lowers_version_reads_through_internal_descriptor_and_pointer_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT id, name, hidden, commit_id FROM lix_version WHERE id = 'main'",
        )
        .expect("version read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_live_v1_lix_version_descriptor"));
        assert!(lowered_sql.contains("lix_internal_live_v1_lix_version_ref"));
        assert!(lowered_sql.contains("untracked = true"));
        assert!(!lowered_sql.contains("FROM lix_version"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["id = 'main'".to_string()]
        );
    }

    #[test]
    fn lowers_registered_schema_reads_through_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT value, lixcol_entity_id FROM lix_registered_schema WHERE lixcol_entity_id = 'x~1'",
        )
        .expect("registered schema read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_live_v1_lix_registered_schema"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("plugin_key = 'lix'"));
        assert!(lowered_sql.contains("global = true"));
        assert!(!lowered_sql.contains("FROM lix_registered_schema"));
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["lixcol_entity_id = 'x~1'".to_string()]
        );
    }
}
