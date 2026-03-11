use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
};
use crate::filesystem::live_projection::{
    build_filesystem_directory_history_projection_sql, build_filesystem_directory_projection_sql,
    build_filesystem_file_history_projection_sql, build_filesystem_file_projection_sql,
    build_filesystem_state_history_source_sql, FilesystemProjectionScope,
};
use crate::sql2::backend::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql2::catalog::{
    SurfaceBinding, SurfaceFamily, SurfaceOverridePredicate, SurfaceOverrideValue, SurfaceRegistry,
    SurfaceVariant,
};
use crate::sql2::core::parser::parse_sql_script;
use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::ir::{
    CanonicalAdminKind, CanonicalStateScan, FilesystemKind, ReadPlan, VersionScope,
};
use crate::sql2::planner::semantics::effective_state_resolver::{
    EffectiveStatePlan, EffectiveStateRequest,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    version_descriptor_schema_key, version_pointer_file_id, version_pointer_schema_key,
    version_pointer_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::LixError;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor, TableWithJoins,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredReadProgram {
    pub(crate) statements: Vec<Statement>,
    pub(crate) pushdown_decision: PushdownDecision,
}

pub(crate) fn lower_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
) -> Result<Option<LoweredReadProgram>, LixError> {
    match canonicalized.surface_binding.descriptor.surface_family {
        SurfaceFamily::State => {
            let Some(effective_state_request) = effective_state_request else {
                return Ok(None);
            };
            let Some(effective_state_plan) = effective_state_plan else {
                return Ok(None);
            };
            lower_state_read_for_execution(
                canonicalized,
                effective_state_request,
                effective_state_plan,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
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
                canonicalized,
                effective_state_request,
                effective_state_plan,
            )
            .map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: build_pushdown_decision(effective_state_plan),
                })
            })
        }
        SurfaceFamily::Change => lower_change_read_for_execution(canonicalized).map(|statement| {
            let pushdown_decision =
                if canonical_working_changes_scan(&canonicalized.read_command.root).is_some() {
                    working_changes_pushdown_decision(canonicalized)
                } else {
                    change_pushdown_decision(canonicalized)
                };
            statement.map(|statement| LoweredReadProgram {
                statements: vec![statement],
                pushdown_decision,
            })
        }),
        SurfaceFamily::Admin => lower_admin_read_for_execution(canonicalized).map(|statement| {
            statement.map(|statement| LoweredReadProgram {
                statements: vec![statement],
                pushdown_decision: admin_pushdown_decision(canonicalized),
            })
        }),
        SurfaceFamily::Filesystem => {
            lower_filesystem_read_for_execution(canonicalized).map(|statement| {
                statement.map(|statement| LoweredReadProgram {
                    statements: vec![statement],
                    pushdown_decision: filesystem_pushdown_decision(canonicalized),
                })
            })
        }
    }
}

pub(crate) fn rewrite_supported_public_read_surfaces_in_statement(
    statement: &mut Statement,
) -> Result<(), LixError> {
    match statement {
        Statement::Query(query) => rewrite_supported_public_read_surfaces_in_query(query),
        Statement::Explain { statement, .. } => {
            rewrite_supported_public_read_surfaces_in_statement(statement)
        }
        _ => Ok(()),
    }
}

fn lower_state_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
) -> Result<Option<Statement>, LixError> {
    if !state_read_references_exposed_columns(
        &canonicalized.surface_binding,
        effective_state_request,
    ) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let (pushdown_predicates, residual_selection) =
        split_state_selection_for_pushdown(select.selection.as_ref(), effective_state_plan);
    let Some(derived_query) = build_state_source_query(
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
    )?
    else {
        return Ok(None);
    };
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };
    select.selection = residual_selection;

    Ok(Some(Statement::Query(query)))
}

fn state_read_references_exposed_columns(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
) -> bool {
    let exposed = surface_binding
        .exposed_columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    effective_state_request
        .required_columns
        .iter()
        .all(|column| exposed.contains(&column.to_ascii_lowercase()))
}

fn lower_entity_read_for_execution(
    canonicalized: &CanonicalizedRead,
    effective_state_request: &EffectiveStateRequest,
    effective_state_plan: &EffectiveStatePlan,
) -> Result<Option<Statement>, LixError> {
    if query_uses_wildcard_projection(&canonicalized.bound_statement.statement) {
        return Ok(None);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let (pushdown_predicates, residual_selection) =
        split_state_selection_for_pushdown(select.selection.as_ref(), effective_state_plan);
    let Some(derived_query) = build_entity_source_query(
        &canonicalized.surface_binding,
        effective_state_request,
        &pushdown_predicates,
    )?
    else {
        return Ok(None);
    };
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };
    select.selection = residual_selection;

    Ok(Some(Statement::Query(query)))
}

fn lower_change_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    if canonical_working_changes_scan(&canonicalized.read_command.root).is_some() {
        return lower_working_changes_read_for_execution(canonicalized);
    }

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query = build_change_source_query()?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn lower_working_changes_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query = build_working_changes_source_query()?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn rewrite_nested_filesystem_surfaces_in_query(
    query: &mut Query,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_set_expr(query.body.as_mut(), top_level)?;
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_query(query: &mut Query) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_set_expr(query.body.as_mut(), true)
}

fn rewrite_supported_public_read_surfaces_in_set_expr(
    expr: &mut SetExpr,
    top_level: bool,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => {
            rewrite_supported_public_read_surfaces_in_select(select, top_level)
        }
        SetExpr::Query(query) => {
            rewrite_supported_public_read_surfaces_in_set_expr(query.body.as_mut(), false)
        }
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_supported_public_read_surfaces_in_set_expr(left.as_mut(), false)?;
            rewrite_supported_public_read_surfaces_in_set_expr(right.as_mut(), false)
        }
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_select(
    select: &mut Select,
    top_level: bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_supported_public_read_surfaces_in_table_with_joins(table, top_level)?;
    }
    if let Some(selection) = &mut select.selection {
        rewrite_supported_public_read_surfaces_in_expr(selection)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_supported_public_read_surfaces_in_expr(expr)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_table_with_joins(
    table: &mut TableWithJoins,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_supported_public_read_surfaces_in_table_factor(&mut table.relation, top_level)?;
    for join in &mut table.joins {
        rewrite_supported_public_read_surfaces_in_table_factor(&mut join.relation, top_level)?;
    }
    Ok(())
}

fn rewrite_supported_public_read_surfaces_in_table_factor(
    relation: &mut TableFactor,
    top_level: bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(surface_name) = table_name_terminal(name) else {
                return Ok(());
            };
            let Some(derived_query) =
                build_supported_public_read_surface_query(surface_name, top_level)?
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
            rewrite_supported_public_read_surfaces_in_set_expr(subquery.body.as_mut(), false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_supported_public_read_surfaces_in_table_with_joins(table_with_joins, false),
        _ => Ok(()),
    }
}

fn rewrite_supported_public_read_surfaces_in_expr(expr: &mut Expr) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(left)?;
            rewrite_supported_public_read_surfaces_in_expr(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => rewrite_supported_public_read_surfaces_in_expr(expr),
        Expr::InList { expr, list, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr)?;
            for item in list {
                rewrite_supported_public_read_surfaces_in_expr(item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_supported_public_read_surfaces_in_expr(expr)?;
            rewrite_supported_public_read_surfaces_in_expr(low)?;
            rewrite_supported_public_read_surfaces_in_expr(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr)?;
            rewrite_supported_public_read_surfaces_in_expr(pattern)
        }
        Expr::Subquery(query) => rewrite_supported_public_read_surfaces_in_query(query),
        Expr::Exists { subquery, .. } => rewrite_supported_public_read_surfaces_in_query(subquery),
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_supported_public_read_surfaces_in_expr(expr)?;
            rewrite_supported_public_read_surfaces_in_query(subquery)
        }
        _ => Ok(()),
    }
}

fn build_supported_public_read_surface_query(
    surface_name: &str,
    _top_level: bool,
) -> Result<Option<Query>, LixError> {
    let registry = SurfaceRegistry::with_builtin_surfaces();
    if let Some(surface_binding) = registry.bind_relation_name(surface_name) {
        match surface_binding.descriptor.surface_family {
            SurfaceFamily::Entity => {
                return build_builtin_entity_surface_query(&surface_binding).map(Some);
            }
            SurfaceFamily::Filesystem => {
                return build_nested_filesystem_surface_query(surface_name);
            }
            SurfaceFamily::State | SurfaceFamily::Admin | SurfaceFamily::Change => {}
        }
    }

    match surface_name.to_ascii_lowercase().as_str() {
        "lix_state_history" => {
            parse_single_query(&build_state_history_source_sql(&[], true)).map(Some)
        }
        "lix_active_version" => {
            build_admin_source_query(CanonicalAdminKind::ActiveVersion).map(Some)
        }
        "lix_active_account" => {
            build_admin_source_query(CanonicalAdminKind::ActiveAccount).map(Some)
        }
        "lix_version" => build_admin_source_query(CanonicalAdminKind::Version).map(Some),
        "lix_stored_schema" => build_admin_source_query(CanonicalAdminKind::StoredSchema).map(Some),
        "lix_change" => build_change_source_query().map(Some),
        "lix_working_changes" => build_working_changes_source_query().map(Some),
        _ => Ok(None),
    }
}

fn build_builtin_entity_surface_query(surface_binding: &SurfaceBinding) -> Result<Query, LixError> {
    let Some(schema_key) = surface_binding.implicit_overrides.fixed_schema_key.clone() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 public-surface rewrite requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let Some(state_scan) = CanonicalStateScan::from_surface_binding(surface_binding.clone()) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 public-surface rewrite could not build canonical state scan for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };
    let request = EffectiveStateRequest {
        schema_set: BTreeSet::from([schema_key]),
        version_scope: state_scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: state_scan.include_tombstones,
        predicate_classes: Vec::new(),
        required_columns: surface_binding.exposed_columns.clone(),
    };
    build_entity_source_query(surface_binding, &request, &[])?.ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "sql2 public-surface rewrite could not lower entity surface '{}'",
            surface_binding.descriptor.public_name
        ),
    })
}

fn rewrite_nested_filesystem_surfaces_in_set_expr(
    expr: &mut SetExpr,
    top_level: bool,
) -> Result<(), LixError> {
    match expr {
        SetExpr::Select(select) => rewrite_nested_filesystem_surfaces_in_select(select, top_level),
        SetExpr::Query(query) => rewrite_nested_filesystem_surfaces_in_query(query, false),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_set_expr(left.as_mut(), top_level)?;
            rewrite_nested_filesystem_surfaces_in_set_expr(right.as_mut(), top_level)
        }
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_select(
    select: &mut Select,
    top_level: bool,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_nested_filesystem_surfaces_in_table_with_joins(table, top_level)?;
    }
    if let Some(selection) = &mut select.selection {
        rewrite_nested_filesystem_surfaces_in_expr(selection)?;
    }
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                rewrite_nested_filesystem_surfaces_in_expr(expr)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_with_joins(
    table: &mut TableWithJoins,
    top_level: bool,
) -> Result<(), LixError> {
    rewrite_nested_filesystem_surfaces_in_table_factor(&mut table.relation, top_level)?;
    for join in &mut table.joins {
        rewrite_nested_filesystem_surfaces_in_table_factor(&mut join.relation, top_level)?;
    }
    Ok(())
}

fn rewrite_nested_filesystem_surfaces_in_table_factor(
    relation: &mut TableFactor,
    top_level: bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(surface_name) = table_name_terminal(name) else {
                return Ok(());
            };
            if top_level || !is_filesystem_public_surface_name(surface_name) {
                return Ok(());
            }
            let Some(derived_query) = build_nested_filesystem_surface_query(surface_name)? else {
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
            rewrite_nested_filesystem_surfaces_in_query(subquery, false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_nested_filesystem_surfaces_in_table_with_joins(table_with_joins, false),
        _ => Ok(()),
    }
}

fn rewrite_nested_filesystem_surfaces_in_expr(expr: &mut Expr) -> Result<(), LixError> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(left)?;
            rewrite_nested_filesystem_surfaces_in_expr(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => rewrite_nested_filesystem_surfaces_in_expr(expr),
        Expr::InList { expr, list, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(expr)?;
            for item in list {
                rewrite_nested_filesystem_surfaces_in_expr(item)?;
            }
            Ok(())
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            rewrite_nested_filesystem_surfaces_in_expr(expr)?;
            rewrite_nested_filesystem_surfaces_in_expr(low)?;
            rewrite_nested_filesystem_surfaces_in_expr(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(expr)?;
            rewrite_nested_filesystem_surfaces_in_expr(pattern)
        }
        Expr::Subquery(query) => rewrite_nested_filesystem_surfaces_in_query(query, false),
        Expr::Exists { subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_query(subquery, false)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            rewrite_nested_filesystem_surfaces_in_expr(expr)?;
            rewrite_nested_filesystem_surfaces_in_query(subquery, false)
        }
        _ => Ok(()),
    }
}

fn build_nested_filesystem_surface_query(surface_name: &str) -> Result<Option<Query>, LixError> {
    let normalized = surface_name.to_ascii_lowercase();
    let query = match normalized.as_str() {
        "lix_file" => parse_single_query(&build_filesystem_file_projection_sql(
            FilesystemProjectionScope::ActiveVersion,
            false,
        ))?,
        "lix_file_by_version" => parse_single_query(&build_filesystem_file_projection_sql(
            FilesystemProjectionScope::ExplicitVersion,
            false,
        ))?,
        "lix_directory" => parse_single_query(&build_filesystem_directory_projection_sql(
            FilesystemProjectionScope::ActiveVersion,
        ))?,
        "lix_directory_by_version" => parse_single_query(
            &build_filesystem_directory_projection_sql(FilesystemProjectionScope::ExplicitVersion),
        )?,
        "lix_file_history" => {
            let state_history_source_sql = build_filesystem_history_source_sql(&[], true);
            parse_single_query(&build_filesystem_file_history_projection_sql(
                &state_history_source_sql,
            ))?
        }
        "lix_file_history_by_version" => {
            let state_history_source_sql = build_filesystem_history_source_sql(&[], false);
            parse_single_query(&build_filesystem_file_history_projection_sql(
                &state_history_source_sql,
            ))?
        }
        "lix_directory_history" => {
            let state_history_source_sql = build_filesystem_history_source_sql(&[], true);
            parse_single_query(&build_filesystem_directory_history_projection_sql(
                &state_history_source_sql,
            ))?
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
            !top_level && table_name_terminal(name).is_some_and(is_filesystem_public_surface_name)
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

fn is_filesystem_public_surface_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "lix_file"
            | "lix_file_by_version"
            | "lix_file_history"
            | "lix_file_history_by_version"
            | "lix_directory"
            | "lix_directory_by_version"
            | "lix_directory_history"
    )
}

fn lower_admin_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    let Some(admin_scan) = canonical_admin_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };

    let derived_query = build_admin_source_query(admin_scan.kind)?;
    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn lower_filesystem_read_for_execution(
    canonicalized: &CanonicalizedRead,
) -> Result<Option<Statement>, LixError> {
    let Some(filesystem_scan) = canonical_filesystem_scan(&canonicalized.read_command.root) else {
        return Ok(None);
    };

    let Statement::Query(mut query) = canonicalized.bound_statement.statement.clone() else {
        return Ok(None);
    };
    rewrite_nested_filesystem_surfaces_in_query(query.as_mut(), true)?;
    let select = select_mut(query.as_mut())?;
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        return Ok(None);
    };
    let relation_name = alias
        .as_ref()
        .map(|value| value.name.value.clone())
        .unwrap_or_else(|| canonicalized.surface_binding.descriptor.public_name.clone());
    let history_pushdown_predicates = collect_filesystem_history_pushdown_predicates(
        select.selection.as_ref(),
        &relation_name,
        allow_unqualified,
    );

    let derived_query = match (filesystem_scan.kind, filesystem_scan.version_scope) {
        (FilesystemKind::File, VersionScope::ActiveVersion) => parse_single_query(
            &build_filesystem_file_projection_sql(FilesystemProjectionScope::ActiveVersion, false),
        )?,
        (FilesystemKind::File, VersionScope::ExplicitVersion)
            if canonicalized.surface_binding.descriptor.public_name == "lix_file_by_version" =>
        {
            parse_single_query(&build_filesystem_file_projection_sql(
                FilesystemProjectionScope::ExplicitVersion,
                false,
            ))?
        }
        (FilesystemKind::Directory, VersionScope::ActiveVersion) => parse_single_query(
            &build_filesystem_directory_projection_sql(FilesystemProjectionScope::ActiveVersion),
        )?,
        (FilesystemKind::Directory, VersionScope::ExplicitVersion)
            if canonicalized.surface_binding.descriptor.public_name
                == "lix_directory_by_version" =>
        {
            parse_single_query(&build_filesystem_directory_projection_sql(
                FilesystemProjectionScope::ExplicitVersion,
            ))?
        }
        (FilesystemKind::File, VersionScope::History)
            if canonicalized.surface_binding.descriptor.public_name == "lix_file_history" =>
        {
            let state_history_source_sql =
                build_filesystem_history_source_sql(&history_pushdown_predicates, true);
            parse_single_query(&build_filesystem_file_history_projection_sql(
                &state_history_source_sql,
            ))?
        }
        (FilesystemKind::File, VersionScope::History)
            if canonicalized.surface_binding.descriptor.public_name
                == "lix_file_history_by_version" =>
        {
            let state_history_source_sql =
                build_filesystem_history_source_sql(&history_pushdown_predicates, false);
            parse_single_query(&build_filesystem_file_history_projection_sql(
                &state_history_source_sql,
            ))?
        }
        (FilesystemKind::Directory, VersionScope::History)
            if canonicalized.surface_binding.descriptor.public_name == "lix_directory_history" =>
        {
            let state_history_source_sql =
                build_filesystem_history_source_sql(&history_pushdown_predicates, true);
            parse_single_query(&build_filesystem_directory_history_projection_sql(
                &state_history_source_sql,
            ))?
        }
        _ => return Ok(None),
    };

    let derived_alias = alias.clone().or_else(|| {
        Some(TableAlias {
            explicit: false,
            name: Ident::new(&canonicalized.surface_binding.descriptor.public_name),
            columns: Vec::new(),
        })
    });
    select.from[0].relation = TableFactor::Derived {
        lateral: false,
        subquery: Box::new(derived_query),
        alias: derived_alias,
    };

    Ok(Some(Statement::Query(query)))
}

fn build_state_source_query(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[String],
) -> Result<Option<Query>, LixError> {
    let sql = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default | SurfaceVariant::ByVersion => build_effective_state_source_sql(
            effective_state_request,
            surface_binding,
            pushdown_predicates,
        )?,
        SurfaceVariant::History => build_state_history_source_sql(pushdown_predicates, true),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => return Ok(None),
    };
    parse_single_query(&sql).map(Some)
}

fn build_admin_source_query(kind: CanonicalAdminKind) -> Result<Query, LixError> {
    let sql = match kind {
        CanonicalAdminKind::ActiveVersion => format!(
            "SELECT \
                entity_id AS id, \
                lix_json_extract(snapshot_content, 'version_id') AS version_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{storage_version_id}' \
               AND snapshot_content IS NOT NULL",
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
        ),
        CanonicalAdminKind::ActiveAccount => format!(
            "SELECT \
                lix_json_extract(snapshot_content, 'account_id') AS id, \
                lix_json_extract(snapshot_content, 'account_id') AS account_id \
             FROM lix_internal_state_untracked \
             WHERE schema_key = '{schema_key}' \
               AND file_id = '{file_id}' \
               AND version_id = '{storage_version_id}' \
               AND snapshot_content IS NOT NULL",
            schema_key = escape_sql_string(active_account_schema_key()),
            file_id = escape_sql_string(active_account_file_id()),
            storage_version_id = escape_sql_string(active_account_storage_version_id()),
        ),
        CanonicalAdminKind::StoredSchema => "SELECT \
                lix_json_extract(snapshot_content, 'value') AS value, \
                lix_json_extract(snapshot_content, 'value.x-lix-key') AS lixcol_schema_key, \
                lix_json_extract(snapshot_content, 'value.x-lix-version') AS lixcol_schema_version \
             FROM lix_internal_stored_schema_bootstrap \
             WHERE snapshot_content IS NOT NULL"
            .to_string(),
        CanonicalAdminKind::Version => format!(
            "SELECT \
                d.entity_id AS id, \
                lix_json_extract(d.snapshot_content, 'name') AS name, \
                COALESCE(lix_json_extract(d.snapshot_content, 'hidden'), 'false') AS hidden, \
                lix_json_extract(t.snapshot_content, 'commit_id') AS commit_id \
             FROM lix_internal_state_materialized_v1_lix_version_descriptor d \
             LEFT JOIN ( \
               SELECT entity_id, snapshot_content \
               FROM ( \
                 SELECT \
                   entity_id, \
                   snapshot_content, \
                   ROW_NUMBER() OVER ( \
                     PARTITION BY entity_id \
                     ORDER BY updated_at DESC, \
                              created_at DESC, \
                              change_id DESC \
                   ) AS rn \
                 FROM lix_internal_state_materialized_v1_lix_version_pointer \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND is_tombstone = 0 \
                   AND snapshot_content IS NOT NULL \
               ) ranked_version_pointers \
               WHERE rn = 1 \
             ) t \
               ON t.entity_id = d.entity_id \
             WHERE d.schema_key = '{descriptor_schema_key}' \
               AND d.version_id = '{global_version}' \
               AND d.is_tombstone = 0 \
               AND d.snapshot_content IS NOT NULL",
            descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        ),
    };

    parse_single_query(&sql)
}

fn build_entity_source_query(
    surface_binding: &SurfaceBinding,
    effective_state_request: &EffectiveStateRequest,
    pushdown_predicates: &[String],
) -> Result<Option<Query>, LixError> {
    let Some(schema_key) = surface_binding
        .implicit_overrides
        .fixed_schema_key
        .as_deref()
    else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 entity read lowerer requires fixed schema binding for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    };

    let projection = entity_projection_sql(surface_binding, effective_state_request);
    let projection = if projection.is_empty() {
        "entity_id AS lixcol_entity_id".to_string()
    } else {
        projection.join(", ")
    };

    let Some(state_source_query) = build_state_source_query(
        surface_binding,
        effective_state_request,
        pushdown_predicates,
    )?
    else {
        return Ok(None);
    };
    let mut predicates = Vec::new();
    if !matches!(
        surface_binding.descriptor.surface_variant,
        SurfaceVariant::Default | SurfaceVariant::ByVersion | SurfaceVariant::History
    ) {
        predicates.push(format!(
            "{} = '{}'",
            render_identifier("schema_key"),
            escape_sql_string(schema_key)
        ));
    }
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
    effective_state_request: &EffectiveStateRequest,
    surface_binding: &SurfaceBinding,
    pushdown_predicates: &[String],
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
                "sql2 state read lowerer requires a bounded schema set for '{}'",
                surface_binding.descriptor.public_name
            ),
        });
    }

    let (target_version_predicates, source_predicates) =
        split_effective_state_pushdown_predicates(pushdown_predicates);
    let target_versions_cte = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => active_target_versions_cte_sql(),
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
    let candidate_rows_sql = effective_state_candidate_rows_sql(&schema_keys, &source_predicates);
    Ok(format!(
        "WITH \
           {target_versions_cte}, \
           commit_by_version AS ( \
             SELECT \
               entity_id AS commit_id, \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_extract(snapshot_content, 'change_id') AS change_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
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
           ), \
           candidates AS ( \
             {candidate_rows_sql} \
           ), \
           ranked AS ( \
             SELECT \
               c.entity_id AS entity_id, \
               c.schema_key AS schema_key, \
               c.file_id AS file_id, \
               c.version_id AS version_id, \
               c.plugin_key AS plugin_key, \
               c.snapshot_content AS snapshot_content, \
               c.schema_version AS schema_version, \
               c.created_at AS created_at, \
               c.updated_at AS updated_at, \
               c.global AS global, \
               c.change_id AS change_id, \
               c.commit_id AS commit_id, \
               c.untracked AS untracked, \
               c.writer_key AS writer_key, \
               c.metadata AS metadata, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY c.version_id, c.entity_id, c.schema_key, c.file_id \
                 ORDER BY \
                   c.precedence ASC, \
                   c.updated_at DESC, \
                   c.created_at DESC, \
                   COALESCE(c.change_id, '') DESC \
               ) AS rn \
             FROM candidates c \
           ) \
         SELECT \
           ranked.entity_id AS entity_id, \
           ranked.schema_key AS schema_key, \
           ranked.file_id AS file_id, \
           ranked.version_id AS version_id, \
           ranked.plugin_key AS plugin_key, \
           ranked.snapshot_content AS snapshot_content, \
           ranked.schema_version AS schema_version, \
           ranked.created_at AS created_at, \
           ranked.updated_at AS updated_at, \
           ranked.global AS global, \
           ranked.change_id AS change_id, \
           ranked.commit_id AS commit_id, \
           ranked.untracked AS untracked, \
           ranked.writer_key AS writer_key, \
           ranked.metadata AS metadata \
         FROM ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL",
        target_versions_cte = target_versions_cte,
        candidate_rows_sql = candidate_rows_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
    ))
}

fn active_target_versions_cte_sql() -> String {
    format!(
        "target_versions AS ( \
           SELECT DISTINCT \
             lix_json_extract(snapshot_content, 'version_id') AS version_id \
           FROM lix_internal_state_untracked \
           WHERE schema_key = '{schema_key}' \
             AND file_id = '{file_id}' \
             AND version_id = '{storage_version_id}' \
             AND snapshot_content IS NOT NULL \
         )",
        schema_key = escape_sql_string(active_version_schema_key()),
        file_id = escape_sql_string(active_version_file_id()),
        storage_version_id = escape_sql_string(active_version_storage_version_id()),
    )
}

fn explicit_target_versions_cte_sql(
    schema_keys: &[String],
    target_version_predicates: &[String],
) -> String {
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
        "snapshot_content IS NOT NULL".to_string(),
        "COALESCE(lix_json_extract(snapshot_content, 'hidden'), 'false') != 'true'".to_string(),
    ];
    let schema_local_rows = schema_keys
        .iter()
        .map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM {table_name} \
                 WHERE version_id <> '{global_version}'",
                table_name =
                    quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}")),
                global_version = escape_sql_string(GLOBAL_VERSION_ID),
            )
        })
        .chain(schema_keys.iter().map(|schema_key| {
            format!(
                "SELECT DISTINCT version_id \
                 FROM lix_internal_state_untracked \
                 WHERE schema_key = '{schema_key}' \
                   AND version_id <> '{global_version}'",
                schema_key = escape_sql_string(schema_key),
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
           FROM lix_internal_state_materialized_v1_lix_version_descriptor \
           WHERE {version_descriptor_predicates}\
           {all_target_versions} \
         ), \
         target_versions AS ( \
           SELECT version_id \
           FROM all_target_versions \
           {target_versions_where} \
         )",
        version_descriptor_predicates = version_descriptor_predicates.join(" AND "),
        all_target_versions = all_target_versions,
        target_versions_where = target_versions_where,
    )
}

fn effective_state_candidate_rows_sql(
    schema_keys: &[String],
    source_predicates: &[String],
) -> String {
    let tracked_predicates = render_where_clause_sql(source_predicates, " AND ");
    let untracked_predicates = render_where_clause_sql(source_predicates, " AND ");
    schema_keys
        .iter()
        .flat_map(|schema_key| {
            let table_name =
                quote_ident(&format!("lix_internal_state_materialized_v1_{schema_key}"));
            let schema_filter = format!("schema_key = '{}'", escape_sql_string(schema_key));
            [
                format!(
                    "SELECT \
                       t.entity_id AS entity_id, \
                       t.schema_key AS schema_key, \
                       t.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       t.plugin_key AS plugin_key, \
                       t.snapshot_content AS snapshot_content, \
                       t.schema_version AS schema_version, \
                       t.created_at AS created_at, \
                       t.updated_at AS updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS global, \
                       t.change_id AS change_id, \
                       cc.commit_id AS commit_id, \
                       false AS untracked, \
                       t.writer_key AS writer_key, \
                       t.metadata AS metadata, \
                       2 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id = t.version_id \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE 1 = 1{tracked_predicates}",
                    table_name = table_name,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    tracked_predicates = tracked_predicates,
                ),
                format!(
                    "SELECT \
                       t.entity_id AS entity_id, \
                       t.schema_key AS schema_key, \
                       t.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       t.plugin_key AS plugin_key, \
                       t.snapshot_content AS snapshot_content, \
                       t.schema_version AS schema_version, \
                       t.created_at AS created_at, \
                       t.updated_at AS updated_at, \
                       true AS global, \
                       t.change_id AS change_id, \
                       cc.commit_id AS commit_id, \
                       false AS untracked, \
                       t.writer_key AS writer_key, \
                       t.metadata AS metadata, \
                       4 AS precedence \
                     FROM {table_name} t \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND t.version_id = '{global_version}' \
                     LEFT JOIN change_commit_by_change_id cc \
                       ON cc.change_id = t.change_id \
                     WHERE t.version_id = '{global_version}'{tracked_predicates}",
                    table_name = table_name,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    tracked_predicates = tracked_predicates,
                ),
                format!(
                    "SELECT \
                       u.entity_id AS entity_id, \
                       u.schema_key AS schema_key, \
                       u.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       u.plugin_key AS plugin_key, \
                       u.snapshot_content AS snapshot_content, \
                       u.schema_version AS schema_version, \
                       u.created_at AS created_at, \
                       u.updated_at AS updated_at, \
                       CASE WHEN tv.version_id = '{global_version}' THEN true ELSE false END AS global, \
                       NULL AS change_id, \
                       'untracked' AS commit_id, \
                       true AS untracked, \
                       u.writer_key AS writer_key, \
                       u.metadata AS metadata, \
                       1 AS precedence \
                     FROM lix_internal_state_untracked u \
                     JOIN target_versions tv \
                       ON tv.version_id = u.version_id \
                     WHERE {schema_filter}{untracked_predicates}",
                    schema_filter = schema_filter,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    untracked_predicates = untracked_predicates,
                ),
                format!(
                    "SELECT \
                       u.entity_id AS entity_id, \
                       u.schema_key AS schema_key, \
                       u.file_id AS file_id, \
                       tv.version_id AS version_id, \
                       u.plugin_key AS plugin_key, \
                       u.snapshot_content AS snapshot_content, \
                       u.schema_version AS schema_version, \
                       u.created_at AS created_at, \
                       u.updated_at AS updated_at, \
                       true AS global, \
                       NULL AS change_id, \
                       'untracked' AS commit_id, \
                       true AS untracked, \
                       u.writer_key AS writer_key, \
                       u.metadata AS metadata, \
                       3 AS precedence \
                     FROM lix_internal_state_untracked u \
                     JOIN target_versions tv \
                       ON tv.version_id <> '{global_version}' \
                      AND u.version_id = '{global_version}' \
                     WHERE {schema_filter} \
                       AND u.version_id = '{global_version}'{untracked_predicates}",
                    schema_filter = schema_filter,
                    global_version = escape_sql_string(GLOBAL_VERSION_ID),
                    untracked_predicates = untracked_predicates,
                ),
            ]
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn build_state_history_source_sql(
    pushdown_predicates: &[String],
    force_active_scope: bool,
) -> String {
    let requested_root_predicates = state_history_requested_root_predicates(pushdown_predicates);
    let requested_version_predicates =
        state_history_requested_version_predicates(pushdown_predicates);
    let mut requested_predicates = Vec::new();
    requested_predicates.extend(requested_root_predicates.clone());
    requested_predicates.extend(requested_version_predicates);
    if force_active_scope && requested_root_predicates.is_empty() {
        requested_predicates
            .push("c.id IN (SELECT root_commit_id FROM default_root_commits)".to_string());
    }
    let requested_where_sql = render_where_clause_sql(&requested_predicates, "WHERE ");

    let active_version_rows_sql = if force_active_scope {
        format!(
            "active_version_rows AS ( \
               SELECT DISTINCT \
                 lix_json_extract(snapshot_content, 'version_id') AS version_id \
               FROM lix_internal_state_untracked \
               WHERE schema_key = '{schema_key}' \
                 AND file_id = '{file_id}' \
                 AND version_id = '{storage_version_id}' \
                 AND snapshot_content IS NOT NULL \
             ), ",
            schema_key = escape_sql_string(active_version_schema_key()),
            file_id = escape_sql_string(active_version_file_id()),
            storage_version_id = escape_sql_string(active_version_storage_version_id()),
        )
    } else {
        String::new()
    };
    let default_root_commits_sql = if force_active_scope {
        format!(
            "default_root_commits AS ( \
               SELECT DISTINCT \
                 lix_json_extract(vp.snapshot_content, 'commit_id') AS root_commit_id, \
                 vp.version_id AS root_version_id \
               FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
               JOIN active_version_rows av \
                 ON av.version_id = vp.entity_id \
               WHERE vp.schema_key = '{schema_key}' \
                 AND vp.file_id = '{file_id}' \
                 AND vp.version_id = '{storage_version_id}' \
                 AND vp.is_tombstone = 0 \
                 AND vp.snapshot_content IS NOT NULL \
             ), ",
            schema_key = escape_sql_string(version_pointer_schema_key()),
            file_id = escape_sql_string(version_pointer_file_id()),
            storage_version_id = escape_sql_string(version_pointer_storage_version_id()),
        )
    } else {
        format!(
            "default_root_commits AS ( \
               SELECT DISTINCT \
                 lix_json_extract(vp.snapshot_content, 'commit_id') AS root_commit_id, \
                 vp.entity_id AS root_version_id \
               FROM lix_internal_state_materialized_v1_lix_version_pointer vp \
               WHERE vp.schema_key = '{schema_key}' \
                 AND vp.file_id = '{file_id}' \
                 AND vp.version_id = '{storage_version_id}' \
                 AND vp.is_tombstone = 0 \
                 AND vp.snapshot_content IS NOT NULL \
             ), ",
            schema_key = escape_sql_string(version_pointer_schema_key()),
            file_id = escape_sql_string(version_pointer_file_id()),
            storage_version_id = escape_sql_string(version_pointer_storage_version_id()),
        )
    };

    format!(
        "WITH \
           {active_version_rows_sql}\
           {default_root_commits_sql}\
           commit_by_version AS ( \
             SELECT \
               entity_id AS id, \
               lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
               created_at AS created_at, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           requested_commits AS ( \
             SELECT DISTINCT \
               c.id AS commit_id, \
               COALESCE(d.root_version_id, c.lixcol_version_id) AS root_version_id \
             FROM commit_by_version c \
             LEFT JOIN default_root_commits d \
               ON d.root_commit_id = c.id \
             {requested_where_sql} \
           ), \
           reachable_commits AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               requested.commit_id AS root_commit_id, \
               requested.root_version_id AS root_version_id, \
               ancestry.depth AS commit_depth \
             FROM requested_commits requested \
             JOIN lix_internal_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= 512 \
           ), \
           filtered_reachable_commits AS ( \
             SELECT \
               rc.commit_id, \
               rc.root_commit_id, \
               rc.root_version_id, \
               rc.commit_depth, \
               c.created_at AS commit_created_at \
             FROM reachable_commits rc \
             JOIN commit_by_version c \
               ON c.id = rc.commit_id \
           ), \
           breakpoint_rows AS ( \
             SELECT \
               bp.root_commit_id, \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               bp.from_depth \
             FROM lix_internal_entity_state_timeline_breakpoint bp \
             JOIN requested_commits requested \
               ON requested.commit_id = bp.root_commit_id \
           ), \
           history_rows AS ( \
             SELECT \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               rc.commit_id AS commit_id, \
               rc.commit_created_at AS commit_created_at, \
               rc.root_commit_id AS root_commit_id, \
               rc.root_version_id AS version_id, \
               rc.commit_depth AS depth \
             FROM filtered_reachable_commits rc \
             JOIN breakpoint_rows bp \
               ON bp.root_commit_id = rc.root_commit_id \
              AND rc.commit_depth = bp.from_depth \
           ) \
         SELECT \
           h.entity_id AS entity_id, \
           h.schema_key AS schema_key, \
           h.file_id AS file_id, \
           h.plugin_key AS plugin_key, \
           s.content AS snapshot_content, \
           h.metadata AS metadata, \
           h.schema_version AS schema_version, \
           h.change_id AS change_id, \
           h.commit_id AS commit_id, \
           h.commit_created_at AS commit_created_at, \
           h.root_commit_id AS root_commit_id, \
           h.depth AS depth, \
           h.version_id AS version_id \
         FROM history_rows h \
         LEFT JOIN lix_internal_snapshot s \
           ON s.id = h.snapshot_id \
         WHERE h.snapshot_id != 'no-content'",
        active_version_rows_sql = active_version_rows_sql,
        default_root_commits_sql = default_root_commits_sql,
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
        requested_where_sql = requested_where_sql,
    )
}

fn build_filesystem_history_source_sql(
    pushdown_predicates: &[String],
    force_active_scope: bool,
) -> String {
    let requested_root_predicates = history_requested_root_predicates(pushdown_predicates);
    let requested_version_predicates = history_requested_version_predicates(pushdown_predicates);
    let requested_roots_where = render_where_clause_sql(&requested_root_predicates, " AND ");
    let requested_versions_where = render_where_clause_sql(&requested_version_predicates, " AND ");
    let default_root_scope = if force_active_scope && requested_root_predicates.is_empty() {
        "AND ( \
           d.root_commit_id IS NOT NULL \
           OR c.entity_id IN (SELECT root_commit_id FROM default_root_commits) \
         )"
        .to_string()
    } else {
        String::new()
    };
    build_filesystem_state_history_source_sql(
        &requested_roots_where,
        &requested_versions_where,
        &default_root_scope,
        force_active_scope,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesystemHistoryFilterColumn {
    RootCommitId,
    VersionId,
}

fn collect_filesystem_history_pushdown_predicates(
    selection: Option<&Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> Vec<String> {
    let Some(selection) = selection else {
        return Vec::new();
    };
    let mut predicates = Vec::new();
    collect_filesystem_history_pushdown_predicates_from_expr(
        selection,
        relation_name,
        allow_unqualified,
        &mut predicates,
    );
    predicates
}

fn collect_filesystem_history_pushdown_predicates_from_expr(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
    predicates: &mut Vec<String>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_filesystem_history_pushdown_predicates_from_expr(
                left,
                relation_name,
                allow_unqualified,
                predicates,
            );
            collect_filesystem_history_pushdown_predicates_from_expr(
                right,
                relation_name,
                allow_unqualified,
                predicates,
            );
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(left, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} {} {}",
                    filesystem_history_filter_column_name(column),
                    op,
                    right
                ));
            } else if let Some(column) =
                extract_filesystem_history_filter_column(right, relation_name, allow_unqualified)
            {
                if let Some(inverted) = invert_filesystem_history_binary_operator(op.clone()) {
                    predicates.push(format!(
                        "{} {} {}",
                        filesystem_history_filter_column_name(column),
                        inverted,
                        left
                    ));
                }
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(expr, relation_name, allow_unqualified)
            {
                let not_sql = if *negated { " NOT" } else { "" };
                predicates.push(format!(
                    "{}{} IN ({})",
                    filesystem_history_filter_column_name(column),
                    not_sql,
                    subquery
                ));
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(column) =
                extract_filesystem_history_filter_column(expr, relation_name, allow_unqualified)
            {
                let not_sql = if *negated { " NOT" } else { "" };
                predicates.push(format!(
                    "{}{} IN ({})",
                    filesystem_history_filter_column_name(column),
                    not_sql,
                    list.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        Expr::IsNull(inner) => {
            if let Some(column) =
                extract_filesystem_history_filter_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} IS NULL",
                    filesystem_history_filter_column_name(column)
                ));
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(column) =
                extract_filesystem_history_filter_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(format!(
                    "{} IS NOT NULL",
                    filesystem_history_filter_column_name(column)
                ));
            }
        }
        Expr::Nested(inner) => collect_filesystem_history_pushdown_predicates_from_expr(
            inner,
            relation_name,
            allow_unqualified,
            predicates,
        ),
        _ => {}
    }
}

fn extract_filesystem_history_filter_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<FilesystemHistoryFilterColumn> {
    let column = match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            parts[1].value.as_str()
        }
        Expr::Identifier(identifier) if allow_unqualified => identifier.value.as_str(),
        Expr::Nested(inner) => {
            return extract_filesystem_history_filter_column(
                inner,
                relation_name,
                allow_unqualified,
            )
        }
        _ => return None,
    };

    match column.to_ascii_lowercase().as_str() {
        "lixcol_root_commit_id" | "root_commit_id" => {
            Some(FilesystemHistoryFilterColumn::RootCommitId)
        }
        "lixcol_version_id" | "version_id" => Some(FilesystemHistoryFilterColumn::VersionId),
        _ => None,
    }
}

fn filesystem_history_filter_column_name(column: FilesystemHistoryFilterColumn) -> &'static str {
    match column {
        FilesystemHistoryFilterColumn::RootCommitId => "root_commit_id",
        FilesystemHistoryFilterColumn::VersionId => "version_id",
    }
}

fn invert_filesystem_history_binary_operator(op: BinaryOperator) -> Option<BinaryOperator> {
    match op {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::NotEq => Some(BinaryOperator::NotEq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        _ => None,
    }
}

fn history_requested_root_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_root_commit_id") {
                Some(predicate.replace("lixcol_root_commit_id", "c.entity_id"))
            } else if predicate.contains("root_commit_id") {
                Some(predicate.replace("root_commit_id", "c.entity_id"))
            } else {
                None
            }
        })
        .collect()
}

fn state_history_requested_root_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_root_commit_id") {
                Some(predicate.replace("lixcol_root_commit_id", "c.id"))
            } else if predicate.contains("root_commit_id") {
                Some(predicate.replace("root_commit_id", "c.id"))
            } else {
                None
            }
        })
        .collect()
}

fn state_history_requested_version_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_version_id") {
                Some(predicate.replace("lixcol_version_id", "d.root_version_id"))
            } else if predicate.contains("version_id") {
                Some(predicate.replace("version_id", "d.root_version_id"))
            } else {
                None
            }
        })
        .collect()
}

fn history_requested_version_predicates(pushdown_predicates: &[String]) -> Vec<String> {
    pushdown_predicates
        .iter()
        .filter_map(|predicate| {
            if predicate.contains("lixcol_version_id") {
                Some(predicate.replace("lixcol_version_id", "d.root_version_id"))
            } else if predicate.contains("version_id") {
                Some(predicate.replace("version_id", "d.root_version_id"))
            } else {
                None
            }
        })
        .collect()
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

fn build_working_changes_source_query() -> Result<Query, LixError> {
    parse_single_query(
        "WITH \
            active_version AS ( \
                SELECT lix_json_extract(snapshot_content, 'version_id') AS version_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_active_version' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
                ORDER BY updated_at DESC \
                LIMIT 1 \
            ), \
            scope_heads AS ( \
                SELECT \
                    'local' AS scope, \
                    (SELECT version_id FROM active_version) AS checkpoint_version_id, \
                    ( \
                        SELECT lix_json_extract(snapshot_content, 'commit_id') \
                        FROM lix_internal_state_materialized_v1_lix_version_pointer \
                        WHERE schema_key = 'lix_version_pointer' \
                          AND entity_id = (SELECT version_id FROM active_version) \
                          AND file_id = 'lix' \
                          AND version_id = 'global' \
                          AND is_tombstone = 0 \
                          AND snapshot_content IS NOT NULL \
                        LIMIT 1 \
                    ) AS tip_commit_id \
                UNION ALL \
                SELECT \
                    'global' AS scope, \
                    'global' AS checkpoint_version_id, \
                    ( \
                        SELECT lix_json_extract(snapshot_content, 'commit_id') \
                        FROM lix_internal_state_materialized_v1_lix_version_pointer \
                        WHERE schema_key = 'lix_version_pointer' \
                          AND entity_id = 'global' \
                          AND file_id = 'lix' \
                          AND version_id = 'global' \
                          AND is_tombstone = 0 \
                          AND snapshot_content IS NOT NULL \
                        LIMIT 1 \
                    ) AS tip_commit_id \
            ), \
            scope_baselines AS ( \
                SELECT \
                    scope, \
                    tip_commit_id, \
                    COALESCE( \
                        ( \
                            SELECT checkpoint_commit_id \
                            FROM lix_internal_last_checkpoint \
                            WHERE version_id = checkpoint_version_id \
                            LIMIT 1 \
                        ), \
                        tip_commit_id \
                    ) AS baseline_commit_id \
                FROM scope_heads \
            ), \
            commit_rows AS ( \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    created_at \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_commit' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
                UNION \
                SELECT \
                    entity_id AS id, \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    created_at \
                FROM lix_internal_state_materialized_v1_lix_commit \
                WHERE schema_key = 'lix_commit' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
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
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    lix_json_extract(snapshot_content, 'change_id') AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_untracked \
                WHERE schema_key = 'lix_change_set_element' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND snapshot_content IS NOT NULL \
                UNION \
                SELECT \
                    lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                    lix_json_extract(snapshot_content, 'change_id') AS change_id, \
                    lix_json_extract(snapshot_content, 'entity_id') AS entity_id, \
                    lix_json_extract(snapshot_content, 'schema_key') AS schema_key, \
                    lix_json_extract(snapshot_content, 'file_id') AS file_id \
                FROM lix_internal_state_materialized_v1_lix_change_set_element \
                WHERE schema_key = 'lix_change_set_element' \
                  AND file_id = 'lix' \
                  AND version_id = 'global' \
                  AND is_tombstone = 0 \
                  AND snapshot_content IS NOT NULL \
            ), \
            tip_ancestry AS ( \
                SELECT \
                    scope.scope AS scope, \
                    anc.ancestor_id AS commit_id, \
                    anc.depth AS depth \
                FROM scope_baselines scope \
                JOIN lix_internal_commit_ancestry anc \
                    ON anc.commit_id = scope.tip_commit_id \
            ), \
            baseline_ancestry AS ( \
                SELECT \
                    scope.scope AS scope, \
                    anc.ancestor_id AS commit_id, \
                    anc.depth AS depth \
                FROM scope_baselines scope \
                JOIN lix_internal_commit_ancestry anc \
                    ON anc.commit_id = scope.baseline_commit_id \
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
                            SELECT tip_commit_id \
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
    )
}

fn canonical_admin_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql2::planner::ir::CanonicalAdminScan> {
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
) -> Option<&crate::sql2::planner::ir::CanonicalWorkingChangesScan> {
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
) -> Option<&crate::sql2::planner::ir::CanonicalFilesystemScan> {
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
        let path = escape_sql_string(column);
        return Some(format!(
            "lix_json_extract(snapshot_content, '{path}') AS {alias}"
        ));
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
        "lixcol_version_id" if variant != SurfaceVariant::Default => Some("version_id"),
        "lixcol_commit_id" if variant == SurfaceVariant::History => Some("commit_id"),
        "lixcol_root_commit_id" if variant == SurfaceVariant::History => Some("root_commit_id"),
        "lixcol_depth" if variant == SurfaceVariant::History => Some("depth"),
        _ => None,
    }
}

fn entity_source_predicates(
    surface_binding: &SurfaceBinding,
    schema_key: &str,
) -> (String, Vec<String>) {
    let mut predicates = vec![format!(
        "{} = '{}'",
        render_identifier("schema_key"),
        escape_sql_string(schema_key)
    )];

    let source_table = match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => {
            if let Some(version_id) = surface_binding
                .implicit_overrides
                .fixed_version_id
                .as_deref()
            {
                predicates.push(format!(
                    "{} = '{}'",
                    render_identifier("version_id"),
                    escape_sql_string(version_id)
                ));
                "lix_state_by_version".to_string()
            } else {
                "lix_state".to_string()
            }
        }
        SurfaceVariant::ByVersion => "lix_state_by_version".to_string(),
        SurfaceVariant::History => "lix_state_history".to_string(),
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => {
            surface_binding.descriptor.public_name.clone()
        }
    };

    (source_table, predicates)
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
        accepted_predicates: effective_state_plan.pushdown_safe_predicates.clone(),
        rejected_predicates: effective_state_plan
            .residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "day-1 sql2 read lowering keeps this predicate above effective-state resolution"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates: effective_state_plan.residual_predicates.clone(),
    }
}

fn change_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "sql2 change-scan lowering keeps change predicates above the derived change source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn working_changes_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: "sql2 working-changes lowering keeps predicates above the derived working-changes source".to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn admin_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "sql2 admin-scan lowering keeps admin predicates above the derived admin source"
                        .to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}

fn filesystem_pushdown_decision(canonicalized: &CanonicalizedRead) -> PushdownDecision {
    let residual_predicates = read_predicates_from_query(canonicalized);
    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason:
                    "sql2 filesystem lowering keeps filesystem predicates above the derived source"
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
    let accepted = effective_state_plan
        .pushdown_safe_predicates
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let Some(selection) = selection else {
        return (Vec::new(), None);
    };

    let mut pushdown = Vec::new();
    let mut residual = Vec::new();
    for predicate in split_conjunctive_predicates(selection) {
        if accepted.contains(&predicate.to_string()) {
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

fn read_predicates_from_query(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };
    let Some(select) = select_ref(query.as_ref()) else {
        return Vec::new();
    };
    let Some(selection) = &select.selection else {
        return Vec::new();
    };

    split_conjunctive_predicates(selection)
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
            description: "expected a single lowered sql2 read statement".to_string(),
        });
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected lowered sql2 read to parse as a query".to_string(),
        });
    };
    Ok(*query)
}

fn query_uses_wildcard_projection(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    let Some(select) = select_query(query.as_ref()) else {
        return false;
    };
    select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    })
}

fn select_query(query: &Query) -> Option<&Select> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select.as_ref())
}

fn select_ref(query: &Query) -> Option<&Select> {
    select_query(query)
}

fn select_mut(query: &mut Query) -> Result<&mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 live read lowering requires a SELECT query".to_string(),
        });
    };
    Ok(select.as_mut())
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
        lower_read_for_execution, rewrite_supported_public_read_surfaces_in_statement,
        LoweredReadProgram,
    };
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::planner::canonicalize::canonicalize_read;
    use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
    use crate::sql2::planner::semantics::effective_state_resolver::build_effective_state;
    use crate::{SqlDialect, Value};

    fn lowered_program(registry: &SurfaceRegistry, sql: &str) -> Option<LoweredReadProgram> {
        let mut statements =
            crate::sql2::core::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            Vec::<Value>::new(),
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        let canonicalized = canonicalize_read(bound, registry).expect("query should canonicalize");
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized);
        let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
        lower_read_for_execution(
            &canonicalized,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
        assert!(lowered_sql.contains("FROM lix_internal_state_untracked"));
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
        let mut statements = crate::sql2::core::parser::parse_sql_script(
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

        assert!(!lowered_sql.contains("FROM lix_entity_label"));
        assert!(!lowered_sql.contains("JOIN lix_label"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_entity_label"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_label"));
    }

    #[test]
    fn lowers_dynamic_entity_reads_with_scalar_override_predicates() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(crate::sql2::catalog::DynamicEntitySurfaceSpec {
            schema_key: "message".to_string(),
            visible_columns: vec!["body".to_string(), "id".to_string()],
            fixed_version_id: None,
            predicate_overrides: vec![
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "file_id".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String("inlang".to_string()),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "plugin_key".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::String(
                        "inlang_sdk".to_string(),
                    ),
                },
                crate::sql2::catalog::SurfaceOverridePredicate {
                    column: "global".to_string(),
                    value: crate::sql2::catalog::SurfaceOverrideValue::Boolean(true),
                },
            ],
        });

        let lowered = lowered_program(
            &registry,
            "SELECT body, lixcol_global FROM message WHERE id = 'm1'",
        )
        .expect("dynamic entity read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_message"));
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

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_key_value"));
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
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
        assert!(lowered_sql.contains("FROM (WITH active_version AS"));
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

        assert!(current_sql.contains("lix_internal_state_materialized_v1_lix_file_descriptor"));
        assert!(current_sql.contains("lix_internal_state_materialized_v1_lix_directory_descriptor"));
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

        assert!(
            by_version_sql.contains("lix_internal_state_materialized_v1_lix_directory_descriptor")
        );
        assert!(by_version_sql.contains("WITH RECURSIVE all_target_versions AS"));
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
    fn lowers_active_version_reads_through_internal_untracked_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT id, version_id FROM lix_active_version WHERE version_id = 'main'",
        )
        .expect("active version read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM lix_internal_state_untracked"));
        assert!(lowered_sql.contains("schema_key = 'lix_active_version'"));
        assert!(lowered_sql.contains("file_id = 'lix'"));
        assert!(lowered_sql.contains("version_id = 'global'"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["version_id = 'main'".to_string()]
        );
    }

    #[test]
    fn lowers_active_account_reads_through_internal_untracked_sources() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT account_id FROM lix_active_account WHERE account_id = 'acct-1'",
        )
        .expect("active account read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM lix_internal_state_untracked"));
        assert!(lowered_sql.contains("schema_key = 'lix_active_account'"));
        assert!(!lowered_sql.contains("FROM lix_active_account"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["account_id = 'acct-1'".to_string()]
        );
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

        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_version_descriptor"));
        assert!(lowered_sql.contains("lix_internal_state_materialized_v1_lix_version_pointer"));
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
    fn lowers_stored_schema_reads_through_bootstrap_table() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT value, lixcol_schema_key FROM lix_stored_schema WHERE lixcol_schema_key = 'x'",
        )
        .expect("stored schema read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("FROM lix_internal_stored_schema_bootstrap"));
        assert!(!lowered_sql.contains("FROM lix_stored_schema"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            Vec::<String>::new()
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["lixcol_schema_key = 'x'".to_string()]
        );
    }

    #[test]
    fn lowers_entity_history_root_commit_alias_through_history_source() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let lowered = lowered_program(
            &registry,
            "SELECT key, lixcol_root_commit_id, lixcol_depth \
             FROM lix_key_value_history \
             WHERE lixcol_root_commit_id = 'commit-1' AND lixcol_depth = 0",
        )
        .expect("entity history read should lower");
        let lowered_sql = lowered.statements[0].to_string();

        assert!(lowered_sql.contains("c.id = 'commit-1'"));
        assert!(!lowered_sql.contains("lixcol_c.entity_id"));
        assert_eq!(
            lowered.pushdown_decision.accepted_predicates,
            vec!["lixcol_root_commit_id = 'commit-1'".to_string()]
        );
        assert_eq!(
            lowered.pushdown_decision.residual_predicates,
            vec!["lixcol_depth = 0".to_string()]
        );
    }
}
