use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SetExpr, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator,
};
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::schema::builtin::builtin_schema_keys;
use crate::schema::live_layout::{
    builtin_live_table_layout, live_schema_key_for_table_name, load_live_row_access_with_backend,
    tracked_live_table_name, untracked_live_table_name, LiveRowAccess, TRACKED_LIVE_TABLE_PREFIX,
};
use crate::live_state::shared::snapshot_sql::{live_snapshot_select_expr, live_snapshot_select_expr_for_schema};
use crate::state::internal::param_context::{
    expr_last_identifier_eq, extract_string_column_values_from_expr, normalize_query_placeholders,
    PlaceholderOrdinalState,
};
use crate::state::internal::{object_name_matches, quote_ident};
use crate::version::GLOBAL_VERSION_ID;
use crate::{errors, LixBackend, LixError, SqlDialect, Value as LixValue};

const VTABLE_NAME: &str = "lix_internal_state_vtable";

pub fn rewrite_query(
    query: Query,
    params: &[LixValue],
    dialect: SqlDialect,
) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;

    let schema_keys = extract_schema_keys_from_query(&query, params).unwrap_or_default();
    let live_accesses = load_builtin_live_accesses(&schema_keys)?;
    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner_with_live_accesses(
        &mut new_query,
        &schema_keys,
        &live_accesses,
        params,
        &mut changed,
        None,
        dialect,
    )?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn load_builtin_live_accesses(
    schema_keys: &[String],
) -> Result<std::collections::BTreeMap<String, LiveRowAccess>, LixError> {
    let mut live_accesses = std::collections::BTreeMap::<String, LiveRowAccess>::new();
    for key in schema_keys {
        let Some(layout) = builtin_live_table_layout(key)? else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "internal state vtable rewrite requires backend-loaded live layout for non-builtin schema '{}'",
                    key
                ),
            });
        };
        live_accesses.insert(key.clone(), LiveRowAccess::new(layout));
    }
    Ok(live_accesses)
}

pub async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[LixValue],
) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;
    let available_schema_keys = fetch_registered_schema_keys(backend).await?;
    let materialized_schema_keys = fetch_materialized_state_schema_keys(backend).await?;

    let mut schema_keys = extract_schema_keys_from_query(&query, params).unwrap_or_default();
    if !schema_keys.is_empty() {
        validate_schema_keys_against_available(&schema_keys, &available_schema_keys)?;
    }

    // If no schema-key literal is available, fall back to plugin-key derived
    // schema resolution and finally to all materialized schema tables.
    if schema_keys.is_empty() {
        let plugin_keys = extract_plugin_keys_from_query(&query, params)
            .or_else(|| extract_plugin_keys_from_top_level_derived_subquery(&query, params))
            .unwrap_or_default();
        if !plugin_keys.is_empty() {
            schema_keys =
                fetch_schema_keys_for_plugins(backend, &plugin_keys, &materialized_schema_keys)
                    .await?;
        }
    }
    if schema_keys.is_empty() {
        schema_keys = materialized_schema_keys.clone();
    }

    let mut live_accesses = std::collections::BTreeMap::<String, LiveRowAccess>::new();
    for schema_key in &schema_keys {
        live_accesses.insert(
            schema_key.clone(),
            load_live_row_access_with_backend(backend, schema_key).await?,
        );
    }

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner_with_live_accesses(
        &mut new_query,
        &schema_keys,
        &live_accesses,
        params,
        &mut changed,
        Some(&available_schema_keys),
        backend.dialect(),
    )?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_query_inner_with_live_accesses(
    query: &mut Query,
    schema_keys: &[String],
    live_accesses: &std::collections::BTreeMap<String, LiveRowAccess>,
    params: &[LixValue],
    changed: &mut bool,
    available_schema_keys: Option<&[String]>,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    let query_schema_keys = resolve_schema_keys_for_query(query, schema_keys, params);
    if let Some(available) = available_schema_keys {
        validate_schema_keys_against_available(&query_schema_keys, available)?;
    }
    let top_level_targets_vtable = query_targets_vtable(&query);
    let requires_snapshot_content =
        top_level_targets_vtable && query_requires_snapshot_content(query);
    let pushdown_predicate = if top_level_targets_vtable {
        extract_pushdown_predicate(&query)
    } else {
        None
    };

    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_inner_with_live_accesses(
                &mut cte.query,
                schema_keys,
                live_accesses,
                params,
                changed,
                available_schema_keys,
                dialect,
            )?;
        }
    }

    let SetExpr::Select(select) = query.body.as_mut() else {
        return Ok(());
    };
    rewrite_select_with_live_accesses(
        select,
        &query_schema_keys,
        live_accesses,
        requires_snapshot_content,
        pushdown_predicate.as_ref(),
        params,
        changed,
        dialect,
    )
}

fn rewrite_select_with_live_accesses(
    select: &mut Box<Select>,
    schema_keys: &[String],
    live_accesses: &std::collections::BTreeMap<String, LiveRowAccess>,
    requires_snapshot_content: bool,
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins_with_live_accesses(
            table,
            schema_keys,
            live_accesses,
            requires_snapshot_content,
            pushdown_predicate,
            params,
            changed,
            dialect,
        )?;
    }
    Ok(())
}

fn rewrite_table_with_joins_with_live_accesses(
    table: &mut TableWithJoins,
    schema_keys: &[String],
    live_accesses: &std::collections::BTreeMap<String, LiveRowAccess>,
    requires_snapshot_content: bool,
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    rewrite_table_factor_with_live_accesses(
        &mut table.relation,
        schema_keys,
        live_accesses,
        requires_snapshot_content,
        pushdown_predicate,
        params,
        changed,
        dialect,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor_with_live_accesses(
            &mut join.relation,
            schema_keys,
            live_accesses,
            requires_snapshot_content,
            pushdown_predicate,
            params,
            changed,
            dialect,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor_with_live_accesses(
    relation: &mut TableFactor,
    schema_keys: &[String],
    live_accesses: &std::collections::BTreeMap<String, LiveRowAccess>,
    requires_snapshot_content: bool,
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
    dialect: SqlDialect,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if !schema_keys.is_empty() && object_name_matches(name, VTABLE_NAME) =>
        {
            let derived_query = build_untracked_union_query_with_accesses(
                schema_keys,
                live_accesses,
                requires_snapshot_content,
                pushdown_predicate,
                params,
                dialect,
            )?;
            let derived_alias = alias.clone().or_else(|| Some(default_vtable_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins_with_live_accesses(
                table_with_joins,
                schema_keys,
                live_accesses,
                requires_snapshot_content,
                pushdown_predicate,
                params,
                changed,
                dialect,
            )?;
        }
        TableFactor::Derived { subquery, .. } => {
            rewrite_query_inner_with_live_accesses(
                subquery,
                schema_keys,
                live_accesses,
                params,
                changed,
                None,
                dialect,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn build_untracked_union_query_with_accesses(
    schema_keys: &[String],
    live_accesses: &std::collections::BTreeMap<String, LiveRowAccess>,
    include_snapshot_content: bool,
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    dialect: SqlDialect,
) -> Result<Query, LixError> {
    let parser_dialect = GenericDialect {};
    let stripped_predicate = pushdown_predicate.and_then(|expr| strip_qualifiers(expr.clone()));
    let has_version_predicate = stripped_predicate
        .as_ref()
        .is_some_and(|expr| expr_references_column(expr, "version_id"));
    let predicate_schema_keys = stripped_predicate
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, expr_is_schema_key_column, params));
    let effective_schema_keys = narrow_schema_keys(schema_keys, predicate_schema_keys.as_deref());
    let effective_schema_keys = if effective_schema_keys.is_empty() {
        builtin_schema_keys()
            .iter()
            .map(|schema_key| (*schema_key).to_string())
            .collect::<Vec<_>>()
    } else {
        effective_schema_keys
    };

    let mut schema_winner_queries = Vec::new();
    for key in &effective_schema_keys {
        let Some(access) = live_accesses.get(key) else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "internal state vtable rewrite missing live-row access for schema '{}'",
                    key
                ),
            });
        };
        schema_winner_queries.push(build_schema_winner_query(
            key,
            access,
            include_snapshot_content,
            stripped_predicate.as_ref(),
            dialect,
            has_version_predicate,
        )?);
    }

    let sql = schema_winner_queries.join(" UNION ALL ");
    Parser::new(&parser_dialect)
        .try_with_sql(&sql)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?
        .parse_query()
        .map(|query| *query)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })
}

fn build_schema_winner_query(
    schema_key: &str,
    access: &LiveRowAccess,
    include_snapshot_content: bool,
    stripped_predicate: Option<&Expr>,
    dialect: SqlDialect,
    has_version_predicate: bool,
) -> Result<String, LixError> {
    if schema_key == "lix_registered_schema" {
        return build_registered_schema_bootstrap_query(
            access,
            include_snapshot_content,
            stripped_predicate,
            dialect,
        );
    }

    let untracked_table = quote_ident(&untracked_live_table_name(schema_key));
    let tracked_table = quote_ident(&tracked_live_table_name(schema_key));
    let tracked_full_projection = access.normalized_projection_sql(Some("t"));
    let untracked_full_projection = access.normalized_projection_sql(Some("u"));
    let ranked_payload_projection = if include_snapshot_content {
        access.normalized_projection_sql(Some("schema_union"))
    } else {
        String::new()
    };

    let untracked_where = stripped_predicate
        .map(|expr| {
            if include_snapshot_content {
                render_pushdown_predicate_for_schema_with_snapshot_expr(
                    expr,
                    &live_snapshot_select_expr(access.layout(), dialect, Some("u")),
                )
            } else {
                render_pushdown_predicate_for_schema(expr, schema_key, dialect)
            }
        })
        .transpose()?
        .map(|predicate| format!(" AND ({predicate})"))
        .unwrap_or_default();

    let tracked_where = stripped_predicate
        .map(|expr| {
            if include_snapshot_content {
                let tracked_snapshot_expr = format!(
                    "CASE WHEN {} THEN NULL ELSE {} END",
                    format!("{} = 1", qualified_column_ref(Some("t"), "is_tombstone")),
                    live_snapshot_select_expr(access.layout(), dialect, Some("t"))
                );
                render_pushdown_predicate_for_schema_with_snapshot_expr(
                    expr,
                    &tracked_snapshot_expr,
                )
            } else {
                render_pushdown_predicate_for_schema(expr, schema_key, dialect)
            }
        })
        .transpose()?
        .map(|predicate| format!(" AND ({predicate})"))
        .unwrap_or_default();

    let partition_version_expr = if has_version_predicate {
        "version_id".to_string()
    } else {
        format!(
            "CASE WHEN global = true THEN '{}' ELSE version_id END",
            escape_string_literal(GLOBAL_VERSION_ID)
        )
    };
    let presentation_rank_expr = if has_version_predicate {
        "0".to_string()
    } else {
        format!(
            "CASE WHEN global = true AND version_id <> '{}' THEN 0 \
                  WHEN global = true THEN 1 \
                  ELSE 0 END",
            escape_string_literal(GLOBAL_VERSION_ID)
        )
    };

    let snapshot_projection = if include_snapshot_content {
        format!(
            "CASE WHEN ranked.is_tombstone = 1 THEN NULL ELSE {} END AS snapshot_content, ",
            live_snapshot_select_expr(access.layout(), dialect, Some("ranked"))
        )
    } else {
        String::new()
    };

    Ok(format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, {snapshot_projection}metadata, schema_version, \
                created_at, updated_at, global, change_id, writer_key, untracked \
         FROM (\
            SELECT entity_id, schema_key, file_id, version_id, plugin_key, metadata, schema_version, \
                   created_at, updated_at, global, change_id, writer_key, untracked, is_tombstone{ranked_payload_projection}, \
                   ROW_NUMBER() OVER (\
                       PARTITION BY entity_id, schema_key, file_id, {partition_version_expr} \
                       ORDER BY {presentation_rank_expr}, priority\
                   ) AS rn \
            FROM (\
                SELECT entity_id, schema_key, file_id, version_id, plugin_key, metadata, schema_version, \
                       created_at, updated_at, global, 'untracked' AS change_id, writer_key, true AS untracked, 0 AS is_tombstone{untracked_full_projection}, 1 AS priority \
                FROM {untracked_table} u \
                WHERE u.untracked = true{untracked_where} \
                UNION ALL \
                SELECT entity_id, schema_key, file_id, version_id, plugin_key, metadata, schema_version, \
                       created_at, updated_at, global, change_id, writer_key, false AS untracked, is_tombstone{tracked_full_projection}, 2 AS priority \
                FROM {tracked_table} t \
                WHERE t.untracked = false{tracked_where} \
            ) AS schema_union\
         ) AS ranked \
         WHERE rn = 1",
        snapshot_projection = snapshot_projection,
        ranked_payload_projection = ranked_payload_projection,
        partition_version_expr = partition_version_expr,
        presentation_rank_expr = presentation_rank_expr,
        untracked_table = untracked_table,
        untracked_where = untracked_where,
        untracked_full_projection = untracked_full_projection,
        tracked_table = tracked_table,
        tracked_where = tracked_where,
        tracked_full_projection = tracked_full_projection,
    ))
}

fn build_registered_schema_bootstrap_query(
    _access: &LiveRowAccess,
    include_snapshot_content: bool,
    stripped_predicate: Option<&Expr>,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let table = "lix_internal_registered_schema_bootstrap";
    let where_sql = stripped_predicate
        .map(|expr| {
            if include_snapshot_content {
                render_pushdown_predicate_for_schema_with_snapshot_expr(
                    expr,
                    &format!(
                        "CASE WHEN {} THEN NULL ELSE {} END",
                        format!("{} = 1", qualified_column_ref(Some("b"), "is_tombstone")),
                        qualified_column_ref(Some("b"), "snapshot_content")
                    ),
                )
            } else {
                render_pushdown_predicate_for_schema(expr, "lix_registered_schema", dialect)
            }
        })
        .transpose()?
        .map(|predicate| format!(" WHERE ({predicate})"))
        .unwrap_or_default();
    let snapshot_projection = if include_snapshot_content {
        "CASE WHEN b.is_tombstone = 1 THEN NULL ELSE b.snapshot_content END AS snapshot_content, "
            .to_string()
    } else {
        String::new()
    };
    let snapshot_payload_projection = if include_snapshot_content {
        ", snapshot_content".to_string()
    } else {
        String::new()
    };
    Ok(format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, {snapshot_projection}metadata, schema_version, \
                created_at, updated_at, global, change_id, writer_key, false AS untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, plugin_key, metadata, schema_version, \
                    created_at, updated_at, global, change_id, writer_key, is_tombstone{snapshot_payload_projection} \
             FROM {table} b{where_sql} \
         ) AS b",
        snapshot_projection = snapshot_projection,
        snapshot_payload_projection = snapshot_payload_projection,
        table = table,
        where_sql = where_sql,
    ))
}

fn query_targets_vtable(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(table_with_joins_targets_vtable)
}

fn query_requires_snapshot_content(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };

    select
        .projection
        .iter()
        .any(select_item_requires_snapshot_content)
        || select
            .selection
            .as_ref()
            .is_some_and(|expr| expr_references_column(expr, "snapshot_content"))
        || query
            .order_by
            .as_ref()
            .is_some_and(order_by_requires_snapshot_content)
}

fn select_item_requires_snapshot_content(item: &sqlparser::ast::SelectItem) -> bool {
    match item {
        sqlparser::ast::SelectItem::Wildcard(_)
        | sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => true,
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_column(expr, "snapshot_content")
        }
    }
}

fn order_by_requires_snapshot_content(order_by: &sqlparser::ast::OrderBy) -> bool {
    match &order_by.kind {
        sqlparser::ast::OrderByKind::Expressions(ordering) => ordering
            .iter()
            .any(|item| expr_references_column(&item.expr, "snapshot_content")),
        _ => false,
    }
}

fn table_with_joins_targets_vtable(table: &TableWithJoins) -> bool {
    table_factor_is_vtable(&table.relation)
        || table
            .joins
            .iter()
            .any(|join| table_factor_is_vtable(&join.relation))
}

fn table_factor_is_vtable(relation: &TableFactor) -> bool {
    matches!(
        relation,
        TableFactor::Table { name, .. } if object_name_matches(name, VTABLE_NAME)
    )
}

fn extract_schema_keys_from_query(query: &Query, params: &[LixValue]) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_schema_key_column, params)
}

fn resolve_schema_keys_for_query(
    query: &Query,
    inherited_schema_keys: &[String],
    params: &[LixValue],
) -> Vec<String> {
    extract_schema_keys_from_query(query, params).unwrap_or_else(|| inherited_schema_keys.to_vec())
}

fn extract_plugin_keys_from_query(query: &Query, params: &[LixValue]) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_plugin_key_column, params)
}

fn extract_plugin_keys_from_top_level_derived_subquery(
    query: &Query,
    params: &[LixValue],
) -> Option<Vec<String>> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    if select.projection.len() != 1 {
        return None;
    }
    let projection_normalized = select.projection[0]
        .to_string()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    if projection_normalized != "count(*)" {
        return None;
    }
    if select.selection.is_some() {
        return None;
    }
    if select.from.len() != 1 {
        return None;
    }
    let table = select.from.first()?;
    if !table.joins.is_empty() {
        return None;
    }
    let TableFactor::Derived { subquery, .. } = &table.relation else {
        return None;
    };
    extract_plugin_keys_from_query(subquery, params)
}

fn extract_pushdown_predicate(query: &Query) -> Option<Expr> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select,
        _ => return None,
    };
    let selection = select.selection.as_ref()?;
    strip_qualifiers(selection.clone())
}

fn extract_column_keys_from_query(
    query: &Query,
    is_target_column: fn(&Expr) -> bool,
    params: &[LixValue],
) -> Option<Vec<String>> {
    extract_column_keys_from_set_expr(&query.body, is_target_column, params)
}

fn extract_column_keys_from_set_expr(
    expr: &SetExpr,
    is_target_column: fn(&Expr) -> bool,
    params: &[LixValue],
) -> Option<Vec<String>> {
    match expr {
        SetExpr::Select(select) => {
            extract_column_keys_from_select(select, is_target_column, params)
        }
        SetExpr::Query(query) => {
            extract_column_keys_from_set_expr(&query.body, is_target_column, params)
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_column_keys_from_set_expr(left, is_target_column, params)
                .or_else(|| extract_column_keys_from_set_expr(right, is_target_column, params))
        }
        _ => None,
    }
}

fn extract_column_keys_from_select(
    select: &Select,
    is_target_column: fn(&Expr) -> bool,
    params: &[LixValue],
) -> Option<Vec<String>> {
    select
        .selection
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, is_target_column, params))
}

fn extract_column_keys_from_expr(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
    params: &[LixValue],
) -> Option<Vec<String>> {
    extract_string_column_values_from_expr(expr, is_target_column, params)
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "schema_key")
}

fn expr_is_plugin_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "plugin_key")
}

fn expr_references_column(expr: &Expr, column_name: &str) -> bool {
    if expr_last_identifier_eq(expr, column_name) {
        return true;
    }

    match expr {
        Expr::BinaryOp { left, right, .. } => {
            expr_references_column(left, column_name) || expr_references_column(right, column_name)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr) => expr_references_column(expr, column_name),
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_references_column(expr, column_name)
                || expr_references_column(low, column_name)
                || expr_references_column(high, column_name)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            expr_references_column(expr, column_name)
                || expr_references_column(pattern, column_name)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_column(expr, column_name)
                || list
                    .iter()
                    .any(|item| expr_references_column(item, column_name))
        }
        Expr::InSubquery { expr, .. } => expr_references_column(expr, column_name),
        Expr::Tuple(items) => items
            .iter()
            .any(|item| expr_references_column(item, column_name)),
        Expr::Function(function) => match &function.args {
            sqlparser::ast::FunctionArguments::List(list) => {
                list.args.iter().any(|arg| match arg {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(expr),
                    ) => expr_references_column(expr, column_name),
                    sqlparser::ast::FunctionArg::Named { arg, .. }
                    | sqlparser::ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                        sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                            expr_references_column(expr, column_name)
                        }
                        _ => false,
                    },
                    _ => false,
                })
            }
            _ => false,
        },
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|value| expr_references_column(value, column_name))
                || conditions.iter().any(|condition| {
                    expr_references_column(&condition.condition, column_name)
                        || expr_references_column(&condition.result, column_name)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| expr_references_column(value, column_name))
        }
        _ => false,
    }
}

fn strip_qualifiers(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::Identifier(ident) => {
            if is_pushdown_column(&ident) {
                Some(Expr::Identifier(ident))
            } else {
                None
            }
        }
        Expr::CompoundIdentifier(_) => None,
        Expr::BinaryOp { left, op, right } => {
            if !is_simple_binary_op(&op) {
                return None;
            }
            let left = strip_qualifiers(*left)?;
            let right = strip_qualifiers(*right)?;
            Some(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        Expr::Nested(inner) => strip_qualifiers(*inner).map(|inner| Expr::Nested(Box::new(inner))),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let list = strip_in_list_values(list)?;
            Some(Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            })
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let low = strip_value_expr(*low)?;
            let high = strip_value_expr(*high)?;
            Some(Expr::Between {
                expr: Box::new(expr),
                negated,
                low: Box::new(low),
                high: Box::new(high),
            })
        }
        Expr::IsNull(inner) => {
            let inner = strip_qualifiers(*inner)?;
            Some(Expr::IsNull(Box::new(inner)))
        }
        Expr::IsNotNull(inner) => {
            let inner = strip_qualifiers(*inner)?;
            Some(Expr::IsNotNull(Box::new(inner)))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            let expr = strip_qualifiers(*expr)?;
            Some(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        }
        Expr::Like {
            expr,
            negated,
            pattern,
            escape_char,
            any,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let pattern = strip_value_expr(*pattern)?;
            Some(Expr::Like {
                expr: Box::new(expr),
                negated,
                pattern: Box::new(pattern),
                escape_char,
                any,
            })
        }
        Expr::ILike {
            expr,
            negated,
            pattern,
            escape_char,
            any,
        } => {
            let expr = strip_qualifiers(*expr)?;
            let pattern = strip_value_expr(*pattern)?;
            Some(Expr::ILike {
                expr: Box::new(expr),
                negated,
                pattern: Box::new(pattern),
                escape_char,
                any,
            })
        }
        Expr::Value(_) => Some(expr),
        _ => None,
    }
}

fn strip_in_list_values(list: Vec<Expr>) -> Option<Vec<Expr>> {
    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let value = strip_value_expr(item)?;
        values.push(value);
    }
    Some(values)
}

fn strip_value_expr(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::Value(_) => Some(expr),
        Expr::Nested(inner) => strip_value_expr(*inner).map(|inner| Expr::Nested(Box::new(inner))),
        _ => None,
    }
}

fn is_pushdown_column(ident: &Ident) -> bool {
    let value = ident.value.to_ascii_lowercase();
    matches!(
        value.as_str(),
        "entity_id"
            | "schema_key"
            | "schema_version"
            | "file_id"
            | "version_id"
            | "plugin_key"
            | "snapshot_content"
            | "metadata"
    )
}

fn is_simple_binary_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    )
}

fn intersect_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for value in left {
        if right.contains(value) && !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
}

fn narrow_schema_keys(
    schema_keys: &[String],
    predicate_schema_keys: Option<&[String]>,
) -> Vec<String> {
    let Some(predicate_schema_keys) = predicate_schema_keys else {
        return schema_keys.to_vec();
    };
    if schema_keys.is_empty() {
        return predicate_schema_keys.to_vec();
    }
    let intersection = intersect_strings(schema_keys, predicate_schema_keys);
    if intersection.is_empty() {
        schema_keys.to_vec()
    } else {
        intersection
    }
}

fn default_vtable_alias() -> TableAlias {
    TableAlias {
        explicit: false,
        name: Ident::new(VTABLE_NAME),
        columns: Vec::new(),
    }
}

fn escape_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn qualified_column_ref(table_alias: Option<&str>, column_name: &str) -> String {
    match table_alias {
        Some(alias) => format!(
            "\"{}\".\"{}\"",
            alias.replace('"', "\"\""),
            column_name.replace('"', "\"\"")
        ),
        None => format!("\"{}\"", column_name.replace('"', "\"\"")),
    }
}

fn render_pushdown_predicate_for_schema(
    predicate: &Expr,
    schema_key: &str,
    dialect: SqlDialect,
) -> Result<String, LixError> {
    let mut rewritten = predicate.clone();
    let replacement_sql = live_snapshot_select_expr_for_schema(schema_key, dialect, None)?;
    let replacement_expr = Parser::new(&GenericDialect {})
        .try_with_sql(&replacement_sql)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?
        .parse_expr()
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?;
    let mut replacer = SnapshotContentPredicateRewriter {
        replacement: replacement_expr.clone(),
    };
    if let ControlFlow::Break(error) = rewritten.visit(&mut replacer) {
        return Err(error);
    }
    Ok(rewritten.to_string())
}

fn render_pushdown_predicate_for_schema_with_snapshot_expr(
    predicate: &Expr,
    replacement_sql: &str,
) -> Result<String, LixError> {
    let mut rewritten = predicate.clone();
    let replacement_expr = Parser::new(&GenericDialect {})
        .try_with_sql(replacement_sql)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?
        .parse_expr()
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?;
    let mut replacer = SnapshotContentPredicateRewriter {
        replacement: replacement_expr.clone(),
    };
    if let ControlFlow::Break(error) = rewritten.visit(&mut replacer) {
        return Err(error);
    }
    Ok(rewritten.to_string())
}

struct SnapshotContentPredicateRewriter {
    replacement: Expr,
}

impl VisitorMut for SnapshotContentPredicateRewriter {
    type Break = LixError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if expr_last_identifier_eq(expr, "snapshot_content") {
            *expr = self.replacement.clone();
        }
        ControlFlow::Continue(())
    }
}

async fn fetch_registered_schema_keys(backend: &dyn LixBackend) -> Result<Vec<String>, LixError> {
    let result = backend
        .execute(
            "SELECT entity_id \
             FROM lix_internal_registered_schema_bootstrap \
             WHERE schema_key = 'lix_registered_schema' \
               AND version_id = 'global' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await?;

    let mut keys = Vec::new();
    for row in &result.rows {
        let Some(LixValue::Text(entity_id)) = row.first() else {
            continue;
        };
        let schema_key = entity_id
            .split_once('~')
            .map(|(key, _)| key)
            .unwrap_or(entity_id.as_str());
        if schema_key.is_empty() {
            continue;
        }
        if !keys.iter().any(|existing| existing == schema_key) {
            keys.push(schema_key.to_string());
        }
    }

    keys.sort();
    Ok(keys)
}

fn validate_schema_keys_against_available(
    schema_keys: &[String],
    available: &[String],
) -> Result<(), LixError> {
    if available.is_empty() {
        return Ok(());
    }
    let available_refs = available.iter().map(String::as_str).collect::<Vec<_>>();
    for schema_key in schema_keys {
        if !available.iter().any(|existing| existing == schema_key) {
            return Err(errors::schema_not_registered_error(
                schema_key,
                &available_refs,
            ));
        }
    }
    Ok(())
}

async fn fetch_schema_keys_for_plugins(
    backend: &dyn LixBackend,
    plugin_keys: &[String],
    materialized_schema_keys: &[String],
) -> Result<Vec<String>, LixError> {
    if plugin_keys.is_empty() {
        return Ok(Vec::new());
    }

    let changes_placeholders = numbered_placeholders(1, plugin_keys.len());
    let mut union_parts = vec![format!(
        "SELECT DISTINCT schema_key \
         FROM lix_internal_change \
         WHERE plugin_key IN ({changes_placeholders})"
    )];
    for schema_key in materialized_schema_keys {
        let untracked_table = quote_ident(&untracked_live_table_name(schema_key));
        union_parts.push(format!(
            "SELECT DISTINCT schema_key \
             FROM {untracked_table} \
             WHERE untracked = true \
               AND plugin_key IN ({plugin_placeholders})",
            untracked_table = untracked_table,
            plugin_placeholders = changes_placeholders,
        ));
    }
    let sql = union_parts.join(" UNION ");

    let mut params = Vec::with_capacity(plugin_keys.len());
    for key in plugin_keys {
        params.push(LixValue::Text(key.clone()));
    }

    let result = backend.execute(&sql, &params).await?;

    let mut keys = Vec::new();
    for row in &result.rows {
        let Some(LixValue::Text(schema_key)) = row.first() else {
            continue;
        };
        if schema_key.is_empty() {
            continue;
        }
        if !keys.iter().any(|existing| existing == schema_key) {
            keys.push(schema_key.clone());
        }
    }

    keys.sort();
    Ok(keys)
}

async fn fetch_materialized_state_schema_keys(
    backend: &dyn LixBackend,
) -> Result<Vec<String>, LixError> {
    let tracked_like = format!("{TRACKED_LIVE_TABLE_PREFIX}%");
    let table_rows = match backend.dialect() {
        crate::SqlDialect::Sqlite => {
            backend
                .execute(
                    &format!(
                        "SELECT name \
                         FROM sqlite_master \
                         WHERE type = 'table' \
                           AND name LIKE '{tracked_like}'"
                    ),
                    &[],
                )
                .await?
        }
        crate::SqlDialect::Postgres => {
            backend
                .execute(
                    &format!(
                        "SELECT table_name \
                         FROM information_schema.tables \
                         WHERE table_schema = current_schema() \
                           AND table_name LIKE '{tracked_like}'"
                    ),
                    &[],
                )
                .await?
        }
    };

    let mut keys = Vec::new();
    for row in &table_rows.rows {
        let Some(LixValue::Text(table_name)) = row.first() else {
            continue;
        };
        let schema_key = live_schema_key_for_table_name(table_name);
        let Some(schema_key) = schema_key else {
            continue;
        };
        if !schema_key.is_empty() && !keys.iter().any(|existing| existing == schema_key) {
            keys.push(schema_key.to_string());
        }
    }
    keys.sort();
    Ok(keys)
}

fn numbered_placeholders(start: usize, count: usize) -> String {
    (0..count)
        .map(|offset| format!("${}", start + offset))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::render_pushdown_predicate_for_schema_with_snapshot_expr;
    use crate::LixError;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_expr(sql: &str) -> Result<sqlparser::ast::Expr, LixError> {
        Parser::new(&GenericDialect {})
            .try_with_sql(sql)
            .map_err(|error| LixError::unknown(error.to_string()))?
            .parse_expr()
            .map_err(|error| LixError::unknown(error.to_string()))
    }

    #[test]
    fn registered_schema_snapshot_predicate_rewrite_does_not_rewrite_inside_replacement() {
        let predicate = parse_expr("snapshot_content IS NOT NULL").expect("predicate should parse");

        let rewritten = render_pushdown_predicate_for_schema_with_snapshot_expr(
            &predicate,
            "CASE WHEN b.is_tombstone = 1 THEN NULL ELSE b.snapshot_content END",
        )
        .expect("rewrite should succeed");

        assert_eq!(
            rewritten,
            "CASE WHEN b.is_tombstone = 1 THEN NULL ELSE b.snapshot_content END IS NOT NULL"
        );
    }
}
