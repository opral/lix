use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SetExpr, Statement, TableAlias, TableFactor,
    TableWithJoins, UnaryOperator,
};
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::engine::sql::planning::param_context::{
    expr_last_identifier_eq, extract_string_column_values_from_expr, normalize_query_placeholders,
    PlaceholderOrdinalState,
};
use crate::internal_state::{object_name_matches, quote_ident};
use crate::version::GLOBAL_VERSION_ID;
use crate::{errors, LixBackend, LixError, Value as LixValue};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub fn rewrite_query(query: Query, params: &[LixValue]) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;

    let schema_keys = extract_schema_keys_from_query(&query, params).unwrap_or_default();

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(&mut new_query, &schema_keys, params, &mut changed, None)?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

pub async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[LixValue],
) -> Result<Option<Query>, LixError> {
    let mut query = query;
    normalize_query_placeholders(&mut query, &mut PlaceholderOrdinalState::new())?;
    let available_schema_keys = fetch_registered_schema_keys(backend).await?;

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
            schema_keys = fetch_schema_keys_for_plugins(backend, &plugin_keys).await?;
        }
    }
    if schema_keys.is_empty() {
        schema_keys = available_schema_keys.clone();
    }

    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(
        &mut new_query,
        &schema_keys,
        params,
        &mut changed,
        Some(&available_schema_keys),
    )?;

    if changed {
        Ok(Some(new_query))
    } else {
        Ok(None)
    }
}

fn rewrite_query_inner(
    query: &mut Query,
    schema_keys: &[String],
    params: &[LixValue],
    changed: &mut bool,
    available_schema_keys: Option<&[String]>,
) -> Result<(), LixError> {
    let query_schema_keys = resolve_schema_keys_for_query(query, schema_keys, params);
    if let Some(available) = available_schema_keys {
        validate_schema_keys_against_available(&query_schema_keys, available)?;
    }
    let top_level_targets_vtable = query_targets_vtable(&query);
    let pushdown_predicate = if top_level_targets_vtable {
        extract_pushdown_predicate(&query)
    } else {
        None
    };

    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_inner(
                &mut cte.query,
                &query_schema_keys,
                params,
                changed,
                available_schema_keys,
            )?;
        }
    }
    query.body = Box::new(rewrite_set_expr(
        (*query.body).clone(),
        &query_schema_keys,
        pushdown_predicate.as_ref(),
        params,
        changed,
        available_schema_keys,
    )?);
    Ok(())
}

fn rewrite_set_expr(
    expr: SetExpr,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
    available_schema_keys: Option<&[String]>,
) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(
                &mut select,
                schema_keys,
                pushdown_predicate,
                params,
                changed,
                available_schema_keys,
            )?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            rewrite_query_inner(
                &mut query,
                schema_keys,
                params,
                changed,
                available_schema_keys,
            )?;
            SetExpr::Query(Box::new(query))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op,
            set_quantifier,
            left: Box::new(rewrite_set_expr(
                *left,
                schema_keys,
                pushdown_predicate,
                params,
                changed,
                available_schema_keys,
            )?),
            right: Box::new(rewrite_set_expr(
                *right,
                schema_keys,
                pushdown_predicate,
                params,
                changed,
                available_schema_keys,
            )?),
        },
        other => other,
    })
}

fn rewrite_select(
    select: &mut Select,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
    available_schema_keys: Option<&[String]>,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, schema_keys, pushdown_predicate, params, changed)?;
    }
    rewrite_subqueries_in_select(select, schema_keys, params, changed, available_schema_keys)?;
    Ok(())
}

fn rewrite_subqueries_in_select(
    select: &mut Select,
    schema_keys: &[String],
    params: &[LixValue],
    changed: &mut bool,
    available_schema_keys: Option<&[String]>,
) -> Result<(), LixError> {
    struct NestedQueryRewriter<'a> {
        schema_keys: &'a [String],
        params: &'a [LixValue],
        changed: &'a mut bool,
        available_schema_keys: Option<&'a [String]>,
    }

    impl VisitorMut for NestedQueryRewriter<'_> {
        type Break = LixError;

        fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            let mut nested_changed = false;
            if let Err(error) = rewrite_query_inner(
                query,
                self.schema_keys,
                self.params,
                &mut nested_changed,
                self.available_schema_keys,
            ) {
                return ControlFlow::Break(error);
            }
            if nested_changed {
                *self.changed = true;
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = NestedQueryRewriter {
        schema_keys,
        params,
        changed,
        available_schema_keys,
    };
    if let ControlFlow::Break(error) = VisitMut::visit(select, &mut visitor) {
        return Err(error);
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        schema_keys,
        pushdown_predicate,
        params,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            schema_keys,
            pushdown_predicate,
            params,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if !schema_keys.is_empty() && object_name_matches(name, VTABLE_NAME) =>
        {
            let derived_query =
                build_untracked_union_query(schema_keys, pushdown_predicate, params)?;
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
            rewrite_table_with_joins(
                table_with_joins,
                schema_keys,
                pushdown_predicate,
                params,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn build_untracked_union_query(
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    params: &[LixValue],
) -> Result<Query, LixError> {
    let dialect = GenericDialect {};
    let stripped_predicate = pushdown_predicate.and_then(|expr| strip_qualifiers(expr.clone()));
    let has_version_predicate = stripped_predicate
        .as_ref()
        .is_some_and(|expr| expr_references_column(expr, "version_id"));
    let predicate_sql = stripped_predicate.as_ref().map(ToString::to_string);
    let predicate_schema_keys = stripped_predicate
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, expr_is_schema_key_column, params));
    let effective_schema_keys = narrow_schema_keys(schema_keys, predicate_schema_keys.as_deref());

    let schema_list = effective_schema_keys
        .iter()
        .map(|key| format!("'{}'", escape_string_literal(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let schema_filter = if effective_schema_keys.is_empty() {
        None
    } else {
        Some(format!("schema_key IN ({schema_list})"))
    };
    let untracked_where = match (schema_filter.as_ref(), predicate_sql.as_ref()) {
        (Some(schema_filter), Some(predicate)) => {
            format!("{schema_filter} AND ({predicate})")
        }
        (Some(schema_filter), None) => schema_filter.clone(),
        (None, Some(predicate)) => format!("({predicate})"),
        (None, None) => "1=1".to_string(),
    };

    let mut union_parts = Vec::new();
    union_parts.push(format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, global, 'untracked' AS change_id, writer_key, true AS untracked, 1 AS priority \
         FROM {untracked} \
         WHERE {untracked_where}",
        untracked = UNTRACKED_TABLE
    ));

    for key in &effective_schema_keys {
        let materialized_table = format!("{MATERIALIZED_PREFIX}{key}");
        let materialized_ident = quote_ident(&materialized_table);
        let materialized_where = predicate_sql
            .as_ref()
            .map(|predicate| format!(" WHERE ({predicate})"))
            .unwrap_or_default();
        union_parts.push(format!(
            "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, global, change_id, writer_key, false AS untracked, 2 AS priority \
             FROM {materialized}{materialized_where}",
            materialized = materialized_ident,
            materialized_where = materialized_where
        ));
    }

    let union_sql = union_parts.join(" UNION ALL ");
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

    let sql = format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, global, change_id, writer_key, untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, global, change_id, writer_key, untracked, \
                    ROW_NUMBER() OVER (\
                        PARTITION BY entity_id, schema_key, file_id, {partition_version_expr} \
                        ORDER BY {presentation_rank_expr}, priority\
                    ) AS rn \
             FROM ({union_sql}) AS lix_state_union\
         ) AS lix_state_ranked \
         WHERE rn = 1",
        partition_version_expr = partition_version_expr,
        presentation_rank_expr = presentation_rank_expr,
    );

    let mut statements = Parser::parse_sql(&dialect, &sql).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: err.to_string(),
    })?;

    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected single derived query statement".to_string(),
        });
    }

    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "derived query did not parse as SELECT".to_string(),
        }),
    }
}

fn query_targets_vtable(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    select.from.iter().any(table_with_joins_targets_vtable)
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

async fn fetch_registered_schema_keys(backend: &dyn LixBackend) -> Result<Vec<String>, LixError> {
    let result = backend
        .execute(
            "SELECT entity_id \
             FROM lix_internal_stored_schema_bootstrap \
             WHERE schema_key = 'lix_stored_schema' \
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
) -> Result<Vec<String>, LixError> {
    if plugin_keys.is_empty() {
        return Ok(Vec::new());
    }

    let changes_placeholders = numbered_placeholders(1, plugin_keys.len());
    let untracked_placeholders = numbered_placeholders(plugin_keys.len() + 1, plugin_keys.len());
    let sql = format!(
        "SELECT DISTINCT schema_key \
         FROM lix_internal_change \
         WHERE plugin_key IN ({changes_placeholders}) \
         UNION \
         SELECT DISTINCT schema_key \
         FROM {untracked_table} \
         WHERE plugin_key IN ({untracked_placeholders})",
        untracked_table = UNTRACKED_TABLE,
    );

    let mut params = Vec::with_capacity(plugin_keys.len() * 2);
    for key in plugin_keys {
        params.push(LixValue::Text(key.clone()));
    }
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

fn numbered_placeholders(start: usize, count: usize) -> String {
    (0..count)
        .map(|offset| format!("${}", start + offset))
        .collect::<Vec<_>>()
        .join(", ")
}
