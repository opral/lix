use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SetExpr, TableAlias, TableFactor, TableWithJoins,
    UnaryOperator, Value, ValueWithSpan,
};

use crate::backend::SqlDialect;
use crate::sql::{object_name_matches, quote_ident};
use crate::{LixBackend, LixError, Value as LixValue};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

#[derive(Debug, Clone, Default)]
pub(crate) struct VtableReadOp {
    pub(crate) top_level_targets_vtable: bool,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RootVtableReadPlan {
    pub(crate) schema_keys: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct VtableReadPlan {
    pub(crate) effective_schema_keys: Vec<String>,
    pub(crate) pushdown_predicate: Option<Expr>,
}

pub(crate) fn infer_op(query: &Query) -> VtableReadOp {
    let top_level_targets_vtable = query_targets_vtable(query);
    let schema_keys = if top_level_targets_vtable {
        extract_schema_keys_from_query(query).unwrap_or_default()
    } else {
        Vec::new()
    };
    let plugin_keys = if top_level_targets_vtable {
        extract_plugin_keys_from_query(query).unwrap_or_default()
    } else {
        extract_plugin_keys_from_top_level_derived_subquery(query).unwrap_or_default()
    };
    VtableReadOp {
        top_level_targets_vtable,
        schema_keys,
        plugin_keys,
    }
}

pub(crate) fn plan_without_backend(op: &VtableReadOp) -> RootVtableReadPlan {
    RootVtableReadPlan {
        schema_keys: if op.top_level_targets_vtable {
            op.schema_keys.clone()
        } else {
            Vec::new()
        },
    }
}

pub(crate) async fn plan_with_backend(
    backend: &dyn LixBackend,
    op: &VtableReadOp,
) -> Result<RootVtableReadPlan, LixError> {
    let mut schema_keys = if op.top_level_targets_vtable {
        op.schema_keys.clone()
    } else {
        Vec::new()
    };

    if schema_keys.is_empty() && !op.plugin_keys.is_empty() {
        schema_keys = fetch_schema_keys_for_plugins(backend, &op.plugin_keys).await?;
    }
    if schema_keys.is_empty() {
        schema_keys = fetch_materialized_schema_keys(backend).await?;
    }

    Ok(RootVtableReadPlan { schema_keys })
}

pub(crate) fn rewrite_query_with_plan(
    query: Query,
    root_plan: &RootVtableReadPlan,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<Option<Query>, LixError> {
    let mut changed = false;
    let mut new_query = query.clone();
    rewrite_query_inner(
        &mut new_query,
        &root_plan.schema_keys,
        &mut changed,
        emitter,
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
    changed: &mut bool,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<(), LixError> {
    let query_schema_keys = resolve_schema_keys_for_query(query, schema_keys);
    let top_level_targets_vtable = query_targets_vtable(query);
    let pushdown_predicate = if top_level_targets_vtable {
        extract_pushdown_predicate(query)
    } else {
        None
    };

    if let Some(with) = query.with.as_mut() {
        for cte in &mut with.cte_tables {
            rewrite_query_inner(&mut cte.query, &query_schema_keys, changed, emitter)?;
        }
    }

    query.body = Box::new(rewrite_set_expr(
        (*query.body).clone(),
        &query_schema_keys,
        pushdown_predicate.as_ref(),
        changed,
        emitter,
    )?);
    Ok(())
}

fn rewrite_set_expr(
    expr: SetExpr,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<SetExpr, LixError> {
    Ok(match expr {
        SetExpr::Select(select) => {
            let mut select = *select;
            rewrite_select(
                &mut select,
                schema_keys,
                pushdown_predicate,
                changed,
                emitter,
            )?;
            SetExpr::Select(Box::new(select))
        }
        SetExpr::Query(query) => {
            let mut query = *query;
            rewrite_query_inner(&mut query, schema_keys, changed, emitter)?;
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
                changed,
                emitter,
            )?),
            right: Box::new(rewrite_set_expr(
                *right,
                schema_keys,
                pushdown_predicate,
                changed,
                emitter,
            )?),
        },
        other => other,
    })
}

fn rewrite_select(
    select: &mut Select,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<(), LixError> {
    for table in &mut select.from {
        rewrite_table_with_joins(table, schema_keys, pushdown_predicate, changed, emitter)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        schema_keys,
        pushdown_predicate,
        changed,
        emitter,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            schema_keys,
            pushdown_predicate,
            changed,
            emitter,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    schema_keys: &[String],
    pushdown_predicate: Option<&Expr>,
    changed: &mut bool,
    emitter: &mut dyn FnMut(&VtableReadPlan) -> Result<Query, LixError>,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if !schema_keys.is_empty() && object_name_matches(name, VTABLE_NAME) =>
        {
            let plan = plan_for_relation(schema_keys, pushdown_predicate);
            let derived_query = emitter(&plan)?;
            let derived_alias = alias.clone().or_else(|| Some(default_vtable_alias()));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            *changed = true;
        }
        TableFactor::Derived { subquery, .. } => {
            let mut subquery_changed = false;
            let mut rewritten_subquery = (**subquery).clone();
            rewrite_query_inner(
                &mut rewritten_subquery,
                schema_keys,
                &mut subquery_changed,
                emitter,
            )?;
            if subquery_changed {
                *subquery = Box::new(rewritten_subquery);
                *changed = true;
            }
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_with_joins(
                table_with_joins,
                schema_keys,
                pushdown_predicate,
                changed,
                emitter,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn plan_for_relation(schema_keys: &[String], pushdown_predicate: Option<&Expr>) -> VtableReadPlan {
    let stripped_predicate = pushdown_predicate.and_then(|expr| strip_qualifiers(expr.clone()));
    let predicate_schema_keys = stripped_predicate
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, expr_is_schema_key_column));
    let effective_schema_keys = narrow_schema_keys(schema_keys, predicate_schema_keys.as_deref());

    VtableReadPlan {
        effective_schema_keys,
        pushdown_predicate: stripped_predicate,
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

fn resolve_schema_keys_for_query(query: &Query, inherited_schema_keys: &[String]) -> Vec<String> {
    extract_schema_keys_from_query(query).unwrap_or_else(|| inherited_schema_keys.to_vec())
}

fn extract_schema_keys_from_query(query: &Query) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_schema_key_column)
}

#[cfg(test)]
pub(crate) fn extract_schema_keys_from_query_deep(query: &Query) -> Vec<String> {
    let mut keys = Vec::new();
    collect_schema_keys_from_query(query, &mut keys);
    dedup_strings(keys)
}

#[cfg(test)]
fn collect_schema_keys_from_query(query: &Query, keys: &mut Vec<String>) {
    if let Some(found) = extract_schema_keys_from_query(query) {
        keys.extend(found);
    }
    if let Some(with) = query.with.as_ref() {
        for cte in &with.cte_tables {
            collect_schema_keys_from_query(&cte.query, keys);
        }
    }
    collect_schema_keys_from_set_expr(&query.body, keys);
}

#[cfg(test)]
fn collect_schema_keys_from_set_expr(expr: &SetExpr, keys: &mut Vec<String>) {
    match expr {
        SetExpr::Select(select) => collect_schema_keys_from_select(select, keys),
        SetExpr::Query(query) => collect_schema_keys_from_query(query, keys),
        SetExpr::SetOperation { left, right, .. } => {
            collect_schema_keys_from_set_expr(left, keys);
            collect_schema_keys_from_set_expr(right, keys);
        }
        _ => {}
    }
}

#[cfg(test)]
fn collect_schema_keys_from_select(select: &Select, keys: &mut Vec<String>) {
    for table in &select.from {
        collect_schema_keys_from_table_factor(&table.relation, keys);
        for join in &table.joins {
            collect_schema_keys_from_table_factor(&join.relation, keys);
        }
    }
}

#[cfg(test)]
fn collect_schema_keys_from_table_factor(relation: &TableFactor, keys: &mut Vec<String>) {
    match relation {
        TableFactor::Derived { subquery, .. } => collect_schema_keys_from_query(subquery, keys),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_schema_keys_from_table_factor(&table_with_joins.relation, keys);
            for join in &table_with_joins.joins {
                collect_schema_keys_from_table_factor(&join.relation, keys);
            }
        }
        _ => {}
    }
}

pub(crate) fn extract_plugin_keys_from_query(query: &Query) -> Option<Vec<String>> {
    extract_column_keys_from_query(query, expr_is_plugin_key_column)
}

pub(crate) fn extract_plugin_keys_from_top_level_derived_subquery(
    query: &Query,
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
    extract_plugin_keys_from_query(subquery)
}

pub(crate) fn extract_pushdown_predicate(query: &Query) -> Option<Expr> {
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
) -> Option<Vec<String>> {
    extract_column_keys_from_set_expr(&query.body, is_target_column)
}

fn extract_column_keys_from_set_expr(
    expr: &SetExpr,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    match expr {
        SetExpr::Select(select) => extract_column_keys_from_select(select, is_target_column),
        SetExpr::Query(query) => extract_column_keys_from_set_expr(&query.body, is_target_column),
        SetExpr::SetOperation { left, right, .. } => {
            extract_column_keys_from_set_expr(left, is_target_column)
                .or_else(|| extract_column_keys_from_set_expr(right, is_target_column))
        }
        _ => None,
    }
}

fn extract_column_keys_from_select(
    select: &Select,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    select
        .selection
        .as_ref()
        .and_then(|expr| extract_column_keys_from_expr(expr, is_target_column))
}

fn extract_column_keys_from_expr(
    expr: &Expr,
    is_target_column: fn(&Expr) -> bool,
) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_target_column(left) {
                return string_literal_value(right).map(|value| vec![value]);
            }
            if is_target_column(right) {
                return string_literal_value(left).map(|value| vec![value]);
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => match (
            extract_column_keys_from_expr(left, is_target_column),
            extract_column_keys_from_expr(right, is_target_column),
        ) {
            (Some(left), Some(right)) => {
                let intersection = intersect_strings(&left, &right);
                if intersection.is_empty() {
                    None
                } else {
                    Some(intersection)
                }
            }
            (Some(keys), None) | (None, Some(keys)) => Some(keys),
            (None, None) => None,
        },
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => match (
            extract_column_keys_from_expr(left, is_target_column),
            extract_column_keys_from_expr(right, is_target_column),
        ) {
            (Some(left), Some(right)) => Some(union_strings(&left, &right)),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !is_target_column(expr) {
                return None;
            }
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let value = string_literal_value(item)?;
                values.push(value);
            }
            if values.is_empty() {
                None
            } else {
                Some(dedup_strings(values))
            }
        }
        Expr::Nested(inner) => extract_column_keys_from_expr(inner, is_target_column),
        _ => None,
    }
}

fn expr_is_schema_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "schema_key")
}

fn expr_is_plugin_key_column(expr: &Expr) -> bool {
    expr_last_identifier_eq(expr, "plugin_key")
}

fn expr_last_identifier_eq(expr: &Expr, target: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value.eq_ignore_ascii_case(target),
        Expr::CompoundIdentifier(idents) => idents
            .last()
            .map(|ident| ident.value.eq_ignore_ascii_case(target))
            .unwrap_or(false),
        _ => false,
    }
}

fn string_literal_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(value),
            ..
        }) => Some(value.clone()),
        _ => None,
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

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        if !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

fn union_strings(left: &[String], right: &[String]) -> Vec<String> {
    let mut out = left.to_vec();
    for value in right {
        if !out.contains(value) {
            out.push(value.clone());
        }
    }
    out
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

async fn fetch_materialized_schema_keys(backend: &dyn LixBackend) -> Result<Vec<String>, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => {
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'lix_internal_state_materialized_v1_%'"
        }
        SqlDialect::Postgres => {
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = current_schema() \
               AND table_type = 'BASE TABLE' \
               AND table_name LIKE 'lix_internal_state_materialized_v1_%'"
        }
    };
    let result = backend.execute(sql, &[]).await?;

    let mut keys = Vec::new();
    for row in &result.rows {
        let Some(LixValue::Text(name)) = row.first() else {
            continue;
        };
        let Some(schema_key) = name.strip_prefix(MATERIALIZED_PREFIX) else {
            continue;
        };
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
        untracked_table = quote_ident(UNTRACKED_TABLE),
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
