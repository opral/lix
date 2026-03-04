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
use crate::engine::sql::planning::rewrite_engine::steps::state_pushdown::StatePushdown;
use crate::engine::sql::planning::rewrite_engine::{
    escape_sql_string, object_name_matches, parse_single_query, quote_ident,
};
use crate::version::{
    version_descriptor_file_id, version_descriptor_schema_key, version_descriptor_storage_version_id,
    GLOBAL_VERSION_ID,
};
use crate::{errors, LixBackend, LixError, Value as LixValue};

const VTABLE_NAME: &str = "lix_internal_state_vtable";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

pub(crate) fn build_effective_state_by_version_query(
    pushdown: &StatePushdown,
    count_only: bool,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    if count_only {
        build_effective_state_by_version_count_query(pushdown)
    } else {
        build_effective_state_by_version_view_query(pushdown, include_commit_mapping)
    }
}

fn build_effective_state_by_version_view_query(
    pushdown: &StatePushdown,
    include_commit_mapping: bool,
) -> Result<Query, LixError> {
    let (target_version_pushdown, ranked_predicates) =
        split_effective_by_version_ranked_pushdown(pushdown);
    let source_pushdown = render_where_clause(&pushdown.source_predicates, " WHERE ");
    let ranked_pushdown = render_where_clause(&ranked_predicates, " AND ");
    let target_versions_cte =
        build_effective_state_target_versions_cte(&target_version_pushdown, VTABLE_NAME);
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let commit_ctes = if include_commit_mapping {
        format!(
            ", \
             commit_by_version AS ( \
               SELECT \
                 COALESCE(lix_json_extract(snapshot_content, 'id'), entity_id) AS commit_id, \
                 lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id \
               FROM {vtable_name} \
               WHERE schema_key = 'lix_commit' \
                 AND version_id = '{global_version}' \
                 AND snapshot_content IS NOT NULL \
             ), \
             change_set_element_by_version AS ( \
               SELECT \
                 lix_json_extract(snapshot_content, 'change_set_id') AS change_set_id, \
                 lix_json_extract(snapshot_content, 'change_id') AS change_id \
               FROM {vtable_name} \
               WHERE schema_key = 'lix_change_set_element' \
                 AND version_id = '{global_version}' \
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
             )",
            vtable_name = VTABLE_NAME,
            global_version = escape_sql_string(GLOBAL_VERSION_ID),
        )
    } else {
        String::new()
    };
    let commit_join = if include_commit_mapping {
        "LEFT JOIN change_commit_by_change_id cc \
             ON cc.change_id = s.change_id"
            .to_string()
    } else {
        String::new()
    };
    let commit_expr = if include_commit_mapping {
        "COALESCE(cc.commit_id, CASE WHEN s.untracked = true THEN 'untracked' ELSE NULL END) \
             AS commit_id"
            .to_string()
    } else {
        "CASE WHEN s.untracked = true THEN 'untracked' ELSE NULL END AS commit_id".to_string()
    };
    let sql = format!(
        "SELECT \
             ranked.entity_id AS entity_id, \
             ranked.schema_key AS schema_key, \
             ranked.file_id AS file_id, \
             ranked.version_id AS version_id, \
             ranked.plugin_key AS plugin_key, \
             ranked.snapshot_content AS snapshot_content, \
             ranked.schema_version AS schema_version, \
             ranked.created_at AS created_at, \
             ranked.updated_at AS updated_at, \
             ranked.inherited_from_version_id AS inherited_from_version_id, \
             ranked.change_id AS change_id, \
             ranked.commit_id AS commit_id, \
             ranked.untracked AS untracked, \
             ranked.writer_key AS writer_key, \
             ranked.metadata AS metadata \
         FROM ( \
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_extract(snapshot_content, 'id') AS version_id, \
                 lix_json_extract(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ) \
             {commit_ctes} \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             vc.target_version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             s.schema_version AS schema_version, \
             s.created_at AS created_at, \
             s.updated_at AS updated_at, \
             CASE \
               WHEN s.inherited_from_version_id IS NOT NULL THEN s.inherited_from_version_id \
               WHEN vc.depth = 0 THEN NULL \
               ELSE s.version_id \
             END AS inherited_from_version_id, \
             s.change_id AS change_id, \
             {commit_expr}, \
             s.untracked AS untracked, \
             s.writer_key AS writer_key, \
             s.metadata AS metadata, \
             ROW_NUMBER() OVER ( \
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
           {commit_join} \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
        target_versions_cte = target_versions_cte,
        commit_ctes = commit_ctes,
        commit_expr = commit_expr,
        commit_join = commit_join,
    );
    parse_single_query(&sql)
}

fn build_effective_state_by_version_count_query(
    pushdown: &StatePushdown,
) -> Result<Query, LixError> {
    let (target_version_pushdown, ranked_predicates) =
        split_effective_by_version_ranked_pushdown(pushdown);
    let source_pushdown = render_where_clause(&pushdown.source_predicates, " WHERE ");
    let ranked_pushdown = render_where_clause(&ranked_predicates, " AND ");
    let target_versions_cte =
        build_effective_state_target_versions_cte(&target_version_pushdown, VTABLE_NAME);
    let descriptor_table = quote_ident(&format!(
        "lix_internal_state_materialized_v1_{}",
        version_descriptor_schema_key()
    ));
    let sql = format!(
        "SELECT \
             ranked.entity_id AS entity_id \
         FROM ( \
           WITH RECURSIVE \
             version_descriptor AS ( \
               SELECT \
                 lix_json_extract(snapshot_content, 'id') AS version_id, \
                 lix_json_extract(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
               FROM {descriptor_table} \
               WHERE schema_key = '{descriptor_schema_key}' \
                 AND file_id = '{descriptor_file_id}' \
                 AND version_id = '{descriptor_storage_version_id}' \
                 AND is_tombstone = 0 \
                 AND snapshot_content IS NOT NULL \
             ), \
             {target_versions_cte}, \
             version_chain(target_version_id, ancestor_version_id, depth) AS ( \
               SELECT \
                 version_id AS target_version_id, \
                 version_id AS ancestor_version_id, \
                 0 AS depth \
               FROM target_versions \
               UNION ALL \
               SELECT \
                 vc.target_version_id, \
                 vd.inherits_from_version_id AS ancestor_version_id, \
                 vc.depth + 1 AS depth \
               FROM version_chain vc \
               JOIN version_descriptor vd \
                 ON vd.version_id = vc.ancestor_version_id \
               WHERE vd.inherits_from_version_id IS NOT NULL \
                 AND vc.depth < 64 \
             ) \
           SELECT \
             s.entity_id AS entity_id, \
             s.schema_key AS schema_key, \
             s.file_id AS file_id, \
             vc.target_version_id AS version_id, \
             s.plugin_key AS plugin_key, \
             s.snapshot_content AS snapshot_content, \
             ROW_NUMBER() OVER ( \
               PARTITION BY vc.target_version_id, s.entity_id, s.schema_key, s.file_id \
               ORDER BY vc.depth ASC \
             ) AS rn \
           FROM {vtable_name} s \
           JOIN version_chain vc \
             ON vc.ancestor_version_id = s.version_id \
           {source_pushdown} \
         ) AS ranked \
         WHERE ranked.rn = 1 \
           AND ranked.snapshot_content IS NOT NULL\
           {ranked_pushdown}",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        vtable_name = VTABLE_NAME,
        source_pushdown = source_pushdown,
        ranked_pushdown = ranked_pushdown,
        target_versions_cte = target_versions_cte,
    );
    parse_single_query(&sql)
}

fn build_effective_state_target_versions_cte(
    target_version_pushdown: &[Expr],
    vtable_name: &str,
) -> String {
    if target_version_pushdown.is_empty() {
        return format!(
            "all_target_versions AS ( \
               SELECT version_id FROM version_descriptor \
               UNION \
               SELECT DISTINCT version_id FROM {vtable_name} \
             ), \
             target_versions AS ( \
               SELECT version_id \
               FROM all_target_versions \
             )",
            vtable_name = vtable_name
        );
    }

    let target_version_filter = join_predicates(target_version_pushdown);
    if target_version_pushdown
        .iter()
        .any(predicate_contains_placeholder_or_subquery)
    {
        return format!(
            "all_target_versions AS ( \
               SELECT version_id FROM version_descriptor \
               UNION \
               SELECT DISTINCT version_id FROM {vtable_name} \
             ), \
             target_versions AS ( \
               SELECT version_id \
               FROM all_target_versions \
               WHERE {target_version_filter} \
             )",
            vtable_name = vtable_name,
            target_version_filter = target_version_filter
        );
    }

    format!(
        "target_versions AS ( \
           SELECT version_id \
           FROM version_descriptor \
           WHERE {target_version_filter} \
           UNION \
           SELECT DISTINCT version_id \
           FROM {vtable_name} \
           WHERE {target_version_filter} \
         )",
        target_version_filter = target_version_filter,
        vtable_name = vtable_name
    )
}

fn split_effective_by_version_ranked_pushdown(
    pushdown: &StatePushdown,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut target_version = Vec::new();
    let mut ranked = Vec::new();
    for predicate in &pushdown.ranked_predicates {
        if let Some(rewritten) = extract_target_version_pushdown_predicate(predicate) {
            target_version.push(rewritten);
            continue;
        }
        ranked.push(predicate.clone());
    }
    (target_version, ranked)
}

fn extract_target_version_pushdown_predicate(predicate: &Expr) -> Option<Expr> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_ranked_version_expr(left) {
                return Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                    op: BinaryOperator::Eq,
                    right: right.clone(),
                });
            }
            if is_ranked_version_expr(right) {
                return Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                    op: BinaryOperator::Eq,
                    right: left.clone(),
                });
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if !is_ranked_version_expr(expr) {
                return None;
            }
            Some(Expr::InList {
                expr: Box::new(Expr::Identifier(Ident::new("version_id"))),
                list: list.clone(),
                negated: false,
            })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            if !is_ranked_version_expr(expr) {
                return None;
            }
            Some(Expr::InSubquery {
                expr: Box::new(Expr::Identifier(Ident::new("version_id"))),
                subquery: subquery.clone(),
                negated: false,
            })
        }
        Expr::IsNull(expr) => {
            if !is_ranked_version_expr(expr) {
                return None;
            }
            Some(Expr::IsNull(Box::new(Expr::Identifier(Ident::new(
                "version_id",
            )))))
        }
        Expr::IsNotNull(expr) => {
            if !is_ranked_version_expr(expr) {
                return None;
            }
            Some(Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new(
                "version_id",
            )))))
        }
        _ => None,
    }
}

fn is_ranked_version_expr(expr: &Expr) -> bool {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            let column = &parts[parts.len() - 1].value;
            qualifier.eq_ignore_ascii_case("ranked") && column.eq_ignore_ascii_case("version_id")
        }
        Expr::Nested(inner) => is_ranked_version_expr(inner),
        _ => false,
    }
}

fn render_where_clause(predicates: &[Expr], prefix: &str) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("{prefix}{}", join_predicates(predicates))
    }
}

fn join_predicates(predicates: &[Expr]) -> String {
    predicates
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn predicate_contains_placeholder_or_subquery(predicate: &Expr) -> bool {
    if predicate.to_string().contains('?') {
        return true;
    }
    predicate_contains_subquery(predicate)
}

fn predicate_contains_subquery(predicate: &Expr) -> bool {
    match predicate {
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            predicate_contains_subquery(left) || predicate_contains_subquery(right)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => predicate_contains_subquery(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            predicate_contains_subquery(expr)
                || predicate_contains_subquery(low)
                || predicate_contains_subquery(high)
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            predicate_contains_subquery(expr) || predicate_contains_subquery(pattern)
        }
        Expr::InList { expr, list, .. } => {
            predicate_contains_subquery(expr)
                || list.iter().any(predicate_contains_subquery)
        }
        Expr::Tuple(items) => items.iter().any(predicate_contains_subquery),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand
                .as_ref()
                .is_some_and(|value| predicate_contains_subquery(value))
                || conditions.iter().any(|condition| {
                    predicate_contains_subquery(&condition.condition)
                        || predicate_contains_subquery(&condition.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|value| predicate_contains_subquery(value))
        }
        _ => false,
    }
}

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
                created_at, updated_at, NULL AS inherited_from_version_id, 'untracked' AS change_id, writer_key, true AS untracked, 1 AS priority \
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
                    created_at, updated_at, inherited_from_version_id, change_id, writer_key, false AS untracked, 2 AS priority \
             FROM {materialized}{materialized_where}",
            materialized = materialized_ident,
            materialized_where = materialized_where
        ));
    }

    let union_sql = union_parts.join(" UNION ALL ");

    let sql = format!(
        "SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                created_at, updated_at, inherited_from_version_id, change_id, writer_key, untracked \
         FROM (\
             SELECT entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, \
                    created_at, updated_at, inherited_from_version_id, change_id, writer_key, untracked, \
                    ROW_NUMBER() OVER (PARTITION BY entity_id, schema_key, file_id, version_id ORDER BY priority) AS rn \
             FROM ({union_sql}) AS lix_state_union\
         ) AS lix_state_ranked \
         WHERE rn = 1",
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
             FROM lix_internal_state_materialized_v1_lix_stored_schema \
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

#[cfg(test)]
mod tests {
    use super::{
        build_untracked_union_query, extract_plugin_keys_from_query,
        extract_plugin_keys_from_top_level_derived_subquery, extract_pushdown_predicate,
    };
    use crate::engine::sql::planning::rewrite_engine::preprocess_sql_rewrite_only as preprocess_sql;
    use sqlparser::ast::{Query, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn compact_sql(sql: &str) -> String {
        sql.chars().filter(|c| !c.is_whitespace()).collect()
    }

    fn union_segment(sql: &str) -> &str {
        let end = sql
            .find(")ASlix_state_union")
            .expect("union segment end not found");
        let start = sql[..end]
            .rfind("FROM(")
            .expect("union segment start not found");
        &sql[start + 5..end]
    }

    fn assert_branch_contains_all(sql: &str, table_marker: &str, needles: &[&str]) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        for needle in needles {
            assert!(
                branch.contains(needle),
                "expected branch for {table_marker} to contain {needle}, got: {branch}"
            );
        }
    }

    fn assert_branch_not_contains(sql: &str, table_marker: &str, needle: &str) {
        let union_sql = union_segment(sql);
        let start = union_sql
            .find(table_marker)
            .or_else(|| union_sql.find(&table_marker.replace('"', "")))
            .expect("table marker not found");
        let slice = &union_sql[start..];
        let end = slice.find("UNIONALL").unwrap_or(slice.len());
        let branch = &slice[..end];
        assert!(
            !branch.contains(needle),
            "expected branch for {table_marker} to not contain {needle}, got: {branch}"
        );
    }

    #[test]
    fn rewrite_pushes_down_predicates_for_schema_key_in() {
        let sql = "SELECT * FROM lix_internal_state_vtable WHERE schema_key IN ('schema_a', 'schema_b') AND entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'",
            ],
        );
    }

    #[test]
    fn rewrite_pushes_down_like_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id LIKE 'entity-%'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idLIKE'entity-%'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idLIKE'entity-%'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_or_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key IN ('schema_a', 'schema_b') \
            AND (entity_id = 'entity-1' OR file_id = 'file-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_keyIN('schema_a','schema_b')",
                "entity_id='entity-1'ORfile_id='file-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_id='entity-1'ORfile_id='file-1'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_b"#,
            &["entity_id='entity-1'ORfile_id='file-1'"],
        );
    }

    #[test]
    fn rewrite_skips_or_with_non_schema_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' OR entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert!(
            !compact.contains("lix_internal_state_untracked"),
            "expected no rewrite for OR with non-schema predicate, got: {compact}"
        );
    }

    #[test]
    fn rewrite_does_not_pushdown_qualified_identifiers() {
        let sql = "SELECT * FROM lix_internal_state_vtable AS a \
            WHERE a.schema_key = 'schema_a' AND a.entity_id = 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_keyIN('schema_a')"],
        );
        assert_branch_not_contains(
            &compact,
            "FROMlix_internal_state_untracked",
            "entity_id='entity-1'",
        );
        assert_branch_not_contains(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            "entity_id='entity-1'",
        );
    }

    #[test]
    fn rewrite_pushes_down_comparison_predicates() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND file_id >= 'file-2' AND entity_id <> 'entity-1'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "file_id>='file-2'",
                "entity_id<>'entity-1'",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["file_id>='file-2'", "entity_id<>'entity-1'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_in_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id NOT IN ('entity-1', 'entity-2')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &[
                "schema_key='schema_a'",
                "entity_idNOTIN('entity-1','entity-2')",
            ],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idNOTIN('entity-1','entity-2')"],
        );
    }

    #[test]
    fn rewrite_pushes_down_is_null_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND snapshot_content IS NULL";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "snapshot_contentISNULL"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["snapshot_contentISNULL"],
        );
    }

    #[test]
    fn rewrite_pushes_down_between_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND entity_id BETWEEN 'a' AND 'm'";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "entity_idBETWEEN'a'AND'm'"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["entity_idBETWEEN'a'AND'm'"],
        );
    }

    #[test]
    fn rewrite_pushes_down_not_predicate() {
        let sql = "SELECT * FROM lix_internal_state_vtable \
            WHERE schema_key = 'schema_a' AND NOT (entity_id = 'entity-1')";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert_branch_contains_all(
            &compact,
            "FROMlix_internal_state_untracked",
            &["schema_key='schema_a'", "NOT(entity_id='entity-1')"],
        );
        assert_branch_contains_all(
            &compact,
            r#"FROM"lix_internal_state_materialized_v1_schema_a"#,
            &["NOT(entity_id='entity-1')"],
        );
    }

    #[test]
    fn extracts_plugin_keys_from_eq_and_in_list() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR plugin_key IN ('plugin_text', 'plugin_json')",
        );
        let keys =
            extract_plugin_keys_from_query(&query, &[]).expect("plugin keys should be extracted");
        assert_eq!(
            keys,
            vec!["plugin_json".to_string(), "plugin_text".to_string()]
        );
    }

    #[test]
    fn extracts_plugin_keys_from_qualified_identifier() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable AS s WHERE s.plugin_key = 'plugin_json'",
        );
        let keys =
            extract_plugin_keys_from_query(&query, &[]).expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn extracts_plugin_keys_from_derived_subquery_filter() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM (SELECT * FROM lix_internal_state_vtable WHERE plugin_key = 'plugin_json') AS ranked",
        );
        let keys = extract_plugin_keys_from_top_level_derived_subquery(&query, &[])
            .expect("plugin keys should be extracted");
        assert_eq!(keys, vec!["plugin_json".to_string()]);
    }

    #[test]
    fn plugin_key_extraction_skips_mixed_or_predicate() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE plugin_key = 'plugin_json' OR schema_key = 'json_pointer'",
        );
        assert!(
            extract_plugin_keys_from_query(&query, &[]).is_none(),
            "mixed OR should not produce a plugin-only key set"
        );
    }

    #[test]
    fn rewrites_vtable_in_expression_subquery() {
        let sql = "SELECT 1 \
             WHERE EXISTS ( \
                 SELECT 1 \
                 FROM lix_internal_state_vtable \
                 WHERE schema_key = 'schema_a' \
             )";
        let output = preprocess_sql(sql).expect("preprocess_sql");
        let compact = compact_sql(&output.sql);

        assert!(
            !compact.contains("FROMlix_internal_state_vtable"),
            "expected nested vtable in expression subquery to be lowered, got: {compact}"
        );
        assert!(
            compact.contains("lix_internal_state_untracked"),
            "expected lowered query to include untracked branch, got: {compact}"
        );
    }

    #[test]
    fn narrows_materialized_union_to_schema_predicate_intersection() {
        let query = parse_query(
            "SELECT * FROM lix_internal_state_vtable \
             WHERE schema_key = 'schema_a' AND entity_id = 'entity-1'",
        );
        let predicate = extract_pushdown_predicate(&query).expect("predicate");
        let derived = build_untracked_union_query(
            &[
                "schema_a".to_string(),
                "schema_b".to_string(),
                "schema_c".to_string(),
            ],
            Some(&predicate),
            &[],
        )
        .expect("derived query");
        let compact = compact_sql(&derived.to_string());

        assert!(compact.contains(r#"lix_internal_state_materialized_v1_schema_a"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_b"#));
        assert!(!compact.contains(r#"lix_internal_state_materialized_v1_schema_c"#));
    }

    fn parse_query(sql: &str) -> Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
