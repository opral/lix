use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, ObjectNamePart, Query, Select, TableFactor, TableWithJoins,
};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, escape_sql_string, parse_single_query, quote_ident, rewrite_query_selects,
    visit_query_selects, visit_table_factors_in_select, RewriteDecision,
};
use crate::{LixBackend, LixError};

use super::target::{
    projected_lixcol_aliases_for_variant, resolve_target_from_object_name,
    resolve_targets_with_backend, EntityViewTarget, EntityViewVariant,
};

#[derive(Debug, Clone, Default)]
struct HistoryPredicatePushdown {
    predicates: Vec<HistoryPredicate>,
}

#[derive(Debug, Clone)]
enum HistoryPredicate {
    Binary {
        source_column: &'static str,
        operator: BinaryOperator,
        rhs_sql: String,
    },
    InSubquery {
        source_column: &'static str,
        subquery_sql: String,
        negated: bool,
    },
    InList {
        source_column: &'static str,
        list_sql: Vec<String>,
        negated: bool,
    },
    IsNull {
        source_column: &'static str,
        negated: bool,
    },
}

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_resolver(query, &mut |name| resolve_target_from_object_name(name))
}

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    let view_names = collect_table_view_names(&query);
    if view_names.is_empty() {
        return Ok(None);
    }

    let resolved = resolve_targets_with_backend(backend, &view_names).await?;
    rewrite_query_with_resolver(query, &mut |name| {
        let Some(view_name) = object_name_terminal(name) else {
            return Ok(None);
        };
        let key = view_name.to_ascii_lowercase();
        Ok(resolved.get(&key).cloned())
    })
}

fn rewrite_query_with_resolver(
    query: Query,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<Option<Query>, LixError> {
    let mut rewrite_select_with_resolver = |select: &mut Select| rewrite_select(select, resolver);
    rewrite_query_selects(query, &mut rewrite_select_with_resolver)
}

fn rewrite_select(
    select: &mut Select,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<RewriteDecision, LixError> {
    let mut changed = RewriteDecision::Unchanged;
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    let selection = select.selection.clone();
    for table in &mut select.from {
        if rewrite_table_with_joins(table, &selection, allow_unqualified, resolver)?
            == RewriteDecision::Changed
        {
            changed = RewriteDecision::Changed;
        }
    }
    Ok(changed)
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    selection: &Option<Expr>,
    allow_unqualified: bool,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<RewriteDecision, LixError> {
    let mut changed =
        rewrite_table_factor(&mut table.relation, selection, allow_unqualified, resolver)?;
    for join in &mut table.joins {
        if rewrite_table_factor(&mut join.relation, selection, false, resolver)?
            == RewriteDecision::Changed
        {
            changed = RewriteDecision::Changed;
        }
    }
    Ok(changed)
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &Option<Expr>,
    allow_unqualified: bool,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<RewriteDecision, LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(target) = resolver(name)? else {
                return Ok(RewriteDecision::Unchanged);
            };

            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| target.view_name.clone());
            let pushdown = collect_history_pushdown_predicates(
                selection,
                &relation_name,
                allow_unqualified,
                target.variant,
            );
            let derived_query = build_entity_view_query(&target, &pushdown)?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_alias(&target.view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(RewriteDecision::Changed)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => rewrite_table_with_joins(table_with_joins, selection, allow_unqualified, resolver),
        _ => Ok(RewriteDecision::Unchanged),
    }
}

fn build_entity_view_query(
    target: &EntityViewTarget,
    pushdown: &HistoryPredicatePushdown,
) -> Result<Query, LixError> {
    let (source, extra_predicates) = match target.variant {
        EntityViewVariant::Base => {
            base_effective_state_source(target.version_id_override.as_deref())
        }
        EntityViewVariant::ByVersion => ("lix_state_by_version".to_string(), Vec::new()),
        EntityViewVariant::History => ("lix_state_history".to_string(), Vec::new()),
    };

    let mut select_parts = Vec::new();
    for property in &target.properties {
        select_parts.push(format!(
            "lix_json_extract(snapshot_content, '{property}') AS {alias}",
            property = escape_sql_string(property),
            alias = quote_ident(property),
        ));
    }
    for (column, alias) in projected_lixcol_aliases_for_variant(target.variant) {
        select_parts.push(format!("{column} AS {alias}"));
    }

    let mut predicates = vec![format!(
        "schema_key = '{schema_key}'",
        schema_key = escape_sql_string(&target.schema_key),
    )];
    predicates.extend(extra_predicates);
    predicates.extend(render_history_pushdown_predicates(pushdown));
    predicates.extend(override_predicates(target));

    let sql = format!(
        "SELECT {projection} \
         FROM {source} \
         WHERE {predicate}",
        projection = select_parts.join(", "),
        source = source,
        predicate = predicates.join(" AND "),
    );
    parse_single_query(&sql)
}

fn collect_history_pushdown_predicates(
    selection: &Option<Expr>,
    relation_name: &str,
    allow_unqualified: bool,
    variant: EntityViewVariant,
) -> HistoryPredicatePushdown {
    if variant != EntityViewVariant::History {
        return HistoryPredicatePushdown::default();
    }

    let mut pushdown = HistoryPredicatePushdown::default();
    let Some(expr) = selection.as_ref() else {
        return pushdown;
    };
    collect_history_pushdown_predicates_from_expr(
        expr,
        relation_name,
        allow_unqualified,
        &mut pushdown.predicates,
    );
    pushdown
}

fn collect_history_pushdown_predicates_from_expr(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
    predicates: &mut Vec<HistoryPredicate>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_history_pushdown_predicates_from_expr(
                left,
                relation_name,
                allow_unqualified,
                predicates,
            );
            collect_history_pushdown_predicates_from_expr(
                right,
                relation_name,
                allow_unqualified,
                predicates,
            );
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(source_column) =
                extract_history_source_column(left, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::Binary {
                    source_column,
                    operator: op.clone(),
                    rhs_sql: right.to_string(),
                });
            } else if let Some(source_column) =
                extract_history_source_column(right, relation_name, allow_unqualified)
            {
                if let Some(inverted) = invert_binary_operator(op.clone()) {
                    predicates.push(HistoryPredicate::Binary {
                        source_column,
                        operator: inverted,
                        rhs_sql: left.to_string(),
                    });
                }
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            if let Some(source_column) =
                extract_history_source_column(expr, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::InSubquery {
                    source_column,
                    subquery_sql: subquery.to_string(),
                    negated: *negated,
                });
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if let Some(source_column) =
                extract_history_source_column(expr, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::InList {
                    source_column,
                    list_sql: list.iter().map(ToString::to_string).collect(),
                    negated: *negated,
                });
            }
        }
        Expr::IsNull(inner) => {
            if let Some(source_column) =
                extract_history_source_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::IsNull {
                    source_column,
                    negated: false,
                });
            }
        }
        Expr::IsNotNull(inner) => {
            if let Some(source_column) =
                extract_history_source_column(inner, relation_name, allow_unqualified)
            {
                predicates.push(HistoryPredicate::IsNull {
                    source_column,
                    negated: true,
                });
            }
        }
        Expr::Nested(value) => collect_history_pushdown_predicates_from_expr(
            value,
            relation_name,
            allow_unqualified,
            predicates,
        ),
        _ => {}
    }
}

fn extract_history_source_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<&'static str> {
    let column = match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            if !parts[0].value.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            parts[1].value.as_str()
        }
        Expr::Identifier(identifier) if allow_unqualified => identifier.value.as_str(),
        _ => return None,
    };

    match column {
        "lixcol_root_commit_id" | "root_commit_id" => Some("root_commit_id"),
        "lixcol_version_id" | "version_id" => Some("version_id"),
        _ => None,
    }
}

fn invert_binary_operator(op: BinaryOperator) -> Option<BinaryOperator> {
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

fn render_history_pushdown_predicates(pushdown: &HistoryPredicatePushdown) -> Vec<String> {
    pushdown
        .predicates
        .iter()
        .map(|predicate| match predicate {
            HistoryPredicate::Binary {
                source_column,
                operator,
                rhs_sql,
            } => format!(
                "{column} {op} {rhs}",
                column = source_column,
                op = operator,
                rhs = rhs_sql
            ),
            HistoryPredicate::InSubquery {
                source_column,
                subquery_sql,
                negated,
            } => {
                let not_sql = if *negated { " NOT" } else { "" };
                format!(
                    "{column}{not_sql} IN ({subquery})",
                    column = source_column,
                    not_sql = not_sql,
                    subquery = subquery_sql
                )
            }
            HistoryPredicate::InList {
                source_column,
                list_sql,
                negated,
            } => {
                let not_sql = if *negated { " NOT" } else { "" };
                format!(
                    "{column}{not_sql} IN ({list})",
                    column = source_column,
                    not_sql = not_sql,
                    list = list_sql.join(", ")
                )
            }
            HistoryPredicate::IsNull {
                source_column,
                negated,
            } => {
                let is_not = if *negated { " NOT" } else { "" };
                format!(
                    "{column} IS{is_not} NULL",
                    column = source_column,
                    is_not = is_not
                )
            }
        })
        .collect()
}

fn base_effective_state_source(version_id_override: Option<&str>) -> (String, Vec<String>) {
    match version_id_override {
        // Base entity views are effective-state wrappers by default.
        // If schema metadata pins a version, route through by-version with
        // that explicit target-version filter.
        Some(version_id) => (
            "lix_state_by_version".to_string(),
            vec![format!("version_id = '{}'", escape_sql_string(version_id))],
        ),
        None => ("lix_state".to_string(), Vec::new()),
    }
}

fn override_predicates(target: &EntityViewTarget) -> Vec<String> {
    target
        .override_predicates
        .iter()
        .map(|predicate| match &predicate.value {
            JsonValue::Null => format!("{column} IS NULL", column = predicate.column),
            value => format!(
                "{column} = {literal}",
                column = predicate.column,
                literal = render_literal(value)
            ),
        })
        .collect()
}

fn render_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => format!("'{}'", escape_sql_string(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            format!("'{}'", escape_sql_string(&value.to_string()))
        }
    }
}

fn collect_table_view_names(query: &Query) -> Vec<String> {
    let mut view_names = Vec::new();
    let _ = visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            if let Some(view_name) = object_name_terminal(name) {
                view_names.push(view_name);
            }
            Ok(())
        })
    });
    view_names
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}
