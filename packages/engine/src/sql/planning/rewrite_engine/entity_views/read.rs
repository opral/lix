use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem, SetExpr,
    TableFactor, TableWithJoins, Value,
};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, parse_single_query, rewrite_query_selects, visit_query_selects,
    visit_table_factors_in_select, RewriteDecision,
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
        rhs: Expr,
    },
    InSubquery {
        source_column: &'static str,
        subquery: Query,
        negated: bool,
    },
    InList {
        source_column: &'static str,
        list: Vec<Expr>,
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

    let mut query = parse_single_query("SELECT 1 FROM lix_state")?;
    let select = select_mut(&mut query)?;
    select.from = vec![TableWithJoins {
        relation: table_factor(&source),
        joins: Vec::new(),
    }];

    let mut projection = Vec::new();
    for property in &target.properties {
        projection.push(SelectItem::ExprWithAlias {
            expr: json_extract_expr("snapshot_content", property)?,
            alias: Ident::new(property),
        });
    }
    for (column, alias) in projected_lixcol_aliases_for_variant(target.variant) {
        projection.push(SelectItem::ExprWithAlias {
            expr: Expr::Identifier(Ident::new(*column)),
            alias: Ident::new(*alias),
        });
    }
    select.projection = projection;

    let mut predicates = vec![Expr::BinaryOp {
        left: Box::new(Expr::Identifier(Ident::new("schema_key"))),
        op: BinaryOperator::Eq,
        right: Box::new(string_literal_expr(&target.schema_key)),
    }];
    predicates.extend(extra_predicates);
    predicates.extend(render_history_pushdown_predicates(pushdown));
    predicates.extend(override_predicates(target));
    select.selection = join_with_and(predicates);

    Ok(query)
}

fn table_factor(table: &str) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(table))]),
        alias: None,
        args: None,
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

fn json_extract_expr(column: &str, key: &str) -> Result<Expr, LixError> {
    let query = parse_single_query(&format!(
        "SELECT lix_json_extract({column}, '{key}')",
        column = column,
        key = key.replace('\'', "''"),
    ))?;
    let SetExpr::Select(select) = *query.body else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT when parsing lix_json_extract expression".to_string(),
        });
    };
    let Some(item) = select.projection.first() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected projection when parsing lix_json_extract expression".to_string(),
        });
    };
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Ok(expr.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "unexpected projection shape for lix_json_extract expression".to_string(),
        }),
    }
}

fn string_literal_expr(value: &str) -> Expr {
    Expr::Value(Value::SingleQuotedString(value.to_string()).into())
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
                    rhs: (*right.clone()),
                });
            } else if let Some(source_column) =
                extract_history_source_column(right, relation_name, allow_unqualified)
            {
                if let Some(inverted) = invert_binary_operator(op.clone()) {
                    predicates.push(HistoryPredicate::Binary {
                        source_column,
                        operator: inverted,
                        rhs: (*left.clone()),
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
                    subquery: (*subquery.clone()),
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
                    list: list.clone(),
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

fn render_history_pushdown_predicates(pushdown: &HistoryPredicatePushdown) -> Vec<Expr> {
    pushdown
        .predicates
        .iter()
        .map(|predicate| match predicate {
            HistoryPredicate::Binary {
                source_column,
                operator,
                rhs,
            } => Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new(*source_column))),
                op: operator.clone(),
                right: Box::new(rhs.clone()),
            },
            HistoryPredicate::InSubquery {
                source_column,
                subquery,
                negated,
            } => Expr::InSubquery {
                expr: Box::new(Expr::Identifier(Ident::new(*source_column))),
                subquery: Box::new(subquery.clone()),
                negated: *negated,
            },
            HistoryPredicate::InList {
                source_column,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(Expr::Identifier(Ident::new(*source_column))),
                list: list.clone(),
                negated: *negated,
            },
            HistoryPredicate::IsNull {
                source_column,
                negated,
            } => {
                if *negated {
                    Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new(*source_column))))
                } else {
                    Expr::IsNull(Box::new(Expr::Identifier(Ident::new(*source_column))))
                }
            }
        })
        .collect()
}

fn base_effective_state_source(version_id_override: Option<&str>) -> (String, Vec<Expr>) {
    match version_id_override {
        // Base entity views are effective-state wrappers by default.
        // If schema metadata pins a version, route through by-version with
        // that explicit target-version filter.
        Some(version_id) => (
            "lix_state_by_version".to_string(),
            vec![Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("version_id"))),
                op: BinaryOperator::Eq,
                right: Box::new(string_literal_expr(version_id)),
            }],
        ),
        None => ("lix_state".to_string(), Vec::new()),
    }
}

fn override_predicates(target: &EntityViewTarget) -> Vec<Expr> {
    target
        .override_predicates
        .iter()
        .map(|predicate| match &predicate.value {
            JsonValue::Null => Expr::IsNull(Box::new(Expr::Identifier(Ident::new(
                &predicate.column,
            )))),
            value => Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new(&predicate.column))),
                op: BinaryOperator::Eq,
                right: Box::new(render_literal_expr(value)),
            },
        })
        .collect()
}

fn render_literal_expr(value: &JsonValue) -> Expr {
    match value {
        JsonValue::Null => Expr::Value(Value::Null.into()),
        JsonValue::Bool(value) => Expr::Value(Value::Boolean(*value).into()),
        JsonValue::Number(value) => Expr::Value(Value::Number(value.to_string(), false).into()),
        JsonValue::String(value) => Expr::Value(Value::SingleQuotedString(value.clone()).into()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Expr::Value(Value::SingleQuotedString(value.to_string()).into())
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

fn join_with_and(mut predicates: Vec<Expr>) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }
    let mut current = predicates.remove(0);
    for predicate in predicates {
        current = Expr::BinaryOp {
            left: Box::new(current),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        };
    }
    Some(current)
}

fn select_mut(query: &mut Query) -> Result<&mut Select, LixError> {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected SELECT body when rewriting entity view".to_string(),
        });
    };
    Ok(select.as_mut())
}
