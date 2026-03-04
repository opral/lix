use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
};

#[derive(Default, Clone)]
pub(crate) struct StatePushdown {
    pub(crate) source_predicates: Vec<Expr>,
    pub(crate) ranked_predicates: Vec<Expr>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PushdownBucket {
    Source = 0,
    Ranked = 1,
}

struct PredicatePart {
    predicate: Expr,
    extracted: Option<(PushdownBucket, Expr)>,
}

pub(crate) fn select_supports_count_fast_path(select: &sqlparser::ast::Select) -> bool {
    if select.projection.len() != 1 {
        return false;
    }
    let projection_normalized = select.projection[0]
        .to_string()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    if projection_normalized != "count(*)" {
        return false;
    }

    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return false;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !exprs.is_empty() || !modifiers.is_empty() {
                return false;
            }
        }
        GroupByExpr::All(_) => return false,
    }

    select.from.len() == 1 && select.from[0].joins.is_empty()
}

pub(crate) fn take_pushdown_predicates(
    selection: &mut Option<Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> StatePushdown {
    let Some(selection_expr) = selection.take() else {
        return StatePushdown::default();
    };

    let mut parts = Vec::new();
    for predicate in split_conjunction(selection_expr) {
        let extracted = extract_pushdown_predicate(&predicate, relation_name, allow_unqualified)
            .and_then(|(column, predicate_expr)| match column.as_str() {
                "entity_id" | "schema_key" | "file_id" => {
                    Some((PushdownBucket::Source, qualify_pushdown_predicate(predicate_expr, "s")))
                }
                "version_id" => Some((
                    PushdownBucket::Ranked,
                    qualify_pushdown_predicate(predicate_expr, "ranked"),
                )),
                "plugin_key" => {
                    // Keep plugin filtering after winner selection to preserve row-choice semantics.
                    Some((
                        PushdownBucket::Ranked,
                        qualify_pushdown_predicate(predicate_expr, "ranked"),
                    ))
                }
                _ => None,
            });
        parts.push(PredicatePart {
            predicate,
            extracted,
        });
    }

    let mut pushdown = StatePushdown::default();
    let mut remaining = Vec::new();
    for part in parts {
        match part.extracted {
            Some((bucket, expr)) => match bucket {
                PushdownBucket::Source => pushdown.source_predicates.push(expr),
                PushdownBucket::Ranked => pushdown.ranked_predicates.push(expr),
            },
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
}

pub(crate) fn retarget_pushdown_predicates(
    predicates: &[Expr],
    from_qualifier: &str,
    to_qualifier: &str,
) -> Vec<Expr> {
    predicates
        .iter()
        .map(|predicate| {
            retarget_pushdown_expr(predicate.clone(), from_qualifier, to_qualifier)
        })
        .collect()
}

fn retarget_pushdown_expr(mut expr: Expr, from_qualifier: &str, to_qualifier: &str) -> Expr {
    match &mut expr {
        Expr::BinaryOp { left, right, .. } => {
            *left = Box::new(retarget_pushdown_expr(
                (**left).clone(),
                from_qualifier,
                to_qualifier,
            ));
            *right = Box::new(retarget_pushdown_expr(
                (**right).clone(),
                from_qualifier,
                to_qualifier,
            ));
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Nested(expr) => {
            *expr = Box::new(retarget_pushdown_expr(
                (**expr).clone(),
                from_qualifier,
                to_qualifier,
            ));
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            *expr = Box::new(retarget_pushdown_expr(
                (**expr).clone(),
                from_qualifier,
                to_qualifier,
            ));
            *low = Box::new(retarget_pushdown_expr(
                (**low).clone(),
                from_qualifier,
                to_qualifier,
            ));
            *high = Box::new(retarget_pushdown_expr(
                (**high).clone(),
                from_qualifier,
                to_qualifier,
            ));
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            *expr = Box::new(retarget_pushdown_expr(
                (**expr).clone(),
                from_qualifier,
                to_qualifier,
            ));
            *pattern = Box::new(retarget_pushdown_expr(
                (**pattern).clone(),
                from_qualifier,
                to_qualifier,
            ));
        }
        Expr::InList { expr, list, .. } => {
            *expr = Box::new(retarget_pushdown_expr(
                (**expr).clone(),
                from_qualifier,
                to_qualifier,
            ));
            *list = list
                .iter()
                .map(|item| retarget_pushdown_expr(item.clone(), from_qualifier, to_qualifier))
                .collect();
        }
        Expr::InSubquery { expr, .. } => {
            *expr = Box::new(retarget_pushdown_expr(
                (**expr).clone(),
                from_qualifier,
                to_qualifier,
            ));
        }
        Expr::Tuple(items) => {
            *items = items
                .iter()
                .map(|item| retarget_pushdown_expr(item.clone(), from_qualifier, to_qualifier))
                .collect();
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(inner) = operand {
                *inner = Box::new(retarget_pushdown_expr(
                    (**inner).clone(),
                    from_qualifier,
                    to_qualifier,
                ));
            }
            for condition in conditions.iter_mut() {
                condition.condition =
                    retarget_pushdown_expr(condition.condition.clone(), from_qualifier, to_qualifier);
                condition.result =
                    retarget_pushdown_expr(condition.result.clone(), from_qualifier, to_qualifier);
            }
            if let Some(inner) = else_result {
                *inner = Box::new(retarget_pushdown_expr(
                    (**inner).clone(),
                    from_qualifier,
                    to_qualifier,
                ));
            }
        }
        Expr::Function(function) => {
            if let FunctionArguments::List(list) = &mut function.args {
                for arg in &mut list.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) => {
                            *inner =
                                retarget_pushdown_expr(inner.clone(), from_qualifier, to_qualifier)
                        }
                        FunctionArg::Named { arg, .. } | FunctionArg::ExprNamed { arg, .. } => {
                            if let FunctionArgExpr::Expr(inner) = arg {
                                *inner = retarget_pushdown_expr(
                                    inner.clone(),
                                    from_qualifier,
                                    to_qualifier,
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            let column = &parts[parts.len() - 1].value;
            if qualifier.eq_ignore_ascii_case(from_qualifier)
                && normalize_state_column(column).is_some()
            {
                let qualifier_index = parts.len() - 2;
                parts[qualifier_index] = Ident::new(to_qualifier);
            }
        }
        _ => {}
    }
    expr
}

fn split_conjunction(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut out = split_conjunction(*left);
            out.extend(split_conjunction(*right));
            out
        }
        other => vec![other],
    }
}

fn join_conjunction(mut predicates: Vec<Expr>) -> Option<Expr> {
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

fn extract_pushdown_predicate(
    predicate: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<(String, Expr)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_target_column(left, relation_name, allow_unqualified) {
                return Some((
                    column.clone(),
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new(column))),
                        op: BinaryOperator::Eq,
                        right: right.clone(),
                    },
                ));
            }
            if let Some(column) = extract_target_column(right, relation_name, allow_unqualified) {
                return Some((
                    column.clone(),
                    Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new(column))),
                        op: BinaryOperator::Eq,
                        right: left.clone(),
                    },
                ));
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            Some((
                column.clone(),
                Expr::InList {
                    expr: Box::new(Expr::Identifier(Ident::new(column))),
                    list: list.clone(),
                    negated: false,
                },
            ))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            Some((
                column.clone(),
                Expr::InSubquery {
                    expr: Box::new(Expr::Identifier(Ident::new(column))),
                    subquery: subquery.clone(),
                    negated: false,
                },
            ))
        }
        Expr::IsNull(expr) if allow_unqualified => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            Some((
                column.clone(),
                Expr::IsNull(Box::new(Expr::Identifier(Ident::new(column)))),
            ))
        }
        Expr::IsNotNull(expr) if allow_unqualified => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            Some((
                column.clone(),
                Expr::IsNotNull(Box::new(Expr::Identifier(Ident::new(column)))),
            ))
        }
        _ => None,
    }
}

fn qualify_pushdown_predicate(predicate: Expr, qualifier: &str) -> Expr {
    match predicate {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(qualify_target_expr(*left, qualifier)),
            op,
            right,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(qualify_target_expr(*expr, qualifier)),
            list,
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(qualify_target_expr(*expr, qualifier)),
            subquery,
            negated,
        },
        Expr::IsNull(expr) => Expr::IsNull(Box::new(qualify_target_expr(*expr, qualifier))),
        Expr::IsNotNull(expr) => Expr::IsNotNull(Box::new(qualify_target_expr(*expr, qualifier))),
        other => other,
    }
}

fn qualify_target_expr(expr: Expr, qualifier: &str) -> Expr {
    match expr {
        Expr::Identifier(ident) if normalize_state_column(&ident.value).is_some() => {
            Expr::CompoundIdentifier(vec![Ident::new(qualifier), ident])
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let column = &parts[parts.len() - 1].value;
            if normalize_state_column(column).is_some() {
                let mut next = parts;
                let qualifier_index = next.len() - 2;
                next[qualifier_index] = Ident::new(qualifier);
                Expr::CompoundIdentifier(next)
            } else {
                Expr::CompoundIdentifier(parts)
            }
        }
        Expr::Nested(inner) => Expr::Nested(Box::new(qualify_target_expr(*inner, qualifier))),
        other => other,
    }
}

fn extract_target_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<String> {
    match expr {
        Expr::Identifier(ident) if allow_unqualified => normalize_state_column(&ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            if !qualifier.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            let column = &parts[parts.len() - 1].value;
            normalize_state_column(column)
        }
        Expr::Nested(inner) => extract_target_column(inner, relation_name, allow_unqualified),
        _ => None,
    }
}

fn normalize_state_column(raw: &str) -> Option<String> {
    match raw.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Some("entity_id".to_string()),
        "schema_key" | "lixcol_schema_key" => Some("schema_key".to_string()),
        "file_id" | "lixcol_file_id" => Some("file_id".to_string()),
        "version_id" | "lixcol_version_id" => Some("version_id".to_string()),
        "plugin_key" | "lixcol_plugin_key" => Some("plugin_key".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{retarget_pushdown_predicates, take_pushdown_predicates};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn extracts_ast_predicates_and_retains_remaining_selection() {
        let mut selection = Some(parse_where_expr(
            "SELECT * FROM lix_state WHERE schema_key = 'x' AND plugin_key = 'p' AND untracked = true",
        ));

        let pushdown = take_pushdown_predicates(&mut selection, "lix_state", true);

        assert_eq!(pushdown.source_predicates.len(), 1);
        assert_eq!(pushdown.ranked_predicates.len(), 1);
        assert_eq!(pushdown.source_predicates[0].to_string(), "s.schema_key = 'x'");
        assert_eq!(pushdown.ranked_predicates[0].to_string(), "ranked.plugin_key = 'p'");
        assert_eq!(
            selection.as_ref().expect("remaining selection").to_string(),
            "untracked = true"
        );
    }

    #[test]
    fn retargets_ranked_predicates_to_source_alias() {
        let mut selection = Some(parse_where_expr(
            "SELECT * FROM lix_state WHERE plugin_key IN ('a', 'b')",
        ));

        let pushdown = take_pushdown_predicates(&mut selection, "lix_state", true);
        let retargeted = retarget_pushdown_predicates(&pushdown.ranked_predicates, "ranked", "s");

        assert_eq!(retargeted.len(), 1);
        assert_eq!(retargeted[0].to_string(), "s.plugin_key IN ('a', 'b')");
    }

    #[test]
    fn extracts_is_null_predicates() {
        let mut selection = Some(parse_where_expr(
            "SELECT * FROM lix_state WHERE file_id IS NULL AND version_id IS NOT NULL",
        ));

        let pushdown = take_pushdown_predicates(&mut selection, "lix_state", true);

        assert_eq!(pushdown.source_predicates.len(), 1);
        assert_eq!(pushdown.ranked_predicates.len(), 1);
        assert_eq!(pushdown.source_predicates[0].to_string(), "s.file_id IS NULL");
        assert_eq!(
            pushdown.ranked_predicates[0].to_string(),
            "ranked.version_id IS NOT NULL"
        );
        assert!(selection.is_none());
    }

    fn parse_where_expr(sql: &str) -> sqlparser::ast::Expr {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("expected select");
        };
        select.selection.expect("where expression")
    }
}
