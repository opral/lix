use sqlparser::ast::{BinaryOperator, Expr, GroupByExpr, Select};

#[derive(Default)]
pub(crate) struct StatePushdown {
    pub(crate) source_predicates: Vec<String>,
    pub(crate) ranked_predicates: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PushdownBucket {
    Source = 0,
    Ranked = 1,
}

struct PredicatePart {
    predicate: Expr,
    extracted: Option<(PushdownBucket, String)>,
}

pub(crate) fn select_supports_count_fast_path(select: &Select) -> bool {
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
            .and_then(|(column, predicate_sql)| match column.as_str() {
                "entity_id" | "schema_key" | "file_id" => {
                    Some((PushdownBucket::Source, format!("s.{predicate_sql}")))
                }
                "version_id" => Some((PushdownBucket::Ranked, format!("ranked.{predicate_sql}"))),
                "plugin_key" => {
                    // Keep plugin filtering after winner selection to preserve row-choice semantics.
                    Some((PushdownBucket::Ranked, format!("ranked.{predicate_sql}")))
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
            Some((bucket, sql)) => match bucket {
                PushdownBucket::Source => pushdown.source_predicates.push(sql),
                PushdownBucket::Ranked => pushdown.ranked_predicates.push(sql),
            },
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
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
) -> Option<(String, String)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_target_column(left, relation_name, allow_unqualified) {
                return Some((column.clone(), format!("{column} = {}", right)));
            }
            if let Some(column) = extract_target_column(right, relation_name, allow_unqualified) {
                return Some((column.clone(), format!("{column} = {}", left)));
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            let list_sql = render_in_list_sql(list);
            Some((column.clone(), format!("{column} IN ({list_sql})")))
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            Some((column.clone(), format!("{column} IN ({subquery})")))
        }
        _ => None,
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

fn render_in_list_sql(list: &[Expr]) -> String {
    list.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
