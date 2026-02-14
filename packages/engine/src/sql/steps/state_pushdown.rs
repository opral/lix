use sqlparser::ast::{BinaryOperator, Expr, GroupByExpr, Select};

#[derive(Default)]
pub(crate) struct StatePushdown {
    pub(crate) source_predicates: Vec<String>,
    pub(crate) ranked_predicates: Vec<String>,
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

    let mut pushdown = StatePushdown::default();
    let mut remaining = Vec::new();
    for predicate in split_conjunction(selection_expr) {
        let Some((column, value_sql)) =
            extract_pushdown_comparison(&predicate, relation_name, allow_unqualified)
        else {
            remaining.push(predicate);
            continue;
        };

        match column.as_str() {
            "entity_id" | "schema_key" | "file_id" => {
                pushdown
                    .source_predicates
                    .push(format!("s.{column} = {value_sql}"));
            }
            "plugin_key" => {
                // Keep plugin filtering after winner selection to preserve row-choice semantics.
                pushdown
                    .ranked_predicates
                    .push(format!("ranked.{column} = {value_sql}"));
            }
            _ => remaining.push(predicate),
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

fn extract_pushdown_comparison(
    predicate: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = predicate
    else {
        return None;
    };

    if let Some(column) = extract_target_column(left, relation_name, allow_unqualified) {
        return Some((column, right.to_string()));
    }
    if let Some(column) = extract_target_column(right, relation_name, allow_unqualified) {
        return Some((column, left.to_string()));
    }
    None
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
        "plugin_key" | "lixcol_plugin_key" => Some("plugin_key".to_string()),
        _ => None,
    }
}
