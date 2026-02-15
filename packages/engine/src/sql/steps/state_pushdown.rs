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
    Remaining = 2,
}

struct PredicatePart {
    predicate: Expr,
    extracted: Option<(PushdownBucket, String)>,
    has_bare_placeholder: bool,
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
        let extracted = extract_pushdown_comparison(&predicate, relation_name, allow_unqualified)
            .and_then(|(column, value_sql)| match column.as_str() {
                "entity_id" | "schema_key" | "file_id" => {
                    Some((PushdownBucket::Source, format!("s.{column} = {value_sql}")))
                }
                "plugin_key" => {
                    // Keep plugin filtering after winner selection to preserve row-choice semantics.
                    Some((
                        PushdownBucket::Ranked,
                        format!("ranked.{column} = {value_sql}"),
                    ))
                }
                _ => None,
            });
        let has_bare_placeholder = expr_contains_bare_placeholder(&predicate);
        parts.push(PredicatePart {
            predicate,
            extracted,
            has_bare_placeholder,
        });
    }

    let has_bare_placeholder_reordering = has_bare_placeholder_reordering(&parts);

    let mut pushdown = StatePushdown::default();
    let mut remaining = Vec::new();
    for part in parts {
        match part.extracted {
            Some((bucket, sql))
                if !(part.has_bare_placeholder && has_bare_placeholder_reordering) =>
            {
                match bucket {
                    PushdownBucket::Source => pushdown.source_predicates.push(sql),
                    PushdownBucket::Ranked => pushdown.ranked_predicates.push(sql),
                    PushdownBucket::Remaining => remaining.push(part.predicate),
                }
            }
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
}

fn has_bare_placeholder_reordering(parts: &[PredicatePart]) -> bool {
    let mut last_bucket = PushdownBucket::Source;
    let mut saw_any = false;
    for part in parts {
        if !part.has_bare_placeholder {
            continue;
        }
        let bucket = part
            .extracted
            .as_ref()
            .map(|(bucket, _)| *bucket)
            .unwrap_or(PushdownBucket::Remaining);
        if saw_any && bucket < last_bucket {
            return true;
        }
        last_bucket = bucket;
        saw_any = true;
    }
    false
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

fn expr_contains_bare_placeholder(expr: &Expr) -> bool {
    let sql = expr.to_string();
    sql_contains_bare_placeholder(&sql)
}

fn sql_contains_bare_placeholder(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut index = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while index < bytes.len() {
        let byte = bytes[index];

        if in_single_quote {
            if byte == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                    continue;
                }
                in_single_quote = false;
            }
            index += 1;
            continue;
        }

        if in_double_quote {
            if byte == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    index += 2;
                    continue;
                }
                in_double_quote = false;
            }
            index += 1;
            continue;
        }

        match byte {
            b'\'' => {
                in_single_quote = true;
                index += 1;
            }
            b'"' => {
                in_double_quote = true;
                index += 1;
            }
            b'?' => {
                let mut lookahead = index + 1;
                while lookahead < bytes.len() && bytes[lookahead].is_ascii_digit() {
                    lookahead += 1;
                }
                if lookahead == index + 1 {
                    return true;
                }
                index = lookahead;
            }
            _ => index += 1,
        }
    }
    false
}
