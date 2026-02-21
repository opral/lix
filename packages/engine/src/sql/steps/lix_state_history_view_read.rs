use sqlparser::ast::{BinaryOperator, Expr, Query, Select, TableFactor, TableWithJoins};

use crate::sql::steps::state_pushdown::select_supports_count_fast_path;
use crate::sql::{
    default_alias, object_name_matches, parse_single_query, rewrite_query_with_select_rewriter,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_select_rewriter(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select, changed: &mut bool) -> Result<(), LixError> {
    let count_fast_path = select_supports_count_fast_path(select);
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &mut select.selection,
            allow_unqualified,
            count_fast_path,
            changed,
        )?;
    }
    Ok(())
}

fn rewrite_table_with_joins(
    table: &mut TableWithJoins,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        selection,
        allow_unqualified,
        count_fast_path,
        changed,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(&mut join.relation, selection, false, false, changed)?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    changed: &mut bool,
) -> Result<(), LixError> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if object_name_matches(name, LIX_STATE_HISTORY_VIEW_NAME) =>
        {
            let relation_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| LIX_STATE_HISTORY_VIEW_NAME.to_string());
            let pushdown =
                take_history_pushdown_predicates(selection, &relation_name, allow_unqualified);
            let derived_query = build_lix_state_history_view_query(
                &pushdown,
                count_fast_path && selection.is_none(),
            )?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_lix_state_history_alias()));
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
                selection,
                allow_unqualified,
                count_fast_path,
                changed,
            )?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Default)]
struct HistoryPushdown {
    change_predicates: Vec<String>,
    requested_predicates: Vec<String>,
    cse_predicates: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HistoryPushdownBucket {
    Change = 0,
    Requested = 1,
    Cse = 2,
    Remaining = 3,
}

enum ExtractedPredicate {
    Push(HistoryPushdownBucket, String),
    Drop,
}

struct PredicatePart {
    predicate: Expr,
    extracted: Option<ExtractedPredicate>,
    has_bare_placeholder: bool,
}

fn take_history_pushdown_predicates(
    selection: &mut Option<Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> HistoryPushdown {
    let Some(selection_expr) = selection.take() else {
        return HistoryPushdown::default();
    };

    let mut parts = Vec::new();
    for predicate in split_conjunction(selection_expr) {
        let extracted =
            extract_history_pushdown_predicate(&predicate, relation_name, allow_unqualified);
        let has_bare_placeholder = expr_contains_bare_placeholder(&predicate);
        parts.push(PredicatePart {
            predicate,
            extracted,
            has_bare_placeholder,
        });
    }

    let has_bare_placeholder_reordering = has_bare_placeholder_reordering(&parts);

    let mut pushdown = HistoryPushdown::default();
    let mut remaining = Vec::new();
    for part in parts {
        match part.extracted {
            Some(ExtractedPredicate::Push(bucket, sql))
                if !(part.has_bare_placeholder && has_bare_placeholder_reordering) =>
            {
                match bucket {
                    HistoryPushdownBucket::Change => pushdown.change_predicates.push(sql),
                    HistoryPushdownBucket::Requested => pushdown.requested_predicates.push(sql),
                    HistoryPushdownBucket::Cse => pushdown.cse_predicates.push(sql),
                    HistoryPushdownBucket::Remaining => remaining.push(part.predicate),
                }
            }
            Some(ExtractedPredicate::Drop)
                if !(part.has_bare_placeholder && has_bare_placeholder_reordering) => {}
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
}

fn has_bare_placeholder_reordering(parts: &[PredicatePart]) -> bool {
    let mut last_bucket = HistoryPushdownBucket::Change;
    let mut saw_any = false;
    for part in parts {
        if !part.has_bare_placeholder {
            continue;
        }
        let bucket = match &part.extracted {
            Some(ExtractedPredicate::Push(bucket, _)) => *bucket,
            Some(ExtractedPredicate::Drop) | None => HistoryPushdownBucket::Remaining,
        };
        if saw_any && bucket < last_bucket {
            return true;
        }
        last_bucket = bucket;
        saw_any = true;
    }
    false
}

fn extract_history_pushdown_predicate(
    predicate: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<ExtractedPredicate> {
    match predicate {
        Expr::BinaryOp { left, op, right } => {
            let left_column = extract_target_column(left, relation_name, allow_unqualified);
            let right_column = extract_target_column(right, relation_name, allow_unqualified);

            if let Some(column) = left_column {
                if let Some(extracted) = extract_binary_pushdown(&column, op.clone(), right) {
                    return Some(extracted);
                }
            }
            if let Some(column) = right_column {
                let swapped_operator = swap_binary_operator(op.clone())?;
                if let Some(extracted) = extract_binary_pushdown(&column, swapped_operator, left) {
                    return Some(extracted);
                }
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            extract_in_list_pushdown(&column, list)
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            extract_in_subquery_pushdown(&column, subquery)
        }
        Expr::IsNotNull(inner) => {
            let column = extract_target_column(inner, relation_name, allow_unqualified)?;
            if column == "snapshot_content" {
                return Some(ExtractedPredicate::Drop);
            }
            None
        }
        _ => None,
    }
}

fn extract_binary_pushdown(
    column: &str,
    operator: BinaryOperator,
    rhs: &Expr,
) -> Option<ExtractedPredicate> {
    let op_sql = operator.to_string();
    match column {
        "schema_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.schema_key {op_sql} {rhs}"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.entity_id {op_sql} {rhs}"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.file_id {op_sql} {rhs}"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.plugin_key {op_sql} {rhs}"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.id {op_sql} {rhs}"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id {op_sql} {rhs}"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_id {op_sql} {rhs}"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_depth {op_sql} {rhs}"),
        )),
        _ => None,
    }
}

fn extract_in_list_pushdown(column: &str, list: &[Expr]) -> Option<ExtractedPredicate> {
    let list_sql = render_in_list_sql(list);
    match column {
        "schema_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.schema_key IN ({list_sql})"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.entity_id IN ({list_sql})"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.file_id IN ({list_sql})"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.plugin_key IN ({list_sql})"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.id IN ({list_sql})"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id IN ({list_sql})"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_id IN ({list_sql})"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_depth IN ({list_sql})"),
        )),
        _ => None,
    }
}

fn extract_in_subquery_pushdown(column: &str, subquery: &Query) -> Option<ExtractedPredicate> {
    match column {
        "schema_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.schema_key IN ({subquery})"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.entity_id IN ({subquery})"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.file_id IN ({subquery})"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.plugin_key IN ({subquery})"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("ic.id IN ({subquery})"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id IN ({subquery})"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_id IN ({subquery})"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("cc_raw.commit_depth IN ({subquery})"),
        )),
        _ => None,
    }
}

fn swap_binary_operator(operator: BinaryOperator) -> Option<BinaryOperator> {
    match operator {
        BinaryOperator::Eq => Some(BinaryOperator::Eq),
        BinaryOperator::Gt => Some(BinaryOperator::Lt),
        BinaryOperator::GtEq => Some(BinaryOperator::LtEq),
        BinaryOperator::Lt => Some(BinaryOperator::Gt),
        BinaryOperator::LtEq => Some(BinaryOperator::GtEq),
        _ => None,
    }
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

fn extract_target_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<String> {
    match expr {
        Expr::Identifier(ident) if allow_unqualified => normalize_history_column(&ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            if !qualifier.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            let column = &parts[parts.len() - 1].value;
            normalize_history_column(column)
        }
        Expr::Nested(inner) => extract_target_column(inner, relation_name, allow_unqualified),
        _ => None,
    }
}

fn normalize_history_column(raw: &str) -> Option<String> {
    match raw.to_ascii_lowercase().as_str() {
        "entity_id" | "lixcol_entity_id" => Some("entity_id".to_string()),
        "schema_key" | "lixcol_schema_key" => Some("schema_key".to_string()),
        "file_id" | "lixcol_file_id" => Some("file_id".to_string()),
        "plugin_key" | "lixcol_plugin_key" => Some("plugin_key".to_string()),
        "change_id" | "lixcol_change_id" => Some("change_id".to_string()),
        "commit_id" | "lixcol_commit_id" => Some("commit_id".to_string()),
        "root_commit_id" | "lixcol_root_commit_id" => Some("root_commit_id".to_string()),
        "depth" | "lixcol_depth" => Some("depth".to_string()),
        "snapshot_content" => Some("snapshot_content".to_string()),
        _ => None,
    }
}

fn render_in_list_sql(list: &[Expr]) -> String {
    list.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
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

fn render_where_clause(predicates: &[String]) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", predicates.join(" AND "))
    }
}

fn build_lix_state_history_view_query(
    pushdown: &HistoryPushdown,
    count_fast_path: bool,
) -> Result<Query, LixError> {
    let mut all_changes_predicates = Vec::new();
    if count_fast_path {
        all_changes_predicates.push("ic.snapshot_id != 'no-content'".to_string());
    }
    all_changes_predicates.extend(pushdown.change_predicates.clone());

    let mut requested_predicates = vec![format!("c.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    requested_predicates.extend(pushdown.requested_predicates.clone());

    let mut cse_predicates = vec![format!("cse_raw.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    cse_predicates.extend(pushdown.cse_predicates.clone());

    let all_changes_where_sql = render_where_clause(&all_changes_predicates);
    let requested_where_sql = render_where_clause(&requested_predicates);
    let cse_where_sql = render_where_clause(&cse_predicates);

    let all_changes_sql = if count_fast_path {
        format!(
            "SELECT \
               ic.id, \
               ic.created_at \
             FROM lix_internal_change ic \
             {all_changes_where}",
            all_changes_where = all_changes_where_sql,
        )
    } else {
        format!(
            "SELECT \
               ic.id, \
               ic.entity_id, \
               ic.schema_key, \
               ic.file_id, \
               ic.plugin_key, \
               ic.schema_version, \
               ic.created_at, \
               CASE \
                 WHEN ic.snapshot_id = 'no-content' THEN NULL \
                 ELSE s.content \
               END AS snapshot_content, \
               ic.metadata AS metadata \
             FROM lix_internal_change ic \
             LEFT JOIN lix_internal_snapshot s ON s.id = ic.snapshot_id \
             {all_changes_where}",
            all_changes_where = all_changes_where_sql,
        )
    };

    let ranked_select_sql = if count_fast_path {
        "SELECT \
           r.target_entity_id, \
           r.target_file_id, \
           r.target_schema_key, \
           r.target_change_id, \
           r.origin_commit_id, \
           r.root_commit_id, \
           r.commit_depth, \
           ROW_NUMBER() OVER ( \
             PARTITION BY \
               r.target_entity_id, \
               r.target_file_id, \
               r.target_schema_key, \
               r.root_commit_id, \
               r.commit_depth \
             ORDER BY \
               target_change.created_at DESC, \
               target_change.id DESC \
           ) AS rn \
         FROM cse_in_reachable_commits r \
         JOIN all_changes_with_snapshots target_change \
           ON target_change.id = r.target_change_id"
            .to_string()
    } else {
        "SELECT \
           target_change.entity_id AS entity_id, \
           target_change.schema_key AS schema_key, \
           target_change.file_id AS file_id, \
           target_change.plugin_key AS plugin_key, \
           target_change.snapshot_content AS snapshot_content, \
           target_change.metadata AS metadata, \
           target_change.schema_version AS schema_version, \
           r.target_change_id AS target_change_id, \
           r.origin_commit_id AS origin_commit_id, \
           r.root_commit_id AS root_commit_id, \
           r.commit_depth AS commit_depth, \
           ROW_NUMBER() OVER ( \
             PARTITION BY \
               r.target_entity_id, \
               r.target_file_id, \
               r.target_schema_key, \
               r.root_commit_id, \
               r.commit_depth \
             ORDER BY \
               target_change.created_at DESC, \
               target_change.id DESC \
           ) AS rn \
         FROM cse_in_reachable_commits r \
         JOIN all_changes_with_snapshots target_change \
           ON target_change.id = r.target_change_id"
            .to_string()
    };

    let final_select_sql = if count_fast_path {
        "SELECT COUNT(*) AS count \
         FROM ranked_cse ranked \
         WHERE ranked.rn = 1"
            .to_string()
    } else {
        format!(
            "SELECT \
               ranked.entity_id AS entity_id, \
               ranked.schema_key AS schema_key, \
               ranked.file_id AS file_id, \
               ranked.plugin_key AS plugin_key, \
               ranked.snapshot_content AS snapshot_content, \
               ranked.metadata AS metadata, \
               ranked.schema_version AS schema_version, \
               ranked.target_change_id AS change_id, \
               ranked.origin_commit_id AS commit_id, \
               ranked.root_commit_id AS root_commit_id, \
               ranked.commit_depth AS depth, \
               '{global_version}' AS version_id \
             FROM ranked_cse ranked \
             WHERE ranked.rn = 1 \
               AND ranked.snapshot_content IS NOT NULL",
            global_version = GLOBAL_VERSION_ID
        )
    };

    let sql = format!(
        "WITH \
           commit_by_version AS ( \
             SELECT \
               entity_id AS id, \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               lix_json_text(snapshot_content, 'change_set_id') AS change_set_id, \
               lix_json_text(snapshot_content, 'change_id') AS change_id, \
               lix_json_text(snapshot_content, 'entity_id') AS entity_id, \
               lix_json_text(snapshot_content, 'schema_key') AS schema_key, \
               lix_json_text(snapshot_content, 'file_id') AS file_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           all_changes_with_snapshots AS ( \
             {all_changes_sql} \
           ), \
           requested_commits AS ( \
             SELECT DISTINCT c.id AS commit_id \
             FROM commit_by_version c \
             {requested_where_sql} \
           ), \
           reachable_commits_from_requested(id, root_commit_id, depth) AS ( \
             SELECT \
               ancestry.ancestor_id AS id, \
               requested.commit_id AS root_commit_id, \
               ancestry.depth AS depth \
             FROM requested_commits requested \
             JOIN lix_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= 512 \
           ), \
           commit_changesets AS ( \
             SELECT \
               c.id AS commit_id, \
               c.change_set_id AS change_set_id, \
               rc.root_commit_id, \
               rc.depth AS commit_depth \
             FROM commit_by_version c \
             JOIN reachable_commits_from_requested rc ON c.id = rc.id \
             WHERE c.lixcol_version_id = '{global_version}' \
           ), \
           cse_in_reachable_commits AS ( \
             SELECT \
               cse_raw.entity_id AS target_entity_id, \
               cse_raw.file_id AS target_file_id, \
               cse_raw.schema_key AS target_schema_key, \
               cse_raw.change_id AS target_change_id, \
               cc_raw.commit_id AS origin_commit_id, \
               cc_raw.root_commit_id AS root_commit_id, \
               cc_raw.commit_depth AS commit_depth \
             FROM change_set_element_by_version cse_raw \
             JOIN commit_changesets cc_raw \
               ON cse_raw.change_set_id = cc_raw.change_set_id \
             {cse_where_sql} \
           ), \
           ranked_cse AS ( \
             {ranked_select_sql} \
           ) \
         {final_select_sql}",
        global_version = GLOBAL_VERSION_ID,
        all_changes_sql = all_changes_sql,
        requested_where_sql = requested_where_sql,
        cse_where_sql = cse_where_sql,
        ranked_select_sql = ranked_select_sql,
        final_select_sql = final_select_sql,
    );
    parse_single_query(&sql)
}

fn default_lix_state_history_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_HISTORY_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::rewrite_query;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn pushes_history_schema_and_root_commit_filters_into_ctes_for_count_fast_path() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.schema_key = ? \
               AND sh.root_commit_id = ? \
               AND sh.snapshot_content IS NOT NULL",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("ic.schema_key = ?"));
        assert!(sql.contains("c.id = ?"));
        assert!(!sql.contains("sh.schema_key = ?"));
        assert!(!sql.contains("sh.root_commit_id = ?"));
        assert!(sql.contains("SELECT COUNT(*) AS count FROM ranked_cse ranked WHERE ranked.rn = 1"));
        assert!(!sql.contains("ranked.snapshot_content IS NOT NULL"));
    }

    #[test]
    fn does_not_push_down_bare_placeholders_when_it_would_reorder_bindings() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.root_commit_id = ? \
               AND sh.schema_key = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("c.id = ?"));
        assert!(!sql.contains("ic.schema_key = ?"));
        assert!(sql.contains("sh.root_commit_id = ?"));
        assert!(sql.contains("sh.schema_key = ?"));
    }

    #[test]
    fn pushes_numbered_placeholders_even_when_predicate_order_reorders() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.root_commit_id = ?1 \
               AND sh.schema_key = ?2",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("c.id = ?1"));
        assert!(sql.contains("ic.schema_key = ?2"));
        assert!(!sql.contains("sh.root_commit_id = ?1"));
        assert!(!sql.contains("sh.schema_key = ?2"));
        assert!(sql.contains("SELECT COUNT(*) AS count FROM ranked_cse ranked WHERE ranked.rn = 1"));
    }

    #[test]
    fn uses_materialized_commit_ancestry_instead_of_recursive_commit_edges() {
        let query = parse_query(
            "SELECT depth, snapshot_content \
             FROM lix_state_history AS sh \
             WHERE sh.root_commit_id = ? \
             ORDER BY depth ASC",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("JOIN lix_commit_ancestry ancestry"));
        assert!(sql.contains("FROM lix_internal_state_materialized_v1_lix_commit"));
        assert!(!sql.contains("WITH RECURSIVE"));
        assert!(!sql.contains("commit_edge_by_version"));
        assert!(
            !sql.contains("FROM lix_internal_state_vtable WHERE schema_key = 'lix_commit_edge'")
        );
    }

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("valid SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            _ => panic!("expected query"),
        }
    }
}
