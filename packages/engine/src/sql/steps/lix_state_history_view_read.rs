use std::collections::BTreeSet;

use sqlparser::ast::{BinaryOperator, Expr, Query, Select, TableFactor, TableWithJoins};

use crate::backend::SqlDialect;
use crate::sql::steps::state_pushdown::select_supports_count_fast_path;
use crate::sql::{
    default_alias, escape_sql_string, object_name_matches, parse_single_query,
    rewrite_query_with_select_rewriter,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, QueryResult, Value};

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";
const TIMELINE_BREAKPOINT_TABLE: &str = "lix_internal_entity_state_timeline_breakpoint";
const TIMELINE_STATUS_TABLE: &str = "lix_internal_timeline_status";
const MAX_HISTORY_DEPTH: i64 = 512;

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let (rewritten, _requests) = rewrite_query_collect_requests(query)?;
    Ok(rewritten)
}

pub async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Option<Query>, LixError> {
    let (rewritten, requests) = rewrite_query_collect_requests(query)?;
    let mut seen_requests = BTreeSet::new();
    for request in requests {
        if should_fallback_to_phase1_query(&request) {
            continue;
        }
        let request_key = format!(
            "{}||{}||{}",
            request.change_predicates.join("&&"),
            request.requested_predicates.join("&&"),
            request.cse_predicates.join("&&")
        );
        if !seen_requests.insert(request_key) {
            continue;
        }
        ensure_history_timeline_materialized_for_request(backend, &request, params).await?;
    }
    Ok(rewritten)
}

fn rewrite_query_collect_requests(
    query: Query,
) -> Result<(Option<Query>, Vec<HistoryPushdown>), LixError> {
    let mut requests = Vec::new();
    let rewritten = rewrite_query_with_select_rewriter(query, &mut |select, changed| {
        rewrite_select(select, changed, &mut requests)
    })?;
    Ok((rewritten, requests))
}

fn rewrite_select(
    select: &mut Select,
    changed: &mut bool,
    requests: &mut Vec<HistoryPushdown>,
) -> Result<(), LixError> {
    let count_fast_path = select_supports_count_fast_path(select);
    let allow_unqualified = select.from.len() == 1 && select.from[0].joins.is_empty();
    for table in &mut select.from {
        rewrite_table_with_joins(
            table,
            &mut select.selection,
            allow_unqualified,
            count_fast_path,
            changed,
            requests,
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
    requests: &mut Vec<HistoryPushdown>,
) -> Result<(), LixError> {
    rewrite_table_factor(
        &mut table.relation,
        selection,
        allow_unqualified,
        count_fast_path,
        changed,
        requests,
    )?;
    for join in &mut table.joins {
        rewrite_table_factor(
            &mut join.relation,
            selection,
            false,
            false,
            changed,
            requests,
        )?;
    }
    Ok(())
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    selection: &mut Option<Expr>,
    allow_unqualified: bool,
    count_fast_path: bool,
    changed: &mut bool,
    requests: &mut Vec<HistoryPushdown>,
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
            requests.push(pushdown.clone());
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
                requests,
            )?;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Default, Clone)]
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
            format!("bp.schema_key {op_sql} {rhs}"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.entity_id {op_sql} {rhs}"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.file_id {op_sql} {rhs}"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.plugin_key {op_sql} {rhs}"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.change_id {op_sql} {rhs}"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id {op_sql} {rhs}"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_id {op_sql} {rhs}"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_depth {op_sql} {rhs}"),
        )),
        _ => None,
    }
}

fn extract_in_list_pushdown(column: &str, list: &[Expr]) -> Option<ExtractedPredicate> {
    let list_sql = render_in_list_sql(list);
    match column {
        "schema_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.schema_key IN ({list_sql})"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.entity_id IN ({list_sql})"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.file_id IN ({list_sql})"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.plugin_key IN ({list_sql})"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.change_id IN ({list_sql})"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id IN ({list_sql})"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_id IN ({list_sql})"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_depth IN ({list_sql})"),
        )),
        _ => None,
    }
}

fn extract_in_subquery_pushdown(column: &str, subquery: &Query) -> Option<ExtractedPredicate> {
    match column {
        "schema_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.schema_key IN ({subquery})"),
        )),
        "entity_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.entity_id IN ({subquery})"),
        )),
        "file_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.file_id IN ({subquery})"),
        )),
        "plugin_key" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.plugin_key IN ({subquery})"),
        )),
        "change_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Change,
            format!("bp.change_id IN ({subquery})"),
        )),
        "root_commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Requested,
            format!("c.id IN ({subquery})"),
        )),
        "commit_id" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_id IN ({subquery})"),
        )),
        "depth" => Some(ExtractedPredicate::Push(
            HistoryPushdownBucket::Cse,
            format!("rc.commit_depth IN ({subquery})"),
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
    if should_fallback_to_phase1_query(pushdown) {
        return build_lix_state_history_view_query_phase1(pushdown, count_fast_path);
    }

    let mut requested_predicates = vec![format!("c.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    requested_predicates.extend(pushdown.requested_predicates.clone());
    let reachable_where_sql = render_where_clause(&pushdown.cse_predicates);
    let breakpoint_where_sql = render_where_clause(&pushdown.change_predicates);
    let requested_where_sql = render_where_clause(&requested_predicates);

    let final_select_sql = if count_fast_path {
        "SELECT COUNT(*) AS count \
         FROM history_rows h \
         WHERE h.snapshot_id != 'no-content'"
            .to_string()
    } else {
        format!(
            "SELECT \
               h.entity_id AS entity_id, \
               h.schema_key AS schema_key, \
               h.file_id AS file_id, \
               h.plugin_key AS plugin_key, \
               s.content AS snapshot_content, \
               h.metadata AS metadata, \
               h.schema_version AS schema_version, \
               h.change_id AS change_id, \
               h.commit_id AS commit_id, \
               h.root_commit_id AS root_commit_id, \
               h.depth AS depth, \
               '{global_version}' AS version_id \
             FROM history_rows h \
             LEFT JOIN lix_internal_snapshot s \
               ON s.id = h.snapshot_id \
             WHERE h.snapshot_id != 'no-content'",
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
           requested_commits AS ( \
             SELECT DISTINCT c.id AS commit_id \
             FROM commit_by_version c \
             {requested_where_sql} \
           ), \
           reachable_commits AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               requested.commit_id AS root_commit_id, \
               ancestry.depth AS commit_depth \
             FROM requested_commits requested \
             JOIN lix_internal_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= {max_depth} \
           ), \
           filtered_reachable_commits AS ( \
             SELECT \
               rc.commit_id, \
               rc.root_commit_id, \
               rc.commit_depth \
             FROM reachable_commits rc \
             {reachable_where_sql} \
           ), \
           breakpoint_rows AS ( \
             SELECT \
               bp.root_commit_id, \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
                bp.metadata, \
                bp.snapshot_id, \
                bp.change_id, \
                bp.from_depth \
             FROM lix_internal_entity_state_timeline_breakpoint bp \
             JOIN requested_commits requested \
               ON requested.commit_id = bp.root_commit_id \
             {breakpoint_where_sql} \
           ), \
           history_rows AS ( \
             SELECT \
               bp.entity_id, \
               bp.schema_key, \
               bp.file_id, \
               bp.plugin_key, \
               bp.schema_version, \
               bp.metadata, \
               bp.snapshot_id, \
               bp.change_id, \
               rc.commit_id AS commit_id, \
               rc.root_commit_id AS root_commit_id, \
               rc.commit_depth AS depth \
             FROM filtered_reachable_commits rc \
             JOIN breakpoint_rows bp \
               ON bp.root_commit_id = rc.root_commit_id \
              AND rc.commit_depth = bp.from_depth \
           ) \
         {final_select_sql}",
        global_version = GLOBAL_VERSION_ID,
        max_depth = MAX_HISTORY_DEPTH,
        requested_where_sql = requested_where_sql,
        reachable_where_sql = reachable_where_sql,
        breakpoint_where_sql = breakpoint_where_sql,
        final_select_sql = final_select_sql,
    );
    parse_single_query(&sql)
}

fn should_fallback_to_phase1_query(pushdown: &HistoryPushdown) -> bool {
    pushdown
        .cse_predicates
        .iter()
        .any(|predicate| predicate.contains("rc.commit_id"))
}

fn build_lix_state_history_view_query_phase1(
    pushdown: &HistoryPushdown,
    count_fast_path: bool,
) -> Result<Query, LixError> {
    let mut all_changes_predicates = Vec::new();
    if count_fast_path {
        all_changes_predicates.push("ic.snapshot_id != 'no-content'".to_string());
    }
    all_changes_predicates.extend(
        pushdown
            .change_predicates
            .iter()
            .map(|predicate| predicate.replace("bp.", "ic.")),
    );

    let mut requested_predicates = vec![format!("c.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    requested_predicates.extend(pushdown.requested_predicates.clone());

    let mut cse_predicates = vec![format!("cse_raw.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    cse_predicates.extend(pushdown.cse_predicates.iter().map(|predicate| {
        predicate
            .replace("rc.commit_depth", "cc_raw.commit_depth")
            .replace("rc.commit_id", "cc_raw.commit_id")
    }));

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
             JOIN lix_internal_commit_ancestry ancestry \
               ON ancestry.commit_id = requested.commit_id \
             WHERE ancestry.depth <= {max_depth} \
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
        max_depth = MAX_HISTORY_DEPTH,
        all_changes_sql = all_changes_sql,
        requested_where_sql = requested_where_sql,
        cse_where_sql = cse_where_sql,
        ranked_select_sql = ranked_select_sql,
        final_select_sql = final_select_sql,
    );
    parse_single_query(&sql)
}

async fn ensure_history_timeline_materialized_for_request(
    backend: &dyn LixBackend,
    pushdown: &HistoryPushdown,
    params: &[Value],
) -> Result<(), LixError> {
    let requested_roots = resolve_requested_root_commits(backend, pushdown, params).await?;
    for root_commit_id in requested_roots {
        ensure_history_timeline_materialized_for_root(backend, &root_commit_id, MAX_HISTORY_DEPTH)
            .await?;
    }
    Ok(())
}

async fn resolve_requested_root_commits(
    backend: &dyn LixBackend,
    pushdown: &HistoryPushdown,
    params: &[Value],
) -> Result<Vec<String>, LixError> {
    let mut requested_predicates = vec![format!("c.lixcol_version_id = '{GLOBAL_VERSION_ID}'",)];
    requested_predicates.extend(pushdown.requested_predicates.clone());
    let requested_where_sql = render_where_clause(&requested_predicates);
    let sql = format!(
        "WITH commit_by_version AS ( \
           SELECT \
             entity_id AS id, \
             version_id AS lixcol_version_id \
           FROM lix_internal_state_materialized_v1_lix_commit \
           WHERE schema_key = 'lix_commit' \
             AND version_id = '{global_version}' \
             AND is_tombstone = 0 \
             AND snapshot_content IS NOT NULL \
         ) \
         SELECT DISTINCT c.id \
         FROM commit_by_version c \
         {requested_where}",
        global_version = GLOBAL_VERSION_ID,
        requested_where = requested_where_sql,
    );
    let rows = backend.execute(&sql, params).await?;
    let mut roots = BTreeSet::new();
    for row in &rows.rows {
        if let Some(id) = text_value_at(row, 0) {
            roots.insert(id.to_string());
        }
    }
    Ok(roots.into_iter().collect())
}

async fn ensure_history_timeline_materialized_for_root(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    required_depth: i64,
) -> Result<(), LixError> {
    let built_max_depth = load_timeline_built_max_depth(backend, root_commit_id).await?;
    if built_max_depth.is_some_and(|built| built >= required_depth) {
        return Ok(());
    }

    let start_depth = built_max_depth.map_or(0, |built| built.saturating_add(1));
    let query_start = if start_depth > 0 { start_depth - 1 } else { 0 };
    let source_rows = load_phase1_source_rows_for_root_range(
        backend,
        root_commit_id,
        query_start,
        required_depth,
    )
    .await?;
    let breakpoints = derive_breakpoints_from_source_rows(root_commit_id, start_depth, source_rows);
    insert_breakpoints(backend, &breakpoints).await?;
    upsert_timeline_status(backend, root_commit_id, required_depth).await?;
    Ok(())
}

async fn load_timeline_built_max_depth(
    backend: &dyn LixBackend,
    root_commit_id: &str,
) -> Result<Option<i64>, LixError> {
    let sql = format!(
        "SELECT built_max_depth \
         FROM {status_table} \
         WHERE root_commit_id = '{root_commit_id}' \
         LIMIT 1",
        status_table = TIMELINE_STATUS_TABLE,
        root_commit_id = escape_sql_string(root_commit_id),
    );
    let result = backend.execute(&sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(row.first().and_then(integer_from_value))
}

async fn load_phase1_source_rows_for_root_range(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> Result<Vec<TimelineSourceRow>, LixError> {
    if start_depth > end_depth {
        return Ok(Vec::new());
    }

    let sql =
        build_phase1_source_query_sql(backend.dialect(), root_commit_id, start_depth, end_depth);
    let result = backend.execute(&sql, &[]).await?;
    parse_timeline_source_rows(result)
}

fn build_phase1_source_query_sql(
    dialect: SqlDialect,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> String {
    let change_set_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_set_id");
    let cse_change_set_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_set_id");
    let cse_change_id_sql = json_text_expr_sql(dialect, "snapshot_content", "change_id");
    let cse_entity_id_sql = json_text_expr_sql(dialect, "snapshot_content", "entity_id");
    let cse_schema_key_sql = json_text_expr_sql(dialect, "snapshot_content", "schema_key");
    let cse_file_id_sql = json_text_expr_sql(dialect, "snapshot_content", "file_id");
    format!(
        "WITH \
           commit_by_version AS ( \
             SELECT \
               entity_id AS id, \
               {change_set_id_sql} AS change_set_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           change_set_element_by_version AS ( \
             SELECT \
               {cse_change_set_id_sql} AS change_set_id, \
               {cse_change_id_sql} AS change_id, \
               {cse_entity_id_sql} AS entity_id, \
               {cse_schema_key_sql} AS schema_key, \
               {cse_file_id_sql} AS file_id, \
               version_id AS lixcol_version_id \
             FROM lix_internal_state_materialized_v1_lix_change_set_element \
             WHERE schema_key = 'lix_change_set_element' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           all_changes AS ( \
             SELECT \
               ic.id, \
               ic.plugin_key, \
               ic.schema_version, \
               ic.metadata, \
               ic.snapshot_id, \
               ic.created_at \
             FROM lix_internal_change ic \
           ), \
           reachable_commits AS ( \
             SELECT \
               ancestry.ancestor_id AS commit_id, \
               ancestry.depth AS commit_depth \
             FROM lix_internal_commit_ancestry ancestry \
             WHERE ancestry.commit_id = '{root_commit_id}' \
               AND ancestry.depth BETWEEN {start_depth} AND {end_depth} \
           ), \
           commit_changesets AS ( \
             SELECT \
               c.id AS commit_id, \
               c.change_set_id AS change_set_id, \
               rc.commit_depth AS commit_depth \
             FROM commit_by_version c \
             JOIN reachable_commits rc ON c.id = rc.commit_id \
             WHERE c.lixcol_version_id = '{global_version}' \
           ), \
           cse_in_reachable AS ( \
             SELECT \
               cse.entity_id AS entity_id, \
               cse.schema_key AS schema_key, \
               cse.file_id AS file_id, \
               cse.change_id AS change_id, \
               cc.commit_depth AS commit_depth \
             FROM change_set_element_by_version cse \
             JOIN commit_changesets cc \
               ON cse.change_set_id = cc.change_set_id \
             WHERE cse.lixcol_version_id = '{global_version}' \
           ), \
           ranked AS ( \
             SELECT \
               r.entity_id, \
               r.schema_key, \
               r.file_id, \
               changes.plugin_key, \
               changes.schema_version, \
               changes.metadata, \
               changes.snapshot_id, \
               r.change_id, \
               r.commit_depth, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY r.entity_id, r.file_id, r.schema_key, r.commit_depth \
                 ORDER BY changes.created_at DESC, changes.id DESC \
               ) AS rn \
             FROM cse_in_reachable r \
             JOIN all_changes changes ON changes.id = r.change_id \
           ) \
         SELECT \
           ranked.entity_id, \
           ranked.schema_key, \
           ranked.file_id, \
           ranked.plugin_key, \
           ranked.schema_version, \
           ranked.metadata, \
           ranked.snapshot_id, \
           ranked.change_id, \
           ranked.commit_depth \
         FROM ranked \
         WHERE ranked.rn = 1 \
         ORDER BY \
           ranked.entity_id ASC, \
           ranked.file_id ASC, \
           ranked.schema_key ASC, \
           ranked.commit_depth ASC",
        global_version = GLOBAL_VERSION_ID,
        change_set_id_sql = change_set_id_sql,
        cse_change_set_id_sql = cse_change_set_id_sql,
        cse_change_id_sql = cse_change_id_sql,
        cse_entity_id_sql = cse_entity_id_sql,
        cse_schema_key_sql = cse_schema_key_sql,
        cse_file_id_sql = cse_file_id_sql,
        root_commit_id = escape_sql_string(root_commit_id),
        start_depth = start_depth,
        end_depth = end_depth,
    )
}

fn json_text_expr_sql(dialect: SqlDialect, column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({column}, '$.\"{field}\"')"),
        SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST({column} AS JSONB), '{field}')")
        }
    }
}

fn parse_timeline_source_rows(result: QueryResult) -> Result<Vec<TimelineSourceRow>, LixError> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        let entity_id = required_text_value(&row, 0, "entity_id")?;
        let schema_key = required_text_value(&row, 1, "schema_key")?;
        let file_id = required_text_value(&row, 2, "file_id")?;
        let plugin_key = required_text_value(&row, 3, "plugin_key")?;
        let schema_version = required_text_value(&row, 4, "schema_version")?;
        let metadata = optional_text_value(&row, 5, "metadata")?;
        let snapshot_id = required_text_value(&row, 6, "snapshot_id")?;
        let change_id = required_text_value(&row, 7, "change_id")?;
        let depth = required_integer_value(&row, 8, "commit_depth")?;

        out.push(TimelineSourceRow {
            entity_id,
            schema_key,
            file_id,
            plugin_key,
            schema_version,
            metadata,
            snapshot_id,
            change_id,
            depth,
        });
    }
    Ok(out)
}

fn derive_breakpoints_from_source_rows(
    root_commit_id: &str,
    start_depth: i64,
    source_rows: Vec<TimelineSourceRow>,
) -> Vec<TimelineBreakpointRow> {
    let mut breakpoints = Vec::new();
    let mut current_key: Option<TimelineEntityKey> = None;
    let mut current_signature: Option<TimelineStateSignature> = None;

    for row in source_rows {
        let key = TimelineEntityKey {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
        };
        let signature = TimelineStateSignature {
            plugin_key: row.plugin_key.clone(),
            schema_version: row.schema_version.clone(),
            metadata: row.metadata.clone(),
            snapshot_id: row.snapshot_id.clone(),
            change_id: row.change_id.clone(),
        };

        if current_key.as_ref() != Some(&key) {
            current_key = Some(key.clone());
            current_signature = None;
        }

        if row.depth < start_depth {
            current_signature = Some(signature);
            continue;
        }

        if current_signature.as_ref() != Some(&signature) {
            breakpoints.push(TimelineBreakpointRow {
                root_commit_id: root_commit_id.to_string(),
                entity_id: key.entity_id,
                schema_key: key.schema_key,
                file_id: key.file_id,
                from_depth: row.depth,
                plugin_key: row.plugin_key,
                schema_version: row.schema_version,
                metadata: row.metadata,
                snapshot_id: row.snapshot_id,
                change_id: row.change_id,
            });
        }

        current_signature = Some(signature);
    }

    breakpoints
}

async fn insert_breakpoints(
    backend: &dyn LixBackend,
    breakpoints: &[TimelineBreakpointRow],
) -> Result<(), LixError> {
    for breakpoint in breakpoints {
        let metadata_sql = breakpoint
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let sql = format!(
            "INSERT INTO {table} (\
               root_commit_id, entity_id, schema_key, file_id, from_depth, \
               plugin_key, schema_version, metadata, snapshot_id, change_id\
             ) VALUES (\
               '{root_commit_id}', '{entity_id}', '{schema_key}', '{file_id}', {from_depth}, \
               '{plugin_key}', '{schema_version}', {metadata_sql}, '{snapshot_id}', '{change_id}'\
             ) \
             ON CONFLICT (root_commit_id, entity_id, schema_key, file_id, from_depth) DO NOTHING",
            table = TIMELINE_BREAKPOINT_TABLE,
            root_commit_id = escape_sql_string(&breakpoint.root_commit_id),
            entity_id = escape_sql_string(&breakpoint.entity_id),
            schema_key = escape_sql_string(&breakpoint.schema_key),
            file_id = escape_sql_string(&breakpoint.file_id),
            from_depth = breakpoint.from_depth,
            plugin_key = escape_sql_string(&breakpoint.plugin_key),
            schema_version = escape_sql_string(&breakpoint.schema_version),
            metadata_sql = metadata_sql,
            snapshot_id = escape_sql_string(&breakpoint.snapshot_id),
            change_id = escape_sql_string(&breakpoint.change_id),
        );
        backend.execute(&sql, &[]).await?;
    }
    Ok(())
}

async fn upsert_timeline_status(
    backend: &dyn LixBackend,
    root_commit_id: &str,
    built_max_depth: i64,
) -> Result<(), LixError> {
    let sql = format!(
        "INSERT INTO {table} (root_commit_id, built_max_depth, built_at) \
         VALUES ('{root_commit_id}', {built_max_depth}, CURRENT_TIMESTAMP) \
         ON CONFLICT (root_commit_id) DO UPDATE \
         SET built_max_depth = CASE \
               WHEN excluded.built_max_depth > {table}.built_max_depth THEN excluded.built_max_depth \
               ELSE {table}.built_max_depth \
             END, \
             built_at = CASE \
               WHEN excluded.built_max_depth > {table}.built_max_depth THEN excluded.built_at \
               ELSE {table}.built_at \
             END",
        table = TIMELINE_STATUS_TABLE,
        root_commit_id = escape_sql_string(root_commit_id),
        built_max_depth = built_max_depth,
    );
    backend.execute(&sql, &[]).await?;
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
struct TimelineEntityKey {
    entity_id: String,
    schema_key: String,
    file_id: String,
}

#[derive(Clone)]
struct TimelineSourceRow {
    entity_id: String,
    schema_key: String,
    file_id: String,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
    depth: i64,
}

#[derive(Clone, PartialEq, Eq)]
struct TimelineStateSignature {
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
}

struct TimelineBreakpointRow {
    root_commit_id: String,
    entity_id: String,
    schema_key: String,
    file_id: String,
    from_depth: i64,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
    snapshot_id: String,
    change_id: String,
}

fn text_value_at(row: &[Value], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn required_text_value(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) => Ok(value.clone()),
        Some(other) => Err(LixError {
            message: format!("expected text for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            message: format!("missing column {field} at index {index}"),
        }),
    }
}

fn optional_text_value(
    row: &[Value],
    index: usize,
    field: &str,
) -> Result<Option<String>, LixError> {
    match row.get(index) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(other) => Err(LixError {
            message: format!("expected nullable text for {field}, got {other:?}"),
        }),
    }
}

fn required_integer_value(row: &[Value], index: usize, field: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(value) => integer_from_value(value).ok_or_else(|| LixError {
            message: format!("expected integer for {field}, got {value:?}"),
        }),
        None => Err(LixError {
            message: format!("missing column {field} at index {index}"),
        }),
    }
}

fn integer_from_value(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(value) => Some(*value),
        Value::Real(value) => Some(*value as i64),
        Value::Text(value) => value.parse::<i64>().ok(),
        _ => None,
    }
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

        assert!(sql.contains("bp.schema_key = ?"));
        assert!(sql.contains("c.id = ?"));
        assert!(!sql.contains("sh.schema_key = ?"));
        assert!(!sql.contains("sh.root_commit_id = ?"));
        assert!(sql.contains("SELECT COUNT(*) AS count"));
        assert!(sql.contains("FROM history_rows h"));
        assert!(sql.contains("no-content"));
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
        assert!(!sql.contains("bp.schema_key = ?"));
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
        assert!(sql.contains("bp.schema_key = ?2"));
        assert!(!sql.contains("sh.root_commit_id = ?1"));
        assert!(!sql.contains("sh.schema_key = ?2"));
        assert!(sql.contains("SELECT COUNT(*) AS count"));
        assert!(sql.contains("FROM history_rows h"));
        assert!(sql.contains("no-content"));
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

        assert!(sql.contains("JOIN lix_internal_commit_ancestry ancestry"));
        assert!(sql.contains("FROM lix_internal_entity_state_timeline_breakpoint bp"));
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
