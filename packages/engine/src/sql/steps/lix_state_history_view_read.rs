use std::collections::BTreeSet;

use sqlparser::ast::{
    BinaryOperator, Expr, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins,
};

use crate::sql::steps::state_pushdown::select_supports_count_fast_path;
use crate::sql::{
    default_alias, object_name_matches, parse_single_query, parse_sql_statements,
    resolve_requested_root_commits_from_predicates, rewrite_query_with_select_rewriter,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};

const LIX_STATE_HISTORY_VIEW_NAME: &str = "lix_state_history";
const MAX_HISTORY_DEPTH: i64 = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateHistoryRequirement {
    pub requested_root_commit_ids: BTreeSet<String>,
    pub required_max_depth: i64,
}

pub fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    let (rewritten, _requests) = rewrite_query_collect_requests(query)?;
    Ok(rewritten)
}

pub(crate) async fn collect_history_requirements_with_backend(
    backend: &dyn LixBackend,
    query: &Query,
    params: &[Value],
) -> Result<Vec<StateHistoryRequirement>, LixError> {
    let (_rewritten, requests) = rewrite_query_collect_requests(query.clone())?;
    let mut out = Vec::new();
    let mut seen_requests = BTreeSet::new();
    for request in requests {
        if should_fallback_to_phase1_query(&request) {
            continue;
        }
        let request_key = request_dedup_key(&request);
        if !seen_requests.insert(request_key) {
            continue;
        }
        let requested_root_commit_ids = resolve_requested_root_commits_from_predicates(
            backend,
            &request.requested_predicates,
            params,
        )
        .await?
        .into_iter()
        .collect();
        out.push(StateHistoryRequirement {
            requested_root_commit_ids,
            required_max_depth: MAX_HISTORY_DEPTH,
        });
    }
    Ok(out)
}

pub async fn rewrite_query_with_backend(
    _backend: &dyn LixBackend,
    query: Query,
    _params: &[Value],
) -> Result<Option<Query>, LixError> {
    rewrite_query(query)
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
    requested_pushdown_blocked_by_bare_placeholders: bool,
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

    let has_cross_bucket_bare_placeholders = has_cross_bucket_bare_placeholders(&parts);

    let mut pushdown = HistoryPushdown::default();
    let mut remaining = Vec::new();
    for part in parts {
        let blocked_by_cross_bucket =
            part.has_bare_placeholder && has_cross_bucket_bare_placeholders;
        match part.extracted {
            Some(ExtractedPredicate::Push(bucket, sql)) if !blocked_by_cross_bucket => match bucket
            {
                HistoryPushdownBucket::Change => pushdown.change_predicates.push(sql),
                HistoryPushdownBucket::Requested => pushdown.requested_predicates.push(sql),
                HistoryPushdownBucket::Cse => pushdown.cse_predicates.push(sql),
                HistoryPushdownBucket::Remaining => remaining.push(part.predicate),
            },
            Some(ExtractedPredicate::Push(HistoryPushdownBucket::Requested, _))
                if blocked_by_cross_bucket =>
            {
                pushdown.requested_pushdown_blocked_by_bare_placeholders = true;
                remaining.push(part.predicate);
            }
            Some(ExtractedPredicate::Drop) if !blocked_by_cross_bucket => {}
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
}

fn has_cross_bucket_bare_placeholders(parts: &[PredicatePart]) -> bool {
    let mut first_bucket: Option<HistoryPushdownBucket> = None;
    for part in parts {
        if !part.has_bare_placeholder {
            continue;
        }
        let bucket = match &part.extracted {
            Some(ExtractedPredicate::Push(bucket, _)) => *bucket,
            Some(ExtractedPredicate::Drop) | None => HistoryPushdownBucket::Remaining,
        };
        match first_bucket {
            None => first_bucket = Some(bucket),
            Some(existing) if existing != bucket => return true,
            Some(_) => {}
        }
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

fn request_dedup_key(request: &HistoryPushdown) -> String {
    format!(
        "{}||{}||{}",
        request.change_predicates.join("&&"),
        request.requested_predicates.join("&&"),
        request.cse_predicates.join("&&")
    )
}

fn should_fallback_to_phase1_query(pushdown: &HistoryPushdown) -> bool {
    pushdown.requested_pushdown_blocked_by_bare_placeholders
        || pushdown
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
            .map(|predicate| remap_change_predicate_for_phase1(predicate))
            .collect::<Result<Vec<_>, _>>()?,
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

fn remap_change_predicate_for_phase1(predicate: &str) -> Result<String, LixError> {
    let mut expr = parse_phase1_predicate_expr(predicate)?;
    remap_phase1_change_predicate_expr(&mut expr);
    Ok(expr.to_string())
}

fn parse_phase1_predicate_expr(predicate: &str) -> Result<Expr, LixError> {
    let sql = format!("SELECT 1 WHERE {predicate}");
    let mut statements = parse_sql_statements(&sql)?;
    let Some(statement) = statements.pop() else {
        return Err(LixError {
            message: "phase1 change predicate parse produced no statements".to_string(),
        });
    };
    let Statement::Query(query) = statement else {
        return Err(LixError {
            message: "phase1 change predicate parse did not produce a query".to_string(),
        });
    };
    let SetExpr::Select(select) = *query.body else {
        return Err(LixError {
            message: "phase1 change predicate parse did not produce a SELECT body".to_string(),
        });
    };
    let Some(selection) = select.selection else {
        return Err(LixError {
            message: "phase1 change predicate parse produced no WHERE expression".to_string(),
        });
    };
    Ok(selection)
}

fn remap_phase1_change_predicate_expr(expr: &mut Expr) {
    match expr {
        Expr::BinaryOp { left, .. } => remap_phase1_change_predicate_column(left.as_mut()),
        Expr::InList { expr, .. } => remap_phase1_change_predicate_column(expr.as_mut()),
        Expr::InSubquery { expr, .. } => remap_phase1_change_predicate_column(expr.as_mut()),
        _ => {}
    }
}

fn remap_phase1_change_predicate_column(expr: &mut Expr) {
    let Expr::CompoundIdentifier(parts) = expr else {
        return;
    };
    if parts.len() != 2 || !parts[0].value.eq_ignore_ascii_case("bp") {
        return;
    }
    parts[0].value = "ic".to_string();
    if parts[1].value.eq_ignore_ascii_case("change_id") {
        parts[1].value = "id".to_string();
    }
}

fn default_lix_state_history_alias() -> sqlparser::ast::TableAlias {
    default_alias(LIX_STATE_HISTORY_VIEW_NAME)
}

#[cfg(test)]
mod tests {
    use super::{rewrite_query, rewrite_query_collect_requests};
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn pushes_history_schema_and_root_commit_filters_into_ctes_for_count_fast_path() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.schema_key = ?2 \
               AND sh.root_commit_id = ?1 \
               AND sh.snapshot_content IS NOT NULL",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("bp.schema_key = ?2"));
        assert!(sql.contains("c.id = ?1"));
        assert!(!sql.contains("sh.schema_key = ?2"));
        assert!(!sql.contains("sh.root_commit_id = ?1"));
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
    fn does_not_push_down_bare_placeholders_across_buckets_when_schema_precedes_root() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.schema_key = ? \
               AND sh.root_commit_id = ?",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(!sql.contains("c.id = ?"));
        assert!(!sql.contains("bp.schema_key = ?"));
        assert!(sql.contains("sh.schema_key = ?"));
        assert!(sql.contains("sh.root_commit_id = ?"));
        assert!(!sql.contains("FROM lix_internal_entity_state_timeline_breakpoint"));
        assert!(sql.contains("ranked_cse"));
    }

    #[test]
    fn marks_requested_pushdown_as_blocked_when_bare_placeholders_cross_buckets() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_history AS sh \
             WHERE sh.schema_key = ? \
               AND sh.root_commit_id = ?",
        );

        let (_rewritten, requests) =
            rewrite_query_collect_requests(query).expect("rewrite should succeed");
        assert_eq!(requests.len(), 1);
        let request = requests.into_iter().next().expect("request should exist");

        assert!(request.requested_pushdown_blocked_by_bare_placeholders);
        assert!(request.requested_predicates.is_empty());
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
    fn phase1_fallback_maps_change_id_predicate_to_change_table_primary_key() {
        let query = parse_query(
            "SELECT * \
             FROM lix_state_history AS sh \
             WHERE sh.commit_id = 'commit-a' \
               AND sh.change_id = 'change-a'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("cc_raw.commit_id = 'commit-a'"));
        assert!(sql.contains("ic.id = 'change-a'"));
        assert!(!sql.contains("ic.change_id = 'change-a'"));
    }

    #[test]
    fn phase1_fallback_does_not_rewrite_bp_substrings_inside_literals() {
        let query = parse_query(
            "SELECT * \
             FROM lix_state_history AS sh \
             WHERE sh.commit_id = 'commit-a' \
               AND sh.schema_key = 'bp.keep.this'",
        );

        let rewritten = rewrite_query(query)
            .expect("rewrite should succeed")
            .expect("query should be rewritten");
        let sql = rewritten.to_string();

        assert!(sql.contains("cc_raw.commit_id = 'commit-a'"));
        assert!(sql.contains("ic.schema_key = 'bp.keep.this'"));
        assert!(!sql.contains("'ic.keep.this'"));
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
