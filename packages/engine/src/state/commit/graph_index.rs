use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;

use crate::sql::storage::sql_text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, SqlDialect};

use super::runtime::parse_single_statement_from_sql;
use super::types::MaterializedStateRow;
use crate::Value as EngineValue;

pub(crate) const COMMIT_GRAPH_NODE_TABLE: &str = "lix_internal_commit_graph_node";

pub(crate) fn append_commit_graph_node_statements(
    statements: &mut Vec<Statement>,
    params: &mut Vec<EngineValue>,
    next_placeholder: &mut usize,
    live_state_rows: &[MaterializedStateRow],
) -> Result<(), LixError> {
    let commit_parents = collect_commit_parent_map(live_state_rows)?;
    for (commit_id, parent_ids) in commit_parents {
        let commit_placeholder = *next_placeholder;
        *next_placeholder += 1;
        params.push(EngineValue::Text(commit_id));

        let sql = if parent_ids.is_empty() {
            format!(
                "INSERT INTO {table} (commit_id, generation) \
                 VALUES (?{commit_placeholder}, 0) \
                 ON CONFLICT (commit_id) DO UPDATE \
                 SET generation = CASE \
                   WHEN excluded.generation > {table}.generation THEN excluded.generation \
                   ELSE {table}.generation \
                 END",
                table = COMMIT_GRAPH_NODE_TABLE,
                commit_placeholder = commit_placeholder,
            )
        } else {
            let mut parent_rows = Vec::with_capacity(parent_ids.len());
            for parent_id in parent_ids {
                let parent_placeholder = *next_placeholder;
                *next_placeholder += 1;
                params.push(EngineValue::Text(parent_id));
                parent_rows.push(format!("SELECT ?{parent_placeholder} AS parent_id"));
            }
            format!(
                "INSERT INTO {table} (commit_id, generation) \
                 SELECT ?{commit_placeholder} AS commit_id, COALESCE(MAX(parent_node.generation), -1) + 1 AS generation \
                 FROM ({parent_rows}) parents \
                 LEFT JOIN {table} parent_node \
                   ON parent_node.commit_id = parents.parent_id \
                 ON CONFLICT (commit_id) DO UPDATE \
                 SET generation = CASE \
                   WHEN excluded.generation > {table}.generation THEN excluded.generation \
                   ELSE {table}.generation \
                 END",
                table = COMMIT_GRAPH_NODE_TABLE,
                commit_placeholder = commit_placeholder,
                parent_rows = parent_rows.join(" UNION ALL "),
            )
        };
        statements.push(parse_single_statement_from_sql(&sql)?);
    }
    Ok(())
}

pub(crate) fn build_reachable_commits_for_root_cte_sql(
    dialect: SqlDialect,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> String {
    let edge_parent_expr =
        commit_edge_json_text_expr(dialect, "edge.snapshot_content", "parent_id");
    let edge_child_expr = commit_edge_json_text_expr(dialect, "edge.snapshot_content", "child_id");
    format!(
        "reachable_commit_walk AS ( \
           SELECT '{root_commit_id}' AS commit_id, 0 AS commit_depth \
           UNION ALL \
           SELECT \
             {edge_parent_expr} AS commit_id, \
             walk.commit_depth + 1 AS commit_depth \
           FROM reachable_commit_walk walk \
           JOIN lix_internal_live_v1_lix_commit_edge edge \
             ON {edge_child_expr} = walk.commit_id \
           WHERE edge.schema_key = 'lix_commit_edge' \
             AND edge.version_id = '{global_version}' \
             AND edge.is_tombstone = 0 \
             AND edge.snapshot_content IS NOT NULL \
             AND {edge_parent_expr} IS NOT NULL \
             AND walk.commit_depth < {end_depth} \
         ), \
         reachable_commits AS ( \
           SELECT commit_id, MIN(commit_depth) AS commit_depth \
           FROM reachable_commit_walk \
           WHERE commit_depth BETWEEN {start_depth} AND {end_depth} \
           GROUP BY commit_id \
         ), ",
        root_commit_id = escape_sql_string(root_commit_id),
        global_version = GLOBAL_VERSION_ID,
        start_depth = start_depth,
        end_depth = end_depth,
        edge_parent_expr = edge_parent_expr,
        edge_child_expr = edge_child_expr,
    )
}

pub(crate) fn build_reachable_commits_from_requested_cte_sql(
    _dialect: SqlDialect,
    requested_commits_cte_name: &str,
    max_depth: i64,
) -> String {
    let edge_parent_expr = "lix_json_extract(edge.snapshot_content, 'parent_id')";
    let edge_child_expr = "lix_json_extract(edge.snapshot_content, 'child_id')";
    format!(
        "reachable_commit_walk AS ( \
           SELECT \
             requested.commit_id AS commit_id, \
             requested.commit_id AS root_commit_id, \
             requested.root_version_id AS root_version_id, \
             0 AS commit_depth \
           FROM {requested_commits_cte_name} requested \
           UNION ALL \
           SELECT \
             {edge_parent_expr} AS commit_id, \
             walk.root_commit_id AS root_commit_id, \
             walk.root_version_id AS root_version_id, \
             walk.commit_depth + 1 AS commit_depth \
           FROM reachable_commit_walk walk \
           JOIN lix_internal_live_v1_lix_commit_edge edge \
             ON {edge_child_expr} = walk.commit_id \
           WHERE edge.schema_key = 'lix_commit_edge' \
             AND edge.version_id = '{global_version}' \
             AND edge.is_tombstone = 0 \
             AND edge.snapshot_content IS NOT NULL \
             AND {edge_parent_expr} IS NOT NULL \
             AND walk.commit_depth < {max_depth} \
         ), \
         reachable_commits AS ( \
           SELECT \
             commit_id, \
             root_commit_id, \
             root_version_id, \
             MIN(commit_depth) AS commit_depth \
           FROM reachable_commit_walk \
           GROUP BY commit_id, root_commit_id, root_version_id \
         ), ",
        requested_commits_cte_name = requested_commits_cte_name,
        global_version = GLOBAL_VERSION_ID,
        max_depth = max_depth,
        edge_parent_expr = edge_parent_expr,
        edge_child_expr = edge_child_expr,
    )
}

pub(crate) fn build_exact_commit_depth_cte_sql(
    _dialect: SqlDialect,
    root_placeholder: &str,
    target_placeholder: &str,
    fallback_depth_placeholder: &str,
) -> String {
    let edge_parent_expr = "lix_json_extract(edge.snapshot_content, 'parent_id')";
    let edge_child_expr = "lix_json_extract(edge.snapshot_content, 'child_id')";
    format!(
        "target_commit_depth AS ( \
           WITH RECURSIVE reachable(commit_id, depth) AS ( \
             SELECT {root_placeholder} AS commit_id, 0 AS depth \
             UNION ALL \
             SELECT \
               {edge_parent_expr} AS commit_id, \
               reachable.depth + 1 AS depth \
             FROM reachable \
             JOIN lix_internal_live_v1_lix_commit_edge edge \
               ON {edge_child_expr} = reachable.commit_id \
             WHERE edge.schema_key = 'lix_commit_edge' \
               AND edge.version_id = '{global_version}' \
               AND edge.is_tombstone = 0 \
               AND edge.snapshot_content IS NOT NULL \
               AND {edge_parent_expr} IS NOT NULL \
               AND reachable.depth < {fallback_depth_placeholder} \
           ) \
           SELECT COALESCE(( \
             SELECT MIN(depth) \
             FROM reachable \
             WHERE commit_id = {target_placeholder} \
           ), {fallback_depth_placeholder}) AS raw_depth \
         ), ",
        root_placeholder = root_placeholder,
        target_placeholder = target_placeholder,
        fallback_depth_placeholder = fallback_depth_placeholder,
        global_version = GLOBAL_VERSION_ID,
        edge_parent_expr = edge_parent_expr,
        edge_child_expr = edge_child_expr,
    )
}

fn commit_edge_json_text_expr(dialect: SqlDialect, column: &str, field: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => format!("json_extract({column}, '$.{field}')"),
        SqlDialect::Postgres => {
            format!("jsonb_extract_path_text(CAST({column} AS JSONB), '{field}')")
        }
    }
}

fn collect_commit_parent_map(
    live_state_rows: &[MaterializedStateRow],
) -> Result<BTreeMap<String, BTreeSet<String>>, LixError> {
    let mut out = BTreeMap::<String, BTreeSet<String>>::new();
    for row in live_state_rows {
        if row.schema_key == "lix_commit" && row.lixcol_version_id == GLOBAL_VERSION_ID {
            out.entry(row.entity_id.clone()).or_default();
        }
    }

    for row in live_state_rows {
        if row.schema_key != "lix_commit_edge" || row.lixcol_version_id != GLOBAL_VERSION_ID {
            continue;
        }
        let Some(raw) = row.snapshot_content.as_deref() else {
            continue;
        };
        let Some((parent_id, child_id)) = parse_commit_edge_snapshot(raw)? else {
            continue;
        };
        if let Some(parents) = out.get_mut(&child_id) {
            parents.insert(parent_id);
        }
    }
    Ok(out)
}

fn parse_commit_edge_snapshot(raw: &str) -> Result<Option<(String, String)>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("commit_edge snapshot invalid JSON: {error}"),
    })?;
    let parent_id = parsed
        .get("parent_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let child_id = parsed
        .get("child_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    match (parent_id, child_id) {
        (Some(parent_id), Some(child_id)) => Ok(Some((parent_id, child_id))),
        _ => Ok(None),
    }
}

pub(crate) fn build_commit_generation_seed_sql() -> String {
    format!(
        "WITH RECURSIVE \
           commits AS ( \
             SELECT entity_id AS commit_id \
             FROM lix_internal_live_v1_lix_commit \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           edges AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'parent_id') AS parent_id, \
               lix_json_extract(snapshot_content, 'child_id') AS child_id \
             FROM lix_internal_live_v1_lix_commit_edge \
             WHERE schema_key = 'lix_commit_edge' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
               AND lix_json_extract(snapshot_content, 'parent_id') IS NOT NULL \
               AND lix_json_extract(snapshot_content, 'child_id') IS NOT NULL \
           ), \
           roots AS ( \
             SELECT c.commit_id \
             FROM commits c \
             LEFT JOIN edges e ON e.child_id = c.commit_id \
             WHERE e.child_id IS NULL \
           ), \
           walk(commit_id, generation) AS ( \
             SELECT r.commit_id, 0 AS generation \
             FROM roots r \
             UNION ALL \
             SELECT e.child_id, walk.generation + 1 AS generation \
             FROM walk \
             JOIN edges e ON e.parent_id = walk.commit_id \
           ) \
         INSERT INTO {table} (commit_id, generation) \
         SELECT commit_id, MAX(generation) AS generation \
         FROM walk \
         GROUP BY commit_id \
         ON CONFLICT (commit_id) DO UPDATE \
         SET generation = CASE \
           WHEN excluded.generation > {table}.generation THEN excluded.generation \
           ELSE {table}.generation \
         END",
        table = COMMIT_GRAPH_NODE_TABLE,
        global_version = GLOBAL_VERSION_ID,
    )
}
