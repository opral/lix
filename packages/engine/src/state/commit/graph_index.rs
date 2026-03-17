use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;

use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, tracked_live_table_name,
};
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
    _dialect: SqlDialect,
    root_commit_id: &str,
    start_depth: i64,
    end_depth: i64,
) -> String {
    let commit_edge_table = tracked_live_table_name("lix_commit_edge");
    let edge_parent_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "parent_id"));
    let edge_child_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "child_id"));
    format!(
        "reachable_commit_walk AS ( \
           SELECT '{root_commit_id}' AS commit_id, 0 AS commit_depth \
           UNION ALL \
           SELECT \
             {edge_parent_expr} AS commit_id, \
             walk.commit_depth + 1 AS commit_depth \
           FROM reachable_commit_walk walk \
           JOIN {commit_edge_table} edge \
             ON {edge_child_expr} = walk.commit_id \
           WHERE edge.schema_key = 'lix_commit_edge' \
             AND edge.version_id = '{global_version}' \
             AND edge.is_tombstone = 0 \
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
    let commit_edge_table = tracked_live_table_name("lix_commit_edge");
    let edge_parent_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "parent_id"));
    let edge_child_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "child_id"));
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
           JOIN {commit_edge_table} edge \
             ON {edge_child_expr} = walk.commit_id \
           WHERE edge.schema_key = 'lix_commit_edge' \
             AND edge.version_id = '{global_version}' \
             AND edge.is_tombstone = 0 \
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
    let commit_edge_table = tracked_live_table_name("lix_commit_edge");
    let edge_parent_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "parent_id"));
    let edge_child_expr = quote_ident(&live_payload_column_name("lix_commit_edge", "child_id"));
    format!(
        "target_commit_depth AS ( \
           WITH RECURSIVE reachable(commit_id, depth) AS ( \
             SELECT {root_placeholder} AS commit_id, 0 AS depth \
             UNION ALL \
             SELECT \
               {edge_parent_expr} AS commit_id, \
               reachable.depth + 1 AS depth \
             FROM reachable \
             JOIN {commit_edge_table} edge \
               ON {edge_child_expr} = reachable.commit_id \
             WHERE edge.schema_key = 'lix_commit_edge' \
               AND edge.version_id = '{global_version}' \
               AND edge.is_tombstone = 0 \
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
    let commit_table = tracked_live_table_name("lix_commit");
    let commit_edge_table = tracked_live_table_name("lix_commit_edge");
    let commit_edge_parent_id_column =
        quote_ident(&live_payload_column_name("lix_commit_edge", "parent_id"));
    let commit_edge_child_id_column =
        quote_ident(&live_payload_column_name("lix_commit_edge", "child_id"));
    format!(
        "WITH RECURSIVE \
           commits AS ( \
             SELECT entity_id AS commit_id \
             FROM {commit_table} \
             WHERE schema_key = 'lix_commit' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
           ), \
           edges AS ( \
             SELECT \
               {commit_edge_parent_id_column} AS parent_id, \
               {commit_edge_child_id_column} AS child_id \
             FROM {commit_edge_table} \
             WHERE schema_key = 'lix_commit_edge' \
               AND version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND {commit_edge_parent_id_column} IS NOT NULL \
               AND {commit_edge_child_id_column} IS NOT NULL \
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
        commit_edge_parent_id_column = commit_edge_parent_id_column,
        commit_edge_child_id_column = commit_edge_child_id_column,
    )
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn live_payload_column_name(schema_key: &str, property_name: &str) -> String {
    let layout = builtin_live_table_layout(schema_key)
        .expect("builtin live layout lookup should succeed")
        .expect("builtin live layout should exist");
    live_column_name_for_property(&layout, property_name)
        .unwrap_or_else(|| {
            panic!("builtin live layout '{schema_key}' must include '{property_name}'")
        })
        .to_string()
}
