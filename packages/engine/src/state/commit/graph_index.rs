use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::schema::live_layout::{
    builtin_live_table_layout, live_column_name_for_property, tracked_live_table_name,
};
use crate::sql::ast::utils::bind_sql;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::Value as EngineValue;
use crate::{LixError, SqlDialect};

use super::state_source::CommitQueryExecutor;
use super::types::MaterializedStateRow;

pub(crate) const COMMIT_GRAPH_NODE_TABLE: &str = "lix_internal_commit_graph_node";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphNodeWriteRow {
    pub(crate) commit_id: String,
    pub(crate) generation: i64,
}

pub(crate) async fn resolve_commit_graph_node_write_rows_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    live_state_rows: &[MaterializedStateRow],
) -> Result<Vec<CommitGraphNodeWriteRow>, LixError> {
    let parent_map = collect_commit_parent_map(live_state_rows)?;
    if parent_map.is_empty() {
        return Ok(Vec::new());
    }

    let new_commit_ids = parent_map.keys().cloned().collect::<BTreeSet<_>>();
    let external_parent_ids = parent_map
        .values()
        .flat_map(|parents| parents.iter().cloned())
        .filter(|parent_id| !new_commit_ids.contains(parent_id))
        .collect::<BTreeSet<_>>();

    let mut external_generations = BTreeMap::new();
    for parent_id in external_parent_ids {
        if let Some(generation) =
            load_commit_graph_generation_with_executor(executor, &parent_id).await?
        {
            external_generations.insert(parent_id, generation);
        }
    }

    let mut resolved = BTreeMap::new();
    let mut resolving = BTreeSet::new();
    let mut rows = Vec::with_capacity(parent_map.len());
    for commit_id in parent_map.keys() {
        let generation = resolve_commit_generation(
            commit_id,
            &parent_map,
            &external_generations,
            &mut resolved,
            &mut resolving,
        )?;
        rows.push(CommitGraphNodeWriteRow {
            commit_id: commit_id.clone(),
            generation,
        });
    }

    Ok(rows)
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

#[allow(dead_code)]
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
            out.entry(row.entity_id.to_string()).or_default();
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

async fn load_commit_graph_generation_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
) -> Result<Option<i64>, LixError> {
    let bound = bind_sql(
        &format!(
            "SELECT generation FROM {table} WHERE commit_id = ?1",
            table = COMMIT_GRAPH_NODE_TABLE
        ),
        &[EngineValue::Text(commit_id.to_string())],
        executor.dialect(),
    )?;
    let result = executor.execute(&bound.sql, &bound.params).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    match value {
        EngineValue::Integer(value) => Ok(Some(*value)),
        EngineValue::Null => Ok(None),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "commit graph generation must be integer".to_string(),
        }),
    }
}

fn resolve_commit_generation(
    commit_id: &str,
    parent_map: &BTreeMap<String, BTreeSet<String>>,
    external_generations: &BTreeMap<String, i64>,
    resolved: &mut BTreeMap<String, i64>,
    resolving: &mut BTreeSet<String>,
) -> Result<i64, LixError> {
    if let Some(generation) = resolved.get(commit_id) {
        return Ok(*generation);
    }
    if !resolving.insert(commit_id.to_string()) {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "commit graph contains a cycle".to_string(),
        });
    }

    let mut max_parent_generation = -1_i64;
    if let Some(parent_ids) = parent_map.get(commit_id) {
        for parent_id in parent_ids {
            let parent_generation = if let Some(generation) = resolved.get(parent_id) {
                Some(*generation)
            } else if let Some(generation) = external_generations.get(parent_id) {
                Some(*generation)
            } else if parent_map.contains_key(parent_id) {
                Some(resolve_commit_generation(
                    parent_id,
                    parent_map,
                    external_generations,
                    resolved,
                    resolving,
                )?)
            } else {
                None
            };
            if let Some(parent_generation) = parent_generation {
                max_parent_generation = max_parent_generation.max(parent_generation);
            }
        }
    }

    resolving.remove(commit_id);
    let generation = max_parent_generation + 1;
    resolved.insert(commit_id.to_string(), generation);
    Ok(generation)
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
