use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::backend::prepared::{PreparedBatch, PreparedStatement};
use crate::sql::binder::bind_sql;
use crate::version::GLOBAL_VERSION_ID;
use crate::Value as EngineValue;
use crate::{LixError, SqlDialect};

use super::graph_sql::{
    build_commit_generation_seed_sql as build_commit_generation_seed_sql_impl,
    build_exact_commit_depth_cte_sql as build_exact_commit_depth_cte_sql_impl,
};
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

pub(crate) fn build_commit_graph_node_prepared_batch(
    rows: &[CommitGraphNodeWriteRow],
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
    let mut batch = PreparedBatch { steps: Vec::new() };
    for row in rows {
        let bound = bind_sql(
            "INSERT INTO lix_internal_commit_graph_node (commit_id, generation) \
             VALUES (?1, ?2) \
             ON CONFLICT (commit_id) DO UPDATE SET generation = excluded.generation",
            &[
                EngineValue::Text(row.commit_id.clone()),
                EngineValue::Integer(row.generation),
            ],
            dialect,
        )?;
        batch.push_statement(PreparedStatement {
            sql: bound.sql,
            params: bound.params,
        });
    }
    Ok(batch)
}

#[allow(dead_code)]
pub(crate) fn build_exact_commit_depth_cte_sql(
    dialect: SqlDialect,
    root_placeholder: &str,
    target_placeholder: &str,
    fallback_depth_placeholder: &str,
) -> String {
    build_exact_commit_depth_cte_sql_impl(
        dialect,
        root_placeholder,
        target_placeholder,
        fallback_depth_placeholder,
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
    build_commit_generation_seed_sql_impl()
}
