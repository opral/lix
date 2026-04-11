use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::backend::{PreparedBatch, PreparedStatement};
use crate::canonical::journal::CanonicalCommitOutput;
use crate::canonical::read::CommitQueryExecutor;
use crate::Value as EngineValue;
use crate::{LixError, SqlDialect};

pub(crate) const COMMIT_GRAPH_NODE_TABLE: &str = "lix_internal_commit_graph_node";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitGraphNodeWriteRow {
    pub(crate) commit_id: String,
    pub(crate) generation: i64,
}

pub(crate) async fn resolve_commit_graph_node_write_rows_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    canonical_output: &CanonicalCommitOutput,
) -> Result<Vec<CommitGraphNodeWriteRow>, LixError> {
    let parent_map = collect_commit_parent_map(canonical_output)?;
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
        let p1 = dialect.placeholder(1);
        let p2 = dialect.placeholder(2);
        batch.push_statement(PreparedStatement {
            sql: format!(
                "INSERT INTO lix_internal_commit_graph_node (commit_id, generation) \
                 VALUES ({p1}, {p2}) \
                 ON CONFLICT (commit_id) DO UPDATE SET generation = excluded.generation"
            ),
            params: vec![
                EngineValue::Text(row.commit_id.clone()),
                EngineValue::Integer(row.generation),
            ],
        });
    }
    Ok(batch)
}

fn collect_commit_parent_map(
    canonical_output: &CanonicalCommitOutput,
) -> Result<BTreeMap<String, BTreeSet<String>>, LixError> {
    let mut out = BTreeMap::<String, BTreeSet<String>>::new();
    for row in &canonical_output.changes {
        if row.schema_key != "lix_commit" {
            continue;
        }
        let Some(raw) = row.snapshot_content.as_deref() else {
            continue;
        };
        out.entry(row.entity_id.to_string())
            .or_default()
            .extend(parse_commit_snapshot_parent_ids(raw)?);
    }
    Ok(out)
}

fn parse_commit_snapshot_parent_ids(raw: &str) -> Result<BTreeSet<String>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("commit snapshot invalid JSON: {error}"),
    })?;
    let Some(parent_commit_ids) = parsed
        .get("parent_commit_ids")
        .and_then(JsonValue::as_array)
    else {
        return Ok(BTreeSet::new());
    };

    let mut out = BTreeSet::new();
    for parent_commit_id in parent_commit_ids {
        let Some(parent_commit_id) = parent_commit_id.as_str().filter(|value| !value.is_empty())
        else {
            continue;
        };
        out.insert(parent_commit_id.to_string());
    }
    Ok(out)
}

async fn load_commit_graph_generation_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
) -> Result<Option<i64>, LixError> {
    let dialect = executor.dialect();
    let p1 = dialect.placeholder(1);
    let sql = format!(
        "SELECT generation FROM {table} WHERE commit_id = {p1}",
        table = COMMIT_GRAPH_NODE_TABLE
    );
    let params = vec![EngineValue::Text(commit_id.to_string())];
    let result = executor.execute(&sql, &params).await?;
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
