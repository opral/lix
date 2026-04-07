//! Checkpoint-history helpers.
//!
//! Checkpoint labels are canonical commit-graph facts. This module owns the
//! rebuildable history/filtering helpers derived from those facts, including
//! `lix_internal_last_checkpoint`.
//!
//! Replay cursor/status state is explicitly out of scope here. That operational
//! state belongs under `live_state/projection/*`.

use std::collections::BTreeSet;

use crate::backend::QueryExecutor;
use crate::canonical::graph::COMMIT_GRAPH_NODE_TABLE;
use crate::canonical::read::load_commit_lineage_entry_by_id;
use crate::checkpoint_artifacts::checkpoint_commit_label_entity_id;
use crate::errors::classification::is_missing_relation_error;
use crate::init::seed::text_value;
use crate::init::InitExecutor;
use crate::text::escape_sql_string;
use crate::{LixError, Value};

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn insert_last_checkpoint_for_version(
        &mut self,
        version_id: &str,
        checkpoint_commit_id: &str,
    ) -> Result<(), LixError> {
        self.execute_backend(
            "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
             VALUES ($1, $2)",
            &[
                Value::Text(version_id.to_string()),
                Value::Text(checkpoint_commit_id.to_string()),
            ],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn rebuild_internal_last_checkpoint(&mut self) -> Result<(), LixError> {
        let version_descriptors = {
            let mut backend = self.backend_adapter();
            crate::canonical::read::load_all_version_descriptors_with_executor(&mut backend).await?
        };

        // `lix_internal_last_checkpoint` is derived checkpoint-history state.
        // Rebuild it from canonical version heads plus canonical checkpoint
        // labels attached to commits.
        self.execute_backend("DELETE FROM lix_internal_last_checkpoint", &[])
            .await?;

        let global_commit_id = self.load_global_version_commit_id().await?;
        let global_checkpoint_commit_id = {
            let mut backend = self.backend_adapter();
            resolve_last_checkpoint_commit_id_for_tip_with_executor(&mut backend, &global_commit_id)
                .await?
        }
        .unwrap_or_else(|| global_commit_id.clone());
        self.insert_last_checkpoint_for_version(
            crate::version::GLOBAL_VERSION_ID,
            &global_checkpoint_commit_id,
        )
        .await?;

        for descriptor in &version_descriptors {
            let version_id = descriptor.version_id.clone();
            if version_id == crate::version::GLOBAL_VERSION_ID {
                continue;
            }
            let commit_id = {
                let mut backend = self.backend_adapter();
                crate::version::load_committed_version_head_commit_id(&mut backend, &version_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("version '{version_id}' is missing a committed head"),
                        )
                    })?
            };
            let checkpoint_commit_id = {
                let mut backend = self.backend_adapter();
                resolve_last_checkpoint_commit_id_for_tip_with_executor(&mut backend, &commit_id)
                    .await?
            }
            .unwrap_or_else(|| commit_id.clone());
            self.insert_last_checkpoint_for_version(&version_id, &checkpoint_commit_id)
                .await?;
        }

        Ok(())
    }
}

pub(crate) async fn resolve_last_checkpoint_commit_id_for_tip_with_executor(
    executor: &mut dyn QueryExecutor,
    head_commit_id: &str,
) -> Result<Option<String>, LixError> {
    let mut frontier = vec![head_commit_id.to_string()];
    let mut visited = BTreeSet::new();

    while !frontier.is_empty() {
        frontier.retain(|commit_id| visited.insert(commit_id.clone()));
        if frontier.is_empty() {
            break;
        }

        if let Some(checkpoint_commit_id) =
            select_best_checkpoint_commit_from_candidates_with_executor(executor, &frontier).await?
        {
            return Ok(Some(checkpoint_commit_id));
        }

        let mut next_frontier = BTreeSet::new();
        for commit_id in &frontier {
            let Some(lineage) = load_commit_lineage_entry_by_id(executor, commit_id).await? else {
                continue;
            };
            for parent_commit_id in lineage.parent_commit_ids {
                if !parent_commit_id.is_empty() && !visited.contains(&parent_commit_id) {
                    next_frontier.insert(parent_commit_id);
                }
            }
        }
        frontier = next_frontier.into_iter().collect();
    }

    Ok(None)
}

async fn select_best_checkpoint_commit_from_candidates_with_executor(
    executor: &mut dyn QueryExecutor,
    commit_ids: &[String],
) -> Result<Option<String>, LixError> {
    if commit_ids.is_empty() {
        return Ok(None);
    }

    let label_entity_ids = commit_ids
        .iter()
        .map(|commit_id| checkpoint_commit_label_entity_id(commit_id))
        .collect::<Vec<_>>();
    let label_in_list = label_entity_ids
        .iter()
        .map(|entity_id| format!("'{}'", escape_sql_string(entity_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let label_sql = format!(
        "SELECT entity_id \
         FROM lix_internal_change \
         WHERE entity_id IN ({label_in_list}) \
           AND schema_key = 'lix_entity_label' \
           AND file_id = 'lix' \
           AND plugin_key = 'lix'"
    );
    let label_result = match executor.execute(&label_sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    let labeled_entity_ids = label_result
        .rows
        .iter()
        .map(|row| text_value(row.first(), "lix_internal_change.entity_id"))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let labeled_commit_ids = commit_ids
        .iter()
        .filter(|commit_id| {
            labeled_entity_ids.contains(&checkpoint_commit_label_entity_id(commit_id))
        })
        .cloned()
        .collect::<Vec<_>>();
    if labeled_commit_ids.is_empty() {
        return Ok(None);
    }

    let commit_in_list = labeled_commit_ids
        .iter()
        .map(|commit_id| format!("'{}'", escape_sql_string(commit_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let order_sql = format!(
        "SELECT commit_id \
         FROM {COMMIT_GRAPH_NODE_TABLE} \
         WHERE commit_id IN ({commit_in_list}) \
         ORDER BY generation DESC, commit_id DESC \
         LIMIT 1"
    );
    let rows = executor.execute(&order_sql, &[]).await?;
    let Some(first) = rows.rows.first() else {
        return Ok(None);
    };
    Ok(Some(text_value(
        first.first(),
        "lix_internal_commit_graph_node.commit_id",
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID;
    use crate::{QueryResult, SqlDialect};
    use async_trait::async_trait;
    use std::collections::BTreeMap;

    #[derive(Default)]
    struct FakeQueryExecutor {
        parents_by_commit: BTreeMap<String, Vec<String>>,
        labeled_commits: BTreeSet<String>,
        generation_by_commit: BTreeMap<String, i64>,
        executed_sql: Vec<String>,
    }

    #[async_trait(?Send)]
    impl QueryExecutor for FakeQueryExecutor {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.executed_sql.push(sql.to_string());

            if sql.contains("schema_key = 'lix_commit'")
                && sql.contains("LEFT JOIN lix_internal_snapshot")
            {
                let Some(commit_id) = extract_single_quoted_value_after(sql, "AND c.entity_id = '")
                else {
                    return Err(LixError::unknown(format!(
                        "missing commit id in sql: {sql}"
                    )));
                };
                let Some(parent_commit_ids) = self.parents_by_commit.get(&commit_id) else {
                    return Ok(QueryResult {
                        rows: Vec::new(),
                        columns: vec!["snapshot_content".to_string()],
                    });
                };
                let snapshot = serde_json::json!({
                    "id": commit_id,
                    "change_set_id": "change-set",
                    "change_ids": [],
                    "parent_commit_ids": parent_commit_ids,
                })
                .to_string();
                return Ok(QueryResult {
                    rows: vec![vec![Value::Text(snapshot)]],
                    columns: vec!["snapshot_content".to_string()],
                });
            }

            if sql.contains("schema_key = 'lix_entity_label'") && sql.contains("entity_id IN (") {
                let rows = extract_single_quoted_values(sql)
                    .into_iter()
                    .filter_map(|entity_id| {
                        let commit_id = entity_id
                            .strip_suffix(&format!("~lix_commit~lix~{}", CHECKPOINT_LABEL_ID))?
                            .to_string();
                        self.labeled_commits
                            .contains(&commit_id)
                            .then_some(vec![Value::Text(entity_id)])
                    })
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["entity_id".to_string()],
                });
            }

            if sql.contains(&format!("FROM {COMMIT_GRAPH_NODE_TABLE}"))
                && sql.contains("commit_id IN (")
            {
                let best = extract_single_quoted_values(sql)
                    .into_iter()
                    .filter_map(|commit_id| {
                        self.generation_by_commit
                            .get(&commit_id)
                            .map(|generation| (commit_id, *generation))
                    })
                    .max_by(|(left_id, left_generation), (right_id, right_generation)| {
                        left_generation
                            .cmp(right_generation)
                            .then_with(|| left_id.cmp(right_id))
                    });
                let rows = best
                    .map(|(commit_id, _)| vec![vec![Value::Text(commit_id)]])
                    .unwrap_or_default();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["commit_id".to_string()],
                });
            }

            Err(LixError::unknown(format!("unexpected sql: {sql}")))
        }
    }

    #[tokio::test]
    async fn checkpoint_history_resolution_uses_graph_labels_not_replay_status() {
        let mut executor = FakeQueryExecutor {
            parents_by_commit: BTreeMap::from([
                ("head".to_string(), vec!["mid".to_string()]),
                ("mid".to_string(), vec!["root".to_string()]),
                ("root".to_string(), Vec::new()),
            ]),
            labeled_commits: BTreeSet::from(["mid".to_string()]),
            generation_by_commit: BTreeMap::from([
                ("head".to_string(), 3),
                ("mid".to_string(), 2),
                ("root".to_string(), 1),
            ]),
            executed_sql: Vec::new(),
        };

        let checkpoint_commit_id =
            resolve_last_checkpoint_commit_id_for_tip_with_executor(&mut executor, "head")
                .await
                .expect("resolution should succeed");

        assert_eq!(checkpoint_commit_id, Some("mid".to_string()));
        assert!(
            executor
                .executed_sql
                .iter()
                .all(|sql| !sql.contains("lix_internal_live_state_status")),
            "checkpoint history should not consult replay status",
        );
    }

    #[tokio::test]
    async fn checkpoint_history_tiebreak_uses_commit_graph_generation() {
        let mut executor = FakeQueryExecutor {
            parents_by_commit: BTreeMap::from([
                (
                    "head".to_string(),
                    vec!["branch-a".to_string(), "branch-b".to_string()],
                ),
                ("branch-a".to_string(), vec!["root".to_string()]),
                ("branch-b".to_string(), vec!["root".to_string()]),
                ("root".to_string(), Vec::new()),
            ]),
            labeled_commits: BTreeSet::from(["branch-a".to_string(), "branch-b".to_string()]),
            generation_by_commit: BTreeMap::from([
                ("head".to_string(), 4),
                ("branch-a".to_string(), 2),
                ("branch-b".to_string(), 3),
                ("root".to_string(), 1),
            ]),
            executed_sql: Vec::new(),
        };

        let checkpoint_commit_id =
            resolve_last_checkpoint_commit_id_for_tip_with_executor(&mut executor, "head")
                .await
                .expect("resolution should succeed");

        assert_eq!(checkpoint_commit_id, Some("branch-b".to_string()));
        assert!(
            executor
                .executed_sql
                .iter()
                .any(|sql| sql.contains(COMMIT_GRAPH_NODE_TABLE)),
            "checkpoint history should order candidates with the commit graph index",
        );
    }

    fn extract_single_quoted_value_after(sql: &str, marker: &str) -> Option<String> {
        let tail = sql.split_once(marker)?.1;
        let value = tail.split_once('\'')?.0;
        Some(value.to_string())
    }

    fn extract_single_quoted_values(sql: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut chars = sql.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch != '\'' {
                continue;
            }
            let mut value = String::new();
            while let Some(next) = chars.next() {
                if next == '\'' {
                    if chars.peek().is_some_and(|peek| *peek == '\'') {
                        value.push('\'');
                        let _ = chars.next();
                        continue;
                    }
                    break;
                }
                value.push(next);
            }
            values.push(value);
        }
        values
    }
}
