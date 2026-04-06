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
use crate::contracts::artifacts::DomainChangeBatch;
use crate::errors::classification::is_missing_relation_error;
use crate::init::seed::text_value;
use crate::init::tables::execute_init_statements;
use crate::init::InitExecutor;
use crate::sql::common::text::escape_sql_string;
use crate::sql::prepare::PreparedPublicWrite;
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

const HISTORY_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_last_checkpoint (\
     version_id TEXT PRIMARY KEY,\
     checkpoint_commit_id TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_last_checkpoint_commit \
     ON lix_internal_last_checkpoint (checkpoint_commit_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "checkpoint", HISTORY_INIT_STATEMENTS).await
}

pub(crate) async fn apply_public_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixBackendTransaction,
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Result<(), LixError> {
    // Public writes to `lix_version` keep the derived checkpoint history cache
    // in sync. The cache stays rebuildable from canonical version heads plus
    // canonical checkpoint labels.
    if public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    match public_write.planned_write.command.operation_kind {
        crate::sql::logical_plan::public_ir::WriteOperationKind::Insert => {
            upsert_last_checkpoint_rows_in_transaction(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                true,
            )
            .await
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows_in_transaction(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                false,
            )
            .await
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Delete => {
            let version_ids = version_ids_from_resolved_write(public_write, batch);
            delete_last_checkpoint_rows_in_transaction(transaction, &version_ids).await
        }
    }
}

pub(crate) async fn upsert_last_checkpoint_for_version_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    checkpoint_commit_id: &str,
) -> Result<(), LixError> {
    upsert_last_checkpoint_rows_in_transaction(
        transaction,
        &[(version_id.to_string(), checkpoint_commit_id.to_string())],
        true,
    )
    .await
}

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

fn version_checkpoint_rows_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Vec<(String, String)> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let rows = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                row.schema_key == crate::version::version_ref_schema_key() && !row.tombstone
            })
            .filter_map(|row| {
                row.values
                    .get("snapshot_content")
                    .and_then(|value| match value {
                        Value::Text(snapshot) => {
                            serde_json::from_str::<serde_json::Value>(snapshot)
                                .ok()
                                .and_then(|snapshot| {
                                    snapshot
                                        .get("commit_id")
                                        .and_then(serde_json::Value::as_str)
                                        .map(|commit_id| {
                                            (row.entity_id.to_string(), commit_id.to_string())
                                        })
                                })
                        }
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return rows;
        }
    }

    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version::version_ref_schema_key())
        .filter_map(|change| {
            change.snapshot_content.as_deref().and_then(|snapshot| {
                serde_json::from_str::<serde_json::Value>(snapshot)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("commit_id")
                            .and_then(serde_json::Value::as_str)
                            .map(|commit_id| (change.entity_id.to_string(), commit_id.to_string()))
                    })
            })
        })
        .collect()
}

fn version_ids_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &DomainChangeBatch,
) -> Vec<String> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let version_ids = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                matches!(
                    row.schema_key.as_str(),
                    "lix_version_ref" | "lix_version_descriptor"
                )
            })
            .map(|row| row.entity_id.to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !version_ids.is_empty() {
            return version_ids;
        }
    }

    batch
        .changes
        .iter()
        .map(|change| change.entity_id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
}

async fn upsert_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[(String, String)],
    update_existing: bool,
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let values_sql = rows
        .iter()
        .map(|(version_id, checkpoint_commit_id)| {
            format!(
                "('{}', '{}')",
                escape_sql_string(version_id),
                escape_sql_string(checkpoint_commit_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_existing {
        "DO UPDATE SET checkpoint_commit_id = excluded.checkpoint_commit_id"
    } else {
        "DO NOTHING"
    };
    let sql = format!(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES {values_sql} \
         ON CONFLICT (version_id) {on_conflict}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn delete_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ids: &[String],
) -> Result<(), LixError> {
    if version_ids.is_empty() {
        return Ok(());
    }

    let in_list = version_ids
        .iter()
        .map(|id| format!("'{}'", escape_sql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM lix_internal_last_checkpoint WHERE version_id IN ({in_list})");
    transaction.execute(&sql, &[]).await?;
    Ok(())
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
        .map(|commit_id| super::checkpoint_commit_label_entity_id(commit_id))
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
            labeled_entity_ids.contains(&super::checkpoint_commit_label_entity_id(commit_id))
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
                            .strip_suffix(&format!(
                                "~lix_commit~lix~{}",
                                crate::checkpoint::CHECKPOINT_LABEL_ID
                            ))?
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
