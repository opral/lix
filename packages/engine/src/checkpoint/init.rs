use std::collections::BTreeSet;

use crate::canonical::readers::load_commit_lineage_entry_by_id;
use crate::init::seed::text_value;
use crate::init::tables::execute_init_statements;
use crate::init::InitExecutor;
use crate::sql::common::text::escape_sql_string;
use crate::Value;
use crate::{LixBackend, LixError};

const CHECKPOINT_INIT_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS lix_internal_last_checkpoint (\
     version_id TEXT PRIMARY KEY,\
     checkpoint_commit_id TEXT NOT NULL\
     )",
    "CREATE INDEX IF NOT EXISTS idx_lix_internal_last_checkpoint_commit \
     ON lix_internal_last_checkpoint (checkpoint_commit_id)",
];

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    execute_init_statements(backend, "checkpoint", CHECKPOINT_INIT_STATEMENTS).await
}

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_default_checkpoint_label().await?;
    executor.rebuild_internal_last_checkpoint().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_default_checkpoint_label(&mut self) -> Result<(), LixError> {
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let existing = self
            .execute_internal(
                "SELECT snapshot_content \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_label' \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[Value::Text(super::CHECKPOINT_LABEL_ID.to_string())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "default checkpoint label query",
                1,
                existing.statements.len(),
            ));
        };
        if let Some(row) = statement.rows.first() {
            let Some(Value::Text(snapshot_content)) = row.first() else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label snapshot_content must be text",
                ));
            };
            let parsed: serde_json::Value = serde_json::from_str(snapshot_content.as_str())
                .map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("checkpoint label snapshot invalid JSON: {error}"),
                })?;
            let id = parsed.get("id").and_then(serde_json::Value::as_str);
            let name = parsed.get("name").and_then(serde_json::Value::as_str);
            if id != Some(super::CHECKPOINT_LABEL_ID) || name != Some(super::CHECKPOINT_LABEL_NAME)
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label canonical row is present but invalid",
                ));
            }
            self.ensure_checkpoint_label_on_bootstrap_commit(
                &bootstrap_commit_id,
                super::CHECKPOINT_LABEL_ID,
            )
            .await?;
            return Ok(());
        }

        let snapshot_content = serde_json::json!({
            "id": super::CHECKPOINT_LABEL_ID,
            "name": super::CHECKPOINT_LABEL_NAME,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            Some(&bootstrap_commit_id),
            super::CHECKPOINT_LABEL_ID,
            "lix_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        self.ensure_checkpoint_label_on_bootstrap_commit(
            &bootstrap_commit_id,
            super::CHECKPOINT_LABEL_ID,
        )
        .await?;
        Ok(())
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &mut self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id = super::checkpoint_commit_label_entity_id(bootstrap_commit_id);
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE entity_id = $1 \
                   AND schema_key = 'lix_entity_label' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(entity_label_id.clone())],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "checkpoint label bootstrap link existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        if label_id != super::CHECKPOINT_LABEL_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected checkpoint label id '{label_id}'"),
            ));
        }
        let snapshot_content = super::checkpoint_commit_label_snapshot(bootstrap_commit_id);
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_label_id,
            "lix_entity_label",
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(())
    }

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
            crate::canonical::version_state::load_all_version_descriptors_with_executor(
                &mut backend,
            )
            .await?
        };

        // `lix_internal_last_checkpoint` is derived convenience state. Rebuild it
        // from canonical version heads plus system-managed checkpoint labels.
        self.execute_backend("DELETE FROM lix_internal_last_checkpoint", &[])
            .await?;

        let global_commit_id = self.load_global_version_commit_id().await?;
        let global_checkpoint_commit_id = self
            .resolve_last_checkpoint_commit_id_for_tip(&global_commit_id)
            .await?
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
                crate::canonical::refs::load_committed_version_head_commit_id(
                    &mut backend,
                    &version_id,
                )
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("version '{version_id}' is missing a committed head"),
                    )
                })?
            };
            let checkpoint_commit_id = self
                .resolve_last_checkpoint_commit_id_for_tip(&commit_id)
                .await?
                .unwrap_or_else(|| commit_id.clone());
            self.insert_last_checkpoint_for_version(&version_id, &checkpoint_commit_id)
                .await?;
        }

        Ok(())
    }

    async fn resolve_last_checkpoint_commit_id_for_tip(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let mut frontier = vec![head_commit_id.to_string()];
        let mut visited = BTreeSet::new();

        while !frontier.is_empty() {
            frontier.retain(|commit_id| visited.insert(commit_id.clone()));
            if frontier.is_empty() {
                break;
            }

            if let Some(checkpoint_commit_id) = self
                .select_best_checkpoint_commit_from_candidates(&frontier)
                .await?
            {
                return Ok(Some(checkpoint_commit_id));
            }

            let mut next_frontier = BTreeSet::new();
            for commit_id in &frontier {
                let lineage = {
                    let mut backend = self.backend_adapter();
                    load_commit_lineage_entry_by_id(&mut backend, commit_id).await?
                };
                let Some(lineage) = lineage else {
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

    async fn select_best_checkpoint_commit_from_candidates(
        &mut self,
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
        let label_rows = self
            .execute_internal(
                &format!(
                    "SELECT entity_id \
                     FROM lix_internal_change \
                     WHERE entity_id IN ({label_in_list}) \
                       AND schema_key = 'lix_entity_label' \
                       AND file_id = 'lix' \
                       AND plugin_key = 'lix'"
                ),
                &[],
            )
            .await?;
        let [label_statement] = label_rows.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "checkpoint label candidate query",
                1,
                label_rows.statements.len(),
            ));
        };
        let labeled_entity_ids = label_statement
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
        let rows = self
            .execute_internal(
                &format!(
                    "SELECT entity_id AS id \
                     FROM lix_internal_change \
                     WHERE schema_key = 'lix_commit' \
                       AND file_id = 'lix' \
                       AND plugin_key = 'lix' \
                       AND entity_id IN ({commit_in_list}) \
                     GROUP BY entity_id \
                     ORDER BY MAX(created_at) DESC, entity_id DESC \
                     LIMIT 1"
                ),
                &[],
            )
            .await?;
        let [statement] = rows.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "checkpoint candidate ordering query",
                1,
                rows.statements.len(),
            ));
        };
        let Some(first) = statement.rows.first() else {
            return Ok(None);
        };
        Ok(Some(text_value(first.get(0), "lix_commit.id")?))
    }
}
