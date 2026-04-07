use crate::init::InitExecutor;
use crate::Value;
use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    crate::checkpoint_cache::init(backend).await
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
                 WHERE schema_key = $2 \
                   AND entity_id = $1 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 ORDER BY updated_at DESC, created_at DESC, change_id DESC \
                 LIMIT 1",
                &[
                    Value::Text(crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID.to_string()),
                    Value::Text(
                        crate::checkpoint_artifacts::CHECKPOINT_LABEL_SCHEMA_KEY.to_string(),
                    ),
                ],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::common::errors::unexpected_statement_count_error(
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
            if id != Some(crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID)
                || name != Some(crate::checkpoint_artifacts::CHECKPOINT_LABEL_NAME)
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "checkpoint label canonical row is present but invalid",
                ));
            }
            self.ensure_checkpoint_label_on_bootstrap_commit(
                &bootstrap_commit_id,
                crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID,
            )
            .await?;
            return Ok(());
        }

        let snapshot_content = crate::checkpoint_artifacts::checkpoint_label_snapshot();
        self.insert_bootstrap_tracked_row(
            Some(&bootstrap_commit_id),
            crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID,
            crate::checkpoint_artifacts::CHECKPOINT_LABEL_SCHEMA_KEY,
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        self.ensure_checkpoint_label_on_bootstrap_commit(
            &bootstrap_commit_id,
            crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID,
        )
        .await?;
        Ok(())
    }

    async fn ensure_checkpoint_label_on_bootstrap_commit(
        &mut self,
        bootstrap_commit_id: &str,
        label_id: &str,
    ) -> Result<(), LixError> {
        let entity_label_id =
            crate::checkpoint_artifacts::checkpoint_commit_label_entity_id(bootstrap_commit_id);
        let existing = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE entity_id = $1 \
                   AND schema_key = $2 \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[
                    Value::Text(entity_label_id.clone()),
                    Value::Text(
                        crate::checkpoint_artifacts::CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY.to_string(),
                    ),
                ],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::common::errors::unexpected_statement_count_error(
                "checkpoint label bootstrap link existence query",
                1,
                existing.statements.len(),
            ));
        };
        if !statement.rows.is_empty() {
            return Ok(());
        }

        if label_id != crate::checkpoint_artifacts::CHECKPOINT_LABEL_ID {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("unexpected checkpoint label id '{label_id}'"),
            ));
        }
        let snapshot_content =
            crate::checkpoint_artifacts::checkpoint_commit_label_snapshot(bootstrap_commit_id);
        self.insert_bootstrap_tracked_row(
            Some(bootstrap_commit_id),
            &entity_label_id,
            crate::checkpoint_artifacts::CHECKPOINT_COMMIT_LABEL_SCHEMA_KEY,
            "1",
            "lix",
            "global",
            "lix",
            &snapshot_content,
        )
        .await?;

        Ok(())
    }
}
