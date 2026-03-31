use crate::init::InitExecutor;
use crate::live_state::{
    apply_untracked_write_batch_in_transaction, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::schema::builtin::types::LixCommit;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn init(_backend: &dyn LixBackend) -> Result<(), LixError> {
    Ok(())
}

pub(crate) async fn seed_bootstrap(
    executor: &mut InitExecutor<'_, '_>,
) -> Result<String, LixError> {
    executor.seed_default_versions().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_default_versions(&mut self) -> Result<String, LixError> {
        let bootstrap_commit_id = match self.load_latest_commit_id().await? {
            Some(commit_id) => commit_id,
            None => {
                let bootstrap_change_set_id = self.generate_runtime_uuid().await?;
                let bootstrap_commit_id = self.generate_runtime_uuid().await?;
                self.seed_bootstrap_change_set(&bootstrap_change_set_id)
                    .await?;
                self.seed_bootstrap_commit(&bootstrap_commit_id, &bootstrap_change_set_id)
                    .await?;
                bootstrap_commit_id
            }
        };
        self.assert_commit_change_set_integrity(&bootstrap_commit_id)
            .await?;

        let main_version_id = match self
            .find_version_id_by_name(super::DEFAULT_ACTIVE_VERSION_NAME)
            .await?
        {
            Some(version_id) => version_id,
            None => {
                let generated_main_id = self.generate_runtime_uuid().await?;
                self.seed_canonical_version_descriptor(
                    &bootstrap_commit_id,
                    &generated_main_id,
                    super::DEFAULT_ACTIVE_VERSION_NAME,
                )
                .await?;
                generated_main_id
            }
        };

        self.seed_canonical_version_descriptor(
            &bootstrap_commit_id,
            super::GLOBAL_VERSION_ID,
            super::GLOBAL_VERSION_ID,
        )
        .await?;
        self.seed_local_version_head(super::GLOBAL_VERSION_ID, &bootstrap_commit_id)
            .await?;
        self.seed_local_version_head(&main_version_id, &bootstrap_commit_id)
            .await?;

        Ok(main_version_id)
    }

    pub(crate) async fn find_version_id_by_name(
        &mut self,
        name: &str,
    ) -> Result<Option<String>, LixError> {
        let mut executor = self.backend_adapter();
        crate::canonical::read::find_version_id_by_name_with_executor(&mut executor, name).await
    }

    pub(crate) async fn assert_commit_change_set_integrity(
        &mut self,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let commit_row = self
            .execute_backend(
                "SELECT s.content \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_commit' \
                   AND c.entity_id = $1 \
                   AND c.file_id = 'lix' \
                   AND s.content IS NOT NULL",
                &[Value::Text(commit_id.to_string())],
            )
            .await?;
        let [row] = commit_row.rows.as_slice() else {
            return Err(if commit_row.rows.is_empty() {
                LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "init invariant violation: commit '{commit_id}' is missing from canonical lix_commit facts"
                    ),
                }
            } else {
                LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "init invariant violation: expected exactly one canonical lix_commit fact for '{commit_id}', got {}",
                        commit_row.rows.len()
                    ),
                }
            });
        };
        let Some(Value::Text(raw_snapshot)) = row.first() else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' canonical snapshot must be text"
                ),
            });
        };
        let commit_snapshot: LixCommit =
            serde_json::from_str(raw_snapshot).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' canonical snapshot is invalid JSON: {error}"
                ),
            })?;
        let Some(change_set_id) = commit_snapshot
            .change_set_id
            .filter(|change_set_id| !change_set_id.is_empty())
        else {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' has empty change_set_id"
                ),
            });
        };

        let existing = self
            .execute_backend(
                "SELECT 1 \
                 FROM lix_internal_change c \
                 JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
                 WHERE c.schema_key = 'lix_change_set' \
                   AND c.entity_id = $1 \
                   AND c.file_id = 'lix' \
                   AND s.content IS NOT NULL \
                 LIMIT 1",
                &[Value::Text(change_set_id.clone())],
            )
            .await?;
        if existing.rows.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "init invariant violation: commit '{commit_id}' references missing change_set '{change_set_id}'"
                ),
            });
        }

        Ok(())
    }

    async fn seed_local_version_head(
        &mut self,
        version_id: &str,
        commit_id: &str,
    ) -> Result<(), LixError> {
        let timestamp = self.generate_runtime_timestamp().await?;
        let row = UntrackedWriteRow {
            entity_id: version_id.to_string(),
            schema_key: super::version_ref_schema_key().to_string(),
            schema_version: super::version_ref_schema_version().to_string(),
            file_id: super::version_ref_file_id().to_string(),
            version_id: super::version_ref_storage_version_id().to_string(),
            global: true,
            plugin_key: super::version_ref_plugin_key().to_string(),
            metadata: None,
            writer_key: None,
            snapshot_content: Some(super::version_ref_snapshot_content(version_id, commit_id)),
            created_at: Some(timestamp.clone()),
            updated_at: timestamp,
            operation: UntrackedWriteOperation::Upsert,
        };
        apply_untracked_write_batch_in_transaction(self.backend_transaction_mut()?, &[row]).await
    }
}
