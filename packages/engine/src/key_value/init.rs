use crate::init::seed::read_scalar_count;
use crate::init::InitExecutor;
use crate::{LixBackend, LixError};

pub(crate) async fn init(_backend: &dyn LixBackend) -> Result<(), LixError> {
    Ok(())
}

pub(crate) async fn seed_bootstrap(
    executor: &mut InitExecutor<'_, '_>,
    default_active_version_id: &str,
) -> Result<(), LixError> {
    executor
        .seed_boot_key_values(default_active_version_id)
        .await?;
    executor.seed_lix_id().await
}

const LIX_ID_KEY: &str = "lix_id";

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_boot_key_values(
        &mut self,
        default_active_version_id: &str,
    ) -> Result<(), LixError> {
        let mut bootstrap_commit_id: Option<String> = None;
        for key_value in self.boot_key_values().to_vec() {
            let version_id = if key_value.lixcol_global.unwrap_or(false) {
                super::KEY_VALUE_GLOBAL_VERSION.to_string()
            } else {
                default_active_version_id.to_string()
            };
            let untracked = key_value.lixcol_untracked.unwrap_or(true);
            let snapshot_content = serde_json::json!({
                "key": key_value.key,
                "value": key_value.value,
            })
            .to_string();

            if untracked {
                self.insert_bootstrap_untracked_row(
                    &key_value.key,
                    super::key_value_schema_key(),
                    super::key_value_schema_version(),
                    super::key_value_file_id(),
                    &version_id,
                    super::key_value_plugin_key(),
                    &snapshot_content,
                )
                .await?;
            } else {
                let commit_id = match &bootstrap_commit_id {
                    Some(commit_id) => commit_id.clone(),
                    None => {
                        let commit_id = self.load_global_version_commit_id().await?;
                        bootstrap_commit_id = Some(commit_id.clone());
                        commit_id
                    }
                };
                self.insert_bootstrap_tracked_row(
                    Some(&commit_id),
                    &key_value.key,
                    super::key_value_schema_key(),
                    super::key_value_schema_version(),
                    super::key_value_file_id(),
                    &version_id,
                    super::key_value_plugin_key(),
                    &snapshot_content,
                )
                .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn seed_lix_id(&mut self) -> Result<(), LixError> {
        let existing = self
            .execute_internal(
                "SELECT COUNT(*) AS c \
                 FROM lix_state_by_version \
                 WHERE schema_key = 'lix_key_value' \
                   AND entity_id = 'lix_id' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL",
                &[],
            )
            .await?;
        let [statement] = existing.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "seed_lix_id existence query",
                1,
                existing.statements.len(),
            ));
        };
        if read_scalar_count(statement, "seed_lix_id existence query")? > 0 {
            return Ok(());
        }

        let lix_id_value = self.generate_runtime_uuid().await?;
        let snapshot_content = serde_json::json!({
            "key": LIX_ID_KEY,
            "value": lix_id_value,
        })
        .to_string();
        self.insert_bootstrap_tracked_row(
            None,
            LIX_ID_KEY,
            super::key_value_schema_key(),
            super::key_value_schema_version(),
            super::key_value_file_id(),
            super::KEY_VALUE_GLOBAL_VERSION,
            super::key_value_plugin_key(),
            &snapshot_content,
        )
        .await?;
        Ok(())
    }
}
