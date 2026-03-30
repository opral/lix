use crate::init::InitExecutor;
use crate::Value;
use crate::{LixBackend, LixError};

pub(crate) async fn init(_backend: &dyn LixBackend) -> Result<(), LixError> {
    Ok(())
}

pub(crate) async fn seed_bootstrap(executor: &mut InitExecutor<'_, '_>) -> Result<(), LixError> {
    executor.seed_boot_account().await
}

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    pub(crate) async fn seed_boot_account(&mut self) -> Result<(), LixError> {
        let Some(account) = self.boot_active_account().cloned() else {
            return Ok(());
        };
        let bootstrap_commit_id = self.load_global_version_commit_id().await?;
        let exists = self
            .execute_internal(
                "SELECT 1 \
                 FROM lix_state_by_version \
                 WHERE schema_key = $1 \
                   AND entity_id = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[
                    Value::Text(super::account_schema_key().to_string()),
                    Value::Text(account.id.clone()),
                    Value::Text(super::account_file_id().to_string()),
                    Value::Text(super::account_storage_version_id().to_string()),
                ],
            )
            .await?;
        let [statement] = exists.statements.as_slice() else {
            return Err(crate::errors::unexpected_statement_count_error(
                "boot account existence query",
                1,
                exists.statements.len(),
            ));
        };
        if statement.rows.is_empty() {
            self.insert_bootstrap_tracked_row(
                Some(&bootstrap_commit_id),
                &account.id,
                super::account_schema_key(),
                super::account_schema_version(),
                super::account_file_id(),
                super::account_storage_version_id(),
                super::account_plugin_key(),
                &super::account_snapshot_content(&account.id, &account.name),
            )
            .await?;
        }

        Ok(())
    }
}
