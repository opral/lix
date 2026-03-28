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
        let account_table =
            crate::live_state::schema_access::tracked_relation_name(super::account_schema_key());

        let exists = self
            .execute_backend(
                &format!(
                    "SELECT 1 \
                     FROM {account_table} \
                     WHERE schema_key = $1 \
                       AND entity_id = $2 \
                       AND file_id = $3 \
                       AND version_id = $4 \
                       AND is_tombstone = 0 \
                     LIMIT 1",
                    account_table = crate::init::seed::quote_ident(&account_table),
                ),
                &[
                    Value::Text(super::account_schema_key().to_string()),
                    Value::Text(account.id.clone()),
                    Value::Text(super::account_file_id().to_string()),
                    Value::Text(super::account_storage_version_id().to_string()),
                ],
            )
            .await?;
        if exists.rows.is_empty() {
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
