use crate::plugin::InstalledPlugin;
use crate::storage::{StorageReadScope, StorageReadTransaction};
use crate::LixError;

use super::SessionContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterPluginOptions {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterPluginReceipt {
    pub plugin_key: String,
}

impl SessionContext {
    pub async fn register_plugin(
        &self,
        options: RegisterPluginOptions,
    ) -> Result<RegisterPluginReceipt, LixError> {
        self.ensure_open()?;
        let plugin_context = self.plugin_context.clone();
        self.with_write_transaction(move |transaction| {
            Box::pin(async move { plugin_context.register_plugin(transaction, options).await })
        })
        .await
    }

    pub async fn list_plugins(&self) -> Result<Vec<InstalledPlugin>, LixError> {
        self.ensure_open()?;
        let active_version_id = self.active_version_id().await?;
        let transaction = self.storage.begin_read_transaction().await?;
        let read_scope =
            StorageReadScope::<Box<dyn StorageReadTransaction + Send + Sync>>::new(transaction);
        let live_state = std::sync::Arc::new(self.live_state.reader(read_scope.store()));
        let blob_reader = std::sync::Arc::new(self.binary_cas.reader(read_scope.store()));
        let result = self
            .plugin_context
            .load_installed_plugins_for_version(live_state, blob_reader, &active_version_id)
            .await;
        match result {
            Ok(plugins) => {
                read_scope.rollback().await?;
                Ok(plugins)
            }
            Err(error) => {
                let _ = read_scope.rollback().await;
                Err(error)
            }
        }
    }
}
