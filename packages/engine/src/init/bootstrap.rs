use super::*;

impl Engine {
    pub async fn init(&self) -> Result<(), LixError> {
        let clear_boot_pending = self.deterministic_boot_pending.load(Ordering::SeqCst);
        let result = async {
            init_backend(self.backend.as_ref()).await?;
            self.ensure_builtin_schemas_installed().await?;
            let default_active_version_id = self.seed_default_versions().await?;
            self.seed_default_active_version(&default_active_version_id)
                .await?;
            self.seed_default_checkpoint_label().await?;
            self.seed_boot_key_values().await?;
            self.seed_boot_account().await?;
            self.load_and_cache_active_version().await
        }
        .await;

        if clear_boot_pending && result.is_ok() {
            self.deterministic_boot_pending
                .store(false, Ordering::SeqCst);
        }

        result
    }
}
