use super::*;

impl Engine {
    pub async fn init(&self) -> Result<(), LixError> {
        if self
            .init_state
            .compare_exchange(
                INIT_STATE_NOT_STARTED,
                INIT_STATE_IN_PROGRESS,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(crate::errors::already_initialized_error());
        }

        let result = async {
            init_backend(self.backend.as_ref()).await?;
            self.ensure_builtin_schemas_installed().await?;
            let default_active_version_id = self.seed_default_versions().await?;
            self.seed_global_system_directories().await?;
            self.seed_commit_ancestry().await?;
            self.seed_default_active_version(&default_active_version_id)
                .await?;
            self.seed_default_checkpoint_label().await?;
            self.rebuild_internal_last_checkpoint().await?;
            self.seed_boot_key_values().await?;
            self.seed_boot_account().await?;
            self.load_and_cache_active_version().await
        }
        .await;

        if result.is_ok() {
            if self.deterministic_boot_pending.load(Ordering::SeqCst) {
                self.deterministic_boot_pending
                    .store(false, Ordering::SeqCst);
            }
            self.init_state
                .store(INIT_STATE_COMPLETED, Ordering::SeqCst);
        } else {
            self.init_state
                .store(INIT_STATE_NOT_STARTED, Ordering::SeqCst);
        }

        result
    }
}
