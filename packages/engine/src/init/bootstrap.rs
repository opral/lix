use super::*;
use crate::SqlDialect;

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

        let init_result = async {
            init_backend(self.backend.as_ref()).await?;
            if self.backend_has_been_initialized().await? {
                return Err(crate::errors::already_initialized_error());
            }

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

        let result = match init_result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.normalize_init_error(error).await),
        };

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

    async fn backend_has_been_initialized(&self) -> Result<bool, LixError> {
        let table_exists = match self.backend.dialect() {
            SqlDialect::Sqlite => {
                let exists = self
                    .backend
                    .execute(
                        "SELECT 1 \
                         FROM sqlite_master \
                         WHERE type = 'table' \
                           AND name = 'lix_internal_state_materialized_v1_lix_version_pointer' \
                         LIMIT 1",
                        &[],
                    )
                    .await?;
                !exists.rows.is_empty()
            }
            SqlDialect::Postgres => {
                let exists = self
                    .backend
                    .execute(
                        "SELECT 1 \
                         FROM information_schema.tables \
                         WHERE table_schema = current_schema() \
                           AND table_name = 'lix_internal_state_materialized_v1_lix_version_pointer' \
                         LIMIT 1",
                        &[],
                    )
                    .await?;
                !exists.rows.is_empty()
            }
        };
        if !table_exists {
            return Ok(false);
        }

        let result = self
            .backend
            .execute(
                "SELECT 1 \
                 FROM lix_internal_state_materialized_v1_lix_version_pointer \
                 WHERE schema_key = 'lix_version_pointer' \
                   AND entity_id = 'global' \
                   AND file_id = 'lix' \
                   AND version_id = 'global' \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[],
            )
            .await?;
        Ok(!result.rows.is_empty())
    }

    async fn normalize_init_error(&self, error: LixError) -> LixError {
        if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() {
            return error;
        }
        if is_init_conflict_error(&error.description) {
            return crate::errors::already_initialized_error();
        }

        match self.backend_has_been_initialized().await {
            Ok(true) => crate::errors::already_initialized_error(),
            _ => error,
        }
    }
}

fn is_init_conflict_error(description: &str) -> bool {
    let normalized = description.to_ascii_lowercase();
    normalized.contains("unique constraint failed")
        || normalized.contains("unique constraint violation")
        || normalized.contains("already exists in version 'global'")
}
