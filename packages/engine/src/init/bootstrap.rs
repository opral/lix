use crate::engine::Engine;
use crate::init::init_backend;
use crate::version::{
    version_ref_file_id, version_ref_schema_key, version_ref_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{LixError, SqlDialect, Value};
use std::time::Duration;

impl Engine {
    #[doc(hidden)]
    pub async fn initialize(&self) -> Result<(), LixError> {
        self.try_mark_init_in_progress()?;

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
            self.load_and_cache_active_version().await?;
            self.refresh_public_surface_registry().await
        }
        .await;

        let result = match init_result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.normalize_init_error(error).await),
        };

        if result.is_ok() {
            if self.deterministic_boot_pending() {
                self.clear_deterministic_boot_pending();
            }
            self.mark_init_completed();
        } else {
            self.reset_init_state();
        }

        result
    }

    #[doc(hidden)]
    pub async fn initialize_if_needed(&self) -> Result<bool, LixError> {
        match self.initialize().await {
            Ok(()) => Ok(true),
            Err(error) if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() => {
                self.load_and_cache_active_version().await?;
                self.refresh_public_surface_registry().await?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn is_initialized(&self) -> Result<bool, LixError> {
        self.backend_has_been_initialized().await
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
                           AND name = 'lix_internal_live_v1_lix_version_ref' \
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
                           AND table_name = 'lix_internal_live_v1_lix_version_ref' \
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
                 FROM lix_internal_live_v1_lix_version_ref \
                 WHERE schema_key = $1 \
                   AND entity_id = $2 \
                   AND file_id = $3 \
                   AND version_id = $4 \
                   AND snapshot_content IS NOT NULL \
                 LIMIT 1",
                &[
                    Value::Text(version_ref_schema_key().to_string()),
                    Value::Text(GLOBAL_VERSION_ID.to_string()),
                    Value::Text(version_ref_file_id().to_string()),
                    Value::Text(version_ref_storage_version_id().to_string()),
                ],
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
        if is_init_locked_error(&error.description)
            && self
                .wait_for_concurrent_init_resolution()
                .await
                .unwrap_or(false)
        {
            return crate::errors::already_initialized_error();
        }

        match self.backend_has_been_initialized().await {
            Ok(true) => crate::errors::already_initialized_error(),
            _ => error,
        }
    }

    async fn wait_for_concurrent_init_resolution(&self) -> Result<bool, LixError> {
        const ATTEMPTS: usize = 60;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match self.backend_has_been_initialized().await {
                Ok(true) => return Ok(true),
                Ok(false) => {
                    if attempt + 1 == ATTEMPTS {
                        return Ok(false);
                    }
                }
                Err(error) if is_init_locked_error(&error.description) => {
                    if attempt + 1 == ATTEMPTS {
                        return Ok(false);
                    }
                }
                Err(error) => return Err(error),
            }
            std::thread::sleep(Duration::from_millis(DELAY_MS));
        }
        Ok(false)
    }
}

fn is_init_conflict_error(description: &str) -> bool {
    let normalized = description.to_ascii_lowercase();
    normalized.contains("unique constraint failed")
        || normalized.contains("unique constraint violation")
        || normalized.contains("already exists in version 'global'")
}

fn is_init_locked_error(description: &str) -> bool {
    let normalized = description.to_ascii_lowercase();
    normalized.contains("database is locked")
        || normalized.contains("database schema is locked")
        || normalized.contains("database table is locked")
}
