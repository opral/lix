use crate::engine::Engine;
use crate::init::init_backend;
use crate::state::live_state::{
    canonical_state_exists, ensure_live_state_ready, load_latest_canonical_watermark,
    mark_live_state_mode_with_backend, mark_live_state_ready_with_backend, LiveStateMode,
};
use crate::LixError;
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
            self.seed_commit_graph_nodes().await?;
            self.seed_default_active_version(&default_active_version_id)
                .await?;
            self.seed_default_checkpoint_label().await?;
            self.rebuild_internal_last_checkpoint().await?;
            self.seed_boot_key_values().await?;
            self.seed_boot_account().await?;
            mark_live_state_mode_with_backend(self.backend.as_ref(), LiveStateMode::Rebuilding)
                .await?;
            self.load_and_cache_active_version().await?;
            self.refresh_public_surface_registry().await?;
            let watermark = load_latest_canonical_watermark(self.backend.as_ref())
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "initialize expected canonical watermark after bootstrap seeding",
                    )
                })?;
            mark_live_state_ready_with_backend(self.backend.as_ref(), &watermark).await
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
                if !self.wait_for_concurrent_init_resolution().await? {
                    ensure_live_state_ready(self.backend.as_ref()).await?;
                }
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
        canonical_state_exists(self.backend.as_ref()).await
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
            return match ensure_live_state_ready(self.backend.as_ref()).await {
                Ok(()) => crate::errors::already_initialized_error(),
                Err(error) => error,
            };
        }

        match self.backend_has_been_initialized().await {
            Ok(true) => match ensure_live_state_ready(self.backend.as_ref()).await {
                Ok(()) => crate::errors::already_initialized_error(),
                Err(error) => error,
            },
            _ => error,
        }
    }

    async fn wait_for_concurrent_init_resolution(&self) -> Result<bool, LixError> {
        const ATTEMPTS: usize = 60;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match ensure_live_state_ready(self.backend.as_ref()).await {
                Ok(()) => return Ok(true),
                Err(error)
                    if error.code == crate::errors::ErrorCode::NotInitialized.as_str()
                        || error.code == crate::errors::ErrorCode::LiveStateNotReady.as_str()
                        || is_init_locked_error(&error.description) =>
                {
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
