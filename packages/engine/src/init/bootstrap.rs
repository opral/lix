use crate::engine::Engine;
use crate::init::init_backend;
use crate::state::live_state::{
    load_latest_canonical_watermark, load_live_state_mode_with_backend,
    mark_live_state_mode_with_backend, mark_live_state_ready_with_backend,
    try_claim_live_state_bootstrap_with_backend, LiveStateMode, LIVE_STATE_STATUS_SEED_ROW_SQL,
};
use crate::LixError;
use std::time::Duration;

impl Engine {
    #[doc(hidden)]
    pub async fn initialize(&self) -> Result<(), LixError> {
        self.try_mark_init_in_progress()?;

        // Wrap the entire initialization in a single transaction via plain SQL.
        // For SQLite, BEGIN IMMEDIATE acquires the write lock immediately:
        // - Other processes block until COMMIT (serializes concurrent inits)
        // - On failure, ROLLBACK undoes everything (no partial state)
        //
        // The in_init_transaction flag tells the engine to use savepoints
        // (via begin_write_unit) for any nested transactions that the init
        // steps may open internally.
        let has_transaction = self.backend.execute("BEGIN IMMEDIATE", &[]).await.is_ok();
        if has_transaction {
            self.set_in_init_transaction(true);
        }

        let mut claimed_bootstrap = false;
        let init_result = async {
            init_backend(self.backend.as_ref())
                .await
                .map_err(|error| init_step_error("init_backend", error))?;
            if !self.claim_live_state_bootstrap_with_repair().await? {
                return Err(crate::errors::already_initialized_error());
            }
            claimed_bootstrap = true;

            self.ensure_builtin_schemas_installed()
                .await
                .map_err(|error| init_step_error("ensure_builtin_schemas_installed", error))?;
            let default_active_version_id = self
                .seed_default_versions()
                .await
                .map_err(|error| init_step_error("seed_default_versions", error))?;
            self.seed_global_system_directories()
                .await
                .map_err(|error| init_step_error("seed_global_system_directories", error))?;
            self.seed_commit_graph_nodes()
                .await
                .map_err(|error| init_step_error("seed_commit_graph_nodes", error))?;
            self.seed_default_active_version(&default_active_version_id)
                .await
                .map_err(|error| init_step_error("seed_default_active_version", error))?;
            self.seed_lix_id()
                .await
                .map_err(|error| init_step_error("seed_lix_id", error))?;
            self.seed_default_checkpoint_label()
                .await
                .map_err(|error| init_step_error("seed_default_checkpoint_label", error))?;
            self.rebuild_internal_last_checkpoint()
                .await
                .map_err(|error| init_step_error("rebuild_internal_last_checkpoint", error))?;
            self.seed_boot_key_values()
                .await
                .map_err(|error| init_step_error("seed_boot_key_values", error))?;
            self.seed_boot_account()
                .await
                .map_err(|error| init_step_error("seed_boot_account", error))?;
            mark_live_state_mode_with_backend(self.backend.as_ref(), LiveStateMode::Rebuilding)
                .await
                .map_err(|error| init_step_error("mark_live_state_rebuilding", error))?;
            self.load_and_cache_active_version()
                .await
                .map_err(|error| init_step_error("load_and_cache_active_version", error))?;
            self.refresh_public_surface_registry()
                .await
                .map_err(|error| init_step_error("refresh_public_surface_registry", error))?;
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

        if has_transaction {
            self.set_in_init_transaction(false);
        }

        if result.is_ok() {
            if has_transaction {
                self.backend.execute("COMMIT", &[]).await?;
            }
            if self.deterministic_boot_pending() {
                self.clear_deterministic_boot_pending();
            }
            self.mark_init_completed();
        } else {
            if has_transaction {
                let _ = self.backend.execute("ROLLBACK", &[]).await;
            }
            if claimed_bootstrap {
                let _ = self.reset_failed_live_state_bootstrap().await;
            }
            self.reset_init_state();
        }

        result
    }

    #[doc(hidden)]
    pub async fn initialize_if_needed(&self) -> Result<bool, LixError> {
        match self.initialize().await {
            Ok(()) => Ok(true),
            Err(error) if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() => {
                self.wait_for_concurrent_init_ready().await?;
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
        Ok(
            load_live_state_mode_with_backend(self.backend.as_ref()).await?
                != LiveStateMode::Uninitialized,
        )
    }

    async fn normalize_init_error(&self, error: LixError) -> LixError {
        if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() {
            return error;
        }
        if is_init_conflict_error(&error.description) {
            return crate::errors::already_initialized_error();
        }
        if is_init_locked_error(&error.description) {
            return match load_live_state_mode_with_backend(self.backend.as_ref()).await {
                Ok(LiveStateMode::Bootstrapping)
                | Ok(
                    LiveStateMode::Ready | LiveStateMode::NeedsRebuild | LiveStateMode::Rebuilding,
                ) => crate::errors::already_initialized_error(),
                _ => error,
            };
        }
        error
    }

    async fn wait_for_concurrent_init_ready(&self) -> Result<(), LixError> {
        const ATTEMPTS: usize = 2400;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match load_live_state_mode_with_backend(self.backend.as_ref()).await? {
                LiveStateMode::Ready => return Ok(()),
                LiveStateMode::Bootstrapping => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::errors::live_state_not_ready_error());
                    }
                }
                LiveStateMode::Uninitialized => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::errors::not_initialized_error());
                    }
                }
                LiveStateMode::NeedsRebuild | LiveStateMode::Rebuilding => {
                    return Err(crate::errors::live_state_not_ready_error())
                }
            }
            std::thread::sleep(Duration::from_millis(DELAY_MS));
        }
        Err(crate::errors::live_state_not_ready_error())
    }

    async fn claim_live_state_bootstrap_with_repair(&self) -> Result<bool, LixError> {
        if try_claim_live_state_bootstrap_with_backend(self.backend.as_ref()).await? {
            return Ok(true);
        }
        let mode = load_live_state_mode_with_backend(self.backend.as_ref()).await?;
        if mode != LiveStateMode::Uninitialized {
            return Ok(false);
        }
        self.backend
            .execute(LIVE_STATE_STATUS_SEED_ROW_SQL, &[])
            .await?;
        try_claim_live_state_bootstrap_with_backend(self.backend.as_ref()).await
    }

    async fn reset_failed_live_state_bootstrap(&self) -> Result<(), LixError> {
        let mode = if load_latest_canonical_watermark(self.backend.as_ref())
            .await?
            .is_some()
        {
            LiveStateMode::NeedsRebuild
        } else {
            LiveStateMode::Uninitialized
        };
        mark_live_state_mode_with_backend(self.backend.as_ref(), mode).await
    }
}

fn init_step_error(step: &str, error: LixError) -> LixError {
    LixError::new(
        &error.code,
        &format!("initialize step `{step}` failed: {}", error.description),
    )
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
