use std::time::Duration;

use crate::engine::{Engine, TransactionBackendAdapter};
use crate::live_state::{
    init as init_live_state, load_latest_canonical_watermark, load_mode_with_backend,
    mark_mode_with_backend, mark_ready_with_backend, try_claim_bootstrap_with_backend,
    LiveStateMode,
};
use crate::LixError;

use super::seed::InitExecutor;
use super::tables::{create_backend_tables, create_builtin_schema_tables};

pub(crate) async fn init(engine: &Engine) -> Result<(), LixError> {
    engine.try_mark_init_in_progress()?;

    if load_mode_with_backend(engine.backend.as_ref()).await? != LiveStateMode::Uninitialized {
        engine.reset_init_state();
        return Err(crate::errors::already_initialized_error());
    }

    let mut transaction = engine.backend.begin_transaction().await?;
    let mut claimed_bootstrap = false;
    let init_result = async {
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            create_backend_tables(&backend)
                .await
                .map_err(|error| init_step_error("create_backend_tables", error))?;
            init_live_state(&backend)
                .await
                .map_err(|error| init_step_error("live_state::init", error))?;
        }
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            if !try_claim_bootstrap_with_backend(&backend).await? {
                return Err(crate::errors::already_initialized_error());
            }
        }
        claimed_bootstrap = true;

        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            create_builtin_schema_tables(&backend)
                .await
                .map_err(|error| init_step_error("create_builtin_schema_tables", error))?;
        }
        let mut init = InitExecutor::new(engine, transaction.as_mut())
            .map_err(|error| init_step_error("init_executor", error))?;
        init.seed_builtin_schemas()
            .await
            .map_err(|error| init_step_error("seed_builtin_schemas", error))?;
        let default_active_version_id = init
            .seed_default_versions()
            .await
            .map_err(|error| init_step_error("seed_default_versions", error))?;
        init.seed_global_system_directories()
            .await
            .map_err(|error| init_step_error("seed_global_system_directories", error))?;
        init.seed_commit_graph_nodes()
            .await
            .map_err(|error| init_step_error("seed_commit_graph_nodes", error))?;
        init.seed_default_active_version(&default_active_version_id)
            .await
            .map_err(|error| init_step_error("seed_default_active_version", error))?;
        init.seed_lix_id()
            .await
            .map_err(|error| init_step_error("seed_lix_id", error))?;
        init.seed_default_checkpoint_label()
            .await
            .map_err(|error| init_step_error("seed_default_checkpoint_label", error))?;
        init.rebuild_internal_last_checkpoint()
            .await
            .map_err(|error| init_step_error("rebuild_internal_last_checkpoint", error))?;
        init.seed_boot_key_values()
            .await
            .map_err(|error| init_step_error("seed_boot_key_values", error))?;
        init.seed_boot_account()
            .await
            .map_err(|error| init_step_error("seed_boot_account", error))?;
        drop(init);
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            mark_mode_with_backend(&backend, LiveStateMode::Rebuilding)
                .await
                .map_err(|error| init_step_error("mark_live_state_rebuilding", error))?;
            let watermark = load_latest_canonical_watermark(&backend)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "initialize expected canonical watermark after bootstrap seeding",
                    )
                })?;
            mark_ready_with_backend(&backend, &watermark).await
        }
    }
    .await;

    let result = match init_result {
        Ok(()) => Ok(()),
        Err(error) => Err(engine.normalize_init_error(error).await),
    };

    if result.is_ok() {
        transaction.commit().await?;
        if engine.deterministic_boot_pending() {
            engine.clear_deterministic_boot_pending();
        }
        engine.mark_init_completed();
        engine.load_and_cache_active_version().await?;
        engine.refresh_public_surface_registry().await?;
    } else {
        let _ = transaction.rollback().await;
        engine.reset_init_state();
    }

    result
}

pub(crate) async fn init_if_needed(engine: &Engine) -> Result<bool, LixError> {
    match init(engine).await {
        Ok(()) => Ok(true),
        Err(error) if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() => {
            engine.wait_for_concurrent_init_ready().await?;
            engine.load_and_cache_active_version().await?;
            engine.refresh_public_surface_registry().await?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

impl Engine {
    #[doc(hidden)]
    pub async fn initialize(&self) -> Result<(), LixError> {
        crate::init::init(self).await
    }

    #[doc(hidden)]
    pub async fn initialize_if_needed(&self) -> Result<bool, LixError> {
        crate::init::init_if_needed(self).await
    }

    pub async fn is_initialized(&self) -> Result<bool, LixError> {
        self.backend_has_been_initialized().await
    }

    async fn backend_has_been_initialized(&self) -> Result<bool, LixError> {
        Ok(load_mode_with_backend(self.backend.as_ref()).await? != LiveStateMode::Uninitialized)
    }

    async fn normalize_init_error(&self, error: LixError) -> LixError {
        if error.code == crate::errors::ErrorCode::AlreadyInitialized.as_str() {
            return error;
        }
        if is_init_conflict_error(&error.description) {
            return crate::errors::already_initialized_error();
        }
        if is_init_locked_error(&error.description) {
            return crate::errors::already_initialized_error();
        }
        error
    }

    async fn wait_for_concurrent_init_ready(&self) -> Result<(), LixError> {
        const ATTEMPTS: usize = 2400;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match load_mode_with_backend(self.backend.as_ref()).await? {
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
        || normalized.contains("already exists")
        || normalized.contains("already exists in version 'global'")
}

fn is_init_locked_error(description: &str) -> bool {
    let normalized = description.to_ascii_lowercase();
    normalized.contains("database is locked")
        || normalized.contains("database schema is locked")
        || normalized.contains("database table is locked")
}
