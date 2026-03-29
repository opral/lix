use std::time::Duration;

use crate::account;
use crate::canonical;
use crate::checkpoint;
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::filesystem;
use crate::key_value;
use crate::live_state;
use crate::live_state::{
    load_latest_canonical_watermark, load_mode_with_backend, mark_mode_with_backend,
    try_claim_bootstrap_with_backend, LiveStateMode, LiveStateRebuildDebugMode,
    LiveStateRebuildRequest, LiveStateRebuildScope,
};
use crate::observe;
use crate::schema;
use crate::undo_redo;
use crate::version;
use crate::{LixError, TransactionMode};

use super::tables::prepare_backend_for_init;
use super::InitExecutor;

pub(crate) async fn init(engine: &Engine) -> Result<(), LixError> {
    engine.try_mark_init_in_progress()?;

    if load_mode_with_backend(engine.backend.as_ref()).await? != LiveStateMode::Uninitialized {
        engine.reset_init_state();
        return Err(crate::errors::already_initialized_error());
    }

    let mut transaction = engine
        .backend
        .begin_transaction(TransactionMode::Write)
        .await?;
    let mut claimed_bootstrap = false;
    let init_result = async {
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            prepare_backend_for_init(&backend)
                .await
                .map_err(|error| init_step_error("prepare_backend_for_init", error))?;
            live_state::init(&backend)
                .await
                .map_err(|error| init_step_error("live_state::init", error))?;
            schema::init(&backend)
                .await
                .map_err(|error| init_step_error("schema::init", error))?;
            canonical::init(&backend)
                .await
                .map_err(|error| init_step_error("canonical::init", error))?;
            filesystem::init(&backend)
                .await
                .map_err(|error| init_step_error("filesystem::init", error))?;
            checkpoint::init(&backend)
                .await
                .map_err(|error| init_step_error("checkpoint::init", error))?;
            undo_redo::init(&backend)
                .await
                .map_err(|error| init_step_error("undo_redo::init", error))?;
            observe::init(&backend)
                .await
                .map_err(|error| init_step_error("observe::init", error))?;
            key_value::init(&backend)
                .await
                .map_err(|error| init_step_error("key_value::init", error))?;
            version::init(&backend)
                .await
                .map_err(|error| init_step_error("version::init", error))?;
            account::init(&backend)
                .await
                .map_err(|error| init_step_error("account::init", error))?;
        }
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            if !try_claim_bootstrap_with_backend(&backend).await? {
                return Err(crate::errors::already_initialized_error());
            }
        }
        claimed_bootstrap = true;

        let mut init = InitExecutor::new(engine, transaction.as_mut())
            .map_err(|error| init_step_error("init_executor", error))?;
        schema::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("schema::seed_bootstrap", error))?;
        let default_active_version_id = version::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("version::seed_bootstrap", error))?;
        filesystem::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("filesystem::seed_bootstrap", error))?;
        canonical::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("canonical::seed_bootstrap", error))?;
        checkpoint::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("checkpoint::seed_bootstrap", error))?;
        key_value::seed_bootstrap(&mut init, &default_active_version_id)
            .await
            .map_err(|error| init_step_error("key_value::seed_bootstrap", error))?;
        account::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("account::seed_bootstrap", error))?;
        init.persist_runtime_state()
            .await
            .map_err(|error| init_step_error("persist_runtime_state", error))?;
        drop(init);
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            mark_mode_with_backend(&backend, LiveStateMode::Rebuilding)
                .await
                .map_err(|error| init_step_error("mark_live_state_rebuilding", error))?;
        }
        live_state::rebuild_scope_in_transaction(
            transaction.as_mut(),
            &LiveStateRebuildRequest {
                scope: LiveStateRebuildScope::Full,
                debug: LiveStateRebuildDebugMode::Off,
                debug_row_limit: 0,
            },
        )
        .await
        .map_err(|error| init_step_error("live_state::rebuild_scope_in_transaction", error))?;
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            let watermark = load_latest_canonical_watermark(&backend)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "initialize expected canonical watermark after bootstrap seeding",
                    )
                })?;
            live_state::projection::mark_live_state_projection_ready_with_backend(
                &backend, &watermark,
            )
            .await
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
