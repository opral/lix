use std::time::Duration;

use crate::binary_cas;
use crate::canonical;
use crate::engine::Engine;
use crate::live_state;
use crate::live_state::{
    load_latest_live_state_replay_cursor_with_backend, load_mode_with_backend,
    mark_live_state_projection_ready_with_backend, mark_mode_with_backend,
    rebuild_scope_in_transaction, try_claim_bootstrap_with_backend, LiveStateMode,
    LiveStateRebuildDebugMode, LiveStateRebuildRequest, LiveStateRebuildScope,
};
use crate::runtime::TransactionBackendAdapter;
use crate::schema;
use crate::session::observe;
use crate::session::version_ops;
use crate::session::version_ops::commit;
use crate::session::workspace;
use crate::version_state;
use crate::{LixBackend, LixError, SqlDialect, TransactionMode};

use super::filesystem;
use super::InitExecutor;

pub(crate) async fn init(engine: &Engine) -> Result<(), LixError> {
    engine.try_mark_init_in_progress()?;

    if load_mode_with_backend(engine.backend().as_ref()).await? != LiveStateMode::Uninitialized {
        engine.reset_init_state();
        return Err(crate::common::errors::already_initialized_error());
    }

    let mut transaction = engine
        .backend()
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
            commit::init(&backend)
                .await
                .map_err(|error| init_step_error("commit::init", error))?;
            binary_cas::init(&backend)
                .await
                .map_err(|error| init_step_error("binary_cas::init", error))?;
            observe::init(&backend)
                .await
                .map_err(|error| init_step_error("observe::init", error))?;
            version_state::checkpoints::cache::init(&backend)
                .await
                .map_err(|error| {
                    init_step_error("version_state::checkpoints::cache::init", error)
                })?;
            version_ops::init(&backend)
                .await
                .map_err(|error| init_step_error("session::version_ops::init", error))?;
            workspace::init(&backend)
                .await
                .map_err(|error| init_step_error("workspace::init", error))?;
        }
        {
            let backend = TransactionBackendAdapter::new(transaction.as_mut());
            if !try_claim_bootstrap_with_backend(&backend).await? {
                return Err(crate::common::errors::already_initialized_error());
            }
        }
        claimed_bootstrap = true;

        let mut init = InitExecutor::new(engine, transaction.as_mut())
            .map_err(|error| init_step_error("init_executor", error))?;
        schema::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("schema::seed_bootstrap", error))?;
        let default_active_version_id = version_ops::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("session::version_ops::seed_bootstrap", error))?;
        filesystem::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("filesystem_bootstrap::seed_bootstrap", error))?;
        canonical::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("canonical::seed_bootstrap", error))?;
        let checkpoint_version_heads = {
            let mut backend = init.backend_adapter();
            crate::session::version_ops::descriptors::load_checkpoint_version_heads_with_executor(
                &mut backend,
            )
            .await
            .map_err(|error| {
                init_step_error(
                    "session::version_ops::load_checkpoint_version_heads_with_executor",
                    error,
                )
            })?
            .into_iter()
            .map(
                |head| crate::canonical::checkpoint_labels::CheckpointVersionHeadFact {
                    version_id: head.version_id,
                    head_commit_id: head.head_commit_id,
                },
            )
            .collect::<Vec<_>>()
        };
        canonical::checkpoint_labels::seed_bootstrap(&mut init, &checkpoint_version_heads)
            .await
            .map_err(|error| {
                init_step_error("canonical::checkpoint_labels::seed_bootstrap", error)
            })?;
        init.seed_boot_config_key_values(&default_active_version_id)
            .await
            .map_err(|error| init_step_error("InitExecutor::seed_boot_config_key_values", error))?;
        init.seed_lix_id_key()
            .await
            .map_err(|error| init_step_error("InitExecutor::seed_lix_id_key", error))?;
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
        rebuild_scope_in_transaction(
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
            let cursor = load_latest_live_state_replay_cursor_with_backend(&backend)
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "initialize expected replay cursor after bootstrap seeding",
                    )
                })?;
            mark_live_state_projection_ready_with_backend(&backend, &cursor).await
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
        Err(error)
            if error.code == crate::common::errors::ErrorCode::AlreadyInitialized.as_str() =>
        {
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
        Ok(load_mode_with_backend(self.backend().as_ref()).await? != LiveStateMode::Uninitialized)
    }

    async fn normalize_init_error(&self, error: LixError) -> LixError {
        if error.code == crate::common::errors::ErrorCode::AlreadyInitialized.as_str() {
            return error;
        }
        if is_init_conflict_error(&error.description) {
            return crate::common::errors::already_initialized_error();
        }
        if is_init_locked_error(&error.description) {
            return crate::common::errors::already_initialized_error();
        }
        error
    }

    async fn wait_for_concurrent_init_ready(&self) -> Result<(), LixError> {
        const ATTEMPTS: usize = 2400;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match load_mode_with_backend(self.backend().as_ref()).await? {
                LiveStateMode::Ready => return Ok(()),
                LiveStateMode::Bootstrapping => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::common::errors::live_state_not_ready_error());
                    }
                }
                LiveStateMode::Uninitialized => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::common::errors::not_initialized_error());
                    }
                }
                LiveStateMode::NeedsRebuild | LiveStateMode::Rebuilding => {
                    return Err(crate::common::errors::live_state_not_ready_error())
                }
            }
            std::thread::sleep(Duration::from_millis(DELAY_MS));
        }
        Err(crate::common::errors::live_state_not_ready_error())
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

async fn prepare_backend_for_init(backend: &dyn LixBackend) -> Result<(), LixError> {
    if backend.dialect() == SqlDialect::Sqlite {
        backend.execute("PRAGMA foreign_keys = ON", &[]).await?;
    }
    Ok(())
}
