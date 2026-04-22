use std::time::Duration;

use crate::backend::TransactionBeginMode;
use crate::binary_cas;
use crate::canonical;
use crate::live_state;
use crate::live_state::{load_mode_with_backend, LiveStateMode};
use crate::session;
use crate::{Lix, LixBackend, LixError, SqlDialect};

use super::filesystem;
use super::seed::InitExecutor;

impl<'engine, 'tx> InitExecutor<'engine, 'tx> {
    async fn load_checkpoint_version_heads_for_init(
        &mut self,
    ) -> Result<Vec<crate::canonical::CheckpointVersionHeadFact>, LixError> {
        let mut backend = crate::backend::transaction_backend_view(self.backend_transaction_mut()?);
        session::load_checkpoint_version_heads_for_init(&mut backend).await
    }
}

pub(crate) async fn init(lix: &Lix) -> Result<(), LixError> {
    lix.engine().try_mark_init_in_progress()?;

    if load_mode_with_backend(lix.engine().backend().as_ref()).await?
        != LiveStateMode::Uninitialized
    {
        lix.engine().reset_init_state();
        return Err(crate::common::already_initialized_error());
    }

    let mut transaction = lix
        .engine()
        .backend()
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    let init_result = async {
        {
            let backend = crate::backend::transaction_backend_view(transaction.as_mut());
            prepare_backend_for_init(&backend)
                .await
                .map_err(|error| init_step_error("prepare_backend_for_init", error))?;
            live_state::init(&backend)
                .await
                .map_err(|error| init_step_error("live_state::init", error))?;
            super::schema_bootstrap::init_builtin_schema_storage(&backend)
                .await
                .map_err(|error| init_step_error("init::init_builtin_schema_storage", error))?;
            canonical::init(&backend)
                .await
                .map_err(|error| init_step_error("canonical::init", error))?;
            binary_cas::init(&backend)
                .await
                .map_err(|error| init_step_error("binary_cas::init", error))?;
            session::init(&backend)
                .await
                .map_err(|error| init_step_error("session::init", error))?;
        }
        let mut init = InitExecutor::new(lix, transaction.as_mut())
            .map_err(|error| init_step_error("init_executor", error))?;
        init.seed_builtin_registered_schemas()
            .await
            .map_err(|error| init_step_error("init::seed_builtin_registered_schemas", error))?;
        let default_active_version_id = init
            .seed_default_versions()
            .await
            .map_err(|error| init_step_error("session::version_ops::seed_bootstrap", error))?;
        filesystem::seed_bootstrap(&mut init)
            .await
            .map_err(|error| init_step_error("filesystem_bootstrap::seed_bootstrap", error))?;
        init.seed_commit_graph_nodes()
            .await
            .map_err(|error| init_step_error("canonical::seed_bootstrap", error))?;
        let checkpoint_version_heads = init
            .load_checkpoint_version_heads_for_init()
            .await
            .map_err(|error| {
                init_step_error(
                    "session::version_ops::load_checkpoint_version_heads_with_executor",
                    error,
                )
            })?;
        init.seed_checkpoint_labels_bootstrap(&checkpoint_version_heads)
            .await
            .map_err(|error| {
                init_step_error("canonical::seed_checkpoint_labels_bootstrap", error)
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
        live_state::initialize_in_transaction(transaction.as_mut())
            .await
            .map_err(|error| init_step_error("live_state::initialize_in_transaction", error))
    }
    .await;

    let result = match init_result {
        Ok(()) => Ok(()),
        Err(error) => Err(lix.normalize_init_error(error).await),
    };

    if result.is_ok() {
        transaction.commit().await?;
        if lix.engine().deterministic_boot_pending() {
            lix.engine().clear_deterministic_boot_pending();
        }
        lix.engine().mark_init_completed();
        lix.open_existing().await?;
    } else {
        let _ = transaction.rollback().await;
        lix.engine().reset_init_state();
    }

    result
}

pub(crate) async fn init_if_needed(lix: &Lix) -> Result<bool, LixError> {
    match init(lix).await {
        Ok(()) => Ok(true),
        Err(error) if error.code == crate::common::ErrorCode::AlreadyInitialized.as_str() => {
            lix.wait_for_concurrent_init_ready().await?;
            lix.open_existing().await?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

impl Lix {
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
        Ok(
            load_mode_with_backend(self.engine().backend().as_ref()).await?
                != LiveStateMode::Uninitialized,
        )
    }

    async fn normalize_init_error(&self, error: LixError) -> LixError {
        if error.code == crate::common::ErrorCode::AlreadyInitialized.as_str() {
            return error;
        }
        if is_init_conflict_error(&error.description) {
            return crate::common::already_initialized_error();
        }
        if is_init_locked_error(&error.description) {
            return crate::common::already_initialized_error();
        }
        error
    }

    async fn wait_for_concurrent_init_ready(&self) -> Result<(), LixError> {
        const ATTEMPTS: usize = 2400;
        const DELAY_MS: u64 = 50;

        for attempt in 0..ATTEMPTS {
            match load_mode_with_backend(self.engine().backend().as_ref()).await? {
                LiveStateMode::Ready => return Ok(()),
                LiveStateMode::Bootstrapping => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::common::live_state_not_ready_error());
                    }
                }
                LiveStateMode::Uninitialized => {
                    if attempt + 1 == ATTEMPTS {
                        return Err(crate::common::not_initialized_error());
                    }
                }
                LiveStateMode::NeedsRebuild | LiveStateMode::Rebuilding => {
                    return Err(crate::common::live_state_not_ready_error())
                }
            }
            std::thread::sleep(Duration::from_millis(DELAY_MS));
        }
        Err(crate::common::live_state_not_ready_error())
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

#[cfg(test)]
mod tests {
    use crate::test_support::TestSqliteBackend;
    use crate::wasm::NoopWasmRuntime;
    use crate::{LixConfig, Value};
    use std::sync::Arc;

    fn value_as_text(value: &Value) -> String {
        match value {
            Value::Text(value) => value.clone(),
            other => panic!("expected text value, got {other:?}"),
        }
    }

    fn value_as_bool(value: &Value) -> bool {
        match value {
            Value::Boolean(value) => *value,
            Value::Integer(value) => *value != 0,
            Value::Text(value) => matches!(value.as_str(), "1" | "true" | "TRUE"),
            other => panic!("expected boolean-compatible value, got {other:?}"),
        }
    }

    #[test]
    fn fresh_init_bootstrap_rows_are_journal_backed_without_compatibility_fallbacks() {
        std::thread::Builder::new()
            .name("fresh-init-bootstrap-journal-invariants".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime should build");
                runtime.block_on(async {
                    let backend = TestSqliteBackend::new();
                    let lix = Arc::new(crate::Lix::boot(LixConfig::new(
                        Box::new(backend),
                        Arc::new(NoopWasmRuntime),
                    )));

                    lix.initialize().await.expect("initialize should succeed");

                    let visible_bootstrap_rows = lix
                        .execute(
                            "SELECT schema_key, entity_id, change_id \
                             FROM lix_state_by_version \
                             WHERE untracked = true \
                               AND schema_key IN ('lix_version_ref', 'lix_registered_schema') \
                             ORDER BY schema_key, entity_id",
                            &[],
                        )
                        .await
                        .expect("bootstrap visible state query should succeed");
                    assert!(
                        !visible_bootstrap_rows.statements[0].rows.is_empty(),
                        "expected fresh init to materialize bootstrap rows"
                    );

                    for row in &visible_bootstrap_rows.statements[0].rows {
                        let schema_key = value_as_text(&row[0]);
                        let entity_id = value_as_text(&row[1]);
                        let change_id = value_as_text(&row[2]);
                        assert!(
                            !change_id.is_empty(),
                            "bootstrap row {schema_key}/{entity_id} must expose a real change_id"
                        );

                        let backing_change = lix
                            .execute(
                                "SELECT id, untracked \
                                 FROM lix_change \
                                 WHERE id = $1",
                                &[Value::Text(change_id.clone())],
                            )
                            .await
                            .expect("backing lix_change query should succeed");
                        assert_eq!(
                            backing_change.statements[0].rows.len(),
                            1,
                            "bootstrap row {schema_key}/{entity_id} should have exactly one canonical backing row"
                        );
                        assert_eq!(
                            value_as_text(&backing_change.statements[0].rows[0][0]),
                            change_id
                        );
                        assert!(
                            value_as_bool(&backing_change.statements[0].rows[0][1]),
                            "bootstrap row {schema_key}/{entity_id} should be backed by a canonical row whose public untracked flag derives from visibility"
                        );
                    }
                });
            })
            .expect("fresh-init test thread should spawn")
            .join()
            .expect("fresh-init test thread should not panic");
    }
}
