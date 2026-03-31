use crate::backend::program::WriteProgram;
use crate::backend::program_runner::execute_write_program_with_transaction;
use crate::deterministic_mode::{
    build_persist_sequence_highest_batch, load_runtime_sequence_start_in_transaction,
    load_runtime_settings, DeterministicSettings, RuntimeFunctionProvider,
};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::runtime::Runtime;
use crate::{LixBackend, LixBackendTransaction, LixError};

impl Runtime {
    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    > {
        let settings = if self.deterministic_boot_pending() {
            self.boot_deterministic_settings()
                .unwrap_or_else(DeterministicSettings::disabled)
        } else if let Some(settings) = self.cached_deterministic_settings() {
            settings
        } else {
            let settings = load_runtime_settings(backend).await?;
            self.cache_deterministic_settings(settings);
            settings
        };

        let functions = SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, None));
        Ok((settings, functions))
    }

    pub(crate) async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if !functions.deterministic_sequence_enabled()
            || functions.deterministic_sequence_initialized()
        {
            return Ok(());
        }
        let sequence_start = load_runtime_sequence_start_in_transaction(transaction).await?;
        let mut functions = functions.clone();
        functions.initialize_deterministic_sequence(sequence_start);
        Ok(())
    }

    pub(crate) async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        settings: DeterministicSettings,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let Some(sequence_start) = functions.with_lock(|provider| provider.sequence_start())
            else {
                return Ok(());
            };
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                let batch =
                    build_persist_sequence_highest_batch(sequence_end - 1, transaction.dialect())?;
                let mut program = WriteProgram::new();
                program.push_batch(batch);
                execute_write_program_with_transaction(transaction, program).await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{boot, BootArgs, NoopWasmRuntime, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct CountingBackend {
        execute_calls: Arc<AtomicUsize>,
    }

    #[async_trait(?Send)]
    impl LixBackend for CountingBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, _sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this test",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    #[tokio::test]
    async fn caches_disabled_deterministic_settings_until_invalidated() {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            execute_calls: Arc::clone(&execute_calls),
        };
        let engine = boot(BootArgs::new(Box::new(backend), Arc::new(NoopWasmRuntime)));

        let (settings, _) = engine
            .runtime()
            .prepare_runtime_functions_with_backend(engine.backend().as_ref())
            .await
            .expect("first runtime preparation should succeed");
        assert!(!settings.enabled);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "first call should read deterministic settings from the backend"
        );

        let (_settings, _) = engine
            .runtime()
            .prepare_runtime_functions_with_backend(engine.backend().as_ref())
            .await
            .expect("second runtime preparation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "disabled deterministic settings should be served from cache"
        );

        engine.runtime().invalidate_deterministic_settings_cache();

        let (_settings, _) = engine
            .runtime()
            .prepare_runtime_functions_with_backend(engine.backend().as_ref())
            .await
            .expect("runtime preparation after invalidation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            2,
            "cache invalidation should force a backend refresh"
        );
    }
}
