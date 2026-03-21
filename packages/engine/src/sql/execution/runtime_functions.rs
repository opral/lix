use crate::deterministic_mode::{
    build_persist_sequence_highest_batch, load_runtime_sequence_start, load_runtime_settings,
    persist_sequence_highest, DeterministicSettings, RuntimeFunctionProvider,
};
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::state::internal::write_program::WriteProgram;
use crate::{LixBackend, LixError, LixTransaction};

impl Engine {
    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
        defer_sequence_load: bool,
    ) -> Result<
        (
            DeterministicSettings,
            i64,
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

        let sequence_start = if settings.enabled && !defer_sequence_load {
            load_runtime_sequence_start(backend).await?
        } else {
            0
        };
        let functions = SharedFunctionProvider::new(RuntimeFunctionProvider::new(
            settings,
            if settings.enabled && defer_sequence_load {
                None
            } else {
                Some(sequence_start)
            },
        ));
        Ok((settings, sequence_start, functions))
    }

    pub(crate) async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if !functions.deterministic_sequence_enabled()
            || functions.deterministic_sequence_initialized()
        {
            return Ok(());
        }
        let backend = TransactionBackendAdapter::new(transaction);
        let sequence_start = load_runtime_sequence_start(&backend).await?;
        let mut functions = functions.clone();
        functions.initialize_deterministic_sequence(sequence_start);
        Ok(())
    }

    pub(crate) async fn persist_runtime_sequence_with_backend(
        &self,
        backend: &dyn LixBackend,
        settings: DeterministicSettings,
        _sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let Some(sequence_start) = functions.with_lock(|provider| provider.sequence_start())
            else {
                return Ok(());
            };
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                persist_sequence_highest(backend, sequence_end - 1).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixTransaction,
        settings: DeterministicSettings,
        _sequence_start: i64,
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

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "transactions are not needed in this test",
            ))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
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

        let (settings, sequence_start, _) = engine
            .prepare_runtime_functions_with_backend(engine.backend.as_ref(), false)
            .await
            .expect("first runtime preparation should succeed");
        assert!(!settings.enabled);
        assert_eq!(sequence_start, 0);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "first call should read deterministic settings from the backend"
        );

        let (_settings, sequence_start, _) = engine
            .prepare_runtime_functions_with_backend(engine.backend.as_ref(), false)
            .await
            .expect("second runtime preparation should succeed");
        assert_eq!(sequence_start, 0);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "disabled deterministic settings should be served from cache"
        );

        engine.invalidate_deterministic_settings_cache();

        let (_settings, sequence_start, _) = engine
            .prepare_runtime_functions_with_backend(engine.backend.as_ref(), false)
            .await
            .expect("runtime preparation after invalidation should succeed");
        assert_eq!(sequence_start, 0);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            2,
            "cache invalidation should force a backend refresh"
        );
    }
}
