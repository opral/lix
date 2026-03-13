use crate::deterministic_mode::{
    build_persist_sequence_highest_batch, load_runtime_sequence_start, load_runtime_settings,
    persist_sequence_highest, DeterministicSettings, RuntimeFunctionProvider,
};
use crate::engine::Engine;
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::state::internal::write_program::WriteProgram;
use crate::{LixBackend, LixError, LixTransaction};

impl Engine {
    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
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

        let sequence_start = if settings.enabled {
            load_runtime_sequence_start(backend).await?
        } else {
            0
        };
        let functions =
            SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, sequence_start));
        Ok((settings, sequence_start, functions))
    }

    pub(crate) async fn persist_runtime_sequence_with_backend(
        &self,
        backend: &dyn LixBackend,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
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
        sequence_start: i64,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError> {
        if settings.enabled {
            let sequence_end = functions.with_lock(|provider| provider.next_sequence());
            if sequence_end > sequence_start {
                let mut program = WriteProgram::new();
                program.push_batch(build_persist_sequence_highest_batch(
                    sequence_end - 1,
                    transaction.dialect(),
                )?);
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
    }

    #[tokio::test]
    async fn caches_disabled_deterministic_settings_until_invalidated() {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let backend = CountingBackend {
            execute_calls: Arc::clone(&execute_calls),
        };
        let engine = boot(BootArgs::new(Box::new(backend), Arc::new(NoopWasmRuntime)));

        let (settings, sequence_start, _) = engine
            .prepare_runtime_functions_with_backend(engine.backend_ref())
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
            .prepare_runtime_functions_with_backend(engine.backend_ref())
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
            .prepare_runtime_functions_with_backend(engine.backend_ref())
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
