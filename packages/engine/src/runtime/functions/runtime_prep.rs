use crate::runtime::deterministic_mode::{
    load_runtime_settings, DeterministicSettings, PersistedKeyValueStorageScope,
    RuntimeFunctionProvider,
};
use crate::runtime::functions::SharedFunctionProvider;
use crate::{LixBackend, LixError};

use crate::runtime::Runtime;

impl Runtime {
    pub(crate) async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
        storage_scope: &PersistedKeyValueStorageScope,
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
            let settings = load_runtime_settings(backend, storage_scope).await?;
            self.cache_deterministic_settings(settings);
            settings
        };

        let functions = SharedFunctionProvider::new(RuntimeFunctionProvider::new(settings, None));
        Ok((settings, functions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::wasm::NoopWasmRuntime;
    use crate::{Lix, LixConfig, QueryResult, SqlDialect, Value};
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
        let lix = Lix::boot(LixConfig::new(Box::new(backend), Arc::new(NoopWasmRuntime)));

        let (settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("first runtime preparation should succeed");
        assert!(!settings.enabled);
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "first call should read deterministic settings from the backend"
        );

        let (_settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("second runtime preparation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            1,
            "disabled deterministic settings should be served from cache"
        );

        lix.runtime().invalidate_deterministic_settings_cache();

        let (_settings, _) = lix
            .prepare_runtime_functions_with_backend(lix.backend().as_ref())
            .await
            .expect("runtime preparation after invalidation should succeed");
        assert_eq!(
            execute_calls.load(Ordering::SeqCst),
            2,
            "cache invalidation should force a backend refresh"
        );
    }
}
