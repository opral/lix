mod backend;
mod wasmtime_runtime;

use lix_engine::{BootKeyValue, EngineConfig, ExecuteOptions, WasmRuntime};
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub struct OpenLixConfig {
    pub backend: Option<Box<dyn LixBackend + Send + Sync>>,
    pub wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    pub key_values: Vec<BootKeyValueConfig>,
}

impl Default for OpenLixConfig {
    fn default() -> Self {
        Self {
            backend: None,
            wasm_runtime: None,
            key_values: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootKeyValueConfig {
    pub key: String,
    pub value: JsonValue,
    pub version_id: Option<String>,
    pub untracked: Option<bool>,
}

pub struct Lix {
    engine: lix_engine::Engine,
}

#[must_use = "LixTransaction must be committed or rolled back"]
pub struct LixTransaction<'a> {
    inner: Option<lix_engine::EngineTransaction<'a>>,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    let resolved = resolve_open_config(config)?;
    let engine = if resolved.backend_was_provided {
        lix_engine::Engine::open(resolved.into_engine_config()).await?
    } else {
        lix_engine::Engine::open_or_init(resolved.into_engine_config())
            .await?
            .engine
    };
    Ok(Lix { engine })
}

pub async fn init_lix(config: OpenLixConfig) -> Result<InitLixResult, LixError> {
    let resolved = resolve_open_config(config)?;
    let result = lix_engine::Engine::open_or_init(resolved.into_engine_config()).await?;
    Ok(InitLixResult {
        initialized: result.initialized,
    })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<ExecuteResult, LixError> {
        self.execute_with_options(sql, params, ExecuteOptionsConfig::default())
            .await
    }

    pub async fn execute_with_options(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptionsConfig,
    ) -> Result<ExecuteResult, LixError> {
        self.engine
            .execute_with_options(sql, params, to_engine_execute_options(options))
            .await
    }

    pub async fn begin_transaction(&self) -> Result<LixTransaction<'_>, LixError> {
        self.begin_transaction_with_options(ExecuteOptionsConfig::default())
            .await
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptionsConfig,
    ) -> Result<LixTransaction<'_>, LixError> {
        let tx = self
            .engine
            .begin_transaction_with_options(to_engine_execute_options(options))
            .await?;
        Ok(LixTransaction { inner: Some(tx) })
    }
}

impl<'a> LixTransaction<'a> {
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        let tx = self.inner.as_mut().ok_or_else(inactive_transaction_error)?;
        tx.execute(sql, params).await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let tx = self.inner.take().ok_or_else(inactive_transaction_error)?;
        tx.commit().await
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let tx = self.inner.take().ok_or_else(inactive_transaction_error)?;
        tx.rollback().await
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExecuteOptionsConfig {
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitLixResult {
    pub initialized: bool,
}

struct ResolvedOpenConfig {
    backend_was_provided: bool,
    backend: Box<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    key_values: Vec<BootKeyValue>,
}

impl ResolvedOpenConfig {
    fn into_engine_config(self) -> EngineConfig {
        EngineConfig {
            backend: self.backend,
            wasm_runtime: self.wasm_runtime,
            key_values: self.key_values,
            active_account: None,
            access_to_internal: false,
        }
    }
}

fn resolve_open_config(config: OpenLixConfig) -> Result<ResolvedOpenConfig, LixError> {
    let OpenLixConfig {
        backend,
        wasm_runtime,
        key_values,
    } = config;

    let backend_was_provided = backend.is_some();
    let backend = match backend {
        Some(backend) => backend,
        None => Box::new(backend::sqlite::SqliteBackend::in_memory()?),
    };
    let key_values = key_values
        .into_iter()
        .map(|item| BootKeyValue {
            key: item.key,
            value: item.value,
            version_id: item.version_id,
            untracked: item.untracked,
        })
        .collect();
    let wasm_runtime = match wasm_runtime {
        Some(runtime) => runtime,
        None => Arc::new(wasmtime_runtime::WasmtimeRuntime::new()?),
    };

    Ok(ResolvedOpenConfig {
        backend_was_provided,
        backend,
        wasm_runtime,
        key_values,
    })
}

fn to_engine_execute_options(options: ExecuteOptionsConfig) -> ExecuteOptions {
    ExecuteOptions {
        writer_key: options.writer_key,
    }
}

fn inactive_transaction_error() -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "transaction is no longer active".to_string(),
    }
}

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::{
    ExecuteResult, LixBackend, LixError, QueryResult, Value, WasmComponentInstance, WasmLimits,
};
pub use wasmtime_runtime::WasmtimeRuntime;
