mod backend;
mod wasmtime_runtime;

use lix_engine::{boot, BootArgs, BootKeyValue, ExecuteOptions, WasmRuntime};
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
    engine: Engine,
}

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    let OpenLixConfig {
        backend,
        wasm_runtime,
        key_values,
    } = config;

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
    let engine = boot(BootArgs {
        backend,
        wasm_runtime,
        key_values,
        active_account: None,
        access_to_internal: false,
    });
    engine.init().await?;
    Ok(Lix { engine })
}

impl Lix {
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.engine
            .execute(sql, params, ExecuteOptions::default())
            .await
    }
}

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::{
    Engine, LixBackend, LixError, QueryResult, Value, WasmComponentInstance, WasmLimits,
};
pub use wasmtime_runtime::WasmtimeRuntime;
