mod backend;
mod wasmtime_runtime;

use lix_engine::{
    boot, init_lix as engine_init_lix, BootArgs, BootKeyValue, ExecuteOptions, InitLixArgs,
    InitLixResult as EngineInitLixResult, WasmRuntime,
};
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

pub async fn open_lix(config: OpenLixConfig) -> Result<Lix, LixError> {
    let resolved = resolve_open_config(config)?;
    let engine = boot(BootArgs {
        backend: resolved.backend,
        wasm_runtime: resolved.wasm_runtime,
        key_values: resolved.key_values,
        active_account: None,
        access_to_internal: false,
    });
    Ok(Lix { engine })
}

pub async fn init_lix(config: OpenLixConfig) -> Result<EngineInitLixResult, LixError> {
    let resolved = resolve_open_config(config)?;
    engine_init_lix(InitLixArgs {
        backend: resolved.backend,
        wasm_runtime: resolved.wasm_runtime,
        key_values: resolved.key_values,
    })
    .await
}

impl Lix {
    pub async fn init(&self) -> Result<(), LixError> {
        self.engine.init_if_needed().await.map(|_| ())
    }

    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.engine
            .execute(sql, params, ExecuteOptions::default())
            .await
    }
}

pub use backend::sqlite::SqliteBackend;
pub use lix_engine::{
    InitLixResult, LixBackend, LixError, QueryResult, Value, WasmComponentInstance, WasmLimits,
};
pub use wasmtime_runtime::WasmtimeRuntime;

struct ResolvedOpenConfig {
    backend: Box<dyn LixBackend + Send + Sync>,
    wasm_runtime: Arc<dyn WasmRuntime>,
    key_values: Vec<BootKeyValue>,
}

fn resolve_open_config(config: OpenLixConfig) -> Result<ResolvedOpenConfig, LixError> {
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

    Ok(ResolvedOpenConfig {
        backend,
        wasm_runtime,
        key_values,
    })
}
