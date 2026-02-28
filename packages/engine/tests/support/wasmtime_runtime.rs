#![allow(dead_code)]

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{LixError, WasmComponentInstance, WasmLimits, WasmRuntime};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{IoView, WasiCtx, WasiCtxBuilder, WasiView};

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "plugin",
    });
}

#[derive(Debug, serde::Deserialize)]
struct WirePluginFile {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Debug, serde::Deserialize)]
struct WireDetectChangesRequest {
    before: Option<WirePluginFile>,
    after: WirePluginFile,
    state_context: Option<WireDetectStateContext>,
}

#[derive(Debug, serde::Deserialize)]
struct WireDetectStateContext {
    active_state: Option<Vec<WireActiveStateRow>>,
}

#[derive(Debug, serde::Deserialize)]
struct WireActiveStateRow {
    entity_id: String,
    schema_key: Option<String>,
    schema_version: Option<String>,
    snapshot_content: Option<String>,
    file_id: Option<String>,
    plugin_key: Option<String>,
    version_id: Option<String>,
    change_id: Option<String>,
    metadata: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct WirePluginEntityChange {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct WireApplyChangesRequest {
    file: WirePluginFile,
    changes: Vec<WirePluginEntityChange>,
}

#[derive(Debug, serde::Serialize)]
struct WirePluginEntityChangeOutput {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    snapshot_content: Option<String>,
}

pub struct TestWasmtimeRuntime {
    engine: Engine,
    component_cache: Mutex<HashMap<ComponentCacheKey, Arc<Component>>>,
}

impl TestWasmtimeRuntime {
    pub fn new() -> Result<Self, LixError> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);
        config.consume_fuel(true);

        let engine = Engine::new(&config).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("Failed to initialize wasmtime engine: {error}"),
        })?;

        Ok(Self {
            engine,
            component_cache: Mutex::new(HashMap::new()),
        })
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct ComponentCacheKey {
    wasm_fingerprint: u64,
    wasm_len: usize,
}

impl ComponentCacheKey {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            wasm_fingerprint: wasm_fingerprint(bytes),
            wasm_len: bytes.len(),
        }
    }
}

struct TestWasmtimeInstance {
    engine: Engine,
    component: Arc<Component>,
}

struct WasiState {
    table: ResourceTable,
    ctx: WasiCtx,
}

impl IoView for WasiState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for WasiState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

#[async_trait(?Send)]
impl WasmRuntime for TestWasmtimeRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let cache_key = ComponentCacheKey::from_bytes(&bytes);

        if let Some(component) = self
            .component_cache
            .lock()
            .expect("component cache mutex poisoned")
            .get(&cache_key)
            .cloned()
        {
            return Ok(Arc::new(TestWasmtimeInstance {
                engine: self.engine.clone(),
                component,
            }));
        }

        let compiled =
            Arc::new(
                Component::new(&self.engine, &bytes).map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!("Failed to compile wasm component: {error}"),
                })?,
            );

        let component = {
            let mut cache = self
                .component_cache
                .lock()
                .expect("component cache mutex poisoned");
            cache
                .entry(cache_key)
                .or_insert_with(|| compiled.clone())
                .clone()
        };

        Ok(Arc::new(TestWasmtimeInstance {
            engine: self.engine.clone(),
            component,
        }))
    }
}

#[async_trait(?Send)]
impl WasmComponentInstance for TestWasmtimeInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let mut store = Store::new(
            &self.engine,
            WasiState {
                table: ResourceTable::new(),
                ctx: WasiCtxBuilder::new().build(),
            },
        );
        store.set_fuel(u64::MAX).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("Failed to configure wasm fuel: {error}"),
        })?;

        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("Failed to add wasi imports to linker: {error}"),
        })?;

        let bindings =
            plugin_bindings::Plugin::instantiate(&mut store, self.component.as_ref(), &linker)
                .map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    title: "Unknown error".to_string(),
                    description: format!("Failed to instantiate wasm component: {error}"),
                })?;

        match export {
            "detect-changes" | "api#detect-changes" => {
                let request: WireDetectChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!(
                            "Failed to decode detect-changes request payload: {error}"
                        ),
                    })?;

                let before = request.before.map(wire_file_to_binding);
                let after = wire_file_to_binding(request.after);
                let state_context = request.state_context.map(wire_state_context_to_binding);

                let result = bindings
                    .lix_plugin_api()
                    .call_detect_changes(
                        &mut store,
                        before.as_ref(),
                        &after,
                        state_context.as_ref(),
                    )
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!("Wasm call failed for export '{export}': {error}"),
                    })?;

                match result {
                    Ok(changes) => {
                        let wire = changes
                            .into_iter()
                            .map(|change| WirePluginEntityChangeOutput {
                                entity_id: change.entity_id,
                                schema_key: change.schema_key,
                                schema_version: change.schema_version,
                                snapshot_content: change.snapshot_content,
                            })
                            .collect::<Vec<_>>();
                        serde_json::to_vec(&wire).map_err(|error| LixError {
                            code: "LIX_ERROR_UNKNOWN".to_string(),
                            title: "Unknown error".to_string(),
                            description: format!(
                                "Failed to encode detect-changes response payload: {error}"
                            ),
                        })
                    }
                    Err(error) => Err(map_plugin_error(error)),
                }
            }
            "apply-changes" | "api#apply-changes" => {
                let request: WireApplyChangesRequest =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!(
                            "Failed to decode apply-changes request payload: {error}"
                        ),
                    })?;

                let file = wire_file_to_binding(request.file);
                let changes = request
                    .changes
                    .into_iter()
                    .map(wire_change_to_binding)
                    .collect::<Vec<_>>();

                let result = bindings
                    .lix_plugin_api()
                    .call_apply_changes(&mut store, &file, &changes)
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        title: "Unknown error".to_string(),
                        description: format!("Wasm call failed for export '{export}': {error}"),
                    })?;

                match result {
                    Ok(output) => Ok(output),
                    Err(error) => Err(map_plugin_error(error)),
                }
            }
            other => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("Unsupported export '{other}' for TestWasmtimeRuntime"),
            }),
        }
    }
}

fn wasm_fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn wire_file_to_binding(file: WirePluginFile) -> plugin_bindings::exports::lix::plugin::api::File {
    plugin_bindings::exports::lix::plugin::api::File {
        id: file.id,
        path: file.path,
        data: file.data,
    }
}

fn wire_change_to_binding(
    change: WirePluginEntityChange,
) -> plugin_bindings::exports::lix::plugin::api::EntityChange {
    plugin_bindings::exports::lix::plugin::api::EntityChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        snapshot_content: change.snapshot_content,
    }
}

fn wire_state_context_to_binding(
    context: WireDetectStateContext,
) -> plugin_bindings::exports::lix::plugin::api::DetectStateContext {
    plugin_bindings::exports::lix::plugin::api::DetectStateContext {
        active_state: context.active_state.map(|rows| {
            rows.into_iter()
                .map(wire_active_state_row_to_binding)
                .collect::<Vec<_>>()
        }),
    }
}

fn wire_active_state_row_to_binding(
    row: WireActiveStateRow,
) -> plugin_bindings::exports::lix::plugin::api::ActiveStateRow {
    plugin_bindings::exports::lix::plugin::api::ActiveStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        snapshot_content: row.snapshot_content,
        file_id: row.file_id,
        plugin_key: row.plugin_key,
        version_id: row.version_id,
        change_id: row.change_id,
        metadata: row.metadata,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn map_plugin_error(error: plugin_bindings::exports::lix::plugin::api::PluginError) -> LixError {
    match error {
        plugin_bindings::exports::lix::plugin::api::PluginError::InvalidInput(message) => {
            LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                title: "Unknown error".to_string(),
                description: format!("Plugin invalid-input error: {message}"),
            }
        }
        plugin_bindings::exports::lix::plugin::api::PluginError::Internal(message) => LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            title: "Unknown error".to_string(),
            description: format!("Plugin internal error: {message}"),
        },
    }
}
