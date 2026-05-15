#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{IoView, WasiCtx, WasiCtxBuilder, WasiView};

use lix_engine::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
use lix_engine::LixError;

wasmtime::component::bindgen!({
    path: "wit",
    world: "plugin",
});

#[derive(Debug, Clone)]
pub struct WasmtimeWasmRuntime {
    engine: Engine,
}

impl Default for WasmtimeWasmRuntime {
    fn default() -> Self {
        Self {
            engine: Engine::default(),
        }
    }
}

impl WasmtimeWasmRuntime {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl WasmRuntime for WasmtimeWasmRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        // Test-only reference runtime. Production embedders should enforce
        // memory/fuel/time limits in their runtime implementation.
        let component = Component::from_binary(&self.engine, &bytes).map_err(wasmtime_error)?;
        let mut linker = Linker::<PluginHostState>::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).map_err(wasmtime_error)?;
        let mut store = Store::new(&self.engine, PluginHostState::default());
        let bindings =
            Plugin::instantiate(&mut store, &component, &linker).map_err(wasmtime_error)?;
        Ok(Arc::new(WasmtimePluginInstance {
            inner: Mutex::new(WasmtimePluginInstanceInner { store, bindings }),
        }))
    }
}

struct WasmtimePluginInstance {
    inner: Mutex<WasmtimePluginInstanceInner>,
}

struct WasmtimePluginInstanceInner {
    store: Store<PluginHostState>,
    bindings: Plugin,
}

#[async_trait]
impl WasmComponentInstance for WasmtimePluginInstance {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let mut guard = self.inner.lock().map_err(|_| LixError {
            code: LixError::CODE_UNKNOWN.to_string(),
            message: "wasmtime plugin instance lock poisoned".to_string(),
            hint: None,
            details: None,
        })?;
        match export {
            "detect-changes" | "api#detect-changes" => {
                let input: DetectChangesInput =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        code: LixError::CODE_UNKNOWN.to_string(),
                        message: format!("detect-changes input must be JSON: {error}"),
                        hint: None,
                        details: None,
                    })?;
                let output = guard.call_detect_changes(input)?;
                serde_json::to_vec(&output).map_err(|error| LixError {
                    code: LixError::CODE_UNKNOWN.to_string(),
                    message: format!("detect-changes output serialization failed: {error}"),
                    hint: None,
                    details: None,
                })
            }
            "apply-changes" | "api#apply-changes" => {
                let input: ApplyChangesInput =
                    serde_json::from_slice(input).map_err(|error| LixError {
                        code: LixError::CODE_UNKNOWN.to_string(),
                        message: format!("apply-changes input must be JSON: {error}"),
                        hint: None,
                        details: None,
                    })?;
                let output = guard.call_apply_changes(input)?;
                serde_json::to_vec(&output).map_err(|error| LixError {
                    code: LixError::CODE_UNKNOWN.to_string(),
                    message: format!("apply-changes output serialization failed: {error}"),
                    hint: None,
                    details: None,
                })
            }
            _ => Err(LixError {
                code: LixError::CODE_UNKNOWN.to_string(),
                message: format!("unknown plugin export '{export}'"),
                hint: None,
                details: None,
            }),
        }
    }
}

impl WasmtimePluginInstanceInner {
    fn call_detect_changes(
        &mut self,
        input: DetectChangesInput,
    ) -> Result<Vec<EntityChangeOutput>, LixError> {
        let before = input.before.map(component_file_from_json);
        let after = component_file_from_json(input.after);
        let state_context = input.state_context.map(component_state_context_from_json);
        let result = self
            .bindings
            .lix_plugin_api()
            .call_detect_changes(
                &mut self.store,
                before.as_ref(),
                &after,
                state_context.as_ref(),
            )
            .map_err(wasmtime_error)?;
        match result {
            Ok(changes) => Ok(changes.into_iter().map(entity_change_to_json).collect()),
            Err(error) => Err(plugin_error(error)),
        }
    }

    fn call_apply_changes(&mut self, input: ApplyChangesInput) -> Result<Vec<u8>, LixError> {
        let file = component_file_from_json(input.file);
        let changes = input
            .changes
            .into_iter()
            .map(component_entity_change_from_json)
            .collect::<Vec<_>>();
        let result = self
            .bindings
            .lix_plugin_api()
            .call_apply_changes(&mut self.store, &file, &changes)
            .map_err(wasmtime_error)?;
        result.map_err(plugin_error)
    }
}

struct PluginHostState {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl Default for PluginHostState {
    fn default() -> Self {
        Self {
            ctx: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
        }
    }
}

impl IoView for PluginHostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for PluginHostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DetectChangesInput {
    before: Option<FileInput>,
    after: FileInput,
    #[serde(default)]
    state_context: Option<DetectStateContextInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApplyChangesInput {
    file: FileInput,
    changes: Vec<EntityChangeOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileInput {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DetectStateContextInput {
    #[serde(default)]
    active_state: Option<Vec<ActiveStateRowInput>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveStateRowInput {
    entity_id: String,
    schema_key: Option<String>,
    snapshot_content: Option<String>,
    file_id: Option<String>,
    plugin_key: Option<String>,
    version_id: Option<String>,
    change_id: Option<String>,
    metadata: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EntityChangeOutput {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) snapshot_content: Option<String>,
}

fn component_file_from_json(file: FileInput) -> exports::lix::plugin::api::File {
    exports::lix::plugin::api::File {
        id: file.id,
        path: file.path,
        data: file.data,
    }
}

fn component_state_context_from_json(
    state_context: DetectStateContextInput,
) -> exports::lix::plugin::api::DetectStateContext {
    exports::lix::plugin::api::DetectStateContext {
        active_state: state_context.active_state.map(|rows| {
            rows.into_iter()
                .map(|row| exports::lix::plugin::api::ActiveStateRow {
                    entity_id: row.entity_id,
                    schema_key: row.schema_key,
                    snapshot_content: row.snapshot_content,
                    file_id: row.file_id,
                    plugin_key: row.plugin_key,
                    version_id: row.version_id,
                    change_id: row.change_id,
                    metadata: row.metadata,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                })
                .collect()
        }),
    }
}

fn component_entity_change_from_json(
    change: EntityChangeOutput,
) -> exports::lix::plugin::api::EntityChange {
    exports::lix::plugin::api::EntityChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        snapshot_content: change.snapshot_content,
    }
}

fn entity_change_to_json(change: exports::lix::plugin::api::EntityChange) -> EntityChangeOutput {
    EntityChangeOutput {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        snapshot_content: change.snapshot_content,
    }
}

fn wasmtime_error(error: impl std::fmt::Display) -> LixError {
    LixError {
        code: LixError::CODE_UNKNOWN.to_string(),
        message: format!("wasmtime plugin runtime error: {error}"),
        hint: None,
        details: None,
    }
}

fn plugin_error(error: exports::lix::plugin::api::PluginError) -> LixError {
    let message = match error {
        exports::lix::plugin::api::PluginError::InvalidInput(message) => {
            format!("plugin invalid input: {message}")
        }
        exports::lix::plugin::api::PluginError::Internal(message) => {
            format!("plugin internal error: {message}")
        }
    };
    LixError {
        code: LixError::CODE_UNKNOWN.to_string(),
        message,
        hint: None,
        details: None,
    }
}
