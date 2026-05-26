use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Cursor, Read};
use std::sync::{Arc, Mutex};

use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{IoView, WasiCtx, WasiCtxBuilder, WasiView};
use zip::read::ZipArchive;

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}

pub use plugin_bindings::exports::lix::plugin::api::{
    EntityChange as PluginEntityChange, File as PluginFile, PluginError,
};

pub struct JsonPluginRuntime {
    engine: Engine,
    component_cache: Mutex<HashMap<ComponentCacheKey, Arc<Component>>>,
}

impl JsonPluginRuntime {
    pub fn new() -> Result<Self, String> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);
        config.consume_fuel(true);

        let engine = Engine::new(&config)
            .map_err(|error| format!("failed to initialize wasmtime engine: {error}"))?;

        Ok(Self {
            engine,
            component_cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn detect_changes(
        &self,
        plugin_archive: &[u8],
        before: Option<PluginFile>,
        after: PluginFile,
    ) -> Result<Vec<PluginEntityChange>, String> {
        let wasm_bytes = plugin_wasm_bytes_from_archive(plugin_archive)?;
        let component = self.component_for_bytes(&wasm_bytes)?;
        let mut store = Store::new(
            &self.engine,
            WasiState {
                table: ResourceTable::new(),
                ctx: WasiCtxBuilder::new().build(),
            },
        );
        store
            .set_fuel(u64::MAX)
            .map_err(|error| format!("failed to configure wasm fuel: {error}"))?;

        let mut linker = Linker::new(&self.engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
            .map_err(|error| format!("failed to add wasi imports to linker: {error}"))?;

        let bindings =
            plugin_bindings::Plugin::instantiate(&mut store, component.as_ref(), &linker)
                .map_err(|error| format!("failed to instantiate wasm component: {error}"))?;

        bindings
            .lix_plugin_api()
            .call_detect_changes(&mut store, before.as_ref(), &after, None)
            .map_err(|error| format!("wasm call failed for detect-changes: {error}"))?
            .map_err(plugin_error_message)
    }

    fn component_for_bytes(&self, bytes: &[u8]) -> Result<Arc<Component>, String> {
        let cache_key = ComponentCacheKey::from_bytes(bytes);

        if let Some(component) = self
            .component_cache
            .lock()
            .map_err(|_| "component cache lock poisoned".to_string())?
            .get(&cache_key)
            .cloned()
        {
            return Ok(component);
        }

        let compiled = Arc::new(
            Component::new(&self.engine, bytes)
                .map_err(|error| format!("failed to compile wasm component: {error}"))?,
        );

        let mut cache = self
            .component_cache
            .lock()
            .map_err(|_| "component cache lock poisoned".to_string())?;
        Ok(cache.entry(cache_key).or_insert_with(|| compiled).clone())
    }
}

fn plugin_wasm_bytes_from_archive(plugin_archive: &[u8]) -> Result<Vec<u8>, String> {
    let mut archive = ZipArchive::new(Cursor::new(plugin_archive))
        .map_err(|error| format!("plugin archive is not a valid zip file: {error}"))?;
    let mut entry = archive
        .by_name("plugin.wasm")
        .map_err(|error| format!("plugin archive is missing plugin.wasm: {error}"))?;
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read plugin.wasm from archive: {error}"))?;
    Ok(bytes)
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

fn wasm_fingerprint(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn plugin_error_message(error: PluginError) -> String {
    match error {
        PluginError::InvalidInput(message) => format!("plugin invalid-input error: {message}"),
        PluginError::Internal(message) => format!("plugin internal error: {message}"),
    }
}
