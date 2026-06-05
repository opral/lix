use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use lix_engine::LixError;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{
    ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, p2::add_to_linker_sync,
};

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "../engine/wit",
        world: "plugin",
    });
}

pub(crate) fn runtime() -> Result<Arc<dyn WasmRuntime>, LixError> {
    Ok(Arc::new(WasmtimePluginRuntime::new()?))
}

fn create_engine(consume_fuel: bool, epoch_interruption: bool) -> wasmtime::Result<Engine> {
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.consume_fuel(consume_fuel);
    config.epoch_interruption(epoch_interruption);
    Engine::new(&config)
}

struct WasmtimePluginRuntime {
    engine: Engine,
    fuel_engine: Engine,
    timeout_engine: Engine,
    fuel_timeout_engine: Engine,
}

impl WasmtimePluginRuntime {
    fn new() -> Result<Self, LixError> {
        let engine = create_engine(false, false)
            .map_err(|error| wasm_runtime_error("failed to create Wasmtime engine", error))?;
        let fuel_engine = create_engine(true, false)
            .map_err(|error| wasm_runtime_error("failed to create Wasmtime fuel engine", error))?;
        let timeout_engine = create_engine(false, true).map_err(|error| {
            wasm_runtime_error("failed to create Wasmtime timeout engine", error)
        })?;
        let fuel_timeout_engine = create_engine(true, true).map_err(|error| {
            wasm_runtime_error("failed to create Wasmtime fuel timeout engine", error)
        })?;
        Ok(Self {
            engine,
            fuel_engine,
            timeout_engine,
            fuel_timeout_engine,
        })
    }
}

struct WasmtimePluginComponent {
    store: Mutex<Store<WasiHostState>>,
    bindings: plugin_bindings::Plugin,
    limits: WasmLimits,
    _timeout_ticker: Option<TimeoutTicker>,
}

struct WasiHostState {
    ctx: WasiCtx,
    table: ResourceTable,
}

impl WasiHostState {
    fn new() -> Self {
        Self {
            ctx: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
        }
    }
}

impl WasiView for WasiHostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

#[async_trait]
impl WasmRuntime for WasmtimePluginRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let engine = match (limits.max_fuel.is_some(), limits.timeout_ms.is_some()) {
            (false, false) => &self.engine,
            (true, false) => &self.fuel_engine,
            (false, true) => &self.timeout_engine,
            (true, true) => &self.fuel_timeout_engine,
        };
        let component = Component::new(engine, bytes)
            .map_err(|error| wasm_runtime_error("failed to compile plugin component", error))?;
        let mut linker = Linker::<WasiHostState>::new(engine);
        add_to_linker_sync(&mut linker)
            .map_err(|error| wasm_runtime_error("failed to configure WASI linker", error))?;
        let mut store = Store::new(engine, WasiHostState::new());
        if let Some(max_fuel) = limits.max_fuel {
            store
                .set_fuel(max_fuel)
                .map_err(|error| wasm_runtime_error("failed to configure WASM fuel", error))?;
        }
        if let Some(timeout_ms) = limits.timeout_ms {
            store.set_epoch_deadline(timeout_ms.max(1));
            store.epoch_deadline_trap();
        }
        let timeout_ticker = limits
            .timeout_ms
            .map(|_| TimeoutTicker::start(engine.clone()));
        let bindings = plugin_bindings::Plugin::instantiate(&mut store, &component, &linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin component", error))?;
        Ok(Arc::new(WasmtimePluginComponent {
            store: Mutex::new(store),
            bindings,
            limits,
            _timeout_ticker: timeout_ticker,
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for WasmtimePluginComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let mut store = self.store("detect-changes")?;
        self.reset_limits(&mut store)?;
        let state = state.into_iter().map(Into::into).collect::<Vec<_>>();
        let file = file.into();
        match self
            .bindings
            .lix_plugin_api()
            .call_detect_changes(&mut *store, &state, &file)
            .map_err(|error| wasm_runtime_error("failed to call detect-changes", error))?
        {
            Ok(changes) => Ok(changes.into_iter().map(Into::into).collect()),
            Err(error) => Err(plugin_error_from_binding("detect-changes", error)),
        }
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let mut store = self.store("render")?;
        self.reset_limits(&mut store)?;
        let state = state.into_iter().map(Into::into).collect::<Vec<_>>();
        match self
            .bindings
            .lix_plugin_api()
            .call_render(&mut *store, &state)
            .map_err(|error| wasm_runtime_error("failed to call render", error))?
        {
            Ok(bytes) => Ok(bytes),
            Err(error) => Err(plugin_error_from_binding("render", error)),
        }
    }
}

impl WasmtimePluginComponent {
    fn store(
        &self,
        export_name: &str,
    ) -> Result<std::sync::MutexGuard<'_, Store<WasiHostState>>, LixError> {
        self.store.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("Wasmtime store lock poisoned before calling {export_name}"),
            )
        })
    }

    fn reset_limits(&self, store: &mut Store<WasiHostState>) -> Result<(), LixError> {
        if let Some(max_fuel) = self.limits.max_fuel {
            store
                .set_fuel(max_fuel)
                .map_err(|error| wasm_runtime_error("failed to reset WASM fuel", error))?;
        }
        if let Some(timeout_ms) = self.limits.timeout_ms {
            store.set_epoch_deadline(timeout_ms.max(1));
        }
        Ok(())
    }
}

struct TimeoutTicker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TimeoutTicker {
    fn start(engine: Engine) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let handle = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
                engine.increment_epoch();
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for TimeoutTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl From<WasmPluginFile> for plugin_bindings::exports::lix::plugin::api::File {
    fn from(file: WasmPluginFile) -> Self {
        Self { data: file.data }
    }
}

impl From<WasmPluginEntityState> for plugin_bindings::exports::lix::plugin::api::EntityState {
    fn from(state: WasmPluginEntityState) -> Self {
        Self {
            entity_pk: state.entity_pk,
            schema_key: state.schema_key,
            snapshot_content: state.snapshot_content,
            metadata: state.metadata,
        }
    }
}

impl From<plugin_bindings::exports::lix::plugin::api::DetectedChange> for WasmPluginDetectedChange {
    fn from(change: plugin_bindings::exports::lix::plugin::api::DetectedChange) -> Self {
        Self {
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            snapshot_content: change.snapshot_content,
            metadata: change.metadata,
        }
    }
}

fn plugin_error_from_binding(
    export_name: &str,
    error: plugin_bindings::exports::lix::plugin::api::PluginError,
) -> LixError {
    let (kind, message) = match error {
        plugin_bindings::exports::lix::plugin::api::PluginError::InvalidInput(message) => {
            ("invalid-input", message)
        }
        plugin_bindings::exports::lix::plugin::api::PluginError::Internal(message) => {
            ("internal", message)
        }
    };
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{export_name} returned plugin error {kind}: {message}"),
    )
}

fn wasm_runtime_error(context: impl Into<String>, error: impl fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{}: {error}", context.into()),
    )
}
