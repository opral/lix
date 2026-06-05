use std::collections::HashMap;
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
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "../engine/wit",
        world: "plugin",
    });
}

type BindingSnapshotContent = HashMap<String, plugin_bindings::exports::lix::plugin::api::Scalar>;

pub(crate) fn runtime() -> Result<Arc<dyn WasmRuntime>, LixError> {
    Ok(Arc::new(WasmtimePluginRuntime::new()?))
}

fn create_engine(consume_fuel: bool, epoch_interruption: bool) -> wasmtime::Result<Engine> {
    let mut config = Config::new();
    config.wasm_component_model(true);
    config.wasm_component_model_map(true);
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
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
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
        let state = state
            .into_iter()
            .map(binding_entity_state_from_wasm)
            .collect::<Result<Vec<_>, _>>()?;
        let file = file.into();
        match self
            .bindings
            .lix_plugin_api()
            .call_detect_changes(&mut *store, &state, &file)
            .map_err(|error| wasm_runtime_error("failed to call detect-changes", error))?
        {
            Ok(changes) => changes
                .into_iter()
                .map(wasm_detected_change_from_binding)
                .collect(),
            Err(error) => Err(plugin_error_from_binding("detect-changes", error)),
        }
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let mut store = self.store("render")?;
        self.reset_limits(&mut store)?;
        let state = state
            .into_iter()
            .map(binding_entity_state_from_wasm)
            .collect::<Result<Vec<_>, _>>()?;
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

fn binding_entity_state_from_wasm(
    state: WasmPluginEntityState,
) -> Result<plugin_bindings::exports::lix::plugin::api::EntityState, LixError> {
    Ok(plugin_bindings::exports::lix::plugin::api::EntityState {
        entity_pk: state.entity_pk,
        schema_key: state.schema_key,
        snapshot_content: snapshot_content_from_json(
            &state.snapshot_content,
            "plugin state snapshot_content",
        )?,
        metadata: state.metadata,
    })
}

fn wasm_detected_change_from_binding(
    change: plugin_bindings::exports::lix::plugin::api::DetectedChange,
) -> Result<WasmPluginDetectedChange, LixError> {
    Ok(WasmPluginDetectedChange {
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        snapshot_content: change
            .snapshot_content
            .as_ref()
            .map(|snapshot_content| {
                snapshot_content_to_json(snapshot_content, "plugin emitted snapshot_content")
            })
            .transpose()?,
        metadata: change.metadata,
    })
}

fn snapshot_content_from_json(raw: &str, label: &str) -> Result<BindingSnapshotContent, LixError> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{label} is invalid JSON: {error}"),
        )
    })?;
    let serde_json::Value::Object(object) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("{label} must be a JSON object"),
        ));
    };

    object
        .into_iter()
        .map(|(key, value)| Ok((key, scalar_from_json_value(value)?)))
        .collect()
}

fn snapshot_content_to_json(
    snapshot_content: &BindingSnapshotContent,
    label: &str,
) -> Result<String, LixError> {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| Ok((key.clone(), json_value_from_scalar(value, label)?)))
        .collect::<Result<serde_json::Map<_, _>, LixError>>()?;
    serde_json::to_string(&serde_json::Value::Object(object)).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode {label} JSON: {error}"),
        )
    })
}

fn scalar_from_json_value(
    value: serde_json::Value,
) -> Result<plugin_bindings::exports::lix::plugin::api::Scalar, LixError> {
    match value {
        serde_json::Value::Null => Ok(plugin_bindings::exports::lix::plugin::api::Scalar::Nil),
        serde_json::Value::Bool(value) => Ok(
            plugin_bindings::exports::lix::plugin::api::Scalar::Boolean(value),
        ),
        serde_json::Value::String(value) => Ok(
            plugin_bindings::exports::lix::plugin::api::Scalar::Text(value),
        ),
        serde_json::Value::Number(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => serde_json::to_string(&value)
            .map(plugin_bindings::exports::lix::plugin::api::Scalar::Json)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("failed to encode snapshot scalar JSON: {error}"),
                )
            }),
    }
}

fn json_value_from_scalar(
    value: &plugin_bindings::exports::lix::plugin::api::Scalar,
    label: &str,
) -> Result<serde_json::Value, LixError> {
    match value {
        plugin_bindings::exports::lix::plugin::api::Scalar::Nil => Ok(serde_json::Value::Null),
        plugin_bindings::exports::lix::plugin::api::Scalar::Boolean(value) => {
            Ok(serde_json::Value::Bool(*value))
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Number(value) => {
            serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("{label} contains NaN or infinite number"),
                    )
                })
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Text(value) => {
            Ok(serde_json::Value::String(value.clone()))
        }
        plugin_bindings::exports::lix::plugin::api::Scalar::Json(value) => {
            serde_json::from_str(value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("{label} contains invalid JSON scalar: {error}"),
                )
            })
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
