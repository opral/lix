use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use lix_engine::LixError;
use lix_engine::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
use serde::{Deserialize, Serialize};
use wasmtime::component::types::ComponentItem;
use wasmtime::component::{Component, ComponentExportIndex, Instance, Linker, Val};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{IoView, ResourceTable, WasiCtx, WasiCtxBuilder, WasiView};

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
    instance: Instance,
    exports: WasmtimePluginExports,
    limits: WasmLimits,
    _timeout_ticker: Option<TimeoutTicker>,
}

#[derive(Clone, Copy)]
struct WasmtimePluginExports {
    detect_changes: ComponentExportIndex,
    render: ComponentExportIndex,
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

impl IoView for WasiHostState {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiView for WasiHostState {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
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
        let exports = WasmtimePluginExports::from_component(engine, &component)?;
        let mut linker = Linker::<WasiHostState>::new(engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker)
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
        let instance = linker
            .instantiate(&mut store, &component)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin component", error))?;
        Ok(Arc::new(WasmtimePluginComponent {
            store: Mutex::new(store),
            instance,
            exports,
            limits,
            _timeout_ticker: timeout_ticker,
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for WasmtimePluginComponent {
    async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
        match export {
            "detect-changes" | "api#detect-changes" => self.detect_changes(input),
            "render" | "api#render" => self.render(input),
            other => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("Wasmtime runtime does not implement export '{other}'"),
            )),
        }
    }
}

impl WasmtimePluginExports {
    fn from_component(engine: &Engine, component: &Component) -> Result<Self, LixError> {
        Ok(Self {
            detect_changes: find_plugin_func_export(engine, component, "detect-changes")?,
            render: find_plugin_func_export(engine, component, "render")?,
        })
    }
}

impl WasmtimePluginComponent {
    fn detect_changes(&self, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let payload: PluginDetectChangesPayload =
            serde_json::from_slice(input).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("plugin detect-changes payload is invalid JSON: {error}"),
                )
            })?;
        let params = [
            entity_state_list_to_val(payload.state),
            Val::Record(vec![("data".to_string(), bytes_to_val(payload.file.data))]),
        ];
        let result =
            self.call_component_func(self.exports.detect_changes, &params, "detect-changes")?;
        let changes = expect_detected_changes_result(result, "detect-changes")?;
        serde_json::to_vec(&changes).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to encode plugin detect-changes output: {error}"),
            )
        })
    }

    fn render(&self, input: &[u8]) -> Result<Vec<u8>, LixError> {
        let payload: PluginRenderPayload = serde_json::from_slice(input).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("plugin render payload is invalid JSON: {error}"),
            )
        })?;
        let params = [entity_state_list_to_val(payload.state)];
        let result = self.call_component_func(self.exports.render, &params, "render")?;
        expect_render_result(result, "render")
    }

    fn call_component_func(
        &self,
        export: ComponentExportIndex,
        params: &[Val],
        export_name: &str,
    ) -> Result<Val, LixError> {
        let mut store = self.store.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "Wasmtime store lock poisoned",
            )
        })?;
        let func = self.instance.get_func(&mut *store, export).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("plugin component export '{export_name}' is not a function"),
            )
        })?;
        if let Some(max_fuel) = self.limits.max_fuel {
            store
                .set_fuel(max_fuel)
                .map_err(|error| wasm_runtime_error("failed to reset WASM fuel", error))?;
        }
        if let Some(timeout_ms) = self.limits.timeout_ms {
            store.set_epoch_deadline(timeout_ms.max(1));
        }
        let mut results = [Val::Result(Ok(None))];
        func.call(&mut *store, params, &mut results)
            .map_err(|error| wasm_runtime_error(format!("failed to call {export_name}"), error))?;
        func.post_return(&mut *store).map_err(|error| {
            wasm_runtime_error(format!("failed to finish {export_name} call"), error)
        })?;
        Ok(results.into_iter().next().unwrap())
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectChangesPayload {
    state: Vec<PluginEntityStatePayload>,
    file: PluginFilePayload,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginRenderPayload {
    state: Vec<PluginEntityStatePayload>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginFilePayload {
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct PluginEntityStatePayload {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: String,
    metadata: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
struct PluginDetectedChangePayload {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<String>,
    metadata: Option<String>,
}

fn find_plugin_func_export(
    engine: &Engine,
    component: &Component,
    func_name: &str,
) -> Result<ComponentExportIndex, LixError> {
    if let Some((ComponentItem::ComponentFunc(_), export)) = component.export_index(None, func_name)
    {
        return Ok(export);
    }

    let component_type = component.component_type();
    for (instance_name, item) in component_type.exports(engine) {
        if !matches!(item, ComponentItem::ComponentInstance(_)) {
            continue;
        }
        let Some((ComponentItem::ComponentInstance(_), instance_export)) =
            component.export_index(None, instance_name)
        else {
            continue;
        };
        if let Some((ComponentItem::ComponentFunc(_), export)) =
            component.export_index(Some(&instance_export), func_name)
        {
            return Ok(export);
        }
    }

    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "plugin component is missing export '{func_name}'. Available exports: {}",
            component_exports_summary(engine, component)
        ),
    ))
}

fn component_exports_summary(engine: &Engine, component: &Component) -> String {
    let component_type = component.component_type();
    let mut exports = Vec::new();
    for (name, item) in component_type.exports(engine) {
        match item {
            ComponentItem::ComponentInstance(instance) => {
                let nested = instance
                    .exports(engine)
                    .map(|(nested_name, _)| nested_name.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                exports.push(format!("{name}({nested})"));
            }
            _ => exports.push(name.to_string()),
        }
    }
    exports.join(", ")
}

fn entity_state_list_to_val(state: Vec<PluginEntityStatePayload>) -> Val {
    Val::List(state.into_iter().map(entity_state_to_val).collect())
}

fn entity_state_to_val(state: PluginEntityStatePayload) -> Val {
    Val::Record(vec![
        ("entity-pk".to_string(), string_list_to_val(state.entity_pk)),
        ("schema-key".to_string(), Val::String(state.schema_key)),
        (
            "snapshot-content".to_string(),
            Val::String(state.snapshot_content),
        ),
        (
            "metadata".to_string(),
            optional_string_to_val(state.metadata),
        ),
    ])
}

fn string_list_to_val(values: Vec<String>) -> Val {
    Val::List(values.into_iter().map(Val::String).collect())
}

fn bytes_to_val(bytes: Vec<u8>) -> Val {
    Val::List(bytes.into_iter().map(Val::U8).collect())
}

fn optional_string_to_val(value: Option<String>) -> Val {
    Val::Option(value.map(|value| Box::new(Val::String(value))))
}

fn expect_detected_changes_result(
    result: Val,
    export_name: &str,
) -> Result<Vec<PluginDetectedChangePayload>, LixError> {
    let output = expect_plugin_ok_result(result, export_name)?;
    let Val::List(values) = output else {
        return Err(plugin_abi_error(format!(
            "{export_name} returned {}, expected list",
            val_type_name(&output)
        )));
    };
    values.into_iter().map(detected_change_from_val).collect()
}

fn expect_render_result(result: Val, export_name: &str) -> Result<Vec<u8>, LixError> {
    let output = expect_plugin_ok_result(result, export_name)?;
    expect_u8_list(output, export_name)
}

fn expect_plugin_ok_result(result: Val, export_name: &str) -> Result<Val, LixError> {
    match result {
        Val::Result(Ok(Some(output))) => Ok(*output),
        Val::Result(Ok(None)) => Err(plugin_abi_error(format!(
            "{export_name} returned ok without a payload"
        ))),
        Val::Result(Err(error)) => Err(plugin_error_from_val(export_name, error.map(|v| *v))),
        other => Err(plugin_abi_error(format!(
            "{export_name} returned {}, expected result",
            val_type_name(&other)
        ))),
    }
}

fn detected_change_from_val(value: Val) -> Result<PluginDetectedChangePayload, LixError> {
    let Val::Record(fields) = value else {
        return Err(plugin_abi_error(format!(
            "detect-changes item was {}, expected record",
            val_type_name(&value)
        )));
    };
    let mut fields = fields.into_iter();
    let entity_pk = expect_string_list(
        expect_next_field(&mut fields, "entity-pk", "detected-change")?,
        "detected-change.entity-pk",
    )?;
    let schema_key = expect_string(
        expect_next_field(&mut fields, "schema-key", "detected-change")?,
        "detected-change.schema-key",
    )?;
    let snapshot_content = expect_optional_string(
        expect_next_field(&mut fields, "snapshot-content", "detected-change")?,
        "detected-change.snapshot-content",
    )?;
    let metadata = expect_optional_string(
        expect_next_field(&mut fields, "metadata", "detected-change")?,
        "detected-change.metadata",
    )?;
    if let Some((field, _)) = fields.next() {
        return Err(plugin_abi_error(format!(
            "detected-change returned unexpected field '{field}'"
        )));
    }
    Ok(PluginDetectedChangePayload {
        entity_pk,
        schema_key,
        snapshot_content,
        metadata,
    })
}

fn expect_next_field(
    fields: &mut impl Iterator<Item = (String, Val)>,
    expected: &str,
    label: &str,
) -> Result<Val, LixError> {
    let Some((field, value)) = fields.next() else {
        return Err(plugin_abi_error(format!(
            "{label} is missing field '{expected}'"
        )));
    };
    if field != expected {
        return Err(plugin_abi_error(format!(
            "{label} returned field '{field}', expected '{expected}'"
        )));
    }
    Ok(value)
}

fn expect_string_list(value: Val, label: &str) -> Result<Vec<String>, LixError> {
    let Val::List(values) = value else {
        return Err(plugin_abi_error(format!(
            "{label} was {}, expected list<string>",
            val_type_name(&value)
        )));
    };
    values
        .into_iter()
        .map(|value| expect_string(value, label))
        .collect()
}

fn expect_u8_list(value: Val, label: &str) -> Result<Vec<u8>, LixError> {
    let Val::List(values) = value else {
        return Err(plugin_abi_error(format!(
            "{label} was {}, expected list<u8>",
            val_type_name(&value)
        )));
    };
    values
        .into_iter()
        .map(|value| match value {
            Val::U8(value) => Ok(value),
            other => Err(plugin_abi_error(format!(
                "{label} list item was {}, expected u8",
                val_type_name(&other)
            ))),
        })
        .collect()
}

fn expect_string(value: Val, label: &str) -> Result<String, LixError> {
    match value {
        Val::String(value) => Ok(value),
        other => Err(plugin_abi_error(format!(
            "{label} was {}, expected string",
            val_type_name(&other)
        ))),
    }
}

fn expect_optional_string(value: Val, label: &str) -> Result<Option<String>, LixError> {
    match value {
        Val::Option(None) => Ok(None),
        Val::Option(Some(value)) => expect_string(*value, label).map(Some),
        other => Err(plugin_abi_error(format!(
            "{label} was {}, expected option<string>",
            val_type_name(&other)
        ))),
    }
}

fn plugin_error_from_val(export_name: &str, value: Option<Val>) -> LixError {
    let message = match value {
        Some(Val::Variant(kind, Some(payload))) => match *payload {
            Val::String(message) => {
                format!("{export_name} returned plugin error {kind}: {message}")
            }
            other => format!(
                "{export_name} returned plugin error {kind} with {} payload",
                val_type_name(&other)
            ),
        },
        Some(Val::Variant(kind, None)) => {
            format!("{export_name} returned plugin error {kind} without payload")
        }
        Some(other) => format!(
            "{export_name} returned malformed plugin error {}",
            val_type_name(&other)
        ),
        None => format!("{export_name} returned plugin error without payload"),
    };
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn val_type_name(value: &Val) -> &'static str {
    match value {
        Val::Bool(_) => "bool",
        Val::S8(_) => "s8",
        Val::U8(_) => "u8",
        Val::S16(_) => "s16",
        Val::U16(_) => "u16",
        Val::S32(_) => "s32",
        Val::U32(_) => "u32",
        Val::S64(_) => "s64",
        Val::U64(_) => "u64",
        Val::Float32(_) => "float32",
        Val::Float64(_) => "float64",
        Val::Char(_) => "char",
        Val::String(_) => "string",
        Val::List(_) => "list",
        Val::Record(_) => "record",
        Val::Tuple(_) => "tuple",
        Val::Variant(_, _) => "variant",
        Val::Enum(_) => "enum",
        Val::Option(_) => "option",
        Val::Result(_) => "result",
        Val::Flags(_) => "flags",
        Val::Resource(_) => "resource",
    }
}

fn plugin_abi_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

fn wasm_runtime_error(context: impl Into<String>, error: impl fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{}: {error}", context.into()),
    )
}
