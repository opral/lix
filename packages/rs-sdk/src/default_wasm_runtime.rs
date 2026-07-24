use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use lix_engine::LixError;
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lru::LruCache;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, ResourceLimiter, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{
    ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, p2::add_to_linker_sync,
};

mod plugin_bindings {
    wasmtime::component::bindgen!({
        path: "../engine/wit",
        world: "plugin",
    });
}

#[path = "default_wasm_runtime_v2.rs"]
mod v2_runtime;

const COMPILED_COMPONENT_CACHE_CAPACITY: usize = 16;
const MAX_PLUGIN_INSTANCES: usize = 64;
const MAX_PLUGIN_MEMORIES: usize = 1;
const MAX_PLUGIN_TABLES: usize = 8;
const MAX_PLUGIN_TABLE_ELEMENTS: usize = 1_000_000;

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
    shared: Arc<WasmtimeSharedRuntime>,
}

struct WasmtimeSharedRuntime {
    engine: Engine,
    fuel_engine: Engine,
    timeout_engine: Engine,
    fuel_timeout_engine: Engine,
    linker: OnceLock<Result<Linker<WasiHostState>, LixError>>,
    fuel_linker: OnceLock<Result<Linker<WasiHostState>, LixError>>,
    timeout_linker: OnceLock<Result<Linker<WasiHostState>, LixError>>,
    fuel_timeout_linker: OnceLock<Result<Linker<WasiHostState>, LixError>>,
    timeout_ticker: Arc<TimeoutTickerRegistry>,
    fuel_timeout_ticker: Arc<TimeoutTickerRegistry>,
    compiled_components: CompiledComponentCache,
}

impl WasmtimePluginRuntime {
    fn new() -> Result<Self, LixError> {
        static SHARED: OnceLock<Result<Arc<WasmtimeSharedRuntime>, LixError>> = OnceLock::new();
        let shared = SHARED
            .get_or_init(|| WasmtimeSharedRuntime::new().map(Arc::new))
            .clone()?;
        Ok(Self { shared })
    }
}

impl WasmtimeSharedRuntime {
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
        let timeout_ticker = Arc::new(TimeoutTickerRegistry::new(timeout_engine.clone()));
        let fuel_timeout_ticker = Arc::new(TimeoutTickerRegistry::new(fuel_timeout_engine.clone()));
        Ok(Self {
            engine,
            fuel_engine,
            timeout_engine,
            fuel_timeout_engine,
            linker: OnceLock::new(),
            fuel_linker: OnceLock::new(),
            timeout_linker: OnceLock::new(),
            fuel_timeout_linker: OnceLock::new(),
            timeout_ticker,
            fuel_timeout_ticker,
            compiled_components: CompiledComponentCache::new(COMPILED_COMPONENT_CACHE_CAPACITY),
        })
    }

    fn engine(&self, profile: CompileProfile) -> &Engine {
        match profile {
            CompileProfile::Plain => &self.engine,
            CompileProfile::Fuel => &self.fuel_engine,
            CompileProfile::Timeout => &self.timeout_engine,
            CompileProfile::FuelAndTimeout => &self.fuel_timeout_engine,
        }
    }

    fn linker(&self, profile: CompileProfile) -> Result<&Linker<WasiHostState>, LixError> {
        let linker = match profile {
            CompileProfile::Plain => &self.linker,
            CompileProfile::Fuel => &self.fuel_linker,
            CompileProfile::Timeout => &self.timeout_linker,
            CompileProfile::FuelAndTimeout => &self.fuel_timeout_linker,
        };
        linker
            .get_or_init(|| create_linker(self.engine(profile)))
            .as_ref()
            .map_err(Clone::clone)
    }

    fn timeout_ticker(
        &self,
        profile: CompileProfile,
    ) -> Result<Option<TimeoutTickerLease>, LixError> {
        let registry = match profile {
            CompileProfile::Plain | CompileProfile::Fuel => return Ok(None),
            CompileProfile::Timeout => &self.timeout_ticker,
            CompileProfile::FuelAndTimeout => &self.fuel_timeout_ticker,
        };
        registry.acquire().map(Some)
    }
}

fn create_linker(engine: &Engine) -> Result<Linker<WasiHostState>, LixError> {
    let mut linker = Linker::<WasiHostState>::new(engine);
    add_to_linker_sync(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure WASI linker", error))?;
    Ok(linker)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CompileProfile {
    Plain,
    Fuel,
    Timeout,
    FuelAndTimeout,
}

impl CompileProfile {
    fn from_limits(limits: WasmLimits) -> Self {
        match (limits.max_fuel.is_some(), limits.timeout_ms.is_some()) {
            (false, false) => Self::Plain,
            (true, false) => Self::Fuel,
            (false, true) => Self::Timeout,
            (true, true) => Self::FuelAndTimeout,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CompiledComponentKey {
    wasm_hash: [u8; 32],
    profile: CompileProfile,
}

impl CompiledComponentKey {
    fn new(profile: CompileProfile, bytes: &[u8]) -> Self {
        Self {
            wasm_hash: *blake3::hash(bytes).as_bytes(),
            profile,
        }
    }
}

type InFlightCompilation = Arc<tokio::sync::OnceCell<Result<Component, LixError>>>;

struct CompiledComponentCacheState {
    ready: LruCache<CompiledComponentKey, Component>,
    in_flight: HashMap<CompiledComponentKey, InFlightCompilation>,
}

struct CompiledComponentCache {
    state: Mutex<CompiledComponentCacheState>,
}

impl CompiledComponentCache {
    fn new(capacity: usize) -> Self {
        let capacity =
            NonZeroUsize::new(capacity).expect("component cache capacity must be nonzero");
        Self {
            state: Mutex::new(CompiledComponentCacheState {
                ready: LruCache::new(capacity),
                in_flight: HashMap::new(),
            }),
        }
    }

    async fn get_or_compile(
        &self,
        key: CompiledComponentKey,
        compile: impl FnOnce() -> Result<Component, LixError>,
    ) -> Result<Component, LixError> {
        let in_flight = {
            let mut state = self.lock()?;
            if let Some(component) = state.ready.get(&key) {
                return Ok(component.clone());
            }
            state
                .in_flight
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };

        // Component::new is synchronous, but same-key waiters yield instead of
        // occupying additional executor threads while the initializer runs.
        let result = in_flight.get_or_init(|| async { compile() }).await.clone();
        let mut state = self.lock()?;
        let owns_entry = state
            .in_flight
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &in_flight));
        if owns_entry {
            state.in_flight.remove(&key);
            if let Ok(component) = &result {
                state.ready.put(key, component.clone());
            }
        }
        result
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, CompiledComponentCacheState>, LixError> {
        self.state.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "compiled WASM component cache lock poisoned",
            )
        })
    }
}

struct WasmtimePluginComponent {
    store: Mutex<Store<WasiHostState>>,
    bindings: plugin_bindings::Plugin,
    limits: WasmLimits,
    _timeout_ticker: Option<TimeoutTickerLease>,
}

struct WasiHostState {
    ctx: WasiCtx,
    table: ResourceTable,
    limits: TrackingStoreLimits,
}

impl WasiHostState {
    fn new(limits: StoreLimits) -> Self {
        Self {
            ctx: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
            limits: TrackingStoreLimits::new(limits),
        }
    }
}

/// Delegates Wasmtime's hard resource limits while recording the one guest
/// linear memory's actual high-water mark. `MAX_PLUGIN_MEMORIES` is one, and
/// Wasm linear memories do not shrink, so the latest successful desired size
/// is also the exact lifetime high-water size. A failed grow restores the
/// pre-request sample before delegating Wasmtime's failure policy.
struct TrackingStoreLimits {
    inner: StoreLimits,
    current_linear_memory_bytes: usize,
    linear_memory_high_water_bytes: usize,
    pending_memory_growth: Option<(usize, usize)>,
}

impl TrackingStoreLimits {
    fn new(inner: StoreLimits) -> Self {
        Self {
            inner,
            current_linear_memory_bytes: 0,
            linear_memory_high_water_bytes: 0,
            pending_memory_growth: None,
        }
    }

    fn linear_memory_high_water_bytes(&self) -> u64 {
        u64::try_from(self.linear_memory_high_water_bytes).unwrap_or(u64::MAX)
    }
}

impl ResourceLimiter for TrackingStoreLimits {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // `current` is authoritative even if a guest previously handled a
        // failed `memory.grow` and continued executing.
        self.current_linear_memory_bytes = current;
        self.linear_memory_high_water_bytes = self.linear_memory_high_water_bytes.max(current);
        self.pending_memory_growth = None;

        let allowed = self.inner.memory_growing(current, desired, maximum)?;
        if allowed {
            self.pending_memory_growth = Some((
                self.current_linear_memory_bytes,
                self.linear_memory_high_water_bytes,
            ));
            // Wasmtime invokes `memory_grow_failed` synchronously if the
            // permitted allocation does not actually succeed.
            self.current_linear_memory_bytes = desired;
            self.linear_memory_high_water_bytes = self.linear_memory_high_water_bytes.max(desired);
        }
        Ok(allowed)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        if let Some((current, high_water)) = self.pending_memory_growth.take() {
            self.current_linear_memory_bytes = current;
            self.linear_memory_high_water_bytes = high_water;
        }
        self.inner.memory_grow_failed(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.inner.table_growing(current, desired, maximum)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.inner.table_grow_failed(error)
    }

    fn instances(&self) -> usize {
        self.inner.instances()
    }

    fn tables(&self) -> usize {
        self.inner.tables()
    }

    fn memories(&self) -> usize {
        self.inner.memories()
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
        let profile = CompileProfile::from_limits(limits);
        let engine = self.shared.engine(profile);
        let key = CompiledComponentKey::new(profile, &bytes);
        let component = self
            .shared
            .compiled_components
            .get_or_compile(key, || {
                Component::new(engine, &bytes).map_err(|error| {
                    wasm_runtime_error("failed to compile plugin component", error)
                })
            })
            .await?;
        let linker = self.shared.linker(profile)?;
        let timeout_ticker = self.shared.timeout_ticker(profile)?;
        // Set an epoch deadline only after hashing/compilation. Once another
        // component has started the shared ticker, cold preparation must not
        // consume this component's execution budget before instantiation.
        let mut store = create_store(engine, limits)?;
        let bindings = plugin_bindings::Plugin::instantiate(&mut store, &component, linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin component", error))?;
        Ok(Arc::new(WasmtimePluginComponent {
            store: Mutex::new(store),
            bindings,
            limits,
            _timeout_ticker: timeout_ticker,
        }))
    }

    async fn compile_component_v2(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn lix_engine::wasm::v2::WasmComponentV2Factory>, LixError> {
        v2_runtime::compile_component(self, bytes, limits).await
    }
}

fn create_store(engine: &Engine, limits: WasmLimits) -> Result<Store<WasiHostState>, LixError> {
    let max_memory_bytes = usize::try_from(limits.max_memory_bytes).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "WASM memory limit {} does not fit this host's address space",
                limits.max_memory_bytes
            ),
        )
    })?;
    let store_limits = StoreLimitsBuilder::new()
        .memory_size(max_memory_bytes)
        .instances(MAX_PLUGIN_INSTANCES)
        .memories(MAX_PLUGIN_MEMORIES)
        .tables(MAX_PLUGIN_TABLES)
        .table_elements(MAX_PLUGIN_TABLE_ELEMENTS)
        .build();
    let mut store = Store::new(engine, WasiHostState::new(store_limits));
    store.limiter(|state| &mut state.limits);
    if let Some(max_fuel) = limits.max_fuel {
        store
            .set_fuel(max_fuel)
            .map_err(|error| wasm_runtime_error("failed to configure WASM fuel", error))?;
    }
    if let Some(timeout_ms) = limits.timeout_ms {
        store.set_epoch_deadline(timeout_ms.max(1));
        store.epoch_deadline_trap();
    }
    Ok(store)
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
        reset_store_limits(store, self.limits)
    }
}

fn reset_store_limits(
    store: &mut Store<WasiHostState>,
    limits: WasmLimits,
) -> Result<(), LixError> {
    if let Some(max_fuel) = limits.max_fuel {
        store
            .set_fuel(max_fuel)
            .map_err(|error| wasm_runtime_error("failed to reset WASM fuel", error))?;
    }
    if let Some(timeout_ms) = limits.timeout_ms {
        store.set_epoch_deadline(timeout_ms.max(1));
    }
    Ok(())
}

struct TimeoutTickerRegistry {
    engine: Engine,
    state: Mutex<TimeoutTickerRegistryState>,
}

struct TimeoutTickerRegistryState {
    ticker: Option<TimeoutTicker>,
    leases: usize,
}

impl TimeoutTickerRegistry {
    fn new(engine: Engine) -> Self {
        Self {
            engine,
            state: Mutex::new(TimeoutTickerRegistryState {
                ticker: None,
                leases: 0,
            }),
        }
    }

    fn acquire(self: &Arc<Self>) -> Result<TimeoutTickerLease, LixError> {
        let mut state = self.state.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "WASM timeout ticker registry lock poisoned",
            )
        })?;
        let next_lease_count = state.leases.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "WASM timeout ticker lease count overflowed",
            )
        })?;
        if state.leases == 0 {
            debug_assert!(state.ticker.is_none());
            state.ticker = Some(TimeoutTicker::start(self.engine.clone()));
        }
        state.leases = next_lease_count;
        Ok(TimeoutTickerLease {
            registry: self.clone(),
        })
    }
}

struct TimeoutTickerLease {
    registry: Arc<TimeoutTickerRegistry>,
}

impl Drop for TimeoutTickerLease {
    fn drop(&mut self) {
        let mut state = match self.registry.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        debug_assert!(state.leases > 0);
        state.leases = state.leases.saturating_sub(1);
        if state.leases == 0 {
            // Keep the registry locked until the old thread has stopped. A new
            // lease cannot start a replacement ticker against the same Engine
            // while the old one is still advancing its epoch.
            let ticker = state.ticker.take();
            drop(ticker);
        }
        drop(state);
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
            loop {
                std::thread::sleep(Duration::from_millis(1));
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
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
        Self {
            filename: file.filename,
            data: file.data,
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;

    use wasm_encoder::{
        BlockType, CodeSection, ComponentBuilder, ComponentExportKind, ComponentValType,
        ExportKind, ExportSection, Function, FunctionSection, Instruction, MemorySection,
        MemoryType, Module, ModuleArg, TypeSection,
    };

    use super::*;

    #[test]
    fn default_runtime_reuses_process_wide_wasmtime_state() {
        let first = WasmtimePluginRuntime::new().expect("first runtime should initialize");
        let second = WasmtimePluginRuntime::new().expect("second runtime should initialize");
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
    }

    #[test]
    fn compiled_component_cache_singleflights_same_key() {
        let cache = Arc::new(CompiledComponentCache::new(2));
        let component = empty_component();
        let key = CompiledComponentKey::new(CompileProfile::Plain, b"same component");
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let cache = cache.clone();
                let component = component.clone();
                let calls = calls.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .expect("test runtime should initialize");
                    start.wait();
                    runtime
                        .block_on(cache.get_or_compile(key, || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(50));
                            Ok(component)
                        }))
                        .expect("component should compile")
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for handle in handles {
            handle.join().expect("cache worker should not panic");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn compiled_component_cache_singleflights_failure_wave_then_retries() {
        let cache = Arc::new(CompiledComponentCache::new(2));
        let key = CompiledComponentKey::new(CompileProfile::Plain, b"invalid component");
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let cache = cache.clone();
                let calls = calls.clone();
                let release = release.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .expect("test runtime should initialize");
                    start.wait();
                    runtime.block_on(cache.get_or_compile(key, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        while !release.load(Ordering::Acquire) {
                            std::thread::yield_now();
                        }
                        Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "synthetic compile failure",
                        ))
                    }))
                })
            })
            .collect::<Vec<_>>();
        start.wait();

        // Wait until both workers hold the same in-flight cell before letting
        // the initializer fail. The map and both workers each own one Arc.
        loop {
            let waiter_joined = cache
                .lock()
                .expect("cache lock should be healthy")
                .in_flight
                .get(&key)
                .is_some_and(|in_flight| Arc::strong_count(in_flight) >= 3);
            if waiter_joined {
                break;
            }
            std::thread::yield_now();
        }
        release.store(true, Ordering::Release);
        for handle in handles {
            assert!(
                handle
                    .join()
                    .expect("cache worker should not panic")
                    .is_err(),
                "compile failure must reach every waiter"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should initialize");
        let retry = runtime.block_on(cache.get_or_compile(key, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "synthetic retry failure",
            ))
        }));
        assert!(retry.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn compiled_component_cache_retries_failures_and_evicts_lru() {
        let cache = CompiledComponentCache::new(1);
        let component = empty_component();
        let failed_key = CompiledComponentKey::new(CompileProfile::Plain, b"invalid");
        let failures = AtomicUsize::new(0);
        for _ in 0..2 {
            let result = cache
                .get_or_compile(failed_key, || {
                    failures.fetch_add(1, Ordering::SeqCst);
                    Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "synthetic compile failure",
                    ))
                })
                .await;
            assert!(result.is_err(), "compile failure must be returned");
        }
        assert_eq!(failures.load(Ordering::SeqCst), 2);

        let first_key = CompiledComponentKey::new(CompileProfile::Plain, b"first");
        let second_key = CompiledComponentKey::new(CompileProfile::Plain, b"second");
        let compiles = AtomicUsize::new(0);
        for key in [first_key, second_key, first_key] {
            cache
                .get_or_compile(key, || {
                    compiles.fetch_add(1, Ordering::SeqCst);
                    Ok(component.clone())
                })
                .await
                .expect("component should compile");
        }
        assert_eq!(compiles.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn compiled_component_key_separates_engine_profiles_not_numeric_limits() {
        let bytes = b"component";
        let plain = CompiledComponentKey::new(CompileProfile::Plain, bytes);
        let fuel = CompiledComponentKey::new(CompileProfile::Fuel, bytes);
        assert_ne!(plain, fuel);
        assert_eq!(
            CompileProfile::from_limits(WasmLimits {
                max_fuel: Some(1),
                ..WasmLimits::default()
            }),
            CompileProfile::from_limits(WasmLimits {
                max_fuel: Some(1_000_000),
                ..WasmLimits::default()
            })
        );
    }

    #[test]
    fn timeout_ticker_is_single_and_stops_after_last_lease() {
        let shared = WasmtimeSharedRuntime::new().expect("shared runtime should initialize");
        let first = shared
            .timeout_ticker(CompileProfile::Timeout)
            .expect("ticker lock should be healthy")
            .expect("timeout profile should have a ticker");
        let second = shared
            .timeout_ticker(CompileProfile::Timeout)
            .expect("ticker lock should be healthy")
            .expect("timeout profile should have a ticker");
        assert!(Arc::ptr_eq(&first.registry, &second.registry));
        {
            let state = first
                .registry
                .state
                .lock()
                .expect("ticker registry lock should be healthy");
            assert_eq!(state.leases, 2);
            assert!(state.ticker.is_some());
        }
        drop(first);
        {
            let state = second
                .registry
                .state
                .lock()
                .expect("ticker registry lock should be healthy");
            assert_eq!(state.leases, 1);
            assert!(state.ticker.is_some());
        }
        let registry = second.registry.clone();
        drop(second);
        let state = registry
            .state
            .lock()
            .expect("ticker registry lock should be healthy");
        assert_eq!(state.leases, 0);
        assert!(
            state.ticker.is_none(),
            "shared runtime must not keep the 1 kHz ticker alive"
        );
    }

    #[test]
    fn store_enforces_memory_limit_at_page_boundary() {
        const PAGE_BYTES: u64 = 65_536;
        let engine = create_engine(false, false).expect("test engine should initialize");
        let bytes = component_with_initial_memory(2);
        let component = Component::new(&engine, bytes).expect("test component should compile");
        let linker = Linker::<WasiHostState>::new(&engine);

        let instantiate = |max_memory_bytes| {
            let limits = WasmLimits {
                max_memory_bytes,
                ..WasmLimits::default()
            };
            let mut store = create_store(&engine, limits).expect("test Store should initialize");
            linker.instantiate(&mut store, &component)
        };
        instantiate(2 * PAGE_BYTES).expect("exactly two pages should fit");
        instantiate(2 * PAGE_BYTES + 1).expect("one byte above two pages should still fit");
        instantiate(2 * PAGE_BYTES - 1)
            .expect_err("one byte below two pages must reject the initial memory");
    }

    #[test]
    fn store_tracks_successful_guest_memory_high_water_without_counting_rejected_growth() {
        const PAGE_BYTES: usize = 65_536;
        let inner = StoreLimitsBuilder::new()
            .memory_size(2 * PAGE_BYTES)
            .build();
        let mut limits = TrackingStoreLimits::new(inner);

        assert!(
            limits
                .memory_growing(0, PAGE_BYTES, None)
                .expect("first page should be allowed")
        );
        assert_eq!(limits.linear_memory_high_water_bytes(), PAGE_BYTES as u64);
        assert!(
            !limits
                .memory_growing(PAGE_BYTES, 3 * PAGE_BYTES, None)
                .expect("over-limit growth should be rejected without trapping")
        );
        assert_eq!(
            limits.linear_memory_high_water_bytes(),
            PAGE_BYTES as u64,
            "a rejected desired size is not guest memory that existed"
        );

        assert!(
            limits
                .memory_growing(PAGE_BYTES, 2 * PAGE_BYTES, None)
                .expect("second page should be permitted")
        );
        assert_eq!(
            limits.linear_memory_high_water_bytes(),
            (2 * PAGE_BYTES) as u64
        );
        limits
            .memory_grow_failed(wasmtime::Error::msg("synthetic allocation failure"))
            .expect("the default limiter ignores allocation failures");
        assert_eq!(
            limits.linear_memory_high_water_bytes(),
            PAGE_BYTES as u64,
            "a permitted but failed allocation must be rolled back"
        );
    }

    #[test]
    fn instantiated_component_memory_is_included_in_high_water() {
        const PAGE_BYTES: u64 = 65_536;
        let engine = create_engine(false, false).expect("test engine should initialize");
        let component = Component::new(&engine, component_with_initial_memory(2))
            .expect("test component should compile");
        let linker = Linker::<WasiHostState>::new(&engine);
        let mut store =
            create_store(&engine, WasmLimits::default()).expect("test Store should initialize");
        linker
            .instantiate(&mut store, &component)
            .expect("test component should instantiate");
        assert_eq!(
            store.data().limits.linear_memory_high_water_bytes(),
            2 * PAGE_BYTES
        );
    }

    #[test]
    fn store_rejects_more_than_one_linear_memory_across_component_instances() {
        let engine = create_engine(false, false).expect("test engine should initialize");
        let component = Component::new(&engine, component_with_initial_memories(2, 1))
            .expect("test component should compile");
        let linker = Linker::<WasiHostState>::new(&engine);
        let mut store =
            create_store(&engine, WasmLimits::default()).expect("test Store should initialize");

        linker
            .instantiate(&mut store, &component)
            .expect_err("the Store-wide memory count must reject a second linear memory");
    }

    #[test]
    fn component_timeout_resets_and_interrupts_a_non_terminating_guest() {
        const TEST_TIMEOUT_MS: u64 = 20;

        let shared = WasmtimeSharedRuntime::new().expect("shared runtime should initialize");
        let profile = CompileProfile::Timeout;
        let _ticker = shared
            .timeout_ticker(profile)
            .expect("ticker registry should be healthy")
            .expect("timeout profile should start a ticker");
        let engine = shared.engine(profile);
        let component = Component::new(engine, component_with_timeout_test_functions())
            .expect("test component should compile");
        let linker = Linker::<WasiHostState>::new(engine);
        let limits = WasmLimits {
            timeout_ms: Some(TEST_TIMEOUT_MS),
            ..WasmLimits::default()
        };
        let mut store = create_store(engine, limits).expect("test Store should initialize");
        let instance = linker
            .instantiate(&mut store, &component)
            .expect("test component should instantiate");
        let quick = instance
            .get_typed_func::<(), ()>(&mut store, "quick")
            .expect("quick export should have the expected type");
        let spin = instance
            .get_typed_func::<(), ()>(&mut store, "spin")
            .expect("spin export should have the expected type");

        // Let the Store's previous deadline expire before each quick call. A
        // successful call therefore demonstrates that reset_store_limits made
        // the timeout relative to this invocation rather than component init.
        for invocation in 1..=2 {
            std::thread::sleep(Duration::from_millis(TEST_TIMEOUT_MS * 2));
            reset_store_limits(&mut store, limits)
                .expect("each invocation should receive a fresh deadline");
            quick
                .call(&mut store, ())
                .unwrap_or_else(|error| panic!("invocation {invocation} should run: {error:#}"));
        }

        reset_store_limits(&mut store, limits)
            .expect("non-terminating invocation should receive a fresh deadline");
        let started = std::time::Instant::now();
        let error = spin
            .call(&mut store, ())
            .expect_err("non-terminating guest call must be interrupted");
        let elapsed = started.elapsed();
        assert_eq!(
            error.downcast_ref::<wasmtime::Trap>(),
            Some(&wasmtime::Trap::Interrupt),
            "guest should trap because its epoch deadline elapsed: {error:#}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "guest exceeded the bounded test window: {elapsed:?}"
        );
    }

    fn empty_component() -> Component {
        let engine = create_engine(false, false).expect("test engine should initialize");
        Component::new(&engine, wasm_encoder::Component::new().finish())
            .expect("empty component should compile")
    }

    fn component_with_initial_memory(pages: u64) -> Vec<u8> {
        component_with_initial_memories(1, pages)
    }

    fn component_with_initial_memories(count: usize, pages: u64) -> Vec<u8> {
        let mut component = ComponentBuilder::default();
        for _ in 0..count {
            let mut memories = MemorySection::new();
            memories.memory(MemoryType {
                minimum: pages,
                maximum: Some(pages),
                memory64: false,
                shared: false,
                page_size_log2: None,
            });
            let mut module = Module::new();
            module.section(&memories);
            let module = component.core_module(None, &module);
            component.core_instantiate(
                None,
                module,
                std::iter::empty::<(&'static str, ModuleArg)>(),
            );
        }
        component.finish()
    }

    fn component_with_timeout_test_functions() -> Vec<u8> {
        let mut types = TypeSection::new();
        types.ty().function([], []);
        let mut functions = FunctionSection::new();
        functions.function(0);
        functions.function(0);
        let mut exports = ExportSection::new();
        exports.export("quick", ExportKind::Func, 0);
        exports.export("spin", ExportKind::Func, 1);
        let mut quick = Function::new([]);
        quick.instruction(&Instruction::End);
        let mut spin = Function::new([]);
        spin.instruction(&Instruction::Loop(BlockType::Empty));
        spin.instruction(&Instruction::Br(0));
        spin.instruction(&Instruction::End);
        spin.instruction(&Instruction::End);
        let mut code = CodeSection::new();
        code.function(&quick);
        code.function(&spin);
        let mut module = Module::new();
        module
            .section(&types)
            .section(&functions)
            .section(&exports)
            .section(&code);

        let mut component = ComponentBuilder::default();
        let module = component.core_module(Some("timeout-test"), &module);
        let instance = component.core_instantiate(
            Some("timeout-test"),
            module,
            std::iter::empty::<(&'static str, ModuleArg)>(),
        );
        let quick = component.core_alias_export(Some("quick"), instance, "quick", ExportKind::Func);
        let spin = component.core_alias_export(Some("spin"), instance, "spin", ExportKind::Func);
        let (function_type, mut encoder) = component.type_function(Some("timeout-test"));
        encoder
            .params(std::iter::empty::<(&'static str, ComponentValType)>())
            .result(None);
        let quick = component.lift_func(Some("quick"), quick, function_type, std::iter::empty());
        let spin = component.lift_func(Some("spin"), spin, function_type, std::iter::empty());
        component.export("quick", ComponentExportKind::Func, quick, None);
        component.export("spin", ComponentExportKind::Func, spin, None);
        component.finish()
    }
}

#[cfg(test)]
mod benchmark_probe {
    use std::hint::black_box;
    use std::path::Path;
    use std::time::{Duration, Instant};

    use super::*;

    #[derive(Clone, Copy)]
    enum Operation {
        Runtime,
        Hash,
        Compile,
        Linker,
        Store,
        Instantiate,
        Init,
        Call,
    }

    #[tokio::test]
    #[ignore = "production Wasmtime lifecycle benchmark probe"]
    async fn wasm_runtime_lifecycle_benchmark_probe() {
        let operation = match std::env::var("LIX_WASM_BENCH_OPERATION")
            .unwrap_or_else(|_| "init".to_string())
            .as_str()
        {
            "runtime" => Operation::Runtime,
            "hash" => Operation::Hash,
            "compile" => Operation::Compile,
            "linker" => Operation::Linker,
            "store" => Operation::Store,
            "instantiate" => Operation::Instantiate,
            "init" => Operation::Init,
            "call" => Operation::Call,
            value => panic!(
                "LIX_WASM_BENCH_OPERATION must be runtime, hash, compile, linker, store, instantiate, init, or call; got {value:?}"
            ),
        };
        let rounds = env_usize("LIX_WASM_BENCH_ROUNDS", 50);
        let warmups = env_usize("LIX_WASM_BENCH_WARMUPS", 5);
        assert!(rounds > 0, "benchmark needs at least one measured round");

        let wasm_path = std::env::var_os("LIX_WASM_BENCH_COMPONENT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                panic!("LIX_WASM_BENCH_COMPONENT must point to a built plugin component")
            });
        let wasm_path = Path::new(&wasm_path);
        let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
            panic!(
                "failed to read bindep-built CSV plugin wasm at {}: {error}",
                wasm_path.display()
            )
        });
        let runtime = WasmtimePluginRuntime::new().expect("benchmark runtime should initialize");
        let engine = runtime.shared.engine(CompileProfile::Plain);
        let component = Component::new(engine, &wasm).expect("benchmark component should compile");
        let mut linker = Linker::<WasiHostState>::new(engine);
        add_to_linker_sync(&mut linker).expect("benchmark linker should initialize");
        let instance = runtime
            .init_component(wasm.clone(), WasmLimits::default())
            .await
            .expect("benchmark component should initialize");

        for _ in 0..warmups {
            run_operation(operation, &runtime, &wasm, &component, &linker, &instance).await;
        }
        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = Instant::now();
            run_operation(operation, &runtime, &wasm, &component, &linker, &instance).await;
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        println!(
            "wasm_runtime_lifecycle_probe operation={} wasm_bytes={} rounds={} p50_ns={} p95_ns={}",
            operation_name(operation),
            wasm.len(),
            rounds,
            percentile(&samples, 50, 100).as_nanos(),
            percentile(&samples, 95, 100).as_nanos(),
        );
    }

    async fn run_operation(
        operation: Operation,
        runtime: &WasmtimePluginRuntime,
        wasm: &[u8],
        component: &Component,
        linker: &Linker<WasiHostState>,
        instance: &Arc<dyn WasmComponentInstance>,
    ) {
        match operation {
            Operation::Runtime => {
                black_box(WasmtimePluginRuntime::new())
                    .expect("benchmark runtime should initialize");
            }
            Operation::Hash => {
                black_box(blake3::hash(black_box(wasm)));
            }
            Operation::Compile => {
                black_box(Component::new(
                    runtime.shared.engine(CompileProfile::Plain),
                    black_box(wasm),
                ))
                .expect("benchmark component should compile");
            }
            Operation::Linker => {
                let mut linker =
                    Linker::<WasiHostState>::new(runtime.shared.engine(CompileProfile::Plain));
                add_to_linker_sync(&mut linker).expect("benchmark linker should initialize");
                black_box(linker);
            }
            Operation::Store => {
                black_box(create_store(
                    runtime.shared.engine(CompileProfile::Plain),
                    WasmLimits::default(),
                ))
                .expect("benchmark Store should initialize");
            }
            Operation::Instantiate => {
                let mut store = create_store(
                    runtime.shared.engine(CompileProfile::Plain),
                    WasmLimits::default(),
                )
                .expect("benchmark Store should initialize");
                black_box(plugin_bindings::Plugin::instantiate(
                    &mut store, component, linker,
                ))
                .expect("benchmark component should instantiate");
            }
            Operation::Init => {
                black_box(
                    runtime
                        .init_component(black_box(wasm.to_vec()), WasmLimits::default())
                        .await,
                )
                .expect("benchmark component should initialize");
            }
            Operation::Call => {
                black_box(
                    instance
                        .detect_changes(
                            Vec::new(),
                            WasmPluginFile {
                                filename: Some("probe.csv".to_string()),
                                data: Vec::new(),
                            },
                        )
                        .await,
                )
                .expect("benchmark guest call should succeed");
            }
        }
    }

    fn operation_name(operation: Operation) -> &'static str {
        match operation {
            Operation::Runtime => "runtime",
            Operation::Hash => "hash",
            Operation::Compile => "compile",
            Operation::Linker => "linker",
            Operation::Store => "store",
            Operation::Instantiate => "instantiate",
            Operation::Init => "init",
            Operation::Call => "call",
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name).map_or(default, |value| {
            value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be a positive integer"))
        })
    }

    fn percentile(samples: &[Duration], numerator: usize, denominator: usize) -> Duration {
        let index = samples.len().saturating_mul(numerator).saturating_sub(1) / denominator;
        samples[index]
    }
}
