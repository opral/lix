use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::binary_cas::BlobHash;
use crate::common::LixError;
use crate::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};

use super::{CompiledPluginCatalog, InstalledPlugin, PluginCatalogCache, PluginRegistry};

/// Installed plugins are untrusted workspace data. Bound every component
/// instantiation and exported call so malformed or adversarial guest code
/// cannot occupy a server executor indefinitely.
const DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS: u64 = 5_000;

fn default_plugin_wasm_limits() -> WasmLimits {
    WasmLimits {
        timeout_ms: Some(DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS),
        ..WasmLimits::default()
    }
}

#[derive(Clone)]
pub(crate) struct CachedPluginComponent {
    pub(crate) wasm_hash: BlobHash,
    pub(crate) instance: Arc<dyn WasmComponentInstance>,
}

#[derive(Clone)]
pub(crate) struct PluginRuntimeHost {
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_component_cache: Arc<Mutex<BTreeMap<String, CachedPluginComponent>>>,
    plugin_catalog_cache: Arc<Mutex<PluginCatalogCache>>,
}

impl PluginRuntimeHost {
    pub(crate) fn new(wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        Self {
            wasm_runtime,
            plugin_component_cache: Arc::new(Mutex::new(BTreeMap::new())),
            plugin_catalog_cache: Arc::new(Mutex::new(PluginCatalogCache::default())),
        }
    }

    /// Returns the compiled matcher for a durable registry generation.
    ///
    /// The host is shared across executions, so warm writes compile globs once
    /// per generation rather than once per transaction or file.
    pub(crate) fn compiled_plugin_catalog(
        &self,
        registry: &PluginRegistry,
    ) -> Result<Arc<CompiledPluginCatalog>, LixError> {
        self.plugin_catalog_cache
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "plugin catalog cache lock poisoned",
                )
            })?
            .get_or_compile(registry)
    }

    /// Returns a warm component without loading its content-addressed bytes.
    ///
    /// The lookup examines only the plugin key and fixed-size content hash, so
    /// its work is independent of the component's byte length. Callers must
    /// retain and execute the returned instance directly: discarding it and
    /// performing a second key-only lookup would reopen a race with another
    /// branch installing a different hash under the same plugin key.
    pub(crate) fn cached_plugin_component(
        &self,
        plugin_key: &str,
        wasm_hash: BlobHash,
    ) -> Result<Option<Arc<dyn WasmComponentInstance>>, LixError> {
        cached_plugin_component(&self.plugin_component_cache, plugin_key, wasm_hash)
    }
}

impl PluginComponentHost for PluginRuntimeHost {
    fn plugin_component_cache(&self) -> &Mutex<BTreeMap<String, CachedPluginComponent>> {
        &self.plugin_component_cache
    }

    fn wasm_runtime(&self) -> &Arc<dyn WasmRuntime> {
        &self.wasm_runtime
    }
}

pub(crate) trait PluginComponentHost {
    fn plugin_component_cache(&self) -> &Mutex<BTreeMap<String, CachedPluginComponent>>;

    fn wasm_runtime(&self) -> &Arc<dyn WasmRuntime>;
}

fn cached_plugin_component(
    cache: &Mutex<BTreeMap<String, CachedPluginComponent>>,
    plugin_key: &str,
    wasm_hash: BlobHash,
) -> Result<Option<Arc<dyn WasmComponentInstance>>, LixError> {
    let guard = cache.lock().map_err(|_| component_cache_lock_error())?;
    Ok(guard
        .get(plugin_key)
        .filter(|cached| cached.wasm_hash == wasm_hash)
        .map(|cached| Arc::clone(&cached.instance)))
}

fn component_cache_lock_error() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "plugin component cache lock poisoned",
    )
}

pub(crate) async fn load_or_init_plugin_component(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
    if let Some(instance) =
        cached_plugin_component(host.plugin_component_cache(), &plugin.key, plugin.wasm_hash)?
    {
        return Ok(instance);
    }

    let initialized = host
        .wasm_runtime()
        .init_component(plugin.wasm.clone(), default_plugin_wasm_limits())
        .await?;
    let mut guard = host
        .plugin_component_cache()
        .lock()
        .map_err(|_| component_cache_lock_error())?;
    if let Some(cached) = guard.get(&plugin.key) {
        if cached.wasm_hash == plugin.wasm_hash {
            return Ok(Arc::clone(&cached.instance));
        }
    }
    guard.insert(
        plugin.key.clone(),
        CachedPluginComponent {
            wasm_hash: plugin.wasm_hash,
            instance: initialized.clone(),
        },
    );
    Ok(initialized)
}

pub(crate) async fn render_with_plugin(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    state: Vec<WasmPluginEntityState>,
) -> Result<Vec<u8>, LixError> {
    let instance = load_or_init_plugin_component(host, plugin).await?;
    instance.render(state).await
}

pub(crate) async fn detect_changes_with_plugin(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
    state: Vec<WasmPluginEntityState>,
    file: WasmPluginFile,
) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
    let instance = load_or_init_plugin_component(host, plugin).await?;
    instance.detect_changes(state, file).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{InstalledPlugin, PluginRuntime};
    use crate::wasm::WasmRuntime;
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestHost {
        wasm_runtime: Arc<dyn WasmRuntime>,
        plugin_component_cache: Mutex<BTreeMap<String, CachedPluginComponent>>,
    }

    impl PluginComponentHost for TestHost {
        fn plugin_component_cache(&self) -> &Mutex<BTreeMap<String, CachedPluginComponent>> {
            &self.plugin_component_cache
        }

        fn wasm_runtime(&self) -> &Arc<dyn WasmRuntime> {
            &self.wasm_runtime
        }
    }

    #[derive(Default)]
    struct CountingRuntime {
        init_calls: Arc<AtomicUsize>,
        init_limits: Arc<Mutex<Vec<WasmLimits>>>,
    }

    struct NoopComponent;

    #[async_trait]
    impl WasmRuntime for CountingRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
            self.init_limits
                .lock()
                .expect("recorded WASM limits lock should be healthy")
                .push(limits);
            Ok(Arc::new(NoopComponent))
        }
    }

    #[async_trait]
    impl WasmComponentInstance for NoopComponent {
        async fn detect_changes(
            &self,
            _state: Vec<WasmPluginEntityState>,
            _file: WasmPluginFile,
        ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
            Ok(Vec::new())
        }

        async fn render(&self, _state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn component_cache_reinitializes_when_same_key_wasm_changes() {
        let runtime = Arc::new(CountingRuntime::default());
        let host = TestHost {
            wasm_runtime: runtime.clone(),
            plugin_component_cache: std::sync::Mutex::new(BTreeMap::default()),
        };
        let mut plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: Vec::new(),
            manifest_json: "{}".to_string(),
            wasm_hash: BlobHash::from_content(&[1]),
            wasm: vec![1],
        };

        load_or_init_plugin_component(&host, &plugin)
            .await
            .expect("first init should succeed");
        load_or_init_plugin_component(&host, &plugin)
            .await
            .expect("second lookup should reuse cache");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);

        plugin.wasm = vec![2];
        plugin.wasm_hash = BlobHash::from_content(&plugin.wasm);
        load_or_init_plugin_component(&host, &plugin)
            .await
            .expect("changed wasm should reinitialize instance");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 2);
        assert!(
            runtime
                .init_limits
                .lock()
                .expect("recorded WASM limits lock should be healthy")
                .iter()
                .all(|limits| {
                    limits.timeout_ms == Some(DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS)
                        && limits.max_memory_bytes == WasmLimits::default().max_memory_bytes
                        && limits.max_fuel.is_none()
                }),
            "every installed component must retain the memory cap and receive an execution deadline"
        );
    }

    #[tokio::test]
    async fn runtime_host_can_hit_component_cache_before_loading_wasm() {
        let runtime = Arc::new(CountingRuntime::default());
        let host = PluginRuntimeHost::new(runtime.clone());
        let plugin = InstalledPlugin {
            key: "k".to_string(),
            runtime: PluginRuntime::WasmComponentV1,
            api_version: "0.1.0".to_string(),
            path_glob: "*.json".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: Vec::new(),
            manifest_json: "{}".to_string(),
            wasm_hash: BlobHash::from_content(&[1]),
            wasm: vec![1],
        };

        let initialized = load_or_init_plugin_component(&host, &plugin)
            .await
            .expect("first load should initialize the component");
        let cached = host
            .cached_plugin_component(&plugin.key, plugin.wasm_hash)
            .expect("warm lookup should acquire the cache")
            .expect("matching key and hash should hit the cache");
        assert!(Arc::ptr_eq(&initialized, &cached));
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 1);

        assert!(
            host.cached_plugin_component(&plugin.key, BlobHash::from_content(&[2]))
                .expect("hash-mismatch lookup should acquire the cache")
                .is_none()
        );
    }
}
