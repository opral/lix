use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::common::LixError;
use crate::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};

use super::InstalledPlugin;

#[derive(Clone)]
pub(crate) struct CachedPluginComponent {
    pub(crate) wasm: Vec<u8>,
    pub(crate) instance: Arc<dyn WasmComponentInstance>,
}

#[derive(Clone)]
pub(crate) struct PluginRuntimeHost {
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_component_cache: Arc<Mutex<BTreeMap<String, CachedPluginComponent>>>,
}

impl PluginRuntimeHost {
    pub(crate) fn new(wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        Self {
            wasm_runtime,
            plugin_component_cache: Arc::new(Mutex::new(BTreeMap::new())),
        }
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

pub(crate) async fn load_or_init_plugin_component(
    host: &impl PluginComponentHost,
    plugin: &InstalledPlugin,
) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
    {
        let guard = host.plugin_component_cache().lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            message: "plugin component cache lock poisoned".to_string(),
            hint: None,
            details: None,
        })?;
        if let Some(cached) = guard.get(&plugin.key) {
            if cached.wasm == plugin.wasm {
                return Ok(cached.instance.clone());
            }
        }
    }

    let initialized = host
        .wasm_runtime()
        .init_component(plugin.wasm.clone(), WasmLimits::default())
        .await?;
    let mut guard = host.plugin_component_cache().lock().map_err(|_| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: "plugin component cache lock poisoned".to_string(),
        hint: None,
        details: None,
    })?;
    if let Some(cached) = guard.get(&plugin.key) {
        if cached.wasm == plugin.wasm {
            return Ok(cached.instance.clone());
        }
    }
    guard.insert(
        plugin.key.clone(),
        CachedPluginComponent {
            wasm: plugin.wasm.clone(),
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
    }

    struct NoopComponent;

    #[async_trait]
    impl WasmRuntime for CountingRuntime {
        async fn init_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            self.init_calls.fetch_add(1, Ordering::SeqCst);
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
        load_or_init_plugin_component(&host, &plugin)
            .await
            .expect("changed wasm should reinitialize instance");
        assert_eq!(runtime.init_calls.load(Ordering::SeqCst), 2);
    }
}
