use std::sync::Arc;

use crate::common::LixError;
use crate::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};

use super::InstalledPlugin;

#[derive(Clone)]
pub(crate) struct CachedPluginComponent {
    pub(crate) wasm: Vec<u8>,
    pub(crate) instance: Arc<dyn WasmComponentInstance>,
}

const RENDER_EXPORTS: &[&str] = &["render", "api#render"];

pub(crate) trait PluginComponentHost {
    fn plugin_component_cache(
        &self,
    ) -> &std::sync::Mutex<std::collections::BTreeMap<String, CachedPluginComponent>>;

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
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let instance = load_or_init_plugin_component(host, plugin).await?;
    invoke_render_export(instance.as_ref(), payload).await
}

async fn invoke_render_export(
    instance: &dyn WasmComponentInstance,
    payload: &[u8],
) -> Result<Vec<u8>, LixError> {
    let mut errors = Vec::new();
    for export in RENDER_EXPORTS {
        match instance.call(export, payload).await {
            Ok(output) => return Ok(output),
            Err(error) => errors.push(format!("{export}: {}", error.message)),
        }
    }

    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        message: format!(
            "plugin materialization: failed to call render export ({})",
            errors.join("; ")
        ),
        hint: None,
        details: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{InstalledPlugin, PluginRuntime};
    use crate::wasm::WasmRuntime;
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestHost {
        wasm_runtime: Arc<dyn WasmRuntime>,
        plugin_component_cache: std::sync::Mutex<BTreeMap<String, CachedPluginComponent>>,
    }

    impl PluginComponentHost for TestHost {
        fn plugin_component_cache(
            &self,
        ) -> &std::sync::Mutex<BTreeMap<String, CachedPluginComponent>> {
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

    #[async_trait(?Send)]
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

    #[async_trait(?Send)]
    impl WasmComponentInstance for NoopComponent {
        async fn call(&self, _export: &str, _input: &[u8]) -> Result<Vec<u8>, LixError> {
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
