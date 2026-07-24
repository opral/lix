use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::binary_cas::BlobHash;
use crate::common::LixError;
use crate::wasm::{WasmComponentV2Factory, WasmLimits, WasmRuntime, WasmTransitionCounters};

use super::{
    CompiledPluginCatalog, DEFAULT_MAX_PLUGIN_FILE_ACTORS, InstalledPlugin, PluginActorCache,
    PluginCatalogCache, PluginRegistry,
};

/// Installed plugins are untrusted workspace data. Bound every component
/// instantiation and exported call so malformed or adversarial guest code
/// cannot occupy a server executor indefinitely.
const DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS: u64 = 5_000;
/// Recursive plugins retain semantic indexes alongside accepted source bytes.
pub(crate) const DEFAULT_PLUGIN_V2_MEMORY_BYTES: u64 = 128 * 1024 * 1024;

fn plugin_v2_wasm_limits(max_memory_bytes: u64) -> Result<WasmLimits, LixError> {
    if max_memory_bytes == 0 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "plugin memory limit must be positive",
        ));
    }
    Ok(WasmLimits {
        max_memory_bytes,
        timeout_ms: Some(DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS),
        ..WasmLimits::default()
    })
}

#[cfg(test)]
fn default_plugin_v2_wasm_limits() -> WasmLimits {
    plugin_v2_wasm_limits(DEFAULT_PLUGIN_V2_MEMORY_BYTES)
        .expect("the default plugin memory limit is positive")
}

#[derive(Clone)]
struct CachedPluginV2Factory {
    wasm_hash: BlobHash,
    factory: Arc<dyn WasmComponentV2Factory>,
}

#[derive(Clone)]
pub(crate) struct PluginRuntimeHost {
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_v2_factory_cache: Arc<Mutex<BTreeMap<String, CachedPluginV2Factory>>>,
    plugin_v2_wasm_limits: WasmLimits,
    plugin_actor_cache: PluginActorCache,
    plugin_v2_transition_counters: Arc<Mutex<WasmTransitionCounters>>,
    plugin_catalog_cache: Arc<Mutex<PluginCatalogCache>>,
    /// Ordinary plugin writes share this gate; lifecycle replacements take it
    /// exclusively. The guards live on transactions through durable commit,
    /// closing the owner-preflight/registry-swap race without serializing
    /// independent file writes against each other.
    plugin_generation_fence: Arc<tokio::sync::RwLock<()>>,
}

impl PluginRuntimeHost {
    pub(crate) fn new(wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        Self::new_with_v2_limits(
            wasm_runtime,
            DEFAULT_PLUGIN_V2_MEMORY_BYTES,
            DEFAULT_MAX_PLUGIN_FILE_ACTORS,
        )
        .expect("default plugin resource limits are valid")
    }

    pub(crate) fn new_with_v2_limits(
        wasm_runtime: Arc<dyn WasmRuntime>,
        max_memory_bytes: u64,
        max_cached_file_actors: usize,
    ) -> Result<Self, LixError> {
        Ok(Self {
            wasm_runtime,
            plugin_v2_factory_cache: Arc::new(Mutex::new(BTreeMap::new())),
            plugin_v2_wasm_limits: plugin_v2_wasm_limits(max_memory_bytes)?,
            plugin_actor_cache: PluginActorCache::new(max_cached_file_actors)?,
            plugin_v2_transition_counters: Arc::new(Mutex::new(WasmTransitionCounters::default())),
            plugin_catalog_cache: Arc::new(Mutex::new(PluginCatalogCache::default())),
            plugin_generation_fence: Arc::new(tokio::sync::RwLock::new(())),
        })
    }

    pub(crate) async fn acquire_plugin_generation_read(
        &self,
    ) -> tokio::sync::OwnedRwLockReadGuard<()> {
        Arc::clone(&self.plugin_generation_fence).read_owned().await
    }

    pub(crate) async fn acquire_plugin_generation_upgrade(
        &self,
    ) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.plugin_generation_fence)
            .write_owned()
            .await
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

    pub(crate) fn actor_cache(&self) -> PluginActorCache {
        self.plugin_actor_cache.clone()
    }

    /// Aggregates validated guest work and host-owned lifecycle facts.
    /// Poison recovery is deliberate: diagnostics must not fail a transaction.
    pub(crate) fn record_v2_transition_counters(&self, counters: WasmTransitionCounters) {
        self.plugin_v2_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accumulate(counters);
    }

    pub(crate) fn v2_transition_counters(&self) -> WasmTransitionCounters {
        *self
            .plugin_v2_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn reset_v2_transition_counters(&self) {
        *self
            .plugin_v2_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WasmTransitionCounters::default();
    }

    /// Shares only compiled code. Every file actor created from this factory
    /// receives a distinct Store/instance through `instantiate_actor`.
    pub(crate) async fn load_or_compile_v2_factory(
        &self,
        plugin: &InstalledPlugin,
    ) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
        if let Some(factory) = self.cached_plugin_v2_factory(&plugin.key, plugin.wasm_hash)? {
            return Ok(factory);
        }
        let compiled = self
            .wasm_runtime
            .compile_component_v2(plugin.wasm.clone(), self.plugin_v2_wasm_limits)
            .await?;
        let mut cache = self
            .plugin_v2_factory_cache
            .lock()
            .map_err(|_| component_cache_lock_error())?;
        if let Some(cached) = cache.get(&plugin.key)
            && cached.wasm_hash == plugin.wasm_hash
        {
            return Ok(Arc::clone(&cached.factory));
        }
        cache.insert(
            plugin.key.clone(),
            CachedPluginV2Factory {
                wasm_hash: plugin.wasm_hash,
                factory: Arc::clone(&compiled),
            },
        );
        Ok(compiled)
    }

    pub(crate) fn cached_plugin_v2_factory(
        &self,
        plugin_key: &str,
        wasm_hash: BlobHash,
    ) -> Result<Option<Arc<dyn WasmComponentV2Factory>>, LixError> {
        let cache = self
            .plugin_v2_factory_cache
            .lock()
            .map_err(|_| component_cache_lock_error())?;
        Ok(cache
            .get(plugin_key)
            .filter(|cached| cached.wasm_hash == wasm_hash)
            .map(|cached| Arc::clone(&cached.factory)))
    }
}

fn component_cache_lock_error() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "plugin component cache lock poisoned",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wasm::UnsupportedWasmRuntime;

    #[test]
    fn plugin_memory_policy_is_explicit() {
        assert_eq!(WasmLimits::default().max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(
            default_plugin_v2_wasm_limits().max_memory_bytes,
            128 * 1024 * 1024
        );
        assert_eq!(
            default_plugin_v2_wasm_limits().timeout_ms,
            Some(DEFAULT_PLUGIN_EXECUTION_TIMEOUT_MS)
        );
        assert!(plugin_v2_wasm_limits(0).is_err());
        assert_eq!(
            plugin_v2_wasm_limits(192 * 1024 * 1024)
                .expect("custom limit should validate")
                .max_memory_bytes,
            192 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn generation_upgrade_gate_serializes_preflight_with_file_commit_window() {
        use std::time::Duration;

        let host = PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime));
        let ordinary_commit_guard = host.acquire_plugin_generation_read().await;
        let attempted_upgrade = Arc::new(tokio::sync::Barrier::new(2));
        let (upgrade_acquired_tx, mut upgrade_acquired_rx) = tokio::sync::oneshot::channel();
        let upgrade_host = host.clone();
        let upgrade_barrier = Arc::clone(&attempted_upgrade);
        let upgrade = tokio::spawn(async move {
            upgrade_barrier.wait().await;
            let guard = upgrade_host.acquire_plugin_generation_upgrade().await;
            let _ = upgrade_acquired_tx.send(());
            guard
        });
        attempted_upgrade.wait().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut upgrade_acquired_rx)
                .await
                .is_err(),
            "upgrade preflight must wait until the ordinary file transaction commits"
        );
        drop(ordinary_commit_guard);
        tokio::time::timeout(Duration::from_secs(1), &mut upgrade_acquired_rx)
            .await
            .expect("upgrade should acquire after ordinary commit")
            .expect("upgrade task should report acquisition");
        let upgrade_guard = upgrade.await.expect("upgrade task should finish");

        let attempted_file = Arc::new(tokio::sync::Barrier::new(2));
        let (file_acquired_tx, mut file_acquired_rx) = tokio::sync::oneshot::channel();
        let file_host = host.clone();
        let file_barrier = Arc::clone(&attempted_file);
        let ordinary = tokio::spawn(async move {
            file_barrier.wait().await;
            let guard = file_host.acquire_plugin_generation_read().await;
            let _ = file_acquired_tx.send(());
            guard
        });
        attempted_file.wait().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut file_acquired_rx)
                .await
                .is_err(),
            "ordinary file reconciliation must wait across upgrade preflight and registry commit"
        );
        drop(upgrade_guard);
        tokio::time::timeout(Duration::from_secs(1), &mut file_acquired_rx)
            .await
            .expect("ordinary file transition should acquire after upgrade commit")
            .expect("ordinary task should report acquisition");
        drop(ordinary.await.expect("ordinary task should finish"));
    }

    #[test]
    fn runtime_host_aggregates_and_resets_v2_transition_counters() {
        let host = PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime));
        host.record_v2_transition_counters(WasmTransitionCounters {
            packet_pages: 2,
            durable_semantic_changes: 1,
            guest_linear_memory_high_water_bytes: 128,
            host_full_content_classification_bytes: 10,
            ..WasmTransitionCounters::default()
        });
        host.record_v2_transition_counters(WasmTransitionCounters {
            packet_pages: 3,
            private_document_cache_hits: 1,
            guest_linear_memory_high_water_bytes: 64,
            host_full_content_classification_bytes: 7,
            ..WasmTransitionCounters::default()
        });

        let counters = host.v2_transition_counters();
        assert_eq!(counters.packet_pages, 5);
        assert_eq!(counters.durable_semantic_changes, 1);
        assert_eq!(counters.private_document_cache_hits, 1);
        assert_eq!(counters.guest_linear_memory_high_water_bytes, 128);
        assert_eq!(counters.host_full_content_classification_bytes, 17);

        host.reset_v2_transition_counters();
        assert_eq!(
            host.v2_transition_counters(),
            WasmTransitionCounters::default()
        );
    }
}
