use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::binary_cas::BlobId;
use crate::common::LixError;
use crate::{
    plugin::runtime::{WasmComponentFactory, WasmRuntime, WasmTransitionCounters},
    wasm::WasmLimits,
};

use super::{
    CompiledPluginCatalog, DEFAULT_MAX_LIVE_PLUGIN_STORES, InstalledPlugin, PluginActorCache,
    PluginCatalogCache, PluginRegistry,
};

/// Installed plugins are untrusted repository data. This is the absolute
/// per-export ceiling; a transition's tighter host budget remains authoritative
/// for normal operations. The cold-file budget may extend up to this ceiling,
/// but no guest call can exceed it.
const MAX_PLUGIN_EXECUTION_TIMEOUT_MS: u64 = 60_000;
/// Preserve enough headroom for recursive plugins and large minified text
/// snapshots. The live-Store working set remains independently bounded.
pub(crate) const DEFAULT_PLUGIN_MEMORY_BYTES: u64 = 192 * 1024 * 1024;

fn plugin_wasm_limits(max_memory_bytes: u64) -> Result<WasmLimits, LixError> {
    if max_memory_bytes == 0 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "plugin memory limit must be positive",
        ));
    }
    Ok(WasmLimits {
        max_memory_bytes,
        timeout_ms: Some(MAX_PLUGIN_EXECUTION_TIMEOUT_MS),
        ..WasmLimits::default()
    })
}

#[cfg(test)]
fn default_plugin_wasm_limits() -> WasmLimits {
    plugin_wasm_limits(DEFAULT_PLUGIN_MEMORY_BYTES)
        .expect("the default plugin memory limit is positive")
}

#[derive(Clone)]
struct CachedPluginFactory {
    wasm_hash: BlobId,
    factory: Arc<dyn WasmComponentFactory>,
}

#[cfg(test)]
#[derive(Default)]
struct PluginRegistryReadCache {
    snapshot: Option<u128>,
    registries: BTreeMap<String, PluginRegistry>,
}

#[derive(Clone)]
pub(crate) struct PluginRuntimeHost {
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_factory_cache: Arc<Mutex<BTreeMap<String, CachedPluginFactory>>>,
    plugin_wasm_limits: WasmLimits,
    plugin_actor_cache: PluginActorCache,
    plugin_transition_counters: Arc<Mutex<WasmTransitionCounters>>,
    plugin_catalog_cache: Arc<Mutex<PluginCatalogCache>>,
    #[cfg(test)]
    plugin_registry_read_cache: Arc<Mutex<PluginRegistryReadCache>>,
    /// Ordinary plugin writes share this gate; lifecycle replacements take it
    /// exclusively. The guards live on transactions through durable commit,
    /// closing the owner-preflight/registry-swap race without serializing
    /// independent file writes against each other.
    plugin_generation_fence: Arc<tokio::sync::RwLock<()>>,
}

impl PluginRuntimeHost {
    pub(crate) fn new(wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        Self::new_with_limits(
            wasm_runtime,
            DEFAULT_PLUGIN_MEMORY_BYTES,
            DEFAULT_MAX_LIVE_PLUGIN_STORES,
        )
        .expect("default plugin resource limits are valid")
    }

    pub(crate) fn new_with_limits(
        wasm_runtime: Arc<dyn WasmRuntime>,
        max_memory_bytes: u64,
        max_live_stores: usize,
    ) -> Result<Self, LixError> {
        Ok(Self {
            wasm_runtime,
            plugin_factory_cache: Arc::new(Mutex::new(BTreeMap::new())),
            plugin_wasm_limits: plugin_wasm_limits(max_memory_bytes)?,
            plugin_actor_cache: PluginActorCache::new(max_live_stores)?,
            plugin_transition_counters: Arc::new(Mutex::new(WasmTransitionCounters::default())),
            plugin_catalog_cache: Arc::new(Mutex::new(PluginCatalogCache::default())),
            #[cfg(test)]
            plugin_registry_read_cache: Arc::new(Mutex::new(PluginRegistryReadCache::default())),
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

    #[cfg(test)]
    pub(crate) fn cached_plugin_registries(
        &self,
        snapshot: u128,
        branch_ids: &BTreeSet<String>,
    ) -> Result<Option<BTreeMap<String, PluginRegistry>>, LixError> {
        let cache = self.plugin_registry_read_cache.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin registry read cache lock poisoned",
            )
        })?;
        if cache.snapshot != Some(snapshot) {
            return Ok(None);
        }
        let registries = branch_ids
            .iter()
            .map(|branch_id| {
                cache
                    .registries
                    .get(branch_id)
                    .cloned()
                    .map(|registry| (branch_id.clone(), registry))
            })
            .collect::<Option<BTreeMap<_, _>>>();
        Ok(registries)
    }

    #[cfg(test)]
    pub(crate) fn cache_plugin_registries(
        &self,
        snapshot: u128,
        registries: &BTreeMap<String, PluginRegistry>,
    ) -> Result<(), LixError> {
        let mut cache = self.plugin_registry_read_cache.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin registry read cache lock poisoned",
            )
        })?;
        if cache.snapshot != Some(snapshot) {
            cache.snapshot = Some(snapshot);
            cache.registries.clear();
        }
        cache.registries.extend(registries.clone());
        Ok(())
    }

    pub(crate) fn actor_cache(&self) -> PluginActorCache {
        self.plugin_actor_cache.clone()
    }

    pub(crate) fn max_live_plugin_stores(&self) -> usize {
        self.plugin_actor_cache.capacity()
    }

    /// Aggregates validated guest work and host-owned lifecycle facts.
    /// Poison recovery is deliberate: diagnostics must not fail a transaction.
    pub(crate) fn record_transition_counters(&self, counters: WasmTransitionCounters) {
        self.plugin_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accumulate(counters);
    }

    pub(crate) fn transition_counters(&self) -> WasmTransitionCounters {
        *self
            .plugin_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn reset_transition_counters(&self) {
        *self
            .plugin_transition_counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = WasmTransitionCounters::default();
    }

    /// Shares only compiled code. Every file actor created from this factory
    /// receives a distinct Store/instance through `instantiate_actor`.
    pub(crate) async fn load_or_compile_factory(
        &self,
        plugin: &InstalledPlugin,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        if let Some(factory) = self.cached_plugin_factory(&plugin.key, plugin.wasm_hash)? {
            return Ok(factory);
        }
        let compiled = self
            .wasm_runtime
            .compile_component(plugin.wasm.clone(), self.plugin_wasm_limits)
            .await?;
        let mut cache = self
            .plugin_factory_cache
            .lock()
            .map_err(|_| component_cache_lock_error())?;
        if let Some(cached) = cache.get(&plugin.key)
            && cached.wasm_hash == plugin.wasm_hash
        {
            return Ok(Arc::clone(&cached.factory));
        }
        cache.insert(
            plugin.key.clone(),
            CachedPluginFactory {
                wasm_hash: plugin.wasm_hash,
                factory: Arc::clone(&compiled),
            },
        );
        Ok(compiled)
    }

    pub(crate) fn cached_plugin_factory(
        &self,
        plugin_key: &str,
        wasm_hash: BlobId,
    ) -> Result<Option<Arc<dyn WasmComponentFactory>>, LixError> {
        let cache = self
            .plugin_factory_cache
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
    use crate::plugin::runtime::UnsupportedWasmRuntime;

    #[test]
    fn plugin_memory_policy_is_explicit() {
        assert_eq!(WasmLimits::default().max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(
            default_plugin_wasm_limits().max_memory_bytes,
            192 * 1024 * 1024
        );
        assert_eq!(
            DEFAULT_PLUGIN_MEMORY_BYTES * DEFAULT_MAX_LIVE_PLUGIN_STORES as u64,
            1_920 * 1024 * 1024
        );
        assert_eq!(
            default_plugin_wasm_limits().timeout_ms,
            Some(MAX_PLUGIN_EXECUTION_TIMEOUT_MS)
        );
        assert!(plugin_wasm_limits(0).is_err());
        assert_eq!(
            plugin_wasm_limits(192 * 1024 * 1024)
                .expect("custom limit should validate")
                .max_memory_bytes,
            192 * 1024 * 1024
        );
    }

    #[test]
    fn plugin_registry_read_cache_isolated_by_snapshot() {
        let host = PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime));
        let branches = BTreeSet::from(["01920000-0000-7000-8000-0000000000a1".to_string()]);
        let registries = BTreeMap::from([(
            "01920000-0000-7000-8000-0000000000a1".to_string(),
            PluginRegistry::empty(),
        )]);

        assert!(
            host.cached_plugin_registries(7, &branches)
                .expect("inspect empty cache")
                .is_none()
        );
        host.cache_plugin_registries(7, &registries)
            .expect("cache registry");
        assert_eq!(
            host.cached_plugin_registries(7, &branches)
                .expect("read matching snapshot"),
            Some(registries)
        );
        assert!(
            host.cached_plugin_registries(8, &branches)
                .expect("read different snapshot")
                .is_none()
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
    fn runtime_host_aggregates_and_resets_transition_counters() {
        let host = PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime));
        host.record_transition_counters(WasmTransitionCounters {
            packet_pages: 2,
            durable_semantic_changes: 1,
            guest_linear_memory_high_water_bytes: 128,
            host_content_classification_bytes: 10,
            ..WasmTransitionCounters::default()
        });
        host.record_transition_counters(WasmTransitionCounters {
            packet_pages: 3,
            private_document_cache_hits: 1,
            guest_linear_memory_high_water_bytes: 64,
            host_content_classification_bytes: 7,
            ..WasmTransitionCounters::default()
        });

        let counters = host.transition_counters();
        assert_eq!(counters.packet_pages, 5);
        assert_eq!(counters.durable_semantic_changes, 1);
        assert_eq!(counters.private_document_cache_hits, 1);
        assert_eq!(counters.guest_linear_memory_high_water_bytes, 128);
        assert_eq!(counters.host_content_classification_bytes, 17);

        host.reset_transition_counters();
        assert_eq!(
            host.transition_counters(),
            WasmTransitionCounters::default()
        );
    }
}
