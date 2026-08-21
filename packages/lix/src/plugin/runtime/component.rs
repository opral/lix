use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::binary_cas::BlobId;
use crate::common::LixError;
use crate::{
    plugin::runtime::{WasmComponentFactory, WasmRuntime, WasmTransitionCounters},
    wasm::WasmLimits,
};

use super::{
    CompiledPluginCatalog, DEFAULT_MAX_LIVE_PLUGIN_STORES, InstalledPlugin, PluginActorCache,
    PluginCatalogCache, PluginRegistry, PluginRegistryEntry, ValidatedColumnMergeTransition,
    VecColumnMergeSource, WasmColumnMergeUpdate, WasmHostColumnMerge, WasmTransitionLimits,
    drain_column_merge_transition_results,
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

#[derive(Default)]
struct PluginRegistryReadCache {
    snapshot: Option<u128>,
    registries: BTreeMap<String, PluginRegistry>,
    durable_registries: BTreeMap<String, (String, PluginRegistry)>,
}

#[derive(Clone)]
pub(crate) struct PluginRuntimeHost {
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_factory_cache: Arc<Mutex<BTreeMap<String, CachedPluginFactory>>>,
    plugin_wasm_limits: WasmLimits,
    plugin_actor_cache: PluginActorCache,
    plugin_transition_counters: Arc<Mutex<WasmTransitionCounters>>,
    plugin_catalog_cache: Arc<Mutex<PluginCatalogCache>>,
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

    pub(crate) fn cached_plugin_registry(
        &self,
        branch_id: &str,
        change_id: &str,
    ) -> Result<Option<PluginRegistry>, LixError> {
        let cache = self.plugin_registry_read_cache.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin registry read cache lock poisoned",
            )
        })?;
        Ok(cache
            .durable_registries
            .get(branch_id)
            .filter(|(cached_change_id, _)| cached_change_id == change_id)
            .map(|(_, registry)| registry.clone()))
    }

    pub(crate) fn cache_plugin_registry(
        &self,
        branch_id: &str,
        change_id: &str,
        registry: &PluginRegistry,
    ) -> Result<(), LixError> {
        let mut cache = self.plugin_registry_read_cache.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin registry read cache lock poisoned",
            )
        })?;
        cache.durable_registries.insert(
            branch_id.to_owned(),
            (change_id.to_owned(), registry.clone()),
        );
        Ok(())
    }

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
        Ok(branch_ids
            .iter()
            .map(|branch_id| {
                cache
                    .registries
                    .get(branch_id)
                    .cloned()
                    .map(|registry| (branch_id.clone(), registry))
            })
            .collect())
    }

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

    /// Invokes one pinned plugin generation for same-column overlaps. The
    /// operation is row-first: `file_id` may be absent and no file descriptor,
    /// path, projection state, or document actor is required.
    pub(crate) async fn merge_columns(
        &self,
        plugin: &PluginRegistryEntry,
        wasm: Option<Vec<u8>>,
        merges: Vec<WasmHostColumnMerge>,
        limits: WasmTransitionLimits,
    ) -> Result<ValidatedColumnMergeTransition, LixError> {
        if !plugin.has_column_merger() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' has no column-merger capability", plugin.key()),
            ));
        }
        if merges.is_empty() {
            return Ok(ValidatedColumnMergeTransition {
                results: Vec::new(),
                counters: WasmTransitionCounters::default(),
            });
        }
        let wasm_hash = BlobId::from_hex(plugin.wasm_blob_hash().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' has no column-merger component", plugin.key()),
            )
        })?)?;
        let factory = match self.cached_plugin_factory(plugin.key(), wasm_hash)? {
            Some(factory) => factory,
            None => {
                let wasm = wasm.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PLUGIN,
                        format!(
                            "plugin '{}' component bytes are required on cache miss",
                            plugin.key()
                        ),
                    )
                })?;
                let installed = plugin.to_installed_plugin(Some(wasm))?;
                self.load_or_compile_factory(&installed).await?
            }
        };
        let _store_permit = self.plugin_actor_cache.admit_store()?;
        let mut actor = factory.instantiate_actor().await?;
        let expected_count = merges.len();
        let source = VecColumnMergeSource::new(merges, limits)?;
        let transition = match actor
            .merge_columns(
                limits,
                WasmColumnMergeUpdate {
                    merges: Box::new(source),
                },
            )
            .await
        {
            Ok(transition) => transition,
            Err(error) => {
                let _ = actor.retire().await;
                return Err(error);
            }
        };
        let result = drain_column_merge_transition_results(
            actor.as_mut(),
            transition,
            expected_count,
            limits,
        )
        .await;
        let retire = actor.retire().await;
        match (result, retire) {
            (Ok(validated), Ok(())) => {
                self.record_transition_counters(validated.counters);
                Ok(validated)
            }
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        }
    }

    /// Shares only compiled code. Every file actor created from this factory
    /// receives a distinct Store/instance through `instantiate_actor`.
    pub(crate) async fn load_or_compile_factory(
        &self,
        plugin: &InstalledPlugin,
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        let wasm_hash = plugin.wasm_hash.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' has no executable component", plugin.key),
            )
        })?;
        let wasm = plugin.wasm.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' executable bytes are unavailable", plugin.key),
            )
        })?;
        if let Some(factory) = self.cached_plugin_factory(&plugin.key, wasm_hash)? {
            return Ok(factory);
        }
        let compiled = self
            .wasm_runtime
            .compile_component(wasm, self.plugin_wasm_limits, plugin.capabilities)
            .await?;
        let mut cache = self
            .plugin_factory_cache
            .lock()
            .map_err(|_| component_cache_lock_error())?;
        if let Some(cached) = cache.get(&plugin.key)
            && cached.wasm_hash == wasm_hash
        {
            return Ok(Arc::clone(&cached.factory));
        }
        cache.insert(
            plugin.key.clone(),
            CachedPluginFactory {
                wasm_hash,
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
    fn plugin_registry_read_cache_isolated_by_durable_change() {
        let host = PluginRuntimeHost::new(Arc::new(UnsupportedWasmRuntime));
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let registry = PluginRegistry::empty();

        assert!(
            host.cached_plugin_registry(branch_id, "change-7")
                .expect("inspect empty cache")
                .is_none()
        );
        host.cache_plugin_registry(branch_id, "change-7", &registry)
            .expect("cache registry");
        assert_eq!(
            host.cached_plugin_registry(branch_id, "change-7")
                .expect("read matching durable change"),
            Some(registry)
        );
        assert!(
            host.cached_plugin_registry(branch_id, "change-8")
                .expect("read different durable change")
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
