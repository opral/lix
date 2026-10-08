use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::binary_cas::BlobId;
use crate::common::LixError;
use crate::{
    plugin::runtime::{WasmComponentFactory, WasmRuntime, WasmTransitionCounters},
    wasm::WasmLimits,
};

use super::compile_cache::{BoundedCompileCache, CompileCacheLimits};

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

const PLUGIN_FACTORY_CACHE_ENTRIES: usize = 16;
const PLUGIN_FACTORY_CACHE_SOURCE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PLUGIN_FACTORY_IN_FLIGHT_GROUPS: usize = 8;
const MAX_PLUGIN_FACTORY_IN_FLIGHT_CALLERS: usize = 32;
const MAX_PLUGIN_FACTORY_IN_FLIGHT_SOURCE_BYTES: u64 = 64 * 1024 * 1024;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PluginFactoryCompileProfile {
    max_memory_bytes: u64,
    max_fuel: Option<u64>,
    timeout_ms: Option<u64>,
}

impl From<WasmLimits> for PluginFactoryCompileProfile {
    fn from(limits: WasmLimits) -> Self {
        Self {
            max_memory_bytes: limits.max_memory_bytes,
            max_fuel: limits.max_fuel,
            timeout_ms: limits.timeout_ms,
        }
    }
}

impl PluginFactoryCompileProfile {
    fn limits(self) -> WasmLimits {
        WasmLimits {
            max_memory_bytes: self.max_memory_bytes,
            max_fuel: self.max_fuel,
            timeout_ms: self.timeout_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct PluginFactoryKey {
    wasm_hash: BlobId,
    column_merger: bool,
    file_projection: bool,
    profile: PluginFactoryCompileProfile,
}

impl PluginFactoryKey {
    fn new(
        wasm_hash: BlobId,
        capabilities: crate::plugin::runtime::PluginCapabilities,
        profile: WasmLimits,
    ) -> Self {
        Self {
            wasm_hash,
            column_merger: capabilities.column_merger,
            file_projection: capabilities.file_projection,
            profile: profile.into(),
        }
    }

    fn capabilities(self) -> crate::plugin::runtime::PluginCapabilities {
        crate::plugin::runtime::PluginCapabilities {
            column_merger: self.column_merger,
            file_projection: self.file_projection,
        }
    }
}

#[derive(Clone, Copy)]
struct PluginFactoryCacheLimits {
    ready_entries: usize,
    ready_source_bytes: u64,
    in_flight_groups: usize,
    in_flight_callers: usize,
    in_flight_source_bytes: u64,
}

struct PluginFactoryCache {
    cache: BoundedCompileCache<PluginFactoryKey, Arc<dyn WasmComponentFactory>>,
}

#[cfg(test)]
type PluginFactoryCacheSnapshot = super::compile_cache::CompileCacheSnapshot;

impl From<PluginFactoryCacheLimits> for CompileCacheLimits {
    fn from(limits: PluginFactoryCacheLimits) -> Self {
        Self {
            ready_entries: limits.ready_entries,
            ready_source_bytes: limits.ready_source_bytes,
            max_in_flight_groups: limits.in_flight_groups,
            max_in_flight_callers: limits.in_flight_callers,
            max_in_flight_source_bytes: limits.in_flight_source_bytes,
        }
    }
}

impl PluginFactoryCache {
    fn new() -> Arc<Self> {
        Self::with_limits(PluginFactoryCacheLimits::ENGINE)
    }

    fn with_limits(limits: PluginFactoryCacheLimits) -> Arc<Self> {
        Arc::new(Self {
            cache: BoundedCompileCache::new(limits.into()),
        })
    }

    fn cached(
        &self,
        key: PluginFactoryKey,
    ) -> Result<Option<Arc<dyn WasmComponentFactory>>, LixError> {
        self.cache.cached(&key)
    }

    async fn load_or_compile(
        &self,
        runtime: &Arc<dyn WasmRuntime>,
        key: PluginFactoryKey,
        source: &[u8],
    ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
        let source_bytes = u64::try_from(source.len()).unwrap_or(u64::MAX);
        let runtime = Arc::clone(runtime);
        self.cache
            .get_or_compile(key, source_bytes, source_bytes, move || async move {
                runtime
                    .compile_component(source.to_vec(), key.profile.limits(), key.capabilities())
                    .await
            })
            .await
    }

    #[cfg(test)]
    fn snapshot(&self) -> PluginFactoryCacheSnapshot {
        self.cache.snapshot()
    }
}

impl PluginFactoryCacheLimits {
    const ENGINE: Self = Self {
        ready_entries: PLUGIN_FACTORY_CACHE_ENTRIES,
        ready_source_bytes: PLUGIN_FACTORY_CACHE_SOURCE_BYTES,
        in_flight_groups: MAX_PLUGIN_FACTORY_IN_FLIGHT_GROUPS,
        in_flight_callers: MAX_PLUGIN_FACTORY_IN_FLIGHT_CALLERS,
        in_flight_source_bytes: MAX_PLUGIN_FACTORY_IN_FLIGHT_SOURCE_BYTES,
    };
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
    plugin_factory_cache: Arc<PluginFactoryCache>,
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
            plugin_factory_cache: PluginFactoryCache::new(),
            plugin_wasm_limits: plugin_wasm_limits(max_memory_bytes)?,
            plugin_actor_cache: PluginActorCache::new(max_live_stores)?,
            plugin_transition_counters: Arc::new(Mutex::new(WasmTransitionCounters::default())),
            plugin_catalog_cache: Arc::new(Mutex::new(PluginCatalogCache::default())),
            plugin_registry_read_cache: Arc::new(Mutex::new(PluginRegistryReadCache::default())),
            plugin_generation_fence: Arc::new(tokio::sync::RwLock::new(())),
        })
    }

    /// Separate engines have independent session/observation identities. Retain
    /// their configured runtime and budgets without sharing actor state.
    pub(crate) fn fork_for_storage_session(&self) -> Self {
        Self::new_with_limits(
            self.wasm_runtime.clone(),
            self.plugin_wasm_limits.max_memory_bytes,
            self.plugin_actor_cache.capacity(),
        )
        .expect("existing plugin resource limits are valid")
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
        let capabilities = crate::plugin::runtime::PluginCapabilities {
            column_merger: plugin.has_column_merger(),
            file_projection: plugin.has_file_projection(),
        };
        let factory = match self.cached_plugin_factory(wasm_hash, capabilities)? {
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
        let key = PluginFactoryKey::new(wasm_hash, plugin.capabilities, self.plugin_wasm_limits);
        if let Some(factory) = self.plugin_factory_cache.cached(key)? {
            return Ok(factory);
        }
        let wasm = plugin.wasm.as_deref().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!("plugin '{}' executable bytes are unavailable", plugin.key),
            )
        })?;
        self.plugin_factory_cache
            .load_or_compile(&self.wasm_runtime, key, wasm)
            .await
    }

    pub(crate) fn cached_plugin_factory(
        &self,
        wasm_hash: BlobId,
        capabilities: crate::plugin::runtime::PluginCapabilities,
    ) -> Result<Option<Arc<dyn WasmComponentFactory>>, LixError> {
        self.plugin_factory_cache.cached(PluginFactoryKey::new(
            wasm_hash,
            capabilities,
            self.plugin_wasm_limits,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::runtime::{PluginCapabilities, UnsupportedWasmRuntime, WasmComponentActor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct TestFactory;

    #[async_trait::async_trait]
    impl WasmComponentFactory for TestFactory {
        async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentActor>, LixError> {
            Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "test factory does not instantiate actors",
            ))
        }
    }

    struct FakeWasmRuntime {
        compile_calls: AtomicUsize,
        failures_left: AtomicUsize,
        release: Option<Arc<tokio::sync::Semaphore>>,
        started: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl FakeWasmRuntime {
        fn new(
            failures: usize,
            blocked: bool,
        ) -> (Arc<Self>, tokio::sync::mpsc::UnboundedReceiver<()>) {
            let (started, started_rx) = tokio::sync::mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    compile_calls: AtomicUsize::new(0),
                    failures_left: AtomicUsize::new(failures),
                    release: blocked.then(|| Arc::new(tokio::sync::Semaphore::new(0))),
                    started,
                }),
                started_rx,
            )
        }

        fn compile_calls(&self) -> usize {
            self.compile_calls.load(Ordering::SeqCst)
        }

        fn take_failure(&self) -> bool {
            self.failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        }
    }

    #[async_trait::async_trait]
    impl WasmRuntime for FakeWasmRuntime {
        async fn compile_component(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
            _capabilities: PluginCapabilities,
        ) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
            self.compile_calls.fetch_add(1, Ordering::SeqCst);
            let _ = self.started.send(());
            if let Some(release) = &self.release {
                release
                    .acquire()
                    .await
                    .map_err(|_| LixError::new(LixError::CODE_INTERNAL_ERROR, "test gate closed"))?
                    .forget();
            }
            if self.take_failure() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "test compilation failure",
                ));
            }
            Ok(Arc::new(TestFactory))
        }
    }

    fn test_key(bytes: &[u8], capabilities: PluginCapabilities) -> PluginFactoryKey {
        PluginFactoryKey::new(
            BlobId::from_content(bytes),
            capabilities,
            default_plugin_wasm_limits(),
        )
    }

    fn test_cache_limits(
        ready_entries: usize,
        ready_source_bytes: u64,
        in_flight_groups: usize,
        in_flight_callers: usize,
        in_flight_source_bytes: u64,
    ) -> PluginFactoryCacheLimits {
        PluginFactoryCacheLimits {
            ready_entries,
            ready_source_bytes,
            in_flight_groups,
            in_flight_callers,
            in_flight_source_bytes,
        }
    }

    async fn wait_for_callers(cache: &PluginFactoryCache, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if cache.snapshot().in_flight_callers == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("calls should reach the bounded flight");
    }

    fn installed_test_plugin(
        key: &str,
        wasm_hash: BlobId,
        wasm: Option<Vec<u8>>,
        capabilities: PluginCapabilities,
    ) -> InstalledPlugin {
        InstalledPlugin {
            key: key.to_owned(),
            runtime: crate::plugin::runtime::PluginRuntime::WasmComponent,
            api_version: "2".to_owned(),
            capabilities,
            path_glob: None,
            content: None,
            entry: None,
            schema_keys: Vec::new(),
            manifest_json: "{}".to_owned(),
            wasm_hash: Some(wasm_hash),
            wasm,
        }
    }

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

    #[tokio::test]
    async fn plugin_factory_cache_singleflights_same_key_and_accounts_callers() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(16, 128, 8, 16, 128));
        let (fake_runtime, mut started) = FakeWasmRuntime::new(0, true);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let source = Arc::<[u8]>::from(&b"same component"[..]);
        let key = test_key(&source, PluginCapabilities::default());
        let callers = 8;

        let first_cache = Arc::clone(&cache);
        let first_runtime = Arc::clone(&runtime);
        let first_source = Arc::clone(&source);
        let first = tokio::spawn(async move {
            first_cache
                .load_or_compile(&first_runtime, key, &first_source)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("first compilation should start")
            .expect("runtime should report compilation start");

        let mut tasks = vec![first];
        for _ in 1..callers {
            let cache = Arc::clone(&cache);
            let runtime = Arc::clone(&runtime);
            let source = Arc::clone(&source);
            tasks.push(tokio::spawn(async move {
                cache.load_or_compile(&runtime, key, &source).await
            }));
        }
        wait_for_callers(&cache, callers).await;
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.in_flight_groups, 1);
        assert_eq!(
            snapshot.in_flight_source_bytes,
            source.len() as u64 * (callers as u64 + 1)
        );
        assert_eq!(fake_runtime.compile_calls(), 1);

        fake_runtime
            .release
            .as_ref()
            .expect("test runtime is blocked")
            .add_permits(1);
        let mut factories = Vec::new();
        for task in tasks {
            factories.push(
                task.await
                    .expect("compile caller should finish")
                    .expect("shared compile should succeed"),
            );
        }
        assert!(
            factories
                .iter()
                .all(|factory| Arc::ptr_eq(factory, &factories[0]))
        );
        assert_eq!(fake_runtime.compile_calls(), 1);
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.in_flight_groups, 0);
        assert_eq!(snapshot.in_flight_source_bytes, 0);
        assert_eq!(snapshot.in_flight_callers, 0);
    }

    #[tokio::test]
    async fn plugin_factory_cache_retries_failed_compilation() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(4, 64, 4, 8, 64));
        let (fake_runtime, _started) = FakeWasmRuntime::new(1, false);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let source = b"retry after failure";
        let key = test_key(source, PluginCapabilities::default());

        assert!(cache.load_or_compile(&runtime, key, source).await.is_err());
        let factory = cache
            .load_or_compile(&runtime, key, source)
            .await
            .expect("later call should retry the failed compile");
        let cached = cache.cached(key).expect("inspect ready cache");

        assert_eq!(fake_runtime.compile_calls(), 2);
        assert!(cached.is_some_and(|cached| Arc::ptr_eq(&cached, &factory)));
        assert_eq!(cache.snapshot().in_flight_callers, 0);
    }

    #[tokio::test]
    async fn plugin_factory_cache_isolates_capabilities_for_identical_wasm() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(4, 128, 4, 8, 128));
        let (fake_runtime, _started) = FakeWasmRuntime::new(0, false);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let source = b"same hash, distinct link capabilities";
        let first_key = test_key(source, PluginCapabilities::default());
        let second_key = test_key(
            source,
            PluginCapabilities {
                column_merger: true,
                file_projection: false,
            },
        );

        let first = cache
            .load_or_compile(&runtime, first_key, source)
            .await
            .expect("first capability profile should compile");
        let second = cache
            .load_or_compile(&runtime, second_key, source)
            .await
            .expect("second capability profile should compile separately");

        assert_eq!(fake_runtime.compile_calls(), 2);
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(cache.cached(first_key).expect("first cache key").is_some());
        assert!(
            cache
                .cached(second_key)
                .expect("second cache key")
                .is_some()
        );
    }

    #[tokio::test]
    async fn plugin_factory_cache_bounds_lru_source_bytes_and_skips_oversize() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(2, 10, 2, 4, 64));
        let (fake_runtime, _started) = FakeWasmRuntime::new(0, false);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let caps = PluginCapabilities::default();
        let first_source = [1_u8; 5];
        let second_source = [2_u8; 5];
        let third_source = [3_u8; 1];
        let first_key = test_key(&first_source, caps);
        let second_key = test_key(&second_source, caps);
        let third_key = test_key(&third_source, caps);

        let first = cache
            .load_or_compile(&runtime, first_key, &first_source)
            .await
            .expect("first component compiles");
        cache
            .load_or_compile(&runtime, second_key, &second_source)
            .await
            .expect("second component compiles");
        let warm = cache
            .load_or_compile(&runtime, first_key, &first_source)
            .await
            .expect("LRU hit should return the first factory");
        assert!(Arc::ptr_eq(&first, &warm));
        cache
            .load_or_compile(&runtime, third_key, &third_source)
            .await
            .expect("third component should evict the least-recent entry");

        let snapshot = cache.snapshot();
        assert_eq!(snapshot.ready_entries, 2);
        assert_eq!(snapshot.ready_source_bytes, 6);
        assert!(
            cache
                .cached(first_key)
                .expect("first remains warm")
                .is_some()
        );
        assert!(cache.cached(second_key).expect("oldest entry").is_none());
        assert!(cache.cached(third_key).expect("third entry").is_some());

        let oversize_source = [9_u8; 11];
        let oversize_key = test_key(&oversize_source, caps);
        cache
            .load_or_compile(&runtime, oversize_key, &oversize_source)
            .await
            .expect("oversize components can still compile");
        assert!(
            cache
                .cached(oversize_key)
                .expect("oversize entries are not retained")
                .is_none()
        );
        assert_eq!(cache.snapshot().ready_source_bytes, 6);
        assert_eq!(fake_runtime.compile_calls(), 4);
    }

    #[tokio::test]
    async fn plugin_factory_cache_rejects_saturated_distinct_keys_and_callers() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(4, 64, 1, 2, 64));
        let (fake_runtime, mut started) = FakeWasmRuntime::new(0, true);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let source = b"active component";
        let key = test_key(source, PluginCapabilities::default());
        let first_cache = Arc::clone(&cache);
        let first_runtime = Arc::clone(&runtime);
        let first = tokio::spawn(async move {
            first_cache
                .load_or_compile(&first_runtime, key, source)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("compile should start")
            .expect("runtime should report start");

        let join_cache = Arc::clone(&cache);
        let join_runtime = Arc::clone(&runtime);
        let joiner =
            tokio::spawn(
                async move { join_cache.load_or_compile(&join_runtime, key, source).await },
            );
        wait_for_callers(&cache, 2).await;
        let distinct_source = b"second active component";
        let distinct_key = test_key(distinct_source, PluginCapabilities::default());
        let error = match cache
            .load_or_compile(&runtime, distinct_key, distinct_source)
            .await
        {
            Ok(_) => panic!("group limit should reject a distinct compile"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);

        let error = match cache.load_or_compile(&runtime, key, source).await {
            Ok(_) => panic!("caller limit should reject another waiter"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);

        fake_runtime
            .release
            .as_ref()
            .expect("test runtime is blocked")
            .add_permits(1);
        first.await.expect("first task").expect("compile succeeds");
        joiner
            .await
            .expect("joiner task")
            .expect("joined compile succeeds");
        assert_eq!(fake_runtime.compile_calls(), 1);
        assert_eq!(cache.snapshot().in_flight_callers, 0);
    }

    #[tokio::test]
    async fn plugin_factory_cache_retries_when_compile_initializer_is_cancelled() {
        let cache = PluginFactoryCache::with_limits(test_cache_limits(4, 64, 2, 4, 64));
        let (fake_runtime, mut started) = FakeWasmRuntime::new(0, true);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let source = b"cancelled initializer";
        let key = test_key(source, PluginCapabilities::default());
        let first_cache = Arc::clone(&cache);
        let first_runtime = Arc::clone(&runtime);
        let first = tokio::spawn(async move {
            first_cache
                .load_or_compile(&first_runtime, key, source)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("first compile should start")
            .expect("runtime should report start");

        let second_cache = Arc::clone(&cache);
        let second_runtime = Arc::clone(&runtime);
        let second = tokio::spawn(async move {
            second_cache
                .load_or_compile(&second_runtime, key, source)
                .await
        });
        wait_for_callers(&cache, 2).await;
        first.abort();
        assert!(first.await.is_err_and(|error| error.is_cancelled()));

        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("remaining waiter should retry initialization")
            .expect("runtime should report retry start");
        fake_runtime
            .release
            .as_ref()
            .expect("test runtime is blocked")
            .add_permits(1);
        second
            .await
            .expect("remaining task should finish")
            .expect("remaining waiter should compile successfully");

        assert_eq!(fake_runtime.compile_calls(), 2);
        let snapshot = cache.snapshot();
        assert_eq!(snapshot.in_flight_groups, 0);
        assert_eq!(snapshot.in_flight_source_bytes, 0);
        assert_eq!(snapshot.in_flight_callers, 0);
    }

    #[tokio::test]
    async fn plugin_factory_cache_bounds_inflight_bytes_and_allows_one_exclusive_oversize() {
        let (fake_runtime, _started) = FakeWasmRuntime::new(0, false);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let cache = PluginFactoryCache::with_limits(test_cache_limits(1, 4, 2, 4, 10));
        let oversize_source = [1_u8; 8];
        let oversize_key = test_key(&oversize_source, PluginCapabilities::default());
        cache
            .load_or_compile(&runtime, oversize_key, &oversize_source)
            .await
            .expect("one oversized source can compile on its own");
        assert_eq!(fake_runtime.compile_calls(), 1);
        assert!(
            cache
                .cached(oversize_key)
                .expect("oversize is not cached")
                .is_none()
        );

        let (blocked_runtime, mut started) = FakeWasmRuntime::new(0, true);
        let blocked_runtime_trait: Arc<dyn WasmRuntime> = blocked_runtime.clone();
        let cache = PluginFactoryCache::with_limits(test_cache_limits(2, 64, 2, 4, 10));
        let first_source = b"1234";
        let first_key = test_key(first_source, PluginCapabilities::default());
        let first_cache = Arc::clone(&cache);
        let first_runtime = Arc::clone(&blocked_runtime_trait);
        let first = tokio::spawn(async move {
            first_cache
                .load_or_compile(&first_runtime, first_key, first_source)
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("first compile should start")
            .expect("runtime should report start");
        let second_source = b"xy";
        let second_key = test_key(second_source, PluginCapabilities::default());
        let error = match cache
            .load_or_compile(&blocked_runtime_trait, second_key, second_source)
            .await
        {
            Ok(_) => panic!("aggregate source budget should reject a second group"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        blocked_runtime
            .release
            .as_ref()
            .expect("test runtime is blocked")
            .add_permits(1);
        first
            .await
            .expect("first task should finish")
            .expect("first compile should succeed");
        assert_eq!(cache.snapshot().in_flight_source_bytes, 0);
    }

    #[tokio::test]
    async fn runtime_host_singleflights_alias_plugins_by_exact_compile_identity() {
        let (fake_runtime, mut started) = FakeWasmRuntime::new(0, true);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let host = PluginRuntimeHost::new(runtime);
        let source = b"shared factory bytes".to_vec();
        let hash = BlobId::from_content(&source);
        let capabilities = PluginCapabilities {
            column_merger: true,
            file_projection: false,
        };
        let first_plugin = installed_test_plugin(
            "plugin_first_alias",
            hash,
            Some(source.clone()),
            capabilities,
        );
        let second_plugin = installed_test_plugin(
            "plugin_second_alias",
            hash,
            Some(source.clone()),
            capabilities,
        );

        let first_host = host.clone();
        let first =
            tokio::spawn(async move { first_host.load_or_compile_factory(&first_plugin).await });
        tokio::time::timeout(Duration::from_secs(2), started.recv())
            .await
            .expect("first host compile should start")
            .expect("runtime should report first compile");
        let second_host = host.clone();
        let second_barrier = Arc::new(tokio::sync::Barrier::new(2));
        let second_test_barrier = Arc::clone(&second_barrier);
        let (second_started_tx, mut second_started_rx) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(async move {
            second_barrier.wait().await;
            let _ = second_started_tx.send(());
            second_host.load_or_compile_factory(&second_plugin).await
        });
        second_test_barrier.wait().await;
        tokio::time::timeout(Duration::from_secs(2), &mut second_started_rx)
            .await
            .expect("alias caller should enter the host path")
            .expect("alias caller should report entry");
        let duplicate_compile = fake_runtime.compile_calls() != 1;
        fake_runtime
            .release
            .as_ref()
            .expect("test runtime is blocked")
            .add_permits(2);
        let first = first
            .await
            .expect("first host call should finish")
            .expect("first host compile should succeed");
        let second = second
            .await
            .expect("alias host call should finish")
            .expect("alias host call should share the compiled factory");

        assert!(
            !duplicate_compile,
            "alias should not start a second compile"
        );
        assert_eq!(fake_runtime.compile_calls(), 1);
        assert!(Arc::ptr_eq(&first, &second));

        let source_free_alias =
            installed_test_plugin("plugin_source_free_alias", hash, None, capabilities);
        let warm = host
            .load_or_compile_factory(&source_free_alias)
            .await
            .expect("warm hit should not need to clone or reload component bytes");
        assert!(Arc::ptr_eq(&first, &warm));
        assert_eq!(fake_runtime.compile_calls(), 1);
    }

    #[tokio::test]
    async fn runtime_host_factory_lru_evicts_churn_but_active_arcs_survive() {
        let (fake_runtime, _started) = FakeWasmRuntime::new(0, false);
        let runtime: Arc<dyn WasmRuntime> = fake_runtime.clone();
        let host = PluginRuntimeHost::new(runtime);
        let capabilities = PluginCapabilities::default();
        let first_bytes = b"historical plugin component".to_vec();
        let first_hash = BlobId::from_content(&first_bytes);
        let first_plugin = installed_test_plugin(
            "historical_plugin",
            first_hash,
            Some(first_bytes.clone()),
            capabilities,
        );
        let active_factory = host
            .load_or_compile_factory(&first_plugin)
            .await
            .expect("first component should compile");
        let active_reference = Arc::clone(&active_factory);

        for index in 0..PLUGIN_FACTORY_CACHE_ENTRIES {
            let bytes = format!("churn component {index}").into_bytes();
            let hash = BlobId::from_content(&bytes);
            let plugin = installed_test_plugin(
                &format!("historical_plugin_{index}"),
                hash,
                Some(bytes),
                capabilities,
            );
            host.load_or_compile_factory(&plugin)
                .await
                .expect("churn component should compile");
        }

        assert!(
            host.cached_plugin_factory(first_hash, capabilities)
                .expect("inspect evicted factory")
                .is_none()
        );
        assert!(Arc::ptr_eq(&active_factory, &active_reference));
        host.load_or_compile_factory(&first_plugin)
            .await
            .expect("evicted component can be compiled again");
        assert_eq!(
            fake_runtime.compile_calls(),
            PLUGIN_FACTORY_CACHE_ENTRIES + 2
        );
    }
}
