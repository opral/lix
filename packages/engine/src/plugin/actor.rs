//! Persistent, failure-isolated v2 plugin actors.
//!
//! A compiled component may be shared, but a mutable Component instance and
//! its document handles belong to exactly one branch/file actor.  This cache
//! deliberately keys path, incarnation, and plugin generation in addition to
//! the file id: none of those identities may be inferred from equal bytes.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

use super::incremental::FileBytesSha256;
use crate::wasm::{WasmComponentV2Actor, WasmDocumentHandle};
use crate::{Blob, LixError};

pub(crate) const DEFAULT_MAX_LIVE_PLUGIN_FILE_ACTORS: usize = 4;
// One predecessor is enough for the required two-reader serialization while
// keeping each file actor's retained working set bounded.
pub(crate) const DEFAULT_MAX_PLUGIN_FILE_HISTORY: usize = 1;

/// Complete authority identity for one mutable guest instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PluginActorKey {
    pub(crate) branch_id: String,
    pub(crate) file_id: String,
    pub(crate) path: String,
    pub(crate) owner_change_id: String,
    pub(crate) plugin_key: String,
    pub(crate) plugin_generation: String,
}

/// An exact private view delivered to one session.
///
/// The semantic root remains authority: two roots that happen to render
/// identical bytes intentionally produce different observations. The cached
/// digest only proves whether transport provenance names this exact byte view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginObservation {
    key: PluginActorKey,
    actor_nonce: u64,
    revision: u64,
    semantic_root: Arc<str>,
    bytes_sha256: FileBytesSha256,
}

impl PluginObservation {
    pub(crate) fn key(&self) -> &PluginActorKey {
        &self.key
    }

    pub(crate) fn semantic_root(&self) -> &str {
        &self.semantic_root
    }

    pub(crate) fn bytes_sha256(&self) -> FileBytesSha256 {
        self.bytes_sha256
    }
}

struct PluginActorAcceptedState {
    store: PluginActorStore,
    document: WasmDocumentHandle,
    bytes: Blob,
    bytes_sha256: FileBytesSha256,
    semantic_root: Arc<str>,
    history: VecDeque<PluginActorHistoricalState>,
}

/// One instantiated Component Store together with the admission token that
/// keeps it within the workspace-wide live-Store bound.
///
/// Field order is deliberate: Rust drops fields in declaration order, so the
/// actor (and its Wasmtime Store) is destroyed before the permit is returned.
pub(crate) struct PluginActorStore {
    actor: Box<dyn WasmComponentV2Actor>,
    _store_permit: PluginActorStorePermit,
}

impl PluginActorStore {
    pub(crate) fn new(
        actor: Box<dyn WasmComponentV2Actor>,
        store_permit: PluginActorStorePermit,
    ) -> Self {
        Self {
            actor,
            _store_permit: store_permit,
        }
    }

    pub(crate) fn actor_mut(&mut self) -> &mut dyn WasmComponentV2Actor {
        self.actor.as_mut()
    }
}

struct PluginActorHistoricalState {
    revision: u64,
    document: WasmDocumentHandle,
    bytes: Blob,
    bytes_sha256: FileBytesSha256,
    semantic_root: Arc<str>,
}

struct PluginActorSlot {
    nonce: u64,
    revision: AtomicU64,
    last_used: AtomicU64,
    retired: AtomicBool,
    state: Arc<AsyncMutex<PluginActorAcceptedState>>,
}

impl PluginActorSlot {
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}

struct PluginActorCacheState {
    actors: BTreeMap<PluginActorKey, Arc<PluginActorSlot>>,
    clock: u64,
    next_nonce: u64,
}

/// Workspace-local index and hard admission bound for per-file actors.
#[derive(Clone)]
pub(crate) struct PluginActorCache {
    capacity: NonZeroUsize,
    store_admission: Arc<Semaphore>,
    state: Arc<Mutex<PluginActorCacheState>>,
    cold_open_gate: Arc<AsyncMutex<()>>,
}

/// RAII admission for exactly one live Component Store.
///
/// The token starts before instantiation and moves into either a pending
/// publication or an installed actor slot. It releases only after the Store
/// itself is dropped, including when a lease outlives cache eviction.
pub(crate) struct PluginActorStorePermit {
    _permit: OwnedSemaphorePermit,
}

pub(crate) enum PluginActorColdOpen {
    Ready(PluginObservation),
    Build(PluginActorColdInstall),
}

pub(crate) struct PluginActorColdInstall {
    key: PluginActorKey,
    expected_stale: Option<PluginActorExpectedStale>,
}

struct PluginActorExpectedStale {
    slot: Arc<PluginActorSlot>,
    revision: u64,
    semantic_root: Arc<str>,
}

impl Default for PluginActorCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_LIVE_PLUGIN_FILE_ACTORS)
            .expect("the default plugin actor capacity is nonzero")
    }
}

impl PluginActorCache {
    pub(crate) fn new(capacity: usize) -> Result<Self, LixError> {
        let capacity = NonZeroUsize::new(capacity).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "plugin actor cache capacity must be positive",
            )
        })?;
        Ok(Self {
            capacity,
            store_admission: Arc::new(Semaphore::new(capacity.get())),
            state: Arc::new(Mutex::new(PluginActorCacheState {
                actors: BTreeMap::new(),
                clock: 0,
                next_nonce: 1,
            })),
            cold_open_gate: Arc::new(AsyncMutex::new(())),
        })
    }

    /// Serializes cold actor construction. The gate is workspace-wide rather
    /// than per-key because cold opens are uncommon and may otherwise retain
    /// multiple full semantic snapshots plus Wasm Stores concurrently.
    pub(crate) async fn cold_open_guard(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.cold_open_gate).lock_owned().await
    }

    /// Reserves one workspace-wide live Store slot before a Component actor is
    /// instantiated. This intentionally fails fast instead of waiting: a
    /// transaction may already retain a pending actor through its durable
    /// commit point, so waiting could deadlock the transaction against itself.
    pub(crate) fn admit_store(&self) -> Result<PluginActorStorePermit, LixError> {
        loop {
            if let Some(permit) = self.try_acquire_store()? {
                return Ok(permit);
            }
            if !self.evict_one_idle_slot() {
                return Err(plugin_store_resource_limit(self.capacity));
            }
        }
    }

    /// Cold replacement additionally knows the exact stale slot it may
    /// supersede. At capacity, release an idle captured predecessor before
    /// building the candidate; a leased predecessor remains live and causes a
    /// deterministic resource-limit error instead of a temporary overcommit.
    pub(crate) fn admit_cold_store(
        &self,
        cold_install: &mut PluginActorColdInstall,
    ) -> Result<PluginActorStorePermit, LixError> {
        if let Some(permit) = self.try_acquire_store()? {
            return Ok(permit);
        }
        if self.drop_detached_retired_cold_predecessor(cold_install) {
            return self.admit_store();
        }
        if self.evict_idle_cold_predecessor(cold_install) {
            return self.admit_store();
        }
        self.admit_store()
    }

    /// Captures the exact same-key slot that a cold open is allowed to replace.
    ///
    /// The token is compare-and-replace authority, not general cache authority:
    /// publication succeeds only if that slot still has the same revision and
    /// semantic root. A concurrent warm commit or cold install therefore wins
    /// without being clobbered by the slower builder.
    pub(crate) async fn prepare_cold_open(
        &self,
        key: &PluginActorKey,
        semantic_root: &str,
    ) -> Result<PluginActorColdOpen, LixError> {
        loop {
            let slot = match self.lookup_slot(key) {
                Ok(slot) => slot,
                Err(error) if error.code == LixError::CODE_PLUGIN_OBSERVATION_STALE => {
                    return Ok(PluginActorColdOpen::Build(PluginActorColdInstall {
                        key: key.clone(),
                        expected_stale: None,
                    }));
                }
                Err(error) => return Err(error),
            };
            let accepted = Arc::clone(&slot.state).lock_owned().await;
            if slot.retired.load(Ordering::Acquire) {
                drop(accepted);
                self.remove_if_same(key, &slot);
                continue;
            }
            let revision = slot.revision.load(Ordering::Acquire);
            if accepted.semantic_root.as_ref() == semantic_root {
                return Ok(PluginActorColdOpen::Ready(PluginObservation {
                    key: key.clone(),
                    actor_nonce: slot.nonce,
                    revision,
                    semantic_root: Arc::clone(&accepted.semantic_root),
                    bytes_sha256: accepted.bytes_sha256,
                }));
            }
            let stale_root = Arc::clone(&accepted.semantic_root);
            drop(accepted);
            return Ok(PluginActorColdOpen::Build(PluginActorColdInstall {
                key: key.clone(),
                expected_stale: Some(PluginActorExpectedStale {
                    slot,
                    revision,
                    semantic_root: stale_root,
                }),
            }));
        }
    }

    /// Publishes an already-opened document. Callers invoke this only after
    /// the semantic state and its rendered bytes are durably committed.
    pub(crate) fn install(
        &self,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        semantic_root: impl Into<Arc<str>>,
    ) -> PluginObservation {
        let semantic_root = semantic_root.into();
        let bytes_sha256 = FileBytesSha256::compute(&bytes);
        let mut state = self.lock();
        state.clock = state.clock.wrapping_add(1);
        let last_used = state.clock;
        let nonce = state.next_nonce;
        state.next_nonce = state.next_nonce.wrapping_add(1).max(1);
        let slot = Arc::new(PluginActorSlot {
            nonce,
            revision: AtomicU64::new(1),
            last_used: AtomicU64::new(last_used),
            retired: AtomicBool::new(false),
            state: Arc::new(AsyncMutex::new(PluginActorAcceptedState {
                store,
                document,
                bytes,
                bytes_sha256,
                semantic_root: Arc::clone(&semantic_root),
                history: VecDeque::new(),
            })),
        });
        if let Some(previous) = state.actors.insert(key.clone(), Arc::clone(&slot)) {
            previous.retire();
        }
        PluginObservation {
            key,
            actor_nonce: nonce,
            revision: 1,
            semantic_root,
            bytes_sha256,
        }
    }

    /// Publishes a cold-opened snapshot only while the key is still vacant or
    /// the exact stale slot captured by `prepare_cold_open` remains unchanged.
    /// A concurrently committed actor is authoritative and is never replaced
    /// by the slower cold candidate. The losing Store is explicitly retired,
    /// then the caller observes the winner only if it represents the same
    /// semantic root.
    pub(crate) async fn install_cold_if_absent(
        &self,
        cold_install: PluginActorColdInstall,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        bytes_sha256: FileBytesSha256,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<PluginObservation, LixError> {
        let semantic_root = semantic_root.into();
        if cold_install.key != key {
            let mut store = store;
            let _ = store.actor.drop_document(document).await;
            let _ = store.actor.retire().await;
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cold plugin actor install token belongs to a different key",
            ));
        }
        let mut candidate = Some((store, document, bytes, bytes_sha256));
        let expected_guard = match &cold_install.expected_stale {
            Some(expected) => Some(Arc::clone(&expected.slot.state).lock_owned().await),
            None => None,
        };
        let installed = {
            let mut state = self.lock();
            if state
                .actors
                .get(&key)
                .is_some_and(|slot| slot.retired.load(Ordering::Acquire))
            {
                state.actors.remove(&key);
            }
            let may_install = match (
                &cold_install.expected_stale,
                expected_guard.as_deref(),
                state.actors.get(&key),
            ) {
                (None, None, None) => true,
                (Some(expected), Some(accepted), Some(current)) => {
                    Arc::ptr_eq(current, &expected.slot)
                        && !expected.slot.retired.load(Ordering::Acquire)
                        && expected.slot.revision.load(Ordering::Acquire) == expected.revision
                        && accepted.semantic_root == expected.semantic_root
                        && accepted.semantic_root != semantic_root
                }
                _ => false,
            };
            if !may_install {
                None
            } else {
                let (store, document, bytes, bytes_sha256) = candidate
                    .take()
                    .expect("vacant cold install retains its candidate");
                state.clock = state.clock.wrapping_add(1);
                let last_used = state.clock;
                let nonce = state.next_nonce;
                state.next_nonce = state.next_nonce.wrapping_add(1).max(1);
                let slot = Arc::new(PluginActorSlot {
                    nonce,
                    revision: AtomicU64::new(1),
                    last_used: AtomicU64::new(last_used),
                    retired: AtomicBool::new(false),
                    state: Arc::new(AsyncMutex::new(PluginActorAcceptedState {
                        store,
                        document,
                        bytes,
                        bytes_sha256,
                        semantic_root: Arc::clone(&semantic_root),
                        history: VecDeque::new(),
                    })),
                });
                if let Some(replaced) = state.actors.insert(key.clone(), slot) {
                    replaced.retire();
                }
                Some(PluginObservation {
                    key: key.clone(),
                    actor_nonce: nonce,
                    revision: 1,
                    semantic_root: Arc::clone(&semantic_root),
                    bytes_sha256,
                })
            }
        };
        drop(expected_guard);
        if let Some(observation) = installed {
            return Ok(observation);
        }

        let (mut store, document, _, _) =
            candidate.expect("occupied cold install retains its candidate");
        let _ = store.actor.drop_document(document).await;
        let _ = store.actor.retire().await;
        self.observe(&key, &semantic_root).await
    }

    /// Creates authority for bytes actually delivered from the exact root.
    pub(crate) async fn observe(
        &self,
        key: &PluginActorKey,
        semantic_root: &str,
    ) -> Result<PluginObservation, LixError> {
        let slot = self.lookup_slot(key)?;
        let accepted = Arc::clone(&slot.state).lock_owned().await;
        if slot.retired.load(Ordering::Acquire) {
            drop(accepted);
            self.remove_if_same(key, &slot);
            return Err(stale_observation("plugin actor was retired"));
        }
        if accepted.semantic_root.as_ref() != semantic_root {
            return Err(stale_observation("plugin actor root is no longer current"));
        }
        Ok(PluginObservation {
            key: key.clone(),
            actor_nonce: slot.nonce,
            revision: slot.revision.load(Ordering::Acquire),
            semantic_root: Arc::clone(&accepted.semantic_root),
            bytes_sha256: accepted.bytes_sha256,
        })
    }

    /// Serializes one transition on the observation-selected actor.
    #[cfg(test)]
    pub(crate) async fn lease(
        &self,
        observation: &PluginObservation,
    ) -> Result<PluginActorLease, LixError> {
        let slot = self.lookup_slot(&observation.key)?;
        if slot.nonce != observation.actor_nonce
            || slot.revision.load(Ordering::Acquire) != observation.revision
        {
            return Err(stale_observation(
                "plugin observation refers to a replaced document version",
            ));
        }
        drop(slot);
        self.lease_for_transition(observation).await
    }

    /// Leases the observation-selected historical document. The caller reads
    /// durable state only after obtaining this serialization point, then uses
    /// `require_accepted_semantic_root` to prove the current accepted root.
    /// This permits two sessions that read the same revision to detect sparse
    /// edits there and reconcile each delta onto the latest accepted document.
    pub(crate) async fn lease_for_transition(
        &self,
        observation: &PluginObservation,
    ) -> Result<PluginActorLease, LixError> {
        let slot = self.lookup_slot(&observation.key)?;
        if slot.nonce != observation.actor_nonce {
            return Err(stale_observation(
                "plugin observation refers to a replaced actor",
            ));
        }
        let guard = Arc::clone(&slot.state).lock_owned().await;
        if slot.retired.load(Ordering::Acquire) || slot.nonce != observation.actor_nonce {
            drop(guard);
            self.remove_if_same(&observation.key, &slot);
            return Err(stale_observation(
                "plugin observation expired while waiting for its actor",
            ));
        }
        let current_revision = slot.revision.load(Ordering::Acquire);
        let observed = if current_revision == observation.revision
            && guard.semantic_root == observation.semantic_root
            && guard.bytes_sha256 == observation.bytes_sha256
        {
            Some((guard.document, guard.bytes.clone(), guard.bytes_sha256))
        } else {
            guard
                .history
                .iter()
                .find(|historical| {
                    historical.revision == observation.revision
                        && historical.semantic_root == observation.semantic_root
                        && historical.bytes_sha256 == observation.bytes_sha256
                })
                .map(|historical| {
                    (
                        historical.document,
                        historical.bytes.clone(),
                        historical.bytes_sha256,
                    )
                })
        };
        let Some((observed_document, observed_bytes, observed_bytes_sha256)) = observed else {
            return Err(stale_observation(
                "plugin observation history was replaced or evicted",
            ));
        };
        Ok(PluginActorLease {
            cache: self.clone(),
            key: observation.key.clone(),
            slot,
            guard: Some(guard),
            observed_document,
            observed_bytes,
            observed_bytes_sha256,
            uncertain_guest_call: false,
            successor: None,
        })
    }

    fn rekey_slot(
        &self,
        old_key: &PluginActorKey,
        new_key: &PluginActorKey,
        expected: &Arc<PluginActorSlot>,
    ) -> Result<(), LixError> {
        let mut state = self.lock();
        let Some(current) = state.actors.get(old_key) else {
            expected.retire();
            return Err(stale_observation(
                "plugin actor disappeared before descriptor publication",
            ));
        };
        if !Arc::ptr_eq(current, expected) || expected.retired.load(Ordering::Acquire) {
            expected.retire();
            return Err(stale_observation(
                "plugin actor was replaced before descriptor publication",
            ));
        }
        if old_key == new_key {
            return Ok(());
        }

        state.actors.remove(old_key);
        if let Some(replaced) = state.actors.insert(new_key.clone(), Arc::clone(expected))
            && !Arc::ptr_eq(&replaced, expected)
        {
            // A cold read may have raced the post-commit derived publication.
            // The transaction's validated successor is authoritative; revoke
            // the redundant actor and every observation it issued.
            replaced.retire();
        }
        state.clock = state.clock.wrapping_add(1);
        expected.last_used.store(state.clock, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().actors.len()
    }

    #[cfg(test)]
    fn live_store_count(&self) -> usize {
        self.capacity
            .get()
            .saturating_sub(self.store_admission.available_permits())
    }

    fn try_acquire_store(&self) -> Result<Option<PluginActorStorePermit>, LixError> {
        match Arc::clone(&self.store_admission).try_acquire_owned() {
            Ok(permit) => Ok(Some(PluginActorStorePermit { _permit: permit })),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin Store admission semaphore was unexpectedly closed",
            )),
        }
    }

    /// Removes the least-recently-used cache slot only when the cache is its
    /// sole owner. A lease, pending publication, or cold-install token keeps a
    /// second strong reference and therefore keeps its Store admitted.
    fn evict_one_idle_slot(&self) -> bool {
        let evicted = {
            let mut state = self.lock();
            let evicted_key = state
                .actors
                .iter()
                .filter(|(_, slot)| Arc::strong_count(slot) == 1)
                .min_by_key(|(_, slot)| slot.last_used.load(Ordering::Relaxed))
                .map(|(key, _)| key.clone());
            evicted_key.and_then(|key| state.actors.remove(&key))
        };
        if let Some(slot) = evicted {
            slot.retire();
            drop(slot);
            true
        } else {
            false
        }
    }

    /// A cold-install token is normally a second reference to its stale
    /// predecessor. At the hard limit, only an otherwise-idle predecessor can
    /// be removed, then the token becomes a vacant-key token so installing the
    /// already admitted candidate cannot revive the retired Store.
    fn evict_idle_cold_predecessor(&self, cold_install: &mut PluginActorColdInstall) -> bool {
        let Some(expected) = cold_install.expected_stale.as_ref() else {
            return false;
        };
        let evicted = {
            let mut state = self.lock();
            let Some(current) = state.actors.get(&cold_install.key) else {
                return false;
            };
            if !Arc::ptr_eq(current, &expected.slot) || Arc::strong_count(current) != 2 {
                return false;
            }
            state.actors.remove(&cold_install.key)
        };
        let Some(slot) = evicted else {
            return false;
        };
        slot.retire();
        drop(slot);
        cold_install.expected_stale = None;
        true
    }

    /// A concurrent trap can retire and unlink the captured predecessor before
    /// this cold builder asks for admission. Once its token is the final owner,
    /// discard it so a no-longer-reachable Store cannot strand capacity.
    fn drop_detached_retired_cold_predecessor(
        &self,
        cold_install: &mut PluginActorColdInstall,
    ) -> bool {
        let Some(expected) = cold_install.expected_stale.as_ref() else {
            return false;
        };
        if !expected.slot.retired.load(Ordering::Acquire) || Arc::strong_count(&expected.slot) != 1
        {
            return false;
        }
        cold_install.expected_stale = None;
        true
    }

    fn lookup_slot(&self, key: &PluginActorKey) -> Result<Arc<PluginActorSlot>, LixError> {
        let mut state = self.lock();
        let Some(slot) = state.actors.get(key).cloned() else {
            return Err(stale_observation(
                "plugin observation is unknown or evicted",
            ));
        };
        if slot.retired.load(Ordering::Acquire) {
            state.actors.remove(key);
            return Err(stale_observation("plugin actor was retired"));
        }
        state.clock = state.clock.wrapping_add(1);
        slot.last_used.store(state.clock, Ordering::Relaxed);
        Ok(slot)
    }

    fn remove_if_same(&self, key: &PluginActorKey, expected: &Arc<PluginActorSlot>) {
        let mut state = self.lock();
        if state
            .actors
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            state.actors.remove(key);
        }
        expected.retire();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, PluginActorCacheState> {
        self.state
            .lock()
            .expect("plugin actor cache mutex should not poison")
    }
}

fn plugin_store_resource_limit(capacity: NonZeroUsize) -> LixError {
    LixError::new(
        LixError::CODE_PLUGIN_RESOURCE_LIMIT,
        format!(
            "plugin live Store limit of {} is exhausted for this engine",
            capacity.get()
        ),
    )
    .with_hint(
        "commit or split transactions retaining plugin actors, or raise EngineOptions::with_plugin_v2_resource_limits",
    )
}

struct PluginActorSuccessor {
    document: WasmDocumentHandle,
    bytes: Blob,
    bytes_sha256: FileBytesSha256,
    semantic_root: Arc<str>,
}

/// Opaque authority for one guest call based on the actor's latest private
/// state. The token owns any prior pending successor while the call is in
/// flight so deterministic rejection can restore it exactly.
pub(crate) struct PluginActorPendingCall {
    document: WasmDocumentHandle,
    bytes: Blob,
    semantic_root: Arc<str>,
    previous_successor: Option<PluginActorSuccessor>,
}

impl PluginActorPendingCall {
    pub(crate) fn document(&self) -> WasmDocumentHandle {
        self.document
    }

    pub(crate) fn bytes(&self) -> Blob {
        self.bytes.clone()
    }

    pub(crate) fn semantic_root(&self) -> &str {
        &self.semantic_root
    }
}

/// Exclusive transition lease. Holding it across the durable commit point is
/// intentional: one file actor is serialized while unrelated files continue.
pub(crate) struct PluginActorLease {
    cache: PluginActorCache,
    key: PluginActorKey,
    slot: Arc<PluginActorSlot>,
    guard: Option<OwnedMutexGuard<PluginActorAcceptedState>>,
    observed_document: WasmDocumentHandle,
    observed_bytes: Blob,
    observed_bytes_sha256: FileBytesSha256,
    uncertain_guest_call: bool,
    successor: Option<PluginActorSuccessor>,
}

impl PluginActorLease {
    pub(crate) fn actor_mut(&mut self) -> &mut dyn WasmComponentV2Actor {
        self.guard
            .as_deref_mut()
            .expect("actor lease guard exists")
            .store
            .actor
            .as_mut()
    }

    pub(crate) fn accepted_document(&self) -> WasmDocumentHandle {
        self.guard
            .as_deref()
            .expect("actor lease guard exists")
            .document
    }

    pub(crate) fn accepted_bytes(&self) -> Blob {
        self.guard
            .as_deref()
            .expect("actor lease guard exists")
            .bytes
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn accepted_bytes_sha256(&self) -> FileBytesSha256 {
        self.guard
            .as_deref()
            .expect("actor lease guard exists")
            .bytes_sha256
    }

    pub(crate) fn observed_document(&self) -> WasmDocumentHandle {
        self.observed_document
    }

    pub(crate) fn observed_bytes(&self) -> Blob {
        self.observed_bytes.clone()
    }

    pub(crate) fn observed_bytes_sha256(&self) -> FileBytesSha256 {
        self.observed_bytes_sha256
    }

    pub(crate) fn accepted_semantic_root(&self) -> &str {
        &self
            .guard
            .as_deref()
            .expect("actor lease guard exists")
            .semantic_root
    }

    pub(crate) fn require_accepted_semantic_root(
        &self,
        visible_root: &str,
    ) -> Result<(), LixError> {
        if self.accepted_semantic_root() != visible_root {
            self.slot.retire();
            return Err(stale_observation(
                "plugin actor root no longer matches visible durable state",
            ));
        }
        Ok(())
    }

    /// Begins a guest call against the latest private successor, or against
    /// the durable accepted state when this is the first call in the lease.
    ///
    /// Taking the previous successor out of the lease makes the returned token
    /// the sole rollback authority. Cancellation leaves the uncertainty bit
    /// set, which retires the Store rather than publishing ambiguous state.
    pub(crate) fn begin_pending_guest_call(&mut self) -> Result<PluginActorPendingCall, LixError> {
        if self.uncertain_guest_call {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin actor already has an in-flight transition",
            ));
        }
        let previous_successor = self.successor.take();
        let (document, bytes, semantic_root) = if let Some(successor) = previous_successor.as_ref()
        {
            (
                successor.document,
                successor.bytes.clone(),
                Arc::clone(&successor.semantic_root),
            )
        } else {
            let accepted = self.guard.as_deref().expect("actor lease guard exists");
            (
                accepted.document,
                accepted.bytes.clone(),
                Arc::clone(&accepted.semantic_root),
            )
        };
        self.uncertain_guest_call = true;
        Ok(PluginActorPendingCall {
            document,
            bytes,
            semantic_root,
            previous_successor,
        })
    }

    /// Resolves a failed chained call. Deterministic rejection restores the
    /// exact prior pending successor; a trap or deadline retires the Store.
    pub(crate) fn handle_pending_guest_call_error(
        &mut self,
        mut call: PluginActorPendingCall,
        error: LixError,
    ) -> LixError {
        if !self.uncertain_guest_call || self.successor.is_some() {
            self.slot.retire();
            return error;
        }
        let runtime_retired = error.message.contains("deadline")
            || self
                .guard
                .as_deref()
                .expect("actor lease guard exists")
                .store
                .actor
                .is_retired();
        if runtime_retired {
            self.slot.retire();
        } else {
            self.successor = call.previous_successor.take();
            self.uncertain_guest_call = false;
        }
        error
    }

    /// Replaces the latest private successor after a fully drained, validated
    /// guest call. The superseded private document is no longer reachable and
    /// is dropped before the next statement may begin.
    pub(crate) async fn complete_pending_guest_call(
        &mut self,
        mut call: PluginActorPendingCall,
        document: WasmDocumentHandle,
        bytes: Blob,
        bytes_sha256: FileBytesSha256,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<(), LixError> {
        if !self.uncertain_guest_call || self.successor.is_some() {
            self.slot.retire();
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin guest completion did not match one in-flight transition",
            ));
        }
        let previous_successor = call.previous_successor.take();
        self.successor = Some(PluginActorSuccessor {
            document,
            bytes,
            bytes_sha256,
            semantic_root: semantic_root.into(),
        });
        if let Some(previous_successor) = previous_successor {
            if let Err(error) = self
                .actor_mut()
                .drop_document(previous_successor.document)
                .await
            {
                self.slot.retire();
                return Err(error);
            }
        }
        self.uncertain_guest_call = false;
        Ok(())
    }

    /// Must immediately precede a guest call. Cancellation or unwinding while
    /// this bit is set retires the whole Store and revokes every observation.
    pub(crate) fn begin_guest_call(&mut self) -> Result<(), LixError> {
        if self.uncertain_guest_call || self.successor.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin actor already has an in-flight or pending transition",
            ));
        }
        self.uncertain_guest_call = true;
        Ok(())
    }

    /// Resolves an error from a warm guest call or its host drain wrapper.
    /// A live runtime proves deterministic rejection and permits reuse of the
    /// accepted actor; a runtime that reports retirement preserves the lease's
    /// fail-closed behavior. Cancellation never reaches this method, leaving
    /// `uncertain_guest_call` set for `Drop` to retire the slot.
    pub(crate) fn handle_guest_call_error(&mut self, error: LixError) -> LixError {
        if !self.uncertain_guest_call || self.successor.is_some() {
            self.slot.retire();
            return error;
        }
        let runtime_retired = error.message.contains("deadline")
            || self
                .guard
                .as_deref()
                .expect("actor lease guard exists")
                .store
                .actor
                .is_retired();
        if runtime_retired {
            self.slot.retire();
        } else {
            self.uncertain_guest_call = false;
        }
        error
    }

    /// Records a fully drained and validated prospective guest document.
    pub(crate) fn complete_guest_call(
        &mut self,
        document: WasmDocumentHandle,
        bytes: Blob,
        bytes_sha256: FileBytesSha256,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<(), LixError> {
        if !self.uncertain_guest_call || self.successor.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin guest completion did not match one in-flight transition",
            ));
        }
        self.uncertain_guest_call = false;
        self.successor = Some(PluginActorSuccessor {
            document,
            bytes,
            bytes_sha256,
            semantic_root: semantic_root.into(),
        });
        Ok(())
    }

    /// Deterministic validation/storage rejection keeps the accepted state.
    pub(crate) async fn discard_successor(mut self) -> Result<(), LixError> {
        let Some(successor) = self.successor.take() else {
            return Ok(());
        };
        self.uncertain_guest_call = true;
        let result = self.actor_mut().drop_document(successor.document).await;
        self.uncertain_guest_call = false;
        if result.is_err() {
            self.slot.retire();
        }
        result
    }

    /// Publishes the successor only after durable commit. A failure here is a
    /// cache failure: the caller must keep the commit successful and cold-open.
    #[cfg(test)]
    pub(crate) async fn commit_successor(self) -> Result<PluginObservation, LixError> {
        let key = self.key.clone();
        self.commit_successor_as(key).await
    }

    /// Publishes a validated successor under a descriptor-successor key.
    ///
    /// Rename transitions execute while the durable actor is still selected
    /// by its old path. The transaction calls this only after storage commit,
    /// so moving the slot here makes the old observations stale atomically
    /// with publishing the successor under the new path identity.
    pub(crate) async fn commit_successor_as(
        mut self,
        successor_key: PluginActorKey,
    ) -> Result<PluginObservation, LixError> {
        let successor = self.successor.take().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin actor commit is missing a validated successor",
            )
        })?;
        let old_revision = self.slot.revision.load(Ordering::Acquire);
        let evicted_document = {
            let accepted = self.guard.as_deref_mut().expect("actor lease guard exists");
            let old_document = std::mem::replace(&mut accepted.document, successor.document);
            let old_bytes = std::mem::replace(&mut accepted.bytes, successor.bytes);
            let old_bytes_sha256 =
                std::mem::replace(&mut accepted.bytes_sha256, successor.bytes_sha256);
            let old_semantic_root = std::mem::replace(
                &mut accepted.semantic_root,
                Arc::clone(&successor.semantic_root),
            );
            accepted.history.push_back(PluginActorHistoricalState {
                revision: old_revision,
                document: old_document,
                bytes: old_bytes,
                bytes_sha256: old_bytes_sha256,
                semantic_root: old_semantic_root,
            });
            (accepted.history.len() > DEFAULT_MAX_PLUGIN_FILE_HISTORY).then(|| {
                accepted
                    .history
                    .pop_front()
                    .expect("over-capacity plugin history is nonempty")
                    .document
            })
        };
        let revision = self.slot.revision.fetch_add(1, Ordering::AcqRel) + 1;

        if let Some(evicted_document) = evicted_document {
            self.uncertain_guest_call = true;
            let result = self.actor_mut().drop_document(evicted_document).await;
            self.uncertain_guest_call = false;
            if let Err(error) = result {
                self.slot.retire();
                return Err(error);
            }
        }
        if let Err(error) = self.cache.rekey_slot(&self.key, &successor_key, &self.slot) {
            self.cache.remove_if_same(&self.key, &self.slot);
            return Err(error);
        }
        Ok(PluginObservation {
            key: successor_key,
            actor_nonce: self.slot.nonce,
            revision,
            semantic_root: successor.semantic_root,
            bytes_sha256: successor.bytes_sha256,
        })
    }
}

impl Drop for PluginActorLease {
    fn drop(&mut self) {
        // A pending successor that was neither committed nor deterministically
        // discarded is an uncertain completion. Never reuse that Store.
        if self.uncertain_guest_call || self.successor.is_some() {
            self.slot.retire();
        }
    }
}

fn stale_observation(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_PLUGIN_OBSERVATION_STALE, message)
        .with_hint("read the exact file bytes again before retrying the edit")
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::wasm::{
        WasmChangeCursorHandle, WasmChangePage, WasmComponentV2Actor, WasmEditCursorHandle,
        WasmEditPage, WasmEntityTransition, WasmEntityUpdate, WasmFileTransition, WasmFileUpdate,
        WasmOpenEntitiesInput, WasmOpenFileInput, WasmTransitionCounters, WasmTransitionHandle,
        WasmTransitionLimits,
    };

    #[derive(Default)]
    struct TestActor {
        retired: bool,
        retirement_probe: Option<Arc<AtomicBool>>,
        dropped_documents: Option<Arc<Mutex<Vec<WasmDocumentHandle>>>>,
        _drop_probe: Option<TestActorDropProbe>,
    }

    struct TestActorDropProbe {
        admission: Arc<Semaphore>,
        observed_permits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for TestActorDropProbe {
        fn drop(&mut self) {
            self.observed_permits
                .store(self.admission.available_permits(), Ordering::Release);
        }
    }

    fn unused() -> LixError {
        LixError::new(LixError::CODE_INTERNAL_ERROR, "unused test actor method")
    }

    #[async_trait]
    impl WasmComponentV2Actor for TestActor {
        async fn fork_document(
            &mut self,
            document: WasmDocumentHandle,
        ) -> Result<WasmDocumentHandle, LixError> {
            Ok(document)
        }

        async fn open_file(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenFileInput,
        ) -> Result<WasmFileTransition, LixError> {
            Err(unused())
        }

        async fn open_entities(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenEntitiesInput,
        ) -> Result<WasmEntityTransition, LixError> {
            Err(unused())
        }

        async fn file_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: WasmFileUpdate,
        ) -> Result<WasmFileTransition, LixError> {
            Err(unused())
        }

        async fn entities_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: WasmEntityUpdate,
        ) -> Result<WasmEntityTransition, LixError> {
            Err(unused())
        }

        async fn next_change_page(
            &mut self,
            _transition: WasmTransitionHandle,
            _cursor: WasmChangeCursorHandle,
            _max_bytes: u32,
        ) -> Result<Option<WasmChangePage>, LixError> {
            Err(unused())
        }

        async fn next_edit_page(
            &mut self,
            _transition: WasmTransitionHandle,
            _cursor: WasmEditCursorHandle,
            _max_edits: u32,
            _max_inline_bytes: u32,
        ) -> Result<Option<WasmEditPage>, LixError> {
            Err(unused())
        }

        async fn output_len(
            &mut self,
            _transition: WasmTransitionHandle,
            _outputs: crate::wasm::WasmByteOutputsHandle,
            _index: u32,
        ) -> Result<u64, LixError> {
            Err(unused())
        }

        async fn read_output(
            &mut self,
            _transition: WasmTransitionHandle,
            _outputs: crate::wasm::WasmByteOutputsHandle,
            _index: u32,
            _offset: u64,
            _length: u32,
        ) -> Result<Vec<u8>, LixError> {
            Err(unused())
        }

        async fn finish_transition(
            &mut self,
            _transition: WasmTransitionHandle,
        ) -> Result<WasmTransitionCounters, LixError> {
            Err(unused())
        }

        async fn discard_transition(
            &mut self,
            _transition: WasmTransitionHandle,
        ) -> Result<(), LixError> {
            Ok(())
        }

        async fn drop_document(&mut self, document: WasmDocumentHandle) -> Result<(), LixError> {
            if let Some(probe) = &self.dropped_documents {
                probe
                    .lock()
                    .expect("dropped document probe should not poison")
                    .push(document);
            }
            Ok(())
        }

        fn is_retired(&self) -> bool {
            self.retired
        }

        async fn retire(&mut self) -> Result<(), LixError> {
            self.retired = true;
            if let Some(probe) = &self.retirement_probe {
                probe.store(true, Ordering::Release);
            }
            Ok(())
        }
    }

    fn key(branch: &str, path: &str, generation: &str) -> PluginActorKey {
        PluginActorKey {
            branch_id: branch.to_owned(),
            file_id: "file".to_owned(),
            path: path.to_owned(),
            owner_change_id: "incarnation".to_owned(),
            plugin_key: "plugin_csv_v2".to_owned(),
            plugin_generation: generation.to_owned(),
        }
    }

    fn install(
        cache: &PluginActorCache,
        key: PluginActorKey,
        document: u64,
        bytes: &'static [u8],
        root: &str,
    ) -> PluginObservation {
        cache.install(
            key,
            PluginActorStore::new(
                Box::new(TestActor::default()),
                cache
                    .admit_store()
                    .expect("test actor should receive Store admission"),
            ),
            WasmDocumentHandle(document),
            bytes.into(),
            Arc::<str>::from(root),
        )
    }

    #[tokio::test]
    async fn successor_is_not_visible_until_commit() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key.clone(), 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"after".as_slice().into(),
                FileBytesSha256::compute(b"after"),
                Arc::<str>::from("root-2"),
            )
            .unwrap();

        // The lease intentionally holds the actor mutex through commit, so
        // inspect its accepted root directly instead of attempting a
        // self-deadlocking concurrent observation.
        assert_eq!(lease.accepted_semantic_root(), "root-1");
        let successor = lease.commit_successor().await.unwrap();
        assert_eq!(successor.semantic_root(), "root-2");
        assert!(cache.lease(&observation).await.is_err());
        assert_eq!(
            cache
                .lease(&successor)
                .await
                .unwrap()
                .accepted_bytes()
                .as_ref(),
            b"after"
        );
    }

    #[tokio::test]
    async fn chained_successors_stay_private_until_single_commit() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let dropped_documents = Arc::new(Mutex::new(Vec::new()));
        let observation = cache.install(
            key,
            PluginActorStore::new(
                Box::new(TestActor {
                    dropped_documents: Some(Arc::clone(&dropped_documents)),
                    ..TestActor::default()
                }),
                cache
                    .admit_store()
                    .expect("test actor should receive Store admission"),
            ),
            WasmDocumentHandle(1),
            b"before".as_slice().into(),
            Arc::<str>::from("root-1"),
        );
        let mut lease = cache.lease(&observation).await.unwrap();

        let first_call = lease.begin_pending_guest_call().unwrap();
        assert_eq!(first_call.document(), WasmDocumentHandle(1));
        assert_eq!(first_call.bytes().as_ref(), b"before");
        assert_eq!(first_call.semantic_root(), "root-1");
        lease
            .complete_pending_guest_call(
                first_call,
                WasmDocumentHandle(2),
                b"middle".as_slice().into(),
                FileBytesSha256::compute(b"middle"),
                Arc::<str>::from("root-2"),
            )
            .await
            .unwrap();

        let second_call = lease.begin_pending_guest_call().unwrap();
        assert_eq!(second_call.document(), WasmDocumentHandle(2));
        assert_eq!(second_call.bytes().as_ref(), b"middle");
        assert_eq!(second_call.semantic_root(), "root-2");
        lease
            .complete_pending_guest_call(
                second_call,
                WasmDocumentHandle(3),
                b"after".as_slice().into(),
                FileBytesSha256::compute(b"after"),
                Arc::<str>::from("root-3"),
            )
            .await
            .unwrap();

        assert_eq!(lease.accepted_document(), WasmDocumentHandle(1));
        assert_eq!(lease.accepted_bytes().as_ref(), b"before");
        assert_eq!(lease.accepted_semantic_root(), "root-1");
        assert_eq!(
            *dropped_documents
                .lock()
                .expect("dropped document probe should not poison"),
            vec![WasmDocumentHandle(2)]
        );

        let successor = lease.commit_successor().await.unwrap();
        assert_eq!(successor.semantic_root(), "root-3");
        assert!(cache.lease(&observation).await.is_err());
        let committed = cache.lease(&successor).await.unwrap();
        assert_eq!(committed.accepted_document(), WasmDocumentHandle(3));
        assert_eq!(committed.accepted_bytes().as_ref(), b"after");
    }

    #[tokio::test]
    async fn discarding_chained_successors_restores_accepted_state() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let dropped_documents = Arc::new(Mutex::new(Vec::new()));
        let observation = cache.install(
            key,
            PluginActorStore::new(
                Box::new(TestActor {
                    dropped_documents: Some(Arc::clone(&dropped_documents)),
                    ..TestActor::default()
                }),
                cache
                    .admit_store()
                    .expect("test actor should receive Store admission"),
            ),
            WasmDocumentHandle(1),
            b"before".as_slice().into(),
            Arc::<str>::from("root-1"),
        );
        let mut lease = cache.lease(&observation).await.unwrap();

        let first_call = lease.begin_pending_guest_call().unwrap();
        lease
            .complete_pending_guest_call(
                first_call,
                WasmDocumentHandle(2),
                b"middle".as_slice().into(),
                FileBytesSha256::compute(b"middle"),
                Arc::<str>::from("root-2"),
            )
            .await
            .unwrap();
        let second_call = lease.begin_pending_guest_call().unwrap();
        lease
            .complete_pending_guest_call(
                second_call,
                WasmDocumentHandle(3),
                b"after".as_slice().into(),
                FileBytesSha256::compute(b"after"),
                Arc::<str>::from("root-3"),
            )
            .await
            .unwrap();
        lease.discard_successor().await.unwrap();

        let accepted = cache.lease(&observation).await.unwrap();
        assert_eq!(accepted.accepted_document(), WasmDocumentHandle(1));
        assert_eq!(accepted.accepted_bytes().as_ref(), b"before");
        assert_eq!(accepted.accepted_semantic_root(), "root-1");
        assert_eq!(
            *dropped_documents
                .lock()
                .expect("dropped document probe should not poison"),
            vec![WasmDocumentHandle(2), WasmDocumentHandle(3)]
        );
    }

    #[tokio::test]
    async fn deterministic_chain_failure_restores_prior_pending_successor() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key, 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();

        let first_call = lease.begin_pending_guest_call().unwrap();
        lease
            .complete_pending_guest_call(
                first_call,
                WasmDocumentHandle(2),
                b"middle".as_slice().into(),
                FileBytesSha256::compute(b"middle"),
                Arc::<str>::from("root-2"),
            )
            .await
            .unwrap();

        let rejected_call = lease.begin_pending_guest_call().unwrap();
        assert_eq!(rejected_call.document(), WasmDocumentHandle(2));
        let rejection = LixError::new(LixError::CODE_INVALID_PLUGIN, "deterministic rejection");
        assert_eq!(
            lease
                .handle_pending_guest_call_error(rejected_call, rejection.clone())
                .message,
            rejection.message
        );
        assert_eq!(lease.accepted_document(), WasmDocumentHandle(1));
        assert_eq!(lease.accepted_semantic_root(), "root-1");

        let retry = lease.begin_pending_guest_call().unwrap();
        assert_eq!(retry.document(), WasmDocumentHandle(2));
        assert_eq!(retry.bytes().as_ref(), b"middle");
        assert_eq!(retry.semantic_root(), "root-2");
        let retry_rejection = LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "deterministic retry rejection",
        );
        lease.handle_pending_guest_call_error(retry, retry_rejection);

        let successor = lease.commit_successor().await.unwrap();
        assert_eq!(successor.semantic_root(), "root-2");
        assert_eq!(
            cache
                .lease(&successor)
                .await
                .unwrap()
                .accepted_bytes()
                .as_ref(),
            b"middle"
        );
    }

    #[tokio::test]
    async fn byte_hash_tracks_observed_historical_and_successor_versions() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let before_hash = FileBytesSha256::compute(b"before");
        let after_hash = FileBytesSha256::compute(b"after");
        let before = install(&cache, key, 1, b"before", "root-1");
        assert_eq!(before.bytes_sha256(), before_hash);

        let mut lease = cache.lease(&before).await.unwrap();
        assert_eq!(lease.observed_bytes_sha256(), before_hash);
        assert_eq!(lease.accepted_bytes_sha256(), before_hash);
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"after".as_slice().into(),
                after_hash,
                Arc::<str>::from("root-2"),
            )
            .unwrap();
        let after = lease.commit_successor().await.unwrap();
        assert_eq!(after.bytes_sha256(), after_hash);

        let historical = cache.lease_for_transition(&before).await.unwrap();
        assert_eq!(historical.observed_bytes_sha256(), before_hash);
        assert_eq!(historical.accepted_bytes_sha256(), after_hash);
    }

    #[tokio::test]
    async fn descriptor_successor_rekeys_only_when_committed() {
        let cache = PluginActorCache::new(2).unwrap();
        let old_key = key("main", "/before.csv", "g1");
        let new_key = key("main", "/after.csv", "g1");
        let observation = install(&cache, old_key.clone(), 1, b"same", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"same".as_slice().into(),
                FileBytesSha256::compute(b"same"),
                Arc::<str>::from("root-2"),
            )
            .unwrap();

        assert!(cache.observe(&new_key, "root-2").await.is_err());
        {
            let state = cache.lock();
            assert!(state.actors.contains_key(&old_key));
            assert!(!state.actors.contains_key(&new_key));
        }

        let successor = lease.commit_successor_as(new_key.clone()).await.unwrap();
        assert_eq!(successor.key(), &new_key);
        assert!(cache.observe(&old_key, "root-2").await.is_err());
        assert!(cache.lease(&observation).await.is_err());
        assert_eq!(
            cache
                .lease(&successor)
                .await
                .unwrap()
                .accepted_bytes()
                .as_ref(),
            b"same"
        );
    }

    #[tokio::test]
    async fn historical_observation_detects_against_old_bytes_and_renders_from_current() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let first = install(&cache, key, 1, b"before", "root-1");
        let mut first_lease = cache.lease(&first).await.unwrap();
        first_lease.begin_guest_call().unwrap();
        first_lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"after-a".as_slice().into(),
                FileBytesSha256::compute(b"after-a"),
                Arc::<str>::from("root-2"),
            )
            .unwrap();
        let current = first_lease.commit_successor().await.unwrap();

        let historical = cache.lease_for_transition(&first).await.unwrap();
        historical
            .require_accepted_semantic_root(current.semantic_root())
            .unwrap();
        assert_eq!(historical.observed_document(), WasmDocumentHandle(1));
        assert_eq!(historical.observed_bytes().as_ref(), b"before");
        assert_eq!(historical.accepted_document(), WasmDocumentHandle(2));
        assert_eq!(historical.accepted_bytes().as_ref(), b"after-a");
    }

    #[tokio::test]
    async fn visible_root_mismatch_retires_cross_engine_stale_actor() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key.clone(), 1, b"before", "root-1");

        let lease = cache.lease_for_transition(&observation).await.unwrap();
        assert!(
            lease
                .require_accepted_semantic_root("external-root")
                .is_err()
        );
        drop(lease);
        assert!(cache.observe(&key, "root-1").await.is_err());
    }

    #[tokio::test]
    async fn historical_observations_are_bounded() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let first = install(&cache, key, 1, b"version-1", "root-1");
        let mut current = first.clone();
        for revision in 2..=(DEFAULT_MAX_PLUGIN_FILE_HISTORY as u64 + 2) {
            let mut lease = cache.lease(&current).await.unwrap();
            lease.begin_guest_call().unwrap();
            let bytes = format!("version-{revision}").into_bytes();
            lease
                .complete_guest_call(
                    WasmDocumentHandle(revision),
                    bytes.clone().into(),
                    FileBytesSha256::compute(&bytes),
                    Arc::<str>::from(format!("root-{revision}")),
                )
                .unwrap();
            current = lease.commit_successor().await.unwrap();
        }

        assert!(cache.lease_for_transition(&first).await.is_err());
        assert!(cache.lease(&current).await.is_ok());
    }

    #[tokio::test]
    async fn deterministic_rejection_keeps_the_accepted_observation() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key, 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"rejected".as_slice().into(),
                FileBytesSha256::compute(b"rejected"),
                Arc::<str>::from("root-2"),
            )
            .unwrap();
        lease.discard_successor().await.unwrap();
        assert_eq!(
            cache
                .lease(&observation)
                .await
                .unwrap()
                .accepted_bytes()
                .as_ref(),
            b"before"
        );
    }

    #[tokio::test]
    async fn known_guest_rejection_clears_uncertainty_and_keeps_actor_live() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key.clone(), 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        let rejection = LixError::new(LixError::CODE_INVALID_PLUGIN, "host validator rejected");
        assert_eq!(
            lease.handle_guest_call_error(rejection.clone()).message,
            rejection.message
        );
        drop(lease);
        assert!(cache.observe(&key, "root-1").await.is_ok());
    }

    #[tokio::test]
    async fn trapped_guest_error_retires_the_actor_slot() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key.clone(), 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease.actor_mut().retire().await.unwrap();
        let trap = LixError::new(LixError::CODE_INTERNAL_ERROR, "guest trapped");
        let _ = lease.handle_guest_call_error(trap);
        drop(lease);
        assert!(cache.observe(&key, "root-1").await.is_err());
    }

    #[tokio::test]
    async fn deadline_error_retires_even_if_runtime_returned_cleanly() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key.clone(), 1, b"before", "root-1");
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        let deadline = LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "v2 transition deadline elapsed",
        );
        let _ = lease.handle_guest_call_error(deadline);
        drop(lease);
        assert!(cache.observe(&key, "root-1").await.is_err());
    }

    #[tokio::test]
    async fn uncertain_completion_retires_only_that_actor() {
        let cache = PluginActorCache::new(2).unwrap();
        let first_key = key("main", "/first.csv", "g1");
        let second_key = key("main", "/second.csv", "g1");
        let first = install(&cache, first_key, 1, b"one", "root-1");
        let second = install(&cache, second_key, 2, b"two", "root-2");
        let mut lease = cache.lease(&first).await.unwrap();
        lease.begin_guest_call().unwrap();
        drop(lease);
        assert!(cache.lease(&first).await.is_err());
        assert!(cache.lease(&second).await.is_ok());
    }

    #[tokio::test]
    async fn byte_identity_never_substitutes_for_root_or_lifecycle_identity() {
        let cache = PluginActorCache::new(2).unwrap();
        let first_key = key("main", "/data.csv", "g1");
        let first = install(&cache, first_key.clone(), 1, b"same", "root-a");
        assert!(cache.observe(&first_key, "root-b").await.is_err());

        let second_key = key("branch-2", "/data.csv", "g1");
        let second = install(&cache, second_key, 2, b"same", "root-a");
        assert_ne!(first.key(), second.key());
    }

    #[tokio::test]
    async fn actor_admission_evicts_the_least_recently_used_file() {
        let cache = PluginActorCache::new(2).unwrap();
        let first = install(&cache, key("main", "/first.csv", "g1"), 1, b"one", "root-1");
        let second = install(
            &cache,
            key("main", "/second.csv", "g1"),
            2,
            b"two",
            "root-2",
        );
        cache.lease(&first).await.unwrap();
        let third = install(
            &cache,
            key("main", "/third.csv", "g1"),
            3,
            b"three",
            "root-3",
        );
        assert_eq!(cache.len(), 2);
        assert!(cache.lease(&first).await.is_ok());
        assert!(cache.lease(&second).await.is_err());
        assert!(cache.lease(&third).await.is_ok());
    }

    #[tokio::test]
    async fn live_store_admission_never_evicts_a_leased_actor() {
        let cache = PluginActorCache::new(1).unwrap();
        let key = key("main", "/data.csv", "g1");
        let observation = install(&cache, key, 1, b"before", "root-1");
        let lease = cache.lease(&observation).await.unwrap();

        let error = match cache.admit_store() {
            Ok(_) => panic!("a live lease must keep its Store admitted"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        assert_eq!(cache.live_store_count(), 1);
        assert_eq!(cache.len(), 1);

        drop(lease);
        let pending = cache
            .admit_store()
            .expect("idle cached Store should be evicted for a new admission");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.live_store_count(), 1);
        drop(pending);
        assert_eq!(cache.live_store_count(), 0);
    }

    #[tokio::test]
    async fn store_permit_outlives_an_evicted_slot_held_by_a_lease() {
        let cache = PluginActorCache::new(1).unwrap();
        let key = key("main", "/data.csv", "g1");
        let permits_seen_during_actor_drop =
            Arc::new(std::sync::atomic::AtomicUsize::new(usize::MAX));
        let observation = cache.install(
            key.clone(),
            PluginActorStore::new(
                Box::new(TestActor {
                    _drop_probe: Some(TestActorDropProbe {
                        admission: Arc::clone(&cache.store_admission),
                        observed_permits: Arc::clone(&permits_seen_during_actor_drop),
                    }),
                    ..TestActor::default()
                }),
                cache
                    .admit_store()
                    .expect("test actor should receive Store admission"),
            ),
            WasmDocumentHandle(1),
            b"before".as_slice().into(),
            Arc::<str>::from("root-1"),
        );
        let lease = cache.lease(&observation).await.unwrap();
        cache.remove_if_same(&key, &lease.slot);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.live_store_count(), 1);

        drop(lease);
        assert_eq!(
            permits_seen_during_actor_drop.load(Ordering::Acquire),
            0,
            "the actor must be destroyed before its Store admission is released"
        );
        assert_eq!(cache.live_store_count(), 0);
    }

    #[tokio::test]
    async fn cold_admission_reclaims_only_an_idle_captured_predecessor() {
        let cache = PluginActorCache::new(1).unwrap();
        let key = key("main", "/data.csv", "g1");
        let stale = install(&cache, key.clone(), 1, b"old", "root-old");
        let mut cold_install = match cache.prepare_cold_open(&key, "root-new").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("different root must need a cold candidate"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };

        let store_permit = cache
            .admit_cold_store(&mut cold_install)
            .expect("idle stale Store should be replaced before cold construction");
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.live_store_count(), 1);

        let replacement = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(Box::new(TestActor::default()), store_permit),
                WasmDocumentHandle(2),
                b"new".as_slice().into(),
                FileBytesSha256::compute(b"new"),
                Arc::<str>::from("root-new"),
            )
            .await
            .expect("vacant cold candidate should install");
        assert!(cache.lease(&stale).await.is_err());
        assert_eq!(cache.observe(&key, "root-new").await.unwrap(), replacement);
        assert_eq!(cache.live_store_count(), 1);
    }

    #[tokio::test]
    async fn cold_admission_refuses_a_leased_predecessor_then_recovers() {
        let cache = PluginActorCache::new(1).unwrap();
        let key = key("main", "/data.csv", "g1");
        let stale = install(&cache, key.clone(), 1, b"old", "root-old");
        let mut cold_install = match cache.prepare_cold_open(&key, "root-new").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("different root must need a cold candidate"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let lease = cache.lease(&stale).await.unwrap();

        let error = match cache.admit_cold_store(&mut cold_install) {
            Ok(_) => panic!("a leased stale Store must not be overcommitted"),
            Err(error) => error,
        };
        assert_eq!(error.code, LixError::CODE_PLUGIN_RESOURCE_LIMIT);
        assert_eq!(cache.len(), 1);

        drop(lease);
        let store_permit = cache
            .admit_cold_store(&mut cold_install)
            .expect("released stale Store should make the cold candidate admissible");
        let replacement = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(Box::new(TestActor::default()), store_permit),
                WasmDocumentHandle(2),
                b"new".as_slice().into(),
                FileBytesSha256::compute(b"new"),
                Arc::<str>::from("root-new"),
            )
            .await
            .expect("released stale candidate should install");
        assert!(cache.lease(&stale).await.is_err());
        assert_eq!(cache.observe(&key, "root-new").await.unwrap(), replacement);
    }

    #[tokio::test]
    async fn cold_admission_releases_a_detached_retired_predecessor() {
        let cache = PluginActorCache::new(1).unwrap();
        let key = key("main", "/data.csv", "g1");
        let stale = install(&cache, key.clone(), 1, b"old", "root-old");
        let mut cold_install = match cache.prepare_cold_open(&key, "root-new").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("different root must need a cold candidate"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let expected = Arc::clone(
            &cold_install
                .expected_stale
                .as_ref()
                .expect("cold token should capture its predecessor")
                .slot,
        );
        cache.remove_if_same(&key, &expected);
        drop(expected);
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.live_store_count(), 1);

        let pending = cache
            .admit_cold_store(&mut cold_install)
            .expect("detached retired predecessor should not strand capacity");
        assert_eq!(cache.live_store_count(), 1);
        drop(pending);
        drop(cold_install);
        assert_eq!(cache.live_store_count(), 0);
        assert!(cache.lease(&stale).await.is_err());
    }

    #[tokio::test]
    async fn losing_cold_candidate_releases_its_store_admission() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let cold_install = match cache.prepare_cold_open(&key, "root-stale").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("vacant key cannot already be ready"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let store_permit = cache
            .admit_store()
            .expect("candidate should consume one Store admission");
        let committed = install(&cache, key.clone(), 2, b"new", "root-new");
        let retirement_probe = Arc::new(AtomicBool::new(false));

        let error = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(
                    Box::new(TestActor {
                        retirement_probe: Some(Arc::clone(&retirement_probe)),
                        ..TestActor::default()
                    }),
                    store_permit,
                ),
                WasmDocumentHandle(1),
                b"stale".as_slice().into(),
                FileBytesSha256::compute(b"stale"),
                Arc::<str>::from("root-stale"),
            )
            .await
            .expect_err("a committed actor must win over a stale cold candidate");
        assert_eq!(error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);
        assert!(retirement_probe.load(Ordering::Acquire));
        assert_eq!(cache.live_store_count(), 1);
        let pending = cache
            .admit_store()
            .expect("losing candidate must release its Store admission");
        assert_eq!(cache.live_store_count(), 2);
        drop(pending);
        assert_eq!(cache.observe(&key, "root-new").await.unwrap(), committed);
    }

    #[tokio::test]
    async fn stale_cold_install_never_replaces_a_committed_actor() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let cold_install = match cache.prepare_cold_open(&key, "root-stale").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("vacant key cannot already be ready"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let committed = install(&cache, key.clone(), 2, b"new", "root-new");
        let retirement_probe = Arc::new(AtomicBool::new(false));
        let error = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(
                    Box::new(TestActor {
                        retirement_probe: Some(Arc::clone(&retirement_probe)),
                        ..TestActor::default()
                    }),
                    cache
                        .admit_store()
                        .expect("cold candidate should receive Store admission"),
                ),
                WasmDocumentHandle(1),
                b"stale".as_slice().into(),
                FileBytesSha256::compute(b"stale"),
                Arc::<str>::from("root-stale"),
            )
            .await
            .expect_err("stale cold state must not replace a committed actor");
        assert_eq!(error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);
        assert!(retirement_probe.load(Ordering::Acquire));
        assert_eq!(cache.observe(&key, "root-new").await.unwrap(), committed);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn cold_open_replaces_the_exact_stale_same_key_actor() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let stale = install(&cache, key.clone(), 1, b"old", "root-old");
        let mut cold_install = match cache.prepare_cold_open(&key, "root-new").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("stale root cannot already be ready"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };
        let store_permit = cache
            .admit_cold_store(&mut cold_install)
            .expect("cold candidate should receive Store admission");

        let replacement = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(Box::new(TestActor::default()), store_permit),
                WasmDocumentHandle(2),
                b"new".as_slice().into(),
                FileBytesSha256::compute(b"new"),
                Arc::<str>::from("root-new"),
            )
            .await
            .expect("the captured stale actor should be replaced");

        assert_eq!(cache.observe(&key, "root-new").await.unwrap(), replacement);
        assert!(cache.lease(&stale).await.is_err());
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn stale_cold_token_does_not_replace_a_concurrent_same_slot_successor() {
        let cache = PluginActorCache::new(2).unwrap();
        let key = key("main", "/data.csv", "g1");
        let old = install(&cache, key.clone(), 1, b"old", "root-old");
        let cold_install = match cache.prepare_cold_open(&key, "root-cold").await.unwrap() {
            PluginActorColdOpen::Ready(_) => panic!("different root cannot already be ready"),
            PluginActorColdOpen::Build(cold_install) => cold_install,
        };

        let mut lease = cache.lease(&old).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                b"winner".as_slice().into(),
                FileBytesSha256::compute(b"winner"),
                Arc::<str>::from("root-winner"),
            )
            .unwrap();
        let winner = lease.commit_successor().await.unwrap();

        let error = cache
            .install_cold_if_absent(
                cold_install,
                key.clone(),
                PluginActorStore::new(
                    Box::new(TestActor::default()),
                    cache
                        .admit_store()
                        .expect("cold candidate should receive Store admission"),
                ),
                WasmDocumentHandle(3),
                b"cold".as_slice().into(),
                FileBytesSha256::compute(b"cold"),
                Arc::<str>::from("root-cold"),
            )
            .await
            .expect_err("a revised same-slot winner must not be replaced");
        assert_eq!(error.code, LixError::CODE_PLUGIN_OBSERVATION_STALE);
        assert_eq!(cache.observe(&key, "root-winner").await.unwrap(), winner);
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn cold_open_gate_serializes_builders() {
        let cache = PluginActorCache::new(2).unwrap();
        let first = cache.cold_open_guard().await;
        let second_cache = cache.clone();
        let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(async move {
            let _second = second_cache.cold_open_guard().await;
            let _ = entered_tx.send(());
        });
        tokio::task::yield_now().await;
        assert!(matches!(
            entered_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), entered_rx)
            .await
            .expect("second cold opener should acquire after the first releases")
            .expect("second cold opener should signal acquisition");
        waiter.await.unwrap();
    }
}
