//! Persistent, failure-isolated component plugin actors.
//!
//! A compiled component may be shared, but a mutable Component instance and
//! its document handles belong to exactly one branch/file actor.  This cache
//! deliberately keys path, incarnation, and plugin generation in addition to
//! the file id: none of those identities may be inferred from equal bytes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, TryAcquireError,
};

use super::incremental::FileBytesSha256;
use crate::wasm::{
    WasmComponentActor, WasmCreateContext, WasmDocumentCheckpoint, WasmDocumentHandle,
    WasmEntityKey,
};
use crate::{Blob, LixError};

pub(crate) const DEFAULT_MAX_LIVE_PLUGIN_STORES: usize = 10;
// The vscode-docs 303-path transition needs roughly 80-96 MiB to retain one
// decoded predecessor per touched file. At 64 MiB the cache reparses 115
// documents; 96 MiB retains the complete measured working set while remaining
// well below the aggregate live-Store budget.
const DEFAULT_MAX_DECODED_CHECKPOINT_BYTES: u64 = 96 * 1024 * 1024;
// One predecessor is enough for the required two-reader serialization while
// keeping each file actor's retained working set bounded.
pub(crate) const DEFAULT_MAX_PLUGIN_FILE_HISTORY: usize = 1;

/// Host-proven identities for schemas whose primary keys are allocated by the
/// mutation create context. Successors retain sparse persistent overlays rather
/// than cloning the complete document-sized set for every tiny edit.
#[derive(Clone)]
pub(crate) struct PluginEntityAuthorities {
    node: Arc<PluginEntityAuthorityNode>,
}

enum PluginEntityAuthorityNode {
    Base {
        ranges: Vec<PluginEntityAuthorityRange>,
        inserted: BTreeSet<WasmEntityKey>,
        removed: BTreeSet<WasmEntityKey>,
    },
    Delta {
        parent: PluginEntityAuthorities,
        inserted: BTreeSet<WasmEntityKey>,
        removed: BTreeSet<WasmEntityKey>,
        depth: u8,
    },
}

#[derive(Clone)]
pub(crate) struct PluginEntityAuthorityRange {
    schema_key: String,
    namespace: [u8; 12],
    first_local_ref: u32,
    last_local_ref: u32,
}

impl PluginEntityAuthorityRange {
    pub(crate) fn new(
        schema_key: String,
        creates: WasmCreateContext,
        first_local_ref: u32,
        last_local_ref: u32,
    ) -> Self {
        let uuid = creates
            .component_uuid_bytes(0)
            .expect("zero local ref always fits the create context");
        let mut namespace = [0_u8; 12];
        namespace.copy_from_slice(&uuid[..12]);
        Self {
            schema_key,
            namespace,
            first_local_ref,
            last_local_ref,
        }
    }

    fn contains(&self, key: &WasmEntityKey) -> bool {
        let [id] = key.entity_pk.as_slice() else {
            return false;
        };
        if key.schema_key.as_str() != self.schema_key {
            return false;
        }
        let Ok(bytes) = uuid::Uuid::parse_str(id).map(uuid::Uuid::into_bytes) else {
            return false;
        };
        if bytes[..12] != self.namespace {
            return false;
        }
        let local_ref = u32::from_be_bytes(
            bytes[12..]
                .try_into()
                .expect("UUID local-ref suffix is four bytes"),
        );
        self.first_local_ref <= local_ref && local_ref <= self.last_local_ref
    }
}

impl PluginEntityAuthorities {
    const MAX_DELTA_DEPTH: u8 = 16;

    pub(crate) fn empty() -> Self {
        Self::from_keys(BTreeSet::new())
    }

    pub(crate) fn from_keys(keys: BTreeSet<WasmEntityKey>) -> Self {
        Self {
            node: Arc::new(PluginEntityAuthorityNode::Base {
                ranges: Vec::new(),
                inserted: keys,
                removed: BTreeSet::new(),
            }),
        }
    }

    pub(crate) fn with_ranges(&self, ranges: Vec<PluginEntityAuthorityRange>) -> Self {
        if ranges.is_empty() {
            return self.clone();
        }
        let (mut existing_ranges, inserted, removed) = self.flatten();
        existing_ranges.extend(ranges);
        Self {
            node: Arc::new(PluginEntityAuthorityNode::Base {
                ranges: existing_ranges,
                inserted,
                removed,
            }),
        }
    }

    pub(crate) fn contains(&self, key: &WasmEntityKey) -> bool {
        match self.node.as_ref() {
            PluginEntityAuthorityNode::Base {
                ranges,
                inserted,
                removed,
            } => {
                if inserted.contains(key) {
                    true
                } else if removed.contains(key) {
                    false
                } else {
                    ranges.iter().any(|range| range.contains(key))
                }
            }
            PluginEntityAuthorityNode::Delta {
                parent,
                inserted,
                removed,
                ..
            } => {
                if inserted.contains(key) {
                    true
                } else if removed.contains(key) {
                    false
                } else {
                    parent.contains(key)
                }
            }
        }
    }

    pub(crate) fn with_delta(
        &self,
        inserted: BTreeSet<WasmEntityKey>,
        removed: BTreeSet<WasmEntityKey>,
    ) -> Self {
        if inserted.is_empty() && removed.is_empty() {
            return self.clone();
        }
        let depth = match self.node.as_ref() {
            PluginEntityAuthorityNode::Base { .. } => 1,
            PluginEntityAuthorityNode::Delta { depth, .. } => depth.saturating_add(1),
        };
        if depth > Self::MAX_DELTA_DEPTH {
            let (ranges, mut base_inserted, mut base_removed) = self.flatten();
            for key in removed {
                base_inserted.remove(&key);
                base_removed.insert(key);
            }
            for key in inserted {
                base_removed.remove(&key);
                base_inserted.insert(key);
            }
            return Self {
                node: Arc::new(PluginEntityAuthorityNode::Base {
                    ranges,
                    inserted: base_inserted,
                    removed: base_removed,
                }),
            };
        }
        Self {
            node: Arc::new(PluginEntityAuthorityNode::Delta {
                parent: self.clone(),
                inserted,
                removed,
                depth,
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let (_, inserted, removed) = self.flatten();
        inserted.len().saturating_sub(removed.len())
    }

    fn flatten(
        &self,
    ) -> (
        Vec<PluginEntityAuthorityRange>,
        BTreeSet<WasmEntityKey>,
        BTreeSet<WasmEntityKey>,
    ) {
        match self.node.as_ref() {
            PluginEntityAuthorityNode::Base {
                ranges,
                inserted,
                removed,
            } => (ranges.clone(), inserted.clone(), removed.clone()),
            PluginEntityAuthorityNode::Delta {
                parent,
                inserted,
                removed,
                ..
            } => {
                let (ranges, mut base_inserted, mut base_removed) = parent.flatten();
                for key in removed {
                    base_inserted.remove(key);
                    base_removed.insert(key.clone());
                }
                for key in inserted {
                    base_removed.remove(key);
                    base_inserted.insert(key.clone());
                }
                (ranges, base_inserted, base_removed)
            }
        }
    }

    pub(crate) fn encode_checkpoint(&self) -> Result<Vec<u8>, LixError> {
        let (ranges, inserted, removed) = self.flatten();
        let mut output = Vec::new();
        output.extend_from_slice(b"LIXAUT01");
        push_authority_len(&mut output, ranges.len())?;
        push_authority_len(&mut output, inserted.len())?;
        push_authority_len(&mut output, removed.len())?;
        for range in ranges {
            push_authority_text(&mut output, &range.schema_key)?;
            output.extend_from_slice(&range.namespace);
            output.extend_from_slice(&range.first_local_ref.to_le_bytes());
            output.extend_from_slice(&range.last_local_ref.to_le_bytes());
        }
        for key in inserted.iter().chain(removed.iter()) {
            push_authority_text(&mut output, key.schema_key.as_str())?;
            push_authority_len(&mut output, key.entity_pk.len())?;
            for component in &key.entity_pk {
                push_authority_text(&mut output, component.as_str())?;
            }
        }
        Ok(output)
    }

    pub(crate) fn encode_checkpoint_bounded(&self, max_bytes: usize) -> Option<Vec<u8>> {
        if self.checkpoint_encoded_upper_bound()? > max_bytes {
            return None;
        }
        let encoded = self.encode_checkpoint().ok()?;
        (encoded.len() <= max_bytes).then_some(encoded)
    }

    fn checkpoint_encoded_upper_bound(&self) -> Option<usize> {
        fn add_text(len: usize, text: &str) -> Option<usize> {
            len.checked_add(4)?.checked_add(text.len())
        }

        fn add_key(mut len: usize, key: &WasmEntityKey) -> Option<usize> {
            len = add_text(len, key.schema_key.as_str())?;
            len = len.checked_add(4)?;
            for component in &key.entity_pk {
                len = add_text(len, component.as_str())?;
            }
            Some(len)
        }

        fn add_keys<'a>(
            mut len: usize,
            keys: impl Iterator<Item = &'a WasmEntityKey>,
        ) -> Option<usize> {
            for key in keys {
                len = add_key(len, key)?;
            }
            Some(len)
        }

        fn node_upper_bound(node: &PluginEntityAuthorityNode) -> Option<usize> {
            match node {
                PluginEntityAuthorityNode::Base {
                    ranges,
                    inserted,
                    removed,
                } => {
                    let mut len = 20_usize;
                    for range in ranges {
                        len = add_text(len, &range.schema_key)?;
                        len = len.checked_add(20)?;
                    }
                    add_keys(len, inserted.iter().chain(removed.iter()))
                }
                PluginEntityAuthorityNode::Delta {
                    parent,
                    inserted,
                    removed,
                    ..
                } => add_keys(
                    node_upper_bound(parent.node.as_ref())?,
                    inserted.iter().chain(removed.iter()),
                ),
            }
        }

        node_upper_bound(self.node.as_ref())
    }

    pub(crate) fn decode_checkpoint(bytes: &[u8]) -> Result<Self, LixError> {
        let mut reader = AuthorityCheckpointReader::new(bytes);
        if reader.take(8)? != b"LIXAUT01" {
            return Err(invalid_authority_checkpoint());
        }
        let range_count = reader.len()?;
        let inserted_count = reader.len()?;
        let removed_count = reader.len()?;
        let mut ranges = Vec::new();
        for _ in 0..range_count {
            let schema_key = reader.text()?;
            let namespace: [u8; 12] = reader
                .take(12)?
                .try_into()
                .map_err(|_| invalid_authority_checkpoint())?;
            let first_local_ref = reader.u32()?;
            let last_local_ref = reader.u32()?;
            if first_local_ref > last_local_ref {
                return Err(invalid_authority_checkpoint());
            }
            ranges.push(PluginEntityAuthorityRange {
                schema_key,
                namespace,
                first_local_ref,
                last_local_ref,
            });
        }
        let mut read_keys = |count: usize| -> Result<BTreeSet<WasmEntityKey>, LixError> {
            let mut keys = BTreeSet::new();
            for _ in 0..count {
                let schema_key = reader.text()?;
                let component_count = reader.len()?;
                let mut entity_pk = Vec::new();
                for _ in 0..component_count {
                    entity_pk.push(reader.text()?);
                }
                if !keys.insert(WasmEntityKey::from_owned_parts(schema_key, entity_pk)) {
                    return Err(invalid_authority_checkpoint());
                }
            }
            Ok(keys)
        };
        let inserted = read_keys(inserted_count)?;
        let removed = read_keys(removed_count)?;
        if !reader.is_empty() || inserted.iter().any(|key| removed.contains(key)) {
            return Err(invalid_authority_checkpoint());
        }
        Ok(Self {
            node: Arc::new(PluginEntityAuthorityNode::Base {
                ranges,
                inserted,
                removed,
            }),
        })
    }
}

fn push_authority_len(output: &mut Vec<u8>, value: usize) -> Result<(), LixError> {
    let value = u32::try_from(value).map_err(|_| invalid_authority_checkpoint())?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn push_authority_text(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    push_authority_len(output, value.len())?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn invalid_authority_checkpoint() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PLUGIN,
        "plugin entity authority checkpoint is corrupt",
    )
}

struct AuthorityCheckpointReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> AuthorityCheckpointReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(invalid_authority_checkpoint)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(invalid_authority_checkpoint)?;
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| invalid_authority_checkpoint())?,
        ))
    }

    fn len(&mut self) -> Result<usize, LixError> {
        usize::try_from(self.u32()?).map_err(|_| invalid_authority_checkpoint())
    }

    fn text(&mut self) -> Result<String, LixError> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| invalid_authority_checkpoint())
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

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
    bytes_sha256: Option<FileBytesSha256>,
}

impl PluginObservation {
    pub(crate) fn key(&self) -> &PluginActorKey {
        &self.key
    }

    pub(crate) fn semantic_root(&self) -> &str {
        &self.semantic_root
    }

    pub(crate) fn bytes_sha256(&self) -> Option<FileBytesSha256> {
        self.bytes_sha256
    }
}

struct PluginActorAcceptedState {
    store: PluginActorStore,
    document: WasmDocumentHandle,
    bytes: Blob,
    bytes_sha256: Option<FileBytesSha256>,
    semantic_root: Arc<str>,
    entity_authorities: PluginEntityAuthorities,
    history: VecDeque<PluginActorHistoricalState>,
}

/// One instantiated Component Store together with the admission token that
/// keeps it within the repository-wide live-Store bound.
///
/// Field order is deliberate: Rust drops fields in declaration order, so the
/// actor (and its Wasmtime Store) is destroyed before the permit is returned.
pub(crate) struct PluginActorStore {
    actor: Box<dyn WasmComponentActor>,
    _store_permit: PluginActorStorePermit,
}

impl PluginActorStore {
    pub(crate) fn new(
        actor: Box<dyn WasmComponentActor>,
        store_permit: PluginActorStorePermit,
    ) -> Self {
        Self {
            actor,
            _store_permit: store_permit,
        }
    }

    pub(crate) fn actor_mut(&mut self) -> &mut dyn WasmComponentActor {
        self.actor.as_mut()
    }
}

struct PluginActorHistoricalState {
    revision: u64,
    document: WasmDocumentHandle,
    bytes: Blob,
    bytes_sha256: Option<FileBytesSha256>,
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
    checkpoints: BTreeMap<(PluginActorKey, String), PluginActorCheckpoint>,
    pending_checkpoints: BTreeMap<u64, PluginActorPendingCheckpoint>,
    checkpoint_bytes: u64,
    clock: u64,
    next_nonce: u64,
    next_checkpoint_nonce: u64,
}

struct PluginActorCheckpoint {
    checkpoint: WasmDocumentCheckpoint,
    last_used: u64,
}

struct PluginActorPendingCheckpoint {
    key: PluginActorKey,
    semantic_root: Arc<str>,
    checkpoint: WasmDocumentCheckpoint,
    last_used: u64,
}

/// Repository-local index and hard admission bound for per-file actors.
#[derive(Clone)]
pub(crate) struct PluginActorCache {
    capacity: NonZeroUsize,
    store_admission: Arc<Semaphore>,
    state: Arc<Mutex<PluginActorCacheState>>,
    cold_open_gate: Arc<AsyncMutex<()>>,
}

pub(crate) struct PluginActorStagedCheckpoint {
    cache: PluginActorCache,
    nonce: Option<u64>,
}

impl PluginActorStagedCheckpoint {
    pub(crate) fn publish(mut self) {
        if let Some(nonce) = self.nonce.take() {
            self.cache.publish_staged_checkpoint(nonce);
        }
    }
}

impl Drop for PluginActorStagedCheckpoint {
    fn drop(&mut self) {
        if let Some(nonce) = self.nonce.take() {
            self.cache.discard_staged_checkpoint(nonce);
        }
    }
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
        Self::new(DEFAULT_MAX_LIVE_PLUGIN_STORES)
            .expect("the default plugin actor capacity is nonzero")
    }
}

impl PluginActorCache {
    pub(crate) fn contains_observation(&self, observation: &PluginObservation) -> bool {
        let state = self.lock();
        state.actors.get(observation.key()).is_some_and(|slot| {
            !slot.retired.load(Ordering::Acquire)
                && slot.nonce == observation.actor_nonce
                && slot.revision.load(Ordering::Acquire) == observation.revision
        })
    }

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
                checkpoints: BTreeMap::new(),
                pending_checkpoints: BTreeMap::new(),
                checkpoint_bytes: 0,
                clock: 0,
                next_nonce: 1,
                next_checkpoint_nonce: 1,
            })),
            cold_open_gate: Arc::new(AsyncMutex::new(())),
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity.get()
    }

    /// Retains one decoded immutable arena root per actor identity. Unlike a
    /// Wasmtime Store, this checkpoint owns no guest linear memory and can be
    /// restored into a fresh actor after cache eviction.
    pub(crate) fn remember_checkpoint(
        &self,
        key: &PluginActorKey,
        semantic_root: &str,
        checkpoint: Option<WasmDocumentCheckpoint>,
    ) {
        let Some(checkpoint) = checkpoint else {
            return;
        };
        let retained_bytes = checkpoint.retained_bytes();
        if retained_bytes > DEFAULT_MAX_DECODED_CHECKPOINT_BYTES {
            return;
        }
        let mut state = self.lock();
        let stale_keys = state
            .checkpoints
            .keys()
            .filter(|(existing, _)| existing == key)
            .cloned()
            .collect::<Vec<_>>();
        for stale_key in stale_keys {
            if let Some(stale) = state.checkpoints.remove(&stale_key) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(stale.checkpoint.retained_bytes());
            }
        }
        state.clock = state.clock.wrapping_add(1);
        let last_used = state.clock;
        state.checkpoint_bytes = state.checkpoint_bytes.saturating_add(retained_bytes);
        state.checkpoints.insert(
            (key.clone(), semantic_root.to_owned()),
            PluginActorCheckpoint {
                checkpoint,
                last_used,
            },
        );
        while state.checkpoint_bytes > DEFAULT_MAX_DECODED_CHECKPOINT_BYTES {
            if !evict_oldest_checkpoint(&mut state) {
                break;
            }
        }
    }

    pub(crate) fn checkpoint(
        &self,
        key: &PluginActorKey,
        semantic_root: &str,
    ) -> Option<WasmDocumentCheckpoint> {
        let mut state = self.lock();
        state.clock = state.clock.wrapping_add(1);
        let last_used = state.clock;
        state
            .checkpoints
            .get_mut(&(key.clone(), semantic_root.to_owned()))
            .map(|entry| {
                entry.last_used = last_used;
                entry.checkpoint.clone()
            })
    }

    pub(crate) fn stage_checkpoint(
        &self,
        key: PluginActorKey,
        semantic_root: Arc<str>,
        checkpoint: Option<WasmDocumentCheckpoint>,
    ) -> Option<PluginActorStagedCheckpoint> {
        let checkpoint = checkpoint?;
        let retained_bytes = checkpoint.retained_bytes();
        if retained_bytes > DEFAULT_MAX_DECODED_CHECKPOINT_BYTES {
            return None;
        }
        let mut state = self.lock();
        state.clock = state.clock.wrapping_add(1);
        let last_used = state.clock;
        let nonce = state.next_checkpoint_nonce;
        state.next_checkpoint_nonce = state.next_checkpoint_nonce.wrapping_add(1).max(1);
        state.checkpoint_bytes = state.checkpoint_bytes.saturating_add(retained_bytes);
        state.pending_checkpoints.insert(
            nonce,
            PluginActorPendingCheckpoint {
                key,
                semantic_root,
                checkpoint,
                last_used,
            },
        );
        while state.checkpoint_bytes > DEFAULT_MAX_DECODED_CHECKPOINT_BYTES {
            if !evict_oldest_checkpoint(&mut state) {
                break;
            }
        }
        drop(state);
        Some(PluginActorStagedCheckpoint {
            cache: self.clone(),
            nonce: Some(nonce),
        })
    }

    fn publish_staged_checkpoint(&self, nonce: u64) {
        let mut state = self.lock();
        let Some(mut pending) = state.pending_checkpoints.remove(&nonce) else {
            return;
        };
        let stale_keys = state
            .checkpoints
            .keys()
            .filter(|(existing, _)| existing == &pending.key)
            .cloned()
            .collect::<Vec<_>>();
        for stale_key in stale_keys {
            if let Some(stale) = state.checkpoints.remove(&stale_key) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(stale.checkpoint.retained_bytes());
            }
        }
        state.clock = state.clock.wrapping_add(1);
        pending.last_used = state.clock;
        state.checkpoints.insert(
            (pending.key, pending.semantic_root.to_string()),
            PluginActorCheckpoint {
                checkpoint: pending.checkpoint,
                last_used: pending.last_used,
            },
        );
    }

    fn discard_staged_checkpoint(&self, nonce: u64) {
        let mut state = self.lock();
        if let Some(pending) = state.pending_checkpoints.remove(&nonce) {
            state.checkpoint_bytes = state
                .checkpoint_bytes
                .saturating_sub(pending.checkpoint.retained_bytes());
        }
    }

    fn forget_checkpoints(&self, key: &PluginActorKey) {
        let mut state = self.lock();
        let stale_keys = state
            .checkpoints
            .keys()
            .filter(|(existing, _)| existing == key)
            .cloned()
            .collect::<Vec<_>>();
        for stale_key in stale_keys {
            if let Some(stale) = state.checkpoints.remove(&stale_key) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(stale.checkpoint.retained_bytes());
            }
        }
    }

    /// Serializes cold actor construction. The gate is repository-wide rather
    /// than per-key because cold opens are uncommon and may otherwise retain
    /// multiple full semantic snapshots plus Wasm Stores concurrently.
    pub(crate) async fn cold_open_guard(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.cold_open_gate).lock_owned().await
    }

    /// Reserves one repository-wide live Store slot before a Component actor is
    /// instantiated. This intentionally fails fast instead of waiting: a
    /// transaction may already retain a pending actor through its durable
    /// commit point, so waiting could deadlock the transaction against itself.
    pub(crate) fn admit_store(&self) -> Result<PluginActorStorePermit, LixError> {
        loop {
            let permit = self.try_acquire_store()?;
            if let Some(permit) = permit {
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
        let permit = self.try_acquire_store()?;
        if let Some(permit) = permit {
            return Ok(permit);
        }
        if Self::drop_detached_retired_cold_predecessor(cold_install) {
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
    #[cfg(test)]
    pub(crate) fn install(
        &self,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        semantic_root: impl Into<Arc<str>>,
    ) -> PluginObservation {
        self.install_with_authorities(
            key,
            store,
            document,
            bytes,
            semantic_root,
            PluginEntityAuthorities::empty(),
        )
    }

    pub(crate) fn install_with_authorities(
        &self,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        semantic_root: impl Into<Arc<str>>,
        entity_authorities: PluginEntityAuthorities,
    ) -> PluginObservation {
        let semantic_root = semantic_root.into();
        let bytes_sha256 = Some(FileBytesSha256::compute(&bytes));
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
                entity_authorities,
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
    #[cfg(test)]
    pub(crate) async fn install_cold_if_absent(
        &self,
        cold_install: PluginActorColdInstall,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        bytes_sha256: impl Into<Option<FileBytesSha256>>,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<PluginObservation, LixError> {
        self.install_cold_if_absent_with_authorities(
            cold_install,
            key,
            store,
            document,
            bytes,
            bytes_sha256,
            semantic_root,
            PluginEntityAuthorities::empty(),
        )
        .await
    }

    pub(crate) async fn install_cold_if_absent_with_authorities(
        &self,
        cold_install: PluginActorColdInstall,
        key: PluginActorKey,
        store: PluginActorStore,
        document: WasmDocumentHandle,
        bytes: Blob,
        bytes_sha256: impl Into<Option<FileBytesSha256>>,
        semantic_root: impl Into<Arc<str>>,
        entity_authorities: PluginEntityAuthorities,
    ) -> Result<PluginObservation, LixError> {
        let semantic_root = semantic_root.into();
        let bytes_sha256 = bytes_sha256.into();
        if cold_install.key != key {
            let mut store = store;
            let _ = store.actor.drop_document(document).await;
            let _ = store.actor.retire().await;
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "cold plugin actor install token belongs to a different key",
            ));
        }
        let mut candidate = Some((store, document, bytes, bytes_sha256, entity_authorities));
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
                let (store, document, bytes, bytes_sha256, entity_authorities) = candidate
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
                        entity_authorities,
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

        let (mut store, document, _, _, _) =
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
        evicted.is_some_and(|slot| {
            slot.retire();
            drop(slot);
            true
        })
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
    fn drop_detached_retired_cold_predecessor(cold_install: &mut PluginActorColdInstall) -> bool {
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

fn evict_oldest_checkpoint(state: &mut PluginActorCacheState) -> bool {
    let committed = state
        .checkpoints
        .iter()
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(key, entry)| (entry.last_used, key.clone()));
    let pending = state
        .pending_checkpoints
        .iter()
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(nonce, entry)| (entry.last_used, *nonce));
    match (committed, pending) {
        (None, None) => false,
        (Some((_, key)), None) => {
            if let Some(evicted) = state.checkpoints.remove(&key) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(evicted.checkpoint.retained_bytes());
            }
            true
        }
        (None, Some((_, nonce))) => {
            if let Some(evicted) = state.pending_checkpoints.remove(&nonce) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(evicted.checkpoint.retained_bytes());
            }
            true
        }
        (Some((committed_used, key)), Some((pending_used, nonce))) => {
            if committed_used <= pending_used {
                if let Some(evicted) = state.checkpoints.remove(&key) {
                    state.checkpoint_bytes = state
                        .checkpoint_bytes
                        .saturating_sub(evicted.checkpoint.retained_bytes());
                }
            } else if let Some(evicted) = state.pending_checkpoints.remove(&nonce) {
                state.checkpoint_bytes = state
                    .checkpoint_bytes
                    .saturating_sub(evicted.checkpoint.retained_bytes());
            }
            true
        }
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
        "finish transactions holding existing-document plugin leases, or raise EngineOptions::with_plugin_resource_limits",
    )
}

struct PluginActorSuccessor {
    document: WasmDocumentHandle,
    checkpoint: Option<WasmDocumentCheckpoint>,
    bytes: Blob,
    bytes_sha256: Option<FileBytesSha256>,
    semantic_root: Arc<str>,
    entity_authorities: PluginEntityAuthorities,
}

/// Opaque authority for one guest call based on the actor's latest private
/// state. The token owns any prior pending successor while the call is in
/// flight so deterministic rejection can restore it exactly.
pub(crate) struct PluginActorPendingCall {
    document: WasmDocumentHandle,
    bytes: Blob,
    semantic_root: Arc<str>,
    entity_authorities: PluginEntityAuthorities,
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

    pub(crate) fn entity_authorities(&self) -> &PluginEntityAuthorities {
        &self.entity_authorities
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
    observed_bytes_sha256: Option<FileBytesSha256>,
    uncertain_guest_call: bool,
    successor: Option<PluginActorSuccessor>,
}

impl PluginActorLease {
    pub(crate) fn actor_mut(&mut self) -> &mut dyn WasmComponentActor {
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
    pub(crate) fn accepted_bytes_sha256(&self) -> Option<FileBytesSha256> {
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

    pub(crate) fn observed_bytes_sha256(&self) -> Option<FileBytesSha256> {
        self.observed_bytes_sha256
    }

    pub(crate) fn accepted_semantic_root(&self) -> &str {
        &self
            .guard
            .as_deref()
            .expect("actor lease guard exists")
            .semantic_root
    }

    pub(crate) fn accepted_entity_authorities(&self) -> &PluginEntityAuthorities {
        &self
            .guard
            .as_deref()
            .expect("actor lease guard exists")
            .entity_authorities
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
        let (document, bytes, semantic_root, entity_authorities) =
            if let Some(successor) = previous_successor.as_ref() {
                (
                    successor.document,
                    successor.bytes.clone(),
                    Arc::clone(&successor.semantic_root),
                    successor.entity_authorities.clone(),
                )
            } else {
                let accepted = self.guard.as_deref().expect("actor lease guard exists");
                (
                    accepted.document,
                    accepted.bytes.clone(),
                    Arc::clone(&accepted.semantic_root),
                    accepted.entity_authorities.clone(),
                )
            };
        self.uncertain_guest_call = true;
        Ok(PluginActorPendingCall {
            document,
            bytes,
            semantic_root,
            entity_authorities,
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
        checkpoint: Option<WasmDocumentCheckpoint>,
        bytes: Blob,
        bytes_sha256: impl Into<Option<FileBytesSha256>>,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<(), LixError> {
        let bytes_sha256 = bytes_sha256.into();
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
            checkpoint,
            bytes,
            bytes_sha256,
            semantic_root: semantic_root.into(),
            entity_authorities: call.entity_authorities.clone(),
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
        checkpoint: Option<WasmDocumentCheckpoint>,
        bytes: Blob,
        bytes_sha256: impl Into<Option<FileBytesSha256>>,
        semantic_root: impl Into<Arc<str>>,
    ) -> Result<(), LixError> {
        let bytes_sha256 = bytes_sha256.into();
        if !self.uncertain_guest_call || self.successor.is_some() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin guest completion did not match one in-flight transition",
            ));
        }
        self.uncertain_guest_call = false;
        self.successor = Some(PluginActorSuccessor {
            document,
            checkpoint,
            bytes,
            bytes_sha256,
            semantic_root: semantic_root.into(),
            entity_authorities: self.accepted_entity_authorities().clone(),
        });
        Ok(())
    }

    /// Replaces the host-proven identity set for the staged successor. This is
    /// called only after create materialization and exact validation have
    /// succeeded, so guest-provided keys never become authority by assertion.
    pub(crate) fn set_successor_entity_authorities(
        &mut self,
        entity_authorities: PluginEntityAuthorities,
    ) -> Result<(), LixError> {
        let successor = self.successor.as_mut().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin authority publication is missing a validated successor",
            )
        })?;
        successor.entity_authorities = entity_authorities;
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

    pub(crate) fn successor_checkpoint(
        &self,
    ) -> Option<(PluginActorCache, Arc<str>, Option<WasmDocumentCheckpoint>)> {
        self.successor.as_ref().map(|successor| {
            (
                self.cache.clone(),
                Arc::clone(&successor.semantic_root),
                successor.checkpoint.clone(),
            )
        })
    }

    pub(crate) fn successor_entity_authorities(&self) -> Option<&PluginEntityAuthorities> {
        self.successor
            .as_ref()
            .map(|successor| &successor.entity_authorities)
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
            let _old_entity_authorities = std::mem::replace(
                &mut accepted.entity_authorities,
                successor.entity_authorities.clone(),
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
        if self.key != successor_key {
            self.cache.forget_checkpoints(&self.key);
        }
        self.cache.remember_checkpoint(
            &successor_key,
            &successor.semantic_root,
            successor.checkpoint,
        );
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
        WasmChangeCursorHandle, WasmChangePage, WasmComponentActor, WasmEditCursorHandle,
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
    impl WasmComponentActor for TestActor {
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
            plugin_key: "plugin_csv".to_owned(),
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

    #[test]
    fn decoded_checkpoints_require_exact_actor_and_semantic_root() {
        let cache = PluginActorCache::new(2).unwrap();
        let actor_key = key("main", "/data.csv", "g1");
        cache.remember_checkpoint(
            &actor_key,
            "root-1",
            Some(WasmDocumentCheckpoint::new(42_u64, 128)),
        );

        assert_eq!(
            cache
                .checkpoint(&actor_key, "root-1")
                .and_then(|checkpoint| checkpoint.downcast_ref::<u64>().copied()),
            Some(42)
        );
        assert!(cache.checkpoint(&actor_key, "root-2").is_none());
        assert!(
            cache
                .checkpoint(&key("main", "/other.csv", "g1"), "root-1")
                .is_none()
        );
    }

    #[test]
    fn pending_and_published_checkpoints_share_one_hard_budget() {
        let cache = PluginActorCache::new(2).unwrap();
        let first_key = key("main", "/first.csv", "g1");
        let second_key = key("main", "/second.csv", "g1");
        let retained_bytes = DEFAULT_MAX_DECODED_CHECKPOINT_BYTES / 2 + 1;
        let first = cache
            .stage_checkpoint(
                first_key.clone(),
                Arc::from("root-1"),
                Some(WasmDocumentCheckpoint::new(1_u64, retained_bytes)),
            )
            .unwrap();
        let second = cache
            .stage_checkpoint(
                second_key.clone(),
                Arc::from("root-2"),
                Some(WasmDocumentCheckpoint::new(2_u64, retained_bytes)),
            )
            .unwrap();

        assert!(cache.checkpoint(&second_key, "root-2").is_none());
        first.publish();
        second.publish();
        assert!(cache.checkpoint(&first_key, "root-1").is_none());
        assert_eq!(
            cache
                .checkpoint(&second_key, "root-2")
                .and_then(|checkpoint| checkpoint.downcast_ref::<u64>().copied()),
            Some(2)
        );
        assert!(cache.lock().checkpoint_bytes <= DEFAULT_MAX_DECODED_CHECKPOINT_BYTES);
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
                None,
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
    async fn entity_authority_delta_publishes_only_with_successor_commit() {
        let cache = PluginActorCache::new(2).unwrap();
        let actor_key = key("main", "/data.json", "g1");
        let before_key =
            WasmEntityKey::from_owned_parts("json_node".to_owned(), vec!["before".to_owned()]);
        let after_key =
            WasmEntityKey::from_owned_parts("json_node".to_owned(), vec!["after".to_owned()]);
        let before = PluginEntityAuthorities::from_keys(BTreeSet::from([before_key.clone()]));
        let observation = cache.install_with_authorities(
            actor_key,
            PluginActorStore::new(Box::new(TestActor::default()), cache.admit_store().unwrap()),
            WasmDocumentHandle(1),
            b"before".as_slice().into(),
            Arc::<str>::from("root-1"),
            before,
        );
        let mut lease = cache.lease(&observation).await.unwrap();
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                None,
                b"after".as_slice().into(),
                FileBytesSha256::compute(b"after"),
                Arc::<str>::from("root-2"),
            )
            .unwrap();
        let successor_authorities = lease.accepted_entity_authorities().with_delta(
            BTreeSet::from([after_key.clone()]),
            BTreeSet::from([before_key.clone()]),
        );
        lease
            .set_successor_entity_authorities(successor_authorities)
            .unwrap();

        assert!(lease.accepted_entity_authorities().contains(&before_key));
        assert!(!lease.accepted_entity_authorities().contains(&after_key));
        let successor = lease.commit_successor().await.unwrap();
        let committed = cache.lease(&successor).await.unwrap();
        assert!(
            !committed
                .accepted_entity_authorities()
                .contains(&before_key)
        );
        assert!(committed.accepted_entity_authorities().contains(&after_key));
    }

    #[test]
    fn entity_authority_deltas_compact_without_losing_membership() {
        let retained =
            WasmEntityKey::from_owned_parts("node".to_owned(), vec!["retained".to_owned()]);
        let mut authorities =
            PluginEntityAuthorities::from_keys(BTreeSet::from([retained.clone()]));
        for ordinal in 0..=PluginEntityAuthorities::MAX_DELTA_DEPTH {
            let inserted = WasmEntityKey::from_owned_parts(
                "node".to_owned(),
                vec![format!("inserted-{ordinal}")],
            );
            authorities = authorities.with_delta(BTreeSet::from([inserted]), BTreeSet::new());
        }
        assert!(authorities.contains(&retained));
        assert_eq!(
            authorities.len(),
            usize::from(PluginEntityAuthorities::MAX_DELTA_DEPTH) + 2
        );
    }

    #[test]
    fn compact_entity_authority_ranges_preserve_sparse_overrides() {
        let creates = WasmCreateContext {
            high: 0x0123_4567_89ab_cdef,
            low: 0xfedc_ba98,
        };
        let key = |local_ref| {
            WasmEntityKey::from_owned_parts(
                "markdown_block".to_owned(),
                creates
                    .entity_pk(local_ref)
                    .expect("test local ref should fit"),
            )
        };
        let inside = key(12);
        let removed = key(13);
        let outside = key(20);
        let authorities = PluginEntityAuthorities::empty()
            .with_ranges(vec![PluginEntityAuthorityRange::new(
                "markdown_block".to_owned(),
                creates,
                10,
                15,
            )])
            .with_delta(BTreeSet::new(), BTreeSet::from([removed.clone()]))
            .with_delta(BTreeSet::from([outside.clone()]), BTreeSet::new());

        assert!(authorities.contains(&inside));
        assert!(!authorities.contains(&removed));
        assert!(authorities.contains(&outside));
        assert!(!authorities.contains(&key(9)));
    }

    #[test]
    fn entity_authority_checkpoint_roundtrips_ranges_and_sparse_overrides() {
        let creates = WasmCreateContext {
            high: 0x0123_4567_89ab_cdef,
            low: 0xfedc_ba98,
        };
        let key = |local_ref| {
            WasmEntityKey::from_owned_parts(
                "row".to_owned(),
                creates.entity_pk(local_ref).expect("local ref should fit"),
            )
        };
        let retained = key(2);
        let removed = key(3);
        let inserted = key(9);
        let authorities = PluginEntityAuthorities::empty()
            .with_ranges(vec![PluginEntityAuthorityRange::new(
                "row".to_owned(),
                creates,
                1,
                4,
            )])
            .with_delta(BTreeSet::new(), BTreeSet::from([removed.clone()]))
            .with_delta(BTreeSet::from([inserted.clone()]), BTreeSet::new());

        let decoded =
            PluginEntityAuthorities::decode_checkpoint(&authorities.encode_checkpoint().unwrap())
                .unwrap();
        assert!(decoded.contains(&retained));
        assert!(!decoded.contains(&removed));
        assert!(decoded.contains(&inserted));
    }

    #[test]
    fn entity_authority_checkpoint_respects_optional_byte_bound() {
        let authorities = PluginEntityAuthorities::empty();
        let encoded = authorities.encode_checkpoint().unwrap();

        assert!(
            authorities
                .encode_checkpoint_bounded(encoded.len() - 1)
                .is_none()
        );
        assert_eq!(
            authorities
                .encode_checkpoint_bounded(encoded.len())
                .unwrap(),
            encoded
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
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
        assert_eq!(before.bytes_sha256(), Some(before_hash));

        let mut lease = cache.lease(&before).await.unwrap();
        assert_eq!(lease.observed_bytes_sha256(), Some(before_hash));
        assert_eq!(lease.accepted_bytes_sha256(), Some(before_hash));
        lease.begin_guest_call().unwrap();
        lease
            .complete_guest_call(
                WasmDocumentHandle(2),
                None,
                b"after".as_slice().into(),
                after_hash,
                Arc::<str>::from("root-2"),
            )
            .unwrap();
        let after = lease.commit_successor().await.unwrap();
        assert_eq!(after.bytes_sha256(), Some(after_hash));

        let historical = cache.lease_for_transition(&before).await.unwrap();
        assert_eq!(historical.observed_bytes_sha256(), Some(before_hash));
        assert_eq!(historical.accepted_bytes_sha256(), Some(after_hash));

        drop(historical);
        let mut latest = cache.lease(&after).await.unwrap();
        latest.begin_guest_call().unwrap();
        latest
            .complete_guest_call(
                WasmDocumentHandle(3),
                None,
                b"later".as_slice().into(),
                None,
                Arc::<str>::from("root-3"),
            )
            .unwrap();
        let later = latest.commit_successor().await.unwrap();
        assert_eq!(later.bytes_sha256(), None);
        assert_eq!(
            cache
                .lease_for_transition(&later)
                .await
                .unwrap()
                .observed_bytes_sha256(),
            None
        );
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
                None,
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
                None,
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
                    None,
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
                None,
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
            "component transition deadline elapsed",
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

        let Err(error) = cache.admit_store() else {
            panic!("a live lease must keep its Store admitted");
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

        let Err(error) = cache.admit_cold_store(&mut cold_install) else {
            panic!("a leased stale Store must not be overcommitted");
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
                None,
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
