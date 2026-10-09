#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
#[cfg(test)]
use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::branch::{BranchHeadControl, BranchHeadControlContext};
use crate::changelog::{ChangeLoadRequest, ChangelogContext, ChangelogReader, CommitId};
use crate::commit_graph::CommitGraphContext;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, build_path_index, load_path_index_revision,
};
use crate::hot_state::tracked_head::{
    BoundedLiveIdentityScan, HotStateTransactionCache, TrackedHeadContext,
};
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateProjection, HotStateReadDomain,
    HotStateReader, HotStateRowFilter, HotStateRowRequest, HotStateScanRequest,
    MaterializedHotStateBatch, MaterializedHotStateBatchBuilder, MaterializedHotStateExactBatch,
    MaterializedHotStateRow, MaterializedHotStateRowRef, VisibilityBranchScope, VisibilityRequest,
    expanded_branch_ids, resolve_visible_batch,
};
use crate::row_pk::RowPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiff, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use std::mem::size_of;
use std::sync::Mutex as StdMutex;

use super::derived::{
    is_derived_only_request, is_derived_schema, request_may_include_derived, scan_derived_rows,
};
use super::visibility::{OrderedVisibilityRun, resolve_visible_ordered_runs};

const BRANCH_READ_CONCURRENCY: usize = 8;
const DOMINANT_BRANCH_MIN_ROWS: usize = 512;
const SMALL_GLOBAL_OVERLAY_MAX_ROWS: usize = 256;
const ROW_COLUMNAR_LAYOUT_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;
const ROW_COLUMNAR_LAYOUT_CACHE_MAX_ENTRIES: usize = 16;
const EXACT_COUNT_GLOBAL_MAX_ENTRIES: usize = 128;
const EXACT_COUNT_GLOBAL_MAX_BYTES: usize = 512 * 1024;
const TRANSACTION_BRANCH_HEAD_CONTROL_CACHE_MAX_ENTRIES: usize = 64;
type BranchHeads = std::collections::BTreeMap<String, BranchHeadControl>;

/// One root-local diff candidate batch retained during this authority's
/// preparation call. The caller supplies it only for an accepted moving
/// working-diff recipe; this context still checks the captured branch control
/// before using the identities for checkpoint mutation paths.
pub(crate) struct PreparedWorkingDiffMutationCandidates {
    pub(crate) branch_id: String,
    pub(crate) checkpoint_commit_id: String,
    pub(crate) head_commit_id: String,
    pub(crate) relation: String,
    pub(crate) filter: TrackedStateFilter,
    pub(crate) retain_payloads: bool,
    pub(crate) projected_columns: Vec<String>,
    pub(crate) diff: std::sync::Arc<TrackedStateDiff>,
}

/// Transaction-local branch publication controls.
///
/// A transaction is fenced by the tracked mutation revision observed when it
/// opens, so repeatedly loading the same immutable generation selector only
/// adds storage round trips. Missing controls are cached as well: branch
/// creation rotates that revision and therefore conflicts with the pinned
/// transaction before commit.
#[derive(Default)]
pub(crate) struct BranchHeadControlCache {
    controls: StdMutex<std::collections::BTreeMap<String, Option<BranchHeadControl>>>,
    hot_state: std::sync::Arc<HotStateTransactionCache>,
}

/// Engine bookkeeping rows that live in the global branch's untracked
/// `lix_key_value` plane and are consulted on **every** transaction open.
///
/// Resolving one costs a projected live batch read plus, on a miss, a point
/// read of the native deterministic-identity presence witness. Both are
/// functions of the global branch-head
/// control's generation and current-state revision, and every write to that
/// plane republishes the control under a CAS with a bumped
/// `current_state_revision`. An unchanged control therefore proves the
/// resolved row, and the closure validated alongside it, unchanged.
///
/// Disposable cache: tagged with the exact control it was resolved under,
/// rebuilt from canonical records on any change, never an authority.
#[derive(Debug, Default)]
pub(crate) struct GlobalKeyValueRowCache {
    entries: StdMutex<Vec<(BranchHeadControl, String, Option<serde_json::Value>)>>,
}

const GLOBAL_KEY_VALUE_ROW_CACHE_MAX_ENTRIES: usize = 8;

impl GlobalKeyValueRowCache {
    pub(crate) fn get(
        &self,
        control: BranchHeadControl,
        key: &str,
    ) -> Option<Option<serde_json::Value>> {
        let entries = self
            .entries
            .lock()
            .expect("global key-value row cache lock should not be poisoned");
        entries
            .iter()
            .find(|(entry_control, entry_key, _)| *entry_control == control && entry_key == key)
            .map(|(_, _, row)| row.clone())
    }

    pub(crate) fn insert(
        &self,
        control: BranchHeadControl,
        key: &str,
        row: Option<serde_json::Value>,
    ) {
        let mut entries = self
            .entries
            .lock()
            .expect("global key-value row cache lock should not be poisoned");
        // A control change retires every entry: they were all resolved under
        // the previous one and none of them can be reused.
        entries
            .retain(|(entry_control, entry_key, _)| *entry_control == control && entry_key != key);
        if entries.len() >= GLOBAL_KEY_VALUE_ROW_CACHE_MAX_ENTRIES {
            entries.remove(0);
        }
        entries.push((control, key.to_string(), row));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct RowColumnarLayoutCacheKey {
    branch_id: String,
    generation: CommitId,
    current_state_revision: u64,
    schema_key: String,
}

#[derive(Debug)]
struct CachedRowColumnarLayout {
    key: RowColumnarLayoutCacheKey,
    id: crate::columnar_row_group::RowGroupSetId,
    manifest: std::sync::Arc<crate::columnar_row_group::RowGroupManifest>,
    manifest_digest: [u8; 32],
    overlay: std::sync::Arc<Vec<crate::hot_state::RowColumnarOverlayRow>>,
    head_commit_id: CommitId,
    live_count: u64,
    bytes: usize,
}

#[derive(Debug, Default)]
struct RowColumnarLayoutCache {
    // Oldest entry first. Columnar planning is infrequent relative to batch
    // execution, so a tiny vector keeps the synchronization and bookkeeping
    // cost below that of a second index.
    entries: StdMutex<Vec<std::sync::Arc<CachedRowColumnarLayout>>>,
}

const EXCLUSIVE_CERTIFIED_BATCH_CACHE_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
struct ExclusiveCertifiedBatchCacheKey {
    branch_id: String,
    head_commit_id: CommitId,
    generation: CommitId,
    current_state_revision: u64,
    schema_key: String,
}

#[derive(Debug)]
struct ExclusiveCertifiedBatchCacheEntry {
    key: ExclusiveCertifiedBatchCacheKey,
    batch: std::sync::Arc<crate::tracked_state::EnvelopeCertifiedNativeProjectionBatch>,
}

#[derive(Debug, Default)]
struct ExclusiveCertifiedBatchCache {
    entry: StdMutex<Option<ExclusiveCertifiedBatchCacheEntry>>,
}

impl ExclusiveCertifiedBatchCache {
    fn get(
        &self,
        key: &ExclusiveCertifiedBatchCacheKey,
    ) -> Option<std::sync::Arc<crate::tracked_state::EnvelopeCertifiedNativeProjectionBatch>> {
        let batch = self
            .entry
            .lock()
            .expect("exclusive certified batch cache lock poisoned")
            .as_ref()
            .filter(|entry| entry.key == *key)
            .map(|entry| std::sync::Arc::clone(&entry.batch));
        if let Some(batch) = &batch {
            batch.mark_reused();
        }
        batch
    }

    fn insert(
        &self,
        key: ExclusiveCertifiedBatchCacheKey,
        batch: &std::sync::Arc<crate::tracked_state::EnvelopeCertifiedNativeProjectionBatch>,
    ) {
        if batch.cache_weight() > EXCLUSIVE_CERTIFIED_BATCH_CACHE_MAX_BYTES {
            return;
        }
        *self
            .entry
            .lock()
            .expect("exclusive certified batch cache lock poisoned") =
            Some(ExclusiveCertifiedBatchCacheEntry {
                key,
                batch: std::sync::Arc::clone(batch),
            });
    }
}

impl RowColumnarLayoutCache {
    fn get(
        &self,
        key: &RowColumnarLayoutCacheKey,
    ) -> Option<std::sync::Arc<CachedRowColumnarLayout>> {
        let mut entries = self
            .entries
            .lock()
            .expect("row columnar layout cache lock poisoned");
        let position = entries.iter().position(|entry| entry.key == *key)?;
        let entry = entries.remove(position);
        entries.push(std::sync::Arc::clone(&entry));
        Some(entry)
    }

    fn insert(
        &self,
        key: RowColumnarLayoutCacheKey,
        id: crate::columnar_row_group::RowGroupSetId,
        manifest: crate::columnar_row_group::RowGroupManifest,
        manifest_digest: [u8; 32],
        overlay: Vec<crate::hot_state::RowColumnarOverlayRow>,
        head_commit_id: CommitId,
        live_count: u64,
    ) -> std::sync::Arc<CachedRowColumnarLayout> {
        self.insert_with_max_bytes(
            key,
            id,
            manifest,
            manifest_digest,
            overlay,
            head_commit_id,
            live_count,
            ROW_COLUMNAR_LAYOUT_CACHE_MAX_BYTES,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_with_max_bytes(
        &self,
        key: RowColumnarLayoutCacheKey,
        id: crate::columnar_row_group::RowGroupSetId,
        manifest: crate::columnar_row_group::RowGroupManifest,
        manifest_digest: [u8; 32],
        overlay: Vec<crate::hot_state::RowColumnarOverlayRow>,
        head_commit_id: CommitId,
        live_count: u64,
        max_bytes: usize,
    ) -> std::sync::Arc<CachedRowColumnarLayout> {
        let overlay = std::sync::Arc::new(overlay);
        // Capacity accounting covers every owned buffer. A 2x admission
        // margin conservatively absorbs allocator and HashMap control-byte
        // overhead that Rust's collections do not expose.
        let bytes =
            estimated_row_columnar_layout_bytes(&key, &manifest, &overlay, overlay.capacity())
                .saturating_mul(2);
        let manifest = std::sync::Arc::new(manifest);
        let entry = std::sync::Arc::new(CachedRowColumnarLayout {
            key,
            id,
            manifest,
            manifest_digest,
            overlay,
            head_commit_id,
            live_count,
            bytes,
        });
        if bytes > max_bytes {
            return entry;
        }

        let mut entries = self
            .entries
            .lock()
            .expect("row columnar layout cache lock poisoned");
        // A newer state revision for one collection makes the older layout
        // useless to subsequent live readers. The exact revision key already
        // prevents stale hits; eager removal also bounds revision churn.
        entries.retain(|resident| {
            resident.key.branch_id != entry.key.branch_id
                || resident.key.schema_key != entry.key.schema_key
        });
        entries.push(std::sync::Arc::clone(&entry));
        let mut resident_bytes = entries.iter().map(|entry| entry.bytes).sum::<usize>();
        while resident_bytes > max_bytes || entries.len() > ROW_COLUMNAR_LAYOUT_CACHE_MAX_ENTRIES {
            resident_bytes = resident_bytes.saturating_sub(entries.remove(0).bytes);
        }
        entry
    }
}

fn estimated_row_columnar_layout_bytes(
    key: &RowColumnarLayoutCacheKey,
    manifest: &crate::columnar_row_group::RowGroupManifest,
    overlay: &[crate::hot_state::RowColumnarOverlayRow],
    overlay_capacity: usize,
) -> usize {
    let manifest_bytes = size_of::<crate::columnar_row_group::RowGroupManifest>()
        .saturating_add(manifest.estimated_heap_bytes());
    size_of::<CachedRowColumnarLayout>()
        .saturating_add(key.branch_id.capacity())
        .saturating_add(key.schema_key.capacity())
        .saturating_add(manifest_bytes)
        .saturating_add(
            overlay_capacity.saturating_mul(size_of::<crate::hot_state::RowColumnarOverlayRow>()),
        )
        .saturating_add(
            overlay
                .iter()
                .map(|row| {
                    row.row_pk
                        .estimated_heap_bytes()
                        .saturating_add(row.snapshot_content.as_ref().map_or(0, Bytes::len))
                        .saturating_add(row.raw_snapshot.as_ref().map_or(0, Bytes::len))
                })
                .sum(),
        )
}

/// Serving facade for visible live-state reads.
///
/// Normal rows are resolved from one durable hot-state projection. Each row
/// carries its own tracked|untracked retention, so readers do not route
/// through a separate retention index or merge retention candidates.
#[derive(Clone)]
pub(crate) struct HotStateContext {
    read_interest_registry: Option<std::sync::Arc<super::ReadInterestRegistry>>,
    partial_scope_policy: Option<super::PartialReadScopePolicy>,
    partial_scope_source: Option<std::sync::Arc<super::PartialReadScopeSource>>,
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
    row_columnar_layout_cache: std::sync::Arc<RowColumnarLayoutCache>,
    exclusive_certified_batch_cache: std::sync::Arc<ExclusiveCertifiedBatchCache>,
    row_columnar_scan_cache:
        std::sync::Arc<std::sync::Mutex<crate::hot_state::RowColumnarShadowMaskCache>>,
    row_decoded_column_cache: crate::hot_state::RowDecodedColumnCache,
    global_key_value_rows: std::sync::Arc<GlobalKeyValueRowCache>,
    root_base_cache: std::sync::Arc<crate::hot_state::tracked_head::RootBaseBatchCache>,
    prepared_read_rows: std::sync::Arc<std::sync::Mutex<PreparedReadRows>>,
}

impl HotStateContext {
    pub(crate) fn with_partial_scope_policy(&self, selected: &str, global: &str) -> Self {
        let mut scoped = self.clone();
        scoped.partial_scope_policy = Some(super::PartialReadScopePolicy::new(selected, global));
        scoped.partial_scope_source = None;
        scoped
    }
    pub(crate) fn with_partial_scope_source(&self, source: super::PartialReadScopeSource) -> Self {
        let mut scoped = self.clone();
        scoped.partial_scope_source = Some(std::sync::Arc::new(source));
        scoped.partial_scope_policy = None;
        scoped
    }
    pub(crate) fn with_partial_read_preparation_epoch(mut self, epoch: &str) -> Self {
        if let Some(policy) = &mut self.partial_scope_policy {
            policy.set_preparation_epoch(epoch);
        }
        self
    }

    pub(crate) fn capture_foreground_read_interests(
        &self,
    ) -> Option<(Self, std::sync::Arc<super::ReadInterestRegistry>)> {
        let parent = self.read_interest_registry.as_ref()?;
        if self.partial_scope_policy.is_none() && self.partial_scope_source.is_none() {
            return None;
        }
        let capture = super::ReadInterestRegistry::capture(std::sync::Arc::clone(parent));
        Some((self.with_read_interest_registry(capture.clone()), capture))
    }

    /// Planning still retains catalog interests globally, but catalog cache
    /// misses are not rows returned by this foreground query.
    pub(crate) fn without_foreground_read_capture(&self) -> Self {
        let mut context = self.clone();
        if let Some(parent) = self
            .read_interest_registry
            .as_ref()
            .and_then(|registry| registry.capture_parent())
        {
            context.read_interest_registry = Some(parent);
        }
        context
    }

    pub(crate) fn is_partial_replica(&self) -> bool {
        self.partial_scope_policy.is_some() || self.partial_scope_source.is_some()
    }

    /// Cached readers retain this stable registry, never an operation lease.
    /// Session entry points hold separate leases before opening storage.
    pub(crate) fn with_read_interest_registry(
        &self,
        registry: std::sync::Arc<super::ReadInterestRegistry>,
    ) -> Self {
        let mut scoped = self.clone();
        scoped.read_interest_registry = Some(registry);
        scoped
    }

    pub(crate) fn read_interest_registry(
        &self,
    ) -> Option<std::sync::Arc<super::ReadInterestRegistry>> {
        self.read_interest_registry.clone()
    }

    pub(crate) async fn begin_read_interest_operation(
        &self,
    ) -> Option<super::ReadInterestOperation> {
        match &self.read_interest_registry {
            Some(registry) => Some(registry.begin_operation().await),
            None => None,
        }
    }

    /// Engine-lifetime cache for the global untracked `lix_key_value` rows read
    /// at every transaction open, fenced by the global branch-head control.
    pub(crate) fn global_key_value_rows(&self) -> &GlobalKeyValueRowCache {
        &self.global_key_value_rows
    }

    pub(crate) fn new(
        tracked_state: TrackedStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        let row_columnar_array_budget =
            std::sync::Arc::new(crate::hot_state::RowColumnarArrayBudget::default());
        Self {
            read_interest_registry: None,
            partial_scope_policy: None,
            partial_scope_source: None,
            tracked_head: TrackedHeadContext::new(),
            commit_graph,
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
            row_columnar_layout_cache: std::sync::Arc::new(RowColumnarLayoutCache::default()),
            exclusive_certified_batch_cache: std::sync::Arc::default(),
            row_columnar_scan_cache: std::sync::Arc::new(std::sync::Mutex::new(
                crate::hot_state::RowColumnarShadowMaskCache::with_array_budget(
                    std::sync::Arc::clone(&row_columnar_array_budget),
                ),
            )),
            row_decoded_column_cache: crate::hot_state::RowDecodedColumnCache::with_array_budget(
                row_columnar_array_budget,
            ),
            global_key_value_rows: std::sync::Arc::new(GlobalKeyValueRowCache::default()),
            root_base_cache: std::sync::Arc::new(
                crate::hot_state::tracked_head::RootBaseBatchCache::with_tracked_state(
                    tracked_state,
                ),
            ),
            prepared_read_rows: std::sync::Arc::default(),
        }
    }

    pub(crate) fn row_columnar_scan_cache(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<crate::hot_state::RowColumnarShadowMaskCache>> {
        std::sync::Arc::clone(&self.row_columnar_scan_cache)
    }

    pub(crate) fn row_decoded_column_cache(&self) -> crate::hot_state::RowDecodedColumnCache {
        self.row_decoded_column_cache.clone()
    }

    /// Creates a visible live-state reader over a caller-provided KV store.
    /// Reuse completed immutable-root projections across candidate retries.
    /// Every mutable serving cache and read-interest registry stays private.
    pub(crate) fn fork_for_native_candidate(&self) -> Self {
        let mut candidate = Self::new(TrackedStateContext::new(), CommitGraphContext::new());
        candidate.root_base_cache = std::sync::Arc::clone(&self.root_base_cache);
        candidate
    }

    pub(crate) fn reader<S>(&self, store: S) -> HotStateContextReader<S>
    where
        S: StorageAdapterRead,
    {
        HotStateContextReader {
            read_interest_registry: self.read_interest_registry.clone(),
            partial_scope_policy: self.partial_scope_policy.clone(),
            partial_scope_source: self.partial_scope_source.clone(),
            resolved_partial_scope: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::clone(&self.row_columnar_layout_cache),
            exclusive_certified_batch_cache: std::sync::Arc::clone(
                &self.exclusive_certified_batch_cache,
            ),
            branch_head_control_cache: None,
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
            prepared_read_rows: std::sync::Arc::clone(&self.prepared_read_rows),
        }
    }

    /// Creates a reader whose branch generation selectors are pinned to one
    /// transaction-local cache.
    pub(crate) fn transaction_reader<S>(
        &self,
        store: S,
        branch_head_control_cache: std::sync::Arc<BranchHeadControlCache>,
    ) -> HotStateContextReader<S>
    where
        S: StorageAdapterRead,
    {
        HotStateContextReader {
            read_interest_registry: self.read_interest_registry.clone(),
            partial_scope_policy: self.partial_scope_policy.clone(),
            partial_scope_source: self.partial_scope_source.clone(),
            resolved_partial_scope: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::clone(&self.row_columnar_layout_cache),
            exclusive_certified_batch_cache: std::sync::Arc::clone(
                &self.exclusive_certified_batch_cache,
            ),
            branch_head_control_cache: Some(branch_head_control_cache),
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
            prepared_read_rows: std::sync::Arc::clone(&self.prepared_read_rows),
        }
    }

    /// Creates a reader whose row-level derived indexes are private to one
    /// retained storage snapshot. Those caches are not revision-tagged, so they
    /// cannot serve an older explicit transaction and stay per-snapshot.
    ///
    /// The filesystem path index is deliberately **not** among them. It is
    /// keyed by the `filesystem.path` revision, which
    /// [`stage_path_index_revision`](crate::filesystem::stage_path_index_revision)
    /// rewrites with a fresh UUIDv7 on every commit that changes the filesystem
    /// view. Equal revision therefore means equal view, and a reader pinned to
    /// an older snapshot loads the older revision from its own store and misses
    /// rather than being served a newer view. Correctness comes from the
    /// revision in the cache key, not from owning a private cache — and giving
    /// each snapshot reader its own empty cache guaranteed a full
    /// whole-repository rebuild for the first statement of every write
    /// transaction, which is the epoch-0 path in
    /// `TransactionSqlWriteExecutionContext::filesystem_path_index`.
    pub(crate) fn snapshot_reader<S>(&self, store: S) -> HotStateContextReader<S>
    where
        S: StorageAdapterRead,
    {
        HotStateContextReader {
            read_interest_registry: self.read_interest_registry.clone(),
            partial_scope_policy: self.partial_scope_policy.clone(),
            partial_scope_source: self.partial_scope_source.clone(),
            resolved_partial_scope: std::sync::Arc::new(tokio::sync::OnceCell::new()),
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::new(RowColumnarLayoutCache::default()),
            exclusive_certified_batch_cache: std::sync::Arc::clone(
                &self.exclusive_certified_batch_cache,
            ),
            branch_head_control_cache: None,
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
            prepared_read_rows: std::sync::Arc::clone(&self.prepared_read_rows),
        }
    }

    pub(crate) fn advance_filesystem_path_indexes(
        &self,
        previous_revision: Option<&[u8]>,
        next_revision: Option<&[u8]>,
        rows: &[MaterializedHotStateRow],
    ) {
        self.filesystem_path_index_cache
            .advance_committed(previous_revision, next_revision, rows);
    }
}

/// Visible live-state reader backed by a caller-provided KV store.
pub(crate) struct HotStateContextReader<S> {
    read_interest_registry: Option<std::sync::Arc<super::ReadInterestRegistry>>,
    partial_scope_policy: Option<super::PartialReadScopePolicy>,
    partial_scope_source: Option<std::sync::Arc<super::PartialReadScopeSource>>,
    store: S,
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
    row_columnar_layout_cache: std::sync::Arc<RowColumnarLayoutCache>,
    exclusive_certified_batch_cache: std::sync::Arc<ExclusiveCertifiedBatchCache>,
    branch_head_control_cache: Option<std::sync::Arc<BranchHeadControlCache>>,
    root_base_cache: std::sync::Arc<crate::hot_state::tracked_head::RootBaseBatchCache>,
    prepared_read_rows: std::sync::Arc<std::sync::Mutex<PreparedReadRows>>,
    resolved_partial_scope: std::sync::Arc<tokio::sync::OnceCell<super::PartialReadScopePolicy>>,
}

impl<S> HotStateContextReader<S>
where
    S: StorageAdapterRead,
{
    /// Replays the exact collection-control dependency without requesting
    /// count or ordered-identity authority. Partial candidate readers use
    /// this point-only path because their control-plane adapter does not
    /// support sibling-scope scans.
    pub(crate) async fn collection_generation_active_token(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<CommitId>, LixError> {
        let controls = load_branch_head_controls(
            &self.store,
            &[branch_id.to_owned()],
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        let Some(control) = controls.get(branch_id).copied() else {
            return Ok(None);
        };
        self.tracked_head
            .reader(&self.store)
            .collection_generation_active_token(branch_id, control.tracked_generation, scope)
            .await
            .map(Some)
    }

    pub(crate) async fn exact_count_with_bounded_global_overlay(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<u64>, LixError> {
        let filter = &request.filter;
        let [schema_key] = filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let [branch_id] = filter.branch_ids.as_slice() else {
            return Ok(None);
        };
        if *branch_id == GLOBAL_BRANCH_ID
            || filter.rows != HotStateRowFilter::All
            || !filter.row_pks.is_empty()
            || filter.row_pk_lower.is_some()
            || filter.row_pk_upper.is_some()
            || !filter.file_ids.is_empty()
            || filter.untracked.is_some()
            || filter.global.is_some()
            || !filter.constraints.is_empty()
            || filter.declared_column_eq.is_some()
            || filter.declared_column_range.is_some()
            || filter.include_tombstones
            || request.limit.is_some()
            || request.projection.columns.len() != 1
            || request.projection.columns[0] != "change_id"
            || request_may_include_derived(request)
            || *schema_key == "lix_registered_schema"
            || crate::schema::is_private_builtin_schema_key(schema_key)
            || self.partial_scope_policy.is_some()
            || self.partial_scope_source.is_some()
        {
            return Ok(None);
        }

        // The count is a logical full-scope read. Keep that broad dependency
        // even though its physical work below uses collection controls and a
        // small bounded identity probe.
        if let Some(operation) = &self.read_interest_registry {
            operation.register(super::LogicalReadInterest::scan(
                request,
                HotStateReadDomain::Combined,
            ))?;
        }

        let scope = scan_scope(
            &self.store,
            request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        if scope.projection_branch_ids.len() != 1
            || scope.projection_branch_ids[0] != *branch_id
            || scope
                .storage_branch_ids
                .iter()
                .any(|candidate| candidate != branch_id && candidate != GLOBAL_BRANCH_ID)
        {
            return Ok(None);
        }
        let Some(local_branch_control) = scope.branch_heads.get(branch_id).copied() else {
            return Ok(None);
        };
        let Some(global_branch_control) = scope.branch_heads.get(GLOBAL_BRANCH_ID).copied() else {
            return Ok(None);
        };
        let tracked = self.tracked_head.reader(&self.store);
        let collection_scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        let Some(local_collection) = tracked
            .stored_collection_generation(
                branch_id,
                local_branch_control.tracked_generation,
                collection_scope,
            )
            .await?
        else {
            return Ok(None);
        };
        let Some(global_collection) = tracked
            .stored_collection_generation(
                GLOBAL_BRANCH_ID,
                global_branch_control.tracked_generation,
                collection_scope,
            )
            .await?
        else {
            return Ok(None);
        };
        if local_collection.active_generation != local_branch_control.tracked_generation
            || global_collection.active_generation != global_branch_control.tracked_generation
            || local_collection.live_count == crate::collection_generation::DEFERRED_LIVE_COUNT
            || global_collection.live_count == crate::collection_generation::DEFERRED_LIVE_COUNT
            || global_collection.live_count > EXACT_COUNT_GLOBAL_MAX_ENTRIES as u64
        {
            return Ok(None);
        }
        for (candidate_branch_id, candidate_generation) in [
            (branch_id.as_str(), local_branch_control.tracked_generation),
            (GLOBAL_BRANCH_ID, global_branch_control.tracked_generation),
        ] {
            if tracked
                .schema_collection_count_may_be_stale(
                    candidate_branch_id,
                    candidate_generation,
                    schema_key,
                )
                .await?
            {
                // Legacy finite controls can predate newer collection fences,
                // and root-backed counts do not certify all inherited scope
                // members. Fall back to the ordinary visibility scan.
                return Ok(None);
            }
        }

        let mut visible_global_rows = 0_u64;
        if global_collection.live_count != 0 {
            let global_request = HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec![schema_key.to_owned()],
                    branch_ids: vec![GLOBAL_BRANCH_ID.to_owned()],
                    ..crate::hot_state::HotStateFilter::default()
                },
                projection: HotStateProjection {
                    columns: vec!["change_id".to_owned()],
                },
                limit: Some(EXACT_COUNT_GLOBAL_MAX_ENTRIES + 1),
            };
            if let Some(operation) = &self.read_interest_registry {
                operation.register(super::LogicalReadInterest::scan(
                    &global_request,
                    HotStateReadDomain::Combined,
                ))?;
            }
            let global_scan = TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema_key.to_owned()],
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: global_request.limit,
            };
            let candidates: Option<BoundedLiveIdentityScan> = tracked
                .try_scan_bounded_live_identities(
                    GLOBAL_BRANCH_ID,
                    global_branch_control,
                    &global_scan,
                    EXACT_COUNT_GLOBAL_MAX_ENTRIES,
                    EXACT_COUNT_GLOBAL_MAX_BYTES,
                )
                .await?;
            let Some(candidates) = candidates else {
                return Ok(None);
            };
            if candidates.identities.len() as u64 != global_collection.live_count {
                return Ok(None);
            }
            if candidates.identities.is_empty() {
                return Ok(None);
            }

            // Exact point reads decode current visibility from the full HOT
            // value, even when the requested projection omits its payload.
            // Keep them one identity at a time so 128 large global rows can
            // never become 128 simultaneously retained values.
            for (candidate_row_pk, candidate_file_id) in &candidates.identities {
                let exact = HotStateExactBatchRequest {
                    rows: vec![HotStateExactRowRequest {
                        schema_key: schema_key.to_owned(),
                        branch_id: branch_id.to_owned(),
                        row_pk: candidate_row_pk.clone(),
                        file_id: candidate_file_id.clone(),
                    }],
                    projection: HotStateProjection {
                        columns: vec!["change_id".to_owned()],
                    },
                    untracked: None,
                    include_tombstones: true,
                };
                // The broad Combined scan interest registered above already
                // captures this logical read. Avoid retaining one redundant
                // Exact recipe per global candidate.
                let resolved = self.load_exact_batch_without_read_interest(&exact).await?;
                if resolved.len() != 1 {
                    return Ok(None);
                }
                let Some(row) = resolved.row(0) else {
                    return Ok(None);
                };
                if row.global() && !row.deleted() {
                    visible_global_rows = visible_global_rows.checked_add(1).ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact count global overlay exceeds u64",
                        )
                    })?;
                }
            }
        }

        Ok(local_collection.live_count.checked_add(visible_global_rows))
    }

    async fn effective_partial_scope_policy(
        &self,
    ) -> Result<Option<&super::PartialReadScopePolicy>, LixError> {
        if let Some(source) = &self.partial_scope_source {
            return self
                .resolved_partial_scope
                .get_or_try_init(|| source.load(&self.store))
                .await
                .map(Some);
        }
        Ok(self.partial_scope_policy.as_ref())
    }

    pub(crate) async fn prepare_packed_identity_membership(
        &self,
        branch_id: &str,
        schema_key: &str,
    ) -> Result<Option<crate::hot_state::PackedIdentityMembership>, LixError> {
        if let Some(registry) = &self.read_interest_registry {
            registry.register(super::LogicalReadInterest::PackedIdentityMembership {
                branch_id: branch_id.to_owned(),
                schema_key: schema_key.to_owned(),
            })?;
        }
        let Some(cache) = self.branch_head_control_cache.as_ref() else {
            return Ok(None);
        };
        let controls =
            load_branch_head_controls(&self.store, &[branch_id.to_owned()], Some(cache.as_ref()))
                .await?;
        let Some(control) = controls.get(branch_id).copied() else {
            return Ok(None);
        };
        self.tracked_head
            .transaction_reader(&self.store, std::sync::Arc::clone(&cache.hot_state))
            .prepare_packed_identity_membership(branch_id, control.tracked_generation, schema_key)
            .await
    }

    /// Returns committed row identities under the same narrow visibility
    /// proof as [`Self::scan_direct_row_snapshots`]. The packed reader
    /// still resolves the authoritative current rows; callers may avoid JSON
    /// decoding only when their projected SQL fields are exact key components.
    pub(crate) async fn scan_direct_row_primary_keys(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        let Some((branch_id, control, schema_key)) =
            self.direct_row_snapshot_scope(request).await?
        else {
            return Ok(None);
        };
        self.tracked_head
            .reader(&self.store)
            .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache))
            .scan_row_primary_keys(
                &branch_id,
                control,
                &schema_key,
                &request.filter.row_pks,
                request.limit,
            )
            .await
            .map(Some)
    }

    /// Returns a bounded set of row-key candidates from the durable root or
    /// from a small authenticated packed-leaf prefix. This is only a candidate
    /// source for an unordered SQL LIMIT; the provider re-reads every key
    /// through `scan_batch` before returning it. Global rows remain eligible
    /// because the authoritative re-read applies local/global collision and
    /// tombstone rules. Rootless aliases, columnar data, and unsupported
    /// mutation directories decline instead of replaying all identities.
    pub(crate) async fn scan_direct_row_limit_candidates(
        &self,
        request: &HotStateScanRequest,
        candidate_limit: usize,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        if let Some(registry) = &self.read_interest_registry {
            registry.register(super::LogicalReadInterest::scan(
                request,
                match request.filter.untracked {
                    Some(true) => HotStateReadDomain::Untracked,
                    Some(false) => HotStateReadDomain::Tracked,
                    None => HotStateReadDomain::Combined,
                },
            ))?;
        }
        if !request
            .limit
            .is_some_and(|limit| (1..=1024).contains(&limit))
            || candidate_limit == 0
            || candidate_limit > 4096
            || request.filter.global.is_some()
            || request.filter.untracked.is_some()
            || request.filter.include_tombstones
            || !matches!(request.filter.rows, HotStateRowFilter::All)
            || !request.filter.row_pks.is_empty()
            || request.filter.row_pk_lower.is_some()
            || request.filter.row_pk_upper.is_some()
            || !request.filter.file_ids.is_empty()
            || !request.filter.constraints.is_empty()
            || request.filter.declared_column_eq.is_some()
            || request.filter.declared_column_range.is_some()
            || request_may_include_derived(request)
            || self.partial_scope_policy.is_some()
            || self.partial_scope_source.is_some()
        {
            return Ok(None);
        }
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let scope = scan_scope(
            &self.store,
            request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        let [requested_branch_id] = scope.projection_branch_ids.as_slice() else {
            return Ok(None);
        };
        if scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id != requested_branch_id && branch_id != GLOBAL_BRANCH_ID)
        {
            return Ok(None);
        }
        let Some(control) = scope.branch_heads.get(requested_branch_id).copied() else {
            return Ok(None);
        };
        let minimum_candidate_count = request
            .limit
            .expect("the candidate route requires a finite limit")
            .max(super::MIN_UNORDERED_LIMIT_CANDIDATES);
        self.tracked_head
            .reader(&self.store)
            .scan_row_limit_candidates(
                requested_branch_id,
                control,
                schema_key,
                candidate_limit,
                minimum_candidate_count,
                // A complete scan of a small live collection is cheaper than
                // probing the full candidate budget and then falling back.
                // Keep this cost gate independent of the minimum useful page.
                candidate_limit,
            )
            .await
    }

    pub(crate) async fn scan_direct_row_snapshots(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
        let Some((branch_id, control, schema_key)) =
            self.direct_row_snapshot_scope(request).await?
        else {
            return Ok(None);
        };
        if !request.filter.row_pks.is_empty() {
            let rows = self
                .tracked_head
                .reader(&self.store)
                .scan_native_row_snapshots(
                    &branch_id,
                    control,
                    &schema_key,
                    &request.filter.row_pks,
                    request.limit,
                )
                .await?;
            return Ok(Some(crate::tracked_state::ExclusiveRowSnapshotBatch::Raw(
                rows,
            )));
        }
        let key = ExclusiveCertifiedBatchCacheKey {
            branch_id: branch_id.clone(),
            head_commit_id: control.head_commit_id,
            generation: control.tracked_generation,
            current_state_revision: control.current_state_revision,
            schema_key: schema_key.clone(),
        };
        if let Some(batch) = self.exclusive_certified_batch_cache.get(&key) {
            return Ok(Some(
                crate::tracked_state::ExclusiveRowSnapshotBatch::CertifiedNative(batch),
            ));
        }
        let rows = self
            .tracked_head
            .reader(&self.store)
            .scan_exclusive_row_snapshots(&branch_id, control, &schema_key)
            .await?;
        if let Some(crate::tracked_state::ExclusiveRowSnapshotBatch::CertifiedNative(batch)) = &rows
        {
            self.exclusive_certified_batch_cache.insert(key, batch);
        }
        Ok(rows)
    }

    pub(crate) async fn scan_direct_row_snapshot_pages(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<
        Option<
            stream::BoxStream<
                'static,
                Result<crate::tracked_state::ExclusiveRowSnapshotBatch, LixError>,
            >,
        >,
        LixError,
    >
    where
        S: Clone + Send + Sync + 'static,
    {
        self.scan_direct_row_snapshot_pages_with_minimum_count(request, 4096)
            .await
    }

    pub(crate) async fn scan_direct_row_snapshot_pages_with_minimum_count(
        &self,
        request: &HotStateScanRequest,
        minimum_collection_count: u64,
    ) -> Result<
        Option<
            stream::BoxStream<
                'static,
                Result<crate::tracked_state::ExclusiveRowSnapshotBatch, LixError>,
            >,
        >,
        LixError,
    >
    where
        S: Clone + Send + Sync + 'static,
    {
        if request.limit.is_some() || !request.filter.row_pks.is_empty() {
            return Ok(None);
        }
        let Some((branch_id, control, schema_key)) =
            self.direct_row_snapshot_stream_scope(request).await?
        else {
            return Ok(None);
        };
        // Streaming pays per-part validation and per-page visibility costs.
        // Keep small, heavily deleted collections on the cheaper batch path.
        let Some(collection) = self
            .tracked_head
            .reader(&self.store)
            .stored_collection_generation(
                &branch_id,
                control.tracked_generation,
                crate::collection_generation::CollectionScopeRef {
                    schema_key: &schema_key,
                    file_id: None,
                },
            )
            .await?
        else {
            return Ok(None);
        };
        if collection.live_count == crate::collection_generation::DEFERRED_LIVE_COUNT
            || collection.live_count < minimum_collection_count
        {
            return Ok(None);
        }
        let Some(local_pages) = self
            .tracked_head
            .reader(self.store.clone())
            .scan_packed_row_snapshot_pages(
                &branch_id,
                control,
                &schema_key,
                request.filter.row_pk_lower.clone(),
                request.filter.row_pk_upper.clone(),
            )
            .await?
        else {
            return Ok(None);
        };

        // Global admission uses a bounded physical identity scan. A logical
        // LIMIT on scan_batch is not a physical bound: its generic route can
        // still walk and materialize an arbitrarily large global collection.
        let mut global_rows = Vec::new();
        if branch_id != GLOBAL_BRANCH_ID {
            const GLOBAL_CANDIDATE_CAP: usize = 64;
            const GLOBAL_CANDIDATE_BYTE_CAP: usize = 512 * 1024;
            let mut global_request = request.clone();
            global_request.filter.branch_ids = vec![GLOBAL_BRANCH_ID.to_owned()];
            global_request.filter.row_pks.clear();
            global_request.limit = Some(GLOBAL_CANDIDATE_CAP + 1);
            global_request.projection.columns = vec!["change_id".to_owned()];
            if let Some(operation) = &self.read_interest_registry {
                operation.register(super::LogicalReadInterest::scan(
                    &global_request,
                    HotStateReadDomain::Combined,
                ))?;
            }
            let global_branch_ids = vec![GLOBAL_BRANCH_ID.to_owned()];
            let global_controls = load_branch_head_controls(
                &self.store,
                &global_branch_ids,
                self.branch_head_control_cache.as_deref(),
            )
            .await?;
            let Some(global_control) = global_controls.get(GLOBAL_BRANCH_ID).copied() else {
                return Ok(None);
            };
            let global_scan = TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema_key.clone()],
                    row_pk_lower: request.filter.row_pk_lower.clone(),
                    row_pk_upper: request.filter.row_pk_upper.clone(),
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: global_request.limit,
            };
            const GLOBAL_CANDIDATE_PHYSICAL_CAP: usize = 128;
            let Some(candidate_keys) = self
                .tracked_head
                .reader(&self.store)
                .try_scan_bounded_live_row_pks(
                    GLOBAL_BRANCH_ID,
                    global_control,
                    &global_scan,
                    GLOBAL_CANDIDATE_CAP,
                    GLOBAL_CANDIDATE_PHYSICAL_CAP,
                    GLOBAL_CANDIDATE_BYTE_CAP,
                )
                .await?
            else {
                return Ok(None);
            };
            if candidate_keys.len() > GLOBAL_CANDIDATE_CAP {
                return Ok(None);
            }
            if !candidate_keys.is_empty() {
                let expected_candidates = candidate_keys
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut winner_request = request.clone();
                winner_request.filter.row_pks = candidate_keys;
                winner_request.filter.include_tombstones = true;
                winner_request.projection.columns = vec!["change_id".to_owned()];
                let winner_exact = HotStateExactBatchRequest {
                    rows: winner_request
                        .filter
                        .row_pks
                        .iter()
                        .map(|row_pk| HotStateExactRowRequest {
                            schema_key: schema_key.clone(),
                            branch_id: branch_id.clone(),
                            row_pk: row_pk.clone(),
                            file_id: None,
                        })
                        .collect(),
                    projection: winner_request.projection.clone(),
                    untracked: None,
                    include_tombstones: true,
                };
                // The broad Combined scan already captures these identities.
                // Internal visibility probes must not add one recipe per key.
                let winners = self
                    .load_exact_batch_without_read_interest(&winner_exact)
                    .await?
                    .into_present_batch();
                let mut observed_candidates = std::collections::BTreeSet::new();
                let mut global_winner_keys = Vec::new();
                for row in winners.iter() {
                    if row.schema_key() != schema_key
                        || row.file_id().is_some()
                        || !expected_candidates.contains(row.row_pk())
                        || !observed_candidates.insert(row.row_pk().clone())
                        || (row.global() && row.deleted())
                    {
                        return Ok(None);
                    }
                    if row.global() && !row.deleted() {
                        global_winner_keys.push(row.row_pk().clone());
                    }
                }
                if observed_candidates != expected_candidates {
                    return Ok(None);
                }
                if global_winner_keys.is_empty() {
                    return Ok(Some(local_pages));
                }
                let expected_global_winners = global_winner_keys
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut observed_global_winners = std::collections::BTreeSet::new();
                let mut payload_bytes = 0_usize;
                for row_pk in expected_global_winners.iter() {
                    let mut payload_request = request.clone();
                    payload_request.filter.branch_ids = vec![GLOBAL_BRANCH_ID.to_owned()];
                    payload_request.filter.row_pks = vec![row_pk.clone()];
                    payload_request.projection.columns = vec!["raw_snapshot".to_owned()];
                    payload_request.limit = Some(1);
                    let payload_exact = HotStateExactBatchRequest {
                        rows: vec![HotStateExactRowRequest {
                            schema_key: schema_key.clone(),
                            branch_id: GLOBAL_BRANCH_ID.to_owned(),
                            row_pk: row_pk.clone(),
                            file_id: None,
                        }],
                        projection: payload_request.projection.clone(),
                        untracked: None,
                        include_tombstones: false,
                    };
                    let payloads = self
                        .load_exact_batch_without_read_interest(&payload_exact)
                        .await?
                        .into_present_batch();
                    let mut payload_rows = payloads.iter();
                    let Some(row) = payload_rows.next() else {
                        return Ok(None);
                    };
                    if payload_rows.next().is_some()
                        || row.schema_key() != schema_key
                        || row.file_id().is_some()
                        || row.deleted()
                        || !observed_global_winners.insert(row.row_pk().clone())
                        || row.row_pk() != row_pk
                    {
                        return Ok(None);
                    }
                    let payload = if let Some(payload) = row.raw_snapshot() {
                        payload.clone()
                    } else if let Some(snapshot) = row.decoded_snapshot() {
                        let payload = snapshot.durable_payload().map_err(|error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("global row snapshot could not be encoded: {error:?}"),
                            )
                        })?;
                        Bytes::copy_from_slice(&payload)
                    } else {
                        return Ok(None);
                    };
                    payload_bytes = payload_bytes
                        .checked_add(payload.len())
                        .ok_or_else(|| LixError::unknown("global snapshot byte count overflow"))?;
                    if payload_bytes > GLOBAL_CANDIDATE_BYTE_CAP {
                        return Ok(None);
                    }
                    global_rows.push((row.row_pk().clone(), payload));
                }
                if observed_global_winners != expected_global_winners {
                    return Ok(None);
                }
            }
        }
        let prefix = (!global_rows.is_empty()).then(|| {
            stream::once(async move {
                Ok(crate::tracked_state::ExclusiveRowSnapshotBatch::Raw(
                    global_rows,
                ))
            })
            .boxed()
        });
        let pages = if let Some(prefix) = prefix {
            prefix.chain(local_pages).boxed()
        } else {
            local_pages
        };
        Ok(Some(pages))
    }

    pub(crate) async fn plan_direct_row_columnar_scan(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<
        Option<(
            crate::columnar_row_group::RowGroupSetId,
            std::sync::Arc<crate::columnar_row_group::RowGroupManifest>,
            [u8; 32],
            std::sync::Arc<Vec<crate::hot_state::RowColumnarOverlayRow>>,
            String,
            CommitId,
            u64,
            u64,
        )>,
        LixError,
    > {
        if !request.filter.row_pks.is_empty()
            || request.limit.is_some()
            || !matches!(request.filter.rows, HotStateRowFilter::All)
            || request.filter.include_tombstones
            || request.filter.untracked.is_some()
            || !request.filter.file_ids.is_empty()
            || !request.filter.constraints.is_empty()
        {
            return Ok(None);
        }
        let Some((branch_id, control, schema_key)) =
            self.direct_row_snapshot_scope(request).await?
        else {
            return Ok(None);
        };
        let key = RowColumnarLayoutCacheKey {
            branch_id: branch_id.clone(),
            generation: control.tracked_generation,
            current_state_revision: control.current_state_revision,
            schema_key: schema_key.clone(),
        };
        if let Some(layout) = self.row_columnar_layout_cache.get(&key) {
            return Ok(Some((
                layout.id,
                std::sync::Arc::clone(&layout.manifest),
                layout.manifest_digest,
                std::sync::Arc::clone(&layout.overlay),
                branch_id,
                layout.head_commit_id,
                control.current_state_revision,
                layout.live_count,
            )));
        }
        let layout = self
            .tracked_head
            .reader(&self.store)
            .row_columnar_layout(&branch_id, control, &schema_key)
            .await?;
        let Some((id, manifest, overlay, live_count)) = layout else {
            return Ok(None);
        };
        let manifest_digest = manifest.content_digest()?;
        let layout = self.row_columnar_layout_cache.insert(
            key,
            id,
            manifest,
            manifest_digest,
            overlay,
            control.head_commit_id,
            live_count,
        );
        Ok(Some((
            layout.id,
            std::sync::Arc::clone(&layout.manifest),
            layout.manifest_digest,
            std::sync::Arc::clone(&layout.overlay),
            branch_id,
            layout.head_commit_id,
            control.current_state_revision,
            layout.live_count,
        )))
    }

    async fn direct_row_snapshot_scope(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<(String, BranchHeadControl, String)>, LixError> {
        self.direct_row_snapshot_scope_with_global(request, false)
            .await
    }

    async fn direct_row_snapshot_stream_scope(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<(String, BranchHeadControl, String)>, LixError> {
        self.direct_row_snapshot_scope_with_global(request, true)
            .await
    }

    async fn direct_row_snapshot_scope_with_global(
        &self,
        request: &HotStateScanRequest,
        allow_global_schema_rows: bool,
    ) -> Result<Option<(String, BranchHeadControl, String)>, LixError> {
        if let Some(operation) = &self.read_interest_registry {
            operation.register(super::LogicalReadInterest::scan(
                request,
                match request.filter.untracked {
                    Some(true) => HotStateReadDomain::Untracked,
                    Some(false) => HotStateReadDomain::Tracked,
                    None => HotStateReadDomain::Combined,
                },
            ))?;
        }
        if request.filter.global.is_some() {
            return Ok(None);
        }
        // The hot index carries tracked and untracked rows in one serving
        // plane, so this route never probes a separate retention index.
        if request.filter.untracked.is_some() || request_may_include_derived(request) {
            return Ok(None);
        }
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&request.filter.branch_ids)?;
        }
        let scope = scan_scope(
            &self.store,
            request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        let [requested_branch_id] = scope.projection_branch_ids.as_slice() else {
            return Ok(None);
        };
        if scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id != requested_branch_id && branch_id != GLOBAL_BRANCH_ID)
        {
            return Ok(None);
        }
        let Some(requested_control) = scope.branch_heads.get(requested_branch_id).copied() else {
            return Ok(None);
        };
        if !allow_global_schema_rows
            && requested_branch_id != GLOBAL_BRANCH_ID
            && let Some(global_control) = scope.branch_heads.get(GLOBAL_BRANCH_ID).copied()
            && self
                .tracked_head
                .reader(&self.store)
                .has_schema_rows(GLOBAL_BRANCH_ID, global_control, schema_key)
                .await?
        {
            return Ok(None);
        }
        Ok(Some((
            requested_branch_id.clone(),
            requested_control,
            schema_key.clone(),
        )))
    }

    pub(crate) async fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.scan_batch_with_schema_presence(request, false).await
    }

    /// Resolves a declared-column equality to correlated hot-row identities
    /// and hydrates those identities in one exact batch. The ordinary scan
    /// route remains the fallback whenever the index lacks a completeness
    /// witness or the request carries dimensions this path cannot preserve.
    async fn scan_indexed_declared_column_batch(
        &self,
        request: &HotStateScanRequest,
        domain: HotStateReadDomain,
    ) -> Result<Option<MaterializedHotStateBatch>, LixError> {
        let Some(predicate) = request.filter.declared_column_eq.as_ref() else {
            return Ok(None);
        };
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let requested_branch_id = match request.filter.branch_ids.as_slice() {
            [requested_branch_id] => requested_branch_id,
            [requested_branch_id, global] if global == GLOBAL_BRANCH_ID => requested_branch_id,
            _ => return Ok(None),
        };
        if predicate.schema_key.as_str() != schema_key
            || !request.filter.row_pks.is_empty()
            || request.filter.row_pk_lower.is_some()
            || request.filter.row_pk_upper.is_some()
            || !request.filter.file_ids.is_empty()
            || !matches!(request.filter.rows, HotStateRowFilter::All)
            || request.filter.declared_column_range.is_some()
            || !request.filter.constraints.is_empty()
            || request.limit.is_some()
            || request_may_include_derived(request)
        {
            return Ok(None);
        }
        if let Some(operation) = &self.read_interest_registry {
            operation.register(super::LogicalReadInterest::scan(request, domain))?;
        }
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&request.filter.branch_ids)?;
        }
        let scope = scan_scope(
            &self.store,
            request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        ensure_partial_projection_complete(
            request,
            &scope,
            self.partial_scope_policy.is_some() || self.partial_scope_source.is_some(),
        )?;

        let mut identities = std::collections::BTreeSet::new();
        for branch_id in &scope.storage_branch_ids {
            let Some(control) = scope.branch_heads.get(branch_id).copied() else {
                return Ok(None);
            };
            if !control.may_have_schema(schema_key) {
                continue;
            }
            let Some(candidates) = self
                .tracked_head
                .reader(&self.store)
                .scan_hot_index_identity_candidates(
                    branch_id,
                    control.tracked_generation,
                    schema_key,
                    predicate.ordinal,
                    &predicate.values,
                )
                .await?
            else {
                return Ok(None);
            };
            let exact_branch_id =
                if branch_id == GLOBAL_BRANCH_ID && requested_branch_id != GLOBAL_BRANCH_ID {
                    requested_branch_id.to_owned()
                } else {
                    branch_id.clone()
                };
            identities.extend(candidates.into_iter().map(|(row_pk, file_id)| {
                HotStateExactRowRequest {
                    schema_key: schema_key.to_owned(),
                    branch_id: exact_branch_id.clone(),
                    row_pk,
                    file_id,
                }
            }));
        }
        if identities.is_empty() {
            return Ok(Some(MaterializedHotStateBatch::default()));
        }
        let exact = HotStateExactBatchRequest {
            rows: identities.into_iter().collect(),
            projection: request.projection.clone(),
            untracked: match domain {
                HotStateReadDomain::Tracked => Some(false),
                HotStateReadDomain::Untracked => Some(true),
                HotStateReadDomain::Combined => None,
            },
            include_tombstones: request.filter.include_tombstones,
        };
        Ok(Some(
            self.load_exact_batch(&exact).await?.into_present_batch(),
        ))
    }

    async fn scan_batch_with_schema_presence(
        &self,
        request: &HotStateScanRequest,
        skip_proven_empty_schema: bool,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        if let Some(operation) = &self.read_interest_registry {
            operation.register(super::LogicalReadInterest::scan(
                request,
                match request.filter.untracked {
                    Some(true) => HotStateReadDomain::Untracked,
                    Some(false) => HotStateReadDomain::Tracked,
                    None => HotStateReadDomain::Combined,
                },
            ))?;
        }
        let store = &self.store;
        let reads_tracked = !is_derived_only_request(request);
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&request.filter.branch_ids)?;
        }
        let scope = scan_scope(
            store,
            request,
            reads_tracked,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        ensure_partial_projection_complete(
            request,
            &scope,
            self.partial_scope_policy.is_some() || self.partial_scope_source.is_some(),
        )?;
        if skip_proven_empty_schema && !scope_may_have_schema_rows(request, &scope) {
            return Ok(MaterializedHotStateBatch::default());
        }
        // Resolve a declared-column equality through the index plane before any
        // route is chosen, so every route below sees an ordinary row-pk
        // request. Candidates are not answers: the caller keeps its own
        // predicate and rejects stale ones.
        let resolved;
        let request = match self.resolve_declared_column_eq(request, &scope).await? {
            Some(rewritten) => {
                resolved = rewritten;
                &resolved
            }
            None => request,
        };
        let filter_global_scope = request.filter.global;
        if filter_global_scope.is_none()
            && let Some(rows) = self.scan_direct_row_pk_batch(request, &scope).await?
        {
            return Ok(rows);
        }
        if filter_global_scope.is_none()
            && let Some(rows) = self.try_scan_limited_single_branch(request, &scope).await?
        {
            return Ok(rows);
        }
        let derived_rows = MaterializedHotStateBatch::from_rows(
            scan_derived_rows(
                store,
                &self.commit_graph,
                request,
                &scope.projection_branch_ids,
                &scope.storage_branch_ids,
                request.filter.untracked,
            )
            .await?,
        );
        let mut hot_branch_rows = if !is_derived_only_request(request) {
            self.scan_hot_branch_rows(request, &scope).await?
        } else {
            Vec::new()
        };
        // The ordered single-branch route bypasses the generic visibility
        // resolver, so apply the retention predicate before taking that fast
        // path. Otherwise `untracked = Some(..)` accidentally returned both
        // member kinds from an already-unified group.
        if request.filter.untracked.is_some() {
            for branch_rows in &mut hot_branch_rows {
                branch_rows.rows = filter_current_row_retention(
                    std::mem::take(&mut branch_rows.rows),
                    request.filter.untracked,
                );
            }
        }
        if filter_global_scope.is_none()
            && derived_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&hot_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_batch(
                std::mem::take(&mut hot_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        if filter_global_scope.is_none()
            && derived_rows.is_empty()
            && let Some(rows) = try_merge_dominant_branch_with_global(
                &mut hot_branch_rows,
                &scope.projection_branch_ids,
                request,
            )
        {
            return Ok(rows);
        }
        if filter_global_scope.is_none() && derived_rows.is_empty() {
            let visibility_request = VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            };
            let ordered_runs = hot_branch_rows
                .iter()
                .map(|branch_rows| OrderedVisibilityRun {
                    branch_id: &branch_rows.branch_id,
                    rows: &branch_rows.rows,
                    ordered_unique: branch_rows.ordered_unique,
                })
                .collect::<Vec<_>>();
            if let Some(rows) = resolve_visible_ordered_runs(&ordered_runs, &visibility_request) {
                return Ok(rows);
            }
        }
        let rows = concat_hot_state_batches(
            std::iter::once(derived_rows).chain(
                hot_branch_rows
                    .into_iter()
                    .map(|branch_rows| branch_rows.rows),
            ),
        );
        let rows = resolve_visible_batch(
            rows,
            MaterializedHotStateBatch::default(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: if filter_global_scope.is_some() {
                    None
                } else {
                    request.limit
                },
            },
        );
        if let Some(global) = filter_global_scope {
            return Ok(rows.filter(|row| row.global() == global, request.limit));
        }
        Ok(rows)
    }

    /// Rewrites a declared-column equality into a row-pk request.
    ///
    /// Returns `None` when the predicate cannot be served — no predicate, more
    /// than one branch in scope, or no completeness witness for the collection
    /// — in which case the caller's ordinary scan runs unchanged and still
    /// produces correct rows, only slower.
    ///
    /// An empty candidate set becomes `HotStateRowFilter::None`, never an
    /// empty `row_pks` list: an empty list means "no identity filter" and
    /// would silently widen the scan to the whole collection.
    async fn resolve_declared_column_eq(
        &self,
        request: &HotStateScanRequest,
        scope: &HotStateScanScope,
    ) -> Result<Option<HotStateScanRequest>, LixError> {
        if request.filter.declared_column_eq.is_none()
            && request.filter.declared_column_range.is_none()
        {
            return Ok(None);
        }
        let mut rewritten = request.clone();
        rewritten.filter.declared_column_eq = None;
        rewritten.filter.declared_column_range = None;
        // An identity filter already names its rows, so no index can narrow it.
        if !request.filter.row_pks.is_empty() {
            return Ok(Some(rewritten));
        }
        // Every branch in scope must be witnessed. A branch whose index is
        // incomplete would contribute no candidates and silently drop its
        // rows, which is the false negative this design cannot have — so one
        // unwitnessed branch sends the whole read back to the scan.
        let mut unique = std::collections::BTreeSet::new();
        // An equality probe is a point prefix per value and is strictly
        // cheaper than walking an interval, so when a scan carries both the
        // equality wins and the range stays a residual predicate.
        if let Some(predicate) = request.filter.declared_column_eq.as_ref() {
            for branch_id in &scope.storage_branch_ids {
                let Some(control) = scope.branch_heads.get(branch_id).copied() else {
                    return Ok(Some(rewritten));
                };
                // A branch the control proves holds no row of this schema
                // contributes no candidates, so it needs no witness. The bloom
                // summarizes both retention lanes for the serving generation.
                if !control.may_have_schema(&predicate.schema_key) {
                    continue;
                }
                let Some(candidates) = self
                    .tracked_head
                    .reader(&self.store)
                    .scan_hot_index_candidates(
                        branch_id,
                        control.tracked_generation,
                        &predicate.schema_key,
                        predicate.ordinal,
                        &predicate.values,
                    )
                    .await?
                else {
                    return Ok(Some(rewritten));
                };
                unique.extend(candidates);
            }
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_hot_index_equality_probe_engaged();
        } else if let Some(predicate) = request.filter.declared_column_range.as_ref() {
            for branch_id in &scope.storage_branch_ids {
                let Some(control) = scope.branch_heads.get(branch_id).copied() else {
                    return Ok(Some(rewritten));
                };
                if !control.may_have_schema(&predicate.schema_key) {
                    continue;
                }
                let Some(candidates) = self
                    .tracked_head
                    .reader(&self.store)
                    .scan_hot_index_range_candidates(
                        branch_id,
                        control.tracked_generation,
                        &predicate.schema_key,
                        predicate.ordinal,
                        predicate
                            .lower
                            .as_ref()
                            .map(|(value, inclusive)| (value, *inclusive)),
                        predicate
                            .upper
                            .as_ref()
                            .map(|(value, inclusive)| (value, *inclusive)),
                    )
                    .await?
                else {
                    return Ok(Some(rewritten));
                };
                unique.extend(candidates);
            }
        }
        if unique.is_empty() {
            rewritten.filter.rows = HotStateRowFilter::None;
        } else {
            rewritten.filter.row_pks = unique.into_iter().collect();
        }
        Ok(Some(rewritten))
    }

    /// Serves finite row-PK scans from the hot current-state index. Every
    /// row already has its retention tag, so an unrelated untracked row
    /// cannot route selected tracked identities through a separate scan.
    #[cfg(test)]
    async fn scan_direct_row_pk_rows(
        &self,
        request: &HotStateScanRequest,
        scope: &HotStateScanScope,
    ) -> Result<Option<Vec<MaterializedHotStateRow>>, LixError> {
        Ok(self
            .scan_direct_row_pk_batch(request, scope)
            .await?
            .map(MaterializedHotStateBatch::into_rows))
    }

    async fn scan_direct_row_pk_batch(
        &self,
        request: &HotStateScanRequest,
        scope: &HotStateScanScope,
    ) -> Result<Option<MaterializedHotStateBatch>, LixError> {
        if !matches!(request.filter.rows, HotStateRowFilter::All)
            || request.filter.branch_ids.is_empty()
            || request.filter.schema_keys.is_empty()
            || request.filter.row_pks.is_empty()
            || !request.filter.file_ids.is_empty()
            || !request.filter.constraints.is_empty()
            || request_may_include_derived(request)
        {
            return Ok(None);
        }
        let controls = scope
            .storage_branch_ids
            .iter()
            .map(|branch_id| {
                scope
                    .branch_heads
                    .get(branch_id)
                    .copied()
                    .map(|control| (branch_id.clone(), control))
            })
            .collect::<Option<Vec<_>>>();
        let Some(mut controls) = controls else {
            return Ok(None);
        };
        // Branch-head schema membership is an atomic, no-false-negative
        // publication filter. Apply it per generation before a finite PK
        // lookup so an absent global schema does not pay the complete hot,
        // packed, and certified point-read stack for every active-branch row.
        // The bloom summary belongs to the published tracked selector. An
        // explicit current-only read must inspect the untracked selector even
        // when the tracked summary has no bit for this schema; otherwise a
        // durable runtime/ownership check is silently skipped.
        if request.filter.untracked.is_none() {
            controls.retain(|(_, control)| {
                request
                    .filter
                    .schema_keys
                    .iter()
                    .any(|schema_key| control.may_have_schema(schema_key))
            });
        }
        if controls.is_empty() {
            return Ok(Some(MaterializedHotStateBatch::default()));
        }
        let tracked_request = tracked_scan_request_from_live(request);
        let tracked_head = self
            .branch_head_control_cache
            .as_ref()
            .map_or_else(
                || self.tracked_head.reader(&self.store),
                |cache| {
                    self.tracked_head
                        .transaction_reader(&self.store, std::sync::Arc::clone(&cache.hot_state))
                },
            )
            .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache));
        let rows_by_branch = tracked_head
            .scan_live_batches_for_controls_with_fallback(
                &controls,
                &tracked_request,
                request.filter.untracked,
                self.partial_scope_policy.is_some() || self.partial_scope_source.is_some(),
            )
            .await?;
        let rows = concat_hot_state_batches(
            rows_by_branch
                .into_iter()
                .map(|(_, rows)| filter_current_row_retention(rows, request.filter.untracked)),
        );
        Ok(Some(resolve_visible_batch(
            rows,
            MaterializedHotStateBatch::default(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        )))
    }

    pub(crate) async fn load_row(
        &self,
        request: &HotStateRowRequest,
    ) -> Result<Option<MaterializedHotStateRow>, LixError> {
        let rows = self
            .scan_batch(&HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    row_pks: vec![request.row_pk.clone()],
                    branch_ids: vec![request.branch_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    include_tombstones: false,
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await?;
        Ok(rows.get(0).map(MaterializedHotStateRowRef::to_owned))
    }

    /// Complete only native inputs for current rows selected by this read's
    /// captured access recipes. Never replay SQL or execute mutation validation.
    pub(crate) fn prepare_captured_read_interests<'a>(
        &'a self,
        captured: &'a super::ReadInterestSnapshot,
        active_account_id: &'a str,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<Vec<(String, crate::tracked_state::TrackedStateKey)>, LixError>,
    > {
        self.prepare_captured_read_interests_with_native_diff_budget(
            captured,
            active_account_id,
            None,
            &[],
        )
    }

    pub(crate) fn prepare_captured_read_interests_with_native_diff_budget<'a>(
        &'a self,
        captured: &'a super::ReadInterestSnapshot,
        active_account_id: &'a str,
        native_diff_budget: Option<crate::tracked_state::NativeDiffIdentityBudget>,
        working_diff_candidates: &'a [PreparedWorkingDiffMutationCandidates],
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<Vec<(String, crate::tracked_state::TrackedStateKey)>, LixError>,
    > {
        Box::pin(async move {
            if self.partial_scope_policy.is_none() && self.partial_scope_source.is_none() {
                return Ok(Vec::new());
            }
            let mut execution_rows = std::collections::BTreeSet::new();
            let mut account_prepared = false;
            for interest in &captured.interests {
                let rows: Vec<CurrentReadIdentity> = match interest.as_ref() {
                    super::LogicalReadInterest::Scan { request, domain } => {
                        let batch = match domain {
                            super::InterestDomain::Tracked => {
                                self.scan_tracked_batch(request).await?
                            }
                            super::InterestDomain::Combined => self.scan_batch(request).await?,
                            super::InterestDomain::Untracked => continue,
                        };
                        batch
                            .iter()
                            .filter(|row| !row.untracked())
                            .filter_map(CurrentReadIdentity::from_row)
                            .collect()
                    }
                    super::LogicalReadInterest::Exact {
                        rows,
                        projection,
                        untracked,
                        include_tombstones,
                    } => {
                        let request = HotStateExactBatchRequest {
                            rows: rows
                                .iter()
                                .map(|row| HotStateExactRowRequest {
                                    schema_key: row.schema_key.clone(),
                                    branch_id: row.branch_id.clone(),
                                    file_id: row.file_id.clone(),
                                    row_pk: row.row_pk.clone(),
                                })
                                .collect(),
                            projection: projection.clone(),
                            untracked: *untracked,
                            include_tombstones: *include_tombstones,
                        };
                        let batch = self.load_exact_batch(&request).await?;
                        (0..request.rows.len())
                            .filter_map(|i| batch.row(i))
                            .filter(|row| !row.untracked())
                            .filter_map(CurrentReadIdentity::from_row)
                            .collect()
                    }
                    // Path-index availability is not a returned-row scope.
                    // File/directory providers retain exact selected identities
                    // after predicates and limits, including cached index reads.
                    super::LogicalReadInterest::FilesystemPaths { .. } => continue,
                    super::LogicalReadInterest::Diff {
                        branch_id: Some(branch),
                        from: super::DiffInterestEndpoint::WorkingCheckpoint,
                        to: super::DiffInterestEndpoint::ActiveHead,
                        filter,
                        relation,
                        retain_payloads,
                        projected_columns,
                        limit,
                    } => {
                        // Only a working diff requests checkpoint publication
                        // inputs. Ordinary current reads and fixed historical
                        // diffs must not acquire this extra historical scope.
                        let controls = load_branch_head_controls(
                            &self.store,
                            std::slice::from_ref(branch),
                            self.branch_head_control_cache.as_deref(),
                        )
                        .await?;
                        let control = controls.get(branch).ok_or_else(|| {
                            LixError::new(
                                "LIX_SYNC_BRANCH_CONTROLS_REQUIRED",
                                "working diff preparation lacks its branch control",
                            )
                        })?;
                        if let Some(checkpoint) = control.working_diff_checkpoint_commit_id
                            && checkpoint != control.head_commit_id
                        {
                            let captured = (!retain_payloads && limit.is_none()).then(|| {
                                working_diff_candidates.iter().find(|candidate| {
                                    candidate.branch_id == *branch
                                        && candidate.checkpoint_commit_id
                                            == checkpoint.to_string()
                                        && candidate.head_commit_id
                                            == control.head_commit_id.to_string()
                        && candidate.relation == *relation
                        && candidate.filter == *filter
                        && candidate.retain_payloads == *retain_payloads
                        && candidate.projected_columns == *projected_columns
                                })
                            }).flatten();
                            if let Some(candidate) = captured {
                                crate::tracked_state::prepare_row_pk_mutation_inputs_at_commit_from_diff(
                                    &self.store,
                                    checkpoint,
                                    &candidate.diff,
                                )
                                .await?;
                            } else {
                                let mut tracked = TrackedStateContext::new().reader(&self.store);
                                if let Some(budget) = native_diff_budget.clone() {
                                    tracked = tracked.with_native_diff_identity_budget(budget);
                                }
                                let diff = tracked
                                    .diff_commits(
                                        &checkpoint.to_string(),
                                        &control.head_commit_id.to_string(),
                                        &crate::tracked_state::TrackedStateDiffRequest {
                                            filter: filter.clone(),
                                            retain_payloads: false,
                                        },
                                    )
                                    .await?;
                                let keys = diff
                                    .entries
                                    .iter()
                                    .map(|entry| crate::tracked_state::TrackedStateKey {
                                        schema_key: entry.identity.schema_key().to_owned(),
                                        file_id: entry.identity.file_id().map(str::to_owned),
                                        row_pk: entry.identity.row_pk().clone(),
                                    })
                                    .collect::<Vec<_>>();
                                crate::tracked_state::prepare_row_pk_mutation_inputs_at_commit(
                                    &self.store,
                                    checkpoint,
                                    &keys,
                                )
                                .await?;
                            }
                        }
                        continue;
                    }
                    // These recipes retain metadata, historical endpoints or
                    // content projections. Any current row reads they perform
                    // independently register Exact/Scan above.
                    super::LogicalReadInterest::CollectionGeneration { .. }
                    | super::LogicalReadInterest::PackedIdentityMembership { .. }
                    | super::LogicalReadInterest::History { .. }
                    | super::LogicalReadInterest::Diff { .. }
                    | super::LogicalReadInterest::FileContent { .. }
                    | super::LogicalReadInterest::FilesystemMetadata { .. } => continue,
                };
                if !rows.is_empty() && !account_prepared {
                    // Every local write validates this session's active actor.
                    // Prepare its exact native value, without changing account
                    // status or treating a read as an authorization proof.
                    self.load_exact_batch(&HotStateExactBatchRequest {
                        rows: vec![HotStateExactRowRequest {
                            schema_key: "lix_account".to_owned(),
                            branch_id: GLOBAL_BRANCH_ID.to_owned(),
                            row_pk: RowPk::uuid_from_canonical(active_account_id).map_err(
                                |_| {
                                    LixError::new(
                                        "LIX_INVALID_ACCOUNT_ID",
                                        "active account ID is not a canonical UUID",
                                    )
                                },
                            )?,
                            file_id: None,
                        }],
                        projection: HotStateProjection {
                            columns: vec!["snapshot_content".to_owned()],
                        },
                        untracked: None,
                        include_tombstones: false,
                    })
                    .await?;
                    account_prepared = true;
                }
                execution_rows.extend(
                    rows.iter()
                        .map(|row| (row.branch_id.clone(), row.key.clone())),
                );
                self.prepare_returned_rows(rows).await?;
            }
            Ok(execution_rows.into_iter().collect())
        })
    }

    fn prepare_returned_rows(
        &self,
        mut rows: Vec<CurrentReadIdentity>,
    ) -> ReturnedChangePreparationFuture<'_> {
        Box::pin(async move {
            if rows.is_empty() {
                return Ok(());
            }
            let epoch = self
                .effective_partial_scope_policy()
                .await?
                .and_then(|policy| policy.preparation_epoch());
            let branches = rows
                .iter()
                .map(|row| row.branch_id.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let controls = load_branch_head_controls(
                &self.store,
                &branches,
                self.branch_head_control_cache.as_deref(),
            )
            .await?;
            let identity = |row: &CurrentReadIdentity| -> Result<PreparedReadKey, LixError> {
                let control = controls.get(&row.branch_id).ok_or_else(|| {
                    LixError::new(
                        "LIX_SYNC_BRANCH_CONTROLS_REQUIRED",
                        "returned row mutation preparation lacks its branch control",
                    )
                })?;
                Ok((row.branch_id.clone(), control.head_commit_id, row.change_id))
            };
            // Validate all control coordinates before consulting an optimization.
            for row in &rows {
                identity(row)?;
            }
            if let Some(epoch) = epoch {
                let mut cache = self
                    .prepared_read_rows
                    .lock()
                    .map_err(|_| LixError::unknown("read preparation cache poisoned"))?;
                if cache.epoch.as_deref() != Some(epoch) {
                    cache.keys.clear();
                    cache.epoch = Some(epoch.to_owned());
                }
                rows.retain(|row| {
                    !cache
                        .keys
                        .contains(&identity(row).expect("validated controls"))
                });
            }
            if rows.is_empty() {
                return Ok(());
            }
            let ids = rows
                .iter()
                .map(|row| row.change_id)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            // Read preparation only warms data for a possible later mutation.
            // Local standalone payloads suffice here; if one is absent, the
            // physical-first resolver fetches just that missing change.
            let standalone = ChangelogContext::new()
                .reader(&self.store)
                .load_changes(ChangeLoadRequest { change_ids: &ids })
                .await?;
            let missing = standalone
                .into_iter()
                .filter_map(|(id, record)| record.is_none().then_some(*id))
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                crate::tracked_state::load_change_records_by_ids(&self.store, &missing).await?;
            }
            let mut keys = std::collections::BTreeMap::<
                String,
                Vec<crate::tracked_state::TrackedStateKey>,
            >::new();
            for row in &rows {
                keys.entry(row.branch_id.clone())
                    .or_default()
                    .push(row.key.clone());
            }
            for (branch, mut keys) in keys {
                keys.sort();
                keys.dedup();
                crate::tracked_state::prepare_current_row_mutation_inputs(
                    &self.store,
                    controls[&branch].head_commit_id,
                    &keys,
                )
                .await?;
            }
            if let Some(epoch) = epoch {
                let mut cache = self
                    .prepared_read_rows
                    .lock()
                    .map_err(|_| LixError::unknown("read preparation cache poisoned"))?;
                if cache.epoch.as_deref() == Some(epoch) {
                    if cache.keys.len().saturating_add(rows.len()) > 4096 {
                        cache.keys.clear();
                    }
                    for row in rows.into_iter().take(4096) {
                        cache.keys.insert(identity(&row)?);
                    }
                }
            }
            Ok(())
        })
    }

    pub(crate) async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.load_exact_batch_inner(request, true).await
    }

    async fn load_exact_batch_without_read_interest(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.load_exact_batch_inner(request, false).await
    }

    async fn load_exact_batch_inner(
        &self,
        request: &HotStateExactBatchRequest,
        register_read_interest: bool,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        if register_read_interest && let Some(operation) = &self.read_interest_registry {
            operation.register(super::LogicalReadInterest::exact(request))?;
        }
        if request.rows.is_empty() {
            return Ok(MaterializedHotStateExactBatch::default());
        }
        // Derived rows are synthesized rather than stored under the
        // requested identity. Preserve their exact scan semantics without
        // widening the optimized durable-state batch.
        if request
            .rows
            .iter()
            .any(|row| is_derived_schema(&row.schema_key))
        {
            let mut builder = MaterializedHotStateBatchBuilder::with_capacity(request.rows.len());
            let mut slots = Vec::with_capacity(request.rows.len());
            for row in &request.rows {
                let rows = self.scan_batch(&request.row_scan_request(row)).await?;
                let found = rows.get(0);
                slots.push(if let Some(found) = found {
                    Some(u32::try_from(builder.push_ref(found, None)).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact derived live-state result exceeds u32 rows",
                        )
                    })?)
                } else {
                    None
                });
            }
            return MaterializedHotStateExactBatch::new(builder.finish(), slots);
        }

        let scope = self.exact_batch_scope(request).await?;
        self.load_exact_batch_in_scope(request, &scope).await
    }

    async fn exact_batch_scope(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<HotStateScanScope, LixError> {
        let branch_ids = request
            .rows
            .iter()
            .map(|row| row.branch_id.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let scope_request = HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                branch_ids,
                untracked: request.untracked,
                ..Default::default()
            },
            ..Default::default()
        };
        // The hot current-state rows are authoritative for both retention modes.
        // Even an untracked-only exact batch therefore needs the branch
        // controls that select the active generation; treating that request
        // as "not tracked" used to skip the controls entirely and made the
        // global untracked rows invisible after hot-index initialization.
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&scope_request.filter.branch_ids)?;
        }
        let scope = scan_scope(
            &self.store,
            &scope_request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        ensure_partial_projection_complete(
            &scope_request,
            &scope,
            self.partial_scope_policy.is_some() || self.partial_scope_source.is_some(),
        )?;
        Ok(scope)
    }

    async fn load_exact_batch_in_scope(
        &self,
        request: &HotStateExactBatchRequest,
        scope: &HotStateScanScope,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        let visible_branch_ids = scope
            .projection_branch_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        let mut storage_identities = Vec::with_capacity(request.rows.len());
        for row in &request.rows {
            if !visible_branch_ids.contains(&row.branch_id) {
                continue;
            }
            storage_identities.push(crate::hot_state::HotStateRowIdentityRef {
                branch_id: row.branch_id.as_str(),
                schema_key: row.schema_key.as_str(),
                row_pk: &row.row_pk,
                file_id: row.file_id.as_deref(),
            });
        }
        storage_identities.sort_unstable();
        storage_identities.dedup();

        let mut branch_ranges = Vec::new();
        let mut offset = 0;
        while offset < storage_identities.len() {
            let branch_id = storage_identities[offset].branch_id;
            let mut end = offset + 1;
            while end < storage_identities.len() && storage_identities[end].branch_id == branch_id {
                end += 1;
            }
            if scope.branch_heads.contains_key(branch_id) {
                branch_ranges.push(offset..end);
            }
            offset = end;
        }
        let projection =
            crate::changelog::ChangeRecordProjection::from_columns(&request.projection.columns);
        let mut current_batches = stream::iter(branch_ranges)
            .map(|range| {
                let identities = &storage_identities[range.clone()];
                let branch_id = identities[0].branch_id;
                let control = scope.branch_heads[branch_id];
                let projection = projection.clone();
                async move {
                    let keys = identities
                        .iter()
                        .map(|identity| crate::tracked_state::TrackedStateKeyRef {
                            schema_key: identity.schema_key,
                            row_pk: identity.row_pk,
                            file_id: identity.file_id,
                        })
                        .collect::<Vec<_>>();
                    let domain =
                        request
                            .untracked
                            .map_or(HotStateReadDomain::Combined, |untracked| {
                                if untracked {
                                    HotStateReadDomain::Untracked
                                } else {
                                    HotStateReadDomain::Tracked
                                }
                            });
                    let rows = self
                        .tracked_head
                        .reader(&self.store)
                        .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache))
                        .load_projected_live_batch_refs_for_domain_with_fallback(
                            branch_id,
                            control,
                            &keys,
                            &projection,
                            domain,
                            (self.partial_scope_policy.is_some()
                                || self.partial_scope_source.is_some())
                            .then_some(control.head_commit_id),
                        )
                        .await?;
                    Ok::<_, LixError>((range, rows))
                }
            })
            .buffered(BRANCH_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        let mut candidate_slots = Vec::with_capacity(storage_identities.len());
        for (batch_index, (range, rows)) in current_batches.iter().enumerate() {
            for (slot, identity_index) in range.clone().enumerate() {
                if rows.row(slot).is_some() {
                    candidate_slots.push((storage_identities[identity_index], batch_index, slot));
                }
            }
        }
        // Resolve the branch-local plane first. A local value or tombstone is
        // authoritative and suppresses the global row; only a genuinely
        // absent (or retention-mismatched) local identity needs a global
        // point. This preserves aligned overlay semantics while avoiding the
        // eager 2*K multiget paid by every ordinary exact batch.
        let mut global_identities = Vec::new();
        for requested in &request.rows {
            if requested.branch_id == GLOBAL_BRANCH_ID
                || !visible_branch_ids.contains(&requested.branch_id)
            {
                continue;
            }
            let local_identity = crate::hot_state::HotStateRowIdentityRef {
                branch_id: requested.branch_id.as_str(),
                schema_key: requested.schema_key.as_str(),
                row_pk: &requested.row_pk,
                file_id: requested.file_id.as_deref(),
            };
            let local = candidate_slots
                .binary_search_by_key(&local_identity, |candidate| candidate.0)
                .ok()
                .and_then(|index| {
                    let (_, batch_index, slot) = candidate_slots[index];
                    current_batches[batch_index].1.row(slot)
                })
                .filter(|row| current_row_matches_retention(*row, request.untracked));
            if local.is_none() {
                global_identities.push(crate::hot_state::HotStateRowIdentityRef {
                    branch_id: GLOBAL_BRANCH_ID,
                    ..local_identity
                });
            }
        }
        global_identities.sort_unstable();
        global_identities.dedup();
        if !global_identities.is_empty()
            && let Some(global_control) = scope.branch_heads.get(GLOBAL_BRANCH_ID).copied()
        {
            let keys = global_identities
                .iter()
                .map(|identity| crate::tracked_state::TrackedStateKeyRef {
                    schema_key: identity.schema_key,
                    row_pk: identity.row_pk,
                    file_id: identity.file_id,
                })
                .collect::<Vec<_>>();
            let domain = request
                .untracked
                .map_or(HotStateReadDomain::Combined, |untracked| {
                    if untracked {
                        HotStateReadDomain::Untracked
                    } else {
                        HotStateReadDomain::Tracked
                    }
                });
            let rows = self
                .tracked_head
                .reader(&self.store)
                .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache))
                .load_projected_live_batch_refs_for_domain_with_fallback(
                    GLOBAL_BRANCH_ID,
                    global_control,
                    &keys,
                    &projection,
                    domain,
                    (self.partial_scope_policy.is_some() || self.partial_scope_source.is_some())
                        .then_some(global_control.head_commit_id),
                )
                .await?;
            let batch_index = current_batches.len();
            for (slot, identity) in global_identities.iter().copied().enumerate() {
                if rows.row(slot).is_some() {
                    candidate_slots.push((identity, batch_index, slot));
                }
            }
            current_batches.push((0..global_identities.len(), rows));
        }
        candidate_slots.sort_unstable_by_key(|candidate| candidate.0);
        debug_assert!(candidate_slots.windows(2).all(|pair| pair[0].0 < pair[1].0));

        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(request.rows.len());
        let mut slots = Vec::with_capacity(request.rows.len());
        for requested in &request.rows {
            if !visible_branch_ids.contains(&requested.branch_id) {
                slots.push(None);
                continue;
            }
            let branch_identity = crate::hot_state::HotStateRowIdentityRef {
                branch_id: requested.branch_id.as_str(),
                schema_key: requested.schema_key.as_str(),
                row_pk: &requested.row_pk,
                file_id: requested.file_id.as_deref(),
            };
            let global_identity = crate::hot_state::HotStateRowIdentityRef {
                branch_id: GLOBAL_BRANCH_ID,
                ..branch_identity
            };
            let lookup = |identity| {
                candidate_slots
                    .binary_search_by_key(&identity, |candidate| candidate.0)
                    .ok()
                    .and_then(|index| {
                        let (_, batch_index, slot) = candidate_slots[index];
                        current_batches[batch_index].1.row(slot)
                    })
            };
            // Filter each source before branch/global precedence. A local
            // row of the other retention must not mask a matching global
            // row from an explicit retention-scoped internal read.
            let row = lookup(branch_identity)
                .filter(|row| current_row_matches_retention(*row, request.untracked))
                .or_else(|| {
                    lookup(global_identity)
                        .filter(|row| current_row_matches_retention(*row, request.untracked))
                });
            let Some(row) = row else {
                slots.push(None);
                continue;
            };
            if row.deleted() && !request.include_tombstones {
                slots.push(None);
                continue;
            }
            let branch_override = (row.branch_id() == GLOBAL_BRANCH_ID
                && requested.branch_id != GLOBAL_BRANCH_ID)
                .then_some(requested.branch_id.as_str());
            slots.push(Some(
                u32::try_from(builder.push_ref(row, branch_override)).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "exact live-state result exceeds u32 rows",
                    )
                })?,
            ));
        }
        MaterializedHotStateExactBatch::new(builder.finish(), slots)
    }

    async fn load_exact_parent_closure(
        &self,
        request: &HotStateExactBatchRequest,
        parent: super::reader::ParentRowPk,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        if request.rows.is_empty() {
            return Ok(MaterializedHotStateBatch::default());
        }
        if request
            .rows
            .iter()
            .any(|row| is_derived_schema(&row.schema_key))
        {
            return super::reader::load_parent_closure_with(request, parent, |batch| async move {
                self.load_exact_batch(&batch).await
            })
            .await;
        }
        let scope = self.exact_batch_scope(request).await?;
        let scope = &scope;
        super::reader::load_parent_closure_with(request, parent, |batch| async move {
            if let Some(registry) = &self.read_interest_registry {
                registry.register(super::LogicalReadInterest::exact(&batch))?;
            }
            self.load_exact_batch_in_scope(&batch, scope).await
        })
        .await
    }

    pub(crate) async fn scan_tracked_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.scan_tracked_batch_with_schema_presence(request, false)
            .await
    }

    async fn scan_tracked_batch_with_schema_presence(
        &self,
        request: &HotStateScanRequest,
        skip_proven_empty_schema: bool,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        if let Some(registry) = &self.read_interest_registry {
            registry.register(super::LogicalReadInterest::scan(
                request,
                HotStateReadDomain::Tracked,
            ))?;
        }
        let store = &self.store;
        let reads_tracked = !is_derived_only_request(request);
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&request.filter.branch_ids)?;
        }
        let scope = scan_scope(
            store,
            request,
            reads_tracked,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        ensure_partial_projection_complete(
            request,
            &scope,
            self.partial_scope_policy.is_some() || self.partial_scope_source.is_some(),
        )?;
        if skip_proven_empty_schema && !scope_may_have_schema_rows(request, &scope) {
            return Ok(MaterializedHotStateBatch::default());
        }
        // The tracked domain is the constraint validator's route, and
        // `validate_committed_unique_constraints` reaches it with an equality
        // on a declared column. Resolving it here is the same access-path
        // choice the combined route already makes, and it is what stops an
        // insert into an `x-lix-unique` collection from scanning that
        // collection.
        let resolved;
        let request = match self.resolve_declared_column_eq(request, &scope).await? {
            Some(rewritten) => {
                resolved = rewritten;
                &resolved
            }
            None => request,
        };
        let derived_rows = MaterializedHotStateBatch::from_rows(
            scan_derived_rows(
                store,
                &self.commit_graph,
                request,
                &scope.projection_branch_ids,
                &scope.storage_branch_ids,
                Some(false),
            )
            .await?,
        );
        let mut hot_branch_rows = if !is_derived_only_request(request) {
            self.scan_hot_branch_rows(request, &scope).await?
        } else {
            Vec::new()
        };
        for branch_rows in &mut hot_branch_rows {
            branch_rows.rows =
                filter_current_row_retention(std::mem::take(&mut branch_rows.rows), Some(false));
        }
        if derived_rows.is_empty()
            && let Some(index) =
                ordered_unique_branch_row_index(&hot_branch_rows, &scope.projection_branch_ids)
        {
            return Ok(finalize_ordered_unique_batch(
                std::mem::take(&mut hot_branch_rows[index].rows),
                request.filter.include_tombstones,
                request.limit,
            ));
        }
        if request.filter.global.is_none()
            && derived_rows.is_empty()
            && let Some(rows) = try_merge_dominant_branch_with_global(
                &mut hot_branch_rows,
                &scope.projection_branch_ids,
                request,
            )
        {
            return Ok(rows);
        }
        if request.filter.global.is_none() && derived_rows.is_empty() {
            let visibility_request = VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            };
            let ordered_runs = hot_branch_rows
                .iter()
                .map(|branch_rows| OrderedVisibilityRun {
                    branch_id: &branch_rows.branch_id,
                    rows: &branch_rows.rows,
                    ordered_unique: branch_rows.ordered_unique,
                })
                .collect::<Vec<_>>();
            if let Some(rows) = resolve_visible_ordered_runs(&ordered_runs, &visibility_request) {
                return Ok(rows);
            }
        }
        let rows = concat_hot_state_batches(
            std::iter::once(derived_rows).chain(
                hot_branch_rows
                    .into_iter()
                    .map(|branch_rows| branch_rows.rows),
            ),
        );
        Ok(resolve_visible_batch(
            rows,
            MaterializedHotStateBatch::default(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: scope.projection_branch_ids,
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        ))
    }

    async fn try_scan_limited_single_branch(
        &self,
        request: &HotStateScanRequest,
        scope: &HotStateScanScope,
    ) -> Result<Option<MaterializedHotStateBatch>, LixError> {
        if request.limit.is_none()
            || !request.filter.row_pks.is_empty()
            || request_may_include_derived(request)
            || self.partial_scope_policy.is_some()
            || self.partial_scope_source.is_some()
            || !matches!(request.filter.rows, HotStateRowFilter::All)
        {
            return Ok(None);
        }
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let [branch_id] = scope.projection_branch_ids.as_slice() else {
            return Ok(None);
        };
        // A limit cannot precede cross-branch shadow resolution. A negative
        // schema bloom proves that no other branch can contribute or shadow.
        if scope.storage_branch_ids.iter().any(|other| {
            other != branch_id
                && scope
                    .branch_heads
                    .get(other)
                    .is_none_or(|control| control.may_have_schema(schema_key))
        }) {
            return Ok(None);
        }
        let Some(control) = scope.branch_heads.get(branch_id) else {
            return Ok(None);
        };
        let mut tracked = tracked_scan_request_from_live(request);
        tracked.limit = request.limit;
        tracked.filter.include_tombstones = request.filter.include_tombstones;
        self.tracked_head
            .reader(&self.store)
            .try_scan_limited_live_batch(branch_id, *control, &tracked, request.filter.untracked)
            .await
    }

    async fn scan_hot_branch_rows(
        &self,
        request: &HotStateScanRequest,
        scope: &HotStateScanScope,
    ) -> Result<Vec<HotBranchRows>, LixError> {
        // `HotStateRowFilter::None` means "no identity can match", which is
        // exactly what an index probe with no candidates produces. The tracked
        // request below carries only `row_pks`, where an empty list means
        // "no identity filter" — the opposite — so the empty case has to be
        // answered before the request is lowered.
        if matches!(request.filter.rows, HotStateRowFilter::None) {
            return Ok(Vec::new());
        }
        let store = &self.store;
        let tracked_request = tracked_scan_request_from_live(request);
        let branches = scope
            .storage_branch_ids
            .iter()
            .filter_map(|branch_id| {
                scope
                    .branch_heads
                    .get(branch_id)
                    .map(|control| (branch_id.clone(), *control))
            })
            .collect::<Vec<_>>();
        let branch_rows = stream::iter(branches)
            .map(|(branch_id, control)| {
                let tracked_request = tracked_request.clone();
                async move {
                    let rows = self
                        .tracked_head
                        .reader(store)
                        .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache))
                        .scan_live_batch_for_retention_with_fallback(
                            &branch_id,
                            control,
                            &tracked_request,
                            request.filter.untracked,
                            (self.partial_scope_policy.is_some()
                                || self.partial_scope_source.is_some())
                            .then_some(control.head_commit_id),
                        )
                        .await?;
                    // Tracked roots are physically ordered by file before row
                    // identity, while visibility merges require row identity
                    // before file. Prove the materialized order here with a
                    // borrowed O(N) scan and no row-sized scratch allocation.
                    let ordered_unique = materialized_batch_is_strictly_ordered_unique(&rows);
                    Ok::<_, LixError>(HotBranchRows {
                        branch_id: branch_id.clone(),
                        rows,
                        ordered_unique,
                    })
                }
            })
            .buffered(BRANCH_READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(branch_rows)
    }
}

#[async_trait]
impl<S> HotStateReader for HotStateContextReader<S>
where
    S: StorageAdapterRead,
{
    fn is_partial_replica(&self) -> bool {
        self.partial_scope_policy.is_some() || self.partial_scope_source.is_some()
    }
    fn scan_batch_resolves_visibility(&self) -> bool {
        true
    }
    fn read_interest_registry(&self) -> Option<std::sync::Arc<super::ReadInterestRegistry>> {
        self.read_interest_registry.clone()
    }
    async fn scan_constraint_batch(
        &self,
        request: &HotStateScanRequest,
        tracked_only: bool,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        if tracked_only {
            self.scan_tracked_batch_with_schema_presence(request, true)
                .await
        } else {
            // A combined constraint read is also the explicit cross-domain
            // identity/collision probe. Its tracked bloom cannot prove that
            // the current-only selector is empty, so never short-circuit it.
            self.scan_batch_with_schema_presence(request, request.filter.untracked.is_some())
                .await
        }
    }

    async fn scan_indexed_declared_column_batch(
        &self,
        request: &HotStateScanRequest,
        domain: HotStateReadDomain,
    ) -> Result<Option<MaterializedHotStateBatch>, LixError> {
        Self::scan_indexed_declared_column_batch(self, request, domain).await
    }

    async fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        Self::scan_batch(self, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        Self::load_exact_batch(self, request).await
    }

    async fn load_exact_parent_closure(
        &self,
        request: &HotStateExactBatchRequest,
        parent: super::reader::ParentRowPk,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        Self::load_exact_parent_closure(self, request, parent).await
    }

    async fn collection_generation(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        if let Some(registry) = &self.read_interest_registry {
            registry.register(super::LogicalReadInterest::CollectionGeneration {
                branch_id: branch_id.to_owned(),
                schema_key: scope.schema_key.to_owned(),
                file_id: scope.file_id.map(str::to_owned),
            })?;
        }
        let controls = load_branch_head_controls(
            &self.store,
            &[branch_id.to_owned()],
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        let Some(control) = controls.get(branch_id).copied() else {
            return Ok(None);
        };
        self.tracked_head
            .reader(&self.store)
            .collection_generation(branch_id, control.tracked_generation, scope)
            .await
            .map(Some)
    }

    async fn scan_tracked_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        Self::scan_tracked_batch(self, request).await
    }
}

#[async_trait]
impl<S> FilesystemPathIndexReader for HotStateContextReader<S>
where
    S: StorageAdapterRead + Send + Sync,
{
    fn prefer_direct_exact_content(&self, branch_ids: &[String], file_id: &str) -> bool {
        self.filesystem_path_index_cache
            .prefer_direct_exact_content(branch_ids, file_id)
    }

    fn record_direct_exact_content(&self, branch_ids: &[String], file_id: &str) {
        self.filesystem_path_index_cache
            .record_direct_exact_content(branch_ids, file_id);
    }

    fn prefer_direct_exact_path(&self, branch_ids: &[String], file_id: &str) -> bool {
        self.filesystem_path_index_cache
            .prefer_direct_exact_path(branch_ids, file_id)
    }

    fn record_direct_exact_path(&self, branch_ids: &[String], file_id: &str) {
        self.filesystem_path_index_cache
            .record_direct_exact_path(branch_ids, file_id);
    }

    fn historical_cache(
        &self,
    ) -> Option<std::sync::Arc<crate::filesystem::HistoricalPathIndexCache>> {
        Some(self.filesystem_path_index_cache.historical.clone())
    }

    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<std::sync::Arc<FilesystemPathIndex>, LixError> {
        if let Some(policy) = self.effective_partial_scope_policy().await? {
            policy.validate(&request.branch_ids)?;
        }
        if let Some(registry) = &self.read_interest_registry {
            registry.register(super::LogicalReadInterest::FilesystemPaths {
                scope: request.scope.clone(),
                branch_ids: request.branch_ids.clone(),
                include_blob_refs: request.include_blob_refs,
                cache_small_blob_data: request.cache_small_blob_data,
            })?;
        }
        let revision = load_path_index_revision(&self.store).await?;
        if let Some(index) = self
            .filesystem_path_index_cache
            .get(request, revision.as_deref())
        {
            return Ok(index);
        }
        // FilesystemPaths already retains the complete index dependency. Its
        // implementation scan is not a request to prepare every file for edits.
        let index_reader = HotStateContextReader {
            read_interest_registry: None,
            partial_scope_policy: self.partial_scope_policy.clone(),
            partial_scope_source: self.partial_scope_source.clone(),
            resolved_partial_scope: self.resolved_partial_scope.clone(),
            store: &self.store,
            tracked_head: self.tracked_head.clone(),
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: self.filesystem_path_index_cache.clone(),
            row_columnar_layout_cache: self.row_columnar_layout_cache.clone(),
            exclusive_certified_batch_cache: self.exclusive_certified_batch_cache.clone(),
            branch_head_control_cache: self.branch_head_control_cache.clone(),
            root_base_cache: self.root_base_cache.clone(),
            prepared_read_rows: self.prepared_read_rows.clone(),
        };
        let mut index = build_path_index(&index_reader, request).await?;
        if request.cache_small_blob_data {
            index = std::sync::Arc::new(
                (*index)
                    .clone()
                    .hydrate_small_blob_data(&self.store)
                    .await?,
            );
        }
        Ok(self
            .filesystem_path_index_cache
            .insert(request, revision.as_deref(), index))
    }
}

fn tracked_scan_request_from_live(request: &HotStateScanRequest) -> TrackedStateScanRequest {
    TrackedStateScanRequest {
        filter: TrackedStateFilter {
            schema_keys: request.filter.schema_keys.clone(),
            row_pks: request.filter.row_pks.clone(),
            row_pk_lower: request.filter.row_pk_lower.clone(),
            row_pk_upper: request.filter.row_pk_upper.clone(),
            file_ids: request.filter.file_ids.clone(),
            // Scan tombstones internally so branch-local tombstones can hide
            // global fallback rows before the serving facade filters them.
            include_tombstones: true,
        },
        read_columns: TrackedStateReadColumns {
            columns: request.projection.columns.clone(),
        },
        limit: None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotStateScanScope {
    storage_branch_ids: Vec<String>,
    projection_branch_ids: Vec<String>,
    branch_heads: BranchHeads,
}

#[test]
fn borrowed_scan_scope_is_send_for_storage_session_open() {
    fn assert_send<T: Send>() {}
    fn assert_borrowed_scope<'a>() {
        assert_send::<&'a HotStateScanScope>();
    }
    assert_borrowed_scope();
}

/// Rows read from one durable hot-state branch source.
///
/// Ordered fast paths require strict visible-identity ordering (which also
/// proves uniqueness) in the actual materialized rows.
struct HotBranchRows {
    branch_id: String,
    rows: MaterializedHotStateBatch,
    ordered_unique: bool,
}

/// Returns the only nonempty branch candidate when it can be served without
/// branch/global visibility resolution. Global rows, multiple requested
/// branches, and synthesized branch-ref candidates all stay on the general
/// visibility path.
fn ordered_unique_branch_row_index(
    branch_rows: &[HotBranchRows],
    projection_branch_ids: &[String],
) -> Option<usize> {
    let [requested_branch_id] = projection_branch_ids else {
        return None;
    };
    let mut candidates = branch_rows
        .iter()
        .enumerate()
        .filter(|(_, branch_rows)| !branch_rows.rows.is_empty());
    let (index, candidate) = candidates.next()?;
    if candidates.next().is_some()
        || !candidate.ordered_unique
        || candidate.branch_id != *requested_branch_id
    {
        return None;
    }
    Some(index)
}

/// Merges a bounded global run into a much larger ordered branch run without
/// rebuilding the branch's row columns. These predicates intentionally match
/// only the shape whose visibility proof is explicit: one requested local
/// branch, no derived rows or global-only predicate, and two authenticated
/// ordered unique source runs. Every uncertain shape stays on the general
/// resolver.
fn try_merge_dominant_branch_with_global(
    branch_rows: &mut [HotBranchRows],
    projection_branch_ids: &[String],
    request: &HotStateScanRequest,
) -> Option<MaterializedHotStateBatch> {
    if request.limit.is_some()
        || !matches!(request.filter.rows, HotStateRowFilter::All)
        || request.filter.global.is_some()
    {
        return None;
    }
    let [branch_id] = projection_branch_ids else {
        return None;
    };
    if branch_id == GLOBAL_BRANCH_ID {
        return None;
    }

    let mut local = None;
    let mut global = None;
    for (index, run) in branch_rows
        .iter()
        .enumerate()
        .filter(|(_, run)| !run.rows.is_empty())
    {
        if !run.ordered_unique {
            return None;
        }
        if run.branch_id == *branch_id {
            if local.replace(index).is_some()
                || run
                    .rows
                    .iter()
                    .any(|row| row.global() || row.branch_id() != run.branch_id)
            {
                return None;
            }
        } else if run.branch_id == GLOBAL_BRANCH_ID {
            if global.replace(index).is_some()
                || run
                    .rows
                    .iter()
                    .any(|row| !row.global() || row.branch_id() != run.branch_id)
            {
                return None;
            }
        } else {
            return None;
        }
    }
    let (local_index, global_index) = (local?, global?);
    let local_rows = &branch_rows[local_index].rows;
    let global_rows = &branch_rows[global_index].rows;
    if local_rows.len() < DOMINANT_BRANCH_MIN_ROWS
        || global_rows.len() > SMALL_GLOBAL_OVERLAY_MAX_ROWS
        || local_rows.len().saturating_add(global_rows.len()) > u32::MAX as usize
    {
        return None;
    }

    // Branch-tier candidates suppress global candidates by the complete
    // visible identity, including file scope. Tombstones participate in this
    // shadow check and are filtered only after the merge.
    let selected_globals = global_rows
        .iter()
        .enumerate()
        .filter_map(|(global_index, global_row)| {
            (!batch_contains_visible_identity(local_rows, global_row)).then_some(global_index)
        })
        .collect::<Vec<_>>();

    if selected_globals.is_empty() {
        let local_rows = std::mem::take(&mut branch_rows[local_index].rows);
        return Some(finalize_dominant_local_batch(
            local_rows,
            request.filter.include_tombstones,
        ));
    }

    let mut additions = MaterializedHotStateBatchBuilder::with_capacity(selected_globals.len());
    for global_index in selected_globals {
        additions.push_ref(global_rows.row(global_index), Some(branch_id));
    }
    let additions = additions.finish();
    let mut local_rows = std::mem::take(&mut branch_rows[local_index].rows);
    let local_len = local_rows.len();
    let merged_len = local_len + additions.len();
    debug_assert!(merged_len <= u32::MAX as usize);
    let mut permutation = Vec::with_capacity(merged_len);
    let (mut local_index, mut global_index) = (0, 0);
    while local_index < local_len && global_index < additions.len() {
        if compare_visible_identity(local_rows.row(local_index), additions.row(global_index))
            != std::cmp::Ordering::Greater
        {
            permutation.push(local_index as u32);
            local_index += 1;
        } else {
            permutation.push((local_len + global_index) as u32);
            global_index += 1;
        }
    }
    while local_index < local_len {
        permutation.push(local_index as u32);
        local_index += 1;
    }
    while global_index < additions.len() {
        permutation.push((local_len + global_index) as u32);
        global_index += 1;
    }
    local_rows.append_batch(additions);
    local_rows.permute_rows(&permutation);
    Some(finalize_dominant_local_batch(
        local_rows,
        request.filter.include_tombstones,
    ))
}

fn finalize_dominant_local_batch(
    mut rows: MaterializedHotStateBatch,
    include_tombstones: bool,
) -> MaterializedHotStateBatch {
    if !include_tombstones {
        rows.retain_rows_in_place(|row| !row.deleted());
    }
    rows
}

fn compare_visible_identity(
    left: MaterializedHotStateRowRef<'_>,
    right: MaterializedHotStateRowRef<'_>,
) -> std::cmp::Ordering {
    left.schema_key()
        .cmp(right.schema_key())
        .then_with(|| left.row_pk().cmp(right.row_pk()))
        .then_with(|| left.file_id().cmp(&right.file_id()))
}

fn materialized_batch_is_strictly_ordered_unique(rows: &MaterializedHotStateBatch) -> bool {
    rows.iter()
        .zip(rows.iter().skip(1))
        .all(|(left, right)| compare_visible_identity(left, right).is_lt())
}

fn batch_contains_visible_identity(
    batch: &MaterializedHotStateBatch,
    candidate: MaterializedHotStateRowRef<'_>,
) -> bool {
    let (mut low, mut high) = (0, batch.len());
    while low < high {
        let middle = low + (high - low) / 2;
        match compare_visible_identity(batch.row(middle), candidate) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

/// Finalizes a table scan whose rows are already ordered and unique for the
/// sole requested branch. This intentionally does no identity sort or
/// deduplication; the tracked-head key codec proves both properties.
fn finalize_ordered_unique_batch(
    rows: MaterializedHotStateBatch,
    include_tombstones: bool,
    limit: Option<usize>,
) -> MaterializedHotStateBatch {
    if limit.is_none_or(|limit| limit >= rows.len())
        && (include_tombstones || !rows.iter().any(|row| row.deleted()))
    {
        return rows;
    }
    rows.filter(|row| include_tombstones || !row.deleted(), limit)
}

fn current_row_matches_retention(
    row: MaterializedHotStateRowRef<'_>,
    requested_untracked: Option<bool>,
) -> bool {
    requested_untracked.is_none_or(|untracked| row.untracked() == untracked)
}

fn filter_current_row_retention(
    rows: MaterializedHotStateBatch,
    requested_untracked: Option<bool>,
) -> MaterializedHotStateBatch {
    if requested_untracked.is_none()
        || rows
            .iter()
            .all(|row| current_row_matches_retention(row, requested_untracked))
    {
        return rows;
    }
    rows.filter(
        |row| current_row_matches_retention(row, requested_untracked),
        None,
    )
}

fn concat_hot_state_batches(
    batches: impl IntoIterator<Item = MaterializedHotStateBatch>,
) -> MaterializedHotStateBatch {
    let mut incoming = batches.into_iter().filter(|batch| !batch.is_empty());
    let Some(first) = incoming.next() else {
        return MaterializedHotStateBatch::default();
    };
    let Some(second) = incoming.next() else {
        return first;
    };
    let mut batches = vec![first, second];
    batches.extend(incoming);
    let capacity = batches.iter().map(MaterializedHotStateBatch::len).sum();
    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(capacity);
    for batch in &batches {
        for row in batch.iter() {
            builder.push_ref(row, None);
        }
    }
    builder.finish()
}

/// Proves a single-schema hot-state scan empty from the atomic branch
/// publication metadata. Missing controls and Bloom false positives fall back
/// to the storage scan; only a negative result from every selected generation
/// can skip it.
///
/// The bloom summarizes the branch's one serving generation, so it covers
/// untracked rows as well: every untracked publication notes its schema keys
/// on the same control. An explicit retention filter therefore needs no
/// escape hatch here.
fn scope_may_have_schema_rows(request: &HotStateScanRequest, scope: &HotStateScanScope) -> bool {
    let [schema_key] = request.filter.schema_keys.as_slice() else {
        return true;
    };
    if is_derived_schema(schema_key) {
        return true;
    }
    scope.storage_branch_ids.iter().any(|branch_id| {
        scope
            .branch_heads
            .get(branch_id)
            .is_none_or(|control| control.may_have_schema(schema_key))
    })
}

/// A partial replica must never turn an unavailable admitted branch into a
/// successful empty result. Full replicas retain the historical branch-list
/// behavior, while partial readers fail closed so the SQL layer can keep the
/// operation pending/error instead of publishing a false negative.
fn ensure_partial_projection_complete(
    request: &HotStateScanRequest,
    scope: &HotStateScanScope,
    partial: bool,
) -> Result<(), LixError> {
    if partial
        && request
            .filter
            .branch_ids
            .iter()
            .any(|branch| !scope.projection_branch_ids.contains(branch))
    {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_SCOPE_UNSUPPORTED",
            "partial query branch admission is not available in the serving snapshot",
        ));
    }
    Ok(())
}

async fn scan_scope(
    store: &(impl StorageAdapterRead + ?Sized),
    request: &HotStateScanRequest,
    resolve_branch_heads: bool,
    branch_head_control_cache: Option<&BranchHeadControlCache>,
) -> Result<HotStateScanScope, LixError> {
    if request.filter.branch_ids.is_empty() {
        if resolve_branch_heads {
            let branch_heads = load_branch_head_controls(store, &[], None).await?;
            return Ok(HotStateScanScope {
                storage_branch_ids: branch_heads.keys().cloned().collect(),
                projection_branch_ids: Vec::new(),
                branch_heads,
            });
        }
        return Ok(HotStateScanScope {
            storage_branch_ids: all_branch_head_control_ids(store).await?,
            projection_branch_ids: Vec::new(),
            branch_heads: BranchHeads::new(),
        });
    }

    if resolve_branch_heads {
        let candidate_branch_ids = expanded_branch_ids(&request.filter.branch_ids);
        let branch_heads =
            load_branch_head_controls(store, &candidate_branch_ids, branch_head_control_cache)
                .await?;
        let projection_branch_ids = request
            .filter
            .branch_ids
            .iter()
            .filter(|branch_id| branch_heads.contains_key(*branch_id))
            .cloned()
            .collect::<Vec<_>>();
        let storage_branch_ids = expanded_branch_ids(&projection_branch_ids);
        return Ok(HotStateScanScope {
            storage_branch_ids,
            projection_branch_ids,
            branch_heads,
        });
    }

    let existing_branch_ids = load_branch_head_control_ids(store, &request.filter.branch_ids)
        .await?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let projection_branch_ids = request
        .filter
        .branch_ids
        .iter()
        .filter(|branch_id| existing_branch_ids.contains(*branch_id))
        .cloned()
        .collect::<Vec<_>>();

    let storage_branch_ids = expanded_branch_ids(&projection_branch_ids);
    Ok(HotStateScanScope {
        storage_branch_ids,
        projection_branch_ids,
        branch_heads: BranchHeads::new(),
    })
}

/// Loads branch-head controls without touching the mutable live-state index. A
/// nonempty request is point-read; the empty request is the explicit
/// all-branches scan used only by broad scans.
async fn load_branch_head_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_ids: &[String],
    cache: Option<&BranchHeadControlCache>,
) -> Result<BranchHeads, LixError> {
    let reader = BranchHeadControlContext::new().reader(store);
    if branch_ids.is_empty() {
        return Ok(reader.scan().await?.into_iter().collect());
    }
    let Some(cache) = cache else {
        let controls = reader.load_many(branch_ids).await?;
        return Ok(branch_ids
            .iter()
            .cloned()
            .zip(controls)
            .filter_map(|(branch_id, control)| control.map(|control| (branch_id, control)))
            .collect());
    };
    let missing = {
        let controls = cache.controls.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction branch-head control cache lock is poisoned",
            )
        })?;
        branch_ids
            .iter()
            .filter(|branch_id| !controls.contains_key(*branch_id))
            .cloned()
            .collect::<Vec<_>>()
    };
    let mut loaded_by_branch = std::collections::BTreeMap::new();
    if !missing.is_empty() {
        let loaded = reader.load_many(&missing).await?;
        let mut controls = cache.controls.lock().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "transaction branch-head control cache lock is poisoned",
            )
        })?;
        for (branch_id, control) in missing.into_iter().zip(loaded) {
            if controls.len() < TRANSACTION_BRANCH_HEAD_CONTROL_CACHE_MAX_ENTRIES {
                controls.entry(branch_id.clone()).or_insert(control);
            }
            loaded_by_branch.insert(branch_id, control);
        }
    }
    let controls = cache.controls.lock().map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "transaction branch-head control cache lock is poisoned",
        )
    })?;
    Ok(branch_ids
        .iter()
        .filter_map(|branch_id| {
            controls
                .get(branch_id)
                .copied()
                .or_else(|| loaded_by_branch.get(branch_id).copied())
                .flatten()
                .map(|control| (branch_id.clone(), control))
        })
        .collect())
}

async fn all_branch_head_control_ids(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<String>, LixError> {
    Ok(BranchHeadControlContext::new()
        .reader(store)
        .scan()
        .await?
        .into_iter()
        .map(|(branch_id, _)| branch_id)
        .collect())
}

async fn load_branch_head_control_ids(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_ids: &[String],
) -> Result<Vec<String>, LixError> {
    if branch_ids.is_empty() {
        return all_branch_head_control_ids(store).await;
    }
    let controls = BranchHeadControlContext::new()
        .reader(store)
        .load_many(branch_ids)
        .await?;
    Ok(branch_ids
        .iter()
        .cloned()
        .zip(controls)
        .filter_map(|(branch_id, control)| control.map(|_| branch_id))
        .collect())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn candidate_fork_shares_only_immutable_roots_not_mutable_negative_proofs() {
        let lix = crate::open_lix().await.unwrap();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .unwrap()
            .unwrap();
        let original = HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
            .with_read_interest_registry(crate::hot_state::ReadInterestRegistry::new_durable(
                16, 4096,
            ))
            .with_partial_scope_policy(GLOBAL_BRANCH_ID, GLOBAL_BRANCH_ID);
        original
            .global_key_value_rows
            .insert(control.clone(), "candidate-negative", None);
        let candidate = original.fork_for_native_candidate();
        assert!(Arc::ptr_eq(
            &candidate.root_base_cache,
            &original.root_base_cache
        ));
        assert_eq!(
            original
                .global_key_value_rows
                .get(control.clone(), "candidate-negative"),
            Some(None)
        );
        assert_eq!(
            candidate
                .global_key_value_rows
                .get(control.clone(), "candidate-negative"),
            None
        );
        candidate.global_key_value_rows.insert(
            control.clone(),
            "candidate-negative",
            Some(serde_json::json!("target")),
        );
        assert_eq!(
            original
                .global_key_value_rows
                .get(control, "candidate-negative"),
            Some(None)
        );
        assert!(candidate.read_interest_registry.is_none());
        assert!(
            !candidate.is_partial_replica(),
            "target scope must be explicitly rebound"
        );
        assert!(!Arc::ptr_eq(
            &candidate.filesystem_path_index_cache,
            &original.filesystem_path_index_cache
        ));
        assert!(!Arc::ptr_eq(
            &candidate.row_columnar_layout_cache,
            &original.row_columnar_layout_cache
        ));
        assert!(!Arc::ptr_eq(
            &candidate.exclusive_certified_batch_cache,
            &original.exclusive_certified_batch_cache
        ));
        drop(read);
        lix.close().await.unwrap();
    }

    use std::sync::Arc;

    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader, CommitId,
        CommitLoadRequest,
    };
    use crate::hot_state::{
        CurrentStateDeltaRef, HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter,
        HotStateProjection, TrackedHeadDeltaRef, WorkingDiffIndexCoverage,
    };
    use crate::row_payload::TypedRow as WasmTypedRow;
    use crate::row_pk::RowPk;
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
    use crate::tracked_state::{
        CommitStateManifest, CommitStateReplayDebt, MaterializedTrackedStateRow,
        TrackedStateCommitDeltaRef, TrackedStateDeltaRef, TrackedStateScanRequest,
        stage_commit_deltas_for_commit_state, stage_commit_state_manifest,
    };
    use serde_json::json;

    fn columnar_cache_key(revision: u64) -> RowColumnarLayoutCacheKey {
        RowColumnarLayoutCacheKey {
            branch_id: "branch".to_owned(),
            generation: CommitId::for_test_label("columnar-cache-generation"),
            current_state_revision: revision,
            schema_key: "cache_schema".to_owned(),
        }
    }

    fn empty_columnar_manifest() -> crate::columnar_row_group::RowGroupManifest {
        crate::columnar_row_group::RowGroupManifest {
            namespace: "cache_schema".to_owned(),
            metadata: std::collections::HashMap::new(),
            fields: Vec::new(),
            groups: Vec::new(),
            encoded_digest: [0; 32],
        }
    }

    #[test]
    fn row_columnar_layout_cache_reuses_arc_overlay() {
        let cache = RowColumnarLayoutCache::default();
        let key = columnar_cache_key(7);
        let inserted = cache.insert(
            key,
            crate::columnar_row_group::RowGroupSetId::new([1; 16]),
            empty_columnar_manifest(),
            [2; 32],
            Vec::new(),
            CommitId::for_test_label("columnar-cache-head"),
            1_000_000,
        );
        let hit = cache
            .get(&columnar_cache_key(7))
            .expect("same revision should hit");
        assert!(Arc::ptr_eq(&inserted, &hit));
        assert!(Arc::ptr_eq(&inserted.overlay, &hit.overlay));
    }

    #[test]
    fn row_columnar_layout_cache_revision_change_invalidates_prior_layout() {
        let cache = RowColumnarLayoutCache::default();
        let first = columnar_cache_key(7);
        cache.insert(
            first,
            crate::columnar_row_group::RowGroupSetId::new([1; 16]),
            empty_columnar_manifest(),
            [2; 32],
            Vec::new(),
            CommitId::for_test_label("columnar-cache-head-7"),
            1_000_000,
        );
        assert!(cache.get(&columnar_cache_key(8)).is_none());
        cache.insert(
            columnar_cache_key(8),
            crate::columnar_row_group::RowGroupSetId::new([2; 16]),
            empty_columnar_manifest(),
            [3; 32],
            Vec::new(),
            CommitId::for_test_label("columnar-cache-head-8"),
            999_999,
        );
        assert!(cache.get(&columnar_cache_key(7)).is_none());
        assert!(cache.get(&columnar_cache_key(8)).is_some());
    }

    #[test]
    fn row_columnar_layout_cache_does_not_admit_oversize_layout() {
        let cache = RowColumnarLayoutCache::default();
        let key = columnar_cache_key(7);
        let returned = cache.insert_with_max_bytes(
            key,
            crate::columnar_row_group::RowGroupSetId::new([1; 16]),
            empty_columnar_manifest(),
            [2; 32],
            Vec::new(),
            CommitId::for_test_label("columnar-cache-head"),
            1_000_000,
            0,
        );
        assert!(returned.bytes > 0);
        assert!(cache.get(&columnar_cache_key(7)).is_none());
    }
    const COMMIT_SCHEMA_KEY: &str = "lix_commit";

    #[derive(Clone)]
    struct MaterializedUntrackedStateRow {
        row_pk: RowPk,
        schema_key: String,
        file_id: Option<String>,
        snapshot_content: Option<String>,
        metadata: Option<String>,
        deleted: bool,
        created_at: String,
        updated_at: String,
        branch_id: String,
    }

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn change_id(label: &str) -> ChangeId {
        ChangeId::for_test_label(label)
    }

    fn hot_state_context() -> HotStateContext {
        HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
    }

    async fn stage_direct_row_head(
        storage: &StorageAdapter,
        branch_id: &str,
        head: CommitId,
        schema_key: &str,
        row_pk: &RowPk,
        snapshot: &str,
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct-head write read");
        let mut writes = StorageWriteSet::new();
        crate::init::stage_repository_protocol(&mut writes);
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                head,
                &[TrackedHeadDeltaRef {
                    schema_key,
                    file_id: None,
                    row_pk,
                    change_id: ChangeId::for_test_label(&format!("{branch_id}-change")),
                    commit_id: head,
                    deleted: false,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    updated_at: ts("2026-01-01T00:00:00Z"),
                    snapshot: Some(snapshot),
                    metadata: None,
                }],
                &std::collections::BTreeSet::new(),
                None,
            )
            .await
            .expect("stage direct row head");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct row head");
    }

    #[derive(Clone, Copy)]
    struct DirectTrackedHeadRow<'a> {
        schema_key: &'a str,
        row_pk: &'a RowPk,
        file_id: Option<&'a str>,
        snapshot: Option<&'a str>,
        deleted: bool,
    }

    async fn stage_direct_tracked_head_rows(
        storage: &StorageAdapter,
        branch_id: &str,
        head: CommitId,
        rows: &[DirectTrackedHeadRow<'_>],
    ) {
        stage_direct_tracked_head_rows_in_generation(storage, branch_id, None, head, rows).await;
    }

    async fn stage_direct_tracked_head_rows_in_generation(
        storage: &StorageAdapter,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        head: CommitId,
        rows: &[DirectTrackedHeadRow<'_>],
    ) -> CommitId {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open hot-state write read");
        let mut writes = StorageWriteSet::new();
        crate::init::stage_repository_protocol(&mut writes);
        let deltas = rows
            .iter()
            .enumerate()
            .map(|(index, row)| TrackedHeadDeltaRef {
                schema_key: row.schema_key,
                file_id: row.file_id,
                row_pk: row.row_pk,
                change_id: ChangeId::for_test_label(&format!("{branch_id}-change-{index}")),
                commit_id: head,
                deleted: row.deleted,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                snapshot: row.snapshot,
                metadata: None,
            })
            .collect::<Vec<_>>();
        let generation = TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                parent_generation,
                head,
                &deltas,
                &std::collections::BTreeSet::new(),
                None,
            )
            .await
            .expect("stage direct hot state");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit direct hot state");
        generation
    }

    async fn stage_root_tracked_head_rows(
        storage: &StorageAdapter,
        branch_id: &str,
        head: CommitId,
        rows: &[MaterializedTrackedStateRow],
    ) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open tracked-root write read");
        let mut writes = StorageWriteSet::new();
        crate::init::stage_repository_protocol(&mut writes);
        crate::test_support::stage_tracked_root_from_materialized(
            &mut read,
            &mut writes,
            &TrackedStateContext::new(),
            &head.to_string(),
            None,
            rows,
        )
        .await
        .expect("stage authenticated tracked root");
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_root_current_base(branch_id, head, head);
        crate::branch::stage_branch_head_control(
            &mut writes,
            branch_id,
            BranchHeadControl {
                head_commit_id: head,
                tracked_generation: head,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: None,
                created_at: ts("2026-01-01T00:00:00Z"),
                updated_at: ts("2026-01-01T00:00:00Z"),
                ref_change_id: ChangeId::for_test_label(&format!("{branch_id}-root-ref")),
                author_id: BranchHeadControl::author_id_bytes(crate::ANONYMOUS_ACCOUNT_ID)
                    .expect("anonymous account ID is canonical"),
            },
        )
        .expect("stage tracked-root branch control");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("publish tracked-root branch");
    }

    fn finite_pk_scan_request(
        branch_id: &str,
        schema_key: &str,
        row_pks: Vec<RowPk>,
    ) -> HotStateScanRequest {
        HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_string()],
                row_pks,
                branch_ids: vec![branch_id.to_string()],
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        }
    }

    async fn scan_direct_row_pk_rows_for_test(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        request: &HotStateScanRequest,
    ) -> Result<Option<Vec<MaterializedHotStateRow>>, LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open direct hot-state scan read");
        let scope = scan_scope(&read, request, true, None).await?;
        hot_state
            .reader(read)
            .scan_direct_row_pk_rows(request, &scope)
            .await
    }

    #[tokio::test]
    async fn transaction_branch_head_control_cache_pins_loaded_generation() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "ffffffff-ffff-7fff-bfff-ffffffffffff";
        let row_pk = RowPk::single("cached-control-row");
        stage_direct_row_head(
            &storage,
            branch_id,
            CommitId::for_test_label("cached-control-head"),
            "schema",
            &row_pk,
            r#"{"value":"one"}"#,
        )
        .await;

        let cache = BranchHeadControlCache::default();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open first branch-control read");
        let first = load_branch_head_controls(&read, &[branch_id.to_string()], Some(&cache))
            .await
            .expect("first branch control should load")[branch_id];
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open branch-control update read");
        let mut current = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .expect("branch control should load")
            .expect("branch control should exist");
        current.current_state_revision += 1;
        let mut writes = StorageWriteSet::new();
        crate::branch::stage_branch_head_control(&mut writes, branch_id, current)
            .expect("updated branch control should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("updated branch control should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open repeated branch-control read");
        let pinned = load_branch_head_controls(&read, &[branch_id.to_string()], Some(&cache))
            .await
            .expect("cached branch control should load")[branch_id];
        let uncached = load_branch_head_controls(&read, &[branch_id.to_string()], None)
            .await
            .expect("uncached branch control should load")[branch_id];
        assert_eq!(pinned, first);
        assert_eq!(uncached, current);
        assert_ne!(
            pinned.current_state_revision,
            uncached.current_state_revision
        );

        let full_cache = BranchHeadControlCache::default();
        {
            let mut controls = full_cache
                .controls
                .lock()
                .expect("branch-control cache lock should not be poisoned");
            for index in 0..TRANSACTION_BRANCH_HEAD_CONTROL_CACHE_MAX_ENTRIES {
                controls.insert(format!("uncached-branch-{index}"), None);
            }
        }
        let overflow =
            load_branch_head_controls(&read, &[branch_id.to_string()], Some(&full_cache))
                .await
                .expect("control beyond the cache capacity should still load")[branch_id];
        assert_eq!(overflow, current);
        assert!(
            !full_cache
                .controls
                .lock()
                .expect("branch-control cache lock should not be poisoned")
                .contains_key(branch_id)
        );
    }

    async fn scan_rows_for_test(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        request: &HotStateScanRequest,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open normal scan read"),
            )
            .scan_batch(request)
            .await
            .map(MaterializedHotStateBatch::into_rows)
    }

    #[tokio::test]
    async fn exact_count_uses_local_control_and_rechecks_small_global_overlay() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-branch";
        let schema_key = "exact_count_rows";
        let local_only = RowPk::single("local-only");
        let local_file_row = RowPk::single("local-file-row");
        let global_only = RowPk::single("global-only");
        let shadowed_global = RowPk::single("shadowed-global");

        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-global-head"),
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &global_only,
                    file_id: None,
                    snapshot: Some(r#"{"value":"global"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &shadowed_global,
                    file_id: None,
                    snapshot: Some(r#"{"value":"shadowed"}"#),
                    deleted: false,
                },
            ],
        )
        .await;
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-local-head"),
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &local_only,
                    file_id: None,
                    snapshot: Some(r#"{"value":"local"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &local_file_row,
                    file_id: Some("file-1"),
                    snapshot: Some(r#"{"value":"local-file"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &shadowed_global,
                    file_id: None,
                    snapshot: None,
                    deleted: true,
                },
            ],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("finite local control and small global overlay should count"),
            Some(3),
            "two local rows (including one file-backed row) and one unshadowed global row should remain visible"
        );

        // The bounded global candidate probe must retain only identities. A
        // COUNT(*) never needs to hydrate the potentially large row payload.
        let global_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global head control should load")
            .expect("global head control should exist");
        let global_candidates = TrackedHeadContext::new()
            .reader(&read)
            .try_scan_bounded_live_identities(
                GLOBAL_BRANCH_ID,
                global_control,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["change_id".to_owned()],
                    },
                    limit: Some(EXACT_COUNT_GLOBAL_MAX_ENTRIES + 1),
                },
                EXACT_COUNT_GLOBAL_MAX_ENTRIES,
                EXACT_COUNT_GLOBAL_MAX_BYTES,
            )
            .await
            .expect("bounded identity probe should execute")
            .expect("two global rows fit the physical entry and byte budgets");
        assert_eq!(global_candidates.identities.len(), 2);
        assert_eq!(global_candidates.physical_entries, 2);
        assert!(global_candidates.physical_bytes <= EXACT_COUNT_GLOBAL_MAX_BYTES);
        assert!(global_candidates.identity_bytes < global_candidates.physical_bytes);
    }

    #[tokio::test]
    async fn exact_count_does_not_retain_one_read_interest_per_global_candidate() {
        let storage = StorageAdapter::new(Memory::new());
        let registry = crate::hot_state::ReadInterestRegistry::new(32, 64 * 1024);
        let hot_state = hot_state_context().with_read_interest_registry(registry.clone());
        let branch_id = "exact-count-interest-branch";
        let schema_key = "exact_count_interest_rows";
        let row_pks = (0..32)
            .map(|index| RowPk::single(format!("global-{index:02}")))
            .collect::<Vec<_>>();
        let global_rows = row_pks
            .iter()
            .map(|row_pk| DirectTrackedHeadRow {
                schema_key,
                row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            })
            .collect::<Vec<_>>();
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-interest-global-head"),
            &global_rows,
        )
        .await;
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-interest-local-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("a bounded overlay should fit a 32-entry read-interest registry"),
            Some(33)
        );

        let interests = registry.snapshot().expect("read interests should snapshot");
        assert_eq!(interests.interests.len(), 2);
        assert!(interests.interests.iter().all(|interest| !matches!(
            interest.as_ref(),
            crate::hot_state::LogicalReadInterest::Exact { .. }
        )));
        assert!(
            interests
                .interests
                .iter()
                .any(|interest| match interest.as_ref() {
                    crate::hot_state::LogicalReadInterest::Scan { request, domain } => {
                        request.filter.schema_keys == [schema_key]
                            && request.filter.branch_ids == [branch_id]
                            && request.projection.columns == ["change_id"]
                            && *domain == crate::hot_state::InterestDomain::Combined
                    }
                    _ => false,
                })
        );
    }

    #[tokio::test]
    async fn exact_count_falls_back_when_collection_control_is_missing() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-missing-control-branch";
        let present_schema = "exact_count_present_rows";
        let absent_schema = "exact_count_missing_rows";
        let global_pk = RowPk::single("global-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-missing-control-global-head"),
            &[DirectTrackedHeadRow {
                schema_key: present_schema,
                row_pk: &global_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            }],
        )
        .await;
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-missing-control-local-head"),
            &[DirectTrackedHeadRow {
                schema_key: present_schema,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![absent_schema.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("missing controls should use the regular scan"),
            None
        );
        assert_eq!(
            hot_state
                .reader(&read)
                .scan_batch(&request)
                .await
                .expect("regular scan should prove the absent schema empty")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn exact_count_falls_back_when_collection_live_count_is_deferred() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-deferred-control-branch";
        let schema_key = "exact_count_deferred_rows";
        let global_pk = RowPk::single("global-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-deferred-global-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            }],
        )
        .await;
        let local_pk = RowPk::single("local-row");
        let local_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            None,
            CommitId::for_test_label("exact-count-deferred-local-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;
        let mut writes = storage.new_write_set();
        crate::hot_state::stage_hot_collection_live_count_for_test(
            &mut writes,
            branch_id,
            local_generation,
            schema_key,
            crate::collection_generation::DEFERRED_LIVE_COUNT,
        )
        .expect("deferred collection control should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("deferred control should commit");

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("deferred controls should use the regular scan"),
            None
        );
        assert_eq!(
            hot_state
                .reader(&read)
                .scan_batch(&request)
                .await
                .expect("regular scan should resolve deferred cardinality")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn exact_count_declines_deferred_global_count_after_collection_fence() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-global-fence-branch";
        let schema_key = "exact_count_global_fence_rows";
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0001_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let retired_pk = RowPk::single("retired-global-row");
        let global_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            None,
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0002_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &retired_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"retired"}"#),
                deleted: false,
            }],
        )
        .await;
        let marker_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0003_0000_7000_8000_0000_0000_0000,
        ));
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(scope));
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            Some(global_generation),
            marker_head,
            &[DirectTrackedHeadRow {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                row_pk: &marker_pk,
                file_id: None,
                snapshot: Some("{}"),
                deleted: false,
            }],
        )
        .await;
        let current_global_pk = RowPk::single("current-global-row");
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            Some(global_generation),
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0004_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &current_global_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"current"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        let global_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global head control should load")
            .expect("global head control should exist");
        let collection = TrackedHeadContext::new()
            .reader(&read)
            .stored_collection_generation(
                GLOBAL_BRANCH_ID,
                global_control.tracked_generation,
                scope,
            )
            .await
            .expect("global collection control should load")
            .expect("global collection control should exist");
        assert_eq!(collection.active_generation, marker_head);
        assert_eq!(
            collection.live_count,
            crate::collection_generation::DEFERRED_LIVE_COUNT
        );
        let reader = hot_state.reader(&read);
        assert_eq!(
            reader
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("post-fence counts should use the regular scan"),
            None,
            "a collection fence cannot prove an exact count while untracked survivors are possible"
        );
        assert_eq!(
            reader
                .scan_batch(&request)
                .await
                .expect("regular scan")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn exact_count_declines_global_overlay_over_entry_budget() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-large-global-branch";
        let schema_key = "exact_count_large_global_rows";
        let row_pks = (0..=EXACT_COUNT_GLOBAL_MAX_ENTRIES)
            .map(|index| RowPk::single(format!("global-{index:03}")))
            .collect::<Vec<_>>();
        let rows = row_pks
            .iter()
            .map(|row_pk| DirectTrackedHeadRow {
                schema_key,
                row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            })
            .collect::<Vec<_>>();
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-large-global-head"),
            &rows,
        )
        .await;
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-large-local-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("an over-budget global scope should fall back"),
            None,
            "the exact path must decline instead of scanning past its global budget"
        );
    }

    #[tokio::test]
    async fn exact_count_declines_global_overlay_over_value_byte_budget() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-large-value-branch";
        let schema_key = "exact_count_large_value_rows";
        let global_pk = RowPk::single("large-global-value");
        let local_pk = RowPk::single("local-row");
        let alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
        let mut state = 0x6d2b_79f5_u32;
        let mut value = String::with_capacity(900 * 1024);
        for _ in 0..(900 * 1024) {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            value.push(alphabet[(state as usize) % alphabet.len()] as char);
        }
        let snapshot = format!(r#"{{"value":"{value}"}}"#);
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-large-value-global-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_pk,
                file_id: None,
                snapshot: Some(&snapshot),
                deleted: false,
            }],
        )
        .await;
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-large-value-local-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("an over-budget HOT value should use the regular scan"),
            None,
            "the scanner must decline after accounting for key and full value bytes"
        );
    }

    #[tokio::test]
    async fn exact_count_declines_small_live_overlay_with_many_physical_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-many-tombstones-branch";
        let schema_key = "exact_count_many_tombstones_rows";
        let row_pks = (0..EXACT_COUNT_GLOBAL_MAX_ENTRIES)
            .map(|index| RowPk::single(format!("retired-{index:03}")))
            .collect::<Vec<_>>();
        let mut rows = row_pks
            .iter()
            .map(|row_pk| DirectTrackedHeadRow {
                schema_key,
                row_pk,
                file_id: None,
                snapshot: None,
                deleted: true,
            })
            .collect::<Vec<_>>();
        let live_pk = RowPk::single("one-live-row");
        rows.push(DirectTrackedHeadRow {
            schema_key,
            row_pk: &live_pk,
            file_id: None,
            snapshot: Some(r#"{"value":"global"}"#),
            deleted: false,
        });
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-many-tombstones-global-head"),
            &rows,
        )
        .await;
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-many-tombstones-local-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("many physical tombstones should use the regular scan"),
            None,
            "a small live count does not justify scanning an unbounded physical key range"
        );
    }

    #[tokio::test]
    async fn exact_count_declines_file_backed_global_overlay() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-global-file-branch";
        let schema_key = "exact_count_global_file_rows";
        let global_file_pk = RowPk::single("global-file-row");
        let local_pk = RowPk::single("local-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-global-file-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_file_pk,
                file_id: Some("global-file"),
                snapshot: Some(r#"{"value":"global-file"}"#),
                deleted: false,
            }],
        )
        .await;
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-local-file-head"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &local_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"local"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("file-backed global rows should use the normal scan"),
            None,
            "the bounded global probe is limited to schemas with no file members"
        );
    }

    #[tokio::test]
    async fn exact_count_declines_legacy_finite_schema_count_after_file_replacement() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-file-fence-branch";
        let schema_key = "exact_count_file_fence_rows";
        let local_keep = RowPk::single("local-keep");
        let shared_pk = RowPk::single("shared-row");
        let retired_file_pk = RowPk::single("retired-file-row");
        let global_only = RowPk::single("global-only");
        let local_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0001_0000_7000_8000_0000_0000_0000,
        ));
        let marker_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0002_0000_7000_8000_0000_0000_0000,
        ));
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-file-fence-global-head"),
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &shared_pk,
                    file_id: None,
                    snapshot: Some(r#"{"value":"shadowed-global"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &global_only,
                    file_id: None,
                    snapshot: Some(r#"{"value":"global"}"#),
                    deleted: false,
                },
            ],
        )
        .await;
        let local_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            None,
            local_head,
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &local_keep,
                    file_id: None,
                    snapshot: Some(r#"{"value":"local"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &shared_pk,
                    file_id: None,
                    snapshot: Some(r#"{"value":"shadowing-local"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &retired_file_pk,
                    file_id: Some("replaced-file"),
                    snapshot: Some(r#"{"value":"retired"}"#),
                    deleted: false,
                },
            ],
        )
        .await;

        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: Some("replaced-file"),
        };
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(scope));
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            Some(local_generation),
            marker_head,
            &[DirectTrackedHeadRow {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                row_pk: &marker_pk,
                file_id: None,
                snapshot: Some("{}"),
                deleted: false,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        let branch_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .expect("local branch control should load")
            .expect("local branch control should exist");
        let collection_scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        let aggregate = TrackedHeadContext::new()
            .reader(&read)
            .stored_collection_generation(
                branch_id,
                branch_control.tracked_generation,
                collection_scope,
            )
            .await
            .expect("schema aggregate should load")
            .expect("schema aggregate should exist");
        assert_eq!(
            aggregate.live_count,
            crate::collection_generation::DEFERRED_LIVE_COUNT,
            "a new file marker must invalidate the schema-wide aggregate"
        );
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("new file fences should use the regular scan"),
            None
        );
        drop(read);

        // Reproduce a repository written before file-marker publication began
        // invalidating the schema aggregate: the old finite count (3) remains
        // beside the newer file-scope fence.
        let mut legacy_writes = storage.new_write_set();
        crate::hot_state::stage_hot_collection_live_count_for_test(
            &mut legacy_writes,
            branch_id,
            branch_control.tracked_generation,
            schema_key,
            3,
        )
        .expect("legacy finite schema control should stage");
        storage
            .commit_write_set(legacy_writes, StorageWriteOptions::default())
            .await
            .expect("legacy finite schema control should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopen exact-count read");
        let reader = hot_state.reader(&read);
        assert_eq!(
            reader
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("legacy file fences should fall back"),
            None,
            "a finite schema count cannot cover a newer file-scope replacement"
        );
        assert_eq!(
            reader
                .scan_batch(&request)
                .await
                .expect("regular scan should apply file fence and global shadowing")
                .len(),
            3,
            "the file row is retired, the local collision wins, and the other global row remains"
        );

        let mut payload_request = request;
        payload_request.projection.columns = vec!["snapshot_content".to_owned()];
        let rows = reader
            .scan_batch(&payload_request)
            .await
            .expect("payload scan should use regular visibility")
            .into_rows();
        let shared_rows = rows
            .iter()
            .filter(|row| row.row_pk == shared_pk)
            .collect::<Vec<_>>();
        assert_eq!(shared_rows.len(), 1);
        assert!(!shared_rows[0].global);
        assert_eq!(
            shared_rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"shadowing-local"}"#)
        );

        let mut exact_request = payload_request;
        exact_request.filter.row_pks = vec![shared_pk.clone()];
        let exact_rows = reader
            .scan_batch(&exact_request)
            .await
            .expect("exact point scan should apply file and schema fences")
            .into_rows();
        assert_eq!(exact_rows.len(), 1);
        assert!(!exact_rows[0].global);
        assert_eq!(
            exact_rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"shadowing-local"}"#)
        );
    }

    #[tokio::test]
    async fn exact_count_declines_legacy_global_file_fence_count() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-global-file-fence-branch";
        let schema_key = "exact_count_global_file_fence_rows";
        let local_pk = RowPk::single("local-row");
        let global_shared = RowPk::single("shared-row");
        let global_only = RowPk::single("global-only");
        let retired_file_pk = RowPk::single("retired-global-file-row");
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-count-global-file-fence-local-head"),
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &local_pk,
                    file_id: None,
                    snapshot: Some(r#"{"value":"local"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &global_shared,
                    file_id: None,
                    snapshot: Some(r#"{"value":"local"}"#),
                    deleted: false,
                },
            ],
        )
        .await;
        let global_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0001_0000_7000_8000_0000_0000_0000,
        ));
        let marker_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0002_0000_7000_8000_0000_0000_0000,
        ));
        let global_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            None,
            global_head,
            &[
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &global_shared,
                    file_id: None,
                    snapshot: Some(r#"{"value":"shadowed-global"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &global_only,
                    file_id: None,
                    snapshot: Some(r#"{"value":"global"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key,
                    row_pk: &retired_file_pk,
                    file_id: Some("global-replaced-file"),
                    snapshot: Some(r#"{"value":"retired"}"#),
                    deleted: false,
                },
            ],
        )
        .await;
        let marker_scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: Some("global-replaced-file"),
        };
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(
            marker_scope,
        ));
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            Some(global_generation),
            marker_head,
            &[DirectTrackedHeadRow {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                row_pk: &marker_pk,
                file_id: None,
                snapshot: Some("{}"),
                deleted: false,
            }],
        )
        .await;

        // Restore the old finite aggregate beside the producer-created file
        // marker; this is the metadata shape persisted by affected versions.
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open global generation read");
        let global_head = BranchHeadControlContext::new()
            .reader(&read)
            .load(GLOBAL_BRANCH_ID)
            .await
            .expect("global branch control should load")
            .expect("global branch control should exist");
        drop(read);
        assert_eq!(global_head.tracked_generation, global_generation);
        let mut legacy_writes = storage.new_write_set();
        crate::hot_state::stage_hot_collection_live_count_for_test(
            &mut legacy_writes,
            GLOBAL_BRANCH_ID,
            global_generation,
            schema_key,
            3,
        )
        .expect("legacy finite global control should stage");
        storage
            .commit_write_set(legacy_writes, StorageWriteOptions::default())
            .await
            .expect("legacy finite global control should commit");

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        let reader = hot_state.reader(&read);
        assert_eq!(
            reader
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("legacy global file fences should fall back"),
            None,
            "the global aggregate must also be checked for file-scope fences"
        );
        assert_eq!(
            reader
                .scan_batch(&request)
                .await
                .expect("regular scan should apply the global file fence")
                .len(),
            3,
            "the retired global file row is hidden, local row shadows its collision, and another global row remains"
        );
        let mut payload_request = request;
        payload_request.projection.columns = vec!["snapshot_content".to_owned()];
        let rows = reader
            .scan_batch(&payload_request)
            .await
            .expect("payload scan should use regular visibility")
            .into_rows();
        let shared_rows = rows
            .iter()
            .filter(|row| row.row_pk == global_shared)
            .collect::<Vec<_>>();
        assert_eq!(shared_rows.len(), 1);
        assert!(!shared_rows[0].global);
        assert_eq!(
            shared_rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"local"}"#)
        );
    }

    #[tokio::test]
    async fn exact_count_declines_legacy_finite_count_for_root_backed_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-root-file-fence-branch";
        let schema_key = "exact_count_root_file_fence_rows";
        let old_file_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0001_0000_7000_8000_0000_0000_0000,
        ));
        let marker_commit = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0002_0000_7000_8000_0000_0000_0000,
        ));
        let root_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0003_0000_7000_8000_0000_0000_0000,
        ));
        let file_pk = RowPk::single("root-retired-row");
        let keep_pk = RowPk::single("root-kept-row");
        let marker_scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: Some("root-replaced-file"),
        };
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(
            marker_scope,
        ));
        let mut root_rows = vec![
            mixed_file_root_row(
                schema_key,
                file_pk,
                Some("root-replaced-file"),
                Some(r#"{"value":"retired"}"#),
                false,
                "exact-count-root-file-row",
                old_file_commit,
            ),
            mixed_file_root_row(
                schema_key,
                keep_pk,
                None,
                Some(r#"{"value":"kept"}"#),
                false,
                "exact-count-root-unfiled-row",
                marker_commit,
            ),
            mixed_file_root_row(
                crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                marker_pk,
                None,
                Some("{}"),
                false,
                "exact-count-root-file-marker",
                marker_commit,
            ),
        ];
        root_rows[2].created_at = "2026-01-01T00:00:01Z".to_owned();
        root_rows[2].updated_at = "2026-01-01T00:00:01Z".to_owned();
        stage_root_tracked_head_rows(&storage, branch_id, root_head, &root_rows).await;
        let global_tombstone = RowPk::single("global-tombstone");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-root-file-global-control"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_tombstone,
                file_id: None,
                snapshot: None,
                deleted: true,
            }],
        )
        .await;
        let mut legacy_writes = storage.new_write_set();
        crate::hot_state::stage_hot_collection_live_count_for_test(
            &mut legacy_writes,
            branch_id,
            root_head,
            schema_key,
            2,
        )
        .expect("legacy finite root-backed control should stage");
        storage
            .commit_write_set(legacy_writes, StorageWriteOptions::default())
            .await
            .expect("legacy finite root-backed control should commit");

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open root-backed exact-count read");
        let collection = TrackedHeadContext::new()
            .reader(&read)
            .collection_generation(
                branch_id,
                root_head,
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                },
            )
            .await
            .expect("root-backed collection metadata should load");
        assert_eq!(
            collection.live_count,
            crate::collection_generation::DEFERRED_LIVE_COUNT,
            "root marker catalogs must invalidate finite HOT schema aggregates"
        );
        assert_eq!(collection.ordered_identity_digest, None);
        let reader = hot_state.reader(&read);
        assert_eq!(
            reader
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("root-backed aggregates should fall back"),
            None,
            "a root marker catalog is not bounded by the finite HOT count"
        );
        assert_eq!(
            reader
                .scan_batch(&request)
                .await
                .expect("regular root scan should apply its file fence")
                .len(),
            1,
            "the old file row is hidden while the unfiled root row remains"
        );
    }

    #[tokio::test]
    async fn legacy_zero_count_fence_preserves_untracked_survivor_visibility() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-untracked-fence-branch";
        let schema_key = "exact_count_untracked_fence_rows";
        let local_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0001_0000_7000_8000_0000_0000_0000,
        ));
        let marker_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0002_0000_7000_8000_0000_0000_0000,
        ));
        let tracked_pk = RowPk::single("tracked-retired");
        let untracked_pk = RowPk::single("untracked-survivor");
        let local_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            None,
            local_head,
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &tracked_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"tracked"}"#),
                deleted: false,
            }],
        )
        .await;
        let untracked_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open untracked write read");
        write_untracked_rows_to_store(
            &storage,
            &untracked_read,
            &[MaterializedUntrackedStateRow {
                row_pk: untracked_pk.clone(),
                schema_key: schema_key.to_owned(),
                file_id: None,
                snapshot_content: Some(r#"{"value":"untracked"}"#.to_owned()),
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_owned(),
                updated_at: "2026-01-01T00:00:00Z".to_owned(),
                branch_id: branch_id.to_owned(),
            }],
        )
        .await;
        drop(untracked_read);

        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(scope));
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            Some(local_generation),
            marker_head,
            &[DirectTrackedHeadRow {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                row_pk: &marker_pk,
                file_id: None,
                snapshot: Some("{}"),
                deleted: false,
            }],
        )
        .await;
        let global_tombstone = RowPk::single("global-control");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("exact-count-untracked-global-control"),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_tombstone,
                file_id: None,
                snapshot: None,
                deleted: true,
            }],
        )
        .await;

        // Recreate a legacy marker control that stored finite zero even though
        // the row-level fence deliberately leaves untracked members visible.
        let mut legacy_writes = storage.new_write_set();
        crate::hot_state::stage_hot_collection_control_for_test(
            &mut legacy_writes,
            branch_id,
            local_generation,
            schema_key,
            None,
            marker_head,
            0,
        )
        .expect("legacy zero schema control should stage");
        storage
            .commit_write_set(legacy_writes, StorageWriteOptions::default())
            .await
            .expect("legacy zero schema control should commit");

        let count_request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open legacy fenced read");
        let reader = hot_state.reader(&read);
        let collection = TrackedHeadContext::new()
            .reader(&read)
            .stored_collection_generation(branch_id, local_generation, scope)
            .await
            .expect("legacy schema control should load")
            .expect("legacy schema control should exist");
        assert_eq!(
            collection.live_count,
            crate::collection_generation::DEFERRED_LIVE_COUNT,
            "a finite count from an older scope generation must be normalized"
        );
        assert_eq!(
            reader
                .exact_count_with_bounded_global_overlay(&count_request)
                .await
                .expect("legacy fence should use normal count scan"),
            None
        );
        let mut payload_request = count_request;
        payload_request.projection.columns = vec!["snapshot_content".to_owned()];
        let rows = reader
            .scan_batch(&payload_request)
            .await
            .expect("ordinary scan should keep untracked survivor")
            .into_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, untracked_pk);
        assert!(rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"untracked"}"#)
        );
    }

    #[tokio::test]
    async fn exact_count_declines_control_across_collection_generation_fence() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "exact-count-fenced-branch";
        let schema_key = "exact_count_fenced_rows";
        let row_pk = RowPk::single("retired-row");
        let local_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            None,
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0001_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"retired"}"#),
                deleted: false,
            }],
        )
        .await;

        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key,
            file_id: None,
        };
        let marker_pk = RowPk::single(crate::collection_generation::collection_scope_key(scope));
        let marker_head = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0000_0002_0000_7000_8000_0000_0000_0000,
        ));
        let local_serving_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            branch_id,
            Some(local_generation),
            marker_head,
            &[DirectTrackedHeadRow {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY,
                row_pk: &marker_pk,
                file_id: None,
                snapshot: Some("{}"),
                deleted: false,
            }],
        )
        .await;
        assert_eq!(local_serving_generation, local_generation);
        // A global control for the same schema is required, but with a zero
        // live count, so the exact path never needs to enumerate global rows.
        let global_row_pk = RowPk::single("retired-global-row");
        let global_generation = stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            None,
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0003_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"global"}"#),
                deleted: false,
            }],
        )
        .await;
        stage_direct_tracked_head_rows_in_generation(
            &storage,
            GLOBAL_BRANCH_ID,
            Some(global_generation),
            CommitId::with_change_address_space(uuid::Uuid::from_u128(
                0x0000_0004_0000_7000_8000_0000_0000_0000,
            )),
            &[DirectTrackedHeadRow {
                schema_key,
                row_pk: &global_row_pk,
                file_id: None,
                snapshot: None,
                deleted: true,
            }],
        )
        .await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            projection: HotStateProjection {
                columns: vec!["change_id".to_owned()],
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open exact-count read");
        let branch_control = BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .expect("local head control should load")
            .expect("local head control should exist");
        let collection = TrackedHeadContext::new()
            .reader(&read)
            .stored_collection_generation(branch_id, branch_control.tracked_generation, scope)
            .await
            .expect("stored collection control should load")
            .expect("stored collection control should exist");
        assert_eq!(
            collection.live_count,
            crate::collection_generation::DEFERRED_LIVE_COUNT
        );
        assert_ne!(
            collection.active_generation, branch_control.tracked_generation,
            "a collection marker fence is distinct from the serving branch generation"
        );
        assert_eq!(collection.active_generation, marker_head);
        let reader = hot_state.reader(&read);
        assert_eq!(
            hot_state
                .reader(&read)
                .exact_count_with_bounded_global_overlay(&request)
                .await
                .expect("post-fence counts should use the regular scan"),
            None,
            "fenced controls cannot certify an aggregate while untracked survivors are possible"
        );
        assert_eq!(
            reader
                .scan_batch(&request)
                .await
                .expect("regular scan should apply the schema fence")
                .len(),
            0
        );
    }

    async fn scan_direct_row_snapshots_for_test(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        schema_key: &str,
        row_pks: &[RowPk],
    ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
        hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open direct row read"),
            )
            .scan_direct_row_snapshots(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![schema_key.to_string()],
                    row_pks: row_pks.to_vec(),
                    branch_ids: vec![branch_id.to_string()],
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await
    }

    #[test]
    fn ordered_head_fast_path_requires_one_matching_branch_candidate() {
        let requested_branch_ids = vec!["branch".to_string()];
        let branch = HotBranchRows {
            branch_id: "branch".to_string(),
            rows: MaterializedHotStateBatch::from_rows(vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )]),
            ordered_unique: true,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[branch], &requested_branch_ids),
            Some(0)
        );

        let branch = HotBranchRows {
            branch_id: "branch".to_string(),
            rows: MaterializedHotStateBatch::from_rows(vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )]),
            ordered_unique: true,
        };
        let global = HotBranchRows {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            rows: MaterializedHotStateBatch::from_rows(vec![tracked_row_at_with_commit(
                GLOBAL_BRANCH_ID,
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                None,
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
            )]),
            ordered_unique: true,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[branch, global], &requested_branch_ids),
            None,
            "a global candidate needs normal branch/global resolution"
        );

        let unordered_candidate = HotBranchRows {
            branch_id: "branch".to_string(),
            rows: MaterializedHotStateBatch::from_rows(vec![tracked_row_at_with_commit(
                "branch", "branch", None, "branch",
            )]),
            ordered_unique: false,
        };
        assert_eq!(
            ordered_unique_branch_row_index(&[unordered_candidate], &requested_branch_ids),
            None,
            "an unordered candidate does not make the table ordering promise"
        );
    }

    fn dominant_merge_test_row(
        branch_id: &str,
        row_pk: &str,
        file_id: Option<&str>,
        deleted: bool,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: RowPk::single(row_pk),
            schema_key: "dominant_merge_test".to_string(),
            file_id: file_id.map(str::to_owned),
            snapshot_content: (!deleted).then(|| "{\"value\":true}".into()),
            metadata: None,
            deleted,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            global: branch_id == GLOBAL_BRANCH_ID,
            change_id: None,
            commit_id: Some(CommitId::for_test_label("dominant-merge")),
            author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            untracked: false,
            branch_id: branch_id.into(),
        }
    }

    fn mixed_file_root_row(
        schema_key: &str,
        row_pk: RowPk,
        file_id: Option<&str>,
        snapshot: Option<&str>,
        deleted: bool,
        change_label: &str,
        commit_id: CommitId,
    ) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            row_pk,
            schema_key: schema_key.to_owned(),
            file_id: file_id.map(str::to_owned),
            snapshot_content: snapshot.map(Into::into),
            decoded_snapshot: None,
            metadata: None,
            deleted,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            change_id: ChangeId::for_test_label(change_label),
            commit_id,
        }
    }

    #[test]
    fn dominant_local_merge_matches_visibility_for_global_collisions_files_and_tombstones() {
        let branch_id = "dominant-merge-branch";
        let local_rows = MaterializedHotStateBatch::from_rows(
            (0..DOMINANT_BRANCH_MIN_ROWS)
                .map(|index| {
                    dominant_merge_test_row(
                        branch_id,
                        &format!("row-{index:04}"),
                        Some("local-file"),
                        index == 256,
                    )
                })
                .collect(),
        );
        let mut global_rows = vec![
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0000", Some("local-file"), false),
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0000a", Some("local-file"), false),
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0002a", Some("local-file"), true),
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0256", Some("local-file"), false),
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0256", Some("other-file"), false),
            dominant_merge_test_row(GLOBAL_BRANCH_ID, "row-0511z", Some("local-file"), false),
        ];
        global_rows.sort_by(|left, right| {
            left.schema_key
                .cmp(&right.schema_key)
                .then_with(|| left.row_pk.cmp(&right.row_pk))
                .then_with(|| left.file_id.cmp(&right.file_id))
        });
        let global_rows = MaterializedHotStateBatch::from_rows(global_rows);
        let projection_branch_ids = vec![branch_id.to_string()];

        for include_tombstones in [false, true] {
            let request = HotStateScanRequest {
                filter: HotStateFilter {
                    include_tombstones,
                    ..Default::default()
                },
                ..Default::default()
            };
            let expected = resolve_visible_ordered_runs(
                &[
                    OrderedVisibilityRun {
                        branch_id,
                        rows: &local_rows,
                        ordered_unique: true,
                    },
                    OrderedVisibilityRun {
                        branch_id: GLOBAL_BRANCH_ID,
                        rows: &global_rows,
                        ordered_unique: true,
                    },
                ],
                &VisibilityRequest {
                    branch_scope: VisibilityBranchScope::BranchIds {
                        branch_ids: projection_branch_ids.clone(),
                    },
                    include_tombstones,
                    limit: None,
                },
            )
            .expect("both source runs are ordered and unique");
            let mut candidates = vec![
                HotBranchRows {
                    branch_id: GLOBAL_BRANCH_ID.to_string(),
                    rows: global_rows.clone(),
                    ordered_unique: true,
                },
                HotBranchRows {
                    branch_id: branch_id.to_string(),
                    rows: local_rows.clone(),
                    ordered_unique: true,
                },
            ];
            let actual = try_merge_dominant_branch_with_global(
                &mut candidates,
                &projection_branch_ids,
                &request,
            )
            .expect("the dominant local plus small global shape is eligible");
            let actual = actual.into_rows();
            assert_eq!(actual, expected.into_rows());
            assert!(actual.windows(2).all(|rows| {
                rows[0]
                    .schema_key
                    .cmp(&rows[1].schema_key)
                    .then_with(|| rows[0].row_pk.cmp(&rows[1].row_pk))
                    .then_with(|| rows[0].file_id.cmp(&rows[1].file_id))
                    .is_le()
            }));
            assert!(actual.iter().all(|row| row.branch_id.as_ref() == branch_id));
            assert_eq!(
                actual.iter().filter(|row| row.deleted).count(),
                if include_tombstones { 2 } else { 0 },
                "both local and global tombstones obey the request"
            );
            assert!(actual.iter().any(|row| {
                row.row_pk == RowPk::single("row-0256")
                    && row.file_id.as_deref() == Some("other-file")
                    && row.global
            }));
            assert_eq!(
                actual
                    .iter()
                    .filter(|row| row.row_pk == RowPk::single("row-0256")
                        && row.file_id.as_deref() == Some("local-file"))
                    .count(),
                if include_tombstones { 1 } else { 0 },
                "the local tombstone shadows a live global row of the same identity"
            );
        }
    }

    #[tokio::test]
    async fn mixed_file_memory_scan_proves_order_before_dominant_visibility_merge() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "mixed-file-order-branch";
        let schema_key = "mixed_file_order_rows";

        // The tracked tree orders file scope before row PK. These 400 `z`
        // rows in the first file therefore precede 200 `a` rows in the second
        // file, although visibility identity order puts every `a` before every
        // `z`. The tombstone is deliberately inside the second file's range,
        // where binary search over the physical order can miss it.
        let local_pks = (0..400)
            .map(|index| RowPk::single(format!("z-row-{index:04}")))
            .chain((0..200).map(|index| RowPk::single(format!("a-row-{index:04}"))))
            .collect::<Vec<_>>();
        let local_head = CommitId::for_test_label("mixed-file-local-root");
        let local_rows = local_pks
            .iter()
            .enumerate()
            .map(|(index, row_pk)| {
                let deleted = index == 450;
                mixed_file_root_row(
                    schema_key,
                    row_pk.clone(),
                    Some(if index < 400 { "a-file" } else { "b-file" }),
                    (!deleted).then_some(r#"{"value":"local"}"#),
                    deleted,
                    &format!("mixed-file-local-change-{index}"),
                    local_head,
                )
            })
            .collect::<Vec<_>>();
        stage_root_tracked_head_rows(&storage, branch_id, local_head, &local_rows).await;

        let global_head = CommitId::for_test_label("mixed-file-global-root");
        let shadowed_pk = RowPk::single("a-row-0050");
        let file_scoped_pk = RowPk::single("a-row-0050");
        let live_collision_pk = RowPk::single("a-row-0051");
        let global_only_pk = RowPk::single("a-global-only");
        let global_rows = vec![
            mixed_file_root_row(
                schema_key,
                shadowed_pk.clone(),
                Some("b-file"),
                Some(r#"{"value":"must-be-hidden"}"#),
                false,
                "mixed-file-global-shadowed-change",
                global_head,
            ),
            mixed_file_root_row(
                schema_key,
                file_scoped_pk.clone(),
                Some("c-file"),
                Some(r#"{"value":"different-file"}"#),
                false,
                "mixed-file-global-other-file-change",
                global_head,
            ),
            mixed_file_root_row(
                schema_key,
                live_collision_pk.clone(),
                Some("b-file"),
                Some(r#"{"value":"must-lose-to-local"}"#),
                false,
                "mixed-file-global-live-collision-change",
                global_head,
            ),
            mixed_file_root_row(
                schema_key,
                global_only_pk.clone(),
                Some("b-file"),
                Some(r#"{"value":"global-only"}"#),
                false,
                "mixed-file-global-only-change",
                global_head,
            ),
        ];
        stage_root_tracked_head_rows(&storage, GLOBAL_BRANCH_ID, global_head, &global_rows).await;

        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                branch_ids: vec![branch_id.to_owned()],
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open mixed-file ordering read");
        let tracked_head_reader = TrackedHeadContext::new().reader(&read);
        assert_eq!(
            tracked_head_reader
                .root_current_base_commit(branch_id, local_head)
                .await
                .expect("read local root-current-base selector"),
            Some(local_head),
            "fixture must exercise the local ROOT_CURRENT_BASE reader"
        );
        assert_eq!(
            tracked_head_reader
                .root_current_base_commit(GLOBAL_BRANCH_ID, global_head)
                .await
                .expect("read global root-current-base selector"),
            Some(global_head),
            "global fallback must also come from ROOT_CURRENT_BASE"
        );
        let scope = scan_scope(&read, &request, true, None)
            .await
            .expect("resolve mixed-file branch scope");
        let reader = hot_state.reader(&read);
        let mut source_runs = reader
            .scan_hot_branch_rows(&request, &scope)
            .await
            .expect("read mixed-file materialized source runs");
        let local_run = source_runs
            .iter()
            .find(|run| run.branch_id == branch_id)
            .expect("local source run exists");
        assert_eq!(local_run.rows.len(), 600);
        assert!(
            !materialized_batch_is_strictly_ordered_unique(&local_run.rows),
            "the producer must not claim row-PK order for file-first storage order"
        );
        assert!(!local_run.ordered_unique);
        assert!(
            try_merge_dominant_branch_with_global(
                &mut source_runs,
                &scope.projection_branch_ids,
                &request,
            )
            .is_none(),
            "the >=512-row dominant path must decline an unproven source order"
        );

        let visible = reader
            .scan_batch(&request)
            .await
            .expect("general visibility fallback resolves mixed-file rows")
            .into_rows();
        assert_eq!(visible.len(), 601);
        assert!(
            !visible.iter().any(|row| {
                row.row_pk == shadowed_pk && row.file_id.as_deref() == Some("b-file")
            }),
            "the local tombstone must hide the same global identity"
        );
        assert!(
            visible.iter().any(|row| {
                row.row_pk == file_scoped_pk
                    && row.file_id.as_deref() == Some("c-file")
                    && row.global
            }),
            "a global row with the same PK in another file remains visible"
        );
        let live_collision = visible
            .iter()
            .filter(|row| {
                row.row_pk == live_collision_pk && row.file_id.as_deref() == Some("b-file")
            })
            .collect::<Vec<_>>();
        assert_eq!(live_collision.len(), 1);
        assert!(!live_collision[0].global);
        assert_eq!(
            live_collision[0].snapshot_content.as_deref(),
            Some(r#"{"value":"local"}"#),
            "a local live row must win over its same-file global collision"
        );
        assert!(visible.iter().any(|row| {
            row.row_pk == global_only_pk && row.file_id.as_deref() == Some("b-file") && row.global
        }));

        let mut including_tombstones = request;
        including_tombstones.filter.include_tombstones = true;
        let visible_with_tombstones = reader
            .scan_batch(&including_tombstones)
            .await
            .expect("tombstone-inclusive visibility fallback resolves mixed-file rows")
            .into_rows();
        assert_eq!(visible_with_tombstones.len(), 602);
        assert!(
            visible_with_tombstones.iter().any(|row| {
                row.row_pk == shadowed_pk
                    && row.file_id.as_deref() == Some("b-file")
                    && row.deleted
                    && !row.global
            }),
            "including tombstones retains the local shadowing row"
        );
        assert!(!visible_with_tombstones.iter().any(|row| {
            row.row_pk == shadowed_pk && row.file_id.as_deref() == Some("b-file") && row.global
        }));

        let mut tracked_request = including_tombstones.clone();
        tracked_request.filter.include_tombstones = false;
        let tracked_visible = reader
            .scan_tracked_batch(&tracked_request)
            .await
            .expect("tracked-domain fallback resolves mixed-file rows")
            .into_rows();
        assert_eq!(tracked_visible.len(), 601);
        assert!(
            !tracked_visible.iter().any(|row| {
                row.row_pk == shadowed_pk && row.file_id.as_deref() == Some("b-file")
            })
        );
        let tracked_live_collision = tracked_visible
            .iter()
            .filter(|row| {
                row.row_pk == live_collision_pk && row.file_id.as_deref() == Some("b-file")
            })
            .collect::<Vec<_>>();
        assert_eq!(tracked_live_collision.len(), 1);
        assert!(!tracked_live_collision[0].global);
        assert_eq!(
            tracked_live_collision[0].snapshot_content.as_deref(),
            Some(r#"{"value":"local"}"#)
        );
        assert!(tracked_visible.iter().any(|row| {
            row.row_pk == file_scoped_pk
                && row.file_id.as_deref() == Some("c-file")
                && row.global
                && row.snapshot_content.as_deref() == Some(r#"{"value":"different-file"}"#)
        }));
        assert!(tracked_visible.iter().any(|row| {
            row.row_pk == global_only_pk
                && row.file_id.as_deref() == Some("b-file")
                && row.global
                && row.snapshot_content.as_deref() == Some(r#"{"value":"global-only"}"#)
        }));

        let tracked_with_tombstones = reader
            .scan_tracked_batch(&including_tombstones)
            .await
            .expect("tombstone-inclusive tracked fallback resolves mixed-file rows")
            .into_rows();
        assert_eq!(tracked_with_tombstones.len(), 602);
        assert!(tracked_with_tombstones.iter().any(|row| {
            row.row_pk == shadowed_pk
                && row.file_id.as_deref() == Some("b-file")
                && row.deleted
                && !row.global
        }));
        assert!(!tracked_with_tombstones.iter().any(|row| {
            row.row_pk == shadowed_pk && row.file_id.as_deref() == Some("b-file") && row.global
        }));
    }

    #[test]
    fn dominant_local_merge_declines_limits_small_tables_and_unordered_runs() {
        let branch_id = "dominant-merge-branch";
        let projection_branch_ids = vec![branch_id.to_string()];
        let local = MaterializedHotStateBatch::from_rows(
            (0..DOMINANT_BRANCH_MIN_ROWS)
                .map(|index| {
                    dominant_merge_test_row(branch_id, &format!("row-{index:04}"), None, false)
                })
                .collect(),
        );
        let global = MaterializedHotStateBatch::from_rows(vec![dominant_merge_test_row(
            GLOBAL_BRANCH_ID,
            "row-between",
            None,
            false,
        )]);
        let mut candidates = vec![
            HotBranchRows {
                branch_id: branch_id.to_string(),
                rows: local.clone(),
                ordered_unique: true,
            },
            HotBranchRows {
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                rows: global.clone(),
                ordered_unique: true,
            },
        ];
        assert!(
            try_merge_dominant_branch_with_global(
                &mut candidates,
                &projection_branch_ids,
                &HotStateScanRequest {
                    limit: Some(100),
                    ..Default::default()
                },
            )
            .is_none()
        );

        let mut small_candidates = candidates
            .iter()
            .map(|run| HotBranchRows {
                branch_id: run.branch_id.clone(),
                rows: run.rows.clone(),
                ordered_unique: run.ordered_unique,
            })
            .collect::<Vec<_>>();
        small_candidates[0].rows =
            MaterializedHotStateBatch::from_rows(vec![dominant_merge_test_row(
                branch_id,
                "only-local",
                None,
                false,
            )]);
        assert!(
            try_merge_dominant_branch_with_global(
                &mut small_candidates,
                &projection_branch_ids,
                &HotStateScanRequest::default(),
            )
            .is_none()
        );

        candidates[0].ordered_unique = false;
        assert!(
            try_merge_dominant_branch_with_global(
                &mut candidates,
                &projection_branch_ids,
                &HotStateScanRequest::default(),
            )
            .is_none()
        );
    }

    #[test]
    fn mutation_preparation_excludes_derived_rows_without_filtering_explicit_ids() {
        let mut commit = commit_hot_state_row("synthetic-commit");
        let synthetic_id = commit.commit_id.unwrap().commit_change_id();
        commit.change_id = Some(synthetic_id);
        let mut ordinary = tracked_row_at_with_commit("branch", "value", None, "ordinary");
        // The schema owner, not the UUID bit pattern, determines eligibility.
        ordinary.change_id = Some(synthetic_id);
        let batch = MaterializedHotStateBatch::from_rows(vec![commit, ordinary]);
        assert!(CurrentReadIdentity::from_row(batch.get(0).unwrap()).is_none());
        assert_eq!(
            CurrentReadIdentity::from_row(batch.get(1).unwrap())
                .unwrap()
                .change_id,
            synthetic_id
        );
    }

    #[test]
    fn ordered_and_single_batch_fast_paths_preserve_the_existing_columns() {
        let batch = MaterializedHotStateBatch::from_rows(vec![
            tracked_row_at_with_commit("branch", "first", None, "first"),
            tracked_row_at_with_commit("branch", "second", None, "second"),
        ]);
        let row_column = batch.row_column_ptr();
        let batch = filter_current_row_retention(batch, Some(false));
        assert_eq!(batch.row_column_ptr(), row_column);

        let row_column = batch.row_column_ptr();
        let batch = finalize_ordered_unique_batch(batch, false, None);
        assert_eq!(batch.row_column_ptr(), row_column);

        let row_column = batch.row_column_ptr();
        let batch = concat_hot_state_batches([
            MaterializedHotStateBatch::default(),
            batch,
            MaterializedHotStateBatch::default(),
        ]);
        assert_eq!(batch.row_column_ptr(), row_column);
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn direct_row_snapshots_fall_back_when_global_tracks_the_schema() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        let local_pk = RowPk::single("local-row");
        stage_direct_row_head(
            &storage,
            branch_id,
            CommitId::for_test_label("branch-head"),
            schema_key,
            &local_pk,
            r#"{"value":"local"}"#,
        )
        .await;
        stage_direct_row_head(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("global-head"),
            schema_key,
            &RowPk::single("global-row"),
            r#"{"value":"ffffffff-ffff-7fff-bfff-ffffffffffff"}"#,
        )
        .await;

        let unsupported_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("unsupported page read should open");
        let unsupported_pages = hot_state
            .reader(Arc::new(unsupported_read))
            .scan_direct_row_snapshot_pages(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec![schema_key.to_owned()],
                    branch_ids: vec![branch_id.to_owned()],
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await
            .expect("unsupported packed page layout should decline cleanly");
        assert!(
            unsupported_pages.is_none(),
            "an unpacked local row must retain the generic branch/global visibility route"
        );

        assert!(
            scan_direct_row_snapshots_for_test(
                &hot_state,
                &storage,
                branch_id,
                schema_key,
                std::slice::from_ref(&local_pk),
            )
            .await
            .expect("global row scan should execute")
            .is_none(),
            "a global tracked row requires the established branch/global resolver"
        );
    }

    #[tokio::test]
    async fn direct_row_snapshots_fall_back_for_retention_scoped_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        for untracked in [false, true] {
            let snapshots = hot_state
                .reader(
                    storage
                        .begin_read(StorageReadOptions::default())
                        .await
                        .expect("open retention-scoped row read"),
                )
                .scan_direct_row_snapshots(&HotStateScanRequest {
                    filter: HotStateFilter {
                        schema_keys: vec!["schema".to_string()],
                        row_pks: vec![RowPk::single("row")],
                        branch_ids: vec!["branch".to_string()],
                        untracked: Some(untracked),
                        ..HotStateFilter::default()
                    },
                    ..HotStateScanRequest::default()
                })
                .await
                .expect("retention-scoped row read should execute");
            assert!(
                snapshots.is_none(),
                "raw snapshot serving must not bypass retention filtering"
            );
        }
    }

    #[tokio::test]
    async fn direct_row_snapshots_read_sorted_exact_primary_keys() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let branch_id = "branch";
        let schema_key = "schema";
        let first = RowPk::single("first");
        let second = RowPk::single("second");
        let rows = [
            DirectTrackedHeadRow {
                schema_key,
                row_pk: &second,
                file_id: None,
                snapshot: Some(r#"{"id":"second","value":"two"}"#),
                deleted: false,
            },
            DirectTrackedHeadRow {
                schema_key,
                row_pk: &first,
                file_id: None,
                snapshot: Some(r#"{"id":"first","value":"one"}"#),
                deleted: false,
            },
        ];
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("exact-primary-keys"),
            &rows,
        )
        .await;

        let snapshots = scan_direct_row_snapshots_for_test(
            &hot_state,
            &storage,
            branch_id,
            schema_key,
            &[
                second.clone(),
                RowPk::single("missing"),
                first.clone(),
                second,
            ],
        )
        .await
        .expect("exact tracked row scan should execute")
        .expect("tracked-only exact row scan should use direct snapshots");
        let crate::tracked_state::ExclusiveRowSnapshotBatch::Raw(snapshots) = snapshots else {
            panic!("an exact primary-key scan should return raw native payloads");
        };
        assert_eq!(
            snapshots
                .iter()
                .map(|(row_pk, payload)| {
                    let decoded = WasmTypedRow::decode_durable_payload(
                        Arc::from(payload.as_ref()),
                        schema_key,
                        row_pk,
                    )
                    .expect("direct snapshot must be a valid native payload");
                    (
                        row_pk.clone().into_parts(),
                        decoded.row.get("value").cloned(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    vec!["first".to_string()],
                    Some(lix_schema::Value::Text("one".to_string())),
                ),
                (
                    vec!["second".to_string()],
                    Some(lix_schema::Value::Text("two".to_string())),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_returns_all_file_id_siblings() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let row_pk = RowPk::single("shared-row");
        let rows = [
            DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &row_pk,
                file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                snapshot: Some(r#"{"value":"a"}"#),
                deleted: false,
            },
            DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &row_pk,
                file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                snapshot: Some(r#"{"value":"b"}"#),
                deleted: false,
            },
        ];
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("hot-row-siblings"),
            &rows,
        )
        .await;

        let request = finite_pk_scan_request(GLOBAL_BRANCH_ID, "schema", vec![row_pk]);
        let direct = scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("direct hot-state scan should execute")
            .expect("finite tracked primary-key scan should use the hot route");
        let file_values = direct
            .iter()
            .map(|row| (row.file_id.as_deref(), row.snapshot_content.as_deref()))
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            file_values,
            std::collections::BTreeMap::from([
                (
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    Some(r#"{"value":"a"}"#)
                ),
                (
                    Some("01920000-0000-7000-8000-0000000000b2"),
                    Some(r#"{"value":"b"}"#)
                ),
            ])
        );

        let normal = scan_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("normal finite primary-key scan should execute");
        assert_eq!(normal, direct);
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_resolves_branch_override_and_tombstone_against_global() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let row_pk = RowPk::single("shared-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("global-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"ffffffff-ffff-7fff-bfff-ffffffffffff"}"#),
                deleted: false,
            }],
        )
        .await;
        stage_direct_tracked_head_rows(
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            CommitId::for_test_label("branch-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &row_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"branch"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = finite_pk_scan_request(
            "01920000-0000-7000-8000-0000000000a1",
            "schema",
            vec![row_pk.clone()],
        );
        let direct = scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("direct hot-state scan should execute")
            .expect("current branch and global controls should use the hot route");
        assert_eq!(direct.len(), 1);
        assert_eq!(
            direct[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(!direct[0].global);
        assert_eq!(
            direct[0].snapshot_content.as_deref(),
            Some(r#"{"value":"branch"}"#)
        );
        assert_eq!(
            scan_rows_for_test(&hot_state, &storage, &request)
                .await
                .expect("normal branch override scan should execute"),
            direct
        );

        stage_direct_tracked_head_rows(
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            CommitId::for_test_label("branch-tombstone"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &row_pk,
                file_id: None,
                snapshot: None,
                deleted: true,
            }],
        )
        .await;

        let hidden = scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("direct hot-state tombstone scan should execute")
            .expect("current controls should retain the hot route");
        assert!(hidden.is_empty(), "local tombstone must hide global row");
        assert!(
            scan_rows_for_test(&hot_state, &storage, &request)
                .await
                .expect("normal branch tombstone scan should execute")
                .is_empty()
        );

        let mut including_tombstones = request.clone();
        including_tombstones.filter.include_tombstones = true;
        let tombstones =
            scan_direct_row_pk_rows_for_test(&hot_state, &storage, &including_tombstones)
                .await
                .expect("direct hot-state tombstone scan should execute")
                .expect("current controls should retain the hot route");
        assert_eq!(tombstones.len(), 1);
        assert!(tombstones[0].deleted);
        assert!(!tombstones[0].global);
        assert_eq!(
            tombstones[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
    }

    #[tokio::test]
    async fn finite_pk_hot_scan_serves_mixed_current_state() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let tracked_pk = RowPk::single("tracked-row");
        let untracked_pk = RowPk::single("untracked-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("tracked-head"),
            &[DirectTrackedHeadRow {
                schema_key: "schema",
                row_pk: &tracked_pk,
                file_id: None,
                snapshot: Some(r#"{"value":"tracked"}"#),
                deleted: false,
            }],
        )
        .await;

        let request = finite_pk_scan_request(
            GLOBAL_BRANCH_ID,
            "schema",
            vec![tracked_pk.clone(), untracked_pk.clone()],
        );
        assert!(
            scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
                .await
                .expect("initial direct hot-state scan should execute")
                .is_some(),
            "the hot current state serves tracked-only rows directly"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open untracked write read");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[MaterializedUntrackedStateRow {
                row_pk: untracked_pk.clone(),
                schema_key: "schema".to_string(),
                file_id: None,
                snapshot_content: Some(r#"{"value":"untracked"}"#.to_string()),
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
            }],
        )
        .await;

        let direct = scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("mixed hot-state scan should execute")
            .expect("one hot index serves both retention modes");
        assert_eq!(direct.len(), 2);
        let rows = scan_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("mixed normal scan should execute");
        assert_eq!(rows, direct);
        assert_eq!(rows.len(), 2);
        let tracked = rows
            .iter()
            .find(|row| row.row_pk == tracked_pk)
            .expect("tracked row should remain visible");
        assert!(!tracked.untracked);
        assert_eq!(
            tracked.snapshot_content.as_deref(),
            Some(r#"{"value":"tracked"}"#)
        );
        let untracked = rows
            .iter()
            .find(|row| row.row_pk == untracked_pk)
            .expect("untracked row should be merged into normal query results");
        assert!(untracked.untracked);
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some(r#"{"value":"untracked"}"#)
        );
    }

    #[tokio::test]
    async fn explicit_file_id_predicate_retains_member_read_path() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let row_pk = RowPk::single("shared-row");
        stage_direct_tracked_head_rows(
            &storage,
            GLOBAL_BRANCH_ID,
            CommitId::for_test_label("member-path"),
            &[
                DirectTrackedHeadRow {
                    schema_key: "schema",
                    row_pk: &row_pk,
                    file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                    snapshot: Some(r#"{"value":"a"}"#),
                    deleted: false,
                },
                DirectTrackedHeadRow {
                    schema_key: "schema",
                    row_pk: &row_pk,
                    file_id: Some("01920000-0000-7000-8000-0000000000b2"),
                    snapshot: Some(r#"{"value":"b"}"#),
                    deleted: false,
                },
            ],
        )
        .await;

        let mut request = finite_pk_scan_request(GLOBAL_BRANCH_ID, "schema", vec![row_pk]);
        request.filter.file_ids = vec![NullableKeyFilter::Value(
            "01920000-0000-7000-8000-0000000000a2".to_string(),
        )];
        assert!(
            scan_direct_row_pk_rows_for_test(&hot_state, &storage, &request)
                .await
                .expect("file-filtered direct hot-state scan should execute")
                .is_none(),
            "an explicit file-id predicate must retain the member projection route"
        );
        let rows = scan_rows_for_test(&hot_state, &storage, &request)
            .await
            .expect("file-filtered normal scan should execute");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000a2")
        );
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some(r#"{"value":"a"}"#)
        );
    }

    async fn write_untracked_rows_to_store(
        storage: &StorageAdapter,
        _read: &(impl StorageAdapterRead + ?Sized),
        rows: &[MaterializedUntrackedStateRow],
    ) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("current-state read should open");
        let mut writes = storage.new_write_set();
        let mut branch_refs = std::collections::BTreeMap::new();
        let mut rows_by_branch =
            std::collections::BTreeMap::<String, Vec<&MaterializedUntrackedStateRow>>::new();
        for row in rows {
            if row.schema_key == BRANCH_REF_SCHEMA_KEY {
                if row.deleted {
                    continue;
                }
                let branch_id = row
                    .row_pk
                    .as_single_string_owned()
                    .expect("test branch ref must have one branch id key");
                let snapshot = row
                    .snapshot_content
                    .as_deref()
                    .expect("test branch ref must have a snapshot");
                let commit_id = serde_json::from_str::<serde_json::Value>(snapshot)
                    .expect("test branch-ref snapshot should be JSON")
                    .get("commit_id")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| CommitId::parse_lix(value, "test branch-ref commit"))
                    .transpose()
                    .expect("test branch-ref commit should parse")
                    .expect("test branch-ref snapshot should name a commit");
                assert!(
                    branch_refs
                        .insert(
                            branch_id,
                            (commit_id, ts(&row.created_at), ts(&row.updated_at))
                        )
                        .is_none(),
                    "test fixture contains duplicate branch refs"
                );
                continue;
            }
            rows_by_branch
                .entry(row.branch_id.clone())
                .or_default()
                .push(row);
        }

        // A branch ref selects a fresh hot-state generation. Its tracked
        // portion is reconstructed from the immutable root and its untracked
        // portion is materialized into the same snapshot; untracked rows have
        // no changelog record.
        for (branch_id, (head_commit_id, created_at, updated_at)) in branch_refs {
            let branch_rows = rows_by_branch.remove(&branch_id).unwrap_or_default();
            let head_commit_id_text = head_commit_id.to_string();
            let mut tracked_reader = TrackedStateContext::new().reader(&read);
            let parent_rows = if tracked_reader
                .has_durable_commit_root(&head_commit_id_text)
                .await
                .expect("test branch root should inspect")
            {
                tracked_reader
                    .scan_batch_at_commit(
                        &head_commit_id_text,
                        &TrackedStateScanRequest {
                            filter: TrackedStateFilter {
                                include_tombstones: true,
                                ..Default::default()
                            },
                            read_columns: TrackedStateReadColumns::default(),
                            limit: None,
                        },
                    )
                    .await
                    .expect("test branch root should load")
                    .into_rows()
            } else {
                Vec::new()
            };
            let snapshots = branch_rows
                .iter()
                .map(|row| {
                    row.snapshot_content.as_deref().map(|snapshot| {
                        let value = serde_json::from_str(snapshot)
                            .expect("test current-state snapshot should parse");
                        WasmTypedRow::from_builtin_json(&row.schema_key, &row.row_pk, &value)
                            .or_else(|_| {
                                WasmTypedRow::from_test_json_unchecked(&row.row_pk, &value)
                            })
                            .expect("test current-state snapshot should type")
                            .durable_payload()
                            .expect("test current-state snapshot should encode")
                            .to_vec()
                    })
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map(|metadata| {
                        lix_schema::Jsonb::from_value(
                            serde_json::from_str(metadata).expect("test metadata should parse"),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let deltas = branch_rows
                .iter()
                .zip(snapshots.iter())
                .zip(metadata.iter())
                .map(|((row, snapshot), metadata)| CurrentStateDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    row_pk: &row.row_pk,
                    change_id: Some(ChangeId::for_test_label("live-state-untracked-store")),
                    commit_id: None,
                    author_id: crate::ANONYMOUS_ACCOUNT_ID,
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_deref(),
                    metadata: metadata.as_ref(),
                    columnar_base_coordinate: None,
                })
                .collect::<Vec<_>>();
            let schema_keys = parent_rows
                .iter()
                .map(|row| row.schema_key.clone())
                .chain(branch_rows.iter().map(|row| row.schema_key.clone()))
                .collect::<Vec<_>>();
            let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
            let generation = TrackedHeadContext::new()
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    &branch_id,
                    None,
                    head_commit_id,
                    &deltas,
                    &std::collections::BTreeSet::new(),
                    Some(parent_rows),
                    None,
                    &mut working_diff_coverage,
                )
                .await
                .expect("test current-state generation should stage");
            let mut control = BranchHeadControl {
                head_commit_id,
                tracked_generation: generation,
                current_state_revision: 0,
                schema_presence_bloom: [0; 4],
                working_diff_checkpoint_commit_id: None,
                created_at,
                updated_at,
                ref_change_id: ChangeId::for_test_label(&format!("test-branch-ref-{branch_id}")),
                author_id: BranchHeadControl::author_id_bytes(crate::ANONYMOUS_ACCOUNT_ID)
                    .expect("anonymous account ID is canonical"),
            };
            control.note_schemas(schema_keys.iter().map(String::as_str));
            crate::branch::stage_branch_head_control(&mut writes, &branch_id, control)
                .expect("test branch-head control should stage");
        }

        // A pure untracked write mutates the active generation in place and
        // publishes a distinct control revision so concurrent writers cannot
        // satisfy the same stale control precondition.
        for (branch_id, branch_rows) in rows_by_branch {
            let control = BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .expect("test branch control should load")
                .expect("untracked fixture needs an existing branch control");
            let snapshots = branch_rows
                .iter()
                .map(|row| {
                    row.snapshot_content.as_deref().map(|snapshot| {
                        let value = serde_json::from_str(snapshot)
                            .expect("test current-state snapshot should parse");
                        WasmTypedRow::from_builtin_json(&row.schema_key, &row.row_pk, &value)
                            .or_else(|_| {
                                WasmTypedRow::from_test_json_unchecked(&row.row_pk, &value)
                            })
                            .expect("test current-state snapshot should type")
                            .durable_payload()
                            .expect("test current-state snapshot should encode")
                            .to_vec()
                    })
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map(|metadata| {
                        lix_schema::Jsonb::from_value(
                            serde_json::from_str(metadata).expect("test metadata should parse"),
                        )
                    })
                })
                .collect::<Vec<_>>();
            let deltas = branch_rows
                .iter()
                .zip(snapshots.iter())
                .zip(metadata.iter())
                .map(|((row, snapshot), metadata)| CurrentStateDeltaRef {
                    schema_key: &row.schema_key,
                    file_id: row.file_id.as_deref(),
                    row_pk: &row.row_pk,
                    change_id: Some(ChangeId::for_test_label("live-state-untracked-store-alt")),
                    commit_id: None,
                    author_id: crate::ANONYMOUS_ACCOUNT_ID,
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_deref(),
                    metadata: metadata.as_ref(),
                    columnar_base_coordinate: None,
                })
                .collect::<Vec<_>>();
            let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
            TrackedHeadContext::new()
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    &branch_id,
                    Some(control.tracked_generation),
                    control.head_commit_id,
                    &deltas,
                    &std::collections::BTreeSet::new(),
                    None,
                    None,
                    &mut working_diff_coverage,
                )
                .await
                .expect("test untracked current state should stage");
            crate::branch::stage_branch_head_control(
                &mut writes,
                &branch_id,
                control
                    .next_current_state_revision()
                    .expect("test branch control revision should advance"),
            )
            .expect("test untracked control should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("current rows should commit");
    }

    async fn write_empty_commits_to_store(
        storage: &StorageAdapter,
        read: &impl StorageAdapterRead,
        commit_ids: &[&str],
    ) {
        let mut writes = storage.new_write_set();
        let mut append = ChangelogAppend::default();
        let mut records = std::collections::BTreeMap::new();
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            let record = crate::changelog::CommitRecord {
                is_checkpoint: false,
                first_parent_checkpoint_summary: None,
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                base_commit_id: None,
                commit_id: CommitId::for_test_label(&commit_id_text),
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: CommitId::for_test_label(&commit_id_text),
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: ts("1970-01-01T00:00:00.000Z"),
            };
            records.insert(record.commit_id, record.clone());
            append.commits.push(record);
        }
        let mut changelog_read = read;
        let mut writer = ChangelogContext::new().writer(&mut changelog_read, &mut writes);
        crate::changelog::ChangelogWriter::stage_append(&mut writer, append)
            .await
            .expect("empty changelog commits should stage");
        drop(writer);
        for commit_id in commit_ids {
            let commit_id_text = CommitId::for_test_label(commit_id).to_string();
            let typed_commit_id = CommitId::for_test_label(commit_id);
            let tracked_state = TrackedStateContext::new();
            let mut root_writer = tracked_state.writer(read, &mut writes);
            root_writer
                .stage_commit_root(&commit_id_text, None, [])
                .await
                .expect("empty tracked roots should stage");
            let snapshot_root = root_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == typed_commit_id)
                .cloned()
                .expect("empty tracked snapshot should stage");
            drop(root_writer);
            let record = records
                .get(&typed_commit_id)
                .expect("empty commit record should exist");
            stage_commit_state_manifest(
                &mut writes,
                &CommitStateManifest {
                    incorporation: crate::tracked_state::CommitStateIncorporation::None,
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: Default::default(),
                    touched_scope_filter: Default::default(),
                    global_scope: false,
                    current_state_scoped_ranges: None,
                    row_pk_index_root_id: None,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )
            .expect("empty commit-state authority should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty commits should commit");
    }

    #[tokio::test]
    async fn commit_point_scan_preserves_typed_identity_and_missing_semantics() {
        let storage = StorageAdapter::new(Memory::new());
        let existing = CommitId::for_test_label("commit-point-existing");
        let missing = CommitId::for_test_label("commit-point-missing");
        let setup_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("setup read should open");
        write_empty_commits_to_store(&storage, &setup_read, &[&existing.to_string()]).await;
        drop(setup_read);

        let scope = HotStateScanScope {
            storage_branch_ids: Vec::new(),
            projection_branch_ids: vec!["test-branch".to_string()],
            branch_heads: BranchHeads::default(),
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("point scan read should open");
        let typed_request = finite_pk_scan_request(
            "test-branch",
            COMMIT_SCHEMA_KEY,
            vec![
                RowPk::uuid_from_bytes(*existing.as_uuid().as_bytes()),
                RowPk::uuid_from_bytes(*existing.as_uuid().as_bytes()),
                RowPk::uuid_from_bytes(*missing.as_uuid().as_bytes()),
            ],
        );
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &typed_request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(false),
        )
        .await
        .expect("typed point scan should succeed");
        assert_eq!(rows.len(), 1, "duplicates and missing keys must flatten");
        assert_eq!(rows[0].row_pk, typed_request.filter.row_pks[0]);
        assert_eq!(rows[0].commit_id, Some(existing));

        let string_request = finite_pk_scan_request(
            "test-branch",
            COMMIT_SCHEMA_KEY,
            vec![RowPk::single(existing.to_string())],
        );
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &string_request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(false),
        )
        .await
        .expect("string-typed point scan should succeed");
        assert!(
            rows.is_empty(),
            "a string component must not match the UUID primary key"
        );
    }

    #[tokio::test]
    async fn derived_scan_honors_proven_empty_row_filter() {
        let storage = StorageAdapter::new(Memory::new());
        let existing = CommitId::for_test_label("derived-empty-filter-existing");
        let setup_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("setup read should open");
        write_empty_commits_to_store(&storage, &setup_read, &[&existing.to_string()]).await;
        drop(setup_read);

        let scope = HotStateScanScope {
            storage_branch_ids: Vec::new(),
            projection_branch_ids: vec!["test-branch".to_string()],
            branch_heads: BranchHeads::default(),
        };
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                rows: HotStateRowFilter::None,
                schema_keys: vec![COMMIT_SCHEMA_KEY.to_string()],
                branch_ids: vec!["test-branch".to_string()],
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("empty-filter scan read should open");
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(false),
        )
        .await
        .expect("proven-empty derived scan should succeed");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn derived_provider_point_access_preserves_branch_ref_uuid_identity() {
        let storage = StorageAdapter::new(Memory::new());
        let branch_id = "01920000-0000-7000-8000-0000000000d1";
        stage_direct_tracked_head_rows(
            &storage,
            branch_id,
            CommitId::for_test_label("derived-branch-ref-head"),
            &[],
        )
        .await;
        let scope = HotStateScanScope {
            storage_branch_ids: vec![GLOBAL_BRANCH_ID.to_string(), branch_id.to_string()],
            projection_branch_ids: vec![branch_id.to_string()],
            branch_heads: BranchHeads::default(),
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("branch-ref point read should open");
        let typed_request = finite_pk_scan_request(
            branch_id,
            BRANCH_REF_SCHEMA_KEY,
            vec![RowPk::uuid_from_canonical(branch_id).expect("valid branch UUID")],
        );
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &typed_request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(true),
        )
        .await
        .expect("typed branch-ref point scan should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_pk, typed_request.filter.row_pks[0]);

        let string_request = finite_pk_scan_request(
            branch_id,
            BRANCH_REF_SCHEMA_KEY,
            vec![RowPk::single(branch_id)],
        );
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &string_request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(true),
        )
        .await
        .expect("string branch-ref point scan should succeed");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn derived_commit_point_exposes_ordered_parent_ids() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("mixed derived setup read should open");
        let parent = CommitId::for_test_label("mixed-derived-parent");
        let child = CommitId::for_test_label("mixed-derived-child");
        let mut writes = storage.new_write_set();
        crate::init::stage_repository_protocol(&mut writes);
        let mut append = ChangelogAppend::default();
        for (commit_id, generation, parents) in [(parent, 0, Vec::new()), (child, 1, vec![parent])]
        {
            let (first_parent_jump_commit_id, first_parent_jump_span) = parents
                .first()
                .copied()
                .map_or((commit_id, 0), |parent| (parent, 1));
            append.commits.push(crate::changelog::CommitRecord {
                is_checkpoint: false,
                first_parent_checkpoint_summary: None,
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                base_commit_id: None,
                commit_id,
                generation,
                parent_commit_ids: parents,
                first_parent_jump_commit_id,
                first_parent_jump_span,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: ts("1970-01-01T00:00:00.000Z"),
            });
        }
        let commit_records = append.commits.clone();
        let mut changelog_read = &read;
        let mut writer = ChangelogContext::new().writer(&mut changelog_read, &mut writes);
        crate::changelog::ChangelogWriter::stage_append(&mut writer, append)
            .await
            .expect("mixed derived commits should stage");
        drop(writer);
        for record in commit_records {
            stage_commit_state_manifest(
                &mut writes,
                &CommitStateManifest {
                    incorporation: crate::tracked_state::CommitStateIncorporation::None,
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt {
                        depth: u16::try_from(record.generation + 1)
                            .expect("fixture generation should fit replay depth"),
                        rows: 0,
                        bytes: 0,
                    },
                    mutations: Default::default(),
                    touched_scope_filter: Default::default(),
                    global_scope: false,
                    current_state_scoped_ranges: None,
                    row_pk_index_root_id: None,
                    snapshot_root: None,
                },
            )
            .expect("mixed derived commit authority should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("mixed derived commits should commit");
        drop(read);

        let branch_id = "test-branch";
        let scope = HotStateScanScope {
            storage_branch_ids: Vec::new(),
            projection_branch_ids: vec![branch_id.to_string()],
            branch_heads: BranchHeads::default(),
        };
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec!["lix_commit".to_string()],
                row_pks: vec![RowPk::uuid_from_bytes(*child.as_uuid().as_bytes())],
                branch_ids: vec![branch_id.to_string()],
                ..HotStateFilter::default()
            },
            ..HotStateScanRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("mixed derived scan read should open");
        let rows = scan_derived_rows(
            &read,
            &CommitGraphContext::new(),
            &request,
            &scope.projection_branch_ids,
            &scope.storage_branch_ids,
            Some(false),
        )
        .await
        .expect("mixed derived scan should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_key, "lix_commit");
        let snapshot = serde_json::from_str::<serde_json::Value>(
            rows[0]
                .snapshot_content
                .as_ref()
                .expect("derived commit snapshot should exist"),
        )
        .expect("derived commit snapshot should decode");
        assert_eq!(snapshot["parent_commit_ids"], serde_json::json!([parent]));
    }

    async fn stage_materialized_live_rows(
        store: &impl StorageAdapterRead,
        writes: &mut StorageWriteSet,
        rows: &[MaterializedHotStateRow],
    ) -> Result<(), LixError> {
        let mut tracked_rows_by_commit = std::collections::BTreeMap::<
            String,
            Vec<(
                ChangeRecord,
                crate::common::LixTimestamp,
                crate::common::LixTimestamp,
            )>,
        >::new();
        let mut parent_by_commit = std::collections::BTreeMap::<String, Option<String>>::new();

        for row in rows {
            if row.untracked {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "test tracked-row helper does not accept untracked rows",
                ));
            }
            let materialized = MaterializedTrackedStateRow::try_from(row)?;
            let commit_id = row.commit_id.clone().ok_or_else(|| {
                LixError::new("LIX_ERROR_UNKNOWN", "test tracked row missing commit_id")
            })?;
            let commit_id_text = commit_id.to_string();
            if row.schema_key == COMMIT_SCHEMA_KEY {
                parent_by_commit.insert(
                    commit_id_text.clone(),
                    parent_commit_id_from_test_commit_row(row)?,
                );
            }
            if row.schema_key != COMMIT_SCHEMA_KEY {
                let change = crate::test_support::tracked_change_from_materialized(&materialized)?;
                tracked_rows_by_commit
                    .entry(commit_id_text)
                    .or_default()
                    .push((
                        change,
                        ts(&materialized.created_at),
                        ts(&materialized.updated_at),
                    ));
            }
        }

        let mut generations = std::collections::BTreeMap::<String, u64>::new();
        for (commit_id, rows) in tracked_rows_by_commit {
            let parent_commit_id = parent_by_commit.remove(&commit_id).flatten();
            let parent_ids = parent_commit_id
                .as_ref()
                .map(|parent| vec![parent.clone()])
                .unwrap_or_default();
            let commit_created_at = rows
                .first()
                .map(|(change, _, _)| change.created_at)
                .unwrap_or_else(|| ts("1970-01-01T00:00:00.000Z"));
            let generation = if let Some(parent) = parent_ids.first() {
                let parent_generation = if let Some(generation) = generations.get(parent) {
                    *generation
                } else {
                    let typed_parent = CommitId::for_test_label(parent);
                    let mut changelog_read = store;
                    ChangelogContext::new()
                        .reader(&mut changelog_read)
                        .load_commits(CommitLoadRequest {
                            commit_ids: &[typed_parent],
                        })
                        .await?
                        .into_iter()
                        .next()
                        .and_then(|(_, value)| value)
                        .ok_or_else(|| {
                            LixError::unknown("test changelog parent commit is missing")
                        })?
                        .generation
                };
                parent_generation
                    .checked_add(1)
                    .ok_or_else(|| LixError::unknown("test commit generation exceeds u64"))?
            } else {
                0
            };
            let typed_parent_ids = parent_ids
                .iter()
                .map(|id| CommitId::for_test_label(id))
                .collect::<Vec<_>>();
            let record = crate::changelog::CommitRecord {
                is_checkpoint: false,
                first_parent_checkpoint_summary: None,
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
                base_commit_id: None,
                commit_id: CommitId::for_test_label(&commit_id),
                generation,
                parent_commit_ids: typed_parent_ids,
                first_parent_jump_commit_id: CommitId::for_test_label(&commit_id),
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: commit_created_at,
            };
            let mut append = ChangelogAppend::default();
            append.commits.push(record.clone());
            let mut changelog_read = store;
            let mut writer = ChangelogContext::new().writer(&mut changelog_read, writes);
            crate::changelog::ChangelogWriter::stage_append(&mut writer, append).await?;
            drop(writer);
            generations.insert(commit_id.clone(), generation);
            let typed_commit_id = CommitId::for_test_label(&commit_id);
            let root_deltas = rows
                .iter()
                .map(|(change, created_at, updated_at)| TrackedStateDeltaRef {
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    row_pk: &change.row_pk,
                    change_id: change.change_id,
                    commit_id: typed_commit_id,
                    author_id: crate::ANONYMOUS_ACCOUNT_ID,
                    deleted: change.snapshot.is_none(),
                    created_at: *created_at,
                    updated_at: *updated_at,
                    semantic_fingerprint: None,
                })
                .collect::<Vec<_>>();
            let commit_deltas = rows
                .iter()
                .zip(&root_deltas)
                .map(|((change, _, _), delta)| TrackedStateCommitDeltaRef {
                    delta: *delta,
                    metadata: change.metadata.as_ref(),
                    snapshot: change.snapshot.as_deref(),
                    origin_key: change.origin_key.as_deref(),
                    base_coordinate: None,
                    authored: true,
                })
                .collect::<Vec<_>>();
            let staged_delta = stage_commit_deltas_for_commit_state(writes, &commit_deltas)?;
            let mutation_inventory = staged_delta.mutation_inventory().clone();
            let tracked_state = TrackedStateContext::new();
            let mut root_writer = tracked_state.writer(&*store, writes);
            root_writer
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), root_deltas)
                .await?;
            let snapshot_root = root_writer
                .staged_commit_roots()
                .find(|root| root.commit_id == typed_commit_id)
                .cloned()
                .ok_or_else(|| LixError::unknown("test materialization did not stage a root"))?;
            drop(root_writer);
            stage_commit_state_manifest(
                writes,
                &CommitStateManifest {
                    incorporation: crate::tracked_state::CommitStateIncorporation::None,
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: mutation_inventory,
                    touched_scope_filter: Default::default(),
                    global_scope: false,
                    current_state_scoped_ranges: None,
                    row_pk_index_root_id: None,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )?;
        }

        Ok(())
    }

    fn parent_commit_id_from_test_commit_row(
        row: &MaterializedHotStateRow,
    ) -> Result<Option<String>, LixError> {
        let Some(metadata) = row.metadata.as_deref() else {
            return Ok(None);
        };
        let metadata = serde_json::from_str::<serde_json::Value>(metadata).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("test commit row has invalid metadata: {error}"),
            )
        })?;
        Ok(metadata
            .get("test_parents")
            .and_then(serde_json::Value::as_array)
            .and_then(|parents| parents.first())
            .and_then(serde_json::Value::as_str)
            .map(str::to_string))
    }

    #[tokio::test]
    async fn hot_state_serves_untracked_member_from_current_state() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        // Keep the tracked commit fixture separate from the untracked member
        // under test: a single identity cannot change retention class.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.row_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &[tracked_row])
            .await
            .expect("tracked row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked row should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(
            &hot_state,
            &storage,
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            false,
        )
        .await
        .expect("scan should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
        assert!(rows[0].untracked);
        assert!(
            rows[0].change_id.is_some_and(|id| !id.as_uuid().is_nil()),
            "untracked rows carry a real change id"
        );

        let loaded = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                row_pk: RowPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("load should succeed")
            .expect("current row should be visible");
        assert!(loaded.untracked);
        assert!(
            loaded.change_id.is_some_and(|id| !id.as_uuid().is_nil()),
            "untracked rows carry a real change id"
        );
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked-value\"}")
        );
    }

    #[tokio::test]
    async fn exact_batch_preserves_duplicate_and_missing_slots_for_current_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = StorageWriteSet::new();
        // The tracked fixture establishes the branch head; use a distinct
        // identity so the selected untracked row is not a retention conflict.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.row_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &[tracked_row])
            .await
            .expect("tracked row should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked row should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-tracked"),
                untracked_row("untracked-value"),
            ],
        )
        .await;

        let selected = HotStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
            row_pk: identity("selected-tab"),
            file_id: None,
        };
        let rows = hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should reopen"),
            )
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![
                    selected.clone(),
                    selected,
                    HotStateExactRowRequest {
                        schema_key: "lix_key_value".to_string(),
                        branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                        row_pk: identity("missing"),
                        file_id: None,
                    },
                ],
                projection: HotStateProjection {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..Default::default()
            })
            .await
            .expect("exact batch should load")
            .into_rows();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], rows[1]);
        assert_eq!(
            rows[0]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"untracked-value\"}")
        );
        assert!(rows[0].as_ref().is_some_and(|row| row.untracked));
        assert_eq!(rows[2], None);
    }

    #[tokio::test]
    async fn tracked_row_is_visible_from_commit_root() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(
                    &read,
                    &mut writes,
                    &[tracked_row_with_commit(
                        "tracked-value",
                        Some("change-tracked"),
                        "commit-tracked",
                    )],
                )
                .await
                .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "commit-tracked",
            )],
        )
        .await;

        let loaded = load_selected_tab(&hot_state, &storage)
            .await
            .expect("load should succeed")
            .expect("tracked row should be visible");
        assert!(!loaded.untracked);
        assert_eq!(loaded.change_id, Some(change_id("change-tracked")));
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"tracked-value\"}")
        );
    }

    #[tokio::test]
    async fn load_row_falls_back_to_global_tracked_row_for_requested_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row(
                    "01920000-0000-7000-8000-0000000000a1",
                    "commit-01920000-0000-7000-8000-0000000000a1",
                ),
            ],
        )
        .await;
        write_empty_commits_to_store(
            &storage,
            &read,
            &["commit-01920000-0000-7000-8000-0000000000a1"],
        )
        .await;

        let loaded =
            load_selected_tab_at(&hot_state, &storage, "01920000-0000-7000-8000-0000000000a1")
                .await
                .expect("load should succeed")
                .expect("global row should be visible for requested branch");

        assert_eq!(
            loaded.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(loaded.global);
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_sees_global_row_by_reading_global_root_separately() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let hot_state = HotStateContext::new(tracked_state.clone(), CommitGraphContext::new());

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("global tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;
        write_empty_commits_to_store(&storage, &read, &["commit-main"]).await;

        let loaded = load_selected_tab_at(&hot_state, &storage, "main")
            .await
            .expect("load should succeed")
            .expect("global row should be projected into main");
        assert_eq!(loaded.branch_id.as_ref(), "main");
        assert!(loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-main").await;
        assert!(
            main_root_rows.is_empty(),
            "derived commit rows must not be stored in tracked roots"
        );
    }

    #[tokio::test]
    async fn load_row_prefers_requested_branch_over_global() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "01920000-0000-7000-8000-0000000000a1",
                    "branch-tracked",
                    Some("change-branch"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let loaded =
            load_selected_tab_at(&hot_state, &storage, "01920000-0000-7000-8000-0000000000a1")
                .await
                .expect("load should succeed")
                .expect("branch row should be visible");

        assert_eq!(
            loaded.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(!loaded.untracked);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn main_override_hides_global_row() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;

        let loaded = load_selected_tab_at(&hot_state, &storage, "main")
            .await
            .expect("load should succeed")
            .expect("main row should be visible");

        assert_eq!(loaded.branch_id.as_ref(), "main");
        assert!(!loaded.global);
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_resolves_requested_branch_over_global() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "01920000-0000-7000-8000-0000000000a1",
                    "branch-tracked",
                    Some("change-branch"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let rows = scan_selected_tab_at(
            &hot_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"branch-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_projects_global_row_into_requested_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row(
                    "01920000-0000-7000-8000-0000000000a1",
                    "commit-01920000-0000-7000-8000-0000000000a1",
                ),
            ],
        )
        .await;
        write_empty_commits_to_store(
            &storage,
            &read,
            &["commit-01920000-0000-7000-8000-0000000000a1"],
        )
        .await;

        let rows = scan_selected_tab_at(
            &hot_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );
    }

    #[tokio::test]
    async fn scan_rows_does_not_project_global_rows_into_missing_branch() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [tracked_row_with_commit(
                "global-tracked",
                Some("change-global"),
                "commit-global",
            )];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked row should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "commit-global",
            )],
        )
        .await;

        let rows = scan_selected_tab_at(&hot_state, &storage, "missing-branch", false)
            .await
            .expect("scan should succeed");

        assert_eq!(
            rows.len(),
            0,
            "global rows must not be projected into a missing branch scope"
        );
    }

    #[tokio::test]
    async fn winning_tombstone_hides_row_unless_tombstones_are_included() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "01920000-0000-7000-8000-0000000000a1",
                    Some("change-tombstone"),
                    "commit-branch",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let hidden = scan_selected_tab_at(
            &hot_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            false,
        )
        .await
        .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let with_tombstone = scan_selected_tab_at(
            &hot_state,
            &storage,
            "01920000-0000-7000-8000-0000000000a1",
            true,
        )
        .await
        .expect("scan should succeed");
        assert_eq!(with_tombstone.len(), 1);
        assert_eq!(
            with_tombstone[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert_eq!(with_tombstone[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn main_tombstone_hides_global_row() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tombstone_tracked_row_at_with_commit(
                    "main",
                    Some("change-main-tombstone"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("main", "commit-main"),
            ],
        )
        .await;

        let hidden = scan_selected_tab_at(&hot_state, &storage, "main", false)
            .await
            .expect("scan should succeed");
        assert_eq!(hidden.len(), 0);

        let tombstones = scan_selected_tab_at(&hot_state, &storage, "main", true)
            .await
            .expect("scan should succeed");
        assert_eq!(tombstones.len(), 1);
        assert_eq!(tombstones[0].branch_id.as_ref(), "main");
        assert!(!tombstones[0].global);
        assert_eq!(tombstones[0].snapshot_content, None);
    }

    #[tokio::test]
    async fn exact_batch_resolves_branch_global_tombstone_projection_and_correlated_keys() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let mut global_fallback =
            tracked_row_with_commit("global-fallback", Some("change-fallback"), "commit-global");
        global_fallback.row_pk = identity("fallback");
        global_fallback.file_id = Some("fallback".to_string());
        global_fallback.metadata = Some("{\"source\":\"global\"}".into());
        let mut global_overridden =
            tracked_row_with_commit("global-old", Some("change-global-old"), "commit-global");
        global_overridden.row_pk = identity("overridden");
        global_overridden.file_id = Some("overridden".to_string());
        let mut branch_override = tracked_row_at_with_commit(
            "01920000-0000-7000-8000-0000000000a1",
            "branch-new",
            Some("change-branch-new"),
            "commit-branch",
        );
        branch_override.row_pk = identity("overridden");
        branch_override.file_id = Some("overridden".to_string());
        let mut global_hidden =
            tracked_row_with_commit("global-hidden", Some("change-hidden"), "commit-global");
        global_hidden.row_pk = identity("hidden");
        global_hidden.file_id = Some("hidden".to_string());
        let mut branch_tombstone = tombstone_tracked_row_at_with_commit(
            "01920000-0000-7000-8000-0000000000a1",
            Some("change-tombstone"),
            "commit-branch",
        );
        branch_tombstone.row_pk = identity("hidden");
        branch_tombstone.file_id = Some("hidden".to_string());
        let mut malformed_cross_pair =
            tracked_row_with_commit("cross-pair", Some("change-cross"), "commit-global");
        malformed_cross_pair.row_pk = identity("row-a");
        malformed_cross_pair.file_id = Some("01920000-0000-7000-8000-0000000000b2".to_string());

        let rows = [
            global_fallback,
            global_overridden,
            global_hidden,
            malformed_cross_pair,
            branch_override,
            branch_tombstone,
        ];
        let mut writes = StorageWriteSet::new();
        stage_materialized_live_rows(&read, &mut writes, &rows)
            .await
            .expect("tracked rows should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("tracked rows should commit");
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[
                branch_ref_row("ffffffff-ffff-7fff-bfff-ffffffffffff", "commit-global"),
                branch_ref_row("01920000-0000-7000-8000-0000000000a1", "commit-branch"),
            ],
        )
        .await;

        let exact = |row: &str, file_id: &str| HotStateExactRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
            row_pk: identity(row),
            file_id: Some(file_id.to_string()),
        };
        let reader = hot_state.reader(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should reopen"),
        );
        let loaded = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![
                    exact("fallback", "fallback"),
                    exact("overridden", "overridden"),
                    exact("hidden", "hidden"),
                    exact("row-a", "01920000-0000-7000-8000-0000000000a2"),
                    exact("row-b", "01920000-0000-7000-8000-0000000000b2"),
                    exact("row-a", "01920000-0000-7000-8000-0000000000b2"),
                    exact("missing", "missing"),
                ],
                projection: HotStateProjection {
                    columns: vec!["snapshot_content".to_string()],
                },
                ..Default::default()
            })
            .await
            .expect("exact tracked batch should load")
            .into_rows();

        let fallback = loaded[0].as_ref().expect("global fallback should load");
        assert!(fallback.global);
        assert_eq!(
            fallback.branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert_eq!(
            fallback.snapshot_content.as_deref(),
            Some("{\"value\":\"global-fallback\"}")
        );
        assert_eq!(fallback.metadata, None, "projection should omit metadata");
        let overridden = loaded[1].as_ref().expect("branch override should load");
        assert!(!overridden.global);
        assert_eq!(
            overridden.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-new\"}")
        );
        assert_eq!(loaded[2], None, "branch tombstone must hide global row");
        assert_eq!(loaded[3], None, "row A/file A must not cross-match");
        assert_eq!(loaded[4], None, "row B/file B must not cross-match");
        assert_eq!(
            loaded[5]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref()),
            Some("{\"value\":\"cross-pair\"}")
        );
        assert_eq!(loaded[6], None);

        let tombstone = reader
            .load_exact_batch(&HotStateExactBatchRequest {
                rows: vec![exact("hidden", "hidden")],
                include_tombstones: true,
                ..Default::default()
            })
            .await
            .expect("exact tombstone read should load")
            .into_rows()
            .pop()
            .flatten()
            .expect("tombstone should be returned when requested");
        assert!(tombstone.deleted);
        assert!(!tombstone.global);
    }

    #[tokio::test]
    async fn writer_allows_commit_fact_to_share_the_touched_branch_commit_id() {
        let storage = StorageAdapter::new(Memory::new());
        let hot_state = hot_state_context();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "01920000-0000-7000-8000-0000000000a1",
                    "branch-row",
                    Some("change-branch"),
                    "commit-branch",
                ),
                commit_hot_state_row("commit-branch"),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("commit facts are changelog projections, not root-local rows");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
        write_untracked_rows_to_store(
            &storage,
            &read,
            &[branch_ref_row(
                "01920000-0000-7000-8000-0000000000a1",
                "commit-branch",
            )],
        )
        .await;

        let loaded =
            load_selected_tab_at(&hot_state, &storage, "01920000-0000-7000-8000-0000000000a1")
                .await
                .expect("load should succeed")
                .expect("branch row should be visible");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"branch-row\"}")
        );
    }

    #[tokio::test]
    async fn writer_uses_first_parent_as_merge_root_base() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        write_empty_commits_to_store(&storage, &read, &["parent-left"]).await;
        let mut writes = StorageWriteSet::new();
        TrackedStateContext::new()
            .writer(&read, &mut writes)
            .stage_commit_root("parent-left", None, [])
            .await
            .expect("first parent tracked root should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first parent tracked root should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_at_with_commit(
                    "01920000-0000-7000-8000-0000000000a1",
                    "branch-row",
                    Some("change-branch"),
                    "commit-merge",
                ),
                commit_hot_state_row_with_parents("commit-merge", &["parent-left", "parent-right"]),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("merge commit should use first parent as tracked-root base");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }
    }

    #[tokio::test]
    async fn non_global_root_does_not_store_global_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        {
            let rows = [
                tracked_row_with_commit("global-tracked", Some("change-global"), "commit-global"),
                tracked_row_at_with_commit(
                    "main",
                    "main-tracked",
                    Some("change-main"),
                    "commit-main",
                ),
            ];
            let mut writes = StorageWriteSet::new();
            {
                stage_materialized_live_rows(&read, &mut writes, &rows)
                    .await
                    .expect("tracked rows should stage");
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("writes should commit");
        }

        let global_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-global").await;
        assert_eq!(global_root_rows.len(), 1);
        let Some(global_row) = global_root_rows
            .iter()
            .find(|row| row.schema_key == "lix_key_value")
        else {
            panic!("global root should contain the explicit global tracked row");
        };
        assert_eq!(
            global_row.snapshot_content.as_deref(),
            Some("{\"value\":\"global-tracked\"}")
        );

        let main_root_rows = scan_tracked_root(&tracked_state, &storage, "commit-main").await;
        assert_eq!(main_root_rows.len(), 1);
        let Some(main_row) = main_root_rows
            .iter()
            .find(|row| row.schema_key == "lix_key_value")
        else {
            panic!("main root should contain the explicit main tracked row");
        };
        assert_eq!(
            main_row.snapshot_content.as_deref(),
            Some("{\"value\":\"main-tracked\"}")
        );
    }

    async fn load_selected_tab(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
    ) -> Result<Option<MaterializedHotStateRow>, LixError> {
        hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
                row_pk: RowPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn load_selected_tab_at(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
    ) -> Result<Option<MaterializedHotStateRow>, LixError> {
        hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: branch_id.to_string(),
                row_pk: RowPk::single("selected-tab"),
                file_id: NullableKeyFilter::Null,
            })
            .await
    }

    async fn scan_selected_tab_at(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        include_tombstones: bool,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        hot_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    row_pks: vec![RowPk::single("selected-tab")],
                    branch_ids: vec![branch_id.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones,
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await
            .map(MaterializedHotStateBatch::into_rows)
    }

    async fn scan_tracked_root(
        tracked_state: &TrackedStateContext,
        storage: &StorageAdapter,
        commit_id: &str,
    ) -> Vec<MaterializedTrackedStateRow> {
        tracked_state
            .reader(
                storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("read should open"),
            )
            .scan_batch_at_commit(
                commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        include_tombstones: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("tracked root should scan")
            .into_rows()
    }

    fn tracked_row_with_commit(
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedHotStateRow {
        tracked_row_at_with_commit(
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            value,
            change_id,
            commit_id,
        )
    }

    fn tracked_row_at_with_commit(
        branch_id: &str,
        value: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedHotStateRow {
        let commit_id = CommitId::for_test_label(commit_id);
        MaterializedHotStateRow {
            row_pk: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            global: branch_id == "ffffffff-ffff-7fff-bfff-ffffffffffff",
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: branch_id.into(),
        }
    }

    fn tombstone_tracked_row_at_with_commit(
        branch_id: &str,
        change_id: Option<&str>,
        commit_id: &str,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            snapshot_content: None,
            deleted: true,
            ..tracked_row_at_with_commit(branch_id, "ignored", change_id, commit_id)
        }
    }

    fn untracked_row(value: &str) -> MaterializedUntrackedStateRow {
        untracked_row_at("ffffffff-ffff-7fff-bfff-ffffffffffff", value)
    }

    fn untracked_row_at(branch_id: &str, value: &str) -> MaterializedUntrackedStateRow {
        MaterializedUntrackedStateRow {
            row_pk: identity("selected-tab"),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branch_id: branch_id.to_string(),
        }
    }

    fn branch_ref_row(branch_id: &str, commit_id: &str) -> MaterializedUntrackedStateRow {
        let commit_id = CommitId::for_test_label(commit_id).to_string();
        MaterializedUntrackedStateRow {
            row_pk: identity(branch_id),
            schema_key: "lix_branch_ref".to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&json!({
                    "id": branch_id,
                    "commit_id": commit_id,
                }))
                .expect("branch ref should serialize"),
            ),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string(),
        }
    }

    fn commit_hot_state_row(commit_id: &str) -> MaterializedHotStateRow {
        commit_hot_state_row_with_parents(commit_id, &[])
    }

    fn commit_hot_state_row_with_parents(
        commit_id: &str,
        parent_ids: &[&str],
    ) -> MaterializedHotStateRow {
        let commit_id_text = CommitId::for_test_label(commit_id).to_string();
        let parent_id_texts = parent_ids
            .iter()
            .map(|parent| CommitId::for_test_label(parent).to_string())
            .collect::<Vec<_>>();
        let mut row = commit_hot_state_row_with_snapshot(
            &commit_id_text,
            json!({
                "id": commit_id_text,
            }),
        );
        row.metadata = Some(
            serde_json::to_string(&json!({ "test_parents": parent_id_texts }))
                .expect("test metadata should serialize")
                .into(),
        );
        row
    }

    fn commit_hot_state_row_with_snapshot(
        commit_id: &str,
        snapshot: serde_json::Value,
    ) -> MaterializedHotStateRow {
        let commit_id = CommitId::for_test_label(commit_id);
        let commit_id_text = commit_id.to_string();
        MaterializedHotStateRow {
            row_pk: identity(&commit_id_text),
            schema_key: COMMIT_SCHEMA_KEY.to_string(),
            file_id: None,
            snapshot_content: Some(
                serde_json::to_string(&snapshot)
                    .expect("commit snapshot should serialize")
                    .into(),
            ),
            metadata: None,
            deleted: false,
            created_at: ts("2026-01-01T00:00:00Z"),
            updated_at: ts("2026-01-01T00:00:00Z"),
            author_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            global: true,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_id}"))),
            commit_id: Some(commit_id),
            untracked: false,
            branch_id: "ffffffff-ffff-7fff-bfff-ffffffffffff".into(),
        }
    }

    fn identity(row_pk: &str) -> RowPk {
        RowPk::single(row_pk)
    }
}

// Keep native payload decoding behind its owner boundary so downstream storage
// adapters need not prove the entire recursive read graph at their default limit.
// StorageAdapterRead and its futures are Send on every engine target.
type ReturnedChangePreparationFuture<'a> =
    futures_util::future::BoxFuture<'a, Result<(), LixError>>;

#[derive(Debug)]
struct CurrentReadIdentity {
    branch_id: String,
    key: crate::tracked_state::TrackedStateKey,
    change_id: crate::changelog::ChangeId,
}
impl CurrentReadIdentity {
    fn from_row(row: MaterializedHotStateRowRef<'_>) -> Option<Self> {
        // Derived relations are served by their authoritative metadata owners.
        // Their identities (including ordinal-zero commit changes) are not
        // selected mutations and have no native row mutation path to prepare.
        if is_derived_schema(row.schema_key()) {
            return None;
        }
        Some(Self {
            branch_id: if row.global() {
                GLOBAL_BRANCH_ID
            } else {
                row.branch_id()
            }
            .to_owned(),
            key: crate::tracked_state::TrackedStateKey {
                schema_key: row.schema_key().to_owned(),
                file_id: row.file_id().map(str::to_owned),
                row_pk: row.row_pk().clone(),
            },
            change_id: row.change_id()?,
        })
    }
}
type PreparedReadKey = (String, CommitId, crate::changelog::ChangeId);
// Partial native GC rejects incomplete ownership, so prepared immutable inputs
// persist within this epoch. Future partial eviction MUST invalidate this cache
// before deleting any referenced input. Capacity eviction only causes rechecking.
#[derive(Default, Debug)]
struct PreparedReadRows {
    epoch: Option<String>,
    keys: std::collections::BTreeSet<PreparedReadKey>,
}

#[cfg(test)]
mod snapshot_scope_tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageKey, StorageWriteOptions};

    #[tokio::test]
    async fn scope_resolution_stays_with_its_read_and_never_falls_back_on_invalid_receipt() {
        let storage = StorageAdapter::new(crate::Memory::new());
        let key = StorageKey(Bytes::from_static(b"scope-test"));
        let space = crate::hot_state::TRACKED_WORKING_DIFF_MARKER_SPACE;
        let publish = |value: &'static [u8]| {
            let mut writes = storage.new_write_set();
            writes.put(space, key.clone(), value);
            writes
        };
        storage
            .commit_write_set(publish(b"first"), StorageWriteOptions::default())
            .await
            .unwrap();
        let source = super::super::PartialReadScopeSource::new(space, key.clone(), 16, |bytes| {
            let selected = match bytes {
                b"first" => "first",
                b"second" => "second",
                _ => {
                    return Err(LixError::new(
                        "INVALID_TEST_RECEIPT",
                        "invalid scope receipt",
                    ));
                }
            };
            let mut policy = super::super::PartialReadScopePolicy::new(selected, GLOBAL_BRANCH_ID);
            policy.set_preparation_epoch("fixed-physical-epoch");
            Ok(policy)
        });
        let hot = HotStateContext::new(TrackedStateContext::new(), CommitGraphContext::new())
            .with_partial_scope_source(source);
        let old = hot.reader(storage.begin_read(Default::default()).await.unwrap());
        old.effective_partial_scope_policy()
            .await
            .unwrap()
            .unwrap()
            .validate(&["first".into()])
            .unwrap();
        storage
            .commit_write_set(publish(b"second"), StorageWriteOptions::default())
            .await
            .unwrap();
        let new = hot.reader(storage.begin_read(Default::default()).await.unwrap());
        new.effective_partial_scope_policy()
            .await
            .unwrap()
            .unwrap()
            .validate(&["second".into()])
            .unwrap();
        assert!(
            old.effective_partial_scope_policy()
                .await
                .unwrap()
                .unwrap()
                .validate(&["second".into()])
                .is_err()
        );
        assert!(
            new.effective_partial_scope_policy()
                .await
                .unwrap()
                .unwrap()
                .validate(&["first".into()])
                .is_err()
        );
        storage
            .commit_write_set(publish(b"invalid"), StorageWriteOptions::default())
            .await
            .unwrap();
        let invalid = hot.reader(storage.begin_read(Default::default()).await.unwrap());
        assert_eq!(
            invalid
                .effective_partial_scope_policy()
                .await
                .unwrap_err()
                .code,
            "INVALID_TEST_RECEIPT"
        );
        let candidate = hot.with_partial_scope_policy("candidate", GLOBAL_BRANCH_ID);
        let candidate = candidate.reader(storage.begin_read(Default::default()).await.unwrap());
        let policy = candidate
            .effective_partial_scope_policy()
            .await
            .unwrap()
            .unwrap();
        policy.validate(&["candidate".into()]).unwrap();
        assert!(policy.preparation_epoch().is_none());
    }
}
