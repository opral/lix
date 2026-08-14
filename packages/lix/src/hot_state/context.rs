#![allow(clippy::borrow_deref_ref, clippy::clone_on_copy)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
#[cfg(test)]
use crate::branch::BRANCH_REF_SCHEMA_KEY;
use crate::branch::{BranchHeadControl, BranchHeadControlContext};
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphContext;
use crate::row_pk::RowPk;
use crate::filesystem::{
    FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, build_path_index, load_path_index_revision,
};
use crate::hot_state::tracked_head::{HotStateTransactionCache, TrackedHeadContext};
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateReadDomain, HotStateReader, HotStateRowFilter,
    HotStateRowRequest, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateBatchBuilder, MaterializedHotStateExactBatch, MaterializedHotStateRow,
    MaterializedHotStateRowRef, VisibilityBranchScope, VisibilityRequest, expanded_branch_ids,
    resolve_visible_batch,
};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns, TrackedStateScanRequest,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use std::mem::size_of;
use std::sync::Mutex as StdMutex;

use super::derived::{
    is_derived_only_request, is_derived_schema, request_may_include_derived, scan_derived_rows,
};

const BRANCH_READ_CONCURRENCY: usize = 8;
const ROW_COLUMNAR_LAYOUT_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;
const ROW_COLUMNAR_LAYOUT_CACHE_MAX_ENTRIES: usize = 16;
const TRANSACTION_BRANCH_HEAD_CONTROL_CACHE_MAX_ENTRIES: usize = 64;
type BranchHeads = std::collections::BTreeMap<String, BranchHeadControl>;

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
/// Resolving one costs a projected live batch read plus — on the expected
/// miss, because the row is absent in a repository that never enabled the
/// feature — a full `validate_exact_collection_closure` scan of the global
/// `lix_key_value` collection. Both are functions of the global branch-head
/// control's generation and current-state revision, and every write to that
/// plane republishes the control under a CAS with a bumped
/// `current_state_revision`. An unchanged control therefore proves the
/// resolved row, and the closure validated alongside it, unchanged.
///
/// Disposable cache: tagged with the exact control it was resolved under,
/// rebuilt from canonical records on any change, never an authority.
#[derive(Debug, Default)]
pub(crate) struct GlobalKeyValueRowCache {
    entries: StdMutex<Vec<(BranchHeadControl, String, Option<MaterializedHotStateRow>)>>,
}

const GLOBAL_KEY_VALUE_ROW_CACHE_MAX_ENTRIES: usize = 8;

impl GlobalKeyValueRowCache {
    pub(crate) fn get(
        &self,
        control: BranchHeadControl,
        key: &str,
    ) -> Option<Option<MaterializedHotStateRow>> {
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
        row: Option<MaterializedHotStateRow>,
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
        while resident_bytes > max_bytes || entries.len() > ROW_COLUMNAR_LAYOUT_CACHE_MAX_ENTRIES
        {
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
            overlay_capacity
                .saturating_mul(size_of::<crate::hot_state::RowColumnarOverlayRow>()),
        )
        .saturating_add(
            overlay
                .iter()
                .map(|row| {
                    row.row_pk
                        .estimated_heap_bytes()
                        .saturating_add(row.snapshot_content.as_ref().map_or(0, Bytes::len))
                })
                .sum(),
        )
}

/// Serving facade for visible live-state reads.
///
/// Normal rows are resolved from one durable hot-state projection. Each row
/// carries its own tracked|untracked retention, so readers do not route
/// through a separate retention index or merge retention candidates.
pub(crate) struct HotStateContext {
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
    row_columnar_layout_cache: std::sync::Arc<RowColumnarLayoutCache>,
    row_columnar_scan_cache:
        std::sync::Arc<std::sync::Mutex<crate::hot_state::RowColumnarShadowMaskCache>>,
    row_decoded_column_cache: crate::hot_state::RowDecodedColumnCache,
    global_key_value_rows: std::sync::Arc<GlobalKeyValueRowCache>,
    root_base_cache: std::sync::Arc<crate::hot_state::tracked_head::RootBaseBatchCache>,
}

impl HotStateContext {
    /// Engine-lifetime cache for the global untracked `lix_key_value` rows read
    /// at every transaction open, fenced by the global branch-head control.
    pub(crate) fn global_key_value_rows(&self) -> &GlobalKeyValueRowCache {
        &self.global_key_value_rows
    }

    pub(crate) fn new(
        _tracked_state: TrackedStateContext,
        commit_graph: CommitGraphContext,
    ) -> Self {
        let row_columnar_array_budget =
            std::sync::Arc::new(crate::hot_state::RowColumnarArrayBudget::default());
        Self {
            tracked_head: TrackedHeadContext::new(),
            commit_graph,
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
            row_columnar_layout_cache: std::sync::Arc::new(RowColumnarLayoutCache::default()),
            row_columnar_scan_cache: std::sync::Arc::new(std::sync::Mutex::new(
                crate::hot_state::RowColumnarShadowMaskCache::with_array_budget(
                    std::sync::Arc::clone(&row_columnar_array_budget),
                ),
            )),
            row_decoded_column_cache:
                crate::hot_state::RowDecodedColumnCache::with_array_budget(
                    row_columnar_array_budget,
                ),
            global_key_value_rows: std::sync::Arc::new(GlobalKeyValueRowCache::default()),
            root_base_cache: std::sync::Arc::default(),
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
    pub(crate) fn reader<S>(&self, store: S) -> HotStateContextReader<S>
    where
        S: StorageAdapterRead,
    {
        HotStateContextReader {
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::clone(&self.row_columnar_layout_cache),
            branch_head_control_cache: None,
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
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
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::clone(&self.row_columnar_layout_cache),
            branch_head_control_cache: Some(branch_head_control_cache),
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
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
            store,
            tracked_head: self.tracked_head,
            commit_graph: self.commit_graph.clone(),
            filesystem_path_index_cache: std::sync::Arc::clone(&self.filesystem_path_index_cache),
            row_columnar_layout_cache: std::sync::Arc::new(RowColumnarLayoutCache::default()),
            branch_head_control_cache: None,
            root_base_cache: std::sync::Arc::clone(&self.root_base_cache),
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
    store: S,
    tracked_head: TrackedHeadContext,
    commit_graph: CommitGraphContext,
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
    row_columnar_layout_cache: std::sync::Arc<RowColumnarLayoutCache>,
    branch_head_control_cache: Option<std::sync::Arc<BranchHeadControlCache>>,
    root_base_cache: std::sync::Arc<crate::hot_state::tracked_head::RootBaseBatchCache>,
}

impl<S> HotStateContextReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn prepare_packed_identity_membership(
        &self,
        branch_id: &str,
        schema_key: &str,
    ) -> Result<Option<crate::hot_state::PackedIdentityMembership>, LixError> {
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

    /// Returns raw current-state snapshot bytes for one current SQL row scan.
    ///
    /// `None` means the normal materialized visibility path remains
    /// authoritative: a global branch projection, multiple result branch
    /// scopes, or commit-derived state all require its full visibility
    /// semantics.
    pub(crate) async fn scan_direct_row_snapshots(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        let Some((requested_branch_id, requested_control, schema_key)) =
            self.direct_row_snapshot_scope(request).await?
        else {
            return Ok(None);
        };
        let snapshots = self
            .tracked_head
            .reader(&self.store)
            .with_root_base_cache(std::sync::Arc::clone(&self.root_base_cache))
            .scan_row_snapshots(
                &requested_branch_id,
                requested_control,
                &schema_key,
                &request.filter.row_pks,
                request.limit,
            )
            .await?;
        Ok(Some(snapshots))
    }

    /// Returns committed row identities under the same narrow visibility
    /// proof as [`Self::scan_direct_row_snapshots`].  The packed reader
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

    #[cfg(test)]
    pub(crate) async fn row_columnar_overlay_len_for_test(
        &self,
        branch_id: &str,
        schema_key: &str,
    ) -> Result<Option<usize>, LixError> {
        let Some(control) = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?
        else {
            return Ok(None);
        };
        Ok(self
            .tracked_head
            .reader(&self.store)
            .row_columnar_layout(branch_id, control, schema_key)
            .await?
            .map(|(_, _, overlay, _)| overlay.len()))
    }

    async fn direct_row_snapshot_scope(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<Option<(String, BranchHeadControl, String)>, LixError> {
        // The hot index carries tracked and untracked rows in one serving
        // plane, so this route never probes a separate retention index.
        if request.filter.untracked.is_some() || request_may_include_derived(request) {
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
        let Some(requested_control) = scope.branch_heads.get(requested_branch_id).copied() else {
            return Ok(None);
        };
        let tracked_head = self.tracked_head.reader(&self.store);
        if requested_branch_id != GLOBAL_BRANCH_ID
            && let Some(global_control) = scope.branch_heads.get(GLOBAL_BRANCH_ID).copied()
            && tracked_head
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

    async fn scan_batch_with_schema_presence(
        &self,
        request: &HotStateScanRequest,
        skip_proven_empty_schema: bool,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let store = &self.store;
        let reads_tracked = !is_derived_only_request(request);
        let scope = scan_scope(
            store,
            request,
            reads_tracked,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
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
        if let Some(rows) = self.scan_direct_row_pk_batch(request, &scope).await? {
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
                    branch_ids: scope.projection_branch_ids.clone(),
                },
                include_tombstones: request.filter.include_tombstones,
                limit: request.limit,
            },
        ))
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
            // contributes no candidates, so it needs no witness. The bloom has
            // no false negatives, so this skip cannot hide a row. Without it a
            // branch that never stores the schema — the global branch, for
            // every ordinary user collection — would be permanently
            // unwitnessed and would veto the index for everyone.
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
            .scan_live_batches_for_controls(&controls, &tracked_request, request.filter.untracked)
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

    pub(crate) async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
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
        let scope = scan_scope(
            &self.store,
            &scope_request,
            true,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
        let visible_branch_ids = scope
            .projection_branch_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();

        let mut storage_identities = Vec::with_capacity(request.rows.len().saturating_mul(2));
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
            if row.branch_id != GLOBAL_BRANCH_ID {
                storage_identities.push(crate::hot_state::HotStateRowIdentityRef {
                    branch_id: GLOBAL_BRANCH_ID,
                    schema_key: row.schema_key.as_str(),
                    row_pk: &row.row_pk,
                    file_id: row.file_id.as_deref(),
                });
            }
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
        let current_batches = stream::iter(branch_ranges)
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
                        .load_projected_live_batch_refs_for_domain(
                            branch_id,
                            control,
                            &keys,
                            &projection,
                            domain,
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
        // `storage_identities` and branch ranges are sorted, and `buffered`
        // preserves input order, so candidate slots are already identity
        // ordered. Keep the assertion close to the binary-search contract.
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
        let store = &self.store;
        let reads_tracked = !is_derived_only_request(request);
        let scope = scan_scope(
            store,
            request,
            reads_tracked,
            self.branch_head_control_cache.as_deref(),
        )
        .await?;
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
                        .scan_live_batch_for_retention(
                            &branch_id,
                            control,
                            &tracked_request,
                            request.filter.untracked,
                        )
                        .await?;
                    Ok::<_, LixError>(HotBranchRows {
                        branch_id: branch_id.clone(),
                        rows,
                        ordered_unique: true,
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

    async fn collection_generation(
        &self,
        branch_id: &str,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
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
    async fn path_index(
        &self,
        request: &FilesystemPathIndexRequest,
    ) -> Result<std::sync::Arc<FilesystemPathIndex>, LixError> {
        let revision = load_path_index_revision(&self.store).await?;
        if let Some(index) = self
            .filesystem_path_index_cache
            .get(request, revision.as_deref())
        {
            return Ok(index);
        }
        let mut index = build_path_index(self, request).await?;
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

/// Rows read from one durable hot-state branch source.
///
/// A matching hot-state projection is storage-key ordered by visible identity
/// and has one row per identity.
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
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogReader, CommitId,
        CommitLoadRequest,
    };
    use crate::row_pk::RowPk;
    use crate::hot_state::{
        CurrentStateDeltaRef, HotStateExactBatchRequest, HotStateExactRowRequest, HotStateFilter,
        HotStateProjection, TrackedHeadDeltaRef, WorkingDiffIndexCoverage,
    };
    use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
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
        assert!(std::sync::Arc::ptr_eq(&inserted, &hit));
        assert!(std::sync::Arc::ptr_eq(&inserted.overlay, &hit.overlay));
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
                    snapshot: crate::json_store::JsonSlotRef::Inline(snapshot),
                    metadata: crate::json_store::JsonSlotRef::None,
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
                snapshot: row.snapshot.map_or(
                    crate::json_store::JsonSlotRef::None,
                    crate::json_store::JsonSlotRef::Inline,
                ),
                metadata: crate::json_store::JsonSlotRef::None,
            })
            .collect::<Vec<_>>();
        TrackedHeadContext::new()
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
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

    async fn scan_direct_row_snapshots_for_test(
        hot_state: &HotStateContext,
        storage: &StorageAdapter,
        branch_id: &str,
        schema_key: &str,
        row_pks: &[RowPk],
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
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
        assert_eq!(
            snapshots
                .iter()
                .map(|snapshot| snapshot
                    .as_deref()
                    .and_then(|bytes| std::str::from_utf8(bytes).ok()))
                .collect::<Vec<_>>(),
            vec![
                Some(r#"{"id":"first","value":"one"}"#),
                Some(r#"{"id":"second","value":"two"}"#),
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
        let mut json_writer = JsonStoreContext::new().writer();
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
            if let Some(snapshot) = row.snapshot_content.as_deref() {
                json_writer
                    .stage_batch(
                        &mut writes,
                        JsonWritePlacementRef::OutOfBand,
                        [NormalizedJsonRef::trusted_prehashed(
                            snapshot,
                            JsonRef::for_content(snapshot.as_bytes()),
                        )],
                    )
                    .expect("untracked snapshot should stage");
            }
            if let Some(metadata) = row.metadata.as_deref() {
                json_writer
                    .stage_batch(
                        &mut writes,
                        JsonWritePlacementRef::OutOfBand,
                        [NormalizedJsonRef::trusted_prehashed(
                            metadata,
                            JsonRef::for_content(metadata.as_bytes()),
                        )],
                    )
                    .expect("untracked metadata should stage");
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
                    row.snapshot_content.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
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
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_ref_slot(),
                    metadata: metadata.as_ref_slot(),
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
                    row.snapshot_content.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
                })
                .collect::<Vec<_>>();
            let metadata = branch_rows
                .iter()
                .map(|row| {
                    row.metadata.as_deref().map_or(
                        crate::json_store::JsonSlot::None,
                        crate::json_store::JsonSlot::from_json,
                    )
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
                    untracked: true,
                    deleted: row.deleted,
                    created_at: ts(&row.created_at),
                    updated_at: ts(&row.updated_at),
                    snapshot: snapshot.as_ref_slot(),
                    metadata: metadata.as_ref_slot(),
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
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
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
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: Default::default(),
                    touched_scope_filter: Default::default(),
                    current_state_scoped_ranges: None,
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
    async fn mixed_derived_identities_choose_access_per_provider() {
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
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
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
                    current_state_scoped_ranges: None,
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
        let edge_pk = RowPk::from_components(smallvec::smallvec![
            crate::row_pk::RowPkComponent::Uuid(*child.as_uuid().as_bytes()),
            crate::row_pk::RowPkComponent::Integer(0),
        ])
        .expect("valid edge identity");
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                schema_keys: vec!["lix_commit".to_string(), "lix_commit_edge".to_string()],
                row_pks: vec![
                    RowPk::uuid_from_bytes(*child.as_uuid().as_bytes()),
                    edge_pk.clone(),
                ],
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
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.schema_key == "lix_commit"));
        assert!(
            rows.iter()
                .any(|row| row.schema_key == "lix_commit_edge" && row.row_pk == edge_pk)
        );
    }

    async fn stage_materialized_live_rows(
        store: &impl StorageAdapterRead,
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
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
                stage_json_payloads_from_materialized(writes, json_writer, &materialized)?;
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
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 4,
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
                    deleted: change.snapshot.is_none(),
                    created_at: *created_at,
                    updated_at: *updated_at,
                })
                .collect::<Vec<_>>();
            let commit_deltas = rows
                .iter()
                .zip(&root_deltas)
                .map(|((change, _, _), delta)| TrackedStateCommitDeltaRef {
                    delta: *delta,
                    snapshot: change.snapshot.as_ref_slot(),
                    metadata: change.metadata.as_ref_slot(),
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
                    commit_id: record.commit_id,
                    change_account_id: record.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: mutation_inventory,
                    touched_scope_filter: Default::default(),
                    current_state_scoped_ranges: None,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
            )?;
        }

        Ok(())
    }

    fn stage_json_payloads_from_materialized(
        writes: &mut StorageWriteSet,
        json_writer: &mut crate::json_store::JsonStoreWriter,
        row: &MaterializedTrackedStateRow,
    ) -> Result<(), LixError> {
        if let Some(snapshot) = row.snapshot_content.as_deref() {
            json_writer.stage_batch(
                writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::trusted_prehashed(
                    snapshot,
                    JsonRef::for_content(snapshot.as_bytes()),
                )],
            )?;
        }
        if let Some(metadata) = row.metadata.as_ref() {
            let serialized = crate::serialize_row_metadata(metadata);
            json_writer.stage_batch(
                writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::trusted_prehashed(
                    &serialized,
                    JsonRef::for_content(serialized.as_bytes()),
                )],
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
        let mut json_writer = JsonStoreContext::new().writer();
        // Keep the tracked commit fixture separate from the untracked member
        // under test: a single identity cannot change retention class.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.row_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &[tracked_row])
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
        let mut json_writer = JsonStoreContext::new().writer();
        // The tracked fixture establishes the branch head; use a distinct
        // identity so the selected untracked row is not a retention conflict.
        let mut tracked_row =
            tracked_row_with_commit("tracked-value", Some("change-tracked"), "commit-tracked");
        tracked_row.row_pk = identity("tracked-tab");
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &[tracked_row])
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(
                    &read,
                    &mut writes,
                    &mut json_writer,
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
        let mut json_writer = JsonStoreContext::new().writer();
        stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
            let mut json_writer = JsonStoreContext::new().writer();
            {
                stage_materialized_live_rows(&read, &mut writes, &mut json_writer, &rows)
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
