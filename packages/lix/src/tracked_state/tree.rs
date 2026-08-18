#![cfg_attr(
    test,
    allow(
        clippy::manual_async_fn,
        reason = "test readers mirror explicit Send future signatures from StorageRead"
    )
)]

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    future::Future,
    num::NonZeroUsize,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use lru::LruCache;

#[cfg(test)]
use crate::changelog::{ChangeId, CommitId};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::codec::{
    ChildSummary, DecodedLeafNodeRef, DecodedNode, DecodedNodeRef, EncodedLeafEntry, PendingChunk,
    PendingChunkBatch, TrackedStateKeyBatchBuilder, boundary_trigger, decode_key,
    decode_key_shared, decode_key_with_trusted_prefix, decode_node, decode_node_ref, decode_value,
    decode_visible_value, encode_internal_node, encode_key, encode_key_ref_into, encode_leaf_node,
    encode_schema_file_prefix, encode_schema_key_prefix, encode_value_ref, encode_value_ref_into,
    hash_bytes,
};
use crate::tracked_state::diff::{TrackedStateTreeDiffBatch, TrackedStateTreeDiffBatchBuilder};
use crate::tracked_state::storage;
#[cfg(test)]
use crate::tracked_state::types::TrackedStateMutation;
#[cfg(test)]
use crate::tracked_state::types::TrackedStateTreeDiffEntry;
use crate::tracked_state::types::{
    TRACKED_STATE_HASH_BYTES, TrackedStateApplyResult, TrackedStateDeltaRef,
    TrackedStateIndexValue, TrackedStateIndexValueRef, TrackedStateKey, TrackedStateKeyRef,
    TrackedStateMutationBatch, TrackedStateRootId, TrackedStateRootMutationRef,
    TrackedStateTreeScanRequest,
};
use crate::{LixError, NullableKeyFilter};

// Nodes are immutable and addressed by their BLAKE3 content hash, so verified
// bytes are safe to share across tree clones. Nodes larger than the configured
// maximum chunk size bypass the cache, bounding production payload bytes at
// about 64 MiB (excluding cache metadata and live `Bytes` views held by callers).
const TRACKED_STATE_NODE_CACHE_CAPACITY: usize = 4096;
type TrackedStateNodeCache = LruCache<[u8; TRACKED_STATE_HASH_BYTES], Bytes>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeOptions {
    pub(crate) target_chunk_bytes: usize,
    pub(crate) min_chunk_bytes: usize,
    pub(crate) max_chunk_bytes: usize,
}

/// A sorted, unique event at the publication frontier.  The event owns one
/// encoded key/value pair; traversal borrows the slice and never constructs a
/// second map or an intermediate root.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FrontierMutation {
    key: Bytes,
    value: Bytes,
}

#[derive(Debug)]
struct FrontierRewrite {
    summaries: Vec<ChildSummary>,
    height: usize,
    changed: bool,
    child_summaries: Option<Vec<ChildSummary>>,
}

impl Default for TrackedStateTreeOptions {
    fn default() -> Self {
        Self {
            target_chunk_bytes: 4 * 1024,
            min_chunk_bytes: 512,
            max_chunk_bytes: 16 * 1024,
        }
    }
}

/// Content-addressed tracked-state tree operations.
///
/// This type owns immutable tree mechanics only. Branch refs, mutable live state,
/// and SQL visibility remain outside the tree.
#[derive(Debug, Clone)]
pub(crate) struct TrackedStateTree {
    options: TrackedStateTreeOptions,
    node_cache: Arc<Mutex<TrackedStateNodeCache>>,
}

impl TrackedStateTree {
    pub(crate) fn new() -> Self {
        Self::with_options_inner(TrackedStateTreeOptions::default())
    }

    #[cfg(test)]
    pub(crate) fn with_options(options: TrackedStateTreeOptions) -> Self {
        Self::with_options_inner(options)
    }

    fn with_options_inner(options: TrackedStateTreeOptions) -> Self {
        Self {
            options,
            node_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(TRACKED_STATE_NODE_CACHE_CAPACITY)
                    .expect("tracked-state node cache capacity must be non-zero"),
            ))),
        }
    }

    pub(crate) async fn load_root(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        commit_id: &str,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        storage::load_root(store, commit_id).await
    }

    /// Returns the distinct schema identities that are physically present in
    /// this authenticated root. Each schema run is discovered with one
    /// lower-bound descent, so callers can plan schema/file-bounded work
    /// without trusting the public registration catalog or materializing the
    /// full tree.
    pub(crate) async fn distinct_schema_keys(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<String>, LixError> {
        let mut schema_keys = Vec::new();
        let mut lower = Vec::new();
        while let Some(encoded_key) = self
            .first_key_at_or_after(store, root_id, &lower)
            .await?
        {
            let key = decode_key(&encoded_key)?;
            let schema_key = key.schema_key;
            let Some(next_lower) =
                lexicographic_successor(&encode_schema_key_prefix(&schema_key))
            else {
                schema_keys.push(schema_key);
                break;
            };
            if next_lower <= lower {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "tracked-state schema inventory did not advance",
                ));
            }
            schema_keys.push(schema_key);
            lower = next_lower;
        }
        Ok(schema_keys)
    }

    async fn first_key_at_or_after(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
        lower: &[u8],
    ) -> Result<Option<Bytes>, LixError> {
        let mut current = *root_id.as_bytes();
        let mut expected_summary = None;
        loop {
            let node = self.load_node(store, &current).await?;
            if let Some(expected) = expected_summary.take() {
                validate_decoded_node_summary(&node, &expected)?;
            }
            match node {
                DecodedNode::Leaf(leaf) => {
                    let mut low = 0usize;
                    let mut high = leaf.len();
                    while low < high {
                        let mid = low + (high - low) / 2;
                        let key = leaf.key(mid).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                "tracked-state leaf key disappeared during lower-bound seek",
                            )
                        })?;
                        if key < lower {
                            low = mid + 1;
                        } else {
                            high = mid;
                        }
                    }
                    return Ok(leaf.entry_owned(low).map(|entry| entry.key));
                }
                DecodedNode::Internal(internal) => {
                    let children = internal.into_children();
                    for child in &children {
                        self.authenticate_subtree_right_edge(store, child).await?;
                    }
                    let Some(child) = children
                        .into_iter()
                        .find(|child| child.last_key.as_ref() >= lower)
                    else {
                        return Ok(None);
                    };
                    current = child.child_hash;
                    expected_summary = Some(child);
                }
            }
        }
    }

    async fn authenticate_subtree_right_edge(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        summary: &ChildSummary,
    ) -> Result<(), LixError> {
        let mut current = summary.child_hash;
        let mut expected = summary.clone();
        loop {
            let node = self.load_node(store, &current).await?;
            validate_decoded_node_summary(&node, &expected)?;
            match node {
                DecodedNode::Leaf(_) => return Ok(()),
                DecodedNode::Internal(internal) => {
                    let Some(child) = internal.children().last().cloned() else {
                        return Err(LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "tracked-state internal node has no right edge",
                        ));
                    };
                    current = child.child_hash;
                    expected = child;
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn get(
        &self,
        store: &impl StorageAdapterRead,
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
    ) -> Result<Option<TrackedStateIndexValue>, LixError> {
        let encoded_key = encode_key(key);
        let mut current = *root_id.as_bytes();
        loop {
            match self.load_node(store, &current).await? {
                DecodedNode::Leaf(leaf) => {
                    let entry = binary_search_leaf_key(&leaf, &encoded_key)?
                        .and_then(|index| leaf.entry(index));
                    return entry.map(|entry| decode_value(entry.value)).transpose();
                }
                DecodedNode::Internal(internal) => {
                    let child = internal
                        .children()
                        .iter()
                        .find(|child| child.last_key.as_ref() >= encoded_key.as_slice())
                        .or_else(|| internal.children().last())
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "tracked-state tree internal node has no children",
                            )
                        })?;
                    current = child.child_hash;
                }
            }
        }
    }

    pub(crate) async fn get_many(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut key_batch = TrackedStateKeyBatchBuilder::with_row_capacity(keys.len());
        for key in keys {
            key_batch.push(TrackedStateKeyRef {
                schema_key: &key.schema_key,
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            });
        }
        let encoded_keys = key_batch.finish();
        self.get_many_encoded(store, root_id, &encoded_keys).await
    }

    /// Resolves an arena-backed encoded identity batch without materializing
    /// row-owned schema or file strings.
    pub(crate) async fn get_many_encoded(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
        keys: &[Bytes],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut encoded_keys = keys.iter().cloned().enumerate().collect::<Vec<_>>();
        encoded_keys.sort_by(|left, right| left.1.cmp(&right.1));

        let mut values = vec![None; keys.len()];
        self.get_many_node(store, *root_id.as_bytes(), &encoded_keys, &mut values)
            .await?;
        Ok(values)
    }

    pub(crate) async fn scan(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        if request.limit == Some(0) {
            return Ok(Vec::new());
        }
        if !request.row_pks.is_empty()
            && request.row_pks.iter().all(|row_pk| {
                !crate::tracked_state::row_pk_satisfies_bounds(
                    row_pk,
                    request.row_pk_lower.as_ref(),
                    request.row_pk_upper.as_ref(),
                )
            })
        {
            return Ok(Vec::new());
        }

        let ranges = scan_ranges(request);
        let key_decode_hint = scan_key_decode_hint(request, &ranges);
        let row_capacity = if request.include_tombstones
            && request.schema_keys.is_empty()
            && request.row_pks.is_empty()
            && request.file_ids.is_empty()
        {
            let row_count = match self.load_node(store, root_id.as_bytes()).await? {
                DecodedNode::Leaf(leaf) => Some(leaf.len() as u64),
                DecodedNode::Internal(internal) => internal
                    .children()
                    .iter()
                    .try_fold(0u64, |count, child| count.checked_add(child.subtree_count)),
            };
            row_count
                .and_then(|count| usize::try_from(count).ok())
                .unwrap_or(0)
        } else {
            0
        };
        let mut rows = Vec::new();
        let reserve = request
            .limit
            .map_or(row_capacity, |limit| limit.min(row_capacity));
        // Subtree counts are storage data. A corrupt count must not turn a
        // best-effort scan allocation hint into a capacity panic.
        let _ = rows.try_reserve_exact(reserve);
        self.scan_node(
            store,
            *root_id.as_bytes(),
            request,
            &ranges,
            key_decode_hint,
            &mut rows,
        )
        .await?;
        Ok(rows)
    }

    pub(crate) async fn diff(
        &self,
        store: &impl StorageAdapterRead,
        left_root: Option<&TrackedStateRootId>,
        right_root: Option<&TrackedStateRootId>,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<TrackedStateTreeDiffBatch, LixError> {
        match (left_root, right_root) {
            (None, None) => Ok(TrackedStateTreeDiffBatch::default()),
            (Some(left), Some(right)) if left == right => Ok(TrackedStateTreeDiffBatch::default()),
            (Some(left), Some(right)) => {
                let mut out = TrackedStateTreeDiffBatchBuilder::with_row_capacity(0);
                self.diff_nodes(
                    store,
                    *left.as_bytes(),
                    *right.as_bytes(),
                    request,
                    &mut out,
                )
                .await?;
                out.finish()
            }
            (Some(left), None) => {
                let ranges = scan_ranges(request);
                let mut out = TrackedStateTreeDiffBatchBuilder::with_row_capacity(0);
                self.collect_root_diff_shared(
                    store,
                    *left.as_bytes(),
                    request,
                    &ranges,
                    true,
                    &mut out,
                )
                .await?;
                out.finish()
            }
            (None, Some(right)) => {
                let ranges = scan_ranges(request);
                let mut out = TrackedStateTreeDiffBatchBuilder::with_row_capacity(0);
                self.collect_root_diff_shared(
                    store,
                    *right.as_bytes(),
                    request,
                    &ranges,
                    false,
                    &mut out,
                )
                .await?;
                out.finish()
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn apply_mutations(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        base_root: Option<&TrackedStateRootId>,
        mutations: TrackedStateMutationBatch,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        let mut overlay = storage::TrackedStateChunkOverlay::new();
        self.apply_mutations_with_overlay(
            store,
            writes,
            &mut overlay,
            base_root,
            mutations,
            commit_id,
        )
        .await
    }

    pub(crate) async fn apply_mutations_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        base_root: Option<&TrackedStateRootId>,
        mutations: TrackedStateMutationBatch,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        let mut events = mutations
            .into_mutations()
            .into_iter()
            .map(|mutation| FrontierMutation {
                key: mutation.encoded_key,
                value: mutation.encoded_value,
            })
            .collect::<Vec<_>>();
        events.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let mut unique = Vec::with_capacity(events.len());
        for event in events {
            if unique
                .last()
                .is_some_and(|previous: &FrontierMutation| previous.key == event.key)
            {
                *unique
                    .last_mut()
                    .expect("duplicate frontier event has a predecessor") = event;
            } else {
                unique.push(event);
            }
        }
        self.apply_sorted_frontier(store, writes, overlay, base_root, &unique, commit_id)
            .await
    }

    async fn apply_sorted_frontier(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        base_root: Option<&TrackedStateRootId>,
        events: &[FrontierMutation],
        _commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        if events.windows(2).any(|pair| pair[0].key >= pair[1].key) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state mutation frontier requires sorted unique keys",
            ));
        }
        if events.is_empty() {
            let Some(root_id) = base_root else {
                let mut chunks = PendingChunkBatchBuilder::default();
                let root = self
                    .build_leaf_level(Vec::new(), &mut chunks)
                    .pop()
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "empty tracked-state frontier produced no root",
                        )
                    })?;
                let chunk_bytes = chunks.data.len();
                let chunks = chunks.finish();
                overlay.stage_chunks(store, writes, &chunks).await?;
                return Ok(TrackedStateApplyResult {
                    root_id: TrackedStateRootId::new(root.child_hash),
                    row_count: 0,
                    tree_height: 1,
                    chunk_count: chunks.len(),
                    chunk_bytes,
                });
            };
            let height = self
                .root_height_with_overlay(store, overlay, *root_id.as_bytes())
                .await?;
            let row_count = decoded_node_row_count(
                &self
                    .load_node_with_overlay(store, overlay, root_id.as_bytes())
                    .await?,
            );
            return Ok(TrackedStateApplyResult {
                root_id: root_id.clone(),
                row_count,
                tree_height: height,
                chunk_count: 0,
                chunk_bytes: 0,
            });
        }

        let mut chunks = PendingChunkBatchBuilder::default();
        let (root_id, row_count, tree_height) = match base_root {
            None => {
                let entries = events
                    .iter()
                    .map(|event| EncodedLeafEntry {
                        key: event.key.clone(),
                        value: event.value.clone(),
                    })
                    .collect::<Vec<_>>();
                let mut summaries = self.build_leaf_level(entries, &mut chunks);
                let mut height = 1usize;
                while summaries.len() > 1 {
                    summaries = self.build_internal_level(summaries, height, &mut chunks);
                    height += 1;
                }
                let root = summaries.pop().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-state frontier produced no root",
                    )
                })?;
                (
                    TrackedStateRootId::new(root.child_hash),
                    root.subtree_count as usize,
                    height,
                )
            }
            Some(root_id) => {
                let height = self
                    .root_height_with_overlay(store, overlay, *root_id.as_bytes())
                    .await?;
                let rewritten = self
                    .rewrite_frontier_node(
                        store,
                        overlay,
                        *root_id.as_bytes(),
                        height.saturating_sub(1),
                        events,
                        &mut chunks,
                    )
                    .await?;
                if !rewritten.changed {
                    return Ok(TrackedStateApplyResult {
                        root_id: root_id.clone(),
                        row_count: rewritten
                            .summaries
                            .first()
                            .map_or(0, |summary| summary.subtree_count as usize),
                        tree_height: rewritten.height,
                        chunk_count: 0,
                        chunk_bytes: 0,
                    });
                }
                let mut summaries = rewritten.summaries;
                let mut new_height = rewritten.height;
                while summaries.len() > 1 {
                    summaries = self.build_internal_level(summaries, new_height, &mut chunks);
                    new_height += 1;
                }
                let root = summaries.pop().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-state frontier rewrite produced no root",
                    )
                })?;
                (
                    TrackedStateRootId::new(root.child_hash),
                    root.subtree_count as usize,
                    new_height,
                )
            }
        };
        let chunk_bytes = chunks.data.len();
        let chunks = chunks.finish();
        overlay.stage_chunks(store, writes, &chunks).await?;
        Ok(TrackedStateApplyResult {
            root_id,
            row_count,
            tree_height,
            chunk_count: chunks.len(),
            chunk_bytes,
        })
    }

    async fn root_height_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        mut hash: [u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<usize, LixError> {
        let mut height = 1usize;
        loop {
            match self.load_node_with_overlay(store, overlay, &hash).await? {
                DecodedNode::Leaf(_) => return Ok(height),
                DecodedNode::Internal(internal) => {
                    hash = internal
                        .children()
                        .first()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "tracked-state internal node has no children",
                            )
                        })?
                        .child_hash;
                    height = height.saturating_add(1);
                }
            }
        }
    }

    fn rewrite_frontier_node<'a, S>(
        &'a self,
        store: &'a S,
        overlay: &'a storage::TrackedStateChunkOverlay,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        level: usize,
        events: &'a [FrontierMutation],
        chunks: &'a mut PendingChunkBatchBuilder,
    ) -> Pin<Box<dyn Future<Output = Result<FrontierRewrite, LixError>> + Send + 'a>>
    where
        S: StorageAdapterRead + ?Sized + 'a,
    {
        Box::pin(async move {
            let node = self.load_node_with_overlay(store, overlay, &hash).await?;
            match node {
                DecodedNode::Leaf(leaf) => {
                    let old_entries = leaf.clone().into_entries();
                    let mut entries = Vec::with_capacity(old_entries.len() + events.len());
                    let mut event_index = 0usize;
                    let mut changed = false;
                    for old in old_entries {
                        while event_index < events.len()
                            && events[event_index].key.as_ref() < old.key.as_ref()
                        {
                            let event = &events[event_index];
                            entries.push(EncodedLeafEntry {
                                key: event.key.clone(),
                                value: event.value.clone(),
                            });
                            event_index += 1;
                            changed = true;
                        }
                        if event_index < events.len()
                            && events[event_index].key.as_ref() == old.key.as_ref()
                        {
                            let event = &events[event_index];
                            changed |= event.value != old.value;
                            entries.push(EncodedLeafEntry {
                                key: old.key,
                                value: event.value.clone(),
                            });
                            event_index += 1;
                        } else {
                            entries.push(old);
                        }
                    }
                    while event_index < events.len() {
                        let event = &events[event_index];
                        entries.push(EncodedLeafEntry {
                            key: event.key.clone(),
                            value: event.value.clone(),
                        });
                        event_index += 1;
                        changed = true;
                    }
                    if !changed {
                        return Ok(FrontierRewrite {
                            summaries: vec![decoded_leaf_summary(hash, &leaf)],
                            height: 1,
                            changed: false,
                            child_summaries: None,
                        });
                    }
                    Ok(FrontierRewrite {
                        summaries: self.build_leaf_level(entries, chunks),
                        height: 1,
                        changed: true,
                        child_summaries: None,
                    })
                }
                DecodedNode::Internal(internal) => {
                    let children = internal.into_children();
                    if children.is_empty() {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "tracked-state internal node has no children",
                        ));
                    }
                    if level == 1 {
                        return self
                            .rewrite_leaf_frontier_window(
                                store, overlay, hash, children, events, chunks,
                            )
                            .await;
                    }
                    return self
                        .rewrite_internal_frontier_window(
                            store, overlay, hash, level, children, events, chunks,
                        )
                        .await;
                }
            }
        })
    }

    async fn rewrite_leaf_frontier_window(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        _hash: [u8; TRACKED_STATE_HASH_BYTES],
        children: Vec<ChildSummary>,
        events: &[FrontierMutation],
        chunks: &mut PendingChunkBatchBuilder,
    ) -> Result<FrontierRewrite, LixError> {
        let first_key = events.first().map_or(&[][..], |event| event.key.as_ref());
        let last_key = events.last().map_or(&[][..], |event| event.key.as_ref());
        let start = children
            .iter()
            .position(|child| child.last_key.as_ref() >= first_key)
            .unwrap_or_else(|| children.len().saturating_sub(1));
        let mut window_entries = Vec::new();
        let mut event_index = 0usize;
        let mut child_index = start;
        loop {
            let child = children.get(child_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state leaf frontier ran past child summaries",
                )
            })?;
            let leaf = self
                .load_node_with_overlay(store, overlay, &child.child_hash)
                .await?;
            let leaf = match leaf {
                DecodedNode::Leaf(leaf) => leaf,
                DecodedNode::Internal(_) => {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-state leaf frontier expected a leaf child",
                    ));
                }
            };
            window_entries.extend(leaf.into_entries());
            if child_index + 1 == children.len() {
                event_index = events.len();
            } else {
                while event_index < events.len()
                    && events[event_index].key.as_ref() <= child.last_key.as_ref()
                {
                    event_index += 1;
                }
            }

            if event_index == events.len() {
                let mut merged = Vec::with_capacity(window_entries.len() + events.len());
                let mut old_index = 0usize;
                let mut incoming_index = 0usize;
                while old_index < window_entries.len() {
                    while incoming_index < events.len()
                        && events[incoming_index].key.as_ref()
                            < window_entries[old_index].key.as_ref()
                    {
                        merged.push(EncodedLeafEntry {
                            key: events[incoming_index].key.clone(),
                            value: events[incoming_index].value.clone(),
                        });
                        incoming_index += 1;
                    }
                    if incoming_index < events.len()
                        && events[incoming_index].key.as_ref()
                            == window_entries[old_index].key.as_ref()
                    {
                        merged.push(EncodedLeafEntry {
                            key: window_entries[old_index].key.clone(),
                            value: events[incoming_index].value.clone(),
                        });
                        incoming_index += 1;
                    } else {
                        merged.push(window_entries[old_index].clone());
                    }
                    old_index += 1;
                }
                while incoming_index < events.len() {
                    merged.push(EncodedLeafEntry {
                        key: events[incoming_index].key.clone(),
                        value: events[incoming_index].value.clone(),
                    });
                    incoming_index += 1;
                }
                let mut candidate_chunks = PendingChunkBatchBuilder::default();
                let candidate = self.build_leaf_level(merged, &mut candidate_chunks);
                if let Some((generated, existing)) =
                    first_resync_index(&candidate, &children[start..], last_key)
                {
                    for summary in &candidate[..generated] {
                        chunks.copy_chunk_from(&candidate_chunks, &summary.child_hash);
                    }
                    let mut output = children[..start].to_vec();
                    output.extend(candidate.into_iter().take(generated));
                    output.extend_from_slice(&children[start + existing..]);
                    let child_summaries = output.clone();
                    return Ok(FrontierRewrite {
                        summaries: self.build_internal_level(output, 1, chunks),
                        height: 2,
                        changed: true,
                        child_summaries: Some(child_summaries),
                    });
                }
                if child_index + 1 == children.len() {
                    chunks.extend(candidate_chunks);
                    let mut output = children[..start].to_vec();
                    output.extend(candidate);
                    let child_summaries = output.clone();
                    return Ok(FrontierRewrite {
                        summaries: self.build_internal_level(output, 1, chunks),
                        height: 2,
                        changed: true,
                        child_summaries: Some(child_summaries),
                    });
                }
            }
            child_index += 1;
        }
    }

    async fn rewrite_internal_frontier_window(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        _hash: [u8; TRACKED_STATE_HASH_BYTES],
        level: usize,
        children: Vec<ChildSummary>,
        events: &[FrontierMutation],
        chunks: &mut PendingChunkBatchBuilder,
    ) -> Result<FrontierRewrite, LixError> {
        let first_key = events.first().map_or(&[][..], |event| event.key.as_ref());
        let last_key = events.last().map_or(&[][..], |event| event.key.as_ref());
        let start = children
            .iter()
            .position(|child| child.last_key.as_ref() >= first_key)
            .unwrap_or_else(|| children.len().saturating_sub(1));
        let mut window_children = Vec::new();
        let mut event_index = 0usize;
        let mut child_index = start;
        loop {
            let child = children.get(child_index).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked-state internal frontier ran past child summaries",
                )
            })?;
            let event_end = if child_index + 1 == children.len() {
                events.len()
            } else {
                let mut end = event_index;
                while end < events.len() && events[end].key.as_ref() <= child.last_key.as_ref() {
                    end += 1;
                }
                end
            };
            if event_index < event_end {
                let rewritten = self
                    .rewrite_frontier_node(
                        store,
                        overlay,
                        child.child_hash,
                        level.saturating_sub(1),
                        &events[event_index..event_end],
                        chunks,
                    )
                    .await?;
                let child_summaries = rewritten.child_summaries.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked-state changed internal child did not expose its frontier",
                    )
                })?;
                window_children.extend(child_summaries);
            } else {
                let node = self
                    .load_node_with_overlay(store, overlay, &child.child_hash)
                    .await?;
                let node_children = match node {
                    DecodedNode::Internal(internal) => internal.into_children(),
                    DecodedNode::Leaf(_) => {
                        return Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "tracked-state internal frontier expected an internal child",
                        ));
                    }
                };
                window_children.extend(node_children);
            }
            event_index = event_end;
            if event_index == events.len() {
                let mut candidate_chunks = PendingChunkBatchBuilder::default();
                let candidate = self.build_internal_level(
                    window_children.iter().cloned().collect(),
                    level - 1,
                    &mut candidate_chunks,
                );
                if let Some((generated, existing)) =
                    first_resync_index(&candidate, &children[start..], last_key)
                {
                    for summary in &candidate[..generated] {
                        chunks.copy_chunk_from(&candidate_chunks, &summary.child_hash);
                    }
                    let mut output = children[..start].to_vec();
                    output.extend(candidate.into_iter().take(generated));
                    output.extend_from_slice(&children[start + existing..]);
                    let child_summaries = output.clone();
                    return Ok(FrontierRewrite {
                        summaries: self.build_internal_level(output, level, chunks),
                        height: level + 1,
                        changed: true,
                        child_summaries: Some(child_summaries),
                    });
                }
                if child_index + 1 == children.len() {
                    chunks.extend(candidate_chunks);
                    let mut output = children[..start].to_vec();
                    output.extend(candidate);
                    let child_summaries = output.clone();
                    return Ok(FrontierRewrite {
                        summaries: self.build_internal_level(output, level, chunks),
                        height: level + 1,
                        changed: true,
                        child_summaries: Some(child_summaries),
                    });
                }
            }
            child_index += 1;
        }
    }

    /// Merges a full, primary-key-sorted mutation batch with a parent root in
    /// one pass and rebuilds canonical chunks directly from that merge.
    ///
    /// The normal tracked bulk path used to first point-read every changed key
    /// for collision/created-at handling, then collect the parent leaves, then
    /// materialize another complete merged vector. This path keeps only one
    /// incoming mutation and one output leaf in memory at a time. It also
    /// reads through `overlay`, so a parent root staged earlier in this write
    /// set is a valid parent for a child commit.
    pub(crate) async fn merge_and_stage_ordered_parent_mutations<'a, I>(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
        mutation_count: usize,
        file_delete_cascades: &BTreeMap<String, TrackedStateDeltaRef<'a>>,
        mutations: I,
        commit_id: Option<&str>,
    ) -> Result<(TrackedStateApplyResult, usize), LixError>
    where
        I: IntoIterator<Item = Result<TrackedStateRootMutationRef<'a>, LixError>>,
    {
        let mut parent_entries = OrderedLeafCursor::new(*root_id.as_bytes());
        let mut mutations = PendingRootMutationCursor::new(mutations.into_iter());
        let mut next_mutation = mutations.next_pending()?;
        let mut assembler = OrderedTreeAssembler::new(&self.options, mutation_count);
        let mut cascaded_rows = 0usize;

        let mut next_parent_entry = parent_entries.next(self, store, overlay).await?;
        while let Some(parent_entry) = next_parent_entry.take() {
            let Some(mutation) = next_mutation.take() else {
                let (parent_entry, cascaded) =
                    cascade_parent_entry(parent_entry, file_delete_cascades)?;
                cascaded_rows += usize::from(cascaded);
                assembler.push(parent_entry)?;
                next_parent_entry = parent_entries.next(self, store, overlay).await?;
                continue;
            };
            match mutation.encoded_key.as_ref().cmp(parent_entry.key.as_ref()) {
                std::cmp::Ordering::Less => {
                    let created_at = mutation.delta.created_at;
                    assembler.push_mutation(mutation, created_at)?;
                    next_mutation = mutations.next_pending()?;
                    next_parent_entry = Some(parent_entry);
                }
                std::cmp::Ordering::Equal => {
                    let parent_value = decode_value(&parent_entry.value)?;
                    if mutation.require_absence && !parent_value.deleted() {
                        return Err(duplicate_root_insert_error(&mutation.delta));
                    }
                    assembler.push_mutation(mutation, parent_value.created_at())?;
                    next_mutation = mutations.next_pending()?;
                    next_parent_entry = parent_entries.next(self, store, overlay).await?;
                }
                std::cmp::Ordering::Greater => {
                    let (parent_entry, cascaded) =
                        cascade_parent_entry(parent_entry, file_delete_cascades)?;
                    cascaded_rows += usize::from(cascaded);
                    assembler.push(parent_entry)?;
                    next_mutation = Some(mutation);
                    next_parent_entry = parent_entries.next(self, store, overlay).await?;
                }
            }
        }

        while let Some(mutation) = next_mutation {
            let created_at = mutation.delta.created_at;
            assembler.push_mutation(mutation, created_at)?;
            next_mutation = mutations.next_pending()?;
        }
        let actual_mutation_count = mutations.consumed_count();
        if actual_mutation_count != mutation_count {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked-state ordered bulk mutation count mismatch: expected {mutation_count}, received {actual_mutation_count}"
                ),
            ));
        }

        let built = assembler.finish(self)?;
        let result = self
            .persist_built_tree(store, writes, overlay, built, commit_id)
            .await?;
        Ok((result, cascaded_rows))
    }

    /// Returns true when every sorted incoming key is strictly beyond the
    /// parent's right edge. The regular patcher already has a specialized
    /// append-only path that reuses existing chunks, so dense rebuilding must
    /// leave that case alone.
    pub(crate) async fn first_key_is_after_root_right_edge(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
        first_key: &[u8],
    ) -> Result<bool, LixError> {
        let mut current = *root_id.as_bytes();
        loop {
            match self
                .load_node_with_overlay(store, overlay, &current)
                .await?
            {
                DecodedNode::Leaf(leaf) => {
                    return Ok(leaf.last_key().is_some_and(|last_key| first_key > last_key));
                }
                DecodedNode::Internal(internal) => {
                    let Some(child) = internal.children().last() else {
                        return Ok(false);
                    };
                    current = child.child_hash;
                }
            }
        }
    }

    async fn diff_nodes(
        &self,
        store: &impl StorageAdapterRead,
        left_hash: [u8; TRACKED_STATE_HASH_BYTES],
        right_hash: [u8; TRACKED_STATE_HASH_BYTES],
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        if left_hash == right_hash {
            return Ok(());
        }

        let left = self.load_node(store, &left_hash).await?;
        let right = self.load_node(store, &right_hash).await?;
        if let (DecodedNode::Leaf(left), DecodedNode::Leaf(right)) = (&left, &right) {
            return self.diff_decoded_leaves(left, right, request, out);
        }

        let mut left = node_diff_frontier(left_hash, left)?;
        let mut right = node_diff_frontier(right_hash, right)?;
        let mut left_window = Vec::new();
        let mut right_window = Vec::new();
        let mut left_loaded = None;
        let mut right_loaded = None;

        loop {
            match (left.front().cloned(), right.front().cloned()) {
                (Some(left_node), Some(right_node))
                    if left_node.child_hash == right_node.child_hash =>
                {
                    self.diff_leaf_entries(&left_window, &right_window, request, out)?;
                    left_window.clear();
                    right_window.clear();
                    left.pop_front();
                    right.pop_front();
                    left_loaded = None;
                    right_loaded = None;
                }
                (Some(left_summary), Some(right_summary)) => {
                    let left_node = match left_loaded.take() {
                        Some(node) => node,
                        None => self.load_node(store, &left_summary.child_hash).await?,
                    };
                    let right_node = match right_loaded.take() {
                        Some(node) => node,
                        None => self.load_node(store, &right_summary.child_hash).await?,
                    };
                    match (left_node, right_node) {
                        (DecodedNode::Internal(left_node), DecodedNode::Internal(right_node)) => {
                            replace_front_with_children(&mut left, left_node.into_children())?;
                            replace_front_with_children(&mut right, right_node.into_children())?;
                        }
                        (DecodedNode::Internal(left_node), right_node @ DecodedNode::Leaf(_)) => {
                            replace_front_with_children(&mut left, left_node.into_children())?;
                            right_loaded = Some(right_node);
                        }
                        (left_node @ DecodedNode::Leaf(_), DecodedNode::Internal(right_node)) => {
                            left_loaded = Some(left_node);
                            replace_front_with_children(&mut right, right_node.into_children())?;
                        }
                        (DecodedNode::Leaf(left_node), DecodedNode::Leaf(right_node)) => {
                            match left_summary.last_key.cmp(&right_summary.last_key) {
                                std::cmp::Ordering::Less => {
                                    left_window.extend(left_node.into_entries());
                                    left.pop_front();
                                    right_loaded = Some(DecodedNode::Leaf(right_node));
                                }
                                std::cmp::Ordering::Greater => {
                                    left_loaded = Some(DecodedNode::Leaf(left_node));
                                    right_window.extend(right_node.into_entries());
                                    right.pop_front();
                                }
                                std::cmp::Ordering::Equal => {
                                    left.pop_front();
                                    right.pop_front();
                                    if left_window.is_empty() && right_window.is_empty() {
                                        self.diff_decoded_leaves(
                                            &left_node,
                                            &right_node,
                                            request,
                                            out,
                                        )?;
                                    } else {
                                        left_window.extend(left_node.into_entries());
                                        right_window.extend(right_node.into_entries());
                                        self.diff_leaf_entries(
                                            &left_window,
                                            &right_window,
                                            request,
                                            out,
                                        )?;
                                        left_window.clear();
                                        right_window.clear();
                                    }
                                }
                            }
                        }
                    }
                }
                (Some(left_summary), None) => {
                    let left_node = match left_loaded.take() {
                        Some(node) => node,
                        None => self.load_node(store, &left_summary.child_hash).await?,
                    };
                    match left_node {
                        DecodedNode::Internal(node) => {
                            replace_front_with_children(&mut left, node.into_children())?;
                        }
                        DecodedNode::Leaf(node) => {
                            left_window.extend(node.into_entries());
                            left.pop_front();
                        }
                    }
                }
                (None, Some(right_summary)) => {
                    let right_node = match right_loaded.take() {
                        Some(node) => node,
                        None => self.load_node(store, &right_summary.child_hash).await?,
                    };
                    match right_node {
                        DecodedNode::Internal(node) => {
                            replace_front_with_children(&mut right, node.into_children())?;
                        }
                        DecodedNode::Leaf(node) => {
                            right_window.extend(node.into_entries());
                            right.pop_front();
                        }
                    }
                }
                (None, None) => {
                    self.diff_leaf_entries(&left_window, &right_window, request, out)?;
                    return Ok(());
                }
            }
        }
    }

    fn diff_leaf_entries(
        &self,
        left: &[EncodedLeafEntry],
        right: &[EncodedLeafEntry],
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        let mut left_index = 0usize;
        let mut right_index = 0usize;
        while left_index < left.len() && right_index < right.len() {
            match left[left_index].key.cmp(&right[right_index].key) {
                std::cmp::Ordering::Less => {
                    self.push_removed_diff(left[left_index].clone(), request, out)?;
                    left_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    self.push_added_diff(right[right_index].clone(), request, out)?;
                    right_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    if left[left_index].value != right[right_index].value {
                        self.push_modified_diff(
                            left[left_index].clone(),
                            right[right_index].clone(),
                            request,
                            out,
                        )?;
                    }
                    left_index += 1;
                    right_index += 1;
                }
            }
        }
        for entry in &left[left_index..] {
            self.push_removed_diff((*entry).clone(), request, out)?;
        }
        for entry in &right[right_index..] {
            self.push_added_diff((*entry).clone(), request, out)?;
        }
        Ok(())
    }

    fn diff_decoded_leaves(
        &self,
        left: &DecodedLeafNodeRef,
        right: &DecodedLeafNodeRef,
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        let mut left_index = 0usize;
        let mut right_index = 0usize;
        while left_index < left.len() && right_index < right.len() {
            let left_entry = decoded_leaf_entry_owned(left, left_index)?;
            let right_entry = decoded_leaf_entry_owned(right, right_index)?;
            match left_entry.key.cmp(&right_entry.key) {
                std::cmp::Ordering::Less => {
                    self.push_removed_diff(left_entry, request, out)?;
                    left_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    self.push_added_diff(right_entry, request, out)?;
                    right_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    if left_entry.value != right_entry.value {
                        self.push_modified_diff(left_entry, right_entry, request, out)?;
                    }
                    left_index += 1;
                    right_index += 1;
                }
            }
        }
        while left_index < left.len() {
            self.push_removed_diff(decoded_leaf_entry_owned(left, left_index)?, request, out)?;
            left_index += 1;
        }
        while right_index < right.len() {
            self.push_added_diff(decoded_leaf_entry_owned(right, right_index)?, request, out)?;
            right_index += 1;
        }
        Ok(())
    }

    #[expect(clippy::unused_self)]
    fn push_removed_diff(
        &self,
        entry: EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        let key = decode_key_shared(entry.key)?;
        let value = decode_value(&entry.value)?;
        if request.matches_ref(key.as_ref(), &value) {
            out.push_shared(key, Some(value), None);
        }
        Ok(())
    }

    #[expect(clippy::unused_self)]
    fn push_added_diff(
        &self,
        entry: EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        let key = decode_key_shared(entry.key)?;
        let value = decode_value(&entry.value)?;
        if request.matches_ref(key.as_ref(), &value) {
            out.push_shared(key, None, Some(value));
        }
        Ok(())
    }

    #[expect(clippy::unused_self)]
    fn push_modified_diff(
        &self,
        left: EncodedLeafEntry,
        right: EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut TrackedStateTreeDiffBatchBuilder,
    ) -> Result<(), LixError> {
        debug_assert_eq!(left.key, right.key);
        let key = decode_key_shared(left.key)?;
        let left_value = decode_value(&left.value)?;
        let right_value = decode_value(&right.value)?;
        if request.matches_ref(key.as_ref(), &left_value)
            || request.matches_ref(key.as_ref(), &right_value)
        {
            out.push_shared(key, Some(left_value), Some(right_value));
        }
        Ok(())
    }

    async fn persist_built_tree(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        built: BuiltTree,
        _commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        overlay.stage_chunks(store, writes, &built.chunks).await?;
        Ok(TrackedStateApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
        })
    }

    #[cfg(test)]
    fn build_tree_from_entries(
        &self,
        entries: Vec<EncodedLeafEntry>,
    ) -> Result<BuiltTree, LixError> {
        let row_count = entries.len();
        let encoded_bytes = entries.iter().fold(64usize, |bytes, entry| {
            bytes
                .saturating_add(entry.key.len())
                .saturating_add(entry.value.len())
                .saturating_add(8)
        });
        let mut chunks = PendingChunkBatchBuilder::with_data_capacity(encoded_bytes);
        let mut summaries = self.build_leaf_level(entries, &mut chunks);
        let mut tree_height = 1usize;
        while summaries.len() > 1 {
            summaries = self.build_internal_level(summaries, tree_height, &mut chunks);
            tree_height += 1;
        }
        let root = summaries.pop().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state tree tree build produced no root",
            )
        })?;
        let chunk_bytes = chunks.data.len();
        let chunks = chunks.finish();
        Ok(BuiltTree {
            root_id: TrackedStateRootId::new(root.child_hash),
            chunks,
            row_count,
            tree_height,
            chunk_bytes,
        })
    }

    #[expect(clippy::cast_possible_truncation)]
    fn build_tree_from_leaf_summaries(
        &self,
        mut leaf_summaries: Vec<ChildSummary>,
        mut chunks: PendingChunkBatchBuilder,
    ) -> Result<BuiltTree, LixError> {
        if leaf_summaries.len() > 1 {
            // The one empty leaf is the canonical empty-tree root, not a
            // child that can coexist with live rows. Append-only patching can
            // retain that sentinel beside newly built leaves unless it is
            // removed here.
            if leaf_summaries
                .iter()
                .any(|summary| summary.subtree_count != 0)
            {
                leaf_summaries.retain(|summary| summary.subtree_count != 0);
            } else {
                leaf_summaries.truncate(1);
            }
        }
        let row_count = leaf_summaries
            .iter()
            .map(|summary| summary.subtree_count as usize)
            .sum();
        let mut summaries = leaf_summaries;
        let mut tree_height = 1usize;
        while summaries.len() > 1 {
            summaries = self.build_internal_level(summaries, tree_height, &mut chunks);
            tree_height += 1;
        }
        let root = summaries.pop().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state tree build from leaves produced no root",
            )
        })?;
        let chunk_bytes = chunks.data.len();
        let chunks = chunks.finish();
        Ok(BuiltTree {
            root_id: TrackedStateRootId::new(root.child_hash),
            chunks,
            row_count,
            tree_height,
            chunk_bytes,
        })
    }

    fn build_leaf_level(
        &self,
        entries: Vec<EncodedLeafEntry>,
        chunks: &mut PendingChunkBatchBuilder,
    ) -> Vec<ChildSummary> {
        let groups = chunk_leaf_entries(entries, &self.options);
        groups
            .into_iter()
            .map(|group| {
                let subtree_count = group.entries.len() as u64;
                let first_key = group
                    .entries
                    .first()
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                let last_key = group
                    .entries
                    .last()
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                let node = encode_leaf_node(&group.entries);
                chunks.insert_node(node, first_key, last_key, subtree_count)
            })
            .collect()
    }

    fn build_internal_level(
        &self,
        children: Vec<ChildSummary>,
        level: usize,
        chunks: &mut PendingChunkBatchBuilder,
    ) -> Vec<ChildSummary> {
        let groups = chunk_internal_entries(children, &self.options, level);
        groups
            .into_iter()
            .map(|group| {
                let subtree_count = group.children.iter().map(|child| child.subtree_count).sum();
                let first_key = group
                    .children
                    .first()
                    .map(|child| child.first_key.clone())
                    .unwrap_or_default();
                let last_key = group
                    .children
                    .last()
                    .map(|child| child.last_key.clone())
                    .unwrap_or_default();
                let node = encode_internal_node(&group.children);
                chunks.insert_node(node, first_key, last_key, subtree_count)
            })
            .collect()
    }

    #[cfg(test)]
    async fn collect_leaf_entries(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let overlay = storage::TrackedStateChunkOverlay::new();
        self.collect_leaf_entries_with_overlay(store, &overlay, root_id)
            .await
    }

    #[cfg(test)]
    async fn collect_leaf_entries_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let mut out = Vec::new();
        let mut current = vec![*root_id.as_bytes()];
        while !current.is_empty() {
            let mut next = Vec::new();
            for hash in current {
                match self.load_node_with_overlay(store, overlay, &hash).await? {
                    DecodedNode::Leaf(leaf) => out.extend(leaf.into_entries()),
                    DecodedNode::Internal(internal) => {
                        next.extend(internal.children().iter().map(|child| child.child_hash));
                    }
                }
            }
            current = next;
        }
        Ok(out)
    }

    pub(crate) async fn reachable_chunk_hashes_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
    ) -> Result<HashSet<[u8; TRACKED_STATE_HASH_BYTES]>, LixError> {
        let mut reachable = HashSet::new();
        let mut pending = vec![*root_id.as_bytes()];
        while let Some(hash) = pending.pop() {
            if !reachable.insert(hash) {
                continue;
            }
            if let DecodedNode::Internal(internal) =
                self.load_node_with_overlay(store, overlay, &hash).await?
            {
                pending.extend(internal.children().iter().map(|child| child.child_hash));
            }
        }
        Ok(reachable)
    }

    fn collect_root_diff_shared<'a, S>(
        &'a self,
        store: &'a S,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        request: &'a TrackedStateTreeScanRequest,
        ranges: &'a [EncodedScanRange],
        before_side: bool,
        out: &'a mut TrackedStateTreeDiffBatchBuilder,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
    where
        S: StorageAdapterRead + ?Sized + 'a,
    {
        Box::pin(async move {
            let node = self.load_node(store, &hash).await?;
            out.reserve_exact_once(tree_diff_capacity_hint(&node, request).min(u32::MAX as usize));
            match node {
                DecodedNode::Leaf(leaf) => {
                    for index in 0..leaf.len() {
                        if scan_limit_reached(request, out.len()) {
                            break;
                        }
                        let entry = leaf.entry_owned(index).ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "tracked-state leaf entry disappeared during one-sided diff",
                            )
                        })?;
                        if !encoded_key_in_scan_ranges(&entry.key, ranges) {
                            continue;
                        }
                        if before_side {
                            self.push_removed_diff(entry, request, out)?;
                        } else {
                            self.push_added_diff(entry, request, out)?;
                        }
                    }
                }
                DecodedNode::Internal(internal) => {
                    for child in internal.children() {
                        if scan_limit_reached(request, out.len()) {
                            break;
                        }
                        if child_summary_overlaps_scan_ranges(child, ranges) {
                            self.collect_root_diff_shared(
                                store,
                                child.child_hash,
                                request,
                                ranges,
                                before_side,
                                out,
                            )
                            .await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn scan_node<'a, S>(
        &'a self,
        store: &'a S,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        request: &'a TrackedStateTreeScanRequest,
        ranges: &'a [EncodedScanRange],
        key_decode_hint: Option<ScanKeyDecodeHint<'a>>,
        rows: &'a mut Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
    where
        S: StorageAdapterRead + ?Sized + 'a,
    {
        Box::pin(async move {
            let bytes = self.load_node_bytes(store, &hash).await?;
            match decode_node_ref(&bytes)? {
                DecodedNodeRef::Leaf(leaf) => {
                    for index in 0..leaf.len() {
                        if scan_limit_reached(request, rows.len()) {
                            break;
                        }
                        let entry = leaf.entry(index).ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "tracked-state leaf entry disappeared during scan",
                            )
                        })?;
                        if !encoded_key_in_scan_ranges(entry.key, ranges) {
                            continue;
                        }
                        let key = match key_decode_hint {
                            Some(hint) => decode_key_with_trusted_prefix(
                                entry.key,
                                hint.schema_key,
                                hint.file_id,
                                hint.prefix_len,
                            )?,
                            None => decode_key(entry.key)?,
                        };
                        if key_decode_hint.is_none() && !key_matches_scan_filters(request, &key) {
                            continue;
                        }
                        let Some(value) =
                            decode_visible_value(entry.value, request.include_tombstones)?
                        else {
                            continue;
                        };
                        // Encoded ranges or key_matches_scan_filters already
                        // proved the key predicates, and decode_visible_value
                        // already enforced tombstone visibility.
                        rows.push((key, value));
                    }
                }
                DecodedNodeRef::Internal(internal) => {
                    for child in internal.children() {
                        if scan_limit_reached(request, rows.len()) {
                            break;
                        }
                        if child_summary_overlaps_scan_ranges(child, ranges) {
                            self.scan_node(
                                store,
                                child.child_hash,
                                request,
                                ranges,
                                key_decode_hint,
                                rows,
                            )
                            .await?;
                        }
                    }
                }
            }
            Ok(())
        })
    }

    fn get_many_node<'a, S>(
        &'a self,
        store: &'a S,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        encoded_keys: &'a [(usize, Bytes)],
        values: &'a mut [Option<TrackedStateIndexValue>],
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
    where
        S: StorageAdapterRead + ?Sized + 'a,
    {
        Box::pin(async move {
            if encoded_keys.is_empty() {
                return Ok(());
            }

            let bytes = self.load_node_bytes(store, &hash).await?;
            match decode_node_ref(&bytes)? {
                DecodedNodeRef::Leaf(leaf) => {
                    for (original_index, encoded_key) in encoded_keys {
                        if let Some(entry_index) = binary_search_leaf_key(&leaf, encoded_key)? {
                            let entry = leaf.entry(entry_index).ok_or_else(|| {
                                LixError::new(
                                    "LIX_ERROR_UNKNOWN",
                                    "tracked-state leaf entry disappeared during get_many",
                                )
                            })?;
                            values[*original_index] = Some(decode_value(entry.value)?);
                        }
                    }
                }
                DecodedNodeRef::Internal(internal) => {
                    let mut start = 0usize;
                    let children = internal.children();
                    for (child_index, child) in children.iter().enumerate() {
                        if start >= encoded_keys.len() {
                            break;
                        }

                        let end = if child_index + 1 == children.len() {
                            encoded_keys.len()
                        } else {
                            let mut end = start;
                            while end < encoded_keys.len()
                                && encoded_keys[end].1.as_ref() <= child.last_key.as_ref()
                            {
                                end += 1;
                            }
                            end
                        };

                        if start < end {
                            self.get_many_node(
                                store,
                                child.child_hash,
                                &encoded_keys[start..end],
                                values,
                            )
                            .await?;
                        }
                        start = end;
                    }
                }
            }
            Ok(())
        })
    }

    #[cfg(test)]
    async fn collect_summary_levels_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<Vec<ChildSummary>>, LixError> {
        let mut levels = Vec::new();
        self.collect_summary_levels_for_node_with_overlay(
            store,
            overlay,
            *root_id.as_bytes(),
            &mut levels,
        )
        .await?;
        Ok(levels)
    }

    #[cfg(test)]
    fn collect_summary_levels_for_node_with_overlay<'a, S>(
        &'a self,
        store: &'a S,
        overlay: &'a storage::TrackedStateChunkOverlay,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        levels: &'a mut Vec<Vec<ChildSummary>>,
    ) -> Pin<Box<dyn Future<Output = Result<(ChildSummary, usize), LixError>> + Send + 'a>>
    where
        S: StorageAdapterRead + ?Sized + 'a,
    {
        Box::pin(async move {
            match self.load_node_with_overlay(store, overlay, &hash).await? {
                DecodedNode::Leaf(leaf) => {
                    let summary = decoded_leaf_summary(hash, &leaf);
                    push_level_summary(levels, 0, summary.clone());
                    Ok((summary, 0))
                }
                DecodedNode::Internal(internal) => {
                    let children = internal.children().to_vec();
                    let child_height = match children.first() {
                        Some(child) => match self
                            .load_node_with_overlay(store, overlay, &child.child_hash)
                            .await?
                        {
                            DecodedNode::Leaf(_) => {
                                if levels.is_empty() {
                                    levels.push(Vec::new());
                                }
                                levels[0].extend(children.iter().cloned());
                                0
                            }
                            DecodedNode::Internal(_) => {
                                let mut child_height = None;
                                for child in &children {
                                    let (_, height) = self
                                        .collect_summary_levels_for_node_with_overlay(
                                            store,
                                            overlay,
                                            child.child_hash,
                                            levels,
                                        )
                                        .await?;
                                    child_height = Some(height);
                                }
                                child_height.unwrap_or(0)
                            }
                        },
                        None => 0,
                    };
                    let height = child_height + 1;
                    let summary = internal_summary(hash, &children)?;
                    push_level_summary(levels, height, summary.clone());
                    Ok((summary, height))
                }
            }
        })
    }

    async fn load_node(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<DecodedNode, LixError> {
        let bytes = self.load_node_bytes(store, hash).await?;
        decode_node(&bytes)
    }

    async fn load_node_bytes(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Bytes, LixError> {
        let cached = {
            let mut cache = self
                .node_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.get(hash).cloned()
        };
        if let Some(bytes) = cached {
            return Ok(bytes);
        }

        let bytes = storage::read_chunk(store, hash).await?.ok_or_else(|| {
            LixError::new("LIX_ERROR_UNKNOWN", "tracked-state tree chunk is missing")
        })?;
        // Verify once on a durable-store miss before making the bytes reusable.
        storage::verify_chunk_hash(hash, &bytes)?;
        if bytes.len() <= self.options.max_chunk_bytes {
            self.node_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .put(*hash, bytes.clone());
        }
        Ok(bytes)
    }

    async fn load_node_with_overlay(
        &self,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<DecodedNode, LixError> {
        if let Some(bytes) = overlay.staged_chunk(hash) {
            // Overlay chunks are not cached until a later durable-store read.
            storage::debug_verify_chunk_hash(hash, bytes)?;
            return decode_node(bytes);
        }
        let bytes = self.load_node_bytes(store, hash).await?;
        decode_node(&bytes)
    }
}

#[derive(Debug)]
struct BuiltTree {
    root_id: TrackedStateRootId,
    chunks: PendingChunkBatch,
    row_count: usize,
    tree_height: usize,
    chunk_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct PendingChunkSpan {
    start: usize,
    len: usize,
}

#[derive(Debug, Default)]
struct PendingChunkBatchBuilder {
    data: Vec<u8>,
    chunks: BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkSpan>,
}

impl PendingChunkBatchBuilder {
    fn with_data_capacity(data_bytes: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_bytes),
            chunks: BTreeMap::new(),
        }
    }

    fn insert_node(
        &mut self,
        node: Vec<u8>,
        first_key: Bytes,
        last_key: Bytes,
        subtree_count: u64,
    ) -> ChildSummary {
        let hash = hash_bytes(&node);
        if !self.chunks.contains_key(&hash) {
            let start = self.data.len();
            let len = node.len();
            self.data.extend_from_slice(&node);
            self.chunks.insert(hash, PendingChunkSpan { start, len });
        }
        ChildSummary {
            first_key,
            last_key,
            child_hash: hash,
            subtree_count,
        }
    }

    fn copy_chunk_from(&mut self, source: &Self, hash: &[u8; TRACKED_STATE_HASH_BYTES]) {
        if self.chunks.contains_key(hash) {
            return;
        }
        let span = source
            .chunks
            .get(hash)
            .copied()
            .expect("generated child summary must reference a pending chunk");
        let start = self.data.len();
        self.data
            .extend_from_slice(&source.data[span.start..span.start + span.len]);
        self.chunks.insert(
            *hash,
            PendingChunkSpan {
                start,
                len: span.len,
            },
        );
    }

    fn extend(&mut self, source: Self) {
        for hash in source.chunks.keys() {
            self.copy_chunk_from(&source, hash);
        }
    }

    fn finish(self) -> PendingChunkBatch {
        PendingChunkBatch::from_parts(
            Bytes::from(self.data),
            self.chunks
                .into_iter()
                .map(|(hash, span)| PendingChunk {
                    hash,
                    data_start: span.start,
                    data_len: span.len,
                })
                .collect(),
        )
    }
}

struct PendingRootMutation<'a> {
    delta: TrackedStateDeltaRef<'a>,
    require_absence: bool,
    encoded_key: Bytes,
}

/// In-order cursor over leaf entries that reads staged chunks before the
/// durable store. It retains only the internal-node path and one decoded leaf,
/// so dense commit materialization does not clone a parent root into a
/// full-workload vector before merging it with the incoming rows.
struct OrderedLeafCursor {
    pending_root: Option<[u8; TRACKED_STATE_HASH_BYTES]>,
    frames: Vec<OrderedLeafCursorFrame>,
    leaf: Option<DecodedLeafNodeRef>,
    leaf_entry_index: usize,
}

struct OrderedLeafCursorFrame {
    children: Vec<ChildSummary>,
    next_child_index: usize,
}

impl OrderedLeafCursor {
    fn new(root_hash: [u8; TRACKED_STATE_HASH_BYTES]) -> Self {
        Self {
            pending_root: Some(root_hash),
            frames: Vec::new(),
            leaf: None,
            leaf_entry_index: 0,
        }
    }

    async fn next(
        &mut self,
        tree: &TrackedStateTree,
        store: &(impl StorageAdapterRead + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
    ) -> Result<Option<EncodedLeafEntry>, LixError> {
        loop {
            if let Some(leaf) = self.leaf.as_ref() {
                if let Some(entry) = leaf.entry_owned(self.leaf_entry_index) {
                    self.leaf_entry_index += 1;
                    return Ok(Some(entry));
                }
                self.leaf = None;
                self.leaf_entry_index = 0;
            }

            let Some(hash) = self.next_node_hash() else {
                return Ok(None);
            };
            match tree.load_node_with_overlay(store, overlay, &hash).await? {
                DecodedNode::Leaf(leaf) => self.leaf = Some(leaf),
                DecodedNode::Internal(internal) => {
                    self.frames.push(OrderedLeafCursorFrame {
                        children: internal.into_children(),
                        next_child_index: 0,
                    });
                }
            }
        }
    }

    fn next_node_hash(&mut self) -> Option<[u8; TRACKED_STATE_HASH_BYTES]> {
        if let Some(root_hash) = self.pending_root.take() {
            return Some(root_hash);
        }
        loop {
            let frame = self.frames.last_mut()?;
            if let Some(child) = frame.children.get(frame.next_child_index) {
                frame.next_child_index += 1;
                return Some(child.child_hash);
            }
            self.frames.pop();
        }
    }
}

fn cascade_parent_entry(
    mut entry: EncodedLeafEntry,
    file_delete_cascades: &BTreeMap<String, TrackedStateDeltaRef<'_>>,
) -> Result<(EncodedLeafEntry, bool), LixError> {
    if file_delete_cascades.is_empty() {
        return Ok((entry, false));
    }
    let key = decode_key(&entry.key)?;
    let Some(file_id) = key.file_id.as_deref() else {
        return Ok((entry, false));
    };
    let Some(cascade) = file_delete_cascades.get(file_id) else {
        return Ok((entry, false));
    };
    let parent_value = decode_value(&entry.value)?;
    if parent_value.deleted() {
        return Ok((entry, false));
    }
    entry.value = encode_value_ref(TrackedStateIndexValueRef {
        change_id: cascade.change_id,
        commit_id: cascade.commit_id,
        deleted: true,
        created_at: parent_value.created_at(),
        updated_at: cascade.updated_at,
    })
    .into();
    Ok((entry, true))
}

const PENDING_ROOT_MUTATION_WINDOW: usize = 4096;

struct PendingRootMutationCursor<'a, I> {
    source: Box<I>,
    pending: VecDeque<PendingRootMutation<'a>>,
    consumed_count: usize,
}

impl<'a, I> PendingRootMutationCursor<'a, I>
where
    I: Iterator<Item = Result<TrackedStateRootMutationRef<'a>, LixError>>,
{
    fn new(source: I) -> Self {
        Self {
            source: Box::new(source),
            pending: VecDeque::new(),
            consumed_count: 0,
        }
    }

    fn next_pending(&mut self) -> Result<Option<PendingRootMutation<'a>>, LixError> {
        if let Some(mutation) = self.pending.pop_front() {
            self.consumed_count = self.consumed_count.saturating_add(1);
            return Ok(Some(mutation));
        }
        let mut key_arena = Vec::with_capacity(PENDING_ROOT_MUTATION_WINDOW * 96);
        let mut pending = Vec::with_capacity(PENDING_ROOT_MUTATION_WINDOW);
        for _ in 0..PENDING_ROOT_MUTATION_WINDOW {
            let Some(mutation) = self.source.next() else {
                break;
            };
            let mutation = mutation?;
            let delta = mutation.delta;
            let encoded_key = encode_key_ref_into(
                &mut key_arena,
                TrackedStateKeyRef {
                    schema_key: delta.schema_key,
                    file_id: delta.file_id,
                    row_pk: delta.row_pk,
                },
            );
            pending.push((delta, mutation.require_absence, encoded_key));
        }
        if pending.is_empty() {
            return Ok(None);
        }
        let key_arena = Bytes::from(key_arena);
        self.pending.extend(
            pending.into_iter().map(
                |(delta, require_absence, encoded_key)| PendingRootMutation {
                    delta,
                    require_absence,
                    encoded_key: key_arena.slice(encoded_key),
                },
            ),
        );
        let mutation = self.pending.pop_front();
        self.consumed_count = self
            .consumed_count
            .saturating_add(usize::from(mutation.is_some()));
        Ok(mutation)
    }

    fn consumed_count(&self) -> usize {
        self.consumed_count
    }
}

fn duplicate_root_insert_error(delta: &TrackedStateDeltaRef<'_>) -> LixError {
    let row_pk = delta
        .row_pk
        .as_json_array_text()
        .unwrap_or_else(|_| "<invalid row_pk>".to_string());
    LixError::new(
        LixError::CODE_UNIQUE,
        format!(
            "primary-key constraint violation on schema '{}': INSERT would duplicate row_pk '{row_pk}'",
            delta.schema_key
        ),
    )
}

struct OrderedTreeAssembler<'a> {
    options: &'a TrackedStateTreeOptions,
    current_leaf: OrderedLeafAccumulator,
    leaf_summaries: Vec<ChildSummary>,
    chunks: PendingChunkBatchBuilder,
}

#[derive(Debug)]
enum OrderedLeafValue {
    Shared(Bytes),
    Arena(Range<usize>),
}

#[derive(Debug)]
struct OrderedLeafEntry {
    key: Bytes,
    value: OrderedLeafValue,
}

#[derive(Debug, Default)]
struct OrderedLeafAccumulator {
    entries: Vec<OrderedLeafEntry>,
    value_arena: Vec<u8>,
    key_bytes: usize,
}

impl OrderedLeafAccumulator {
    fn into_entries(self) -> Vec<EncodedLeafEntry> {
        let value_arena = Bytes::from(self.value_arena);
        self.entries
            .into_iter()
            .map(|entry| EncodedLeafEntry {
                key: entry.key,
                value: match entry.value {
                    OrderedLeafValue::Shared(value) => value,
                    OrderedLeafValue::Arena(range) => value_arena.slice(range),
                },
            })
            .collect()
    }
}

impl<'a> OrderedTreeAssembler<'a> {
    fn new(options: &'a TrackedStateTreeOptions, mutation_count: usize) -> Self {
        Self {
            options,
            current_leaf: OrderedLeafAccumulator::default(),
            leaf_summaries: Vec::new(),
            chunks: PendingChunkBatchBuilder::with_data_capacity(mutation_count.saturating_mul(64)),
        }
    }

    fn push(&mut self, entry: EncodedLeafEntry) -> Result<(), LixError> {
        let item_size = self.prepare_key(&entry.key)?;
        self.finish_push(
            OrderedLeafEntry {
                key: entry.key,
                value: OrderedLeafValue::Shared(entry.value),
            },
            item_size,
        );
        Ok(())
    }

    fn push_mutation(
        &mut self,
        mutation: PendingRootMutation<'_>,
        created_at: crate::common::LixTimestamp,
    ) -> Result<(), LixError> {
        let item_size = self.prepare_key(&mutation.encoded_key)?;
        let value_start = self.current_leaf.value_arena.len();
        encode_value_ref_into(
            &mut self.current_leaf.value_arena,
            TrackedStateIndexValueRef {
                change_id: mutation.delta.change_id,
                commit_id: mutation.delta.commit_id,
                deleted: mutation.delta.deleted,
                created_at,
                updated_at: mutation.delta.updated_at,
            },
        );
        let value_end = self.current_leaf.value_arena.len();
        self.finish_push(
            OrderedLeafEntry {
                key: mutation.encoded_key,
                value: OrderedLeafValue::Arena(value_start..value_end),
            },
            item_size,
        );
        Ok(())
    }

    fn prepare_key(&mut self, key: &Bytes) -> Result<usize, LixError> {
        let previous_key = self
            .current_leaf
            .entries
            .last()
            .map(|previous| previous.key.as_ref())
            .or_else(|| {
                self.leaf_summaries
                    .last()
                    .map(|previous| previous.last_key.as_ref())
            });
        if previous_key.is_some_and(|previous| previous >= key.as_ref()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state ordered bulk mutation keys must be strictly ascending",
            ));
        }

        let item_size = estimate_leaf_boundary_entry_size(key.len());
        let projected_size = estimate_leaf_boundary_chunk_size(
            self.current_leaf.entries.len() + 1,
            self.current_leaf.key_bytes + key.len(),
        );
        if !self.current_leaf.entries.is_empty() && projected_size > self.options.max_chunk_bytes {
            self.flush_leaf();
        }
        Ok(item_size)
    }

    fn finish_push(&mut self, entry: OrderedLeafEntry, item_size: usize) {
        self.current_leaf.key_bytes += entry.key.len();
        self.current_leaf.entries.push(entry);
        let current_size = estimate_leaf_boundary_chunk_size(
            self.current_leaf.entries.len(),
            self.current_leaf.key_bytes,
        );
        if current_size >= self.options.min_chunk_bytes
            && (current_size >= self.options.max_chunk_bytes
                || self.current_leaf.entries.last().is_some_and(|entry| {
                    boundary_trigger(
                        &entry.key,
                        0,
                        current_size,
                        item_size,
                        self.options.target_chunk_bytes,
                    )
                }))
        {
            self.flush_leaf();
        }
    }

    fn finish(mut self, tree: &TrackedStateTree) -> Result<BuiltTree, LixError> {
        if !self.current_leaf.entries.is_empty() || self.leaf_summaries.is_empty() {
            self.flush_leaf();
        }
        tree.build_tree_from_leaf_summaries(self.leaf_summaries, self.chunks)
    }

    fn flush_leaf(&mut self) {
        let leaf = std::mem::take(&mut self.current_leaf);
        let subtree_count = leaf.entries.len() as u64;
        let first_key = leaf
            .entries
            .first()
            .map(|entry| entry.key.clone())
            .unwrap_or_default();
        let last_key = leaf
            .entries
            .last()
            .map(|entry| entry.key.clone())
            .unwrap_or_default();
        let entries = leaf.into_entries();
        let node = encode_leaf_node(&entries);
        let summary = self
            .chunks
            .insert_node(node, first_key, last_key, subtree_count);
        self.leaf_summaries.push(summary);
    }
}

struct EncodedScanRange {
    start: Vec<u8>,
    end: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy)]
struct ScanKeyDecodeHint<'a> {
    schema_key: &'a str,
    file_id: Option<&'a str>,
    prefix_len: usize,
}

fn binary_search_leaf_key(
    leaf: &DecodedLeafNodeRef,
    encoded_key: &[u8],
) -> Result<Option<usize>, LixError> {
    let mut low = 0usize;
    let mut high = leaf.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let key = leaf.key(mid).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state leaf key disappeared during binary search",
            )
        })?;
        match key.cmp(encoded_key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Equal => return Ok(Some(mid)),
            std::cmp::Ordering::Greater => high = mid,
        }
    }
    Ok(None)
}

#[derive(Debug, Default)]
struct LeafChunkAccumulator {
    entries: Vec<EncodedLeafEntry>,
    key_bytes: usize,
}

#[derive(Debug, Default)]
struct InternalChunkAccumulator {
    children: Vec<ChildSummary>,
    first_key_bytes: usize,
    last_key_bytes: usize,
}

fn chunk_leaf_entries(
    entries: Vec<EncodedLeafEntry>,
    options: &TrackedStateTreeOptions,
) -> Vec<LeafChunkAccumulator> {
    if entries.is_empty() {
        return vec![LeafChunkAccumulator::default()];
    }
    let mut groups = Vec::new();
    let mut current = LeafChunkAccumulator::default();
    for entry in entries {
        let item_size = estimate_leaf_boundary_entry_size(entry.key.len());
        let projected_size = estimate_leaf_boundary_chunk_size(
            current.entries.len() + 1,
            current.key_bytes + entry.key.len(),
        );
        if !current.entries.is_empty() && projected_size > options.max_chunk_bytes {
            groups.push(std::mem::take(&mut current));
        }

        current.key_bytes += entry.key.len();
        current.entries.push(entry);
        let current_size =
            estimate_leaf_boundary_chunk_size(current.entries.len(), current.key_bytes);
        if current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || current.entries.last().is_some_and(|entry| {
                    boundary_trigger(
                        &entry.key,
                        0,
                        current_size,
                        item_size,
                        options.target_chunk_bytes,
                    )
                }))
        {
            groups.push(std::mem::take(&mut current));
        }
    }
    if !current.entries.is_empty() {
        groups.push(current);
    }
    groups
}

fn chunk_internal_entries(
    children: Vec<ChildSummary>,
    options: &TrackedStateTreeOptions,
    level: usize,
) -> Vec<InternalChunkAccumulator> {
    let mut groups = Vec::new();
    let mut current = InternalChunkAccumulator::default();
    for child in children {
        let item_size = child.first_key.len()
            + child.last_key.len()
            + TRACKED_STATE_HASH_BYTES
            + size_of::<u64>();
        let projected_size = estimate_internal_chunk_size(
            current.children.len() + 1,
            current.first_key_bytes + child.first_key.len(),
            current.last_key_bytes + child.last_key.len(),
        );
        if !current.children.is_empty() && projected_size > options.max_chunk_bytes {
            groups.push(std::mem::take(&mut current));
        }

        current.first_key_bytes += child.first_key.len();
        current.last_key_bytes += child.last_key.len();
        current.children.push(child);
        let current_size = estimate_internal_chunk_size(
            current.children.len(),
            current.first_key_bytes,
            current.last_key_bytes,
        );
        if current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || current.children.last().is_some_and(|child| {
                    boundary_trigger(
                        &child.first_key,
                        level,
                        current_size,
                        item_size,
                        options.target_chunk_bytes,
                    )
                }))
        {
            groups.push(std::mem::take(&mut current));
        }
    }
    if !current.children.is_empty() {
        groups.push(current);
    }
    groups
}

fn estimate_leaf_chunk_size(entry_count: usize, key_bytes: usize, value_bytes: usize) -> usize {
    10 + entry_count * 12 + key_bytes + value_bytes
}

fn estimate_leaf_boundary_chunk_size(entry_count: usize, key_bytes: usize) -> usize {
    estimate_leaf_chunk_size(entry_count, key_bytes, 0)
}

fn estimate_leaf_boundary_entry_size(key_bytes: usize) -> usize {
    12 + key_bytes
}

fn estimate_internal_chunk_size(
    child_count: usize,
    first_key_bytes: usize,
    last_key_bytes: usize,
) -> usize {
    16 + child_count * (8 + TRACKED_STATE_HASH_BYTES + size_of::<u64>())
        + first_key_bytes
        + last_key_bytes
}

fn first_resync_index(
    generated: &[ChildSummary],
    existing: &[ChildSummary],
    last_mutation_key: &[u8],
) -> Option<(usize, usize)> {
    for (generated_index, generated) in generated.iter().enumerate() {
        // A matching old chunk before the final mutation key is only an
        // unchanged gap within the frontier. Resync is valid only after every
        // mutation in the batch has been emitted.
        if generated.first_key.as_ref() <= last_mutation_key {
            continue;
        }
        if let Some(existing_index) = existing.iter().position(|existing| generated == existing) {
            return Some((generated_index, existing_index));
        }
    }
    None
}

fn node_diff_frontier(
    hash: [u8; TRACKED_STATE_HASH_BYTES],
    node: DecodedNode,
) -> Result<VecDeque<ChildSummary>, LixError> {
    match node {
        DecodedNode::Leaf(leaf) => Ok(VecDeque::from([decoded_leaf_summary(hash, &leaf)])),
        DecodedNode::Internal(internal) => {
            let children = internal.into_children();
            if children.is_empty() {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "tracked-state internal node has no children",
                ));
            }
            Ok(children.into())
        }
    }
}

fn replace_front_with_children(
    frontier: &mut VecDeque<ChildSummary>,
    children: Vec<ChildSummary>,
) -> Result<(), LixError> {
    if children.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state internal node has no children",
        ));
    }
    frontier.pop_front().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state diff frontier unexpectedly became empty",
        )
    })?;
    for child in children.into_iter().rev() {
        frontier.push_front(child);
    }
    Ok(())
}

fn decoded_leaf_entry_owned(
    leaf: &DecodedLeafNodeRef,
    index: usize,
) -> Result<EncodedLeafEntry, LixError> {
    leaf.entry_owned(index).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked-state leaf entry disappeared during diff",
        )
    })
}

fn decoded_node_row_count(node: &DecodedNode) -> usize {
    match node {
        DecodedNode::Leaf(leaf) => leaf.len(),
        DecodedNode::Internal(internal) => internal
            .children()
            .iter()
            .try_fold(0_u64, |total, child| total.checked_add(child.subtree_count))
            .and_then(|total| usize::try_from(total).ok())
            .unwrap_or(0),
    }
}

fn tree_diff_capacity_hint(node: &DecodedNode, request: &TrackedStateTreeScanRequest) -> usize {
    let row_count = decoded_node_row_count(node);
    if request.include_tombstones
        && request.schema_keys.is_empty()
        && request.row_pks.is_empty()
        && request.file_ids.is_empty()
    {
        request
            .limit
            .map_or(row_count, |limit| limit.min(row_count))
    } else {
        0
    }
}

fn decoded_leaf_summary(
    hash: [u8; TRACKED_STATE_HASH_BYTES],
    leaf: &DecodedLeafNodeRef,
) -> ChildSummary {
    ChildSummary {
        first_key: leaf.first_key_owned().unwrap_or_default(),
        last_key: leaf.last_key_owned().unwrap_or_default(),
        child_hash: hash,
        subtree_count: leaf.len() as u64,
    }
}

fn validate_decoded_node_summary(
    node: &DecodedNode,
    expected: &ChildSummary,
) -> Result<(), LixError> {
    let (first_key, last_key, subtree_count) = match node {
        DecodedNode::Leaf(leaf) => (
            leaf.first_key().unwrap_or_default(),
            leaf.last_key().unwrap_or_default(),
            leaf.len() as u64,
        ),
        DecodedNode::Internal(internal) => {
            let children = internal.children();
            let first_key = children.first().map(|child| child.first_key.as_ref());
            let last_key = children.last().map(|child| child.last_key.as_ref());
            let subtree_count = children
                .iter()
                .try_fold(0_u64, |total, child| total.checked_add(child.subtree_count));
            let (Some(first_key), Some(last_key), Some(subtree_count)) =
                (first_key, last_key, subtree_count)
            else {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "tracked-state internal child summary is invalid",
                ));
            };
            (first_key, last_key, subtree_count)
        }
    };
    if first_key != expected.first_key.as_ref()
        || last_key != expected.last_key.as_ref()
        || subtree_count != expected.subtree_count
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "tracked-state child summary does not match authenticated child contents",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn internal_summary(
    hash: [u8; TRACKED_STATE_HASH_BYTES],
    children: &[ChildSummary],
) -> Result<ChildSummary, LixError> {
    let first_key = children
        .first()
        .map(|child| child.first_key.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node has no children",
            )
        })?;
    let last_key = children
        .last()
        .map(|child| child.last_key.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state internal node has no children",
            )
        })?;
    Ok(ChildSummary {
        first_key,
        last_key,
        child_hash: hash,
        subtree_count: children.iter().map(|child| child.subtree_count).sum(),
    })
}

#[cfg(test)]
fn push_level_summary(levels: &mut Vec<Vec<ChildSummary>>, level: usize, summary: ChildSummary) {
    while levels.len() <= level {
        levels.push(Vec::new());
    }
    levels[level].push(summary);
}

fn scan_ranges(request: &TrackedStateTreeScanRequest) -> Vec<EncodedScanRange> {
    if request.schema_keys.is_empty() {
        return Vec::new();
    }

    let can_bind_row = !request.row_pks.is_empty()
        && !request.file_ids.is_empty()
        && request
            .file_ids
            .iter()
            .all(|filter| !matches!(filter, NullableKeyFilter::Any));

    let mut ranges = Vec::new();
    for schema_key in &request.schema_keys {
        if can_bind_row {
            for file_filter in &request.file_ids {
                let file_id = match file_filter {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
                    NullableKeyFilter::Any => unreachable!("filtered above"),
                };
                for row_pk in &request.row_pks {
                    if !crate::tracked_state::row_pk_satisfies_bounds(
                        row_pk,
                        request.row_pk_lower.as_ref(),
                        request.row_pk_upper.as_ref(),
                    ) {
                        continue;
                    }
                    let key = TrackedStateKey {
                        schema_key: schema_key.clone(),
                        file_id: file_id.clone(),
                        row_pk: row_pk.clone(),
                    };
                    ranges.push(exact_scan_range(encode_key(&key)));
                }
            }
            continue;
        }

        if request.file_ids.is_empty()
            || request
                .file_ids
                .iter()
                .any(|filter| matches!(filter, NullableKeyFilter::Any))
        {
            ranges.push(prefix_scan_range(encode_schema_key_prefix(schema_key)));
            continue;
        }

        for file_filter in &request.file_ids {
            let (prefix, file_id) = match file_filter {
                NullableKeyFilter::Null => (encode_schema_file_prefix(schema_key, None), None),
                NullableKeyFilter::Value(file_id) => (
                    encode_schema_file_prefix(schema_key, Some(file_id)),
                    Some(file_id.clone()),
                ),
                NullableKeyFilter::Any => unreachable!("handled above"),
            };
            ranges.push(row_pk_scan_range(
                schema_key,
                file_id,
                prefix,
                request.row_pk_lower.as_ref(),
                request.row_pk_upper.as_ref(),
            ));
        }
    }
    ranges
}

fn scan_key_decode_hint<'a>(
    request: &'a TrackedStateTreeScanRequest,
    ranges: &[EncodedScanRange],
) -> Option<ScanKeyDecodeHint<'a>> {
    if ranges.len() != 1 || request.schema_keys.len() != 1 || request.file_ids.len() != 1 {
        return None;
    }
    if !request.row_pks.is_empty() {
        return None;
    }
    let file_id = match request.file_ids.first()? {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(file_id) => Some(file_id.as_str()),
        NullableKeyFilter::Any => return None,
    };
    Some(ScanKeyDecodeHint {
        schema_key: request.schema_keys.first()?.as_str(),
        file_id,
        prefix_len: encode_schema_file_prefix(request.schema_keys.first()?, file_id).len(),
    })
}

fn prefix_scan_range(prefix: Vec<u8>) -> EncodedScanRange {
    EncodedScanRange {
        end: lexicographic_successor(&prefix),
        start: prefix,
    }
}

fn exact_scan_range(key: Vec<u8>) -> EncodedScanRange {
    EncodedScanRange {
        end: lexicographic_successor(&key),
        start: key,
    }
}

fn row_pk_scan_range(
    schema_key: &str,
    file_id: Option<String>,
    prefix: Vec<u8>,
    lower: Option<&crate::tracked_state::RowPkRangeBound>,
    upper: Option<&crate::tracked_state::RowPkRangeBound>,
) -> EncodedScanRange {
    let start = lower.map_or_else(
        || prefix.clone(),
        |bound| {
            let key = encode_key(&TrackedStateKey {
                schema_key: schema_key.to_owned(),
                file_id: file_id.clone(),
                row_pk: bound.row_pk.clone(),
            });
            if bound.inclusive {
                key
            } else {
                lexicographic_successor(&key).unwrap_or(key)
            }
        },
    );
    let end = upper.map_or_else(
        || lexicographic_successor(&prefix),
        |bound| {
            let key = encode_key(&TrackedStateKey {
                schema_key: schema_key.to_owned(),
                file_id,
                row_pk: bound.row_pk.clone(),
            });
            if bound.inclusive {
                lexicographic_successor(&key)
            } else {
                Some(key)
            }
        },
    );
    EncodedScanRange { start, end }
}

fn lexicographic_successor(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut out = bytes.to_vec();
    for index in (0..out.len()).rev() {
        if out[index] != u8::MAX {
            out[index] += 1;
            out.truncate(index + 1);
            return Some(out);
        }
    }
    None
}

fn child_summary_overlaps_scan_ranges(child: &ChildSummary, ranges: &[EncodedScanRange]) -> bool {
    ranges.is_empty()
        || ranges.iter().any(|range| {
            child.last_key.as_ref() >= range.start.as_slice()
                && range
                    .end
                    .as_ref()
                    .is_none_or(|end| child.first_key.as_ref() < end.as_slice())
        })
}

fn encoded_key_in_scan_ranges(key: &[u8], ranges: &[EncodedScanRange]) -> bool {
    ranges.is_empty()
        || ranges.iter().any(|range| {
            key >= range.start.as_slice()
                && range.end.as_ref().is_none_or(|end| key < end.as_slice())
        })
}

fn key_matches_scan_filters(request: &TrackedStateTreeScanRequest, key: &TrackedStateKey) -> bool {
    if !request.schema_keys.is_empty() && !request.schema_keys.contains(&key.schema_key) {
        return false;
    }
    if !request.row_pks.is_empty() && !request.row_pks.contains(&key.row_pk) {
        return false;
    }
    if !crate::tracked_state::row_pk_satisfies_bounds(
        &key.row_pk,
        request.row_pk_lower.as_ref(),
        request.row_pk_upper.as_ref(),
    ) {
        return false;
    }
    if !request.file_ids.is_empty()
        && !request
            .file_ids
            .iter()
            .any(|filter| filter.matches(key.file_id.as_ref()))
    {
        return false;
    }
    true
}

fn scan_limit_reached(request: &TrackedStateTreeScanRequest, row_count: usize) -> bool {
    request.limit.is_some_and(|limit| row_count >= limit)
}

/// Test-only content-addressed chunks for the GC sweep fixture. The empty
/// leaf is a valid serving root; labelled leaves are valid hash-addressed
/// chunks that are intentionally not referenced by that root.
#[cfg(test)]
pub(crate) fn test_gc_leaf_chunk(label: &[u8]) -> ([u8; TRACKED_STATE_HASH_BYTES], Bytes) {
    let entries = if label.is_empty() {
        Vec::new()
    } else {
        let timestamp = crate::common::LixTimestamp::expect_parse(
            "GC fixture timestamp",
            "2026-01-01T00:00:00Z",
        );
        vec![EncodedLeafEntry {
            key: Bytes::copy_from_slice(label),
            value: Bytes::from(encode_value_ref(TrackedStateIndexValueRef {
                change_id: ChangeId::for_test_label("gc-fixture-change"),
                commit_id: CommitId::for_test_label("gc-fixture-commit"),
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
            })),
        }]
    };
    let bytes = Bytes::from(encode_leaf_node(&entries));
    (hash_bytes(&bytes), bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use crate::changelog::{ChangeId, CommitId};
    use crate::row_pk::RowPk;
    use crate::storage::{
        BeginScanOptions, GetManyResult, KeyRange, ProjectedValue, ScanCursor, Storage,
        StorageError, StorageRead,
    };
    use crate::storage_adapter::{Memory, StorageReadOptions, StorageWriteOptions};
    use crate::storage_adapter::{StorageAdapter, StorageAdapterReadScope};
    use crate::tracked_state::codec::{encode_value, hash_bytes};

    #[test]
    fn schema_inventory_rejects_too_low_routing_boundary() {
        let key = encode_key(&TrackedStateKey {
            schema_key: "private_schema".to_string(),
            file_id: Some("file-a".to_string()),
            row_pk: RowPk::single("row-a"),
        });
        let value = encode_value(&value("change-a", Some("present")));
        let bytes = Bytes::from(encode_leaf_node(&[EncodedLeafEntry {
            key: Bytes::from(key.clone()),
            value: Bytes::from(value),
        }]));
        let node = decode_node(&bytes).expect("fixture leaf should decode");
        let error = validate_decoded_node_summary(
            &node,
            &ChildSummary {
                first_key: Bytes::from(key),
                last_key: Bytes::from_static(b"forged-too-low"),
                child_hash: hash_bytes(&bytes),
                subtree_count: 1,
            },
        )
        .expect_err("hash-valid child with substituted boundary must fail closed");
        assert!(error.message.contains("child summary does not match"));
    }

    struct CountingChunkRead {
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        bytes: Vec<u8>,
        storage_reads: Arc<AtomicUsize>,
        corrupt_first_read: bool,
    }

    struct CountingStorageRead<R> {
        read: R,
        tree_chunk_reads: Arc<AtomicUsize>,
    }

    impl<R> StorageRead for CountingStorageRead<R>
    where
        R: StorageRead,
    {
        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
            for request in requests {
                if request.space == storage::TRACKED_STATE_TREE_CHUNK_SPACE {
                    self.tree_chunk_reads
                        .fetch_add(request.keys.len(), Ordering::Relaxed);
                }
            }
            self.read.get_many(requests)
        }

        fn begin_scan(
            &self,
            space: crate::storage::StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
            self.read.begin_scan(space, range, opts)
        }
    }

    impl StorageRead for CountingChunkRead {
        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
            async move {
                assert!(
                    requests
                        .iter()
                        .all(|request| request.space == storage::TRACKED_STATE_TREE_CHUNK_SPACE)
                );
                let read_index = self.storage_reads.fetch_add(1, Ordering::Relaxed);
                let bytes = if self.corrupt_first_read && read_index == 0 {
                    Bytes::from_static(b"corrupt tracked-state node")
                } else {
                    Bytes::copy_from_slice(&self.bytes)
                };
                Ok(GetManyResult::new(
                    requests
                        .iter()
                        .flat_map(|request| request.keys)
                        .map(|key| {
                            (key.0.as_ref() == self.hash)
                                .then(|| ProjectedValue::FullValue(bytes.clone()))
                        })
                        .collect(),
                ))
            }
        }

        fn begin_scan(
            &self,
            _space: crate::storage::StorageSpace,
            _range: KeyRange,
            _opts: BeginScanOptions,
        ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
            async { unreachable!("tracked-state node cache test only performs point reads") }
        }
    }

    #[tokio::test]
    async fn repeated_node_loads_from_tree_clones_avoid_storage_reads() {
        let bytes = encode_leaf_node(&[]);
        let hash = hash_bytes(&bytes);
        let storage_reads = Arc::new(AtomicUsize::new(0));
        let store = StorageAdapterReadScope::new(CountingChunkRead {
            hash,
            bytes,
            storage_reads: Arc::clone(&storage_reads),
            corrupt_first_read: false,
        });
        let tree = TrackedStateTree::new();
        let cloned_tree = tree.clone();

        assert!(matches!(
            tree.load_node(&store, &hash)
                .await
                .expect("first node load should succeed"),
            DecodedNode::Leaf(_)
        ));
        assert!(matches!(
            cloned_tree
                .load_node(&store, &hash)
                .await
                .expect("cached node load should succeed"),
            DecodedNode::Leaf(_)
        ));

        assert_eq!(storage_reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn exact_candidates_outside_primary_key_bounds_do_zero_tree_reads() {
        let bytes = encode_leaf_node(&[]);
        let hash = hash_bytes(&bytes);
        let storage_reads = Arc::new(AtomicUsize::new(0));
        let store = StorageAdapterReadScope::new(CountingChunkRead {
            hash,
            bytes,
            storage_reads: Arc::clone(&storage_reads),
            corrupt_first_read: false,
        });
        let rows = TrackedStateTree::new()
            .scan(
                &store,
                &TrackedStateRootId::new(hash),
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema".to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    row_pks: vec![RowPk::single("a"), RowPk::single("b")],
                    row_pk_lower: Some(crate::tracked_state::RowPkRangeBound {
                        row_pk: RowPk::single("c"),
                        inclusive: true,
                    }),
                    row_pk_upper: Some(crate::tracked_state::RowPkRangeBound {
                        row_pk: RowPk::single("d"),
                        inclusive: true,
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("empty exact/range intersection should short circuit");
        assert!(rows.is_empty());
        assert_eq!(storage_reads.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn oversized_node_loads_are_not_cached() {
        let bytes = encode_leaf_node(&[]);
        assert!(bytes.len() > 1, "test node must have an encoded body");
        let hash = hash_bytes(&bytes);
        let storage_reads = Arc::new(AtomicUsize::new(0));
        let store = StorageAdapterReadScope::new(CountingChunkRead {
            hash,
            bytes: bytes.clone(),
            storage_reads: Arc::clone(&storage_reads),
            corrupt_first_read: false,
        });
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: bytes.len() - 1,
            min_chunk_bytes: 1,
            max_chunk_bytes: bytes.len() - 1,
        });

        tree.load_node(&store, &hash)
            .await
            .expect("first oversized node load should succeed");
        tree.load_node(&store, &hash)
            .await
            .expect("second oversized node load should succeed");

        assert_eq!(storage_reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn corrupt_node_miss_does_not_poison_cache() {
        let bytes = encode_leaf_node(&[]);
        let hash = hash_bytes(&bytes);
        let storage_reads = Arc::new(AtomicUsize::new(0));
        let store = StorageAdapterReadScope::new(CountingChunkRead {
            hash,
            bytes,
            storage_reads: Arc::clone(&storage_reads),
            corrupt_first_read: true,
        });
        let tree = TrackedStateTree::new();

        let error = tree
            .load_node(&store, &hash)
            .await
            .expect_err("corrupt node load should fail");
        assert!(error.message.contains("chunk hash mismatch"));
        tree.load_node(&store, &hash)
            .await
            .expect("valid retry should succeed");
        tree.load_node(&store, &hash)
            .await
            .expect("validated node should be cached");

        assert_eq!(storage_reads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn sparse_diff_reads_only_the_changed_path() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let builder = TrackedStateTree::new();
        let rows = (0..10_000)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("row-{index:05}")),
                    value(&format!("change-{index}"), Some("{}")),
                )
            })
            .collect::<Vec<_>>();
        let base = apply_mutations_for_test(&builder, &storage, None, rows, None)
            .await
            .expect("base should build");
        let updated = apply_mutations_for_test(
            &builder,
            &storage,
            Some(&base.root_id),
            vec![mutation_owned(
                key("schema", None, "row-05000"),
                value("change-updated", Some("{}")),
            )],
            None,
        )
        .await
        .expect("sparse update should build");
        assert_eq!(base.tree_height, updated.tree_height);
        assert!(base.tree_height > 1, "fixture must have internal nodes");
        let inserted = apply_mutations_for_test(
            &builder,
            &storage,
            Some(&base.root_id),
            vec![mutation_owned(
                key("schema", None, "row-10000"),
                value("change-inserted", Some("{}")),
            )],
            None,
        )
        .await
        .expect("sparse append should build");

        let tree_chunk_reads = Arc::new(AtomicUsize::new(0));
        let read = memory
            .begin_read(crate::storage::ReadOptions::default())
            .await
            .expect("read should open");
        let store = StorageAdapterReadScope::new(CountingStorageRead {
            read,
            tree_chunk_reads: Arc::clone(&tree_chunk_reads),
        });
        let cold_tree = TrackedStateTree::new();
        let request = TrackedStateTreeScanRequest::default();

        let identical = cold_tree
            .diff(&store, Some(&base.root_id), Some(&base.root_id), &request)
            .await
            .expect("identical-root diff should run");
        assert!(identical.is_empty());
        assert_eq!(tree_chunk_reads.load(Ordering::Relaxed), 0);

        let sparse = cold_tree
            .diff(
                &store,
                Some(&base.root_id),
                Some(&updated.root_id),
                &request,
            )
            .await
            .expect("sparse diff should run");
        assert_eq!(sparse.len(), 1);
        assert!(
            sparse.row_capacity() <= 16,
            "one emitted row retained capacity for the 10k-row root: {}",
            sparse.row_capacity()
        );
        assert_eq!(
            tree_chunk_reads.load(Ordering::Relaxed),
            base.tree_height * 2,
            "one value update should read one node from each root per level"
        );

        tree_chunk_reads.store(0, Ordering::Relaxed);
        let cold_insert_tree = TrackedStateTree::new();
        let inserted_diff = cold_insert_tree
            .diff(
                &store,
                Some(&base.root_id),
                Some(&inserted.root_id),
                &request,
            )
            .await
            .expect("sparse append diff should run");
        assert_eq!(inserted_diff.len(), 1);
        let insert_reads = tree_chunk_reads.load(Ordering::Relaxed);
        let max_height = base.tree_height.max(inserted.tree_height);
        assert!(
            insert_reads <= max_height * 2 + 4,
            "one appended key read {insert_reads} chunks across height {max_height}"
        );
    }

    #[tokio::test]
    async fn repeated_existing_updates_keep_tree_shape_and_bounded_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let rows = (0..10_000)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("row-{index:05}")),
                    value(&format!("change-{index}"), Some("{}")),
                )
            })
            .collect::<Vec<_>>();
        let base = apply_mutations_for_test(&tree, &storage, None, rows, None)
            .await
            .expect("base should build");
        let mut current = base.root_id.clone();
        for index in 0..100 {
            let updated = apply_mutations_for_test(
                &tree,
                &storage,
                Some(&current),
                vec![mutation_owned(
                    key("schema", None, &format!("row-{:05}", 5_000 + index)),
                    value(&format!("updated-{index}"), Some("{}")),
                )],
                None,
            )
            .await
            .expect("existing update should path-copy");
            assert_eq!(updated.row_count, 10_000);
            assert_eq!(updated.tree_height, base.tree_height);
            assert!(
                updated.chunk_count <= base.tree_height,
                "one existing update wrote {} chunks for height {}",
                updated.chunk_count,
                base.tree_height
            );
            current = updated.root_id;
        }
    }

    #[tokio::test]
    async fn hierarchical_diff_matches_naive_diff_across_shifted_boundaries() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
        });
        let base_rows = (0..512usize)
            .map(|index| {
                (
                    key("schema", None, &format!("row-{:05}", index * 2)),
                    value(&format!("base-change-{index}"), Some("{}")),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let base = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            base_rows
                .iter()
                .map(|(key, value)| mutation(key, value))
                .collect(),
            None,
        )
        .await
        .expect("deep base should build");
        assert!(
            base.tree_height >= 4,
            "fixture must exercise a deep hierarchy, got height {}",
            base.tree_height
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let overlay = storage::TrackedStateChunkOverlay::new();
        let levels = tree
            .collect_summary_levels_with_overlay(&read, &overlay, &base.root_id)
            .await
            .expect("summary levels should load");
        let leaf_summaries = levels.first().expect("base should have a leaf level");
        assert!(leaf_summaries.len() > 2, "fixture needs several leaves");
        let boundary_key = decode_key(&leaf_summaries[leaf_summaries.len() / 2].last_key)
            .expect("leaf boundary key should decode");
        let boundary_number = boundary_key
            .row_pk
            .as_single_string()
            .expect("fixture key should be scalar")
            .strip_prefix("row-")
            .expect("fixture key should have its prefix")
            .parse::<usize>()
            .expect("fixture key suffix should be numeric");
        let boundary_insert_number = boundary_number + 1;
        let middle_insert_number = if boundary_insert_number == 501 {
            503
        } else {
            501
        };

        let mut changed_rows = base_rows.clone();
        assert!(
            changed_rows.remove(&boundary_key).is_some(),
            "selected boundary key must exist in the base"
        );
        changed_rows.insert(
            key(
                "schema",
                None,
                &format!("row-{boundary_insert_number:05}"),
            ),
            value("boundary-insert", Some("{}")),
        );
        changed_rows.insert(
            key("schema", None, "row--prepend"),
            value("prepend-insert", Some("{}")),
        );
        changed_rows.insert(
            key("schema", None, &format!("row-{middle_insert_number:05}")),
            value("middle-insert", Some("{}")),
        );
        changed_rows.insert(
            key("schema", None, "row-99999"),
            value("append-insert", Some("{}")),
        );
        changed_rows.insert(
            key("schema", None, "row-00020"),
            value("updated-existing", Some("{}")),
        );
        changed_rows.insert(
            key("schema", None, "row-00040"),
            value("tombstoned-existing", None),
        );

        let changed = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            changed_rows
                .iter()
                .map(|(key, value)| mutation(key, value))
                .collect(),
            None,
        )
        .await
        .expect("changed tree should build");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("diff read should open");
        let actual = TrackedStateTree::with_options(tree.options.clone())
            .diff(
                &read,
                Some(&base.root_id),
                Some(&changed.root_id),
                &TrackedStateTreeScanRequest::default(),
            )
            .await
            .expect("hierarchical diff should run")
            .into_rows_for_test();
        let expected = naive_tree_diff(&base_rows, &changed_rows);

        assert_eq!(
            actual, expected,
            "frontier diff must match ordered map diff"
        );
    }

    #[tokio::test]
    async fn root_backed_one_sided_diff_matches_naive_diff_in_both_directions() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
        });
        let rows = (0..256usize)
            .map(|index| {
                (
                    key("schema", Some("file"), &format!("row-{index:05}")),
                    value(&format!("change-{index}"), Some("{}")),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let root = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            rows.iter()
                .map(|(key, value)| mutation(key, value))
                .collect(),
            None,
        )
        .await
        .expect("one-sided fixture should build");
        assert!(
            root.tree_height > 1,
            "fixture must exercise recursive root traversal"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("diff read should open");
        let request = TrackedStateTreeScanRequest::default();
        let added = TrackedStateTree::with_options(tree.options.clone())
            .diff(&read, None, Some(&root.root_id), &request)
            .await
            .expect("empty-to-root diff should run")
            .into_rows_for_test();
        let removed = TrackedStateTree::with_options(tree.options.clone())
            .diff(&read, Some(&root.root_id), None, &request)
            .await
            .expect("root-to-empty diff should run")
            .into_rows_for_test();
        let empty = BTreeMap::new();

        assert_eq!(added, naive_tree_diff(&empty, &rows));
        assert_eq!(removed, naive_tree_diff(&rows, &empty));
    }

    #[tokio::test]
    async fn hierarchical_diff_handles_root_height_transition() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
        });
        let mut previous: Option<TrackedStateApplyResult> = None;
        let mut transition = None;

        for index in 0..2_048usize {
            let inserted_key = key("schema", None, &format!("row-{index:05}"));
            let inserted_value = value(&format!("change-{index}"), Some("{}"));
            let next = apply_mutations_for_test(
                &tree,
                &storage,
                previous.as_ref().map(|result| &result.root_id),
                vec![mutation(&inserted_key, &inserted_value)],
                None,
            )
            .await
            .expect("incremental append should build");
            if let Some(before) = previous.as_ref()
                && before.tree_height >= 3
                && next.tree_height > before.tree_height
            {
                transition = Some((before.clone(), next, inserted_key, inserted_value));
                break;
            }
            previous = Some(next);
        }

        let (before, after, inserted_key, inserted_value) =
            transition.expect("fixture should cross a root-height boundary");
        assert_eq!(after.tree_height, before.tree_height + 1);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("diff read should open");
        let actual = TrackedStateTree::with_options(tree.options.clone())
            .diff(
                &read,
                Some(&before.root_id),
                Some(&after.root_id),
                &TrackedStateTreeScanRequest::default(),
            )
            .await
            .expect("height-mismatched diff should run")
            .into_rows_for_test();

        assert_eq!(
            actual,
            vec![TrackedStateTreeDiffEntry {
                key: inserted_key,
                before: None,
                after: Some(inserted_value),
            }]
        );
    }

    #[tokio::test]
    async fn exact_read_roundtrips_from_applied_root() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "row");
        let value = value("change-1", Some("{}"));
        let result =
            apply_mutations_for_test(&tree, &storage, None, vec![mutation(&key, &value)], None)
                .await
                .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            tree.get(&store, &result.root_id, &key)
                .await
                .expect("row should load"),
            Some(value)
        );
    }

    #[tokio::test]
    async fn v3_keys_route_through_multilevel_gets_and_prefix_scans() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
        });
        let rows = (0..96usize)
            .map(|index| {
                let schema_key = match index % 3 {
                    0 => "schema",
                    1 => "schema\0",
                    _ => "schéma",
                };
                let file_id = match index % 4 {
                    0 => None,
                    1 => Some(""),
                    2 => Some("file\0"),
                    _ => Some("文件"),
                };
                let row_pk = if index % 2 == 0 {
                    RowPk::single(format!("row-{index:03}"))
                } else {
                    RowPk::from_parts_unchecked(vec![
                        format!("row-{index:03}"),
                        "尾-".to_string(),
                    ])
                };
                let key = TrackedStateKey {
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                    row_pk,
                };
                let value = value(&format!("change-{index}"), Some("{}"));
                (key, value)
            })
            .collect::<Vec<_>>();
        let mutations = rows
            .iter()
            .map(|(key, value)| mutation(key, value))
            .collect();
        let result = apply_mutations_for_test(&tree, &storage, None, mutations, None)
            .await
            .expect("v3-key mutations should apply");
        assert!(
            result.tree_height > 1,
            "fixture must exercise internal nodes"
        );

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let keys = rows.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        assert_eq!(
            tree.get_many(&store, &result.root_id, &keys)
                .await
                .expect("all exact keys should load"),
            rows.iter()
                .map(|(_, value)| Some(value.clone()))
                .collect::<Vec<_>>()
        );

        let scanned = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema\0".to_string()],
                    file_ids: vec![NullableKeyFilter::Value(String::new())],
                    ..Default::default()
                },
            )
            .await
            .expect("schema/file prefix scan should succeed");
        let expected = rows
            .iter()
            .filter(|(key, _)| key.schema_key == "schema\0" && key.file_id.as_deref() == Some(""))
            .count();
        assert_eq!(scanned.len(), expected);
        assert!(scanned.iter().all(|(key, _)| {
            key.schema_key == "schema\0" && key.file_id.as_deref() == Some("")
        }));
    }

    #[tokio::test]
    async fn latest_mutation_for_key_wins() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "row");
        let old_value = value("change-old", Some("{\"v\":1}"));
        let new_value = value("change-new", Some("{\"v\":2}"));
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![mutation(&key, &old_value), mutation(&key, &new_value)],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let loaded = tree
            .get(&store, &result.root_id, &key)
            .await
            .expect("row should load")
            .expect("row should exist");
        assert_eq!(loaded.change_id, "change-new");
        assert_eq!(loaded.commit_id, "commit");
    }

    #[tokio::test]
    async fn scan_filters_by_index_key_without_materializing_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation_owned(key("schema-a", None, "visible"), value("c1", Some("{}"))),
                mutation_owned(key("schema-a", None, "deleted"), value("c2", None)),
                mutation_owned(key("schema-b", None, "other"), value("c3", Some("{}"))),
            ],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");
        assert_eq!(rows.len(), 2);
        let identities = rows
            .iter()
            .map(|(key, _)| key.row_pk.as_single_string_owned().expect("identity"))
            .collect::<Vec<_>>();
        assert_eq!(identities, vec!["deleted", "visible"]);

        let live_rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    include_tombstones: false,
                    ..Default::default()
                },
            )
            .await
            .expect("live scan should succeed");
        let live_identities = live_rows
            .iter()
            .map(|(key, _)| key.row_pk.as_single_string_owned().expect("identity"))
            .collect::<Vec<_>>();
        assert_eq!(live_identities, vec!["visible"]);
    }

    #[tokio::test]
    async fn scan_filters_by_schema_row_and_file() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-a",
                    ),
                    value("c1", Some("{}")),
                ),
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000b2"),
                        "row-a",
                    ),
                    value("c2", Some("{}")),
                ),
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-b",
                    ),
                    value("c3", Some("{}")),
                ),
                mutation_owned(
                    key(
                        "schema-b",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-a",
                    ),
                    value("c4", Some("{}")),
                ),
            ],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    row_pks: vec![RowPk::single("row-a")],
                    file_ids: vec![NullableKeyFilter::Value(
                        "01920000-0000-7000-8000-0000000000a2".to_string(),
                    )],
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.schema_key, "schema-a");
        assert_eq!(
            rows[0]
                .0
                .row_pk
                .as_single_string_owned()
                .expect("identity"),
            "row-a"
        );
        assert_eq!(
            rows[0].0.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000a2")
        );

        let bounded_exact_rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    row_pks: vec![RowPk::single("row-a"), RowPk::single("row-b")],
                    file_ids: vec![NullableKeyFilter::Value(
                        "01920000-0000-7000-8000-0000000000a2".to_string(),
                    )],
                    row_pk_lower: Some(crate::tracked_state::RowPkRangeBound {
                        row_pk: RowPk::single("row-b"),
                        inclusive: true,
                    }),
                    row_pk_upper: Some(crate::tracked_state::RowPkRangeBound {
                        row_pk: RowPk::single("row-b"),
                        inclusive: true,
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("exact candidates must intersect range bounds");
        assert_eq!(bounded_exact_rows.len(), 1);
        assert_eq!(
            bounded_exact_rows[0]
                .0
                .row_pk
                .as_single_string_owned()
                .expect("identity"),
            "row-b"
        );

        // With no schema predicate there is no encoded scan range or trusted
        // prefix. This exercises the decoded-key filter path directly.
        let row_only_rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    row_pks: vec![RowPk::single("row-b")],
                    ..Default::default()
                },
            )
            .await
            .expect("row-only scan should succeed");
        assert_eq!(row_only_rows.len(), 1);
        assert_eq!(row_only_rows[0].0.schema_key, "schema-a");
        assert_eq!(
            row_only_rows[0].0.file_id.as_deref(),
            Some("01920000-0000-7000-8000-0000000000a2")
        );
    }

    #[tokio::test]
    async fn scan_schema_file_prefix_honors_tombstones_and_limit() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-a",
                    ),
                    value("c1", Some("{}")),
                ),
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-b",
                    ),
                    value("c2", None),
                ),
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000a2"),
                        "row-c",
                    ),
                    value("c3", Some("{}")),
                ),
                mutation_owned(
                    key(
                        "schema-a",
                        Some("01920000-0000-7000-8000-0000000000b2"),
                        "row-d",
                    ),
                    value("c4", Some("{}")),
                ),
            ],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    file_ids: vec![NullableKeyFilter::Value(
                        "01920000-0000-7000-8000-0000000000a2".to_string(),
                    )],
                    include_tombstones: false,
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(key, _)| key.schema_key == "schema-a"
            && key.file_id.as_deref() == Some("01920000-0000-7000-8000-0000000000a2")));
        assert_eq!(
            rows.iter()
                .map(|(key, _)| key.row_pk.as_single_string_owned().expect("identity"))
                .collect::<Vec<_>>(),
            vec!["row-a", "row-c"]
        );
    }

    #[tokio::test]
    async fn scan_schema_file_primary_key_range_honors_open_closed_and_empty_bounds() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            ["row-a", "row-b", "row-c", "row-d"]
                .into_iter()
                .enumerate()
                .map(|(index, row_pk)| {
                    mutation_owned(
                        key("schema-a", Some(file_id), row_pk),
                        value(&format!("range-c{index}"), Some("{}")),
                    )
                })
                .collect(),
            None,
        )
        .await
        .expect("range fixture should apply");
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("range read should open");
        let request = |lower: (&str, bool), upper: (&str, bool)| {
            TrackedStateTreeScanRequest {
                schema_keys: vec!["schema-a".to_owned()],
                file_ids: vec![NullableKeyFilter::Value(file_id.to_owned())],
                row_pk_lower: Some(crate::tracked_state::RowPkRangeBound {
                    row_pk: RowPk::single(lower.0),
                    inclusive: lower.1,
                }),
                row_pk_upper: Some(crate::tracked_state::RowPkRangeBound {
                    row_pk: RowPk::single(upper.0),
                    inclusive: upper.1,
                }),
                ..Default::default()
            }
        };

        let rows = tree
            .scan(&store, &result.root_id, &request(("row-a", false), ("row-d", false)))
            .await
            .expect("open range should scan");
        assert_eq!(
            rows.into_iter()
                .map(|(key, _)| key.row_pk.into_parts())
                .collect::<Vec<_>>(),
            vec![vec!["row-b"], vec!["row-c"]]
        );

        let rows = tree
            .scan(&store, &result.root_id, &request(("row-b", true), ("row-c", true)))
            .await
            .expect("closed range should scan");
        assert_eq!(
            rows.into_iter()
                .map(|(key, _)| key.row_pk.into_parts())
                .collect::<Vec<_>>(),
            vec![vec!["row-b"], vec!["row-c"]]
        );

        assert!(
            tree.scan(&store, &result.root_id, &request(("row-d", true), ("row-a", true)))
                .await
                .expect("empty inverted range should not fail")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn applying_to_base_root_reuses_existing_rows_and_overwrites_changed_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let unchanged_key = key("schema", None, "unchanged");
        let changed_key = key("schema", None, "changed");
        let unchanged_value = value("c1", Some("{}"));
        let old_changed_value = value("c2", Some("{\"old\":true}"));
        let new_changed_value = value("c3", Some("{\"new\":true}"));
        let base = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation(&unchanged_key, &unchanged_value),
                mutation(&changed_key, &old_changed_value),
            ],
            None,
        )
        .await
        .expect("base should build");
        let next = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![mutation(&changed_key, &new_changed_value)],
            None,
        )
        .await
        .expect("next should build");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            tree.get(&store, &next.root_id, &unchanged_key)
                .await
                .expect("unchanged read")
                .expect("unchanged exists")
                .change_id,
            "c1"
        );
        assert_eq!(
            tree.get(&store, &next.root_id, &changed_key)
                .await
                .expect("changed read")
                .expect("changed exists")
                .change_id,
            "c3"
        );
    }

    #[tokio::test]
    async fn two_commit_roots_can_share_unchanged_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::new();
        let shared_key = key("schema", None, "shared");
        let branch_a_key = key("schema", None, "01920000-0000-7000-8000-0000000000a1");
        let branch_b_key = key("schema", None, "01920000-0000-7000-8000-0000000000b1");
        let shared_value = value("shared-change", Some("{\"shared\":true}"));
        let branch_a_value = value(
            "01920000-0000-7000-8000-0000000000a1-change",
            Some("{\"branch\":\"a\"}"),
        );
        let branch_b_value = value(
            "01920000-0000-7000-8000-0000000000b1-change",
            Some("{\"branch\":\"b\"}"),
        );
        let base = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![mutation(&shared_key, &shared_value)],
            Some("commit-base"),
        )
        .await
        .expect("base root should build");
        let branch_a = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![mutation(&branch_a_key, &branch_a_value)],
            Some("commit-a"),
        )
        .await
        .expect("branch a root should build");
        let branch_b = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![mutation(&branch_b_key, &branch_b_value)],
            Some("commit-b"),
        )
        .await
        .expect("branch b root should build");

        assert_ne!(branch_a.root_id, branch_b.root_id);
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            tree.get(&store, &branch_a.root_id, &shared_key)
                .await
                .expect("branch a shared row should load"),
            Some(value("shared-change", Some("{\"shared\":true}")))
        );
        assert_eq!(
            tree.get(&store, &branch_b.root_id, &shared_key)
                .await
                .expect("branch b shared row should load"),
            Some(value("shared-change", Some("{\"shared\":true}")))
        );
        assert!(
            tree.get(&store, &branch_a.root_id, &branch_b_key)
                .await
                .expect("branch a should read")
                .is_none()
        );
        assert!(
            tree.get(&store, &branch_b.root_id, &branch_a_key)
                .await
                .expect("branch b should read")
                .is_none()
        );
    }

    #[tokio::test]
    async fn single_update_matches_full_canonical_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("record-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let changed_key = key("schema", None, "record-000");
        let changed_value = value("changed", Some("{\"v\":\"changed\"}"));
        let base = apply_mutations_for_test(&tree, &storage, None, rows, None)
            .await
            .expect("base should build");
        let fast = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![mutation(&changed_key, &changed_value)],
            None,
        )
        .await
        .expect("fast path should apply");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        assert!(
            canonical_entries
                .windows(2)
                .all(|window| window[0].key < window[1].key)
        );
        let encoded_changed_key = encode_key(&changed_key);
        let encoded_changed_value = encode_value(&changed_value);
        let index = canonical_entries
            .binary_search_by(|entry| entry.key.as_ref().cmp(&encoded_changed_key))
            .expect("changed key should exist");
        canonical_entries[index].value = encoded_changed_value.into();
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn single_insert_matches_full_canonical_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("record-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let inserted_key = key("schema", None, "record-050b");
        let inserted_value = value("inserted", Some("{\"v\":\"inserted\"}"));
        let base = apply_mutations_for_test(&tree, &storage, None, rows, None)
            .await
            .expect("base should build");
        let fast = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![mutation(&inserted_key, &inserted_value)],
            None,
        )
        .await
        .expect("fast path should apply");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        let encoded_inserted_key = encode_key(&inserted_key);
        let encoded_inserted_value = encode_value(&inserted_value);
        let index = canonical_entries
            .binary_search_by(|entry| entry.key.as_ref().cmp(&encoded_inserted_key))
            .expect_err("inserted key should not exist");
        canonical_entries.insert(
            index,
            EncodedLeafEntry {
                key: encoded_inserted_key.into(),
                value: encoded_inserted_value.into(),
            },
        );
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn batch_update_matches_full_canonical_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("record-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        // A dense sorted batch still uses the same authenticated frontier;
        // there is no separate whole-tree fallback.
        let updates = (10..90)
            .map(|index| {
                (
                    key("schema", None, &format!("record-{index:03}")),
                    value(
                        &format!("changed-{index}"),
                        Some(&format!("{{\"changed\":{index}}}")),
                    ),
                )
            })
            .collect::<Vec<_>>();
        let base = apply_mutations_for_test(&tree, &storage, None, rows, None)
            .await
            .expect("base should build");
        let fast = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            updates
                .iter()
                .map(|(key, value)| mutation(key, value))
                .collect(),
            None,
        )
        .await
        .expect("batch path should apply");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        for (key, value) in updates {
            let encoded_key = encode_key(&key);
            let encoded_value = encode_value(&value);
            let index = canonical_entries
                .binary_search_by(|entry| entry.key.as_ref().cmp(&encoded_key))
                .expect("updated key should exist");
            canonical_entries[index].value = encoded_value.into();
        }
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn batch_insert_matches_full_canonical_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        // Preserve this arbitrary primary-key payload byte-for-byte: the
        // regression is deliberately pinned to the page split geometry it
        // produces. It is stored row data, not an engine/API term.
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("entity-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let inserts = ["entity-050a", "entity-050b", "entity-050c"]
            .into_iter()
            .enumerate()
            .map(|(index, row_pk)| {
                (
                    key("schema", None, row_pk),
                    value(
                        &format!("inserted-{index}"),
                        Some(&format!("{{\"inserted\":{index}}}")),
                    ),
                )
            })
            .collect::<Vec<_>>();
        let base = apply_mutations_for_test(&tree, &storage, None, rows, None)
            .await
            .expect("base should build");
        let fast = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            inserts
                .iter()
                .map(|(key, value)| mutation(key, value))
                .collect(),
            None,
        )
        .await
        .expect("batch path should apply");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        for (key, value) in inserts {
            let encoded_key = encode_key(&key);
            let encoded_value = encode_value(&value);
            let index = canonical_entries
                .binary_search_by(|entry| entry.key.as_ref().cmp(&encoded_key))
                .expect_err("inserted key should not exist");
            canonical_entries.insert(
                index,
                EncodedLeafEntry {
                    key: encoded_key.into(),
                    value: encoded_value.into(),
                },
            );
        }
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn randomized_frontier_matches_canonical_rebuild() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let initial = (0..192)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("row-{index:04}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let mut current = apply_mutations_for_test(&tree, &storage, None, initial, None)
            .await
            .expect("initial root should build")
            .root_id;
        let mut state = 0x7f4a_7c15_u64;
        for step in 0..96 {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            let index = if step % 3 == 0 {
                (state as usize) % 192
            } else {
                192 + step
            };
            let logical_key = key("schema", None, &format!("row-{index:04}"));
            let logical_value = value(
                &format!("random-{step}"),
                Some(&format!("{{\"step\":{step},\"state\":{state}}}")),
            );
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut canonical_entries = tree
                .collect_leaf_entries(&read, &current)
                .await
                .expect("current entries should collect");
            let encoded_key = encode_key(&logical_key);
            let encoded_value = encode_value(&logical_value);
            match canonical_entries.binary_search_by(|entry| entry.key.as_ref().cmp(&encoded_key)) {
                Ok(existing) => canonical_entries[existing].value = encoded_value.clone().into(),
                Err(insert) => canonical_entries.insert(
                    insert,
                    EncodedLeafEntry {
                        key: encoded_key.into(),
                        value: encoded_value.into(),
                    },
                ),
            }
            let fast = apply_mutations_for_test(
                &tree,
                &storage,
                Some(&current),
                vec![mutation(&logical_key, &logical_value)],
                None,
            )
            .await
            .expect("frontier mutation should apply");
            let canonical = tree
                .build_tree_from_entries(canonical_entries)
                .expect("canonical root should build");
            assert_eq!(fast.root_id, canonical.root_id, "step {step} diverged");
            assert_eq!(fast.row_count, canonical.row_count, "step {step} row count");
            assert_eq!(
                fast.tree_height, canonical.tree_height,
                "step {step} height"
            );
            current = fast.root_id;
        }
    }

    #[tokio::test]
    async fn batch_frontier_does_not_resync_between_mutations() {
        let storage = StorageAdapter::new(Memory::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let initial = (0..256)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("row-{index:04}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let base = apply_mutations_for_test(&tree, &storage, None, initial, None)
            .await
            .expect("base should build");
        let first_key = key("schema", None, "row-0010");
        let first_value = value("first-updated", Some("{\"updated\":10}"));
        let last_key = key("schema", None, "row-0240");
        let last_value = value("last-updated", Some("{\"updated\":240}"));
        let updated = apply_mutations_for_test(
            &tree,
            &storage,
            Some(&base.root_id),
            vec![
                mutation(&first_key, &first_value),
                mutation(&last_key, &last_value),
            ],
            None,
        )
        .await
        .expect("batch frontier should apply");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        assert_eq!(
            tree.get_many(&read, &updated.root_id, &[first_key, last_key])
                .await
                .expect("updated rows should load"),
            vec![Some(first_value), Some(last_value)],
        );
        assert_eq!(updated.row_count, base.row_count);
    }

    #[test]
    fn leaf_chunk_boundaries_ignore_value_bytes() {
        let options = TrackedStateTreeOptions {
            target_chunk_bytes: 64,
            min_chunk_bytes: 32,
            max_chunk_bytes: 96,
        };
        let short_entries = encoded_entries_with_change_id("c");
        let large_entries = encoded_entries_with_change_id(&"c".repeat(4096));

        assert_eq!(
            leaf_chunk_boundary_keys(chunk_leaf_entries(short_entries, &options)),
            leaf_chunk_boundary_keys(chunk_leaf_entries(large_entries, &options))
        );
    }

    async fn apply_mutations_for_test(
        tree: &TrackedStateTree,
        storage: &StorageAdapter,
        base_root: Option<&TrackedStateRootId>,
        mutations: Vec<TrackedStateMutation>,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let result = tree
            .apply_mutations(
                &read,
                &mut writes,
                base_root,
                TrackedStateMutationBatch::from_shared(mutations),
                commit_id,
            )
            .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await?;
        Ok(result)
    }

    fn mutation(key: &TrackedStateKey, value: &TrackedStateIndexValue) -> TrackedStateMutation {
        TrackedStateMutation::put_encoded(encode_key(key), encode_value(value))
    }

    fn mutation_owned(key: TrackedStateKey, value: TrackedStateIndexValue) -> TrackedStateMutation {
        mutation(&key, &value)
    }

    fn naive_tree_diff(
        before: &BTreeMap<TrackedStateKey, TrackedStateIndexValue>,
        after: &BTreeMap<TrackedStateKey, TrackedStateIndexValue>,
    ) -> Vec<TrackedStateTreeDiffEntry> {
        before
            .keys()
            .chain(after.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|key| match (before.get(&key), after.get(&key)) {
                (Some(left), Some(right)) if left == right => None,
                (left, right) => Some(TrackedStateTreeDiffEntry {
                    key,
                    before: left.cloned(),
                    after: right.cloned(),
                }),
            })
            .collect()
    }

    fn encoded_entries_with_change_id(change_id: &str) -> Vec<EncodedLeafEntry> {
        (0..64)
            .map(|index| {
                let key = key("schema", None, &format!("row-{index:03}"));
                EncodedLeafEntry {
                    key: encode_key(&key).into(),
                    value: encode_value(&value(change_id, Some("{}"))).into(),
                }
            })
            .collect()
    }

    fn leaf_chunk_boundary_keys(
        groups: Vec<LeafChunkAccumulator>,
    ) -> Vec<(Vec<u8>, Vec<u8>, usize)> {
        groups
            .into_iter()
            .map(|group| {
                let first_key = group
                    .entries
                    .first()
                    .map(|entry| entry.key.to_vec())
                    .unwrap_or_default();
                let last_key = group
                    .entries
                    .last()
                    .map(|entry| entry.key.to_vec())
                    .unwrap_or_default();
                (first_key, last_key, group.entries.len())
            })
            .collect()
    }

    fn key(schema_key: &str, file_id: Option<&str>, row_pk: &str) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            row_pk: RowPk::single(row_pk),
        }
    }

    fn value(change_id: &str, snapshot_content: Option<&str>) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_id: ChangeId::for_test_label(change_id),
            commit_id: CommitId::for_test_label("commit"),
            deleted: snapshot_content.is_none(),
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-01-01T00:00:00Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-01-01T00:00:00Z",
            ),
        }
    }
}
