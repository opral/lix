use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    ops::Range,
    pin::Pin,
};

use crate::storage::{StorageRead, StorageWriteSet};
use crate::tracked_state::codec::{
    boundary_trigger, child_summary_from_node, decode_key, decode_key_with_trusted_prefix,
    decode_node, decode_node_ref, decode_value, decode_visible_value, encode_internal_node,
    encode_internal_node_refs, encode_key, encode_leaf_node, encode_leaf_node_refs,
    encode_schema_file_prefix, encode_schema_key_prefix, ChildSummary, ChildSummaryRef,
    DecodedLeafNodeRef, DecodedNode, DecodedNodeRef, EncodedLeafEntry, EncodedLeafEntryRef,
    PendingChunkWrite,
};
use crate::tracked_state::storage;
use crate::tracked_state::types::{
    TrackedStateApplyResult, TrackedStateIndexValue, TrackedStateKey, TrackedStateMutation,
    TrackedStateRootId, TrackedStateTreeDiffEntry, TrackedStateTreeScanRequest,
    TRACKED_STATE_HASH_BYTES,
};
use crate::{LixError, NullableKeyFilter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeOptions {
    pub(crate) target_chunk_bytes: usize,
    pub(crate) min_chunk_bytes: usize,
    pub(crate) max_chunk_bytes: usize,
}

enum MutationApply<T> {
    Applied(TrackedStateApplyResult),
    Fallback(T),
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
/// This type owns tracked-state tree mechanics only. Branch refs, untracked overlay,
/// and SQL visibility remain outside the tree.
#[derive(Debug, Clone)]
pub(crate) struct TrackedStateTree {
    options: TrackedStateTreeOptions,
}

impl TrackedStateTree {
    pub(crate) fn new() -> Self {
        Self {
            options: TrackedStateTreeOptions::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_options(options: TrackedStateTreeOptions) -> Self {
        Self { options }
    }

    pub(crate) async fn load_root(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        commit_id: &str,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        storage::load_root(store, commit_id).await
    }

    #[cfg(test)]
    pub(crate) async fn get(
        &self,
        store: &(impl StorageRead + Send + Sync),
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
    ) -> Result<Option<TrackedStateIndexValue>, LixError> {
        let encoded_key = encode_key(key);
        let mut current = *root_id.as_bytes();
        loop {
            match self.load_node(store, &current).await? {
                DecodedNode::Leaf(leaf) => {
                    let entry = leaf
                        .entries()
                        .binary_search_by(|entry| entry.key.as_slice().cmp(&encoded_key))
                        .ok()
                        .map(|index| &leaf.entries()[index]);
                    return entry.map(|entry| decode_value(&entry.value)).transpose();
                }
                DecodedNode::Internal(internal) => {
                    let child = internal
                        .children()
                        .iter()
                        .find(|child| child.last_key.as_slice() >= encoded_key.as_slice())
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
        store: &(impl StorageRead + Send + Sync + ?Sized),
        root_id: &TrackedStateRootId,
        keys: &[TrackedStateKey],
    ) -> Result<Vec<Option<TrackedStateIndexValue>>, LixError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut encoded_keys = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (index, encode_key(key)))
            .collect::<Vec<_>>();
        encoded_keys.sort_by(|left, right| left.1.cmp(&right.1));

        let mut values = vec![None; keys.len()];
        self.get_many_node(store, *root_id.as_bytes(), &encoded_keys, &mut values)
            .await?;
        Ok(values)
    }

    pub(crate) async fn scan(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        root_id: &TrackedStateRootId,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        if request.limit == Some(0) {
            return Ok(Vec::new());
        }

        let ranges = scan_ranges(request);
        let key_decode_hint = scan_key_decode_hint(request, &ranges);
        let mut rows = Vec::new();
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
        store: &(impl StorageRead + Send + Sync),
        left_root: Option<&TrackedStateRootId>,
        right_root: Option<&TrackedStateRootId>,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<TrackedStateTreeDiffEntry>, LixError> {
        match (left_root, right_root) {
            (None, None) => Ok(Vec::new()),
            (Some(left), Some(right)) if left == right => Ok(Vec::new()),
            (Some(left), Some(right)) => {
                let mut out = Vec::new();
                self.diff_nodes(
                    store,
                    *left.as_bytes(),
                    *right.as_bytes(),
                    request,
                    &mut out,
                )
                .await?;
                Ok(out)
            }
            (Some(left), None) => Ok(self
                .collect_filtered_entries(store, left, request)
                .await?
                .into_iter()
                .map(|(key, value)| TrackedStateTreeDiffEntry {
                    before: Some((key, value)),
                    after: None,
                })
                .collect()),
            (None, Some(right)) => Ok(self
                .collect_filtered_entries(store, right, request)
                .await?
                .into_iter()
                .map(|(key, value)| TrackedStateTreeDiffEntry {
                    before: None,
                    after: Some((key, value)),
                })
                .collect()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn apply_mutations(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        writes: &mut StorageWriteSet,
        base_root: Option<&TrackedStateRootId>,
        mutations: Vec<TrackedStateMutation>,
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
        store: &(impl StorageRead + Send + Sync + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        base_root: Option<&TrackedStateRootId>,
        mut mutations: Vec<TrackedStateMutation>,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        if let Some(root_id) = base_root {
            if mutations.len() == 1 {
                let mutation = mutations.pop().expect("single mutation should exist");
                match self
                    .apply_single_mutation(store, writes, overlay, root_id, mutation, commit_id)
                    .await?
                {
                    MutationApply::Applied(result) => return Ok(result),
                    MutationApply::Fallback(mutation) => mutations = vec![mutation],
                }
            } else if mutations.len() > 1 {
                match self
                    .apply_sorted_mutations_chunker(
                        store, writes, overlay, root_id, mutations, commit_id,
                    )
                    .await?
                {
                    MutationApply::Applied(result) => return Ok(result),
                    MutationApply::Fallback(fallback_mutations) => mutations = fallback_mutations,
                }
            }
        }

        let mut entries = match base_root {
            Some(root_id) => self
                .collect_leaf_entries_with_overlay(store, overlay, root_id)
                .await?
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<BTreeMap<_, _>>(),
            None => BTreeMap::new(),
        };

        // Apply in caller order so repeated writes to the same key behave like
        // normal transaction staging: the latest mutation wins.
        for mutation in mutations {
            entries.insert(mutation.encoded_key, mutation.encoded_value);
        }

        let built = self.build_tree_from_entries(
            entries
                .into_iter()
                .map(|(key, value)| EncodedLeafEntry { key, value })
                .collect(),
        )?;
        overlay.stage_chunks(writes, &built.chunks);

        Ok(TrackedStateApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
        })
    }

    async fn apply_single_mutation(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
        mutation: TrackedStateMutation,
        commit_id: Option<&str>,
    ) -> Result<MutationApply<TrackedStateMutation>, LixError> {
        let mutation = match self
            .apply_single_mutation_from_seek_path(
                store, writes, overlay, root_id, mutation, commit_id,
            )
            .await?
        {
            MutationApply::Applied(result) => return Ok(MutationApply::Applied(result)),
            MutationApply::Fallback(mutation) => mutation,
        };

        let TrackedStateMutation {
            encoded_key,
            encoded_value,
        } = mutation;

        let levels = self
            .collect_summary_levels_with_overlay(store, overlay, root_id)
            .await?;
        let Some(leaves) = levels.first() else {
            return Ok(MutationApply::Fallback(TrackedStateMutation {
                encoded_key,
                encoded_value,
            }));
        };
        let target_leaf_index = leaves
            .iter()
            .position(|leaf| leaf.last_key.as_slice() >= encoded_key.as_slice())
            .unwrap_or_else(|| leaves.len().saturating_sub(1));
        let Some(target_leaf) = leaves.get(target_leaf_index).cloned() else {
            return Ok(MutationApply::Fallback(TrackedStateMutation {
                encoded_key,
                encoded_value,
            }));
        };

        let mut entries = self
            .load_leaf_entries_with_overlay(store, overlay, &target_leaf.child_hash)
            .await?;
        let mutation_entry_index = match entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(encoded_key.as_slice()))
        {
            Ok(index) => {
                if entries[index].value.as_slice() == encoded_value.as_slice() {
                    return Ok(MutationApply::Fallback(TrackedStateMutation {
                        encoded_key,
                        encoded_value,
                    }));
                }
                entries[index].value = encoded_value;
                index
            }
            Err(index) => {
                entries.insert(
                    index,
                    EncodedLeafEntry {
                        key: encoded_key,
                        value: encoded_value,
                    },
                );
                index
            }
        };

        let mut chunks = BTreeMap::new();
        let mut suffix_entries = entries;
        let mut next_leaf_index = target_leaf_index + 1;
        let mut replacement_leaves;
        let old_leaf_count;

        // Rechunk from the edited leaf until a generated leaf matches an
        // existing post-mutation leaf, then reuse the rest of the old suffix.
        loop {
            let mut candidate_chunks = BTreeMap::new();
            let candidate_summaries = self.build_leaf_level_from_refs(
                suffix_entries.iter().map(EncodedLeafEntry::as_ref),
                &mut candidate_chunks,
            );

            if let Some((generated_resync_index, existing_resync_index)) = first_resync_index(
                &candidate_summaries,
                &leaves[target_leaf_index..],
                suffix_entries[mutation_entry_index].key.as_slice(),
            ) {
                for summary in &candidate_summaries[..generated_resync_index] {
                    if let Some(chunk) = candidate_chunks.remove(&summary.child_hash) {
                        chunks.entry(chunk.hash).or_insert(chunk);
                    }
                }
                replacement_leaves = candidate_summaries
                    .into_iter()
                    .take(generated_resync_index)
                    .collect();
                old_leaf_count = existing_resync_index;
                break;
            }

            if next_leaf_index >= leaves.len() {
                chunks.extend(candidate_chunks);
                replacement_leaves = candidate_summaries;
                old_leaf_count = leaves.len() - target_leaf_index;
                break;
            }

            suffix_entries.extend(
                self.load_leaf_entries_with_overlay(
                    store,
                    overlay,
                    &leaves[next_leaf_index].child_hash,
                )
                .await?,
            );
            next_leaf_index += 1;
        }

        let built = self.build_tree_from_leaf_patch(
            &levels,
            target_leaf_index,
            old_leaf_count,
            std::mem::take(&mut replacement_leaves),
            chunks,
            suffix_entries[mutation_entry_index].key.as_slice(),
        )?;
        overlay.stage_chunks(writes, &built.chunks);

        Ok(MutationApply::Applied(TrackedStateApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
        }))
    }

    fn diff_nodes<'a, S>(
        &'a self,
        store: &'a S,
        left_hash: [u8; TRACKED_STATE_HASH_BYTES],
        right_hash: [u8; TRACKED_STATE_HASH_BYTES],
        request: &'a TrackedStateTreeScanRequest,
        out: &'a mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + 'a>>
    where
        S: StorageRead + Send + Sync + 'a,
    {
        Box::pin(async move {
            if left_hash == right_hash {
                return Ok(());
            }

            let left = self.load_node(store, &left_hash).await?;
            let right = self.load_node(store, &right_hash).await?;
            match (left, right) {
                (DecodedNode::Leaf(left), DecodedNode::Leaf(right)) => {
                    self.diff_leaf_entries(left.entries(), right.entries(), request, out)?;
                }
                (DecodedNode::Internal(left), DecodedNode::Internal(right))
                    if internal_boundaries_match(left.children(), right.children()) =>
                {
                    for (left_child, right_child) in left.children().iter().zip(right.children()) {
                        if left_child == right_child {
                            continue;
                        }
                        self.diff_nodes(
                            store,
                            left_child.child_hash,
                            right_child.child_hash,
                            request,
                            out,
                        )
                        .await?;
                    }
                }
                _ => {
                    self.diff_leaf_summary_cursors(store, left_hash, right_hash, request, out)
                        .await?;
                }
            }
            Ok(())
        })
    }

    async fn diff_leaf_summary_cursors(
        &self,
        store: &(impl StorageRead + Send + Sync),
        left_hash: [u8; TRACKED_STATE_HASH_BYTES],
        right_hash: [u8; TRACKED_STATE_HASH_BYTES],
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        let mut left = LeafSummaryCursor::new(self, store, left_hash).await?;
        let mut right = LeafSummaryCursor::new(self, store, right_hash).await?;
        let mut left_window = Vec::new();
        let mut right_window = Vec::new();

        loop {
            match (left.current(), right.current()) {
                (Some(left_leaf), Some(right_leaf)) if left_leaf == right_leaf => {
                    self.diff_leaf_summary_window(store, &left_window, &right_window, request, out)
                        .await?;
                    left_window.clear();
                    right_window.clear();
                    left.advance(self, store).await?;
                    right.advance(self, store).await?;
                }
                (Some(left_leaf), Some(right_leaf)) => {
                    match left_leaf.last_key.cmp(&right_leaf.last_key) {
                        std::cmp::Ordering::Less => {
                            left_window.push(left_leaf.clone());
                            left.advance(self, store).await?;
                        }
                        std::cmp::Ordering::Greater => {
                            right_window.push(right_leaf.clone());
                            right.advance(self, store).await?;
                        }
                        std::cmp::Ordering::Equal => {
                            left_window.push(left_leaf.clone());
                            right_window.push(right_leaf.clone());
                            left.advance(self, store).await?;
                            right.advance(self, store).await?;
                        }
                    }
                }
                (Some(left_leaf), None) => {
                    left_window.push(left_leaf.clone());
                    left.advance(self, store).await?;
                }
                (None, Some(right_leaf)) => {
                    right_window.push(right_leaf.clone());
                    right.advance(self, store).await?;
                }
                (None, None) => {
                    self.diff_leaf_summary_window(store, &left_window, &right_window, request, out)
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    async fn diff_leaf_summary_window(
        &self,
        store: &(impl StorageRead + Send + Sync),
        left_leaves: &[ChildSummary],
        right_leaves: &[ChildSummary],
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        if left_leaves.is_empty() && right_leaves.is_empty() {
            return Ok(());
        }
        let left_entries = self
            .collect_entries_from_leaf_summaries(store, left_leaves)
            .await?;
        let right_entries = self
            .collect_entries_from_leaf_summaries(store, right_leaves)
            .await?;
        self.diff_leaf_entries(&left_entries, &right_entries, request, out)
    }

    fn diff_leaf_entries(
        &self,
        left: &[EncodedLeafEntry],
        right: &[EncodedLeafEntry],
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        let mut left_index = 0usize;
        let mut right_index = 0usize;
        while left_index < left.len() && right_index < right.len() {
            match left[left_index].key.cmp(&right[right_index].key) {
                std::cmp::Ordering::Less => {
                    self.push_removed_diff(&left[left_index], request, out)?;
                    left_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    self.push_added_diff(&right[right_index], request, out)?;
                    right_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    if left[left_index].value != right[right_index].value {
                        self.push_modified_diff(
                            &left[left_index],
                            &right[right_index],
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
            self.push_removed_diff(entry, request, out)?;
        }
        for entry in &right[right_index..] {
            self.push_added_diff(entry, request, out)?;
        }
        Ok(())
    }

    fn push_removed_diff(
        &self,
        entry: &EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        let (key, value) = decode_entry(entry)?;
        if request.matches(&key, &value) {
            out.push(TrackedStateTreeDiffEntry {
                before: Some((key, value)),
                after: None,
            });
        }
        Ok(())
    }

    fn push_added_diff(
        &self,
        entry: &EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        let (key, value) = decode_entry(entry)?;
        if request.matches(&key, &value) {
            out.push(TrackedStateTreeDiffEntry {
                before: None,
                after: Some((key, value)),
            });
        }
        Ok(())
    }

    fn push_modified_diff(
        &self,
        left: &EncodedLeafEntry,
        right: &EncodedLeafEntry,
        request: &TrackedStateTreeScanRequest,
        out: &mut Vec<TrackedStateTreeDiffEntry>,
    ) -> Result<(), LixError> {
        let (left_key, left_value) = decode_entry(left)?;
        let (right_key, right_value) = decode_entry(right)?;
        if request.matches(&left_key, &left_value) || request.matches(&right_key, &right_value) {
            out.push(TrackedStateTreeDiffEntry {
                before: Some((left_key, left_value)),
                after: Some((right_key, right_value)),
            });
        }
        Ok(())
    }

    async fn apply_sorted_mutations_chunker(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
        mutations: Vec<TrackedStateMutation>,
        commit_id: Option<&str>,
    ) -> Result<MutationApply<Vec<TrackedStateMutation>>, LixError> {
        let mut mutation_map = BTreeMap::new();
        for mutation in mutations {
            mutation_map.insert(mutation.encoded_key, mutation.encoded_value);
        }
        if mutation_map.is_empty() {
            return Ok(MutationApply::Fallback(Vec::new()));
        }

        let levels = self
            .collect_summary_levels_with_overlay(store, overlay, root_id)
            .await?;
        let Some(leaves) = levels.first() else {
            return Ok(MutationApply::Fallback(
                mutation_map
                    .into_iter()
                    .map(|(encoded_key, encoded_value)| TrackedStateMutation {
                        encoded_key,
                        encoded_value,
                    })
                    .collect(),
            ));
        };

        let base_row_count = leaves
            .iter()
            .map(|leaf| leaf.subtree_count as usize)
            .sum::<usize>();
        let first_mutation_key = mutation_map
            .keys()
            .next()
            .expect("non-empty mutation map should have first key");
        let append_only = leaves
            .last()
            .is_some_and(|leaf| first_mutation_key.as_slice() > leaf.last_key.as_slice());
        if !append_only && mutation_map.len() * 2 > base_row_count {
            return Ok(MutationApply::Fallback(
                mutation_map
                    .into_iter()
                    .map(|(encoded_key, encoded_value)| TrackedStateMutation {
                        encoded_key,
                        encoded_value,
                    })
                    .collect(),
            ));
        }

        let mut mutations = mutation_map.into_iter().collect::<VecDeque<_>>();
        let mut output_leaves = Vec::new();
        let mut chunks = BTreeMap::new();
        let mut leaf_index = 0usize;

        while leaf_index < leaves.len() {
            let current_leaf_has_mutation = mutations
                .front()
                .is_some_and(|(key, _)| key.as_slice() <= leaves[leaf_index].last_key.as_slice());
            if !current_leaf_has_mutation {
                output_leaves.push(leaves[leaf_index].clone());
                leaf_index += 1;
                continue;
            }

            let window_start = leaf_index;
            let mut window_entries = BTreeMap::new();
            let mut window_mutation_ceiling = mutations
                .front()
                .map(|(key, _)| key.clone())
                .expect("window with mutation should have front mutation");

            loop {
                if leaf_index < leaves.len() {
                    let leaf = &leaves[leaf_index];
                    for entry in self
                        .load_leaf_entries_with_overlay(store, overlay, &leaf.child_hash)
                        .await?
                    {
                        window_entries.insert(entry.key, entry.value);
                    }

                    while mutations
                        .front()
                        .is_some_and(|(key, _)| key.as_slice() <= leaf.last_key.as_slice())
                    {
                        let (key, value) = mutations
                            .pop_front()
                            .expect("front mutation should be present");
                        window_mutation_ceiling = key.clone();
                        window_entries.insert(key, value);
                    }
                    leaf_index += 1;
                }

                while let Some((key, _)) = mutations.front() {
                    if leaf_index < leaves.len()
                        && key.as_slice() >= leaves[leaf_index].first_key.as_slice()
                    {
                        break;
                    }
                    let (key, value) = mutations
                        .pop_front()
                        .expect("front mutation should be present");
                    window_mutation_ceiling = key.clone();
                    window_entries.insert(key, value);
                }

                if leaf_index < leaves.len()
                    && mutations.front().is_some_and(|(key, _)| {
                        key.as_slice() <= leaves[leaf_index].last_key.as_slice()
                    })
                {
                    continue;
                }

                let mut candidate_chunks = BTreeMap::new();
                let candidate_leaves = self.build_leaf_level_from_refs(
                    window_entries
                        .iter()
                        .map(|(key, value)| EncodedLeafEntryRef { key, value }),
                    &mut candidate_chunks,
                );

                if let Some((generated_resync_index, existing_resync_index)) = first_resync_index(
                    &candidate_leaves,
                    &leaves[window_start..],
                    &window_mutation_ceiling,
                ) {
                    for summary in &candidate_leaves[..generated_resync_index] {
                        if let Some(chunk) = candidate_chunks.remove(&summary.child_hash) {
                            chunks.entry(chunk.hash).or_insert(chunk);
                        }
                    }
                    output_leaves.extend(candidate_leaves.into_iter().take(generated_resync_index));
                    leaf_index = window_start + existing_resync_index;
                    break;
                }

                if leaf_index >= leaves.len() {
                    chunks.extend(candidate_chunks);
                    output_leaves.extend(candidate_leaves);
                    break;
                }
            }
        }

        if !mutations.is_empty() {
            let entries = mutations
                .into_iter()
                .map(|(key, value)| EncodedLeafEntry { key, value })
                .collect();
            output_leaves.extend(self.build_leaf_level(entries, &mut chunks));
        }

        let built = self.build_tree_from_leaf_summaries(output_leaves, chunks)?;
        Ok(MutationApply::Applied(
            self.persist_built_tree(writes, overlay, built, commit_id)
                .await?,
        ))
    }

    async fn apply_single_mutation_from_seek_path(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
        mutation: TrackedStateMutation,
        commit_id: Option<&str>,
    ) -> Result<MutationApply<TrackedStateMutation>, LixError> {
        let TrackedStateMutation {
            encoded_key,
            encoded_value,
        } = mutation;
        let mut current = *root_id.as_bytes();
        let mut path = Vec::new();
        let mut entries = loop {
            match self
                .load_node_with_overlay(store, overlay, &current)
                .await?
            {
                DecodedNode::Leaf(leaf) => break leaf.entries().to_vec(),
                DecodedNode::Internal(internal) => {
                    let children = internal.children().to_vec();
                    let child_index = children
                        .iter()
                        .position(|child| child.last_key.as_slice() >= encoded_key.as_slice())
                        .or_else(|| (!children.is_empty()).then_some(children.len() - 1))
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "tracked-state tree internal node has no children",
                            )
                        })?;
                    current = children[child_index].child_hash;
                    path.push(SeekPathFrame {
                        children,
                        child_index,
                    });
                }
            }
        };

        let mutation_entry_index = match entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(encoded_key.as_slice()))
        {
            Ok(index) => {
                if entries[index].value.as_slice() == encoded_value.as_slice() {
                    return Ok(MutationApply::Fallback(TrackedStateMutation {
                        encoded_key,
                        encoded_value,
                    }));
                }
                entries[index].value = encoded_value;
                index
            }
            Err(index) => {
                entries.insert(
                    index,
                    EncodedLeafEntry {
                        key: encoded_key,
                        value: encoded_value,
                    },
                );
                index
            }
        };

        let mut chunks = BTreeMap::new();
        let mut replacement_children;
        let mut old_child_count;

        let Some(leaf_parent) = path.pop() else {
            let built = self.build_tree_from_entries(entries)?;
            return Ok(MutationApply::Applied(
                self.persist_built_tree(writes, overlay, built, commit_id)
                    .await?,
            ));
        };
        let mutation_is_right_edge = leaf_parent.child_index + 1 == leaf_parent.children.len()
            && path
                .iter()
                .all(|frame| frame.child_index + 1 == frame.children.len());

        let mut leaf_entries = entries;
        let mut next_leaf_index = leaf_parent.child_index + 1;
        loop {
            let mut candidate_chunks = BTreeMap::new();
            let candidate_leaves = self.build_leaf_level_from_refs(
                leaf_entries.iter().map(EncodedLeafEntry::as_ref),
                &mut candidate_chunks,
            );
            if let Some((generated_resync_index, existing_resync_index)) = first_resync_index(
                &candidate_leaves,
                &leaf_parent.children[leaf_parent.child_index..],
                leaf_entries[mutation_entry_index].key.as_slice(),
            ) {
                for summary in &candidate_leaves[..generated_resync_index] {
                    if let Some(chunk) = candidate_chunks.remove(&summary.child_hash) {
                        chunks.entry(chunk.hash).or_insert(chunk);
                    }
                }
                replacement_children = candidate_leaves
                    .into_iter()
                    .take(generated_resync_index)
                    .collect();
                old_child_count = existing_resync_index;
                break;
            }

            if next_leaf_index >= leaf_parent.children.len() {
                if !mutation_is_right_edge {
                    let entry = leaf_entries.remove(mutation_entry_index);
                    return Ok(MutationApply::Fallback(TrackedStateMutation {
                        encoded_key: entry.key,
                        encoded_value: entry.value,
                    }));
                }
                chunks.extend(candidate_chunks);
                replacement_children = candidate_leaves;
                old_child_count = leaf_parent.children.len() - leaf_parent.child_index;
                break;
            }

            leaf_entries.extend(
                self.load_leaf_entries_with_overlay(
                    store,
                    overlay,
                    &leaf_parent.children[next_leaf_index].child_hash,
                )
                .await?,
            );
            next_leaf_index += 1;
        }

        let mut child_index = leaf_parent.child_index;
        let mut children = leaf_parent.children;
        let mut parent_level = 1usize;
        loop {
            children.splice(
                child_index..child_index + old_child_count,
                replacement_children,
            );
            replacement_children = self.build_internal_level(children, parent_level, &mut chunks);
            old_child_count = 1;

            let Some(frame) = path.pop() else {
                let mut summaries = replacement_children;
                let mut tree_height = parent_level + 1;
                while summaries.len() > 1 {
                    summaries = self.build_internal_level(summaries, tree_height, &mut chunks);
                    tree_height += 1;
                }
                let root = summaries.pop().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "tracked-state seek-path mutation produced no root",
                    )
                })?;
                let chunks = chunks.into_values().collect::<Vec<_>>();
                let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
                let built = BuiltTree {
                    root_id: TrackedStateRootId::new(root.child_hash),
                    chunks,
                    row_count: root.subtree_count as usize,
                    tree_height,
                    chunk_bytes,
                };
                return Ok(MutationApply::Applied(
                    self.persist_built_tree(writes, overlay, built, commit_id)
                        .await?,
                ));
            };

            child_index = frame.child_index;
            children = frame.children;
            parent_level += 1;
        }
    }

    async fn persist_built_tree(
        &self,
        writes: &mut StorageWriteSet,
        overlay: &mut storage::TrackedStateChunkOverlay,
        built: BuiltTree,
        _commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        overlay.stage_chunks(writes, &built.chunks);
        Ok(TrackedStateApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
        })
    }

    fn build_tree_from_entries(
        &self,
        entries: Vec<EncodedLeafEntry>,
    ) -> Result<BuiltTree, LixError> {
        let row_count = entries.len();
        let mut chunks = BTreeMap::<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>::new();
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
        let chunks = chunks.into_values().collect::<Vec<_>>();
        let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
        Ok(BuiltTree {
            root_id: TrackedStateRootId::new(root.child_hash),
            chunks,
            row_count,
            tree_height,
            chunk_bytes,
        })
    }

    fn build_tree_from_leaf_summaries(
        &self,
        leaf_summaries: Vec<ChildSummary>,
        mut chunks: BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
    ) -> Result<BuiltTree, LixError> {
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
        let chunks = chunks.into_values().collect::<Vec<_>>();
        let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
        Ok(BuiltTree {
            root_id: TrackedStateRootId::new(root.child_hash),
            chunks,
            row_count,
            tree_height,
            chunk_bytes,
        })
    }

    fn build_tree_from_leaf_patch(
        &self,
        levels: &[Vec<ChildSummary>],
        leaf_start: usize,
        old_leaf_count: usize,
        replacement_leaves: Vec<ChildSummary>,
        mut chunks: BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
        mutation_key: &[u8],
    ) -> Result<BuiltTree, LixError> {
        if levels.len() <= 1 {
            let mut leaves = levels.first().cloned().unwrap_or_default();
            leaves.splice(leaf_start..leaf_start + old_leaf_count, replacement_leaves);
            return self.build_tree_from_leaf_summaries(leaves, chunks);
        }

        let mut child_start = leaf_start;
        let mut old_child_count = old_leaf_count;
        let mut replacement_children = replacement_leaves;

        for level in 0..levels.len() - 1 {
            let patch = self.patch_parent_level(
                &levels[level],
                &levels[level + 1],
                child_start,
                old_child_count,
                replacement_children,
                level + 1,
                &mut chunks,
                mutation_key,
            )?;
            child_start = patch.parent_start;
            old_child_count = patch.old_parent_count;
            replacement_children = patch.replacement_parents;
        }

        let mut summaries = replacement_children;
        let mut tree_height = levels.len();
        while summaries.len() > 1 {
            summaries = self.build_internal_level(summaries, tree_height, &mut chunks);
            tree_height += 1;
        }
        let root = summaries.pop().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state patched tree produced no root",
            )
        })?;
        let chunks = chunks.into_values().collect::<Vec<_>>();
        let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
        Ok(BuiltTree {
            root_id: TrackedStateRootId::new(root.child_hash),
            chunks,
            row_count: root.subtree_count as usize,
            tree_height,
            chunk_bytes,
        })
    }

    fn patch_parent_level(
        &self,
        old_children: &[ChildSummary],
        old_parents: &[ChildSummary],
        child_start: usize,
        old_child_count: usize,
        replacement_children: Vec<ChildSummary>,
        parent_level: usize,
        chunks: &mut BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
        mutation_key: &[u8],
    ) -> Result<ParentLevelPatch, LixError> {
        if old_parents.is_empty() {
            return Ok(ParentLevelPatch {
                parent_start: 0,
                old_parent_count: 0,
                replacement_parents: self.build_internal_level(
                    replacement_children,
                    parent_level,
                    chunks,
                ),
            });
        }

        let parent_start = parent_index_for_child_index(old_children, old_parents, child_start);
        let parent_child_range = child_range_for_parent(old_children, &old_parents[parent_start])?;
        let old_child_end = child_start + old_child_count;
        let parent_end = if old_child_count == 0 {
            parent_start
        } else {
            parent_index_for_child_index(old_children, old_parents, old_child_end - 1)
        };
        let parent_end_child_range =
            child_range_for_parent(old_children, &old_parents[parent_end])?;
        let mut window_children = Vec::new();
        window_children.extend(
            old_children[parent_child_range.start..child_start]
                .iter()
                .map(ChildSummary::as_ref),
        );
        window_children.extend(replacement_children.iter().map(ChildSummary::as_ref));
        window_children.extend(
            old_children[old_child_end..parent_end_child_range.end]
                .iter()
                .map(ChildSummary::as_ref),
        );
        let mut next_parent_index = parent_end + 1;

        loop {
            let mut candidate_chunks = BTreeMap::new();
            let candidate_parents = self.build_internal_level_from_refs(
                window_children.iter().copied(),
                parent_level,
                &mut candidate_chunks,
            );

            if let Some((generated_resync_index, existing_resync_index)) = first_resync_index(
                &candidate_parents,
                &old_parents[parent_start..],
                mutation_key,
            ) {
                for summary in &candidate_parents[..generated_resync_index] {
                    if let Some(chunk) = candidate_chunks.remove(&summary.child_hash) {
                        chunks.entry(chunk.hash).or_insert(chunk);
                    }
                }
                return Ok(ParentLevelPatch {
                    parent_start,
                    old_parent_count: existing_resync_index,
                    replacement_parents: candidate_parents
                        .into_iter()
                        .take(generated_resync_index)
                        .collect(),
                });
            }

            if next_parent_index >= old_parents.len() {
                chunks.extend(candidate_chunks);
                return Ok(ParentLevelPatch {
                    parent_start,
                    old_parent_count: old_parents.len() - parent_start,
                    replacement_parents: candidate_parents,
                });
            }

            let next_range = child_range_for_parent(old_children, &old_parents[next_parent_index])?;
            window_children.extend(old_children[next_range].iter().map(ChildSummary::as_ref));
            next_parent_index += 1;
        }
    }

    fn build_leaf_level(
        &self,
        entries: Vec<EncodedLeafEntry>,
        chunks: &mut BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
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
                let (chunk, summary) =
                    child_summary_from_node(node, first_key, last_key, subtree_count);
                chunks.entry(chunk.hash).or_insert(chunk);
                summary
            })
            .collect()
    }

    fn build_leaf_level_from_refs<'a>(
        &self,
        entries: impl IntoIterator<Item = EncodedLeafEntryRef<'a>>,
        chunks: &mut BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
    ) -> Vec<ChildSummary> {
        let groups = chunk_leaf_entry_refs(entries, &self.options);
        groups
            .into_iter()
            .map(|group| {
                let subtree_count = group.entries.len() as u64;
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
                let node = encode_leaf_node_refs(&group.entries);
                let (chunk, summary) =
                    child_summary_from_node(node, first_key, last_key, subtree_count);
                chunks.entry(chunk.hash).or_insert(chunk);
                summary
            })
            .collect()
    }

    fn build_internal_level(
        &self,
        children: Vec<ChildSummary>,
        level: usize,
        chunks: &mut BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
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
                let (chunk, summary) =
                    child_summary_from_node(node, first_key, last_key, subtree_count);
                chunks.entry(chunk.hash).or_insert(chunk);
                summary
            })
            .collect()
    }

    fn build_internal_level_from_refs<'a>(
        &self,
        children: impl IntoIterator<Item = ChildSummaryRef<'a>>,
        level: usize,
        chunks: &mut BTreeMap<[u8; TRACKED_STATE_HASH_BYTES], PendingChunkWrite>,
    ) -> Vec<ChildSummary> {
        let groups = chunk_internal_entry_refs(children, &self.options, level);
        groups
            .into_iter()
            .map(|group| {
                let subtree_count = group.children.iter().map(|child| child.subtree_count).sum();
                let first_key = group
                    .children
                    .first()
                    .map(|child| child.first_key.to_vec())
                    .unwrap_or_default();
                let last_key = group
                    .children
                    .last()
                    .map(|child| child.last_key.to_vec())
                    .unwrap_or_default();
                let node = encode_internal_node_refs(&group.children);
                let (chunk, summary) =
                    child_summary_from_node(node, first_key, last_key, subtree_count);
                chunks.entry(chunk.hash).or_insert(chunk);
                summary
            })
            .collect()
    }

    #[cfg(test)]
    async fn collect_leaf_entries(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let overlay = storage::TrackedStateChunkOverlay::new();
        self.collect_leaf_entries_with_overlay(store, &overlay, root_id)
            .await
    }

    async fn collect_leaf_entries_with_overlay(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let mut out = Vec::new();
        let mut current = vec![*root_id.as_bytes()];
        while !current.is_empty() {
            let mut next = Vec::new();
            for hash in current {
                match self.load_node_with_overlay(store, overlay, &hash).await? {
                    DecodedNode::Leaf(leaf) => out.extend(leaf.entries().iter().cloned()),
                    DecodedNode::Internal(internal) => {
                        next.extend(internal.children().iter().map(|child| child.child_hash));
                    }
                }
            }
            current = next;
        }
        Ok(out)
    }

    async fn collect_filtered_entries(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        root_id: &TrackedStateRootId,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateIndexValue)>, LixError> {
        self.scan(store, root_id, request).await
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
        S: StorageRead + Send + Sync + ?Sized + 'a,
    {
        Box::pin(async move {
            let bytes = self.load_node_bytes(store, &hash).await?;
            match decode_node_ref(&bytes)? {
                DecodedNodeRef::Leaf(leaf) => {
                    for index in 0..leaf.len() {
                        if scan_limit_reached(request, rows.len()) {
                            break;
                        }
                        let entry = leaf.entry(index)?.ok_or_else(|| {
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
                        if key_decode_hint.is_some() || request.matches(&key, &value) {
                            rows.push((key, value));
                        }
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
        encoded_keys: &'a [(usize, Vec<u8>)],
        values: &'a mut [Option<TrackedStateIndexValue>],
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + Send + 'a>>
    where
        S: StorageRead + Send + Sync + ?Sized + 'a,
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
                            let entry = leaf.entry(entry_index)?.ok_or_else(|| {
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

                        let mut end = start;
                        if child_index + 1 == children.len() {
                            end = encoded_keys.len();
                        } else {
                            while end < encoded_keys.len()
                                && encoded_keys[end].1.as_slice() <= child.last_key.as_slice()
                            {
                                end += 1;
                            }
                        }

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

    async fn collect_entries_from_leaf_summaries(
        &self,
        store: &(impl StorageRead + Send + Sync),
        leaves: &[ChildSummary],
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let mut entries = Vec::new();
        for leaf in leaves {
            entries.extend(self.load_leaf_entries(store, &leaf.child_hash).await?);
        }
        Ok(entries)
    }

    async fn collect_summary_levels_with_overlay(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
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

    fn collect_summary_levels_for_node_with_overlay<'a, S>(
        &'a self,
        store: &'a S,
        overlay: &'a storage::TrackedStateChunkOverlay,
        hash: [u8; TRACKED_STATE_HASH_BYTES],
        levels: &'a mut Vec<Vec<ChildSummary>>,
    ) -> Pin<Box<dyn Future<Output = Result<(ChildSummary, usize), LixError>> + 'a>>
    where
        S: StorageRead + Send + Sync + ?Sized + 'a,
    {
        Box::pin(async move {
            match self.load_node_with_overlay(store, overlay, &hash).await? {
                DecodedNode::Leaf(leaf) => {
                    let summary = leaf_summary(hash, leaf.entries());
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

    async fn load_leaf_entries(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        match self.load_node(store, hash).await? {
            DecodedNode::Leaf(leaf) => Ok(leaf.entries().to_vec()),
            DecodedNode::Internal(_) => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state expected leaf chunk but found internal node",
            )),
        }
    }

    async fn load_leaf_entries_with_overlay(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        match self.load_node_with_overlay(store, overlay, hash).await? {
            DecodedNode::Leaf(leaf) => Ok(leaf.entries().to_vec()),
            DecodedNode::Internal(_) => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state expected leaf chunk but found internal node",
            )),
        }
    }

    async fn load_node(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<DecodedNode, LixError> {
        let bytes = self.load_node_bytes(store, hash).await?;
        decode_node(&bytes)
    }

    async fn load_node_bytes(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Vec<u8>, LixError> {
        let bytes = storage::read_chunk(store, hash).await?.ok_or_else(|| {
            LixError::new("LIX_ERROR_UNKNOWN", "tracked-state tree chunk is missing")
        })?;
        storage::verify_chunk_hash(hash, &bytes)?;
        Ok(bytes)
    }

    async fn load_node_with_overlay(
        &self,
        store: &(impl StorageRead + Send + Sync + ?Sized),
        overlay: &storage::TrackedStateChunkOverlay,
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<DecodedNode, LixError> {
        let bytes = overlay.read_chunk(store, hash).await?.ok_or_else(|| {
            LixError::new("LIX_ERROR_UNKNOWN", "tracked-state tree chunk is missing")
        })?;
        storage::verify_chunk_hash(hash, &bytes)?;
        decode_node(&bytes)
    }
}

#[derive(Debug)]
struct BuiltTree {
    root_id: TrackedStateRootId,
    chunks: Vec<PendingChunkWrite>,
    row_count: usize,
    tree_height: usize,
    chunk_bytes: usize,
}

struct ParentLevelPatch {
    parent_start: usize,
    old_parent_count: usize,
    replacement_parents: Vec<ChildSummary>,
}

struct SeekPathFrame {
    children: Vec<ChildSummary>,
    child_index: usize,
}

#[derive(Debug, Clone)]
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
    leaf: &DecodedLeafNodeRef<'_>,
    encoded_key: &[u8],
) -> Result<Option<usize>, LixError> {
    let mut low = 0usize;
    let mut high = leaf.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let key = leaf.key(mid)?.ok_or_else(|| {
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

struct LeafSummaryCursor {
    stack: Vec<LeafSummaryCursorFrame>,
    current: Option<ChildSummary>,
}

struct LeafSummaryCursorFrame {
    children: Vec<ChildSummary>,
    next_index: usize,
    children_are_leaves: bool,
}

impl LeafSummaryCursor {
    async fn new(
        tree: &TrackedStateTree,
        store: &(impl StorageRead + Send + Sync),
        root_hash: [u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<Self, LixError> {
        let mut cursor = Self {
            stack: Vec::new(),
            current: None,
        };
        match tree.load_node(store, &root_hash).await? {
            DecodedNode::Leaf(leaf) => {
                cursor.current = Some(leaf_summary(root_hash, leaf.entries()));
            }
            DecodedNode::Internal(internal) => {
                let children = internal.children().to_vec();
                let children_are_leaves =
                    child_summaries_are_leaves(tree, store, &children).await?;
                cursor.stack.push(LeafSummaryCursorFrame {
                    children,
                    next_index: 0,
                    children_are_leaves,
                });
                cursor.advance(tree, store).await?;
            }
        }
        Ok(cursor)
    }

    fn current(&self) -> Option<&ChildSummary> {
        self.current.as_ref()
    }

    async fn advance(
        &mut self,
        tree: &TrackedStateTree,
        store: &(impl StorageRead + Send + Sync),
    ) -> Result<(), LixError> {
        self.current = None;
        while let Some(frame) = self.stack.last_mut() {
            if frame.next_index >= frame.children.len() {
                self.stack.pop();
                continue;
            }

            let next = frame.children[frame.next_index].clone();
            let next_is_leaf = frame.children_are_leaves;
            frame.next_index += 1;
            if next_is_leaf {
                self.current = Some(next);
                return Ok(());
            }
            self.descend_to_leaf(tree, store, next).await?;
            return Ok(());
        }
        Ok(())
    }

    async fn descend_to_leaf(
        &mut self,
        tree: &TrackedStateTree,
        store: &(impl StorageRead + Send + Sync),
        mut summary: ChildSummary,
    ) -> Result<(), LixError> {
        loop {
            match tree.load_node(store, &summary.child_hash).await? {
                DecodedNode::Leaf(_) => {
                    self.current = Some(summary);
                    return Ok(());
                }
                DecodedNode::Internal(internal) => {
                    let children = internal.children().to_vec();
                    let children_are_leaves =
                        child_summaries_are_leaves(tree, store, &children).await?;
                    let Some(first_child) = children.first().cloned() else {
                        return Err(LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            "tracked-state internal node has no children",
                        ));
                    };
                    self.stack.push(LeafSummaryCursorFrame {
                        children,
                        next_index: 1,
                        children_are_leaves,
                    });
                    if children_are_leaves {
                        self.current = Some(first_child);
                        return Ok(());
                    } else {
                        summary = first_child;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct LeafChunkAccumulator {
    entries: Vec<EncodedLeafEntry>,
    key_bytes: usize,
}

#[derive(Debug, Default)]
struct LeafChunkRefAccumulator<'a> {
    entries: Vec<EncodedLeafEntryRef<'a>>,
    key_bytes: usize,
}

#[derive(Debug, Default)]
struct InternalChunkAccumulator {
    children: Vec<ChildSummary>,
    first_key_bytes: usize,
    last_key_bytes: usize,
}

#[derive(Debug, Default)]
struct InternalChunkRefAccumulator<'a> {
    children: Vec<ChildSummaryRef<'a>>,
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

fn chunk_leaf_entry_refs<'a>(
    entries: impl IntoIterator<Item = EncodedLeafEntryRef<'a>>,
    options: &TrackedStateTreeOptions,
) -> Vec<LeafChunkRefAccumulator<'a>> {
    let mut iter = entries.into_iter().peekable();
    if iter.peek().is_none() {
        return vec![LeafChunkRefAccumulator::default()];
    }
    let mut groups = Vec::new();
    let mut current = LeafChunkRefAccumulator::default();
    for entry in iter {
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
                        entry.key,
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
            + std::mem::size_of::<u64>();
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

fn chunk_internal_entry_refs<'a>(
    children: impl IntoIterator<Item = ChildSummaryRef<'a>>,
    options: &TrackedStateTreeOptions,
    level: usize,
) -> Vec<InternalChunkRefAccumulator<'a>> {
    let mut groups = Vec::new();
    let mut current = InternalChunkRefAccumulator::default();
    for child in children {
        let item_size = child.first_key.len()
            + child.last_key.len()
            + TRACKED_STATE_HASH_BYTES
            + std::mem::size_of::<u64>();
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
                        child.first_key,
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
    16 + child_count * (8 + TRACKED_STATE_HASH_BYTES + std::mem::size_of::<u64>())
        + first_key_bytes
        + last_key_bytes
}

fn first_resync_index(
    generated: &[ChildSummary],
    existing: &[ChildSummary],
    mutation_key: &[u8],
) -> Option<(usize, usize)> {
    for (generated_index, generated) in generated.iter().enumerate() {
        // A matching old chunk before the mutation key is only unchanged
        // prefix; resync is only valid after the mutation has been emitted.
        if generated.first_key.as_slice() <= mutation_key {
            continue;
        }
        if let Some(existing_index) = existing.iter().position(|existing| generated == existing) {
            return Some((generated_index, existing_index));
        }
    }
    None
}

fn internal_boundaries_match(left: &[ChildSummary], right: &[ChildSummary]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.first_key == right.first_key && left.last_key == right.last_key
        })
}

async fn child_summaries_are_leaves(
    tree: &TrackedStateTree,
    store: &(impl StorageRead + Send + Sync),
    children: &[ChildSummary],
) -> Result<bool, LixError> {
    let Some(first_child) = children.first() else {
        return Ok(false);
    };
    Ok(matches!(
        tree.load_node(store, &first_child.child_hash).await?,
        DecodedNode::Leaf(_)
    ))
}

fn decode_entry(
    entry: &EncodedLeafEntry,
) -> Result<(TrackedStateKey, TrackedStateIndexValue), LixError> {
    Ok((decode_key(&entry.key)?, decode_value(&entry.value)?))
}

fn parent_index_for_child_index(
    old_children: &[ChildSummary],
    old_parents: &[ChildSummary],
    child_index: usize,
) -> usize {
    let key = if child_index < old_children.len() {
        old_children[child_index].first_key.as_slice()
    } else {
        old_children
            .last()
            .map(|child| child.last_key.as_slice())
            .unwrap_or_default()
    };
    old_parents
        .iter()
        .position(|parent| parent.last_key.as_slice() >= key)
        .unwrap_or_else(|| old_parents.len().saturating_sub(1))
}

fn child_range_for_parent(
    old_children: &[ChildSummary],
    parent: &ChildSummary,
) -> Result<Range<usize>, LixError> {
    let start = old_children
        .iter()
        .position(|child| child.last_key.as_slice() >= parent.first_key.as_slice())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state parent summary does not overlap child summaries",
            )
        })?;
    let end = old_children[start..]
        .iter()
        .position(|child| child.last_key == parent.last_key)
        .map(|offset| start + offset + 1)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked-state parent summary end does not match child summaries",
            )
        })?;
    Ok(start..end)
}

fn leaf_summary(
    hash: [u8; TRACKED_STATE_HASH_BYTES],
    entries: &[EncodedLeafEntry],
) -> ChildSummary {
    ChildSummary {
        first_key: entries
            .first()
            .map(|entry| entry.key.clone())
            .unwrap_or_default(),
        last_key: entries
            .last()
            .map(|entry| entry.key.clone())
            .unwrap_or_default(),
        child_hash: hash,
        subtree_count: entries.len() as u64,
    }
}

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

    let can_bind_entity = !request.entity_pks.is_empty()
        && !request.file_ids.is_empty()
        && request
            .file_ids
            .iter()
            .all(|filter| !matches!(filter, NullableKeyFilter::Any));

    let mut ranges = Vec::new();
    for schema_key in &request.schema_keys {
        if can_bind_entity {
            for file_filter in &request.file_ids {
                let file_id = match file_filter {
                    NullableKeyFilter::Null => None,
                    NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
                    NullableKeyFilter::Any => unreachable!("filtered above"),
                };
                for entity_pk in &request.entity_pks {
                    let key = TrackedStateKey {
                        schema_key: schema_key.clone(),
                        file_id: file_id.clone(),
                        entity_pk: entity_pk.clone(),
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
            let prefix = match file_filter {
                NullableKeyFilter::Null => encode_schema_file_prefix(schema_key, None),
                NullableKeyFilter::Value(file_id) => {
                    encode_schema_file_prefix(schema_key, Some(file_id))
                }
                NullableKeyFilter::Any => unreachable!("handled above"),
            };
            ranges.push(prefix_scan_range(prefix));
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
    if !request.entity_pks.is_empty() {
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
        prefix_len: ranges.first()?.start.len(),
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
            child.last_key.as_slice() >= range.start.as_slice()
                && range
                    .end
                    .as_ref()
                    .is_none_or(|end| child.first_key.as_slice() < end.as_slice())
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
    if !request.entity_pks.is_empty() && !request.entity_pks.contains(&key.entity_pk) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::codec::encode_value;

    #[tokio::test]
    async fn exact_read_roundtrips_from_applied_root() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "entity");
        let value = value("change-1", Some("{}"));
        let result =
            apply_mutations_for_test(&tree, &storage, None, vec![mutation(&key, &value)], None)
                .await
                .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert_eq!(
            tree.get(&store, &result.root_id, &key)
                .await
                .expect("row should load"),
            Some(value)
        );
    }

    #[tokio::test]
    async fn latest_mutation_for_key_wins() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "entity");
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            .map(|(key, _)| key.entity_pk.as_single_string_owned().expect("identity"))
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
            .map(|(key, _)| key.entity_pk.as_single_string_owned().expect("identity"))
            .collect::<Vec<_>>();
        assert_eq!(live_identities, vec!["visible"]);
    }

    #[tokio::test]
    async fn scan_filters_by_schema_entity_and_file() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::new();
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation_owned(
                    key("schema-a", Some("file-a"), "entity-a"),
                    value("c1", Some("{}")),
                ),
                mutation_owned(
                    key("schema-a", Some("file-b"), "entity-a"),
                    value("c2", Some("{}")),
                ),
                mutation_owned(
                    key("schema-a", Some("file-a"), "entity-b"),
                    value("c3", Some("{}")),
                ),
                mutation_owned(
                    key("schema-b", Some("file-a"), "entity-a"),
                    value("c4", Some("{}")),
                ),
            ],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    entity_pks: vec![crate::entity_pk::EntityPk::single("entity-a")],
                    file_ids: vec![crate::NullableKeyFilter::Value("file-a".to_string())],
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
                .entity_pk
                .as_single_string_owned()
                .expect("identity"),
            "entity-a"
        );
        assert_eq!(rows[0].0.file_id.as_deref(), Some("file-a"));
    }

    #[tokio::test]
    async fn scan_schema_file_prefix_honors_tombstones_and_limit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::new();
        let result = apply_mutations_for_test(
            &tree,
            &storage,
            None,
            vec![
                mutation_owned(
                    key("schema-a", Some("file-a"), "entity-a"),
                    value("c1", Some("{}")),
                ),
                mutation_owned(
                    key("schema-a", Some("file-a"), "entity-b"),
                    value("c2", None),
                ),
                mutation_owned(
                    key("schema-a", Some("file-a"), "entity-c"),
                    value("c3", Some("{}")),
                ),
                mutation_owned(
                    key("schema-a", Some("file-b"), "entity-d"),
                    value("c4", Some("{}")),
                ),
            ],
            None,
        )
        .await
        .expect("mutations should apply");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let rows = tree
            .scan(
                &store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    file_ids: vec![crate::NullableKeyFilter::Value("file-a".to_string())],
                    include_tombstones: false,
                    limit: Some(2),
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(
            |(key, _)| key.schema_key == "schema-a" && key.file_id.as_deref() == Some("file-a")
        ));
        assert_eq!(
            rows.iter()
                .map(|(key, _)| key.entity_pk.as_single_string_owned().expect("identity"))
                .collect::<Vec<_>>(),
            vec!["entity-a", "entity-c"]
        );
    }

    #[tokio::test]
    async fn applying_to_base_root_reuses_existing_rows_and_overwrites_changed_rows() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::new();
        let shared_key = key("schema", None, "shared");
        let branch_a_key = key("schema", None, "branch-a");
        let branch_b_key = key("schema", None, "branch-b");
        let shared_value = value("shared-change", Some("{\"shared\":true}"));
        let branch_a_value = value("branch-a-change", Some("{\"branch\":\"a\"}"));
        let branch_b_value = value("branch-b-change", Some("{\"branch\":\"b\"}"));
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
        assert!(tree
            .get(&store, &branch_a.root_id, &branch_b_key)
            .await
            .expect("branch a should read")
            .is_none());
        assert!(tree
            .get(&store, &branch_b.root_id, &branch_a_key)
            .await
            .expect("branch b should read")
            .is_none());
    }

    #[tokio::test]
    async fn single_update_matches_full_canonical_rebuild() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("entity-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let changed_key = key("schema", None, "entity-000");
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
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        assert!(canonical_entries
            .windows(2)
            .all(|window| window[0].key < window[1].key));
        let encoded_changed_key = encode_key(&changed_key);
        let encoded_changed_value = encode_value(&changed_value);
        let index = canonical_entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(&encoded_changed_key))
            .expect("changed key should exist");
        canonical_entries[index].value = encoded_changed_value;
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn single_insert_matches_full_canonical_rebuild() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("entity-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let inserted_key = key("schema", None, "entity-050a");
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
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        let encoded_inserted_key = encode_key(&inserted_key);
        let encoded_inserted_value = encode_value(&inserted_value);
        let index = canonical_entries
            .binary_search_by(|entry| entry.key.as_slice().cmp(&encoded_inserted_key))
            .expect_err("inserted key should not exist");
        canonical_entries.insert(
            index,
            EncodedLeafEntry {
                key: encoded_inserted_key,
                value: encoded_inserted_value,
            },
        );
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn batch_update_matches_full_canonical_rebuild() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
        let rows = (0..100)
            .map(|index| {
                mutation_owned(
                    key("schema", None, &format!("entity-{index:03}")),
                    value(&format!("c-{index}"), Some(&format!("{{\"v\":{index}}}"))),
                )
            })
            .collect::<Vec<_>>();
        let updates = (10..25)
            .map(|index| {
                (
                    key("schema", None, &format!("entity-{index:03}")),
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
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        for (key, value) in updates {
            let encoded_key = encode_key(&key);
            let encoded_value = encode_value(&value);
            let index = canonical_entries
                .binary_search_by(|entry| entry.key.as_slice().cmp(&encoded_key))
                .expect("updated key should exist");
            canonical_entries[index].value = encoded_value;
        }
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
    }

    #[tokio::test]
    async fn batch_insert_matches_full_canonical_rebuild() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let tree = TrackedStateTree::with_options(TrackedStateTreeOptions {
            target_chunk_bytes: 128,
            min_chunk_bytes: 64,
            max_chunk_bytes: 256,
        });
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
            .map(|(index, entity_pk)| {
                (
                    key("schema", None, entity_pk),
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
            .expect("read should open");
        let mut canonical_entries = tree
            .collect_leaf_entries(&read, &base.root_id)
            .await
            .expect("base entries should collect");
        for (key, value) in inserts {
            let encoded_key = encode_key(&key);
            let encoded_value = encode_value(&value);
            let index = canonical_entries
                .binary_search_by(|entry| entry.key.as_slice().cmp(&encoded_key))
                .expect_err("inserted key should not exist");
            canonical_entries.insert(
                index,
                EncodedLeafEntry {
                    key: encoded_key,
                    value: encoded_value,
                },
            );
        }
        let canonical = tree
            .build_tree_from_entries(canonical_entries)
            .expect("canonical root should build");

        assert_eq!(fast.root_id, canonical.root_id);
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
        storage: &StorageContext,
        base_root: Option<&TrackedStateRootId>,
        mutations: Vec<TrackedStateMutation>,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let result = tree
            .apply_mutations(&read, &mut writes, base_root, mutations, commit_id)
            .await?;
        storage.commit_write_set(writes, StorageWriteOptions::default())?;
        Ok(result)
    }

    fn mutation(key: &TrackedStateKey, value: &TrackedStateIndexValue) -> TrackedStateMutation {
        TrackedStateMutation::put_encoded(encode_key(key), encode_value(value))
    }

    fn mutation_owned(key: TrackedStateKey, value: TrackedStateIndexValue) -> TrackedStateMutation {
        mutation(&key, &value)
    }

    fn encoded_entries_with_change_id(change_id: &str) -> Vec<EncodedLeafEntry> {
        (0..64)
            .map(|index| {
                let key = key("schema", None, &format!("entity-{index:03}"));
                EncodedLeafEntry {
                    key: encode_key(&key),
                    value: encode_value(&value(change_id, Some("{}"))),
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
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                let last_key = group
                    .entries
                    .last()
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                (first_key, last_key, group.entries.len())
            })
            .collect()
    }

    fn key(schema_key: &str, file_id: Option<&str>, entity_pk: &str) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            entity_pk: EntityPk::single(entity_pk),
        }
    }

    fn value(change_id: &str, snapshot_content: Option<&str>) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_id: change_id.to_string(),
            commit_id: "commit".to_string(),
            deleted: snapshot_content.is_none(),
            snapshot_ref: snapshot_content
                .map(|content| crate::json_store::JsonRef::for_content(content.as_bytes())),
            metadata_ref: None,
            created_updated_at: TrackedStateIndexValue::created_updated_at(
                "2026-01-01T00:00:00Z".to_string(),
                "2026-01-01T00:00:00Z".to_string(),
            ),
        }
    }
}
