mod cache;
mod codec;
#[allow(dead_code)]
mod mutator;
mod storage;
mod types;

use crate::live_tracked_state::cache::LiveTrackedNodeCache;
use crate::live_tracked_state::codec::{
    boundary_trigger, child_summary_from_node, compare_encoded_key_to_key, decode_entity_key,
    decode_entity_value, encode_entity_key, encode_entity_value_canonical, encode_internal_node,
    encode_leaf_node, hash_bytes, leaf_codec_profile, ChildSummary, DecodedNode, EncodedLeafEntry,
    PendingChunkWrite, PendingValueWrite,
};
use crate::live_tracked_state::storage::{
    LiveTrackedChunkStore, LiveTrackedRootStore, SqlLiveTrackedStorage,
};
use crate::{LixBackend, LixError};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub use types::{
    LiveTrackedApplyResult, LiveTrackedCodecProfile, LiveTrackedEntityKey, LiveTrackedEntityValue,
    LiveTrackedFieldValue, LiveTrackedKeyComponent, LiveTrackedKeyField, LiveTrackedMutation,
    LiveTrackedPayloadColumn, LiveTrackedRangeBound, LiveTrackedRangeField,
    LiveTrackedRangeRequest, LiveTrackedReadRequest, LiveTrackedRootId, LiveTrackedRow,
    LiveTrackedScan, LiveTrackedStateOptions, LiveTrackedValueRef,
};

pub struct LiveTrackedState<'a> {
    backend: &'a dyn LixBackend,
    storage: SqlLiveTrackedStorage,
    cache: Arc<LiveTrackedNodeCache>,
    options: LiveTrackedStateOptions,
}

#[derive(Debug, Clone)]
struct BuiltTree {
    root_id: LiveTrackedRootId,
    chunks: Vec<PendingChunkWrite>,
    values: Vec<PendingValueWrite>,
    row_count: usize,
    tree_height: usize,
    chunk_bytes: usize,
    value_ref_bytes: usize,
}

#[derive(Debug, Default)]
struct LeafChunkAccumulator {
    entries: Vec<EncodedLeafEntry>,
    key_bytes: usize,
    value_bytes: usize,
}

#[derive(Debug, Default)]
struct InternalChunkAccumulator {
    children: Vec<ChildSummary>,
    first_key_bytes: usize,
    last_key_bytes: usize,
}

#[derive(Debug)]
struct PreparedMutation {
    key: LiveTrackedEntityKey,
    entry: EncodedLeafEntry,
}

#[derive(Debug)]
struct PreparedMutations {
    mutations: Vec<PreparedMutation>,
    values: Vec<PendingValueWrite>,
    value_ref_bytes: usize,
}

#[derive(Debug)]
struct RootLeafTopology {
    leaf_summaries: Vec<ChildSummary>,
    tree_height: usize,
}

#[derive(Debug, Default)]
struct LeafStreamRechunker {
    emitted: Vec<LeafChunkAccumulator>,
    current: LeafChunkAccumulator,
}

const INCREMENTAL_LEAF_REWRITE_MUTATION_CUTOFF: usize = 1_024;

impl<'a> LiveTrackedState<'a> {
    pub fn new(backend: &'a dyn LixBackend) -> Self {
        Self::with_options(backend, LiveTrackedStateOptions::default())
    }

    pub fn with_options(backend: &'a dyn LixBackend, options: LiveTrackedStateOptions) -> Self {
        Self {
            backend,
            storage: SqlLiveTrackedStorage,
            cache: Arc::new(LiveTrackedNodeCache::new(options.cache_capacity)),
            options,
        }
    }

    pub fn options(&self) -> &LiveTrackedStateOptions {
        &self.options
    }

    pub async fn ensure_schema(&self) -> Result<(), LixError> {
        self.storage.ensure_schema(self.backend).await
    }

    pub fn clear_cache(&self) {
        self.cache.clear();
    }

    pub async fn load_root(&self, commit_id: &str) -> Result<Option<LiveTrackedRootId>, LixError> {
        self.storage.load_root(self.backend, commit_id).await
    }

    pub async fn store_root(
        &self,
        commit_id: &str,
        root_id: &LiveTrackedRootId,
    ) -> Result<(), LixError> {
        let mut transaction = self.backend.begin_transaction().await?;
        self.storage
            .store_root(transaction.as_mut(), commit_id, root_id)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn apply_mutations<I>(
        &self,
        base_root: Option<&LiveTrackedRootId>,
        mutations: I,
    ) -> Result<LiveTrackedApplyResult, LixError>
    where
        I: IntoIterator<Item = LiveTrackedMutation>,
    {
        self.persist_built_tree(None, base_root, mutations).await
    }

    pub async fn apply_mutations_and_store_root<I>(
        &self,
        commit_id: &str,
        base_root: Option<&LiveTrackedRootId>,
        mutations: I,
    ) -> Result<LiveTrackedApplyResult, LixError>
    where
        I: IntoIterator<Item = LiveTrackedMutation>,
    {
        self.persist_built_tree(Some(commit_id), base_root, mutations)
            .await
    }

    pub async fn get(
        &self,
        root_id: &LiveTrackedRootId,
        key: &LiveTrackedEntityKey,
    ) -> Result<Option<LiveTrackedEntityValue>, LixError> {
        let mut current = *root_id.as_bytes();
        loop {
            let node = self.load_node(&current).await?;
            match node.as_ref() {
                DecodedNode::Leaf(leaf) => {
                    if let Some(index) = leaf_binary_search(leaf, key) {
                        return Ok(Some(decode_entity_value(leaf.value_at(index))?));
                    }
                    return Ok(None);
                }
                DecodedNode::Internal(internal) => {
                    let index = internal_child_index(internal, key);
                    current = *internal.child_hash_at(index);
                }
            }
        }
    }

    pub async fn read(
        &self,
        root_id: &LiveTrackedRootId,
        request: &LiveTrackedReadRequest,
    ) -> Result<Option<LiveTrackedEntityValue>, LixError> {
        self.get(root_id, &request.key).await
    }

    pub async fn scan(
        &self,
        root_id: &LiveTrackedRootId,
        range: &LiveTrackedRangeRequest,
    ) -> Result<LiveTrackedScan, LixError> {
        range.validate()?;
        let mut rows = Vec::new();
        let mut current_level = vec![*root_id.as_bytes()];
        while !current_level.is_empty() {
            let nodes = self.load_many_nodes(&current_level).await?;
            let mut next_level = Vec::new();
            for node in nodes {
                match node.as_ref() {
                    DecodedNode::Leaf(leaf) => {
                        for index in 0..leaf.entry_count() {
                            let key = decode_entity_key(leaf.key_at(index))?;
                            if range.matches(&key) {
                                rows.push(LiveTrackedRow::new(
                                    key,
                                    decode_entity_value(leaf.value_at(index))?,
                                ));
                            } else if range.contiguous && key_above_upper_bound(range, &key) {
                                break;
                            }
                        }
                    }
                    DecodedNode::Internal(internal) => {
                        for index in 0..internal.child_count() {
                            if range.contiguous {
                                let first_key = decode_entity_key(internal.first_key_at(index))?;
                                let last_key = decode_entity_key(internal.last_key_at(index))?;
                                if key_above_upper_bound(range, &first_key)
                                    || key_below_lower_bound(range, &last_key)
                                {
                                    continue;
                                }
                            }
                            next_level.push(*internal.child_hash_at(index));
                        }
                    }
                }
            }
            current_level = next_level;
        }
        Ok(LiveTrackedScan::new(rows))
    }

    pub async fn profile_leaf_codec(
        &self,
        rows: &[LiveTrackedRow],
    ) -> Result<LiveTrackedCodecProfile, LixError> {
        leaf_codec_profile(rows, &self.options)
    }

    async fn persist_built_tree<I>(
        &self,
        commit_id: Option<&str>,
        base_root: Option<&LiveTrackedRootId>,
        mutations: I,
    ) -> Result<LiveTrackedApplyResult, LixError>
    where
        I: IntoIterator<Item = LiveTrackedMutation>,
    {
        let built = self
            .build_tree_from_mutations(base_root, mutations.into_iter().collect())
            .await?;
        let mut transaction = self.backend.begin_transaction().await?;
        self.storage
            .write_values(transaction.as_mut(), &built.values)
            .await?;
        self.storage
            .write_chunks(transaction.as_mut(), &built.chunks)
            .await?;
        let persisted_root = if let Some(commit_id) = commit_id {
            self.storage
                .store_root(transaction.as_mut(), commit_id, &built.root_id)
                .await?;
            true
        } else {
            false
        };
        transaction.commit().await?;
        self.populate_cache(&built.chunks)?;
        Ok(LiveTrackedApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
            value_ref_count: built.values.len(),
            value_ref_bytes: built.value_ref_bytes,
            persisted_root,
        })
    }

    async fn build_tree_from_mutations(
        &self,
        base_root: Option<&LiveTrackedRootId>,
        mutations: Vec<LiveTrackedMutation>,
    ) -> Result<BuiltTree, LixError> {
        let prepared = self.prepare_mutations(sort_and_dedup_mutations(mutations))?;
        if let Some(base_root) = base_root {
            return self
                .build_tree_from_base_and_prepared_mutations(base_root, prepared)
                .await;
        }
        let PreparedMutations {
            mutations,
            values,
            value_ref_bytes,
        } = prepared;
        self.build_tree_from_encoded_entries(
            mutations
                .into_iter()
                .map(|mutation| mutation.entry)
                .collect(),
            values,
            value_ref_bytes,
        )
    }

    fn prepare_mutations(
        &self,
        mutations: Vec<LiveTrackedMutation>,
    ) -> Result<PreparedMutations, LixError> {
        let mut value_writes = BTreeMap::new();
        let mut prepared = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            let (key, value) = match mutation {
                LiveTrackedMutation::Put { key, value }
                | LiveTrackedMutation::Delete { key, value } => (key, value),
            };
            let encoded_key = encode_entity_key(&key);
            let value = encode_entity_value_canonical(&value, &self.options, &mut value_writes)?;
            prepared.push(PreparedMutation {
                key,
                entry: EncodedLeafEntry {
                    key: encoded_key,
                    value,
                },
            });
        }

        let value_ref_bytes = value_writes.values().map(|value| value.size_bytes).sum();
        Ok(PreparedMutations {
            mutations: prepared,
            values: value_writes.into_values().collect(),
            value_ref_bytes,
        })
    }

    async fn build_tree_from_base_and_prepared_mutations(
        &self,
        base_root: &LiveTrackedRootId,
        prepared: PreparedMutations,
    ) -> Result<BuiltTree, LixError> {
        let PreparedMutations {
            mutations,
            values,
            value_ref_bytes,
        } = prepared;
        if mutations.is_empty() {
            let topology = self.load_root_leaf_topology(base_root).await?;
            let row_count = topology
                .leaf_summaries
                .iter()
                .map(|summary| summary.subtree_count as usize)
                .sum();
            return Ok(BuiltTree {
                root_id: base_root.clone(),
                chunks: Vec::new(),
                values,
                row_count,
                tree_height: topology.tree_height,
                chunk_bytes: 0,
                value_ref_bytes,
            });
        }

        if mutations.len() >= INCREMENTAL_LEAF_REWRITE_MUTATION_CUTOFF {
            let leaf_entries = self
                .merge_base_root_with_prepared_mutations(base_root, mutations)
                .await?;
            return self.build_tree_from_encoded_entries(leaf_entries, values, value_ref_bytes);
        }

        self.build_tree_from_base_with_dolt_chunker(
            base_root,
            PreparedMutations {
                mutations,
                values,
                value_ref_bytes,
            },
        )
        .await
    }

    fn build_tree_from_level_summaries(
        &self,
        mut summaries: Vec<ChildSummary>,
        values: Vec<PendingValueWrite>,
        value_ref_bytes: usize,
        mut chunk_map: BTreeMap<[u8; 32], PendingChunkWrite>,
        mut tree_height: usize,
    ) -> Result<BuiltTree, LixError> {
        let row_count = summaries
            .iter()
            .map(|summary| summary.subtree_count as usize)
            .sum();
        while summaries.len() > 1 {
            summaries = self.build_internal_level(summaries, tree_height, &mut chunk_map);
            tree_height += 1;
        }
        let root_summary = summaries
            .pop()
            .ok_or_else(|| LixError::unknown("live tracked tree build produced no root"))?;
        let root_id = LiveTrackedRootId::new(root_summary.child_hash);
        let chunks = chunk_map.into_values().collect::<Vec<_>>();
        let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
        Ok(BuiltTree {
            root_id,
            chunks,
            values,
            row_count,
            tree_height,
            chunk_bytes,
            value_ref_bytes,
        })
    }

    async fn load_root_leaf_topology(
        &self,
        root_id: &LiveTrackedRootId,
    ) -> Result<RootLeafTopology, LixError> {
        let root_hash = *root_id.as_bytes();
        self.collect_root_leaf_topology(root_hash).await
    }

    async fn rewrite_leaf_level_incremental(
        &self,
        leaf_summaries: &[ChildSummary],
        mutations: &[PreparedMutation],
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<Vec<ChildSummary>, LixError> {
        let existing_leaf_hashes = leaf_summaries
            .iter()
            .map(|summary| summary.child_hash)
            .collect::<HashSet<_>>();
        let mut rebuilt = Vec::with_capacity(leaf_summaries.len() + mutations.len());
        let mut mutation_index = 0;
        let mut leaf_index = 0;

        while leaf_index < leaf_summaries.len() {
            let next_first_key = leaf_summaries
                .get(leaf_index + 1)
                .map(|summary| summary.first_key.as_slice());
            let leaf_mutation_end =
                mutation_partition_end(mutations, mutation_index, next_first_key);
            if leaf_mutation_end == mutation_index {
                rebuilt.push(leaf_summaries[leaf_index].clone());
                leaf_index += 1;
                continue;
            }

            let mut rechunker = LeafStreamRechunker::default();
            loop {
                let summary = &leaf_summaries[leaf_index];
                let next_first_key = leaf_summaries
                    .get(leaf_index + 1)
                    .map(|next| next.first_key.as_slice());
                let leaf_mutation_end =
                    mutation_partition_end(mutations, mutation_index, next_first_key);
                let node = self.load_node(&summary.child_hash).await?;
                let DecodedNode::Leaf(leaf) = node.as_ref() else {
                    return Err(LixError::unknown(
                        "live tracked incremental rewrite expected a leaf node",
                    ));
                };
                append_leaf_entries_with_mutations(
                    &mut rechunker,
                    leaf,
                    &mutations[mutation_index..leaf_mutation_end],
                    &self.options,
                );
                mutation_index = leaf_mutation_end;
                leaf_index += 1;

                if rechunker.at_boundary() {
                    append_rechunked_leaf_groups(
                        rechunker.take_groups(),
                        &existing_leaf_hashes,
                        chunk_map,
                        &mut rebuilt,
                    );
                    break;
                }

                if leaf_index == leaf_summaries.len() {
                    append_rechunked_leaf_groups(
                        rechunker.finish(),
                        &existing_leaf_hashes,
                        chunk_map,
                        &mut rebuilt,
                    );
                    break;
                }
            }
        }

        if mutation_index != mutations.len() {
            return Err(LixError::unknown(
                "live tracked incremental rewrite left trailing mutations unassigned",
            ));
        }
        Ok(rebuilt)
    }

    async fn merge_base_root_with_prepared_mutations(
        &self,
        root_id: &LiveTrackedRootId,
        mutations: Vec<PreparedMutation>,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let mut merged = Vec::with_capacity(mutations.len());
        let mut mutations = mutations.into_iter().peekable();
        let mut current_level = vec![*root_id.as_bytes()];
        while !current_level.is_empty() {
            let nodes = self.load_many_nodes(&current_level).await?;
            let mut next_level = Vec::new();
            for node in nodes {
                match node.as_ref() {
                    DecodedNode::Leaf(leaf) => {
                        for index in 0..leaf.entry_count() {
                            let base_key = leaf.key_at(index);
                            while let Some(mutation) = mutations.peek() {
                                if compare_encoded_key_to_key(base_key, &mutation.key)
                                    != Ordering::Greater
                                {
                                    break;
                                }
                                merged.push(mutations.next().expect("prepared mutation").entry);
                            }

                            match mutations.peek() {
                                Some(mutation) => {
                                    match compare_encoded_key_to_key(base_key, &mutation.key) {
                                        Ordering::Less => merged.push(EncodedLeafEntry {
                                            key: base_key.to_vec(),
                                            value: leaf.value_at(index).to_vec(),
                                        }),
                                        Ordering::Equal => {
                                            merged.push(
                                                mutations.next().expect("prepared mutation").entry,
                                            );
                                        }
                                        Ordering::Greater => {
                                            unreachable!("greater keys handled above")
                                        }
                                    }
                                }
                                None => merged.push(EncodedLeafEntry {
                                    key: base_key.to_vec(),
                                    value: leaf.value_at(index).to_vec(),
                                }),
                            }
                        }
                    }
                    DecodedNode::Internal(internal) => {
                        for index in 0..internal.child_count() {
                            next_level.push(*internal.child_hash_at(index));
                        }
                    }
                }
            }
            current_level = next_level;
        }
        merged.extend(mutations.map(|mutation| mutation.entry));
        Ok(merged)
    }

    fn build_tree_from_encoded_entries(
        &self,
        leaf_entries: Vec<EncodedLeafEntry>,
        values: Vec<PendingValueWrite>,
        value_ref_bytes: usize,
    ) -> Result<BuiltTree, LixError> {
        let row_count = leaf_entries.len();
        let mut chunk_map = BTreeMap::<[u8; 32], PendingChunkWrite>::new();
        let mut summaries = self.build_leaf_level(leaf_entries, &mut chunk_map);
        let mut tree_height = 1;
        while summaries.len() > 1 {
            summaries = self.build_internal_level(summaries, tree_height, &mut chunk_map);
            tree_height += 1;
        }
        let root_summary = summaries
            .pop()
            .ok_or_else(|| LixError::unknown("live tracked tree build produced no root"))?;
        let root_id = LiveTrackedRootId::new(root_summary.child_hash);
        let chunks = chunk_map.into_values().collect::<Vec<_>>();
        let chunk_bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
        Ok(BuiltTree {
            root_id,
            chunks,
            values,
            row_count,
            tree_height,
            chunk_bytes,
            value_ref_bytes,
        })
    }

    fn build_leaf_level(
        &self,
        entries: Vec<EncodedLeafEntry>,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Vec<ChildSummary> {
        let groups = chunk_leaf_entries(entries, &self.options);
        let mut summaries = Vec::with_capacity(groups.len());
        for group in groups {
            let node = encode_leaf_node(&group.entries);
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
            let (chunk, summary) =
                child_summary_from_node(node, first_key, last_key, subtree_count);
            chunk_map.entry(chunk.hash).or_insert(chunk);
            summaries.push(summary);
        }
        summaries
    }

    fn build_internal_level(
        &self,
        children: Vec<ChildSummary>,
        level: usize,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Vec<ChildSummary> {
        let groups = chunk_internal_entries(children, &self.options, level);
        let mut summaries = Vec::with_capacity(groups.len());
        for group in groups {
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
            chunk_map.entry(chunk.hash).or_insert(chunk);
            summaries.push(summary);
        }
        summaries
    }

    async fn load_node(&self, hash: &[u8; 32]) -> Result<Arc<DecodedNode>, LixError> {
        if let Some(node) = self.cache.get(hash) {
            return Ok(node);
        }
        let data = self
            .storage
            .read_chunk(self.backend, hash)
            .await?
            .ok_or_else(|| {
                LixError::unknown(format!("live tracked chunk {} is missing", hex(hash)))
            })?;
        verify_chunk_hash(hash, &data)?;
        let decoded = Arc::new(DecodedNode::decode(data)?);
        Ok(self.cache.insert(*hash, decoded))
    }

    async fn load_many_nodes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<Vec<Arc<DecodedNode>>, LixError> {
        let mut results = vec![None; hashes.len()];
        let mut missing_positions = HashMap::<[u8; 32], Vec<usize>>::new();
        let mut unique_missing = Vec::<[u8; 32]>::new();
        for (index, hash) in hashes.iter().copied().enumerate() {
            if let Some(node) = self.cache.get(&hash) {
                results[index] = Some(node);
            } else {
                missing_positions.entry(hash).or_default().push(index);
                if !unique_missing.contains(&hash) {
                    unique_missing.push(hash);
                }
            }
        }

        if !unique_missing.is_empty() {
            let fetched = self
                .storage
                .read_many(self.backend, &unique_missing)
                .await?;
            let mut fetched_map = HashMap::<[u8; 32], Arc<DecodedNode>>::new();
            for (hash, data) in fetched {
                verify_chunk_hash(&hash, &data)?;
                let decoded = Arc::new(DecodedNode::decode(data)?);
                let cached = self.cache.insert(hash, decoded);
                fetched_map.insert(hash, cached);
            }

            for hash in unique_missing {
                let node = fetched_map.get(&hash).cloned().ok_or_else(|| {
                    LixError::unknown(format!("live tracked chunk {} is missing", hex(&hash)))
                })?;
                if let Some(indexes) = missing_positions.get(&hash) {
                    for index in indexes {
                        results[*index] = Some(Arc::clone(&node));
                    }
                }
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                node.ok_or_else(|| {
                    LixError::unknown(format!(
                        "live tracked node load at position {index} returned no node"
                    ))
                })
            })
            .collect()
    }

    fn populate_cache(&self, chunks: &[PendingChunkWrite]) -> Result<(), LixError> {
        for chunk in chunks {
            let decoded = Arc::new(DecodedNode::decode(chunk.data.clone())?);
            self.cache.insert(chunk.hash, decoded);
        }
        Ok(())
    }

    async fn collect_root_leaf_topology(
        &self,
        root_hash: [u8; 32],
    ) -> Result<RootLeafTopology, LixError> {
        let root = self.load_node(&root_hash).await?;
        match root.as_ref() {
            DecodedNode::Leaf(leaf) => Ok(RootLeafTopology {
                leaf_summaries: vec![summary_from_root_leaf(root_hash, leaf)],
                tree_height: 1,
            }),
            DecodedNode::Internal(_) => {
                let mut current_hashes = vec![root_hash];
                let mut tree_height = 1;

                loop {
                    let nodes = self.load_many_nodes(&current_hashes).await?;
                    let mut next_hashes = Vec::new();
                    let mut next_summaries = Vec::new();

                    for node in &nodes {
                        match node.as_ref() {
                            DecodedNode::Leaf(_) => return Err(LixError::unknown(
                                "live tracked topology mixed internal and leaf nodes at one level",
                            )),
                            DecodedNode::Internal(internal) => {
                                for index in 0..internal.child_count() {
                                    next_hashes.push(*internal.child_hash_at(index));
                                    next_summaries
                                        .push(child_summary_from_internal_child(internal, index));
                                }
                            }
                        }
                    }

                    let Some(first_child_hash) = next_hashes.first() else {
                        return Err(LixError::unknown(
                            "live tracked topology traversal produced an empty child level",
                        ));
                    };
                    tree_height += 1;
                    let first_child = self.load_node(first_child_hash).await?;
                    if matches!(first_child.as_ref(), DecodedNode::Leaf(_)) {
                        return Ok(RootLeafTopology {
                            leaf_summaries: next_summaries,
                            tree_height,
                        });
                    }
                    current_hashes = next_hashes;
                }
            }
        }
    }
}

impl LeafStreamRechunker {
    fn append(&mut self, entry: EncodedLeafEntry, options: &LiveTrackedStateOptions) {
        let projected = estimate_leaf_chunk_size(
            self.current.entries.len() + 1,
            self.current.key_bytes + entry.key.len(),
            self.current.value_bytes + entry.value.len(),
        );
        if !self.current.entries.is_empty() && projected > options.max_chunk_bytes {
            self.emitted.push(std::mem::take(&mut self.current));
        }

        self.current.key_bytes += entry.key.len();
        self.current.value_bytes += entry.value.len();
        self.current.entries.push(entry);

        let previous_size = estimate_leaf_chunk_size(
            self.current.entries.len().saturating_sub(1),
            self.current.key_bytes.saturating_sub(
                self.current
                    .entries
                    .last()
                    .map(|entry| entry.key.len())
                    .unwrap_or(0),
            ),
            self.current.value_bytes.saturating_sub(
                self.current
                    .entries
                    .last()
                    .map(|entry| entry.value.len())
                    .unwrap_or(0),
            ),
        );
        let current_size = estimate_leaf_chunk_size(
            self.current.entries.len(),
            self.current.key_bytes,
            self.current.value_bytes,
        );
        let should_split = current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || self
                    .current
                    .entries
                    .last()
                    .map(|entry| {
                        boundary_trigger(
                            &entry.key,
                            0,
                            current_size,
                            current_size.saturating_sub(previous_size),
                            options.target_chunk_bytes,
                        )
                    })
                    .unwrap_or(false));
        if should_split {
            self.emitted.push(std::mem::take(&mut self.current));
        }
    }

    fn at_boundary(&self) -> bool {
        self.current.entries.is_empty()
    }

    fn take_groups(&mut self) -> Vec<LeafChunkAccumulator> {
        std::mem::take(&mut self.emitted)
    }

    fn finish(mut self) -> Vec<LeafChunkAccumulator> {
        if !self.current.entries.is_empty() {
            self.emitted.push(self.current);
        }
        self.emitted
    }
}

fn child_summary_from_internal_child(
    internal: &codec::DecodedInternalNode,
    index: usize,
) -> ChildSummary {
    ChildSummary {
        first_key: internal.first_key_at(index).to_vec(),
        last_key: internal.last_key_at(index).to_vec(),
        child_hash: *internal.child_hash_at(index),
        subtree_count: internal.subtree_count_at(index),
    }
}

fn summary_from_root_leaf(hash: [u8; 32], leaf: &codec::DecodedLeafNode) -> ChildSummary {
    ChildSummary {
        first_key: if leaf.entry_count() > 0 {
            leaf.key_at(0).to_vec()
        } else {
            Vec::new()
        },
        last_key: leaf
            .entry_count()
            .checked_sub(1)
            .map(|index| leaf.key_at(index).to_vec())
            .unwrap_or_default(),
        child_hash: hash,
        subtree_count: leaf.entry_count() as u64,
    }
}

fn mutation_partition_end(
    mutations: &[PreparedMutation],
    start: usize,
    next_first_key: Option<&[u8]>,
) -> usize {
    let Some(next_first_key) = next_first_key else {
        return mutations.len();
    };
    let mut end = start;
    while end < mutations.len()
        && compare_encoded_key_to_key(next_first_key, &mutations[end].key) == Ordering::Greater
    {
        end += 1;
    }
    end
}

fn append_leaf_entries_with_mutations(
    rechunker: &mut LeafStreamRechunker,
    leaf: &codec::DecodedLeafNode,
    mutations: &[PreparedMutation],
    options: &LiveTrackedStateOptions,
) {
    let mut mutation_index = 0;
    for index in 0..leaf.entry_count() {
        let base_key = leaf.key_at(index);
        while mutation_index < mutations.len()
            && compare_encoded_key_to_key(base_key, &mutations[mutation_index].key)
                == Ordering::Greater
        {
            rechunker.append(clone_prepared_entry(&mutations[mutation_index]), options);
            mutation_index += 1;
        }

        match mutations.get(mutation_index) {
            Some(mutation) => match compare_encoded_key_to_key(base_key, &mutation.key) {
                Ordering::Less => rechunker.append(
                    EncodedLeafEntry {
                        key: base_key.to_vec(),
                        value: leaf.value_at(index).to_vec(),
                    },
                    options,
                ),
                Ordering::Equal => {
                    rechunker.append(clone_prepared_entry(mutation), options);
                    mutation_index += 1;
                }
                Ordering::Greater => unreachable!("greater mutations are drained before compare"),
            },
            None => rechunker.append(
                EncodedLeafEntry {
                    key: base_key.to_vec(),
                    value: leaf.value_at(index).to_vec(),
                },
                options,
            ),
        }
    }

    while mutation_index < mutations.len() {
        rechunker.append(clone_prepared_entry(&mutations[mutation_index]), options);
        mutation_index += 1;
    }
}

fn clone_prepared_entry(mutation: &PreparedMutation) -> EncodedLeafEntry {
    EncodedLeafEntry {
        key: mutation.entry.key.clone(),
        value: mutation.entry.value.clone(),
    }
}

fn append_rechunked_leaf_groups(
    groups: Vec<LeafChunkAccumulator>,
    existing_leaf_hashes: &HashSet<[u8; 32]>,
    chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    rebuilt: &mut Vec<ChildSummary>,
) {
    for group in groups {
        let node = encode_leaf_node(&group.entries);
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
        let (chunk, summary) = child_summary_from_node(node, first_key, last_key, subtree_count);
        if !existing_leaf_hashes.contains(&chunk.hash) {
            chunk_map.entry(chunk.hash).or_insert(chunk);
        }
        rebuilt.push(summary);
    }
}

fn chunk_leaf_entries(
    entries: Vec<EncodedLeafEntry>,
    options: &LiveTrackedStateOptions,
) -> Vec<LeafChunkAccumulator> {
    if entries.is_empty() {
        return vec![LeafChunkAccumulator::default()];
    }

    let mut groups = Vec::new();
    let mut current = LeafChunkAccumulator::default();
    for entry in entries {
        let projected = estimate_leaf_chunk_size(
            current.entries.len() + 1,
            current.key_bytes + entry.key.len(),
            current.value_bytes + entry.value.len(),
        );
        if !current.entries.is_empty() && projected > options.max_chunk_bytes {
            groups.push(std::mem::take(&mut current));
        }

        current.key_bytes += entry.key.len();
        current.value_bytes += entry.value.len();
        current.entries.push(entry);

        let previous_size = estimate_leaf_chunk_size(
            current.entries.len().saturating_sub(1),
            current.key_bytes.saturating_sub(
                current
                    .entries
                    .last()
                    .map(|entry| entry.key.len())
                    .unwrap_or(0),
            ),
            current.value_bytes.saturating_sub(
                current
                    .entries
                    .last()
                    .map(|entry| entry.value.len())
                    .unwrap_or(0),
            ),
        );
        let current_size = estimate_leaf_chunk_size(
            current.entries.len(),
            current.key_bytes,
            current.value_bytes,
        );
        let should_split = current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || current
                    .entries
                    .last()
                    .map(|entry| {
                        boundary_trigger(
                            &entry.key,
                            0,
                            current_size,
                            current_size.saturating_sub(previous_size),
                            options.target_chunk_bytes,
                        )
                    })
                    .unwrap_or(false));
        if should_split {
            groups.push(std::mem::take(&mut current));
        }
    }

    if !current.entries.is_empty() {
        groups.push(current);
    }
    groups
}

fn sort_and_dedup_mutations(mut mutations: Vec<LiveTrackedMutation>) -> Vec<LiveTrackedMutation> {
    if mutations.len() <= 1 {
        return mutations;
    }

    let mut out_of_order = false;
    let mut has_duplicate = false;
    for pair in mutations.windows(2) {
        match pair[0].key().cmp(pair[1].key()) {
            Ordering::Greater => out_of_order = true,
            Ordering::Equal => has_duplicate = true,
            Ordering::Less => {}
        }
    }

    if out_of_order {
        mutations.sort_by(|left, right| left.key().cmp(right.key()));
        has_duplicate = true;
    }

    if has_duplicate {
        let mut write = 1;
        for read in 1..mutations.len() {
            if mutations[write - 1].key() == mutations[read].key() {
                mutations[write - 1] = mutations[read].clone();
            } else {
                if write != read {
                    mutations.swap(write, read);
                }
                write += 1;
            }
        }
        mutations.truncate(write);
    }

    mutations
}

fn chunk_internal_entries(
    children: Vec<ChildSummary>,
    options: &LiveTrackedStateOptions,
    level: usize,
) -> Vec<InternalChunkAccumulator> {
    if children.is_empty() {
        return vec![InternalChunkAccumulator::default()];
    }

    let mut groups = Vec::new();
    let mut current = InternalChunkAccumulator::default();
    for child in children {
        let projected = estimate_internal_chunk_size(
            current.children.len() + 1,
            current.first_key_bytes + child.first_key.len(),
            current.last_key_bytes + child.last_key.len(),
        );
        if !current.children.is_empty() && projected > options.max_chunk_bytes {
            groups.push(std::mem::take(&mut current));
        }

        current.first_key_bytes += child.first_key.len();
        current.last_key_bytes += child.last_key.len();
        current.children.push(child);
        let previous_size = estimate_internal_chunk_size(
            current.children.len().saturating_sub(1),
            current.first_key_bytes.saturating_sub(
                current
                    .children
                    .last()
                    .map(|child| child.first_key.len())
                    .unwrap_or(0),
            ),
            current.last_key_bytes.saturating_sub(
                current
                    .children
                    .last()
                    .map(|child| child.last_key.len())
                    .unwrap_or(0),
            ),
        );
        let current_size = estimate_internal_chunk_size(
            current.children.len(),
            current.first_key_bytes,
            current.last_key_bytes,
        );
        let should_split = current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || current
                    .children
                    .last()
                    .map(|child| {
                        boundary_trigger(
                            &child.first_key,
                            level,
                            current_size,
                            current_size.saturating_sub(previous_size),
                            options.target_chunk_bytes,
                        )
                    })
                    .unwrap_or(false));
        if should_split {
            groups.push(std::mem::take(&mut current));
        }
    }

    if !current.children.is_empty() {
        groups.push(current);
    }
    groups
}

fn leaf_binary_search(leaf: &codec::DecodedLeafNode, key: &LiveTrackedEntityKey) -> Option<usize> {
    let mut low = 0;
    let mut high = leaf.entry_count();
    while low < high {
        let mid = (low + high) / 2;
        match compare_encoded_key_to_key(leaf.key_at(mid), key) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid,
            Ordering::Equal => return Some(mid),
        }
    }
    None
}

fn internal_child_index(
    internal: &codec::DecodedInternalNode,
    key: &LiveTrackedEntityKey,
) -> usize {
    if internal.child_count() <= 1 {
        return 0;
    }
    let mut low = 0;
    let mut high = internal.child_count();
    while low < high {
        let mid = (low + high) / 2;
        if compare_encoded_key_to_key(internal.first_key_at(mid), key) == Ordering::Greater {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    low.saturating_sub(1)
}

fn key_below_lower_bound(range: &LiveTrackedRangeRequest, key: &LiveTrackedEntityKey) -> bool {
    if !range.contiguous {
        return false;
    }
    for field in &range.fields {
        let Some(lower) = &field.lower else {
            continue;
        };
        let ordering = compare_key_component(key, field.field, &lower.value);
        match ordering {
            Ordering::Less => return true,
            Ordering::Greater => return false,
            Ordering::Equal if !lower.inclusive => return true,
            Ordering::Equal => continue,
        }
    }
    false
}

fn key_above_upper_bound(range: &LiveTrackedRangeRequest, key: &LiveTrackedEntityKey) -> bool {
    if !range.contiguous {
        return false;
    }
    for field in &range.fields {
        let Some(upper) = &field.upper else {
            continue;
        };
        let ordering = compare_key_component(key, field.field, &upper.value);
        match ordering {
            Ordering::Greater => return true,
            Ordering::Less => return false,
            Ordering::Equal if !upper.inclusive => return true,
            Ordering::Equal => continue,
        }
    }
    false
}

fn compare_key_component(
    key: &LiveTrackedEntityKey,
    field: LiveTrackedKeyField,
    bound: &LiveTrackedKeyComponent,
) -> Ordering {
    match (field, bound) {
        (LiveTrackedKeyField::SchemaKey, LiveTrackedKeyComponent::SchemaKey(value)) => {
            key.schema_key.cmp(value)
        }
        (LiveTrackedKeyField::FileId, LiveTrackedKeyComponent::FileId(value)) => {
            key.file_id.cmp(value)
        }
        (LiveTrackedKeyField::EntityId, LiveTrackedKeyComponent::EntityId(value)) => {
            key.entity_id.cmp(value)
        }
        _ => Ordering::Equal,
    }
}

fn hex(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(hash.len() * 2);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn verify_chunk_hash(expected: &[u8; 32], data: &[u8]) -> Result<(), LixError> {
    let actual = hash_bytes(data);
    if &actual != expected {
        return Err(LixError::unknown(format!(
            "live tracked chunk {} hash mismatch (actual {})",
            hex(expected),
            hex(&actual),
        )));
    }
    Ok(())
}

fn estimate_leaf_chunk_size(entry_count: usize, key_bytes: usize, value_bytes: usize) -> usize {
    32 + (entry_count * (std::mem::size_of::<u16>() * 2)) + key_bytes + value_bytes
}

fn estimate_internal_chunk_size(
    child_count: usize,
    first_key_bytes: usize,
    last_key_bytes: usize,
) -> usize {
    32 + (child_count
        * ((std::mem::size_of::<u16>() * 2)
            + types::LIVE_TRACKED_HASH_BYTES
            + std::mem::size_of::<u64>()))
        + first_key_bytes
        + last_key_bytes
}

#[cfg(test)]
mod tests {
    use super::storage::PROLLY_CHUNK_TABLE;
    use super::*;
    use crate::{
        CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId,
        LixTransaction, QueryResult, SqlDialect, Value,
    };
    use async_trait::async_trait;
    use rusqlite::{params, params_from_iter, Connection, Row};
    use std::collections::BTreeMap;
    use std::sync::{Mutex, MutexGuard};

    #[tokio::test]
    async fn stores_loads_reads_and_scans_a_root() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let state = LiveTrackedState::new(&backend);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-1")?;
        let result = state
            .apply_mutations_and_store_root(
                "commit-1",
                None,
                vec![
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-0001")?,
                        ),
                        sample_value("change-1", "plugin.bench", 1, "small", None)?,
                    ),
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-0002")?,
                        ),
                        sample_value(
                            "change-2",
                            "plugin.bench",
                            2,
                            "large",
                            Some(vec![b'x'; 4096]),
                        )?,
                    ),
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-0003")?,
                        ),
                        sample_value("change-3", "plugin.bench", 3, "tail", None)?,
                    ),
                ],
            )
            .await?;
        assert!(result.persisted_root);

        let loaded_root = state.load_root("commit-1").await?;
        assert_eq!(loaded_root, Some(result.root_id.clone()));

        let value = state
            .get(
                &result.root_id,
                &LiveTrackedEntityKey::new(
                    schema.clone(),
                    file.clone(),
                    EntityId::try_from("entity-0002")?,
                ),
            )
            .await?
            .expect("row should exist");
        assert!(value
            .columns
            .iter()
            .any(|column| matches!(column.value, LiveTrackedFieldValue::LargeBlob(_))));

        let profile = state
            .profile_leaf_codec(&[LiveTrackedRow::new(
                LiveTrackedEntityKey::new(
                    schema.clone(),
                    file.clone(),
                    EntityId::try_from("codec-1")?,
                ),
                sample_value(
                    "codec-change",
                    "plugin.bench",
                    9,
                    "codec",
                    Some(vec![b'z'; 4096]),
                )?,
            )])
            .await?;
        assert!(profile.encoded_leaf_bytes <= state.options().max_chunk_bytes);
        assert_eq!(profile.large_value_count, 1);

        let scan = state
            .scan(
                &result.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema), Some(file), None),
            )
            .await?;
        assert_eq!(scan.len(), 3);
        assert_eq!(scan.rows()[0].key.entity_id.as_str(), "entity-0001");
        assert_eq!(scan.rows()[2].key.entity_id.as_str(), "entity-0003");
        Ok(())
    }

    #[tokio::test]
    async fn applies_updates_and_tombstones_on_top_of_base_root() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let state = LiveTrackedState::new(&backend);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-2")?;
        let base_result = state
            .apply_mutations_and_store_root(
                "commit-base",
                None,
                vec![
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-a")?,
                        ),
                        sample_value("base-1", "plugin.bench", 1, "alpha", None)?,
                    ),
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-b")?,
                        ),
                        sample_value("base-2", "plugin.bench", 2, "beta", None)?,
                    ),
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-c")?,
                        ),
                        sample_value("base-3", "plugin.bench", 3, "gamma", None)?,
                    ),
                ],
            )
            .await?;

        let updated = sample_value("update-2", "plugin.bench", 22, "beta-2", None)?;
        let deleted = LiveTrackedEntityValue::tombstone(
            "delete-3",
            CanonicalSchemaVersion::try_from("1")?,
            CanonicalPluginKey::try_from("plugin.bench")?,
            Some("{\"kind\":\"delete\"}".to_string()),
        )?;
        let next_result = state
            .apply_mutations_and_store_root(
                "commit-next",
                Some(&base_result.root_id),
                vec![
                    LiveTrackedMutation::put(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-b")?,
                        ),
                        updated.clone(),
                    ),
                    LiveTrackedMutation::delete(
                        LiveTrackedEntityKey::new(
                            schema.clone(),
                            file.clone(),
                            EntityId::try_from("entity-c")?,
                        ),
                        deleted.clone(),
                    )?,
                ],
            )
            .await?;

        let updated_row = state
            .get(
                &next_result.root_id,
                &LiveTrackedEntityKey::new(
                    schema.clone(),
                    file.clone(),
                    EntityId::try_from("entity-b")?,
                ),
            )
            .await?
            .expect("updated row should exist");
        assert_eq!(updated_row, updated);

        let deleted_row = state
            .get(
                &next_result.root_id,
                &LiveTrackedEntityKey::new(
                    schema.clone(),
                    file.clone(),
                    EntityId::try_from("entity-c")?,
                ),
            )
            .await?
            .expect("deleted row should still be present as tombstone");
        assert!(deleted_row.tombstone);

        let range = LiveTrackedRangeRequest {
            fields: vec![
                LiveTrackedRangeField::exact(LiveTrackedKeyComponent::SchemaKey(schema)),
                LiveTrackedRangeField::exact(LiveTrackedKeyComponent::FileId(file)),
                LiveTrackedRangeField::interval(
                    LiveTrackedKeyField::EntityId,
                    Some(LiveTrackedRangeBound::inclusive(
                        LiveTrackedKeyComponent::EntityId(EntityId::try_from("entity-b")?),
                    )),
                    Some(LiveTrackedRangeBound::inclusive(
                        LiveTrackedKeyComponent::EntityId(EntityId::try_from("entity-c")?),
                    )),
                )?,
            ],
            contiguous: true,
        };
        let scan = state.scan(&next_result.root_id, &range).await?;
        assert_eq!(scan.len(), 2);
        assert_eq!(scan.rows()[0].key.entity_id.as_str(), "entity-b");
        assert_eq!(scan.rows()[1].key.entity_id.as_str(), "entity-c");
        assert!(scan.rows()[1].value.tombstone);
        Ok(())
    }

    #[tokio::test]
    async fn sparse_incremental_rewrite_matches_full_rebuild_root() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            large_value_threshold_bytes: 16 * 1024,
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-sparse")?;
        let base_mutations = (0..256)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    sample_value(
                        &format!("base-{index}"),
                        "plugin.bench",
                        index as i64,
                        &format!("base-{index}"),
                        None,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let base_result = state
            .apply_mutations_and_store_root("commit-sparse-base", None, base_mutations)
            .await?;

        let updated_indexes = [3usize, 4, 63, 64, 65, 129, 190, 191, 192];
        let sparse_mutations = updated_indexes
            .into_iter()
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    sample_value(
                        &format!("update-{index}"),
                        "plugin.bench",
                        (1_000 + index) as i64,
                        &format!("updated-{index}"),
                        Some(vec![u8::try_from(index % 251).unwrap_or(0); 600]),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let incremental_result = state
            .apply_mutations_and_store_root(
                "commit-sparse-incremental",
                Some(&base_result.root_id),
                sparse_mutations,
            )
            .await?;

        let full_mutations = (0..256)
            .map(|index| {
                let value = if updated_indexes.contains(&index) {
                    sample_value(
                        &format!("update-{index}"),
                        "plugin.bench",
                        (1_000 + index) as i64,
                        &format!("updated-{index}"),
                        Some(vec![u8::try_from(index % 251).unwrap_or(0); 600]),
                    )?
                } else {
                    sample_value(
                        &format!("base-{index}"),
                        "plugin.bench",
                        index as i64,
                        &format!("base-{index}"),
                        None,
                    )?
                };
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    value,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let full_result = state
            .apply_mutations_and_store_root("commit-sparse-full", None, full_mutations)
            .await?;

        let incremental_scan = state
            .scan(
                &incremental_result.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema.clone()), Some(file.clone()), None),
            )
            .await?;
        let full_scan = state
            .scan(
                &full_result.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema), Some(file), None),
            )
            .await?;
        if incremental_scan.rows() != full_scan.rows() {
            let first_diff = incremental_scan
                .rows()
                .iter()
                .zip(full_scan.rows().iter())
                .position(|(left, right)| left != right);
            panic!(
                "scan mismatch incremental_len={} full_len={} first_diff={first_diff:?} incremental_at_diff={:?} full_at_diff={:?}",
                incremental_scan.rows().len(),
                full_scan.rows().len(),
                first_diff.and_then(|index| incremental_scan.rows().get(index)),
                first_diff.and_then(|index| full_scan.rows().get(index)),
            );
        }
        assert_eq!(incremental_result.root_id, full_result.root_id);
        Ok(())
    }

    #[tokio::test]
    async fn dolt_chunker_sparse_rewrite_matches_full_rebuild_root() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            large_value_threshold_bytes: 16 * 1024,
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-sparse")?;
        let base_mutations = (0..256)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    sample_value(
                        &format!("base-{index}"),
                        "plugin.bench",
                        index as i64,
                        &format!("base-{index}"),
                        None,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let base_result = state
            .apply_mutations_and_store_root("commit-dolt-base", None, base_mutations)
            .await?;

        let updated_indexes = [3usize, 4, 63, 64, 65, 129, 190, 191, 192];
        let sparse_mutations = updated_indexes
            .into_iter()
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    sample_value(
                        &format!("update-{index}"),
                        "plugin.bench",
                        (1_000 + index) as i64,
                        &format!("updated-{index}"),
                        Some(vec![u8::try_from(index % 251).unwrap_or(0); 600]),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let prepared = state.prepare_mutations(sort_and_dedup_mutations(sparse_mutations))?;
        let dolt_built = state
            .build_tree_from_base_with_dolt_chunker(&base_result.root_id, prepared)
            .await?;

        let mut transaction = state.backend.begin_transaction().await?;
        state
            .storage
            .write_values(transaction.as_mut(), &dolt_built.values)
            .await?;
        state
            .storage
            .write_chunks(transaction.as_mut(), &dolt_built.chunks)
            .await?;
        state
            .storage
            .store_root(
                transaction.as_mut(),
                "commit-dolt-incremental",
                &dolt_built.root_id,
            )
            .await?;
        transaction.commit().await?;
        state.populate_cache(&dolt_built.chunks)?;

        let full_mutations = (0..256)
            .map(|index| {
                let value = if updated_indexes.contains(&index) {
                    sample_value(
                        &format!("update-{index}"),
                        "plugin.bench",
                        (1_000 + index) as i64,
                        &format!("updated-{index}"),
                        Some(vec![u8::try_from(index % 251).unwrap_or(0); 600]),
                    )?
                } else {
                    sample_value(
                        &format!("base-{index}"),
                        "plugin.bench",
                        index as i64,
                        &format!("base-{index}"),
                        None,
                    )?
                };
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        schema.clone(),
                        file.clone(),
                        EntityId::try_from(format!("entity-{index:04}").as_str())?,
                    ),
                    value,
                ))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let full_result = state
            .apply_mutations_and_store_root("commit-dolt-full", None, full_mutations)
            .await?;

        let dolt_scan = state
            .scan(
                &dolt_built.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema.clone()), Some(file.clone()), None),
            )
            .await?;
        let full_scan = state
            .scan(
                &full_result.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema), Some(file), None),
            )
            .await?;
        if dolt_scan.rows() != full_scan.rows() {
            let first_diff = dolt_scan
                .rows()
                .iter()
                .zip(full_scan.rows().iter())
                .position(|(left, right)| left != right);
            panic!(
                "dolt scan mismatch dolt_len={} full_len={} first_diff={first_diff:?} dolt_at_diff={:?} full_at_diff={:?}",
                dolt_scan.rows().len(),
                full_scan.rows().len(),
                first_diff.and_then(|index| dolt_scan.rows().get(index)),
                first_diff.and_then(|index| full_scan.rows().get(index)),
            );
        }
        assert_eq!(dolt_built.root_id, full_result.root_id);
        Ok(())
    }

    #[tokio::test]
    async fn round_trips_large_tree_in_order() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            large_value_threshold_bytes: 8_192,
            target_chunk_bytes: 512,
            min_chunk_bytes: 256,
            max_chunk_bytes: 1_024,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-roundtrip")?;
        let mut model = BTreeMap::new();
        for index in 0..4_096usize {
            let blob_len = if index % 13 == 0 { Some(900) } else { None };
            model.insert(index, numbered_value("roundtrip", index, blob_len)?);
        }

        let result = state
            .apply_mutations_and_store_root(
                "commit-roundtrip",
                None,
                mutations_from_model(&schema, &file, &model)?,
            )
            .await?;
        assert_eq!(result.row_count, model.len());
        assert!(result.tree_height > 1);

        let scan = scan_prefix_rows(&state, &result.root_id, &schema, &file).await?;
        let expected = rows_from_model(&schema, &file, &model)?;
        assert_rows_match("round trip", &scan, &expected);

        for index in [0usize, 2_048, 4_095] {
            let actual = state
                .get(&result.root_id, &numbered_key(&schema, &file, index)?)
                .await?
                .expect("row should exist");
            assert_eq!(
                actual,
                model.get(&index).cloned().expect("fixture row exists")
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn inserts_before_first_and_after_last_match_full_rebuild() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let state = LiveTrackedState::new(&backend);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-edge")?;
        let mut base_model = BTreeMap::new();
        for index in 100..356usize {
            base_model.insert(index, numbered_value("base", index, None)?);
        }
        let base_result = state
            .apply_mutations_and_store_root(
                "commit-edge-base",
                None,
                mutations_from_model(&schema, &file, &base_model)?,
            )
            .await?;

        let before_value = numbered_value("edge-before", 0, Some(700))?;
        let after_value = numbered_value("edge-after", 99_999, None)?;
        let incremental_result = state
            .apply_mutations_and_store_root(
                "commit-edge-incremental",
                Some(&base_result.root_id),
                vec![
                    LiveTrackedMutation::put(
                        numbered_key(&schema, &file, 0)?,
                        before_value.clone(),
                    ),
                    LiveTrackedMutation::put(
                        numbered_key(&schema, &file, 99_999)?,
                        after_value.clone(),
                    ),
                ],
            )
            .await?;

        let mut expected_model = base_model.clone();
        expected_model.insert(0, before_value);
        expected_model.insert(99_999, after_value);
        let full_result = state
            .apply_mutations_and_store_root(
                "commit-edge-full",
                None,
                mutations_from_model(&schema, &file, &expected_model)?,
            )
            .await?;

        assert_eq!(incremental_result.root_id, full_result.root_id);
        let scan = scan_prefix_rows(&state, &incremental_result.root_id, &schema, &file).await?;
        let expected = rows_from_model(&schema, &file, &expected_model)?;
        assert_rows_match("edge inserts", &scan, &expected);
        assert_eq!(
            scan.first().map(|row| row.key.clone()),
            Some(numbered_key(&schema, &file, 0)?)
        );
        assert_eq!(
            scan.last().map(|row| row.key.clone()),
            Some(numbered_key(&schema, &file, 99_999)?)
        );

        Ok(())
    }

    #[tokio::test]
    async fn mixed_mutations_match_full_rebuild_and_model_across_scales() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            large_value_threshold_bytes: 8_192,
            target_chunk_bytes: 32 * 1_024,
            min_chunk_bytes: 16 * 1_024,
            max_chunk_bytes: 64 * 1_024,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        for scale in [20usize, 200] {
            let file = FileId::try_from(format!("file-mixed-{scale}").as_str())?;
            let mut base_model = BTreeMap::new();
            for index in (0..(scale * 2)).step_by(2) {
                base_model.insert(index, numbered_value("base", index, None)?);
            }

            let base_commit = format!("commit-mixed-base-{scale}");
            let base_result = state
                .apply_mutations_and_store_root(
                    &base_commit,
                    None,
                    mutations_from_model(&schema, &file, &base_model)?,
                )
                .await?;

            let mut expected_model = base_model.clone();
            let mut mutations = Vec::with_capacity(scale * 2);
            for i in 0..scale {
                let even_index = i * 2;
                let odd_index = even_index + 1;

                let insert_value = numbered_value("insert", odd_index, None)?;
                mutations.push(LiveTrackedMutation::put(
                    numbered_key(&schema, &file, odd_index)?,
                    insert_value.clone(),
                ));
                expected_model.insert(odd_index, insert_value);

                if i % 2 == 0 {
                    let update_value = numbered_value("update", even_index, None)?;
                    mutations.push(LiveTrackedMutation::put(
                        numbered_key(&schema, &file, even_index)?,
                        update_value.clone(),
                    ));
                    expected_model.insert(even_index, update_value);
                } else {
                    let tombstone = numbered_tombstone("delete", even_index)?;
                    mutations.push(LiveTrackedMutation::delete(
                        numbered_key(&schema, &file, even_index)?,
                        tombstone.clone(),
                    )?);
                    expected_model.insert(even_index, tombstone);
                }
            }

            let incremental_commit = format!("commit-mixed-incremental-{scale}");
            let incremental_result = state
                .apply_mutations_and_store_root(
                    &incremental_commit,
                    Some(&base_result.root_id),
                    mutations,
                )
                .await?;
            let full_commit = format!("commit-mixed-full-{scale}");
            let full_result = state
                .apply_mutations_and_store_root(
                    &full_commit,
                    None,
                    mutations_from_model(&schema, &file, &expected_model)?,
                )
                .await?;

            let expected = rows_from_model(&schema, &file, &expected_model)?;
            let incremental_scan =
                scan_prefix_rows(&state, &incremental_result.root_id, &schema, &file).await?;
            let full_scan = scan_prefix_rows(&state, &full_result.root_id, &schema, &file).await?;
            assert_rows_match(
                &format!("mixed mutations incremental scale {scale}"),
                &incremental_scan,
                &expected,
            );
            assert_rows_match(
                &format!("mixed mutations full rebuild scale {scale}"),
                &full_scan,
                &expected,
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn no_op_rewrite_preserves_root_hash() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            target_chunk_bytes: 512,
            min_chunk_bytes: 256,
            max_chunk_bytes: 1_024,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-stable")?;
        let mut model = BTreeMap::new();
        for index in 0..512usize {
            model.insert(
                index,
                numbered_value(
                    "stable",
                    index,
                    if index % 17 == 0 { Some(1_536) } else { None },
                )?,
            );
        }

        let base_result = state
            .apply_mutations_and_store_root(
                "commit-stable-base",
                None,
                mutations_from_model(&schema, &file, &model)?,
            )
            .await?;
        let noop_result = state
            .apply_mutations_and_store_root(
                "commit-stable-noop",
                Some(&base_result.root_id),
                mutations_from_model(&schema, &file, &model)?,
            )
            .await?;

        assert_eq!(noop_result.root_id, base_result.root_id);
        let scan = scan_prefix_rows(&state, &noop_result.root_id, &schema, &file).await?;
        let expected = rows_from_model(&schema, &file, &model)?;
        assert_rows_match("no-op rewrite", &scan, &expected);

        Ok(())
    }

    #[tokio::test]
    async fn corrupt_chunk_bytes_are_detected_on_point_read() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let state = LiveTrackedState::new(&backend);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-corrupt-point")?;
        let key = numbered_key(&schema, &file, 7)?;
        let result = state
            .apply_mutations_and_store_root(
                "commit-corrupt-point",
                None,
                vec![LiveTrackedMutation::put(
                    key.clone(),
                    numbered_value("corrupt-point", 7, None)?,
                )],
            )
            .await?;

        backend.corrupt_chunk(result.root_id.as_bytes())?;
        state.clear_cache();

        let error = state
            .get(&result.root_id, &key)
            .await
            .expect_err("point read should fail on corrupted chunk");
        assert_hash_mismatch(&error);
        Ok(())
    }

    #[tokio::test]
    async fn corrupt_chunk_bytes_are_detected_on_scan_batch_read() -> Result<(), LixError> {
        let backend = TestSqliteBackend::in_memory()?;
        let options = LiveTrackedStateOptions {
            target_chunk_bytes: 256,
            min_chunk_bytes: 128,
            max_chunk_bytes: 512,
            ..LiveTrackedStateOptions::default()
        };
        let state = LiveTrackedState::with_options(&backend, options);
        state.ensure_schema().await?;

        let schema = CanonicalSchemaKey::try_from("bench.schema")?;
        let file = FileId::try_from("file-corrupt-scan")?;
        let mut model = BTreeMap::new();
        for index in 0..512usize {
            model.insert(index, numbered_value("corrupt-scan", index, None)?);
        }
        let result = state
            .apply_mutations_and_store_root(
                "commit-corrupt-scan",
                None,
                mutations_from_model(&schema, &file, &model)?,
            )
            .await?;
        let topology = state
            .collect_root_leaf_topology(*result.root_id.as_bytes())
            .await?;
        assert!(topology.leaf_summaries.len() > 1);

        backend.corrupt_chunk(&topology.leaf_summaries[1].child_hash)?;
        state.clear_cache();

        let error = state
            .scan(
                &result.root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema), Some(file), None),
            )
            .await
            .expect_err("scan should fail on corrupted leaf chunk");
        assert_hash_mismatch(&error);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "diagnostic profiling helper"]
    async fn profile_write_breakdown_empty_and_rewrite() -> Result<(), LixError> {
        const COUNT: usize = 10_000;

        let backend = TestSqliteBackend::in_memory()?;
        let state = LiveTrackedState::new(&backend);
        state.ensure_schema().await?;

        let empty_mutations = bench_like_mutations(COUNT, 0)?;
        let empty_breakdown =
            measure_apply_breakdown(&state, "commit-empty", None, empty_mutations).await?;
        println!(
            "empty total={:.3}ms sort={:.3}ms build={:.3}ms tx={:.3}ms values={:.3}ms chunks={:.3}ms root={:.3}ms chunks={} values={}",
            empty_breakdown.total_ms,
            empty_breakdown.sort_mutations_ms,
            empty_breakdown.build_tree_ms,
            empty_breakdown.begin_transaction_ms,
            empty_breakdown.write_values_ms,
            empty_breakdown.write_chunks_ms,
            empty_breakdown.store_root_ms,
            empty_breakdown.chunk_count,
            empty_breakdown.value_ref_count,
        );

        let base_result = state
            .apply_mutations_and_store_root("commit-base", None, bench_like_mutations(COUNT, 0)?)
            .await?;
        let rewrite_breakdown = measure_apply_breakdown(
            &state,
            "commit-rewrite",
            Some(&base_result.root_id),
            bench_like_mutations(COUNT, 1)?,
        )
        .await?;
        println!(
            "rewrite total={:.3}ms load_base={:.3}ms sort={:.3}ms merge={:.3}ms build={:.3}ms tx={:.3}ms values={:.3}ms chunks={:.3}ms root={:.3}ms chunks={} values={}",
            rewrite_breakdown.total_ms,
            rewrite_breakdown.load_base_ms,
            rewrite_breakdown.sort_mutations_ms,
            rewrite_breakdown.merge_ms,
            rewrite_breakdown.build_tree_ms,
            rewrite_breakdown.begin_transaction_ms,
            rewrite_breakdown.write_values_ms,
            rewrite_breakdown.write_chunks_ms,
            rewrite_breakdown.store_root_ms,
            rewrite_breakdown.chunk_count,
            rewrite_breakdown.value_ref_count,
        );

        Ok(())
    }

    fn sample_value(
        change_id: &str,
        plugin_key: &str,
        ordinal: i64,
        label: &str,
        blob: Option<Vec<u8>>,
    ) -> Result<LiveTrackedEntityValue, LixError> {
        let mut columns = vec![
            LiveTrackedPayloadColumn::new("label", LiveTrackedFieldValue::Text(label.to_string()))?,
            LiveTrackedPayloadColumn::new("ordinal", LiveTrackedFieldValue::Integer(ordinal))?,
        ];
        if let Some(blob) = blob {
            columns.push(LiveTrackedPayloadColumn::new(
                "payload",
                LiveTrackedFieldValue::Blob(blob),
            )?);
        }
        LiveTrackedEntityValue::new(
            change_id,
            CanonicalSchemaVersion::try_from("1")?,
            CanonicalPluginKey::try_from(plugin_key)?,
            Some(format!("{{\"ordinal\":{ordinal}}}")),
            columns,
        )
    }

    fn numbered_key(
        schema: &CanonicalSchemaKey,
        file: &FileId,
        index: usize,
    ) -> Result<LiveTrackedEntityKey, LixError> {
        Ok(LiveTrackedEntityKey::new(
            schema.clone(),
            file.clone(),
            EntityId::try_from(format!("entity-{index:05}").as_str())?,
        ))
    }

    fn numbered_value(
        revision: &str,
        index: usize,
        blob_len: Option<usize>,
    ) -> Result<LiveTrackedEntityValue, LixError> {
        sample_value(
            &format!("{revision}-change-{index:05}"),
            "plugin.bench",
            index as i64,
            &format!("{revision}-label-{index:05}"),
            blob_len.map(|len| vec![u8::try_from(index % 251).unwrap_or(0); len]),
        )
    }

    fn numbered_tombstone(
        revision: &str,
        index: usize,
    ) -> Result<LiveTrackedEntityValue, LixError> {
        LiveTrackedEntityValue::tombstone(
            format!("{revision}-change-{index:05}"),
            CanonicalSchemaVersion::try_from("1")?,
            CanonicalPluginKey::try_from("plugin.bench")?,
            Some(format!("{{\"deleted\":{index}}}")),
        )
    }

    fn rows_from_model(
        schema: &CanonicalSchemaKey,
        file: &FileId,
        model: &BTreeMap<usize, LiveTrackedEntityValue>,
    ) -> Result<Vec<LiveTrackedRow>, LixError> {
        model
            .iter()
            .map(|(index, value)| {
                Ok(LiveTrackedRow::new(
                    numbered_key(schema, file, *index)?,
                    value.clone(),
                ))
            })
            .collect()
    }

    fn mutations_from_model(
        schema: &CanonicalSchemaKey,
        file: &FileId,
        model: &BTreeMap<usize, LiveTrackedEntityValue>,
    ) -> Result<Vec<LiveTrackedMutation>, LixError> {
        model
            .iter()
            .map(|(index, value)| {
                let key = numbered_key(schema, file, *index)?;
                if value.tombstone {
                    LiveTrackedMutation::delete(key, value.clone())
                } else {
                    Ok(LiveTrackedMutation::put(key, value.clone()))
                }
            })
            .collect()
    }

    async fn scan_prefix_rows(
        state: &LiveTrackedState<'_>,
        root_id: &LiveTrackedRootId,
        schema: &CanonicalSchemaKey,
        file: &FileId,
    ) -> Result<Vec<LiveTrackedRow>, LixError> {
        Ok(state
            .scan(
                root_id,
                &LiveTrackedRangeRequest::prefix(Some(schema.clone()), Some(file.clone()), None),
            )
            .await?
            .rows()
            .to_vec())
    }

    #[track_caller]
    fn assert_rows_match(context: &str, actual: &[LiveTrackedRow], expected: &[LiveTrackedRow]) {
        if actual == expected {
            return;
        }

        let first_diff = actual
            .iter()
            .zip(expected.iter())
            .position(|(left, right)| left != right)
            .or_else(|| {
                if actual.len() != expected.len() {
                    Some(actual.len().min(expected.len()))
                } else {
                    None
                }
            });
        panic!(
            "{context} rows mismatch actual_len={} expected_len={} first_diff={first_diff:?} actual_at_diff={:?} expected_at_diff={:?}",
            actual.len(),
            expected.len(),
            first_diff.and_then(|index| actual.get(index)),
            first_diff.and_then(|index| expected.get(index)),
        );
    }

    #[track_caller]
    fn assert_hash_mismatch(error: &LixError) {
        assert!(
            error.description.contains("hash mismatch"),
            "expected hash mismatch, got {}",
            error.description
        );
    }

    struct TestSqliteBackend {
        connection: Mutex<Connection>,
    }

    struct TestSqliteTransaction<'a> {
        connection: MutexGuard<'a, Connection>,
        finalized: bool,
    }

    impl TestSqliteBackend {
        fn in_memory() -> Result<Self, LixError> {
            Ok(Self {
                connection: Mutex::new(Connection::open_in_memory().map_err(sqlite_error)?),
            })
        }

        fn corrupt_chunk(&self, hash: &[u8; 32]) -> Result<(), LixError> {
            let connection = self
                .connection
                .lock()
                .map_err(|_| LixError::unknown("test sqlite mutex poisoned"))?;
            let mut data: Vec<u8> = connection
                .query_row(
                    &format!("SELECT data FROM {PROLLY_CHUNK_TABLE} WHERE chunk_hash = ?1 LIMIT 1"),
                    params![&hash[..]],
                    |row| row.get(0),
                )
                .map_err(sqlite_error)?;
            let Some(last) = data.last_mut() else {
                return Err(LixError::unknown(
                    "cannot corrupt an empty live tracked chunk",
                ));
            };
            *last ^= 0x5a;
            connection
                .execute(
                    &format!("UPDATE {PROLLY_CHUNK_TABLE} SET data = ?1 WHERE chunk_hash = ?2"),
                    params![data, &hash[..]],
                )
                .map_err(sqlite_error)?;
            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl crate::LixBackend for TestSqliteBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            let connection = self
                .connection
                .lock()
                .map_err(|_| LixError::unknown("test sqlite mutex poisoned"))?;
            execute_sql(&connection, sql, params)
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            let connection = self
                .connection
                .lock()
                .map_err(|_| LixError::unknown("test sqlite mutex poisoned"))?;
            connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(sqlite_error)?;
            Ok(Box::new(TestSqliteTransaction {
                connection,
                finalized: false,
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            self.begin_transaction().await
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for TestSqliteTransaction<'_> {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            execute_sql(&self.connection, sql, params)
        }

        async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
            self.connection
                .execute_batch("COMMIT")
                .map_err(sqlite_error)?;
            self.finalized = true;
            Ok(())
        }

        async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
            self.connection
                .execute_batch("ROLLBACK")
                .map_err(sqlite_error)?;
            self.finalized = true;
            Ok(())
        }
    }

    impl Drop for TestSqliteTransaction<'_> {
        fn drop(&mut self) {
            if self.finalized || std::thread::panicking() {
                return;
            }
            let _ = self.connection.execute_batch("ROLLBACK");
        }
    }

    fn execute_sql(
        connection: &Connection,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, LixError> {
        if params.is_empty() && sql.contains(';') {
            connection.execute_batch(sql).map_err(sqlite_error)?;
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
        let columns = statement
            .column_names()
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>();
        let bound_params = params.iter().cloned().map(to_sql_value);
        let mut rows = statement
            .query(params_from_iter(bound_params))
            .map_err(sqlite_error)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            out.push(read_row(row)?);
        }
        Ok(QueryResult { rows: out, columns })
    }

    fn read_row(row: &Row<'_>) -> Result<Vec<Value>, LixError> {
        let mut out = Vec::with_capacity(row.as_ref().column_count());
        for index in 0..row.as_ref().column_count() {
            let value = row.get_ref(index).map_err(sqlite_error)?;
            out.push(match value {
                rusqlite::types::ValueRef::Null => Value::Null,
                rusqlite::types::ValueRef::Integer(value) => Value::Integer(value),
                rusqlite::types::ValueRef::Real(value) => Value::Real(value),
                rusqlite::types::ValueRef::Text(value) => {
                    Value::Text(String::from_utf8_lossy(value).to_string())
                }
                rusqlite::types::ValueRef::Blob(value) => Value::Blob(value.to_vec()),
            });
        }
        Ok(out)
    }

    fn to_sql_value(value: Value) -> rusqlite::types::Value {
        match value {
            Value::Null => rusqlite::types::Value::Null,
            Value::Boolean(value) => rusqlite::types::Value::Integer(if value { 1 } else { 0 }),
            Value::Integer(value) => rusqlite::types::Value::Integer(value),
            Value::Real(value) => rusqlite::types::Value::Real(value),
            Value::Text(value) => rusqlite::types::Value::Text(value),
            Value::Json(value) => rusqlite::types::Value::Text(value.to_string()),
            Value::Blob(value) => rusqlite::types::Value::Blob(value),
        }
    }

    fn sqlite_error(error: impl std::fmt::Display) -> LixError {
        LixError::unknown(error.to_string())
    }

    struct ApplyBreakdown {
        total_ms: f64,
        load_base_ms: f64,
        sort_mutations_ms: f64,
        merge_ms: f64,
        build_tree_ms: f64,
        begin_transaction_ms: f64,
        write_values_ms: f64,
        write_chunks_ms: f64,
        store_root_ms: f64,
        chunk_count: usize,
        value_ref_count: usize,
    }

    async fn measure_apply_breakdown(
        state: &LiveTrackedState<'_>,
        commit_id: &str,
        base_root: Option<&LiveTrackedRootId>,
        mutations: Vec<LiveTrackedMutation>,
    ) -> Result<ApplyBreakdown, LixError> {
        use std::time::Instant;

        let total_start = Instant::now();
        let load_base_ms = 0.0;
        let merge_ms = 0.0;

        let sort_start = Instant::now();
        let mutations = sort_and_dedup_mutations(mutations);
        let sort_mutations_ms = sort_start.elapsed().as_secs_f64() * 1000.0;

        let build_start = Instant::now();
        let prepared = state.prepare_mutations(mutations)?;
        let built = if let Some(base_root) = base_root {
            state
                .build_tree_from_base_and_prepared_mutations(base_root, prepared)
                .await?
        } else {
            let PreparedMutations {
                mutations,
                values,
                value_ref_bytes,
            } = prepared;
            state.build_tree_from_encoded_entries(
                mutations
                    .into_iter()
                    .map(|mutation| mutation.entry)
                    .collect(),
                values,
                value_ref_bytes,
            )?
        };
        let build_tree_ms = build_start.elapsed().as_secs_f64() * 1000.0;

        let tx_start = Instant::now();
        let mut transaction = state.backend.begin_transaction().await?;
        let begin_transaction_ms = tx_start.elapsed().as_secs_f64() * 1000.0;

        let values_start = Instant::now();
        state
            .storage
            .write_values(transaction.as_mut(), &built.values)
            .await?;
        let write_values_ms = values_start.elapsed().as_secs_f64() * 1000.0;

        let chunks_start = Instant::now();
        state
            .storage
            .write_chunks(transaction.as_mut(), &built.chunks)
            .await?;
        let write_chunks_ms = chunks_start.elapsed().as_secs_f64() * 1000.0;

        let root_start = Instant::now();
        state
            .storage
            .store_root(transaction.as_mut(), commit_id, &built.root_id)
            .await?;
        transaction.commit().await?;
        let store_root_ms = root_start.elapsed().as_secs_f64() * 1000.0;

        Ok(ApplyBreakdown {
            total_ms: total_start.elapsed().as_secs_f64() * 1000.0,
            load_base_ms,
            sort_mutations_ms,
            merge_ms,
            build_tree_ms,
            begin_transaction_ms,
            write_values_ms,
            write_chunks_ms,
            store_root_ms,
            chunk_count: built.chunks.len(),
            value_ref_count: built.values.len(),
        })
    }

    fn bench_like_mutations(
        count: usize,
        revision: usize,
    ) -> Result<Vec<LiveTrackedMutation>, LixError> {
        (0..count)
            .map(|index| {
                Ok(LiveTrackedMutation::put(
                    LiveTrackedEntityKey::new(
                        CanonicalSchemaKey::try_from("bench.schema")?,
                        FileId::try_from("bench-file")?,
                        EntityId::try_from(format!("entity-{index:05}").as_str())?,
                    ),
                    LiveTrackedEntityValue::new(
                        format!("change-{revision}-{index}"),
                        CanonicalSchemaVersion::try_from("1")?,
                        CanonicalPluginKey::try_from("plugin.bench")?,
                        Some(format!("{{\"r\":{revision}}}")),
                        vec![
                            LiveTrackedPayloadColumn::new(
                                "label",
                                LiveTrackedFieldValue::Text(format!(
                                    "entity-{index:05}-rev-{revision}"
                                )),
                            )?,
                            LiveTrackedPayloadColumn::new(
                                "ordinal",
                                LiveTrackedFieldValue::Integer(((revision * count) + index) as i64),
                            )?,
                        ],
                    )?,
                ))
            })
            .collect()
    }
}
