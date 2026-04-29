use std::collections::BTreeMap;

use crate::backend::{KvStore, KvWriter};
use crate::engine2::tracked_state::codec::{
    boundary_trigger, child_summary_from_node, decode_key, decode_node, decode_value,
    encode_internal_node, encode_key, encode_leaf_node, encode_value, ChildSummary, DecodedNode,
    EncodedLeafEntry, PendingChunkWrite,
};
use crate::engine2::tracked_state::storage;
use crate::engine2::tracked_state::tree_types::{
    TrackedStateApplyResult, TrackedStateKey, TrackedStateMutation, TrackedStateRootId,
    TrackedStateTreeScanRequest, TrackedStateValue, TRACKED_STATE_HASH_BYTES,
};
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateTreeOptions {
    pub(crate) target_chunk_bytes: usize,
    pub(crate) min_chunk_bytes: usize,
    pub(crate) max_chunk_bytes: usize,
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
/// This type owns tracked-state tree mechanics only. Version refs, untracked overlay,
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

    #[allow(dead_code)]
    pub(crate) fn with_options(options: TrackedStateTreeOptions) -> Self {
        Self { options }
    }

    pub(crate) async fn load_root(
        &self,
        store: &mut (impl KvStore + ?Sized),
        commit_id: &str,
    ) -> Result<Option<TrackedStateRootId>, LixError> {
        storage::load_root(store, commit_id).await
    }

    pub(crate) async fn get(
        &self,
        store: &mut impl KvStore,
        root_id: &TrackedStateRootId,
        key: &TrackedStateKey,
    ) -> Result<Option<TrackedStateValue>, LixError> {
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

    pub(crate) async fn scan(
        &self,
        store: &mut impl KvStore,
        root_id: &TrackedStateRootId,
        request: &TrackedStateTreeScanRequest,
    ) -> Result<Vec<(TrackedStateKey, TrackedStateValue)>, LixError> {
        let entries = self.collect_leaf_entries(store, root_id).await?;
        let mut rows = Vec::new();
        for entry in entries {
            let key = decode_key(&entry.key)?;
            let value = decode_value(&entry.value)?;
            if request.matches(&key, &value) {
                rows.push((key, value));
                if request.limit.is_some_and(|limit| rows.len() >= limit) {
                    break;
                }
            }
        }
        Ok(rows)
    }

    pub(crate) async fn apply_mutations(
        &self,
        writer: &mut impl KvWriter,
        base_root: Option<&TrackedStateRootId>,
        mutations: Vec<TrackedStateMutation>,
        commit_id: Option<&str>,
    ) -> Result<TrackedStateApplyResult, LixError> {
        let mut entries = match base_root {
            Some(root_id) => self
                .collect_leaf_entries(writer, root_id)
                .await?
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<BTreeMap<_, _>>(),
            None => BTreeMap::new(),
        };

        // Apply in caller order so repeated writes to the same key behave like
        // normal transaction staging: the latest mutation wins.
        for mutation in mutations {
            match mutation {
                TrackedStateMutation::Put { key, value } => {
                    entries.insert(encode_key(&key), encode_value(&value));
                }
            }
        }

        let built = self.build_tree_from_entries(
            entries
                .into_iter()
                .map(|(key, value)| EncodedLeafEntry { key, value })
                .collect(),
        )?;
        storage::write_chunks(writer, &built.chunks).await?;
        let persisted_root = if let Some(commit_id) = commit_id {
            storage::store_root(writer, commit_id, &built.root_id).await?;
            true
        } else {
            false
        };

        Ok(TrackedStateApplyResult {
            root_id: built.root_id,
            row_count: built.row_count,
            tree_height: built.tree_height,
            chunk_count: built.chunks.len(),
            chunk_bytes: built.chunk_bytes,
            persisted_root,
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

    async fn collect_leaf_entries(
        &self,
        store: &mut impl KvStore,
        root_id: &TrackedStateRootId,
    ) -> Result<Vec<EncodedLeafEntry>, LixError> {
        let mut out = Vec::new();
        let mut current = vec![*root_id.as_bytes()];
        while !current.is_empty() {
            let mut next = Vec::new();
            for hash in current {
                match self.load_node(store, &hash).await? {
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

    async fn load_node(
        &self,
        store: &mut impl KvStore,
        hash: &[u8; TRACKED_STATE_HASH_BYTES],
    ) -> Result<DecodedNode, LixError> {
        let bytes = storage::read_chunk(store, hash).await?.ok_or_else(|| {
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
        let item_size = entry.key.len() + entry.value.len();
        let projected_size = estimate_leaf_chunk_size(
            current.entries.len() + 1,
            current.key_bytes + entry.key.len(),
            current.value_bytes + entry.value.len(),
        );
        if !current.entries.is_empty() && projected_size > options.max_chunk_bytes {
            groups.push(std::mem::take(&mut current));
        }

        current.key_bytes += entry.key.len();
        current.value_bytes += entry.value.len();
        current.entries.push(entry);
        let current_size = estimate_leaf_chunk_size(
            current.entries.len(),
            current.key_bytes,
            current.value_bytes,
        );
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

fn estimate_leaf_chunk_size(entry_count: usize, key_bytes: usize, value_bytes: usize) -> usize {
    16 + entry_count * 8 + key_bytes + value_bytes
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};

    #[tokio::test]
    async fn exact_read_roundtrips_from_stored_root() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "entity");
        let value = value("change-1", Some("{}"));

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let result = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![TrackedStateMutation::put(key.clone(), value.clone())],
                Some("commit-1"),
            )
            .await
            .expect("mutations should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        assert_eq!(
            tree.load_root(&mut store, "commit-1")
                .await
                .expect("root should load"),
            Some(result.root_id.clone())
        );
        assert_eq!(
            tree.get(&mut store, &result.root_id, &key)
                .await
                .expect("row should load"),
            Some(value)
        );
    }

    #[tokio::test]
    async fn latest_mutation_for_key_wins() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();
        let key = key("schema", None, "entity");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let result = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![
                    TrackedStateMutation::put(key.clone(), value("change-old", Some("{\"v\":1}"))),
                    TrackedStateMutation::put(key.clone(), value("change-new", Some("{\"v\":2}"))),
                ],
                None,
            )
            .await
            .expect("mutations should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        let loaded = tree
            .get(&mut store, &result.root_id, &key)
            .await
            .expect("row should load")
            .expect("row should exist");
        assert_eq!(loaded.change_id, "change-new");
        assert_eq!(loaded.snapshot_content.as_deref(), Some("{\"v\":2}"));
    }

    #[tokio::test]
    async fn scan_filters_and_hides_tombstones_by_default() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let result = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![
                    TrackedStateMutation::put(
                        key("schema-a", None, "visible"),
                        value("c1", Some("{}")),
                    ),
                    TrackedStateMutation::put(key("schema-a", None, "deleted"), value("c2", None)),
                    TrackedStateMutation::put(
                        key("schema-b", None, "other"),
                        value("c3", Some("{}")),
                    ),
                ],
                None,
            )
            .await
            .expect("mutations should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        let rows = tree
            .scan(
                &mut store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.entity_id, "visible");
    }

    #[tokio::test]
    async fn scan_filters_by_schema_entity_and_file() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let result = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![
                    TrackedStateMutation::put(
                        key("schema-a", Some("file-a"), "entity-a"),
                        value("c1", Some("{}")),
                    ),
                    TrackedStateMutation::put(
                        key("schema-a", Some("file-b"), "entity-a"),
                        value("c2", Some("{}")),
                    ),
                    TrackedStateMutation::put(
                        key("schema-a", Some("file-a"), "entity-b"),
                        value("c3", Some("{}")),
                    ),
                    TrackedStateMutation::put(
                        key("schema-b", Some("file-a"), "entity-a"),
                        value("c4", Some("{}")),
                    ),
                ],
                None,
            )
            .await
            .expect("mutations should apply");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        let rows = tree
            .scan(
                &mut store,
                &result.root_id,
                &TrackedStateTreeScanRequest {
                    schema_keys: vec!["schema-a".to_string()],
                    entity_ids: vec!["entity-a".to_string()],
                    file_ids: vec![crate::NullableKeyFilter::Value("file-a".to_string())],
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.schema_key, "schema-a");
        assert_eq!(rows[0].0.entity_id, "entity-a");
        assert_eq!(rows[0].0.file_id.as_deref(), Some("file-a"));
    }

    #[tokio::test]
    async fn applying_to_base_root_reuses_existing_rows_and_overwrites_changed_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();
        let unchanged_key = key("schema", None, "unchanged");
        let changed_key = key("schema", None, "changed");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let base = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![
                    TrackedStateMutation::put(unchanged_key.clone(), value("c1", Some("{}"))),
                    TrackedStateMutation::put(
                        changed_key.clone(),
                        value("c2", Some("{\"old\":true}")),
                    ),
                ],
                None,
            )
            .await
            .expect("base should build");
        let next = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                Some(&base.root_id),
                vec![TrackedStateMutation::put(
                    changed_key.clone(),
                    value("c3", Some("{\"new\":true}")),
                )],
                None,
            )
            .await
            .expect("next should build");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = Arc::clone(&backend);
        assert_eq!(
            tree.get(&mut store, &next.root_id, &unchanged_key)
                .await
                .expect("unchanged read")
                .expect("unchanged exists")
                .change_id,
            "c1"
        );
        assert_eq!(
            tree.get(&mut store, &next.root_id, &changed_key)
                .await
                .expect("changed read")
                .expect("changed exists")
                .change_id,
            "c3"
        );
    }

    #[tokio::test]
    async fn two_commit_roots_can_share_unchanged_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tree = TrackedStateTree::new();
        let shared_key = key("schema", None, "shared");
        let branch_a_key = key("schema", None, "branch-a");
        let branch_b_key = key("schema", None, "branch-b");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let base = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                None,
                vec![TrackedStateMutation::put(
                    shared_key.clone(),
                    value("shared-change", Some("{\"shared\":true}")),
                )],
                Some("commit-base"),
            )
            .await
            .expect("base root should build");
        let branch_a = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                Some(&base.root_id),
                vec![TrackedStateMutation::put(
                    branch_a_key.clone(),
                    value("branch-a-change", Some("{\"branch\":\"a\"}")),
                )],
                Some("commit-a"),
            )
            .await
            .expect("branch a root should build");
        let branch_b = tree
            .apply_mutations(
                &mut transaction.as_mut(),
                Some(&base.root_id),
                vec![TrackedStateMutation::put(
                    branch_b_key.clone(),
                    value("branch-b-change", Some("{\"branch\":\"b\"}")),
                )],
                Some("commit-b"),
            )
            .await
            .expect("branch b root should build");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        assert_ne!(branch_a.root_id, branch_b.root_id);
        let mut store = Arc::clone(&backend);
        assert_eq!(
            tree.get(&mut store, &branch_a.root_id, &shared_key)
                .await
                .expect("branch a shared row should load"),
            Some(value("shared-change", Some("{\"shared\":true}")))
        );
        assert_eq!(
            tree.get(&mut store, &branch_b.root_id, &shared_key)
                .await
                .expect("branch b shared row should load"),
            Some(value("shared-change", Some("{\"shared\":true}")))
        );
        assert!(tree
            .get(&mut store, &branch_a.root_id, &branch_b_key)
            .await
            .expect("branch a should read")
            .is_none());
        assert!(tree
            .get(&mut store, &branch_b.root_id, &branch_a_key)
            .await
            .expect("branch b should read")
            .is_none());
    }

    fn key(schema_key: &str, file_id: Option<&str>, entity_id: &str) -> TrackedStateKey {
        TrackedStateKey {
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            entity_id: entity_id.to_string(),
        }
    }

    fn value(change_id: &str, snapshot_content: Option<&str>) -> TrackedStateValue {
        TrackedStateValue {
            plugin_key: None,
            snapshot_content: snapshot_content.map(str::to_string),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            change_id: change_id.to_string(),
            commit_id: "commit".to_string(),
        }
    }
}
