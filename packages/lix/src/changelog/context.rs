#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::match_same_arms,
    clippy::needless_pass_by_ref_mut,
    clippy::redundant_closure,
    clippy::unnecessary_wraps,
    clippy::unused_self
)]

use std::collections::{HashMap, HashSet};
use std::fmt;

use async_trait::async_trait;
use bytes::Bytes;

use super::codec::{
    append_change_record, append_commit_record, append_transaction_change_record,
    decode_change_record,
};
use super::semantic_history::{
    CHANGE_RECORD_KIND, COMMIT_RECORD_KIND, ChildSelector, DIRECTORY_FANOUT, HISTORY_KEY_BYTES,
    LEAF_MAX_RECORDS, MEMBERSHIP_RECORD_KIND, NODE_KEY_PREFIX, ROOT_KEY, SemanticHistoryDirectory,
    SemanticHistoryRecord, SemanticHistoryRoot, SemanticHistorySegment, decode_directory,
    decode_root, decode_segment, encode_directory, encode_root, encode_segment, leaf_key, node_key,
    record_key, record_sort_key, selector_for_leaf, selector_for_node,
};
use super::store::SEMANTIC_HISTORY_SPACE;
use crate::changelog::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeRecord, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, ChangelogReader, ChangelogWriter, CommitId, CommitLoadBatch,
    CommitLoadRequest, CommitRecord, CommitScanBatch, CommitScanRequest,
    TransactionChangelogAppend,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapter, StorageAdapterRead, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageReadOptions, StorageSpace, StorageWriteSet,
};
use crate::{LixError, storage_codec};

const SCAN_PAGE_LIMIT: usize = 1024;

fn exactly_one_get(values: Vec<Option<Vec<u8>>>, what: &str) -> Result<Option<Vec<u8>>, LixError> {
    let mut values = values.into_iter();
    let value = values.next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("semantic-history {what} returned no result slot"),
        )
    })?;
    if values.next().is_some() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("semantic-history {what} returned extra result slots"),
        ));
    }
    Ok(value)
}

fn corruption(message: &str) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn validate_directory_selector(
    selector: &ChildSelector,
    node: &SemanticHistoryDirectory,
) -> Result<(), LixError> {
    if selector_for_node(node)? != *selector {
        return Err(corruption("semantic-history directory selector mismatch"));
    }
    Ok(())
}

fn select_child_index(children: &[ChildSelector], key: [u8; HISTORY_KEY_BYTES]) -> Option<usize> {
    children
        .iter()
        .position(|child| child.first <= key && key <= child.last)
        .or_else(|| children.iter().position(|child| key < child.first))
        .or_else(|| children.len().checked_sub(1))
}

struct SemanticTreeEditor<'a, S: ?Sized> {
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
    root: SemanticHistoryRoot,
    observed_root: Option<Bytes>,
    nodes: HashMap<Vec<u8>, SemanticHistoryDirectory>,
    leaves: HashMap<Vec<u8>, SemanticHistorySegment>,
    new_objects: HashMap<Vec<u8>, Vec<u8>>,
    retired: HashSet<Vec<u8>>,
}

impl<'a, S> SemanticTreeEditor<'a, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    fn new(
        store: &'a mut S,
        writes: &'a mut StorageWriteSet,
        root: SemanticHistoryRoot,
        observed_root: Option<Bytes>,
    ) -> Self {
        Self {
            store,
            writes,
            root,
            observed_root,
            nodes: HashMap::new(),
            leaves: HashMap::new(),
            new_objects: HashMap::new(),
            retired: HashSet::new(),
        }
    }

    async fn read_object(&mut self, key: &[u8]) -> Result<Vec<u8>, LixError> {
        exactly_one_get(
            self.store
                .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![key.to_vec()])
                .await?,
            "content object read",
        )?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "semantic-history root references a missing content object",
            )
        })
    }

    async fn load_node(&mut self, key: &[u8]) -> Result<SemanticHistoryDirectory, LixError> {
        if key.first() != Some(&NODE_KEY_PREFIX) || key.len() != 33 {
            return Err(corruption("semantic-history node locator is malformed"));
        }
        if let Some(node) = self.nodes.get(key) {
            return Ok(node.clone());
        }
        let node = if let Some(bytes) = self.new_objects.get(key) {
            decode_directory(bytes)?
        } else {
            decode_directory(&self.read_object(key).await?)?
        };
        if node.digest != key[1..] {
            return Err(corruption(
                "semantic-history node digest does not match locator",
            ));
        }
        self.nodes.insert(key.to_vec(), node.clone());
        Ok(node)
    }

    fn validate_node_selector(
        selector: &ChildSelector,
        node: &SemanticHistoryDirectory,
    ) -> Result<(), LixError> {
        let expected = selector_for_node(node)?;
        if expected != *selector {
            return Err(corruption("semantic-history directory selector mismatch"));
        }
        Ok(())
    }

    async fn load_leaf(
        &mut self,
        selector: &ChildSelector,
    ) -> Result<SemanticHistorySegment, LixError> {
        if selector.key.first() != Some(&crate::changelog::semantic_history::LEAF_KEY_PREFIX)
            || selector.key.len() != 33
        {
            return Err(corruption("semantic-history leaf locator is malformed"));
        }
        if let Some(segment) = self.leaves.get(&selector.key) {
            return Ok(segment.clone());
        }
        let segment = if let Some(bytes) = self.new_objects.get(&selector.key) {
            decode_segment(bytes)?
        } else {
            decode_segment(&self.read_object(&selector.key).await?)?
        };
        if segment.digest != selector.digest
            || segment.min_key != selector.first
            || segment.max_key != selector.last
            || segment.records.len() != selector.record_count as usize
        {
            return Err(corruption("semantic-history leaf selector mismatch"));
        }
        self.leaves.insert(selector.key.clone(), segment.clone());
        Ok(segment)
    }

    async fn locate(
        &mut self,
        key: [u8; HISTORY_KEY_BYTES],
    ) -> Result<Option<(Vec<(Vec<u8>, usize)>, ChildSelector)>, LixError> {
        let Some(target) = self.root.target.clone() else {
            return Ok(None);
        };
        let mut node_key = target.key.clone();
        let mut expected_selector = Some(target);
        let mut path = Vec::new();
        loop {
            let node = self.load_node(&node_key).await?;
            if let Some(selector) = expected_selector.take() {
                Self::validate_node_selector(&selector, &node)?;
            }
            let index = select_child_index(&node.children, key)
                .ok_or_else(|| corruption("semantic-history directory has no children"))?;
            let child = node.children[index].clone();
            path.push((node_key.clone(), index));
            if node.level == 0 {
                return Ok(Some((path, child)));
            }
            if child.key.first() != Some(&NODE_KEY_PREFIX) {
                return Err(corruption(
                    "semantic-history directory child level mismatch",
                ));
            }
            node_key = child.key.clone();
            expected_selector = Some(child);
        }
    }

    async fn apply(
        &mut self,
        additions: Vec<SemanticHistoryRecord>,
        removals: &[(u8, [u8; 16])],
    ) -> Result<SemanticHistoryRoot, LixError> {
        let next_generation = self.root.generation.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "semantic-history generation overflow",
            )
        })?;
        let mut groups = HashMap::<
            Option<Vec<u8>>,
            (Vec<SemanticHistoryRecord>, Vec<[u8; HISTORY_KEY_BYTES]>),
        >::new();
        for record in additions {
            let key = record_sort_key(&record);
            let leaf = self.locate(key).await?.map(|(_, selector)| selector.key);
            groups.entry(leaf).or_default().0.push(record);
        }
        for (kind, id) in removals {
            let key = record_key(*kind, *id);
            let Some((_, selector)) = self.locate(key).await? else {
                return Err(corruption(
                    "semantic-history delete references a missing record",
                ));
            };
            groups.entry(Some(selector.key)).or_default().1.push(key);
        }
        let mut group_keys = groups.keys().cloned().collect::<Vec<_>>();
        group_keys.sort();
        for leaf_key in group_keys {
            let (additions, removals) = groups.remove(&leaf_key).expect("group key exists");
            let (path, selector, mut records) = if let Some(_leaf_key) = leaf_key {
                let key = additions
                    .first()
                    .map(record_sort_key)
                    .or_else(|| removals.first().copied())
                    .ok_or_else(|| corruption("empty semantic-history mutation group"))?;
                let (path, selector) = self
                    .locate(key)
                    .await?
                    .ok_or_else(|| corruption("semantic-history leaf disappeared"))?;
                let records = self.load_leaf(&selector).await?.records;
                (Some(path), Some(selector), records)
            } else {
                (None, None, Vec::new())
            };
            for key in removals {
                let before = records.len();
                records.retain(|record| record_sort_key(record) != key);
                if records.len() == before {
                    return Err(corruption(
                        "semantic-history delete references a missing record",
                    ));
                }
            }
            records.extend(additions);
            records.sort_by_key(record_sort_key);
            for pair in records.windows(2) {
                if record_sort_key(&pair[0]) == record_sort_key(&pair[1]) {
                    return Err(corruption("semantic-history duplicate record"));
                }
            }
            let replacements = self.make_leaf_selectors(next_generation, records)?;
            if let Some(path) = path {
                let old_leaf = selector.expect("path has leaf");
                self.retired.insert(old_leaf.key.clone());
                let root_key = self
                    .root
                    .target
                    .as_ref()
                    .expect("path has root")
                    .key
                    .clone();
                let selectors = self.replace_node_sync(root_key, &path, replacements)?;
                self.root.target = self.promote_root(selectors)?;
            } else {
                let selectors = self.make_directory_selectors(0, replacements)?;
                self.root.target = self.promote_root(selectors)?;
            }
            self.root.record_count = self
                .root
                .target
                .as_ref()
                .map_or(0, |target| target.record_count as u64);
        }
        self.root.generation = next_generation;
        self.stage_reachable_objects()?;
        self.root.clone().seal()
    }

    fn promote_root(
        &mut self,
        mut selectors: Vec<ChildSelector>,
    ) -> Result<Option<ChildSelector>, LixError> {
        if selectors.is_empty() {
            return Ok(None);
        }
        let mut level = self
            .nodes
            .values()
            .map(|node| node.level)
            .max()
            .unwrap_or(0);
        while selectors.len() > 1 {
            level = level.saturating_add(1);
            selectors = self.make_directory_selectors(level, selectors)?;
        }
        Ok(selectors.pop())
    }

    fn make_leaf_selectors(
        &mut self,
        generation: u64,
        records: Vec<SemanticHistoryRecord>,
    ) -> Result<Vec<ChildSelector>, LixError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        let mut current = Vec::new();
        for record in records {
            current.push(record);
            if current.len() == LEAF_MAX_RECORDS {
                output.push(self.seal_leaf(generation, std::mem::take(&mut current))?);
            } else if SemanticHistorySegment::seal(generation, current.clone()).is_err()
                && current.len() > 1
            {
                let last = current.pop().expect("current is non-empty");
                output.push(self.seal_leaf(generation, std::mem::take(&mut current))?);
                current.push(last);
            }
        }
        if !current.is_empty() {
            output.push(self.seal_leaf(generation, current)?);
        }
        Ok(output)
    }

    fn seal_leaf(
        &mut self,
        generation: u64,
        records: Vec<SemanticHistoryRecord>,
    ) -> Result<ChildSelector, LixError> {
        let segment = SemanticHistorySegment::seal(generation, records)?;
        let key = leaf_key(segment.digest);
        self.leaves.insert(key.clone(), segment.clone());
        self.new_objects.insert(key, encode_segment(&segment)?);
        selector_for_leaf(&segment)
    }

    fn make_directory_selectors(
        &mut self,
        level: u8,
        children: Vec<ChildSelector>,
    ) -> Result<Vec<ChildSelector>, LixError> {
        children
            .chunks(DIRECTORY_FANOUT)
            .map(|chunk| {
                let node = SemanticHistoryDirectory::seal(level, chunk.to_vec())?;
                let key = node_key(node.digest);
                self.new_objects
                    .insert(key.clone(), encode_directory(&node)?);
                self.nodes.insert(key, node.clone());
                selector_for_node(&node)
            })
            .collect()
    }

    fn replace_node_sync(
        &mut self,
        node_key: Vec<u8>,
        path: &[(Vec<u8>, usize)],
        replacements: Vec<ChildSelector>,
    ) -> Result<Vec<ChildSelector>, LixError> {
        let node = self
            .nodes
            .get(&node_key)
            .cloned()
            .ok_or_else(|| corruption("semantic-history path node was not loaded"))?;
        let index = path
            .iter()
            .find(|(key, _)| key == &node_key)
            .map(|(_, index)| *index)
            .ok_or_else(|| corruption("semantic-history path index missing"))?;
        let children = if node.level == 0 {
            let mut children = node.children;
            children.splice(index..=index, replacements);
            children
        } else {
            let child_key = node.children[index].key.clone();
            let child_replacements = self.replace_node_sync(child_key, path, replacements)?;
            let mut children = node.children;
            children.splice(index..=index, child_replacements);
            children
        };
        self.retired.insert(node_key);
        if children.is_empty() {
            return Ok(Vec::new());
        }
        self.make_directory_selectors(node.level, children)
    }

    fn stage_reachable_objects(&mut self) -> Result<(), LixError> {
        let mut reachable = HashSet::new();
        if let Some(target) = &self.root.target {
            self.collect_new_keys(&target.key, &mut reachable)?;
        }
        for (key, bytes) in self.new_objects.clone() {
            if reachable.contains(&key) {
                self.writes.put(SEMANTIC_HISTORY_SPACE, key, bytes);
            }
        }
        for key in self.retired.drain() {
            if !reachable.contains(&key) {
                self.new_objects.remove(&key);
                self.writes.delete(SEMANTIC_HISTORY_SPACE, key);
            }
        }
        Ok(())
    }

    fn collect_new_keys(
        &self,
        key: &[u8],
        reachable: &mut HashSet<Vec<u8>>,
    ) -> Result<(), LixError> {
        if !self.new_objects.contains_key(key) || !reachable.insert(key.to_vec()) {
            return Ok(());
        }
        if key.first() == Some(&NODE_KEY_PREFIX) {
            let node = self
                .nodes
                .get(key)
                .ok_or_else(|| corruption("new directory missing"))?;
            for child in &node.children {
                self.collect_new_keys(&child.key, reachable)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::semantic_history::LEAF_KEY_PREFIX;
    use super::*;
    use crate::changelog::{
        ChangeLoadRequest, ChangeRecord, ChangelogReader, CommitLoadRequest, CommitRecord,
        TransactionChangeRecordRef, TransactionChangelogAppend,
    };
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::json_store::JsonSlot;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn selector(first: u8, last: u8) -> ChildSelector {
        let mut first_key = [0; HISTORY_KEY_BYTES];
        first_key[0] = COMMIT_RECORD_KIND;
        first_key[HISTORY_KEY_BYTES - 1] = first;
        let mut last_key = [0; HISTORY_KEY_BYTES];
        last_key[0] = COMMIT_RECORD_KIND;
        last_key[HISTORY_KEY_BYTES - 1] = last;
        ChildSelector {
            first: first_key,
            last: last_key,
            key: vec![LEAF_KEY_PREFIX; 33],
            record_count: 1,
            byte_len: 1,
            digest: [0; 32],
        }
    }

    #[test]
    fn identity_gap_selects_next_child_not_last_child() {
        let children = [selector(10, 15), selector(30, 35), selector(50, 55)];
        let mut key = [0; HISTORY_KEY_BYTES];
        key[0] = COMMIT_RECORD_KIND;
        key[HISTORY_KEY_BYTES - 1] = 20;

        assert_eq!(select_child_index(&children, key), Some(1));
    }

    fn test_commit(label: &str, change_id: ChangeId) -> CommitRecord {
        CommitRecord {
            format_version: 2,
            commit_id: CommitId::for_test_label(label),
            generation: 0,
            parent_commit_ids: Vec::new(),
            change_id,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::expect_parse(
                "semantic-history test timestamp",
                "2026-01-01T00:00:00.000Z",
            ),
        }
    }

    fn test_change(change_id: ChangeId) -> ChangeRecord {
        ChangeRecord {
            format_version: 2,
            change_id,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            schema_key: "semantic_history_test".to_string(),
            entity_pk: EntityPk::single("entity"),
            file_id: None,
            snapshot: JsonSlot::None,
            metadata: JsonSlot::None,
            created_at: LixTimestamp::expect_parse(
                "semantic-history test timestamp",
                "2026-01-01T00:00:00.000Z",
            ),
            origin_key: None,
        }
    }

    async fn append_test_records(
        storage: &StorageAdapter<Memory>,
        commits: Vec<CommitRecord>,
        changes: &[ChangeRecord],
    ) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("semantic-history test read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_transaction_append(TransactionChangelogAppend {
                commits,
                changes: changes
                    .iter()
                    .map(TransactionChangeRecordRef::from)
                    .collect(),
            })
            .await
            .expect("semantic-history test append should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("semantic-history test append should commit");
    }

    #[tokio::test]
    async fn semantic_gc_retires_envelope_only_and_persisted_change_records() {
        let storage = StorageAdapter::new(Memory::new());
        let envelope_change = ChangeId::for_test_label("semantic-envelope-only-change");
        let persisted_change = ChangeId::for_test_label("semantic-persisted-change");
        let retained_change = ChangeId::for_test_label("semantic-retained-change");
        let envelope_commit = test_commit("semantic-envelope-only-commit", envelope_change);
        let persisted_commit = test_commit("semantic-persisted-change-commit", persisted_change);
        let retained_commit = test_commit("semantic-retained-commit", retained_change);
        let persisted_change_record = test_change(persisted_change);
        append_test_records(
            &storage,
            vec![
                envelope_commit.clone(),
                persisted_commit.clone(),
                retained_commit.clone(),
            ],
            std::slice::from_ref(&persisted_change_record),
        )
        .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("semantic-history retirement read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_delete_records(
                &[envelope_commit.commit_id, persisted_commit.commit_id],
                &[],
            )
            .await
            .expect("envelope-only retirement should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("semantic-history retirement should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reopened semantic-history read should open");
        let mut reader = ChangelogContext::new().reader(read);
        assert!(
            reader
                .load_commits(CommitLoadRequest {
                    commit_ids: &[
                        envelope_commit.commit_id,
                        persisted_commit.commit_id,
                        retained_commit.commit_id,
                    ],
                })
                .await
                .expect("retired commits should load")
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
                .as_slice()
                == [None, None, Some(retained_commit.clone())]
        );
        assert!(
            reader
                .load_changes(ChangeLoadRequest {
                    change_ids: &[persisted_change],
                })
                .await
                .expect("retired persisted change should load")
                .into_iter()
                .all(|(_, record)| record.is_none())
        );
    }

    #[tokio::test]
    async fn semantic_gc_rejects_malformed_present_change_records() {
        let storage = StorageAdapter::new(Memory::new());
        let change_id = ChangeId::for_test_label("semantic-malformed-change");
        let commit = test_commit("semantic-malformed-commit", change_id);
        let change = test_change(change_id);
        append_test_records(
            &storage,
            vec![commit.clone()],
            std::slice::from_ref(&change),
        )
        .await;

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("semantic-history corruption read should open");
        let root_bytes = exactly_one_get(
            read.changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![ROOT_KEY.to_vec()])
                .await
                .expect("semantic-history root should read"),
            "test root read",
        )
        .expect("semantic-history root read should succeed")
        .expect("semantic-history root should exist");
        let root = decode_root(&root_bytes).expect("semantic-history root should decode");
        let node_bytes = exactly_one_get(
            read.changelog_get_many(
                SEMANTIC_HISTORY_SPACE,
                vec![root.target.expect("root target should exist").key],
            )
            .await
            .expect("semantic-history node should read"),
            "test node read",
        )
        .expect("semantic-history node read should succeed")
        .expect("semantic-history node should exist");
        let node = decode_directory(&node_bytes).expect("semantic-history node should decode");
        let leaf_key = node.children[0].key.clone();
        drop(read);

        let mut writes = storage.new_write_set();
        writes.put(SEMANTIC_HISTORY_SPACE, leaf_key, b"malformed".to_vec());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("semantic-history corruption should commit");

        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("semantic-history malformed read should open");
        let mut writes = storage.new_write_set();
        let error = ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_delete_records(&[commit.commit_id], &[])
            .await
            .expect_err("malformed semantic-history records must fail closed");
        assert!(error.message.contains("semantic-history"));
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn reader<S>(&self, store: S) -> ChangelogStoreReader<S>
    where
        S: ChangelogStorageRead,
    {
        ChangelogStoreReader { store }
    }

    pub(crate) fn writer<'a, S>(
        &self,
        store: &'a mut S,
        writes: &'a mut StorageWriteSet,
    ) -> ChangelogStoreWriter<'a, S>
    where
        S: ChangelogStorageRead + ?Sized,
    {
        ChangelogStoreWriter {
            store,
            writes,
            staged_commits: HashMap::new(),
            staged_changes: HashMap::new(),
            staged_change_deletes: HashSet::new(),
        }
    }
}

pub(crate) struct ChangelogStoreReader<S> {
    store: S,
}

pub(crate) struct ChangelogStoreWriter<'a, S: ?Sized> {
    store: &'a mut S,
    writes: &'a mut StorageWriteSet,
    staged_commits: HashMap<CommitId, CommitRecord>,
    staged_changes: HashMap<ChangeId, ChangeRecord>,
    staged_change_deletes: HashSet<ChangeId>,
}

#[async_trait]
pub(crate) trait ChangelogStorageRead {
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError>;
}

#[async_trait]
impl<T> ChangelogStorageRead for T
where
    T: StorageAdapterRead + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        native_get_many(self, space, keys).await
    }
}

#[async_trait]
impl<StorageImpl> ChangelogStorageRead for StorageAdapter<StorageImpl>
where
    StorageImpl: Storage + Send,
{
    async fn changelog_get_many(
        &mut self,
        space: StorageSpace,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LixError> {
        let mut read = self.begin_read(StorageReadOptions::default()).await?;
        native_get_many(&mut read, space, keys).await
    }
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreReader<S>
where
    S: ChangelogStorageRead + Send,
{
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError> {
        load_commits_from_store(&mut self.store, request).await
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        scan_commits_from_store(&mut self.store, request).await
    }

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError> {
        load_changes_from_store(&mut self.store, request).await
    }

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError> {
        scan_changes_from_store(&mut self.store, request).await
    }
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError> {
        let stored = load_commits_from_store(self.store, request).await?;
        let entries = stored
            .into_iter()
            .map(|(commit_id, stored)| {
                if let Some(record) = self.staged_commits.get(commit_id) {
                    return Some(record.clone());
                }
                stored
            })
            .collect();
        CommitLoadBatch::try_new("changelog commit overlay", request.commit_ids, entries)
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        let mut batch = scan_commits_from_store(self.store, request).await?;
        let mut staged = self
            .staged_commits
            .values()
            .filter(|commit| {
                request
                    .start_after
                    .map(|start_after| commit.commit_id.to_string().as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by_key(|left| left.commit_id);
        batch.entries.extend(staged);
        batch.entries.sort_by_key(|commit| commit.commit_id);
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().map(|commit| commit.commit_id);
        }
        Ok(batch)
    }

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError> {
        let stored = load_changes_from_store(self.store, request).await?;
        let entries = stored
            .into_iter()
            .map(|(change_id, stored)| self.staged_changes.get(change_id).cloned().or(stored))
            .collect();
        ChangeLoadBatch::try_new("changelog change overlay", request.change_ids, entries)
    }

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError> {
        let mut batch = scan_changes_from_store(self.store, request).await?;
        let mut staged = self
            .staged_changes
            .values()
            .filter(|change| {
                request
                    .start_after
                    .map(|start_after| change.change_id.to_string().as_str() > start_after)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        staged.sort_by_key(|left| left.change_id);
        batch.entries.extend(staged);
        batch.entries.sort_by_key(|left| left.change_id);
        batch
            .entries
            .dedup_by(|left, right| left.change_id == right.change_id);
        let limit = request.limit.unwrap_or(usize::MAX);
        if batch.entries.len() > limit {
            batch.entries.truncate(limit);
            batch.next_start_after = batch.entries.last().map(|change| change.change_id);
        }
        Ok(batch)
    }
}

#[async_trait]
impl<S> ChangelogWriter for ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn stage_append(&mut self, append: ChangelogAppend) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        self.validate_append(&append).await?;
        self.stage_append_records(append).await
    }

    async fn stage_delete_standalone_changes(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        let change_ids = change_ids.iter().copied().collect::<HashSet<_>>();
        for change_id in &change_ids {
            if self.staged_changes.contains_key(change_id) {
                return Err(LixError::unknown(format!(
                    "cannot delete changelog change '{change_id}' because it was staged in the same transaction"
                )));
            }
        }
        for change_id in change_ids {
            if self.staged_change_deletes.insert(change_id) {
                // The terminal transaction append coalesces these removals
                // with its additions into one root publication. This writer
                // is not otherwise used for standalone deletion.
            }
        }
        Ok(())
    }
}

impl<S> ChangelogStoreWriter<'_, S>
where
    S: ChangelogStorageRead + Send + ?Sized,
{
    async fn semantic_root_observed(
        &mut self,
    ) -> Result<(SemanticHistoryRoot, Option<Bytes>), LixError> {
        let value = exactly_one_get(
            self.store
                .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![ROOT_KEY.to_vec()])
                .await?,
            "root read",
        )?;
        value.map_or_else(
            || Ok((SemanticHistoryRoot::empty(), None)),
            |bytes| decode_root(&bytes).map(|root| (root, Some(Bytes::from(bytes)))),
        )
    }

    async fn stage_semantic_records(
        &mut self,
        additions: Vec<SemanticHistoryRecord>,
        removals: &[(u8, [u8; 16])],
    ) -> Result<(), LixError> {
        if additions.is_empty() && removals.is_empty() {
            return Ok(());
        }
        let (root, observed_root) = self.semantic_root_observed().await?;
        let mut editor = SemanticTreeEditor::new(self.store, self.writes, root, observed_root);
        let root = editor.apply(additions, removals).await?;
        let root_precondition = editor.observed_root.map_or_else(
            || crate::storage::Precondition::KeyAbsent {
                space: SEMANTIC_HISTORY_SPACE,
                key: crate::storage::Key(Bytes::copy_from_slice(ROOT_KEY)),
            },
            |expected| crate::storage::Precondition::KeyValueEquals {
                space: SEMANTIC_HISTORY_SPACE,
                key: crate::storage::Key(Bytes::copy_from_slice(ROOT_KEY)),
                expected,
            },
        );
        editor.writes.add_precondition(root_precondition);
        editor
            .writes
            .put(SEMANTIC_HISTORY_SPACE, ROOT_KEY, encode_root(&root)?);
        Ok(())
    }

    /// Removes semantic records through the same owner used for appends. The
    /// selector is rewritten once for the complete retirement set, so GC
    /// cannot leave a change row and its reverse membership in different
    /// physical authorities.
    pub(crate) async fn stage_delete_records(
        &mut self,
        commit_ids: &[CommitId],
        change_ids: &[ChangeId],
    ) -> Result<(), LixError> {
        let mut removals = Vec::with_capacity(commit_ids.len() * 2 + change_ids.len());
        for commit_id in commit_ids {
            let Some(value) = semantic_load_record(
                self.store,
                COMMIT_RECORD_KIND,
                *commit_id.as_uuid().as_bytes(),
            )
            .await?
            else {
                continue;
            };
            let record: CommitRecord = storage_codec::decode("commit record", &value)?;
            // The terminal transaction writer publishes this reverse membership
            // with every commit; unlike the envelope change id, it is mandatory.
            let membership = semantic_load_record(
                self.store,
                MEMBERSHIP_RECORD_KIND,
                *record.change_id.as_uuid().as_bytes(),
            )
            .await?
            .ok_or_else(|| corruption("semantic-history commit membership is missing"))?;
            let membership_commit = uuid::Uuid::from_slice(&membership).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("semantic-history membership has invalid commit id: {error}"),
                )
            })?;
            if membership_commit != *commit_id.as_uuid() {
                return Err(corruption(
                    "semantic-history membership references the wrong commit",
                ));
            }
            removals.push((COMMIT_RECORD_KIND, *commit_id.as_uuid().as_bytes()));
            removals.push((
                MEMBERSHIP_RECORD_KIND,
                *record.change_id.as_uuid().as_bytes(),
            ));
            if let Some(change) = semantic_load_record(
                self.store,
                CHANGE_RECORD_KIND,
                *record.change_id.as_uuid().as_bytes(),
            )
            .await?
            {
                decode_change_record(&change, record.change_id)?;
                removals.push((CHANGE_RECORD_KIND, *record.change_id.as_uuid().as_bytes()));
            }
        }
        for change_id in change_ids {
            let Some(change) = semantic_load_record(
                self.store,
                CHANGE_RECORD_KIND,
                *change_id.as_uuid().as_bytes(),
            )
            .await?
            else {
                return Err(corruption("semantic-history standalone change is missing"));
            };
            decode_change_record(&change, *change_id)?;
            removals.push((CHANGE_RECORD_KIND, *change_id.as_uuid().as_bytes()));
        }
        // A committed change is owned by its semantic commit. Callers may
        // also pass the same change id in `change_ids`; canonicalize the
        // retirement frontier before applying it so one physical record is
        // never removed twice.
        removals.sort_unstable();
        removals.dedup();
        self.stage_semantic_records(Vec::new(), &removals).await
    }

    /// Stages a terminal append assembled from already-prepared transaction rows.
    ///
    /// The transaction owns ID generation, parent selection, and change-ref
    /// construction. This path has no read-your-writes overlay: its writer is
    /// dropped immediately after the append, so retaining a second owned copy
    /// of every row would only add allocation and drop work. Direct changelog
    /// callers continue to use `stage_append`.
    pub(crate) async fn stage_transaction_append(
        &mut self,
        append: TransactionChangelogAppend<'_>,
    ) -> Result<(), LixError> {
        self.ensure_changelog_mutation_is_allowed()?;
        let TransactionChangelogAppend { commits, changes } = append;
        let mut records = Vec::with_capacity(commits.len() + changes.len() * 2);
        for change in &changes {
            let mut value = Vec::new();
            append_transaction_change_record(&mut value, change)?;
            records.push(SemanticHistoryRecord {
                kind: CHANGE_RECORD_KIND,
                id: *change.change_id.as_uuid().as_bytes(),
                value,
            });
        }
        for commit in &commits {
            let mut value = Vec::new();
            append_commit_record(&mut value, commit)?;
            records.push(SemanticHistoryRecord {
                kind: COMMIT_RECORD_KIND,
                id: *commit.commit_id.as_uuid().as_bytes(),
                value,
            });
            records.push(SemanticHistoryRecord {
                kind: MEMBERSHIP_RECORD_KIND,
                id: *commit.change_id.as_uuid().as_bytes(),
                value: commit.commit_id.as_uuid().as_bytes().to_vec(),
            });
        }
        let removals = self
            .staged_change_deletes
            .iter()
            .map(|change_id| (CHANGE_RECORD_KIND, *change_id.as_uuid().as_bytes()))
            .collect::<Vec<_>>();
        self.stage_semantic_records(records, &removals).await?;
        Ok(())
    }

    async fn stage_append_records(&mut self, append: ChangelogAppend) -> Result<(), LixError> {
        let ChangelogAppend { commits, changes } = append;
        let mut records = Vec::with_capacity(commits.len() + changes.len() * 2);
        for change in &changes {
            let mut value = Vec::new();
            append_change_record(&mut value, change)?;
            records.push(SemanticHistoryRecord {
                kind: CHANGE_RECORD_KIND,
                id: *change.change_id.as_uuid().as_bytes(),
                value,
            });
        }
        for commit in &commits {
            let mut value = Vec::new();
            append_commit_record(&mut value, commit)?;
            records.push(SemanticHistoryRecord {
                kind: COMMIT_RECORD_KIND,
                id: *commit.commit_id.as_uuid().as_bytes(),
                value,
            });
            records.push(SemanticHistoryRecord {
                kind: MEMBERSHIP_RECORD_KIND,
                id: *commit.change_id.as_uuid().as_bytes(),
                value: commit.commit_id.as_uuid().as_bytes().to_vec(),
            });
        }
        // The semantic segment owner is the only writer.  The old per-family
        // batches above are intentionally no longer staged.
        self.stage_semantic_records(records, &[]).await?;

        self.staged_changes.reserve(changes.len());
        self.staged_commits.reserve(commits.len());
        for change in changes {
            self.staged_changes.insert(change.change_id, change);
        }
        for commit in commits {
            self.staged_commits.insert(commit.commit_id, commit);
        }
        Ok(())
    }
    fn ensure_changelog_mutation_is_allowed(&self) -> Result<(), LixError> {
        if !self.writes.changelog_gc_is_sealed() {
            return Ok(());
        }
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "cannot stage changelog mutations after garbage collection in the same transaction",
        ))
    }

    async fn validate_append(&mut self, append: &ChangelogAppend) -> Result<(), LixError> {
        validate_unique(
            append.commits.iter().map(|commit| commit.commit_id),
            "commit_id",
        )?;
        validate_unique(
            append.changes.iter().map(|change| change.change_id),
            "change_id",
        )?;
        validate_unique(
            append.commits.iter().map(|commit| commit.change_id),
            "commit change_id",
        )?;

        let append_commit_ids = append
            .commits
            .iter()
            .map(|commit| commit.commit_id)
            .collect::<HashSet<_>>();
        let append_changes = append
            .changes
            .iter()
            .map(|change| (change.change_id, change))
            .collect::<HashMap<_, _>>();

        if let Some(change_id) = append_changes
            .keys()
            .find(|change_id| self.staged_change_deletes.contains(change_id))
        {
            return Err(LixError::unknown(format!(
                "cannot append changelog change '{change_id}' because it was deleted in the same transaction"
            )));
        }

        self.reject_existing_id_collisions(append, &append_commit_ids, &append_changes)
            .await?;
        self.validate_parent_commits(append, &append_commit_ids)
            .await?;

        Ok(())
    }

    async fn reject_existing_id_collisions(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
        append_changes: &HashMap<ChangeId, &ChangeRecord>,
    ) -> Result<(), LixError> {
        for commit_id in append_commit_ids {
            if semantic_load_record(
                self.store,
                COMMIT_RECORD_KIND,
                *commit_id.as_uuid().as_bytes(),
            )
            .await?
            .is_some()
                || self.staged_commits.contains_key(commit_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{commit_id}' already exists"
                )));
            }
        }
        for change_id in append_changes.keys() {
            if semantic_load_record(
                self.store,
                CHANGE_RECORD_KIND,
                *change_id.as_uuid().as_bytes(),
            )
            .await?
            .is_some()
                || self.staged_changes.contains_key(&change_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog change '{change_id}' already exists"
                )));
            }
        }
        for commit in &append.commits {
            let change_id = commit.change_id;
            if append_changes.contains_key(&change_id)
                || semantic_load_record(
                    self.store,
                    CHANGE_RECORD_KIND,
                    *change_id.as_uuid().as_bytes(),
                )
                .await?
                .is_some()
                || self.staged_changes.contains_key(&change_id)
                || self
                    .staged_commits
                    .values()
                    .any(|staged| staged.change_id == change_id)
            {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' derived change_id '{}' collides with an existing change id",
                    commit.commit_id, commit.change_id
                )));
            }
            if semantic_load_record(
                self.store,
                MEMBERSHIP_RECORD_KIND,
                *change_id.as_uuid().as_bytes(),
            )
            .await?
            .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog commit derived change_id '{change_id}' already exists"
                )));
            }
        }
        Ok(())
    }

    async fn validate_parent_commits(
        &mut self,
        append: &ChangelogAppend,
        append_commit_ids: &HashSet<CommitId>,
    ) -> Result<(), LixError> {
        let mut parent_ids = append
            .commits
            .iter()
            .flat_map(|commit| commit.parent_commit_ids.iter().copied())
            .filter(|parent_id| !append_commit_ids.contains(parent_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        parent_ids.sort_unstable();
        let mut parent_generations = HashMap::<CommitId, u64>::new();
        for parent_id in &parent_ids {
            let found = semantic_load_record(
                self.store,
                COMMIT_RECORD_KIND,
                *parent_id.as_uuid().as_bytes(),
            )
            .await?;
            let generation = match found {
                Some(bytes) => {
                    let record: CommitRecord = storage_codec::decode("commit record", &bytes)?;
                    record.generation
                }
                None => self
                    .staged_commits
                    .get(parent_id)
                    .map(|commit| commit.generation)
                    .ok_or_else(|| {
                        LixError::unknown(format!(
                            "changelog parent commit '{parent_id}' does not exist"
                        ))
                    })?,
            };
            parent_generations.insert(*parent_id, generation);
        }
        let append_generations = append
            .commits
            .iter()
            .map(|commit| (commit.commit_id, commit.generation))
            .collect::<HashMap<_, _>>();
        for commit in &append.commits {
            let expected_generation = commit
                .parent_commit_ids
                .iter()
                .map(|parent_id| {
                    append_generations
                        .get(parent_id)
                        .or_else(|| parent_generations.get(parent_id))
                        .copied()
                        .expect("every parent generation was resolved")
                })
                .max()
                .map_or(Ok(0), |generation| {
                    generation
                        .checked_add(1)
                        .ok_or_else(|| LixError::unknown("commit generation exceeds u64"))
                })?;
            if commit.generation != expected_generation {
                return Err(LixError::unknown(format!(
                    "changelog commit '{}' has generation {}, expected {expected_generation}",
                    commit.commit_id, commit.generation
                )));
            }
        }
        Ok(())
    }
}

async fn semantic_load_record(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    kind: u8,
    id: [u8; 16],
) -> Result<Option<Vec<u8>>, LixError> {
    let root_bytes = exactly_one_get(
        store
            .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![ROOT_KEY.to_vec()])
            .await?,
        "root read",
    )?;
    let Some(root_bytes) = root_bytes else {
        return Ok(None);
    };
    let root = decode_root(&root_bytes)?;
    let Some(target) = root.target else {
        return Ok(None);
    };
    let requested = record_key(kind, id);
    let mut node_key = target.key.clone();
    let mut expected_selector = Some(target);
    loop {
        let bytes = exactly_one_get(
            store
                .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![node_key.clone()])
                .await?,
            "directory read",
        )?
        .ok_or_else(|| corruption("semantic-history root references missing directory"))?;
        let node = decode_directory(&bytes)?;
        if node.digest != node_key[1..] {
            return Err(corruption("semantic-history directory digest mismatch"));
        }
        if let Some(selector) = expected_selector.take() {
            validate_directory_selector(&selector, &node)?;
        }
        let Some(selector) = node
            .children
            .iter()
            .find(|child| child.first <= requested && requested <= child.last)
            .cloned()
        else {
            return Ok(None);
        };
        if node.level > 0 {
            if selector.key.first() != Some(&NODE_KEY_PREFIX) {
                return Err(corruption(
                    "semantic-history directory child level mismatch",
                ));
            }
            node_key = selector.key.clone();
            expected_selector = Some(selector);
            continue;
        }
        let bytes = exactly_one_get(
            store
                .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![selector.key.clone()])
                .await?,
            "leaf read",
        )?
        .ok_or_else(|| corruption("semantic-history directory references missing leaf"))?;
        let segment = decode_segment(&bytes)?;
        if segment.digest != selector.digest
            || segment.min_key != selector.first
            || segment.max_key != selector.last
            || segment.records.len() != selector.record_count as usize
        {
            return Err(corruption("semantic-history leaf selector mismatch"));
        }
        return Ok(segment
            .records
            .into_iter()
            .find(|record| record.kind == kind && record.id == id)
            .map(|record| record.value));
    }
}

/// Resolves the reverse commit membership from the same authenticated
/// semantic-history segment as the direct commit/change records.
pub(crate) async fn load_commit_id_for_change<S>(
    store: &S,
    change_id: ChangeId,
) -> Result<Option<CommitId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut store = store;
    let Some(bytes) = semantic_load_record(
        &mut store,
        MEMBERSHIP_RECORD_KIND,
        *change_id.as_uuid().as_bytes(),
    )
    .await?
    else {
        return Ok(None);
    };
    let uuid = uuid::Uuid::from_slice(&bytes).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("semantic-history membership has invalid commit id: {error}"),
        )
    })?;
    Ok(Some(CommitId::new(uuid)))
}

async fn semantic_scan_records(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    kind: u8,
    start_after: Option<[u8; HISTORY_KEY_BYTES]>,
    limit: usize,
) -> Result<Vec<SemanticHistoryRecord>, LixError> {
    let root_bytes = exactly_one_get(
        store
            .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![ROOT_KEY.to_vec()])
            .await?,
        "root read",
    )?;
    let Some(root_bytes) = root_bytes else {
        return Ok(Vec::new());
    };
    let root = decode_root(&root_bytes)?;
    let Some(target) = root.target else {
        return Ok(Vec::new());
    };
    let mut node_keys = vec![(target.key.clone(), Some(target))];
    let mut leaf_selectors = Vec::new();
    let mut records = Vec::new();
    while let Some((node_key, expected_selector)) = node_keys.pop() {
        let bytes = exactly_one_get(
            store
                .changelog_get_many(SEMANTIC_HISTORY_SPACE, vec![node_key.clone()])
                .await?,
            "directory read",
        )?
        .ok_or_else(|| corruption("semantic-history root references missing directory"))?;
        let node = decode_directory(&bytes)?;
        if node.digest != node_key[1..] {
            return Err(corruption("semantic-history directory digest mismatch"));
        }
        if let Some(selector) = expected_selector {
            validate_directory_selector(&selector, &node)?;
        }
        if node.level == 0 {
            leaf_selectors.extend(
                node.children
                    .into_iter()
                    .filter(|selector| start_after.is_none_or(|after| selector.last > after)),
            );
        } else {
            for selector in node.children.into_iter().rev() {
                if selector.key.first() != Some(&NODE_KEY_PREFIX) {
                    return Err(corruption(
                        "semantic-history directory child level mismatch",
                    ));
                }
                if start_after.is_none_or(|after| selector.last > after) {
                    node_keys.push((selector.key.clone(), Some(selector)));
                }
            }
        }
        if leaf_selectors.len() >= 32 || (node_keys.is_empty() && !leaf_selectors.is_empty()) {
            let values = store
                .changelog_get_many(
                    SEMANTIC_HISTORY_SPACE,
                    leaf_selectors
                        .iter()
                        .map(|selector| selector.key.clone())
                        .collect(),
                )
                .await?;
            if values.len() != leaf_selectors.len() {
                return Err(corruption(
                    "semantic-history leaf batch returned an incomplete result",
                ));
            }
            for (selector, value) in leaf_selectors.drain(..).zip(values) {
                let value = value.ok_or_else(|| {
                    corruption("semantic-history directory references missing leaf")
                })?;
                let segment = decode_segment(&value)?;
                if segment.digest != selector.digest
                    || segment.min_key != selector.first
                    || segment.max_key != selector.last
                    || segment.records.len() != selector.record_count as usize
                {
                    return Err(corruption("semantic-history leaf selector mismatch"));
                }
                records.extend(segment.records.into_iter().filter(|record| {
                    record.kind == kind
                        && start_after.is_none_or(|after| record_sort_key(record) > after)
                }));
            }
            if records.len() >= limit {
                break;
            }
        }
    }
    records.truncate(limit);
    Ok(records)
}

async fn load_commits_from_store<'a>(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitLoadRequest<'a>,
) -> Result<CommitLoadBatch<'a>, LixError> {
    let mut entries = Vec::with_capacity(request.commit_ids.len());
    for commit_id in request.commit_ids {
        let value =
            semantic_load_record(store, COMMIT_RECORD_KIND, *commit_id.as_uuid().as_bytes())
                .await?;
        let Some(value) = value else {
            entries.push(None);
            continue;
        };
        let record = storage_codec::decode("commit record", &value)?;
        entries.push(Some(record));
    }
    CommitLoadBatch::try_new("changelog commit", request.commit_ids, entries)
}

async fn scan_commits_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: CommitScanRequest<'_>,
) -> Result<CommitScanBatch, LixError> {
    let limit = request.limit.unwrap_or(SCAN_PAGE_LIMIT);
    if limit == 0 {
        return Ok(CommitScanBatch {
            entries: Vec::new(),
            next_start_after: request
                .start_after
                .map(|id| CommitId::parse_lix(id, "commit scan start_after"))
                .transpose()?,
        });
    }
    let start_after = request
        .start_after
        .map(|id| CommitId::parse_lix(id, "commit scan start_after"))
        .transpose()?;
    let start_key = start_after.map(|id| record_key(COMMIT_RECORD_KIND, *id.as_uuid().as_bytes()));
    let records = semantic_scan_records(
        store,
        COMMIT_RECORD_KIND,
        start_key,
        limit.saturating_add(1),
    )
    .await?;
    let mut entries = records
        .into_iter()
        .map(|record| storage_codec::decode("commit record", &record.value))
        .collect::<Result<Vec<CommitRecord>, _>>()?;
    let next_start_after = (entries.len() > limit).then(|| entries[limit - 1].commit_id);
    entries.truncate(limit);
    Ok(CommitScanBatch {
        entries,
        next_start_after,
    })
}

async fn load_changes_from_store<'a>(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: ChangeLoadRequest<'a>,
) -> Result<ChangeLoadBatch<'a>, LixError> {
    let mut entries = Vec::with_capacity(request.change_ids.len());
    for change_id in request.change_ids {
        let value =
            semantic_load_record(store, CHANGE_RECORD_KIND, *change_id.as_uuid().as_bytes())
                .await?;
        entries.push(
            value
                .map(|value| decode_change_record(&value, *change_id))
                .transpose()?,
        );
    }
    ChangeLoadBatch::try_new("changelog change", request.change_ids, entries)
}

async fn scan_changes_from_store(
    store: &mut (impl ChangelogStorageRead + ?Sized),
    request: ChangeScanRequest<'_>,
) -> Result<ChangeScanBatch, LixError> {
    let limit = request.limit.unwrap_or(SCAN_PAGE_LIMIT);
    if limit == 0 {
        return Ok(ChangeScanBatch {
            entries: Vec::new(),
            next_start_after: request
                .start_after
                .map(|id| ChangeId::parse_lix(id, "change scan start_after"))
                .transpose()?,
        });
    }
    let start_after = request
        .start_after
        .map(|id| ChangeId::parse_lix(id, "change scan start_after"))
        .transpose()?;
    let start_key = start_after.map(|id| record_key(CHANGE_RECORD_KIND, *id.as_uuid().as_bytes()));
    let records = semantic_scan_records(
        store,
        CHANGE_RECORD_KIND,
        start_key,
        limit.saturating_add(1),
    )
    .await?;
    let mut entries = records
        .into_iter()
        .map(|record| {
            decode_change_record(
                &record.value,
                ChangeId::new(uuid::Uuid::from_bytes(record.id)),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let next_start_after = (entries.len() > limit).then(|| entries[limit - 1].change_id);
    entries.truncate(limit);
    Ok(ChangeScanBatch {
        entries,
        next_start_after,
    })
}

fn validate_unique<T>(values: impl IntoIterator<Item = T>, label: &str) -> Result<(), LixError>
where
    T: fmt::Display,
{
    let mut seen = HashSet::new();
    for value in values {
        if !seen.insert(value.to_string()) {
            return Err(LixError::unknown(format!(
                "changelog append contains duplicate {label} '{value}'"
            )));
        }
    }
    Ok(())
}

async fn native_get_many<R>(
    read: &mut R,
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys = keys
        .into_iter()
        .map(|key| StorageKey(Bytes::from(key)))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(space, &keys)
        .materialize(read, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .map(|value| match value {
            Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
            Some(StorageProjectedValue::KeyOnly) => Some(Vec::new()),
            None => None,
        })
        .collect())
}
