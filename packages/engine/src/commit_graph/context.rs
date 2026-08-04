#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_pass_by_ref_mut,
    clippy::unused_self
)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest,
    CommitRecord, CommitScanRequest,
};
use crate::commit_graph::walker::{best_common_ancestors, walk_reachable_nodes};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphHistory, CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode,
};
use crate::common::ExactBatch;
use crate::entity_pk::EntityPk;
use crate::storage_adapter::StorageAdapterRead;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
/// Read model for resolving authoritative commit manifests into entity state
/// at a head.
///
/// The changelog commit plane is a compact serving projection. Every graph
/// read validates it against the commit-state manifest before exposing
/// topology.
#[derive(Clone)]
pub(crate) struct CommitGraphContext;

impl CommitGraphContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a graph reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommitGraphStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        CommitGraphStoreReader {
            store,
            node_cache: HashMap::new(),
            reachable_nodes_cache: HashMap::new(),
            member_changes_cache: HashMap::new(),
        }
    }
}

/// Commit-graph reader that resolves changelog entities at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    store: S,
    node_cache: HashMap<CommitId, Option<CommitGraphNode>>,
    reachable_nodes_cache: HashMap<CommitId, Arc<[ReachableCommitGraphNode]>>,
    // A reader is bound to one pinned storage snapshot for the duration of a
    // SQL statement. File-history shaping asks the same reader for distinct
    // schema slices of that history, so retain immutable change records here.
    member_changes_cache: HashMap<Vec<String>, HashMap<CommitId, Vec<CommitGraphChange>>>,
}

impl<S> CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    #[cfg(feature = "storage-benches")]
    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Loads one topology node without reading its member delta or payloads.
    pub(crate) async fn load_node(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphNode>, LixError> {
        Ok(self
            .load_nodes(std::slice::from_ref(commit_id))
            .await?
            .into_iter()
            .next()
            .and_then(|(_, value)| value))
    }

    pub(crate) async fn load_nodes<'a>(
        &mut self,
        commit_ids: &'a [CommitId],
    ) -> Result<ExactBatch<'a, CommitId, CommitGraphNode>, LixError> {
        let uncached_ids = commit_ids
            .iter()
            .filter(|commit_id| !self.node_cache.contains_key(commit_id))
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !uncached_ids.is_empty() {
            let mut reader = ChangelogContext::new().reader(&self.store);
            let batch = reader
                .load_commits(CommitLoadRequest {
                    commit_ids: &uncached_ids,
                })
                .await?;
            let manifests =
                crate::tracked_state::load_commit_state_manifests(&self.store, &uncached_ids)
                    .await?;
            for ((commit_id, record), manifest) in batch.into_iter().zip(manifests) {
                let node = commit_graph_node_from_authority(*commit_id, record, manifest)?;
                self.node_cache.insert(*commit_id, node);
            }
        }
        let nodes = commit_ids
            .iter()
            .map(|commit_id| self.node_cache.get(commit_id).cloned().unwrap_or(None))
            .collect();
        ExactBatch::try_new("commit graph", commit_ids, nodes)
    }

    /// Loads every direct commit fact from the commit-state authority.
    ///
    /// This is used by global commit surfaces where the caller wants the durable
    /// graph facts themselves, not reachability from a particular branch head.
    pub(crate) async fn all_nodes(&mut self) -> Result<Vec<CommitGraphNode>, LixError> {
        let mut commits = Vec::new();
        let mut start_after = None::<String>;
        loop {
            let mut reader = ChangelogContext::new().reader(&self.store);
            let scan = reader
                .scan_commits(CommitScanRequest {
                    start_after: start_after.as_deref(),
                    limit: Some(1024),
                })
                .await?;
            let commit_ids = scan
                .entries
                .iter()
                .map(|record| record.commit_id)
                .collect::<Vec<_>>();
            let manifests =
                crate::tracked_state::load_commit_state_manifests(&self.store, &commit_ids).await?;
            for (record, manifest) in scan.entries.into_iter().zip(manifests) {
                let commit_id = record.commit_id;
                let node = commit_graph_node_from_authority(commit_id, Some(record), manifest)?
                    .expect("scanned commit projection produces a graph node");
                self.node_cache.insert(node.commit_id, Some(node.clone()));
                commits.push(node);
            }
            let Some(next) = scan.next_start_after else {
                break;
            };
            start_after = Some(next.to_string());
        }
        Ok(commits)
    }

    /// Walks from `head_commit_id` through parent commits and records nearest depth.
    pub(crate) async fn reachable_nodes(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        if let Some(nodes) = self.reachable_nodes_cache.get(head_commit_id) {
            return Ok(Arc::clone(nodes));
        }
        let nodes = Arc::from(walk_reachable_nodes(self, head_commit_id).await?);
        self.reachable_nodes_cache
            .insert(*head_commit_id, Arc::clone(&nodes));
        Ok(nodes)
    }

    /// Returns the best common ancestors shared by two commit heads.
    ///
    /// This is the commit-DAG primitive. It can return more than one commit in
    /// criss-cross histories. Merge code should layer an explicit merge-base
    /// policy on top when it needs exactly one base for a three-way merge.
    pub(crate) async fn best_common_ancestors(
        &mut self,
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
    ) -> Result<Vec<CommitGraphNode>, LixError> {
        best_common_ancestors(self, left_commit_id, right_commit_id).await
    }

    /// Resolves the single commit base to use for a three-way merge.
    ///
    /// This is merge policy layered over `best_common_ancestors(...)`. Histories
    /// with no shared base or multiple equally good bases are rejected for now
    /// so merge code cannot accidentally hide unsupported graph semantics.
    pub(crate) async fn merge_base(
        &mut self,
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
    ) -> Result<CommitId, LixError> {
        let head_ids = [*left_commit_id, *right_commit_id];
        let heads = self.load_nodes(&head_ids).await?;
        let mut heads = heads.into_iter().map(|(_, node)| node);
        let left = heads
            .next()
            .flatten()
            .ok_or_else(|| missing_commit_graph_error(left_commit_id))?;
        let right = heads
            .next()
            .flatten()
            .ok_or_else(|| missing_commit_graph_error(right_commit_id))?;

        if left_commit_id == right_commit_id {
            return Ok(*left_commit_id);
        }
        if left.parent_commit_ids.as_slice() == [*right_commit_id] {
            return Ok(*right_commit_id);
        }
        if right.parent_commit_ids.as_slice() == [*left_commit_id] {
            return Ok(*left_commit_id);
        }
        if let ([left_parent], [right_parent]) = (
            left.parent_commit_ids.as_slice(),
            right.parent_commit_ids.as_slice(),
        ) && left_parent == right_parent
        {
            let parent_ids = [*left_parent];
            if self
                .load_nodes(&parent_ids)
                .await?
                .into_iter()
                .next()
                .and_then(|(_, value)| value)
                .is_none()
            {
                return Err(missing_commit_graph_error(left_parent));
            }
            return Ok(*left_parent);
        }

        let ancestors = self
            .best_common_ancestors(left_commit_id, right_commit_id)
            .await?;
        match ancestors.as_slice() {
            [] => Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "commit_graph found no common history between '{left_commit_id}' and '{right_commit_id}'"
                ),
            )),
            [base] => Ok(base.commit_id),
            _ => Err(LixError::ambiguous_merge_base(
                left_commit_id,
                right_commit_id,
                ancestors
                    .iter()
                    .map(|ancestor| ancestor.commit_id.to_string())
                    .collect(),
            )),
        }
    }

    /// Returns canonical changes reachable from `start_commit_id`.
    ///
    /// This is the primitive history API. It reports the commit/depth where a
    /// reachable commit's change-ref set first exposes each matching canonical
    /// change during graph traversal and leaves row shaping to callers such as
    /// SQL providers.
    pub(crate) async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError> {
        let nodes = self.reachable_nodes(start_commit_id).await?;
        let member_schema_keys = request
            .schema_keys
            .iter()
            .filter(|schema_key| schema_key.as_str() != COMMIT_SCHEMA_KEY)
            .cloned()
            .collect::<Vec<_>>();
        let mut member_schema_keys = member_schema_keys;
        member_schema_keys.sort();
        member_schema_keys.dedup();
        let may_include_members = request.schema_keys.is_empty() || !member_schema_keys.is_empty();
        let may_include_commits = request.schema_keys.is_empty()
            || request
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == COMMIT_SCHEMA_KEY);
        let mut entries = Vec::new();
        let mut seen_changes = BTreeSet::new();

        for reachable in nodes.iter() {
            if !depth_matches(reachable.depth, request) {
                continue;
            }

            let node = &reachable.commit;
            if may_include_commits {
                let canonical_change = canonical_commit_change(node);
                if seen_changes.insert(history_change_identity(&canonical_change))
                    && change_matches_history_request(&canonical_change, request)
                {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change: canonical_change,
                        observed_commit_id: node.commit_id,
                        start_commit_id: *start_commit_id,
                        depth: reachable.depth,
                    });
                }
            }

            if !may_include_members {
                continue;
            }
            for change in self
                .load_member_changes(node.commit_id, &member_schema_keys)
                .await?
            {
                if !seen_changes.insert(history_change_identity(&change)) {
                    continue;
                }
                if change_matches_history_request(&change, request) {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change,
                        observed_commit_id: node.commit_id,
                        start_commit_id: *start_commit_id,
                        depth: reachable.depth,
                    });
                }
            }
        }

        Ok(CommitGraphHistory {
            entries,
            reachable_nodes: nodes,
        })
    }

    async fn load_member_changes(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<Vec<CommitGraphChange>, LixError> {
        if let Some(changes) = self
            .member_changes_cache
            .get(schema_keys)
            .and_then(|by_commit| by_commit.get(&commit_id))
        {
            return Ok(changes.clone());
        }
        let members = crate::tracked_state::load_commit_delta_members_with_payloads_for_schemas(
            &self.store,
            commit_id,
            schema_keys,
            usize::MAX,
        )
        .await?
        .expect("unbounded commit member load cannot exceed its segment limit");
        let mut changes = members
            .into_iter()
            .map(|member| commit_graph_change_from_change_record(member.change))
            .collect::<Vec<_>>();
        changes.sort_by_key(|change| change.id);
        self.member_changes_cache
            .entry(schema_keys.to_vec())
            .or_default()
            .insert(commit_id, changes.clone());
        Ok(changes)
    }
}

fn commit_graph_change_from_change_record(change: ChangeRecord) -> CommitGraphChange {
    CommitGraphChange {
        id: change.change_id,
        account_id: change.account_id,
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot: change.snapshot,
        metadata: change.metadata,
        created_at: change.created_at,
        origin_key: change.origin_key,
    }
}

fn commit_graph_node_from_authority(
    commit_id: CommitId,
    record: Option<CommitRecord>,
    manifest: Option<crate::tracked_state::CommitStateManifest>,
) -> Result<Option<CommitGraphNode>, LixError> {
    // Public graph membership belongs to the compact changelog projection.
    // GC may retain an immutable manifest after removing that projection so
    // selected payloads remain addressable for recovery.
    let Some(record) = record else {
        return Ok(None);
    };
    let Some(manifest) = manifest else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit_graph projection for commit '{commit_id}' has no commit-state authority"
            ),
        ));
    };
    if record.commit_id != manifest.commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit_graph projection disagrees with commit-state authority for commit '{commit_id}'"
            ),
        ));
    }
    Ok(Some(CommitGraphNode {
        commit_id: record.commit_id,
        change_id: record.change_id,
        account_id: record.account_id,
        generation: record.generation,
        parent_commit_ids: record.parent_commit_ids,
        created_at: record.created_at,
    }))
}

fn missing_commit_graph_error(commit_id: &CommitId) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("commit_graph missing commit '{commit_id}'"),
    )
}

#[async_trait::async_trait]
impl<S> CommitGraphReader for CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn load_node(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphNode>, LixError> {
        Self::load_node(self, commit_id).await
    }

    async fn reachable_nodes(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        Self::reachable_nodes(self, head_commit_id).await
    }

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError> {
        Self::change_history_from_commit(self, start_commit_id, request).await
    }
}

fn depth_matches(depth: u32, request: &CommitGraphChangeHistoryRequest) -> bool {
    request.min_depth.is_none_or(|min| depth >= min)
        && request.max_depth.is_none_or(|max| depth <= max)
}

fn change_matches_history_request(
    change: &CommitGraphChange,
    request: &CommitGraphChangeHistoryRequest,
) -> bool {
    (request.include_tombstones || change.snapshot.is_some())
        && (request.entity_pks.is_empty() || request.entity_pks.contains(&change.entity_pk))
        && (request.schema_keys.is_empty() || request.schema_keys.contains(&change.schema_key))
        && (request.file_ids.is_empty()
            || change
                .file_id
                .as_ref()
                .is_some_and(|file_id| request.file_ids.contains(file_id)))
}

fn history_change_identity(
    change: &CommitGraphChange,
) -> (ChangeId, String, Option<String>, EntityPk) {
    (
        change.id,
        change.schema_key.clone(),
        change.file_id.clone(),
        change.entity_pk.clone(),
    )
}

pub(crate) fn canonical_commit_change(node: &CommitGraphNode) -> CommitGraphChange {
    let snapshot_content = crate::changelog::commit_row_snapshot_json(&node.commit_id.to_string())
        .expect("lix_commit snapshot serialization should not fail");
    CommitGraphChange {
        id: node.change_id,
        account_id: node.account_id.clone(),
        entity_pk: EntityPk::uuid_from_canonical(&node.commit_id.to_string())
            .expect("commit IDs are canonical UUIDs"),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: node.created_at,
        origin_key: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitId,
        CommitRecord,
    };
    use crate::commit_graph::{
        CommitGraphChange, CommitGraphChangeHistoryRequest, CommitGraphContext,
    };
    use crate::storage::{
        GetManyResult, KeyRange, ScanChunk, ScanOptions, StorageError, StorageRead,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, Storage, StorageAdapter, StorageAdapterReadScope, StorageKey,
        StorageReadOptions, StorageWriteOptions,
    };
    use crate::tracked_state::{
        CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
        TrackedStateCommitDeltaRef, TrackedStateDeltaRef, stage_commit_deltas_for_commit_state,
        stage_commit_state_manifest,
    };

    #[derive(Clone)]
    struct CountingMemoryRead {
        inner: MemoryRead,
        change_get_many_calls: Arc<AtomicUsize>,
        member_segment_get_many_calls: Arc<AtomicUsize>,
    }

    impl StorageRead for CountingMemoryRead {
        async fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            if requests
                .iter()
                .any(|request| request.space == crate::changelog::CHANGE_SPACE)
            {
                self.change_get_many_calls.fetch_add(1, Ordering::Relaxed);
            }
            if requests.iter().any(|request| {
                request.space == crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE
            }) {
                self.member_segment_get_many_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: crate::storage::StorageSpace,
            range: KeyRange,
            opts: ScanOptions,
        ) -> Result<ScanChunk, StorageError> {
            self.inner.scan(space, range, opts).await
        }
    }

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn commit_id(label: &str) -> CommitId {
        CommitId::for_test_label(label)
    }

    fn change_id(label: &str) -> ChangeId {
        ChangeId::for_test_label(label)
    }

    fn commit_ids<const N: usize>(labels: [&str; N]) -> Vec<CommitId> {
        labels.into_iter().map(commit_id).collect()
    }

    fn sorted_commit_ids<const N: usize>(labels: [&str; N]) -> Vec<CommitId> {
        let mut ids = commit_ids(labels);
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn load_node_returns_topology_without_member_payloads() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                entity_change("change-1", "entity-1", "example", "{}"),
                entity_change("change-2", "entity-2", "example", "{}"),
                commit_change(
                    "commit-1-change",
                    "commit-1",
                    &["change-1", "change-2"],
                    &["parent-1"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_1 = commit_id("commit-1");
        let commit = reader
            .load_node(&commit_1)
            .await
            .expect("commit load should succeed")
            .expect("commit should exist");

        assert_eq!(commit.commit_id, commit_id("commit-1"));
        assert_eq!(commit.parent_commit_ids, commit_ids(["parent-1"]));
        assert_eq!(commit.change_id, change_id("commit-1-change"));
    }

    #[tokio::test]
    async fn load_commit_returns_none_for_missing_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let missing = commit_id("missing");

        let commit = reader
            .load_node(&missing)
            .await
            .expect("commit load should succeed");

        assert_eq!(commit, None);
    }

    #[tokio::test]
    async fn retained_manifest_without_projection_is_not_a_public_graph_node() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = commit_id("retained-payload-authority");
        let mut writes = storage.new_write_set();
        stage_commit_state_manifest(
            &mut writes,
            &CommitStateManifest {
                commit_id,
                change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                replay_debt: CommitStateReplayDebt {
                    depth: 1,
                    rows: 0,
                    bytes: 0,
                },
                mutations: CommitStateMutationInventory::default(),
                touched_scope_filter: Default::default(),
                current_state_scoped_ranges: None,
            },
        )
        .expect("retained payload authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("retained payload authority should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        assert!(
            reader
                .load_node(&commit_id)
                .await
                .expect("retained authority lookup should succeed")
                .is_none()
        );
        assert!(
            reader
                .all_nodes()
                .await
                .expect("retained authority scan should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn load_node_rejects_projection_without_commit_state_authority() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[commit_change(
                "missing-authority-change",
                "missing-authority",
                &[],
                &[],
            )],
        )
        .await;
        let commit_id = commit_id("missing-authority");
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            StorageKey(bytes::Bytes::copy_from_slice(
                commit_id.as_uuid().as_bytes(),
            )),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("manifest deletion should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = CommitGraphContext::new()
            .reader(read)
            .load_node(&commit_id)
            .await
            .expect_err("a changelog projection cannot replace missing authority");
        assert!(error.message.contains("has no commit-state authority"));
    }

    #[tokio::test]
    async fn all_nodes_returns_parsed_commits_sorted_by_id() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-b-change", "commit-b", &[], &[]),
                entity_change("change-1", "entity-1", "example", "{}"),
                commit_change("commit-a-change", "commit-a", &[], &[]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commits = reader
            .all_nodes()
            .await
            .expect("commit scan should succeed");

        assert_eq!(
            commits
                .iter()
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            sorted_commit_ids(["commit-a", "commit-b"])
        );
    }

    #[test]
    fn commit_edges_are_derived_from_parent_commit_ids() {
        let commits = vec![parsed_commit(
            "commit-head",
            &[],
            &["commit-left", "commit-right"],
        )];

        let edges = crate::commit_graph::commit_edges(&commits);

        assert_eq!(
            edges
                .iter()
                .map(|edge| (
                    edge.parent_commit_id.clone(),
                    edge.child_commit_id.clone(),
                    edge.parent_order,
                ))
                .collect::<Vec<_>>(),
            vec![
                (commit_id("commit-left"), commit_id("commit-head"), 0),
                (commit_id("commit-right"), commit_id("commit-head"), 1)
            ]
        );
    }

    #[tokio::test]
    async fn change_history_from_commit_reports_matching_canonical_changes_with_depth() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                entity_change("change-root", "entity-root", "test_schema", "{}"),
                entity_change("change-head", "entity-head", "test_schema", "{}"),
                commit_change("commit-root-change", "commit-root", &["change-root"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-head"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let history = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["test_schema".to_string()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");

        assert_eq!(
            history
                .entries
                .iter()
                .map(|entry| (
                    entry.change.id.clone(),
                    entry.observed_commit_id.clone(),
                    entry.start_commit_id.clone(),
                    entry.depth
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    change_id("change-head"),
                    commit_id("commit-head"),
                    commit_id("commit-head"),
                    0
                ),
                (
                    change_id("change-root"),
                    commit_id("commit-root"),
                    commit_id("commit-head"),
                    1
                ),
            ]
        );
    }

    #[tokio::test]
    async fn change_history_reuses_canonical_changes_across_requests() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        append_changes(
            &storage,
            &[
                entity_change("change-root", "entity-root", "test_schema", "{}"),
                entity_change("change-head", "entity-head", "test_schema", "{}"),
                commit_change("commit-root-change", "commit-root", &["change-root"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-head"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let change_get_many_calls = Arc::new(AtomicUsize::new(0));
        let member_segment_get_many_calls = Arc::new(AtomicUsize::new(0));
        let read = memory
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(StorageAdapterReadScope::new(CountingMemoryRead {
            inner: read,
            change_get_many_calls: Arc::clone(&change_get_many_calls),
            member_segment_get_many_calls,
        }));
        let request = CommitGraphChangeHistoryRequest {
            schema_keys: vec!["test_schema".to_string()],
            include_tombstones: true,
            ..CommitGraphChangeHistoryRequest::default()
        };
        let commit_head = commit_id("commit-head");

        let first = reader
            .change_history_from_commit(&commit_head, &request)
            .await
            .expect("first history should resolve");
        let calls_after_first = change_get_many_calls.load(Ordering::Relaxed);
        assert_eq!(
            calls_after_first, 0,
            "packed commit members retain their payloads without global change reads"
        );

        let second = reader
            .change_history_from_commit(&commit_head, &request)
            .await
            .expect("second history should resolve");
        assert_eq!(second, first);
        assert!(Arc::ptr_eq(&first.reachable_nodes, &second.reachable_nodes));
        assert_eq!(
            change_get_many_calls.load(Ordering::Relaxed),
            calls_after_first,
            "a pinned reader should reuse previously loaded canonical changes",
        );
    }

    #[tokio::test]
    async fn schema_sliced_history_caches_full_selected_tombstone_identity() {
        let storage = StorageAdapter::new(Memory::new());
        let commit_id = commit_id("selected-tombstone-cache");
        let shared_change_id = change_id("shared-selected-tombstone");
        let alpha_pk = crate::entity_pk::EntityPk::single("alpha-entity");
        let alpha_second_pk = crate::entity_pk::EntityPk::single("alpha-second-entity");
        let beta_pk = crate::entity_pk::EntityPk::single("beta-entity");
        let created_at = ts("2026-01-02T00:00:00Z");
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(ChangelogAppend {
                changes: Vec::new(),
                commits: vec![CommitRecord {
                    format_version: 2,
                    commit_id,
                    generation: 0,
                    parent_commit_ids: Vec::new(),
                    change_id: change_id("selected-tombstone-commit-change"),
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    created_at,
                }],
            })
            .await
            .expect("commit should stage");
        let deltas = [
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "alpha",
                    file_id: None,
                    entity_pk: &alpha_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: false,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "beta",
                    file_id: None,
                    entity_pk: &beta_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: false,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "alpha",
                    file_id: None,
                    entity_pk: &alpha_second_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                snapshot: crate::json_store::JsonSlotRef::None,
                metadata: crate::json_store::JsonSlotRef::None,
                origin_key: None,
                base_coordinate: None,
                authored: false,
            },
        ];
        let staged = stage_commit_deltas_for_commit_state(&mut writes, &deltas)
            .expect("selected tombstones should stage");
        stage_commit_state_manifest(
            &mut writes,
            &CommitStateManifest {
                commit_id,
                change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                replay_debt: CommitStateReplayDebt {
                    depth: 1,
                    rows: 3,
                    bytes: 0,
                },
                mutations: staged.mutation_inventory().clone(),
                touched_scope_filter: Default::default(),
                current_state_scoped_ranges: None,
            },
        )
        .expect("selected tombstone commit-state manifest should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("pinned read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let schema_history = |schema_key: &str| CommitGraphChangeHistoryRequest {
            schema_keys: vec![schema_key.to_string()],
            include_tombstones: true,
            ..CommitGraphChangeHistoryRequest::default()
        };
        let alpha_first = reader
            .change_history_from_commit(&commit_id, &schema_history("alpha"))
            .await
            .expect("alpha history should load");
        let beta = reader
            .change_history_from_commit(&commit_id, &schema_history("beta"))
            .await
            .expect("beta history should load");
        let alpha_second = reader
            .change_history_from_commit(&commit_id, &schema_history("alpha"))
            .await
            .expect("cached alpha history should load");

        assert_eq!(alpha_first.entries.len(), 2);
        assert!(
            alpha_first
                .entries
                .iter()
                .all(|entry| entry.change.schema_key == "alpha")
        );
        assert!(
            alpha_first
                .entries
                .iter()
                .any(|entry| entry.change.entity_pk == alpha_pk)
        );
        assert!(
            alpha_first
                .entries
                .iter()
                .any(|entry| entry.change.entity_pk == alpha_second_pk)
        );
        assert_eq!(beta.entries.len(), 1);
        assert_eq!(beta.entries[0].change.schema_key, "beta");
        assert_eq!(beta.entries[0].change.entity_pk, beta_pk);
        assert_eq!(alpha_second.entries, alpha_first.entries);
        assert!(Arc::ptr_eq(
            &alpha_first.reachable_nodes,
            &alpha_second.reachable_nodes
        ));
        let entity_history = reader
            .change_history_from_commit(
                &commit_id,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["alpha".to_string()],
                    entity_pks: vec![alpha_second_pk.clone()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("identity-filtered selected tombstone should load");
        assert_eq!(entity_history.entries.len(), 1);
        assert_eq!(entity_history.entries[0].change.entity_pk, alpha_second_pk);
    }

    #[tokio::test]
    async fn topology_reads_do_not_load_commit_member_payloads() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        append_changes(
            &storage,
            &[
                entity_change("change-root", "entity-root", "test_schema", "{}"),
                entity_change("change-head", "entity-head", "test_schema", "{}"),
                commit_change("commit-root-change", "commit-root", &["change-root"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-head"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let member_segment_get_many_calls = Arc::new(AtomicUsize::new(0));
        let read = memory
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader =
            CommitGraphContext::new().reader(StorageAdapterReadScope::new(CountingMemoryRead {
                inner: read,
                change_get_many_calls: Arc::new(AtomicUsize::new(0)),
                member_segment_get_many_calls: Arc::clone(&member_segment_get_many_calls),
            }));
        let head = commit_id("commit-head");
        let root = commit_id("commit-root");

        reader
            .load_node(&head)
            .await
            .expect("node load should succeed");
        reader
            .reachable_nodes(&head)
            .await
            .expect("topology walk should succeed");
        reader
            .best_common_ancestors(&head, &root)
            .await
            .expect("ancestor walk should succeed");
        reader.all_nodes().await.expect("node scan should succeed");
        assert_eq!(
            member_segment_get_many_calls.load(Ordering::Relaxed),
            0,
            "topology APIs must never touch commit member storage",
        );

        let commit_history = reader
            .change_history_from_commit(
                &head,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec![super::COMMIT_SCHEMA_KEY.to_string()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("commit-only history should derive node changes");
        assert_eq!(commit_history.entries.len(), 2);
        assert_eq!(
            member_segment_get_many_calls.load(Ordering::Relaxed),
            0,
            "commit-only history must not hydrate unrelated member payloads",
        );

        let history = reader
            .change_history_from_commit(
                &head,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["test_schema".to_string()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should hydrate requested payloads");
        assert_eq!(history.entries.len(), 2);
        assert_eq!(
            member_segment_get_many_calls.load(Ordering::Relaxed),
            0,
            "these tiny commit inventories are inline in their authority manifests",
        );
    }

    #[tokio::test]
    async fn change_history_from_commit_filters_depth_entity_file_and_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                entity_change_with_file(
                    "change-01920000-0000-7000-8000-0000000000a2",
                    "entity-1",
                    "test_schema",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    "{}",
                ),
                entity_tombstone("change-tombstone", "entity-1", "test_schema"),
                entity_change_with_file(
                    "change-01920000-0000-7000-8000-0000000000b2",
                    "entity-2",
                    "test_schema",
                    Some("01920000-0000-7000-8000-0000000000b2"),
                    "{}",
                ),
                commit_change(
                    "commit-root-change",
                    "commit-root",
                    &["change-01920000-0000-7000-8000-0000000000a2"],
                    &[],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &[
                        "change-tombstone",
                        "change-01920000-0000-7000-8000-0000000000b2",
                    ],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let history = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    entity_pks: vec![crate::entity_pk::EntityPk::single("entity-1")],
                    file_ids: vec!["01920000-0000-7000-8000-0000000000a2".to_string()],
                    min_depth: Some(1),
                    max_depth: Some(1),
                    include_tombstones: false,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");

        assert_eq!(history.entries.len(), 1);
        assert_eq!(
            history.entries[0].change.id,
            change_id("change-01920000-0000-7000-8000-0000000000a2")
        );
        assert_eq!(history.entries[0].depth, 1);
    }

    #[tokio::test]
    async fn change_history_from_commit_includes_tombstones_when_requested() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                entity_tombstone("change-deleted", "entity-1", "test_schema"),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-deleted"],
                    &[],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let hidden = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["test_schema".to_string()],
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");
        let visible = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["test_schema".to_string()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");

        assert!(hidden.entries.is_empty());
        assert_eq!(visible.entries.len(), 1);
        assert_eq!(visible.entries[0].change.id, change_id("change-deleted"));
    }

    #[derive(Clone)]
    struct TestChange {
        change: CommitGraphChange,
        commit_change_ids: Vec<ChangeId>,
        parent_commit_ids: Vec<CommitId>,
    }

    impl TestChange {
        fn commit(
            change_id: &str,
            commit_id: &str,
            change_ids: &[&str],
            parent_commit_ids: &[&str],
        ) -> Self {
            Self {
                change: CommitGraphChange {
                    id: ChangeId::for_test_label(change_id),
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single(commit_id),
                    schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot: crate::json_store::JsonSlot::None,
                    metadata: crate::json_store::JsonSlot::None,
                    created_at: ts("2026-01-01T00:00:00Z"),
                    origin_key: None,
                },
                commit_change_ids: change_ids
                    .iter()
                    .map(|id| ChangeId::for_test_label(id))
                    .collect(),
                parent_commit_ids: parent_commit_ids
                    .iter()
                    .map(|id| CommitId::for_test_label(id))
                    .collect(),
            }
        }

        fn entity(
            change_id: &str,
            entity_pk: &str,
            schema_key: &str,
            file_id: Option<&str>,
            snapshot_content: Option<&str>,
            created_at: &str,
        ) -> Self {
            Self {
                change: CommitGraphChange {
                    id: ChangeId::for_test_label(change_id),
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                    snapshot: snapshot_content
                        .map_or(crate::json_store::JsonSlot::None, |content| {
                            crate::json_store::JsonSlot::from_json(content)
                        }),
                    metadata: crate::json_store::JsonSlot::None,
                    created_at: ts(created_at),
                    origin_key: None,
                },
                commit_change_ids: Vec::new(),
                parent_commit_ids: Vec::new(),
            }
        }

        fn is_commit(&self) -> bool {
            self.change.schema_key == super::COMMIT_SCHEMA_KEY
        }
    }

    async fn append_changes(storage: &StorageAdapter, changes: &[TestChange]) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let canonical_changes = changes
            .iter()
            .filter(|change| !change.is_commit())
            .cloned()
            .collect::<Vec<_>>();
        let changes_by_id: BTreeMap<ChangeId, &TestChange> = canonical_changes
            .iter()
            .map(|change| (change.change.id, change))
            .collect::<BTreeMap<_, _>>();
        let provided_commit_ids = changes
            .iter()
            .filter(|change| change.is_commit())
            .map(|change| {
                CommitId::for_test_label(
                    change
                        .change
                        .entity_pk
                        .as_single_string()
                        .expect("commit fixture should use single entity pk"),
                )
            })
            .collect::<BTreeSet<_>>();
        let mut staged_commit_ids = BTreeSet::new();
        let changelog = ChangelogContext::new();
        let mut writer = changelog.writer(&mut read, &mut writes);
        let mut append = ChangelogAppend::default();
        let mut commit_members = Vec::<(CommitId, Vec<ChangeRecord>)>::new();
        let mut generations = BTreeMap::<CommitId, u64>::new();
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit_label = change
                .change
                .entity_pk
                .as_single_string()
                .expect("commit fixture should use single entity pk")
                .to_string();
            let commit_id = CommitId::for_test_label(&commit_label);
            for parent_commit_id in &change.parent_commit_ids {
                if !provided_commit_ids.contains(parent_commit_id)
                    && staged_commit_ids.insert(*parent_commit_id)
                {
                    append_empty_commit(&mut append, *parent_commit_id);
                    generations.insert(*parent_commit_id, 0);
                }
            }
            let generation = change
                .parent_commit_ids
                .iter()
                .filter_map(|parent| generations.get(parent).copied())
                .max()
                .map_or(0, |parent_generation| parent_generation + 1);
            let mut members = Vec::new();
            for change_id in &change.commit_change_ids {
                if let Some(change) = changes_by_id.get(change_id) {
                    members.push(change_record_from_test_change(change));
                }
            }

            append.commits.push(CommitRecord {
                format_version: 2,
                commit_id,
                generation,
                parent_commit_ids: change.parent_commit_ids.clone(),
                change_id: change.change.id,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: change.change.created_at,
            });
            commit_members.push((commit_id, members));
            staged_commit_ids.insert(commit_id);
            generations.insert(commit_id, generation);
        }
        let commit_records = append.commits.clone();
        writer
            .stage_append(append)
            .await
            .expect("changelog append should stage");
        drop(writer);
        let mut inventories = BTreeMap::new();
        for (commit_id, members) in &commit_members {
            let deltas = members
                .iter()
                .map(|change| TrackedStateCommitDeltaRef {
                    delta: TrackedStateDeltaRef {
                        schema_key: &change.schema_key,
                        file_id: change.file_id.as_deref(),
                        entity_pk: &change.entity_pk,
                        change_id: change.change_id,
                        commit_id: *commit_id,
                        deleted: change.snapshot.is_none(),
                        created_at: change.created_at,
                        updated_at: change.created_at,
                    },
                    snapshot: change.snapshot.as_ref_slot(),
                    metadata: change.metadata.as_ref_slot(),
                    origin_key: change.origin_key.as_deref(),
                    base_coordinate: None,
                    authored: true,
                })
                .collect::<Vec<_>>();
            let staged = stage_commit_deltas_for_commit_state(&mut writes, &deltas)
                .expect("packed commit members should stage");
            inventories.insert(*commit_id, staged.mutation_inventory().clone());
        }
        for record in &commit_records {
            stage_test_commit_manifest(
                &mut writes,
                record,
                inventories.remove(&record.commit_id).unwrap_or_default(),
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit should succeed");
    }

    fn stage_test_commit_manifest(
        writes: &mut crate::storage_adapter::StorageWriteSet,
        record: &CommitRecord,
        mutations: CommitStateMutationInventory,
    ) {
        stage_commit_state_manifest(
            writes,
            &CommitStateManifest {
                commit_id: record.commit_id,
                change_account_id: record.account_id.clone(),
                replay_debt: CommitStateReplayDebt {
                    depth: u16::try_from(record.generation + 1)
                        .expect("test generation should fit replay depth"),
                    rows: u64::from(mutations.member_count),
                    bytes: 0,
                },
                mutations,
                touched_scope_filter: Default::default(),
                current_state_scoped_ranges: None,
            },
        )
        .expect("test commit-state manifest should stage");
    }

    fn append_empty_commit(append: &mut ChangelogAppend, commit_id: CommitId) {
        let change_id = format!("{commit_id}-change");
        append.commits.push(CommitRecord {
            format_version: 2,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            change_id: ChangeId::for_test_label(&change_id),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: ts("2026-01-01T00:00:00Z"),
        });
    }

    fn change_record_from_test_change(change: &TestChange) -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: change.change.id,
            account_id: change.change.account_id.clone(),
            entity_pk: change.change.entity_pk.clone(),
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            snapshot: change.change.snapshot.clone(),
            metadata: change.change.metadata.clone(),
            created_at: change.change.created_at,
            origin_key: change.change.origin_key.clone(),
        }
    }

    fn commit_change(
        change_id: &str,
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> TestChange {
        TestChange::commit(change_id, commit_id, change_ids, parent_commit_ids)
    }

    fn parsed_commit(
        commit_label: &str,
        _change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> crate::commit_graph::CommitGraphNode {
        let commit_id = CommitId::for_test_label(commit_label);
        crate::commit_graph::CommitGraphNode {
            commit_id,
            change_id: ChangeId::for_test_label(&format!("{commit_label}-change")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            generation: 0,
            parent_commit_ids: parent_commit_ids
                .iter()
                .map(|parent_id| CommitId::for_test_label(parent_id))
                .collect(),
            created_at: ts("2026-01-01T00:00:00Z"),
        }
    }

    fn entity_change(
        change_id: &str,
        entity_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> TestChange {
        entity_change_at(
            change_id,
            entity_pk,
            schema_key,
            snapshot_content,
            "2026-01-01T00:00:00Z",
        )
    }

    fn entity_change_at(
        change_id: &str,
        entity_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
        created_at: &str,
    ) -> TestChange {
        TestChange::entity(
            change_id,
            entity_pk,
            schema_key,
            None,
            Some(snapshot_content),
            created_at,
        )
    }

    fn entity_change_with_file(
        change_id: &str,
        entity_pk: &str,
        schema_key: &str,
        file_id: Option<&str>,
        snapshot_content: &str,
    ) -> TestChange {
        TestChange::entity(
            change_id,
            entity_pk,
            schema_key,
            file_id,
            Some(snapshot_content),
            "2026-01-01T00:00:00Z",
        )
    }

    fn entity_tombstone(change_id: &str, entity_pk: &str, schema_key: &str) -> TestChange {
        TestChange::entity(
            change_id,
            entity_pk,
            schema_key,
            None,
            None,
            "2026-01-02T00:00:00Z",
        )
    }
}
