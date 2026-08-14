#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_pass_by_ref_mut,
    clippy::unused_self
)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::LixError;
use crate::changelog::{ChangeId, ChangeRecord, CommitId, CommitRecord};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphHistory, CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode,
};
use crate::common::ExactBatch;
use crate::row_pk::RowPk;
use crate::forktree::{
    SELECTOR_SPACE, SnapshotSelectorV1, SnapshotTargetV1, load_object_bytes, snapshot_selector_key,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StorageKeyRange,
    StorageProjectedValue, StorageScanOrder,
};

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
impl<S> CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// Creates a graph reader over the caller-owned read capability.
    pub(crate) fn new(store: S) -> Self {
        CommitGraphStoreReader {
            topology: crate::forktree::CommitTopologyReader::new(store),
            node_cache: HashMap::new(),
            reachable_nodes_cache: HashMap::new(),
            member_changes_cache: HashMap::new(),
        }
    }
}

/// Commit-graph reader that resolves changelog rows at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    topology: crate::forktree::CommitTopologyReader<S>,
    node_cache: HashMap<CommitId, CommitGraphNode>,
    reachable_nodes_cache: HashMap<CommitId, Arc<[ReachableCommitGraphNode]>>,
    // A reader is bound to one pinned storage snapshot for the duration of a
    // SQL statement. File-history shaping asks the same reader for distinct
    // schema slices of that history, so retain immutable change records here.
    member_changes_cache: HashMap<Vec<String>, HashMap<CommitId, Vec<CommitGraphChange>>>,
}

enum LinearMergeBase {
    Resolved(CommitId),
    Disconnected,
    GeneralGraph,
}

impl<S> CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
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
            let loaded = self.topology.load(&uncached_ids).await?;
            for topology in loaded.cache_seeded {
                let node = commit_graph_node_from_topology(topology);
                self.node_cache.insert(node.commit_id, node);
            }
            let batch = ExactBatch::try_new(
                "ForkTree commit graph",
                &uncached_ids,
                loaded
                    .requested
                    .into_iter()
                    .map(|topology| topology.map(commit_graph_node_from_topology))
                    .collect(),
            )?;
            for (commit_id, topology) in batch {
                if let Some(topology) = topology {
                    self.node_cache.insert(*commit_id, topology);
                }
            }
        }
        let nodes = commit_ids
            .iter()
            .map(|commit_id| self.node_cache.get(commit_id).cloned())
            .collect();
        ExactBatch::try_new("commit graph", commit_ids, nodes)
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

    /// Reads checkpoint/recovery/undo/redo/tombstone roots from the
    /// authenticated selector space in this reader's one retained view.
    pub(crate) async fn snapshot_roots(&mut self) -> Result<Vec<(String, CommitId)>, LixError> {
        let read = self.topology.read();
        let mut cursor = read
            .begin_scan(
                SELECTOR_SPACE,
                StorageKeyRange {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Unbounded,
                },
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    order: StorageScanOrder::Ascending,
                },
            )
            .await?;
        let mut roots = Vec::new();
        loop {
            let page = cursor.next_page(256).await?;
            for entry in &page.entries {
                let key = entry.key.0.as_ref();
                let is_snapshot = key.starts_with(b"checkpoint/")
                    || key.starts_with(b"recovery/")
                    || key.starts_with(b"undo/")
                    || key.starts_with(b"redo/")
                    || key.starts_with(b"branch-tombstone/");
                if !is_snapshot {
                    continue;
                }
                let bytes = match &entry.value {
                    StorageProjectedValue::FullValue(bytes) => bytes,
                    StorageProjectedValue::KeyOnly => {
                        return Err(LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "ForkTree snapshot selector scan returned key-only data",
                        ));
                    }
                };
                let selector = SnapshotSelectorV1::decode(bytes)?;
                if key != snapshot_selector_key(selector.role, selector.selector_id).as_ref() {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "ForkTree snapshot selector key/identity mismatch",
                    ));
                }
                let target_bytes = load_object_bytes(read, selector.target_object_id).await?;
                let target = SnapshotTargetV1::decode(selector.target_object_id, &target_bytes)?;
                if target.role != selector.role || target.selector_id != selector.selector_id {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "ForkTree snapshot selector/target identity mismatch",
                    ));
                }
                let commit_bytes =
                    load_object_bytes(read, target.semantic_commit_object_id).await?;
                let commit = crate::forktree::CommitObjectV1::decode(
                    target.semantic_commit_object_id,
                    &commit_bytes,
                )?;
                let commit_id = CommitId::new(uuid::Uuid::from_bytes(*commit.commit_id.as_bytes()));
                let branch_id = uuid::Uuid::from_bytes(*target.branch_id.as_bytes()).to_string();
                roots.push((branch_id, commit_id));
            }
            if !page.has_more {
                break;
            }
            if page.entries.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "ForkTree snapshot selector scan made no progress",
                ));
            }
        }
        Ok(roots)
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
            validate_parent_generation(&left, &right)?;
            return Ok(*right_commit_id);
        }
        if right.parent_commit_ids.as_slice() == [*left_commit_id] {
            validate_parent_generation(&right, &left)?;
            return Ok(*left_commit_id);
        }
        if let ([left_parent], [right_parent]) = (
            left.parent_commit_ids.as_slice(),
            right.parent_commit_ids.as_slice(),
        ) && left_parent == right_parent
        {
            let parent_ids = [*left_parent];
            let parent = self
                .load_nodes(&parent_ids)
                .await?
                .into_iter()
                .next()
                .and_then(|(_, value)| value)
                .ok_or_else(|| missing_commit_graph_error(left_parent))?;
            validate_parent_generation(&left, &parent)?;
            validate_parent_generation(&right, &parent)?;
            return Ok(*left_parent);
        }

        match self.linear_merge_base(left, right).await? {
            LinearMergeBase::Resolved(base) => return Ok(base),
            LinearMergeBase::Disconnected => {
                return Err(no_common_history_error(left_commit_id, right_commit_id));
            }
            LinearMergeBase::GeneralGraph => {}
        }

        let ancestors = self
            .best_common_ancestors(left_commit_id, right_commit_id)
            .await?;
        match ancestors.as_slice() {
            [] => Err(no_common_history_error(left_commit_id, right_commit_id)),
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

    /// Uses authoritative generation and parent facts to zip two linear
    /// frontiers without allocating the general DAG walk's ordered sets. Two
    /// same-generation parents are loaded together so remote and LSM-backed
    /// adapters receive one point-read batch per frontier step. Encountering a
    /// merge commit returns to the general algorithm with every observed node
    /// retained in this reader's immutable node cache.
    async fn linear_merge_base(
        &mut self,
        mut left: CommitGraphNode,
        mut right: CommitGraphNode,
    ) -> Result<LinearMergeBase, LixError> {
        loop {
            if left.commit_id == right.commit_id {
                return Ok(LinearMergeBase::Resolved(left.commit_id));
            }
            match left.generation.cmp(&right.generation) {
                Ordering::Greater => {
                    let [parent_id] = left.parent_commit_ids.as_slice() else {
                        return Ok(LinearMergeBase::GeneralGraph);
                    };
                    left = self.load_linear_parent(&left, *parent_id).await?;
                }
                Ordering::Less => {
                    let [parent_id] = right.parent_commit_ids.as_slice() else {
                        return Ok(LinearMergeBase::GeneralGraph);
                    };
                    right = self.load_linear_parent(&right, *parent_id).await?;
                }
                Ordering::Equal => match (
                    left.parent_commit_ids.as_slice(),
                    right.parent_commit_ids.as_slice(),
                ) {
                    ([], []) => return Ok(LinearMergeBase::Disconnected),
                    ([left_parent_id], [right_parent_id]) => {
                        let parent_ids = [*left_parent_id, *right_parent_id];
                        let parents = self.load_nodes(&parent_ids).await?;
                        let mut parents = parents.into_iter().map(|(_, parent)| parent);
                        let left_parent = parents
                            .next()
                            .flatten()
                            .ok_or_else(|| missing_commit_graph_error(left_parent_id))?;
                        let right_parent = parents
                            .next()
                            .flatten()
                            .ok_or_else(|| missing_commit_graph_error(right_parent_id))?;
                        validate_parent_generation(&left, &left_parent)?;
                        validate_parent_generation(&right, &right_parent)?;
                        left = left_parent;
                        right = right_parent;
                    }
                    _ => return Ok(LinearMergeBase::GeneralGraph),
                },
            }
        }
    }

    async fn load_linear_parent(
        &mut self,
        child: &CommitGraphNode,
        parent_id: CommitId,
    ) -> Result<CommitGraphNode, LixError> {
        let parent_ids = [parent_id];
        let parent = self
            .load_nodes(&parent_ids)
            .await?
            .into_iter()
            .next()
            .and_then(|(_, parent)| parent)
            .ok_or_else(|| missing_commit_graph_error(&parent_id))?;
        validate_parent_generation(child, &parent)?;
        Ok(parent)
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
                let records = self
                    .load_commit_records(std::slice::from_ref(&node.commit_id))
                    .await?;
                let record = records
                    .into_iter()
                    .next()
                    .flatten()
                    .ok_or_else(|| missing_commit_graph_error(&node.commit_id))?;
                let canonical_change = canonical_commit_change(&record);
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

    /// Loads semantic commit records through the same retained authenticated
    /// view as topology and member reads.
    pub(crate) async fn load_commit_records(
        &mut self,
        commit_ids: &[CommitId],
    ) -> Result<Vec<Option<CommitRecord>>, LixError> {
        crate::forktree::load_commit_records(self.topology.read(), commit_ids).await
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
        let members = crate::forktree::load_commit_member_records(self.topology.read(), commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        let mut changes = members
            .into_iter()
            .filter(|change| schema_keys.is_empty() || schema_keys.contains(&change.schema_key))
            .map(commit_graph_change_from_change_record)
            .collect::<Vec<_>>();
        changes.sort_by_key(|change| change.id);
        self.member_changes_cache
            .entry(schema_keys.to_vec())
            .or_default()
            .insert(commit_id, changes.clone());
        Ok(changes)
    }
}

/// Storage-free graph walk over authenticated ForkTree Commit objects. The
/// graph algorithm remains local to the semantic reader; there is no legacy
/// walker owner or persisted chronology accelerator.
async fn walk_reachable_nodes<S>(
    reader: &mut CommitGraphStoreReader<S>,
    head_commit_id: &CommitId,
) -> Result<Vec<ReachableCommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    let mut visiting = BTreeSet::new();
    let mut nearest_depths = BTreeMap::new();
    let mut stack = vec![TraversalFrame {
        commit_id: *head_commit_id,
        depth: 0,
        expanded: false,
    }];
    while let Some(frame) = stack.pop() {
        if frame.expanded {
            visiting.remove(&frame.commit_id);
            continue;
        }
        if visiting.contains(&frame.commit_id) {
            return Err(LixError::unknown(format!(
                "commit_graph cycle detected at commit '{}'",
                frame.commit_id
            )));
        }
        if nearest_depths
            .get(&frame.commit_id)
            .is_some_and(|previous| *previous <= frame.depth)
        {
            continue;
        }
        let commit = reader
            .load_node(&frame.commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&frame.commit_id))?;
        nearest_depths.insert(frame.commit_id, frame.depth);
        visiting.insert(frame.commit_id);
        stack.push(TraversalFrame {
            commit_id: frame.commit_id,
            depth: frame.depth,
            expanded: true,
        });
        for parent_commit_id in commit.parent_commit_ids.iter().rev() {
            stack.push(TraversalFrame {
                commit_id: *parent_commit_id,
                depth: frame.depth + 1,
                expanded: false,
            });
        }
    }
    let mut commits = Vec::with_capacity(nearest_depths.len());
    for (commit_id, depth) in nearest_depths {
        let commit = reader
            .load_node(&commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        commits.push(ReachableCommitGraphNode { commit, depth });
    }
    commits.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| left.commit.commit_id.cmp(&right.commit.commit_id))
    });
    Ok(commits)
}

async fn best_common_ancestors<S>(
    reader: &mut CommitGraphStoreReader<S>,
    left_commit_id: &CommitId,
    right_commit_id: &CommitId,
) -> Result<Vec<CommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    const LEFT: u8 = 1;
    const RIGHT: u8 = 2;
    const BOTH: u8 = LEFT | RIGHT;
    const STALE: u8 = 4;

    let left = reader
        .load_node(left_commit_id)
        .await?
        .ok_or_else(|| missing_commit_graph_error(left_commit_id))?;
    let right = reader
        .load_node(right_commit_id)
        .await?
        .ok_or_else(|| missing_commit_graph_error(right_commit_id))?;
    let mut colors = BTreeMap::from([(*left_commit_id, LEFT), (*right_commit_id, RIGHT)]);
    if left_commit_id == right_commit_id {
        colors.insert(*left_commit_id, BOTH);
    }
    let mut queue = BTreeSet::from([
        (left.generation, *left_commit_id),
        (right.generation, *right_commit_id),
    ]);
    let mut non_stale_queued = BTreeSet::from([*left_commit_id, *right_commit_id]);
    let mut best = Vec::new();
    while !queue.is_empty() {
        if !best.is_empty() && non_stale_queued.is_empty() {
            break;
        }
        let (generation, commit_id) = queue.pop_last().expect("queue is not empty");
        non_stale_queued.remove(&commit_id);
        let commit = reader
            .load_node(&commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        if commit.generation != generation {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' generation changed during graph walk"
            )));
        }
        let mut color = colors[&commit_id];
        if color & STALE == 0 && color & BOTH == BOTH {
            best.push(commit_id);
            color |= STALE;
            colors.insert(commit_id, color);
        }
        for parent_commit_id in commit.parent_commit_ids.iter().copied() {
            let parent = reader
                .load_node(&parent_commit_id)
                .await?
                .ok_or_else(|| missing_commit_graph_error(&parent_commit_id))?;
            validate_parent_generation(&commit, &parent)?;
            let parent_color = colors.entry(parent_commit_id).or_default();
            *parent_color |= color;
            queue.insert((parent.generation, parent_commit_id));
            if *parent_color & STALE == 0 {
                non_stale_queued.insert(parent_commit_id);
            } else {
                non_stale_queued.remove(&parent_commit_id);
            }
        }
    }
    best.sort_unstable();
    best.dedup();
    let mut nodes = Vec::with_capacity(best.len());
    for commit_id in best {
        nodes.push(
            reader
                .load_node(&commit_id)
                .await?
                .ok_or_else(|| missing_commit_graph_error(&commit_id))?,
        );
    }
    Ok(nodes)
}

struct TraversalFrame {
    commit_id: CommitId,
    depth: u32,
    expanded: bool,
}

fn commit_graph_change_from_change_record(change: ChangeRecord) -> CommitGraphChange {
    CommitGraphChange {
        id: change.change_id,
        account_id: change.account_id,
        row_pk: change.row_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot: change.snapshot,
        metadata: change.metadata,
        created_at: change.created_at,
        origin_key: change.origin_key,
    }
}

fn commit_graph_node_from_topology(topology: crate::forktree::CommitTopology) -> CommitGraphNode {
    CommitGraphNode {
        commit_id: topology.commit_id,
        generation: topology.generation,
        parent_commit_ids: topology.parent_commit_ids,
    }
}

fn missing_commit_graph_error(commit_id: &CommitId) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("commit_graph missing commit '{commit_id}'"),
    )
}

fn validate_parent_generation(
    child: &CommitGraphNode,
    parent: &CommitGraphNode,
) -> Result<(), LixError> {
    if parent.generation >= child.generation {
        return Err(LixError::unknown(format!(
            "commit '{}' parent '{}' does not have a lower generation",
            child.commit_id, parent.commit_id
        )));
    }
    Ok(())
}

fn no_common_history_error(left_commit_id: &CommitId, right_commit_id: &CommitId) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "commit_graph found no common history between '{left_commit_id}' and '{right_commit_id}'"
        ),
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

    async fn snapshot_roots(&mut self) -> Result<Vec<(String, CommitId)>, LixError> {
        Self::snapshot_roots(self).await
    }

    async fn load_commit_records(
        &mut self,
        commit_ids: &[CommitId],
    ) -> Result<Vec<Option<CommitRecord>>, LixError> {
        Self::load_commit_records(self, commit_ids).await
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
        && (request.row_pks.is_empty() || request.row_pks.contains(&change.row_pk))
        && (request.schema_keys.is_empty() || request.schema_keys.contains(&change.schema_key))
        && (request.file_ids.is_empty()
            || change
                .file_id
                .as_ref()
                .is_some_and(|file_id| request.file_ids.contains(file_id)))
}

fn history_change_identity(
    change: &CommitGraphChange,
) -> (ChangeId, String, Option<String>, RowPk) {
    (
        change.id,
        change.schema_key.clone(),
        change.file_id.clone(),
        change.row_pk.clone(),
    )
}

pub(crate) fn canonical_commit_change(record: &CommitRecord) -> CommitGraphChange {
    let snapshot_content =
        crate::changelog::commit_row_snapshot_json(&record.commit_id.to_string())
            .expect("lix_commit snapshot serialization should not fail");
    CommitGraphChange {
        id: record.change_id,
        account_id: record.account_id.clone(),
        row_pk: RowPk::uuid_from_canonical(&record.commit_id.to_string())
            .expect("commit IDs are canonical UUIDs"),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::Inline(snapshot_content.into()),
        metadata: crate::json_store::JsonSlot::None,
        created_at: record.created_at,
        origin_key: None,
    }
}
