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
    ChangeId, ChangeRecord, ChangelogContext, ChangelogReader, CommitId, CommitRecord,
    CommitScanRequest, CommitScopeKey,
};
use crate::commit_graph::scope_digest_census::{
    ScopeDigestOutcome, record_scope_digest_outcome, scope_digest_census,
};
use crate::commit_graph::walker::{ReachableWalk, best_common_ancestors, walk_reachable_nodes};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphHistory, CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode,
};
use crate::common::ExactBatch;
use crate::row_pk::RowPk;
use crate::storage_adapter::{
    StorageAdapterRead, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StorageProjectedValue, exact_get_many,
};
use crate::storage_codec;
use bytes::Bytes;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
/// Maximum number of known sparse bodies one history attempt asks sync to
/// hydrate. The retry loop can request another batch, so memory and network
/// work stay bounded independently of repository age.
const DEFERRED_HISTORY_DEMAND_CENSUS_BATCH_SIZE: usize = 64;
/// Read model for resolving changelog commit facts at a head.
///
/// The commit graph owns semantic commit metadata. Physical tracked-state
/// manifests are required by state/history payload readers, but GC may retire
/// those manifests while retaining a changelog projection until the semantic
/// commit itself becomes unreachable. Metadata reads must therefore not make
/// the physical serving manifest a second membership authority.
///
/// The changelog commit plane is a compact serving projection. State/history
/// payload readers validate physical commit-state authority before decoding
/// tracked data; metadata topology does not require that physical projection.
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

/// Commit-graph reader that resolves changelog rows at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    store: S,
    node_cache: HashMap<CommitId, Option<CommitGraphNode>>,
    /// Keyed by traversal head and depth bound. A bounded walk is a distinct
    /// result from the unbounded one, but an already materialized unbounded
    /// walk answers every bounded request without touching storage again.
    reachable_nodes_cache: HashMap<(CommitId, Option<u32>), Arc<[ReachableCommitGraphNode]>>,
    // A reader is bound to one pinned storage snapshot for the duration of a
    // SQL statement. File-history shaping asks the same reader for distinct
    // schema slices of that history, so retain immutable change records here.
    member_changes_cache:
        HashMap<(Vec<String>, Vec<String>), HashMap<CommitId, Vec<CommitGraphChange>>>,
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
            let commit_keys = uncached_ids
                .iter()
                .map(|commit_id| StorageKey(Bytes::from(crate::changelog::commit_key(*commit_id))))
                .collect::<Vec<_>>();
            let requests = [StorageGetManyRequest {
                space: crate::changelog::COMMIT_SPACE,
                keys: &commit_keys,
                opts: StorageGetOptions::default(),
            }];
            let mut values = exact_get_many(&self.store, &requests)
                .await?
                .values
                .into_iter();
            let records = uncached_ids
                .iter()
                .map(|_| {
                    let value = values.next().expect("exact commit slot is present");
                    let Some(bytes) = full_value_bytes(value) else {
                        return Ok(None);
                    };
                    let record = storage_codec::decode("commit record", &bytes)?;
                    Ok(Some(record))
                })
                .collect::<Result<Vec<Option<CommitRecord>>, LixError>>()?;
            let batch = ExactBatch::try_new("changelog commit", &uncached_ids, records)?;
            debug_assert!(values.next().is_none());
            for (commit_id, record) in batch {
                let node = commit_graph_node_from_record(record)?;
                self.node_cache.insert(*commit_id, node);
            }
        }
        let nodes = commit_ids
            .iter()
            .map(|commit_id| self.node_cache.get(commit_id).cloned().unwrap_or(None))
            .collect();
        ExactBatch::try_new("commit graph", commit_ids, nodes)
    }

    /// Loads every direct commit fact from the immutable changelog authority.
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
            for record in scan.entries {
                let node = commit_graph_node_from_record(Some(record))?
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
        self.reachable_nodes_within_depth(head_commit_id, None)
            .await
    }

    /// Walks only the complete nearest-depth layers needed to satisfy `limit`.
    pub(crate) async fn reachable_nodes_limited(
        &mut self,
        head_commit_id: &CommitId,
        limit: usize,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        if limit == 0 {
            return Ok(Arc::from([]));
        }
        if let Some(nodes) = self.reachable_nodes_cache.get(&(*head_commit_id, None)) {
            return Ok(nodes.iter().take(limit).cloned().collect());
        }
        let mut walk = ReachableWalk::new(*head_commit_id);
        let mut nodes = Vec::new();
        while let Some(layer) = walk.next_layer(self).await? {
            let depth = layer.depth;
            nodes.extend(
                layer
                    .commits
                    .into_iter()
                    .map(|commit| ReachableCommitGraphNode { commit, depth }),
            );
            if nodes.len() >= limit {
                break;
            }
        }
        nodes.truncate(limit);
        Ok(Arc::from(nodes))
    }

    /// Walks from `head_commit_id` and stops once `max_depth` is complete.
    async fn reachable_nodes_within_depth(
        &mut self,
        head_commit_id: &CommitId,
        max_depth: Option<u32>,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        if let Some(nodes) = self
            .reachable_nodes_cache
            .get(&(*head_commit_id, max_depth))
        {
            return Ok(Arc::clone(nodes));
        }
        if let Some(max_depth_value) = max_depth
            && let Some(nodes) = self.reachable_nodes_cache.get(&(*head_commit_id, None))
        {
            let bounded = nodes
                .iter()
                .take_while(|reachable| reachable.depth <= max_depth_value)
                .cloned()
                .collect::<Arc<[_]>>();
            self.reachable_nodes_cache
                .insert((*head_commit_id, max_depth), Arc::clone(&bounded));
            return Ok(bounded);
        }
        let nodes = Arc::from(walk_reachable_nodes(self, head_commit_id, max_depth).await?);
        self.reachable_nodes_cache
            .insert((*head_commit_id, max_depth), Arc::clone(&nodes));
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
        while left.generation != right.generation {
            let (deeper, target_generation) = if left.generation > right.generation {
                (&mut left, right.generation)
            } else {
                (&mut right, left.generation)
            };
            let [_] = deeper.parent_commit_ids.as_slice() else {
                return Ok(LinearMergeBase::GeneralGraph);
            };
            let jump = self.load_linear_jump(deeper).await?;
            if jump.generation >= target_generation {
                *deeper = jump;
            } else {
                let parent_id = deeper.parent_commit_ids[0];
                *deeper = self.load_linear_parent(deeper, parent_id).await?;
            }
        }

        while left.commit_id != right.commit_id {
            let ([left_parent_id], [right_parent_id]) = (
                left.parent_commit_ids.as_slice(),
                right.parent_commit_ids.as_slice(),
            ) else {
                return Ok(
                    if left.parent_commit_ids.is_empty() && right.parent_commit_ids.is_empty() {
                        LinearMergeBase::Disconnected
                    } else {
                        LinearMergeBase::GeneralGraph
                    },
                );
            };
            let jump_ids = [
                left.first_parent_jump_commit_id,
                right.first_parent_jump_commit_id,
            ];
            let jumps = self.load_nodes(&jump_ids).await?;
            let mut jumps = jumps.into_iter().map(|(_, jump)| jump);
            let left_jump = jumps
                .next()
                .flatten()
                .ok_or_else(|| missing_commit_graph_error(&jump_ids[0]))?;
            let right_jump = jumps
                .next()
                .flatten()
                .ok_or_else(|| missing_commit_graph_error(&jump_ids[1]))?;
            validate_first_parent_jump(&left, &left_jump)?;
            validate_first_parent_jump(&right, &right_jump)?;
            // Myers' simultaneous LCA step assumes equal lane depth. Merge
            // commits reset lanes, so unequal jump generations mean that one
            // side crossed a reset boundary; let the general DAG walker own
            // that case rather than risking an over-jump.
            if left_jump.generation != right_jump.generation {
                return Ok(LinearMergeBase::GeneralGraph);
            }
            if left_jump.commit_id == right_jump.commit_id {
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
            } else {
                left = left_jump;
                right = right_jump;
            }
        }
        Ok(LinearMergeBase::Resolved(left.commit_id))
    }

    async fn load_linear_jump(
        &mut self,
        node: &CommitGraphNode,
    ) -> Result<CommitGraphNode, LixError> {
        let jump_ids = [node.first_parent_jump_commit_id];
        let jump = self
            .load_nodes(&jump_ids)
            .await?
            .into_iter()
            .next()
            .and_then(|(_, jump)| jump)
            .ok_or_else(|| missing_commit_graph_error(&jump_ids[0]))?;
        validate_first_parent_jump(node, &jump)?;
        Ok(jump)
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
        let shaping = HistoryShaping::new(request);
        let mut state = HistoryCollection::default();
        let census_before = scope_digest_census();

        // Unbounded row demand still materializes and caches the whole
        // depth-bounded topology, because callers reuse it for commit metadata.
        let Some(limit) = request.limit else {
            let nodes = self
                .reachable_nodes_within_depth(start_commit_id, request.max_depth)
                .await?;
            if shaping.may_include_members
                && crate::tracked_state::has_deferred_commit_history(&self.store).await?
            {
                let candidates = nodes
                    .iter()
                    .filter(|reachable| depth_matches(reachable.depth, request))
                    .filter(|reachable| {
                        scope_digest_outcome(&reachable.commit, request, &shaping)
                            != ScopeDigestOutcome::Pruned
                    })
                    .map(|reachable| reachable.commit.commit_id)
                    .collect::<Vec<_>>();
                for candidate_batch in
                    candidates.chunks(DEFERRED_HISTORY_DEMAND_CENSUS_BATCH_SIZE)
                {
                    let deferred = crate::tracked_state::deferred_commit_history_ids(
                        &self.store,
                        candidate_batch,
                    )
                    .await?;
                    if !deferred.is_empty() {
                        return Err(crate::tracked_state::sync_history_required_for_commits(
                            &deferred,
                        ));
                    }
                }
            }
            for reachable in nodes.iter() {
                self.extend_history_entries(
                    start_commit_id,
                    &reachable.commit,
                    reachable.depth,
                    request,
                    &shaping,
                    &mut state,
                )
                .await?;
            }
            scope_digest_census()
                .since(&census_before)
                .emit(start_commit_id);
            return Ok(CommitGraphHistory {
                entries: state.entries,
                reachable_nodes: nodes,
            });
        };

        // Bounded row demand stops the traversal itself. Breadth-first layers
        // arrive in the same order the entries are published, so the first
        // `limit` entries are exactly the ones an unbounded read would expose.
        let mut walk = ReachableWalk::new(*start_commit_id);
        let mut reachable_nodes = Vec::new();
        while state.entries.len() < limit {
            let Some(layer) = walk.next_layer(self).await? else {
                break;
            };
            let depth = layer.depth;
            for node in layer.commits {
                self.extend_history_entries(
                    start_commit_id,
                    &node,
                    depth,
                    request,
                    &shaping,
                    &mut state,
                )
                .await?;
                reachable_nodes.push(ReachableCommitGraphNode {
                    commit: node,
                    depth,
                });
                if state.entries.len() >= limit {
                    break;
                }
            }
            if request
                .max_depth
                .is_some_and(|max_depth| depth >= max_depth)
            {
                break;
            }
        }

        scope_digest_census()
            .since(&census_before)
            .emit(start_commit_id);
        Ok(CommitGraphHistory {
            entries: state.entries,
            reachable_nodes: Arc::from(reachable_nodes),
        })
    }

    async fn extend_history_entries(
        &mut self,
        start_commit_id: &CommitId,
        node: &CommitGraphNode,
        depth: u32,
        request: &CommitGraphChangeHistoryRequest,
        shaping: &HistoryShaping,
        state: &mut HistoryCollection,
    ) -> Result<(), LixError> {
        if !depth_matches(depth, request) {
            return Ok(());
        }

        if shaping.may_include_commits {
            let canonical_change = canonical_commit_change(node);
            if state
                .seen_changes
                .insert(history_change_identity(&canonical_change))
                && change_matches_history_request(&canonical_change, request)
            {
                state.entries.push(CommitGraphChangeHistoryEntry {
                    change: canonical_change,
                    observed_commit_id: node.commit_id,
                    start_commit_id: *start_commit_id,
                    depth,
                });
            }
        }

        if !shaping.may_include_members {
            return Ok(());
        }

        // The per-commit membership test. `node` is already in hand — the
        // traversal had to load it to find this commit's parents — so proving
        // that none of the requested scopes has a member here costs no storage
        // read at all, and skips the replay-state header + inventory pair that
        // `load_member_changes` would otherwise fetch for this commit.
        let outcome = scope_digest_outcome(node, request, shaping);
        record_scope_digest_outcome(outcome);
        #[cfg(test)]
        crate::commit_graph::scope_digest_census::by_projection::record(
            &shaping.member_schema_keys,
            outcome,
        );
        if outcome == ScopeDigestOutcome::Pruned {
            return Ok(());
        }

        for change in self
            .load_member_changes(
                node.commit_id,
                &shaping.member_schema_keys,
                &shaping.member_file_ids,
            )
            .await?
        {
            if !state.seen_changes.insert(history_change_identity(&change)) {
                continue;
            }
            if change_matches_history_request(&change, request) {
                state.entries.push(CommitGraphChangeHistoryEntry {
                    change,
                    observed_commit_id: node.commit_id,
                    start_commit_id: *start_commit_id,
                    depth,
                });
            }
        }
        Ok(())
    }

    async fn load_member_changes(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
        file_ids: &[String],
    ) -> Result<Vec<CommitGraphChange>, LixError> {
        let cache_key = (schema_keys.to_vec(), file_ids.to_vec());
        if let Some(changes) = self
            .member_changes_cache
            .get(&cache_key)
            .and_then(|by_commit| by_commit.get(&commit_id))
        {
            return Ok(changes.clone());
        }
        let members = crate::tracked_state::load_commit_history_members_with_payloads_for_schemas(
            &self.store,
            commit_id,
            schema_keys,
            file_ids,
        )
        .await?;
        let mut changes = members
            .into_iter()
            .map(|member| commit_graph_change_from_change_record(member.change))
            .collect::<Vec<_>>();
        changes.sort_by_key(|change| change.id);
        self.member_changes_cache
            .entry(cache_key)
            .or_default()
            .insert(commit_id, changes.clone());
        Ok(changes)
    }
}

/// Request-derived shaping decisions that do not change while a history read
/// walks the graph.
struct HistoryShaping {
    member_schema_keys: Vec<String>,
    /// Files the request restricts member changes to.
    ///
    /// `change_matches_history_request` discards any member whose `file_id` is
    /// not in `request.file_ids`, so bounding the storage read on the same
    /// component returns the same entries. It is only a selector when the
    /// schema list is also known: a `schema_key | file_id` range needs both
    /// components, and without the schema list the read visits every schema
    /// anyway.
    member_file_ids: Vec<String>,
    may_include_members: bool,
    may_include_commits: bool,
}

impl HistoryShaping {
    fn new(request: &CommitGraphChangeHistoryRequest) -> Self {
        let mut member_schema_keys = request
            .schema_keys
            .iter()
            .filter(|schema_key| schema_key.as_str() != COMMIT_SCHEMA_KEY)
            .cloned()
            .collect::<Vec<_>>();
        member_schema_keys.sort();
        member_schema_keys.dedup();
        let mut member_file_ids = if member_schema_keys.is_empty() {
            Vec::new()
        } else {
            request.file_ids.clone()
        };
        member_file_ids.sort();
        member_file_ids.dedup();
        let may_include_members = request.schema_keys.is_empty() || !member_schema_keys.is_empty();
        let may_include_commits = request.schema_keys.is_empty()
            || request
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == COMMIT_SCHEMA_KEY);
        Self {
            member_schema_keys,
            member_file_ids,
            may_include_members,
            may_include_commits,
        }
    }
}

#[derive(Default)]
struct HistoryCollection {
    entries: Vec<CommitGraphChangeHistoryEntry>,
    seen_changes: BTreeSet<(ChangeId, String, Option<String>, RowPk)>,
}

fn commit_graph_change_from_change_record(change: ChangeRecord) -> CommitGraphChange {
    CommitGraphChange {
        id: change.change_id,
        account_id: change.account_id,
        row_pk: change.row_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        metadata: change.metadata,
        snapshot: change.snapshot,
        created_at: change.created_at,
        origin_key: change.origin_key,
    }
}

fn commit_graph_node_from_record(
    record: Option<CommitRecord>,
) -> Result<Option<CommitGraphNode>, LixError> {
    let Some(record) = record else {
        return Ok(None);
    };
    let node = CommitGraphNode {
        commit_id: record.commit_id,
        change_id: record.change_id(),
        account_id: record.account_id,
        generation: record.generation,
        parent_commit_ids: record.parent_commit_ids,
        first_parent_jump_commit_id: record.first_parent_jump_commit_id,
        first_parent_jump_span: record.first_parent_jump_span,
        created_at: record.created_at,
        touched_scope_digest: record.touched_scope_digest,
    };
    node.touched_scope_digest.validate()?;
    validate_first_parent_jump_summary(&node)?;
    Ok(Some(node))
}

fn first_parent_jump_generation(node: &CommitGraphNode) -> Result<u64, LixError> {
    node.generation
        .checked_sub(u64::from(node.first_parent_jump_span))
        .ok_or_else(|| {
            LixError::unknown(format!(
                "commit '{}' first-parent jump span exceeds its generation",
                node.commit_id
            ))
        })
}

fn validate_first_parent_jump_summary(node: &CommitGraphNode) -> Result<(), LixError> {
    first_parent_jump_generation(node)?;
    if node.parent_commit_ids.len() == 1 {
        if node.first_parent_jump_span == 0 || node.first_parent_jump_commit_id == node.commit_id {
            return Err(LixError::unknown(format!(
                "linear commit '{}' has no advancing first-parent jump",
                node.commit_id
            )));
        }
    } else if node.first_parent_jump_span != 0 || node.first_parent_jump_commit_id != node.commit_id
    {
        return Err(LixError::unknown(format!(
            "root or merge commit '{}' does not reset its first-parent jump",
            node.commit_id
        )));
    }
    Ok(())
}

fn validate_first_parent_jump(
    node: &CommitGraphNode,
    jump: &CommitGraphNode,
) -> Result<(), LixError> {
    if jump.commit_id != node.first_parent_jump_commit_id
        || jump.generation != first_parent_jump_generation(node)?
    {
        return Err(LixError::unknown(format!(
            "commit '{}' has an invalid first-parent jump '{}'",
            node.commit_id, node.first_parent_jump_commit_id
        )));
    }
    Ok(())
}

pub(super) fn missing_commit_graph_error(commit_id: &CommitId) -> LixError {
    LixError::commit_not_found(commit_id.to_string(), "walk_commit_graph", "graph_node")
}

fn full_value_bytes(value: Option<StorageProjectedValue>) -> Option<Bytes> {
    match value? {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
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

    async fn reachable_nodes_limited(
        &mut self,
        head_commit_id: &CommitId,
        limit: usize,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        Self::reachable_nodes_limited(self, head_commit_id, limit).await
    }

    async fn reachable_nodes_through_depth(
        &mut self,
        head_commit_id: &CommitId,
        max_depth: u32,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        Self::reachable_nodes_within_depth(self, head_commit_id, Some(max_depth)).await
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

/// Bound on how many `(schema_key, file_id)` pairs the membership test will
/// probe before falling back to the schema-family-only test.
///
/// Each pair costs one BLAKE3 keyed hash. A history request with a wide file
/// filter must not turn a free test into a per-commit hashing loop; the
/// schema-only test still prunes, just less selectively.
const MAX_PROBED_SCOPE_PAIRS: usize = 32;

/// Decides whether this commit's delta can be skipped without loading it.
///
/// Only ever returns [`ScopeDigestOutcome::Pruned`] on an **exact** digest that
/// proves every requested scope absent. Every other answer loads the delta, so
/// a wrong digest can only cost time, never rows — except for one real
/// obligation: the digest must contain a token for every scope the delta has a
/// member in. That is what `commit_delta_member_scopes` guarantees, and why a
/// delta whose member scopes are not enumerable publishes `opaque` rather than
/// a partial filter.
fn scope_digest_outcome(
    node: &CommitGraphNode,
    request: &CommitGraphChangeHistoryRequest,
    shaping: &HistoryShaping,
) -> ScopeDigestOutcome {
    if shaping.member_schema_keys.is_empty() {
        // An unconstrained request wants every member of every commit; there
        // is nothing to prove absent.
        return ScopeDigestOutcome::Unconstrained;
    }
    let digest = &node.touched_scope_digest;
    if digest.is_absent() {
        return ScopeDigestOutcome::LoadedAbsent;
    }
    if !digest.is_exact() {
        return ScopeDigestOutcome::LoadedOpaque;
    }

    // Schema-family test first: it is the cheapest and it is the one every
    // history projection can use, whether or not it also filters by file.
    let mut family_present = false;
    for schema_key in &shaping.member_schema_keys {
        if !digest.proves_absent(&CommitScopeKey {
            schema_key: schema_key.clone(),
            file_id: None,
        }) {
            family_present = true;
            break;
        }
    }
    if !family_present {
        return ScopeDigestOutcome::Pruned;
    }

    // A request that also pins file ids can ask the sharper question. Entries
    // with no file id cannot satisfy a non-empty `file_ids` filter (see
    // `change_matches_history_request`), so probing only the pairs is exact.
    if request.file_ids.is_empty()
        || shaping
            .member_schema_keys
            .len()
            .saturating_mul(request.file_ids.len())
            > MAX_PROBED_SCOPE_PAIRS
    {
        return ScopeDigestOutcome::LoadedPresent;
    }
    for schema_key in &shaping.member_schema_keys {
        for file_id in &request.file_ids {
            if !digest.proves_absent(&CommitScopeKey {
                schema_key: schema_key.clone(),
                file_id: Some(file_id.clone()),
            }) {
                return ScopeDigestOutcome::LoadedPresent;
            }
        }
    }
    ScopeDigestOutcome::Pruned
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

pub(crate) fn canonical_commit_change(node: &CommitGraphNode) -> CommitGraphChange {
    let snapshot_content = crate::changelog::commit_row_snapshot_json(&node.commit_id.to_string())
        .expect("lix_commit snapshot serialization should not fail");
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot_content)
        .expect("canonical lix_commit snapshot is valid JSON");
    let row_pk = RowPk::uuid_from_canonical(&node.commit_id.to_string())
        .expect("commit IDs are canonical UUIDs");
    let typed = crate::plugin::runtime::WasmTypedRow::from_builtin_json(
        COMMIT_SCHEMA_KEY,
        &row_pk,
        &snapshot,
    )
    .expect("derived lix_commit row satisfies its embedded schema");
    let snapshot = typed
        .durable_payload()
        .expect("derived lix_commit row has a native payload")
        .to_vec();
    CommitGraphChange {
        id: node.change_id,
        account_id: node.account_id.clone(),
        row_pk,
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        metadata: None,
        snapshot: Some(snapshot),
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
        BeginScanOptions, GetManyResult, KeyRange, ScanCursor, StorageError, StorageRead,
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
        commit_get_many_keys: Arc<AtomicUsize>,
        change_get_many_calls: Arc<AtomicUsize>,
        member_segment_get_many_calls: Arc<AtomicUsize>,
        commit_state_manifest_get_many_calls: Arc<AtomicUsize>,
    }

    impl StorageRead for CountingMemoryRead {
        async fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.commit_get_many_keys.fetch_add(
                requests
                    .iter()
                    .filter(|request| request.space == crate::changelog::COMMIT_SPACE)
                    .map(|request| request.keys.len())
                    .sum::<usize>(),
                Ordering::Relaxed,
            );
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
            if requests.iter().any(|request| {
                request.space == crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE
            }) {
                self.commit_state_manifest_get_many_calls
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: crate::storage::StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.inner.begin_scan(space, range, opts).await
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
                row_change("change-1", "row-1", "example", "{}"),
                row_change("change-2", "row-2", "example", "{}"),
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
        assert_eq!(commit.change_id, commit_id("commit-1").commit_change_id());
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
                snapshot_root: None,
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
    async fn load_node_serves_projection_without_commit_state_authority() {
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
        let node = CommitGraphContext::new()
            .reader(read)
            .load_node(&commit_id)
            .await
            .expect("commit metadata should remain readable")
            .expect("changelog projection should produce a node");
        assert_eq!(node.commit_id, commit_id);
    }

    #[tokio::test]
    async fn all_nodes_returns_parsed_commits_sorted_by_id() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-b-change", "commit-b", &[], &[]),
                row_change("change-1", "row-1", "example", "{}"),
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
                row_change("change-root", "row-root", "test_schema", "{}"),
                row_change("change-head", "row-head", "test_schema", "{}"),
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
    async fn bounded_history_stops_traversing_instead_of_truncating() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                row_change("change-root", "row-root", "test_schema", "{}"),
                row_change("change-middle", "row-middle", "test_schema", "{}"),
                row_change("change-head", "row-head", "test_schema", "{}"),
                commit_change("commit-root-change", "commit-root", &["change-root"], &[]),
                commit_change(
                    "commit-middle-change",
                    "commit-middle",
                    &["change-middle"],
                    &["commit-root"],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-head"],
                    &["commit-middle"],
                ),
            ],
        )
        .await;

        let commit_head = commit_id("commit-head");
        let base_request = CommitGraphChangeHistoryRequest {
            schema_keys: vec!["test_schema".to_string()],
            include_tombstones: true,
            ..CommitGraphChangeHistoryRequest::default()
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let full = reader
            .change_history_from_commit(&commit_head, &base_request)
            .await
            .expect("full history should resolve");
        assert_eq!(full.entries.len(), 3);
        assert_eq!(full.reachable_nodes.len(), 3);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let limited = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    limit: Some(1),
                    ..base_request.clone()
                },
            )
            .await
            .expect("bounded history should resolve");
        assert_eq!(
            limited
                .entries
                .iter()
                .map(|entry| entry.change.id)
                .collect::<Vec<_>>(),
            full.entries[..1]
                .iter()
                .map(|entry| entry.change.id)
                .collect::<Vec<_>>(),
            "a bounded read must expose the same prefix an unbounded read would"
        );
        assert_eq!(
            limited.reachable_nodes.len(),
            1,
            "a satisfied row bound must stop the walk, not truncate afterwards"
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let shallow = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    max_depth: Some(0),
                    ..base_request
                },
            )
            .await
            .expect("depth-bounded history should resolve");
        assert_eq!(shallow.entries.len(), 1);
        assert_eq!(
            shallow.reachable_nodes.len(),
            1,
            "a depth bound must stop the walk, not filter it afterwards"
        );
    }

    #[tokio::test]
    async fn depth_bounded_history_does_not_load_ancestry_below_its_frontier() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        append_changes(
            &storage,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change(
                    "commit-parent-change",
                    "commit-parent",
                    &[],
                    &["commit-root"],
                ),
                commit_change("commit-head-change", "commit-head", &[], &["commit-parent"]),
            ],
        )
        .await;

        let commit_get_many_keys = Arc::new(AtomicUsize::new(0));
        let read = memory
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader =
            CommitGraphContext::new().reader(StorageAdapterReadScope::new(CountingMemoryRead {
                inner: read,
                commit_get_many_keys: Arc::clone(&commit_get_many_keys),
                change_get_many_calls: Arc::new(AtomicUsize::new(0)),
                member_segment_get_many_calls: Arc::new(AtomicUsize::new(0)),
                commit_state_manifest_get_many_calls: Arc::new(AtomicUsize::new(0)),
            }));
        let history = reader
            .change_history_from_commit(
                &commit_id("commit-head"),
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec![super::COMMIT_SCHEMA_KEY.to_string()],
                    max_depth: Some(0),
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("bounded history should resolve");

        assert_eq!(history.entries.len(), 1);
        assert_eq!(history.reachable_nodes.len(), 1);
        assert_eq!(history.reachable_nodes[0].depth, 0);
        assert_eq!(
            commit_get_many_keys.load(Ordering::Relaxed),
            1,
            "depth zero history must load only its anchor commit",
        );
    }

    #[tokio::test]
    async fn change_history_reuses_canonical_changes_across_requests() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        append_changes(
            &storage,
            &[
                row_change("change-root", "row-root", "test_schema", "{}"),
                row_change("change-head", "row-head", "test_schema", "{}"),
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
            commit_get_many_keys: Arc::new(AtomicUsize::new(0)),
            change_get_many_calls: Arc::clone(&change_get_many_calls),
            member_segment_get_many_calls,
            commit_state_manifest_get_many_calls: Arc::new(AtomicUsize::new(0)),
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
        let alpha_pk = crate::row_pk::RowPk::single("alpha-row");
        let alpha_second_pk = crate::row_pk::RowPk::single("alpha-second-row");
        let beta_pk = crate::row_pk::RowPk::single("beta-row");
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
                    touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                    format_version: 3,
                    commit_id,
                    generation: 0,
                    parent_commit_ids: Vec::new(),
                    first_parent_jump_commit_id: commit_id,
                    first_parent_jump_span: 0,
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
                    row_pk: &alpha_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                metadata: None,
                snapshot: None,
                origin_key: None,
                base_coordinate: None,
                authored: false,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "beta",
                    file_id: None,
                    row_pk: &beta_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                metadata: None,
                snapshot: None,
                origin_key: None,
                base_coordinate: None,
                authored: false,
            },
            TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "alpha",
                    file_id: None,
                    row_pk: &alpha_second_pk,
                    change_id: shared_change_id,
                    commit_id,
                    deleted: true,
                    created_at,
                    updated_at: created_at,
                },
                metadata: None,
                snapshot: None,
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
                snapshot_root: None,
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
                .any(|entry| entry.change.row_pk == alpha_pk)
        );
        assert!(
            alpha_first
                .entries
                .iter()
                .any(|entry| entry.change.row_pk == alpha_second_pk)
        );
        assert_eq!(beta.entries.len(), 1);
        assert_eq!(beta.entries[0].change.schema_key, "beta");
        assert_eq!(beta.entries[0].change.row_pk, beta_pk);
        assert_eq!(alpha_second.entries, alpha_first.entries);
        assert!(Arc::ptr_eq(
            &alpha_first.reachable_nodes,
            &alpha_second.reachable_nodes
        ));
        let row_history = reader
            .change_history_from_commit(
                &commit_id,
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["alpha".to_string()],
                    row_pks: vec![alpha_second_pk.clone()],
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("identity-filtered selected tombstone should load");
        assert_eq!(row_history.entries.len(), 1);
        assert_eq!(row_history.entries[0].change.row_pk, alpha_second_pk);
    }

    #[tokio::test]
    async fn topology_reads_do_not_load_commit_member_payloads() {
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        append_changes(
            &storage,
            &[
                row_change("change-root", "row-root", "test_schema", "{}"),
                row_change("change-head", "row-head", "test_schema", "{}"),
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
        let commit_state_manifest_get_many_calls = Arc::new(AtomicUsize::new(0));
        let read = memory
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader =
            CommitGraphContext::new().reader(StorageAdapterReadScope::new(CountingMemoryRead {
                inner: read,
                commit_get_many_keys: Arc::new(AtomicUsize::new(0)),
                change_get_many_calls: Arc::new(AtomicUsize::new(0)),
                member_segment_get_many_calls: Arc::clone(&member_segment_get_many_calls),
                commit_state_manifest_get_many_calls: Arc::clone(
                    &commit_state_manifest_get_many_calls,
                ),
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
        assert_eq!(
            commit_state_manifest_get_many_calls.load(Ordering::Relaxed),
            0,
            "topology APIs must read only the immutable changelog authority",
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
    async fn change_history_from_commit_filters_depth_row_file_and_tombstones() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                row_change_with_file(
                    "change-01920000-0000-7000-8000-0000000000a2",
                    "row-1",
                    "test_schema",
                    Some("01920000-0000-7000-8000-0000000000a2"),
                    "{}",
                ),
                row_tombstone("change-tombstone", "row-1", "test_schema"),
                row_change_with_file(
                    "change-01920000-0000-7000-8000-0000000000b2",
                    "row-2",
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
                    row_pks: vec![crate::row_pk::RowPk::single("row-1")],
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
                row_tombstone("change-deleted", "row-1", "test_schema"),
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
                    row_pk: crate::row_pk::RowPk::single(commit_id),
                    schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
                    file_id: None,
                    metadata: None,
                    snapshot: None,
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

        fn row(
            change_id: &str,
            row_pk: &str,
            schema_key: &str,
            file_id: Option<&str>,
            snapshot_content: Option<&str>,
            created_at: &str,
        ) -> Self {
            let row_pk = crate::row_pk::RowPk::single(row_pk);
            let snapshot = snapshot_content.map(|content| {
                let snapshot = serde_json::from_str(content)
                    .expect("commit-graph fixture snapshot should be valid JSON");
                let row = crate::plugin::runtime::WasmTypedRow::from_test_json_unchecked(
                    &row_pk, &snapshot,
                )
                .expect("commit-graph fixture should construct a typed row");
                row.durable_payload()
                    .map(|payload| payload.to_vec())
                    .expect("commit-graph fixture should encode a durable typed payload")
            });
            Self {
                change: CommitGraphChange {
                    id: ChangeId::for_test_label(change_id),
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    row_pk,
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                    metadata: None,
                    snapshot,
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
                        .row_pk
                        .as_single_string()
                        .expect("commit fixture should use single row pk"),
                )
            })
            .collect::<BTreeSet<_>>();
        let mut staged_commit_ids = BTreeSet::new();
        let changelog = ChangelogContext::new();
        let mut writer = changelog.writer(&mut read, &mut writes);
        let mut append = ChangelogAppend::default();
        let mut commit_members = Vec::<(CommitId, Vec<ChangeRecord>)>::new();
        let mut generations = BTreeMap::<CommitId, u64>::new();
        let mut topology_records = BTreeMap::<CommitId, CommitRecord>::new();
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit_label = change
                .change
                .row_pk
                .as_single_string()
                .expect("commit fixture should use single row pk")
                .to_string();
            let commit_id = CommitId::for_test_label(&commit_label);
            for parent_commit_id in &change.parent_commit_ids {
                if !provided_commit_ids.contains(parent_commit_id)
                    && staged_commit_ids.insert(*parent_commit_id)
                {
                    append_empty_commit(&mut append, *parent_commit_id);
                    generations.insert(*parent_commit_id, 0);
                    topology_records.insert(
                        *parent_commit_id,
                        append
                            .commits
                            .last()
                            .expect("empty commit was appended")
                            .clone(),
                    );
                }
            }
            let generation = change
                .parent_commit_ids
                .iter()
                .filter_map(|parent| generations.get(parent).copied())
                .max()
                .map_or(0, |parent_generation| parent_generation + 1);
            let parent = match change.parent_commit_ids.as_slice() {
                [parent_commit_id] => topology_records.get(parent_commit_id),
                _ => None,
            };
            let parent_jump = parent.map(|parent| {
                topology_records
                    .get(&parent.first_parent_jump_commit_id)
                    .expect("test parent jump target exists")
            });
            let (first_parent_jump_commit_id, first_parent_jump_span) =
                crate::changelog::next_first_parent_jump(
                    commit_id,
                    &change.parent_commit_ids,
                    parent,
                    parent_jump,
                )
                .expect("test commit jump should derive");
            let mut members = Vec::new();
            for change_id in &change.commit_change_ids {
                if let Some(change) = changes_by_id.get(change_id) {
                    members.push(change_record_from_test_change(change));
                }
            }

            let record = CommitRecord {
                touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
                format_version: 3,
                commit_id,
                generation,
                parent_commit_ids: change.parent_commit_ids.clone(),
                first_parent_jump_commit_id,
                first_parent_jump_span,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: change.change.created_at,
            };
            append.commits.push(record.clone());
            topology_records.insert(commit_id, record);
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
                        row_pk: &change.row_pk,
                        change_id: change.change_id,
                        commit_id: *commit_id,
                        deleted: change.snapshot.is_none(),
                        created_at: change.created_at,
                        updated_at: change.created_at,
                    },
                    metadata: change.metadata.as_ref(),
                    snapshot: change.snapshot.as_deref(),
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
                snapshot_root: None,
            },
        )
        .expect("test commit-state manifest should stage");
    }

    fn append_empty_commit(append: &mut ChangelogAppend, commit_id: CommitId) {
        append.commits.push(CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: ts("2026-01-01T00:00:00Z"),
        });
    }

    fn change_record_from_test_change(change: &TestChange) -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: change.change.id,
            account_id: change.change.account_id.clone(),
            row_pk: change.change.row_pk.clone(),
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            metadata: change.change.metadata.clone(),
            snapshot: change.change.snapshot.clone(),
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
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            commit_id,
            change_id: ChangeId::for_test_label(&format!("{commit_label}-change")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            generation: 0,
            parent_commit_ids: parent_commit_ids
                .iter()
                .map(|parent_id| CommitId::for_test_label(parent_id))
                .collect(),
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
            created_at: ts("2026-01-01T00:00:00Z"),
        }
    }

    fn row_change(
        change_id: &str,
        row_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> TestChange {
        row_change_at(
            change_id,
            row_pk,
            schema_key,
            snapshot_content,
            "2026-01-01T00:00:00Z",
        )
    }

    fn row_change_at(
        change_id: &str,
        row_pk: &str,
        schema_key: &str,
        snapshot_content: &str,
        created_at: &str,
    ) -> TestChange {
        TestChange::row(
            change_id,
            row_pk,
            schema_key,
            None,
            Some(snapshot_content),
            created_at,
        )
    }

    fn row_change_with_file(
        change_id: &str,
        row_pk: &str,
        schema_key: &str,
        file_id: Option<&str>,
        snapshot_content: &str,
    ) -> TestChange {
        TestChange::row(
            change_id,
            row_pk,
            schema_key,
            file_id,
            Some(snapshot_content),
            "2026-01-01T00:00:00Z",
        )
    }

    fn row_tombstone(change_id: &str, row_pk: &str, schema_key: &str) -> TestChange {
        TestChange::row(
            change_id,
            row_pk,
            schema_key,
            None,
            None,
            "2026-01-02T00:00:00Z",
        )
    }
}
