use std::collections::{BTreeMap, BTreeSet};

use crate::commit_graph::walker::{best_common_ancestors, walk_reachable_commits};
use crate::commit_graph::{
    CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
    CommitGraphEdge, CommitGraphEntity, CommitGraphReader, ReachableCommitGraphCommit,
};
use crate::commit_store::{Change, Commit, CommitStoreContext, CommitStoreReader};
use crate::entity_identity::EntityIdentity;
use crate::storage::StorageReader;
use crate::storage::{ScopedStorageReader, StorageReadScope};
use crate::LixError;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

/// Read model for resolving commit-store commits into entity state at a head.
///
/// This module does not own durable storage. It reads immutable commit-store
/// facts through a caller-provided KV store and applies commit graph rules on
/// top.
#[derive(Clone)]
pub(crate) struct CommitGraphContext {
    commit_store: CommitStoreContext,
}

impl CommitGraphContext {
    pub(crate) fn new() -> Self {
        Self {
            commit_store: CommitStoreContext::new(),
        }
    }

    /// Creates a graph reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommitGraphStoreReader<S>
    where
        S: StorageReader,
    {
        let read_scope = StorageReadScope::new(store);
        CommitGraphStoreReader {
            commit_store_reader: self.commit_store.reader(read_scope.store()),
        }
    }
}

/// Commit-graph reader that resolves commit-store entities at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageReader,
{
    commit_store_reader: CommitStoreReader<ScopedStorageReader<S>>,
}

impl<S> CommitGraphStoreReader<S>
where
    S: StorageReader,
{
    /// Returns the canonical entities that are effective at `head_commit_id`.
    ///
    /// Reachable commits are visited nearest-first. For each commit, the commit
    /// row itself is visible, then introduced/adopted `change_ids` are visited
    /// in reverse order so later writes in the same commit win.
    pub(crate) async fn entities_at(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<CommitGraphEntity>, LixError> {
        let commits = self.reachable_commits(head_commit_id).await?;
        self.select_entities(commits).await
    }

    /// Loads and parses a `lix_commit` canonical change by commit id.
    pub(crate) async fn load_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        let Some(commit) = self.commit_store_reader.load_commit(commit_id).await? else {
            return Ok(None);
        };
        self.graph_commit_from_store_commit(commit).await.map(Some)
    }

    /// Loads every commit fact from the commit store.
    ///
    /// This is used by global commit surfaces where the caller wants the durable
    /// graph facts themselves, not reachability from a particular version head.
    pub(crate) async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
        let stored_commits = self.commit_store_reader.scan_commits().await?;
        let mut commits = Vec::new();
        for commit in stored_commits {
            commits.push(self.graph_commit_from_store_commit(commit).await?);
        }
        commits.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));
        Ok(commits)
    }

    /// Walks from `head_commit_id` through parent commits and records nearest depth.
    pub(crate) async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
        walk_reachable_commits(self, head_commit_id).await
    }

    /// Returns the best common ancestors shared by two commit heads.
    ///
    /// This is the commit-DAG primitive. It can return more than one commit in
    /// criss-cross histories. Merge code should layer an explicit merge-base
    /// policy on top when it needs exactly one base for a three-way merge.
    pub(crate) async fn best_common_ancestors(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<Vec<CommitGraphCommit>, LixError> {
        best_common_ancestors(self, left_commit_id, right_commit_id).await
    }

    /// Resolves the single commit base to use for a three-way merge.
    ///
    /// This is merge policy layered over `best_common_ancestors(...)`. Histories
    /// with no shared base or multiple equally good bases are rejected for now
    /// so merge code cannot accidentally hide unsupported graph semantics.
    pub(crate) async fn merge_base(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<CommitGraphCommit, LixError> {
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
            [base] => Ok(base.clone()),
            _ => Err(LixError::ambiguous_merge_base(
                left_commit_id,
                right_commit_id,
                ancestors
                    .iter()
                    .map(|ancestor| ancestor.commit_id.clone())
                    .collect(),
            )),
        }
    }

    /// Derives parent/child edges from parsed commits.
    pub(crate) fn commit_edges(&self, commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge> {
        commits
            .iter()
            .flat_map(|commit| {
                commit.parent_commit_ids.iter().enumerate().map(
                    |(parent_order, parent_commit_id)| CommitGraphEdge {
                        parent_commit_id: parent_commit_id.clone(),
                        child_commit_id: commit.commit_id.clone(),
                        parent_order: parent_order as u32,
                    },
                )
            })
            .collect()
    }

    /// Returns canonical changes reachable from `start_commit_id`.
    ///
    /// This is the primitive history API. It reports the commit/depth where
    /// each matching canonical change was introduced or adopted during graph
    /// traversal and leaves row shaping to callers such as SQL providers.
    pub(crate) async fn change_history_from_commit(
        &mut self,
        start_commit_id: &str,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
        let commits = self.reachable_commits(start_commit_id).await?;
        let mut entries = Vec::new();
        let mut seen_change_ids = BTreeSet::new();

        for reachable in commits {
            if !depth_matches(reachable.depth, request) {
                continue;
            }

            let commit_id = reachable.commit.commit_id;
            for change_id in reachable.commit.change_ids {
                if !seen_change_ids.insert(change_id.clone()) {
                    continue;
                }
                let change = self
                    .load_member_canonical_change(&change_id, &commit_id)
                    .await?;
                if change_matches_history_request(&change, request) {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change,
                        observed_commit_id: commit_id.clone(),
                        start_commit_id: start_commit_id.to_string(),
                        depth: reachable.depth,
                    });
                }
            }
        }

        Ok(entries)
    }

    async fn load_member_canonical_change(
        &mut self,
        change_id: &str,
        source_commit_id: &str,
    ) -> Result<Change, LixError> {
        let change_ids = vec![change_id.to_string()];
        self.load_canonical_changes(&change_ids)
            .await?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "commit_graph commit '{source_commit_id}' references missing change '{change_id}'"
                    ),
                )
            })
    }

    /// Selects the first reachable change for each canonical entity identity.
    async fn select_entities(
        &mut self,
        commits: Vec<ReachableCommitGraphCommit>,
    ) -> Result<Vec<CommitGraphEntity>, LixError> {
        let mut order = Vec::new();
        let mut entities = BTreeMap::new();

        for reachable in commits {
            let depth = reachable.depth;
            let source_commit_id = reachable.commit.commit_id;

            observe_change(
                &mut order,
                &mut entities,
                reachable.commit.canonical_change,
                source_commit_id.clone(),
                depth,
            );

            for change_id in reachable.commit.change_ids.iter().rev() {
                let change = self
                    .load_member_canonical_change(change_id, &source_commit_id)
                    .await?;
                observe_change(
                    &mut order,
                    &mut entities,
                    change,
                    source_commit_id.clone(),
                    depth,
                );
            }
        }

        Ok(order
            .into_iter()
            .filter_map(|identity| {
                entities
                    .remove(&identity)
                    .map(|accumulator| accumulator.entity)
            })
            .collect())
    }

    async fn graph_commit_from_store_commit(
        &mut self,
        commit: Commit,
    ) -> Result<CommitGraphCommit, LixError> {
        let change_ids = self.load_commit_change_ids(&commit).await?;
        Ok(commit_graph_commit_from_store_commit(commit, change_ids)?)
    }

    async fn load_commit_change_ids(&self, commit: &Commit) -> Result<Vec<String>, LixError> {
        let mut change_ids = Vec::new();
        for pack_id in 0..commit.change_pack_count {
            let Some(changes) = self
                .commit_store_reader
                .load_change_pack(&commit.id, pack_id)
                .await?
            else {
                return Err(missing_pack_error("change", &commit.id, pack_id));
            };
            change_ids.extend(changes.into_iter().map(|change| change.id));
        }
        for pack_id in 0..commit.membership_pack_count {
            let Some(members) = self
                .commit_store_reader
                .load_membership_pack(&commit.id, pack_id)
                .await?
            else {
                return Err(missing_pack_error("membership", &commit.id, pack_id));
            };
            change_ids.extend(members.into_iter().map(|locator| locator.change_id));
        }
        Ok(change_ids)
    }

    async fn load_canonical_changes(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<Change>>, LixError> {
        self.commit_store_reader
            .load_changes(change_ids)
            .await
            .map(|changes| {
                changes
                    .into_iter()
                    .map(|change| change.map(canonical_change_from_store_change))
                    .collect()
            })
    }
}

#[async_trait::async_trait]
impl<S> CommitGraphReader for CommitGraphStoreReader<S>
where
    S: StorageReader,
{
    async fn load_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        CommitGraphStoreReader::load_commit(self, commit_id).await
    }

    async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
        CommitGraphStoreReader::all_commits(self).await
    }

    async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
        CommitGraphStoreReader::reachable_commits(self, head_commit_id).await
    }

    async fn best_common_ancestors(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<Vec<CommitGraphCommit>, LixError> {
        CommitGraphStoreReader::best_common_ancestors(self, left_commit_id, right_commit_id).await
    }

    async fn merge_base(
        &mut self,
        left_commit_id: &str,
        right_commit_id: &str,
    ) -> Result<CommitGraphCommit, LixError> {
        CommitGraphStoreReader::merge_base(self, left_commit_id, right_commit_id).await
    }

    fn commit_edges(&self, commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge> {
        CommitGraphStoreReader::commit_edges(self, commits)
    }

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &str,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
        CommitGraphStoreReader::change_history_from_commit(self, start_commit_id, request).await
    }
}

fn depth_matches(depth: u32, request: &CommitGraphChangeHistoryRequest) -> bool {
    request.min_depth.map_or(true, |min| depth >= min)
        && request.max_depth.map_or(true, |max| depth <= max)
}

fn change_matches_history_request(
    change: &Change,
    request: &CommitGraphChangeHistoryRequest,
) -> bool {
    (request.include_tombstones || change.snapshot_ref.is_some())
        && (request.entity_ids.is_empty() || request.entity_ids.contains(&change.entity_id))
        && (request.schema_keys.is_empty() || request.schema_keys.contains(&change.schema_key))
        && (request.file_ids.is_empty()
            || change
                .file_id
                .as_ref()
                .is_some_and(|file_id| request.file_ids.contains(file_id)))
}

fn observe_change(
    order: &mut Vec<CanonicalEntityIdentity>,
    entities: &mut BTreeMap<CanonicalEntityIdentity, EntityAccumulator>,
    change: Change,
    source_commit_id: String,
    depth: u32,
) {
    let identity = CanonicalEntityIdentity::from_change(&change);
    if let Some(accumulator) = entities.get_mut(&identity) {
        // TODO: represent unresolved parent-parent merge conflicts instead of
        // collapsing them through deterministic traversal order. A head commit
        // change for the same identity should remain the explicit resolution.
        accumulator.entity.created_at = change.created_at.clone();
        return;
    }

    order.push(identity.clone());
    entities.insert(
        identity,
        EntityAccumulator {
            entity: CommitGraphEntity {
                created_at: change.created_at.clone(),
                updated_at: change.created_at.clone(),
                change,
                source_commit_id,
                depth,
            },
        },
    );
}

#[derive(Debug)]
struct EntityAccumulator {
    entity: CommitGraphEntity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalEntityIdentity {
    entity_id: EntityIdentity,
    schema_key: String,
    file_id: Option<String>,
}

impl CanonicalEntityIdentity {
    fn from_change(change: &Change) -> Self {
        Self {
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
        }
    }
}

fn commit_graph_commit_from_store_commit(
    commit: Commit,
    change_ids: Vec<String>,
) -> Result<CommitGraphCommit, LixError> {
    let change = commit_header_canonical_change(commit.clone());
    Ok(CommitGraphCommit {
        canonical_change: change.clone(),
        change,
        commit_id: commit.id,
        change_ids,
        author_account_ids: commit.author_account_ids,
        parent_commit_ids: commit.parent_ids,
    })
}

fn commit_header_canonical_change(commit: Commit) -> Change {
    Change {
        id: commit.change_id,
        entity_id: EntityIdentity::single(&commit.id),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_ref: None,
        metadata_ref: None,
        created_at: commit.created_at,
    }
}

fn canonical_change_from_store_change(change: Change) -> Change {
    Change {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at: change.created_at,
    }
}

fn missing_pack_error(label: &str, commit_id: &str, pack_id: u32) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("commit_graph missing {label} pack ({commit_id}, {pack_id})"),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::backend::testing::UnitTestBackend;
    use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphContext};
    use crate::commit_store::{
        Change, ChangeLocator, ChangeRef, CommitDraftRef, CommitStoreContext,
    };
    use crate::storage::{StorageContext, StorageWriteSet};

    #[tokio::test]
    async fn load_commit_parses_commit_snapshot() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[commit_change(
                "commit-1-change",
                "commit-1",
                &["change-1", "change-2"],
                &["parent-1"],
            )],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let commit = reader
            .load_commit("commit-1")
            .await
            .expect("commit load should succeed")
            .expect("commit should exist");

        assert_eq!(commit.commit_id, "commit-1");
        assert_eq!(commit.change_ids, vec!["change-1", "change-2"]);
        assert_eq!(commit.parent_commit_ids, vec!["parent-1"]);
        assert_eq!(commit.change.id, "commit-1-change");
    }

    #[tokio::test]
    async fn load_commit_returns_none_for_missing_commit() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);

        let commit = reader
            .load_commit("missing")
            .await
            .expect("commit load should succeed");

        assert_eq!(commit, None);
    }

    #[tokio::test]
    async fn all_commits_returns_parsed_commits_sorted_by_id() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                commit_change("commit-b-change", "commit-b", &[], &[]),
                entity_change("change-1", "entity-1", "example", "{}"),
                commit_change("commit-a-change", "commit-a", &[], &[]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let commits = reader
            .all_commits()
            .await
            .expect("commit scan should succeed");

        assert_eq!(
            commits
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-a", "commit-b"]
        );
    }

    #[tokio::test]
    async fn entities_at_walks_ancestors_and_computes_nearest_depth() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-left-change", "commit-left", &[], &["commit-root"]),
                commit_change("commit-right-change", "commit-right", &[], &["commit-root"]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &[],
                    &["commit-left", "commit-right"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("ancestor traversal should succeed");

        let depths = entities
            .into_iter()
            .map(|entity| (entity.source_commit_id, entity.depth))
            .collect::<Vec<_>>();
        assert_eq!(
            depths,
            vec![
                ("commit-head".to_string(), 0),
                ("commit-left".to_string(), 1),
                ("commit-right".to_string(), 1),
                ("commit-root".to_string(), 2),
            ]
        );
    }

    #[tokio::test]
    async fn commit_edges_are_derived_from_parent_commit_ids() {
        let graph = CommitGraphContext::new();
        let reader = graph.reader(StorageContext::new(Arc::new(UnitTestBackend::new())));
        let commits = vec![parsed_commit(
            "commit-head",
            &[],
            &["commit-left", "commit-right"],
        )];

        let edges = reader.commit_edges(&commits);

        assert_eq!(
            edges
                .iter()
                .map(|edge| (
                    edge.parent_commit_id.as_str(),
                    edge.child_commit_id.as_str(),
                    edge.parent_order,
                ))
                .collect::<Vec<_>>(),
            vec![
                ("commit-left", "commit-head", 0),
                ("commit-right", "commit-head", 1)
            ]
        );
    }

    #[tokio::test]
    async fn change_history_from_commit_reports_matching_canonical_changes_with_depth() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
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
        let mut reader = graph.reader(storage);
        let history = reader
            .change_history_from_commit(
                "commit-head",
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
                .iter()
                .map(|entry| (
                    entry.change.id.as_str(),
                    entry.observed_commit_id.as_str(),
                    entry.start_commit_id.as_str(),
                    entry.depth
                ))
                .collect::<Vec<_>>(),
            vec![
                ("change-head", "commit-head", "commit-head", 0),
                ("change-root", "commit-root", "commit-head", 1),
            ]
        );
    }

    #[tokio::test]
    async fn change_history_from_commit_filters_depth_entity_file_and_tombstones() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change_with_file(
                    "change-file-a",
                    "entity-1",
                    "test_schema",
                    Some("file-a"),
                    "{}",
                ),
                entity_tombstone("change-tombstone", "entity-1", "test_schema"),
                entity_change_with_file(
                    "change-file-b",
                    "entity-2",
                    "test_schema",
                    Some("file-b"),
                    "{}",
                ),
                commit_change("commit-root-change", "commit-root", &["change-file-a"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-tombstone", "change-file-b"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let history = reader
            .change_history_from_commit(
                "commit-head",
                &CommitGraphChangeHistoryRequest {
                    entity_ids: vec![crate::entity_identity::EntityIdentity::single("entity-1")],
                    file_ids: vec!["file-a".to_string()],
                    min_depth: Some(1),
                    max_depth: Some(1),
                    include_tombstones: false,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");

        assert_eq!(history.len(), 1);
        assert_eq!(history[0].change.id, "change-file-a");
        assert_eq!(history[0].depth, 1);
    }

    #[tokio::test]
    async fn change_history_from_commit_includes_tombstones_when_requested() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
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
        let mut reader = graph.reader(storage);
        let hidden = reader
            .change_history_from_commit("commit-head", &CommitGraphChangeHistoryRequest::default())
            .await
            .expect("history should resolve");
        let visible = reader
            .change_history_from_commit(
                "commit-head",
                &CommitGraphChangeHistoryRequest {
                    include_tombstones: true,
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");

        assert!(hidden.is_empty());
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].change.id, "change-deleted");
    }

    #[tokio::test]
    async fn entities_at_selects_nearest_member_change_for_identity() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change(
                    "change-old",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"old\"}",
                ),
                entity_change(
                    "change-new",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"new\"}",
                ),
                commit_change("commit-root-change", "commit-root", &["change-old"], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-new"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-new".to_string(), "commit-head".to_string(), 0)]
        );
    }

    #[tokio::test]
    async fn entities_at_reports_created_at_from_oldest_reachable_change() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change_at(
                    "change-created",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"created\"}",
                    "2026-01-01T00:00:00Z",
                ),
                entity_change_at(
                    "change-updated",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"updated\"}",
                    "2026-01-02T00:00:00Z",
                ),
                commit_change(
                    "commit-root-change",
                    "commit-root",
                    &["change-created"],
                    &[],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-updated"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");
        let entity = entities
            .iter()
            .find(|entity| entity.change.schema_key == "test_schema")
            .expect("test entity should resolve");

        assert_eq!(entity.change.id, "change-updated");
        assert_eq!(entity.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(entity.updated_at, "2026-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn entities_at_uses_reverse_change_order_within_commit() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change(
                    "change-first",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"first\"}",
                ),
                entity_change(
                    "change-last",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"last\"}",
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-first", "change-last"],
                    &[],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-last".to_string(), "commit-head".to_string(), 0)]
        );
    }

    #[tokio::test]
    async fn entities_at_head_change_overrides_both_merge_parents() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change(
                    "change-left",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"left\"}",
                ),
                entity_change(
                    "change-right",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"right\"}",
                ),
                entity_change(
                    "change-resolved",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"resolved\"}",
                ),
                commit_change("commit-left-change", "commit-left", &["change-left"], &[]),
                commit_change(
                    "commit-right-change",
                    "commit-right",
                    &["change-right"],
                    &[],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-resolved"],
                    &["commit-left", "commit-right"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-resolved".to_string(), "commit-head".to_string(), 0)]
        );
    }

    #[tokio::test]
    async fn entities_at_distinguishes_same_entity_with_different_file_id() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change_with_file(
                    "change-file-a",
                    "entity-1",
                    "test_schema",
                    Some("file-a"),
                    "{\"value\":\"file-a\"}",
                ),
                entity_change_with_file(
                    "change-file-b",
                    "entity-1",
                    "test_schema",
                    Some("file-b"),
                    "{\"value\":\"file-b\"}",
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-file-a", "change-file-b"],
                    &[],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![
                ("change-file-b".to_string(), "commit-head".to_string(), 0),
                ("change-file-a".to_string(), "commit-head".to_string(), 0),
            ]
        );
    }

    #[tokio::test]
    async fn entities_at_uses_latest_change_for_same_entity_identity() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change_with_file(
                    "change-entity-a",
                    "entity-1",
                    "test_schema",
                    None,
                    "{\"value\":\"a\"}",
                ),
                entity_change_with_file(
                    "change-entity-b",
                    "entity-1",
                    "test_schema",
                    None,
                    "{\"value\":\"b\"}",
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-entity-a", "change-entity-b"],
                    &[],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");
        let entity = entities
            .iter()
            .find(|entity| entity.change.schema_key == "test_schema")
            .expect("entity should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-entity-b".to_string(), "commit-head".to_string(), 0)]
        );
        assert!(entity.change.snapshot_ref.is_some());
    }

    #[tokio::test]
    async fn entities_at_head_tombstone_hides_parent_entity() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                entity_change(
                    "change-created",
                    "entity-1",
                    "test_schema",
                    "{\"value\":\"created\"}",
                ),
                entity_tombstone("change-deleted", "entity-1", "test_schema"),
                commit_change(
                    "commit-root-change",
                    "commit-root",
                    &["change-created"],
                    &[],
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-deleted"],
                    &["commit-root"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");
        let entity = entities
            .iter()
            .find(|entity| entity.change.schema_key == "test_schema")
            .expect("tombstone entity should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-deleted".to_string(), "commit-head".to_string(), 0)]
        );
        assert_eq!(entity.change.snapshot_ref, None);
    }

    #[tokio::test]
    async fn entities_at_includes_reachable_commit_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-head-change", "commit-head", &[], &["commit-root"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, super::COMMIT_SCHEMA_KEY),
            vec![
                (
                    "commit-head-change".to_string(),
                    "commit-head".to_string(),
                    0
                ),
                (
                    "commit-root-change".to_string(),
                    "commit-root".to_string(),
                    1
                ),
            ]
        );
    }

    #[tokio::test]
    async fn entities_at_errors_on_missing_member_change() {
        let backend = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend.clone());
        append_changes(
            storage.clone(),
            &[commit_change(
                "commit-head-change",
                "commit-head",
                &["missing-change"],
                &[],
            )],
        )
        .await;

        let graph = CommitGraphContext::new();
        let mut reader = graph.reader(storage);
        let error = reader
            .entities_at("commit-head")
            .await
            .expect_err("missing member change should fail");

        assert!(error.message.contains("missing-change"));
    }

    #[derive(Clone)]
    struct TestChange {
        change: Change,
        commit_change_ids: Vec<String>,
        parent_commit_ids: Vec<String>,
        author_account_ids: Vec<String>,
    }

    impl TestChange {
        fn commit(
            change_id: &str,
            commit_id: &str,
            change_ids: &[&str],
            parent_commit_ids: &[&str],
        ) -> Self {
            Self {
                change: Change {
                    id: change_id.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(commit_id),
                    schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot_ref: None,
                    metadata_ref: None,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                },
                commit_change_ids: change_ids.iter().map(|id| id.to_string()).collect(),
                parent_commit_ids: parent_commit_ids.iter().map(|id| id.to_string()).collect(),
                author_account_ids: Vec::new(),
            }
        }

        fn entity(
            change_id: &str,
            entity_id: &str,
            schema_key: &str,
            file_id: Option<&str>,
            snapshot_content: Option<&str>,
            created_at: &str,
        ) -> Self {
            Self {
                change: Change {
                    id: change_id.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                    snapshot_ref: snapshot_content.map(|content| {
                        crate::json_store::JsonRef::from_hash(blake3::hash(content.as_bytes()))
                    }),
                    metadata_ref: None,
                    created_at: created_at.to_string(),
                },
                commit_change_ids: Vec::new(),
                parent_commit_ids: Vec::new(),
                author_account_ids: Vec::new(),
            }
        }

        fn is_commit(&self) -> bool {
            self.change.schema_key == super::COMMIT_SCHEMA_KEY
        }
    }

    async fn append_changes(storage: StorageContext, changes: &[TestChange]) {
        let mut tx = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_changes = changes
            .iter()
            .filter(|change| !change.is_commit())
            .map(|change| change.change.clone())
            .collect::<Vec<_>>();
        let changes_by_id: BTreeMap<&str, &Change> = canonical_changes
            .iter()
            .map(|change| (change.id.as_str(), change))
            .collect::<BTreeMap<_, _>>();
        let mut authored_change_ids = BTreeSet::new();
        let commit_store = CommitStoreContext::new();
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit = crate::commit_graph::CommitGraphCommit {
                canonical_change: change.change.clone(),
                change: change.change.clone(),
                commit_id: change
                    .change
                    .entity_id
                    .as_single_string()
                    .expect("commit fixture should use single entity id")
                    .to_string(),
                change_ids: change.commit_change_ids.clone(),
                author_account_ids: change.author_account_ids.clone(),
                parent_commit_ids: change.parent_commit_ids.clone(),
            };
            let parent_commit_ids = commit.parent_commit_ids.clone();
            let author_account_ids = commit.author_account_ids.clone();
            let commit_draft = CommitDraftRef {
                id: &commit.commit_id,
                change_id: &commit.canonical_change.id,
                parent_ids: &parent_commit_ids,
                author_account_ids: &author_account_ids,
                created_at: &commit.canonical_change.created_at,
            };

            let mut authored_changes = Vec::new();
            let mut adopted_changes = Vec::new();
            let mut corrupt_missing_members = Vec::new();
            for change_id in &commit.change_ids {
                if let Some(change) = changes_by_id.get(change_id.as_str()) {
                    if authored_change_ids.insert(change_id.clone()) {
                        authored_changes.push(change_ref_from_canonical(change.as_ref()));
                    } else {
                        adopted_changes.push(change_ref_from_canonical(change.as_ref()));
                    }
                } else {
                    corrupt_missing_members.push(change_id.clone());
                }
            }

            if corrupt_missing_members.is_empty() {
                commit_store
                    .writer(tx.as_mut(), &mut writes)
                    .stage_commit_draft(commit_draft, authored_changes, adopted_changes)
                    .await
                    .expect("commit-store append should succeed");
            } else {
                crate::commit_store::storage::stage_commit(
                    &mut writes,
                    commit_draft,
                    authored_changes,
                    corrupt_missing_members
                        .into_iter()
                        .map(|change_id| ChangeLocator {
                            source_commit_id: "missing-source-commit".to_string(),
                            source_pack_id: 0,
                            source_ordinal: 0,
                            change_id,
                        })
                        .collect(),
                )
                .expect("corrupt commit-store fixture should stage");
            }
        }
        writes
            .apply(&mut tx.as_mut())
            .await
            .expect("writes should apply");
        tx.commit().await.expect("commit should succeed");
    }

    fn change_ref_from_canonical<'a>(change: crate::commit_store::ChangeRef<'a>) -> ChangeRef<'a> {
        ChangeRef {
            id: change.id,
            entity_id: change.entity_id,
            schema_key: change.schema_key,
            file_id: change.file_id,
            snapshot_ref: change.snapshot_ref,
            metadata_ref: change.metadata_ref,
            created_at: change.created_at,
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
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> crate::commit_graph::CommitGraphCommit {
        let fixture = commit_change(
            &format!("{commit_id}-change"),
            commit_id,
            change_ids,
            parent_commit_ids,
        );
        crate::commit_graph::CommitGraphCommit {
            canonical_change: fixture.change.clone(),
            change: fixture.change,
            commit_id: commit_id.to_string(),
            change_ids: change_ids
                .iter()
                .map(|change_id| change_id.to_string())
                .collect(),
            author_account_ids: Vec::new(),
            parent_commit_ids: parent_commit_ids
                .iter()
                .map(|parent_id| parent_id.to_string())
                .collect(),
        }
    }

    fn entity_change(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> TestChange {
        entity_change_at(
            change_id,
            entity_id,
            schema_key,
            snapshot_content,
            "2026-01-01T00:00:00Z",
        )
    }

    fn entity_change_at(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: &str,
        created_at: &str,
    ) -> TestChange {
        TestChange::entity(
            change_id,
            entity_id,
            schema_key,
            None,
            Some(snapshot_content),
            created_at,
        )
    }

    fn entity_change_with_file(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
        snapshot_content: &str,
    ) -> TestChange {
        TestChange::entity(
            change_id,
            entity_id,
            schema_key,
            file_id,
            Some(snapshot_content),
            "2026-01-01T00:00:00Z",
        )
    }

    fn entity_tombstone(change_id: &str, entity_id: &str, schema_key: &str) -> TestChange {
        TestChange::entity(
            change_id,
            entity_id,
            schema_key,
            None,
            None,
            "2026-01-02T00:00:00Z",
        )
    }

    fn entity_ids_for_schema(
        entities: &[crate::commit_graph::CommitGraphEntity],
        schema_key: &str,
    ) -> Vec<(String, String, u32)> {
        entities
            .iter()
            .filter(|entity| entity.change.schema_key == schema_key)
            .map(|entity| {
                (
                    entity.change.id.clone(),
                    entity.source_commit_id.clone(),
                    entity.depth,
                )
            })
            .collect()
    }
}
