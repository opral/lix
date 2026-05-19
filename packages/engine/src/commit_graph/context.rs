use std::collections::BTreeSet;

use crate::changelog::{
    Change, ChangeLoadEntry, ChangeLoadRequest, ChangeProjection, ChangeVisibilityMode,
    ChangelogContext, CommitBody, CommitHeader, CommitLoadEntry, CommitLoadRequest,
    CommitProjection, CommitVisibilityMode, SegmentChange,
};
use crate::commit_graph::walker::{best_common_ancestors, walk_reachable_commits};
use crate::commit_graph::{
    CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
    CommitGraphEdge, CommitGraphReader, LocatedChange, ReachableCommitGraphCommit,
};
use crate::entity_identity::EntityIdentity;
use crate::storage::StorageRead;
use crate::LixError;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

/// Read model for resolving changelog commits into entity state at a head.
///
/// This module does not own durable storage. It reads immutable changelog
/// facts through a caller-provided KV store and applies commit graph rules on
/// top.
#[derive(Clone)]
pub(crate) struct CommitGraphContext;

impl CommitGraphContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a graph reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommitGraphStoreReader<S>
    where
        S: StorageRead + Send + Sync,
    {
        CommitGraphStoreReader { store }
    }
}

/// Commit-graph reader that resolves changelog entities at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    store: S,
}

impl<S> CommitGraphStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    /// Loads and parses a `lix_commit` canonical change by commit id.
    pub(crate) async fn load_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        self.load_visible_commit(commit_id)
            .await?
            .map(|(header, body)| self.graph_commit_from_changelog_commit(header, body))
            .transpose()
    }

    /// Loads every visible commit fact from the changelog.
    ///
    /// This is used by global commit surfaces where the caller wants the durable
    /// graph facts themselves, not reachability from a particular version head.
    pub(crate) async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
        let commit_ids = {
            let mut reader = ChangelogContext::new().reader(&self.store);
            reader
                .scan_commit_visibilities()
                .await?
                .into_iter()
                .map(|visibility| visibility.commit_id)
                .collect::<Vec<_>>()
        };
        let mut commits = Vec::new();
        for commit_id in commit_ids {
            if let Some((header, body)) = self.load_visible_commit(&commit_id).await? {
                commits.push(self.graph_commit_from_changelog_commit(header, body)?);
            }
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
                if change_matches_history_request(&change.record, request) {
                    entries.push(CommitGraphChangeHistoryEntry {
                        located_change: change,
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
    ) -> Result<LocatedChange, LixError> {
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

    async fn load_visible_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<(CommitHeader, CommitBody)>, LixError> {
        let mut reader = ChangelogContext::new().reader(&self.store);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &[commit_id.to_string()],
                projection: CommitProjection::Full,
                visibility: CommitVisibilityMode::RequireVisible,
            })
            .await?;
        let Some(entry) = batch.entries.into_iter().next().flatten() else {
            return Ok(None);
        };
        match entry {
            CommitLoadEntry::Full { header, body } => Ok(Some((header, body))),
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "changelog full commit projection returned non-full entry",
            )),
        }
    }

    fn graph_commit_from_changelog_commit(
        &mut self,
        header: CommitHeader,
        body: CommitBody,
    ) -> Result<CommitGraphCommit, LixError> {
        let change_ids = body
            .membership
            .into_iter()
            .map(|membership| membership.member_change_id)
            .collect::<Vec<_>>();
        Ok(commit_graph_commit_from_changelog_commit(
            header, change_ids,
        ))
    }

    async fn load_canonical_changes(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<LocatedChange>>, LixError> {
        let mut reader = ChangelogContext::new().reader(&self.store);
        let batch = reader
            .load_changes(ChangeLoadRequest {
                change_ids,
                projection: ChangeProjection::Segment,
                visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await?;
        batch
            .entries
            .into_iter()
            .map(|entry| match entry {
                Some(ChangeLoadEntry::Segment(change)) => {
                    let source_commit_id = change.authored_commit_id.clone().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("changelog change '{}' has no authored commit", change.id),
                        )
                    })?;
                    Ok(Some(LocatedChange {
                        record: logical_change_from_segment_change(&change),
                        source_commit_id,
                        inline_payloads: change.inline_payloads,
                    }))
                }
                Some(_) => Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "changelog segment change projection returned non-segment entry",
                )),
                None => Ok(None),
            })
            .collect()
    }
}

fn logical_change_from_segment_change(change: &SegmentChange) -> Change {
    Change {
        id: change.id.clone(),
        authored_commit_id: change.authored_commit_id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at: change.created_at.clone(),
    }
}

#[async_trait::async_trait]
impl<S> CommitGraphReader for CommitGraphStoreReader<S>
where
    S: StorageRead + Send + Sync,
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

fn commit_graph_commit_from_changelog_commit(
    header: CommitHeader,
    change_ids: Vec<String>,
) -> CommitGraphCommit {
    let change = commit_header_canonical_change(&header);
    CommitGraphCommit {
        canonical_change: change.clone(),
        change,
        commit_id: header.id,
        change_ids,
        author_account_ids: header.author_account_ids,
        parent_commit_ids: header.parent_commit_ids,
    }
}

fn commit_header_canonical_change(header: &CommitHeader) -> Change {
    Change {
        id: header.derivable_change_id.clone(),
        authored_commit_id: Some(header.id.clone()),
        entity_id: EntityIdentity::single(&header.id),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_ref: None,
        metadata_ref: None,
        created_at: header.created_at.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::changelog::{
        Change, ChangelogContext, CommitBody, CommitHeader, MembershipRecord, MembershipRole,
        Segment, SegmentChange, SegmentChangeDirectory, SegmentCommit, SegmentCommitDirectory,
        SegmentDirectory, SegmentHeader, SegmentInlinePayload,
    };
    use crate::commit_graph::{CommitGraphChangeHistoryRequest, CommitGraphContext};
    use crate::json_store::JsonRef;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};

    #[tokio::test]
    async fn load_commit_parses_commit_snapshot() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            .expect("read should open");
        let mut reader = graph.reader(read);
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);

        let commit = reader
            .load_commit("missing")
            .await
            .expect("commit load should succeed");

        assert_eq!(commit, None);
    }

    #[tokio::test]
    async fn all_commits_returns_parsed_commits_sorted_by_id() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            .expect("read should open");
        let mut reader = graph.reader(read);
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
    async fn commit_edges_are_derived_from_parent_commit_ids() {
        let graph = CommitGraphContext::new();
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let reader = graph.reader(read);
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            .expect("read should open");
        let mut reader = graph.reader(read);
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
                    entry.located_change.record.id.as_str(),
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
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
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
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
        assert_eq!(history[0].located_change.record.id, "change-file-a");
        assert_eq!(history[0].depth, 1);
    }

    #[tokio::test]
    async fn change_history_from_commit_includes_tombstones_when_requested() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            .expect("read should open");
        let mut reader = graph.reader(read);
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
        assert_eq!(visible[0].located_change.record.id, "change-deleted");
    }

    #[derive(Clone)]
    struct TestChange {
        change: Change,
        snapshot_content: Option<String>,
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
                    authored_commit_id: Some(commit_id.to_string()),
                    entity_id: crate::entity_identity::EntityIdentity::single(commit_id),
                    schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot_ref: None,
                    metadata_ref: None,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                },
                snapshot_content: None,
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
                    authored_commit_id: None,
                    entity_id: crate::entity_identity::EntityIdentity::single(entity_id),
                    schema_key: schema_key.to_string(),
                    file_id: file_id.map(str::to_string),
                    snapshot_ref: snapshot_content.map(|content| {
                        crate::json_store::JsonRef::from_hash(blake3::hash(content.as_bytes()))
                    }),
                    metadata_ref: None,
                    created_at: created_at.to_string(),
                },
                snapshot_content: snapshot_content.map(str::to_string),
                commit_change_ids: Vec::new(),
                parent_commit_ids: Vec::new(),
                author_account_ids: Vec::new(),
            }
        }

        fn is_commit(&self) -> bool {
            self.change.schema_key == super::COMMIT_SCHEMA_KEY
        }
    }

    async fn append_changes(storage: &StorageContext, changes: &[TestChange]) {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let canonical_changes = changes
            .iter()
            .filter(|change| !change.is_commit())
            .cloned()
            .collect::<Vec<_>>();
        let changes_by_id: BTreeMap<&str, &TestChange> = canonical_changes
            .iter()
            .map(|change| (change.change.id.as_str(), change))
            .collect::<BTreeMap<_, _>>();
        let mut authored_change_ids = BTreeSet::new();
        let provided_commit_ids = changes
            .iter()
            .filter(|change| change.is_commit())
            .map(|change| {
                change
                    .change
                    .entity_id
                    .as_single_string()
                    .expect("commit fixture should use single entity id")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        let mut staged_commit_ids = BTreeSet::new();
        let changelog = ChangelogContext::new();
        let mut writer = changelog.writer(&mut read, &mut writes);
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
            for parent_commit_id in &commit.parent_commit_ids {
                if !provided_commit_ids.contains(parent_commit_id)
                    && staged_commit_ids.insert(parent_commit_id.clone())
                {
                    stage_empty_commit(&mut writer, parent_commit_id)
                        .await
                        .expect("implicit parent commit should stage");
                }
            }
            let mut segment_changes = Vec::new();
            let mut membership = Vec::new();
            for change_id in &commit.change_ids {
                if let Some(change) = changes_by_id.get(change_id.as_str()) {
                    if authored_change_ids.insert(change_id.clone()) {
                        segment_changes
                            .push(segment_change_from_test_change(change, &commit.commit_id));
                        membership.push(MembershipRecord {
                            member_change_id: change_id.clone(),
                            role: MembershipRole::Authored,
                            source_parent_ordinal: None,
                        });
                    } else {
                        membership.push(MembershipRecord {
                            member_change_id: change_id.clone(),
                            role: MembershipRole::Adopted,
                            source_parent_ordinal: Some(0),
                        });
                    }
                }
            }

            let membership_ordinals = membership
                .iter()
                .enumerate()
                .map(|(index, member)| (member.member_change_id.clone(), index as u32))
                .collect();
            let state_row_identities = membership
                .iter()
                .filter_map(|member| {
                    changes_by_id
                        .get(member.member_change_id.as_str())
                        .map(|change| {
                            state_row_identity_for_test_change(change)
                                .map(|identity| (identity, member.member_change_id.clone()))
                        })
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("test change should have a valid state row identity");
            let segment_id = format!("segment-{}", commit.commit_id);
            let segment = Segment {
                header: SegmentHeader {
                    segment_id,
                    format_version: 1,
                    commit_count: 1,
                    change_count: segment_changes.len() as u32,
                    byte_count: 0,
                    payload_count: segment_changes
                        .iter()
                        .map(|change| change.inline_payloads.len() as u32)
                        .sum(),
                    checksum: String::new(),
                },
                directory: SegmentDirectory::default(),
                commits: vec![SegmentCommit {
                    header: CommitHeader {
                        id: commit.commit_id.clone(),
                        parent_commit_ids: commit.parent_commit_ids.clone(),
                        derivable_change_id: commit.canonical_change.id.clone(),
                        author_account_ids: commit.author_account_ids.clone(),
                        created_at: commit.canonical_change.created_at.clone(),
                        membership_count: membership.len() as u32,
                    },
                    body: CommitBody { membership },
                    directory: SegmentCommitDirectory {
                        state_row_identities,
                        membership_ordinals,
                    },
                    checksum: String::new(),
                }],
                changes: segment_changes,
            };
            writer
                .stage_segment(segment)
                .await
                .expect("changelog segment should stage");
            writer
                .stage_publish_commit(&commit.commit_id)
                .await
                .expect("changelog commit should publish");
            staged_commit_ids.insert(commit.commit_id.clone());
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("commit should succeed");
    }

    async fn stage_empty_commit<S>(
        writer: &mut crate::changelog::ChangelogStoreWriter<'_, S>,
        commit_id: &str,
    ) -> Result<(), crate::LixError>
    where
        S: crate::storage::StorageRead + Send,
    {
        writer
            .stage_segment(Segment {
                header: SegmentHeader {
                    segment_id: format!("segment-{commit_id}"),
                    format_version: 1,
                    commit_count: 1,
                    change_count: 0,
                    byte_count: 0,
                    payload_count: 0,
                    checksum: String::new(),
                },
                directory: SegmentDirectory::default(),
                commits: vec![SegmentCommit {
                    header: CommitHeader {
                        id: commit_id.to_string(),
                        parent_commit_ids: Vec::new(),
                        derivable_change_id: format!("{commit_id}-change"),
                        author_account_ids: Vec::new(),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        membership_count: 0,
                    },
                    body: CommitBody::default(),
                    directory: SegmentCommitDirectory::default(),
                    checksum: String::new(),
                }],
                changes: Vec::new(),
            })
            .await?;
        writer.stage_publish_commit(commit_id).await
    }

    fn segment_change_from_test_change(change: &TestChange, commit_id: &str) -> SegmentChange {
        let inline_payloads = change
            .snapshot_content
            .iter()
            .map(|content| SegmentInlinePayload {
                json_ref: JsonRef::for_content(content.as_bytes()),
                bytes: content.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>();
        SegmentChange {
            id: change.change.id.clone(),
            authored_commit_id: Some(commit_id.to_string()),
            entity_id: change.change.entity_id.clone(),
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            snapshot_ref: change.change.snapshot_ref,
            metadata_ref: change.change.metadata_ref,
            created_at: change.change.created_at.clone(),
            inline_payloads,
            directory: SegmentChangeDirectory::default(),
        }
    }

    fn state_row_identity_for_test_change(
        change: &TestChange,
    ) -> Result<crate::changelog::StateRowIdentity, crate::LixError> {
        Ok(crate::changelog::StateRowIdentity {
            schema_key: crate::common::CanonicalSchemaKey::new(change.change.schema_key.clone())?,
            file_id: crate::common::FileId::new(
                change
                    .change
                    .file_id
                    .clone()
                    .unwrap_or_else(|| "__global__".to_string()),
            )?,
            entity_id: crate::common::EntityId::new(change.change.entity_id.as_json_array_text()?)?,
        })
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
}
