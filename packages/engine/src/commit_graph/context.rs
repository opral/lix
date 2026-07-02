#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_pass_by_ref_mut,
    clippy::unused_self
)]

use std::collections::BTreeSet;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeLoadRequest, ChangeRecord, ChangelogContext, ChangelogReader, CommitId,
    CommitLoadEntry, CommitLoadRequest, CommitProjection, CommitRecord, CommitScanRequest,
};
use crate::commit_graph::walker::{best_common_ancestors, walk_reachable_commits};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphCommit, CommitGraphEdge, CommitGraphReader, ReachableCommitGraphCommit,
};
use crate::entity_pk::EntityPk;
use crate::storage::StorageRead;

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
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        self.load_changelog_commit(commit_id).await
    }

    /// Loads every direct commit fact from the changelog.
    ///
    /// This is used by global commit surfaces where the caller wants the durable
    /// graph facts themselves, not reachability from a particular branch head.
    pub(crate) async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
        let mut commits = Vec::new();
        let mut start_after = None::<String>;
        loop {
            let mut reader = ChangelogContext::new().reader(&self.store);
            let scan = reader
                .scan_commits(CommitScanRequest {
                    start_after: start_after.as_deref(),
                    limit: Some(1024),
                    projection: CommitProjection::Record,
                })
                .await?;
            for entry in scan.entries {
                let CommitLoadEntry::Record(record) = entry else {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "changelog commit scan returned non-record entry",
                    ));
                };
                commits.push(commit_graph_commit_from_commit_record(record, Vec::new()));
            }
            let Some(next) = scan.next_start_after else {
                break;
            };
            start_after = Some(next.to_string());
        }
        commits.sort_by_key(|left| left.commit_id);
        Ok(commits)
    }

    /// Walks from `head_commit_id` through parent commits and records nearest depth.
    pub(crate) async fn reachable_commits(
        &mut self,
        head_commit_id: &CommitId,
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
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
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
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
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
                    .map(|ancestor| ancestor.commit_id.to_string())
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
                        parent_commit_id: *parent_commit_id,
                        child_commit_id: commit.commit_id,
                        parent_order: parent_order as u32,
                    },
                )
            })
            .collect()
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
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
        let commits = self.reachable_commits(start_commit_id).await?;
        let mut entries = Vec::new();
        let mut seen_change_ids = BTreeSet::new();

        for reachable in commits {
            if !depth_matches(reachable.depth, request) {
                continue;
            }

            let commit_id = reachable.commit.commit_id;
            let canonical_change = reachable.commit.canonical_change;
            if seen_change_ids.insert(canonical_change.id)
                && change_matches_history_request(&canonical_change, request)
            {
                entries.push(CommitGraphChangeHistoryEntry {
                    change: canonical_change,
                    observed_commit_id: commit_id,
                    start_commit_id: *start_commit_id,
                    depth: reachable.depth,
                });
            }

            for change_id in reachable.commit.change_ids {
                if !seen_change_ids.insert(change_id) {
                    continue;
                }
                let change = self.load_member_canonical_change(&change_id).await?;
                if change_matches_history_request(&change, request) {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change,
                        observed_commit_id: commit_id,
                        start_commit_id: *start_commit_id,
                        depth: reachable.depth,
                    });
                }
            }
        }

        Ok(entries)
    }

    async fn load_member_canonical_change(
        &mut self,
        change_id: &ChangeId,
    ) -> Result<CommitGraphChange, LixError> {
        self.load_canonical_changes(std::slice::from_ref(change_id))
            .await?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("commit_graph references missing change '{change_id}'"),
                )
            })
    }

    async fn load_changelog_commit(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        let mut reader = ChangelogContext::new().reader(&self.store);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: std::slice::from_ref(commit_id),
                projection: CommitProjection::Full,
            })
            .await?;
        let Some(entry) = batch.entries.into_iter().next().flatten() else {
            return Ok(None);
        };
        match entry {
            CommitLoadEntry::Full {
                record,
                change_ref_chunks,
            } => {
                let change_ids = change_ref_chunks
                    .into_iter()
                    .flat_map(|chunk| chunk.entries)
                    .collect::<Vec<_>>();
                Ok(Some(commit_graph_commit_from_commit_record(
                    record, change_ids,
                )))
            }
            CommitLoadEntry::Record(_) => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "changelog full commit projection returned non-full entry",
            )),
        }
    }

    async fn load_canonical_changes(
        &self,
        change_ids: &[ChangeId],
    ) -> Result<Vec<Option<CommitGraphChange>>, LixError> {
        let mut reader = ChangelogContext::new().reader(&self.store);
        let batch = reader
            .load_changes(ChangeLoadRequest { change_ids })
            .await?;
        batch
            .entries
            .into_iter()
            .map(|entry| Ok(entry.map(commit_graph_change_from_change_record)))
            .collect()
    }
}

fn commit_graph_change_from_change_record(change: ChangeRecord) -> CommitGraphChange {
    CommitGraphChange {
        id: change.change_id,
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot: change.snapshot,
        metadata: change.metadata,
        created_at: change.created_at,
        origin_key: change.origin_key,
    }
}

#[async_trait::async_trait]
impl<S> CommitGraphReader for CommitGraphStoreReader<S>
where
    S: StorageRead + Send + Sync,
{
    async fn load_commit(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        Self::load_commit(self, commit_id).await
    }

    async fn reachable_commits(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
        Self::reachable_commits(self, head_commit_id).await
    }

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
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

fn commit_graph_commit_from_commit_record(
    record: CommitRecord,
    change_ids: Vec<ChangeId>,
) -> CommitGraphCommit {
    let change = commit_record_canonical_change(&record);
    CommitGraphCommit {
        canonical_change: change.clone(),
        change,
        commit_id: record.commit_id,
        change_ids,
        author_account_ids: record.author_account_ids,
        parent_commit_ids: record.parent_commit_ids,
    }
}

fn commit_record_canonical_change(record: &CommitRecord) -> CommitGraphChange {
    let snapshot_content =
        crate::changelog::commit_row_snapshot_json(&record.commit_id.to_string())
            .expect("lix_commit snapshot serialization should not fail");
    CommitGraphChange {
        id: record.change_id,
        entity_pk: EntityPk::single(record.commit_id),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: record.created_at,
        origin_key: None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::changelog::{
        ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter,
        CommitChangeRefSet, CommitId, CommitRecord,
    };
    use crate::commit_graph::{
        CommitGraphChange, CommitGraphChangeHistoryRequest, CommitGraphContext,
    };
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};

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

    fn change_ids<const N: usize>(labels: [&str; N]) -> Vec<ChangeId> {
        labels.into_iter().map(change_id).collect()
    }

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
        let commit_1 = commit_id("commit-1");
        let commit = reader
            .load_commit(&commit_1)
            .await
            .expect("commit load should succeed")
            .expect("commit should exist");

        assert_eq!(commit.commit_id, commit_id("commit-1"));
        assert_eq!(commit.change_ids, change_ids(["change-1", "change-2"]));
        assert_eq!(commit.parent_commit_ids, commit_ids(["parent-1"]));
        assert_eq!(commit.change.id, change_id("commit-1-change"));
    }

    #[tokio::test]
    async fn load_commit_returns_none_for_missing_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let missing = commit_id("missing");

        let commit = reader
            .load_commit(&missing)
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
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            sorted_commit_ids(["commit-a", "commit-b"])
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
        let commit_head = commit_id("commit-head");
        let history = reader
            .change_history_from_commit(
                &commit_head,
                &CommitGraphChangeHistoryRequest {
                    entity_pks: vec![crate::entity_pk::EntityPk::single("entity-1")],
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
        assert_eq!(history[0].change.id, change_id("change-file-a"));
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

        assert!(hidden.is_empty());
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].change.id, change_id("change-deleted"));
    }

    #[derive(Clone)]
    struct TestChange {
        change: CommitGraphChange,
        commit_change_ids: Vec<ChangeId>,
        parent_commit_ids: Vec<CommitId>,
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
                change: CommitGraphChange {
                    id: ChangeId::for_test_label(change_id),
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
                author_account_ids: Vec::new(),
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
        let changes_by_id: BTreeMap<ChangeId, &TestChange> = canonical_changes
            .iter()
            .map(|change| (change.change.id, change))
            .collect::<BTreeMap<_, _>>();
        let mut authored_change_ids = BTreeSet::new();
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
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit_label = change
                .change
                .entity_pk
                .as_single_string()
                .expect("commit fixture should use single entity pk")
                .to_string();
            let commit = crate::commit_graph::CommitGraphCommit {
                canonical_change: change.change.clone(),
                change: change.change.clone(),
                commit_id: CommitId::for_test_label(&commit_label),
                change_ids: change.commit_change_ids.clone(),
                author_account_ids: change.author_account_ids.clone(),
                parent_commit_ids: change.parent_commit_ids.clone(),
            };
            for parent_commit_id in &change.parent_commit_ids {
                if !provided_commit_ids.contains(parent_commit_id)
                    && staged_commit_ids.insert(*parent_commit_id)
                {
                    append_empty_commit(&mut append, *parent_commit_id);
                }
            }
            let mut refs = Vec::new();
            for change_id in &commit.change_ids {
                if let Some(change) = changes_by_id.get(change_id) {
                    if authored_change_ids.insert(*change_id) {
                        append.changes.push(change_record_from_test_change(change));
                    }
                    refs.push(commit_change_ref_from_test_change(change));
                }
            }

            append.commits.push(CommitRecord {
                format_version: 1,
                commit_id: commit.commit_id,
                parent_commit_ids: commit.parent_commit_ids.clone(),
                change_id: commit.canonical_change.id,
                author_account_ids: commit.author_account_ids.clone(),
                created_at: commit.canonical_change.created_at,
            });
            append.commit_change_refs.push(CommitChangeRefSet {
                commit_id: commit.commit_id,
                entries: refs,
            });
            staged_commit_ids.insert(commit.commit_id);
        }
        writer
            .stage_append(append)
            .await
            .expect("changelog append should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("commit should succeed");
    }

    fn append_empty_commit(append: &mut ChangelogAppend, commit_id: CommitId) {
        let change_id = format!("{commit_id}-change");
        append.commits.push(CommitRecord {
            format_version: 1,
            commit_id,
            parent_commit_ids: Vec::new(),
            change_id: ChangeId::for_test_label(&change_id),
            author_account_ids: Vec::new(),
            created_at: ts("2026-01-01T00:00:00Z"),
        });
        append.commit_change_refs.push(CommitChangeRefSet {
            commit_id,
            entries: Vec::new(),
        });
    }

    fn change_record_from_test_change(change: &TestChange) -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: change.change.id,
            entity_pk: change.change.entity_pk.clone(),
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            snapshot: change.change.snapshot.clone(),
            metadata: change.change.metadata.clone(),
            created_at: change.change.created_at,
            origin_key: change.change.origin_key.clone(),
        }
    }

    fn commit_change_ref_from_test_change(change: &TestChange) -> ChangeId {
        change.change.id
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
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> crate::commit_graph::CommitGraphCommit {
        let commit_id = CommitId::for_test_label(commit_label);
        let fixture = commit_change(
            &format!("{commit_label}-change"),
            commit_label,
            change_ids,
            parent_commit_ids,
        );
        let mut change = fixture.change;
        change.entity_pk = crate::entity_pk::EntityPk::single(&commit_id);
        crate::commit_graph::CommitGraphCommit {
            canonical_change: change.clone(),
            change,
            commit_id,
            change_ids: change_ids
                .iter()
                .map(|change_id| ChangeId::for_test_label(change_id))
                .collect(),
            author_account_ids: Vec::new(),
            parent_commit_ids: parent_commit_ids
                .iter()
                .map(|parent_id| CommitId::for_test_label(parent_id))
                .collect(),
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
