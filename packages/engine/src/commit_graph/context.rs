use std::collections::BTreeSet;

use crate::changelog::{
    ChangeLoadRequest, ChangeRecord, ChangelogContext, ChangelogReader, CommitLoadEntry,
    CommitLoadRequest, CommitProjection, CommitRecord, CommitScanRequest,
};
use crate::commit_graph::walker::{best_common_ancestors, walk_reachable_commits};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphCommit, CommitGraphEdge, CommitGraphReader, ReachableCommitGraphCommit,
};
use crate::entity_pk::EntityPk;
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
        self.load_changelog_commit(commit_id).await
    }

    /// Loads every direct commit fact from the changelog.
    ///
    /// This is used by global commit surfaces where the caller wants the durable
    /// graph facts themselves, not reachability from a particular version head.
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
            start_after = Some(next);
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
    /// This is the primitive history API. It reports the commit/depth where a
    /// reachable commit's change-ref set first exposes each matching canonical
    /// change during graph traversal and leaves row shaping to callers such as
    /// SQL providers.
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
            let canonical_change = reachable.commit.canonical_change;
            if seen_change_ids.insert(canonical_change.id.clone())
                && change_matches_history_request(&canonical_change, request)
            {
                entries.push(CommitGraphChangeHistoryEntry {
                    change: canonical_change,
                    observed_commit_id: commit_id.clone(),
                    start_commit_id: start_commit_id.to_string(),
                    depth: reachable.depth,
                });
            }

            for change_id in reachable.commit.change_ids {
                if !seen_change_ids.insert(change_id.clone()) {
                    continue;
                }
                let change = self.load_member_canonical_change(&change_id).await?;
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
    ) -> Result<CommitGraphChange, LixError> {
        let change_ids = vec![change_id.to_string()];
        self.load_canonical_changes(&change_ids)
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
        commit_id: &str,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        let mut reader = ChangelogContext::new().reader(&self.store);
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &[commit_id.to_string()],
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
                    .flat_map(|chunk| chunk.entries.into_iter().map(|entry| entry.change_id))
                    .collect::<Vec<_>>();
                Ok(Some(commit_graph_commit_from_commit_record(
                    record, change_ids,
                )))
            }
            _ => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "changelog full commit projection returned non-full entry",
            )),
        }
    }

    async fn load_canonical_changes(
        &self,
        change_ids: &[String],
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
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at: change.created_at,
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

    async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
        CommitGraphStoreReader::reachable_commits(self, head_commit_id).await
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
    change: &CommitGraphChange,
    request: &CommitGraphChangeHistoryRequest,
) -> bool {
    (request.include_tombstones || change.snapshot_ref.is_some())
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
    change_ids: Vec<String>,
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
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": record.commit_id,
    }))
    .expect("lix_commit snapshot serialization should not fail");
    CommitGraphChange {
        id: record.change_id.clone(),
        entity_pk: EntityPk::single(&record.commit_id),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_ref: Some(crate::json_store::JsonRef::for_content(
            snapshot_content.as_bytes(),
        )),
        metadata_ref: None,
        created_at: record.created_at.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::changelog::{
        ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRef,
        CommitChangeRefSet, CommitRecord,
    };
    use crate::commit_graph::{
        CommitGraphChange, CommitGraphChangeHistoryRequest, CommitGraphContext,
    };
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
        assert_eq!(history[0].change.id, "change-file-a");
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
            .change_history_from_commit(
                "commit-head",
                &CommitGraphChangeHistoryRequest {
                    schema_keys: vec!["test_schema".to_string()],
                    ..CommitGraphChangeHistoryRequest::default()
                },
            )
            .await
            .expect("history should resolve");
        let visible = reader
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

        assert!(hidden.is_empty());
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].change.id, "change-deleted");
    }

    #[derive(Clone)]
    struct TestChange {
        change: CommitGraphChange,
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
                change: CommitGraphChange {
                    id: change_id.to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single(commit_id),
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
            entity_pk: &str,
            schema_key: &str,
            file_id: Option<&str>,
            snapshot_content: Option<&str>,
            created_at: &str,
        ) -> Self {
            Self {
                change: CommitGraphChange {
                    id: change_id.to_string(),
                    entity_pk: crate::entity_pk::EntityPk::single(entity_pk),
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
                    .entity_pk
                    .as_single_string()
                    .expect("commit fixture should use single entity pk")
                    .to_string()
            })
            .collect::<BTreeSet<_>>();
        let mut staged_commit_ids = BTreeSet::new();
        let changelog = ChangelogContext::new();
        let mut writer = changelog.writer(&mut read, &mut writes);
        let mut append = ChangelogAppend::default();
        for change in changes.iter().filter(|change| change.is_commit()) {
            let commit = crate::commit_graph::CommitGraphCommit {
                canonical_change: change.change.clone(),
                change: change.change.clone(),
                commit_id: change
                    .change
                    .entity_pk
                    .as_single_string()
                    .expect("commit fixture should use single entity pk")
                    .to_string(),
                change_ids: change.commit_change_ids.clone(),
                author_account_ids: change.author_account_ids.clone(),
                parent_commit_ids: change.parent_commit_ids.clone(),
            };
            for parent_commit_id in &commit.parent_commit_ids {
                if !provided_commit_ids.contains(parent_commit_id)
                    && staged_commit_ids.insert(parent_commit_id.clone())
                {
                    append_empty_commit(&mut append, parent_commit_id);
                }
            }
            let mut refs = Vec::new();
            for change_id in &commit.change_ids {
                if let Some(change) = changes_by_id.get(change_id.as_str()) {
                    if authored_change_ids.insert(change_id.clone()) {
                        append.changes.push(change_record_from_test_change(change));
                    }
                    refs.push(commit_change_ref_from_test_change(change));
                }
            }

            append.commits.push(CommitRecord {
                format_version: 1,
                commit_id: commit.commit_id.clone(),
                parent_commit_ids: commit.parent_commit_ids.clone(),
                change_id: commit.canonical_change.id.clone(),
                author_account_ids: commit.author_account_ids.clone(),
                created_at: commit.canonical_change.created_at.clone(),
            });
            append.commit_change_refs.push(CommitChangeRefSet {
                commit_id: commit.commit_id.clone(),
                entries: refs,
            });
            staged_commit_ids.insert(commit.commit_id.clone());
        }
        writer
            .stage_append(append)
            .await
            .expect("changelog append should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("commit should succeed");
    }

    fn append_empty_commit(append: &mut ChangelogAppend, commit_id: &str) {
        let change_id = format!("{commit_id}-change");
        append.commits.push(CommitRecord {
            format_version: 1,
            commit_id: commit_id.to_string(),
            parent_commit_ids: Vec::new(),
            change_id: change_id.clone(),
            author_account_ids: Vec::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        });
        append.commit_change_refs.push(CommitChangeRefSet {
            commit_id: commit_id.to_string(),
            entries: Vec::new(),
        });
    }

    fn change_record_from_test_change(change: &TestChange) -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: change.change.id.clone(),
            entity_pk: change.change.entity_pk.clone(),
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            snapshot_ref: change.change.snapshot_ref,
            metadata_ref: change.change.metadata_ref,
            created_at: change.change.created_at.clone(),
        }
    }

    fn commit_change_ref_from_test_change(change: &TestChange) -> CommitChangeRef {
        CommitChangeRef {
            schema_key: change.change.schema_key.clone(),
            file_id: change.change.file_id.clone(),
            entity_pk: change.change.entity_pk.clone(),
            change_id: change.change.id.clone(),
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
