use std::collections::{BTreeMap, BTreeSet};

use crate::commit_graph::{CommitGraphCommit, CommitGraphStoreReader, ReachableCommitGraphCommit};
use crate::storage::StorageRead;
use crate::LixError;

/// Walks parent links from `head_commit_id` and returns reachable commits
/// nearest-first.
///
/// The walker is intentionally storage-free. It asks `CommitGraphReader` to
/// load parsed commit facts and owns only traversal concerns: caching, cycle
/// detection, and nearest-depth selection.
pub(crate) async fn walk_reachable_commits<S>(
    reader: &mut CommitGraphStoreReader<S>,
    head_commit_id: &str,
) -> Result<Vec<ReachableCommitGraphCommit>, LixError>
where
    S: StorageRead + Send + Sync,
{
    let mut loader = CommitTraversalLoader::new(reader);
    let mut visiting = BTreeSet::new();
    let mut nearest_depths = BTreeMap::new();
    loader
        .walk_commit(head_commit_id, 0, &mut visiting, &mut nearest_depths)
        .await?;

    let mut commits = nearest_depths
        .into_iter()
        .map(|(commit_id, depth)| {
            let commit = loader
                .loaded
                .remove(&commit_id)
                .expect("visited commit should be cached");
            ReachableCommitGraphCommit { commit, depth }
        })
        .collect::<Vec<_>>();
    commits.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| left.commit.commit_id.cmp(&right.commit.commit_id))
    });
    Ok(commits)
}

/// Returns the best common ancestors shared by two commit heads.
///
/// This is graph math, not merge policy. A commit is "best" when it is a
/// common ancestor and no descendant of it is also a common ancestor.
///
/// Simple history has one best common ancestor:
///
/// ```text
/// A -- B -- C   left
///       \
///        D      right
/// ```
///
/// `best_common_ancestors(C, D)` returns `[B]`.
///
/// Commit history is a DAG, not a tree, so criss-cross histories can have
/// multiple equally good answers. Callers that need one merge base should wrap
/// this API with an explicit policy instead of pretending the graph always has
/// a single lowest common ancestor.
pub(crate) async fn best_common_ancestors<S>(
    reader: &mut CommitGraphStoreReader<S>,
    left_commit_id: &str,
    right_commit_id: &str,
) -> Result<Vec<CommitGraphCommit>, LixError>
where
    S: StorageRead + Send + Sync,
{
    let left_reachable = walk_reachable_commits(reader, left_commit_id).await?;
    let right_reachable = walk_reachable_commits(reader, right_commit_id).await?;
    let right_ids = right_reachable
        .iter()
        .map(|reachable| reachable.commit.commit_id.clone())
        .collect::<BTreeSet<_>>();
    let common_ids = left_reachable
        .iter()
        .filter(|reachable| right_ids.contains(&reachable.commit.commit_id))
        .map(|reachable| reachable.commit.commit_id.clone())
        .collect::<BTreeSet<_>>();

    let mut best = Vec::new();
    for reachable in left_reachable {
        let commit_id = &reachable.commit.commit_id;
        if !common_ids.contains(commit_id) {
            continue;
        }

        if has_descendant_in_set(reader, commit_id, &common_ids).await? {
            continue;
        }

        best.push(reachable.commit);
    }
    best.sort_by(|left, right| left.commit_id.cmp(&right.commit_id));
    Ok(best)
}

async fn has_descendant_in_set<S>(
    reader: &mut CommitGraphStoreReader<S>,
    commit_id: &str,
    candidate_descendant_ids: &BTreeSet<String>,
) -> Result<bool, LixError>
where
    S: StorageRead + Send + Sync,
{
    for candidate_descendant_id in candidate_descendant_ids {
        if candidate_descendant_id == commit_id {
            continue;
        }
        let reachable = walk_reachable_commits(reader, candidate_descendant_id).await?;
        if reachable
            .iter()
            .any(|reachable| reachable.commit.commit_id == commit_id)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

struct CommitTraversalLoader<'a, S>
where
    S: StorageRead + Send + Sync,
{
    reader: &'a mut CommitGraphStoreReader<S>,
    loaded: BTreeMap<String, CommitGraphCommit>,
}

impl<'a, S> CommitTraversalLoader<'a, S>
where
    S: StorageRead + Send + Sync,
{
    fn new(reader: &'a mut CommitGraphStoreReader<S>) -> Self {
        Self {
            reader,
            loaded: BTreeMap::new(),
        }
    }

    async fn walk_commit(
        &mut self,
        commit_id: &str,
        depth: u32,
        visiting: &mut BTreeSet<String>,
        nearest_depths: &mut BTreeMap<String, u32>,
    ) -> Result<(), LixError> {
        let mut stack = vec![TraversalFrame {
            commit_id: commit_id.to_string(),
            depth,
            expanded: false,
        }];

        while let Some(frame) = stack.pop() {
            if frame.expanded {
                visiting.remove(&frame.commit_id);
                continue;
            }

            if visiting.contains(&frame.commit_id) {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "commit_graph cycle detected at commit '{}'",
                        frame.commit_id
                    ),
                ));
            }

            if let Some(previous_depth) = nearest_depths.get(&frame.commit_id) {
                if *previous_depth <= frame.depth {
                    continue;
                }
            }

            let commit = self.load_commit(&frame.commit_id).await?;
            nearest_depths.insert(frame.commit_id.clone(), frame.depth);

            visiting.insert(frame.commit_id.clone());
            stack.push(TraversalFrame {
                commit_id: frame.commit_id,
                depth: frame.depth,
                expanded: true,
            });
            for parent_commit_id in commit.parent_commit_ids.iter().rev() {
                stack.push(TraversalFrame {
                    commit_id: parent_commit_id.clone(),
                    depth: frame.depth + 1,
                    expanded: false,
                });
            }
        }
        Ok(())
    }

    async fn load_commit(&mut self, commit_id: &str) -> Result<CommitGraphCommit, LixError> {
        if let Some(commit) = self.loaded.get(commit_id) {
            return Ok(commit.clone());
        }
        let Some(commit) = self.reader.load_commit(commit_id).await? else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("commit_graph missing commit '{commit_id}'"),
            ));
        };
        self.loaded.insert(commit_id.to_string(), commit.clone());
        Ok(commit)
    }
}

struct TraversalFrame {
    commit_id: String,
    depth: u32,
    expanded: bool,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::changelog::{
        ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRefSet, CommitRecord,
    };
    use crate::commit_graph::CommitGraphChange;
    use crate::commit_graph::CommitGraphContext;
    use crate::storage::StorageContext;
    use crate::storage::{InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions};
    use crate::LixError;

    #[tokio::test]
    async fn reachable_commits_returns_commits_nearest_first() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commits = reader
            .reachable_commits("commit-head")
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.as_str(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![("commit-head", 0), ("commit-parent", 1), ("commit-root", 2)]
        );
    }

    #[tokio::test]
    async fn reachable_commits_errors_on_missing_parent_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let error = append_changes_result(
            &storage,
            &[commit_change(
                "commit-head-change",
                "commit-head",
                &[],
                &["missing-parent"],
            )],
        )
        .await
        .expect_err("changelog should reject missing parent");

        assert!(error.message.contains("missing-parent"));
    }

    #[tokio::test]
    async fn reachable_commits_errors_on_cycle() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-a-change", "commit-a", &[], &["commit-b"]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let error = reader
            .reachable_commits("commit-a")
            .await
            .expect_err("walker should reject parent cycles");

        assert!(error.message.contains("cycle"));
    }

    #[tokio::test]
    async fn reachable_commits_dedupes_shared_ancestors_in_diamond() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
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
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commits = reader
            .reachable_commits("commit-head")
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.as_str(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![
                ("commit-head", 0),
                ("commit-left", 1),
                ("commit-right", 1),
                ("commit-root", 2),
            ]
        );
    }

    #[tokio::test]
    async fn reachable_commits_keeps_nearest_depth_for_multiple_paths() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &[],
                    &["commit-root", "commit-parent"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commits = reader
            .reachable_commits("commit-head")
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.as_str(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![("commit-head", 0), ("commit-parent", 1), ("commit-root", 1)]
        );
    }

    #[tokio::test]
    async fn reachable_commits_orders_same_depth_commits_by_id() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-z-change", "commit-z", &[], &[]),
                commit_change("commit-a-change", "commit-a", &[], &[]),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &[],
                    &["commit-z", "commit-a"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commits = reader
            .reachable_commits("commit-head")
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.as_str(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![("commit-head", 0), ("commit-a", 1), ("commit-z", 1)]
        );
    }

    #[tokio::test]
    async fn reachable_commits_errors_on_missing_head_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);

        let error = reader
            .reachable_commits("missing-head")
            .await
            .expect_err("missing head should fail");

        assert!(error.message.contains("missing-head"));
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_nearest_common_commit_in_simple_graph() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-a-change", "commit-a", &[], &[]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
                commit_change("commit-c-change", "commit-c", &[], &["commit-b"]),
                commit_change("commit-d-change", "commit-d", &[], &["commit-b"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let ancestors = reader
            .best_common_ancestors("commit-c", "commit-d")
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-b"]
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_shared_fork_in_diamond_graph() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-left-change", "commit-left", &[], &["commit-root"]),
                commit_change("commit-right-change", "commit-right", &[], &["commit-root"]),
                commit_change(
                    "commit-left-head-change",
                    "commit-left-head",
                    &[],
                    &["commit-left"],
                ),
                commit_change(
                    "commit-right-head-change",
                    "commit-right-head",
                    &[],
                    &["commit-right"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let ancestors = reader
            .best_common_ancestors("commit-left-head", "commit-right-head")
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-root"]
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_parent_when_one_side_is_ancestor() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-a-change", "commit-a", &[], &[]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
                commit_change("commit-c-change", "commit-c", &[], &["commit-b"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let ancestors = reader
            .best_common_ancestors("commit-b", "commit-c")
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-b"]
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_multiple_bases_for_criss_cross_graph() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-left-change", "commit-left", &[], &["commit-root"]),
                commit_change("commit-right-change", "commit-right", &[], &["commit-root"]),
                commit_change(
                    "commit-head-left-change",
                    "commit-head-left",
                    &[],
                    &["commit-left", "commit-right"],
                ),
                commit_change(
                    "commit-head-right-change",
                    "commit-head-right",
                    &[],
                    &["commit-right", "commit-left"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let ancestors = reader
            .best_common_ancestors("commit-head-left", "commit-head-right")
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.as_str())
                .collect::<Vec<_>>(),
            vec!["commit-left", "commit-right"]
        );
    }

    #[tokio::test]
    async fn merge_base_returns_single_best_common_ancestor() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-a-change", "commit-a", &[], &[]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
                commit_change("commit-c-change", "commit-c", &[], &["commit-b"]),
                commit_change("commit-d-change", "commit-d", &[], &["commit-b"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let base = reader
            .merge_base("commit-c", "commit-d")
            .await
            .expect("single merge base should resolve");

        assert_eq!(base.commit_id, "commit-b");
    }

    #[tokio::test]
    async fn merge_base_errors_when_histories_have_no_common_commit() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-left-change", "commit-left", &[], &[]),
                commit_change("commit-right-change", "commit-right", &[], &[]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let error = reader
            .merge_base("commit-left", "commit-right")
            .await
            .expect_err("unrelated histories should not have a merge base");

        assert!(error.message.contains("no common history"));
    }

    #[tokio::test]
    async fn merge_base_errors_when_best_common_ancestor_is_ambiguous() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-left-change", "commit-left", &[], &["commit-root"]),
                commit_change("commit-right-change", "commit-right", &[], &["commit-root"]),
                commit_change(
                    "commit-head-left-change",
                    "commit-head-left",
                    &[],
                    &["commit-left", "commit-right"],
                ),
                commit_change(
                    "commit-head-right-change",
                    "commit-head-right",
                    &[],
                    &["commit-right", "commit-left"],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut reader = graph.reader(read);
        let error = reader
            .merge_base("commit-head-left", "commit-head-right")
            .await
            .expect_err("ambiguous best common ancestors should fail");

        assert_eq!(error.code, LixError::CODE_AMBIGUOUS_MERGE_BASE);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("left_commit_id")),
            Some(&json!("commit-head-left"))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("right_commit_id")),
            Some(&json!("commit-head-right"))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("candidates")),
            Some(&json!(["commit-left", "commit-right"]))
        );
    }

    #[derive(Clone)]
    struct TestCommitChange {
        change: CommitGraphChange,
        parent_commit_ids: Vec<String>,
    }

    async fn append_changes(storage: &StorageContext, changes: &[TestCommitChange]) {
        append_changes_result(storage, changes)
            .await
            .expect("changelog fixture should append");
    }

    async fn append_changes_result(
        storage: &StorageContext,
        changes: &[TestCommitChange],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut append = ChangelogAppend::default();
        for change in changes {
            let commit_id = change
                .change
                .entity_pk
                .as_single_string()
                .expect("commit fixture should have single id")
                .to_string();
            append.commits.push(CommitRecord {
                format_version: 1,
                commit_id: commit_id.clone(),
                parent_commit_ids: change.parent_commit_ids.clone(),
                change_id: change.change.id.clone(),
                author_account_ids: Vec::new(),
                created_at: change.change.created_at.clone(),
            });
            append.commit_change_refs.push(CommitChangeRefSet {
                commit_id: commit_id.clone(),
                entries: Vec::new(),
            });
        }
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(append)
            .await?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("commit should succeed");
        Ok(())
    }

    fn commit_change(
        change_id: &str,
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> TestCommitChange {
        let _ = change_ids;
        TestCommitChange {
            change: CommitGraphChange {
                id: change_id.to_string(),
                entity_pk: crate::entity_pk::EntityPk::single(commit_id),
                schema_key: "lix_commit".to_string(),
                file_id: None,
                snapshot_ref: None,
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            },
            parent_commit_ids: parent_commit_ids.iter().map(|id| id.to_string()).collect(),
        }
    }
}
