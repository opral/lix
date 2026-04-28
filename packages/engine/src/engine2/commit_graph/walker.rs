use std::collections::{BTreeMap, BTreeSet};

use crate::backend::KvStore;
use crate::engine2::commit_graph::{
    CommitGraphCommit, CommitGraphStoreReader, ReachableCommitGraphCommit,
};
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
    S: KvStore,
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

struct CommitTraversalLoader<'a, S>
where
    S: KvStore,
{
    reader: &'a mut CommitGraphStoreReader<S>,
    loaded: BTreeMap<String, CommitGraphCommit>,
}

impl<'a, S> CommitTraversalLoader<'a, S>
where
    S: KvStore,
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
    use std::sync::Arc;

    use serde_json::json;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
    use crate::engine2::commit_graph::CommitGraphContext;

    #[tokio::test]
    async fn reachable_commits_returns_commits_nearest_first() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[commit_change(
                "commit-head-change",
                "commit-head",
                &[],
                &["missing-parent"],
            )],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
        let error = reader
            .reachable_commits("commit-head")
            .await
            .expect_err("missing parent should fail");

        assert!(error.description.contains("missing-parent"));
    }

    #[tokio::test]
    async fn reachable_commits_errors_on_cycle() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[
                commit_change("commit-a-change", "commit-a", &[], &["commit-b"]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
        let error = reader
            .reachable_commits("commit-a")
            .await
            .expect_err("cycle should fail");

        assert!(error.description.contains("cycle"));
    }

    #[tokio::test]
    async fn reachable_commits_dedupes_shared_ancestors_in_diamond() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let backend = Arc::new(UnitTestBackend::new());
        let graph = CommitGraphContext::new(ChangelogContext::new());
        let mut reader = graph.reader(backend);

        let error = reader
            .reachable_commits("missing-head")
            .await
            .expect_err("missing head should fail");

        assert!(error.description.contains("missing-head"));
    }

    async fn append_changes(
        backend: Arc<UnitTestBackend>,
        changelog: &ChangelogContext,
        changes: &[CanonicalChange],
    ) {
        let mut tx = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        changelog
            .writer(tx.as_mut())
            .append_changes(changes)
            .await
            .expect("append should succeed");
        tx.commit().await.expect("commit should succeed");
    }

    fn commit_change(
        change_id: &str,
        commit_id: &str,
        change_ids: &[&str],
        parent_commit_ids: &[&str],
    ) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: commit_id.to_string(),
            schema_key: "lix_commit".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                serde_json::to_string(&json!({
                    "id": commit_id,
                    "change_set_id": "change-set-1",
                    "change_ids": change_ids,
                    "parent_commit_ids": parent_commit_ids,
                }))
                .expect("snapshot should serialize"),
            ),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }
}
