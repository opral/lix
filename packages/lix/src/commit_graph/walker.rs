#![allow(clippy::clone_on_copy, clippy::iter_cloned_collect)]

use std::collections::{BTreeMap, BTreeSet};

use crate::LixError;
use crate::changelog::CommitId;
use crate::commit_graph::{CommitGraphNode, CommitGraphStoreReader, ReachableCommitGraphNode};
use crate::storage_adapter::StorageAdapterRead;

/// Walks parent links from `head_commit_id` and returns reachable commits
/// nearest-first.
///
/// `max_depth` bounds the walk itself. A caller that only wants the first few
/// generations pays for those generations, not for the whole graph.
///
/// The walker is intentionally storage-free. It asks `CommitGraphReader` to
/// load parsed commit facts and owns only traversal concerns: batching, cycle
/// detection, and nearest-depth selection.
pub(crate) async fn walk_reachable_nodes<S>(
    reader: &mut CommitGraphStoreReader<S>,
    head_commit_id: &CommitId,
    max_depth: Option<u32>,
) -> Result<Vec<ReachableCommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    let mut walk = ReachableWalk::new(*head_commit_id);
    let mut commits = Vec::new();
    while let Some(layer) = walk.next_layer(reader).await? {
        let depth = layer.depth;
        commits.extend(
            layer
                .commits
                .into_iter()
                .map(|commit| ReachableCommitGraphNode { commit, depth }),
        );
        if max_depth.is_some_and(|max_depth| depth >= max_depth) {
            break;
        }
    }
    Ok(commits)
}

/// One nearest-depth generation of a reachable-commit walk.
pub(crate) struct ReachableLayer {
    pub(crate) depth: u32,
    pub(crate) commits: Vec<CommitGraphNode>,
}

/// Resumable breadth-first cursor over the reachable commit graph.
///
/// Breadth-first order is the order the history surfaces already publish
/// (nearest depth first, commit id within a depth), so a caller that has
/// collected enough rows can simply stop asking for the next layer instead of
/// materializing the remainder of the graph and truncating afterwards.
///
/// Each layer is loaded with one batched point read rather than one storage
/// round-trip per commit.
pub(crate) struct ReachableWalk {
    seen: BTreeSet<CommitId>,
    /// Frontier entries carry the generation of the child that discovered them.
    /// Commit generations are strictly increasing away from the roots, so a
    /// parent that fails to sit below its child proves a corrupt cycle.
    frontier: Vec<(CommitId, u64)>,
    depth: u32,
}

impl ReachableWalk {
    pub(crate) fn new(head_commit_id: CommitId) -> Self {
        Self {
            seen: BTreeSet::from([head_commit_id]),
            frontier: vec![(head_commit_id, u64::MAX)],
            depth: 0,
        }
    }

    pub(crate) async fn next_layer<S>(
        &mut self,
        reader: &mut CommitGraphStoreReader<S>,
    ) -> Result<Option<ReachableLayer>, LixError>
    where
        S: StorageAdapterRead,
    {
        if self.frontier.is_empty() {
            return Ok(None);
        }
        let depth = self.depth;
        let mut frontier = std::mem::take(&mut self.frontier);
        frontier.sort_unstable();
        let commit_ids = frontier
            .iter()
            .map(|(commit_id, _)| *commit_id)
            .collect::<Vec<_>>();
        let loaded = reader.load_nodes(&commit_ids).await?;
        let mut commits = Vec::with_capacity(commit_ids.len());
        let mut next_frontier = Vec::new();
        for ((commit_id, node), (_, child_generation)) in loaded.into_iter().zip(frontier.iter()) {
            let Some(node) = node else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("commit_graph missing commit '{commit_id}'"),
                ));
            };
            if node.generation >= *child_generation {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "commit_graph cycle detected at commit '{commit_id}': it does not have a lower generation than the child that reaches it"
                    ),
                ));
            }
            for parent_commit_id in &node.parent_commit_ids {
                if self.seen.insert(*parent_commit_id) {
                    next_frontier.push((*parent_commit_id, node.generation));
                }
            }
            commits.push(node);
        }
        self.frontier = next_frontier;
        self.depth = depth.saturating_add(1);
        Ok(Some(ReachableLayer { depth, commits }))
    }
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
    left_commit_id: &CommitId,
    right_commit_id: &CommitId,
) -> Result<Vec<CommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    let mut loader = NodeTraversalLoader::new(reader);
    let best = loader
        .best_common_ids(*left_commit_id, *right_commit_id)
        .await?;
    let mut nodes = Vec::with_capacity(best.len());
    for commit_id in best {
        nodes.push(loader.load_node(&commit_id).await?);
    }
    Ok(nodes)
}

struct NodeTraversalLoader<'a, S>
where
    S: StorageAdapterRead,
{
    reader: &'a mut CommitGraphStoreReader<S>,
}

impl<'a, S> NodeTraversalLoader<'a, S>
where
    S: StorageAdapterRead,
{
    fn new(reader: &'a mut CommitGraphStoreReader<S>) -> Self {
        Self { reader }
    }

    async fn best_common_ids(
        &mut self,
        left_commit_id: CommitId,
        right_commit_id: CommitId,
    ) -> Result<Vec<CommitId>, LixError> {
        const LEFT: u8 = 1;
        const RIGHT: u8 = 2;
        const BOTH: u8 = LEFT | RIGHT;
        const STALE: u8 = 4;

        let left = self.load_node(&left_commit_id).await?;
        let right = self.load_node(&right_commit_id).await?;
        let mut colors = BTreeMap::from([(left_commit_id, LEFT), (right_commit_id, RIGHT)]);
        if left_commit_id == right_commit_id {
            colors.insert(left_commit_id, BOTH);
        }
        let mut queue = BTreeSet::from([
            (left.generation, left_commit_id),
            (right.generation, right_commit_id),
        ]);
        let mut non_stale_queued = BTreeSet::from([left_commit_id, right_commit_id]);
        let mut best = Vec::new();

        while !queue.is_empty() {
            if !best.is_empty() && non_stale_queued.is_empty() {
                break;
            }
            let (generation, commit_id) = queue.pop_last().expect("queue is not empty");
            non_stale_queued.remove(&commit_id);
            let commit = self.load_node(&commit_id).await?;
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
            for parent_commit_id in commit.parent_commit_ids {
                let parent = self.load_node(&parent_commit_id).await?;
                if parent.generation >= generation {
                    return Err(LixError::unknown(format!(
                        "commit '{commit_id}' parent '{parent_commit_id}' does not have a lower generation"
                    )));
                }
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
        Ok(best)
    }

    async fn load_node(&mut self, commit_id: &CommitId) -> Result<CommitGraphNode, LixError> {
        let Some(commit) = self.reader.load_node(commit_id).await? else {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("commit_graph missing commit '{commit_id}'"),
            ));
        };
        Ok(commit)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;

    use crate::LixError;
    use crate::changelog::{
        ChangeId, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitId, CommitRecord,
    };
    use crate::commit_graph::CommitGraphChange;
    use crate::commit_graph::CommitGraphContext;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{Memory, StorageKey, StorageReadOptions, StorageWriteOptions};
    use crate::tracked_state::{
        CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
    };

    fn ts(value: &str) -> crate::common::LixTimestamp {
        crate::common::LixTimestamp::expect_parse("timestamp", value)
    }

    fn commit_id(label: &str) -> CommitId {
        CommitId::for_test_label(label)
    }

    fn commit_ids<const N: usize>(labels: [&str; N]) -> Vec<CommitId> {
        labels.into_iter().map(commit_id).collect()
    }

    fn sorted_commit_ids<const N: usize>(labels: [&str; N]) -> Vec<CommitId> {
        let mut ids = commit_ids(labels);
        ids.sort();
        ids
    }

    fn sorted_commit_ids_at_depth<const N: usize>(
        labels: [&str; N],
        depth: u32,
    ) -> Vec<(CommitId, u32)> {
        sorted_commit_ids(labels)
            .into_iter()
            .map(|id| (id, depth))
            .collect()
    }

    #[tokio::test]
    async fn reachable_nodes_returns_commits_nearest_first() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let commits = reader
            .reachable_nodes(&commit_head)
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.clone(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![
                (commit_id("commit-head"), 0),
                (commit_id("commit-parent"), 1),
                (commit_id("commit-root"), 2)
            ]
        );
    }

    #[tokio::test]
    async fn reachable_nodes_errors_on_missing_parent_commit() {
        let storage = StorageAdapter::new(Memory::new());
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

        assert!(
            error
                .message
                .contains(&commit_id("missing-parent").to_string())
        );
    }

    #[tokio::test]
    async fn exact_graph_read_rejects_record_stored_under_another_commit_key() {
        let storage = StorageAdapter::new(Memory::new());
        let requested = commit_id("requested-commit");
        let embedded = commit_id("embedded-commit");
        let mut writes = storage.new_write_set();
        writes.put(
            crate::changelog::COMMIT_SPACE,
            StorageKey(Bytes::copy_from_slice(requested.as_uuid().as_bytes())),
            crate::changelog::encode_commit_record(&CommitRecord {
                format_version: 4,
                commit_id: embedded,
                generation: 0,
                parent_commit_ids: Vec::new(),
                first_parent_jump_commit_id: embedded,
                first_parent_jump_span: 0,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: ts("2026-01-01T00:00:00Z"),
            })
            .expect("mismatched commit should encode"),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("mismatched commit should persist");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = reader
            .load_node(&requested)
            .await
            .expect_err("exact read must reject a mismatched embedded commit id");

        assert!(
            error
                .message
                .contains("identity does not match requested key")
        );
    }

    #[tokio::test]
    async fn reachable_nodes_errors_on_cycle() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        for (label, parent) in [("commit-a", "commit-b"), ("commit-b", "commit-a")] {
            let current_commit_id = commit_id(label);
            let parent_commit_id = commit_id(parent);
            let record = CommitRecord {
                format_version: 4,
                commit_id: current_commit_id,
                generation: 1,
                parent_commit_ids: vec![parent_commit_id],
                first_parent_jump_commit_id: parent_commit_id,
                first_parent_jump_span: 1,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: ts("2026-01-01T00:00:00Z"),
            };
            writes.put(
                crate::changelog::COMMIT_SPACE,
                StorageKey(Bytes::copy_from_slice(
                    current_commit_id.as_uuid().as_bytes(),
                )),
                crate::changelog::encode_commit_record(&record)
                    .expect("cycle commit should encode"),
            );
            stage_test_commit_manifest(&mut writes, &record)
                .expect("cycle commit authority should stage");
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt cycle should persist");

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_a = commit_id("commit-a");
        let error = reader
            .reachable_nodes(&commit_a)
            .await
            .expect_err("walker should reject parent cycles");

        assert!(error.message.contains("cycle"));
    }

    #[tokio::test]
    async fn reachable_nodes_dedupes_shared_ancestors_in_diamond() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let commits = reader
            .reachable_nodes(&commit_head)
            .await
            .expect("reachable commits should load");
        let mut expected = vec![(commit_id("commit-head"), 0)];
        expected.extend(sorted_commit_ids_at_depth(
            ["commit-left", "commit-right"],
            1,
        ));
        expected.push((commit_id("commit-root"), 2));

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.clone(), reachable.depth))
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[tokio::test]
    async fn reachable_nodes_keeps_nearest_depth_for_multiple_paths() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let commits = reader
            .reachable_nodes(&commit_head)
            .await
            .expect("reachable commits should load");

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.clone(), reachable.depth))
                .collect::<Vec<_>>(),
            vec![
                (commit_id("commit-head"), 0),
                (commit_id("commit-parent"), 1),
                (commit_id("commit-root"), 1)
            ]
        );
    }

    #[tokio::test]
    async fn reachable_nodes_orders_same_depth_commits_by_id() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head = commit_id("commit-head");
        let commits = reader
            .reachable_nodes(&commit_head)
            .await
            .expect("reachable commits should load");
        let mut expected = vec![(commit_id("commit-head"), 0)];
        expected.extend(sorted_commit_ids_at_depth(["commit-z", "commit-a"], 1));

        assert_eq!(
            commits
                .iter()
                .map(|reachable| (reachable.commit.commit_id.clone(), reachable.depth))
                .collect::<Vec<_>>(),
            expected
        );
    }

    #[tokio::test]
    async fn bounded_reachable_nodes_match_unbounded_prefix_for_a_diamond() {
        let storage = StorageAdapter::new(Memory::new());
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

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let head = commit_id("commit-head");
        let full = reader
            .reachable_nodes(&head)
            .await
            .expect("full diamond should load");
        for max_depth in [0, 1, 2] {
            let bounded = super::walk_reachable_nodes(&mut reader, &head, Some(max_depth))
                .await
                .expect("bounded diamond should load");
            let expected = full
                .iter()
                .filter(|reachable| reachable.depth <= max_depth)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(bounded, expected);
        }
    }

    #[tokio::test]
    async fn bounded_reachable_nodes_match_unbounded_prefix_for_linear_history() {
        let storage = StorageAdapter::new(Memory::new());
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

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let head = commit_id("commit-head");
        let full = reader
            .reachable_nodes(&head)
            .await
            .expect("full linear history should load");
        for max_depth in [0, 1, 2] {
            let bounded = super::walk_reachable_nodes(&mut reader, &head, Some(max_depth))
                .await
                .expect("bounded linear history should load");
            let expected = full
                .iter()
                .filter(|reachable| reachable.depth <= max_depth)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(bounded, expected);
        }
    }

    #[tokio::test]
    async fn bounded_reachable_nodes_validate_only_parents_inside_the_frontier() {
        let storage = StorageAdapter::new(Memory::new());
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
        let root = commit_id("commit-root");
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::changelog::COMMIT_SPACE,
            StorageKey(Bytes::copy_from_slice(root.as_uuid().as_bytes())),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("old ancestry corruption should stage");

        let head = commit_id("commit-head");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let bounded = super::walk_reachable_nodes(&mut reader, &head, Some(1))
            .await
            .expect("parent below the frontier must not load");
        assert_eq!(bounded.len(), 2);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = super::walk_reachable_nodes(&mut reader, &head, Some(2))
            .await
            .expect_err("missing parent inside the frontier must fail");
        assert!(error.message.contains(&root.to_string()));
    }

    #[tokio::test]
    async fn bounded_reachable_nodes_reject_cycles_closed_at_the_frontier() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[commit_change("commit-root-change", "commit-root", &[], &[])],
        )
        .await;
        let root = commit_id("commit-root");
        let left = commit_id("commit-cycle-left");
        let right = commit_id("commit-cycle-right");
        let record = |commit_id, parents| CommitRecord {
            format_version: 4,
            commit_id,
            generation: 2,
            parent_commit_ids: parents,
            first_parent_jump_commit_id: commit_id,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: ts("2026-01-01T00:00:00Z"),
        };
        let records = [
            record(left, vec![right, root]),
            record(right, vec![left, root]),
        ];
        let mut writes = storage.new_write_set();
        for record in records {
            writes.put(
                crate::changelog::COMMIT_SPACE,
                StorageKey(Bytes::copy_from_slice(
                    record.commit_id.as_uuid().as_bytes(),
                )),
                crate::changelog::encode_commit_record(&record)
                    .expect("cyclic record should encode"),
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("cycle should persist");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = super::walk_reachable_nodes(&mut reader, &left, Some(1))
            .await
            .expect_err("cycle closed at the frontier must fail");
        assert!(error.message.contains("cycle detected"));
    }

    #[tokio::test]
    async fn reachable_nodes_errors_on_missing_head_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let missing_head = commit_id("missing-head");

        let error = reader
            .reachable_nodes(&missing_head)
            .await
            .expect_err("missing head should fail");

        assert!(error.message.contains(&missing_head.to_string()));
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_nearest_common_commit_in_simple_graph() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_c = commit_id("commit-c");
        let commit_d = commit_id("commit-d");
        let ancestors = reader
            .best_common_ancestors(&commit_c, &commit_d)
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            commit_ids(["commit-b"])
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_does_not_walk_the_shared_history_below_the_frontier() {
        let storage = StorageAdapter::new(Memory::new());
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
                commit_change("commit-base-change", "commit-base", &[], &["commit-parent"]),
                commit_change("commit-left-change", "commit-left", &[], &["commit-base"]),
                commit_change("commit-right-change", "commit-right", &[], &["commit-base"]),
            ],
        )
        .await;
        let root = commit_id("commit-root");
        let mut writes = storage.new_write_set();
        writes.delete(
            crate::changelog::COMMIT_SPACE,
            StorageKey(Bytes::copy_from_slice(root.as_uuid().as_bytes())),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("old shared history corruption should stage");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let ancestors = reader
            .best_common_ancestors(&commit_id("commit-left"), &commit_id("commit-right"))
            .await
            .expect("merge base should stop at the shared frontier");

        assert_eq!(
            ancestors
                .into_iter()
                .map(|commit| commit.commit_id)
                .collect::<Vec<_>>(),
            commit_ids(["commit-base"])
        );
    }

    #[tokio::test]
    async fn merge_base_and_general_walk_reject_non_decreasing_parent_generation() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-child-change", "commit-child", &[], &["commit-root"]),
                commit_change(
                    "commit-sibling-change",
                    "commit-sibling",
                    &[],
                    &["commit-root"],
                ),
                commit_change(
                    "commit-grandchild-change",
                    "commit-grandchild",
                    &[],
                    &["commit-child"],
                ),
            ],
        )
        .await;
        let child = commit_id("commit-child");
        let mut writes = storage.new_write_set();
        let record = CommitRecord {
            format_version: 4,
            commit_id: child,
            generation: 0,
            parent_commit_ids: commit_ids(["commit-root"]),
            first_parent_jump_commit_id: child,
            first_parent_jump_span: 0,
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: ts("2026-01-01T00:00:00Z"),
        };
        writes.put(
            crate::changelog::COMMIT_SPACE,
            StorageKey(Bytes::copy_from_slice(child.as_uuid().as_bytes())),
            crate::changelog::encode_commit_record(&record).expect("corrupt commit should encode"),
        );
        stage_test_commit_manifest(&mut writes, &record)
            .expect("matching corrupt authority should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt commit should persist");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = reader
            .best_common_ancestors(&child, &commit_id("commit-root"))
            .await
            .expect_err("invalid generations should fail the graph walk");

        assert!(
            error.message.contains("does not have a lower generation")
                || error
                    .message
                    .contains("first-parent jump span exceeds its generation")
                || error.message.contains("no advancing first-parent jump")
        );

        for (left, right) in [
            ("commit-root", "commit-child"),
            ("commit-child", "commit-root"),
        ] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut reader = CommitGraphContext::new().reader(read);
            let error = reader
                .merge_base(&commit_id(left), &commit_id(right))
                .await
                .expect_err("direct-parent shortcut must reject invalid generations");
            assert!(
                error.message.contains("does not have a lower generation")
                    || error.message.contains("no advancing first-parent jump")
            );
        }

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = reader
            .merge_base(&commit_id("commit-child"), &commit_id("commit-sibling"))
            .await
            .expect_err("shared-parent shortcut must reject invalid generations");
        assert!(
            error.message.contains("does not have a lower generation")
                || error.message.contains("no advancing first-parent jump")
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let error = reader
            .merge_base(&commit_id("commit-root"), &commit_id("commit-grandchild"))
            .await
            .expect_err("linear prefix and DAG fallback must preserve generation validation");
        assert!(
            error.message.contains("does not have a lower generation")
                || error.message.contains("no advancing first-parent jump")
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_shared_fork_in_diamond_graph() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_left_head = commit_id("commit-left-head");
        let commit_right_head = commit_id("commit-right-head");
        let ancestors = reader
            .best_common_ancestors(&commit_left_head, &commit_right_head)
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            commit_ids(["commit-root"])
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_parent_when_one_side_is_ancestor() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_b = commit_id("commit-b");
        let commit_c = commit_id("commit-c");
        let ancestors = reader
            .best_common_ancestors(&commit_b, &commit_c)
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            commit_ids(["commit-b"])
        );
    }

    #[tokio::test]
    async fn best_common_ancestors_returns_multiple_bases_for_criss_cross_graph() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_head_left = commit_id("commit-head-left");
        let commit_head_right = commit_id("commit-head-right");
        let ancestors = reader
            .best_common_ancestors(&commit_head_left, &commit_head_right)
            .await
            .expect("best common ancestors should load");

        assert_eq!(
            ancestors
                .iter()
                .map(|commit| commit.commit_id.clone())
                .collect::<Vec<_>>(),
            sorted_commit_ids(["commit-left", "commit-right"])
        );
    }

    #[tokio::test]
    async fn merge_base_returns_single_best_common_ancestor() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_c = commit_id("commit-c");
        let commit_d = commit_id("commit-d");
        let base = reader
            .merge_base(&commit_c, &commit_d)
            .await
            .expect("single merge base should resolve");

        assert_eq!(base, commit_id("commit-b"));
    }

    #[tokio::test]
    async fn merge_base_resolves_deep_linear_ancestor_and_fork() {
        let storage = StorageAdapter::new(Memory::new());
        append_changes(
            &storage,
            &[
                commit_change("commit-a-change", "commit-a", &[], &[]),
                commit_change("commit-b-change", "commit-b", &[], &["commit-a"]),
                commit_change("commit-c-change", "commit-c", &[], &["commit-b"]),
                commit_change("commit-d-change", "commit-d", &[], &["commit-c"]),
                commit_change("commit-e-change", "commit-e", &[], &["commit-d"]),
                commit_change("commit-x-change", "commit-x", &[], &["commit-b"]),
                commit_change("commit-y-change", "commit-y", &[], &["commit-x"]),
                commit_change("commit-z-change", "commit-z", &[], &["commit-y"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);

        assert_eq!(
            reader
                .merge_base(&commit_id("commit-b"), &commit_id("commit-e"))
                .await
                .expect("deep ancestor should resolve"),
            commit_id("commit-b")
        );
        assert_eq!(
            reader
                .merge_base(&commit_id("commit-e"), &commit_id("commit-z"))
                .await
                .expect("deep linear fork should resolve"),
            commit_id("commit-b")
        );
    }

    #[tokio::test]
    async fn merge_base_segment_skips_refine_shared_fork_and_unequal_head() {
        let storage = StorageAdapter::new(Memory::new());
        let mut changes = Vec::new();
        changes.push(commit_change(
            "segment-root-change",
            "segment-root",
            &[],
            &[],
        ));
        let mut trunk_parent = "segment-root".to_string();
        for index in 1..=70 {
            let label = format!("segment-trunk-{index}");
            changes.push(commit_change(
                &format!("{label}-change"),
                &label,
                &[],
                &[&trunk_parent],
            ));
            trunk_parent = label;
        }
        let fork = trunk_parent.clone();
        let mut left_parent = fork.clone();
        let mut right_parent = fork.clone();
        for index in 1..=130 {
            let left = format!("segment-left-{index}");
            changes.push(commit_change(
                &format!("{left}-change"),
                &left,
                &[],
                &[&left_parent],
            ));
            left_parent = left;
            let right = format!("segment-right-{index}");
            changes.push(commit_change(
                &format!("{right}-change"),
                &right,
                &[],
                &[&right_parent],
            ));
            right_parent = right;
        }
        append_changes(&storage, &changes).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("general-walker read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        let general = reader
            .best_common_ancestors(&commit_id(&left_parent), &commit_id(&right_parent))
            .await
            .expect("general walker should resolve fork");
        assert_eq!(
            general
                .iter()
                .map(|node| node.commit_id)
                .collect::<Vec<_>>(),
            vec![commit_id(&fork)],
            "Myers fast path must agree with the general DAG walker",
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        assert_eq!(
            reader
                .merge_base(&commit_id(&left_parent), &commit_id(&right_parent))
                .await
                .expect("segmented fork should resolve exactly"),
            commit_id(&fork)
        );

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("second read should open");
        let mut reader = CommitGraphContext::new().reader(read);
        assert_eq!(
            reader
                .merge_base(&commit_id("segment-trunk-10"), &commit_id(&left_parent))
                .await
                .expect("unequal segmented ancestor should resolve exactly"),
            commit_id("segment-trunk-10")
        );
    }

    #[tokio::test]
    async fn merge_base_errors_when_histories_have_no_common_commit() {
        let storage = StorageAdapter::new(Memory::new());
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
            .await
            .expect("read should open");
        let mut reader = graph.reader(read);
        let commit_left = commit_id("commit-left");
        let commit_right = commit_id("commit-right");
        let error = reader
            .merge_base(&commit_left, &commit_right)
            .await
            .expect_err("unrelated histories should not have a merge base");

        assert!(error.message.contains("no common history"));
    }

    #[tokio::test]
    async fn merge_base_errors_when_best_common_ancestor_is_ambiguous() {
        let storage = StorageAdapter::new(Memory::new());
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
                commit_change(
                    "commit-left-tail-1-change",
                    "commit-left-tail-1",
                    &[],
                    &["commit-head-left"],
                ),
                commit_change(
                    "commit-left-tail-2-change",
                    "commit-left-tail-2",
                    &[],
                    &["commit-left-tail-1"],
                ),
                commit_change(
                    "commit-right-tail-1-change",
                    "commit-right-tail-1",
                    &[],
                    &["commit-head-right"],
                ),
                commit_change(
                    "commit-right-tail-2-change",
                    "commit-right-tail-2",
                    &[],
                    &["commit-right-tail-1"],
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
        let commit_head_left = commit_id("commit-left-tail-2");
        let commit_head_right = commit_id("commit-right-tail-2");
        let error = reader
            .merge_base(&commit_head_left, &commit_head_right)
            .await
            .expect_err("ambiguous best common ancestors should fail");

        assert_eq!(error.code, LixError::CODE_AMBIGUOUS_MERGE_BASE);
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("left_commit_id")),
            Some(&json!(commit_id("commit-left-tail-2").to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("right_commit_id")),
            Some(&json!(commit_id("commit-right-tail-2").to_string()))
        );
        assert_eq!(
            error
                .details
                .as_ref()
                .and_then(|details| details.get("candidates")),
            Some(&json!(
                sorted_commit_ids(["commit-left", "commit-right"])
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
            ))
        );
    }

    #[derive(Clone)]
    struct TestCommitChange {
        change: CommitGraphChange,
        parent_commit_ids: Vec<CommitId>,
    }

    async fn append_changes(storage: &StorageAdapter, changes: &[TestCommitChange]) {
        append_changes_result(storage, changes)
            .await
            .expect("changelog fixture should append");
    }

    async fn append_changes_result(
        storage: &StorageAdapter,
        changes: &[TestCommitChange],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut append = ChangelogAppend::default();
        let mut generations = std::collections::BTreeMap::<CommitId, u64>::new();
        let mut topology_records = std::collections::BTreeMap::<CommitId, CommitRecord>::new();
        for change in changes {
            let commit_id = change
                .change
                .entity_pk
                .as_single_string()
                .expect("commit fixture should have single id")
                .to_string();
            let parent_commit_ids = change.parent_commit_ids.iter().copied().collect::<Vec<_>>();
            let generation = parent_commit_ids
                .iter()
                .filter_map(|parent| generations.get(parent).copied())
                .max()
                .map_or_else(
                    || usize::from(!parent_commit_ids.is_empty()) as u64,
                    |parent_generation| parent_generation + 1,
                );
            let typed_commit_id = CommitId::for_test_label(&commit_id);
            let parent = match parent_commit_ids.as_slice() {
                [parent_commit_id] => topology_records.get(parent_commit_id),
                _ => None,
            };
            let (first_parent_jump_commit_id, first_parent_jump_span) = match parent {
                Some(parent) => {
                    let parent_jump = topology_records
                        .get(&parent.first_parent_jump_commit_id)
                        .expect("test parent jump target exists");
                    crate::changelog::next_first_parent_jump(
                        typed_commit_id,
                        &parent_commit_ids,
                        Some(parent),
                        Some(parent_jump),
                    )
                    .expect("test commit jump should derive")
                }
                None => match parent_commit_ids.as_slice() {
                    [missing_parent] => (*missing_parent, 1),
                    _ => (typed_commit_id, 0),
                },
            };
            let record = CommitRecord {
                format_version: 4,
                commit_id: typed_commit_id,
                generation,
                parent_commit_ids,
                first_parent_jump_commit_id,
                first_parent_jump_span,
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                created_at: change.change.created_at,
            };
            append.commits.push(record.clone());
            topology_records.insert(typed_commit_id, record);
            generations.insert(typed_commit_id, generation);
        }
        let commit_records = append.commits.clone();
        ChangelogContext::new()
            .writer(&mut read, &mut writes)
            .stage_append(append)
            .await?;
        for record in &commit_records {
            stage_test_commit_manifest(&mut writes, record)?;
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit should succeed");
        Ok(())
    }

    fn stage_test_commit_manifest(
        writes: &mut crate::storage_adapter::StorageWriteSet,
        record: &CommitRecord,
    ) -> Result<(), LixError> {
        crate::tracked_state::stage_resealed_commit_state_manifest_for_test(
            writes,
            &CommitStateManifest {
                commit_id: record.commit_id,
                change_account_id: record.account_id.clone(),
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
                id: ChangeId::for_test_label(change_id),
                account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                entity_pk: crate::entity_pk::EntityPk::single(commit_id),
                schema_key: "lix_commit".to_string(),
                file_id: None,
                snapshot: crate::json_store::JsonSlot::None,
                metadata: crate::json_store::JsonSlot::None,
                created_at: ts("2026-01-01T00:00:00Z"),
                origin_key: None,
            },
            parent_commit_ids: parent_commit_ids
                .iter()
                .map(|id| CommitId::for_test_label(id))
                .collect(),
        }
    }
}
