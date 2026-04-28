use std::collections::{BTreeMap, BTreeSet};

use crate::backend::KvStore;
use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
use crate::engine2::commit_graph::{CommitGraphCommit, CommitGraphEntity};
use crate::LixError;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

/// Read model for resolving changelog commits into entity state at a head.
///
/// This module does not own durable storage. It reads immutable changelog facts
/// through a caller-provided KV store and applies commit graph rules on top.
pub(crate) struct CommitGraphContext {
    changelog: ChangelogContext,
}

impl CommitGraphContext {
    pub(crate) fn new(changelog: ChangelogContext) -> Self {
        Self { changelog }
    }

    /// Creates a graph reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommitGraphReader<S>
    where
        S: KvStore,
    {
        CommitGraphReader {
            changelog: self.changelog.reader(store),
        }
    }
}

/// Reader that resolves entities at a commit head.
pub(crate) struct CommitGraphReader<S>
where
    S: KvStore,
{
    changelog: crate::engine2::changelog::ChangelogReader<S>,
}

impl<S> CommitGraphReader<S>
where
    S: KvStore,
{
    /// Returns the canonical entities that are effective at `head_commit_id`.
    ///
    /// Reachable commits are visited nearest-first. For each commit, the commit
    /// row itself is visible, then member `change_ids` are visited in reverse
    /// order so later writes in the same commit win.
    pub(crate) async fn entities_at(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<CommitGraphEntity>, LixError> {
        let commits = self.reachable_commits(head_commit_id).await?;
        self.select_entities(commits).await
    }

    /// Loads and parses a `lix_commit` canonical change by commit id.
    async fn load_commit(
        &mut self,
        commit_id: &str,
    ) -> Result<Option<CommitGraphCommit>, LixError> {
        let Some(change) = find_commit_change(&mut self.changelog, commit_id).await? else {
            return Ok(None);
        };
        parse_commit_change(change).map(Some)
    }

    /// Walks from `head_commit_id` through parent commits and records nearest depth.
    async fn reachable_commits(
        &mut self,
        head_commit_id: &str,
    ) -> Result<Vec<ReachableCommit>, LixError> {
        let mut loader = CommitTraversalLoader::new(self);
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
                ReachableCommit { commit, depth }
            })
            .collect::<Vec<_>>();
        commits.sort_by(|left, right| {
            left.depth
                .cmp(&right.depth)
                .then_with(|| left.commit.commit_id.cmp(&right.commit.commit_id))
        });
        Ok(commits)
    }

    /// Selects the first reachable change for each canonical entity identity.
    async fn select_entities(
        &mut self,
        commits: Vec<ReachableCommit>,
    ) -> Result<Vec<CommitGraphEntity>, LixError> {
        let mut order = Vec::new();
        let mut entities = BTreeMap::new();

        for reachable in commits {
            let depth = reachable.depth;
            let source_commit_id = reachable.commit.commit_id;

            observe_change(
                &mut order,
                &mut entities,
                reachable.commit.change,
                source_commit_id.clone(),
                depth,
            );

            for change_id in reachable.commit.change_ids.iter().rev() {
                let change = self
                    .load_member_change(change_id, &source_commit_id)
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

    async fn load_member_change(
        &mut self,
        change_id: &str,
        source_commit_id: &str,
    ) -> Result<CanonicalChange, LixError> {
        self.changelog
            .load_change(change_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "commit_graph commit '{source_commit_id}' references missing change '{change_id}'"
                    ),
                )
            })
    }
}

#[derive(Debug)]
struct ReachableCommit {
    commit: CommitGraphCommit,
    depth: u32,
}

struct CommitTraversalLoader<'a, S>
where
    S: KvStore,
{
    reader: &'a mut CommitGraphReader<S>,
    loaded: BTreeMap<String, CommitGraphCommit>,
}

impl<'a, S> CommitTraversalLoader<'a, S>
where
    S: KvStore,
{
    fn new(reader: &'a mut CommitGraphReader<S>) -> Self {
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

fn observe_change(
    order: &mut Vec<CanonicalEntityIdentity>,
    entities: &mut BTreeMap<CanonicalEntityIdentity, EntityAccumulator>,
    change: CanonicalChange,
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
    entity_id: String,
    schema_key: String,
    file_id: Option<String>,
}

impl CanonicalEntityIdentity {
    fn from_change(change: &CanonicalChange) -> Self {
        Self {
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
        }
    }
}

async fn find_commit_change<S>(
    changelog: &mut crate::engine2::changelog::ChangelogReader<S>,
    commit_id: &str,
) -> Result<Option<crate::engine2::changelog::CanonicalChange>, LixError>
where
    S: KvStore,
{
    let changes = changelog
        .scan_changes(&crate::engine2::changelog::ChangelogScanRequest { limit: None })
        .await?;
    Ok(changes
        .into_iter()
        .find(|change| change.schema_key == COMMIT_SCHEMA_KEY && change.entity_id == commit_id))
}

fn parse_commit_change(
    change: crate::engine2::changelog::CanonicalChange,
) -> Result<CommitGraphCommit, LixError> {
    if change.schema_key != COMMIT_SCHEMA_KEY {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "commit_graph expected schema_key '{COMMIT_SCHEMA_KEY}' but got '{}'",
                change.schema_key
            ),
        ));
    }

    let snapshot_content = change.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "commit_graph commit '{}' is missing snapshot_content",
                change.entity_id
            ),
        )
    })?;
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "commit_graph commit '{}' snapshot_content is invalid JSON: {error}",
                    change.entity_id
                ),
            )
        })?;

    let commit_id = required_string(&snapshot, "id", &change.entity_id)?;
    if commit_id != change.entity_id {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "commit_graph commit change entity_id '{}' does not match snapshot id '{}'",
                change.entity_id, commit_id
            ),
        ));
    }

    let change_ids = required_string_array(&snapshot, "change_ids", &change.entity_id)?;
    let parent_commit_ids =
        required_string_array(&snapshot, "parent_commit_ids", &change.entity_id)?;

    Ok(CommitGraphCommit {
        change,
        commit_id,
        change_ids,
        parent_commit_ids,
    })
}

fn required_string(
    snapshot: &serde_json::Value,
    field: &str,
    commit_id: &str,
) -> Result<String, LixError> {
    snapshot
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("commit_graph commit '{commit_id}' requires string field '{field}'"),
            )
        })
}

fn required_string_array(
    snapshot: &serde_json::Value,
    field: &str,
    commit_id: &str,
) -> Result<Vec<String>, LixError> {
    let values = snapshot
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("commit_graph commit '{commit_id}' requires array field '{field}'"),
            )
        })?;

    values
        .iter()
        .map(|value| {
            value.as_str().filter(|value| !value.is_empty()).map(str::to_string).ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "commit_graph commit '{commit_id}' field '{field}' must contain only non-empty strings"
                    ),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
    use crate::engine2::commit_graph::CommitGraphContext;

    #[tokio::test]
    async fn load_commit_parses_commit_snapshot() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[commit_change(
                "commit-1-change",
                "commit-1",
                &["change-1", "change-2"],
                &["parent-1"],
            )],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let graph = CommitGraphContext::new(ChangelogContext::new());
        let mut reader = graph.reader(backend);

        let commit = reader
            .load_commit("missing")
            .await
            .expect("commit load should succeed");

        assert_eq!(commit, None);
    }

    #[tokio::test]
    async fn load_commit_rejects_malformed_snapshot() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[CanonicalChange {
                id: "commit-1-change".to_string(),
                entity_id: "commit-1".to_string(),
                schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some("{\"id\":\"commit-1\"}".to_string()),
                metadata: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            }],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
        let error = reader
            .load_commit("commit-1")
            .await
            .expect_err("malformed commit should fail");

        assert!(error.description.contains("change_ids"));
    }

    #[tokio::test]
    async fn entities_at_walks_ancestors_and_computes_nearest_depth() {
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
    async fn entities_at_errors_on_missing_parent_commit() {
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
            .entities_at("commit-head")
            .await
            .expect_err("missing parent should fail");

        assert!(error.description.contains("missing-parent"));
    }

    #[tokio::test]
    async fn entities_at_errors_on_cycle() {
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
            .entities_at("commit-a")
            .await
            .expect_err("cycle should fail");

        assert!(error.description.contains("cycle"));
    }

    #[tokio::test]
    async fn entities_at_selects_nearest_member_change_for_identity() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[
                entity_change_with_file_and_plugin(
                    "change-file-a",
                    "entity-1",
                    "test_schema",
                    Some("file-a"),
                    None,
                    "{\"value\":\"file-a\"}",
                ),
                entity_change_with_file_and_plugin(
                    "change-file-b",
                    "entity-1",
                    "test_schema",
                    Some("file-b"),
                    None,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
    async fn entities_at_does_not_distinguish_same_entity_with_different_plugin_key() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[
                entity_change_with_file_and_plugin(
                    "change-plugin-a",
                    "entity-1",
                    "test_schema",
                    None,
                    Some("plugin-a"),
                    "{\"value\":\"plugin-a\"}",
                ),
                entity_change_with_file_and_plugin(
                    "change-plugin-b",
                    "entity-1",
                    "test_schema",
                    None,
                    Some("plugin-b"),
                    "{\"value\":\"plugin-b\"}",
                ),
                commit_change(
                    "commit-head-change",
                    "commit-head",
                    &["change-plugin-a", "change-plugin-b"],
                    &[],
                ),
            ],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
        let entities = reader
            .entities_at("commit-head")
            .await
            .expect("entities should resolve");
        let entity = entities
            .iter()
            .find(|entity| entity.change.schema_key == "test_schema")
            .expect("plugin-key entity should resolve");

        assert_eq!(
            entity_ids_for_schema(&entities, "test_schema"),
            vec![("change-plugin-b".to_string(), "commit-head".to_string(), 0)]
        );
        assert_eq!(entity.change.plugin_key.as_deref(), Some("plugin-b"));
    }

    #[tokio::test]
    async fn entities_at_head_tombstone_hides_parent_entity() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
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

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        assert_eq!(entity.change.snapshot_content, None);
    }

    #[tokio::test]
    async fn entities_at_includes_reachable_commit_rows() {
        let backend = Arc::new(UnitTestBackend::new());
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[
                commit_change("commit-root-change", "commit-root", &[], &[]),
                commit_change("commit-head-change", "commit-head", &[], &["commit-root"]),
            ],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
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
        let changelog = ChangelogContext::new();
        append_changes(
            Arc::clone(&backend),
            &changelog,
            &[commit_change(
                "commit-head-change",
                "commit-head",
                &["missing-change"],
                &[],
            )],
        )
        .await;

        let graph = CommitGraphContext::new(changelog);
        let mut reader = graph.reader(backend);
        let error = reader
            .entities_at("commit-head")
            .await
            .expect_err("missing member change should fail");

        assert!(error.description.contains("missing-change"));
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
            schema_key: super::COMMIT_SCHEMA_KEY.to_string(),
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

    fn entity_change(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        snapshot_content: &str,
    ) -> CanonicalChange {
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
    ) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            created_at: created_at.to_string(),
        }
    }

    fn entity_change_with_file_and_plugin(
        change_id: &str,
        entity_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
        plugin_key: Option<&str>,
        snapshot_content: &str,
    ) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: file_id.map(str::to_string),
            plugin_key: plugin_key.map(str::to_string),
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    fn entity_tombstone(change_id: &str, entity_id: &str, schema_key: &str) -> CanonicalChange {
        CanonicalChange {
            id: change_id.to_string(),
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: None,
            metadata: None,
            created_at: "2026-01-02T00:00:00Z".to_string(),
        }
    }

    fn entity_ids_for_schema(
        entities: &[crate::engine2::commit_graph::CommitGraphEntity],
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
