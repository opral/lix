//! Committed-state resolution over canonical history.
//!
//! Semantically, committed meaning/state is resolved from replica-local version
//! heads plus commit-graph facts derived from canonical changes. This module
//! answers that question directly from canonical-owned data, independent of
//! live-state replay status and other derived mirrors.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::errors::classification::is_missing_relation_error;
use crate::schema::builtin::types::LixCommit;
use crate::{LixBackend, LixError, Value, VersionId};

use super::roots::{load_committed_version_head_commit_id, load_head_commit_id_for_version};
use super::types::{VersionInfo, VersionSnapshot};

/// Canonical committed row resolved from commit-graph facts plus local
/// version-head selection.
///
/// This type intentionally excludes workspace-owned selectors and annotations.
/// Callers that need workspace overlays such as `writer_key` must apply them in
/// a separate effective-state layer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactCommittedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) source_change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitLineageEntry {
    pub(crate) id: String,
    pub(crate) change_set_id: Option<String>,
    pub(crate) change_ids: Vec<String>,
    pub(crate) parent_commit_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedCanonicalChangeRow {
    pub(crate) id: String,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactCommittedStateRowRequest {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) exact_filters: BTreeMap<String, Value>,
}

pub(crate) use crate::backend::QueryExecutor as CommitQueryExecutor;

pub(crate) async fn load_version_info_for_versions(
    executor: &mut dyn CommitQueryExecutor,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let mut versions = BTreeMap::new();
    if version_ids.is_empty() {
        return Ok(versions);
    }

    for version_id in version_ids {
        versions.insert(
            version_id.clone(),
            VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: VersionId::new(version_id.clone())?,
                },
            },
        );
    }
    for version_id in version_ids {
        if let Some(commit_id) = load_committed_version_head_commit_id(executor, version_id).await?
        {
            versions.insert(
                version_id.clone(),
                VersionInfo {
                    parent_commit_ids: vec![commit_id],
                    snapshot: VersionSnapshot {
                        id: VersionId::new(version_id.clone())?,
                    },
                },
            );
        }
    }

    Ok(versions)
}

pub(crate) async fn load_exact_committed_state_row_at_version_head(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let mut executor = backend;
    load_exact_committed_state_row_at_version_head_with_executor(&mut executor, request).await
}

pub(crate) async fn load_exact_committed_state_row_at_version_head_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(head_commit_id) =
        load_head_commit_id_for_version(executor, &request.version_id).await?
    else {
        return Ok(None);
    };

    load_exact_committed_state_row_from_commit_with_executor(executor, &head_commit_id, request)
        .await
}

pub(crate) async fn load_exact_committed_state_row_from_commit_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    head_commit_id: &str,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let lineage = load_reachable_commit_lineage(executor, head_commit_id).await?;
    let reachable_depths = compute_min_commit_depths(head_commit_id, &lineage);
    let mut ordered_commits = reachable_depths.into_iter().collect::<Vec<_>>();
    ordered_commits.sort_by(
        |(left_commit_id, left_depth), (right_commit_id, right_depth)| {
            left_depth
                .cmp(right_depth)
                .then_with(|| left_commit_id.cmp(right_commit_id))
        },
    );

    let mut change_cache = BTreeMap::<String, Option<CommittedCanonicalChangeRow>>::new();
    for (commit_id, _) in ordered_commits {
        let Some(commit) = lineage.get(&commit_id) else {
            return Err(LixError::unknown(format!(
                "committed state traversal lost commit '{}'",
                commit_id
            )));
        };
        for change_id in commit.change_ids.iter().rev() {
            let change = match change_cache.get(change_id) {
                Some(cached) => cached.clone(),
                None => {
                    let loaded = load_canonical_change_row_by_id(executor, change_id).await?;
                    change_cache.insert(change_id.clone(), loaded.clone());
                    loaded
                }
            };
            let Some(change) = change else {
                return Err(LixError::unknown(format!(
                    "canonical commit '{}' references missing change '{}'",
                    commit_id, change_id
                )));
            };
            if !canonical_change_matches_request(&change, request) {
                continue;
            }
            return exact_committed_state_row_from_change(change, &request.version_id);
        }
    }

    Ok(None)
}

async fn load_reachable_commit_lineage(
    executor: &mut dyn CommitQueryExecutor,
    head_commit_id: &str,
) -> Result<BTreeMap<String, CommitLineageEntry>, LixError> {
    let mut lineage = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut stack = vec![(head_commit_id.to_string(), false)];

    while let Some((commit_id, finishing)) = stack.pop() {
        if finishing {
            visiting.remove(&commit_id);
            visited.insert(commit_id);
            continue;
        }
        if visited.contains(&commit_id) {
            continue;
        }
        let Some(mut entry) = load_commit_lineage_entry_by_id(executor, &commit_id).await? else {
            return Err(LixError::unknown(format!(
                "canonical lineage references missing commit '{}'",
                commit_id
            )));
        };
        entry.change_ids.retain(|value| !value.is_empty());
        entry.parent_commit_ids.retain(|value| !value.is_empty());
        if !visiting.insert(commit_id.clone()) {
            return Err(LixError::unknown(format!(
                "canonical commit graph contains a cycle at '{}'",
                commit_id
            )));
        }
        let parent_commit_ids = entry.parent_commit_ids.clone();
        lineage.insert(commit_id.clone(), entry);
        stack.push((commit_id.clone(), true));
        for parent_commit_id in parent_commit_ids.into_iter().rev() {
            if visiting.contains(&parent_commit_id) {
                return Err(LixError::unknown(format!(
                    "canonical commit graph contains a cycle via '{}' -> '{}'",
                    commit_id, parent_commit_id
                )));
            }
            if !visited.contains(&parent_commit_id) {
                stack.push((parent_commit_id, false));
            }
        }
    }

    Ok(lineage)
}

fn compute_min_commit_depths(
    head_commit_id: &str,
    lineage: &BTreeMap<String, CommitLineageEntry>,
) -> BTreeMap<String, usize> {
    let mut depths = BTreeMap::new();
    let mut queue = VecDeque::from([(head_commit_id.to_string(), 0_usize)]);

    while let Some((commit_id, depth)) = queue.pop_front() {
        if matches!(depths.get(&commit_id), Some(existing) if *existing <= depth) {
            continue;
        }
        depths.insert(commit_id.clone(), depth);
        if let Some(entry) = lineage.get(&commit_id) {
            for parent_commit_id in &entry.parent_commit_ids {
                queue.push_back((parent_commit_id.clone(), depth + 1));
            }
        }
    }

    depths
}

fn canonical_change_matches_request(
    change: &CommittedCanonicalChangeRow,
    request: &ExactCommittedStateRowRequest,
) -> bool {
    if change.entity_id != request.entity_id || change.schema_key != request.schema_key {
        return false;
    }
    for column in ["file_id", "plugin_key", "schema_version"] {
        let Some(expected) = request.exact_filters.get(column).and_then(text_from_value) else {
            continue;
        };
        let actual = match column {
            "file_id" => &change.file_id,
            "plugin_key" => &change.plugin_key,
            "schema_version" => &change.schema_version,
            _ => continue,
        };
        if actual != &expected {
            return false;
        }
    }
    true
}

fn exact_committed_state_row_from_change(
    change: CommittedCanonicalChangeRow,
    version_id: &str,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(snapshot_content) = change.snapshot_content else {
        return Ok(None);
    };

    let mut values = BTreeMap::new();
    values.insert(
        "entity_id".to_string(),
        Value::Text(change.entity_id.clone()),
    );
    values.insert(
        "schema_key".to_string(),
        Value::Text(change.schema_key.clone()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(change.schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(change.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(version_id.to_string()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(change.plugin_key.clone()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    if let Some(metadata) = change.metadata.clone() {
        values.insert("metadata".to_string(), Value::Text(metadata));
    }

    Ok(Some(ExactCommittedStateRow {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        file_id: change.file_id,
        version_id: version_id.to_string(),
        values,
        source_change_id: Some(change.id),
    }))
}

pub(crate) async fn load_commit_lineage_entry_by_id(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
) -> Result<Option<CommitLineageEntry>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = 'lix_commit' \
           AND c.entity_id = '{commit_id}' \
           AND c.file_id = 'lix' \
           AND c.plugin_key = 'lix' \
           AND s.content IS NOT NULL \
         LIMIT 1",
        commit_id = escape_sql_string(commit_id),
    );
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    let Some(snapshot_content) = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(text_from_value)
    else {
        return Ok(None);
    };
    let parsed: LixCommit = serde_json::from_str(&snapshot_content).map_err(|error| {
        LixError::unknown(format!(
            "commit snapshot_content invalid JSON for '{}': {error}",
            commit_id
        ))
    })?;
    Ok(Some(CommitLineageEntry {
        id: parsed.id,
        change_set_id: parsed.change_set_id,
        change_ids: parsed.change_ids,
        parent_commit_ids: parsed.parent_commit_ids,
    }))
}

pub(crate) async fn load_canonical_change_row_by_id(
    executor: &mut dyn CommitQueryExecutor,
    change_id: &str,
) -> Result<Option<CommittedCanonicalChangeRow>, LixError> {
    let sql = "SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content, c.metadata, c.created_at \
               FROM lix_internal_change c \
               LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
               WHERE c.id = $1 \
               LIMIT 1";
    let result = executor
        .execute(sql, &[Value::Text(change_id.to_string())])
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(CommittedCanonicalChangeRow {
        id: required_text(row, 0, "lix_internal_change.id")?,
        entity_id: required_text(row, 1, "lix_internal_change.entity_id")?,
        schema_key: required_text(row, 2, "lix_internal_change.schema_key")?,
        schema_version: required_text(row, 3, "lix_internal_change.schema_version")?,
        file_id: required_text(row, 4, "lix_internal_change.file_id")?,
        plugin_key: required_text(row, 5, "lix_internal_change.plugin_key")?,
        snapshot_content: row.get(6).and_then(text_from_value),
        metadata: row.get(7).and_then(text_from_value),
        created_at: required_text(row, 8, "lix_internal_change.created_at")?,
    }))
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn required_text(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError::unknown(format!("{field} is empty"))),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text-like value for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing {field}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::canonical::roots::load_head_commit_id_for_version;
    use crate::test_support::{
        init_test_backend_core, seed_canonical_change_row, seed_local_version_head,
        CanonicalChangeSeed, TestSqliteBackend,
    };
    use std::collections::BTreeMap;

    async fn init_state_source_backend() -> TestSqliteBackend {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        backend
    }

    async fn seed_committed_history_fixture(
        backend: &TestSqliteBackend,
        include_local_head: bool,
    ) -> Result<(), LixError> {
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-fallback",
                entity_id: "file-1",
                schema_key: "lix_file_descriptor",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-file-1",
                snapshot_content: Some(
                    "{\"id\":\"file-1\",\"directory_id\":null,\"name\":\"contract\",\"extension\":null,\"metadata\":{\"k\":\"v\"},\"hidden\":false}",
                ),
                metadata: Some("{\"k\":\"v\"}"),
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await?;
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-parent-1",
                entity_id: "parent-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-parent-1",
                snapshot_content: Some(
                    "{\"id\":\"parent-1\",\"change_set_id\":\"cs-parent\",\"change_ids\":[],\"parent_commit_ids\":[]}",
                ),
                metadata: None,
                created_at: "2026-03-30T00:00:30Z",
            },
        )
        .await?;
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-commit-1",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-commit-1",
                snapshot_content: Some(
                    "{\"id\":\"commit-1\",\"change_set_id\":\"cs-1\",\"change_ids\":[\"change-fallback\"],\"parent_commit_ids\":[\"parent-1\"]}",
                ),
                metadata: None,
                created_at: "2026-03-30T00:01:00Z",
            },
        )
        .await?;
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-commit-2",
                entity_id: "commit-2",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-commit-2",
                snapshot_content: Some(
                    "{\"id\":\"commit-2\",\"change_set_id\":\"cs-2\",\"change_ids\":[],\"parent_commit_ids\":[\"commit-1\"]}",
                ),
                metadata: None,
                created_at: "2026-03-30T00:02:00Z",
            },
        )
        .await?;
        if include_local_head {
            seed_local_version_head(backend, "v1", "commit-2", "2026-03-30T00:03:00Z").await?;
        }
        Ok(())
    }

    fn synthetic_timestamp(step: usize) -> String {
        let day = 1 + (step / (24 * 60));
        let hour = (step / 60) % 24;
        let minute = step % 60;
        format!("2026-03-{day:02}T{hour:02}:{minute:02}:00Z")
    }

    async fn seed_deep_committed_history_fixture(
        backend: &TestSqliteBackend,
        depth: usize,
    ) -> Result<(), LixError> {
        seed_canonical_change_row(
            backend,
            CanonicalChangeSeed {
                id: "change-deep-root",
                entity_id: "file-1",
                schema_key: "lix_file_descriptor",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-deep-root",
                snapshot_content: Some(
                    "{\"id\":\"file-1\",\"directory_id\":null,\"name\":\"deep\",\"extension\":null,\"metadata\":null,\"hidden\":false}",
                ),
                metadata: None,
                created_at: &synthetic_timestamp(0),
            },
        )
        .await?;

        for index in 0..=depth {
            let parent_commit_ids = if index == 0 {
                "[]".to_string()
            } else {
                format!("[\"commit-{}\"]", index - 1)
            };
            let change_ids = if index == 0 {
                "[\"change-deep-root\"]".to_string()
            } else {
                "[]".to_string()
            };
            let snapshot_content = format!(
                "{{\"id\":\"commit-{index}\",\"change_set_id\":\"cs-{index}\",\"change_ids\":{change_ids},\"parent_commit_ids\":{parent_commit_ids}}}"
            );
            let change_id = format!("change-commit-{index}");
            let snapshot_id = format!("snapshot-commit-{index}");
            let created_at = synthetic_timestamp(index + 1);
            seed_canonical_change_row(
                backend,
                CanonicalChangeSeed {
                    id: &change_id,
                    entity_id: &format!("commit-{index}"),
                    schema_key: "lix_commit",
                    schema_version: "1",
                    file_id: "lix",
                    plugin_key: "lix",
                    snapshot_id: &snapshot_id,
                    snapshot_content: Some(&snapshot_content),
                    metadata: None,
                    created_at: &created_at,
                },
            )
            .await?;
        }

        seed_local_version_head(
            backend,
            "v1",
            &format!("commit-{depth}"),
            &synthetic_timestamp(depth + 2),
        )
        .await?;
        Ok(())
    }

    fn exact_file_descriptor_request() -> ExactCommittedStateRowRequest {
        ExactCommittedStateRowRequest {
            entity_id: "file-1".to_string(),
            schema_key: "lix_file_descriptor".to_string(),
            version_id: "v1".to_string(),
            exact_filters: BTreeMap::from([
                ("file_id".to_string(), Value::Text("lix".to_string())),
                ("plugin_key".to_string(), Value::Text("lix".to_string())),
                ("schema_version".to_string(), Value::Text("1".to_string())),
            ]),
        }
    }

    #[tokio::test]
    async fn canonical_version_head_contract_does_not_fall_back_when_local_head_is_absent() {
        let backend = init_state_source_backend().await;
        seed_committed_history_fixture(&backend, false)
            .await
            .expect("canonical history fixture should seed");
        backend.clear_query_log();

        let mut executor = &backend;
        let commit_id = load_head_commit_id_for_version(&mut executor, "v1")
            .await
            .expect("canonical version head lookup should succeed");

        assert!(
            backend.count_sql_matching(|sql| sql.contains("WITH RECURSIVE commit_walk")) == 0,
            "local version-head lookup should not infer a fallback from canonical changes"
        );
        assert!(commit_id.is_none());
    }

    #[tokio::test]
    async fn canonical_exact_state_contract_walks_commit_history() {
        let backend = init_state_source_backend().await;
        seed_committed_history_fixture(&backend, true)
            .await
            .expect("canonical history fixture should seed");
        backend.clear_query_log();

        let row = load_exact_committed_state_row_at_version_head(
            &backend,
            &exact_file_descriptor_request(),
        )
        .await
        .expect("canonical exact-state lookup should succeed")
        .expect("canonical exact-state lookup should return a row");

        assert!(
            backend.count_sql_matching(|sql| sql.contains("FROM lix_internal_change c")) >= 1,
            "canonical exact-state lookup should read canonical journal rows"
        );
        assert_eq!(
            backend.count_sql_matching(|sql| sql.contains("WITH RECURSIVE commit_walk")),
            0,
            "canonical exact-state lookup should no longer rely on hidden recursive SQL fallback"
        );
        assert_eq!(
            backend.count_sql_matching(|sql| {
                sql.contains("lix_internal_live_v1_lix_commit")
                    || sql.contains("lix_internal_live_v1_lix_commit_edge")
                    || sql.contains("lix_internal_live_v1_lix_change_set")
                    || sql.contains("lix_internal_live_v1_lix_change_set_element")
            }),
            0,
            "canonical exact-state lookup should not depend on live commit-family mirrors"
        );
        assert_eq!(row.entity_id, "file-1");
        assert_eq!(row.source_change_id.as_deref(), Some("change-fallback"));
    }

    #[tokio::test]
    async fn canonical_exact_state_contract_resolves_history_beyond_old_depth_limit() {
        let backend = init_state_source_backend().await;
        seed_deep_committed_history_fixture(&backend, 2055)
            .await
            .expect("deep canonical history fixture should seed");

        let row = load_exact_committed_state_row_at_version_head(
            &backend,
            &exact_file_descriptor_request(),
        )
        .await
        .expect("deep canonical exact-state lookup should succeed")
        .expect("deep canonical exact-state lookup should return a row");

        assert_eq!(row.entity_id, "file-1");
        assert_eq!(row.source_change_id.as_deref(), Some("change-deep-root"));
    }

    #[tokio::test]
    async fn canonical_exact_state_contract_rejects_commit_cycles_explicitly() {
        let backend = init_state_source_backend().await;
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-cycle-row",
                entity_id: "file-1",
                schema_key: "lix_file_descriptor",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-cycle-row",
                snapshot_content: Some(
                    "{\"id\":\"file-1\",\"directory_id\":null,\"name\":\"cycle\",\"extension\":null,\"metadata\":null,\"hidden\":false}",
                ),
                metadata: None,
                created_at: "2026-03-30T01:00:00Z",
            },
        )
        .await
        .expect("cycle fixture change should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-cycle-commit-1",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-cycle-commit-1",
                snapshot_content: Some(
                    "{\"id\":\"commit-1\",\"change_set_id\":\"cs-1\",\"change_ids\":[\"change-cycle-row\"],\"parent_commit_ids\":[\"commit-2\"]}",
                ),
                metadata: None,
                created_at: "2026-03-30T01:01:00Z",
            },
        )
        .await
        .expect("cycle fixture commit-1 should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-cycle-commit-2",
                entity_id: "commit-2",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-cycle-commit-2",
                snapshot_content: Some(
                    "{\"id\":\"commit-2\",\"change_set_id\":\"cs-2\",\"change_ids\":[],\"parent_commit_ids\":[\"commit-1\"]}",
                ),
                metadata: None,
                created_at: "2026-03-30T01:02:00Z",
            },
        )
        .await
        .expect("cycle fixture commit-2 should seed");
        seed_local_version_head(&backend, "v1", "commit-2", "2026-03-30T01:03:00Z")
            .await
            .expect("cycle fixture local head should seed");

        let error = load_exact_committed_state_row_at_version_head(
            &backend,
            &exact_file_descriptor_request(),
        )
        .await
        .expect_err("cycle fixture should fail explicitly");
        assert!(
            error.description.contains("cycle"),
            "expected explicit cycle error, got: {}",
            error.description
        );
    }

    #[tokio::test]
    async fn canonical_exact_state_contract_rejects_missing_parent_commit_explicitly() {
        let backend = init_state_source_backend().await;
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-missing-parent-row",
                entity_id: "file-1",
                schema_key: "lix_file_descriptor",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-missing-parent-row",
                snapshot_content: Some(
                    "{\"id\":\"file-1\",\"directory_id\":null,\"name\":\"broken-parent\",\"extension\":null,\"metadata\":null,\"hidden\":false}",
                ),
                metadata: None,
                created_at: "2026-03-30T02:00:00Z",
            },
        )
        .await
        .expect("missing-parent fixture change should seed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-missing-parent-commit",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-missing-parent-commit",
                snapshot_content: Some(
                    "{\"id\":\"commit-1\",\"change_set_id\":\"cs-1\",\"change_ids\":[\"change-missing-parent-row\"],\"parent_commit_ids\":[\"missing-parent\"]}",
                ),
                metadata: None,
                created_at: "2026-03-30T02:01:00Z",
            },
        )
        .await
        .expect("missing-parent fixture commit should seed");
        seed_local_version_head(&backend, "v1", "commit-1", "2026-03-30T02:02:00Z")
            .await
            .expect("missing-parent fixture local head should seed");

        let error = load_exact_committed_state_row_at_version_head(
            &backend,
            &exact_file_descriptor_request(),
        )
        .await
        .expect_err("missing-parent fixture should fail explicitly");
        assert!(
            error
                .description
                .contains("missing commit 'missing-parent'"),
            "expected explicit missing-parent error, got: {}",
            error.description
        );
    }

    #[tokio::test]
    async fn canonical_commit_lineage_contract_reads_commit_snapshot_from_journal() {
        let backend = init_state_source_backend().await;
        seed_committed_history_fixture(&backend, true)
            .await
            .expect("canonical history fixture should seed");

        let mut executor = &backend;
        let entry = load_commit_lineage_entry_by_id(&mut executor, "commit-1")
            .await
            .expect("canonical lineage lookup should succeed")
            .expect("canonical lineage lookup should return a row");

        assert_eq!(entry.id, "commit-1");
        assert_eq!(entry.change_ids, vec!["change-fallback".to_string()]);
        assert_eq!(entry.parent_commit_ids, vec!["parent-1".to_string()]);
    }

    #[tokio::test]
    async fn canonical_exact_state_rows_do_not_carry_workspace_writer_key_annotation() {
        let backend = init_state_source_backend().await;
        seed_committed_history_fixture(&backend, true)
            .await
            .expect("canonical history fixture should seed");

        let row = load_exact_committed_state_row_at_version_head(
            &backend,
            &exact_file_descriptor_request(),
        )
        .await
        .expect("canonical exact-state lookup should succeed")
        .expect("canonical exact-state lookup should return a row");

        assert!(
            !row.values.contains_key("writer_key"),
            "canonical committed rows should not carry workspace writer_key annotation"
        );
    }
}
