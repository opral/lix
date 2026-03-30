//! Committed-state resolution over canonical history.
//!
//! Semantically, committed meaning/state is resolved from replica-local version
//! heads plus commit-graph facts derived from canonical changes. This module
//! answers that question directly from canonical-owned data, independent of
//! live-state replay status and other derived mirrors.

use std::collections::{BTreeMap, BTreeSet};

use crate::errors::classification::is_missing_relation_error;
use crate::schema::builtin::types::LixCommit;
use crate::{LixBackend, LixError, Value, VersionId};

use super::roots::{load_committed_version_head_commit_id, load_head_commit_id_for_version};
use super::types::{VersionInfo, VersionSnapshot};

const CANONICAL_FALLBACK_MAX_COMMIT_DEPTH: usize = 2048;

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
    // Resolve committed state by walking the canonical commit lineage selected
    // by the requested head commit and then choosing the nearest visible change
    // for the requested entity. This is semantic resolution, not live-state
    // replay.
    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        executor.dialect(),
        "commit_snapshot.content",
        "parent_commit_ids",
        "parent_rows",
        "parent_id",
    );
    let (change_join_sql, change_value_expr, change_position_expr) =
        json_array_text_join_with_position_sql(
            executor.dialect(),
            "commit_snapshot.content",
            "change_ids",
            "change_rows",
            "change_id",
            "change_position",
        );

    let mut filters = vec![
        format!("c.entity_id = '{}'", escape_sql_string(&request.entity_id)),
        format!(
            "c.schema_key = '{}'",
            escape_sql_string(&request.schema_key)
        ),
    ];
    for column in ["file_id", "plugin_key", "schema_version"] {
        let Some(expected) = request.exact_filters.get(column).and_then(text_from_value) else {
            continue;
        };
        filters.push(format!(
            "c.{column} = '{expected}'",
            column = column,
            expected = escape_sql_string(&expected),
        ));
    }

    let sql = format!(
        "WITH RECURSIVE commit_walk(commit_id, depth) AS ( \
           SELECT '{head_commit_id}' AS commit_id, 0 AS depth \
           UNION ALL \
           SELECT {parent_value_expr} AS commit_id, commit_walk.depth + 1 AS depth \
           FROM commit_walk \
           JOIN lix_internal_change commit_change \
             ON commit_change.schema_key = 'lix_commit' \
            AND commit_change.entity_id = commit_walk.commit_id \
           LEFT JOIN lix_internal_snapshot commit_snapshot \
             ON commit_snapshot.id = commit_change.snapshot_id \
           {parent_join_sql} \
           WHERE commit_snapshot.content IS NOT NULL \
             AND {parent_value_expr} IS NOT NULL \
             AND commit_walk.depth < {max_depth} \
         ), reachable_commits AS ( \
           SELECT commit_id, MIN(depth) AS depth \
           FROM commit_walk \
           GROUP BY commit_id \
         ), ranked_changes AS ( \
           SELECT c.entity_id, c.schema_key, c.schema_version, c.file_id, '{version_id}' AS version_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.id AS change_id, reachable_commits.depth, {change_position_expr} AS change_position \
           FROM reachable_commits \
           JOIN lix_internal_change commit_change \
             ON commit_change.schema_key = 'lix_commit' \
            AND commit_change.entity_id = reachable_commits.commit_id \
           LEFT JOIN lix_internal_snapshot commit_snapshot \
             ON commit_snapshot.id = commit_change.snapshot_id \
           {change_join_sql} \
           JOIN lix_internal_change c \
             ON c.id = {change_value_expr} \
           LEFT JOIN lix_internal_snapshot s \
             ON s.id = c.snapshot_id \
           WHERE {filters} \
         ) \
         SELECT entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, metadata, change_id \
         FROM ranked_changes \
         ORDER BY depth ASC, change_position DESC \
         LIMIT 1",
        head_commit_id = escape_sql_string(head_commit_id),
        parent_value_expr = parent_value_expr,
        parent_join_sql = parent_join_sql,
        max_depth = CANONICAL_FALLBACK_MAX_COMMIT_DEPTH,
        version_id = escape_sql_string(&request.version_id),
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
        change_position_expr = change_position_expr,
        filters = filters.join(" AND "),
    );

    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };

    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let entity_id = required_text(row, 0, "committed_state.entity_id")?;
    let schema_key = required_text(row, 1, "committed_state.schema_key")?;
    let schema_version = required_text(row, 2, "committed_state.schema_version")?;
    let file_id = required_text(row, 3, "committed_state.file_id")?;
    let version_id = required_text(row, 4, "committed_state.version_id")?;
    let plugin_key = required_text(row, 5, "committed_state.plugin_key")?;
    let Some(snapshot_content) = row.get(6).and_then(text_from_value) else {
        return Ok(None);
    };
    let metadata = row.get(7).and_then(text_from_value);
    let source_change_id = row.get(8).and_then(text_from_value);

    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
    values.insert("schema_key".to_string(), Value::Text(schema_key.clone()));
    values.insert("schema_version".to_string(), Value::Text(schema_version));
    values.insert("file_id".to_string(), Value::Text(file_id.clone()));
    values.insert("version_id".to_string(), Value::Text(version_id.clone()));
    values.insert("plugin_key".to_string(), Value::Text(plugin_key));
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    if let Some(metadata) = metadata {
        values.insert("metadata".to_string(), Value::Text(metadata));
    }

    Ok(Some(ExactCommittedStateRow {
        entity_id,
        schema_key,
        file_id,
        version_id,
        values,
        source_change_id,
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

fn json_array_text_join_sql(
    dialect: crate::SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
) -> (String, String) {
    match dialect {
        crate::SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
        ),
        crate::SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') AS {alias}({value_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
        ),
    }
}

fn json_array_text_join_with_position_sql(
    dialect: crate::SqlDialect,
    json_column: &str,
    field: &str,
    alias: &str,
    value_column: &str,
    position_column: &str,
) -> (String, String, String) {
    match dialect {
        crate::SqlDialect::Sqlite => (
            format!("JOIN json_each({json_column}, '$.{field}') AS {alias}"),
            format!("{alias}.value"),
            format!("CAST({alias}.key AS INTEGER)"),
        ),
        crate::SqlDialect::Postgres => (
            format!(
                "JOIN LATERAL jsonb_array_elements_text(CAST({json_column} AS JSONB) -> '{field}') WITH ORDINALITY AS {alias}({value_column}, {position_column}) ON TRUE"
            ),
            format!("{alias}.{value_column}"),
            format!("{alias}.{position_column}"),
        ),
    }
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
            backend.count_sql_matching(|sql| sql.contains("WITH RECURSIVE commit_walk")) >= 1,
            "canonical exact-state lookup should read canonical changes"
        );
        assert_eq!(row.entity_id, "file-1");
        assert_eq!(row.source_change_id.as_deref(), Some("change-fallback"));
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
