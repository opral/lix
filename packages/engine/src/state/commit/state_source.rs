use std::collections::{BTreeMap, BTreeSet};

use crate::schema::builtin::types::LixVersionRef;
use crate::schema::live_store::{
    load_exact_live_row_with_executor, logical_snapshot_text, LiveRowScope,
};
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, Value};

use super::types::{VersionInfo, VersionSnapshot};

#[cfg(test)]
use crate::errors::classification::is_missing_relation_error;
#[cfg(test)]
use crate::schema::live_layout::{logical_snapshot_text_from_projected_row, LiveRowAccess};

#[cfg(test)]
const CANONICAL_FALLBACK_MAX_COMMIT_DEPTH: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactCommittedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) source_change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactCommittedStateRowRequest {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) exact_filters: BTreeMap<String, Value>,
}

pub(crate) use crate::backend::QueryExecutor as CommitQueryExecutor;

pub(crate) async fn load_committed_version_head_commit_id_from_live_state(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let snapshot_content = match load_current_pointer_snapshot_content(
        executor,
        version_ref_schema_key(),
        version_id,
        version_ref_file_id(),
        version_ref_plugin_key(),
        version_ref_storage_version_id(),
    )
    .await?
    {
        Some(snapshot_content) => Some(snapshot_content),
        None => None,
    };
    if let Some(snapshot_content) = snapshot_content {
        let Some(pointer) = parse_version_ref_snapshot(&snapshot_content)? else {
            return Ok(None);
        };
        if pointer.commit_id.is_empty() {
            return Ok(None);
        }
        return Ok(Some(pointer.commit_id));
    }

    Ok(None)
}

#[cfg(test)]
pub(crate) async fn reconstruct_committed_version_head_commit_id_from_canonical(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let Some(snapshot_content) =
        load_version_ref_snapshot_content_from_canonical(executor, version_id).await?
    else {
        return Ok(None);
    };
    let Some(pointer) = parse_version_ref_snapshot(&snapshot_content)? else {
        return Ok(None);
    };
    if pointer.commit_id.is_empty() {
        return Ok(None);
    }
    Ok(Some(pointer.commit_id))
}

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
                    id: version_id.clone(),
                },
            },
        );
    }
    for version_id in version_ids {
        if let Some(commit_id) =
            load_committed_version_head_commit_id_from_live_state(executor, version_id).await?
        {
            versions.insert(
                version_id.clone(),
                VersionInfo {
                    parent_commit_ids: vec![commit_id],
                    snapshot: VersionSnapshot {
                        id: version_id.clone(),
                    },
                },
            );
        }
    }

    Ok(versions)
}

async fn load_current_pointer_snapshot_content(
    executor: &mut dyn CommitQueryExecutor,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    storage_version_id: &str,
) -> Result<Option<Value>, LixError> {
    let filters = BTreeMap::from([
        ("entity_id", entity_id.to_string()),
        ("file_id", file_id.to_string()),
        ("plugin_key", plugin_key.to_string()),
        ("version_id", storage_version_id.to_string()),
    ]);
    let Some(row) =
        load_exact_live_row_with_executor(executor, LiveRowScope::Untracked, schema_key, &filters)
            .await?
    else {
        return Ok(None);
    };
    let access =
        crate::schema::live_layout::load_live_row_access_with_executor(executor, schema_key)
            .await?;
    logical_snapshot_text(&access, &row).map(|snapshot| snapshot.map(Value::Text))
}

pub(crate) async fn load_exact_committed_state_row_from_live_state(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let mut executor = backend;
    load_exact_committed_state_row_from_live_state_with_executor(&mut executor, request).await
}

pub(crate) async fn load_exact_committed_state_row_from_live_state_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let mut filters = BTreeMap::from([
        ("entity_id", request.entity_id.clone()),
        ("version_id", request.version_id.clone()),
    ]);
    for column in ["file_id", "plugin_key", "schema_version"] {
        let Some(expected) = request.exact_filters.get(column).and_then(text_from_value) else {
            continue;
        };
        filters.insert(column, expected);
    }
    let Some(row) = load_exact_live_row_with_executor(
        executor,
        LiveRowScope::Tracked,
        &request.schema_key,
        &filters,
    )
    .await?
    else {
        return Ok(None);
    };
    let access = crate::schema::live_layout::load_live_row_access_with_executor(
        executor,
        &request.schema_key,
    )
    .await?;
    let Some(snapshot_content) = logical_snapshot_text(&access, &row)? else {
        return Ok(None);
    };
    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(row.entity_id.clone()));
    values.insert(
        "schema_key".to_string(),
        Value::Text(row.schema_key.clone()),
    );
    values.insert(
        "schema_version".to_string(),
        Value::Text(row.schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(row.file_id.clone()));
    values.insert(
        "version_id".to_string(),
        Value::Text(row.version_id.clone()),
    );
    values.insert(
        "plugin_key".to_string(),
        Value::Text(row.plugin_key.clone()),
    );
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    if let Some(metadata) = row.metadata.clone() {
        values.insert("metadata".to_string(), Value::Text(metadata));
    }
    Ok(Some(ExactCommittedStateRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        file_id: row.file_id,
        version_id: row.version_id,
        values,
        source_change_id: row.change_id,
    }))
}

#[cfg(test)]
async fn load_version_ref_snapshot_content_from_canonical(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<Value>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND s.content IS NOT NULL \
         ORDER BY c.created_at DESC, c.id DESC \
         LIMIT 1",
        schema_key = escape_sql_string(version_ref_schema_key()),
        entity_id = escape_sql_string(version_id),
        file_id = escape_sql_string(version_ref_file_id()),
        plugin_key = escape_sql_string(version_ref_plugin_key()),
    );

    match executor.execute(&sql, &[]).await {
        Ok(result) => Ok(result.rows.first().and_then(|row| row.first()).cloned()),
        Err(err) if is_missing_relation_error(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
pub(crate) async fn reconstruct_exact_committed_state_row_from_canonical(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(head_commit_id) =
        reconstruct_committed_version_head_commit_id_from_canonical(executor, &request.version_id)
            .await?
    else {
        return Ok(None);
    };

    let (parent_join_sql, parent_value_expr) = json_array_text_join_sql(
        executor.dialect(),
        "commit_snapshot.content",
        "parent_commit_ids",
        "parent_rows",
        "parent_id",
    );
    let (change_join_sql, change_value_expr) = json_array_text_join_sql(
        executor.dialect(),
        "commit_snapshot.content",
        "change_ids",
        "change_rows",
        "change_id",
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
           SELECT c.entity_id, c.schema_key, c.schema_version, c.file_id, '{version_id}' AS version_id, c.plugin_key, s.content AS snapshot_content, c.metadata, c.id AS change_id, reachable_commits.depth, c.created_at \
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
         ORDER BY depth ASC, created_at DESC, change_id DESC \
         LIMIT 1",
        head_commit_id = escape_sql_string(&head_commit_id),
        parent_value_expr = parent_value_expr,
        parent_join_sql = parent_join_sql,
        max_depth = CANONICAL_FALLBACK_MAX_COMMIT_DEPTH,
        version_id = escape_sql_string(&request.version_id),
        change_join_sql = change_join_sql,
        change_value_expr = change_value_expr,
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
    exact_committed_state_row_from_row(row, None, 6, 0)
}

#[cfg(test)]
fn exact_committed_state_row_from_row(
    row: &[Value],
    access: Option<&LiveRowAccess>,
    snapshot_index: usize,
    normalized_start_index: usize,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(entity_id) = row.first().and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(schema_key) = row.get(1).and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(schema_version) = row.get(2).and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(file_id) = row.get(3).and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(version_id) = row.get(4).and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(plugin_key) = row.get(5).and_then(text_from_value) else {
        return Ok(None);
    };
    let Some(snapshot_content) = logical_snapshot_text_from_projected_row(
        access,
        &schema_key,
        row,
        snapshot_index,
        normalized_start_index,
    )?
    else {
        return Ok(None);
    };
    let metadata_index = if access.is_some() { 6 } else { 7 };
    let source_change_id_index = if access.is_some() { 7 } else { 8 };
    let metadata = row.get(metadata_index).and_then(text_from_value);
    let source_change_id = row.get(source_change_id_index).and_then(text_from_value);

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

fn parse_version_ref_snapshot(value: &Value) -> Result<Option<LixVersionRef>, LixError> {
    let raw_snapshot = match value {
        Value::Text(value) => value,
        Value::Null => return Ok(None),
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "version ref snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionRef = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("version ref snapshot_content invalid JSON: {error}"),
    })?;
    Ok(Some(snapshot))
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

#[cfg(test)]
fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::LixTransaction;
    use crate::schema::live_layout::builtin_live_table_layout;
    use crate::QueryResult;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn mock_normalized_live_row(schema_key: &str) -> (Vec<Value>, String) {
        let layout = builtin_live_table_layout(schema_key)
            .expect("builtin live layout should compile")
            .expect("builtin schema should exist");
        let access = LiveRowAccess::new(layout.clone());
        let mut row = vec![
            Value::Text("file-1".to_string()),
            Value::Text(schema_key.to_string()),
            Value::Text("1".to_string()),
            Value::Text("lix".to_string()),
            Value::Text("v1".to_string()),
            Value::Text("lix".to_string()),
            Value::Text("{\"k\":\"v\"}".to_string()),
            Value::Text("change-1".to_string()),
        ];
        for column in &layout.columns {
            let value = if column.property_name == "id" {
                Value::Text("file-1".to_string())
            } else {
                Value::Null
            };
            row.push(value);
        }
        let snapshot = access
            .logical_snapshot_text_from_row(&row, 8)
            .expect("live snapshot reconstruction should succeed")
            .expect("live snapshot should be present");
        (row, snapshot)
    }

    struct ExactCommittedStateBackend {
        live_table_query_seen: Arc<AtomicBool>,
    }

    struct UnusedTransaction;

    #[async_trait(?Send)]
    impl LixBackend for ExactCommittedStateBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"") {
                self.live_table_query_seen.store(true, Ordering::SeqCst);
                let (row, _) = mock_normalized_live_row("lix_file_descriptor");
                return Ok(QueryResult {
                    rows: vec![row],
                    columns: Vec::new(),
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for UnusedTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(
            &mut self,
            _sql: &str,
            _params: &[Value],
        ) -> Result<QueryResult, LixError> {
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn exact_committed_state_reads_authoritative_live_table() {
        let live_table_query_seen = Arc::new(AtomicBool::new(false));
        let backend = ExactCommittedStateBackend {
            live_table_query_seen: Arc::clone(&live_table_query_seen),
        };

        let row = load_exact_committed_state_row_from_live_state(
            &backend,
            &ExactCommittedStateRowRequest {
                entity_id: "file-1".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                version_id: "v1".to_string(),
                exact_filters: BTreeMap::from([
                    ("file_id".to_string(), Value::Text("lix".to_string())),
                    ("plugin_key".to_string(), Value::Text("lix".to_string())),
                    ("schema_version".to_string(), Value::Text("1".to_string())),
                ]),
            },
        )
        .await
        .expect("exact committed live row lookup should succeed")
        .expect("exact committed live row should exist");

        assert!(
            live_table_query_seen.load(Ordering::SeqCst),
            "exact committed state should read directly from the authoritative live table"
        );
        assert_eq!(row.entity_id, "file-1");
        assert_eq!(row.schema_key, "lix_file_descriptor");
        assert_eq!(row.file_id, "lix");
        assert_eq!(row.version_id, "v1");
        assert_eq!(row.source_change_id.as_deref(), Some("change-1"));
        let (_, expected_snapshot) = mock_normalized_live_row("lix_file_descriptor");
        assert_eq!(
            row.values.get("snapshot_content"),
            Some(&Value::Text(expected_snapshot))
        );
    }

    struct CanonicalFallbackBackend {
        canonical_query_seen: Arc<AtomicBool>,
    }

    #[async_trait(?Send)]
    impl LixBackend for CanonicalFallbackBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"")
                || sql.contains("FROM lix_internal_live_untracked_v1_lix_version_ref")
            {
                return Err(LixError::new("LIX_ERROR_UNKNOWN", "no such table"));
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
            {
                self.canonical_query_seen.store(true, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![Value::Text(
                        "{\"id\":\"v1\",\"commit_id\":\"commit-1\"}".to_string(),
                    )]],
                    columns: vec!["snapshot_content".to_string()],
                });
            }
            if sql.contains("WITH RECURSIVE commit_walk")
                && sql.contains("c.schema_key = 'lix_file_descriptor'")
            {
                self.canonical_query_seen.store(true, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("lix_file_descriptor".to_string()),
                        Value::Text("1".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text("v1".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text("{\"id\":\"file-1\",\"name\":\"contract\"}".to_string()),
                        Value::Text("{\"k\":\"v\"}".to_string()),
                        Value::Text("change-fallback".to_string()),
                    ]],
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "version_id".to_string(),
                        "plugin_key".to_string(),
                        "snapshot_content".to_string(),
                        "metadata".to_string(),
                        "change_id".to_string(),
                    ],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }
    }

    #[tokio::test]
    async fn canonical_version_head_reconstruction_reads_journal() {
        let canonical_query_seen = Arc::new(AtomicBool::new(false));
        let backend = CanonicalFallbackBackend {
            canonical_query_seen: Arc::clone(&canonical_query_seen),
        };

        let mut executor = &backend;
        let commit_id =
            reconstruct_committed_version_head_commit_id_from_canonical(&mut executor, "v1")
                .await
                .expect("canonical version head reconstruction should succeed");

        assert!(
            canonical_query_seen.load(Ordering::SeqCst),
            "canonical version head reconstruction should read canonical changes"
        );
        assert_eq!(commit_id.as_deref(), Some("commit-1"));
    }

    #[tokio::test]
    async fn canonical_exact_state_reconstruction_walks_commit_history() {
        let canonical_query_seen = Arc::new(AtomicBool::new(false));
        let backend = CanonicalFallbackBackend {
            canonical_query_seen: Arc::clone(&canonical_query_seen),
        };

        let mut executor = &backend;
        let row = reconstruct_exact_committed_state_row_from_canonical(
            &mut executor,
            &ExactCommittedStateRowRequest {
                entity_id: "file-1".to_string(),
                schema_key: "lix_file_descriptor".to_string(),
                version_id: "v1".to_string(),
                exact_filters: BTreeMap::from([
                    ("file_id".to_string(), Value::Text("lix".to_string())),
                    ("plugin_key".to_string(), Value::Text("lix".to_string())),
                    ("schema_version".to_string(), Value::Text("1".to_string())),
                ]),
            },
        )
        .await
        .expect("canonical exact-state reconstruction should succeed")
        .expect("canonical exact-state reconstruction should return a row");

        assert!(
            canonical_query_seen.load(Ordering::SeqCst),
            "canonical exact-state reconstruction should read canonical history"
        );
        assert_eq!(row.entity_id, "file-1");
        assert_eq!(row.source_change_id.as_deref(), Some("change-fallback"));
    }
}
