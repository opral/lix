use std::collections::{BTreeMap, BTreeSet};

use crate::errors::classification::is_missing_relation_error;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::load_live_row_access_with_executor;
use crate::live_state::raw::{scan_rows_with_executor, snapshot_text, RawStorage};
use crate::live_state::roots::load_version_ref_with_executor;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value, VersionId};

use super::types::{VersionInfo, VersionSnapshot};

const CANONICAL_FALLBACK_MAX_COMMIT_DEPTH: usize = 2048;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactCommittedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) writer_key: Option<String>,
    pub(crate) source_change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitLineageEntry {
    pub(crate) id: String,
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

pub(crate) async fn load_committed_version_head_commit_id_from_live_state(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    if let Some(pointer) = load_version_ref_with_executor(executor, version_id).await? {
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
                    id: VersionId::new(version_id.clone())?,
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
                        id: VersionId::new(version_id.clone())?,
                    },
                },
            );
        }
    }

    Ok(versions)
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
    let access = load_live_row_access_with_executor(executor, &request.schema_key).await?;
    let mut constraints = vec![ScanConstraint {
        field: ScanField::EntityId,
        operator: ScanOperator::Eq(Value::Text(request.entity_id.clone())),
    }];
    if let Some(file_id) = request
        .exact_filters
        .get("file_id")
        .and_then(text_from_value)
    {
        constraints.push(ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text(file_id.to_string())),
        });
    }
    if let Some(plugin_key) = request
        .exact_filters
        .get("plugin_key")
        .and_then(text_from_value)
    {
        constraints.push(ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text(plugin_key.to_string())),
        });
    }
    if let Some(schema_version) = request
        .exact_filters
        .get("schema_version")
        .and_then(text_from_value)
    {
        constraints.push(ScanConstraint {
            field: ScanField::SchemaVersion,
            operator: ScanOperator::Eq(Value::Text(schema_version.to_string())),
        });
    }
    let mut rows = scan_rows_with_executor(
        executor,
        RawStorage::Tracked,
        &request.schema_key,
        &request.version_id,
        &constraints,
        &access
            .columns()
            .iter()
            .map(|column| column.property_name.clone())
            .collect::<Vec<_>>(),
    )
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() > 1 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "expected at most one committed tracked row for schema '{}' entity '{}' version '{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        ));
    }
    let row = rows.remove(0);
    let snapshot_content = snapshot_text(&access, &row)?;
    let Some(row) = row.into_tracked() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked committed-state lookup returned untracked row",
        ));
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
        writer_key: row.writer_key,
        source_change_id: row.change_id,
    }))
}

pub(crate) async fn load_exact_committed_state_row_from_commit_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    head_commit_id: &str,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
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
        head_commit_id = escape_sql_string(head_commit_id),
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
        writer_key: None,
        source_change_id,
    }))
}

pub(crate) async fn load_commit_lineage_entry_by_id(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
) -> Result<Option<CommitLineageEntry>, LixError> {
    let access = load_live_row_access_with_executor(executor, "lix_commit").await?;
    let constraints = vec![
        ScanConstraint {
            field: ScanField::EntityId,
            operator: ScanOperator::Eq(Value::Text(commit_id.to_string())),
        },
        ScanConstraint {
            field: ScanField::FileId,
            operator: ScanOperator::Eq(Value::Text("lix".to_string())),
        },
        ScanConstraint {
            field: ScanField::PluginKey,
            operator: ScanOperator::Eq(Value::Text("lix".to_string())),
        },
    ];
    let required_columns = access
        .columns()
        .iter()
        .map(|column| column.property_name.clone())
        .collect::<Vec<_>>();
    let mut tracked_rows = scan_rows_with_executor(
        executor,
        RawStorage::Tracked,
        "lix_commit",
        GLOBAL_VERSION_ID,
        &constraints,
        &required_columns,
    )
    .await?;
    let snapshot_content = if let Some(row) = tracked_rows.pop() {
        snapshot_text(&access, &row)?
    } else {
        let mut untracked_rows = scan_rows_with_executor(
            executor,
            RawStorage::Untracked,
            "lix_commit",
            GLOBAL_VERSION_ID,
            &constraints,
            &required_columns,
        )
        .await?;
        let Some(row) = untracked_rows.pop() else {
            return Ok(None);
        };
        snapshot_text(&access, &row)?
    };
    if snapshot_content.is_empty() {
        return Ok(None);
    }
    let parsed: crate::schema::builtin::types::LixCommit = serde_json::from_str(&snapshot_content)
        .map_err(|error| {
            LixError::unknown(format!(
                "commit snapshot_content invalid JSON for '{}': {error}",
                commit_id
            ))
        })?;
    Ok(Some(CommitLineageEntry {
        id: parsed.id,
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

    load_exact_committed_state_row_from_commit_with_executor(executor, &head_commit_id, request)
        .await
}

#[cfg(test)]
use crate::schema::builtin::types::LixVersionRef;

#[cfg(test)]
use crate::version::{version_ref_file_id, version_ref_plugin_key, version_ref_schema_key};

#[cfg(test)]
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
    use crate::backend::LixBackendTransaction;
    use crate::live_state::{builtin_live_table_layout, LiveRowAccess};
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
            Value::Boolean(false),
            Value::Text("lix".to_string()),
            Value::Text("{\"k\":\"v\"}".to_string()),
            Value::Text("change-1".to_string()),
            Value::Null,
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
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
            .logical_snapshot_text_from_row(&row, 12)
            .expect("live snapshot reconstruction should succeed")
            .expect("live snapshot should be present");
        (row, snapshot)
    }

    fn fake_version_ref_live_row(version_id: &str, commit_id: &str) -> Vec<Value> {
        let layout = builtin_live_table_layout(crate::version::version_ref_schema_key())
            .expect("builtin layout should load")
            .expect("version ref layout should exist");
        let mut row = vec![
            Value::Text(version_id.to_string()),
            Value::Text(crate::version::version_ref_schema_key().to_string()),
            Value::Text(crate::version::version_ref_schema_version().to_string()),
            Value::Text(crate::version::version_ref_file_id().to_string()),
            Value::Text(crate::version::version_ref_storage_version_id().to_string()),
            Value::Boolean(true),
            Value::Text(crate::version::version_ref_plugin_key().to_string()),
            Value::Null,
            Value::Null,
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
            Value::Text("2026-03-06T14:22:00.000Z".to_string()),
        ];
        for column in &layout.columns {
            row.push(match column.property_name.as_str() {
                "id" => Value::Text(version_id.to_string()),
                "commit_id" => Value::Text(commit_id.to_string()),
                _ => Value::Null,
            });
        }
        row
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

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for UnusedTransaction {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
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
                || sql.contains("FROM lix_internal_live_v1_lix_version_ref")
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

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
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

    struct LiveVersionHeadBackend {
        live_query_seen: Arc<AtomicBool>,
        canonical_query_seen: Arc<AtomicBool>,
    }

    #[async_trait(?Send)]
    impl LixBackend for LiveVersionHeadBackend {
        fn dialect(&self) -> crate::SqlDialect {
            crate::SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("lix_internal_live_v1_lix_version_ref") {
                self.live_query_seen.store(true, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![fake_version_ref_live_row("v1", "commit-live")],
                    columns: Vec::new(),
                });
            }
            if sql.contains("FROM lix_internal_change c")
                && sql.contains("c.schema_key = 'lix_version_ref'")
            {
                self.canonical_query_seen.store(true, Ordering::SeqCst);
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            Ok(Box::new(UnusedTransaction))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[tokio::test]
    async fn committed_version_head_reads_live_version_ref_when_present() {
        let live_query_seen = Arc::new(AtomicBool::new(false));
        let canonical_query_seen = Arc::new(AtomicBool::new(false));
        let backend = LiveVersionHeadBackend {
            live_query_seen: Arc::clone(&live_query_seen),
            canonical_query_seen: Arc::clone(&canonical_query_seen),
        };

        let mut executor = &backend;
        let commit_id = load_committed_version_head_commit_id_from_live_state(&mut executor, "v1")
            .await
            .expect("live version head lookup should succeed");

        assert!(live_query_seen.load(Ordering::SeqCst));
        assert!(!canonical_query_seen.load(Ordering::SeqCst));
        assert_eq!(commit_id.as_deref(), Some("commit-live"));
    }
}
