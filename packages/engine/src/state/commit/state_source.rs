use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::errors::classification::is_missing_relation_error;
use crate::schema::builtin::types::LixVersionRef;
use crate::version::{
    version_ref_file_id, version_ref_plugin_key, version_ref_schema_key,
    version_ref_storage_version_id,
};
use crate::{LixBackend, LixError, QueryResult, Value};

use super::types::{VersionInfo, VersionSnapshot};

const VERSION_REF_TABLE: &str = "lix_internal_live_v1_lix_version_ref";

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

#[async_trait(?Send)]
pub(crate) trait CommitQueryExecutor {
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;
}

#[async_trait(?Send)]
impl<T> CommitQueryExecutor for &T
where
    T: LixBackend + ?Sized,
{
    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        (*self).execute(sql, params).await
    }
}

pub(crate) async fn load_committed_version_head_commit_id(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let snapshot_content = match load_current_pointer_snapshot_content(
        executor,
        VERSION_REF_TABLE,
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
        if let Some(commit_id) = load_committed_version_head_commit_id(executor, version_id).await?
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
    table: &str,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
    storage_version_id: &str,
) -> Result<Option<Value>, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table} \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND plugin_key = '{plugin_key}' \
           AND version_id = '{storage_version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        table = table,
        schema_key = escape_sql_string(schema_key),
        entity_id = escape_sql_string(entity_id),
        file_id = escape_sql_string(file_id),
        plugin_key = escape_sql_string(plugin_key),
        storage_version_id = escape_sql_string(storage_version_id),
    );

    match executor.execute(&sql, &[]).await {
        Ok(result) => Ok(result.rows.first().and_then(|row| row.first()).cloned()),
        Err(err) if is_missing_relation_error(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

pub(crate) async fn load_exact_committed_state_row(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let table_name = quote_ident(&format!("lix_internal_live_v1_{}", request.schema_key));
    let mut sql = format!(
        "SELECT entity_id, schema_key, schema_version, file_id, version_id, plugin_key, snapshot_content, metadata, change_id \
         FROM {table_name} \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND version_id = '{version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL",
        table_name = table_name,
        entity_id = escape_sql_string(&request.entity_id),
        schema_key = escape_sql_string(&request.schema_key),
        version_id = escape_sql_string(&request.version_id),
    );
    for column in ["file_id", "plugin_key", "schema_version"] {
        let Some(expected) = request.exact_filters.get(column).and_then(text_from_value) else {
            continue;
        };
        sql.push_str(&format!(
            " AND {column} = '{expected}'",
            column = column,
            expected = escape_sql_string(&expected),
        ));
    }
    sql.push_str(" LIMIT 2");

    let result = match backend.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };

    if result.rows.len() > 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "tracked write state source requires exactly one committed target row for '{}:{}@{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        });
    }
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    exact_committed_state_row_from_live_table_row(row)
}

fn exact_committed_state_row_from_live_table_row(
    row: &[Value],
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

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{LixTransaction, SqlDialect};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct ExactCommittedStateBackend {
        live_table_query_seen: Arc<AtomicBool>,
    }

    struct UnusedTransaction;

    #[async_trait(?Send)]
    impl LixBackend for ExactCommittedStateBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM \"lix_internal_live_v1_lix_file_descriptor\"") {
                self.live_table_query_seen.store(true, Ordering::SeqCst);
                return Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text("file-1".to_string()),
                        Value::Text("lix_file_descriptor".to_string()),
                        Value::Text("1".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text("v1".to_string()),
                        Value::Text("lix".to_string()),
                        Value::Text("{\"id\":\"file-1\"}".to_string()),
                        Value::Text("{\"k\":\"v\"}".to_string()),
                        Value::Text("change-1".to_string()),
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

    #[async_trait(?Send)]
    impl LixTransaction for UnusedTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
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

        let row = load_exact_committed_state_row(
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
        assert_eq!(
            row.values.get("snapshot_content"),
            Some(&Value::Text("{\"id\":\"file-1\"}".to_string()))
        );
    }
}
