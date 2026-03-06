use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::builtin_schema::types::LixVersionPointer;
use crate::error_classification::is_missing_relation_error;
use crate::version::{
    version_pointer_file_id, version_pointer_plugin_key, version_pointer_schema_key,
};
use crate::{LixBackend, LixError, QueryResult, Value};

use super::types::{VersionInfo, VersionSnapshot};

const CHANGE_TABLE: &str = "lix_internal_change";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

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

pub(crate) async fn load_committed_version_tip_commit_id(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM {change_table} c \
         LEFT JOIN {snapshot_table} s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND s.content IS NOT NULL \
         ORDER BY c.created_at DESC, c.id DESC \
         LIMIT 1",
        change_table = CHANGE_TABLE,
        snapshot_table = SNAPSHOT_TABLE,
        schema_key = escape_sql_string(version_pointer_schema_key()),
        entity_id = escape_sql_string(version_id),
        file_id = escape_sql_string(version_pointer_file_id()),
        plugin_key = escape_sql_string(version_pointer_plugin_key()),
    );
    let result = executor.execute(&sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.first() else {
        return Ok(None);
    };
    let Some(pointer) = parse_version_pointer_snapshot(snapshot_content)? else {
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

    let in_list = version_ids
        .iter()
        .map(|version_id| format!("'{}'", escape_sql_string(version_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT c.entity_id, s.content AS snapshot_content \
         FROM {change_table} c \
         LEFT JOIN {snapshot_table} s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND c.entity_id IN ({in_list}) \
           AND s.content IS NOT NULL \
         ORDER BY c.entity_id ASC, c.created_at DESC, c.id DESC",
        change_table = CHANGE_TABLE,
        snapshot_table = SNAPSHOT_TABLE,
        schema_key = escape_sql_string(version_pointer_schema_key()),
        file_id = escape_sql_string(version_pointer_file_id()),
        plugin_key = escape_sql_string(version_pointer_plugin_key()),
        in_list = in_list,
    );

    match executor.execute(&sql, &[]).await {
        Ok(result) => {
            let mut seen = BTreeSet::new();
            for row in result.rows {
                if row.len() < 2 {
                    continue;
                }
                let entity_id = match &row[0] {
                    Value::Text(value) => value.clone(),
                    Value::Null => continue,
                    _ => {
                        return Err(LixError {
                            code: "LIX_ERROR_UNKNOWN".to_string(),
                            description: "version tip entity_id must be text".to_string(),
                        });
                    }
                };
                if !version_ids.contains(&entity_id) || !seen.insert(entity_id.clone()) {
                    continue;
                }
                let Some(parsed) = parse_version_info_from_tip_snapshot(&row[1], &entity_id)?
                else {
                    continue;
                };
                versions.insert(entity_id, parsed);
            }
        }
        Err(err) if is_missing_relation_error(&err) => {}
        Err(err) => return Err(err),
    }

    Ok(versions)
}

pub(crate) async fn load_exact_committed_state_row(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let mut predicates = vec![
        format!("entity_id = '{}'", escape_sql_string(&request.entity_id)),
        format!("version_id = '{}'", escape_sql_string(&request.version_id)),
        "is_tombstone = 0".to_string(),
        "snapshot_content IS NOT NULL".to_string(),
    ];
    for column in ["schema_key", "file_id", "plugin_key", "schema_version"] {
        if let Some(value) = request.exact_filters.get(column) {
            let Some(value) = text_from_value(value) else {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "tracked write state source requires text-compatible exact filter values for '{column}'"
                    ),
                });
            };
            predicates.push(format!("{column} = '{}'", escape_sql_string(&value)));
        }
    }

    let sql = format!(
        "SELECT \
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, \
             snapshot_content, metadata, change_id \
         FROM {table_name} \
         WHERE {predicates} \
         LIMIT 2",
        table_name = quote_ident(&format!("{MATERIALIZED_PREFIX}{}", request.schema_key)),
        predicates = predicates.join(" AND "),
    );
    let mut result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(None),
        Err(error) => return Err(error),
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
    let Some(row) = result.rows.pop() else {
        return Ok(None);
    };
    if row.len() < 9 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "tracked write state source query returned too few columns".to_string(),
        });
    }

    let entity_id = required_text_value(&row[0], "entity_id")?;
    let schema_key = required_text_value(&row[1], "schema_key")?;
    let schema_version = required_text_value(&row[2], "schema_version")?;
    let file_id = required_text_value(&row[3], "file_id")?;
    let version_id = required_text_value(&row[4], "version_id")?;
    let plugin_key = required_text_value(&row[5], "plugin_key")?;
    let snapshot_content = required_text_value(&row[6], "snapshot_content")?;
    let metadata = optional_value(&row[7]);
    let change_id = optional_value(&row[8]);

    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
    values.insert("schema_key".to_string(), Value::Text(schema_key.clone()));
    values.insert(
        "schema_version".to_string(),
        Value::Text(schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id.clone()));
    values.insert("version_id".to_string(), Value::Text(version_id.clone()));
    values.insert("plugin_key".to_string(), Value::Text(plugin_key));
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    if let Some(metadata) = metadata {
        values.insert("metadata".to_string(), metadata);
    }

    Ok(Some(ExactCommittedStateRow {
        entity_id,
        schema_key,
        file_id,
        version_id,
        values,
        source_change_id: change_id.and_then(|value| text_from_value(&value)),
    }))
}

fn parse_version_info_from_tip_snapshot(
    value: &Value,
    fallback_version_id: &str,
) -> Result<Option<VersionInfo>, LixError> {
    let Some(snapshot) = parse_version_pointer_snapshot(value)? else {
        return Ok(None);
    };
    let version_id = if snapshot.id.is_empty() {
        fallback_version_id.to_string()
    } else {
        snapshot.id
    };
    let parent_commit_ids = if snapshot.commit_id.is_empty() {
        Vec::new()
    } else {
        vec![snapshot.commit_id]
    };

    Ok(Some(VersionInfo {
        parent_commit_ids,
        snapshot: VersionSnapshot { id: version_id },
    }))
}

fn parse_version_pointer_snapshot(value: &Value) -> Result<Option<LixVersionPointer>, LixError> {
    let raw_snapshot = match value {
        Value::Text(value) => value,
        Value::Null => return Ok(None),
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "version tip snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionPointer =
        serde_json::from_str(raw_snapshot).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("version tip snapshot_content invalid JSON: {error}"),
        })?;
    Ok(Some(snapshot))
}

fn required_text_value(value: &Value, label: &str) -> Result<String, LixError> {
    match value {
        Value::Text(value) => Ok(value.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("tracked write state source expected text for '{label}'"),
        }),
    }
}

fn optional_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        other => Some(other.clone()),
    }
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
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}
