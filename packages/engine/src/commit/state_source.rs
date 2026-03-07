use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::builtin_schema::types::LixVersionPointer;
use crate::error_classification::is_missing_relation_error;
use crate::materialization::{
    materialization_plan, MaterializationDebugMode, MaterializationRequest, MaterializationScope,
    MaterializationWrite, MaterializationWriteOp,
};
use crate::version::{
    version_pointer_file_id, version_pointer_plugin_key, version_pointer_schema_key,
};
use crate::{LixBackend, LixError, QueryResult, Value};

use super::types::{VersionInfo, VersionSnapshot};

const CHANGE_TABLE: &str = "lix_internal_change";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";

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
    pub(crate) global_filter: Option<bool>,
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
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let mut target_versions = BTreeSet::new();
    target_versions.insert(request.version_id.clone());
    let plan = materialization_plan(
        backend,
        &MaterializationRequest {
            scope: MaterializationScope::Versions(target_versions),
            debug: MaterializationDebugMode::Off,
            debug_row_limit: 0,
        },
    )
    .await?;

    let matching_rows = plan
        .writes
        .iter()
        .filter(|row| materialized_write_matches_request(row, request))
        .collect::<Vec<_>>();

    if matching_rows.len() > 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "tracked write state source requires exactly one committed target row for '{}:{}@{}'",
                request.schema_key, request.entity_id, request.version_id
            ),
        });
    }
    let Some(row) = matching_rows.into_iter().next() else {
        return Ok(None);
    };
    exact_committed_state_row_from_materialized_write(row)
}

fn materialized_write_matches_request(
    row: &MaterializationWrite,
    request: &ExactCommittedStateRowRequest,
) -> bool {
    if row.entity_id != request.entity_id
        || row.schema_key != request.schema_key
        || row.version_id != request.version_id
    {
        return false;
    }

    if let Some(global) = request.global_filter {
        if row.global != global {
            return false;
        }
    }

    for column in ["file_id", "plugin_key", "schema_version"] {
        if let Some(expected) = request.exact_filters.get(column) {
            let Some(expected) = text_from_value(expected) else {
                return false;
            };
            let actual = match column {
                "file_id" => row.file_id.as_str(),
                "plugin_key" => row.plugin_key.as_str(),
                "schema_version" => row.schema_version.as_str(),
                _ => unreachable!(),
            };
            if actual != expected {
                return false;
            }
        }
    }

    match row.op {
        MaterializationWriteOp::Upsert => row.snapshot_content.is_some(),
        MaterializationWriteOp::Tombstone => false,
    }
}

fn exact_committed_state_row_from_materialized_write(
    row: &MaterializationWrite,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.clone() else {
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
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        values,
        source_change_id: Some(row.change_id.clone()),
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
