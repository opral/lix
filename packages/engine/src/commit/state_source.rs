use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;

use crate::builtin_schema::types::LixVersionPointer;
use crate::error_classification::is_missing_relation_error;
use crate::materialization::{
    materialization_plan, MaterializationDebugMode, MaterializationRequest, MaterializationScope,
    MaterializationWrite, MaterializationWriteOp,
};
use crate::version::{
    global_pointer_file_id, global_pointer_plugin_key, global_pointer_schema_key,
    global_pointer_storage_version_id, version_pointer_storage_version_id,
    version_pointer_file_id, version_pointer_plugin_key, version_pointer_schema_key,
};
use crate::{LixBackend, LixError, QueryResult, Value};

use super::types::{VersionInfo, VersionSnapshot};

const VERSION_POINTER_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_pointer";
const GLOBAL_POINTER_TABLE: &str = "lix_internal_state_materialized_v1_lix_global_pointer";

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
    let snapshot_content = match load_current_pointer_snapshot_content(
        executor,
        VERSION_POINTER_TABLE,
        version_pointer_schema_key(),
        version_id,
        version_pointer_file_id(),
        version_pointer_plugin_key(),
        version_pointer_storage_version_id(),
    )
    .await?
    {
        Some(snapshot_content) => Some(snapshot_content),
        None => None,
    };
    if let Some(snapshot_content) = snapshot_content {
        let Some(pointer) = parse_version_pointer_snapshot(&snapshot_content)? else {
            return Ok(None);
        };
        if pointer.commit_id.is_empty() {
            return Ok(None);
        }
        return Ok(Some(pointer.commit_id));
    }

    load_pointer_tip_commit_id_from_change_log(
        executor,
        version_pointer_schema_key(),
        version_id,
        version_pointer_file_id(),
        version_pointer_plugin_key(),
    )
    .await
}

pub(crate) async fn load_committed_global_tip_commit_id(
    executor: &mut dyn CommitQueryExecutor,
) -> Result<Option<String>, LixError> {
    let snapshot_content = match load_current_pointer_snapshot_content(
        executor,
        GLOBAL_POINTER_TABLE,
        global_pointer_schema_key(),
        "global",
        global_pointer_file_id(),
        global_pointer_plugin_key(),
        global_pointer_storage_version_id(),
    )
    .await?
    {
        Some(snapshot_content) => Some(snapshot_content),
        None => None,
    };
    if let Some(snapshot_content) = snapshot_content {
        let Some(pointer) = parse_version_pointer_snapshot(&snapshot_content)? else {
            return Ok(None);
        };
        if pointer.commit_id.is_empty() {
            return Ok(None);
        }
        return Ok(Some(pointer.commit_id));
    }

    load_pointer_tip_commit_id_from_change_log(
        executor,
        global_pointer_schema_key(),
        "global",
        global_pointer_file_id(),
        global_pointer_plugin_key(),
    )
    .await
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
        if let Some(commit_id) = load_committed_version_tip_commit_id(executor, version_id).await? {
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
           AND global = true \
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

async fn load_pointer_tip_commit_id_from_change_log(
    executor: &mut dyn CommitQueryExecutor,
    schema_key: &str,
    entity_id: &str,
    file_id: &str,
    plugin_key: &str,
) -> Result<Option<String>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
           AND s.content IS NOT NULL",
        schema_key = escape_sql_string(schema_key),
        entity_id = escape_sql_string(entity_id),
        file_id = escape_sql_string(file_id),
        plugin_key = escape_sql_string(plugin_key),
    );
    let result = executor.execute(&sql, &[]).await?;
    let mut candidate_commit_ids = BTreeSet::new();
    for row in &result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        let Some(pointer) = parse_version_pointer_snapshot(value)? else {
            continue;
        };
        if !pointer.commit_id.is_empty() {
            candidate_commit_ids.insert(pointer.commit_id);
        }
    }

    if candidate_commit_ids.is_empty() {
        return Ok(None);
    }
    if candidate_commit_ids.len() == 1 {
        return Ok(candidate_commit_ids.pop_first());
    }

    select_tip_commit_from_ancestry(executor, &candidate_commit_ids).await
}

async fn select_tip_commit_from_ancestry(
    executor: &mut dyn CommitQueryExecutor,
    candidate_commit_ids: &BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    let in_list = candidate_commit_ids
        .iter()
        .map(|commit_id| format!("'{}'", escape_sql_string(commit_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT commit_id, ancestor_id \
         FROM lix_internal_commit_ancestry \
         WHERE commit_id IN ({in_list}) \
           AND ancestor_id IN ({in_list})",
    );
    let result = executor.execute(&sql, &[]).await?;
    let ancestry_pairs = result
        .rows
        .iter()
        .filter_map(|row| match (row.first(), row.get(1)) {
            (Some(commit_id), Some(ancestor_id)) => {
                Some((text_from_value(commit_id)?, text_from_value(ancestor_id)?))
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();

    let mut resolved_tip = None;
    for candidate in candidate_commit_ids {
        let dominates_all = candidate_commit_ids.iter().all(|other| {
            candidate == other
                || ancestry_pairs.contains(&(candidate.clone(), other.clone()))
        });
        if dominates_all {
            if resolved_tip.is_some() {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "pointer tip fallback found multiple candidate tips in commit ancestry"
                        .to_string(),
                });
            }
            resolved_tip = Some(candidate.clone());
        }
    }

    if resolved_tip.is_none() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description:
                "pointer tip fallback could not resolve a current tip from commit ancestry"
                    .to_string(),
        });
    }

    Ok(resolved_tip)
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
