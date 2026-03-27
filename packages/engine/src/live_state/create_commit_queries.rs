use crate::filesystem::runtime::{
    ExactFilesystemDescriptorState, FilesystemDescriptorState, FILESYSTEM_DESCRIPTOR_FILE_ID,
    FILESYSTEM_DESCRIPTOR_PLUGIN_KEY, FILESYSTEM_FILE_SCHEMA_KEY,
};
use crate::key_value::key_value_schema_key;
use crate::live_state::schema_access::{
    payload_column_name_for_schema, snapshot_select_expr_for_schema, tracked_relation_name,
};
use crate::sql_support::text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, Value};
use std::collections::BTreeMap;

pub(crate) async fn load_create_commit_deterministic_sequence_start(
    executor: &mut dyn crate::canonical::state_source::CommitQueryExecutor,
) -> Result<Option<i64>, LixError> {
    let value_column = payload_column_name_for_schema(key_value_schema_key(), None, "value")?;
    let sql = format!(
        "SELECT {value_column} \
         FROM {table_name} \
         WHERE file_id = '{file_id}' \
           AND entity_id = 'lix_deterministic_sequence_number' \
           AND version_id = '{version_id}' \
           AND untracked = true \
           AND {value_column} IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        value_column = quote_ident(&value_column),
        table_name = quote_ident(&tracked_relation_name(key_value_schema_key())),
        file_id = FILESYSTEM_DESCRIPTOR_FILE_ID,
        version_id = GLOBAL_VERSION_ID,
    );
    let result = executor.execute(&sql, &[]).await?;
    if let Some(value_json) = result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(value_as_text)
    {
        let snapshot_content =
            format!("{{\"key\":\"lix_deterministic_sequence_number\",\"value\":{value_json}}}");
        return parse_deterministic_sequence_snapshot(&snapshot_content).map(Some);
    }

    let tracked =
        crate::canonical::state_source::load_exact_committed_state_row_from_live_state_with_executor(
            executor,
            &crate::canonical::state_source::ExactCommittedStateRowRequest {
                entity_id: "lix_deterministic_sequence_number".to_string(),
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                exact_filters: BTreeMap::from([
                    (
                        "file_id".to_string(),
                        Value::Text(FILESYSTEM_DESCRIPTOR_FILE_ID.to_string()),
                    ),
                    (
                        "plugin_key".to_string(),
                        Value::Text(FILESYSTEM_DESCRIPTOR_PLUGIN_KEY.to_string()),
                    ),
                ]),
            },
        )
        .await?;
    let Some(snapshot_content) = tracked
        .as_ref()
        .and_then(|row| row.values.get("snapshot_content"))
        .and_then(value_as_text)
    else {
        return Ok(Some(0));
    };
    parse_deterministic_sequence_snapshot(&snapshot_content).map(Some)
}

pub(crate) async fn load_untracked_file_descriptor(
    executor: &mut dyn crate::canonical::state_source::CommitQueryExecutor,
    file_id: &str,
    version_id: &str,
) -> Result<Option<ExactFilesystemDescriptorState>, LixError> {
    let snapshot_expr =
        snapshot_select_expr_for_schema(FILESYSTEM_FILE_SCHEMA_KEY, None, executor.dialect(), None)?;
    let sql = format!(
        "SELECT {snapshot_expr} AS snapshot_content, metadata \
         FROM {table_name} \
         WHERE entity_id = '{entity_id}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true \
           AND {snapshot_expr} IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        snapshot_expr = snapshot_expr,
        table_name = quote_ident(&tracked_relation_name(FILESYSTEM_FILE_SCHEMA_KEY)),
        entity_id = escape_sql_string(file_id),
        file_id = escape_sql_string(FILESYSTEM_DESCRIPTOR_FILE_ID),
        version_id = escape_sql_string(version_id),
    );
    let result = executor.execute(&sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(snapshot_content) = row.first().and_then(value_as_text) else {
        return Ok(None);
    };
    let metadata = row.get(1).and_then(value_as_text);
    parse_file_descriptor_preflight_row(&snapshot_content, metadata, true).map(Some)
}

fn quote_ident(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) if !text.is_empty() => Some(text.clone()),
        _ => None,
    }
}

fn parse_deterministic_sequence_snapshot(snapshot_content: &str) -> Result<i64, LixError> {
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic sequence snapshot invalid JSON: {error}"),
        )
    })?;
    Ok(parsed
        .get("value")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0))
}

fn parse_file_descriptor_preflight_row(
    snapshot_content: &str,
    metadata: Option<String>,
    untracked: bool,
) -> Result<ExactFilesystemDescriptorState, LixError> {
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "create commit preflight file descriptor snapshot could not be parsed: {error}"
            ),
        )
    })?;
    Ok(ExactFilesystemDescriptorState {
        descriptor: FilesystemDescriptorState {
            directory_id: parsed
                .get("directory_id")
                .and_then(|value| match value {
                    serde_json::Value::Null => None,
                    serde_json::Value::String(text) => Some(text.clone()),
                    _ => None,
                })
                .unwrap_or_default(),
            name: parsed
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
            extension: parsed.get("extension").and_then(|value| match value {
                serde_json::Value::Null => None,
                serde_json::Value::String(text) if text.is_empty() => None,
                serde_json::Value::String(text) => Some(text.clone()),
                _ => None,
            }),
            hidden: parsed
                .get("hidden")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            metadata,
        },
        untracked,
    })
}
