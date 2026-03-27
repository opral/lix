use std::collections::BTreeMap;

use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::live_state::schema_access::{payload_column_name_for_schema, tracked_relation_name};
use crate::sql_support::text::escape_sql_string;
use crate::{LixBackend, LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;

pub(crate) async fn load_key_value_payloads(
    backend: &dyn LixBackend,
    entity_ids: &[&str],
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    if entity_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let table_name = tracked_relation_name(key_value_schema_key());
    let untracked_value_expr = "\"u\".\"value_json\"";
    let tracked_value_expr = "\"t\".\"value_json\"";
    let in_list = entity_ids
        .iter()
        .map(|entity_id| format!("'{}'", escape_sql_string(entity_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT entity_id, value_json, precedence \
         FROM (\
           SELECT u.entity_id, {untracked_value_expr} AS value_json, 0 AS precedence \
           FROM {untracked_table} u \
           WHERE entity_id IN ({in_list}) \
             AND version_id = '{version_id}' \
             AND u.untracked = true \
             AND {untracked_value_expr} IS NOT NULL \
           UNION ALL \
           SELECT t.entity_id, {tracked_value_expr} AS value_json, 1 AS precedence \
           FROM {table_name} t \
           WHERE entity_id IN ({in_list}) \
             AND version_id = '{version_id}' \
             AND t.untracked = false \
             AND {tracked_value_expr} IS NOT NULL \
             AND is_tombstone = 0\
         ) visible_key_values \
         ORDER BY entity_id ASC, precedence ASC",
        untracked_table = tracked_relation_name(key_value_schema_key()),
        untracked_value_expr = untracked_value_expr,
        in_list = in_list,
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        table_name = table_name,
        tracked_value_expr = tracked_value_expr,
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut values = BTreeMap::new();
    for row in result.rows {
        let Some(entity_id_value) = row.first() else {
            continue;
        };
        let entity_id = value_to_string(entity_id_value, "entity_id")?;
        if values.contains_key(&entity_id) {
            continue;
        }
        let Some(value_json) = row.get(1) else {
            continue;
        };
        let raw = value_to_string(value_json, "value_json")?;
        let parsed: JsonValue = serde_json::from_str(&raw).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("deterministic mode value_json invalid JSON: {err}"),
        })?;
        values.insert(entity_id, parsed);
    }

    Ok(values)
}

pub(crate) fn build_ensure_runtime_sequence_row_sql(
    highest_seen: i64,
    _dialect: SqlDialect,
    sequence_key: &str,
) -> String {
    let key_column = payload_column_name_for_schema(key_value_schema_key(), None, "key")
        .expect("key-value live schema should include key");
    let value_column = payload_column_name_for_schema(key_value_schema_key(), None, "value")
        .expect("key-value live schema should include value");
    let value_json = serde_json::to_string(&serde_json::Value::from(highest_seen))
        .expect("deterministic highest-seen JSON serialization should succeed");

    format!(
        "INSERT INTO {table_name} \
         (entity_id, schema_key, file_id, version_id, global, plugin_key, metadata, writer_key, schema_version, untracked, created_at, updated_at, {key_column}, {value_column}) \
         VALUES ('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', FALSE, '{plugin_key}', NULL, NULL, '{schema_version}', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '{key_value}', '{value_json}') \
         ON CONFLICT (entity_id, file_id, version_id, untracked) DO NOTHING",
        table_name = tracked_relation_name(key_value_schema_key()),
        key_column = key_column,
        value_column = value_column,
        entity_id = escape_sql_string(sequence_key),
        key_value = escape_sql_string(sequence_key),
        value_json = escape_sql_string(&value_json),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        plugin_key = escape_sql_string(key_value_plugin_key()),
        schema_version = escape_sql_string(key_value_schema_version()),
    )
}

pub(crate) fn build_lock_runtime_sequence_row_sql(
    dialect: SqlDialect,
    sequence_key: &str,
) -> String {
    let value_column = payload_column_name_for_schema(key_value_schema_key(), None, "value")
        .expect("key-value live schema should include value");
    let for_update = match dialect {
        SqlDialect::Postgres => " FOR UPDATE",
        SqlDialect::Sqlite => "",
    };

    format!(
        "SELECT {value_column} AS value_json \
         FROM {table_name} \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true \
         LIMIT 1{for_update}",
        value_column = value_column,
        table_name = tracked_relation_name(key_value_schema_key()),
        entity_id = escape_sql_string(sequence_key),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        for_update = for_update,
    )
}

pub(crate) fn build_update_runtime_sequence_highest_sql(
    highest_seen: i64,
    _dialect: SqlDialect,
    sequence_key: &str,
) -> String {
    let value_column = payload_column_name_for_schema(key_value_schema_key(), None, "value")
        .expect("key-value live schema should include value");
    let value_json = serde_json::to_string(&serde_json::Value::from(highest_seen))
        .expect("deterministic highest-seen JSON serialization should succeed");

    format!(
        "UPDATE {table_name} \
         SET {value_column} = '{value_json}', updated_at = CURRENT_TIMESTAMP \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND untracked = true",
        table_name = tracked_relation_name(key_value_schema_key()),
        value_column = value_column,
        value_json = escape_sql_string(&value_json),
        entity_id = escape_sql_string(sequence_key),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
    )
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}
