use crate::contracts::artifacts::PreparedBatch;
use crate::contracts::functions::LixFunctionProvider;
use crate::common::errors::classification::is_missing_relation_error;
use crate::live_state::storage::{payload_column_name_for_schema, tracked_relation_name};
use crate::schema::builtin::storage::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
};
use crate::common::text::escape_sql_string;
use crate::version_state::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, SqlDialect, Value};

const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

pub(crate) fn deterministic_sequence_key() -> &'static str {
    DETERMINISTIC_SEQUENCE_KEY
}

pub(crate) async fn load_runtime_sequence_start_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let visible_sequence_start =
        load_visible_runtime_sequence_start_in_transaction(transaction).await?;
    let ensure_sql =
        build_ensure_runtime_sequence_row_sql(visible_sequence_start - 1, transaction.dialect());
    transaction.execute(&ensure_sql, &[]).await?;

    let load_sql = build_lock_runtime_sequence_row_sql(transaction.dialect());
    let result = transaction.execute(&load_sql, &[]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(0);
    };
    let Some(value_json) = row.first() else {
        return Ok(0);
    };
    let raw = value_to_string(value_json, "value_json")?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("deterministic sequence value_json invalid JSON: {err}"),
    })?;
    Ok(parsed.as_i64().unwrap_or(-1) + 1)
}

pub(crate) async fn ensure_runtime_sequence_initialized_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(), LixError> {
    if !functions.deterministic_sequence_enabled() || functions.deterministic_sequence_initialized()
    {
        return Ok(());
    }
    let sequence_start = load_runtime_sequence_start_in_transaction(transaction).await?;
    functions.initialize_deterministic_sequence(sequence_start);
    Ok(())
}

pub(crate) fn build_ensure_runtime_sequence_row_sql(
    highest_seen: i64,
    _dialect: SqlDialect,
) -> String {
    let sequence_key = deterministic_sequence_key();
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
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
        plugin_key = escape_sql_string(key_value_plugin_key()),
        schema_version = escape_sql_string(key_value_schema_version()),
    )
}

pub(crate) async fn persist_runtime_sequence_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    functions: &dyn LixFunctionProvider,
) -> Result<(), LixError> {
    let Some(highest_seen) = functions.deterministic_sequence_persist_highest_seen() else {
        return Ok(());
    };
    let batch = build_persist_sequence_highest_batch(highest_seen, transaction.dialect())?;
    transaction.execute_batch(&batch).await?;
    Ok(())
}

pub(crate) fn build_persist_sequence_highest_batch(
    highest_seen: i64,
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
    let mut batch = PreparedBatch { steps: Vec::new() };
    batch.append_sql(build_update_runtime_sequence_highest_sql(
        highest_seen,
        dialect,
    ));
    Ok(batch)
}

pub(crate) fn build_update_runtime_sequence_highest_sql(
    highest_seen: i64,
    _dialect: SqlDialect,
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
        entity_id = escape_sql_string(deterministic_sequence_key()),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
    )
}

async fn load_visible_runtime_sequence_start_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let sql = format!(
        "SELECT value_json \
         FROM (\
           SELECT u.value_json AS value_json, 0 AS precedence \
           FROM {table_name} u \
           WHERE u.entity_id = '{entity_id}' \
             AND u.version_id = '{version_id}' \
             AND u.untracked = true \
             AND u.value_json IS NOT NULL \
           UNION ALL \
           SELECT t.value_json AS value_json, 1 AS precedence \
           FROM {table_name} t \
           WHERE t.entity_id = '{entity_id}' \
             AND t.version_id = '{version_id}' \
             AND t.untracked = false \
             AND t.value_json IS NOT NULL \
             AND t.is_tombstone = 0\
         ) visible_key_values \
         ORDER BY precedence ASC \
         LIMIT 1",
        table_name = tracked_relation_name(key_value_schema_key()),
        entity_id = escape_sql_string(deterministic_sequence_key()),
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
    );
    let result = match transaction.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(error) if is_missing_relation_error(&error) => return Ok(0),
        Err(error) => return Err(error),
    };
    let Some(row) = result.rows.first() else {
        return Ok(0);
    };
    let Some(value_json) = row.first() else {
        return Ok(0);
    };
    let raw = value_to_string(value_json, "value_json")?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("deterministic sequence value_json invalid JSON: {err}"),
    })?;
    Ok(parsed.as_i64().unwrap_or(-1) + 1)
}

fn build_lock_runtime_sequence_row_sql(dialect: SqlDialect) -> String {
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
        entity_id = escape_sql_string(deterministic_sequence_key()),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
        for_update = for_update,
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
