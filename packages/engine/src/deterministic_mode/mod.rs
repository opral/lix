use serde_json::Value as JsonValue;

use crate::errors::classification::is_missing_relation_error;
use crate::functions::SystemFunctionProvider;
use crate::functions::{timestamp::timestamp, uuid_v7::uuid_v7, LixFunctionProvider};
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::sql::ast::utils::parse_sql_statements;
use crate::sql::execution::preprocess::preprocess_statements_with_provider_to_plan as preprocess_statements_with_provider;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::LixBackend;
use crate::{LixError, Value};

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
const SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
const EPOCH_TIMESTAMP: &str = "1970-01-01T00:00:00Z";
const DETERMINISTIC_UUID_COUNTER_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;

#[derive(Debug, Clone, Copy)]
pub struct DeterministicSettings {
    pub enabled: bool,
    pub uuid_v7_enabled: bool,
    pub timestamp_enabled: bool,
    pub timestamp_shuffle_enabled: bool,
}

impl DeterministicSettings {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            uuid_v7_enabled: true,
            timestamp_enabled: true,
            timestamp_shuffle_enabled: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeFunctionProvider {
    settings: DeterministicSettings,
    next_sequence: i64,
}

impl RuntimeFunctionProvider {
    pub fn new(settings: DeterministicSettings, next_sequence: i64) -> Self {
        Self {
            settings,
            next_sequence,
        }
    }

    pub fn next_sequence(&self) -> i64 {
        self.next_sequence
    }

    fn take_sequence(&mut self) -> i64 {
        let current = self.next_sequence;
        self.next_sequence += 1;
        current
    }
}

impl LixFunctionProvider for RuntimeFunctionProvider {
    fn uuid_v7(&mut self) -> String {
        if self.settings.enabled && self.settings.uuid_v7_enabled {
            let counter = self.take_sequence();
            let counter_bits = (counter as u64) & DETERMINISTIC_UUID_COUNTER_MASK;
            return format!("01920000-0000-7000-8000-{counter_bits:012x}");
        }
        uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        if self.settings.enabled && self.settings.timestamp_enabled {
            let counter = self.take_sequence();
            let millis = if self.settings.timestamp_shuffle_enabled {
                shuffled_timestamp_millis(counter)
            } else {
                counter
            };
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis)
                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
            return dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        }
        timestamp()
    }
}

fn shuffled_timestamp_millis(counter: i64) -> i64 {
    const WINDOW: i64 = 1000;
    const MULTIPLIER: i64 = 733;
    const OFFSET: i64 = 271;

    let cycle = counter.div_euclid(WINDOW);
    let within = counter.rem_euclid(WINDOW);
    let shuffled = (within * MULTIPLIER + OFFSET).rem_euclid(WINDOW);
    cycle * WINDOW + shuffled
}

pub(crate) fn parse_deterministic_settings_value(mode_value: &JsonValue) -> DeterministicSettings {
    let Some(object) = mode_value.as_object() else {
        return DeterministicSettings::disabled();
    };

    let enabled = object
        .get("enabled")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    if !enabled {
        return DeterministicSettings::disabled();
    }

    let uuid_v7_enabled = object
        .get("uuid_v7")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let timestamp_enabled = object
        .get("timestamp")
        .and_then(JsonValue::as_bool)
        .unwrap_or(true);
    let timestamp_shuffle_enabled = object
        .get("timestamp_shuffle")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);

    DeterministicSettings {
        enabled,
        uuid_v7_enabled,
        timestamp_enabled,
        timestamp_shuffle_enabled,
    }
}

pub async fn load_settings(backend: &dyn LixBackend) -> Result<DeterministicSettings, LixError> {
    let mode_value = match load_key_value_payload(backend, DETERMINISTIC_MODE_KEY).await {
        Ok(value) => value,
        Err(err) if is_missing_relation_error(&err) => return Ok(DeterministicSettings::disabled()),
        Err(err) => return Err(err),
    };
    let Some(mode_value) = mode_value else {
        return Ok(DeterministicSettings::disabled());
    };

    Ok(parse_deterministic_settings_value(&mode_value))
}

pub async fn load_persisted_sequence_next(backend: &dyn LixBackend) -> Result<i64, LixError> {
    let committed_highest = load_sequence_highest_from_committed_state(backend).await?;
    let untracked_highest = load_sequence_highest_from_untracked_state(backend).await?;
    let highest_seen = committed_highest
        .into_iter()
        .chain(untracked_highest)
        .max()
        .unwrap_or(-1);
    Ok(highest_seen + 1)
}

pub async fn persist_sequence_highest(
    backend: &dyn LixBackend,
    highest_seen: i64,
) -> Result<(), LixError> {
    let snapshot_content = serde_json::json!({
        "key": SEQUENCE_KEY,
        "value": highest_seen
    })
    .to_string();

    let sql = format!(
        "INSERT INTO lix_internal_state_vtable \
         (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, untracked) \
         VALUES ('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', '{schema_version}', true)",
        entity_id = escape_sql_string(SEQUENCE_KEY),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        plugin_key = escape_sql_string(key_value_plugin_key()),
        schema_version = escape_sql_string(key_value_schema_version()),
        snapshot_content = escape_sql_string(&snapshot_content),
    );

    let mut provider = FixedTimestampFunctionProvider;
    let statements = parse_sql_statements(&sql)?;
    let rewritten =
        preprocess_statements_with_provider(statements, &[], &mut provider, backend.dialect())?;
    if let Err(err) = backend
        .execute(&rewritten.sql, rewritten.single_statement_params()?)
        .await
    {
        if is_missing_relation_error(&err) {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

async fn load_key_value_payload(
    backend: &dyn LixBackend,
    entity_id: &str,
) -> Result<Option<JsonValue>, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM lix_internal_state_vtable \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL \
         LIMIT 1",
        schema_key = escape_sql_string(key_value_schema_key()),
        entity_id = escape_sql_string(entity_id),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
    );
    let statements = parse_sql_statements(&sql)?;
    let mut provider = SystemFunctionProvider;
    let rewritten =
        preprocess_statements_with_provider(statements, &[], &mut provider, backend.dialect())?;
    let result = backend
        .execute(&rewritten.sql, rewritten.single_statement_params()?)
        .await?;
    parse_first_payload(result.rows.first())
}

async fn load_sequence_highest_from_committed_state(
    backend: &dyn LixBackend,
) -> Result<Option<i64>, LixError> {
    let table_name = format!(
        "lix_internal_state_materialized_v1_{}",
        key_value_schema_key()
    );
    load_sequence_highest_from_table(
        backend,
        &table_name,
        &format!(
            "entity_id = '{entity_id}' \
             AND version_id = '{version_id}' \
             AND snapshot_content IS NOT NULL \
             AND is_tombstone = 0",
            entity_id = escape_sql_string(SEQUENCE_KEY),
            version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        ),
    )
    .await
}

async fn load_sequence_highest_from_untracked_state(
    backend: &dyn LixBackend,
) -> Result<Option<i64>, LixError> {
    load_sequence_highest_from_table(
        backend,
        "lix_internal_state_untracked",
        &format!(
            "schema_key = '{schema_key}' \
             AND entity_id = '{entity_id}' \
             AND version_id = '{version_id}' \
             AND snapshot_content IS NOT NULL",
            schema_key = escape_sql_string(key_value_schema_key()),
            entity_id = escape_sql_string(SEQUENCE_KEY),
            version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        ),
    )
    .await
}

async fn load_sequence_highest_from_table(
    backend: &dyn LixBackend,
    table_name: &str,
    where_clause: &str,
) -> Result<Option<i64>, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE {where_clause}",
    );
    let result = match backend.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    Ok(result
        .rows
        .iter()
        .filter_map(|row| parse_first_payload(Some(row)).ok().flatten())
        .filter_map(|value| parse_integer_value(&value))
        .max())
}

fn parse_integer_value(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(number) => number.as_i64(),
        JsonValue::String(text) => text.parse::<i64>().ok(),
        _ => None,
    }
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

fn parse_first_payload(row: Option<&Vec<Value>>) -> Result<Option<JsonValue>, LixError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let raw = value_to_string(&row[0], "snapshot_content")?;
    let parsed: JsonValue = serde_json::from_str(&raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("deterministic mode snapshot_content invalid JSON: {err}"),
    })?;
    Ok(parsed.get("value").cloned())
}

struct FixedTimestampFunctionProvider;

impl LixFunctionProvider for FixedTimestampFunctionProvider {
    fn uuid_v7(&mut self) -> String {
        uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        EPOCH_TIMESTAMP.to_string()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{parse_deterministic_settings_value, DeterministicSettings};

    #[test]
    fn non_boolean_flags_do_not_enable_or_disable_settings() {
        let settings = parse_deterministic_settings_value(&json!({
            "enabled": "1",
            "uuid_v7": "0",
            "timestamp": "",
            "timestamp_shuffle": 1
        }));

        assert_eq!(settings.enabled, DeterministicSettings::disabled().enabled);
        assert_eq!(
            settings.uuid_v7_enabled,
            DeterministicSettings::disabled().uuid_v7_enabled
        );
        assert_eq!(
            settings.timestamp_enabled,
            DeterministicSettings::disabled().timestamp_enabled
        );
        assert_eq!(
            settings.timestamp_shuffle_enabled,
            DeterministicSettings::disabled().timestamp_shuffle_enabled
        );
    }
}
