use serde_json::Value as JsonValue;

use crate::functions::{timestamp::timestamp, uuid_v7::uuid_v7, LixFunctionProvider};
use crate::json_truthiness::{loosely_false, loosely_true};
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::sql::escape_sql_string;
use crate::LixBackend;
use crate::{LixError, SqlDialect, Value};

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
const SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
const EPOCH_TIMESTAMP: &str = "1970-01-01T00:00:00Z";
const DETERMINISTIC_UUID_COUNTER_MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";

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

pub async fn load_settings(backend: &dyn LixBackend) -> Result<DeterministicSettings, LixError> {
    let mode_value = match load_key_value_payload(backend, DETERMINISTIC_MODE_KEY).await {
        Ok(value) => value,
        Err(err) if is_missing_relation_error(&err) => return Ok(DeterministicSettings::disabled()),
        Err(err) => return Err(err),
    };
    let Some(mode_value) = mode_value else {
        return Ok(DeterministicSettings::disabled());
    };

    let enabled = mode_value.get("enabled").map(loosely_true).unwrap_or(false);
    if !enabled {
        return Ok(DeterministicSettings::disabled());
    }

    let uuid_v7_enabled = !mode_value
        .get("uuid_v7")
        .map(loosely_false)
        .unwrap_or(false);
    let timestamp_enabled = !mode_value
        .get("timestamp")
        .map(loosely_false)
        .unwrap_or(false);
    let timestamp_shuffle_enabled = mode_value
        .get("timestamp_shuffle")
        .map(loosely_true)
        .unwrap_or(false);

    Ok(DeterministicSettings {
        enabled,
        uuid_v7_enabled,
        timestamp_enabled,
        timestamp_shuffle_enabled,
    })
}

pub async fn load_persisted_sequence_next(backend: &dyn LixBackend) -> Result<i64, LixError> {
    let sequence_value = match load_key_value_payload(backend, SEQUENCE_KEY).await {
        Ok(value) => value,
        Err(err) if is_missing_relation_error(&err) => return Ok(0),
        Err(err) => return Err(err),
    };
    let next = sequence_value
        .as_ref()
        .and_then(parse_integer_value)
        .map(|highest| highest + 1)
        .unwrap_or(0);
    Ok(next)
}

pub async fn persist_sequence_highest(
    backend: &dyn LixBackend,
    highest_seen: i64,
) -> Result<(), LixError> {
    if !table_exists(backend, UNTRACKED_TABLE).await? {
        return Ok(());
    }

    let snapshot_content = serde_json::json!({
        "key": SEQUENCE_KEY,
        "value": highest_seen
    })
    .to_string();

    let sql = format!(
        "INSERT INTO {table_name} \
         (entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, metadata, schema_version, created_at, updated_at) \
         VALUES ('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', '{plugin_key}', '{snapshot_content}', NULL, '{schema_version}', '{created_at}', '{updated_at}') \
         ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE SET \
           plugin_key = EXCLUDED.plugin_key, \
           snapshot_content = EXCLUDED.snapshot_content, \
           schema_version = EXCLUDED.schema_version, \
           updated_at = EXCLUDED.updated_at",
        table_name = UNTRACKED_TABLE,
        entity_id = escape_sql_string(SEQUENCE_KEY),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        plugin_key = escape_sql_string(key_value_plugin_key()),
        schema_version = escape_sql_string(key_value_schema_version()),
        snapshot_content = escape_sql_string(&snapshot_content),
        created_at = EPOCH_TIMESTAMP,
        updated_at = EPOCH_TIMESTAMP,
    );

    backend.execute(&sql, &[]).await?;
    Ok(())
}

async fn load_key_value_payload(
    backend: &dyn LixBackend,
    entity_id: &str,
) -> Result<Option<JsonValue>, LixError> {
    if table_exists(backend, UNTRACKED_TABLE).await? {
        let payload = load_key_value_payload_from_untracked(backend, entity_id).await?;
        if payload.is_some() {
            return Ok(payload);
        }
    }

    let materialized_table = format!("{MATERIALIZED_PREFIX}{}", key_value_schema_key());
    if table_exists(backend, &materialized_table).await? {
        let payload =
            load_key_value_payload_from_materialized(backend, &materialized_table, entity_id)
                .await?;
        if payload.is_some() {
            return Ok(payload);
        }
    }

    Ok(None)
}

async fn load_key_value_payload_from_untracked(
    backend: &dyn LixBackend,
    entity_id: &str,
) -> Result<Option<JsonValue>, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        table_name = UNTRACKED_TABLE,
        schema_key = escape_sql_string(key_value_schema_key()),
        entity_id = escape_sql_string(entity_id),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
    );
    let result = backend.execute(&sql, &[]).await?;
    parse_first_payload(result.rows.first())
}

async fn load_key_value_payload_from_materialized(
    backend: &dyn LixBackend,
    table_name: &str,
    entity_id: &str,
) -> Result<Option<JsonValue>, LixError> {
    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND entity_id = '{entity_id}' \
           AND version_id = '{version_id}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
         ORDER BY updated_at DESC \
         LIMIT 1",
        table_name = table_name,
        schema_key = escape_sql_string(key_value_schema_key()),
        entity_id = escape_sql_string(entity_id),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
    );
    let result = backend.execute(&sql, &[]).await?;
    parse_first_payload(result.rows.first())
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
            message: format!("expected text value for {name}"),
        }),
    }
}

fn is_missing_relation_error(err: &LixError) -> bool {
    let lower = err.message.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

async fn table_exists(backend: &dyn LixBackend, table_name: &str) -> Result<bool, LixError> {
    let sql = match backend.dialect() {
        SqlDialect::Sqlite => format!(
            "SELECT 1 FROM sqlite_master \
             WHERE type = 'table' \
               AND name = '{table_name}' \
             LIMIT 1",
            table_name = escape_sql_string(table_name),
        ),
        SqlDialect::Postgres => format!(
            "SELECT 1 FROM information_schema.tables \
             WHERE table_schema = current_schema() \
               AND table_name = '{table_name}' \
             LIMIT 1",
            table_name = escape_sql_string(table_name),
        ),
    };
    let result = backend.execute(&sql, &[]).await?;
    Ok(!result.rows.is_empty())
}

fn parse_first_payload(row: Option<&Vec<Value>>) -> Result<Option<JsonValue>, LixError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let raw = value_to_string(&row[0], "snapshot_content")?;
    let parsed: JsonValue = serde_json::from_str(&raw).map_err(|err| LixError {
        message: format!("deterministic mode snapshot_content invalid JSON: {err}"),
    })?;
    Ok(parsed.get("value").cloned())
}
