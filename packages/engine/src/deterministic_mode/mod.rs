use serde_json::Value as JsonValue;

use crate::errors::classification::is_missing_relation_error;
use crate::functions::{timestamp::timestamp, uuid_v7::uuid_v7, LixFunctionProvider};
use crate::key_value::{
    key_value_file_id, key_value_plugin_key, key_value_schema_key, key_value_schema_version,
    KEY_VALUE_GLOBAL_VERSION,
};
use crate::sql::ast::utils::parse_sql_statements;
use crate::sql::execution::contracts::prepared_statement::{PreparedBatch, PreparedStatement};
use crate::sql::execution::preprocess::preprocess_statements_with_provider_to_plan as preprocess_statements_with_provider;
use crate::sql::execution::write_program_runner::execute_write_program_with_backend;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::internal::write_program::WriteProgram;
use crate::{LixBackend, LixError, SqlDialect, Value};

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

pub(crate) fn deterministic_mode_key() -> &'static str {
    DETERMINISTIC_MODE_KEY
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
    sequence_start: Option<i64>,
    next_sequence: i64,
}

impl RuntimeFunctionProvider {
    pub fn new(settings: DeterministicSettings, sequence_start: Option<i64>) -> Self {
        let next_sequence = sequence_start.unwrap_or(0);
        Self {
            settings,
            sequence_start,
            next_sequence,
        }
    }

    pub fn next_sequence(&self) -> i64 {
        self.next_sequence
    }

    pub fn sequence_start(&self) -> Option<i64> {
        self.sequence_start
    }

    fn take_sequence(&mut self) -> i64 {
        assert!(
            !self.settings.enabled || self.sequence_start.is_some(),
            "deterministic runtime sequence used before initialization"
        );
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

    fn deterministic_sequence_enabled(&self) -> bool {
        self.settings.enabled
    }

    fn deterministic_sequence_initialized(&self) -> bool {
        !self.settings.enabled || self.sequence_start.is_some()
    }

    fn initialize_deterministic_sequence(&mut self, sequence_start: i64) {
        if !self.settings.enabled || self.sequence_start.is_some() {
            return;
        }
        self.sequence_start = Some(sequence_start);
        self.next_sequence = sequence_start;
    }

    fn deterministic_sequence_persist_highest_seen(&self) -> Option<i64> {
        let sequence_start = self.sequence_start?;
        if !self.settings.enabled || self.next_sequence <= sequence_start {
            return None;
        }
        Some(self.next_sequence - 1)
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

pub async fn persist_sequence_highest(
    backend: &dyn LixBackend,
    highest_seen: i64,
) -> Result<(), LixError> {
    let batch = build_persist_sequence_highest_batch(highest_seen, backend.dialect())?;
    let mut program = WriteProgram::new();
    program.push_batch(batch);
    match execute_write_program_with_backend(backend, program).await {
        Ok(_) => {}
        Err(err) if is_missing_relation_error(&err) => return Ok(()),
        Err(err) => return Err(err),
    }
    Ok(())
}

pub(crate) async fn load_runtime_settings(
    backend: &dyn LixBackend,
) -> Result<DeterministicSettings, LixError> {
    let values = match load_key_value_payloads(backend, &[DETERMINISTIC_MODE_KEY]).await {
        Ok(values) => values,
        Err(err) if is_missing_relation_error(&err) => return Ok(DeterministicSettings::disabled()),
        Err(err) => return Err(err),
    };

    Ok(values
        .get(DETERMINISTIC_MODE_KEY)
        .map(parse_deterministic_settings_value)
        .unwrap_or_else(DeterministicSettings::disabled))
}

pub(crate) async fn load_runtime_sequence_start(backend: &dyn LixBackend) -> Result<i64, LixError> {
    let values = match load_key_value_payloads(backend, &[SEQUENCE_KEY]).await {
        Ok(values) => values,
        Err(err) if is_missing_relation_error(&err) => return Ok(0),
        Err(err) => return Err(err),
    };

    let highest_seen = values.get(SEQUENCE_KEY).and_then(parse_integer_value);
    Ok(highest_seen.unwrap_or(-1) + 1)
}

pub(crate) fn build_persist_sequence_highest_batch(
    highest_seen: i64,
    dialect: SqlDialect,
) -> Result<PreparedBatch, LixError> {
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
    let rewritten = preprocess_statements_with_provider(statements, &[], &mut provider, dialect)?;
    let params = rewritten.single_statement_params()?.to_vec();
    Ok(PreparedBatch {
        steps: vec![PreparedStatement {
            sql: rewritten.sql,
            params,
        }],
    })
}

pub(crate) fn build_persist_sequence_highest_sql(highest_seen: i64) -> String {
    let snapshot_content = serde_json::json!({
        "key": SEQUENCE_KEY,
        "value": highest_seen
    })
    .to_string();

    format!(
        "INSERT INTO lix_internal_live_untracked_v1 \
         (entity_id, schema_key, file_id, version_id, global, plugin_key, snapshot_content, metadata, writer_key, schema_version, created_at, updated_at) \
         VALUES ('{entity_id}', '{schema_key}', '{file_id}', '{version_id}', FALSE, '{plugin_key}', '{snapshot_content}', NULL, NULL, '{schema_version}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) \
         ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE SET \
           global = excluded.global, \
           plugin_key = excluded.plugin_key, \
           snapshot_content = excluded.snapshot_content, \
           metadata = excluded.metadata, \
           writer_key = excluded.writer_key, \
           schema_version = excluded.schema_version, \
           updated_at = CURRENT_TIMESTAMP",
        entity_id = escape_sql_string(SEQUENCE_KEY),
        schema_key = escape_sql_string(key_value_schema_key()),
        file_id = escape_sql_string(key_value_file_id()),
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        plugin_key = escape_sql_string(key_value_plugin_key()),
        schema_version = escape_sql_string(key_value_schema_version()),
        snapshot_content = escape_sql_string(&snapshot_content),
    )
}

async fn load_key_value_payloads(
    backend: &dyn LixBackend,
    entity_ids: &[&str],
) -> Result<std::collections::BTreeMap<String, JsonValue>, LixError> {
    if entity_ids.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    let table_name = format!("lix_internal_live_v1_{}", key_value_schema_key());
    let in_list = entity_ids
        .iter()
        .map(|entity_id| format!("'{}'", escape_sql_string(entity_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT entity_id, snapshot_content, precedence \
         FROM (\
           SELECT entity_id, snapshot_content, 0 AS precedence \
           FROM lix_internal_live_untracked_v1 \
           WHERE schema_key = '{schema_key}' \
             AND entity_id IN ({in_list}) \
             AND version_id = '{version_id}' \
             AND snapshot_content IS NOT NULL \
           UNION ALL \
           SELECT entity_id, snapshot_content, 1 AS precedence \
           FROM {table_name} \
           WHERE entity_id IN ({in_list}) \
             AND version_id = '{version_id}' \
             AND snapshot_content IS NOT NULL \
             AND is_tombstone = 0\
         ) visible_key_values \
         ORDER BY entity_id ASC, precedence ASC",
        schema_key = escape_sql_string(key_value_schema_key()),
        in_list = in_list,
        version_id = escape_sql_string(KEY_VALUE_GLOBAL_VERSION),
        table_name = table_name,
    );
    let result = backend.execute(&sql, &[]).await?;
    let mut values = std::collections::BTreeMap::new();
    for row in result.rows {
        let Some(entity_id_value) = row.first() else {
            continue;
        };
        let entity_id = value_to_string(entity_id_value, "entity_id")?;
        if values.contains_key(&entity_id) {
            continue;
        }
        let Some(snapshot_value) = row.get(1) else {
            continue;
        };
        let raw = value_to_string(snapshot_value, "snapshot_content")?;
        let parsed: JsonValue = serde_json::from_str(&raw).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("deterministic mode snapshot_content invalid JSON: {err}"),
        })?;
        if let Some(value) = parsed.get("value") {
            values.insert(entity_id, value.clone());
        }
    }

    Ok(values)
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
