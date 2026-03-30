use serde_json::Value as JsonValue;

use crate::backend::prepared::PreparedBatch;
use crate::engine::TransactionBackendAdapter;
use crate::errors::classification::is_missing_relation_error;
use crate::functions::{timestamp::timestamp, uuid_v7::uuid_v7, LixFunctionProvider};
use crate::key_value::{
    build_ensure_runtime_sequence_row_sql as build_ensure_runtime_sequence_row_sql_impl,
    build_lock_runtime_sequence_row_sql as build_lock_runtime_sequence_row_sql_impl,
    build_update_runtime_sequence_highest_sql as build_update_runtime_sequence_highest_sql_impl,
    load_key_value_payloads,
};
use crate::{LixBackend, LixBackendTransaction, LixError, SqlDialect, Value};

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
const SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";
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

pub(crate) async fn load_runtime_sequence_start_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let visible_sequence_start = {
        let backend = TransactionBackendAdapter::new(transaction);
        load_runtime_sequence_start(&backend).await?
    };
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
    let parsed: JsonValue = serde_json::from_str(&raw).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("deterministic sequence value_json invalid JSON: {err}"),
    })?;
    Ok(parsed.as_i64().unwrap_or(-1) + 1)
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

pub(crate) async fn load_runtime_sequence_start(backend: &dyn LixBackend) -> Result<i64, LixError> {
    let values = match load_key_value_payloads(backend, &[SEQUENCE_KEY]).await {
        Ok(values) => values,
        Err(err) if is_missing_relation_error(&err) => return Ok(0),
        Err(err) => return Err(err),
    };

    let highest_seen = values.get(SEQUENCE_KEY).and_then(parse_integer_value);
    Ok(highest_seen.unwrap_or(-1) + 1)
}

pub(crate) fn build_ensure_runtime_sequence_row_sql(
    highest_seen: i64,
    dialect: SqlDialect,
) -> String {
    build_ensure_runtime_sequence_row_sql_impl(highest_seen, dialect, SEQUENCE_KEY)
}

pub(crate) fn build_lock_runtime_sequence_row_sql(dialect: SqlDialect) -> String {
    build_lock_runtime_sequence_row_sql_impl(dialect, SEQUENCE_KEY)
}

pub(crate) fn build_update_runtime_sequence_highest_sql(
    highest_seen: i64,
    dialect: SqlDialect,
) -> String {
    build_update_runtime_sequence_highest_sql_impl(highest_seen, dialect, SEQUENCE_KEY)
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
