mod scope;
mod sequence;
mod storage;

use serde_json::Value as JsonValue;

use crate::diagnostics::is_missing_relation_error;
use crate::runtime::functions::{timestamp::timestamp, uuid_v7::uuid_v7, LixFunctionProvider};
use crate::{LixBackend, LixError};
pub(crate) use scope::global_deterministic_settings_storage_scope;
pub(crate) use sequence::{
    build_ensure_runtime_sequence_row_sql, build_update_runtime_sequence_highest_sql,
    deterministic_sequence_key, ensure_runtime_sequence_initialized_in_transaction,
    persist_runtime_sequence_in_transaction,
};
use storage::load_persisted_key_value_payloads;
pub(crate) use storage::PersistedKeyValueStorageScope;

const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
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
    storage_scope: &PersistedKeyValueStorageScope,
) -> Result<DeterministicSettings, LixError> {
    let values =
        match load_persisted_key_value_payloads(backend, storage_scope, &[DETERMINISTIC_MODE_KEY])
            .await
        {
            Ok(values) => values,
            Err(err) if is_missing_relation_error(&err) => {
                return Ok(DeterministicSettings::disabled());
            }
            Err(err) => return Err(err),
        };

    Ok(values
        .get(DETERMINISTIC_MODE_KEY)
        .map(parse_deterministic_settings_value)
        .unwrap_or_else(DeterministicSettings::disabled))
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
