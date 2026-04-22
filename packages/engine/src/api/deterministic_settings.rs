use serde_json::Value as JsonValue;

use crate::common::is_missing_relation_error;
use crate::{LixBackend, LixError};

pub(crate) const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeterministicSettings {
    pub(crate) enabled: bool,
    pub(crate) uuid_v7_enabled: bool,
    pub(crate) timestamp_enabled: bool,
    pub(crate) timestamp_shuffle_enabled: bool,
}

impl DeterministicSettings {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            uuid_v7_enabled: true,
            timestamp_enabled: true,
            timestamp_shuffle_enabled: false,
        }
    }
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

pub(crate) async fn load_global_runtime_settings(
    backend: &dyn LixBackend,
) -> Result<DeterministicSettings, LixError> {
    let values = match crate::api::storage::load_global_runtime_setting_payloads(
        backend,
        &[DETERMINISTIC_MODE_KEY],
    )
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
