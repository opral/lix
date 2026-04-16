use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::common::escape_sql_string;
use crate::common::is_missing_relation_error;
use crate::live_state::{key_value_schema_key, tracked_relation_name};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};

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

#[derive(Debug, Clone)]
struct PersistedKeyValueStorageScope {
    table_name: String,
    version_id: String,
}

impl PersistedKeyValueStorageScope {
    fn new(table_name: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            version_id: version_id.into(),
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
    let scope = PersistedKeyValueStorageScope::new(
        tracked_relation_name(key_value_schema_key()),
        GLOBAL_VERSION_ID,
    );
    let values =
        match load_persisted_key_value_payloads(backend, &scope, &[DETERMINISTIC_MODE_KEY]).await {
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

async fn load_persisted_key_value_payloads(
    backend: &dyn LixBackend,
    scope: &PersistedKeyValueStorageScope,
    entity_ids: &[&str],
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    if entity_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

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
           FROM {table_name} u \
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
        untracked_value_expr = untracked_value_expr,
        in_list = in_list,
        version_id = escape_sql_string(&scope.version_id),
        table_name = scope.table_name,
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
            hint: None,
        })?;
        values.insert(entity_id, parsed);
    }

    Ok(values)
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
            hint: None,
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
