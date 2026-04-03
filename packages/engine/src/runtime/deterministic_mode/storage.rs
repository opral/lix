use std::collections::BTreeMap;

use crate::live_state::schema_access::tracked_relation_name;
use crate::schema::builtin::storage::key_value_schema_key;
use crate::text::escape_sql_string;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};
use serde_json::Value as JsonValue;

pub(super) async fn load_persisted_key_value_payloads(
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
        version_id = escape_sql_string(GLOBAL_VERSION_ID),
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

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}
