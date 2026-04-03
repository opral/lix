use std::collections::BTreeMap;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::runtime::cel::CelEvaluator;
use crate::LixError;
use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LixcolOverrideValue {
    Null,
    Boolean(bool),
    Number(String),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LixcolOverride {
    pub(crate) key: String,
    pub(crate) value: LixcolOverrideValue,
}

pub(crate) fn collect_lixcol_overrides(
    schema: &JsonValue,
    schema_key: &str,
    evaluator: &CelEvaluator,
) -> Result<Vec<LixcolOverride>, LixError> {
    reject_removed_lixcol_version_override(schema, schema_key)?;

    let mut overrides = Vec::new();
    for key in [
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_file_id",
        "lixcol_plugin_key",
        "lixcol_global",
        "lixcol_metadata",
        "lixcol_untracked",
        "lixcol_writer_key",
    ] {
        let Some(value) = extract_lixcol_scalar_override(schema, schema_key, key, evaluator)?
        else {
            continue;
        };
        overrides.push(LixcolOverride {
            key: key.to_string(),
            value,
        });
    }
    Ok(overrides)
}

pub(crate) fn collect_state_column_overrides(
    schema: &JsonValue,
    schema_key: &str,
    evaluator: &CelEvaluator,
) -> Result<BTreeMap<String, Value>, LixError> {
    let mut out = BTreeMap::new();
    for override_entry in collect_lixcol_overrides(schema, schema_key, evaluator)? {
        let Some(column) = entity_state_column_name(&override_entry.key) else {
            continue;
        };
        out.insert(
            column.to_string(),
            lixcol_override_to_engine_value(&override_entry.value),
        );
    }
    Ok(out)
}

fn raw_lixcol_override_expression<'a>(schema: &'a JsonValue, key: &str) -> Option<&'a str> {
    schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .and_then(|overrides| overrides.get(key))
        .and_then(JsonValue::as_str)
}

fn reject_removed_lixcol_version_override(
    schema: &JsonValue,
    schema_key: &str,
) -> Result<(), LixError> {
    if raw_lixcol_override_expression(schema, "lixcol_version_id").is_some() {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "schema '{}' uses removed x-lix-override-lixcols.lixcol_version_id support; use lixcol_global for global write scope",
                schema_key
            ),
        });
    }

    Ok(())
}

fn evaluate_lixcol_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<JsonValue>, LixError> {
    let Some(raw_expression) = raw_lixcol_override_expression(schema, key) else {
        return Ok(None);
    };
    let expression = raw_expression.trim();
    if expression.is_empty() {
        return Ok(None);
    }
    evaluator
        .evaluate(expression, &JsonMap::new())
        .map(Some)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "invalid x-lix-override-lixcols expression for '{}.{}': {}",
                schema_key, key, error.description
            ),
        })
}

fn extract_lixcol_scalar_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<LixcolOverrideValue>, LixError> {
    let Some(value) = evaluate_lixcol_override(schema, schema_key, key, evaluator)? else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(Some(LixcolOverrideValue::Null)),
        JsonValue::Bool(value) => Ok(Some(LixcolOverrideValue::Boolean(value))),
        JsonValue::Number(value) => Ok(Some(LixcolOverrideValue::Number(value.to_string()))),
        JsonValue::String(value) => Ok(Some(LixcolOverrideValue::String(value))),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "x-lix-override-lixcols '{}.{}' must evaluate to a scalar or null",
                schema_key, key
            ),
        }),
    }
}

fn entity_state_column_name(column: &str) -> Option<&'static str> {
    match column.to_ascii_lowercase().as_str() {
        "lixcol_entity_id" => Some("entity_id"),
        "lixcol_schema_key" => Some("schema_key"),
        "lixcol_file_id" => Some("file_id"),
        "lixcol_plugin_key" => Some("plugin_key"),
        "lixcol_schema_version" => Some("schema_version"),
        "lixcol_global" => Some("global"),
        "lixcol_writer_key" => Some("writer_key"),
        "lixcol_untracked" => Some("untracked"),
        "lixcol_metadata" => Some("metadata"),
        _ => None,
    }
}

fn lixcol_override_to_engine_value(value: &LixcolOverrideValue) -> Value {
    match value {
        LixcolOverrideValue::Null => Value::Null,
        LixcolOverrideValue::Boolean(value) => Value::Boolean(*value),
        LixcolOverrideValue::Number(value) => value
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| value.parse::<f64>().map(Value::Real))
            .unwrap_or_else(|_| Value::Text(value.clone())),
        LixcolOverrideValue::String(value) => Value::Text(value.clone()),
    }
}
