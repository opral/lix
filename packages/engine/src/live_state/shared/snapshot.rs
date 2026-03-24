use std::collections::BTreeMap;

use crate::{LixError, Value};

pub(crate) fn values_from_snapshot_content(
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    let Some(snapshot_content) = snapshot_content else {
        return Ok(BTreeMap::new());
    };

    let parsed = serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("failed to decode transaction snapshot content: {error}"),
        )
    })?;

    let serde_json::Value::Object(object) = parsed else {
        return Ok(BTreeMap::new());
    };

    Ok(object
        .into_iter()
        .map(|(key, value)| (key, value_from_json(value)))
        .collect())
}

fn value_from_json(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Boolean(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Value::Integer(value)
            } else if let Some(value) = value.as_f64() {
                Value::Real(value)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(value) => Value::Text(value),
        serde_json::Value::Array(value) => {
            Value::Json(serde_json::Value::Array(value))
        }
        serde_json::Value::Object(value) => {
            Value::Json(serde_json::Value::Object(value))
        }
    }
}
