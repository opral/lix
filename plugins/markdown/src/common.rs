use crate::exports::lix::plugin::api::{PluginError, Scalar};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(crate) type SnapshotContent = BTreeMap<String, Scalar>;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DocumentSnapshotContent {
    pub(crate) id: String,
    pub(crate) order: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockSnapshotContent {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) node_type: String,
    pub(crate) node: Value,
    pub(crate) markdown: String,
}

pub(crate) fn snapshot_content_from_value(
    value: Value,
    label: &str,
) -> Result<SnapshotContent, PluginError> {
    let Value::Object(object) = value else {
        return Err(PluginError::Internal(format!(
            "{label} snapshot must serialize to a JSON object"
        )));
    };

    object
        .into_iter()
        .map(|(key, value)| Ok((key, scalar_from_json_value(value)?)))
        .collect()
}

pub(crate) fn snapshot_content_to_json(
    snapshot_content: &SnapshotContent,
    label: &str,
) -> Result<String, PluginError> {
    let object = snapshot_content
        .iter()
        .map(|(key, value)| Ok((key.clone(), json_value_from_scalar(value, label)?)))
        .collect::<Result<Map<_, _>, _>>()?;
    serde_json::to_string(&Value::Object(object)).map_err(|error| {
        PluginError::Internal(format!("failed to encode {label} snapshot JSON: {error}"))
    })
}

fn scalar_from_json_value(value: Value) -> Result<Scalar, PluginError> {
    match value {
        Value::Null => Ok(Scalar::Nil),
        Value::Bool(value) => Ok(Scalar::Boolean(value)),
        Value::String(value) => Ok(Scalar::Text(value)),
        Value::Number(_) | Value::Array(_) | Value::Object(_) => serde_json::to_string(&value)
            .map(Scalar::Json)
            .map_err(|error| {
                PluginError::Internal(format!("failed to encode snapshot scalar JSON: {error}"))
            }),
    }
}

fn json_value_from_scalar(value: &Scalar, label: &str) -> Result<Value, PluginError> {
    match value {
        Scalar::Nil => Ok(Value::Null),
        Scalar::Boolean(value) => Ok(Value::Bool(*value)),
        Scalar::Number(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| {
                PluginError::InvalidInput(format!(
                    "{label} snapshot contains NaN or infinite number"
                ))
            }),
        Scalar::Text(value) => Ok(Value::String(value.clone())),
        Scalar::Json(value) => serde_json::from_str(value).map_err(|error| {
            PluginError::InvalidInput(format!(
                "{label} snapshot contains invalid JSON scalar: {error}"
            ))
        }),
    }
}
