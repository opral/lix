#![allow(dead_code)]

use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use text_plugin::{DetectedChange, File, Scalar};

pub type SnapshotContent = BTreeMap<String, Scalar>;

#[derive(Debug, Deserialize)]
pub struct LineSnapshot {
    pub content_base64: String,
    pub ending: String,
}

#[derive(Debug, Deserialize)]
pub struct DocumentSnapshot {
    pub line_ids: Vec<String>,
}

pub fn file_from_bytes(data: &[u8]) -> File {
    File {
        data: data.to_vec(),
    }
}

pub fn parse_line_snapshot(change: &DetectedChange) -> LineSnapshot {
    serde_json::from_value(snapshot_value(change)).expect("line snapshot should parse")
}

pub fn parse_document_snapshot(change: &DetectedChange) -> DocumentSnapshot {
    serde_json::from_value(snapshot_value(change)).expect("document snapshot should parse")
}

pub fn snapshot_content(value: Value) -> SnapshotContent {
    let Value::Object(object) = value else {
        panic!("snapshot_content must be a JSON object");
    };

    object
        .into_iter()
        .map(|(key, value)| (key, scalar_from_value(value)))
        .collect()
}

fn snapshot_value(change: &DetectedChange) -> Value {
    let snapshot_content = change
        .snapshot_content
        .as_ref()
        .expect("snapshot should exist");
    let object = snapshot_content
        .iter()
        .map(|(key, value)| (key.clone(), value_from_scalar(value)))
        .collect::<Map<_, _>>();
    Value::Object(object)
}

fn scalar_from_value(value: Value) -> Scalar {
    match value {
        Value::Null => Scalar::Nil,
        Value::Bool(value) => Scalar::Boolean(value),
        Value::String(value) => Scalar::Text(value),
        Value::Number(_) | Value::Array(_) | Value::Object(_) => {
            Scalar::Json(serde_json::to_string(&value).expect("snapshot scalar should encode"))
        }
    }
}

fn value_from_scalar(value: &Scalar) -> Value {
    match value {
        Scalar::Nil => Value::Null,
        Scalar::Boolean(value) => Value::Bool(*value),
        Scalar::Number(value) => {
            Value::Number(serde_json::Number::from_f64(*value).expect("finite JSON number"))
        }
        Scalar::Text(value) => Value::String(value.clone()),
        Scalar::Json(value) => serde_json::from_str(value).expect("JSON scalar should parse"),
    }
}
