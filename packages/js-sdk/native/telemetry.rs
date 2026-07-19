use std::collections::BTreeMap;

use lix_sdk::{CompletedTelemetrySpan, TelemetrySpanStatus, TelemetryValue};
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelemetrySpanDto {
    schema_version: u8,
    name: &'static str,
    started_at_unix_ms: f64,
    duration_ms: f64,
    status: &'static str,
    attributes: BTreeMap<&'static str, serde_json::Value>,
}

impl From<CompletedTelemetrySpan> for TelemetrySpanDto {
    fn from(span: CompletedTelemetrySpan) -> Self {
        let mut attributes = BTreeMap::new();
        for attribute in span.start.attributes.into_iter().chain(span.end.attributes) {
            let value = match attribute.value {
                TelemetryValue::String(value) => serde_json::Value::String(value),
                TelemetryValue::U64(value) => serde_json::Value::from(value),
                TelemetryValue::Boolean(value) => serde_json::Value::Bool(value),
            };
            attributes.insert(attribute.key, value);
        }
        Self {
            schema_version: 1,
            name: span.start.name,
            started_at_unix_ms: span.start.started_at_unix_ms as f64,
            duration_ms: span.end.duration_ns as f64 / 1_000_000.0,
            status: match span.end.status {
                TelemetrySpanStatus::Ok => "ok",
                TelemetrySpanStatus::Error => "error",
            },
            attributes,
        }
    }
}
