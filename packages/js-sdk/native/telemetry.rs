use std::collections::BTreeMap;

use lix::telemetry::{CompletedTelemetrySpan, SpanContext, SpanKind, Status, TelemetryValue};
use opentelemetry::trace::{SpanId, TraceFlags, TraceId, TraceState};
use serde::{Deserialize, Serialize};
use std::str::FromStr as _;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TelemetrySpanDto {
    schema_version: u8,
    name: &'static str,
    kind: &'static str,
    trace_id: String,
    span_id: String,
    trace_flags: u8,
    #[serde(skip_serializing_if = "String::is_empty")]
    trace_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    links: Vec<TelemetrySpanLinkDto>,
    started_at_unix_ms: f64,
    duration_ms: f64,
    status: TelemetrySpanStatusDto,
    attributes: BTreeMap<&'static str, serde_json::Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TelemetrySpanLinkDto {
    trace_id: String,
    span_id: String,
    trace_flags: u8,
    #[serde(skip_serializing_if = "String::is_empty")]
    trace_state: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TelemetrySpanStatusDto {
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TelemetryParentContextDto {
    trace_id: String,
    span_id: String,
    trace_flags: u8,
    #[serde(default)]
    trace_state: String,
}

impl TryFrom<TelemetryParentContextDto> for SpanContext {
    type Error = String;

    fn try_from(value: TelemetryParentContextDto) -> Result<Self, Self::Error> {
        if value.trace_id.len() != 32
            || value.span_id.len() != 16
            || !value
                .trace_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !value
                .span_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(
                "telemetry parent traceId/spanId must be 32/16 hexadecimal characters".into(),
            );
        }
        let trace_id = TraceId::from_hex(&value.trace_id)
            .map_err(|_| "telemetry parent traceId must be 32 hexadecimal characters")?;
        let span_id = SpanId::from_hex(&value.span_id)
            .map_err(|_| "telemetry parent spanId must be 16 hexadecimal characters")?;
        let trace_state = TraceState::from_str(&value.trace_state)
            .map_err(|_| "telemetry parent traceState is not a valid W3C tracestate")?;
        let context = SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::new(value.trace_flags),
            true,
            trace_state,
        );
        if !context.is_valid() {
            return Err("telemetry parent context must contain non-zero traceId and spanId".into());
        }
        Ok(context)
    }
}

pub(crate) fn parse_parent_context_json(
    value: Option<String>,
) -> Result<Option<SpanContext>, String> {
    value
        .map(|value| {
            serde_json::from_str::<TelemetryParentContextDto>(&value)
                .map_err(|error| format!("invalid telemetry parent context: {error}"))?
                .try_into()
        })
        .transpose()
}

#[expect(
    clippy::cast_precision_loss,
    reason = "JavaScript telemetry timestamps and durations are represented as Number"
)]
impl From<CompletedTelemetrySpan> for TelemetrySpanDto {
    fn from(span: CompletedTelemetrySpan) -> Self {
        let mut attributes = BTreeMap::new();
        for attribute in span.start.attributes.into_iter().chain(span.end.attributes) {
            let value = match attribute.value {
                TelemetryValue::String(value) => serde_json::Value::String(value),
                TelemetryValue::I64(value) => serde_json::Value::from(value),
                TelemetryValue::Boolean(value) => serde_json::Value::Bool(value),
            };
            attributes.insert(attribute.key, value);
        }
        Self {
            schema_version: 3,
            name: span.start.name,
            kind: match span.start.kind {
                SpanKind::Client => "client",
                SpanKind::Consumer => "consumer",
                SpanKind::Internal => "internal",
                SpanKind::Producer => "producer",
                SpanKind::Server => "server",
            },
            trace_id: span.span_context.trace_id().to_string(),
            span_id: span.span_context.span_id().to_string(),
            trace_flags: span.span_context.trace_flags().to_u8(),
            trace_state: span.span_context.trace_state().header(),
            parent_span_id: span
                .start
                .parent_span_context
                .as_ref()
                .map(|parent| parent.span_id().to_string()),
            links: span
                .start
                .links
                .into_iter()
                .map(|link| TelemetrySpanLinkDto {
                    trace_id: link.trace_id().to_string(),
                    span_id: link.span_id().to_string(),
                    trace_flags: link.trace_flags().to_u8(),
                    trace_state: link.trace_state().header(),
                })
                .collect(),
            started_at_unix_ms: span.start.started_at_unix_ms as f64,
            duration_ms: span.end.duration_ns as f64 / 1_000_000.0,
            status: match span.end.status {
                Status::Unset => TelemetrySpanStatusDto {
                    code: "unset",
                    description: None,
                },
                Status::Error { description } => TelemetrySpanStatusDto {
                    code: "error",
                    description: Some(description.into_owned()),
                },
                Status::Ok => TelemetrySpanStatusDto {
                    code: "ok",
                    description: None,
                },
            },
            attributes,
        }
    }
}
