use lix::telemetry::{CompletedTelemetrySpan, TelemetryValue};
use opentelemetry::Context;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt as _};
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value},
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::SpanKind, status::StatusCode},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use prost::Message;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct W3CTraceContext {
    traceparent: String,
    #[serde(default)]
    tracestate: String,
}

pub(crate) fn parse_parent_context_json(value: Option<String>) -> Option<SpanContext> {
    let value = value?;
    let headers: W3CTraceContext = serde_json::from_str(&value).ok()?;
    let context = TraceContextPropagator::new().extract_with_context(&Context::new(), &headers);
    let span_context = context.span().span_context().clone();
    span_context.is_valid().then_some(span_context)
}

impl Extractor for W3CTraceContext {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            Some(&self.traceparent)
        } else if key.eq_ignore_ascii_case("tracestate") {
            Some(&self.tracestate)
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn telemetry_parent_uses_w3c_trace_context_headers() {
        let parent = parse_parent_context_json(Some(
            r#"{"traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01","tracestate":"vendor=opaque"}"#.to_owned(),
        ))
        .expect("parent context");
        assert_eq!(
            parent.trace_id().to_string(),
            "0123456789abcdef0123456789abcdef"
        );
        assert_eq!(parent.span_id().to_string(), "0123456789abcdef");
        assert!(parent.is_remote());
        assert!(parent.is_sampled());
        assert_eq!(parent.trace_state().header(), "vendor=opaque");
    }

    #[test]
    fn telemetry_parent_ignores_invalid_w3c_trace_context() {
        for traceparent in [
            "00-00000000000000000000000000000000-0123456789abcdef-01",
            "00-0123456789abcdef0123456789abcdef-0000000000000000-01",
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-0A",
        ] {
            let context = serde_json::json!({ "traceparent": traceparent }).to_string();
            assert!(parse_parent_context_json(Some(context)).is_none());
        }
        assert!(parse_parent_context_json(Some("not json".to_owned())).is_none());
    }

    #[test]
    fn telemetry_parent_drops_invalid_tracestate_but_keeps_traceparent() {
        let context = parse_parent_context_json(Some(
            r#"{"traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01","tracestate":"invalid state"}"#.to_owned(),
        ))
        .expect("valid traceparent");
        assert!(context.is_valid());
        assert!(context.trace_state().header().is_empty());
    }

    #[test]
    fn telemetry_parent_uses_standard_future_version_parsing() {
        let valid =
            r#"{"traceparent":"01-0123456789abcdef0123456789abcdef-0123456789abcdef-01-a1-b2"}"#;
        assert!(parse_parent_context_json(Some(valid.to_owned())).is_some());
    }

    #[test]
    fn encoded_engine_span_is_a_parented_sanitized_otlp_span() {
        let parent = parse_parent_context_json(Some(
            r#"{"traceparent":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01","tracestate":"vendor=opaque"}"#.to_owned(),
        ))
        .expect("parent context");
        let spans = Arc::new(Mutex::new(Vec::new()));
        let captured_spans = Arc::clone(&spans);
        let sink = lix::telemetry::CallbackTelemetrySink::new(move |span| {
            captured_spans.lock().expect("span lock").push(span);
        })
        .with_root_parent(parent.clone());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let lix = lix::open_lix()
                .with_telemetry(Arc::new(sink))
                .await
                .expect("open engine");
            lix.execute("SELECT 'private-value' AS value, 42 AS number", &[])
                .await
                .expect("execute query");
            lix.close().await.expect("close engine");
        });

        let span = spans
            .lock()
            .expect("span lock")
            .iter()
            .find(|span| span.start.name == "lix.sql.query")
            .cloned()
            .expect("SQL query span");
        let encoded = encode_otlp_request(span);
        let request = ExportTraceServiceRequest::decode(encoded.as_slice())
            .expect("valid OTLP ExportTraceServiceRequest");
        let resource_spans = request.resource_spans.first().expect("resource spans");
        assert!(resource_spans.resource.is_none());
        let scope_spans = resource_spans.scope_spans.first().expect("scope spans");
        assert_eq!(scope_spans.scope.as_ref().unwrap().name, "lix");
        let span = scope_spans.spans.first().expect("OTLP span");
        assert_eq!(span.name, "SELECT");
        assert_eq!(span.kind, SpanKind::Client as i32);
        assert_eq!(span.trace_id, parent.trace_id().to_bytes());
        assert_eq!(span.parent_span_id, parent.span_id().to_bytes());
        assert_eq!(span.trace_state, "vendor=opaque");
        assert_eq!(span.flags, 1);
        assert!(span.start_time_unix_nano > 0);
        assert!(span.end_time_unix_nano >= span.start_time_unix_nano);
        assert_eq!(span.status.as_ref().unwrap().code, StatusCode::Unset as i32);
        assert!(
            !span
                .attributes
                .iter()
                .any(|attribute| attribute.key == "otel.name")
        );
        let query_text = span
            .attributes
            .iter()
            .find(|attribute| attribute.key == "db.query.text")
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.value.as_ref())
            .and_then(|value| match value {
                Value::StringValue(value) => Some(value.as_str()),
                _ => None,
            })
            .expect("sanitized SQL attribute");
        assert_eq!(query_text, "SELECT ? AS value, ? AS number");
        assert!(!query_text.contains("private-value"));
    }
}

pub(crate) fn encode_otlp_request(span: CompletedTelemetrySpan) -> Vec<u8> {
    let mut attributes = span
        .start
        .attributes
        .into_iter()
        .chain(span.end.attributes)
        .map(|attribute| KeyValue {
            key: attribute.key.to_owned(),
            value: Some(AnyValue {
                value: Some(match attribute.value {
                    TelemetryValue::String(value) => Value::StringValue(value),
                    TelemetryValue::I64(value) => Value::IntValue(value),
                    TelemetryValue::Boolean(value) => Value::BoolValue(value),
                }),
            }),
            key_strindex: 0,
        })
        .collect::<Vec<_>>();
    let name = attributes
        .iter()
        .find(|attribute| attribute.key == "otel.name")
        .and_then(|attribute| attribute.value.as_ref())
        .and_then(|value| value.value.as_ref())
        .and_then(|value| match value {
            Value::StringValue(value) => Some(value.clone()),
            _ => None,
        })
        .unwrap_or_else(|| span.start.name.to_owned());
    attributes.retain(|attribute| attribute.key != "otel.name");
    attributes.sort_by(|left, right| left.key.cmp(&right.key));

    let status = match span.end.status {
        lix::telemetry::Status::Unset => StatusCode::Unset,
        lix::telemetry::Status::Ok => StatusCode::Ok,
        lix::telemetry::Status::Error { description } => {
            return request(
                span.span_context,
                name,
                span.start.kind,
                span.start.started_at_unix_ns,
                span.end.duration_ns,
                attributes,
                span.start.parent_span_context,
                span.start.links,
                StatusCode::Error,
                description.into_owned(),
            );
        }
    };
    request(
        span.span_context,
        name,
        span.start.kind,
        span.start.started_at_unix_ns,
        span.end.duration_ns,
        attributes,
        span.start.parent_span_context,
        span.start.links,
        status,
        String::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn request(
    context: SpanContext,
    name: String,
    kind: lix::telemetry::SpanKind,
    start_time_unix_nano: u64,
    duration_ns: u64,
    attributes: Vec<KeyValue>,
    parent: Option<SpanContext>,
    links: Vec<SpanContext>,
    status_code: StatusCode,
    status_message: String,
) -> Vec<u8> {
    let kind = match kind {
        lix::telemetry::SpanKind::Internal => SpanKind::Internal,
        lix::telemetry::SpanKind::Server => SpanKind::Server,
        lix::telemetry::SpanKind::Client => SpanKind::Client,
        lix::telemetry::SpanKind::Producer => SpanKind::Producer,
        lix::telemetry::SpanKind::Consumer => SpanKind::Consumer,
    };
    let span = Span {
        trace_id: context.trace_id().to_bytes().to_vec(),
        span_id: context.span_id().to_bytes().to_vec(),
        trace_state: context.trace_state().header(),
        parent_span_id: parent
            .filter(SpanContext::is_valid)
            .map_or_else(Vec::new, |parent| parent.span_id().to_bytes().to_vec()),
        name,
        kind: kind as i32,
        start_time_unix_nano,
        end_time_unix_nano: start_time_unix_nano.saturating_add(duration_ns),
        attributes,
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: links
            .into_iter()
            .filter(SpanContext::is_valid)
            .map(|link| opentelemetry_proto::tonic::trace::v1::span::Link {
                trace_id: link.trace_id().to_bytes().to_vec(),
                span_id: link.span_id().to_bytes().to_vec(),
                trace_state: link.trace_state().header(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                flags: u32::from(link.trace_flags().to_u8()),
            })
            .collect(),
        dropped_links_count: 0,
        status: Some(Status {
            message: status_message,
            code: status_code as i32,
        }),
        flags: u32::from(context.trace_flags().to_u8()),
    };
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            // The embedding host owns resource identity (service name,
            // version, deployment, and repository attributes).
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "lix".to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    ..InstrumentationScope::default()
                }),
                spans: vec![span],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}
