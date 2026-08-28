use anyhow::{Context, Result};
use lix_sdk::telemetry::{OpenTelemetryTracingSink, TelemetrySink};
use opentelemetry::{KeyValue, trace::TracerProvider as _};
use opentelemetry_otlp::{Compression, Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    runtime,
    trace::{
        BatchConfigBuilder, SdkTracerProvider, SpanExporter,
        span_processor_with_async_runtime::BatchSpanProcessor,
    },
};
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::{Subscriber, field::Visit, span::Attributes};
use tracing_subscriber::{
    EnvFilter, Layer,
    fmt::format::FmtSpan,
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};

const PERF_SPAN_EVENTS_ENV: &str = "LIX_SERVER_PERF_SPANS";
const OTEL_TELEMETRY_FILTER: &str = "lix_server=info,lix=info,lix_sql=info";

#[derive(Clone, Debug, Default)]
/// Tracks SQL spans that have started under a protocol request but have not closed.
pub struct InFlightSqlRegistry {
    inner: Arc<Mutex<InFlightSqlState>>,
}

#[derive(Debug, Default)]
struct InFlightSqlState {
    request_by_sql_span: HashMap<tracing::span::Id, tracing::span::Id>,
    activity_by_request:
        HashMap<tracing::span::Id, HashMap<tracing::span::Id, InFlightSqlActivity>>,
    errors_by_request: HashMap<tracing::span::Id, Vec<InFlightSqlActivity>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InFlightSqlActivity {
    pub(crate) operation: Option<String>,
    pub(crate) fingerprint: Option<String>,
    pub(crate) execution_kind: Option<String>,
    pub(crate) batch_index: Option<u64>,
    pub(crate) error_code: Option<String>,
}

impl InFlightSqlRegistry {
    pub(crate) fn current(&self) -> Vec<InFlightSqlActivity> {
        let Some(request_span_id) = tracing::Span::current().id() else {
            return Vec::new();
        };
        let state = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut activities = state
            .activity_by_request
            .get(&request_span_id)
            .map(|activities| activities.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        activities.sort_by_key(|activity| activity.batch_index);
        activities
    }

    /// Drains completed SQL failures for the current protocol request.
    ///
    /// Lix SQL spans finish before the protocol response is returned. Keeping
    /// only their redacted fingerprint and stable error code until this call
    /// lets the HTTP boundary log actionable failures without buffering or
    /// parsing request/response bodies.
    pub(crate) fn take_errors(&self) -> Vec<InFlightSqlActivity> {
        let Some(request_span_id) = tracing::Span::current().id() else {
            return Vec::new();
        };
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .errors_by_request
            .remove(&request_span_id)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
struct InFlightSqlLayer {
    registry: InFlightSqlRegistry,
}

impl<S> Layer<S> for InFlightSqlLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attributes: &Attributes<'_>,
        id: &tracing::span::Id,
        context: LayerContext<'_, S>,
    ) {
        if attributes.metadata().name() != "lix.sql.query" {
            return;
        }
        let Some(sql_span) = context.span(id) else {
            return;
        };
        let mut ancestor = sql_span.parent();
        let mut request_span_id = None;
        while let Some(span) = ancestor {
            if span.metadata().name() == "lix.protocol.request" {
                request_span_id = Some(span.id().clone());
                break;
            }
            ancestor = span.parent();
        }
        let Some(request_span_id) = request_span_id else {
            return;
        };

        let mut activity = InFlightSqlActivity::default();
        attributes.record(&mut SqlActivityVisitor(&mut activity));
        let mut state = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .request_by_sql_span
            .insert(id.clone(), request_span_id.clone());
        state
            .activity_by_request
            .entry(request_span_id)
            .or_default()
            .insert(id.clone(), activity);
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _context: LayerContext<'_, S>,
    ) {
        let mut state = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(request_span_id) = state.request_by_sql_span.get(id).cloned() else {
            return;
        };
        let Some(activity) = state
            .activity_by_request
            .get_mut(&request_span_id)
            .and_then(|activities| activities.get_mut(id))
        else {
            return;
        };
        values.record(&mut SqlActivityVisitor(activity));
    }

    fn on_close(&self, id: tracing::span::Id, context: LayerContext<'_, S>) {
        let mut state = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if context
            .span(&id)
            .is_some_and(|span| span.metadata().name() == "lix.protocol.request")
        {
            state.activity_by_request.remove(&id);
            state.errors_by_request.remove(&id);
            state
                .request_by_sql_span
                .retain(|_, request_span_id| request_span_id != &id);
            return;
        }
        let Some(request_span_id) = state.request_by_sql_span.remove(&id) else {
            return;
        };
        let (activity, remove_request) = state
            .activity_by_request
            .get_mut(&request_span_id)
            .map_or((None, false), |activities| {
                let activity = activities.remove(&id);
                (activity, activities.is_empty())
            });
        if remove_request {
            state.activity_by_request.remove(&request_span_id);
        }
        if let Some(activity) = activity {
            if activity.error_code.is_some() {
                state
                    .errors_by_request
                    .entry(request_span_id)
                    .or_default()
                    .push(activity);
            }
        }
    }
}

struct SqlActivityVisitor<'a>(&'a mut InFlightSqlActivity);

impl Visit for SqlActivityVisitor<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "db.operation.name" => self.0.operation = Some(value.to_string()),
            "lix.sql.fingerprint" => self.0.fingerprint = Some(value.to_string()),
            "lix.execution.kind" => self.0.execution_kind = Some(value.to_string()),
            "error.type" => self.0.error_code = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "lix.batch.index" {
            self.0.batch_index = Some(value);
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

/// Host-owned telemetry runtime. Lix receives only the vendor-neutral sink;
/// exporters and filtering remain server concerns.
pub struct TelemetryRuntime {
    pub trace_provider: SdkTracerProvider,
    pub in_flight_sql: InFlightSqlRegistry,
    pub lix_sink: Arc<dyn TelemetrySink>,
}

impl std::fmt::Debug for TelemetryRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryRuntime")
            .field("trace_provider", &self.trace_provider)
            .field("in_flight_sql", &self.in_flight_sql)
            .field("lix_sink", &"dyn TelemetrySink")
            .finish()
    }
}

/// Installs server tracing and builds the explicitly configured Lix sink.
pub fn init() -> TelemetryRuntime {
    let provider = match provider_from_env() {
        Ok(provider) => provider,
        Err(error) => {
            eprintln!("failed to initialize OTLP telemetry; continuing without export: {error:#}");
            SdkTracerProvider::builder().build()
        }
    };
    let tracer = provider.tracer("lix-server");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let span_events = if perf_span_events_enabled() {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };
    let in_flight_sql = InFlightSqlRegistry::default();
    let log_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let telemetry_filter = EnvFilter::new(OTEL_TELEMETRY_FILTER);
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(span_events)
                .with_filter(log_filter),
        )
        .with(telemetry_layer.with_filter(telemetry_filter))
        .with(InFlightSqlLayer {
            registry: in_flight_sql.clone(),
        });
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::set_global_default(dispatch.clone())
        .expect("install the lix-server tracing dispatch once");

    TelemetryRuntime {
        trace_provider: provider,
        in_flight_sql,
        lix_sink: Arc::new(OpenTelemetryTracingSink::new(dispatch)),
    }
}

fn perf_span_events_enabled() -> bool {
    env::var(PERF_SPAN_EVENTS_ENV)
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true"))
}

fn provider_from_env() -> Result<SdkTracerProvider> {
    let Some(endpoint) = optional_nonempty_env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")? else {
        return Ok(SdkTracerProvider::builder().build());
    };
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(5))
        .with_compression(Compression::Gzip)
        .build()
        .context("build OTLP HTTP exporter")?;
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("lix-server")
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(batch_span_processor(exporter))
        .build())
}

fn batch_span_processor<T>(exporter: T) -> BatchSpanProcessor<runtime::Tokio>
where
    T: SpanExporter + 'static,
{
    BatchSpanProcessor::builder(exporter, runtime::Tokio)
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_max_export_timeout(Duration::from_secs(5))
                .build(),
        )
        .build()
}

fn optional_nonempty_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => anyhow::bail!("{name} must not be empty when set"),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}
