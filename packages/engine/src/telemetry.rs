use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A stable category that lets sinks filter spans before Lix builds attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySpanKind {
    SqlQuery,
    SqlBatch,
    SqlCoherentReadBatch,
}

/// One vendor-neutral telemetry attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryAttribute {
    pub key: &'static str,
    pub value: TelemetryValue,
}

impl TelemetryAttribute {
    pub fn string(key: &'static str, value: impl Into<String>) -> Self {
        Self {
            key,
            value: TelemetryValue::String(value.into()),
        }
    }

    pub fn u64(key: &'static str, value: u64) -> Self {
        Self {
            key,
            value: TelemetryValue::U64(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryValue {
    String(String),
    U64(u64),
    Boolean(bool),
}

/// Information available when an engine operation begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanStart {
    pub kind: TelemetrySpanKind,
    pub name: &'static str,
    pub started_at_unix_ms: u64,
    pub attributes: Vec<TelemetryAttribute>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySpanStatus {
    Ok,
    Error,
}

/// Information available when an engine operation finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanEnd {
    pub duration_ns: u64,
    pub status: TelemetrySpanStatus,
    pub attributes: Vec<TelemetryAttribute>,
}

/// A completed span used by callback and cross-runtime adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTelemetrySpan {
    pub start: TelemetrySpanStart,
    pub end: TelemetrySpanEnd,
}

/// Per-engine telemetry destination. Lix never installs a global exporter.
pub trait TelemetrySink: Send + Sync {
    /// Called before Lix sanitizes or fingerprints SQL. Returning false makes
    /// the disabled path avoid all telemetry-specific work.
    fn enabled(&self, _kind: TelemetrySpanKind) -> bool {
        true
    }

    fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle>;
}

/// A live span. `enter` is used on every future poll so native tracing keeps
/// correct async parentage without holding an entered span across an await.
pub trait TelemetrySpanHandle: Send + Sync {
    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_>;
    fn finish(self: Box<Self>, end: TelemetrySpanEnd);
}

pub trait TelemetrySpanEnterGuard {}

impl TelemetrySpanEnterGuard for () {}

/// Sink adapter for hosts that consume completed spans through a callback.
#[expect(missing_debug_implementations)]
pub struct CallbackTelemetrySink {
    callback: Arc<dyn Fn(CompletedTelemetrySpan) + Send + Sync>,
}

impl CallbackTelemetrySink {
    pub fn new(callback: impl Fn(CompletedTelemetrySpan) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }
}

impl TelemetrySink for CallbackTelemetrySink {
    fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
        Box::new(CallbackTelemetrySpan {
            callback: Arc::clone(&self.callback),
            start,
        })
    }
}

struct CallbackTelemetrySpan {
    callback: Arc<dyn Fn(CompletedTelemetrySpan) + Send + Sync>,
    start: TelemetrySpanStart,
}

impl TelemetrySpanHandle for CallbackTelemetrySpan {
    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
        Box::new(())
    }

    fn finish(self: Box<Self>, end: TelemetrySpanEnd) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.callback)(CompletedTelemetrySpan {
                start: self.start,
                end,
            });
        }));
    }
}

/// Explicit adapter from engine telemetry into the Rust `tracing` ecosystem.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingTelemetrySink;

impl TracingTelemetrySink {
    pub fn new() -> Self {
        Self
    }
}

impl TelemetrySink for TracingTelemetrySink {
    fn enabled(&self, _kind: TelemetrySpanKind) -> bool {
        tracing::enabled!(target: "lix_sql", tracing::Level::INFO)
    }

    fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
        let span = tracing_span(&start);
        Box::new(TracingTelemetrySpan { span })
    }
}

struct TracingTelemetrySpan {
    span: tracing::Span,
}

struct TracingTelemetrySpanEnterGuard<'a>(#[allow(dead_code)] tracing::span::Entered<'a>);

impl TelemetrySpanEnterGuard for TracingTelemetrySpanEnterGuard<'_> {}

impl TelemetrySpanHandle for TracingTelemetrySpan {
    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
        Box::new(TracingTelemetrySpanEnterGuard(self.span.enter()))
    }

    fn finish(self: Box<Self>, end: TelemetrySpanEnd) {
        for attribute in &end.attributes {
            record_attribute(&self.span, attribute);
        }
        drop(self);
    }
}

fn tracing_span(start: &TelemetrySpanStart) -> tracing::Span {
    let span = match start.kind {
        TelemetrySpanKind::SqlQuery => tracing::info_span!(
            target: "lix_sql",
            "lix.sql.query",
            "otel.name" = tracing::field::Empty,
            "otel.kind" = tracing::field::Empty,
            "db.system.name" = tracing::field::Empty,
            "db.operation.name" = tracing::field::Empty,
            "db.query.summary" = tracing::field::Empty,
            "db.query.text" = tracing::field::Empty,
            "lix.sql.fingerprint" = tracing::field::Empty,
            "lix.execution.kind" = tracing::field::Empty,
            "lix.batch.index" = tracing::field::Empty,
            "db.response.returned_rows" = tracing::field::Empty,
            "lix.rows_affected" = tracing::field::Empty,
            "error.type" = tracing::field::Empty,
            "otel.status_code" = tracing::field::Empty,
        ),
        TelemetrySpanKind::SqlBatch => tracing::info_span!(
            target: "lix_sql",
            "lix.sql.batch",
            "otel.name" = tracing::field::Empty,
            "otel.kind" = tracing::field::Empty,
            "db.system.name" = tracing::field::Empty,
            "db.operation.batch.size" = tracing::field::Empty,
            "lix.execution.kind" = tracing::field::Empty,
            "error.type" = tracing::field::Empty,
            "otel.status_code" = tracing::field::Empty,
        ),
        TelemetrySpanKind::SqlCoherentReadBatch => tracing::info_span!(
            target: "lix_sql",
            "lix.sql.coherent_read_batch",
            "otel.name" = tracing::field::Empty,
            "otel.kind" = tracing::field::Empty,
            "db.system.name" = tracing::field::Empty,
            "db.operation.batch.size" = tracing::field::Empty,
            "lix.execution.kind" = tracing::field::Empty,
            "error.type" = tracing::field::Empty,
            "otel.status_code" = tracing::field::Empty,
        ),
    };
    for attribute in &start.attributes {
        record_attribute(&span, attribute);
    }
    span
}

fn record_attribute(span: &tracing::Span, attribute: &TelemetryAttribute) {
    match &attribute.value {
        TelemetryValue::String(value) => span.record(attribute.key, value.as_str()),
        TelemetryValue::U64(value) => span.record(attribute.key, *value),
        TelemetryValue::Boolean(value) => span.record(attribute.key, *value),
    };
}

pub(crate) struct ActiveTelemetrySpan {
    handle: Box<dyn TelemetrySpanHandle>,
    started: web_time::Instant,
}

impl ActiveTelemetrySpan {
    pub(crate) fn start(sink: &Arc<dyn TelemetrySink>, start: TelemetrySpanStart) -> Self {
        Self {
            handle: sink.start_span(start),
            started: web_time::Instant::now(),
        }
    }

    pub(crate) async fn instrument<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        TelemetryInstrumentedFuture {
            future: Box::pin(future),
            handle: self.handle.as_ref(),
        }
        .await
    }

    pub(crate) fn finish(self, status: TelemetrySpanStatus, attributes: Vec<TelemetryAttribute>) {
        let duration_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.handle.finish(TelemetrySpanEnd {
            duration_ns,
            status,
            attributes,
        });
    }
}

struct TelemetryInstrumentedFuture<'a, F> {
    future: Pin<Box<F>>,
    handle: &'a dyn TelemetrySpanHandle,
}

impl<F> Future for TelemetryInstrumentedFuture<'_, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let _entered = self.handle.enter();
        self.future.as_mut().poll(context)
    }
}

pub(crate) fn unix_time_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}
