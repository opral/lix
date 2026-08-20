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
    /// A client session bound to a Lix. Not engine construction or cache-admit.
    LixOpened,
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
///
/// Production subscribers export INFO `lix_sql` (SQL batch / query) and INFO
/// `lix` (`lix.opened`). Commit, storage, notify, checkpoint, and session /
/// engine-open phases that can take tens of milliseconds use those same
/// targets at INFO so they appear in the same tree. Debug-only `lix_perf`
/// micro-phases stay off the production plane.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingTelemetrySink;

impl TracingTelemetrySink {
    pub fn new() -> Self {
        Self
    }
}

impl TelemetrySink for TracingTelemetrySink {
    fn enabled(&self, kind: TelemetrySpanKind) -> bool {
        match kind {
            TelemetrySpanKind::SqlQuery
            | TelemetrySpanKind::SqlBatch
            | TelemetrySpanKind::SqlCoherentReadBatch => {
                tracing::enabled!(target: "lix_sql", tracing::Level::INFO)
            }
            TelemetrySpanKind::LixOpened => {
                tracing::enabled!(target: "lix", tracing::Level::INFO)
            }
        }
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
        TelemetrySpanKind::LixOpened => tracing::info_span!(
            target: "lix",
            "lix.opened",
            "lix.id" = tracing::field::Empty,
            "lix.branch_id" = tracing::field::Empty,
            "lix.account_id" = tracing::field::Empty,
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

    pub(crate) fn instrument<F>(&self, future: F) -> TelemetryInstrumentedFuture<'_, F>
    where
        F: Future,
    {
        TelemetryInstrumentedFuture {
            future: Box::pin(future),
            handle: self.handle.as_ref(),
        }
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

pub(crate) struct TelemetryInstrumentedFuture<'a, F> {
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

/// Records that a client session has bound to a Lix.
///
/// This is the only place that starts a [`TelemetrySpanKind::LixOpened`] span.
/// Handshake session creation and in-process [`crate::open_lix`] call it.
/// Protocol roots opened with [`crate::OpenLixBuilder::as_protocol_root`]
/// skip it so a cached runtime can inherit a sink without emitting.
/// Hosts that mint a session against an already-open runtime (MCP signed
/// context, cache hit) should call the same helper instead of opening another
/// engine.
///
/// No-op when `sink` is absent or `enabled(LixOpened)` is false. Those paths
/// do not build attributes or timestamps.
pub fn bind_session(
    sink: Option<&Arc<dyn TelemetrySink>>,
    lix_id: &str,
    branch_id: &str,
    account_id: Option<&str>,
) {
    let Some(sink) = sink else {
        return;
    };
    if !sink.enabled(TelemetrySpanKind::LixOpened) {
        return;
    }
    let mut attributes = vec![
        TelemetryAttribute::string("lix.id", lix_id),
        TelemetryAttribute::string("lix.branch_id", branch_id),
    ];
    if let Some(account_id) = account_id.filter(|id| !id.is_empty()) {
        attributes.push(TelemetryAttribute::string("lix.account_id", account_id));
    }
    let span = ActiveTelemetrySpan::start(
        sink,
        TelemetrySpanStart {
            kind: TelemetrySpanKind::LixOpened,
            name: "lix.opened",
            started_at_unix_ms: unix_time_ms(),
            attributes,
        },
    );
    span.finish(TelemetrySpanStatus::Ok, Vec::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct RecordingSink {
        opened_enabled: bool,
        sql_enabled: bool,
        started: Mutex<Vec<TelemetrySpanStart>>,
        enabled_kinds: Mutex<Vec<TelemetrySpanKind>>,
    }

    impl RecordingSink {
        fn new(opened_enabled: bool) -> Self {
            Self {
                opened_enabled,
                sql_enabled: true,
                started: Mutex::new(Vec::new()),
                enabled_kinds: Mutex::new(Vec::new()),
            }
        }

        fn into_sink(self: Arc<Self>) -> Arc<dyn TelemetrySink> {
            self
        }
    }

    impl TelemetrySink for RecordingSink {
        fn enabled(&self, kind: TelemetrySpanKind) -> bool {
            self.enabled_kinds.lock().expect("enabled kinds").push(kind);
            match kind {
                TelemetrySpanKind::LixOpened => self.opened_enabled,
                TelemetrySpanKind::SqlQuery
                | TelemetrySpanKind::SqlBatch
                | TelemetrySpanKind::SqlCoherentReadBatch => self.sql_enabled,
            }
        }

        fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
            self.started.lock().expect("started spans").push(start);
            Box::new(RecordingHandle)
        }
    }

    struct RecordingHandle;

    impl TelemetrySpanHandle for RecordingHandle {
        fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
            Box::new(())
        }

        fn finish(self: Box<Self>, _end: TelemetrySpanEnd) {}
    }

    fn attribute_string<'a>(start: &'a TelemetrySpanStart, key: &str) -> Option<&'a str> {
        start.attributes.iter().find_map(|attribute| {
            if attribute.key == key {
                match &attribute.value {
                    TelemetryValue::String(value) => Some(value.as_str()),
                    _ => None,
                }
            } else {
                None
            }
        })
    }

    #[test]
    fn absent_sink_does_no_opened_span_work() {
        bind_session(None, "lix-id", "branch-id", Some("account-id"));
    }

    #[test]
    fn disabled_opened_kind_does_not_start_a_span() {
        let sink = Arc::new(RecordingSink::new(false));
        let trait_sink = Arc::clone(&sink).into_sink();
        bind_session(Some(&trait_sink), "lix-id", "branch-id", Some("account-id"));
        assert_eq!(
            *sink.enabled_kinds.lock().expect("enabled kinds"),
            [TelemetrySpanKind::LixOpened]
        );
        assert!(sink.started.lock().expect("started spans").is_empty());
    }

    #[test]
    fn bind_session_emits_vendor_neutral_opened_attributes() {
        let sink = Arc::new(RecordingSink::new(true));
        let trait_sink = Arc::clone(&sink).into_sink();
        bind_session(Some(&trait_sink), "lix-id", "branch-id", Some("account-id"));
        let started = sink.started.lock().expect("started spans").clone();
        assert_eq!(started.len(), 1);
        assert_eq!(started[0].kind, TelemetrySpanKind::LixOpened);
        assert_eq!(started[0].name, "lix.opened");
        assert_eq!(attribute_string(&started[0], "lix.id"), Some("lix-id"));
        assert_eq!(
            attribute_string(&started[0], "lix.branch_id"),
            Some("branch-id")
        );
        assert_eq!(
            attribute_string(&started[0], "lix.account_id"),
            Some("account-id")
        );
        assert!(
            started[0]
                .attributes
                .iter()
                .all(|attribute| attribute.key.starts_with("lix."))
        );
    }

    #[test]
    fn bind_session_omits_account_when_absent() {
        let sink = Arc::new(RecordingSink::new(true));
        let trait_sink = Arc::clone(&sink).into_sink();
        bind_session(Some(&trait_sink), "lix-id", "branch-id", None);
        let started = sink.started.lock().expect("started spans").clone();
        assert_eq!(started.len(), 1);
        assert!(attribute_string(&started[0], "lix.account_id").is_none());
    }
}
