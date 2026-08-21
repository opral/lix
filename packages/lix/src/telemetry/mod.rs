use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::task::{Context, Poll};

pub(crate) mod spans;

/// A stable, coarse category that lets sinks filter telemetry without making
/// every Lix operation a public Rust enum variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySpanClass {
    Sql,
    Lifecycle,
    Performance,
}

/// Opaque production-span identity. Declared once by the internal descriptor
/// macro: stable name, coarse class, allowed attributes, and the tracing
/// callsite used by [`TracingTelemetrySink`].
#[derive(Clone, Copy)]
pub struct TelemetrySpanDescriptor {
    pub(crate) name: &'static str,
    pub(crate) class: TelemetrySpanClass,
    pub(crate) allowed_attributes: &'static [&'static str],
    pub(crate) create_tracing_span: fn() -> tracing::Span,
}

impl std::fmt::Debug for TelemetrySpanDescriptor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetrySpanDescriptor")
            .field("name", &self.name)
            .field("class", &self.class)
            .finish()
    }
}

impl PartialEq for TelemetrySpanDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for TelemetrySpanDescriptor {}

impl TelemetrySpanDescriptor {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn class(&self) -> TelemetrySpanClass {
        self.class
    }

    fn allows(&self, key: &str) -> bool {
        self.allowed_attributes.contains(&key)
    }
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

/// A causal link from one physical operation to a logical transaction span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanLink {
    pub trace_id: String,
    pub span_id: String,
}

/// Information available when an engine operation begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanStart {
    pub class: TelemetrySpanClass,
    pub name: &'static str,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub links: Vec<TelemetrySpanLink>,
    pub started_at_unix_ms: u64,
    pub attributes: Vec<TelemetryAttribute>,
    descriptor: &'static TelemetrySpanDescriptor,
}

impl TelemetrySpanStart {
    pub fn new(
        descriptor: &'static TelemetrySpanDescriptor,
        attributes: Vec<TelemetryAttribute>,
    ) -> Self {
        debug_assert_attributes(descriptor, &attributes);
        Self {
            class: descriptor.class,
            name: descriptor.name,
            trace_id: String::new(),
            span_id: String::new(),
            parent_span_id: None,
            links: Vec::new(),
            started_at_unix_ms: unix_time_ms(),
            attributes,
            descriptor,
        }
    }

    pub fn descriptor(&self) -> &'static TelemetrySpanDescriptor {
        self.descriptor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetrySpanStatus {
    Ok,
    Error,
    Cancelled,
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
    /// Called before Lix builds attributes. The descriptor is the only source
    /// of the stable production name.
    fn enabled(&self, _descriptor: &TelemetrySpanDescriptor) -> bool {
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
/// This sink is not a second source of names. It opens the callsite owned by
/// the descriptor and records the same ids the callback plane receives.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingTelemetrySink;

impl TracingTelemetrySink {
    pub fn new() -> Self {
        Self
    }
}

impl TelemetrySink for TracingTelemetrySink {
    fn enabled(&self, descriptor: &TelemetrySpanDescriptor) -> bool {
        match descriptor.class {
            TelemetrySpanClass::Sql | TelemetrySpanClass::Performance => {
                tracing::enabled!(target: "lix_sql", tracing::Level::INFO)
            }
            TelemetrySpanClass::Lifecycle => {
                tracing::enabled!(target: "lix", tracing::Level::INFO)
            }
        }
    }

    fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
        let span = (start.descriptor.create_tracing_span)();
        span.record("lix.trace_id", start.trace_id.as_str());
        span.record("lix.span_id", start.span_id.as_str());
        if let Some(parent_span_id) = start.parent_span_id.as_deref() {
            span.record("lix.parent_span_id", parent_span_id);
        }
        if !start.links.is_empty() {
            let links = encode_links(&start.links);
            span.record("lix.span.links", links.as_str());
        }
        for attribute in &start.attributes {
            record_attribute(&span, attribute);
        }
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
        self.span.record(
            "otel.status_code",
            match end.status {
                TelemetrySpanStatus::Ok => "OK",
                TelemetrySpanStatus::Error | TelemetrySpanStatus::Cancelled => "ERROR",
            },
        );
        if end.status == TelemetrySpanStatus::Cancelled {
            self.span.record("error.type", "cancelled");
        }
        drop(self);
    }
}

fn encode_links(links: &[TelemetrySpanLink]) -> String {
    links
        .iter()
        .map(|link| format!("{}:{}", link.trace_id, link.span_id))
        .collect::<Vec<_>>()
        .join(",")
}

fn record_attribute(span: &tracing::Span, attribute: &TelemetryAttribute) {
    match &attribute.value {
        TelemetryValue::String(value) => span.record(attribute.key, value.as_str()),
        TelemetryValue::U64(value) => span.record(attribute.key, *value),
        TelemetryValue::Boolean(value) => span.record(attribute.key, *value),
    };
}

fn debug_assert_attributes(descriptor: &TelemetrySpanDescriptor, attributes: &[TelemetryAttribute]) {
    debug_assert!(
        attributes
            .iter()
            .all(|attribute| descriptor.allows(attribute.key)),
        "attribute not allowed on {}: {:?}",
        descriptor.name,
        attributes
            .iter()
            .filter(|attribute| !descriptor.allows(attribute.key))
            .map(|attribute| attribute.key)
            .collect::<Vec<_>>()
    );
}

#[derive(Clone)]
pub(crate) struct TelemetryContext {
    sink: Arc<dyn TelemetrySink>,
    trace_id: String,
    span_id: String,
    commit_cohort_id: Option<String>,
    links: Vec<TelemetrySpanLink>,
}

thread_local! {
    static CURRENT_TELEMETRY: RefCell<Vec<TelemetryContext>> = const { RefCell::new(Vec::new()) };
}

static NEXT_TELEMETRY_ID: AtomicU64 = AtomicU64::new(1);

fn next_span_id() -> String {
    format!("{:016x}", NEXT_TELEMETRY_ID.fetch_add(1, Ordering::Relaxed))
}

pub(crate) fn next_commit_cohort_id() -> String {
    next_span_id()
}

fn next_trace_id() -> String {
    format!("{:016x}{}", unix_time_ms(), next_span_id())
}

fn current_context_for(sink: &Arc<dyn TelemetrySink>) -> Option<TelemetryContext> {
    CURRENT_TELEMETRY.with(|current| {
        current
            .borrow()
            .last()
            .filter(|context| Arc::ptr_eq(&context.sink, sink))
            .cloned()
    })
}

pub(crate) fn current_telemetry_context() -> Option<TelemetryContext> {
    CURRENT_TELEMETRY.with(|current| current.borrow().last().cloned())
}

pub(crate) fn current_commit_cohort_id() -> Option<String> {
    current_telemetry_context().and_then(|context| context.commit_cohort_id)
}

struct CurrentTelemetryGuard;

impl Drop for CurrentTelemetryGuard {
    fn drop(&mut self) {
        CURRENT_TELEMETRY.with(|current| {
            current.borrow_mut().pop();
        });
    }
}

impl TelemetryContext {
    pub(crate) fn root(sink: Arc<dyn TelemetrySink>) -> Self {
        Self {
            sink,
            trace_id: next_trace_id(),
            span_id: String::new(),
            commit_cohort_id: None,
            links: Vec::new(),
        }
    }

    pub(crate) fn with_commit_cohort_id(mut self, commit_cohort_id: String) -> Self {
        self.commit_cohort_id = Some(commit_cohort_id);
        self
    }

    pub(crate) fn with_links(mut self, links: Vec<TelemetrySpanLink>) -> Self {
        self.links = links;
        self
    }

    pub(crate) fn as_link(&self) -> Option<TelemetrySpanLink> {
        (!self.span_id.is_empty()).then(|| TelemetrySpanLink {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(sink: Arc<dyn TelemetrySink>, trace_id: &str, span_id: &str) -> Self {
        Self {
            sink,
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            commit_cohort_id: None,
            links: Vec::new(),
        }
    }

    fn enter(&self) -> CurrentTelemetryGuard {
        CURRENT_TELEMETRY.with(|current| current.borrow_mut().push(self.clone()));
        CurrentTelemetryGuard
    }

    pub(crate) fn instrument<F>(&self, future: F) -> TelemetryContextFuture<'_, F>
    where
        F: Future,
    {
        TelemetryContextFuture {
            future: Box::pin(future),
            context: self,
        }
    }
}

pub(crate) struct TelemetryContextFuture<'a, F> {
    future: Pin<Box<F>>,
    context: &'a TelemetryContext,
}

impl<F: Future> Future for TelemetryContextFuture<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, task: &mut Context<'_>) -> Poll<Self::Output> {
        let _entered = self.context.enter();
        self.future.as_mut().poll(task)
    }
}

pub(crate) struct ActiveTelemetrySpan {
    handle: Option<Box<dyn TelemetrySpanHandle>>,
    started: web_time::Instant,
    context: TelemetryContext,
    descriptor: &'static TelemetrySpanDescriptor,
}

impl ActiveTelemetrySpan {
    pub(crate) fn start(sink: &Arc<dyn TelemetrySink>, mut start: TelemetrySpanStart) -> Self {
        let parent = current_context_for(sink);
        if start.class == TelemetrySpanClass::Performance {
            if !start
                .attributes
                .iter()
                .any(|attribute| attribute.key == "lix.commit_cohort_id")
                && let Some(commit_cohort_id) = parent
                    .as_ref()
                    .and_then(|parent| parent.commit_cohort_id.as_deref())
            {
                start.attributes.push(TelemetryAttribute::string(
                    "lix.commit_cohort_id",
                    commit_cohort_id,
                ));
            }
            if start.links.is_empty() {
                if let Some(parent) = parent.as_ref() {
                    if !parent.links.is_empty() {
                        start.links = parent.links.clone();
                    } else if let Some(link) = parent.as_link() {
                        start.links.push(link);
                    }
                }
            }
        }
        start.trace_id = parent
            .as_ref()
            .map_or_else(next_trace_id, |parent| parent.trace_id.clone());
        start.span_id = next_span_id();
        start.parent_span_id = parent
            .as_ref()
            .filter(|parent| !parent.span_id.is_empty())
            .map(|parent| parent.span_id.clone());
        let context = TelemetryContext {
            sink: Arc::clone(sink),
            trace_id: start.trace_id.clone(),
            span_id: start.span_id.clone(),
            commit_cohort_id: parent
                .as_ref()
                .and_then(|parent| parent.commit_cohort_id.clone()),
            links: start.links.clone(),
        };
        debug_assert_attributes(start.descriptor, &start.attributes);
        let descriptor = start.descriptor;
        Self {
            handle: Some(sink.start_span(start)),
            started: web_time::Instant::now(),
            context,
            descriptor,
        }
    }

    pub(crate) fn start_if_enabled(
        sink: &Arc<dyn TelemetrySink>,
        descriptor: &'static TelemetrySpanDescriptor,
        attributes: Vec<TelemetryAttribute>,
    ) -> Option<Self> {
        sink.enabled(descriptor)
            .then(|| Self::start(sink, TelemetrySpanStart::new(descriptor, attributes)))
    }

    pub(crate) fn start_current(
        descriptor: &'static TelemetrySpanDescriptor,
        attributes: Vec<TelemetryAttribute>,
    ) -> Option<Self> {
        let context = current_telemetry_context()?;
        Self::start_if_enabled(&context.sink, descriptor, attributes)
    }

    pub(crate) fn instrument<F>(&self, future: F) -> TelemetryInstrumentedFuture<'_, F>
    where
        F: Future,
    {
        TelemetryInstrumentedFuture {
            future: Box::pin(future),
            span: self,
        }
    }

    pub(crate) fn finish(
        mut self,
        status: TelemetrySpanStatus,
        attributes: Vec<TelemetryAttribute>,
    ) {
        debug_assert_attributes(self.descriptor, &attributes);
        let duration_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if let Some(handle) = self.handle.take() {
            handle.finish(TelemetrySpanEnd {
                duration_ns,
                status,
                attributes,
            });
        }
    }

    pub(crate) fn enter(&self) -> ActiveTelemetryEnterGuard<'_> {
        let current = self.context.enter();
        let sink = self
            .handle
            .as_ref()
            .expect("active telemetry span has a handle")
            .enter();
        ActiveTelemetryEnterGuard {
            _sink: sink,
            _current: current,
        }
    }
}

/// Instruments `future` without growing the caller's async state machine.
///
/// An `async fn` wrapper would store `F` in every caller. Combined with
/// `tokio`/`futures_lite` `block_on` pinning that future on a 2 MiB worker
/// or coordinator thread, engine-open and commit phases overflowed
/// `deterministic_replica_scenarios`. Boxing here keeps the wrapper
/// pointer-sized whether or not a span is active.
pub(crate) fn instrument_lix_result<T, F>(
    span: Option<ActiveTelemetrySpan>,
    future: F,
) -> InstrumentLixResult<F>
where
    F: Future<Output = Result<T, crate::LixError>>,
{
    InstrumentLixResult {
        future: Box::pin(future),
        span,
    }
}

pub(crate) fn instrument_value<T, F>(
    span: Option<ActiveTelemetrySpan>,
    future: F,
) -> InstrumentValue<F>
where
    F: Future<Output = T>,
{
    InstrumentValue {
        future: Box::pin(future),
        span,
    }
}

pub(crate) struct InstrumentLixResult<F> {
    future: Pin<Box<F>>,
    span: Option<ActiveTelemetrySpan>,
}

impl<T, F> Future for InstrumentLixResult<F>
where
    F: Future<Output = Result<T, crate::LixError>>,
{
    type Output = Result<T, crate::LixError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let entered = this.span.as_ref().map(ActiveTelemetrySpan::enter);
        let result = this.future.as_mut().poll(context);
        drop(entered);
        match result {
            Poll::Ready(result) => {
                if let Some(span) = this.span.take() {
                    match &result {
                        Ok(_) => span.finish(TelemetrySpanStatus::Ok, Vec::new()),
                        Err(error) => span.finish(
                            TelemetrySpanStatus::Error,
                            vec![TelemetryAttribute::string("error.type", error.code.clone())],
                        ),
                    }
                }
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(crate) struct InstrumentValue<F> {
    future: Pin<Box<F>>,
    span: Option<ActiveTelemetrySpan>,
}

impl<T, F> Future for InstrumentValue<F>
where
    F: Future<Output = T>,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let entered = this.span.as_ref().map(ActiveTelemetrySpan::enter);
        let result = this.future.as_mut().poll(context);
        drop(entered);
        match result {
            Poll::Ready(value) => {
                if let Some(span) = this.span.take() {
                    span.finish(TelemetrySpanStatus::Ok, Vec::new());
                }
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ActiveTelemetrySpan {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let duration_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        handle.finish(TelemetrySpanEnd {
            duration_ns,
            status: TelemetrySpanStatus::Cancelled,
            attributes: Vec::new(),
        });
    }
}

pub(crate) struct ActiveTelemetryEnterGuard<'a> {
    _sink: Box<dyn TelemetrySpanEnterGuard + 'a>,
    _current: CurrentTelemetryGuard,
}

pub(crate) struct TelemetryInstrumentedFuture<'a, F> {
    future: Pin<Box<F>>,
    span: &'a ActiveTelemetrySpan,
}

impl<F> Future for TelemetryInstrumentedFuture<'_, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let _entered = self.span.enter();
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
/// This is the only place that emits `lix.repository.opened`.
/// Handshake session creation and in-process [`crate::open_lix`] call it.
/// Protocol roots opened with [`crate::OpenLixBuilder::as_protocol_root`]
/// skip it so a cached runtime can inherit a sink without emitting.
/// Hosts that mint a session against an already-open runtime (MCP signed
/// context, cache hit) should call the same helper instead of opening another
/// engine.
///
/// No-op when `sink` is absent or lifecycle telemetry is disabled. Those paths
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
    if !sink.enabled(&spans::REPOSITORY_OPENED) {
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
        TelemetrySpanStart::new(&spans::REPOSITORY_OPENED, attributes),
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
        enabled_names: Mutex<Vec<&'static str>>,
    }

    impl RecordingSink {
        fn new(opened_enabled: bool) -> Self {
            Self {
                opened_enabled,
                sql_enabled: true,
                started: Mutex::new(Vec::new()),
                enabled_names: Mutex::new(Vec::new()),
            }
        }

        fn into_sink(self: Arc<Self>) -> Arc<dyn TelemetrySink> {
            self
        }
    }

    impl TelemetrySink for RecordingSink {
        fn enabled(&self, descriptor: &TelemetrySpanDescriptor) -> bool {
            self.enabled_names
                .lock()
                .expect("enabled names")
                .push(descriptor.name());
            match descriptor.class() {
                TelemetrySpanClass::Lifecycle => self.opened_enabled,
                TelemetrySpanClass::Sql | TelemetrySpanClass::Performance => self.sql_enabled,
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
    fn production_contract_has_eleven_names_and_no_legacy_aliases() {
        assert_eq!(spans::PRODUCTION_NAMES.len(), 11);
        assert_eq!(spans::ALL.len(), 11);
        let mut names = spans::PRODUCTION_NAMES.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 11);
        for forbidden in spans::FORBIDDEN_PRODUCTION_NAMES {
            assert!(!spans::PRODUCTION_NAMES.contains(forbidden));
        }
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
            *sink.enabled_names.lock().expect("enabled names"),
            ["lix.repository.opened"]
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
        assert_eq!(started[0].class, TelemetrySpanClass::Lifecycle);
        assert_eq!(started[0].name, "lix.repository.opened");
        assert_eq!(started[0].descriptor(), &spans::REPOSITORY_OPENED);
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

    #[test]
    fn dropped_span_finishes_as_cancelled() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        drop(ActiveTelemetrySpan::start(
            &sink,
            TelemetrySpanStart::new(&spans::ENGINE_OPEN, Vec::new()),
        ));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start.name, "lix.engine.open");
        assert_eq!(completed[0].end.status, TelemetrySpanStatus::Cancelled);
    }

    #[test]
    fn dropped_instrumented_future_finishes_as_cancelled() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        let span = ActiveTelemetrySpan::start(
            &sink,
            TelemetrySpanStart::new(&spans::SESSION_OPEN, Vec::new()),
        );
        drop(instrument_lix_result(
            Some(span),
            std::future::pending::<Result<(), crate::LixError>>(),
        ));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].end.status, TelemetrySpanStatus::Cancelled);
    }

    #[test]
    fn performance_spans_inherit_cohort_id_and_links() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        let links = vec![
            TelemetrySpanLink {
                trace_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                span_id: "1111111111111111".to_string(),
            },
            TelemetrySpanLink {
                trace_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                span_id: "2222222222222222".to_string(),
            },
        ];
        let context = TelemetryContext::root(Arc::clone(&sink))
            .with_commit_cohort_id("cohort-1".to_string())
            .with_links(links.clone());
        futures_lite::future::block_on(context.instrument(async {
            let span = ActiveTelemetrySpan::start_current(
                &spans::TRANSACTION_MATERIALIZE,
                vec![TelemetryAttribute::u64("lix.transaction.count", 2)],
            )
            .expect("materialize enabled");
            span.finish(TelemetrySpanStatus::Ok, Vec::new());
        }));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start.name, "lix.transaction.materialize");
        assert_eq!(completed[0].start.links, links);
        assert_eq!(
            attribute_string(&completed[0].start, "lix.commit_cohort_id"),
            Some("cohort-1")
        );
    }
}
