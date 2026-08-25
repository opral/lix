use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

pub use opentelemetry::trace::{SpanContext, SpanKind, Status};
use opentelemetry::trace::{TraceContextExt as _, TraceFlags, TraceState};
use opentelemetry::Context as OpenTelemetryContext;
use opentelemetry_sdk::trace::{IdGenerator as _, RandomIdGenerator};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

mod spans;
pub(crate) use spans::{
    CHECKPOINT_CREATE, ENGINE_OPEN, REPOSITORY_OPENED, SESSION_OPEN, SQL_BATCH,
    SQL_COHERENT_READ_BATCH, SQL_QUERY, TRANSACTION_MATERIALIZE, TRANSACTION_NOTIFY,
    TRANSACTION_STORAGE, TRANSACTION_WAIT,
};
#[cfg(test)]
pub(crate) use spans::{FORBIDDEN_PRODUCTION_NAMES, PRODUCTION_NAMES};

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
/// callsite used by [`OpenTelemetryTracingSink`].
#[derive(Clone)]
pub struct TelemetrySpanDescriptor {
    pub(crate) name: &'static str,
    pub(crate) class: TelemetrySpanClass,
    pub(crate) kind: SpanKind,
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

    pub fn kind(&self) -> SpanKind {
        self.kind.clone()
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

    pub fn i64(key: &'static str, value: i64) -> Self {
        Self {
            key,
            value: TelemetryValue::I64(value),
        }
    }

    pub fn boolean(key: &'static str, value: bool) -> Self {
        Self {
            key,
            value: TelemetryValue::Boolean(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TelemetryValue {
    String(String),
    I64(i64),
    Boolean(bool),
}

/// Information available when an engine operation begins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanStart {
    pub class: TelemetrySpanClass,
    pub name: &'static str,
    pub kind: SpanKind,
    /// The authoritative OpenTelemetry parent, including sampling flags and
    /// trace state. Sinks must use this as the actual parent, not as attributes.
    pub parent_span_context: Option<SpanContext>,
    /// OpenTelemetry span links for causal parents that are not the tree parent.
    pub links: Vec<SpanContext>,
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
            kind: descriptor.kind.clone(),
            parent_span_context: None,
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

/// Information available when an engine operation finishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetrySpanEnd {
    pub duration_ns: u64,
    pub status: Status,
    pub attributes: Vec<TelemetryAttribute>,
}

/// A completed span used by callback and cross-runtime adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTelemetrySpan {
    /// The context assigned by the sink that created the span.
    pub span_context: SpanContext,
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
    /// Returns the authoritative context assigned by this sink. It must be
    /// valid according to the OpenTelemetry trace specification.
    fn span_context(&self) -> &SpanContext;
    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_>;
    fn finish(self: Box<Self>, end: TelemetrySpanEnd);
}

pub trait TelemetrySpanEnterGuard {}

impl TelemetrySpanEnterGuard for () {}

/// Sink adapter for hosts that consume completed spans through a callback.
#[expect(missing_debug_implementations)]
pub struct CallbackTelemetrySink {
    callback: Arc<dyn Fn(CompletedTelemetrySpan) + Send + Sync>,
    root_parent: Option<SpanContext>,
}

impl CallbackTelemetrySink {
    pub fn new(callback: impl Fn(CompletedTelemetrySpan) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
            root_parent: None,
        }
    }

    /// Uses `parent` as the remote parent of every otherwise-root span.
    /// This is the explicit propagation boundary for hosts whose active
    /// OpenTelemetry context cannot cross the Rust FFI boundary implicitly.
    pub fn with_root_parent(mut self, parent: SpanContext) -> Self {
        assert!(parent.is_valid(), "telemetry root parent must be valid");
        self.root_parent = Some(parent);
        self
    }
}

impl TelemetrySink for CallbackTelemetrySink {
    fn start_span(&self, mut start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
        if start.parent_span_context.is_none() {
            start.parent_span_context = self.root_parent.clone();
        }
        let span_context = new_span_context(start.parent_span_context.as_ref());
        let recording = span_context.is_sampled();
        Box::new(CallbackTelemetrySpan {
            callback: Arc::clone(&self.callback),
            span_context,
            recording,
            start,
        })
    }
}

struct CallbackTelemetrySpan {
    callback: Arc<dyn Fn(CompletedTelemetrySpan) + Send + Sync>,
    span_context: SpanContext,
    recording: bool,
    start: TelemetrySpanStart,
}

impl TelemetrySpanHandle for CallbackTelemetrySpan {
    fn span_context(&self) -> &SpanContext {
        &self.span_context
    }

    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
        Box::new(())
    }

    fn finish(self: Box<Self>, end: TelemetrySpanEnd) {
        if !self.recording {
            return;
        }
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.callback)(CompletedTelemetrySpan {
                span_context: self.span_context,
                start: self.start,
                end,
            });
        }));
    }
}

pub(crate) fn new_span_context(parent: Option<&SpanContext>) -> SpanContext {
    let generator = RandomIdGenerator::default();
    let trace_id = parent
        .filter(|parent| parent.is_valid())
        .map_or_else(|| generator.new_trace_id(), SpanContext::trace_id);
    let trace_flags = parent.map_or(TraceFlags::SAMPLED, SpanContext::trace_flags);
    let trace_state = parent
        .map(SpanContext::trace_state)
        .cloned()
        .unwrap_or(TraceState::NONE);
    SpanContext::new(
        trace_id,
        generator.new_span_id(),
        trace_flags,
        false,
        trace_state,
    )
}

/// OpenTelemetry-compliant adapter from engine telemetry into `tracing`.
///
/// The active subscriber must include `tracing-opentelemetry`. This adapter
/// sets real OpenTelemetry parents, links, attributes, and status, and returns
/// the context assigned by that layer. It never creates shadow identifiers.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenTelemetryTracingSink;

impl OpenTelemetryTracingSink {
    pub fn new() -> Self {
        Self
    }
}

impl TelemetrySink for OpenTelemetryTracingSink {
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
        if let Some(parent) = start.parent_span_context.as_ref() {
            span.set_parent(
                OpenTelemetryContext::new().with_remote_span_context(parent.clone()),
            )
            .expect("tracing-opentelemetry rejected the Lix span parent");
        }
        for link in &start.links {
            span.add_link(link.clone());
        }
        for attribute in &start.attributes {
            record_attribute(&span, attribute);
        }
        let span_context = span.context().span().span_context().clone();
        assert!(
            span_context.is_valid(),
            "OpenTelemetryTracingSink requires an active tracing-opentelemetry layer"
        );
        Box::new(OpenTelemetryTracingSpan { span, span_context })
    }
}

struct OpenTelemetryTracingSpan {
    span: tracing::Span,
    span_context: SpanContext,
}

struct OpenTelemetryTracingSpanEnterGuard<'a>(#[allow(dead_code)] tracing::span::Entered<'a>);

impl TelemetrySpanEnterGuard for OpenTelemetryTracingSpanEnterGuard<'_> {}

impl TelemetrySpanHandle for OpenTelemetryTracingSpan {
    fn span_context(&self) -> &SpanContext {
        &self.span_context
    }

    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
        Box::new(OpenTelemetryTracingSpanEnterGuard(self.span.enter()))
    }

    fn finish(self: Box<Self>, end: TelemetrySpanEnd) {
        for attribute in &end.attributes {
            record_attribute(&self.span, attribute);
        }
        self.span.set_status(end.status.clone());
        drop(self);
    }
}

fn record_attribute(span: &tracing::Span, attribute: &TelemetryAttribute) {
    match &attribute.value {
        TelemetryValue::String(value) => span.record(attribute.key, value.as_str()),
        TelemetryValue::I64(value) => span.record(attribute.key, *value),
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
    span_context: SpanContext,
    commit_cohort_id: Option<String>,
    links: Vec<SpanContext>,
}

thread_local! {
    static CURRENT_TELEMETRY: RefCell<Vec<TelemetryContext>> = const { RefCell::new(Vec::new()) };
    static CURRENT_REMOTE_PARENT: RefCell<Vec<SpanContext>> = const { RefCell::new(Vec::new()) };
}

struct CurrentRemoteParentGuard;

impl Drop for CurrentRemoteParentGuard {
    fn drop(&mut self) {
        CURRENT_REMOTE_PARENT.with(|current| {
            current.borrow_mut().pop();
        });
    }
}

fn enter_remote_parent(parent: &SpanContext) -> CurrentRemoteParentGuard {
    CURRENT_REMOTE_PARENT.with(|current| current.borrow_mut().push(parent.clone()));
    CurrentRemoteParentGuard
}

/// Scopes an FFI-propagated remote parent to polls of one operation future.
/// Unrelated tasks polled by the same executor do not observe this context.
pub fn instrument_remote_parent<F>(
    parent: Option<SpanContext>,
    future: F,
) -> impl Future<Output = F::Output>
where
    F: Future,
{
    RemoteParentFuture {
        future: Box::pin(future),
        parent,
    }
}

struct RemoteParentFuture<F> {
    future: Pin<Box<F>>,
    parent: Option<SpanContext>,
}

impl<F: Future> Future for RemoteParentFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, task: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let _entered = self.parent.as_ref().map(enter_remote_parent);
        self.future.as_mut().poll(task)
    }
}

pub(crate) fn next_commit_cohort_id() -> String {
    RandomIdGenerator::default().new_trace_id().to_string()
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
            span_context: SpanContext::NONE,
            commit_cohort_id: None,
            links: Vec::new(),
        }
    }

    pub(crate) fn with_commit_cohort_id(mut self, commit_cohort_id: String) -> Self {
        self.commit_cohort_id = Some(commit_cohort_id);
        self
    }

    pub(crate) fn with_links(mut self, links: Vec<SpanContext>) -> Self {
        self.links = links;
        self
    }

    pub(crate) fn as_link(&self) -> Option<SpanContext> {
        self.span_context
            .is_valid()
            .then(|| self.span_context.clone())
    }

    #[cfg(test)]
    pub(crate) fn for_test(sink: Arc<dyn TelemetrySink>, span_context: SpanContext) -> Self {
        Self {
            sink,
            span_context,
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

    fn poll(mut self: Pin<&mut Self>, task: &mut TaskContext<'_>) -> Poll<Self::Output> {
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
                    }
                }
            }
        }
        if let Some(parent_span_context) = parent.as_ref().and_then(TelemetryContext::as_link) {
            start.parent_span_context = Some(parent_span_context);
        } else if start.parent_span_context.is_none() {
            start.parent_span_context = CURRENT_REMOTE_PARENT
                .with(|current| current.borrow().last().cloned());
        }
        debug_assert_attributes(start.descriptor, &start.attributes);
        let descriptor = start.descriptor;
        let links = start.links.clone();
        let handle = sink.start_span(start);
        let span_context = handle.span_context().clone();
        assert!(
            span_context.is_valid(),
            "TelemetrySink returned an invalid OpenTelemetry SpanContext"
        );
        let context = TelemetryContext {
            sink: Arc::clone(sink),
            span_context,
            commit_cohort_id: parent
                .as_ref()
                .and_then(|parent| parent.commit_cohort_id.clone()),
            links,
        };
        Self {
            handle: Some(handle),
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

    pub(crate) fn telemetry_context(&self) -> TelemetryContext {
        self.context.clone()
    }

    pub(crate) fn finish(
        mut self,
        status: Status,
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

    fn poll(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let entered = this.span.as_ref().map(ActiveTelemetrySpan::enter);
        let result = this.future.as_mut().poll(context);
        drop(entered);
        match result {
            Poll::Ready(result) => {
                if let Some(span) = this.span.take() {
                    match &result {
                        Ok(_) => span.finish(Status::Unset, Vec::new()),
                        Err(error) => span.finish(
                            Status::error(error.code.clone()),
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

    fn poll(self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let entered = this.span.as_ref().map(ActiveTelemetrySpan::enter);
        let result = this.future.as_mut().poll(context);
        drop(entered);
        match result {
            Poll::Ready(value) => {
                if let Some(span) = this.span.take() {
                    span.finish(Status::Unset, Vec::new());
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
            status: Status::Unset,
            attributes: vec![TelemetryAttribute::boolean("lix.operation.cancelled", true)],
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

    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
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
    if !sink.enabled(&REPOSITORY_OPENED) {
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
        TelemetrySpanStart::new(&REPOSITORY_OPENED, attributes),
    );
    span.finish(Status::Unset, Vec::new());
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use std::sync::Mutex;
    use tracing_subscriber::prelude::*;

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
            Box::new(RecordingHandle(new_span_context(None)))
        }
    }

    struct RecordingHandle(SpanContext);

    impl TelemetrySpanHandle for RecordingHandle {
        fn span_context(&self) -> &SpanContext {
            &self.0
        }

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
        assert_eq!(PRODUCTION_NAMES.len(), 11);
        assert_eq!(spans::ALL.len(), 11);
        let mut names = PRODUCTION_NAMES.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 11);
        for forbidden in FORBIDDEN_PRODUCTION_NAMES {
            assert!(!PRODUCTION_NAMES.contains(forbidden));
        }
    }

    #[test]
    fn callback_context_inherits_trace_flags_and_trace_state() {
        let root = new_span_context(None);
        let trace_state = TraceState::from_key_value([("vendor", "state")]).expect("trace state");
        let parent = SpanContext::new(
            root.trace_id(),
            root.span_id(),
            TraceFlags::NOT_SAMPLED,
            true,
            trace_state.clone(),
        );
        let child = new_span_context(Some(&parent));
        assert_eq!(child.trace_id(), parent.trace_id());
        assert_eq!(child.trace_flags(), TraceFlags::NOT_SAMPLED);
        assert_eq!(child.trace_state(), &trace_state);
        assert!(!child.is_remote());
        assert_ne!(child.span_id(), parent.span_id());
    }

    #[test]
    fn callback_sink_does_not_export_unsampled_spans() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink = CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        });
        let root = new_span_context(None);
        let parent = SpanContext::new(
            root.trace_id(),
            root.span_id(),
            TraceFlags::NOT_SAMPLED,
            false,
            TraceState::NONE,
        );
        let mut start = TelemetrySpanStart::new(&SQL_QUERY, Vec::new());
        start.parent_span_context = Some(parent);
        let handle = sink.start_span(start);
        assert!(!handle.span_context().is_sampled());
        handle.finish(TelemetrySpanEnd {
            duration_ns: 1,
            status: Status::Unset,
            attributes: Vec::new(),
        });
        assert!(completed.lock().expect("completed").is_empty());
    }

    #[test]
    fn callback_sink_uses_explicit_remote_parent_for_root_spans() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let generated = new_span_context(None);
        let parent = SpanContext::new(
            generated.trace_id(),
            generated.span_id(),
            TraceFlags::SAMPLED,
            true,
            TraceState::NONE,
        );
        let sink = CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        })
        .with_root_parent(parent.clone());
        let handle = sink.start_span(TelemetrySpanStart::new(&SQL_QUERY, Vec::new()));
        assert_eq!(handle.span_context().trace_id(), parent.trace_id());
        handle.finish(TelemetrySpanEnd {
            duration_ns: 1,
            status: Status::Unset,
            attributes: Vec::new(),
        });
        let completed = completed.lock().expect("completed");
        assert_eq!(completed[0].start.parent_span_context, Some(parent));
    }

    #[test]
    fn ffi_remote_parent_is_scoped_to_one_operation_future() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        let generated = new_span_context(None);
        let parent = SpanContext::new(
            generated.trace_id(),
            generated.span_id(),
            TraceFlags::SAMPLED,
            true,
            TraceState::NONE,
        );
        futures_lite::future::block_on(instrument_remote_parent(
            Some(parent.clone()),
            async {
                let span = ActiveTelemetrySpan::start_if_enabled(
                    &sink,
                    &SQL_QUERY,
                    Vec::new(),
                )
                .expect("query span");
                span.finish(Status::Unset, Vec::new());
            },
        ));
        let outside = ActiveTelemetrySpan::start_if_enabled(&sink, &SQL_QUERY, Vec::new())
            .expect("outside query span");
        outside.finish(Status::Unset, Vec::new());

        let completed = completed.lock().expect("completed");
        assert_eq!(completed[0].start.parent_span_context, Some(parent));
        assert!(completed[1].start.parent_span_context.is_none());
        assert_ne!(completed[0].span_context.trace_id(), completed[1].span_context.trace_id());
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
        assert_eq!(started[0].descriptor(), &REPOSITORY_OPENED);
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
            TelemetrySpanStart::new(&ENGINE_OPEN, Vec::new()),
        ));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].start.name, "lix.engine.open");
        assert_eq!(completed[0].end.status, Status::Unset);
        assert!(completed[0].end.attributes.iter().any(|attribute| {
            attribute.key == "lix.operation.cancelled"
                && attribute.value == TelemetryValue::Boolean(true)
        }));
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
            TelemetrySpanStart::new(&SESSION_OPEN, Vec::new()),
        );
        drop(instrument_lix_result(
            Some(span),
            std::future::pending::<Result<(), crate::LixError>>(),
        ));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].end.status, Status::Unset);
        assert!(completed[0].end.attributes.iter().any(|attribute| {
            attribute.key == "lix.operation.cancelled"
                && attribute.value == TelemetryValue::Boolean(true)
        }));
    }

    #[test]
    fn performance_spans_inherit_cohort_id_and_links() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        let links = vec![new_span_context(None), new_span_context(None)];
        let context = TelemetryContext::root(Arc::clone(&sink))
            .with_commit_cohort_id("cohort-1".to_string())
            .with_links(links.clone());
        futures_lite::future::block_on(context.instrument(async {
            let span = ActiveTelemetrySpan::start_current(
                &TRANSACTION_MATERIALIZE,
                vec![TelemetryAttribute::i64("lix.transaction.count", 2)],
            )
            .expect("materialize enabled");
            span.finish(Status::Unset, Vec::new());
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

    #[test]
    fn single_transaction_performance_span_has_parent_without_duplicate_link() {
        let completed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&completed);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            captured.lock().expect("completed").push(span);
        }));
        let expected_parent = new_span_context(None);
        let context = TelemetryContext::for_test(Arc::clone(&sink), expected_parent.clone());
        futures_lite::future::block_on(context.instrument(async {
            let span = ActiveTelemetrySpan::start_current(
                &TRANSACTION_MATERIALIZE,
                vec![TelemetryAttribute::i64("lix.transaction.count", 1)],
            )
            .expect("materialize enabled");
            span.finish(Status::Unset, Vec::new());
        }));
        let completed = completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0].start.parent_span_context.as_ref(),
            Some(&expected_parent)
        );
        assert!(completed[0].start.links.is_empty());
    }

    #[test]
    fn tracing_sink_exports_real_parent_context_links_and_status() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("lix");
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer)),
        );
        let sink: Arc<dyn TelemetrySink> = Arc::new(OpenTelemetryTracingSink::new());
        let parent = ActiveTelemetrySpan::start(
            &sink,
            TelemetrySpanStart::new(&SQL_BATCH, Vec::new()),
        );
        let parent_context = parent.telemetry_context();
        let parent_span_context = parent_context.span_context.clone();
        let linked_context = new_span_context(None);
        let child_context = parent_context.with_links(vec![linked_context.clone()]);
        futures_lite::future::block_on(child_context.instrument(async {
            let child = ActiveTelemetrySpan::start_current(
                &TRANSACTION_MATERIALIZE,
                vec![TelemetryAttribute::i64("lix.transaction.count", 2)],
            )
            .expect("materialize span");
            child.finish(Status::Unset, Vec::new());
        }));
        parent.finish(Status::Unset, Vec::new());
        provider.force_flush().expect("flush spans");

        let spans = exporter.get_finished_spans().expect("exported spans");
        let parent = spans
            .iter()
            .find(|span| span.name == "lix.sql.batch")
            .expect("parent span");
        let child = spans
            .iter()
            .find(|span| span.name == "lix.transaction.materialize")
            .expect("child span");
        assert_eq!(parent.span_context, parent_span_context);
        assert_eq!(child.span_context.trace_id(), parent.span_context.trace_id());
        assert_eq!(child.parent_span_id, parent.span_context.span_id());
        assert_eq!(child.links.len(), 1);
        assert_eq!(child.links[0].span_context, linked_context);
        assert_eq!(child.span_kind, SpanKind::Internal);
        assert_eq!(child.status, Status::Unset);
        assert!(child.attributes.iter().all(|attribute| {
            !matches!(
                attribute.key.as_str(),
                "lix.trace_id"
                    | "lix.span_id"
                    | "lix.parent_span_id"
                    | "lix.span.links"
                    | "otel.status_code"
            )
        }));
    }
}
