#![allow(missing_debug_implementations)]

use std::cell::{Cell, RefCell};
use std::future::{Future, IntoFuture};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use futures_util::future::{AbortHandle, Abortable};
use js_sys::{Array, Function, Reflect};
use lix::telemetry::{CallbackTelemetrySink, SpanContext, TelemetrySink, instrument_remote_parent};
use lix::{
    BROWSER_TRANSPORT_CONFIG_HEADER, CreateBranchOptions as RsCreateBranchOptions,
    ExecuteBatchStatement as RsExecuteBatchStatement, ExecuteResult as RsExecuteResult,
    InternalSyncCacheOptions, Lix as RsLix, LixError, LixTransaction as RsLixTransaction, Memory,
    MergeBranchOptions as RsMergeBranchOptions, MergeBranchOutcome, MergeBranchPreviewOptions,
    ObserveEvents as RsObserveEvents, OpenPhase, OpenProgress, OpenProgressSink, OpenReport,
    StatementAuthorityRoute, SwitchBranchOptions as RsSwitchBranchOptions, Value, open_lix,
    register_browser_sync_transport, unregister_browser_sync_transport,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
#[cfg(feature = "storage-bridge-bench")]
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_futures::spawn_local;

use crate::browser_storage::BrowserStorage;
use crate::js_storage::{JsStorage, JsStorageProvider};

#[path = "wasm_remote.rs"]
mod remote;

type BrowserLix = RsLix<BrowserStorage>;
type BrowserTransaction = RsLixTransaction<BrowserStorage>;
type BrowserObserveEvents = RsObserveEvents<BrowserStorage>;

#[cfg(feature = "storage-bridge-bench")]
#[wasm_bindgen]
extern "C" {
    /// Benchmark-only shape matching the asynchronous Rust -> JavaScript
    /// storage calls used by the browser SQLite adapter.
    pub type StorageBridgeBenchmarkBackend;

    #[wasm_bindgen(method, js_name = roundTrip)]
    fn round_trip(this: &StorageBridgeBenchmarkBackend, payload: JsValue) -> js_sys::Promise;
}

#[cfg(feature = "storage-bridge-bench")]
#[derive(Serialize, Deserialize)]
struct StorageBridgeBenchmarkEntry {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    #[serde(with = "serde_bytes")]
    value: Vec<u8>,
}

/// Measures the actual wasm-bindgen + Promise + serde bridge used by browser
/// storage without mixing provider I/O into the result. `calls` controls
/// boundary crossings and `items_per_call` controls batching.
#[cfg(feature = "storage-bridge-bench")]
#[wasm_bindgen(js_name = benchmarkStorageBridge)]
pub async fn benchmark_storage_bridge(
    backend: StorageBridgeBenchmarkBackend,
    calls: usize,
    items_per_call: usize,
    value_bytes: usize,
) -> Result<u32, JsValue> {
    let mut checksum = 0_u32;
    for call in 0..calls {
        let entries = (0..items_per_call)
            .map(|item| StorageBridgeBenchmarkEntry {
                key: (call.wrapping_mul(items_per_call).wrapping_add(item))
                    .to_le_bytes()
                    .to_vec(),
                value: vec![u8::try_from(item & 0xff).unwrap_or_default(); value_bytes],
            })
            .collect::<Vec<_>>();
        let payload = to_js(&entries)?;
        let response = JsFuture::from(backend.round_trip(payload)).await?;
        let response: Vec<StorageBridgeBenchmarkEntry> = from_js(response)?;
        checksum = checksum.wrapping_add(
            response
                .iter()
                .map(|entry| {
                    u32::try_from(entry.key.len().wrapping_add(entry.value.len()))
                        .unwrap_or(u32::MAX)
                })
                .sum::<u32>(),
        );
    }
    Ok(checksum)
}

#[wasm_bindgen]
pub struct WasmLix {
    inner: BrowserLix,
    storage: BrowserStorage,
    storage_sessions: Rc<Cell<usize>>,
    closed: Cell<bool>,
    browser_sync_transport_id: Rc<RefCell<Option<String>>>,
    telemetry_parent: Option<PendingTelemetryParent>,
}

const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 64 * 1024;
type WasmSnapshotMessage = Result<Option<Vec<u8>>, LixError>;

struct WasmSnapshotWriter {
    sender: async_channel::Sender<WasmSnapshotMessage>,
    buffer: Vec<u8>,
    pending: Option<Pin<Box<dyn Future<Output = io::Result<()>> + Send>>>,
}

impl WasmSnapshotWriter {
    fn new(sender: async_channel::Sender<WasmSnapshotMessage>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(SNAPSHOT_STREAM_CHUNK_BYTES),
            pending: None,
        }
    }

    fn start_send(&mut self) {
        let sender = self.sender.clone();
        let chunk = std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(SNAPSHOT_STREAM_CHUNK_BYTES),
        );
        self.pending = Some(Box::pin(async move {
            sender.send(Ok(Some(chunk))).await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "snapshot export was canceled")
            })
        }));
    }

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(pending) = self.pending.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match pending.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl futures_lite::io::AsyncWrite for WasmSnapshotWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_pending(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        let available = SNAPSHOT_STREAM_CHUNK_BYTES - self.buffer.len();
        let written = available.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..written]);
        if self.buffer.len() == SNAPSHOT_STREAM_CHUNK_BYTES {
            self.start_send();
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match self.poll_pending(cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
            if self.buffer.is_empty() {
                return Poll::Ready(Ok(()));
            }
            self.start_send();
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.sender.close();
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

#[wasm_bindgen]
pub struct WasmSnapshotExport {
    receiver: async_channel::Receiver<WasmSnapshotMessage>,
    completion: async_channel::Receiver<()>,
    canceled: Cell<bool>,
}

#[wasm_bindgen]
impl WasmSnapshotExport {
    #[wasm_bindgen]
    pub async fn next(&self) -> Result<Option<Vec<u8>>, JsValue> {
        match self.receiver.recv().await {
            Ok(Ok(Some(chunk))) => Ok(Some(chunk)),
            Ok(Ok(None)) | Err(_) => {
                let _ = self.completion.recv().await;
                Ok(None)
            }
            Ok(Err(error)) => {
                let _ = self.completion.recv().await;
                Err(lix_error_to_js(error))
            }
        }
    }

    #[wasm_bindgen]
    pub async fn cancel(&self) -> Result<(), JsValue> {
        if self.canceled.replace(true) {
            return Ok(());
        }
        self.receiver.close();
        let _ = self.completion.recv().await;
        Ok(())
    }
}

impl Drop for WasmSnapshotExport {
    fn drop(&mut self) {
        self.receiver.close();
    }
}

enum WasmSnapshotInputMessage {
    Chunk(Vec<u8>),
    Done,
}

struct WasmSnapshotReader {
    receiver: Pin<Box<async_channel::Receiver<WasmSnapshotInputMessage>>>,
    pending: Option<
        Pin<
            Box<
                dyn Future<Output = Result<WasmSnapshotInputMessage, async_channel::RecvError>>
                    + Send,
            >,
        >,
    >,
    chunk: Vec<u8>,
    offset: usize,
    finished: bool,
}

impl WasmSnapshotReader {
    fn new(receiver: async_channel::Receiver<WasmSnapshotInputMessage>) -> Self {
        Self {
            receiver: Box::pin(receiver),
            pending: None,
            chunk: Vec::new(),
            offset: 0,
            finished: false,
        }
    }
}

impl futures_lite::io::AsyncRead for WasmSnapshotReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            if this.offset < this.chunk.len() {
                let read = output.len().min(this.chunk.len() - this.offset);
                output[..read].copy_from_slice(&this.chunk[this.offset..this.offset + read]);
                this.offset += read;
                return Poll::Ready(Ok(read));
            }
            if this.finished {
                return Poll::Ready(Ok(0));
            }
            if this.pending.is_none() {
                let receiver = this.receiver.as_ref().get_ref().clone();
                this.pending = Some(Box::pin(async move { receiver.recv().await }));
            }
            let pending = this
                .pending
                .as_mut()
                .expect("pending receive was initialized");
            match pending.as_mut().poll(cx) {
                Poll::Ready(Ok(WasmSnapshotInputMessage::Chunk(chunk))) => {
                    this.pending = None;
                    this.chunk = chunk;
                    this.offset = 0;
                }
                Poll::Ready(Ok(WasmSnapshotInputMessage::Done)) => {
                    this.pending = None;
                    this.finished = true;
                }
                Poll::Ready(Err(_)) => {
                    this.pending = None;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "snapshot input was canceled",
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[wasm_bindgen]
pub struct WasmSnapshotRestore {
    input: async_channel::Sender<WasmSnapshotInputMessage>,
    result: async_channel::Receiver<Result<WasmLix, JsValue>>,
    finished: Cell<bool>,
    complete: Rc<Cell<bool>>,
}

#[wasm_bindgen]
impl WasmSnapshotRestore {
    #[wasm_bindgen(js_name = isComplete)]
    pub fn is_complete(&self) -> bool {
        self.complete.get()
    }

    #[wasm_bindgen]
    pub async fn write(&self, chunk: Vec<u8>) -> Result<(), JsValue> {
        if self.finished.get() {
            return Err(JsValue::from_str("snapshot restore input is closed"));
        }
        self.input
            .send(WasmSnapshotInputMessage::Chunk(chunk))
            .await
            .map_err(|_| JsValue::from_str("snapshot restore is no longer accepting input"))
    }

    #[wasm_bindgen]
    pub async fn finish(&self) -> Result<WasmLix, JsValue> {
        if self.finished.replace(true) {
            return Err(JsValue::from_str(
                "snapshot restore input is already closed",
            ));
        }
        // Preserve the decoder's semantic error if it stopped reading before
        // EOF; failure to send Done only means the result is already ready.
        let _ = self.input.send(WasmSnapshotInputMessage::Done).await;
        self.input.close();
        self.result.recv().await.unwrap_or_else(|_| {
            Err(JsValue::from_str(
                "snapshot restore stopped without returning a result",
            ))
        })
    }

    #[wasm_bindgen]
    pub async fn cancel(&self) -> Result<(), JsValue> {
        if self.finished.replace(true) {
            return Ok(());
        }
        self.input.close();
        // Wait for the restore task to release storage ownership. Its expected
        // cancellation error is intentionally discarded.
        let _ = self.result.recv().await;
        Ok(())
    }
}

impl Drop for WasmSnapshotRestore {
    fn drop(&mut self) {
        self.input.close();
    }
}

type PendingTelemetryParent = Rc<RefCell<Option<SpanContext>>>;

#[wasm_bindgen]
pub struct WasmLixTransaction {
    inner: Option<BrowserTransaction>,
    telemetry_parent: Option<PendingTelemetryParent>,
}

#[wasm_bindgen]
pub struct WasmObserveEvents {
    inner: RefCell<Option<BrowserObserveEvents>>,
    closed: Cell<bool>,
    next_abort: RefCell<Option<AbortHandle>>,
    telemetry_parent: Option<PendingTelemetryParent>,
}

#[wasm_bindgen(js_name = openMemory)]
pub async fn open_memory(
    telemetry_dispatch: Option<Function>,
    telemetry_parent: Option<JsValue>,
    server: Option<JsValue>,
    open_progress_dispatch: Option<Function>,
) -> Result<WasmLix, JsValue> {
    open_browser_storage(
        BrowserStorage::Memory(Memory::new()),
        None,
        telemetry_dispatch,
        telemetry_parent,
        server,
        open_progress_dispatch,
    )
    .await
}

#[wasm_bindgen(js_name = openMemoryFromSnapshot)]
pub fn open_memory_from_snapshot(
    telemetry_dispatch: Option<Function>,
    telemetry_parent: Option<JsValue>,
    open_progress_dispatch: Option<Function>,
) -> WasmSnapshotRestore {
    let (input, receiver) = async_channel::bounded(1);
    let source = WasmSnapshotReader::new(receiver);
    let open = open_browser_storage(
        BrowserStorage::Memory(Memory::new()),
        Some(source),
        telemetry_dispatch,
        telemetry_parent,
        None,
        open_progress_dispatch,
    );
    let (result_sender, result) = async_channel::bounded(1);
    let complete = Rc::new(Cell::new(false));
    let task_complete = Rc::clone(&complete);
    spawn_local(async move {
        let _ = result_sender.send(open.await).await;
        task_complete.set(true);
    });
    WasmSnapshotRestore {
        input,
        result,
        finished: Cell::new(false),
        complete,
    }
}

#[wasm_bindgen(js_name = openJsStorage)]
pub async fn open_js_storage(
    provider: JsStorageProvider,
    telemetry_dispatch: Option<Function>,
    telemetry_parent: Option<JsValue>,
    server: Option<JsValue>,
    open_progress_dispatch: Option<Function>,
) -> Result<WasmLix, JsValue> {
    let storage = JsStorage::new(provider);
    let browser_storage = BrowserStorage::Js(storage);
    match open_browser_storage(
        browser_storage.clone(),
        None,
        telemetry_dispatch,
        telemetry_parent,
        server,
        open_progress_dispatch,
    )
    .await
    {
        Ok(lix) => Ok(lix),
        Err(error) => {
            let _ = browser_storage.close().await;
            Err(error)
        }
    }
}

#[wasm_bindgen(js_name = openJsStorageFromSnapshot)]
pub fn open_js_storage_from_snapshot(
    provider: JsStorageProvider,
    telemetry_dispatch: Option<Function>,
    telemetry_parent: Option<JsValue>,
    open_progress_dispatch: Option<Function>,
) -> WasmSnapshotRestore {
    let storage = JsStorage::new(provider);
    let browser_storage = BrowserStorage::Js(storage);
    let (input, receiver) = async_channel::bounded(1);
    let source = WasmSnapshotReader::new(receiver);
    let open_storage = browser_storage.clone();
    let open = async move {
        match open_browser_storage(
            open_storage.clone(),
            Some(source),
            telemetry_dispatch,
            telemetry_parent,
            None,
            open_progress_dispatch,
        )
        .await
        {
            Ok(lix) => Ok(lix),
            Err(error) => {
                let _ = open_storage.close().await;
                Err(error)
            }
        }
    };
    let (result_sender, result) = async_channel::bounded(1);
    let complete = Rc::new(Cell::new(false));
    let task_complete = Rc::clone(&complete);
    spawn_local(async move {
        let _ = result_sender.send(open.await).await;
        task_complete.set(true);
    });
    WasmSnapshotRestore {
        input,
        result,
        finished: Cell::new(false),
        complete,
    }
}

async fn open_browser_storage(
    storage: BrowserStorage,
    snapshot: Option<WasmSnapshotReader>,
    telemetry_dispatch: Option<Function>,
    telemetry_parent: Option<JsValue>,
    server: Option<JsValue>,
    open_progress_dispatch: Option<Function>,
) -> Result<WasmLix, JsValue> {
    console_error_panic_hook::set_once();
    let telemetry_parent = telemetry_parent
        .map(|value| {
            js_sys::JSON::stringify(&value)
                .map_err(|_| JsValue::from_str("telemetry parent context must be serializable"))?
                .as_string()
                .ok_or_else(|| JsValue::from_str("telemetry parent context must be an object"))
        })
        .transpose()?
        .map(|json| crate::telemetry::parse_parent_context_json(Some(json)))
        .transpose()
        .map_err(|error| JsValue::from_str(&error))?
        .flatten();
    let telemetry_parent_source = telemetry_dispatch
        .as_ref()
        .map(|_| Rc::new(RefCell::new(None)));
    let telemetry = telemetry_dispatch.map(|dispatch| {
        let dispatch = BrowserTelemetryDispatch(dispatch);
        let sink = CallbackTelemetrySink::new(move |span| {
            let Ok(span) = to_js(&crate::telemetry::TelemetrySpanDto::from(span)) else {
                return;
            };
            let _ = dispatch.0.call1(&JsValue::UNDEFINED, &span);
        });
        let sink: Arc<dyn TelemetrySink> = Arc::new(sink);
        sink
    });
    let open_progress = open_progress_dispatch.map(|dispatch| {
        let sink: Arc<dyn OpenProgressSink> =
            Arc::new(BrowserOpenProgressSink(BrowserFunctionDispatch(dispatch)));
        sink
    });
    #[derive(Deserialize)]
    struct BrowserSyncServerOptions {
        url: String,
        headers: Vec<(String, String)>,
    }
    static NEXT_BROWSER_SYNC_TRANSPORT_ID: AtomicU64 = AtomicU64::new(1);
    let mut browser_sync_transport_id = None;
    let server = match server {
        Some(value) => {
            let mut parsed =
                serde_wasm_bindgen::from_value::<BrowserSyncServerOptions>(value.clone())?;
            let header_provider = optional_function_property(&value, "headerProvider")?;
            let fetch = optional_function_property(&value, "fetch")?;
            if header_provider.is_some() || fetch.is_some() {
                let id = format!(
                    "browser-{}",
                    NEXT_BROWSER_SYNC_TRANSPORT_ID.fetch_add(1, Ordering::Relaxed)
                );
                register_browser_sync_transport(id.clone(), header_provider, fetch);
                parsed
                    .headers
                    .push((BROWSER_TRANSPORT_CONFIG_HEADER.to_owned(), id.clone()));
                browser_sync_transport_id = Some(id);
            }
            Some(InternalSyncCacheOptions::sync(parsed.url).with_headers(parsed.headers))
        }
        None => None,
    };
    let open = async {
        let mut builder = open_lix().with_storage(storage.clone());
        if let Some(telemetry) = telemetry {
            builder = builder.with_telemetry(telemetry);
        }
        if let Some(open_progress) = open_progress {
            builder = builder.with_open_progress_sink(open_progress);
        }
        match (server, snapshot) {
            // SAFETY: the worker never exposes this raw sync binding. The JS
            // open path immediately wraps it with the paired authority binding
            // and its publication/alignment gates.
            (Some(server), None) => unsafe { builder.with_internal_sync_cache(server) }.await,
            (None, Some(snapshot)) => builder.from_snapshot(snapshot).await,
            (None, None) => builder.await,
            (Some(_), Some(_)) => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "snapshot restore cannot be combined with server mode",
            )),
        }
    };
    let inner = instrument_remote_parent(telemetry_parent, open)
        .await
        .map_err(|error| {
            if let Some(id) = browser_sync_transport_id.as_deref() {
                unregister_browser_sync_transport(id);
            }
            lix_error_to_js(error)
        })?;
    Ok(WasmLix {
        inner,
        storage,
        storage_sessions: Rc::new(Cell::new(1)),
        closed: Cell::new(false),
        browser_sync_transport_id: Rc::new(RefCell::new(browser_sync_transport_id)),
        telemetry_parent: telemetry_parent_source,
    })
}

fn optional_function_property(value: &JsValue, name: &str) -> Result<Option<Function>, JsValue> {
    let property = Reflect::get(value, &name.into())?;
    if property.is_null() || property.is_undefined() {
        return Ok(None);
    }
    property
        .dyn_into::<Function>()
        .map(Some)
        .map_err(Into::into)
}

struct BrowserTelemetryDispatch(Function);

struct BrowserFunctionDispatch(Function);

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but the shared telemetry trait requires Send"
)]
unsafe impl Send for BrowserTelemetryDispatch {}
unsafe impl Sync for BrowserTelemetryDispatch {}

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but the shared progress trait requires Send"
)]
unsafe impl Send for BrowserFunctionDispatch {}
unsafe impl Sync for BrowserFunctionDispatch {}

struct BrowserOpenProgressSink(BrowserFunctionDispatch);

impl OpenProgressSink for BrowserOpenProgressSink {
    fn report(&self, progress: OpenProgress) {
        let Ok(progress) = to_js(&OpenProgressDto::from(progress)) else {
            return;
        };
        let _ = self.0.0.call1(&JsValue::UNDEFINED, &progress);
    }
}

impl WasmLix {
    fn instrument_operation<F: IntoFuture>(&self, future: F) -> impl Future<Output = F::Output> {
        instrument_remote_parent(
            self.telemetry_parent
                .as_ref()
                .and_then(|parent| parent.borrow_mut().take()),
            future.into_future(),
        )
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OpenProgressDto {
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    from_format: Option<u32>,
    to_format: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<f64>,
}

#[expect(
    clippy::cast_precision_loss,
    reason = "JavaScript progress counters are observational Number values"
)]
impl From<OpenProgress> for OpenProgressDto {
    fn from(progress: OpenProgress) -> Self {
        Self {
            phase: open_phase_name(progress.phase),
            from_format: progress.from_format,
            to_format: progress.to_format,
            completed: progress.completed.map(|value| value as f64),
            total: progress.total.map(|value| value as f64),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OpenMigrationReportDto {
    from_format: u32,
    to_format: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OpenReportDto {
    format: u32,
    initialized: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    migration: Option<OpenMigrationReportDto>,
}

impl From<&OpenReport> for OpenReportDto {
    fn from(report: &OpenReport) -> Self {
        Self {
            format: report.format,
            initialized: report.initialized,
            migration: report.migration.map(|migration| OpenMigrationReportDto {
                from_format: migration.from_format,
                to_format: migration.to_format,
            }),
        }
    }
}

fn open_phase_name(phase: OpenPhase) -> &'static str {
    match phase {
        OpenPhase::Inspecting => "inspecting",
        OpenPhase::Migrating => "migrating",
        OpenPhase::Validating => "validating",
        OpenPhase::Opening => "opening",
        OpenPhase::Complete => "complete",
        _ => "opening",
    }
}

fn authority_route_name(route: StatementAuthorityRoute) -> &'static str {
    match route {
        StatementAuthorityRoute::HotRead => "hot",
        StatementAuthorityRoute::AuthorityRead => "history",
        StatementAuthorityRoute::AuthorityWrite => "mutation",
    }
}

#[wasm_bindgen]
impl WasmLix {
    #[wasm_bindgen(js_name = openReport)]
    pub fn open_report(&self) -> Result<JsValue, JsValue> {
        to_js(&OpenReportDto::from(self.inner.open_report()))
    }

    #[wasm_bindgen(js_name = setTelemetryParent)]
    pub fn set_telemetry_parent(&self, parent: Option<JsValue>) -> Result<(), JsValue> {
        let Some(parent_source) = &self.telemetry_parent else {
            return Ok(());
        };
        let parent = parent
            .map(|value| {
                js_sys::JSON::stringify(&value)
                    .map_err(|_| {
                        JsValue::from_str("telemetry parent context must be serializable")
                    })?
                    .as_string()
                    .ok_or_else(|| JsValue::from_str("telemetry parent context must be an object"))
            })
            .transpose()?
            .map(|json| crate::telemetry::parse_parent_context_json(Some(json)))
            .transpose()
            .map_err(|error| JsValue::from_str(&error))?
            .flatten();
        *parent_source.borrow_mut() = parent;
        Ok(())
    }

    #[wasm_bindgen(js_name = openAnotherSession)]
    pub async fn open_another_session(&self, options: JsValue) -> Result<WasmLix, JsValue> {
        let options: OpenAnotherSessionOptionsDto = from_js(options)?;
        let mut builder = self.inner.open_another_session();
        if let Some(branch_id) = options.branch_id {
            builder = builder.with_branch(branch_id);
        }
        if let Some(account_id) = options.account_id {
            builder = builder.with_account(account_id);
        }
        let inner = self
            .instrument_operation(builder)
            .await
            .map_err(lix_error_to_js)?;
        self.storage_sessions
            .set(self.storage_sessions.get().saturating_add(1));
        Ok(WasmLix {
            inner,
            storage: self.storage.clone(),
            storage_sessions: self.storage_sessions.clone(),
            closed: Cell::new(false),
            browser_sync_transport_id: self.browser_sync_transport_id.clone(),
            telemetry_parent: self.telemetry_parent.clone(),
        })
    }

    #[wasm_bindgen(js_name = exportSnapshot)]
    pub fn export_snapshot(&self) -> WasmSnapshotExport {
        let (sender, receiver) = async_channel::bounded(1);
        let (completion_sender, completion) = async_channel::bounded(1);
        let builder = self.inner.export_snapshot();
        let telemetry_parent = self
            .telemetry_parent
            .as_ref()
            .and_then(|parent| parent.borrow_mut().take());
        let task_sender = sender.clone();
        spawn_local(async move {
            let mut writer = WasmSnapshotWriter::new(task_sender.clone());
            let result =
                instrument_remote_parent(telemetry_parent, builder.write_to(&mut writer)).await;
            let terminal = match result {
                Ok(_) => Ok(None),
                Err(error) => Err(error),
            };
            let _ = task_sender.send(terminal).await;
            let _ = completion_sender.send(()).await;
        });
        WasmSnapshotExport {
            receiver,
            completion,
            canceled: Cell::new(false),
        }
    }

    #[wasm_bindgen(js_name = execute)]
    pub async fn execute(
        &self,
        sql: String,
        params: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let params = values_from_js(params)?;
        let options = execute_options_from_js(options)?;
        let execution = self.inner.execute(&sql, &params);
        let execution = match options {
            Some(origin_key) => execution.with_origin_key(origin_key),
            None => execution,
        };
        let result = self
            .instrument_operation(execution)
            .await
            .map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = executeBatch)]
    pub async fn execute_batch(
        &self,
        statements: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let statements = batch_statements_from_js(statements)?;
        let options = execute_options_from_js(options)?;
        let execution = self.inner.execute_batch(&statements);
        let execution = match options {
            Some(origin_key) => execution.with_origin_key(origin_key),
            None => execution,
        };
        let results = self
            .instrument_operation(execution)
            .await
            .map_err(lix_error_to_js)?;
        let results = results
            .into_iter()
            .map(ExecuteResultDto::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(lix_error_to_js)?;
        to_js(&results)
    }

    #[wasm_bindgen(js_name = executionRoute)]
    pub async fn execution_route(&self, statements: JsValue) -> Result<String, JsValue> {
        let statements = from_js::<Vec<String>>(statements)?;
        self.inner
            .execution_authority_route(&statements)
            .map(authority_route_name)
            .map(str::to_owned)
            .map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = observe)]
    pub async fn observe(
        &self,
        sql: String,
        params: JsValue,
    ) -> Result<WasmObserveEvents, JsValue> {
        let params = values_from_js(params)?;
        let inner = self
            .instrument_operation(async { self.inner.observe(&sql, &params) })
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmObserveEvents {
            inner: RefCell::new(Some(inner)),
            closed: Cell::new(false),
            next_abort: RefCell::new(None),
            telemetry_parent: self
                .telemetry_parent
                .as_ref()
                .map(|_| Rc::new(RefCell::new(None))),
        })
    }

    #[wasm_bindgen(js_name = beginTransaction)]
    pub async fn begin_transaction(&self) -> Result<WasmLixTransaction, JsValue> {
        let inner = self
            .instrument_operation(self.inner.begin_transaction())
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmLixTransaction {
            inner: Some(inner),
            telemetry_parent: self.telemetry_parent.clone(),
        })
    }

    #[wasm_bindgen(js_name = activeBranchId)]
    pub async fn active_branch_id(&self) -> Result<String, JsValue> {
        self.instrument_operation(self.inner.active_branch_id())
            .await
            .map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = activeAccountId)]
    pub async fn active_account_id(&self) -> Result<String, JsValue> {
        Ok(self.inner.active_account_id().to_string())
    }

    #[wasm_bindgen(js_name = createBranch)]
    pub async fn create_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: CreateBranchOptionsDto = from_js(options)?;
        let receipt = self
            .instrument_operation(self.inner.create_branch(RsCreateBranchOptions {
                id: options.id,
                name: options.name,
                from_commit_id: options.from_commit_id,
            }))
            .await
            .map_err(lix_error_to_js)?;
        to_js(&CreateBranchReceiptDto {
            id: receipt.id,
            name: receipt.name,
            hidden: receipt.hidden,
            commit_id: receipt.commit_id,
        })
    }

    #[wasm_bindgen(js_name = undo)]
    pub async fn undo(&self) -> Result<JsValue, JsValue> {
        let receipt = self
            .instrument_operation(self.inner.undo())
            .await
            .map_err(lix_error_to_js)?;
        to_js(&UndoReceiptDto {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            inverse_commit_id: receipt.inverse_commit_id,
        })
    }

    #[wasm_bindgen(js_name = redo)]
    pub async fn redo(&self) -> Result<JsValue, JsValue> {
        let receipt = self
            .instrument_operation(self.inner.redo())
            .await
            .map_err(lix_error_to_js)?;
        to_js(&RedoReceiptDto {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            replay_commit_id: receipt.replay_commit_id,
        })
    }

    #[wasm_bindgen(js_name = switchBranch)]
    pub async fn switch_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: SwitchBranchOptionsDto = from_js(options)?;
        let receipt = self
            .instrument_operation(self.inner.switch_branch(RsSwitchBranchOptions {
                branch_id: options.branch_id,
            }))
            .await
            .map_err(lix_error_to_js)?;
        to_js(&SwitchBranchReceiptDto {
            branch_id: receipt.branch_id,
        })
    }

    #[wasm_bindgen(js_name = importFilesystemPaths)]
    pub async fn import_filesystem_paths(&self, _paths: JsValue) -> Result<(), JsValue> {
        Err(lix_error_to_js(LixError::new(
            "LIX_UNSUPPORTED_STORAGE",
            "importFilesystemPaths requires a filesystem storage",
        )))
    }

    #[wasm_bindgen(js_name = mergeBranchPreview)]
    pub async fn merge_branch_preview(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: MergeBranchOptionsDto = from_js(options)?;
        let preview = self
            .instrument_operation(self.inner.merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: options.source_branch_id,
            }))
            .await
            .map_err(lix_error_to_js)?;
        to_js(&MergeBranchPreviewDto::from(preview))
    }

    #[wasm_bindgen(js_name = mergeBranch)]
    pub async fn merge_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: MergeBranchOptionsDto = from_js(options)?;
        let receipt = self
            .instrument_operation(self.inner.merge_branch(RsMergeBranchOptions {
                source_branch_id: options.source_branch_id,
            }))
            .await
            .map_err(lix_error_to_js)?;
        to_js(&MergeBranchReceiptDto::from(receipt))
    }

    #[wasm_bindgen(js_name = syncDiskToLix)]
    pub async fn sync_disk_to_lix(&self) -> Result<(), JsValue> {
        Err(lix_error_to_js(LixError::new(
            "LIX_UNSUPPORTED_STORAGE",
            "syncDiskToLix requires a filesystem storage",
        )))
    }

    #[wasm_bindgen(js_name = close)]
    pub async fn close(&self) -> Result<(), JsValue> {
        if self.closed.replace(true) {
            return Ok(());
        }
        if let Err(error) = self.instrument_operation(self.inner.close()).await {
            self.closed.set(false);
            return Err(lix_error_to_js(error));
        }
        let remaining = self.storage_sessions.get().saturating_sub(1);
        self.storage_sessions.set(remaining);
        if remaining == 0 {
            if let Some(id) = self.browser_sync_transport_id.borrow_mut().take() {
                unregister_browser_sync_transport(&id);
            }
            self.storage
                .close()
                .await
                .map_err(|error| lix_error_to_js(error.into()))?;
        }
        Ok(())
    }
}

impl WasmLixTransaction {
    fn instrument_operation<F: IntoFuture>(&self, future: F) -> impl Future<Output = F::Output> {
        instrument_remote_parent(
            self.telemetry_parent
                .as_ref()
                .and_then(|parent| parent.borrow_mut().take()),
            future.into_future(),
        )
    }
}

#[wasm_bindgen]
impl WasmLixTransaction {
    #[wasm_bindgen(js_name = execute)]
    pub async fn execute(
        &mut self,
        sql: String,
        params: JsValue,
        options: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let params = values_from_js(params)?;
        let options = execute_options_from_js(options)?;
        let telemetry_parent = self
            .telemetry_parent
            .as_ref()
            .and_then(|parent| parent.borrow_mut().take());
        let inner = self.inner.as_mut().ok_or_else(transaction_closed_error)?;
        let execution = inner.execute(&sql, &params);
        let execution = match options {
            Some(origin_key) => execution.with_origin_key(origin_key),
            None => execution,
        };
        let result = instrument_remote_parent(telemetry_parent, execution.into_future())
            .await
            .map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = commit)]
    pub async fn commit(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        self.instrument_operation(inner.commit())
            .await
            .map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = rollback)]
    pub async fn rollback(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        self.instrument_operation(inner.rollback())
            .await
            .map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmObserveEvents {
    #[wasm_bindgen(js_name = setTelemetryParent)]
    pub fn set_telemetry_parent(&self, parent: Option<JsValue>) -> Result<(), JsValue> {
        let Some(parent_source) = &self.telemetry_parent else {
            return Ok(());
        };
        let parent = parent
            .map(|value| {
                js_sys::JSON::stringify(&value)
                    .map_err(|_| {
                        JsValue::from_str("telemetry parent context must be serializable")
                    })?
                    .as_string()
                    .ok_or_else(|| JsValue::from_str("telemetry parent context must be an object"))
            })
            .transpose()?
            .map(|json| crate::telemetry::parse_parent_context_json(Some(json)))
            .transpose()
            .map_err(|error| JsValue::from_str(&error))?
            .flatten();
        *parent_source.borrow_mut() = parent;
        Ok(())
    }

    #[wasm_bindgen(js_name = next)]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        if self.closed.get() {
            return Ok(JsValue::UNDEFINED);
        }
        let mut inner = self
            .inner
            .borrow_mut()
            .take()
            .ok_or_else(observe_next_in_flight_error)?;
        let (abort, registration) = AbortHandle::new_pair();
        self.next_abort.borrow_mut().replace(abort);
        let telemetry_parent = self
            .telemetry_parent
            .as_ref()
            .and_then(|parent| parent.borrow_mut().take());
        let result =
            instrument_remote_parent(telemetry_parent, Abortable::new(inner.next(), registration))
                .await;
        self.next_abort.borrow_mut().take();
        let result = match result {
            Ok(result) if !self.closed.get() => result,
            Ok(_) | Err(_) => {
                inner.close();
                Ok(None)
            }
        };
        self.inner.borrow_mut().replace(inner);
        let Some(event) = result.map_err(lix_error_to_js)? else {
            return Ok(JsValue::UNDEFINED);
        };
        let rows = ExecuteResultDto::try_from(event.rows).map_err(lix_error_to_js)?;
        to_js(&ObserveEventDto {
            sequence: js_number(event.sequence),
            mutation_sequence: js_number(event.mutation_sequence),
            rows,
        })
    }

    #[wasm_bindgen(js_name = close)]
    pub fn close(&self) {
        self.closed.set(true);
        if let Some(abort) = self.next_abort.borrow_mut().take() {
            abort.abort();
        } else if let Some(inner) = self.inner.borrow_mut().as_mut() {
            inner.close();
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecuteOptionsDto {
    origin_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OpenAnotherSessionOptionsDto {
    pub(super) branch_id: Option<String>,
    pub(super) account_id: Option<String>,
}

pub(super) fn execute_options_from_js(options: Option<JsValue>) -> Result<Option<String>, JsValue> {
    match options {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let options: ExecuteOptionsDto = from_js(value)?;
            Ok(options.origin_key)
        }
        _ => Ok(None),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CreateBranchOptionsDto {
    pub(super) id: Option<String>,
    pub(super) name: String,
    pub(super) from_commit_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CreateBranchReceiptDto {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) hidden: bool,
    pub(super) commit_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct UndoReceiptDto {
    pub(super) branch_id: String,
    pub(super) target_commit_id: String,
    pub(super) inverse_commit_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RedoReceiptDto {
    pub(super) branch_id: String,
    pub(super) target_commit_id: String,
    pub(super) replay_commit_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SwitchBranchOptionsDto {
    pub(super) branch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SwitchBranchReceiptDto {
    pub(super) branch_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeBranchOptionsDto {
    source_branch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergeBranchReceiptDto {
    outcome: &'static str,
    target_branch_id: String,
    source_branch_id: String,
    base_commit_id: String,
    target_head_before_commit_id: String,
    source_head_before_commit_id: String,
    target_head_after_commit_id: String,
    created_merge_commit_id: Option<String>,
    change_stats: MergeChangeStatsDto,
}

impl From<lix::MergeBranchReceipt> for MergeBranchReceiptDto {
    fn from(receipt: lix::MergeBranchReceipt) -> Self {
        Self {
            outcome: merge_outcome(receipt.outcome),
            target_branch_id: receipt.target_branch_id,
            source_branch_id: receipt.source_branch_id,
            base_commit_id: receipt.base_commit_id,
            target_head_before_commit_id: receipt.target_head_before_commit_id,
            source_head_before_commit_id: receipt.source_head_before_commit_id,
            target_head_after_commit_id: receipt.target_head_after_commit_id,
            created_merge_commit_id: receipt.created_merge_commit_id,
            change_stats: receipt.change_stats.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergeBranchPreviewDto {
    outcome: &'static str,
    target_branch_id: String,
    source_branch_id: String,
    base_commit_id: String,
    target_head_commit_id: String,
    source_head_commit_id: String,
    change_stats: MergeChangeStatsDto,
    conflicts: Vec<MergeConflictDto>,
}

impl From<lix::MergeBranchPreview> for MergeBranchPreviewDto {
    fn from(preview: lix::MergeBranchPreview) -> Self {
        Self {
            outcome: merge_outcome(preview.outcome),
            target_branch_id: preview.target_branch_id,
            source_branch_id: preview.source_branch_id,
            base_commit_id: preview.base_commit_id,
            target_head_commit_id: preview.target_head_commit_id,
            source_head_commit_id: preview.source_head_commit_id,
            change_stats: preview.change_stats.into(),
            conflicts: preview.conflicts.into_iter().map(Into::into).collect(),
        }
    }
}

fn merge_outcome(outcome: MergeBranchOutcome) -> &'static str {
    match outcome {
        MergeBranchOutcome::AlreadyUpToDate => "alreadyUpToDate",
        MergeBranchOutcome::FastForward => "fastForward",
        MergeBranchOutcome::MergeCommitted => "mergeCommitted",
    }
}

#[derive(Serialize)]
struct MergeChangeStatsDto {
    total: usize,
    added: usize,
    modified: usize,
    removed: usize,
}

impl From<lix::MergeChangeStats> for MergeChangeStatsDto {
    fn from(stats: lix::MergeChangeStats) -> Self {
        Self {
            total: stats.total,
            added: stats.added,
            modified: stats.modified,
            removed: stats.removed,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergeConflictDto {
    kind: &'static str,
    row_ref: String,
    file_id: Option<String>,
    target: MergeConflictSideDto,
    source: MergeConflictSideDto,
}

impl From<lix::MergeConflict> for MergeConflictDto {
    fn from(conflict: lix::MergeConflict) -> Self {
        Self {
            kind: "sameRowChanged",
            row_ref: conflict.row_ref.to_string(),
            file_id: conflict.file_id,
            target: conflict.target.into(),
            source: conflict.source.into(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MergeConflictSideDto {
    kind: &'static str,
    before_change_id: Option<String>,
    after_change_id: Option<String>,
}

impl From<lix::MergeConflictSide> for MergeConflictSideDto {
    fn from(side: lix::MergeConflictSide) -> Self {
        let kind = match side.kind {
            lix::MergeConflictChangeKind::Added => "added",
            lix::MergeConflictChangeKind::Modified => "modified",
            lix::MergeConflictChangeKind::Removed => "removed",
        };
        Self {
            kind,
            before_change_id: side.before_change_id,
            after_change_id: side.after_change_id,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct LixValueDto {
    kind: String,
    value: Option<serde_json::Value>,
    blob: Option<ByteBuf>,
}

pub(super) fn values_from_js(value: JsValue) -> Result<Vec<Value>, JsValue> {
    let values: Vec<LixValueDto> = from_js(value)?;
    values
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(lix_error_to_js)
}

pub(super) fn batch_statements_from_js(
    value: JsValue,
) -> Result<Vec<RsExecuteBatchStatement>, JsValue> {
    if !Array::is_array(&value) {
        return Err(lix_error_to_js(invalid_param(
            "executeBatch statements must be an array",
        )));
    }
    Array::from(&value)
        .iter()
        .enumerate()
        .map(|(index, statement)| {
            let sql = Reflect::get(&statement, &JsValue::from_str("sql"))
                .ok()
                .and_then(|value| value.as_string())
                .ok_or_else(|| {
                    lix_error_to_js(invalid_param(format!(
                        "executeBatch statement at index {index} must include SQL text"
                    )))
                })?;
            let params = Reflect::get(&statement, &JsValue::from_str("params"))?;
            let params = if params.is_undefined() {
                Vec::new()
            } else {
                values_from_js(params)?
            };
            let label = Reflect::get(&statement, &JsValue::from_str("label"))?;
            let label = if label.is_undefined() {
                None
            } else {
                Some(label.as_string().ok_or_else(|| {
                    lix_error_to_js(invalid_param(format!(
                        "executeBatch statement at index {index} label must be a string"
                    )))
                })?)
            };
            Ok(RsExecuteBatchStatement { sql, params, label })
        })
        .collect()
}

impl TryFrom<LixValueDto> for Value {
    type Error = LixError;

    fn try_from(value: LixValueDto) -> Result<Self, Self::Error> {
        match value.kind.as_str() {
            "null" => Ok(Self::Null),
            "boolean" => value
                .value
                .and_then(|value| value.as_bool())
                .map(Self::Boolean)
                .ok_or_else(|| invalid_param("boolean value must be a boolean")),
            "integer" => value
                .value
                .and_then(|value| value.as_i64())
                .map(Self::Integer)
                .ok_or_else(|| invalid_param("integer value must be an integer")),
            "real" => value
                .value
                .and_then(|value| value.as_f64())
                .filter(|value| value.is_finite())
                .map(Self::Real)
                .ok_or_else(|| invalid_param("real value must be a finite number")),
            "text" => value
                .value
                .and_then(|value| value.as_str().map(ToOwned::to_owned))
                .map(Self::Text)
                .ok_or_else(|| invalid_param("text value must be a string")),
            "jsonb" => Ok(Self::Jsonb(
                value.value.unwrap_or(serde_json::Value::Null).into(),
            )),
            "row_ref" => {
                let encoded = value
                    .value
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| invalid_param("row_ref value must be a string"))?;
                Ok(Self::RowRef(lix::RowRef::from_encoded(encoded)?))
            }
            "timestamptz" => {
                let raw = value
                    .value
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| invalid_param("timestamptz value must be an RFC 3339 string"))?;
                let parsed = chrono::DateTime::parse_from_rfc3339(&raw).map_err(|error| {
                    invalid_param(format!("timestamptz value is invalid: {error}"))
                })?;
                Ok(Self::Timestamptz(parsed.timestamp_micros()))
            }
            "blob" => value
                .blob
                .map(|bytes| Self::Blob(bytes.into_vec().into()))
                .ok_or_else(|| invalid_param("blob value must include bytes")),
            other => Err(invalid_param(format!("unsupported LixValue kind: {other}"))),
        }
    }
}

impl TryFrom<&Value> for LixValueDto {
    type Error = LixError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let (kind, value, blob) = match value {
            Value::Null => ("null", Some(serde_json::Value::Null), None),
            Value::Boolean(value) => ("boolean", Some(serde_json::json!(value)), None),
            Value::Integer(value) => ("integer", Some(serde_json::json!(value)), None),
            Value::Real(value) if value.is_finite() => {
                ("real", Some(serde_json::json!(value)), None)
            }
            Value::Real(_) => return Err(invalid_param("cannot encode non-finite real value")),
            Value::Text(value) => ("text", Some(serde_json::json!(value)), None),
            Value::Jsonb(value) => ("jsonb", Some(value.to_value()), None),
            Value::RowRef(value) => ("row_ref", Some(serde_json::json!(value.as_str())), None),
            Value::Timestamptz(value) => {
                let value = chrono::DateTime::from_timestamp_micros(*value)
                    .ok_or_else(|| invalid_param("timestamptz is out of range"))?;
                (
                    "timestamptz",
                    Some(serde_json::Value::String(
                        value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    )),
                    None,
                )
            }
            Value::Blob(value) => ("blob", None, Some(ByteBuf::from(value.to_vec()))),
        };
        Ok(Self {
            kind: kind.to_string(),
            value,
            blob,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExecuteResultDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    statement_index: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    columns: Vec<ExecuteColumnDto>,
    rows: Vec<Vec<LixValueDto>>,
    rows_affected: f64,
    notices: Vec<LixNoticeDto>,
}

#[derive(Serialize)]
struct ExecuteColumnDto {
    name: String,
    #[serde(rename = "type")]
    column_type: lix::ResultColumnType,
}

impl TryFrom<RsExecuteResult> for ExecuteResultDto {
    type Error = LixError;

    fn try_from(result: RsExecuteResult) -> Result<Self, Self::Error> {
        let rows = result
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(LixValueDto::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            statement_index: result.statement_index().map(js_index),
            label: result.label().map(str::to_owned),
            columns: result
                .columns()
                .iter()
                .cloned()
                .zip(result.column_types().iter().copied())
                .map(|(name, column_type)| ExecuteColumnDto { name, column_type })
                .collect(),
            rows,
            rows_affected: js_number(result.rows_affected()),
            notices: result
                .notices()
                .iter()
                .map(|notice| LixNoticeDto {
                    code: notice.code.clone(),
                    message: notice.message.clone(),
                    hint: notice.hint.clone(),
                })
                .collect(),
        })
    }
}

#[derive(Serialize)]
struct LixNoticeDto {
    code: String,
    message: String,
    hint: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ObserveEventDto {
    sequence: f64,
    mutation_sequence: f64,
    rows: ExecuteResultDto,
}

pub(super) fn execute_result_to_js(result: RsExecuteResult) -> Result<JsValue, JsValue> {
    let result = ExecuteResultDto::try_from(result).map_err(lix_error_to_js)?;
    to_js(&result)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "the public JavaScript SDK represents counts and sequences as numbers"
)]
fn js_number(value: u64) -> f64 {
    value as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "WASM32 statement indexes are exactly representable as JavaScript numbers"
)]
fn js_index(value: usize) -> f64 {
    value as f64
}

fn invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message.into())
}

fn transaction_closed_error() -> JsValue {
    lix_error_to_js(LixError::new(
        "LIX_INVALID_TRANSACTION_STATE",
        "Lix transaction is closed",
    ))
}

pub(super) fn observe_next_in_flight_error() -> JsValue {
    lix_error_to_js(
        LixError::new(
            "LIX_OBSERVE_NEXT_IN_FLIGHT",
            "ObserveEvents.next() is already in flight",
        )
        .with_hint("Await the pending next() call before calling next() again."),
    )
}

pub(super) fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| js_bridge_error(format!("invalid JavaScript value: {error}")))
}

pub(super) fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_missing_as_null(true),
        )
        .map_err(|error| js_bridge_error(format!("could not encode JavaScript value: {error}")))
}

fn js_bridge_error(message: impl AsRef<str>) -> JsValue {
    js_sys::Error::new(message.as_ref()).into()
}

pub(super) fn lix_error_to_js(error: LixError) -> JsValue {
    let js_error = js_sys::Error::new(&error.message);
    js_error.set_name("LixError");
    let object: &JsValue = js_error.as_ref();
    let _ = Reflect::set(
        object,
        &JsValue::from_str("code"),
        &JsValue::from_str(&error.code),
    );
    if let Some(hint) = error.hint {
        let _ = Reflect::set(
            object,
            &JsValue::from_str("hint"),
            &JsValue::from_str(&hint),
        );
    }
    if let Some(details) = error.details {
        if let Some(status) = details.get("httpStatus").and_then(|value| value.as_u64()) {
            let status = JsValue::from_f64(status as f64);
            let _ = Reflect::set(object, &JsValue::from_str("status"), &status);
            let _ = Reflect::set(object, &JsValue::from_str("httpStatus"), &status);
        }
        if let Ok(details) = to_js(&details) {
            let _ = Reflect::set(object, &JsValue::from_str("details"), &details);
        }
    }
    js_error.into()
}

#[cfg(test)]
mod value_kind_tests {
    use super::*;

    #[test]
    fn legacy_json_and_timestamp_value_kinds_are_rejected() {
        for kind in ["json", "timestamp"] {
            let error = Value::try_from(LixValueDto {
                kind: kind.to_owned(),
                value: Some(serde_json::Value::Null),
                blob: None,
            })
            .expect_err("legacy WebAssembly value kind must not decode");
            assert!(error.message.contains("unsupported LixValue kind"));
        }
    }
}
