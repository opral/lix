use lix::telemetry::{CallbackTelemetrySink, SpanContext, TelemetrySink, instrument_remote_parent};
use lix::{
    CreateBranchOptions as RsCreateBranchOptions, CreateBranchReceipt, CreateCheckpointReceipt,
    ExecuteBatchStatement as RsExecuteBatchStatement, ExecuteResult as RsExecuteResult,
    Lix as RsLix, LixError, LixTransaction as RsLixTransaction, Memory,
    MergeBranchOptions as RsMergeBranchOptions, MergeBranchOutcome, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, ObserveEvent as RsObserveEvent,
    ObserveEvents as RsObserveEvents, OpenPhase, OpenProgress, OpenProgressSink, OpenReport,
    RedoReceipt, ServerOptions, SwitchBranchOptions as RsSwitchBranchOptions, SwitchBranchReceipt,
    UndoReceipt, Value, open_lix,
};
use lix_storage_filesystem::FilesystemStorage;
use napi::JsDeferred;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::mpsc::{self, Sender, SyncSender, TrySendError};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::thread;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;

type JsTelemetryDispatch = ThreadsafeFunction<String, (), String, Status, false>;
type SharedJsTelemetryDispatch = Arc<JsTelemetryDispatch>;
type JsOpenProgressDispatch = ThreadsafeFunction<String, (), String, Status, false>;
type SharedJsOpenProgressDispatch = Arc<JsOpenProgressDispatch>;

// SQL command and observation actors can compose catalog, filesystem, plugin,
// and live-state async futures before their first suspension point. They must
// not inherit the platform's small default thread stack for that graph.
const NATIVE_ENGINE_ACTOR_STACK_SIZE: usize = 32 * 1024 * 1024;
const NATIVE_SNAPSHOT_EXPORT_HANDOFF_CAPACITY: usize = 1;

fn optional_telemetry_dispatch(
    dispatch: Option<Function<'_, String, ()>>,
) -> Result<Option<SharedJsTelemetryDispatch>> {
    dispatch
        .map(|dispatch| dispatch.build_threadsafe_function().build().map(Arc::new))
        .transpose()
}

fn optional_open_progress_dispatch(
    dispatch: Option<Function<'_, String, ()>>,
) -> Result<Option<SharedJsOpenProgressDispatch>> {
    dispatch
        .map(|dispatch| dispatch.build_threadsafe_function().build().map(Arc::new))
        .transpose()
}

#[expect(missing_debug_implementations)]
#[napi(js_name = "Lix")]
pub struct NativeLix {
    actor: NativeLixActor,
    telemetry_parent: Option<PendingTelemetryParent>,
    open_report: NativeOpenReport,
}

enum NativeLixInner {
    Memory(RsLix<Memory>),
    FilesystemStorage(
        RsLix<FilesystemStorage>,
        FilesystemStorage,
        Arc<AtomicUsize>,
    ),
}

enum NativeSnapshotExportBuilder {
    Memory(lix::snapshot::SnapshotExportBuilder<Memory>),
    FilesystemStorage(lix::snapshot::SnapshotExportBuilder<FilesystemStorage>),
}

impl NativeSnapshotExportBuilder {
    async fn write_to<W>(self, writer: &mut W) -> std::result::Result<(), LixError>
    where
        W: futures_lite::io::AsyncWrite + Unpin + Send + ?Sized,
    {
        match self {
            Self::Memory(builder) => builder.write_to(writer).await?,
            Self::FilesystemStorage(builder) => builder.write_to(writer).await?,
        };
        Ok(())
    }
}

enum NativeLixTransactionInner {
    Memory(RsLixTransaction<Memory>),
    FilesystemStorage(RsLixTransaction<FilesystemStorage>),
}

enum NativeObserveEventsInner {
    Memory(RsObserveEvents<Memory>),
    FilesystemStorage(RsObserveEvents<FilesystemStorage>),
}

#[napi(object)]
#[derive(Debug)]
pub struct NativeExecuteOptions {
    #[napi(js_name = "originKey")]
    pub origin_key: Option<String>,
}

#[napi(object)]
#[derive(Debug)]
pub struct NativeOpenAnotherSessionOptions {
    #[napi(js_name = "branchId")]
    pub branch_id: Option<String>,
    #[napi(js_name = "accountId")]
    pub account_id: Option<String>,
}

#[napi(object)]
#[derive(Clone, Copy, Debug)]
pub struct NativeOpenMigrationReport {
    #[napi(js_name = "fromFormat")]
    pub from_format: u32,
    #[napi(js_name = "toFormat")]
    pub to_format: u32,
}

#[napi(object)]
#[derive(Clone, Copy, Debug)]
pub struct NativeOpenReport {
    pub format: u32,
    pub initialized: bool,
    pub migration: Option<NativeOpenMigrationReport>,
}

impl From<&OpenReport> for NativeOpenReport {
    fn from(report: &OpenReport) -> Self {
        Self {
            format: report.format,
            initialized: report.initialized,
            migration: report.migration.map(|migration| NativeOpenMigrationReport {
                from_format: migration.from_format,
                to_format: migration.to_format,
            }),
        }
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

#[napi(object)]
#[expect(missing_debug_implementations)]
pub struct NativeExecuteBatchStatement {
    pub sql: String,
    pub params: Option<Vec<LixValue>>,
    pub label: Option<String>,
}

#[derive(Clone)]
struct NativeLixActor {
    commands: Sender<QueuedLixCommand>,
    closed: Arc<AtomicBool>,
    send_lock: Arc<Mutex<()>>,
    next_transaction_id: Arc<AtomicU64>,
    telemetry_parent: Option<PendingTelemetryParent>,
}

struct NativeLixActorState {
    lix: NativeLixInner,
    transactions: HashMap<u64, NativeLixTransactionInner>,
    snapshot_exports: SyncSender<NativeSnapshotExportJob>,
    snapshot_export_active: Arc<AtomicBool>,
}

struct NativeSnapshotExportJob {
    builder: NativeSnapshotExportBuilder,
    sender: async_channel::Sender<NativeSnapshotMessage>,
    completion: Arc<NativeSnapshotExportCompletion>,
    telemetry_parent: Option<SpanContext>,
    active: Arc<AtomicBool>,
}

#[derive(Default)]
struct NativeSnapshotExportCompletion {
    finished: Mutex<bool>,
    changed: Condvar,
}

impl NativeSnapshotExportCompletion {
    fn finish(&self) {
        let mut finished = self
            .finished
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *finished = true;
        self.changed.notify_all();
    }

    fn wait(&self) {
        let mut finished = self
            .finished
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !*finished {
            finished = self
                .changed
                .wait(finished)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

type NativeResult<T> = std::result::Result<T, LixError>;
type NativeResolver<T> = Box<dyn FnOnce(Env) -> Result<T> + Send>;
type NativeDeferred<T> = JsDeferred<T, NativeResolver<T>>;
type NativeExecuteDeferred = NativeDeferred<ExecuteResult>;
type NativeExecuteBatchDeferred = NativeDeferred<Vec<ExecuteResult>>;
type NativeTransactionDeferred = NativeDeferred<NativeLixTransaction>;
type NativeLixDeferred = NativeDeferred<NativeLix>;
type NativeStringDeferred = NativeDeferred<String>;
type NativeCreateBranchDeferred = NativeDeferred<CreateBranchReceiptDto>;
type NativeCreateCheckpointDeferred = NativeDeferred<CreateCheckpointReceiptDto>;
type NativeUndoDeferred = NativeDeferred<UndoReceiptDto>;
type NativeRedoDeferred = NativeDeferred<RedoReceiptDto>;
type NativeSwitchBranchDeferred = NativeDeferred<SwitchBranchReceiptDto>;
type NativeMergePreviewDeferred = NativeDeferred<MergeBranchPreviewDto>;
type NativeMergeReceiptDeferred = NativeDeferred<MergeBranchReceiptDto>;
type NativeUnitDeferred = NativeDeferred<()>;
type PendingTelemetryParent = Arc<Mutex<Option<SpanContext>>>;

enum LixCommand {
    OpenAnotherSession {
        options: NativeOpenAnotherSessionOptions,
        telemetry_parent: Option<PendingTelemetryParent>,
        deferred: NativeLixDeferred,
    },
    Execute {
        sql: String,
        params: Vec<Value>,
        options: Option<String>,
        deferred: NativeExecuteDeferred,
    },
    ExecuteBatch {
        statements: Vec<RsExecuteBatchStatement>,
        options: Option<String>,
        deferred: NativeExecuteBatchDeferred,
    },
    BeginTransaction {
        transaction_id: u64,
        actor: NativeLixActor,
        deferred: NativeTransactionDeferred,
    },
    ActiveBranchId(NativeStringDeferred),
    ActiveAccountId(NativeStringDeferred),
    CreateBranch {
        options: RsCreateBranchOptions,
        deferred: NativeCreateBranchDeferred,
    },
    CreateCheckpoint(NativeCreateCheckpointDeferred),
    Undo(NativeUndoDeferred),
    Redo(NativeRedoDeferred),
    SwitchBranch {
        options: RsSwitchBranchOptions,
        deferred: NativeSwitchBranchDeferred,
    },
    ImportFilesystemPaths {
        paths: Vec<String>,
        deferred: NativeUnitDeferred,
    },
    MergeBranchPreview {
        options: MergeBranchPreviewOptions,
        deferred: NativeMergePreviewDeferred,
    },
    MergeBranch {
        options: RsMergeBranchOptions,
        deferred: NativeMergeReceiptDeferred,
    },
    SyncDiskToLix(NativeUnitDeferred),
    ExportSnapshot {
        sender: async_channel::Sender<NativeSnapshotMessage>,
        completion: Arc<NativeSnapshotExportCompletion>,
    },
    Close(NativeUnitDeferred),
    Observe {
        sql: String,
        params: Vec<Value>,
        telemetry_parent: Option<PendingTelemetryParent>,
        deferred: NativeDeferred<NativeObserveEvents>,
    },
    TransactionExecute {
        transaction_id: u64,
        sql: String,
        params: Vec<Value>,
        options: Option<String>,
        deferred: NativeExecuteDeferred,
    },
    TransactionCommit {
        transaction_id: u64,
        deferred: NativeUnitDeferred,
    },
    TransactionRollback {
        transaction_id: u64,
        deferred: NativeUnitDeferred,
    },
    TransactionAbandon {
        transaction_id: u64,
    },
}

struct QueuedLixCommand {
    command: LixCommand,
    telemetry_parent: Option<SpanContext>,
}

const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 64 * 1024;

enum NativeSnapshotMessage {
    Chunk(Vec<u8>),
    Done,
    Error(LixError),
}

enum NativeSnapshotInputMessage {
    Chunk(Vec<u8>),
    Done,
}

struct NativeSnapshotReader {
    receiver: Pin<Box<async_channel::Receiver<NativeSnapshotInputMessage>>>,
    chunk: Vec<u8>,
    offset: usize,
    finished: bool,
}

impl NativeSnapshotReader {
    fn new(receiver: async_channel::Receiver<NativeSnapshotInputMessage>) -> Self {
        Self {
            receiver: Box::pin(receiver),
            chunk: Vec::new(),
            offset: 0,
            finished: false,
        }
    }
}

impl futures_lite::io::AsyncRead for NativeSnapshotReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
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
            match this.receiver.recv_blocking() {
                Ok(NativeSnapshotInputMessage::Chunk(chunk)) => {
                    this.chunk = chunk;
                    this.offset = 0;
                }
                Ok(NativeSnapshotInputMessage::Done) => this.finished = true,
                Err(_) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "snapshot input was canceled",
                    )));
                }
            }
        }
    }
}

enum NativeSnapshotSource {
    Bytes(futures_lite::io::Cursor<Vec<u8>>),
    Stream(NativeSnapshotReader),
}

impl futures_lite::io::AsyncRead for NativeSnapshotSource {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Bytes(cursor) => Pin::new(cursor).poll_read(cx, output),
            Self::Stream(reader) => Pin::new(reader).poll_read(cx, output),
        }
    }
}

struct NativeSnapshotWriter {
    sender: async_channel::Sender<NativeSnapshotMessage>,
    buffer: Vec<u8>,
}

impl NativeSnapshotWriter {
    fn new(sender: async_channel::Sender<NativeSnapshotMessage>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(SNAPSHOT_STREAM_CHUNK_BYTES),
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::replace(
            &mut self.buffer,
            Vec::with_capacity(SNAPSHOT_STREAM_CHUNK_BYTES),
        );
        self.sender
            .send_blocking(NativeSnapshotMessage::Chunk(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "snapshot export was canceled"))
    }
}

impl futures_lite::io::AsyncWrite for NativeSnapshotWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let available = SNAPSHOT_STREAM_CHUNK_BYTES - self.buffer.len();
        let written = available.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..written]);
        if self.buffer.len() == SNAPSHOT_STREAM_CHUNK_BYTES {
            self.send_buffer()?;
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.send_buffer())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

#[napi]
#[expect(missing_debug_implementations)]
pub struct NativeSnapshotExport {
    receiver: async_channel::Receiver<NativeSnapshotMessage>,
    completion: Arc<NativeSnapshotExportCompletion>,
    canceled: Arc<AtomicBool>,
}

#[expect(missing_debug_implementations)]
pub struct NativeSnapshotNextTask {
    receiver: async_channel::Receiver<NativeSnapshotMessage>,
    completion: Arc<NativeSnapshotExportCompletion>,
}

impl Task for NativeSnapshotNextTask {
    type Output = std::result::Result<Option<Buffer>, LixError>;
    type JsValue = Option<Buffer>;

    fn compute(&mut self) -> Result<Self::Output> {
        Ok(match self.receiver.recv_blocking() {
            Ok(NativeSnapshotMessage::Chunk(chunk)) => Ok(Some(Buffer::from(chunk))),
            Ok(NativeSnapshotMessage::Done) | Err(_) => {
                self.completion.wait();
                Ok(None)
            }
            Ok(NativeSnapshotMessage::Error(error)) => {
                self.completion.wait();
                Err(error)
            }
        })
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

#[napi]
impl NativeSnapshotExport {
    #[napi]
    pub fn next(&self) -> AsyncTask<NativeSnapshotNextTask> {
        AsyncTask::new(NativeSnapshotNextTask {
            receiver: self.receiver.clone(),
            completion: self.completion.clone(),
        })
    }

    #[napi]
    pub fn cancel(&self) -> Result<AsyncTask<NativeSnapshotExportCancelTask>> {
        if self.canceled.swap(true, Ordering::SeqCst) {
            return Err(Error::from_reason("snapshot export is already canceled"));
        }
        // Close before scheduling the completion wait. Both next() and cancel()
        // use Node's worker pool; deferring this close to the cancel task can
        // deadlock when a blocking next() occupies the last pool thread.
        self.receiver.close();
        Ok(AsyncTask::new(NativeSnapshotExportCancelTask {
            completion: self.completion.clone(),
        }))
    }
}

#[expect(missing_debug_implementations)]
pub struct NativeSnapshotExportCancelTask {
    completion: Arc<NativeSnapshotExportCompletion>,
}

impl Task for NativeSnapshotExportCancelTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.completion.wait();
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

impl Drop for NativeSnapshotExport {
    fn drop(&mut self) {
        self.receiver.close();
    }
}

#[napi]
#[expect(missing_debug_implementations)]
pub struct NativeSnapshotRestore {
    input: async_channel::Sender<NativeSnapshotInputMessage>,
    result: async_channel::Receiver<NativeResult<NativeLix>>,
    finished: Arc<AtomicBool>,
    complete: Arc<AtomicBool>,
}

#[expect(missing_debug_implementations)]
pub struct NativeSnapshotWriteTask {
    input: async_channel::Sender<NativeSnapshotInputMessage>,
    chunk: Option<Vec<u8>>,
}

impl Task for NativeSnapshotWriteTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.input
            .send_blocking(NativeSnapshotInputMessage::Chunk(
                self.chunk.take().unwrap_or_default(),
            ))
            .map_err(|_| Error::from_reason("snapshot restore is no longer accepting input"))
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

#[expect(missing_debug_implementations)]
pub struct NativeSnapshotFinishTask {
    input: async_channel::Sender<NativeSnapshotInputMessage>,
    result: async_channel::Receiver<NativeResult<NativeLix>>,
}

impl Task for NativeSnapshotFinishTask {
    type Output = NativeResult<NativeLix>;
    type JsValue = NativeLix;

    fn compute(&mut self) -> Result<Self::Output> {
        // A decoder may reject and drop the receiver before EOF is sent. The
        // semantic restore result is authoritative; a failed EOF send is only
        // evidence that the result is already ready.
        let _ = self.input.send_blocking(NativeSnapshotInputMessage::Done);
        self.input.close();
        Ok(self.result.recv_blocking().unwrap_or_else(|_| {
            Err(LixError::unknown(
                "snapshot restore stopped without returning a result",
            ))
        }))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

#[expect(missing_debug_implementations)]
pub struct NativeSnapshotCancelTask {
    input: async_channel::Sender<NativeSnapshotInputMessage>,
    result: async_channel::Receiver<NativeResult<NativeLix>>,
}

impl Task for NativeSnapshotCancelTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.input.close();
        // Cancellation is completion-aware so callers can immediately reuse a
        // destination after the restore task releases its storage claims.
        let _ = self.result.recv_blocking();
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

#[napi]
impl NativeSnapshotRestore {
    #[napi(js_name = "isComplete")]
    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::SeqCst)
    }

    #[napi]
    pub fn write(&self, chunk: Uint8Array) -> Result<AsyncTask<NativeSnapshotWriteTask>> {
        if self.finished.load(Ordering::SeqCst) {
            return Err(Error::from_reason("snapshot restore input is closed"));
        }
        Ok(AsyncTask::new(NativeSnapshotWriteTask {
            input: self.input.clone(),
            chunk: Some(chunk.to_vec()),
        }))
    }

    #[napi]
    pub fn finish(&self) -> Result<AsyncTask<NativeSnapshotFinishTask>> {
        if self.finished.swap(true, Ordering::SeqCst) {
            return Err(Error::from_reason(
                "snapshot restore input is already closed",
            ));
        }
        Ok(AsyncTask::new(NativeSnapshotFinishTask {
            input: self.input.clone(),
            result: self.result.clone(),
        }))
    }

    #[napi]
    pub fn cancel(&self) -> Result<AsyncTask<NativeSnapshotCancelTask>> {
        if self.finished.swap(true, Ordering::SeqCst) {
            return Err(Error::from_reason(
                "snapshot restore input is already closed",
            ));
        }
        Ok(AsyncTask::new(NativeSnapshotCancelTask {
            input: self.input.clone(),
            result: self.result.clone(),
        }))
    }
}

impl Drop for NativeSnapshotRestore {
    fn drop(&mut self) {
        self.input.close();
    }
}

fn take_pending_telemetry_parent(parent: &PendingTelemetryParent) -> Option<SpanContext> {
    parent
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .take()
}

impl NativeLixActor {
    fn start(
        lix: NativeLixInner,
        telemetry_parent: Option<PendingTelemetryParent>,
    ) -> Result<Self> {
        let (commands, receiver) = mpsc::channel();
        let (snapshot_exports, snapshot_export_receiver) =
            mpsc::sync_channel(NATIVE_SNAPSHOT_EXPORT_HANDOFF_CAPACITY);
        let snapshot_export_active = Arc::new(AtomicBool::new(false));
        thread::Builder::new()
            .name("lix-snapshot-export".to_string())
            .stack_size(NATIVE_ENGINE_ACTOR_STACK_SIZE)
            .spawn(move || run_native_snapshot_export_executor(snapshot_export_receiver))
            .map_err(to_napi_error)?;
        let actor = Self {
            commands,
            closed: Arc::new(AtomicBool::new(false)),
            send_lock: Arc::new(Mutex::new(())),
            next_transaction_id: Arc::new(AtomicU64::new(1)),
            telemetry_parent,
        };
        let actor_closed = Arc::clone(&actor.closed);
        let actor_send_lock = Arc::clone(&actor.send_lock);
        thread::Builder::new()
            .name("lix-native".to_string())
            .stack_size(NATIVE_ENGINE_ACTOR_STACK_SIZE)
            .spawn(move || {
                run_lix_actor(
                    lix,
                    receiver,
                    actor_closed,
                    actor_send_lock,
                    snapshot_exports,
                    snapshot_export_active,
                )
            })
            .map_err(to_napi_error)?;
        Ok(actor)
    }

    fn next_transaction_id(&self) -> u64 {
        self.next_transaction_id.fetch_add(1, Ordering::SeqCst)
    }

    fn send_with_deferred<T>(
        &self,
        deferred: NativeDeferred<T>,
        command: impl FnOnce(NativeDeferred<T>) -> LixCommand,
    ) where
        T: ToNapiValue + Send + 'static,
    {
        let Ok(_send_guard) = self.send_lock.lock() else {
            settle_deferred(deferred, Err(lix_closed_error()));
            return;
        };
        if self.closed.load(Ordering::SeqCst) {
            settle_deferred(deferred, Err(lix_closed_error()));
            return;
        }
        let queued = QueuedLixCommand {
            command: command(deferred),
            telemetry_parent: self
                .telemetry_parent
                .as_ref()
                .and_then(take_pending_telemetry_parent),
        };
        match self.commands.send(queued) {
            Ok(()) => {}
            Err(error) => {
                settle_command_after_close(error.0.command);
            }
        }
    }

    fn send_snapshot_export(
        &self,
        sender: async_channel::Sender<NativeSnapshotMessage>,
        completion: Arc<NativeSnapshotExportCompletion>,
    ) {
        let Ok(_send_guard) = self.send_lock.lock() else {
            let _ = sender.send_blocking(NativeSnapshotMessage::Error(lix_closed_error()));
            completion.finish();
            return;
        };
        if self.closed.load(Ordering::SeqCst) {
            let _ = sender.send_blocking(NativeSnapshotMessage::Error(lix_closed_error()));
            completion.finish();
            return;
        }
        let queued = QueuedLixCommand {
            command: LixCommand::ExportSnapshot {
                sender: sender.clone(),
                completion: completion.clone(),
            },
            telemetry_parent: self
                .telemetry_parent
                .as_ref()
                .and_then(take_pending_telemetry_parent),
        };
        if self.commands.send(queued).is_err() {
            let _ = sender.send_blocking(NativeSnapshotMessage::Error(lix_closed_error()));
            completion.finish();
        }
    }
}

impl NativeLixActor {
    fn abandon_transaction(&self, transaction_id: u64) {
        let _ = self.commands.send(QueuedLixCommand {
            command: LixCommand::TransactionAbandon { transaction_id },
            telemetry_parent: self
                .telemetry_parent
                .as_ref()
                .and_then(take_pending_telemetry_parent),
        });
    }
}

fn settle_deferred<T>(deferred: NativeDeferred<T>, result: NativeResult<T>)
where
    T: ToNapiValue + Send + 'static,
{
    deferred.resolve(Box::new(move |env| {
        result.map_err(|error| lix_error_to_napi_error(&env, error))
    }));
}

fn run_lix_actor(
    lix: NativeLixInner,
    receiver: mpsc::Receiver<QueuedLixCommand>,
    closed: Arc<AtomicBool>,
    send_lock: Arc<Mutex<()>>,
    snapshot_exports: SyncSender<NativeSnapshotExportJob>,
    snapshot_export_active: Arc<AtomicBool>,
) {
    let rt = match Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(error) => {
            closed.store(true, Ordering::SeqCst);
            reject_pending_lix_commands(receiver, error);
            return;
        }
    };
    let mut state = Some(NativeLixActorState {
        lix,
        transactions: HashMap::new(),
        snapshot_exports,
        snapshot_export_active,
    });

    while let Ok(queued) = receiver.recv() {
        let Some(open_state) = state.as_mut() else {
            settle_command_after_close(queued.command);
            continue;
        };
        if closed.load(Ordering::SeqCst) {
            drop(state.take());
            settle_command_after_close(queued.command);
            drain_commands_after_close(&receiver, &send_lock);
            break;
        }
        if handle_lix_command(
            &rt,
            open_state,
            &closed,
            queued.command,
            queued.telemetry_parent,
        ) {
            drop(state.take());
            drain_commands_after_close(&receiver, &send_lock);
            break;
        }
    }
    closed.store(true, Ordering::SeqCst);
}

fn drain_commands_after_close(receiver: &mpsc::Receiver<QueuedLixCommand>, send_lock: &Mutex<()>) {
    let Ok(_send_guard) = send_lock.lock() else {
        return;
    };
    for queued in receiver.try_iter() {
        settle_command_after_close(queued.command);
    }
}

fn reject_pending_lix_commands(receiver: mpsc::Receiver<QueuedLixCommand>, error: io::Error) {
    while let Ok(queued) = receiver.recv() {
        match queued.command {
            LixCommand::Execute { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::OpenAnotherSession { deferred, .. } => {
                deferred.reject(to_napi_error(&error))
            }
            LixCommand::ExecuteBatch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::BeginTransaction { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::ActiveBranchId(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::ActiveAccountId(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::CreateBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::CreateCheckpoint(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::Undo(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::Redo(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::SwitchBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::MergeBranchPreview { deferred, .. } => {
                deferred.reject(to_napi_error(&error));
            }
            LixCommand::MergeBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::ExportSnapshot { sender, completion } => {
                let _ = sender.send_blocking(NativeSnapshotMessage::Error(LixError::unknown(
                    error.to_string(),
                )));
                completion.finish();
            }
            LixCommand::SyncDiskToLix(deferred)
            | LixCommand::Close(deferred)
            | LixCommand::ImportFilesystemPaths { deferred, .. }
            | LixCommand::TransactionCommit { deferred, .. }
            | LixCommand::TransactionRollback { deferred, .. } => {
                deferred.reject(to_napi_error(&error));
            }
            LixCommand::Observe { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::TransactionExecute { deferred, .. } => {
                deferred.reject(to_napi_error(&error));
            }
            LixCommand::TransactionAbandon { .. } => {}
        }
    }
}

fn handle_lix_command(
    rt: &Runtime,
    state: &mut NativeLixActorState,
    closed: &AtomicBool,
    command: LixCommand,
    telemetry_parent: Option<SpanContext>,
) -> bool {
    macro_rules! block_on {
        ($future:expr) => {
            rt.block_on(instrument_remote_parent(telemetry_parent.clone(), $future))
        };
    }
    match command {
        LixCommand::OpenAnotherSession {
            options,
            telemetry_parent,
            deferred,
        } => {
            let result = block_on!(state.lix.open_another_session(options))
                .and_then(|lix| NativeLix::new(lix, telemetry_parent));
            settle_deferred(deferred, result);
            false
        }
        LixCommand::Execute {
            sql,
            params,
            options,
            deferred,
        } => {
            let result = block_on!(state.lix.execute(&sql, &params, options))
                .and_then(ExecuteResult::try_from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ExecuteBatch {
            statements,
            options,
            deferred,
        } => {
            let result =
                block_on!(state.lix.execute_batch(&statements, options)).and_then(|results| {
                    results
                        .into_iter()
                        .map(ExecuteResult::try_from)
                        .collect::<std::result::Result<Vec<_>, _>>()
                });
            settle_deferred(deferred, result);
            false
        }
        LixCommand::BeginTransaction {
            transaction_id,
            actor,
            deferred,
        } => {
            let result = block_on!(state.lix.begin_transaction()).map(|transaction| {
                state.transactions.insert(transaction_id, transaction);
                NativeLixTransaction::new(actor, transaction_id)
            });
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ActiveBranchId(deferred) => {
            let result = block_on!(state.lix.active_branch_id());
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ActiveAccountId(deferred) => {
            settle_deferred(deferred, Ok(state.lix.active_account_id().to_string()));
            false
        }
        LixCommand::CreateBranch { options, deferred } => {
            let result =
                block_on!(state.lix.create_branch(options)).map(CreateBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::CreateCheckpoint(deferred) => {
            let result =
                block_on!(state.lix.create_checkpoint()).map(CreateCheckpointReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::Undo(deferred) => {
            let result = block_on!(state.lix.undo()).map(UndoReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::Redo(deferred) => {
            let result = block_on!(state.lix.redo()).map(RedoReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::SwitchBranch { options, deferred } => {
            let result =
                block_on!(state.lix.switch_branch(options)).map(SwitchBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ImportFilesystemPaths { paths, deferred } => {
            let result = block_on!(state.lix.import_filesystem_paths(paths));
            settle_deferred(deferred, result);
            false
        }
        LixCommand::MergeBranchPreview { options, deferred } => {
            let result =
                block_on!(state.lix.merge_branch_preview(options)).map(MergeBranchPreviewDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::MergeBranch { options, deferred } => {
            let result =
                block_on!(state.lix.merge_branch(options)).map(MergeBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::SyncDiskToLix(deferred) => {
            let result = block_on!(state.lix.sync_disk_to_lix());
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ExportSnapshot { sender, completion } => {
            if state.snapshot_export_active.swap(true, Ordering::SeqCst) {
                finish_native_snapshot_export(
                    sender,
                    completion,
                    Err(LixError::new(
                        "LIX_ERROR_SNAPSHOT_EXPORT_BUSY",
                        "a native snapshot export is already active; finish or cancel it before starting another",
                    )),
                );
                return false;
            }
            let job = NativeSnapshotExportJob {
                builder: state.lix.snapshot_export_builder(),
                sender,
                completion,
                telemetry_parent,
                active: Arc::clone(&state.snapshot_export_active),
            };
            match state.snapshot_exports.try_send(job) {
                Ok(()) => {}
                Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => {
                    job.active.store(false, Ordering::SeqCst);
                    finish_native_snapshot_export(
                        job.sender,
                        job.completion,
                        Err(LixError::unknown(
                            "native snapshot export executor is unavailable",
                        )),
                    );
                }
            }
            false
        }
        LixCommand::Close(deferred) => {
            let result = block_on!(state.lix.close());
            let should_drop_state = result.is_ok();
            if result.is_ok() {
                closed.store(true, Ordering::SeqCst);
            }
            settle_deferred(deferred, result);
            should_drop_state
        }
        LixCommand::Observe {
            sql,
            params,
            telemetry_parent,
            deferred,
        } => {
            let result = block_on!(async {
                state.lix.observe(&sql, &params).and_then(|events| {
                    NativeObserveEvents::new(events, telemetry_parent).map_err(|error| {
                        LixError::unknown(format!("failed to start observe actor: {error}"))
                    })
                })
            });
            settle_deferred(deferred, result);
            false
        }
        LixCommand::TransactionExecute {
            transaction_id,
            sql,
            params,
            options,
            deferred,
        } => {
            let result = state.transactions.get_mut(&transaction_id).map_or_else(
                || Err(transaction_closed_error()),
                |transaction| {
                    block_on!(transaction.execute(&sql, &params, options))
                        .and_then(ExecuteResult::try_from)
                },
            );
            settle_deferred(deferred, result);
            false
        }
        LixCommand::TransactionCommit {
            transaction_id,
            deferred,
        } => {
            let result = state.transactions.remove(&transaction_id).map_or_else(
                || Err(transaction_closed_error()),
                |transaction| block_on!(transaction.commit()),
            );
            settle_deferred(deferred, result);
            false
        }
        LixCommand::TransactionRollback {
            transaction_id,
            deferred,
        } => {
            let result = state.transactions.remove(&transaction_id).map_or_else(
                || Err(transaction_closed_error()),
                |transaction| block_on!(transaction.rollback()),
            );
            settle_deferred(deferred, result);
            false
        }
        LixCommand::TransactionAbandon { transaction_id } => {
            if let Some(transaction) = state.transactions.remove(&transaction_id) {
                let _ = block_on!(transaction.rollback());
            }
            false
        }
    }
}

fn run_native_snapshot_export_executor(receiver: mpsc::Receiver<NativeSnapshotExportJob>) {
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            while let Ok(job) = receiver.recv() {
                finish_active_native_snapshot_export(
                    job.sender,
                    job.completion,
                    job.active,
                    Err(LixError::unknown(format!(
                        "failed to create snapshot export runtime: {error}"
                    ))),
                );
            }
            return;
        }
    };

    while let Ok(job) = receiver.recv() {
        let NativeSnapshotExportJob {
            builder,
            sender,
            completion,
            telemetry_parent,
            active,
        } = job;
        if sender.is_closed() {
            active.store(false, Ordering::SeqCst);
            completion.finish();
            continue;
        }
        let mut writer = NativeSnapshotWriter::new(sender.clone());
        let result = runtime.block_on(instrument_remote_parent(
            telemetry_parent,
            builder.write_to(&mut writer),
        ));
        finish_active_native_snapshot_export(sender, completion, active, result);
    }
}

fn finish_active_native_snapshot_export(
    sender: async_channel::Sender<NativeSnapshotMessage>,
    completion: Arc<NativeSnapshotExportCompletion>,
    active: Arc<AtomicBool>,
    result: NativeResult<()>,
) {
    let message = match result {
        Ok(()) => NativeSnapshotMessage::Done,
        Err(error) => NativeSnapshotMessage::Error(error),
    };
    let _ = sender.send_blocking(message);
    active.store(false, Ordering::SeqCst);
    completion.finish();
}

fn finish_native_snapshot_export(
    sender: async_channel::Sender<NativeSnapshotMessage>,
    completion: Arc<NativeSnapshotExportCompletion>,
    result: NativeResult<()>,
) {
    let message = match result {
        Ok(()) => NativeSnapshotMessage::Done,
        Err(error) => NativeSnapshotMessage::Error(error),
    };
    let _ = sender.send_blocking(message);
    completion.finish();
}

fn settle_command_after_close(command: LixCommand) {
    match command {
        LixCommand::Close(deferred) => settle_deferred(deferred, Ok(())),
        LixCommand::OpenAnotherSession { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::Execute { deferred, .. } | LixCommand::TransactionExecute { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::ExecuteBatch { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::BeginTransaction { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::ActiveBranchId(deferred) => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::ActiveAccountId(deferred) => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::CreateBranch { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::CreateCheckpoint(deferred) => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::Undo(deferred) => settle_deferred(deferred, Err(lix_closed_error())),
        LixCommand::Redo(deferred) => settle_deferred(deferred, Err(lix_closed_error())),
        LixCommand::SwitchBranch { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::MergeBranchPreview { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::MergeBranch { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::Observe { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::ExportSnapshot { sender, completion } => {
            let _ = sender.send_blocking(NativeSnapshotMessage::Error(lix_closed_error()));
            completion.finish();
        }
        LixCommand::ImportFilesystemPaths { deferred, .. }
        | LixCommand::SyncDiskToLix(deferred)
        | LixCommand::TransactionCommit { deferred, .. }
        | LixCommand::TransactionRollback { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
        LixCommand::TransactionAbandon { .. } => {}
    }
}

fn lix_closed_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "Lix handle is closed")
        .with_hint("Open a new Lix handle before calling this method.")
}

fn transaction_closed_error() -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
}

impl NativeLixInner {
    fn open_report(&self) -> &OpenReport {
        match self {
            Self::Memory(lix) => lix.open_report(),
            Self::FilesystemStorage(lix, _, _) => lix.open_report(),
        }
    }

    fn snapshot_export_builder(&self) -> NativeSnapshotExportBuilder {
        match self {
            Self::Memory(lix) => NativeSnapshotExportBuilder::Memory(lix.export_snapshot()),
            Self::FilesystemStorage(lix, _, _) => {
                NativeSnapshotExportBuilder::FilesystemStorage(lix.export_snapshot())
            }
        }
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: Option<String>,
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(lix) => {
                let execution = lix.execute(sql, params);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
            Self::FilesystemStorage(lix, _, _) => {
                let execution = lix.execute(sql, params);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
        }
    }

    async fn execute_batch(
        &self,
        statements: &[RsExecuteBatchStatement],
        options: Option<String>,
    ) -> std::result::Result<Vec<RsExecuteResult>, LixError> {
        match self {
            Self::Memory(lix) => {
                let execution = lix.execute_batch(statements);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
            Self::FilesystemStorage(lix, _, _) => {
                let execution = lix.execute_batch(statements);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
        }
    }

    async fn begin_transaction(&self) -> std::result::Result<NativeLixTransactionInner, LixError> {
        match self {
            Self::Memory(lix) => Ok(NativeLixTransactionInner::Memory(
                lix.begin_transaction().await?,
            )),
            Self::FilesystemStorage(lix, _, _) => Ok(NativeLixTransactionInner::FilesystemStorage(
                lix.begin_transaction().await?,
            )),
        }
    }

    fn observe(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::result::Result<NativeObserveEventsInner, LixError> {
        match self {
            Self::Memory(lix) => Ok(NativeObserveEventsInner::Memory(lix.observe(sql, params)?)),
            Self::FilesystemStorage(lix, _, _) => Ok(NativeObserveEventsInner::FilesystemStorage(
                lix.observe(sql, params)?,
            )),
        }
    }

    async fn active_branch_id(&self) -> std::result::Result<String, LixError> {
        match self {
            Self::Memory(lix) => lix.active_branch_id().await,
            Self::FilesystemStorage(lix, _, _) => lix.active_branch_id().await,
        }
    }

    fn active_account_id(&self) -> &str {
        match self {
            Self::Memory(lix) => lix.active_account_id(),
            Self::FilesystemStorage(lix, _, _) => lix.active_account_id(),
        }
    }

    async fn create_branch(
        &self,
        options: RsCreateBranchOptions,
    ) -> std::result::Result<CreateBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.create_branch(options).await,
            Self::FilesystemStorage(lix, _, _) => lix.create_branch(options).await,
        }
    }

    async fn create_checkpoint(&self) -> std::result::Result<CreateCheckpointReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.create_checkpoint().await,
            Self::FilesystemStorage(lix, _, _) => lix.create_checkpoint().await,
        }
    }

    async fn undo(&self) -> std::result::Result<UndoReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.undo().await,
            Self::FilesystemStorage(lix, _, _) => lix.undo().await,
        }
    }

    async fn redo(&self) -> std::result::Result<RedoReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.redo().await,
            Self::FilesystemStorage(lix, _, _) => lix.redo().await,
        }
    }

    async fn switch_branch(
        &self,
        options: RsSwitchBranchOptions,
    ) -> std::result::Result<SwitchBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.switch_branch(options).await,
            Self::FilesystemStorage(lix, _, _) => lix.switch_branch(options).await,
        }
    }

    async fn import_filesystem_paths(
        &self,
        paths: Vec<String>,
    ) -> std::result::Result<(), LixError> {
        match self {
            Self::FilesystemStorage(_, storage, _) => storage.import_paths(paths).await,
            Self::Memory(_) => Err(LixError::new(
                "LIX_UNSUPPORTED_STORAGE",
                "importFilesystemPaths requires a filesystem storage",
            )),
        }
    }

    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> std::result::Result<MergeBranchPreview, LixError> {
        match self {
            Self::Memory(lix) => lix.merge_branch_preview(options).await,
            Self::FilesystemStorage(lix, _, _) => lix.merge_branch_preview(options).await,
        }
    }

    async fn merge_branch(
        &self,
        options: RsMergeBranchOptions,
    ) -> std::result::Result<MergeBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.merge_branch(options).await,
            Self::FilesystemStorage(lix, _, _) => lix.merge_branch(options).await,
        }
    }

    async fn sync_disk_to_lix(&self) -> std::result::Result<(), LixError> {
        match self {
            Self::FilesystemStorage(_, storage, _) => storage.sync_disk_to_lix().await,
            Self::Memory(_) => Err(LixError::new(
                "LIX_UNSUPPORTED_STORAGE",
                "syncDiskToLix requires a filesystem storage",
            )),
        }
    }

    async fn close(&self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(lix) => lix.close().await,
            Self::FilesystemStorage(lix, storage, sessions) => {
                if sessions.fetch_sub(1, Ordering::SeqCst) == 1 {
                    storage.stop_sync().await?;
                }
                lix.close().await
            }
        }
    }

    async fn open_another_session(
        &self,
        options: NativeOpenAnotherSessionOptions,
    ) -> std::result::Result<Self, LixError> {
        match self {
            Self::Memory(lix) => {
                let mut builder = lix.open_another_session();
                if let Some(branch_id) = options.branch_id {
                    builder = builder.with_branch(branch_id);
                }
                if let Some(account_id) = options.account_id {
                    builder = builder.with_account(account_id);
                }
                Ok(Self::Memory(builder.await?))
            }
            Self::FilesystemStorage(lix, storage, sessions) => {
                let mut builder = lix.open_another_session();
                if let Some(branch_id) = options.branch_id {
                    builder = builder.with_branch(branch_id);
                }
                if let Some(account_id) = options.account_id {
                    builder = builder.with_account(account_id);
                }
                let opened = builder.await?;
                sessions.fetch_add(1, Ordering::SeqCst);
                Ok(Self::FilesystemStorage(
                    opened,
                    storage.clone(),
                    sessions.clone(),
                ))
            }
        }
    }
}

impl NativeLixTransactionInner {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: Option<String>,
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(transaction) => {
                let execution = transaction.execute(sql, params);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
            Self::FilesystemStorage(transaction) => {
                let execution = transaction.execute(sql, params);
                match options {
                    Some(origin_key) => execution.with_origin_key(origin_key).await,
                    None => execution.await,
                }
            }
        }
    }

    async fn commit(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.commit().await,
            Self::FilesystemStorage(transaction) => transaction.commit().await,
        }
    }

    async fn rollback(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.rollback().await,
            Self::FilesystemStorage(transaction) => transaction.rollback().await,
        }
    }
}

impl NativeObserveEventsInner {
    async fn next(&mut self) -> std::result::Result<Option<RsObserveEvent>, LixError> {
        match self {
            Self::Memory(events) => events.next().await,
            Self::FilesystemStorage(events) => events.next().await,
        }
    }

    fn close(&mut self) {
        match self {
            Self::Memory(events) => events.close(),
            Self::FilesystemStorage(events) => events.close(),
        }
    }
}

#[expect(missing_debug_implementations)]
pub struct OpenFilesystemStorageTask {
    path: String,
    sync_all_files: bool,
    telemetry_dispatch: Option<SharedJsTelemetryDispatch>,
    telemetry_parent: Option<SpanContext>,
    open_progress_dispatch: Option<SharedJsOpenProgressDispatch>,
    server_url: Option<String>,
    server_headers: Vec<(String, String)>,
    snapshot: Option<Vec<u8>>,
}

#[expect(missing_debug_implementations)]
pub struct OpenMemoryTask {
    telemetry_dispatch: Option<SharedJsTelemetryDispatch>,
    telemetry_parent: Option<SpanContext>,
    open_progress_dispatch: Option<SharedJsOpenProgressDispatch>,
    server_url: Option<String>,
    server_headers: Vec<(String, String)>,
    snapshot: Option<Vec<u8>>,
}

impl Task for OpenFilesystemStorageTask {
    type Output = std::result::Result<NativeLix, LixError>;
    type JsValue = NativeLix;

    fn compute(&mut self) -> Result<Self::Output> {
        Ok(open_filesystem_storage_native(
            std::mem::take(&mut self.path),
            self.sync_all_files,
            self.telemetry_dispatch.take(),
            self.telemetry_parent.take(),
            self.open_progress_dispatch.take(),
            self.server_url.take(),
            std::mem::take(&mut self.server_headers),
            self.snapshot
                .take()
                .map(|bytes| NativeSnapshotSource::Bytes(futures_lite::io::Cursor::new(bytes))),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

impl Task for OpenMemoryTask {
    type Output = std::result::Result<NativeLix, LixError>;
    type JsValue = NativeLix;

    fn compute(&mut self) -> Result<Self::Output> {
        Ok(open_memory_native(
            self.telemetry_dispatch.take(),
            self.telemetry_parent.take(),
            self.open_progress_dispatch.take(),
            self.server_url.take(),
            std::mem::take(&mut self.server_headers),
            self.snapshot
                .take()
                .map(|bytes| NativeSnapshotSource::Bytes(futures_lite::io::Cursor::new(bytes))),
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

fn telemetry_sink(
    dispatch: SharedJsTelemetryDispatch,
) -> (Arc<dyn TelemetrySink>, PendingTelemetryParent) {
    let parent_source = Arc::new(Mutex::new(None));
    let sink = CallbackTelemetrySink::new(move |span| {
        let Ok(json) = serde_json::to_string(&crate::telemetry::TelemetrySpanDto::from(span))
        else {
            return;
        };
        let _ = dispatch.call(json, ThreadsafeFunctionCallMode::NonBlocking);
    });
    (Arc::new(sink), parent_source)
}

struct NativeOpenProgressSink {
    dispatch: SharedJsOpenProgressDispatch,
}

impl OpenProgressSink for NativeOpenProgressSink {
    fn report(&self, progress: OpenProgress) {
        let Ok(json) = serde_json::to_string(&OpenProgressDto::from(progress)) else {
            return;
        };
        let _ = self
            .dispatch
            .call(json, ThreadsafeFunctionCallMode::NonBlocking);
    }
}

fn open_progress_sink(dispatch: SharedJsOpenProgressDispatch) -> Arc<dyn OpenProgressSink> {
    Arc::new(NativeOpenProgressSink { dispatch })
}

fn parse_server_headers(headers: Option<Vec<Vec<String>>>) -> Result<Vec<(String, String)>> {
    headers
        .unwrap_or_default()
        .into_iter()
        .map(|pair| match pair.as_slice() {
            [name, value] => Ok((name.clone(), value.clone())),
            _ => Err(Error::from_reason(
                "sync server headers must contain [name, value] pairs",
            )),
        })
        .collect()
}

fn open_memory_native(
    telemetry_dispatch: Option<SharedJsTelemetryDispatch>,
    telemetry_parent: Option<SpanContext>,
    open_progress_dispatch: Option<SharedJsOpenProgressDispatch>,
    server_url: Option<String>,
    server_headers: Vec<(String, String)>,
    snapshot: Option<NativeSnapshotSource>,
) -> std::result::Result<NativeLix, LixError> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError::unknown(format!("failed to create tokio runtime: {error}")))?;
    let (telemetry, telemetry_parent_source) = telemetry_dispatch
        .map(telemetry_sink)
        .map_or((None, None), |(sink, parent)| (Some(sink), Some(parent)));
    let mut builder = open_lix();
    if let Some(telemetry) = telemetry {
        builder = builder.with_telemetry(telemetry);
    }
    if let Some(dispatch) = open_progress_dispatch {
        builder = builder.with_open_progress_sink(open_progress_sink(dispatch));
    }
    if let Some(url) = server_url {
        builder = builder.with_server(ServerOptions::sync(url).with_headers(server_headers));
    }
    let lix = rt.block_on(instrument_remote_parent(telemetry_parent, async move {
        match snapshot {
            Some(snapshot) => builder.from_snapshot(snapshot).await,
            None => builder.await,
        }
    }));
    let lix = lix?;
    NativeLix::new(NativeLixInner::Memory(lix), telemetry_parent_source)
}

fn open_filesystem_storage_native(
    path: String,
    sync_all_files: bool,
    telemetry_dispatch: Option<SharedJsTelemetryDispatch>,
    telemetry_parent: Option<SpanContext>,
    open_progress_dispatch: Option<SharedJsOpenProgressDispatch>,
    server_url: Option<String>,
    server_headers: Vec<(String, String)>,
    snapshot: Option<NativeSnapshotSource>,
) -> std::result::Result<NativeLix, LixError> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError::unknown(format!("failed to create tokio runtime: {error}")))?;
    let storage = FilesystemStorage::new(path)
        .sync_all_files(sync_all_files)
        .open()?;
    let (telemetry, telemetry_parent_source) = telemetry_dispatch
        .map(telemetry_sink)
        .map_or((None, None), |(sink, parent)| (Some(sink), Some(parent)));
    let mut builder = open_lix().with_storage(storage.clone());
    if let Some(telemetry) = telemetry {
        builder = builder.with_telemetry(telemetry);
    }
    if let Some(dispatch) = open_progress_dispatch {
        builder = builder.with_open_progress_sink(open_progress_sink(dispatch));
    }
    if let Some(url) = server_url {
        builder = builder.with_server(ServerOptions::sync(url).with_headers(server_headers));
    }
    let lix = rt.block_on(instrument_remote_parent(telemetry_parent, async move {
        match snapshot {
            Some(snapshot) => builder.from_snapshot(snapshot).await,
            None => builder.await,
        }
    }));
    let lix = lix?;
    rt.block_on(storage.start_sync(&lix))?;
    NativeLix::new(
        NativeLixInner::FilesystemStorage(lix, storage, Arc::new(AtomicUsize::new(1))),
        telemetry_parent_source,
    )
}

fn start_native_snapshot_restore<F>(open: F) -> Result<NativeSnapshotRestore>
where
    F: FnOnce(NativeSnapshotSource) -> NativeResult<NativeLix> + Send + 'static,
{
    let (input, receiver) = async_channel::bounded(1);
    let (result_sender, result) = async_channel::bounded(1);
    let complete = Arc::new(AtomicBool::new(false));
    let task_complete = Arc::clone(&complete);
    thread::Builder::new()
        .name("lix-snapshot-restore".to_string())
        .stack_size(NATIVE_ENGINE_ACTOR_STACK_SIZE)
        .spawn(move || {
            let source = NativeSnapshotSource::Stream(NativeSnapshotReader::new(receiver));
            let _ = result_sender.send_blocking(open(source));
            task_complete.store(true, Ordering::SeqCst);
        })
        .map_err(to_napi_error)?;
    Ok(NativeSnapshotRestore {
        input,
        result,
        finished: Arc::new(AtomicBool::new(false)),
        complete,
    })
}

#[napi]
impl NativeLix {
    #[napi(js_name = "openReport")]
    pub fn open_report(&self) -> NativeOpenReport {
        self.open_report.clone()
    }

    #[napi(js_name = "setTelemetryParent")]
    pub fn set_telemetry_parent(&self, parent_json: Option<String>) -> Result<()> {
        if let Some(parent_source) = &self.telemetry_parent {
            *parent_source
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                crate::telemetry::parse_parent_context_json(parent_json)
                    .map_err(Error::from_reason)?;
        }
        Ok(())
    }

    #[napi(js_name = "openMemory")]
    pub fn open_memory(
        telemetry_dispatch: Option<Function<'_, String, ()>>,
        telemetry_parent_json: Option<String>,
        server_url: Option<String>,
        server_headers: Option<Vec<Vec<String>>>,
        open_progress_dispatch: Option<Function<'_, String, ()>>,
    ) -> Result<AsyncTask<OpenMemoryTask>> {
        Ok(AsyncTask::new(OpenMemoryTask {
            telemetry_dispatch: optional_telemetry_dispatch(telemetry_dispatch)?,
            telemetry_parent: crate::telemetry::parse_parent_context_json(telemetry_parent_json)
                .map_err(Error::from_reason)?,
            open_progress_dispatch: optional_open_progress_dispatch(open_progress_dispatch)?,
            server_url,
            server_headers: parse_server_headers(server_headers)?,
            snapshot: None,
        }))
    }

    #[napi(js_name = "openMemoryFromSnapshot")]
    pub fn open_memory_from_snapshot(
        telemetry_dispatch: Option<Function<'_, String, ()>>,
        telemetry_parent_json: Option<String>,
        open_progress_dispatch: Option<Function<'_, String, ()>>,
    ) -> Result<NativeSnapshotRestore> {
        let telemetry_dispatch = optional_telemetry_dispatch(telemetry_dispatch)?;
        let telemetry_parent = crate::telemetry::parse_parent_context_json(telemetry_parent_json)
            .map_err(Error::from_reason)?;
        let open_progress_dispatch = optional_open_progress_dispatch(open_progress_dispatch)?;
        start_native_snapshot_restore(move |snapshot| {
            open_memory_native(
                telemetry_dispatch,
                telemetry_parent,
                open_progress_dispatch,
                None,
                Vec::new(),
                Some(snapshot),
            )
        })
    }

    #[napi(js_name = "openFilesystemStorage")]
    pub fn open_filesystem_storage(
        path: String,
        sync_all_files: bool,
        telemetry_dispatch: Option<Function<'_, String, ()>>,
        telemetry_parent_json: Option<String>,
        server_url: Option<String>,
        server_headers: Option<Vec<Vec<String>>>,
        open_progress_dispatch: Option<Function<'_, String, ()>>,
    ) -> Result<AsyncTask<OpenFilesystemStorageTask>> {
        Ok(AsyncTask::new(OpenFilesystemStorageTask {
            path,
            sync_all_files,
            telemetry_dispatch: optional_telemetry_dispatch(telemetry_dispatch)?,
            telemetry_parent: crate::telemetry::parse_parent_context_json(telemetry_parent_json)
                .map_err(Error::from_reason)?,
            open_progress_dispatch: optional_open_progress_dispatch(open_progress_dispatch)?,
            server_url,
            server_headers: parse_server_headers(server_headers)?,
            snapshot: None,
        }))
    }

    #[napi(js_name = "openFilesystemStorageFromSnapshot")]
    pub fn open_filesystem_storage_from_snapshot(
        path: String,
        sync_all_files: bool,
        telemetry_dispatch: Option<Function<'_, String, ()>>,
        telemetry_parent_json: Option<String>,
        open_progress_dispatch: Option<Function<'_, String, ()>>,
    ) -> Result<NativeSnapshotRestore> {
        let telemetry_dispatch = optional_telemetry_dispatch(telemetry_dispatch)?;
        let telemetry_parent = crate::telemetry::parse_parent_context_json(telemetry_parent_json)
            .map_err(Error::from_reason)?;
        let open_progress_dispatch = optional_open_progress_dispatch(open_progress_dispatch)?;
        start_native_snapshot_restore(move |snapshot| {
            open_filesystem_storage_native(
                path,
                sync_all_files,
                telemetry_dispatch,
                telemetry_parent,
                open_progress_dispatch,
                None,
                Vec::new(),
                Some(snapshot),
            )
        })
    }

    #[napi(js_name = "openAnotherSession")]
    pub fn open_another_session<'env>(
        &self,
        env: &'env Env,
        options: Option<NativeOpenAnotherSessionOptions>,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeLixDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::OpenAnotherSession {
                options: options.unwrap_or(NativeOpenAnotherSessionOptions {
                    branch_id: None,
                    account_id: None,
                }),
                telemetry_parent: self.telemetry_parent.clone(),
                deferred,
            });
        Ok(promise)
    }

    #[napi]
    pub fn execute<'env>(
        &self,
        env: &'env Env,
        sql: String,
        params: Option<Vec<LixValue>>,
        options: Option<NativeExecuteOptions>,
    ) -> Result<Object<'env>> {
        let params = match params {
            Some(params) => params
                .into_iter()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| throw_lix_error(env, error))?,
            None => Vec::new(),
        };
        let options = options.and_then(|options| options.origin_key);
        let (deferred, promise): (NativeExecuteDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::Execute {
                sql,
                params,
                options,
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "executeBatch")]
    pub fn execute_batch<'env>(
        &self,
        env: &'env Env,
        statements: Vec<NativeExecuteBatchStatement>,
        options: Option<NativeExecuteOptions>,
    ) -> Result<Object<'env>> {
        let statements = statements
            .into_iter()
            .map(|statement| {
                let params = statement
                    .params
                    .unwrap_or_default()
                    .into_iter()
                    .map(Value::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(RsExecuteBatchStatement {
                    sql: statement.sql,
                    params,
                    label: statement.label,
                })
            })
            .collect::<std::result::Result<Vec<_>, LixError>>()
            .map_err(|error| throw_lix_error(env, error))?;
        let options = options.and_then(|options| options.origin_key);
        let (deferred, promise): (NativeExecuteBatchDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::ExecuteBatch {
                statements,
                options,
                deferred,
            });
        Ok(promise)
    }

    #[napi]
    pub fn observe<'env>(
        &self,
        env: &'env Env,
        sql: String,
        params: Option<Vec<LixValue>>,
    ) -> Result<Object<'env>> {
        let params = match params {
            Some(params) => params
                .into_iter()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| throw_lix_error(env, error))?,
            None => Vec::new(),
        };
        let (deferred, promise): (NativeDeferred<NativeObserveEvents>, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::Observe {
                sql,
                params,
                telemetry_parent: self.telemetry_parent.clone(),
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let transaction_id = self.actor.next_transaction_id();
        let (deferred, promise): (NativeTransactionDeferred, Object<'env>) =
            env.create_deferred()?;
        let actor = self.actor.clone();
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::BeginTransaction {
                transaction_id,
                actor,
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "activeBranchId")]
    pub fn active_branch_id<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeStringDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, LixCommand::ActiveBranchId);
        Ok(promise)
    }

    #[napi(js_name = "activeAccountId")]
    pub fn active_account_id<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeStringDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, LixCommand::ActiveAccountId);
        Ok(promise)
    }

    #[napi(js_name = "createBranch")]
    pub fn create_branch<'env>(
        &self,
        env: &'env Env,
        options: CreateBranchOptions,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeCreateBranchDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::CreateBranch {
                options: options.into(),
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "createCheckpoint")]
    pub fn create_checkpoint<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeCreateCheckpointDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, LixCommand::CreateCheckpoint);
        Ok(promise)
    }

    #[napi(js_name = "undo")]
    pub fn undo<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUndoDeferred, Object<'env>) = env.create_deferred()?;
        self.actor.send_with_deferred(deferred, LixCommand::Undo);
        Ok(promise)
    }

    #[napi(js_name = "redo")]
    pub fn redo<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeRedoDeferred, Object<'env>) = env.create_deferred()?;
        self.actor.send_with_deferred(deferred, LixCommand::Redo);
        Ok(promise)
    }

    #[napi(js_name = "switchBranch")]
    pub fn switch_branch<'env>(
        &self,
        env: &'env Env,
        options: SwitchBranchOptions,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeSwitchBranchDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::SwitchBranch {
                options: options.into(),
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "importFilesystemPaths")]
    pub fn import_filesystem_paths<'env>(
        &self,
        env: &'env Env,
        paths: Vec<String>,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUnitDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::ImportFilesystemPaths {
                paths,
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "mergeBranchPreview")]
    pub fn merge_branch_preview<'env>(
        &self,
        env: &'env Env,
        options: MergeBranchOptions,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeMergePreviewDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::MergeBranchPreview {
                options: options.into_preview(),
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "mergeBranch")]
    pub fn merge_branch<'env>(
        &self,
        env: &'env Env,
        options: MergeBranchOptions,
    ) -> Result<Object<'env>> {
        let (deferred, promise): (NativeMergeReceiptDeferred, Object<'env>) =
            env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::MergeBranch {
                options: options.into(),
                deferred,
            });
        Ok(promise)
    }

    #[napi(js_name = "syncDiskToLix")]
    pub fn sync_disk_to_lix<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUnitDeferred, Object<'env>) = env.create_deferred()?;
        self.actor
            .send_with_deferred(deferred, LixCommand::SyncDiskToLix);
        Ok(promise)
    }

    #[napi(js_name = "exportSnapshot")]
    pub fn export_snapshot(&self) -> NativeSnapshotExport {
        let (sender, receiver) = async_channel::bounded(1);
        let completion = Arc::new(NativeSnapshotExportCompletion::default());
        self.actor
            .send_snapshot_export(sender, Arc::clone(&completion));
        NativeSnapshotExport {
            receiver,
            completion,
            canceled: Arc::new(AtomicBool::new(false)),
        }
    }

    #[napi]
    pub fn close<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUnitDeferred, Object<'env>) = env.create_deferred()?;
        if self.actor.closed.load(Ordering::SeqCst) {
            settle_deferred(deferred, Ok(()));
            return Ok(promise);
        }
        self.actor.send_with_deferred(deferred, LixCommand::Close);
        Ok(promise)
    }
}

#[expect(missing_debug_implementations)]
#[napi(js_name = "ObserveEvents")]
pub struct NativeObserveEvents {
    commands: Sender<ObserveCommand>,
    closed: Arc<AtomicBool>,
    close_signal: watch::Sender<bool>,
    next_in_flight: Arc<AtomicBool>,
    telemetry_parent: Option<PendingTelemetryParent>,
}

#[napi]
impl NativeObserveEvents {
    fn new(
        events: NativeObserveEventsInner,
        telemetry_parent: Option<PendingTelemetryParent>,
    ) -> Result<Self> {
        let (commands, receiver) = mpsc::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let (close_signal, actor_close_signal) = watch::channel(false);
        let next_in_flight = Arc::new(AtomicBool::new(false));
        let telemetry_parent = telemetry_parent.map(|_| Arc::new(Mutex::new(None)));

        let actor_closed = Arc::clone(&closed);
        let actor_next_in_flight = Arc::clone(&next_in_flight);
        thread::Builder::new()
            .name("lix-observe-events".to_string())
            .stack_size(NATIVE_ENGINE_ACTOR_STACK_SIZE)
            .spawn(move || {
                run_observe_actor(
                    events,
                    receiver,
                    actor_closed,
                    actor_close_signal,
                    actor_next_in_flight,
                );
            })
            .map_err(to_napi_error)?;

        Ok(Self {
            commands,
            closed,
            close_signal,
            next_in_flight,
            telemetry_parent,
        })
    }

    #[napi]
    pub fn next<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        if self.closed.load(Ordering::SeqCst) {
            let (deferred, promise): (ObserveNextDeferred, Object<'env>) = env.create_deferred()?;
            resolve_observe_deferred(deferred, Ok(None), Arc::clone(&self.next_in_flight));
            return Ok(promise);
        }

        if self
            .next_in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(throw_lix_error(
                env,
                LixError::new(
                    "LIX_OBSERVE_NEXT_IN_FLIGHT",
                    "ObserveEvents.next() is already in flight",
                )
                .with_hint("Await the pending next() call before calling next() again."),
            ));
        }

        let (deferred, promise): (ObserveNextDeferred, Object<'env>) = match env.create_deferred() {
            Ok(deferred) => deferred,
            Err(error) => {
                self.next_in_flight.store(false, Ordering::SeqCst);
                return Err(error);
            }
        };
        let telemetry_parent = self
            .telemetry_parent
            .as_ref()
            .and_then(take_pending_telemetry_parent);
        match self.commands.send(ObserveCommand::Next {
            deferred,
            telemetry_parent,
        }) {
            Ok(()) => Ok(promise),
            Err(error) => {
                self.closed.store(true, Ordering::SeqCst);
                let ObserveCommand::Next { deferred, .. } = error.0 else {
                    unreachable!("next() only sends ObserveCommand::Next");
                };
                resolve_observe_deferred(deferred, Ok(None), Arc::clone(&self.next_in_flight));
                Ok(promise)
            }
        }
    }

    #[napi(js_name = "setTelemetryParent")]
    pub fn set_telemetry_parent(&self, parent_json: Option<String>) -> Result<()> {
        if let Some(parent_source) = &self.telemetry_parent {
            *parent_source
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                crate::telemetry::parse_parent_context_json(parent_json)
                    .map_err(Error::from_reason)?;
        }
        Ok(())
    }

    #[napi]
    pub fn close(&self) {
        close_observe_events(&self.commands, &self.closed, &self.close_signal);
    }
}

impl Drop for NativeObserveEvents {
    fn drop(&mut self) {
        close_observe_events(&self.commands, &self.closed, &self.close_signal);
    }
}

type ObserveNextResult = std::result::Result<Option<RsObserveEvent>, LixError>;
type ObserveNextResolver = Box<dyn FnOnce(Env) -> Result<Option<ObserveEventDto>> + Send>;
type ObserveNextDeferred = JsDeferred<Option<ObserveEventDto>, ObserveNextResolver>;

enum ObserveCommand {
    Next {
        deferred: ObserveNextDeferred,
        telemetry_parent: Option<SpanContext>,
    },
    Close,
}

fn run_observe_actor(
    mut events: NativeObserveEventsInner,
    receiver: mpsc::Receiver<ObserveCommand>,
    closed: Arc<AtomicBool>,
    mut close_signal: watch::Receiver<bool>,
    next_in_flight: Arc<AtomicBool>,
) {
    let rt = match Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(error) => {
            closed.store(true, Ordering::SeqCst);
            while let Ok(command) = receiver.recv() {
                match command {
                    ObserveCommand::Next { deferred, .. } => {
                        next_in_flight.store(false, Ordering::SeqCst);
                        deferred.reject(to_napi_error(&error));
                    }
                    ObserveCommand::Close => break,
                }
            }
            return;
        }
    };

    while let Ok(command) = receiver.recv() {
        match command {
            ObserveCommand::Next {
                deferred,
                telemetry_parent,
            } => {
                let result = rt.block_on(instrument_remote_parent(
                    telemetry_parent,
                    observe_next(&mut events, &closed, &mut close_signal),
                ));
                let result = match result {
                    Ok(Some(_)) | Err(_) if closed.load(Ordering::SeqCst) => Ok(None),
                    Err(error) if error.code == LixError::CODE_CLOSED => Ok(None),
                    other => other,
                };
                let result = match result {
                    Ok(Some(_)) | Err(_)
                        if closed.load(Ordering::SeqCst) || *close_signal.borrow() =>
                    {
                        Ok(None)
                    }
                    other => other,
                };
                let terminal = observe_result_is_terminal(&result);
                if terminal {
                    closed.store(true, Ordering::SeqCst);
                }
                resolve_observe_deferred(deferred, result, Arc::clone(&next_in_flight));
                if terminal {
                    events.close();
                    break;
                }
            }
            ObserveCommand::Close => {
                closed.store(true, Ordering::SeqCst);
                events.close();
                break;
            }
        }
    }
    closed.store(true, Ordering::SeqCst);
}

async fn observe_next(
    events: &mut NativeObserveEventsInner,
    closed: &AtomicBool,
    close_signal: &mut watch::Receiver<bool>,
) -> ObserveNextResult {
    if closed.load(Ordering::SeqCst) || *close_signal.borrow() {
        events.close();
        return Ok(None);
    }

    let result = tokio::select! {
        result = events.next() => result,
        changed = close_signal.changed() => {
            events.close();
            match changed {
                Ok(()) | Err(_) => Ok(None),
            }
        }
    };

    match result {
        Ok(Some(_)) | Err(_) if closed.load(Ordering::SeqCst) || *close_signal.borrow() => {
            events.close();
            Ok(None)
        }
        Ok(Some(event)) => Ok(Some(event)),
        Ok(None) => {
            closed.store(true, Ordering::SeqCst);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn observe_result_is_terminal(result: &ObserveNextResult) -> bool {
    match result {
        Ok(None) => true,
        Err(error) if error.code == LixError::CODE_CLOSED => true,
        _ => false,
    }
}

fn resolve_observe_deferred(
    deferred: ObserveNextDeferred,
    result: ObserveNextResult,
    next_in_flight: Arc<AtomicBool>,
) {
    deferred.resolve(Box::new(move |env| {
        next_in_flight.store(false, Ordering::SeqCst);
        observe_next_to_js(&env, result)
    }));
}

fn observe_next_to_js(env: &Env, result: ObserveNextResult) -> Result<Option<ObserveEventDto>> {
    match result {
        Ok(Some(event)) => Ok(Some(
            ObserveEventDto::try_from(event)
                .map_err(|error| lix_error_to_napi_error(env, error))?,
        )),
        Ok(None) => Ok(None),
        Err(error) => Err(lix_error_to_napi_error(env, error)),
    }
}

fn close_observe_events(
    commands: &Sender<ObserveCommand>,
    closed: &AtomicBool,
    close_signal: &watch::Sender<bool>,
) {
    if closed.swap(true, Ordering::SeqCst) {
        return;
    }
    let _ = close_signal.send(true);
    let _ = commands.send(ObserveCommand::Close);
}

impl NativeLix {
    fn new(
        lix: NativeLixInner,
        telemetry_parent: Option<PendingTelemetryParent>,
    ) -> std::result::Result<Self, LixError> {
        let open_report = NativeOpenReport::from(lix.open_report());
        let actor = NativeLixActor::start(lix, telemetry_parent.clone())
            .map_err(|error| LixError::unknown(format!("failed to start native actor: {error}")))?;
        Ok(Self {
            actor,
            telemetry_parent,
            open_report,
        })
    }
}

#[expect(missing_debug_implementations)]
#[napi(js_name = "LixTransaction")]
pub struct NativeLixTransaction {
    actor: NativeLixActor,
    transaction_id: u64,
    closed: Arc<AtomicBool>,
}

#[napi]
impl NativeLixTransaction {
    fn new(actor: NativeLixActor, transaction_id: u64) -> Self {
        Self {
            actor,
            transaction_id,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    #[napi]
    pub fn execute<'env>(
        &self,
        env: &'env Env,
        sql: String,
        params: Option<Vec<LixValue>>,
        options: Option<NativeExecuteOptions>,
    ) -> Result<Object<'env>> {
        let params = match params {
            Some(params) => params
                .into_iter()
                .map(Value::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| throw_lix_error(env, error))?,
            None => Vec::new(),
        };
        let options = options.and_then(|options| options.origin_key);
        let (deferred, promise): (NativeExecuteDeferred, Object<'env>) = env.create_deferred()?;
        if self.closed.load(Ordering::SeqCst) {
            settle_deferred(deferred, Err(transaction_closed_error()));
            return Ok(promise);
        }
        let transaction_id = self.transaction_id;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::TransactionExecute {
                transaction_id,
                sql,
                params,
                options,
                deferred,
            });
        Ok(promise)
    }

    #[napi]
    pub fn commit<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUnitDeferred, Object<'env>) = env.create_deferred()?;
        if self.closed.swap(true, Ordering::SeqCst) {
            settle_deferred(deferred, Err(transaction_closed_error()));
            return Ok(promise);
        }
        let transaction_id = self.transaction_id;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::TransactionCommit {
                transaction_id,
                deferred,
            });
        Ok(promise)
    }

    #[napi]
    pub fn rollback<'env>(&self, env: &'env Env) -> Result<Object<'env>> {
        let (deferred, promise): (NativeUnitDeferred, Object<'env>) = env.create_deferred()?;
        if self.closed.swap(true, Ordering::SeqCst) {
            settle_deferred(deferred, Err(transaction_closed_error()));
            return Ok(promise);
        }
        let transaction_id = self.transaction_id;
        self.actor
            .send_with_deferred(deferred, |deferred| LixCommand::TransactionRollback {
                transaction_id,
                deferred,
            });
        Ok(promise)
    }
}

impl Drop for NativeLixTransaction {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::SeqCst) {
            self.actor.abandon_transaction(self.transaction_id);
        }
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct CreateBranchOptions {
    pub id: Option<String>,
    pub name: String,
    pub from_commit_id: Option<String>,
}

impl From<CreateBranchOptions> for RsCreateBranchOptions {
    fn from(options: CreateBranchOptions) -> Self {
        Self {
            id: options.id,
            name: options.name,
            from_commit_id: options.from_commit_id,
        }
    }
}

#[napi(object)]
pub struct CreateBranchReceiptDto {
    pub id: String,
    pub name: String,
    pub hidden: bool,
    pub commit_id: String,
}

impl From<CreateBranchReceipt> for CreateBranchReceiptDto {
    fn from(receipt: CreateBranchReceipt) -> Self {
        Self {
            id: receipt.id,
            name: receipt.name,
            hidden: receipt.hidden,
            commit_id: receipt.commit_id,
        }
    }
}

#[napi(object)]
pub struct CreateCheckpointReceiptDto {
    pub commit_id: String,
}

impl From<CreateCheckpointReceipt> for CreateCheckpointReceiptDto {
    fn from(receipt: CreateCheckpointReceipt) -> Self {
        Self {
            commit_id: receipt.commit_id,
        }
    }
}

#[napi(object)]
pub struct UndoReceiptDto {
    pub branch_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

impl From<UndoReceipt> for UndoReceiptDto {
    fn from(receipt: UndoReceipt) -> Self {
        Self {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            inverse_commit_id: receipt.inverse_commit_id,
        }
    }
}

#[napi(object)]
pub struct RedoReceiptDto {
    pub branch_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}

impl From<RedoReceipt> for RedoReceiptDto {
    fn from(receipt: RedoReceipt) -> Self {
        Self {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            replay_commit_id: receipt.replay_commit_id,
        }
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct SwitchBranchOptions {
    pub branch_id: String,
}

impl From<SwitchBranchOptions> for RsSwitchBranchOptions {
    fn from(options: SwitchBranchOptions) -> Self {
        Self {
            branch_id: options.branch_id,
        }
    }
}

#[napi(object)]
pub struct SwitchBranchReceiptDto {
    pub branch_id: String,
}

impl From<SwitchBranchReceipt> for SwitchBranchReceiptDto {
    fn from(receipt: SwitchBranchReceipt) -> Self {
        Self {
            branch_id: receipt.branch_id,
        }
    }
}

#[expect(missing_debug_implementations)]
#[napi(object)]
pub struct MergeBranchOptions {
    pub source_branch_id: String,
}

impl MergeBranchOptions {
    fn into_preview(self) -> MergeBranchPreviewOptions {
        MergeBranchPreviewOptions {
            source_branch_id: self.source_branch_id,
        }
    }
}

impl From<MergeBranchOptions> for RsMergeBranchOptions {
    fn from(options: MergeBranchOptions) -> Self {
        Self {
            source_branch_id: options.source_branch_id,
        }
    }
}

#[napi(object)]
pub struct MergeBranchReceiptDto {
    pub outcome: String,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_before_commit_id: String,
    pub source_head_before_commit_id: String,
    pub target_head_after_commit_id: String,
    pub created_merge_commit_id: Option<String>,
    pub change_stats: MergeChangeStatsDto,
}

impl From<MergeBranchReceipt> for MergeBranchReceiptDto {
    fn from(receipt: MergeBranchReceipt) -> Self {
        Self {
            outcome: merge_branch_outcome_to_string(receipt.outcome),
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

#[napi(object)]
pub struct MergeBranchPreviewDto {
    pub outcome: String,
    pub target_branch_id: String,
    pub source_branch_id: String,
    pub base_commit_id: String,
    pub target_head_commit_id: String,
    pub source_head_commit_id: String,
    pub change_stats: MergeChangeStatsDto,
    pub conflicts: Vec<MergeConflictDto>,
}

impl From<MergeBranchPreview> for MergeBranchPreviewDto {
    fn from(preview: MergeBranchPreview) -> Self {
        Self {
            outcome: merge_branch_outcome_to_string(preview.outcome),
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

fn merge_branch_outcome_to_string(outcome: MergeBranchOutcome) -> String {
    match outcome {
        MergeBranchOutcome::AlreadyUpToDate => "alreadyUpToDate",
        MergeBranchOutcome::FastForward => "fastForward",
        MergeBranchOutcome::MergeCommitted => "mergeCommitted",
    }
    .to_string()
}

#[napi(object)]
pub struct MergeChangeStatsDto {
    pub total: u32,
    pub added: u32,
    pub modified: u32,
    pub removed: u32,
}

impl From<MergeChangeStats> for MergeChangeStatsDto {
    #[expect(clippy::cast_possible_truncation)]
    fn from(stats: MergeChangeStats) -> Self {
        Self {
            total: stats.total as u32,
            added: stats.added as u32,
            modified: stats.modified as u32,
            removed: stats.removed as u32,
        }
    }
}

#[napi(object)]
pub struct MergeConflictDto {
    pub kind: String,
    pub schema_key: String,
    pub row_pk: serde_json::Value,
    pub file_id: Option<String>,
    pub target: MergeConflictSideDto,
    pub source: MergeConflictSideDto,
}

impl From<MergeConflict> for MergeConflictDto {
    fn from(conflict: MergeConflict) -> Self {
        Self {
            kind: merge_conflict_kind_to_string(conflict.kind),
            schema_key: conflict.schema_key,
            row_pk: conflict.row_pk,
            file_id: conflict.file_id,
            target: conflict.target.into(),
            source: conflict.source.into(),
        }
    }
}

fn merge_conflict_kind_to_string(kind: MergeConflictKind) -> String {
    match kind {
        MergeConflictKind::SameRowChanged => "sameRowChanged",
    }
    .to_string()
}

#[napi(object)]
pub struct MergeConflictSideDto {
    pub kind: String,
    pub before_change_id: Option<String>,
    pub after_change_id: Option<String>,
}

impl From<MergeConflictSide> for MergeConflictSideDto {
    fn from(side: MergeConflictSide) -> Self {
        Self {
            kind: merge_conflict_change_kind_to_string(side.kind),
            before_change_id: side.before_change_id,
            after_change_id: side.after_change_id,
        }
    }
}

fn merge_conflict_change_kind_to_string(kind: MergeConflictChangeKind) -> String {
    match kind {
        MergeConflictChangeKind::Added => "added",
        MergeConflictChangeKind::Modified => "modified",
        MergeConflictChangeKind::Removed => "removed",
    }
    .to_string()
}

#[expect(missing_debug_implementations)]
#[napi(object)]
pub struct LixValue {
    pub kind: String,
    pub value: Option<serde_json::Value>,
    pub blob: Option<Buffer>,
}

impl TryFrom<LixValue> for Value {
    type Error = LixError;

    fn try_from(value: LixValue) -> std::result::Result<Self, Self::Error> {
        match value.kind.as_str() {
            "null" => Ok(Self::Null),
            "boolean" => Ok(Self::Boolean(
                value.value.and_then(|v| v.as_bool()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "boolean value must be a boolean",
                    )
                })?,
            )),
            "integer" => Ok(Self::Integer(
                value.value.and_then(|v| v.as_i64()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "integer value must be an integer",
                    )
                })?,
            )),
            "real" => {
                let value = value.value.and_then(|v| v.as_f64()).ok_or_else(|| {
                    LixError::new(LixError::CODE_INVALID_PARAM, "real value must be a number")
                })?;
                if !value.is_finite() {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "real value must be a finite number",
                    ));
                }
                Ok(Self::Real(value))
            }
            "text" => Ok(Self::Text(
                value
                    .value
                    .and_then(|v| v.as_str().map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        LixError::new(LixError::CODE_INVALID_PARAM, "text value must be a string")
                    })?,
            )),
            "jsonb" => Ok(Self::Jsonb(
                value.value.unwrap_or(serde_json::Value::Null).into(),
            )),
            "timestamptz" => {
                let raw = value
                    .value
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "timestamptz value must be an RFC 3339 string",
                        )
                    })?;
                let parsed = chrono::DateTime::parse_from_rfc3339(&raw).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("timestamptz value is invalid: {error}"),
                    )
                })?;
                Ok(Self::Timestamptz(parsed.timestamp_micros()))
            }
            "blob" => {
                let bytes = value.blob.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "blob value must include bytes",
                    )
                })?;
                Ok(Self::Blob(bytes.to_vec().into()))
            }
            other => Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("unsupported LixValue kind: {other}"),
            )),
        }
    }
}

impl TryFrom<&Value> for LixValue {
    type Error = LixError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(Self {
                kind: "null".to_string(),
                value: Some(serde_json::Value::Null),
                blob: None,
            }),
            Value::Boolean(value) => Ok(Self {
                kind: "boolean".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Integer(value) => Ok(Self {
                kind: "integer".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Real(value) => {
                if !value.is_finite() {
                    return Err(LixError::new(
                        "LIX_ERROR_JS_SDK_NATIVE",
                        "cannot encode non-finite real value",
                    ));
                }
                Ok(Self {
                    kind: "real".to_string(),
                    value: Some(serde_json::json!(value)),
                    blob: None,
                })
            }
            Value::Text(value) => Ok(Self {
                kind: "text".to_string(),
                value: Some(serde_json::json!(value)),
                blob: None,
            }),
            Value::Jsonb(value) => Ok(Self {
                kind: "jsonb".to_string(),
                value: Some(value.to_value()),
                blob: None,
            }),
            Value::Timestamptz(value) => {
                let value = chrono::DateTime::from_timestamp_micros(*value).ok_or_else(|| {
                    LixError::new("LIX_ERROR_JS_SDK_NATIVE", "timestamptz is out of range")
                })?;
                Ok(Self {
                    kind: "timestamptz".to_string(),
                    value: Some(serde_json::Value::String(
                        value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    )),
                    blob: None,
                })
            }
            Value::Blob(value) => Ok(Self {
                kind: "blob".to_string(),
                value: None,
                blob: Some(Buffer::from(value.to_vec())),
            }),
        }
    }
}

#[napi(object)]
pub struct ExecuteResult {
    pub statement_index: Option<u32>,
    pub label: Option<String>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<LixValue>>,
    pub rows_affected: u32,
    pub notices: Vec<LixNotice>,
}

impl TryFrom<RsExecuteResult> for ExecuteResult {
    type Error = LixError;

    #[expect(clippy::cast_possible_truncation)]
    fn try_from(result: RsExecuteResult) -> std::result::Result<Self, Self::Error> {
        let mut rows = Vec::with_capacity(result.rows().len());
        for row in result.rows() {
            let mut values = Vec::with_capacity(row.values().len());
            for value in row.values() {
                values.push(LixValue::try_from(value)?);
            }
            rows.push(values);
        }
        Ok(Self {
            statement_index: result.statement_index().map(|index| index as u32),
            label: result.label().map(str::to_owned),
            columns: result.columns().to_vec(),
            rows,
            rows_affected: result.rows_affected() as u32,
            notices: result
                .notices()
                .iter()
                .map(|notice| LixNotice {
                    code: notice.code.clone(),
                    message: notice.message.clone(),
                    hint: notice.hint.clone(),
                })
                .collect(),
        })
    }
}

#[napi(object)]
pub struct ObserveEventDto {
    pub sequence: f64,
    pub mutation_sequence: f64,
    pub rows: ExecuteResult,
}

impl TryFrom<RsObserveEvent> for ObserveEventDto {
    type Error = LixError;

    #[expect(clippy::cast_precision_loss)]
    fn try_from(event: RsObserveEvent) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            sequence: event.sequence as f64,
            mutation_sequence: event.mutation_sequence as f64,
            rows: ExecuteResult::try_from(event.rows)?,
        })
    }
}

#[napi(object)]
pub struct LixNotice {
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
}

fn to_napi_error(error: impl std::fmt::Display) -> Error {
    Error::from_reason(error.to_string())
}

fn create_lix_error<'env>(env: &'env Env, error: &LixError) -> Result<Object<'env>> {
    let mut js_error = env.create_error(Error::new(Status::GenericFailure, &error.message))?;
    js_error.set_named_property("name", "LixError")?;
    js_error.set_named_property("code", error.code.clone())?;
    if let Some(hint) = &error.hint {
        js_error.set_named_property("hint", hint.clone())?;
    }
    if let Some(details) = &error.details {
        js_error.set_named_property("details", details.clone())?;
    }
    Ok(js_error)
}

fn lix_error_to_napi_error(env: &Env, error: LixError) -> Error {
    create_lix_error(env, &error)
        .map(|js_error| Error::from(js_error.to_unknown()))
        .unwrap_or_else(|fallback| fallback)
}

fn throw_lix_error(env: &Env, error: LixError) -> Error {
    let thrown = (|| -> Result<()> {
        let js_error = create_lix_error(env, &error)?;
        env.throw(js_error)?;
        Ok(())
    })();

    match thrown {
        Ok(()) => Error::new(Status::PendingException, ""),
        Err(error) => error,
    }
}
