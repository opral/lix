use lix_sdk::{
    CreateBranchOptions as RsCreateBranchOptions, CreateBranchReceipt,
    ExecuteBatchStatement as RsExecuteBatchStatement, ExecuteOptions as RsExecuteOptions,
    ExecuteResult as RsExecuteResult, Lix as RsLix, LixError, LixTransaction as RsLixTransaction,
    LocalFilesystem, LocalFilesystemOpenOptions, Memory,
    MergeBranchOptions as RsMergeBranchOptions, MergeBranchOutcome, MergeBranchPreview,
    MergeBranchPreviewOptions, MergeBranchReceipt, MergeChangeStats, MergeConflict,
    MergeConflictChangeKind, MergeConflictKind, MergeConflictSide, ObserveEvent as RsObserveEvent,
    ObserveEvents as RsObserveEvents, OpenLixOptions as RsOpenLixOptions, SQLite, SQLiteOptions,
    SwitchBranchOptions as RsSwitchBranchOptions, SwitchBranchReceipt, Value, WasmRuntime,
    open_lix,
};
use napi::JsDeferred;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::mpsc::{self, Sender};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::watch;

use crate::js_wasm_runtime::{JsWasmRuntime, SharedJsWasmRuntimeDispatch};

#[expect(missing_debug_implementations)]
#[napi(js_name = "Lix")]
pub struct NativeLix {
    actor: NativeLixActor,
}

enum NativeLixInner {
    Memory(RsLix<Memory>),
    SQLite(RsLix<SQLite>),
    LocalFilesystem(RsLix<LocalFilesystem>, LocalFilesystem),
}

enum NativeLixTransactionInner {
    Memory(RsLixTransaction<Memory>),
    SQLite(RsLixTransaction<SQLite>),
    LocalFilesystem(RsLixTransaction<LocalFilesystem>),
}

enum NativeObserveEventsInner {
    Memory(RsObserveEvents<Memory>),
    SQLite(RsObserveEvents<SQLite>),
    LocalFilesystem(RsObserveEvents<LocalFilesystem>),
}

#[napi(object)]
#[derive(Debug)]
pub struct NativeExecuteOptions {
    #[napi(js_name = "originKey")]
    pub origin_key: Option<String>,
}

impl From<NativeExecuteOptions> for RsExecuteOptions {
    fn from(options: NativeExecuteOptions) -> Self {
        Self {
            origin_key: options.origin_key,
        }
    }
}

#[napi(object)]
#[expect(missing_debug_implementations)]
pub struct NativeExecuteBatchStatement {
    pub sql: String,
    pub params: Option<Vec<LixValue>>,
}

#[derive(Clone)]
struct NativeLixActor {
    commands: Sender<LixCommand>,
    closed: Arc<AtomicBool>,
    send_lock: Arc<Mutex<()>>,
    next_transaction_id: Arc<AtomicU64>,
}

struct NativeLixActorState {
    lix: NativeLixInner,
    transactions: HashMap<u64, NativeLixTransactionInner>,
}

type NativeResult<T> = std::result::Result<T, LixError>;
type NativeResolver<T> = Box<dyn FnOnce(Env) -> Result<T> + Send>;
type NativeDeferred<T> = JsDeferred<T, NativeResolver<T>>;
type NativeExecuteDeferred = NativeDeferred<ExecuteResult>;
type NativeExecuteBatchDeferred = NativeDeferred<Vec<ExecuteResult>>;
type NativeTransactionDeferred = NativeDeferred<NativeLixTransaction>;
type NativeStringDeferred = NativeDeferred<String>;
type NativeCreateBranchDeferred = NativeDeferred<CreateBranchReceiptDto>;
type NativeSwitchBranchDeferred = NativeDeferred<SwitchBranchReceiptDto>;
type NativeMergePreviewDeferred = NativeDeferred<MergeBranchPreviewDto>;
type NativeMergeReceiptDeferred = NativeDeferred<MergeBranchReceiptDto>;
type NativeUnitDeferred = NativeDeferred<()>;

enum LixCommand {
    Execute {
        sql: String,
        params: Vec<Value>,
        options: RsExecuteOptions,
        deferred: NativeExecuteDeferred,
    },
    ExecuteBatch {
        statements: Vec<RsExecuteBatchStatement>,
        options: RsExecuteOptions,
        deferred: NativeExecuteBatchDeferred,
    },
    BeginTransaction {
        transaction_id: u64,
        actor: NativeLixActor,
        deferred: NativeTransactionDeferred,
    },
    ActiveBranchId(NativeStringDeferred),
    CreateBranch {
        options: RsCreateBranchOptions,
        deferred: NativeCreateBranchDeferred,
    },
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
    Close(NativeUnitDeferred),
    Observe {
        sql: String,
        params: Vec<Value>,
        deferred: NativeDeferred<NativeObserveEvents>,
    },
    TransactionExecute {
        transaction_id: u64,
        sql: String,
        params: Vec<Value>,
        options: RsExecuteOptions,
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

impl NativeLixActor {
    fn start(lix: NativeLixInner) -> Result<Self> {
        let (commands, receiver) = mpsc::channel();
        let actor = Self {
            commands,
            closed: Arc::new(AtomicBool::new(false)),
            send_lock: Arc::new(Mutex::new(())),
            next_transaction_id: Arc::new(AtomicU64::new(1)),
        };
        let actor_closed = Arc::clone(&actor.closed);
        let actor_send_lock = Arc::clone(&actor.send_lock);
        thread::Builder::new()
            .name("lix-native".to_string())
            .spawn(move || run_lix_actor(lix, receiver, actor_closed, actor_send_lock))
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
        let command = command(deferred);
        match self.commands.send(command) {
            Ok(()) => {}
            Err(error) => {
                settle_command_after_close(error.0);
            }
        }
    }
}

impl NativeLixActor {
    fn abandon_transaction(&self, transaction_id: u64) {
        let _ = self
            .commands
            .send(LixCommand::TransactionAbandon { transaction_id });
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
    receiver: mpsc::Receiver<LixCommand>,
    closed: Arc<AtomicBool>,
    send_lock: Arc<Mutex<()>>,
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
    });

    while let Ok(command) = receiver.recv() {
        let Some(open_state) = state.as_mut() else {
            settle_command_after_close(command);
            continue;
        };
        if closed.load(Ordering::SeqCst) {
            drop(state.take());
            settle_command_after_close(command);
            drain_commands_after_close(&receiver, &send_lock);
            break;
        }
        if handle_lix_command(&rt, open_state, &closed, command) {
            drop(state.take());
            drain_commands_after_close(&receiver, &send_lock);
            break;
        }
    }
    closed.store(true, Ordering::SeqCst);
}

fn drain_commands_after_close(receiver: &mpsc::Receiver<LixCommand>, send_lock: &Mutex<()>) {
    let Ok(_send_guard) = send_lock.lock() else {
        return;
    };
    for command in receiver.try_iter() {
        settle_command_after_close(command);
    }
}

fn reject_pending_lix_commands(receiver: mpsc::Receiver<LixCommand>, error: std::io::Error) {
    while let Ok(command) = receiver.recv() {
        match command {
            LixCommand::Execute { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::ExecuteBatch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::BeginTransaction { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::ActiveBranchId(deferred) => deferred.reject(to_napi_error(&error)),
            LixCommand::CreateBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::SwitchBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
            LixCommand::MergeBranchPreview { deferred, .. } => {
                deferred.reject(to_napi_error(&error));
            }
            LixCommand::MergeBranch { deferred, .. } => deferred.reject(to_napi_error(&error)),
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
) -> bool {
    match command {
        LixCommand::Execute {
            sql,
            params,
            options,
            deferred,
        } => {
            let result = rt
                .block_on(state.lix.execute(&sql, &params, options))
                .and_then(ExecuteResult::try_from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ExecuteBatch {
            statements,
            options,
            deferred,
        } => {
            let result = rt
                .block_on(state.lix.execute_batch(&statements, options))
                .and_then(|results| {
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
            let result = rt
                .block_on(state.lix.begin_transaction())
                .map(|transaction| {
                    state.transactions.insert(transaction_id, transaction);
                    NativeLixTransaction::new(actor, transaction_id)
                });
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ActiveBranchId(deferred) => {
            let result = rt.block_on(state.lix.active_branch_id());
            settle_deferred(deferred, result);
            false
        }
        LixCommand::CreateBranch { options, deferred } => {
            let result = rt
                .block_on(state.lix.create_branch(options))
                .map(CreateBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::SwitchBranch { options, deferred } => {
            let result = rt
                .block_on(state.lix.switch_branch(options))
                .map(SwitchBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::ImportFilesystemPaths { paths, deferred } => {
            let result = rt.block_on(state.lix.import_filesystem_paths(paths));
            settle_deferred(deferred, result);
            false
        }
        LixCommand::MergeBranchPreview { options, deferred } => {
            let result = rt
                .block_on(state.lix.merge_branch_preview(options))
                .map(MergeBranchPreviewDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::MergeBranch { options, deferred } => {
            let result = rt
                .block_on(state.lix.merge_branch(options))
                .map(MergeBranchReceiptDto::from);
            settle_deferred(deferred, result);
            false
        }
        LixCommand::SyncDiskToLix(deferred) => {
            let result = rt.block_on(state.lix.sync_disk_to_lix());
            settle_deferred(deferred, result);
            false
        }
        LixCommand::Close(deferred) => {
            let result = rt.block_on(state.lix.close());
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
            deferred,
        } => {
            let result = state.lix.observe(&sql, &params).and_then(|events| {
                NativeObserveEvents::new(events).map_err(|error| {
                    LixError::unknown(format!("failed to start observe actor: {error}"))
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
                    rt.block_on(transaction.execute(&sql, &params, options))
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
                |transaction| rt.block_on(transaction.commit()),
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
                |transaction| rt.block_on(transaction.rollback()),
            );
            settle_deferred(deferred, result);
            false
        }
        LixCommand::TransactionAbandon { transaction_id } => {
            if let Some(transaction) = state.transactions.remove(&transaction_id) {
                let _ = rt.block_on(transaction.rollback());
            }
            false
        }
    }
}

fn settle_command_after_close(command: LixCommand) {
    match command {
        LixCommand::Close(deferred) => settle_deferred(deferred, Ok(())),
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
        LixCommand::CreateBranch { deferred, .. } => {
            settle_deferred(deferred, Err(lix_closed_error()));
        }
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
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: RsExecuteOptions,
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(lix) => lix.execute_with_options(sql, params, options).await,
            Self::SQLite(lix) => lix.execute_with_options(sql, params, options).await,
            Self::LocalFilesystem(lix, _) => lix.execute_with_options(sql, params, options).await,
        }
    }

    async fn execute_batch(
        &self,
        statements: &[RsExecuteBatchStatement],
        options: RsExecuteOptions,
    ) -> std::result::Result<Vec<RsExecuteResult>, LixError> {
        match self {
            Self::Memory(lix) => lix.execute_batch_with_options(statements, options).await,
            Self::SQLite(lix) => lix.execute_batch_with_options(statements, options).await,
            Self::LocalFilesystem(lix, _) => {
                lix.execute_batch_with_options(statements, options).await
            }
        }
    }

    async fn begin_transaction(&self) -> std::result::Result<NativeLixTransactionInner, LixError> {
        match self {
            Self::Memory(lix) => Ok(NativeLixTransactionInner::Memory(
                lix.begin_transaction().await?,
            )),
            Self::SQLite(lix) => Ok(NativeLixTransactionInner::SQLite(
                lix.begin_transaction().await?,
            )),
            Self::LocalFilesystem(lix, _) => Ok(NativeLixTransactionInner::LocalFilesystem(
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
            Self::SQLite(lix) => Ok(NativeObserveEventsInner::SQLite(lix.observe(sql, params)?)),
            Self::LocalFilesystem(lix, _) => Ok(NativeObserveEventsInner::LocalFilesystem(
                lix.observe(sql, params)?,
            )),
        }
    }

    async fn active_branch_id(&self) -> std::result::Result<String, LixError> {
        match self {
            Self::Memory(lix) => lix.active_branch_id().await,
            Self::SQLite(lix) => lix.active_branch_id().await,
            Self::LocalFilesystem(lix, _) => lix.active_branch_id().await,
        }
    }

    async fn create_branch(
        &self,
        options: RsCreateBranchOptions,
    ) -> std::result::Result<CreateBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.create_branch(options).await,
            Self::SQLite(lix) => lix.create_branch(options).await,
            Self::LocalFilesystem(lix, _) => lix.create_branch(options).await,
        }
    }

    async fn switch_branch(
        &self,
        options: RsSwitchBranchOptions,
    ) -> std::result::Result<SwitchBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.switch_branch(options).await,
            Self::SQLite(lix) => lix.switch_branch(options).await,
            Self::LocalFilesystem(lix, _) => lix.switch_branch(options).await,
        }
    }

    async fn import_filesystem_paths(
        &self,
        paths: Vec<String>,
    ) -> std::result::Result<(), LixError> {
        match self {
            Self::LocalFilesystem(_, storage) => storage.import_paths(paths).await,
            Self::Memory(_) | Self::SQLite(_) => Err(LixError::new(
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
            Self::SQLite(lix) => lix.merge_branch_preview(options).await,
            Self::LocalFilesystem(lix, _) => lix.merge_branch_preview(options).await,
        }
    }

    async fn merge_branch(
        &self,
        options: RsMergeBranchOptions,
    ) -> std::result::Result<MergeBranchReceipt, LixError> {
        match self {
            Self::Memory(lix) => lix.merge_branch(options).await,
            Self::SQLite(lix) => lix.merge_branch(options).await,
            Self::LocalFilesystem(lix, _) => lix.merge_branch(options).await,
        }
    }

    async fn sync_disk_to_lix(&self) -> std::result::Result<(), LixError> {
        match self {
            Self::LocalFilesystem(_, storage) => storage.sync_disk_to_lix().await,
            Self::Memory(_) | Self::SQLite(_) => Err(LixError::new(
                "LIX_UNSUPPORTED_STORAGE",
                "syncDiskToLix requires a filesystem storage",
            )),
        }
    }

    async fn close(&self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(lix) => lix.close().await,
            Self::SQLite(lix) => lix.close().await,
            Self::LocalFilesystem(lix, _) => lix.close().await,
        }
    }
}

impl NativeLixTransactionInner {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: RsExecuteOptions,
    ) -> std::result::Result<RsExecuteResult, LixError> {
        match self {
            Self::Memory(transaction) => {
                transaction.execute_with_options(sql, params, options).await
            }
            Self::SQLite(transaction) => {
                transaction.execute_with_options(sql, params, options).await
            }
            Self::LocalFilesystem(transaction) => {
                transaction.execute_with_options(sql, params, options).await
            }
        }
    }

    async fn commit(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.commit().await,
            Self::SQLite(transaction) => transaction.commit().await,
            Self::LocalFilesystem(transaction) => transaction.commit().await,
        }
    }

    async fn rollback(self) -> std::result::Result<(), LixError> {
        match self {
            Self::Memory(transaction) => transaction.rollback().await,
            Self::SQLite(transaction) => transaction.rollback().await,
            Self::LocalFilesystem(transaction) => transaction.rollback().await,
        }
    }
}

impl NativeObserveEventsInner {
    async fn next(&mut self) -> std::result::Result<Option<RsObserveEvent>, LixError> {
        match self {
            Self::Memory(events) => events.next().await,
            Self::SQLite(events) => events.next().await,
            Self::LocalFilesystem(events) => events.next().await,
        }
    }

    fn close(&mut self) {
        match self {
            Self::Memory(events) => events.close(),
            Self::SQLite(events) => events.close(),
            Self::LocalFilesystem(events) => events.close(),
        }
    }
}

#[expect(missing_debug_implementations)]
pub struct OpenLocalFilesystemTask {
    path: String,
    lix_dir: Option<String>,
    sync_all_files: bool,
    wasm_runtime_dispatch: Option<SharedJsWasmRuntimeDispatch>,
}

#[expect(missing_debug_implementations)]
pub struct OpenMemoryTask {
    wasm_runtime_dispatch: Option<SharedJsWasmRuntimeDispatch>,
}

#[expect(missing_debug_implementations)]
pub struct OpenSQLiteTask {
    path: String,
    wasm_runtime_dispatch: Option<SharedJsWasmRuntimeDispatch>,
}

impl Task for OpenLocalFilesystemTask {
    type Output = std::result::Result<NativeLix, LixError>;
    type JsValue = NativeLix;

    fn compute(&mut self) -> Result<Self::Output> {
        let wasm_runtime_dispatch = take_wasm_runtime_dispatch(&mut self.wasm_runtime_dispatch)?;
        Ok(open_local_filesystem_native(
            std::mem::take(&mut self.path),
            self.lix_dir.take(),
            std::mem::take(&mut self.sync_all_files),
            wasm_runtime_dispatch,
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
        let wasm_runtime_dispatch = take_wasm_runtime_dispatch(&mut self.wasm_runtime_dispatch)?;
        Ok(open_memory_native(wasm_runtime_dispatch))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

impl Task for OpenSQLiteTask {
    type Output = std::result::Result<NativeLix, LixError>;
    type JsValue = NativeLix;

    fn compute(&mut self) -> Result<Self::Output> {
        let wasm_runtime_dispatch = take_wasm_runtime_dispatch(&mut self.wasm_runtime_dispatch)?;
        Ok(open_sqlite_native(
            std::mem::take(&mut self.path),
            wasm_runtime_dispatch,
        ))
    }

    fn resolve(&mut self, env: Env, output: Self::Output) -> Result<Self::JsValue> {
        output.map_err(|error| lix_error_to_napi_error(&env, error))
    }
}

fn take_wasm_runtime_dispatch(
    dispatch: &mut Option<SharedJsWasmRuntimeDispatch>,
) -> Result<SharedJsWasmRuntimeDispatch> {
    dispatch.take().ok_or_else(|| {
        Error::new(
            Status::GenericFailure,
            "JavaScript WASM runtime dispatch was already consumed",
        )
    })
}

fn open_memory_native(
    wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
) -> std::result::Result<NativeLix, LixError> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError::unknown(format!("failed to create tokio runtime: {error}")))?;
    let options = RsOpenLixOptions::default()
        .with_wasm_runtime(Arc::new(JsWasmRuntime::new(wasm_runtime_dispatch)));
    let lix = rt.block_on(open_lix(options))?;
    NativeLix::new(NativeLixInner::Memory(lix))
}

fn open_sqlite_native(
    path: String,
    wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
) -> std::result::Result<NativeLix, LixError> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError::unknown(format!("failed to create tokio runtime: {error}")))?;
    let storage = SQLite::new(SQLiteOptions { path: path.into() })?;
    let options = RsOpenLixOptions::new(storage)
        .with_wasm_runtime(Arc::new(JsWasmRuntime::new(wasm_runtime_dispatch)));
    let lix = rt.block_on(open_lix(options))?;
    NativeLix::new(NativeLixInner::SQLite(lix))
}

fn open_local_filesystem_native(
    path: String,
    lix_dir: Option<String>,
    sync_all_files: bool,
    wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
) -> std::result::Result<NativeLix, LixError> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LixError::unknown(format!("failed to create tokio runtime: {error}")))?;
    let mut options = LocalFilesystemOpenOptions::new(path, sync_all_files);
    options.lix_dir = lix_dir.map(Into::into);
    let wasm_runtime: Arc<dyn WasmRuntime> = Arc::new(JsWasmRuntime::new(wasm_runtime_dispatch));
    let storage = rt.block_on(LocalFilesystem::open_with_options_and_wasm_runtime(
        options,
        Arc::clone(&wasm_runtime),
    ))?;
    let options = RsOpenLixOptions::new(storage.clone()).with_wasm_runtime(wasm_runtime);
    let lix = rt.block_on(open_lix(options))?;
    NativeLix::new(NativeLixInner::LocalFilesystem(lix, storage))
}

#[napi]
impl NativeLix {
    #[napi(js_name = "openMemory")]
    pub fn open_memory(
        wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
    ) -> AsyncTask<OpenMemoryTask> {
        AsyncTask::new(OpenMemoryTask {
            wasm_runtime_dispatch: Some(wasm_runtime_dispatch),
        })
    }

    #[napi(js_name = "openSQLite")]
    pub fn open_sqlite(
        path: String,
        wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
    ) -> AsyncTask<OpenSQLiteTask> {
        AsyncTask::new(OpenSQLiteTask {
            path,
            wasm_runtime_dispatch: Some(wasm_runtime_dispatch),
        })
    }

    #[napi(js_name = "openLocalFilesystem")]
    pub fn open_local_filesystem(
        path: String,
        lix_dir: Option<String>,
        sync_all_files: bool,
        wasm_runtime_dispatch: SharedJsWasmRuntimeDispatch,
    ) -> AsyncTask<OpenLocalFilesystemTask> {
        AsyncTask::new(OpenLocalFilesystemTask {
            path,
            lix_dir,
            sync_all_files,
            wasm_runtime_dispatch: Some(wasm_runtime_dispatch),
        })
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
        let options = options.map(RsExecuteOptions::from).unwrap_or_default();
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
                })
            })
            .collect::<std::result::Result<Vec<_>, LixError>>()
            .map_err(|error| throw_lix_error(env, error))?;
        let options = options.map(RsExecuteOptions::from).unwrap_or_default();
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
}

#[napi]
impl NativeObserveEvents {
    fn new(events: NativeObserveEventsInner) -> Result<Self> {
        let (commands, receiver) = mpsc::channel();
        let closed = Arc::new(AtomicBool::new(false));
        let (close_signal, actor_close_signal) = watch::channel(false);
        let next_in_flight = Arc::new(AtomicBool::new(false));

        let actor_closed = Arc::clone(&closed);
        let actor_next_in_flight = Arc::clone(&next_in_flight);
        thread::Builder::new()
            .name("lix-observe-events".to_string())
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
        match self.commands.send(ObserveCommand::Next(deferred)) {
            Ok(()) => Ok(promise),
            Err(error) => {
                self.closed.store(true, Ordering::SeqCst);
                let ObserveCommand::Next(deferred) = error.0 else {
                    unreachable!("next() only sends ObserveCommand::Next");
                };
                resolve_observe_deferred(deferred, Ok(None), Arc::clone(&self.next_in_flight));
                Ok(promise)
            }
        }
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
    Next(ObserveNextDeferred),
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
                    ObserveCommand::Next(deferred) => {
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
            ObserveCommand::Next(deferred) => {
                let result = rt.block_on(observe_next(&mut events, &closed, &mut close_signal));
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
    fn new(lix: NativeLixInner) -> std::result::Result<Self, LixError> {
        let actor = NativeLixActor::start(lix)
            .map_err(|error| LixError::unknown(format!("failed to start native actor: {error}")))?;
        Ok(Self { actor })
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
        let options = options.map(RsExecuteOptions::from).unwrap_or_default();
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
    pub entity_pk: serde_json::Value,
    pub file_id: Option<String>,
    pub target: MergeConflictSideDto,
    pub source: MergeConflictSideDto,
}

impl From<MergeConflict> for MergeConflictDto {
    fn from(conflict: MergeConflict) -> Self {
        Self {
            kind: merge_conflict_kind_to_string(conflict.kind),
            schema_key: conflict.schema_key,
            entity_pk: conflict.entity_pk,
            file_id: conflict.file_id,
            target: conflict.target.into(),
            source: conflict.source.into(),
        }
    }
}

fn merge_conflict_kind_to_string(kind: MergeConflictKind) -> String {
    match kind {
        MergeConflictKind::SameEntityChanged => "sameEntityChanged",
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
            "json" => Ok(Self::Json(value.value.unwrap_or(serde_json::Value::Null))),
            "blob" => {
                let bytes = value.blob.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "blob value must include bytes",
                    )
                })?;
                Ok(Self::Blob(bytes.to_vec()))
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
            Value::Json(value) => Ok(Self {
                kind: "json".to_string(),
                value: Some(value.clone()),
                blob: None,
            }),
            Value::Blob(value) => Ok(Self {
                kind: "blob".to_string(),
                value: None,
                blob: Some(Buffer::from(value.clone())),
            }),
        }
    }
}

#[napi(object)]
pub struct ExecuteResult {
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
