#![allow(missing_debug_implementations)]

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::future::{AbortHandle, Abortable};
use js_sys::{Array, Function, Reflect};
use lix::telemetry::{CallbackTelemetrySink, TelemetrySink};
use lix::{
    BROWSER_TRANSPORT_CONFIG_HEADER, CreateBranchOptions as RsCreateBranchOptions,
    ExecuteBatchStatement as RsExecuteBatchStatement, ExecuteResult as RsExecuteResult,
    Lix as RsLix, LixError, LixTransaction as RsLixTransaction, Memory,
    MergeBranchOptions as RsMergeBranchOptions, MergeBranchOutcome, MergeBranchPreviewOptions,
    ObserveEvents as RsObserveEvents, ServerOptions, SwitchBranchOptions as RsSwitchBranchOptions,
    Value, open_lix, register_browser_sync_transport, unregister_browser_sync_transport,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
#[cfg(feature = "storage-bridge-bench")]
use wasm_bindgen_futures::JsFuture;

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
}

#[wasm_bindgen]
pub struct WasmLixTransaction {
    inner: Option<BrowserTransaction>,
}

#[wasm_bindgen]
pub struct WasmObserveEvents {
    inner: RefCell<Option<BrowserObserveEvents>>,
    closed: Cell<bool>,
    next_abort: RefCell<Option<AbortHandle>>,
}

#[wasm_bindgen(js_name = openMemory)]
pub async fn open_memory(
    telemetry_dispatch: Option<Function>,
    server: Option<JsValue>,
) -> Result<WasmLix, JsValue> {
    open_browser_storage(
        BrowserStorage::Memory(Memory::new()),
        telemetry_dispatch,
        server,
    )
    .await
}

#[wasm_bindgen(js_name = openJsStorage)]
pub async fn open_js_storage(
    provider: JsStorageProvider,
    telemetry_dispatch: Option<Function>,
    server: Option<JsValue>,
) -> Result<WasmLix, JsValue> {
    let storage = JsStorage::new(provider);
    let browser_storage = BrowserStorage::Js(storage);
    match open_browser_storage(browser_storage.clone(), telemetry_dispatch, server).await {
        Ok(lix) => Ok(lix),
        Err(error) => {
            let _ = browser_storage.close().await;
            Err(error)
        }
    }
}

async fn open_browser_storage(
    storage: BrowserStorage,
    telemetry_dispatch: Option<Function>,
    server: Option<JsValue>,
) -> Result<WasmLix, JsValue> {
    console_error_panic_hook::set_once();
    let telemetry = telemetry_dispatch.map(|dispatch| {
        let dispatch = BrowserTelemetryDispatch(dispatch);
        let sink: Arc<dyn TelemetrySink> = Arc::new(CallbackTelemetrySink::new(move |span| {
            let Ok(span) = to_js(&crate::telemetry::TelemetrySpanDto::from(span)) else {
                return;
            };
            let _ = dispatch.0.call1(&JsValue::UNDEFINED, &span);
        }));
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
            Some(ServerOptions::sync(parsed.url).with_headers(parsed.headers))
        }
        None => None,
    };
    let inner = match telemetry {
        Some(telemetry) => {
            let builder = open_lix()
                .with_storage(storage.clone())
                .with_telemetry(telemetry);
            match server {
                Some(server) => builder.with_server(server).await,
                None => builder.await,
            }
        }
        None => {
            let builder = open_lix().with_storage(storage.clone());
            match server {
                Some(server) => builder.with_server(server).await,
                None => builder.await,
            }
        }
    }
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

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but the shared telemetry trait requires Send"
)]
unsafe impl Send for BrowserTelemetryDispatch {}
unsafe impl Sync for BrowserTelemetryDispatch {}

#[wasm_bindgen]
impl WasmLix {
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
        let inner = builder.await.map_err(lix_error_to_js)?;
        self.storage_sessions
            .set(self.storage_sessions.get().saturating_add(1));
        Ok(WasmLix {
            inner,
            storage: self.storage.clone(),
            storage_sessions: self.storage_sessions.clone(),
            closed: Cell::new(false),
            browser_sync_transport_id: self.browser_sync_transport_id.clone(),
        })
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
        let result = execution.await.map_err(lix_error_to_js)?;
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
        let results = execution.await.map_err(lix_error_to_js)?;
        let results = results
            .into_iter()
            .map(ExecuteResultDto::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(lix_error_to_js)?;
        to_js(&results)
    }

    #[wasm_bindgen(js_name = observe)]
    pub async fn observe(
        &self,
        sql: String,
        params: JsValue,
    ) -> Result<WasmObserveEvents, JsValue> {
        let params = values_from_js(params)?;
        let inner = self.inner.observe(&sql, &params).map_err(lix_error_to_js)?;
        Ok(WasmObserveEvents {
            inner: RefCell::new(Some(inner)),
            closed: Cell::new(false),
            next_abort: RefCell::new(None),
        })
    }

    #[wasm_bindgen(js_name = beginTransaction)]
    pub async fn begin_transaction(&self) -> Result<WasmLixTransaction, JsValue> {
        let inner = self
            .inner
            .begin_transaction()
            .await
            .map_err(lix_error_to_js)?;
        Ok(WasmLixTransaction { inner: Some(inner) })
    }

    #[wasm_bindgen(js_name = activeBranchId)]
    pub async fn active_branch_id(&self) -> Result<String, JsValue> {
        self.inner.active_branch_id().await.map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = activeAccountId)]
    pub async fn active_account_id(&self) -> Result<String, JsValue> {
        Ok(self.inner.active_account_id().to_string())
    }

    #[wasm_bindgen(js_name = createBranch)]
    pub async fn create_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: CreateBranchOptionsDto = from_js(options)?;
        let receipt = self
            .inner
            .create_branch(RsCreateBranchOptions {
                id: options.id,
                name: options.name,
                from_commit_id: options.from_commit_id,
            })
            .await
            .map_err(lix_error_to_js)?;
        to_js(&CreateBranchReceiptDto {
            id: receipt.id,
            name: receipt.name,
            hidden: receipt.hidden,
            commit_id: receipt.commit_id,
        })
    }

    #[wasm_bindgen(js_name = createCheckpoint)]
    pub async fn create_checkpoint(&self) -> Result<JsValue, JsValue> {
        let receipt = self
            .inner
            .create_checkpoint()
            .await
            .map_err(lix_error_to_js)?;
        to_js(&CreateCheckpointReceiptDto {
            commit_id: receipt.commit_id,
        })
    }

    #[wasm_bindgen(js_name = undo)]
    pub async fn undo(&self) -> Result<JsValue, JsValue> {
        let receipt = self.inner.undo().await.map_err(lix_error_to_js)?;
        to_js(&UndoReceiptDto {
            branch_id: receipt.branch_id,
            target_commit_id: receipt.target_commit_id,
            inverse_commit_id: receipt.inverse_commit_id,
        })
    }

    #[wasm_bindgen(js_name = redo)]
    pub async fn redo(&self) -> Result<JsValue, JsValue> {
        let receipt = self.inner.redo().await.map_err(lix_error_to_js)?;
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
            .inner
            .switch_branch(RsSwitchBranchOptions {
                branch_id: options.branch_id,
            })
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
            .inner
            .merge_branch_preview(MergeBranchPreviewOptions {
                source_branch_id: options.source_branch_id,
            })
            .await
            .map_err(lix_error_to_js)?;
        to_js(&MergeBranchPreviewDto::from(preview))
    }

    #[wasm_bindgen(js_name = mergeBranch)]
    pub async fn merge_branch(&self, options: JsValue) -> Result<JsValue, JsValue> {
        let options: MergeBranchOptionsDto = from_js(options)?;
        let receipt = self
            .inner
            .merge_branch(RsMergeBranchOptions {
                source_branch_id: options.source_branch_id,
            })
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
        if let Err(error) = self.inner.close().await {
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
        let inner = self.inner.as_mut().ok_or_else(transaction_closed_error)?;
        let execution = inner.execute(&sql, &params);
        let execution = match options {
            Some(origin_key) => execution.with_origin_key(origin_key),
            None => execution,
        };
        let result = execution.await.map_err(lix_error_to_js)?;
        execute_result_to_js(result)
    }

    #[wasm_bindgen(js_name = commit)]
    pub async fn commit(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        inner.commit().await.map_err(lix_error_to_js)
    }

    #[wasm_bindgen(js_name = rollback)]
    pub async fn rollback(&mut self) -> Result<(), JsValue> {
        let inner = self.inner.take().ok_or_else(transaction_closed_error)?;
        inner.rollback().await.map_err(lix_error_to_js)
    }
}

#[wasm_bindgen]
impl WasmObserveEvents {
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
        let result = Abortable::new(inner.next(), registration).await;
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
pub(super) struct CreateCheckpointReceiptDto {
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
    schema_key: String,
    row_pk: serde_json::Value,
    file_id: Option<String>,
    target: MergeConflictSideDto,
    source: MergeConflictSideDto,
}

impl From<lix::MergeConflict> for MergeConflictDto {
    fn from(conflict: lix::MergeConflict) -> Self {
        Self {
            kind: "sameRowChanged",
            schema_key: conflict.schema_key,
            row_pk: conflict.row_pk,
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
    columns: Vec<String>,
    rows: Vec<Vec<LixValueDto>>,
    rows_affected: f64,
    notices: Vec<LixNoticeDto>,
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
            columns: result.columns().to_vec(),
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
