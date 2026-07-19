#![allow(missing_debug_implementations)]

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures_util::future::{AbortHandle, Abortable};
use js_sys::{Array, Function, Promise, Reflect};
use lix_sdk::{
    CallbackTelemetrySink, CreateBranchOptions as RsCreateBranchOptions,
    ExecuteBatchStatement as RsExecuteBatchStatement, ExecuteOptions as RsExecuteOptions,
    ExecuteResult as RsExecuteResult, Lix as RsLix, LixError, LixTransaction as RsLixTransaction,
    Memory, MergeBranchOptions as RsMergeBranchOptions, MergeBranchOutcome,
    MergeBranchPreviewOptions, ObserveEvents as RsObserveEvents,
    OpenLixOptions as RsOpenLixOptions, SqlScriptPlan,
    SwitchBranchOptions as RsSwitchBranchOptions, TelemetrySink, Value, WasmComponentInstance,
    WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState, WasmPluginFile, WasmRuntime,
    open_lix, open_lix_with_telemetry, parse_sql_script as parse_rs_sql_script,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_bytes::ByteBuf;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

type BrowserLix = RsLix<Memory>;
type BrowserTransaction = RsLixTransaction<Memory>;
type BrowserObserveEvents = RsObserveEvents<Memory>;

#[wasm_bindgen]
pub struct WasmLix {
    inner: BrowserLix,
    storage: Memory,
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
    plugin_runtime_dispatch: Function,
    telemetry_dispatch: Option<Function>,
) -> Result<WasmLix, JsValue> {
    open_memory_from_snapshot(plugin_runtime_dispatch, telemetry_dispatch, None).await
}

#[wasm_bindgen(js_name = openMemoryFromSnapshot)]
pub async fn open_memory_from_snapshot(
    plugin_runtime_dispatch: Function,
    telemetry_dispatch: Option<Function>,
    snapshot: Option<Vec<u8>>,
) -> Result<WasmLix, JsValue> {
    console_error_panic_hook::set_once();
    let runtime = Arc::new(BrowserJsWasmRuntime::new(plugin_runtime_dispatch));
    let storage = match snapshot {
        Some(snapshot) => {
            Memory::from_snapshot(&snapshot).map_err(|error| lix_error_to_js(error.into()))?
        }
        None => Memory::new(),
    };
    let options = RsOpenLixOptions::new(storage.clone()).with_wasm_runtime(runtime);
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
    let inner = match telemetry {
        Some(telemetry) => open_lix_with_telemetry(options, telemetry).await,
        None => open_lix(options).await,
    }
    .map_err(lix_error_to_js)?;
    Ok(WasmLix { inner, storage })
}

struct BrowserTelemetryDispatch(Function);

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but the shared telemetry trait requires Send"
)]
unsafe impl Send for BrowserTelemetryDispatch {}
unsafe impl Sync for BrowserTelemetryDispatch {}

#[wasm_bindgen(js_name = parseSqlScript)]
pub fn parse_sql_script(sql: String, provided_param_count: usize) -> Result<JsValue, JsValue> {
    let plan = parse_rs_sql_script(&sql, provided_param_count).map_err(lix_error_to_js)?;
    to_js(&SqlScriptPlanDto::from(plan))
}

#[wasm_bindgen]
impl WasmLix {
    #[wasm_bindgen(js_name = exportSnapshot)]
    pub async fn export_snapshot(&self) -> Result<Vec<u8>, JsValue> {
        self.storage
            .export_snapshot()
            .map_err(|error| lix_error_to_js(error.into()))
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
        let result = self
            .inner
            .execute_with_options(&sql, &params, options)
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
        let results = self
            .inner
            .execute_batch_with_options(&statements, options)
            .await
            .map_err(lix_error_to_js)?;
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

    #[wasm_bindgen(js_name = close)]
    pub async fn close(&self) -> Result<(), JsValue> {
        self.inner.close().await.map_err(lix_error_to_js)
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
        let result = inner
            .execute_with_options(&sql, &params, options)
            .await
            .map_err(lix_error_to_js)?;
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlScriptPlanDto {
    statements: Vec<SqlScriptStatementDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlScriptStatementDto {
    sql: String,
    param_start: usize,
    param_end: usize,
}

impl From<SqlScriptPlan> for SqlScriptPlanDto {
    fn from(plan: SqlScriptPlan) -> Self {
        Self {
            statements: plan
                .statements
                .into_iter()
                .map(|statement| SqlScriptStatementDto {
                    sql: statement.sql,
                    param_start: statement.params.start,
                    param_end: statement.params.end,
                })
                .collect(),
        }
    }
}

fn execute_options_from_js(options: Option<JsValue>) -> Result<RsExecuteOptions, JsValue> {
    match options {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            let options: ExecuteOptionsDto = from_js(value)?;
            Ok(RsExecuteOptions {
                origin_key: options.origin_key,
            })
        }
        _ => Ok(RsExecuteOptions::default()),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchOptionsDto {
    id: Option<String>,
    name: String,
    from_commit_id: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateBranchReceiptDto {
    id: String,
    name: String,
    hidden: bool,
    commit_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchOptionsDto {
    branch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SwitchBranchReceiptDto {
    branch_id: String,
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

impl From<lix_sdk::MergeBranchReceipt> for MergeBranchReceiptDto {
    fn from(receipt: lix_sdk::MergeBranchReceipt) -> Self {
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

impl From<lix_sdk::MergeBranchPreview> for MergeBranchPreviewDto {
    fn from(preview: lix_sdk::MergeBranchPreview) -> Self {
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

impl From<lix_sdk::MergeChangeStats> for MergeChangeStatsDto {
    fn from(stats: lix_sdk::MergeChangeStats) -> Self {
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
    entity_pk: serde_json::Value,
    file_id: Option<String>,
    target: MergeConflictSideDto,
    source: MergeConflictSideDto,
}

impl From<lix_sdk::MergeConflict> for MergeConflictDto {
    fn from(conflict: lix_sdk::MergeConflict) -> Self {
        Self {
            kind: "sameEntityChanged",
            schema_key: conflict.schema_key,
            entity_pk: conflict.entity_pk,
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

impl From<lix_sdk::MergeConflictSide> for MergeConflictSideDto {
    fn from(side: lix_sdk::MergeConflictSide) -> Self {
        let kind = match side.kind {
            lix_sdk::MergeConflictChangeKind::Added => "added",
            lix_sdk::MergeConflictChangeKind::Modified => "modified",
            lix_sdk::MergeConflictChangeKind::Removed => "removed",
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

fn values_from_js(value: JsValue) -> Result<Vec<Value>, JsValue> {
    let values: Vec<LixValueDto> = from_js(value)?;
    values
        .into_iter()
        .map(Value::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(lix_error_to_js)
}

fn batch_statements_from_js(value: JsValue) -> Result<Vec<RsExecuteBatchStatement>, JsValue> {
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
            Ok(RsExecuteBatchStatement { sql, params })
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
            "json" => Ok(Self::Json(value.value.unwrap_or(serde_json::Value::Null))),
            "blob" => value
                .blob
                .map(|bytes| Self::Blob(bytes.into_vec()))
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
            Value::Json(value) => ("json", Some(value.clone()), None),
            Value::Blob(value) => ("blob", None, Some(ByteBuf::from(value.clone()))),
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
struct ExecuteResultDto {
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

fn execute_result_to_js(result: RsExecuteResult) -> Result<JsValue, JsValue> {
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

fn invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message.into())
}

fn transaction_closed_error() -> JsValue {
    lix_error_to_js(LixError::new(
        "LIX_INVALID_TRANSACTION_STATE",
        "Lix transaction is closed",
    ))
}

fn observe_next_in_flight_error() -> JsValue {
    lix_error_to_js(
        LixError::new(
            "LIX_OBSERVE_NEXT_IN_FLIGHT",
            "ObserveEvents.next() is already in flight",
        )
        .with_hint("Await the pending next() call before calling next() again."),
    )
}

fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|error| js_bridge_error(format!("invalid JavaScript value: {error}")))
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
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

fn lix_error_to_js(error: LixError) -> JsValue {
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
    if let Some(details) = error.details
        && let Ok(details) = to_js(&details)
    {
        let _ = Reflect::set(object, &JsValue::from_str("details"), &details);
    }
    js_error.into()
}

#[derive(Clone)]
struct BrowserJsWasmRuntime {
    dispatch: Function,
}

impl BrowserJsWasmRuntime {
    fn new(dispatch: Function) -> Self {
        Self { dispatch }
    }

    async fn dispatch(
        &self,
        operation: &str,
        request: &PluginRuntimeRequestDto,
    ) -> Result<PluginRuntimeResponseDto, LixError> {
        let request = to_js(request).map_err(|error| js_runtime_error(operation, error))?;
        let promise = self
            .dispatch
            .call1(&JsValue::UNDEFINED, &request)
            .map(|value| Promise::resolve(&value))
            .map_err(|error| js_runtime_error(operation, error))?;
        let response = SendJsFuture(JsFuture::from(promise))
            .await
            .map_err(|error| js_runtime_error(operation, error))?;
        let response: PluginRuntimeResponseDto = serde_wasm_bindgen::from_value(response)
            .map_err(|error| js_runtime_error(operation, error))?;
        if let Some(message) = response.error_message.as_deref() {
            return Err(js_runtime_error(operation, message));
        }
        Ok(response)
    }
}

// Browser WASM is single-threaded. The SDK's runtime trait requires Send futures
// for native implementations, so this wrapper records the target invariant.
struct SendJsFuture(JsFuture);

#[expect(
    clippy::non_send_fields_in_send_ty,
    reason = "browser WASM is single-threaded but the shared runtime trait requires Send"
)]
unsafe impl Send for SendJsFuture {}

impl Future for SendJsFuture {
    type Output = Result<JsValue, JsValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

unsafe impl Send for BrowserJsWasmRuntime {}
unsafe impl Sync for BrowserJsWasmRuntime {}

#[async_trait]
impl WasmRuntime for BrowserJsWasmRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let response = self
            .dispatch(
                "init-component",
                &PluginRuntimeRequestDto {
                    operation: "initComponent",
                    component_id: None,
                    component_bytes: Some(ByteBuf::from(bytes)),
                    max_memory_bytes: Some(limits.max_memory_bytes.to_string()),
                    max_fuel: limits.max_fuel.map(|value| value.to_string()),
                    timeout_ms: limits.timeout_ms.map(|value| value.to_string()),
                    state: None,
                    file: None,
                },
            )
            .await?;
        let component_id = response.component_id.ok_or_else(|| {
            js_runtime_error("init-component", "response did not include a component id")
        })?;
        Ok(Arc::new(BrowserJsWasmComponent {
            component_id,
            runtime: self.clone(),
        }))
    }
}

struct BrowserJsWasmComponent {
    component_id: u32,
    runtime: BrowserJsWasmRuntime,
}

#[async_trait]
impl WasmComponentInstance for BrowserJsWasmComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let response = self
            .runtime
            .dispatch(
                "detect-changes",
                &PluginRuntimeRequestDto {
                    operation: "detectChanges",
                    component_id: Some(self.component_id),
                    component_bytes: None,
                    max_memory_bytes: None,
                    max_fuel: None,
                    timeout_ms: None,
                    state: Some(state.into_iter().map(Into::into).collect()),
                    file: Some(file.into()),
                },
            )
            .await?;
        Ok(response
            .changes
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        let response = self
            .runtime
            .dispatch(
                "render",
                &PluginRuntimeRequestDto {
                    operation: "render",
                    component_id: Some(self.component_id),
                    component_bytes: None,
                    max_memory_bytes: None,
                    max_fuel: None,
                    timeout_ms: None,
                    state: Some(state.into_iter().map(Into::into).collect()),
                    file: None,
                },
            )
            .await?;
        response
            .bytes
            .map(ByteBuf::into_vec)
            .ok_or_else(|| js_runtime_error("render", "response did not include bytes"))
    }
}

impl Drop for BrowserJsWasmComponent {
    fn drop(&mut self) {
        if let Ok(request) = to_js(&PluginRuntimeRequestDto {
            operation: "closeComponent",
            component_id: Some(self.component_id),
            component_bytes: None,
            max_memory_bytes: None,
            max_fuel: None,
            timeout_ms: None,
            state: None,
            file: None,
        }) {
            let _ = self.runtime.dispatch.call1(&JsValue::UNDEFINED, &request);
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginRuntimeRequestDto {
    operation: &'static str,
    component_id: Option<u32>,
    component_bytes: Option<ByteBuf>,
    max_memory_bytes: Option<String>,
    max_fuel: Option<String>,
    timeout_ms: Option<String>,
    state: Option<Vec<PluginEntityStateDto>>,
    file: Option<PluginFileDto>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluginRuntimeResponseDto {
    component_id: Option<u32>,
    changes: Option<Vec<PluginDetectedChangeDto>>,
    bytes: Option<ByteBuf>,
    error_message: Option<String>,
}

#[derive(Serialize)]
struct PluginFileDto {
    filename: Option<String>,
    data: ByteBuf,
}

impl From<WasmPluginFile> for PluginFileDto {
    fn from(file: WasmPluginFile) -> Self {
        Self {
            filename: file.filename,
            data: ByteBuf::from(file.data),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginEntityStateDto {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: String,
    metadata: Option<String>,
}

impl From<WasmPluginEntityState> for PluginEntityStateDto {
    fn from(state: WasmPluginEntityState) -> Self {
        Self {
            entity_pk: state.entity_pk,
            schema_key: state.schema_key,
            snapshot_content: state.snapshot_content,
            metadata: state.metadata,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PluginDetectedChangeDto {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<String>,
    metadata: Option<String>,
}

impl From<PluginDetectedChangeDto> for WasmPluginDetectedChange {
    fn from(change: PluginDetectedChangeDto) -> Self {
        Self {
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            snapshot_content: change.snapshot_content,
            metadata: change.metadata,
        }
    }
}

fn js_runtime_error(operation: &str, error: impl std::fmt::Debug) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("JavaScript WASM runtime {operation} failed: {error:?}"),
    )
}
