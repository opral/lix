#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use js_sys::{Array, Object, Reflect};
    use lix_rs_sdk::{
        open_lix as open_lix_rs, Backend, BackendKvGetRequest, BackendKvGetResult,
        BackendKvGetResultGroup, BackendKvPair, BackendKvScanRange, BackendKvScanRequest,
        BackendKvScanResult, BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction,
        BackendWriteTransaction, CreateVersionOptions, ExecuteResult, Lix as RsLix, LixError,
        MergeVersionOptions, MergeVersionPreviewOptions, OpenLixOptions, SwitchVersionOptions,
        Value,
    };
    use serde::Serialize;
    use serde_json::json;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;

    #[wasm_bindgen(typescript_custom_section)]
    const LIX_TYPES: &str = r#"
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export type LixValue =
  | { kind: "null"; value: null }
  | { kind: "boolean"; value: boolean }
  | { kind: "integer"; value: number }
  | { kind: "real"; value: number }
  | { kind: "text"; value: string }
  | { kind: "json"; value: JsonValue }
  | { kind: "blob"; base64: string };

export type ExecuteResult = {
  columns: string[];
  rows: LixValue[][];
  rowsAffected: number;
  notices: LixNotice[];
};

export type LixNotice = {
  code: string;
  message: string;
  hint?: string;
};

export type TransactionBeginMode = "read" | "write" | "deferred";

export type KvScanRange =
  | { kind: "prefix"; prefix: Uint8Array }
  | { kind: "range"; start: Uint8Array; end: Uint8Array };

export type KvPair = {
  key: Uint8Array;
  value: Uint8Array;
};

export type BackendTransaction = {
  kvGet(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
  kvScan(namespace: string, range: KvScanRange, limit?: number | null): KvPair[];
  kvPut(namespace: string, key: Uint8Array, value: Uint8Array): void;
  kvDelete(namespace: string, key: Uint8Array): void;
  commit(): void;
  rollback(): void;
};

export type Backend = {
  beginTransaction(mode: TransactionBeginMode): BackendTransaction;
  kvGet?(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
  kvScan?(namespace: string, range: KvScanRange, limit?: number | null): KvPair[];
  close?(): void;
};

export type OpenLixOptions = {
  backend?: Backend;
};

export type CreateVersionOptions = {
  id?: string;
  name: string;
  fromCommitId?: string;
};

export type CreateVersionResult = {
  id: string;
  name: string;
  hidden: boolean;
  commitId: string;
};

export type SwitchVersionOptions = {
  versionId: string;
};

export type SwitchVersionResult = {
  versionId: string;
};

export type MergeVersionOptions = {
  sourceVersionId: string;
};

export type MergeVersionOutcome =
  | "alreadyUpToDate"
  | "fastForward"
  | "mergeCommitted";

export type MergeVersionResult = {
  outcome: MergeVersionOutcome;
  targetVersionId: string;
  sourceVersionId: string;
  baseCommitId: string;
  targetHeadBeforeCommitId: string;
  sourceHeadBeforeCommitId: string;
  targetHeadAfterCommitId: string;
  createdMergeCommitId: string | null;
  changeStats: MergeChangeStats;
};

export type MergeVersionPreviewResult = {
  outcome: MergeVersionOutcome;
  targetVersionId: string;
  sourceVersionId: string;
  baseCommitId: string;
  targetHeadCommitId: string;
  sourceHeadCommitId: string;
  changeStats: MergeChangeStats;
  conflicts: MergeConflict[];
};

export type MergeChangeStats = {
  total: number;
  added: number;
  modified: number;
  removed: number;
};

export type MergeConflict = {
  kind: "sameEntityChanged";
  schemaKey: string;
  entityId: string;
  fileId: string | null;
  target: MergeConflictSide;
  source: MergeConflictSide;
};

export type MergeConflictSide = {
  kind: "added" | "modified" | "removed";
  beforeChangeId: string | null;
  afterChangeId: string | null;
};
"#;

    #[wasm_bindgen]
    pub struct Lix {
        inner: RsLix,
    }

    #[wasm_bindgen]
    impl Lix {
        /// Executes one DataFusion SQL statement against this Lix session.
        ///
        /// The SQL dialect is DataFusion SQL, not SQLite SQL. Positional
        /// placeholders use `$1`, `$2`, and so on. SQLite-specific catalog
        /// tables and transaction statements such as `sqlite_master`, `BEGIN`,
        /// and `COMMIT` are not part of this contract; use
        /// `information_schema` for catalog inspection.
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: JsValue, params: JsValue) -> Result<JsValue, JsValue> {
            let sql = sql
                .as_string()
                .ok_or_else(|| invalid_argument_error("execute", "sql", "string", &sql))
                .map_err(js_error)?;
            if !Array::is_array(&params) {
                return Err(js_error(invalid_argument_error(
                    "execute", "params", "array", &params,
                )));
            }
            let params = Array::from(&params);
            let values = params
                .iter()
                .map(value_from_js)
                .collect::<Result<Vec<_>, _>>()
                .map_err(js_error)?;
            let result = self.inner.execute(&sql, &values).await.map_err(js_error)?;
            execute_result_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = activeVersionId)]
        pub async fn active_version_id(&self) -> Result<String, JsValue> {
            self.inner.active_version_id().await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = createVersion)]
        pub async fn create_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_create_version_options(args).map_err(js_error)?;
            let result = self.inner.create_version(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "id", &result.id).map_err(js_error)?;
            set_string(&object, "name", &result.name).map_err(js_error)?;
            Reflect::set(
                &object,
                &JsValue::from_str("hidden"),
                &JsValue::from_bool(result.hidden),
            )
            .map_err(|_| js_error(js_sdk_error("could not set hidden")))?;
            set_string(&object, "commitId", &result.commit_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = switchVersion)]
        pub async fn switch_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_switch_version_options(args).map_err(js_error)?;
            let result = self.inner.switch_version(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "versionId", &result.version_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = mergeVersionPreview)]
        pub async fn merge_version_preview(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_merge_version_preview_options(args).map_err(js_error)?;
            let result = self
                .inner
                .merge_version_preview(options)
                .await
                .map_err(js_error)?;
            merge_version_preview_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = mergeVersion)]
        pub async fn merge_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_merge_version_options(args).map_err(js_error)?;
            let result = self.inner.merge_version(options).await.map_err(js_error)?;
            let object = Object::new();
            let outcome = match result.outcome {
                lix_rs_sdk::MergeVersionOutcome::AlreadyUpToDate => "alreadyUpToDate",
                lix_rs_sdk::MergeVersionOutcome::FastForward => "fastForward",
                lix_rs_sdk::MergeVersionOutcome::MergeCommitted => "mergeCommitted",
            };
            set_string(&object, "outcome", outcome).map_err(js_error)?;
            set_string(&object, "targetVersionId", &result.target_version_id).map_err(js_error)?;
            set_string(&object, "sourceVersionId", &result.source_version_id).map_err(js_error)?;
            set_string(&object, "baseCommitId", &result.base_commit_id).map_err(js_error)?;
            set_string(
                &object,
                "targetHeadBeforeCommitId",
                &result.target_head_before_commit_id,
            )
            .map_err(js_error)?;
            set_string(
                &object,
                "sourceHeadBeforeCommitId",
                &result.source_head_before_commit_id,
            )
            .map_err(js_error)?;
            set_string(
                &object,
                "targetHeadAfterCommitId",
                &result.target_head_after_commit_id,
            )
            .map_err(js_error)?;
            set_optional_string(
                &object,
                "createdMergeCommitId",
                result.created_merge_commit_id.as_deref(),
            )
            .map_err(js_error)?;
            Reflect::set(
                &object,
                &JsValue::from_str("changeStats"),
                &merge_change_stats_to_js(&result.change_stats).map_err(js_error)?,
            )
            .map_err(|_| js_error(js_sdk_error("could not set changeStats")))?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = close)]
        pub async fn close(&self) -> Result<(), JsValue> {
            self.inner.close().await.map_err(js_error)
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(args: Option<JsValue>) -> Result<Lix, JsValue> {
        let options = parse_open_lix_options(args).map_err(js_error)?;
        let inner = open_lix_rs(options).await.map_err(js_error)?;
        Ok(Lix { inner })
    }

    fn parse_open_lix_options(args: Option<JsValue>) -> Result<OpenLixOptions, LixError> {
        let Some(value) = args else {
            return Ok(OpenLixOptions::default());
        };
        if value.is_undefined() || value.is_null() {
            return Ok(OpenLixOptions::default());
        }
        if !value.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                "openLix() options must be an object",
            ));
        }
        let backend = Reflect::get(&value, &JsValue::from_str("backend"))
            .map_err(|_| js_sdk_error("openLix() could not read backend"))?;
        if backend.is_undefined() || backend.is_null() {
            return Ok(OpenLixOptions::default());
        }
        if !backend.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                "openLix() backend must be an object",
            ));
        }
        Ok(OpenLixOptions {
            backend: Some(Box::new(JsBackend::new(backend))),
        })
    }

    struct JsBackend {
        inner: JsValue,
    }

    impl JsBackend {
        fn new(inner: JsValue) -> Self {
            Self { inner }
        }
    }

    unsafe impl Send for JsBackend {}
    unsafe impl Sync for JsBackend {}

    #[async_trait]
    impl Backend for JsBackend {
        async fn begin_read_transaction(
            &self,
        ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
            let transaction =
                call_method1(&self.inner, "beginTransaction", &JsValue::from_str("read"))?;
            if transaction.is_null() || transaction.is_undefined() || !transaction.is_object() {
                return Err(js_sdk_error(
                    "backend.beginTransaction() must return a transaction object",
                ));
            }
            Ok(Box::new(JsBackendTransaction { inner: transaction }))
        }

        async fn begin_write_transaction(
            &self,
        ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
            let transaction =
                call_method1(&self.inner, "beginTransaction", &JsValue::from_str("write"))?;
            if transaction.is_null() || transaction.is_undefined() || !transaction.is_object() {
                return Err(js_sdk_error(
                    "backend.beginTransaction() must return a transaction object",
                ));
            }
            Ok(Box::new(JsBackendTransaction { inner: transaction }))
        }

        async fn close(&self) -> Result<(), LixError> {
            let method = Reflect::get(&self.inner, &JsValue::from_str("close"))
                .map_err(|_| js_sdk_error("backend.close could not be read"))?;
            if method.is_undefined() || method.is_null() {
                return Ok(());
            }
            call_function0(&method, &self.inner)?;
            Ok(())
        }
    }

    struct JsBackendTransaction {
        inner: JsValue,
    }

    unsafe impl Send for JsBackendTransaction {}
    unsafe impl Sync for JsBackendTransaction {}

    #[async_trait]
    impl BackendReadTransaction for JsBackendTransaction {
        async fn get_kv_many(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvGetResult, LixError> {
            let mut groups = Vec::with_capacity(request.groups.len());
            for group in request.groups {
                let mut values = Vec::with_capacity(group.keys.len());
                for key in group.keys {
                    values.push(js_value_to_optional_bytes(
                        call_method2(
                            &self.inner,
                            "kvGet",
                            &JsValue::from_str(&group.namespace),
                            &bytes_to_js(&key),
                        )?,
                        "transaction.kvGet",
                    )?);
                }
                groups.push(BackendKvGetResultGroup {
                    namespace: group.namespace,
                    values,
                });
            }
            Ok(BackendKvGetResult { groups })
        }

        async fn scan_kv(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvScanResult, LixError> {
            let scan_limit = request
                .limit
                .checked_add(1 + usize::from(request.after.is_some()))
                .unwrap_or(request.limit);
            let mut rows = js_value_to_kv_pairs(
                call_method3(
                    &self.inner,
                    "kvScan",
                    &JsValue::from_str(&request.namespace),
                    &kv_scan_range_to_js(&request.range)?,
                    &usize_to_js(scan_limit),
                )?,
                "transaction.kvScan",
            )?
            .into_iter()
            .filter(|row| {
                request
                    .after
                    .as_deref()
                    .is_none_or(|after| row.key.as_slice() > after)
            })
            .collect::<Vec<_>>();
            let has_more = rows.len() > request.limit;
            rows.truncate(request.limit);
            let resume_after = has_more
                .then(|| rows.last().map(|row| row.key.clone()))
                .flatten();
            Ok(BackendKvScanResult { rows, resume_after })
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            call_method0(&self.inner, "rollback")?;
            Ok(())
        }
    }

    #[async_trait]
    impl BackendWriteTransaction for JsBackendTransaction {
        async fn write_kv_batch(
            &mut self,
            batch: BackendKvWriteBatch,
        ) -> Result<BackendKvWriteStats, LixError> {
            let mut stats = BackendKvWriteStats::default();
            for group in batch.groups {
                for put in group.puts {
                    stats.puts += 1;
                    stats.bytes_written += put.key.len() + put.value.len();
                    call_method3(
                        &self.inner,
                        "kvPut",
                        &JsValue::from_str(&group.namespace),
                        &bytes_to_js(&put.key),
                        &bytes_to_js(&put.value),
                    )?;
                }
                for key in group.deletes {
                    stats.deletes += 1;
                    stats.bytes_written += key.len();
                    call_method2(
                        &self.inner,
                        "kvDelete",
                        &JsValue::from_str(&group.namespace),
                        &bytes_to_js(&key),
                    )?;
                }
            }
            Ok(stats)
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            call_method0(&self.inner, "commit")?;
            Ok(())
        }
    }

    fn call_method0(receiver: &JsValue, method_name: &str) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function0(&method, receiver)
    }

    fn call_method1(
        receiver: &JsValue,
        method_name: &str,
        arg1: &JsValue,
    ) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function1(&method, receiver, arg1)
    }

    fn call_method2(
        receiver: &JsValue,
        method_name: &str,
        arg1: &JsValue,
        arg2: &JsValue,
    ) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function2(&method, receiver, arg1, arg2)
    }

    fn call_method3(
        receiver: &JsValue,
        method_name: &str,
        arg1: &JsValue,
        arg2: &JsValue,
        arg3: &JsValue,
    ) -> Result<JsValue, LixError> {
        let method = Reflect::get(receiver, &JsValue::from_str(method_name))
            .map_err(|_| js_sdk_error(format!("{method_name} could not be read")))?;
        call_function3(&method, receiver, arg1, arg2, arg3)
    }

    fn call_function0(function: &JsValue, receiver: &JsValue) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(function.call0(receiver).map_err(js_to_lix_error)?)
    }

    fn call_function1(
        function: &JsValue,
        receiver: &JsValue,
        arg1: &JsValue,
    ) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(function.call1(receiver, arg1).map_err(js_to_lix_error)?)
    }

    fn call_function2(
        function: &JsValue,
        receiver: &JsValue,
        arg1: &JsValue,
        arg2: &JsValue,
    ) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(
            function
                .call2(receiver, arg1, arg2)
                .map_err(js_to_lix_error)?,
        )
    }

    fn call_function3(
        function: &JsValue,
        receiver: &JsValue,
        arg1: &JsValue,
        arg2: &JsValue,
        arg3: &JsValue,
    ) -> Result<JsValue, LixError> {
        let function = function
            .dyn_ref::<js_sys::Function>()
            .ok_or_else(|| js_sdk_error("backend method must be a function"))?;
        reject_promise(
            function
                .call3(receiver, arg1, arg2, arg3)
                .map_err(js_to_lix_error)?,
        )
    }

    fn reject_promise(value: JsValue) -> Result<JsValue, LixError> {
        if value.is_instance_of::<js_sys::Promise>() {
            return Err(js_sdk_error(
                "JavaScript Backend methods must return synchronously",
            ));
        }
        Ok(value)
    }

    fn bytes_to_js(bytes: &[u8]) -> JsValue {
        js_sys::Uint8Array::from(bytes).into()
    }

    fn js_value_to_optional_bytes(
        value: JsValue,
        context: &str,
    ) -> Result<Option<Vec<u8>>, LixError> {
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        Ok(Some(js_value_to_bytes(value, context)?))
    }

    fn js_value_to_bytes(value: JsValue, context: &str) -> Result<Vec<u8>, LixError> {
        if !value.is_instance_of::<js_sys::Uint8Array>() {
            return Err(js_sdk_error(format!("{context} must return Uint8Array")));
        }
        Ok(js_sys::Uint8Array::from(value).to_vec())
    }

    fn usize_to_js(value: usize) -> JsValue {
        JsValue::from_f64(value as f64)
    }

    fn kv_scan_range_to_js(range: &BackendKvScanRange) -> Result<JsValue, LixError> {
        let object = Object::new();
        match range {
            BackendKvScanRange::Prefix(prefix) => {
                set_string(&object, "kind", "prefix")?;
                Reflect::set(&object, &JsValue::from_str("prefix"), &bytes_to_js(prefix))
                    .map_err(|_| js_sdk_error("could not set range.prefix"))?;
            }
            BackendKvScanRange::Range { start, end } => {
                set_string(&object, "kind", "range")?;
                Reflect::set(&object, &JsValue::from_str("start"), &bytes_to_js(start))
                    .map_err(|_| js_sdk_error("could not set range.start"))?;
                Reflect::set(&object, &JsValue::from_str("end"), &bytes_to_js(end))
                    .map_err(|_| js_sdk_error("could not set range.end"))?;
            }
        }
        Ok(object.into())
    }

    fn js_value_to_kv_pairs(value: JsValue, context: &str) -> Result<Vec<BackendKvPair>, LixError> {
        if !Array::is_array(&value) {
            return Err(js_sdk_error(format!("{context} must return an array")));
        }
        Array::from(&value)
            .iter()
            .map(|row| {
                if row.is_null() || row.is_undefined() || !row.is_object() {
                    return Err(js_sdk_error(format!("{context} rows must be objects")));
                }
                let key = Reflect::get(&row, &JsValue::from_str("key"))
                    .map_err(|_| js_sdk_error(format!("{context} row key could not be read")))?;
                let value = Reflect::get(&row, &JsValue::from_str("value"))
                    .map_err(|_| js_sdk_error(format!("{context} row value could not be read")))?;
                Ok(BackendKvPair::new(
                    js_value_to_bytes(key, "kv pair key")?,
                    js_value_to_bytes(value, "kv pair value")?,
                ))
            })
            .collect()
    }

    fn js_to_lix_error(value: JsValue) -> LixError {
        if let Some(message) = value.as_string() {
            return js_sdk_error(message);
        }
        let code = Reflect::get(&value, &JsValue::from_str("code"))
            .ok()
            .and_then(|code| code.as_string());
        let message = Reflect::get(&value, &JsValue::from_str("message"))
            .ok()
            .and_then(|message| message.as_string())
            .unwrap_or_else(|| "JavaScript backend error".to_string());
        let hint = Reflect::get(&value, &JsValue::from_str("hint"))
            .ok()
            .and_then(|hint| hint.as_string());
        let details = Reflect::get(&value, &JsValue::from_str("details"))
            .ok()
            .and_then(|details| {
                if details.is_undefined() || details.is_null() {
                    None
                } else {
                    serde_wasm_bindgen::from_value(details).ok()
                }
            });
        let mut error = LixError::new(
            code.unwrap_or_else(|| "LIX_ERROR_JS_SDK".to_string()),
            message,
        );
        if let Some(hint) = hint {
            error = error.with_hint(hint);
        }
        if let Some(details) = details {
            error = error.with_details(details);
        }
        error
    }

    fn parse_create_version_options(value: JsValue) -> Result<CreateVersionOptions, LixError> {
        let object = expect_object(value, "createVersion")?;
        let id = optional_string(&object, "id", "createVersion")?;
        let name = required_string(&object, "name", "createVersion")?;
        let from_commit_id = optional_string(&object, "fromCommitId", "createVersion")?;
        Ok(CreateVersionOptions {
            id,
            name,
            from_commit_id,
        })
    }

    fn parse_switch_version_options(value: JsValue) -> Result<SwitchVersionOptions, LixError> {
        let object = expect_object(value, "switchVersion")?;
        let version_id = required_string(&object, "versionId", "switchVersion")?;
        Ok(SwitchVersionOptions { version_id })
    }

    fn parse_merge_version_options(value: JsValue) -> Result<MergeVersionOptions, LixError> {
        let object = expect_object(value, "mergeVersion")?;
        let source_version_id = required_string(&object, "sourceVersionId", "mergeVersion")?;
        Ok(MergeVersionOptions { source_version_id })
    }

    fn parse_merge_version_preview_options(
        value: JsValue,
    ) -> Result<MergeVersionPreviewOptions, LixError> {
        let object = expect_object(value, "mergeVersionPreview")?;
        let source_version_id = required_string(&object, "sourceVersionId", "mergeVersionPreview")?;
        Ok(MergeVersionPreviewOptions { source_version_id })
    }

    fn expect_object(value: JsValue, method: &str) -> Result<Object, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() options must be an object"),
            ));
        }
        Ok(Object::from(value))
    }

    fn invalid_argument_error(
        operation: &str,
        argument: &str,
        expected: &str,
        actual_value: &JsValue,
    ) -> LixError {
        LixError::new(
            "LIX_INVALID_ARGUMENT",
            format!(
                "lix.{operation}() expected {argument} to be {} {expected}",
                expected_article(expected)
            ),
        )
        .with_details(json!({
            "operation": operation,
            "argument": argument,
            "expected": expected,
            "actual": js_type_name(actual_value),
        }))
    }

    fn expected_article(expected: &str) -> &'static str {
        match expected.chars().next().map(|c| c.to_ascii_lowercase()) {
            Some('a' | 'e' | 'i' | 'o' | 'u') => "an",
            _ => "a",
        }
    }

    fn js_type_name(value: &JsValue) -> &'static str {
        if value.is_null() {
            "null"
        } else if Array::is_array(value) {
            "array"
        } else if value.is_undefined() {
            "undefined"
        } else if value.is_string() {
            "string"
        } else if value.as_bool().is_some() {
            "boolean"
        } else if value.as_f64().is_some() {
            "number"
        } else if value.is_function() {
            "function"
        } else if value.is_object() {
            "object"
        } else {
            "unknown"
        }
    }

    fn required_string(object: &Object, key: &str, method: &str) -> Result<String, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|_| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() could not read {key}"),
            )
        })?;
        if let Some(value) = value.as_string() {
            if !value.is_empty() {
                return Ok(value);
            }
        }
        Err(LixError::new(
            "LIX_ERROR_JS_SDK",
            format!("{method}() requires non-empty string {key}"),
        ))
    }

    fn optional_string(
        object: &Object,
        key: &str,
        method: &str,
    ) -> Result<Option<String>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(|_| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() could not read {key}"),
            )
        })?;
        if value.is_undefined() || value.is_null() {
            return Ok(None);
        }
        if let Some(value) = value.as_string() {
            if !value.is_empty() {
                return Ok(Some(value));
            }
        }
        Err(LixError::new(
            "LIX_ERROR_JS_SDK",
            format!("{method}() requires {key} to be a non-empty string when provided"),
        ))
    }

    fn value_from_js(value: JsValue) -> Result<Value, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(invalid_param(
                "parameter must be an explicit Lix value object",
                &value,
            ));
        }

        let object = Object::from(value.clone());
        let kind = Reflect::get(&object, &JsValue::from_str("kind"))
            .ok()
            .and_then(|value| value.as_string());
        match kind.as_deref() {
            Some("null") => Ok(Value::Null),
            Some("boolean") => Ok(Value::Boolean(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| invalid_param("boolean value must be boolean", &value))?,
            )),
            Some("integer") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_param("integer value must be number", &value))?;
                if !value.is_finite() || value.fract() != 0.0 {
                    return Err(invalid_param_message(
                        "integer value must be a finite integer",
                    ));
                }
                Ok(Value::Integer(value as i64))
            }
            Some("real") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_param("real value must be number", &value))?;
                if !value.is_finite() {
                    return Err(invalid_param_message("real value must be a finite number"));
                }
                Ok(Value::Real(value))
            }
            Some("text") => Ok(Value::Text(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_param("text value must be string", &value))?,
            )),
            Some("json") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .map_err(|_| invalid_param("json value is missing", &value))?;
                let json = serde_wasm_bindgen::from_value(value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("json value must be JSON-serializable: {error}"),
                    )
                })?;
                Ok(Value::Json(json))
            }
            Some("blob") => {
                let base64 = Reflect::get(&object, &JsValue::from_str("base64"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_param("blob base64 must be string", &value))?;
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_INVALID_PARAM,
                                format!("blob base64 must be valid base64: {error}"),
                            )
                        })?;
                Ok(Value::Blob(bytes))
            }
            _ => Err(invalid_param(
                "parameter must be an explicit Lix value object",
                &value,
            )),
        }
    }

    fn execute_result_to_js(result: ExecuteResult) -> Result<JsValue, LixError> {
        let object = Object::new();
        let columns = Array::new();
        for column in result.columns() {
            columns.push(&JsValue::from_str(column));
        }
        Reflect::set(&object, &JsValue::from_str("columns"), &columns)
            .map_err(|_| js_sdk_error("could not set columns"))?;
        let values = Array::new();
        for row in result.rows() {
            let row_values = Array::new();
            for value in row.values() {
                row_values.push(&value_to_js(value)?);
            }
            values.push(&row_values);
        }
        Reflect::set(&object, &JsValue::from_str("rows"), &values)
            .map_err(|_| js_sdk_error("could not set rows"))?;
        set_number(&object, "rowsAffected", result.rows_affected() as f64)?;
        let notices = Array::new();
        for notice in result.notices() {
            let notice_object = Object::new();
            set_string(&notice_object, "code", &notice.code)?;
            set_string(&notice_object, "message", &notice.message)?;
            if let Some(hint) = &notice.hint {
                set_string(&notice_object, "hint", hint)?;
            }
            notices.push(&notice_object);
        }
        Reflect::set(&object, &JsValue::from_str("notices"), &notices)
            .map_err(|_| js_sdk_error("could not set notices"))?;
        Ok(object.into())
    }

    fn merge_version_preview_to_js(
        result: lix_rs_sdk::MergeVersionPreview,
    ) -> Result<JsValue, LixError> {
        let object = Object::new();
        let outcome = match result.outcome {
            lix_rs_sdk::MergeVersionOutcome::AlreadyUpToDate => "alreadyUpToDate",
            lix_rs_sdk::MergeVersionOutcome::FastForward => "fastForward",
            lix_rs_sdk::MergeVersionOutcome::MergeCommitted => "mergeCommitted",
        };
        set_string(&object, "outcome", outcome)?;
        set_string(&object, "targetVersionId", &result.target_version_id)?;
        set_string(&object, "sourceVersionId", &result.source_version_id)?;
        set_string(&object, "baseCommitId", &result.base_commit_id)?;
        set_string(&object, "targetHeadCommitId", &result.target_head_commit_id)?;
        set_string(&object, "sourceHeadCommitId", &result.source_head_commit_id)?;
        Reflect::set(
            &object,
            &JsValue::from_str("changeStats"),
            &merge_change_stats_to_js(&result.change_stats)?,
        )
        .map_err(|_| js_sdk_error("could not set changeStats"))?;
        let conflicts = Array::new();
        for conflict in result.conflicts {
            conflicts.push(&merge_conflict_to_js(&conflict)?);
        }
        Reflect::set(&object, &JsValue::from_str("conflicts"), &conflicts)
            .map_err(|_| js_sdk_error("could not set conflicts"))?;
        Ok(object.into())
    }

    fn merge_change_stats_to_js(stats: &lix_rs_sdk::MergeChangeStats) -> Result<JsValue, LixError> {
        let object = Object::new();
        set_number(&object, "total", stats.total as f64)?;
        set_number(&object, "added", stats.added as f64)?;
        set_number(&object, "modified", stats.modified as f64)?;
        set_number(&object, "removed", stats.removed as f64)?;
        Ok(object.into())
    }

    fn merge_conflict_to_js(conflict: &lix_rs_sdk::MergeConflict) -> Result<JsValue, LixError> {
        let object = Object::new();
        let kind = match conflict.kind {
            lix_rs_sdk::MergeConflictKind::SameEntityChanged => "sameEntityChanged",
        };
        set_string(&object, "kind", kind)?;
        set_string(&object, "schemaKey", &conflict.schema_key)?;
        set_string(&object, "entityId", &conflict.entity_id)?;
        set_optional_string(&object, "fileId", conflict.file_id.as_deref())?;
        Reflect::set(
            &object,
            &JsValue::from_str("target"),
            &merge_conflict_side_to_js(&conflict.target)?,
        )
        .map_err(|_| js_sdk_error("could not set target conflict side"))?;
        Reflect::set(
            &object,
            &JsValue::from_str("source"),
            &merge_conflict_side_to_js(&conflict.source)?,
        )
        .map_err(|_| js_sdk_error("could not set source conflict side"))?;
        Ok(object.into())
    }

    fn merge_conflict_side_to_js(
        side: &lix_rs_sdk::MergeConflictSide,
    ) -> Result<JsValue, LixError> {
        let object = Object::new();
        let kind = match side.kind {
            lix_rs_sdk::MergeConflictChangeKind::Added => "added",
            lix_rs_sdk::MergeConflictChangeKind::Modified => "modified",
            lix_rs_sdk::MergeConflictChangeKind::Removed => "removed",
        };
        set_string(&object, "kind", kind)?;
        set_optional_string(&object, "beforeChangeId", side.before_change_id.as_deref())?;
        set_optional_string(&object, "afterChangeId", side.after_change_id.as_deref())?;
        Ok(object.into())
    }

    fn value_to_js(value: &Value) -> Result<JsValue, LixError> {
        let object = Object::new();
        match value {
            Value::Null => {
                set_string(&object, "kind", "null")?;
                Reflect::set(&object, &JsValue::from_str("value"), &JsValue::NULL)
                    .map_err(|_| js_sdk_error("could not set null value"))?;
            }
            Value::Boolean(value) => {
                set_string(&object, "kind", "boolean")?;
                Reflect::set(
                    &object,
                    &JsValue::from_str("value"),
                    &JsValue::from_bool(*value),
                )
                .map_err(|_| js_sdk_error("could not set boolean value"))?;
            }
            Value::Integer(value) => {
                set_string(&object, "kind", "integer")?;
                set_number(&object, "value", *value as f64)?;
            }
            Value::Real(value) => {
                set_string(&object, "kind", "real")?;
                set_number(&object, "value", *value)?;
            }
            Value::Text(value) => {
                set_string(&object, "kind", "text")?;
                set_string(&object, "value", value)?;
            }
            Value::Json(value) => {
                set_string(&object, "kind", "json")?;
                let serializer = serde_wasm_bindgen::Serializer::json_compatible();
                let value = value.serialize(&serializer).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_JS_SDK",
                        format!("could not serialize JSON value: {error}"),
                    )
                })?;
                Reflect::set(&object, &JsValue::from_str("value"), &value)
                    .map_err(|_| js_sdk_error("could not set json value"))?;
            }
            Value::Blob(value) => {
                set_string(&object, "kind", "blob")?;
                set_string(
                    &object,
                    "base64",
                    &base64::Engine::encode(&base64::engine::general_purpose::STANDARD, value),
                )?;
            }
        }
        Ok(object.into())
    }

    fn set_string(object: &Object, key: &str, value: &str) -> Result<(), LixError> {
        Reflect::set(object, &JsValue::from_str(key), &JsValue::from_str(value))
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn set_optional_string(
        object: &Object,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), LixError> {
        let value = value.map(JsValue::from_str).unwrap_or(JsValue::NULL);
        Reflect::set(object, &JsValue::from_str(key), &value)
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn set_number(object: &Object, key: &str, value: f64) -> Result<(), LixError> {
        Reflect::set(object, &JsValue::from_str(key), &JsValue::from_f64(value))
            .map(|_| ())
            .map_err(|_| js_sdk_error(format!("could not set {key}")))
    }

    fn invalid_param(message: impl Into<String>, value: &JsValue) -> LixError {
        LixError::new(LixError::CODE_INVALID_PARAM, message.into()).with_details(json!({
            "operation": "execute",
            "actual": js_type_name(value),
        }))
    }

    fn invalid_param_message(message: impl Into<String>) -> LixError {
        LixError::new(LixError::CODE_INVALID_PARAM, message.into()).with_details(json!({
            "operation": "execute",
        }))
    }

    fn js_sdk_error(message: impl Into<String>) -> LixError {
        LixError::new("LIX_ERROR_JS_SDK", message.into())
    }

    fn js_error(error: LixError) -> JsValue {
        let js_error = js_sys::Error::new(&error.message);
        let object: &Object = js_error.as_ref();
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
            let serializer = serde_wasm_bindgen::Serializer::json_compatible();
            if let Ok(value) = details.serialize(&serializer) {
                let _ = Reflect::set(object, &JsValue::from_str("details"), &value);
            }
        }
        js_error.into()
    }
}
