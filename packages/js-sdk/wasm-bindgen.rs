#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use js_sys::{Array, Object, Reflect};
    use lix_rs_sdk::{
        open_lix as open_lix_rs, CreateVersionOptions, ExecuteResult, KvPair, KvScanRange,
        Lix as RsLix, LixBackend, LixBackendTransaction, LixError, MergeVersionOptions,
        OpenLixOptions, SwitchVersionOptions, TransactionBeginMode, Value,
    };
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
};

export type TransactionBeginMode = "read" | "write" | "deferred";

export type KvScanRange =
  | { kind: "prefix"; prefix: Uint8Array }
  | { kind: "range"; start: Uint8Array; end: Uint8Array };

export type KvPair = {
  key: Uint8Array;
  value: Uint8Array;
};

export type LixBackendTransaction = {
  kvGet(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
  kvScan(namespace: string, range: KvScanRange, limit?: number | null): KvPair[];
  kvPut(namespace: string, key: Uint8Array, value: Uint8Array): void;
  kvDelete(namespace: string, key: Uint8Array): void;
  commit(): void;
  rollback(): void;
};

export type LixBackend = {
  beginTransaction(mode: TransactionBeginMode): LixBackendTransaction;
  kvGet?(namespace: string, key: Uint8Array): Uint8Array | null | undefined;
  kvScan?(namespace: string, range: KvScanRange, limit?: number | null): KvPair[];
  close?(): void;
};

export type OpenLixOptions = {
  backend?: LixBackend;
};

export type CreateVersionOptions = {
  id?: string;
  name: string;
};

export type CreateVersionResult = {
  versionId: string;
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

export type MergeVersionOutcome = "alreadyUpToDate" | "mergeCommitted";

export type MergeVersionResult = {
  outcome: MergeVersionOutcome;
  targetVersionId: string;
  sourceVersionId: string;
  mergeBaseCommitId: string | null;
  targetHeadBeforeCommitId: string;
  sourceHeadBeforeCommitId: string;
  targetHeadAfterCommitId: string;
  createdMergeCommitId: string | null;
  appliedChangeCount: number;
};
"#;

    #[wasm_bindgen]
    pub struct Lix {
        inner: RsLix,
    }

    #[wasm_bindgen]
    impl Lix {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: String, params: JsValue) -> Result<JsValue, JsValue> {
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
            set_string(&object, "versionId", &result.version_id).map_err(js_error)?;
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

        #[wasm_bindgen(js_name = mergeVersion)]
        pub async fn merge_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_merge_version_options(args).map_err(js_error)?;
            let result = self.inner.merge_version(options).await.map_err(js_error)?;
            let object = Object::new();
            let outcome = match result.outcome {
                lix_rs_sdk::MergeVersionOutcome::AlreadyUpToDate => "alreadyUpToDate",
                lix_rs_sdk::MergeVersionOutcome::MergeCommitted => "mergeCommitted",
            };
            set_string(&object, "outcome", outcome).map_err(js_error)?;
            set_string(&object, "targetVersionId", &result.target_version_id).map_err(js_error)?;
            set_string(&object, "sourceVersionId", &result.source_version_id).map_err(js_error)?;
            set_optional_string(
                &object,
                "mergeBaseCommitId",
                result.merge_base_commit_id.as_deref(),
            )
            .map_err(js_error)?;
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
            set_number(
                &object,
                "appliedChangeCount",
                result.applied_change_count as f64,
            )
            .map_err(js_error)?;
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
    impl LixBackend for JsBackend {
        async fn begin_transaction(
            &self,
            mode: TransactionBeginMode,
        ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
            let transaction = call_method1(
                &self.inner,
                "beginTransaction",
                &JsValue::from_str(transaction_mode_to_js(mode)),
            )?;
            if transaction.is_null() || transaction.is_undefined() || !transaction.is_object() {
                return Err(js_sdk_error(
                    "backend.beginTransaction() must return a transaction object",
                ));
            }
            Ok(Box::new(JsBackendTransaction {
                mode,
                inner: transaction,
            }))
        }

        async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
            {
                let method = Reflect::get(&self.inner, &JsValue::from_str("kvGet"))
                    .map_err(|_| js_sdk_error("backend.kvGet could not be read"))?;
                if !method.is_undefined() && !method.is_null() {
                    return js_value_to_optional_bytes(
                        call_function2(
                            &method,
                            &self.inner,
                            &JsValue::from_str(namespace),
                            &bytes_to_js(key),
                        )?,
                        "backend.kvGet",
                    );
                }
            }

            let mut tx = self.begin_transaction(TransactionBeginMode::Read).await?;
            let value = tx.kv_get(namespace, key).await;
            tx.rollback().await?;
            value
        }

        async fn kv_scan(
            &self,
            namespace: &str,
            range: KvScanRange,
            limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LixError> {
            {
                let method = Reflect::get(&self.inner, &JsValue::from_str("kvScan"))
                    .map_err(|_| js_sdk_error("backend.kvScan could not be read"))?;
                if !method.is_undefined() && !method.is_null() {
                    return js_value_to_kv_pairs(
                        call_function3(
                            &method,
                            &self.inner,
                            &JsValue::from_str(namespace),
                            &kv_scan_range_to_js(&range)?,
                            &optional_usize_to_js(limit),
                        )?,
                        "backend.kvScan",
                    );
                }
            }

            let mut tx = self.begin_transaction(TransactionBeginMode::Read).await?;
            let rows = tx.kv_scan(namespace, range, limit).await;
            tx.rollback().await?;
            rows
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
        mode: TransactionBeginMode,
        inner: JsValue,
    }

    unsafe impl Send for JsBackendTransaction {}
    unsafe impl Sync for JsBackendTransaction {}

    #[async_trait]
    impl LixBackendTransaction for JsBackendTransaction {
        fn mode(&self) -> TransactionBeginMode {
            self.mode
        }

        async fn kv_get(
            &mut self,
            namespace: &str,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, LixError> {
            js_value_to_optional_bytes(
                call_method2(
                    &self.inner,
                    "kvGet",
                    &JsValue::from_str(namespace),
                    &bytes_to_js(key),
                )?,
                "transaction.kvGet",
            )
        }

        async fn kv_scan(
            &mut self,
            namespace: &str,
            range: KvScanRange,
            limit: Option<usize>,
        ) -> Result<Vec<KvPair>, LixError> {
            js_value_to_kv_pairs(
                call_method3(
                    &self.inner,
                    "kvScan",
                    &JsValue::from_str(namespace),
                    &kv_scan_range_to_js(&range)?,
                    &optional_usize_to_js(limit),
                )?,
                "transaction.kvScan",
            )
        }

        async fn kv_put(
            &mut self,
            namespace: &str,
            key: &[u8],
            value: &[u8],
        ) -> Result<(), LixError> {
            call_method3(
                &self.inner,
                "kvPut",
                &JsValue::from_str(namespace),
                &bytes_to_js(key),
                &bytes_to_js(value),
            )?;
            Ok(())
        }

        async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
            call_method2(
                &self.inner,
                "kvDelete",
                &JsValue::from_str(namespace),
                &bytes_to_js(key),
            )?;
            Ok(())
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            call_method0(&self.inner, "commit")?;
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            call_method0(&self.inner, "rollback")?;
            Ok(())
        }
    }

    fn transaction_mode_to_js(mode: TransactionBeginMode) -> &'static str {
        match mode {
            TransactionBeginMode::Read => "read",
            TransactionBeginMode::Write => "write",
            TransactionBeginMode::Deferred => "deferred",
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
                "JavaScript LixBackend methods must return synchronously",
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

    fn optional_usize_to_js(value: Option<usize>) -> JsValue {
        value
            .map(|value| JsValue::from_f64(value as f64))
            .unwrap_or(JsValue::NULL)
    }

    fn kv_scan_range_to_js(range: &KvScanRange) -> Result<JsValue, LixError> {
        let object = Object::new();
        match range {
            KvScanRange::Prefix(prefix) => {
                set_string(&object, "kind", "prefix")?;
                Reflect::set(&object, &JsValue::from_str("prefix"), &bytes_to_js(prefix))
                    .map_err(|_| js_sdk_error("could not set range.prefix"))?;
            }
            KvScanRange::Range { start, end } => {
                set_string(&object, "kind", "range")?;
                Reflect::set(&object, &JsValue::from_str("start"), &bytes_to_js(start))
                    .map_err(|_| js_sdk_error("could not set range.start"))?;
                Reflect::set(&object, &JsValue::from_str("end"), &bytes_to_js(end))
                    .map_err(|_| js_sdk_error("could not set range.end"))?;
            }
        }
        Ok(object.into())
    }

    fn js_value_to_kv_pairs(value: JsValue, context: &str) -> Result<Vec<KvPair>, LixError> {
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
                Ok(KvPair::new(
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
        let message = Reflect::get(&value, &JsValue::from_str("message"))
            .ok()
            .and_then(|message| message.as_string())
            .unwrap_or_else(|| "JavaScript backend error".to_string());
        js_sdk_error(message)
    }

    fn parse_create_version_options(value: JsValue) -> Result<CreateVersionOptions, LixError> {
        let object = expect_object(value, "createVersion")?;
        let id = optional_string(&object, "id", "createVersion")?;
        let name = required_string(&object, "name", "createVersion")?;
        Ok(CreateVersionOptions { id, name })
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

    fn expect_object(value: JsValue, method: &str) -> Result<Object, LixError> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(LixError::new(
                "LIX_ERROR_JS_SDK",
                format!("{method}() options must be an object"),
            ));
        }
        Ok(Object::from(value))
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
        if value.is_null() || value.is_undefined() {
            return Ok(Value::Null);
        }
        if let Some(value) = value.as_bool() {
            return Ok(Value::Boolean(value));
        }
        if let Some(value) = value.as_f64() {
            if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                return Ok(Value::Integer(value as i64));
            }
            return Ok(Value::Real(value));
        }
        if let Some(value) = value.as_string() {
            return Ok(Value::Text(value));
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
                    .ok_or_else(|| invalid_value("boolean value must be boolean"))?,
            )),
            Some("integer") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_value("integer value must be number"))?;
                Ok(Value::Integer(value as i64))
            }
            Some("real") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_value("real value must be number"))?;
                Ok(Value::Real(value))
            }
            Some("text") => Ok(Value::Text(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_value("text value must be string"))?,
            )),
            Some("json") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .map_err(|_| invalid_value("json value is missing"))?;
                let json = serde_wasm_bindgen::from_value(value).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_JS_SDK",
                        format!("json value must be JSON-serializable: {error}"),
                    )
                })?;
                Ok(Value::Json(json))
            }
            Some("blob") => {
                let base64 = Reflect::get(&object, &JsValue::from_str("base64"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .ok_or_else(|| invalid_value("blob base64 must be string"))?;
                let bytes =
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64)
                        .map_err(|error| {
                            LixError::new(
                                "LIX_ERROR_JS_SDK",
                                format!("blob base64 must be valid base64: {error}"),
                            )
                        })?;
                Ok(Value::Blob(bytes))
            }
            _ => {
                let json = serde_wasm_bindgen::from_value(value).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_JS_SDK",
                        format!("parameter must be a Lix value or JSON scalar: {error}"),
                    )
                })?;
                Ok(Value::Json(json))
            }
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
                let value = serde_wasm_bindgen::to_value(value).map_err(|error| {
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

    fn invalid_value(message: impl Into<String>) -> LixError {
        LixError::new("LIX_ERROR_JS_SDK", message.into())
    }

    fn js_sdk_error(message: impl Into<String>) -> LixError {
        LixError::new("LIX_ERROR_JS_SDK", message.into())
    }

    fn js_error(error: LixError) -> JsValue {
        let js_error = js_sys::Error::new(&error.description);
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
        js_error.into()
    }
}
