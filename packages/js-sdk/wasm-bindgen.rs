#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use futures_util::future::{AbortHandle, Abortable};
    use js_sys::{Array, ArrayBuffer, Function, Object, Promise, Reflect, Uint8Array};
    use lix_engine::image::ImageChunkWriter;
    use lix_engine::wasm::{WasmComponentInstance, WasmLimits, WasmRuntime};
    use lix_engine::wire::{WireQueryResult, WireValue};
    use lix_engine::{
        BootKeyValue, CreateCheckpointResult, CreateVersionOptions, CreateVersionResult,
        ExecuteOptions, ExecuteResult as EngineExecuteResult, InitResult as EngineInitResult,
        Lix as CoreLix, LixBackend, LixBackendTransaction, LixConfig, LixError,
        ObserveEvent as EngineObserveEvent, ObserveEventsOwned as EngineObserveEvents,
        ObserveQuery as EngineObserveQuery, QueryResult as EngineQueryResult, RedoOptions,
        RedoResult, SqlDialect, TransactionMode, UndoOptions, UndoResult, Value as EngineValue,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen(typescript_custom_section)]
    const LIX_BACKEND_TYPES: &str = r#"
export type LixSqlDialect = "sqlite" | "postgres";
export type LixTransactionMode = "read" | "write" | "deferred";

export type LixValue =
  | { kind: "null"; value: null }
  | { kind: "bool"; value: boolean }
  | { kind: "int"; value: number }
  | { kind: "float"; value: number }
  | { kind: "text"; value: string }
  | { kind: "json"; value: unknown }
  | { kind: "blob"; base64: string };

export type LixQueryResult = {
  rows: LixValue[][];
  columns: string[];
};

export type LixExecuteResult = {
  statements: LixQueryResult[];
};

export type LixBackendTransaction = {
  dialect?: LixSqlDialect | (() => LixSqlDialect);
  execute(
    sql: string,
    params: LixValue[],
  ): Promise<LixQueryResult> | LixQueryResult;
  commit(): Promise<void> | void;
  rollback(): Promise<void> | void;
};

export type LixBackend = {
  dialect?: LixSqlDialect | (() => LixSqlDialect);
  execute(
    sql: string,
    params: LixValue[],
  ): Promise<LixQueryResult> | LixQueryResult;
  beginTransaction?: (
    mode?: LixTransactionMode,
  ) => Promise<LixBackendTransaction> | LixBackendTransaction;
  // Should return a SQLite database file payload.
  export_image?: () => Promise<Uint8Array | ArrayBuffer> | Uint8Array | ArrayBuffer;
};

export type LixWasmLimits = {
  maxMemoryBytes?: number;
  maxFuel?: number;
  timeoutMs?: number;
};

export type LixWasmComponentInstance = {
  call(
    exportName: string,
    input: Uint8Array,
  ): Promise<Uint8Array | ArrayBuffer> | Uint8Array | ArrayBuffer;
  close?: () => Promise<void> | void;
};

export type LixWasmRuntime = {
  initComponent(
    bytes: Uint8Array,
    limits?: LixWasmLimits,
  ): Promise<LixWasmComponentInstance> | LixWasmComponentInstance;
};

export type LixBootKeyValue = {
  key: string;
  value: unknown;
  lixcol_global?: boolean;
  lixcol_untracked?: boolean;
};

export type InitLixResult = {
  initialized: boolean;
};

export type CreateVersionOptions = {
  id?: string;
  name?: string;
  sourceVersionId?: string;
  hidden?: boolean;
};

export type CreateVersionResult = {
  id: string;
  name: string;
};

export type UndoOptions = {
  /** Target `lix_version.id`. If omitted, uses the active `versionId`. */
  versionId?: string;
};

export type RedoOptions = {
  /** Target `lix_version.id`. If omitted, uses the active `versionId`. */
  versionId?: string;
};

export type UndoResult = {
  versionId: string;
  targetCommitId: string;
  inverseCommitId: string;
};

export type RedoResult = {
  versionId: string;
  targetCommitId: string;
  replayCommitId: string;
};

export type ObserveQuery = {
  sql: string;
  params?: LixValue[];
};

export type ExecuteOptions = {
  writerKey?: string | null;
};

export type OpenSessionOptions = {
  activeVersionId?: string;
  activeAccountIds?: string[];
};

export type ObserveEvent = {
  sequence: number;
  rows: LixQueryResult;
};

export type LixObserveEvents = {
  next(): Promise<ObserveEvent | undefined>;
  close(): void;
};
"#;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(typescript_type = "LixBackend")]
        pub type JsLixBackend;
        #[wasm_bindgen(typescript_type = "LixWasmRuntime")]
        pub type JsLixWasmRuntime;
    }

    #[wasm_bindgen]
    pub struct Lix {
        lix: Arc<CoreLix>,
    }

    #[wasm_bindgen(js_name = ObserveEvents)]
    pub struct JsObserveEvents {
        inner: std::sync::Mutex<Option<EngineObserveEvents>>,
        in_flight_next_abort: std::sync::Mutex<Option<AbortHandle>>,
        closed: AtomicBool,
    }

    #[wasm_bindgen]
    impl Lix {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(
            &self,
            sql: String,
            params: JsValue,
            options: Option<JsValue>,
        ) -> Result<JsValue, JsValue> {
            let params = Array::from(&params);
            let mut values = Vec::new();
            for value in params.iter() {
                values.push(value_from_js(value).map_err(js_error)?);
            }
            let execute_options = parse_execute_options(options, "execute").map_err(js_error)?;
            let result = self
                .lix
                .execute_with_options(&sql, &values, execute_options)
                .await
                .map_err(js_error)?;
            execute_result_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = activeVersionId)]
        pub async fn active_version_id(&self) -> Result<String, JsValue> {
            self.lix.active_version_id().await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = activeAccountIds)]
        pub async fn active_account_ids(&self) -> Result<JsValue, JsValue> {
            let account_ids = self.lix.active_account_ids().await.map_err(js_error)?;
            let values = Array::new();
            for account_id in account_ids {
                values.push(&JsValue::from_str(&account_id));
            }
            Ok(values.into())
        }

        #[wasm_bindgen(js_name = installPlugin)]
        pub async fn install_plugin(&self, archive_bytes: Uint8Array) -> Result<(), JsValue> {
            let mut bytes = vec![0u8; archive_bytes.length() as usize];
            archive_bytes.copy_to(&mut bytes);
            self.lix.install_plugin(&bytes).await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = createCheckpoint)]
        pub async fn create_checkpoint(&self) -> Result<JsValue, JsValue> {
            let result = self.lix.create_checkpoint().await.map_err(js_error)?;
            Ok(create_checkpoint_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = undo)]
        pub async fn undo(&self, args: Option<JsValue>) -> Result<JsValue, JsValue> {
            let options = parse_undo_options(args).map_err(js_error)?;
            let result = self
                .lix
                .undo_with_options(options)
                .await
                .map_err(js_error)?;
            Ok(undo_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = redo)]
        pub async fn redo(&self, args: Option<JsValue>) -> Result<JsValue, JsValue> {
            let options = parse_redo_options(args).map_err(js_error)?;
            let result = self
                .lix
                .redo_with_options(options)
                .await
                .map_err(js_error)?;
            Ok(redo_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = createVersion)]
        pub async fn create_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_create_version_options(args).map_err(js_error)?;
            let result = self.lix.create_version(options).await.map_err(js_error)?;
            Ok(create_version_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = switchVersion)]
        pub async fn switch_version(&self, version_id: String) -> Result<(), JsValue> {
            self.lix.switch_version(version_id).await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = openChildSession)]
        pub async fn open_child_session(&self, args: Option<JsValue>) -> Result<Lix, JsValue> {
            let options = parse_open_child_session_options(args).map_err(js_error)?;
            let session = self
                .lix
                .open_child_session(options)
                .await
                .map_err(js_error)?;
            Ok(Lix {
                lix: Arc::new(session),
            })
        }

        #[wasm_bindgen(js_name = export_image)]
        pub async fn export_image(&self) -> Result<Uint8Array, JsValue> {
            let bytes = self.lix.export_image().await.map_err(js_error)?;
            Ok(Uint8Array::from(bytes.as_slice()))
        }

        #[wasm_bindgen(js_name = observe)]
        pub fn observe(&self, query: JsValue) -> Result<JsObserveEvents, JsValue> {
            let query = parse_observe_query(query).map_err(js_error)?;
            let events = self.lix.observe(query).map_err(js_error)?;
            Ok(JsObserveEvents {
                inner: std::sync::Mutex::new(Some(events)),
                in_flight_next_abort: std::sync::Mutex::new(None),
                closed: AtomicBool::new(false),
            })
        }
    }

    #[wasm_bindgen(js_class = ObserveEvents)]
    impl JsObserveEvents {
        #[wasm_bindgen(js_name = next)]
        pub async fn next(&self) -> Result<JsValue, JsValue> {
            if self.closed.load(Ordering::SeqCst) {
                return Ok(JsValue::UNDEFINED);
            }

            let events = {
                let mut guard = self.inner.lock().map_err(|_| {
                    js_error(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "observe events lock poisoned".to_string(),
                    })
                })?;
                guard.take()
            };
            let Some(mut events) = events else {
                return Ok(JsValue::UNDEFINED);
            };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            {
                let mut guard = self.in_flight_next_abort.lock().map_err(|_| {
                    js_error(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "observe events abort lock poisoned".to_string(),
                    })
                })?;
                if self.closed.load(Ordering::SeqCst) {
                    events.close();
                    return Ok(JsValue::UNDEFINED);
                }
                *guard = Some(abort_handle);
            }

            let next = Abortable::new(events.next(), abort_registration).await;
            {
                let mut guard = self.in_flight_next_abort.lock().map_err(|_| {
                    js_error(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "observe events abort lock poisoned".to_string(),
                    })
                })?;
                guard.take();
            }

            let next = match next {
                Ok(Ok(next)) => next,
                Ok(Err(error)) => {
                    let mut guard = self.inner.lock().map_err(|_| {
                        js_error(LixError {
                            code: "LIX_ERROR_JS_SDK".to_string(),
                            description: "observe events lock poisoned".to_string(),
                        })
                    })?;
                    if self.closed.load(Ordering::SeqCst) {
                        events.close();
                        return Ok(JsValue::UNDEFINED);
                    }
                    *guard = Some(events);
                    return Err(js_error(error));
                }
                Err(_) => {
                    events.close();
                    return Ok(JsValue::UNDEFINED);
                }
            };

            if self.closed.load(Ordering::SeqCst) || next.is_none() {
                events.close();
                return Ok(JsValue::UNDEFINED);
            }

            {
                let mut guard = self.inner.lock().map_err(|_| {
                    js_error(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "observe events lock poisoned".to_string(),
                    })
                })?;
                if self.closed.load(Ordering::SeqCst) {
                    events.close();
                    return Ok(JsValue::UNDEFINED);
                }
                *guard = Some(events);
            }

            Ok(observe_event_to_js(next.expect("checked is_some"))
                .map_err(js_error)?
                .into())
        }

        #[wasm_bindgen(js_name = close)]
        pub fn close(&self) -> Result<(), JsValue> {
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            {
                let mut guard = self.in_flight_next_abort.lock().map_err(|_| {
                    js_error(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "observe events abort lock poisoned".to_string(),
                    })
                })?;
                if let Some(abort) = guard.take() {
                    abort.abort();
                }
            }
            let mut guard = self.inner.lock().map_err(|_| {
                js_error(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "observe events lock poisoned".to_string(),
                })
            })?;
            if let Some(mut events) = guard.take() {
                events.close();
            }
            Ok(())
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(
        backend: JsLixBackend,
        wasm_runtime: JsLixWasmRuntime,
    ) -> Result<Lix, JsValue> {
        let lix = CoreLix::open(build_lix_config(backend, wasm_runtime, Vec::new()))
            .await
            .map_err(js_error)?;
        Ok(Lix { lix: Arc::new(lix) })
    }

    #[wasm_bindgen(js_name = initLix)]
    pub async fn init_lix(
        backend: JsLixBackend,
        wasm_runtime: JsLixWasmRuntime,
        boot_key_values: Option<JsValue>,
    ) -> Result<JsValue, JsValue> {
        let mut key_values = Vec::new();
        if let Some(raw_key_values) = boot_key_values {
            key_values = parse_boot_key_values(raw_key_values).map_err(js_error)?;
        }
        let result = CoreLix::init(build_lix_config(backend, wasm_runtime, key_values))
            .await
            .map_err(js_error)?;
        init_result_to_js(result).map_err(js_error)
    }

    fn build_lix_config(
        backend: JsLixBackend,
        wasm_runtime: JsLixWasmRuntime,
        key_values: Vec<BootKeyValue>,
    ) -> LixConfig {
        let mut config = LixConfig::new(
            Box::new(JsBackend {
                backend: backend.into(),
            }),
            Arc::new(JsHostWasmRuntime {
                runtime: wasm_runtime.into(),
            }) as Arc<dyn WasmRuntime>,
        );
        config.key_values = key_values;
        config
    }

    fn parse_open_child_session_options(
        input: Option<JsValue>,
    ) -> Result<lix_engine::OpenSessionOptions, LixError> {
        let Some(input) = input else {
            return Ok(lix_engine::OpenSessionOptions::default());
        };
        if input.is_null() || input.is_undefined() {
            return Ok(lix_engine::OpenSessionOptions::default());
        }
        let object = Object::from(input);
        let active_version_id = match Reflect::get(&object, &JsValue::from_str("activeVersionId"))
            .map_err(|_| {
            LixError::new(
                "LIX_ERROR_JS_SDK",
                "openChildSession activeVersionId lookup failed",
            )
        })? {
            value if value.is_null() || value.is_undefined() => None,
            _value => Some(read_required_string_property(
                &object,
                "activeVersionId",
                "openChildSession options",
            )?),
        };
        let active_account_ids = match Reflect::get(&object, &JsValue::from_str("activeAccountIds"))
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_JS_SDK",
                    "openChildSession activeAccountIds lookup failed",
                )
            })? {
            value if value.is_null() || value.is_undefined() => None,
            value => {
                if !Array::is_array(&value) {
                    return Err(LixError::new(
                        "LIX_ERROR_JS_SDK",
                        "openChildSession activeAccountIds must be an array",
                    ));
                }
                let values = Array::from(&value);
                let mut parsed = Vec::with_capacity(values.length() as usize);
                for entry in values.iter() {
                    let account_id = entry.as_string().ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_JS_SDK",
                            "openChildSession activeAccountIds entries must be strings",
                        )
                    })?;
                    if account_id.is_empty() {
                        return Err(LixError::new(
                            "LIX_ERROR_JS_SDK",
                            "openChildSession activeAccountIds entries must be non-empty strings",
                        ));
                    }
                    parsed.push(account_id);
                }
                Some(parsed)
            }
        };
        Ok(lix_engine::OpenSessionOptions {
            active_version_id,
            active_account_ids,
        })
    }

    fn parse_boot_key_values(input: JsValue) -> Result<Vec<BootKeyValue>, LixError> {
        if input.is_null() || input.is_undefined() {
            return Ok(Vec::new());
        }
        if !Array::is_array(&input) {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "initLix keyValues must be an array".to_string(),
            });
        }

        let values = Array::from(&input);
        let mut parsed = Vec::with_capacity(values.length() as usize);
        for entry in values.iter() {
            if !entry.is_object() {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "initLix keyValues entries must be objects".to_string(),
                });
            }

            let key = read_required_string_property(&entry, "key", "initLix keyValues entry")?;
            let value =
                Reflect::get(&entry, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
            let value = js_to_json_value(value, &format!("initLix keyValues[{key}].value"))?;

            if Reflect::has(&entry, &JsValue::from_str("versionId")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description:
                        "initLix keyValues entries must use 'lixcol_global' instead of 'versionId'"
                            .to_string(),
                });
            }
            if Reflect::has(&entry, &JsValue::from_str("version_id")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description:
                        "initLix keyValues entries must use 'lixcol_global' instead of 'version_id'"
                            .to_string(),
                });
            }
            if Reflect::has(&entry, &JsValue::from_str("lixcol_version_id"))
                .map_err(js_to_lix_error)?
            {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description:
                        "initLix keyValues entries must use 'lixcol_global' instead of 'lixcol_version_id'"
                            .to_string(),
                });
            }
            if Reflect::has(&entry, &JsValue::from_str("global")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description:
                        "initLix keyValues entries must use 'lixcol_global' instead of 'global'"
                            .to_string(),
                });
            }
            if Reflect::has(&entry, &JsValue::from_str("untracked")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description:
                        "initLix keyValues entries must use 'lixcol_untracked' instead of 'untracked'"
                            .to_string(),
                });
            }

            let lixcol_global = read_optional_bool_property_with_context(
                &entry,
                "lixcol_global",
                "initLix keyValues entry",
            )?;
            let lixcol_untracked = read_optional_bool_property_with_context(
                &entry,
                "lixcol_untracked",
                "initLix keyValues entry",
            )?;

            parsed.push(BootKeyValue {
                key,
                value,
                lixcol_global,
                lixcol_untracked,
            });
        }

        Ok(parsed)
    }

    fn parse_create_version_options(input: JsValue) -> Result<CreateVersionOptions, LixError> {
        if input.is_null() || input.is_undefined() {
            return Ok(CreateVersionOptions::default());
        }
        if !input.is_object() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "createVersion options must be an object".to_string(),
            });
        }

        let id = read_optional_string_property_with_context(&input, "id", "createVersion")?;
        let name = read_optional_string_property_with_context(&input, "name", "createVersion")?;
        let source_version_id =
            read_optional_string_property_with_context(&input, "sourceVersionId", "createVersion")?;

        let hidden = read_optional_bool_property_with_context(&input, "hidden", "createVersion")?
            .unwrap_or(false);

        Ok(CreateVersionOptions {
            id,
            name,
            source_version_id,
            hidden,
        })
    }

    fn parse_undo_options(input: Option<JsValue>) -> Result<UndoOptions, LixError> {
        let Some(input) = input else {
            return Ok(UndoOptions::default());
        };
        if input.is_null() || input.is_undefined() {
            return Ok(UndoOptions::default());
        }
        let version_id = read_optional_string_property_with_context(&input, "versionId", "undo")?;
        Ok(UndoOptions { version_id })
    }

    fn parse_redo_options(input: Option<JsValue>) -> Result<RedoOptions, LixError> {
        let Some(input) = input else {
            return Ok(RedoOptions::default());
        };
        if input.is_null() || input.is_undefined() {
            return Ok(RedoOptions::default());
        }
        let version_id = read_optional_string_property_with_context(&input, "versionId", "redo")?;
        Ok(RedoOptions { version_id })
    }

    fn parse_observe_query(input: JsValue) -> Result<EngineObserveQuery, LixError> {
        if input.is_null() || input.is_undefined() || !input.is_object() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "observe query must be an object".to_string(),
            });
        }
        let sql = read_required_string_property(&input, "sql", "observe query")?;
        let params = Reflect::get(&input, &JsValue::from_str("params")).map_err(js_to_lix_error)?;
        let params = if params.is_null() || params.is_undefined() {
            Vec::new()
        } else if Array::is_array(&params) {
            let mut values = Vec::new();
            for value in Array::from(&params).iter() {
                values.push(value_from_js(value)?);
            }
            values
        } else {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "observe query.params must be an array".to_string(),
            });
        };
        Ok(EngineObserveQuery { sql, params })
    }

    fn parse_execute_options(
        input: Option<JsValue>,
        context: &str,
    ) -> Result<ExecuteOptions, LixError> {
        let Some(input) = input else {
            return Ok(ExecuteOptions::default());
        };
        if input.is_null() || input.is_undefined() {
            return Ok(ExecuteOptions::default());
        }
        if !input.is_object() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!("{context} options must be an object"),
            });
        }
        if Reflect::has(&input, &JsValue::from_str("writer_key")).map_err(js_to_lix_error)? {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!(
                    "{context} options must use 'writerKey' instead of 'writer_key'"
                ),
            });
        }

        let writer_key = read_optional_string_property_with_context(&input, "writerKey", context)?;
        Ok(ExecuteOptions { writer_key })
    }

    fn read_optional_bool_property_with_context(
        object: &JsValue,
        key: &str,
        context: &str,
    ) -> Result<Option<bool>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(js_to_lix_error)?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        value
            .as_bool()
            .ok_or_else(|| LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!("{context}.{key} must be a boolean"),
            })
            .map(Some)
    }

    fn create_checkpoint_result_to_js(result: CreateCheckpointResult) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("id"),
            &JsValue::from_str(&result.id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("changeSetId"),
            &JsValue::from_str(&result.change_set_id),
        );
        object
    }

    fn init_result_to_js(result: EngineInitResult) -> Result<JsValue, LixError> {
        let object = Object::new();
        Reflect::set(
            &object,
            &JsValue::from_str("initialized"),
            &JsValue::from_bool(result.initialized),
        )
        .map_err(js_to_lix_error)?;
        Ok(object.into())
    }

    fn create_version_result_to_js(result: CreateVersionResult) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("id"),
            &JsValue::from_str(&result.id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("name"),
            &JsValue::from_str(&result.name),
        );
        object
    }

    fn undo_result_to_js(result: UndoResult) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("versionId"),
            &JsValue::from_str(&result.version_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("targetCommitId"),
            &JsValue::from_str(&result.target_commit_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("inverseCommitId"),
            &JsValue::from_str(&result.inverse_commit_id),
        );
        object
    }

    fn redo_result_to_js(result: RedoResult) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("versionId"),
            &JsValue::from_str(&result.version_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("targetCommitId"),
            &JsValue::from_str(&result.target_commit_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("replayCommitId"),
            &JsValue::from_str(&result.replay_commit_id),
        );
        object
    }

    fn observe_event_to_js(event: EngineObserveEvent) -> Result<Object, LixError> {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("sequence"),
            &JsValue::from_f64(event.sequence as f64),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("rows"),
            &query_result_to_js(event.rows)?,
        );
        Ok(object)
    }

    fn read_required_string_property(
        object: &JsValue,
        key: &str,
        context: &str,
    ) -> Result<String, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(js_to_lix_error)?;
        let text = value.as_string().unwrap_or_default();
        if text.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!("{context}.{key} must be a non-empty string"),
            });
        }
        Ok(text)
    }

    fn read_optional_string_property_with_context(
        object: &JsValue,
        key: &str,
        context: &str,
    ) -> Result<Option<String>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(js_to_lix_error)?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        let text = value.as_string().ok_or_else(|| LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: format!("{context}.{key} must be a string"),
        })?;
        if text.trim().is_empty() {
            return Ok(None);
        }
        Ok(Some(text))
    }

    fn js_to_json_value(value: JsValue, context: &str) -> Result<serde_json::Value, LixError> {
        if value.is_undefined() {
            return Ok(serde_json::Value::Null);
        }
        let stringified = js_sys::JSON::stringify(&value).map_err(js_to_lix_error)?;
        let Some(json_text) = stringified.as_string() else {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!("{context} must be JSON-serializable"),
            });
        };
        serde_json::from_str(&json_text).map_err(|error| LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: format!("{context} invalid JSON value: {error}"),
        })
    }

    fn serde_json_to_js(value: &serde_json::Value) -> Result<JsValue, LixError> {
        let json_text = serde_json::to_string(value).map_err(|error| LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: format!("failed to serialize JSON value for wasm bridge: {error}"),
        })?;
        js_sys::JSON::parse(&json_text).map_err(js_to_lix_error)
    }

    struct JsHostWasmRuntime {
        runtime: JsValue,
    }

    struct JsHostWasmComponentInstance {
        component: JsValue,
    }

    // WASM is single-threaded by default; this avoids Send/Sync bounds in the engine.
    unsafe impl Send for JsHostWasmRuntime {}
    unsafe impl Sync for JsHostWasmRuntime {}
    unsafe impl Send for JsHostWasmComponentInstance {}
    unsafe impl Sync for JsHostWasmComponentInstance {}

    #[async_trait(?Send)]
    impl WasmRuntime for JsHostWasmRuntime {
        async fn init_component(
            &self,
            bytes: Vec<u8>,
            limits: WasmLimits,
        ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
            if bytes.is_empty() {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "plugin wasm bytes are empty".to_string(),
                });
            }

            let init_component =
                required_method(&self.runtime, "initComponent", "wasmRuntime.initComponent")?;
            let bytes_arg = Uint8Array::new_with_length(bytes.len() as u32);
            bytes_arg.copy_from(&bytes);
            let limits_arg = wasm_limits_to_js(limits)?;

            let result = init_component
                .call2(&self.runtime, &bytes_arg.into(), &limits_arg)
                .map_err(js_to_lix_error)?;
            let resolved = JsBackend::await_if_promise(result).await?;
            if resolved.is_null() || resolved.is_undefined() {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "wasmRuntime.initComponent returned no component instance"
                        .to_string(),
                });
            }

            Ok(Arc::new(JsHostWasmComponentInstance {
                component: resolved,
            }))
        }
    }

    #[async_trait(?Send)]
    impl WasmComponentInstance for JsHostWasmComponentInstance {
        async fn call(&self, export: &str, input: &[u8]) -> Result<Vec<u8>, LixError> {
            let call_method =
                required_method(&self.component, "call", "wasmComponentInstance.call")?;
            let input_arg = Uint8Array::new_with_length(input.len() as u32);
            input_arg.copy_from(input);
            let result = call_method
                .call2(
                    &self.component,
                    &JsValue::from_str(export),
                    &input_arg.into(),
                )
                .map_err(js_to_lix_error)?;
            let resolved = JsBackend::await_if_promise(result).await?;
            js_bytes_from_value(resolved, "wasmComponentInstance.call result")
        }

        async fn close(&self) -> Result<(), LixError> {
            let Some(close_method) = JsBackend::get_optional_method(&self.component, "close")?
            else {
                return Ok(());
            };
            let result = close_method
                .call0(&self.component)
                .map_err(js_to_lix_error)?;
            let _ = JsBackend::await_if_promise(result).await?;
            Ok(())
        }
    }

    fn required_method(target: &JsValue, name: &str, context: &str) -> Result<Function, LixError> {
        JsBackend::get_optional_method(target, name)?.ok_or_else(|| LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: format!("{context} is required"),
        })
    }

    fn wasm_limits_to_js(limits: WasmLimits) -> Result<JsValue, LixError> {
        let object = Object::new();
        Reflect::set(
            &object,
            &JsValue::from_str("maxMemoryBytes"),
            &JsValue::from_f64(limits.max_memory_bytes as f64),
        )
        .map_err(js_to_lix_error)?;
        if let Some(max_fuel) = limits.max_fuel {
            Reflect::set(
                &object,
                &JsValue::from_str("maxFuel"),
                &JsValue::from_f64(max_fuel as f64),
            )
            .map_err(js_to_lix_error)?;
        }
        if let Some(timeout_ms) = limits.timeout_ms {
            Reflect::set(
                &object,
                &JsValue::from_str("timeoutMs"),
                &JsValue::from_f64(timeout_ms as f64),
            )
            .map_err(js_to_lix_error)?;
        }
        Ok(object.into())
    }

    fn js_bytes_from_value(value: JsValue, context: &str) -> Result<Vec<u8>, LixError> {
        if value.is_instance_of::<Uint8Array>() {
            let array = value.unchecked_into::<Uint8Array>();
            let mut bytes = vec![0u8; array.length() as usize];
            array.copy_to(&mut bytes);
            return Ok(bytes);
        }

        if value.is_instance_of::<ArrayBuffer>() {
            let array = Uint8Array::new(&value);
            let mut bytes = vec![0u8; array.length() as usize];
            array.copy_to(&mut bytes);
            return Ok(bytes);
        }

        Err(LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: format!("{context} must be Uint8Array or ArrayBuffer"),
        })
    }

    struct JsBackend {
        backend: JsValue,
    }

    impl JsBackend {
        fn dialect_from_object(target: &JsValue) -> Option<SqlDialect> {
            let raw = Reflect::get(target, &JsValue::from_str("dialect"))
                .ok()
                .and_then(|value| {
                    if let Some(text) = value.as_string() {
                        return Some(text);
                    }
                    value
                        .dyn_into::<Function>()
                        .ok()
                        .and_then(|func| func.call0(target).ok())
                        .and_then(|value| value.as_string())
                })?;
            match raw.trim().to_ascii_lowercase().as_str() {
                "postgres" | "postgresql" => Some(SqlDialect::Postgres),
                "sqlite" => Some(SqlDialect::Sqlite),
                _ => None,
            }
        }

        fn get_optional_method(target: &JsValue, name: &str) -> Result<Option<Function>, LixError> {
            let value = Reflect::get(target, &JsValue::from_str(name)).map_err(js_to_lix_error)?;
            if value.is_null() || value.is_undefined() {
                return Ok(None);
            }
            value
                .dyn_into::<Function>()
                .map(Some)
                .map_err(js_to_lix_error)
        }

        async fn await_if_promise(value: JsValue) -> Result<JsValue, LixError> {
            if value.is_instance_of::<Promise>() {
                let promise: Promise = value.unchecked_into();
                return JsFuture::from(promise).await.map_err(js_to_lix_error);
            }
            Ok(value)
        }

        async fn execute_raw_on(
            &self,
            target: &JsValue,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            let func = Self::get_optional_method(target, "execute")?.ok_or_else(|| LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "backend.execute is required".to_string(),
            })?;
            let js_params = Array::new();
            for param in params {
                let wire = WireValue::try_from_engine(param)?;
                let value = wire_value_to_js(wire)?;
                js_params.push(&value);
            }
            let result = func
                .call2(target, &JsValue::from_str(sql), &js_params)
                .map_err(js_to_lix_error)?;
            let resolved = Self::await_if_promise(result).await?;
            query_result_from_js(resolved)
        }

        async fn execute_raw(
            &self,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            self.execute_raw_on(&self.backend, sql, params).await
        }
    }

    enum JsTransactionKind {
        Js { transaction: JsValue },
        Savepoint { name: String },
    }

    struct JsTransaction<'a> {
        backend: &'a JsBackend,
        kind: JsTransactionKind,
        closed: bool,
        mode: TransactionMode,
    }

    // WASM is single-threaded by default; this avoids Send/Sync bounds in the engine.
    unsafe impl Send for JsBackend {}
    unsafe impl Sync for JsBackend {}

    #[async_trait(?Send)]
    impl LixBackend for JsBackend {
        fn dialect(&self) -> SqlDialect {
            Self::dialect_from_object(&self.backend).unwrap_or(SqlDialect::Sqlite)
        }

        async fn execute(
            &self,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            self.execute_raw(sql, params).await
        }

        async fn begin_transaction(
            &self,
            mode: TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            let begin_transaction = Self::get_optional_method(&self.backend, "beginTransaction")?
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK_BACKEND_BEGIN_TRANSACTION_REQUIRED".to_string(),
                    description:
                        "backend.beginTransaction is required; raw SQL transaction control is not supported"
                            .to_string(),
                })?;
            let transaction = begin_transaction
                .call1(
                    &self.backend,
                    &JsValue::from_str(match mode {
                        TransactionMode::Read => "read",
                        TransactionMode::Write => "write",
                        TransactionMode::Deferred => "deferred",
                    }),
                )
                .map_err(js_to_lix_error)?;
            let transaction = Self::await_if_promise(transaction).await?;
            if transaction.is_null() || transaction.is_undefined() {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "beginTransaction() returned no transaction object".to_string(),
                });
            }
            Ok(Box::new(JsTransaction {
                backend: self,
                kind: JsTransactionKind::Js { transaction },
                closed: false,
                mode,
            }))
        }

        async fn begin_savepoint(
            &self,
            name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.execute_raw(&format!("SAVEPOINT {}", sql_identifier(name)), &[])
                .await?;
            Ok(Box::new(JsTransaction {
                backend: self,
                kind: JsTransactionKind::Savepoint {
                    name: name.to_string(),
                },
                closed: false,
                mode: TransactionMode::Write,
            }))
        }

        async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
            let export_image = Self::get_optional_method(&self.backend, "export_image")?
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "backend.export_image is required for export_image".to_string(),
                })?;
            let result = export_image.call0(&self.backend).map_err(js_to_lix_error)?;
            let resolved = Self::await_if_promise(result).await?;
            let bytes = js_bytes_from_value(resolved, "backend.export_image result")?;
            writer.write_chunk(&bytes).await?;
            writer.finish().await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for JsTransaction<'_> {
        fn dialect(&self) -> SqlDialect {
            match &self.kind {
                JsTransactionKind::Js { transaction } => {
                    JsBackend::dialect_from_object(transaction).unwrap_or(self.backend.dialect())
                }
                JsTransactionKind::Savepoint { .. } => self.backend.dialect(),
            }
        }

        fn mode(&self) -> TransactionMode {
            self.mode
        }

        async fn execute(
            &mut self,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            if self.closed {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "transaction is already closed".to_string(),
                });
            }
            match &self.kind {
                JsTransactionKind::Js { transaction } => {
                    self.backend.execute_raw_on(transaction, sql, params).await
                }
                JsTransactionKind::Savepoint { .. } => self.backend.execute_raw(sql, params).await,
            }
        }

        async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
            if self.closed {
                return Ok(());
            }
            match &self.kind {
                JsTransactionKind::Js { transaction } => {
                    let commit = JsBackend::get_optional_method(transaction, "commit")?
                        .ok_or_else(|| LixError {
                            code: "LIX_ERROR_JS_SDK".to_string(),
                            description: "transaction.commit is required".to_string(),
                        })?;
                    let result = commit.call0(transaction).map_err(js_to_lix_error)?;
                    JsBackend::await_if_promise(result).await?;
                }
                JsTransactionKind::Savepoint { name } => {
                    self.backend
                        .execute_raw(&format!("RELEASE SAVEPOINT {}", sql_identifier(name)), &[])
                        .await?;
                }
            }
            self.closed = true;
            Ok(())
        }

        async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
            if self.closed {
                return Ok(());
            }
            match &self.kind {
                JsTransactionKind::Js { transaction } => {
                    let rollback = JsBackend::get_optional_method(transaction, "rollback")?
                        .ok_or_else(|| LixError {
                            code: "LIX_ERROR_JS_SDK".to_string(),
                            description: "transaction.rollback is required".to_string(),
                        })?;
                    let result = rollback.call0(transaction).map_err(js_to_lix_error)?;
                    JsBackend::await_if_promise(result).await?;
                }
                JsTransactionKind::Savepoint { name } => {
                    let savepoint = sql_identifier(name);
                    self.backend
                        .execute_raw(&format!("ROLLBACK TO SAVEPOINT {savepoint}"), &[])
                        .await?;
                    self.backend
                        .execute_raw(&format!("RELEASE SAVEPOINT {savepoint}"), &[])
                        .await?;
                }
            }
            self.closed = true;
            Ok(())
        }
    }

    fn sql_identifier(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn js_to_lix_error(value: JsValue) -> LixError {
        LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: js_value_to_string(&value),
        }
    }

    fn query_result_from_js(value: JsValue) -> Result<EngineQueryResult, LixError> {
        if !value.is_object() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "query result must be an object".to_string(),
            });
        }

        let rows_value =
            Reflect::get(&value, &JsValue::from_str("rows")).map_err(js_to_lix_error)?;
        let rows_value = if let Ok(func) = rows_value.clone().dyn_into::<Function>() {
            func.call0(&value).map_err(js_to_lix_error)?
        } else {
            rows_value
        };
        if !Array::is_array(&rows_value) {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "query result 'rows' must be an array".to_string(),
            });
        }

        let rows_array = Array::from(&rows_value);
        let mut rows = Vec::new();
        for row in rows_array.iter() {
            if !Array::is_array(&row) {
                return Err(LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "query result rows must be arrays".to_string(),
                });
            }
            let row_array = Array::from(&row);
            let mut values = Vec::new();
            for cell in row_array.iter() {
                values.push(value_from_js(cell)?);
            }
            rows.push(values);
        }

        let raw_columns =
            Reflect::get(&value, &JsValue::from_str("columns")).map_err(js_to_lix_error)?;
        let columns_value = if let Ok(func) = raw_columns.clone().dyn_into::<Function>() {
            func.call0(&value).map_err(js_to_lix_error)?
        } else {
            raw_columns
        };
        if !Array::is_array(&columns_value) {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "query result 'columns' must be an array of strings".to_string(),
            });
        }

        let mut columns = Vec::new();
        for column in Array::from(&columns_value).iter() {
            columns.push(column.as_string().ok_or_else(|| LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "query result 'columns' must be an array of strings".to_string(),
            })?);
        }

        Ok(EngineQueryResult { rows, columns })
    }

    fn value_from_js(value: JsValue) -> Result<EngineValue, LixError> {
        wire_value_from_js(value)?.try_into_engine()
    }

    fn wire_value_from_js(value: JsValue) -> Result<WireValue, LixError> {
        if !value.is_object() {
            return Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: "SQL value must be a canonical LixValue object".to_string(),
            });
        }

        let kind_value =
            Reflect::get(&value, &JsValue::from_str("kind")).map_err(js_to_lix_error)?;
        let kind = kind_value.as_string().ok_or_else(|| LixError {
            code: "LIX_ERROR_JS_SDK".to_string(),
            description: "SQL value kind must be a string".to_string(),
        })?;

        match kind.as_str() {
            "null" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                if raw.is_null() {
                    Ok(WireValue::Null { value: () })
                } else {
                    Err(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "LixValue 'null' must contain value: null".to_string(),
                    })
                }
            }
            "bool" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                let parsed = raw.as_bool().ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "LixValue 'bool' must contain a boolean 'value'".to_string(),
                })?;
                Ok(WireValue::Bool { value: parsed })
            }
            "int" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                let parsed = raw.as_f64().ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "LixValue 'int' must contain a numeric 'value'".to_string(),
                })?;
                if !parsed.is_finite() || parsed.fract() != 0.0 {
                    return Err(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "LixValue 'int' must be a finite integer number".to_string(),
                    });
                }
                if parsed < i64::MIN as f64 || parsed > i64::MAX as f64 {
                    return Err(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "LixValue 'int' is outside i64 range".to_string(),
                    });
                }
                Ok(WireValue::Int {
                    value: parsed as i64,
                })
            }
            "float" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                let parsed = raw.as_f64().ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "LixValue 'float' must contain a numeric 'value'".to_string(),
                })?;
                if !parsed.is_finite() {
                    return Err(LixError {
                        code: "LIX_ERROR_JS_SDK".to_string(),
                        description: "LixValue 'float' must be a finite number".to_string(),
                    });
                }
                Ok(WireValue::Float { value: parsed })
            }
            "text" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                let parsed = raw.as_string().ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "LixValue 'text' must contain a string 'value'".to_string(),
                })?;
                Ok(WireValue::Text { value: parsed })
            }
            "json" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
                Ok(WireValue::Json {
                    value: js_to_json_value(raw, "LixValue 'json'.value")?,
                })
            }
            "blob" => {
                let raw =
                    Reflect::get(&value, &JsValue::from_str("base64")).map_err(js_to_lix_error)?;
                let base64 = raw.as_string().ok_or_else(|| LixError {
                    code: "LIX_ERROR_JS_SDK".to_string(),
                    description: "LixValue 'blob' must contain a string 'base64'".to_string(),
                })?;
                Ok(WireValue::Blob { base64 })
            }
            _ => Err(LixError {
                code: "LIX_ERROR_JS_SDK".to_string(),
                description: format!("unsupported LixValue kind '{kind}'"),
            }),
        }
    }

    fn query_result_to_js(result: EngineQueryResult) -> Result<JsValue, LixError> {
        let wire = WireQueryResult::try_from_engine(&result)?;
        let rows = Array::new();
        for row in wire.rows {
            let js_row = Array::new();
            for value in row {
                js_row.push(&wire_value_to_js(value)?);
            }
            rows.push(&js_row);
        }
        let columns = Array::new();
        for column in wire.columns {
            columns.push(&JsValue::from_str(&column));
        }
        let obj = Object::new();
        let _ = Reflect::set(&obj, &JsValue::from_str("rows"), &rows);
        let _ = Reflect::set(&obj, &JsValue::from_str("columns"), &columns);
        Ok(obj.into())
    }

    fn execute_result_to_js(result: EngineExecuteResult) -> Result<JsValue, LixError> {
        let statements = Array::new();
        for statement in result.statements {
            statements.push(&query_result_to_js(statement)?);
        }
        let obj = Object::new();
        let _ = Reflect::set(&obj, &JsValue::from_str("statements"), &statements);
        Ok(obj.into())
    }

    fn wire_value_to_js(value: WireValue) -> Result<JsValue, LixError> {
        let obj = Object::new();
        match value {
            WireValue::Null { value: _ } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("null"));
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &JsValue::NULL);
            }
            WireValue::Bool { value } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("bool"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_bool(value),
                );
            }
            WireValue::Int { value } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("int"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_f64(value as f64),
                );
            }
            WireValue::Float { value } => {
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("kind"),
                    &JsValue::from_str("float"),
                );
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &JsValue::from_f64(value));
            }
            WireValue::Text { value } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("text"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_str(&value),
                );
            }
            WireValue::Json { value } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("json"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &serde_json_to_js(&value)?,
                );
            }
            WireValue::Blob { base64 } => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("blob"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("base64"),
                    &JsValue::from_str(&base64),
                );
            }
        }
        Ok(obj.into())
    }

    fn js_error(value: impl std::fmt::Display) -> JsValue {
        JsValue::from_str(&value.to_string())
    }

    fn js_value_to_string(value: &JsValue) -> String {
        if let Ok(error) = value.clone().dyn_into::<js_sys::Error>() {
            let message: String = error.message().into();
            if !message.is_empty() {
                let stack = Reflect::get(&error, &JsValue::from_str("stack"))
                    .ok()
                    .and_then(|value| value.as_string())
                    .unwrap_or_default();
                if !stack.is_empty() {
                    return format!("{message}\n{stack}");
                }
                return message;
            }
        }
        if let Ok(message) = Reflect::get(value, &JsValue::from_str("message")) {
            if let Some(text) = message.as_string() {
                if !text.is_empty() {
                    return text;
                }
            }
        }
        value
            .as_string()
            .or_else(|| js_sys::JSON::stringify(value).ok().map(|v| v.into()))
            .unwrap_or_else(|| "js error".to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod wasm {
    pub struct Lix;

    pub fn open_lix(_: (), _: Option<()>) -> Result<Lix, String> {
        Err("engine-wasm is only available for wasm32 targets".to_string())
    }

    pub fn init_lix(_: (), _: Option<()>, _: Option<()>) -> Result<(), String> {
        Err("engine-wasm is only available for wasm32 targets".to_string())
    }
}

pub use wasm::*;
