#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use futures_util::future::{AbortHandle, Abortable};
    use js_sys::{Array, ArrayBuffer, Function, Object, Promise, Reflect, Uint8Array};
    use lix_engine::{
        boot, observe_owned, BootArgs, BootKeyValue, CreateCheckpointResult, CreateVersionOptions,
        CreateVersionResult, ExecuteOptions, LixBackend, LixError, LixTransaction,
        ObserveEvent as EngineObserveEvent, ObserveEventsOwned as EngineObserveEvents,
        ObserveQuery as EngineObserveQuery, QueryResult as EngineQueryResult, SnapshotChunkWriter,
        SqlDialect, StateCommitStream as EngineStateCommitStream, StateCommitStreamBatch,
        StateCommitStreamChange, StateCommitStreamFilter, StateCommitStreamOperation,
        Value as EngineValue, WasmComponentInstance, WasmLimits, WasmRuntime,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen(typescript_custom_section)]
    const LIX_BACKEND_TYPES: &str = r#"
export type LixSqlDialect = "sqlite" | "postgres";

export type LixValueLike =
  | { kind: "Null" | "Boolean" | "Integer" | "Real" | "Text" | "Blob"; value: unknown }
  | null
  | undefined
  | boolean
  | number
  | string
  | Uint8Array
  | ArrayBuffer;

export type LixQueryResultLike =
  | { rows: LixValueLike[][]; columns?: string[] }
  | LixValueLike[][];

export type LixTransaction = {
  dialect?: LixSqlDialect | (() => LixSqlDialect);
  execute(
    sql: string,
    params: LixValueLike[],
  ): Promise<LixQueryResultLike> | LixQueryResultLike;
  commit(): Promise<void> | void;
  rollback(): Promise<void> | void;
};

export type LixBackend = {
  dialect?: LixSqlDialect | (() => LixSqlDialect);
  execute(
    sql: string,
    params: LixValueLike[],
  ): Promise<LixQueryResultLike> | LixQueryResultLike;
  beginTransaction?: () => Promise<LixTransaction> | LixTransaction;
  // Should return a SQLite database file payload.
  exportSnapshot?: () => Promise<Uint8Array | ArrayBuffer> | Uint8Array | ArrayBuffer;
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
  lixcol_version_id?: string;
  lixcol_untracked?: boolean;
};

export type CreateVersionOptions = {
  id?: string;
  name?: string;
  inheritsFromVersionId?: string;
  hidden?: boolean;
};

export type CreateVersionResult = {
  id: string;
  name: string;
  inheritsFromVersionId: string;
};

export type StateCommitStreamFilter = {
  schemaKeys?: string[];
  entityIds?: string[];
  fileIds?: string[];
  versionIds?: string[];
  writerKeys?: string[];
  excludeWriterKeys?: string[];
  includeUntracked?: boolean;
};

export type StateCommitStreamOperation = "Insert" | "Update" | "Delete";

export type StateCommitStreamChange = {
  operation: StateCommitStreamOperation;
  entityId: string;
  schemaKey: string;
  schemaVersion: string;
  fileId: string;
  versionId: string;
  pluginKey: string;
  snapshotContent: unknown | null;
  untracked: boolean;
  writerKey: string | null;
};

export type StateCommitStreamBatch = {
  sequence: number;
  changes: StateCommitStreamChange[];
};

export type LixStateCommitStream = {
  tryNext(): StateCommitStreamBatch | undefined;
  close(): void;
};

export type ObserveQuery = {
  sql: string;
  params?: LixValueLike[];
};

export type LixTransactionStatement = {
  sql: string;
  params?: LixValueLike[];
};

export type ExecuteOptions = {
  writerKey?: string | null;
};

export type ObserveEvent = {
  sequence: number;
  rows: LixQueryResultLike;
  stateCommitSequence: number | null;
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
        engine: Arc<lix_engine::Engine>,
    }

    #[wasm_bindgen(js_name = StateCommitStream)]
    pub struct JsStateCommitStream {
        inner: std::sync::Mutex<Option<EngineStateCommitStream>>,
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
                .engine
                .execute(&sql, &values, execute_options)
                .await
                .map_err(js_error)?;
            Ok(query_result_to_js(result))
        }

        #[wasm_bindgen(js_name = executeTransaction)]
        pub async fn execute_transaction(
            &self,
            statements: JsValue,
            options: Option<JsValue>,
        ) -> Result<JsValue, JsValue> {
            let statements = parse_transaction_statements(statements).map_err(js_error)?;
            let (transaction_sql, transaction_params) =
                build_transaction_script_and_params(statements).map_err(js_error)?;
            let execute_options =
                parse_execute_options(options, "executeTransaction").map_err(js_error)?;
            let result = self
                .engine
                .execute(&transaction_sql, &transaction_params, execute_options)
                .await
                .map_err(js_error)?;
            Ok(query_result_to_js(result))
        }

        #[wasm_bindgen(js_name = installPlugin)]
        pub async fn install_plugin(
            &self,
            manifest_json: String,
            wasm_bytes: Uint8Array,
        ) -> Result<(), JsValue> {
            let mut bytes = vec![0u8; wasm_bytes.length() as usize];
            wasm_bytes.copy_to(&mut bytes);
            self.engine
                .install_plugin(&manifest_json, &bytes)
                .await
                .map_err(js_error)
        }

        #[wasm_bindgen(js_name = createCheckpoint)]
        pub async fn create_checkpoint(&self) -> Result<JsValue, JsValue> {
            let result = self.engine.create_checkpoint().await.map_err(js_error)?;
            Ok(create_checkpoint_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = createVersion)]
        pub async fn create_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let options = parse_create_version_options(args).map_err(js_error)?;
            let result = self
                .engine
                .create_version(options)
                .await
                .map_err(js_error)?;
            Ok(create_version_result_to_js(result).into())
        }

        #[wasm_bindgen(js_name = switchVersion)]
        pub async fn switch_version(&self, version_id: String) -> Result<(), JsValue> {
            self.engine
                .switch_version(version_id)
                .await
                .map_err(js_error)
        }

        #[wasm_bindgen(js_name = exportSnapshot)]
        pub async fn export_snapshot(&self) -> Result<Uint8Array, JsValue> {
            let mut writer = VecSnapshotWriter::default();
            self.engine
                .export_snapshot(&mut writer)
                .await
                .map_err(js_error)?;
            Ok(Uint8Array::from(writer.bytes.as_slice()))
        }

        #[wasm_bindgen(js_name = stateCommitStream)]
        pub fn state_commit_stream(&self, filter: JsValue) -> Result<JsStateCommitStream, JsValue> {
            let filter = parse_state_commit_stream_filter(filter).map_err(js_error)?;
            let events = self.engine.state_commit_stream(filter);
            Ok(JsStateCommitStream {
                inner: std::sync::Mutex::new(Some(events)),
            })
        }

        #[wasm_bindgen(js_name = observe)]
        pub fn observe(&self, query: JsValue) -> Result<JsObserveEvents, JsValue> {
            let query = parse_observe_query(query).map_err(js_error)?;
            let events = observe_owned(Arc::clone(&self.engine), query).map_err(js_error)?;
            Ok(JsObserveEvents {
                inner: std::sync::Mutex::new(Some(events)),
                in_flight_next_abort: std::sync::Mutex::new(None),
                closed: AtomicBool::new(false),
            })
        }
    }

    #[wasm_bindgen(js_class = StateCommitStream)]
    impl JsStateCommitStream {
        #[wasm_bindgen(js_name = tryNext)]
        pub fn try_next(&self) -> Result<JsValue, JsValue> {
            let guard = self.inner.lock().map_err(|_| {
                js_error(LixError {
                    message: "state commit stream lock poisoned".to_string(),
                })
            })?;
            let Some(events) = guard.as_ref() else {
                return Ok(JsValue::UNDEFINED);
            };
            match events.try_next() {
                Some(batch) => Ok(state_commit_stream_batch_to_js(batch).into()),
                None => Ok(JsValue::UNDEFINED),
            }
        }

        #[wasm_bindgen(js_name = close)]
        pub fn close(&self) -> Result<(), JsValue> {
            let mut guard = self.inner.lock().map_err(|_| {
                js_error(LixError {
                    message: "state commit stream lock poisoned".to_string(),
                })
            })?;
            if let Some(events) = guard.take() {
                events.close();
            }
            Ok(())
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
                        message: "observe events lock poisoned".to_string(),
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
                        message: "observe events abort lock poisoned".to_string(),
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
                        message: "observe events abort lock poisoned".to_string(),
                    })
                })?;
                guard.take();
            }

            let next = match next {
                Ok(Ok(next)) => next,
                Ok(Err(error)) => {
                    let mut guard = self.inner.lock().map_err(|_| {
                        js_error(LixError {
                            message: "observe events lock poisoned".to_string(),
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
                        message: "observe events lock poisoned".to_string(),
                    })
                })?;
                if self.closed.load(Ordering::SeqCst) {
                    events.close();
                    return Ok(JsValue::UNDEFINED);
                }
                *guard = Some(events);
            }

            Ok(observe_event_to_js(next.expect("checked is_some")).into())
        }

        #[wasm_bindgen(js_name = close)]
        pub fn close(&self) -> Result<(), JsValue> {
            if self.closed.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            {
                let mut guard = self.in_flight_next_abort.lock().map_err(|_| {
                    js_error(LixError {
                        message: "observe events abort lock poisoned".to_string(),
                    })
                })?;
                if let Some(abort) = guard.take() {
                    abort.abort();
                }
            }
            let mut guard = self.inner.lock().map_err(|_| {
                js_error(LixError {
                    message: "observe events lock poisoned".to_string(),
                })
            })?;
            if let Some(mut events) = guard.take() {
                events.close();
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct VecSnapshotWriter {
        bytes: Vec<u8>,
    }

    #[async_trait(?Send)]
    impl SnapshotChunkWriter for VecSnapshotWriter {
        async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
            self.bytes.extend_from_slice(chunk);
            Ok(())
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(
        backend: JsLixBackend,
        wasm_runtime: JsLixWasmRuntime,
        boot_key_values: Option<JsValue>,
    ) -> Result<Lix, JsValue> {
        let backend = Box::new(JsBackend {
            backend: backend.into(),
        });
        let mut boot_args = BootArgs::new(
            backend,
            Arc::new(JsHostWasmRuntime {
                runtime: wasm_runtime.into(),
            }) as Arc<dyn WasmRuntime>,
        );
        if let Some(raw_key_values) = boot_key_values {
            boot_args.key_values = parse_boot_key_values(raw_key_values).map_err(js_error)?;
        }
        let engine = boot(boot_args);
        engine.init().await.map_err(js_error)?;
        Ok(Lix {
            engine: Arc::new(engine),
        })
    }

    fn parse_boot_key_values(input: JsValue) -> Result<Vec<BootKeyValue>, LixError> {
        if input.is_null() || input.is_undefined() {
            return Ok(Vec::new());
        }
        if !Array::is_array(&input) {
            return Err(LixError {
                message: "openLix keyValues must be an array".to_string(),
            });
        }

        let values = Array::from(&input);
        let mut parsed = Vec::with_capacity(values.length() as usize);
        for entry in values.iter() {
            if !entry.is_object() {
                return Err(LixError {
                    message: "openLix keyValues entries must be objects".to_string(),
                });
            }

            let key = read_required_string_property(&entry, "key", "openLix keyValues entry")?;
            let value =
                Reflect::get(&entry, &JsValue::from_str("value")).map_err(js_to_lix_error)?;
            let value = js_to_json_value(value, &format!("openLix keyValues[{key}].value"))?;

            if Reflect::has(&entry, &JsValue::from_str("versionId")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    message:
                        "openLix keyValues entries must use 'lixcol_version_id' instead of 'versionId'"
                            .to_string(),
                });
            }
            if Reflect::has(&entry, &JsValue::from_str("version_id")).map_err(js_to_lix_error)? {
                return Err(LixError {
                    message:
                        "openLix keyValues entries must use 'lixcol_version_id' instead of 'version_id'"
                            .to_string(),
                });
            }

            let version_id = read_optional_string_property_with_context(
                &entry,
                "lixcol_version_id",
                "openLix keyValues entry",
            )?;
            let untracked = read_optional_bool_property_with_context(
                &entry,
                "lixcol_untracked",
                "openLix keyValues entry",
            )?;

            parsed.push(BootKeyValue {
                key,
                value,
                version_id,
                untracked,
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
                message: "createVersion options must be an object".to_string(),
            });
        }

        let id = read_optional_string_property_with_context(&input, "id", "createVersion")?;
        let name = read_optional_string_property_with_context(&input, "name", "createVersion")?;
        let inherits_from_version_id = read_optional_string_property_with_context(
            &input,
            "inheritsFromVersionId",
            "createVersion",
        )?
        .or(read_optional_string_property_with_context(
            &input,
            "inherits_from_version_id",
            "createVersion",
        )?);

        let hidden = read_optional_bool_property_with_context(&input, "hidden", "createVersion")?
            .unwrap_or(false);

        Ok(CreateVersionOptions {
            id,
            name,
            inherits_from_version_id,
            hidden,
        })
    }

    fn parse_state_commit_stream_filter(
        input: JsValue,
    ) -> Result<StateCommitStreamFilter, LixError> {
        if input.is_null() || input.is_undefined() {
            return Ok(StateCommitStreamFilter::default());
        }
        if !input.is_object() {
            return Err(LixError {
                message: "stateCommitStream filter must be an object".to_string(),
            });
        }

        Ok(StateCommitStreamFilter {
            schema_keys: read_optional_string_array_property(&input, "schemaKeys")?
                .unwrap_or_default(),
            entity_ids: read_optional_string_array_property(&input, "entityIds")?
                .unwrap_or_default(),
            file_ids: read_optional_string_array_property(&input, "fileIds")?.unwrap_or_default(),
            version_ids: read_optional_string_array_property(&input, "versionIds")?
                .unwrap_or_default(),
            writer_keys: read_optional_string_array_property(&input, "writerKeys")?
                .unwrap_or_default(),
            exclude_writer_keys: read_optional_string_array_property(&input, "excludeWriterKeys")?
                .unwrap_or_default(),
            include_untracked: read_optional_bool_property(&input, "includeUntracked")?
                .unwrap_or(true),
        })
    }

    fn parse_observe_query(input: JsValue) -> Result<EngineObserveQuery, LixError> {
        if input.is_null() || input.is_undefined() || !input.is_object() {
            return Err(LixError {
                message: "observe query must be an object".to_string(),
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
                message: "observe query.params must be an array".to_string(),
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
                message: format!("{context} options must be an object"),
            });
        }
        if Reflect::has(&input, &JsValue::from_str("writer_key")).map_err(js_to_lix_error)? {
            return Err(LixError {
                message: format!("{context} options must use 'writerKey' instead of 'writer_key'"),
            });
        }

        let writer_key = read_optional_string_property_with_context(&input, "writerKey", context)?;
        Ok(ExecuteOptions { writer_key })
    }

    struct TransactionStatement {
        sql: String,
        params: Vec<EngineValue>,
    }

    fn parse_transaction_statements(input: JsValue) -> Result<Vec<TransactionStatement>, LixError> {
        if !Array::is_array(&input) {
            return Err(LixError {
                message: "executeTransaction statements must be an array".to_string(),
            });
        }

        let values = Array::from(&input);
        let mut parsed = Vec::with_capacity(values.length() as usize);

        for (index, entry) in values.iter().enumerate() {
            if !entry.is_object() {
                return Err(LixError {
                    message: format!("executeTransaction statements[{index}] must be an object"),
                });
            }

            let sql = read_required_string_property(
                &entry,
                "sql",
                &format!("executeTransaction statements[{index}]"),
            )?;
            let params =
                Reflect::get(&entry, &JsValue::from_str("params")).map_err(js_to_lix_error)?;
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
                    message: format!(
                        "executeTransaction statements[{index}].params must be an array"
                    ),
                });
            };

            parsed.push(TransactionStatement { sql, params });
        }

        Ok(parsed)
    }

    fn build_transaction_script_and_params(
        statements: Vec<TransactionStatement>,
    ) -> Result<(String, Vec<EngineValue>), LixError> {
        let mut sql = String::from("BEGIN;");
        let mut params = Vec::new();

        for (index, statement) in statements.into_iter().enumerate() {
            let normalized_sql = statement.sql.trim();
            if normalized_sql.is_empty() {
                return Err(LixError {
                    message: format!("executeTransaction statements[{index}] has empty sql"),
                });
            }
            sql.push(' ');
            sql.push_str(normalized_sql);
            if !normalized_sql.ends_with(';') {
                sql.push(';');
            }
            params.extend(statement.params);
        }

        sql.push_str(" COMMIT;");
        Ok((sql, params))
    }

    fn read_optional_string_array_property(
        object: &JsValue,
        key: &str,
    ) -> Result<Option<Vec<String>>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(js_to_lix_error)?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        if !Array::is_array(&value) {
            return Err(LixError {
                message: format!("stateCommitStream filter '{key}' must be an array of strings"),
            });
        }
        let values = Array::from(&value);
        let mut out = Vec::with_capacity(values.length() as usize);
        for item in values.iter() {
            let text = item.as_string().ok_or_else(|| LixError {
                message: format!("stateCommitStream filter '{key}' must be an array of strings"),
            })?;
            if !text.trim().is_empty() {
                out.push(text);
            }
        }
        Ok(Some(out))
    }

    fn read_optional_bool_property(object: &JsValue, key: &str) -> Result<Option<bool>, LixError> {
        let value = Reflect::get(object, &JsValue::from_str(key)).map_err(js_to_lix_error)?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        value
            .as_bool()
            .ok_or_else(|| LixError {
                message: format!("stateCommitStream filter '{key}' must be a boolean"),
            })
            .map(Some)
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
                message: format!("{context}.{key} must be a boolean"),
            })
            .map(Some)
    }

    fn state_commit_stream_batch_to_js(batch: StateCommitStreamBatch) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("sequence"),
            &JsValue::from_f64(batch.sequence as f64),
        );
        let changes = Array::new();
        for change in batch.changes {
            changes.push(&state_commit_stream_change_to_js(change).into());
        }
        let _ = Reflect::set(&object, &JsValue::from_str("changes"), &changes);
        object
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
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("inheritsFromVersionId"),
            &JsValue::from_str(&result.inherits_from_version_id),
        );
        object
    }

    fn state_commit_stream_change_to_js(change: StateCommitStreamChange) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("operation"),
            &JsValue::from_str(match change.operation {
                StateCommitStreamOperation::Insert => "Insert",
                StateCommitStreamOperation::Update => "Update",
                StateCommitStreamOperation::Delete => "Delete",
            }),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("entityId"),
            &JsValue::from_str(&change.entity_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("schemaKey"),
            &JsValue::from_str(&change.schema_key),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("schemaVersion"),
            &JsValue::from_str(&change.schema_version),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("fileId"),
            &JsValue::from_str(&change.file_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("versionId"),
            &JsValue::from_str(&change.version_id),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("pluginKey"),
            &JsValue::from_str(&change.plugin_key),
        );
        let snapshot_content = change
            .snapshot_content
            .and_then(|value| serde_json::to_string(&value).ok())
            .and_then(|text| js_sys::JSON::parse(&text).ok())
            .unwrap_or(JsValue::NULL);
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("snapshotContent"),
            &snapshot_content,
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("untracked"),
            &JsValue::from_bool(change.untracked),
        );
        let writer_key = match change.writer_key {
            Some(value) => JsValue::from_str(&value),
            None => JsValue::NULL,
        };
        let _ = Reflect::set(&object, &JsValue::from_str("writerKey"), &writer_key);
        object
    }

    fn observe_event_to_js(event: EngineObserveEvent) -> Object {
        let object = Object::new();
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("sequence"),
            &JsValue::from_f64(event.sequence as f64),
        );
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("rows"),
            &query_result_to_js(event.rows),
        );
        let state_commit_sequence = match event.state_commit_sequence {
            Some(value) => JsValue::from_f64(value as f64),
            None => JsValue::NULL,
        };
        let _ = Reflect::set(
            &object,
            &JsValue::from_str("stateCommitSequence"),
            &state_commit_sequence,
        );
        object
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
                message: format!("{context}.{key} must be a non-empty string"),
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
            message: format!("{context}.{key} must be a string"),
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
                message: format!("{context} must be JSON-serializable"),
            });
        };
        serde_json::from_str(&json_text).map_err(|error| LixError {
            message: format!("{context} invalid JSON value: {error}"),
        })
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
                    message: "plugin wasm bytes are empty".to_string(),
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
                    message: "wasmRuntime.initComponent returned no component instance".to_string(),
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
            message: format!("{context} is required"),
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
            message: format!("{context} must be Uint8Array or ArrayBuffer"),
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
                message: "backend.execute is required".to_string(),
            })?;
            let js_params = Array::new();
            for param in params.iter().cloned() {
                let value: JsValue = value_to_js(param);
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
        Sql,
        Js { transaction: JsValue },
    }

    struct JsTransaction<'a> {
        backend: &'a JsBackend,
        kind: JsTransactionKind,
        closed: bool,
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

        async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
            if let Some(begin_transaction) =
                Self::get_optional_method(&self.backend, "beginTransaction")?
            {
                let transaction = begin_transaction
                    .call0(&self.backend)
                    .map_err(js_to_lix_error)?;
                let transaction = Self::await_if_promise(transaction).await?;
                if transaction.is_null() || transaction.is_undefined() {
                    return Err(LixError {
                        message: "beginTransaction() returned no transaction object".to_string(),
                    });
                }
                return Ok(Box::new(JsTransaction {
                    backend: self,
                    kind: JsTransactionKind::Js { transaction },
                    closed: false,
                }));
            }

            self.execute_raw("BEGIN", &[]).await?;
            Ok(Box::new(JsTransaction {
                backend: self,
                kind: JsTransactionKind::Sql,
                closed: false,
            }))
        }

        async fn export_snapshot(
            &self,
            writer: &mut dyn SnapshotChunkWriter,
        ) -> Result<(), LixError> {
            let export_snapshot = Self::get_optional_method(&self.backend, "exportSnapshot")?
                .ok_or_else(|| LixError {
                    message: "backend.exportSnapshot is required for export_snapshot".to_string(),
                })?;
            let result = export_snapshot
                .call0(&self.backend)
                .map_err(js_to_lix_error)?;
            let resolved = Self::await_if_promise(result).await?;
            let bytes = js_bytes_from_value(resolved, "backend.exportSnapshot result")?;
            writer.write_chunk(&bytes).await?;
            writer.finish().await
        }
    }

    #[async_trait(?Send)]
    impl LixTransaction for JsTransaction<'_> {
        fn dialect(&self) -> SqlDialect {
            match &self.kind {
                JsTransactionKind::Sql => self.backend.dialect(),
                JsTransactionKind::Js { transaction } => {
                    JsBackend::dialect_from_object(transaction).unwrap_or(self.backend.dialect())
                }
            }
        }

        async fn execute(
            &mut self,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            if self.closed {
                return Err(LixError {
                    message: "transaction is already closed".to_string(),
                });
            }
            match &self.kind {
                JsTransactionKind::Sql => self.backend.execute_raw(sql, params).await,
                JsTransactionKind::Js { transaction } => {
                    self.backend.execute_raw_on(transaction, sql, params).await
                }
            }
        }

        async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
            if self.closed {
                return Ok(());
            }
            match &self.kind {
                JsTransactionKind::Sql => {
                    self.backend.execute_raw("COMMIT", &[]).await?;
                }
                JsTransactionKind::Js { transaction } => {
                    let commit = JsBackend::get_optional_method(transaction, "commit")?
                        .ok_or_else(|| LixError {
                            message: "transaction.commit is required".to_string(),
                        })?;
                    let result = commit.call0(transaction).map_err(js_to_lix_error)?;
                    JsBackend::await_if_promise(result).await?;
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
                JsTransactionKind::Sql => {
                    self.backend.execute_raw("ROLLBACK", &[]).await?;
                }
                JsTransactionKind::Js { transaction } => {
                    let rollback = JsBackend::get_optional_method(transaction, "rollback")?
                        .ok_or_else(|| LixError {
                            message: "transaction.rollback is required".to_string(),
                        })?;
                    let result = rollback.call0(transaction).map_err(js_to_lix_error)?;
                    JsBackend::await_if_promise(result).await?;
                }
            }
            self.closed = true;
            Ok(())
        }
    }

    fn js_to_lix_error(value: JsValue) -> LixError {
        LixError {
            message: js_value_to_string(&value),
        }
    }

    fn query_result_from_js(value: JsValue) -> Result<EngineQueryResult, LixError> {
        let rows_value = if let Ok(rows) = Reflect::get(&value, &JsValue::from_str("rows")) {
            if let Ok(func) = rows.clone().dyn_into::<Function>() {
                func.call0(&value).map_err(js_to_lix_error)?
            } else {
                rows
            }
        } else {
            value.clone()
        };

        let rows_array = Array::from(&rows_value);
        let mut rows = Vec::new();
        for row in rows_array.iter() {
            let row_array = Array::from(&row);
            let mut values = Vec::new();
            for cell in row_array.iter() {
                values.push(value_from_js(cell)?);
            }
            rows.push(values);
        }

        let mut columns = Vec::new();
        if let Ok(raw_columns) = Reflect::get(&value, &JsValue::from_str("columns")) {
            let columns_value = if let Ok(func) = raw_columns.clone().dyn_into::<Function>() {
                func.call0(&value).map_err(js_to_lix_error)?
            } else {
                raw_columns
            };

            if !columns_value.is_null() && !columns_value.is_undefined() {
                if !Array::is_array(&columns_value) {
                    return Err(LixError {
                        message: "query result 'columns' must be an array of strings".to_string(),
                    });
                }

                for column in Array::from(&columns_value).iter() {
                    columns.push(column.as_string().ok_or_else(|| LixError {
                        message: "query result 'columns' must be an array of strings".to_string(),
                    })?);
                }
            }
        }

        Ok(EngineQueryResult { rows, columns })
    }

    fn get_kind(value: &JsValue) -> Option<String> {
        let kind = Reflect::get(value, &JsValue::from_str("kind")).ok()?;
        if let Ok(func) = kind.clone().dyn_into::<Function>() {
            func.call0(value).ok()?.as_string()
        } else {
            kind.as_string()
        }
    }

    fn get_value_field_or_method(
        value: &JsValue,
        field: &str,
        method: &str,
    ) -> Result<JsValue, LixError> {
        if let Ok(val) = Reflect::get(value, &JsValue::from_str(field)) {
            if !val.is_undefined() {
                return Ok(val);
            }
        }
        let func = Reflect::get(value, &JsValue::from_str(method))
            .map_err(js_to_lix_error)?
            .dyn_into::<Function>()
            .map_err(js_to_lix_error)?;
        func.call0(value).map_err(js_to_lix_error)
    }

    fn value_from_js(value: JsValue) -> Result<EngineValue, LixError> {
        if value.is_null() || value.is_undefined() {
            return Ok(EngineValue::Null);
        }
        if let Some(boolean) = value.as_bool() {
            return Ok(EngineValue::Boolean(boolean));
        }
        if let Some(number) = value.as_f64() {
            if number.fract() == 0.0 {
                return Ok(EngineValue::Integer(number as i64));
            }
            return Ok(EngineValue::Real(number));
        }
        if let Some(text) = value.as_string() {
            return Ok(EngineValue::Text(text));
        }
        if let Ok(buffer) = value.clone().dyn_into::<Uint8Array>() {
            let mut bytes = vec![0u8; buffer.length() as usize];
            buffer.copy_to(&mut bytes);
            return Ok(EngineValue::Blob(bytes));
        }
        if let Some(kind) = get_kind(&value) {
            return match kind.as_str() {
                "Null" => Ok(EngineValue::Null),
                "Boolean" => {
                    let v = get_value_field_or_method(&value, "value", "asBoolean")?;
                    Ok(EngineValue::Boolean(v.as_bool().unwrap_or(false)))
                }
                "Integer" => {
                    let v = get_value_field_or_method(&value, "value", "asInteger")?;
                    Ok(EngineValue::Integer(v.as_f64().unwrap_or(0.0) as i64))
                }
                "Real" => {
                    let v = get_value_field_or_method(&value, "value", "asReal")?;
                    Ok(EngineValue::Real(v.as_f64().unwrap_or(0.0)))
                }
                "Text" => {
                    let v = get_value_field_or_method(&value, "value", "asText")?;
                    Ok(EngineValue::Text(v.as_string().unwrap_or_default()))
                }
                "Blob" => {
                    let v = get_value_field_or_method(&value, "value", "asBlob")?;
                    let buffer = v.dyn_into::<Uint8Array>().map_err(js_to_lix_error)?;
                    let mut bytes = vec![0u8; buffer.length() as usize];
                    buffer.copy_to(&mut bytes);
                    Ok(EngineValue::Blob(bytes))
                }
                _ => Ok(EngineValue::Null),
            };
        }
        Ok(EngineValue::Text(js_value_to_string(&value)))
    }

    fn query_result_to_js(result: EngineQueryResult) -> JsValue {
        let rows = Array::new();
        for row in result.rows {
            let js_row = Array::new();
            for value in row {
                js_row.push(&value_to_js(value));
            }
            rows.push(&js_row);
        }
        let columns = Array::new();
        for column in result.columns {
            columns.push(&JsValue::from_str(&column));
        }
        let obj = Object::new();
        let _ = Reflect::set(&obj, &JsValue::from_str("rows"), &rows);
        let _ = Reflect::set(&obj, &JsValue::from_str("columns"), &columns);
        obj.into()
    }

    fn value_to_js(value: EngineValue) -> JsValue {
        let obj = Object::new();
        match value {
            EngineValue::Null => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("Null"));
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &JsValue::NULL);
            }
            EngineValue::Boolean(value) => {
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("kind"),
                    &JsValue::from_str("Boolean"),
                );
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_bool(value),
                );
            }
            EngineValue::Integer(value) => {
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("kind"),
                    &JsValue::from_str("Integer"),
                );
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_f64(value as f64),
                );
            }
            EngineValue::Real(value) => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("Real"));
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &JsValue::from_f64(value));
            }
            EngineValue::Text(value) => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("Text"));
                let _ = Reflect::set(
                    &obj,
                    &JsValue::from_str("value"),
                    &JsValue::from_str(&value),
                );
            }
            EngineValue::Blob(value) => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("Blob"));
                let bytes = Uint8Array::from(value.as_slice());
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &bytes);
            }
        }
        obj.into()
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

    pub fn open_lix(_: (), _: Option<()>, _: Option<()>) -> Result<Lix, String> {
        Err("engine-wasm is only available for wasm32 targets".to_string())
    }
}

pub use wasm::*;
