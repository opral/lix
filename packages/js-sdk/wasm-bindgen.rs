#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
    use lix_engine::{
        boot, BootArgs, ExecuteOptions, LixBackend, LixError, LixTransaction,
        QueryResult as EngineQueryResult, SqlDialect, Value as EngineValue,
    };
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen(typescript_custom_section)]
    const LIX_BACKEND_TYPES: &str = r#"
export type LixSqlDialect = "sqlite" | "postgres";

export type LixValueLike =
  | { kind: "Null" | "Integer" | "Real" | "Text" | "Blob"; value: unknown }
  | null
  | undefined
  | number
  | string
  | Uint8Array
  | ArrayBuffer;

export type LixQueryResultLike = { rows: LixValueLike[][] } | LixValueLike[][];

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
};
"#;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(typescript_type = "LixBackend")]
        pub type JsLixBackend;
    }

    #[wasm_bindgen]
    pub struct Lix {
        engine: lix_engine::Engine,
    }

    #[wasm_bindgen]
    impl Lix {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: String, params: JsValue) -> Result<JsValue, JsValue> {
            let params = Array::from(&params);
            let mut values = Vec::new();
            for value in params.iter() {
                values.push(value_from_js(value).map_err(js_error)?);
            }
            let result = self
                .engine
                .execute(&sql, &values, ExecuteOptions::default())
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
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(backend: JsLixBackend) -> Result<Lix, JsValue> {
        let backend = Box::new(JsBackend {
            backend: backend.into(),
        });
        let engine = boot(BootArgs::new(backend));
        engine.init().await.map_err(js_error)?;
        Ok(Lix { engine })
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
            value
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
        Ok(EngineQueryResult { rows })
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
        let obj = Object::new();
        let _ = Reflect::set(&obj, &JsValue::from_str("rows"), &rows);
        obj.into()
    }

    fn value_to_js(value: EngineValue) -> JsValue {
        let obj = Object::new();
        match value {
            EngineValue::Null => {
                let _ = Reflect::set(&obj, &JsValue::from_str("kind"), &JsValue::from_str("Null"));
                let _ = Reflect::set(&obj, &JsValue::from_str("value"), &JsValue::NULL);
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

    pub fn open_lix(_: ()) -> Result<Lix, String> {
        Err("engine-wasm is only available for wasm32 targets".to_string())
    }
}

pub use wasm::*;
