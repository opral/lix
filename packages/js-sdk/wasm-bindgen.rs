#[cfg(target_arch = "wasm32")]
mod wasm {
    use js_sys::{Array, Object, Reflect};
    use lix_rs_sdk::{
        open_lix as open_lix_rs, CreateVersionOptions, ExecuteResult, Lix as RsLix, LixError,
        MergeVersionOptions, OpenLixOptions, SwitchVersionOptions, Value,
    };
    use wasm_bindgen::prelude::*;

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
  | { kind: "bool"; value: boolean }
  | { kind: "int"; value: number }
  | { kind: "float"; value: number }
  | { kind: "text"; value: string }
  | { kind: "json"; value: JsonValue }
  | { kind: "blob"; base64: string };

export type RowSet = {
  columns: string[];
  rows: LixValue[][];
};

export type ExecuteResult =
  | { kind: "rows"; rows: RowSet }
  | { kind: "affectedRows"; affectedRows: number };

export type OpenLixOptions = Record<string, never>;

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
        inner: Option<RsLix>,
    }

    #[wasm_bindgen]
    impl Lix {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: String, params: JsValue) -> Result<JsValue, JsValue> {
            let lix = self.inner.as_ref().ok_or_else(closed_error)?;
            let params = Array::from(&params);
            let values = params
                .iter()
                .map(value_from_js)
                .collect::<Result<Vec<_>, _>>()
                .map_err(js_error)?;
            let result = lix.execute(&sql, &values).await.map_err(js_error)?;
            execute_result_to_js(result).map_err(js_error)
        }

        #[wasm_bindgen(js_name = activeVersionId)]
        pub async fn active_version_id(&self) -> Result<String, JsValue> {
            let lix = self.inner.as_ref().ok_or_else(closed_error)?;
            lix.active_version_id().await.map_err(js_error)
        }

        #[wasm_bindgen(js_name = createVersion)]
        pub async fn create_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let lix = self.inner.as_ref().ok_or_else(closed_error)?;
            let options = parse_create_version_options(args).map_err(js_error)?;
            let result = lix.create_version(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "versionId", &result.version_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = switchVersion)]
        pub async fn switch_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let lix = self.inner.as_ref().ok_or_else(closed_error)?;
            let options = parse_switch_version_options(args).map_err(js_error)?;
            let result = lix.switch_version(options).await.map_err(js_error)?;
            let object = Object::new();
            set_string(&object, "versionId", &result.version_id).map_err(js_error)?;
            Ok(object.into())
        }

        #[wasm_bindgen(js_name = mergeVersion)]
        pub async fn merge_version(&self, args: JsValue) -> Result<JsValue, JsValue> {
            let lix = self.inner.as_ref().ok_or_else(closed_error)?;
            let options = parse_merge_version_options(args).map_err(js_error)?;
            let result = lix.merge_version(options).await.map_err(js_error)?;
            let object = Object::new();
            let outcome = match result.outcome {
                lix_rs_sdk::MergeVersionOutcome::AlreadyUpToDate => "alreadyUpToDate",
                lix_rs_sdk::MergeVersionOutcome::MergeCommitted => "mergeCommitted",
            };
            set_string(&object, "outcome", outcome).map_err(js_error)?;
            set_string(&object, "targetVersionId", &result.target_version_id).map_err(js_error)?;
            set_string(&object, "sourceVersionId", &result.source_version_id).map_err(js_error)?;
            set_optional_string(&object, "mergeBaseCommitId", result.merge_base_commit_id.as_deref())
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
        pub async fn close(&mut self) -> Result<(), JsValue> {
            let Some(lix) = self.inner.take() else {
                return Ok(());
            };
            lix.close().await.map_err(js_error)
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub async fn open_lix(args: Option<JsValue>) -> Result<Lix, JsValue> {
        parse_open_lix_options(args).map_err(js_error)?;
        let inner = open_lix_rs(OpenLixOptions::default())
            .await
            .map_err(js_error)?;
        Ok(Lix { inner: Some(inner) })
    }

    fn parse_open_lix_options(args: Option<JsValue>) -> Result<(), LixError> {
        if let Some(value) = args {
            if !value.is_undefined() && !value.is_null() && !value.is_object() {
                return Err(LixError::new(
                    "LIX_ERROR_JS_SDK",
                    "openLix() options must be an object",
                ));
            }
        }
        Ok(())
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
            LixError::new("LIX_ERROR_JS_SDK", format!("{method}() could not read {key}"))
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
            LixError::new("LIX_ERROR_JS_SDK", format!("{method}() could not read {key}"))
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
            Some("bool") => Ok(Value::Boolean(
                Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| invalid_value("bool value must be boolean"))?,
            )),
            Some("int") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_value("int value must be number"))?;
                Ok(Value::Integer(value as i64))
            }
            Some("float") => {
                let value = Reflect::get(&object, &JsValue::from_str("value"))
                    .ok()
                    .and_then(|value| value.as_f64())
                    .ok_or_else(|| invalid_value("float value must be number"))?;
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
                let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, base64)
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
        match result {
            ExecuteResult::Rows(rows) => {
                set_string(&object, "kind", "rows")?;
                let rows_object = Object::new();
                let columns = Array::new();
                for column in rows.columns() {
                    columns.push(&JsValue::from_str(column));
                }
                Reflect::set(&rows_object, &JsValue::from_str("columns"), &columns)
                    .map_err(|_| js_sdk_error("could not set columns"))?;
                let values = Array::new();
                for row in rows.rows() {
                    let row_values = Array::new();
                    for value in row.values() {
                        row_values.push(&value_to_js(value)?);
                    }
                    values.push(&row_values);
                }
                Reflect::set(&rows_object, &JsValue::from_str("rows"), &values)
                    .map_err(|_| js_sdk_error("could not set rows"))?;
                Reflect::set(&object, &JsValue::from_str("rows"), &rows_object)
                    .map_err(|_| js_sdk_error("could not set result rows"))?;
            }
            ExecuteResult::AffectedRows(count) => {
                set_string(&object, "kind", "affectedRows")?;
                set_number(&object, "affectedRows", count as f64)?;
            }
        }
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
                set_string(&object, "kind", "bool")?;
                Reflect::set(
                    &object,
                    &JsValue::from_str("value"),
                    &JsValue::from_bool(*value),
                )
                .map_err(|_| js_sdk_error("could not set bool value"))?;
            }
            Value::Integer(value) => {
                set_string(&object, "kind", "int")?;
                set_number(&object, "value", *value as f64)?;
            }
            Value::Real(value) => {
                set_string(&object, "kind", "float")?;
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
                    &base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        value,
                    ),
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

    fn closed_error() -> JsValue {
        js_error(LixError::new("LIX_ERROR_JS_SDK", "lix is closed"))
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
            let _ = Reflect::set(object, &JsValue::from_str("hint"), &JsValue::from_str(&hint));
        }
        js_error.into()
    }
}
