#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use js_sys::{Array, Function, Object, Promise, Reflect, Uint8Array};
    use lix_engine::{
        boot, LixBackend, LixError, QueryResult as EngineQueryResult, Value as EngineValue,
    };
    use wasm_bindgen::prelude::*;
    use wasm_bindgen::JsCast;
    use wasm_bindgen_futures::JsFuture;

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
            let result = self.engine.execute(&sql, &values).await.map_err(js_error)?;
            Ok(query_result_to_js(result))
        }
    }

    #[wasm_bindgen(js_name = openLix)]
    pub fn open_lix(backend: JsValue) -> Result<Lix, JsValue> {
        let backend = Box::new(JsBackend { backend });
        Ok(Lix {
            engine: boot(backend),
        })
    }

    struct JsBackend {
        backend: JsValue,
    }

    // WASM is single-threaded by default; this avoids Send/Sync bounds in the engine.
    unsafe impl Send for JsBackend {}
    unsafe impl Sync for JsBackend {}

    #[async_trait(?Send)]
    impl LixBackend for JsBackend {
        async fn execute(
            &self,
            sql: &str,
            params: &[EngineValue],
        ) -> Result<EngineQueryResult, LixError> {
            let func = Reflect::get(&self.backend, &JsValue::from_str("execute"))
                .map_err(js_to_lix_error)?
                .dyn_into::<Function>()
                .map_err(js_to_lix_error)?;
            let js_params = Array::new();
            for param in params.iter().cloned() {
                let value: JsValue = value_to_js(param);
                js_params.push(&value);
            }
            let promise = func
                .call2(&self.backend, &JsValue::from_str(sql), &js_params)
                .map_err(js_to_lix_error)?;
            let result = JsFuture::from(Promise::from(promise))
                .await
                .map_err(js_to_lix_error)?;
            query_result_from_js(result)
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
