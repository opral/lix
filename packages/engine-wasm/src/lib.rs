#[cfg(target_arch = "wasm32")]
mod wasm {
    use async_trait::async_trait;
    use js_sys::{Function, Promise, Reflect};
    use lix_engine::{boot, LixBackend, LixError, QueryResult, Value};
    use serde_wasm_bindgen as swb;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen]
    pub struct Lix {
        engine: lix_engine::Engine,
    }

    #[wasm_bindgen]
    impl Lix {
        #[wasm_bindgen(js_name = execute)]
        pub async fn execute(&self, sql: String, params: JsValue) -> Result<JsValue, JsValue> {
            let params: Vec<Value> = swb::from_value(params).map_err(js_error)?;
            let result = self.engine.execute(&sql, &params).await.map_err(js_error)?;
            swb::to_value(&result).map_err(js_error)
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
        async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
            let func = Reflect::get(&self.backend, &JsValue::from_str("execute"))
                .map_err(js_to_lix_error)?
                .dyn_into::<Function>()
                .map_err(js_to_lix_error)?;
            let params = swb::to_value(params).map_err(|err| LixError {
                message: err.to_string(),
            })?;
            let promise = func
                .call2(&self.backend, &JsValue::from_str(sql), &params)
                .map_err(js_to_lix_error)?;
            let result = JsFuture::from(Promise::from(promise))
                .await
                .map_err(js_to_lix_error)?;
            swb::from_value(result).map_err(|err| LixError {
                message: err.to_string(),
            })
        }
    }

    fn js_to_lix_error(value: JsValue) -> LixError {
        LixError {
            message: js_value_to_string(&value),
        }
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
