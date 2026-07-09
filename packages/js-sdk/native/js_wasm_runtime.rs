use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_sdk::{
    LixError, WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use napi::Status;
use napi::bindgen_prelude::{Buffer, CallbackContext, PromiseRaw, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;

pub(crate) type JsWasmRuntimeDispatch = ThreadsafeFunction<
    JsWasmRuntimeRequest,
    PromiseRaw<'static, JsWasmRuntimeResponse>,
    JsWasmRuntimeRequest,
    Status,
    false,
>;
pub(crate) type SharedJsWasmRuntimeDispatch = Arc<JsWasmRuntimeDispatch>;

pub(crate) struct JsWasmRuntime {
    dispatch: SharedJsWasmRuntimeDispatch,
}

impl JsWasmRuntime {
    pub(crate) fn new(dispatch: SharedJsWasmRuntimeDispatch) -> Self {
        Self { dispatch }
    }

    async fn dispatch(
        &self,
        operation: &'static str,
        request: JsWasmRuntimeRequest,
    ) -> Result<JsWasmRuntimeResponse, LixError> {
        dispatch_request(&self.dispatch, operation, request).await
    }
}

#[async_trait]
impl WasmRuntime for JsWasmRuntime {
    async fn init_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        let response = self
            .dispatch(
                "init-component",
                JsWasmRuntimeRequest {
                    operation: "initComponent".to_string(),
                    component_id: None,
                    component_bytes: Some(bytes.into()),
                    max_memory_bytes: Some(limits.max_memory_bytes.to_string()),
                    max_fuel: limits.max_fuel.map(|value| value.to_string()),
                    timeout_ms: limits.timeout_ms.map(|value| value.to_string()),
                    state: None,
                    file: None,
                },
            )
            .await?;
        let component_id = response.component_id.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "JavaScript WASM runtime did not return a component id",
            )
        })?;
        Ok(Arc::new(JsWasmComponent {
            component_id,
            dispatch: Arc::clone(&self.dispatch),
        }))
    }
}

struct JsWasmComponent {
    component_id: u32,
    dispatch: SharedJsWasmRuntimeDispatch,
}

impl JsWasmComponent {
    async fn dispatch(
        &self,
        operation: &'static str,
        request: JsWasmRuntimeRequest,
    ) -> Result<JsWasmRuntimeResponse, LixError> {
        dispatch_request(&self.dispatch, operation, request).await
    }
}

#[async_trait]
impl WasmComponentInstance for JsWasmComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        let response = self
            .dispatch(
                "detect-changes",
                JsWasmRuntimeRequest {
                    operation: "detectChanges".to_string(),
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
            .dispatch(
                "render",
                JsWasmRuntimeRequest {
                    operation: "render".to_string(),
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
        response.bytes.map(Into::into).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "JavaScript WASM runtime did not return rendered bytes",
            )
        })
    }
}

impl Drop for JsWasmComponent {
    fn drop(&mut self) {
        let _ = self.dispatch.call(
            JsWasmRuntimeRequest {
                operation: "closeComponent".to_string(),
                component_id: Some(self.component_id),
                component_bytes: None,
                max_memory_bytes: None,
                max_fuel: None,
                timeout_ms: None,
                state: None,
                file: None,
            },
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

async fn dispatch_request(
    dispatch: &JsWasmRuntimeDispatch,
    operation: &'static str,
    request: JsWasmRuntimeRequest,
) -> Result<JsWasmRuntimeResponse, LixError> {
    type DispatchResult = Result<JsWasmRuntimeResponse, String>;
    let (sender, receiver) = tokio::sync::oneshot::channel::<DispatchResult>();
    let sender = Arc::new(Mutex::new(Some(sender)));
    let callback_sender = Arc::clone(&sender);
    let status = dispatch.call_with_return_value(
        request,
        ThreadsafeFunctionCallMode::NonBlocking,
        move |promise, _env| {
            let promise = match promise {
                Ok(promise) => promise,
                Err(error) => {
                    settle_dispatch(&callback_sender, Err(error.to_string()));
                    return Ok(());
                }
            };
            let resolve_sender = Arc::clone(&callback_sender);
            let reject_sender = Arc::clone(&callback_sender);
            promise
                .then(move |context| {
                    settle_dispatch(&resolve_sender, Ok(context.value));
                    Ok(())
                })?
                .catch(move |_context: CallbackContext<Unknown>| {
                    settle_dispatch(
                        &reject_sender,
                        Err("JavaScript runtime promise rejected".to_string()),
                    );
                    Ok(())
                })?;
            Ok(())
        },
    );
    if status != Status::Ok {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("JavaScript WASM runtime {operation} dispatch failed: {status:?}"),
        ));
    }
    let response = receiver
        .await
        .map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("JavaScript WASM runtime {operation} response channel closed"),
            )
        })?
        .map_err(|message| js_runtime_error(operation, message))?;
    if let Some(message) = response.error_message.as_deref() {
        return Err(js_runtime_error(operation, message));
    }
    Ok(response)
}

fn settle_dispatch(
    sender: &Mutex<Option<tokio::sync::oneshot::Sender<Result<JsWasmRuntimeResponse, String>>>>,
    result: Result<JsWasmRuntimeResponse, String>,
) {
    if let Ok(mut sender) = sender.lock()
        && let Some(sender) = sender.take()
    {
        let _ = sender.send(result);
    }
}

fn js_runtime_error(operation: &str, error: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("JavaScript WASM runtime {operation} failed: {error}"),
    )
}

#[expect(missing_debug_implementations)]
#[napi(object)]
pub struct JsWasmRuntimeRequest {
    pub operation: String,
    pub component_id: Option<u32>,
    pub component_bytes: Option<Buffer>,
    pub max_memory_bytes: Option<String>,
    pub max_fuel: Option<String>,
    pub timeout_ms: Option<String>,
    pub state: Option<Vec<JsWasmPluginEntityState>>,
    pub file: Option<JsWasmPluginFile>,
}

#[expect(missing_debug_implementations)]
#[napi(object)]
pub struct JsWasmRuntimeResponse {
    pub component_id: Option<u32>,
    pub changes: Option<Vec<JsWasmPluginDetectedChange>>,
    pub bytes: Option<Buffer>,
    pub error_message: Option<String>,
}

#[expect(missing_debug_implementations)]
#[napi(object)]
pub struct JsWasmPluginFile {
    pub filename: Option<String>,
    pub data: Buffer,
}

impl From<WasmPluginFile> for JsWasmPluginFile {
    fn from(file: WasmPluginFile) -> Self {
        Self {
            filename: file.filename,
            data: file.data.into(),
        }
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct JsWasmPluginEntityState {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: String,
    pub metadata: Option<String>,
}

impl From<WasmPluginEntityState> for JsWasmPluginEntityState {
    fn from(state: WasmPluginEntityState) -> Self {
        Self {
            entity_pk: state.entity_pk,
            schema_key: state.schema_key,
            snapshot_content: state.snapshot_content,
            metadata: state.metadata,
        }
    }
}

#[derive(Debug)]
#[napi(object)]
pub struct JsWasmPluginDetectedChange {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
}

impl From<JsWasmPluginDetectedChange> for WasmPluginDetectedChange {
    fn from(change: JsWasmPluginDetectedChange) -> Self {
        Self {
            entity_pk: change.entity_pk,
            schema_key: change.schema_key,
            snapshot_content: change.snapshot_content,
            metadata: change.metadata,
        }
    }
}
