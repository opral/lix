//! JavaScript adapter for the shared Component host and actor implementation.
use super::component_runtime::*;
use super::{ComponentCompiler, ComponentGuest, ComponentGuestFactory, ComponentHost};
use crate::plugin::runtime::*;
use crate::{LixError, wasm::WasmLimits};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::{Value, json};
use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

pub(super) type RuntimeResult<T> = Result<T, LixError>;
pub(super) type ResourceTableError = LixError;
pub(super) type TimeoutTickerLease = ();
pub(super) fn wasm_runtime_error(
    context: impl Into<String>,
    error: impl std::fmt::Display,
) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{}: {error}", context.into()),
    )
}

pub(super) struct Resource<T> {
    id: u32,
    marker: PhantomData<fn() -> T>,
}
impl<T> Resource<T> {
    pub(super) fn new_borrow(id: u32) -> Self {
        Self {
            id,
            marker: PhantomData,
        }
    }
    pub(super) fn rep(&self) -> u32 {
        self.id
    }
}
impl<T> Serialize for Resource<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.id)
    }
}
#[derive(Default)]
pub(super) struct ResourceTable {
    next: u32,
    values: HashMap<u32, Box<dyn Any + Send>>,
}
impl ResourceTable {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub(super) fn push<T: Send + 'static>(&mut self, value: T) -> RuntimeResult<Resource<T>> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| component_error("resource handle overflow"))?;
        self.values.insert(self.next, Box::new(value));
        Ok(Resource::new_borrow(self.next))
    }
    pub(super) fn get<T: Send + 'static>(&self, key: &Resource<T>) -> RuntimeResult<&T> {
        self.values
            .get(&key.id)
            .and_then(|v| v.downcast_ref())
            .ok_or_else(|| component_error("unknown component resource"))
    }
    pub(super) fn get_mut<T: Send + 'static>(
        &mut self,
        key: &Resource<T>,
    ) -> RuntimeResult<&mut T> {
        self.values
            .get_mut(&key.id)
            .and_then(|v| v.downcast_mut())
            .ok_or_else(|| component_error("unknown component resource"))
    }
    pub(super) fn delete<T: Send + 'static>(&mut self, key: Resource<T>) -> RuntimeResult<T> {
        if self.get(&key).is_err() {
            return Err(component_error("unknown component resource"));
        }
        self.values
            .remove(&key.id)
            .and_then(|v| v.downcast::<T>().ok())
            .map(|v| *v)
            .ok_or_else(|| component_error("unknown component resource"))
    }
}
#[derive(Default)]
pub(super) struct HostState {
    pub(super) table: ResourceTable,
    pub(super) limits: MemoryLimits,
}
#[derive(Default)]
pub(super) struct MemoryLimits {
    high_water: u64,
}
impl MemoryLimits {
    pub(super) fn linear_memory_high_water_bytes(&self) -> u64 {
        self.high_water
    }
}
pub(super) struct HostStore {
    retired: Arc<std::sync::atomic::AtomicBool>,
    state: Arc<Mutex<HostState>>,
    limits: WasmLimits,
}
impl HostStore {
    pub(super) fn data(&self) -> MutexGuard<'_, HostState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn data_mut(&mut self) -> MutexGuard<'_, HostState> {
        self.data()
    }
    pub(super) fn set_epoch_deadline(&mut self, milliseconds: u64) {
        self.limits.timeout_ms = Some(milliseconds);
    }
}
pub(super) fn reset_store_limits(store: &mut HostStore, limits: WasmLimits) -> RuntimeResult<()> {
    store.limits = limits;
    Ok(())
}

pub(super) mod bindings {
    pub mod lix {
        pub mod plugin {
            pub mod host {
                use super::super::super::super::*;
                #[derive(Debug, Serialize)]
                #[serde(tag = "tag", content = "val", rename_all = "kebab-case")]
                pub enum HostError {
                    InvalidRange,
                    LimitExceeded(String),
                    Rejected(String),
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct RecordChunk {
                    #[serde(serialize_with = "u64_string")]
                    pub total_len: u64,
                    pub bytes: Vec<u8>,
                }
                #[derive(Serialize, serde::Deserialize)]
                pub struct RowPage {
                    pub payload: Vec<u8>,
                    pub attachments: Vec<Vec<u8>>,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct FileEdit {
                    #[serde(serialize_with = "u64_string")]
                    pub offset: u64,
                    #[serde(serialize_with = "u64_string")]
                    pub delete_len: u64,
                    pub insert: Vec<u8>,
                }
                pub enum MergeSide {
                    Base,
                    A,
                    B,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct ColumnMergeMeta {
                    pub ordinal: u32,
                    pub schema_key: String,
                    pub primary_key: Vec<Vec<u8>>,
                    pub schema_fingerprint: Vec<u8>,
                    pub file_id: Option<String>,
                    pub column: String,
                    #[serde(serialize_with = "optional_u64_string")]
                    pub base_len: Option<u64>,
                    #[serde(serialize_with = "optional_u64_string")]
                    pub a_len: Option<u64>,
                    #[serde(serialize_with = "optional_u64_string")]
                    pub b_len: Option<u64>,
                    #[serde(serialize_with = "u64_string")]
                    pub base_row_len: u64,
                    #[serde(serialize_with = "u64_string")]
                    pub a_row_len: u64,
                    #[serde(serialize_with = "u64_string")]
                    pub b_row_len: u64,
                }
                pub trait HostSnapshot {
                    fn file_len(&mut self, resource: Resource<SnapshotResource>) -> u64;
                    fn read_file(
                        &mut self,
                        resource: Resource<SnapshotResource>,
                        offset: u64,
                        length: u32,
                    ) -> Result<Vec<u8>, HostError>;
                    fn read_state(
                        &mut self,
                        resource: Resource<SnapshotResource>,
                        key: Vec<u8>,
                        offset: u64,
                        max_bytes: u32,
                    ) -> Result<Option<RecordChunk>, HostError>;
                    fn drop(&mut self, resource: Resource<SnapshotResource>) -> RuntimeResult<()>;
                }
                pub trait HostTransition {
                    fn max_batch_bytes(&mut self, resource: Resource<TransitionResource>) -> u32;
                    fn put_state(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        key: Vec<u8>,
                        value: Vec<u8>,
                    ) -> Result<(), HostError>;
                    fn delete_state(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        key: Vec<u8>,
                    ) -> Result<(), HostError>;
                    fn delete_state_prefix(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        prefix: Vec<u8>,
                    ) -> Result<(), HostError>;
                    fn emit_rows(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        page: RowPage,
                    ) -> Result<(), HostError>;
                    fn replace_all_rows(
                        &mut self,
                        resource: Resource<TransitionResource>,
                    ) -> Result<(), HostError>;
                    fn emit_file_edit(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        edit: FileEdit,
                    ) -> Result<(), HostError>;
                    fn begin_file_replacement(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        total_length: u64,
                    ) -> Result<(), HostError>;
                    fn write_file_replacement(
                        &mut self,
                        resource: Resource<TransitionResource>,
                        chunk: Vec<u8>,
                    ) -> Result<(), HostError>;
                    fn finish_file_replacement(
                        &mut self,
                        resource: Resource<TransitionResource>,
                    ) -> Result<(), HostError>;
                    fn drop(&mut self, resource: Resource<TransitionResource>)
                    -> RuntimeResult<()>;
                }
                pub trait HostRowSource {
                    fn next_page(
                        &mut self,
                        resource: Resource<RowSourceResource>,
                        max_bytes: u32,
                    ) -> Result<Option<RowPage>, HostError>;
                    fn drop(&mut self, resource: Resource<RowSourceResource>) -> RuntimeResult<()>;
                }
                pub trait HostColumnMergeSource {
                    fn len(&mut self, resource: Resource<ConflictSourceResource>) -> u32;
                    fn get(
                        &mut self,
                        resource: Resource<ConflictSourceResource>,
                        index: u32,
                    ) -> Result<ColumnMergeMeta, HostError>;
                    fn read_value(
                        &mut self,
                        resource: Resource<ConflictSourceResource>,
                        index: u32,
                        side: MergeSide,
                        offset: u64,
                        length: u32,
                    ) -> Result<Option<Vec<u8>>, HostError>;
                    fn read_row(
                        &mut self,
                        resource: Resource<ConflictSourceResource>,
                        index: u32,
                        side: MergeSide,
                        offset: u64,
                        length: u32,
                    ) -> Result<Vec<u8>, HostError>;
                    fn drop(
                        &mut self,
                        resource: Resource<ConflictSourceResource>,
                    ) -> RuntimeResult<()>;
                }
                pub trait HostColumnMergeSink {
                    fn max_batch_bytes(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                    ) -> u32;
                    fn use_lww(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                        ordinal: u32,
                    ) -> Result<(), HostError>;
                    fn begin_replace(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                        ordinal: u32,
                        total_length: Option<u64>,
                    ) -> Result<(), HostError>;
                    fn write_replacement(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                        chunk: Vec<u8>,
                    ) -> Result<(), HostError>;
                    fn finish_replace(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                    ) -> Result<(), HostError>;
                    fn drop(
                        &mut self,
                        resource: Resource<ResolutionSinkResource>,
                    ) -> RuntimeResult<()>;
                }
            }
            pub mod types {
                use super::super::super::super::*;
                use super::host::FileEdit;
                pub type PluginError = LixError;
                #[derive(Serialize)]
                pub struct CreateContext {
                    #[serde(serialize_with = "u64_string")]
                    pub high: u64,
                    pub low: u32,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct ParseRequest {
                    pub file_id: String,
                    pub path: String,
                    pub file: Resource<SnapshotResource>,
                    pub creates: CreateContext,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct ParseChangesRequest {
                    pub file_id: String,
                    pub before_path: String,
                    pub after_path: String,
                    pub before: Resource<SnapshotResource>,
                    pub file_edits: Vec<FileEdit>,
                    pub rows: Option<Resource<RowSourceResource>>,
                    pub creates: CreateContext,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct SerializeRequest {
                    pub file_id: String,
                    pub path: String,
                    pub rows: Resource<RowSourceResource>,
                    pub before: Option<Resource<SnapshotResource>>,
                }
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                pub struct SerializeChangesRequest {
                    pub file_id: String,
                    pub path: String,
                    pub before: Resource<SnapshotResource>,
                    pub row_changes: Resource<RowSourceResource>,
                }
            }
        }
    }
}
fn u64_string<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}
fn optional_u64_string<S: serde::Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.map(|v| v.to_string()).serialize(serializer)
}

pub(super) struct FileProjectionGuest(Arc<dyn ComponentGuest>);
pub(super) struct ColumnMergerGuest(Arc<dyn ComponentGuest>);
async fn invoke(
    guest: &dyn ComponentGuest,
    store: &mut HostStore,
    operation: &str,
    input: Value,
) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
    let host = Arc::new(InvocationHost {
        state: Arc::downgrade(&store.state),
        max_memory_bytes: store.limits.max_memory_bytes,
        active: std::sync::atomic::AtomicBool::new(true),
    });
    let mut guard = InvocationGuard {
        host: host.clone(),
        retired: store.retired.clone(),
        completed: false,
    };
    let result = guest
        .invoke(operation, input, host.clone(), store.limits)
        .await;
    guard.completed = true;
    if result
        .as_ref()
        .is_err_and(|error| error.code != LixError::CODE_INVALID_PLUGIN)
    {
        store
            .retired
            .store(true, std::sync::atomic::Ordering::Release);
    }
    match result {
        Ok(()) => Ok(Ok(())),
        Err(error) if error.code == LixError::CODE_INVALID_PLUGIN => Ok(Err(error)),
        Err(error) => Err(wasm_runtime_error(
            format!("component {operation} trapped"),
            error,
        )),
    }
}
impl FileProjectionGuest {
    pub(super) async fn call_parse(
        &self,
        store: &mut HostStore,
        input: &bindings::lix::plugin::types::ParseRequest,
        output: Resource<TransitionResource>,
    ) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
        invoke(
            self.0.as_ref(),
            store,
            "parse",
            json!({"input":input,"output":output}),
        )
        .await
    }
    pub(super) async fn call_parse_changes(
        &self,
        store: &mut HostStore,
        input: &bindings::lix::plugin::types::ParseChangesRequest,
        output: Resource<TransitionResource>,
    ) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
        invoke(
            self.0.as_ref(),
            store,
            "parseChanges",
            json!({"input":input,"output":output}),
        )
        .await
    }
    pub(super) async fn call_serialize(
        &self,
        store: &mut HostStore,
        input: &bindings::lix::plugin::types::SerializeRequest,
        output: Resource<TransitionResource>,
    ) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
        invoke(
            self.0.as_ref(),
            store,
            "serialize",
            json!({"input":input,"output":output}),
        )
        .await
    }
    pub(super) async fn call_serialize_changes(
        &self,
        store: &mut HostStore,
        input: &bindings::lix::plugin::types::SerializeChangesRequest,
        output: Resource<TransitionResource>,
    ) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
        invoke(
            self.0.as_ref(),
            store,
            "serializeChanges",
            json!({"input":input,"output":output}),
        )
        .await
    }
}
impl ColumnMergerGuest {
    pub(super) async fn call_merge(
        &self,
        store: &mut HostStore,
        input: Resource<ConflictSourceResource>,
        output: Resource<ResolutionSinkResource>,
    ) -> RuntimeResult<Result<(), bindings::lix::plugin::types::PluginError>> {
        invoke(
            self.0.as_ref(),
            store,
            "merge",
            json!({"input":input,"output":output}),
        )
        .await
    }
}

struct InvocationHost {
    state: std::sync::Weak<Mutex<HostState>>,
    max_memory_bytes: u64,
    active: std::sync::atomic::AtomicBool,
}
struct InvocationGuard {
    host: Arc<InvocationHost>,
    retired: Arc<std::sync::atomic::AtomicBool>,
    completed: bool,
}
impl Drop for InvocationGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.retired
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.host
            .active
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl ComponentHost for InvocationHost {
    fn dispatch(&self, method: &str, arguments: Value) -> RuntimeResult<Value> {
        if !self.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(component_error("component invocation has ended"));
        }
        let state_owner = self
            .state
            .upgrade()
            .ok_or_else(|| component_error("component actor was dropped"))?;
        if method == "runtime.memoryHighWater" {
            let bytes = argument_u64(&arguments, "bytes")?;
            if bytes > self.max_memory_bytes {
                return Err(component_error("guest memory limit exceeded"));
            }
            let mut state = state_owner
                .lock()
                .map_err(|_| component_error("component host state poisoned"))?;
            state.limits.high_water = state.limits.high_water.max(bytes);
            return Ok(json!({"ok":null}));
        }
        let resource = argument_u32(&arguments, "resource")?;
        let mut state = state_owner
            .lock()
            .map_err(|_| component_error("component host state poisoned"))?;
        match method.split('.').next() {
            Some("snapshot") => {
                state
                    .table
                    .get(&Resource::<SnapshotResource>::new_borrow(resource))?;
            }
            Some("transition") => {
                state
                    .table
                    .get(&Resource::<TransitionResource>::new_borrow(resource))?;
            }
            Some("rowSource") => {
                state
                    .table
                    .get(&Resource::<RowSourceResource>::new_borrow(resource))?;
            }
            Some("columnMergeSource") => {
                state
                    .table
                    .get(&Resource::<ConflictSourceResource>::new_borrow(resource))?;
            }
            Some("columnMergeSink") => {
                state
                    .table
                    .get(&Resource::<ResolutionSinkResource>::new_borrow(resource))?;
            }
            _ => return Err(component_error("unknown component resource kind")),
        }
        match method {
            "snapshot.fileLen" => Ok(
                json!({"ok":bindings::lix::plugin::host::HostSnapshot::file_len(&mut *state, Resource::new_borrow(resource)).to_string()}),
            ),
            "snapshot.readFile" => {
                host_result(bindings::lix::plugin::host::HostSnapshot::read_file(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u64(&arguments, "offset")?,
                    argument_u32(&arguments, "length")?,
                ))
            }
            "snapshot.readState" => {
                host_result(bindings::lix::plugin::host::HostSnapshot::read_state(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "key")?,
                    argument_u64(&arguments, "offset")?,
                    argument_u32(&arguments, "maxBytes")?,
                ))
            }
            "snapshot.drop" => {
                bindings::lix::plugin::host::HostSnapshot::drop(
                    &mut *state,
                    Resource::new_borrow(resource),
                )?;
                Ok(json!({"ok":null}))
            }
            "transition.maxBatchBytes" => Ok(
                json!({"ok":bindings::lix::plugin::host::HostTransition::max_batch_bytes(&mut *state, Resource::new_borrow(resource))}),
            ),
            "transition.putState" => {
                host_result(bindings::lix::plugin::host::HostTransition::put_state(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "key")?,
                    argument(&arguments, "value")?,
                ))
            }
            "transition.deleteState" => {
                host_result(bindings::lix::plugin::host::HostTransition::delete_state(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "key")?,
                ))
            }
            "transition.deleteStatePrefix" => host_result(
                bindings::lix::plugin::host::HostTransition::delete_state_prefix(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "prefix")?,
                ),
            ),
            "transition.emitRows" => {
                host_result(bindings::lix::plugin::host::HostTransition::emit_rows(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "page")?,
                ))
            }
            "transition.replaceAllRows" => host_result(
                bindings::lix::plugin::host::HostTransition::replace_all_rows(
                    &mut *state,
                    Resource::new_borrow(resource),
                ),
            ),
            "transition.emitFileEdit" => {
                host_result(bindings::lix::plugin::host::HostTransition::emit_file_edit(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_edit(&arguments, "edit")?,
                ))
            }
            "transition.beginFileReplacement" => host_result(
                bindings::lix::plugin::host::HostTransition::begin_file_replacement(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u64(&arguments, "totalLength")?,
                ),
            ),
            "transition.writeFileReplacement" => host_result(
                bindings::lix::plugin::host::HostTransition::write_file_replacement(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "chunk")?,
                ),
            ),
            "transition.finishFileReplacement" => host_result(
                bindings::lix::plugin::host::HostTransition::finish_file_replacement(
                    &mut *state,
                    Resource::new_borrow(resource),
                ),
            ),
            "transition.drop" => {
                bindings::lix::plugin::host::HostTransition::drop(
                    &mut *state,
                    Resource::new_borrow(resource),
                )?;
                Ok(json!({"ok":null}))
            }
            "rowSource.nextPage" => {
                host_result(bindings::lix::plugin::host::HostRowSource::next_page(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "maxBytes")?,
                ))
            }
            "rowSource.drop" => {
                bindings::lix::plugin::host::HostRowSource::drop(
                    &mut *state,
                    Resource::new_borrow(resource),
                )?;
                Ok(json!({"ok":null}))
            }
            "columnMergeSource.len" => Ok(
                json!({"ok":bindings::lix::plugin::host::HostColumnMergeSource::len(&mut *state, Resource::new_borrow(resource))}),
            ),
            "columnMergeSource.get" => {
                host_result(bindings::lix::plugin::host::HostColumnMergeSource::get(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "index")?,
                ))
            }
            "columnMergeSource.readValue" => host_result(
                bindings::lix::plugin::host::HostColumnMergeSource::read_value(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "index")?,
                    argument_side(&arguments, "side")?,
                    argument_u64(&arguments, "offset")?,
                    argument_u32(&arguments, "length")?,
                ),
            ),
            "columnMergeSource.readRow" => host_result(
                bindings::lix::plugin::host::HostColumnMergeSource::read_row(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "index")?,
                    argument_side(&arguments, "side")?,
                    argument_u64(&arguments, "offset")?,
                    argument_u32(&arguments, "length")?,
                ),
            ),
            "columnMergeSource.drop" => {
                bindings::lix::plugin::host::HostColumnMergeSource::drop(
                    &mut *state,
                    Resource::new_borrow(resource),
                )?;
                Ok(json!({"ok":null}))
            }
            "columnMergeSink.maxBatchBytes" => Ok(
                json!({"ok":bindings::lix::plugin::host::HostColumnMergeSink::max_batch_bytes(&mut *state, Resource::new_borrow(resource))}),
            ),
            "columnMergeSink.useLww" => {
                host_result(bindings::lix::plugin::host::HostColumnMergeSink::use_lww(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "ordinal")?,
                ))
            }
            "columnMergeSink.beginReplace" => host_result(
                bindings::lix::plugin::host::HostColumnMergeSink::begin_replace(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument_u32(&arguments, "ordinal")?,
                    argument_optional_u64(&arguments, "totalLength")?,
                ),
            ),
            "columnMergeSink.writeReplacement" => host_result(
                bindings::lix::plugin::host::HostColumnMergeSink::write_replacement(
                    &mut *state,
                    Resource::new_borrow(resource),
                    argument(&arguments, "chunk")?,
                ),
            ),
            "columnMergeSink.finishReplace" => host_result(
                bindings::lix::plugin::host::HostColumnMergeSink::finish_replace(
                    &mut *state,
                    Resource::new_borrow(resource),
                ),
            ),
            "columnMergeSink.drop" => {
                bindings::lix::plugin::host::HostColumnMergeSink::drop(
                    &mut *state,
                    Resource::new_borrow(resource),
                )?;
                Ok(json!({"ok":null}))
            }
            _ => Err(component_error(format!(
                "unknown component import {method}"
            ))),
        }
    }
}
fn host_result<T: Serialize>(
    result: Result<T, bindings::lix::plugin::host::HostError>,
) -> RuntimeResult<Value> {
    Ok(match result {
        Ok(value) => json!({"ok":value}),
        Err(error) => json!({"error":error}),
    })
}
fn argument<T: serde::de::DeserializeOwned>(args: &Value, key: &str) -> RuntimeResult<T> {
    serde_json::from_value(
        args.get(key)
            .cloned()
            .ok_or_else(|| component_error(format!("missing argument {key}")))?,
    )
    .map_err(|e| component_error(format!("invalid argument {key}: {e}")))
}
fn argument_u64(args: &Value, key: &str) -> RuntimeResult<u64> {
    args.get(key)
        .and_then(Value::as_str)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| component_error(format!("argument {key} must be a decimal u64 string")))
}
fn argument_optional_u64(args: &Value, key: &str) -> RuntimeResult<Option<u64>> {
    if args.get(key).is_none_or(Value::is_null) {
        Ok(None)
    } else {
        argument_u64(args, key).map(Some)
    }
}
fn argument_u32(args: &Value, key: &str) -> RuntimeResult<u32> {
    argument(args, key)
}
fn argument_side(args: &Value, key: &str) -> RuntimeResult<bindings::lix::plugin::host::MergeSide> {
    use bindings::lix::plugin::host::MergeSide;
    match args.get(key).and_then(Value::as_str) {
        Some("base") => Ok(MergeSide::Base),
        Some("a") => Ok(MergeSide::A),
        Some("b") => Ok(MergeSide::B),
        _ => Err(component_error("invalid merge side")),
    }
}
fn argument_edit(args: &Value, key: &str) -> RuntimeResult<bindings::lix::plugin::host::FileEdit> {
    let edit = args
        .get(key)
        .ok_or_else(|| component_error("missing file edit"))?;
    Ok(bindings::lix::plugin::host::FileEdit {
        offset: argument_u64(edit, "offset")?,
        delete_len: argument_u64(edit, "deleteLen")?,
        insert: argument(edit, "insert")?,
    })
}

pub(super) struct JsRuntime(pub(super) Arc<dyn ComponentCompiler>);
#[async_trait]
impl WasmRuntime for JsRuntime {
    async fn compile_component(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
        capabilities: PluginCapabilities,
    ) -> RuntimeResult<Arc<dyn WasmComponentFactory>> {
        if limits.max_memory_bytes == 0 {
            return Err(component_error("component memory limit must be positive"));
        }
        if !capabilities.file_projection && !capabilities.column_merger {
            return Err(component_error("component has no executable capability"));
        }
        let factory = self.0.compile(bytes, limits, capabilities).await?;
        Ok(Arc::new(JsFactory {
            factory,
            limits,
            capabilities,
            execution_permit: Arc::new(tokio::sync::Semaphore::new(
                COMPONENT_MAX_CONCURRENT_EXECUTIONS_PER_COMPONENT,
            )),
        }))
    }
}
struct JsFactory {
    factory: Arc<dyn ComponentGuestFactory>,
    limits: WasmLimits,
    capabilities: PluginCapabilities,
    execution_permit: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl WasmComponentFactory for JsFactory {
    async fn instantiate_actor(&self) -> RuntimeResult<Box<dyn WasmComponentActor>> {
        let initial = self
            .execution_permit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| component_error("component scheduler stopped"))?;
        let guest = self.factory.instantiate().await?;
        Ok(Box::new(ComponentActor {
            worker: ComponentWorker {
                store: HostStore {
                    retired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    state: Arc::new(Mutex::new(HostState::default())),
                    limits: self.limits,
                },
                file_projection: self
                    .capabilities
                    .file_projection
                    .then(|| FileProjectionGuest(guest.clone())),
                column_merger: self
                    .capabilities
                    .column_merger
                    .then(|| ColumnMergerGuest(guest)),
                limits: self.limits,
                documents: HashMap::new(),
                next_document: 1,
            },
            execution_permit: self.execution_permit.clone(),
            initial_execution_permit: Some(initial),
            _timeout_ticker: (),
            next_handle: 1,
            cursors: HashMap::new(),
            column_merge_cursors: HashMap::new(),
            edit_cursors: HashMap::new(),
            outputs: HashMap::new(),
            transitions: HashMap::new(),
            transition_permits: HashMap::new(),
            prospective_documents: ProspectiveDocuments::default(),
            retired: false,
            next_document: 1,
        }))
    }
}

pub(super) fn store_is_retired(store: &HostStore) -> bool {
    store.retired.load(std::sync::atomic::Ordering::Acquire)
}
