//! Wasmtime adapter for the shared Component host and actor implementation.
use super::component_runtime::*;
pub use super::component_runtime::{
    ConflictSourceResource, ResolutionSinkResource, RowSourceResource, SnapshotResource,
    TransitionResource,
};
use crate::plugin::runtime::*;
use crate::{LixError, wasm::WasmLimits};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
pub(super) use wasmtime::component::{Resource, ResourceTable, ResourceTableError};
pub(super) type RuntimeResult<T> = wasmtime::Result<T>;
pub(super) type HostStore = Store<WasiHostState>;
pub(super) use super::{TimeoutTickerLease, reset_store_limits, wasm_runtime_error};
pub(super) type HostState = WasiHostState;
use super::WasiHostState;
use super::{
    CompileProfile, CompiledComponentKey, WasmtimePluginRuntime, add_to_linker_sync, create_store,
};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};
pub(super) mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "plugin",
        with: {
            "lix:plugin/host.snapshot": super::SnapshotResource,
            "lix:plugin/host.transition": super::TransitionResource,
            "lix:plugin/host.column-merge-source": super::ConflictSourceResource,
            "lix:plugin/host.row-source": super::RowSourceResource,
            "lix:plugin/host.column-merge-sink": super::ResolutionSinkResource,
        },
    });
}

pub(super) mod file_projection_bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "file-projection-plugin",
        with: {
            "lix:plugin/host": super::bindings::lix::plugin::host,
            "lix:plugin/types": super::bindings::lix::plugin::types,
        },
    });
}

pub(super) mod column_merger_bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "column-merger-plugin",
        with: {
            "lix:plugin/host": super::bindings::lix::plugin::host,
            "lix:plugin/types": super::bindings::lix::plugin::types,
        },
    });
}

pub(super) struct ComponentFactory {
    component: Component,
    linker: ComponentLinker,
    runtime: Arc<super::WasmtimeSharedRuntime>,
    limits: WasmLimits,
    profile: CompileProfile,
    execution_permit: Arc<tokio::sync::Semaphore>,
}

enum ComponentLinker {
    Combined(Arc<Linker<WasiHostState>>),
    FileProjection(Arc<Linker<WasiHostState>>),
    ColumnMerger(Arc<Linker<WasiHostState>>),
}

pub(super) async fn compile_component(
    runtime: &WasmtimePluginRuntime,
    bytes: Vec<u8>,
    limits: WasmLimits,
    capabilities: PluginCapabilities,
) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
    if limits.max_memory_bytes == 0 {
        return Err(component_error(
            "component component memory limit must be positive",
        ));
    }
    let profile = if limits.max_fuel.is_some() {
        CompileProfile::FuelAndTimeout
    } else {
        CompileProfile::Timeout
    };
    let engine = runtime.shared.engine(profile);
    let key = CompiledComponentKey::new(profile, &bytes);
    let component = runtime
        .shared
        .compiled_components
        .get_or_compile(key, || {
            Component::new(engine, &bytes)
                .map_err(|error| wasm_runtime_error("failed to compile plugin component", error))
        })
        .await?;
    let mut linker = Linker::<WasiHostState>::new(engine);
    add_to_linker_sync(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure component WASI linker", error))?;
    let linker = match (capabilities.column_merger, capabilities.file_projection) {
        (true, true) => {
            bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
                &mut linker,
                |state| state,
            )
            .map_err(|error| {
                wasm_runtime_error("failed to configure combined plugin linker", error)
            })?;
            ComponentLinker::Combined(Arc::new(linker))
        }
        (false, true) => {
            file_projection_bindings::FileProjectionPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure file projection linker", error)
            })?;
            ComponentLinker::FileProjection(Arc::new(linker))
        }
        (true, false) => {
            column_merger_bindings::ColumnMergerPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure column merger linker", error)
            })?;
            ComponentLinker::ColumnMerger(Arc::new(linker))
        }
        (false, false) => {
            return Err(component_error(
                "cannot compile a plugin component without an executable capability",
            ));
        }
    };
    Ok(Arc::new(ComponentFactory {
        component,
        linker,
        runtime: runtime.shared.clone(),
        limits,
        profile,
        execution_permit: Arc::new(tokio::sync::Semaphore::new(
            COMPONENT_MAX_CONCURRENT_EXECUTIONS_PER_COMPONENT,
        )),
    }))
}

#[async_trait]
impl WasmComponentFactory for ComponentFactory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentActor>, LixError> {
        let initial_execution_permit = self
            .execution_permit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| component_error("component component execution scheduler stopped"))?;
        let engine = self.runtime.engine(self.profile);
        let timeout_ticker = self
            .runtime
            .timeout_ticker(self.profile)?
            .ok_or_else(|| component_error("component actor requires an epoch timeout ticker"))?;
        let mut store = create_store(engine, self.limits)?;
        store.epoch_deadline_trap();
        let (file_projection, column_merger) = match &self.linker {
            ComponentLinker::Combined(linker) => {
                let instance = bindings::Plugin::instantiate(&mut store, &self.component, linker)
                    .map_err(|error| {
                    wasm_runtime_error("failed to instantiate combined plugin actor", error)
                })?;
                (
                    Some(FileProjectionGuest::Combined(
                        instance.lix_plugin_file_projection().clone(),
                    )),
                    Some(ColumnMergerGuest::Combined(
                        instance.lix_plugin_column_merger().clone(),
                    )),
                )
            }
            ComponentLinker::FileProjection(linker) => {
                let instance = file_projection_bindings::FileProjectionPlugin::instantiate(
                    &mut store,
                    &self.component,
                    linker,
                )
                .map_err(|error| {
                    wasm_runtime_error("failed to instantiate file projection actor", error)
                })?;
                (
                    Some(FileProjectionGuest::Narrow(
                        instance.lix_plugin_file_projection().clone(),
                    )),
                    None,
                )
            }
            ComponentLinker::ColumnMerger(linker) => {
                let instance = column_merger_bindings::ColumnMergerPlugin::instantiate(
                    &mut store,
                    &self.component,
                    linker,
                )
                .map_err(|error| {
                    wasm_runtime_error("failed to instantiate column merger actor", error)
                })?;
                (
                    None,
                    Some(ColumnMergerGuest::Narrow(
                        instance.lix_plugin_column_merger().clone(),
                    )),
                )
            }
        };
        let worker = ComponentWorker {
            store,
            file_projection,
            column_merger,
            limits: self.limits,
            documents: HashMap::new(),
            next_document: 1,
        };
        Ok(Box::new(ComponentActor {
            worker,
            execution_permit: self.execution_permit.clone(),
            initial_execution_permit: Some(initial_execution_permit),
            _timeout_ticker: timeout_ticker,
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

pub(super) enum FileProjectionGuest {
    Combined(bindings::exports::lix::plugin::file_projection::Guest),
    Narrow(file_projection_bindings::exports::lix::plugin::file_projection::Guest),
}

pub(super) enum ColumnMergerGuest {
    Combined(bindings::exports::lix::plugin::column_merger::Guest),
    Narrow(column_merger_bindings::exports::lix::plugin::column_merger::Guest),
}

impl FileProjectionGuest {
    pub(super) async fn call_parse(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::ParseRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::Combined(guest) => guest.call_parse(store, input, output),
            Self::Narrow(guest) => guest.call_parse(store, input, output),
        }
    }

    pub(super) async fn call_parse_changes(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::ParseChangesRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::Combined(guest) => guest.call_parse_changes(store, input, output),
            Self::Narrow(guest) => guest.call_parse_changes(store, input, output),
        }
    }

    pub(super) async fn call_serialize(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::SerializeRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::Combined(guest) => guest.call_serialize(store, input, output),
            Self::Narrow(guest) => guest.call_serialize(store, input, output),
        }
    }

    pub(super) async fn call_serialize_changes(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::SerializeChangesRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::Combined(guest) => guest.call_serialize_changes(store, input, output),
            Self::Narrow(guest) => guest.call_serialize_changes(store, input, output),
        }
    }
}

impl ColumnMergerGuest {
    pub(super) async fn call_merge(
        &self,
        store: &mut Store<WasiHostState>,
        input: Resource<ConflictSourceResource>,
        output: Resource<ResolutionSinkResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::Combined(guest) => guest.call_merge(store, input, output),
            Self::Narrow(guest) => guest.call_merge(store, input, output),
        }
    }
}

pub(super) fn store_is_retired(_store: &HostStore) -> bool {
    false
}
