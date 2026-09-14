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
use super::{CompileProfile, CompiledComponentKey, WasmtimePluginRuntime, create_store};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};
pub(super) mod bindings {
    mod generated {
        wasmtime::component::bindgen!({
            path: "wit",
            world: "plugin",
            exports: { default: async },
            with: {
                "lix:plugin-v2/host.snapshot": super::super::SnapshotResource,
                "lix:plugin-v2/host.transition": super::super::TransitionResource,
                "lix:plugin-v2/host.column-merge-source": super::super::ConflictSourceResource,
                "lix:plugin-v2/host.row-source": super::super::RowSourceResource,
                "lix:plugin-v2/host.column-merge-sink": super::super::ResolutionSinkResource,
            },
        });
    }
    pub use generated::Plugin;
    pub mod lix {
        pub use super::generated::lix::plugin_v2 as plugin;
    }
    pub mod exports {
        pub mod lix {
            pub use super::super::generated::exports::lix::plugin_v2 as plugin;
        }
    }
}

pub(super) mod file_projection_bindings {
    mod generated {
        wasmtime::component::bindgen!({
            path: "wit",
            world: "file-projection-plugin",
            exports: { default: async },
            with: {
                "lix:plugin-v2/host": super::super::bindings::lix::plugin::host,
                "lix:plugin-v2/types": super::super::bindings::lix::plugin::types,
            },
        });
    }
    pub use generated::FileProjectionPlugin;
    pub mod exports {
        pub mod lix {
            pub use super::super::generated::exports::lix::plugin_v2 as plugin;
        }
    }
}

pub(super) mod column_merger_bindings {
    mod generated {
        wasmtime::component::bindgen!({
            path: "wit",
            world: "column-merger-plugin",
            exports: { default: async },
            with: {
                "lix:plugin-v2/host": super::super::bindings::lix::plugin::host,
                "lix:plugin-v2/types": super::super::bindings::lix::plugin::types,
            },
        });
    }
    pub use generated::ColumnMergerPlugin;
    pub mod exports {
        pub mod lix {
            pub use super::super::generated::exports::lix::plugin_v2 as plugin;
        }
    }
}

// Retained host bindings for archives built before the major-only package name.
pub(super) mod legacy_bindings {
    wasmtime::component::bindgen!({
        path: "wit-legacy-v2",
        world: "plugin",
        exports: { default: async },
        with: {
            "lix:plugin/types": super::bindings::lix::plugin::types,
            "lix:plugin/host.snapshot": super::SnapshotResource,
            "lix:plugin/host.transition": super::TransitionResource,
            "lix:plugin/host.column-merge-source": super::ConflictSourceResource,
            "lix:plugin/host.row-source": super::RowSourceResource,
            "lix:plugin/host.column-merge-sink": super::ResolutionSinkResource,
        },
    });
}

pub(super) mod legacy_file_projection_bindings {
    wasmtime::component::bindgen!({
        path: "wit-legacy-v2",
        world: "file-projection-plugin",
        exports: { default: async },
        with: {
            "lix:plugin/host": super::legacy_bindings::lix::plugin::host,
            "lix:plugin/types": super::bindings::lix::plugin::types,
        },
    });
}

pub(super) mod legacy_column_merger_bindings {
    wasmtime::component::bindgen!({
        path: "wit-legacy-v2",
        world: "column-merger-plugin",
        exports: { default: async },
        with: {
            "lix:plugin/host": super::legacy_bindings::lix::plugin::host,
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
    LegacyCombined(Arc<Linker<WasiHostState>>),
    LegacyFileProjection(Arc<Linker<WasiHostState>>),
    LegacyColumnMerger(Arc<Linker<WasiHostState>>),
    Combined(Arc<Linker<WasiHostState>>),
    FileProjection(Arc<Linker<WasiHostState>>),
    ColumnMerger(Arc<Linker<WasiHostState>>),
}

fn configure_wasi_linker(linker: &mut Linker<WasiHostState>) -> wasmtime::Result<()> {
    // Guest diagnostics and abort paths may flush WASI streams. Their sync
    // adapters call block_on, which cannot run inside our async actor methods.
    wasmtime_wasi::p2::add_to_linker_async(linker)
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
    configure_wasi_linker(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure component WASI linker", error))?;
    let legacy = component.component_type().exports(engine).any(|(name, _)| {
        matches!(
            name,
            "lix:plugin/file-projection@2.0.0" | "lix:plugin/column-merger@2.0.0"
        )
    });
    let linker = match (
        legacy,
        capabilities.column_merger,
        capabilities.file_projection,
    ) {
        (true, true, true) => {
            legacy_bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
                &mut linker,
                |state| state,
            )
            .map_err(|error| {
                wasm_runtime_error("failed to configure combined plugin linker", error)
            })?;
            ComponentLinker::LegacyCombined(Arc::new(linker))
        }
        (true, false, true) => {
            legacy_file_projection_bindings::FileProjectionPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure file projection linker", error)
            })?;
            ComponentLinker::LegacyFileProjection(Arc::new(linker))
        }
        (true, true, false) => {
            legacy_column_merger_bindings::ColumnMergerPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure column merger linker", error)
            })?;
            ComponentLinker::LegacyColumnMerger(Arc::new(linker))
        }
        (false, true, true) => {
            bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(
                &mut linker,
                |state| state,
            )
            .map_err(|error| {
                wasm_runtime_error("failed to configure combined plugin linker", error)
            })?;
            ComponentLinker::Combined(Arc::new(linker))
        }
        (false, false, true) => {
            file_projection_bindings::FileProjectionPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure file projection linker", error)
            })?;
            ComponentLinker::FileProjection(Arc::new(linker))
        }
        (false, true, false) => {
            column_merger_bindings::ColumnMergerPlugin::add_to_linker::<
                _,
                wasmtime::component::HasSelf<_>,
            >(&mut linker, |state| state)
            .map_err(|error| {
                wasm_runtime_error("failed to configure column merger linker", error)
            })?;
            ComponentLinker::ColumnMerger(Arc::new(linker))
        }
        (_, false, false) => {
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
            ComponentLinker::LegacyCombined(linker) => {
                let instance =
                    legacy_bindings::Plugin::instantiate_async(&mut store, &self.component, linker)
                        .await
                        .map_err(|error| {
                            wasm_runtime_error("failed to instantiate combined plugin actor", error)
                        })?;
                (
                    Some(FileProjectionGuest::LegacyCombined(
                        instance.lix_plugin_file_projection().clone(),
                    )),
                    Some(ColumnMergerGuest::LegacyCombined(
                        instance.lix_plugin_column_merger().clone(),
                    )),
                )
            }
            ComponentLinker::LegacyFileProjection(linker) => {
                let instance =
                    legacy_file_projection_bindings::FileProjectionPlugin::instantiate_async(
                        &mut store,
                        &self.component,
                        linker,
                    )
                    .await
                    .map_err(|error| {
                        wasm_runtime_error("failed to instantiate file projection actor", error)
                    })?;
                (
                    Some(FileProjectionGuest::LegacyNarrow(
                        instance.lix_plugin_file_projection().clone(),
                    )),
                    None,
                )
            }
            ComponentLinker::LegacyColumnMerger(linker) => {
                let instance =
                    legacy_column_merger_bindings::ColumnMergerPlugin::instantiate_async(
                        &mut store,
                        &self.component,
                        linker,
                    )
                    .await
                    .map_err(|error| {
                        wasm_runtime_error("failed to instantiate column merger actor", error)
                    })?;
                (
                    None,
                    Some(ColumnMergerGuest::LegacyNarrow(
                        instance.lix_plugin_column_merger().clone(),
                    )),
                )
            }
            ComponentLinker::Combined(linker) => {
                let instance =
                    bindings::Plugin::instantiate_async(&mut store, &self.component, linker)
                        .await
                        .map_err(|error| {
                            wasm_runtime_error("failed to instantiate combined plugin actor", error)
                        })?;
                (
                    Some(FileProjectionGuest::Combined(
                        instance.lix_plugin_v2_file_projection().clone(),
                    )),
                    Some(ColumnMergerGuest::Combined(
                        instance.lix_plugin_v2_column_merger().clone(),
                    )),
                )
            }
            ComponentLinker::FileProjection(linker) => {
                let instance = file_projection_bindings::FileProjectionPlugin::instantiate_async(
                    &mut store,
                    &self.component,
                    linker,
                )
                .await
                .map_err(|error| {
                    wasm_runtime_error("failed to instantiate file projection actor", error)
                })?;
                (
                    Some(FileProjectionGuest::Narrow(
                        instance.lix_plugin_v2_file_projection().clone(),
                    )),
                    None,
                )
            }
            ComponentLinker::ColumnMerger(linker) => {
                let instance = column_merger_bindings::ColumnMergerPlugin::instantiate_async(
                    &mut store,
                    &self.component,
                    linker,
                )
                .await
                .map_err(|error| {
                    wasm_runtime_error("failed to instantiate column merger actor", error)
                })?;
                (
                    None,
                    Some(ColumnMergerGuest::Narrow(
                        instance.lix_plugin_v2_column_merger().clone(),
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
    LegacyCombined(legacy_bindings::exports::lix::plugin::file_projection::Guest),
    LegacyNarrow(legacy_file_projection_bindings::exports::lix::plugin::file_projection::Guest),
    Combined(bindings::exports::lix::plugin::file_projection::Guest),
    Narrow(file_projection_bindings::exports::lix::plugin::file_projection::Guest),
}

pub(super) enum ColumnMergerGuest {
    LegacyCombined(legacy_bindings::exports::lix::plugin::column_merger::Guest),
    LegacyNarrow(legacy_column_merger_bindings::exports::lix::plugin::column_merger::Guest),
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
            Self::LegacyCombined(guest) => guest.call_parse(store, input, output).await,
            Self::LegacyNarrow(guest) => guest.call_parse(store, input, output).await,
            Self::Combined(guest) => guest.call_parse(store, input, output).await,
            Self::Narrow(guest) => guest.call_parse(store, input, output).await,
        }
    }

    pub(super) async fn call_parse_changes(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::ParseChangesRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::LegacyCombined(guest) => guest.call_parse_changes(store, input, output).await,
            Self::LegacyNarrow(guest) => guest.call_parse_changes(store, input, output).await,
            Self::Combined(guest) => guest.call_parse_changes(store, input, output).await,
            Self::Narrow(guest) => guest.call_parse_changes(store, input, output).await,
        }
    }

    pub(super) async fn call_serialize(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::SerializeRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::LegacyCombined(guest) => guest.call_serialize(store, input, output).await,
            Self::LegacyNarrow(guest) => guest.call_serialize(store, input, output).await,
            Self::Combined(guest) => guest.call_serialize(store, input, output).await,
            Self::Narrow(guest) => guest.call_serialize(store, input, output).await,
        }
    }

    pub(super) async fn call_serialize_changes(
        &self,
        store: &mut Store<WasiHostState>,
        input: &bindings::lix::plugin::types::SerializeChangesRequest,
        output: Resource<TransitionResource>,
    ) -> wasmtime::Result<Result<(), bindings::lix::plugin::types::PluginError>> {
        match self {
            Self::LegacyCombined(guest) => guest.call_serialize_changes(store, input, output).await,
            Self::LegacyNarrow(guest) => guest.call_serialize_changes(store, input, output).await,
            Self::Combined(guest) => guest.call_serialize_changes(store, input, output).await,
            Self::Narrow(guest) => guest.call_serialize_changes(store, input, output).await,
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
            Self::LegacyCombined(guest) => guest.call_merge(store, input, output).await,
            Self::LegacyNarrow(guest) => guest.call_merge(store, input, output).await,
            Self::Combined(guest) => guest.call_merge(store, input, output).await,
            Self::Narrow(guest) => guest.call_merge(store, input, output).await,
        }
    }
}

pub(super) fn store_is_retired(_store: &HostStore) -> bool {
    false
}

// The Wasmtime linker requires these aggregate marker traits in addition to
// the resource implementations provided by the shared host.
impl bindings::lix::plugin::host::Host for HostState {}
impl bindings::lix::plugin::types::Host for HostState {}

// Legacy interface names retain the same v2 host behavior.
impl legacy_bindings::lix::plugin::host::HostSnapshot for HostState {
    fn file_len(&mut self, resource: Resource<SnapshotResource>) -> u64 {
        <Self as bindings::lix::plugin::host::HostSnapshot>::file_len(self, resource)
    }
    fn read_file(
        &mut self,
        resource: Resource<SnapshotResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostSnapshot>::read_file(
            self, resource, offset, length,
        )
        .map_err(Into::into)
    }
    fn read_state(
        &mut self,
        resource: Resource<SnapshotResource>,
        key: Vec<u8>,
        offset: u64,
        max_bytes: u32,
    ) -> Result<
        Option<legacy_bindings::lix::plugin::host::RecordChunk>,
        legacy_bindings::lix::plugin::host::HostError,
    > {
        <Self as bindings::lix::plugin::host::HostSnapshot>::read_state(
            self, resource, key, offset, max_bytes,
        )
        .map(|value| value.map(Into::into))
        .map_err(Into::into)
    }
    fn drop(&mut self, resource: Resource<SnapshotResource>) -> RuntimeResult<()> {
        <Self as bindings::lix::plugin::host::HostSnapshot>::drop(self, resource)
    }
}
impl legacy_bindings::lix::plugin::host::HostTransition for HostState {
    fn max_batch_bytes(&mut self, resource: Resource<TransitionResource>) -> u32 {
        <Self as bindings::lix::plugin::host::HostTransition>::max_batch_bytes(self, resource)
    }
    fn put_state(
        &mut self,
        resource: Resource<TransitionResource>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::put_state(self, resource, key, value)
            .map_err(Into::into)
    }
    fn delete_state(
        &mut self,
        resource: Resource<TransitionResource>,
        key: Vec<u8>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::delete_state(self, resource, key)
            .map_err(Into::into)
    }
    fn delete_state_prefix(
        &mut self,
        resource: Resource<TransitionResource>,
        prefix: Vec<u8>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::delete_state_prefix(
            self, resource, prefix,
        )
        .map_err(Into::into)
    }
    fn emit_rows(
        &mut self,
        resource: Resource<TransitionResource>,
        page: legacy_bindings::lix::plugin::host::RowPage,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::emit_rows(
            self,
            resource,
            page.into(),
        )
        .map_err(Into::into)
    }
    fn replace_all_rows(
        &mut self,
        resource: Resource<TransitionResource>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::replace_all_rows(self, resource)
            .map_err(Into::into)
    }
    fn emit_file_edit(
        &mut self,
        resource: Resource<TransitionResource>,
        edit: legacy_bindings::lix::plugin::host::FileEdit,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::emit_file_edit(
            self,
            resource,
            edit.into(),
        )
        .map_err(Into::into)
    }
    fn begin_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
        total_length: u64,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::begin_file_replacement(
            self,
            resource,
            total_length,
        )
        .map_err(Into::into)
    }
    fn write_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
        chunk: Vec<u8>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::write_file_replacement(
            self, resource, chunk,
        )
        .map_err(Into::into)
    }
    fn finish_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostTransition>::finish_file_replacement(
            self, resource,
        )
        .map_err(Into::into)
    }
    fn drop(&mut self, resource: Resource<TransitionResource>) -> RuntimeResult<()> {
        <Self as bindings::lix::plugin::host::HostTransition>::drop(self, resource)
    }
}
impl legacy_bindings::lix::plugin::host::HostRowSource for HostState {
    fn next_page(
        &mut self,
        resource: Resource<RowSourceResource>,
        max_bytes: u32,
    ) -> Result<
        Option<legacy_bindings::lix::plugin::host::RowPage>,
        legacy_bindings::lix::plugin::host::HostError,
    > {
        <Self as bindings::lix::plugin::host::HostRowSource>::next_page(self, resource, max_bytes)
            .map(|value| value.map(Into::into))
            .map_err(Into::into)
    }
    fn drop(&mut self, resource: Resource<RowSourceResource>) -> RuntimeResult<()> {
        <Self as bindings::lix::plugin::host::HostRowSource>::drop(self, resource)
    }
}
impl legacy_bindings::lix::plugin::host::HostColumnMergeSource for HostState {
    fn len(&mut self, resource: Resource<ConflictSourceResource>) -> u32 {
        <Self as bindings::lix::plugin::host::HostColumnMergeSource>::len(self, resource)
    }
    fn get(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
    ) -> Result<
        legacy_bindings::lix::plugin::host::ColumnMergeMeta,
        legacy_bindings::lix::plugin::host::HostError,
    > {
        <Self as bindings::lix::plugin::host::HostColumnMergeSource>::get(self, resource, index)
            .map(Into::into)
            .map_err(Into::into)
    }
    fn read_value(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
        side: legacy_bindings::lix::plugin::host::MergeSide,
        offset: u64,
        length: u32,
    ) -> Result<Option<Vec<u8>>, legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSource>::read_value(
            self,
            resource,
            index,
            side.into(),
            offset,
            length,
        )
        .map_err(Into::into)
    }
    fn read_row(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
        side: legacy_bindings::lix::plugin::host::MergeSide,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSource>::read_row(
            self,
            resource,
            index,
            side.into(),
            offset,
            length,
        )
        .map_err(Into::into)
    }
    fn drop(&mut self, resource: Resource<ConflictSourceResource>) -> RuntimeResult<()> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSource>::drop(self, resource)
    }
}
impl legacy_bindings::lix::plugin::host::HostColumnMergeSink for HostState {
    fn max_batch_bytes(&mut self, resource: Resource<ResolutionSinkResource>) -> u32 {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::max_batch_bytes(self, resource)
    }
    fn use_lww(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::use_lww(self, resource, ordinal)
            .map_err(Into::into)
    }
    fn begin_replace(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
        total_length: Option<u64>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::begin_replace(
            self,
            resource,
            ordinal,
            total_length,
        )
        .map_err(Into::into)
    }
    fn write_replacement(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        chunk: Vec<u8>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::write_replacement(
            self, resource, chunk,
        )
        .map_err(Into::into)
    }
    fn finish_replace(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
    ) -> Result<(), legacy_bindings::lix::plugin::host::HostError> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::finish_replace(self, resource)
            .map_err(Into::into)
    }
    fn drop(&mut self, resource: Resource<ResolutionSinkResource>) -> RuntimeResult<()> {
        <Self as bindings::lix::plugin::host::HostColumnMergeSink>::drop(self, resource)
    }
}
impl legacy_bindings::lix::plugin::host::Host for HostState {}
impl From<bindings::lix::plugin::host::RecordChunk>
    for legacy_bindings::lix::plugin::host::RecordChunk
{
    fn from(value: bindings::lix::plugin::host::RecordChunk) -> Self {
        Self {
            total_len: value.total_len,
            bytes: value.bytes,
        }
    }
}
impl From<legacy_bindings::lix::plugin::host::RecordChunk>
    for bindings::lix::plugin::host::RecordChunk
{
    fn from(value: legacy_bindings::lix::plugin::host::RecordChunk) -> Self {
        Self {
            total_len: value.total_len,
            bytes: value.bytes,
        }
    }
}
impl From<bindings::lix::plugin::host::RowPage> for legacy_bindings::lix::plugin::host::RowPage {
    fn from(value: bindings::lix::plugin::host::RowPage) -> Self {
        Self {
            payload: value.payload,
            attachments: value.attachments,
        }
    }
}
impl From<legacy_bindings::lix::plugin::host::RowPage> for bindings::lix::plugin::host::RowPage {
    fn from(value: legacy_bindings::lix::plugin::host::RowPage) -> Self {
        Self {
            payload: value.payload,
            attachments: value.attachments,
        }
    }
}
impl From<bindings::lix::plugin::host::FileEdit> for legacy_bindings::lix::plugin::host::FileEdit {
    fn from(value: bindings::lix::plugin::host::FileEdit) -> Self {
        Self {
            offset: value.offset,
            delete_len: value.delete_len,
            insert: value.insert,
        }
    }
}
impl From<legacy_bindings::lix::plugin::host::FileEdit> for bindings::lix::plugin::host::FileEdit {
    fn from(value: legacy_bindings::lix::plugin::host::FileEdit) -> Self {
        Self {
            offset: value.offset,
            delete_len: value.delete_len,
            insert: value.insert,
        }
    }
}
impl From<bindings::lix::plugin::host::ColumnMergeMeta>
    for legacy_bindings::lix::plugin::host::ColumnMergeMeta
{
    fn from(value: bindings::lix::plugin::host::ColumnMergeMeta) -> Self {
        Self {
            ordinal: value.ordinal,
            schema_key: value.schema_key,
            primary_key: value.primary_key,
            schema_fingerprint: value.schema_fingerprint,
            file_id: value.file_id,
            column: value.column,
            base_len: value.base_len,
            a_len: value.a_len,
            b_len: value.b_len,
            base_row_len: value.base_row_len,
            a_row_len: value.a_row_len,
            b_row_len: value.b_row_len,
        }
    }
}
impl From<legacy_bindings::lix::plugin::host::ColumnMergeMeta>
    for bindings::lix::plugin::host::ColumnMergeMeta
{
    fn from(value: legacy_bindings::lix::plugin::host::ColumnMergeMeta) -> Self {
        Self {
            ordinal: value.ordinal,
            schema_key: value.schema_key,
            primary_key: value.primary_key,
            schema_fingerprint: value.schema_fingerprint,
            file_id: value.file_id,
            column: value.column,
            base_len: value.base_len,
            a_len: value.a_len,
            b_len: value.b_len,
            base_row_len: value.base_row_len,
            a_row_len: value.a_row_len,
            b_row_len: value.b_row_len,
        }
    }
}
impl From<bindings::lix::plugin::host::HostError>
    for legacy_bindings::lix::plugin::host::HostError
{
    fn from(value: bindings::lix::plugin::host::HostError) -> Self {
        match value {
            bindings::lix::plugin::host::HostError::InvalidRange => Self::InvalidRange,
            bindings::lix::plugin::host::HostError::LimitExceeded(message) => {
                Self::LimitExceeded(message)
            }
            bindings::lix::plugin::host::HostError::Rejected(message) => Self::Rejected(message),
        }
    }
}
impl From<legacy_bindings::lix::plugin::host::MergeSide>
    for bindings::lix::plugin::host::MergeSide
{
    fn from(value: legacy_bindings::lix::plugin::host::MergeSide) -> Self {
        match value {
            legacy_bindings::lix::plugin::host::MergeSide::Base => Self::Base,
            legacy_bindings::lix::plugin::host::MergeSide::A => Self::A,
            legacy_bindings::lix::plugin::host::MergeSide::B => Self::B,
        }
    }
}

#[cfg(test)]
mod tests {
    use wasmtime::component::{Component, Linker};
    use wasmtime::{Engine, Store};

    async fn flushing_guest_trap_is_an_error() {
        let engine = Engine::default();
        let component = Component::new(
            &engine,
            include_bytes!(
                "../../../../tests/fixtures/plugin-api/diagnostics/flush-then-trap.wasm"
            ),
        )
        .unwrap();
        let mut linker = Linker::<super::WasiHostState>::new(&engine);
        super::configure_wasi_linker(&mut linker).unwrap();
        // A guest trap must leave the Tokio caller able to create and run
        // another component. No real OOM is needed to exercise stderr flushing.
        for _ in 0..2 {
            let mut store =
                super::create_store(&engine, crate::wasm::WasmLimits::default()).unwrap();
            let guest = wasmtime_wasi::p2::bindings::Command::instantiate_async(
                &mut store, &component, &linker,
            )
            .await
            .unwrap();
            assert!(guest.wasi_cli_run().call_run(&mut store).await.is_err());
        }
    }

    #[tokio::test]
    async fn wasi_diagnostic_flush_returns_guest_error_on_current_thread() {
        flushing_guest_trap_is_an_error().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wasi_diagnostic_flush_returns_guest_error_on_multiple_threads() {
        flushing_guest_trap_is_an_error().await;
    }

    #[test]
    fn major_only_host_can_add_functions_without_rebuilding_existing_components() {
        // This frozen component imports only existing-operation. Adding another
        // host function under the same major identity must not change its ABI.
        let engine = Engine::default();
        let component = Component::new(
            &engine,
            include_bytes!("../../../../tests/fixtures/plugin-api/v2/import-subset.wasm"),
        )
        .expect("frozen component should compile");
        let mut linker = Linker::<()>::new(&engine);
        let mut host = linker.instance("lix:plugin-v2/host").unwrap();
        host.func_wrap("existing-operation", |_, (): ()| Ok((7_u32,)))
            .unwrap();
        host.func_wrap("new-operation", |_, (): ()| Ok((9_u32,)))
            .unwrap();
        let mut store = Store::new(&engine, ());
        let instance = linker.instantiate(&mut store, &component).unwrap();
        let run = instance
            .get_typed_func::<(), (u32,)>(&mut store, "run")
            .unwrap();
        assert_eq!(run.call(&mut store, ()).unwrap(), (7,));
    }
}
