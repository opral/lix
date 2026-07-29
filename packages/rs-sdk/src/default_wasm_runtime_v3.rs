//! Wasmtime Component bindings for Plugin API v3.
//!
//! This starts with the generated hard-cut contract. Host resource
//! implementations live here rather than in the engine-neutral crate so
//! content-addressed arena roots never acquire Wasmtime lifetimes.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lix_engine::LixError;
use lix_engine::wasm::v3::{
    Error as ArenaStoreError, KeyedPage, Root, Transaction, WasmComponentV3Actor,
    WasmComponentV3Factory, WasmV3ByteEdit, WasmV3ChangeCursorHandle, WasmV3CreateContext,
    WasmV3EditCursorHandle, WasmV3EntityChange, WasmV3EntityTransition, WasmV3EntityUpdate,
    WasmV3FileDescriptor, WasmV3FileTransition, WasmV3FileUpdate, WasmV3InputBytes,
    WasmV3OpenEntitiesInput, WasmV3OpenFileInput, WasmV3TransitionCounters, WasmV3TransitionHandle,
    WasmV3TransitionLimits, entity_arena_key,
};
use wasmtime::component::{Component, Linker, Resource, ResourceAny};

use super::*;

pub struct V3BudgetResource {
    max_page_bytes: u32,
    max_pages: u32,
    max_total_bytes: u64,
    deadline_nanoseconds: u64,
    started: Instant,
    pages: u32,
    bytes: u64,
    counters: WasmV3TransitionCounters,
}

impl V3BudgetResource {
    pub(super) fn new(
        max_page_bytes: u32,
        max_pages: u32,
        max_total_bytes: u64,
        deadline_nanoseconds: u64,
    ) -> Self {
        Self {
            max_page_bytes,
            max_pages,
            max_total_bytes,
            deadline_nanoseconds,
            started: Instant::now(),
            pages: 0,
            bytes: 0,
            counters: WasmV3TransitionCounters::default(),
        }
    }

    fn remaining_nanoseconds(&self) -> u64 {
        self.deadline_nanoseconds
            .saturating_sub(u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }

    fn charge(&mut self, bytes: usize, kind: ArenaReadKind) -> Result<(), ArenaError> {
        if self.remaining_nanoseconds() == 0 {
            return Err(ArenaError::DeadlineExceeded);
        }
        let bytes = u64::try_from(bytes).map_err(|_| ArenaError::RecordTooLarge(u64::MAX))?;
        if bytes > u64::from(self.max_page_bytes) {
            return Err(ArenaError::RecordTooLarge(bytes));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| ArenaError::LimitExceeded("page counter overflowed".to_owned()))?;
        if self.pages > self.max_pages {
            return Err(ArenaError::LimitExceeded(
                "transition page limit exceeded".to_owned(),
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| ArenaError::LimitExceeded("byte counter overflowed".to_owned()))?;
        if self.bytes > self.max_total_bytes {
            return Err(ArenaError::LimitExceeded(
                "transition byte limit exceeded".to_owned(),
            ));
        }
        match kind {
            ArenaReadKind::File => {
                self.counters.file_page_reads = self.counters.file_page_reads.saturating_add(1);
                self.counters.file_page_bytes = self.counters.file_page_bytes.saturating_add(bytes);
            }
            ArenaReadKind::Entity => {
                self.counters.entity_page_reads = self.counters.entity_page_reads.saturating_add(1);
                self.counters.entity_page_bytes =
                    self.counters.entity_page_bytes.saturating_add(bytes);
            }
            ArenaReadKind::State => {
                self.counters.state_page_reads = self.counters.state_page_reads.saturating_add(1);
                self.counters.state_page_bytes =
                    self.counters.state_page_bytes.saturating_add(bytes);
            }
        }
        Ok(())
    }
}

pub struct V3RootResource(pub(super) Root);
pub struct V3TransactionResource(pub(super) Option<Transaction>);

#[derive(Clone, Copy)]
enum ArenaReadKind {
    File,
    Entity,
    State,
}

struct WasmtimeV3Factory {
    shared: Arc<WasmtimeSharedRuntime>,
    component: Component,
    linker: Arc<Linker<WasiHostState>>,
    limits: WasmLimits,
    profile: CompileProfile,
}

struct ActiveTransition {
    budget_rep: u32,
    transaction_rep: u32,
    cursor_handle: u64,
    cursor_kind: CursorKind,
    eof: bool,
    seen_entity_keys: BTreeSet<Vec<u8>>,
    previous_edit_end: u64,
    counters: WasmV3TransitionCounters,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CursorKind {
    Changes,
    Edits,
}

struct WasmtimeV3Actor {
    store: Option<Store<WasiHostState>>,
    guest: bindings::exports::lix::plugin::api::Guest,
    limits: WasmLimits,
    _timeout_ticker: TimeoutTickerLease,
    next_handle: u64,
    cursors: HashMap<u64, ResourceAny>,
    transitions: HashMap<u64, ActiveTransition>,
}

pub(super) mod bindings {
    wasmtime::component::bindgen!({
        path: "wit/v3",
        world: "plugin",
        with: {
            "lix:plugin/arena.budget": super::V3BudgetResource,
            "lix:plugin/arena.root": super::V3RootResource,
            "lix:plugin/arena.transaction": super::V3TransactionResource,
        },
    });
}

pub(super) async fn compile_component(
    runtime: &WasmtimePluginRuntime,
    bytes: Vec<u8>,
    limits: WasmLimits,
) -> Result<Arc<dyn WasmComponentV3Factory>, LixError> {
    if limits.max_memory_bytes == 0 {
        return Err(v3_invalid_param(
            "v3 component memory limit must be positive",
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
                .map_err(|error| wasm_runtime_error("failed to compile v3 plugin component", error))
        })
        .await?;
    let linker = Arc::new(create_linker(engine)?);
    Ok(Arc::new(WasmtimeV3Factory {
        shared: runtime.shared.clone(),
        component,
        linker,
        limits,
        profile,
    }))
}

fn create_linker(engine: &Engine) -> Result<Linker<WasiHostState>, LixError> {
    let mut linker = Linker::<WasiHostState>::new(engine);
    add_to_linker_sync(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure v3 WASI linker", error))?;
    bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |state| {
        state
    })
    .map_err(|error| wasm_runtime_error("failed to configure v3 plugin linker", error))?;
    Ok(linker)
}

#[async_trait]
impl WasmComponentV3Factory for WasmtimeV3Factory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV3Actor>, LixError> {
        let engine = self.shared.engine(self.profile);
        let timeout_ticker = self
            .shared
            .timeout_ticker(self.profile)?
            .ok_or_else(|| v3_invalid_plugin("v3 actor requires an epoch timeout ticker"))?;
        let mut store = create_store(engine, self.limits)?;
        store.epoch_deadline_trap();
        reset_store_limits(&mut store, self.limits)?;
        let plugin = bindings::Plugin::instantiate(&mut store, &self.component, &self.linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate v3 plugin actor", error))?;
        Ok(Box::new(WasmtimeV3Actor {
            store: Some(store),
            guest: plugin.lix_plugin_api().clone(),
            limits: self.limits,
            _timeout_ticker: timeout_ticker,
            next_handle: 1,
            cursors: HashMap::new(),
            transitions: HashMap::new(),
        }))
    }
}

impl WasmtimeV3Actor {
    fn store_mut(&mut self) -> Result<&mut Store<WasiHostState>, LixError> {
        self.store
            .as_mut()
            .ok_or_else(|| v3_invalid_plugin("v3 actor was retired after a trap"))
    }

    fn allocate_handle(&mut self) -> Result<u64, LixError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| v3_invalid_plugin("v3 actor handle space exhausted"))?;
        Ok(handle)
    }

    fn prepare_call(&mut self, limits: WasmV3TransitionLimits) -> Result<(), LixError> {
        let component_limits = self.limits;
        reset_store_limits(self.store_mut()?, component_limits)?;
        let epoch_ticks = limits
            .deadline_nanoseconds
            .saturating_add(999_999)
            .div_ceil(1_000_000);
        self.store_mut()?.set_epoch_deadline(epoch_ticks.max(1));
        Ok(())
    }

    fn push_inputs(
        &mut self,
        limits: WasmV3TransitionLimits,
        root: Root,
        transaction: Transaction,
    ) -> Result<
        (
            Resource<V3BudgetResource>,
            Resource<V3RootResource>,
            Resource<V3TransactionResource>,
        ),
        LixError,
    > {
        let store = self.store_mut()?;
        let budget = store
            .data_mut()
            .table
            .push(V3BudgetResource::new(
                limits.max_page_bytes,
                limits.max_pages,
                limits.max_total_bytes,
                limits.deadline_nanoseconds,
            ))
            .map_err(|error| wasm_runtime_error("failed to allocate v3 budget", error))?;
        let root = store
            .data_mut()
            .table
            .push(V3RootResource(root))
            .map_err(|error| wasm_runtime_error("failed to allocate v3 root", error))?;
        let transaction = store
            .data_mut()
            .table
            .push(V3TransactionResource(Some(transaction)))
            .map_err(|error| wasm_runtime_error("failed to allocate v3 transaction", error))?;
        Ok((budget, root, transaction))
    }

    fn register_file_transition(
        &mut self,
        budget_rep: u32,
        value: bindings::exports::lix::plugin::api::FileTransition,
    ) -> Result<WasmV3FileTransition, LixError> {
        let transaction = value.successor;
        let cursor_handle = self.allocate_handle()?;
        self.cursors.insert(cursor_handle, value.changes);
        let transition_handle = self.allocate_handle()?;
        self.transitions.insert(
            transition_handle,
            ActiveTransition {
                budget_rep,
                transaction_rep: transaction.rep(),
                cursor_handle,
                cursor_kind: CursorKind::Changes,
                eof: false,
                seen_entity_keys: BTreeSet::new(),
                previous_edit_end: 0,
                counters: WasmV3TransitionCounters::default(),
            },
        );
        Ok(WasmV3FileTransition {
            transition: WasmV3TransitionHandle(transition_handle),
            changes: WasmV3ChangeCursorHandle(cursor_handle),
        })
    }

    fn register_entity_transition(
        &mut self,
        budget_rep: u32,
        value: bindings::exports::lix::plugin::api::EntityTransition,
    ) -> Result<WasmV3EntityTransition, LixError> {
        let transaction = value.successor;
        let cursor_handle = self.allocate_handle()?;
        self.cursors.insert(cursor_handle, value.edits);
        let transition_handle = self.allocate_handle()?;
        self.transitions.insert(
            transition_handle,
            ActiveTransition {
                budget_rep,
                transaction_rep: transaction.rep(),
                cursor_handle,
                cursor_kind: CursorKind::Edits,
                eof: false,
                seen_entity_keys: BTreeSet::new(),
                previous_edit_end: 0,
                counters: WasmV3TransitionCounters::default(),
            },
        );
        Ok(WasmV3EntityTransition {
            transition: WasmV3TransitionHandle(transition_handle),
            edits: WasmV3EditCursorHandle(cursor_handle),
        })
    }

    fn plugin_error(
        operation: &str,
        error: bindings::exports::lix::plugin::api::PluginError,
    ) -> LixError {
        use bindings::exports::lix::plugin::api::PluginError;
        match error {
            PluginError::InvalidInput(message) => LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("{operation}: {message}"),
            ),
            PluginError::RecordTooLarge(bytes) => v3_invalid_param(format!(
                "{operation}: plugin record is too large ({bytes} bytes)"
            )),
            PluginError::LimitExceeded(message) => {
                v3_invalid_param(format!("{operation}: {message}"))
            }
            PluginError::DeadlineExceeded => LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("{operation}: deadline elapsed"),
            ),
            PluginError::Internal(message) => v3_invalid_plugin(format!("{operation}: {message}")),
        }
    }
}

fn descriptor_to_binding(
    descriptor: WasmV3FileDescriptor,
) -> bindings::exports::lix::plugin::api::FileDescriptor {
    bindings::exports::lix::plugin::api::FileDescriptor {
        path: descriptor.path,
        media_type: descriptor.media_type,
        plugin_key: descriptor.plugin_key,
        generation: descriptor.generation,
    }
}

fn creates_to_binding(
    creates: WasmV3CreateContext,
) -> bindings::exports::lix::plugin::api::CreateContext {
    bindings::exports::lix::plugin::api::CreateContext {
        high: creates.high,
        low: creates.low,
    }
}

fn input_splice_to_binding(
    edit: lix_engine::wasm::v3::WasmV3InputSplice,
) -> bindings::exports::lix::plugin::api::InputSplice {
    use bindings::exports::lix::plugin::api::{InputBytes, SourceRange};
    bindings::exports::lix::plugin::api::InputSplice {
        offset: edit.offset,
        delete_len: edit.delete_len,
        insert: match edit.insert {
            WasmV3InputBytes::Inline(bytes) => InputBytes::Inline(bytes),
            WasmV3InputBytes::AfterRange(range) => InputBytes::AfterRange(SourceRange {
                offset: range.offset,
                length: range.length,
            }),
        },
    }
}

#[async_trait]
impl WasmComponentV3Actor for WasmtimeV3Actor {
    async fn open_file(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3OpenFileInput,
    ) -> Result<WasmV3FileTransition, LixError> {
        let limits = limits.validate()?;
        self.prepare_call(limits)?;
        let (budget, accepted, successor) =
            self.push_inputs(limits, input.accepted, input.successor)?;
        let budget_rep = budget.rep();
        let binding_input = bindings::exports::lix::plugin::api::OpenFileInput {
            descriptor: descriptor_to_binding(input.descriptor),
            accepted,
            successor,
            creates: creates_to_binding(input.creates),
        };
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.call_open_file(
                self.store_mut()?,
                Resource::new_borrow(budget_rep),
                &binding_input,
            )
        });
        let value = self.resolve_top_level_call("v3 open-file", budget_rep, result)?;
        self.register_file_transition(budget_rep, value)
    }

    async fn file_changed(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3FileUpdate,
    ) -> Result<WasmV3FileTransition, LixError> {
        let limits = limits.validate()?;
        self.prepare_call(limits)?;
        let (budget, before, successor) =
            self.push_inputs(limits, input.before, input.successor)?;
        let budget_rep = budget.rep();
        let binding_input = bindings::exports::lix::plugin::api::FileUpdate {
            before_descriptor: descriptor_to_binding(input.before_descriptor),
            after_descriptor: descriptor_to_binding(input.after_descriptor),
            before,
            edits: input
                .edits
                .into_iter()
                .map(input_splice_to_binding)
                .collect(),
            successor,
            creates: creates_to_binding(input.creates),
        };
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.call_file_changed(
                self.store_mut()?,
                Resource::new_borrow(budget_rep),
                &binding_input,
            )
        });
        let value = self.resolve_top_level_call("v3 file-changed", budget_rep, result)?;
        self.register_file_transition(budget_rep, value)
    }

    async fn open_entities(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3OpenEntitiesInput,
    ) -> Result<WasmV3EntityTransition, LixError> {
        let limits = limits.validate()?;
        self.prepare_call(limits)?;
        let (budget, durable, successor) =
            self.push_inputs(limits, input.durable, input.successor)?;
        let budget_rep = budget.rep();
        let binding_input = bindings::exports::lix::plugin::api::OpenEntitiesInput {
            descriptor: descriptor_to_binding(input.descriptor),
            durable,
            successor,
            creates: creates_to_binding(input.creates),
        };
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.call_open_entities(
                self.store_mut()?,
                Resource::new_borrow(budget_rep),
                &binding_input,
            )
        });
        let value = self.resolve_top_level_call("v3 open-entities", budget_rep, result)?;
        self.register_entity_transition(budget_rep, value)
    }

    async fn entities_changed(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3EntityUpdate,
    ) -> Result<WasmV3EntityTransition, LixError> {
        let limits = limits.validate()?;
        self.prepare_call(limits)?;
        let (budget, before, successor) =
            self.push_inputs(limits, input.before, input.successor)?;
        let budget_rep = budget.rep();
        let binding_input = bindings::exports::lix::plugin::api::EntityUpdate {
            before_descriptor: descriptor_to_binding(input.before_descriptor),
            after_descriptor: descriptor_to_binding(input.after_descriptor),
            before,
            changed_entity_keys: input.changed_entity_keys,
            successor,
            creates: creates_to_binding(input.creates),
        };
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.call_entities_changed(
                self.store_mut()?,
                Resource::new_borrow(budget_rep),
                &binding_input,
            )
        });
        let value = self.resolve_top_level_call("v3 entities-changed", budget_rep, result)?;
        self.register_entity_transition(budget_rep, value)
    }

    async fn next_change_page(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor: WasmV3ChangeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmV3EntityChange>>, LixError> {
        let mut active = self.take_active(transition, cursor.0, CursorKind::Changes)?;
        if active.eof {
            self.transitions.insert(transition.0, active);
            return Ok(None);
        }
        let cursor_resource = *self
            .cursors
            .get(&cursor.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3 change cursor"))?;
        self.prepare_nested_call(&active)?;
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.change_cursor().call_next(
                self.store_mut()?,
                cursor_resource,
                Resource::new_borrow(active.budget_rep),
                max_bytes,
            )
        });
        let page = match result {
            Ok(Ok(page)) => page,
            Ok(Err(error)) => {
                let error = Self::plugin_error("v3 change-cursor.next", error);
                self.abort_active(active)?;
                return Err(error);
            }
            Err(error) => {
                self.retire();
                return Err(wasm_runtime_error("v3 change cursor trapped", error));
            }
        };
        let Some(page) = page else {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        };
        if page.is_empty() {
            self.abort_active(active)?;
            return Err(v3_invalid_plugin(
                "v3 change cursor returned an empty non-EOF page",
            ));
        }

        let mut output = Vec::with_capacity(page.len());
        for change in page {
            let key = entity_arena_key(&change.schema_key, &change.entity_pk)?;
            if !active.seen_entity_keys.insert(key.clone()) {
                self.abort_active(active)?;
                return Err(v3_invalid_plugin("v3 change cursor repeated an entity key"));
            }
            let page_bytes = entity_change_bytes(&change)?;
            active.counters.component_boundary_bytes = active
                .counters
                .component_boundary_bytes
                .saturating_add(page_bytes);
            let transaction = self.transaction_by_rep_mut(active.transaction_rep)?;
            match &change.snapshot {
                Some(snapshot) => transaction.upsert_entity(key, snapshot.clone()),
                None => transaction.delete_entity(key),
            }
            output.push(WasmV3EntityChange {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot,
                format_only: change.format_only,
            });
        }
        self.transitions.insert(transition.0, active);
        Ok(Some(output))
    }

    async fn next_edit_page(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor: WasmV3EditCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmV3ByteEdit>>, LixError> {
        let mut active = self.take_active(transition, cursor.0, CursorKind::Edits)?;
        if active.eof {
            self.transitions.insert(transition.0, active);
            return Ok(None);
        }
        let cursor_resource = *self
            .cursors
            .get(&cursor.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3 edit cursor"))?;
        self.prepare_nested_call(&active)?;
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.edit_cursor().call_next(
                self.store_mut()?,
                cursor_resource,
                Resource::new_borrow(active.budget_rep),
                max_bytes,
            )
        });
        let page = match result {
            Ok(Ok(page)) => page,
            Ok(Err(error)) => {
                let error = Self::plugin_error("v3 edit-cursor.next", error);
                self.abort_active(active)?;
                return Err(error);
            }
            Err(error) => {
                self.retire();
                return Err(wasm_runtime_error("v3 edit cursor trapped", error));
            }
        };
        let Some(page) = page else {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        };
        if page.is_empty() {
            self.abort_active(active)?;
            return Err(v3_invalid_plugin(
                "v3 edit cursor returned an empty non-EOF page",
            ));
        }

        let mut output = Vec::with_capacity(page.len());
        for edit in page {
            let end = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or_else(|| v3_invalid_plugin("v3 byte edit range overflowed"))?;
            if edit.offset < active.previous_edit_end {
                self.abort_active(active)?;
                return Err(v3_invalid_plugin(
                    "v3 byte edits are not globally base-relative and ordered",
                ));
            }
            let page_bytes = 24_u64.saturating_add(edit.insert.len() as u64);
            active.counters.component_boundary_bytes = active
                .counters
                .component_boundary_bytes
                .saturating_add(page_bytes);
            active.previous_edit_end = end;
            self.transaction_by_rep_mut(active.transaction_rep)?
                .edit_bytes(lix_engine::wasm::v3::ByteEdit {
                    offset: edit.offset,
                    delete_len: edit.delete_len,
                    insert: edit.insert.clone(),
                });
            output.push(WasmV3ByteEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert,
            });
        }
        self.transitions.insert(transition.0, active);
        Ok(Some(output))
    }

    async fn finish_transition(
        &mut self,
        transition: WasmV3TransitionHandle,
    ) -> Result<(Root, WasmV3TransitionCounters), LixError> {
        let active = self
            .transitions
            .remove(&transition.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3 transition"))?;
        if !active.eof {
            self.abort_active(active)?;
            return Err(v3_invalid_plugin(
                "v3 transition cannot commit before cursor EOF",
            ));
        }
        self.drop_cursor(active.cursor_handle)?;
        let guest_high_water = self
            .store_mut()?
            .data()
            .limits
            .linear_memory_high_water_bytes();
        let mut counters = self.budget_counters(active.budget_rep)?;
        counters.component_boundary_bytes = active.counters.component_boundary_bytes;
        counters.guest_linear_memory_high_water_bytes = guest_high_water;
        let transaction = self.take_transaction(active.transaction_rep)?;
        self.drop_budget(active.budget_rep)?;
        let root = transaction
            .commit()
            .map_err(|error| v3_invalid_plugin(format!("v3 commit failed: {error}")))?;
        Ok((root, counters))
    }

    async fn abort_transition(
        &mut self,
        transition: WasmV3TransitionHandle,
    ) -> Result<(), LixError> {
        let active = self
            .transitions
            .remove(&transition.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3 transition"))?;
        self.abort_active(active)
    }
}

impl WasmtimeV3Actor {
    fn resolve_top_level_call<T>(
        &mut self,
        operation: &str,
        budget_rep: u32,
        result: wasmtime::Result<Result<T, bindings::exports::lix::plugin::api::PluginError>>,
    ) -> Result<T, LixError> {
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                self.drop_budget(budget_rep)?;
                Err(Self::plugin_error(operation, error))
            }
            Err(error) => {
                self.retire();
                Err(wasm_runtime_error(operation, error))
            }
        }
    }

    fn take_active(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor_handle: u64,
        kind: CursorKind,
    ) -> Result<ActiveTransition, LixError> {
        let active = self
            .transitions
            .remove(&transition.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3 transition"))?;
        if active.cursor_handle != cursor_handle || active.cursor_kind != kind {
            self.transitions.insert(transition.0, active);
            return Err(v3_invalid_plugin(
                "v3 cursor does not belong to the transition",
            ));
        }
        Ok(active)
    }

    fn prepare_nested_call(&mut self, active: &ActiveTransition) -> Result<(), LixError> {
        let component_limits = self.limits;
        reset_store_limits(self.store_mut()?, component_limits)?;
        let remaining = self
            .store_mut()?
            .data()
            .table
            .get(&Resource::<V3BudgetResource>::new_borrow(active.budget_rep))
            .map_err(|error| wasm_runtime_error("missing v3 transition budget", error))?
            .remaining_nanoseconds();
        if remaining == 0 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "v3 transition deadline elapsed",
            ));
        }
        self.store_mut()?
            .set_epoch_deadline(remaining.saturating_add(999_999).div_ceil(1_000_000));
        Ok(())
    }

    fn transaction_by_rep_mut(&mut self, rep: u32) -> Result<&mut Transaction, LixError> {
        self.store_mut()?
            .data_mut()
            .table
            .get_mut(&Resource::<V3TransactionResource>::new_borrow(rep))
            .map_err(|error| wasm_runtime_error("missing v3 transaction", error))?
            .0
            .as_mut()
            .ok_or_else(|| v3_invalid_plugin("v3 transaction was already committed"))
    }

    fn take_transaction(&mut self, rep: u32) -> Result<Transaction, LixError> {
        self.store_mut()?
            .data_mut()
            .table
            .delete(Resource::<V3TransactionResource>::new_own(rep))
            .map_err(|error| wasm_runtime_error("failed to take v3 transaction", error))?
            .0
            .ok_or_else(|| v3_invalid_plugin("v3 transaction was already committed"))
    }

    fn drop_budget(&mut self, rep: u32) -> Result<(), LixError> {
        self.store_mut()?
            .data_mut()
            .table
            .delete(Resource::<V3BudgetResource>::new_own(rep))
            .map_err(|error| wasm_runtime_error("failed to drop v3 budget", error))?;
        Ok(())
    }

    fn budget_counters(&mut self, rep: u32) -> Result<WasmV3TransitionCounters, LixError> {
        Ok(self
            .store_mut()?
            .data()
            .table
            .get(&Resource::<V3BudgetResource>::new_borrow(rep))
            .map_err(|error| wasm_runtime_error("missing v3 budget counters", error))?
            .counters)
    }

    fn drop_cursor(&mut self, handle: u64) -> Result<(), LixError> {
        if let Some(cursor) = self.cursors.remove(&handle) {
            cursor
                .resource_drop(self.store_mut()?)
                .map_err(|error| wasm_runtime_error("failed to drop v3 cursor", error))?;
        }
        Ok(())
    }

    fn abort_active(&mut self, active: ActiveTransition) -> Result<(), LixError> {
        self.drop_cursor(active.cursor_handle)?;
        self.store_mut()?
            .data_mut()
            .table
            .delete(Resource::<V3TransactionResource>::new_own(
                active.transaction_rep,
            ))
            .map_err(|error| wasm_runtime_error("failed to roll back v3 transaction", error))?;
        self.drop_budget(active.budget_rep)
    }

    fn retire(&mut self) {
        self.store = None;
        self.cursors.clear();
        self.transitions.clear();
    }
}

fn entity_change_bytes(
    change: &bindings::exports::lix::plugin::api::EntityChange,
) -> Result<u64, LixError> {
    let mut bytes = 16_u64.saturating_add(change.schema_key.len() as u64);
    for part in &change.entity_pk {
        bytes = bytes.saturating_add(4).saturating_add(part.len() as u64);
    }
    bytes = bytes.saturating_add(change.snapshot.as_ref().map_or(0, Vec::len) as u64);
    Ok(bytes)
}

fn call_sync_guest<T: Send>(call: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| match scope.spawn(call).join() {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    })
}

fn v3_invalid_plugin(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn v3_invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

use bindings::lix::plugin::arena::{
    ArenaError, HostBudget, HostRoot, HostTransaction, KeyedPage as WitKeyedPage, Limits,
};

impl HostBudget for WasiHostState {
    fn limits(&mut self, budget: Resource<V3BudgetResource>) -> Limits {
        let budget = self
            .table
            .get(&budget)
            .expect("canonical resource handles cannot reference a missing v3 budget");
        Limits {
            max_page_bytes: budget.max_page_bytes,
            max_pages: budget.max_pages,
            max_total_bytes: budget.max_total_bytes,
            deadline_nanoseconds: budget.deadline_nanoseconds,
        }
    }

    fn remaining_nanoseconds(&mut self, budget: Resource<V3BudgetResource>) -> u64 {
        self.table
            .get(&budget)
            .expect("canonical resource handles cannot reference a missing v3 budget")
            .remaining_nanoseconds()
    }

    fn drop(&mut self, budget: Resource<V3BudgetResource>) -> wasmtime::Result<()> {
        self.table.delete(budget)?;
        Ok(())
    }
}

impl HostRoot for WasiHostState {
    fn generation(&mut self, root: Resource<V3RootResource>) -> String {
        self.table
            .get(&root)
            .expect("canonical resource handles cannot reference a missing v3 root")
            .0
            .generation
            .to_string()
    }

    fn file_len(&mut self, root: Resource<V3RootResource>) -> u64 {
        self.table
            .get(&root)
            .expect("canonical resource handles cannot reference a missing v3 root")
            .0
            .bytes
            .len()
    }

    fn read_file(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ArenaError> {
        self.charge_budget(&budget, length as usize, ArenaReadKind::File)?;
        self.table
            .get(&root)
            .map_err(unavailable)?
            .0
            .bytes
            .read(offset, u64::from(length))
            .map_err(arena_error)
    }

    fn get_entity(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ArenaError> {
        let value = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .entities
            .get(&key)
            .map_err(arena_error)?;
        self.charge_budget(
            &budget,
            keyed_value_bytes(&key, value.as_deref())?,
            ArenaReadKind::Entity,
        )?;
        Ok(value)
    }

    fn scan_entities(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_bytes: u32,
    ) -> Result<WitKeyedPage, ArenaError> {
        let page = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .entities
            .scan(after_key.as_deref(), max_bytes as usize)
            .map_err(arena_error)?;
        self.charge_page(&budget, page, ArenaReadKind::Entity)
    }

    fn get_state(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ArenaError> {
        let value = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .state
            .get(&key)
            .map_err(arena_error)?;
        self.charge_budget(
            &budget,
            keyed_value_bytes(&key, value.as_deref())?,
            ArenaReadKind::State,
        )?;
        Ok(value)
    }

    fn scan_state(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_bytes: u32,
    ) -> Result<WitKeyedPage, ArenaError> {
        let page = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .state
            .scan(after_key.as_deref(), max_bytes as usize)
            .map_err(arena_error)?;
        self.charge_page(&budget, page, ArenaReadKind::State)
    }

    fn drop(&mut self, root: Resource<V3RootResource>) -> wasmtime::Result<()> {
        self.table.delete(root)?;
        Ok(())
    }
}

impl HostTransaction for WasiHostState {
    fn file_len(
        &mut self,
        transaction: Resource<V3TransactionResource>,
    ) -> Result<u64, ArenaError> {
        self.transaction(&transaction)?
            .file_len()
            .map_err(arena_error)
    }

    fn read_file(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ArenaError> {
        self.charge_budget(&budget, length as usize, ArenaReadKind::File)?;
        self.transaction(&transaction)?
            .read_file(offset, u64::from(length))
            .map_err(arena_error)
    }

    fn get_entity(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ArenaError> {
        let value = self
            .transaction(&transaction)?
            .get_entity(&key)
            .map_err(arena_error)?;
        self.charge_budget(
            &budget,
            keyed_value_bytes(&key, value.as_deref())?,
            ArenaReadKind::Entity,
        )?;
        Ok(value)
    }

    fn scan_entities(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_bytes: u32,
    ) -> Result<WitKeyedPage, ArenaError> {
        let page = self
            .transaction(&transaction)?
            .scan_entities(after_key.as_deref(), max_bytes as usize)
            .map_err(arena_error)?;
        self.charge_page(&budget, page, ArenaReadKind::Entity)
    }

    fn get_state(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ArenaError> {
        let value = self
            .transaction(&transaction)?
            .get_state(&key)
            .map_err(arena_error)?;
        self.charge_budget(
            &budget,
            keyed_value_bytes(&key, value.as_deref())?,
            ArenaReadKind::State,
        )?;
        Ok(value)
    }

    fn scan_state(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_bytes: u32,
    ) -> Result<WitKeyedPage, ArenaError> {
        let page = self
            .transaction(&transaction)?
            .scan_state(after_key.as_deref(), max_bytes as usize)
            .map_err(arena_error)?;
        self.charge_page(&budget, page, ArenaReadKind::State)
    }

    fn put_state(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), ArenaError> {
        self.charge_budget(
            &budget,
            keyed_value_bytes(&key, Some(&value))?,
            ArenaReadKind::State,
        )?;
        self.transaction_mut(&transaction)?.put_state(key, value);
        Ok(())
    }

    fn delete_state(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        key: Vec<u8>,
    ) -> Result<(), ArenaError> {
        self.charge_budget(&budget, key.len(), ArenaReadKind::State)?;
        self.transaction_mut(&transaction)?.delete_state(key);
        Ok(())
    }

    fn drop(&mut self, transaction: Resource<V3TransactionResource>) -> wasmtime::Result<()> {
        self.table.delete(transaction)?;
        Ok(())
    }
}

impl bindings::lix::plugin::arena::Host for WasiHostState {}

impl WasiHostState {
    fn charge_budget(
        &mut self,
        budget: &Resource<V3BudgetResource>,
        bytes: usize,
        kind: ArenaReadKind,
    ) -> Result<(), ArenaError> {
        self.table
            .get_mut(budget)
            .map_err(unavailable)?
            .charge(bytes, kind)
    }

    fn transaction(
        &self,
        transaction: &Resource<V3TransactionResource>,
    ) -> Result<&Transaction, ArenaError> {
        self.table
            .get(transaction)
            .map_err(unavailable)?
            .0
            .as_ref()
            .ok_or_else(|| ArenaError::Unavailable("transaction was already committed".to_owned()))
    }

    fn transaction_mut(
        &mut self,
        transaction: &Resource<V3TransactionResource>,
    ) -> Result<&mut Transaction, ArenaError> {
        self.table
            .get_mut(transaction)
            .map_err(unavailable)?
            .0
            .as_mut()
            .ok_or_else(|| ArenaError::Unavailable("transaction was already committed".to_owned()))
    }

    fn charge_page(
        &mut self,
        budget: &Resource<V3BudgetResource>,
        page: KeyedPage,
        kind: ArenaReadKind,
    ) -> Result<WitKeyedPage, ArenaError> {
        let bytes = page
            .entries
            .iter()
            .try_fold(0usize, |total, (key, value)| {
                total
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(value.len()))
                    .ok_or(ArenaError::RecordTooLarge(u64::MAX))
            })?;
        self.charge_budget(budget, bytes, kind)?;
        Ok(WitKeyedPage {
            next_key: page.next_key,
            entries: page.entries,
        })
    }
}

fn keyed_value_bytes(key: &[u8], value: Option<&[u8]>) -> Result<usize, ArenaError> {
    key.len()
        .checked_add(value.map_or(0, <[u8]>::len))
        .ok_or(ArenaError::RecordTooLarge(u64::MAX))
}

fn arena_error(error: ArenaStoreError) -> ArenaError {
    match error {
        ArenaStoreError::RangeOutOfBounds
        | ArenaStoreError::RangeOverflow
        | ArenaStoreError::InvalidPageRange
        | ArenaStoreError::InvalidEdits => ArenaError::InvalidRange,
        ArenaStoreError::LimitExceeded => {
            ArenaError::LimitExceeded("arena page limit exceeded".to_owned())
        }
        ArenaStoreError::CorruptArchive
        | ArenaStoreError::DifferentStores
        | ArenaStoreError::MissingPage(_) => ArenaError::Unavailable(error.to_string()),
    }
}

fn unavailable(error: impl fmt::Display) -> ArenaError {
    ArenaError::Unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use lix_engine::wasm::WasmRuntime;
    use lix_engine::wasm::v3::{
        ByteEdit, Store as ArenaStore, WasmV3CreateContext, WasmV3FileDescriptor, WasmV3FileUpdate,
        WasmV3InputBytes, WasmV3InputSplice, WasmV3OpenFileInput, WasmV3TransitionLimits,
        entity_arena_key,
    };

    use super::*;

    const FIXTURE_BYTES: usize = 10 * 1024 * 1024;
    const V2_JSON_TOTAL_BYTES: usize = 37_158_912;
    const V3_TOTAL_BYTES_TARGET: usize = V2_JSON_TOTAL_BYTES / 3;

    #[tokio::test]
    async fn real_component_reads_only_the_affected_successor_page_and_commits_atomically() {
        let Some(wasm_path) =
            option_env!("CARGO_CDYLIB_FILE_PLUGIN_ARENA_FIXTURE_V3_plugin_arena_fixture_v3")
        else {
            panic!("the v3 arena fixture artifact dependency must be available");
        };
        let wasm = std::fs::read(wasm_path).expect("v3 fixture component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 8 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("v3 fixture should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("v3 fixture should instantiate");

        let bytes = unique_fixture();
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "fixture-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = descriptor();
        let limits = transition_limits();
        store.reset_metrics();
        let opened = actor
            .open_file(
                limits,
                WasmV3OpenFileInput {
                    descriptor: descriptor.clone(),
                    accepted: imported.clone(),
                    successor: imported.transaction(),
                    creates: creates(),
                },
            )
            .await
            .expect("v3 cold open should start");
        let changes = actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .expect("cold changes should drain")
            .expect("cold open should emit one entity");
        assert_eq!(changes.len(), 1);
        assert!(
            actor
                .next_change_page(opened.transition, opened.changes, 64 * 1024)
                .await
                .expect("cold cursor should reach EOF")
                .is_none()
        );
        let (accepted, cold_counters) = actor
            .finish_transition(opened.transition)
            .await
            .expect("cold transition should commit");
        assert_eq!(store.metrics().page_bytes_read, FIXTURE_BYTES as u64);
        assert_eq!(cold_counters.file_page_reads, 160);
        assert_eq!(cold_counters.file_page_bytes, FIXTURE_BYTES as u64);
        assert!(cold_counters.guest_linear_memory_high_water_bytes <= 8 * 1024 * 1024);

        let offset = FIXTURE_BYTES / 2;
        let mut transaction = accepted.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 1,
            insert: vec![b'X'],
        });
        store.reset_metrics();
        let updated = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor,
                    before: accepted.clone(),
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 1,
                        insert: WasmV3InputBytes::Inline(vec![b'X']),
                    }],
                    successor: transaction,
                    creates: creates(),
                },
            )
            .await
            .expect("v3 warm edit should start");
        let changes = actor
            .next_change_page(updated.transition, updated.changes, 64 * 1024)
            .await
            .expect("warm changes should drain")
            .expect("warm edit should emit one entity");
        assert_eq!(changes.len(), 1);
        assert!(
            actor
                .next_change_page(updated.transition, updated.changes, 64 * 1024)
                .await
                .expect("warm cursor should reach EOF")
                .is_none()
        );
        let (successor, warm_counters) = actor
            .finish_transition(updated.transition)
            .await
            .expect("warm transition should commit");

        assert_eq!(
            store.metrics().page_bytes_read,
            1,
            "the guest should materialize only the affected prospective byte"
        );
        assert_eq!(warm_counters.file_page_reads, 1);
        assert_eq!(warm_counters.file_page_bytes, 1);
        assert!(warm_counters.component_boundary_bytes < 1024);
        assert!(warm_counters.guest_linear_memory_high_water_bytes <= 8 * 1024 * 1024);
        eprintln!(
            "v3_component_arena_fixture bytes={FIXTURE_BYTES} host_unique_bytes={} \
             warm_guest_high_water_bytes={} warm_total_owned_bytes={} warm_boundary_bytes={} \
             warm_file_page_reads={} warm_file_page_bytes={}",
            store.unique_page_bytes(),
            warm_counters.guest_linear_memory_high_water_bytes,
            store
                .unique_page_bytes()
                .saturating_add(warm_counters.guest_linear_memory_high_water_bytes as usize),
            warm_counters.component_boundary_bytes,
            warm_counters.file_page_reads,
            warm_counters.file_page_bytes,
        );
        assert_eq!(successor.bytes.read(offset as u64, 1).unwrap(), b"X");
        assert_eq!(
            successor.state.get(b"fixture/length").unwrap(),
            Some((FIXTURE_BYTES as u64).to_le_bytes().to_vec())
        );
        let key = entity_arena_key("fixture", &["root".to_owned()]).unwrap();
        assert_eq!(
            successor.entities.get(&key).unwrap(),
            Some(format!("{{\"length\":{FIXTURE_BYTES}}}").into_bytes())
        );
        assert_eq!(
            accepted.bytes.read(offset as u64, 1).unwrap(),
            vec![bytes[offset]],
            "the accepted base must remain immutable"
        );
    }

    #[tokio::test]
    #[ignore = "manual release-mode v3 Component latency scorecard"]
    async fn v3_component_ten_mib_sparse_edit_benchmark() {
        const WARMUPS: usize = 100;
        const SAMPLES: usize = 2_000;

        let Some(wasm_path) =
            option_env!("CARGO_CDYLIB_FILE_PLUGIN_ARENA_FIXTURE_V3_plugin_arena_fixture_v3")
        else {
            panic!("the v3 arena fixture artifact dependency must be available");
        };
        let wasm = std::fs::read(wasm_path).expect("v3 fixture component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 8 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("v3 fixture should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("v3 fixture should instantiate");
        let bytes = unique_fixture();
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "fixture-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = descriptor();
        let limits = transition_limits();
        let opened = actor
            .open_file(
                limits,
                WasmV3OpenFileInput {
                    descriptor: descriptor.clone(),
                    accepted: imported.clone(),
                    successor: imported.transaction(),
                    creates: creates(),
                },
            )
            .await
            .unwrap();
        assert!(
            actor
                .next_change_page(opened.transition, opened.changes, 64 * 1024)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            actor
                .next_change_page(opened.transition, opened.changes, 64 * 1024)
                .await
                .unwrap()
                .is_none()
        );
        let (mut accepted, _) = actor.finish_transition(opened.transition).await.unwrap();

        let mut samples = Vec::with_capacity(SAMPLES);
        let mut replacement = b'X';
        let mut last_counters = WasmV3TransitionCounters::default();
        for sample in 0..WARMUPS + SAMPLES {
            let offset = FIXTURE_BYTES / 2;
            let mut transaction = accepted.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: offset as u64,
                delete_len: 1,
                insert: vec![replacement],
            });
            let started = Instant::now();
            let updated = actor
                .file_changed(
                    limits,
                    WasmV3FileUpdate {
                        before_descriptor: descriptor.clone(),
                        after_descriptor: descriptor.clone(),
                        before: accepted.clone(),
                        edits: vec![WasmV3InputSplice {
                            offset: offset as u64,
                            delete_len: 1,
                            insert: WasmV3InputBytes::Inline(vec![replacement]),
                        }],
                        successor: transaction,
                        creates: creates(),
                    },
                )
                .await
                .unwrap();
            assert!(
                actor
                    .next_change_page(updated.transition, updated.changes, 64 * 1024)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                actor
                    .next_change_page(updated.transition, updated.changes, 64 * 1024)
                    .await
                    .unwrap()
                    .is_none()
            );
            (accepted, last_counters) = actor.finish_transition(updated.transition).await.unwrap();
            if sample >= WARMUPS {
                samples.push(
                    u64::try_from(started.elapsed().as_nanos())
                        .expect("benchmark sample duration should fit u64"),
                );
            }
            replacement = if replacement == b'X' { b'Y' } else { b'X' };
        }

        samples.sort_unstable();
        let p50_ns = percentile(&samples, 50);
        let p95_ns = percentile(&samples, 95);
        let total_owned_bytes = store
            .unique_page_bytes()
            .saturating_add(last_counters.guest_linear_memory_high_water_bytes as usize);
        eprintln!(
            "v3_component_sparse_edit bytes={FIXTURE_BYTES} warmups={WARMUPS} samples={SAMPLES} \
             p50_us={:.3} p95_us={:.3} host_unique_bytes={} guest_high_water_bytes={} \
             total_owned_bytes={} memory_reduction_vs_v2_json={:.3} boundary_bytes={} \
             file_page_reads={} file_page_bytes={}",
            p50_ns as f64 / 1_000.0,
            p95_ns as f64 / 1_000.0,
            store.unique_page_bytes(),
            last_counters.guest_linear_memory_high_water_bytes,
            total_owned_bytes,
            V2_JSON_TOTAL_BYTES as f64 / total_owned_bytes as f64,
            last_counters.component_boundary_bytes,
            last_counters.file_page_reads,
            last_counters.file_page_bytes,
        );
        assert!(p95_ns <= 2_750_000, "runtime control must clear 2x latency");
        assert!(
            total_owned_bytes <= V3_TOTAL_BYTES_TARGET,
            "runtime control must clear 3x total memory"
        );
        assert_eq!(last_counters.file_page_bytes, 1);
    }

    fn percentile(samples: &[u64], percentile: usize) -> u64 {
        let rank = (samples.len() * percentile).div_ceil(100);
        samples[rank.saturating_sub(1)]
    }

    fn unique_fixture() -> Vec<u8> {
        (0..FIXTURE_BYTES)
            .map(|index| ((index ^ (index >> 8) ^ (index >> 16)) & 0xff) as u8)
            .collect()
    }

    fn descriptor() -> WasmV3FileDescriptor {
        WasmV3FileDescriptor {
            path: Some("/fixture.json".to_owned()),
            media_type: Some("application/json".to_owned()),
            plugin_key: "plugin_arena_fixture_v3".to_owned(),
            generation: "fixture-generation".to_owned(),
        }
    }

    fn creates() -> WasmV3CreateContext {
        WasmV3CreateContext {
            high: 0x0190_0000_0000_7000,
            low: 0x8000_0000_0000_0000,
        }
    }

    fn transition_limits() -> WasmV3TransitionLimits {
        WasmV3TransitionLimits {
            max_page_bytes: 64 * 1024,
            max_pages: 256,
            max_total_bytes: 16 * 1024 * 1024,
            deadline_nanoseconds: 10_000_000_000,
        }
    }
}
