//! Wasmtime Component bindings for Plugin API v3.
//!
//! This starts with the generated hard-cut contract. Host resource
//! implementations live here rather than in the engine-neutral crate so
//! content-addressed arena roots never acquire Wasmtime lifetimes.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use lix_engine::LixError;
use lix_engine::wasm::WasmChangeEffect;
use lix_engine::wasm::v3::{
    Error as ArenaStoreError, KeyedPage, Root, SemanticPageBatch, Transaction,
    WasmComponentV3Actor, WasmComponentV3Factory, WasmV3ByteEdit, WasmV3ChangeCursorHandle,
    WasmV3ChangedEntity, WasmV3ConflictChoice, WasmV3ConflictResolution, WasmV3ConflictTransition,
    WasmV3ConflictUpdate, WasmV3CreateContext, WasmV3EditCursorHandle, WasmV3EntityChange,
    WasmV3EntityConflict, WasmV3EntityTransition, WasmV3EntityUpdate, WasmV3FileDescriptor,
    WasmV3FileTransition, WasmV3FileUpdate, WasmV3InputBytes, WasmV3OpenEntitiesInput,
    WasmV3OpenFileInput, WasmV3ResolutionCursorHandle, WasmV3TransitionCounters,
    WasmV3TransitionHandle, WasmV3TransitionLimits,
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
            ArenaReadKind::Boundary => {}
        }
        self.counters.component_boundary_bytes =
            self.counters.component_boundary_bytes.saturating_add(bytes);
        Ok(())
    }
}

pub struct V3RootResource(pub(super) Root);
pub struct V3TransactionResource(pub(super) Option<Transaction>);
pub struct V3ConflictSetResource(pub(super) Vec<WasmV3EntityConflict>);

#[derive(Clone, Copy)]
enum ArenaReadKind {
    File,
    Entity,
    State,
    Boundary,
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
    transaction_rep: Option<u32>,
    cursor_handle: u64,
    cursor_kind: CursorKind,
    eof: bool,
    seen_entity_keys: BTreeSet<Vec<u8>>,
    last_ordered_entity_key: Option<Vec<u8>>,
    previous_edit_end: u64,
    buffered_change_packet: Option<Vec<u8>>,
    buffered_edits: Option<Vec<bindings::exports::lix::plugin::api::ByteEdit>>,
    buffered_resolutions: Option<Vec<bindings::exports::lix::plugin::api::ConflictResolution>>,
    has_guest_cursor: bool,
    guest_change_cursor_nanoseconds: u64,
    change_packet_decode_nanoseconds: u64,
    ordered_entity_stage_nanoseconds: u64,
    change_output_nanoseconds: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CursorKind {
    Changes,
    Edits,
    Resolutions,
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
            "lix:plugin/arena.conflict-set": super::V3ConflictSetResource,
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

    fn push_conflicts(
        &mut self,
        limits: WasmV3TransitionLimits,
        conflicts: Vec<WasmV3EntityConflict>,
    ) -> Result<(Resource<V3BudgetResource>, Resource<V3ConflictSetResource>), LixError> {
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
            .map_err(|error| wasm_runtime_error("failed to allocate v3.2 budget", error))?;
        let conflicts = store
            .data_mut()
            .table
            .push(V3ConflictSetResource(conflicts))
            .map_err(|error| wasm_runtime_error("failed to allocate v3.2 conflict set", error))?;
        Ok((budget, conflicts))
    }

    fn register_file_transition(
        &mut self,
        budget_rep: u32,
        value: bindings::exports::lix::plugin::api::FileTransition,
    ) -> Result<WasmV3FileTransition, LixError> {
        let transaction = value.successor;
        let cursor_handle = self.allocate_handle()?;
        let has_guest_cursor = value.changes.is_some();
        if let Some(cursor) = value.changes {
            self.cursors.insert(cursor_handle, cursor);
        }
        let transition_handle = self.allocate_handle()?;
        self.transitions.insert(
            transition_handle,
            ActiveTransition {
                budget_rep,
                transaction_rep: Some(transaction.rep()),
                cursor_handle,
                cursor_kind: CursorKind::Changes,
                eof: false,
                seen_entity_keys: BTreeSet::new(),
                last_ordered_entity_key: None,
                previous_edit_end: 0,
                buffered_change_packet: (!value.first_change_packet.is_empty())
                    .then_some(value.first_change_packet),
                buffered_edits: None,
                buffered_resolutions: None,
                has_guest_cursor,
                guest_change_cursor_nanoseconds: 0,
                change_packet_decode_nanoseconds: 0,
                ordered_entity_stage_nanoseconds: 0,
                change_output_nanoseconds: 0,
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
        let has_guest_cursor = value.edits.is_some();
        if let Some(cursor) = value.edits {
            self.cursors.insert(cursor_handle, cursor);
        }
        let transition_handle = self.allocate_handle()?;
        self.transitions.insert(
            transition_handle,
            ActiveTransition {
                budget_rep,
                transaction_rep: Some(transaction.rep()),
                cursor_handle,
                cursor_kind: CursorKind::Edits,
                eof: false,
                seen_entity_keys: BTreeSet::new(),
                last_ordered_entity_key: None,
                previous_edit_end: 0,
                buffered_change_packet: None,
                buffered_edits: (!value.first_edits.is_empty()).then_some(value.first_edits),
                buffered_resolutions: None,
                has_guest_cursor,
                guest_change_cursor_nanoseconds: 0,
                change_packet_decode_nanoseconds: 0,
                ordered_entity_stage_nanoseconds: 0,
                change_output_nanoseconds: 0,
            },
        );
        Ok(WasmV3EntityTransition {
            transition: WasmV3TransitionHandle(transition_handle),
            edits: WasmV3EditCursorHandle(cursor_handle),
        })
    }

    fn register_conflict_transition(
        &mut self,
        budget_rep: u32,
        value: bindings::exports::lix::plugin::api::ConflictTransition,
    ) -> Result<WasmV3ConflictTransition, LixError> {
        let cursor_handle = self.allocate_handle()?;
        let has_guest_cursor = value.resolutions.is_some();
        if let Some(cursor) = value.resolutions {
            self.cursors.insert(cursor_handle, cursor);
        }
        let transition_handle = self.allocate_handle()?;
        self.transitions.insert(
            transition_handle,
            ActiveTransition {
                budget_rep,
                transaction_rep: None,
                cursor_handle,
                cursor_kind: CursorKind::Resolutions,
                eof: false,
                seen_entity_keys: BTreeSet::new(),
                last_ordered_entity_key: None,
                previous_edit_end: 0,
                buffered_change_packet: None,
                buffered_edits: None,
                buffered_resolutions: (!value.first_resolutions.is_empty())
                    .then_some(value.first_resolutions),
                has_guest_cursor,
                guest_change_cursor_nanoseconds: 0,
                change_packet_decode_nanoseconds: 0,
                ordered_entity_stage_nanoseconds: 0,
                change_output_nanoseconds: 0,
            },
        );
        Ok(WasmV3ConflictTransition {
            transition: WasmV3TransitionHandle(transition_handle),
            resolutions: WasmV3ResolutionCursorHandle(cursor_handle),
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

fn changed_entity_to_binding(
    changed: WasmV3ChangedEntity,
) -> bindings::exports::lix::plugin::api::ChangedEntity {
    bindings::exports::lix::plugin::api::ChangedEntity {
        key: changed.key,
        format_only: changed.format_only,
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
            changed_entities: input
                .changed_entities
                .into_iter()
                .map(changed_entity_to_binding)
                .collect(),
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

    async fn resolve_conflicts(
        &mut self,
        limits: WasmV3TransitionLimits,
        input: WasmV3ConflictUpdate,
    ) -> Result<WasmV3ConflictTransition, LixError> {
        let limits = limits.validate()?;
        self.prepare_call(limits)?;
        let (budget, conflicts) = self.push_conflicts(limits, input.conflicts)?;
        let budget_rep = budget.rep();
        let binding_input = bindings::exports::lix::plugin::api::ConflictUpdate {
            descriptor: descriptor_to_binding(input.descriptor),
            conflicts,
        };
        let guest = self.guest.clone();
        let result = call_sync_guest(|| {
            guest.call_resolve_conflicts(
                self.store_mut()?,
                Resource::new_borrow(budget_rep),
                &binding_input,
            )
        });
        let value = self.resolve_top_level_call("v3.2 resolve-conflicts", budget_rep, result)?;
        self.register_conflict_transition(budget_rep, value)
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
        let packet = if let Some(packet) = active.buffered_change_packet.take() {
            Some(packet)
        } else if !active.has_guest_cursor {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        } else {
            let Some(cursor_resource) = self.cursors.get(&cursor.0).copied() else {
                return self.fail_active(active, v3_invalid_plugin("unknown v3 change cursor"));
            };
            if let Err(error) = self.prepare_nested_call(&active) {
                return self.fail_active(active, error);
            }
            let guest = self.guest.clone();
            let guest_started = Instant::now();
            let result = call_sync_guest(|| {
                guest.change_cursor().call_next(
                    self.store_mut()?,
                    cursor_resource,
                    Resource::new_borrow(active.budget_rep),
                    max_bytes,
                )
            });
            active.guest_change_cursor_nanoseconds = active
                .guest_change_cursor_nanoseconds
                .saturating_add(guest_started.elapsed().as_nanos() as u64);
            match result {
                Ok(Ok(page)) => page,
                Ok(Err(error)) => {
                    let error = Self::plugin_error("v3 change-cursor.next", error);
                    return self.fail_active(active, error);
                }
                Err(error) => {
                    self.retire();
                    return Err(wasm_runtime_error("v3 change cursor trapped", error));
                }
            }
        };
        let Some(packet) = packet else {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        };
        if packet.is_empty() {
            return self.fail_active(
                active,
                v3_invalid_plugin("v3 change cursor returned an empty non-EOF page"),
            );
        }
        if let Err(error) =
            self.charge_transition_output(active.budget_rep, packet.len() as u64, max_bytes)
        {
            return self.fail_active(active, error);
        }
        let packet = match normalize_change_packet(packet, max_bytes) {
            Ok(packet) => packet,
            Err(error) => return self.fail_active(active, error),
        };

        let decode_started = Instant::now();
        let validated = match decode_change_packet(&packet) {
            Ok(changes) => changes,
            Err(error) => return self.fail_active(active, error),
        };
        active.change_packet_decode_nanoseconds = active
            .change_packet_decode_nanoseconds
            .saturating_add(decode_started.elapsed().as_nanos() as u64);
        let ordered = match self.transaction_by_rep_mut(active.transaction_rep) {
            Ok(transaction) => transaction.has_ordered_entity_output(),
            Err(error) => return self.fail_active(active, error),
        };
        if ordered {
            let first_is_valid = validated.first().is_some_and(|(first, _, _)| {
                active
                    .last_ordered_entity_key
                    .as_deref()
                    .is_none_or(|previous| previous < first.as_slice())
            });
            let page_is_valid = validated
                .windows(2)
                .all(|pair| pair[0].0.as_slice() < pair[1].0.as_slice());
            if !first_is_valid || !page_is_valid {
                return self.fail_active(
                    active,
                    v3_invalid_plugin("v3 ordered change cursor keys are not strictly increasing"),
                );
            }
            active.last_ordered_entity_key = validated.last().map(|(key, _, _)| key.clone());
        } else {
            for (key, _, _) in &validated {
                if !active.seen_entity_keys.insert(key.clone()) {
                    return self.fail_active(
                        active,
                        v3_invalid_plugin("v3 change cursor repeated an entity key"),
                    );
                }
            }
        }
        if ordered {
            let ordered_ranges = validated
                .iter()
                .map(|(key, change, snapshot_range)| {
                    change.snapshot.as_ref().ok_or_else(|| {
                        v3_invalid_plugin("ordered v3 entity output cannot contain deletions")
                    })?;
                    snapshot_range
                        .clone()
                        .map(|range| (key.clone(), range))
                        .ok_or_else(|| {
                            v3_invalid_plugin("ordered v3 entity output cannot contain deletions")
                        })
                })
                .collect::<Result<Vec<_>, _>>();
            let ordered_ranges = match ordered_ranges {
                Ok(ranges) => ranges,
                Err(error) => return self.fail_active(active, error),
            };
            let stage_started = Instant::now();
            let stage_result = self
                .transaction_by_rep_mut(active.transaction_rep)
                .and_then(|transaction| {
                    transaction
                        .stream_ordered_entity_packet(packet, ordered_ranges)
                        .map_err(|error| {
                            v3_invalid_plugin(format!(
                                "ordered v3 entity output is invalid: {error}"
                            ))
                        })
                });
            if let Err(error) = stage_result {
                return self.fail_active(active, error);
            }
            active.ordered_entity_stage_nanoseconds = active
                .ordered_entity_stage_nanoseconds
                .saturating_add(stage_started.elapsed().as_nanos() as u64);
        }
        let output_started = Instant::now();
        let mut output = Vec::with_capacity(validated.len());
        for (key, change, _) in validated {
            if !ordered {
                let transaction = self.transaction_by_rep_mut(active.transaction_rep);
                let transaction = match transaction {
                    Ok(transaction) => transaction,
                    Err(error) => return self.fail_active(active, error),
                };
                match &change.snapshot {
                    Some(snapshot) => transaction.stream_upsert_entity(key, snapshot.clone()),
                    None => transaction.stream_delete_entity(key),
                }
            }
            output.push(WasmV3EntityChange {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
                snapshot: change.snapshot,
                format_only: change.format_only,
            });
        }
        active.change_output_nanoseconds = active
            .change_output_nanoseconds
            .saturating_add(output_started.elapsed().as_nanos() as u64);
        if !active.has_guest_cursor {
            active.eof = true;
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
        let page = if let Some(page) = active.buffered_edits.take() {
            Some(page)
        } else if !active.has_guest_cursor {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        } else {
            let Some(cursor_resource) = self.cursors.get(&cursor.0).copied() else {
                return self.fail_active(active, v3_invalid_plugin("unknown v3 edit cursor"));
            };
            if let Err(error) = self.prepare_nested_call(&active) {
                return self.fail_active(active, error);
            }
            let guest = self.guest.clone();
            let result = call_sync_guest(|| {
                guest.edit_cursor().call_next(
                    self.store_mut()?,
                    cursor_resource,
                    Resource::new_borrow(active.budget_rep),
                    max_bytes,
                )
            });
            match result {
                Ok(Ok(page)) => page,
                Ok(Err(error)) => {
                    let error = Self::plugin_error("v3 edit-cursor.next", error);
                    return self.fail_active(active, error);
                }
                Err(error) => {
                    self.retire();
                    return Err(wasm_runtime_error("v3 edit cursor trapped", error));
                }
            }
        };
        let Some(page) = page else {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        };
        if page.is_empty() {
            return self.fail_active(
                active,
                v3_invalid_plugin("v3 edit cursor returned an empty non-EOF page"),
            );
        }

        let mut validated = Vec::with_capacity(page.len());
        let mut page_bytes = 0_u64;
        for edit in page {
            let Some(end) = edit.offset.checked_add(edit.delete_len) else {
                return self
                    .fail_active(active, v3_invalid_plugin("v3 byte edit range overflowed"));
            };
            if edit.offset < active.previous_edit_end {
                return self.fail_active(
                    active,
                    v3_invalid_plugin("v3 byte edits are not globally base-relative and ordered"),
                );
            }
            page_bytes = match page_bytes
                .checked_add(24_u64.saturating_add(edit.insert.len() as u64))
            {
                Some(bytes) => bytes,
                None => {
                    return self
                        .fail_active(active, v3_invalid_plugin("v3 edit page size overflowed"));
                }
            };
            active.previous_edit_end = end;
            validated.push(edit);
        }
        if let Err(error) = self.charge_transition_output(active.budget_rep, page_bytes, max_bytes)
        {
            return self.fail_active(active, error);
        }

        let mut output = Vec::with_capacity(validated.len());
        for edit in validated {
            let stage_result = self.transaction_by_rep_mut(active.transaction_rep);
            let transaction = match stage_result {
                Ok(transaction) => transaction,
                Err(error) => return self.fail_active(active, error),
            };
            transaction.edit_bytes(lix_engine::wasm::v3::ByteEdit {
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
        if !active.has_guest_cursor {
            active.eof = true;
        }
        self.transitions.insert(transition.0, active);
        Ok(Some(output))
    }

    async fn next_resolution_page(
        &mut self,
        transition: WasmV3TransitionHandle,
        cursor: WasmV3ResolutionCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmV3ConflictResolution>>, LixError> {
        let mut active = self.take_active(transition, cursor.0, CursorKind::Resolutions)?;
        if active.eof {
            self.transitions.insert(transition.0, active);
            return Ok(None);
        }
        let page = if let Some(page) = active.buffered_resolutions.take() {
            Some(page)
        } else if !active.has_guest_cursor {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        } else {
            let Some(cursor_resource) = self.cursors.get(&cursor.0).copied() else {
                return self
                    .fail_active(active, v3_invalid_plugin("unknown v3.2 resolution cursor"));
            };
            if let Err(error) = self.prepare_nested_call(&active) {
                return self.fail_active(active, error);
            }
            let guest = self.guest.clone();
            let result = call_sync_guest(|| {
                guest.resolution_cursor().call_next(
                    self.store_mut()?,
                    cursor_resource,
                    Resource::new_borrow(active.budget_rep),
                    max_bytes,
                )
            });
            match result {
                Ok(Ok(page)) => page,
                Ok(Err(error)) => {
                    let error = Self::plugin_error("v3.2 resolution-cursor.next", error);
                    return self.fail_active(active, error);
                }
                Err(error) => {
                    self.retire();
                    return Err(wasm_runtime_error("v3.2 resolution cursor trapped", error));
                }
            }
        };
        let Some(page) = page else {
            active.eof = true;
            self.transitions.insert(transition.0, active);
            return Ok(None);
        };
        if page.is_empty() {
            return self.fail_active(
                active,
                v3_invalid_plugin("v3.2 resolution cursor returned an empty non-EOF page"),
            );
        }
        let mut page_bytes = 0_u64;
        let mut output = Vec::with_capacity(page.len());
        for resolution in page {
            if resolution.ordinal != active.previous_edit_end {
                return self.fail_active(
                    active,
                    v3_invalid_plugin(
                        "v3.2 conflict resolutions are not complete and ordinal ordered",
                    ),
                );
            }
            active.previous_edit_end = active.previous_edit_end.saturating_add(1);
            let choice = match resolution.choice {
                bindings::exports::lix::plugin::api::ConflictChoice::TakeBase => {
                    WasmV3ConflictChoice::TakeBase
                }
                bindings::exports::lix::plugin::api::ConflictChoice::TakeA => {
                    WasmV3ConflictChoice::TakeA
                }
                bindings::exports::lix::plugin::api::ConflictChoice::TakeB => {
                    WasmV3ConflictChoice::TakeB
                }
                bindings::exports::lix::plugin::api::ConflictChoice::Replace(replacement) => {
                    page_bytes = page_bytes.saturating_add(replacement.snapshot.len() as u64);
                    WasmV3ConflictChoice::Replace {
                        snapshot: replacement.snapshot,
                        effect: if replacement.format_only {
                            WasmChangeEffect::FormatOnly
                        } else {
                            WasmChangeEffect::Content
                        },
                    }
                }
                bindings::exports::lix::plugin::api::ConflictChoice::Delete => {
                    WasmV3ConflictChoice::Delete
                }
            };
            page_bytes = page_bytes.saturating_add(16);
            output.push(WasmV3ConflictResolution {
                ordinal: resolution.ordinal,
                choice,
            });
        }
        if let Err(error) = self.charge_transition_output(active.budget_rep, page_bytes, max_bytes)
        {
            return self.fail_active(active, error);
        }
        if !active.has_guest_cursor {
            active.eof = true;
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
            return self.fail_active(
                active,
                v3_invalid_plugin("v3 transition cannot commit before cursor EOF"),
            );
        }
        self.drop_cursor(active.cursor_handle)?;
        let guest_high_water = self
            .store_mut()?
            .data()
            .limits
            .linear_memory_high_water_bytes();
        let mut counters = self.budget_counters(active.budget_rep)?;
        counters.guest_linear_memory_high_water_bytes = guest_high_water;
        counters.guest_change_cursor_nanoseconds = active.guest_change_cursor_nanoseconds;
        counters.change_packet_decode_nanoseconds = active.change_packet_decode_nanoseconds;
        counters.ordered_entity_stage_nanoseconds = active.ordered_entity_stage_nanoseconds;
        counters.change_output_nanoseconds = active.change_output_nanoseconds;
        let transaction_rep = active.transaction_rep.ok_or_else(|| {
            v3_invalid_plugin("v3.2 conflict transition cannot publish an arena root")
        })?;
        let transaction = self.take_transaction(transaction_rep)?;
        self.drop_budget(active.budget_rep)?;
        let root = transaction
            .commit()
            .map_err(|error| v3_invalid_plugin(format!("v3 commit failed: {error}")))?;
        Ok((root, counters))
    }

    async fn finish_conflict_transition(
        &mut self,
        transition: WasmV3TransitionHandle,
    ) -> Result<WasmV3TransitionCounters, LixError> {
        let active = self
            .transitions
            .remove(&transition.0)
            .ok_or_else(|| v3_invalid_plugin("unknown v3.2 conflict transition"))?;
        if active.transaction_rep.is_some() {
            return self.fail_active(
                active,
                v3_invalid_plugin("arena transition requires finish-transition"),
            );
        }
        if !active.eof {
            return self.fail_active(
                active,
                v3_invalid_plugin("conflict transition cannot finish before cursor EOF"),
            );
        }
        self.drop_cursor(active.cursor_handle)?;
        let guest_high_water = self
            .store_mut()?
            .data()
            .limits
            .linear_memory_high_water_bytes();
        let mut counters = self.budget_counters(active.budget_rep)?;
        counters.guest_linear_memory_high_water_bytes = guest_high_water;
        self.drop_budget(active.budget_rep)?;
        Ok(counters)
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

    fn transaction_by_rep_mut(&mut self, rep: Option<u32>) -> Result<&mut Transaction, LixError> {
        let rep = rep.ok_or_else(|| {
            v3_invalid_plugin("v3.2 conflict transition has no successor transaction")
        })?;
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

    fn charge_transition_output(
        &mut self,
        budget_rep: u32,
        bytes: u64,
        max_bytes: u32,
    ) -> Result<(), LixError> {
        if bytes > u64::from(max_bytes) {
            return Err(v3_invalid_plugin(format!(
                "v3 guest output page exceeded {max_bytes} bytes"
            )));
        }
        let bytes = usize::try_from(bytes)
            .map_err(|_| v3_invalid_plugin("v3 guest output size exceeds usize"))?;
        self.store_mut()?
            .data_mut()
            .table
            .get_mut(&Resource::<V3BudgetResource>::new_borrow(budget_rep))
            .map_err(|error| wasm_runtime_error("missing v3 output budget", error))?
            .charge(bytes, ArenaReadKind::Boundary)
            .map_err(|error| v3_invalid_plugin(format!("v3 output budget failed: {error:?}")))
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
        if let Some(transaction_rep) = active.transaction_rep {
            self.store_mut()?
                .data_mut()
                .table
                .delete(Resource::<V3TransactionResource>::new_own(transaction_rep))
                .map_err(|error| wasm_runtime_error("failed to roll back v3 transaction", error))?;
        }
        self.drop_budget(active.budget_rep)
    }

    fn fail_active<T>(&mut self, active: ActiveTransition, error: LixError) -> Result<T, LixError> {
        if let Err(abort_error) = self.abort_active(active) {
            self.retire();
            return Err(abort_error);
        }
        Err(error)
    }

    fn retire(&mut self) {
        self.store = None;
        self.cursors.clear();
        self.transitions.clear();
    }
}

const CHANGE_PACKET_MAGIC: &[u8; 4] = b"L3C1";
const COMPRESSED_CHANGE_PACKET_MAGIC: &[u8; 4] = b"L3Z1";

fn normalize_change_packet(packet: Vec<u8>, max_bytes: u32) -> Result<Vec<u8>, LixError> {
    if packet.get(..4) != Some(COMPRESSED_CHANGE_PACKET_MAGIC) {
        return Ok(packet);
    }
    let compressed = packet
        .get(4..)
        .ok_or_else(|| v3_invalid_plugin("truncated v3 compressed change packet"))?;
    let raw_len = compressed
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| v3_invalid_plugin("truncated v3 compressed change packet size"))?;
    if raw_len > max_bytes {
        return Err(v3_invalid_plugin(format!(
            "v3 compressed change packet expands beyond {max_bytes} bytes"
        )));
    }
    lz4_flex::block::decompress_size_prepended(compressed)
        .map_err(|_| v3_invalid_plugin("v3 compressed change packet is corrupt"))
}

fn decode_change_packet(
    packet: &[u8],
) -> Result<
    Vec<(
        Vec<u8>,
        bindings::exports::lix::plugin::api::EntityChange,
        Option<Range<usize>>,
    )>,
    LixError,
> {
    if packet.get(..4) != Some(CHANGE_PACKET_MAGIC) {
        return Err(v3_invalid_plugin("v3 change packet has invalid magic"));
    }
    let mut offset = 4;
    let count = take_packet_u32(packet, &mut offset, "change count")? as usize;
    if count == 0 {
        return Err(v3_invalid_plugin(
            "v3 change packet contains no entity changes",
        ));
    }
    let mut changes = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let key = take_packet_bytes(packet, &mut offset, "entity key")?.to_vec();
        let (schema_key, entity_pk) = decode_entity_arena_key(&key)?;
        let flags = *packet
            .get(offset)
            .ok_or_else(|| v3_invalid_plugin("truncated v3 change packet flags"))?;
        offset += 1;
        if flags & !0b11 != 0 {
            return Err(v3_invalid_plugin("v3 change packet contains unknown flags"));
        }
        let (snapshot, snapshot_range) = if flags & 1 != 0 {
            let length_offset = offset;
            let snapshot = take_packet_bytes(packet, &mut offset, "snapshot")?;
            let start = length_offset
                .checked_add(4)
                .ok_or_else(|| v3_invalid_plugin("v3 snapshot range overflowed"))?;
            (Some(snapshot.to_vec()), Some(start..offset))
        } else {
            (None, None)
        };
        changes.push((
            key,
            bindings::exports::lix::plugin::api::EntityChange {
                schema_key,
                entity_pk,
                snapshot,
                format_only: flags & (1 << 1) != 0,
            },
            snapshot_range,
        ));
    }
    if offset != packet.len() {
        return Err(v3_invalid_plugin(
            "v3 change packet has trailing unclaimed bytes",
        ));
    }
    Ok(changes)
}

fn decode_entity_arena_key(key: &[u8]) -> Result<(String, Vec<String>), LixError> {
    let mut offset = 0;
    let schema_key = take_key_string(key, &mut offset)?;
    let mut entity_pk = Vec::new();
    while offset < key.len() {
        entity_pk.push(take_key_string(key, &mut offset)?);
    }
    if entity_pk.is_empty() {
        return Err(v3_invalid_plugin(
            "v3 entity arena key has no primary-key components",
        ));
    }
    Ok((schema_key, entity_pk))
}

fn take_key_string(key: &[u8], offset: &mut usize) -> Result<String, LixError> {
    let len = take_packet_u32(key, offset, "entity key component length")? as usize;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| v3_invalid_plugin("v3 entity arena key length overflowed"))?;
    let value = key
        .get(*offset..end)
        .ok_or_else(|| v3_invalid_plugin("truncated v3 entity arena key component"))?;
    *offset = end;
    String::from_utf8(value.to_vec())
        .map_err(|_| v3_invalid_plugin("v3 entity arena key component is not UTF-8"))
}

fn take_packet_bytes<'a>(
    packet: &'a [u8],
    offset: &mut usize,
    field: &str,
) -> Result<&'a [u8], LixError> {
    let len = take_packet_u32(packet, offset, field)? as usize;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| v3_invalid_plugin(format!("v3 change packet {field} length overflowed")))?;
    let value = packet
        .get(*offset..end)
        .ok_or_else(|| v3_invalid_plugin(format!("truncated v3 change packet {field}")))?;
    *offset = end;
    Ok(value)
}

fn take_packet_u32(packet: &[u8], offset: &mut usize, field: &str) -> Result<u32, LixError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| v3_invalid_plugin(format!("v3 change packet {field} offset overflowed")))?;
    let value = packet
        .get(*offset..end)
        .ok_or_else(|| v3_invalid_plugin(format!("truncated v3 change packet {field}")))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        value.try_into().expect("u32 slice has exactly four bytes"),
    ))
}

fn call_sync_guest<T>(call: impl FnOnce() -> T) -> T {
    // API v3 components import only the arena interface and cannot invoke
    // WASI's blocking adapter. Calling them directly avoids one OS-thread
    // spawn per boundary while the v2 compatibility runtime retains its
    // broader WASI-safe trampoline.
    call()
}

fn v3_invalid_plugin(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn v3_invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

use bindings::lix::plugin::arena::{
    ArenaError, ByteRecordLocator as WitByteRecordLocator, ConflictPage as WitConflictPage,
    ConflictSide as WitConflictSide, ConflictSummary as WitConflictSummary, HostBudget,
    HostConflictSet, HostRoot, HostTransaction, KeyedPage as WitKeyedPage, Limits,
    SemanticPage as WitSemanticPage, SemanticPageBatch as WitSemanticPageBatch,
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

impl HostConflictSet for WasiHostState {
    fn scan(
        &mut self,
        conflicts: Resource<V3ConflictSetResource>,
        budget: Resource<V3BudgetResource>,
        after_ordinal: Option<u64>,
        max_bytes: u32,
    ) -> Result<WitConflictPage, ArenaError> {
        if max_bytes == 0 {
            return Err(ArenaError::LimitExceeded(
                "conflict page size must be positive".to_owned(),
            ));
        }
        let start = after_ordinal
            .map(|ordinal| ordinal.saturating_add(1))
            .unwrap_or(0);
        let start = usize::try_from(start).map_err(|_| ArenaError::RecordTooLarge(u64::MAX))?;
        let values = &self.table.get(&conflicts).map_err(unavailable)?.0;
        if start > values.len() {
            return Err(ArenaError::InvalidRange);
        }
        let mut bytes = 0usize;
        let mut entries = Vec::new();
        for (index, conflict) in values.iter().enumerate().skip(start) {
            let entry_bytes = conflict
                .key
                .len()
                .checked_add(40)
                .ok_or(ArenaError::RecordTooLarge(u64::MAX))?;
            if entries.is_empty() && entry_bytes > max_bytes as usize {
                return Err(ArenaError::RecordTooLarge(entry_bytes as u64));
            }
            if bytes.saturating_add(entry_bytes) > max_bytes as usize {
                break;
            }
            bytes += entry_bytes;
            entries.push(WitConflictSummary {
                ordinal: index as u64,
                key: conflict.key.clone(),
                base_length: conflict.base.as_ref().map(|value| value.len() as u64),
                a_length: conflict.a.as_ref().map(|value| value.len() as u64),
                b_length: conflict.b.as_ref().map(|value| value.len() as u64),
            });
        }
        let consumed = start.saturating_add(entries.len());
        let next_ordinal =
            (consumed < values.len()).then(|| entries.last().expect("non-empty page").ordinal);
        self.charge_budget(&budget, bytes, ArenaReadKind::Entity)?;
        Ok(WitConflictPage {
            next_ordinal,
            entries,
        })
    }

    fn read_value(
        &mut self,
        conflicts: Resource<V3ConflictSetResource>,
        budget: Resource<V3BudgetResource>,
        ordinal: u64,
        side: WitConflictSide,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, ArenaError> {
        let ordinal = usize::try_from(ordinal).map_err(|_| ArenaError::RecordTooLarge(u64::MAX))?;
        let conflict = self
            .table
            .get(&conflicts)
            .map_err(unavailable)?
            .0
            .get(ordinal)
            .ok_or(ArenaError::InvalidRange)?;
        let value = match side {
            WitConflictSide::Base => conflict.base.as_deref(),
            WitConflictSide::A => conflict.a.as_deref(),
            WitConflictSide::B => conflict.b.as_deref(),
        }
        .ok_or(ArenaError::InvalidRange)?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or(ArenaError::InvalidRange)?;
        let range = usize::try_from(offset)
            .ok()
            .zip(usize::try_from(end).ok())
            .and_then(|(start, end)| value.get(start..end))
            .ok_or(ArenaError::InvalidRange)?
            .to_vec();
        self.charge_budget(&budget, range.len(), ArenaReadKind::Entity)?;
        Ok(range)
    }

    fn drop(&mut self, conflicts: Resource<V3ConflictSetResource>) -> wasmtime::Result<()> {
        self.table.delete(conflicts)?;
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

    fn locate_file_record(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        range_offset: u64,
        range_length: u64,
        position: u64,
        delimiter: u8,
        forbidden: Vec<u8>,
    ) -> Result<Option<WitByteRecordLocator>, ArenaError> {
        let locator = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .bytes
            .locate_record(range_offset, range_length, position, delimiter, &forbidden)
            .map_err(arena_error)?;
        let materialized = locator
            .as_ref()
            .map_or(24, |locator| 24usize.saturating_add(locator.content.len()));
        self.charge_budget(&budget, materialized, ArenaReadKind::File)?;
        Ok(locator.map(|locator| WitByteRecordLocator {
            offset: locator.offset,
            length: locator.length,
            ordinal: locator.ordinal,
            content: locator.content,
        }))
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

    fn scan_entity_pages(
        &mut self,
        root: Resource<V3RootResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_pages: u32,
    ) -> Result<WitSemanticPageBatch, ArenaError> {
        let max_bytes = self.validate_semantic_page_request(&budget, max_pages)?;
        let pages = self
            .table
            .get(&root)
            .map_err(unavailable)?
            .0
            .entities
            .semantic_pages_bounded(after_key.as_deref(), max_pages, max_bytes)
            .map_err(arena_error)?;
        self.charge_semantic_pages(&budget, pages)
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

    fn locate_file_record(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        range_offset: u64,
        range_length: u64,
        position: u64,
        delimiter: u8,
        forbidden: Vec<u8>,
    ) -> Result<Option<WitByteRecordLocator>, ArenaError> {
        let locator = self
            .transaction(&transaction)?
            .locate_file_record(range_offset, range_length, position, delimiter, &forbidden)
            .map_err(arena_error)?;
        let materialized = locator
            .as_ref()
            .map_or(24, |locator| 24usize.saturating_add(locator.content.len()));
        self.charge_budget(&budget, materialized, ArenaReadKind::File)?;
        Ok(locator.map(|locator| WitByteRecordLocator {
            offset: locator.offset,
            length: locator.length,
            ordinal: locator.ordinal,
            content: locator.content,
        }))
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

    fn scan_entity_pages(
        &mut self,
        transaction: Resource<V3TransactionResource>,
        budget: Resource<V3BudgetResource>,
        after_key: Option<Vec<u8>>,
        max_pages: u32,
    ) -> Result<WitSemanticPageBatch, ArenaError> {
        let max_bytes = self.validate_semantic_page_request(&budget, max_pages)?;
        let pages = self
            .transaction(&transaction)?
            .semantic_entity_pages_bounded(after_key.as_deref(), max_pages, max_bytes)
            .map_err(arena_error)?;
        self.charge_semantic_pages(&budget, pages)
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

    fn declare_ordered_entity_output(
        &mut self,
        transaction: Resource<V3TransactionResource>,
    ) -> Result<(), ArenaError> {
        self.transaction_mut(&transaction)?
            .declare_ordered_entity_output()
            .map_err(arena_error)
    }

    fn drop(&mut self, transaction: Resource<V3TransactionResource>) -> wasmtime::Result<()> {
        self.table.delete(transaction)?;
        Ok(())
    }
}

impl bindings::lix::plugin::arena::Host for WasiHostState {}

impl WasiHostState {
    fn validate_semantic_page_request(
        &self,
        budget: &Resource<V3BudgetResource>,
        max_pages: u32,
    ) -> Result<usize, ArenaError> {
        let budget = self.table.get(budget).map_err(unavailable)?;
        validate_semantic_page_request_limits(budget.max_page_bytes, budget.max_pages, max_pages)?;
        Ok(budget.max_page_bytes as usize)
    }

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

    fn charge_semantic_pages(
        &mut self,
        budget: &Resource<V3BudgetResource>,
        batch: SemanticPageBatch,
    ) -> Result<WitSemanticPageBatch, ArenaError> {
        let bytes = batch.pages.iter().try_fold(0usize, |total, page| {
            total
                .checked_add(page.first_key.len())
                .and_then(|total| total.checked_add(page.last_key.len()))
                .and_then(|total| total.checked_add(page.fingerprint.len()))
                .and_then(|total| total.checked_add(4))
                .ok_or(ArenaError::RecordTooLarge(u64::MAX))
        })?;
        self.charge_budget(budget, bytes, ArenaReadKind::Entity)?;
        Ok(WitSemanticPageBatch {
            next_key: batch.next_key,
            pages: batch
                .pages
                .into_iter()
                .map(|page| WitSemanticPage {
                    first_key: page.first_key,
                    last_key: page.last_key,
                    fingerprint: page.fingerprint.to_vec(),
                    record_count: page.record_count,
                })
                .collect(),
        })
    }
}

fn validate_semantic_page_request_limits(
    max_page_bytes: u32,
    max_pages_limit: u32,
    requested_pages: u32,
) -> Result<(), ArenaError> {
    if requested_pages == 0 {
        return Err(ArenaError::LimitExceeded(
            "semantic page request must be positive".to_owned(),
        ));
    }
    if requested_pages > max_pages_limit {
        return Err(ArenaError::LimitExceeded(
            "semantic page request exceeds transition page limit".to_owned(),
        ));
    }
    // Every summary necessarily lowers a 32-byte fingerprint and u32 record
    // count, before either range key is included. Reject requests that cannot
    // fit one boundary page before allocating the host vector.
    let minimum_bytes = u64::from(requested_pages).saturating_mul(36);
    if minimum_bytes > u64::from(max_page_bytes) {
        return Err(ArenaError::RecordTooLarge(minimum_bytes));
    }
    Ok(())
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
        | ArenaStoreError::InvalidOrderedOutput
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
        ByteEdit, Store as ArenaStore, WasmV3ChangedEntity, WasmV3CreateContext,
        WasmV3EntityUpdate, WasmV3FileDescriptor, WasmV3FileUpdate, WasmV3InputBytes,
        WasmV3InputSplice, WasmV3OpenEntitiesInput, WasmV3OpenFileInput, WasmV3TransitionLimits,
        entity_arena_key,
    };

    use super::*;

    const FIXTURE_BYTES: usize = 10 * 1024 * 1024;
    const V2_JSON_TOTAL_BYTES: usize = 37_158_912;
    const V3_TOTAL_BYTES_TARGET: usize = V2_JSON_TOTAL_BYTES / 3;

    #[test]
    fn compressed_change_transport_is_bounded_and_rejects_corruption() {
        let mut raw = b"L3C1".to_vec();
        raw.extend_from_slice(&1_u32.to_le_bytes());
        raw.extend(std::iter::repeat_n(b'x', 16 * 1024));
        let mut wire = b"L3Z1".to_vec();
        wire.extend_from_slice(&lz4_flex::block::compress_prepend_size(&raw));
        assert_eq!(
            normalize_change_packet(wire, raw.len() as u32).unwrap(),
            raw
        );

        let mut oversized = b"L3Z1".to_vec();
        oversized.extend_from_slice(&65_537_u32.to_le_bytes());
        assert!(normalize_change_packet(oversized, 65_536).is_err());

        let mut corrupt = b"L3Z1".to_vec();
        corrupt.extend_from_slice(&128_u32.to_le_bytes());
        corrupt.extend_from_slice(b"not-lz4");
        match normalize_change_packet(corrupt, 65_536) {
            Ok(decoded) => assert!(decode_change_packet(&decoded).is_err()),
            Err(_) => {}
        }
    }

    #[tokio::test]
    async fn csv_v3_component_resolves_conflicts_lazily_and_direction_independently() {
        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3");
        let wasm = std::fs::read(wasm_path).expect("read CSV v3.2 component");
        let runtime = WasmtimePluginRuntime::new().expect("initialize Wasmtime runtime");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 64 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(30_000),
                },
            )
            .await
            .expect("compile CSV v3.2 component");
        let mut actor = factory.instantiate_actor().await.unwrap();
        let limits = WasmV3TransitionLimits {
            max_page_bytes: 128 * 1024,
            max_pages: 32,
            max_total_bytes: 512 * 1024,
            deadline_nanoseconds: 30_000_000_000,
        };
        let row_key = entity_arena_key("csv_v2_row", &["row".to_owned()]).unwrap();
        let base = br#"{"id":"row","order_key":"01","cells":["before","middle","after"]}"#.to_vec();
        let a = br#"{"id":"row","order_key":"01","cells":["A","middle","after"]}"#.to_vec();
        let b = br#"{"id":"row","order_key":"01","cells":["before","middle","B"]}"#.to_vec();
        let oversized = vec![b'x'; 65 * 1024];
        let transition = actor
            .resolve_conflicts(
                limits,
                WasmV3ConflictUpdate {
                    descriptor: WasmV3FileDescriptor {
                        path: Some("/conflict.csv".to_owned()),
                        media_type: Some("text/csv".to_owned()),
                        plugin_key: "plugin_csv_v3".to_owned(),
                        generation: "csv-v3.2-generation".to_owned(),
                    },
                    conflicts: vec![
                        WasmV3EntityConflict {
                            key: row_key.clone(),
                            base: Some(base),
                            a: Some(a),
                            b: Some(b),
                        },
                        WasmV3EntityConflict {
                            key: row_key,
                            base: Some(oversized.clone()),
                            a: Some(oversized.clone()),
                            b: Some(oversized),
                        },
                    ],
                },
            )
            .await
            .expect("resolve CSV v3.2 conflicts");
        let mut resolutions = Vec::new();
        while let Some(page) = actor
            .next_resolution_page(transition.transition, transition.resolutions, 128 * 1024)
            .await
            .expect("drain CSV v3.2 resolutions")
        {
            resolutions.extend(page);
        }
        assert_eq!(
            resolutions,
            [
                WasmV3ConflictResolution {
                    ordinal: 0,
                    choice: WasmV3ConflictChoice::Replace {
                        snapshot: br#"{"cells":["A","middle","B"],"id":"row","order_key":"01"}"#
                            .to_vec(),
                        effect: WasmChangeEffect::Content,
                    },
                },
                WasmV3ConflictResolution {
                    ordinal: 1,
                    choice: WasmV3ConflictChoice::TakeB,
                },
            ]
        );
        let counters = actor
            .finish_conflict_transition(transition.transition)
            .await
            .expect("finish CSV v3.2 conflict transition");
        assert!(
            counters.entity_page_bytes < 16 * 1024,
            "the oversized canonical-B conflict must not lower any snapshot bytes"
        );
    }

    #[test]
    fn semantic_page_requests_are_rejected_before_unbounded_host_allocation() {
        assert!(validate_semantic_page_request_limits(4 * 1024, 64, 16).is_ok());
        assert!(matches!(
            validate_semantic_page_request_limits(4 * 1024, 64, 0),
            Err(ArenaError::LimitExceeded(_))
        ));
        assert!(matches!(
            validate_semantic_page_request_limits(4 * 1024, 8, 9),
            Err(ArenaError::LimitExceeded(_))
        ));
        assert!(matches!(
            validate_semantic_page_request_limits(36, 64, 2),
            Err(ArenaError::RecordTooLarge(72))
        ));
    }

    fn large_excalidraw_bytes() -> Vec<u8> {
        const ELEMENTS: usize = 42_000;
        let mut bytes =
            br#"{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":["#
                .to_vec();
        for index in 0..ELEMENTS {
            if index != 0 {
                bytes.push(b',');
            }
            bytes.extend_from_slice(
                format!(
                    "{{\"id\":\"shape-{index:05}\",\"type\":\"rectangle\",\"x\":{index},\"y\":20,\"width\":100,\"height\":80,\"angle\":0,\"strokeColor\":\"#1b1b1f\",\"backgroundColor\":\"transparent\",\"fillStyle\":\"solid\",\"strokeWidth\":1,\"roughness\":1,\"opacity\":100,\"isDeleted\":false,\"customData\":{{\"benchmarkPadding\":\"0123456789abcdef0123456789abcdef\"}}}}"
                )
                .as_bytes(),
            );
        }
        bytes.extend_from_slice(br#"],"appState":{"gridSize":20},"files":{}}"#);
        assert!(bytes.len() >= 10 * 1024 * 1024);
        bytes
    }

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
        let mut abandoned_transaction = accepted.transaction();
        abandoned_transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 1,
            insert: vec![b'!'],
        });
        let abandoned = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor.clone(),
                    before: accepted.clone(),
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 1,
                        insert: WasmV3InputBytes::Inline(vec![b'!']),
                    }],
                    successor: abandoned_transaction,
                    creates: creates(),
                },
            )
            .await
            .expect("abandoned v3 edit should start");
        actor
            .finish_transition(abandoned.transition)
            .await
            .expect_err("a v3 transition must not publish before cursor EOF");
        assert_eq!(
            accepted.bytes.read(offset as u64, 1).unwrap(),
            vec![bytes[offset]],
            "early finish must roll back staged bytes and plugin state"
        );

        let mut invalid_descriptor = descriptor.clone();
        invalid_descriptor.path = Some("/invalid-output".to_owned());
        let mut invalid_transaction = accepted.transaction();
        invalid_transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 1,
            insert: vec![b'?'],
        });
        let invalid = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: invalid_descriptor,
                    before: accepted.clone(),
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 1,
                        insert: WasmV3InputBytes::Inline(vec![b'?']),
                    }],
                    successor: invalid_transaction,
                    creates: creates(),
                },
            )
            .await
            .expect("invalid-output fixture transition should start");
        actor
            .next_change_page(invalid.transition, invalid.changes, 64 * 1024)
            .await
            .expect_err("malformed guest output must abort the complete transition");
        assert_eq!(
            accepted.bytes.read(offset as u64, 1).unwrap(),
            vec![bytes[offset]],
            "malformed output must not publish staged bytes"
        );
        assert_eq!(
            accepted.state.get(b"fixture/invalid").unwrap(),
            None,
            "malformed output must not publish staged plugin state"
        );

        let mut invalid_edit_descriptor = descriptor.clone();
        invalid_edit_descriptor.path = Some("/invalid-edits".to_owned());
        let invalid_edits = actor
            .open_entities(
                limits,
                WasmV3OpenEntitiesInput {
                    descriptor: invalid_edit_descriptor,
                    durable: accepted.clone(),
                    successor: accepted.transaction(),
                    creates: creates(),
                },
            )
            .await
            .expect("invalid-edit fixture transition should start");
        actor
            .next_edit_page(invalid_edits.transition, invalid_edits.edits, 64 * 1024)
            .await
            .expect_err("overlapping guest byte edits must abort the complete transition");
        assert_eq!(
            accepted.state.get(b"fixture/invalid-edits").unwrap(),
            None,
            "invalid byte edits must not publish staged plugin state"
        );

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
                    after_descriptor: descriptor.clone(),
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
        assert_eq!(
            warm_counters.entity_page_reads, 2,
            "the guest should compare predecessor and staged-successor semantic summaries"
        );
        assert!(
            warm_counters.entity_page_bytes > 0,
            "semantic page summaries must cross the Component boundary"
        );
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
    async fn all_v3_format_components_are_deterministic_across_fresh_cold_actors() {
        struct Case {
            label: &'static str,
            wasm_path: &'static str,
            bytes: Vec<u8>,
            descriptor: WasmV3FileDescriptor,
        }

        let cases = vec![
            Case {
                label: "JSON",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_JSON_INCREMENTAL_V3_plugin_json_incremental_v3"
                ),
                bytes: br#"{"left":1,"right":[true,null]}"#.to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/deterministic.json".to_owned()),
                    media_type: Some("application/json".to_owned()),
                    plugin_key: "plugin_json_incremental_v3".to_owned(),
                    generation: "json-v3-generation".to_owned(),
                },
            },
            Case {
                label: "CSV",
                wasm_path: env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3"),
                bytes: b"name,value\r\nalpha,one\r\nbravo,two\r\n".to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/deterministic.csv".to_owned()),
                    media_type: Some("text/csv".to_owned()),
                    plugin_key: "plugin_csv_v3".to_owned(),
                    generation: "csv-v3-generation".to_owned(),
                },
            },
            Case {
                label: "Markdown",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_INCREMENTAL_V3_plugin_markdown_incremental_v3"
                ),
                bytes: b"# Stable\n\nFirst paragraph.\n\n## Child\n\nSecond paragraph.\n".to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/deterministic.md".to_owned()),
                    media_type: Some("text/markdown".to_owned()),
                    plugin_key: "plugin_markdown_incremental_v3".to_owned(),
                    generation: "markdown-v3-generation".to_owned(),
                },
            },
            Case {
                label: "Excalidraw",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_V3_plugin_excalidraw_v3"
                ),
                bytes: br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"shape-a","type":"rectangle","x":1,"y":2,"width":10,"height":20,"isDeleted":false}],"appState":{"gridSize":20},"files":{}}"#.to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/deterministic.excalidraw".to_owned()),
                    media_type: Some("application/json".to_owned()),
                    plugin_key: "plugin_excalidraw_v3".to_owned(),
                    generation: "excalidraw-v3-generation".to_owned(),
                },
            },
        ];

        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        for case in cases {
            let factory = runtime
                .compile_component_v3(
                    std::fs::read(case.wasm_path)
                        .unwrap_or_else(|error| panic!("read {} component: {error}", case.label)),
                    WasmLimits {
                        max_memory_bytes: 32 * 1024 * 1024,
                        max_fuel: None,
                        timeout_ms: Some(10_000),
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("compile {} v3: {error}", case.label));
            let mut observations = Vec::new();
            for _ in 0..2 {
                let mut actor = factory
                    .instantiate_actor()
                    .await
                    .unwrap_or_else(|error| panic!("instantiate {} v3: {error}", case.label));
                let store = ArenaStore::default();
                let imported = Root::import(
                    store,
                    case.descriptor.generation.clone(),
                    &case.bytes,
                    std::iter::empty(),
                    std::iter::empty(),
                );
                let opened = actor
                    .open_file(
                        transition_limits(),
                        WasmV3OpenFileInput {
                            descriptor: case.descriptor.clone(),
                            accepted: imported.clone(),
                            successor: imported.transaction(),
                            creates: creates(),
                        },
                    )
                    .await
                    .unwrap_or_else(|error| panic!("open {} v3: {error}", case.label));
                let mut emitted_keys = Vec::new();
                while let Some(page) = actor
                    .next_change_page(opened.transition, opened.changes, 64 * 1024)
                    .await
                    .unwrap_or_else(|error| panic!("drain {} v3: {error}", case.label))
                {
                    emitted_keys.extend(page.into_iter().map(|change| {
                        entity_arena_key(&change.schema_key, &change.entity_pk).unwrap()
                    }));
                }
                let (accepted, _) = actor
                    .finish_transition(opened.transition)
                    .await
                    .unwrap_or_else(|error| panic!("finish {} v3: {error}", case.label));
                assert_eq!(
                    accepted
                        .bytes
                        .read(0, accepted.bytes.len())
                        .expect("accepted bytes remain readable"),
                    case.bytes,
                    "{} must preserve exact accepted bytes",
                    case.label
                );
                if observations.is_empty() {
                    let accepted_id = accepted.id();
                    let byte_id = accepted.bytes.id();
                    let entity_id = accepted.entities.id();
                    let state_id = accepted.state.id();
                    let (_, reopened) = accepted
                        .archive()
                        .unwrap_or_else(|error| panic!("archive {} v3: {error}", case.label))
                        .reopen()
                        .unwrap_or_else(|error| panic!("reopen {} v3: {error}", case.label));
                    assert_eq!(
                        reopened.id(),
                        accepted_id,
                        "{} eviction/reopen must preserve the complete root",
                        case.label
                    );

                    let upgraded_generation = format!("{}-next", case.descriptor.generation);
                    let mut upgrade = reopened.transaction();
                    upgrade.upgrade_to(upgraded_generation.clone());
                    let upgraded = upgrade
                        .commit()
                        .unwrap_or_else(|error| panic!("upgrade {} v3: {error}", case.label));
                    assert_ne!(upgraded.id(), accepted_id);
                    assert_eq!(upgraded.bytes.id(), byte_id);
                    assert_eq!(upgraded.entities.id(), entity_id);
                    assert_eq!(upgraded.state.id(), state_id);

                    let mut upgraded_descriptor = case.descriptor.clone();
                    upgraded_descriptor.generation = upgraded_generation.clone();
                    let opened_entities = actor
                        .open_entities(
                            transition_limits(),
                            WasmV3OpenEntitiesInput {
                                descriptor: upgraded_descriptor,
                                durable: upgraded.clone(),
                                successor: upgraded.transaction(),
                                creates: creates(),
                            },
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!("open upgraded {} v3 entities: {error}", case.label)
                        });
                    let mut edit_count = 0usize;
                    while let Some(page) = actor
                        .next_edit_page(
                            opened_entities.transition,
                            opened_entities.edits,
                            64 * 1024,
                        )
                        .await
                        .unwrap_or_else(|error| {
                            panic!("drain upgraded {} v3 edits: {error}", case.label)
                        })
                    {
                        edit_count += page.len();
                    }
                    assert_eq!(
                        edit_count, 0,
                        "{} upgraded exact bytes must need no rendering edits",
                        case.label
                    );
                    let (upgraded, _) = actor
                        .finish_transition(opened_entities.transition)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("finish upgraded {} v3: {error}", case.label)
                        });
                    assert_eq!(upgraded.generation.as_ref(), upgraded_generation);
                    assert_eq!(upgraded.bytes.id(), byte_id);
                    assert_eq!(upgraded.entities.id(), entity_id);
                    assert_eq!(
                        upgraded
                            .bytes
                            .read(0, upgraded.bytes.len())
                            .expect("upgraded accepted bytes remain readable"),
                        case.bytes
                    );
                }
                emitted_keys.sort();
                observations.push((
                    accepted.id(),
                    accepted.entities.keys(),
                    accepted.state.id(),
                    emitted_keys,
                ));
            }
            assert_eq!(
                observations[0], observations[1],
                "{} must produce identical roots and durable keys in fresh actors",
                case.label
            );
        }
    }

    #[tokio::test]
    async fn all_v3_format_components_render_disjoint_merges_direction_independently() {
        struct Case {
            label: &'static str,
            wasm_path: &'static str,
            bytes: Vec<u8>,
            descriptor: WasmV3FileDescriptor,
            first: (&'static [u8], &'static [u8]),
            second: (&'static [u8], &'static [u8]),
        }

        fn find_once(bytes: &[u8], needle: &[u8]) -> usize {
            let offsets = bytes
                .windows(needle.len())
                .enumerate()
                .filter_map(|(offset, window)| (window == needle).then_some(offset))
                .collect::<Vec<_>>();
            assert_eq!(offsets.len(), 1, "merge edit needle must be unique");
            offsets[0]
        }

        async fn cold_import(
            actor: &mut dyn WasmComponentV3Actor,
            bytes: &[u8],
            descriptor: &WasmV3FileDescriptor,
        ) -> Root {
            let store = ArenaStore::default();
            let imported = Root::import(
                store,
                descriptor.generation.clone(),
                bytes,
                std::iter::empty(),
                std::iter::empty(),
            );
            let opened = actor
                .open_file(
                    transition_limits(),
                    WasmV3OpenFileInput {
                        descriptor: descriptor.clone(),
                        accepted: imported.clone(),
                        successor: imported.transaction(),
                        creates: creates(),
                    },
                )
                .await
                .unwrap();
            while actor
                .next_change_page(opened.transition, opened.changes, 64 * 1024)
                .await
                .unwrap()
                .is_some()
            {}
            actor.finish_transition(opened.transition).await.unwrap().0
        }

        async fn apply_file_edit(
            actor: &mut dyn WasmComponentV3Actor,
            base: &Root,
            descriptor: &WasmV3FileDescriptor,
            offset: usize,
            before: &[u8],
            after: &[u8],
            create_low_delta: u64,
        ) -> Root {
            assert_eq!(before.len(), after.len());
            let mut transaction = base.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: offset as u64,
                delete_len: before.len() as u64,
                insert: after.to_vec(),
            });
            let transition = actor
                .file_changed(
                    transition_limits(),
                    WasmV3FileUpdate {
                        before_descriptor: descriptor.clone(),
                        after_descriptor: descriptor.clone(),
                        before: base.clone(),
                        edits: vec![WasmV3InputSplice {
                            offset: offset as u64,
                            delete_len: before.len() as u64,
                            insert: WasmV3InputBytes::Inline(after.to_vec()),
                        }],
                        successor: transaction,
                        creates: WasmV3CreateContext {
                            high: creates().high,
                            low: creates().low + create_low_delta,
                        },
                    },
                )
                .await
                .unwrap();
            while actor
                .next_change_page(transition.transition, transition.changes, 64 * 1024)
                .await
                .unwrap()
                .is_some()
            {}
            actor
                .finish_transition(transition.transition)
                .await
                .unwrap()
                .0
        }

        async fn render_entities(
            actor: &mut dyn WasmComponentV3Actor,
            before: Root,
            merged: Root,
            descriptor: &WasmV3FileDescriptor,
            changed_keys: &BTreeSet<Vec<u8>>,
        ) -> Root {
            let transition = actor
                .entities_changed(
                    transition_limits(),
                    WasmV3EntityUpdate {
                        before_descriptor: descriptor.clone(),
                        after_descriptor: descriptor.clone(),
                        before,
                        changed_entities: changed_keys
                            .iter()
                            .cloned()
                            .map(|key| WasmV3ChangedEntity {
                                key,
                                format_only: false,
                            })
                            .collect(),
                        successor: merged.transaction(),
                        creates: creates(),
                    },
                )
                .await
                .unwrap();
            while actor
                .next_edit_page(transition.transition, transition.edits, 64 * 1024)
                .await
                .unwrap()
                .is_some()
            {}
            actor
                .finish_transition(transition.transition)
                .await
                .unwrap()
                .0
        }

        let cases = vec![
            Case {
                label: "JSON",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_JSON_INCREMENTAL_V3_plugin_json_incremental_v3"
                ),
                bytes: br#"{"left":1,"right":2}"#.to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/merge.json".to_owned()),
                    media_type: Some("application/json".to_owned()),
                    plugin_key: "plugin_json_incremental_v3".to_owned(),
                    generation: "json-v3-generation".to_owned(),
                },
                first: (br#""left":1"#, br#""left":3"#),
                second: (br#""right":2"#, br#""right":4"#),
            },
            Case {
                label: "CSV",
                wasm_path: env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3"),
                bytes: b"name,value\nalpha,one\nbravo,two\n".to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/merge.csv".to_owned()),
                    media_type: Some("text/csv".to_owned()),
                    plugin_key: "plugin_csv_v3".to_owned(),
                    generation: "csv-v3-generation".to_owned(),
                },
                first: (b"alpha", b"ALPHA"),
                second: (b"bravo", b"BRAVO"),
            },
            Case {
                label: "Markdown",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_INCREMENTAL_V3_plugin_markdown_incremental_v3"
                ),
                bytes: b"# First\n\nAlpha paragraph.\n\n# Second\n\nBravo paragraph.\n".to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/merge.md".to_owned()),
                    media_type: Some("text/markdown".to_owned()),
                    plugin_key: "plugin_markdown_incremental_v3".to_owned(),
                    generation: "markdown-v3-generation".to_owned(),
                },
                first: (b"Alpha", b"Omega"),
                second: (b"Bravo", b"Delta"),
            },
            Case {
                label: "Excalidraw",
                wasm_path: env!(
                    "CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_V3_plugin_excalidraw_v3"
                ),
                bytes: br#"{"type":"excalidraw","version":2,"source":"test","elements":[{"id":"a","type":"rectangle","x":1,"y":2,"width":10,"height":20,"isDeleted":false},{"id":"b","type":"ellipse","x":3,"y":4,"width":30,"height":40,"isDeleted":false}],"appState":{},"files":{}}"#.to_vec(),
                descriptor: WasmV3FileDescriptor {
                    path: Some("/merge.excalidraw".to_owned()),
                    media_type: Some("application/json".to_owned()),
                    plugin_key: "plugin_excalidraw_v3".to_owned(),
                    generation: "excalidraw-v3-generation".to_owned(),
                },
                first: (br#""x":1"#, br#""x":5"#),
                second: (br#""x":3"#, br#""x":7"#),
            },
        ];

        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        for case in cases {
            let factory = runtime
                .compile_component_v3(
                    std::fs::read(case.wasm_path).unwrap(),
                    WasmLimits {
                        max_memory_bytes: 32 * 1024 * 1024,
                        max_fuel: None,
                        timeout_ms: Some(10_000),
                    },
                )
                .await
                .unwrap();
            let mut actor = factory.instantiate_actor().await.unwrap();
            let base = cold_import(actor.as_mut(), &case.bytes, &case.descriptor).await;
            let first_offset = find_once(&case.bytes, case.first.0);
            let second_offset = find_once(&case.bytes, case.second.0);
            let a = apply_file_edit(
                actor.as_mut(),
                &base,
                &case.descriptor,
                first_offset,
                case.first.0,
                case.first.1,
                1,
            )
            .await;
            let b = apply_file_edit(
                actor.as_mut(),
                &base,
                &case.descriptor,
                second_offset,
                case.second.0,
                case.second.1,
                2,
            )
            .await;
            let changed_keys = |side: &Root| {
                base.entities
                    .keys()
                    .into_iter()
                    .filter(|key| base.entities.value_id(key) != side.entities.value_id(key))
                    .collect::<BTreeSet<_>>()
            };
            let a_changed = changed_keys(&a);
            let b_changed = changed_keys(&b);
            assert!(
                !a_changed.is_empty(),
                "{} branch A must change an entity",
                case.label
            );
            assert!(
                !b_changed.is_empty(),
                "{} branch B must change an entity",
                case.label
            );
            assert!(
                a_changed.is_disjoint(&b_changed),
                "{} fixture edits must target disjoint durable entities: A={a_changed:?} B={b_changed:?}",
                case.label
            );
            let ab = Root::merge_entities(&base, &a, &b).unwrap();
            let ba = Root::merge_entities(&base, &b, &a).unwrap();
            assert_eq!(
                ab.entities.id(),
                ba.entities.id(),
                "{} entity merge must be direction-independent",
                case.label
            );

            let rendered_ab =
                render_entities(actor.as_mut(), a, ab, &case.descriptor, &b_changed).await;
            let rendered_ba =
                render_entities(actor.as_mut(), b, ba, &case.descriptor, &a_changed).await;
            let mut expected = case.bytes;
            expected[first_offset..first_offset + case.first.0.len()].copy_from_slice(case.first.1);
            expected[second_offset..second_offset + case.second.0.len()]
                .copy_from_slice(case.second.1);
            assert_eq!(
                rendered_ab.bytes.read(0, rendered_ab.bytes.len()).unwrap(),
                expected,
                "{} must render both disjoint branch edits",
                case.label
            );
            assert_eq!(
                rendered_ba.bytes.read(0, rendered_ba.bytes.len()).unwrap(),
                expected,
                "{} reverse merge must render the same exact bytes",
                case.label
            );
            assert_eq!(rendered_ab.entities.id(), rendered_ba.entities.id());
        }
    }

    #[tokio::test]
    async fn json_v3_preserves_exact_bytes_schema_keys_and_stable_identities() {
        let Some(wasm_path) =
            option_env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_INCREMENTAL_V3_plugin_json_incremental_v3")
        else {
            panic!("the JSON v3 artifact dependency must be available");
        };
        let wasm = std::fs::read(wasm_path).expect("JSON v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 16 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("JSON v3 should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("JSON v3 should instantiate");

        let before = br#"{"left":1,"right":[true]}"#.to_vec();
        let store = ArenaStore::default();
        let imported = Root::import(
            store,
            "json-v3-generation",
            &before,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/identity.json".to_owned()),
            media_type: Some("application/json".to_owned()),
            plugin_key: "plugin_json_incremental_v3".to_owned(),
            generation: "json-v3-generation".to_owned(),
        };
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
        let mut cold_changes = Vec::new();
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .unwrap()
        {
            cold_changes.extend(page);
        }
        let (accepted, _) = actor.finish_transition(opened.transition).await.unwrap();
        let accepted_keys = cold_changes
            .iter()
            .map(|change| entity_arena_key(&change.schema_key, &change.entity_pk).unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(accepted.entities.len(), cold_changes.len());
        assert_eq!(
            cold_changes
                .iter()
                .map(|change| change.schema_key.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["json_array_item", "json_object_member", "json_root"])
        );

        let scalar_offset = before
            .iter()
            .position(|byte| *byte == b'1')
            .expect("fixture scalar should exist");
        let mut transaction = accepted.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: scalar_offset as u64,
            delete_len: 1,
            insert: b"2".to_vec(),
        });
        let updated = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor.clone(),
                    before: accepted.clone(),
                    edits: vec![WasmV3InputSplice {
                        offset: scalar_offset as u64,
                        delete_len: 1,
                        insert: WasmV3InputBytes::Inline(b"2".to_vec()),
                    }],
                    successor: transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 1,
                    },
                },
            )
            .await
            .unwrap();
        let mut warm_changes = Vec::new();
        while let Some(page) = actor
            .next_change_page(updated.transition, updated.changes, 64 * 1024)
            .await
            .unwrap()
        {
            warm_changes.extend(page);
        }
        let (successor, _) = actor.finish_transition(updated.transition).await.unwrap();

        let mut after = before.clone();
        after[scalar_offset] = b'2';
        assert_eq!(
            successor.bytes.read(0, successor.bytes.len()).unwrap(),
            after
        );
        assert_eq!(warm_changes.len(), 1);
        assert_eq!(warm_changes[0].schema_key, "json_object_member");
        assert_eq!(
            successor
                .entities
                .keys()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            accepted_keys,
            "a scalar edit must not churn any durable identity"
        );
        assert_eq!(
            successor.state.get(b"json/v3/index-version").unwrap(),
            Some(b"scalar-byte-windows-v2".to_vec())
        );
        assert_eq!(
            accepted.bytes.read(0, accepted.bytes.len()).unwrap(),
            before,
            "the prior accepted root remains immutable"
        );
    }

    #[tokio::test]
    async fn markdown_v3_preserves_exact_bytes_and_changes_one_top_level_entity() {
        let wasm_path =
            env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_INCREMENTAL_V3_plugin_markdown_incremental_v3");
        let wasm = std::fs::read(wasm_path).expect("Markdown v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 16 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("Markdown v3 should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("Markdown v3 should instantiate");
        let before =
            b"# Title\n\nFirst paragraph.\n\nSecond paragraph.\n\nThird paragraph.\n".to_vec();
        let store = ArenaStore::default();
        let imported = Root::import(
            store,
            "markdown-v3-generation",
            &before,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/identity.md".to_owned()),
            media_type: Some("text/markdown".to_owned()),
            plugin_key: "plugin_markdown_incremental_v3".to_owned(),
            generation: "markdown-v3-generation".to_owned(),
        };
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
            .expect("Markdown v3 cold import should start");
        let mut cold_changes = Vec::new();
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .expect("Markdown v3 cold changes should drain")
        {
            cold_changes.extend(page);
        }
        let (accepted, _) = actor
            .finish_transition(opened.transition)
            .await
            .expect("Markdown v3 cold import should commit");
        assert!(!cold_changes.is_empty());
        assert!(cold_changes.iter().all(|change| {
            change.schema_key == "markdown_node_v2" && change.snapshot.is_some()
        }));
        assert_eq!(accepted.bytes.read(0, before.len() as u64).unwrap(), before);
        let cold_keys = cold_changes
            .iter()
            .map(|change| entity_arena_key(&change.schema_key, &change.entity_pk).unwrap())
            .collect::<BTreeSet<_>>();

        let offset = before
            .windows("Second".len())
            .position(|window| window == b"Second")
            .expect("fixture contains second paragraph");
        let mut transaction = accepted.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 6,
            insert: b"2nd".to_vec(),
        });
        let updated = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor.clone(),
                    before: accepted.clone(),
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 6,
                        insert: WasmV3InputBytes::Inline(b"2nd".to_vec()),
                    }],
                    successor: transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 1,
                    },
                },
            )
            .await
            .expect("Markdown v3 sparse edit should start");
        let changes = actor
            .next_change_page(updated.transition, updated.changes, 64 * 1024)
            .await
            .expect("Markdown v3 sparse changes should drain")
            .expect("Markdown v3 sparse edit emits one entity");
        assert_eq!(changes.len(), 1);
        assert!(
            cold_keys.contains(
                &entity_arena_key(&changes[0].schema_key, &changes[0].entity_pk).unwrap()
            )
        );
        assert!(
            actor
                .next_change_page(updated.transition, updated.changes, 64 * 1024)
                .await
                .expect("Markdown v3 sparse cursor reaches EOF")
                .is_none()
        );
        let (successor, counters) = actor
            .finish_transition(updated.transition)
            .await
            .expect("Markdown v3 sparse transition should commit");
        let mut expected = before.clone();
        expected.splice(offset..offset + 6, b"2nd".iter().copied());
        assert_eq!(
            successor.bytes.read(0, expected.len() as u64).unwrap(),
            expected
        );
        assert_eq!(counters.entity_page_reads, 1);
        assert!(counters.state_page_reads <= 4);
        assert!(
            counters.file_page_bytes < 64 * 1024,
            "sparse Markdown must read one affected top-level range"
        );

        let third_offset = expected
            .windows("Third".len())
            .position(|window| window == b"Third")
            .expect("shifted fixture contains third paragraph");
        let mut second_transaction = successor.transaction();
        second_transaction.edit_bytes(ByteEdit {
            offset: third_offset as u64,
            delete_len: 1,
            insert: b"t".to_vec(),
        });
        let second = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor,
                    before: successor,
                    edits: vec![WasmV3InputSplice {
                        offset: third_offset as u64,
                        delete_len: 1,
                        insert: WasmV3InputBytes::Inline(b"t".to_vec()),
                    }],
                    successor: second_transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 2,
                    },
                },
            )
            .await
            .expect("Markdown v3 shifted sparse edit should start");
        let second_changes = actor
            .next_change_page(second.transition, second.changes, 64 * 1024)
            .await
            .expect("Markdown shifted changes should drain")
            .expect("Markdown shifted edit emits one entity");
        assert_eq!(second_changes.len(), 1);
        assert!(
            actor
                .next_change_page(second.transition, second.changes, 64 * 1024)
                .await
                .expect("Markdown shifted cursor reaches EOF")
                .is_none()
        );
        let (second_successor, _) = actor
            .finish_transition(second.transition)
            .await
            .expect("Markdown shifted transition should commit");
        expected[third_offset] = b't';
        assert_eq!(
            second_successor
                .bytes
                .read(0, expected.len() as u64)
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn csv_v3_preserves_exact_bytes_and_one_stable_row_identity() {
        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3");
        let wasm = std::fs::read(wasm_path).expect("CSV v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 16 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("CSV v3 should compile");
        let mut actor = factory.instantiate_actor().await.unwrap();
        let before = b"name,value\r\nalpha,one\r\nbravo,two\r\ncharlie,three\r\n".to_vec();
        let store = ArenaStore::default();
        let imported = Root::import(
            store,
            "csv-v3-generation",
            &before,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/identity.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin_key: "plugin_csv_v3".to_owned(),
            generation: "csv-v3-generation".to_owned(),
        };
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
        let mut cold_changes = Vec::new();
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .unwrap()
        {
            cold_changes.extend(page);
        }
        assert_eq!(cold_changes.len(), 5, "four rows plus the table");
        let (accepted, _) = actor.finish_transition(opened.transition).await.unwrap();
        assert_eq!(accepted.bytes.read(0, before.len() as u64).unwrap(), before);
        let cold_keys = accepted
            .entities
            .keys()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let offset = before
            .windows(5)
            .position(|window| window == b"bravo")
            .expect("fixture contains target row");
        let mut transaction = accepted.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 5,
            insert: b"BRAVO".to_vec(),
        });
        let updated = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor,
                    before: accepted,
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 5,
                        insert: WasmV3InputBytes::Inline(b"BRAVO".to_vec()),
                    }],
                    successor: transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 1,
                    },
                },
            )
            .await
            .unwrap();
        let changes = actor
            .next_change_page(updated.transition, updated.changes, 64 * 1024)
            .await
            .unwrap()
            .expect("CSV edit emits one row");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].schema_key, "csv_v2_row");
        let changed_key = entity_arena_key(&changes[0].schema_key, &changes[0].entity_pk).unwrap();
        assert!(cold_keys.contains(&changed_key));
        let (successor, counters) = actor.finish_transition(updated.transition).await.unwrap();
        let mut expected = before;
        expected[offset..offset + 5].copy_from_slice(b"BRAVO");
        assert_eq!(
            successor.bytes.read(0, expected.len() as u64).unwrap(),
            expected
        );
        assert_eq!(
            successor.entities.keys(),
            cold_keys.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(counters.entity_page_reads, 1);
        assert!(counters.file_page_bytes <= 16 * 1024);
    }

    #[tokio::test]
    async fn excalidraw_v3_preserves_exact_bytes_identity_and_shifted_sparse_edits() {
        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_V3_plugin_excalidraw_v3");
        let wasm = std::fs::read(wasm_path).expect("Excalidraw v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 16 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(10_000),
                },
            )
            .await
            .expect("Excalidraw v3 should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("Excalidraw v3 should instantiate");
        let before = br##"{
  "type": "excalidraw",
  "version": 2,
  "source": "https://excalidraw.com",
  "elements": [
    {"id":"a","type":"rectangle","x":1.25,"y":2,"width":100,"height":80,"isDeleted":false},
    {"id":"b","type":"ellipse","x":20,"y":30,"width":50,"height":40,"isDeleted":false}
  ],
  "appState": {"gridSize":20,"viewBackgroundColor":"#ffffff"},
  "files": {
    "file-1": {"id":"file-1","mimeType":"image/png","dataURL":"data:image/png;base64,AA==","created":123}
  }
}
"##
        .to_vec();
        let store = ArenaStore::default();
        let imported = Root::import(
            store,
            "excalidraw-v3-generation",
            &before,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/identity.excalidraw".to_owned()),
            media_type: Some("application/json".to_owned()),
            plugin_key: "plugin_excalidraw_v3".to_owned(),
            generation: "excalidraw-v3-generation".to_owned(),
        };
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
            .expect("Excalidraw v3 cold import should start");
        let mut cold_changes = Vec::new();
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .expect("Excalidraw v3 cold changes should drain")
        {
            cold_changes.extend(page);
        }
        let (accepted, _) = actor
            .finish_transition(opened.transition)
            .await
            .expect("Excalidraw v3 cold import should commit");
        assert_eq!(cold_changes.len(), 4);
        assert_eq!(
            cold_changes
                .iter()
                .map(|change| change.schema_key.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["excalidraw_scene", "excalidraw_element", "excalidraw_file",])
        );
        assert_eq!(accepted.bytes.read(0, before.len() as u64).unwrap(), before);
        let cold_keys = accepted
            .entities
            .keys()
            .into_iter()
            .collect::<BTreeSet<_>>();

        let offset = before
            .windows(4)
            .position(|window| window == b"1.25")
            .expect("fixture contains first element x");
        let mut transaction = accepted.transaction();
        transaction.edit_bytes(ByteEdit {
            offset: offset as u64,
            delete_len: 4,
            insert: b"123.5".to_vec(),
        });
        let updated = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor.clone(),
                    before: accepted,
                    edits: vec![WasmV3InputSplice {
                        offset: offset as u64,
                        delete_len: 4,
                        insert: WasmV3InputBytes::Inline(b"123.5".to_vec()),
                    }],
                    successor: transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 1,
                    },
                },
            )
            .await
            .expect("Excalidraw v3 sparse edit should start");
        let changes = actor
            .next_change_page(updated.transition, updated.changes, 64 * 1024)
            .await
            .expect("Excalidraw v3 sparse changes should drain")
            .expect("Excalidraw v3 sparse edit emits one entity");
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].schema_key, "excalidraw_element");
        assert_eq!(changes[0].entity_pk, ["a"]);
        assert!(
            cold_keys.contains(
                &entity_arena_key(&changes[0].schema_key, &changes[0].entity_pk).unwrap()
            )
        );
        assert!(
            actor
                .next_change_page(updated.transition, updated.changes, 64 * 1024)
                .await
                .unwrap()
                .is_none()
        );
        let (successor, counters) = actor.finish_transition(updated.transition).await.unwrap();
        let mut expected = before.clone();
        expected.splice(offset..offset + 4, b"123.5".iter().copied());
        assert_eq!(
            successor.bytes.read(0, expected.len() as u64).unwrap(),
            expected
        );
        assert_eq!(
            successor.entities.keys(),
            cold_keys.iter().cloned().collect::<Vec<_>>()
        );
        assert_eq!(counters.entity_page_reads, 1);
        assert!(counters.file_page_bytes < 4 * 1024);

        let second_offset = expected
            .windows(6)
            .position(|window| window == b"\"x\":20")
            .expect("shifted fixture contains second element x")
            + 4;
        let mut second_transaction = successor.transaction();
        second_transaction.edit_bytes(ByteEdit {
            offset: second_offset as u64,
            delete_len: 2,
            insert: b"21".to_vec(),
        });
        let second = actor
            .file_changed(
                limits,
                WasmV3FileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor,
                    before: successor,
                    edits: vec![WasmV3InputSplice {
                        offset: second_offset as u64,
                        delete_len: 2,
                        insert: WasmV3InputBytes::Inline(b"21".to_vec()),
                    }],
                    successor: second_transaction,
                    creates: WasmV3CreateContext {
                        high: creates().high,
                        low: creates().low + 2,
                    },
                },
            )
            .await
            .expect("Excalidraw v3 shifted sparse edit should start");
        let second_changes = actor
            .next_change_page(second.transition, second.changes, 64 * 1024)
            .await
            .unwrap()
            .expect("shifted sparse edit emits one entity");
        assert_eq!(second_changes.len(), 1);
        assert_eq!(second_changes[0].entity_pk, ["b"]);
        let (second_successor, _) = actor.finish_transition(second.transition).await.unwrap();
        expected[second_offset..second_offset + 2].copy_from_slice(b"21");
        assert_eq!(
            second_successor
                .bytes
                .read(0, expected.len() as u64)
                .unwrap(),
            expected
        );
        assert_eq!(
            second_successor.entities.keys(),
            cold_keys.into_iter().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    #[ignore = "manual production-shaped 10 MiB Excalidraw v3 sparse-edit benchmark"]
    async fn excalidraw_v3_ten_mib_sparse_edit_benchmark() {
        const WARMUPS: usize = 20;
        const SAMPLES: usize = 200;
        const V2_P95_NS: u64 = 947_766_000;
        const V2_TOTAL_OWNED_BYTES: u64 = 225_877_856;
        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_EXCALIDRAW_V3_plugin_excalidraw_v3");
        let wasm = std::fs::read(wasm_path).expect("read Excalidraw v3 component");
        let runtime = WasmtimePluginRuntime::new().expect("initialize Wasmtime runtime");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 256 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(120_000),
                },
            )
            .await
            .expect("compile Excalidraw v3 component");
        let bytes = large_excalidraw_bytes();
        let needle = b"\"id\":\"shape-21000\",\"type\":\"rectangle\",\"x\":21000";
        let object_offset = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("large Excalidraw target element exists");
        let edit_offset = object_offset + needle.len() - 5;
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "excalidraw-v3-benchmark",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/large.excalidraw".to_owned()),
            media_type: Some("application/json".to_owned()),
            plugin_key: "plugin_excalidraw_v3".to_owned(),
            generation: "excalidraw-v3-benchmark".to_owned(),
        };
        let limits = WasmV3TransitionLimits {
            max_page_bytes: 64 * 1024,
            max_pages: 8_192,
            max_total_bytes: 512 * 1024 * 1024,
            deadline_nanoseconds: 120_000_000_000,
        };
        let mut cold_actor = factory.instantiate_actor().await.unwrap();
        let cold = cold_actor
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
            .expect("open large Excalidraw v3 base");
        let mut cold_changes = 0usize;
        while let Some(page) = cold_actor
            .next_change_page(cold.transition, cold.changes, 64 * 1024)
            .await
            .unwrap()
        {
            cold_changes += page.len();
        }
        assert_eq!(cold_changes, 42_001);
        let (accepted, cold_counters) =
            cold_actor.finish_transition(cold.transition).await.unwrap();
        drop(cold_actor);
        store.retain_reachable(&accepted);
        store.evict_resident_pages();
        let baseline_host_bytes = store.resident_page_bytes() as u64;
        let mut actor = factory.instantiate_actor().await.unwrap();
        let mut elapsed = Vec::with_capacity(SAMPLES);
        let mut boundaries = Vec::with_capacity(SAMPLES);
        let mut guest_high_waters = Vec::with_capacity(SAMPLES);
        for sample in 0..WARMUPS + SAMPLES {
            let mut transaction = accepted.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: edit_offset as u64,
                delete_len: 5,
                insert: b"21001".to_vec(),
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
                            offset: edit_offset as u64,
                            delete_len: 5,
                            insert: WasmV3InputBytes::Inline(b"21001".to_vec()),
                        }],
                        successor: transaction,
                        creates: WasmV3CreateContext {
                            high: creates().high,
                            low: creates().low + sample as u64 + 1,
                        },
                    },
                )
                .await
                .expect("run Excalidraw v3 sparse edit");
            let mut changes = Vec::new();
            while let Some(page) = actor
                .next_change_page(updated.transition, updated.changes, 64 * 1024)
                .await
                .unwrap()
            {
                changes.extend(page);
            }
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].schema_key, "excalidraw_element");
            assert_eq!(changes[0].entity_pk, ["shape-21000"]);
            let (successor, counters) = actor.finish_transition(updated.transition).await.unwrap();
            assert_eq!(successor.bytes.len(), bytes.len() as u64);
            if sample >= WARMUPS {
                elapsed.push(started.elapsed().as_nanos() as u64);
                boundaries.push(counters.component_boundary_bytes);
                guest_high_waters.push(counters.guest_linear_memory_high_water_bytes);
            }
            drop(successor);
            store.retain_reachable(&accepted);
            store.evict_resident_pages();
        }
        elapsed.sort_unstable();
        boundaries.sort_unstable();
        guest_high_waters.sort_unstable();
        let percentile = |values: &[u64], value: usize| {
            values[(values.len() * value).div_ceil(100).saturating_sub(1)]
        };
        let p95_ns = percentile(&elapsed, 95);
        let p95_owned = baseline_host_bytes.saturating_add(percentile(&guest_high_waters, 95));
        eprintln!(
            "excalidraw_v3_sparse_edit bytes={} elements=42000 warmups={WARMUPS} \
             samples={SAMPLES} p50_ms={:.3} p95_ms={:.3} speedup_vs_v2={:.3} \
             baseline_host_bytes={} p95_guest_high_water_bytes={} \
             p95_retained_owned_bytes={} retained_reduction_vs_v2={:.3} \
             p95_boundary_bytes={} cold_guest_high_water_bytes={} \
             cold_boundary_bytes={} latency_passes={} retained_memory_passes={}",
            bytes.len(),
            percentile(&elapsed, 50) as f64 / 1_000_000.0,
            p95_ns as f64 / 1_000_000.0,
            V2_P95_NS as f64 / p95_ns as f64,
            baseline_host_bytes,
            percentile(&guest_high_waters, 95),
            p95_owned,
            V2_TOTAL_OWNED_BYTES as f64 / p95_owned as f64,
            percentile(&boundaries, 95),
            cold_counters.guest_linear_memory_high_water_bytes,
            cold_counters.component_boundary_bytes,
            p95_ns.saturating_mul(2) <= V2_P95_NS,
            p95_owned.saturating_mul(3) <= V2_TOTAL_OWNED_BYTES,
        );
    }

    #[tokio::test]
    #[ignore = "manual real VS Code API Markdown affected-range benchmark"]
    async fn markdown_v3_vscode_api_real_history_benchmark() {
        const WARMUPS: usize = 20;
        const SAMPLES: usize = 200;
        const BEFORE_COMMIT: &str = "b668f69";
        const AFTER_COMMIT: &str = "578def9";
        const PATH: &str = "api/references/vscode-api.md";

        let repository = env::var("LIX_MARKDOWN_BENCH_REPO")
            .unwrap_or_else(|_| "/root/projects/vscode-api-repro".to_owned());
        let git_show = |commit: &str| {
            let output = std::process::Command::new("git")
                .args(["-C", &repository, "show", &format!("{commit}:{PATH}")])
                .output()
                .expect("run git show for Markdown benchmark");
            assert!(
                output.status.success(),
                "git show failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        };
        let before = git_show(BEFORE_COMMIT);
        let after = git_show(AFTER_COMMIT);
        let prefix = before
            .iter()
            .zip(&after)
            .take_while(|(left, right)| left == right)
            .count();
        let suffix = before[prefix..]
            .iter()
            .rev()
            .zip(after[prefix..].iter().rev())
            .take_while(|(left, right)| left == right)
            .count();
        let delete_len = before.len() - prefix - suffix;
        let insert = after[prefix..after.len() - suffix].to_vec();
        assert_eq!(before.len(), 1_237_841);
        assert_eq!(after.len(), 1_237_840);

        let wasm_path =
            env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_INCREMENTAL_V3_plugin_markdown_incremental_v3");
        let wasm = std::fs::read(wasm_path).expect("read Markdown v3 component");
        let runtime = WasmtimePluginRuntime::new().expect("initialize Wasmtime runtime");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 128 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(120_000),
                },
            )
            .await
            .expect("compile Markdown v3 component");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("instantiate Markdown v3 actor");
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "markdown-v3-generation",
            &before,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some(format!("/{PATH}")),
            media_type: Some("text/markdown".to_owned()),
            plugin_key: "plugin_markdown_incremental_v3".to_owned(),
            generation: "markdown-v3-generation".to_owned(),
        };
        let limits = WasmV3TransitionLimits {
            max_page_bytes: 64 * 1024,
            max_pages: 4_096,
            max_total_bytes: 256 * 1024 * 1024,
            deadline_nanoseconds: 120_000_000_000,
        };
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
            .expect("open real Markdown fixture");
        while actor
            .next_change_page(opened.transition, opened.changes, 64 * 1024)
            .await
            .expect("drain Markdown cold changes")
            .is_some()
        {}
        let (accepted, _) = actor
            .finish_transition(opened.transition)
            .await
            .expect("commit Markdown cold import");
        drop(actor);
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("instantiate cold-successor Markdown actor");

        let mut elapsed = Vec::with_capacity(SAMPLES);
        let mut boundaries = Vec::with_capacity(SAMPLES);
        let mut file_bytes = Vec::with_capacity(SAMPLES);
        let mut entity_bytes = Vec::with_capacity(SAMPLES);
        let mut state_bytes = Vec::with_capacity(SAMPLES);
        let mut guest_high_waters = Vec::with_capacity(SAMPLES);
        for sample in 0..WARMUPS + SAMPLES {
            let mut transaction = accepted.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: prefix as u64,
                delete_len: delete_len as u64,
                insert: insert.clone(),
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
                            offset: prefix as u64,
                            delete_len: delete_len as u64,
                            insert: WasmV3InputBytes::Inline(insert.clone()),
                        }],
                        successor: transaction,
                        creates: WasmV3CreateContext {
                            high: creates().high,
                            low: creates().low + sample as u64 + 1,
                        },
                    },
                )
                .await
                .expect("run Markdown affected-range transition");
            let mut changes = 0usize;
            while let Some(page) = actor
                .next_change_page(updated.transition, updated.changes, 64 * 1024)
                .await
                .expect("drain Markdown sparse changes")
            {
                changes += page.len();
            }
            let (successor, counters) = actor
                .finish_transition(updated.transition)
                .await
                .expect("finish Markdown sparse transition");
            let sample_elapsed = started.elapsed().as_nanos() as u64;
            assert_eq!(changes, 1);
            assert_eq!(successor.bytes.read(0, after.len() as u64).unwrap(), after);
            drop(successor);
            store.retain_reachable(&accepted);
            if sample >= WARMUPS {
                elapsed.push(sample_elapsed);
                boundaries.push(counters.component_boundary_bytes);
                file_bytes.push(counters.file_page_bytes);
                entity_bytes.push(counters.entity_page_bytes);
                state_bytes.push(counters.state_page_bytes);
                guest_high_waters.push(counters.guest_linear_memory_high_water_bytes);
            }
        }
        elapsed.sort_unstable();
        boundaries.sort_unstable();
        file_bytes.sort_unstable();
        entity_bytes.sort_unstable();
        state_bytes.sort_unstable();
        guest_high_waters.sort_unstable();
        eprintln!(
            "markdown_v3_vscode_api bytes_before={} bytes_after={} edit_offset={} \
             delete_bytes={} insert_bytes={} warmups={WARMUPS} samples={SAMPLES} \
             p50_ms={:.3} p95_ms={:.3} p95_guest_high_water_bytes={} \
             p95_boundary_bytes={} p95_file_bytes={} p95_entity_bytes={} p95_state_bytes={}",
            before.len(),
            after.len(),
            prefix,
            delete_len,
            insert.len(),
            percentile(&elapsed, 50) as f64 / 1_000_000.0,
            percentile(&elapsed, 95) as f64 / 1_000_000.0,
            percentile(&guest_high_waters, 95),
            percentile(&boundaries, 95),
            percentile(&file_bytes, 95),
            percentile(&entity_bytes, 95),
            percentile(&state_bytes, 95),
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

    #[tokio::test]
    #[ignore = "manual release-mode JSON v3 affected-page scorecard"]
    async fn json_v3_ten_mib_affected_page_benchmark() {
        const WARMUPS: usize = 100;
        const SAMPLES: usize = 2_000;

        let Some(wasm_path) =
            option_env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_INCREMENTAL_V3_plugin_json_incremental_v3")
        else {
            panic!("the JSON v3 artifact dependency must be available");
        };
        let wasm = std::fs::read(wasm_path).expect("JSON v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 128 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(60_000),
                },
            )
            .await
            .expect("JSON v3 should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("JSON v3 should instantiate");

        let (bytes, edit_offset) = json_ten_mib_fixture();
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "json-v3-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/ten-mib.json".to_owned()),
            media_type: Some("application/json".to_owned()),
            plugin_key: "plugin_json_incremental_v3".to_owned(),
            generation: "json-v3-generation".to_owned(),
        };
        let limits = WasmV3TransitionLimits {
            max_page_bytes: 1024 * 1024,
            max_pages: 2_048,
            max_total_bytes: 128 * 1024 * 1024,
            deadline_nanoseconds: 60_000_000_000,
        };
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
        let mut cold_change_count = 0usize;
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 1024 * 1024)
            .await
            .unwrap()
        {
            cold_change_count += page.len();
        }
        let (mut accepted, cold_counters) =
            actor.finish_transition(opened.transition).await.unwrap();
        assert_eq!(cold_change_count, 39_871);
        drop(actor);
        store.retain_reachable(&accepted);
        store.evict_resident_pages();
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("JSON v3 should cold-instantiate after host eviction");

        let mut samples = Vec::with_capacity(SAMPLES);
        let mut replacement = b'0';
        let mut last_replacement = replacement;
        let mut last_counters = WasmV3TransitionCounters::default();
        for sample in 0..WARMUPS + SAMPLES {
            let mut transaction = accepted.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: edit_offset as u64,
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
                            offset: edit_offset as u64,
                            delete_len: 1,
                            insert: WasmV3InputBytes::Inline(vec![replacement]),
                        }],
                        successor: transaction,
                        creates: creates(),
                    },
                )
                .await
                .unwrap();
            let mut change_count = 0usize;
            while let Some(page) = actor
                .next_change_page(updated.transition, updated.changes, 1024 * 1024)
                .await
                .unwrap()
            {
                change_count += page.len();
            }
            assert_eq!(change_count, 1);
            (accepted, last_counters) = actor.finish_transition(updated.transition).await.unwrap();
            last_replacement = replacement;
            if sample >= WARMUPS {
                samples.push(
                    u64::try_from(started.elapsed().as_nanos())
                        .expect("benchmark duration should fit u64"),
                );
            }
            replacement = if replacement == b'0' { b'1' } else { b'0' };
        }
        samples.sort_unstable();
        let p50_ns = percentile(&samples, 50);
        let p95_ns = percentile(&samples, 95);
        let total_owned_bytes = store
            .resident_page_bytes()
            .saturating_add(last_counters.guest_linear_memory_high_water_bytes as usize);
        eprintln!(
            "json_v3_affected_page bytes={} entities={cold_change_count} warmups={WARMUPS} \
             samples={SAMPLES} p50_ms={:.3} p95_ms={:.3} host_resident_bytes={} \
             host_logical_durable_bytes={} \
             guest_high_water_bytes={} total_owned_bytes={} boundary_bytes={} \
             entity_page_reads={} entity_page_bytes={} cold_boundary_bytes={} accepted={}",
            bytes.len(),
            p50_ns as f64 / 1_000_000.0,
            p95_ns as f64 / 1_000_000.0,
            store.resident_page_bytes(),
            store.unique_page_bytes(),
            last_counters.guest_linear_memory_high_water_bytes,
            total_owned_bytes,
            last_counters.component_boundary_bytes,
            last_counters.entity_page_reads,
            last_counters.entity_page_bytes,
            cold_counters.component_boundary_bytes,
            p95_ns <= 2_750_000 && total_owned_bytes <= V3_TOTAL_BYTES_TARGET,
        );
        assert_eq!(
            accepted.bytes.read(edit_offset as u64, 1).unwrap(),
            [last_replacement]
        );
        assert!(p95_ns <= 2_750_000, "JSON v3 must clear 2x latency");
        assert!(
            total_owned_bytes <= V3_TOTAL_BYTES_TARGET,
            "JSON v3 must clear 3x resident owned payload"
        );
    }

    #[tokio::test]
    #[ignore = "manual release-mode 10 MiB CSV v3 cold-import scorecard"]
    async fn csv_v3_ten_mib_cold_import_benchmark() {
        const WARMUPS: usize = 2;
        const SAMPLES: usize = 20;
        const ROWS: usize = 220_000;
        const PAGE_BYTES: u32 = 256 * 1024;

        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3");
        let wasm = std::fs::read(wasm_path).expect("CSV v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 128 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(120_000),
                },
            )
            .await
            .expect("CSV v3 should compile");
        let bytes = csv_ten_mib_fixture();
        let descriptor = WasmV3FileDescriptor {
            path: Some("/ten-mib.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin_key: "plugin_csv_v3".to_owned(),
            generation: "csv-v3-generation".to_owned(),
        };
        let limits = WasmV3TransitionLimits {
            max_page_bytes: PAGE_BYTES,
            max_pages: 2_048,
            max_total_bytes: 256 * 1024 * 1024,
            deadline_nanoseconds: 120_000_000_000,
        };
        let mut samples = Vec::with_capacity(SAMPLES);
        let mut open_samples = Vec::with_capacity(SAMPLES);
        let mut drain_samples = Vec::with_capacity(SAMPLES);
        let mut finish_samples = Vec::with_capacity(SAMPLES);
        let mut guest_cursor_samples = Vec::with_capacity(SAMPLES);
        let mut packet_decode_samples = Vec::with_capacity(SAMPLES);
        let mut ordered_stage_samples = Vec::with_capacity(SAMPLES);
        let mut change_output_samples = Vec::with_capacity(SAMPLES);
        let mut guest_high_waters = Vec::with_capacity(SAMPLES);
        let mut resident_before_gc = Vec::with_capacity(SAMPLES);
        let mut resident_after_gc = Vec::with_capacity(SAMPLES);
        let mut logical_durable = Vec::with_capacity(SAMPLES);
        let mut boundaries = Vec::with_capacity(SAMPLES);

        for sample in 0..WARMUPS + SAMPLES {
            // Both the actor and arena are fresh for every sample. Importing
            // accepted file bytes into the host arena is setup, matching v2's
            // already-host-owned source bytes at the open-file boundary.
            let mut actor = factory
                .instantiate_actor()
                .await
                .expect("CSV v3 cold actor should instantiate");
            let store = ArenaStore::default();
            let imported = Root::import(
                store.clone(),
                "csv-v3-generation",
                &bytes,
                std::iter::empty(),
                std::iter::empty(),
            );
            let started = Instant::now();
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
                .expect("CSV v3 cold import should open");
            let opened_at = Instant::now();
            let mut changes = 0usize;
            while let Some(page) = actor
                .next_change_page(opened.transition, opened.changes, PAGE_BYTES)
                .await
                .expect("CSV v3 cold import changes should drain")
            {
                changes += page.len();
            }
            let drained_at = Instant::now();
            assert_eq!(changes, ROWS + 1);
            let (accepted, counters) = actor
                .finish_transition(opened.transition)
                .await
                .expect("CSV v3 cold import should finish");
            let finished_at = Instant::now();
            let elapsed = finished_at.duration_since(started).as_nanos() as u64;
            assert_eq!(accepted.bytes.len(), bytes.len() as u64);
            let before_gc = store.resident_page_bytes() as u64;
            store.retain_reachable(&accepted);
            let after_gc = store.resident_page_bytes() as u64;
            if sample >= WARMUPS {
                samples.push(elapsed);
                open_samples.push(opened_at.duration_since(started).as_nanos() as u64);
                drain_samples.push(drained_at.duration_since(opened_at).as_nanos() as u64);
                finish_samples.push(finished_at.duration_since(drained_at).as_nanos() as u64);
                guest_cursor_samples.push(counters.guest_change_cursor_nanoseconds);
                packet_decode_samples.push(counters.change_packet_decode_nanoseconds);
                ordered_stage_samples.push(counters.ordered_entity_stage_nanoseconds);
                change_output_samples.push(counters.change_output_nanoseconds);
                guest_high_waters.push(counters.guest_linear_memory_high_water_bytes);
                resident_before_gc.push(before_gc);
                resident_after_gc.push(after_gc);
                logical_durable.push(store.unique_page_bytes() as u64);
                boundaries.push(counters.component_boundary_bytes);
            }
        }

        samples.sort_unstable();
        open_samples.sort_unstable();
        drain_samples.sort_unstable();
        finish_samples.sort_unstable();
        guest_cursor_samples.sort_unstable();
        packet_decode_samples.sort_unstable();
        ordered_stage_samples.sort_unstable();
        change_output_samples.sort_unstable();
        guest_high_waters.sort_unstable();
        resident_before_gc.sort_unstable();
        resident_after_gc.sort_unstable();
        logical_durable.sort_unstable();
        boundaries.sort_unstable();
        let p95_guest = percentile(&guest_high_waters, 95);
        let p95_resident_before_gc = percentile(&resident_before_gc, 95);
        let p95_resident_after_gc = percentile(&resident_after_gc, 95);
        eprintln!(
            "csv_v3_cold_import bytes={} rows={ROWS} warmups={WARMUPS} samples={SAMPLES} \
             p50_ms={:.3} p95_ms={:.3} p95_guest_high_water_bytes={} \
             p95_open_ms={:.3} p95_drain_ms={:.3} p95_finish_ms={:.3} \
             p95_guest_cursor_ms={:.3} p95_packet_decode_ms={:.3} \
             p95_ordered_stage_ms={:.3} p95_change_output_ms={:.3} \
             p95_host_resident_before_gc_bytes={} p95_host_resident_after_gc_bytes={} \
             p95_total_owned_before_gc_bytes={} p95_total_owned_after_gc_bytes={} \
             p95_logical_durable_bytes={} p95_boundary_bytes={}",
            bytes.len(),
            percentile(&samples, 50) as f64 / 1_000_000.0,
            percentile(&samples, 95) as f64 / 1_000_000.0,
            p95_guest,
            percentile(&open_samples, 95) as f64 / 1_000_000.0,
            percentile(&drain_samples, 95) as f64 / 1_000_000.0,
            percentile(&finish_samples, 95) as f64 / 1_000_000.0,
            percentile(&guest_cursor_samples, 95) as f64 / 1_000_000.0,
            percentile(&packet_decode_samples, 95) as f64 / 1_000_000.0,
            percentile(&ordered_stage_samples, 95) as f64 / 1_000_000.0,
            percentile(&change_output_samples, 95) as f64 / 1_000_000.0,
            p95_resident_before_gc,
            p95_resident_after_gc,
            p95_guest.saturating_add(p95_resident_before_gc),
            p95_guest.saturating_add(p95_resident_after_gc),
            percentile(&logical_durable, 95),
            percentile(&boundaries, 95),
        );
    }

    #[tokio::test]
    #[ignore = "manual release-mode CSV v3 affected-page scorecard"]
    async fn csv_v3_ten_mib_affected_page_benchmark() {
        const WARMUPS: usize = 100;
        const SAMPLES: usize = 2_000;
        const ROWS: usize = 220_000;
        const V2_CSV_TOTAL_BYTES: usize = 72_677_056;

        let wasm_path = env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V3_plugin_csv_v3");
        let wasm = std::fs::read(wasm_path).expect("CSV v3 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("Wasmtime runtime should initialize");
        let factory = runtime
            .compile_component_v3(
                wasm,
                WasmLimits {
                    max_memory_bytes: 128 * 1024 * 1024,
                    max_fuel: None,
                    timeout_ms: Some(120_000),
                },
            )
            .await
            .expect("CSV v3 should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("CSV v3 should instantiate");
        let bytes = csv_ten_mib_fixture();
        let edit_offset = 110_000usize * 49 + 16;
        assert_eq!(bytes[edit_offset], b'1');
        let store = ArenaStore::default();
        let imported = Root::import(
            store.clone(),
            "csv-v3-generation",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let descriptor = WasmV3FileDescriptor {
            path: Some("/ten-mib.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin_key: "plugin_csv_v3".to_owned(),
            generation: "csv-v3-generation".to_owned(),
        };
        let limits = WasmV3TransitionLimits {
            max_page_bytes: 1024 * 1024,
            max_pages: 2_048,
            max_total_bytes: 256 * 1024 * 1024,
            deadline_nanoseconds: 120_000_000_000,
        };
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
            .expect("cold import CSV v3");
        let mut cold_changes = 0usize;
        while let Some(page) = actor
            .next_change_page(opened.transition, opened.changes, 1024 * 1024)
            .await
            .expect("drain cold CSV changes")
        {
            cold_changes += page.len();
        }
        assert_eq!(cold_changes, ROWS + 1);
        let (mut accepted, cold_counters) = actor
            .finish_transition(opened.transition)
            .await
            .expect("commit cold CSV import");
        drop(actor);
        store.retain_reachable(&accepted);
        store.evict_resident_pages();
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("cold instantiate CSV v3 after eviction");

        let mut samples = Vec::with_capacity(SAMPLES);
        let mut replacement = b'x';
        let mut last_counters = WasmV3TransitionCounters::default();
        for sample in 0..WARMUPS + SAMPLES {
            let mut transaction = accepted.transaction();
            transaction.edit_bytes(ByteEdit {
                offset: edit_offset as u64,
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
                            offset: edit_offset as u64,
                            delete_len: 1,
                            insert: WasmV3InputBytes::Inline(vec![replacement]),
                        }],
                        successor: transaction,
                        creates: creates(),
                    },
                )
                .await
                .expect("run CSV v3 edit");
            let mut changes = 0usize;
            while let Some(page) = actor
                .next_change_page(updated.transition, updated.changes, 1024 * 1024)
                .await
                .expect("drain warm CSV changes")
            {
                changes += page.len();
            }
            assert_eq!(changes, 1);
            (accepted, last_counters) = actor
                .finish_transition(updated.transition)
                .await
                .expect("commit warm CSV successor");
            if sample >= WARMUPS {
                samples.push(started.elapsed().as_nanos() as u64);
            }
            replacement = if replacement == b'x' { b'y' } else { b'x' };
        }
        samples.sort_unstable();
        let p50_ns = percentile(&samples, 50);
        let p95_ns = percentile(&samples, 95);
        let total_owned_bytes = store
            .resident_page_bytes()
            .saturating_add(last_counters.guest_linear_memory_high_water_bytes as usize);
        eprintln!(
            "csv_v3_affected_page bytes={} rows={ROWS} warmups={WARMUPS} samples={SAMPLES} \
             p50_ms={:.3} p95_ms={:.3} host_resident_bytes={} host_logical_durable_bytes={} \
             guest_high_water_bytes={} total_owned_bytes={} memory_reduction_vs_v2={:.3} \
             boundary_bytes={} entity_page_reads={} entity_page_bytes={} cold_boundary_bytes={}",
            bytes.len(),
            p50_ns as f64 / 1_000_000.0,
            p95_ns as f64 / 1_000_000.0,
            store.resident_page_bytes(),
            store.unique_page_bytes(),
            last_counters.guest_linear_memory_high_water_bytes,
            total_owned_bytes,
            V2_CSV_TOTAL_BYTES as f64 / total_owned_bytes as f64,
            last_counters.component_boundary_bytes,
            last_counters.entity_page_reads,
            last_counters.entity_page_bytes,
            cold_counters.component_boundary_bytes,
        );
        assert!(matches!(
            accepted
                .bytes
                .read(edit_offset as u64, 1)
                .unwrap()
                .as_slice(),
            [b'x'] | [b'y']
        ));
    }

    fn percentile(samples: &[u64], percentile: usize) -> u64 {
        let rank = (samples.len() * percentile).div_ceil(100);
        samples[rank.saturating_sub(1)]
    }

    fn json_ten_mib_fixture() -> (Vec<u8>, usize) {
        const PROPERTY_COUNT: usize = 39_870;
        const TARGET_BYTES: usize = 10 * 1024 * 1024;
        const BASE_MEMBER_BYTES: usize = 44;
        let base_bytes = 2 + PROPERTY_COUNT * BASE_MEMBER_BYTES + PROPERTY_COUNT - 1;
        let padding = TARGET_BYTES - base_bytes;
        let padding_per_property = padding / PROPERTY_COUNT;
        let extra_padding_properties = padding % PROPERTY_COUNT;
        let mut bytes = Vec::with_capacity(TARGET_BYTES);
        let mut state = 0x6a73_6f6e_2d31_306du64;
        let edited_index = PROPERTY_COUNT / 2;
        let mut edit_offset = None;
        bytes.push(b'{');
        for index in 0..PROPERTY_COUNT {
            if index > 0 {
                bytes.push(b',');
            }
            state = splitmix64(state);
            let first = state;
            state = splitmix64(state);
            let second = state as u32;
            bytes.extend_from_slice(
                format!("\"property_{index:06}\":\"{first:016x}{second:08x}").as_bytes(),
            );
            if index == edited_index {
                edit_offset = Some(bytes.len() - 24);
            }
            let property_padding =
                padding_per_property + usize::from(index < extra_padding_properties);
            bytes.extend(std::iter::repeat_n(b'f', property_padding));
            bytes.push(b'"');
        }
        bytes.push(b'}');
        assert_eq!(bytes.len(), TARGET_BYTES);
        (bytes, edit_offset.unwrap())
    }

    fn csv_ten_mib_fixture() -> Vec<u8> {
        const ROWS: usize = 220_000;
        const LONG: &[u8] = b"000000000000000,1111111111,2222222222,3333333333\n";
        const SHORT: &[u8] = b"00000000000000,1111111111,2222222222,3333333333\n";
        let mut bytes = Vec::with_capacity(10_680_000);
        for index in 0..ROWS {
            bytes.extend_from_slice(if index < 120_000 { LONG } else { SHORT });
        }
        assert_eq!(bytes.len(), 10_680_000);
        bytes
    }

    fn splitmix64(mut state: u64) -> u64 {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
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
