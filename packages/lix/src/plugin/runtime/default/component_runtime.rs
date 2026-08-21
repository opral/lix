//! Host-owned arena runtime for the row-first Component API.
//!
//! File bytes and opaque plugin state live in immutable host roots; one
//! exported guest call reads sparse ranges and pushes bounded semantic pages.

// `WasmRowKey`'s only interior mutability is a JSONB decode cache; it cannot
// change equality or ordering after insertion into these collections.
#![allow(clippy::mutable_key_type)]

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::plugin::wire::typed::{
    self as typed_wire, Mutation as BorrowedTypedMutation, OwnedMutation as TypedMutation,
};
use async_trait::async_trait;
use bytes::Bytes;
use lix::plugin::runtime::v1::{
    ByteEdit as ArenaByteEdit, Root as ArenaRoot, Store as ArenaStore,
    Transaction as ArenaTransaction,
};
use lix::plugin::runtime::{
    CURRENT_PACKET_FORMAT, PluginCapabilities, WasmByteOutputsHandle, WasmChangeCursorHandle,
    WasmChangeEffect, WasmChangePage, WasmColdFileUpdate, WasmColumnMergeCursorHandle,
    WasmColumnMergeResult as RuntimeColumnMergeResult, WasmColumnMergeResultPage,
    WasmColumnMergeTransition, WasmColumnMergeUpdate, WasmComponentActor, WasmComponentFactory,
    WasmCreateContext, WasmDocumentCheckpoint, WasmDocumentHandle, WasmEditCursorHandle,
    WasmEditPage, WasmFileTransition, WasmFileUpdate, WasmGuestBytes, WasmGuestColumnValue,
    WasmGuestRowPayload, WasmHostBytes, WasmHostColumnMerge, WasmInputBytes, WasmOpenFileInput,
    WasmOpenRowsInput, WasmOutputRange, WasmOutputSplice, WasmRow, WasmRowChange, WasmRowChanges,
    WasmRowKey, WasmRowTransition, WasmRowUpdate, WasmTransitionCounters, WasmTransitionHandle,
    WasmTransitionLimits, WasmTypedRow,
};
use lix::wasm::WasmLimits;
use lix::{LixError, SharedStr};
use wasmtime::Store;
use wasmtime::component::{Component, Linker, Resource, ResourceTable};

use super::{
    CompileProfile, CompiledComponentKey, TimeoutTickerLease, WasiHostState, WasmtimePluginRuntime,
    add_to_linker_sync, create_store, reset_store_limits, wasm_runtime_error,
};

// Warm transitions normally retain the engine's 2 MiB fixed page schedule.
// Cold admission may scale as high as 16 MiB so a valid single text row
// (for example a one-line source map) can cross the Component push sink.
const COMPONENT_MAX_BATCH_BYTES: u32 = 16 * 1024 * 1024;
// Admit one capability export per compiled plugin component before allocating its
// Wasmtime Store. This bounds both actor and pushed-page residency without
// creating an executor thread or serializing different plugin types.
const COMPONENT_MAX_CONCURRENT_EXECUTIONS_PER_COMPONENT: usize = 1;

fn take_borrowed_resource<T: Send + 'static>(
    table: &mut ResourceTable,
    resource: Resource<T>,
    context: &str,
) -> Result<T, LixError> {
    table
        .delete(resource)
        .map_err(|error| component_error(format!("{context}: {error}")))
}

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

pub struct SnapshotResource {
    root: ArenaRoot,
    state: SharedTransitionState,
}

pub struct TransitionResource {
    transaction: ArenaTransaction,
    state: SharedTransitionState,
}

pub struct ConflictSourceResource {
    state: SharedResolutionState,
}

pub struct RowSourceResource {
    state: SharedRowChangeState,
}

pub struct ResolutionSinkResource {
    state: SharedResolutionState,
}

type SharedTransitionState = Arc<Mutex<TransitionState>>;
type SharedResolutionState = Arc<Mutex<ResolutionState>>;
type SharedRowChangeState = Arc<Mutex<RowChangeState>>;
type SharedByteBudget = Arc<Mutex<u64>>;

struct TransitionState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: SharedByteBudget,
    pages: VecDeque<PendingChangePage>,
    replace_all_rows: bool,
    counters: WasmTransitionCounters,
    allow_file_replacement: bool,
    file_replacement: Option<PendingFileReplacement>,
    file_edits: Vec<PendingFileEdit>,
}

struct PendingFileEdit {
    offset: u64,
    delete_len: u64,
    insert: Bytes,
}

struct PendingFileReplacement {
    expected_len: u64,
    bytes: Vec<u8>,
    complete: bool,
}

struct RowChangeState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: SharedByteBudget,
    source: RowChangeInputSource,
    track_seen_row_keys: bool,
    seen_row_keys: BTreeSet<WasmRowKey>,
    counters: WasmTransitionCounters,
}

enum RowChangeInputSource {
    Rows(Box<dyn lix::plugin::runtime::WasmRowSource>),
    Changes(Box<dyn lix::plugin::runtime::WasmRowChangeSource>),
}

struct ResolutionState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: u64,
    conflicts: Vec<WasmHostColumnMerge>,
    resolutions: Vec<ComponentResolution>,
    pending: Option<PendingReplacement>,
    counters: WasmTransitionCounters,
}

enum ComponentResolution {
    UseLww,
    Replace(Option<Bytes>),
}

struct PendingReplacement {
    ordinal: u32,
    expected_len: Option<u64>,
    bytes: Vec<u8>,
}

/// Host-owned wire pages retained until the engine asks for the next page.
///
/// The first Component implementation decoded every pushed page immediately and retained
/// the resulting row graph until the guest export returned. Large imports
/// therefore held all generic row objects plus the gradually constructed
/// canonical output. Keeping the bounded wire pages defers ownership expansion
/// to the existing one-page-at-a-time drain.
#[derive(Clone)]
enum PendingChangePage {
    Typed {
        schema_key: String,
        schema_fingerprint: [u8; 32],
        mutations: Vec<TypedMutation>,
    },
    Decoded(WasmChangePage),
}

impl PendingChangePage {
    fn decode(self) -> Result<WasmChangePage, LixError> {
        match self {
            Self::Typed {
                schema_key,
                schema_fingerprint,
                mutations,
            } => decode_typed_change_page(schema_key, schema_fingerprint, mutations),
            Self::Decoded(page) => Ok(page),
        }
    }
}

fn decode_typed_change_page(
    schema_key: String,
    schema_fingerprint: [u8; 32],
    mutations: Vec<TypedMutation>,
) -> Result<WasmChangePage, LixError> {
    let schema_key = SharedStr::from(schema_key);
    let mut changes = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        match mutation {
            TypedMutation::Create { local_ref, row } => changes.push(WasmRowChange::Create {
                schema_key: schema_key.clone(),
                local_ref: u64::from(local_ref),
                resolved_key: None,
                payload: WasmGuestRowPayload::Typed(Arc::new(WasmTypedRow {
                    schema_fingerprint,
                    row_pk: Arc::from([]),
                    row,
                    native_payload: std::sync::OnceLock::new(),
                    boundary_create_validation: std::sync::OnceLock::new(),
                })),
            }),
            TypedMutation::Upsert {
                row_pk,
                row,
                effect,
            } => {
                let row_pk: Arc<[lix_schema::Value]> = row_pk.into();
                let key =
                    typed_row_key(schema_key.clone(), schema_fingerprint, Arc::clone(&row_pk))?;
                changes.push(WasmRowChange::Upsert {
                    row: WasmRow {
                        key,
                        payload: WasmGuestRowPayload::Typed(Arc::new(WasmTypedRow {
                            schema_fingerprint,
                            row_pk,
                            row,
                            native_payload: std::sync::OnceLock::new(),
                            boundary_create_validation: std::sync::OnceLock::new(),
                        })),
                    },
                    effect: match effect {
                        typed_wire::ChangeEffect::Content => WasmChangeEffect::Content,
                        typed_wire::ChangeEffect::FormatOnly => WasmChangeEffect::FormatOnly,
                    },
                });
            }
            TypedMutation::Delete { row_pk } => {
                changes.push(WasmRowChange::Delete(typed_row_key(
                    schema_key.clone(),
                    schema_fingerprint,
                    row_pk.into(),
                )?));
            }
        }
    }
    if changes.is_empty() {
        return Err(component_error("typed row page contains no records"));
    }
    Ok(WasmChangePage {
        format_version: CURRENT_PACKET_FORMAT,
        changes: WasmRowChanges { changes },
        outputs: None,
    })
}

fn typed_row_key(
    schema_key: SharedStr,
    schema_fingerprint: [u8; 32],
    row_pk: Arc<[lix_schema::Value]>,
) -> Result<WasmRowKey, LixError> {
    if row_pk.is_empty() {
        return Err(component_error("typed row identity must not be empty"));
    }
    WasmRowKey::from_typed_parts(schema_key, schema_fingerprint, row_pk)
}

fn typed_input_mutations<'a>(
    changes: &'a [WasmRowChange<WasmHostBytes>],
) -> Result<Option<(String, [u8; 32], Vec<BorrowedTypedMutation<'a>>)>, LixError> {
    if changes.is_empty() {
        return Ok(None);
    }
    let mut schema = None;
    let mut mutations = Vec::with_capacity(changes.len());
    for change in changes {
        let (schema_key, fingerprint, mutation) = match change {
            WasmRowChange::Create {
                schema_key,
                local_ref,
                payload: WasmHostBytes::Typed(row),
                ..
            } => (
                schema_key.as_str(),
                row.schema_fingerprint,
                BorrowedTypedMutation::Create {
                    local_ref: u32::try_from(*local_ref)
                        .map_err(|_| component_error("typed create reference exceeds u32"))?,
                    row: &row.row,
                },
            ),
            WasmRowChange::Upsert {
                row:
                    WasmRow {
                        key: _,
                        payload: WasmHostBytes::Typed(row),
                    },
                effect,
            } => (
                "",
                row.schema_fingerprint,
                BorrowedTypedMutation::Upsert {
                    row_pk: &row.row_pk,
                    row: &row.row,
                    effect: match effect {
                        WasmChangeEffect::Content => typed_wire::ChangeEffect::Content,
                        WasmChangeEffect::FormatOnly => typed_wire::ChangeEffect::FormatOnly,
                    },
                },
            ),
            WasmRowChange::Delete(key) => (
                key.schema_key.as_str(),
                key.schema_fingerprint,
                BorrowedTypedMutation::Delete {
                    row_pk: &key.row_pk,
                },
            ),
        };
        let schema_key = if schema_key.is_empty() {
            match change {
                WasmRowChange::Upsert { row, .. } => row.key.schema_key.as_str(),
                _ => schema_key,
            }
        } else {
            schema_key
        };
        if let Some((expected_key, expected_fingerprint)) = schema.as_ref() {
            if expected_key != schema_key || *expected_fingerprint != fingerprint {
                return Err(component_error(
                    "one typed input page must contain one schema fingerprint",
                ));
            }
        } else {
            schema = Some((schema_key.to_owned(), fingerprint));
        }
        mutations.push(mutation);
    }
    let (schema_key, fingerprint) = schema.expect("non-empty typed page has a schema");
    Ok(Some((schema_key, fingerprint, mutations)))
}

fn append_replace_all_deletes(
    state: &mut TransitionState,
    mut prior_keys: BTreeSet<WasmRowKey>,
) -> Result<(), LixError> {
    if !state.replace_all_rows || prior_keys.is_empty() {
        return Ok(());
    }
    for page in state.pages.iter().cloned() {
        for change in page.decode()?.changes.changes {
            match change {
                WasmRowChange::Upsert { row, .. } => {
                    prior_keys.remove(&row.key);
                }
                WasmRowChange::Delete(key) => {
                    prior_keys.remove(&key);
                }
                WasmRowChange::Create { .. } => {}
            }
        }
    }
    if !prior_keys.is_empty() {
        state
            .pages
            .push_back(PendingChangePage::Decoded(WasmChangePage {
                format_version: CURRENT_PACKET_FORMAT,
                changes: WasmRowChanges {
                    changes: prior_keys.into_iter().map(WasmRowChange::Delete).collect(),
                },
                outputs: None,
            }));
    }
    Ok(())
}

fn component_limit(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

fn validate_source_admission(length: u64, limits: WasmTransitionLimits) -> Result<(), LixError> {
    if length > limits.max_total_bytes {
        return Err(component_limit(
            "component source exceeds max-total-bytes before admission",
        ));
    }
    Ok(())
}

impl TransitionState {
    fn new(
        limits: WasmTransitionLimits,
        _creates: WasmCreateContext,
        allow_file_replacement: bool,
        total_bytes: Option<SharedByteBudget>,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            total_bytes: total_bytes.unwrap_or_default(),
            pages: VecDeque::new(),
            replace_all_rows: false,
            counters: WasmTransitionCounters {
                guest_export_calls: 1,
                ..WasmTransitionCounters::default()
            },
            allow_file_replacement,
            file_replacement: None,
            file_edits: Vec::new(),
        })
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component transition deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn take_pages(&mut self) -> Result<VecDeque<PendingChangePage>, LixError> {
        Ok(std::mem::take(&mut self.pages))
    }

    fn charge_page(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component source or sink page exceeds max-page-bytes".to_owned(),
            ));
        }
        self.charge_total(bytes)
    }

    fn charge_source(
        &mut self,
        bytes: usize,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        self.charge_total(bytes)
    }

    fn charge_total(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        let mut total_bytes = self
            .total_bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *total_bytes = total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "component transition byte count overflowed".to_owned(),
            )
        })?;
        if *total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component transition exceeds max-total-bytes".to_owned(),
            ));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .saturating_add(bytes as u64);
        Ok(())
    }
}

impl ResolutionState {
    fn new(
        limits: WasmTransitionLimits,
        conflicts: Vec<WasmHostColumnMerge>,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            total_bytes: 0,
            conflicts,
            resolutions: Vec::new(),
            pending: None,
            counters: WasmTransitionCounters {
                guest_export_calls: 1,
                conflict_resolution_calls: 1,
                ..WasmTransitionCounters::default()
            },
        })
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component conflict deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component conflict chunk exceeds max-page-bytes".to_owned(),
            ));
        }
        self.total_bytes = self.total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "component conflict byte count overflowed".to_owned(),
            )
        })?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component conflict transition exceeds max-total-bytes".to_owned(),
            ));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .saturating_add(bytes as u64);
        Ok(())
    }

    fn next_ordinal(&self) -> Result<u32, bindings::lix::plugin::host::HostError> {
        u32::try_from(self.resolutions.len()).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "component column merge result count exceeds u32".to_owned(),
            )
        })
    }

    fn validate_ordinal(
        &self,
        ordinal: u32,
    ) -> Result<usize, bindings::lix::plugin::host::HostError> {
        let expected = self.next_ordinal()?;
        if ordinal != expected {
            return Err(bindings::lix::plugin::host::HostError::Rejected(format!(
                "component column merge result ordinal {ordinal}, expected {expected}"
            )));
        }
        let index = ordinal as usize;
        if self.conflicts.get(index).is_none() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component column merger returned excess output".to_owned(),
            ));
        }
        Ok(index)
    }
}

impl RowChangeState {
    fn from_rows(
        limits: WasmTransitionLimits,
        source: Box<dyn lix::plugin::runtime::WasmRowSource>,
        total_bytes: SharedByteBudget,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            total_bytes,
            source: RowChangeInputSource::Rows(source),
            track_seen_row_keys: true,
            seen_row_keys: BTreeSet::new(),
            counters: WasmTransitionCounters::default(),
        })
    }

    fn from_rows_without_key_inventory(
        limits: WasmTransitionLimits,
        source: Box<dyn lix::plugin::runtime::WasmRowSource>,
        total_bytes: SharedByteBudget,
    ) -> Result<Self, LixError> {
        let mut state = Self::from_rows(limits, source, total_bytes)?;
        state.track_seen_row_keys = false;
        Ok(state)
    }

    fn from_changes(
        limits: WasmTransitionLimits,
        source: Box<dyn lix::plugin::runtime::WasmRowChangeSource>,
        total_bytes: SharedByteBudget,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            total_bytes,
            source: RowChangeInputSource::Changes(source),
            track_seen_row_keys: false,
            seen_row_keys: BTreeSet::new(),
            counters: WasmTransitionCounters::default(),
        })
    }

    fn next_page(
        &mut self,
        max_bytes: u32,
    ) -> Result<Option<Vec<WasmRowChange<WasmHostBytes>>>, LixError> {
        match &mut self.source {
            RowChangeInputSource::Rows(source) => Ok(source.next_page(max_bytes)?.map(|page| {
                if self.track_seen_row_keys {
                    self.seen_row_keys
                        .extend(page.rows.iter().map(|row| row.key.clone()));
                }
                page.rows
                    .into_iter()
                    .map(|row| WasmRowChange::Upsert {
                        row,
                        effect: WasmChangeEffect::Content,
                    })
                    .collect()
            })),
            RowChangeInputSource::Changes(source) => {
                Ok(source.next_page(max_bytes)?.map(|page| page.changes))
            }
        }
    }

    fn drain_complete_row_keys(&mut self) -> Result<BTreeSet<WasmRowKey>, LixError> {
        while self.next_page(self.limits.max_page_bytes)?.is_some() {}
        Ok(std::mem::take(&mut self.seen_row_keys))
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component row transition deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component row-change chunk exceeds max-page-bytes".to_owned(),
            ));
        }
        self.charge_total(bytes)
    }

    fn charge_total(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        let mut total_bytes = self
            .total_bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *total_bytes = total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "component row-change byte count overflowed".to_owned(),
            )
        })?;
        if *total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component row transition exceeds max-total-bytes".to_owned(),
            ));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .saturating_add(bytes as u64);
        Ok(())
    }
}

fn merge_row_input_profile(target: &mut WasmTransitionCounters, source: &WasmTransitionCounters) {
    target.row_input_pages = target
        .row_input_pages
        .saturating_add(source.row_input_pages);
    target.row_input_records = target
        .row_input_records
        .saturating_add(source.row_input_records);
    target.row_input_wire_bytes = target
        .row_input_wire_bytes
        .saturating_add(source.row_input_wire_bytes);
    target.row_input_attachment_reads = target
        .row_input_attachment_reads
        .saturating_add(source.row_input_attachment_reads);
    target.row_input_attachment_bytes = target
        .row_input_attachment_bytes
        .saturating_add(source.row_input_attachment_bytes);
    target.typed_row_encode_records = target
        .typed_row_encode_records
        .saturating_add(source.typed_row_encode_records);
    target.typed_row_encode_bytes = target
        .typed_row_encode_bytes
        .saturating_add(source.typed_row_encode_bytes);
    target.row_page_callback_calls = target
        .row_page_callback_calls
        .saturating_add(source.row_page_callback_calls);
    target.row_input_page_eof_callbacks = target
        .row_input_page_eof_callbacks
        .saturating_add(source.row_input_page_eof_callbacks);
}

fn conflict_value<'a>(
    conflict: &'a WasmHostColumnMerge,
    side: bindings::lix::plugin::host::MergeSide,
) -> Option<&'a lix_schema::Value> {
    match side {
        bindings::lix::plugin::host::MergeSide::Base => conflict.base.as_ref(),
        bindings::lix::plugin::host::MergeSide::A => conflict.a.as_ref(),
        bindings::lix::plugin::host::MergeSide::B => conflict.b.as_ref(),
    }
}

fn conflict_row(
    conflict: &WasmHostColumnMerge,
    side: bindings::lix::plugin::host::MergeSide,
) -> &WasmTypedRow {
    match side {
        bindings::lix::plugin::host::MergeSide::Base => &conflict.base_row,
        bindings::lix::plugin::host::MergeSide::A => &conflict.a_row,
        bindings::lix::plugin::host::MergeSide::B => &conflict.b_row,
    }
}

fn read_encoded_bytes(bytes: &[u8], offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| component_error("component conflict range overflowed"))?;
    if end > bytes.len() as u64 {
        return Err(component_error("component conflict range is out of bounds"));
    }
    Ok(bytes[offset as usize..end as usize].to_vec())
}

fn encoded_merge_value(value: &lix_schema::Value) -> Result<Vec<u8>, LixError> {
    typed_wire::encode_value_bytes(value)
        .map_err(|error| component_error(format!("failed to encode typed merge value: {error:?}")))
}

fn encoded_merge_value_len(value: &lix_schema::Value) -> Result<u64, LixError> {
    typed_wire::encoded_value_size(value)
        .map(|length| length as u64)
        .map_err(|error| component_error(format!("failed to size typed merge value: {error:?}")))
}

fn encoded_merge_row(row: &WasmTypedRow) -> Result<Vec<u8>, LixError> {
    typed_wire::encode_row_bytes(&row.row)
        .map_err(|error| component_error(format!("failed to encode typed merge row: {error:?}")))
}

fn encoded_merge_row_len(row: &WasmTypedRow) -> Result<u64, LixError> {
    typed_wire::encoded_row_size(&row.row)
        .map(|length| length as u64)
        .map_err(|error| component_error(format!("failed to size typed merge row: {error:?}")))
}

#[cfg(test)]
fn create_context_from_uuid(value: uuid::Uuid) -> WasmCreateContext {
    let bytes = value.into_bytes();
    WasmCreateContext {
        high: u64::from_be_bytes(bytes[..8].try_into().expect("eight UUID bytes")),
        low: u32::from_be_bytes(bytes[8..12].try_into().expect("four UUID bytes")),
    }
}

#[cfg(test)]
fn create_context_from_generated_row(
    generated_schema: &str,
    row: &WasmRow<WasmHostBytes>,
) -> Option<WasmCreateContext> {
    if row.key.schema_key.as_str() != generated_schema {
        return None;
    }
    let [lix_schema::Value::Uuid(id)] = row.key.row_pk.as_ref() else {
        return None;
    };
    Some(create_context_from_uuid(*id))
}

fn ensure_source_page(
    length: u32,
    max_page_bytes: u32,
) -> Result<(), bindings::lix::plugin::host::HostError> {
    if length > max_page_bytes {
        return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
            "component source read exceeds max-page-bytes".to_owned(),
        ));
    }
    Ok(())
}

impl bindings::lix::plugin::host::HostSnapshot for WasiHostState {
    fn file_len(&mut self, resource: Resource<SnapshotResource>) -> u64 {
        self.table
            .get(&resource)
            .expect("component root resource must be live")
            .root
            .bytes
            .len()
    }

    fn read_file(
        &mut self,
        resource: Resource<SnapshotResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, bindings::lix::plugin::host::HostError> {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
        ensure_source_page(
            length,
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .limits
                .max_page_bytes,
        )?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        if end > root.bytes.len() {
            return Err(bindings::lix::plugin::host::HostError::InvalidRange);
        }
        let bytes = root
            .bytes
            .read(offset, u64::from(length))
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        if bytes.len() != length as usize {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component root returned a short read".to_owned(),
            ));
        }
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_source(bytes.len())?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(bytes.len() as u64);
        state.counters.file_read_calls = state.counters.file_read_calls.saturating_add(1);
        state.counters.file_bytes_read = state
            .counters
            .file_bytes_read
            .saturating_add(bytes.len() as u64);
        Ok(bytes)
    }

    fn read_state(
        &mut self,
        resource: Resource<SnapshotResource>,
        key: Vec<u8>,
        offset: u64,
        max_bytes: u32,
    ) -> Result<
        Option<bindings::lix::plugin::host::RecordChunk>,
        bindings::lix::plugin::host::HostError,
    > {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
        ensure_source_page(
            max_bytes,
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .limits
                .max_page_bytes,
        )?;
        let total_len = root.state.value_len(&key);
        let Some(total_len) = total_len else {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_source(key.len())?;
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
            state.counters.state_read_calls = state.counters.state_read_calls.saturating_add(1);
            state.counters.state_key_bytes = state
                .counters
                .state_key_bytes
                .saturating_add(key.len() as u64);
            return Ok(None);
        };
        if offset > total_len {
            return Err(bindings::lix::plugin::host::HostError::InvalidRange);
        }
        let length = u64::from(max_bytes).min(total_len - offset);
        let value = root
            .state
            .read(&key, offset, length)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        let bytes = value.ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "component snapshot record disappeared during read".to_owned(),
            )
        })?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_source(key.len().saturating_add(bytes.len()))?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(bytes.len() as u64);
        state.counters.state_read_calls = state.counters.state_read_calls.saturating_add(1);
        state.counters.state_key_bytes = state
            .counters
            .state_key_bytes
            .saturating_add(key.len() as u64);
        state.counters.state_value_bytes_read = state
            .counters
            .state_value_bytes_read
            .saturating_add(bytes.len() as u64);
        Ok(Some(bindings::lix::plugin::host::RecordChunk {
            total_len,
            bytes,
        }))
    }

    fn drop(&mut self, resource: Resource<SnapshotResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostTransition for WasiHostState {
    fn max_batch_bytes(&mut self, resource: Resource<TransitionResource>) -> u32 {
        self.table
            .get(&resource)
            .expect("component transition resource must be live")
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits
            .max_page_bytes
    }

    fn put_state(
        &mut self,
        resource: Resource<TransitionResource>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        if key.starts_with(b"\0lix/") {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component host-reserved state key".to_owned(),
            ));
        }
        let state = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_page(key.len().saturating_add(value.len()))?;
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        self.table
            .get_mut(&resource)
            .map_err(host_table_error)?
            .transaction
            .put_state(key, value);
        Ok(())
    }

    fn delete_state(
        &mut self,
        resource: Resource<TransitionResource>,
        key: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        if key.starts_with(b"\0lix/") {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component host-reserved state key".to_owned(),
            ));
        }
        let state = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_page(key.len())?;
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        self.table
            .get_mut(&resource)
            .map_err(host_table_error)?
            .transaction
            .delete_state(key);
        Ok(())
    }

    fn emit_rows(
        &mut self,
        resource: Resource<TransitionResource>,
        page: bindings::lix::plugin::host::RowPage,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let state = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        state.counters.row_page_callback_calls =
            state.counters.row_page_callback_calls.saturating_add(1);
        let attachment_bytes = page
            .attachments
            .iter()
            .map(|attachment| attachment.len() as u64)
            .sum::<u64>();
        let attachment_count = page.attachments.len() as u64;
        if state
            .counters
            .row_output_attachment_writes
            .saturating_add(page.attachments.len() as u64)
            > u64::from(state.limits.max_attachment_refs)
        {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "typed row attachment count exceeds its transition limit".to_owned(),
            ));
        }
        let page_wire_bytes = (page.payload.len() as u64).saturating_add(attachment_bytes);
        if state.counters.packet_pages.saturating_add(1) > u64::from(state.limits.max_pages) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "row page count exceeds its transition limit".to_owned(),
            ));
        }
        // The framed page remains independently bounded. Attachments are
        // charged against the aggregate transition budget so one native value
        // may exceed a page without reintroducing inline JSON chunking.
        state.charge_page(page.payload.len())?;
        state.charge_total(page.attachments.iter().map(Vec::len).sum())?;
        let bindings::lix::plugin::host::RowPage {
            payload,
            attachments,
        } = page;
        let decode_started = Instant::now();
        let (schema_key, schema_fingerprint, mutations) =
            typed_wire::decode_page_parts_owned(payload, attachments).map_err(|error| {
                bindings::lix::plugin::host::HostError::Rejected(format!(
                    "invalid typed row page: {error:?}"
                ))
            })?;
        state.counters.typed_row_decode_nanos = state
            .counters
            .typed_row_decode_nanos
            .saturating_add(u64::try_from(decode_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let record_count = u32::try_from(mutations.len()).map_err(|_| {
            bindings::lix::plugin::host::HostError::Rejected(
                "typed row page record count exceeds u32".to_owned(),
            )
        })?;
        let total_record_count = record_count;
        state.counters.typed_row_decode_records = state
            .counters
            .typed_row_decode_records
            .saturating_add(u64::from(record_count));
        state.counters.typed_row_decode_bytes = state
            .counters
            .typed_row_decode_bytes
            .saturating_add(page_wire_bytes);
        let pending = PendingChangePage::Typed {
            schema_key,
            schema_fingerprint,
            mutations,
        };
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.packet_pages = state.counters.packet_pages.saturating_add(1);
        state.counters.packet_records = state
            .counters
            .packet_records
            .saturating_add(u64::from(total_record_count));
        state.counters.row_output_pages = state.counters.row_output_pages.saturating_add(1);
        state.counters.row_output_records = state
            .counters
            .row_output_records
            .saturating_add(u64::from(total_record_count));
        state.counters.row_output_wire_bytes = state
            .counters
            .row_output_wire_bytes
            .saturating_add(page_wire_bytes);
        state.counters.row_output_attachment_writes = state
            .counters
            .row_output_attachment_writes
            .saturating_add(attachment_count);
        state.counters.row_output_attachment_bytes = state
            .counters
            .row_output_attachment_bytes
            .saturating_add(attachment_bytes);
        state.pages.push_back(pending);
        Ok(())
    }

    fn replace_all_rows(
        &mut self,
        resource: Resource<TransitionResource>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        if state.replace_all_rows {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "replace-all-rows was already requested".to_owned(),
            ));
        }
        state.replace_all_rows = true;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn emit_file_edit(
        &mut self,
        resource: Resource<TransitionResource>,
        edit: bindings::lix::plugin::host::FileEdit,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        {
            let mut state = shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.check_active()?;
            if !state.allow_file_replacement {
                return Err(bindings::lix::plugin::host::HostError::Rejected(
                    "file edits are unavailable while parsing bytes".to_owned(),
                ));
            }
            if state.file_replacement.is_some() {
                return Err(bindings::lix::plugin::host::HostError::Rejected(
                    "file edits cannot be mixed with a file replacement".to_owned(),
                ));
            }
            if let Some(previous) = state.file_edits.last()
                && edit.offset <= previous.offset
            {
                return Err(bindings::lix::plugin::host::HostError::Rejected(
                    "file edits must have strictly increasing offsets".to_owned(),
                ));
            }
            state.charge_page(edit.insert.len())?;
            state.file_edits.push(PendingFileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: Bytes::from(edit.insert.clone()),
            });
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        self.table
            .get_mut(&resource)
            .map_err(host_table_error)?
            .transaction
            .edit_bytes(ArenaByteEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert,
            });
        Ok(())
    }

    fn begin_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
        total_length: u64,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        if !state.allow_file_replacement {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement is unavailable for byte transitions".to_owned(),
            ));
        }
        if state.file_replacement.is_some() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement is already open".to_owned(),
            ));
        }
        if !state.file_edits.is_empty() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement cannot be mixed with file edits".to_owned(),
            ));
        }
        if total_length > state.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "file replacement exceeds max-total-bytes".to_owned(),
            ));
        }
        let capacity = usize::try_from(total_length).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "file replacement exceeds host address space".to_owned(),
            )
        })?;
        state.file_replacement = Some(PendingFileReplacement {
            expected_len: total_length,
            bytes: Vec::with_capacity(capacity),
            complete: false,
        });
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn write_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
        chunk: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_page(chunk.len())?;
        let pending = state.file_replacement.as_mut().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "file replacement chunk has no open replacement".to_owned(),
            )
        })?;
        if pending.complete {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement is already complete".to_owned(),
            ));
        }
        let next_len = pending
            .bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "file replacement length overflowed".to_owned(),
                )
            })?;
        if next_len as u64 > pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement exceeds its declared length".to_owned(),
            ));
        }
        pending.bytes.extend_from_slice(&chunk);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn finish_file_replacement(
        &mut self,
        resource: Resource<TransitionResource>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        let pending = state.file_replacement.as_mut().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "file replacement finish has no open replacement".to_owned(),
            )
        })?;
        if pending.bytes.len() as u64 != pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "file replacement ended before its declared length".to_owned(),
            ));
        }
        pending.complete = true;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn drop(&mut self, resource: Resource<TransitionResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostRowSource for WasiHostState {
    fn next_page(
        &mut self,
        resource: Resource<RowSourceResource>,
        max_bytes: u32,
    ) -> Result<Option<bindings::lix::plugin::host::RowPage>, bindings::lix::plugin::host::HostError>
    {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        state.counters.row_page_callback_calls =
            state.counters.row_page_callback_calls.saturating_add(1);
        ensure_source_page(max_bytes, state.limits.max_page_bytes)?;
        let envelope_bytes = u32::try_from(
            crate::plugin::wire::typed_page_overhead("").expect("fixed page overhead"),
        )
        .expect("fixed page overhead fits u32");
        let payload_budget = max_bytes.checked_sub(envelope_bytes).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "row input page budget cannot hold its envelope".to_owned(),
            )
        })?;
        let Some(changes) = state
            .next_page(payload_budget)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?
        else {
            state.counters.row_input_page_eof_callbacks = state
                .counters
                .row_input_page_eof_callbacks
                .saturating_add(1);
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
            return Ok(None);
        };
        if changes.is_empty() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component row source returned an empty page".to_owned(),
            ));
        }
        if let Some((schema_key, schema_fingerprint, mutations)) =
            typed_input_mutations(&changes)
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?
        {
            let record_count = u32::try_from(mutations.len()).map_err(|_| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "typed row input record count exceeds u32".to_owned(),
                )
            })?;
            let (page, attachments) =
                typed_wire::encode_page_parts(&schema_key, &schema_fingerprint, &mutations)
                    .map_err(|error| {
                        bindings::lix::plugin::host::HostError::Rejected(format!(
                            "failed to encode typed row input page: {error:?}"
                        ))
                    })?;
            let page_bytes = page
                .len()
                .saturating_add(attachments.iter().map(Vec::len).sum::<usize>());
            if page.len() > max_bytes as usize {
                return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                    "typed row input page exceeds the requested byte limit".to_owned(),
                ));
            }
            if state
                .counters
                .row_input_attachment_reads
                .saturating_add(attachments.len() as u64)
                > u64::from(state.limits.max_attachment_refs)
            {
                return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                    "typed row input attachment count exceeds its transition limit".to_owned(),
                ));
            }
            let boundary_bytes = page_bytes;
            state.charge(page.len())?;
            state.charge_total(attachments.iter().map(Vec::len).sum())?;
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
            state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
            state.counters.source_bytes_read = state
                .counters
                .source_bytes_read
                .saturating_add(boundary_bytes as u64);
            state.counters.row_input_pages = state.counters.row_input_pages.saturating_add(1);
            state.counters.row_input_records = state
                .counters
                .row_input_records
                .saturating_add(u64::from(record_count));
            state.counters.row_input_wire_bytes = state
                .counters
                .row_input_wire_bytes
                .saturating_add(boundary_bytes as u64);
            state.counters.row_input_attachment_reads = state
                .counters
                .row_input_attachment_reads
                .saturating_add(attachments.len() as u64);
            state.counters.row_input_attachment_bytes =
                state.counters.row_input_attachment_bytes.saturating_add(
                    attachments
                        .iter()
                        .map(|attachment| attachment.len() as u64)
                        .sum(),
                );
            state.counters.typed_row_encode_records = state
                .counters
                .typed_row_encode_records
                .saturating_add(u64::from(record_count));
            state.counters.typed_row_encode_bytes = state
                .counters
                .typed_row_encode_bytes
                .saturating_add(boundary_bytes as u64);
            return Ok(Some(bindings::lix::plugin::host::RowPage {
                payload: page,
                attachments,
            }));
        }
        return Err(bindings::lix::plugin::host::HostError::Rejected(
            "host row input contains a legacy untyped row; typed Schema v1 rows are required"
                .to_owned(),
        ));
    }

    fn drop(&mut self, resource: Resource<RowSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostColumnMergeSource for WasiHostState {
    fn len(&mut self, resource: Resource<ConflictSourceResource>) -> u32 {
        u32::try_from(
            self.table
                .get(&resource)
                .expect("component conflict source must be live")
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .conflicts
                .len(),
        )
        .unwrap_or(u32::MAX)
    }

    fn get(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
    ) -> Result<bindings::lix::plugin::host::ColumnMergeMeta, bindings::lix::plugin::host::HostError>
    {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        let conflict = state
            .conflicts
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let primary_key = conflict
            .key
            .row_pk
            .iter()
            .map(|component| {
                typed_wire::encode_key_value_bytes(component).map_err(|error| {
                    bindings::lix::plugin::host::HostError::Rejected(format!(
                        "invalid typed merge identity: {error:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let meta = bindings::lix::plugin::host::ColumnMergeMeta {
            ordinal: conflict.ordinal,
            schema_key: conflict.key.schema_key.to_string(),
            primary_key,
            schema_fingerprint: conflict.schema_fingerprint.to_vec(),
            file_id: conflict.file_id.clone(),
            column: conflict.column.clone(),
            base_len: conflict
                .base
                .as_ref()
                .map(encoded_merge_value_len)
                .transpose()
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
            a_len: conflict
                .a
                .as_ref()
                .map(encoded_merge_value_len)
                .transpose()
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
            b_len: conflict
                .b
                .as_ref()
                .map(encoded_merge_value_len)
                .transpose()
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
            base_row_len: encoded_merge_row_len(&conflict.base_row)
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
            a_row_len: encoded_merge_row_len(&conflict.a_row)
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
            b_row_len: encoded_merge_row_len(&conflict.b_row)
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?,
        };
        let metadata_bytes = meta.schema_key.len()
            + meta.primary_key.iter().map(Vec::len).sum::<usize>()
            + meta.schema_fingerprint.len()
            + 32;
        state.charge(metadata_bytes)?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(meta)
    }

    fn read_value(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
        side: bindings::lix::plugin::host::MergeSide,
        offset: u64,
        length: u32,
    ) -> Result<Option<Vec<u8>>, bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        ensure_source_page(length, state.limits.max_page_bytes)?;
        let conflict = state
            .conflicts
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let Some(value) = conflict_value(conflict, side) else {
            return Ok(None);
        };
        let encoded = encoded_merge_value(value)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
        let bytes = read_encoded_bytes(&encoded, offset, length)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
        state.charge(bytes.len())?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn read_row(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
        side: bindings::lix::plugin::host::MergeSide,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        ensure_source_page(length, state.limits.max_page_bytes)?;
        let conflict = state
            .conflicts
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let encoded = encoded_merge_row(conflict_row(conflict, side))
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
        let bytes = read_encoded_bytes(&encoded, offset, length)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
        state.charge(bytes.len())?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(bytes.len() as u64);
        Ok(bytes)
    }

    fn drop(&mut self, resource: Resource<ConflictSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostColumnMergeSink for WasiHostState {
    fn max_batch_bytes(&mut self, resource: Resource<ResolutionSinkResource>) -> u32 {
        self.table
            .get(&resource)
            .expect("component resolution sink must be live")
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits
            .max_page_bytes
    }

    fn use_lww(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        if state.pending.is_some() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component replacement is incomplete".to_owned(),
            ));
        }
        state.validate_ordinal(ordinal)?;
        state.resolutions.push(ComponentResolution::UseLww);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.conflict_resolution_takes =
            state.counters.conflict_resolution_takes.saturating_add(1);
        Ok(())
    }

    fn begin_replace(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
        total_length: Option<u64>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        if state.pending.is_some() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component replacement is already open".to_owned(),
            ));
        }
        state.validate_ordinal(ordinal)?;
        if total_length.is_some_and(|length| length > state.limits.max_total_bytes) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "component replacement exceeds max-total-bytes".to_owned(),
            ));
        }
        let capacity = usize::try_from(total_length.unwrap_or(0)).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "component replacement exceeds host address space".to_owned(),
            )
        })?;
        state.pending = Some(PendingReplacement {
            ordinal,
            expected_len: total_length,
            bytes: Vec::with_capacity(capacity),
        });
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn write_replacement(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        chunk: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge(chunk.len())?;
        let pending = state.pending.as_mut().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "component replacement chunk has no open replacement".to_owned(),
            )
        })?;
        let next_len = pending
            .bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "component replacement length overflowed".to_owned(),
                )
            })?;
        if pending
            .expected_len
            .is_none_or(|expected| next_len as u64 > expected)
        {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component replacement exceeds its declared length".to_owned(),
            ));
        }
        pending.bytes.extend_from_slice(&chunk);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn finish_replace(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
        let shared = self
            .table
            .get(&resource)
            .map_err(host_table_error)?
            .state
            .clone();
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.check_active()?;
        let pending = state.pending.take().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "component replacement finish has no open replacement".to_owned(),
            )
        })?;
        if pending
            .expected_len
            .is_some_and(|expected| pending.bytes.len() as u64 != expected)
        {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component replacement ended before its declared length".to_owned(),
            ));
        }
        debug_assert_eq!(pending.ordinal as usize, state.resolutions.len());
        state.resolutions.push(ComponentResolution::Replace(
            pending.expected_len.map(|_| Bytes::from(pending.bytes)),
        ));
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn drop(&mut self, resource: Resource<ResolutionSinkResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::Host for WasiHostState {}
impl bindings::lix::plugin::types::Host for WasiHostState {}

fn host_table_error(
    error: wasmtime::component::ResourceTableError,
) -> bindings::lix::plugin::host::HostError {
    bindings::lix::plugin::host::HostError::Rejected(error.to_string())
}

fn read_source_all(
    source: &Arc<dyn lix::plugin::runtime::WasmByteSource>,
) -> Result<Vec<u8>, LixError> {
    const CHUNK_BYTES: u32 = 1024 * 1024;
    let length = source.len();
    let mut output = Vec::with_capacity(
        usize::try_from(length)
            .map_err(|_| component_error("component source exceeds host address space"))?,
    );
    let mut offset = 0_u64;
    while offset < length {
        let chunk = u32::try_from((length - offset).min(u64::from(CHUNK_BYTES)))
            .expect("bounded component source read fits u32");
        let bytes = source.read(offset, chunk).map_err(|error| {
            component_error(format!("failed to read component source: {error}"))
        })?;
        if bytes.len() != chunk as usize {
            return Err(component_error("component source returned a short read"));
        }
        output.extend_from_slice(&bytes);
        offset += u64::from(chunk);
    }
    Ok(output)
}

struct ComponentFactory {
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

struct ComponentWorker {
    store: Store<WasiHostState>,
    file_projection: Option<FileProjectionGuest>,
    column_merger: Option<ColumnMergerGuest>,
    limits: WasmLimits,
    documents: HashMap<u64, ComponentDocument>,
    next_document: u64,
}

enum FileProjectionGuest {
    Combined(bindings::exports::lix::plugin::file_projection::Guest),
    Narrow(file_projection_bindings::exports::lix::plugin::file_projection::Guest),
}

enum ColumnMergerGuest {
    Combined(bindings::exports::lix::plugin::column_merger::Guest),
    Narrow(column_merger_bindings::exports::lix::plugin::column_merger::Guest),
}

impl FileProjectionGuest {
    fn call_parse(
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

    fn call_parse_changes(
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

    fn call_serialize(
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

    fn call_serialize_changes(
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
    fn call_merge(
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

#[derive(Clone)]
struct ComponentDocument {
    root: ArenaRoot,
}

struct ResolutionWorkerOutput {
    resolutions: Vec<ComponentResolution>,
    counters: WasmTransitionCounters,
}

struct RowWorkerOutput {
    edits: Vec<PendingFileEdit>,
    counters: WasmTransitionCounters,
}

struct HydrateWorkerOutput {
    replacement: Option<Bytes>,
    accepted_len: u64,
    counters: WasmTransitionCounters,
}

struct FileWorkerOutput {
    pages: VecDeque<PendingChangePage>,
    replace_all_rows: bool,
    counters: WasmTransitionCounters,
}

fn hydration_replacement_edit(replacement_len: u64, accepted_len: u64) -> WasmOutputSplice {
    WasmOutputSplice {
        offset: 0,
        delete_len: accepted_len,
        insert: WasmGuestBytes::Output(WasmOutputRange {
            index: 0,
            offset: 0,
            length: replacement_len,
        }),
    }
}

impl ComponentWorker {
    fn open_file(
        &mut self,
        document: u64,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<FileWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        validate_source_admission(input.file.len(), limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            input.creates,
            false,
            None,
        )?));
        let bytes = read_source_all(&input.file)?;
        let arena_store = ArenaStore::default();
        let root = ArenaRoot::import(
            arena_store,
            "plugin-file-arena",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let accepted = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
                root: root.clone(),
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!("failed to register component snapshot: {error}"))
            })?;
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction: root.transaction(),
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!("failed to register component transition: {error}"))
            })?;
        let transition_rep = transition.rep();
        let binding_input = bindings::lix::plugin::types::ParseRequest {
            file_id: input.descriptor.file_id.clone(),
            path: required_plugin_path(input.descriptor.path, "parse")?,
            file: accepted,
            creates: bindings::lix::plugin::types::CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
        };
        let result = self
            .file_projection
            .as_ref()
            .ok_or_else(|| component_error("plugin has no file-projection capability"))?
            .call_parse(
                &mut self.store,
                &binding_input,
                Resource::new_borrow(transition_rep),
            );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover component transaction",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(component_error(format!(
                    "component parse rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("component parse trapped", error));
            }
        }
        let TransitionResource {
            transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let root = transaction.commit().map_err(|error| {
            component_error(format!("failed to commit component arena root: {error}"))
        })?;
        self.documents.insert(document, ComponentDocument { root });
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| {
                component_error("component transition resources remained live after open-file")
            })?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        let pages = state.take_pages()?;
        Ok(FileWorkerOutput {
            pages,
            replace_all_rows: state.replace_all_rows,
            counters: state.counters,
        })
    }

    fn file_changed(
        &mut self,
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        mut update: WasmFileUpdate,
    ) -> Result<FileWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let root = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.component_arena_prepare"
        )
        .in_scope(|| {
            self.documents
                .get(&document)
                .map(|document| document.root.clone())
                .ok_or_else(|| component_error("unknown component document handle"))
        })?;
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            update.creates,
            false,
            None,
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
                // Byte-successor parsing is defined over accepted bytes plus
                // opaque plugin state. Semantic rows are outputs and host
                // authority, not an additional warm-only input dependency.
                root: root.successor_checkpoint(),
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!("failed to register component before root: {error}"))
            })?;
        let mut transaction = root.transaction();
        let mut binding_edits = Vec::with_capacity(update.edits.len());
        for edit in update.edits {
            let insert = match edit.insert {
                WasmInputBytes::Inline(bytes) => bytes,
                WasmInputBytes::AfterRange(range) => update
                    .after
                    .read(
                        range.offset,
                        u32::try_from(range.length)
                            .map_err(|_| component_error("component after-range exceeds u32"))?,
                    )
                    .map_err(|error| {
                        component_error(format!(
                            "failed to read component after-range bytes: {error}"
                        ))
                    })?,
            };
            transaction.edit_bytes(ArenaByteEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: insert.clone(),
            });
            binding_edits.push(bindings::lix::plugin::host::FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            });
        }
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction,
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!("failed to register component transition: {error}"))
            })?;
        let row_state = update
            .rows
            .take()
            .map(|rows| {
                RowChangeState::from_rows(limits, rows, SharedByteBudget::default())
                    .map(|state| Arc::new(Mutex::new(state)))
            })
            .transpose()?;
        let rows = row_state
            .as_ref()
            .map(|row_state| {
                self.store
                    .data_mut()
                    .table
                    .push(RowSourceResource {
                        state: row_state.clone(),
                    })
                    .map_err(|error| {
                        component_error(format!(
                            "failed to register component parse-changes row source: {error}"
                        ))
                    })
            })
            .transpose()?;
        let transition_rep = transition.rep();
        let binding_update = bindings::lix::plugin::types::ParseChangesRequest {
            file_id: update.after_descriptor.file_id.clone(),
            before_path: required_plugin_path(
                update.before_descriptor.path,
                "parse-changes predecessor",
            )?,
            after_path: required_plugin_path(
                update.after_descriptor.path,
                "parse-changes successor",
            )?,
            before,
            file_edits: binding_edits,
            rows,
            creates: bindings::lix::plugin::types::CreateContext {
                high: update.creates.high,
                low: update.creates.low,
            },
        };
        let result =
            tracing::debug_span!(target: "lix_perf", "lix.perf.component_guest_file_changed")
                .in_scope(|| {
                    self.file_projection
                        .as_ref()
                        .ok_or_else(|| component_error("plugin has no file-projection capability"))?
                        .call_parse_changes(
                            &mut self.store,
                            &binding_update,
                            Resource::new_borrow(transition_rep),
                        )
                });
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover component transaction",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(component_error(format!(
                    "component parse-changes rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("component parse-changes trapped", error));
            }
        }
        let TransitionResource {
            transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let root = tracing::debug_span!(target: "lix_perf", "lix.perf.component_arena_commit")
            .in_scope(|| {
                transaction.commit().map_err(|error| {
                    component_error(format!("failed to commit component arena root: {error}"))
                })
            })?;
        self.documents
            .insert(next_document, ComponentDocument { root });
        self.next_document = self.next_document.max(next_document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| {
                component_error("component transition resources remained live after file-changed")
            })?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut row_state = row_state
            .map(|row_state| {
                Arc::try_unwrap(row_state)
                    .map_err(|_| {
                        component_error("component parse-changes row source remained live")
                    })
                    .map(|state| {
                        state
                            .into_inner()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                    })
            })
            .transpose()?;
        let prior_keys = if state.replace_all_rows {
            match row_state.as_mut() {
                Some(row_state) => row_state.drain_complete_row_keys()?,
                None => update
                    .prior_row_keys
                    .take()
                    .ok_or_else(|| {
                        component_error(
                            "parse_changes replacement requires predecessor row identities",
                        )
                    })?
                    .into_keys()?,
            }
        } else {
            BTreeSet::new()
        };
        append_replace_all_deletes(&mut state, prior_keys)?;
        if let Some(row_state) = &row_state {
            state.counters.component_import_calls = state
                .counters
                .component_import_calls
                .saturating_add(row_state.counters.component_import_calls);
            state.counters.component_boundary_bytes = state
                .counters
                .component_boundary_bytes
                .saturating_add(row_state.counters.component_boundary_bytes);
            state.counters.source_read_calls = state
                .counters
                .source_read_calls
                .saturating_add(row_state.counters.source_read_calls);
            state.counters.source_bytes_read = state
                .counters
                .source_bytes_read
                .saturating_add(row_state.counters.source_bytes_read);
            merge_row_input_profile(&mut state.counters, &row_state.counters);
        }
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        let pages = state.take_pages()?;
        Ok(FileWorkerOutput {
            pages,
            replace_all_rows: state.replace_all_rows,
            counters: state.counters,
        })
    }

    fn cold_file_changed(
        &mut self,
        document: u64,
        limits: WasmTransitionLimits,
        cold: WasmColdFileUpdate,
    ) -> Result<FileWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        cold.validate(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let before_source = cold.before.as_ref().ok_or_else(|| {
            component_error("component cold successor requires accepted predecessor bytes")
        })?;
        let root_bytes = read_source_all(before_source)?;
        let root = ArenaRoot::import(
            ArenaStore::default(),
            "component-cold-successor",
            &root_bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let total_bytes = SharedByteBudget::default();
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            cold.creates,
            false,
            Some(total_bytes.clone()),
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
                root: root.clone(),
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component cold-successor snapshot: {error}"
                ))
            })?;
        let row_state = Arc::new(Mutex::new(RowChangeState::from_rows(
            limits,
            cold.rows,
            total_bytes,
        )?));
        let source = self
            .store
            .data_mut()
            .table
            .push(RowSourceResource {
                state: row_state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component cold-successor row source: {error}"
                ))
            })?;
        let mut transaction = root.transaction();
        let mut binding_edits = Vec::with_capacity(cold.edits.len());
        for edit in cold.edits {
            let insert = match edit.insert {
                WasmInputBytes::Inline(bytes) => bytes,
                WasmInputBytes::AfterRange(range) => cold
                    .after
                    .read(
                        range.offset,
                        u32::try_from(range.length)
                            .map_err(|_| component_error("component after-range exceeds u32"))?,
                    )
                    .map_err(|error| {
                        component_error(format!(
                            "failed to read component after-range bytes: {error}"
                        ))
                    })?,
            };
            transaction.edit_bytes(ArenaByteEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: insert.clone(),
            });
            binding_edits.push(bindings::lix::plugin::host::FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            });
        }
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction,
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component cold-successor transition: {error}"
                ))
            })?;
        let transition_rep = transition.rep();
        let binding_input = bindings::lix::plugin::types::ParseChangesRequest {
            file_id: cold.after_descriptor.file_id.clone(),
            before_path: required_plugin_path(
                cold.before_descriptor.path,
                "cold parse-changes predecessor",
            )?,
            after_path: required_plugin_path(
                cold.after_descriptor.path,
                "cold parse-changes successor",
            )?,
            before,
            file_edits: binding_edits,
            rows: Some(source),
            creates: bindings::lix::plugin::types::CreateContext {
                high: cold.creates.high,
                low: cold.creates.low,
            },
        };
        let result = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.component_guest_cold_successor"
        )
        .in_scope(|| {
            self.file_projection
                .as_ref()
                .ok_or_else(|| component_error("plugin has no file-projection capability"))?
                .call_parse_changes(
                    &mut self.store,
                    &binding_input,
                    Resource::new_borrow(transition_rep),
                )
        });
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover component cold-successor transaction",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(component_error(format!(
                    "component cold parse-changes rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error(
                    "component cold parse-changes trapped",
                    error,
                ));
            }
        }
        let TransitionResource {
            transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let root = transaction.commit().map_err(|error| {
            component_error(format!(
                "failed to commit component cold-successor arena root: {error}"
            ))
        })?;
        self.documents.insert(document, ComponentDocument { root });
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| component_error("component cold-successor resources remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut row_state = Arc::try_unwrap(row_state)
            .map_err(|_| component_error("component cold-successor row source remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prior_keys = if state.replace_all_rows {
            row_state.drain_complete_row_keys()?
        } else {
            BTreeSet::new()
        };
        append_replace_all_deletes(&mut state, prior_keys)?;
        state.counters.component_import_calls = state
            .counters
            .component_import_calls
            .saturating_add(row_state.counters.component_import_calls);
        state.counters.component_boundary_bytes = state
            .counters
            .component_boundary_bytes
            .saturating_add(row_state.counters.component_boundary_bytes);
        state.counters.source_read_calls = state
            .counters
            .source_read_calls
            .saturating_add(row_state.counters.source_read_calls);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(row_state.counters.source_bytes_read);
        merge_row_input_profile(&mut state.counters, &row_state.counters);
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        let pages = state.take_pages()?;
        Ok(FileWorkerOutput {
            pages,
            replace_all_rows: state.replace_all_rows,
            counters: state.counters,
        })
    }

    fn merge_columns(
        &mut self,
        limits: WasmTransitionLimits,
        mut update: WasmColumnMergeUpdate,
    ) -> Result<ResolutionWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let mut conflicts = Vec::new();
        while let Some(page) = update.merges.next_page(limits.max_page_bytes)? {
            if page.merges.is_empty() {
                return Err(component_error(
                    "component conflict source returned an empty page",
                ));
            }
            for conflict in page.merges {
                let expected = u32::try_from(conflicts.len())
                    .map_err(|_| component_error("component conflict count exceeds u32"))?;
                if conflict.ordinal != expected {
                    return Err(component_error(format!(
                        "component conflict source ordinal {}, expected {expected}",
                        conflict.ordinal
                    )));
                }
                conflicts.push(conflict);
            }
        }
        let expected_count = conflicts.len();
        let state = Arc::new(Mutex::new(ResolutionState::new(limits, conflicts)?));
        let source = self
            .store
            .data_mut()
            .table
            .push(ConflictSourceResource {
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component conflict source: {error}"
                ))
            })?;
        let sink = self
            .store
            .data_mut()
            .table
            .push(ResolutionSinkResource {
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component resolution sink: {error}"
                ))
            })?;
        let sink_rep = sink.rep();
        let result = self
            .column_merger
            .as_ref()
            .ok_or_else(|| component_error("plugin has no column-merger capability"))?
            .call_merge(&mut self.store, source, Resource::new_borrow(sink_rep));
        let _ = self.store.data_mut().table.delete(sink);
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(component_error(format!(
                    "component column merge rejected input: {error:?}"
                )));
            }
            Err(error) => {
                return Err(wasm_runtime_error("component column merge trapped", error));
            }
        }
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| {
                component_error("component column merge resources remained live after merging")
            })?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_some() {
            return Err(component_error(
                "component column replacement remained incomplete",
            ));
        }
        if state.resolutions.len() != expected_count {
            return Err(component_error(format!(
                "component column merger returned {} results for {expected_count} overlaps",
                state.resolutions.len()
            )));
        }
        state.counters.conflict_resolution_records =
            u64::try_from(state.resolutions.len()).unwrap_or(u64::MAX);
        state.counters.packet_records = state.counters.conflict_resolution_records;
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(ResolutionWorkerOutput {
            resolutions: state.resolutions,
            counters: state.counters,
        })
    }

    fn rows_changed(
        &mut self,
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        update: WasmRowUpdate,
    ) -> Result<RowWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let root = self
            .documents
            .get(&document)
            .map(|document| document.root.clone())
            .ok_or_else(|| component_error("unknown component document handle"))?;
        let total_bytes = SharedByteBudget::default();
        let row_state = Arc::new(Mutex::new(RowChangeState::from_changes(
            limits,
            update.changes,
            total_bytes.clone(),
        )?));
        let transition_state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            WasmCreateContext { high: 0, low: 0 },
            true,
            Some(total_bytes),
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
                root: root.clone(),
                state: transition_state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component row snapshot: {error}"
                ))
            })?;
        let source = self
            .store
            .data_mut()
            .table
            .push(RowSourceResource {
                state: row_state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component row-change source: {error}"
                ))
            })?;
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction: root.transaction(),
                state: transition_state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component row transition: {error}"
                ))
            })?;
        let transition_rep = transition.rep();
        let binding_input = bindings::lix::plugin::types::SerializeChangesRequest {
            file_id: update.before_descriptor.file_id.clone(),
            path: required_plugin_path(update.before_descriptor.path, "serialize-changes path")?,
            before,
            row_changes: source,
        };
        let result = self
            .file_projection
            .as_ref()
            .ok_or_else(|| component_error("plugin has no file-projection capability"))?
            .call_serialize_changes(
                &mut self.store,
                &binding_input,
                Resource::new_borrow(transition_rep),
            );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover component row transition",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(component_error(format!(
                    "component serialize-changes rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error(
                    "component serialize-changes trapped",
                    error,
                ));
            }
        }
        let TransitionResource {
            mut transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let mut state = Arc::try_unwrap(transition_state)
            .map_err(|_| component_error("component row transition resources remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let edits = if let Some(replacement) = state.file_replacement.take() {
            if !replacement.complete {
                return Err(component_error(
                    "component serialize-changes file replacement is incomplete",
                ));
            }
            transaction.edit_bytes(ArenaByteEdit {
                offset: 0,
                delete_len: root.bytes.len(),
                insert: replacement.bytes.clone(),
            });
            vec![PendingFileEdit {
                offset: 0,
                delete_len: root.bytes.len(),
                insert: Bytes::from(replacement.bytes),
            }]
        } else {
            std::mem::take(&mut state.file_edits)
        };
        let successor = transaction.commit().map_err(|error| {
            component_error(format!("failed to commit component row root: {error}"))
        })?;
        self.documents
            .insert(next_document, ComponentDocument { root: successor });
        self.next_document = self.next_document.max(next_document.saturating_add(1));
        let row_state = Arc::try_unwrap(row_state)
            .map_err(|_| component_error("component row-change source remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.component_import_calls = state
            .counters
            .component_import_calls
            .saturating_add(row_state.counters.component_import_calls);
        state.counters.component_boundary_bytes = state
            .counters
            .component_boundary_bytes
            .saturating_add(row_state.counters.component_boundary_bytes);
        state.counters.source_read_calls = state
            .counters
            .source_read_calls
            .saturating_add(row_state.counters.source_read_calls);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(row_state.counters.source_bytes_read);
        merge_row_input_profile(&mut state.counters, &row_state.counters);
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(RowWorkerOutput {
            edits,
            counters: state.counters,
        })
    }

    fn hydrate_file(
        &mut self,
        document: u64,
        limits: WasmTransitionLimits,
        input: WasmOpenRowsInput,
    ) -> Result<HydrateWorkerOutput, LixError> {
        let limits = component_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let accepted_len = input
            .accepted
            .as_ref()
            .map(|accepted| accepted.len())
            .unwrap_or(0);
        validate_source_admission(accepted_len, limits)?;
        let bytes = input
            .accepted
            .as_ref()
            .map(|accepted| read_source_all(accepted))
            .transpose()?
            .unwrap_or_default();
        let total_bytes = SharedByteBudget::default();
        // Serialization consumes a complete durable row stream but never
        // computes replacement tombstones. Avoid cloning and sorting every
        // identity into the parse-changes-only predecessor inventory.
        let row_state = Arc::new(Mutex::new(RowChangeState::from_rows_without_key_inventory(
            limits,
            input.rows,
            total_bytes.clone(),
        )?));
        let root = ArenaRoot::import(
            ArenaStore::default(),
            "component-cold-successor",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            WasmCreateContext { high: 0, low: 0 },
            true,
            Some(total_bytes),
        )?));
        let accepted = input
            .accepted
            .is_some()
            .then(|| {
                self.store.data_mut().table.push(SnapshotResource {
                    root: root.clone(),
                    state: state.clone(),
                })
            })
            .transpose()
            .map_err(|error| {
                component_error(format!(
                    "failed to register component cold snapshot: {error}"
                ))
            })?;
        let source = self
            .store
            .data_mut()
            .table
            .push(RowSourceResource {
                state: row_state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component hydration source: {error}"
                ))
            })?;
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction: root.transaction(),
                state: state.clone(),
            })
            .map_err(|error| {
                component_error(format!(
                    "failed to register component cold transition: {error}"
                ))
            })?;
        let transition_rep = transition.rep();
        let request = bindings::lix::plugin::types::SerializeRequest {
            file_id: input.descriptor.file_id.clone(),
            path: required_plugin_path(input.descriptor.path, "serialize")?,
            rows: source,
            before: accepted,
        };
        let result = self
            .file_projection
            .as_ref()
            .ok_or_else(|| component_error("plugin has no file-projection capability"))?
            .call_serialize(
                &mut self.store,
                &request,
                Resource::new_borrow(transition_rep),
            );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover component cold transition",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(component_error(format!(
                    "component serialize rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("component serialize trapped", error));
            }
        }
        let TransitionResource {
            mut transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let mut transition_state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let replacement = transition_state.file_replacement.take();
        let rendered = replacement
            .as_ref()
            .map(|replacement| Bytes::copy_from_slice(&replacement.bytes));
        if let Some(replacement) = replacement {
            if !replacement.complete {
                return Err(component_error(
                    "component hydration file replacement is incomplete",
                ));
            }
            transaction.edit_bytes(ArenaByteEdit {
                offset: 0,
                delete_len: root.bytes.len(),
                insert: replacement.bytes,
            });
        } else if input.accepted.is_none() {
            return Err(component_error(
                "component derived cold hydration did not emit a file replacement",
            ));
        }
        drop(transition_state);
        let root = transaction.commit().map_err(|error| {
            component_error(format!("failed to commit component cold root: {error}"))
        })?;
        self.documents.insert(document, ComponentDocument { root });
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut counters = Arc::try_unwrap(state)
            .map_err(|_| component_error("component cold hydration resources remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .counters;
        let row_state = Arc::try_unwrap(row_state)
            .map_err(|_| component_error("component hydration source remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counters.component_import_calls = counters
            .component_import_calls
            .saturating_add(row_state.counters.component_import_calls);
        counters.component_boundary_bytes = counters
            .component_boundary_bytes
            .saturating_add(row_state.counters.component_boundary_bytes);
        merge_row_input_profile(&mut counters, &row_state.counters);
        counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(HydrateWorkerOutput {
            replacement: rendered,
            accepted_len,
            counters,
        })
    }

    fn fork(&mut self, document: u64) -> Result<u64, LixError> {
        let document = self
            .documents
            .get(&document)
            .cloned()
            .ok_or_else(|| component_error("unknown component document handle"))?;
        let handle = self.next_document;
        self.next_document = self
            .next_document
            .checked_add(1)
            .ok_or_else(|| component_error("component document handle overflowed"))?;
        self.documents.insert(handle, document);
        Ok(handle)
    }

    fn drop_document(&mut self, document: u64) -> Result<(), LixError> {
        self.documents.remove(&document);
        Ok(())
    }
}

struct CursorState {
    transition: WasmTransitionHandle,
    pages: VecDeque<PendingChangePage>,
}

struct ColumnMergeCursorState {
    transition: WasmTransitionHandle,
    pages: VecDeque<WasmColumnMergeResultPage>,
}

struct OutputState {
    transition: WasmTransitionHandle,
    values: Vec<Bytes>,
}

struct ComponentEditCursorState {
    transition: WasmTransitionHandle,
    page: Option<WasmEditPage>,
}

struct ComponentActor {
    worker: ComponentWorker,
    execution_permit: Arc<tokio::sync::Semaphore>,
    initial_execution_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    _timeout_ticker: TimeoutTickerLease,
    next_handle: u64,
    cursors: HashMap<u64, CursorState>,
    column_merge_cursors: HashMap<u64, ColumnMergeCursorState>,
    edit_cursors: HashMap<u64, ComponentEditCursorState>,
    outputs: HashMap<u64, OutputState>,
    transitions: HashMap<u64, WasmTransitionCounters>,
    transition_permits: HashMap<u64, tokio::sync::OwnedSemaphorePermit>,
    prospective_documents: ProspectiveDocuments,
    retired: bool,
    next_document: u64,
}

#[derive(Default)]
struct ProspectiveDocuments(HashMap<u64, u64>);

impl ProspectiveDocuments {
    fn track(&mut self, transition: WasmTransitionHandle, document: u64) {
        self.0.insert(transition.0, document);
    }

    fn accept(&mut self, transition: WasmTransitionHandle) {
        self.0.remove(&transition.0);
    }

    fn reject(&mut self, transition: WasmTransitionHandle) -> Option<u64> {
        self.0.remove(&transition.0)
    }
}

fn is_guest_trap(error: &LixError) -> bool {
    error.code == LixError::CODE_INTERNAL_ERROR && error.message.contains(" trapped")
}

impl ComponentActor {
    fn allocate_handle(&mut self) -> Result<u64, LixError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| component_error("component actor handle overflowed"))?;
        Ok(handle)
    }

    fn allocate_document(&mut self) -> Result<u64, LixError> {
        let document = self.next_document;
        self.next_document = self
            .next_document
            .checked_add(1)
            .ok_or_else(|| component_error("component document handle overflowed"))?;
        Ok(document)
    }

    fn ensure_active(&self) -> Result<(), LixError> {
        if self.retired {
            return Err(component_error("component actor is retired"));
        }
        Ok(())
    }

    fn execution_scheduler(&self) -> Arc<tokio::sync::Semaphore> {
        Arc::clone(&self.execution_permit)
    }

    async fn acquire_execution_permit(
        initial: Option<tokio::sync::OwnedSemaphorePermit>,
        scheduler: Arc<tokio::sync::Semaphore>,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, LixError> {
        match initial {
            Some(permit) => Ok(permit),
            None => scheduler
                .acquire_owned()
                .await
                .map_err(|_| component_error("component component execution scheduler stopped")),
        }
    }

    fn retire_after_trap(&mut self, error: &LixError) {
        if is_guest_trap(error) {
            self.retired = true;
        }
    }
}

#[async_trait]
impl WasmComponentActor for ComponentActor {
    fn cold_open_hydrates_without_render(&self) -> bool {
        true
    }

    fn cold_open_requires_rows(&self) -> bool {
        true
    }

    async fn fork_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<WasmDocumentHandle, LixError> {
        self.ensure_active()?;
        let fork = self.worker.fork(document.0)?;
        self.next_document = self.next_document.max(fork.saturating_add(1));
        Ok(WasmDocumentHandle(fork))
    }

    async fn checkpoint_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<Option<WasmDocumentCheckpoint>, LixError> {
        self.ensure_active()?;
        let root = self
            .worker
            .documents
            .get(&document.0)
            .ok_or_else(|| component_error("unknown component document handle"))?
            .root
            .successor_checkpoint();
        let retained_bytes = u64::try_from(root.retained_heap_bytes()).unwrap_or(u64::MAX);
        Ok(Some(WasmDocumentCheckpoint::new(root, retained_bytes)))
    }

    async fn restore_document(
        &mut self,
        checkpoint: &WasmDocumentCheckpoint,
    ) -> Result<WasmDocumentHandle, LixError> {
        self.ensure_active()?;
        let root = checkpoint
            .downcast_ref::<ArenaRoot>()
            .ok_or_else(|| {
                component_error("component document checkpoint belongs to another runtime")
            })?
            .clone();
        let document = self.allocate_document()?;
        self.worker
            .documents
            .insert(document, ComponentDocument { root });
        self.worker.next_document = self.worker.next_document.max(document.saturating_add(1));
        Ok(WasmDocumentHandle(document))
    }

    async fn open_file(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<WasmFileTransition, LixError> {
        let document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmChangeCursorHandle(self.allocate_handle()?);
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let output = self.worker.open_file(document, limits, input);
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        self.transitions.insert(transition.0, output.counters);
        self.cursors.insert(
            cursor.0,
            CursorState {
                transition,
                pages: output.pages,
            },
        );
        self.transition_permits.insert(transition.0, permit);
        self.prospective_documents.track(transition, document);
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(document),
            changes: cursor,
            replace_all_rows: true,
        })
    }

    async fn open_rows(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenRowsInput,
    ) -> Result<WasmRowTransition, LixError> {
        let document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let edits = WasmEditCursorHandle(self.allocate_handle()?);
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let resolved = self.worker.hydrate_file(document, limits, input);
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        let page = if let Some(replacement) = resolved.replacement {
            let outputs = WasmByteOutputsHandle(self.allocate_handle()?);
            let length = replacement.len() as u64;
            self.outputs.insert(
                outputs.0,
                OutputState {
                    transition,
                    values: vec![replacement],
                },
            );
            Some(WasmEditPage {
                edits: vec![hydration_replacement_edit(length, resolved.accepted_len)],
                outputs: Some(outputs),
            })
        } else {
            None
        };
        self.transitions.insert(transition.0, resolved.counters);
        self.prospective_documents.track(transition, document);
        self.edit_cursors
            .insert(edits.0, ComponentEditCursorState { transition, page });
        self.transition_permits.insert(transition.0, permit);
        Ok(WasmRowTransition {
            transition,
            document: WasmDocumentHandle(document),
            edits,
        })
    }

    async fn file_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
    ) -> Result<WasmFileTransition, LixError> {
        update.validate(limits)?;
        let next_document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmChangeCursorHandle(self.allocate_handle()?);
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let output = self
            .worker
            .file_changed(document.0, next_document, limits, update);
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        self.transitions.insert(transition.0, output.counters);
        self.cursors.insert(
            cursor.0,
            CursorState {
                transition,
                pages: output.pages,
            },
        );
        self.transition_permits.insert(transition.0, permit);
        self.prospective_documents.track(transition, next_document);
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(next_document),
            changes: cursor,
            replace_all_rows: output.replace_all_rows,
        })
    }

    async fn cold_file_changed(
        &mut self,
        limits: WasmTransitionLimits,
        update: WasmColdFileUpdate,
    ) -> Result<WasmFileTransition, LixError> {
        update.validate(limits)?;
        let document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmChangeCursorHandle(self.allocate_handle()?);
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let output = self.worker.cold_file_changed(document, limits, update);
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        self.transitions.insert(transition.0, output.counters);
        self.cursors.insert(
            cursor.0,
            CursorState {
                transition,
                pages: output.pages,
            },
        );
        self.transition_permits.insert(transition.0, permit);
        self.prospective_documents.track(transition, document);
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(document),
            changes: cursor,
            replace_all_rows: output.replace_all_rows,
        })
    }

    async fn rows_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmRowUpdate,
    ) -> Result<WasmRowTransition, LixError> {
        let next_document = self.allocate_document()?;
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let resolved = self
            .worker
            .rows_changed(document.0, next_document, limits, update);
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let edits = WasmEditCursorHandle(self.allocate_handle()?);
        let output_handle = (!resolved.edits.is_empty())
            .then(|| self.allocate_handle().map(WasmByteOutputsHandle))
            .transpose()?;
        let mut output_values = Vec::with_capacity(resolved.edits.len());
        let output_edits = resolved
            .edits
            .into_iter()
            .enumerate()
            .map(|(index, edit)| {
                let length = edit.insert.len() as u64;
                output_values.push(edit.insert);
                Ok(WasmOutputSplice {
                    offset: edit.offset,
                    delete_len: edit.delete_len,
                    insert: WasmGuestBytes::Output(WasmOutputRange {
                        index: u32::try_from(index).map_err(|_| {
                            component_error("component file edit count exceeds u32")
                        })?,
                        offset: 0,
                        length,
                    }),
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        if let Some(outputs) = output_handle {
            self.outputs.insert(
                outputs.0,
                OutputState {
                    transition,
                    values: output_values,
                },
            );
        }
        self.transitions.insert(transition.0, resolved.counters);
        self.prospective_documents.track(transition, next_document);
        self.edit_cursors.insert(
            edits.0,
            ComponentEditCursorState {
                transition,
                page: (!output_edits.is_empty()).then_some(WasmEditPage {
                    edits: output_edits,
                    outputs: output_handle,
                }),
            },
        );
        self.transition_permits.insert(transition.0, permit);
        Ok(WasmRowTransition {
            transition,
            document: WasmDocumentHandle(next_document),
            edits,
        })
    }

    async fn merge_columns(
        &mut self,
        limits: WasmTransitionLimits,
        update: WasmColumnMergeUpdate,
    ) -> Result<WasmColumnMergeTransition, LixError> {
        self.ensure_active()?;
        let permit = Self::acquire_execution_permit(
            self.initial_execution_permit.take(),
            self.execution_scheduler(),
        )
        .await?;
        let resolved = self.worker.merge_columns(limits, update);
        let resolved = match resolved {
            Ok(resolved) => resolved,
            Err(error) => {
                self.retire_after_trap(&error);
                return Err(error);
            }
        };
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmColumnMergeCursorHandle(self.allocate_handle()?);
        let records_per_page = (limits.max_page_bytes as usize / 64).max(1);
        let mut pages = VecDeque::new();
        let mut ordinal = 0_u32;
        let mut resolutions = resolved.resolutions.into_iter();
        loop {
            let mut page_ordinals = Vec::with_capacity(records_per_page);
            let mut page_results = Vec::with_capacity(records_per_page);
            let mut page_outputs = Vec::new();
            for _ in 0..records_per_page {
                let Some(resolution) = resolutions.next() else {
                    break;
                };
                page_ordinals.push(ordinal);
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| component_error("component resolution ordinal overflowed"))?;
                page_results.push(match resolution {
                    ComponentResolution::UseLww => RuntimeColumnMergeResult::UseLww,
                    ComponentResolution::Replace(None) => RuntimeColumnMergeResult::Replace(None),
                    ComponentResolution::Replace(Some(snapshot)) => {
                        let index = u32::try_from(page_outputs.len()).map_err(|_| {
                            component_error("component replacement output count exceeds u32")
                        })?;
                        let length = snapshot.len() as u64;
                        page_outputs.push(snapshot);
                        RuntimeColumnMergeResult::Replace(Some(WasmGuestColumnValue::Output(
                            WasmOutputRange {
                                index,
                                offset: 0,
                                length,
                            },
                        )))
                    }
                });
            }
            if page_results.is_empty() {
                break;
            }
            let outputs = if page_outputs.is_empty() {
                None
            } else {
                let handle = WasmByteOutputsHandle(self.allocate_handle()?);
                self.outputs.insert(
                    handle.0,
                    OutputState {
                        transition,
                        values: page_outputs,
                    },
                );
                Some(handle)
            };
            pages.push_back(WasmColumnMergeResultPage {
                format_version: CURRENT_PACKET_FORMAT,
                ordinals: page_ordinals,
                results: page_results,
                outputs,
            });
        }
        self.transitions.insert(transition.0, resolved.counters);
        self.column_merge_cursors
            .insert(cursor.0, ColumnMergeCursorState { transition, pages });
        self.transition_permits.insert(transition.0, permit);
        Ok(WasmColumnMergeTransition {
            transition,
            results: cursor,
        })
    }

    async fn next_change_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmChangeCursorHandle,
        _max_bytes: u32,
    ) -> Result<Option<WasmChangePage>, LixError> {
        let cursor = self
            .cursors
            .get_mut(&cursor.0)
            .ok_or_else(|| component_error("unknown component change cursor"))?;
        if cursor.transition != transition {
            return Err(component_error(
                "component change cursor belongs to another transition",
            ));
        }
        match cursor.pages.pop_front() {
            Some(PendingChangePage::Decoded(page)) => Ok(Some(page)),
            Some(PendingChangePage::Typed {
                schema_key,
                schema_fingerprint,
                mutations,
            }) => Ok(Some(decode_typed_change_page(
                schema_key,
                schema_fingerprint,
                mutations,
            )?)),
            None => Ok(None),
        }
    }

    async fn next_column_merge_result_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmColumnMergeCursorHandle,
        _max_bytes: u32,
    ) -> Result<Option<WasmColumnMergeResultPage>, LixError> {
        let cursor = self
            .column_merge_cursors
            .get_mut(&cursor.0)
            .ok_or_else(|| component_error("unknown component column merge cursor"))?;
        if cursor.transition != transition {
            return Err(component_error(
                "component column merge cursor belongs to another transition",
            ));
        }
        Ok(cursor.pages.pop_front())
    }

    async fn next_edit_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmEditCursorHandle,
        _max_edits: u32,
        _max_inline_bytes: u32,
    ) -> Result<Option<WasmEditPage>, LixError> {
        match self.edit_cursors.get_mut(&cursor.0) {
            Some(state) if state.transition == transition => Ok(state.page.take()),
            Some(_) => Err(component_error(
                "component hydration edit cursor belongs to another transition",
            )),
            None => Err(component_error("unknown component hydration edit cursor")),
        }
    }

    async fn output_len(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
    ) -> Result<u64, LixError> {
        let outputs = self
            .outputs
            .get(&outputs.0)
            .ok_or_else(|| component_error("unknown component byte outputs"))?;
        if outputs.transition != transition {
            return Err(component_error(
                "component byte outputs belong to another transition",
            ));
        }
        outputs
            .values
            .get(index as usize)
            .map(|bytes| bytes.len() as u64)
            .ok_or_else(|| component_error("component byte output index is out of bounds"))
    }

    async fn read_output(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, LixError> {
        let outputs = self
            .outputs
            .get(&outputs.0)
            .ok_or_else(|| component_error("unknown component byte outputs"))?;
        if outputs.transition != transition {
            return Err(component_error(
                "component byte outputs belong to another transition",
            ));
        }
        let bytes = outputs
            .values
            .get(index as usize)
            .ok_or_else(|| component_error("component byte output index is out of bounds"))?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or_else(|| component_error("component byte output range overflowed"))?;
        if end > bytes.len() as u64 {
            return Err(component_error(
                "component byte output range is out of bounds",
            ));
        }
        Ok(bytes.slice(offset as usize..end as usize).to_vec())
    }

    async fn finish_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<WasmTransitionCounters, LixError> {
        // Keep component admission until every guest-pushed page and output
        // owned by this transition has been drained and reclaimed.
        let _permit = self.transition_permits.remove(&transition.0);
        self.cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.column_merge_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.edit_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.outputs
            .retain(|_, outputs| outputs.transition != transition);
        self.prospective_documents.accept(transition);
        self.transitions
            .remove(&transition.0)
            .ok_or_else(|| component_error("unknown component transition"))
    }

    async fn discard_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<(), LixError> {
        // Hold the removed permit locally through cleanup, including the
        // prospective guest document drop on validation rejection.
        let _permit = self.transition_permits.remove(&transition.0);
        self.cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.column_merge_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.edit_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.outputs
            .retain(|_, outputs| outputs.transition != transition);
        self.transitions.remove(&transition.0);
        if let Some(document) = self.prospective_documents.reject(transition)
            && !self.retired
        {
            self.worker.drop_document(document)?;
        }
        Ok(())
    }

    fn is_retired(&self) -> bool {
        self.retired
    }

    async fn drop_document(&mut self, document: WasmDocumentHandle) -> Result<(), LixError> {
        self.ensure_active()?;
        self.worker.drop_document(document.0)
    }

    async fn retire(&mut self) -> Result<(), LixError> {
        self.retired = true;
        Ok(())
    }
}

impl Drop for ComponentActor {
    fn drop(&mut self) {
        self.retired = true;
    }
}

fn required_plugin_path(path: Option<String>, transition: &str) -> Result<String, LixError> {
    path.ok_or_else(|| {
        component_error(format!(
            "component {transition} requires a resolved plugin-owned file path"
        ))
    })
}

fn component_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn component_transition_limits(
    mut limits: WasmTransitionLimits,
) -> Result<WasmTransitionLimits, LixError> {
    limits.max_page_bytes = COMPONENT_MAX_BATCH_BYTES.min(
        u32::try_from(limits.max_total_bytes)
            .unwrap_or(u32::MAX)
            .max(limits.max_record_bytes),
    );
    // Batches are the record envelope. Keeping the inherited 1 MiB
    // record cap while advertising a 2 MiB push page rejects otherwise valid
    // single-snapshot pages (notably Markdown lexical fallbacks).
    limits.max_record_bytes = limits.max_page_bytes;
    limits.validate()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host_typed_row(schema_key: &str, id: &str) -> WasmHostBytes {
        WasmHostBytes::Typed(Arc::new(WasmTypedRow {
            schema_fingerprint: [0; 32],
            row_pk: vec![lix_schema::Value::Text(id.to_owned())].into(),
            row: lix_schema::Row::from([
                ("id".to_owned(), lix_schema::Value::Text(id.to_owned())),
                (
                    "schema".to_owned(),
                    lix_schema::Value::Text(schema_key.to_owned()),
                ),
            ]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        }))
    }

    fn guest_typed_row(schema_key: &str, id: &str) -> WasmGuestRowPayload {
        match host_typed_row(schema_key, id) {
            WasmHostBytes::Typed(row) => WasmGuestRowPayload::Typed(row),
        }
    }

    fn text_key(schema_key: &str, values: &[&str]) -> WasmRowKey {
        WasmRowKey::from_typed_parts(
            schema_key,
            [0; 32],
            values
                .iter()
                .map(|value| lix_schema::Value::Text((*value).to_owned()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    fn uuid_key(schema_key: &str, value: uuid::Uuid) -> WasmRowKey {
        WasmRowKey::from_typed_parts(schema_key, [0; 32], vec![lix_schema::Value::Uuid(value)])
            .unwrap()
    }

    #[test]
    fn warm_parse_changes_row_source_exposes_complete_current_rows() {
        let limits = WasmTransitionLimits::default();
        let key = text_key("note", &["note-1"]);
        let source = crate::plugin::runtime::VecRowSource::new(
            vec![WasmRow {
                key: key.clone(),
                payload: host_typed_row("note", "note-1"),
            }],
            limits,
        )
        .expect("valid durable row source");
        let mut state =
            RowChangeState::from_rows(limits, Box::new(source), SharedByteBudget::default())
                .expect("warm parse-changes source should initialize");
        let page = state
            .next_page(limits.max_page_bytes)
            .expect("row source should be readable")
            .expect("one row page should exist");
        assert!(matches!(
            page.as_slice(),
            [WasmRowChange::Upsert { row, .. }] if row.key == key
        ));
        assert!(
            state
                .next_page(limits.max_page_bytes)
                .expect("row source EOF should be readable")
                .is_none()
        );
    }

    #[test]
    fn replace_all_rows_synthesizes_delete_for_omitted_prior_row() {
        let limits = WasmTransitionLimits::default();
        let keep = text_key("note", &["keep"]);
        let omitted = text_key("note", &["omitted"]);
        let mut state =
            TransitionState::new(limits, WasmCreateContext { high: 1, low: 2 }, false, None)
                .expect("transition state");
        state.replace_all_rows = true;
        state
            .pages
            .push_back(PendingChangePage::Decoded(WasmChangePage {
                format_version: CURRENT_PACKET_FORMAT,
                changes: WasmRowChanges {
                    changes: vec![WasmRowChange::Upsert {
                        row: WasmRow {
                            key: keep.clone(),
                            payload: guest_typed_row("note", "keep"),
                        },
                        effect: WasmChangeEffect::Content,
                    }],
                },
                outputs: None,
            }));
        append_replace_all_deletes(&mut state, BTreeSet::from([keep, omitted.clone()]))
            .expect("replacement deletion synthesis");
        let deletes = state
            .pages
            .pop_back()
            .expect("synthesized delete page")
            .decode()
            .expect("decode synthesized page")
            .changes
            .changes;
        assert!(matches!(deletes.as_slice(), [WasmRowChange::Delete(key)] if key == &omitted));
    }

    #[test]
    fn plugin_lifecycle_boundary_requires_a_resolved_path() {
        assert_eq!(
            required_plugin_path(Some("/document.csv".to_owned()), "open")
                .expect("resolved plugin path"),
            "/document.csv"
        );
        let error = required_plugin_path(None, "open").expect_err("missing path must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert_eq!(
            error.message,
            "component open requires a resolved plugin-owned file path"
        );
    }

    #[test]
    fn hydration_replacement_deletes_the_complete_accepted_blob() {
        assert_eq!(
            hydration_replacement_edit(17, 41),
            WasmOutputSplice {
                offset: 0,
                delete_len: 41,
                insert: WasmGuestBytes::Output(WasmOutputRange {
                    index: 0,
                    offset: 0,
                    length: 17,
                }),
            }
        );
        assert_eq!(
            hydration_replacement_edit(17, 0).delete_len,
            0,
            "derived hydration still replaces an empty accepted source"
        );
    }

    #[test]
    fn source_reads_are_rejected_before_exceeding_one_page() {
        assert!(ensure_source_page(2 * 1024 * 1024, 2 * 1024 * 1024).is_ok());
        assert!(matches!(
            ensure_source_page(2 * 1024 * 1024 + 1, 2 * 1024 * 1024),
            Err(bindings::lix::plugin::host::HostError::LimitExceeded(_))
        ));
    }

    #[test]
    fn row_sources_and_transition_sinks_share_one_byte_budget() {
        struct EmptyRowSource;
        impl lix::plugin::runtime::WasmRowSource for EmptyRowSource {
            fn next_page(
                &mut self,
                _max_bytes: u32,
            ) -> Result<Option<lix::plugin::runtime::WasmRowPage>, LixError> {
                Ok(None)
            }
        }

        let mut limits = WasmTransitionLimits::default();
        limits.max_page_bytes = 10;
        limits.max_record_bytes = 10;
        limits.max_total_bytes = 10;
        limits.max_inline_input_bytes = 10;
        let budget = SharedByteBudget::default();
        let mut source =
            RowChangeState::from_rows(limits, Box::new(EmptyRowSource), budget.clone())
                .expect("source state");
        let mut sink = TransitionState::new(
            limits,
            WasmCreateContext { high: 0, low: 0 },
            true,
            Some(budget),
        )
        .expect("sink state");

        source.charge(6).expect("first resource uses shared budget");
        assert!(matches!(
            sink.charge_page(5),
            Err(bindings::lix::plugin::host::HostError::LimitExceeded(_))
        ));
    }

    #[test]
    fn oversized_sources_are_rejected_before_materialization() {
        let mut limits = WasmTransitionLimits::default();
        limits.max_total_bytes = 10;
        assert!(validate_source_admission(10, limits).is_ok());
        assert!(validate_source_admission(11, limits).is_err());
    }

    #[test]
    fn component_binding_preserves_scaled_cold_pages() {
        let cold = WasmTransitionLimits::for_cold_file_bytes(5_298_078);
        let admitted =
            component_transition_limits(cold).expect("scaled cold page should be admitted");

        assert!(admitted.max_page_bytes > 7 * 1024 * 1024);
        assert_eq!(admitted.max_record_bytes, admitted.max_page_bytes);
        assert!(admitted.max_page_bytes <= COMPONENT_MAX_BATCH_BYTES);
    }

    #[test]
    fn borrowed_transition_owner_is_reclaimed_before_error_propagation() {
        let mut table = ResourceTable::new();
        let transition = table.push(vec![1_u8, 2, 3]).expect("transition resource");

        let recovered =
            take_borrowed_resource(&mut table, transition, "recover rejected transition")
                .expect("host owner remains deletable after a borrowed guest call");

        assert_eq!(recovered, vec![1, 2, 3]);
        assert!(table.is_empty());
    }

    #[test]
    fn rejected_transition_returns_only_its_prospective_document() {
        let accepted = WasmTransitionHandle(1);
        let rejected = WasmTransitionHandle(2);
        let mut documents = ProspectiveDocuments::default();
        documents.track(accepted, 11);
        documents.track(rejected, 12);

        documents.accept(accepted);

        assert_eq!(documents.reject(rejected), Some(12));
        assert_eq!(documents.reject(accepted), None);
    }

    #[test]
    fn cold_namespace_ignores_uuid_shaped_user_keys() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let generated_id = creates.component(7).expect("generated id");
        let generated_id_text = generated_id.to_string();
        let user_key = WasmRow {
            key: text_key("json_object_member", &["root", &generated_id_text]),
            payload: host_typed_row("json_object_member", &generated_id_text),
        };
        assert_eq!(
            create_context_from_generated_row("json_array_item", &user_key),
            None
        );

        let generated_item = WasmRow {
            key: uuid_key("json_array_item", generated_id),
            payload: host_typed_row("json_array_item", "item"),
        };
        assert_eq!(
            create_context_from_generated_row("json_array_item", &generated_item),
            Some(creates)
        );
    }

    #[test]
    fn only_runtime_traps_trigger_actor_retirement() {
        assert!(is_guest_trap(&LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "component file-changed trapped: unreachable"
        )));
        assert!(!is_guest_trap(&LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "component file-changed rejected input"
        )));
    }
}
