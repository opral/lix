//! Host-owned arena Component API v3 prototype.
//!
//! The adapter deliberately implements the existing v2 actor trait so engine
//! reconciliation, validation, and storage lowering remain unchanged. File
//! bytes and opaque plugin state live in immutable host roots; one exported
//! guest call reads sparse ranges and pushes bounded semantic pages.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use lix_engine::LixError;
use lix_engine::wasm::WasmLimits;
use lix_engine::wasm::v2::{
    PACKET_FORMAT_V1, WasmByteOutputsHandle, WasmCertifiedEntityBatch, WasmChangeCursorHandle,
    WasmChangePage, WasmComponentV2Actor, WasmComponentV2Factory, WasmCreateContext,
    WasmDocumentHandle, WasmEditCursorHandle, WasmEditPage, WasmEntityChange, WasmEntityKey,
    WasmEntityTransition, WasmEntityUpdate, WasmFileTransition, WasmFileUpdate, WasmGuestBytes,
    WasmGuestEntityChanges, WasmInputBytes, WasmOpenEntitiesInput, WasmOpenFileInput,
    WasmTransitionCounters, WasmTransitionHandle, WasmTransitionLimits,
};
use lix_engine::wasm::v3::{
    ByteEdit as ArenaByteEdit, Root as ArenaRoot, Store as ArenaStore,
    Transaction as ArenaTransaction,
};
use wasmtime::Store;
use wasmtime::component::{Component, Linker, Resource};

use super::{
    CompileProfile, CompiledComponentKey, TimeoutTickerLease, WasiHostState, WasmtimePluginRuntime,
    add_to_linker_sync, create_store, reset_store_limits, wasm_runtime_error,
};

const V3_MAX_BATCH_BYTES: u32 = 2 * 1024 * 1024;
const CERTIFIED_TYPED_CSV_V1: u16 = 1;
const CERTIFIED_CREATED_PACKET_V1: u16 = 2;

pub(super) mod bindings {
    wasmtime::component::bindgen!({
        path: "../plugin-api-v3-prototype/wit",
        world: "plugin",
        with: {
            "lix:plugin/host.root": super::RootResource,
            "lix:plugin/host.transaction": super::TransactionResource,
            "lix:plugin/host.transition-sink": super::SinkResource,
        },
    });
}

pub struct RootResource {
    root: ArenaRoot,
    state: SharedTransitionState,
}

pub struct TransactionResource {
    transaction: ArenaTransaction,
    state: SharedTransitionState,
}

pub struct SinkResource {
    state: SharedTransitionState,
}

type SharedTransitionState = Arc<Mutex<TransitionState>>;

struct TransitionState {
    limits: WasmTransitionLimits,
    creates: WasmCreateContext,
    started: Instant,
    total_bytes: u64,
    events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
    counters: WasmTransitionCounters,
}

/// Host-owned wire pages retained until the engine asks for the next page.
///
/// The first v3 prototype decoded every pushed page immediately and retained
/// the resulting entity graph until the guest export returned. Large imports
/// therefore held all generic entity objects plus the gradually constructed
/// canonical output. Keeping the bounded wire pages defers ownership expansion
/// to the existing one-page-at-a-time drain.
enum PendingChangePage {
    Packet {
        record_count: u32,
        payload: Vec<u8>,
        max_page_bytes: u32,
        limits: WasmTransitionLimits,
        creates: WasmCreateContext,
    },
    TypedCsv {
        row_count: u32,
        payload: Vec<u8>,
        creates: WasmCreateContext,
    },
}

enum WorkerTransitionEvent {
    Page(PendingChangePage),
    Finished(Result<WasmTransitionCounters, LixError>),
}

impl PendingChangePage {
    fn decode(self) -> Result<WasmChangePage, LixError> {
        match self {
            Self::Packet {
                record_count,
                payload,
                max_page_bytes,
                limits,
                ..
            } => super::v2_runtime::decode_inline_change_page(
                record_count,
                payload,
                max_page_bytes,
                limits,
            ),
            Self::TypedCsv {
                row_count,
                payload,
                creates,
            } => decode_typed_csv_rows(row_count, &payload, creates).map_err(v3_error),
        }
    }
}

impl TransitionState {
    fn new(
        limits: WasmTransitionLimits,
        creates: WasmCreateContext,
        events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
        executor_thread_created: bool,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            creates,
            started: Instant::now(),
            total_bytes: 0,
            events,
            counters: WasmTransitionCounters {
                guest_export_calls: 1,
                actor_executor_threads_created: u64::from(executor_thread_created),
                ..WasmTransitionCounters::default()
            },
        })
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 transition deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn charge_page(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 source or sink page exceeds max-page-bytes".to_owned(),
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
        self.total_bytes = self.total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 transition byte count overflowed".to_owned(),
            )
        })?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 transition exceeds max-total-bytes".to_owned(),
            ));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .saturating_add(bytes as u64);
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostRoot for WasiHostState {
    fn generation(&mut self, resource: Resource<RootResource>) -> String {
        self.table
            .get(&resource)
            .expect("v3 root resource must be live")
            .root
            .generation
            .to_string()
    }

    fn file_len(&mut self, resource: Resource<RootResource>) -> u64 {
        self.table
            .get(&resource)
            .expect("v3 root resource must be live")
            .root
            .bytes
            .len()
    }

    fn read_file(
        &mut self,
        resource: Resource<RootResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, bindings::lix::plugin::host::HostError> {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
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
                "v3 root returned a short read".to_owned(),
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
        Ok(bytes)
    }

    fn get_entity(
        &mut self,
        resource: Resource<RootResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, bindings::lix::plugin::host::HostError> {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
        let value = root
            .entities
            .get(&key)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_source(key.len().saturating_add(value.as_ref().map_or(0, Vec::len)))?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(value)
    }

    fn get_state(
        &mut self,
        resource: Resource<RootResource>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, bindings::lix::plugin::host::HostError> {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
        let value = root
            .state
            .get(&key)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_source(key.len().saturating_add(value.as_ref().map_or(0, Vec::len)))?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(value)
    }

    fn state_len(&mut self, resource: Resource<RootResource>, key: Vec<u8>) -> Option<u64> {
        let (root, state) = {
            let resource = self
                .table
                .get(&resource)
                .expect("v3 root resource must be live");
            (resource.root.clone(), resource.state.clone())
        };
        let length = root.state.value_len(&key);
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.component_boundary_bytes = state
            .counters
            .component_boundary_bytes
            .saturating_add(key.len() as u64);
        length
    }

    fn read_state(
        &mut self,
        resource: Resource<RootResource>,
        key: Vec<u8>,
        offset: u64,
        length: u32,
    ) -> Result<Option<Vec<u8>>, bindings::lix::plugin::host::HostError> {
        let (root, state) = {
            let resource = self.table.get(&resource).map_err(host_table_error)?;
            (resource.root.clone(), resource.state.clone())
        };
        let value = root
            .state
            .read(&key, offset, u64::from(length))
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.charge_source(key.len().saturating_add(value.as_ref().map_or(0, Vec::len)))?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(value.as_ref().map_or(0, |bytes| bytes.len()) as u64);
        Ok(value)
    }

    fn drop(&mut self, resource: Resource<RootResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostTransaction for WasiHostState {
    fn put_state(
        &mut self,
        resource: Resource<TransactionResource>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
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
        resource: Resource<TransactionResource>,
        key: Vec<u8>,
    ) -> Result<(), bindings::lix::plugin::host::HostError> {
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

    fn drop(&mut self, resource: Resource<TransactionResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostTransitionSink for WasiHostState {
    fn emit_changes(
        &mut self,
        resource: Resource<SinkResource>,
        page: bindings::lix::plugin::host::ChangePage,
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
        if page.format_version != PACKET_FORMAT_V1 {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 Prototype A requires packet-v1 output".to_owned(),
            ));
        }
        if state.counters.packet_pages >= u64::from(state.limits.max_pages) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 change page count exceeds its limit".to_owned(),
            ));
        }
        state.charge_page(page.payload.len())?;
        if page.record_count == 0 {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 change page is empty".to_owned(),
            ));
        }
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.packet_pages = state.counters.packet_pages.saturating_add(1);
        state.counters.packet_records = state
            .counters
            .packet_records
            .saturating_add(u64::from(page.record_count));
        let limits = state.limits;
        let creates = state.creates;
        let page = PendingChangePage::Packet {
            record_count: page.record_count,
            payload: page.payload,
            max_page_bytes: limits.max_page_bytes,
            limits,
            creates,
        };
        state
            .events
            .blocking_send(WorkerTransitionEvent::Page(page))
            .map_err(|_| {
                bindings::lix::plugin::host::HostError::Rejected(
                    "v3 transition consumer stopped".to_owned(),
                )
            })?;
        Ok(())
    }

    fn emit_csv_rows(
        &mut self,
        resource: Resource<SinkResource>,
        batch: bindings::lix::plugin::host::CsvRowBatch,
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
        if state.counters.packet_pages >= u64::from(state.limits.max_pages) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 CSV batch count exceeds its limit".to_owned(),
            ));
        }
        state.charge_page(batch.payload.len())?;
        if batch.row_count == 0 {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 CSV batch is empty".to_owned(),
            ));
        }
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.packet_pages = state.counters.packet_pages.saturating_add(1);
        state.counters.packet_records = state
            .counters
            .packet_records
            .saturating_add(u64::from(batch.row_count));
        let creates = state.creates;
        let page = PendingChangePage::TypedCsv {
            row_count: batch.row_count,
            payload: batch.payload,
            creates,
        };
        state
            .events
            .blocking_send(WorkerTransitionEvent::Page(page))
            .map_err(|_| {
                bindings::lix::plugin::host::HostError::Rejected(
                    "v3 transition consumer stopped".to_owned(),
                )
            })?;
        Ok(())
    }

    fn drop(&mut self, resource: Resource<SinkResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

fn decode_typed_csv_rows(
    row_count: u32,
    payload: &[u8],
    creates: WasmCreateContext,
) -> Result<WasmChangePage, String> {
    if row_count == 0 {
        return Err("typed CSV batch is empty".to_owned());
    }
    let mut input = TypedCsvReader { payload, offset: 0 };
    let mut snapshots = Vec::with_capacity(payload.len().saturating_mul(2));
    let mut rows = Vec::with_capacity(row_count as usize);
    for _ in 0..row_count {
        let local_ref = u64::from(input.u32()?);
        let order_rank = input.u64()?;
        let ending = input.u8()?;
        if ending > 4 {
            return Err("typed CSV row has an invalid terminator code".to_owned());
        }
        let quote_layout_len = input.u32()? as usize;
        let quote_layout = input.bytes(quote_layout_len)?;
        let field_count = input.u16()?;
        if field_count == 0 {
            return Err("typed CSV row has no fields".to_owned());
        }
        let snapshot_start = snapshots.len();
        snapshots.extend_from_slice(b"{\"cells\":[");
        for field in 0..field_count {
            if field > 0 {
                snapshots.push(b',');
            }
            let cell_len = input.u32()? as usize;
            let cell = input.bytes(cell_len)?;
            let cell = std::str::from_utf8(cell)
                .map_err(|error| format!("typed CSV cell is not UTF-8: {error}"))?;
            write_json_string(&mut snapshots, cell);
        }
        snapshots.push(b']');
        let id = creates
            .component(local_ref)
            .map_err(|error| error.to_string())?;
        snapshots.extend_from_slice(b",\"id\":");
        write_json_string(&mut snapshots, &id);
        if !quote_layout.is_empty() || ending != 0 {
            snapshots.extend_from_slice(b",\"layout\":{");
            let mut comma = false;
            if !quote_layout.is_empty() {
                snapshots.extend_from_slice(b"\"force_quote\":");
                let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(quote_layout);
                write_json_string(&mut snapshots, &encoded);
                comma = true;
            }
            if ending != 0 {
                if comma {
                    snapshots.push(b',');
                }
                snapshots.extend_from_slice(b"\"terminator\":");
                write_json_string(
                    &mut snapshots,
                    match ending {
                        1 => "",
                        2 => "\n",
                        3 => "\r\n",
                        4 => "\r",
                        _ => unreachable!("typed terminator was validated"),
                    },
                );
            }
            snapshots.push(b'}');
        }
        snapshots.extend_from_slice(b",\"order_key\":\"");
        use std::fmt::Write as _;
        write!(&mut VecFormatter(&mut snapshots), "{order_rank:016x}")
            .map_err(|_| "failed to encode CSV order rank".to_owned())?;
        snapshots.extend_from_slice(b"\"}");
        rows.push((
            local_ref,
            WasmEntityKey::from_owned_parts("csv_v2_row", vec![id]),
            snapshot_start,
            snapshots.len(),
        ));
    }
    if input.offset != payload.len() {
        return Err("typed CSV batch has trailing bytes".to_owned());
    }
    let snapshots = Bytes::from(snapshots);
    let changes = rows
        .into_iter()
        .map(
            |(local_ref, resolved_key, start, end)| WasmEntityChange::Create {
                schema_key: "csv_v2_row".to_owned(),
                local_ref,
                resolved_key: Some(resolved_key),
                snapshot_content: WasmGuestBytes::Inline(snapshots.slice(start..end)),
            },
        )
        .collect();
    Ok(WasmChangePage {
        format_version: PACKET_FORMAT_V1,
        changes: WasmGuestEntityChanges { changes },
        outputs: None,
    })
}

fn validate_typed_csv_rows(row_count: u32, payload: &[u8]) -> Result<(u32, u32), String> {
    if row_count == 0 {
        return Err("typed CSV batch is empty".to_owned());
    }
    let mut input = TypedCsvReader { payload, offset: 0 };
    let mut first_local_ref = None;
    let mut previous_local_ref = None;
    for _ in 0..row_count {
        let local_ref = input.u32()?;
        if previous_local_ref.is_some_and(|previous| previous >= local_ref) {
            return Err("typed CSV local refs must be strictly increasing".to_owned());
        }
        first_local_ref.get_or_insert(local_ref);
        previous_local_ref = Some(local_ref);
        let _order_rank = input.u64()?;
        let ending = input.u8()?;
        if ending > 4 {
            return Err("typed CSV row has an invalid terminator code".to_owned());
        }
        let quote_layout_len = input.u32()? as usize;
        let _quote_layout = input.bytes(quote_layout_len)?;
        let field_count = input.u16()?;
        if field_count == 0 {
            return Err("typed CSV row has no fields".to_owned());
        }
        for _ in 0..field_count {
            let cell_len = input.u32()? as usize;
            std::str::from_utf8(input.bytes(cell_len)?)
                .map_err(|error| format!("typed CSV cell is not UTF-8: {error}"))?;
        }
    }
    if input.offset != payload.len() {
        return Err("typed CSV batch has trailing bytes".to_owned());
    }
    Ok((
        first_local_ref.expect("positive row count has a first local ref"),
        previous_local_ref.expect("positive row count has a last local ref"),
    ))
}

/// Returns the schemas when every framed record is an inline snapshot write.
/// Delete or attachment pages fall back to the generic v2 decoder.
fn validate_created_packet_page(
    record_count: u32,
    payload: &[u8],
) -> Result<Option<std::collections::BTreeSet<String>>, String> {
    if record_count == 0 {
        return Err("packet page is empty".to_owned());
    }
    let mut input = TypedCsvReader { payload, offset: 0 };
    let mut schemas = std::collections::BTreeSet::new();
    let mut previous_local_ref = None;
    for _ in 0..record_count {
        let record_len = input.u32()? as usize;
        let record_bytes = input.bytes(record_len)?;
        let mut record = TypedCsvReader {
            payload: record_bytes,
            offset: 0,
        };
        let tag = record.u8()?;
        let schema_len = record.u32()? as usize;
        let schema = std::str::from_utf8(record.bytes(schema_len)?)
            .map_err(|error| format!("packet schema is not UTF-8: {error}"))?;
        if schema.is_empty() {
            return Err("packet create schema is empty".to_owned());
        }
        schemas.insert(schema.to_owned());
        match tag {
            0 => {
                let component_count = record.u32()?;
                if component_count == 0 {
                    return Err("packet upsert key has no components".to_owned());
                }
                for _ in 0..component_count {
                    let component_len = record.u32()? as usize;
                    std::str::from_utf8(record.bytes(component_len)?)
                        .map_err(|error| format!("packet key component is not UTF-8: {error}"))?;
                }
                if record.u8()? > 1 {
                    return Err("packet upsert has an invalid effect".to_owned());
                }
            }
            2 => {
                let local_ref = record.u64()?;
                if previous_local_ref.is_some_and(|previous| previous >= local_ref) {
                    return Err("packet create local refs must be strictly increasing".to_owned());
                }
                previous_local_ref = Some(local_ref);
            }
            1 => return Ok(None),
            _ => return Err("packet page has an unknown change tag".to_owned()),
        }
        if record.u8()? != 0 {
            return Ok(None);
        }
        let snapshot_len = record.u32()? as usize;
        let _snapshot = record.bytes(snapshot_len)?;
        if record.offset != record.payload.len() {
            return Err("packet create record has trailing bytes".to_owned());
        }
    }
    if input.offset != payload.len() {
        return Err("packet page has trailing bytes".to_owned());
    }
    Ok(Some(schemas))
}

struct TypedCsvReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> TypedCsvReader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "typed CSV batch range overflowed".to_owned())?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| "typed CSV batch ended early".to_owned())?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("exact u16 width"),
        ))
    }

    fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("exact u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("exact u64 width"),
        ))
    }
}

struct VecFormatter<'a>(&'a mut Vec<u8>);

impl std::fmt::Write for VecFormatter<'_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0.extend_from_slice(value.as_bytes());
        Ok(())
    }
}

fn write_json_string(output: &mut Vec<u8>, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    for &byte in value.as_bytes() {
        match byte {
            b'"' => output.extend_from_slice(br#"\""#),
            b'\\' => output.extend_from_slice(br#"\\"#),
            b'\n' => output.extend_from_slice(br#"\n"#),
            b'\r' => output.extend_from_slice(br#"\r"#),
            b'\t' => output.extend_from_slice(br#"\t"#),
            0x08 => output.extend_from_slice(br#"\b"#),
            0x0c => output.extend_from_slice(br#"\f"#),
            0x00..=0x1f => {
                output.extend_from_slice(b"\\u00");
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => output.push(byte),
        }
    }
    output.push(b'"');
}

impl bindings::lix::plugin::host::Host for WasiHostState {}

fn host_table_error(
    error: wasmtime::component::ResourceTableError,
) -> bindings::lix::plugin::host::HostError {
    bindings::lix::plugin::host::HostError::Rejected(error.to_string())
}

fn read_source_all(
    source: &Arc<dyn lix_engine::wasm::v2::WasmByteSource>,
) -> Result<Vec<u8>, LixError> {
    const CHUNK_BYTES: u32 = 1024 * 1024;
    let length = source.len();
    let mut output = Vec::with_capacity(
        usize::try_from(length).map_err(|_| v3_error("v3 source exceeds host address space"))?,
    );
    let mut offset = 0_u64;
    while offset < length {
        let chunk = u32::try_from((length - offset).min(u64::from(CHUNK_BYTES)))
            .expect("bounded v3 source read fits u32");
        let bytes = source
            .read(offset, chunk)
            .map_err(|error| v3_error(format!("failed to read v3 source: {error}")))?;
        if bytes.len() != chunk as usize {
            return Err(v3_error("v3 source returned a short read"));
        }
        output.extend_from_slice(&bytes);
        offset += u64::from(chunk);
    }
    Ok(output)
}

struct V3Factory {
    component: Component,
    linker: Arc<Linker<WasiHostState>>,
    runtime: Arc<super::WasmtimeSharedRuntime>,
    limits: WasmLimits,
    profile: CompileProfile,
}

pub(super) async fn compile_component(
    runtime: &WasmtimePluginRuntime,
    bytes: Vec<u8>,
    limits: WasmLimits,
) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
    if limits.max_memory_bytes == 0 {
        return Err(v3_error("v3 component memory limit must be positive"));
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
            Component::new(engine, &bytes).map_err(|error| {
                wasm_runtime_error("failed to compile v3 prototype component", error)
            })
        })
        .await?;
    let mut linker = Linker::<WasiHostState>::new(engine);
    add_to_linker_sync(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure v3 WASI linker", error))?;
    bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |state| {
        state
    })
    .map_err(|error| wasm_runtime_error("failed to configure v3 plugin linker", error))?;
    Ok(Arc::new(V3Factory {
        component,
        linker: Arc::new(linker),
        runtime: runtime.shared.clone(),
        limits,
        profile,
    }))
}

#[async_trait]
impl WasmComponentV2Factory for V3Factory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV2Actor>, LixError> {
        let engine = self.runtime.engine(self.profile);
        let timeout_ticker = self
            .runtime
            .timeout_ticker(self.profile)?
            .ok_or_else(|| v3_error("v3 actor requires an epoch timeout ticker"))?;
        let mut store = create_store(engine, self.limits)?;
        store.epoch_deadline_trap();
        let bindings = bindings::Plugin::instantiate(&mut store, &self.component, &self.linker)
            .map_err(|error| {
                wasm_runtime_error("failed to instantiate v3 prototype actor", error)
            })?;
        let guest = bindings.lix_plugin_api().clone();
        let (sender, receiver) = mpsc::channel();
        let worker = V3Worker {
            store,
            guest,
            limits: self.limits,
            documents: HashMap::new(),
            next_document: 1,
            first_transition: true,
        };
        let thread = std::thread::Builder::new()
            .name("lix-plugin-v3-actor".to_owned())
            .spawn(move || worker.run(receiver))
            .map_err(|error| v3_error(format!("failed to create v3 actor executor: {error}")))?;
        Ok(Box::new(V3Actor {
            sender,
            thread: Some(thread),
            _timeout_ticker: timeout_ticker,
            next_handle: 1,
            cursors: HashMap::new(),
            transitions: HashMap::new(),
            retired: false,
            next_document: 1,
        }))
    }
}

enum WorkerCommand {
    OpenFile {
        document: u64,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
        events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
    },
    FileChanged {
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
        events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
    },
    Fork {
        document: u64,
        response: tokio::sync::oneshot::Sender<Result<u64, LixError>>,
    },
    DropDocument {
        document: u64,
        response: tokio::sync::oneshot::Sender<Result<(), LixError>>,
    },
    Shutdown,
}

struct V3Worker {
    store: Store<WasiHostState>,
    guest: bindings::exports::lix::plugin::api::Guest,
    limits: WasmLimits,
    documents: HashMap<u64, ArenaRoot>,
    next_document: u64,
    first_transition: bool,
}

impl V3Worker {
    fn run(mut self, receiver: mpsc::Receiver<WorkerCommand>) {
        while let Ok(command) = receiver.recv() {
            match command {
                WorkerCommand::OpenFile {
                    document,
                    limits,
                    input,
                    events,
                } => {
                    let result = self.open_file(document, limits, input, events.clone());
                    let _ = events.blocking_send(WorkerTransitionEvent::Finished(result));
                }
                WorkerCommand::FileChanged {
                    document,
                    next_document,
                    limits,
                    update,
                    events,
                } => {
                    let result =
                        self.file_changed(document, next_document, limits, update, events.clone());
                    let _ = events.blocking_send(WorkerTransitionEvent::Finished(result));
                }
                WorkerCommand::Fork { document, response } => {
                    let _ = response.send(self.fork(document));
                }
                WorkerCommand::DropDocument { document, response } => {
                    let _ = response.send(self.drop_document(document));
                }
                WorkerCommand::Shutdown => break,
            }
        }
    }

    fn open_file(
        &mut self,
        document: u64,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
        events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
    ) -> Result<WasmTransitionCounters, LixError> {
        let limits = v3_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            input.creates,
            events,
            std::mem::take(&mut self.first_transition),
        )?));
        let bytes = read_source_all(&input.file)?;
        let arena_store = ArenaStore::default();
        let root = ArenaRoot::import(
            arena_store,
            "csv-v3-arena",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let accepted = self
            .store
            .data_mut()
            .table
            .push(RootResource {
                root: root.clone(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 root: {error}")))?;
        let successor = self
            .store
            .data_mut()
            .table
            .push(TransactionResource {
                transaction: root.transaction(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 transaction: {error}")))?;
        let sink = self
            .store
            .data_mut()
            .table
            .push(SinkResource {
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 sink: {error}")))?;
        let sink_rep = sink.rep();
        let binding_input = bindings::exports::lix::plugin::api::OpenFileInput {
            descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: input.descriptor.path,
                media_type: input.descriptor.media_type,
            },
            accepted,
            successor,
            creates: bindings::exports::lix::plugin::api::CreateContext {
                high: input.creates.high,
                low: input.creates.low,
            },
            max_batch_bytes: limits.max_page_bytes,
        };
        let result = self.guest.call_open_file(
            &mut self.store,
            &binding_input,
            Resource::new_borrow(sink_rep),
        );
        let _ = self.store.data_mut().table.delete(sink);
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                return Err(v3_error(format!("v3 open-file rejected input: {error:?}")));
            }
            Err(error) => {
                return Err(wasm_runtime_error("v3 open-file trapped", error));
            }
        };
        let transaction = self
            .store
            .data_mut()
            .table
            .delete(value.successor)
            .map_err(|error| v3_error(format!("failed to recover v3 transaction: {error}")))?;
        let TransactionResource {
            transaction,
            state: transaction_state,
        } = transaction;
        drop(transaction_state);
        let root = transaction
            .commit()
            .map_err(|error| v3_error(format!("failed to commit v3 arena root: {error}")))?;
        self.documents.insert(document, root);
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 transition resources remained live after open-file"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if value.summary.entity_count != state.counters.packet_records
            || value.summary.batch_count as u64 != state.counters.packet_pages
            || value.summary.payload_bytes > state.counters.component_boundary_bytes
        {
            return Err(v3_error(
                "v3 guest summary does not match emitted packet pages",
            ));
        }
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(state.counters)
    }

    fn file_changed(
        &mut self,
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
        events: tokio::sync::mpsc::Sender<WorkerTransitionEvent>,
    ) -> Result<WasmTransitionCounters, LixError> {
        let limits = v3_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let root = self
            .documents
            .get(&document)
            .cloned()
            .ok_or_else(|| v3_error("unknown v3 document handle"))?;
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            update.creates,
            events,
            false,
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(RootResource {
                root: root.clone(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 before root: {error}")))?;
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
                            .map_err(|_| v3_error("v3 after-range exceeds u32"))?,
                    )
                    .map_err(|error| {
                        v3_error(format!("failed to read v3 after-range bytes: {error}"))
                    })?,
            };
            transaction.edit_bytes(ArenaByteEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: insert.clone(),
            });
            binding_edits.push(bindings::exports::lix::plugin::api::InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: bindings::exports::lix::plugin::api::InputBytes::Inline(insert),
            });
        }
        let successor = self
            .store
            .data_mut()
            .table
            .push(TransactionResource {
                transaction,
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 transaction: {error}")))?;
        let sink = self
            .store
            .data_mut()
            .table
            .push(SinkResource {
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 sink: {error}")))?;
        let sink_rep = sink.rep();
        let binding_update = bindings::exports::lix::plugin::api::FileUpdate {
            before_descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: update.before_descriptor.path,
                media_type: update.before_descriptor.media_type,
            },
            after_descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: update.after_descriptor.path,
                media_type: update.after_descriptor.media_type,
            },
            before,
            edits: binding_edits,
            successor,
            creates: bindings::exports::lix::plugin::api::CreateContext {
                high: update.creates.high,
                low: update.creates.low,
            },
            max_batch_bytes: limits.max_page_bytes,
        };
        let result = self.guest.call_file_changed(
            &mut self.store,
            &binding_update,
            Resource::new_borrow(sink_rep),
        );
        let _ = self.store.data_mut().table.delete(sink);
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                return Err(v3_error(format!(
                    "v3 file-changed rejected input: {error:?}"
                )));
            }
            Err(error) => {
                return Err(wasm_runtime_error("v3 file-changed trapped", error));
            }
        };
        let transaction = self
            .store
            .data_mut()
            .table
            .delete(value.successor)
            .map_err(|error| v3_error(format!("failed to recover v3 transaction: {error}")))?;
        let TransactionResource {
            transaction,
            state: transaction_state,
        } = transaction;
        drop(transaction_state);
        let root = transaction
            .commit()
            .map_err(|error| v3_error(format!("failed to commit v3 arena root: {error}")))?;
        self.documents.insert(next_document, root);
        self.next_document = self.next_document.max(next_document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 transition resources remained live after file-changed"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if value.summary.entity_count != state.counters.packet_records
            || value.summary.batch_count as u64 != state.counters.packet_pages
            || value.summary.payload_bytes > state.counters.component_boundary_bytes
        {
            return Err(v3_error(
                "v3 guest summary does not match emitted packet pages",
            ));
        }
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(state.counters)
    }

    fn fork(&mut self, document: u64) -> Result<u64, LixError> {
        let root = self
            .documents
            .get(&document)
            .cloned()
            .ok_or_else(|| v3_error("unknown v3 document handle"))?;
        let handle = self.next_document;
        self.next_document = self
            .next_document
            .checked_add(1)
            .ok_or_else(|| v3_error("v3 document handle overflowed"))?;
        self.documents.insert(handle, root);
        Ok(handle)
    }

    fn drop_document(&mut self, document: u64) -> Result<(), LixError> {
        self.documents.remove(&document);
        Ok(())
    }
}

struct CursorState {
    transition: WasmTransitionHandle,
    events: tokio::sync::mpsc::Receiver<WorkerTransitionEvent>,
    complete_file_state: bool,
    certified_csv_pages: Vec<Bytes>,
    certified_csv_rows: u64,
    certified_csv_creates: Option<WasmCreateContext>,
    certified_csv_last_local_ref: Option<u32>,
    certified_packet_pages: Vec<Bytes>,
    certified_packet_rows: u64,
    certified_packet_creates: Option<WasmCreateContext>,
    certified_packet_schema_keys: std::collections::BTreeSet<String>,
}

struct V3Actor {
    sender: mpsc::Sender<WorkerCommand>,
    thread: Option<std::thread::JoinHandle<()>>,
    _timeout_ticker: TimeoutTickerLease,
    next_handle: u64,
    cursors: HashMap<u64, CursorState>,
    transitions: HashMap<u64, WasmTransitionCounters>,
    retired: bool,
    next_document: u64,
}

impl V3Actor {
    fn allocate_handle(&mut self) -> Result<u64, LixError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| v3_error("v3 actor handle overflowed"))?;
        Ok(handle)
    }

    fn allocate_document(&mut self) -> Result<u64, LixError> {
        let document = self.next_document;
        self.next_document = self
            .next_document
            .checked_add(1)
            .ok_or_else(|| v3_error("v3 document handle overflowed"))?;
        Ok(document)
    }

    async fn request<T>(
        &mut self,
        build: impl FnOnce(tokio::sync::oneshot::Sender<Result<T, LixError>>) -> WorkerCommand,
    ) -> Result<T, LixError> {
        if self.retired {
            return Err(v3_error("v3 actor is retired"));
        }
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(build(sender))
            .map_err(|_| v3_error("v3 actor executor stopped"))?;
        receiver
            .await
            .map_err(|_| v3_error("v3 actor executor dropped its response"))?
    }
}

#[async_trait]
impl WasmComponentV2Actor for V3Actor {
    async fn fork_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<WasmDocumentHandle, LixError> {
        let fork = self
            .request(|response| WorkerCommand::Fork {
                document: document.0,
                response,
            })
            .await?;
        self.next_document = self.next_document.max(fork.saturating_add(1));
        Ok(WasmDocumentHandle(fork))
    }

    async fn open_file(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<WasmFileTransition, LixError> {
        let document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmChangeCursorHandle(self.allocate_handle()?);
        let (events, receiver) = tokio::sync::mpsc::channel(2);
        self.sender
            .send(WorkerCommand::OpenFile {
                document,
                limits,
                input,
                events,
            })
            .map_err(|_| v3_error("v3 actor executor stopped"))?;
        self.cursors.insert(
            cursor.0,
            CursorState {
                transition,
                events: receiver,
                complete_file_state: true,
                certified_csv_pages: Vec::new(),
                certified_csv_rows: 0,
                certified_csv_creates: None,
                certified_csv_last_local_ref: None,
                certified_packet_pages: Vec::new(),
                certified_packet_rows: 0,
                certified_packet_creates: None,
                certified_packet_schema_keys: std::collections::BTreeSet::new(),
            },
        );
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(document),
            changes: cursor,
        })
    }

    async fn open_entities(
        &mut self,
        _limits: WasmTransitionLimits,
        _input: WasmOpenEntitiesInput,
    ) -> Result<WasmEntityTransition, LixError> {
        Err(v3_unsupported("open-entities"))
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
        let (events, receiver) = tokio::sync::mpsc::channel(2);
        self.sender
            .send(WorkerCommand::FileChanged {
                document: document.0,
                next_document,
                limits,
                update,
                events,
            })
            .map_err(|_| v3_error("v3 actor executor stopped"))?;
        self.cursors.insert(
            cursor.0,
            CursorState {
                transition,
                events: receiver,
                complete_file_state: false,
                certified_csv_pages: Vec::new(),
                certified_csv_rows: 0,
                certified_csv_creates: None,
                certified_csv_last_local_ref: None,
                certified_packet_pages: Vec::new(),
                certified_packet_rows: 0,
                certified_packet_creates: None,
                certified_packet_schema_keys: std::collections::BTreeSet::new(),
            },
        );
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(next_document),
            changes: cursor,
        })
    }

    async fn entities_changed(
        &mut self,
        _document: WasmDocumentHandle,
        _limits: WasmTransitionLimits,
        _update: WasmEntityUpdate,
    ) -> Result<WasmEntityTransition, LixError> {
        Err(v3_unsupported("entities-changed"))
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
            .ok_or_else(|| v3_error("unknown v3 change cursor"))?;
        if cursor.transition != transition {
            return Err(v3_error("v3 change cursor belongs to another transition"));
        }
        loop {
            match cursor.events.recv().await {
                Some(WorkerTransitionEvent::Page(PendingChangePage::TypedCsv {
                    row_count,
                    payload,
                    creates,
                })) => {
                    let (first_local_ref, last_local_ref) =
                        validate_typed_csv_rows(row_count, &payload).map_err(v3_error)?;
                    if cursor
                        .certified_csv_last_local_ref
                        .is_some_and(|previous| previous >= first_local_ref)
                    {
                        return Err(v3_error(
                            "certified CSV local refs must increase across pages",
                        ));
                    }
                    if cursor
                        .certified_csv_creates
                        .is_some_and(|existing| existing != creates)
                    {
                        return Err(v3_error(
                            "one certified CSV transition used multiple create contexts",
                        ));
                    }
                    cursor.certified_csv_creates = Some(creates);
                    cursor.certified_csv_rows = cursor
                        .certified_csv_rows
                        .checked_add(u64::from(row_count))
                        .ok_or_else(|| v3_error("certified CSV row count overflowed"))?;
                    cursor.certified_csv_last_local_ref = Some(last_local_ref);
                    cursor.certified_csv_pages.push(Bytes::from(payload));
                }
                Some(WorkerTransitionEvent::Page(page @ PendingChangePage::Packet { .. })) => {
                    let PendingChangePage::Packet {
                        record_count,
                        payload,
                        max_page_bytes,
                        limits,
                        creates,
                    } = page
                    else {
                        unreachable!("matched packet page")
                    };
                    if let Some(schema_keys) =
                        validate_created_packet_page(record_count, &payload).map_err(v3_error)?
                    {
                        // Certified immutable segments are the ownership unit
                        // for complete bulk state. Sparse successors stay as
                        // ordinary row overlays: this lets validation observe
                        // their durable base directly and avoids paying a new
                        // segment/manifest lifecycle for one or two rows.
                        if !cursor.complete_file_state {
                            return PendingChangePage::Packet {
                                record_count,
                                payload,
                                max_page_bytes,
                                limits,
                                creates,
                            }
                            .decode()
                            .map(Some);
                        }
                        if cursor
                            .certified_packet_creates
                            .is_some_and(|existing| existing != creates)
                        {
                            return Err(v3_error(
                                "one certified packet transition used multiple create contexts",
                            ));
                        }
                        cursor.certified_packet_creates = Some(creates);
                        cursor.certified_packet_rows = cursor
                            .certified_packet_rows
                            .checked_add(u64::from(record_count))
                            .ok_or_else(|| v3_error("certified packet row count overflowed"))?;
                        cursor.certified_packet_schema_keys.extend(schema_keys);
                        cursor.certified_packet_pages.push(Bytes::from(payload));
                    } else {
                        return PendingChangePage::Packet {
                            record_count,
                            payload,
                            max_page_bytes,
                            limits,
                            creates,
                        }
                        .decode()
                        .map(Some);
                    }
                }
                Some(WorkerTransitionEvent::Finished(result)) => {
                    let counters = result?;
                    self.transitions.insert(transition.0, counters);
                    return Ok(None);
                }
                None => return Err(v3_error("v3 transition producer stopped")),
            }
        }
    }

    fn take_certified_entity_batches(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Vec<WasmCertifiedEntityBatch> {
        let Some(cursor) = self
            .cursors
            .values_mut()
            .find(|cursor| cursor.transition == transition)
        else {
            return Vec::new();
        };
        let mut batches = Vec::with_capacity(2);
        if let Some(creates) = cursor.certified_csv_creates.take() {
            batches.push(WasmCertifiedEntityBatch {
                format: CERTIFIED_TYPED_CSV_V1,
                schema_keys: vec!["csv_v2_row".to_owned()],
                row_count: std::mem::take(&mut cursor.certified_csv_rows),
                creates,
                complete_file_state: cursor.complete_file_state,
                pages: std::mem::take(&mut cursor.certified_csv_pages),
            });
        }
        if let Some(creates) = cursor.certified_packet_creates.take() {
            batches.push(WasmCertifiedEntityBatch {
                format: CERTIFIED_CREATED_PACKET_V1,
                schema_keys: std::mem::take(&mut cursor.certified_packet_schema_keys)
                    .into_iter()
                    .collect(),
                row_count: std::mem::take(&mut cursor.certified_packet_rows),
                creates,
                complete_file_state: cursor.complete_file_state,
                pages: std::mem::take(&mut cursor.certified_packet_pages),
            });
        }
        batches
    }

    async fn next_edit_page(
        &mut self,
        _transition: WasmTransitionHandle,
        _cursor: WasmEditCursorHandle,
        _max_edits: u32,
        _max_inline_bytes: u32,
    ) -> Result<Option<WasmEditPage>, LixError> {
        Err(v3_unsupported("edit cursor"))
    }

    async fn output_len(
        &mut self,
        _transition: WasmTransitionHandle,
        _outputs: WasmByteOutputsHandle,
        _index: u32,
    ) -> Result<u64, LixError> {
        Err(v3_unsupported("output attachment"))
    }

    async fn read_output(
        &mut self,
        _transition: WasmTransitionHandle,
        _outputs: WasmByteOutputsHandle,
        _index: u32,
        _offset: u64,
        _length: u32,
    ) -> Result<Vec<u8>, LixError> {
        Err(v3_unsupported("output attachment"))
    }

    async fn finish_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<WasmTransitionCounters, LixError> {
        self.cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.transitions
            .remove(&transition.0)
            .ok_or_else(|| v3_error("unknown v3 transition"))
    }

    async fn discard_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<(), LixError> {
        self.cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.transitions.remove(&transition.0);
        Ok(())
    }

    fn is_retired(&self) -> bool {
        self.retired
    }

    async fn drop_document(&mut self, document: WasmDocumentHandle) -> Result<(), LixError> {
        self.request(|response| WorkerCommand::DropDocument {
            document: document.0,
            response,
        })
        .await
    }

    async fn retire(&mut self) -> Result<(), LixError> {
        if !self.retired {
            self.retired = true;
            let _ = self.sender.send(WorkerCommand::Shutdown);
            if let Some(thread) = self.thread.take() {
                thread
                    .join()
                    .map_err(|_| v3_error("v3 actor executor panicked"))?;
            }
        }
        Ok(())
    }
}

impl Drop for V3Actor {
    fn drop(&mut self) {
        let _ = self.sender.send(WorkerCommand::Shutdown);
    }
}

fn v3_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn v3_transition_limits(
    mut limits: WasmTransitionLimits,
) -> Result<WasmTransitionLimits, LixError> {
    limits.max_page_bytes = V3_MAX_BATCH_BYTES.min(
        u32::try_from(limits.max_total_bytes)
            .unwrap_or(u32::MAX)
            .max(limits.max_record_bytes),
    );
    // v3 batches are the record envelope. Keeping the inherited v2 1 MiB
    // record cap while advertising a 2 MiB push page rejects otherwise valid
    // single-snapshot pages (notably Markdown lexical fallbacks).
    limits.max_record_bytes = limits.max_page_bytes;
    limits.validate()
}

fn v3_unsupported(operation: &str) -> LixError {
    v3_error(format!(
        "Component v3 Prototype A does not implement {operation}"
    ))
}
