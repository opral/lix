//! Host-owned arena runtime for the fused Component API.
//!
//! File bytes and opaque plugin state live in immutable host roots; one
//! exported guest call reads sparse ranges and pushes bounded semantic pages.

use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Instant;

use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use lix_engine::wasm::WasmLimits;
use lix_engine::wasm::v3::{
    ByteEdit as ArenaByteEdit, Root as ArenaRoot, Store as ArenaStore,
    Transaction as ArenaTransaction,
};
use lix_engine::wasm::{
    PACKET_FORMAT_V1, WasmByteOutputsHandle, WasmCertifiedEntityBatch, WasmChangeCursorHandle,
    WasmChangeEffect, WasmChangePage, WasmComponentActor, WasmComponentFactory,
    WasmConflictResolution, WasmConflictResolutionPage, WasmConflictTake, WasmConflictTransition,
    WasmConflictUpdate, WasmCreateContext, WasmDocumentHandle, WasmEditCursorHandle, WasmEditPage,
    WasmEntity, WasmEntityChange, WasmEntityChanges, WasmEntityKey, WasmEntityTransition,
    WasmEntityUpdate, WasmFileTransition, WasmFileUpdate, WasmGuestBytes, WasmGuestEntityChanges,
    WasmHostBytes, WasmHostEntityConflict, WasmInputBytes, WasmOpenEntitiesInput,
    WasmOpenFileInput, WasmOutputRange, WasmOutputSplice, WasmResolutionCursorHandle,
    WasmTransitionCounters, WasmTransitionHandle, WasmTransitionLimits,
};
use lix_engine::{LixError, SharedStr};
use wasmtime::Store;
use wasmtime::component::{Component, Linker, Resource, ResourceTable};

use super::{
    CompileProfile, CompiledComponentKey, TimeoutTickerLease, WasiHostState, WasmtimePluginRuntime,
    add_to_linker_sync, create_store, reset_store_limits, wasm_runtime_error,
};

const V3_MAX_BATCH_BYTES: u32 = 2 * 1024 * 1024;
const CERTIFIED_TYPED_CSV_V1: u16 = 1;
const CERTIFIED_CREATED_PACKET_V1: u16 = 2;

fn take_borrowed_resource<T: Send + 'static>(
    table: &mut ResourceTable,
    resource: Resource<T>,
    context: &str,
) -> Result<T, LixError> {
    table
        .delete(resource)
        .map_err(|error| v3_error(format!("{context}: {error}")))
}

pub(super) mod bindings {
    wasmtime::component::bindgen!({
        path: "../plugin-api/wit",
        world: "plugin",
        with: {
            "lix:plugin/host.snapshot": super::SnapshotResource,
            "lix:plugin/host.transition": super::TransitionResource,
            "lix:plugin/host.conflict-source": super::ConflictSourceResource,
            "lix:plugin/host.entity-change-source": super::EntityChangeSourceResource,
            "lix:plugin/host.resolution-sink": super::ResolutionSinkResource,
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

pub struct EntityChangeSourceResource {
    state: SharedEntityChangeState,
}

pub struct ResolutionSinkResource {
    state: SharedResolutionState,
}

type SharedTransitionState = Arc<Mutex<TransitionState>>;
type SharedResolutionState = Arc<Mutex<ResolutionState>>;
type SharedEntityChangeState = Arc<Mutex<EntityChangeState>>;

struct TransitionState {
    limits: WasmTransitionLimits,
    creates: WasmCreateContext,
    started: Instant,
    total_bytes: u64,
    events: Option<tokio::sync::mpsc::Sender<WorkerTransitionEvent>>,
    counters: WasmTransitionCounters,
    allow_file_replacement: bool,
    file_replacement: Option<PendingFileReplacement>,
}

struct PendingFileReplacement {
    expected_len: u64,
    bytes: Vec<u8>,
    complete: bool,
}

struct EntityChangeState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: u64,
    changes: Vec<WasmEntityChange<WasmHostBytes>>,
    counters: WasmTransitionCounters,
}

struct ResolutionState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: u64,
    conflicts: Vec<WasmHostEntityConflict>,
    resolutions: Vec<V3Resolution>,
    pending: Option<PendingReplacement>,
    counters: WasmTransitionCounters,
}

enum V3Resolution {
    Take(WasmConflictTake),
    Replace {
        snapshot: Bytes,
        effect: WasmChangeEffect,
    },
    Delete,
}

struct PendingReplacement {
    ordinal: u32,
    effect: WasmChangeEffect,
    expected_len: u64,
    bytes: Vec<u8>,
}

/// Host-owned wire pages retained until the engine asks for the next page.
///
/// The first fused implementation decoded every pushed page immediately and retained
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
            } => decode_inline_change_page(record_count, payload, max_page_bytes, limits),
            Self::TypedCsv {
                row_count,
                payload,
                creates,
            } => decode_typed_csv_rows(row_count, &payload, creates).map_err(v3_error),
        }
    }
}

fn decode_inline_change_page(
    record_count: u32,
    payload: Vec<u8>,
    max_bytes: u32,
    limits: WasmTransitionLimits,
) -> Result<WasmChangePage, LixError> {
    if record_count == 0 {
        return Err(v3_error("guest returned a zero-record change page"));
    }
    if max_bytes == 0 || max_bytes > limits.max_page_bytes {
        return Err(component_limit(
            "change page max-bytes is outside its transition limit",
        ));
    }
    if payload.len() > max_bytes as usize {
        return Err(component_limit(
            "guest change payload exceeds the requested max-bytes",
        ));
    }
    let record_count = usize::try_from(record_count)
        .map_err(|_| v3_error("guest record count exceeds host bounds"))?;
    if record_count > payload.len() / size_of::<u32>() {
        return Err(v3_error(
            "guest record count exceeds its bounded payload framing",
        ));
    }

    let payload = Bytes::from(payload);
    let mut framed = PacketReader::new(&payload);
    let row_size = size_of::<WasmEntityChange<WasmGuestBytes>>();
    let capacity = record_count.min(payload.len().checked_div(row_size).unwrap_or(record_count));
    let mut changes = Vec::with_capacity(capacity);
    for _ in 0..record_count {
        let record_len = framed.read_u32()? as usize;
        if record_len > limits.max_record_bytes as usize {
            return Err(component_limit(format!(
                "record is too large: {record_len} bytes"
            )));
        }
        let mut record = framed.read_reader(record_len)?;
        let change = match record.read_u8()? {
            0 => {
                let key = decode_entity_key(&mut record)?;
                let effect = match record.read_u8()? {
                    0 => WasmChangeEffect::Content,
                    1 => WasmChangeEffect::FormatOnly,
                    _ => return Err(v3_error("unknown change effect tag")),
                };
                WasmEntityChange::Upsert {
                    entity: WasmEntity {
                        key,
                        snapshot_content: decode_inline_guest_blob(&mut record)?,
                    },
                    effect,
                }
            }
            1 => WasmEntityChange::Delete(decode_entity_key(&mut record)?),
            2 => WasmEntityChange::Create {
                schema_key: record.read_text()?.to_string(),
                local_ref: record.read_u64()?,
                resolved_key: None,
                snapshot_content: decode_inline_guest_blob(&mut record)?,
            },
            _ => return Err(v3_error("unknown change tag")),
        };
        record.finish()?;
        changes.push(change);
    }
    framed.finish()?;
    Ok(WasmChangePage {
        format_version: PACKET_FORMAT_V1,
        changes: WasmEntityChanges { changes },
        outputs: None,
    })
}

fn decode_entity_key(reader: &mut PacketReader<'_>) -> Result<WasmEntityKey, LixError> {
    let schema_key = reader.read_text()?;
    let pk_count = reader.read_u32()?;
    if pk_count as usize > reader.remaining() / size_of::<u32>() {
        return Err(v3_error(
            "entity primary-key component count exceeds packet bounds",
        ));
    }
    let mut key = WasmEntityKey::from_shared_parts(schema_key, std::iter::empty());
    for _ in 0..pk_count {
        key.entity_pk.push(reader.read_text()?);
    }
    Ok(key)
}

fn decode_inline_guest_blob(reader: &mut PacketReader<'_>) -> Result<WasmGuestBytes, LixError> {
    match reader.read_u8()? {
        0 => {
            let length = reader.read_u32()? as usize;
            Ok(WasmGuestBytes::Inline(reader.read_bytes(length)?))
        }
        1 => Err(v3_error(
            "packet-v1 push pages cannot contain output attachments",
        )),
        _ => Err(v3_error("unknown blob-reference tag")),
    }
}

struct PacketReader<'a> {
    bytes: &'a Bytes,
    offset: usize,
    end: usize,
}

impl<'a> PacketReader<'a> {
    fn new(bytes: &'a Bytes) -> Self {
        Self {
            bytes,
            offset: 0,
            end: bytes.len(),
        }
    }

    fn remaining(&self) -> usize {
        self.end.saturating_sub(self.offset)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| v3_error("packet range overflowed"))?;
        if end > self.end {
            return Err(v3_error("truncated packet"));
        }
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| v3_error("truncated packet"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_bytes(&mut self, length: usize) -> Result<Bytes, LixError> {
        let start = self.offset;
        self.read_exact(length)?;
        Ok(self.bytes.slice(start..self.offset))
    }

    fn read_reader(&mut self, length: usize) -> Result<Self, LixError> {
        let start = self.offset;
        self.read_exact(length)?;
        Ok(Self {
            bytes: self.bytes,
            offset: start,
            end: self.offset,
        })
    }

    fn read_u8(&mut self) -> Result<u8, LixError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?
                .try_into()
                .map_err(|_| v3_error("invalid u32 field"))?,
        ))
    }

    fn read_u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?
                .try_into()
                .map_err(|_| v3_error("invalid u64 field"))?,
        ))
    }

    fn read_text(&mut self) -> Result<SharedStr, LixError> {
        let length = self.read_u32()? as usize;
        let bytes = self.read_exact(length)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| v3_error("packet text is not valid UTF-8"))?;
        SharedStr::from_utf8_slice(self.bytes.clone(), value)
            .ok_or_else(|| v3_error("packet text is outside its page allocation"))
    }

    fn finish(&self) -> Result<(), LixError> {
        if self.offset != self.end {
            return Err(v3_error("packet contains trailing bytes"));
        }
        Ok(())
    }
}

fn component_limit(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

impl TransitionState {
    fn new(
        limits: WasmTransitionLimits,
        creates: WasmCreateContext,
        events: Option<tokio::sync::mpsc::Sender<WorkerTransitionEvent>>,
        executor_thread_created: bool,
        allow_file_replacement: bool,
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
            allow_file_replacement,
            file_replacement: None,
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

impl ResolutionState {
    fn new(
        limits: WasmTransitionLimits,
        conflicts: Vec<WasmHostEntityConflict>,
        executor_thread_created: bool,
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
                actor_executor_threads_created: u64::from(executor_thread_created),
                ..WasmTransitionCounters::default()
            },
        })
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 conflict deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 conflict chunk exceeds max-page-bytes".to_owned(),
            ));
        }
        self.total_bytes = self.total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 conflict byte count overflowed".to_owned(),
            )
        })?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 conflict transition exceeds max-total-bytes".to_owned(),
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
                "v3 conflict resolution count exceeds u32".to_owned(),
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
                "v3 conflict resolution ordinal {ordinal}, expected {expected}"
            )));
        }
        let index = ordinal as usize;
        if self.conflicts.get(index).is_none() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 conflict resolver returned excess output".to_owned(),
            ));
        }
        Ok(index)
    }
}

impl EntityChangeState {
    fn new(
        limits: WasmTransitionLimits,
        changes: Vec<WasmEntityChange<WasmHostBytes>>,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            total_bytes: 0,
            changes,
            counters: WasmTransitionCounters::default(),
        })
    }

    fn check_active(&self) -> Result<(), bindings::lix::plugin::host::HostError> {
        if self.started.elapsed().as_nanos() >= u128::from(self.limits.total_deadline_nanoseconds) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 entity transition deadline elapsed".to_owned(),
            ));
        }
        Ok(())
    }

    fn charge(&mut self, bytes: usize) -> Result<(), bindings::lix::plugin::host::HostError> {
        self.check_active()?;
        if bytes > self.limits.max_page_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 entity-change chunk exceeds max-page-bytes".to_owned(),
            ));
        }
        self.total_bytes = self.total_bytes.checked_add(bytes as u64).ok_or_else(|| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 entity-change byte count overflowed".to_owned(),
            )
        })?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 entity transition exceeds max-total-bytes".to_owned(),
            ));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .saturating_add(bytes as u64);
        Ok(())
    }
}

fn conflict_value<'a>(
    conflict: &'a WasmHostEntityConflict,
    side: bindings::lix::plugin::host::ConflictSide,
) -> Option<&'a WasmHostBytes> {
    match side {
        bindings::lix::plugin::host::ConflictSide::Base => conflict.base.as_ref(),
        bindings::lix::plugin::host::ConflictSide::A => conflict.a.as_ref(),
        bindings::lix::plugin::host::ConflictSide::B => conflict.b.as_ref(),
    }
}

fn read_host_bytes(value: &WasmHostBytes, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| v3_error("v3 conflict range overflowed"))?;
    if end > value.len() {
        return Err(v3_error("v3 conflict range is out of bounds"));
    }
    match value {
        WasmHostBytes::Inline(bytes) => Ok(bytes.slice(offset as usize..end as usize).to_vec()),
        WasmHostBytes::CanonicalJson(json) => {
            Ok(json.normalized().as_bytes()[offset as usize..end as usize].to_vec())
        }
        WasmHostBytes::Source(slice) => slice
            .source
            .read(
                slice
                    .range
                    .offset
                    .checked_add(offset)
                    .ok_or_else(|| v3_error("v3 conflict source offset overflowed"))?,
                length,
            )
            .map_err(|error| v3_error(format!("failed to read v3 conflict source: {error}"))),
    }
}

#[cfg(test)]
fn create_context_from_uuid(value: &str) -> Option<WasmCreateContext> {
    let id = uuid::Uuid::parse_str(value).ok()?;
    if id.to_string() != value {
        return None;
    }
    let bytes = id.into_bytes();
    Some(WasmCreateContext {
        high: u64::from_be_bytes(bytes[..8].try_into().expect("eight UUID bytes")),
        low: u32::from_be_bytes(bytes[8..12].try_into().expect("four UUID bytes")),
    })
}

#[cfg(test)]
fn create_context_from_generated_entity(
    plugin_key: &str,
    entity: &WasmEntity<WasmHostBytes>,
) -> Option<WasmCreateContext> {
    let generated_schema = match plugin_key {
        "plugin_csv" => "csv_v2_row",
        "plugin_json" => "json_array_item",
        "plugin_markdown" => "markdown_node_v2",
        "plugin_git_text" => "git_text_line_v2",
        // Excalidraw identities are native format IDs, not host-generated IDs.
        _ => return None,
    };
    if entity.key.schema_key.as_str() != generated_schema {
        return None;
    }
    let [id] = entity.key.entity_pk.as_slice() else {
        return None;
    };
    create_context_from_uuid(id.as_str())
}

impl bindings::lix::plugin::host::HostSnapshot for WasiHostState {
    fn file_len(&mut self, resource: Resource<SnapshotResource>) -> u64 {
        self.table
            .get(&resource)
            .expect("v3 root resource must be live")
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

    fn read_record(
        &mut self,
        resource: Resource<SnapshotResource>,
        space: bindings::lix::plugin::host::MapSpace,
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
        let total_len = match space {
            bindings::lix::plugin::host::MapSpace::Entity => root.entities.value_len(&key),
            bindings::lix::plugin::host::MapSpace::State => root.state.value_len(&key),
        };
        let Some(total_len) = total_len else {
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_source(key.len())?;
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
            return Ok(None);
        };
        if offset > total_len {
            return Err(bindings::lix::plugin::host::HostError::InvalidRange);
        }
        let length = u64::from(max_bytes).min(total_len - offset);
        let value = match space {
            bindings::lix::plugin::host::MapSpace::Entity => {
                root.entities.read(&key, offset, length)
            }
            bindings::lix::plugin::host::MapSpace::State => root.state.read(&key, offset, length),
        }
        .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.to_string()))?;
        let bytes = value.ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "v3 snapshot record disappeared during read".to_owned(),
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
            .expect("v3 transition resource must be live")
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
                "v3 host-reserved state key".to_owned(),
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
                "v3 host-reserved state key".to_owned(),
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

    fn emit_changes(
        &mut self,
        resource: Resource<TransitionResource>,
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
        if let Some(events) = &state.events {
            events
                .blocking_send(WorkerTransitionEvent::Page(page))
                .map_err(|_| {
                    bindings::lix::plugin::host::HostError::Rejected(
                        "v3 transition consumer stopped".to_owned(),
                    )
                })?;
        }
        Ok(())
    }

    fn emit_packed(
        &mut self,
        resource: Resource<TransitionResource>,
        batch: bindings::lix::plugin::host::PackedPage,
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
        if batch.codec != "lix.csv.rows" || batch.format_version != CERTIFIED_TYPED_CSV_V1 {
            return Err(bindings::lix::plugin::host::HostError::Rejected(format!(
                "unsupported v3 packed codec '{}'/{}",
                batch.codec, batch.format_version
            )));
        }
        if state.counters.packet_pages >= u64::from(state.limits.max_pages) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 CSV batch count exceeds its limit".to_owned(),
            ));
        }
        state.charge_page(batch.payload.len())?;
        if batch.record_count == 0 {
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
            .saturating_add(u64::from(batch.record_count));
        let creates = state.creates;
        let page = PendingChangePage::TypedCsv {
            row_count: batch.record_count,
            payload: batch.payload,
            creates,
        };
        if let Some(events) = &state.events {
            events
                .blocking_send(WorkerTransitionEvent::Page(page))
                .map_err(|_| {
                    bindings::lix::plugin::host::HostError::Rejected(
                        "v3 transition consumer stopped".to_owned(),
                    )
                })?;
        }
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

impl bindings::lix::plugin::host::HostEntityChangeSource for WasiHostState {
    fn len(&mut self, resource: Resource<EntityChangeSourceResource>) -> u32 {
        u32::try_from(
            self.table
                .get(&resource)
                .expect("v3 entity-change source must be live")
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .changes
                .len(),
        )
        .unwrap_or(u32::MAX)
    }

    fn get(
        &mut self,
        resource: Resource<EntityChangeSourceResource>,
        index: u32,
    ) -> Result<bindings::lix::plugin::host::EntityChangeMeta, bindings::lix::plugin::host::HostError>
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
        let change = state
            .changes
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let (schema_key, entity_pk, snapshot_len, effect) = match change {
            WasmEntityChange::Create {
                schema_key,
                resolved_key,
                snapshot_content,
                ..
            } => (
                schema_key.clone(),
                resolved_key
                    .as_ref()
                    .map(|key| key.entity_pk.iter().map(ToString::to_string).collect())
                    .unwrap_or_default(),
                Some(snapshot_content.len()),
                WasmChangeEffect::Content,
            ),
            WasmEntityChange::Upsert { entity, effect } => (
                entity.key.schema_key.to_string(),
                entity
                    .key
                    .entity_pk
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                Some(entity.snapshot_content.len()),
                *effect,
            ),
            WasmEntityChange::Delete(key) => (
                key.schema_key.to_string(),
                key.entity_pk.iter().map(ToString::to_string).collect(),
                None,
                WasmChangeEffect::Content,
            ),
        };
        let meta = bindings::lix::plugin::host::EntityChangeMeta {
            ordinal: index,
            schema_key,
            entity_pk,
            snapshot_len,
            effect: match effect {
                WasmChangeEffect::Content => bindings::lix::plugin::host::ChangeEffect::Content,
                WasmChangeEffect::FormatOnly => {
                    bindings::lix::plugin::host::ChangeEffect::FormatOnly
                }
            },
        };
        let metadata_bytes =
            meta.schema_key.len() + meta.entity_pk.iter().map(String::len).sum::<usize>() + 24;
        state.charge(metadata_bytes)?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(meta)
    }

    fn read_snapshot(
        &mut self,
        resource: Resource<EntityChangeSourceResource>,
        index: u32,
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
        let change = state
            .changes
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let value = match change {
            WasmEntityChange::Create {
                snapshot_content, ..
            } => Some(snapshot_content),
            WasmEntityChange::Upsert { entity, .. } => Some(&entity.snapshot_content),
            WasmEntityChange::Delete(_) => None,
        };
        let Some(value) = value else {
            return Ok(None);
        };
        let bytes = read_host_bytes(value, offset, length)
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

    fn drop(&mut self, resource: Resource<EntityChangeSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostConflictSource for WasiHostState {
    fn len(&mut self, resource: Resource<ConflictSourceResource>) -> u32 {
        u32::try_from(
            self.table
                .get(&resource)
                .expect("v3 conflict source must be live")
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
    ) -> Result<bindings::lix::plugin::host::ConflictMeta, bindings::lix::plugin::host::HostError>
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
        let meta = bindings::lix::plugin::host::ConflictMeta {
            ordinal: conflict.ordinal,
            schema_key: conflict.key.schema_key.to_string(),
            entity_pk: conflict
                .key
                .entity_pk
                .iter()
                .map(ToString::to_string)
                .collect(),
            base_len: conflict.base.as_ref().map(WasmHostBytes::len),
            a_len: conflict.a.as_ref().map(WasmHostBytes::len),
            b_len: conflict.b.as_ref().map(WasmHostBytes::len),
        };
        let metadata_bytes =
            meta.schema_key.len() + meta.entity_pk.iter().map(String::len).sum::<usize>() + 32;
        state.charge(metadata_bytes)?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(meta)
    }

    fn read_value(
        &mut self,
        resource: Resource<ConflictSourceResource>,
        index: u32,
        side: bindings::lix::plugin::host::ConflictSide,
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
        let conflict = state
            .conflicts
            .get(index as usize)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let Some(value) = conflict_value(conflict, side) else {
            return Ok(None);
        };
        let bytes = read_host_bytes(value, offset, length)
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

    fn drop(&mut self, resource: Resource<ConflictSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostResolutionSink for WasiHostState {
    fn max_batch_bytes(&mut self, resource: Resource<ResolutionSinkResource>) -> u32 {
        self.table
            .get(&resource)
            .expect("v3 resolution sink must be live")
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits
            .max_page_bytes
    }

    fn take(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
        side: bindings::lix::plugin::host::ConflictSide,
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
                "v3 replacement is incomplete".to_owned(),
            ));
        }
        let index = state.validate_ordinal(ordinal)?;
        if conflict_value(&state.conflicts[index], side).is_none() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 conflict resolver selected an absent side".to_owned(),
            ));
        }
        let take = match side {
            bindings::lix::plugin::host::ConflictSide::Base => WasmConflictTake::Base,
            bindings::lix::plugin::host::ConflictSide::A => WasmConflictTake::A,
            bindings::lix::plugin::host::ConflictSide::B => WasmConflictTake::B,
        };
        state.resolutions.push(V3Resolution::Take(take));
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.conflict_resolution_takes =
            state.counters.conflict_resolution_takes.saturating_add(1);
        Ok(())
    }

    fn delete(
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
                "v3 replacement is incomplete".to_owned(),
            ));
        }
        state.validate_ordinal(ordinal)?;
        state.resolutions.push(V3Resolution::Delete);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn begin_replace(
        &mut self,
        resource: Resource<ResolutionSinkResource>,
        ordinal: u32,
        effect: bindings::lix::plugin::host::ResolutionEffect,
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
        if state.pending.is_some() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 replacement is already open".to_owned(),
            ));
        }
        state.validate_ordinal(ordinal)?;
        if total_length > state.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 replacement exceeds max-total-bytes".to_owned(),
            ));
        }
        let capacity = usize::try_from(total_length).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "v3 replacement exceeds host address space".to_owned(),
            )
        })?;
        state.pending = Some(PendingReplacement {
            ordinal,
            effect: match effect {
                bindings::lix::plugin::host::ResolutionEffect::Content => WasmChangeEffect::Content,
                bindings::lix::plugin::host::ResolutionEffect::FormatOnly => {
                    WasmChangeEffect::FormatOnly
                }
            },
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
                "v3 replacement chunk has no open replacement".to_owned(),
            )
        })?;
        let next_len = pending
            .bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "v3 replacement length overflowed".to_owned(),
                )
            })?;
        if next_len as u64 > pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 replacement exceeds its declared length".to_owned(),
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
                "v3 replacement finish has no open replacement".to_owned(),
            )
        })?;
        if pending.bytes.len() as u64 != pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "v3 replacement ended before its declared length".to_owned(),
            ));
        }
        debug_assert_eq!(pending.ordinal as usize, state.resolutions.len());
        state.resolutions.push(V3Resolution::Replace {
            snapshot: Bytes::from(pending.bytes),
            effect: pending.effect,
        });
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn drop(&mut self, resource: Resource<ResolutionSinkResource>) -> wasmtime::Result<()> {
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

struct ValidatedCreatedPacketPage {
    schemas: std::collections::BTreeSet<String>,
    identities: Vec<CreatedPacketIdentity>,
}

enum CreatedPacketIdentity {
    Explicit(WasmEntityKey),
    Create { schema_key: String, local_ref: u64 },
}

#[derive(Debug, Default)]
struct CertifiedPacketSchemaKeys {
    create_refs: std::collections::BTreeSet<u64>,
    explicit_keys: std::collections::BTreeSet<Vec<String>>,
    explicit_create_refs: std::collections::BTreeSet<u64>,
    create_ref_ranges: Vec<(u64, u64)>,
}

#[derive(Debug, Default)]
struct CertifiedPacketEntityKeys {
    schemas: std::collections::BTreeMap<String, CertifiedPacketSchemaKeys>,
}

impl CertifiedPacketEntityKeys {
    fn contains_create_ref(keys: &CertifiedPacketSchemaKeys, local_ref: u64) -> bool {
        keys.create_refs.contains(&local_ref)
            || keys
                .create_ref_ranges
                .iter()
                .any(|(first, last)| *first <= local_ref && local_ref <= *last)
    }

    fn generated_local_ref(components: &[String], creates: WasmCreateContext) -> Option<u64> {
        let [component] = components else {
            return None;
        };
        let id = uuid::Uuid::parse_str(component).ok()?;
        if id.to_string() != *component {
            return None;
        }
        let bytes = id.into_bytes();
        if bytes[..8] != creates.high.to_be_bytes() || bytes[8..12] != creates.low.to_be_bytes() {
            return None;
        }
        Some(u64::from(u32::from_be_bytes(bytes[12..].try_into().ok()?)))
    }

    fn insert(
        &mut self,
        identity: CreatedPacketIdentity,
        creates: WasmCreateContext,
        existing: &Self,
    ) -> Result<(), LixError> {
        let (schema_key, explicit_key, create_ref) = match identity {
            CreatedPacketIdentity::Explicit(key) => {
                let components = key
                    .entity_pk
                    .into_iter()
                    .map(|component| component.as_str().to_owned())
                    .collect::<Vec<_>>();
                let create_ref = Self::generated_local_ref(&components, creates);
                (
                    key.schema_key.as_str().to_owned(),
                    Some(components),
                    create_ref,
                )
            }
            CreatedPacketIdentity::Create {
                schema_key,
                local_ref,
            } => (schema_key, None, Some(local_ref)),
        };
        let existing = existing.schemas.get(&schema_key);
        let page = self.schemas.entry(schema_key).or_default();
        match explicit_key {
            Some(key) => {
                if existing.is_some_and(|keys| keys.explicit_keys.contains(&key))
                    || !page.explicit_keys.insert(key)
                    || create_ref.is_some_and(|local_ref| {
                        existing.is_some_and(|keys| Self::contains_create_ref(keys, local_ref))
                            || Self::contains_create_ref(page, local_ref)
                    })
                {
                    return Err(duplicate_certified_packet_key());
                }
                if let Some(local_ref) = create_ref {
                    page.explicit_create_refs.insert(local_ref);
                }
            }
            None => {
                let local_ref = create_ref.expect("create identity has a local reference");
                if existing.is_some_and(|keys| {
                    Self::contains_create_ref(keys, local_ref)
                        || keys.explicit_create_refs.contains(&local_ref)
                }) || page.explicit_create_refs.contains(&local_ref)
                    || page
                        .create_ref_ranges
                        .iter()
                        .any(|(first, last)| *first <= local_ref && local_ref <= *last)
                    || !page.create_refs.insert(local_ref)
                {
                    return Err(duplicate_certified_packet_key());
                }
            }
        }
        Ok(())
    }

    fn insert_create_ref_range(
        &mut self,
        schema_key: &str,
        first: u64,
        last: u64,
        existing: &Self,
    ) -> Result<(), LixError> {
        let collides = |keys: &CertifiedPacketSchemaKeys| {
            keys.create_refs.range(first..=last).next().is_some()
                || keys
                    .explicit_create_refs
                    .range(first..=last)
                    .next()
                    .is_some()
                || keys
                    .create_ref_ranges
                    .iter()
                    .any(|(range_first, range_last)| *range_first <= last && first <= *range_last)
        };
        if existing.schemas.get(schema_key).is_some_and(collides) {
            return Err(duplicate_certified_packet_key());
        }
        let page = self.schemas.entry(schema_key.to_owned()).or_default();
        if collides(page) {
            return Err(duplicate_certified_packet_key());
        }
        page.create_ref_ranges.push((first, last));
        Ok(())
    }

    fn extend(&mut self, page: Self) {
        for (schema_key, page) in page.schemas {
            let keys = self.schemas.entry(schema_key).or_default();
            keys.create_refs.extend(page.create_refs);
            keys.explicit_keys.extend(page.explicit_keys);
            keys.explicit_create_refs.extend(page.explicit_create_refs);
            keys.create_ref_ranges.extend(page.create_ref_ranges);
        }
    }
}

fn duplicate_certified_packet_key() -> LixError {
    v3_error("a component entity key may occur only once across certified packet pages")
}

fn validate_new_certified_packet_keys(
    page: ValidatedCreatedPacketPage,
    creates: WasmCreateContext,
    existing: &CertifiedPacketEntityKeys,
) -> Result<
    (
        std::collections::BTreeSet<String>,
        CertifiedPacketEntityKeys,
    ),
    LixError,
> {
    let mut page_keys = CertifiedPacketEntityKeys::default();
    for identity in page.identities {
        page_keys.insert(identity, creates, existing)?;
    }
    Ok((page.schemas, page_keys))
}

fn validate_ordinary_packet_page_keys(
    page: &WasmChangePage,
    creates: WasmCreateContext,
    existing: &CertifiedPacketEntityKeys,
) -> Result<CertifiedPacketEntityKeys, LixError> {
    let mut page_keys = CertifiedPacketEntityKeys::default();
    for change in &page.changes.changes {
        let identity = match change {
            WasmEntityChange::Create {
                schema_key,
                local_ref,
                ..
            } => CreatedPacketIdentity::Create {
                schema_key: schema_key.clone(),
                local_ref: *local_ref,
            },
            WasmEntityChange::Upsert { entity, .. } => {
                CreatedPacketIdentity::Explicit(entity.key.clone())
            }
            WasmEntityChange::Delete(key) => CreatedPacketIdentity::Explicit(key.clone()),
        };
        page_keys.insert(identity, creates, existing)?;
    }
    Ok(page_keys)
}

/// Returns packet metadata when every framed record is an inline snapshot write.
/// Delete, attachment, and non-certified-schema pages fall back to the generic
/// packet decoder.
fn validate_created_packet_page(
    record_count: u32,
    payload: &[u8],
) -> Result<Option<ValidatedCreatedPacketPage>, String> {
    if record_count == 0 {
        return Err("packet page is empty".to_owned());
    }
    let mut input = TypedCsvReader { payload, offset: 0 };
    let mut schemas = std::collections::BTreeSet::new();
    let mut identities = Vec::with_capacity(record_count as usize);
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
                let mut components = Vec::with_capacity(component_count as usize);
                for _ in 0..component_count {
                    let component_len = record.u32()? as usize;
                    components.push(
                        std::str::from_utf8(record.bytes(component_len)?)
                            .map_err(|error| format!("packet key component is not UTF-8: {error}"))?
                            .to_owned(),
                    );
                }
                if record.u8()? > 1 {
                    return Err("packet upsert has an invalid effect".to_owned());
                }
                identities.push(CreatedPacketIdentity::Explicit(
                    WasmEntityKey::from_owned_parts(schema, components),
                ));
            }
            2 => {
                let local_ref = record.u64()?;
                if previous_local_ref.is_some_and(|previous| previous >= local_ref) {
                    return Err("packet create local refs must be strictly increasing".to_owned());
                }
                previous_local_ref = Some(local_ref);
                identities.push(CreatedPacketIdentity::Create {
                    schema_key: schema.to_owned(),
                    local_ref,
                });
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
    // Dense Git-text rows have a valid generic schema but no storage-native
    // streaming validator yet. Keep them on the ordinary bounded packet path
    // instead of falsely certifying a segment the engine cannot consume.
    if schemas.contains("git_text_line_v2") {
        return Ok(None);
    }
    Ok(Some(ValidatedCreatedPacketPage {
        schemas,
        identities,
    }))
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
    source: &Arc<dyn lix_engine::wasm::WasmByteSource>,
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
) -> Result<Arc<dyn WasmComponentFactory>, LixError> {
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
            Component::new(engine, &bytes)
                .map_err(|error| wasm_runtime_error("failed to compile plugin component", error))
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
impl WasmComponentFactory for V3Factory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentActor>, LixError> {
        let engine = self.runtime.engine(self.profile);
        let timeout_ticker = self
            .runtime
            .timeout_ticker(self.profile)?
            .ok_or_else(|| v3_error("v3 actor requires an epoch timeout ticker"))?;
        let mut store = create_store(engine, self.limits)?;
        store.epoch_deadline_trap();
        let bindings = bindings::Plugin::instantiate(&mut store, &self.component, &self.linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate plugin actor", error))?;
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
            resolution_cursors: HashMap::new(),
            edit_cursors: HashMap::new(),
            outputs: HashMap::new(),
            transitions: HashMap::new(),
            prospective_documents: ProspectiveDocuments::default(),
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
    HydrateFile {
        document: u64,
        limits: WasmTransitionLimits,
        input: WasmOpenEntitiesInput,
        response: tokio::sync::oneshot::Sender<Result<HydrateWorkerOutput, LixError>>,
    },
    EntitiesChanged {
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        update: WasmEntityUpdate,
        response: tokio::sync::oneshot::Sender<Result<EntityWorkerOutput, LixError>>,
    },
    ResolveConflicts {
        limits: WasmTransitionLimits,
        update: WasmConflictUpdate,
        response: tokio::sync::oneshot::Sender<Result<ResolutionWorkerOutput, LixError>>,
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
    documents: HashMap<u64, V3Document>,
    next_document: u64,
    first_transition: bool,
}

#[derive(Clone)]
struct V3Document {
    root: ArenaRoot,
}

struct ResolutionWorkerOutput {
    resolutions: Vec<V3Resolution>,
    counters: WasmTransitionCounters,
}

struct EntityWorkerOutput {
    replacement: Bytes,
    counters: WasmTransitionCounters,
}

struct HydrateWorkerOutput {
    replacement: Option<Bytes>,
    accepted_len: u64,
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
                WorkerCommand::HydrateFile {
                    document,
                    limits,
                    input,
                    response,
                } => {
                    let _ = response.send(self.hydrate_file(document, limits, input));
                }
                WorkerCommand::EntitiesChanged {
                    document,
                    next_document,
                    limits,
                    update,
                    response,
                } => {
                    let _ = response.send(self.entities_changed(
                        document,
                        next_document,
                        limits,
                        update,
                    ));
                }
                WorkerCommand::ResolveConflicts {
                    limits,
                    update,
                    response,
                } => {
                    let _ = response.send(self.resolve_conflicts(limits, update));
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
            Some(events),
            std::mem::take(&mut self.first_transition),
            false,
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
            .push(SnapshotResource {
                root: root.clone(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 snapshot: {error}")))?;
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction: root.transaction(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 transition: {error}")))?;
        let transition_rep = transition.rep();
        let binding_input = bindings::exports::lix::plugin::api::TransitionRequest::Open(
            bindings::exports::lix::plugin::api::OpenRequest {
                descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                    path: input.descriptor.path,
                    media_type: input.descriptor.media_type,
                },
                accepted,
                creates: bindings::exports::lix::plugin::api::CreateContext {
                    high: input.creates.high,
                    low: input.creates.low,
                },
            },
        );
        let result = self.guest.call_apply(
            &mut self.store,
            &binding_input,
            Resource::new_borrow(transition_rep),
        );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover v3 transaction",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(v3_error(format!("v3 open-file rejected input: {error:?}")));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("v3 open-file trapped", error));
            }
        }
        let TransitionResource {
            transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let root = transaction
            .commit()
            .map_err(|error| v3_error(format!("failed to commit v3 arena root: {error}")))?;
        self.documents.insert(document, V3Document { root });
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 transition resources remained live after open-file"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let root = tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.v3_arena_prepare"
        )
        .in_scope(|| {
            self.documents
                .get(&document)
                .map(|document| document.root.clone())
                .ok_or_else(|| v3_error("unknown v3 document handle"))
        })?;
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            update.creates,
            Some(events),
            false,
            false,
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
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
            .map_err(|error| v3_error(format!("failed to register v3 transition: {error}")))?;
        let transition_rep = transition.rep();
        let binding_update = bindings::exports::lix::plugin::api::TransitionRequest::Update(
            bindings::exports::lix::plugin::api::UpdateRequest {
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
                creates: bindings::exports::lix::plugin::api::CreateContext {
                    high: update.creates.high,
                    low: update.creates.low,
                },
            },
        );
        let result = tracing::debug_span!(target: "lix_perf", "lix.perf.v3_guest_file_changed")
            .in_scope(|| {
                self.guest.call_apply(
                    &mut self.store,
                    &binding_update,
                    Resource::new_borrow(transition_rep),
                )
            });
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover v3 transaction",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(v3_error(format!(
                    "v3 file-changed rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("v3 file-changed trapped", error));
            }
        }
        let TransitionResource {
            transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let root = tracing::debug_span!(target: "lix_perf", "lix.perf.v3_arena_commit").in_scope(
            || {
                transaction
                    .commit()
                    .map_err(|error| v3_error(format!("failed to commit v3 arena root: {error}")))
            },
        )?;
        self.documents.insert(next_document, V3Document { root });
        self.next_document = self.next_document.max(next_document.saturating_add(1));
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 transition resources remained live after file-changed"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(state.counters)
    }

    fn resolve_conflicts(
        &mut self,
        limits: WasmTransitionLimits,
        mut update: WasmConflictUpdate,
    ) -> Result<ResolutionWorkerOutput, LixError> {
        let limits = v3_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let mut conflicts = Vec::new();
        while let Some(page) = update.conflicts.next_page(limits.max_page_bytes)? {
            if page.conflicts.is_empty() {
                return Err(v3_error("v3 conflict source returned an empty page"));
            }
            for conflict in page.conflicts {
                let expected = u32::try_from(conflicts.len())
                    .map_err(|_| v3_error("v3 conflict count exceeds u32"))?;
                if conflict.ordinal != expected {
                    return Err(v3_error(format!(
                        "v3 conflict source ordinal {}, expected {expected}",
                        conflict.ordinal
                    )));
                }
                conflicts.push(conflict);
            }
        }
        let expected_count = conflicts.len();
        let state = Arc::new(Mutex::new(ResolutionState::new(
            limits,
            conflicts,
            std::mem::take(&mut self.first_transition),
        )?));
        let source = self
            .store
            .data_mut()
            .table
            .push(ConflictSourceResource {
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 conflict source: {error}")))?;
        let sink = self
            .store
            .data_mut()
            .table
            .push(ResolutionSinkResource {
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 resolution sink: {error}")))?;
        let sink_rep = sink.rep();
        let binding_input = bindings::exports::lix::plugin::api::ConflictUpdate {
            descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: update.descriptor.path,
                media_type: update.descriptor.media_type,
            },
            conflicts: source,
        };
        let result = self.guest.call_resolve_conflicts(
            &mut self.store,
            &binding_input,
            Resource::new_borrow(sink_rep),
        );
        let _ = self.store.data_mut().table.delete(sink);
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Err(v3_error(format!(
                    "v3 resolve-conflicts rejected input: {error:?}"
                )));
            }
            Err(error) => {
                return Err(wasm_runtime_error("v3 resolve-conflicts trapped", error));
            }
        }
        let mut state = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 conflict resources remained live after resolution"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_some() {
            return Err(v3_error("v3 conflict replacement remained incomplete"));
        }
        if state.resolutions.len() != expected_count {
            return Err(v3_error(format!(
                "v3 conflict resolver returned {} results for {expected_count} conflicts",
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

    fn entities_changed(
        &mut self,
        document: u64,
        next_document: u64,
        limits: WasmTransitionLimits,
        mut update: WasmEntityUpdate,
    ) -> Result<EntityWorkerOutput, LixError> {
        let limits = v3_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let root = self
            .documents
            .get(&document)
            .map(|document| document.root.clone())
            .ok_or_else(|| v3_error("unknown v3 document handle"))?;
        let mut changes = Vec::new();
        while let Some(page) = update.changes.next_page(limits.max_page_bytes)? {
            if page.changes.is_empty() {
                return Err(v3_error("v3 entity-change source returned an empty page"));
            }
            changes.extend(page.changes);
        }
        let entity_state = Arc::new(Mutex::new(EntityChangeState::new(limits, changes)?));
        let transition_state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            WasmCreateContext { high: 0, low: 0 },
            None,
            false,
            true,
        )?));
        let before = self
            .store
            .data_mut()
            .table
            .push(SnapshotResource {
                root: root.clone(),
                state: transition_state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 entity snapshot: {error}")))?;
        let source = self
            .store
            .data_mut()
            .table
            .push(EntityChangeSourceResource {
                state: entity_state.clone(),
            })
            .map_err(|error| {
                v3_error(format!(
                    "failed to register v3 entity-change source: {error}"
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
                v3_error(format!("failed to register v3 entity transition: {error}"))
            })?;
        let transition_rep = transition.rep();
        let binding_input = bindings::exports::lix::plugin::api::EntityUpdate {
            before_descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: update.before_descriptor.path,
                media_type: update.before_descriptor.media_type,
            },
            after_descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: update.after_descriptor.path,
                media_type: update.after_descriptor.media_type,
            },
            before,
            changes: source,
        };
        let result = self.guest.call_entities_changed(
            &mut self.store,
            &binding_input,
            Resource::new_borrow(transition_rep),
        );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover v3 entity transition",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(v3_error(format!(
                    "v3 entities-changed rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("v3 entities-changed trapped", error));
            }
        }
        let TransitionResource {
            mut transaction,
            state: transaction_state,
        } = transition;
        drop(transaction_state);
        let mut state = Arc::try_unwrap(transition_state)
            .map_err(|_| v3_error("v3 entity transition resources remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let replacement = state
            .file_replacement
            .take()
            .ok_or_else(|| v3_error("v3 entities-changed did not emit a file replacement"))?;
        if !replacement.complete {
            return Err(v3_error(
                "v3 entities-changed file replacement is incomplete",
            ));
        }
        transaction.edit_bytes(ArenaByteEdit {
            offset: 0,
            delete_len: root.bytes.len(),
            insert: replacement.bytes.clone(),
        });
        let successor = transaction
            .commit()
            .map_err(|error| v3_error(format!("failed to commit v3 entity root: {error}")))?;
        self.documents
            .insert(next_document, V3Document { root: successor });
        self.next_document = self.next_document.max(next_document.saturating_add(1));
        let entity_state = Arc::try_unwrap(entity_state)
            .map_err(|_| v3_error("v3 entity-change source remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.counters.component_import_calls = state
            .counters
            .component_import_calls
            .saturating_add(entity_state.counters.component_import_calls);
        state.counters.component_boundary_bytes = state
            .counters
            .component_boundary_bytes
            .saturating_add(entity_state.counters.component_boundary_bytes);
        state.counters.source_read_calls = state
            .counters
            .source_read_calls
            .saturating_add(entity_state.counters.source_read_calls);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(entity_state.counters.source_bytes_read);
        state.counters.guest_linear_memory_high_water_bytes =
            self.store.data().limits.linear_memory_high_water_bytes();
        Ok(EntityWorkerOutput {
            replacement: Bytes::from(replacement.bytes),
            counters: state.counters,
        })
    }

    fn hydrate_file(
        &mut self,
        document: u64,
        limits: WasmTransitionLimits,
        mut input: WasmOpenEntitiesInput,
    ) -> Result<HydrateWorkerOutput, LixError> {
        let limits = v3_transition_limits(limits)?;
        reset_store_limits(&mut self.store, self.limits)?;
        let ticks = limits.total_deadline_nanoseconds.saturating_add(999_999) / 1_000_000;
        self.store.set_epoch_deadline(ticks.max(1));
        let accepted_len = input
            .accepted
            .as_ref()
            .map(|accepted| accepted.len())
            .unwrap_or(0);
        let bytes = input
            .accepted
            .as_ref()
            .map(|accepted| read_source_all(accepted))
            .transpose()?
            .unwrap_or_default();
        let mut entities = Vec::new();
        while let Some(page) = input.entities.next_page(limits.max_page_bytes)? {
            for entity in page.entities {
                entities.push(WasmEntityChange::Upsert {
                    entity,
                    effect: WasmChangeEffect::Content,
                });
            }
        }
        let entity_state = Arc::new(Mutex::new(EntityChangeState::new(limits, entities)?));
        let root = ArenaRoot::import(
            ArenaStore::default(),
            "v3-cold-successor",
            &bytes,
            std::iter::empty(),
            std::iter::empty(),
        );
        let state = Arc::new(Mutex::new(TransitionState::new(
            limits,
            WasmCreateContext { high: 0, low: 0 },
            None,
            std::mem::take(&mut self.first_transition),
            true,
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
            .map_err(|error| v3_error(format!("failed to register v3 cold snapshot: {error}")))?;
        let source = self
            .store
            .data_mut()
            .table
            .push(EntityChangeSourceResource {
                state: entity_state.clone(),
            })
            .map_err(|error| {
                v3_error(format!("failed to register v3 hydration source: {error}"))
            })?;
        let transition = self
            .store
            .data_mut()
            .table
            .push(TransitionResource {
                transaction: root.transaction(),
                state: state.clone(),
            })
            .map_err(|error| v3_error(format!("failed to register v3 cold transition: {error}")))?;
        let transition_rep = transition.rep();
        let request = bindings::exports::lix::plugin::api::HydrateRequest {
            descriptor: bindings::exports::lix::plugin::api::FileDescriptor {
                path: input.descriptor.path,
                media_type: input.descriptor.media_type,
            },
            accepted,
            entities: source,
        };
        let result = self.guest.call_hydrate(
            &mut self.store,
            &request,
            Resource::new_borrow(transition_rep),
        );
        let transition = take_borrowed_resource(
            &mut self.store.data_mut().table,
            transition,
            "failed to recover v3 cold transition",
        )?;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(transition);
                return Err(v3_error(format!(
                    "v3 cold hydration rejected input: {error:?}"
                )));
            }
            Err(error) => {
                drop(transition);
                return Err(wasm_runtime_error("v3 cold hydration trapped", error));
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
                return Err(v3_error("v3 hydration file replacement is incomplete"));
            }
            transaction.edit_bytes(ArenaByteEdit {
                offset: 0,
                delete_len: root.bytes.len(),
                insert: replacement.bytes,
            });
        } else if input.accepted.is_none() {
            return Err(v3_error(
                "v3 derived cold hydration did not emit a file replacement",
            ));
        }
        drop(transition_state);
        let root = transaction
            .commit()
            .map_err(|error| v3_error(format!("failed to commit v3 cold root: {error}")))?;
        self.documents.insert(document, V3Document { root });
        self.next_document = self.next_document.max(document.saturating_add(1));
        let mut counters = Arc::try_unwrap(state)
            .map_err(|_| v3_error("v3 cold hydration resources remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .counters;
        let entity_state = Arc::try_unwrap(entity_state)
            .map_err(|_| v3_error("v3 hydration source remained live"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counters.component_import_calls = counters
            .component_import_calls
            .saturating_add(entity_state.counters.component_import_calls);
        counters.component_boundary_bytes = counters
            .component_boundary_bytes
            .saturating_add(entity_state.counters.component_boundary_bytes);
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
            .ok_or_else(|| v3_error("unknown v3 document handle"))?;
        let handle = self.next_document;
        self.next_document = self
            .next_document
            .checked_add(1)
            .ok_or_else(|| v3_error("v3 document handle overflowed"))?;
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
    certified_packet_entity_keys: CertifiedPacketEntityKeys,
}

struct ResolutionCursorState {
    transition: WasmTransitionHandle,
    pages: VecDeque<WasmConflictResolutionPage>,
}

struct OutputState {
    transition: WasmTransitionHandle,
    values: Vec<Bytes>,
}

struct V3EditCursorState {
    transition: WasmTransitionHandle,
    page: Option<WasmEditPage>,
}

struct V3Actor {
    sender: mpsc::Sender<WorkerCommand>,
    thread: Option<std::thread::JoinHandle<()>>,
    _timeout_ticker: TimeoutTickerLease,
    next_handle: u64,
    cursors: HashMap<u64, CursorState>,
    resolution_cursors: HashMap<u64, ResolutionCursorState>,
    edit_cursors: HashMap<u64, V3EditCursorState>,
    outputs: HashMap<u64, OutputState>,
    transitions: HashMap<u64, WasmTransitionCounters>,
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
        let result = receiver
            .await
            .map_err(|_| v3_error("v3 actor executor dropped its response"))?;
        if let Err(error) = &result {
            self.retire_after_trap(error);
        }
        result
    }

    fn retire_after_trap(&mut self, error: &LixError) {
        if is_guest_trap(error) {
            self.retired = true;
            let _ = self.sender.send(WorkerCommand::Shutdown);
        }
    }
}

#[async_trait]
impl WasmComponentActor for V3Actor {
    fn cold_open_hydrates_without_render(&self) -> bool {
        true
    }

    fn cold_open_requires_entities(&self) -> bool {
        true
    }

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
                certified_packet_entity_keys: CertifiedPacketEntityKeys::default(),
            },
        );
        self.prospective_documents.track(transition, document);
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(document),
            changes: cursor,
        })
    }

    async fn open_entities(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenEntitiesInput,
    ) -> Result<WasmEntityTransition, LixError> {
        let document = self.allocate_document()?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let edits = WasmEditCursorHandle(self.allocate_handle()?);
        let resolved = self
            .request(|response| WorkerCommand::HydrateFile {
                document,
                limits,
                input,
                response,
            })
            .await?;
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
            .insert(edits.0, V3EditCursorState { transition, page });
        Ok(WasmEntityTransition {
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
                certified_packet_entity_keys: CertifiedPacketEntityKeys::default(),
            },
        );
        self.prospective_documents.track(transition, next_document);
        Ok(WasmFileTransition {
            transition,
            document: WasmDocumentHandle(next_document),
            changes: cursor,
        })
    }

    async fn entities_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmEntityUpdate,
    ) -> Result<WasmEntityTransition, LixError> {
        let next_document = self.allocate_document()?;
        let before_len = update.before.len();
        let resolved = self
            .request(|response| WorkerCommand::EntitiesChanged {
                document: document.0,
                next_document,
                limits,
                update,
                response,
            })
            .await?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let edits = WasmEditCursorHandle(self.allocate_handle()?);
        let outputs = WasmByteOutputsHandle(self.allocate_handle()?);
        let length = resolved.replacement.len() as u64;
        self.outputs.insert(
            outputs.0,
            OutputState {
                transition,
                values: vec![resolved.replacement],
            },
        );
        self.transitions.insert(transition.0, resolved.counters);
        self.prospective_documents.track(transition, next_document);
        self.edit_cursors.insert(
            edits.0,
            V3EditCursorState {
                transition,
                page: Some(WasmEditPage {
                    edits: vec![WasmOutputSplice {
                        offset: 0,
                        delete_len: before_len,
                        insert: WasmGuestBytes::Output(WasmOutputRange {
                            index: 0,
                            offset: 0,
                            length,
                        }),
                    }],
                    outputs: Some(outputs),
                }),
            },
        );
        Ok(WasmEntityTransition {
            transition,
            document: WasmDocumentHandle(next_document),
            edits,
        })
    }

    async fn resolve_conflicts(
        &mut self,
        limits: WasmTransitionLimits,
        update: WasmConflictUpdate,
    ) -> Result<WasmConflictTransition, LixError> {
        let resolved = self
            .request(|response| WorkerCommand::ResolveConflicts {
                limits,
                update,
                response,
            })
            .await?;
        let transition = WasmTransitionHandle(self.allocate_handle()?);
        let cursor = WasmResolutionCursorHandle(self.allocate_handle()?);
        let records_per_page = (limits.max_page_bytes as usize / 64).max(1);
        let mut pages = VecDeque::new();
        let mut ordinal = 0_u32;
        let mut resolutions = resolved.resolutions.into_iter();
        loop {
            let mut page_ordinals = Vec::with_capacity(records_per_page);
            let mut page_resolutions = Vec::with_capacity(records_per_page);
            let mut page_outputs = Vec::new();
            for _ in 0..records_per_page {
                let Some(resolution) = resolutions.next() else {
                    break;
                };
                page_ordinals.push(ordinal);
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| v3_error("v3 resolution ordinal overflowed"))?;
                page_resolutions.push(match resolution {
                    V3Resolution::Take(side) => WasmConflictResolution::Take(side),
                    V3Resolution::Delete => WasmConflictResolution::Delete,
                    V3Resolution::Replace { snapshot, effect } => {
                        let index = u32::try_from(page_outputs.len())
                            .map_err(|_| v3_error("v3 replacement output count exceeds u32"))?;
                        let length = snapshot.len() as u64;
                        page_outputs.push(snapshot);
                        WasmConflictResolution::Replace {
                            snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                                index,
                                offset: 0,
                                length,
                            }),
                            effect,
                        }
                    }
                });
            }
            if page_resolutions.is_empty() {
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
            pages.push_back(WasmConflictResolutionPage {
                format_version: PACKET_FORMAT_V1,
                ordinals: page_ordinals,
                resolutions: page_resolutions,
                outputs,
            });
        }
        self.transitions.insert(transition.0, resolved.counters);
        self.resolution_cursors
            .insert(cursor.0, ResolutionCursorState { transition, pages });
        Ok(WasmConflictTransition {
            transition,
            resolutions: cursor,
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
                    let mut page_keys = CertifiedPacketEntityKeys::default();
                    page_keys.insert_create_ref_range(
                        "csv_v2_row",
                        u64::from(first_local_ref),
                        u64::from(last_local_ref),
                        &cursor.certified_packet_entity_keys,
                    )?;
                    cursor.certified_csv_creates = Some(creates);
                    cursor.certified_csv_rows = cursor
                        .certified_csv_rows
                        .checked_add(u64::from(row_count))
                        .ok_or_else(|| v3_error("certified CSV row count overflowed"))?;
                    cursor.certified_csv_last_local_ref = Some(last_local_ref);
                    cursor.certified_csv_pages.push(Bytes::from(payload));
                    cursor.certified_packet_entity_keys.extend(page_keys);
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
                    if let Some(validated_page) =
                        validate_created_packet_page(record_count, &payload).map_err(v3_error)?
                    {
                        // Certified immutable segments are the ownership unit
                        // for complete bulk state. Sparse successors stay as
                        // ordinary row overlays: this lets validation observe
                        // their durable base directly and avoids paying a new
                        // segment/manifest lifecycle for one or two rows.
                        if !cursor.complete_file_state {
                            let decoded = PendingChangePage::Packet {
                                record_count,
                                payload,
                                max_page_bytes,
                                limits,
                                creates,
                            }
                            .decode()?;
                            let page_keys = validate_ordinary_packet_page_keys(
                                &decoded,
                                creates,
                                &cursor.certified_packet_entity_keys,
                            )?;
                            cursor.certified_packet_entity_keys.extend(page_keys);
                            return Ok(Some(decoded));
                        }
                        if cursor
                            .certified_packet_creates
                            .is_some_and(|existing| existing != creates)
                        {
                            return Err(v3_error(
                                "one certified packet transition used multiple create contexts",
                            ));
                        }
                        let (schema_keys, page_keys) = validate_new_certified_packet_keys(
                            validated_page,
                            creates,
                            &cursor.certified_packet_entity_keys,
                        )?;
                        cursor.certified_packet_creates = Some(creates);
                        cursor.certified_packet_rows = cursor
                            .certified_packet_rows
                            .checked_add(u64::from(record_count))
                            .ok_or_else(|| v3_error("certified packet row count overflowed"))?;
                        cursor.certified_packet_schema_keys.extend(schema_keys);
                        cursor.certified_packet_entity_keys.extend(page_keys);
                        cursor.certified_packet_pages.push(Bytes::from(payload));
                    } else {
                        let decoded = PendingChangePage::Packet {
                            record_count,
                            payload,
                            max_page_bytes,
                            limits,
                            creates,
                        }
                        .decode()?;
                        let page_keys = validate_ordinary_packet_page_keys(
                            &decoded,
                            creates,
                            &cursor.certified_packet_entity_keys,
                        )?;
                        cursor.certified_packet_entity_keys.extend(page_keys);
                        return Ok(Some(decoded));
                    }
                }
                Some(WorkerTransitionEvent::Finished(result)) => {
                    let counters = match result {
                        Ok(counters) => counters,
                        Err(error) => {
                            self.retire_after_trap(&error);
                            return Err(error);
                        }
                    };
                    self.transitions.insert(transition.0, counters);
                    return Ok(None);
                }
                None => return Err(v3_error("v3 transition producer stopped")),
            }
        }
    }

    async fn next_resolution_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmResolutionCursorHandle,
        _max_bytes: u32,
    ) -> Result<Option<WasmConflictResolutionPage>, LixError> {
        let cursor = self
            .resolution_cursors
            .get_mut(&cursor.0)
            .ok_or_else(|| v3_error("unknown v3 resolution cursor"))?;
        if cursor.transition != transition {
            return Err(v3_error(
                "v3 resolution cursor belongs to another transition",
            ));
        }
        Ok(cursor.pages.pop_front())
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
            cursor.certified_packet_entity_keys = CertifiedPacketEntityKeys::default();
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
        transition: WasmTransitionHandle,
        cursor: WasmEditCursorHandle,
        _max_edits: u32,
        _max_inline_bytes: u32,
    ) -> Result<Option<WasmEditPage>, LixError> {
        match self.edit_cursors.get_mut(&cursor.0) {
            Some(state) if state.transition == transition => Ok(state.page.take()),
            Some(_) => Err(v3_error(
                "v3 hydration edit cursor belongs to another transition",
            )),
            None => Err(v3_error("unknown v3 hydration edit cursor")),
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
            .ok_or_else(|| v3_error("unknown v3 byte outputs"))?;
        if outputs.transition != transition {
            return Err(v3_error("v3 byte outputs belong to another transition"));
        }
        outputs
            .values
            .get(index as usize)
            .map(|bytes| bytes.len() as u64)
            .ok_or_else(|| v3_error("v3 byte output index is out of bounds"))
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
            .ok_or_else(|| v3_error("unknown v3 byte outputs"))?;
        if outputs.transition != transition {
            return Err(v3_error("v3 byte outputs belong to another transition"));
        }
        let bytes = outputs
            .values
            .get(index as usize)
            .ok_or_else(|| v3_error("v3 byte output index is out of bounds"))?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or_else(|| v3_error("v3 byte output range overflowed"))?;
        if end > bytes.len() as u64 {
            return Err(v3_error("v3 byte output range is out of bounds"));
        }
        Ok(bytes.slice(offset as usize..end as usize).to_vec())
    }

    async fn finish_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<WasmTransitionCounters, LixError> {
        self.cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.resolution_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.edit_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.outputs
            .retain(|_, outputs| outputs.transition != transition);
        self.prospective_documents.accept(transition);
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
        self.resolution_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.edit_cursors
            .retain(|_, cursor| cursor.transition != transition);
        self.outputs
            .retain(|_, outputs| outputs.transition != transition);
        self.transitions.remove(&transition.0);
        if let Some(document) = self.prospective_documents.reject(transition)
            && !self.retired
        {
            self.request(|response| WorkerCommand::DropDocument { document, response })
                .await?;
        }
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
        }
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| v3_error("v3 actor executor panicked"))?;
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
    // Batches are the record envelope. Keeping the inherited 1 MiB
    // record cap while advertising a 2 MiB push page rejects otherwise valid
    // single-snapshot pages (notably Markdown lexical fallbacks).
    limits.max_record_bytes = limits.max_page_bytes;
    limits.validate()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_text(output: &mut Vec<u8>, value: &str) {
        output.extend_from_slice(&(value.len() as u32).to_le_bytes());
        output.extend_from_slice(value.as_bytes());
    }

    fn packet_page(tag: u8, schema: &str, identity: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut record = vec![tag];
        push_text(&mut record, schema);
        identity(&mut record);
        record.push(0);
        record.extend_from_slice(&2_u32.to_le_bytes());
        record.extend_from_slice(b"{}");

        let mut page = Vec::with_capacity(record.len() + 4);
        page.extend_from_slice(&(record.len() as u32).to_le_bytes());
        page.extend_from_slice(&record);
        page
    }

    fn create_page(schema: &str, local_ref: u64) -> Vec<u8> {
        packet_page(2, schema, |record| {
            record.extend_from_slice(&local_ref.to_le_bytes());
        })
    }

    fn upsert_page(schema: &str, component: &str) -> Vec<u8> {
        packet_page(0, schema, |record| {
            record.extend_from_slice(&1_u32.to_le_bytes());
            push_text(record, component);
            record.push(0);
        })
    }

    fn accept_page(
        payload: &[u8],
        creates: WasmCreateContext,
        existing: &mut CertifiedPacketEntityKeys,
    ) -> Result<(), LixError> {
        let page = validate_created_packet_page(1, payload)
            .expect("well-framed packet")
            .expect("certifiable packet");
        let (_, keys) = validate_new_certified_packet_keys(page, creates, existing)?;
        existing.extend(keys);
        Ok(())
    }

    #[test]
    fn certified_packet_rejects_duplicate_entity_keys_across_pages() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let mut existing = CertifiedPacketEntityKeys::default();

        accept_page(&create_page("row", 7), creates, &mut existing).expect("first page is unique");
        let duplicate_create = accept_page(&create_page("row", 7), creates, &mut existing)
            .expect_err("a later page must not repeat a create identity");
        assert_eq!(
            duplicate_create.message,
            "a component entity key may occur only once across certified packet pages"
        );

        let generated_id = creates.component(7).expect("create identity");
        let explicit_collision =
            accept_page(&upsert_page("row", &generated_id), creates, &mut existing)
                .expect_err("an explicit key must not collide with an earlier create");
        assert_eq!(
            explicit_collision.message,
            "a component entity key may occur only once across certified packet pages"
        );

        let mut explicit_keys = CertifiedPacketEntityKeys::default();
        accept_page(
            &upsert_page("row", "stable-key"),
            creates,
            &mut explicit_keys,
        )
        .expect("first explicit key is unique");
        let duplicate_explicit = accept_page(
            &upsert_page("row", "stable-key"),
            creates,
            &mut explicit_keys,
        )
        .expect_err("a later page must not repeat an explicit key");
        assert_eq!(
            duplicate_explicit.message,
            "a component entity key may occur only once across certified packet pages"
        );
    }

    #[test]
    fn certified_packet_rejects_keys_repeated_by_ordinary_pages() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let generated_id = creates.component(7).expect("create identity");
        let ordinary_payload = upsert_page("row", &generated_id);
        let ordinary = decode_inline_change_page(
            1,
            ordinary_payload.clone(),
            ordinary_payload.len() as u32,
            WasmTransitionLimits::default(),
        )
        .expect("ordinary packet decodes");

        let mut certified_first = CertifiedPacketEntityKeys::default();
        accept_page(&create_page("row", 7), creates, &mut certified_first)
            .expect("certified create is unique");
        let duplicate_ordinary =
            validate_ordinary_packet_page_keys(&ordinary, creates, &certified_first)
                .expect_err("ordinary write must not repeat a certified identity");
        assert_eq!(
            duplicate_ordinary.message,
            "a component entity key may occur only once across certified packet pages"
        );

        let mut ordinary_first = CertifiedPacketEntityKeys::default();
        let ordinary_keys = validate_ordinary_packet_page_keys(&ordinary, creates, &ordinary_first)
            .expect("ordinary write is initially unique");
        ordinary_first.extend(ordinary_keys);
        let duplicate_certified = accept_page(
            &upsert_page("row", &generated_id),
            creates,
            &mut ordinary_first,
        )
        .expect_err("certified write must not repeat an ordinary identity");
        assert_eq!(
            duplicate_certified.message,
            "a component entity key may occur only once across certified packet pages"
        );
    }

    #[test]
    fn typed_csv_ranges_share_packet_identity_validation() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let mut csv_first = CertifiedPacketEntityKeys::default();
        csv_first
            .insert_create_ref_range(
                "csv_v2_row",
                0,
                219_999,
                &CertifiedPacketEntityKeys::default(),
            )
            .expect("typed CSV range is initially unique");
        let duplicate_packet = accept_page(&create_page("csv_v2_row", 17), creates, &mut csv_first)
            .expect_err("packet create must not repeat a typed CSV identity");
        assert_eq!(
            duplicate_packet.message,
            "a component entity key may occur only once across certified packet pages"
        );

        let mut packet_first = CertifiedPacketEntityKeys::default();
        accept_page(&create_page("csv_v2_row", 17), creates, &mut packet_first)
            .expect("packet create is initially unique");
        let mut csv_range = CertifiedPacketEntityKeys::default();
        let duplicate_csv = csv_range
            .insert_create_ref_range("csv_v2_row", 0, 219_999, &packet_first)
            .expect_err("typed CSV range must not repeat a packet identity");
        assert_eq!(
            duplicate_csv.message,
            "a component entity key may occur only once across certified packet pages"
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
        let user_key = WasmEntity {
            key: WasmEntityKey::from_owned_parts(
                "json_object_member",
                vec!["root".to_owned(), generated_id.clone()],
            ),
            snapshot_content: WasmHostBytes::Inline(Bytes::from(format!(
                r#"{{"key":"{generated_id}"}}"#
            ))),
        };
        assert_eq!(
            create_context_from_generated_entity("plugin_json", &user_key),
            None
        );

        let generated_item = WasmEntity {
            key: WasmEntityKey::from_owned_parts("json_array_item", vec![generated_id]),
            snapshot_content: WasmHostBytes::Inline(Bytes::from_static(b"{}")),
        };
        assert_eq!(
            create_context_from_generated_entity("plugin_json", &generated_item),
            Some(creates)
        );
    }

    #[test]
    fn only_runtime_traps_trigger_actor_retirement() {
        assert!(is_guest_trap(&LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "v3 file-changed trapped: unreachable"
        )));
        assert!(!is_guest_trap(&LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "v3 file-changed rejected input"
        )));
    }
}
