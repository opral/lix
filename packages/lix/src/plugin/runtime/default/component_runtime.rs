//! Host-owned arena runtime for the row-first Component API.
//!
//! File bytes and opaque plugin state live in immutable host roots; one
//! exported guest call reads sparse ranges and pushes bounded semantic pages.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::plugin::wire::{Operation, Page as RowPage, Representation, encode_single_section};
use async_trait::async_trait;
use bytes::Bytes;
use lix::plugin::runtime::v1::{
    ByteEdit as ArenaByteEdit, Digest as ArenaDigest, Root as ArenaRoot, Store as ArenaStore,
    Transaction as ArenaTransaction,
};
use lix::plugin::runtime::{
    PACKET_FORMAT_V1, PluginCapabilities, WasmByteOutputsHandle, WasmCertifiedCreateRange,
    WasmCertifiedRowBatch, WasmChangeCursorHandle, WasmChangeEffect, WasmChangePage,
    WasmColdFileUpdate, WasmColumnMergeCursorHandle,
    WasmColumnMergeResult as RuntimeColumnMergeResult, WasmColumnMergeResultPage,
    WasmColumnMergeTransition, WasmColumnMergeUpdate, WasmComponentActor, WasmComponentFactory,
    WasmCreateContext, WasmDocumentCheckpoint, WasmDocumentHandle, WasmDurableDocumentCheckpoint,
    WasmEditCursorHandle, WasmEditPage, WasmFileTransition, WasmFileUpdate, WasmGuestBytes,
    WasmHostBytes, WasmHostColumnMerge, WasmInputBytes, WasmOpenFileInput, WasmOpenRowsInput,
    WasmOutputRange, WasmOutputSplice, WasmRow, WasmRowChange, WasmRowChanges, WasmRowKey,
    WasmRowTransition, WasmRowUpdate, WasmTransitionCounters, WasmTransitionHandle,
    WasmTransitionLimits,
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
const CERTIFIED_CREATED_PACKET_V1: u16 = 2;

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
    creates: WasmCreateContext,
    started: Instant,
    total_bytes: SharedByteBudget,
    pages: VecDeque<PendingChangePage>,
    replace_all_rows: bool,
    attachments: Vec<Bytes>,
    pending_attachment: Option<PendingRowAttachment>,
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

struct PendingRowAttachment {
    expected_len: u64,
    bytes: Vec<u8>,
}

struct RowChangeState {
    limits: WasmTransitionLimits,
    started: Instant,
    total_bytes: SharedByteBudget,
    source: RowChangeInputSource,
    next_ordinal: u32,
    lazy_snapshots: HashMap<u32, WasmHostBytes>,
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
    Packet {
        record_count: u32,
        payload: Vec<u8>,
        attachments: Arc<[Bytes]>,
        max_page_bytes: u32,
        limits: WasmTransitionLimits,
        creates: WasmCreateContext,
    },
    Decoded(WasmChangePage),
}

impl PendingChangePage {
    fn decode(self) -> Result<WasmChangePage, LixError> {
        match self {
            Self::Packet {
                record_count,
                payload,
                attachments,
                max_page_bytes,
                limits,
                ..
            } => decode_inline_change_page(
                record_count,
                payload,
                &attachments,
                max_page_bytes,
                limits,
            ),
            Self::Decoded(page) => Ok(page),
        }
    }
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
                format_version: PACKET_FORMAT_V1,
                changes: WasmRowChanges {
                    changes: prior_keys.into_iter().map(WasmRowChange::Delete).collect(),
                },
                outputs: None,
            }));
    }
    Ok(())
}

fn decode_inline_change_page(
    record_count: u32,
    payload: Vec<u8>,
    attachments: &[Bytes],
    max_bytes: u32,
    limits: WasmTransitionLimits,
) -> Result<WasmChangePage, LixError> {
    if record_count == 0 {
        return Err(component_error("guest returned a zero-record change page"));
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
        .map_err(|_| component_error("guest record count exceeds host bounds"))?;
    if record_count > payload.len() / size_of::<u32>() {
        return Err(component_error(
            "guest record count exceeds its bounded payload framing",
        ));
    }

    let payload = Bytes::from(payload);
    let mut framed = PacketReader::new(&payload);
    let row_size = size_of::<WasmRowChange<WasmGuestBytes>>();
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
                let key = decode_row_key(&mut record)?;
                let effect = match record.read_u8()? {
                    0 => WasmChangeEffect::Content,
                    1 => WasmChangeEffect::FormatOnly,
                    _ => return Err(component_error("unknown change effect tag")),
                };
                WasmRowChange::Upsert {
                    row: WasmRow {
                        key,
                        snapshot_content: decode_guest_blob(&mut record, attachments)?,
                    },
                    effect,
                }
            }
            1 => WasmRowChange::Delete(decode_row_key(&mut record)?),
            2 => WasmRowChange::Create {
                schema_key: record.read_text()?.to_string(),
                local_ref: record.read_u64()?,
                resolved_key: None,
                snapshot_content: decode_guest_blob(&mut record, attachments)?,
            },
            _ => return Err(component_error("unknown change tag")),
        };
        record.finish()?;
        changes.push(change);
    }
    framed.finish()?;
    Ok(WasmChangePage {
        format_version: PACKET_FORMAT_V1,
        changes: WasmRowChanges { changes },
        outputs: None,
    })
}

fn encode_packet_text(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| component_error("row packet text exceeds u32"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_row_key(reader: &mut PacketReader<'_>) -> Result<WasmRowKey, LixError> {
    let schema_key = reader.read_text()?;
    let pk_count = reader.read_u32()?;
    if pk_count as usize > reader.remaining() / size_of::<u32>() {
        return Err(component_error(
            "row primary-key component count exceeds packet bounds",
        ));
    }
    let mut key = WasmRowKey::from_shared_parts(schema_key, std::iter::empty());
    for _ in 0..pk_count {
        key.row_pk.push(reader.read_text()?);
    }
    Ok(key)
}

fn decode_guest_blob(
    reader: &mut PacketReader<'_>,
    attachments: &[Bytes],
) -> Result<WasmGuestBytes, LixError> {
    match reader.read_u8()? {
        0 => {
            let length = reader.read_u32()? as usize;
            Ok(WasmGuestBytes::Inline(reader.read_bytes(length)?))
        }
        1 => {
            let ordinal = reader.read_u32()?;
            let length = reader.read_u64()?;
            let bytes = attachments
                .get(ordinal as usize)
                .ok_or_else(|| component_error("row attachment ordinal is out of bounds"))?;
            if bytes.len() as u64 != length {
                return Err(component_error("row attachment length does not match"));
            }
            Ok(WasmGuestBytes::Inline(bytes.clone()))
        }
        _ => Err(component_error("unknown blob-reference tag")),
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
            .ok_or_else(|| component_error("packet range overflowed"))?;
        if end > self.end {
            return Err(component_error("truncated packet"));
        }
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| component_error("truncated packet"))?;
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
                .map_err(|_| component_error("invalid u32 field"))?,
        ))
    }

    fn read_u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?
                .try_into()
                .map_err(|_| component_error("invalid u64 field"))?,
        ))
    }

    fn read_text(&mut self) -> Result<SharedStr, LixError> {
        let length = self.read_u32()? as usize;
        let bytes = self.read_exact(length)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| component_error("packet text is not valid UTF-8"))?;
        SharedStr::from_utf8_slice(self.bytes.clone(), value)
            .ok_or_else(|| component_error("packet text is outside its page allocation"))
    }

    fn finish(&self) -> Result<(), LixError> {
        if self.offset != self.end {
            return Err(component_error("packet contains trailing bytes"));
        }
        Ok(())
    }
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
        creates: WasmCreateContext,
        allow_file_replacement: bool,
        total_bytes: Option<SharedByteBudget>,
    ) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            creates,
            started: Instant::now(),
            total_bytes: total_bytes.unwrap_or_default(),
            pages: VecDeque::new(),
            replace_all_rows: false,
            attachments: Vec::new(),
            pending_attachment: None,
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
        if self.pending_attachment.is_some() {
            return Err(component_error(
                "row attachment remained incomplete after plugin return",
            ));
        }
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
            next_ordinal: 0,
            lazy_snapshots: HashMap::new(),
            seen_row_keys: BTreeSet::new(),
            counters: WasmTransitionCounters::default(),
        })
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
            next_ordinal: 0,
            lazy_snapshots: HashMap::new(),
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
                self.seen_row_keys
                    .extend(page.rows.iter().map(|row| row.key.clone()));
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
}

fn conflict_value<'a>(
    conflict: &'a WasmHostColumnMerge,
    side: bindings::lix::plugin::host::MergeSide,
) -> Option<&'a WasmHostBytes> {
    match side {
        bindings::lix::plugin::host::MergeSide::Base => conflict.base.as_ref(),
        bindings::lix::plugin::host::MergeSide::A => conflict.a.as_ref(),
        bindings::lix::plugin::host::MergeSide::B => conflict.b.as_ref(),
    }
}

fn conflict_row(
    conflict: &WasmHostColumnMerge,
    side: bindings::lix::plugin::host::MergeSide,
) -> &WasmHostBytes {
    match side {
        bindings::lix::plugin::host::MergeSide::Base => &conflict.base_row,
        bindings::lix::plugin::host::MergeSide::A => &conflict.a_row,
        bindings::lix::plugin::host::MergeSide::B => &conflict.b_row,
    }
}

fn read_host_bytes(value: &WasmHostBytes, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| component_error("component conflict range overflowed"))?;
    if end > value.len() {
        return Err(component_error("component conflict range is out of bounds"));
    }
    match value {
        WasmHostBytes::Inline(bytes) => Ok(bytes.slice(offset as usize..end as usize).to_vec()),
        WasmHostBytes::CanonicalJson(json) => {
            Ok(json.normalized().as_bytes()[offset as usize..end as usize].to_vec())
        }
        WasmHostBytes::Source(slice) => slice
            .source
            .read(
                slice.range.offset.checked_add(offset).ok_or_else(|| {
                    component_error("component conflict source offset overflowed")
                })?,
                length,
            )
            .map_err(|error| {
                component_error(format!("failed to read component conflict source: {error}"))
            }),
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
fn create_context_from_generated_row(
    generated_schema: &str,
    row: &WasmRow<WasmHostBytes>,
) -> Option<WasmCreateContext> {
    if row.key.schema_key.as_str() != generated_schema {
        return None;
    }
    let [id] = row.key.row_pk.as_slice() else {
        return None;
    };
    create_context_from_uuid(id.as_str())
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

    fn begin_row_attachment(
        &mut self,
        resource: Resource<TransitionResource>,
        total_length: u64,
    ) -> Result<u32, bindings::lix::plugin::host::HostError> {
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
        if state.pending_attachment.is_some() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "row attachment is already open".to_owned(),
            ));
        }
        if state.attachments.len() >= state.limits.max_attachment_refs as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "row attachment count exceeds its transition limit".to_owned(),
            ));
        }
        if total_length > state.limits.max_total_bytes {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "row attachment exceeds max-total-bytes".to_owned(),
            ));
        }
        let capacity = usize::try_from(total_length).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "row attachment exceeds host address space".to_owned(),
            )
        })?;
        let ordinal = u32::try_from(state.attachments.len()).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "row attachment ordinal exceeds u32".to_owned(),
            )
        })?;
        state.pending_attachment = Some(PendingRowAttachment {
            expected_len: total_length,
            bytes: Vec::with_capacity(capacity),
        });
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(ordinal)
    }

    fn write_row_attachment(
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
        let pending = state.pending_attachment.as_mut().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "row attachment chunk has no open attachment".to_owned(),
            )
        })?;
        let next_len = pending
            .bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "row attachment length overflowed".to_owned(),
                )
            })?;
        if next_len as u64 > pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "row attachment exceeds its declared length".to_owned(),
            ));
        }
        pending.bytes.extend_from_slice(&chunk);
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.row_output_attachment_writes = state
            .counters
            .row_output_attachment_writes
            .saturating_add(1);
        state.counters.row_output_attachment_bytes = state
            .counters
            .row_output_attachment_bytes
            .saturating_add(chunk.len() as u64);
        Ok(())
    }

    fn finish_row_attachment(
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
        let pending = state.pending_attachment.take().ok_or_else(|| {
            bindings::lix::plugin::host::HostError::Rejected(
                "row attachment finish has no open attachment".to_owned(),
            )
        })?;
        if pending.bytes.len() as u64 != pending.expected_len {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "row attachment ended before its declared length".to_owned(),
            ));
        }
        state.attachments.push(Bytes::from(pending.bytes));
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        Ok(())
    }

    fn emit_rows(
        &mut self,
        resource: Resource<TransitionResource>,
        mut page: bindings::lix::plugin::host::RowPage,
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
        let decoded = RowPage::decode(&page.payload).map_err(|error| {
            bindings::lix::plugin::host::HostError::Rejected(format!("invalid row page: {error:?}"))
        })?;
        let page_wire_bytes = page.payload.len() as u64;
        if state.counters.packet_pages.saturating_add(1) > u64::from(state.limits.max_pages) {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "row page count exceeds its transition limit".to_owned(),
            ));
        }
        state.charge_page(page.payload.len())?;
        let limits = state.limits;
        let creates = state.creates;
        let attachments: Arc<[Bytes]> = state.attachments.clone().into();
        let total_record_count = decoded.record_count();
        let section = decoded.section().map_err(|error| {
            bindings::lix::plugin::host::HostError::Rejected(format!(
                "invalid row page section: {error:?}"
            ))
        })?;
        let payload_end = section.payload.len();
        let record_count = section.record_count;
        if section.representation != Representation::Snapshots {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "row page must use snapshot records".to_owned(),
            ));
        }
        page.payload.truncate(payload_end);
        let pending = PendingChangePage::Packet {
            record_count,
            payload: page.payload,
            attachments,
            max_page_bytes: limits.max_page_bytes,
            limits,
            creates,
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
        ensure_source_page(max_bytes, state.limits.max_page_bytes)?;
        let envelope_bytes = u32::try_from(
            crate::plugin::wire::single_section_overhead("", &[]).expect("fixed page overhead"),
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
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
            return Ok(None);
        };
        if changes.is_empty() {
            return Err(bindings::lix::plugin::host::HostError::Rejected(
                "component row source returned an empty page".to_owned(),
            ));
        }
        state.lazy_snapshots.clear();
        let record_count = u32::try_from(changes.len()).map_err(|_| {
            bindings::lix::plugin::host::HostError::LimitExceeded(
                "row input page record count exceeds u32".to_owned(),
            )
        })?;
        let mut payload = Vec::new();
        for change in changes {
            let ordinal = state.next_ordinal;
            state.next_ordinal = state.next_ordinal.checked_add(1).ok_or_else(|| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "component row input ordinal overflowed".to_owned(),
                )
            })?;
            let (schema_key, row_pk, snapshot_content, effect) = match change {
                WasmRowChange::Create {
                    schema_key,
                    resolved_key,
                    snapshot_content,
                    ..
                } => {
                    let key = resolved_key.ok_or_else(|| {
                        bindings::lix::plugin::host::HostError::Rejected(
                            "host-to-guest row input contains an unresolved create".to_owned(),
                        )
                    })?;
                    (
                        schema_key,
                        key.row_pk
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>(),
                        Some(snapshot_content),
                        WasmChangeEffect::Content,
                    )
                }
                WasmRowChange::Upsert { row, effect } => (
                    row.key.schema_key.to_string(),
                    row.key.row_pk.iter().map(ToString::to_string).collect(),
                    Some(row.snapshot_content),
                    effect,
                ),
                WasmRowChange::Delete(key) => (
                    key.schema_key.to_string(),
                    key.row_pk.iter().map(ToString::to_string).collect(),
                    None,
                    WasmChangeEffect::Content,
                ),
            };
            let record_start = payload.len();
            payload.extend_from_slice(&0_u32.to_le_bytes());
            if snapshot_content.is_some() {
                payload.push(0);
            } else {
                payload.push(1);
            }
            encode_packet_text(&mut payload, &schema_key)
                .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
            payload.extend_from_slice(
                &u32::try_from(row_pk.len())
                    .map_err(|_| {
                        bindings::lix::plugin::host::HostError::LimitExceeded(
                            "row input primary key has too many components".to_owned(),
                        )
                    })?
                    .to_le_bytes(),
            );
            for component in &row_pk {
                encode_packet_text(&mut payload, component).map_err(|error| {
                    bindings::lix::plugin::host::HostError::Rejected(error.message)
                })?;
            }
            if let Some(snapshot_content) = snapshot_content {
                payload.push(match effect {
                    WasmChangeEffect::Content => 0,
                    WasmChangeEffect::FormatOnly => 1,
                });
                let inline_len = u32::try_from(snapshot_content.len()).ok();
                let inline_fits = inline_len.is_some_and(|length| {
                    payload
                        .len()
                        .checked_add(1 + 4 + length as usize)
                        .is_some_and(|end| {
                            end <= payload_budget as usize
                                && end - record_start <= state.limits.max_record_bytes as usize
                        })
                });
                if inline_fits {
                    let length = inline_len.expect("checked inline length");
                    payload.push(0);
                    payload.extend_from_slice(&length.to_le_bytes());
                    payload.extend_from_slice(
                        &read_host_bytes(&snapshot_content, 0, length).map_err(|error| {
                            bindings::lix::plugin::host::HostError::Rejected(error.message)
                        })?,
                    );
                } else {
                    payload.push(1);
                    payload.extend_from_slice(&ordinal.to_le_bytes());
                    payload.extend_from_slice(&snapshot_content.len().to_le_bytes());
                    state.lazy_snapshots.insert(ordinal, snapshot_content);
                }
            }
            let record_len = u32::try_from(payload.len() - record_start - 4).map_err(|_| {
                bindings::lix::plugin::host::HostError::LimitExceeded(
                    "row input record exceeds u32".to_owned(),
                )
            })?;
            payload[record_start..record_start + 4].copy_from_slice(&record_len.to_le_bytes());
        }
        let page = encode_single_section(
            Representation::Snapshots,
            Operation::Mixed,
            "",
            &[],
            record_count,
            payload,
        )
        .map_err(|error| {
            bindings::lix::plugin::host::HostError::Rejected(format!(
                "failed to encode row input page: {error:?}"
            ))
        })?;
        if page.len() > max_bytes as usize {
            return Err(bindings::lix::plugin::host::HostError::LimitExceeded(
                "row input page exceeds the requested byte limit".to_owned(),
            ));
        }
        let boundary_bytes = page.len();
        state.charge(boundary_bytes)?;
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
        Ok(Some(bindings::lix::plugin::host::RowPage { payload: page }))
    }

    fn read_attachment(
        &mut self,
        resource: Resource<RowSourceResource>,
        ordinal: u32,
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
        let snapshot = state
            .lazy_snapshots
            .get(&ordinal)
            .ok_or(bindings::lix::plugin::host::HostError::InvalidRange)?;
        let bytes = read_host_bytes(snapshot, offset, length)
            .map_err(|error| bindings::lix::plugin::host::HostError::Rejected(error.message))?;
        state.charge(bytes.len())?;
        state.counters.component_import_calls =
            state.counters.component_import_calls.saturating_add(1);
        state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
        state.counters.source_bytes_read = state
            .counters
            .source_bytes_read
            .saturating_add(bytes.len() as u64);
        state.counters.row_input_attachment_reads =
            state.counters.row_input_attachment_reads.saturating_add(1);
        state.counters.row_input_attachment_bytes = state
            .counters
            .row_input_attachment_bytes
            .saturating_add(bytes.len() as u64);
        Ok(Some(bytes))
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
        let meta = bindings::lix::plugin::host::ColumnMergeMeta {
            ordinal: conflict.ordinal,
            schema_key: conflict.key.schema_key.to_string(),
            row_pk: conflict
                .key
                .row_pk
                .iter()
                .map(ToString::to_string)
                .collect(),
            file_id: conflict.file_id.clone(),
            column: conflict.column.clone(),
            base_len: conflict.base.as_ref().map(WasmHostBytes::len),
            a_len: conflict.a.as_ref().map(WasmHostBytes::len),
            b_len: conflict.b.as_ref().map(WasmHostBytes::len),
            base_row_len: conflict.base_row.len(),
            a_row_len: conflict.a_row.len(),
            b_row_len: conflict.b_row.len(),
        };
        let metadata_bytes =
            meta.schema_key.len() + meta.row_pk.iter().map(String::len).sum::<usize>() + 32;
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
        let bytes = read_host_bytes(conflict_row(conflict, side), offset, length)
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

struct ValidatedCreatedPacketPage {
    schemas: Vec<String>,
    identities: Vec<ValidatedPacketIdentity>,
}

enum ValidatedPacketIdentity {
    Explicit {
        schema_index: u32,
        fingerprint: [u8; 32],
        generated_local_ref: Option<u64>,
    },
    Create {
        schema_index: u32,
        local_ref: u64,
    },
}

enum CreatedPacketIdentity {
    Explicit(WasmRowKey),
    Create { schema_key: String, local_ref: u64 },
}

#[derive(Debug, Default)]
struct CertifiedPacketSchemaKeys {
    create_refs: std::collections::HashSet<u64>,
    explicit_keys: std::collections::HashSet<[u8; 32]>,
    explicit_create_refs: std::collections::HashSet<u64>,
    create_ref_ranges: Vec<(u64, u64)>,
}

#[derive(Debug, Default)]
struct CertifiedPacketRowKeys {
    schemas: std::collections::BTreeMap<String, CertifiedPacketSchemaKeys>,
}

impl CertifiedPacketRowKeys {
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
        Self::generated_local_ref_component(component, creates)
    }

    fn generated_local_ref_component(component: &str, creates: WasmCreateContext) -> Option<u64> {
        if component.len() != 36
            || component
                .as_bytes()
                .iter()
                .enumerate()
                .any(|(index, byte)| {
                    if matches!(index, 8 | 13 | 18 | 23) {
                        *byte != b'-'
                    } else {
                        !byte.is_ascii_digit() && !(b'a'..=b'f').contains(byte)
                    }
                })
        {
            return None;
        }
        let id = uuid::Uuid::parse_str(component).ok()?;
        let bytes = id.into_bytes();
        if bytes[..8] != creates.high.to_be_bytes() || bytes[8..12] != creates.low.to_be_bytes() {
            return None;
        }
        Some(u64::from(u32::from_be_bytes(bytes[12..].try_into().ok()?)))
    }

    fn explicit_key_fingerprint<'a>(components: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix-certified-packet-explicit-key-v1\0");
        for component in components {
            hasher.update(
                &u64::try_from(component.len())
                    .expect("packet key component length fits u64")
                    .to_be_bytes(),
            );
            hasher.update(component);
        }
        *hasher.finalize().as_bytes()
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
                    .row_pk
                    .into_iter()
                    .map(|component| component.as_str().to_owned())
                    .collect::<Vec<_>>();
                let create_ref = Self::generated_local_ref(&components, creates);
                (
                    key.schema_key.as_str().to_owned(),
                    Some(Self::explicit_key_fingerprint(
                        components.iter().map(String::as_bytes),
                    )),
                    create_ref,
                )
            }
            CreatedPacketIdentity::Create {
                schema_key,
                local_ref,
            } => (schema_key, None, Some(local_ref)),
        };
        self.insert_compact(schema_key, explicit_key, create_ref, existing)
    }

    fn insert_compact(
        &mut self,
        schema_key: String,
        explicit_key: Option<[u8; 32]>,
        create_ref: Option<u64>,
        existing: &Self,
    ) -> Result<(), LixError> {
        let existing = existing.schemas.get(&schema_key);
        let page = self.schemas.entry(schema_key).or_default();
        match explicit_key {
            Some(fingerprint) => {
                if existing.is_some_and(|keys| keys.explicit_keys.contains(&fingerprint))
                    || !page.explicit_keys.insert(fingerprint)
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

    fn insert_validated(
        &mut self,
        identity: ValidatedPacketIdentity,
        schemas: &[String],
        existing: &Self,
    ) -> Result<(), LixError> {
        let (schema_index, fingerprint, create_ref, explicit) = match identity {
            ValidatedPacketIdentity::Explicit {
                schema_index,
                fingerprint,
                generated_local_ref,
            } => (schema_index, Some(fingerprint), generated_local_ref, true),
            ValidatedPacketIdentity::Create {
                schema_index,
                local_ref,
            } => (schema_index, None, Some(local_ref), false),
        };
        let schema_key = schemas
            .get(usize::try_from(schema_index).expect("schema index fits usize"))
            .expect("validated packet identity has a known schema");
        debug_assert_eq!(explicit, fingerprint.is_some());
        self.insert_compact(schema_key.clone(), fingerprint, create_ref, existing)
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

    fn extend_ref(&mut self, page: &Self) {
        for (schema_key, page) in &page.schemas {
            let keys = self.schemas.entry(schema_key.clone()).or_default();
            keys.create_refs.extend(page.create_refs.iter().copied());
            keys.explicit_keys
                .extend(page.explicit_keys.iter().copied());
            keys.explicit_create_refs
                .extend(page.explicit_create_refs.iter().copied());
            keys.create_ref_ranges
                .extend(page.create_ref_ranges.iter().copied());
        }
    }

    fn take_create_ranges_for(&mut self, schema_keys: &[String]) -> Vec<WasmCertifiedCreateRange> {
        let mut output = Vec::new();
        for schema_key in schema_keys {
            let Some(keys) = self.schemas.remove(schema_key) else {
                continue;
            };
            let mut ranges = keys
                .create_ref_ranges
                .into_iter()
                .chain(
                    keys.create_refs
                        .into_iter()
                        .map(|local_ref| (local_ref, local_ref)),
                )
                .chain(
                    keys.explicit_create_refs
                        .into_iter()
                        .map(|local_ref| (local_ref, local_ref)),
                )
                .collect::<Vec<_>>();
            ranges.sort_unstable();
            let mut compact = Vec::<(u64, u64)>::with_capacity(ranges.len());
            for (first, last) in ranges {
                if let Some((_, compact_last)) = compact.last_mut()
                    && first <= compact_last.saturating_add(1)
                {
                    *compact_last = (*compact_last).max(last);
                    continue;
                }
                compact.push((first, last));
            }
            output.extend(
                compact
                    .into_iter()
                    .map(|(first, last)| WasmCertifiedCreateRange {
                        schema_key: schema_key.clone(),
                        first_local_ref: u32::try_from(first)
                            .expect("validated create local ref fits u32"),
                        last_local_ref: u32::try_from(last)
                            .expect("validated create local ref fits u32"),
                    }),
            );
        }
        output
    }
}

fn duplicate_certified_packet_key() -> LixError {
    component_error("a component row key may occur only once across certified packet pages")
}

fn validate_new_certified_packet_keys(
    page: ValidatedCreatedPacketPage,
    existing: &CertifiedPacketRowKeys,
) -> Result<(BTreeSet<String>, CertifiedPacketRowKeys), LixError> {
    let ValidatedCreatedPacketPage {
        schemas,
        identities,
    } = page;
    let mut page_keys = CertifiedPacketRowKeys::default();
    for identity in identities {
        page_keys.insert_validated(identity, &schemas, existing)?;
    }
    Ok((schemas.into_iter().collect(), page_keys))
}

fn validate_ordinary_packet_page_keys(
    page: &WasmChangePage,
    creates: WasmCreateContext,
    existing: &CertifiedPacketRowKeys,
) -> Result<CertifiedPacketRowKeys, LixError> {
    let mut page_keys = CertifiedPacketRowKeys::default();
    for change in &page.changes.changes {
        let identity = match change {
            WasmRowChange::Create {
                schema_key,
                local_ref,
                ..
            } => CreatedPacketIdentity::Create {
                schema_key: schema_key.clone(),
                local_ref: *local_ref,
            },
            WasmRowChange::Upsert { row, .. } => CreatedPacketIdentity::Explicit(row.key.clone()),
            WasmRowChange::Delete(key) => CreatedPacketIdentity::Explicit(key.clone()),
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
    creates: WasmCreateContext,
) -> Result<Option<ValidatedCreatedPacketPage>, String> {
    if record_count == 0 {
        return Err("packet page is empty".to_owned());
    }
    let mut input = PacketSliceReader { payload, offset: 0 };
    let mut schemas = Vec::<String>::new();
    let mut identities = Vec::with_capacity(record_count as usize);
    let mut previous_local_ref = None;
    for _ in 0..record_count {
        let record_len = input.u32()? as usize;
        let record_bytes = input.bytes(record_len)?;
        let mut record = PacketSliceReader {
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
        let schema_index = match schemas.iter().position(|candidate| candidate == schema) {
            Some(index) => index,
            None => {
                schemas.push(schema.to_owned());
                schemas.len() - 1
            }
        };
        let schema_index =
            u32::try_from(schema_index).map_err(|_| "packet has too many schemas")?;
        match tag {
            0 => {
                let component_count = record.u32()?;
                if component_count == 0 {
                    return Err("packet upsert key has no components".to_owned());
                }
                let mut fingerprint = blake3::Hasher::new();
                fingerprint.update(b"lix-certified-packet-explicit-key-v1\0");
                let mut only_component = None;
                for _ in 0..component_count {
                    let component_len = record.u32()? as usize;
                    let component = record.bytes(component_len)?;
                    let component = std::str::from_utf8(component)
                        .map_err(|error| format!("packet key component is not UTF-8: {error}"))?;
                    fingerprint.update(
                        &u64::try_from(component.len())
                            .expect("packet key component length fits u64")
                            .to_be_bytes(),
                    );
                    fingerprint.update(component.as_bytes());
                    if component_count == 1 {
                        only_component = Some(component);
                    }
                }
                if record.u8()? > 1 {
                    return Err("packet upsert has an invalid effect".to_owned());
                }
                identities.push(ValidatedPacketIdentity::Explicit {
                    schema_index,
                    fingerprint: *fingerprint.finalize().as_bytes(),
                    generated_local_ref: only_component.and_then(|component| {
                        CertifiedPacketRowKeys::generated_local_ref_component(component, creates)
                    }),
                });
            }
            2 => {
                let local_ref = record.u64()?;
                if u32::try_from(local_ref).is_err() {
                    return Err("packet create local ref exceeds u32".to_owned());
                }
                if previous_local_ref.is_some_and(|previous| previous >= local_ref) {
                    return Err("packet create local refs must be strictly increasing".to_owned());
                }
                previous_local_ref = Some(local_ref);
                identities.push(ValidatedPacketIdentity::Create {
                    schema_index,
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
    Ok(Some(ValidatedCreatedPacketPage {
        schemas,
        identities,
    }))
}

struct PacketSliceReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PacketSliceReader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "packet range overflowed".to_owned())?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| "packet ended early".to_owned())?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.bytes(1)?[0])
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
            durable_checkpoint: DurableCheckpointCache::default(),
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
        let row_state = Arc::new(Mutex::new(RowChangeState::from_rows(
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
    complete_file_state: bool,
    /// Whether a complete parse may be retained as a certified packet. See
    /// [`WasmOpenFileInput::certified_packets_available`]: a file with no
    /// published commit has no root to expand a packet from, so its pages take
    /// the ordinary decode path instead.
    certified_packets_available: bool,
    certified_all_row_keys: CertifiedPacketRowKeys,
    certified_packet_pages: Vec<Bytes>,
    certified_packet_rows: u64,
    certified_packet_creates: Option<WasmCreateContext>,
    certified_packet_schema_keys: BTreeSet<String>,
    certified_packet_row_keys: CertifiedPacketRowKeys,
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
    durable_checkpoint: DurableCheckpointCache,
    retired: bool,
    next_document: u64,
}

fn encode_durable_document_checkpoint(
    root: &ArenaRoot,
    max_decoded_bytes: usize,
) -> Option<WasmDurableDocumentCheckpoint> {
    if root.successor_checkpoint_encoded_len().ok()? > max_decoded_bytes {
        return None;
    }
    let bytes = root.encode_successor_checkpoint().ok()?.into();
    WasmDurableDocumentCheckpoint::new(bytes).ok()
}

#[derive(Default)]
struct DurableCheckpointCache(Option<(ArenaDigest, WasmDurableDocumentCheckpoint)>);

impl DurableCheckpointCache {
    fn get(&self, state_id: &ArenaDigest) -> Option<WasmDurableDocumentCheckpoint> {
        self.0
            .as_ref()
            .filter(|(cached_state_id, _)| cached_state_id == state_id)
            .map(|(_, checkpoint)| checkpoint.clone())
    }

    fn insert(&mut self, state_id: ArenaDigest, checkpoint: WasmDurableDocumentCheckpoint) {
        self.0 = Some((state_id, checkpoint));
    }
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
        let state_id = root.state.id();
        let durable = if let Some(checkpoint) = self.durable_checkpoint.get(&state_id) {
            Some(checkpoint)
        } else {
            encode_durable_document_checkpoint(
                &root,
                WasmDurableDocumentCheckpoint::MAX_DECODED_BYTES,
            )
            .inspect(|checkpoint| {
                self.durable_checkpoint.insert(state_id, checkpoint.clone());
            })
        };
        Ok(Some(if let Some(durable) = durable {
            WasmDocumentCheckpoint::new_with_durable(root, retained_bytes, durable)
        } else {
            WasmDocumentCheckpoint::new(root, retained_bytes)
        }))
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

    async fn restore_durable_document(
        &mut self,
        checkpoint: &[u8],
        accepted: &[u8],
    ) -> Result<WasmDocumentHandle, LixError> {
        self.ensure_active()?;
        let root =
            ArenaRoot::decode_successor_checkpoint(accepted, checkpoint).map_err(|error| {
                component_error(format!(
                    "failed to decode component document checkpoint: {error}"
                ))
            })?;
        self.durable_checkpoint.insert(
            root.state.id(),
            WasmDurableDocumentCheckpoint::new(checkpoint.to_vec().into())?,
        );
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
        let certified_packets_available = input.certified_packets_available;
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
                complete_file_state: true,
                certified_packets_available,
                certified_all_row_keys: CertifiedPacketRowKeys::default(),
                certified_packet_pages: Vec::new(),
                certified_packet_rows: 0,
                certified_packet_creates: None,
                certified_packet_schema_keys: BTreeSet::new(),
                certified_packet_row_keys: CertifiedPacketRowKeys::default(),
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
                complete_file_state: output.replace_all_rows,
                certified_packets_available: true,
                certified_all_row_keys: CertifiedPacketRowKeys::default(),
                certified_packet_pages: Vec::new(),
                certified_packet_rows: 0,
                certified_packet_creates: None,
                certified_packet_schema_keys: BTreeSet::new(),
                certified_packet_row_keys: CertifiedPacketRowKeys::default(),
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
                complete_file_state: output.replace_all_rows,
                certified_packets_available: true,
                certified_all_row_keys: CertifiedPacketRowKeys::default(),
                certified_packet_pages: Vec::new(),
                certified_packet_rows: 0,
                certified_packet_creates: None,
                certified_packet_schema_keys: BTreeSet::new(),
                certified_packet_row_keys: CertifiedPacketRowKeys::default(),
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
                        RuntimeColumnMergeResult::Replace(Some(WasmGuestBytes::Output(
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
                format_version: PACKET_FORMAT_V1,
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
        loop {
            match cursor.pages.pop_front() {
                Some(PendingChangePage::Decoded(page)) => return Ok(Some(page)),
                Some(PendingChangePage::Packet {
                    record_count,
                    payload,
                    attachments,
                    max_page_bytes,
                    limits,
                    creates,
                }) => {
                    if let Some(validated_page) =
                        validate_created_packet_page(record_count, &payload, creates)
                            .map_err(component_error)?
                    {
                        // Certified immutable segments are the ownership unit
                        // for complete bulk state. Sparse successors stay as
                        // ordinary row overlays: this lets validation observe
                        // their durable base directly and avoids paying a new
                        // segment/manifest lifecycle for one or two rows.
                        //
                        // A file that publishes no commit takes the same
                        // ordinary path even for a complete parse, because a
                        // certified packet is materialized by expanding it from
                        // the file's commit root and there is none to expand
                        // from. Same rows, row-shaped instead of packed.
                        if !cursor.complete_file_state || !cursor.certified_packets_available {
                            let decoded = PendingChangePage::Packet {
                                record_count,
                                payload,
                                attachments,
                                max_page_bytes,
                                limits,
                                creates,
                            }
                            .decode()?;
                            let page_keys = validate_ordinary_packet_page_keys(
                                &decoded,
                                creates,
                                &cursor.certified_all_row_keys,
                            )?;
                            cursor.certified_all_row_keys.extend(page_keys);
                            return Ok(Some(decoded));
                        }
                        if cursor
                            .certified_packet_creates
                            .is_some_and(|existing| existing != creates)
                        {
                            return Err(component_error(
                                "one certified packet transition used multiple create contexts",
                            ));
                        }
                        let (schema_keys, page_keys) = validate_new_certified_packet_keys(
                            validated_page,
                            &cursor.certified_all_row_keys,
                        )?;
                        cursor.certified_packet_creates = Some(creates);
                        cursor.certified_packet_rows = cursor
                            .certified_packet_rows
                            .checked_add(u64::from(record_count))
                            .ok_or_else(|| {
                                component_error("certified packet row count overflowed")
                            })?;
                        cursor.certified_packet_schema_keys.extend(schema_keys);
                        cursor.certified_all_row_keys.extend_ref(&page_keys);
                        cursor.certified_packet_row_keys.extend(page_keys);
                        cursor.certified_packet_pages.push(Bytes::from(payload));
                    } else {
                        let decoded = PendingChangePage::Packet {
                            record_count,
                            payload,
                            attachments,
                            max_page_bytes,
                            limits,
                            creates,
                        }
                        .decode()?;
                        let page_keys = validate_ordinary_packet_page_keys(
                            &decoded,
                            creates,
                            &cursor.certified_all_row_keys,
                        )?;
                        cursor.certified_all_row_keys.extend(page_keys);
                        return Ok(Some(decoded));
                    }
                }
                None => return Ok(None),
            }
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

    fn take_certified_row_batches(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Vec<WasmCertifiedRowBatch> {
        let Some(cursor) = self
            .cursors
            .values_mut()
            .find(|cursor| cursor.transition == transition)
        else {
            return Vec::new();
        };
        let mut batches = Vec::with_capacity(1);
        if let Some(creates) = cursor.certified_packet_creates.take() {
            let schema_keys = std::mem::take(&mut cursor.certified_packet_schema_keys)
                .into_iter()
                .collect::<Vec<_>>();
            let create_ranges = cursor
                .certified_packet_row_keys
                .take_create_ranges_for(&schema_keys);
            cursor.certified_packet_row_keys = CertifiedPacketRowKeys::default();
            batches.push(WasmCertifiedRowBatch {
                format: CERTIFIED_CREATED_PACKET_V1,
                schema_keys,
                row_count: std::mem::take(&mut cursor.certified_packet_rows),
                creates,
                create_ranges,
                complete_file_state: cursor.complete_file_state,
                pages: std::mem::take(&mut cursor.certified_packet_pages),
            });
        }
        cursor.certified_all_row_keys = CertifiedPacketRowKeys::default();
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

    #[test]
    fn warm_parse_changes_row_source_exposes_complete_current_rows() {
        let limits = WasmTransitionLimits::default();
        let key = WasmRowKey::from_owned_parts("note", vec!["note-1".to_owned()]);
        let source = crate::plugin::runtime::VecRowSource::new(
            vec![WasmRow {
                key: key.clone(),
                snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                    br#"{"body":"current"}"#,
                )),
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
        let keep = WasmRowKey::from_owned_parts("note", vec!["keep".to_owned()]);
        let omitted = WasmRowKey::from_owned_parts("note", vec!["omitted".to_owned()]);
        let mut state =
            TransitionState::new(limits, WasmCreateContext { high: 1, low: 2 }, false, None)
                .expect("transition state");
        state.replace_all_rows = true;
        state
            .pages
            .push_back(PendingChangePage::Decoded(WasmChangePage {
                format_version: PACKET_FORMAT_V1,
                changes: WasmRowChanges {
                    changes: vec![WasmRowChange::Upsert {
                        row: WasmRow {
                            key: keep.clone(),
                            snapshot_content: WasmGuestBytes::Inline(Bytes::from_static(
                                br#"{"body":"kept"}"#,
                            )),
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

    #[cfg(any())]
    #[derive(Clone, Debug)]
    struct JsonTestSource(Vec<u8>);

    #[cfg(any())]
    impl WasmByteSource for JsonTestSource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }

        fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
            let start = usize::try_from(offset)
                .map_err(|_| component_error("JSON test source offset exceeds usize"))?;
            let end = start
                .checked_add(length as usize)
                .ok_or_else(|| component_error("JSON test source range overflowed"))?;
            self.0
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| component_error("JSON test source range is out of bounds"))
        }
    }

    #[cfg(any())]
    struct JsonTestRowSource {
        rows: Option<Vec<WasmRow<WasmHostBytes>>>,
    }

    #[cfg(any())]
    impl lix::plugin::runtime::WasmRowSource for JsonTestRowSource {
        fn next_page(
            &mut self,
            _max_bytes: u32,
        ) -> Result<Option<lix::plugin::runtime::WasmRowPage>, LixError> {
            Ok(self
                .rows
                .take()
                .map(|rows| lix::plugin::runtime::WasmRowPage { rows }))
        }
    }

    #[cfg(any())]
    fn assert_json_full_fallback_state_only(checkpoint: &WasmDocumentCheckpoint) {
        let root = checkpoint
            .downcast_ref::<ArenaRoot>()
            .expect("default runtime checkpoint should contain an arena root");
        let keys = root.state.keys().collect::<Vec<_>>();
        assert_eq!(
            keys.len(),
            3,
            "cold fallback should retain only namespace, fallback manifest, and one fallback page"
        );
        assert!(keys.contains(&b"json/fallback-rows".as_slice()));
        assert!(
            keys.iter()
                .any(|key| key.starts_with(b"json/fallback-row-page/"))
        );
        assert!(!keys.contains(&b"json/scalar-index".as_slice()));
        assert!(
            !keys
                .iter()
                .any(|key| key.starts_with(b"json/scalar-index-page/"))
        );
        assert!(!keys.iter().any(|key| key.starts_with(b"json/scalar-page/")));
    }

    // Compiled-plugin behavior is covered by `lix_e2e`; keeping an artifact
    // dependency here would create `lix -> plugin -> lix`.
    #[cfg(any())]
    #[tokio::test]
    async fn json_cold_full_fallback_checkpoint_omits_scalar_state() {
        let wasm = std::fs::read(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json"))
            .expect("read JSON component");
        let factory = crate::default_wasm_runtime()
            .expect("default Wasm runtime")
            .compile_component(
                wasm,
                WasmLimits::default(),
                PluginCapabilities {
                    column_merger: true,
                    file_projection: true,
                },
            )
            .await
            .expect("compile JSON component");
        let descriptor = WasmFileDescriptor {
            file_id: "direct-cold-fallback".to_owned(),
            path: Some("/direct-cold-fallback.json".to_owned()),
            plugin: WasmPluginSelection {
                plugin_key: "plugin_json".to_owned(),
                generation: "direct".to_owned(),
            },
        };
        let creates = WasmCreateContext { high: 13, low: 17 };
        let before = br#"{"a":"one","b":"two"}"#.to_vec();
        let cold_after = br#"{"a":"ONE","b":"two"}"#.to_vec();
        let rows = vec![
            WasmRow {
                key: WasmRowKey::from_owned_parts("json_root", vec!["root".to_owned()]),
                snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                    br#"{"id":"root","kind":"object"}"#,
                )),
            },
            WasmRow {
                key: WasmRowKey::from_owned_parts(
                    "json_object_member",
                    vec!["root".to_owned(), "a".to_owned()],
                ),
                snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                    br#"{"parent_id":"root","key":"a","order_key":"40","kind":"string","scalar_json":"\"one\""}"#,
                )),
            },
            WasmRow {
                key: WasmRowKey::from_owned_parts(
                    "json_object_member",
                    vec!["root".to_owned(), "b".to_owned()],
                ),
                snapshot_content: WasmHostBytes::Inline(Bytes::from_static(
                    br#"{"parent_id":"root","key":"b","order_key":"80","kind":"string","scalar_json":"\"two\""}"#,
                )),
            },
        ];
        let limits = WasmTransitionLimits::default();
        let mut actor = factory.instantiate_actor().await.unwrap();
        let cold = actor
            .cold_file_changed(
                limits,
                WasmColdFileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor.clone(),
                    before: Some(Arc::new(JsonTestSource(before))),
                    edits: vec![WasmInputSplice {
                        offset: 6,
                        delete_len: 3,
                        insert: WasmInputBytes::Inline(b"ONE".to_vec()),
                    }],
                    after: Arc::new(JsonTestSource(cold_after.clone())),
                    creates,
                    rows: Box::new(JsonTestRowSource { rows: Some(rows) }),
                },
            )
            .await
            .expect("cold full-fallback transition should succeed");
        while actor
            .next_change_page(cold.transition, cold.changes, 2 * 1024 * 1024)
            .await
            .unwrap()
            .is_some()
        {}
        actor.finish_transition(cold.transition).await.unwrap();

        let checkpoint = actor
            .checkpoint_document(cold.document)
            .await
            .unwrap()
            .expect("component actor should expose a checkpoint");
        assert_json_full_fallback_state_only(&checkpoint);
        let durable = checkpoint
            .durable_checkpoint()
            .expect("small JSON checkpoint should be durable");
        let decoded = WasmDurableDocumentCheckpoint::decode(durable.bytes().as_ref())
            .expect("durable JSON checkpoint should decode");
        actor.retire().await.unwrap();

        let mut reopened = factory.instantiate_actor().await.unwrap();
        let document = reopened
            .restore_durable_document(&decoded, &cold_after)
            .await
            .expect("durable fallback checkpoint should reopen against accepted bytes");
        let successor_after = br#"{"a":"ONE","b":"TWO"}"#.to_vec();
        let edit_offset = cold_after
            .windows(3)
            .position(|window| window == b"two")
            .expect("fixture should contain edited scalar");
        let successor = reopened
            .file_changed(
                document,
                limits,
                WasmFileUpdate {
                    before_descriptor: descriptor.clone(),
                    after_descriptor: descriptor,
                    before: Arc::new(JsonTestSource(cold_after)),
                    edits: vec![WasmInputSplice {
                        offset: edit_offset as u64,
                        delete_len: 3,
                        insert: WasmInputBytes::Inline(b"TWO".to_vec()),
                    }],
                    after: Arc::new(JsonTestSource(successor_after.clone())),
                    creates,
                    rows: Some(Box::new(JsonTestRowSource { rows: Some(rows) })),
                    prior_row_keys: None,
                },
            )
            .await
            .expect("full-path successor should read fallback state");
        let mut changes = Vec::new();
        while let Some(page) = reopened
            .next_change_page(successor.transition, successor.changes, 2 * 1024 * 1024)
            .await
            .unwrap()
        {
            changes.extend(page.changes.changes);
        }
        let counters = reopened
            .finish_transition(successor.transition)
            .await
            .unwrap();
        assert!(
            counters.state_read_calls >= 3,
            "reopened successor should read namespace, fallback manifest, and fallback page"
        );
        assert_eq!(changes.len(), 1);
        let WasmRowChange::Upsert { row, effect } = &changes[0] else {
            panic!("successor should upsert the edited JSON member");
        };
        assert_eq!(*effect, WasmChangeEffect::Content);
        assert_eq!(row.key.schema_key.as_str(), "json_object_member");
        assert_eq!(row.key.row_pk[1].as_str(), "b");
        let WasmGuestBytes::Inline(snapshot) = &row.snapshot_content else {
            panic!("small JSON snapshot should remain inline");
        };
        let snapshot: serde_json::Value =
            serde_json::from_slice(snapshot).expect("successor snapshot should be JSON");
        assert_eq!(snapshot["scalar_json"], r#""TWO""#);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&successor_after).unwrap(),
            serde_json::json!({"a": "ONE", "b": "TWO"})
        );

        let successor_checkpoint = reopened
            .checkpoint_document(successor.document)
            .await
            .unwrap()
            .expect("successor should expose a checkpoint");
        assert_json_full_fallback_state_only(&successor_checkpoint);
        reopened.retire().await.unwrap();
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
    fn oversized_durable_checkpoint_is_omitted_without_rejecting_the_root() {
        let root = ArenaRoot::import(
            ArenaStore::default(),
            "checkpoint-test",
            b"accepted",
            std::iter::empty(),
            [(b"index".to_vec(), b"opaque-state".to_vec())],
        )
        .successor_checkpoint();
        let encoded_len = root.successor_checkpoint_encoded_len().unwrap();

        assert!(encode_durable_document_checkpoint(&root, encoded_len - 1).is_none());
        assert!(encode_durable_document_checkpoint(&root, encoded_len).is_some());
    }

    #[test]
    fn durable_checkpoint_cache_retains_only_the_latest_state() {
        let first = ArenaRoot::import(
            ArenaStore::default(),
            "checkpoint-test",
            b"accepted",
            std::iter::empty(),
            [(b"index".to_vec(), b"first".to_vec())],
        )
        .successor_checkpoint();
        let second = ArenaRoot::import(
            ArenaStore::default(),
            "checkpoint-test",
            b"accepted",
            std::iter::empty(),
            [(b"index".to_vec(), b"second".to_vec())],
        )
        .successor_checkpoint();
        let first_id = first.state.id();
        let second_id = second.state.id();
        let mut cache = DurableCheckpointCache::default();

        cache.insert(
            first_id,
            encode_durable_document_checkpoint(
                &first,
                WasmDurableDocumentCheckpoint::MAX_DECODED_BYTES,
            )
            .unwrap(),
        );
        cache.insert(
            second_id,
            encode_durable_document_checkpoint(
                &second,
                WasmDurableDocumentCheckpoint::MAX_DECODED_BYTES,
            )
            .unwrap(),
        );

        assert!(cache.get(&first_id).is_none());
        assert!(cache.get(&second_id).is_some());
    }

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
        upsert_components_page(schema, &[component])
    }

    fn upsert_components_page(schema: &str, components: &[&str]) -> Vec<u8> {
        packet_page(0, schema, |record| {
            record.extend_from_slice(&(components.len() as u32).to_le_bytes());
            for component in components {
                push_text(record, component);
            }
            record.push(0);
        })
    }

    fn accept_page(
        payload: &[u8],
        creates: WasmCreateContext,
        existing: &mut CertifiedPacketRowKeys,
    ) -> Result<(), LixError> {
        let page = validate_created_packet_page(1, payload, creates)
            .expect("well-framed packet")
            .expect("certifiable packet");
        let (_, keys) = validate_new_certified_packet_keys(page, existing)?;
        existing.extend(keys);
        Ok(())
    }

    #[test]
    fn certified_packet_rejects_duplicate_row_keys_across_pages() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let mut existing = CertifiedPacketRowKeys::default();

        accept_page(&create_page("row", 7), creates, &mut existing).expect("first page is unique");
        let duplicate_create = accept_page(&create_page("row", 7), creates, &mut existing)
            .expect_err("a later page must not repeat a create identity");
        assert_eq!(
            duplicate_create.message,
            "a component row key may occur only once across certified packet pages"
        );

        let generated_id = creates.component(7).expect("create identity");
        let explicit_collision =
            accept_page(&upsert_page("row", &generated_id), creates, &mut existing)
                .expect_err("an explicit key must not collide with an earlier create");
        assert_eq!(
            explicit_collision.message,
            "a component row key may occur only once across certified packet pages"
        );

        let mut explicit_keys = CertifiedPacketRowKeys::default();
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
            "a component row key may occur only once across certified packet pages"
        );

        let mut canonical_keys = CertifiedPacketRowKeys::default();
        accept_page(&create_page("row", 7), creates, &mut canonical_keys)
            .expect("canonical generated key");
        accept_page(
            &upsert_page("row", &generated_id.to_uppercase()),
            creates,
            &mut canonical_keys,
        )
        .expect("noncanonical UUID spelling is a distinct explicit key");

        let mut component_boundaries = CertifiedPacketRowKeys::default();
        accept_page(
            &upsert_components_page("row", &["ab", "c"]),
            creates,
            &mut component_boundaries,
        )
        .expect("first compound key");
        accept_page(
            &upsert_components_page("row", &["a", "bc"]),
            creates,
            &mut component_boundaries,
        )
        .expect("component lengths must delimit the compact fingerprint");
    }

    #[test]
    fn certified_packet_rejects_create_refs_outside_authority_format() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let result =
            validate_created_packet_page(1, &create_page("row", u64::from(u32::MAX) + 1), creates);
        assert!(matches!(
            result,
            Err(message) if message == "packet create local ref exceeds u32"
        ));
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
            &[],
            ordinary_payload.len() as u32,
            WasmTransitionLimits::default(),
        )
        .expect("ordinary packet decodes");

        let mut certified_first = CertifiedPacketRowKeys::default();
        accept_page(&create_page("row", 7), creates, &mut certified_first)
            .expect("certified create is unique");
        let duplicate_ordinary =
            validate_ordinary_packet_page_keys(&ordinary, creates, &certified_first)
                .expect_err("ordinary write must not repeat a certified identity");
        assert_eq!(
            duplicate_ordinary.message,
            "a component row key may occur only once across certified packet pages"
        );

        let mut ordinary_first = CertifiedPacketRowKeys::default();
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
            "a component row key may occur only once across certified packet pages"
        );
    }

    #[test]
    fn certified_create_authorities_are_exported_as_compact_ranges() {
        let creates = WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        };
        let mut keys = CertifiedPacketRowKeys::default();
        for local_ref in [7, 8, 9, 12] {
            accept_page(&create_page("row", local_ref), creates, &mut keys)
                .expect("create identity is unique");
        }

        assert_eq!(
            keys.take_create_ranges_for(&["row".to_owned()]),
            vec![
                WasmCertifiedCreateRange {
                    schema_key: "row".to_owned(),
                    first_local_ref: 7,
                    last_local_ref: 9,
                },
                WasmCertifiedCreateRange {
                    schema_key: "row".to_owned(),
                    first_local_ref: 12,
                    last_local_ref: 12,
                },
            ]
        );
        assert!(keys.schemas.is_empty());
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
        let user_key = WasmRow {
            key: WasmRowKey::from_owned_parts(
                "json_object_member",
                vec!["root".to_owned(), generated_id.clone()],
            ),
            snapshot_content: WasmHostBytes::Inline(Bytes::from(format!(
                r#"{{"key":"{generated_id}"}}"#
            ))),
        };
        assert_eq!(
            create_context_from_generated_row("json_array_item", &user_key),
            None
        );

        let generated_item = WasmRow {
            key: WasmRowKey::from_owned_parts("json_array_item", vec![generated_id]),
            snapshot_content: WasmHostBytes::Inline(Bytes::from_static(b"{}")),
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
