//! Host-neutral contract for the incremental Wasm Component v2 protocol.
//!
//! The Component binding lives in `packages/rs-sdk`; this module deliberately
//! contains no Wasmtime types. A compiled component factory is shared, while
//! each branch/file actor owns one isolated mutable instance and all document,
//! cursor, output-table, and transition handles created by that instance.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;

use crate::{LixError, wasm::WasmLimits};

pub const PACKET_FORMAT_V1: u16 = 1;
pub const WASM_COMPONENT_V2_API_VERSION: &str = "2.0.0";

const MIB: u64 = 1024 * 1024;

/// Aggregate limits for one top-level v2 transition, including cursor and
/// attachment draining after the exported guest call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmTransitionLimits {
    pub max_record_bytes: u32,
    pub max_page_bytes: u32,
    pub max_pages: u32,
    pub max_total_bytes: u64,
    pub max_inline_edits: u32,
    pub max_inline_input_bytes: u64,
    pub max_attachment_refs: u32,
    pub total_deadline_nanoseconds: u64,
}

impl Default for WasmTransitionLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: MIB as u32,
            max_page_bytes: MIB as u32,
            max_pages: 1_024,
            max_total_bytes: 64 * MIB,
            max_inline_edits: 4_096,
            max_inline_input_bytes: MIB,
            max_attachment_refs: 4_096,
            total_deadline_nanoseconds: 5_000_000_000,
        }
    }
}

impl WasmTransitionLimits {
    pub fn validate(self) -> Result<Self, LixError> {
        if self.max_record_bytes == 0
            || self.max_page_bytes == 0
            || self.max_pages == 0
            || self.max_total_bytes == 0
            || self.max_inline_edits == 0
            || self.max_attachment_refs == 0
            || self.total_deadline_nanoseconds == 0
        {
            return Err(invalid_param(
                "v2 transition limits must use positive record, page, count, byte, reference, and deadline bounds",
            ));
        }
        if self.max_record_bytes > self.max_page_bytes {
            return Err(invalid_param(
                "v2 max_record_bytes must not exceed max_page_bytes",
            ));
        }
        if u64::from(self.max_page_bytes) > self.max_total_bytes {
            return Err(invalid_param(
                "v2 max_page_bytes must not exceed max_total_bytes",
            ));
        }
        if self.max_inline_input_bytes > self.max_total_bytes {
            return Err(invalid_param(
                "v2 max_inline_input_bytes must not exceed max_total_bytes",
            ));
        }
        Ok(self)
    }
}

/// Measurable work for a v2 transition. Engine-owned counters and binding-
/// owned counters share one snapshot so benchmarks can fail on hidden
/// O(document) work even when wall-clock timing happens to improve.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WasmTransitionCounters {
    pub source_read_calls: u64,
    pub source_bytes_read: u64,
    pub packet_pages: u64,
    pub packet_records: u64,
    pub attachment_reads: u64,
    pub attachment_bytes_read: u64,
    pub component_import_calls: u64,
    pub component_boundary_bytes: u64,
    pub guest_linear_memory_high_water_bytes: u64,
    /// Bytes examined by the host-only full-blob diff fallback. Validated
    /// transport splice provenance keeps this zero.
    pub host_full_diff_bytes_compared: u64,
    /// Bytes examined by a host-only full-payload content-type classification.
    /// Warm, provenance-backed text edits validate only their bounded splice
    /// window and keep this zero.
    pub host_full_content_classification_bytes: u64,
    pub full_state_semantic_rows_materialized: u64,
    pub change_payload_requests: u64,
    pub returned_change_payloads: u64,
    pub durable_semantic_changes: u64,
    pub private_document_cache_hits: u64,
    pub shared_renderer_cache_hits: u64,
    pub full_document_reparses: u64,
    pub full_renderer_invocations: u64,
    pub filesystem_sync_full_renders: u64,
}

impl WasmTransitionCounters {
    /// Adds one completed transition snapshot to an engine-wide aggregate.
    ///
    /// Counters saturate instead of wrapping so diagnostic instrumentation can
    /// never report a deceptively small value after a long-running process.
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.source_read_calls = self
            .source_read_calls
            .saturating_add(other.source_read_calls);
        self.source_bytes_read = self
            .source_bytes_read
            .saturating_add(other.source_bytes_read);
        self.packet_pages = self.packet_pages.saturating_add(other.packet_pages);
        self.packet_records = self.packet_records.saturating_add(other.packet_records);
        self.attachment_reads = self.attachment_reads.saturating_add(other.attachment_reads);
        self.attachment_bytes_read = self
            .attachment_bytes_read
            .saturating_add(other.attachment_bytes_read);
        self.component_import_calls = self
            .component_import_calls
            .saturating_add(other.component_import_calls);
        self.component_boundary_bytes = self
            .component_boundary_bytes
            .saturating_add(other.component_boundary_bytes);
        self.guest_linear_memory_high_water_bytes = self
            .guest_linear_memory_high_water_bytes
            .max(other.guest_linear_memory_high_water_bytes);
        self.host_full_diff_bytes_compared = self
            .host_full_diff_bytes_compared
            .saturating_add(other.host_full_diff_bytes_compared);
        self.host_full_content_classification_bytes = self
            .host_full_content_classification_bytes
            .saturating_add(other.host_full_content_classification_bytes);
        self.full_state_semantic_rows_materialized = self
            .full_state_semantic_rows_materialized
            .saturating_add(other.full_state_semantic_rows_materialized);
        self.change_payload_requests = self
            .change_payload_requests
            .saturating_add(other.change_payload_requests);
        self.returned_change_payloads = self
            .returned_change_payloads
            .saturating_add(other.returned_change_payloads);
        self.durable_semantic_changes = self
            .durable_semantic_changes
            .saturating_add(other.durable_semantic_changes);
        self.private_document_cache_hits = self
            .private_document_cache_hits
            .saturating_add(other.private_document_cache_hits);
        self.shared_renderer_cache_hits = self
            .shared_renderer_cache_hits
            .saturating_add(other.shared_renderer_cache_hits);
        self.full_document_reparses = self
            .full_document_reparses
            .saturating_add(other.full_document_reparses);
        self.full_renderer_invocations = self
            .full_renderer_invocations
            .saturating_add(other.full_renderer_invocations);
        self.filesystem_sync_full_renders = self
            .filesystem_sync_full_renders
            .saturating_add(other.filesystem_sync_full_renders);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmPluginSelection {
    pub plugin_key: String,
    /// Content-addressed component generation selected by the engine.
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmFileDescriptor {
    pub path: Option<String>,
    pub media_type: Option<String>,
    pub plugin: WasmPluginSelection,
}

impl WasmFileDescriptor {
    pub fn validate_warm_successor(&self, after: &Self) -> Result<(), LixError> {
        if self.plugin != after.plugin {
            return Err(invalid_param(
                "warm v2 transitions require the same plugin key and generation",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmSourceRange {
    pub offset: u64,
    pub length: u64,
}

impl WasmSourceRange {
    pub fn end(self) -> Result<u64, LixError> {
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| invalid_param("v2 source range overflowed"))
    }
}

/// Immutable random-access bytes owned by the engine.
pub trait WasmByteSource: Send + Sync {
    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError>;
}

#[derive(Clone)]
pub struct WasmSourceSlice {
    pub source: Arc<dyn WasmByteSource>,
    pub range: WasmSourceRange,
}

impl fmt::Debug for WasmSourceSlice {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmSourceSlice")
            .field("source_len", &self.source.len())
            .field("range", &self.range)
            .finish()
    }
}

impl WasmSourceSlice {
    pub fn validate(&self) -> Result<(), LixError> {
        if self.range.end()? > self.source.len() {
            return Err(invalid_param("v2 source slice is out of bounds"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum WasmHostBytes {
    Inline(Vec<u8>),
    Source(WasmSourceSlice),
}

impl WasmHostBytes {
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::Source(slice) => slice.range.length,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmInputBytes {
    Inline(Vec<u8>),
    /// A range in the `after` source of the enclosing file update.
    AfterRange(WasmSourceRange),
}

impl WasmInputBytes {
    fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::AfterRange(range) => range.length,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmInputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: WasmInputBytes,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WasmEntityKey {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WasmEntity<B> {
    pub key: WasmEntityKey,
    /// Complete Snapshot JSON for this schema. The production v2 CSV slice is
    /// currently restricted to Lix's durable JSON subset; the packet-v1
    /// arbitrary-precision extension remains gated on a durable codec.
    pub snapshot_content: B,
}

pub type WasmHostEntity = WasmEntity<WasmHostBytes>;
pub type WasmGuestEntity = WasmEntity<WasmGuestBytes>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WasmChangeEffect {
    #[default]
    Content,
    FormatOnly,
}

#[derive(Debug, Clone)]
pub enum WasmEntityChange<B> {
    Upsert {
        entity: WasmEntity<B>,
        effect: WasmChangeEffect,
    },
    Delete(WasmEntityKey),
}

impl<B> WasmEntityChange<B> {
    pub fn key(&self) -> &WasmEntityKey {
        match self {
            Self::Upsert { entity, .. } => &entity.key,
            Self::Delete(key) => key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WasmMergeGroup<B> {
    pub changes: Vec<WasmEntityChange<B>>,
}

#[derive(Debug, Default, Clone)]
pub struct WasmEntityChanges<B> {
    pub groups: Vec<WasmMergeGroup<B>>,
}

impl<B> WasmEntityChanges<B> {
    pub fn validate(&self) -> Result<(), LixError> {
        let mut seen = BTreeSet::new();
        for group in &self.groups {
            if group.changes.is_empty() {
                return Err(invalid_param("v2 merge groups must not be empty"));
            }
            for change in &group.changes {
                if !seen.insert(change.key()) {
                    return Err(invalid_param(
                        "a v2 entity key may occur only once in one transition page",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn entity_change_count(&self) -> usize {
        self.groups.iter().map(|group| group.changes.len()).sum()
    }
}

pub type WasmHostEntityChanges = WasmEntityChanges<WasmHostBytes>;
pub type WasmGuestEntityChanges = WasmEntityChanges<WasmGuestBytes>;

#[derive(Debug, Clone)]
pub struct WasmEntityPage {
    pub entities: Vec<WasmHostEntity>,
}

/// Bounded, complete host entities. `None` is permanent EOF and every
/// successful page must be non-empty and no larger than `max_bytes` once
/// packet-v1 encoded.
pub trait WasmEntitySource: Send {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError>;
}

/// Bounded, merge-resolved host changes supplied to `entities_changed`.
pub trait WasmEntityChangeSource: Send {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmHostEntityChanges>, LixError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmIdNamespace {
    pub high: u64,
    pub low: u64,
}

impl WasmIdNamespace {
    /// Encodes namespace || big-endian ordinal as exactly 32 unpadded
    /// base64url characters, matching packet-v1's generated-ID rule.
    pub fn entity_pk(self, ordinal: u64) -> Vec<String> {
        vec![self.component(ordinal)]
    }

    pub fn component(self, ordinal: u64) -> String {
        const BASE64URL: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut input = [0u8; 24];
        input[..8].copy_from_slice(&self.high.to_be_bytes());
        input[8..16].copy_from_slice(&self.low.to_be_bytes());
        input[16..].copy_from_slice(&ordinal.to_be_bytes());

        let mut output = String::with_capacity(32);
        for chunk in input.chunks_exact(3) {
            let value = u32::from(chunk[0]) << 16 | u32::from(chunk[1]) << 8 | u32::from(chunk[2]);
            output.push(BASE64URL[((value >> 18) & 0x3f) as usize] as char);
            output.push(BASE64URL[((value >> 12) & 0x3f) as usize] as char);
            output.push(BASE64URL[((value >> 6) & 0x3f) as usize] as char);
            output.push(BASE64URL[(value & 0x3f) as usize] as char);
        }
        output
    }
}

pub struct WasmOpenFileInput {
    pub descriptor: WasmFileDescriptor,
    pub file: Arc<dyn WasmByteSource>,
    pub ids: WasmIdNamespace,
}

impl fmt::Debug for WasmOpenFileInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmOpenFileInput")
            .field("descriptor", &self.descriptor)
            .field("file_len", &self.file.len())
            .field("ids", &self.ids)
            .finish()
    }
}

pub struct WasmOpenEntitiesInput {
    pub descriptor: WasmFileDescriptor,
    pub entities: Box<dyn WasmEntitySource>,
}

impl fmt::Debug for WasmOpenEntitiesInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmOpenEntitiesInput")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

pub struct WasmFileUpdate {
    pub before_descriptor: WasmFileDescriptor,
    pub after_descriptor: WasmFileDescriptor,
    pub before: Arc<dyn WasmByteSource>,
    pub edits: Vec<WasmInputSplice>,
    pub after: Arc<dyn WasmByteSource>,
    pub ids: WasmIdNamespace,
}

impl fmt::Debug for WasmFileUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmFileUpdate")
            .field("before_descriptor", &self.before_descriptor)
            .field("after_descriptor", &self.after_descriptor)
            .field("before_len", &self.before.len())
            .field("edits", &self.edits)
            .field("after_len", &self.after.len())
            .field("ids", &self.ids)
            .finish()
    }
}

impl WasmFileUpdate {
    /// Validates attacker-controlled splice metadata before Canonical-ABI
    /// lowering allocates the guest-side edit list.
    pub fn validate(&self, limits: WasmTransitionLimits) -> Result<(), LixError> {
        self.before_descriptor
            .validate_warm_successor(&self.after_descriptor)?;
        limits.validate()?;
        if self.edits.len() > limits.max_inline_edits as usize {
            return Err(invalid_param("v2 input splice count exceeds its limit"));
        }

        let before_len = self.before.len();
        let after_len = self.after.len();
        let mut previous_start = None;
        let mut previous_end = 0u64;
        let mut deleted = 0u64;
        let mut inserted = 0u64;
        let mut inline = 0u64;
        for edit in &self.edits {
            let end = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or_else(|| invalid_param("v2 input splice deletion range overflowed"))?;
            if previous_start == Some(edit.offset) || edit.offset < previous_end || end > before_len
            {
                return Err(invalid_param(
                    "v2 input splices must have strictly increasing starts, be non-overlapping, and stay in the accepted base",
                ));
            }
            if let WasmInputBytes::AfterRange(range) = &edit.insert
                && range.end()? > after_len
            {
                return Err(invalid_param("v2 after-source range is out of bounds"));
            }
            if let WasmInputBytes::Inline(bytes) = &edit.insert {
                inline = inline
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid_param("v2 inline input byte count overflowed"))?;
            }
            deleted = deleted
                .checked_add(edit.delete_len)
                .ok_or_else(|| invalid_param("v2 deleted byte count overflowed"))?;
            inserted = inserted
                .checked_add(edit.insert.len())
                .ok_or_else(|| invalid_param("v2 inserted byte count overflowed"))?;
            previous_start = Some(edit.offset);
            previous_end = end;
        }
        if inline > limits.max_inline_input_bytes {
            return Err(invalid_param("v2 inline input bytes exceed their limit"));
        }
        let reconstructed_len = before_len
            .checked_sub(deleted)
            .and_then(|len| len.checked_add(inserted))
            .ok_or_else(|| invalid_param("v2 reconstructed file length overflowed"))?;
        if reconstructed_len != after_len {
            return Err(invalid_param(
                "v2 input splices do not reconstruct the declared after source length",
            ));
        }
        Ok(())
    }
}

pub struct WasmEntityUpdate {
    pub before_descriptor: WasmFileDescriptor,
    pub after_descriptor: WasmFileDescriptor,
    pub before: Arc<dyn WasmByteSource>,
    pub changes: Box<dyn WasmEntityChangeSource>,
    pub activated_entities: Box<dyn WasmEntitySource>,
    pub current_entities: Box<dyn WasmEntitySource>,
}

impl fmt::Debug for WasmEntityUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmEntityUpdate")
            .field("before_descriptor", &self.before_descriptor)
            .field("after_descriptor", &self.after_descriptor)
            .field("before_len", &self.before.len())
            .finish_non_exhaustive()
    }
}

macro_rules! handle_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    };
}

handle_type!(WasmDocumentHandle);
handle_type!(WasmChangeCursorHandle);
handle_type!(WasmEditCursorHandle);
handle_type!(WasmByteOutputsHandle);
handle_type!(WasmTransitionHandle);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmOutputRange {
    pub index: u32,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmGuestBytes {
    Inline(Vec<u8>),
    Output(WasmOutputRange),
}

#[derive(Debug, Clone)]
pub struct WasmChangePage {
    pub format_version: u16,
    pub changes: WasmGuestEntityChanges,
    /// Exactly one page-local table supplies all `Output` values in `changes`.
    pub outputs: Option<WasmByteOutputsHandle>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmOutputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: WasmGuestBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmEditPage {
    pub edits: Vec<WasmOutputSplice>,
    /// Exactly one page-local table supplies all output ranges in `edits`.
    pub outputs: Option<WasmByteOutputsHandle>,
}

/// Cross-page validator for a guest change cursor. Raw packet framing and
/// inline byte bounds are checked by the binding before it constructs a typed
/// page; this validator owns transition-wide uniqueness, page, reference, and
/// permanent-EOF invariants.
#[derive(Debug)]
pub struct WasmChangeDrainValidator {
    limits: WasmTransitionLimits,
    seen: BTreeSet<WasmEntityKey>,
    pages: u32,
    attachment_refs: u32,
    reached_eof: bool,
}

impl WasmChangeDrainValidator {
    pub fn new(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            seen: BTreeSet::new(),
            pages: 0,
            attachment_refs: 0,
            reached_eof: false,
        })
    }

    pub fn accept_page(&mut self, page: &WasmChangePage) -> Result<(), LixError> {
        if self.reached_eof {
            return Err(invalid_param("a v2 change cursor advanced after EOF"));
        }
        if page.format_version != PACKET_FORMAT_V1 {
            return Err(invalid_param("unsupported v2 change packet format version"));
        }
        if page.changes.groups.is_empty() {
            return Err(invalid_param("a v2 change page must not be empty"));
        }
        page.changes.validate()?;
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_param("v2 change page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_param("v2 change page count exceeds its limit"));
        }

        let mut page_refs = 0u32;
        for group in &page.changes.groups {
            for change in &group.changes {
                if !self.seen.insert(change.key().clone()) {
                    return Err(invalid_param(
                        "a v2 entity key may occur only once across a change cursor",
                    ));
                }
                if let WasmEntityChange::Upsert { entity, .. } = change
                    && let WasmGuestBytes::Output(range) = &entity.snapshot_content
                {
                    range
                        .offset
                        .checked_add(range.length)
                        .ok_or_else(|| invalid_param("v2 change output range overflowed"))?;
                    page_refs = page_refs
                        .checked_add(1)
                        .ok_or_else(|| invalid_param("v2 attachment reference count overflowed"))?;
                }
            }
        }
        validate_attachment_table_presence(page_refs, page.outputs.is_some())?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(page_refs)
            .ok_or_else(|| invalid_param("v2 attachment reference count overflowed"))?;
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_param(
                "v2 attachment reference count exceeds its limit",
            ));
        }
        Ok(())
    }

    pub fn accept_eof(&mut self) {
        self.reached_eof = true;
    }
}

/// Cross-page validator for renderer splices whose offsets all address the
/// same accepted base document.
#[derive(Debug)]
pub struct WasmEditDrainValidator {
    limits: WasmTransitionLimits,
    base_len: u64,
    pages: u32,
    attachment_refs: u32,
    previous_start: Option<u64>,
    previous_end: u64,
    reached_eof: bool,
}

impl WasmEditDrainValidator {
    pub fn new(base_len: u64, limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            base_len,
            pages: 0,
            attachment_refs: 0,
            previous_start: None,
            previous_end: 0,
            reached_eof: false,
        })
    }

    pub fn accept_page(&mut self, page: &WasmEditPage) -> Result<(), LixError> {
        if self.reached_eof {
            return Err(invalid_param("a v2 edit cursor advanced after EOF"));
        }
        if page.edits.is_empty() {
            return Err(invalid_param("a v2 edit page must not be empty"));
        }
        if page.edits.len() > self.limits.max_inline_edits as usize {
            return Err(invalid_param("v2 edit page count exceeds its limit"));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_param("v2 edit page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_param("v2 edit page count exceeds its limit"));
        }

        let mut page_inline_bytes = 0u64;
        let mut page_refs = 0u32;
        for edit in &page.edits {
            let end = edit
                .offset
                .checked_add(edit.delete_len)
                .ok_or_else(|| invalid_param("v2 output splice deletion range overflowed"))?;
            if self.previous_start == Some(edit.offset)
                || edit.offset < self.previous_end
                || end > self.base_len
            {
                return Err(invalid_param(
                    "v2 output splices must have globally increasing starts, be non-overlapping, and stay in the accepted base",
                ));
            }
            match &edit.insert {
                WasmGuestBytes::Inline(bytes) => {
                    page_inline_bytes = page_inline_bytes
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| invalid_param("v2 output inline bytes overflowed"))?;
                }
                WasmGuestBytes::Output(range) => {
                    range
                        .offset
                        .checked_add(range.length)
                        .ok_or_else(|| invalid_param("v2 edit output range overflowed"))?;
                    page_refs = page_refs
                        .checked_add(1)
                        .ok_or_else(|| invalid_param("v2 attachment reference count overflowed"))?;
                }
            }
            self.previous_start = Some(edit.offset);
            self.previous_end = end;
        }
        if page_inline_bytes > u64::from(self.limits.max_page_bytes) {
            return Err(invalid_param("v2 output inline bytes exceed their limit"));
        }
        validate_attachment_table_presence(page_refs, page.outputs.is_some())?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(page_refs)
            .ok_or_else(|| invalid_param("v2 attachment reference count overflowed"))?;
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_param(
                "v2 attachment reference count exceeds its limit",
            ));
        }
        Ok(())
    }

    pub fn accept_eof(&mut self) {
        self.reached_eof = true;
    }
}

fn validate_attachment_table_presence(
    reference_count: u32,
    has_table: bool,
) -> Result<(), LixError> {
    if (reference_count == 0) == has_table {
        return Err(invalid_param(
            "a v2 page must own an output table exactly when it contains output references",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmFileTransition {
    pub transition: WasmTransitionHandle,
    pub document: WasmDocumentHandle,
    pub changes: WasmChangeCursorHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmEntityTransition {
    pub transition: WasmTransitionHandle,
    pub document: WasmDocumentHandle,
    pub edits: WasmEditCursorHandle,
}

/// A compiled v2 Component. Implementations share this factory but create one
/// isolated Store/instance for every branch/file actor.
#[async_trait]
pub trait WasmComponentV2Factory: Send + Sync {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV2Actor>, LixError>;
}

/// One serial branch/file actor. Handles are instance-local and invalid after
/// `retire`; callers must retire the complete actor after traps, timeouts,
/// cancellation, or uncertain completion.
#[async_trait]
pub trait WasmComponentV2Actor: Send {
    async fn fork_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<WasmDocumentHandle, LixError>;

    async fn open_file(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<WasmFileTransition, LixError>;

    async fn open_entities(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenEntitiesInput,
    ) -> Result<WasmEntityTransition, LixError>;

    async fn file_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
    ) -> Result<WasmFileTransition, LixError>;

    async fn entities_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmEntityUpdate,
    ) -> Result<WasmEntityTransition, LixError>;

    async fn next_change_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmChangeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<WasmChangePage>, LixError>;

    async fn next_edit_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmEditCursorHandle,
        max_edits: u32,
        max_inline_bytes: u32,
    ) -> Result<Option<WasmEditPage>, LixError>;

    async fn output_len(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
    ) -> Result<u64, LixError>;

    async fn read_output(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, LixError>;

    /// Ends the aggregate budget after all output has been validated/drained.
    async fn finish_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<WasmTransitionCounters, LixError>;

    /// Deterministically rejects one prospective transition, dropping its
    /// input fork, successor document, cursors, outputs, and budget without
    /// revoking separately accepted documents. Implementations should make
    /// this idempotent so a guest-returned error that already cleaned itself
    /// can pass through host drain cleanup safely.
    async fn discard_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<(), LixError>;

    /// True once a trap, deadline, cancellation cleanup, or other uncertain
    /// failure has made every instance-local handle unusable.
    fn is_retired(&self) -> bool;

    async fn drop_document(&mut self, _document: WasmDocumentHandle) -> Result<(), LixError> {
        Ok(())
    }

    /// Revokes every instance-local handle. The default supports lightweight
    /// test runtimes; production runtimes should eagerly destroy their Store.
    async fn retire(&mut self) -> Result<(), LixError> {
        Ok(())
    }
}

pub fn validate_component_v2_limits(
    component: WasmLimits,
    transition: WasmTransitionLimits,
) -> Result<(), LixError> {
    if component.max_memory_bytes == 0 {
        return Err(invalid_param("v2 component memory limit must be positive"));
    }
    transition.validate()?;
    Ok(())
}

fn invalid_param(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MemorySource(Vec<u8>);

    impl WasmByteSource for MemorySource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }

        fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
            let start = usize::try_from(offset).map_err(|_| invalid_param("offset"))?;
            let end = start
                .checked_add(length as usize)
                .ok_or_else(|| invalid_param("range"))?;
            self.0
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| invalid_param("range"))
        }
    }

    fn descriptor(generation: &str) -> WasmFileDescriptor {
        WasmFileDescriptor {
            path: Some("data.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin: WasmPluginSelection {
                plugin_key: "plugin_csv".to_owned(),
                generation: generation.to_owned(),
            },
        }
    }

    #[test]
    fn generated_ids_match_fixed_base64url_vectors() {
        let zero = WasmIdNamespace { high: 0, low: 0 };
        let max = WasmIdNamespace {
            high: u64::MAX,
            low: u64::MAX,
        };
        assert_eq!(zero.component(0), "A".repeat(32));
        assert_eq!(max.component(u64::MAX), "_".repeat(32));
        assert_eq!(zero.entity_pk(7).len(), 1);
    }

    #[test]
    fn splice_validation_is_pre_lowering_and_base_relative() {
        let before: Arc<dyn WasmByteSource> = Arc::new(MemorySource(b"abc".to_vec()));
        let after: Arc<dyn WasmByteSource> = Arc::new(MemorySource(b"aXYZc".to_vec()));
        let update = WasmFileUpdate {
            before_descriptor: descriptor("hash-a"),
            after_descriptor: descriptor("hash-a"),
            before,
            edits: vec![WasmInputSplice {
                offset: 1,
                delete_len: 1,
                insert: WasmInputBytes::AfterRange(WasmSourceRange {
                    offset: 1,
                    length: 3,
                }),
            }],
            after,
            ids: WasmIdNamespace { high: 1, low: 2 },
        };
        update.validate(WasmTransitionLimits::default()).unwrap();

        let mut wrong_generation = update;
        wrong_generation.after_descriptor = descriptor("hash-b");
        assert!(
            wrong_generation
                .validate(WasmTransitionLimits::default())
                .is_err()
        );
    }

    #[test]
    fn rejects_duplicate_entity_keys_and_empty_groups() {
        let key = WasmEntityKey {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec!["row".to_owned()],
        };
        let duplicate = WasmEntityChanges::<WasmGuestBytes> {
            groups: vec![WasmMergeGroup {
                changes: vec![
                    WasmEntityChange::Delete(key.clone()),
                    WasmEntityChange::Delete(key),
                ],
            }],
        };
        assert!(duplicate.validate().is_err());
        let empty = WasmEntityChanges::<WasmGuestBytes> {
            groups: vec![WasmMergeGroup { changes: vec![] }],
        };
        assert!(empty.validate().is_err());
    }

    #[test]
    fn transition_limits_reject_unbounded_or_inverted_values() {
        assert!(WasmTransitionLimits::default().validate().is_ok());
        assert!(
            WasmTransitionLimits {
                max_record_bytes: 2,
                max_page_bytes: 1,
                ..WasmTransitionLimits::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            WasmTransitionLimits {
                total_deadline_nanoseconds: 0,
                ..WasmTransitionLimits::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn change_drain_validation_is_transition_wide() {
        let key = WasmEntityKey {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec!["row".to_owned()],
        };
        let page = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                groups: vec![WasmMergeGroup {
                    changes: vec![WasmEntityChange::Delete(key)],
                }],
            },
            outputs: None,
        };
        let mut validator = WasmChangeDrainValidator::new(WasmTransitionLimits::default()).unwrap();
        validator.accept_page(&page).unwrap();
        assert!(validator.accept_page(&page).is_err());
        validator.accept_eof();
        assert!(validator.accept_page(&page).is_err());
    }

    #[test]
    fn edit_drain_validation_requires_exact_page_attachment_table() {
        let range_edit = WasmOutputSplice {
            offset: 0,
            delete_len: 0,
            insert: WasmGuestBytes::Output(WasmOutputRange {
                index: 0,
                offset: 0,
                length: 10,
            }),
        };
        let missing_table = WasmEditPage {
            edits: vec![range_edit.clone()],
            outputs: None,
        };
        let mut validator =
            WasmEditDrainValidator::new(0, WasmTransitionLimits::default()).unwrap();
        assert!(validator.accept_page(&missing_table).is_err());

        let with_table = WasmEditPage {
            edits: vec![range_edit],
            outputs: Some(WasmByteOutputsHandle(1)),
        };
        let mut validator =
            WasmEditDrainValidator::new(0, WasmTransitionLimits::default()).unwrap();
        validator.accept_page(&with_table).unwrap();
    }

    #[test]
    fn production_wit_is_versioned_and_kept_out_of_the_v1_package_root() {
        let wit = include_str!("../../wit/v2/lix-plugin-v2.wit");
        assert!(wit.starts_with("package lix:plugin@2.0.0;"));
        assert!(wit.contains("resource document"));
        assert!(wit.contains("file-changed:"));
        assert!(wit.contains("entities-changed:"));
    }
}
