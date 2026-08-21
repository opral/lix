//! Host-neutral contract for the Lix plugin Wasm Component protocol.
//!
//! The Component binding lives under `lix::plugin`; this module deliberately
//! contains no Wasmtime types. A compiled component factory is shared, while
//! each branch/file actor owns one isolated mutable instance and all document,
//! cursor, output-table, and transition handles created by that instance.

use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use crate::{LixError, common::SharedStr, row_pk::RowPk, wasm::WasmLimits};
use async_trait::async_trait;
use bytes::Bytes;

pub const PACKET_FORMAT_V2: u16 = 2;
pub const CURRENT_PACKET_FORMAT: u16 = PACKET_FORMAT_V2;
pub const WASM_COMPONENT_API_VERSION: &str = "2.0.0";
/// Canonical ABI page charge for one renderer splice before inline insert
/// bytes. Both inline and output-backed edits pay this fixed metadata cost.
pub const EDIT_SPLICE_METADATA_BYTES: u64 = 24;

const MIB: u64 = 1024 * 1024;
const MIB_U32: u32 = 1024 * 1024;
const TRANSITION_PAGE_BYTES: u32 = 2 * MIB_U32;
const COLD_TRANSITION_MAX_PAGE_BYTES: u64 = 16 * MIB;
const COLD_TRANSITION_RECORD_OVERHEAD_BYTES: u64 = 64 * 1024;
const COLD_FILE_MAX_DEADLINE_NANOSECONDS: u64 = 60_000_000_000;
const COLD_FILE_EXTRA_DEADLINE_NANOSECONDS_PER_MIB: u64 = 1_000_000_000;
pub(crate) const COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION: u8 = 4;
// Zstd's fixed per-frame workspace dominates the small engine rows that make
// up SQL state. Keep those rows in the compact typed encoding directly; only
// pay the compression/decompression cost for payloads large enough to amortize
// it.
const ENGINE_ROW_COMPRESSION_THRESHOLD: usize = 4 * 1024;
const ENGINE_ROW_PAYLOAD_MAX_BYTES: usize = 128 * 1024 * 1024;

/// Aggregate limits for one top-level component transition, including cursor and
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
            max_record_bytes: MIB_U32,
            // Match the largest fixed page schedule that the v2 typed-page sink
            // safely uses under the same Wasm linear-memory limit. Records
            // remain capped independently at one MiB.
            max_page_bytes: TRANSITION_PAGE_BYTES,
            max_pages: 1_024,
            // A 10 MiB recursive JSON import can legitimately carry roughly
            // one compact row per leaf plus packet keys. Keep the
            // transition bounded, but do not confuse aggregate paging traffic
            // with the component's independent linear-memory ceiling.
            max_total_bytes: 128 * MIB,
            max_inline_edits: 4_096,
            max_inline_input_bytes: MIB,
            max_attachment_refs: 4_096,
            total_deadline_nanoseconds: 5_000_000_000,
        }
    }
}

impl WasmTransitionLimits {
    /// Returns the normal transition budget with enough bounded page space for
    /// one semantic record derived from the supplied file bytes.
    pub fn for_file_bytes(file_bytes: u64) -> Self {
        let mut limits = Self::default();
        limits.scale_page_for_file_bytes(file_bytes);
        limits
    }

    /// Returns a bounded budget for a first cold parse of one submitted file.
    ///
    /// A fresh `open-file` must legitimately examine all input bytes, unlike a
    /// warm sparse edit or column merge. Keep the normal five
    /// second bound for a zero/small file, add one second per started MiB, and
    /// cap the result at one minute. This is admission policy, not an excuse
    /// for a plugin to make the hot path proportional to file size.
    pub fn for_cold_file_bytes(file_bytes: u64) -> Self {
        let mut limits = Self::default();
        limits.scale_page_for_file_bytes(file_bytes);
        let extra = file_bytes
            .div_ceil(MIB)
            .saturating_mul(COLD_FILE_EXTRA_DEADLINE_NANOSECONDS_PER_MIB);
        limits.total_deadline_nanoseconds = limits
            .total_deadline_nanoseconds
            .saturating_add(extra)
            .min(COLD_FILE_MAX_DEADLINE_NANOSECONDS);
        limits
    }

    fn scale_page_for_file_bytes(&mut self, file_bytes: u64) {
        // Text rows encode their byte-exact content as unpadded base64.
        // A generated/source-map file may legitimately be one long line, so
        // its single semantic record can be larger than the normal 2 MiB
        // scheduling page. Retain a hard 16 MiB page bound; small files and
        // ordinary sparse transitions keep the fixed 2 MiB schedule.
        let encoded_record_bytes = file_bytes
            .saturating_mul(4)
            .div_ceil(3)
            .saturating_add(COLD_TRANSITION_RECORD_OVERHEAD_BYTES);
        let cold_page_bytes = encoded_record_bytes
            .max(u64::from(TRANSITION_PAGE_BYTES))
            .min(COLD_TRANSITION_MAX_PAGE_BYTES)
            .min(self.max_total_bytes);
        self.max_page_bytes = u32::try_from(cold_page_bytes).unwrap_or(u32::MAX);
        self.max_record_bytes = self.max_page_bytes;
    }

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
                "component transition limits must use positive record, page, count, byte, reference, and deadline bounds",
            ));
        }
        if self.max_record_bytes > self.max_page_bytes {
            return Err(invalid_param(
                "component max_record_bytes must not exceed max_page_bytes",
            ));
        }
        if u64::from(self.max_page_bytes) > self.max_total_bytes {
            return Err(invalid_param(
                "component max_page_bytes must not exceed max_total_bytes",
            ));
        }
        if self.max_inline_input_bytes > self.max_total_bytes {
            return Err(invalid_param(
                "component max_inline_input_bytes must not exceed max_total_bytes",
            ));
        }
        Ok(self)
    }
}

/// Forbidden outer-row JSON work at the plugin boundary.
/// One canonical JSON row backed by cursor-page storage.
/// The typed-row hard cut has no production caller. This enum and
/// [`WasmTransitionCounters::record_outer_row_json_operation`] are the single
/// instrumentation choke point for any future implementation: recording is
/// required before an outer snapshot is parsed, serialized, canonicalized, or
/// materialized as a JSON DOM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum OuterRowJsonOperation {
    Parse,
    Serialize,
    Canonicalize,
    DomFallback,
}

/// Measurable work for a component transition. Engine-owned counters and binding-
/// owned counters share one snapshot so benchmarks can fail on hidden
/// O(document) work even when wall-clock timing happens to improve.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WasmTransitionCounters {
    pub source_read_calls: u64,
    pub source_bytes_read: u64,
    /// File-byte reads through the immutable snapshot capability.
    pub file_read_calls: u64,
    pub file_bytes_read: u64,
    /// Opaque plugin-state reads through the immutable snapshot capability.
    pub state_read_calls: u64,
    pub state_key_bytes: u64,
    pub state_value_bytes_read: u64,
    pub packet_pages: u64,
    pub packet_records: u64,
    /// Universal row pages sent from host to guest.
    pub row_input_pages: u64,
    pub row_input_records: u64,
    pub row_input_wire_bytes: u64,
    /// Universal row pages sent from guest to host.
    pub row_output_pages: u64,
    pub row_output_records: u64,
    pub row_output_wire_bytes: u64,
    pub attachment_reads: u64,
    pub attachment_bytes_read: u64,
    pub row_input_attachment_reads: u64,
    pub row_input_attachment_bytes: u64,
    pub row_output_attachment_writes: u64,
    pub row_output_attachment_bytes: u64,
    /// Typed row transport work at the host/guest boundary.
    pub typed_row_decode_records: u64,
    pub typed_row_decode_bytes: u64,
    /// Host CPU time spent decoding native typed pages, measured per callback.
    pub typed_row_decode_nanos: u64,
    pub typed_row_encode_records: u64,
    pub typed_row_encode_bytes: u64,
    pub typed_row_schema_validation_calls: u64,
    pub typed_row_schema_validation_bytes: u64,
    /// Host CPU time spent authenticating decoded rows against Schema v1.
    pub typed_row_schema_validation_nanos: u64,
    /// Complete native rows observed by transaction preparation after either
    /// the component or SQL construction boundary. Already-certified rows are
    /// counted as typed-path evidence without being validated a second time.
    pub typed_transaction_validation_calls: u64,
    pub typed_transaction_validation_bytes: u64,
    pub row_page_callback_calls: u64,
    /// Input-source callbacks that returned the terminal `none` page. Together
    /// with input/output page counts this exactly accounts for every callback.
    pub row_input_page_eof_callbacks: u64,
    /// Hard-cut proof counters. Production typed-row workloads must leave all
    /// of these at zero; a positive-control test proves every counter can
    /// become nonzero through the centralized operation hook.
    pub outer_row_json_parse_calls: u64,
    pub outer_row_json_parse_bytes: u64,
    pub outer_row_json_serialize_calls: u64,
    pub outer_row_json_serialize_bytes: u64,
    pub outer_row_json_canonicalize_calls: u64,
    pub outer_row_json_canonicalize_bytes: u64,
    pub outer_row_json_dom_fallback_calls: u64,
    pub outer_row_json_dom_fallback_bytes: u64,
    pub component_import_calls: u64,
    /// Exported guest entries. Prototype A requires exactly one for an initial
    /// file transition, independent of page and source-read counts.
    pub guest_export_calls: u64,
    /// Persistent executor threads created for the actor while serving this
    /// transition. This is one on the actor's first transition and zero for
    /// subsequent transitions; it must never scale with source/sink calls.
    pub actor_executor_threads_created: u64,
    pub component_boundary_bytes: u64,
    pub guest_linear_memory_high_water_bytes: u64,
    /// Bytes examined by the host-only full-blob diff fallback. Validated
    /// transport splice provenance keeps this zero.
    pub host_full_diff_bytes_compared: u64,
    /// Bytes examined by host-only content-type classification. A matcher may
    /// deliberately use a bounded predicate (for example the text plugin.s 8 KiB NUL
    /// window), while a UTF-8 parser examines the complete payload. Warm,
    /// provenance-backed edits keep this zero when their invariant proof is
    /// sufficient.
    pub host_content_classification_bytes: u64,
    pub full_state_semantic_rows_materialized: u64,
    pub change_payload_requests: u64,
    pub returned_change_payloads: u64,
    pub durable_semantic_changes: u64,
    pub private_document_cache_hits: u64,
    pub shared_renderer_cache_hits: u64,
    pub full_document_reparses: u64,
    pub full_renderer_invocations: u64,
    pub filesystem_sync_full_renders: u64,
    /// Static conflict-resolution guest calls. A merge batches all colliding
    /// rows for one file/plugin generation into one call, so this should
    /// remain O(files), not O(conflicts).
    pub conflict_resolution_calls: u64,
    /// Conflict triples delivered to a plugin resolver.
    pub conflict_resolution_records: u64,
    /// Resolutions that selected an immutable input version without returning
    /// a replacement snapshot through guest linear memory.
    pub conflict_resolution_takes: u64,
}

impl WasmTransitionCounters {
    #[doc(hidden)]
    pub fn record_outer_row_json_operation(
        &mut self,
        operation: OuterRowJsonOperation,
        bytes: u64,
    ) {
        let (calls, measured_bytes) = match operation {
            OuterRowJsonOperation::Parse => (
                &mut self.outer_row_json_parse_calls,
                &mut self.outer_row_json_parse_bytes,
            ),
            OuterRowJsonOperation::Serialize => (
                &mut self.outer_row_json_serialize_calls,
                &mut self.outer_row_json_serialize_bytes,
            ),
            OuterRowJsonOperation::Canonicalize => (
                &mut self.outer_row_json_canonicalize_calls,
                &mut self.outer_row_json_canonicalize_bytes,
            ),
            OuterRowJsonOperation::DomFallback => (
                &mut self.outer_row_json_dom_fallback_calls,
                &mut self.outer_row_json_dom_fallback_bytes,
            ),
        };
        *calls = calls.saturating_add(1);
        *measured_bytes = measured_bytes.saturating_add(bytes);
    }

    /// Adds one completed transition snapshot to an engine-wide aggregate.
    ///
    /// Counters saturate instead of wrapping so diagnostic instrumentation can
    /// never report a deceptively small value after a long-running process.
    pub fn accumulate(&mut self, other: Self) {
        self.source_read_calls = self
            .source_read_calls
            .saturating_add(other.source_read_calls);
        self.source_bytes_read = self
            .source_bytes_read
            .saturating_add(other.source_bytes_read);
        self.file_read_calls = self.file_read_calls.saturating_add(other.file_read_calls);
        self.file_bytes_read = self.file_bytes_read.saturating_add(other.file_bytes_read);
        self.state_read_calls = self.state_read_calls.saturating_add(other.state_read_calls);
        self.state_key_bytes = self.state_key_bytes.saturating_add(other.state_key_bytes);
        self.state_value_bytes_read = self
            .state_value_bytes_read
            .saturating_add(other.state_value_bytes_read);
        self.packet_pages = self.packet_pages.saturating_add(other.packet_pages);
        self.packet_records = self.packet_records.saturating_add(other.packet_records);
        self.row_input_pages = self.row_input_pages.saturating_add(other.row_input_pages);
        self.row_input_records = self
            .row_input_records
            .saturating_add(other.row_input_records);
        self.row_input_wire_bytes = self
            .row_input_wire_bytes
            .saturating_add(other.row_input_wire_bytes);
        self.row_output_pages = self.row_output_pages.saturating_add(other.row_output_pages);
        self.row_output_records = self
            .row_output_records
            .saturating_add(other.row_output_records);
        self.row_output_wire_bytes = self
            .row_output_wire_bytes
            .saturating_add(other.row_output_wire_bytes);
        self.attachment_reads = self.attachment_reads.saturating_add(other.attachment_reads);
        self.attachment_bytes_read = self
            .attachment_bytes_read
            .saturating_add(other.attachment_bytes_read);
        self.row_input_attachment_reads = self
            .row_input_attachment_reads
            .saturating_add(other.row_input_attachment_reads);
        self.row_input_attachment_bytes = self
            .row_input_attachment_bytes
            .saturating_add(other.row_input_attachment_bytes);
        self.row_output_attachment_writes = self
            .row_output_attachment_writes
            .saturating_add(other.row_output_attachment_writes);
        self.row_output_attachment_bytes = self
            .row_output_attachment_bytes
            .saturating_add(other.row_output_attachment_bytes);
        self.typed_row_decode_records = self
            .typed_row_decode_records
            .saturating_add(other.typed_row_decode_records);
        self.typed_row_decode_bytes = self
            .typed_row_decode_bytes
            .saturating_add(other.typed_row_decode_bytes);
        self.typed_row_decode_nanos = self
            .typed_row_decode_nanos
            .saturating_add(other.typed_row_decode_nanos);
        self.typed_row_encode_records = self
            .typed_row_encode_records
            .saturating_add(other.typed_row_encode_records);
        self.typed_row_encode_bytes = self
            .typed_row_encode_bytes
            .saturating_add(other.typed_row_encode_bytes);
        self.typed_row_schema_validation_calls = self
            .typed_row_schema_validation_calls
            .saturating_add(other.typed_row_schema_validation_calls);
        self.typed_row_schema_validation_bytes = self
            .typed_row_schema_validation_bytes
            .saturating_add(other.typed_row_schema_validation_bytes);
        self.typed_row_schema_validation_nanos = self
            .typed_row_schema_validation_nanos
            .saturating_add(other.typed_row_schema_validation_nanos);
        self.typed_transaction_validation_calls = self
            .typed_transaction_validation_calls
            .saturating_add(other.typed_transaction_validation_calls);
        self.typed_transaction_validation_bytes = self
            .typed_transaction_validation_bytes
            .saturating_add(other.typed_transaction_validation_bytes);
        self.row_page_callback_calls = self
            .row_page_callback_calls
            .saturating_add(other.row_page_callback_calls);
        self.row_input_page_eof_callbacks = self
            .row_input_page_eof_callbacks
            .saturating_add(other.row_input_page_eof_callbacks);
        self.outer_row_json_parse_calls = self
            .outer_row_json_parse_calls
            .saturating_add(other.outer_row_json_parse_calls);
        self.outer_row_json_parse_bytes = self
            .outer_row_json_parse_bytes
            .saturating_add(other.outer_row_json_parse_bytes);
        self.outer_row_json_serialize_calls = self
            .outer_row_json_serialize_calls
            .saturating_add(other.outer_row_json_serialize_calls);
        self.outer_row_json_serialize_bytes = self
            .outer_row_json_serialize_bytes
            .saturating_add(other.outer_row_json_serialize_bytes);
        self.outer_row_json_canonicalize_calls = self
            .outer_row_json_canonicalize_calls
            .saturating_add(other.outer_row_json_canonicalize_calls);
        self.outer_row_json_canonicalize_bytes = self
            .outer_row_json_canonicalize_bytes
            .saturating_add(other.outer_row_json_canonicalize_bytes);
        self.outer_row_json_dom_fallback_calls = self
            .outer_row_json_dom_fallback_calls
            .saturating_add(other.outer_row_json_dom_fallback_calls);
        self.outer_row_json_dom_fallback_bytes = self
            .outer_row_json_dom_fallback_bytes
            .saturating_add(other.outer_row_json_dom_fallback_bytes);
        self.component_import_calls = self
            .component_import_calls
            .saturating_add(other.component_import_calls);
        self.guest_export_calls = self
            .guest_export_calls
            .saturating_add(other.guest_export_calls);
        self.actor_executor_threads_created = self
            .actor_executor_threads_created
            .saturating_add(other.actor_executor_threads_created);
        self.component_boundary_bytes = self
            .component_boundary_bytes
            .saturating_add(other.component_boundary_bytes);
        self.guest_linear_memory_high_water_bytes = self
            .guest_linear_memory_high_water_bytes
            .max(other.guest_linear_memory_high_water_bytes);
        self.host_full_diff_bytes_compared = self
            .host_full_diff_bytes_compared
            .saturating_add(other.host_full_diff_bytes_compared);
        self.host_content_classification_bytes = self
            .host_content_classification_bytes
            .saturating_add(other.host_content_classification_bytes);
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
        self.conflict_resolution_calls = self
            .conflict_resolution_calls
            .saturating_add(other.conflict_resolution_calls);
        self.conflict_resolution_records = self
            .conflict_resolution_records
            .saturating_add(other.conflict_resolution_records);
        self.conflict_resolution_takes = self
            .conflict_resolution_takes
            .saturating_add(other.conflict_resolution_takes);
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
    pub file_id: String,
    pub path: Option<String>,
    pub plugin: WasmPluginSelection,
}

impl WasmFileDescriptor {
    pub fn validate_warm_successor(&self, after: &Self) -> Result<(), LixError> {
        if self.file_id != after.file_id {
            return Err(invalid_param(
                "warm component transitions require the same stable file id",
            ));
        }
        if self.plugin != after.plugin {
            return Err(invalid_param(
                "warm component transitions require the same plugin key and generation",
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
            .ok_or_else(|| invalid_param("component source range overflowed"))
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

#[derive(Debug, Clone)]
pub enum WasmHostBytes {
    /// Native Schema v1 values retained after typed-row page validation. This
    /// is the only row payload accepted by the plugin runtime.
    Typed(Arc<WasmTypedRow>),
}

#[derive(Debug)]
pub struct WasmTypedRow {
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) row_pk: Arc<[lix_schema::Value]>,
    pub(crate) row: lix_schema::Row,
    pub(crate) native_payload: OnceLock<NativePayloadCache>,
    pub(crate) boundary_create_validation: OnceLock<BoundaryValidationToken>,
}

impl Clone for WasmTypedRow {
    fn clone(&self) -> Self {
        // Boundary certificates authorize this exact owner and must never be
        // copied onto a value that `Arc::make_mut` is about to modify. Encoded
        // bytes are cleared for the same reason: the clone may be mutated and
        // must not retain a payload for its predecessor row.
        Self {
            schema_fingerprint: self.schema_fingerprint,
            row_pk: self.row_pk.clone(),
            row: self.row.clone(),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum NativePayloadCache {
    /// Bytes reconstructed from durable state or encoded by an internal
    /// caller. Their presence alone carries no ingress-validation authority.
    Durable {
        bytes: Arc<[u8]>,
        boundary_validation: OnceLock<BoundaryValidationToken>,
    },
    /// Bytes encoded only after the catalog-backed component boundary proved
    /// the complete row shape and its typed identity.
    BoundaryValidated {
        bytes: Arc<[u8]>,
        _proof: BoundaryValidationToken,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct BoundaryValidationToken {
    _private: (),
}

impl NativePayloadCache {
    fn bytes(&self) -> &Arc<[u8]> {
        match self {
            Self::Durable { bytes, .. } | Self::BoundaryValidated { bytes, .. } => bytes,
        }
    }
}

impl PartialEq for WasmTypedRow {
    fn eq(&self, other: &Self) -> bool {
        self.schema_fingerprint == other.schema_fingerprint
            && self.row_pk == other.row_pk
            && self.row == other.row
    }
}

fn json_ingress_error(schema_key: &str, error: lix_schema::Error) -> LixError {
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("snapshot_content conversion failed for schema '{schema_key}': {error}"),
    )
}

impl WasmTypedRow {
    /// Encodes the exact two-column row shape certified by the SQL
    /// `path`/`value` replacement fast path without first materializing and
    /// reparsing an outer JSON object.
    pub(crate) fn append_certified_path_value_payload(
        output: &mut Vec<u8>,
        plan: &crate::catalog::SchemaPlan,
        path: &str,
        value: serde_json::Value,
    ) -> Result<(), LixError> {
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(&plan.key.schema_key)
            .is_some_and(|(_, builtin)| builtin.fingerprint() == plan.fingerprint());
        let value = lix_schema::Jsonb::from(value);
        if !engine_compact {
            return crate::plugin::wire::typed::append_native_path_value_payload(
                output,
                &plan.fingerprint().bytes(),
                path,
                &value,
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "typed row for schema '{}' is not durably encodable: {error:?}",
                        plan.key.schema_key
                    ),
                )
            });
        }
        let row = lix_schema::Row::from([
            ("path", lix_schema::Value::Text(path.to_owned())),
            ("value", lix_schema::Value::Jsonb(value)),
        ]);
        plan.compiled_schema
            .validate_complete_row(&row)
            .map_err(|error| json_ingress_error(&plan.key.schema_key, error))?;
        let payload =
            crate::plugin::wire::typed::encode_engine_row_payload(&plan.compiled_schema, &row)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "typed row for schema '{}' is not durably encodable: {error:?}",
                            plan.key.schema_key
                        ),
                    )
                })?;
        let payload = compress_durable_payload(payload).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot compress typed row for schema '{}': {error:?}",
                    plan.key.schema_key
                ),
            )
        })?;
        output.extend_from_slice(&payload);
        Ok(())
    }

    /// Canonical-text counterpart used by bound SQL batches. Plugin-owned
    /// rows can retain the validated parameter bytes directly; built-in
    /// compact rows keep the general schema-validation fallback.
    pub(crate) fn try_append_certified_path_value_payload_from_canonical_json(
        output: &mut Vec<u8>,
        plan: &crate::catalog::SchemaPlan,
        path: &str,
        canonical_json: &[u8],
    ) -> Result<bool, LixError> {
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(&plan.key.schema_key)
            .is_some_and(|(_, builtin)| builtin.fingerprint() == plan.fingerprint());
        if engine_compact {
            if lix_schema::validate_canonical_json_text(canonical_json).is_err() {
                return Ok(false);
            }
            let value = serde_json::from_slice(canonical_json).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("canonical JSON parameter could not be decoded: {error}"),
                )
            })?;
            Self::append_certified_path_value_payload(output, plan, path, value)?;
            return Ok(true);
        }
        crate::plugin::wire::typed::try_append_native_path_value_payload_from_canonical_json(
            output,
            &plan.fingerprint().bytes(),
            path,
            canonical_json,
        )
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' is not durably encodable: {error:?}",
                    plan.key.schema_key
                ),
            )
        })
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn from_test_json_unchecked(
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let object = value.as_object().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "test row must be a JSON object",
            )
        })?;
        let row_pk = stored_row_pk
            .components
            .iter()
            .map(|component| match component {
                crate::row_pk::RowPkComponent::Uuid(value) => {
                    lix_schema::Value::Uuid(uuid::Uuid::from_bytes(*value))
                }
                crate::row_pk::RowPkComponent::Integer(value) => lix_schema::Value::Int8(*value),
                crate::row_pk::RowPkComponent::String(value) => {
                    lix_schema::Value::Text(value.as_str().to_owned())
                }
                crate::row_pk::RowPkComponent::Bytes(value) => {
                    lix_schema::Value::Text(String::from_utf8_lossy(value).into_owned())
                }
            })
            .collect::<Vec<_>>();
        let row = object
            .iter()
            .map(|(name, value)| {
                let value = match value {
                    serde_json::Value::Null => lix_schema::Value::Null,
                    serde_json::Value::Bool(value) => lix_schema::Value::Boolean(*value),
                    serde_json::Value::Number(value) if value.is_i64() => {
                        lix_schema::Value::Int8(value.as_i64().expect("checked integer"))
                    }
                    serde_json::Value::Number(value) => lix_schema::Value::Float8(
                        value.as_f64().expect("JSON number converts to f64"),
                    ),
                    serde_json::Value::String(value) => lix_schema::Value::Text(value.clone()),
                    value => lix_schema::Value::Jsonb(value.clone().into()),
                };
                (name.clone(), value)
            })
            .collect();
        Ok(Self {
            schema_fingerprint: [0; 32],
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        })
    }

    /// Materializes the public JSON row shape only for legacy/API consumers
    /// that explicitly project `snapshot_content`. Durable state never calls
    /// this conversion.
    pub(crate) fn to_json_value(&self) -> Result<serde_json::Value, LixError> {
        let mut object = serde_json::Map::with_capacity(self.row.len());
        for (name, value) in &self.row {
            let value = match value {
                lix_schema::Value::Null => serde_json::Value::Null,
                lix_schema::Value::Text(value) => serde_json::Value::String(value.clone()),
                lix_schema::Value::Uuid(value) => serde_json::Value::String(value.to_string()),
                lix_schema::Value::Int8(value) => (*value).into(),
                lix_schema::Value::Float8(value) => serde_json::Number::from_f64(*value)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "typed row contains a non-finite float",
                        )
                    })?,
                lix_schema::Value::Boolean(value) => (*value).into(),
                lix_schema::Value::Jsonb(value) => value.as_value().clone(),
                lix_schema::Value::Timestamptz(value) => {
                    let timestamp =
                        chrono::DateTime::from_timestamp_micros(*value).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "typed row contains an out-of-range timestamp",
                            )
                        })?;
                    let format = if value.rem_euclid(1_000) == 0 {
                        chrono::SecondsFormat::Millis
                    } else {
                        chrono::SecondsFormat::Micros
                    };
                    serde_json::Value::String(timestamp.to_rfc3339_opts(format, true))
                }
            };
            object.insert(name.to_owned(), value);
        }
        Ok(serde_json::Value::Object(object))
    }

    pub(crate) fn to_json_shared(&self) -> Result<SharedStr, LixError> {
        serde_json::to_string(&self.to_json_value()?)
            .map(Into::into)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("cannot materialize typed row JSON projection: {error}"),
                )
            })
    }

    /// Builds an engine-owned row from a schema embedded in the binary. This
    /// is available before the persisted schema catalog can be hydrated.
    pub(crate) fn from_builtin_json(
        schema_key: &str,
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(schema_key)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("embedded schema catalog is missing '{schema_key}'"),
                )
            })?;
        let row = plan
            .compiled_schema
            .row_from_json(value)
            .map_err(|error| json_ingress_error(schema_key, error))?;
        Self::from_compiled_row(
            schema_key,
            &plan.compiled_schema,
            plan.fingerprint().bytes(),
            stored_row_pk,
            row,
            true,
        )
    }

    /// Converts an engine-owned canonical JSON row with a resolved Schema v1
    /// plan into the sole durable row representation.
    pub(crate) fn from_normalized_json(
        plan: &crate::catalog::SchemaPlan,
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        Self::from_compiled_normalized_json(
            &plan.key.schema_key,
            &plan.compiled_schema,
            plan.fingerprint().bytes(),
            stored_row_pk,
            value,
        )
    }

    pub(crate) fn from_compiled_normalized_json(
        schema_key: &str,
        compiled_schema: &lix_schema::CompiledSchema,
        schema_fingerprint: [u8; 32],
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let row = compiled_schema
            .row_from_json(value)
            .map_err(|error| json_ingress_error(schema_key, error))?;
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(schema_key)
            .is_some_and(|(_, plan)| plan.fingerprint().bytes() == schema_fingerprint);
        Self::from_compiled_row(
            schema_key,
            compiled_schema,
            schema_fingerprint,
            stored_row_pk,
            row,
            engine_compact,
        )
    }

    fn from_compiled_row(
        schema_key: &str,
        compiled_schema: &lix_schema::CompiledSchema,
        schema_fingerprint: [u8; 32],
        stored_row_pk: &RowPk,
        row: lix_schema::Row,
        engine_compact: bool,
    ) -> Result<Self, LixError> {
        let row_pk = compiled_schema
            .primary_key()
            .iter()
            .map(|column| {
                row.get(column).cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "typed row for schema '{}' is missing primary-key column '{column}'",
                            schema_key
                        ),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let durable_row_pk = RowPk::from_schema_values(&row_pk).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' has an invalid primary key: {error}",
                    schema_key
                ),
            )
        })?;
        if &durable_row_pk != stored_row_pk {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "typed row for schema '{}' changed identity during JSON ingress conversion",
                    schema_key
                ),
            ));
        }
        let payload = (if engine_compact {
            crate::plugin::wire::typed::encode_engine_row_payload(compiled_schema, &row)
        } else {
            crate::plugin::wire::typed::encode_native_row_payload(
                &schema_fingerprint,
                &row_pk,
                &row,
            )
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' is not durably encodable: {error:?}",
                    schema_key
                ),
            )
        })?;
        let payload: Arc<[u8]> = if engine_compact {
            compress_durable_payload(payload).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("cannot compress typed row for schema '{schema_key}': {error:?}"),
                )
            })?
        } else {
            payload
        }
        .into();
        let typed = Self {
            schema_fingerprint,
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::from(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            }),
            boundary_create_validation: OnceLock::new(),
        };
        Ok(typed)
    }

    pub(crate) fn validate_durable_envelope(
        &self,
        stored_schema_key: &str,
        stored_row_pk: &RowPk,
    ) -> Result<(), LixError> {
        let payload_row_pk = RowPk::from_schema_values(&self.row_pk).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload for schema '{stored_schema_key}' has an invalid row identity: {error}"
                ),
            )
        })?;
        if &payload_row_pk != stored_row_pk {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload identity does not match the stored envelope for schema '{stored_schema_key}'"
                ),
            ));
        }
        Ok(())
    }

    /// Decodes a durable typed row and binds its embedded identity to the
    /// authoritative storage envelope before the row can be materialized.
    pub(crate) fn decode_durable_payload(
        payload: Arc<[u8]>,
        stored_schema_key: &str,
        stored_row_pk: &RowPk,
    ) -> Result<Self, LixError> {
        let decoded_engine_payload =
            if payload.first().copied() == Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION) {
                Some(decompress_engine_row_payload(&payload)?)
            } else {
                None
            };
        let engine_payload = decoded_engine_payload.as_deref().unwrap_or(&payload);
        let (schema_fingerprint, row_pk, row) = if engine_payload.first().copied()
            == Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
        {
            let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
                .plan_for_key(stored_schema_key)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "compact engine typed payload references non-built-in schema '{stored_schema_key}'"
                        ),
                    )
                })?;
            let row = crate::plugin::wire::typed::decode_engine_row_payload(
                engine_payload,
                &plan.compiled_schema,
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot decode durable typed payload for schema '{stored_schema_key}': {error:?}"
                    ),
                )
            })?;
            let row_pk = plan
                .compiled_schema
                .primary_key()
                .iter()
                .map(|column| {
                    row.get(column).cloned().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "compact engine typed row for schema '{stored_schema_key}' is missing primary-key column '{column}'"
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            (plan.fingerprint().bytes(), row_pk, row)
        } else {
            let decoded = crate::plugin::wire::typed::decode_native_row_payload(engine_payload)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "cannot decode durable typed payload for schema '{stored_schema_key}': {error:?}"
                        ),
                    )
                })?;
            let row_pk = if decoded.row_pk.is_empty()
                && engine_payload.first().copied()
                    == Some(crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION)
            {
                stored_row_pk
                    .components
                    .iter()
                    .map(|component| match component {
                        crate::row_pk::RowPkComponent::String(value) => {
                            Ok(lix_schema::Value::Text(value.as_str().to_owned()))
                        }
                        crate::row_pk::RowPkComponent::Uuid(value) => {
                            Ok(lix_schema::Value::Uuid(uuid::Uuid::from_bytes(*value)))
                        }
                        crate::row_pk::RowPkComponent::Integer(value) => {
                            Ok(lix_schema::Value::Int8(*value))
                        }
                        crate::row_pk::RowPkComponent::Bytes(_) => Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "Schema v1 storage payload has a non-schema primary-key component",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                decoded.row_pk
            };
            (decoded.schema_fingerprint, row_pk, decoded.row)
        };
        let row = Self {
            schema_fingerprint,
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::from(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            }),
            boundary_create_validation: OnceLock::new(),
        };
        row.validate_durable_envelope(stored_schema_key, stored_row_pk)?;
        Ok(row)
    }

    /// Binds a decoded durable row's storage envelope to the schema selected
    /// by its consumer. This must run before typed values are filtered,
    /// projected, or otherwise exposed.
    pub(crate) fn validate_resolved_schema_binding(
        &self,
        stored_schema_key: &str,
        resolved_schema_key: &str,
        resolved_schema_fingerprint: &[u8; 32],
    ) -> Result<(), LixError> {
        if stored_schema_key != resolved_schema_key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed row stored as schema '{stored_schema_key}' cannot be exposed as resolved schema '{resolved_schema_key}'"
                ),
            ));
        }
        if &self.schema_fingerprint != resolved_schema_fingerprint {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "durable typed row fingerprint for schema '{stored_schema_key}' does not match the resolved schema"
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn invalidate_durable_payload(&mut self) {
        self.native_payload.take();
    }

    pub(crate) fn durable_payload(&self) -> Result<Arc<[u8]>, crate::plugin::wire::typed::Error> {
        if let Some(payload) = self.native_payload.get() {
            return Ok(Arc::clone(payload.bytes()));
        }
        let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
            &self.schema_fingerprint,
            &self.row_pk,
            &self.row,
        )?
        .into();
        let _ = self.native_payload.set(NativePayloadCache::Durable {
            bytes: Arc::clone(&payload),
            boundary_validation: OnceLock::new(),
        });
        Ok(self
            .native_payload
            .get()
            .map_or(payload, |cached| Arc::clone(cached.bytes())))
    }

    /// Returns the canonical durable payload without cloning its cached owner.
    ///
    /// Terminal storage carriers borrow this slice from the typed row so their
    /// physical snapshot has one representation regardless of whether the
    /// cache was populated by decoding or by first-time encoding.
    pub(crate) fn durable_payload_ref(&self) -> Result<&[u8], crate::plugin::wire::typed::Error> {
        if self.native_payload.get().is_none() {
            let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
                &self.schema_fingerprint,
                &self.row_pk,
                &self.row,
            )?
            .into();
            let _ = self.native_payload.set(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            });
        }
        Ok(self
            .native_payload
            .get()
            .expect("durable payload cache was initialized")
            .bytes()
            .as_ref())
    }

    /// Records the stronger proof produced by catalog-backed component
    /// ingress. Callers must first validate the complete Schema v1 row and
    /// its primary-key envelope.
    pub(crate) fn certify_boundary_validation(
        &self,
    ) -> Result<(), crate::plugin::wire::typed::Error> {
        if let Some(NativePayloadCache::Durable {
            boundary_validation,
            ..
        }) = self.native_payload.get()
        {
            let _ = boundary_validation.set(BoundaryValidationToken { _private: () });
        } else if self.native_payload.get().is_none() {
            let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
                &self.schema_fingerprint,
                &self.row_pk,
                &self.row,
            )?
            .into();
            let _ = self
                .native_payload
                .set(NativePayloadCache::BoundaryValidated {
                    bytes: payload,
                    _proof: BoundaryValidationToken { _private: () },
                });
        }
        Ok(())
    }

    pub(crate) fn boundary_validation_certified(&self) -> bool {
        matches!(
            self.native_payload.get(),
            Some(NativePayloadCache::BoundaryValidated { .. })
        ) || matches!(
            self.native_payload.get(),
            Some(NativePayloadCache::Durable {
                boundary_validation,
                ..
            }) if boundary_validation.get().is_some()
        )
    }

    pub(super) fn certify_boundary_create_validation(&self) {
        let _ = self
            .boundary_create_validation
            .set(BoundaryValidationToken { _private: () });
    }

    pub(super) fn boundary_create_validation_certified(&self) -> bool {
        self.boundary_create_validation.get().is_some()
    }

    pub fn estimated_size(&self) -> u64 {
        let key = 4_u64.saturating_add(self.row_pk.iter().map(typed_value_size).sum::<u64>());
        let values = self
            .row
            .iter()
            .map(|(name, value)| {
                4_u64
                    .saturating_add(name.len() as u64)
                    .saturating_add(typed_value_size(value))
            })
            .sum::<u64>();
        key.saturating_add(4)
            .saturating_add(values)
            .saturating_add(64)
    }
}

pub(crate) fn compress_durable_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    if payload.len() < ENGINE_ROW_COMPRESSION_THRESHOLD
        || payload.first().copied() != Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
        || payload.first().copied() == Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION)
    {
        return Ok(payload);
    }
    compress_native_snapshot_payload(payload)
}

pub(crate) fn compress_hot_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    if payload.len() < 256
        || payload.first().is_none_or(|version| {
            !matches!(
                *version,
                crate::plugin::wire::typed::NATIVE_ROW_PAYLOAD_VERSION
                    | crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION
                    | crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION
            )
        })
    {
        return Ok(payload);
    }
    compress_native_snapshot_payload(payload)
}

fn compress_native_snapshot_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    let compressed = lz4_flex::block::compress(&payload);
    if compressed.len().saturating_add(5) >= payload.len() {
        return Ok(payload);
    }
    let mut framed = Vec::with_capacity(5 + compressed.len());
    framed.push(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION);
    framed.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| {
                crate::plugin::wire::typed::Error::Invalid(
                    "durable typed payload exceeds u32 framing",
                )
            })?
            .to_le_bytes(),
    );
    framed.extend_from_slice(&compressed);
    Ok(framed)
}

pub(crate) fn decompress_engine_row_payload(payload: &[u8]) -> Result<Arc<[u8]>, LixError> {
    let expected_len = u32::from_le_bytes(
        payload
            .get(1..5)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "compressed engine typed payload is truncated",
                )
            })?
            .try_into()
            .expect("four-byte compact payload length"),
    ) as usize;
    if expected_len > ENGINE_ROW_PAYLOAD_MAX_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "compressed engine typed payload exceeds its decoded size limit",
        ));
    }
    let decoded = lz4_flex::block::decompress(&payload[5..], expected_len).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("cannot decompress compact engine typed payload: {error}"),
        )
    })?;
    if decoded.len() != expected_len
        || !matches!(
            decoded.first().copied(),
            Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
                | Some(crate::plugin::wire::typed::NATIVE_ROW_PAYLOAD_VERSION)
                | Some(crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION)
        )
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "compressed engine typed payload has an invalid decoded envelope",
        ));
    }
    Ok(decoded.into())
}

fn typed_value_size(value: &lix_schema::Value) -> u64 {
    match value {
        lix_schema::Value::Null => 1,
        lix_schema::Value::Text(value) => value.len() as u64 + 5,
        lix_schema::Value::Uuid(_) => 17,
        lix_schema::Value::Int8(_) | lix_schema::Value::Float8(_) => 9,
        lix_schema::Value::Boolean(_) => 2,
        lix_schema::Value::Jsonb(value) => value.estimated_binary_size().saturating_add(5),
        lix_schema::Value::Timestamptz(_) => 9,
    }
}

impl WasmHostBytes {
    pub fn len(&self) -> u64 {
        match self {
            Self::Typed(row) => row.estimated_size(),
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

#[derive(Debug, Clone)]
pub struct WasmRowKey {
    pub schema_key: SharedStr,
    pub schema_fingerprint: [u8; 32],
    pub row_pk: Arc<[lix_schema::Value]>,
}

impl PartialEq for WasmRowKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.schema_key == other.schema_key
            && self.schema_fingerprint == other.schema_fingerprint
            && typed_key_values_cmp(&self.row_pk, &other.row_pk).is_eq()
    }
}

impl Eq for WasmRowKey {}

impl PartialOrd for WasmRowKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WasmRowKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.schema_key
            .cmp(&other.schema_key)
            .then_with(|| self.schema_fingerprint.cmp(&other.schema_fingerprint))
            .then_with(|| typed_key_values_cmp(&self.row_pk, &other.row_pk))
    }
}

impl Hash for WasmRowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema_key.hash(state);
        self.schema_fingerprint.hash(state);
        for value in self.row_pk.iter() {
            hash_typed_key_value(value, state);
        }
    }
}

impl WasmRowKey {
    pub fn from_typed_parts(
        schema_key: impl Into<SharedStr>,
        schema_fingerprint: [u8; 32],
        row_pk: impl Into<Arc<[lix_schema::Value]>>,
    ) -> Result<Self, LixError> {
        let row_pk = row_pk.into();
        if row_pk.is_empty()
            || row_pk.iter().any(|value| {
                !matches!(
                    value,
                    lix_schema::Value::Text(_)
                        | lix_schema::Value::Uuid(_)
                        | lix_schema::Value::Int8(_)
                )
            })
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                "typed row keys must contain one or more text, uuid, or int8 values",
            ));
        }
        Ok(Self {
            schema_key: schema_key.into(),
            schema_fingerprint,
            row_pk,
        })
    }
}

#[inline]
fn typed_key_values_cmp(
    left: &[lix_schema::Value],
    right: &[lix_schema::Value],
) -> std::cmp::Ordering {
    use lix_schema::Value;

    match (left, right) {
        ([Value::Text(left)], [Value::Text(right)]) => return left.cmp(right),
        ([Value::Uuid(left)], [Value::Uuid(right)]) => return left.cmp(right),
        ([Value::Int8(left)], [Value::Int8(right)]) => return left.cmp(right),
        _ => {}
    }
    for (left, right) in left.iter().zip(right) {
        let ordering = typed_key_value_cmp(left, right);
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn typed_key_value_cmp(left: &lix_schema::Value, right: &lix_schema::Value) -> std::cmp::Ordering {
    fn tag(value: &lix_schema::Value) -> u8 {
        match value {
            lix_schema::Value::Text(_) => 0,
            lix_schema::Value::Uuid(_) => 1,
            lix_schema::Value::Int8(_) => 2,
            _ => unreachable!("typed row key construction rejects non-key values"),
        }
    }
    tag(left)
        .cmp(&tag(right))
        .then_with(|| match (left, right) {
            (lix_schema::Value::Text(left), lix_schema::Value::Text(right)) => left.cmp(right),
            (lix_schema::Value::Uuid(left), lix_schema::Value::Uuid(right)) => left.cmp(right),
            (lix_schema::Value::Int8(left), lix_schema::Value::Int8(right)) => left.cmp(right),
            _ => std::cmp::Ordering::Equal,
        })
}

fn hash_typed_key_value<H: Hasher>(value: &lix_schema::Value, state: &mut H) {
    match value {
        lix_schema::Value::Text(value) => {
            0_u8.hash(state);
            value.hash(state);
        }
        lix_schema::Value::Uuid(value) => {
            1_u8.hash(state);
            value.hash(state);
        }
        lix_schema::Value::Int8(value) => {
            2_u8.hash(state);
            value.hash(state);
        }
        _ => unreachable!("typed row key construction rejects non-key values"),
    }
}

#[derive(Debug, Clone)]
pub struct WasmRow<B> {
    pub key: WasmRowKey,
    /// Native Schema v1 row payload for the schema identified by `key`.
    pub payload: B,
}

pub type WasmHostRow = WasmRow<WasmHostBytes>;
pub type WasmGuestRow = WasmRow<WasmGuestBytes>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum WasmChangeEffect {
    #[default]
    Content,
    FormatOnly,
}

#[derive(Debug, Clone)]
pub enum WasmRowChange<B> {
    Create {
        schema_key: SharedStr,
        local_ref: u64,
        /// The v2 host may resolve the host-namespaced primary key before canonical
        /// validation. The component leaves this absent and retains keyless semantics
        /// until create-context materialization.
        resolved_key: Option<WasmRowKey>,
        payload: B,
    },
    Upsert {
        row: WasmRow<B>,
        effect: WasmChangeEffect,
    },
    Delete(WasmRowKey),
}

impl<B> WasmRowChange<B> {
    pub fn row_key(&self) -> Option<&WasmRowKey> {
        match self {
            Self::Create { resolved_key, .. } => resolved_key.as_ref(),
            Self::Upsert { row, .. } => Some(&row.key),
            Self::Delete(key) => Some(key),
        }
    }

    pub fn schema_key(&self) -> &str {
        match self {
            Self::Create { schema_key, .. } => schema_key,
            Self::Upsert { row, .. } => &row.key.schema_key,
            Self::Delete(key) => &key.schema_key,
        }
    }

    pub fn local_ref(&self) -> Option<u64> {
        match self {
            Self::Create { local_ref, .. } => Some(*local_ref),
            Self::Upsert { .. } | Self::Delete(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WasmRowChanges<B> {
    pub changes: Vec<WasmRowChange<B>>,
}

impl<B> Default for WasmRowChanges<B> {
    fn default() -> Self {
        Self {
            changes: Vec::new(),
        }
    }
}

impl<B> WasmRowChanges<B> {
    pub fn validate(&self) -> Result<(), LixError> {
        if change_keys_have_duplicates(&self.changes) {
            return Err(invalid_param(
                "a component row key may occur only once in one transition",
            ));
        }
        if create_refs_have_duplicates(&self.changes) {
            return Err(invalid_param(
                "a component create local reference may occur only once per schema in one transition",
            ));
        }
        Ok(())
    }

    pub fn row_change_count(&self) -> usize {
        self.changes.len()
    }
}

/// Builds the sole transition-wide duplicate-check index as borrowed key
/// references. Sorting never moves or clones a key owner, and the exact
/// capacity prevents geometric growth for a large cursor.
fn sorted_change_key_refs<B>(changes: &[WasmRowChange<B>]) -> Vec<&WasmRowKey> {
    let mut keys = Vec::with_capacity(changes.len());
    keys.extend(changes.iter().filter_map(WasmRowChange::row_key));
    keys.sort_unstable();
    keys
}

fn sorted_create_refs<B>(changes: &[WasmRowChange<B>]) -> Vec<(&str, u64)> {
    let mut creates = Vec::with_capacity(changes.len());
    creates.extend(changes.iter().filter_map(|change| match change {
        WasmRowChange::Create {
            schema_key,
            local_ref,
            ..
        } => Some((schema_key.as_str(), *local_ref)),
        WasmRowChange::Upsert { .. } | WasmRowChange::Delete(_) => None,
    }));
    creates.sort_unstable();
    creates
}

fn change_keys_have_duplicates<B>(changes: &[WasmRowChange<B>]) -> bool {
    sorted_change_key_refs(changes)
        .windows(2)
        .any(|pair| pair[0] == pair[1])
}

fn create_refs_have_duplicates<B>(changes: &[WasmRowChange<B>]) -> bool {
    sorted_create_refs(changes)
        .windows(2)
        .any(|pair| pair[0] == pair[1])
}

/// Validates cursor-wide uniqueness after all pages have been moved into their
/// stable host vector.
///
/// Keeping this separate from [`WasmChangeDrainValidator`] avoids cloning every
/// untrusted key into a transition-lived owning tree while preserving
/// arbitrary guest output order and the established rejection text.
pub(crate) fn validate_change_cursor_key_uniqueness<B>(
    changes: &[WasmRowChange<B>],
) -> Result<(), LixError> {
    if change_keys_have_duplicates(changes) {
        return Err(invalid_param(
            "a component row key may occur only once across a change cursor",
        ));
    }
    if create_refs_have_duplicates(changes) {
        return Err(invalid_param(
            "a component create local reference may occur only once per schema across a change cursor",
        ));
    }
    Ok(())
}

pub type WasmHostRowChanges = WasmRowChanges<WasmHostBytes>;
pub type WasmGuestRowChanges = WasmRowChanges<WasmGuestRowPayload>;

#[derive(Debug, Clone)]
pub struct WasmRowPage {
    pub rows: Vec<WasmHostRow>,
}

/// Bounded, complete host rows. `None` is permanent EOF and every
/// successful page must be non-empty and no larger than `max_bytes` once
/// encoded in the current typed-page format.
pub trait WasmRowSource: Send {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmRowPage>, LixError>;
}

/// Lazily materialized predecessor identities used only when a warm
/// `parse_changes` transition elects to replace every row.
pub trait WasmRowKeySource: Send {
    fn into_keys(self: Box<Self>) -> Result<BTreeSet<WasmRowKey>, LixError>;
}

/// Bounded, merge-resolved host changes supplied to `rows_changed`.
pub trait WasmRowChangeSource: Send {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmHostRowChanges>, LixError>;
}

/// One same-column concurrent overlap presented to a column merger. Complete
/// rows and overlapping values are native Schema v1 values; no row snapshot
/// or JSON column representation crosses the component boundary.
#[derive(Debug, Clone)]
pub struct WasmColumnMerge {
    pub ordinal: u32,
    pub key: WasmRowKey,
    pub file_id: Option<String>,
    pub column: String,
    pub schema_fingerprint: [u8; 32],
    pub base: Option<lix_schema::Value>,
    pub a: Option<lix_schema::Value>,
    pub b: Option<lix_schema::Value>,
    pub base_row: Arc<WasmTypedRow>,
    pub a_row: Arc<WasmTypedRow>,
    pub b_row: Arc<WasmTypedRow>,
}

pub type WasmHostColumnMerge = WasmColumnMerge;

#[derive(Debug, Clone)]
pub struct WasmColumnMergePage {
    pub merges: Vec<WasmHostColumnMerge>,
}

pub trait WasmColumnMergeSource: Send {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmColumnMergePage>, LixError>;
}

#[derive(Debug, Clone)]
pub enum WasmColumnMergeResult<B> {
    UseLww,
    /// `None` removes an optional column. `Some` contains the native Schema v1
    /// value, including `Value::Jsonb(Null)` for an explicit JSON null.
    Replace(Option<B>),
}

pub type WasmGuestColumnMergeResult = WasmColumnMergeResult<WasmGuestColumnValue>;

#[derive(Debug, Clone)]
pub struct WasmColumnMergeResultPage {
    pub format_version: u16,
    pub ordinals: Vec<u32>,
    pub results: Vec<WasmGuestColumnMergeResult>,
    pub outputs: Option<WasmByteOutputsHandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmCreateContext {
    pub high: u64,
    pub low: u32,
}

impl WasmCreateContext {
    pub fn row_pk(self, local_ref: u64) -> Result<Vec<lix_schema::Value>, LixError> {
        Ok(vec![lix_schema::Value::Uuid(self.component(local_ref)?)])
    }

    pub fn component(self, local_ref: u64) -> Result<uuid::Uuid, LixError> {
        Ok(uuid::Uuid::from_bytes(
            self.component_uuid_bytes(local_ref)?,
        ))
    }

    pub(crate) fn component_uuid_bytes(self, local_ref: u64) -> Result<[u8; 16], LixError> {
        let local_ref = u32::try_from(local_ref).map_err(|_| {
            invalid_param(
                "component create local references must fit in an unsigned 32-bit integer",
            )
        })?;
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.high.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.low.to_be_bytes());
        bytes[12..].copy_from_slice(&local_ref.to_be_bytes());
        Ok(bytes)
    }
}

pub struct WasmOpenFileInput {
    pub descriptor: WasmFileDescriptor,
    pub file: Arc<dyn WasmByteSource>,
    pub creates: WasmCreateContext,
}

impl fmt::Debug for WasmOpenFileInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmOpenFileInput")
            .field("descriptor", &self.descriptor)
            .field("file_len", &self.file.len())
            .field("creates", &self.creates)
            .finish()
    }
}

pub struct WasmOpenRowsInput {
    pub descriptor: WasmFileDescriptor,
    pub rows: Box<dyn WasmRowSource>,
    pub accepted: Option<Arc<dyn WasmByteSource>>,
}

impl fmt::Debug for WasmOpenRowsInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmOpenRowsInput")
            .field("descriptor", &self.descriptor)
            .field(
                "accepted_len",
                &self.accepted.as_ref().map(|source| source.len()),
            )
            .finish_non_exhaustive()
    }
}

pub struct WasmFileUpdate {
    pub before_descriptor: WasmFileDescriptor,
    pub after_descriptor: WasmFileDescriptor,
    pub before: Arc<dyn WasmByteSource>,
    pub edits: Vec<WasmInputSplice>,
    pub after: Arc<dyn WasmByteSource>,
    pub creates: WasmCreateContext,
    pub rows: Option<Box<dyn WasmRowSource>>,
    pub prior_row_keys: Option<Box<dyn WasmRowKeySource>>,
}

pub struct WasmColdFileUpdate {
    pub before_descriptor: WasmFileDescriptor,
    pub after_descriptor: WasmFileDescriptor,
    pub before: Option<Arc<dyn WasmByteSource>>,
    pub edits: Vec<WasmInputSplice>,
    pub after: Arc<dyn WasmByteSource>,
    pub creates: WasmCreateContext,
    pub rows: Box<dyn WasmRowSource>,
}

impl fmt::Debug for WasmColdFileUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmColdFileUpdate")
            .field("before_descriptor", &self.before_descriptor)
            .field("after_descriptor", &self.after_descriptor)
            .field(
                "before_len",
                &self.before.as_ref().map(|source| source.len()),
            )
            .field("edits", &self.edits)
            .field("after_len", &self.after.len())
            .field("creates", &self.creates)
            .field("rows", &"lazy complete row source")
            .finish()
    }
}

impl WasmColdFileUpdate {
    pub fn validate(&self, limits: WasmTransitionLimits) -> Result<(), LixError> {
        match &self.before {
            Some(before) => WasmFileUpdate {
                before_descriptor: self.before_descriptor.clone(),
                after_descriptor: self.after_descriptor.clone(),
                before: Arc::clone(before),
                edits: self.edits.clone(),
                after: Arc::clone(&self.after),
                creates: self.creates,
                rows: None,
                prior_row_keys: None,
            }
            .validate(limits),
            None => {
                self.before_descriptor
                    .validate_warm_successor(&self.after_descriptor)?;
                limits.validate()?;
                if !self.edits.is_empty() {
                    return Err(invalid_param(
                        "a derived cold successor must not carry host byte splices",
                    ));
                }
                if self.after.len() > limits.max_total_bytes {
                    return Err(invalid_param(
                        "a derived cold successor source exceeds max_total_bytes",
                    ));
                }
                Ok(())
            }
        }
    }
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
            .field("creates", &self.creates)
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
            return Err(invalid_param(
                "component input splice count exceeds its limit",
            ));
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
                .ok_or_else(|| invalid_param("component input splice deletion range overflowed"))?;
            if previous_start == Some(edit.offset) || edit.offset < previous_end || end > before_len
            {
                return Err(invalid_param(
                    "component input splices must have strictly increasing starts, be non-overlapping, and stay in the accepted base",
                ));
            }
            if let WasmInputBytes::AfterRange(range) = &edit.insert
                && range.end()? > after_len
            {
                return Err(invalid_param(
                    "component after-source range is out of bounds",
                ));
            }
            if let WasmInputBytes::Inline(bytes) = &edit.insert {
                inline = inline
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid_param("component inline input byte count overflowed"))?;
            }
            deleted = deleted
                .checked_add(edit.delete_len)
                .ok_or_else(|| invalid_param("component deleted byte count overflowed"))?;
            inserted = inserted
                .checked_add(edit.insert.len())
                .ok_or_else(|| invalid_param("component inserted byte count overflowed"))?;
            previous_start = Some(edit.offset);
            previous_end = end;
        }
        if inline > limits.max_inline_input_bytes {
            return Err(invalid_param(
                "component inline input bytes exceed their limit",
            ));
        }
        let reconstructed_len = before_len
            .checked_sub(deleted)
            .and_then(|len| len.checked_add(inserted))
            .ok_or_else(|| invalid_param("component reconstructed file length overflowed"))?;
        if reconstructed_len != after_len {
            return Err(invalid_param(
                "component input splices do not reconstruct the declared after source length",
            ));
        }
        Ok(())
    }
}

pub struct WasmRowUpdate {
    pub before_descriptor: WasmFileDescriptor,
    pub after_descriptor: WasmFileDescriptor,
    pub before: Arc<dyn WasmByteSource>,
    pub changes: Box<dyn WasmRowChangeSource>,
}

/// Input for one stateless, file-scoped conflict-resolution call. It has no
/// document handle on purpose: a one-row/one-paragraph merge must not force a
/// cold open of all semantic rows in the file.
pub struct WasmColumnMergeUpdate {
    pub merges: Box<dyn WasmColumnMergeSource>,
}

impl fmt::Debug for WasmColumnMergeUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmColumnMergeUpdate")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for WasmRowUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmRowUpdate")
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

#[derive(Clone)]
pub struct WasmDocumentCheckpoint {
    payload: Arc<dyn Any + Send + Sync>,
    retained_bytes: u64,
}

impl fmt::Debug for WasmDocumentCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmDocumentCheckpoint")
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}

impl WasmDocumentCheckpoint {
    pub fn new<T>(payload: T, retained_bytes: u64) -> Self
    where
        T: Any + Send + Sync,
    {
        Self {
            payload: Arc::new(payload),
            retained_bytes,
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.payload.downcast_ref()
    }

    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }
}
handle_type!(WasmChangeCursorHandle);
handle_type!(WasmColumnMergeCursorHandle);
handle_type!(WasmEditCursorHandle);
handle_type!(WasmByteOutputsHandle);
handle_type!(WasmTransitionHandle);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmOutputRange {
    pub index: u32,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WasmGuestRowPayload {
    /// Native Schema v1 values emitted by a typed-row page.
    Typed(Arc<WasmTypedRow>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum WasmGuestBytes {
    /// A shared slice of the guest packet payload. Every inline value decoded
    /// from one packet page retains the same backing allocation.
    Inline(Bytes),
    Output(WasmOutputRange),
}

/// A typed column-merge replacement whose bytes are streamed through the
/// page-local output table and decoded as one native Schema v1 value by the
/// host.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WasmGuestColumnValue {
    Output(WasmOutputRange),
}

#[derive(Debug, Clone)]
pub struct WasmChangePage {
    pub format_version: u16,
    pub changes: WasmGuestRowChanges,
    /// Exactly one page-local table supplies all `Output` values in `changes`.
    pub outputs: Option<WasmByteOutputsHandle>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WasmOutputSplice {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: WasmGuestBytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WasmEditPage {
    pub edits: Vec<WasmOutputSplice>,
    /// Exactly one page-local table supplies all output ranges in `edits`.
    pub outputs: Option<WasmByteOutputsHandle>,
}

/// Cross-page validator for a guest change cursor. Raw packet framing and
/// inline byte bounds are checked by the binding before it constructs a typed
/// page; this validator owns page, reference, and permanent-EOF invariants.
/// Cursor-wide uniqueness is checked once over borrowed keys after the stable
/// output vector has been assembled.
#[derive(Debug, Clone, Copy)]
pub struct WasmChangeDrainValidator {
    limits: WasmTransitionLimits,
    pages: u32,
    attachment_refs: u32,
    reached_eof: bool,
}

impl WasmChangeDrainValidator {
    pub fn new(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            pages: 0,
            attachment_refs: 0,
            reached_eof: false,
        })
    }

    pub fn accept_page(&mut self, page: &WasmChangePage) -> Result<(), LixError> {
        if self.reached_eof {
            return Err(invalid_param(
                "a component change cursor advanced after EOF",
            ));
        }
        if page.format_version != CURRENT_PACKET_FORMAT {
            return Err(invalid_param(
                "unsupported component change packet format version",
            ));
        }
        if page.changes.changes.is_empty() {
            return Err(invalid_param("a component change page must not be empty"));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_param("component change page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_param(
                "component change page count exceeds its limit",
            ));
        }

        for change in &page.changes.changes {
            match change {
                WasmRowChange::Create {
                    payload: WasmGuestRowPayload::Typed(_),
                    ..
                }
                | WasmRowChange::Upsert {
                    row:
                        WasmRow {
                            payload: WasmGuestRowPayload::Typed(_),
                            ..
                        },
                    ..
                }
                | WasmRowChange::Delete(_) => {}
            }
        }
        validate_attachment_table_presence(0, page.outputs.is_some())?;
        let page_refs = 0u32;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(page_refs)
            .ok_or_else(|| invalid_param("component attachment reference count overflowed"))?;
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_param(
                "component attachment reference count exceeds its limit",
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
#[derive(Debug, Clone, Copy)]
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
            return Err(invalid_param("a component edit cursor advanced after EOF"));
        }
        if page.edits.is_empty() {
            return Err(invalid_param("a component edit page must not be empty"));
        }
        if page.edits.len() > self.limits.max_inline_edits as usize {
            return Err(invalid_param("component edit page count exceeds its limit"));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_param("component edit page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_param("component edit page count exceeds its limit"));
        }

        let mut page_record_bytes = 0u64;
        let mut page_refs = 0u32;
        for edit in &page.edits {
            let mut record_bytes = EDIT_SPLICE_METADATA_BYTES;
            let end = edit.offset.checked_add(edit.delete_len).ok_or_else(|| {
                invalid_param("component output splice deletion range overflowed")
            })?;
            if self.previous_start == Some(edit.offset)
                || edit.offset < self.previous_end
                || end > self.base_len
            {
                return Err(invalid_param(
                    "component output splices must have globally increasing starts, be non-overlapping, and stay in the accepted base",
                ));
            }
            match &edit.insert {
                WasmGuestBytes::Inline(bytes) => {
                    record_bytes =
                        record_bytes
                            .checked_add(bytes.len() as u64)
                            .ok_or_else(|| {
                                invalid_param("component output edit record bytes overflowed")
                            })?;
                }
                WasmGuestBytes::Output(range) => {
                    range
                        .offset
                        .checked_add(range.length)
                        .ok_or_else(|| invalid_param("component edit output range overflowed"))?;
                    page_refs = page_refs.checked_add(1).ok_or_else(|| {
                        invalid_param("component attachment reference count overflowed")
                    })?;
                }
            }
            if record_bytes > u64::from(self.limits.max_record_bytes) {
                return Err(invalid_param(
                    "component output edit record exceeds max_record_bytes",
                ));
            }
            page_record_bytes = page_record_bytes
                .checked_add(record_bytes)
                .ok_or_else(|| invalid_param("component output edit page bytes overflowed"))?;
            self.previous_start = Some(edit.offset);
            self.previous_end = end;
        }
        if page_record_bytes > u64::from(self.limits.max_page_bytes) {
            return Err(invalid_param(
                "component output edit page exceeds max_page_bytes",
            ));
        }
        validate_attachment_table_presence(page_refs, page.outputs.is_some())?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(page_refs)
            .ok_or_else(|| invalid_param("component attachment reference count overflowed"))?;
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_param(
                "component attachment reference count exceeds its limit",
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
            "a component page must own an output table exactly when it contains output references",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmFileTransition {
    pub transition: WasmTransitionHandle,
    pub document: WasmDocumentHandle,
    pub changes: WasmChangeCursorHandle,
    pub replace_all_rows: bool,
}

/// One immutable, host-validated semantic batch which remains encoded until
/// storage/query consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmCertifiedCreateRange {
    pub schema_key: String,
    pub first_local_ref: u32,
    pub last_local_ref: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmCertifiedRowBatch {
    pub format: u16,
    pub schema_keys: Vec<String>,
    pub row_count: u64,
    pub creates: WasmCreateContext,
    pub create_ranges: Vec<WasmCertifiedCreateRange>,
    pub complete_file_state: bool,
    pub pages: Vec<Bytes>,
}

pub(crate) const HOST_CERTIFIED_PACKET_FORMAT: u16 = 3;
pub(crate) const HOST_CERTIFIED_ZSTD_PACKET_FORMAT: u16 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmRowTransition {
    pub transition: WasmTransitionHandle,
    pub document: WasmDocumentHandle,
    pub edits: WasmEditCursorHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmColumnMergeTransition {
    pub transition: WasmTransitionHandle,
    pub results: WasmColumnMergeCursorHandle,
}

/// A compiled Component. Implementations share this factory but create one
/// isolated Store/instance for every branch/file actor.
#[async_trait]
pub trait WasmComponentFactory: Send + Sync {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentActor>, LixError>;
}

/// One serial branch/file actor. Handles are instance-local and invalid after
/// `retire`; callers must retire the complete actor after traps, timeouts,
/// cancellation, or uncertain completion.
#[async_trait]
pub trait WasmComponentActor: Send {
    /// True when `open_rows` retains accepted bytes only and returns them
    /// unchanged without invoking a semantic renderer.
    fn cold_open_hydrates_without_render(&self) -> bool {
        false
    }

    /// False when a cold actor reconstructs its transition state from the
    /// accepted file bytes and does not consume durable semantic rows.
    fn cold_open_requires_rows(&self) -> bool {
        true
    }

    async fn fork_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<WasmDocumentHandle, LixError>;

    /// Captures runtime-private immutable state without serializing it through
    /// the Component boundary. Implementations that cannot externalize a
    /// document return `None` and retain the normal hydration path.
    async fn checkpoint_document(
        &mut self,
        _document: WasmDocumentHandle,
    ) -> Result<Option<WasmDocumentCheckpoint>, LixError> {
        Ok(None)
    }

    /// Restores a compatible runtime-private checkpoint into this fresh actor.
    async fn restore_document(
        &mut self,
        _checkpoint: &WasmDocumentCheckpoint,
    ) -> Result<WasmDocumentHandle, LixError> {
        Err(invalid_param(
            "this component actor does not support decoded document checkpoints",
        ))
    }

    async fn open_file(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<WasmFileTransition, LixError>;

    async fn open_rows(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenRowsInput,
    ) -> Result<WasmRowTransition, LixError>;

    async fn file_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
    ) -> Result<WasmFileTransition, LixError>;

    async fn cold_file_changed(
        &mut self,
        limits: WasmTransitionLimits,
        update: WasmColdFileUpdate,
    ) -> Result<WasmFileTransition, LixError> {
        let _ = (limits, update);
        Err(invalid_param(
            "this component actor does not implement cold successor reconciliation",
        ))
    }

    async fn rows_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmRowUpdate,
    ) -> Result<WasmRowTransition, LixError>;

    async fn merge_columns(
        &mut self,
        limits: WasmTransitionLimits,
        update: WasmColumnMergeUpdate,
    ) -> Result<WasmColumnMergeTransition, LixError> {
        let _ = (limits, update);
        Err(invalid_param(
            "this component actor does not implement column merging",
        ))
    }

    async fn next_change_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmChangeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<WasmChangePage>, LixError>;

    async fn next_column_merge_result_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmColumnMergeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<WasmColumnMergeResultPage>, LixError> {
        let _ = (transition, cursor, max_bytes);
        Err(invalid_param(
            "this component actor does not expose column merge results",
        ))
    }

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

pub fn validate_component_limits(
    component: WasmLimits,
    transition: WasmTransitionLimits,
) -> Result<(), LixError> {
    if component.max_memory_bytes == 0 {
        return Err(invalid_param(
            "component component memory limit must be positive",
        ));
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

    fn path_value_plan() -> crate::catalog::SchemaPlan {
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "canonical_path_value_probe",
            "columns": [
                {"name": "path", "type": "text", "nullable": false},
                {"name": "value", "type": "jsonb", "nullable": false}
            ],
            "primary_key": ["path"]
        });
        crate::catalog::SchemaPlan::compile_standalone_for_test(
            crate::catalog::SchemaCatalogKey {
                schema_key: "canonical_path_value_probe".to_owned(),
            },
            schema,
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::new(),
        )
        .expect("path/value schema should compile")
    }

    #[test]
    fn canonical_parameter_route_matches_owned_route_and_rejects_noncanonical_input() {
        let plan = path_value_plan();
        assert!(plan.accepts_canonical_certificate());
        let value = serde_json::json!({"z": [2, 1], "a": "β"});
        let canonical = lix_schema::Jsonb::from(value.clone())
            .to_json_string()
            .unwrap();

        let mut expected = Vec::new();
        WasmTypedRow::append_certified_path_value_payload(
            &mut expected,
            &plan,
            "/packages/β",
            value,
        )
        .unwrap();
        let mut actual = Vec::new();
        assert!(
            WasmTypedRow::try_append_certified_path_value_payload_from_canonical_json(
                &mut actual,
                &plan,
                "/packages/β",
                canonical.as_bytes(),
            )
            .unwrap()
        );
        assert_eq!(actual, expected);

        let mut retained = b"retained".to_vec();
        assert!(
            !WasmTypedRow::try_append_certified_path_value_payload_from_canonical_json(
                &mut retained,
                &plan,
                "/packages/β",
                br#" {"z":2,"a":1}"#,
            )
            .unwrap()
        );
        assert_eq!(retained, b"retained");
    }

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
            file_id: "file-1".to_owned(),
            path: Some("data.csv".to_owned()),
            plugin: WasmPluginSelection {
                plugin_key: "plugin_csv".to_owned(),
                generation: generation.to_owned(),
            },
        }
    }

    #[test]
    fn warm_successor_rejects_a_different_stable_file_id() {
        let before = descriptor("generation-1");
        let mut after = before.clone();
        after.file_id = "file-2".to_owned();
        let error = before
            .validate_warm_successor(&after)
            .expect_err("a warm transition cannot cross file identities");
        assert!(error.message.contains("stable file id"));
    }

    #[test]
    fn transition_counter_aggregation_keeps_directional_page_and_source_metrics() {
        let mut total = WasmTransitionCounters::default();
        total.accumulate(WasmTransitionCounters {
            file_read_calls: 2,
            file_bytes_read: 11,
            state_read_calls: 3,
            state_key_bytes: 13,
            state_value_bytes_read: 17,
            row_input_pages: 5,
            row_input_records: 19,
            row_input_wire_bytes: 23,
            row_output_pages: 7,
            row_output_records: 29,
            row_output_wire_bytes: 31,
            row_input_attachment_reads: 37,
            row_input_attachment_bytes: 41,
            row_output_attachment_writes: 43,
            row_output_attachment_bytes: 47,
            ..WasmTransitionCounters::default()
        });

        assert_eq!(total.file_read_calls, 2);
        assert_eq!(total.file_bytes_read, 11);
        assert_eq!(total.state_read_calls, 3);
        assert_eq!(total.state_key_bytes, 13);
        assert_eq!(total.state_value_bytes_read, 17);
        assert_eq!(total.row_input_pages, 5);
        assert_eq!(total.row_input_records, 19);
        assert_eq!(total.row_input_wire_bytes, 23);
        assert_eq!(total.row_output_pages, 7);
        assert_eq!(total.row_output_records, 29);
        assert_eq!(total.row_output_wire_bytes, 31);
        assert_eq!(total.row_input_attachment_reads, 37);
        assert_eq!(total.row_input_attachment_bytes, 41);
        assert_eq!(total.row_output_attachment_writes, 43);
        assert_eq!(total.row_output_attachment_bytes, 47);
    }

    #[test]
    fn outer_row_json_counter_positive_control_covers_every_forbidden_operation() {
        let mut counters = WasmTransitionCounters::default();
        for operation in [
            OuterRowJsonOperation::Parse,
            OuterRowJsonOperation::Serialize,
            OuterRowJsonOperation::Canonicalize,
            OuterRowJsonOperation::DomFallback,
        ] {
            counters.record_outer_row_json_operation(operation, 17);
        }
        let mut aggregate = WasmTransitionCounters::default();
        aggregate.accumulate(counters);

        assert_eq!(aggregate.outer_row_json_parse_calls, 1);
        assert_eq!(aggregate.outer_row_json_parse_bytes, 17);
        assert_eq!(aggregate.outer_row_json_serialize_calls, 1);
        assert_eq!(aggregate.outer_row_json_serialize_bytes, 17);
        assert_eq!(aggregate.outer_row_json_canonicalize_calls, 1);
        assert_eq!(aggregate.outer_row_json_canonicalize_bytes, 17);
        assert_eq!(aggregate.outer_row_json_dom_fallback_calls, 1);
        assert_eq!(aggregate.outer_row_json_dom_fallback_bytes, 17);
    }

    #[test]
    fn durable_payload_cache_can_be_boundary_certified_after_encoding() {
        let row = WasmTypedRow {
            schema_fingerprint: [7; 32],
            row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
            row: lix_schema::Row::from([("id", lix_schema::Value::Text("row-1".to_owned()))]),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        };

        row.durable_payload().expect("encode durable payload");
        assert!(!row.boundary_validation_certified());
        row.certify_boundary_validation()
            .expect("certify encoded payload");
        assert!(row.boundary_validation_certified());
    }

    #[test]
    fn durable_payload_ref_borrows_the_cached_native_bytes() {
        let row = WasmTypedRow {
            schema_fingerprint: [8; 32],
            row_pk: vec![lix_schema::Value::Text("row-ref".to_owned())].into(),
            row: lix_schema::Row::from([("id", lix_schema::Value::Text("row-ref".to_owned()))]),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        };

        let first = row
            .durable_payload_ref()
            .expect("borrowed durable payload should encode");
        let first_pointer = first.as_ptr();
        let second = row
            .durable_payload_ref()
            .expect("borrowed durable payload should reuse cache");
        assert_eq!(second.as_ptr(), first_pointer);
        assert_eq!(second, row.durable_payload().unwrap().as_ref());
    }

    #[test]
    fn compressed_durable_payload_round_trips_and_rejects_invalid_framing() {
        let payload = vec![
            crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION;
            ENGINE_ROW_COMPRESSION_THRESHOLD * 2
        ];
        let compressed = compress_durable_payload(payload.clone())
            .expect("repetitive engine payload should compress");
        assert_eq!(
            compressed.first().copied(),
            Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION)
        );
        assert_eq!(
            decompress_engine_row_payload(&compressed)
                .expect("compressed engine payload should decode")
                .as_ref(),
            payload
        );

        let truncated = [COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        assert!(decompress_engine_row_payload(&truncated).is_err());

        let mut oversized = vec![COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        oversized.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decompress_engine_row_payload(&oversized).is_err());

        let mut invalid_inner = vec![99; ENGINE_ROW_COMPRESSION_THRESHOLD * 2];
        invalid_inner[0] = 99;
        let invalid_inner = compress_durable_payload(invalid_inner)
            .expect("invalid inner payload should still frame");
        assert!(decompress_engine_row_payload(&invalid_inner).is_err());
    }

    #[test]
    fn duplicate_sort_borrows_native_key_owners() {
        let schema = SharedStr::from_static("csv_row");
        let changes = (0..10_000)
            .map(|ordinal| {
                let row_pk = if ordinal % 2 == 0 {
                    vec![lix_schema::Value::Text("namespace".to_owned())]
                } else {
                    vec![
                        lix_schema::Value::Text("namespace".to_owned()),
                        lix_schema::Value::Text("row".to_owned()),
                    ]
                };
                WasmRowChange::<WasmGuestBytes>::Delete(WasmRowKey {
                    schema_key: schema.clone(),
                    schema_fingerprint: [7; 32],
                    row_pk: row_pk.into(),
                })
            })
            .collect::<Vec<_>>();

        for (ordinal, change) in changes.iter().enumerate() {
            let key = change.row_key().expect("delete carries a row key");
            assert_eq!(key.row_pk.len(), 1 + (ordinal % 2));
            assert!(key.schema_key.shares_buffer_with(&schema));
        }

        let mut original_owner_addresses = changes
            .iter()
            .map(|change| {
                let key: *const WasmRowKey = change.row_key().expect("delete carries a row key");
                key as usize
            })
            .collect::<Vec<_>>();
        let sorted = sorted_change_key_refs(&changes);
        let mut sorted_owner_addresses = sorted
            .iter()
            .map(|key| {
                let key: *const WasmRowKey = *key;
                key as usize
            })
            .collect::<Vec<_>>();
        original_owner_addresses.sort_unstable();
        sorted_owner_addresses.sort_unstable();

        assert_eq!(sorted.len(), changes.len());
        assert_eq!(
            sorted_owner_addresses, original_owner_addresses,
            "the duplicate index must contain references to original key owners"
        );
        assert!(change_keys_have_duplicates(&changes));
        assert_eq!(
            validate_change_cursor_key_uniqueness(&changes)
                .expect_err("the repeated structural keys are duplicates")
                .message,
            "a component row key may occur only once across a change cursor"
        );
    }

    #[test]
    fn typed_row_key_retains_native_components() {
        let id =
            uuid::Uuid::parse_str("01920000-0000-7000-8000-0000000000aa").expect("fixture UUID");
        let values = vec![lix_schema::Value::Uuid(id), lix_schema::Value::Int8(42)];
        let key =
            WasmRowKey::from_typed_parts("typed", [7; 32], values.clone()).expect("typed key");
        let same_native_key =
            WasmRowKey::from_typed_parts("typed", [7; 32], values.clone()).expect("typed key");
        assert_eq!(key.row_pk.as_ref(), values.as_slice());
        assert_eq!(key.schema_fingerprint, [7; 32]);
        assert_eq!(key, same_native_key);
        assert_eq!(key.cmp(&same_native_key), std::cmp::Ordering::Equal);
    }

    #[test]
    fn cursor_key_uniqueness_accepts_arbitrary_unique_order() {
        let changes = vec![
            WasmRowChange::<WasmGuestBytes>::Delete(
                WasmRowKey::from_typed_parts(
                    "schema-z",
                    [1; 32],
                    vec![lix_schema::Value::Text("row-z".to_owned())],
                )
                .unwrap(),
            ),
            WasmRowChange::<WasmGuestBytes>::Delete(
                WasmRowKey::from_typed_parts(
                    "schema-a",
                    [2; 32],
                    vec![lix_schema::Value::Text("row-a".to_owned())],
                )
                .unwrap(),
            ),
        ];

        validate_change_cursor_key_uniqueness(&changes)
            .expect("cursor duplicate validation must not impose key order");
    }

    #[test]
    fn create_context_produces_canonical_uuid_components() {
        let creates = WasmCreateContext {
            high: 0x0192_0000_0000_7000,
            low: 0x8000_0000,
        };
        assert_eq!(
            creates.component(42).unwrap(),
            uuid::Uuid::parse_str("01920000-0000-7000-8000-00000000002a").unwrap()
        );
        assert_eq!(creates.row_pk(7).unwrap().len(), 1);
        assert!(creates.component(u64::from(u32::MAX) + 1).is_err());
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
            creates: WasmCreateContext { high: 1, low: 2 },
            rows: None,
            prior_row_keys: None,
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
    fn rejects_duplicate_row_keys() {
        let key = WasmRowKey::from_typed_parts(
            "csv_row",
            [0; 32],
            vec![lix_schema::Value::Text("row".to_owned())],
        )
        .unwrap();
        let duplicate = WasmRowChanges::<WasmGuestBytes> {
            changes: vec![
                WasmRowChange::Delete(key.clone()),
                WasmRowChange::Delete(key),
            ],
        };
        assert!(duplicate.validate().is_err());
        assert!(
            WasmRowChanges::<WasmGuestBytes>::default()
                .validate()
                .is_ok()
        );
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
    fn cold_file_budget_scales_with_input_and_stays_bounded() {
        assert_eq!(
            WasmTransitionLimits::for_cold_file_bytes(0).total_deadline_nanoseconds,
            WasmTransitionLimits::default().total_deadline_nanoseconds
        );
        assert_eq!(
            WasmTransitionLimits::for_cold_file_bytes(10 * MIB).total_deadline_nanoseconds,
            15_000_000_000
        );
        assert_eq!(
            WasmTransitionLimits::for_cold_file_bytes(128 * MIB).total_deadline_nanoseconds,
            COLD_FILE_MAX_DEADLINE_NANOSECONDS
        );
    }

    #[test]
    fn cold_file_page_fits_one_base64_encoded_source_map_line() {
        const OPENCLAW_SOURCE_MAP_BYTES: u64 = 5_298_078;
        let limits = WasmTransitionLimits::for_cold_file_bytes(OPENCLAW_SOURCE_MAP_BYTES);
        let encoded_line_bytes = OPENCLAW_SOURCE_MAP_BYTES.saturating_mul(4).div_ceil(3);

        assert!(u64::from(limits.max_record_bytes) > encoded_line_bytes);
        assert_eq!(limits.max_record_bytes, limits.max_page_bytes);
        assert!(u64::from(limits.max_page_bytes) <= COLD_TRANSITION_MAX_PAGE_BYTES);
        limits
            .validate()
            .expect("scaled cold limits should validate");
    }

    #[test]
    fn warm_file_page_also_fits_one_base64_encoded_source_map_line() {
        const OPENCLAW_SOURCE_MAP_BYTES: u64 = 5_298_078;
        let limits = WasmTransitionLimits::for_file_bytes(OPENCLAW_SOURCE_MAP_BYTES);
        let encoded_line_bytes = OPENCLAW_SOURCE_MAP_BYTES.saturating_mul(4).div_ceil(3);

        assert!(u64::from(limits.max_record_bytes) > encoded_line_bytes);
        assert_eq!(
            limits.total_deadline_nanoseconds,
            WasmTransitionLimits::default().total_deadline_nanoseconds
        );
        limits
            .validate()
            .expect("scaled warm limits should validate");
    }

    #[test]
    fn change_drain_validator_owns_framing_but_not_key_copies() {
        let key = WasmRowKey::from_typed_parts(
            "csv_row",
            [0; 32],
            vec![lix_schema::Value::Text("row".to_owned())],
        )
        .unwrap();
        let page = WasmChangePage {
            format_version: CURRENT_PACKET_FORMAT,
            changes: WasmRowChanges {
                changes: vec![WasmRowChange::Delete(key)],
            },
            outputs: None,
        };
        let mut validator = WasmChangeDrainValidator::new(WasmTransitionLimits::default()).unwrap();
        validator.accept_page(&page).unwrap();
        validator.accept_page(&page).unwrap();
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
    fn edit_drain_validation_charges_metadata_to_record_and_page_limits() {
        let inline = |offset, bytes: &[u8]| WasmOutputSplice {
            offset,
            delete_len: 0,
            insert: WasmGuestBytes::Inline(bytes.to_vec().into()),
        };

        let record_limits = WasmTransitionLimits {
            max_record_bytes: u32::try_from(EDIT_SPLICE_METADATA_BYTES)
                .expect("edit splice metadata size should fit u32"),
            ..WasmTransitionLimits::default()
        };
        let mut validator = WasmEditDrainValidator::new(0, record_limits).unwrap();
        assert!(
            validator
                .accept_page(&WasmEditPage {
                    edits: vec![inline(0, b"x")],
                    outputs: None,
                })
                .expect_err("metadata plus inline bytes must fit one record")
                .message
                .contains("max_record_bytes")
        );

        let output_limits = WasmTransitionLimits {
            max_record_bytes: u32::try_from(EDIT_SPLICE_METADATA_BYTES - 1)
                .expect("edit splice metadata size should fit u32"),
            ..WasmTransitionLimits::default()
        };
        let mut validator = WasmEditDrainValidator::new(0, output_limits).unwrap();
        assert!(
            validator
                .accept_page(&WasmEditPage {
                    edits: vec![WasmOutputSplice {
                        offset: 0,
                        delete_len: 0,
                        insert: WasmGuestBytes::Output(WasmOutputRange {
                            index: 0,
                            offset: 0,
                            length: 1,
                        }),
                    }],
                    outputs: Some(WasmByteOutputsHandle(1)),
                })
                .expect_err("output-backed edits still pay fixed record metadata")
                .message
                .contains("max_record_bytes")
        );

        let page_limits = WasmTransitionLimits {
            max_record_bytes: 30,
            max_page_bytes: 49,
            ..WasmTransitionLimits::default()
        };
        let mut validator = WasmEditDrainValidator::new(1, page_limits).unwrap();
        assert!(
            validator
                .accept_page(&WasmEditPage {
                    edits: vec![inline(0, b"x"), inline(1, b"y")],
                    outputs: None,
                })
                .expect_err("the page pays metadata for every edit")
                .message
                .contains("max_page_bytes")
        );
    }

    #[test]
    fn production_wit_is_versioned_and_row_first() {
        let wit = include_str!("../../../wit/lix-plugin.wit");
        assert!(wit.starts_with("package lix:plugin@2.0.0;"));
        assert!(wit.contains("resource transition"));
        assert!(wit.contains("interface column-merger"));
        assert!(wit.contains("interface file-projection"));
        assert!(wit.contains("parse-changes: func("));
        assert!(wit.contains("serialize-changes: func("));
        assert!(wit.contains("world column-merger-plugin"));
        assert!(wit.contains("world file-projection-plugin"));
        assert!(!wit.contains("resolve-conflicts"));
    }
}
