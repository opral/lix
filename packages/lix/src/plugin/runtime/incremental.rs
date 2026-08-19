//! Engine-side plumbing for incremental Wasm Component transitions.
//!
//! This module owns validation and bounded host adapters around the host-
//! neutral `wasm` traits. It deliberately does not decide transaction,
//! conflict-resolution, observation, or actor-publication policy.

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use tracing::Instrument as _;

use crate::catalog::{CatalogSnapshot, SchemaPlan};
use crate::common::RequestBlobSpliceProvenance;
use crate::hot_state::MaterializedHotStateBatch;
use crate::plugin::runtime::{
    CURRENT_PACKET_FORMAT, EDIT_SPLICE_METADATA_BYTES, WasmByteOutputsHandle, WasmByteSource,
    WasmChangeDrainValidator, WasmChangePage, WasmColumnMergePage, WasmColumnMergeResult,
    WasmColumnMergeSource, WasmColumnMergeTransition, WasmComponentActor, WasmDocumentHandle,
    WasmEditDrainValidator, WasmEditPage, WasmFileTransition, WasmGuestBytes, WasmGuestColumnValue,
    WasmGuestRowPayload, WasmHostBytes, WasmHostColumnMerge, WasmHostRow, WasmHostRowChanges,
    WasmInputBytes, WasmInputSplice, WasmOutputRange, WasmRow, WasmRowChange, WasmRowChangeSource,
    WasmRowChanges, WasmRowKey, WasmRowPage, WasmRowSource, WasmRowTransition, WasmSourceRange,
    WasmTransitionCounters, WasmTransitionHandle, WasmTransitionLimits, WasmTypedRow,
    validate_change_cursor_key_uniqueness,
};
use crate::plugin::wire::typed as typed_wire;
use crate::{Blob, LixError};

/// Exact SHA-256 identity for one actor-owned byte version.
///
/// Actor versions cache this value so trusted transport provenance can be
/// authorized without rescanning a large accepted document on every edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileBytesSha256([u8; 32]);

impl FileBytesSha256 {
    pub(crate) fn compute(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub(crate) fn from_lower_hex(value: &str) -> Option<Self> {
        if value.len() != 64 {
            return None;
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_nibble(pair[0])?;
            let low = lower_hex_nibble(pair[1])?;
            bytes[index] = (high << 4) | low;
        }
        Some(Self(bytes))
    }

    pub(crate) fn matches_lower_hex(self, value: &str) -> bool {
        Self::from_lower_hex(value) == Some(self)
    }
}

fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Immutable contiguous bytes with shared ownership and observable range-read
/// counters. Clones share both the allocation and counters.
#[derive(Debug, Clone)]
pub(crate) struct ArcByteSource {
    bytes: Blob,
    reads: Arc<ArcByteSourceCounters>,
}

#[derive(Debug, Default)]
struct ArcByteSourceCounters {
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl ArcByteSource {
    pub(crate) fn new(bytes: Blob) -> Self {
        Self {
            bytes,
            reads: Arc::new(ArcByteSourceCounters::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        Self::new(bytes.into())
    }

    #[cfg(test)]
    pub(crate) fn counters(&self) -> WasmTransitionCounters {
        WasmTransitionCounters {
            source_read_calls: self.reads.calls.load(Ordering::Relaxed),
            source_bytes_read: self.reads.bytes.load(Ordering::Relaxed),
            ..WasmTransitionCounters::default()
        }
    }
}

impl WasmByteSource for ArcByteSource {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
        if length == 0 {
            return Err(invalid_input(
                "component byte-source reads must request bytes",
            ));
        }
        let start = usize::try_from(offset)
            .map_err(|_| invalid_input("component byte-source offset does not fit this host"))?;
        if start > self.bytes.len() {
            return Err(invalid_input(
                "component byte-source offset is out of bounds",
            ));
        }
        let end = start.saturating_add(length as usize).min(self.bytes.len());
        let result = self.bytes[start..end].to_vec();
        self.reads.calls.fetch_add(1, Ordering::Relaxed);
        self.reads
            .bytes
            .fetch_add(result.len() as u64, Ordering::Relaxed);
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuiltInputSplices {
    pub(crate) edits: Vec<WasmInputSplice>,
    pub(crate) used_transport_provenance: bool,
    /// Exact identity of `after` when verified transport already supplied it.
    ///
    /// Ordinary full-byte SQL writes do not need a protocol SHA-256. Keeping
    /// this optional avoids hashing a large accepted file solely in case a
    /// later request happens to carry transport splice provenance.
    pub(crate) after_sha256: Option<FileBytesSha256>,
    /// Bytes examined only by the bounded host full-diff fallback.
    pub(crate) full_diff_bytes_compared: u64,
}

impl BuiltInputSplices {
    /// Returns one host-verified base-relative replacement for storage reuse.
    ///
    /// This is format-neutral transition evidence. The WIT input remains the
    /// ordinary splice vector, and callers still retain the complete accepted
    /// output bytes as authority.
    pub(crate) fn replacement(&self) -> Option<(usize, usize, usize)> {
        let [splice] = self.edits.as_slice() else {
            return None;
        };
        let insert_len = match &splice.insert {
            WasmInputBytes::Inline(bytes) => bytes.len(),
            WasmInputBytes::AfterRange(range) => usize::try_from(range.length).ok()?,
        };
        Some((
            usize::try_from(splice.offset).ok()?,
            usize::try_from(splice.delete_len).ok()?,
            insert_len,
        ))
    }

    /// Returns the one fixed-width base-relative replacement, if this update
    /// can retain its prior binary-CAS chunk boundaries. This is private host
    /// staging information; the WIT input remains the ordinary splice vector.
    pub(crate) fn same_length_replacement(&self) -> Option<(usize, usize)> {
        let (offset, delete_len, insert_len) = self.replacement()?;
        (delete_len == insert_len && delete_len != 0).then_some((offset, delete_len))
    }
}

/// Builds one coalesced base-relative replacement. Protocol-verified transport
/// provenance preserves the exact remote splice only when its base digest
/// names `before`. Without that proof, the fallback compares the full
/// before/after blobs once to find their maximal common prefix/suffix.
pub(crate) fn build_file_update_splices(
    before: &[u8],
    before_sha256: impl Into<Option<FileBytesSha256>>,
    after: &[u8],
    provenance: Option<&RequestBlobSpliceProvenance>,
    limits: WasmTransitionLimits,
) -> Result<BuiltInputSplices, LixError> {
    limits.validate()?;
    let before_sha256 = before_sha256.into();
    let verified_transport = provenance.and_then(|provenance| {
        let provenance_base = FileBytesSha256::from_lower_hex(provenance.base_sha256())?;
        let provenance_result = FileBytesSha256::from_lower_hex(provenance.result_sha256())?;
        let before_sha256 = before_sha256.unwrap_or_else(|| FileBytesSha256::compute(before));
        (provenance_base == before_sha256
            && provenance.matches_result(after)
            && validate_transport_splice(before, after, provenance).is_ok())
        .then_some((provenance, provenance_result))
    });

    let (prefix, suffix, insert, used_transport_provenance, after_sha256, compared) =
        if let Some((provenance, result_sha256)) = verified_transport {
            (
                provenance.prefix_bytes(),
                provenance.suffix_bytes(),
                provenance.insert(),
                true,
                Some(result_sha256),
                0,
            )
        } else {
            let (prefix, prefix_bytes_compared) = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_splice_prefix"
            )
            .in_scope(|| common_prefix_len(before, after));
            if prefix == before.len() && prefix == after.len() {
                return Ok(BuiltInputSplices {
                    edits: Vec::new(),
                    used_transport_provenance: false,
                    after_sha256: before_sha256,
                    full_diff_bytes_compared: 0,
                });
            }
            let max_suffix = before
                .len()
                .saturating_sub(prefix)
                .min(after.len().saturating_sub(prefix));
            let (suffix, suffix_bytes_compared) = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_splice_suffix"
            )
            .in_scope(|| common_suffix_len(before, after, max_suffix));
            let insert_end = after.len() - suffix;
            (
                prefix,
                suffix,
                &after[prefix..insert_end],
                false,
                None,
                prefix_bytes_compared.saturating_add(suffix_bytes_compared),
            )
        };

    let delete_len = before
        .len()
        .checked_sub(prefix)
        .and_then(|length| length.checked_sub(suffix))
        .ok_or_else(|| invalid_input("component splice deletion length underflowed"))?;
    if delete_len == 0 && insert.is_empty() {
        return Ok(BuiltInputSplices {
            edits: Vec::new(),
            used_transport_provenance,
            after_sha256,
            full_diff_bytes_compared: compared,
        });
    }
    let insert = if insert.len() as u64 <= limits.max_inline_input_bytes {
        WasmInputBytes::Inline(insert.to_vec())
    } else {
        WasmInputBytes::AfterRange(WasmSourceRange {
            offset: prefix as u64,
            length: insert.len() as u64,
        })
    };
    let edits = vec![WasmInputSplice {
        offset: prefix as u64,
        delete_len: delete_len as u64,
        insert,
    }];
    Ok(BuiltInputSplices {
        edits,
        used_transport_provenance,
        after_sha256,
        full_diff_bytes_compared: compared,
    })
}

/// Proves that a trusted transport splice preserves a previously established
/// UTF-8 document invariant without rescanning unchanged bytes.
///
/// At most the inserted range plus three bytes on either side are inspected.
/// A false result is deliberately inconclusive: callers fall back to the full
/// classifier, which handles malformed provenance and awkward code-point
/// boundaries without weakening selection correctness.
pub(crate) fn transport_splice_preserves_utf8(
    after: &[u8],
    provenance: &RequestBlobSpliceProvenance,
) -> bool {
    const UTF8_BOUNDARY_CONTEXT: usize = 3;

    if !provenance.matches_result(after) {
        return false;
    }
    let prefix = provenance.prefix_bytes();
    let suffix = provenance.suffix_bytes();
    let Some(insert_end) = prefix.checked_add(provenance.insert().len()) else {
        return false;
    };
    let Some(expected_after_len) = insert_end.checked_add(suffix) else {
        return false;
    };
    if expected_after_len != after.len()
        || prefix > after.len()
        || insert_end > after.len()
        || provenance.insert() != &after[prefix..insert_end]
    {
        return false;
    }

    let window_start = prefix.saturating_sub(UTF8_BOUNDARY_CONTEXT);
    let window_end = insert_end
        .saturating_add(UTF8_BOUNDARY_CONTEXT)
        .min(after.len());
    std::str::from_utf8(&after[window_start..window_end]).is_ok()
}

/// Proves that a trusted transport splice preserves a bounded prefix-exclusion
/// predicate without rescanning unchanged bytes.
pub(crate) fn transport_splice_preserves_prefix_exclusion(
    after: &[u8],
    provenance: &RequestBlobSpliceProvenance,
    forbidden_byte: u8,
    scan_bytes: usize,
) -> bool {
    if !provenance.matches_result(after) {
        return false;
    }
    let prefix = provenance.prefix_bytes();
    let suffix = provenance.suffix_bytes();
    let Some(insert_end) = prefix.checked_add(provenance.insert().len()) else {
        return false;
    };
    let Some(expected_after_len) = insert_end.checked_add(suffix) else {
        return false;
    };
    if expected_after_len != after.len()
        || prefix > after.len()
        || insert_end > after.len()
        || provenance.insert() != &after[prefix..insert_end]
    {
        return false;
    }

    !after[..after.len().min(scan_bytes)].contains(&forbidden_byte)
}

fn validate_transport_splice(
    before: &[u8],
    after: &[u8],
    provenance: &RequestBlobSpliceProvenance,
) -> Result<(), LixError> {
    if !provenance.matches_result(after) {
        return Err(invalid_input(
            "transport splice metadata is not bound to the submitted result blob",
        ));
    }
    let prefix = provenance.prefix_bytes();
    let suffix = provenance.suffix_bytes();
    if prefix > before.len()
        || suffix > before.len()
        || prefix.saturating_add(suffix) > before.len()
        || prefix > after.len()
        || suffix > after.len()
        || prefix.saturating_add(suffix) > after.len()
    {
        return Err(invalid_input(
            "transport splice prefix and suffix are out of bounds",
        ));
    }
    let expected_after_len = prefix
        .checked_add(provenance.insert().len())
        .and_then(|length| length.checked_add(suffix))
        .ok_or_else(|| invalid_input("transport splice result length overflowed"))?;
    // This sidecar is constructed only after the Lix Server Protocol verifies the
    // result hash and reconstructs the ordinary SQL blob. The caller has
    // separately matched the base hash to the actor's exact observed version;
    // rechecking unchanged prefix/suffix here would turn a localized edit back
    // into an O(document) scan. Bounds, result length, and the small inserted
    // range remain cheap defense-in-depth checks.
    if expected_after_len != after.len()
        || provenance.insert() != &after[prefix..after.len() - suffix]
    {
        return Err(invalid_input(
            "transport splice metadata does not match the accepted before/after bytes",
        ));
    }
    Ok(())
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> (usize, u64) {
    const WORD_BYTES: usize = size_of::<u64>();
    let max = left.len().min(right.len());
    let mut common = 0usize;
    let mut bytes_compared = 0u64;

    while common + WORD_BYTES <= max {
        let left_word = u64::from_le_bytes(
            left[common..common + WORD_BYTES]
                .try_into()
                .expect("fixed-size prefix word"),
        );
        let right_word = u64::from_le_bytes(
            right[common..common + WORD_BYTES]
                .try_into()
                .expect("fixed-size prefix word"),
        );
        let different = left_word ^ right_word;
        bytes_compared = bytes_compared.saturating_add((WORD_BYTES * 2) as u64);
        if different != 0 {
            common += (different.trailing_zeros() / u8::BITS) as usize;
            return (common, bytes_compared);
        }
        common += WORD_BYTES;
    }

    while common < max {
        bytes_compared = bytes_compared.saturating_add(2);
        if left[common] != right[common] {
            break;
        }
        common += 1;
    }
    (common, bytes_compared)
}

fn common_suffix_len(left: &[u8], right: &[u8], max: usize) -> (usize, u64) {
    const WORD_BYTES: usize = size_of::<u64>();
    let mut common = 0usize;
    let mut bytes_compared = 0u64;

    while common + WORD_BYTES <= max {
        let left_end = left.len() - common;
        let right_end = right.len() - common;
        let left_word = u64::from_le_bytes(
            left[left_end - WORD_BYTES..left_end]
                .try_into()
                .expect("fixed-size suffix word"),
        );
        let right_word = u64::from_le_bytes(
            right[right_end - WORD_BYTES..right_end]
                .try_into()
                .expect("fixed-size suffix word"),
        );
        let different = left_word ^ right_word;
        bytes_compared = bytes_compared.saturating_add((WORD_BYTES * 2) as u64);
        if different != 0 {
            common += (different.leading_zeros() / u8::BITS) as usize;
            return (common, bytes_compared);
        }
        common += WORD_BYTES;
    }

    while common < max {
        bytes_compared = bytes_compared.saturating_add(2);
        if left[left.len() - common - 1] != right[right.len() - common - 1] {
            break;
        }
        common += 1;
    }
    (common, bytes_compared)
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaAllowlist {
    schema_keys: BTreeSet<String>,
    catalog: Option<Arc<CatalogSnapshot>>,
}

impl SchemaAllowlist {
    pub(crate) fn new(schema_keys: impl IntoIterator<Item = String>) -> Result<Self, LixError> {
        let schema_keys = schema_keys.into_iter().collect::<BTreeSet<_>>();
        if schema_keys.is_empty() {
            return Err(invalid_input(
                "component schema allowlist must not be empty",
            ));
        }
        Ok(Self {
            schema_keys,
            catalog: None,
        })
    }

    pub(crate) fn from_slice(schema_keys: &[String]) -> Result<Self, LixError> {
        Self::new(schema_keys.iter().cloned())
    }

    pub(crate) fn from_catalog(
        schema_keys: &[String],
        catalog: Arc<CatalogSnapshot>,
    ) -> Result<Self, LixError> {
        let mut allowlist = Self::from_slice(schema_keys)?;
        allowlist.catalog = Some(catalog);
        Ok(allowlist)
    }

    fn validate(&self, schema_key: &str) -> Result<(), LixError> {
        if !self.schema_keys.contains(schema_key) {
            return Err(invalid_guest(format!(
                "component plugin emitted undeclared schema '{schema_key}'"
            )));
        }
        Ok(())
    }

    fn schema_plan(&self, schema_key: &str) -> Option<&SchemaPlan> {
        self.catalog
            .as_deref()?
            .plan_for_key(schema_key)
            .map(|(_, plan)| plan)
    }

    fn validate_typed_row(&self, schema_key: &str, row: &WasmTypedRow) -> Result<(), LixError> {
        let Some(plan) = self.schema_plan(schema_key) else {
            // Unit-level runtime tests can intentionally omit a catalog. The
            // allowlist still authenticates the schema key in that mode; the
            // catalog-backed production path performs the full typed check.
            if row.row.is_empty() {
                return Err(invalid_guest("typed row must contain at least one column"));
            }
            return Ok(());
        };
        if !plan.fingerprint().matches_bytes(&row.schema_fingerprint) {
            return Err(invalid_guest(format!(
                "typed row fingerprint does not match schema '{schema_key}'"
            )));
        }
        let primary_key = plan.compiled_schema.primary_key();
        if row.row_pk.is_empty() {
            // A create may omit its defaulted identity. The create context
            // materializer supplies that typed value before staging.
            plan.compiled_schema
                .validate_create_row(&row.row)
                .map_err(|error| invalid_guest(format!("typed create row is invalid: {error}")))?;
            row.certify_boundary_create_validation();
            return Ok(());
        }
        plan.compiled_schema
            .validate_complete_row(&row.row)
            .map_err(|error| invalid_guest(format!("typed row is incomplete: {error}")))?;
        if row.row_pk.len() != primary_key.len() {
            return Err(invalid_guest(format!(
                "typed row identity for schema '{schema_key}' has the wrong component count"
            )));
        }
        for (column, expected) in primary_key.iter().zip(row.row_pk.iter()) {
            let actual = row.row.get(column).ok_or_else(|| {
                invalid_guest(format!("typed row identity column '{column}' is missing"))
            })?;
            if actual != expected {
                return Err(invalid_guest(format!(
                    "typed row identity does not match column '{column}'"
                )));
            }
        }
        if !plan.compiled_schema.defaults_would_apply(&row.row) {
            row.certify_boundary_validation().map_err(|error| {
                invalid_guest(format!("typed row is not durably encodable: {error:?}"))
            })?;
        }
        Ok(())
    }
}

pub(crate) struct VecRowSource {
    rows: VecDeque<WasmHostRow>,
    state: VecSourceState,
}

impl VecRowSource {
    pub(crate) fn new(
        rows: Vec<WasmHostRow>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        validate_row_order(&rows)?;
        for row in &rows {
            validate_host_row(row)?;
        }
        Ok(Self {
            rows: rows.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmRowSource for VecRowSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmRowPage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.rows.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut rows: Vec<WasmHostRow> = Vec::new();
        while let Some(row) = self.rows.front() {
            if let Some(first) = rows.first()
                && (first.key.schema_key != row.key.schema_key
                    || first.key.schema_fingerprint != row.key.schema_fingerprint)
            {
                break;
            }
            let record_bytes = encoded_row_record_bytes(row)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component row record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component row frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if rows.is_empty() {
                    return Err(invalid_input(
                        "component row record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            rows.push(self.rows.pop_front().expect("front row was just inspected"));
        }
        self.state.accept_page(page_bytes, 0)?;
        Ok(Some(WasmRowPage { rows }))
    }
}

/// Page-lazy complete-row source backed by the engine's columnar live-state
/// batch. This keeps shared snapshot buffers in their storage-native owner and
/// constructs generic Wasm rows only for the page currently crossing the
/// component boundary.
#[derive(Debug)]
pub(crate) struct LiveBatchRowSource {
    rows: MaterializedHotStateBatch,
    ordinals: VecDeque<u32>,
    pending: Option<WasmHostRow>,
    state: VecSourceState,
}

impl LiveBatchRowSource {
    pub(crate) fn new_typed(
        rows: MaterializedHotStateBatch,
        ordinals: Vec<u32>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        for pair in ordinals.windows(2) {
            let left = rows.row(pair[0] as usize);
            let right = rows.row(pair[1] as usize);
            if left.schema_key() == right.schema_key() && left.row_pk() == right.row_pk() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "durable component row hydration returned duplicate keys",
                ));
            }
        }
        Ok(Self {
            rows,
            ordinals: ordinals.into(),
            pending: None,
            state: VecSourceState::new(limits)?,
        })
    }

    fn next_row(&mut self) -> Result<Option<WasmHostRow>, LixError> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        let Some(ordinal) = self.ordinals.pop_front() else {
            return Ok(None);
        };
        let row = self.rows.get(ordinal as usize).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin state selection references a row outside its batch owner",
            )
        })?;
        if let Some(typed) = row.typed_snapshot() {
            return Ok(Some(WasmRow {
                key: WasmRowKey::from_typed_parts(
                    row.schema_key().to_owned(),
                    typed.schema_fingerprint,
                    typed.row_pk.clone(),
                )?,
                payload: WasmHostBytes::Typed(Arc::clone(typed)),
            }));
        }
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "plugin state row '{}' has no native typed payload",
                row.schema_key()
            ),
        ))
    }
}

impl WasmRowSource for LiveBatchRowSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmRowPage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        let mut page_bytes = 0_u64;
        let mut rows: Vec<WasmHostRow> = Vec::new();
        while let Some(row) = self.next_row()? {
            let record_bytes = encoded_row_record_bytes(&row)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component row record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component row frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if rows.is_empty() {
                    return Err(invalid_input(
                        "component row record does not fit the requested page",
                    ));
                }
                self.pending = Some(row);
                break;
            }
            if let Some(first) = rows.first()
                && (first.key.schema_key != row.key.schema_key
                    || first.key.schema_fingerprint != row.key.schema_fingerprint)
            {
                self.pending = Some(row);
                break;
            }
            page_bytes += framed_bytes;
            rows.push(row);
        }
        if rows.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }
        self.state.accept_page(page_bytes, 0)?;
        Ok(Some(WasmRowPage { rows }))
    }
}

/// Vec-backed source for the final resolved semantic changes supplied to
/// `rows_changed`.
#[derive(Debug)]
pub(crate) struct VecRowChangeSource {
    changes: VecDeque<WasmRowChange<WasmHostBytes>>,
    state: VecSourceState,
}

impl VecRowChangeSource {
    pub(crate) fn new(
        changes: WasmHostRowChanges,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        changes.validate()?;
        validate_change_order(&changes.changes)?;
        for change in &changes.changes {
            match change {
                WasmRowChange::Create { .. } => {
                    return Err(invalid_input(
                        "host-to-guest row changes cannot contain keyless creates",
                    ));
                }
                WasmRowChange::Upsert { row, .. } => validate_host_row(row)?,
                WasmRowChange::Delete(key) if key.row_pk.is_empty() => {
                    return Err(invalid_input(
                        "component row primary keys must not be empty",
                    ));
                }
                WasmRowChange::Delete(_) => {}
            }
        }
        Ok(Self {
            changes: changes.changes.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmRowChangeSource for VecRowChangeSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmHostRowChanges>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.changes.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut changes: Vec<WasmRowChange<WasmHostBytes>> = Vec::new();
        while let Some(change) = self.changes.front() {
            let change_key = change.row_key().ok_or_else(|| {
                invalid_input("host-to-guest row changes require a resolved typed key")
            })?;
            if let Some(first_key) = changes.first().and_then(WasmRowChange::row_key)
                && (first_key.schema_key != change_key.schema_key
                    || first_key.schema_fingerprint != change_key.schema_fingerprint)
            {
                break;
            }
            let record_bytes = encoded_row_change_record_bytes(change)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component change record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component change frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if changes.is_empty() {
                    return Err(invalid_input(
                        "component change record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            changes.push(
                self.changes
                    .pop_front()
                    .expect("front change was just inspected"),
            );
        }
        self.state.accept_page(page_bytes, 0)?;
        Ok(Some(WasmRowChanges { changes }))
    }
}

#[derive(Debug)]
pub(crate) struct VecColumnMergeSource {
    merges: VecDeque<WasmHostColumnMerge>,
    state: VecSourceState,
}

impl VecColumnMergeSource {
    pub(crate) fn new(
        merges: Vec<WasmHostColumnMerge>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        for (expected, merge) in merges.iter().enumerate() {
            if merge.ordinal
                != u32::try_from(expected)
                    .map_err(|_| invalid_input("column merge source exceeds u32"))?
                || merge.key.row_pk.is_empty()
                || merge.column.is_empty()
            {
                return Err(invalid_input(
                    "column merge source requires contiguous ordinals, a row key, and a column",
                ));
            }
            if merge.schema_fingerprint != merge.base_row.schema_fingerprint
                || merge.schema_fingerprint != merge.a_row.schema_fingerprint
                || merge.schema_fingerprint != merge.b_row.schema_fingerprint
            {
                return Err(invalid_input(
                    "column merge rows must share the declared schema fingerprint",
                ));
            }
            for row in [&merge.base_row, &merge.a_row, &merge.b_row] {
                if row.row.is_empty() || row.row_pk.is_empty() {
                    return Err(invalid_input(
                        "column merge context rows must contain typed identity and values",
                    ));
                }
            }
        }
        Ok(Self {
            merges: merges.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmColumnMergeSource for VecColumnMergeSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmColumnMergePage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.merges.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }
        let mut page_bytes = 0u64;
        let mut page_refs = 0u32;
        let mut merges = Vec::new();
        while let Some(merge) = self.merges.front() {
            // Values and context rows stay host-side and are exposed through
            // range reads. Only metadata references are admitted to this page.
            let record_bytes = encoded_row_key_bytes(&merge.key)?
                .saturating_add(merge.column.len() as u64)
                .saturating_add(merge.file_id.as_ref().map_or(0, |id| id.len()) as u64)
                .saturating_add(64);
            let record_refs = 0u32;
            if page_bytes.saturating_add(record_bytes) > page_limit {
                if merges.is_empty() {
                    return Err(invalid_input(
                        "column merge record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes = page_bytes.saturating_add(record_bytes);
            page_refs = page_refs.saturating_add(record_refs);
            merges.push(self.merges.pop_front().expect("front merge was inspected"));
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmColumnMergePage { merges }))
    }
}

#[derive(Debug)]
struct VecSourceState {
    limits: WasmTransitionLimits,
    pages: u32,
    total_inline_bytes: u64,
    attachment_refs: u32,
    reached_eof: bool,
}

impl VecSourceState {
    fn new(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            pages: 0,
            total_inline_bytes: 0,
            attachment_refs: 0,
            reached_eof: false,
        })
    }

    fn page_limit(&self, requested: u32) -> Result<u64, LixError> {
        if requested == 0 {
            return Err(invalid_input(
                "component packet source page size must be positive",
            ));
        }
        // `max-bytes` is an upper bound supplied by the consumer, not an
        // exact-size demand. A source with a smaller fixed schedule remains
        // valid and must simply return a smaller page.
        Ok(u64::from(requested.min(self.limits.max_page_bytes)))
    }

    fn accept_page(&mut self, inline_bytes: u64, refs: u32) -> Result<(), LixError> {
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_input("component packet source page count overflowed"))?;
        self.total_inline_bytes = self
            .total_inline_bytes
            .checked_add(inline_bytes)
            .ok_or_else(|| invalid_input("component packet source byte count overflowed"))?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(refs)
            .ok_or_else(|| invalid_input("component packet source attachment count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_input("component packet source exceeds max_pages"));
        }
        if self.total_inline_bytes > self.limits.max_total_bytes {
            return Err(invalid_input(
                "component packet source exceeds max_total_bytes",
            ));
        }
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_input(
                "component packet source exceeds max_attachment_refs",
            ));
        }
        Ok(())
    }
}

fn validate_row_order(rows: &[WasmHostRow]) -> Result<(), LixError> {
    for pair in rows.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(invalid_input(
                "component complete row sources must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_change_order<B>(changes: &[WasmRowChange<B>]) -> Result<(), LixError> {
    if changes
        .iter()
        .any(|change| matches!(change, WasmRowChange::Create { .. }))
    {
        return Ok(());
    }
    for pair in changes.windows(2) {
        if pair[0].row_key() >= pair[1].row_key() {
            return Err(invalid_input(
                "component row changes must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_host_row(row: &WasmHostRow) -> Result<(), LixError> {
    if row.key.row_pk.is_empty() {
        return Err(invalid_input(
            "component row primary keys must not be empty",
        ));
    }
    if !matches!(row.payload, WasmHostBytes::Typed(_)) {
        return Err(invalid_input(
            "component host rows must use native typed-row payloads",
        ));
    }
    Ok(())
}

fn encoded_row_record_bytes(row: &WasmHostRow) -> Result<u64, LixError> {
    row.key
        .row_pk
        .iter()
        .try_fold(row.key.schema_key.len() as u64 + 32, |size, component| {
            let encoded_len = typed_wire::encoded_key_value_size(component)
                .map_err(|_| invalid_input("component row key contains an invalid value"))?;
            size.checked_add(encoded_len as u64)
                .ok_or_else(|| invalid_input("component row record size overflowed"))
        })?
        .checked_add(encoded_host_bytes_ref_bytes(&row.payload)?)
        .ok_or_else(|| invalid_input("component row record size overflowed"))
}

fn encoded_row_change_record_bytes(change: &WasmRowChange<WasmHostBytes>) -> Result<u64, LixError> {
    if let WasmRowChange::Create {
        schema_key,
        payload,
        ..
    } = change
    {
        let snapshot_bytes = encoded_host_bytes_ref_bytes(payload)?;
        return (schema_key.len() as u64)
            .checked_add(32)
            .and_then(|size| size.checked_add(snapshot_bytes))
            .ok_or_else(|| invalid_input("component create record size overflowed"));
    }
    let key = change.row_key().expect("non-create change has a row key");
    let mut size =
        key.row_pk
            .iter()
            .try_fold(key.schema_key.len() as u64 + 32, |size, component| {
                let encoded_len = typed_wire::encoded_key_value_size(component)
                    .map_err(|_| invalid_input("component row key contains an invalid value"))?;
                size.checked_add(encoded_len as u64)
                    .ok_or_else(|| invalid_input("component change record size overflowed"))
            })?;
    if let WasmRowChange::Upsert { row, .. } = change {
        size = size
            .checked_add(encoded_host_bytes_ref_bytes(&row.payload)?)
            .ok_or_else(|| invalid_input("component change record size overflowed"))?;
    }
    Ok(size)
}

fn encoded_row_key_bytes(key: &WasmRowKey) -> Result<u64, LixError> {
    if key.row_pk.is_empty() {
        return Err(invalid_input(
            "component row primary keys must not be empty",
        ));
    }
    let _ = u32::try_from(key.row_pk.len())
        .map_err(|_| invalid_input("component row primary key has too many components"))?;
    let mut size = encoded_text_bytes(&key.schema_key)?
        .checked_add(32 + 4)
        .ok_or_else(|| invalid_input("component row key size overflowed"))?;
    for component in key.row_pk.iter() {
        let component_len = typed_wire::encoded_key_value_size(component)
            .map_err(|_| invalid_input("component row key contains an invalid value"))?;
        size = size
            .checked_add(4 + component_len as u64)
            .ok_or_else(|| invalid_input("component row key size overflowed"))?;
    }
    Ok(size)
}

fn encoded_text_bytes(value: &str) -> Result<u64, LixError> {
    let length = u32::try_from(value.len())
        .map_err(|_| invalid_input("component packet text exceeds u32 framing"))?;
    Ok(u64::from(length) + 4)
}

fn encoded_host_bytes_ref_bytes(value: &WasmHostBytes) -> Result<u64, LixError> {
    match value {
        WasmHostBytes::Typed(row) => Ok(1 + 4 + row.estimated_size()),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedFileTransition {
    pub(crate) document: WasmDocumentHandle,
    pub(crate) changes: WasmHostRowChanges,
    pub(crate) replace_all_rows: bool,
    pub(crate) counters: WasmTransitionCounters,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedColumnMergeTransition {
    pub(crate) results: Vec<WasmColumnMergeResult<lix_schema::Value>>,
    pub(crate) counters: WasmTransitionCounters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOutputSplice {
    pub(crate) offset: u64,
    pub(crate) delete_len: u64,
    pub(crate) insert: Bytes,
}

/// One fixed-width renderer splice which the host has applied against the
/// accepted actor bytes.
///
/// This is deliberately not guest authority. It is created only after the
/// complete renderer output has passed host range/order validation and has
/// been reconstructed from the immutable base. Transaction code may pair it
/// with an independently loaded durable blob hash as a private CAS hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ValidatedSameLengthOutputSplice {
    pub(crate) offset: usize,
    pub(crate) length: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedRowTransition {
    pub(crate) document: WasmDocumentHandle,
    pub(crate) bytes: Blob,
    pub(crate) bytes_sha256: Option<FileBytesSha256>,
    /// Present only for one non-empty fixed-width edit that the host applied
    /// to the complete accepted base. It carries no inserted guest bytes and
    /// is not persisted.
    pub(crate) same_length_output_splice: Option<ValidatedSameLengthOutputSplice>,
    #[cfg(test)]
    pub(crate) edits: Vec<ResolvedOutputSplice>,
    pub(crate) counters: WasmTransitionCounters,
}

/// Drains and validates every change before returning any proposed semantic
/// state to transaction code. Validation of a page's key shape, attachment
/// count, and aggregate budget happens before the first attachment method is
/// invoked. Cursor-wide key uniqueness is checked once over borrowed references
/// after the final stable change vector has been assembled.
pub(crate) async fn drain_file_transition_changes(
    actor: &mut dyn WasmComponentActor,
    transition: WasmFileTransition,
    _creates: crate::plugin::runtime::WasmCreateContext,
    schemas: &SchemaAllowlist,
    limits: WasmTransitionLimits,
) -> Result<ValidatedFileTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_file_transition_changes_inner(actor, transition, schemas, limits).await {
        Ok(validated) => Ok(validated),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

async fn drain_file_transition_changes_inner(
    actor: &mut dyn WasmComponentActor,
    transition: WasmFileTransition,
    schemas: &SchemaAllowlist,
    limits: WasmTransitionLimits,
) -> Result<ValidatedFileTransition, LixError> {
    let mut validator = WasmChangeDrainValidator::new(limits)?;
    let mut budget = OutputDrainBudget::new(limits)?;
    let mut local_counters = WasmTransitionCounters::default();
    let mut changes = Vec::new();

    loop {
        let Some(page) = actor
            .next_change_page(
                transition.transition,
                transition.changes,
                limits.max_page_bytes,
            )
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.plugin_drain_next_page"
            ))
            .await?
        else {
            validator.accept_eof();
            break;
        };
        let validation_started = std::time::Instant::now();
        tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.plugin_drain_prevalidate_page"
        )
        .in_scope(|| {
            validator.accept_page(&page).map_err(|error| {
                invalid_guest(format!(
                    "invalid component change cursor page: {}",
                    error.message
                ))
            })?;
            prevalidate_change_page(&page, schemas, &mut budget, &mut local_counters)
        })?;
        local_counters.typed_row_schema_validation_nanos = local_counters
            .typed_row_schema_validation_nanos
            .saturating_add(
                u64::try_from(validation_started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
        local_counters.packet_pages = local_counters.packet_pages.saturating_add(1);
        local_counters.packet_records = local_counters
            .packet_records
            .saturating_add(page.changes.changes.len() as u64);

        changes.reserve(page.changes.changes.len());
        for change in page.changes.changes {
            let resolved = match change {
                WasmRowChange::Create {
                    schema_key,
                    local_ref,
                    resolved_key,
                    payload: WasmGuestRowPayload::Typed(row),
                } => WasmRowChange::Create {
                    schema_key,
                    local_ref,
                    resolved_key,
                    payload: WasmHostBytes::Typed(row),
                },
                WasmRowChange::Upsert {
                    row:
                        WasmRow {
                            key,
                            payload: WasmGuestRowPayload::Typed(row),
                        },
                    effect,
                } => WasmRowChange::Upsert {
                    row: WasmRow {
                        key,
                        payload: WasmHostBytes::Typed(row),
                    },
                    effect,
                },
                WasmRowChange::Delete(key) => WasmRowChange::Delete(key),
            };
            changes.push(resolved);
        }
    }

    validate_change_cursor_key_uniqueness(&changes).map_err(|error| {
        invalid_guest(format!(
            "invalid component change cursor page: {}",
            error.message
        ))
    })?;
    let runtime_counters = actor
        .finish_transition(transition.transition)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.plugin_drain_finish"
        ))
        .await?;
    Ok(ValidatedFileTransition {
        document: transition.document,
        changes: WasmRowChanges { changes },
        replace_all_rows: transition.replace_all_rows,
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

pub(crate) async fn drain_column_merge_transition_results(
    actor: &mut dyn WasmComponentActor,
    transition: WasmColumnMergeTransition,
    expected_count: usize,
    limits: WasmTransitionLimits,
) -> Result<ValidatedColumnMergeTransition, LixError> {
    let transition_handle = transition.transition;
    let result = async {
        limits.validate()?;
        let mut budget = OutputDrainBudget::new(limits)?;
        let mut counters = WasmTransitionCounters {
            conflict_resolution_calls: 1,
            ..WasmTransitionCounters::default()
        };
        let mut results = Vec::with_capacity(expected_count);
        loop {
            let Some(page) = actor
                .next_column_merge_result_page(
                    transition.transition,
                    transition.results,
                    limits.max_page_bytes,
                )
                .await?
            else {
                break;
            };
            if page.format_version != CURRENT_PACKET_FORMAT
                || page.ordinals.len() != page.results.len()
                || page.results.is_empty()
            {
                return Err(invalid_guest("invalid component column merge result page"));
            }
            let has_output_refs = page.results.iter().any(|result| {
                matches!(
                    result,
                    WasmColumnMergeResult::Replace(Some(WasmGuestColumnValue::Output(_)))
                )
            });
            if has_output_refs != page.outputs.is_some() {
                return Err(invalid_guest(
                    "column merge output table must exist exactly when referenced",
                ));
            }
            counters.packet_pages = counters.packet_pages.saturating_add(1);
            counters.packet_records = counters
                .packet_records
                .saturating_add(page.results.len() as u64);
            for (ordinal, result) in page.ordinals.into_iter().zip(page.results) {
                let expected = u32::try_from(results.len())
                    .map_err(|_| invalid_guest("column merge result count exceeds u32"))?;
                if ordinal != expected {
                    return Err(invalid_guest(format!(
                        "column merger returned ordinal {ordinal}, expected {expected}",
                    )));
                }
                let result = match result {
                    WasmColumnMergeResult::UseLww => WasmColumnMergeResult::UseLww,
                    WasmColumnMergeResult::Replace(None) => WasmColumnMergeResult::Replace(None),
                    WasmColumnMergeResult::Replace(Some(WasmGuestColumnValue::Output(range))) => {
                        let outputs = page.outputs.ok_or_else(|| {
                            invalid_guest(
                                "component output range is missing its page-local output table",
                            )
                        })?;
                        let value = read_output_range(
                            actor,
                            transition.transition,
                            outputs,
                            range,
                            &mut budget,
                            &mut counters,
                        )
                        .await?;
                        let value = typed_wire::decode_value_bytes(&value).map_err(|error| {
                            invalid_guest(format!(
                                "column merger returned an invalid typed value: {error:?}"
                            ))
                        })?;
                        WasmColumnMergeResult::Replace(Some(value))
                    }
                };
                results.push(result);
                if results.len() > expected_count {
                    return Err(invalid_guest(
                        "column merger returned more results than input overlaps",
                    ));
                }
            }
        }
        if results.len() != expected_count {
            return Err(invalid_guest(format!(
                "column merger returned {} results for {expected_count} overlaps",
                results.len(),
            )));
        }
        counters.conflict_resolution_records = results.len() as u64;
        let runtime = actor.finish_transition(transition.transition).await?;
        Ok(ValidatedColumnMergeTransition {
            results,
            counters: merge_counter_snapshots(counters, runtime),
        })
    }
    .await;
    match result {
        Ok(result) => Ok(result),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

/// Drains renderer edit pages, resolves lazy output ranges, applies the edits
/// against one immutable base, and optionally proves byte equality with an
/// independently reconstructed expected result.
pub(crate) async fn drain_row_transition_edits(
    actor: &mut dyn WasmComponentActor,
    transition: WasmRowTransition,
    base: &[u8],
    expected: Option<Blob>,
    expected_delta: Option<&[WasmInputSplice]>,
    limits: WasmTransitionLimits,
) -> Result<ValidatedRowTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_row_transition_edits_inner(
        actor,
        transition,
        base,
        expected,
        expected_delta,
        limits,
    )
    .await
    {
        Ok(validated) => Ok(validated),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

async fn drain_row_transition_edits_inner(
    actor: &mut dyn WasmComponentActor,
    transition: WasmRowTransition,
    base: &[u8],
    expected: Option<Blob>,
    expected_delta: Option<&[WasmInputSplice]>,
    limits: WasmTransitionLimits,
) -> Result<ValidatedRowTransition, LixError> {
    let mut validator = WasmEditDrainValidator::new(base.len() as u64, limits)?;
    let mut budget = OutputDrainBudget::new(limits)?;
    let mut local_counters = WasmTransitionCounters::default();
    let mut edits = Vec::new();

    loop {
        let Some(page) = actor
            .next_edit_page(
                transition.transition,
                transition.edits,
                limits.max_inline_edits,
                limits.max_page_bytes,
            )
            .await?
        else {
            validator.accept_eof();
            break;
        };
        validator.accept_page(&page).map_err(|error| {
            invalid_guest(format!(
                "invalid component edit cursor page: {}",
                error.message
            ))
        })?;
        prevalidate_edit_page(&page, &mut budget)?;
        local_counters.packet_pages = local_counters.packet_pages.saturating_add(1);
        local_counters.packet_records = local_counters
            .packet_records
            .saturating_add(page.edits.len() as u64);

        let outputs = page.outputs;
        for edit in page.edits {
            let insert = resolve_guest_bytes(
                actor,
                transition.transition,
                outputs,
                edit.insert,
                &mut budget,
                &mut local_counters,
            )
            .await?;
            edits.push(ResolvedOutputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            });
        }
    }

    let (bytes, same_length_output_splice) =
        if let (Some(expected), Some(expected_delta)) = (&expected, expected_delta) {
            validate_resolved_output_against_known_delta(base, expected, expected_delta, &edits)?;
            (expected.clone(), None)
        } else {
            let (rendered_bytes, same_length_output_splice) =
                apply_resolved_output_splices(base, &edits)?;
            let bytes: Blob = rendered_bytes.into();
            if expected
                .as_ref()
                .is_some_and(|expected| expected.as_ref() != bytes.as_ref())
            {
                return Err(invalid_guest(
                    "component renderer edits do not reproduce the independently expected bytes",
                ));
            }
            (bytes, same_length_output_splice)
        };
    // Cold-open validation benefits from caching the accepted protocol
    // digest. A normal semantic render does not: defer that O(file) SHA-256
    // until a later request actually presents transport splice provenance.
    let bytes_sha256 = expected.as_ref().map(|_| FileBytesSha256::compute(&bytes));
    let runtime_counters = actor.finish_transition(transition.transition).await?;
    Ok(ValidatedRowTransition {
        document: transition.document,
        bytes,
        bytes_sha256,
        same_length_output_splice,
        #[cfg(test)]
        edits,
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

/// Extracts a private CAS hint after the surrounding output application has
/// completed its range/order validation. A guest cannot manufacture this fact
/// by merely emitting splice metadata: malformed, overlapping,
/// out-of-bounds, empty, length-changing, and multi-splice outputs all yield
/// no hint.
fn same_length_output_splice_after_host_validation(
    base_len: usize,
    output_len: usize,
    edits: &[ResolvedOutputSplice],
) -> Option<ValidatedSameLengthOutputSplice> {
    let [edit] = edits else {
        return None;
    };
    let offset = usize::try_from(edit.offset).ok()?;
    let length = usize::try_from(edit.delete_len).ok()?;
    let end = offset.checked_add(length)?;
    if length == 0 || length != edit.insert.len() || end > base_len || output_len != base_len {
        return None;
    }
    Some(ValidatedSameLengthOutputSplice { offset, length })
}

async fn cleanup_rejected_transition(
    actor: &mut dyn WasmComponentActor,
    transition: WasmTransitionHandle,
    rejection: LixError,
) -> LixError {
    // Traps and deadlines make cleanup completion unknowable; the runtime has
    // already retired those actors. A live actor, by contrast, reached a
    // deterministic host/guest rejection and can discard only its prospective
    // transition before the accepted actor is reused.
    if actor.is_retired() {
        return rejection;
    }
    if rejection.message.contains("deadline") {
        let _ = actor.retire().await;
        return rejection;
    }
    if let Err(cleanup_error) = actor.discard_transition(transition).await {
        // A destructor trap or failed budget release makes the attempted
        // cleanup uncertain even for runtimes that do not eagerly self-retire.
        let _ = actor.retire().await;
        return cleanup_error;
    }
    rejection
}

/// Proves a renderer patch equals a previously validated input delta without
/// copying or comparing the unchanged document prefix/suffix. Every input
/// splice must be covered by exactly one renderer edit, and each renderer
/// insertion must equal the corresponding base region with those input
/// splices applied. This is the warm exact-observation fast path.
fn validate_resolved_output_against_known_delta(
    base: &[u8],
    expected: &[u8],
    input: &[WasmInputSplice],
    output: &[ResolvedOutputSplice],
) -> Result<(), LixError> {
    let output_len = output.iter().try_fold(base.len(), |length, edit| {
        let delete_len = usize::try_from(edit.delete_len)
            .map_err(|_| invalid_guest("component output deletion does not fit this host"))?;
        length
            .checked_sub(delete_len)
            .and_then(|length| length.checked_add(edit.insert.len()))
            .ok_or_else(|| invalid_guest("component rendered result length overflowed"))
    })?;
    if output_len != expected.len() {
        return Err(invalid_guest(
            "component renderer edits do not reproduce the independently expected length",
        ));
    }

    let mut covered = vec![false; input.len()];
    for edit in output {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("component output offset does not fit this host"))?;
        let end = start
            .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                invalid_guest("component output deletion length does not fit this host")
            })?)
            .ok_or_else(|| invalid_guest("component output deletion range overflowed"))?;
        let mut reconstructed = Vec::with_capacity(edit.insert.len());
        let mut cursor = start;
        for (index, input_edit) in input.iter().enumerate() {
            let input_start = usize::try_from(input_edit.offset)
                .map_err(|_| invalid_input("component input offset does not fit this host"))?;
            let input_end = input_start
                .checked_add(usize::try_from(input_edit.delete_len).map_err(|_| {
                    invalid_input("component input deletion length does not fit this host")
                })?)
                .ok_or_else(|| invalid_input("component input deletion range overflowed"))?;
            let is_inside = input_start >= start
                && input_end <= end
                && (input_start < end || (input_start == end && input_edit.delete_len == 0));
            if !is_inside {
                continue;
            }
            if covered[index] || input_start < cursor {
                return Err(invalid_guest(
                    "component renderer edits do not cover the known input delta exactly once",
                ));
            }
            reconstructed.extend_from_slice(&base[cursor..input_start]);
            match &input_edit.insert {
                WasmInputBytes::Inline(bytes) => reconstructed.extend_from_slice(bytes),
                WasmInputBytes::AfterRange(range) => {
                    let range_start = usize::try_from(range.offset).map_err(|_| {
                        invalid_input("component after-range offset does not fit this host")
                    })?;
                    let range_end = usize::try_from(range.end()?).map_err(|_| {
                        invalid_input("component after-range end does not fit this host")
                    })?;
                    reconstructed.extend_from_slice(
                        expected.get(range_start..range_end).ok_or_else(|| {
                            invalid_input("component after-range is out of expected-result bounds")
                        })?,
                    );
                }
            }
            cursor = input_end;
            covered[index] = true;
        }
        reconstructed
            .extend_from_slice(base.get(cursor..end).ok_or_else(|| {
                invalid_guest("component output deletion exceeds accepted bytes")
            })?);
        if reconstructed != edit.insert {
            return Err(invalid_guest(
                "component renderer edit does not reproduce the independently expected local region",
            ));
        }
    }
    if covered.iter().any(|covered| !covered) {
        return Err(invalid_guest(
            "component renderer edits omitted part of the independently expected delta",
        ));
    }
    Ok(())
}

fn prevalidate_change_page(
    page: &WasmChangePage,
    schemas: &SchemaAllowlist,
    _budget: &mut OutputDrainBudget,
    counters: &mut WasmTransitionCounters,
) -> Result<(), LixError> {
    if page.format_version != CURRENT_PACKET_FORMAT {
        return Err(invalid_guest("unsupported component change packet format"));
    }
    let mut inline_bytes = 0u64;
    let output_bytes = 0u64;
    let minimum_attachment_reads = 0u64;
    let references = 0u32;
    for change in &page.changes.changes {
        schemas.validate(change.schema_key())?;
        if let Some(key) = change.row_key()
            && key.row_pk.is_empty()
        {
            return Err(invalid_guest(
                "component row primary keys must not be empty",
            ));
        }
        let snapshot = match change {
            WasmRowChange::Create { payload, .. } => Some(payload),
            WasmRowChange::Upsert { row, .. } => Some(&row.payload),
            WasmRowChange::Delete(_) => None,
        };
        if let Some(snapshot) = snapshot {
            match snapshot {
                WasmGuestRowPayload::Typed(row) => {
                    if let WasmRowChange::Upsert { row: emitted, .. } = change
                        && (emitted.key.schema_fingerprint != row.schema_fingerprint
                            || emitted.key.row_pk.as_ref() != row.row_pk.as_ref())
                    {
                        return Err(invalid_guest(
                            "component row key does not match its typed payload identity",
                        ));
                    }
                    let encoded_bytes = row
                        .estimated_size()
                        .checked_add(1 + 4)
                        .ok_or_else(|| invalid_guest("typed row payload size overflowed"))?;
                    inline_bytes = inline_bytes
                        .checked_add(encoded_bytes)
                        .ok_or_else(|| invalid_guest("typed row page bytes overflowed"))?;
                    counters.typed_row_schema_validation_calls =
                        counters.typed_row_schema_validation_calls.saturating_add(1);
                    counters.typed_row_schema_validation_bytes = counters
                        .typed_row_schema_validation_bytes
                        .saturating_add(row.estimated_size());
                    schemas.validate_typed_row(change.schema_key(), row)?;
                }
            }
        }
    }
    _budget.preflight_cursor_page(
        inline_bytes,
        output_bytes,
        0,
        references,
        minimum_attachment_reads,
    )
}

fn prevalidate_edit_page(
    page: &WasmEditPage,
    budget: &mut OutputDrainBudget,
) -> Result<(), LixError> {
    let mut inline_bytes = 0u64;
    let mut output_bytes = 0u64;
    let cursor_metadata_bytes = EDIT_SPLICE_METADATA_BYTES
        .checked_mul(page.edits.len() as u64)
        .ok_or_else(|| invalid_guest("component edit page metadata bytes overflowed"))?;
    let mut minimum_attachment_reads = 0u64;
    let mut references = 0u32;
    for edit in &page.edits {
        match &edit.insert {
            WasmGuestBytes::Inline(bytes) => {
                inline_bytes = inline_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid_guest("component inline edit bytes overflowed"))?;
            }
            WasmGuestBytes::Output(range) => {
                output_bytes = output_bytes
                    .checked_add(range.length)
                    .ok_or_else(|| invalid_guest("component output edit bytes overflowed"))?;
                minimum_attachment_reads = minimum_attachment_reads
                    .checked_add(budget.minimum_attachment_reads(range.length))
                    .ok_or_else(|| invalid_guest("component attachment page count overflowed"))?;
                references = references
                    .checked_add(1)
                    .ok_or_else(|| invalid_guest("component edit output references overflowed"))?;
            }
        }
    }
    budget.preflight_cursor_page(
        inline_bytes,
        output_bytes,
        cursor_metadata_bytes,
        references,
        minimum_attachment_reads,
    )
}

async fn resolve_guest_bytes(
    actor: &mut dyn WasmComponentActor,
    transition: WasmTransitionHandle,
    outputs: Option<WasmByteOutputsHandle>,
    bytes: WasmGuestBytes,
    budget: &mut OutputDrainBudget,
    counters: &mut WasmTransitionCounters,
) -> Result<Bytes, LixError> {
    match bytes {
        WasmGuestBytes::Inline(bytes) => {
            budget.charge_inline(bytes.len() as u64)?;
            counters.component_boundary_bytes = counters
                .component_boundary_bytes
                .saturating_add(bytes.len() as u64);
            Ok(bytes)
        }
        WasmGuestBytes::Output(range) => {
            let outputs = outputs.ok_or_else(|| {
                invalid_guest("component output range is missing its page-local output table")
            })?;
            read_output_range(actor, transition, outputs, range, budget, counters).await
        }
    }
}

async fn read_output_range(
    actor: &mut dyn WasmComponentActor,
    transition: WasmTransitionHandle,
    outputs: WasmByteOutputsHandle,
    range: WasmOutputRange,
    budget: &mut OutputDrainBudget,
    counters: &mut WasmTransitionCounters,
) -> Result<Bytes, LixError> {
    let end = range
        .offset
        .checked_add(range.length)
        .ok_or_else(|| invalid_guest("component output range overflowed"))?;
    let output_len = actor.output_len(transition, outputs, range.index).await?;
    if end > output_len {
        return Err(invalid_guest("component output range is out of bounds"));
    }
    let capacity = usize::try_from(range.length)
        .map_err(|_| invalid_guest("component output range does not fit this host"))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut offset = range.offset;
    while bytes.len() < capacity {
        let remaining = capacity - bytes.len();
        let page_limit = usize::try_from(budget.limits.max_page_bytes)
            .map_err(|_| invalid_guest("component output page limit does not fit this host"))?;
        let requested_len = remaining.min(page_limit);
        let requested = u32::try_from(requested_len)
            .map_err(|_| invalid_guest("component output read length exceeds the component ABI"))?;
        let chunk = actor
            .read_output(transition, outputs, range.index, offset, requested)
            .await?;
        if chunk.is_empty() || chunk.len() > requested_len {
            return Err(invalid_guest(
                "component output reads must return a non-empty bounded prefix before EOF",
            ));
        }
        budget.charge_attachment_read(chunk.len() as u64)?;
        counters.attachment_reads = counters.attachment_reads.saturating_add(1);
        counters.attachment_bytes_read = counters
            .attachment_bytes_read
            .saturating_add(chunk.len() as u64);
        counters.component_boundary_bytes = counters
            .component_boundary_bytes
            .saturating_add(chunk.len() as u64);
        offset = offset
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| invalid_guest("component output read offset overflowed"))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.into())
}

#[derive(Debug)]
struct OutputDrainBudget {
    limits: WasmTransitionLimits,
    pages: u32,
    total_bytes: u64,
    attachment_refs: u32,
}

impl OutputDrainBudget {
    fn new(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            pages: 0,
            total_bytes: 0,
            attachment_refs: 0,
        })
    }

    /// Reserves only cursor-page/reference accounting. The complete page's
    /// prospective bytes are checked before attachment methods, then charged
    /// exactly as inline values and output chunks are consumed.
    fn preflight_cursor_page(
        &mut self,
        inline_bytes: u64,
        output_bytes: u64,
        cursor_metadata_bytes: u64,
        references: u32,
        minimum_attachment_reads: u64,
    ) -> Result<(), LixError> {
        let prospective_bytes = inline_bytes
            .checked_add(output_bytes)
            .and_then(|bytes| bytes.checked_add(cursor_metadata_bytes))
            .and_then(|bytes| self.total_bytes.checked_add(bytes))
            .ok_or_else(|| invalid_guest("component transition output byte count overflowed"))?;
        let prospective_refs = self
            .attachment_refs
            .checked_add(references)
            .ok_or_else(|| invalid_guest("component attachment reference count overflowed"))?;
        let minimum_pages = u64::from(self.pages)
            .checked_add(1)
            .and_then(|pages| pages.checked_add(minimum_attachment_reads))
            .ok_or_else(|| invalid_guest("component output page count overflowed"))?;
        if prospective_bytes > self.limits.max_total_bytes {
            return Err(invalid_guest(
                "component transition output exceeds max_total_bytes",
            ));
        }
        if prospective_refs > self.limits.max_attachment_refs {
            return Err(invalid_guest(
                "component transition output exceeds max_attachment_refs",
            ));
        }
        if minimum_pages > u64::from(self.limits.max_pages) {
            return Err(invalid_guest(
                "component transition output exceeds max_pages",
            ));
        }
        self.pages += 1;
        self.attachment_refs = prospective_refs;
        self.total_bytes = self
            .total_bytes
            .checked_add(cursor_metadata_bytes)
            .expect("prospective byte count was checked above");
        Ok(())
    }

    fn minimum_attachment_reads(&self, bytes: u64) -> u64 {
        bytes.div_ceil(u64::from(self.limits.max_page_bytes))
    }

    fn charge_inline(&mut self, bytes: u64) -> Result<(), LixError> {
        self.charge_bytes(bytes)
    }

    fn charge_attachment_read(&mut self, bytes: u64) -> Result<(), LixError> {
        if bytes == 0 || bytes > u64::from(self.limits.max_page_bytes) {
            return Err(invalid_guest(
                "component attachment read violates its page bound",
            ));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_guest("component transition page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_guest(
                "component transition output exceeds max_pages",
            ));
        }
        self.charge_bytes(bytes)
    }

    fn charge_bytes(&mut self, bytes: u64) -> Result<(), LixError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid_guest("component transition byte count overflowed"))?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(invalid_guest(
                "component transition output exceeds max_total_bytes",
            ));
        }
        Ok(())
    }
}

fn apply_resolved_output_splices(
    base: &[u8],
    edits: &[ResolvedOutputSplice],
) -> Result<(Vec<u8>, Option<ValidatedSameLengthOutputSplice>), LixError> {
    let mut capacity = base.len();
    let mut previous_start = None;
    let mut previous_end = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("component output splice offset does not fit this host"))?;
        let delete_len = usize::try_from(edit.delete_len).map_err(|_| {
            invalid_guest("component output splice deletion does not fit this host")
        })?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| invalid_guest("component output splice deletion range overflowed"))?;
        if previous_start == Some(start) || start < previous_end || end > base.len() {
            return Err(invalid_guest(
                "component output splices are not globally sorted, unique, and in bounds",
            ));
        }
        capacity = capacity
            .checked_sub(delete_len)
            .and_then(|capacity| capacity.checked_add(edit.insert.len()))
            .ok_or_else(|| invalid_guest("component rendered result length overflowed"))?;
        previous_start = Some(start);
        previous_end = end;
    }

    let mut bytes = Vec::with_capacity(capacity);
    let mut cursor = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("component output splice offset does not fit this host"))?;
        let delete_len = usize::try_from(edit.delete_len).map_err(|_| {
            invalid_guest("component output splice deletion does not fit this host")
        })?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| invalid_guest("component output splice deletion range overflowed"))?;
        bytes.extend_from_slice(&base[cursor..start]);
        bytes.extend_from_slice(&edit.insert);
        cursor = end;
    }
    bytes.extend_from_slice(&base[cursor..]);
    let same_length_output_splice =
        same_length_output_splice_after_host_validation(base.len(), bytes.len(), edits);
    Ok((bytes, same_length_output_splice))
}

fn merge_counter_snapshots(
    local: WasmTransitionCounters,
    runtime: WasmTransitionCounters,
) -> WasmTransitionCounters {
    WasmTransitionCounters {
        source_read_calls: local.source_read_calls.max(runtime.source_read_calls),
        source_bytes_read: local.source_bytes_read.max(runtime.source_bytes_read),
        file_read_calls: local.file_read_calls.max(runtime.file_read_calls),
        file_bytes_read: local.file_bytes_read.max(runtime.file_bytes_read),
        state_read_calls: local.state_read_calls.max(runtime.state_read_calls),
        state_key_bytes: local.state_key_bytes.max(runtime.state_key_bytes),
        state_value_bytes_read: local
            .state_value_bytes_read
            .max(runtime.state_value_bytes_read),
        packet_pages: local.packet_pages.max(runtime.packet_pages),
        packet_records: local.packet_records.max(runtime.packet_records),
        row_input_pages: local.row_input_pages.max(runtime.row_input_pages),
        row_input_records: local.row_input_records.max(runtime.row_input_records),
        row_input_wire_bytes: local.row_input_wire_bytes.max(runtime.row_input_wire_bytes),
        row_output_pages: local.row_output_pages.max(runtime.row_output_pages),
        row_output_records: local.row_output_records.max(runtime.row_output_records),
        row_output_wire_bytes: local
            .row_output_wire_bytes
            .max(runtime.row_output_wire_bytes),
        attachment_reads: local.attachment_reads.max(runtime.attachment_reads),
        attachment_bytes_read: local
            .attachment_bytes_read
            .max(runtime.attachment_bytes_read),
        row_input_attachment_reads: local
            .row_input_attachment_reads
            .max(runtime.row_input_attachment_reads),
        row_input_attachment_bytes: local
            .row_input_attachment_bytes
            .max(runtime.row_input_attachment_bytes),
        row_output_attachment_writes: local
            .row_output_attachment_writes
            .max(runtime.row_output_attachment_writes),
        row_output_attachment_bytes: local
            .row_output_attachment_bytes
            .max(runtime.row_output_attachment_bytes),
        typed_row_decode_records: local
            .typed_row_decode_records
            .max(runtime.typed_row_decode_records),
        typed_row_decode_bytes: local
            .typed_row_decode_bytes
            .max(runtime.typed_row_decode_bytes),
        typed_row_decode_nanos: local
            .typed_row_decode_nanos
            .max(runtime.typed_row_decode_nanos),
        typed_row_encode_records: local
            .typed_row_encode_records
            .max(runtime.typed_row_encode_records),
        typed_row_encode_bytes: local
            .typed_row_encode_bytes
            .max(runtime.typed_row_encode_bytes),
        typed_row_schema_validation_calls: local
            .typed_row_schema_validation_calls
            .max(runtime.typed_row_schema_validation_calls),
        typed_row_schema_validation_bytes: local
            .typed_row_schema_validation_bytes
            .max(runtime.typed_row_schema_validation_bytes),
        typed_row_schema_validation_nanos: local
            .typed_row_schema_validation_nanos
            .max(runtime.typed_row_schema_validation_nanos),
        typed_transaction_validation_calls: local
            .typed_transaction_validation_calls
            .max(runtime.typed_transaction_validation_calls),
        typed_transaction_validation_bytes: local
            .typed_transaction_validation_bytes
            .max(runtime.typed_transaction_validation_bytes),
        row_page_callback_calls: local
            .row_page_callback_calls
            .max(runtime.row_page_callback_calls),
        row_input_page_eof_callbacks: local
            .row_input_page_eof_callbacks
            .max(runtime.row_input_page_eof_callbacks),
        outer_row_json_parse_calls: local
            .outer_row_json_parse_calls
            .max(runtime.outer_row_json_parse_calls),
        outer_row_json_parse_bytes: local
            .outer_row_json_parse_bytes
            .max(runtime.outer_row_json_parse_bytes),
        outer_row_json_serialize_calls: local
            .outer_row_json_serialize_calls
            .max(runtime.outer_row_json_serialize_calls),
        outer_row_json_serialize_bytes: local
            .outer_row_json_serialize_bytes
            .max(runtime.outer_row_json_serialize_bytes),
        outer_row_json_canonicalize_calls: local
            .outer_row_json_canonicalize_calls
            .max(runtime.outer_row_json_canonicalize_calls),
        outer_row_json_canonicalize_bytes: local
            .outer_row_json_canonicalize_bytes
            .max(runtime.outer_row_json_canonicalize_bytes),
        outer_row_json_dom_fallback_calls: local
            .outer_row_json_dom_fallback_calls
            .max(runtime.outer_row_json_dom_fallback_calls),
        outer_row_json_dom_fallback_bytes: local
            .outer_row_json_dom_fallback_bytes
            .max(runtime.outer_row_json_dom_fallback_bytes),
        component_import_calls: local
            .component_import_calls
            .max(runtime.component_import_calls),
        guest_export_calls: local.guest_export_calls.max(runtime.guest_export_calls),
        actor_executor_threads_created: local
            .actor_executor_threads_created
            .max(runtime.actor_executor_threads_created),
        component_boundary_bytes: local
            .component_boundary_bytes
            .max(runtime.component_boundary_bytes),
        guest_linear_memory_high_water_bytes: local
            .guest_linear_memory_high_water_bytes
            .max(runtime.guest_linear_memory_high_water_bytes),
        host_full_diff_bytes_compared: local
            .host_full_diff_bytes_compared
            .max(runtime.host_full_diff_bytes_compared),
        host_content_classification_bytes: local
            .host_content_classification_bytes
            .max(runtime.host_content_classification_bytes),
        full_state_semantic_rows_materialized: local
            .full_state_semantic_rows_materialized
            .max(runtime.full_state_semantic_rows_materialized),
        change_payload_requests: local
            .change_payload_requests
            .max(runtime.change_payload_requests),
        returned_change_payloads: local
            .returned_change_payloads
            .max(runtime.returned_change_payloads),
        durable_semantic_changes: local
            .durable_semantic_changes
            .max(runtime.durable_semantic_changes),
        private_document_cache_hits: local
            .private_document_cache_hits
            .max(runtime.private_document_cache_hits),
        shared_renderer_cache_hits: local
            .shared_renderer_cache_hits
            .max(runtime.shared_renderer_cache_hits),
        full_document_reparses: local
            .full_document_reparses
            .max(runtime.full_document_reparses),
        full_renderer_invocations: local
            .full_renderer_invocations
            .max(runtime.full_renderer_invocations),
        filesystem_sync_full_renders: local
            .filesystem_sync_full_renders
            .max(runtime.filesystem_sync_full_renders),
        conflict_resolution_calls: local
            .conflict_resolution_calls
            .max(runtime.conflict_resolution_calls),
        conflict_resolution_records: local
            .conflict_resolution_records
            .max(runtime.conflict_resolution_records),
        conflict_resolution_takes: local
            .conflict_resolution_takes
            .max(runtime.conflict_resolution_takes),
    }
}

fn invalid_input(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

fn invalid_guest(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::plugin::runtime::{
        WasmChangeCursorHandle, WasmChangeEffect, WasmCreateContext, WasmEditCursorHandle,
        WasmOpenFileInput, WasmOpenRowsInput, WasmOutputSplice,
    };

    fn test_creates() -> WasmCreateContext {
        WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        }
    }

    fn key(id: &str) -> WasmRowKey {
        WasmRowKey::from_typed_parts(
            "csv_row",
            [0; 32],
            vec![lix_schema::Value::Text(id.to_owned())],
        )
        .unwrap()
    }

    fn host_row(id: &str) -> WasmHostRow {
        let id = id.to_owned();
        WasmRow {
            key: key(&id),
            payload: WasmHostBytes::Typed(Arc::new(WasmTypedRow {
                schema_fingerprint: [0; 32],
                row_pk: vec![lix_schema::Value::Text(id.clone())].into(),
                row: lix_schema::Row::from([
                    (
                        "cells".to_owned(),
                        lix_schema::Value::Jsonb(serde_json::Value::Array(Vec::new()).into()),
                    ),
                    ("id".to_owned(), lix_schema::Value::Text(id)),
                    (
                        "order_key".to_owned(),
                        lix_schema::Value::Text("a".to_owned()),
                    ),
                ]),
                native_payload: std::sync::OnceLock::new(),
                boundary_create_validation: std::sync::OnceLock::new(),
            })),
        }
    }

    fn host_row_for_schema(id: &str, schema_key: &str, fingerprint: [u8; 32]) -> WasmHostRow {
        let mut row = host_row(id);
        row.key.schema_key = schema_key.into();
        row.key.schema_fingerprint = fingerprint;
        let WasmHostBytes::Typed(payload) = &mut row.payload;
        Arc::make_mut(payload).schema_fingerprint = fingerprint;
        row
    }

    fn guest_row(id: &str) -> WasmGuestRowPayload {
        match host_row(id).payload {
            WasmHostBytes::Typed(row) => WasmGuestRowPayload::Typed(row),
        }
    }

    #[test]
    fn catalog_boundary_validation_certifies_only_fully_validated_native_rows() {
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "native_row",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "body", "type": "jsonb", "nullable": false }
            ],
            "primary_key": ["id"]
        });
        let catalog = Arc::new(
            CatalogSnapshot::from_visible_schemas(&[schema])
                .expect("native boundary test catalog should build"),
        );
        let fingerprint = catalog
            .plan_for_key("native_row")
            .expect("native row schema should resolve")
            .1
            .fingerprint()
            .bytes();
        let allowlist = SchemaAllowlist::from_catalog(&["native_row".to_owned()], catalog)
            .expect("native boundary allowlist should build");
        let row = WasmTypedRow {
            schema_fingerprint: fingerprint,
            row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
            row: lix_schema::Row::from([
                (
                    "body".to_owned(),
                    lix_schema::Value::Jsonb(json!({ "value": true }).into()),
                ),
                ("id".to_owned(), lix_schema::Value::Text("row-1".to_owned())),
            ]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        };

        assert!(!row.boundary_validation_certified());
        allowlist
            .validate_typed_row("native_row", &row)
            .expect("complete native row should validate");
        assert!(row.boundary_validation_certified());
        assert!(
            !row.clone().boundary_validation_certified(),
            "cloning a row owner must not copy its boundary certificate"
        );
    }

    #[test]
    fn durable_encoding_does_not_forge_a_boundary_validation_certificate() {
        let row = WasmTypedRow {
            schema_fingerprint: [7; 32],
            row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
            row: lix_schema::Row::from([(
                "id".to_owned(),
                lix_schema::Value::Text("row-1".to_owned()),
            )]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        };

        row.durable_payload()
            .expect("ordinary durable encoding should succeed");
        assert!(!row.boundary_validation_certified());
    }

    #[test]
    fn arc_byte_source_is_bounded_and_counts_actual_prefixes() {
        let source = ArcByteSource::from_vec(b"abcdef".to_vec());
        assert_eq!(source.read(2, 3).unwrap(), b"cde");
        assert_eq!(source.read(5, 10).unwrap(), b"f");
        assert_eq!(source.read(6, 10).unwrap(), b"");
        assert!(source.read(7, 1).is_err());
        assert!(source.read(0, 0).is_err());
        assert_eq!(source.counters().source_read_calls, 3);
        assert_eq!(source.counters().source_bytes_read, 4);
    }

    #[test]
    fn splice_builder_preserves_transport_delta_and_has_a_full_diff_fallback() {
        let before = b"abcdef";
        let after: Blob = b"abXYef".as_slice().into();
        let before_sha256 = FileBytesSha256::compute(before);
        let after_sha256 = FileBytesSha256::compute(&after);
        let provenance = RequestBlobSpliceProvenance::new_validated_for_test(
            before,
            &after,
            2,
            2,
            b"XY".to_vec(),
        );
        let from_transport = build_file_update_splices(
            before,
            before_sha256,
            &after,
            Some(&provenance),
            WasmTransitionLimits::default(),
        )
        .unwrap();
        assert!(from_transport.used_transport_provenance);
        assert_eq!(from_transport.full_diff_bytes_compared, 0);
        assert_eq!(from_transport.after_sha256, Some(after_sha256));
        assert_eq!(from_transport.replacement(), Some((2, 2, 2)));
        assert_eq!(from_transport.same_length_replacement(), Some((2, 2)));
        assert_eq!(
            from_transport.edits,
            vec![WasmInputSplice {
                offset: 2,
                delete_len: 2,
                insert: WasmInputBytes::Inline(b"XY".to_vec()),
            }]
        );

        let lazily_verified_transport = build_file_update_splices(
            before,
            None,
            &after,
            Some(&provenance),
            WasmTransitionLimits::default(),
        )
        .unwrap();
        assert!(lazily_verified_transport.used_transport_provenance);
        assert_eq!(lazily_verified_transport.after_sha256, Some(after_sha256));

        let fallback = build_file_update_splices(
            before,
            before_sha256,
            &after,
            None,
            WasmTransitionLimits::default(),
        )
        .unwrap();
        assert!(!fallback.used_transport_provenance);
        assert_eq!(fallback.full_diff_bytes_compared, 12);
        assert_eq!(fallback.after_sha256, None);
        assert_eq!(fallback.edits, from_transport.edits);

        let lazy = build_file_update_splices(
            before,
            before_sha256,
            &after,
            None,
            WasmTransitionLimits {
                max_inline_input_bytes: 1,
                ..WasmTransitionLimits::default()
            },
        )
        .unwrap();
        assert!(matches!(
            lazy.edits[0].insert,
            WasmInputBytes::AfterRange(WasmSourceRange {
                offset: 2,
                length: 2
            })
        ));
        assert_eq!(lazy.same_length_replacement(), Some((2, 2)));

        let length_changing = build_file_update_splices(
            before,
            before_sha256,
            b"abXYZef",
            None,
            WasmTransitionLimits::default(),
        )
        .expect("length-changing splice should still build");
        assert_eq!(length_changing.same_length_replacement(), None);
        assert_eq!(length_changing.replacement(), Some((2, 2, 3)));
    }

    #[test]
    fn word_scans_find_prefix_and_suffix_mismatches_at_every_alignment() {
        let baseline = (0u8..96).collect::<Vec<_>>();
        for mismatch in 0..baseline.len() {
            let mut changed = baseline.clone();
            changed[mismatch] ^= 0xff;

            assert_eq!(common_prefix_len(&baseline, &changed).0, mismatch);
            assert_eq!(
                common_suffix_len(&baseline, &changed, baseline.len()).0,
                baseline.len() - mismatch - 1
            );
        }
        assert_eq!(common_prefix_len(&baseline, &baseline).0, baseline.len());
        assert_eq!(
            common_suffix_len(&baseline, &baseline, baseline.len()).0,
            baseline.len()
        );
    }

    #[test]
    fn splice_builder_silently_full_diffs_mismatched_base_provenance() {
        let before = b"abcdef";
        let proof_base = b"uvwxyz";
        let after: Blob = b"uvXYyz".as_slice().into();
        let before_sha256 = FileBytesSha256::compute(before);
        let provenance = RequestBlobSpliceProvenance::new_validated_for_test(
            proof_base,
            &after,
            2,
            2,
            b"XY".to_vec(),
        );

        let fallback = build_file_update_splices(
            before,
            before_sha256,
            &after,
            Some(&provenance),
            WasmTransitionLimits::default(),
        )
        .expect("cross-file provenance is only an optimization miss");
        assert!(!fallback.used_transport_provenance);
        assert_eq!(fallback.after_sha256, None);
        assert_eq!(
            fallback.edits,
            vec![WasmInputSplice {
                offset: 0,
                delete_len: before.len() as u64,
                insert: WasmInputBytes::Inline(after.to_vec()),
            }]
        );
    }

    #[test]
    fn splice_builder_rejects_provenance_transplanted_onto_other_result_bytes() {
        let before = b"abcdef";
        let validated_result: Blob = b"abXYef".as_slice().into();
        let provenance = RequestBlobSpliceProvenance::new_validated_for_test(
            before,
            &validated_result,
            2,
            2,
            b"XY".to_vec(),
        );
        // Same length and inserted middle, but forged unchanged prefix/suffix.
        let submitted: Blob = b"qbXYez".as_slice().into();
        let fallback = build_file_update_splices(
            before,
            FileBytesSha256::compute(before),
            &submitted,
            Some(&provenance),
            WasmTransitionLimits::default(),
        )
        .expect("transplanted provenance must fall back safely");

        assert!(!fallback.used_transport_provenance);
        assert_eq!(fallback.after_sha256, None);
        assert_eq!(
            fallback.edits,
            vec![WasmInputSplice {
                offset: 0,
                delete_len: before.len() as u64,
                insert: WasmInputBytes::Inline(submitted.to_vec()),
            }]
        );
    }

    #[test]
    fn utf8_splice_proof_checks_only_the_changed_boundary_window() {
        let before = b"alpha,old,omega";
        let after: Blob = b"alpha,BETA,omega".as_slice().into();
        let valid = RequestBlobSpliceProvenance::new_validated_for_test(
            before,
            &after,
            6,
            6,
            b"BETA".to_vec(),
        );
        assert!(transport_splice_preserves_utf8(&after, &valid));

        let invalid_after: Blob = b"a\xa9z".as_slice().into();
        let split_code_point = RequestBlobSpliceProvenance::new_validated_for_test(
            &invalid_after,
            &invalid_after,
            1,
            2,
            Vec::new(),
        );
        assert!(!transport_splice_preserves_utf8(
            &invalid_after,
            &split_code_point
        ));

        let copied_after: Blob = after.to_vec().into();
        assert!(!transport_splice_preserves_utf8(&copied_after, &valid));
    }

    #[test]
    fn prefix_exclusion_splice_proof_checks_only_its_bounded_window() {
        let before = vec![b'a'; 16_000];
        let after: Blob = before.clone().into();
        let valid = RequestBlobSpliceProvenance::new_validated_for_test(
            &before,
            &after,
            16_000,
            0,
            Vec::new(),
        );
        assert!(transport_splice_preserves_prefix_exclusion(
            &after, &valid, 0, 8_000
        ));

        let mut nul_in_window = after.to_vec();
        nul_in_window[7_999] = 0;
        let nul_in_window: Blob = nul_in_window.into();
        let nul_provenance = RequestBlobSpliceProvenance::new_validated_for_test(
            &before,
            &nul_in_window,
            7_999,
            8_000,
            vec![0],
        );
        assert!(!transport_splice_preserves_prefix_exclusion(
            &nul_in_window,
            &nul_provenance,
            0,
            8_000,
        ));
    }

    #[test]
    fn vec_row_sources_page_without_splitting_records() {
        let first = host_row("a");
        let first_page_bytes = encoded_row_record_bytes(&first).unwrap() + 4;
        let mut source =
            VecRowSource::new(vec![first, host_row("b")], WasmTransitionLimits::default()).unwrap();
        let first_page = source
            .next_page(u32::try_from(first_page_bytes).expect("test page size fits u32"))
            .unwrap()
            .unwrap();
        assert_eq!(first_page.rows.len(), 1);
        assert_eq!(
            first_page.rows[0].key.row_pk[0],
            lix_schema::Value::Text("a".to_owned())
        );
        let second_page = source
            .next_page(WasmTransitionLimits::default().max_page_bytes)
            .unwrap()
            .unwrap();
        assert_eq!(
            second_page.rows[0].key.row_pk[0],
            lix_schema::Value::Text("b".to_owned())
        );
        assert!(source.next_page(1).unwrap().is_none());
        assert!(source.next_page(1).unwrap().is_none());

        assert!(
            VecRowSource::new(
                vec![host_row("b"), host_row("a")],
                WasmTransitionLimits::default()
            )
            .is_err()
        );
    }

    #[test]
    fn vec_row_source_clamps_a_larger_consumer_page_hint() {
        let limits = WasmTransitionLimits::default();
        let mut source = VecRowSource::new(vec![host_row("a")], limits).unwrap();

        let page = source
            .next_page(limits.max_page_bytes.saturating_mul(4))
            .expect("larger consumer hint should be clamped")
            .expect("one row page");

        assert_eq!(page.rows.len(), 1);
    }

    #[test]
    fn vec_row_source_splits_pages_at_schema_and_fingerprint_boundaries() {
        let limits = WasmTransitionLimits::default();
        let rows = vec![
            host_row_for_schema("a", "csv_row", [0; 32]),
            host_row_for_schema("b", "csv_row", [1; 32]),
            host_row_for_schema("c", "json_node", [2; 32]),
        ];
        let mut source = VecRowSource::new(rows, limits).unwrap();

        for expected_fingerprint in [[0; 32], [1; 32], [2; 32]] {
            let page = source.next_page(limits.max_page_bytes).unwrap().unwrap();
            assert_eq!(page.rows.len(), 1);
            assert_eq!(page.rows[0].key.schema_fingerprint, expected_fingerprint);
        }
        assert!(source.next_page(limits.max_page_bytes).unwrap().is_none());
    }

    #[test]
    fn vec_change_source_rejects_unsorted_changes_and_pages_records() {
        let unsorted = WasmRowChanges {
            changes: vec![
                WasmRowChange::Delete(key("b")),
                WasmRowChange::Delete(key("a")),
            ],
        };
        assert!(VecRowChangeSource::new(unsorted, WasmTransitionLimits::default()).is_err());

        let first = WasmRowChange::Delete(key("a"));
        let first_page_bytes = encoded_row_change_record_bytes(&first).unwrap() + 4;
        let changes = WasmRowChanges {
            changes: vec![first, WasmRowChange::Delete(key("b"))],
        };
        let mut source = VecRowChangeSource::new(changes, WasmTransitionLimits::default()).unwrap();
        assert_eq!(
            source
                .next_page(u32::try_from(first_page_bytes).expect("test page size fits u32"))
                .unwrap()
                .unwrap()
                .changes
                .len(),
            1
        );
        assert_eq!(
            source
                .next_page(WasmTransitionLimits::default().max_page_bytes)
                .unwrap()
                .unwrap()
                .changes
                .len(),
            1
        );
        assert!(source.next_page(1).unwrap().is_none());
    }

    #[test]
    fn vec_change_source_splits_pages_at_schema_and_fingerprint_boundaries() {
        let limits = WasmTransitionLimits::default();
        let changes = WasmRowChanges {
            changes: vec![
                WasmRowChange::Delete(host_row_for_schema("a", "csv_row", [0; 32]).key),
                WasmRowChange::Delete(host_row_for_schema("b", "csv_row", [1; 32]).key),
                WasmRowChange::Delete(host_row_for_schema("c", "json_node", [2; 32]).key),
            ],
        };
        let mut source = VecRowChangeSource::new(changes, limits).unwrap();

        for expected_fingerprint in [[0; 32], [1; 32], [2; 32]] {
            let page = source.next_page(limits.max_page_bytes).unwrap().unwrap();
            assert_eq!(page.changes.len(), 1);
            assert_eq!(
                page.changes[0].row_key().unwrap().schema_fingerprint,
                expected_fingerprint
            );
        }
        assert!(source.next_page(limits.max_page_bytes).unwrap().is_none());
    }

    struct FakeActor {
        change_pages: VecDeque<WasmChangePage>,
        edit_pages: VecDeque<WasmEditPage>,
        outputs: BTreeMap<(WasmByteOutputsHandle, u32), Vec<u8>>,
        max_read_prefix: usize,
        output_len_calls: usize,
        finished: bool,
        discarded_transitions: Vec<WasmTransitionHandle>,
        retired: bool,
        discard_fails: bool,
        runtime_counters: WasmTransitionCounters,
    }

    impl Default for FakeActor {
        fn default() -> Self {
            Self {
                change_pages: VecDeque::new(),
                edit_pages: VecDeque::new(),
                outputs: BTreeMap::new(),
                max_read_prefix: usize::MAX,
                output_len_calls: 0,
                finished: false,
                discarded_transitions: Vec::new(),
                retired: false,
                discard_fails: false,
                runtime_counters: WasmTransitionCounters::default(),
            }
        }
    }

    fn unused() -> LixError {
        LixError::new(LixError::CODE_INTERNAL_ERROR, "unused fake actor method")
    }

    #[async_trait]
    impl WasmComponentActor for FakeActor {
        async fn fork_document(
            &mut self,
            document: WasmDocumentHandle,
        ) -> Result<WasmDocumentHandle, LixError> {
            Ok(document)
        }

        async fn open_file(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenFileInput,
        ) -> Result<WasmFileTransition, LixError> {
            Err(unused())
        }

        async fn open_rows(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenRowsInput,
        ) -> Result<WasmRowTransition, LixError> {
            Err(unused())
        }

        async fn file_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: crate::plugin::runtime::WasmFileUpdate,
        ) -> Result<WasmFileTransition, LixError> {
            Err(unused())
        }

        async fn rows_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: crate::plugin::runtime::WasmRowUpdate,
        ) -> Result<WasmRowTransition, LixError> {
            Err(unused())
        }

        async fn next_change_page(
            &mut self,
            _transition: WasmTransitionHandle,
            _cursor: WasmChangeCursorHandle,
            _max_bytes: u32,
        ) -> Result<Option<WasmChangePage>, LixError> {
            Ok(self.change_pages.pop_front())
        }

        async fn next_edit_page(
            &mut self,
            _transition: WasmTransitionHandle,
            _cursor: WasmEditCursorHandle,
            _max_edits: u32,
            _max_inline_bytes: u32,
        ) -> Result<Option<WasmEditPage>, LixError> {
            Ok(self.edit_pages.pop_front())
        }

        async fn output_len(
            &mut self,
            _transition: WasmTransitionHandle,
            outputs: WasmByteOutputsHandle,
            index: u32,
        ) -> Result<u64, LixError> {
            self.output_len_calls += 1;
            self.outputs
                .get(&(outputs, index))
                .map(|bytes| bytes.len() as u64)
                .ok_or_else(|| invalid_guest("missing fake output"))
        }

        async fn read_output(
            &mut self,
            _transition: WasmTransitionHandle,
            outputs: WasmByteOutputsHandle,
            index: u32,
            offset: u64,
            length: u32,
        ) -> Result<Vec<u8>, LixError> {
            let bytes = self
                .outputs
                .get(&(outputs, index))
                .ok_or_else(|| invalid_guest("missing fake output"))?;
            let start = usize::try_from(offset)
                .map_err(|_| invalid_guest("fake output offset does not fit usize"))?;
            let end = start
                .saturating_add(length as usize)
                .min(start.saturating_add(self.max_read_prefix))
                .min(bytes.len());
            bytes
                .get(start..end)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| invalid_guest("fake output range"))
        }

        async fn finish_transition(
            &mut self,
            _transition: WasmTransitionHandle,
        ) -> Result<WasmTransitionCounters, LixError> {
            self.finished = true;
            Ok(self.runtime_counters)
        }

        async fn discard_transition(
            &mut self,
            transition: WasmTransitionHandle,
        ) -> Result<(), LixError> {
            self.discarded_transitions.push(transition);
            if self.discard_fails {
                return Err(invalid_guest("synthetic transition cleanup failure"));
            }
            Ok(())
        }

        fn is_retired(&self) -> bool {
            self.retired
        }

        async fn retire(&mut self) -> Result<(), LixError> {
            self.retired = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn change_drain_rejects_duplicate_keys_across_pages() {
        let duplicate = WasmChangePage {
            format_version: CURRENT_PACKET_FORMAT,
            changes: WasmRowChanges {
                changes: vec![WasmRowChange::Delete(key("row"))],
            },
            outputs: None,
        };
        let mut actor = FakeActor {
            change_pages: [duplicate.clone(), duplicate].into(),
            ..FakeActor::default()
        };
        let transition = WasmFileTransition {
            transition: WasmTransitionHandle(1),
            document: WasmDocumentHandle(2),
            changes: WasmChangeCursorHandle(3),
            replace_all_rows: false,
        };

        let error = drain_file_transition_changes(
            &mut actor,
            transition,
            test_creates(),
            &SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("the engine drain must remain the transition-wide uniqueness authority");

        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert_eq!(
            error.message,
            "invalid component change cursor page: a component row key may occur only once across a change cursor"
        );
        assert_eq!(actor.discarded_transitions, vec![transition.transition]);
        assert!(!actor.finished);
    }

    #[tokio::test]
    async fn host_validation_rejection_discards_transition_and_allows_retry() {
        let page = WasmChangePage {
            format_version: CURRENT_PACKET_FORMAT,
            changes: WasmRowChanges {
                changes: vec![WasmRowChange::Upsert {
                    row: WasmRow {
                        key: WasmRowKey::from_typed_parts(
                            "not_allowed",
                            [0; 32],
                            vec![lix_schema::Value::Text("row".to_owned())],
                        )
                        .unwrap(),
                        payload: guest_row("row"),
                    },
                    effect: WasmChangeEffect::Content,
                }],
            },
            outputs: None,
        };
        let mut actor = FakeActor {
            change_pages: [page].into(),
            ..FakeActor::default()
        };
        let error = drain_file_transition_changes(
            &mut actor,
            WasmFileTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                changes: WasmChangeCursorHandle(3),
                replace_all_rows: false,
            },
            test_creates(),
            &SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("undeclared schemas must fail closed");
        assert!(error.message.contains("undeclared schema"));
        assert_eq!(actor.output_len_calls, 0);
        assert!(!actor.finished);
        assert_eq!(actor.discarded_transitions, vec![WasmTransitionHandle(1)]);
        assert!(!actor.retired);

        actor.change_pages.push_back(WasmChangePage {
            format_version: CURRENT_PACKET_FORMAT,
            changes: WasmRowChanges {
                changes: vec![WasmRowChange::Delete(key("row"))],
            },
            outputs: None,
        });
        let retried = drain_file_transition_changes(
            &mut actor,
            WasmFileTransition {
                transition: WasmTransitionHandle(4),
                document: WasmDocumentHandle(5),
                changes: WasmChangeCursorHandle(6),
                replace_all_rows: false,
            },
            test_creates(),
            &SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect("a deterministic host rejection must leave the actor reusable");
        assert_eq!(retried.document, WasmDocumentHandle(5));
        assert_eq!(retried.changes.row_change_count(), 1);
        assert!(actor.finished);
        assert!(!actor.retired);
    }

    #[tokio::test]
    async fn uncertain_transition_cleanup_retires_the_actor() {
        let mut actor = FakeActor {
            change_pages: [WasmChangePage {
                format_version: CURRENT_PACKET_FORMAT,
                changes: WasmRowChanges {
                    changes: vec![WasmRowChange::Delete(
                        WasmRowKey::from_typed_parts(
                            "not_allowed",
                            [0; 32],
                            vec![lix_schema::Value::Text("row".to_owned())],
                        )
                        .unwrap(),
                    )],
                },
                outputs: None,
            }]
            .into(),
            discard_fails: true,
            ..FakeActor::default()
        };
        let error = drain_file_transition_changes(
            &mut actor,
            WasmFileTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                changes: WasmChangeCursorHandle(3),
                replace_all_rows: false,
            },
            test_creates(),
            &SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("failed cleanup must reject the transition");
        assert!(
            error
                .message
                .contains("synthetic transition cleanup failure")
        );
        assert!(actor.retired);
    }

    #[tokio::test]
    async fn edit_drain_applies_global_base_coordinates_and_checks_expected_bytes() {
        let outputs = WasmByteOutputsHandle(9);
        let mut actor = FakeActor {
            edit_pages: [
                WasmEditPage {
                    edits: vec![WasmOutputSplice {
                        offset: 1,
                        delete_len: 2,
                        insert: WasmGuestBytes::Inline(b"XY".to_vec().into()),
                    }],
                    outputs: None,
                },
                WasmEditPage {
                    edits: vec![WasmOutputSplice {
                        offset: 5,
                        delete_len: 1,
                        insert: WasmGuestBytes::Output(WasmOutputRange {
                            index: 0,
                            offset: 0,
                            length: 1,
                        }),
                    }],
                    outputs: Some(outputs),
                },
            ]
            .into(),
            ..FakeActor::default()
        };
        actor.outputs.insert((outputs, 0), b"Z".to_vec());
        let drained = drain_row_transition_edits(
            &mut actor,
            WasmRowTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                edits: WasmEditCursorHandle(3),
            },
            b"abcdef",
            Some(b"aXYdeZ".as_slice().into()),
            None,
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(drained.bytes.as_ref(), b"aXYdeZ");
        assert_eq!(drained.edits.len(), 2);
        assert_eq!(drained.counters.packet_pages, 2);
        assert!(actor.finished);
    }

    #[tokio::test]
    async fn edit_drain_marks_one_host_validated_same_length_splice() {
        let mut actor = FakeActor {
            edit_pages: [WasmEditPage {
                edits: vec![WasmOutputSplice {
                    offset: 2,
                    delete_len: 2,
                    insert: WasmGuestBytes::Inline(b"XY".to_vec().into()),
                }],
                outputs: None,
            }]
            .into(),
            ..FakeActor::default()
        };
        let drained = drain_row_transition_edits(
            &mut actor,
            WasmRowTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                edits: WasmEditCursorHandle(3),
            },
            b"abcdef",
            None,
            None,
            WasmTransitionLimits::default(),
        )
        .await
        .expect("one valid fixed-width renderer edit should drain");

        assert_eq!(drained.bytes.as_ref(), b"abXYef");
        assert_eq!(
            drained.same_length_output_splice,
            Some(ValidatedSameLengthOutputSplice {
                offset: 2,
                length: 2,
            })
        );
        assert!(actor.finished);
    }

    #[test]
    fn only_one_nonempty_fixed_width_output_splice_is_cas_eligible() {
        let fixed_width = ResolvedOutputSplice {
            offset: 2,
            delete_len: 2,
            insert: Bytes::from_static(b"XY"),
        };
        assert_eq!(
            same_length_output_splice_after_host_validation(
                b"abcdef".len(),
                b"abXYef".len(),
                std::slice::from_ref(&fixed_width),
            ),
            Some(ValidatedSameLengthOutputSplice {
                offset: 2,
                length: 2,
            })
        );

        let length_changing = ResolvedOutputSplice {
            offset: 2,
            delete_len: 1,
            insert: Bytes::from_static(b"XY"),
        };
        assert_eq!(
            same_length_output_splice_after_host_validation(
                b"abcdef".len(),
                b"abXYdef".len(),
                std::slice::from_ref(&length_changing),
            ),
            None
        );
        assert_eq!(
            same_length_output_splice_after_host_validation(
                b"abcdef".len(),
                b"aXcdYf".len(),
                &[
                    ResolvedOutputSplice {
                        offset: 1,
                        delete_len: 1,
                        insert: Bytes::from_static(b"X"),
                    },
                    ResolvedOutputSplice {
                        offset: 4,
                        delete_len: 1,
                        insert: Bytes::from_static(b"Y"),
                    },
                ],
            ),
            None,
        );
        assert_eq!(
            same_length_output_splice_after_host_validation(
                b"abcdef".len(),
                b"abcdef".len(),
                &[ResolvedOutputSplice {
                    offset: 6,
                    delete_len: 1,
                    insert: Bytes::from_static(b"X"),
                }],
            ),
            None,
        );
    }

    #[tokio::test]
    async fn edit_drain_reuses_exact_expected_blob_after_local_delta_proof() {
        let mut actor = FakeActor {
            edit_pages: [WasmEditPage {
                edits: vec![WasmOutputSplice {
                    offset: 1,
                    delete_len: 4,
                    insert: WasmGuestBytes::Inline(b"bXYe".to_vec().into()),
                }],
                outputs: None,
            }]
            .into(),
            ..FakeActor::default()
        };
        let expected: Blob = b"abXYef".as_slice().into();
        let input = [WasmInputSplice {
            offset: 2,
            delete_len: 2,
            insert: WasmInputBytes::Inline(b"XY".to_vec()),
        }];
        let drained = drain_row_transition_edits(
            &mut actor,
            WasmRowTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                edits: WasmEditCursorHandle(3),
            },
            b"abcdef",
            Some(expected.clone()),
            Some(&input),
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(drained.bytes.as_ptr(), expected.as_ptr());
        assert_eq!(drained.bytes.len(), expected.len());
        assert_eq!(
            drained.same_length_output_splice, None,
            "known-delta validation reuses independently supplied bytes and never grants CAS splice provenance"
        );
        assert!(actor.finished);
    }
}
