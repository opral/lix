//! Engine-side plumbing for incremental Wasm Component transitions.
//!
//! This module owns validation and bounded host adapters around the host-
//! neutral `wasm` traits. It deliberately does not decide transaction,
//! conflict-resolution, observation, or actor-publication policy.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::Deserialize;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use sha2::{Digest as _, Sha256};
use tracing::Instrument as _;

use crate::catalog::{CatalogSnapshot, SchemaPlan, SchemaPlanFingerprint};
use crate::common::{RequestBlobSpliceProvenance, SharedStr};
use crate::entity_pk::EntityPk;
use crate::hot_state::MaterializedHotStateBatch;
use crate::plugin::runtime::{
    EDIT_SPLICE_METADATA_BYTES, PACKET_FORMAT_V1, WasmByteOutputsHandle, WasmByteSource,
    WasmCanonicalJson, WasmCanonicalJsonCertificate, WasmCertifiedEntityBatch,
    WasmChangeDrainValidator, WasmChangePage, WasmComponentActor, WasmConflictResolution,
    WasmConflictResolutionDrainValidator, WasmConflictResolutionPage, WasmConflictTransition,
    WasmDocumentHandle, WasmEditDrainValidator, WasmEditPage, WasmEntity, WasmEntityChange,
    WasmEntityChangeSource, WasmEntityChanges, WasmEntityConflict, WasmEntityConflictPage,
    WasmEntityConflictSource, WasmEntityKey, WasmEntityPage, WasmEntitySource,
    WasmEntityTransition, WasmFileTransition, WasmGuestBytes, WasmHostBytes,
    WasmHostConflictResolution, WasmHostEntity, WasmHostEntityChanges, WasmInputBytes,
    WasmInputSplice, WasmOutputRange, WasmSourceRange, WasmTransitionCounters,
    WasmTransitionHandle, WasmTransitionLimits, validate_change_cursor_key_uniqueness,
};
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

/// Builds one host entity while keeping an indivisible oversized snapshot out
/// of the packet's inline record arena.
///
/// Packet-v1 records cannot cross pages. The entity key, blob tag/length, and
/// four-byte record frame all consume the record/page limits, so deciding from
/// the snapshot length alone has an off-by-overhead failure at the boundary.
/// A lazy source keeps the record bounded while retaining immutable ownership
/// of the complete Snapshot JSON for the guest to read on demand.
pub(crate) fn host_entity_with_lazy_snapshot(
    key: WasmEntityKey,
    snapshot: Bytes,
    limits: WasmTransitionLimits,
) -> Result<WasmHostEntity, LixError> {
    let limits = limits.validate()?;
    let mut entity = WasmEntity {
        key,
        snapshot_content: WasmHostBytes::Inline(snapshot),
    };
    if host_entity_record_fits(&entity, limits) {
        return Ok(entity);
    }

    entity.snapshot_content = lazy_source_from_inline(entity.snapshot_content);
    validate_host_entity(&entity)?;
    require_framed_record_fits(encoded_entity_record_bytes(&entity)?, limits, "entity")?;
    Ok(entity)
}

/// Change-record counterpart of [`host_entity_with_lazy_snapshot`].
pub(crate) fn host_entity_change_with_lazy_snapshot(
    key: WasmEntityKey,
    snapshot: Bytes,
    effect: crate::plugin::runtime::WasmChangeEffect,
    limits: WasmTransitionLimits,
) -> Result<WasmEntityChange<WasmHostBytes>, LixError> {
    let limits = limits.validate()?;
    let mut change = WasmEntityChange::Upsert {
        entity: WasmEntity {
            key,
            snapshot_content: WasmHostBytes::Inline(snapshot),
        },
        effect,
    };
    if host_change_record_fits(&change, limits) {
        return Ok(change);
    }

    let WasmEntityChange::Upsert { entity, .. } = &mut change else {
        unreachable!("the helper constructs an upsert")
    };
    let snapshot = std::mem::replace(
        &mut entity.snapshot_content,
        WasmHostBytes::Inline(Bytes::new()),
    );
    entity.snapshot_content = lazy_source_from_inline(snapshot);
    validate_host_entity(entity)?;
    require_framed_record_fits(
        encoded_entity_change_record_bytes(&change)?,
        limits,
        "entity change",
    )?;
    Ok(change)
}

fn lazy_source_from_inline(bytes: WasmHostBytes) -> WasmHostBytes {
    let WasmHostBytes::Inline(bytes) = bytes else {
        return bytes;
    };
    let length = bytes.len() as u64;
    WasmHostBytes::Source(crate::plugin::runtime::WasmSourceSlice {
        source: Arc::new(ArcByteSource::new(bytes.into())),
        range: WasmSourceRange { offset: 0, length },
    })
}

fn host_entity_record_fits(entity: &WasmHostEntity, limits: WasmTransitionLimits) -> bool {
    encoded_entity_record_bytes(entity)
        .ok()
        .is_some_and(|bytes| framed_record_fits(bytes, limits))
}

fn host_change_record_fits(
    change: &WasmEntityChange<WasmHostBytes>,
    limits: WasmTransitionLimits,
) -> bool {
    encoded_entity_change_record_bytes(change)
        .ok()
        .is_some_and(|bytes| framed_record_fits(bytes, limits))
}

fn framed_record_fits(bytes: u64, limits: WasmTransitionLimits) -> bool {
    bytes <= u64::from(limits.max_record_bytes)
        && bytes
            .checked_add(4)
            .is_some_and(|framed| framed <= u64::from(limits.max_page_bytes))
}

fn require_framed_record_fits(
    bytes: u64,
    limits: WasmTransitionLimits,
    kind: &str,
) -> Result<(), LixError> {
    if bytes > u64::from(limits.max_record_bytes) {
        return Err(invalid_input(format!(
            "component {kind} record exceeds max_record_bytes even with a lazy snapshot attachment"
        )));
    }
    if bytes
        .checked_add(4)
        .is_none_or(|framed| framed > u64::from(limits.max_page_bytes))
    {
        return Err(invalid_input(format!(
            "component {kind} record does not fit max_page_bytes even with a lazy snapshot attachment"
        )));
    }
    Ok(())
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
    // This sidecar is constructed only after the remote protocol verifies the
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
}

struct NumberFreeJsonValue(serde_json::Value);

impl<'de> Deserialize<'de> for NumberFreeJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_any(NumberFreeJsonValueVisitor)
            .map(Self)
    }
}

struct NumberFreeJsonValueVisitor;

impl<'de> Visitor<'de> for NumberFreeJsonValueVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("number-free JSON")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(serde_json::Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(value))
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom(
            "JSON numbers are not enabled for production component",
        ))
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom(
            "JSON numbers are not enabled for production component",
        ))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom(
            "JSON numbers are not enabled for production component",
        ))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(NumberFreeJsonValue(value)) = sequence.next_element()? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom(format!(
                    "duplicate decoded JSON object key '{key}'"
                )));
            }
            let NumberFreeJsonValue(value) = map.next_value()?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

/// Parses, duplicate-checks, number-gates, and canonically encodes one component
/// snapshot. Snapshot roots must be objects.
pub(crate) fn canonicalize_snapshot(bytes: &[u8]) -> Result<Vec<u8>, LixError> {
    let value = parse_number_free_snapshot(bytes)?;
    if !value.is_object() {
        return Err(invalid_guest(
            "component entity snapshots must be JSON objects",
        ));
    }
    let mut canonical = Vec::new();
    encode_number_free_json(&value, &mut canonical)?;
    Ok(canonical)
}

#[derive(Debug)]
struct CanonicalJsonBatchBuilder {
    row_kinds: Vec<CanonicalJsonBatchRowKind>,
    decoded_values: Vec<serde_json::Value>,
    certified_normalized: Vec<SharedStr>,
    certified_entity_pks: Vec<EntityPk>,
    schema_fingerprints: Vec<Arc<SchemaPlanFingerprint>>,
    schema_fingerprint_indices: Vec<u32>,
    normalized_ends: Vec<u32>,
    parse_count: usize,
    serialize_count: usize,
    normalized_len: u32,
    row_capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum CanonicalJsonBatchRowKind {
    Decoded,
    Certified,
}

impl CanonicalJsonBatchBuilder {
    fn with_row_capacity(row_count: usize) -> Self {
        Self {
            row_kinds: Vec::with_capacity(row_count),
            decoded_values: Vec::new(),
            certified_normalized: Vec::new(),
            certified_entity_pks: Vec::new(),
            schema_fingerprints: Vec::new(),
            schema_fingerprint_indices: Vec::new(),
            normalized_ends: Vec::with_capacity(row_count),
            parse_count: 0,
            serialize_count: 0,
            normalized_len: 0,
            row_capacity: row_count,
        }
    }

    fn reserve_decoded_column(&mut self) {
        if self.decoded_values.capacity() == 0 {
            self.decoded_values.reserve_exact(self.row_capacity);
        }
    }

    fn reserve_certified_columns(&mut self) {
        if self.certified_normalized.capacity() == 0 {
            self.certified_normalized.reserve_exact(self.row_capacity);
            self.certified_entity_pks.reserve_exact(self.row_capacity);
            self.schema_fingerprint_indices
                .reserve_exact(self.row_capacity);
        }
    }

    fn schema_fingerprint_index(
        &mut self,
        fingerprint: Arc<SchemaPlanFingerprint>,
    ) -> Result<u32, LixError> {
        if let Some(index) = self
            .schema_fingerprints
            .iter()
            .position(|existing| Arc::ptr_eq(existing, &fingerprint))
        {
            return u32::try_from(index)
                .map_err(|_| invalid_guest("component canonical JSON page has too many schemas"));
        }
        let index = u32::try_from(self.schema_fingerprints.len())
            .map_err(|_| invalid_guest("component canonical JSON page has too many schemas"))?;
        self.schema_fingerprints.push(fingerprint);
        Ok(index)
    }

    fn push(&mut self, bytes: &[u8]) -> Result<usize, LixError> {
        self.parse_count = self.parse_count.saturating_add(1);
        let value = parse_number_free_snapshot(bytes)?;
        if !value.is_object() {
            return Err(invalid_guest(
                "component entity snapshots must be JSON objects",
            ));
        }

        let encoded_len = canonical_json_encoded_len(&value)?;
        let start = self.normalized_len;
        let end = start
            .checked_add(encoded_len)
            .ok_or_else(|| invalid_guest("component canonical JSON page exceeds u32"))?;
        self.normalized_len = end;
        let row = self.row_kinds.len();
        self.reserve_decoded_column();
        self.decoded_values.push(value);
        self.row_kinds.push(CanonicalJsonBatchRowKind::Decoded);
        self.normalized_ends.push(end);
        Ok(row)
    }

    fn push_plugin(
        &mut self,
        bytes: Bytes,
        key: &WasmEntityKey,
        schemas: &SchemaAllowlist,
    ) -> Result<usize, LixError> {
        self.parse_count = self.parse_count.saturating_add(1);
        let plan = schemas.schema_plan(&key.schema_key);
        let certificate = if let Some(plan) = plan {
            plan.certify_or_normalize_plugin_row(&bytes, key)?
                .map(|row| (row, plan.shared_fingerprint()))
        } else {
            None
        };
        if let Some((certified, schema_fingerprint)) = certificate {
            let normalized = match certified.normalized {
                Some(normalized) => {
                    self.serialize_count = self.serialize_count.saturating_add(1);
                    Bytes::from(normalized)
                }
                None => bytes,
            };
            let encoded_len = u32::try_from(normalized.len())
                .map_err(|_| invalid_guest("component canonical JSON row exceeds u32"))?;
            let start = self.normalized_len;
            let end = start
                .checked_add(encoded_len)
                .ok_or_else(|| invalid_guest("component canonical JSON page exceeds u32"))?;
            self.normalized_len = end;
            let row = self.row_kinds.len();
            self.reserve_certified_columns();
            let schema_fingerprint_index = self.schema_fingerprint_index(schema_fingerprint)?;
            self.certified_normalized.push(
                SharedStr::from_utf8(normalized)
                    .map_err(|_| invalid_guest("certified canonical JSON row is not UTF-8"))?,
            );
            self.certified_entity_pks.push(certified.entity_pk);
            self.schema_fingerprint_indices
                .push(schema_fingerprint_index);
            self.row_kinds.push(CanonicalJsonBatchRowKind::Certified);
            self.normalized_ends.push(end);
            return Ok(row);
        }

        // Plans that cannot use the streaming certificate parser take one DOM
        // pass and serialize once at batch finalization.
        let value = parse_number_free_snapshot(&bytes)?;
        if !value.is_object() {
            return Err(invalid_guest(
                "component entity snapshots must be JSON objects",
            ));
        }
        if let Some(plan) = plan {
            if let Err(errors) = plan.compiled_schema.validate(&value) {
                let details = errors
                    .take(3)
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");
                return Err(invalid_guest(format!(
                    "component snapshot failed schema validation: {details}"
                )));
            }
            let primary_key = plan.primary_key.as_deref().ok_or_else(|| {
                invalid_guest("component snapshot schema has no primary-key definition")
            })?;
            let emitted = json_pointer_components(&value, primary_key)?;
            if emitted.as_slice() != key.entity_pk.as_slice() {
                return Err(invalid_guest(
                    "component snapshot primary key does not match its entity key",
                ));
            }
            let component_types = plan.primary_key_component_types.as_deref().ok_or_else(|| {
                invalid_guest("component snapshot schema has no typed primary key")
            })?;
            let entity_pk =
                EntityPk::from_external_parts(emitted, component_types).map_err(|error| {
                    invalid_guest(format!("component entity key is invalid: {error}"))
                })?;
            let encoded_len = canonical_json_encoded_len(&value)?;
            let mut normalized = Vec::with_capacity(encoded_len as usize);
            encode_number_free_json(&value, &mut normalized)?;
            self.serialize_count = self.serialize_count.saturating_add(1);
            let start = self.normalized_len;
            let end = start
                .checked_add(encoded_len)
                .ok_or_else(|| invalid_guest("component canonical JSON page exceeds u32"))?;
            self.normalized_len = end;
            let row = self.row_kinds.len();
            self.reserve_certified_columns();
            let schema_fingerprint_index =
                self.schema_fingerprint_index(plan.shared_fingerprint())?;
            self.certified_normalized.push(
                SharedStr::from_utf8(Bytes::from(normalized))
                    .map_err(|_| invalid_guest("certified canonical JSON row is not UTF-8"))?,
            );
            self.certified_entity_pks.push(entity_pk);
            self.schema_fingerprint_indices
                .push(schema_fingerprint_index);
            self.row_kinds.push(CanonicalJsonBatchRowKind::Certified);
            self.normalized_ends.push(end);
            return Ok(row);
        }
        let encoded_len = canonical_json_encoded_len(&value)?;
        let start = self.normalized_len;
        let end = start
            .checked_add(encoded_len)
            .ok_or_else(|| invalid_guest("component canonical JSON page exceeds u32"))?;
        self.normalized_len = end;
        let row = self.row_kinds.len();
        self.reserve_decoded_column();
        self.decoded_values.push(value);
        self.row_kinds.push(CanonicalJsonBatchRowKind::Decoded);
        self.normalized_ends.push(end);
        Ok(row)
    }

    fn finish(self) -> Result<Vec<WasmCanonicalJson>, LixError> {
        if self.decoded_values.is_empty()
            && self.certified_normalized.len() == self.row_kinds.len()
            && self.serialize_count == 0
        {
            debug_assert!(
                self.row_kinds
                    .iter()
                    .all(|kind| *kind == CanonicalJsonBatchRowKind::Certified)
            );
            debug_assert_eq!(
                self.certified_normalized
                    .iter()
                    .map(|row| row.len())
                    .sum::<usize>(),
                self.normalized_len as usize
            );
            return WasmCanonicalJson::from_certified_batch_parts(
                self.certified_normalized,
                self.certified_entity_pks,
                self.schema_fingerprints,
                self.schema_fingerprint_indices,
                self.parse_count,
            );
        }

        let mut normalized = Vec::with_capacity(self.normalized_len as usize);
        let mut values = Vec::with_capacity(self.row_kinds.len());
        let mut certificates = Vec::with_capacity(self.row_kinds.len());
        let mut offsets = Vec::with_capacity(self.row_kinds.len());
        let mut decoded_values = self.decoded_values.into_iter();
        let mut certified_normalized = self.certified_normalized.into_iter();
        let mut certified_entity_pks = self.certified_entity_pks.into_iter();
        let mut schema_fingerprint_indices = self.schema_fingerprint_indices.into_iter();
        let mut serialize_count = self.serialize_count;
        let mut start = 0_u32;
        for (kind, end) in self.row_kinds.into_iter().zip(self.normalized_ends) {
            debug_assert_eq!(normalized.len(), start as usize);
            match kind {
                CanonicalJsonBatchRowKind::Decoded => {
                    let value = decoded_values
                        .next()
                        .expect("decoded canonical JSON row has a value");
                    encode_number_free_json(&value, &mut normalized)?;
                    values.push(Some(value));
                    certificates.push(None);
                    serialize_count += 1;
                }
                CanonicalJsonBatchRowKind::Certified => {
                    let canonical = certified_normalized
                        .next()
                        .expect("certified canonical JSON row has bytes");
                    let entity_pk = certified_entity_pks
                        .next()
                        .expect("certified canonical JSON row has an entity identity");
                    let schema_fingerprint_index = schema_fingerprint_indices
                        .next()
                        .expect("certified canonical JSON row has a schema index");
                    let schema_fingerprint = self
                        .schema_fingerprints
                        .get(schema_fingerprint_index as usize)
                        .expect("certified canonical JSON row schema index was interned")
                        .clone();
                    normalized.extend_from_slice(canonical.as_bytes());
                    values.push(None);
                    certificates.push(Some(WasmCanonicalJsonCertificate::new(
                        entity_pk,
                        schema_fingerprint,
                    )));
                }
            }
            debug_assert_eq!(normalized.len(), end as usize);
            offsets.push((start, end));
            start = end;
        }
        #[cfg(debug_assertions)]
        {
            assert!(decoded_values.next().is_none());
            assert!(certified_normalized.next().is_none());
            assert!(certified_entity_pks.next().is_none());
            assert!(schema_fingerprint_indices.next().is_none());
        }
        debug_assert_eq!(normalized.len(), normalized.capacity());
        WasmCanonicalJson::from_mixed_batch_parts(
            values,
            certificates,
            normalized,
            offsets,
            self.parse_count,
            serialize_count,
        )
    }
}

fn canonical_json_encoded_len(value: &serde_json::Value) -> Result<u32, LixError> {
    fn add(total: &mut u64, value: u64) -> Result<(), LixError> {
        *total = total
            .checked_add(value)
            .ok_or_else(|| invalid_guest("component canonical JSON size overflowed"))?;
        Ok(())
    }

    fn visit(value: &serde_json::Value, total: &mut u64) -> Result<(), LixError> {
        match value {
            serde_json::Value::Null => add(total, 4),
            serde_json::Value::Bool(true) => add(total, 4),
            serde_json::Value::Bool(false) => add(total, 5),
            serde_json::Value::Number(_) => Err(invalid_guest(
                "JSON numbers are not enabled for production component",
            )),
            serde_json::Value::String(value) => encoded_json_string_len(value, total),
            serde_json::Value::Array(values) => {
                add(total, 2)?;
                if values.len() > 1 {
                    add(total, (values.len() - 1) as u64)?;
                }
                for value in values {
                    visit(value, total)?;
                }
                Ok(())
            }
            serde_json::Value::Object(values) => {
                add(total, 2)?;
                if values.len() > 1 {
                    add(total, (values.len() - 1) as u64)?;
                }
                for (key, value) in values {
                    encoded_json_string_len(key, total)?;
                    add(total, 1)?;
                    visit(value, total)?;
                }
                Ok(())
            }
        }
    }

    fn encoded_json_string_len(value: &str, total: &mut u64) -> Result<(), LixError> {
        add(total, 2)?;
        for scalar in value.chars() {
            add(
                total,
                match scalar {
                    '"' | '\\' | '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
                    scalar if scalar <= '\u{1f}' => 6,
                    scalar => scalar.len_utf8() as u64,
                },
            )?;
        }
        Ok(())
    }

    let mut total = 0;
    visit(value, &mut total)?;
    u32::try_from(total).map_err(|_| invalid_guest("component canonical JSON row exceeds u32"))
}

fn parse_number_free_snapshot(bytes: &[u8]) -> Result<serde_json::Value, LixError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let NumberFreeJsonValue(value) =
        NumberFreeJsonValue::deserialize(&mut deserializer).map_err(|error| {
            invalid_guest(format!(
                "component snapshot must be duplicate-free number-free UTF-8 JSON: {error}"
            ))
        })?;
    deserializer.end().map_err(|error| {
        invalid_guest(format!(
            "component snapshot contains trailing or invalid JSON input: {error}"
        ))
    })?;
    Ok(value)
}

fn encode_number_free_json(
    value: &serde_json::Value,
    output: &mut Vec<u8>,
) -> Result<(), LixError> {
    match value {
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Bool(true) => output.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => output.extend_from_slice(b"false"),
        serde_json::Value::String(value) => encode_json_string(value, output),
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                encode_number_free_json(value, output)?;
            }
            output.push(b']');
        }
        serde_json::Value::Object(values) => {
            output.push(b'{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                encode_json_string(key, output);
                output.push(b':');
                encode_number_free_json(value, output)?;
            }
            output.push(b'}');
        }
        serde_json::Value::Number(_) => {
            return Err(invalid_guest(
                "JSON numbers are not enabled for production component",
            ));
        }
    }
    Ok(())
}

fn encode_json_string(value: &str, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    for scalar in value.chars() {
        match scalar {
            '"' => output.extend_from_slice(br#"\""#),
            '\\' => output.extend_from_slice(br#"\\"#),
            '\u{08}' => output.extend_from_slice(br#"\b"#),
            '\t' => output.extend_from_slice(br#"\t"#),
            '\n' => output.extend_from_slice(br#"\n"#),
            '\u{0c}' => output.extend_from_slice(br#"\f"#),
            '\r' => output.extend_from_slice(br#"\r"#),
            scalar if scalar <= '\u{1f}' => {
                let scalar = scalar as usize;
                output.extend_from_slice(b"\\u00");
                output.push(HEX[(scalar >> 4) & 0x0f]);
                output.push(HEX[scalar & 0x0f]);
            }
            scalar => {
                let mut encoded = [0_u8; 4];
                output.extend_from_slice(scalar.encode_utf8(&mut encoded).as_bytes());
            }
        }
    }
    output.push(b'"');
}

/// Vec-backed complete-entity packet source for cold rendering. Construction
/// enforces packet-v1's global key order.
#[derive(Debug)]
pub(crate) struct VecEntitySource {
    entities: VecDeque<WasmHostEntity>,
    state: VecSourceState,
}

impl VecEntitySource {
    pub(crate) fn new(
        entities: Vec<WasmHostEntity>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        validate_entity_order(&entities)?;
        for entity in &entities {
            validate_host_entity(entity)?;
        }
        Ok(Self {
            entities: entities.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmEntitySource for VecEntitySource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.entities.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut page_refs = 0u32;
        let mut entities = Vec::new();
        while let Some(entity) = self.entities.front() {
            let record_bytes = encoded_entity_record_bytes(entity)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component entity record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component entity frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if entities.is_empty() {
                    return Err(invalid_input(
                        "component entity record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            page_refs = page_refs
                .checked_add(host_bytes_attachment_refs(&entity.snapshot_content))
                .ok_or_else(|| invalid_input("component entity attachment count overflowed"))?;
            entities.push(
                self.entities
                    .pop_front()
                    .expect("front entity was just inspected"),
            );
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmEntityPage { entities }))
    }
}

/// Page-lazy complete-entity source backed by the engine's columnar live-state
/// batch. This keeps shared snapshot buffers in their storage-native owner and
/// constructs generic Wasm entities only for the page currently crossing the
/// component boundary.
#[derive(Debug)]
pub(crate) struct LiveBatchEntitySource {
    rows: MaterializedHotStateBatch,
    ordinals: VecDeque<u32>,
    pending: Option<WasmHostEntity>,
    state: VecSourceState,
}

impl LiveBatchEntitySource {
    pub(crate) fn new(
        rows: MaterializedHotStateBatch,
        ordinals: Vec<u32>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        for pair in ordinals.windows(2) {
            let left = rows.row(pair[0] as usize);
            let right = rows.row(pair[1] as usize);
            if left.schema_key() == right.schema_key() && left.entity_pk() == right.entity_pk() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "durable component entity hydration returned duplicate keys",
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

    fn next_entity(&mut self) -> Result<Option<WasmHostEntity>, LixError> {
        if let Some(entity) = self.pending.take() {
            return Ok(Some(entity));
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
        let snapshot = row.snapshot_content().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "plugin state selection references a tombstoned row",
            )
        })?;
        host_entity_with_lazy_snapshot(
            WasmEntityKey::from_owned_parts(
                row.schema_key().to_owned(),
                row.entity_pk().clone().into_parts(),
            ),
            snapshot.clone().into_bytes(),
            self.state.limits,
        )
        .map(Some)
    }
}

impl WasmEntitySource for LiveBatchEntitySource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        let mut page_bytes = 0_u64;
        let mut page_refs = 0_u32;
        let mut entities = Vec::new();
        while let Some(entity) = self.next_entity()? {
            let record_bytes = encoded_entity_record_bytes(&entity)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component entity record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component entity frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if entities.is_empty() {
                    return Err(invalid_input(
                        "component entity record does not fit the requested page",
                    ));
                }
                self.pending = Some(entity);
                break;
            }
            page_bytes += framed_bytes;
            page_refs = page_refs
                .checked_add(host_bytes_attachment_refs(&entity.snapshot_content))
                .ok_or_else(|| invalid_input("component entity attachment count overflowed"))?;
            entities.push(entity);
        }
        if entities.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmEntityPage { entities }))
    }
}

/// Vec-backed source for the final resolved semantic changes supplied to
/// `entities_changed`.
#[derive(Debug)]
pub(crate) struct VecEntityChangeSource {
    changes: VecDeque<WasmEntityChange<WasmHostBytes>>,
    state: VecSourceState,
}

impl VecEntityChangeSource {
    pub(crate) fn new(
        changes: WasmHostEntityChanges,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        changes.validate()?;
        validate_change_order(&changes.changes)?;
        for change in &changes.changes {
            match change {
                WasmEntityChange::Create { .. } => {
                    return Err(invalid_input(
                        "host-to-guest entity changes cannot contain keyless creates",
                    ));
                }
                WasmEntityChange::Upsert { entity, .. } => validate_host_entity(entity)?,
                WasmEntityChange::Delete(key) if key.entity_pk.is_empty() => {
                    return Err(invalid_input(
                        "component entity primary keys must not be empty",
                    ));
                }
                WasmEntityChange::Delete(_) => {}
            }
        }
        Ok(Self {
            changes: changes.changes.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmEntityChangeSource for VecEntityChangeSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmHostEntityChanges>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.changes.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut page_refs = 0u32;
        let mut changes = Vec::new();
        while let Some(change) = self.changes.front() {
            let record_bytes = encoded_entity_change_record_bytes(change)?;
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
            page_refs = page_refs
                .checked_add(change_attachment_refs(change))
                .ok_or_else(|| invalid_input("component change attachment count overflowed"))?;
            changes.push(
                self.changes
                    .pop_front()
                    .expect("front change was just inspected"),
            );
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmEntityChanges { changes }))
    }
}

/// Vec-backed lazy source for same-identity three-way conflict triples. The
/// source is deliberately separate from `VecEntityChangeSource`: a conflict
/// exposes three immutable versions, while an entity update exposes only the
/// already resolved final state.
#[derive(Debug)]
pub(crate) struct VecEntityConflictSource {
    conflicts: VecDeque<WasmEntityConflict<WasmHostBytes>>,
    state: VecSourceState,
}

impl VecEntityConflictSource {
    pub(crate) fn new(
        conflicts: Vec<WasmEntityConflict<WasmHostBytes>>,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        validate_conflict_order(&conflicts)?;
        for (expected_ordinal, conflict) in conflicts.iter().enumerate() {
            if conflict.ordinal
                != u32::try_from(expected_ordinal).map_err(|_| {
                    invalid_input("component conflict source has more than u32::MAX records")
                })?
            {
                return Err(invalid_input(
                    "component conflict source ordinals must be zero-based and contiguous",
                ));
            }
            for snapshot in [&conflict.base, &conflict.a, &conflict.b]
                .into_iter()
                .flatten()
            {
                validate_host_entity(&WasmEntity {
                    key: conflict.key.clone(),
                    snapshot_content: snapshot.clone(),
                })?;
            }
        }
        Ok(Self {
            conflicts: conflicts.into(),
            state: VecSourceState::new(limits)?,
        })
    }
}

impl WasmEntityConflictSource for VecEntityConflictSource {
    fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityConflictPage>, LixError> {
        if self.state.reached_eof {
            return Ok(None);
        }
        let page_limit = self.state.page_limit(max_bytes)?;
        if self.conflicts.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut page_refs = 0u32;
        let mut conflicts = Vec::new();
        while let Some(conflict) = self.conflicts.front() {
            let record_bytes = encoded_entity_conflict_record_bytes(conflict)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "component conflict record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("component conflict frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if conflicts.is_empty() {
                    return Err(invalid_input(
                        "component conflict record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            for snapshot in [&conflict.base, &conflict.a, &conflict.b]
                .into_iter()
                .flatten()
            {
                page_refs = page_refs
                    .checked_add(host_bytes_attachment_refs(snapshot))
                    .ok_or_else(|| {
                        invalid_input("component conflict attachment count overflowed")
                    })?;
            }
            conflicts.push(
                self.conflicts
                    .pop_front()
                    .expect("front conflict was just inspected"),
            );
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmEntityConflictPage { conflicts }))
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

fn validate_entity_order(entities: &[WasmHostEntity]) -> Result<(), LixError> {
    for pair in entities.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(invalid_input(
                "component complete entity sources must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_change_order<B>(changes: &[WasmEntityChange<B>]) -> Result<(), LixError> {
    if changes
        .iter()
        .any(|change| matches!(change, WasmEntityChange::Create { .. }))
    {
        return Ok(());
    }
    for pair in changes.windows(2) {
        if pair[0].entity_key() >= pair[1].entity_key() {
            return Err(invalid_input(
                "component entity changes must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_conflict_order(
    conflicts: &[WasmEntityConflict<WasmHostBytes>],
) -> Result<(), LixError> {
    for conflict in conflicts {
        if conflict.key.entity_pk.is_empty() {
            return Err(invalid_input(
                "component conflict entity primary keys must not be empty",
            ));
        }
    }
    for pair in conflicts.windows(2) {
        if pair[0].key > pair[1].key {
            return Err(invalid_input(
                "component conflict sources must be key-sorted",
            ));
        }
    }
    Ok(())
}

fn validate_host_entity(entity: &WasmHostEntity) -> Result<(), LixError> {
    if entity.key.entity_pk.is_empty() {
        return Err(invalid_input(
            "component entity primary keys must not be empty",
        ));
    }
    if let WasmHostBytes::Source(slice) = &entity.snapshot_content {
        slice.validate()?;
    }
    Ok(())
}

fn encoded_entity_record_bytes(entity: &WasmHostEntity) -> Result<u64, LixError> {
    entity
        .key
        .entity_pk
        .iter()
        .try_fold(
            entity.key.schema_key.len() as u64 + 32,
            |size, component| {
                size.checked_add(component.len() as u64)
                    .ok_or_else(|| invalid_input("component entity record size overflowed"))
            },
        )?
        .checked_add(encoded_host_bytes_ref_bytes(&entity.snapshot_content)?)
        .ok_or_else(|| invalid_input("component entity record size overflowed"))
}

fn encoded_entity_change_record_bytes(
    change: &WasmEntityChange<WasmHostBytes>,
) -> Result<u64, LixError> {
    if let WasmEntityChange::Create {
        schema_key,
        snapshot_content,
        ..
    } = change
    {
        let snapshot_bytes = encoded_host_bytes_ref_bytes(snapshot_content)?;
        return (schema_key.len() as u64)
            .checked_add(32)
            .and_then(|size| size.checked_add(snapshot_bytes))
            .ok_or_else(|| invalid_input("component create record size overflowed"));
    }
    let key = change
        .entity_key()
        .expect("non-create change has an entity key");
    let mut size =
        key.entity_pk
            .iter()
            .try_fold(key.schema_key.len() as u64 + 32, |size, component| {
                size.checked_add(component.len() as u64)
                    .ok_or_else(|| invalid_input("component change record size overflowed"))
            })?;
    if let WasmEntityChange::Upsert { entity, .. } = change {
        size = size
            .checked_add(encoded_host_bytes_ref_bytes(&entity.snapshot_content)?)
            .ok_or_else(|| invalid_input("component change record size overflowed"))?;
    }
    Ok(size)
}

fn encoded_entity_conflict_record_bytes(
    conflict: &WasmEntityConflict<WasmHostBytes>,
) -> Result<u64, LixError> {
    let mut size = encoded_entity_key_bytes(&conflict.key)?
        .checked_add(4)
        .ok_or_else(|| invalid_input("component conflict record size overflowed"))?;
    for snapshot in [&conflict.base, &conflict.a, &conflict.b] {
        // One state tag: 0 for an absent/tombstoned value, 1 followed by the
        // normal lazy blob reference for a live complete snapshot.
        size = size
            .checked_add(1)
            .ok_or_else(|| invalid_input("component conflict record size overflowed"))?;
        if let Some(snapshot) = snapshot {
            size = size
                .checked_add(encoded_host_bytes_ref_bytes(snapshot)?)
                .ok_or_else(|| invalid_input("component conflict record size overflowed"))?;
        }
    }
    Ok(size)
}

fn encoded_entity_key_bytes(key: &WasmEntityKey) -> Result<u64, LixError> {
    if key.entity_pk.is_empty() {
        return Err(invalid_input(
            "component entity primary keys must not be empty",
        ));
    }
    let _ = u32::try_from(key.entity_pk.len())
        .map_err(|_| invalid_input("component entity primary key has too many components"))?;
    let mut size = encoded_text_bytes(&key.schema_key)?
        .checked_add(4)
        .ok_or_else(|| invalid_input("component entity key size overflowed"))?;
    for component in &key.entity_pk {
        size = size
            .checked_add(encoded_text_bytes(component)?)
            .ok_or_else(|| invalid_input("component entity key size overflowed"))?;
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
        WasmHostBytes::Inline(bytes) => {
            let length = u32::try_from(bytes.len())
                .map_err(|_| invalid_input("component inline snapshot exceeds u32 framing"))?;
            Ok(1 + 4 + u64::from(length))
        }
        WasmHostBytes::CanonicalJson(json) => {
            let length = u32::try_from(json.normalized().len())
                .map_err(|_| invalid_input("component inline snapshot exceeds u32 framing"))?;
            Ok(1 + 4 + u64::from(length))
        }
        WasmHostBytes::Source(slice) => {
            slice.validate()?;
            Ok(1 + 4 + 8 + 8)
        }
    }
}

fn host_bytes_attachment_refs(value: &WasmHostBytes) -> u32 {
    u32::from(matches!(value, WasmHostBytes::Source(_)))
}

fn change_attachment_refs(change: &WasmEntityChange<WasmHostBytes>) -> u32 {
    match change {
        WasmEntityChange::Create {
            snapshot_content, ..
        } => u32::from(matches!(snapshot_content, WasmHostBytes::Source(_))),
        WasmEntityChange::Upsert { entity, .. } => {
            host_bytes_attachment_refs(&entity.snapshot_content)
        }
        WasmEntityChange::Delete(_) => 0,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedFileTransition {
    pub(crate) document: WasmDocumentHandle,
    pub(crate) changes: WasmHostEntityChanges,
    pub(crate) certified_batches: Vec<WasmCertifiedEntityBatch>,
    pub(crate) counters: WasmTransitionCounters,
}

/// Fully drained static conflict-resolution output. Results remain aligned to
/// the caller's conflict list; the merge planner owns the entity keys and
/// historical rows used by a `Take` result.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedConflictTransition {
    pub(crate) resolutions: Vec<WasmHostConflictResolution>,
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
pub(crate) struct ValidatedEntityTransition {
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

fn validate_certified_entity_batches(
    batches: &mut [WasmCertifiedEntityBatch],
    schemas: &SchemaAllowlist,
) -> Result<(), LixError> {
    for batch in batches {
        for schema_key in &batch.schema_keys {
            schemas.validate(schema_key)?;
        }
        match batch.format {
            2 => validate_certified_snapshot_packets(batch, schemas)?,
            format => {
                return Err(invalid_guest(format!(
                    "unknown certified entity batch format {format}"
                )));
            }
        }
    }
    Ok(())
}

const HOST_CERTIFIED_PACKET_TARGET_BYTES: usize = 256 * 1024;
const HOST_CERTIFIED_PACKET_MIN_ROWS: usize = 64;

/// Retains a complete, eagerly validated generic-text import in dense packet
/// pages. The ordinary component change list remains intact for changelog and
/// transaction materialization; the tracked-head writer may use this complete
/// batch as the authoritative current-state owner instead of persisting a
/// second expanded HOT row per line.
pub(crate) fn certify_dense_fresh_file(
    transition: &mut ValidatedFileTransition,
    creates: crate::plugin::runtime::WasmCreateContext,
    schemas: &SchemaAllowlist,
) -> Result<(), LixError> {
    if !transition.certified_batches.is_empty()
        || transition.changes.changes.len() < HOST_CERTIFIED_PACKET_MIN_ROWS
    {
        return Ok(());
    }
    let Some(schema_key) = transition.changes.changes.first().and_then(|change| {
        let WasmEntityChange::Create { schema_key, .. } = change else {
            return None;
        };
        Some(schema_key.as_str())
    }) else {
        return Ok(());
    };
    if !transition.changes.changes.iter().all(|change| {
        matches!(
            change,
            WasmEntityChange::Create {
                schema_key: candidate_schema_key,
                resolved_key: None,
                snapshot_content: WasmHostBytes::CanonicalJson(_),
                ..
            } if candidate_schema_key == schema_key
        )
    }) {
        return Ok(());
    }
    schemas.validate(schema_key)?;
    let plan = schemas.schema_plan(schema_key).ok_or_else(|| {
        invalid_guest(format!(
            "dense batch schema '{schema_key}' has no validation plan"
        ))
    })?;
    if plan
        .primary_key
        .as_ref()
        .is_none_or(|paths| paths.len() != 1)
    {
        return Ok(());
    }
    let primary_key_path = &plan.primary_key.as_ref().expect("checked above")[0];
    let snapshot_bytes = transition
        .changes
        .changes
        .iter()
        .filter_map(|change| match change {
            WasmEntityChange::Create {
                snapshot_content: WasmHostBytes::CanonicalJson(snapshot),
                ..
            } => Some(snapshot.normalized().len()),
            _ => None,
        })
        .sum::<usize>();
    let compressed_pages = snapshot_bytes >= 1024 * 1024;

    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(HOST_CERTIFIED_PACKET_TARGET_BYTES);
    let mut page_first_local_ref = None;
    let mut page_last_local_ref = None;
    for change in &transition.changes.changes {
        let WasmEntityChange::Create {
            schema_key,
            local_ref,
            snapshot_content: WasmHostBytes::CanonicalJson(snapshot),
            ..
        } = change
        else {
            unreachable!("dense text eligibility was checked above");
        };
        let schema_bytes = schema_key.as_bytes();
        let id = creates.component(*local_ref)?;
        let snapshot_bytes = crate::plugin::wire::insert_generated_id(
            snapshot.normalized().as_bytes(),
            primary_key_path,
            &id,
        )
        .map_err(|error| {
            invalid_guest(format!(
                "dense batch generated identity is invalid: {error}"
            ))
        })?;
        let record_len = 1_usize
            .checked_add(4)
            .and_then(|len| len.checked_add(schema_bytes.len()))
            .and_then(|len| len.checked_add(4 + 4 + id.len() + 1 + 4))
            .and_then(|len| len.checked_add(snapshot_bytes.len()))
            .ok_or_else(|| invalid_guest("host certified packet record size overflowed"))?;
        let framed_len = 4_usize
            .checked_add(record_len)
            .ok_or_else(|| invalid_guest("host certified packet frame size overflowed"))?;
        let page_local_ref = u32::try_from(*local_ref)
            .map_err(|_| invalid_guest("host certified packet local reference exceeds u32"))?;
        if !page.is_empty()
            && page.len().saturating_add(framed_len) > HOST_CERTIFIED_PACKET_TARGET_BYTES
        {
            pages.push(finish_host_certified_packet_page(
                std::mem::take(&mut page),
                page_first_local_ref
                    .take()
                    .expect("non-empty packet page has a first local ref"),
                page_last_local_ref
                    .take()
                    .expect("non-empty packet page has a last local ref"),
                compressed_pages,
            )?);
            page = Vec::with_capacity(HOST_CERTIFIED_PACKET_TARGET_BYTES.max(framed_len));
        }
        page_first_local_ref.get_or_insert(page_local_ref);
        page_last_local_ref = Some(page_local_ref);
        page.extend_from_slice(
            &u32::try_from(record_len)
                .map_err(|_| invalid_guest("host certified packet record exceeds u32"))?
                .to_le_bytes(),
        );
        page.push(3);
        page.extend_from_slice(
            &u32::try_from(schema_bytes.len())
                .map_err(|_| invalid_guest("host certified packet schema exceeds u32"))?
                .to_le_bytes(),
        );
        page.extend_from_slice(schema_bytes);
        page.extend_from_slice(&1_u32.to_le_bytes());
        page.extend_from_slice(
            &u32::try_from(id.len())
                .map_err(|_| invalid_guest("generated identity exceeds u32"))?
                .to_le_bytes(),
        );
        page.extend_from_slice(id.as_bytes());
        page.push(0);
        page.extend_from_slice(
            &u32::try_from(snapshot_bytes.len())
                .map_err(|_| invalid_guest("host certified packet snapshot exceeds u32"))?
                .to_le_bytes(),
        );
        page.extend_from_slice(&snapshot_bytes);
    }
    if !page.is_empty() {
        pages.push(finish_host_certified_packet_page(
            page,
            page_first_local_ref.expect("non-empty packet page has a first local ref"),
            page_last_local_ref.expect("non-empty packet page has a last local ref"),
            compressed_pages,
        )?);
    }
    let batch = WasmCertifiedEntityBatch {
        // Formats 3 and 4 are host-only equivalents of the format-2 packet
        // codec. Guest batches are validated before this synthesis point and
        // the guest-facing validator intentionally rejects both formats.
        format: if compressed_pages {
            crate::plugin::runtime::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
        } else {
            crate::plugin::runtime::HOST_CERTIFIED_PACKET_FORMAT
        },
        schema_keys: vec![schema_key.to_owned()],
        row_count: transition.changes.changes.len() as u64,
        creates,
        create_ranges: Vec::new(),
        complete_file_state: true,
        pages,
    };
    transition.certified_batches.push(batch);
    Ok(())
}

fn finish_host_certified_packet_page(
    page: Vec<u8>,
    first_local_ref: u32,
    last_local_ref: u32,
    compressed: bool,
) -> Result<Bytes, LixError> {
    if !compressed {
        return Ok(Bytes::from(page));
    }
    let compressed = crate::compression::compress_zstd_level_1(&page).map_err(|error| {
        LixError::new(
            LixError::CODE_UNKNOWN,
            format!("host-certified packet compression failed: {error}"),
        )
    })?;
    let mut encoded = Vec::with_capacity(12 + compressed.len());
    encoded.extend_from_slice(&first_local_ref.to_le_bytes());
    encoded.extend_from_slice(&last_local_ref.to_le_bytes());
    encoded.extend_from_slice(
        &u32::try_from(page.len())
            .map_err(|_| invalid_guest("host-certified packet page exceeds u32"))?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&compressed);
    Ok(Bytes::from(encoded))
}

fn validate_certified_snapshot_packets(
    batch: &mut WasmCertifiedEntityBatch,
    schemas: &SchemaAllowlist,
) -> Result<(), LixError> {
    let mut rows = 0_u64;
    let mut encountered = BTreeSet::new();
    let validate_relationships =
        batch
            .schema_keys
            .iter()
            .try_fold(false, |found, schema_key| {
                let plan = schemas.schema_plan(schema_key).ok_or_else(|| {
                    invalid_guest(format!(
                        "certified batch schema '{schema_key}' has no validation plan"
                    ))
                })?;
                Ok::<_, LixError>(found || !plan.foreign_keys.is_empty())
            })?;
    let mut entity_keys = BTreeSet::new();
    let mut foreign_keys = Vec::new();
    for page in &batch.pages {
        let mut page = CertifiedPacketReader::new(page);
        while !page.finished() {
            let record_len = page.u32()? as usize;
            let record_bytes = page.bytes(record_len)?;
            let mut record = CertifiedPacketReader::new(record_bytes);
            let tag = record.u8()?;
            let schema_key = record.text()?;
            schemas.validate(schema_key)?;
            encountered.insert(schema_key);
            let normalized = match tag {
                0 => {
                    let component_count = record.u32()? as usize;
                    if component_count == 0 {
                        return Err(invalid_guest(
                            "certified packet upsert key has no components",
                        ));
                    }
                    let mut components = smallvec::SmallVec::<[&str; 2]>::new();
                    for _ in 0..component_count {
                        components.push(record.text()?);
                    }
                    if record.u8()? > 1 {
                        return Err(invalid_guest("certified packet upsert has invalid effect"));
                    }
                    let snapshot = record.inline_blob()?;
                    let plan = schemas.schema_plan(schema_key).ok_or_else(|| {
                        invalid_guest(format!(
                            "certified batch schema '{schema_key}' has no validation plan"
                        ))
                    })?;
                    validate_certified_record(
                        plan,
                        schema_key,
                        &components,
                        snapshot,
                        batch.complete_file_state,
                        validate_relationships,
                        &mut entity_keys,
                        &mut foreign_keys,
                    )?
                }
                2 => {
                    let local_ref = record.u64()?;
                    let id = batch.creates.component(local_ref)?;
                    let snapshot = record.inline_blob()?;
                    let plan = schemas.schema_plan(schema_key).ok_or_else(|| {
                        invalid_guest(format!(
                            "certified batch schema '{schema_key}' has no validation plan"
                        ))
                    })?;
                    let [_primary_key_path] = plan.primary_key.as_deref().unwrap_or_default()
                    else {
                        return Err(invalid_guest(
                            "certified creates require exactly one generated primary-key field",
                        ));
                    };
                    let normalized = validate_certified_record(
                        plan,
                        schema_key,
                        &[id.as_str()],
                        snapshot,
                        batch.complete_file_state,
                        validate_relationships,
                        &mut entity_keys,
                        &mut foreign_keys,
                    )?;
                    normalized
                }
                _ => {
                    return Err(invalid_guest(
                        "certified snapshot packet contains a non-snapshot change",
                    ));
                }
            };
            record.finish()?;
            if normalized.is_some() {
                return Err(invalid_guest(
                    "certified batch snapshot is not in canonical storage form",
                ));
            }
            rows = rows
                .checked_add(1)
                .ok_or_else(|| invalid_guest("certified batch row count overflowed"))?;
        }
    }
    if rows != batch.row_count {
        return Err(invalid_guest(format!(
            "certified batch declared {} rows but validated {rows}",
            batch.row_count
        )));
    }
    if encountered.len() != batch.schema_keys.len()
        || !batch
            .schema_keys
            .iter()
            .all(|schema_key| encountered.contains(schema_key.as_str()))
    {
        return Err(invalid_guest(
            "certified batch schema header does not match its records",
        ));
    }
    if let Some((schema_key, components)) =
        foreign_keys.iter().find(|key| !entity_keys.contains(*key))
    {
        return Err(invalid_guest(format!(
            "certified foreign key '{}:{components:?}' is absent from the complete batch",
            schema_key
        )));
    }
    Ok(())
}

type CertifiedEntityKey = (String, Vec<String>);

fn validate_certified_record(
    plan: &SchemaPlan,
    schema_key: &str,
    components: &[&str],
    snapshot: &[u8],
    complete_file_state: bool,
    validate_relationships: bool,
    entity_keys: &mut BTreeSet<CertifiedEntityKey>,
    foreign_keys: &mut Vec<CertifiedEntityKey>,
) -> Result<Option<Vec<u8>>, LixError> {
    if let Some(normalized) =
        plan.certify_or_normalize_plugin_row_parts(snapshot, schema_key, components)?
    {
        if validate_relationships {
            entity_keys.insert((
                schema_key.to_owned(),
                components.iter().map(|value| (*value).to_owned()).collect(),
            ));
        }
        return Ok(normalized);
    }
    if !complete_file_state && !plan.foreign_keys.is_empty() {
        return Err(invalid_guest(
            "a certified batch with foreign keys must contain complete file state",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(snapshot)
        .map_err(|error| invalid_guest(format!("certified snapshot is invalid JSON: {error}")))?;
    if let Err(errors) = plan.compiled_schema.validate(&value) {
        let details = errors
            .take(3)
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        return Err(invalid_guest(format!(
            "certified snapshot failed schema validation: {details}"
        )));
    }
    let primary_key = plan
        .primary_key
        .as_deref()
        .ok_or_else(|| invalid_guest("certified snapshot schema has no primary-key definition"))?;
    let emitted = json_pointer_components(&value, primary_key)?;
    if emitted
        .iter()
        .map(String::as_str)
        .ne(components.iter().copied())
    {
        return Err(invalid_guest(
            "certified snapshot primary key does not match its entity key",
        ));
    }
    if validate_relationships {
        entity_keys.insert((schema_key.to_owned(), emitted));
        for foreign_key in &plan.foreign_keys {
            let values = json_pointer_components_optional(&value, &foreign_key.local_properties)?;
            if let Some(values) = values {
                foreign_keys.push((foreign_key.referenced_schema.schema_key.clone(), values));
            }
        }
    }
    let canonical = canonicalize_snapshot(snapshot)?;
    Ok((canonical.as_slice() != snapshot).then_some(canonical))
}

fn json_pointer_components(
    value: &serde_json::Value,
    paths: &[Vec<String>],
) -> Result<Vec<String>, LixError> {
    json_pointer_components_optional(value, paths)?
        .ok_or_else(|| invalid_guest("certified primary-key component cannot be null"))
}

fn json_pointer_components_optional(
    value: &serde_json::Value,
    paths: &[Vec<String>],
) -> Result<Option<Vec<String>>, LixError> {
    let mut components = Vec::with_capacity(paths.len());
    for path in paths {
        let mut current = value;
        for segment in path {
            current = current.get(segment).ok_or_else(|| {
                invalid_guest(format!(
                    "certified snapshot is missing pointer '/{}'",
                    path.join("/")
                ))
            })?;
        }
        match current {
            serde_json::Value::Null => return Ok(None),
            serde_json::Value::String(value) => components.push(value.clone()),
            _ => {
                return Err(invalid_guest(
                    "certified key components must be strings or null",
                ));
            }
        }
    }
    Ok(Some(components))
}

struct CertifiedPacketReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CertifiedPacketReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid_guest("certified packet ended early"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, LixError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed packet u32"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed packet u64"),
        ))
    }

    fn text(&mut self) -> Result<&'a str, LixError> {
        let length = self.u32()? as usize;
        std::str::from_utf8(self.bytes(length)?)
            .map_err(|error| invalid_guest(format!("certified packet text is invalid: {error}")))
    }

    fn inline_blob(&mut self) -> Result<&'a [u8], LixError> {
        if self.u8()? != 0 {
            return Err(invalid_guest("certified packet snapshot is not inline"));
        }
        let length = self.u32()? as usize;
        self.bytes(length)
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn finish(&self) -> Result<(), LixError> {
        if self.finished() {
            Ok(())
        } else {
            Err(invalid_guest("certified packet record has trailing bytes"))
        }
    }
}

/// Drains and validates every change before returning any proposed semantic
/// state to transaction code. Validation of a page's key shape, attachment
/// count, and aggregate budget happens before the first attachment method is
/// invoked. Cursor-wide key uniqueness is checked once over borrowed references
/// after the final stable change vector has been assembled.
pub(crate) async fn drain_file_transition_changes(
    actor: &mut dyn WasmComponentActor,
    transition: WasmFileTransition,
    creates: crate::plugin::runtime::WasmCreateContext,
    schemas: &SchemaAllowlist,
    limits: WasmTransitionLimits,
) -> Result<ValidatedFileTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_file_transition_changes_inner(actor, transition, creates, schemas, limits).await {
        Ok(validated) => Ok(validated),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

async fn drain_file_transition_changes_inner(
    actor: &mut dyn WasmComponentActor,
    transition: WasmFileTransition,
    creates: crate::plugin::runtime::WasmCreateContext,
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
            prevalidate_change_page(&page, schemas, &mut budget)
        })?;
        local_counters.packet_pages = local_counters.packet_pages.saturating_add(1);
        local_counters.packet_records = local_counters
            .packet_records
            .saturating_add(page.changes.changes.len() as u64);

        let page_row_count = page.changes.changes.len();
        let page_snapshot_count = page
            .changes
            .changes
            .iter()
            .filter(|change| !matches!(change, WasmEntityChange::Delete(_)))
            .count();
        let page_start = changes.len();
        changes.reserve(page_row_count);
        let mut snapshots = CanonicalJsonBatchBuilder::with_row_capacity(page_snapshot_count);
        let outputs = page.outputs;
        async {
            let mut page_snapshot_ordinal = 0usize;
            for change in page.changes.changes {
                let resolved = match change {
                    WasmEntityChange::Create {
                        schema_key,
                        local_ref,
                        mut resolved_key,
                        snapshot_content,
                    } => {
                        let snapshot = resolve_guest_bytes(
                            actor,
                            transition.transition,
                            outputs,
                            snapshot_content,
                            &mut budget,
                            &mut local_counters,
                        )
                        .await?;
                        let snapshot_row = match &resolved_key {
                            Some(key) => snapshots.push_plugin(snapshot, key, schemas)?,
                            None => {
                                let plan = schemas.schema_plan(&schema_key).ok_or_else(|| {
                                    invalid_guest(format!(
                                        "created schema '{schema_key}' has no validation plan"
                                    ))
                                })?;
                                let [primary_key_path] =
                                    plan.primary_key.as_deref().unwrap_or_default()
                                else {
                                    return Err(invalid_guest(
                                        "created entities require exactly one generated primary-key field",
                                    ));
                                };
                                let id = creates.component(local_ref)?;
                                crate::plugin::wire::validate_generated_id(
                                    &snapshot,
                                    primary_key_path,
                                    &id,
                                )
                                .map_err(|error| {
                                    invalid_guest(format!(
                                        "created entity identity is invalid: {error}"
                                    ))
                                })?;
                                let key = WasmEntityKey::from_owned_parts(
                                    schema_key.clone(),
                                    vec![id],
                                );
                                let row = snapshots.push_plugin(snapshot, &key, schemas)?;
                                resolved_key = Some(key);
                                row
                            }
                        };
                        debug_assert_eq!(snapshot_row, page_snapshot_ordinal);
                        page_snapshot_ordinal += 1;
                        WasmEntityChange::Create {
                            schema_key,
                            local_ref,
                            resolved_key,
                            // Patched with the page-owned canonical row after
                            // every snapshot validates.
                            snapshot_content: WasmHostBytes::Inline(Bytes::new()),
                        }
                    }
                    WasmEntityChange::Delete(key) => WasmEntityChange::Delete(key),
                    WasmEntityChange::Upsert { entity, effect } => {
                        let snapshot = resolve_guest_bytes(
                            actor,
                            transition.transition,
                            outputs,
                            entity.snapshot_content,
                            &mut budget,
                            &mut local_counters,
                        )
                        .await?;
                        let snapshot_row = snapshots.push_plugin(snapshot, &entity.key, schemas)?;
                        debug_assert_eq!(snapshot_row, page_snapshot_ordinal);
                        page_snapshot_ordinal += 1;
                        WasmEntityChange::Upsert {
                            entity: WasmEntity {
                                key: entity.key,
                                // The canonical page does not exist until every
                                // snapshot has validated. Patch this sentinel in
                                // place after `finish`, retaining source order
                                // without a second page-sized row vector.
                                snapshot_content: WasmHostBytes::Inline(Bytes::new()),
                            },
                            effect,
                        }
                    }
                };
                changes.push(resolved);
            }
            debug_assert_eq!(page_snapshot_ordinal, page_snapshot_count);
            Ok::<(), LixError>(())
        }
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.plugin_drain_resolve_page"
        ))
        .await?;

        let mut canonical = snapshots.finish()?.into_iter();
        debug_assert_eq!(changes.len() - page_start, page_row_count);
        for change in &mut changes[page_start..] {
            let snapshot_content = match change {
                WasmEntityChange::Create {
                    snapshot_content, ..
                } => Some(snapshot_content),
                WasmEntityChange::Upsert { entity, .. } => Some(&mut entity.snapshot_content),
                WasmEntityChange::Delete(_) => None,
            };
            if let Some(snapshot_content) = snapshot_content {
                debug_assert!(matches!(
                    snapshot_content,
                    WasmHostBytes::Inline(bytes) if bytes.is_empty()
                ));
                let snapshot = canonical
                    .next()
                    .expect("one canonical snapshot exists for every appended write");
                *snapshot_content = WasmHostBytes::CanonicalJson(snapshot);
            }
        }
        #[cfg(debug_assertions)]
        assert!(canonical.next().is_none());
    }

    validate_change_cursor_key_uniqueness(&changes).map_err(|error| {
        invalid_guest(format!(
            "invalid component change cursor page: {}",
            error.message
        ))
    })?;
    let mut certified_batches = actor.take_certified_entity_batches(transition.transition);
    validate_certified_entity_batches(&mut certified_batches, schemas)?;
    let runtime_counters = actor
        .finish_transition(transition.transition)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.plugin_drain_finish"
        ))
        .await?;
    Ok(ValidatedFileTransition {
        document: transition.document,
        changes: WasmEntityChanges { changes },
        certified_batches,
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

/// Drains one static resolver cursor. The resolver cannot invent keys: output
/// cardinality is exactly the input conflict count and every result remains in
/// the source's canonical key order. This lets `Take(B)` reuse a durable
/// historical row without moving its snapshot through guest memory.
pub(crate) async fn drain_conflict_transition_resolutions(
    actor: &mut dyn WasmComponentActor,
    transition: WasmConflictTransition,
    expected_count: usize,
    limits: WasmTransitionLimits,
) -> Result<ValidatedConflictTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_conflict_transition_resolutions_inner(actor, transition, expected_count, limits)
        .await
    {
        Ok(validated) => Ok(validated),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

async fn drain_conflict_transition_resolutions_inner(
    actor: &mut dyn WasmComponentActor,
    transition: WasmConflictTransition,
    expected_count: usize,
    limits: WasmTransitionLimits,
) -> Result<ValidatedConflictTransition, LixError> {
    let mut validator = WasmConflictResolutionDrainValidator::new(limits)?;
    let mut budget = OutputDrainBudget::new(limits)?;
    let mut local_counters = WasmTransitionCounters {
        conflict_resolution_calls: 1,
        ..WasmTransitionCounters::default()
    };
    let mut resolutions = Vec::with_capacity(expected_count);

    loop {
        let Some(page) = actor
            .next_resolution_page(
                transition.transition,
                transition.resolutions,
                limits.max_page_bytes,
            )
            .await?
        else {
            validator.accept_eof();
            break;
        };
        validator.accept_page(&page).map_err(|error| {
            invalid_guest(format!(
                "invalid component resolution cursor page: {}",
                error.message
            ))
        })?;
        // Check every replacement's complete byte range before asking the
        // guest output table for its length or allocating its output buffer.
        // `Take` and `Delete` are snapshot-free, but an untrusted `Replace`
        // may otherwise name an arbitrarily large output range.
        prevalidate_conflict_resolution_page(&page, &mut budget)?;
        local_counters.packet_pages = local_counters.packet_pages.saturating_add(1);
        local_counters.packet_records = local_counters
            .packet_records
            .saturating_add(page.resolutions.len() as u64);

        let page_row_count = page.resolutions.len();
        let page_snapshot_count = page
            .resolutions
            .iter()
            .filter(|resolution| matches!(resolution, WasmConflictResolution::Replace { .. }))
            .count();
        let page_start = resolutions.len();
        let mut snapshots = CanonicalJsonBatchBuilder::with_row_capacity(page_snapshot_count);
        let outputs = page.outputs;
        let mut page_snapshot_ordinal = 0usize;
        for (ordinal, resolution) in page.ordinals.into_iter().zip(page.resolutions) {
            let expected_ordinal = u32::try_from(resolutions.len()).map_err(|_| {
                invalid_guest("component conflict resolver has more than u32::MAX results")
            })?;
            if ordinal != expected_ordinal {
                return Err(invalid_guest(format!(
                    "component conflict resolver returned ordinal {ordinal}, expected {expected_ordinal}",
                )));
            }
            let resolved = match resolution {
                WasmConflictResolution::Take(side) => {
                    local_counters.conflict_resolution_takes =
                        local_counters.conflict_resolution_takes.saturating_add(1);
                    WasmConflictResolution::Take(side)
                }
                WasmConflictResolution::Delete => WasmConflictResolution::Delete,
                WasmConflictResolution::Replace {
                    snapshot_content,
                    effect,
                } => {
                    let snapshot = resolve_guest_bytes(
                        actor,
                        transition.transition,
                        outputs,
                        snapshot_content,
                        &mut budget,
                        &mut local_counters,
                    )
                    .await?;
                    let snapshot_row = snapshots.push(&snapshot)?;
                    debug_assert_eq!(snapshot_row, page_snapshot_ordinal);
                    page_snapshot_ordinal += 1;
                    WasmConflictResolution::Replace {
                        // See the file-transition drain above: finish the
                        // canonical page once, then replace this sentinel
                        // inside the stable result vector.
                        snapshot_content: WasmHostBytes::Inline(Bytes::new()),
                        effect,
                    }
                }
            };
            resolutions.push(resolved);
            if resolutions.len() > expected_count {
                return Err(invalid_guest(
                    "component conflict resolver returned more results than input conflicts",
                ));
            }
        }
        debug_assert_eq!(page_snapshot_ordinal, page_snapshot_count);
        let mut canonical = snapshots.finish()?.into_iter();
        debug_assert_eq!(resolutions.len() - page_start, page_row_count);
        for resolution in &mut resolutions[page_start..] {
            if let WasmConflictResolution::Replace {
                snapshot_content, ..
            } = resolution
            {
                debug_assert!(matches!(
                    snapshot_content,
                    WasmHostBytes::Inline(bytes) if bytes.is_empty()
                ));
                let snapshot = canonical
                    .next()
                    .expect("one canonical snapshot exists for every appended replacement");
                *snapshot_content = WasmHostBytes::CanonicalJson(snapshot);
            }
        }
        #[cfg(debug_assertions)]
        assert!(canonical.next().is_none());
    }
    if resolutions.len() != expected_count {
        return Err(invalid_guest(format!(
            "component conflict resolver returned {} results for {expected_count} input conflicts",
            resolutions.len()
        )));
    }
    local_counters.conflict_resolution_records =
        u64::try_from(resolutions.len()).unwrap_or(u64::MAX);
    let runtime_counters = actor.finish_transition(transition.transition).await?;
    Ok(ValidatedConflictTransition {
        resolutions,
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

/// Drains renderer edit pages, resolves lazy output ranges, applies the edits
/// against one immutable base, and optionally proves byte equality with an
/// independently reconstructed expected result.
pub(crate) async fn drain_entity_transition_edits(
    actor: &mut dyn WasmComponentActor,
    transition: WasmEntityTransition,
    base: &[u8],
    expected: Option<Blob>,
    expected_delta: Option<&[WasmInputSplice]>,
    limits: WasmTransitionLimits,
) -> Result<ValidatedEntityTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_entity_transition_edits_inner(
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

async fn drain_entity_transition_edits_inner(
    actor: &mut dyn WasmComponentActor,
    transition: WasmEntityTransition,
    base: &[u8],
    expected: Option<Blob>,
    expected_delta: Option<&[WasmInputSplice]>,
    limits: WasmTransitionLimits,
) -> Result<ValidatedEntityTransition, LixError> {
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
    Ok(ValidatedEntityTransition {
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
    budget: &mut OutputDrainBudget,
) -> Result<(), LixError> {
    if page.format_version != PACKET_FORMAT_V1 {
        return Err(invalid_guest("unsupported component change packet format"));
    }
    let mut inline_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut minimum_attachment_reads = 0u64;
    let mut references = 0u32;
    for change in &page.changes.changes {
        schemas.validate(change.schema_key())?;
        if let Some(key) = change.entity_key()
            && key.entity_pk.is_empty()
        {
            return Err(invalid_guest(
                "component entity primary keys must not be empty",
            ));
        }
        let snapshot = match change {
            WasmEntityChange::Create {
                snapshot_content, ..
            } => Some(snapshot_content),
            WasmEntityChange::Upsert { entity, .. } => Some(&entity.snapshot_content),
            WasmEntityChange::Delete(_) => None,
        };
        if let Some(snapshot) = snapshot {
            match snapshot {
                WasmGuestBytes::Inline(bytes) => {
                    inline_bytes =
                        inline_bytes
                            .checked_add(bytes.len() as u64)
                            .ok_or_else(|| {
                                invalid_guest("component inline snapshot bytes overflowed")
                            })?;
                }
                WasmGuestBytes::Output(range) => {
                    output_bytes = output_bytes.checked_add(range.length).ok_or_else(|| {
                        invalid_guest("component output snapshot bytes overflowed")
                    })?;
                    minimum_attachment_reads = minimum_attachment_reads
                        .checked_add(budget.minimum_attachment_reads(range.length))
                        .ok_or_else(|| {
                            invalid_guest("component attachment page count overflowed")
                        })?;
                    references = references
                        .checked_add(1)
                        .ok_or_else(|| invalid_guest("component output references overflowed"))?;
                }
            }
        }
    }
    budget.preflight_cursor_page(
        inline_bytes,
        output_bytes,
        0,
        references,
        minimum_attachment_reads,
    )
}

/// Preflights every complete snapshot returned by a static conflict resolver.
/// This must run before `resolve_guest_bytes`: an output-backed replacement's
/// declared length is guest-controlled, and `read_output_range` intentionally
/// reserves its final buffer capacity before draining bounded chunks.
fn prevalidate_conflict_resolution_page(
    page: &WasmConflictResolutionPage,
    budget: &mut OutputDrainBudget,
) -> Result<(), LixError> {
    let mut inline_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut minimum_attachment_reads = 0u64;
    let mut references = 0u32;
    for resolution in &page.resolutions {
        let WasmConflictResolution::Replace {
            snapshot_content, ..
        } = resolution
        else {
            continue;
        };
        match snapshot_content {
            WasmGuestBytes::Inline(bytes) => {
                inline_bytes = inline_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| {
                        invalid_guest("component conflict replacement inline bytes overflowed")
                    })?;
            }
            WasmGuestBytes::Output(range) => {
                output_bytes = output_bytes.checked_add(range.length).ok_or_else(|| {
                    invalid_guest("component conflict replacement output bytes overflowed")
                })?;
                minimum_attachment_reads = minimum_attachment_reads
                    .checked_add(budget.minimum_attachment_reads(range.length))
                    .ok_or_else(|| invalid_guest("component attachment page count overflowed"))?;
                references = references
                    .checked_add(1)
                    .ok_or_else(|| invalid_guest("component output references overflowed"))?;
            }
        }
    }
    // Resolution pages are packet-v1 frames, so the runtime has already
    // charged their wire metadata. Only the replacement values are charged
    // here, as for a change page.
    budget.preflight_cursor_page(
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
        entity_input_pages: local.entity_input_pages.max(runtime.entity_input_pages),
        entity_input_records: local.entity_input_records.max(runtime.entity_input_records),
        entity_input_wire_bytes: local
            .entity_input_wire_bytes
            .max(runtime.entity_input_wire_bytes),
        entity_output_pages: local.entity_output_pages.max(runtime.entity_output_pages),
        entity_output_records: local
            .entity_output_records
            .max(runtime.entity_output_records),
        entity_output_wire_bytes: local
            .entity_output_wire_bytes
            .max(runtime.entity_output_wire_bytes),
        attachment_reads: local.attachment_reads.max(runtime.attachment_reads),
        attachment_bytes_read: local
            .attachment_bytes_read
            .max(runtime.attachment_bytes_read),
        entity_input_attachment_reads: local
            .entity_input_attachment_reads
            .max(runtime.entity_input_attachment_reads),
        entity_input_attachment_bytes: local
            .entity_input_attachment_bytes
            .max(runtime.entity_input_attachment_bytes),
        entity_output_attachment_writes: local
            .entity_output_attachment_writes
            .max(runtime.entity_output_attachment_writes),
        entity_output_attachment_bytes: local
            .entity_output_attachment_bytes
            .max(runtime.entity_output_attachment_bytes),
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

    use super::*;
    use crate::plugin::runtime::{
        WasmChangeCursorHandle, WasmChangeEffect, WasmCreateContext, WasmEditCursorHandle,
        WasmOpenEntitiesInput, WasmOpenFileInput, WasmOutputSplice,
    };

    const UUID_A: &str = "019a0000-0000-7000-8000-000000000001";
    const UUID_B: &str = "019a0000-0000-7000-8000-000000000002";
    const UUID_C: &str = "019a0000-0000-7000-8000-000000000003";

    fn test_creates() -> WasmCreateContext {
        WasmCreateContext {
            high: 0x019a_0000_0000_7000,
            low: 0x8000_0000,
        }
    }

    fn key(id: &str) -> WasmEntityKey {
        WasmEntityKey::from_owned_parts("csv_row", vec![id.to_owned()])
    }

    fn host_entity(id: &str) -> WasmHostEntity {
        WasmEntity {
            key: key(id),
            snapshot_content: WasmHostBytes::Inline(
                format!(r#"{{"cells":[],"id":"{id}","order_key":"a"}}"#)
                    .into_bytes()
                    .into(),
            ),
        }
    }

    #[test]
    fn compressed_host_certified_packet_page_roundtrips() {
        let packet = vec![b'x'; 32 * 1024];
        let encoded = finish_host_certified_packet_page(packet.clone(), 7, 11, true).unwrap();
        let first = u32::from_le_bytes(encoded[..4].try_into().unwrap());
        let last = u32::from_le_bytes(encoded[4..8].try_into().unwrap());
        let uncompressed_len = u32::from_le_bytes(encoded[8..12].try_into().unwrap()) as usize;
        let compressed = &encoded[12..];
        assert_eq!((first, last), (7, 11));
        assert_eq!(uncompressed_len, packet.len());
        assert!(compressed.len() < packet.len());
        assert_eq!(
            crate::compression::decompress_zstd(compressed, uncompressed_len).unwrap(),
            packet
        );
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
    fn canonical_csv_json_rejects_numbers_and_decoded_duplicate_keys() {
        let canonical =
            canonicalize_snapshot(r#"{"z":"\n","a":[true,null,"é"],"slash":"/"}"#.as_bytes())
                .unwrap();
        assert_eq!(
            canonical,
            r#"{"a":[true,null,"é"],"slash":"/","z":"\n"}"#.as_bytes()
        );
        let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(1);
        batch
            .push(r#"{"z":"\n","a":[true,null,"é"],"slash":"/"}"#.as_bytes())
            .unwrap();
        let json = batch.finish().unwrap().pop().unwrap();
        assert_eq!(
            json.normalized(),
            r#"{"a":[true,null,"é"],"slash":"/","z":"\n"}"#
        );
        assert_eq!(json.value()["z"], "\n");
        assert_eq!(json.value()["a"][0], true);

        assert!(canonicalize_snapshot(br#"{"nested":{"n":1}}"#).is_err());
        let duplicate = canonicalize_snapshot(br#"{"a":"x","\u0061":"y"}"#)
            .expect_err("escaped and literal decoded keys are duplicates");
        assert!(duplicate.message.contains("duplicate"), "{duplicate:?}");
        assert!(canonicalize_snapshot(br#"["not","an","object"]"#).is_err());
    }

    #[test]
    fn canonical_json_uses_exact_serde_control_escape_spelling() {
        let canonical = canonicalize_snapshot(
            br#"{"controls":"\b\t\n\f\r\u0001\u001f","quote":"\"","slash":"\\","solidus":"/"}"#,
        )
        .unwrap();
        assert_eq!(
            canonical,
            br#"{"controls":"\b\t\n\f\r\u0001\u001f","quote":"\"","slash":"\\","solidus":"/"}"#
        );
    }

    #[test]
    fn canonical_json_rows_share_one_arena_and_validate_once() {
        let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(2);
        batch.push(br#"{"id":"a","value":"first"}"#).unwrap();
        batch.push(br#"{"id":"b","value":"second"}"#).unwrap();
        let rows = batch.finish().unwrap();

        assert_eq!(rows.len(), 2);
        assert!(rows[0].shares_batch_with(&rows[1]));
        assert_eq!(rows[0].row_index(), 0);
        assert_eq!(rows[1].row_index(), 1);
        assert_eq!(rows[0].batch_row_count(), 2);
        assert_eq!(
            rows[0].batch_arena_len(),
            rows[0].normalized().len() + rows[1].normalized().len()
        );
        assert_eq!(rows[0].validation_counts(), (2, 2));
        assert_eq!(rows[0].batch_arena_allocation_count(), 1);
        assert_eq!(rows[0].value()["id"], "a");
        assert_eq!(rows[1].value()["id"], "b");
    }

    #[test]
    fn certified_builder_uses_sub_64k_parallel_columns_for_a_cursor_page() {
        const PAGE_ROWS: usize = 1_024;
        const LARGE_ALLOCATION_BYTES: usize = 64 * 1_024;

        let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(PAGE_ROWS);
        batch.reserve_decoded_column();
        batch.reserve_certified_columns();

        assert_eq!(size_of::<CanonicalJsonBatchRowKind>(), 1);
        assert!(
            batch.row_kinds.capacity() * size_of::<CanonicalJsonBatchRowKind>()
                < LARGE_ALLOCATION_BYTES
        );
        assert!(
            batch.decoded_values.capacity() * size_of::<serde_json::Value>()
                < LARGE_ALLOCATION_BYTES
        );
        assert!(
            batch.certified_normalized.capacity() * size_of::<SharedStr>() < LARGE_ALLOCATION_BYTES
        );
        assert!(
            batch.certified_entity_pks.capacity() * size_of::<EntityPk>() < LARGE_ALLOCATION_BYTES
        );
        assert!(
            batch.schema_fingerprint_indices.capacity() * size_of::<u32>() < LARGE_ALLOCATION_BYTES
        );
        assert!(batch.normalized_ends.capacity() * size_of::<u32>() < LARGE_ALLOCATION_BYTES);
    }

    #[test]
    fn certified_plugin_rows_retain_source_buffers_without_an_arena() {
        let schema = serde_json::from_str(include_str!(
            "../../../../../plugins/csv/schema/csv_row.json"
        ))
        .expect("CSV row schema");
        let catalog =
            CatalogSnapshot::from_schema_facts(&[crate::catalog::SchemaCatalogFact::new(
                crate::domain::Domain::schema_catalog("main", false),
                crate::schema::SchemaKey::new("csv_row"),
                schema,
            )])
            .expect("CSV row catalog");
        let schemas = SchemaAllowlist::from_catalog(&["csv_row".to_owned()], Arc::new(catalog))
            .expect("CSV allowlist");

        let first = Bytes::from(
            br#"{"cells":["a"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#
                .as_slice()
                .to_vec(),
        );
        let second = Bytes::from(
            br#"{"cells":["b"],"id":"019a0000-0000-7000-8000-000000000002","order_key":"03"}"#
                .as_slice()
                .to_vec(),
        );
        let first_ptr = first.as_ptr();
        let second_ptr = second.as_ptr();
        let normalized_len = first.len() + second.len();
        let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(2);
        batch
            .push_plugin(
                first,
                &WasmEntityKey::from_owned_parts("csv_row", vec![UUID_A.to_owned()]),
                &schemas,
            )
            .expect("first canonical row");
        batch
            .push_plugin(
                second,
                &WasmEntityKey::from_owned_parts("csv_row", vec![UUID_B.to_owned()]),
                &schemas,
            )
            .expect("second canonical row");
        let rows = batch.finish().expect("certified canonical batch");

        assert_eq!(rows.len(), 2);
        assert!(rows[0].shares_batch_with(&rows[1]));
        assert_eq!(rows[0].row_index(), 0);
        assert_eq!(rows[1].row_index(), 1);
        assert_eq!(rows[0].batch_row_count(), 2);
        assert_eq!(rows[0].batch_arena_len(), normalized_len);
        assert_eq!(rows[0].batch_arena_allocation_count(), 0);
        assert_eq!(rows[0].validation_counts(), (2, 0));
        assert_eq!(rows[0].batch_decoded_value_count(), 0);
        assert_eq!(rows[0].batch_certified_schema_count(), 1);
        assert!(rows.iter().all(|row| row.certificate().is_some()));
        assert_eq!(
            rows[0]
                .certificate()
                .expect("first certificate")
                .entity_pk(),
            &EntityPk::uuid_from_canonical(UUID_A).expect("canonical UUID")
        );
        assert_eq!(
            rows[1]
                .certificate()
                .expect("second certificate")
                .entity_pk(),
            &EntityPk::uuid_from_canonical(UUID_B).expect("canonical UUID")
        );
        assert_eq!(
            rows[0].normalized(),
            r#"{"cells":["a"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#
        );
        assert_eq!(
            rows[1].normalized(),
            r#"{"cells":["b"],"id":"019a0000-0000-7000-8000-000000000002","order_key":"03"}"#
        );
        assert_eq!(rows[0].normalized_shared().as_bytes().as_ptr(), first_ptr);
        assert_eq!(rows[1].normalized_shared().as_bytes().as_ptr(), second_ptr);
    }

    #[test]
    fn canonical_plugin_rows_skip_dom_and_share_the_normalized_arena() {
        let schema = serde_json::from_str(include_str!(
            "../../../../../plugins/csv/schema/csv_row.json"
        ))
        .expect("CSV row schema");
        let catalog =
            CatalogSnapshot::from_schema_facts(&[crate::catalog::SchemaCatalogFact::new(
                crate::domain::Domain::schema_catalog("main", false),
                crate::schema::SchemaKey::new("csv_row"),
                schema,
            )])
            .expect("CSV row catalog");
        let schemas = SchemaAllowlist::from_catalog(&["csv_row".to_owned()], Arc::new(catalog))
            .expect("CSV allowlist");
        let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(2);
        batch
            .push_plugin(
                Bytes::from_static(
                    br#"{"cells":["a"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#,
                ),
                &WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_A.to_owned()],
                ),
                &schemas,
            )
            .expect("canonical row");
        batch
            .push_plugin(
                Bytes::from_static(
                    br#"{"id":"019a0000-0000-7000-8000-000000000002","order_key":"03","cells":["b"]}"#,
                ),
                &WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_B.to_owned()],
                ),
                &schemas,
            )
            .expect("compatibility row");
        let rows = batch.finish().expect("mixed canonical batch");

        assert!(rows[0].certificate().is_some());
        assert!(rows[1].certificate().is_some());
        assert_eq!(rows[0].batch_decoded_value_count(), 0);
        assert_eq!(rows[0].validation_counts(), (2, 1));
        assert_eq!(rows[0].batch_arena_allocation_count(), 1);
        assert!(rows[0].shares_batch_with(&rows[1]));
        assert_eq!(
            rows[0].normalized(),
            r#"{"cells":["a"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#
        );
        assert_eq!(
            rows[1].normalized(),
            r#"{"cells":["b"],"id":"019a0000-0000-7000-8000-000000000002","order_key":"03"}"#
        );
    }

    #[test]
    fn plugin_row_parser_counts_one_pass_for_canonical_compatibility_and_invalid_rows() {
        let schema = serde_json::from_str(include_str!(
            "../../../../../plugins/csv/schema/csv_row.json"
        ))
        .expect("CSV row schema");
        let catalog =
            CatalogSnapshot::from_schema_facts(&[crate::catalog::SchemaCatalogFact::new(
                crate::domain::Domain::schema_catalog("main", false),
                crate::schema::SchemaKey::new("csv_row"),
                schema,
            )])
            .expect("CSV row catalog");
        let schemas = SchemaAllowlist::from_catalog(&["csv_row".to_owned()], Arc::new(catalog))
            .expect("CSV allowlist");

        let mut canonical = CanonicalJsonBatchBuilder::with_row_capacity(1);
        canonical
            .push_plugin(
                Bytes::from_static(
                    br#"{"cells":["a"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#,
                ),
                &WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_A.to_owned()],
                ),
                &schemas,
            )
            .expect("canonical row");
        let canonical = canonical.finish().expect("canonical batch");
        assert_eq!(canonical[0].validation_counts(), (1, 0));
        assert_eq!(canonical[0].batch_arena_allocation_count(), 0);

        let mut compatibility = CanonicalJsonBatchBuilder::with_row_capacity(1);
        compatibility
            .push_plugin(
                Bytes::from_static(
                    br#" { "id":"019a0000-0000-7000-8000-000000000002", "order_key":"03", "cells":["b"] } "#,
                ),
                &WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_B.to_owned()],
                ),
                &schemas,
            )
            .expect("compatibility row");
        let compatibility = compatibility.finish().expect("compatibility batch");
        assert_eq!(compatibility[0].validation_counts(), (1, 1));
        assert_eq!(compatibility[0].batch_decoded_value_count(), 0);
        assert!(compatibility[0].certificate().is_some());
        assert_eq!(
            compatibility[0].normalized(),
            r#"{"cells":["b"],"id":"019a0000-0000-7000-8000-000000000002","order_key":"03"}"#
        );

        let mut invalid = CanonicalJsonBatchBuilder::with_row_capacity(1);
        let error = invalid
            .push_plugin(
                Bytes::from_static(
                    br#"{"cells":[1],"id":"019a0000-0000-7000-8000-000000000003","order_key":"05"}"#,
                ),
                &WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_C.to_owned()],
                ),
                &schemas,
            )
            .expect_err("number-bearing plugin row must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert_eq!(invalid.parse_count, 1);
        assert_eq!(invalid.serialize_count, 0);
        assert!(invalid.row_kinds.is_empty());
    }

    #[test]
    fn streaming_plugin_parser_matches_dom_canonicalization_for_compatibility_corpus() {
        let row_schema = serde_json::from_str(include_str!(
            "../../../../../plugins/csv/schema/csv_row.json"
        ))
        .expect("CSV row schema");
        let table_schema = serde_json::from_str(include_str!(
            "../../../../../plugins/csv/schema/csv_table.json"
        ))
        .expect("CSV table schema");
        let catalog = CatalogSnapshot::from_schema_facts(&[
            crate::catalog::SchemaCatalogFact::new(
                crate::domain::Domain::schema_catalog("main", false),
                crate::schema::SchemaKey::new("csv_row"),
                row_schema,
            ),
            crate::catalog::SchemaCatalogFact::new(
                crate::domain::Domain::schema_catalog("main", false),
                crate::schema::SchemaKey::new("csv_table"),
                table_schema,
            ),
        ])
        .expect("CSV catalog");
        let schemas = SchemaAllowlist::from_catalog(
            &["csv_row".to_owned(), "csv_table".to_owned()],
            Arc::new(catalog),
        )
        .expect("CSV allowlist");

        let valid = [
            (
                br#" { "id":"019a0000-0000-7000-8000-000000000001", "order_key":"01", "cells":[ "a", "b" ] } "#.as_slice(),
                WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_A.to_owned()],
                ),
            ),
            (
                br#"{"cells":["\uD83D\uDE00","line\u000Abreak"],"\u0069d":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#
                    .as_slice(),
                WasmEntityKey::from_owned_parts(
                    "csv_row",
                    vec![UUID_A.to_owned()],
                ),
            ),
            (
                br#" { "id":"root", "dialect": { "terminator":"\u000A", "quote":"\u0022", "\u0064elimiter":"," } } "#
                    .as_slice(),
                WasmEntityKey::from_owned_parts(
                    "csv_table",
                    vec!["root".to_owned()],
                ),
            ),
        ];
        for (input, key) in valid {
            let expected = canonicalize_snapshot(input).expect("DOM compatibility oracle");
            let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(1);
            batch
                .push_plugin(Bytes::copy_from_slice(input), &key, &schemas)
                .expect("streaming compatibility row");
            let rows = batch.finish().expect("streaming compatibility batch");

            assert_eq!(rows[0].normalized().as_bytes(), expected);
            assert_eq!(rows[0].validation_counts(), (1, 1));
            assert_eq!(rows[0].batch_decoded_value_count(), 0);
            assert_eq!(
                rows[0]
                    .certificate()
                    .expect("streaming row certificate")
                    .entity_pk()
                    .clone(),
                if key.schema_key == "csv_row" {
                    EntityPk::uuid_from_canonical(&key.entity_pk[0]).expect("canonical UUID")
                } else {
                    EntityPk::single(key.entity_pk[0].as_str())
                }
            );
        }

        for input in [
            br#"{"id":"019a0000-0000-7000-8000-000000000001","order_key":"01","\u0069d":"019a0000-0000-7000-8000-000000000001","cells":["a"]}"#.as_slice(),
            br#"{"cells":["\x"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#.as_slice(),
            br#"{"cells":["\uD83D"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#.as_slice(),
            br#"{"cells":["\uDE00"],"id":"019a0000-0000-7000-8000-000000000001","order_key":"01"}"#.as_slice(),
        ] {
            let dom_error =
                canonicalize_snapshot(input).expect_err("DOM oracle must reject hostile input");
            let mut batch = CanonicalJsonBatchBuilder::with_row_capacity(1);
            let streaming_error = batch
                .push_plugin(
                    Bytes::copy_from_slice(input),
                    &WasmEntityKey::from_owned_parts(
                        "csv_row",
                        vec![UUID_A.to_owned()],
                    ),
                    &schemas,
                )
                .expect_err("streaming parser must reject hostile input");
            assert_eq!(streaming_error.code, dom_error.code, "{input:?}");
            assert_eq!(batch.parse_count, 1);
            assert_eq!(batch.serialize_count, 0);
            assert!(batch.row_kinds.is_empty());
        }
    }

    #[test]
    fn vec_entity_sources_page_without_splitting_records() {
        let first = host_entity("a");
        let first_page_bytes = encoded_entity_record_bytes(&first).unwrap() + 4;
        let mut source = VecEntitySource::new(
            vec![first, host_entity("b")],
            WasmTransitionLimits::default(),
        )
        .unwrap();
        let first_page = source
            .next_page(u32::try_from(first_page_bytes).expect("test page size fits u32"))
            .unwrap()
            .unwrap();
        assert_eq!(first_page.entities.len(), 1);
        assert_eq!(first_page.entities[0].key.entity_pk[0], "a");
        let second_page = source
            .next_page(WasmTransitionLimits::default().max_page_bytes)
            .unwrap()
            .unwrap();
        assert_eq!(second_page.entities[0].key.entity_pk[0], "b");
        assert!(source.next_page(1).unwrap().is_none());
        assert!(source.next_page(1).unwrap().is_none());

        assert!(
            VecEntitySource::new(
                vec![host_entity("b"), host_entity("a")],
                WasmTransitionLimits::default()
            )
            .is_err()
        );
    }

    #[test]
    fn vec_entity_source_clamps_a_larger_consumer_page_hint() {
        let limits = WasmTransitionLimits::default();
        let mut source = VecEntitySource::new(vec![host_entity("a")], limits).unwrap();

        let page = source
            .next_page(limits.max_page_bytes.saturating_mul(4))
            .expect("larger consumer hint should be clamped")
            .expect("one entity page");

        assert_eq!(page.entities.len(), 1);
    }

    #[test]
    fn vec_change_source_rejects_unsorted_changes_and_pages_records() {
        let unsorted = WasmEntityChanges {
            changes: vec![
                WasmEntityChange::Delete(key("b")),
                WasmEntityChange::Delete(key("a")),
            ],
        };
        assert!(VecEntityChangeSource::new(unsorted, WasmTransitionLimits::default()).is_err());

        let first = WasmEntityChange::Delete(key("a"));
        let first_page_bytes = encoded_entity_change_record_bytes(&first).unwrap() + 4;
        let changes = WasmEntityChanges {
            changes: vec![first, WasmEntityChange::Delete(key("b"))],
        };
        let mut source =
            VecEntityChangeSource::new(changes, WasmTransitionLimits::default()).unwrap();
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

    struct FakeActor {
        change_pages: VecDeque<WasmChangePage>,
        resolution_pages: VecDeque<WasmConflictResolutionPage>,
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
                resolution_pages: VecDeque::new(),
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

        async fn open_entities(
            &mut self,
            _limits: WasmTransitionLimits,
            _input: WasmOpenEntitiesInput,
        ) -> Result<WasmEntityTransition, LixError> {
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

        async fn entities_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: crate::plugin::runtime::WasmEntityUpdate,
        ) -> Result<WasmEntityTransition, LixError> {
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

        async fn next_resolution_page(
            &mut self,
            _transition: WasmTransitionHandle,
            _cursor: crate::plugin::runtime::WasmResolutionCursorHandle,
            _max_bytes: u32,
        ) -> Result<Option<WasmConflictResolutionPage>, LixError> {
            Ok(self.resolution_pages.pop_front())
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
    async fn conflict_drain_requires_contiguous_host_ordinals() {
        let transition = WasmConflictTransition {
            transition: WasmTransitionHandle(41),
            resolutions: crate::plugin::runtime::WasmResolutionCursorHandle(42),
        };
        let page = WasmConflictResolutionPage {
            format_version: PACKET_FORMAT_V1,
            ordinals: vec![1],
            resolutions: vec![WasmConflictResolution::Take(
                crate::plugin::runtime::WasmConflictTake::B,
            )],
            outputs: None,
        };
        let mut actor = FakeActor {
            resolution_pages: [page].into(),
            ..FakeActor::default()
        };

        let error = drain_conflict_transition_resolutions(
            &mut actor,
            transition,
            1,
            WasmTransitionLimits::default(),
        )
        .await
        .expect_err("a reordered resolution must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert_eq!(actor.discarded_transitions, vec![transition.transition]);
        assert!(!actor.finished);
    }

    #[tokio::test]
    async fn conflict_drain_preserves_take_without_snapshot_output() {
        let transition = WasmConflictTransition {
            transition: WasmTransitionHandle(51),
            resolutions: crate::plugin::runtime::WasmResolutionCursorHandle(52),
        };
        let page = WasmConflictResolutionPage {
            format_version: PACKET_FORMAT_V1,
            ordinals: vec![0, 1],
            resolutions: vec![
                WasmConflictResolution::Take(crate::plugin::runtime::WasmConflictTake::B),
                WasmConflictResolution::Delete,
            ],
            outputs: None,
        };
        let mut actor = FakeActor {
            resolution_pages: [page].into(),
            ..FakeActor::default()
        };

        let drained = drain_conflict_transition_resolutions(
            &mut actor,
            transition,
            2,
            WasmTransitionLimits::default(),
        )
        .await
        .expect("aligned resolution cursor should drain");
        assert_eq!(drained.resolutions.len(), 2);
        assert_eq!(drained.counters.conflict_resolution_calls, 1);
        assert_eq!(drained.counters.conflict_resolution_records, 2);
        assert_eq!(drained.counters.conflict_resolution_takes, 1);
        assert!(actor.finished);
    }

    #[tokio::test]
    async fn conflict_replacements_share_one_canonical_batch() {
        let transition = WasmConflictTransition {
            transition: WasmTransitionHandle(56),
            resolutions: crate::plugin::runtime::WasmResolutionCursorHandle(57),
        };
        let page = WasmConflictResolutionPage {
            format_version: PACKET_FORMAT_V1,
            ordinals: vec![0, 1],
            resolutions: vec![
                WasmConflictResolution::Replace {
                    snapshot_content: WasmGuestBytes::Inline(br#"{"id":"a"}"#.to_vec().into()),
                    effect: WasmChangeEffect::Content,
                },
                WasmConflictResolution::Replace {
                    snapshot_content: WasmGuestBytes::Inline(br#"{"id":"b"}"#.to_vec().into()),
                    effect: WasmChangeEffect::FormatOnly,
                },
            ],
            outputs: None,
        };
        let mut actor = FakeActor {
            resolution_pages: [page].into(),
            ..FakeActor::default()
        };

        let drained = drain_conflict_transition_resolutions(
            &mut actor,
            transition,
            2,
            WasmTransitionLimits::default(),
        )
        .await
        .expect("valid replacements should drain");
        let WasmConflictResolution::Replace {
            snapshot_content: WasmHostBytes::CanonicalJson(first),
            ..
        } = &drained.resolutions[0]
        else {
            panic!("first resolution must retain canonical JSON")
        };
        let WasmConflictResolution::Replace {
            snapshot_content: WasmHostBytes::CanonicalJson(second),
            ..
        } = &drained.resolutions[1]
        else {
            panic!("second resolution must retain canonical JSON")
        };
        assert!(first.shares_batch_with(second));
        assert_eq!(first.validation_counts(), (2, 2));
        assert_eq!(first.batch_arena_allocation_count(), 1);
        assert_eq!(first.normalized(), r#"{"id":"a"}"#);
        assert_eq!(second.normalized(), r#"{"id":"b"}"#);
    }

    #[tokio::test]
    async fn conflict_drain_patches_replacements_without_reordering_other_results() {
        let transition = WasmConflictTransition {
            transition: WasmTransitionHandle(58),
            resolutions: crate::plugin::runtime::WasmResolutionCursorHandle(59),
        };
        let page = WasmConflictResolutionPage {
            format_version: PACKET_FORMAT_V1,
            ordinals: vec![0, 1, 2, 3],
            resolutions: vec![
                WasmConflictResolution::Replace {
                    snapshot_content: WasmGuestBytes::Inline(br#"{"id":"a"}"#.to_vec().into()),
                    effect: WasmChangeEffect::Content,
                },
                WasmConflictResolution::Take(crate::plugin::runtime::WasmConflictTake::B),
                WasmConflictResolution::Delete,
                WasmConflictResolution::Replace {
                    snapshot_content: WasmGuestBytes::Inline(br#"{"id":"b"}"#.to_vec().into()),
                    effect: WasmChangeEffect::FormatOnly,
                },
            ],
            outputs: None,
        };
        let mut actor = FakeActor {
            resolution_pages: [page].into(),
            ..FakeActor::default()
        };

        let drained = drain_conflict_transition_resolutions(
            &mut actor,
            transition,
            4,
            WasmTransitionLimits::default(),
        )
        .await
        .expect("interleaved replacements should retain their input ordinals");

        let WasmConflictResolution::Replace {
            snapshot_content: WasmHostBytes::CanonicalJson(first),
            effect: WasmChangeEffect::Content,
        } = &drained.resolutions[0]
        else {
            panic!("ordinal zero must remain the first replacement")
        };
        assert!(matches!(
            drained.resolutions[1],
            WasmConflictResolution::Take(crate::plugin::runtime::WasmConflictTake::B)
        ));
        assert!(matches!(
            drained.resolutions[2],
            WasmConflictResolution::Delete
        ));
        let WasmConflictResolution::Replace {
            snapshot_content: WasmHostBytes::CanonicalJson(second),
            effect: WasmChangeEffect::FormatOnly,
        } = &drained.resolutions[3]
        else {
            panic!("ordinal three must remain the second replacement")
        };
        assert!(first.shares_batch_with(second));
        assert_eq!(first.normalized(), r#"{"id":"a"}"#);
        assert_eq!(second.normalized(), r#"{"id":"b"}"#);
    }

    #[tokio::test]
    async fn conflict_drain_rejects_oversized_replacement_before_output_read() {
        let transition = WasmConflictTransition {
            transition: WasmTransitionHandle(61),
            resolutions: crate::plugin::runtime::WasmResolutionCursorHandle(62),
        };
        let limits = WasmTransitionLimits::default();
        let page = WasmConflictResolutionPage {
            format_version: PACKET_FORMAT_V1,
            ordinals: vec![0],
            resolutions: vec![WasmConflictResolution::Replace {
                snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                    index: 0,
                    offset: 0,
                    length: limits
                        .max_total_bytes
                        .checked_add(1)
                        .expect("default total-byte budget has headroom"),
                }),
                effect: WasmChangeEffect::Content,
            }],
            outputs: Some(WasmByteOutputsHandle(63)),
        };
        let mut actor = FakeActor {
            resolution_pages: [page].into(),
            ..FakeActor::default()
        };

        let error = drain_conflict_transition_resolutions(&mut actor, transition, 1, limits)
            .await
            .expect_err("oversized replacement must fail before output allocation or reads");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("max_total_bytes"), "{error:?}");
        assert_eq!(
            actor.output_len_calls, 0,
            "the preflight must reject before querying the guest output table"
        );
        assert_eq!(actor.discarded_transitions, vec![transition.transition]);
        assert!(!actor.finished);
    }

    #[tokio::test]
    async fn change_drain_validates_before_reading_and_canonicalizes_attachments() {
        let outputs = WasmByteOutputsHandle(7);
        let snapshot = br#"{"order_key":"a","id":"row","cells":[]}"#.to_vec();
        let second_snapshot = br#"{"order_key":"b","id":"row2","cells":[]}"#.to_vec();
        let page = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                changes: vec![
                    WasmEntityChange::Upsert {
                        entity: WasmEntity {
                            key: key("row"),
                            snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                                index: 0,
                                offset: 0,
                                length: snapshot.len() as u64,
                            }),
                        },
                        effect: WasmChangeEffect::Content,
                    },
                    WasmEntityChange::Upsert {
                        entity: WasmEntity {
                            key: key("row2"),
                            snapshot_content: WasmGuestBytes::Inline(second_snapshot.into()),
                        },
                        effect: WasmChangeEffect::Content,
                    },
                ],
            },
            outputs: Some(outputs),
        };
        let mut actor = FakeActor {
            change_pages: [page].into(),
            max_read_prefix: 7,
            runtime_counters: WasmTransitionCounters {
                source_read_calls: 2,
                ..WasmTransitionCounters::default()
            },
            ..FakeActor::default()
        };
        actor.outputs.insert((outputs, 0), snapshot);
        let transition = WasmFileTransition {
            transition: WasmTransitionHandle(1),
            document: WasmDocumentHandle(2),
            changes: WasmChangeCursorHandle(3),
        };
        let schemas = SchemaAllowlist::new(["csv_row".to_owned()]).unwrap();

        let drained = drain_file_transition_changes(
            &mut actor,
            transition,
            test_creates(),
            &schemas,
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(drained.document, WasmDocumentHandle(2));
        let WasmEntityChange::Upsert { entity, .. } = &drained.changes.changes[0] else {
            panic!("expected upsert")
        };
        let WasmHostBytes::CanonicalJson(json) = &entity.snapshot_content else {
            panic!("resolved snapshots must retain parsed canonical JSON")
        };
        assert_eq!(
            json.normalized(),
            r#"{"cells":[],"id":"row","order_key":"a"}"#
        );
        assert_eq!(json.value()["id"], "row");
        let WasmEntityChange::Upsert { entity, .. } = &drained.changes.changes[1] else {
            panic!("expected second upsert")
        };
        let WasmHostBytes::CanonicalJson(second_json) = &entity.snapshot_content else {
            panic!("resolved snapshots must retain parsed canonical JSON")
        };
        assert!(json.shares_batch_with(second_json));
        assert_eq!(json.validation_counts(), (2, 2));
        assert_eq!(json.batch_arena_allocation_count(), 1);
        assert!(drained.counters.attachment_reads > 1);
        assert_eq!(drained.counters.source_read_calls, 2);
        assert!(actor.finished);
    }

    #[tokio::test]
    async fn change_drain_bounds_canonical_arenas_to_cursor_pages() {
        let page = |id: &str| WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                changes: vec![WasmEntityChange::Upsert {
                    entity: WasmEntity {
                        key: key(id),
                        snapshot_content: WasmGuestBytes::Inline(
                            format!(r#"{{"id":"{id}"}}"#).into_bytes().into(),
                        ),
                    },
                    effect: WasmChangeEffect::Content,
                }],
            },
            outputs: None,
        };
        let mut actor = FakeActor {
            change_pages: [page("a"), page("b")].into(),
            ..FakeActor::default()
        };
        let transition = WasmFileTransition {
            transition: WasmTransitionHandle(11),
            document: WasmDocumentHandle(12),
            changes: WasmChangeCursorHandle(13),
        };
        let schemas = SchemaAllowlist::new(["csv_row".to_owned()]).unwrap();

        let drained = drain_file_transition_changes(
            &mut actor,
            transition,
            test_creates(),
            &schemas,
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        let rows = drained
            .changes
            .changes
            .iter()
            .map(|change| match change {
                WasmEntityChange::Upsert {
                    entity:
                        WasmEntity {
                            snapshot_content: WasmHostBytes::CanonicalJson(json),
                            ..
                        },
                    ..
                } => json,
                _ => panic!("test pages contain only canonical upserts"),
            })
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert!(!rows[0].shares_batch_with(rows[1]));
        assert_eq!(rows[0].batch_row_count(), 1);
        assert_eq!(rows[1].batch_row_count(), 1);
        assert_eq!(rows[0].validation_counts(), (1, 1));
        assert_eq!(rows[1].validation_counts(), (1, 1));
        assert_eq!(rows[0].batch_arena_allocation_count(), 1);
        assert_eq!(rows[1].batch_arena_allocation_count(), 1);
    }

    #[tokio::test]
    async fn change_drain_patches_only_appended_page_ranges_in_source_order() {
        let upsert = |id: &str| WasmEntityChange::Upsert {
            entity: WasmEntity {
                key: key(id),
                snapshot_content: WasmGuestBytes::Inline(
                    format!(r#"{{"id":"{id}"}}"#).into_bytes().into(),
                ),
            },
            effect: WasmChangeEffect::Content,
        };
        let page = |changes| WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges { changes },
            outputs: None,
        };
        let mut actor = FakeActor {
            change_pages: [
                page(vec![
                    upsert("a"),
                    WasmEntityChange::Delete(key("gone-a")),
                    upsert("b"),
                ]),
                page(vec![WasmEntityChange::Delete(key("gone-b")), upsert("c")]),
            ]
            .into(),
            ..FakeActor::default()
        };
        let transition = WasmFileTransition {
            transition: WasmTransitionHandle(14),
            document: WasmDocumentHandle(15),
            changes: WasmChangeCursorHandle(16),
        };
        let schemas = SchemaAllowlist::new(["csv_row".to_owned()]).unwrap();

        let drained = drain_file_transition_changes(
            &mut actor,
            transition,
            test_creates(),
            &schemas,
            WasmTransitionLimits::default(),
        )
        .await
        .expect("interleaved rows should retain exact source order");
        let changes = &drained.changes.changes;
        assert_eq!(changes.len(), 5);
        assert_eq!(
            changes[0].entity_key().expect("entity key").entity_pk[0],
            "a"
        );
        assert_eq!(
            changes[1].entity_key().expect("entity key").entity_pk[0],
            "gone-a"
        );
        assert_eq!(
            changes[2].entity_key().expect("entity key").entity_pk[0],
            "b"
        );
        assert_eq!(
            changes[3].entity_key().expect("entity key").entity_pk[0],
            "gone-b"
        );
        assert_eq!(
            changes[4].entity_key().expect("entity key").entity_pk[0],
            "c"
        );
        assert!(matches!(changes[1], WasmEntityChange::Delete(_)));
        assert!(matches!(changes[3], WasmEntityChange::Delete(_)));

        fn canonical(change: &WasmEntityChange<WasmHostBytes>) -> &WasmCanonicalJson {
            match change {
                WasmEntityChange::Upsert {
                    entity:
                        WasmEntity {
                            snapshot_content: WasmHostBytes::CanonicalJson(json),
                            ..
                        },
                    ..
                } => json,
                _ => panic!("expected canonical upsert"),
            }
        }
        let first = canonical(&changes[0]);
        let second = canonical(&changes[2]);
        let third = canonical(&changes[4]);
        assert_eq!(first.normalized(), r#"{"id":"a"}"#);
        assert_eq!(second.normalized(), r#"{"id":"b"}"#);
        assert_eq!(third.normalized(), r#"{"id":"c"}"#);
        assert!(first.shares_batch_with(second));
        assert!(!second.shares_batch_with(third));
    }

    #[tokio::test]
    async fn change_drain_rejects_duplicate_keys_across_pages() {
        let duplicate = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                changes: vec![WasmEntityChange::Delete(key("row"))],
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
            "invalid component change cursor page: a component entity key may occur only once across a change cursor"
        );
        assert_eq!(actor.discarded_transitions, vec![transition.transition]);
        assert!(!actor.finished);
    }

    #[tokio::test]
    async fn host_validation_rejection_discards_transition_and_allows_retry() {
        let outputs = WasmByteOutputsHandle(7);
        let page = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                changes: vec![WasmEntityChange::Upsert {
                    entity: WasmEntity {
                        key: WasmEntityKey::from_owned_parts("not_allowed", vec!["row".to_owned()]),
                        snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                            index: 0,
                            offset: 0,
                            length: 2,
                        }),
                    },
                    effect: WasmChangeEffect::Content,
                }],
            },
            outputs: Some(outputs),
        };
        let mut actor = FakeActor {
            change_pages: [page].into(),
            ..FakeActor::default()
        };
        actor.outputs.insert((outputs, 0), b"{}".to_vec());
        let error = drain_file_transition_changes(
            &mut actor,
            WasmFileTransition {
                transition: WasmTransitionHandle(1),
                document: WasmDocumentHandle(2),
                changes: WasmChangeCursorHandle(3),
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
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                changes: vec![WasmEntityChange::Delete(key("row"))],
            },
            outputs: None,
        });
        let retried = drain_file_transition_changes(
            &mut actor,
            WasmFileTransition {
                transition: WasmTransitionHandle(4),
                document: WasmDocumentHandle(5),
                changes: WasmChangeCursorHandle(6),
            },
            test_creates(),
            &SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
            WasmTransitionLimits::default(),
        )
        .await
        .expect("a deterministic host rejection must leave the actor reusable");
        assert_eq!(retried.document, WasmDocumentHandle(5));
        assert_eq!(retried.changes.entity_change_count(), 1);
        assert!(actor.finished);
        assert!(!actor.retired);
    }

    #[tokio::test]
    async fn uncertain_transition_cleanup_retires_the_actor() {
        let mut actor = FakeActor {
            change_pages: [WasmChangePage {
                format_version: PACKET_FORMAT_V1,
                changes: WasmEntityChanges {
                    changes: vec![WasmEntityChange::Delete(WasmEntityKey::from_owned_parts(
                        "not_allowed",
                        vec!["row".to_owned()],
                    ))],
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
        let drained = drain_entity_transition_edits(
            &mut actor,
            WasmEntityTransition {
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
        let drained = drain_entity_transition_edits(
            &mut actor,
            WasmEntityTransition {
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
        let drained = drain_entity_transition_edits(
            &mut actor,
            WasmEntityTransition {
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
