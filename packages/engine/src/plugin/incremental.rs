//! Engine-side plumbing for incremental Wasm Component v2 transitions.
//!
//! This module owns validation and bounded host adapters around the host-
//! neutral `wasm::v2` traits. It deliberately does not decide transaction,
//! conflict-resolution, observation, or actor-publication policy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};

use crate::LixError;
use crate::common::RequestBlobSpliceProvenance;
use crate::wasm::{
    PACKET_FORMAT_V1, WasmByteOutputsHandle, WasmByteSource, WasmChangeDrainValidator,
    WasmChangePage, WasmComponentV2Actor, WasmDocumentHandle, WasmEditDrainValidator, WasmEditPage,
    WasmEntity, WasmEntityChange, WasmEntityChangeSource, WasmEntityChanges, WasmEntityPage,
    WasmEntitySource, WasmEntityTransition, WasmFileTransition, WasmGuestBytes, WasmHostBytes,
    WasmHostEntity, WasmHostEntityChanges, WasmInputBytes, WasmInputSplice, WasmMergeGroup,
    WasmOutputRange, WasmSourceRange, WasmTransitionCounters, WasmTransitionHandle,
    WasmTransitionLimits,
};

/// Immutable contiguous bytes with shared ownership and observable range-read
/// counters. Clones share both the allocation and counters.
#[derive(Debug, Clone)]
pub(crate) struct ArcByteSource {
    bytes: Arc<[u8]>,
    reads: Arc<ArcByteSourceCounters>,
}

#[derive(Debug, Default)]
struct ArcByteSourceCounters {
    calls: AtomicU64,
    bytes: AtomicU64,
}

impl ArcByteSource {
    pub(crate) fn new(bytes: Arc<[u8]>) -> Self {
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
            return Err(invalid_input("v2 byte-source reads must request bytes"));
        }
        let start = usize::try_from(offset)
            .map_err(|_| invalid_input("v2 byte-source offset does not fit this host"))?;
        if start > self.bytes.len() {
            return Err(invalid_input("v2 byte-source offset is out of bounds"));
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
    /// Bytes examined only by the bounded host full-diff fallback.
    pub(crate) full_diff_bytes_compared: u64,
}

/// Builds one coalesced base-relative replacement. Valid transport provenance
/// preserves the exact remote splice. Without it, the fallback compares the
/// full before/after blobs once to find their maximal common prefix/suffix.
pub(crate) fn build_file_update_splices(
    before: &[u8],
    after: &[u8],
    provenance: Option<&RequestBlobSpliceProvenance>,
    limits: WasmTransitionLimits,
) -> Result<BuiltInputSplices, LixError> {
    limits.validate()?;
    if provenance.is_none() && before == after {
        return Ok(BuiltInputSplices {
            edits: Vec::new(),
            used_transport_provenance: false,
            full_diff_bytes_compared: 0,
        });
    }

    let (prefix, suffix, insert, used_transport_provenance, compared) =
        if let Some(provenance) = provenance {
            validate_transport_splice(before, after, provenance)?;
            (
                provenance.prefix_bytes,
                provenance.suffix_bytes,
                provenance.insert.as_slice(),
                true,
                0,
            )
        } else {
            let (prefix, prefix_bytes_compared) = common_prefix_len(before, after);
            let max_suffix = before
                .len()
                .saturating_sub(prefix)
                .min(after.len().saturating_sub(prefix));
            let (suffix, suffix_bytes_compared) = common_suffix_len(before, after, max_suffix);
            let insert_end = after.len() - suffix;
            (
                prefix,
                suffix,
                &after[prefix..insert_end],
                false,
                prefix_bytes_compared.saturating_add(suffix_bytes_compared),
            )
        };

    let delete_len = before
        .len()
        .checked_sub(prefix)
        .and_then(|length| length.checked_sub(suffix))
        .ok_or_else(|| invalid_input("v2 splice deletion length underflowed"))?;
    if delete_len == 0 && insert.is_empty() {
        return Ok(BuiltInputSplices {
            edits: Vec::new(),
            used_transport_provenance,
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

    let prefix = provenance.prefix_bytes;
    let suffix = provenance.suffix_bytes;
    let Some(insert_end) = prefix.checked_add(provenance.insert.len()) else {
        return false;
    };
    let Some(expected_after_len) = insert_end.checked_add(suffix) else {
        return false;
    };
    if expected_after_len != after.len()
        || prefix > after.len()
        || insert_end > after.len()
        || provenance.insert != after[prefix..insert_end]
    {
        return false;
    }

    let window_start = prefix.saturating_sub(UTF8_BOUNDARY_CONTEXT);
    let window_end = insert_end
        .saturating_add(UTF8_BOUNDARY_CONTEXT)
        .min(after.len());
    std::str::from_utf8(&after[window_start..window_end]).is_ok()
}

fn validate_transport_splice(
    before: &[u8],
    after: &[u8],
    provenance: &RequestBlobSpliceProvenance,
) -> Result<(), LixError> {
    let prefix = provenance.prefix_bytes;
    let suffix = provenance.suffix_bytes;
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
        .checked_add(provenance.insert.len())
        .and_then(|length| length.checked_add(suffix))
        .ok_or_else(|| invalid_input("transport splice result length overflowed"))?;
    let valid_sha256 =
        |value: &str| value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit);
    // This sidecar is constructed only after the remote protocol verifies the
    // base/result hashes and reconstructs the ordinary SQL blob. Rechecking
    // the unchanged prefix and suffix here would turn a localized edit back
    // into an O(document) scan. Bounds, hashes' shape, result length, and the
    // small inserted range remain cheap defense-in-depth checks.
    if expected_after_len != after.len()
        || !valid_sha256(&provenance.base_sha256)
        || !valid_sha256(&provenance.result_sha256)
        || provenance.insert != after[prefix..after.len() - suffix]
    {
        return Err(invalid_input(
            "transport splice metadata does not match the accepted before/after bytes",
        ));
    }
    Ok(())
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> (usize, u64) {
    let mut common = 0usize;
    let mut bytes_compared = 0u64;
    for (left, right) in left.iter().zip(right) {
        bytes_compared = bytes_compared.saturating_add(2);
        if left != right {
            break;
        }
        common += 1;
    }
    (common, bytes_compared)
}

fn common_suffix_len(left: &[u8], right: &[u8], max: usize) -> (usize, u64) {
    let mut common = 0usize;
    let mut bytes_compared = 0u64;
    for (left, right) in left.iter().rev().take(max).zip(right.iter().rev()) {
        bytes_compared = bytes_compared.saturating_add(2);
        if left != right {
            break;
        }
        common += 1;
    }
    (common, bytes_compared)
}

#[derive(Debug, Clone)]
pub(crate) struct V2SchemaAllowlist {
    schema_keys: BTreeSet<String>,
}

impl V2SchemaAllowlist {
    pub(crate) fn new(schema_keys: impl IntoIterator<Item = String>) -> Result<Self, LixError> {
        let schema_keys = schema_keys.into_iter().collect::<BTreeSet<_>>();
        if schema_keys.is_empty() {
            return Err(invalid_input("v2 schema allowlist must not be empty"));
        }
        Ok(Self { schema_keys })
    }

    pub(crate) fn from_slice(schema_keys: &[String]) -> Result<Self, LixError> {
        Self::new(schema_keys.iter().cloned())
    }

    fn validate(&self, schema_key: &str) -> Result<(), LixError> {
        if !self.schema_keys.contains(schema_key) {
            return Err(invalid_guest(format!(
                "v2 plugin emitted undeclared schema '{schema_key}'"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum NumberFreeJson {
    Null,
    Bool(bool),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl<'de> Deserialize<'de> for NumberFreeJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(NumberFreeJsonVisitor)
    }
}

struct NumberFreeJsonVisitor;

impl<'de> Visitor<'de> for NumberFreeJsonVisitor {
    type Value = NumberFreeJson;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("number-free JSON")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(NumberFreeJson::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(NumberFreeJson::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(NumberFreeJson::Bool(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(NumberFreeJson::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(NumberFreeJson::String(value))
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom("JSON numbers are not enabled for production v2"))
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom("JSON numbers are not enabled for production v2"))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom("JSON numbers are not enabled for production v2"))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(value) = sequence.next_element()? {
            values.push(value);
        }
        Ok(NumberFreeJson::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(A::Error::custom(format!(
                    "duplicate decoded JSON object key '{key}'"
                )));
            }
            values.insert(key, map.next_value()?);
        }
        Ok(NumberFreeJson::Object(values))
    }
}

/// Parses, duplicate-checks, number-gates, and canonically encodes one CSV v2
/// snapshot. Snapshot roots must be objects.
pub(crate) fn canonicalize_csv_snapshot(bytes: &[u8]) -> Result<Vec<u8>, LixError> {
    let value = parse_number_free_snapshot(bytes)?;
    if !matches!(value, NumberFreeJson::Object(_)) {
        return Err(invalid_guest("v2 entity snapshots must be JSON objects"));
    }
    let mut canonical = String::new();
    encode_number_free_json(&value, &mut canonical);
    Ok(canonical.into_bytes())
}

fn parse_number_free_snapshot(bytes: &[u8]) -> Result<NumberFreeJson, LixError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = NumberFreeJson::deserialize(&mut deserializer).map_err(|error| {
        invalid_guest(format!(
            "v2 snapshot must be duplicate-free number-free UTF-8 JSON: {error}"
        ))
    })?;
    deserializer.end().map_err(|error| {
        invalid_guest(format!(
            "v2 snapshot contains trailing or invalid JSON input: {error}"
        ))
    })?;
    Ok(value)
}

fn encode_number_free_json(value: &NumberFreeJson, output: &mut String) {
    match value {
        NumberFreeJson::Null => output.push_str("null"),
        NumberFreeJson::Bool(true) => output.push_str("true"),
        NumberFreeJson::Bool(false) => output.push_str("false"),
        NumberFreeJson::String(value) => encode_json_string(value, output),
        NumberFreeJson::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                encode_number_free_json(value, output);
            }
            output.push(']');
        }
        NumberFreeJson::Object(values) => {
            output.push('{');
            for (index, (key, value)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                encode_json_string(key, output);
                output.push(':');
                encode_number_free_json(value, output);
            }
            output.push('}');
        }
    }
}

fn encode_json_string(value: &str, output: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    output.push('"');
    for scalar in value.chars() {
        match scalar {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            scalar if scalar <= '\u{1f}' => {
                let scalar = scalar as usize;
                output.push_str("\\u00");
                output.push(HEX[(scalar >> 4) & 0x0f] as char);
                output.push(HEX[scalar & 0x0f] as char);
            }
            scalar => output.push(scalar),
        }
    }
    output.push('"');
}

/// Vec-backed complete-entity packet source for cold render and activated-
/// entity hydration. Construction enforces packet-v1's global key order.
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

    pub(crate) fn empty(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Self::new(Vec::new(), limits)
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
                return Err(invalid_input("v2 entity record exceeds max_record_bytes"));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("v2 entity frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if entities.is_empty() {
                    return Err(invalid_input(
                        "v2 entity record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            page_refs = page_refs
                .checked_add(host_bytes_attachment_refs(&entity.snapshot_content))
                .ok_or_else(|| invalid_input("v2 entity attachment count overflowed"))?;
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

/// Vec-backed merge-group source for the final resolved semantic changes
/// supplied to `entities_changed`.
#[derive(Debug)]
pub(crate) struct VecEntityChangeSource {
    groups: VecDeque<WasmMergeGroup<WasmHostBytes>>,
    state: VecSourceState,
}

impl VecEntityChangeSource {
    pub(crate) fn new(
        changes: WasmHostEntityChanges,
        limits: WasmTransitionLimits,
    ) -> Result<Self, LixError> {
        changes.validate()?;
        for group in &changes.groups {
            validate_group_member_order(group)?;
            for change in &group.changes {
                if let WasmEntityChange::Upsert { entity, .. } = change {
                    validate_host_entity(entity)?;
                } else if change.key().entity_pk.is_empty() {
                    return Err(invalid_input("v2 entity primary keys must not be empty"));
                }
            }
        }
        Ok(Self {
            groups: changes.groups.into(),
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
        if self.groups.is_empty() {
            self.state.reached_eof = true;
            return Ok(None);
        }

        let mut page_bytes = 0u64;
        let mut page_refs = 0u32;
        let mut groups = Vec::new();
        while let Some(group) = self.groups.front() {
            let record_bytes = encoded_merge_group_record_bytes(group)?;
            if record_bytes > u64::from(self.state.limits.max_record_bytes) {
                return Err(invalid_input(
                    "v2 merge-group record exceeds max_record_bytes",
                ));
            }
            let framed_bytes = record_bytes
                .checked_add(4)
                .ok_or_else(|| invalid_input("v2 merge-group frame length overflowed"))?;
            if page_bytes
                .checked_add(framed_bytes)
                .is_none_or(|size| size > page_limit)
            {
                if groups.is_empty() {
                    return Err(invalid_input(
                        "v2 merge-group record does not fit the requested page",
                    ));
                }
                break;
            }
            page_bytes += framed_bytes;
            page_refs = page_refs
                .checked_add(group_attachment_refs(group))
                .ok_or_else(|| invalid_input("v2 change attachment count overflowed"))?;
            groups.push(
                self.groups
                    .pop_front()
                    .expect("front merge group was just inspected"),
            );
        }
        self.state.accept_page(page_bytes, page_refs)?;
        Ok(Some(WasmEntityChanges { groups }))
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
            return Err(invalid_input("v2 packet source page size must be positive"));
        }
        if requested > self.limits.max_page_bytes {
            return Err(invalid_input(
                "v2 packet source page request exceeds max_page_bytes",
            ));
        }
        Ok(u64::from(requested))
    }

    fn accept_page(&mut self, inline_bytes: u64, refs: u32) -> Result<(), LixError> {
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_input("v2 packet source page count overflowed"))?;
        self.total_inline_bytes = self
            .total_inline_bytes
            .checked_add(inline_bytes)
            .ok_or_else(|| invalid_input("v2 packet source byte count overflowed"))?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(refs)
            .ok_or_else(|| invalid_input("v2 packet source attachment count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_input("v2 packet source exceeds max_pages"));
        }
        if self.total_inline_bytes > self.limits.max_total_bytes {
            return Err(invalid_input("v2 packet source exceeds max_total_bytes"));
        }
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(invalid_input(
                "v2 packet source exceeds max_attachment_refs",
            ));
        }
        Ok(())
    }
}

fn validate_entity_order(entities: &[WasmHostEntity]) -> Result<(), LixError> {
    for pair in entities.windows(2) {
        if pair[0].key >= pair[1].key {
            return Err(invalid_input(
                "v2 complete entity sources must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_group_member_order<B>(group: &WasmMergeGroup<B>) -> Result<(), LixError> {
    for pair in group.changes.windows(2) {
        if pair[0].key() >= pair[1].key() {
            return Err(invalid_input(
                "v2 merge-group members must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

fn validate_host_entity(entity: &WasmHostEntity) -> Result<(), LixError> {
    if entity.key.entity_pk.is_empty() {
        return Err(invalid_input("v2 entity primary keys must not be empty"));
    }
    if let WasmHostBytes::Source(slice) = &entity.snapshot_content {
        slice.validate()?;
    }
    Ok(())
}

fn encoded_entity_record_bytes(entity: &WasmHostEntity) -> Result<u64, LixError> {
    encoded_entity_key_bytes(&entity.key)?
        .checked_add(encoded_host_bytes_ref_bytes(&entity.snapshot_content)?)
        .ok_or_else(|| invalid_input("v2 entity record size overflowed"))
}

fn encoded_merge_group_record_bytes(
    group: &WasmMergeGroup<WasmHostBytes>,
) -> Result<u64, LixError> {
    let _ = u32::try_from(group.changes.len())
        .map_err(|_| invalid_input("v2 merge group has too many members"))?;
    let mut size = 4u64;
    for change in &group.changes {
        let key_bytes = encoded_entity_key_bytes(change.key())?;
        size = size
            .checked_add(1)
            .and_then(|size| size.checked_add(key_bytes))
            .ok_or_else(|| invalid_input("v2 merge-group record size overflowed"))?;
        if let WasmEntityChange::Upsert { entity, .. } = change {
            let snapshot_bytes = encoded_host_bytes_ref_bytes(&entity.snapshot_content)?;
            size = size
                .checked_add(1)
                .and_then(|size| size.checked_add(snapshot_bytes))
                .ok_or_else(|| invalid_input("v2 merge-group record size overflowed"))?;
        }
    }
    Ok(size)
}

fn encoded_entity_key_bytes(key: &crate::wasm::WasmEntityKey) -> Result<u64, LixError> {
    if key.entity_pk.is_empty() {
        return Err(invalid_input("v2 entity primary keys must not be empty"));
    }
    let _ = u32::try_from(key.entity_pk.len())
        .map_err(|_| invalid_input("v2 entity primary key has too many components"))?;
    let mut size = encoded_text_bytes(&key.schema_key)?
        .checked_add(4)
        .ok_or_else(|| invalid_input("v2 entity key size overflowed"))?;
    for component in &key.entity_pk {
        size = size
            .checked_add(encoded_text_bytes(component)?)
            .ok_or_else(|| invalid_input("v2 entity key size overflowed"))?;
    }
    Ok(size)
}

fn encoded_text_bytes(value: &str) -> Result<u64, LixError> {
    let length = u32::try_from(value.len())
        .map_err(|_| invalid_input("v2 packet text exceeds u32 framing"))?;
    Ok(u64::from(length) + 4)
}

fn encoded_host_bytes_ref_bytes(value: &WasmHostBytes) -> Result<u64, LixError> {
    match value {
        WasmHostBytes::Inline(bytes) => {
            let length = u32::try_from(bytes.len())
                .map_err(|_| invalid_input("v2 inline snapshot exceeds u32 framing"))?;
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

fn group_attachment_refs(group: &WasmMergeGroup<WasmHostBytes>) -> u32 {
    group
        .changes
        .iter()
        .map(|change| match change {
            WasmEntityChange::Upsert { entity, .. } => {
                host_bytes_attachment_refs(&entity.snapshot_content)
            }
            WasmEntityChange::Delete(_) => 0,
        })
        .sum()
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedFileTransition {
    pub(crate) document: WasmDocumentHandle,
    pub(crate) changes: WasmHostEntityChanges,
    pub(crate) counters: WasmTransitionCounters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedOutputSplice {
    pub(crate) offset: u64,
    pub(crate) delete_len: u64,
    pub(crate) insert: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedEntityTransition {
    pub(crate) document: WasmDocumentHandle,
    pub(crate) bytes: Arc<[u8]>,
    #[cfg(test)]
    pub(crate) edits: Vec<ResolvedOutputSplice>,
    pub(crate) counters: WasmTransitionCounters,
}

/// Drains and validates every merge group before returning any proposed
/// semantic change to transaction code. Validation of a page's keys,
/// attachment count, and aggregate budget happens before the first attachment
/// method is invoked.
pub(crate) async fn drain_file_transition_changes(
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmFileTransition,
    schemas: &V2SchemaAllowlist,
    limits: WasmTransitionLimits,
) -> Result<ValidatedFileTransition, LixError> {
    let transition_handle = transition.transition;
    match drain_file_transition_changes_inner(actor, transition, schemas, limits).await {
        Ok(validated) => Ok(validated),
        Err(error) => Err(cleanup_rejected_transition(actor, transition_handle, error).await),
    }
}

async fn drain_file_transition_changes_inner(
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmFileTransition,
    schemas: &V2SchemaAllowlist,
    limits: WasmTransitionLimits,
) -> Result<ValidatedFileTransition, LixError> {
    let mut validator = WasmChangeDrainValidator::new(limits)?;
    let mut budget = OutputDrainBudget::new(limits)?;
    let mut local_counters = WasmTransitionCounters::default();
    let mut groups = Vec::new();

    loop {
        let Some(page) = actor
            .next_change_page(
                transition.transition,
                transition.changes,
                limits.max_page_bytes,
            )
            .await?
        else {
            validator.accept_eof();
            break;
        };
        validator.accept_page(&page).map_err(|error| {
            invalid_guest(format!("invalid v2 change cursor page: {}", error.message))
        })?;
        prevalidate_change_page(&page, schemas, &mut budget)?;
        local_counters.packet_pages = local_counters.packet_pages.saturating_add(1);
        local_counters.packet_records = local_counters
            .packet_records
            .saturating_add(page.changes.groups.len() as u64);

        let outputs = page.outputs;
        for group in page.changes.groups {
            let mut resolved_changes = Vec::with_capacity(group.changes.len());
            for change in group.changes {
                let resolved = match change {
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
                        let snapshot = canonicalize_csv_snapshot(&snapshot)?;
                        WasmEntityChange::Upsert {
                            entity: WasmEntity {
                                key: entity.key,
                                snapshot_content: WasmHostBytes::Inline(snapshot),
                            },
                            effect,
                        }
                    }
                };
                resolved_changes.push(resolved);
            }
            groups.push(WasmMergeGroup {
                changes: resolved_changes,
            });
        }
    }

    let runtime_counters = actor.finish_transition(transition.transition).await?;
    Ok(ValidatedFileTransition {
        document: transition.document,
        changes: WasmEntityChanges { groups },
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

/// Drains renderer edit pages, resolves lazy output ranges, applies the edits
/// against one immutable base, and optionally proves byte equality with an
/// independently reconstructed expected result.
pub(crate) async fn drain_entity_transition_edits(
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmEntityTransition,
    base: &[u8],
    expected: Option<Arc<[u8]>>,
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
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmEntityTransition,
    base: &[u8],
    expected: Option<Arc<[u8]>>,
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
            invalid_guest(format!("invalid v2 edit cursor page: {}", error.message))
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

    let bytes = if let (Some(expected), Some(expected_delta)) = (&expected, expected_delta) {
        validate_resolved_output_against_known_delta(base, expected, expected_delta, &edits)?;
        Arc::clone(expected)
    } else {
        let bytes: Arc<[u8]> = apply_resolved_output_splices(base, &edits)?.into();
        if expected
            .as_ref()
            .is_some_and(|expected| expected.as_ref() != bytes.as_ref())
        {
            return Err(invalid_guest(
                "v2 renderer edits do not reproduce the independently expected bytes",
            ));
        }
        bytes
    };
    let runtime_counters = actor.finish_transition(transition.transition).await?;
    Ok(ValidatedEntityTransition {
        document: transition.document,
        bytes,
        #[cfg(test)]
        edits,
        counters: merge_counter_snapshots(local_counters, runtime_counters),
    })
}

async fn cleanup_rejected_transition(
    actor: &mut dyn WasmComponentV2Actor,
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
            .map_err(|_| invalid_guest("v2 output deletion does not fit this host"))?;
        length
            .checked_sub(delete_len)
            .and_then(|length| length.checked_add(edit.insert.len()))
            .ok_or_else(|| invalid_guest("v2 rendered result length overflowed"))
    })?;
    if output_len != expected.len() {
        return Err(invalid_guest(
            "v2 renderer edits do not reproduce the independently expected length",
        ));
    }

    let mut covered = vec![false; input.len()];
    for edit in output {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("v2 output offset does not fit this host"))?;
        let end =
            start
                .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                    invalid_guest("v2 output deletion length does not fit this host")
                })?)
                .ok_or_else(|| invalid_guest("v2 output deletion range overflowed"))?;
        let mut reconstructed = Vec::with_capacity(edit.insert.len());
        let mut cursor = start;
        for (index, input_edit) in input.iter().enumerate() {
            let input_start = usize::try_from(input_edit.offset)
                .map_err(|_| invalid_input("v2 input offset does not fit this host"))?;
            let input_end = input_start
                .checked_add(usize::try_from(input_edit.delete_len).map_err(|_| {
                    invalid_input("v2 input deletion length does not fit this host")
                })?)
                .ok_or_else(|| invalid_input("v2 input deletion range overflowed"))?;
            let is_inside = input_start >= start
                && input_end <= end
                && (input_start < end || (input_start == end && input_edit.delete_len == 0));
            if !is_inside {
                continue;
            }
            if covered[index] || input_start < cursor {
                return Err(invalid_guest(
                    "v2 renderer edits do not cover the known input delta exactly once",
                ));
            }
            reconstructed.extend_from_slice(&base[cursor..input_start]);
            match &input_edit.insert {
                WasmInputBytes::Inline(bytes) => reconstructed.extend_from_slice(bytes),
                WasmInputBytes::AfterRange(range) => {
                    let range_start = usize::try_from(range.offset).map_err(|_| {
                        invalid_input("v2 after-range offset does not fit this host")
                    })?;
                    let range_end = usize::try_from(range.end()?)
                        .map_err(|_| invalid_input("v2 after-range end does not fit this host"))?;
                    reconstructed.extend_from_slice(
                        expected.get(range_start..range_end).ok_or_else(|| {
                            invalid_input("v2 after-range is out of expected-result bounds")
                        })?,
                    );
                }
            }
            cursor = input_end;
            covered[index] = true;
        }
        reconstructed.extend_from_slice(
            base.get(cursor..end)
                .ok_or_else(|| invalid_guest("v2 output deletion exceeds accepted bytes"))?,
        );
        if reconstructed != edit.insert {
            return Err(invalid_guest(
                "v2 renderer edit does not reproduce the independently expected local region",
            ));
        }
    }
    if covered.iter().any(|covered| !covered) {
        return Err(invalid_guest(
            "v2 renderer edits omitted part of the independently expected delta",
        ));
    }
    Ok(())
}

fn prevalidate_change_page(
    page: &WasmChangePage,
    schemas: &V2SchemaAllowlist,
    budget: &mut OutputDrainBudget,
) -> Result<(), LixError> {
    if page.format_version != PACKET_FORMAT_V1 {
        return Err(invalid_guest("unsupported v2 change packet format"));
    }
    let mut inline_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut minimum_attachment_reads = 0u64;
    let mut references = 0u32;
    for group in &page.changes.groups {
        validate_guest_group_member_order(group)?;
        for change in &group.changes {
            schemas.validate(&change.key().schema_key)?;
            if change.key().entity_pk.is_empty() {
                return Err(invalid_guest("v2 entity primary keys must not be empty"));
            }
            if let WasmEntityChange::Upsert { entity, .. } = change {
                match &entity.snapshot_content {
                    WasmGuestBytes::Inline(bytes) => {
                        inline_bytes = inline_bytes
                            .checked_add(bytes.len() as u64)
                            .ok_or_else(|| invalid_guest("v2 inline snapshot bytes overflowed"))?;
                    }
                    WasmGuestBytes::Output(range) => {
                        output_bytes = output_bytes
                            .checked_add(range.length)
                            .ok_or_else(|| invalid_guest("v2 output snapshot bytes overflowed"))?;
                        minimum_attachment_reads = minimum_attachment_reads
                            .checked_add(budget.minimum_attachment_reads(range.length))
                            .ok_or_else(|| invalid_guest("v2 attachment page count overflowed"))?;
                        references = references
                            .checked_add(1)
                            .ok_or_else(|| invalid_guest("v2 output references overflowed"))?;
                    }
                }
            }
        }
    }
    budget.preflight_cursor_page(
        inline_bytes,
        output_bytes,
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
    let mut minimum_attachment_reads = 0u64;
    let mut references = 0u32;
    for edit in &page.edits {
        match &edit.insert {
            WasmGuestBytes::Inline(bytes) => {
                inline_bytes = inline_bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid_guest("v2 inline edit bytes overflowed"))?;
            }
            WasmGuestBytes::Output(range) => {
                output_bytes = output_bytes
                    .checked_add(range.length)
                    .ok_or_else(|| invalid_guest("v2 output edit bytes overflowed"))?;
                minimum_attachment_reads = minimum_attachment_reads
                    .checked_add(budget.minimum_attachment_reads(range.length))
                    .ok_or_else(|| invalid_guest("v2 attachment page count overflowed"))?;
                references = references
                    .checked_add(1)
                    .ok_or_else(|| invalid_guest("v2 edit output references overflowed"))?;
            }
        }
    }
    budget.preflight_cursor_page(
        inline_bytes,
        output_bytes,
        references,
        minimum_attachment_reads,
    )
}

fn validate_guest_group_member_order(
    group: &WasmMergeGroup<WasmGuestBytes>,
) -> Result<(), LixError> {
    for pair in group.changes.windows(2) {
        if pair[0].key() >= pair[1].key() {
            return Err(invalid_guest(
                "v2 guest merge-group members must be strictly key-sorted and unique",
            ));
        }
    }
    Ok(())
}

async fn resolve_guest_bytes(
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmTransitionHandle,
    outputs: Option<WasmByteOutputsHandle>,
    bytes: WasmGuestBytes,
    budget: &mut OutputDrainBudget,
    counters: &mut WasmTransitionCounters,
) -> Result<Vec<u8>, LixError> {
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
                invalid_guest("v2 output range is missing its page-local output table")
            })?;
            read_output_range(actor, transition, outputs, range, budget, counters).await
        }
    }
}

async fn read_output_range(
    actor: &mut dyn WasmComponentV2Actor,
    transition: WasmTransitionHandle,
    outputs: WasmByteOutputsHandle,
    range: WasmOutputRange,
    budget: &mut OutputDrainBudget,
    counters: &mut WasmTransitionCounters,
) -> Result<Vec<u8>, LixError> {
    let end = range
        .offset
        .checked_add(range.length)
        .ok_or_else(|| invalid_guest("v2 output range overflowed"))?;
    let output_len = actor.output_len(transition, outputs, range.index).await?;
    if end > output_len {
        return Err(invalid_guest("v2 output range is out of bounds"));
    }
    let capacity = usize::try_from(range.length)
        .map_err(|_| invalid_guest("v2 output range does not fit this host"))?;
    let mut bytes = Vec::with_capacity(capacity);
    let mut offset = range.offset;
    while bytes.len() < capacity {
        let remaining = capacity - bytes.len();
        let page_limit = usize::try_from(budget.limits.max_page_bytes)
            .map_err(|_| invalid_guest("v2 output page limit does not fit this host"))?;
        let requested_len = remaining.min(page_limit);
        let requested = u32::try_from(requested_len)
            .map_err(|_| invalid_guest("v2 output read length exceeds the component ABI"))?;
        let chunk = actor
            .read_output(transition, outputs, range.index, offset, requested)
            .await?;
        if chunk.is_empty() || chunk.len() > requested_len {
            return Err(invalid_guest(
                "v2 output reads must return a non-empty bounded prefix before EOF",
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
            .ok_or_else(|| invalid_guest("v2 output read offset overflowed"))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
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
        references: u32,
        minimum_attachment_reads: u64,
    ) -> Result<(), LixError> {
        let prospective_bytes = inline_bytes
            .checked_add(output_bytes)
            .and_then(|bytes| self.total_bytes.checked_add(bytes))
            .ok_or_else(|| invalid_guest("v2 transition output byte count overflowed"))?;
        let prospective_refs = self
            .attachment_refs
            .checked_add(references)
            .ok_or_else(|| invalid_guest("v2 attachment reference count overflowed"))?;
        let minimum_pages = u64::from(self.pages)
            .checked_add(1)
            .and_then(|pages| pages.checked_add(minimum_attachment_reads))
            .ok_or_else(|| invalid_guest("v2 output page count overflowed"))?;
        if prospective_bytes > self.limits.max_total_bytes {
            return Err(invalid_guest(
                "v2 transition output exceeds max_total_bytes",
            ));
        }
        if prospective_refs > self.limits.max_attachment_refs {
            return Err(invalid_guest(
                "v2 transition output exceeds max_attachment_refs",
            ));
        }
        if minimum_pages > u64::from(self.limits.max_pages) {
            return Err(invalid_guest("v2 transition output exceeds max_pages"));
        }
        self.pages += 1;
        self.attachment_refs = prospective_refs;
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
            return Err(invalid_guest("v2 attachment read violates its page bound"));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid_guest("v2 transition page count overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(invalid_guest("v2 transition output exceeds max_pages"));
        }
        self.charge_bytes(bytes)
    }

    fn charge_bytes(&mut self, bytes: u64) -> Result<(), LixError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(bytes)
            .ok_or_else(|| invalid_guest("v2 transition byte count overflowed"))?;
        if self.total_bytes > self.limits.max_total_bytes {
            return Err(invalid_guest(
                "v2 transition output exceeds max_total_bytes",
            ));
        }
        Ok(())
    }
}

fn apply_resolved_output_splices(
    base: &[u8],
    edits: &[ResolvedOutputSplice],
) -> Result<Vec<u8>, LixError> {
    let mut capacity = base.len();
    let mut previous_start = None;
    let mut previous_end = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("v2 output splice offset does not fit this host"))?;
        let delete_len = usize::try_from(edit.delete_len)
            .map_err(|_| invalid_guest("v2 output splice deletion does not fit this host"))?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| invalid_guest("v2 output splice deletion range overflowed"))?;
        if previous_start == Some(start) || start < previous_end || end > base.len() {
            return Err(invalid_guest(
                "v2 output splices are not globally sorted, unique, and in bounds",
            ));
        }
        capacity = capacity
            .checked_sub(delete_len)
            .and_then(|capacity| capacity.checked_add(edit.insert.len()))
            .ok_or_else(|| invalid_guest("v2 rendered result length overflowed"))?;
        previous_start = Some(start);
        previous_end = end;
    }

    let mut bytes = Vec::with_capacity(capacity);
    let mut cursor = 0usize;
    for edit in edits {
        let start = usize::try_from(edit.offset)
            .map_err(|_| invalid_guest("v2 output splice offset does not fit this host"))?;
        let delete_len = usize::try_from(edit.delete_len)
            .map_err(|_| invalid_guest("v2 output splice deletion does not fit this host"))?;
        let end = start
            .checked_add(delete_len)
            .ok_or_else(|| invalid_guest("v2 output splice deletion range overflowed"))?;
        bytes.extend_from_slice(&base[cursor..start]);
        bytes.extend_from_slice(&edit.insert);
        cursor = end;
    }
    bytes.extend_from_slice(&base[cursor..]);
    Ok(bytes)
}

fn merge_counter_snapshots(
    local: WasmTransitionCounters,
    runtime: WasmTransitionCounters,
) -> WasmTransitionCounters {
    WasmTransitionCounters {
        source_read_calls: local.source_read_calls.max(runtime.source_read_calls),
        source_bytes_read: local.source_bytes_read.max(runtime.source_bytes_read),
        packet_pages: local.packet_pages.max(runtime.packet_pages),
        packet_records: local.packet_records.max(runtime.packet_records),
        attachment_reads: local.attachment_reads.max(runtime.attachment_reads),
        attachment_bytes_read: local
            .attachment_bytes_read
            .max(runtime.attachment_bytes_read),
        component_import_calls: local
            .component_import_calls
            .max(runtime.component_import_calls),
        component_boundary_bytes: local
            .component_boundary_bytes
            .max(runtime.component_boundary_bytes),
        guest_linear_memory_high_water_bytes: local
            .guest_linear_memory_high_water_bytes
            .max(runtime.guest_linear_memory_high_water_bytes),
        host_full_diff_bytes_compared: local
            .host_full_diff_bytes_compared
            .max(runtime.host_full_diff_bytes_compared),
        host_full_content_classification_bytes: local
            .host_full_content_classification_bytes
            .max(runtime.host_full_content_classification_bytes),
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
    use crate::wasm::{
        WasmChangeCursorHandle, WasmChangeEffect, WasmEditCursorHandle, WasmOpenEntitiesInput,
        WasmOpenFileInput, WasmOutputSplice,
    };

    fn key(id: &str) -> crate::wasm::WasmEntityKey {
        crate::wasm::WasmEntityKey {
            schema_key: "csv_row".to_owned(),
            entity_pk: vec![id.to_owned()],
        }
    }

    fn host_entity(id: &str) -> WasmHostEntity {
        WasmEntity {
            key: key(id),
            snapshot_content: WasmHostBytes::Inline(
                format!(r#"{{"cells":[],"id":"{id}","order_key":"a"}}"#).into_bytes(),
            ),
        }
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
        let after = b"abXYef";
        let provenance = RequestBlobSpliceProvenance {
            base_sha256: "a".repeat(64),
            result_sha256: "b".repeat(64),
            prefix_bytes: 2,
            suffix_bytes: 2,
            insert: b"XY".to_vec(),
        };
        let from_transport = build_file_update_splices(
            before,
            after,
            Some(&provenance),
            WasmTransitionLimits::default(),
        )
        .unwrap();
        assert!(from_transport.used_transport_provenance);
        assert_eq!(from_transport.full_diff_bytes_compared, 0);
        assert_eq!(
            from_transport.edits,
            vec![WasmInputSplice {
                offset: 2,
                delete_len: 2,
                insert: WasmInputBytes::Inline(b"XY".to_vec()),
            }]
        );

        let fallback =
            build_file_update_splices(before, after, None, WasmTransitionLimits::default())
                .unwrap();
        assert!(!fallback.used_transport_provenance);
        assert_eq!(fallback.full_diff_bytes_compared, 12);
        assert_eq!(fallback.edits, from_transport.edits);

        let lazy = build_file_update_splices(
            before,
            after,
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
    }

    #[test]
    fn utf8_splice_proof_checks_only_the_changed_boundary_window() {
        let after = b"alpha,BETA,omega";
        let valid = RequestBlobSpliceProvenance {
            base_sha256: "a".repeat(64),
            result_sha256: "b".repeat(64),
            prefix_bytes: 6,
            suffix_bytes: 6,
            insert: b"BETA".to_vec(),
        };
        assert!(transport_splice_preserves_utf8(after, &valid));

        let invalid_after = b"a\xa9z";
        let split_code_point = RequestBlobSpliceProvenance {
            base_sha256: "a".repeat(64),
            result_sha256: "b".repeat(64),
            prefix_bytes: 1,
            suffix_bytes: 2,
            insert: Vec::new(),
        };
        assert!(!transport_splice_preserves_utf8(
            invalid_after,
            &split_code_point
        ));

        let mut malformed = valid;
        malformed.suffix_bytes += 1;
        assert!(!transport_splice_preserves_utf8(after, &malformed));
    }

    #[test]
    fn canonical_csv_json_rejects_numbers_and_decoded_duplicate_keys() {
        let canonical =
            canonicalize_csv_snapshot(r#"{"z":"\n","a":[true,null,"é"],"slash":"/"}"#.as_bytes())
                .unwrap();
        assert_eq!(
            canonical,
            r#"{"a":[true,null,"é"],"slash":"/","z":"\u000A"}"#.as_bytes()
        );

        assert!(canonicalize_csv_snapshot(br#"{"nested":{"n":1}}"#).is_err());
        let duplicate = canonicalize_csv_snapshot(br#"{"a":"x","\u0061":"y"}"#)
            .expect_err("escaped and literal decoded keys are duplicates");
        assert!(duplicate.message.contains("duplicate"), "{duplicate:?}");
        assert!(canonicalize_csv_snapshot(br#"["not","an","object"]"#).is_err());
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
        assert_eq!(first_page.entities[0].key.entity_pk, ["a".to_owned()]);
        let second_page = source
            .next_page(WasmTransitionLimits::default().max_page_bytes)
            .unwrap()
            .unwrap();
        assert_eq!(second_page.entities[0].key.entity_pk, ["b".to_owned()]);
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
    fn vec_change_source_rejects_unsorted_members_and_pages_groups() {
        let unsorted = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![
                    WasmEntityChange::Delete(key("b")),
                    WasmEntityChange::Delete(key("a")),
                ],
            }],
        };
        assert!(VecEntityChangeSource::new(unsorted, WasmTransitionLimits::default()).is_err());

        let first = WasmMergeGroup {
            changes: vec![WasmEntityChange::Delete(key("a"))],
        };
        let first_page_bytes = encoded_merge_group_record_bytes(&first).unwrap() + 4;
        let changes = WasmEntityChanges {
            groups: vec![
                first,
                WasmMergeGroup {
                    changes: vec![WasmEntityChange::Delete(key("b"))],
                },
            ],
        };
        let mut source =
            VecEntityChangeSource::new(changes, WasmTransitionLimits::default()).unwrap();
        assert_eq!(
            source
                .next_page(u32::try_from(first_page_bytes).expect("test page size fits u32"))
                .unwrap()
                .unwrap()
                .groups
                .len(),
            1
        );
        assert_eq!(
            source
                .next_page(WasmTransitionLimits::default().max_page_bytes)
                .unwrap()
                .unwrap()
                .groups
                .len(),
            1
        );
        assert!(source.next_page(1).unwrap().is_none());
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
    impl WasmComponentV2Actor for FakeActor {
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
            _update: crate::wasm::WasmFileUpdate,
        ) -> Result<WasmFileTransition, LixError> {
            Err(unused())
        }

        async fn entities_changed(
            &mut self,
            _document: WasmDocumentHandle,
            _limits: WasmTransitionLimits,
            _update: crate::wasm::WasmEntityUpdate,
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
    async fn change_drain_validates_before_reading_and_canonicalizes_attachments() {
        let outputs = WasmByteOutputsHandle(7);
        let snapshot = br#"{"order_key":"a","id":"row","cells":[]}"#.to_vec();
        let page = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                groups: vec![WasmMergeGroup {
                    changes: vec![WasmEntityChange::Upsert {
                        entity: WasmEntity {
                            key: key("row"),
                            snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                                index: 0,
                                offset: 0,
                                length: snapshot.len() as u64,
                            }),
                        },
                        effect: WasmChangeEffect::Content,
                    }],
                }],
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
        let schemas = V2SchemaAllowlist::new(["csv_row".to_owned()]).unwrap();

        let drained = drain_file_transition_changes(
            &mut actor,
            transition,
            &schemas,
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        assert_eq!(drained.document, WasmDocumentHandle(2));
        let WasmEntityChange::Upsert { entity, .. } = &drained.changes.groups[0].changes[0] else {
            panic!("expected upsert")
        };
        let WasmHostBytes::Inline(snapshot) = &entity.snapshot_content else {
            panic!("resolved snapshots must be inline owned bytes")
        };
        assert_eq!(snapshot, br#"{"cells":[],"id":"row","order_key":"a"}"#);
        assert!(drained.counters.attachment_reads > 1);
        assert_eq!(drained.counters.source_read_calls, 2);
        assert!(actor.finished);
    }

    #[tokio::test]
    async fn host_validation_rejection_discards_transition_and_allows_retry() {
        let outputs = WasmByteOutputsHandle(7);
        let page = WasmChangePage {
            format_version: PACKET_FORMAT_V1,
            changes: WasmEntityChanges {
                groups: vec![WasmMergeGroup {
                    changes: vec![WasmEntityChange::Upsert {
                        entity: WasmEntity {
                            key: crate::wasm::WasmEntityKey {
                                schema_key: "not_allowed".to_owned(),
                                entity_pk: vec!["row".to_owned()],
                            },
                            snapshot_content: WasmGuestBytes::Output(WasmOutputRange {
                                index: 0,
                                offset: 0,
                                length: 2,
                            }),
                        },
                        effect: WasmChangeEffect::Content,
                    }],
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
            &V2SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
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
                groups: vec![WasmMergeGroup {
                    changes: vec![WasmEntityChange::Delete(key("row"))],
                }],
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
            &V2SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
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
                    groups: vec![WasmMergeGroup {
                        changes: vec![WasmEntityChange::Delete(crate::wasm::WasmEntityKey {
                            schema_key: "not_allowed".to_owned(),
                            entity_pk: vec!["row".to_owned()],
                        })],
                    }],
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
            &V2SchemaAllowlist::new(["csv_row".to_owned()]).unwrap(),
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
                        insert: WasmGuestBytes::Inline(b"XY".to_vec()),
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
            Some(Arc::from(&b"aXYdeZ"[..])),
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
    async fn edit_drain_reuses_exact_expected_blob_after_local_delta_proof() {
        let mut actor = FakeActor {
            edit_pages: [WasmEditPage {
                edits: vec![WasmOutputSplice {
                    offset: 1,
                    delete_len: 4,
                    insert: WasmGuestBytes::Inline(b"bXYe".to_vec()),
                }],
                outputs: None,
            }]
            .into(),
            ..FakeActor::default()
        };
        let expected: Arc<[u8]> = Arc::from(&b"abXYef"[..]);
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
            Some(Arc::clone(&expected)),
            Some(&input),
            WasmTransitionLimits::default(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&drained.bytes, &expected));
        assert!(actor.finished);
    }
}
