use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use lix_engine::wasm::v2::{
    PACKET_FORMAT_V1, WasmByteOutputsHandle, WasmByteSource, WasmChangeCursorHandle,
    WasmChangeDrainValidator, WasmChangeEffect, WasmChangePage, WasmComponentV2Actor,
    WasmComponentV2Factory, WasmDocumentHandle, WasmEditCursorHandle, WasmEditDrainValidator,
    WasmEditPage, WasmEntity, WasmEntityChange, WasmEntityChangeSource, WasmEntityChanges,
    WasmEntityKey, WasmEntityPage, WasmEntitySource, WasmEntityTransition, WasmEntityUpdate,
    WasmFileDescriptor, WasmFileTransition, WasmFileUpdate, WasmGuestBytes, WasmHostBytes,
    WasmIdNamespace, WasmInputBytes, WasmOpenEntitiesInput, WasmOpenFileInput, WasmOutputRange,
    WasmOutputSplice, WasmSourceSlice, WasmTransitionCounters, WasmTransitionHandle,
    WasmTransitionLimits,
};
use wasmtime::component::{Resource, ResourceAny};

use super::*;

struct TransitionBudgetState {
    limits: WasmTransitionLimits,
    started: Instant,
    counters: WasmTransitionCounters,
    pages: u32,
    bytes: u64,
    attachment_refs: u32,
    finished: bool,
}

impl TransitionBudgetState {
    fn new(limits: WasmTransitionLimits) -> Result<Self, LixError> {
        Ok(Self {
            limits: limits.validate()?,
            started: Instant::now(),
            counters: WasmTransitionCounters::default(),
            pages: 0,
            bytes: 0,
            attachment_refs: 0,
            finished: false,
        })
    }

    fn remaining_nanoseconds(&self) -> u64 {
        self.limits
            .total_deadline_nanoseconds
            .saturating_sub(self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64)
    }

    fn check_active(&self) -> Result<(), LixError> {
        if self.finished {
            return Err(v2_invalid_plugin(
                "v2 transition budget was used after completion",
            ));
        }
        if self.remaining_nanoseconds() == 0 {
            return Err(v2_deadline("v2 transition deadline elapsed"));
        }
        Ok(())
    }

    fn charge_boundary(&mut self, bytes: u64) -> Result<(), LixError> {
        self.check_active()?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| v2_limit("v2 transition byte counter overflowed"))?;
        if self.bytes > self.limits.max_total_bytes {
            return Err(v2_limit("v2 transition total byte limit exceeded"));
        }
        self.counters.component_boundary_bytes = self
            .counters
            .component_boundary_bytes
            .checked_add(bytes)
            .ok_or_else(|| v2_limit("v2 component boundary byte counter overflowed"))?;
        Ok(())
    }

    fn charge_page(&mut self, bytes: u64) -> Result<(), LixError> {
        if bytes > u64::from(self.limits.max_page_bytes) {
            return Err(v2_limit("v2 page exceeds max-page-bytes"));
        }
        self.pages = self
            .pages
            .checked_add(1)
            .ok_or_else(|| v2_limit("v2 page counter overflowed"))?;
        if self.pages > self.limits.max_pages {
            return Err(v2_limit("v2 transition page limit exceeded"));
        }
        self.charge_boundary(bytes)
    }

    fn charge_attachment_refs(&mut self, count: u32) -> Result<(), LixError> {
        self.check_active()?;
        self.attachment_refs = self
            .attachment_refs
            .checked_add(count)
            .ok_or_else(|| v2_limit("v2 attachment reference counter overflowed"))?;
        if self.attachment_refs > self.limits.max_attachment_refs {
            return Err(v2_limit(
                "v2 transition attachment reference limit exceeded",
            ));
        }
        Ok(())
    }
}

type SharedBudget = Arc<Mutex<TransitionBudgetState>>;

pub struct TransitionBudgetResource {
    state: SharedBudget,
}

pub struct ByteSourceResource {
    source: Arc<dyn WasmByteSource>,
    base_offset: u64,
    len: u64,
    budget: Weak<Mutex<TransitionBudgetState>>,
}

pub struct ByteSourcesResource {
    sources: Vec<WasmSourceSlice>,
    budget: Weak<Mutex<TransitionBudgetState>>,
}

enum PacketSourceValue {
    Entities(Box<dyn WasmEntitySource>),
    Changes(Box<dyn WasmEntityChangeSource>),
}

pub struct PacketSourceResource {
    source: PacketSourceValue,
    budget: Weak<Mutex<TransitionBudgetState>>,
    eof: bool,
    previous_entity_key: Option<WasmEntityKey>,
    seen_change_keys: BTreeSet<WasmEntityKey>,
}

struct EncodedPacketPage {
    record_count: u32,
    payload: Vec<u8>,
    attachments: Vec<WasmSourceSlice>,
}

struct WasmtimeV2Factory {
    shared: Arc<WasmtimeSharedRuntime>,
    component: Component,
    linker: Arc<Linker<WasiHostState>>,
    limits: WasmLimits,
    profile: CompileProfile,
}

struct ActiveTransition {
    budget_rep: u32,
    budget: SharedBudget,
    /// Warm transitions run against a caller-created document fork. A clean
    /// plugin rejection must discard that working fork without touching the
    /// actor's separately held accepted document.
    input_document: Option<u64>,
    /// Document returned by the guest before its cursor has been completely
    /// drained and validated. It is prospective until `finish_transition`.
    successor_document: Option<u64>,
}

struct ChangeCursorState {
    resource: ResourceAny,
    transition: u64,
    validator: WasmChangeDrainValidator,
    eof: bool,
}

struct EditCursorState {
    resource: ResourceAny,
    transition: u64,
    validator: WasmEditDrainValidator,
    eof: bool,
}

struct OutputState {
    resource: ResourceAny,
    transition: u64,
    lengths: HashMap<u32, u64>,
}

struct WasmtimeV2Actor {
    store: Option<Store<WasiHostState>>,
    guest: bindings::exports::lix::plugin::api::Guest,
    limits: WasmLimits,
    _timeout_ticker: TimeoutTickerLease,
    next_handle: u64,
    documents: HashMap<u64, ResourceAny>,
    change_cursors: HashMap<u64, ChangeCursorState>,
    edit_cursors: HashMap<u64, EditCursorState>,
    outputs: HashMap<u64, OutputState>,
    transitions: HashMap<u64, ActiveTransition>,
}

pub(super) mod bindings {
    wasmtime::component::bindgen!({
        path: "../engine/wit/v2",
        world: "plugin",
        with: {
            "lix:plugin/host.transition-budget": super::TransitionBudgetResource,
            "lix:plugin/host.byte-source": super::ByteSourceResource,
            "lix:plugin/host.byte-sources": super::ByteSourcesResource,
            "lix:plugin/host.packet-source": super::PacketSourceResource,
        },
    });
}

fn encode_entity_packet(
    page: WasmEntityPage,
    max_bytes: u32,
    limits: WasmTransitionLimits,
    previous_key: &mut Option<WasmEntityKey>,
) -> Result<EncodedPacketPage, LixError> {
    if page.entities.is_empty() {
        return Err(v2_invalid_plugin("v2 entity source returned an empty page"));
    }
    let mut records = Vec::with_capacity(page.entities.len());
    let mut attachments = Vec::new();
    for entity in page.entities {
        if previous_key.as_ref().is_some_and(|key| key >= &entity.key) {
            return Err(v2_invalid_plugin(
                "v2 entity source keys are not globally strictly increasing",
            ));
        }
        let mut record = Vec::new();
        encode_entity_key(&entity.key, &mut record)?;
        encode_host_blob(entity.snapshot_content, &mut record, &mut attachments)?;
        validate_record_len(record.len(), limits)?;
        *previous_key = Some(entity.key);
        records.push(record);
    }
    frame_records(records, attachments, max_bytes, limits)
}

fn encode_change_packet(
    page: WasmEntityChanges<WasmHostBytes>,
    max_bytes: u32,
    limits: WasmTransitionLimits,
    seen_keys: &mut BTreeSet<WasmEntityKey>,
) -> Result<EncodedPacketPage, LixError> {
    if page.groups.is_empty() {
        return Err(v2_invalid_plugin("v2 change source returned an empty page"));
    }
    page.validate()?;
    let mut records = Vec::with_capacity(page.groups.len());
    let mut attachments = Vec::new();
    for group in page.groups {
        if group.changes.is_empty() {
            return Err(v2_invalid_plugin("v2 merge group must not be empty"));
        }
        let mut record = Vec::new();
        push_u32(
            &mut record,
            checked_u32(group.changes.len(), "merge group member count")?,
        );
        let mut previous_key: Option<&WasmEntityKey> = None;
        for change in &group.changes {
            let key = change.key();
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(v2_invalid_plugin(
                    "v2 merge-group keys are not strictly increasing",
                ));
            }
            if !seen_keys.insert(key.clone()) {
                return Err(v2_invalid_plugin(
                    "v2 change source repeated an entity key across its transition",
                ));
            }
            match change {
                WasmEntityChange::Upsert { entity, effect } => {
                    record.push(0);
                    encode_entity_key(&entity.key, &mut record)?;
                    record.push(match effect {
                        WasmChangeEffect::Content => 0,
                        WasmChangeEffect::FormatOnly => 1,
                    });
                    encode_host_blob(
                        entity.snapshot_content.clone(),
                        &mut record,
                        &mut attachments,
                    )?;
                }
                WasmEntityChange::Delete(key) => {
                    record.push(1);
                    encode_entity_key(key, &mut record)?;
                }
            }
            previous_key = Some(key);
        }
        validate_record_len(record.len(), limits)?;
        records.push(record);
    }
    frame_records(records, attachments, max_bytes, limits)
}

fn frame_records(
    records: Vec<Vec<u8>>,
    attachments: Vec<WasmSourceSlice>,
    max_bytes: u32,
    limits: WasmTransitionLimits,
) -> Result<EncodedPacketPage, LixError> {
    if max_bytes == 0 || max_bytes > limits.max_page_bytes {
        return Err(v2_limit(
            "v2 packet max-bytes is outside its transition limit",
        ));
    }
    let mut payload = Vec::new();
    for record in &records {
        push_u32(
            &mut payload,
            checked_u32(record.len(), "packet record length")?,
        );
        payload.extend_from_slice(record);
    }
    if payload.len() > max_bytes as usize || payload.len() > limits.max_page_bytes as usize {
        return Err(v2_limit("v2 packet source page exceeds max-bytes"));
    }
    Ok(EncodedPacketPage {
        record_count: checked_u32(records.len(), "packet record count")?,
        payload,
        attachments,
    })
}

fn validate_record_len(len: usize, limits: WasmTransitionLimits) -> Result<(), LixError> {
    if len > limits.max_record_bytes as usize {
        return Err(v2_record_too_large(len as u64));
    }
    Ok(())
}

fn encode_entity_key(key: &WasmEntityKey, output: &mut Vec<u8>) -> Result<(), LixError> {
    encode_text(&key.schema_key, output)?;
    push_u32(
        output,
        checked_u32(key.entity_pk.len(), "entity primary-key component count")?,
    );
    for component in &key.entity_pk {
        encode_text(component, output)?;
    }
    Ok(())
}

fn encode_text(value: &str, output: &mut Vec<u8>) -> Result<(), LixError> {
    push_u32(output, checked_u32(value.len(), "packet text length")?);
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_host_blob(
    bytes: WasmHostBytes,
    output: &mut Vec<u8>,
    attachments: &mut Vec<WasmSourceSlice>,
) -> Result<(), LixError> {
    match bytes {
        WasmHostBytes::Inline(bytes) => {
            validate_number_free_snapshot(&bytes)?;
            output.push(0);
            push_u32(output, checked_u32(bytes.len(), "inline snapshot length")?);
            output.extend_from_slice(&bytes);
        }
        WasmHostBytes::Source(slice) => {
            slice.validate()?;
            output.push(1);
            push_u32(
                output,
                checked_u32(attachments.len(), "packet attachment index")?,
            );
            push_u64(output, 0);
            push_u64(output, slice.range.length);
            attachments.push(slice);
        }
    }
    Ok(())
}

struct DecodedChangePacket {
    changes: WasmEntityChanges<WasmGuestBytes>,
    output_ranges: Vec<WasmOutputRange>,
}

fn decode_change_packet(
    record_count: u32,
    payload: &[u8],
    limits: WasmTransitionLimits,
) -> Result<DecodedChangePacket, LixError> {
    if record_count == 0 {
        return Err(v2_invalid_plugin(
            "v2 guest returned a zero-record change page",
        ));
    }
    if payload.len() > limits.max_page_bytes as usize {
        return Err(v2_limit("v2 guest change payload exceeds max-page-bytes"));
    }
    let mut framed = PacketReader::new(payload);
    let mut groups = Vec::with_capacity(record_count as usize);
    let mut output_ranges = Vec::new();
    for _ in 0..record_count {
        let record_len = framed.read_u32()? as usize;
        if record_len > limits.max_record_bytes as usize {
            return Err(v2_record_too_large(record_len as u64));
        }
        let record_bytes = framed.read_exact(record_len)?;
        let mut record = PacketReader::new(record_bytes);
        let member_count = record.read_u32()?;
        if member_count == 0 {
            return Err(v2_invalid_plugin("v2 guest returned an empty merge group"));
        }
        let mut changes = Vec::with_capacity(member_count as usize);
        let mut previous_key: Option<WasmEntityKey> = None;
        for _ in 0..member_count {
            let change_tag = record.read_u8()?;
            let key = decode_entity_key(&mut record)?;
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(v2_invalid_plugin(
                    "v2 guest merge-group keys are not strictly increasing",
                ));
            }
            let change = match change_tag {
                0 => {
                    let effect = match record.read_u8()? {
                        0 => WasmChangeEffect::Content,
                        1 => WasmChangeEffect::FormatOnly,
                        _ => return Err(v2_invalid_plugin("unknown v2 change effect tag")),
                    };
                    let snapshot_content = decode_guest_blob(&mut record, &mut output_ranges)?;
                    WasmEntityChange::Upsert {
                        entity: WasmEntity {
                            key: key.clone(),
                            snapshot_content,
                        },
                        effect,
                    }
                }
                1 => WasmEntityChange::Delete(key.clone()),
                _ => return Err(v2_invalid_plugin("unknown v2 change tag")),
            };
            previous_key = Some(key);
            changes.push(change);
        }
        record.finish()?;
        groups.push(lix_engine::wasm::v2::WasmMergeGroup { changes });
    }
    framed.finish()?;
    let changes = WasmEntityChanges { groups };
    changes.validate()?;
    Ok(DecodedChangePacket {
        changes,
        output_ranges,
    })
}

fn decode_entity_key(reader: &mut PacketReader<'_>) -> Result<WasmEntityKey, LixError> {
    let schema_key = reader.read_text()?;
    let pk_count = reader.read_u32()?;
    let remaining = reader.remaining();
    if pk_count as usize > remaining / 4 {
        return Err(v2_invalid_plugin(
            "v2 entity primary-key component count exceeds packet bounds",
        ));
    }
    let mut entity_pk = Vec::with_capacity(pk_count as usize);
    for _ in 0..pk_count {
        entity_pk.push(reader.read_text()?);
    }
    Ok(WasmEntityKey {
        schema_key,
        entity_pk,
    })
}

fn decode_guest_blob(
    reader: &mut PacketReader<'_>,
    ranges: &mut Vec<WasmOutputRange>,
) -> Result<WasmGuestBytes, LixError> {
    match reader.read_u8()? {
        0 => {
            let length = reader.read_u32()? as usize;
            let bytes = reader.read_exact(length)?.to_vec();
            validate_number_free_snapshot(&bytes)?;
            Ok(WasmGuestBytes::Inline(bytes))
        }
        1 => {
            let range = WasmOutputRange {
                index: reader.read_u32()?,
                offset: reader.read_u64()?,
                length: reader.read_u64()?,
            };
            range
                .offset
                .checked_add(range.length)
                .ok_or_else(|| v2_invalid_plugin("v2 output attachment range overflowed"))?;
            ranges.push(range);
            Ok(WasmGuestBytes::Output(range))
        }
        _ => Err(v2_invalid_plugin("unknown v2 blob-reference tag")),
    }
}

struct PacketReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PacketReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| v2_invalid_plugin("v2 packet range overflowed"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| v2_invalid_plugin("truncated v2 packet"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, LixError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, LixError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| v2_invalid_plugin("invalid v2 u32 field"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, LixError> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| v2_invalid_plugin("invalid v2 u64 field"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_text(&mut self) -> Result<String, LixError> {
        let length = self.read_u32()? as usize;
        let bytes = self.read_exact(length)?;
        String::from_utf8(bytes.to_vec())
            .map_err(|_| v2_invalid_plugin("v2 packet text is not valid UTF-8"))
    }

    fn finish(&self) -> Result<(), LixError> {
        if self.offset != self.bytes.len() {
            return Err(v2_invalid_plugin("v2 packet contains trailing bytes"));
        }
        Ok(())
    }
}

fn checked_u32(value: usize, name: &str) -> Result<u32, LixError> {
    u32::try_from(value).map_err(|_| v2_limit(format!("v2 {name} exceeds u32")))
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

/// Validates the production packet-v1 number-free durable profile without
/// first lowering snapshots through `serde_json::Value` (which would silently
/// lose duplicate keys and packet-v1's arbitrary-precision number semantics).
fn validate_number_free_snapshot(bytes: &[u8]) -> Result<(), LixError> {
    let mut parser = NumberFreeJsonParser { bytes, offset: 0 };
    parser.skip_whitespace();
    parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.offset != bytes.len() {
        return Err(v2_invalid_plugin("snapshot JSON contains trailing bytes"));
    }
    Ok(())
}

struct NumberFreeJsonParser<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl NumberFreeJsonParser<'_> {
    const MAX_DEPTH: usize = 256;

    fn parse_value(&mut self, depth: usize) -> Result<(), LixError> {
        if depth > Self::MAX_DEPTH {
            return Err(v2_limit("snapshot JSON nesting exceeds the v2 limit"));
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'n') => self.keyword(b"null"),
            Some(b't') => self.keyword(b"true"),
            Some(b'f') => self.keyword(b"false"),
            Some(b'\"') => self.parse_string().map(drop),
            Some(b'[') => self.parse_array(depth + 1),
            Some(b'{') => self.parse_object(depth + 1),
            Some(b'-' | b'0'..=b'9') => Err(v2_invalid_plugin(
                "packet-v1 numeric snapshots are not durable in this production slice",
            )),
            Some(_) => Err(v2_invalid_plugin("invalid snapshot JSON value")),
            None => Err(v2_invalid_plugin("truncated snapshot JSON value")),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<(), LixError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        if self.consume(b']') {
            return Ok(());
        }
        loop {
            self.parse_value(depth)?;
            self.skip_whitespace();
            if self.consume(b']') {
                return Ok(());
            }
            self.expect(b',')?;
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<(), LixError> {
        self.expect(b'{')?;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Ok(());
        }
        let mut keys = BTreeSet::new();
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if !keys.insert(key) {
                return Err(v2_invalid_plugin(
                    "snapshot JSON contains a duplicate decoded object key",
                ));
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.parse_value(depth)?;
            self.skip_whitespace();
            if self.consume(b'}') {
                return Ok(());
            }
            self.expect(b',')?;
        }
    }

    fn parse_string(&mut self) -> Result<String, LixError> {
        self.expect(b'\"')?;
        let mut value = String::new();
        let mut raw_start = self.offset;
        loop {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or_else(|| v2_invalid_plugin("unterminated snapshot JSON string"))?;
            match byte {
                b'\"' => {
                    self.push_raw_string_segment(raw_start, self.offset, &mut value)?;
                    self.offset += 1;
                    return Ok(value);
                }
                b'\\' => {
                    self.push_raw_string_segment(raw_start, self.offset, &mut value)?;
                    self.offset += 1;
                    let escape = self.take()?;
                    match escape {
                        b'\"' => value.push('\"'),
                        b'\\' => value.push('\\'),
                        b'/' => value.push('/'),
                        b'b' => value.push('\u{0008}'),
                        b'f' => value.push('\u{000c}'),
                        b'n' => value.push('\n'),
                        b'r' => value.push('\r'),
                        b't' => value.push('\t'),
                        b'u' => value.push(self.parse_unicode_escape()?),
                        _ => return Err(v2_invalid_plugin("invalid snapshot JSON escape")),
                    }
                    raw_start = self.offset;
                }
                0x00..=0x1f => {
                    return Err(v2_invalid_plugin(
                        "snapshot JSON string contains an unescaped control byte",
                    ));
                }
                _ => self.offset += 1,
            }
        }
    }

    fn parse_unicode_escape(&mut self) -> Result<char, LixError> {
        let first = self.read_hex_quad()?;
        let scalar = if (0xd800..=0xdbff).contains(&first) {
            if self.take()? != b'\\' || self.take()? != b'u' {
                return Err(v2_invalid_plugin(
                    "snapshot JSON high surrogate is not followed by a low surrogate",
                ));
            }
            let second = self.read_hex_quad()?;
            if !(0xdc00..=0xdfff).contains(&second) {
                return Err(v2_invalid_plugin("invalid snapshot JSON low surrogate"));
            }
            0x1_0000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
        } else if (0xdc00..=0xdfff).contains(&first) {
            return Err(v2_invalid_plugin("unpaired snapshot JSON low surrogate"));
        } else {
            u32::from(first)
        };
        char::from_u32(scalar).ok_or_else(|| v2_invalid_plugin("invalid snapshot JSON scalar"))
    }

    fn read_hex_quad(&mut self) -> Result<u16, LixError> {
        let mut value = 0u16;
        for _ in 0..4 {
            let digit = match self.take()? {
                byte @ b'0'..=b'9' => u16::from(byte - b'0'),
                byte @ b'a'..=b'f' => u16::from(byte - b'a' + 10),
                byte @ b'A'..=b'F' => u16::from(byte - b'A' + 10),
                _ => return Err(v2_invalid_plugin("invalid snapshot JSON unicode escape")),
            };
            value = value * 16 + digit;
        }
        Ok(value)
    }

    fn push_raw_string_segment(
        &self,
        start: usize,
        end: usize,
        output: &mut String,
    ) -> Result<(), LixError> {
        let segment = std::str::from_utf8(&self.bytes[start..end])
            .map_err(|_| v2_invalid_plugin("snapshot JSON string is not valid UTF-8"))?;
        output.push_str(segment);
        Ok(())
    }

    fn keyword(&mut self, keyword: &[u8]) -> Result<(), LixError> {
        let end = self
            .offset
            .checked_add(keyword.len())
            .ok_or_else(|| v2_invalid_plugin("snapshot JSON keyword overflowed"))?;
        if self.bytes.get(self.offset..end) != Some(keyword) {
            return Err(v2_invalid_plugin("invalid snapshot JSON keyword"));
        }
        self.offset = end;
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.offset += 1;
        }
    }

    fn expect(&mut self, expected: u8) -> Result<(), LixError> {
        if self.consume(expected) {
            Ok(())
        } else {
            Err(v2_invalid_plugin("unexpected snapshot JSON token"))
        }
    }

    fn consume(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    fn take(&mut self) -> Result<u8, LixError> {
        let byte = self
            .peek()
            .ok_or_else(|| v2_invalid_plugin("truncated snapshot JSON escape"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }
}

fn v2_invalid_plugin(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn v2_limit(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PARAM, message)
}

fn v2_deadline(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message)
}

fn v2_record_too_large(size: u64) -> LixError {
    v2_limit(format!("v2 record is too large: {size} bytes"))
}

pub(super) async fn compile_component(
    runtime: &WasmtimePluginRuntime,
    bytes: Vec<u8>,
    limits: WasmLimits,
) -> Result<Arc<dyn WasmComponentV2Factory>, LixError> {
    if limits.max_memory_bytes == 0 {
        return Err(v2_limit("v2 component memory limit must be positive"));
    }
    // Every v2 transition has a wall-clock deadline even if the caller did
    // not add a stricter component-wide timeout, so v2 always uses an epoch-
    // interruptible engine profile.
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
                .map_err(|error| wasm_runtime_error("failed to compile v2 plugin component", error))
        })
        .await?;
    let linker = Arc::new(create_linker(engine)?);
    Ok(Arc::new(WasmtimeV2Factory {
        shared: runtime.shared.clone(),
        component,
        linker,
        limits,
        profile,
    }))
}

#[async_trait]
impl WasmComponentV2Factory for WasmtimeV2Factory {
    async fn instantiate_actor(&self) -> Result<Box<dyn WasmComponentV2Actor>, LixError> {
        let engine = self.shared.engine(self.profile);
        let timeout_ticker = self
            .shared
            .timeout_ticker(self.profile)?
            .ok_or_else(|| v2_invalid_plugin("v2 actor requires an epoch timeout ticker"))?;
        let mut store = create_store(engine, self.limits)?;
        // `create_store` installs this only for an explicit component timeout;
        // v2's mandatory transition deadline needs the same trapping behavior.
        store.epoch_deadline_trap();
        reset_standalone_call_limits(&mut store, self.limits)?;
        let bindings = bindings::Plugin::instantiate(&mut store, &self.component, &self.linker)
            .map_err(|error| wasm_runtime_error("failed to instantiate v2 plugin actor", error))?;
        let guest = bindings.lix_plugin_api().clone();
        Ok(Box::new(WasmtimeV2Actor {
            store: Some(store),
            guest,
            limits: self.limits,
            _timeout_ticker: timeout_ticker,
            next_handle: 1,
            documents: HashMap::new(),
            change_cursors: HashMap::new(),
            edit_cursors: HashMap::new(),
            outputs: HashMap::new(),
            transitions: HashMap::new(),
        }))
    }
}

pub(super) fn create_linker(engine: &Engine) -> Result<Linker<WasiHostState>, LixError> {
    let mut linker = Linker::<WasiHostState>::new(engine);
    add_to_linker_sync(&mut linker)
        .map_err(|error| wasm_runtime_error("failed to configure v2 WASI linker", error))?;
    bindings::Plugin::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |state| {
        state
    })
    .map_err(|error| wasm_runtime_error("failed to configure v2 plugin linker", error))?;
    Ok(linker)
}

impl bindings::lix::plugin::host::HostTransitionBudget for WasiHostState {
    fn limits(
        &mut self,
        resource: Resource<TransitionBudgetResource>,
    ) -> bindings::lix::plugin::host::TransitionLimits {
        let state = self
            .table
            .get(&resource)
            .expect("v2 transition budget resource must be live")
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.limits.into()
    }

    fn remaining_nanoseconds(&mut self, resource: Resource<TransitionBudgetResource>) -> u64 {
        self.table
            .get(&resource)
            .expect("v2 transition budget resource must be live")
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remaining_nanoseconds()
    }

    fn drop(&mut self, resource: Resource<TransitionBudgetResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostByteSource for WasiHostState {
    fn len(&mut self, resource: Resource<ByteSourceResource>) -> u64 {
        self.table
            .get(&resource)
            .expect("v2 byte source resource must be live")
            .len
    }

    fn read(
        &mut self,
        resource: Resource<ByteSourceResource>,
        budget: Resource<TransitionBudgetResource>,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, bindings::lix::plugin::host::SourceError> {
        let (source, base_offset, source_len, expected_budget) = {
            let source = self.table.get(&resource).map_err(table_source_error)?;
            (
                source.source.clone(),
                source.base_offset,
                source.len,
                source.budget.clone(),
            )
        };
        let budget = self.budget_state(&budget)?;
        ensure_budget(&expected_budget, &budget)?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)?;
        if end > source_len {
            return Err(bindings::lix::plugin::host::SourceError::InvalidRange);
        }
        {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.check_active().map_err(lix_source_error)?;
            if length > state.limits.max_page_bytes {
                return Err(bindings::lix::plugin::host::SourceError::LimitExceeded(
                    "byte-source read exceeds max-page-bytes".to_owned(),
                ));
            }
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        let absolute_offset = base_offset
            .checked_add(offset)
            .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)?;
        let bytes = source
            .read(absolute_offset, length)
            .map_err(lix_source_error)?;
        if bytes.len() != length as usize {
            return Err(bindings::lix::plugin::host::SourceError::Unavailable(
                "byte source returned a short read".to_owned(),
            ));
        }
        if !bytes.is_empty() {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .charge_page(bytes.len() as u64)
                .map_err(lix_source_error)?;
            state.counters.source_read_calls = state.counters.source_read_calls.saturating_add(1);
            state.counters.source_bytes_read = state
                .counters
                .source_bytes_read
                .saturating_add(bytes.len() as u64);
        }
        Ok(bytes)
    }

    fn drop(&mut self, resource: Resource<ByteSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostByteSources for WasiHostState {
    fn len(
        &mut self,
        resource: Resource<ByteSourcesResource>,
        index: u32,
    ) -> Result<u64, bindings::lix::plugin::host::SourceError> {
        self.table
            .get(&resource)
            .map_err(table_source_error)?
            .sources
            .get(index as usize)
            .map(|source| source.range.length)
            .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)
    }

    fn read(
        &mut self,
        resource: Resource<ByteSourcesResource>,
        budget: Resource<TransitionBudgetResource>,
        index: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, bindings::lix::plugin::host::SourceError> {
        let (slice, expected_budget) = {
            let table = self.table.get(&resource).map_err(table_source_error)?;
            let slice = table
                .sources
                .get(index as usize)
                .cloned()
                .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)?;
            (slice, table.budget.clone())
        };
        let budget = self.budget_state(&budget)?;
        ensure_budget(&expected_budget, &budget)?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)?;
        if end > slice.range.length {
            return Err(bindings::lix::plugin::host::SourceError::InvalidRange);
        }
        {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.check_active().map_err(lix_source_error)?;
            if length > state.limits.max_page_bytes {
                return Err(bindings::lix::plugin::host::SourceError::LimitExceeded(
                    "byte-sources read exceeds max-page-bytes".to_owned(),
                ));
            }
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        let absolute_offset = slice
            .range
            .offset
            .checked_add(offset)
            .ok_or(bindings::lix::plugin::host::SourceError::InvalidRange)?;
        let bytes = slice
            .source
            .read(absolute_offset, length)
            .map_err(lix_source_error)?;
        if bytes.len() != length as usize {
            return Err(bindings::lix::plugin::host::SourceError::Unavailable(
                "attachment source returned a short read".to_owned(),
            ));
        }
        if !bytes.is_empty() {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .charge_page(bytes.len() as u64)
                .map_err(lix_source_error)?;
            state.counters.attachment_reads = state.counters.attachment_reads.saturating_add(1);
            state.counters.attachment_bytes_read = state
                .counters
                .attachment_bytes_read
                .saturating_add(bytes.len() as u64);
        }
        Ok(bytes)
    }

    fn drop(&mut self, resource: Resource<ByteSourcesResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::HostPacketSource for WasiHostState {
    fn next(
        &mut self,
        resource: Resource<PacketSourceResource>,
        budget_resource: Resource<TransitionBudgetResource>,
        max_bytes: u32,
    ) -> Result<
        Option<bindings::lix::plugin::host::PacketPage>,
        bindings::lix::plugin::host::SourceError,
    > {
        let budget = self.budget_state(&budget_resource)?;
        let limits = {
            let state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.check_active().map_err(lix_source_error)?;
            state.limits
        };
        let encoded = {
            let source = self.table.get_mut(&resource).map_err(table_source_error)?;
            ensure_budget(&source.budget, &budget)?;
            if source.eof {
                return Ok(None);
            }
            let page = match &mut source.source {
                PacketSourceValue::Entities(entities) => {
                    match entities.next_page(max_bytes).map_err(lix_source_error)? {
                        Some(page) => Some(encode_entity_packet(
                            page,
                            max_bytes,
                            limits,
                            &mut source.previous_entity_key,
                        )),
                        None => None,
                    }
                }
                PacketSourceValue::Changes(changes) => {
                    match changes.next_page(max_bytes).map_err(lix_source_error)? {
                        Some(page) => Some(encode_change_packet(
                            page,
                            max_bytes,
                            limits,
                            &mut source.seen_change_keys,
                        )),
                        None => None,
                    }
                }
            };
            match page {
                Some(page) => page.map_err(lix_source_error)?,
                None => {
                    source.eof = true;
                    return Ok(None);
                }
            }
        };

        let attachment_count =
            checked_u32(encoded.attachments.len(), "attachment count").map_err(lix_source_error)?;
        {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .charge_attachment_refs(attachment_count)
                .map_err(lix_source_error)?;
            state
                .charge_page(encoded.payload.len() as u64)
                .map_err(lix_source_error)?;
            state.counters.packet_pages = state.counters.packet_pages.saturating_add(1);
            state.counters.packet_records = state
                .counters
                .packet_records
                .saturating_add(u64::from(encoded.record_count));
            state.counters.component_import_calls =
                state.counters.component_import_calls.saturating_add(1);
        }
        let attachments = if encoded.attachments.is_empty() {
            None
        } else {
            Some(
                self.table
                    .push(ByteSourcesResource {
                        sources: encoded.attachments,
                        budget: Arc::downgrade(&budget),
                    })
                    .map_err(table_source_error)?,
            )
        };
        Ok(Some(bindings::lix::plugin::host::PacketPage {
            format_version: PACKET_FORMAT_V1,
            record_count: encoded.record_count,
            payload: encoded.payload,
            attachments,
        }))
    }

    fn drop(&mut self, resource: Resource<PacketSourceResource>) -> wasmtime::Result<()> {
        self.table.delete(resource)?;
        Ok(())
    }
}

impl bindings::lix::plugin::host::Host for WasiHostState {}

impl WasiHostState {
    fn budget_state(
        &self,
        resource: &Resource<TransitionBudgetResource>,
    ) -> Result<SharedBudget, bindings::lix::plugin::host::SourceError> {
        self.table
            .get(resource)
            .map(|resource| resource.state.clone())
            .map_err(table_source_error)
    }
}

fn ensure_budget(
    expected: &Weak<Mutex<TransitionBudgetState>>,
    actual: &SharedBudget,
) -> Result<(), bindings::lix::plugin::host::SourceError> {
    let expected = expected.upgrade().ok_or_else(|| {
        bindings::lix::plugin::host::SourceError::Unavailable(
            "v2 transition budget has been released".to_owned(),
        )
    })?;
    if !Arc::ptr_eq(&expected, actual) {
        return Err(bindings::lix::plugin::host::SourceError::Unavailable(
            "v2 resource was called with a different transition budget".to_owned(),
        ));
    }
    Ok(())
}

fn table_source_error(error: impl fmt::Display) -> bindings::lix::plugin::host::SourceError {
    bindings::lix::plugin::host::SourceError::Unavailable(format!(
        "v2 resource table error: {error}"
    ))
}

fn lix_source_error(error: LixError) -> bindings::lix::plugin::host::SourceError {
    if error.message.contains("deadline") {
        bindings::lix::plugin::host::SourceError::DeadlineExceeded
    } else if error.message.contains("too large") {
        bindings::lix::plugin::host::SourceError::RecordTooLarge(0)
    } else if error.code == LixError::CODE_INVALID_PARAM {
        bindings::lix::plugin::host::SourceError::LimitExceeded(error.message)
    } else {
        bindings::lix::plugin::host::SourceError::Unavailable(error.message)
    }
}

impl From<WasmTransitionLimits> for bindings::lix::plugin::host::TransitionLimits {
    fn from(limits: WasmTransitionLimits) -> Self {
        Self {
            max_record_bytes: limits.max_record_bytes,
            max_page_bytes: limits.max_page_bytes,
            max_pages: limits.max_pages,
            max_total_bytes: limits.max_total_bytes,
            max_inline_edits: limits.max_inline_edits,
            max_inline_input_bytes: limits.max_inline_input_bytes,
            max_attachment_refs: limits.max_attachment_refs,
            total_deadline_nanoseconds: limits.total_deadline_nanoseconds,
        }
    }
}

impl WasmtimeV2Actor {
    fn store_mut(&mut self) -> Result<&mut Store<WasiHostState>, LixError> {
        self.store
            .as_mut()
            .ok_or_else(|| v2_invalid_plugin("v2 plugin actor has been retired"))
    }

    fn allocate_handle(&mut self) -> Result<u64, LixError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or_else(|| v2_limit("v2 actor handle space exhausted"))?;
        Ok(handle)
    }

    fn begin_transition(
        &mut self,
        limits: WasmTransitionLimits,
        input_document: Option<u64>,
    ) -> Result<(u64, Resource<TransitionBudgetResource>), LixError> {
        limits.validate()?;
        if !self.transitions.is_empty() {
            return Err(v2_invalid_plugin(
                "v2 actor cannot begin a second transition before finishing the first",
            ));
        }
        let budget = Arc::new(Mutex::new(TransitionBudgetState::new(limits)?));
        let component_limits = self.limits;
        let store = self.store_mut()?;
        reset_store_limits(store, component_limits)?;
        set_transition_deadline(store, &budget, component_limits)?;
        let resource = store
            .data_mut()
            .table
            .push(TransitionBudgetResource {
                state: budget.clone(),
            })
            .map_err(|error| {
                wasm_runtime_error("failed to allocate v2 transition budget", error)
            })?;
        let transition = self.allocate_handle()?;
        self.transitions.insert(
            transition,
            ActiveTransition {
                budget_rep: resource.rep(),
                budget,
                input_document,
                successor_document: None,
            },
        );
        Ok((transition, resource))
    }

    fn prepare_nested_call(&mut self, transition: u64) -> Result<u32, LixError> {
        let active = self
            .transitions
            .get(&transition)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 transition handle"))?;
        let budget = active.budget.clone();
        let budget_rep = active.budget_rep;
        let limits = self.limits;
        set_transition_deadline(self.store_mut()?, &budget, limits)?;
        Ok(budget_rep)
    }

    /// Re-arms Wasmtime's epoch deadline before a guest destructor that is
    /// owned by an active transition. This deliberately does not reset fuel:
    /// cleanup remains part of the transition's aggregate execution budget.
    fn prepare_transition_resource_drop(&mut self, budget: &SharedBudget) -> Result<(), LixError> {
        let limits = self.limits;
        let result = self
            .store
            .as_mut()
            .ok_or_else(|| v2_invalid_plugin("v2 plugin actor has been retired"))
            .and_then(|store| set_transition_deadline(store, budget, limits));
        if let Err(error) = result {
            self.retire_now();
            return Err(error);
        }
        Ok(())
    }

    fn push_byte_source(
        &mut self,
        source: Arc<dyn WasmByteSource>,
        budget: &SharedBudget,
    ) -> Result<Resource<ByteSourceResource>, LixError> {
        let len = source.len();
        self.store_mut()?
            .data_mut()
            .table
            .push(ByteSourceResource {
                source,
                base_offset: 0,
                len,
                budget: Arc::downgrade(budget),
            })
            .map_err(|error| wasm_runtime_error("failed to allocate v2 byte source", error))
    }

    fn push_packet_source(
        &mut self,
        source: PacketSourceValue,
        budget: &SharedBudget,
    ) -> Result<Resource<PacketSourceResource>, LixError> {
        self.store_mut()?
            .data_mut()
            .table
            .push(PacketSourceResource {
                source,
                budget: Arc::downgrade(budget),
                eof: false,
                previous_entity_key: None,
                seen_change_keys: BTreeSet::new(),
            })
            .map_err(|error| wasm_runtime_error("failed to allocate v2 packet source", error))
    }

    fn transition_budget(&self, transition: u64) -> Result<SharedBudget, LixError> {
        self.transitions
            .get(&transition)
            .map(|transition| transition.budget.clone())
            .ok_or_else(|| v2_invalid_plugin("unknown v2 transition handle"))
    }

    /// Gives a guest call made outside a top-level transition fresh component
    /// fuel and a bounded epoch deadline. In particular, an accepted document
    /// may sit idle longer than the preceding transition's deadline before the
    /// next warm write forks it.
    fn prepare_standalone_guest_call(&mut self) -> Result<(), LixError> {
        if !self.transitions.is_empty() {
            return Err(v2_invalid_plugin(
                "v2 standalone guest call cannot run during an active transition",
            ));
        }
        let limits = self.limits;
        reset_standalone_call_limits(self.store_mut()?, limits)
    }

    fn retire_with_error(&mut self, context: &str, error: impl fmt::Display) -> LixError {
        self.retire_now();
        wasm_runtime_error(context, error)
    }

    fn retire_now(&mut self) {
        self.store.take();
        self.documents.clear();
        self.change_cursors.clear();
        self.edit_cursors.clear();
        self.outputs.clear();
        self.transitions.clear();
    }

    fn plugin_error(
        &self,
        context: &str,
        error: bindings::exports::lix::plugin::api::PluginError,
    ) -> LixError {
        use bindings::exports::lix::plugin::api::PluginError;
        let (kind, message) = match error {
            PluginError::InvalidInput(message) => ("invalid-input", message),
            PluginError::RecordTooLarge(size) => {
                return v2_record_too_large(size);
            }
            PluginError::LimitExceeded(message) => ("limit-exceeded", message),
            PluginError::DeadlineExceeded => ("deadline-exceeded", String::new()),
            PluginError::Internal(message) => ("internal", message),
        };
        v2_invalid_plugin(format!("{context} returned {kind}: {message}"))
    }

    /// A guest-returned error has deterministic completion semantics. Unless
    /// it reports a deadline, discard only this prospective transition and
    /// keep the Store plus all accepted documents reusable. Traps still use
    /// `retire_with_error`, and a returned deadline remains a timeout that
    /// retires the complete Store.
    fn handle_returned_plugin_error(
        &mut self,
        transition: u64,
        context: &str,
        error: bindings::exports::lix::plugin::api::PluginError,
    ) -> LixError {
        let must_retire = matches!(
            &error,
            bindings::exports::lix::plugin::api::PluginError::DeadlineExceeded
        );
        let plugin_error = self.plugin_error(context, error);
        if must_retire {
            self.retire_now();
            return plugin_error;
        }
        match self.discard_transition(transition) {
            Ok(()) => plugin_error,
            Err(cleanup_error) => cleanup_error,
        }
    }

    fn discard_transition(&mut self, transition: u64) -> Result<(), LixError> {
        // Guest-returned cursor errors already discard their transition before
        // propagating to the engine drain wrapper. Host cleanup is deliberately
        // idempotent so that wrapper can use one rejection path for both guest
        // errors and host-side validation failures.
        if !self.transitions.contains_key(&transition) {
            return Ok(());
        }
        let active = self
            .transitions
            .remove(&transition)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 transition handle"))?;
        let mut resources = self
            .change_cursors
            .extract_if(|_, cursor| cursor.transition == transition)
            .map(|(_, cursor)| cursor.resource)
            .chain(
                self.edit_cursors
                    .extract_if(|_, cursor| cursor.transition == transition)
                    .map(|(_, cursor)| cursor.resource),
            )
            .chain(
                self.outputs
                    .extract_if(|_, output| output.transition == transition)
                    .map(|(_, output)| output.resource),
            )
            .collect::<Vec<_>>();
        for handle in [active.successor_document, active.input_document]
            .into_iter()
            .flatten()
        {
            if let Some(resource) = self.documents.remove(&handle) {
                resources.push(resource);
            }
        }
        for resource in resources {
            self.prepare_transition_resource_drop(&active.budget)?;
            if let Err(error) = resource.resource_drop(self.store_mut()?) {
                return Err(
                    self.retire_with_error("failed to discard v2 transition resource", error)
                );
            }
        }
        active
            .budget
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished = true;
        let result = self.store_mut()?.data_mut().table.delete(
            Resource::<TransitionBudgetResource>::new_own(active.budget_rep),
        );
        if let Err(error) = result {
            return Err(self.retire_with_error("failed to discard v2 transition budget", error));
        }
        Ok(())
    }

    fn register_file_transition(
        &mut self,
        transition: u64,
        value: bindings::exports::lix::plugin::api::FileTransition,
    ) -> Result<WasmFileTransition, LixError> {
        let limits = self
            .transition_budget(transition)?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits;
        let document = self.allocate_handle()?;
        let cursor = self.allocate_handle()?;
        self.documents.insert(document, value.document);
        self.transitions
            .get_mut(&transition)
            .expect("transition was checked above")
            .successor_document = Some(document);
        self.change_cursors.insert(
            cursor,
            ChangeCursorState {
                resource: value.changes,
                transition,
                validator: WasmChangeDrainValidator::new(limits)?,
                eof: false,
            },
        );
        Ok(WasmFileTransition {
            transition: WasmTransitionHandle(transition),
            document: WasmDocumentHandle(document),
            changes: WasmChangeCursorHandle(cursor),
        })
    }

    fn register_entity_transition(
        &mut self,
        transition: u64,
        base_len: u64,
        value: bindings::exports::lix::plugin::api::EntityTransition,
    ) -> Result<WasmEntityTransition, LixError> {
        let limits = self
            .transition_budget(transition)?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits;
        let document = self.allocate_handle()?;
        let cursor = self.allocate_handle()?;
        self.documents.insert(document, value.document);
        self.transitions
            .get_mut(&transition)
            .expect("transition was checked above")
            .successor_document = Some(document);
        self.edit_cursors.insert(
            cursor,
            EditCursorState {
                resource: value.edits,
                transition,
                validator: WasmEditDrainValidator::new(base_len, limits)?,
                eof: false,
            },
        );
        Ok(WasmEntityTransition {
            transition: WasmTransitionHandle(transition),
            document: WasmDocumentHandle(document),
            edits: WasmEditCursorHandle(cursor),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const LARGE_CSV_ROWS: u32 = 220_000;

    struct LargeCsvEntitySource {
        next_row: u32,
        emitted_table: bool,
    }

    #[derive(Debug, Clone)]
    struct TestByteSource(Arc<Vec<u8>>);

    impl WasmByteSource for TestByteSource {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }

        fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
            let start = usize::try_from(offset)
                .map_err(|_| v2_invalid_plugin("test byte-source offset overflowed"))?;
            let end = start
                .checked_add(length as usize)
                .ok_or_else(|| v2_invalid_plugin("test byte-source range overflowed"))?;
            self.0
                .get(start..end)
                .map(ToOwned::to_owned)
                .ok_or_else(|| v2_invalid_plugin("test byte-source range is out of bounds"))
        }
    }

    struct DelayedLenByteSource {
        bytes: Arc<Vec<u8>>,
        delay: Duration,
    }

    impl WasmByteSource for DelayedLenByteSource {
        fn len(&self) -> u64 {
            std::thread::sleep(self.delay);
            self.bytes.len() as u64
        }

        fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
            TestByteSource(self.bytes.clone()).read(offset, length)
        }
    }

    struct EmptyEntitySource;

    impl WasmEntitySource for EmptyEntitySource {
        fn next_page(&mut self, _max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError> {
            Ok(None)
        }
    }

    struct SingleChangeSource(Option<WasmEntityChanges<WasmHostBytes>>);

    impl WasmEntityChangeSource for SingleChangeSource {
        fn next_page(
            &mut self,
            _max_bytes: u32,
        ) -> Result<Option<WasmEntityChanges<WasmHostBytes>>, LixError> {
            Ok(self.0.take())
        }
    }

    impl WasmEntitySource for LargeCsvEntitySource {
        fn next_page(&mut self, max_bytes: u32) -> Result<Option<WasmEntityPage>, LixError> {
            if self.next_row == LARGE_CSV_ROWS && self.emitted_table {
                return Ok(None);
            }
            // The production snapshots below encode below 256 bytes each;
            // leave ample framing headroom under the host-provided page cap.
            let page_rows = (max_bytes / 256).clamp(1, 2_048);
            let end = self.next_row.saturating_add(page_rows).min(LARGE_CSV_ROWS);
            let mut entities = Vec::with_capacity((end - self.next_row) as usize + 1);
            while self.next_row < end {
                let index = self.next_row;
                let id = format!("{index:032x}");
                let denominator = u128::from(LARGE_CSV_ROWS) + 1;
                let order_rank =
                    u64::try_from((u128::from(index) + 1) * u128::from(u64::MAX) / denominator)
                        .expect("rank fits")
                        | 1;
                let first = if index < 120_000 {
                    "000000000000000"
                } else {
                    "00000000000000"
                };
                let snapshot = format!(
                    "{{\"id\":\"{id}\",\"order_key\":\"{order_rank:016x}\",\"cells\":[\"{first}\",\"1111111111\",\"2222222222\",\"3333333333\"]}}"
                )
                .into_bytes();
                entities.push(WasmEntity {
                    key: WasmEntityKey {
                        schema_key: "csv_row".to_owned(),
                        entity_pk: vec![id],
                    },
                    snapshot_content: WasmHostBytes::Inline(snapshot),
                });
                self.next_row += 1;
            }
            if self.next_row == LARGE_CSV_ROWS && !self.emitted_table {
                entities.push(WasmEntity {
                    key: WasmEntityKey {
                        schema_key: "csv_table".to_owned(),
                        entity_pk: vec!["root".to_owned()],
                    },
                    snapshot_content: WasmHostBytes::Inline(
                        br#"{"id":"root","dialect":{"delimiter":",","quote":"\"","terminator":"\n"}}"#
                            .to_vec(),
                    ),
                });
                self.emitted_table = true;
            }
            Ok(Some(WasmEntityPage { entities }))
        }
    }

    fn memory_probe_descriptor() -> WasmFileDescriptor {
        WasmFileDescriptor {
            path: Some("large.csv".to_owned()),
            media_type: Some("text/csv".to_owned()),
            plugin: lix_engine::wasm::v2::WasmPluginSelection {
                plugin_key: "plugin_csv_v2".to_owned(),
                generation: "memory-probe".to_owned(),
            },
        }
    }

    fn large_csv_bytes() -> Vec<u8> {
        let long: &[u8] = b"000000000000000,1111111111,2222222222,3333333333\n";
        let short: &[u8] = b"00000000000000,1111111111,2222222222,3333333333\n";
        let mut bytes = Vec::with_capacity(10_680_000);
        for index in 0..LARGE_CSV_ROWS {
            bytes.extend_from_slice(if index < 120_000 { long } else { short });
        }
        assert_eq!(bytes.len(), 10_680_000);
        bytes
    }

    async fn csv_test_actor_with_limits(limits: WasmLimits) -> Box<dyn WasmComponentV2Actor> {
        let wasm_path = option_env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2")
            .expect("the CSV v2 artifact dependency must be available");
        let wasm = std::fs::read(wasm_path).expect("CSV v2 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("test runtime should initialize");
        let factory = runtime
            .compile_component_v2(wasm, limits)
            .await
            .expect("CSV v2 component should compile");
        factory
            .instantiate_actor()
            .await
            .expect("CSV v2 actor should instantiate")
    }

    async fn csv_test_actor() -> Box<dyn WasmComponentV2Actor> {
        csv_test_actor_with_limits(WasmLimits::default()).await
    }

    async fn open_small_csv(
        actor: &mut dyn WasmComponentV2Actor,
    ) -> (WasmDocumentHandle, WasmEntity<WasmHostBytes>) {
        let limits = WasmTransitionLimits::default();
        let transition = actor
            .open_file(
                limits,
                WasmOpenFileInput {
                    descriptor: memory_probe_descriptor(),
                    file: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 7,
                    },
                },
            )
            .await
            .expect("small CSV should open");
        let mut row = None;
        while let Some(page) = actor
            .next_change_page(
                transition.transition,
                transition.changes,
                limits.max_page_bytes,
            )
            .await
            .expect("small CSV changes should drain")
        {
            assert!(page.outputs.is_none());
            for group in page.changes.groups {
                for change in group.changes {
                    if let WasmEntityChange::Upsert { entity, .. } = change
                        && entity.key.schema_key == "csv_row"
                    {
                        let snapshot_content = match entity.snapshot_content {
                            WasmGuestBytes::Inline(bytes) => WasmHostBytes::Inline(bytes),
                            WasmGuestBytes::Output(_) => {
                                panic!("small CSV row snapshot must stay inline")
                            }
                        };
                        row = Some(WasmEntity {
                            key: entity.key,
                            snapshot_content,
                        });
                    }
                }
            }
        }
        actor
            .finish_transition(transition.transition)
            .await
            .expect("small CSV transition should finish");
        (
            transition.document,
            row.expect("small CSV should emit one row entity"),
        )
    }

    async fn assert_document_was_discarded(
        actor: &mut dyn WasmComponentV2Actor,
        document: WasmDocumentHandle,
    ) {
        let error = actor
            .drop_document(document)
            .await
            .expect_err("failed transition document should already be discarded");
        assert!(error.message.contains("unknown v2 document handle"));
    }

    async fn assert_actor_is_reusable(
        actor: &mut dyn WasmComponentV2Actor,
        accepted: WasmDocumentHandle,
    ) {
        let fork = actor
            .fork_document(accepted)
            .await
            .expect("accepted document should survive a deterministic plugin error");
        actor
            .drop_document(fork)
            .await
            .expect("post-error fork should drop");
        let (retry, _) = open_small_csv(actor).await;
        actor
            .drop_document(retry)
            .await
            .expect("a new transition should start after deterministic cleanup");
    }

    #[tokio::test]
    async fn idle_actor_fork_receives_fresh_fuel_and_epoch_deadline() {
        let mut actor = csv_test_actor_with_limits(WasmLimits {
            max_fuel: Some(100_000_000),
            timeout_ms: Some(250),
            ..WasmLimits::default()
        })
        .await;
        let (accepted, _) = open_small_csv(actor.as_mut()).await;

        // The preceding transition's absolute Wasmtime epoch deadline is now
        // stale. A persistent actor must refresh limits before invoking the
        // guest's standalone document.fork export.
        std::thread::sleep(Duration::from_millis(750));
        let fork = actor
            .fork_document(accepted)
            .await
            .expect("an idle accepted document should still fork");
        assert_ne!(fork, accepted);

        // Keep a smoke check for the standalone destructor hardening. The CSV
        // destructor is trivial, so the fork above is the assertion that
        // independently demonstrates stale-deadline recovery.
        std::thread::sleep(Duration::from_millis(750));
        actor
            .drop_document(fork)
            .await
            .expect("an idle document destructor should still run");
        actor
            .retire()
            .await
            .expect("idle test actor should retire cleanly");
    }

    #[test]
    fn standalone_call_preparation_restores_configured_fuel() {
        const MAX_FUEL: u64 = 12_345;

        let engine = create_engine(true, true).expect("test engine should initialize");
        let limits = WasmLimits {
            max_fuel: Some(MAX_FUEL),
            timeout_ms: Some(250),
            ..WasmLimits::default()
        };
        let mut store = create_store(&engine, limits).expect("test Store should initialize");
        store.set_fuel(0).expect("test should exhaust Store fuel");

        reset_standalone_call_limits(&mut store, limits)
            .expect("standalone preparation should reset Store limits");

        assert_eq!(
            store.get_fuel().expect("fuel should be configured"),
            MAX_FUEL
        );
    }

    #[tokio::test]
    async fn top_level_export_rearms_deadline_after_input_construction() {
        let mut actor = csv_test_actor_with_limits(WasmLimits {
            timeout_ms: Some(250),
            ..WasmLimits::default()
        })
        .await;
        let limits = WasmTransitionLimits::default();
        let transition = actor
            .open_file(
                limits,
                WasmOpenFileInput {
                    descriptor: memory_probe_descriptor(),
                    file: Arc::new(DelayedLenByteSource {
                        bytes: Arc::new(b"a,b\n".to_vec()),
                        delay: Duration::from_millis(750),
                    }),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 8,
                    },
                },
            )
            .await
            .expect("guest export should receive a fresh component deadline");
        actor
            .discard_transition(transition.transition)
            .await
            .expect("fresh transition should discard cleanly");
        assert!(!actor.is_retired());
    }

    #[tokio::test]
    async fn expired_transition_cleanup_retires_before_guest_destructor() {
        let mut actor = csv_test_actor().await;
        let limits = WasmTransitionLimits {
            total_deadline_nanoseconds: 500_000_000,
            ..WasmTransitionLimits::default()
        };
        let transition = actor
            .open_file(
                limits,
                WasmOpenFileInput {
                    descriptor: memory_probe_descriptor(),
                    file: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 9,
                    },
                },
            )
            .await
            .expect("small CSV should open before its deadline");

        std::thread::sleep(Duration::from_millis(750));
        let error = actor
            .discard_transition(transition.transition)
            .await
            .expect_err("expired cleanup must not enter a guest destructor");
        assert!(error.message.contains("deadline"));
        assert!(actor.is_retired());
    }

    #[tokio::test]
    async fn returned_warm_plugin_errors_discard_only_the_working_transition() {
        let mut actor = csv_test_actor().await;
        let (accepted, _) = open_small_csv(actor.as_mut()).await;
        let limits = WasmTransitionLimits::default();

        let invalid_file_fork = actor
            .fork_document(accepted)
            .await
            .expect("file detection fork should succeed");
        let error = actor
            .file_changed(
                invalid_file_fork,
                limits,
                WasmFileUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    edits: vec![lix_engine::wasm::v2::WasmInputSplice {
                        offset: 0,
                        delete_len: 1,
                        insert: WasmInputBytes::Inline(vec![0xff]),
                    }],
                    after: Arc::new(TestByteSource(Arc::new(vec![0xff, b',', b'b', b'\n']))),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 8,
                    },
                },
            )
            .await
            .expect_err("invalid UTF-8 should be a clean plugin rejection");
        assert!(
            error
                .message
                .contains("file-changed returned invalid-input")
        );
        assert_document_was_discarded(actor.as_mut(), invalid_file_fork).await;
        assert_actor_is_reusable(actor.as_mut(), accepted).await;

        let invalid_entity_fork = actor
            .fork_document(accepted)
            .await
            .expect("renderer fork should succeed");
        let unsupported = WasmEntityChanges {
            groups: vec![lix_engine::wasm::v2::WasmMergeGroup {
                changes: vec![WasmEntityChange::Upsert {
                    entity: WasmEntity {
                        key: WasmEntityKey {
                            schema_key: "unsupported".to_owned(),
                            entity_pk: vec!["one".to_owned()],
                        },
                        snapshot_content: WasmHostBytes::Inline(br#"{"id":"one"}"#.to_vec()),
                    },
                    effect: WasmChangeEffect::Content,
                }],
            }],
        };
        let error = actor
            .entities_changed(
                invalid_entity_fork,
                limits,
                WasmEntityUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    changes: Box::new(SingleChangeSource(Some(unsupported))),
                    activated_entities: Box::new(EmptyEntitySource),
                    current_entities: Box::new(EmptyEntitySource),
                },
            )
            .await
            .expect_err("unsupported entity should be a clean plugin rejection");
        assert!(
            error
                .message
                .contains("entities-changed returned invalid-input")
        );
        assert_document_was_discarded(actor.as_mut(), invalid_entity_fork).await;
        assert_actor_is_reusable(actor.as_mut(), accepted).await;
        actor
            .drop_document(accepted)
            .await
            .expect("accepted document should remain live until explicitly dropped");
    }

    #[tokio::test]
    async fn returned_cursor_errors_discard_successors_without_retiring_the_actor() {
        let mut actor = csv_test_actor().await;
        let (accepted, row) = open_small_csv(actor.as_mut()).await;
        let limits = WasmTransitionLimits::default();

        let detection_fork = actor
            .fork_document(accepted)
            .await
            .expect("detection fork should succeed");
        let detected = actor
            .file_changed(
                detection_fork,
                limits,
                WasmFileUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    edits: vec![lix_engine::wasm::v2::WasmInputSplice {
                        offset: 0,
                        delete_len: 1,
                        insert: WasmInputBytes::Inline(vec![b'x']),
                    }],
                    after: Arc::new(TestByteSource(Arc::new(b"x,b\n".to_vec()))),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 9,
                    },
                },
            )
            .await
            .expect("valid warm detection should return a cursor");
        let error = actor
            .next_change_page(detected.transition, detected.changes, 1)
            .await
            .expect_err("one byte cannot contain a complete CSV change record");
        assert!(error.message.contains("v2 record is too large"));
        assert_document_was_discarded(actor.as_mut(), detection_fork).await;
        assert_document_was_discarded(actor.as_mut(), detected.document).await;
        assert!(
            actor
                .finish_transition(detected.transition)
                .await
                .expect_err("failed cursor transition should already be discarded")
                .message
                .contains("unknown v2 transition handle")
        );
        assert_actor_is_reusable(actor.as_mut(), accepted).await;

        let snapshot = match row.snapshot_content {
            WasmHostBytes::Inline(bytes) => bytes,
            WasmHostBytes::Source(_) => panic!("small row snapshot must be inline"),
        };
        let snapshot = String::from_utf8(snapshot)
            .expect("CSV row snapshot is UTF-8")
            .replace("[\"a\",\"b\"]", "[\"x\",\"b\"]")
            .into_bytes();
        let renderer_fork = actor
            .fork_document(accepted)
            .await
            .expect("renderer fork should succeed");
        let rendered = actor
            .entities_changed(
                renderer_fork,
                limits,
                WasmEntityUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(Arc::new(b"a,b\n".to_vec()))),
                    changes: Box::new(SingleChangeSource(Some(WasmEntityChanges {
                        groups: vec![lix_engine::wasm::v2::WasmMergeGroup {
                            changes: vec![WasmEntityChange::Upsert {
                                entity: WasmEntity {
                                    key: row.key,
                                    snapshot_content: WasmHostBytes::Inline(snapshot),
                                },
                                effect: WasmChangeEffect::Content,
                            }],
                        }],
                    }))),
                    activated_entities: Box::new(EmptyEntitySource),
                    current_entities: Box::new(EmptyEntitySource),
                },
            )
            .await
            .expect("valid warm render should return a cursor");
        let error = actor
            .next_edit_page(
                rendered.transition,
                rendered.edits,
                0,
                limits.max_page_bytes,
            )
            .await
            .expect_err("zero max-edits should be a clean cursor rejection");
        assert!(
            error
                .message
                .contains("edit-cursor.next returned limit-exceeded")
        );
        assert_document_was_discarded(actor.as_mut(), renderer_fork).await;
        assert_document_was_discarded(actor.as_mut(), rendered.document).await;
        assert!(
            actor
                .finish_transition(rendered.transition)
                .await
                .expect_err("failed edit transition should already be discarded")
                .message
                .contains("unknown v2 transition handle")
        );
        assert_actor_is_reusable(actor.as_mut(), accepted).await;
        actor
            .drop_document(accepted)
            .await
            .expect("accepted document should remain live until explicitly dropped");
    }

    #[test]
    fn packet_v1_number_free_profile_rejects_duplicates_numbers_and_bad_scalars() {
        validate_number_free_snapshot(br#"{"table":["a",null,true]}"#)
            .expect("number-free snapshot should validate");
        assert!(
            validate_number_free_snapshot(br#"{"a":null,"\u0061":true}"#).is_err(),
            "decoded duplicate keys must be rejected"
        );
        assert!(
            validate_number_free_snapshot(br#"{"value":1}"#).is_err(),
            "the production CSV slice must reject number-bearing packet-v1 snapshots"
        );
        assert!(
            validate_number_free_snapshot(br#"{"value":"\uD800"}"#).is_err(),
            "unpaired surrogates must be rejected"
        );
    }

    #[test]
    fn packet_v1_change_decoder_rejects_trailing_and_overflowed_attachment_data() {
        let limits = WasmTransitionLimits::default();
        let mut record = Vec::new();
        push_u32(&mut record, 1);
        record.push(1); // delete
        encode_entity_key(
            &WasmEntityKey {
                schema_key: "csv_row".to_owned(),
                entity_pk: vec!["row".to_owned()],
            },
            &mut record,
        )
        .unwrap();
        let mut packet = Vec::new();
        push_u32(&mut packet, record.len() as u32);
        packet.extend_from_slice(&record);
        let decoded = decode_change_packet(1, &packet, limits).unwrap();
        assert_eq!(decoded.changes.entity_change_count(), 1);

        packet.push(0);
        assert!(decode_change_packet(1, &packet, limits).is_err());

        let mut attachment_record = Vec::new();
        push_u32(&mut attachment_record, 1);
        attachment_record.push(0); // upsert
        encode_entity_key(
            &WasmEntityKey {
                schema_key: "csv_row".to_owned(),
                entity_pk: vec!["row".to_owned()],
            },
            &mut attachment_record,
        )
        .unwrap();
        attachment_record.push(0); // content effect
        attachment_record.push(1); // output attachment
        push_u32(&mut attachment_record, 0);
        push_u64(&mut attachment_record, u64::MAX);
        push_u64(&mut attachment_record, 1);
        let mut attachment_packet = Vec::new();
        push_u32(&mut attachment_packet, attachment_record.len() as u32);
        attachment_packet.extend_from_slice(&attachment_record);
        assert!(decode_change_packet(1, &attachment_packet, limits).is_err());
    }

    #[test]
    fn fresh_v2_actor_stores_have_isolated_resource_tables() {
        let engine = create_engine(false, true).expect("test engine should initialize");
        let limits = WasmLimits::default();
        assert_eq!(limits.max_memory_bytes, 64 * 1024 * 1024);
        let mut first = create_store(&engine, limits).expect("first Store should initialize");
        let second = create_store(&engine, limits).expect("second Store should initialize");
        let budget = Arc::new(Mutex::new(
            TransitionBudgetState::new(WasmTransitionLimits::default()).unwrap(),
        ));
        let resource = first
            .data_mut()
            .table
            .push(TransitionBudgetResource { state: budget })
            .unwrap();
        assert!(first.data().table.get(&resource).is_ok());
        assert!(
            second.data().table.get(&resource).is_err(),
            "a resource handle from one actor Store must not resolve in another"
        );
    }

    #[tokio::test]
    async fn production_csv_v2_initial_import_stays_under_64_mib() {
        let wasm_path = option_env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2")
            .expect("the CSV v2 artifact dependency must be available");
        let wasm = std::fs::read(wasm_path).expect("CSV v2 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("test runtime should initialize");
        let factory = runtime
            .compile_component_v2(wasm, WasmLimits::default())
            .await
            .expect("CSV v2 component should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("CSV v2 actor should instantiate under the 64 MiB limit");
        let limits = WasmTransitionLimits {
            total_deadline_nanoseconds: 120_000_000_000,
            ..WasmTransitionLimits::default()
        };
        let bytes = Arc::new(large_csv_bytes());
        let transition = actor
            .open_file(
                limits,
                WasmOpenFileInput {
                    descriptor: memory_probe_descriptor(),
                    file: Arc::new(TestByteSource(bytes)),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 0,
                    },
                },
            )
            .await
            .expect("220k-row initial import must fit under the production guest limit");
        let mut change_count = 0usize;
        while let Some(page) = actor
            .next_change_page(
                transition.transition,
                transition.changes,
                limits.max_page_bytes,
            )
            .await
            .expect("initial semantic changes should drain")
        {
            assert!(page.outputs.is_none());
            change_count += page.changes.entity_change_count();
        }
        assert_eq!(change_count, LARGE_CSV_ROWS as usize + 1);
        let counters = actor
            .finish_transition(transition.transition)
            .await
            .expect("initial import transition should finish");
        eprintln!(
            "v2_csv_initial_import rows={LARGE_CSV_ROWS} source_bytes=10680000 guest_linear_memory_high_water_bytes={}",
            counters.guest_linear_memory_high_water_bytes
        );
        assert!(counters.guest_linear_memory_high_water_bytes > 0);
        assert!(
            counters.guest_linear_memory_high_water_bytes <= 64 * 1024 * 1024,
            "initial import guest high water was {} bytes",
            counters.guest_linear_memory_high_water_bytes
        );
        actor
            .drop_document(transition.document)
            .await
            .expect("initial accepted document should drop");
    }

    #[tokio::test]
    async fn production_csv_v2_cold_open_and_warm_edit_stay_under_64_mib() {
        let wasm_path = option_env!("CARGO_CDYLIB_FILE_PLUGIN_CSV_V2_plugin_csv_v2")
            .expect("the CSV v2 artifact dependency must be available");
        let wasm = std::fs::read(wasm_path).expect("CSV v2 component should be readable");
        let runtime = WasmtimePluginRuntime::new().expect("test runtime should initialize");
        let factory = runtime
            .compile_component_v2(wasm, WasmLimits::default())
            .await
            .expect("CSV v2 component should compile");
        let mut actor = factory
            .instantiate_actor()
            .await
            .expect("CSV v2 actor should instantiate under the 64 MiB limit");
        let limits = WasmTransitionLimits {
            total_deadline_nanoseconds: 120_000_000_000,
            ..WasmTransitionLimits::default()
        };
        let transition = actor
            .open_entities(
                limits,
                WasmOpenEntitiesInput {
                    descriptor: memory_probe_descriptor(),
                    entities: Box::new(LargeCsvEntitySource {
                        next_row: 0,
                        emitted_table: false,
                    }),
                },
            )
            .await
            .expect("220k-row cold open must fit under the production guest limit");
        let page = actor
            .next_edit_page(
                transition.transition,
                transition.edits,
                limits.max_inline_edits,
                limits.max_page_bytes,
            )
            .await
            .expect("renderer edit should drain")
            .expect("cold open should render one edit");
        assert_eq!(page.edits.len(), 1);
        assert_eq!(page.edits[0].offset, 0);
        assert_eq!(page.edits[0].delete_len, 0);
        let outputs = page
            .outputs
            .expect("10.68 MiB output should use an attachment");
        assert_eq!(
            actor
                .output_len(transition.transition, outputs, 0)
                .await
                .expect("renderer output length should resolve"),
            10_680_000
        );
        let mut read = 0u64;
        let mut actual_hash = blake3::Hasher::new();
        while read < 10_680_000 {
            let requested = u32::try_from((10_680_000 - read).min(1024 * 1024)).unwrap();
            let bytes = actor
                .read_output(transition.transition, outputs, 0, read, requested)
                .await
                .expect("renderer output should be readable in bounded pages");
            assert!(!bytes.is_empty());
            actual_hash.update(&bytes);
            read += bytes.len() as u64;
        }
        let mut expected_hash = blake3::Hasher::new();
        let long: &[u8] = b"000000000000000,1111111111,2222222222,3333333333\n";
        let short: &[u8] = b"00000000000000,1111111111,2222222222,3333333333\n";
        for index in 0..LARGE_CSV_ROWS {
            expected_hash.update(if index < 120_000 { long } else { short });
        }
        assert_eq!(
            actual_hash.finalize(),
            expected_hash.finalize(),
            "cold renderer output must exactly match the 10.68 MiB fixture"
        );
        assert!(
            actor
                .next_edit_page(
                    transition.transition,
                    transition.edits,
                    limits.max_inline_edits,
                    limits.max_page_bytes,
                )
                .await
                .expect("renderer cursor should reach EOF")
                .is_none()
        );
        let counters = actor
            .finish_transition(transition.transition)
            .await
            .expect("cold-open transition should finish");
        eprintln!(
            "v2_csv_cold_open rows={LARGE_CSV_ROWS} rendered_bytes=10680000 guest_linear_memory_high_water_bytes={}",
            counters.guest_linear_memory_high_water_bytes
        );
        assert!(counters.guest_linear_memory_high_water_bytes > 0);
        assert!(
            counters.guest_linear_memory_high_water_bytes <= 64 * 1024 * 1024,
            "guest high water was {} bytes",
            counters.guest_linear_memory_high_water_bytes
        );

        let cold_document = transition.document;
        let before = Arc::new(large_csv_bytes());
        let mut after_bytes = before.as_ref().clone();
        let edited_offset = 110_000usize * 49 + 16;
        assert_eq!(after_bytes[edited_offset], b'1');
        after_bytes[edited_offset] = b'x';
        let after = Arc::new(after_bytes);
        let detection_base = actor
            .fork_document(cold_document)
            .await
            .expect("warm detection document should fork without copying its blob");
        let detection = actor
            .file_changed(
                detection_base,
                limits,
                WasmFileUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(before.clone())),
                    edits: vec![lix_engine::wasm::v2::WasmInputSplice {
                        offset: edited_offset as u64,
                        delete_len: 1,
                        insert: WasmInputBytes::Inline(vec![b'x']),
                    }],
                    after: Arc::new(TestByteSource(after.clone())),
                    ids: WasmIdNamespace {
                        high: 0x4c49_5832,
                        low: 1,
                    },
                },
            )
            .await
            .expect("one-row warm detection must fit under the production guest limit");
        let detected = actor
            .next_change_page(
                detection.transition,
                detection.changes,
                limits.max_page_bytes,
            )
            .await
            .expect("warm semantic changes should drain")
            .expect("one-row edit should emit one semantic page");
        assert!(detected.outputs.is_none());
        assert_eq!(detected.changes.entity_change_count(), 1);
        let host_changes = WasmEntityChanges {
            groups: detected
                .changes
                .groups
                .into_iter()
                .map(|group| lix_engine::wasm::v2::WasmMergeGroup {
                    changes: group
                        .changes
                        .into_iter()
                        .map(|change| match change {
                            WasmEntityChange::Upsert { entity, effect } => {
                                let snapshot_content = match entity.snapshot_content {
                                    WasmGuestBytes::Inline(bytes) => WasmHostBytes::Inline(bytes),
                                    WasmGuestBytes::Output(_) => {
                                        panic!("one-row CSV snapshot should stay inline")
                                    }
                                };
                                WasmEntityChange::Upsert {
                                    entity: WasmEntity {
                                        key: entity.key,
                                        snapshot_content,
                                    },
                                    effect,
                                }
                            }
                            WasmEntityChange::Delete(key) => WasmEntityChange::Delete(key),
                        })
                        .collect(),
                })
                .collect(),
        };
        assert!(
            actor
                .next_change_page(
                    detection.transition,
                    detection.changes,
                    limits.max_page_bytes,
                )
                .await
                .expect("warm change cursor should reach EOF")
                .is_none()
        );
        let detection_counters = actor
            .finish_transition(detection.transition)
            .await
            .expect("warm detection transition should finish");
        assert!(
            detection_counters.guest_linear_memory_high_water_bytes <= 64 * 1024 * 1024,
            "warm detection guest high water was {} bytes",
            detection_counters.guest_linear_memory_high_water_bytes
        );
        actor
            .drop_document(detection.document)
            .await
            .expect("discarded detection successor should drop");
        actor
            .drop_document(detection_base)
            .await
            .expect("detection fork should drop");

        let renderer_base = actor
            .fork_document(cold_document)
            .await
            .expect("warm renderer document should fork without copying its blob");
        let rendered = actor
            .entities_changed(
                renderer_base,
                limits,
                WasmEntityUpdate {
                    before_descriptor: memory_probe_descriptor(),
                    after_descriptor: memory_probe_descriptor(),
                    before: Arc::new(TestByteSource(before.clone())),
                    changes: Box::new(SingleChangeSource(Some(host_changes))),
                    activated_entities: Box::new(EmptyEntitySource),
                    current_entities: Box::new(EmptyEntitySource),
                },
            )
            .await
            .expect("one-row warm renderer transition must fit under 64 MiB");
        let edit_page = actor
            .next_edit_page(
                rendered.transition,
                rendered.edits,
                limits.max_inline_edits,
                limits.max_page_bytes,
            )
            .await
            .expect("warm renderer edits should drain")
            .expect("one-row semantic change should emit one byte edit page");
        assert_eq!(edit_page.edits.len(), 1);
        let edit = &edit_page.edits[0];
        let insert = match &edit.insert {
            WasmGuestBytes::Inline(bytes) => bytes.clone(),
            WasmGuestBytes::Output(range) => {
                let outputs = edit_page
                    .outputs
                    .expect("output-backed edit must own an output table");
                actor
                    .read_output(
                        rendered.transition,
                        outputs,
                        range.index,
                        range.offset,
                        u32::try_from(range.length).expect("one row fits u32"),
                    )
                    .await
                    .expect("warm renderer output should be readable")
            }
        };
        let start = usize::try_from(edit.offset).expect("fixture offset fits usize");
        let end = start + usize::try_from(edit.delete_len).expect("fixture length fits usize");
        let mut materialized = Vec::with_capacity(before.len() - (end - start) + insert.len());
        materialized.extend_from_slice(&before[..start]);
        materialized.extend_from_slice(&insert);
        materialized.extend_from_slice(&before[end..]);
        assert_eq!(
            materialized.as_slice(),
            after.as_slice(),
            "warm renderer splice must be exact"
        );
        assert!(
            actor
                .next_edit_page(
                    rendered.transition,
                    rendered.edits,
                    limits.max_inline_edits,
                    limits.max_page_bytes,
                )
                .await
                .expect("warm edit cursor should reach EOF")
                .is_none()
        );
        let renderer_counters = actor
            .finish_transition(rendered.transition)
            .await
            .expect("warm renderer transition should finish");
        eprintln!(
            "v2_csv_warm_edit rows={LARGE_CSV_ROWS} guest_linear_memory_high_water_bytes={}",
            renderer_counters.guest_linear_memory_high_water_bytes
        );
        assert!(
            renderer_counters.guest_linear_memory_high_water_bytes <= 64 * 1024 * 1024,
            "warm renderer guest high water was {} bytes",
            renderer_counters.guest_linear_memory_high_water_bytes
        );
        actor
            .drop_document(renderer_base)
            .await
            .expect("renderer fork should drop");
        actor
            .drop_document(rendered.document)
            .await
            .expect("renderer successor should drop");
        actor
            .drop_document(cold_document)
            .await
            .expect("cold accepted document should drop");
    }
}

fn set_transition_deadline(
    store: &mut Store<WasiHostState>,
    budget: &SharedBudget,
    component_limits: WasmLimits,
) -> Result<(), LixError> {
    let remaining = budget
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remaining_nanoseconds();
    if remaining == 0 {
        return Err(v2_deadline("v2 transition deadline elapsed"));
    }
    let component_remaining = component_limits
        .timeout_ms
        .map(|milliseconds| milliseconds.saturating_mul(1_000_000))
        .unwrap_or(u64::MAX);
    let remaining = remaining.min(component_remaining);
    let epoch_ticks = remaining.saturating_add(999_999) / 1_000_000;
    store.set_epoch_deadline(epoch_ticks.max(1));
    Ok(())
}

fn reset_standalone_call_limits(
    store: &mut Store<WasiHostState>,
    component_limits: WasmLimits,
) -> Result<(), LixError> {
    reset_store_limits(store, component_limits)?;
    let budget = Arc::new(Mutex::new(TransitionBudgetState::new(
        WasmTransitionLimits::default(),
    )?));
    set_transition_deadline(store, &budget, component_limits)
}

fn descriptor_to_binding(
    descriptor: &WasmFileDescriptor,
) -> bindings::exports::lix::plugin::api::FileDescriptor {
    bindings::exports::lix::plugin::api::FileDescriptor {
        path: descriptor.path.clone(),
        media_type: descriptor.media_type.clone(),
        plugin: bindings::exports::lix::plugin::api::PluginSelection {
            plugin_key: descriptor.plugin.plugin_key.clone(),
            generation: descriptor.plugin.generation.clone(),
        },
    }
}

fn ids_to_binding(ids: WasmIdNamespace) -> bindings::lix::plugin::host::IdNamespace {
    bindings::lix::plugin::host::IdNamespace {
        high: ids.high,
        low: ids.low,
    }
}

#[async_trait]
impl WasmComponentV2Actor for WasmtimeV2Actor {
    async fn fork_document(
        &mut self,
        document: WasmDocumentHandle,
    ) -> Result<WasmDocumentHandle, LixError> {
        let resource = *self
            .documents
            .get(&document.0)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 document handle"))?;
        self.prepare_standalone_guest_call()?;
        let guest = self.guest.clone();
        let result = guest.document().call_fork(self.store_mut()?, resource);
        let fork = match result {
            Ok(fork) => fork,
            Err(error) => {
                return Err(self.retire_with_error("v2 document fork trapped", error));
            }
        };
        let handle = self.allocate_handle()?;
        self.documents.insert(handle, fork);
        Ok(WasmDocumentHandle(handle))
    }

    async fn open_file(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenFileInput,
    ) -> Result<WasmFileTransition, LixError> {
        let (transition, _budget_resource) = self.begin_transition(limits, None)?;
        let budget = self.transition_budget(transition)?;
        let file = self.push_byte_source(input.file, &budget)?;
        let binding_input = bindings::exports::lix::plugin::api::OpenFileInput {
            descriptor: descriptor_to_binding(&input.descriptor),
            file,
            ids: ids_to_binding(input.ids),
        };
        let guest = self.guest.clone();
        let budget_rep = match self.prepare_nested_call(transition) {
            Ok(budget_rep) => budget_rep,
            Err(error) => {
                self.retire_now();
                return Err(error);
            }
        };
        let result = guest.call_open_file(
            self.store_mut()?,
            Resource::new_borrow(budget_rep),
            &binding_input,
        );
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let error = self.plugin_error("open-file", error);
                self.retire_now();
                return Err(error);
            }
            Err(error) => return Err(self.retire_with_error("v2 open-file trapped", error)),
        };
        self.register_file_transition(transition, value)
    }

    async fn open_entities(
        &mut self,
        limits: WasmTransitionLimits,
        input: WasmOpenEntitiesInput,
    ) -> Result<WasmEntityTransition, LixError> {
        let (transition, _budget_resource) = self.begin_transition(limits, None)?;
        let budget = self.transition_budget(transition)?;
        let entities =
            self.push_packet_source(PacketSourceValue::Entities(input.entities), &budget)?;
        let binding_input = bindings::exports::lix::plugin::api::OpenEntitiesInput {
            descriptor: descriptor_to_binding(&input.descriptor),
            entities,
        };
        let guest = self.guest.clone();
        let budget_rep = match self.prepare_nested_call(transition) {
            Ok(budget_rep) => budget_rep,
            Err(error) => {
                self.retire_now();
                return Err(error);
            }
        };
        let result = guest.call_open_entities(
            self.store_mut()?,
            Resource::new_borrow(budget_rep),
            &binding_input,
        );
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                let error = self.plugin_error("open-entities", error);
                self.retire_now();
                return Err(error);
            }
            Err(error) => return Err(self.retire_with_error("v2 open-entities trapped", error)),
        };
        self.register_entity_transition(transition, 0, value)
    }

    async fn file_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmFileUpdate,
    ) -> Result<WasmFileTransition, LixError> {
        update.validate(limits)?;
        let document_resource = *self
            .documents
            .get(&document.0)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 document handle"))?;
        let (transition, _budget_resource) = self.begin_transition(limits, Some(document.0))?;
        let budget = self.transition_budget(transition)?;
        let before = self.push_byte_source(update.before, &budget)?;
        let after = self.push_byte_source(update.after, &budget)?;
        let edits = update
            .edits
            .into_iter()
            .map(|edit| bindings::exports::lix::plugin::api::InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: match edit.insert {
                    WasmInputBytes::Inline(bytes) => {
                        bindings::exports::lix::plugin::api::InputBytes::Inline(bytes)
                    }
                    WasmInputBytes::AfterRange(range) => {
                        bindings::exports::lix::plugin::api::InputBytes::AfterRange(
                            bindings::exports::lix::plugin::api::SourceRange {
                                offset: range.offset,
                                length: range.length,
                            },
                        )
                    }
                },
            })
            .collect();
        let binding_update = bindings::exports::lix::plugin::api::FileUpdate {
            before_descriptor: descriptor_to_binding(&update.before_descriptor),
            after_descriptor: descriptor_to_binding(&update.after_descriptor),
            before,
            edits,
            after,
            ids: ids_to_binding(update.ids),
        };
        let guest = self.guest.clone();
        let budget_rep = match self.prepare_nested_call(transition) {
            Ok(budget_rep) => budget_rep,
            Err(error) => {
                self.retire_now();
                return Err(error);
            }
        };
        let result = guest.document().call_file_changed(
            self.store_mut()?,
            document_resource,
            Resource::new_borrow(budget_rep),
            &binding_update,
        );
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                return Err(self.handle_returned_plugin_error(transition, "file-changed", error));
            }
            Err(error) => return Err(self.retire_with_error("v2 file-changed trapped", error)),
        };
        self.register_file_transition(transition, value)
    }

    async fn entities_changed(
        &mut self,
        document: WasmDocumentHandle,
        limits: WasmTransitionLimits,
        update: WasmEntityUpdate,
    ) -> Result<WasmEntityTransition, LixError> {
        update
            .before_descriptor
            .validate_warm_successor(&update.after_descriptor)?;
        let document_resource = *self
            .documents
            .get(&document.0)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 document handle"))?;
        let base_len = update.before.len();
        let (transition, _budget_resource) = self.begin_transition(limits, Some(document.0))?;
        let budget = self.transition_budget(transition)?;
        let before = self.push_byte_source(update.before, &budget)?;
        let changes =
            self.push_packet_source(PacketSourceValue::Changes(update.changes), &budget)?;
        let activated_entities = self.push_packet_source(
            PacketSourceValue::Entities(update.activated_entities),
            &budget,
        )?;
        let current_entities = self.push_packet_source(
            PacketSourceValue::Entities(update.current_entities),
            &budget,
        )?;
        let binding_update = bindings::exports::lix::plugin::api::EntityUpdate {
            before_descriptor: descriptor_to_binding(&update.before_descriptor),
            after_descriptor: descriptor_to_binding(&update.after_descriptor),
            before,
            changes,
            activated_entities,
            current_entities,
        };
        let guest = self.guest.clone();
        let budget_rep = match self.prepare_nested_call(transition) {
            Ok(budget_rep) => budget_rep,
            Err(error) => {
                self.retire_now();
                return Err(error);
            }
        };
        let result = guest.document().call_entities_changed(
            self.store_mut()?,
            document_resource,
            Resource::new_borrow(budget_rep),
            &binding_update,
        );
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                return Err(self.handle_returned_plugin_error(
                    transition,
                    "entities-changed",
                    error,
                ));
            }
            Err(error) => {
                return Err(self.retire_with_error("v2 entities-changed trapped", error));
            }
        };
        self.register_entity_transition(transition, base_len, value)
    }

    async fn next_change_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmChangeCursorHandle,
        max_bytes: u32,
    ) -> Result<Option<WasmChangePage>, LixError> {
        let (resource, cursor_transition, eof) = self
            .change_cursors
            .get(&cursor.0)
            .map(|cursor| (cursor.resource, cursor.transition, cursor.eof))
            .ok_or_else(|| v2_invalid_plugin("unknown v2 change cursor handle"))?;
        if cursor_transition != transition.0 {
            return Err(v2_invalid_plugin(
                "v2 change cursor belongs to a different transition",
            ));
        }
        if eof {
            return Ok(None);
        }
        let budget_rep = self.prepare_nested_call(transition.0)?;
        let guest = self.guest.clone();
        let result = guest.change_cursor().call_next(
            self.store_mut()?,
            resource,
            Resource::new_borrow(budget_rep),
            max_bytes,
        );
        let page = match result {
            Ok(Ok(Some(page))) => page,
            Ok(Ok(None)) => {
                let cursor = self
                    .change_cursors
                    .get_mut(&cursor.0)
                    .expect("cursor was checked above");
                cursor.eof = true;
                cursor.validator.accept_eof();
                return Ok(None);
            }
            Ok(Err(error)) => {
                return Err(self.handle_returned_plugin_error(
                    transition.0,
                    "change-cursor.next",
                    error,
                ));
            }
            Err(error) => {
                return Err(self.retire_with_error("v2 change-cursor.next trapped", error));
            }
        };
        if page.format_version != PACKET_FORMAT_V1 {
            self.retire_now();
            return Err(v2_invalid_plugin("unsupported v2 guest packet version"));
        }
        let limits = self
            .transition_budget(transition.0)?
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits;
        let decoded = decode_change_packet(page.record_count, &page.payload, limits)?;
        let reference_count = checked_u32(decoded.output_ranges.len(), "output reference count")?;
        if (reference_count == 0) == page.attachments.is_some() {
            self.retire_now();
            return Err(v2_invalid_plugin(
                "v2 change page output table presence does not match its references",
            ));
        }
        let budget = self.transition_budget(transition.0)?;
        {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_attachment_refs(reference_count)?;
            state.charge_page(page.payload.len() as u64)?;
            state.counters.packet_pages = state.counters.packet_pages.saturating_add(1);
            state.counters.packet_records = state
                .counters
                .packet_records
                .saturating_add(u64::from(page.record_count));
        }
        let outputs = if let Some(resource) = page.attachments {
            let lengths =
                self.validate_output_ranges(transition.0, resource, &decoded.output_ranges)?;
            let handle = self.allocate_handle()?;
            self.outputs.insert(
                handle,
                OutputState {
                    resource,
                    transition: transition.0,
                    lengths,
                },
            );
            Some(WasmByteOutputsHandle(handle))
        } else {
            None
        };
        let result = WasmChangePage {
            format_version: page.format_version,
            changes: decoded.changes,
            outputs,
        };
        if let Err(error) = self
            .change_cursors
            .get_mut(&cursor.0)
            .expect("cursor was checked above")
            .validator
            .accept_page(&result)
        {
            self.retire_now();
            return Err(error);
        }
        Ok(Some(result))
    }

    async fn next_edit_page(
        &mut self,
        transition: WasmTransitionHandle,
        cursor: WasmEditCursorHandle,
        max_edits: u32,
        max_inline_bytes: u32,
    ) -> Result<Option<WasmEditPage>, LixError> {
        let (resource, cursor_transition, eof) = self
            .edit_cursors
            .get(&cursor.0)
            .map(|cursor| (cursor.resource, cursor.transition, cursor.eof))
            .ok_or_else(|| v2_invalid_plugin("unknown v2 edit cursor handle"))?;
        if cursor_transition != transition.0 {
            return Err(v2_invalid_plugin(
                "v2 edit cursor belongs to a different transition",
            ));
        }
        if eof {
            return Ok(None);
        }
        let budget_rep = self.prepare_nested_call(transition.0)?;
        let guest = self.guest.clone();
        let result = guest.edit_cursor().call_next(
            self.store_mut()?,
            resource,
            Resource::new_borrow(budget_rep),
            max_edits,
            max_inline_bytes,
        );
        let page = match result {
            Ok(Ok(Some(page))) => page,
            Ok(Ok(None)) => {
                let cursor = self
                    .edit_cursors
                    .get_mut(&cursor.0)
                    .expect("cursor was checked above");
                cursor.eof = true;
                cursor.validator.accept_eof();
                return Ok(None);
            }
            Ok(Err(error)) => {
                return Err(self.handle_returned_plugin_error(
                    transition.0,
                    "edit-cursor.next",
                    error,
                ));
            }
            Err(error) => {
                return Err(self.retire_with_error("v2 edit-cursor.next trapped", error));
            }
        };
        if page.edits.is_empty() || page.edits.len() > max_edits as usize {
            self.retire_now();
            return Err(v2_invalid_plugin(
                "v2 guest returned an invalid edit page size",
            ));
        }
        let mut ranges = Vec::new();
        let mut inline_bytes = 0u64;
        let edits = page
            .edits
            .into_iter()
            .map(|edit| {
                let insert = match edit.insert {
                    bindings::exports::lix::plugin::api::OutputBytes::Inline(bytes) => {
                        inline_bytes = inline_bytes.saturating_add(bytes.len() as u64);
                        WasmGuestBytes::Inline(bytes)
                    }
                    bindings::exports::lix::plugin::api::OutputBytes::Output(range) => {
                        let range = WasmOutputRange {
                            index: range.index,
                            offset: range.offset,
                            length: range.length,
                        };
                        ranges.push(range);
                        WasmGuestBytes::Output(range)
                    }
                };
                WasmOutputSplice {
                    offset: edit.offset,
                    delete_len: edit.delete_len,
                    insert,
                }
            })
            .collect::<Vec<_>>();
        if inline_bytes > u64::from(max_inline_bytes) {
            self.retire_now();
            return Err(v2_limit("v2 guest edit page exceeds max-inline-bytes"));
        }
        let reference_count = checked_u32(ranges.len(), "output reference count")?;
        if (reference_count == 0) == page.outputs.is_some() {
            self.retire_now();
            return Err(v2_invalid_plugin(
                "v2 edit page output table presence does not match its references",
            ));
        }
        let budget = self.transition_budget(transition.0)?;
        {
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_attachment_refs(reference_count)?;
            state.charge_page(inline_bytes.saturating_add((edits.len() as u64) * 24))?;
        }
        let outputs = if let Some(resource) = page.outputs {
            let lengths = self.validate_output_ranges(transition.0, resource, &ranges)?;
            let handle = self.allocate_handle()?;
            self.outputs.insert(
                handle,
                OutputState {
                    resource,
                    transition: transition.0,
                    lengths,
                },
            );
            Some(WasmByteOutputsHandle(handle))
        } else {
            None
        };
        let result = WasmEditPage { edits, outputs };
        if let Err(error) = self
            .edit_cursors
            .get_mut(&cursor.0)
            .expect("cursor was checked above")
            .validator
            .accept_page(&result)
        {
            self.retire_now();
            return Err(error);
        }
        Ok(Some(result))
    }

    async fn output_len(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
    ) -> Result<u64, LixError> {
        let (resource, owner, cached) = self
            .outputs
            .get(&outputs.0)
            .map(|output| {
                (
                    output.resource,
                    output.transition,
                    output.lengths.get(&index).copied(),
                )
            })
            .ok_or_else(|| v2_invalid_plugin("unknown v2 output table handle"))?;
        if owner != transition.0 {
            return Err(v2_invalid_plugin(
                "v2 output table belongs to a different transition",
            ));
        }
        if let Some(length) = cached {
            return Ok(length);
        }
        self.prepare_nested_call(transition.0)?;
        let guest = self.guest.clone();
        let result = guest
            .byte_outputs()
            .call_len(self.store_mut()?, resource, index);
        let length = match result {
            Ok(Ok(length)) => length,
            Ok(Err(error)) => {
                let error = self.plugin_error("byte-outputs.len", error);
                self.retire_now();
                return Err(error);
            }
            Err(error) => {
                return Err(self.retire_with_error("v2 byte-outputs.len trapped", error));
            }
        };
        self.outputs
            .get_mut(&outputs.0)
            .expect("output table was checked above")
            .lengths
            .insert(index, length);
        Ok(length)
    }

    async fn read_output(
        &mut self,
        transition: WasmTransitionHandle,
        outputs: WasmByteOutputsHandle,
        index: u32,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, LixError> {
        let total_len = self.output_len(transition, outputs, index).await?;
        let end = offset
            .checked_add(u64::from(length))
            .ok_or_else(|| v2_invalid_plugin("v2 output read range overflowed"))?;
        if end > total_len {
            return Err(v2_invalid_plugin("v2 output read is out of range"));
        }
        let (resource, owner) = self
            .outputs
            .get(&outputs.0)
            .map(|output| (output.resource, output.transition))
            .ok_or_else(|| v2_invalid_plugin("unknown v2 output table handle"))?;
        if owner != transition.0 {
            return Err(v2_invalid_plugin(
                "v2 output table belongs to a different transition",
            ));
        }
        let budget_rep = self.prepare_nested_call(transition.0)?;
        let guest = self.guest.clone();
        let result = guest.byte_outputs().call_read(
            self.store_mut()?,
            resource,
            Resource::new_borrow(budget_rep),
            index,
            offset,
            length,
        );
        let bytes = match result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                let error = self.plugin_error("byte-outputs.read", error);
                self.retire_now();
                return Err(error);
            }
            Err(error) => {
                return Err(self.retire_with_error("v2 byte-outputs.read trapped", error));
            }
        };
        if bytes.len() > length as usize || (length > 0 && offset < total_len && bytes.is_empty()) {
            self.retire_now();
            return Err(v2_invalid_plugin(
                "v2 output read did not obey bounded progress",
            ));
        }
        if !bytes.is_empty() {
            let budget = self.transition_budget(transition.0)?;
            let mut state = budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.charge_page(bytes.len() as u64)?;
            state.counters.attachment_reads = state.counters.attachment_reads.saturating_add(1);
            state.counters.attachment_bytes_read = state
                .counters
                .attachment_bytes_read
                .saturating_add(bytes.len() as u64);
        }
        Ok(bytes)
    }

    async fn finish_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<WasmTransitionCounters, LixError> {
        let active = self
            .transitions
            .remove(&transition.0)
            .ok_or_else(|| v2_invalid_plugin("unknown v2 transition handle"))?;
        if self
            .change_cursors
            .values()
            .any(|cursor| cursor.transition == transition.0 && !cursor.eof)
            || self
                .edit_cursors
                .values()
                .any(|cursor| cursor.transition == transition.0 && !cursor.eof)
        {
            self.retire_now();
            return Err(v2_invalid_plugin(
                "v2 transition finished before its cursor reached EOF",
            ));
        }
        let change_resources = self
            .change_cursors
            .extract_if(|_, cursor| cursor.transition == transition.0)
            .map(|(_, cursor)| cursor.resource)
            .collect::<Vec<_>>();
        let edit_resources = self
            .edit_cursors
            .extract_if(|_, cursor| cursor.transition == transition.0)
            .map(|(_, cursor)| cursor.resource)
            .collect::<Vec<_>>();
        let output_resources = self
            .outputs
            .extract_if(|_, output| output.transition == transition.0)
            .map(|(_, output)| output.resource)
            .collect::<Vec<_>>();
        for resource in change_resources
            .into_iter()
            .chain(edit_resources)
            .chain(output_resources)
        {
            self.prepare_transition_resource_drop(&active.budget)?;
            if let Err(error) = resource.resource_drop(self.store_mut()?) {
                return Err(self.retire_with_error("failed to drop v2 guest resource", error));
            }
        }
        let guest_linear_memory_high_water_bytes = self
            .store
            .as_ref()
            .ok_or_else(|| v2_invalid_plugin("v2 plugin actor has been retired"))?
            .data()
            .limits
            .linear_memory_high_water_bytes();
        let counters = {
            let mut state = active
                .budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.check_active().map(|()| {
                state.finished = true;
                state.counters.guest_linear_memory_high_water_bytes = state
                    .counters
                    .guest_linear_memory_high_water_bytes
                    .max(guest_linear_memory_high_water_bytes);
                state.counters
            })
        };
        let counters = match counters {
            Ok(counters) => counters,
            Err(error) => {
                self.retire_now();
                return Err(error);
            }
        };
        let result = self.store_mut()?.data_mut().table.delete(
            Resource::<TransitionBudgetResource>::new_own(active.budget_rep),
        );
        if let Err(error) = result {
            return Err(self.retire_with_error("failed to release v2 transition budget", error));
        }
        Ok(counters)
    }

    async fn discard_transition(
        &mut self,
        transition: WasmTransitionHandle,
    ) -> Result<(), LixError> {
        WasmtimeV2Actor::discard_transition(self, transition.0)
    }

    fn is_retired(&self) -> bool {
        self.store.is_none()
    }

    async fn drop_document(&mut self, document: WasmDocumentHandle) -> Result<(), LixError> {
        if !self.documents.contains_key(&document.0) {
            return Err(v2_invalid_plugin("unknown v2 document handle"));
        }
        self.prepare_standalone_guest_call()?;
        let resource = self
            .documents
            .remove(&document.0)
            .expect("v2 document handle was checked before standalone preparation");
        if let Err(error) = resource.resource_drop(self.store_mut()?) {
            return Err(self.retire_with_error("failed to drop v2 document", error));
        }
        Ok(())
    }

    async fn retire(&mut self) -> Result<(), LixError> {
        self.retire_now();
        Ok(())
    }
}

impl WasmtimeV2Actor {
    fn validate_output_ranges(
        &mut self,
        transition: u64,
        resource: ResourceAny,
        ranges: &[WasmOutputRange],
    ) -> Result<HashMap<u32, u64>, LixError> {
        let mut lengths = HashMap::new();
        let guest = self.guest.clone();
        for range in ranges {
            let length = if let Some(length) = lengths.get(&range.index) {
                *length
            } else {
                self.prepare_nested_call(transition)?;
                match guest
                    .byte_outputs()
                    .call_len(self.store_mut()?, resource, range.index)
                {
                    Ok(Ok(length)) => {
                        lengths.insert(range.index, length);
                        length
                    }
                    Ok(Err(error)) => {
                        let error = self.plugin_error("byte-outputs.len", error);
                        self.retire_now();
                        return Err(error);
                    }
                    Err(error) => {
                        return Err(self.retire_with_error("v2 byte-outputs.len trapped", error));
                    }
                }
            };
            let end = range
                .offset
                .checked_add(range.length)
                .ok_or_else(|| v2_invalid_plugin("v2 output range overflowed"))?;
            if end > length {
                self.retire_now();
                return Err(v2_invalid_plugin("v2 output range is out of bounds"));
            }
        }
        Ok(lengths)
    }
}
