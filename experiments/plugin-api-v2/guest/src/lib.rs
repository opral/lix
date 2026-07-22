//! A deliberately small core-Wasm guest used to isolate plugin API data movement.
//!
//! The host never calls a native implementation of the four candidate paths.  It
//! copies requests into this module's linear memory and invokes `run`.  The
//! persistent candidates retain their document in this module between calls.

use std::cell::RefCell;
use std::mem::forget;
use std::slice;

const CHECKPOINT_MAGIC: &[u8; 8] = b"LIXAPIV2";

const OP_INIT_PERSISTENT: i32 = 0;
const OP_STATELESS_V1: i32 = 1;
const OP_FULL_FILE_PERSISTENT: i32 = 2;
const OP_SPLICE_PERSISTENT: i32 = 3;
const OP_CHECKPOINT_REDUCER: i32 = 4;
const OP_SNAPSHOT: i32 = 5;
const OP_OUTCOME: i32 = 6;
const OP_CREATE_CHECKPOINT: i32 = 7;
const OP_STATELESS_V1_CHECKPOINT: i32 = 8;
const OP_HOST_CONTEXT_FINE: i32 = 9;
const OP_HOST_CONTEXT_BATCHED: i32 = 10;
const OP_INIT_HYBRID_INDEX: i32 = 11;
const OP_HYBRID_INDEX_EDIT: i32 = 12;

#[link(wasm_import_module = "lix_context")]
unsafe extern "C" {
    fn context_entity_at_offset(offset: i32) -> i32;
    fn context_entity_count() -> i32;
    fn context_entity_id(index: i32) -> i64;
    fn context_entity_start(view: i32, index: i32) -> i32;
    fn context_entity_end(view: i32, index: i32) -> i32;
    fn context_source_byte(view: i32, offset: i32) -> i32;
    fn context_source_read(view: i32, offset: i32, pointer: i32, len: i32) -> i32;
    fn context_source_len(view: i32) -> i32;
}

thread_local! {
    static DOCUMENT: RefCell<Option<Document>> = const { RefCell::new(None) };
    static HYBRID_INDEX: RefCell<Option<HybridIndex>> = const { RefCell::new(None) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Format {
    Csv = 0,
    Markdown = 1,
    Json = 2,
    Excalidraw = 3,
    Text = 4,
}

impl Format {
    fn decode(value: u8) -> Self {
        match value {
            0 => Self::Csv,
            1 => Self::Markdown,
            2 => Self::Json,
            3 => Self::Excalidraw,
            4 => Self::Text,
            _ => panic!("unknown format {value}"),
        }
    }

    fn has_intrinsic_ids(self) -> bool {
        matches!(self, Self::Json | Self::Excalidraw)
    }
}

#[derive(Clone, Debug)]
struct Entity {
    id: u64,
    start: u32,
    end: u32,
}

#[derive(Clone, Debug)]
struct IndexedEntity {
    id: u64,
    hash: u64,
    start: u32,
    end: u32,
}

#[derive(Clone, Debug)]
struct HybridIndex {
    entities: Vec<IndexedEntity>,
    revision: u64,
}

#[derive(Clone, Debug)]
struct Document {
    format: Format,
    bytes: Vec<u8>,
    entities: Vec<Entity>,
    next_id: u64,
    last_changed_id: u64,
    revision: u64,
}

impl Document {
    fn parse(format: Format, bytes: Vec<u8>) -> Self {
        assert!(u32::try_from(bytes.len()).is_ok(), "fixture exceeds 4 GiB");
        let mut entities = scan_entities(format, &bytes);
        let mut next_id = 1_u64;
        if !format.has_intrinsic_ids() {
            for entity in &mut entities {
                entity.id = allocated_id(format, next_id);
                next_id += 1;
            }
        }
        Self {
            format,
            bytes,
            entities,
            next_id,
            last_changed_id: 0,
            revision: 0,
        }
    }

    fn replace_full_file(&mut self, bytes: Vec<u8>) {
        assert!(u32::try_from(bytes.len()).is_ok(), "fixture exceeds 4 GiB");
        let mut fresh = scan_entities(self.format, &bytes);
        let old_len = self.entities.len();
        let new_len = fresh.len();

        let mut prefix = 0;
        while prefix < old_len && prefix < new_len {
            let old = &self.entities[prefix];
            let new = &fresh[prefix];
            if entity_bytes(&self.bytes, old) != entity_bytes(&bytes, new) {
                break;
            }
            prefix += 1;
        }

        let mut suffix = 0;
        while suffix < old_len.saturating_sub(prefix) && suffix < new_len.saturating_sub(prefix) {
            let old = &self.entities[old_len - suffix - 1];
            let new = &fresh[new_len - suffix - 1];
            if entity_bytes(&self.bytes, old) != entity_bytes(&bytes, new) {
                break;
            }
            suffix += 1;
        }

        if !self.format.has_intrinsic_ids() {
            #[allow(clippy::needless_range_loop)]
            for index in 0..prefix {
                fresh[index].id = self.entities[index].id;
            }
            for offset in 0..suffix {
                fresh[new_len - offset - 1].id = self.entities[old_len - offset - 1].id;
            }

            let old_middle = old_len.saturating_sub(prefix + suffix);
            let new_middle = new_len.saturating_sub(prefix + suffix);
            if old_middle == new_middle {
                for offset in 0..new_middle {
                    fresh[prefix + offset].id = self.entities[prefix + offset].id;
                }
            } else {
                for entity in fresh
                    .iter_mut()
                    .skip(prefix)
                    .take(new_middle)
                    .filter(|entity| entity.id == 0)
                {
                    entity.id = allocated_id(self.format, self.next_id);
                    self.next_id += 1;
                }
            }
        }

        self.last_changed_id = if prefix < fresh.len() {
            fresh[prefix].id
        } else if prefix > 0 {
            fresh[prefix - 1].id
        } else {
            0
        };
        self.bytes = bytes;
        self.entities = fresh;
        self.revision = self.revision.wrapping_add(1);
    }

    fn apply_splice(&mut self, request: &[u8]) {
        let start = usize::try_from(read_u64(request, 0)).expect("splice start fits usize");
        let delete_len =
            usize::try_from(read_u64(request, 8)).expect("splice delete length fits usize");
        let insert = request.get(16..).expect("splice payload");
        let end = start.checked_add(delete_len).expect("splice end overflow");
        assert!(end <= self.bytes.len(), "splice outside document");

        let entity_index = self
            .entities
            .iter()
            .position(|entity| {
                let entity_start = entity.start as usize;
                let entity_end = entity.end as usize;
                start >= entity_start && end <= entity_end
            })
            .expect("prototype only accepts a splice contained by one entity");

        self.bytes.splice(start..end, insert.iter().copied());
        let delta = isize::try_from(insert.len()).expect("insert length fits isize")
            - isize::try_from(delete_len).expect("delete length fits isize");
        let changed_id = self.entities[entity_index].id;
        self.entities[entity_index].end = shift_u32(self.entities[entity_index].end, delta);
        if delta != 0 {
            for entity in self.entities.iter_mut().skip(entity_index + 1) {
                entity.start = shift_u32(entity.start, delta);
                entity.end = shift_u32(entity.end, delta);
            }
        }
        self.last_changed_id = changed_id;
        self.revision = self.revision.wrapping_add(1);
    }

    fn outcome(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(32);
        push_u64(&mut output, self.entities.len() as u64);
        push_u64(&mut output, self.last_changed_id);
        push_u64(&mut output, self.bytes.len() as u64);
        push_u64(&mut output, self.revision);
        output
    }

    fn encode_checkpoint(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(56 + self.bytes.len() + self.entities.len() * 16);
        output.extend_from_slice(CHECKPOINT_MAGIC);
        output.push(self.format as u8);
        output.extend_from_slice(&[0; 7]);
        push_u64(&mut output, self.next_id);
        push_u64(&mut output, self.last_changed_id);
        push_u64(&mut output, self.revision);
        push_u64(&mut output, self.bytes.len() as u64);
        push_u64(&mut output, self.entities.len() as u64);
        output.extend_from_slice(&self.bytes);
        for entity in &self.entities {
            push_u64(&mut output, entity.id);
            output.extend_from_slice(&entity.start.to_le_bytes());
            output.extend_from_slice(&entity.end.to_le_bytes());
        }
        output
    }

    fn decode_checkpoint(input: &[u8]) -> Self {
        assert_eq!(input.get(0..8), Some(CHECKPOINT_MAGIC.as_slice()));
        let format = Format::decode(input[8]);
        let next_id = read_u64(input, 16);
        let last_changed_id = read_u64(input, 24);
        let revision = read_u64(input, 32);
        let file_len = usize::try_from(read_u64(input, 40)).expect("file length fits usize");
        let entity_count = usize::try_from(read_u64(input, 48)).expect("entity count fits usize");
        let file_end = 56_usize.checked_add(file_len).expect("checkpoint overflow");
        let bytes = input.get(56..file_end).expect("checkpoint file").to_vec();
        let records = input.get(file_end..).expect("checkpoint records");
        assert_eq!(records.len(), entity_count * 16);
        let mut entities = Vec::with_capacity(entity_count);
        for record in records.chunks_exact(16) {
            entities.push(Entity {
                id: read_u64(record, 0),
                start: u32::from_le_bytes(record[8..12].try_into().expect("entity start")),
                end: u32::from_le_bytes(record[12..16].try_into().expect("entity end")),
            });
        }
        Self {
            format,
            bytes,
            entities,
            next_id,
            last_changed_id,
            revision,
        }
    }
}

impl HybridIndex {
    fn hydrate(format: Format) -> Self {
        let mut entities = scan_host_entities(format, 1);
        let mut next_id = 1_u64;
        if !format.has_intrinsic_ids() {
            for entity in &mut entities {
                entity.id = allocated_id(format, next_id);
                next_id += 1;
            }
        }
        Self {
            entities,
            revision: 0,
        }
    }

    fn apply_splice(&mut self, request: &[u8]) -> Vec<u8> {
        let start = usize::try_from(read_u64(request, 0)).expect("splice start fits usize");
        let delete_len =
            usize::try_from(read_u64(request, 8)).expect("splice delete length fits usize");
        let insert_len = request.len().checked_sub(16).expect("splice payload");
        let end = start.checked_add(delete_len).expect("splice end overflow");
        let entity_index = self
            .entities
            .partition_point(|entity| entity.start as usize <= start)
            .checked_sub(1)
            .filter(|index| end <= self.entities[*index].end as usize)
            .expect("hybrid splice must be contained by one indexed entity");
        let entity = &self.entities[entity_index];
        let before_start = entity.start as usize;
        let before_end = entity.end as usize;
        let before = read_host_range_batched(0, before_start, before_end);
        assert_eq!(
            fnv1a(&before),
            entity.hash,
            "hybrid before-source/index drift"
        );

        let delta = isize::try_from(insert_len).expect("insert length fits isize")
            - isize::try_from(delete_len).expect("delete length fits isize");
        let after_end = usize::try_from(
            isize::try_from(before_end)
                .expect("entity end fits isize")
                .checked_add(delta)
                .expect("entity end overflow"),
        )
        .expect("entity end remains positive");
        let after = read_host_range_batched(1, before_start, after_end);
        let after_hash = fnv1a(&after);
        assert!(
            before != after,
            "hybrid source edit must change its semantic entity"
        );

        let changed_id = self.entities[entity_index].id;
        self.entities[entity_index].end = u32::try_from(after_end).expect("entity end fits u32");
        self.entities[entity_index].hash = after_hash;
        if delta != 0 {
            for entity in self.entities.iter_mut().skip(entity_index + 1) {
                entity.start = shift_u32(entity.start, delta);
                entity.end = shift_u32(entity.end, delta);
            }
        }
        self.revision = self.revision.wrapping_add(1);

        let mut output = Vec::with_capacity(24 + after.len());
        push_u64(&mut output, self.entities.len() as u64);
        push_u64(&mut output, changed_id);
        push_u64(&mut output, after.len() as u64);
        output.extend_from_slice(&after);
        output
    }
}

fn allocated_id(format: Format, sequence: u64) -> u64 {
    let namespace = [format as u8, b'l', b'i', b'x'];
    fnv1a_continue(fnv1a(&namespace), &sequence.to_le_bytes())
}

fn entity_bytes<'a>(bytes: &'a [u8], entity: &Entity) -> &'a [u8] {
    &bytes[entity.start as usize..entity.end as usize]
}

fn shift_u32(value: u32, delta: isize) -> u32 {
    let shifted = isize::try_from(value).expect("offset fits isize") + delta;
    u32::try_from(shifted).expect("shifted offset fits u32")
}

fn scan_entities(format: Format, bytes: &[u8]) -> Vec<Entity> {
    match format {
        Format::Csv | Format::Text => scan_delimited(bytes, b"\n", false, format),
        Format::Markdown => scan_delimited(bytes, b"\n\n", false, format),
        Format::Json => scan_delimited(bytes, b"\n", true, format),
        Format::Excalidraw => scan_delimited(bytes, b"\n", true, format),
    }
}

fn scan_delimited(bytes: &[u8], delimiter: &[u8], intrinsic: bool, format: Format) -> Vec<Entity> {
    let mut entities = Vec::new();
    let mut start = 0_usize;
    while start < bytes.len() {
        let relative_end = find_subslice(&bytes[start..], delimiter);
        let end = relative_end.map_or(bytes.len(), |offset| start + offset);
        let line = &bytes[start..end];
        if is_entity_line(format, line) {
            let id = if intrinsic {
                intrinsic_id(format, line)
            } else {
                0
            };
            entities.push(Entity {
                id,
                start: u32::try_from(start).expect("entity start fits u32"),
                end: u32::try_from(end).expect("entity end fits u32"),
            });
        }
        if end == bytes.len() {
            break;
        }
        start = end + delimiter.len();
    }
    entities
}

fn is_entity_line(format: Format, line: &[u8]) -> bool {
    if line.is_empty() {
        return false;
    }
    match format {
        Format::Csv | Format::Markdown | Format::Text => true,
        Format::Json => {
            let trimmed = trim_ascii_start(line);
            trimmed.starts_with(b"\"") && trimmed.windows(2).any(|window| window == b"\":")
        }
        Format::Excalidraw => {
            let trimmed = trim_ascii_start(line);
            trimmed.starts_with(b"{") && find_subslice(trimmed, b"\"id\":\"").is_some()
        }
    }
}

fn intrinsic_id(format: Format, line: &[u8]) -> u64 {
    match format {
        Format::Json => {
            let trimmed = trim_ascii_start(line);
            let key_end = trimmed[1..]
                .iter()
                .position(|byte| *byte == b'"')
                .map(|offset| offset + 1)
                .expect("JSON fixture key");
            fnv1a(&trimmed[1..key_end])
        }
        Format::Excalidraw => {
            let marker = b"\"id\":\"";
            let marker_start = find_subslice(line, marker).expect("Excalidraw fixture id");
            let id_start = marker_start + marker.len();
            let id_end = line[id_start..]
                .iter()
                .position(|byte| *byte == b'"')
                .map(|offset| id_start + offset)
                .expect("Excalidraw fixture id end");
            fnv1a(&line[id_start..id_end])
        }
        _ => unreachable!("only intrinsic formats call intrinsic_id"),
    }
}

fn trim_ascii_start(mut bytes: &[u8]) -> &[u8] {
    while matches!(bytes.first(), Some(b' ' | b'\t' | b'\r')) {
        bytes = &bytes[1..];
    }
    bytes
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_continue(FNV_OFFSET, bytes)
}

fn fnv1a_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("eight-byte field"),
    )
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn init_persistent(input: &[u8]) -> Vec<u8> {
    let format = Format::decode(*input.first().expect("format byte"));
    DOCUMENT.with(|slot| {
        let document = Document::parse(format, input[1..].to_vec());
        let outcome = document.outcome();
        *slot.borrow_mut() = Some(document);
        outcome
    })
}

fn stateless_v1(input: &[u8]) -> Vec<u8> {
    decode_and_replace_stateless(input).outcome()
}

fn stateless_v1_checkpoint(input: &[u8]) -> Vec<u8> {
    decode_and_replace_stateless(input).encode_checkpoint()
}

fn decode_and_replace_stateless(input: &[u8]) -> Document {
    let checkpoint_len = usize::try_from(read_u64(input, 0)).expect("checkpoint length");
    let checkpoint_end = 8 + checkpoint_len;
    let mut document = Document::decode_checkpoint(&input[8..checkpoint_end]);
    document.replace_full_file(input[checkpoint_end..].to_vec());
    document
}

fn full_file_persistent(input: &[u8]) -> Vec<u8> {
    DOCUMENT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let document = slot.as_mut().expect("persistent document initialized");
        document.replace_full_file(input.to_vec());
        document.outcome()
    })
}

fn splice_persistent(input: &[u8]) -> Vec<u8> {
    DOCUMENT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let document = slot.as_mut().expect("persistent document initialized");
        document.apply_splice(input);
        document.outcome()
    })
}

fn checkpoint_reducer(input: &[u8]) -> Vec<u8> {
    let checkpoint_len = usize::try_from(read_u64(input, 0)).expect("checkpoint length");
    let checkpoint_end = 8 + checkpoint_len;
    let mut document = Document::decode_checkpoint(&input[8..checkpoint_end]);
    document.apply_splice(&input[checkpoint_end..]);
    document.encode_checkpoint()
}

fn snapshot() -> Vec<u8> {
    DOCUMENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .expect("persistent document initialized")
            .encode_checkpoint()
    })
}

fn outcome() -> Vec<u8> {
    DOCUMENT.with(|slot| {
        slot.borrow()
            .as_ref()
            .expect("persistent document initialized")
            .outcome()
    })
}

fn create_checkpoint(input: &[u8]) -> Vec<u8> {
    let format = Format::decode(*input.first().expect("format byte"));
    Document::parse(format, input[1..].to_vec()).encode_checkpoint()
}

fn init_hybrid_index(input: &[u8]) -> Vec<u8> {
    let format = Format::decode(*input.first().expect("format byte"));
    HYBRID_INDEX.with(|slot| {
        let index = HybridIndex::hydrate(format);
        let mut output = Vec::with_capacity(24);
        push_u64(&mut output, index.entities.len() as u64);
        push_u64(
            &mut output,
            (index.entities.len() * std::mem::size_of::<IndexedEntity>()) as u64,
        );
        push_u64(&mut output, 0);
        *slot.borrow_mut() = Some(index);
        output
    })
}

fn hybrid_index_edit(input: &[u8]) -> Vec<u8> {
    HYBRID_INDEX.with(|slot| {
        slot.borrow_mut()
            .as_mut()
            .expect("hybrid index initialized")
            .apply_splice(input)
    })
}

fn host_context_change(input: &[u8], batched: bool) -> Vec<u8> {
    let edit_offset = i32::try_from(read_u64(input, 0)).expect("edit offset fits wasm32");
    let entity_index = unsafe { context_entity_at_offset(edit_offset) };
    assert!(entity_index >= 0, "host context did not resolve the edit");
    let entity_id = unsafe { context_entity_id(entity_index) } as u64;
    let before_start = unsafe { context_entity_start(0, entity_index) };
    let before_end = unsafe { context_entity_end(0, entity_index) };
    let after_start = unsafe { context_entity_start(1, entity_index) };
    let after_end = unsafe { context_entity_end(1, entity_index) };
    assert!(before_start >= 0 && before_end >= before_start);
    assert!(after_start >= 0 && after_end >= after_start);

    let before_hash = if batched {
        hash_host_range_batched(0, before_start as usize, before_end as usize)
    } else {
        hash_host_range_fine(0, before_start, before_end)
    };
    let after_hash = if batched {
        hash_host_range_batched(1, after_start as usize, after_end as usize)
    } else {
        hash_host_range_fine(1, after_start, after_end)
    };
    let changed = before_hash != after_hash || before_end - before_start != after_end - after_start;
    assert!(changed, "host context edit must change its semantic entity");

    let mut output = Vec::with_capacity(32);
    push_u64(
        &mut output,
        u64::try_from(unsafe { context_entity_count() }).expect("entity count"),
    );
    push_u64(&mut output, entity_id);
    push_u64(&mut output, after_hash);
    push_u64(&mut output, u64::from(changed));
    output
}

fn hash_host_range_fine(view: i32, start: i32, end: i32) -> u64 {
    let mut hash = FNV_OFFSET;
    for offset in start..end {
        let byte = unsafe { context_source_byte(view, offset) };
        assert!((0..=255).contains(&byte), "host source byte outside u8");
        hash = fnv1a_continue(hash, &[byte as u8]);
    }
    hash
}

fn read_host_range_batched(view: i32, start: usize, end: usize) -> Vec<u8> {
    let len = end.checked_sub(start).expect("host range order");
    let mut bytes = vec![0_u8; len];
    let read = unsafe {
        context_source_read(
            view,
            i32::try_from(start).expect("range start fits wasm32"),
            i32::try_from(bytes.as_mut_ptr() as usize).expect("wasm32 range pointer"),
            i32::try_from(len).expect("range length fits wasm32"),
        )
    };
    assert_eq!(read, len as i32, "host range read was short");
    bytes
}

fn hash_host_range_batched(view: i32, start: usize, end: usize) -> u64 {
    fnv1a(&read_host_range_batched(view, start, end))
}

fn scan_host_entities(format: Format, view: i32) -> Vec<IndexedEntity> {
    const PAGE_BYTES: usize = 64 * 1_024;
    let source_len = usize::try_from(unsafe { context_source_len(view) })
        .expect("host source length fits usize");
    let delimiter = match format {
        Format::Markdown => b"\n\n".as_slice(),
        Format::Csv | Format::Json | Format::Excalidraw | Format::Text => b"\n".as_slice(),
    };
    let mut entities = Vec::new();
    let mut pending = Vec::with_capacity(PAGE_BYTES + 1_024);
    let mut pending_start = 0_usize;
    let mut offset = 0_usize;
    while offset < source_len {
        let end = offset.saturating_add(PAGE_BYTES).min(source_len);
        pending.extend_from_slice(&read_host_range_batched(view, offset, end));
        offset = end;

        let mut consumed = 0_usize;
        while let Some(relative_end) = find_subslice(&pending[consumed..], delimiter) {
            let record_end = consumed + relative_end;
            push_indexed_entity(
                &mut entities,
                format,
                &pending[consumed..record_end],
                pending_start + consumed,
                pending_start + record_end,
            );
            consumed = record_end + delimiter.len();
        }
        if consumed > 0 {
            pending.drain(..consumed);
            pending_start += consumed;
        }
    }
    if !pending.is_empty() {
        push_indexed_entity(
            &mut entities,
            format,
            &pending,
            pending_start,
            pending_start + pending.len(),
        );
    }
    entities
}

fn push_indexed_entity(
    entities: &mut Vec<IndexedEntity>,
    format: Format,
    bytes: &[u8],
    start: usize,
    end: usize,
) {
    if !is_entity_line(format, bytes) {
        return;
    }
    entities.push(IndexedEntity {
        id: if format.has_intrinsic_ids() {
            intrinsic_id(format, bytes)
        } else {
            0
        },
        hash: fnv1a(bytes),
        start: u32::try_from(start).expect("entity start fits u32"),
        end: u32::try_from(end).expect("entity end fits u32"),
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: i32) -> i32 {
    let len = usize::try_from(len).expect("allocation length");
    let mut allocation = vec![0_u8; len];
    let pointer = allocation.as_mut_ptr();
    forget(allocation);
    i32::try_from(pointer as usize).expect("wasm32 pointer")
}

#[unsafe(no_mangle)]
pub extern "C" fn dealloc(pointer: i32, len: i32) {
    let pointer = usize::try_from(pointer).expect("pointer") as *mut u8;
    let len = usize::try_from(len).expect("allocation length");
    drop(unsafe { Vec::from_raw_parts(pointer, len, len) });
}

#[unsafe(no_mangle)]
pub extern "C" fn run(operation: i32, pointer: i32, len: i32) -> i64 {
    let pointer = usize::try_from(pointer).expect("pointer") as *mut u8;
    let len = usize::try_from(len).expect("input length");
    let input = unsafe { Vec::from_raw_parts(pointer, len, len) };
    let output = match operation {
        OP_INIT_PERSISTENT => init_persistent(&input),
        OP_STATELESS_V1 => stateless_v1(&input),
        OP_FULL_FILE_PERSISTENT => full_file_persistent(&input),
        OP_SPLICE_PERSISTENT => splice_persistent(&input),
        OP_CHECKPOINT_REDUCER => checkpoint_reducer(&input),
        OP_SNAPSHOT => snapshot(),
        OP_OUTCOME => outcome(),
        OP_CREATE_CHECKPOINT => create_checkpoint(&input),
        OP_STATELESS_V1_CHECKPOINT => stateless_v1_checkpoint(&input),
        OP_HOST_CONTEXT_FINE => host_context_change(&input, false),
        OP_HOST_CONTEXT_BATCHED => host_context_change(&input, true),
        OP_INIT_HYBRID_INDEX => init_hybrid_index(&input),
        OP_HYBRID_INDEX_EDIT => hybrid_index_edit(&input),
        _ => panic!("unknown operation {operation}"),
    };
    pack_output(output)
}

fn pack_output(mut output: Vec<u8>) -> i64 {
    let pointer = output.as_mut_ptr() as usize;
    let len = output.len();
    forget(output);
    let packed = (u64::try_from(pointer).expect("output pointer") << 32)
        | u64::try_from(len).expect("output length");
    packed as i64
}

// Keep an explicit start function out of the module. Wasmtime initializes the
// Rust runtime when the instance is created, and calls only the exports above.
#[allow(dead_code)]
fn _assert_send_free_design() {
    let _ = slice::from_ref(&0_u8);
}
