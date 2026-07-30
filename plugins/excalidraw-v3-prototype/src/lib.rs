//! Fused Excalidraw experiment for Component API v3.
#![allow(dead_code)]

#[path = "../../excalidraw-v2/src/core.rs"]
mod core;

use core::{ArenaElementSpan, ChangeEffect, Document, EntityChange, IdNamespace, InputSplice};
use lix_plugin_api_v3_prototype as sdk;

struct ExcalidrawV3Prototype;

const ELEMENT_INDEX_KEY: &[u8] = b"excalidraw/element-spans-v1";
const ELEMENT_SHIFTS_KEY: &[u8] = b"excalidraw/element-shifts-v1";
const ELEMENT_INDEX_MAGIC: &[u8; 4] = b"EXS1";
const ELEMENT_INDEX_HEADER_BYTES: u32 = 8;
const ELEMENT_INDEX_ENTRY_BYTES: u32 = 32;

impl sdk::FormatPlugin for ExcalidrawV3Prototype {
    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let namespace = IdNamespace::from_halves(input.creates.high, u64::from(input.creates.low));
        let (document, changes) = Document::open_file(
            input.accepted.read_all()?,
            input.file.path.as_deref(),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
        input.successor.put_state(
            ELEMENT_INDEX_KEY,
            &encode_element_index(&document.arena_element_spans())?,
        )?;
        emit_changes(changes, sink)?;
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let namespace =
            IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
        let inserts = update
            .edits
            .iter()
            .map(|edit| edit.insert.clone())
            .collect::<Vec<_>>();
        let splices = update
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        if update.before_file.path == update.after_file.path
            && let [edit] = update.edits.as_slice()
            && let Some((change, successor_shifts)) =
                sparse_element_change(update, edit, &inserts[0])?
        {
            update
                .successor
                .put_state(ELEMENT_SHIFTS_KEY, &successor_shifts)?;
            emit_changes([Ok(change)], sink)?;
            return Ok(());
        }

        let (document, _) = Document::open_file(
            update.before.read_all()?,
            update.before_file.path.as_deref(),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
        let (document, changes) = document
            .file_changed(&splices, namespace)
            .map_err(sdk::Error::invalid_input)?;
        update.successor.put_state(
            ELEMENT_INDEX_KEY,
            &encode_element_index(&document.arena_element_spans())?,
        )?;
        emit_changes(changes.into_iter().map(Ok), sink)?;
        Ok(())
    }
}

fn sparse_element_change(
    update: &sdk::FileUpdate<'_>,
    edit: &sdk::InputSplice,
    insert: &[u8],
) -> sdk::Result<Option<(EntityChange, Vec<u8>)>> {
    let state_len = match update.before.state_len(ELEMENT_INDEX_KEY)? {
        Some(length) => length,
        None => return Ok(None),
    };
    let header = update
        .before
        .read_state_range(ELEMENT_INDEX_KEY, 0, ELEMENT_INDEX_HEADER_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw element index disappeared"))?;
    if header.get(..4) != Some(ELEMENT_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Excalidraw element index",
        ));
    }
    let count = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("validated Excalidraw index header"),
    );
    let entry_bytes = u64::from(count)
        .checked_mul(u64::from(ELEMENT_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index size overflowed"))?;
    let blob_offset = u64::from(ELEMENT_INDEX_HEADER_BYTES)
        .checked_add(entry_bytes)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index size overflowed"))?;
    if blob_offset > state_len {
        return Err(sdk::Error::invalid_input(
            "truncated Excalidraw element index",
        ));
    }
    let mut shifts = decode_shifts(
        update
            .before
            .get_state(ELEMENT_SHIFTS_KEY)?
            .as_deref()
            .unwrap_or_default(),
    )?;
    let edit_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw edit range overflowed"))?;

    let mut low = 0_u32;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = read_index_entry(update, middle)?;
        if effective_offset(entry.offset, middle, &shifts)? <= edit.offset {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if low == 0 {
        return Ok(None);
    }
    let ordinal = low - 1;
    let entry = read_index_entry(update, ordinal)?;
    let span_offset = effective_offset(entry.offset, ordinal, &shifts)?;
    let span_length = effective_length(entry.length, ordinal, &shifts)?;
    if edit.offset < span_offset
        || edit_end
            > span_offset
                .checked_add(span_length)
                .ok_or_else(|| sdk::Error::invalid_input("Excalidraw span overflowed"))?
    {
        return Ok(None);
    }
    let metadata_length = entry
        .id_len
        .checked_add(entry.order_key_len)
        .and_then(|length| length.checked_add(entry.leading_json_len))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw metadata size overflowed"))?;
    let metadata = update
        .before
        .read_state_range(
            ELEMENT_INDEX_KEY,
            blob_offset
                .checked_add(u64::from(entry.metadata_offset))
                .ok_or_else(|| {
                    sdk::Error::invalid_input("Excalidraw metadata offset overflowed")
                })?,
            metadata_length,
        )?
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw element index disappeared"))?;
    let id_end = entry.id_len as usize;
    let order_key_end = id_end + entry.order_key_len as usize;
    let leading_json_end = order_key_end + entry.leading_json_len as usize;
    let id = state_text(&metadata[..id_end])?;
    let order_key = state_text(&metadata[id_end..order_key_end])?;
    let leading_json = state_text(&metadata[order_key_end..leading_json_end])?;

    let mut element = update.before.read_range(span_offset, span_length)?;
    let local_start = usize::try_from(edit.offset - span_offset)
        .map_err(|_| sdk::Error::invalid_input("Excalidraw edit offset exceeds guest memory"))?;
    let local_end = local_start
        .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
            sdk::Error::invalid_input("Excalidraw edit deletion exceeds guest memory")
        })?)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw edit range overflowed"))?;
    element.splice(local_start..local_end, insert.iter().copied());
    let element_json = String::from_utf8(element)
        .map_err(|error| sdk::Error::invalid_input(format!("invalid Excalidraw UTF-8: {error}")))?;
    let change = Document::element_change_from_source(&id, order_key, leading_json, element_json)
        .map_err(sdk::Error::invalid_input)?;

    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("Excalidraw insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("Excalidraw span growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("Excalidraw span shrink exceeds i64"))?
    };
    shifts.push((ordinal, delta));
    Ok(Some((change, encode_shifts(&shifts))))
}

fn encode_element_index(spans: &[ArenaElementSpan]) -> sdk::Result<Vec<u8>> {
    let entries_bytes = spans
        .len()
        .checked_mul(ELEMENT_INDEX_ENTRY_BYTES as usize)
        .ok_or_else(|| sdk::Error::limit_exceeded("Excalidraw index size overflowed"))?;
    let metadata_bytes = spans.iter().try_fold(0_usize, |total, span| {
        total
            .checked_add(span.id.len())
            .and_then(|value| value.checked_add(span.order_key.len()))
            .and_then(|value| value.checked_add(span.leading_json.len()))
            .ok_or_else(|| sdk::Error::limit_exceeded("Excalidraw metadata size overflowed"))
    })?;
    let mut output = Vec::with_capacity(8 + entries_bytes + metadata_bytes);
    output.extend_from_slice(ELEMENT_INDEX_MAGIC);
    output.extend_from_slice(
        &u32::try_from(spans.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Excalidraw element spans"))?
            .to_le_bytes(),
    );
    let mut metadata_offset = 0_u32;
    for span in spans {
        output.extend_from_slice(&span.offset.to_le_bytes());
        output.extend_from_slice(&span.length.to_le_bytes());
        output.extend_from_slice(&metadata_offset.to_le_bytes());
        for length in [span.id.len(), span.order_key.len(), span.leading_json.len()] {
            let length = u32::try_from(length)
                .map_err(|_| sdk::Error::limit_exceeded("Excalidraw metadata exceeds 4GiB"))?;
            output.extend_from_slice(&length.to_le_bytes());
            metadata_offset = metadata_offset
                .checked_add(length)
                .ok_or_else(|| sdk::Error::limit_exceeded("Excalidraw metadata overflowed"))?;
        }
    }
    for span in spans {
        output.extend_from_slice(span.id.as_bytes());
        output.extend_from_slice(span.order_key.as_bytes());
        output.extend_from_slice(span.leading_json.as_bytes());
    }
    Ok(output)
}

#[derive(Clone, Copy)]
struct IndexEntry {
    offset: u64,
    length: u64,
    metadata_offset: u32,
    id_len: u32,
    order_key_len: u32,
    leading_json_len: u32,
}

fn read_index_entry(update: &sdk::FileUpdate<'_>, ordinal: u32) -> sdk::Result<IndexEntry> {
    let offset = u64::from(ELEMENT_INDEX_HEADER_BYTES)
        .checked_add(u64::from(ordinal) * u64::from(ELEMENT_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index offset overflowed"))?;
    let bytes = update
        .before
        .read_state_range(ELEMENT_INDEX_KEY, offset, ELEMENT_INDEX_ENTRY_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw element index disappeared"))?;
    Ok(IndexEntry {
        offset: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed index entry")),
        length: u64::from_le_bytes(bytes[8..16].try_into().expect("fixed index entry")),
        metadata_offset: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed index entry")),
        id_len: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed index entry")),
        order_key_len: u32::from_le_bytes(bytes[24..28].try_into().expect("fixed index entry")),
        leading_json_len: u32::from_le_bytes(bytes[28..32].try_into().expect("fixed index entry")),
    })
}

fn effective_offset(base: u64, ordinal: u32, shifts: &[(u32, i64)]) -> sdk::Result<u64> {
    apply_shift(
        base,
        shifts
            .iter()
            .filter(|(changed, _)| *changed < ordinal)
            .map(|(_, delta)| *delta)
            .sum(),
    )
}

fn effective_length(base: u64, ordinal: u32, shifts: &[(u32, i64)]) -> sdk::Result<u64> {
    apply_shift(
        base,
        shifts
            .iter()
            .filter(|(changed, _)| *changed == ordinal)
            .map(|(_, delta)| *delta)
            .sum(),
    )
}

fn apply_shift(base: u64, delta: i64) -> sdk::Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64)
    } else {
        base.checked_sub(delta.unsigned_abs())
    }
    .ok_or_else(|| sdk::Error::invalid_input("Excalidraw span shift overflowed"))
}

fn decode_shifts(bytes: &[u8]) -> sdk::Result<Vec<(u32, i64)>> {
    if bytes.len() % 12 != 0 {
        return Err(sdk::Error::invalid_input(
            "truncated Excalidraw shift overlay",
        ));
    }
    Ok(bytes
        .chunks_exact(12)
        .map(|record| {
            (
                u32::from_le_bytes(record[0..4].try_into().expect("fixed shift record")),
                i64::from_le_bytes(record[4..12].try_into().expect("fixed shift record")),
            )
        })
        .collect())
}

fn encode_shifts(shifts: &[(u32, i64)]) -> Vec<u8> {
    let mut output = Vec::with_capacity(shifts.len() * 12);
    for (ordinal, delta) in shifts {
        output.extend_from_slice(&ordinal.to_le_bytes());
        output.extend_from_slice(&delta.to_le_bytes());
    }
    output
}

fn state_text(bytes: &[u8]) -> sdk::Result<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|error| sdk::Error::invalid_input(format!("invalid Excalidraw state: {error}")))
}

fn emit_changes<I>(changes: I, sink: &mut sdk::Sink<'_>) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<EntityChange, String>>,
{
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change.map_err(sdk::Error::invalid_input)?, sink)?;
    }
    encoder.flush(sink)
}

struct BatchEncoder {
    max_bytes: usize,
    payload: Vec<u8>,
    records: u32,
}

impl BatchEncoder {
    fn new(max_bytes: u32) -> Self {
        Self {
            max_bytes: max_bytes as usize,
            payload: Vec::with_capacity(max_bytes as usize),
            records: 0,
        }
    }

    fn push(&mut self, change: EntityChange, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let mut record = Vec::new();
        encode_change(change, &mut record)?;
        if record.len() > self.max_bytes {
            return Err(sdk::Error::limit_exceeded(
                "one Excalidraw entity exceeds the v3 batch limit",
            ));
        }
        if self.records > 0 && self.payload.len() + record.len() > self.max_bytes {
            self.flush(sink)?;
        }
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("Excalidraw batch count overflowed"))?;
        Ok(())
    }

    fn flush(&mut self, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        if self.records == 0 {
            return Ok(());
        }
        let payload = std::mem::replace(&mut self.payload, Vec::with_capacity(self.max_bytes));
        let records = std::mem::take(&mut self.records);
        sink.emit_changes(records, payload)
    }
}

fn encode_change(change: EntityChange, output: &mut Vec<u8>) -> sdk::Result<()> {
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match change.snapshot {
        Some(snapshot) => {
            output.push(0);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
            output.push(match change.effect {
                ChangeEffect::Content => 0,
                ChangeEffect::FormatOnly => 1,
            });
            push_inline_blob(output, &snapshot)?;
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
        }
    }
    let length = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("Excalidraw packet exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn push_entity_key(
    output: &mut Vec<u8>,
    schema_key: &str,
    components: &[String],
) -> sdk::Result<()> {
    push_text(output, schema_key)?;
    output.extend_from_slice(
        &u32::try_from(components.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many primary-key components"))?
            .to_le_bytes(),
    );
    for component in components {
        push_text(output, component)?;
    }
    Ok(())
}

fn push_text(output: &mut Vec<u8>, value: &str) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| sdk::Error::limit_exceeded("packet text is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_inline_blob(output: &mut Vec<u8>, bytes: &[u8]) -> sdk::Result<()> {
    output.push(0);
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .map_err(|_| sdk::Error::limit_exceeded("snapshot is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(bytes);
    Ok(())
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v3_prototype::export_v3_prototype!(ExcalidrawV3Prototype);
