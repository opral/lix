//! Excalidraw support for the row-first Component API v1.
#![allow(dead_code)]

mod core;
mod order_key;

use core::{
    ArenaElementSpan, ChangeEffect, Document, FileEdit, IdNamespace, RowChange, RowImportBuilder,
    RowRecord,
};
use lix::plugin as sdk;

struct ExcalidrawPlugin;

const ELEMENT_INDEX_KEY: &[u8] = b"excalidraw/element-spans";
const ELEMENT_SHIFTS_KEY: &[u8] = b"excalidraw/element-shifts";
const ID_NAMESPACE_STATE: &[u8] = b"excalidraw/id-namespace";
const ELEMENT_INDEX_MAGIC: &[u8; 4] = b"EXS2";
const ELEMENT_INDEX_HEADER_BYTES: u32 = 16;
const ELEMENT_INDEX_ENTRY_BYTES: u32 = 32;
const ELEMENT_INDEX_PAGE_BYTES: usize = 1024 * 1024;
const MAX_ELEMENT_SHIFT_RECORDS: usize = 4096;

fn cold_parse_changes(
    update: &mut sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let accepted = update.before.read_all()?;
    let mut builder = RowImportBuilder::new();
    let rows = update
        .rows
        .as_mut()
        .ok_or_else(|| sdk::Error::internal("cold parse_changes requires durable rows"))?;
    while let Some(row) = rows.next()? {
        builder
            .push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                snapshot: row.snapshot,
            })
            .map_err(sdk::Error::invalid_input)?;
    }
    let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
    let (mut document, _) = builder.finish().map_err(sdk::Error::invalid_input)?;
    let rendered = document.bytes();
    if rendered != accepted {
        let reconcile = [FileEdit {
            offset: 0,
            delete_len: rendered.len() as u64,
            insert: &accepted,
        }];
        document = document
            .file_changed(&reconcile, namespace)
            .map_err(sdk::Error::invalid_input)?
            .0;
    }
    let inserts = update
        .file_edits
        .iter()
        .map(|edit| edit.insert.clone())
        .collect::<Vec<_>>();
    let splices = update
        .file_edits
        .iter()
        .zip(&inserts)
        .map(|(edit, insert)| FileEdit {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert,
        })
        .collect::<Vec<_>>();
    let (successor, changes) = document
        .file_changed(&splices, namespace)
        .map_err(sdk::Error::invalid_input)?;
    sink.put_state(ID_NAMESPACE_STATE, &update.creates.namespace_bytes())?;
    store_element_index(
        sink,
        &encode_element_index(&successor.arena_element_spans())?,
    )?;
    emit_changes(changes.into_iter().map(Ok), sink)
}

impl sdk::FileProjection for ExcalidrawPlugin {
    fn serialize_changes(
        mut update: sdk::SerializeChangesInput<'_>,
        sink: &mut sdk::FileEditOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let before = update.before.read_all()?;
        let mut changes = Vec::new();
        while let Some(change) = update.row_changes.next()? {
            changes.push(RowChange {
                schema_key: change.schema_key,
                row_pk: change.row_pk,
                snapshot: change.snapshot,
                effect: match change.effect {
                    sdk::ChangeEffect::Content => ChangeEffect::Content,
                    sdk::ChangeEffect::FormatOnly => ChangeEffect::FormatOnly,
                },
            });
        }
        let namespace = read_namespace(&update.before)?
            .or_else(|| namespace_from_changes(&changes))
            .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        let (document, _) = Document::open_file(before.clone(), Some(update.path), namespace)
            .map_err(sdk::Error::invalid_input)?;
        let (successor, edits) = document
            .rows_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            sink.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        store_element_index(
            sink,
            &encode_element_index(&successor.arena_element_spans())?,
        )?;
        sink.delete_state(ELEMENT_SHIFTS_KEY)?;
        Ok(())
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        sink: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(row) = input.rows.next()? {
            records.push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                snapshot: row.snapshot,
            });
        }
        let (document, _) = Document::open_rows(records).map_err(sdk::Error::invalid_input)?;
        store_element_index(
            sink,
            &encode_element_index(&document.arena_element_spans())?,
        )?;
        sink.write(&document.bytes())
    }

    fn parse(input: sdk::ParseInput<'_>, sink: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        let namespace = IdNamespace::from_namespace_bytes(input.creates.namespace_bytes());
        let (document, changes) =
            Document::open_file(input.file.read_all()?, Some(input.path), namespace)
                .map_err(sdk::Error::invalid_input)?;
        sink.put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        store_element_index(
            sink,
            &encode_element_index(&document.arena_element_spans())?,
        )?;
        emit_changes(changes, sink)?;
        Ok(())
    }

    fn parse_changes(
        mut update: sdk::ParseChangesInput<'_>,
        sink: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        if update.before.state_len(ELEMENT_INDEX_KEY)?.is_none() {
            return cold_parse_changes(&mut update, sink);
        }
        let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
        let inserts = update
            .file_edits
            .iter()
            .map(|edit| edit.insert.clone())
            .collect::<Vec<_>>();
        let splices = update
            .file_edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        if update.before_path == update.after_path
            && update.file_edits.iter().len() == 1
            && let Some(edit) = update.file_edits.iter().next()
            && let Some((change, successor_shifts)) =
                sparse_element_change(&update, edit, &inserts[0])?
        {
            sink.put_state(ELEMENT_SHIFTS_KEY, &successor_shifts)?;
            emit_changes([Ok(change)], sink)?;
            return Ok(());
        }

        let (document, _) = Document::open_file(
            update.before.read_all()?,
            Some(update.before_path),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
        let (document, changes) = document
            .file_changed(&splices, namespace)
            .map_err(sdk::Error::invalid_input)?;
        replace_element_index(
            &update.before,
            sink,
            &encode_element_index(&document.arena_element_spans())?,
        )?;
        sink.delete_state(ELEMENT_SHIFTS_KEY)?;
        emit_changes(changes.into_iter().map(Ok), sink)?;
        Ok(())
    }
}

fn read_namespace(root: &sdk::Snapshot<'_>) -> sdk::Result<Option<IdNamespace>> {
    let Some(bytes) = root.get_state(ID_NAMESPACE_STATE)? else {
        return Ok(None);
    };
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("Excalidraw ID namespace has invalid length"))?;
    Ok(Some(IdNamespace::from_halves(
        u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        u64::from(u32::from_be_bytes(
            bytes[8..].try_into().expect("four bytes"),
        )),
    )))
}

fn namespace_from_changes(changes: &[RowChange]) -> Option<IdNamespace> {
    changes
        .iter()
        .flat_map(|change| &change.row_pk)
        .find_map(|component| uuid::Uuid::parse_str(component).ok())
        .map(|id| {
            let bytes = id.into_bytes();
            IdNamespace::from_halves(
                u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
                u64::from(u32::from_be_bytes(
                    bytes[8..12].try_into().expect("four bytes"),
                )),
            )
        })
}

fn apply_edits(mut bytes: Vec<u8>, edits: &[core::ByteEdit]) -> sdk::Result<Vec<u8>> {
    for edit in edits.iter().rev() {
        let start = usize::try_from(edit.offset).map_err(|_| {
            sdk::Error::invalid_input("Excalidraw edit offset exceeds guest memory")
        })?;
        let end = start
            .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                sdk::Error::invalid_input("Excalidraw edit deletion exceeds guest memory")
            })?)
            .ok_or_else(|| sdk::Error::invalid_input("Excalidraw edit range overflowed"))?;
        if end > bytes.len() {
            return Err(sdk::Error::invalid_input(
                "Excalidraw edit exceeds accepted bytes",
            ));
        }
        bytes.splice(start..end, edit.insert.iter().copied());
    }
    Ok(bytes)
}

fn sparse_element_change(
    update: &sdk::ParseChangesInput<'_>,
    edit: &sdk::FileEdit,
    insert: &[u8],
) -> sdk::Result<Option<(RowChange, Vec<u8>)>> {
    match update.before.state_len(ELEMENT_INDEX_KEY)? {
        Some(_) => {}
        None => return Ok(None),
    }
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
    let payload_len = u64::from(u32::from_le_bytes(
        header[8..12]
            .try_into()
            .expect("validated Excalidraw index header"),
    ));
    let entry_bytes = u64::from(count)
        .checked_mul(u64::from(ELEMENT_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index size overflowed"))?;
    let blob_offset = entry_bytes;
    if blob_offset > payload_len {
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
    let metadata_offset = blob_offset
        .checked_add(u64::from(entry.metadata_offset))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw metadata offset overflowed"))?;
    let metadata = read_element_index_range(&update.before, metadata_offset, metadata_length)?
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
    let Some(change) =
        Document::element_change_from_source(&id, order_key, leading_json, element_json)
            .map_err(sdk::Error::invalid_input)?
    else {
        return Ok(None);
    };

    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("Excalidraw insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("Excalidraw span growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("Excalidraw span shrink exceeds i64"))?
    };
    if !add_element_shift(&mut shifts, ordinal, delta)? {
        return Ok(None);
    }
    Ok(Some((change, encode_shifts(&shifts))))
}

struct EncodedElementIndex {
    count: u32,
    payload: Vec<u8>,
}

fn encode_element_index(spans: &[ArenaElementSpan]) -> sdk::Result<EncodedElementIndex> {
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
    let count = u32::try_from(spans.len())
        .map_err(|_| sdk::Error::limit_exceeded("too many Excalidraw element spans"))?;
    let mut output = Vec::with_capacity(entries_bytes + metadata_bytes);
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
    Ok(EncodedElementIndex {
        count,
        payload: output,
    })
}

fn store_element_index(
    successor: &mut impl StateOutput,
    encoded: &EncodedElementIndex,
) -> sdk::Result<()> {
    let page_count = u32::try_from(encoded.payload.len().div_ceil(ELEMENT_INDEX_PAGE_BYTES))
        .map_err(|_| sdk::Error::limit_exceeded("too many Excalidraw index pages"))?;
    let mut manifest = Vec::with_capacity(ELEMENT_INDEX_HEADER_BYTES as usize);
    manifest.extend_from_slice(ELEMENT_INDEX_MAGIC);
    manifest.extend_from_slice(&encoded.count.to_le_bytes());
    manifest.extend_from_slice(
        &u32::try_from(encoded.payload.len())
            .map_err(|_| sdk::Error::limit_exceeded("Excalidraw index exceeds 4GiB"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(&page_count.to_le_bytes());
    successor.put_state(ELEMENT_INDEX_KEY, &manifest)?;
    for (ordinal, page) in encoded.payload.chunks(ELEMENT_INDEX_PAGE_BYTES).enumerate() {
        successor.put_state(&element_index_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn replace_element_index(
    before: &sdk::Snapshot<'_>,
    successor: &mut impl StateOutput,
    encoded: &EncodedElementIndex,
) -> sdk::Result<()> {
    let old_page_count = element_index_page_count(before)?;
    store_element_index(successor, encoded)?;
    let new_page_count = u32::try_from(encoded.payload.len().div_ceil(ELEMENT_INDEX_PAGE_BYTES))
        .map_err(|_| sdk::Error::limit_exceeded("too many Excalidraw index pages"))?;
    for ordinal in new_page_count..old_page_count {
        successor.delete_state(&element_index_page_key(ordinal))?;
    }
    Ok(())
}

fn element_index_page_count(root: &sdk::Snapshot<'_>) -> sdk::Result<u32> {
    let Some(header) = root.read_state_range(ELEMENT_INDEX_KEY, 0, ELEMENT_INDEX_HEADER_BYTES)?
    else {
        return Ok(0);
    };
    if header.get(..4) != Some(ELEMENT_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Excalidraw element index",
        ));
    }
    Ok(u32::from_le_bytes(
        header[12..16]
            .try_into()
            .expect("fixed Excalidraw manifest"),
    ))
}

fn element_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"excalidraw/element-index-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn read_element_index_range(
    root: &sdk::Snapshot<'_>,
    offset: u64,
    length: u32,
) -> sdk::Result<Option<Vec<u8>>> {
    let mut output = Vec::with_capacity(length as usize);
    let mut cursor = offset;
    let end = offset
        .checked_add(u64::from(length))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index range overflowed"))?;
    while cursor < end {
        let page = cursor / ELEMENT_INDEX_PAGE_BYTES as u64;
        let page_offset = cursor % ELEMENT_INDEX_PAGE_BYTES as u64;
        let remaining = end - cursor;
        let take = remaining.min(ELEMENT_INDEX_PAGE_BYTES as u64 - page_offset) as u32;
        let Some(bytes) =
            root.read_state_range(&element_index_page_key(page as u32), page_offset, take)?
        else {
            return Ok(None);
        };
        output.extend_from_slice(&bytes);
        cursor += u64::from(take);
    }
    Ok(Some(output))
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

fn read_index_entry(update: &sdk::ParseChangesInput<'_>, ordinal: u32) -> sdk::Result<IndexEntry> {
    let offset = u64::from(ordinal)
        .checked_mul(u64::from(ELEMENT_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index offset overflowed"))?;
    let bytes = read_element_index_range(&update.before, offset, ELEMENT_INDEX_ENTRY_BYTES)?
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
    let mut compact = std::collections::BTreeMap::<u32, i64>::new();
    for record in bytes.chunks_exact(12) {
        let ordinal = u32::from_le_bytes(record[0..4].try_into().expect("fixed shift record"));
        let delta = i64::from_le_bytes(record[4..12].try_into().expect("fixed shift record"));
        let total = compact
            .get(&ordinal)
            .copied()
            .unwrap_or_default()
            .checked_add(delta)
            .ok_or_else(|| sdk::Error::invalid_input("Excalidraw span shift overflowed"))?;
        if total == 0 {
            compact.remove(&ordinal);
        } else {
            compact.insert(ordinal, total);
        }
    }
    Ok(compact.into_iter().collect())
}

fn add_element_shift(shifts: &mut Vec<(u32, i64)>, ordinal: u32, delta: i64) -> sdk::Result<bool> {
    if delta == 0 {
        return Ok(true);
    }
    match shifts.binary_search_by_key(&ordinal, |(ordinal, _)| *ordinal) {
        Ok(index) => {
            let total = shifts[index]
                .1
                .checked_add(delta)
                .ok_or_else(|| sdk::Error::invalid_input("Excalidraw span shift overflowed"))?;
            if total == 0 {
                shifts.remove(index);
            } else {
                shifts[index].1 = total;
            }
            Ok(true)
        }
        Err(index) if shifts.len() < MAX_ELEMENT_SHIFT_RECORDS => {
            shifts.insert(index, (ordinal, delta));
            Ok(true)
        }
        Err(_) => Ok(false),
    }
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

fn emit_changes<I>(changes: I, sink: &mut impl MutationOutput) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<RowChange, String>>,
{
    for change in changes {
        let change = change.map_err(sdk::Error::invalid_input)?;
        match change.snapshot {
            Some(snapshot) => sink.upsert(
                &change.schema_key,
                &change.row_pk,
                &snapshot,
                match change.effect {
                    ChangeEffect::Content => sdk::ChangeEffect::Content,
                    ChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
                },
            )?,
            None => sink.delete(&change.schema_key, &change.row_pk)?,
        }
    }
    Ok(())
}

trait StateOutput {
    fn put_state(&mut self, key: &[u8], value: &[u8]) -> sdk::Result<()>;
    fn delete_state(&mut self, key: &[u8]) -> sdk::Result<()>;
}
macro_rules! impl_state_output {
    ($type:ty) => {
        impl StateOutput for $type {
            fn put_state(&mut self, key: &[u8], value: &[u8]) -> sdk::Result<()> {
                <$type>::put_state(self, key, value)
            }
            fn delete_state(&mut self, key: &[u8]) -> sdk::Result<()> {
                <$type>::delete_state(self, key)
            }
        }
    };
}
impl_state_output!(sdk::RowOutput<'_, '_>);
impl_state_output!(sdk::RowChangeOutput<'_, '_>);
impl_state_output!(sdk::FileOutput<'_, '_>);
impl_state_output!(sdk::FileEditOutput<'_, '_>);

trait MutationOutput {
    fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[String],
        snapshot: &[u8],
        effect: sdk::ChangeEffect,
    ) -> sdk::Result<()>;
    fn delete(&mut self, schema_key: &str, row_pk: &[String]) -> sdk::Result<()>;
}
impl MutationOutput for sdk::RowOutput<'_, '_> {
    fn upsert(&mut self, s: &str, k: &[String], v: &[u8], _: sdk::ChangeEffect) -> sdk::Result<()> {
        self.upsert(s, k, v)
    }
    fn delete(&mut self, _: &str, _: &[String]) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "initial Excalidraw parse produced a deletion",
        ))
    }
}
impl MutationOutput for sdk::RowChangeOutput<'_, '_> {
    fn upsert(&mut self, s: &str, k: &[String], v: &[u8], e: sdk::ChangeEffect) -> sdk::Result<()> {
        self.upsert(s, k, v, e)
    }
    fn delete(&mut self, s: &str, k: &[String]) -> sdk::Result<()> {
        self.delete(s, k)
    }
}

#[cfg(target_family = "wasm")]
lix::plugin::export_capabilities! { file_projection: ExcalidrawPlugin }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_element_index_is_split_into_bounded_pages() {
        let spans = (0..70_000_u64)
            .map(|ordinal| ArenaElementSpan {
                id: format!("element-{ordinal}"),
                order_key: ordinal.to_string(),
                leading_json: String::new(),
                offset: ordinal,
                length: 1,
            })
            .collect::<Vec<_>>();

        let encoded = encode_element_index(&spans).expect("encode element index");
        let pages = encoded
            .payload
            .chunks(ELEMENT_INDEX_PAGE_BYTES)
            .collect::<Vec<_>>();

        assert!(encoded.payload.len() > 2 * 1024 * 1024);
        assert!(pages.len() > 1);
        assert!(
            pages
                .iter()
                .all(|page| page.len() <= ELEMENT_INDEX_PAGE_BYTES)
        );
        assert_eq!(encoded.count, spans.len() as u32);
    }

    #[test]
    fn sparse_element_shifts_are_coalesced_and_bounded() {
        let mut shifts = Vec::new();
        for _ in 0..100_000 {
            assert!(add_element_shift(&mut shifts, 7, 1).expect("coalesce shift"));
        }
        assert_eq!(shifts, [(7, 100_000)]);
        assert!(add_element_shift(&mut shifts, 7, -100_000).expect("cancel shift"));
        assert!(shifts.is_empty());

        for ordinal in 0..MAX_ELEMENT_SHIFT_RECORDS as u32 {
            assert!(add_element_shift(&mut shifts, ordinal, 1).expect("insert bounded shift"));
        }
        assert!(!add_element_shift(&mut shifts, u32::MAX, 1).expect("request index rebuild"));
        assert_eq!(encode_shifts(&shifts).len(), MAX_ELEMENT_SHIFT_RECORDS * 12);
    }

    #[test]
    fn sparse_element_path_falls_back_when_edit_adds_an_element() {
        let element_and_sibling = concat!(
            r#"{"id":"a","type":"rectangle"}"#,
            ",",
            r#"{"id":"b","type":"ellipse"}"#
        );

        assert_eq!(
            Document::element_change_from_source(
                "a",
                "80".to_owned(),
                String::new(),
                element_and_sibling.to_owned(),
            )
            .expect("structural edit is an optimization miss"),
            None
        );
    }
}
