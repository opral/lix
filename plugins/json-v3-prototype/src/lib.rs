//! Fused JSON experiment for Component API v3.
#![allow(dead_code)]

#[path = "../../json-v2/src/core.rs"]
mod core;

use core::{ArenaJsonScalar, ChangeEffect, Document, EntityChange, IdNamespace, InputSplice};
use lix_plugin_api_v3_prototype as sdk;
use serde_json::Value;

struct JsonV3Prototype;

const SCALAR_INDEX_STATE: &[u8] = b"json/scalar-index-v1";
const SCALAR_SHIFTS_STATE: &[u8] = b"json/scalar-shifts-v1";
const SCALAR_INDEX_MAGIC: &[u8; 4] = b"JSS1";
const SCALAR_INDEX_HEADER_BYTES: u32 = 12;
const SCALAR_INDEX_ENTRY_BYTES: u32 = 20;
const SCALAR_PAGE_BYTES: usize = 1024 * 1024;

impl sdk::FormatPlugin for JsonV3Prototype {
    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let bytes = input.accepted.read_all()?;
        let namespace = IdNamespace::from_halves(input.creates.high, u64::from(input.creates.low));
        let (document, changes) = Document::open_file(bytes, input.file.path.as_deref(), namespace)
            .map_err(sdk::Error::invalid_input)?;
        let (index, pages) = encode_scalar_state(
            &document
                .arena_scalars()
                .map_err(sdk::Error::invalid_input)?,
        )?;
        input.successor.put_state(SCALAR_INDEX_STATE, &index)?;
        for (ordinal, page) in pages.iter().enumerate() {
            input
                .successor
                .put_state(&scalar_page_key(ordinal as u32), page)?;
        }
        emit_changes(changes, input.creates, sink)?;
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let namespace =
            IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
        let inserts = update
            .edits
            .iter()
            .map(|edit| match &edit.insert {
                sdk::SpliceInsert::Inline(bytes) => Ok(bytes.clone()),
                sdk::SpliceInsert::AfterRange { .. } => Err(sdk::Error::invalid_input(
                    "arena host must lower after-range edits to inline bytes",
                )),
            })
            .collect::<sdk::Result<Vec<_>>>()?;
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
            && let Some((change, shifts)) = sparse_scalar_change(update, edit, &inserts[0])?
        {
            update.successor.put_state(SCALAR_SHIFTS_STATE, &shifts)?;
            emit_changes([Ok(change)], update.creates, sink)?;
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
        let old_page_count = scalar_page_count(update)?;
        let (index, pages) = encode_scalar_state(
            &document
                .arena_scalars()
                .map_err(sdk::Error::invalid_input)?,
        )?;
        update.successor.put_state(SCALAR_INDEX_STATE, &index)?;
        for (ordinal, page) in pages.iter().enumerate() {
            update
                .successor
                .put_state(&scalar_page_key(ordinal as u32), page)?;
        }
        for ordinal in pages.len() as u32..old_page_count {
            update.successor.delete_state(&scalar_page_key(ordinal))?;
        }
        update.successor.delete_state(SCALAR_SHIFTS_STATE)?;
        emit_changes(changes.into_iter().map(Ok), update.creates, sink)?;
        Ok(())
    }
}

fn sparse_scalar_change(
    update: &sdk::FileUpdate<'_>,
    edit: &sdk::InputSplice,
    insert: &[u8],
) -> sdk::Result<Option<(EntityChange, Vec<u8>)>> {
    let state_len = match update.before.state_len(SCALAR_INDEX_STATE) {
        Some(length) => length,
        None => return Ok(None),
    };
    let header = update
        .before
        .read_state_range(SCALAR_INDEX_STATE, 0, SCALAR_INDEX_HEADER_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index disappeared"))?;
    if header.get(..4) != Some(SCALAR_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported JSON scalar index"));
    }
    let count = u32::from_le_bytes(header[4..8].try_into().expect("fixed JSON index header"));
    let index_end = u64::from(SCALAR_INDEX_HEADER_BYTES)
        .checked_add(u64::from(count) * u64::from(SCALAR_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index overflowed"))?;
    if index_end > state_len {
        return Err(sdk::Error::invalid_input("truncated JSON scalar index"));
    }
    let mut shifts = decode_scalar_shifts(
        update
            .before
            .get_state(SCALAR_SHIFTS_STATE)?
            .as_deref()
            .unwrap_or_default(),
    )?;
    let edit_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| sdk::Error::invalid_input("JSON edit range overflowed"))?;
    let mut low = 0_u32;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = read_scalar_entry(update, middle)?;
        if effective_scalar_start(entry.start, middle, &shifts)? <= edit.offset {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if low == 0 {
        return Ok(None);
    }
    let ordinal = low - 1;
    let entry = read_scalar_entry(update, ordinal)?;
    let start = effective_scalar_start(entry.start, ordinal, &shifts)?;
    let length = effective_scalar_length(entry.length, ordinal, &shifts)?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar range overflowed"))?;
    if edit.offset < start || edit_end > end {
        return Ok(None);
    }
    let metadata = update
        .before
        .read_state_range(
            &scalar_page_key(entry.page),
            u64::from(entry.blob_offset),
            entry.blob_len,
        )?
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar page disappeared"))?;
    let (schema_key, entity_pk, snapshot) = decode_scalar_metadata(&metadata)?;
    let mut scalar = update.before.read_range(start, length)?;
    let local_start = usize::try_from(edit.offset - start)
        .map_err(|_| sdk::Error::invalid_input("JSON edit offset exceeds guest memory"))?;
    let local_end = local_start
        .checked_add(
            usize::try_from(edit.delete_len)
                .map_err(|_| sdk::Error::invalid_input("JSON delete exceeds guest memory"))?,
        )
        .ok_or_else(|| sdk::Error::invalid_input("JSON edit range overflowed"))?;
    scalar.splice(local_start..local_end, insert.iter().copied());
    let change = Document::scalar_change_from_arena(schema_key, entity_pk, &snapshot, &scalar)
        .map_err(sdk::Error::invalid_input)?;
    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("JSON insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar shrink exceeds i64"))?
    };
    shifts.push((ordinal, delta));
    Ok(Some((change, encode_scalar_shifts(&shifts))))
}

fn encode_scalar_state(scalars: &[ArenaJsonScalar]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut pages = vec![Vec::with_capacity(SCALAR_PAGE_BYTES)];
    let mut locations = Vec::with_capacity(scalars.len());
    for scalar in scalars {
        let metadata = encode_scalar_metadata(scalar)?;
        if metadata.len() > SCALAR_PAGE_BYTES {
            return Err(sdk::Error::limit_exceeded(
                "one JSON scalar state exceeds the page limit",
            ));
        }
        if !pages.last().expect("one page").is_empty()
            && pages.last().expect("one page").len() + metadata.len() > SCALAR_PAGE_BYTES
        {
            pages.push(Vec::with_capacity(SCALAR_PAGE_BYTES));
        }
        let page = u32::try_from(pages.len() - 1)
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalar pages"))?;
        let offset = u32::try_from(pages.last().expect("one page").len())
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar page exceeds 4GiB"))?;
        pages
            .last_mut()
            .expect("one page")
            .extend_from_slice(&metadata);
        locations.push((page, offset, metadata.len() as u32));
    }
    if scalars.is_empty() {
        pages.clear();
    }
    let mut index = Vec::with_capacity(
        SCALAR_INDEX_HEADER_BYTES as usize + scalars.len() * SCALAR_INDEX_ENTRY_BYTES as usize,
    );
    index.extend_from_slice(SCALAR_INDEX_MAGIC);
    index.extend_from_slice(
        &u32::try_from(scalars.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalars"))?
            .to_le_bytes(),
    );
    index.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalar pages"))?
            .to_le_bytes(),
    );
    for (scalar, (page, offset, length)) in scalars.iter().zip(locations) {
        index.extend_from_slice(&scalar.start.to_le_bytes());
        index.extend_from_slice(&scalar.length.to_le_bytes());
        index.extend_from_slice(&page.to_le_bytes());
        index.extend_from_slice(&offset.to_le_bytes());
        index.extend_from_slice(&length.to_le_bytes());
    }
    Ok((index, pages))
}

fn encode_scalar_metadata(scalar: &ArenaJsonScalar) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    push_state_bytes(&mut output, scalar.schema_key.as_bytes())?;
    output.extend_from_slice(
        &u32::try_from(scalar.entity_pk.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON key components"))?
            .to_le_bytes(),
    );
    for component in &scalar.entity_pk {
        push_state_bytes(&mut output, component.as_bytes())?;
    }
    push_state_bytes(&mut output, &scalar.snapshot)?;
    Ok(output)
}

fn decode_scalar_metadata(bytes: &[u8]) -> sdk::Result<(String, Vec<String>, Vec<u8>)> {
    let mut input = bytes;
    let schema_key = take_state_text(&mut input)?;
    let count = take_state_u32(&mut input)? as usize;
    let mut entity_pk = Vec::with_capacity(count);
    for _ in 0..count {
        entity_pk.push(take_state_text(&mut input)?);
    }
    let snapshot = take_state_value(&mut input)?.to_vec();
    if !input.is_empty() {
        return Err(sdk::Error::invalid_input(
            "JSON scalar state has trailing bytes",
        ));
    }
    Ok((schema_key, entity_pk, snapshot))
}

#[derive(Clone, Copy)]
struct ScalarEntry {
    start: u32,
    length: u32,
    page: u32,
    blob_offset: u32,
    blob_len: u32,
}

fn read_scalar_entry(update: &sdk::FileUpdate<'_>, ordinal: u32) -> sdk::Result<ScalarEntry> {
    let offset = u64::from(SCALAR_INDEX_HEADER_BYTES)
        .checked_add(u64::from(ordinal) * u64::from(SCALAR_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index offset overflowed"))?;
    let bytes = update
        .before
        .read_state_range(SCALAR_INDEX_STATE, offset, SCALAR_INDEX_ENTRY_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index disappeared"))?;
    Ok(ScalarEntry {
        start: u32::from_le_bytes(bytes[0..4].try_into().expect("fixed scalar entry")),
        length: u32::from_le_bytes(bytes[4..8].try_into().expect("fixed scalar entry")),
        page: u32::from_le_bytes(bytes[8..12].try_into().expect("fixed scalar entry")),
        blob_offset: u32::from_le_bytes(bytes[12..16].try_into().expect("fixed scalar entry")),
        blob_len: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed scalar entry")),
    })
}

fn effective_scalar_start(base: u32, ordinal: u32, shifts: &[(u32, i64)]) -> sdk::Result<u64> {
    apply_scalar_shift(
        u64::from(base),
        shifts
            .iter()
            .filter(|(changed, _)| *changed < ordinal)
            .try_fold(0_i64, |total, (_, delta)| total.checked_add(*delta))
            .ok_or_else(|| sdk::Error::invalid_input("JSON scalar shift overflowed"))?,
    )
}

fn effective_scalar_length(base: u32, ordinal: u32, shifts: &[(u32, i64)]) -> sdk::Result<u64> {
    apply_scalar_shift(
        u64::from(base),
        shifts
            .iter()
            .filter(|(changed, _)| *changed == ordinal)
            .try_fold(0_i64, |total, (_, delta)| total.checked_add(*delta))
            .ok_or_else(|| sdk::Error::invalid_input("JSON scalar shift overflowed"))?,
    )
}

fn apply_scalar_shift(base: u64, delta: i64) -> sdk::Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64)
    } else {
        base.checked_sub(delta.unsigned_abs())
    }
    .ok_or_else(|| sdk::Error::invalid_input("JSON scalar position overflowed"))
}

fn scalar_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"json/scalar-page-v1/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn scalar_page_count(update: &sdk::FileUpdate<'_>) -> sdk::Result<u32> {
    let Some(header) =
        update
            .before
            .read_state_range(SCALAR_INDEX_STATE, 0, SCALAR_INDEX_HEADER_BYTES)?
    else {
        return Ok(0);
    };
    if header.get(..4) != Some(SCALAR_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported JSON scalar index"));
    }
    Ok(u32::from_le_bytes(
        header[8..12].try_into().expect("fixed JSON index header"),
    ))
}

fn decode_scalar_shifts(bytes: &[u8]) -> sdk::Result<Vec<(u32, i64)>> {
    if bytes.len() % 12 != 0 {
        return Err(sdk::Error::invalid_input(
            "truncated JSON scalar shift overlay",
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

fn encode_scalar_shifts(shifts: &[(u32, i64)]) -> Vec<u8> {
    let mut output = Vec::with_capacity(shifts.len() * 12);
    for (ordinal, delta) in shifts {
        output.extend_from_slice(&ordinal.to_le_bytes());
        output.extend_from_slice(&delta.to_le_bytes());
    }
    output
}

fn push_state_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .map_err(|_| sdk::Error::limit_exceeded("JSON state value exceeds 4GiB"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(bytes);
    Ok(())
}

fn take_state_u32(input: &mut &[u8]) -> sdk::Result<u32> {
    let bytes: [u8; 4] = input
        .get(..4)
        .ok_or_else(|| sdk::Error::invalid_input("truncated JSON scalar state"))?
        .try_into()
        .expect("four-byte state field");
    *input = &input[4..];
    Ok(u32::from_le_bytes(bytes))
}

fn take_state_value<'a>(input: &mut &'a [u8]) -> sdk::Result<&'a [u8]> {
    let length = take_state_u32(input)? as usize;
    let value = input
        .get(..length)
        .ok_or_else(|| sdk::Error::invalid_input("truncated JSON scalar state"))?;
    *input = &input[length..];
    Ok(value)
}

fn take_state_text(input: &mut &[u8]) -> sdk::Result<String> {
    String::from_utf8(take_state_value(input)?.to_vec())
        .map_err(|error| sdk::Error::invalid_input(format!("invalid JSON scalar state: {error}")))
}

fn emit_changes<I>(
    changes: I,
    creates: sdk::CreateContext,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<EntityChange, String>>,
{
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change.map_err(sdk::Error::invalid_input)?, creates, sink)?;
    }
    encoder.flush(sink)
}

struct BatchEncoder {
    max_bytes: usize,
    payload: Vec<u8>,
    records: u32,
    creates_only: Option<bool>,
}

impl BatchEncoder {
    fn new(max_bytes: u32) -> Self {
        Self {
            max_bytes: max_bytes as usize,
            payload: Vec::with_capacity(max_bytes as usize),
            records: 0,
            creates_only: None,
        }
    }

    fn push(
        &mut self,
        change: EntityChange,
        creates: sdk::CreateContext,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let mut record = Vec::new();
        let is_create = encode_change(change, creates, &mut record)?;
        if record.len() > self.max_bytes {
            return Err(sdk::Error::limit_exceeded(
                "one JSON entity exceeds the v3 batch limit",
            ));
        }
        if self.records > 0
            && (self.payload.len() + record.len() > self.max_bytes
                || self.creates_only != Some(is_create))
        {
            self.flush(sink)?;
        }
        self.creates_only = Some(is_create);
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("JSON batch record count overflowed"))?;
        Ok(())
    }

    fn flush(&mut self, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        if self.records == 0 {
            return Ok(());
        }
        let payload = std::mem::replace(&mut self.payload, Vec::with_capacity(self.max_bytes));
        let records = std::mem::take(&mut self.records);
        self.creates_only = None;
        sink.emit_changes(records, payload)
    }
}

fn encode_change(
    change: EntityChange,
    creates: sdk::CreateContext,
    output: &mut Vec<u8>,
) -> sdk::Result<bool> {
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    let is_create = match change.snapshot {
        Some(snapshot) => {
            let local_ref = change
                .entity_pk
                .as_slice()
                .first()
                .filter(|_| change.entity_pk.len() == 1)
                .and_then(|id| local_ref(creates, id));
            if let Some(local_ref) = local_ref {
                output.push(2);
                push_text(output, &change.schema_key)?;
                output.extend_from_slice(&u64::from(local_ref).to_le_bytes());
                push_inline_blob(output, &remove_created_id(snapshot)?)?;
                true
            } else {
                output.push(0);
                push_entity_key(output, &change.schema_key, &change.entity_pk)?;
                output.push(effect_tag(change.effect));
                push_inline_blob(output, &snapshot)?;
                false
            }
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
            false
        }
    };
    let record_len = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("JSON packet record exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&record_len.to_le_bytes());
    Ok(is_create)
}

fn local_ref(creates: sdk::CreateContext, id: &str) -> Option<u32> {
    let id = uuid::Uuid::parse_str(id).ok()?;
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
}

fn remove_created_id(snapshot: Vec<u8>) -> sdk::Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(&snapshot)
        .map_err(|error| sdk::Error::invalid_input(format!("invalid JSON snapshot: {error}")))?;
    value
        .as_object_mut()
        .and_then(|object| object.remove("id"))
        .ok_or_else(|| sdk::Error::invalid_input("created JSON snapshot has no id"))?;
    serde_json::to_vec(&value)
        .map_err(|error| sdk::Error::internal(format!("encode JSON snapshot: {error}")))
}

fn effect_tag(effect: ChangeEffect) -> u8 {
    match effect {
        ChangeEffect::Content => 0,
        ChangeEffect::FormatOnly => 1,
    }
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
lix_plugin_api_v3_prototype::export_v3_prototype!(JsonV3Prototype);
