//! Fused Markdown experiment for Component API v3.
#![allow(dead_code)]

#[path = "../../markdown-v2/src/core.rs"]
mod core;
#[path = "../../markdown-v2/src/markdown_file.rs"]
mod markdown_file;
#[path = "../../markdown-v2/src/model.rs"]
mod model;
#[path = "../../markdown-v2/src/schemas.rs"]
mod schemas;

use core::{
    ArenaMarkdownBlock, ChangeEffect, Document, EntityChange, IdNamespace, InputSplice, PluginError,
};
use lix_plugin_api_v3_prototype as sdk;
use serde_json::Value;

struct MarkdownV3Prototype;

const ID_NAMESPACE_STATE: &[u8] = b"markdown/id-namespace-v1";
const ROOT_STATE: &[u8] = b"markdown/root-v1";
const BLOCKS_STATE: &[u8] = b"markdown/blocks-v1";
const BLOCK_SHIFTS_STATE: &[u8] = b"markdown/block-shifts-v1";
const BLOCK_INDEX_MAGIC: &[u8; 4] = b"MDB1";
const BLOCK_INDEX_HEADER_BYTES: u32 = 12;
const BLOCK_INDEX_ENTRY_BYTES: u32 = 28;
const BLOCK_PAGE_BYTES: usize = 1024 * 1024;

impl sdk::FormatPlugin for MarkdownV3Prototype {
    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let bytes = input.accepted.read_all()?;
        let namespace = IdNamespace::from_halves(input.creates.high, input.creates.low);
        input
            .successor
            .put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        let (document, changes) = Document::open_file(bytes, input.file.path.as_deref(), namespace)
            .map_err(core_error)?;
        let (root, blocks) = document.arena_state().map_err(core_error)?;
        input.successor.put_state(ROOT_STATE, &root)?;
        let (index, pages) = encode_blocks(&blocks)?;
        input.successor.put_state(BLOCKS_STATE, &index)?;
        for (ordinal, page) in pages.iter().enumerate() {
            input
                .successor
                .put_state(&block_page_key(ordinal as u32), page)?;
        }
        emit_changes(changes, input.creates, sink)?;
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let namespace = update
            .before
            .get_state(ID_NAMESPACE_STATE)?
            .ok_or_else(|| sdk::Error::invalid_input("Markdown arena root has no ID namespace"))?;
        let namespace: [u8; 12] = namespace.try_into().map_err(|_| {
            sdk::Error::invalid_input("Markdown arena ID namespace has invalid length")
        })?;
        let namespace = IdNamespace::from_halves(
            u64::from_be_bytes(
                namespace[..8]
                    .try_into()
                    .expect("eight-byte namespace prefix"),
            ),
            u32::from_be_bytes(
                namespace[8..]
                    .try_into()
                    .expect("four-byte namespace suffix"),
            ),
        );
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
            && let Some((changes, root, block_key, block, shifts)) =
                sparse_block_change(update, edit, &inserts[0], namespace)?
        {
            update.successor.put_state(ROOT_STATE, &root)?;
            update.successor.put_state(&block_key, &block)?;
            update.successor.put_state(BLOCK_SHIFTS_STATE, &shifts)?;
            emit_changes(changes, update.creates, sink)?;
            return Ok(());
        }

        let (document, _) = Document::open_file(
            update.before.read_all()?,
            update.before_file.path.as_deref(),
            namespace,
        )
        .map_err(core_error)?;
        let (document, changes) = document
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        let (root, blocks) = document.arena_state().map_err(core_error)?;
        update.successor.put_state(ROOT_STATE, &root)?;
        let old_page_count = block_page_count(update)?;
        let (index, pages) = encode_blocks(&blocks)?;
        update.successor.put_state(BLOCKS_STATE, &index)?;
        for (ordinal, page) in pages.iter().enumerate() {
            update
                .successor
                .put_state(&block_page_key(ordinal as u32), page)?;
        }
        for ordinal in pages.len() as u32..old_page_count {
            update.successor.delete_state(&block_page_key(ordinal))?;
        }
        if let Some(shifts) = update.before.get_state(BLOCK_SHIFTS_STATE)? {
            for (ordinal, _) in decode_block_shifts(&shifts)? {
                update.successor.delete_state(&block_overlay_key(ordinal))?;
            }
        }
        update.successor.delete_state(BLOCK_SHIFTS_STATE)?;
        emit_changes(changes, update.creates, sink)?;
        Ok(())
    }
}

type SparseBlockResult = (Vec<EntityChange>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

fn sparse_block_change(
    update: &sdk::FileUpdate<'_>,
    edit: &sdk::InputSplice,
    insert: &[u8],
    namespace: IdNamespace,
) -> sdk::Result<Option<SparseBlockResult>> {
    let state_len = match update.before.state_len(BLOCKS_STATE) {
        Some(length) => length,
        None => return Ok(None),
    };
    let header = update
        .before
        .read_state_range(BLOCKS_STATE, 0, BLOCK_INDEX_HEADER_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block index disappeared"))?;
    if header.get(..4) != Some(BLOCK_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Markdown block index",
        ));
    }
    let count = u32::from_le_bytes(header[4..8].try_into().expect("fixed Markdown header"));
    let index_end = u64::from(BLOCK_INDEX_HEADER_BYTES)
        .checked_add(u64::from(count) * u64::from(BLOCK_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block index overflowed"))?;
    if index_end > state_len {
        return Err(sdk::Error::invalid_input("truncated Markdown block index"));
    }
    let mut shifts = decode_block_shifts(
        update
            .before
            .get_state(BLOCK_SHIFTS_STATE)?
            .as_deref()
            .unwrap_or_default(),
    )?;
    let edit_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown edit range overflowed"))?;
    let mut low = 0_u32;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let entry = read_block_entry(update, middle)?;
        if effective_block_position(entry.start, middle, &shifts)? <= edit.offset {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if low == 0 {
        return Ok(None);
    }
    let ordinal = low - 1;
    let entry = read_block_entry(update, ordinal)?;
    let start = effective_block_position(entry.start, ordinal, &shifts)?;
    let end = effective_block_position(entry.end, ordinal + 1, &shifts)?;
    if edit.offset < start || edit_end > end {
        return Ok(None);
    }
    let block_key = block_overlay_key(ordinal);
    let block = match update.before.get_state(&block_key)? {
        Some(block) => block,
        None => update
            .before
            .read_state_range(
                &block_page_key(entry.page),
                u64::from(entry.blob_offset),
                entry.blob_len,
            )?
            .ok_or_else(|| sdk::Error::invalid_input("Markdown block index disappeared"))?,
    };
    let root = update
        .before
        .get_state(ROOT_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown arena root is missing"))?;
    let before = update.before.read_all()?;
    let Some((changes, root, block)) = Document::file_changed_from_arena_block(
        before,
        &root,
        &block,
        start,
        end,
        InputSplice {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert,
        },
        namespace,
    )
    .map_err(core_error)?
    else {
        return Ok(None);
    };
    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("Markdown insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block shrink exceeds i64"))?
    };
    shifts.push((ordinal, delta));
    Ok(Some((
        changes,
        root,
        block_key,
        block,
        encode_block_shifts(&shifts),
    )))
}

fn encode_blocks(blocks: &[ArenaMarkdownBlock]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let entries_bytes = blocks
        .len()
        .checked_mul(BLOCK_INDEX_ENTRY_BYTES as usize)
        .ok_or_else(|| sdk::Error::limit_exceeded("Markdown block index overflowed"))?;
    let mut pages = vec![Vec::with_capacity(BLOCK_PAGE_BYTES)];
    let mut locations = Vec::with_capacity(blocks.len());
    for block in blocks {
        if block.tree_json.len() > BLOCK_PAGE_BYTES {
            return Err(sdk::Error::limit_exceeded(
                "one Markdown block exceeds the state-page limit",
            ));
        }
        if !pages.last().expect("one page").is_empty()
            && pages.last().expect("one page").len() + block.tree_json.len() > BLOCK_PAGE_BYTES
        {
            pages.push(Vec::with_capacity(BLOCK_PAGE_BYTES));
        }
        let page = u32::try_from(pages.len() - 1)
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block pages"))?;
        let blob_offset = u32::try_from(pages.last().expect("one page").len())
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block page exceeds 4GiB"))?;
        pages
            .last_mut()
            .expect("one page")
            .extend_from_slice(&block.tree_json);
        locations.push((page, blob_offset));
    }
    if blocks.is_empty() {
        pages.clear();
    }
    let mut output = Vec::with_capacity(BLOCK_INDEX_HEADER_BYTES as usize + entries_bytes);
    output.extend_from_slice(BLOCK_INDEX_MAGIC);
    output.extend_from_slice(
        &u32::try_from(blocks.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown blocks"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block pages"))?
            .to_le_bytes(),
    );
    for (block, (page, blob_offset)) in blocks.iter().zip(locations) {
        output.extend_from_slice(&block.start.to_le_bytes());
        output.extend_from_slice(&block.end.to_le_bytes());
        output.extend_from_slice(&page.to_le_bytes());
        output.extend_from_slice(&blob_offset.to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(block.tree_json.len())
                .map_err(|_| sdk::Error::limit_exceeded("Markdown block state exceeds 4GiB"))?
                .to_le_bytes(),
        );
    }
    Ok((output, pages))
}

#[derive(Clone, Copy)]
struct BlockEntry {
    start: u64,
    end: u64,
    page: u32,
    blob_offset: u32,
    blob_len: u32,
}

fn read_block_entry(update: &sdk::FileUpdate<'_>, ordinal: u32) -> sdk::Result<BlockEntry> {
    let offset = u64::from(BLOCK_INDEX_HEADER_BYTES)
        .checked_add(u64::from(ordinal) * u64::from(BLOCK_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block index offset overflowed"))?;
    let bytes = update
        .before
        .read_state_range(BLOCKS_STATE, offset, BLOCK_INDEX_ENTRY_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block index disappeared"))?;
    Ok(BlockEntry {
        start: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed block entry")),
        end: u64::from_le_bytes(bytes[8..16].try_into().expect("fixed block entry")),
        page: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed block entry")),
        blob_offset: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed block entry")),
        blob_len: u32::from_le_bytes(bytes[24..28].try_into().expect("fixed block entry")),
    })
}

fn block_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"markdown/block-page-v1/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn block_page_count(update: &sdk::FileUpdate<'_>) -> sdk::Result<u32> {
    let Some(header) = update
        .before
        .read_state_range(BLOCKS_STATE, 0, BLOCK_INDEX_HEADER_BYTES)?
    else {
        return Ok(0);
    };
    if header.get(..4) != Some(BLOCK_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Markdown block index",
        ));
    }
    Ok(u32::from_le_bytes(
        header[8..12].try_into().expect("fixed Markdown header"),
    ))
}

fn effective_block_position(base: u64, ordinal: u32, shifts: &[(u32, i64)]) -> sdk::Result<u64> {
    let delta = shifts
        .iter()
        .filter(|(changed, _)| *changed < ordinal)
        .try_fold(0_i64, |total, (_, delta)| total.checked_add(*delta))
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block shift overflowed"))?;
    if delta >= 0 {
        base.checked_add(delta as u64)
    } else {
        base.checked_sub(delta.unsigned_abs())
    }
    .ok_or_else(|| sdk::Error::invalid_input("Markdown block position overflowed"))
}

fn block_overlay_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"markdown/block-overlay-v1/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn decode_block_shifts(bytes: &[u8]) -> sdk::Result<Vec<(u32, i64)>> {
    if bytes.len() % 12 != 0 {
        return Err(sdk::Error::invalid_input(
            "truncated Markdown block shift overlay",
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

fn encode_block_shifts(shifts: &[(u32, i64)]) -> Vec<u8> {
    let mut output = Vec::with_capacity(shifts.len() * 12);
    for (ordinal, delta) in shifts {
        output.extend_from_slice(&ordinal.to_le_bytes());
        output.extend_from_slice(&delta.to_le_bytes());
    }
    output
}

fn core_error(error: PluginError) -> sdk::Error {
    match error {
        PluginError::InvalidInput(message) => sdk::Error::invalid_input(message),
        PluginError::Internal(message) => sdk::Error::internal(message),
    }
}

fn emit_changes(
    changes: impl IntoIterator<Item = EntityChange>,
    creates: sdk::CreateContext,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()> {
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change, creates, sink)?;
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
                "one Markdown entity exceeds the v3 batch limit",
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
            .ok_or_else(|| sdk::Error::limit_exceeded("Markdown batch count overflowed"))?;
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
        .map_err(|_| sdk::Error::limit_exceeded("Markdown packet record exceeds 4GiB"))?;
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
    let mut value: Value = serde_json::from_slice(&snapshot).map_err(|error| {
        sdk::Error::invalid_input(format!("invalid Markdown snapshot: {error}"))
    })?;
    value
        .as_object_mut()
        .and_then(|object| object.remove("id"))
        .ok_or_else(|| sdk::Error::invalid_input("created Markdown snapshot has no id"))?;
    serde_json::to_vec(&value)
        .map_err(|error| sdk::Error::internal(format!("encode Markdown snapshot: {error}")))
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
lix_plugin_api_v3_prototype::export_v3_prototype!(MarkdownV3Prototype);
