//! Markdown support for the fused Component API v3.
#![allow(dead_code)]

mod core;
mod markdown_file;
mod model;
mod schemas;

use core::{
    ArenaMarkdownBlock, ChangeEffect, Document, EntityChange, IdNamespace, InputSplice,
    NODE_SCHEMA_KEY, PluginError,
};
use lix_plugin_api as sdk;
use serde_json::Value;

struct MarkdownPlugin;

const ID_NAMESPACE_STATE: &[u8] = b"markdown/id-namespace-v1";
const ROOT_STATE: &[u8] = b"markdown/root-v1";
const BLOCKS_STATE: &[u8] = b"markdown/blocks-v1";
const BLOCK_SHIFTS_STATE: &[u8] = b"markdown/block-shifts-v1";
const NEXT_ID_ORDINAL_STATE: &[u8] = b"markdown/next-id-ordinal-v1";
const LEXICAL_FALLBACK_FIELD: &str = "lexical_fallback_base64";
const BLOCK_INDEX_MAGIC: &[u8; 4] = b"MDB2";
const BLOCK_INDEX_HEADER_BYTES: u32 = 16;
const BLOCK_INDEX_ENTRY_BYTES: u32 = 28;
const BLOCK_PAGE_BYTES: usize = 1024 * 1024;

impl sdk::FormatPlugin for MarkdownPlugin {
    fn entities_changed(
        update: &mut sdk::EntityUpdate<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let before = update.before.read_all()?;
        let mut changes = Vec::new();
        while let Some(change) = update.changes.next()? {
            changes.push(EntityChange {
                schema_key: change.schema_key,
                entity_pk: change.entity_pk,
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
        let (document, _) = Document::open_file(
            before.clone(),
            update.before_file.path.as_deref(),
            namespace,
        )
        .map_err(core_error)?;
        let (successor, edits) = document.entities_changed(changes).map_err(core_error)?;
        sink.replace_file(&apply_edits(before, &edits)?)?;
        store_rendered_markdown_state(&update.before, sink, &successor)?;
        Ok(())
    }

    fn resolve_conflict(conflict: sdk::EntityConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
        let Some(b) = conflict.b.as_ref() else {
            return Ok(sdk::ConflictResolution::Delete);
        };
        if conflict.schema_key != NODE_SCHEMA_KEY {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let (Some(base), Some(a)) = (&conflict.base, &conflict.a) else {
            return Ok(sdk::ConflictResolution::TakeB);
        };
        if base.len() > 64 * 1024 || a.len() > 64 * 1024 || b.len() > 64 * 1024 {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let base = base.read()?;
        let a = a.read()?;
        let b = b.read()?;
        let resolved =
            Document::resolve_entity_conflict(Some(base.clone()), Some(a.clone()), Some(b.clone()));
        Ok(match resolved {
            None => sdk::ConflictResolution::Delete,
            Some(resolved) if resolved == b => sdk::ConflictResolution::TakeB,
            Some(resolved) if resolved == a => sdk::ConflictResolution::TakeA,
            Some(resolved) if resolved == base => sdk::ConflictResolution::TakeBase,
            Some(resolved) => sdk::ConflictResolution::Replace(resolved),
        })
    }

    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let bytes = input.accepted.read_all()?;
        let namespace = IdNamespace::from_halves(input.creates.high, input.creates.low);
        input
            .successor
            .put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        let (document, mut changes) =
            Document::open_file(bytes, input.file.path.as_deref(), namespace)
                .map_err(core_error)?;
        strip_duplicated_lexical_fallback(&mut changes)?;
        store_markdown_state(&input.successor, &document, input.creates)?;
        emit_changes(changes, input.creates, None, sink)?;
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
            && let Some((
                changes,
                create_from_ordinal,
                next_ordinal,
                root,
                block_key,
                block,
                shifts,
            )) = sparse_block_change(update, edit, &inserts[0], namespace)?
        {
            update.successor.put_state(ROOT_STATE, &root)?;
            update.successor.put_state(&block_key, &block)?;
            update.successor.put_state(BLOCK_SHIFTS_STATE, &shifts)?;
            update
                .successor
                .put_state(NEXT_ID_ORDINAL_STATE, &next_ordinal.to_le_bytes())?;
            emit_changes(changes, update.creates, Some(create_from_ordinal), sink)?;
            return Ok(());
        }

        let (document, _) = Document::open_file(
            update.before.read_all()?,
            update.before_file.path.as_deref(),
            namespace,
        )
        .map_err(core_error)?;
        let next_ordinal = read_next_ordinal(update)?;
        let (document, mut changes) = document
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        strip_duplicated_lexical_fallback(&mut changes)?;
        let (root, blocks) = document.arena_state().map_err(core_error)?;
        update.successor.put_state(ROOT_STATE, &root)?;
        let (old_index_pages, old_block_pages) = block_page_counts(&update.before)?;
        let encoded = encode_blocks(&blocks)?;
        update
            .successor
            .put_state(BLOCKS_STATE, &encoded.manifest)?;
        for (ordinal, page) in encoded.index_pages.iter().enumerate() {
            update
                .successor
                .put_state(&block_index_page_key(ordinal as u32), page)?;
        }
        for ordinal in encoded.index_pages.len() as u32..old_index_pages {
            update
                .successor
                .delete_state(&block_index_page_key(ordinal))?;
        }
        for (ordinal, page) in encoded.block_pages.iter().enumerate() {
            update
                .successor
                .put_state(&block_page_key(ordinal as u32), page)?;
        }
        for ordinal in encoded.block_pages.len() as u32..old_block_pages {
            update.successor.delete_state(&block_page_key(ordinal))?;
        }
        if let Some(shifts) = update.before.get_state(BLOCK_SHIFTS_STATE)? {
            for (ordinal, _) in decode_block_shifts(&shifts)? {
                update.successor.delete_state(&block_overlay_key(ordinal))?;
            }
        }
        update.successor.delete_state(BLOCK_SHIFTS_STATE)?;
        let successor_next = successor_next_ordinal(&changes, update.creates, next_ordinal)?;
        update
            .successor
            .put_state(NEXT_ID_ORDINAL_STATE, &successor_next.to_le_bytes())?;
        emit_changes(changes, update.creates, Some(next_ordinal), sink)?;
        Ok(())
    }
}

fn store_rendered_markdown_state(
    before: &sdk::Root<'_>,
    sink: &mut sdk::Sink<'_>,
    document: &Document,
) -> sdk::Result<()> {
    let (root, blocks) = document.arena_state().map_err(core_error)?;
    sink.put_state(ROOT_STATE, &root)?;
    let (old_index_pages, old_block_pages) = block_page_counts(before)?;
    let encoded = encode_blocks(&blocks)?;
    sink.put_state(BLOCKS_STATE, &encoded.manifest)?;
    for (ordinal, page) in encoded.index_pages.iter().enumerate() {
        sink.put_state(&block_index_page_key(ordinal as u32), page)?;
    }
    for ordinal in encoded.index_pages.len() as u32..old_index_pages {
        sink.delete_state(&block_index_page_key(ordinal))?;
    }
    for (ordinal, page) in encoded.block_pages.iter().enumerate() {
        sink.put_state(&block_page_key(ordinal as u32), page)?;
    }
    for ordinal in encoded.block_pages.len() as u32..old_block_pages {
        sink.delete_state(&block_page_key(ordinal))?;
    }
    if let Some(shifts) = before.get_state(BLOCK_SHIFTS_STATE)? {
        for (ordinal, _) in decode_block_shifts(&shifts)? {
            sink.delete_state(&block_overlay_key(ordinal))?;
        }
    }
    sink.delete_state(BLOCK_SHIFTS_STATE)?;
    Ok(())
}

fn read_namespace(root: &sdk::Root<'_>) -> sdk::Result<Option<IdNamespace>> {
    let Some(bytes) = root.get_state(ID_NAMESPACE_STATE)? else {
        return Ok(None);
    };
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("Markdown ID namespace has invalid length"))?;
    Ok(Some(IdNamespace::from_halves(
        u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        u32::from_be_bytes(bytes[8..].try_into().expect("four bytes")),
    )))
}

fn namespace_from_changes(changes: &[EntityChange]) -> Option<IdNamespace> {
    changes
        .iter()
        .flat_map(|change| &change.entity_pk)
        .find_map(|component| uuid::Uuid::parse_str(component).ok())
        .map(|id| {
            let bytes = id.into_bytes();
            IdNamespace::from_halves(
                u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
                u32::from_be_bytes(bytes[8..12].try_into().expect("four bytes")),
            )
        })
}

fn apply_edits(mut bytes: Vec<u8>, edits: &[core::ByteEdit]) -> sdk::Result<Vec<u8>> {
    for edit in edits.iter().rev() {
        let start = usize::try_from(edit.offset)
            .map_err(|_| sdk::Error::invalid_input("Markdown edit offset exceeds guest memory"))?;
        let end = start
            .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                sdk::Error::invalid_input("Markdown edit deletion exceeds guest memory")
            })?)
            .ok_or_else(|| sdk::Error::invalid_input("Markdown edit range overflowed"))?;
        if end > bytes.len() {
            return Err(sdk::Error::invalid_input(
                "Markdown edit exceeds accepted bytes",
            ));
        }
        bytes.splice(start..end, edit.insert.iter().copied());
    }
    Ok(bytes)
}

fn store_markdown_state(
    successor: &sdk::Transaction,
    document: &Document,
    creates: sdk::CreateContext,
) -> sdk::Result<()> {
    let (root, blocks) = document.arena_state().map_err(core_error)?;
    successor.put_state(ROOT_STATE, &root)?;
    let encoded = encode_blocks(&blocks)?;
    successor.put_state(BLOCKS_STATE, &encoded.manifest)?;
    for (ordinal, page) in encoded.index_pages.iter().enumerate() {
        successor.put_state(&block_index_page_key(ordinal as u32), page)?;
    }
    for (ordinal, page) in encoded.block_pages.iter().enumerate() {
        successor.put_state(&block_page_key(ordinal as u32), page)?;
    }
    let next_ordinal = next_arena_ordinal(&root, &blocks, creates)?;
    successor.put_state(NEXT_ID_ORDINAL_STATE, &next_ordinal.to_le_bytes())?;
    Ok(())
}

type SparseBlockResult = (
    Vec<EntityChange>,
    u32,
    u32,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
);

fn sparse_block_change(
    update: &sdk::FileUpdate<'_>,
    edit: &sdk::InputSplice,
    insert: &[u8],
    namespace: IdNamespace,
) -> sdk::Result<Option<SparseBlockResult>> {
    match update.before.state_len(BLOCKS_STATE)? {
        Some(_) => {}
        None => return Ok(None),
    }
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
    let create_from_ordinal = read_next_ordinal(update)?;
    let block_len = end
        .checked_sub(start)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block range is inverted"))?;
    let before = update.before.read_range(start, block_len)?;
    let Some((changes, root, block)) = Document::file_changed_from_arena_block(
        before,
        &root,
        &block,
        0,
        block_len,
        InputSplice {
            offset: edit.offset - start,
            delete_len: edit.delete_len,
            insert,
        },
        namespace,
        create_from_ordinal,
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
    let next_ordinal = successor_next_ordinal(&changes, update.creates, create_from_ordinal)?;
    Ok(Some((
        changes,
        create_from_ordinal,
        next_ordinal,
        root,
        block_key,
        block,
        encode_block_shifts(&shifts),
    )))
}

struct EncodedBlocks {
    manifest: Vec<u8>,
    index_pages: Vec<Vec<u8>>,
    block_pages: Vec<Vec<u8>>,
}

fn encode_blocks(blocks: &[ArenaMarkdownBlock]) -> sdk::Result<EncodedBlocks> {
    let mut block_pages = vec![Vec::with_capacity(BLOCK_PAGE_BYTES)];
    let mut locations = Vec::with_capacity(blocks.len());
    for block in blocks {
        if block.tree_json.len() > BLOCK_PAGE_BYTES {
            return Err(sdk::Error::limit_exceeded(
                "one Markdown block exceeds the state-page limit",
            ));
        }
        if !block_pages.last().expect("one page").is_empty()
            && block_pages.last().expect("one page").len() + block.tree_json.len()
                > BLOCK_PAGE_BYTES
        {
            block_pages.push(Vec::with_capacity(BLOCK_PAGE_BYTES));
        }
        let page = u32::try_from(block_pages.len() - 1)
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block pages"))?;
        let blob_offset = u32::try_from(block_pages.last().expect("one page").len())
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block page exceeds 4GiB"))?;
        block_pages
            .last_mut()
            .expect("one page")
            .extend_from_slice(&block.tree_json);
        locations.push((page, blob_offset));
    }
    if blocks.is_empty() {
        block_pages.clear();
    }
    let mut entries = Vec::with_capacity(
        blocks
            .len()
            .checked_mul(BLOCK_INDEX_ENTRY_BYTES as usize)
            .ok_or_else(|| sdk::Error::limit_exceeded("Markdown block index overflowed"))?,
    );
    for (block, (page, blob_offset)) in blocks.iter().zip(locations) {
        entries.extend_from_slice(&block.start.to_le_bytes());
        entries.extend_from_slice(&block.end.to_le_bytes());
        entries.extend_from_slice(&page.to_le_bytes());
        entries.extend_from_slice(&blob_offset.to_le_bytes());
        entries.extend_from_slice(
            &u32::try_from(block.tree_json.len())
                .map_err(|_| sdk::Error::limit_exceeded("Markdown block state exceeds 4GiB"))?
                .to_le_bytes(),
        );
    }
    let entries_per_page = BLOCK_PAGE_BYTES / BLOCK_INDEX_ENTRY_BYTES as usize;
    let index_page_bytes = entries_per_page * BLOCK_INDEX_ENTRY_BYTES as usize;
    let index_pages = entries
        .chunks(index_page_bytes)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let mut manifest = Vec::with_capacity(BLOCK_INDEX_HEADER_BYTES as usize);
    manifest.extend_from_slice(BLOCK_INDEX_MAGIC);
    manifest.extend_from_slice(
        &u32::try_from(blocks.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown blocks"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(block_pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block pages"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(index_pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block index pages"))?
            .to_le_bytes(),
    );
    Ok(EncodedBlocks {
        manifest,
        index_pages,
        block_pages,
    })
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
    let entries_per_page = (BLOCK_PAGE_BYTES / BLOCK_INDEX_ENTRY_BYTES as usize) as u32;
    let page = ordinal / entries_per_page;
    let offset = u64::from(ordinal % entries_per_page) * u64::from(BLOCK_INDEX_ENTRY_BYTES);
    let bytes = update
        .before
        .read_state_range(&block_index_page_key(page), offset, BLOCK_INDEX_ENTRY_BYTES)?
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

fn block_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"markdown/block-index-page-v2/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn block_page_counts(root: &sdk::Root<'_>) -> sdk::Result<(u32, u32)> {
    let Some(header) = root.read_state_range(BLOCKS_STATE, 0, BLOCK_INDEX_HEADER_BYTES)? else {
        return Ok((0, 0));
    };
    if header.get(..4) != Some(BLOCK_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Markdown block index",
        ));
    }
    Ok((
        u32::from_le_bytes(header[12..16].try_into().expect("fixed Markdown header")),
        u32::from_le_bytes(header[8..12].try_into().expect("fixed Markdown header")),
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

fn strip_duplicated_lexical_fallback(changes: &mut [EntityChange]) -> sdk::Result<()> {
    for change in changes {
        let Some(snapshot) = &mut change.snapshot else {
            continue;
        };
        if !snapshot
            .windows(LEXICAL_FALLBACK_FIELD.len())
            .any(|window| window == LEXICAL_FALLBACK_FIELD.as_bytes())
        {
            continue;
        }
        let mut value: Value = serde_json::from_slice(snapshot).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid Markdown root snapshot: {error}"))
        })?;
        let Some(format_json) = value
            .as_object_mut()
            .and_then(|object| object.get_mut("format_json"))
            .and_then(|value| value.as_str())
        else {
            continue;
        };
        let mut format: Value = serde_json::from_str(format_json).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid Markdown root format_json: {error}"))
        })?;
        if format
            .as_object_mut()
            .and_then(|format| format.remove(LEXICAL_FALLBACK_FIELD))
            .is_none()
        {
            continue;
        }
        value
            .as_object_mut()
            .expect("validated Markdown wire snapshot is an object")
            .insert(
                "format_json".to_owned(),
                Value::String(serde_json::to_string(&format).map_err(|error| {
                    sdk::Error::internal(format!("encode compact Markdown root format: {error}"))
                })?),
            );
        *snapshot = serde_json::to_vec(&value).map_err(|error| {
            sdk::Error::internal(format!("encode compact Markdown root snapshot: {error}"))
        })?;
    }
    Ok(())
}

fn next_arena_ordinal(
    root: &[u8],
    blocks: &[ArenaMarkdownBlock],
    creates: sdk::CreateContext,
) -> sdk::Result<u32> {
    let root = root
        .get(1..)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown arena root is empty"))?;
    let mut next = next_json_ordinal(root, creates, 0)?;
    for block in blocks {
        next = next_json_ordinal(&block.tree_json, creates, next)?;
    }
    Ok(next)
}

fn next_json_ordinal(bytes: &[u8], creates: sdk::CreateContext, current: u32) -> sdk::Result<u32> {
    fn visit(value: &Value, creates: sdk::CreateContext, next: &mut u32) -> sdk::Result<()> {
        match value {
            Value::Object(object) => {
                if let Some(ordinal) = object
                    .get("id")
                    .and_then(Value::as_str)
                    .and_then(|id| local_ref(creates, id))
                {
                    *next = (*next).max(ordinal.checked_add(1).ok_or_else(|| {
                        sdk::Error::limit_exceeded("Markdown ID ordinal space is exhausted")
                    })?);
                }
                for value in object.values() {
                    visit(value, creates, next)?;
                }
            }
            Value::Array(values) => {
                for value in values {
                    visit(value, creates, next)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let value: Value = serde_json::from_slice(bytes).map_err(|error| {
        sdk::Error::invalid_input(format!("invalid Markdown arena JSON: {error}"))
    })?;
    let mut next = current;
    visit(&value, creates, &mut next)?;
    Ok(next)
}

fn read_next_ordinal(update: &sdk::FileUpdate<'_>) -> sdk::Result<u32> {
    let bytes = update
        .before
        .get_state(NEXT_ID_ORDINAL_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown arena has no ID high-water mark"))?;
    let bytes: [u8; 4] = bytes.try_into().map_err(|_| {
        sdk::Error::invalid_input("Markdown arena ID high-water mark has invalid length")
    })?;
    Ok(u32::from_le_bytes(bytes))
}

fn successor_next_ordinal(
    changes: &[EntityChange],
    creates: sdk::CreateContext,
    current: u32,
) -> sdk::Result<u32> {
    let mut next = current;
    for change in changes {
        if let Some(id) = change
            .entity_pk
            .first()
            .filter(|_| change.entity_pk.len() == 1)
            && let Some(ordinal) = local_ref(creates, id)
        {
            next = next.max(ordinal.checked_add(1).ok_or_else(|| {
                sdk::Error::limit_exceeded("Markdown ID ordinal space is exhausted")
            })?);
        }
        if let Some(snapshot) = &change.snapshot {
            next = next_json_ordinal(snapshot, creates, next)?;
        }
    }
    Ok(next)
}

fn emit_changes(
    changes: impl IntoIterator<Item = EntityChange>,
    creates: sdk::CreateContext,
    create_from_ordinal: Option<u32>,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()> {
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change, creates, create_from_ordinal, sink)?;
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
        create_from_ordinal: Option<u32>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let mut record = Vec::new();
        let is_create = encode_change(change, creates, create_from_ordinal, &mut record)?;
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
    create_from_ordinal: Option<u32>,
    output: &mut Vec<u8>,
) -> sdk::Result<bool> {
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match change.snapshot {
        Some(snapshot) => {
            let local_ref = change
                .entity_pk
                .first()
                .filter(|_| change.entity_pk.len() == 1)
                .and_then(|id| local_ref(creates, id))
                .filter(|ordinal| create_from_ordinal.is_none_or(|minimum| *ordinal >= minimum));
            if let Some(local_ref) = local_ref {
                output.push(2);
                push_text(output, &change.schema_key)?;
                output.extend_from_slice(&u64::from(local_ref).to_le_bytes());
                push_inline_blob(output, &remove_created_id(snapshot)?)?;
            } else {
                output.push(0);
                push_entity_key(output, &change.schema_key, &change.entity_pk)?;
                output.push(effect_tag(change.effect));
                push_inline_blob(output, &snapshot)?;
            }
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
        }
    }
    let record_len = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("Markdown packet record exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&record_len.to_le_bytes());
    Ok(output[record_start + 4] == 2)
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
lix_plugin_api::export_plugin!(MarkdownPlugin);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_block_index_is_split_into_bounded_pages() {
        let blocks = (0..80_000_u64)
            .map(|ordinal| ArenaMarkdownBlock {
                start: ordinal,
                end: ordinal + 1,
                tree_json: Vec::new(),
            })
            .collect::<Vec<_>>();

        let encoded = encode_blocks(&blocks).expect("encode block index");

        assert_eq!(encoded.manifest.len(), BLOCK_INDEX_HEADER_BYTES as usize);
        assert!(encoded.index_pages.len() > 1);
        assert!(
            encoded
                .index_pages
                .iter()
                .all(|page| page.len() <= BLOCK_PAGE_BYTES)
        );
        assert_eq!(
            encoded
                .index_pages
                .iter()
                .map(|page| page.len())
                .sum::<usize>(),
            blocks.len() * BLOCK_INDEX_ENTRY_BYTES as usize
        );
    }
}
