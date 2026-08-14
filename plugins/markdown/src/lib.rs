//! Markdown support for the fused Component API v1.
#![allow(dead_code)]

mod core;
mod markdown_file;
mod model;
mod order_key;
mod schemas;

use core::{
    ArenaMarkdownBlock, ChangeEffect, Document, FileEdit, IdNamespace, NODE_SCHEMA_KEY,
    PluginError, RowChange, RowRecord,
};
use lix::plugin as sdk;
use serde_json::Value;

struct MarkdownPlugin;

const ROOT_STATE: &[u8] = b"markdown/root";
const BLOCKS_STATE: &[u8] = b"markdown/blocks";
const BLOCK_SHIFTS_STATE: &[u8] = b"markdown/block-shifts";
const LEXICAL_FALLBACK_FIELD: &str = "lexical_fallback_base64";
const LEXICAL_SOURCE_REQUIRED_FIELD: &str = "lexical_source_required";
const BLOCK_INDEX_MAGIC: &[u8; 4] = b"MDB2";
const BLOCK_INDEX_HEADER_BYTES: u32 = 16;
const BLOCK_INDEX_ENTRY_BYTES: u32 = 28;
const BLOCK_PAGE_BYTES: usize = 1024 * 1024;
const MAX_BLOCK_SHIFT_RECORDS: usize = 4096;

impl sdk::Plugin for MarkdownPlugin {
    fn cold_file_changed(
        update: &mut sdk::ColdUpdate<'_>,
        sink: &mut sdk::Output<'_>,
    ) -> sdk::Result<()> {
        let accepted = update.before.read_all()?;
        let mut records = Vec::new();
        while let Some(row) = update.rows.next()? {
            records.push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                snapshot: row.snapshot,
            });
        }
        let (document, _) = Document::open_rows(records, Some(accepted)).map_err(core_error)?;
        let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
        let inserts = update
            .edits
            .iter()
            .map(|edit| edit.insert.clone())
            .collect::<Vec<_>>();
        let splices = update
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let (document, mut changes) = document
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        strip_duplicated_lexical_fallback(&mut changes)?;
        store_markdown_state(sink, &document)?;
        emit_changes(changes, update.creates, Some(0), sink)?;
        Ok(())
    }

    fn rows_changed(
        update: &mut sdk::RowUpdate<'_>,
        sink: &mut sdk::Output<'_>,
    ) -> sdk::Result<()> {
        let before = update.before.read_all()?;
        let mut changes = Vec::new();
        while let Some(change) = update.changes.next()? {
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
        let document = load_markdown_document(&update.before, before.clone())?;
        let (successor, edits) = document.rows_changed(changes).map_err(core_error)?;
        sink.replace_file(&apply_edits(before, &edits)?)?;
        store_rendered_markdown_state(&update.before, sink, &successor)?;
        Ok(())
    }

    fn restore(input: &mut sdk::RestoreFile<'_>, sink: &mut sdk::Output<'_>) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(row) = input.rows.next()? {
            records.push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                snapshot: row.snapshot,
            });
        }
        let accepted = input
            .accepted
            .as_ref()
            .map(sdk::Snapshot::read_all)
            .transpose()?;
        let (document, _) = Document::open_rows(records, accepted).map_err(core_error)?;
        store_markdown_state(sink, &document)?;
        if input.accepted.is_none() {
            sink.replace_file(&document.bytes())?;
        }
        Ok(())
    }

    fn resolve_conflict(conflict: sdk::RowConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
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
            Document::resolve_row_conflict(Some(base.clone()), Some(a.clone()), Some(b.clone()));
        Ok(match resolved {
            None => sdk::ConflictResolution::Delete,
            Some(resolved) if resolved == b => sdk::ConflictResolution::TakeB,
            Some(resolved) if resolved == a => sdk::ConflictResolution::TakeA,
            Some(resolved) if resolved == base => sdk::ConflictResolution::TakeBase,
            Some(resolved) => sdk::ConflictResolution::Replace(resolved),
        })
    }

    fn open(input: &sdk::OpenFile<'_>, sink: &mut sdk::Output<'_>) -> sdk::Result<()> {
        let bytes = input.accepted.read_all()?;
        let namespace = IdNamespace::from_namespace_bytes(input.creates.namespace_bytes());
        let (document, mut changes) =
            Document::open_file(bytes, Some(input.path.as_str()), namespace).map_err(core_error)?;
        strip_duplicated_lexical_fallback(&mut changes)?;
        store_markdown_state(sink, &document)?;
        emit_changes(changes, input.creates, None, sink)?;
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Output<'_>) -> sdk::Result<()> {
        let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
        let inserts = update
            .edits
            .iter()
            .map(|edit| edit.insert.clone())
            .collect::<Vec<_>>();
        let splices = update
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        if update.before_path == update.after_path
            && let [edit] = update.edits.as_slice()
            && let Some((changes, root, block_key, block, shifts)) =
                sparse_block_change(update, edit, &inserts[0], namespace)?
        {
            sink.put_state(ROOT_STATE, &root)?;
            sink.put_state(&block_key, &block)?;
            sink.put_state(BLOCK_SHIFTS_STATE, &shifts)?;
            emit_changes(changes, update.creates, Some(0), sink)?;
            return Ok(());
        }

        let document = load_markdown_document(&update.before, update.before.read_all()?)?;
        let (document, mut changes) = document
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        strip_duplicated_lexical_fallback(&mut changes)?;
        let (root, blocks) = document.arena_state().map_err(core_error)?;
        sink.put_state(ROOT_STATE, &root)?;
        let (old_index_pages, old_block_pages) = block_page_counts(&update.before)?;
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
        if let Some(shifts) = update.before.get_state(BLOCK_SHIFTS_STATE)? {
            for (ordinal, _) in decode_block_shifts(&shifts)? {
                sink.delete_state(&block_overlay_key(ordinal))?;
            }
        }
        sink.delete_state(BLOCK_SHIFTS_STATE)?;
        emit_changes(changes, update.creates, Some(0), sink)?;
        Ok(())
    }
}

fn store_rendered_markdown_state(
    before: &sdk::Snapshot<'_>,
    sink: &mut sdk::Output<'_>,
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

fn store_markdown_state(successor: &sdk::Output, document: &Document) -> sdk::Result<()> {
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
    Ok(())
}

type SparseBlockResult = (Vec<RowChange>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);

fn sparse_block_change(
    update: &sdk::FileUpdate<'_>,
    edit: &sdk::FileEdit,
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
        let entry = read_block_entry(&update.before, middle)?;
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
    let entry = read_block_entry(&update.before, ordinal)?;
    let start = effective_block_position(entry.start, ordinal, &shifts)?;
    let end = effective_block_position(entry.end, ordinal + 1, &shifts)?;
    if edit.offset < start || edit_end > end {
        return Ok(None);
    }
    let block_key = block_overlay_key(ordinal);
    let block = match update.before.get_state(&block_key)? {
        Some(block) => block,
        None => read_block_blob(&update.before, entry)?,
    };
    if block.len() > BLOCK_PAGE_BYTES {
        return Ok(None);
    }
    let root = update
        .before
        .get_state(ROOT_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown arena root is missing"))?;
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
        FileEdit {
            offset: edit.offset - start,
            delete_len: edit.delete_len,
            insert,
        },
        namespace,
        0,
    )
    .map_err(core_error)?
    else {
        return Ok(None);
    };
    if block.len() > BLOCK_PAGE_BYTES {
        return Ok(None);
    }
    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("Markdown insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block shrink exceeds i64"))?
    };
    if !add_block_shift(&mut shifts, ordinal, delta)? {
        return Ok(None);
    }
    Ok(Some((
        changes,
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
        if block_pages.last().expect("one page").len() == BLOCK_PAGE_BYTES {
            block_pages.push(Vec::with_capacity(BLOCK_PAGE_BYTES));
        }
        let page = u32::try_from(block_pages.len() - 1)
            .map_err(|_| sdk::Error::limit_exceeded("too many Markdown block pages"))?;
        let blob_offset = u32::try_from(block_pages.last().expect("one page").len())
            .map_err(|_| sdk::Error::limit_exceeded("Markdown block page exceeds 4GiB"))?;
        locations.push((page, blob_offset));
        let mut remaining = block.tree_json.as_slice();
        while !remaining.is_empty() {
            let available = BLOCK_PAGE_BYTES - block_pages.last().expect("one page").len();
            if available == 0 {
                block_pages.push(Vec::with_capacity(BLOCK_PAGE_BYTES));
                continue;
            }
            let take = available.min(remaining.len());
            block_pages
                .last_mut()
                .expect("one page")
                .extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
        }
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

fn read_block_entry(root: &sdk::Snapshot<'_>, ordinal: u32) -> sdk::Result<BlockEntry> {
    let entries_per_page = (BLOCK_PAGE_BYTES / BLOCK_INDEX_ENTRY_BYTES as usize) as u32;
    let page = ordinal / entries_per_page;
    let offset = u64::from(ordinal % entries_per_page) * u64::from(BLOCK_INDEX_ENTRY_BYTES);
    let bytes = root
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

fn read_block_blob(root: &sdk::Snapshot<'_>, entry: BlockEntry) -> sdk::Result<Vec<u8>> {
    let mut page = entry.page;
    let mut offset = usize::try_from(entry.blob_offset)
        .map_err(|_| sdk::Error::invalid_input("Markdown block offset exceeds usize"))?;
    if offset >= BLOCK_PAGE_BYTES {
        return Err(sdk::Error::invalid_input(
            "Markdown block offset exceeds its state page",
        ));
    }
    let mut remaining = usize::try_from(entry.blob_len)
        .map_err(|_| sdk::Error::limit_exceeded("Markdown block length exceeds usize"))?;
    let mut output = Vec::with_capacity(remaining);
    while remaining > 0 {
        let take = remaining.min(BLOCK_PAGE_BYTES - offset);
        let bytes = root
            .read_state_range(
                &block_page_key(page),
                u64::try_from(offset)
                    .map_err(|_| sdk::Error::limit_exceeded("Markdown state offset exceeds u64"))?,
                u32::try_from(take)
                    .map_err(|_| sdk::Error::limit_exceeded("Markdown state read exceeds u32"))?,
            )?
            .ok_or_else(|| sdk::Error::invalid_input("Markdown block page disappeared"))?;
        if bytes.len() != take {
            return Err(sdk::Error::invalid_input(
                "Markdown block page is truncated",
            ));
        }
        output.extend_from_slice(&bytes);
        remaining -= take;
        if remaining > 0 {
            page = page
                .checked_add(1)
                .ok_or_else(|| sdk::Error::limit_exceeded("Markdown block page overflowed"))?;
        }
        offset = 0;
    }
    Ok(output)
}

fn load_markdown_document(root: &sdk::Snapshot<'_>, bytes: Vec<u8>) -> sdk::Result<Document> {
    let root_json = root
        .get_state(ROOT_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown arena root is missing"))?;
    let manifest = root
        .get_state(BLOCKS_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Markdown block manifest is missing"))?;
    if manifest.len() != BLOCK_INDEX_HEADER_BYTES as usize
        || manifest.get(..4) != Some(BLOCK_INDEX_MAGIC)
    {
        return Err(sdk::Error::invalid_input(
            "unsupported Markdown block index",
        ));
    }
    let count = u32::from_le_bytes(manifest[4..8].try_into().expect("fixed Markdown header"));
    let shifts = decode_block_shifts(
        root.get_state(BLOCK_SHIFTS_STATE)?
            .as_deref()
            .unwrap_or_default(),
    )?;
    let mut blocks = Vec::with_capacity(count as usize);
    for ordinal in 0..count {
        let entry = read_block_entry(root, ordinal)?;
        let start = effective_block_position(entry.start, ordinal, &shifts)?;
        let end = effective_block_position(entry.end, ordinal + 1, &shifts)?;
        let tree_json = match root.get_state(&block_overlay_key(ordinal))? {
            Some(block) => block,
            None => read_block_blob(root, entry)?,
        };
        blocks.push(ArenaMarkdownBlock {
            start,
            end,
            tree_json,
        });
    }
    Document::open_arena(bytes, &root_json, blocks).map_err(core_error)
}

fn block_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"markdown/block-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn block_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"markdown/block-index-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn block_page_counts(root: &sdk::Snapshot<'_>) -> sdk::Result<(u32, u32)> {
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
    let mut key = b"markdown/block-overlay/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn decode_block_shifts(bytes: &[u8]) -> sdk::Result<Vec<(u32, i64)>> {
    if bytes.len() % 12 != 0 {
        return Err(sdk::Error::invalid_input(
            "truncated Markdown block shift overlay",
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
            .ok_or_else(|| sdk::Error::invalid_input("Markdown block shift overflowed"))?;
        if total == 0 {
            compact.remove(&ordinal);
        } else {
            compact.insert(ordinal, total);
        }
    }
    Ok(compact.into_iter().collect())
}

fn add_block_shift(shifts: &mut Vec<(u32, i64)>, ordinal: u32, delta: i64) -> sdk::Result<bool> {
    if delta == 0 {
        return Ok(true);
    }
    match shifts.binary_search_by_key(&ordinal, |(ordinal, _)| *ordinal) {
        Ok(index) => {
            let total = shifts[index]
                .1
                .checked_add(delta)
                .ok_or_else(|| sdk::Error::invalid_input("Markdown block shift overflowed"))?;
            if total == 0 {
                shifts.remove(index);
            } else {
                shifts[index].1 = total;
            }
            Ok(true)
        }
        Err(index) if shifts.len() < MAX_BLOCK_SHIFT_RECORDS => {
            shifts.insert(index, (ordinal, delta));
            Ok(true)
        }
        Err(_) => Ok(false),
    }
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

fn strip_duplicated_lexical_fallback(changes: &mut [RowChange]) -> sdk::Result<()> {
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
        format
            .as_object_mut()
            .expect("validated Markdown root format is an object")
            .insert(LEXICAL_SOURCE_REQUIRED_FIELD.to_owned(), Value::Bool(true));
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

fn emit_changes(
    changes: impl IntoIterator<Item = RowChange>,
    creates: sdk::CreateContext,
    create_from_ordinal: Option<u32>,
    sink: &mut sdk::Output<'_>,
) -> sdk::Result<()> {
    for change in changes {
        match change.snapshot {
            Some(snapshot) => {
                let local_ref = change
                    .row_pk
                    .first()
                    .filter(|_| change.row_pk.len() == 1)
                    .and_then(|id| local_ref(creates, id))
                    .filter(|ordinal| {
                        create_from_ordinal.is_none_or(|minimum| *ordinal >= minimum)
                    });
                if let Some(local_ref) = local_ref {
                    sink.row(sdk::RowMutation::Create {
                        schema_key: &change.schema_key,
                        local_ref,
                        snapshot: &snapshot,
                    })?;
                } else {
                    sink.row(sdk::RowMutation::Upsert {
                        schema_key: &change.schema_key,
                        row_pk: &change.row_pk,
                        snapshot: &snapshot,
                        effect: match change.effect {
                            ChangeEffect::Content => sdk::ChangeEffect::Content,
                            ChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
                        },
                    })?;
                }
            }
            None => sink.row(sdk::RowMutation::Delete {
                schema_key: &change.schema_key,
                row_pk: &change.row_pk,
            })?,
        }
    }
    Ok(())
}

fn local_ref(creates: sdk::CreateContext, id: &str) -> Option<u32> {
    let id = uuid::Uuid::parse_str(id).ok()?;
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
}

#[cfg(target_family = "wasm")]
lix::plugin::export!(MarkdownPlugin);

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

    #[test]
    fn one_large_block_spans_bounded_state_pages() {
        let tree_json = vec![b'x'; BLOCK_PAGE_BYTES + 257];
        let blocks = [ArenaMarkdownBlock {
            start: 0,
            end: tree_json.len() as u64,
            tree_json: tree_json.clone(),
        }];

        let encoded = encode_blocks(&blocks).expect("encode spanning block");

        assert_eq!(encoded.block_pages.len(), 2);
        assert!(
            encoded
                .block_pages
                .iter()
                .all(|page| page.len() <= BLOCK_PAGE_BYTES)
        );
        assert_eq!(
            encoded.block_pages.concat(),
            tree_json,
            "the paged block stream must remain byte-exact"
        );
        let entry = &encoded.index_pages[0][..BLOCK_INDEX_ENTRY_BYTES as usize];
        assert_eq!(
            u32::from_le_bytes(entry[16..20].try_into().expect("page")),
            0
        );
        assert_eq!(
            u32::from_le_bytes(entry[20..24].try_into().expect("offset")),
            0
        );
        assert_eq!(
            u32::from_le_bytes(entry[24..28].try_into().expect("length")) as usize,
            tree_json.len()
        );
    }

    #[test]
    fn sparse_block_shifts_are_coalesced_and_bounded() {
        let mut shifts = Vec::new();
        for _ in 0..100_000 {
            assert!(add_block_shift(&mut shifts, 7, 1).expect("coalesce shift"));
        }
        assert_eq!(shifts, [(7, 100_000)]);
        assert!(add_block_shift(&mut shifts, 7, -100_000).expect("cancel shift"));
        assert!(shifts.is_empty());

        for ordinal in 0..MAX_BLOCK_SHIFT_RECORDS as u32 {
            assert!(add_block_shift(&mut shifts, ordinal, 1).expect("insert bounded shift"));
        }
        assert!(!add_block_shift(&mut shifts, u32::MAX, 1).expect("request index rebuild"));
        assert_eq!(
            encode_block_shifts(&shifts).len(),
            MAX_BLOCK_SHIFT_RECORDS * 12
        );
    }

    #[test]
    fn persisted_arena_ranges_exclude_parser_only_eof_sentinels() {
        let bytes = b"# Heading\n\nFirst paragraph.\n\nSecond paragraph.\n".to_vec();
        let namespace = IdNamespace::from_halves(7, 11);
        let (document, _) =
            Document::open_file(bytes.clone(), Some("doc.md"), namespace).expect("parse Markdown");
        let (_, blocks) = document.arena_state().expect("arena state");
        assert!(
            blocks
                .iter()
                .all(|block| block.start <= block.end && block.end <= bytes.len() as u64),
            "persisted arena ranges must exclude parser-only EOF sentinels",
        );
    }
}
