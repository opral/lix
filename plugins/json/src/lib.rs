//! JSON support for the row-first Component API v1.
#![allow(dead_code)]

mod core;

use core::{
    ArenaJsonRelation, ArenaJsonScalar, ChangeEffect, Document, FileEdit, IdNamespace, RowChange,
    RowImportBuilder, RowRecord,
};
use lix::plugin as sdk;

struct JsonPlugin;

const SCALAR_INDEX_STATE: &[u8] = b"json/scalar-index";
const SCALAR_SHIFTS_STATE: &[u8] = b"json/scalar-shifts";
const ID_NAMESPACE_STATE: &[u8] = b"json/id-namespace";
const FALLBACK_ROWS_STATE: &[u8] = b"json/fallback-rows";
const SCALAR_INDEX_MAGIC: &[u8; 4] = b"JSS2";
const FALLBACK_ROWS_MAGIC: &[u8; 4] = b"JFE2";
const SCALAR_INDEX_HEADER_BYTES: u32 = 16;
const SCALAR_INDEX_ENTRY_BYTES: u32 = 20;
const SCALAR_PAGE_BYTES: usize = 1024 * 1024;
const STATE_PAGE_BYTES: usize = 1024 * 1024;

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
    let create_namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
    let (mut document, _) = builder.finish().map_err(sdk::Error::invalid_input)?;
    if !document.bytes_equal(&accepted) {
        let reconcile = [FileEdit {
            offset: 0,
            delete_len: document.byte_len() as u64,
            insert: &accepted,
        }];
        document = document
            .file_changed(&reconcile, create_namespace)
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
    let (document, changes) = document
        .file_changed(&splices, create_namespace)
        .map_err(sdk::Error::invalid_input)?;
    sink.put_state(ID_NAMESPACE_STATE, &update.creates.namespace_bytes())?;
    store_fallback_rows_fresh(
        sink,
        &document.row_records().map_err(sdk::Error::invalid_input)?,
    )?;
    emit_changes(changes.into_iter().map(Ok), update.creates, sink)?;
    Ok(())
}

impl sdk::FileProjection for JsonPlugin {
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
        let namespace = read_namespace(&update.before, ID_NAMESPACE_STATE)?
            .or_else(|| namespace_from_changes(&changes))
            .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        let document =
            read_fallback_document(&update.before, before.clone(), Some(update.path), namespace)?;
        let (successor, edits) = document
            .rows_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            sink.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        store_fallback_rows(&update.before, sink, &successor)?;
        Ok(())
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        sink: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut builder = RowImportBuilder::new();
        let mut records = Vec::new();
        while let Some(row) = input.rows.next()? {
            let record = RowRecord {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                snapshot: row.snapshot,
            };
            builder
                .push(record.clone())
                .map_err(sdk::Error::invalid_input)?;
            records.push(record);
        }
        let (document, _) = builder.finish().map_err(sdk::Error::invalid_input)?;
        store_fallback_rows_fresh(sink, &records)?;
        sink.write(&document.bytes())
    }

    fn parse(input: sdk::ParseInput<'_>, sink: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        let bytes = input.file.read_all()?;
        let namespace = IdNamespace::from_namespace_bytes(input.creates.namespace_bytes());
        let (document, changes) = Document::open_fresh_file(bytes, Some(input.path), namespace)
            .map_err(sdk::Error::invalid_input)?;
        sink.put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        store_scalar_state(sink, &document)?;
        emit_changes(changes, input.creates, sink)?;
        Ok(())
    }

    fn parse_changes(
        mut update: sdk::ParseChangesInput<'_>,
        sink: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        if update.before.state_len(SCALAR_INDEX_STATE)?.is_none()
            && update.before.state_len(FALLBACK_ROWS_STATE)?.is_none()
        {
            return cold_parse_changes(&mut update, sink);
        }
        let create_namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
        let accepted_namespace =
            read_namespace(&update.before, ID_NAMESPACE_STATE)?.unwrap_or(create_namespace);
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
        if update.before.state_len(FALLBACK_ROWS_STATE)?.is_none()
            && update.before_path == update.after_path
            && update.file_edits.iter().len() == 1
            && let Some(edit) = update.file_edits.iter().next()
            && let Some((change, shifts)) = sparse_scalar_change(&update, edit, &inserts[0])?
        {
            if shifts.is_empty() {
                if update.before.state_len(SCALAR_SHIFTS_STATE)?.is_some() {
                    sink.delete_state(SCALAR_SHIFTS_STATE)?;
                }
            } else {
                sink.put_state(SCALAR_SHIFTS_STATE, &shifts)?;
            }
            emit_changes([Ok(change)], update.creates, sink)?;
            return Ok(());
        }

        let before_bytes = update.before.read_all()?;
        let document = read_fallback_document(
            &update.before,
            before_bytes,
            Some(update.before_path),
            accepted_namespace,
        )?;
        let (document, changes) = document
            .file_changed(&splices, create_namespace)
            .map_err(sdk::Error::invalid_input)?;
        store_fallback_rows_in_transaction(&update.before, sink, &document)?;
        let (old_index_page_count, old_scalar_page_count) = scalar_page_counts(&update)?;
        sink.delete_state(SCALAR_INDEX_STATE)?;
        for ordinal in 0..old_index_page_count {
            sink.delete_state(&scalar_index_page_key(ordinal))?;
        }
        for ordinal in 0..old_scalar_page_count {
            sink.delete_state(&scalar_page_key(ordinal))?;
        }
        sink.delete_state(SCALAR_SHIFTS_STATE)?;
        emit_changes(changes.into_iter().map(Ok), update.creates, sink)?;
        Ok(())
    }
}

fn read_fallback_document(
    root: &sdk::Snapshot<'_>,
    accepted: Vec<u8>,
    path: Option<&str>,
    namespace: IdNamespace,
) -> sdk::Result<Document> {
    let Some(manifest) = root.get_state(FALLBACK_ROWS_STATE)? else {
        return Document::open_file(accepted, path, namespace)
            .map(|(document, _)| document)
            .map_err(sdk::Error::invalid_input);
    };
    let (record_count, page_count) = decode_fallback_manifest(&manifest)?;
    let mut pages = Vec::with_capacity(page_count as usize);
    for ordinal in 0..page_count {
        pages.push(
            root.get_state(&fallback_row_page_key(ordinal))?
                .ok_or_else(|| sdk::Error::invalid_input("JSON fallback page disappeared"))?,
        );
    }
    let (document, _) = Document::open_rows(decode_row_records(record_count, pages)?)
        .map_err(sdk::Error::invalid_input)?;
    let rendered = document.bytes();
    if rendered == accepted {
        return Ok(document);
    }
    let reconcile = [FileEdit {
        offset: 0,
        delete_len: rendered.len() as u64,
        insert: &accepted,
    }];
    document
        .file_changed(&reconcile, namespace)
        .map(|(document, _)| document)
        .map_err(sdk::Error::invalid_input)
}

fn store_fallback_rows(
    before: &sdk::Snapshot<'_>,
    sink: &mut impl StateOutput,
    document: &Document,
) -> sdk::Result<()> {
    let records = document.row_records().map_err(sdk::Error::invalid_input)?;
    let old_page_count = fallback_row_page_count(before)?;
    let (manifest, pages) = encode_row_records(&records)?;
    sink.put_state(FALLBACK_ROWS_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        sink.put_state(&fallback_row_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        sink.delete_state(&fallback_row_page_key(ordinal))?;
    }
    let (index_page_count, scalar_page_count) = scalar_page_counts_root(before)?;
    sink.delete_state(SCALAR_INDEX_STATE)?;
    for ordinal in 0..index_page_count {
        sink.delete_state(&scalar_index_page_key(ordinal))?;
    }
    for ordinal in 0..scalar_page_count {
        sink.delete_state(&scalar_page_key(ordinal))?;
    }
    sink.delete_state(SCALAR_SHIFTS_STATE)?;
    Ok(())
}

fn store_fallback_rows_in_transaction(
    before: &sdk::Snapshot<'_>,
    successor: &mut impl StateOutput,
    document: &Document,
) -> sdk::Result<()> {
    let records = document.row_records().map_err(sdk::Error::invalid_input)?;
    let old_page_count = fallback_row_page_count(before)?;
    let (manifest, pages) = encode_row_records(&records)?;
    successor.put_state(FALLBACK_ROWS_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&fallback_row_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        successor.delete_state(&fallback_row_page_key(ordinal))?;
    }
    Ok(())
}

fn store_fallback_rows_fresh(
    successor: &mut impl StateOutput,
    records: &[RowRecord],
) -> sdk::Result<()> {
    let (manifest, pages) = encode_row_records(records)?;
    successor.put_state(FALLBACK_ROWS_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&fallback_row_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn encode_row_records(records: &[RowRecord]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let record_count = u32::try_from(records.len())
        .map_err(|_| sdk::Error::limit_exceeded("too many JSON fallback rows"))?;
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(STATE_PAGE_BYTES);
    for record in records {
        let mut encoded = Vec::new();
        push_text(&mut encoded, &record.schema_key)?;
        encoded.extend_from_slice(
            &u32::try_from(record.row_pk.len())
                .map_err(|_| sdk::Error::limit_exceeded("too many JSON key components"))?
                .to_le_bytes(),
        );
        for component in &record.row_pk {
            push_text(&mut encoded, component)?;
        }
        encoded.extend_from_slice(
            &u32::try_from(record.snapshot.len())
                .map_err(|_| sdk::Error::limit_exceeded("JSON snapshot is too large"))?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&record.snapshot);
        push_paged_state(&mut pages, &mut page, &encoded);
    }
    if !page.is_empty() {
        pages.push(page);
    }
    let mut manifest = Vec::with_capacity(12);
    manifest.extend_from_slice(FALLBACK_ROWS_MAGIC);
    manifest.extend_from_slice(&record_count.to_le_bytes());
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON fallback pages"))?
            .to_le_bytes(),
    );
    Ok((manifest, pages))
}

fn push_paged_state(pages: &mut Vec<Vec<u8>>, page: &mut Vec<u8>, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let available = STATE_PAGE_BYTES - page.len();
        let take = available.min(bytes.len());
        page.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        if page.len() == STATE_PAGE_BYTES {
            pages.push(std::mem::replace(
                page,
                Vec::with_capacity(STATE_PAGE_BYTES),
            ));
        }
    }
}

fn decode_fallback_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32)> {
    if bytes.len() != 12 || bytes.get(..4) != Some(FALLBACK_ROWS_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported JSON fallback row manifest",
        ));
    }
    Ok((
        u32::from_le_bytes(bytes[4..8].try_into().expect("record count")),
        u32::from_le_bytes(bytes[8..12].try_into().expect("page count")),
    ))
}

fn fallback_row_page_count(root: &sdk::Snapshot<'_>) -> sdk::Result<u32> {
    root.get_state(FALLBACK_ROWS_STATE)?
        .map(|manifest| decode_fallback_manifest(&manifest).map(|(_, pages)| pages))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn decode_row_records(record_count: u32, pages: Vec<Vec<u8>>) -> sdk::Result<Vec<RowRecord>> {
    let mut input = PagedStateReader::new(pages);
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let schema_key = input.text()?;
        let component_count = input.u32()? as usize;
        let mut row_pk = Vec::with_capacity(component_count.min(4));
        for _ in 0..component_count {
            row_pk.push(input.text()?);
        }
        let snapshot_len = input.u32()? as usize;
        let snapshot = input.bytes(snapshot_len)?;
        records.push(RowRecord {
            schema_key,
            row_pk,
            snapshot,
        });
    }
    if !input.finished() {
        return Err(sdk::Error::invalid_input(
            "JSON fallback state contains trailing bytes",
        ));
    }
    Ok(records)
}

struct PagedStateReader {
    pages: Vec<Vec<u8>>,
    page: usize,
    offset: usize,
}

impl PagedStateReader {
    fn new(pages: Vec<Vec<u8>>) -> Self {
        Self {
            pages,
            page: 0,
            offset: 0,
        }
    }

    fn bytes(&mut self, mut length: usize) -> sdk::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(length);
        while length > 0 {
            let page = self
                .pages
                .get(self.page)
                .ok_or_else(|| sdk::Error::invalid_input("JSON fallback state is truncated"))?;
            let available = page.len().saturating_sub(self.offset);
            if available == 0 {
                self.page += 1;
                self.offset = 0;
                continue;
            }
            let take = available.min(length);
            output.extend_from_slice(&page[self.offset..self.offset + take]);
            self.offset += take;
            length -= take;
        }
        Ok(output)
    }

    fn u32(&mut self) -> sdk::Result<u32> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?
                .try_into()
                .expect("paged reader returned four bytes"),
        ))
    }

    fn text(&mut self) -> sdk::Result<String> {
        let length = self.u32()? as usize;
        String::from_utf8(self.bytes(length)?)
            .map_err(|_| sdk::Error::invalid_input("JSON fallback text is not UTF-8"))
    }

    fn finished(&self) -> bool {
        match self.pages.get(self.page) {
            None => true,
            Some(page) => {
                self.offset == page.len()
                    && self
                        .pages
                        .iter()
                        .skip(self.page.saturating_add(1))
                        .all(Vec::is_empty)
            }
        }
    }
}

fn fallback_row_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"json/fallback-row-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn read_namespace(root: &sdk::Snapshot<'_>, key: &[u8]) -> sdk::Result<Option<IdNamespace>> {
    let Some(bytes) = root.get_state(key)? else {
        return Ok(None);
    };
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("JSON ID namespace has invalid length"))?;
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
        let start = usize::try_from(edit.offset)
            .map_err(|_| sdk::Error::invalid_input("JSON edit offset exceeds guest memory"))?;
        let end = start
            .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                sdk::Error::invalid_input("JSON edit deletion exceeds guest memory")
            })?)
            .ok_or_else(|| sdk::Error::invalid_input("JSON edit range overflowed"))?;
        if end > bytes.len() {
            return Err(sdk::Error::invalid_input(
                "JSON edit exceeds accepted bytes",
            ));
        }
        bytes.splice(start..end, edit.insert.iter().copied());
    }
    Ok(bytes)
}

fn apply_file_splices(mut bytes: Vec<u8>, splices: &[FileEdit<'_>]) -> sdk::Result<Vec<u8>> {
    for splice in splices.iter().rev() {
        let start = usize::try_from(splice.offset)
            .map_err(|_| sdk::Error::invalid_input("JSON splice offset exceeds guest memory"))?;
        let end = start
            .checked_add(usize::try_from(splice.delete_len).map_err(|_| {
                sdk::Error::invalid_input("JSON splice deletion exceeds guest memory")
            })?)
            .ok_or_else(|| sdk::Error::invalid_input("JSON splice range overflowed"))?;
        if end > bytes.len() {
            return Err(sdk::Error::invalid_input(
                "JSON splice exceeds accepted bytes",
            ));
        }
        bytes.splice(start..end, splice.insert.iter().copied());
    }
    Ok(bytes)
}

fn store_scalar_state(successor: &mut impl StateOutput, document: &Document) -> sdk::Result<()> {
    let state = encode_scalar_state(
        &document
            .arena_scalars()
            .map_err(sdk::Error::invalid_input)?,
    )?;
    successor.put_state(SCALAR_INDEX_STATE, &state.manifest)?;
    for (ordinal, page) in state.index_pages.iter().enumerate() {
        successor.put_state(&scalar_index_page_key(ordinal as u32), page)?;
    }
    for (ordinal, page) in state.scalar_pages.iter().enumerate() {
        successor.put_state(&scalar_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn sparse_scalar_change(
    update: &sdk::ParseChangesInput<'_>,
    edit: &sdk::FileEdit,
    insert: &[u8],
) -> sdk::Result<Option<(RowChange, Vec<u8>)>> {
    let manifest_len = match update.before.state_len(SCALAR_INDEX_STATE)? {
        Some(length) => length,
        None => return Ok(None),
    };
    if manifest_len != u64::from(SCALAR_INDEX_HEADER_BYTES) {
        return Err(sdk::Error::invalid_input(
            "invalid JSON scalar index manifest length",
        ));
    }
    let header = update
        .before
        .read_state_range(SCALAR_INDEX_STATE, 0, SCALAR_INDEX_HEADER_BYTES)?
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index disappeared"))?;
    if header.get(..4) != Some(SCALAR_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported JSON scalar index"));
    }
    let count = u32::from_le_bytes(header[4..8].try_into().expect("fixed JSON index header"));
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
    let metadata = decode_scalar_metadata(&metadata)?;
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
    let Some(change) =
        Document::scalar_change_from_arena(metadata, &scalar).map_err(sdk::Error::invalid_input)?
    else {
        return Ok(None);
    };
    let insert_len = u64::try_from(insert.len())
        .map_err(|_| sdk::Error::limit_exceeded("JSON insert exceeds u64"))?;
    let delta = if insert_len >= edit.delete_len {
        i64::try_from(insert_len - edit.delete_len)
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar growth exceeds i64"))?
    } else {
        -i64::try_from(edit.delete_len - insert_len)
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar shrink exceeds i64"))?
    };
    if delta != 0 {
        match shifts.binary_search_by_key(&ordinal, |(changed, _)| *changed) {
            Ok(index) => {
                shifts[index].1 = shifts[index]
                    .1
                    .checked_add(delta)
                    .ok_or_else(|| sdk::Error::invalid_input("JSON scalar shift overflowed"))?;
                if shifts[index].1 == 0 {
                    shifts.remove(index);
                }
            }
            Err(index) => shifts.insert(index, (ordinal, delta)),
        }
    }
    Ok(Some((change, encode_scalar_shifts(&shifts))))
}

struct EncodedScalarState {
    manifest: Vec<u8>,
    index_pages: Vec<Vec<u8>>,
    scalar_pages: Vec<Vec<u8>>,
}

fn encode_scalar_state(scalars: &[ArenaJsonScalar]) -> sdk::Result<EncodedScalarState> {
    let mut scalar_pages = vec![Vec::with_capacity(SCALAR_PAGE_BYTES)];
    let mut locations = Vec::with_capacity(scalars.len());
    for scalar in scalars {
        let metadata = encode_scalar_metadata(scalar)?;
        if metadata.len() > SCALAR_PAGE_BYTES {
            return Err(sdk::Error::limit_exceeded(
                "one JSON scalar state exceeds the page limit",
            ));
        }
        if !scalar_pages.last().expect("one page").is_empty()
            && scalar_pages.last().expect("one page").len() + metadata.len() > SCALAR_PAGE_BYTES
        {
            scalar_pages.push(Vec::with_capacity(SCALAR_PAGE_BYTES));
        }
        let page = u32::try_from(scalar_pages.len() - 1)
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalar pages"))?;
        let offset = u32::try_from(scalar_pages.last().expect("one page").len())
            .map_err(|_| sdk::Error::limit_exceeded("JSON scalar page exceeds 4GiB"))?;
        scalar_pages
            .last_mut()
            .expect("one page")
            .extend_from_slice(&metadata);
        locations.push((page, offset, metadata.len() as u32));
    }
    if scalars.is_empty() {
        scalar_pages.clear();
    }
    let entries_per_page = STATE_PAGE_BYTES / SCALAR_INDEX_ENTRY_BYTES as usize;
    let mut index_pages = Vec::with_capacity(scalars.len().div_ceil(entries_per_page));
    for chunk_start in (0..scalars.len()).step_by(entries_per_page) {
        let chunk_end = (chunk_start + entries_per_page).min(scalars.len());
        let mut page =
            Vec::with_capacity((chunk_end - chunk_start) * SCALAR_INDEX_ENTRY_BYTES as usize);
        for (scalar, (metadata_page, offset, length)) in scalars[chunk_start..chunk_end]
            .iter()
            .zip(&locations[chunk_start..chunk_end])
        {
            page.extend_from_slice(&scalar.start.to_le_bytes());
            page.extend_from_slice(&scalar.length.to_le_bytes());
            page.extend_from_slice(&metadata_page.to_le_bytes());
            page.extend_from_slice(&offset.to_le_bytes());
            page.extend_from_slice(&length.to_le_bytes());
        }
        index_pages.push(page);
    }
    let mut manifest = Vec::with_capacity(SCALAR_INDEX_HEADER_BYTES as usize);
    manifest.extend_from_slice(SCALAR_INDEX_MAGIC);
    manifest.extend_from_slice(
        &u32::try_from(scalars.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalars"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(scalar_pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalar pages"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(index_pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many JSON scalar index pages"))?
            .to_le_bytes(),
    );
    Ok(EncodedScalarState {
        manifest,
        index_pages,
        scalar_pages,
    })
}

fn encode_scalar_metadata(scalar: &ArenaJsonScalar) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    match (scalar.relation, scalar.row_pk.as_slice()) {
        (ArenaJsonRelation::Snapshot, [id]) if id == "root" => output.push(0),
        (ArenaJsonRelation::Object, [parent_id, key]) => {
            output.push(1);
            push_state_bytes(&mut output, parent_id.as_bytes())?;
            push_state_bytes(&mut output, key.as_bytes())?;
            push_state_bytes(
                &mut output,
                scalar
                    .order_key
                    .as_deref()
                    .ok_or_else(|| {
                        sdk::Error::invalid_input("JSON object scalar has no order key")
                    })?
                    .as_bytes(),
            )?;
        }
        (ArenaJsonRelation::Array, [id]) => {
            output.push(2);
            push_state_bytes(&mut output, id.as_bytes())?;
            push_state_bytes(
                &mut output,
                scalar
                    .parent_id
                    .as_deref()
                    .ok_or_else(|| sdk::Error::invalid_input("JSON array scalar has no parent ID"))?
                    .as_bytes(),
            )?;
            push_state_bytes(
                &mut output,
                scalar
                    .order_key
                    .as_deref()
                    .ok_or_else(|| sdk::Error::invalid_input("JSON array scalar has no order key"))?
                    .as_bytes(),
            )?;
        }
        _ => {
            return Err(sdk::Error::invalid_input(
                "invalid JSON arena scalar identity",
            ));
        }
    }
    let flags = u8::from(scalar.prefix_json.is_some())
        | (u8::from(scalar.suffix_json.is_some()) << 1)
        | (u8::from(scalar.empty_json.is_some()) << 2);
    output.push(flags);
    for value in [
        scalar.prefix_json.as_deref(),
        scalar.suffix_json.as_deref(),
        scalar.empty_json.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        push_state_bytes(&mut output, value.as_bytes())?;
    }
    Ok(output)
}

fn decode_scalar_metadata(bytes: &[u8]) -> sdk::Result<ArenaJsonScalar> {
    let mut input = bytes;
    let relation = *input
        .first()
        .ok_or_else(|| sdk::Error::invalid_input("truncated JSON scalar state"))?;
    input = &input[1..];
    let (relation, row_pk, parent_id, order_key) = match relation {
        0 => (
            ArenaJsonRelation::Snapshot,
            vec!["root".to_owned()],
            None,
            None,
        ),
        1 => (
            ArenaJsonRelation::Object,
            vec![take_state_text(&mut input)?, take_state_text(&mut input)?],
            None,
            Some(take_state_text(&mut input)?),
        ),
        2 => (
            ArenaJsonRelation::Array,
            vec![take_state_text(&mut input)?],
            Some(take_state_text(&mut input)?),
            Some(take_state_text(&mut input)?),
        ),
        _ => {
            return Err(sdk::Error::invalid_input(
                "unsupported JSON scalar relation",
            ));
        }
    };
    let flags = *input
        .first()
        .ok_or_else(|| sdk::Error::invalid_input("truncated JSON scalar layout"))?;
    input = &input[1..];
    if flags & !0b111 != 0 {
        return Err(sdk::Error::invalid_input(
            "unsupported JSON scalar layout flags",
        ));
    }
    let prefix_json = (flags & 1 != 0)
        .then(|| take_state_text(&mut input))
        .transpose()?;
    let suffix_json = (flags & 2 != 0)
        .then(|| take_state_text(&mut input))
        .transpose()?;
    let empty_json = (flags & 4 != 0)
        .then(|| take_state_text(&mut input))
        .transpose()?;
    if !input.is_empty() {
        return Err(sdk::Error::invalid_input(
            "JSON scalar state has trailing bytes",
        ));
    }
    Ok(ArenaJsonScalar {
        start: 0,
        length: 0,
        relation,
        row_pk,
        parent_id,
        order_key,
        prefix_json,
        suffix_json,
        empty_json,
    })
}

#[derive(Clone, Copy)]
struct ScalarEntry {
    start: u32,
    length: u32,
    page: u32,
    blob_offset: u32,
    blob_len: u32,
}

fn read_scalar_entry(
    update: &sdk::ParseChangesInput<'_>,
    ordinal: u32,
) -> sdk::Result<ScalarEntry> {
    let entries_per_page = u32::try_from(STATE_PAGE_BYTES / SCALAR_INDEX_ENTRY_BYTES as usize)
        .expect("scalar index page capacity fits u32");
    let page = ordinal / entries_per_page;
    let page_ordinal = ordinal % entries_per_page;
    let offset = u64::from(page_ordinal)
        .checked_mul(u64::from(SCALAR_INDEX_ENTRY_BYTES))
        .ok_or_else(|| sdk::Error::invalid_input("JSON scalar index offset overflowed"))?;
    let bytes = update
        .before
        .read_state_range(
            &scalar_index_page_key(page),
            offset,
            SCALAR_INDEX_ENTRY_BYTES,
        )?
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
    let mut key = b"json/scalar-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn scalar_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"json/scalar-index-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn scalar_page_counts(update: &sdk::ParseChangesInput<'_>) -> sdk::Result<(u32, u32)> {
    scalar_page_counts_root(&update.before)
}

fn scalar_page_counts_root(root: &sdk::Snapshot<'_>) -> sdk::Result<(u32, u32)> {
    let Some(header) = root.read_state_range(SCALAR_INDEX_STATE, 0, SCALAR_INDEX_HEADER_BYTES)?
    else {
        return Ok((0, 0));
    };
    if header.get(..4) != Some(SCALAR_INDEX_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported JSON scalar index"));
    }
    Ok((
        u32::from_le_bytes(header[12..16].try_into().expect("index page count")),
        u32::from_le_bytes(header[8..12].try_into().expect("scalar page count")),
    ))
}

fn decode_scalar_shifts(bytes: &[u8]) -> sdk::Result<Vec<(u32, i64)>> {
    if bytes.len() % 12 != 0 {
        return Err(sdk::Error::invalid_input(
            "truncated JSON scalar shift overlay",
        ));
    }
    let mut shifts = std::collections::BTreeMap::<u32, i64>::new();
    for record in bytes.chunks_exact(12) {
        let ordinal = u32::from_le_bytes(record[0..4].try_into().expect("fixed shift record"));
        let delta = i64::from_le_bytes(record[4..12].try_into().expect("fixed shift record"));
        let total = shifts
            .get(&ordinal)
            .copied()
            .unwrap_or_default()
            .checked_add(delta)
            .ok_or_else(|| sdk::Error::invalid_input("JSON scalar shift overflowed"))?;
        if total == 0 {
            shifts.remove(&ordinal);
        } else {
            shifts.insert(ordinal, total);
        }
    }
    Ok(shifts.into_iter().collect())
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
    sink: &mut impl MutationOutput,
) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<RowChange, String>>,
{
    for change in changes {
        let change = change.map_err(sdk::Error::invalid_input)?;
        match change.snapshot {
            Some(snapshot) => {
                let local_ref = change
                    .row_pk
                    .first()
                    .filter(|_| change.row_pk.len() == 1)
                    .and_then(|id| local_ref(creates, id));
                if let Some(local_ref) = local_ref {
                    sink.create(&change.schema_key, local_ref, &snapshot)?;
                } else {
                    sink.upsert(
                        &change.schema_key,
                        &change.row_pk,
                        &snapshot,
                        match change.effect {
                            ChangeEffect::Content => sdk::ChangeEffect::Content,
                            ChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
                        },
                    )?;
                }
            }
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
    fn create(&mut self, schema_key: &str, local_ref: u32, snapshot: &[u8]) -> sdk::Result<()>;
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
    fn create(&mut self, s: &str, l: u32, v: &[u8]) -> sdk::Result<()> {
        self.create(s, l, v)
    }
    fn upsert(&mut self, s: &str, k: &[String], v: &[u8], _: sdk::ChangeEffect) -> sdk::Result<()> {
        self.upsert(s, k, v)
    }
    fn delete(&mut self, _: &str, _: &[String]) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "initial JSON parse produced a deletion",
        ))
    }
}
impl MutationOutput for sdk::RowChangeOutput<'_, '_> {
    fn create(&mut self, s: &str, l: u32, v: &[u8]) -> sdk::Result<()> {
        self.create(s, l, v)
    }
    fn upsert(&mut self, s: &str, k: &[String], v: &[u8], e: sdk::ChangeEffect) -> sdk::Result<()> {
        self.upsert(s, k, v, e)
    }
    fn delete(&mut self, s: &str, k: &[String]) -> sdk::Result<()> {
        self.delete(s, k)
    }
}

fn local_ref(creates: sdk::CreateContext, id: &str) -> Option<u32> {
    let id = uuid::Uuid::parse_str(id).ok()?;
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
}

fn push_text(output: &mut Vec<u8>, value: &str) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| sdk::Error::limit_exceeded("state text is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

#[cfg(target_family = "wasm")]
lix::plugin::export_capabilities! { file_projection: JsonPlugin }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_shift_overlay_compacts_repeated_and_zero_delta_edits() {
        let mut encoded = Vec::new();
        for (ordinal, delta) in [(7_u32, 3_i64), (2, 4), (7, -1), (2, -4), (7, 0)] {
            encoded.extend_from_slice(&ordinal.to_le_bytes());
            encoded.extend_from_slice(&delta.to_le_bytes());
        }

        let shifts = decode_scalar_shifts(&encoded).expect("valid shift overlay");
        assert_eq!(shifts, vec![(7, 2)]);
        assert_eq!(encode_scalar_shifts(&shifts).len(), 12);
    }

    #[test]
    fn scalar_index_is_split_into_bounded_state_pages() {
        let scalar = ArenaJsonScalar {
            start: 0,
            length: 4,
            relation: ArenaJsonRelation::Snapshot,
            row_pk: vec!["root".to_owned()],
            parent_id: None,
            order_key: None,
            prefix_json: None,
            suffix_json: None,
            empty_json: None,
        };
        let scalars = vec![scalar; 105_000];

        let encoded = encode_scalar_state(&scalars).expect("large scalar index");

        assert_eq!(encoded.manifest.len(), SCALAR_INDEX_HEADER_BYTES as usize);
        assert!(encoded.index_pages.len() > 1);
        assert!(
            encoded
                .index_pages
                .iter()
                .all(|page| page.len() <= STATE_PAGE_BYTES)
        );
    }

    #[test]
    fn fallback_rows_roundtrip_across_bounded_state_pages() {
        let records = (0..16_000)
            .map(|ordinal| RowRecord {
                schema_key: "json_object_member".to_owned(),
                row_pk: vec!["root".to_owned(), format!("key-{ordinal}")],
                snapshot: vec![b'x'; 128],
            })
            .collect::<Vec<_>>();

        let (manifest, pages) = encode_row_records(&records).expect("large fallback state");
        let (record_count, page_count) =
            decode_fallback_manifest(&manifest).expect("fallback manifest");
        assert!(pages.len() > 1);
        assert_eq!(page_count as usize, pages.len());
        assert!(pages.iter().all(|page| page.len() <= STATE_PAGE_BYTES));
        assert_eq!(
            decode_row_records(record_count, pages).expect("paged fallback roundtrip"),
            records
        );
    }

    #[test]
    fn scalar_sparse_path_falls_back_when_edit_adds_a_sibling() {
        let metadata = ArenaJsonScalar {
            start: 5,
            length: 1,
            relation: ArenaJsonRelation::Object,
            row_pk: vec!["root".to_owned(), "a".to_owned()],
            parent_id: None,
            order_key: Some("80".to_owned()),
            prefix_json: Some("\"a\":".to_owned()),
            suffix_json: None,
            empty_json: None,
        };

        assert_eq!(
            Document::scalar_change_from_arena(metadata, br#"1,"b":2"#)
                .expect("structural edit is an optimization miss"),
            None
        );
    }
}
