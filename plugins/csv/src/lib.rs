//! CSV support for the fused Component API v3.
#![allow(dead_code)]

mod core;

use core::{
    ArenaRowIndex, ChangeEffect, ColdInitialImport, Document, EntityChange, EntityRecord,
    IdNamespace, InputSplice, ROW_SCHEMA_KEY, RowConflictResolution, TABLE_SCHEMA_KEY,
    resolve_row_conflict,
};
use lix_plugin_api as sdk;
use serde_json::Value;

struct CsvPlugin;

const CERTIFIED_CSV_PAGE_BYTES: usize = 256 * 1024;
const CSV_INDEX_KEY: &[u8] = b"csv/index-v1";
const CSV_FALLBACK_ENTITIES_KEY: &[u8] = b"csv/fallback-entities-v1";
const CSV_FALLBACK_ENTITIES_MAGIC: &[u8; 4] = b"CFE2";
const ID_NAMESPACE_STATE: &[u8] = b"csv/id-namespace-v1";
const CSV_INDEX_HEADER_BYTES: u32 = 36;
const CSV_INDEX_PAGE_BYTES: usize = 1024 * 1024;
const CSV_STATE_PAGE_BYTES: usize = 1024 * 1024;

impl sdk::FormatPlugin for CsvPlugin {
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
        let document = if let Some(manifest) = update.before.get_state(CSV_FALLBACK_ENTITIES_KEY)? {
            let (record_count, page_count) = decode_fallback_manifest(&manifest)?;
            Document::open_entities(decode_entity_records(
                record_count,
                read_fallback_pages(&update.before, page_count)?,
            )?)
            .map_err(sdk::Error::invalid_input)?
            .0
        } else {
            let namespace = read_namespace(&update.before)?
                .or_else(|| namespace_from_changes(&changes))
                .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
            Document::open_file(
                before.clone(),
                update.before_file.path.as_deref(),
                namespace,
            )
            .map_err(sdk::Error::invalid_input)?
            .0
        };
        let (successor, edits) = document
            .entities_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        sink.replace_file(&apply_edits(before, &edits)?)?;
        let records = successor
            .entity_records()
            .map_err(sdk::Error::invalid_input)?;
        store_fallback_entities_from_sink(&update.before, sink, &records)?;
        delete_csv_index_from_sink(&update.before, sink)?;
        Ok(())
    }

    fn hydrate(input: &mut sdk::HydrateFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(entity) = input.entities.next()? {
            let snapshot = entity
                .snapshot
                .ok_or_else(|| sdk::Error::invalid_input("CSV hydration received a tombstone"))?;
            records.push(EntityRecord {
                schema_key: entity.schema_key,
                entity_pk: entity.entity_pk,
                snapshot,
            });
        }
        let (document, _) =
            Document::open_entities(records.clone()).map_err(sdk::Error::invalid_input)?;
        store_fallback_entities_fresh(&input.successor, &records)?;
        if input.accepted.is_none() {
            sink.replace_file(&document.bytes())?;
        }
        Ok(())
    }

    fn resolve_conflict(conflict: sdk::EntityConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
        if conflict.schema_key != ROW_SCHEMA_KEY {
            return Ok(conflict.take_b_or_delete());
        }
        let (Some(base), Some(a), Some(b)) = (&conflict.base, &conflict.a, &conflict.b) else {
            return Ok(conflict.take_b_or_delete());
        };
        if base.len() > 64 * 1024 || a.len() > 64 * 1024 || b.len() > 64 * 1024 {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let base = base.read()?;
        let a = a.read()?;
        let b = b.read()?;
        Ok(
            match resolve_row_conflict(Some(&base), Some(&a), Some(&b)) {
                RowConflictResolution::TakeA => sdk::ConflictResolution::TakeA,
                RowConflictResolution::TakeB => sdk::ConflictResolution::TakeB,
                RowConflictResolution::Replace(snapshot) => {
                    sdk::ConflictResolution::Replace(snapshot)
                }
                RowConflictResolution::Delete => sdk::ConflictResolution::Delete,
            },
        )
    }

    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let bytes = input.accepted.read_all()?;
        let mut import = ColdInitialImport::open(bytes, input.file.path.as_deref())
            .map_err(sdk::Error::invalid_input)?;
        let state = import.arena_state(input.creates.namespace_bytes());
        input
            .successor
            .put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        store_csv_index(&input.successor, &state)?;
        let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
        encoder.push(import.table_change(), input.creates, sink)?;
        encoder.flush(sink)?;
        let page_bytes = (sink.max_batch_bytes() as usize).min(CERTIFIED_CSV_PAGE_BYTES);
        while let Some((payload, row_count)) = import
            .next_typed_batch(page_bytes)
            .map_err(sdk::Error::invalid_input)?
        {
            sink.emit_csv_rows(row_count, payload)?;
        }
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        if update
            .before
            .state_len(CSV_FALLBACK_ENTITIES_KEY)?
            .is_some()
            || update.before_file.path != update.after_file.path
            || update.edits.len() != 1
            || u64::try_from(update.edits[0].insert.len()).expect("usize fits u64")
                != update.edits[0].delete_len
        {
            return fallback_file_changed(update, sink);
        }
        let [edit] = update.edits.as_slice() else {
            unreachable!("the sparse path requires exactly one edit")
        };
        let insert = &edit.insert;
        let (index, range, discovered_state) = if update.before.state_len(CSV_INDEX_KEY)?.is_some()
        {
            let header = update
                .before
                .read_state_range(CSV_INDEX_KEY, 0, CSV_INDEX_HEADER_BYTES)?
                .ok_or_else(|| sdk::Error::invalid_input("CSV arena root has no row index"))?;
            let row_count = u32::from_le_bytes(header[32..36].try_into().expect("CSV row count"));
            let logical_state_len = u64::from(CSV_INDEX_HEADER_BYTES)
                .checked_add(u64::from(row_count) * 4)
                .ok_or_else(|| sdk::Error::invalid_input("CSV row index size overflowed"))?;
            let index = ArenaRowIndex::decode_header(&header, logical_state_len)
                .map_err(sdk::Error::invalid_input)?;
            let range = index
                .row_range_for_edit_reader(edit.offset, edit.delete_len, |ordinal| {
                    let offsets_per_page = (CSV_INDEX_PAGE_BYTES / 4) as u32;
                    let page = ordinal / offsets_per_page;
                    let offset = u64::from(ordinal % offsets_per_page) * 4;
                    let bytes = update
                        .before
                        .read_state_range(&csv_index_page_key(page), offset, 4)
                        .map_err(|error| format!("CSV arena row-index read failed: {error:?}"))?
                        .ok_or_else(|| "CSV arena row index disappeared".to_owned())?;
                    let bytes: [u8; 4] = bytes
                        .try_into()
                        .map_err(|_| "CSV arena row-index read was truncated".to_owned())?;
                    Ok(u32::from_le_bytes(bytes))
                })
                .map_err(sdk::Error::invalid_input)?;
            (index, range, None)
        } else {
            let import = ColdInitialImport::open(
                update.before.read_all()?,
                update.before_file.path.as_deref(),
            )
            .map_err(sdk::Error::invalid_input)?;
            let state = import.arena_state(update.creates.namespace_bytes());
            let index = ArenaRowIndex::decode(&state).map_err(sdk::Error::invalid_input)?;
            let range = index
                .row_range_for_edit(edit.offset, edit.delete_len)
                .map_err(sdk::Error::invalid_input)?;
            (index, range, Some(state))
        };
        let (ordinal, row_start, row_end) = range;
        let mut row = update.before.read_range(row_start, row_end - row_start)?;
        let local_start = usize::try_from(edit.offset - row_start)
            .map_err(|_| sdk::Error::invalid_input("CSV edit offset exceeds guest memory"))?;
        let local_end =
            local_start
                .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                    sdk::Error::invalid_input("CSV edit deletion exceeds guest memory")
                })?)
                .ok_or_else(|| sdk::Error::invalid_input("CSV edit range overflowed"))?;
        if index.edit_touches_structure(&row[local_start..local_end], insert) {
            return fallback_file_changed(update, sink);
        }
        row.splice(local_start..local_end, insert.iter().copied());
        let change = index
            .row_change(ordinal, row)
            .map_err(sdk::Error::invalid_input)?;
        if let Some(state) = discovered_state {
            store_csv_index(&update.successor, &state)?;
        }
        let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
        encoder.push(change, update.creates, sink)?;
        encoder.flush(sink)?;
        Ok(())
    }
}

fn store_csv_index(successor: &sdk::Transaction<'_>, state: &[u8]) -> sdk::Result<()> {
    let (header, pages) = split_csv_index(state)?;
    successor.put_state(CSV_INDEX_KEY, header)?;
    for (ordinal, page) in pages.into_iter().enumerate() {
        successor.put_state(&csv_index_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn split_csv_index(state: &[u8]) -> sdk::Result<(&[u8], Vec<&[u8]>)> {
    if state.len() < CSV_INDEX_HEADER_BYTES as usize {
        return Err(sdk::Error::invalid_input("CSV row index is truncated"));
    }
    let header = &state[..CSV_INDEX_HEADER_BYTES as usize];
    let pages = state[CSV_INDEX_HEADER_BYTES as usize..]
        .chunks(CSV_INDEX_PAGE_BYTES)
        .collect();
    Ok((header, pages))
}

fn csv_index_page_count(root: &sdk::Root<'_>) -> sdk::Result<u32> {
    let Some(header) = root.read_state_range(CSV_INDEX_KEY, 0, CSV_INDEX_HEADER_BYTES)? else {
        return Ok(0);
    };
    if header.len() != CSV_INDEX_HEADER_BYTES as usize {
        return Err(sdk::Error::invalid_input(
            "CSV row index header is truncated",
        ));
    }
    let row_count = u32::from_le_bytes(header[32..36].try_into().expect("CSV row count"));
    let offsets_bytes = usize::try_from(row_count)
        .expect("u32 fits usize")
        .checked_mul(4)
        .ok_or_else(|| sdk::Error::invalid_input("CSV row index size overflowed"))?;
    Ok(u32::try_from(offsets_bytes.div_ceil(CSV_INDEX_PAGE_BYTES))
        .map_err(|_| sdk::Error::limit_exceeded("too many CSV row index pages"))?)
}

fn delete_csv_index(before: &sdk::Root<'_>, successor: &sdk::Transaction<'_>) -> sdk::Result<()> {
    let page_count = csv_index_page_count(before)?;
    successor.delete_state(CSV_INDEX_KEY)?;
    for ordinal in 0..page_count {
        successor.delete_state(&csv_index_page_key(ordinal))?;
    }
    Ok(())
}

fn delete_csv_index_from_sink(before: &sdk::Root<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
    let page_count = csv_index_page_count(before)?;
    sink.delete_state(CSV_INDEX_KEY)?;
    for ordinal in 0..page_count {
        sink.delete_state(&csv_index_page_key(ordinal))?;
    }
    Ok(())
}

fn csv_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"csv/index-page-v2/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn fallback_file_changed(
    update: &sdk::FileUpdate<'_>,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()> {
    let before_bytes = update.before.read_all()?;
    let document = if let Some(manifest) = update.before.get_state(CSV_FALLBACK_ENTITIES_KEY)? {
        let (record_count, page_count) = decode_fallback_manifest(&manifest)?;
        let pages = read_fallback_pages(&update.before, page_count)?;
        let (document, _) = Document::open_entities(decode_entity_records(record_count, pages)?)
            .map_err(sdk::Error::invalid_input)?;
        let rendered = document.bytes();
        if rendered == before_bytes {
            document
        } else {
            let reconcile = [InputSplice {
                offset: 0,
                delete_len: rendered.len() as u64,
                insert: &before_bytes,
            }];
            let namespace =
                IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
            document
                .file_changed_with_paths(
                    &reconcile,
                    update.before_file.path.as_deref(),
                    update.before_file.path.as_deref(),
                    namespace,
                )
                .map_err(sdk::Error::invalid_input)?
                .0
        }
    } else {
        let namespace =
            read_namespace(&update.before)?.unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        Document::open_file(before_bytes, update.before_file.path.as_deref(), namespace)
            .map_err(sdk::Error::invalid_input)?
            .0
    };
    let splices = update
        .edits
        .iter()
        .map(|edit| InputSplice {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert: &edit.insert,
        })
        .collect::<Vec<_>>();
    let namespace = IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
    let (successor, changes) = document
        .file_changed_with_paths(
            &splices,
            update.before_file.path.as_deref(),
            update.after_file.path.as_deref(),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
    let records = successor
        .entity_records()
        .map_err(sdk::Error::invalid_input)?;
    store_fallback_entities_in_transaction(&update.before, &update.successor, &records)?;
    delete_csv_index(&update.before, &update.successor)?;
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change, update.creates, sink)?;
    }
    encoder.flush(sink)
}

fn store_fallback_entities_from_sink(
    before: &sdk::Root<'_>,
    sink: &mut sdk::Sink<'_>,
    records: &[EntityRecord],
) -> sdk::Result<()> {
    let old_page_count = fallback_entity_page_count(before)?;
    let (manifest, pages) = encode_entity_records(records)?;
    sink.put_state(CSV_FALLBACK_ENTITIES_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        sink.put_state(&csv_fallback_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        sink.delete_state(&csv_fallback_page_key(ordinal))?;
    }
    Ok(())
}

fn store_fallback_entities_in_transaction(
    before: &sdk::Root<'_>,
    successor: &sdk::Transaction<'_>,
    records: &[EntityRecord],
) -> sdk::Result<()> {
    let old_page_count = fallback_entity_page_count(before)?;
    let (manifest, pages) = encode_entity_records(records)?;
    successor.put_state(CSV_FALLBACK_ENTITIES_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&csv_fallback_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        successor.delete_state(&csv_fallback_page_key(ordinal))?;
    }
    Ok(())
}

fn store_fallback_entities_fresh(
    successor: &sdk::Transaction<'_>,
    records: &[EntityRecord],
) -> sdk::Result<()> {
    let (manifest, pages) = encode_entity_records(records)?;
    successor.put_state(CSV_FALLBACK_ENTITIES_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&csv_fallback_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn encode_entity_records(records: &[EntityRecord]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let record_count = u32::try_from(records.len())
        .map_err(|_| sdk::Error::limit_exceeded("too many CSV fallback entities"))?;
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(CSV_STATE_PAGE_BYTES);
    for record in records {
        let mut encoded = Vec::new();
        push_text(&mut encoded, &record.schema_key)?;
        encoded.extend_from_slice(
            &u32::try_from(record.entity_pk.len())
                .map_err(|_| sdk::Error::limit_exceeded("too many CSV key components"))?
                .to_le_bytes(),
        );
        for component in &record.entity_pk {
            push_text(&mut encoded, component)?;
        }
        encoded.extend_from_slice(
            &u32::try_from(record.snapshot.len())
                .map_err(|_| sdk::Error::limit_exceeded("CSV snapshot is too large"))?
                .to_le_bytes(),
        );
        encoded.extend_from_slice(&record.snapshot);
        push_csv_paged_state(&mut pages, &mut page, &encoded);
    }
    if !page.is_empty() {
        pages.push(page);
    }
    let mut manifest = Vec::with_capacity(12);
    manifest.extend_from_slice(CSV_FALLBACK_ENTITIES_MAGIC);
    manifest.extend_from_slice(&record_count.to_le_bytes());
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many CSV fallback pages"))?
            .to_le_bytes(),
    );
    Ok((manifest, pages))
}

fn decode_entity_records(record_count: u32, pages: Vec<Vec<u8>>) -> sdk::Result<Vec<EntityRecord>> {
    let mut input = StateReader::new(pages);
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let schema_key = input.text()?;
        let component_count = input.u32()? as usize;
        let mut entity_pk = Vec::with_capacity(component_count.min(4));
        for _ in 0..component_count {
            entity_pk.push(input.text()?);
        }
        let snapshot_len = input.u32()? as usize;
        let snapshot = input.bytes(snapshot_len)?;
        records.push(EntityRecord {
            schema_key,
            entity_pk,
            snapshot,
        });
    }
    if !input.finished() {
        return Err(sdk::Error::invalid_input(
            "CSV fallback state contains trailing bytes",
        ));
    }
    Ok(records)
}

fn decode_fallback_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32)> {
    if bytes.len() != 12 || bytes.get(..4) != Some(CSV_FALLBACK_ENTITIES_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported CSV fallback state"));
    }
    Ok((
        u32::from_le_bytes(bytes[4..8].try_into().expect("fixed CSV fallback manifest")),
        u32::from_le_bytes(
            bytes[8..12]
                .try_into()
                .expect("fixed CSV fallback manifest"),
        ),
    ))
}

fn fallback_entity_page_count(root: &sdk::Root<'_>) -> sdk::Result<u32> {
    let Some(manifest) = root.get_state(CSV_FALLBACK_ENTITIES_KEY)? else {
        return Ok(0);
    };
    decode_fallback_manifest(&manifest).map(|(_, page_count)| page_count)
}

fn read_fallback_pages(root: &sdk::Root<'_>, page_count: u32) -> sdk::Result<Vec<Vec<u8>>> {
    let mut pages = Vec::with_capacity(page_count as usize);
    for ordinal in 0..page_count {
        pages.push(
            root.get_state(&csv_fallback_page_key(ordinal))?
                .ok_or_else(|| sdk::Error::invalid_input("CSV fallback page disappeared"))?,
        );
    }
    Ok(pages)
}

fn csv_fallback_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"csv/fallback-entity-page-v2/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn push_csv_paged_state(pages: &mut Vec<Vec<u8>>, page: &mut Vec<u8>, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let available = CSV_STATE_PAGE_BYTES - page.len();
        let take = available.min(bytes.len());
        page.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        if page.len() == CSV_STATE_PAGE_BYTES {
            pages.push(std::mem::replace(
                page,
                Vec::with_capacity(CSV_STATE_PAGE_BYTES),
            ));
        }
    }
}

struct StateReader {
    pages: Vec<Vec<u8>>,
    page: usize,
    offset: usize,
}

impl StateReader {
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
                .ok_or_else(|| sdk::Error::invalid_input("CSV fallback state is truncated"))?;
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
            .map_err(|_| sdk::Error::invalid_input("CSV fallback text is not UTF-8"))
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

fn read_namespace(root: &sdk::Root<'_>) -> sdk::Result<Option<IdNamespace>> {
    let Some(bytes) = root.get_state(ID_NAMESPACE_STATE)? else {
        return Ok(None);
    };
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("CSV ID namespace has invalid length"))?;
    Ok(Some(IdNamespace::from_halves(
        u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        u64::from(u32::from_be_bytes(
            bytes[8..].try_into().expect("four bytes"),
        )),
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
                u64::from(u32::from_be_bytes(
                    bytes[8..12].try_into().expect("four bytes"),
                )),
            )
        })
}

fn apply_edits(mut bytes: Vec<u8>, edits: &[core::ByteEdit]) -> sdk::Result<Vec<u8>> {
    for edit in edits.iter().rev() {
        let start = usize::try_from(edit.offset)
            .map_err(|_| sdk::Error::invalid_input("CSV edit offset exceeds guest memory"))?;
        let end =
            start
                .checked_add(usize::try_from(edit.delete_len).map_err(|_| {
                    sdk::Error::invalid_input("CSV edit deletion exceeds guest memory")
                })?)
                .ok_or_else(|| sdk::Error::invalid_input("CSV edit range overflowed"))?;
        if end > bytes.len() {
            return Err(sdk::Error::invalid_input("CSV edit exceeds accepted bytes"));
        }
        bytes.splice(start..end, edit.insert.iter().copied());
    }
    Ok(bytes)
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

    fn push(
        &mut self,
        change: EntityChange,
        creates: sdk::CreateContext,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let mut record = Vec::new();
        encode_change(change, creates, &mut record)?;
        if record.len() > self.max_bytes {
            return Err(sdk::Error::limit_exceeded(
                "one CSV entity exceeds the v3 batch limit",
            ));
        }
        if self.records > 0 && self.payload.len() + record.len() > self.max_bytes {
            self.flush(sink)?;
        }
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("CSV batch record count overflowed"))?;
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

fn encode_change(
    change: EntityChange,
    creates: sdk::CreateContext,
    output: &mut Vec<u8>,
) -> sdk::Result<()> {
    match change.schema_key.as_str() {
        TABLE_SCHEMA_KEY | ROW_SCHEMA_KEY => {}
        schema => {
            return Err(sdk::Error::invalid_input(format!(
                "unsupported CSV schema '{schema}'"
            )));
        }
    }
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match change.snapshot {
        Some(snapshot) => {
            let local_ref = change
                .entity_pk
                .as_slice()
                .first()
                .and_then(|id| local_ref(creates, id));
            if change.schema_key == ROW_SCHEMA_KEY && local_ref.is_some() {
                output.push(2);
                push_text(output, &change.schema_key)?;
                output
                    .extend_from_slice(&u64::from(local_ref.expect("checked above")).to_le_bytes());
                let snapshot = remove_created_id(snapshot)?;
                push_inline_blob(output, &snapshot)?;
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
    let record_len = output
        .len()
        .checked_sub(record_start + 4)
        .ok_or_else(|| sdk::Error::internal("CSV packet record length underflowed"))?;
    let record_len = u32::try_from(record_len)
        .map_err(|_| sdk::Error::limit_exceeded("CSV packet record exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&record_len.to_le_bytes());
    Ok(())
}

fn local_ref(creates: sdk::CreateContext, id: &str) -> Option<u32> {
    let id = uuid::Uuid::parse_str(id);
    let id = id.ok()?;
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
}

fn remove_created_id(snapshot: Vec<u8>) -> sdk::Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(&snapshot)
        .map_err(|error| sdk::Error::invalid_input(format!("invalid CSV row snapshot: {error}")))?;
    value
        .as_object_mut()
        .and_then(|object| object.remove("id"))
        .ok_or_else(|| sdk::Error::invalid_input("created CSV row snapshot has no id"))?;
    serde_json::to_vec(&value)
        .map_err(|error| sdk::Error::internal(format!("encode CSV row snapshot: {error}")))
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
    let count = u32::try_from(components.len())
        .map_err(|_| sdk::Error::limit_exceeded("too many primary-key components"))?;
    output.extend_from_slice(&count.to_le_bytes());
    for component in components {
        push_text(output, component)?;
    }
    Ok(())
}

fn push_text(output: &mut Vec<u8>, value: &str) -> sdk::Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| sdk::Error::limit_exceeded("packet text is too large"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_inline_blob(output: &mut Vec<u8>, bytes: &[u8]) -> sdk::Result<()> {
    output.push(0);
    let length = u32::try_from(bytes.len())
        .map_err(|_| sdk::Error::limit_exceeded("snapshot is too large"))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

#[cfg(target_family = "wasm")]
lix_plugin_api::export_plugin!(CsvPlugin);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_row_index_is_split_into_bounded_pages() {
        let row_count = 600_000_u32;
        let mut state = vec![0; CSV_INDEX_HEADER_BYTES as usize];
        state[32..36].copy_from_slice(&row_count.to_le_bytes());
        state.resize(state.len() + row_count as usize * 4, 0);

        let (header, pages) = split_csv_index(&state).expect("split row index");

        assert_eq!(header.len(), CSV_INDEX_HEADER_BYTES as usize);
        assert!(pages.len() > 1);
        assert!(pages.iter().all(|page| page.len() <= CSV_INDEX_PAGE_BYTES));
        assert_eq!(
            pages.iter().map(|page| page.len()).sum::<usize>(),
            row_count as usize * 4
        );
    }

    #[test]
    fn large_fallback_checkpoint_is_split_into_bounded_pages() {
        let records = (0..30_000)
            .map(|ordinal| EntityRecord {
                schema_key: ROW_SCHEMA_KEY.to_owned(),
                entity_pk: vec![format!("row-{ordinal}")],
                snapshot: vec![b'x'; 96],
            })
            .collect::<Vec<_>>();

        let (manifest, pages) = encode_entity_records(&records).expect("encode fallback rows");
        let (record_count, page_count) =
            decode_fallback_manifest(&manifest).expect("decode manifest");
        let decoded =
            decode_entity_records(record_count, pages.clone()).expect("decode fallback rows");

        assert!(pages.len() > 1);
        assert!(pages.iter().all(|page| page.len() <= CSV_STATE_PAGE_BYTES));
        assert_eq!(page_count as usize, pages.len());
        assert_eq!(decoded, records);
    }
}
