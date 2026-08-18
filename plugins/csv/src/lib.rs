//! CSV support for the row-first Component API v1.
#![allow(dead_code)]

mod core;

use core::{
    ArenaRowIndex, ChangeEffect, ColdInitialImport, Document, FileEdit, IdNamespace,
    ROW_SCHEMA_KEY, RowChange, RowConflictResolution, RowRecord, TABLE_SCHEMA_KEY,
    resolve_row_conflict,
};
use lix::plugin as sdk;

struct CsvPlugin;

const CSV_INDEX_KEY: &[u8] = b"csv/index";
const CSV_FALLBACK_ROWS_KEY: &[u8] = b"csv/fallback-rows";
const CSV_FALLBACK_ROWS_MAGIC: &[u8; 4] = b"CFE2";
const ID_NAMESPACE_STATE: &[u8] = b"csv/id-namespace";
const CSV_INDEX_HEADER_BYTES: u32 = 36;
const CSV_INDEX_PAGE_BYTES: usize = 1024 * 1024;
const CSV_STATE_PAGE_BYTES: usize = 1024 * 1024;

fn cold_parse_changes(
    update: &mut sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let accepted = update.before.read_all()?;
    let mut builder = core::RowImportBuilder::new();
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
    if !document.bytes_equal(&accepted) {
        let reconcile = [FileEdit {
            offset: 0,
            delete_len: document.byte_len() as u64,
            insert: &accepted,
        }];
        document = document
            .file_changed_with_paths(
                &reconcile,
                Some(update.before_path),
                Some(update.before_path),
                namespace,
            )
            .map_err(sdk::Error::invalid_input)?
            .0;
    }
    let splices = update
        .file_edits
        .iter()
        .map(|edit| FileEdit {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert: &edit.insert,
        })
        .collect::<Vec<_>>();
    let (successor, changes) = document
        .file_changed_with_paths(
            &splices,
            Some(update.before_path),
            Some(update.after_path),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
    if let Some((namespace, state)) = successor.canonical_arena_state() {
        sink.put_state(ID_NAMESPACE_STATE, &namespace)?;
        store_csv_index(sink, &state)?;
    } else {
        sink.put_state(ID_NAMESPACE_STATE, &update.creates.namespace_bytes())?;
        store_fallback_rows_fresh(
            sink,
            &successor.row_records().map_err(sdk::Error::invalid_input)?,
        )?;
    }
    for change in changes {
        emit_change(change, update.creates, sink)?;
    }
    Ok(())
}

impl sdk::FileProjection for CsvPlugin {
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
        let document = if let Some(manifest) = update.before.get_state(CSV_FALLBACK_ROWS_KEY)? {
            let (record_count, page_count) = decode_fallback_manifest(&manifest)?;
            Document::open_rows(decode_row_records(
                record_count,
                read_fallback_pages(&update.before, page_count)?,
            )?)
            .map_err(sdk::Error::invalid_input)?
            .0
        } else {
            let namespace = read_namespace(&update.before)?
                .or_else(|| namespace_from_changes(&changes))
                .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
            Document::open_file(before.clone(), Some(update.path), namespace)
                .map_err(sdk::Error::invalid_input)?
                .0
        };
        let (successor, edits) = document
            .rows_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            sink.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        let records = successor.row_records().map_err(sdk::Error::invalid_input)?;
        store_fallback_rows_in_transaction(&update.before, sink, &records)?;
        delete_csv_index(&update.before, sink)?;
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
        let (document, _) =
            Document::open_rows(records.clone()).map_err(sdk::Error::invalid_input)?;
        store_fallback_rows_fresh(sink, &records)?;
        sink.write(&document.bytes())
    }

    fn parse(input: sdk::ParseInput<'_>, sink: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        let bytes = input.file.read_all()?;
        let mut import =
            ColdInitialImport::open(bytes, Some(input.path)).map_err(sdk::Error::invalid_input)?;
        let state = import.arena_state(input.creates.namespace_bytes());
        sink.put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        store_csv_index(sink, &state)?;
        emit_change(import.table_change(), input.creates, sink)?;
        let mut next_local_ref = 0_u32;
        loop {
            let id = input.creates.id(next_local_ref);
            let Some((local_ref, snapshot)) = import
                .next_row_snapshot(&id)
                .map_err(sdk::Error::invalid_input)?
            else {
                break;
            };
            debug_assert_eq!(local_ref, next_local_ref);
            sink.create(ROW_SCHEMA_KEY, local_ref, &snapshot)?;
            next_local_ref = next_local_ref
                .checked_add(1)
                .ok_or_else(|| sdk::Error::limit_exceeded("CSV row count exceeds u32"))?;
        }
        Ok(())
    }

    fn parse_changes(
        mut update: sdk::ParseChangesInput<'_>,
        sink: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        if update.before.state_len(CSV_INDEX_KEY)?.is_none()
            && update.before.state_len(CSV_FALLBACK_ROWS_KEY)?.is_none()
        {
            return cold_parse_changes(&mut update, sink);
        }
        if update.before.state_len(CSV_FALLBACK_ROWS_KEY)?.is_some()
            || update.before_path != update.after_path
            || update.file_edits.iter().len() != 1
        {
            return fallback_file_changed(update, sink);
        }
        let edit = update
            .file_edits
            .iter()
            .next()
            .expect("the sparse path requires exactly one edit");
        if u64::try_from(edit.insert.len()).expect("usize fits u64") != edit.delete_len {
            return fallback_file_changed(update, sink);
        }
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
            let import =
                ColdInitialImport::open(update.before.read_all()?, Some(update.before_path))
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
            store_csv_index(sink, &state)?;
        }
        emit_change(change, update.creates, sink)?;
        Ok(())
    }
}

impl sdk::ColumnMerger for CsvPlugin {
    fn merge(input: sdk::ColumnMerge<'_>) -> sdk::Result<sdk::ColumnMergeResult> {
        if input.row.schema_key != ROW_SCHEMA_KEY || input.column != "cells" {
            return Ok(sdk::ColumnMergeResult::UseLww);
        }
        if [input.base.len(), input.a.len(), input.b.len()]
            .into_iter()
            .flatten()
            .any(|length| length > 64 * 1024)
        {
            return Ok(sdk::ColumnMergeResult::UseLww);
        }
        let base = input.rows.base.read()?;
        let a = input.rows.a.read()?;
        let b = input.rows.b.read()?;
        let resolved = resolve_row_conflict(Some(&base), Some(&a), Some(&b));
        let snapshot = match resolved {
            RowConflictResolution::TakeA => a,
            RowConflictResolution::Replace(snapshot) => snapshot,
            RowConflictResolution::TakeB | RowConflictResolution::Delete => {
                return Ok(sdk::ColumnMergeResult::UseLww);
            }
        };
        let row: serde_json::Value = serde_json::from_slice(&snapshot).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid merged CSV row: {error}"))
        })?;
        let value = row
            .get("cells")
            .ok_or_else(|| sdk::Error::invalid_input("merged CSV row has no cells column"))?;
        let encoded = serde_json::to_vec(value)
            .map_err(|error| sdk::Error::internal(format!("encode merged CSV cells: {error}")))?;
        Ok(sdk::ColumnMergeResult::Replace(
            sdk::OwnedColumnValue::json(encoded),
        ))
    }
}

fn store_csv_index(successor: &mut impl StateOutput, state: &[u8]) -> sdk::Result<()> {
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

fn csv_index_page_count(root: &sdk::Snapshot<'_>) -> sdk::Result<u32> {
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

fn delete_csv_index(
    before: &sdk::Snapshot<'_>,
    successor: &mut impl StateOutput,
) -> sdk::Result<()> {
    let page_count = csv_index_page_count(before)?;
    successor.delete_state(CSV_INDEX_KEY)?;
    for ordinal in 0..page_count {
        successor.delete_state(&csv_index_page_key(ordinal))?;
    }
    Ok(())
}

fn csv_index_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"csv/index-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn fallback_file_changed(
    update: sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let before_bytes = update.before.read_all()?;
    let document = if let Some(manifest) = update.before.get_state(CSV_FALLBACK_ROWS_KEY)? {
        let (record_count, page_count) = decode_fallback_manifest(&manifest)?;
        let pages = read_fallback_pages(&update.before, page_count)?;
        let (document, _) = Document::open_rows(decode_row_records(record_count, pages)?)
            .map_err(sdk::Error::invalid_input)?;
        let rendered = document.bytes();
        if rendered == before_bytes {
            document
        } else {
            let reconcile = [FileEdit {
                offset: 0,
                delete_len: rendered.len() as u64,
                insert: &before_bytes,
            }];
            let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
            document
                .file_changed_with_paths(
                    &reconcile,
                    Some(update.before_path),
                    Some(update.before_path),
                    namespace,
                )
                .map_err(sdk::Error::invalid_input)?
                .0
        }
    } else {
        let namespace =
            read_namespace(&update.before)?.unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        Document::open_file(before_bytes, Some(update.before_path), namespace)
            .map_err(sdk::Error::invalid_input)?
            .0
    };
    let splices = update
        .file_edits
        .iter()
        .map(|edit| FileEdit {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert: &edit.insert,
        })
        .collect::<Vec<_>>();
    let namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
    let (successor, changes) = document
        .file_changed_with_paths(
            &splices,
            Some(update.before_path),
            Some(update.after_path),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
    let records = successor.row_records().map_err(sdk::Error::invalid_input)?;
    store_fallback_rows_in_transaction(&update.before, sink, &records)?;
    delete_csv_index(&update.before, sink)?;
    for change in changes {
        emit_change(change, update.creates, sink)?;
    }
    Ok(())
}

fn store_fallback_rows_in_transaction(
    before: &sdk::Snapshot<'_>,
    successor: &mut impl StateOutput,
    records: &[RowRecord],
) -> sdk::Result<()> {
    let old_page_count = fallback_row_page_count(before)?;
    let (manifest, pages) = encode_row_records(records)?;
    successor.put_state(CSV_FALLBACK_ROWS_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&csv_fallback_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        successor.delete_state(&csv_fallback_page_key(ordinal))?;
    }
    Ok(())
}

fn store_fallback_rows_fresh(
    successor: &mut impl StateOutput,
    records: &[RowRecord],
) -> sdk::Result<()> {
    let (manifest, pages) = encode_row_records(records)?;
    successor.put_state(CSV_FALLBACK_ROWS_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&csv_fallback_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn encode_row_records(records: &[RowRecord]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let record_count = u32::try_from(records.len())
        .map_err(|_| sdk::Error::limit_exceeded("too many CSV fallback rows"))?;
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(CSV_STATE_PAGE_BYTES);
    for record in records {
        let mut encoded = Vec::new();
        push_text(&mut encoded, &record.schema_key)?;
        encoded.extend_from_slice(
            &u32::try_from(record.row_pk.len())
                .map_err(|_| sdk::Error::limit_exceeded("too many CSV key components"))?
                .to_le_bytes(),
        );
        for component in &record.row_pk {
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
    manifest.extend_from_slice(CSV_FALLBACK_ROWS_MAGIC);
    manifest.extend_from_slice(&record_count.to_le_bytes());
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many CSV fallback pages"))?
            .to_le_bytes(),
    );
    Ok((manifest, pages))
}

fn decode_row_records(record_count: u32, pages: Vec<Vec<u8>>) -> sdk::Result<Vec<RowRecord>> {
    let mut input = StateReader::new(pages);
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
            "CSV fallback state contains trailing bytes",
        ));
    }
    Ok(records)
}

fn decode_fallback_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32)> {
    if bytes.len() != 12 || bytes.get(..4) != Some(CSV_FALLBACK_ROWS_MAGIC) {
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

fn fallback_row_page_count(root: &sdk::Snapshot<'_>) -> sdk::Result<u32> {
    let Some(manifest) = root.get_state(CSV_FALLBACK_ROWS_KEY)? else {
        return Ok(0);
    };
    decode_fallback_manifest(&manifest).map(|(_, page_count)| page_count)
}

fn read_fallback_pages(root: &sdk::Snapshot<'_>, page_count: u32) -> sdk::Result<Vec<Vec<u8>>> {
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
    let mut key = b"csv/fallback-row-page/".to_vec();
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

fn read_namespace(root: &sdk::Snapshot<'_>) -> sdk::Result<Option<IdNamespace>> {
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

fn emit_change(
    change: RowChange,
    creates: sdk::CreateContext,
    sink: &mut impl MutationOutput,
) -> sdk::Result<()> {
    match change.schema_key.as_str() {
        TABLE_SCHEMA_KEY | ROW_SCHEMA_KEY => {}
        schema => {
            return Err(sdk::Error::invalid_input(format!(
                "unsupported CSV schema '{schema}'"
            )));
        }
    }
    match change.snapshot {
        Some(snapshot) => {
            let local_ref = change
                .row_pk
                .as_slice()
                .first()
                .and_then(|id| local_ref(creates, id));
            if change.schema_key == ROW_SCHEMA_KEY && local_ref.is_some() {
                sink.create(
                    &change.schema_key,
                    local_ref.expect("checked above"),
                    &snapshot,
                )?;
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
            "initial CSV parse produced a deletion",
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
    let id = uuid::Uuid::parse_str(id);
    let id = id.ok()?;
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
lix::plugin::export_capabilities! {
    column_merger: CsvPlugin,
    file_projection: CsvPlugin,
}

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
            .map(|ordinal| RowRecord {
                schema_key: ROW_SCHEMA_KEY.to_owned(),
                row_pk: vec![format!("row-{ordinal}")],
                snapshot: vec![b'x'; 96],
            })
            .collect::<Vec<_>>();

        let (manifest, pages) = encode_row_records(&records).expect("encode fallback rows");
        let (record_count, page_count) =
            decode_fallback_manifest(&manifest).expect("decode manifest");
        let decoded =
            decode_row_records(record_count, pages.clone()).expect("decode fallback rows");

        assert!(pages.len() > 1);
        assert!(pages.iter().all(|page| page.len() <= CSV_STATE_PAGE_BYTES));
        assert_eq!(page_count as usize, pages.len());
        assert_eq!(decoded, records);
    }

    #[test]
    fn dense_successor_uses_the_compact_arena_without_row_snapshots() {
        let namespace = IdNamespace::from_halves(7, 11);
        let (document, _) =
            Document::open_file(b"alpha,one\nbeta,two\n".to_vec(), None, namespace).unwrap();
        let (stored_namespace, state) = document
            .canonical_arena_state()
            .expect("initial dense identities should use the compact arena");

        assert_eq!(stored_namespace, namespace.0[..12]);
        let index = ArenaRowIndex::decode(&state).expect("compact arena should decode");
        assert_eq!(index.row_range_for_edit(1, 1).unwrap(), (0, 0, 10));
    }

    #[test]
    fn structural_successor_keeps_the_paged_identity_checkpoint() {
        let namespace = IdNamespace::from_halves(7, 11);
        let (document, _) =
            Document::open_file(b"alpha,one\nbeta,two\n".to_vec(), None, namespace).unwrap();
        let inserted_namespace = IdNamespace::from_halves(13, 17);
        let (successor, _) = document
            .file_changed_with_paths(
                &[FileEdit {
                    offset: 0,
                    delete_len: 0,
                    insert: b"new,zero\n",
                }],
                None,
                None,
                inserted_namespace,
            )
            .unwrap();

        assert!(
            successor.canonical_arena_state().is_none(),
            "a mixed-namespace structural successor cannot be represented by one dense namespace"
        );
    }
}
