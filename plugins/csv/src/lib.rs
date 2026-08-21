//! CSV support for the row-first Component API v2.
#![allow(dead_code)]

mod core;

use core::{
    ArenaRowIndex, ChangeEffect, ColdInitialImport, Dialect, Document, FileEdit, IdNamespace,
    ROW_SCHEMA_KEY, RowChange, RowIdentity, RowRecord, TABLE_SCHEMA_KEY, Terminator,
};
use lix::plugin as sdk;
use std::sync::OnceLock;

struct CsvPlugin;

const CSV_INDEX_KEY: &[u8] = b"csv/index";
const CSV_IDENTITIES_KEY: &[u8] = b"csv/identities";
const CSV_IDENTITIES_MAGIC: &[u8; 4] = b"CSI1";
const ID_NAMESPACE_STATE: &[u8] = b"csv/id-namespace";
const CSV_INDEX_HEADER_BYTES: u32 = 36;
const CSV_INDEX_PAGE_BYTES: usize = 1024 * 1024;
const CSV_IDENTITY_PAGE_BYTES: usize = 1024 * 1024;
const TABLE_SCHEMA_JSON: &str = include_str!("../schema/csv_table.json");
const ROW_SCHEMA_JSON: &str = include_str!("../schema/csv_row.json");
static TABLE_SCHEMA_FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();
static ROW_SCHEMA_FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();
static TABLE_COMPILED_SCHEMA: OnceLock<lix_schema::CompiledSchema> = OnceLock::new();
static ROW_COMPILED_SCHEMA: OnceLock<lix_schema::CompiledSchema> = OnceLock::new();

fn cold_parse_changes(
    update: &mut sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let accepted = update.before.read_all()?;
    let mut builder = core::RowImportBuilder::new();
    let rows = update
        .typed_rows
        .as_mut()
        .ok_or_else(|| sdk::Error::internal("cold parse_changes requires durable typed rows"))?;
    while let Some(row) = rows.next()? {
        builder
            .push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.primary_key,
                row: row.row,
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
        delete_identity_checkpoint(&update.before, sink)?;
    } else {
        delete_csv_index(&update.before, sink)?;
        store_identity_checkpoint(Some(&update.before), sink, &successor)?;
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
        while let Some(change) = update.typed_row_changes.next()? {
            changes.push(RowChange {
                schema_key: change.schema_key,
                row_pk: change.primary_key,
                row: change.row,
                effect: match change.effect {
                    sdk::ChangeEffect::Content => ChangeEffect::Content,
                    sdk::ChangeEffect::FormatOnly => ChangeEffect::FormatOnly,
                },
            });
        }
        let namespace = read_namespace(&update.before)?
            .or_else(|| namespace_from_changes(&changes))
            .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        let document = open_document(before.clone(), update.path, namespace, &update.before)?;
        let (successor, edits) = document
            .rows_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            sink.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        store_identity_checkpoint(Some(&update.before), sink, &successor)?;
        delete_csv_index(&update.before, sink)?;
        Ok(())
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        sink: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(row) = input.typed_rows.next()? {
            records.push(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.primary_key,
                row: row.row,
            });
        }
        let (document, _) = Document::open_rows(records).map_err(sdk::Error::invalid_input)?;
        if let Some((namespace, state)) = document.canonical_arena_state() {
            sink.put_state(ID_NAMESPACE_STATE, &namespace)?;
            store_csv_index(sink, &state)?;
            if let Some(before) = input.before.as_ref() {
                delete_identity_checkpoint(before, sink)?;
            }
        } else {
            if let Some(before) = input.before.as_ref() {
                delete_csv_index(before, sink)?;
            }
            store_identity_checkpoint(input.before.as_ref(), sink, &document)?;
        }
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
            let Some((local_ref, row)) = import.next_row(id).map_err(sdk::Error::invalid_input)?
            else {
                break;
            };
            debug_assert_eq!(local_ref, next_local_ref);
            let (_, _, fingerprint) = typed_schema(ROW_SCHEMA_KEY)?;
            sink.create(ROW_SCHEMA_KEY, fingerprint, local_ref, &row)?;
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
            && update.before.state_len(CSV_IDENTITIES_KEY)?.is_none()
        {
            return cold_parse_changes(&mut update, sink);
        }
        if update.before.state_len(CSV_IDENTITIES_KEY)?.is_some()
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
        let base = input.row_payloads.base.typed()?;
        let a = input.row_payloads.a.typed()?;
        let b = input.row_payloads.b.typed()?;
        let Some(cells) = merge_typed_csv_cells(&base, &a, &b)? else {
            return Ok(sdk::ColumnMergeResult::UseLww);
        };
        Ok(sdk::ColumnMergeResult::Replace(
            sdk::OwnedColumnValue::typed(&sdk::TypedValue::Jsonb(
                serde_json::Value::Array(cells).into(),
            ))?,
        ))
    }
}

/// Merges the native row values used by the CSV column merger. Structural
/// fields remain immutable; only disjoint cell edits are composed.
fn merge_typed_csv_cells(
    base: &sdk::TypedRow,
    a: &sdk::TypedRow,
    b: &sdk::TypedRow,
) -> sdk::Result<Option<Vec<serde_json::Value>>> {
    let cells = |row: &sdk::TypedRow| -> sdk::Result<Vec<serde_json::Value>> {
        let Some(sdk::TypedValue::Jsonb(value)) = row.get("cells") else {
            return Err(sdk::Error::invalid_input(
                "CSV typed row cells must be a JSONB array",
            ));
        };
        let serde_json::Value::Array(cells) = value.as_value() else {
            return Err(sdk::Error::invalid_input(
                "CSV typed row cells must be a JSONB array",
            ));
        };
        Ok(cells.clone())
    };
    if a == b || base == a {
        return Ok(None);
    }
    if base == b {
        return Ok(Some(cells(a)?));
    }
    let base_cells = cells(base)?;
    let a_cells = cells(a)?;
    let b_cells = cells(b)?;
    if base.get("id") != a.get("id")
        || base.get("id") != b.get("id")
        || base.get("order_key") != a.get("order_key")
        || base.get("order_key") != b.get("order_key")
        || base.get("layout") != a.get("layout")
        || base.get("layout") != b.get("layout")
        || base_cells.len() != a_cells.len()
        || base_cells.len() != b_cells.len()
    {
        return Ok(None);
    }
    let mut merged = b_cells.clone();
    let mut changed = false;
    for index in 0..merged.len() {
        let a_changed = base_cells[index] != a_cells[index];
        let b_changed = base_cells[index] != b_cells[index];
        match (a_changed, b_changed) {
            (false, _) => {}
            (true, false) => {
                merged[index] = a_cells[index].clone();
                changed = true;
            }
            (true, true) if a_cells[index] == b_cells[index] => {}
            (true, true) => return Ok(None),
        }
    }
    Ok(changed.then_some(merged))
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

fn identity_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"csv/identity-page/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn identity_page_count(before: &sdk::Snapshot<'_>) -> sdk::Result<u32> {
    let Some(manifest) = before.get_state(CSV_IDENTITIES_KEY)? else {
        return Ok(0);
    };
    decode_identity_manifest(&manifest).map(|(_, page_count, _)| page_count)
}

fn delete_identity_checkpoint(
    before: &sdk::Snapshot<'_>,
    sink: &mut impl StateOutput,
) -> sdk::Result<()> {
    let page_count = identity_page_count(before)?;
    sink.delete_state(CSV_IDENTITIES_KEY)?;
    for ordinal in 0..page_count {
        sink.delete_state(&identity_page_key(ordinal))?;
    }
    Ok(())
}

fn store_identity_checkpoint(
    before: Option<&sdk::Snapshot<'_>>,
    sink: &mut impl StateOutput,
    document: &Document,
) -> sdk::Result<()> {
    let old_page_count = before.map(identity_page_count).transpose()?.unwrap_or(0);
    let (dialect, identities) = document.identity_checkpoint();
    let mut payload = Vec::new();
    for identity in &identities {
        payload.extend_from_slice(identity.id.as_bytes());
        payload.extend_from_slice(
            &u32::try_from(identity.order_key.len())
                .map_err(|_| sdk::Error::limit_exceeded("CSV order key is too large"))?
                .to_le_bytes(),
        );
        payload.extend_from_slice(identity.order_key.as_bytes());
    }
    let pages = payload.chunks(CSV_IDENTITY_PAGE_BYTES).collect::<Vec<_>>();
    let mut manifest = Vec::with_capacity(16);
    manifest.extend_from_slice(CSV_IDENTITIES_MAGIC);
    manifest.extend_from_slice(
        &u32::try_from(identities.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many CSV identities"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many CSV identity pages"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(&[
        dialect.delimiter,
        dialect.quote.unwrap_or(0),
        match dialect.terminator {
            Terminator::Lf => 1,
            Terminator::CrLf => 2,
            Terminator::Cr => 3,
        },
        0,
    ]);
    sink.put_state(CSV_IDENTITIES_KEY, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        sink.put_state(&identity_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        sink.delete_state(&identity_page_key(ordinal))?;
    }
    Ok(())
}

fn decode_identity_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32, Dialect)> {
    if bytes.len() != 16 || bytes.get(..4) != Some(CSV_IDENTITIES_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported CSV identity checkpoint",
        ));
    }
    let terminator = match bytes[14] {
        1 => Terminator::Lf,
        2 => Terminator::CrLf,
        3 => Terminator::Cr,
        _ => {
            return Err(sdk::Error::invalid_input(
                "invalid CSV checkpoint terminator",
            ));
        }
    };
    Ok((
        u32::from_le_bytes(bytes[4..8].try_into().expect("identity row count")),
        u32::from_le_bytes(bytes[8..12].try_into().expect("identity page count")),
        Dialect {
            delimiter: bytes[12],
            quote: (bytes[13] != 0).then_some(bytes[13]),
            terminator,
        },
    ))
}

fn read_identity_checkpoint(
    root: &sdk::Snapshot<'_>,
) -> sdk::Result<Option<(Dialect, Vec<RowIdentity>)>> {
    let Some(manifest) = root.get_state(CSV_IDENTITIES_KEY)? else {
        return Ok(None);
    };
    let (row_count, page_count, dialect) = decode_identity_manifest(&manifest)?;
    let mut payload = Vec::new();
    for ordinal in 0..page_count {
        payload.extend_from_slice(
            &root
                .get_state(&identity_page_key(ordinal))?
                .ok_or_else(|| sdk::Error::invalid_input("CSV identity page disappeared"))?,
        );
    }
    let mut offset = 0usize;
    let mut identities = Vec::with_capacity(row_count as usize);
    for _ in 0..row_count {
        let id_end = offset
            .checked_add(16)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| sdk::Error::invalid_input("CSV identity checkpoint is truncated"))?;
        let id = uuid::Uuid::from_slice(&payload[offset..id_end])
            .map_err(|_| sdk::Error::invalid_input("CSV checkpoint identity is invalid"))?;
        offset = id_end;
        let length_end = offset
            .checked_add(4)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| sdk::Error::invalid_input("CSV identity checkpoint is truncated"))?;
        let length = u32::from_le_bytes(
            payload[offset..length_end]
                .try_into()
                .expect("order length"),
        ) as usize;
        offset = length_end;
        let order_end = offset
            .checked_add(length)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| sdk::Error::invalid_input("CSV identity checkpoint is truncated"))?;
        let order_key = std::str::from_utf8(&payload[offset..order_end])
            .map_err(|_| sdk::Error::invalid_input("CSV checkpoint order key is not UTF-8"))?
            .to_owned();
        offset = order_end;
        identities.push(RowIdentity { id, order_key });
    }
    if offset != payload.len() {
        return Err(sdk::Error::invalid_input(
            "CSV identity checkpoint contains trailing bytes",
        ));
    }
    Ok(Some((dialect, identities)))
}

fn open_document(
    bytes: Vec<u8>,
    path: &str,
    namespace: IdNamespace,
    snapshot: &sdk::Snapshot<'_>,
) -> sdk::Result<Document> {
    let Some((dialect, identities)) = read_identity_checkpoint(snapshot)? else {
        return Document::open_file(bytes, Some(path), namespace)
            .map(|(document, _)| document)
            .map_err(sdk::Error::invalid_input);
    };
    Document::open_file_with_identities(bytes, dialect, namespace, &identities)
        .map_err(sdk::Error::invalid_input)
}

fn fallback_file_changed(
    update: sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let before_bytes = update.before.read_all()?;
    let namespace =
        read_namespace(&update.before)?.unwrap_or_else(|| IdNamespace::from_halves(0, 0));
    let document = open_document(before_bytes, update.before_path, namespace, &update.before)?;
    let splices = update
        .file_edits
        .iter()
        .map(|edit| FileEdit {
            offset: edit.offset,
            delete_len: edit.delete_len,
            insert: &edit.insert,
        })
        .collect::<Vec<_>>();
    let create_namespace = IdNamespace::from_namespace_bytes(update.creates.namespace_bytes());
    let (successor, changes) = document
        .file_changed_with_paths(
            &splices,
            Some(update.before_path),
            Some(update.after_path),
            create_namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
    if let Some((namespace, state)) = successor.canonical_arena_state() {
        sink.put_state(ID_NAMESPACE_STATE, &namespace)?;
        store_csv_index(sink, &state)?;
        delete_identity_checkpoint(&update.before, sink)?;
    } else {
        delete_csv_index(&update.before, sink)?;
        store_identity_checkpoint(Some(&update.before), sink, &successor)?;
    }
    for change in changes {
        emit_change(change, update.creates, sink)?;
    }
    Ok(())
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
        .find_map(|component| match component {
            sdk::TypedValue::Uuid(id) => Some(*id),
            _ => None,
        })
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
    match change.schema_key.as_ref() {
        TABLE_SCHEMA_KEY | ROW_SCHEMA_KEY => {}
        schema => {
            return Err(sdk::Error::invalid_input(format!(
                "unsupported CSV schema '{schema}'"
            )));
        }
    }
    match change.row {
        Some(row) => {
            let local_ref = change.row_pk.as_slice().first().and_then(|id| match id {
                sdk::TypedValue::Uuid(id) => local_ref(creates, id),
                _ => None,
            });
            if change.schema_key.as_ref() == ROW_SCHEMA_KEY && local_ref.is_some() {
                sink.create(&change.schema_key, local_ref.expect("checked above"), &row)?;
            } else {
                sink.upsert(
                    &change.schema_key,
                    &change.row_pk,
                    &row,
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
    fn create(&mut self, schema_key: &str, local_ref: u32, row: &sdk::TypedRow) -> sdk::Result<()>;
    fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        effect: sdk::ChangeEffect,
    ) -> sdk::Result<()>;
    fn delete(&mut self, schema_key: &str, row_pk: &[sdk::TypedValue]) -> sdk::Result<()>;
}
impl MutationOutput for sdk::RowOutput<'_, '_> {
    fn create(&mut self, s: &str, l: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, _, fingerprint) = typed_schema(s)?;
        self.create(schema_key, fingerprint, l, row)
    }
    fn upsert(
        &mut self,
        s: &str,
        k: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        _: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, schema_json, fingerprint) = typed_schema(s)?;
        validate_typed_key(schema_json, k)?;
        self.upsert(schema_key, fingerprint, k.to_vec(), row)
    }
    fn delete(&mut self, _: &str, _: &[sdk::TypedValue]) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "initial CSV parse produced a deletion",
        ))
    }
}
impl MutationOutput for sdk::RowChangeOutput<'_, '_> {
    fn create(&mut self, s: &str, l: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, _, fingerprint) = typed_schema(s)?;
        self.create(schema_key, fingerprint, l, row)
    }
    fn upsert(
        &mut self,
        s: &str,
        k: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        e: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, schema_json, fingerprint) = typed_schema(s)?;
        validate_typed_key(schema_json, k)?;
        self.upsert(schema_key, fingerprint, k.to_vec(), row, e)
    }
    fn delete(&mut self, s: &str, k: &[sdk::TypedValue]) -> sdk::Result<()> {
        let (schema_key, schema_json, fingerprint) = typed_schema(s)?;
        validate_typed_key(schema_json, k)?;
        self.delete(schema_key, fingerprint, k.to_vec())
    }
}

fn validate_typed_key(schema_json: &str, key: &[sdk::TypedValue]) -> sdk::Result<()> {
    let (compiled, schema_key) = match schema_json {
        TABLE_SCHEMA_JSON => (&TABLE_COMPILED_SCHEMA, TABLE_SCHEMA_KEY),
        ROW_SCHEMA_JSON => (&ROW_COMPILED_SCHEMA, ROW_SCHEMA_KEY),
        _ => return Err(sdk::Error::internal("unknown embedded CSV schema")),
    };
    let compiled = compiled.get_or_init(|| {
        let schema = lix_schema::from_json(schema_json).expect("embedded CSV schema must be valid");
        lix_schema::CompiledSchema::compile(&schema).expect("embedded CSV schema must compile")
    });
    if compiled.primary_key().len() != key.len() {
        return Err(sdk::Error::invalid_input(format!(
            "schema '{}' expects {} primary-key components, got {}",
            schema_key,
            compiled.primary_key().len(),
            key.len()
        )));
    }
    Ok(())
}

fn typed_schema(schema_key: &str) -> sdk::Result<(&'static str, &'static str, [u8; 32])> {
    let (schema_key, schema_json, fingerprint) = match schema_key {
        TABLE_SCHEMA_KEY => (
            TABLE_SCHEMA_KEY,
            TABLE_SCHEMA_JSON,
            &TABLE_SCHEMA_FINGERPRINT,
        ),
        ROW_SCHEMA_KEY => (ROW_SCHEMA_KEY, ROW_SCHEMA_JSON, &ROW_SCHEMA_FINGERPRINT),
        _ => {
            return Err(sdk::Error::invalid_input(format!(
                "CSV plugin does not support schema '{schema_key}'"
            )));
        }
    };
    Ok((
        schema_key,
        schema_json,
        *fingerprint.get_or_init(|| {
            sdk::schema_fingerprint(schema_json).expect("embedded CSV schema must be valid")
        }),
    ))
}

fn local_ref(creates: sdk::CreateContext, id: &uuid::Uuid) -> Option<u32> {
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
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
    fn dense_successor_uses_the_compact_arena() {
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
    fn structural_successor_is_not_a_dense_arena() {
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
