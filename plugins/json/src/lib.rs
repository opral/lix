//! JSON support for the row-first Component API v2.
#![allow(dead_code)]

mod core;

use core::{
    ArenaJsonRelation, ArenaJsonScalar, ChangeEffect, Document, FileEdit, IdNamespace, RowChange,
    RowImportBuilder, RowRecord,
};
use lix::plugin as sdk;
use std::sync::OnceLock;

struct JsonPlugin;

const SCALAR_INDEX_STATE: &[u8] = b"json/scalar-index";
const SCALAR_SHIFTS_STATE: &[u8] = b"json/scalar-shifts";
const ID_NAMESPACE_STATE: &[u8] = b"json/id-namespace";
const SCALAR_INDEX_MAGIC: &[u8; 4] = b"JSS3";
const SCALAR_INDEX_HEADER_BYTES: u32 = 16;
const SCALAR_INDEX_ENTRY_BYTES: u32 = 20;
const SCALAR_PAGE_BYTES: usize = 1024 * 1024;
const STATE_PAGE_BYTES: usize = 1024 * 1024;
const ROOT_SCHEMA_JSON: &str = include_str!("../schema/json_root.json");
const OBJECT_MEMBER_SCHEMA_JSON: &str = include_str!("../schema/json_object_member.json");
const ARRAY_ITEM_SCHEMA_JSON: &str = include_str!("../schema/json_array_item.json");
static ROOT_SCHEMA_FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();
static OBJECT_MEMBER_SCHEMA_FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();
static ARRAY_ITEM_SCHEMA_FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();

fn cold_parse_changes(
    update: &mut sdk::ParseChangesInput<'_>,
    sink: &mut sdk::RowChangeOutput<'_, '_>,
) -> sdk::Result<()> {
    let accepted = update.before.read_all()?;
    let mut builder = RowImportBuilder::new();
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
    let (_, changes) = document
        .file_changed(&splices, create_namespace)
        .map_err(sdk::Error::invalid_input)?;
    sink.put_state(ID_NAMESPACE_STATE, &update.creates.namespace_bytes())?;
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
        let namespace = read_namespace(&update.before, ID_NAMESPACE_STATE)?
            .or_else(|| namespace_from_changes(&changes))
            .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        let document = Document::open_file(before, Some(update.path), namespace)
            .map(|(document, _)| document)
            .map_err(sdk::Error::invalid_input)?;
        let (_, edits) = document
            .rows_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            sink.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        Ok(())
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        sink: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut builder = RowImportBuilder::new();
        while let Some(row) = input.typed_rows.next()? {
            let record = RowRecord {
                schema_key: row.schema_key,
                row_pk: row.primary_key,
                row: row.row,
            };
            builder.push(record).map_err(sdk::Error::invalid_input)?;
        }
        let (document, _) = builder.finish().map_err(sdk::Error::invalid_input)?;
        sink.write(&document.bytes())
    }

    fn parse(input: sdk::ParseInput<'_>, sink: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        sink.coalesce_schema_pages()?;
        let bytes = input.file.read_all()?;
        let namespace = IdNamespace::from_namespace_bytes(input.creates.namespace_bytes());
        let (document, changes) = Document::open_fresh_file(bytes, Some(input.path), namespace)
            .map_err(sdk::Error::invalid_input)?;
        sink.put_state(ID_NAMESPACE_STATE, &input.creates.namespace_bytes())?;
        store_scalar_state(sink, &document)?;
        emit_initial_changes(changes, input.creates, sink)?;
        Ok(())
    }

    fn parse_changes(
        mut update: sdk::ParseChangesInput<'_>,
        sink: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        if update.before.state_len(SCALAR_INDEX_STATE)?.is_none() {
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
        if update.before_path == update.after_path
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
        let document =
            Document::open_file(before_bytes, Some(update.before_path), accepted_namespace)
                .map(|(document, _)| document)
                .map_err(sdk::Error::invalid_input)?;
        let (_, changes) = document
            .file_changed(&splices, create_namespace)
            .map_err(sdk::Error::invalid_input)?;
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
        .find_map(|component| match component {
            sdk::TypedValue::Uuid(value) => Some(*value),
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
        (ArenaJsonRelation::Snapshot, [sdk::TypedValue::Text(id)]) if id == "root" => {
            output.push(0)
        }
        (
            ArenaJsonRelation::Object,
            [sdk::TypedValue::Text(parent_id), sdk::TypedValue::Text(key)],
        ) => {
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
        (ArenaJsonRelation::Array, [sdk::TypedValue::Uuid(id)]) => {
            output.push(2);
            output.extend_from_slice(id.as_bytes());
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
            vec![sdk::TypedValue::Text("root".to_owned())],
            None,
            None,
        ),
        1 => (
            ArenaJsonRelation::Object,
            vec![
                sdk::TypedValue::Text(take_state_text(&mut input)?),
                sdk::TypedValue::Text(take_state_text(&mut input)?),
            ],
            None,
            Some(take_state_text(&mut input)?),
        ),
        2 => {
            let id = take_state_uuid(&mut input)?;
            (
                ArenaJsonRelation::Array,
                vec![sdk::TypedValue::Uuid(id)],
                Some(take_state_text(&mut input)?),
                Some(take_state_text(&mut input)?),
            )
        }
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

fn take_state_uuid(input: &mut &[u8]) -> sdk::Result<uuid::Uuid> {
    let bytes: [u8; 16] = input
        .get(..16)
        .ok_or_else(|| sdk::Error::invalid_input("truncated JSON scalar UUID"))?
        .try_into()
        .expect("sixteen-byte UUID field");
    *input = &input[16..];
    Ok(uuid::Uuid::from_bytes(bytes))
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
        emit_change(change, creates, sink)?;
    }
    Ok(())
}

fn emit_initial_changes<I>(
    changes: I,
    creates: sdk::CreateContext,
    sink: &mut sdk::RowOutput<'_, '_>,
) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<RowChange, String>>,
{
    for change in changes {
        emit_change(change.map_err(sdk::Error::invalid_input)?, creates, sink)?;
    }
    Ok(())
}

fn emit_change(
    change: RowChange,
    creates: sdk::CreateContext,
    sink: &mut impl MutationOutput,
) -> sdk::Result<()> {
    match change.row {
        Some(row) => {
            let local_ref = change
                .row_pk
                .first()
                .filter(|_| change.row_pk.len() == 1)
                .and_then(|id| local_ref(creates, id));
            if let Some(local_ref) = local_ref {
                sink.create(&change.schema_key, local_ref, &row)
            } else {
                sink.upsert(
                    &change.schema_key,
                    change.row_pk,
                    &row,
                    match change.effect {
                        ChangeEffect::Content => sdk::ChangeEffect::Content,
                        ChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
                    },
                )
            }
        }
        None => sink.delete(&change.schema_key, change.row_pk),
    }
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
        row_pk: Vec<sdk::TypedValue>,
        row: &sdk::TypedRow,
        effect: sdk::ChangeEffect,
    ) -> sdk::Result<()>;
    fn delete(&mut self, schema_key: &str, row_pk: Vec<sdk::TypedValue>) -> sdk::Result<()>;
}
impl MutationOutput for sdk::RowOutput<'_, '_> {
    fn create(&mut self, s: &str, l: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(s)?;
        self.create(schema_key, fingerprint, l, row)
    }
    fn upsert(
        &mut self,
        s: &str,
        k: Vec<sdk::TypedValue>,
        row: &sdk::TypedRow,
        _: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(s)?;
        self.upsert(schema_key, fingerprint, k, row)
    }
    fn delete(&mut self, _: &str, _: Vec<sdk::TypedValue>) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "initial JSON parse produced a deletion",
        ))
    }
}
impl MutationOutput for sdk::RowChangeOutput<'_, '_> {
    fn create(&mut self, s: &str, l: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(s)?;
        self.create(schema_key, fingerprint, l, row)
    }
    fn upsert(
        &mut self,
        s: &str,
        k: Vec<sdk::TypedValue>,
        row: &sdk::TypedRow,
        e: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(s)?;
        self.upsert(schema_key, fingerprint, k, row, e)
    }
    fn delete(&mut self, s: &str, k: Vec<sdk::TypedValue>) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(s)?;
        self.delete(schema_key, fingerprint, k)
    }
}

fn typed_schema(schema_key: &str) -> sdk::Result<(&'static str, [u8; 32])> {
    let (schema_key, schema_json, fingerprint) = match schema_key {
        "json_root" => ("json_root", ROOT_SCHEMA_JSON, &ROOT_SCHEMA_FINGERPRINT),
        "json_object_member" => (
            "json_object_member",
            OBJECT_MEMBER_SCHEMA_JSON,
            &OBJECT_MEMBER_SCHEMA_FINGERPRINT,
        ),
        "json_array_item" => (
            "json_array_item",
            ARRAY_ITEM_SCHEMA_JSON,
            &ARRAY_ITEM_SCHEMA_FINGERPRINT,
        ),
        _ => {
            return Err(sdk::Error::invalid_input(format!(
                "JSON plugin does not support schema '{schema_key}'"
            )));
        }
    };
    Ok((
        schema_key,
        *fingerprint.get_or_init(|| {
            sdk::schema_fingerprint(schema_json).expect("embedded JSON schema must be valid")
        }),
    ))
}

fn local_ref(creates: sdk::CreateContext, id: &sdk::TypedValue) -> Option<u32> {
    let sdk::TypedValue::Uuid(id) = id else {
        return None;
    };
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
            row_pk: vec![sdk::TypedValue::Text("root".to_owned())],
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
    fn scalar_arena_change_carries_native_jsonb() {
        let metadata = ArenaJsonScalar {
            start: 0,
            length: 1,
            relation: ArenaJsonRelation::Snapshot,
            row_pk: vec![sdk::TypedValue::Text("root".to_owned())],
            parent_id: None,
            order_key: None,
            prefix_json: None,
            suffix_json: None,
            empty_json: None,
        };

        let change = Document::scalar_change_from_arena(metadata, b"1")
            .expect("valid scalar")
            .expect("scalar change");
        assert_eq!(
            change.row.and_then(|row| row.get("scalar_json").cloned()),
            Some(sdk::TypedValue::Jsonb(serde_json::json!(1).into()))
        );
    }

    #[test]
    fn scalar_state_preserves_native_uuid_identity() {
        let id = uuid::Uuid::from_u128(0x1234);
        let scalar = ArenaJsonScalar {
            start: 7,
            length: 4,
            relation: ArenaJsonRelation::Array,
            row_pk: vec![sdk::TypedValue::Uuid(id)],
            parent_id: Some("root".to_owned()),
            order_key: Some("01".to_owned()),
            prefix_json: Some(" ".to_owned()),
            suffix_json: None,
            empty_json: None,
        };

        let encoded = encode_scalar_metadata(&scalar).expect("typed scalar metadata");
        let decoded = decode_scalar_metadata(&encoded).expect("typed scalar metadata roundtrip");
        assert_eq!(decoded.row_pk, vec![sdk::TypedValue::Uuid(id)]);
        assert_eq!(decoded.relation, ArenaJsonRelation::Array);
        assert_eq!(decoded.parent_id.as_deref(), Some("root"));
    }

    #[test]
    fn nested_document_emits_every_typed_row() {
        let source = br#"{"profile":{"name":"Ada","active":true},"items":[{"label":"one"},{"label":"two"}]}"#.to_vec();
        let (document, _) =
            Document::open_fresh_file(source, Some("nested.json"), IdNamespace::from_halves(1, 2))
                .expect("nested JSON document");
        let rows = document.row_records().expect("nested typed rows");

        assert_eq!(rows.len(), 9);
        let name = rows
            .iter()
            .find(|record| record.row.get("key") == Some(&sdk::TypedValue::Text("name".to_owned())))
            .expect("nested name row");
        assert!(matches!(
            name.row_pk.as_slice(),
            [sdk::TypedValue::Text(_), sdk::TypedValue::Text(key)] if key == "name"
        ));
        assert_eq!(
            name.row.get("scalar_json"),
            Some(&sdk::TypedValue::Jsonb(serde_json::json!("Ada").into()))
        );
    }

    #[test]
    fn ten_mib_flat_document_streams_all_native_jsonb_rows() {
        use std::io::Write as _;

        const BYTES: usize = 10 * 1024 * 1024;
        const PROPERTIES: usize = 4_096;
        let mut source = Vec::with_capacity(BYTES);
        source.push(b'{');
        let fixed_bytes = 2 + PROPERTIES * 12 + PROPERTIES.saturating_sub(1);
        let padding = BYTES - fixed_bytes;
        for index in 0..PROPERTIES {
            if index > 0 {
                source.push(b',');
            }
            write!(&mut source, "\"p{index:06}\":\"").expect("write property prefix");
            let property_padding = padding / PROPERTIES + usize::from(index < padding % PROPERTIES);
            source.extend(std::iter::repeat_n(b'x', property_padding));
            source.push(b'"');
        }
        source.push(b'}');
        assert_eq!(source.len(), BYTES);

        let (_, changes) =
            Document::open_fresh_file(source, Some("large.json"), IdNamespace::from_halves(1, 2))
                .expect("large JSON document");
        let mut rows = 0usize;
        for change in changes {
            let change = change.expect("large typed row");
            if rows > 0 {
                assert!(matches!(
                    change.row.as_ref().and_then(|row| row.get("scalar_json")),
                    Some(sdk::TypedValue::Jsonb(value))
                        if matches!(value.as_value(), serde_json::Value::String(_))
                ));
            }
            rows += 1;
        }
        assert_eq!(rows, PROPERTIES + 1);
    }

    #[test]
    fn typed_rows_roundtrip_native_jsonb_scalars() {
        let source = br#"{"number":1,"boolean":true,"null":null,"string":"value"}"#.to_vec();
        let (document, _) = Document::open_fresh_file(
            source.clone(),
            Some("document.json"),
            IdNamespace::from_halves(1, 2),
        )
        .expect("valid JSON document");
        let records = document.row_records().expect("typed rows");

        for (key, expected) in [
            ("number", serde_json::json!(1)),
            ("boolean", serde_json::json!(true)),
            ("null", serde_json::Value::Null),
            ("string", serde_json::json!("value")),
        ] {
            let row = records
                .iter()
                .find(|record| {
                    record.row.get("key") == Some(&sdk::TypedValue::Text(key.to_owned()))
                })
                .unwrap_or_else(|| panic!("missing row for {key}"));
            assert_eq!(
                row.row.get("scalar_json"),
                Some(&sdk::TypedValue::Jsonb(expected.into()))
            );
        }

        let (roundtrip, _) = Document::open_rows(records).expect("valid typed rows");
        assert_eq!(roundtrip.bytes(), source);
    }

    #[test]
    fn semantic_scalar_write_consumes_native_jsonb() {
        let source = br#"{"value":1}"#.to_vec();
        let (document, _) = Document::open_file(
            source,
            Some("document.json"),
            IdNamespace::from_halves(1, 2),
        )
        .expect("valid JSON document");
        let mut record = document
            .row_records()
            .expect("typed rows")
            .into_iter()
            .find(|record| record.schema_key.as_ref() == "json_object_member")
            .expect("object member row");
        record.row.insert(
            "scalar_json".to_owned(),
            sdk::TypedValue::Jsonb(serde_json::json!(2).into()),
        );
        let change = RowChange {
            schema_key: record.schema_key,
            row_pk: record.row_pk,
            row: Some(record.row),
            effect: ChangeEffect::Content,
        };

        let (_, edits) = document.rows_changed(&[change]).expect("semantic write");
        assert_eq!(
            apply_edits(document.bytes(), &edits).expect("valid edits"),
            br#"{"value":2}"#
        );
    }

    #[test]
    fn scalar_sparse_path_defers_to_complete_document_when_edit_adds_a_sibling() {
        let metadata = ArenaJsonScalar {
            start: 5,
            length: 1,
            relation: ArenaJsonRelation::Object,
            row_pk: vec![
                sdk::TypedValue::Text("root".to_owned()),
                sdk::TypedValue::Text("a".to_owned()),
            ],
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
