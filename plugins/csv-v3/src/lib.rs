//! Functional CSV port to Plugin API v3.
//!
//! This first profile lane deliberately reconstructs the document from durable
//! rows in a cold guest. The measured result is the baseline for replacing it
//! with affected row-span and identity pages.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;
use plugin_csv_core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord, IdNamespace,
    InputSplice as CoreInputSplice, ROW_SCHEMA_KEY, V3RowIndexRecord, encode_row_snapshot,
    parse_row_snapshot,
};

const INDEX_VERSION_KEY: &[u8] = b"csv/v3/index-version";
const INDEX_VERSION: &[u8] = b"row-byte-windows-v1";
const INDEX_KEY_PREFIX: &[u8] = b"csv/v3/rows/";
const INDEX_WINDOW_BYTES: u64 = 16 * 1024;

struct CsvV3Plugin;

impl sdk::FormatPlugin for CsvV3Plugin {
    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let bytes = read_root_bytes(&input.accepted, budget)?;
        let (document, changes) = CoreDocument::open_file(
            bytes,
            input.descriptor.path.as_deref(),
            namespace(input.creates),
        )
        .map_err(sdk::Error::invalid_input)?;
        write_row_index(&input.successor, budget, &document)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: changes
                .map(|change| {
                    change
                        .map(core_change_to_sdk)
                        .map_err(sdk::Error::invalid_input)
                })
                .collect::<sdk::Result<Vec<_>>>()?,
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        if let Some(changes) = sparse_row_file_changed(&input, budget)? {
            return Ok(sdk::FileResult {
                successor: input.successor,
                changes,
            });
        }
        let document = document_from_root(&input.before, budget)?;
        let inserts = input
            .edits
            .iter()
            .map(|edit| match &edit.insert {
                sdk::InputBytes::Inline(bytes) => Ok(bytes.clone()),
                sdk::InputBytes::AfterRange(range) => input
                    .successor
                    .read_file(
                        budget,
                        range.offset,
                        u32::try_from(range.length)
                            .map_err(|_| sdk::Error::RecordTooLarge(range.length))?,
                    )
                    .map_err(arena_error),
            })
            .collect::<sdk::Result<Vec<_>>>()?;
        let splices = input
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| CoreInputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let (after, changes) = document
            .file_changed_with_paths(
                &splices,
                input.before_descriptor.path.as_deref(),
                input.after_descriptor.path.as_deref(),
                namespace(input.creates),
            )
            .map_err(sdk::Error::invalid_input)?;
        write_row_index(&input.successor, budget, &after)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: changes.into_iter().map(core_change_to_sdk).collect(),
        })
    }

    fn open_entities(
        budget: &sdk::Budget,
        input: sdk::OpenEntitiesInput,
    ) -> sdk::Result<sdk::EntityResult> {
        let entities = read_entities_from_root(&input.durable, budget)?;
        let (document, mut edit) =
            CoreDocument::open_entities(entities).map_err(sdk::Error::invalid_input)?;
        let accepted = read_root_bytes(&input.durable, budget)?;
        let edits = if edit.insert.as_slice() == accepted {
            Vec::new()
        } else {
            edit.delete_len = accepted.len() as u64;
            vec![core_edit_to_sdk(edit)]
        };
        write_row_index(&input.successor, budget, &document)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits,
        })
    }

    fn entities_changed(
        budget: &sdk::Budget,
        input: sdk::EntityUpdate,
    ) -> sdk::Result<sdk::EntityResult> {
        let document = document_from_root(&input.before, budget)?;
        let mut changes = Vec::with_capacity(input.changed_entities.len());
        for changed in &input.changed_entities {
            let (schema_key, entity_pk) = sdk::decode_entity_key(&changed.key)?;
            changes.push(CoreEntityChange {
                schema_key,
                entity_pk,
                snapshot: input
                    .successor
                    .get_entity(budget, &changed.key)
                    .map_err(arena_error)?,
                effect: if changed.format_only {
                    CoreChangeEffect::FormatOnly
                } else {
                    CoreChangeEffect::Content
                },
            });
        }
        let (after, edits) = document
            .entities_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        write_row_index(&input.successor, budget, &after)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
        })
    }
}

fn document_from_root(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<CoreDocument> {
    CoreDocument::open_entities(read_entities_from_root(root, budget)?)
        .map(|(document, _)| document)
        .map_err(sdk::Error::invalid_input)
}

fn read_root_bytes(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<u8>> {
    let len = root.file_len();
    let max_page = budget.limits().max_page_bytes.max(1);
    let mut bytes =
        Vec::with_capacity(usize::try_from(len).map_err(|_| sdk::Error::RecordTooLarge(len))?);
    let mut offset = 0_u64;
    while offset < len {
        let requested = u32::try_from((len - offset).min(u64::from(max_page)))
            .expect("request is bounded by max-page-bytes");
        bytes.extend(
            root.read_file(budget, offset, requested)
                .map_err(arena_error)?,
        );
        offset += u64::from(requested);
    }
    Ok(bytes)
}

fn read_entities_from_root(
    root: &sdk::Root,
    budget: &sdk::Budget,
) -> sdk::Result<Vec<CoreEntityRecord>> {
    let mut entities = Vec::new();
    let mut after_key = None;
    loop {
        let page = root
            .scan_entities(
                budget,
                after_key.as_deref(),
                budget.limits().max_page_bytes.max(1),
            )
            .map_err(arena_error)?;
        for (key, snapshot) in page.entries {
            let (schema_key, entity_pk) = sdk::decode_entity_key(&key)?;
            entities.push(CoreEntityRecord {
                schema_key,
                entity_pk,
                snapshot,
            });
        }
        let Some(next_key) = page.next_key else {
            break;
        };
        after_key = Some(next_key);
    }
    Ok(entities)
}

fn write_row_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    document: &CoreDocument,
) -> sdk::Result<()> {
    let mut pages = std::collections::BTreeMap::<u64, Vec<V3RowIndexRecord>>::new();
    for record in document.v3_row_index_records() {
        let first = u64::from(record.row_start) / INDEX_WINDOW_BYTES;
        let inclusive_end =
            u64::from(record.row_start).saturating_add(u64::from(record.row_len).saturating_sub(1));
        let last = inclusive_end / INDEX_WINDOW_BYTES;
        for page in first..=last {
            pages.entry(page).or_default().push(record.clone());
        }
    }
    for (page, records) in pages {
        transaction
            .put_state(budget, &row_index_key(page), &encode_row_index(&records)?)
            .map_err(arena_error)?;
    }
    transaction
        .put_state(budget, INDEX_VERSION_KEY, INDEX_VERSION)
        .map_err(arena_error)
}

fn sparse_row_file_changed(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
) -> sdk::Result<Option<Vec<sdk::EntityChange>>> {
    if input.edits.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if input.before_descriptor.path != input.after_descriptor.path {
        return Ok(None);
    }
    let deleted = input.edits.iter().try_fold(0_u64, |total, edit| {
        total
            .checked_add(edit.delete_len)
            .ok_or_else(|| sdk::Error::invalid_input("CSV edit size overflowed"))
    })?;
    let inserted = input.edits.iter().try_fold(0_u64, |total, edit| {
        let len = match &edit.insert {
            sdk::InputBytes::Inline(bytes) => bytes.len() as u64,
            sdk::InputBytes::AfterRange(range) => range.length,
        };
        total
            .checked_add(len)
            .ok_or_else(|| sdk::Error::invalid_input("CSV edit size overflowed"))
    })?;
    if deleted != inserted {
        return Ok(None);
    }
    if input
        .before
        .get_state(budget, INDEX_VERSION_KEY)
        .map_err(arena_error)?
        .as_deref()
        != Some(INDEX_VERSION)
    {
        return Ok(None);
    }
    let first_offset = input.edits[0].offset;
    let Some(index) = input
        .before
        .get_state(budget, &row_index_key(first_offset / INDEX_WINDOW_BYTES))
        .map_err(arena_error)?
    else {
        return Ok(None);
    };
    let Some(record) = decode_row_index(&index)?.into_iter().find(|record| {
        let start = u64::from(record.row_start);
        let end = start.saturating_add(u64::from(record.row_len));
        input.edits.iter().all(|edit| {
            edit.offset
                .checked_add(edit.delete_len)
                .is_some_and(|edit_end| edit.offset >= start && edit_end <= end)
        })
    }) else {
        return Ok(None);
    };
    let row_bytes = input
        .successor
        .read_file(budget, u64::from(record.row_start), record.row_len)
        .map_err(arena_error)?;
    let (_, changes) = CoreDocument::open_file(
        row_bytes,
        input.after_descriptor.path.as_deref(),
        namespace(input.creates),
    )
    .map_err(sdk::Error::invalid_input)?;
    let generated = changes
        .filter_map(Result::ok)
        .find(|change| change.schema_key == ROW_SCHEMA_KEY)
        .and_then(|change| change.snapshot)
        .ok_or_else(|| sdk::Error::invalid_input("edited CSV range is not exactly one row"))?;
    let key = sdk::entity_key(ROW_SCHEMA_KEY, std::slice::from_ref(&record.id))?;
    let Some(before_snapshot) = input.before.get_entity(budget, &key).map_err(arena_error)? else {
        return Ok(None);
    };
    let before = parse_row_snapshot(&before_snapshot).map_err(sdk::Error::invalid_input)?;
    if before.id != record.id {
        return Err(sdk::Error::internal("CSV row locator identity drifted"));
    }
    let mut after = parse_row_snapshot(&generated).map_err(sdk::Error::invalid_input)?;
    after.id.clone_from(&before.id);
    after.order_key.clone_from(&before.order_key);
    let format_only = before.cells == after.cells;
    let snapshot = encode_row_snapshot(&after).map_err(sdk::Error::invalid_input)?;
    if snapshot == before_snapshot {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(vec![sdk::EntityChange {
        schema_key: ROW_SCHEMA_KEY.to_owned(),
        entity_pk: vec![record.id],
        snapshot: Some(snapshot),
        format_only,
    }]))
}

fn row_index_key(page: u64) -> Vec<u8> {
    let mut key = INDEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(&page.to_be_bytes());
    key
}

fn encode_row_index(records: &[V3RowIndexRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    for record in records {
        let id = record.id.as_bytes();
        let id_len =
            u32::try_from(id.len()).map_err(|_| sdk::Error::RecordTooLarge(id.len() as u64))?;
        output.extend_from_slice(&record.row_start.to_le_bytes());
        output.extend_from_slice(&record.row_len.to_le_bytes());
        output.extend_from_slice(&id_len.to_le_bytes());
        output.extend_from_slice(id);
    }
    Ok(output)
}

fn decode_row_index(mut bytes: &[u8]) -> sdk::Result<Vec<V3RowIndexRecord>> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 12 {
            return Err(sdk::Error::internal("truncated CSV row index"));
        }
        let row_start = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let row_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let id_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        bytes = &bytes[12..];
        if bytes.len() < id_len {
            return Err(sdk::Error::internal("truncated CSV row index identity"));
        }
        let id = String::from_utf8(bytes[..id_len].to_vec())
            .map_err(|_| sdk::Error::internal("CSV row index identity is not UTF-8"))?;
        bytes = &bytes[id_len..];
        records.push(V3RowIndexRecord {
            row_start,
            row_len,
            id,
        });
    }
    Ok(records)
}

fn namespace(creates: sdk::CreateContext) -> IdNamespace {
    IdNamespace::from_halves(creates.high, creates.low)
}

fn core_change_to_sdk(change: CoreEntityChange) -> sdk::EntityChange {
    sdk::EntityChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot: change.snapshot,
        format_only: change.effect == CoreChangeEffect::FormatOnly,
    }
}

fn core_edit_to_sdk(edit: CoreByteEdit) -> sdk::ByteEdit {
    sdk::ByteEdit {
        offset: edit.offset,
        delete_len: edit.delete_len,
        insert: edit.insert.as_ref().clone(),
    }
}

fn arena_error(error: sdk::lix::plugin::arena::ArenaError) -> sdk::Error {
    use sdk::lix::plugin::arena::ArenaError;
    match error {
        ArenaError::InvalidRange => sdk::Error::invalid_input("invalid arena range"),
        ArenaError::RecordTooLarge(bytes) => sdk::Error::RecordTooLarge(bytes),
        ArenaError::LimitExceeded(message) => sdk::Error::LimitExceeded(message),
        ArenaError::DeadlineExceeded => sdk::Error::DeadlineExceeded,
        ArenaError::Unavailable(message) => sdk::Error::internal(message),
    }
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v3::export_v3!(CsvV3Plugin);
