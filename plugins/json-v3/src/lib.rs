//! First functional JSON port to Plugin API v3.
//!
//! This establishes exact schema/identity behavior on the real arena runtime.
//! The current fallback reconstructs the format index from durable entities;
//! the profile-driven follow-up replaces that fallback with affected state
//! pages for the warm scalar path.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;
use plugin_json_incremental_v2::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord, IdNamespace,
    InputSplice as CoreInputSplice, V3ScalarIndexRecord,
};

const INDEX_VERSION_KEY: &[u8] = b"json/v3/index-version";
const INDEX_VERSION: &[u8] = b"scalar-byte-windows-v1";
const INDEX_KEY_PREFIX: &[u8] = b"json/v3/scalars/";
const INDEX_WINDOW_BYTES: u64 = 64 * 1024;

struct JsonV3Plugin;

impl sdk::FormatPlugin for JsonV3Plugin {
    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let bytes = read_root_bytes(&input.accepted, budget)?;
        let namespace = namespace(input.creates);
        let (document, changes) =
            CoreDocument::open_file(bytes, input.descriptor.path.as_deref(), namespace)
                .map_err(sdk::Error::invalid_input)?;
        write_scalar_index(&input.successor, budget, &document)?;
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
        if let Some(changes) = sparse_scalar_file_changed(&input, budget)? {
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
            .file_changed(&splices, namespace(input.creates))
            .map_err(sdk::Error::invalid_input)?;
        write_scalar_index(&input.successor, budget, &after)?;
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
        write_scalar_index(&input.successor, budget, &document)?;
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
        write_scalar_index(&input.successor, budget, &after)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
        })
    }
}

fn document_from_root(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<CoreDocument> {
    let entities = read_entities_from_root(root, budget)?;
    CoreDocument::open_entities(entities)
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
            .expect("request is bounded by u32 max-page-bytes");
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
    let max_page = budget.limits().max_page_bytes.max(1);
    let mut entities = Vec::new();
    let mut after_key = None;
    loop {
        let page = root
            .scan_entities(budget, after_key.as_deref(), max_page)
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

fn write_scalar_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    document: &CoreDocument,
) -> sdk::Result<()> {
    let mut pages = std::collections::BTreeMap::<u64, Vec<V3ScalarIndexRecord>>::new();
    for record in document
        .v3_scalar_index_records()
        .map_err(sdk::Error::invalid_input)?
    {
        let first = u64::from(record.value_start) / INDEX_WINDOW_BYTES;
        let inclusive_end = u64::from(record.value_start)
            .saturating_add(u64::from(record.value_len).saturating_sub(1));
        let last = inclusive_end / INDEX_WINDOW_BYTES;
        for page in first..=last {
            pages.entry(page).or_default().push(record.clone());
        }
    }
    for (page, records) in pages {
        transaction
            .put_state(
                budget,
                &scalar_index_key(page),
                &encode_scalar_index(&records)?,
            )
            .map_err(arena_error)?;
    }
    transaction
        .put_state(budget, INDEX_VERSION_KEY, INDEX_VERSION)
        .map_err(arena_error)
}

fn sparse_scalar_file_changed(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
) -> sdk::Result<Option<Vec<sdk::EntityChange>>> {
    if input.edits.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let deleted = input.edits.iter().try_fold(0_u64, |total, edit| {
        total
            .checked_add(edit.delete_len)
            .ok_or_else(|| sdk::Error::invalid_input("JSON edit size overflowed"))
    })?;
    let inserted = input.edits.iter().try_fold(0_u64, |total, edit| {
        let len = match &edit.insert {
            sdk::InputBytes::Inline(bytes) => bytes.len() as u64,
            sdk::InputBytes::AfterRange(range) => range.length,
        };
        total
            .checked_add(len)
            .ok_or_else(|| sdk::Error::invalid_input("JSON edit size overflowed"))
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
        .get_state(budget, &scalar_index_key(first_offset / INDEX_WINDOW_BYTES))
        .map_err(arena_error)?
    else {
        return Ok(None);
    };
    let records = decode_scalar_index(&index)?;
    let Some(record) = records.into_iter().find(|record| {
        let start = u64::from(record.value_start);
        let end = start.saturating_add(u64::from(record.value_len));
        input.edits.iter().all(|edit| {
            edit.offset
                .checked_add(edit.delete_len)
                .is_some_and(|edit_end| edit.offset >= start && edit_end <= end)
        })
    }) else {
        return Ok(None);
    };
    let scalar = input
        .successor
        .read_file(budget, u64::from(record.value_start), record.value_len)
        .map_err(arena_error)?;
    let parsed: serde_json::Value =
        serde_json::from_slice(&scalar).map_err(|_| sdk::Error::invalid_input("invalid JSON"))?;
    let kind = match parsed {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => return Ok(None),
    };
    let key = sdk::entity_key(&record.schema_key, &record.entity_pk)?;
    let Some(snapshot) = input.before.get_entity(budget, &key).map_err(arena_error)? else {
        return Ok(None);
    };
    let mut snapshot: serde_json::Value = serde_json::from_slice(&snapshot)
        .map_err(|error| sdk::Error::internal(format!("invalid durable JSON entity: {error}")))?;
    let object = snapshot
        .as_object_mut()
        .ok_or_else(|| sdk::Error::internal("durable JSON entity is not an object"))?;
    object.insert(
        "kind".to_owned(),
        serde_json::Value::String(kind.to_owned()),
    );
    object.insert(
        "scalar_json".to_owned(),
        serde_json::Value::String(
            String::from_utf8(scalar).map_err(|error| {
                sdk::Error::invalid_input(format!("JSON is not UTF-8: {error}"))
            })?,
        ),
    );
    let snapshot = serde_json::to_vec(&snapshot)
        .map_err(|error| sdk::Error::internal(format!("failed to encode JSON entity: {error}")))?;
    Ok(Some(vec![sdk::EntityChange {
        schema_key: record.schema_key,
        entity_pk: record.entity_pk,
        snapshot: Some(snapshot),
        format_only: false,
    }]))
}

fn scalar_index_key(page: u64) -> Vec<u8> {
    let mut key = INDEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(&page.to_be_bytes());
    key
}

fn encode_scalar_index(records: &[V3ScalarIndexRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    for record in records {
        let key = sdk::entity_key(&record.schema_key, &record.entity_pk)?;
        let key_len =
            u32::try_from(key.len()).map_err(|_| sdk::Error::RecordTooLarge(key.len() as u64))?;
        output.extend_from_slice(&record.value_start.to_le_bytes());
        output.extend_from_slice(&record.value_len.to_le_bytes());
        output.extend_from_slice(&key_len.to_le_bytes());
        output.extend_from_slice(&key);
    }
    Ok(output)
}

fn decode_scalar_index(mut bytes: &[u8]) -> sdk::Result<Vec<V3ScalarIndexRecord>> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 12 {
            return Err(sdk::Error::internal("truncated JSON scalar index"));
        }
        let value_start = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let value_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let key_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        bytes = &bytes[12..];
        if bytes.len() < key_len {
            return Err(sdk::Error::internal("truncated JSON scalar index key"));
        }
        let (schema_key, entity_pk) = sdk::decode_entity_key(&bytes[..key_len])?;
        bytes = &bytes[key_len..];
        records.push(V3ScalarIndexRecord {
            value_start,
            value_len,
            schema_key,
            entity_pk,
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
lix_plugin_api_v3::export_v3!(JsonV3Plugin);
