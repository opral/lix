//! Production-schema Excalidraw port to Component v3 host arenas.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;
use plugin_excalidraw_core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord, IdNamespace,
    InputSplice as CoreInputSplice, V3ObjectIndexRecord, v3_reparse_object_snapshot,
};

const INDEX_VERSION_KEY: &[u8] = b"excalidraw/v3/index-version";
const INDEX_VERSION: &[u8] = b"object-byte-windows-v1";
const INDEX_KEY_PREFIX: &[u8] = b"excalidraw/v3/objects/";
const SHIFT_OVERLAY_KEY: &[u8] = b"excalidraw/v3/shift-overlay-v1";
const INDEX_WINDOW_BYTES: u64 = 4 * 1024;

struct ExcalidrawV3Plugin;

#[derive(Clone, Debug)]
struct ShiftRecord {
    at_base: u64,
    delta: i64,
    schema_key: String,
    id: String,
    new_len: u64,
}

impl sdk::FormatPlugin for ExcalidrawV3Plugin {
    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let bytes = read_root_bytes(&input.accepted, budget)?;
        let (document, changes) = CoreDocument::open_file(
            bytes,
            input.descriptor.path.as_deref(),
            namespace(input.creates),
        )
        .map_err(sdk::Error::invalid_input)?;
        write_object_index(&input.successor, budget, document.v3_object_index_records())?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: sdk::EntityChanges::from_results(changes.map(|change| {
                change
                    .map(core_change_to_sdk)
                    .map_err(sdk::Error::invalid_input)
            })),
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        if let Some(changes) = sparse_object_file_changed(&input, budget)? {
            return Ok(sdk::FileResult {
                successor: input.successor,
                changes: changes.into(),
            });
        }
        let document = document_from_root(&input.before, budget)?;
        let inserts = resolve_inserts(&input, budget)?;
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
        write_object_index(&input.successor, budget, after.v3_object_index_records())?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: changes
                .into_iter()
                .map(core_change_to_sdk)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    fn open_entities(
        budget: &sdk::Budget,
        input: sdk::OpenEntitiesInput,
    ) -> sdk::Result<sdk::EntityResult> {
        let entities = read_entities(&input.durable, budget)?;
        let accepted = read_root_bytes(&input.durable, budget)?;
        let document = CoreDocument::open_entities_with_accepted(entities, accepted)
            .map_err(sdk::Error::invalid_input)?;
        write_object_index(&input.successor, budget, document.v3_object_index_records())?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: Vec::new(),
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
        write_object_index(&input.successor, budget, after.v3_object_index_records())?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
        })
    }
}

fn sparse_object_file_changed(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
) -> sdk::Result<Option<Vec<sdk::EntityChange>>> {
    let [edit] = input.edits.as_slice() else {
        return Ok(None);
    };
    let insert = resolve_insert(&input.successor, edit, budget)?;
    let shifts = read_shifts(&input.before, budget)?;
    let base_offset = inverse_offset(edit.offset, &shifts)?;
    let base_page = base_offset / INDEX_WINDOW_BYTES;
    let mut candidates = Vec::new();
    for page in base_page.saturating_sub(1)..=base_page.saturating_add(1) {
        if let Some(bytes) = input
            .before
            .get_state(budget, &index_key(page))
            .map_err(arena_error)?
        {
            candidates.extend(decode_index(&bytes)?);
        }
    }
    candidates.sort_by(|a, b| (&a.schema_key, &a.id).cmp(&(&b.schema_key, &b.id)));
    candidates.dedup_by(|a, b| a.schema_key == b.schema_key && a.id == b.id);
    let edit_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw edit overflowed"))?;
    let matches = candidates
        .into_iter()
        .map(|base| {
            let current = current_record(&base, &shifts)?;
            Ok((base, current))
        })
        .collect::<sdk::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, record)| {
            record.offset <= edit.offset && edit_end <= record.offset.saturating_add(record.len)
        })
        .collect::<Vec<_>>();
    let [(base, record)] = matches.as_slice() else {
        return Ok(None);
    };
    let key = sdk::entity_key(&record.schema_key, std::slice::from_ref(&record.id))?;
    let Some(before_snapshot) = input.before.get_entity(budget, &key).map_err(arena_error)? else {
        return Ok(None);
    };
    let successor_len = record
        .len
        .checked_sub(edit.delete_len)
        .and_then(|len| len.checked_add(insert.len() as u64))
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw object length overflowed"))?;
    let bytes = input
        .successor
        .read_file(
            budget,
            record.offset,
            u32::try_from(successor_len).map_err(|_| sdk::Error::RecordTooLarge(successor_len))?,
        )
        .map_err(arena_error)?;
    let snapshot =
        v3_reparse_object_snapshot(&record.schema_key, &record.id, &before_snapshot, &bytes)
            .map_err(sdk::Error::invalid_input)?;
    if successor_len != record.len {
        let mut successor_shifts = shifts;
        successor_shifts.push(ShiftRecord {
            at_base: base.offset.saturating_add(base.len),
            delta: i64::try_from(i128::from(successor_len) - i128::from(record.len))
                .map_err(|_| sdk::Error::invalid_input("Excalidraw shift exceeds i64"))?,
            schema_key: record.schema_key.clone(),
            id: record.id.clone(),
            new_len: successor_len,
        });
        input
            .successor
            .put_state(
                budget,
                SHIFT_OVERLAY_KEY,
                &encode_shifts(&successor_shifts)?,
            )
            .map_err(arena_error)?;
    }
    if snapshot == before_snapshot {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(vec![sdk::EntityChange {
        schema_key: record.schema_key.clone(),
        entity_pk: vec![record.id.clone()],
        snapshot: Some(snapshot),
        format_only: false,
    }]))
}

fn document_from_root(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<CoreDocument> {
    CoreDocument::open_entities_with_accepted(
        read_entities(root, budget)?,
        read_root_bytes(root, budget)?,
    )
    .map_err(sdk::Error::invalid_input)
}

fn read_root_bytes(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<u8>> {
    let len = root.file_len();
    let mut output =
        Vec::with_capacity(usize::try_from(len).map_err(|_| sdk::Error::RecordTooLarge(len))?);
    let mut offset = 0;
    while offset < len {
        let requested =
            u32::try_from((len - offset).min(u64::from(budget.limits().max_page_bytes.max(1))))
                .expect("bounded Excalidraw read");
        output.extend(
            root.read_file(budget, offset, requested)
                .map_err(arena_error)?,
        );
        offset += u64::from(requested);
    }
    Ok(output)
}

fn read_entities(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<CoreEntityRecord>> {
    let mut output = Vec::new();
    let mut after = None;
    loop {
        let page = root
            .scan_entities(
                budget,
                after.as_deref(),
                budget.limits().max_page_bytes.max(1),
            )
            .map_err(arena_error)?;
        for (key, snapshot) in page.entries {
            let (schema_key, entity_pk) = sdk::decode_entity_key(&key)?;
            output.push(CoreEntityRecord {
                schema_key,
                entity_pk,
                snapshot,
            });
        }
        let Some(next) = page.next_key else {
            break;
        };
        after = Some(next);
    }
    Ok(output)
}

fn resolve_inserts(input: &sdk::FileUpdate, budget: &sdk::Budget) -> sdk::Result<Vec<Vec<u8>>> {
    input
        .edits
        .iter()
        .map(|edit| resolve_insert(&input.successor, edit, budget))
        .collect()
}

fn resolve_insert(
    successor: &sdk::Transaction,
    edit: &sdk::InputSplice,
    budget: &sdk::Budget,
) -> sdk::Result<Vec<u8>> {
    match &edit.insert {
        sdk::InputBytes::Inline(bytes) => Ok(bytes.clone()),
        sdk::InputBytes::AfterRange(range) => successor
            .read_file(
                budget,
                range.offset,
                u32::try_from(range.length)
                    .map_err(|_| sdk::Error::RecordTooLarge(range.length))?,
            )
            .map_err(arena_error),
    }
}

fn write_object_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    records: Vec<V3ObjectIndexRecord>,
) -> sdk::Result<()> {
    let mut pages = std::collections::BTreeMap::<u64, Vec<V3ObjectIndexRecord>>::new();
    for record in records {
        let first = record.offset / INDEX_WINDOW_BYTES;
        let last = record.offset.saturating_add(record.len.saturating_sub(1)) / INDEX_WINDOW_BYTES;
        for page in first..=last {
            pages.entry(page).or_default().push(record.clone());
        }
    }
    for (page, records) in pages {
        transaction
            .put_state(budget, &index_key(page), &encode_index(&records)?)
            .map_err(arena_error)?;
    }
    transaction
        .put_state(budget, INDEX_VERSION_KEY, INDEX_VERSION)
        .map_err(arena_error)
}

fn encode_index(records: &[V3ObjectIndexRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    put_len(&mut output, records.len())?;
    for record in records {
        put_bytes(&mut output, record.schema_key.as_bytes())?;
        put_bytes(&mut output, record.id.as_bytes())?;
        output.extend_from_slice(&record.offset.to_le_bytes());
        output.extend_from_slice(&record.len.to_le_bytes());
    }
    Ok(output)
}

fn decode_index(bytes: &[u8]) -> sdk::Result<Vec<V3ObjectIndexRecord>> {
    let mut offset = 0;
    let count = take_u32(bytes, &mut offset)? as usize;
    let mut output = Vec::with_capacity(count);
    for _ in 0..count {
        let schema_key = take_string(bytes, &mut offset)?;
        let id = take_string(bytes, &mut offset)?;
        let record_offset = take_u64(bytes, &mut offset)?;
        let len = take_u64(bytes, &mut offset)?;
        output.push(V3ObjectIndexRecord {
            schema_key,
            id,
            offset: record_offset,
            len,
        });
    }
    exact_end(bytes, offset)?;
    Ok(output)
}

fn read_shifts(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<ShiftRecord>> {
    root.get_state(budget, SHIFT_OVERLAY_KEY)
        .map_err(arena_error)?
        .map_or_else(|| Ok(Vec::new()), |bytes| decode_shifts(&bytes))
}

fn encode_shifts(shifts: &[ShiftRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    put_len(&mut output, shifts.len())?;
    for shift in shifts {
        output.extend_from_slice(&shift.at_base.to_le_bytes());
        output.extend_from_slice(&shift.delta.to_le_bytes());
        put_bytes(&mut output, shift.schema_key.as_bytes())?;
        put_bytes(&mut output, shift.id.as_bytes())?;
        output.extend_from_slice(&shift.new_len.to_le_bytes());
    }
    Ok(output)
}

fn decode_shifts(bytes: &[u8]) -> sdk::Result<Vec<ShiftRecord>> {
    let mut offset = 0;
    let count = take_u32(bytes, &mut offset)? as usize;
    let mut output = Vec::with_capacity(count);
    for _ in 0..count {
        output.push(ShiftRecord {
            at_base: take_u64(bytes, &mut offset)?,
            delta: take_i64(bytes, &mut offset)?,
            schema_key: take_string(bytes, &mut offset)?,
            id: take_string(bytes, &mut offset)?,
            new_len: take_u64(bytes, &mut offset)?,
        });
    }
    exact_end(bytes, offset)?;
    Ok(output)
}

fn inverse_offset(current: u64, shifts: &[ShiftRecord]) -> sdk::Result<u64> {
    let mut ordered = shifts.to_vec();
    ordered.sort_by_key(|shift| shift.at_base);
    let mut cumulative = 0_i128;
    for shift in ordered {
        if i128::from(current) >= i128::from(shift.at_base) + cumulative + i128::from(shift.delta) {
            cumulative += i128::from(shift.delta);
        }
    }
    u64::try_from(i128::from(current) - cumulative)
        .map_err(|_| sdk::Error::invalid_input("Excalidraw inverse shift overflowed"))
}

fn current_record(
    base: &V3ObjectIndexRecord,
    shifts: &[ShiftRecord],
) -> sdk::Result<V3ObjectIndexRecord> {
    let delta: i128 = shifts
        .iter()
        .filter(|shift| shift.at_base <= base.offset)
        .map(|shift| i128::from(shift.delta))
        .sum();
    Ok(V3ObjectIndexRecord {
        schema_key: base.schema_key.clone(),
        id: base.id.clone(),
        offset: u64::try_from(i128::from(base.offset) + delta)
            .map_err(|_| sdk::Error::invalid_input("Excalidraw shifted offset overflowed"))?,
        len: shifts
            .iter()
            .rev()
            .find(|shift| shift.schema_key == base.schema_key && shift.id == base.id)
            .map_or(base.len, |shift| shift.new_len),
    })
}

fn index_key(page: u64) -> Vec<u8> {
    let mut key = INDEX_KEY_PREFIX.to_vec();
    key.extend_from_slice(&page.to_be_bytes());
    key
}

fn put_len(output: &mut Vec<u8>, len: usize) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(len)
            .map_err(|_| sdk::Error::RecordTooLarge(len as u64))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> sdk::Result<()> {
    put_len(output, bytes.len())?;
    output.extend_from_slice(bytes);
    Ok(())
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> sdk::Result<u32> {
    Ok(u32::from_le_bytes(take_fixed(bytes, offset)?))
}

fn take_u64(bytes: &[u8], offset: &mut usize) -> sdk::Result<u64> {
    Ok(u64::from_le_bytes(take_fixed(bytes, offset)?))
}

fn take_i64(bytes: &[u8], offset: &mut usize) -> sdk::Result<i64> {
    Ok(i64::from_le_bytes(take_fixed(bytes, offset)?))
}

fn take_fixed<const N: usize>(bytes: &[u8], offset: &mut usize) -> sdk::Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw index overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| sdk::Error::invalid_input("truncated Excalidraw index"))?;
    *offset = end;
    Ok(value.try_into().expect("fixed slice length matches"))
}

fn take_string(bytes: &[u8], offset: &mut usize) -> sdk::Result<String> {
    let len = take_u32(bytes, offset)? as usize;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| sdk::Error::invalid_input("Excalidraw string overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| sdk::Error::invalid_input("truncated Excalidraw string"))?;
    *offset = end;
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| sdk::Error::invalid_input("Excalidraw index string is not UTF-8"))
}

fn exact_end(bytes: &[u8], offset: usize) -> sdk::Result<()> {
    if offset == bytes.len() {
        Ok(())
    } else {
        Err(sdk::Error::invalid_input(
            "Excalidraw index has trailing bytes",
        ))
    }
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
lix_plugin_api_v3::export_v3!(ExcalidrawV3Plugin);
