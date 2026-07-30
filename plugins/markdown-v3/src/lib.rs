//! Production-schema Markdown port to the host-owned Component v3 arenas.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;
use plugin_markdown_core::{
    ByteEdit as CoreByteEdit, ChangeEffect as CoreChangeEffect, Document as CoreDocument,
    EntityChange as CoreEntityChange, EntityRecord as CoreEntityRecord, IdNamespace,
    InputSplice as CoreInputSplice, NODE_SCHEMA_KEY, PluginError as CoreError,
    V3TopLevelIndexRecord, v3_reidentify_snapshot, v3_single_top_level_snapshot,
};

const INDEX_VERSION_KEY: &[u8] = b"markdown/v3/index-version";
const INDEX_VERSION: &[u8] = b"top-level-byte-windows-v2";
const INDEX_KEY_PREFIX: &[u8] = b"markdown/v3/top-level/";
const SHIFT_OVERLAY_KEY: &[u8] = b"markdown/v3/shift-overlay-v1";
const INDEX_WINDOW_BYTES: u64 = 1024;

struct MarkdownV3Plugin;

#[derive(Clone, Debug)]
struct ShiftRecord {
    at_base: u64,
    delta: i64,
    affected_id: String,
    new_len: u64,
}

impl sdk::FormatPlugin for MarkdownV3Plugin {
    fn resolve_conflict(conflict: sdk::EntityConflict<'_>) -> sdk::Result<sdk::ConflictResolution> {
        const MAX_HEURISTIC_SNAPSHOT_BYTES: u64 = 64 * 1024;

        let Some(b) = conflict.b.as_ref() else {
            return Ok(sdk::ConflictResolution::Delete);
        };
        if conflict.schema_key != NODE_SCHEMA_KEY {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let (Some(base), Some(a)) = (&conflict.base, &conflict.a) else {
            return Ok(sdk::ConflictResolution::TakeB);
        };
        if [base, a, b]
            .into_iter()
            .any(|snapshot| snapshot.len() > MAX_HEURISTIC_SNAPSHOT_BYTES)
        {
            return Ok(sdk::ConflictResolution::TakeB);
        }
        let base = base.read()?;
        let a = a.read()?;
        let b = b.read()?;
        let resolved = CoreDocument::resolve_entity_conflict(
            Some(base.clone()),
            Some(a.clone()),
            Some(b.clone()),
        );
        Ok(match resolved {
            None => sdk::ConflictResolution::Delete,
            Some(resolved) if resolved == b => sdk::ConflictResolution::TakeB,
            Some(resolved) if resolved == a => sdk::ConflictResolution::TakeA,
            Some(resolved) if resolved == base => sdk::ConflictResolution::TakeBase,
            Some(resolved) => sdk::ConflictResolution::Replace(resolved),
        })
    }

    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let bytes = read_root_bytes(&input.accepted, budget)?;
        let (document, changes) = CoreDocument::open_file_host_owned(
            bytes,
            input.descriptor.path.as_deref(),
            namespace(input.creates),
        )
        .map_err(core_error)?;
        write_top_level_index(&input.successor, budget, &document)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: changes
                .into_iter()
                .map(core_change_to_sdk)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        if let Some(changes) = sparse_top_level_file_changed(&input, budget)? {
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
            .map_err(core_error)?;
        write_top_level_index(&input.successor, budget, &after)?;
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
        let entities = read_entities_from_root(&input.durable, budget)?;
        let accepted = read_root_bytes(&input.durable, budget)?;
        let (document, edits) =
            CoreDocument::open_entities(entities, Some(accepted)).map_err(core_error)?;
        write_top_level_index(&input.successor, budget, &document)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
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
        let (after, edits) = document.entities_changed(changes).map_err(core_error)?;
        write_top_level_index(&input.successor, budget, &after)?;
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: edits.into_iter().map(core_edit_to_sdk).collect(),
        })
    }
}

fn sparse_top_level_file_changed(
    input: &sdk::FileUpdate,
    budget: &sdk::Budget,
) -> sdk::Result<Option<Vec<sdk::EntityChange>>> {
    let [edit] = input.edits.as_slice() else {
        return Ok(None);
    };
    let insert = match &edit.insert {
        sdk::InputBytes::Inline(bytes) => bytes.clone(),
        sdk::InputBytes::AfterRange(range) => input
            .successor
            .read_file(
                budget,
                range.offset,
                u32::try_from(range.length)
                    .map_err(|_| sdk::Error::RecordTooLarge(range.length))?,
            )
            .map_err(arena_error)?,
    };
    let shifts = read_shift_overlay(&input.before, budget)?;
    let base_offset = current_offset_to_base(edit.offset, &shifts)?;
    let base_page = base_offset / INDEX_WINDOW_BYTES;
    let mut candidates = Vec::new();
    for page in base_page.saturating_sub(1)..=base_page.saturating_add(1) {
        if let Some(encoded) = input
            .before
            .get_state(budget, &top_level_index_key(page))
            .map_err(arena_error)?
        {
            candidates.extend(decode_top_level_index(&encoded)?);
        }
    }
    candidates.sort_by(|left, right| left.id.cmp(&right.id));
    candidates.dedup_by(|left, right| left.id == right.id);
    let edit_end = edit
        .offset
        .checked_add(edit.delete_len)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown splice range overflowed"))?;
    let matches = candidates
        .into_iter()
        .map(|base| {
            let current = current_top_level_record(&base, &shifts)?;
            Ok((base, current))
        })
        .collect::<sdk::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, record)| {
            let end = record.start.saturating_add(record.len);
            record.start <= edit.offset && edit_end <= end
        })
        .collect::<Vec<_>>();
    let [(base_record, record)] = matches.as_slice() else {
        return Ok(None);
    };
    let key = sdk::entity_key(NODE_SCHEMA_KEY, std::slice::from_ref(&record.id))?;
    let Some(before_snapshot) = input.before.get_entity(budget, &key).map_err(arena_error)? else {
        return Ok(None);
    };
    let successor_len = record
        .len
        .checked_sub(edit.delete_len)
        .and_then(|len| len.checked_add(insert.len() as u64))
        .ok_or_else(|| sdk::Error::invalid_input("Markdown successor range overflowed"))?;
    let fragment_len =
        u32::try_from(successor_len).map_err(|_| sdk::Error::RecordTooLarge(successor_len))?;
    let fragment = input
        .successor
        .read_file(budget, record.start, fragment_len)
        .map_err(arena_error)?;
    let generated = v3_single_top_level_snapshot(
        fragment,
        input.after_descriptor.path.as_deref(),
        namespace(input.creates),
    )
    .map_err(core_error)?;
    let (snapshot, effect) =
        v3_reidentify_snapshot(&before_snapshot, &generated).map_err(core_error)?;
    if successor_len != record.len {
        let mut successor_shifts = shifts;
        successor_shifts.push(ShiftRecord {
            at_base: base_record.start.saturating_add(base_record.len),
            delta: i64::try_from(i128::from(successor_len) - i128::from(record.len))
                .map_err(|_| sdk::Error::invalid_input("Markdown shift exceeds i64"))?,
            affected_id: record.id.clone(),
            new_len: successor_len,
        });
        input
            .successor
            .put_state(
                budget,
                SHIFT_OVERLAY_KEY,
                &encode_shift_overlay(&successor_shifts)?,
            )
            .map_err(arena_error)?;
    }
    if snapshot == before_snapshot {
        return Ok(Some(Vec::new()));
    }
    Ok(Some(vec![sdk::EntityChange {
        schema_key: NODE_SCHEMA_KEY.to_owned(),
        entity_pk: vec![record.id.clone()],
        snapshot: Some(snapshot),
        format_only: effect == CoreChangeEffect::FormatOnly,
    }]))
}

fn read_shift_overlay(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<ShiftRecord>> {
    root.get_state(budget, SHIFT_OVERLAY_KEY)
        .map_err(arena_error)?
        .map_or_else(|| Ok(Vec::new()), |bytes| decode_shift_overlay(&bytes))
}

fn current_offset_to_base(current: u64, shifts: &[ShiftRecord]) -> sdk::Result<u64> {
    let mut ordered = shifts.to_vec();
    ordered.sort_by_key(|shift| shift.at_base);
    let mut cumulative = 0_i128;
    for shift in ordered {
        let after_boundary = i128::from(shift.at_base) + cumulative + i128::from(shift.delta);
        if i128::from(current) >= after_boundary {
            cumulative += i128::from(shift.delta);
        }
    }
    u64::try_from(i128::from(current) - cumulative)
        .map_err(|_| sdk::Error::invalid_input("Markdown inverse shift overflowed"))
}

fn current_top_level_record(
    base: &V3TopLevelIndexRecord,
    shifts: &[ShiftRecord],
) -> sdk::Result<V3TopLevelIndexRecord> {
    let delta = shifts
        .iter()
        .filter(|shift| shift.at_base <= base.start)
        .try_fold(0_i128, |total, shift| {
            total
                .checked_add(i128::from(shift.delta))
                .ok_or_else(|| sdk::Error::invalid_input("Markdown shift sum overflowed"))
        })?;
    let start = u64::try_from(i128::from(base.start) + delta)
        .map_err(|_| sdk::Error::invalid_input("Markdown shifted start overflowed"))?;
    let len = shifts
        .iter()
        .rev()
        .find(|shift| shift.affected_id == base.id)
        .map_or(base.len, |shift| shift.new_len);
    Ok(V3TopLevelIndexRecord {
        start,
        len,
        id: base.id.clone(),
    })
}

fn encode_shift_overlay(shifts: &[ShiftRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(
        &u32::try_from(shifts.len())
            .map_err(|_| sdk::Error::RecordTooLarge(u64::MAX))?
            .to_le_bytes(),
    );
    for shift in shifts {
        output.extend_from_slice(&shift.at_base.to_le_bytes());
        output.extend_from_slice(&shift.delta.to_le_bytes());
        output.extend_from_slice(&shift.new_len.to_le_bytes());
        let id = shift.affected_id.as_bytes();
        output.extend_from_slice(
            &u32::try_from(id.len())
                .map_err(|_| sdk::Error::RecordTooLarge(id.len() as u64))?
                .to_le_bytes(),
        );
        output.extend_from_slice(id);
    }
    Ok(output)
}

fn decode_shift_overlay(bytes: &[u8]) -> sdk::Result<Vec<ShiftRecord>> {
    let mut offset = 0usize;
    let count = take_u32(bytes, &mut offset)? as usize;
    let mut shifts = Vec::with_capacity(count);
    for _ in 0..count {
        let at_base = take_u64(bytes, &mut offset)?;
        let delta = take_i64(bytes, &mut offset)?;
        let new_len = take_u64(bytes, &mut offset)?;
        let id_len = take_u32(bytes, &mut offset)? as usize;
        let end = offset
            .checked_add(id_len)
            .ok_or_else(|| sdk::Error::invalid_input("Markdown shift ID overflowed"))?;
        let affected_id = std::str::from_utf8(
            bytes
                .get(offset..end)
                .ok_or_else(|| sdk::Error::invalid_input("truncated Markdown shift ID"))?,
        )
        .map_err(|_| sdk::Error::invalid_input("Markdown shift ID is not UTF-8"))?
        .to_owned();
        offset = end;
        shifts.push(ShiftRecord {
            at_base,
            delta,
            affected_id,
            new_len,
        });
    }
    if offset != bytes.len() {
        return Err(sdk::Error::invalid_input(
            "Markdown shift overlay has trailing bytes",
        ));
    }
    Ok(shifts)
}

fn document_from_root(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<CoreDocument> {
    let entities = read_entities_from_root(root, budget)?;
    let accepted = read_root_bytes(root, budget)?;
    CoreDocument::open_entities(entities, Some(accepted))
        .map(|(document, _)| document)
        .map_err(core_error)
}

fn resolve_inserts(input: &sdk::FileUpdate, budget: &sdk::Budget) -> sdk::Result<Vec<Vec<u8>>> {
    input
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
        .collect()
}

fn read_root_bytes(root: &sdk::Root, budget: &sdk::Budget) -> sdk::Result<Vec<u8>> {
    let len = root.file_len();
    let max_page = budget.limits().max_page_bytes.max(1);
    let mut output =
        Vec::with_capacity(usize::try_from(len).map_err(|_| sdk::Error::RecordTooLarge(len))?);
    let mut offset = 0_u64;
    while offset < len {
        let requested = u32::try_from((len - offset).min(u64::from(max_page)))
            .expect("Markdown request is bounded by max-page-bytes");
        output.extend(
            root.read_file(budget, offset, requested)
                .map_err(arena_error)?,
        );
        offset += u64::from(requested);
    }
    Ok(output)
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

fn write_top_level_index(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    document: &CoreDocument,
) -> sdk::Result<()> {
    write_top_level_records(
        transaction,
        budget,
        document.v3_top_level_index_records().into_iter(),
    )
}

fn write_top_level_records(
    transaction: &sdk::Transaction,
    budget: &sdk::Budget,
    records: impl Iterator<Item = V3TopLevelIndexRecord>,
) -> sdk::Result<()> {
    let mut pages = std::collections::BTreeMap::<u64, Vec<V3TopLevelIndexRecord>>::new();
    for record in records {
        let first = record.start / INDEX_WINDOW_BYTES;
        let inclusive_end = record.start.saturating_add(record.len.saturating_sub(1));
        let last = inclusive_end / INDEX_WINDOW_BYTES;
        for page in first..=last {
            pages.entry(page).or_default().push(record.clone());
        }
    }
    for (page, records) in pages {
        transaction
            .put_state(
                budget,
                &top_level_index_key(page),
                &encode_top_level_index(&records)?,
            )
            .map_err(arena_error)?;
    }
    transaction
        .put_state(budget, INDEX_VERSION_KEY, INDEX_VERSION)
        .map_err(arena_error)
}

fn encode_top_level_index(records: &[V3TopLevelIndexRecord]) -> sdk::Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(
        &u32::try_from(records.len())
            .map_err(|_| sdk::Error::RecordTooLarge(u64::MAX))?
            .to_le_bytes(),
    );
    for record in records {
        output.extend_from_slice(&record.start.to_le_bytes());
        output.extend_from_slice(&record.len.to_le_bytes());
        let id = record.id.as_bytes();
        output.extend_from_slice(
            &u32::try_from(id.len())
                .map_err(|_| sdk::Error::RecordTooLarge(id.len() as u64))?
                .to_le_bytes(),
        );
        output.extend_from_slice(id);
    }
    Ok(output)
}

fn decode_top_level_index(bytes: &[u8]) -> sdk::Result<Vec<V3TopLevelIndexRecord>> {
    let mut offset = 0usize;
    let count = take_u32(bytes, &mut offset)? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let start = take_u64(bytes, &mut offset)?;
        let len = take_u64(bytes, &mut offset)?;
        let id_len = take_u32(bytes, &mut offset)? as usize;
        let end = offset
            .checked_add(id_len)
            .ok_or_else(|| sdk::Error::invalid_input("Markdown index length overflowed"))?;
        let id = std::str::from_utf8(
            bytes
                .get(offset..end)
                .ok_or_else(|| sdk::Error::invalid_input("truncated Markdown index ID"))?,
        )
        .map_err(|_| sdk::Error::invalid_input("Markdown index ID is not UTF-8"))?
        .to_owned();
        offset = end;
        records.push(V3TopLevelIndexRecord { start, len, id });
    }
    if offset != bytes.len() {
        return Err(sdk::Error::invalid_input(
            "Markdown index has trailing bytes",
        ));
    }
    Ok(records)
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> sdk::Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown index offset overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| sdk::Error::invalid_input("truncated Markdown index"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        value.try_into().expect("u32 slice has four bytes"),
    ))
}

fn take_u64(bytes: &[u8], offset: &mut usize) -> sdk::Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown index offset overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| sdk::Error::invalid_input("truncated Markdown index"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        value.try_into().expect("u64 slice has eight bytes"),
    ))
}

fn take_i64(bytes: &[u8], offset: &mut usize) -> sdk::Result<i64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| sdk::Error::invalid_input("Markdown index offset overflowed"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| sdk::Error::invalid_input("truncated Markdown index"))?;
    *offset = end;
    Ok(i64::from_le_bytes(
        value.try_into().expect("i64 slice has eight bytes"),
    ))
}

fn top_level_index_key(page: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(INDEX_KEY_PREFIX.len() + 8);
    key.extend_from_slice(INDEX_KEY_PREFIX);
    key.extend_from_slice(&page.to_be_bytes());
    key
}

fn namespace(creates: sdk::CreateContext) -> IdNamespace {
    IdNamespace::from_halves(
        creates.high,
        (creates.low as u32) ^ ((creates.low >> 32) as u32),
    )
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

fn core_error(error: CoreError) -> sdk::Error {
    match error {
        CoreError::InvalidInput(message) => sdk::Error::invalid_input(message),
        CoreError::Internal(message) => sdk::Error::internal(message),
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
lix_plugin_api_v3::export_v3!(MarkdownV3Plugin);
