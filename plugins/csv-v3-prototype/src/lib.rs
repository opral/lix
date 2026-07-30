//! Fused initial-import CSV experiment for Component API v3.
#![allow(dead_code)]

#[path = "../../csv-v2/src/core.rs"]
mod core;

use core::{
    ArenaRowIndex, ChangeEffect, ColdInitialImport, Document, EntityChange, IdNamespace,
    ROW_SCHEMA_KEY, RowConflictResolution, TABLE_SCHEMA_KEY, resolve_row_conflict,
};
use lix_plugin_api_v3_prototype as sdk;
use serde_json::Value;

struct CsvV3Prototype;

const CERTIFIED_CSV_PAGE_BYTES: usize = 256 * 1024;
const CSV_INDEX_KEY: &[u8] = b"csv/index-v1";
const ID_NAMESPACE_STATE: &[u8] = b"csv/id-namespace-v1";
const CSV_INDEX_HEADER_BYTES: u32 = 36;

impl sdk::FormatPlugin for CsvV3Prototype {
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
        let namespace = read_namespace(&update.before)?
            .or_else(|| namespace_from_changes(&changes))
            .unwrap_or_else(|| IdNamespace::from_halves(0, 0));
        let (document, _) = Document::open_file(
            before.clone(),
            update.before_file.path.as_deref(),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
        let (_, edits) = document
            .entities_changed(&changes)
            .map_err(sdk::Error::invalid_input)?;
        sink.replace_file(&apply_edits(before, &edits)?)?;
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
        input.successor.put_state(CSV_INDEX_KEY, &state)?;
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
        if update.before_file.path != update.after_file.path {
            return Err(sdk::Error::invalid_input(
                "arena CSV prototype does not yet support descriptor changes",
            ));
        }
        let [edit] = update.edits.as_slice() else {
            return Err(sdk::Error::invalid_input(
                "arena CSV prototype requires one sparse byte edit",
            ));
        };
        let insert = &edit.insert;
        if u64::try_from(insert.len()).expect("usize fits u64") != edit.delete_len {
            return Err(sdk::Error::invalid_input(
                "arena CSV prototype currently requires a length-preserving edit",
            ));
        }
        let (index, range) = if let Some(state_len) = update.before.state_len(CSV_INDEX_KEY)? {
            let header = update
                .before
                .read_state_range(CSV_INDEX_KEY, 0, CSV_INDEX_HEADER_BYTES)?
                .ok_or_else(|| sdk::Error::invalid_input("CSV arena root has no row index"))?;
            let index = ArenaRowIndex::decode_header(&header, state_len)
                .map_err(sdk::Error::invalid_input)?;
            let range = index
                .row_range_for_edit_reader(edit.offset, edit.delete_len, |ordinal| {
                    let offset = u64::from(CSV_INDEX_HEADER_BYTES)
                        .checked_add(u64::from(ordinal) * 4)
                        .ok_or_else(|| "CSV arena row-index offset overflowed".to_owned())?;
                    let bytes = update
                        .before
                        .read_state_range(CSV_INDEX_KEY, offset, 4)
                        .map_err(|error| format!("CSV arena row-index read failed: {error:?}"))?
                        .ok_or_else(|| "CSV arena row index disappeared".to_owned())?;
                    let bytes: [u8; 4] = bytes
                        .try_into()
                        .map_err(|_| "CSV arena row-index read was truncated".to_owned())?;
                    Ok(u32::from_le_bytes(bytes))
                })
                .map_err(sdk::Error::invalid_input)?;
            (index, range)
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
            update.successor.put_state(CSV_INDEX_KEY, &state)?;
            (index, range)
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
        row.splice(local_start..local_end, insert.iter().copied());
        let change = index
            .row_change(ordinal, row)
            .map_err(sdk::Error::invalid_input)?;
        let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
        encoder.push(change, update.creates, sink)?;
        encoder.flush(sink)?;
        Ok(())
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
lix_plugin_api_v3_prototype::export_v3_prototype!(CsvV3Prototype);
