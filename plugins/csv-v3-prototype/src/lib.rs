//! Fused initial-import CSV experiment for Component API v3.

#[path = "../../csv-v2/src/core.rs"]
mod core;

use core::{
    ChangeEffect, ColdInitialImport, Document, EntityChange, IdNamespace, ROW_SCHEMA_KEY,
    TABLE_SCHEMA_KEY,
};
use lix_plugin_api_v3_prototype as sdk;
use serde_json::Value;

struct CsvV3Prototype;

impl sdk::FormatPlugin for CsvV3Prototype {
    type Document = Document;

    fn open_file(
        input: sdk::OpenFile<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<Self::Document> {
        const MAX_RETAINED_IMPORT_BYTES: u64 = 8 * 1024 * 1024;
        let source_len = input.source.len();
        let bytes = input.source.read_all()?;
        let namespace = IdNamespace::from_halves(input.creates.high, u64::from(input.creates.low));
        if source_len > MAX_RETAINED_IMPORT_BYTES {
            let mut import = ColdInitialImport::open(bytes, input.file.path.as_deref())
                .map_err(sdk::Error::invalid_input)?;
            let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
            encoder.push(import.table_change(), input.creates, sink)?;
            encoder.flush(sink)?;
            while let Some((payload, row_count)) = import
                .next_typed_batch(sink.max_batch_bytes() as usize)
                .map_err(sdk::Error::invalid_input)?
            {
                sink.emit_csv_rows(row_count, payload)?;
            }
            drop(import);
            return Document::open_file(Vec::new(), input.file.path.as_deref(), namespace)
                .map(|(document, _)| document)
                .map_err(sdk::Error::internal);
        }
        let (document, mut changes) =
            Document::open_file(bytes, input.file.path.as_deref(), namespace)
                .map_err(sdk::Error::invalid_input)?;
        let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
        let table = changes
            .next()
            .ok_or_else(|| sdk::Error::internal("CSV import omitted its table entity"))?
            .map_err(sdk::Error::invalid_input)?;
        encoder.push(table, input.creates, sink)?;
        encoder.flush(sink)?;
        drop(changes);

        let mut next_row = 0usize;
        while let Some((payload, row_count, after)) = document
            .initial_typed_csv_batch(next_row, sink.max_batch_bytes() as usize)
            .map_err(sdk::Error::invalid_input)?
        {
            sink.emit_csv_rows(row_count, payload)?;
            next_row = after;
        }
        Ok(document)
    }

    fn file_changed(
        document: &Self::Document,
        update: sdk::FileUpdate<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<Self::Document> {
        let inserts = update
            .edits
            .iter()
            .map(|edit| match &edit.insert {
                sdk::SpliceInsert::Inline(bytes) => Ok(bytes.clone()),
                sdk::SpliceInsert::AfterRange { offset, length } => {
                    update.after.read_range(*offset, *length)
                }
            })
            .collect::<sdk::Result<Vec<_>>>()?;
        let splices = update
            .edits
            .iter()
            .zip(&inserts)
            .map(|(edit, insert)| core::InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let namespace =
            IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
        let (document, changes) = document
            .file_changed_with_paths(
                &splices,
                update.before_file.path.as_deref(),
                update.after_file.path.as_deref(),
                namespace,
            )
            .map_err(sdk::Error::invalid_input)?;
        let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
        for change in changes {
            encoder.push(change, update.creates, sink)?;
        }
        encoder.flush(sink)?;
        Ok(document)
    }
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
