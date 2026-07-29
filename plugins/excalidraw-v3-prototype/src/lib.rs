//! Fused Excalidraw experiment for Component API v3.
#![allow(dead_code)]

#[path = "../../excalidraw-v2/src/core.rs"]
mod core;

use core::{ChangeEffect, Document, EntityChange, IdNamespace, InputSplice};
use lix_plugin_api_v3_prototype as sdk;

struct ExcalidrawV3Prototype;

impl sdk::FormatPlugin for ExcalidrawV3Prototype {
    type Document = Document;

    fn open_file(
        input: sdk::OpenFile<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<Self::Document> {
        let namespace = IdNamespace::from_halves(input.creates.high, u64::from(input.creates.low));
        let (document, changes) = Document::open_file(
            input.source.read_all()?,
            input.file.path.as_deref(),
            namespace,
        )
        .map_err(sdk::Error::invalid_input)?;
        emit_changes(changes, sink)?;
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
            .map(|(edit, insert)| InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert,
            })
            .collect::<Vec<_>>();
        let namespace =
            IdNamespace::from_halves(update.creates.high, u64::from(update.creates.low));
        let (document, changes) = document
            .file_changed(&splices, namespace)
            .map_err(sdk::Error::invalid_input)?;
        emit_changes(changes.into_iter().map(Ok), sink)?;
        Ok(document)
    }
}

fn emit_changes<I>(changes: I, sink: &mut sdk::Sink<'_>) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<EntityChange, String>>,
{
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change.map_err(sdk::Error::invalid_input)?, sink)?;
    }
    encoder.flush(sink)
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

    fn push(&mut self, change: EntityChange, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let mut record = Vec::new();
        encode_change(change, &mut record)?;
        if record.len() > self.max_bytes {
            return Err(sdk::Error::limit_exceeded(
                "one Excalidraw entity exceeds the v3 batch limit",
            ));
        }
        if self.records > 0 && self.payload.len() + record.len() > self.max_bytes {
            self.flush(sink)?;
        }
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("Excalidraw batch count overflowed"))?;
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

fn encode_change(change: EntityChange, output: &mut Vec<u8>) -> sdk::Result<()> {
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match change.snapshot {
        Some(snapshot) => {
            output.push(0);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
            output.push(match change.effect {
                ChangeEffect::Content => 0,
                ChangeEffect::FormatOnly => 1,
            });
            push_inline_blob(output, &snapshot)?;
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
        }
    }
    let length = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("Excalidraw packet exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn push_entity_key(
    output: &mut Vec<u8>,
    schema_key: &str,
    components: &[String],
) -> sdk::Result<()> {
    push_text(output, schema_key)?;
    output.extend_from_slice(
        &u32::try_from(components.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many primary-key components"))?
            .to_le_bytes(),
    );
    for component in components {
        push_text(output, component)?;
    }
    Ok(())
}

fn push_text(output: &mut Vec<u8>, value: &str) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| sdk::Error::limit_exceeded("packet text is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_inline_blob(output: &mut Vec<u8>, bytes: &[u8]) -> sdk::Result<()> {
    output.push(0);
    output.extend_from_slice(
        &u32::try_from(bytes.len())
            .map_err(|_| sdk::Error::limit_exceeded("snapshot is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(bytes);
    Ok(())
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v3_prototype::export_v3_prototype!(ExcalidrawV3Prototype);
