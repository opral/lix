//! Fused Markdown experiment for Component API v3.

#[path = "../../markdown-v2/src/core.rs"]
mod core;
#[path = "../../markdown-v2/src/markdown_file.rs"]
mod markdown_file;
#[path = "../../markdown-v2/src/model.rs"]
mod model;
#[path = "../../markdown-v2/src/schemas.rs"]
mod schemas;

use core::{ChangeEffect, Document, EntityChange, IdNamespace, InputSplice, PluginError};
use lix_plugin_api_v3_prototype as sdk;
use serde_json::Value;

struct MarkdownV3Prototype;

impl sdk::FormatPlugin for MarkdownV3Prototype {
    type Document = Document;

    fn open_file(
        input: sdk::OpenFile<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<Self::Document> {
        let bytes = input.source.read_all()?;
        let namespace = IdNamespace::from_halves(input.creates.high, input.creates.low);
        let (document, changes) = Document::open_file(bytes, input.file.path.as_deref(), namespace)
            .map_err(core_error)?;
        emit_changes(changes, input.creates, sink)?;
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
        let namespace = IdNamespace::from_halves(update.creates.high, update.creates.low);
        let (document, changes) = document
            .file_changed(&splices, namespace)
            .map_err(core_error)?;
        emit_changes(changes, update.creates, sink)?;
        Ok(document)
    }
}

fn core_error(error: PluginError) -> sdk::Error {
    match error {
        PluginError::InvalidInput(message) => sdk::Error::invalid_input(message),
        PluginError::Internal(message) => sdk::Error::internal(message),
    }
}

fn emit_changes(
    changes: impl IntoIterator<Item = EntityChange>,
    creates: sdk::CreateContext,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()> {
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change, creates, sink)?;
    }
    encoder.flush(sink)
}

struct BatchEncoder {
    max_bytes: usize,
    payload: Vec<u8>,
    records: u32,
    creates_only: Option<bool>,
}

impl BatchEncoder {
    fn new(max_bytes: u32) -> Self {
        Self {
            max_bytes: max_bytes as usize,
            payload: Vec::with_capacity(max_bytes as usize),
            records: 0,
            creates_only: None,
        }
    }

    fn push(
        &mut self,
        change: EntityChange,
        creates: sdk::CreateContext,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let mut record = Vec::new();
        let is_create = encode_change(change, creates, &mut record)?;
        if record.len() > self.max_bytes {
            return Err(sdk::Error::limit_exceeded(
                "one Markdown entity exceeds the v3 batch limit",
            ));
        }
        if self.records > 0
            && (self.payload.len() + record.len() > self.max_bytes
                || self.creates_only != Some(is_create))
        {
            self.flush(sink)?;
        }
        self.creates_only = Some(is_create);
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("Markdown batch count overflowed"))?;
        Ok(())
    }

    fn flush(&mut self, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        if self.records == 0 {
            return Ok(());
        }
        let payload = std::mem::replace(&mut self.payload, Vec::with_capacity(self.max_bytes));
        let records = std::mem::take(&mut self.records);
        self.creates_only = None;
        sink.emit_changes(records, payload)
    }
}

fn encode_change(
    change: EntityChange,
    creates: sdk::CreateContext,
    output: &mut Vec<u8>,
) -> sdk::Result<bool> {
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    let is_create = match change.snapshot {
        Some(snapshot) => {
            let local_ref = change
                .entity_pk
                .first()
                .filter(|_| change.entity_pk.len() == 1)
                .and_then(|id| local_ref(creates, id));
            if let Some(local_ref) = local_ref {
                output.push(2);
                push_text(output, &change.schema_key)?;
                output.extend_from_slice(&u64::from(local_ref).to_le_bytes());
                push_inline_blob(output, &remove_created_id(snapshot)?)?;
                true
            } else {
                output.push(0);
                push_entity_key(output, &change.schema_key, &change.entity_pk)?;
                output.push(effect_tag(change.effect));
                push_inline_blob(output, &snapshot)?;
                false
            }
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
            false
        }
    };
    let record_len = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("Markdown packet record exceeds 4GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&record_len.to_le_bytes());
    Ok(is_create)
}

fn local_ref(creates: sdk::CreateContext, id: &str) -> Option<u32> {
    let id = uuid::Uuid::parse_str(id).ok()?;
    let bytes = id.as_bytes();
    if bytes[..12] != creates.namespace_bytes() {
        return None;
    }
    Some(u32::from_be_bytes(bytes[12..].try_into().ok()?))
}

fn remove_created_id(snapshot: Vec<u8>) -> sdk::Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(&snapshot).map_err(|error| {
        sdk::Error::invalid_input(format!("invalid Markdown snapshot: {error}"))
    })?;
    value
        .as_object_mut()
        .and_then(|object| object.remove("id"))
        .ok_or_else(|| sdk::Error::invalid_input("created Markdown snapshot has no id"))?;
    serde_json::to_vec(&value)
        .map_err(|error| sdk::Error::internal(format!("encode Markdown snapshot: {error}")))
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
lix_plugin_api_v3_prototype::export_v3_prototype!(MarkdownV3Prototype);
