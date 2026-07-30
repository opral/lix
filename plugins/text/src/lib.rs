//! Git-style line semantics for NUL-free text files.
//!
//! A line is a durable entity rather than a display-only diff hunk. The
//! component preserves source bytes exactly, including invalid UTF-8 and final
//! unterminated lines, by storing each LF-delimited byte segment as base64.
#![allow(dead_code)]

mod core;
mod model;

use core::{Document, InputSplice};
use lix_plugin_api as sdk;
use model::{ChangeEffect, EntityChange};

struct GitTextPlugin;

const ID_NAMESPACE_STATE: &[u8] = b"git-text/id-namespace-v1";

impl sdk::FormatPlugin for GitTextPlugin {
    fn open_file(input: &sdk::OpenFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let namespace = input.creates;
        let (document, changes) = Document::open_file(input.accepted.read_all()?, |ordinal| {
            namespace.id(local_ref(ordinal))
        })
        .map_err(sdk::Error::invalid_input)?;
        input
            .successor
            .put_state(ID_NAMESPACE_STATE, &namespace.namespace_bytes())?;
        emit_changes(changes, namespace, sink)?;
        drop(document);
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let original_namespace = read_namespace(&update.before)?;
        let (before, _) = Document::open_file(update.before.read_all()?, |ordinal| {
            original_namespace.id(local_ref(ordinal))
        })
        .map_err(sdk::Error::invalid_input)?;
        let splices = update
            .edits
            .iter()
            .map(|edit| InputSplice {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert.clone(),
            })
            .collect::<Vec<_>>();
        let creates = update.creates;
        let (_, changes) = before
            .file_changed(&splices, |ordinal| creates.id(local_ref(ordinal)))
            .map_err(sdk::Error::invalid_input)?;
        emit_changes(changes.into_iter().map(Ok), creates, sink)
    }

    fn entities_changed(
        update: &mut sdk::EntityUpdate<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let namespace = read_namespace(&update.before)?;
        let (before, _) = Document::open_file(update.before.read_all()?, |ordinal| {
            namespace.id(local_ref(ordinal))
        })
        .map_err(sdk::Error::invalid_input)?;
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
        let (after, _edits) = before
            .entities_changed(changes)
            .map_err(sdk::Error::invalid_input)?;
        sink.replace_file(after.bytes())
    }
}

fn read_namespace(root: &sdk::Root<'_>) -> sdk::Result<sdk::CreateContext> {
    let bytes = root
        .get_state(ID_NAMESPACE_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Git text namespace state is missing"))?;
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("Git text namespace state has invalid length"))?;
    Ok(sdk::CreateContext {
        high: u64::from_be_bytes(bytes[..8].try_into().expect("eight bytes")),
        low: u32::from_be_bytes(bytes[8..].try_into().expect("four bytes")),
    })
}

fn local_ref(ordinal: u64) -> u32 {
    u32::try_from(ordinal).expect("materialized text rows fit the create local-ref range")
}

fn emit_changes<I>(
    changes: I,
    creates: sdk::CreateContext,
    sink: &mut sdk::Sink<'_>,
) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<EntityChange, String>>,
{
    let mut encoder = BatchEncoder::new(sink.max_batch_bytes());
    for change in changes {
        encoder.push(change.map_err(sdk::Error::invalid_input)?, creates, sink)?;
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
                "one Git text entity exceeds the batch limit",
            ));
        }
        if self.records > 0 && self.payload.len() + record.len() > self.max_bytes {
            self.flush(sink)?;
        }
        self.payload.extend_from_slice(&record);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| sdk::Error::limit_exceeded("Git text batch count overflowed"))?;
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
    let record_start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match change.snapshot {
        Some(snapshot) => {
            if let Some(local_ref) = create_local_ref(&change.entity_pk, creates) {
                output.push(2);
                push_text(output, &change.schema_key)?;
                output.extend_from_slice(&u64::from(local_ref).to_le_bytes());
                push_inline_blob(output, &snapshot_without_id(&snapshot)?)?;
            } else {
                output.push(0);
                push_entity_key(output, &change.schema_key, &change.entity_pk)?;
                output.push(match change.effect {
                    ChangeEffect::Content => 0,
                    ChangeEffect::FormatOnly => 1,
                });
                push_inline_blob(output, &snapshot)?;
            }
        }
        None => {
            output.push(1);
            push_entity_key(output, &change.schema_key, &change.entity_pk)?;
        }
    }
    let length = u32::try_from(output.len() - record_start - 4)
        .map_err(|_| sdk::Error::limit_exceeded("Git text packet exceeds 4 GiB"))?;
    output[record_start..record_start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn create_local_ref(entity_pk: &[String], creates: sdk::CreateContext) -> Option<u32> {
    let [id] = entity_pk else {
        return None;
    };
    let bytes = uuid::Uuid::parse_str(id).ok()?.into_bytes();
    (bytes[..12] == creates.namespace_bytes())
        .then(|| u32::from_be_bytes(bytes[12..].try_into().expect("four UUID bytes")))
}

fn snapshot_without_id(snapshot: &[u8]) -> sdk::Result<Vec<u8>> {
    let mut value: serde_json::Value = serde_json::from_slice(snapshot).map_err(|error| {
        sdk::Error::invalid_input(format!("invalid Git text snapshot: {error}"))
    })?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| sdk::Error::invalid_input("Git text snapshot must be an object"))?;
    object
        .remove("id")
        .ok_or_else(|| sdk::Error::invalid_input("Git text snapshot is missing its id"))?;
    serde_json::to_vec(&value).map_err(|error| {
        sdk::Error::internal(format!(
            "failed to encode Git text create snapshot: {error}"
        ))
    })
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

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 1] = [(
    "schema/git_text_line_v2.json",
    include_str!("../schema/git_text_line_v2.json"),
)];

#[cfg(test)]
mod tests;

#[cfg(target_family = "wasm")]
lix_plugin_api::export_plugin!(GitTextPlugin);
