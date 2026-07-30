//! Git-style line semantics for NUL-free text files.
//!
//! A line is a durable entity rather than a display-only diff hunk. The
//! component preserves source bytes exactly, including invalid UTF-8 and final
//! unterminated lines, by storing each LF-delimited byte segment as base64.
#![allow(dead_code)]

mod core;
mod model;

use core::{Document, InputSplice, LineIdentity};
use lix_plugin_api as sdk;
use model::{ChangeEffect, EntityChange, EntityRecord};

struct GitTextPlugin;

const ID_NAMESPACE_STATE: &[u8] = b"git-text/id-namespace-v1";
const LINE_IDENTITIES_STATE: &[u8] = b"git-text/line-identities-v1";
const LINE_IDENTITIES_MAGIC: &[u8; 4] = b"GTI1";
const STATE_PAGE_BYTES: usize = 1024 * 1024;

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
        store_identities_in_transaction(&input.successor, &document)?;
        emit_changes(changes, namespace, sink)?;
        Ok(())
    }

    fn file_changed(update: &sdk::FileUpdate<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let before = read_document(&update.before)?;
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
        let (after, changes) = before
            .file_changed(&splices, |ordinal| creates.id(local_ref(ordinal)))
            .map_err(sdk::Error::invalid_input)?;
        replace_identities_in_transaction(&update.before, &update.successor, &after)?;
        emit_changes(changes.into_iter().map(Ok), creates, sink)
    }

    fn entities_changed(
        update: &mut sdk::EntityUpdate<'_>,
        sink: &mut sdk::Sink<'_>,
    ) -> sdk::Result<()> {
        let before = read_document(&update.before)?;
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
        sink.replace_file(after.bytes())?;
        replace_identities_from_sink(&update.before, sink, &after)
    }

    fn hydrate(input: &mut sdk::HydrateFile<'_>, sink: &mut sdk::Sink<'_>) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(entity) = input.entities.next()? {
            records.push(Ok(EntityRecord {
                schema_key: entity.schema_key,
                entity_pk: entity.entity_pk,
                snapshot: entity.snapshot.ok_or_else(|| {
                    sdk::Error::invalid_input("Git text hydration received a tombstone")
                })?,
            }));
        }
        let document =
            Document::open_entities_fallible(records).map_err(sdk::Error::invalid_input)?;
        store_identities_in_transaction(&input.successor, &document)?;
        if input.accepted.is_none() {
            sink.replace_file(document.bytes())?;
        }
        Ok(())
    }
}

fn read_document(root: &sdk::Root<'_>) -> sdk::Result<Document> {
    let manifest = root
        .get_state(LINE_IDENTITIES_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Git text identity state is missing"))?;
    let (line_count, page_count) = decode_identity_manifest(&manifest)?;
    let mut pages = Vec::with_capacity(page_count as usize);
    for ordinal in 0..page_count {
        pages.push(
            root.get_state(&line_identity_page_key(ordinal))?
                .ok_or_else(|| sdk::Error::invalid_input("Git text identity page disappeared"))?,
        );
    }
    Document::open_file_with_identities(root.read_all()?, decode_identities(line_count, pages)?)
        .map_err(sdk::Error::invalid_input)
}

fn store_identities_in_transaction(
    successor: &sdk::Transaction<'_>,
    document: &Document,
) -> sdk::Result<()> {
    let (manifest, pages) = encode_identities(&document.identities())?;
    successor.put_state(LINE_IDENTITIES_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&line_identity_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn replace_identities_in_transaction(
    before: &sdk::Root<'_>,
    successor: &sdk::Transaction<'_>,
    document: &Document,
) -> sdk::Result<()> {
    let old_page_count = identity_page_count(before)?;
    let (manifest, pages) = encode_identities(&document.identities())?;
    successor.put_state(LINE_IDENTITIES_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&line_identity_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        successor.delete_state(&line_identity_page_key(ordinal))?;
    }
    Ok(())
}

fn replace_identities_from_sink(
    before: &sdk::Root<'_>,
    sink: &mut sdk::Sink<'_>,
    document: &Document,
) -> sdk::Result<()> {
    let old_page_count = identity_page_count(before)?;
    let (manifest, pages) = encode_identities(&document.identities())?;
    sink.put_state(LINE_IDENTITIES_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        sink.put_state(&line_identity_page_key(ordinal as u32), page)?;
    }
    for ordinal in pages.len() as u32..old_page_count {
        sink.delete_state(&line_identity_page_key(ordinal))?;
    }
    Ok(())
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

fn encode_identities(identities: &[LineIdentity]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(STATE_PAGE_BYTES);
    for identity in identities {
        let mut record = Vec::new();
        push_text(&mut record, &identity.id)?;
        push_text(&mut record, &identity.order_key)?;
        push_paged_state(&mut pages, &mut page, &record);
    }
    if !page.is_empty() {
        pages.push(page);
    }
    let mut manifest = Vec::with_capacity(12);
    manifest.extend_from_slice(LINE_IDENTITIES_MAGIC);
    manifest.extend_from_slice(
        &u32::try_from(identities.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Git text lines"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Git text identity pages"))?
            .to_le_bytes(),
    );
    Ok((manifest, pages))
}

fn decode_identities(line_count: u32, pages: Vec<Vec<u8>>) -> sdk::Result<Vec<LineIdentity>> {
    let mut reader = IdentityReader::new(pages);
    let mut identities = Vec::with_capacity(line_count as usize);
    for _ in 0..line_count {
        identities.push(LineIdentity {
            id: reader.text()?,
            order_key: reader.text()?,
        });
    }
    if !reader.finished() {
        return Err(sdk::Error::invalid_input(
            "Git text identity state contains trailing bytes",
        ));
    }
    Ok(identities)
}

fn decode_identity_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32)> {
    if bytes.len() != 12 || bytes.get(..4) != Some(LINE_IDENTITIES_MAGIC) {
        return Err(sdk::Error::invalid_input(
            "unsupported Git text identity state",
        ));
    }
    Ok((
        u32::from_le_bytes(bytes[4..8].try_into().expect("fixed identity manifest")),
        u32::from_le_bytes(bytes[8..12].try_into().expect("fixed identity manifest")),
    ))
}

fn identity_page_count(root: &sdk::Root<'_>) -> sdk::Result<u32> {
    let Some(manifest) = root.get_state(LINE_IDENTITIES_STATE)? else {
        return Ok(0);
    };
    decode_identity_manifest(&manifest).map(|(_, page_count)| page_count)
}

fn line_identity_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"git-text/line-identity-page-v1/".to_vec();
    key.extend_from_slice(&ordinal.to_le_bytes());
    key
}

fn push_paged_state(pages: &mut Vec<Vec<u8>>, page: &mut Vec<u8>, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let available = STATE_PAGE_BYTES - page.len();
        let take = available.min(bytes.len());
        page.extend_from_slice(&bytes[..take]);
        bytes = &bytes[take..];
        if page.len() == STATE_PAGE_BYTES {
            pages.push(std::mem::replace(
                page,
                Vec::with_capacity(STATE_PAGE_BYTES),
            ));
        }
    }
}

struct IdentityReader {
    pages: Vec<Vec<u8>>,
    page: usize,
    offset: usize,
}

impl IdentityReader {
    fn new(pages: Vec<Vec<u8>>) -> Self {
        Self {
            pages,
            page: 0,
            offset: 0,
        }
    }

    fn bytes(&mut self, mut length: usize) -> sdk::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(length);
        while length > 0 {
            let page = self
                .pages
                .get(self.page)
                .ok_or_else(|| sdk::Error::invalid_input("Git text identity state is truncated"))?;
            let available = page.len().saturating_sub(self.offset);
            if available == 0 {
                self.page += 1;
                self.offset = 0;
                continue;
            }
            let take = available.min(length);
            output.extend_from_slice(&page[self.offset..self.offset + take]);
            self.offset += take;
            length -= take;
        }
        Ok(output)
    }

    fn text(&mut self) -> sdk::Result<String> {
        let length = u32::from_le_bytes(
            self.bytes(4)?
                .try_into()
                .expect("identity reader returned four bytes"),
        ) as usize;
        String::from_utf8(self.bytes(length)?)
            .map_err(|_| sdk::Error::invalid_input("Git text identity is not UTF-8"))
    }

    fn finished(&self) -> bool {
        match self.pages.get(self.page) {
            None => true,
            Some(page) => {
                self.offset == page.len()
                    && self
                        .pages
                        .iter()
                        .skip(self.page.saturating_add(1))
                        .all(Vec::is_empty)
            }
        }
    }
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
