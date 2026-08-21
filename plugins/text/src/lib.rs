//! byte-exact line semantics for NUL-free text files.
//!
//! A line is a durable row rather than a display-only diff hunk. The
//! component preserves source bytes exactly, including invalid UTF-8 and final
//! unterminated lines, by storing each LF-delimited byte segment as base64.
#![allow(dead_code)]

mod core;
mod model;
mod order_key;

use core::{Document, FileEdit, LineIdentity};
use lix::plugin as sdk;
use model::{ChangeEffect, RowChange, RowRecord};
use std::sync::OnceLock;

struct TextPlugin;

const ID_NAMESPACE_STATE: &[u8] = b"text/id-namespace";
const LINE_IDENTITIES_STATE: &[u8] = b"text/line-identities";
const LINE_IDENTITIES_MAGIC: &[u8; 4] = b"LTX2";
const STATE_PAGE_BYTES: usize = 1024 * 1024;
const LINE_SCHEMA_JSON: &str = include_str!("../schema/text_line.json");

impl sdk::FileProjection for TextPlugin {
    fn parse(input: sdk::ParseInput<'_>, output: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        let namespace = input.creates;
        let (document, changes) = Document::open_file(input.file.read_all()?, |ordinal| {
            created_uuid(namespace, local_ref(ordinal))
        })
        .map_err(sdk::Error::invalid_input)?;
        output.put_state(ID_NAMESPACE_STATE, &namespace.namespace_bytes())?;
        store_identities(output, &document)?;
        emit_changes(changes, namespace, output)
    }

    fn parse_changes(
        mut update: sdk::ParseChangesInput<'_>,
        output: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        if update.before.state_len(LINE_IDENTITIES_STATE)?.is_some() {
            let before = read_document(&update.before)?;
            let splices = update
                .file_edits
                .iter()
                .map(|edit| FileEdit {
                    offset: edit.offset,
                    delete_len: edit.delete_len,
                    insert: edit.insert.clone(),
                })
                .collect::<Vec<_>>();
            let creates = update.creates;
            let (after, changes) = before
                .file_changed(&splices, |ordinal| {
                    created_uuid(creates, local_ref(ordinal))
                })
                .map_err(sdk::Error::invalid_input)?;
            replace_identities(&update.before, output, &after)?;
            return emit_changes(changes.into_iter().map(Ok), creates, output);
        }

        let accepted = update.before.read_all()?;
        let mut records = Vec::new();
        let rows = update.typed_rows.as_mut().ok_or_else(|| {
            sdk::Error::internal("cold parse_changes requires durable typed rows")
        })?;
        while let Some(row) = rows.next()? {
            records.push(Ok(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.primary_key,
                row: row.row,
            }));
        }
        let creates = update.creates;
        let mut document =
            Document::open_rows_fallible(records).map_err(sdk::Error::invalid_input)?;
        if document.bytes() != accepted {
            let reconcile = [FileEdit {
                offset: 0,
                delete_len: document.bytes().len() as u64,
                insert: accepted,
            }];
            document = document
                .file_changed(&reconcile, |ordinal| {
                    created_uuid(creates, local_ref(ordinal))
                })
                .map_err(sdk::Error::invalid_input)?
                .0;
        }
        let splices = update
            .file_edits
            .iter()
            .map(|edit| FileEdit {
                offset: edit.offset,
                delete_len: edit.delete_len,
                insert: edit.insert.clone(),
            })
            .collect::<Vec<_>>();
        let (successor, changes) = document
            .file_changed(&splices, |ordinal| {
                created_uuid(creates, local_ref(ordinal))
            })
            .map_err(sdk::Error::invalid_input)?;
        output.put_state(ID_NAMESPACE_STATE, &creates.namespace_bytes())?;
        store_identities(output, &successor)?;
        emit_changes(changes.into_iter().map(Ok), creates, output)
    }

    fn serialize_changes(
        mut update: sdk::SerializeChangesInput<'_>,
        output: &mut sdk::FileEditOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let before = read_document(&update.before)?;
        let mut changes = Vec::new();
        while let Some(change) = update.typed_row_changes.next()? {
            changes.push(RowChange {
                schema_key: change.schema_key,
                row_pk: change.primary_key,
                row: change.row,
                effect: match change.effect {
                    sdk::ChangeEffect::Content => ChangeEffect::Content,
                    sdk::ChangeEffect::FormatOnly => ChangeEffect::FormatOnly,
                },
            });
        }
        let (after, edits) = before
            .rows_changed(changes)
            .map_err(sdk::Error::invalid_input)?;
        for edit in edits {
            output.replace(edit.offset, edit.delete_len, &edit.insert)?;
        }
        replace_identities(&update.before, output, &after)
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        output: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut records = Vec::new();
        while let Some(row) = input.typed_rows.next()? {
            records.push(Ok(RowRecord {
                schema_key: row.schema_key,
                row_pk: row.primary_key,
                row: row.row,
            }));
        }
        let document = Document::open_rows_fallible(records).map_err(sdk::Error::invalid_input)?;
        store_identities(output, &document)?;
        output.write(document.bytes())
    }
}

fn read_document(root: &sdk::Snapshot<'_>) -> sdk::Result<Document> {
    let manifest = root
        .get_state(LINE_IDENTITIES_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Text identity state is missing"))?;
    let (line_count, page_count) = decode_identity_manifest(&manifest)?;
    let mut pages = Vec::with_capacity(page_count as usize);
    for ordinal in 0..page_count {
        pages.push(
            root.get_state(&line_identity_page_key(ordinal))?
                .ok_or_else(|| sdk::Error::invalid_input("Text identity page disappeared"))?,
        );
    }
    Document::open_file_with_identities(root.read_all()?, decode_identities(line_count, pages)?)
        .map_err(sdk::Error::invalid_input)
}

fn store_identities(successor: &mut impl StateOutput, document: &Document) -> sdk::Result<()> {
    let (manifest, pages) = encode_identities(&document.identities())?;
    successor.put_state(LINE_IDENTITIES_STATE, &manifest)?;
    for (ordinal, page) in pages.iter().enumerate() {
        successor.put_state(&line_identity_page_key(ordinal as u32), page)?;
    }
    Ok(())
}

fn replace_identities(
    before: &sdk::Snapshot<'_>,
    successor: &mut impl StateOutput,
    document: &Document,
) -> sdk::Result<()> {
    let old_page_count = match before.get_state(LINE_IDENTITIES_STATE)? {
        Some(manifest) => decode_identity_manifest(&manifest)?.1,
        None => 0,
    };
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

fn read_namespace(root: &sdk::Snapshot<'_>) -> sdk::Result<sdk::CreateContext> {
    let bytes = root
        .get_state(ID_NAMESPACE_STATE)?
        .ok_or_else(|| sdk::Error::invalid_input("Text namespace state is missing"))?;
    let bytes: [u8; 12] = bytes
        .try_into()
        .map_err(|_| sdk::Error::invalid_input("Text namespace state has invalid length"))?;
    Ok(sdk::CreateContext::from_namespace_bytes(bytes))
}

fn encode_identities(identities: &[LineIdentity]) -> sdk::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(STATE_PAGE_BYTES);
    for identity in identities {
        let mut record = Vec::new();
        record.extend_from_slice(identity.id.as_bytes());
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
            .map_err(|_| sdk::Error::limit_exceeded("too many Text lines"))?
            .to_le_bytes(),
    );
    manifest.extend_from_slice(
        &u32::try_from(pages.len())
            .map_err(|_| sdk::Error::limit_exceeded("too many Text identity pages"))?
            .to_le_bytes(),
    );
    Ok((manifest, pages))
}

fn decode_identities(line_count: u32, pages: Vec<Vec<u8>>) -> sdk::Result<Vec<LineIdentity>> {
    let mut reader = IdentityReader::new(pages);
    let mut identities = Vec::with_capacity(line_count as usize);
    for _ in 0..line_count {
        identities.push(LineIdentity {
            id: uuid::Uuid::from_bytes(
                reader
                    .bytes(16)?
                    .try_into()
                    .expect("identity reader returned sixteen UUID bytes"),
            ),
            order_key: reader.text()?,
        });
    }
    if !reader.finished() {
        return Err(sdk::Error::invalid_input(
            "Text identity state contains trailing bytes",
        ));
    }
    Ok(identities)
}

fn decode_identity_manifest(bytes: &[u8]) -> sdk::Result<(u32, u32)> {
    if bytes.len() != 12 || bytes.get(..4) != Some(LINE_IDENTITIES_MAGIC) {
        return Err(sdk::Error::invalid_input("unsupported Text identity state"));
    }
    Ok((
        u32::from_le_bytes(bytes[4..8].try_into().expect("fixed identity manifest")),
        u32::from_le_bytes(bytes[8..12].try_into().expect("fixed identity manifest")),
    ))
}

fn line_identity_page_key(ordinal: u32) -> Vec<u8> {
    let mut key = b"text/line-identity-page/".to_vec();
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
                .ok_or_else(|| sdk::Error::invalid_input("Text identity state is truncated"))?;
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
            .map_err(|_| sdk::Error::invalid_input("Text identity is not UTF-8"))
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
    sink: &mut impl MutationOutput,
) -> sdk::Result<()>
where
    I: IntoIterator<Item = Result<RowChange, String>>,
{
    for change in changes {
        let change = change.map_err(sdk::Error::invalid_input)?;
        match change.row {
            Some(row) => {
                if let Some(local_ref) = create_local_ref(&change.row_pk, creates) {
                    sink.create(&change.schema_key, local_ref, &row)?;
                } else {
                    sink.upsert(
                        &change.schema_key,
                        &change.row_pk,
                        &row,
                        match change.effect {
                            ChangeEffect::Content => sdk::ChangeEffect::Content,
                            ChangeEffect::FormatOnly => sdk::ChangeEffect::FormatOnly,
                        },
                    )?;
                }
            }
            None => sink.delete(&change.schema_key, &change.row_pk)?,
        }
    }
    Ok(())
}

trait StateOutput {
    fn put_state(&mut self, key: &[u8], value: &[u8]) -> sdk::Result<()>;
    fn delete_state(&mut self, key: &[u8]) -> sdk::Result<()>;
}

macro_rules! impl_state_output {
    ($type:ty) => {
        impl StateOutput for $type {
            fn put_state(&mut self, key: &[u8], value: &[u8]) -> sdk::Result<()> {
                <$type>::put_state(self, key, value)
            }
            fn delete_state(&mut self, key: &[u8]) -> sdk::Result<()> {
                <$type>::delete_state(self, key)
            }
        }
    };
}

impl_state_output!(sdk::RowOutput<'_, '_>);
impl_state_output!(sdk::RowChangeOutput<'_, '_>);
impl_state_output!(sdk::FileOutput<'_, '_>);
impl_state_output!(sdk::FileEditOutput<'_, '_>);

trait MutationOutput {
    fn create(&mut self, schema_key: &str, local_ref: u32, row: &sdk::TypedRow) -> sdk::Result<()>;
    fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        effect: sdk::ChangeEffect,
    ) -> sdk::Result<()>;
    fn delete(&mut self, schema_key: &str, row_pk: &[sdk::TypedValue]) -> sdk::Result<()>;
}

impl MutationOutput for sdk::RowOutput<'_, '_> {
    fn create(&mut self, schema_key: &str, local_ref: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(schema_key)?;
        self.create(schema_key, fingerprint, local_ref, row)
    }
    fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        _effect: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(schema_key)?;
        self.upsert(schema_key, fingerprint, row_pk.to_vec(), row)
    }
    fn delete(&mut self, _schema_key: &str, _row_pk: &[sdk::TypedValue]) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "initial Text parse produced a deletion",
        ))
    }
}

impl MutationOutput for sdk::RowChangeOutput<'_, '_> {
    fn create(&mut self, schema_key: &str, local_ref: u32, row: &sdk::TypedRow) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(schema_key)?;
        self.create(schema_key, fingerprint, local_ref, row)
    }
    fn upsert(
        &mut self,
        schema_key: &str,
        row_pk: &[sdk::TypedValue],
        row: &sdk::TypedRow,
        effect: sdk::ChangeEffect,
    ) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(schema_key)?;
        self.upsert(schema_key, fingerprint, row_pk.to_vec(), row, effect)
    }
    fn delete(&mut self, schema_key: &str, row_pk: &[sdk::TypedValue]) -> sdk::Result<()> {
        let (schema_key, fingerprint) = typed_schema(schema_key)?;
        self.delete(schema_key, fingerprint, row_pk.to_vec())
    }
}

fn typed_schema(schema_key: &str) -> sdk::Result<(&'static str, [u8; 32])> {
    static FINGERPRINT: OnceLock<[u8; 32]> = OnceLock::new();
    if schema_key != "text_line" {
        return Err(sdk::Error::invalid_input(format!(
            "Text plugin does not support schema '{schema_key}'"
        )));
    }
    Ok((
        "text_line",
        *FINGERPRINT.get_or_init(|| {
            sdk::schema_fingerprint(LINE_SCHEMA_JSON).expect("embedded text schema must be valid")
        }),
    ))
}

fn create_local_ref(row_pk: &[sdk::TypedValue], creates: sdk::CreateContext) -> Option<u32> {
    let [sdk::TypedValue::Uuid(id)] = row_pk else {
        return None;
    };
    let bytes = id.into_bytes();
    (bytes[..12] == creates.namespace_bytes())
        .then(|| u32::from_be_bytes(bytes[12..].try_into().expect("four UUID bytes")))
}

fn created_uuid(creates: sdk::CreateContext, local_ref: u32) -> uuid::Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..12].copy_from_slice(&creates.namespace_bytes());
    bytes[12..].copy_from_slice(&local_ref.to_be_bytes());
    uuid::Uuid::from_bytes(bytes)
}

fn push_text(output: &mut Vec<u8>, value: &str) -> sdk::Result<()> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| sdk::Error::limit_exceeded("state text is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
pub const SCHEMAS: [(&str, &str); 1] = [(
    "schema/text_line.json",
    include_str!("../schema/text_line.json"),
)];

#[cfg(test)]
mod tests;

#[cfg(target_family = "wasm")]
lix::plugin::export_capabilities! { file_projection: TextPlugin }
