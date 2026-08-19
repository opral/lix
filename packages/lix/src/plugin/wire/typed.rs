//! Schema v1 typed-row page records.
//!
//! A typed page carries one schema key and its exact 32-byte Schema v1
//! fingerprint in the envelope. Records contain typed primary-key values and
//! typed column values; there is no outer row object to parse or canonicalize.

use std::sync::Arc;

use lix_schema::{Row, Value};

use super::{Error as PageError, encode_typed_page};

const MAX_COLUMNS: u32 = 4096;
const MAX_KEY_COMPONENTS: u32 = 128;
pub(crate) const MAX_RECORDS_PER_PAGE: u32 = 65_536;
const MAX_TEXT_BYTES: usize = 16 * 1024 * 1024;
const ATTACHMENT_THRESHOLD_BYTES: usize = 8 * 1024;
/// Canonical ABI width of one `list<u8>` entry in the page attachment table
/// (32-bit pointer plus 32-bit length). The bytes live beside, rather than in,
/// the framed payload but still occupy guest memory while the page is sent.
pub(crate) const ATTACHMENT_TABLE_ENTRY_BYTES: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Mutation<'a> {
    Create {
        local_ref: u32,
        row: &'a Row,
    },
    Upsert {
        row_pk: &'a [Value],
        row: &'a Row,
        effect: ChangeEffect,
    },
    Delete {
        row_pk: &'a [Value],
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum OwnedMutation {
    Create {
        local_ref: u32,
        row: Row,
    },
    Upsert {
        row_pk: Vec<Value>,
        row: Row,
        effect: ChangeEffect,
    },
    Delete {
        row_pk: Vec<Value>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Page(PageError),
    Invalid(&'static str),
    Message(String),
}

impl From<PageError> for Error {
    fn from(error: PageError) -> Self {
        Self::Page(error)
    }
}

pub fn encode_page(
    schema_key: &str,
    schema_fingerprint: &[u8; 32],
    mutations: &[Mutation<'_>],
) -> Result<Vec<u8>, Error> {
    encode_page_parts(schema_key, schema_fingerprint, mutations).map(|(page, _)| page)
}

pub fn encode_page_parts(
    schema_key: &str,
    schema_fingerprint: &[u8; 32],
    mutations: &[Mutation<'_>],
) -> Result<(Vec<u8>, Vec<Vec<u8>>), Error> {
    if mutations.is_empty() {
        return Err(Error::Invalid("typed row page must not be empty"));
    }
    if mutations.len() > MAX_RECORDS_PER_PAGE as usize {
        return Err(Error::Invalid("typed row page has too many records"));
    }
    let columns = page_columns(mutations)?;
    let mut payload = Vec::new();
    encode_columns(&mut payload, &columns)?;
    let mut attachments = Vec::new();
    for mutation in mutations {
        encode_mutation(&mut payload, mutation, &columns, &mut attachments, false)?;
    }
    let page = encode_typed_page(
        schema_key,
        schema_fingerprint,
        u32::try_from(mutations.len()).map_err(|_| Error::Invalid("too many typed rows"))?,
        payload,
    )
    .map_err(Error::Page)?;
    Ok((page, attachments))
}

/// Appends one borrowed mutation to an in-progress page payload.
pub(crate) fn append_mutation(
    payload: &mut Vec<u8>,
    attachments: &mut Vec<Vec<u8>>,
    mutation: &Mutation<'_>,
    columns: &[Arc<str>],
) -> Result<(), Error> {
    // `TransitionOutput::typed_row` has already compared this row with the
    // active page layout. Avoid repeating every column-name comparison while
    // streaming the values into that page.
    encode_mutation(payload, mutation, columns, attachments, true)
}

/// Writes the canonical per-page column layout. Complete rows then carry only
/// their values in this exact order; the page's schema fingerprint binds the
/// layout to the declared Schema v1 definition.
pub(crate) fn begin_page_payload(columns: &[Arc<str>]) -> Result<Vec<u8>, Error> {
    let mut payload = Vec::new();
    encode_columns(&mut payload, columns)?;
    Ok(payload)
}

/// Frames a payload assembled with [`append_owned_mutation`].
pub(crate) fn finish_page_parts(
    schema_key: &str,
    schema_fingerprint: &[u8; 32],
    record_count: u32,
    payload: Vec<u8>,
    attachments: Vec<Vec<u8>>,
) -> Result<(Vec<u8>, Vec<Vec<u8>>), Error> {
    if record_count == 0 {
        return Err(Error::Invalid("typed row page must not be empty"));
    }
    if record_count > MAX_RECORDS_PER_PAGE {
        return Err(Error::Invalid("typed row page has too many records"));
    }
    let page = encode_typed_page(schema_key, schema_fingerprint, record_count, payload)
        .map_err(Error::Page)?;
    Ok((page, attachments))
}

/// Encodes one native Schema v1 value for the column-merge protocol.
pub fn encode_value_bytes(value: &Value) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    encode_value_inline(&mut output, value)?;
    Ok(output)
}

/// Decodes one native Schema v1 value from the column-merge protocol.
pub fn decode_value_bytes(bytes: &[u8]) -> Result<Value, Error> {
    let mut reader = Reader::new(bytes);
    let value = reader.value(&mut [])?;
    reader.finish()?;
    Ok(value)
}

/// Encodes a complete native Schema v1 row for merge context reads.
pub fn encode_row_bytes(row: &Row) -> Result<Vec<u8>, Error> {
    let mut output = Vec::new();
    encode_row_inline(&mut output, row)?;
    Ok(output)
}

pub(crate) fn encoded_value_size(value: &Value) -> Result<usize, Error> {
    encoded_inline_value_size(value)
}

pub(crate) fn encoded_row_size(row: &Row) -> Result<usize, Error> {
    encoded_inline_row_size(row)
}

/// Decodes a complete native Schema v1 row from merge context bytes.
pub fn decode_row_bytes(bytes: &[u8]) -> Result<Row, Error> {
    let mut reader = Reader::new(bytes);
    let row = reader.inline_row(&mut [])?;
    reader.finish()?;
    Ok(row)
}

const NATIVE_ROW_PAYLOAD_VERSION: u8 = 2;
// A boundary value may itself contain 16 MiB. Leave room for row framing and
// for several native values while retaining the same 128 MiB aggregate ceiling
// used by accepted component transitions and durable component checkpoints.
const NATIVE_ROW_PAYLOAD_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct NativeRowPayload {
    pub schema_fingerprint: [u8; 32],
    pub row_pk: Vec<Value>,
    pub row: Row,
}

/// Encodes the durable, storage-neutral payload used by typed plugin rows.
/// This lives beside the typed wire codec so canonical storage layers do not
/// depend on the derived hot-state module.
pub fn encode_native_row_payload(
    schema_fingerprint: &[u8; 32],
    row_pk: &[Value],
    row: &Row,
) -> Result<Vec<u8>, Error> {
    if row_pk.is_empty() || row_pk.len() > MAX_KEY_COMPONENTS as usize {
        return Err(Error::Invalid(
            "typed row payload key count exceeds its bounds",
        ));
    }
    let key_bytes = row_pk.iter().try_fold(4usize, |size, value| {
        validate_key_value(value)?;
        size.checked_add(4)
            .and_then(|size| size.checked_add(encoded_inline_value_size(value).ok()?))
            .ok_or(Error::Invalid("typed row payload size overflowed"))
    })?;
    let row_bytes = encoded_inline_row_size(row)?;
    let capacity = 1usize
        .checked_add(32)
        .and_then(|size| size.checked_add(key_bytes))
        .and_then(|size| size.checked_add(4 + row_bytes))
        .ok_or(Error::Invalid("typed row payload size overflowed"))?;
    if capacity > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(NATIVE_ROW_PAYLOAD_VERSION);
    bytes.extend_from_slice(schema_fingerprint);
    append_framed_u32(
        &mut bytes,
        u32::try_from(row_pk.len()).map_err(|_| Error::Invalid("typed row key is too large"))?,
    );
    for value in row_pk {
        validate_key_value(value)?;
        append_framed_encoded(&mut bytes, |bytes| encode_value_inline(bytes, value))?;
    }
    append_framed_encoded(&mut bytes, |bytes| encode_row_inline(bytes, row))?;
    if bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    Ok(bytes)
}

fn encoded_inline_row_size(row: &Row) -> Result<usize, Error> {
    if row.len() > MAX_COLUMNS as usize {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    row.iter().try_fold(4usize, |size, (name, value)| {
        size.checked_add(4 + name.len())
            .and_then(|size| size.checked_add(encoded_inline_value_size(value).ok()?))
            .ok_or(Error::Invalid("typed row payload size overflowed"))
    })
}

fn encoded_inline_value_size(value: &Value) -> Result<usize, Error> {
    let size = match value {
        Value::Null => 1,
        Value::Text(value) => 1usize
            .checked_add(4 + value.len())
            .ok_or(Error::Invalid("typed row payload size overflowed"))?,
        Value::Uuid(_) => 17,
        Value::Int8(_) | Value::Float8(_) | Value::Timestamptz(_) => 9,
        Value::Boolean(_) => 2,
        Value::Jsonb(value) => 1usize
            .checked_add(
                4 + value
                    .binary()
                    .map_err(|error| Error::Invalid(error.0))?
                    .len(),
            )
            .ok_or(Error::Invalid("typed row payload size overflowed"))?,
    };
    Ok(size)
}

fn append_framed_encoded(
    bytes: &mut Vec<u8>,
    encode: impl FnOnce(&mut Vec<u8>) -> Result<(), Error>,
) -> Result<(), Error> {
    let frame = bytes.len();
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    if let Err(error) = encode(bytes) {
        bytes.truncate(frame);
        return Err(error);
    }
    let length = bytes.len() - frame - 4;
    let length =
        u32::try_from(length).map_err(|_| Error::Invalid("typed row frame is too large"))?;
    bytes[frame..frame + 4].copy_from_slice(&length.to_be_bytes());
    Ok(())
}

/// Decodes one durable native typed-row payload.
pub fn decode_native_row_payload(bytes: &[u8]) -> Result<NativeRowPayload, Error> {
    if bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut offset = 0usize;
    let version = take_payload_bytes(bytes, &mut offset, 1)?[0];
    if version != NATIVE_ROW_PAYLOAD_VERSION {
        return Err(Error::Message(format!(
            "unsupported typed row payload version {version}"
        )));
    }
    let schema_fingerprint: [u8; 32] = take_payload_bytes(bytes, &mut offset, 32)?
        .try_into()
        .expect("fixed fingerprint width");
    let key_count = read_payload_u32(bytes, &mut offset)? as usize;
    if key_count == 0 || key_count > MAX_KEY_COMPONENTS as usize || key_count > bytes.len() / 5 {
        return Err(Error::Invalid(
            "typed row payload key count exceeds its bounds",
        ));
    }
    let mut row_pk = Vec::with_capacity(key_count);
    for _ in 0..key_count {
        row_pk.push(decode_key_value_bytes(take_payload_frame(
            bytes,
            &mut offset,
        )?)?);
    }
    let row = decode_row_bytes(take_payload_frame(bytes, &mut offset)?)?;
    if offset != bytes.len() {
        return Err(Error::Invalid("typed row payload has trailing bytes"));
    }
    Ok(NativeRowPayload {
        schema_fingerprint,
        row_pk,
        row,
    })
}

fn append_framed_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn read_payload_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, Error> {
    Ok(u32::from_be_bytes(
        take_payload_bytes(bytes, offset, 4)?
            .try_into()
            .expect("fixed u32 width"),
    ))
}

fn take_payload_frame<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], Error> {
    let length =
        usize::try_from(read_payload_u32(bytes, offset)?).expect("u32 frame length fits usize");
    take_payload_bytes(bytes, offset, length)
}

fn take_payload_bytes<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], Error> {
    let end = offset
        .checked_add(length)
        .ok_or(Error::Invalid("typed row payload offset overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or(Error::Invalid("typed row payload is truncated"))?;
    *offset = end;
    Ok(value)
}

pub fn decode_page(bytes: &[u8]) -> Result<(String, [u8; 32], Vec<OwnedMutation>), Error> {
    decode_page_parts(bytes, Vec::new())
}

pub fn decode_page_parts(
    bytes: &[u8],
    attachments: Vec<Vec<u8>>,
) -> Result<(String, [u8; 32], Vec<OwnedMutation>), Error> {
    decode_page_parts_inner(bytes, None, attachments)
}

/// Decodes an owned component page while retaining one shared allocation for
/// inline variable-width native values.
pub fn decode_page_parts_owned(
    bytes: Vec<u8>,
    attachments: Vec<Vec<u8>>,
) -> Result<(String, [u8; 32], Vec<OwnedMutation>), Error> {
    let bytes = Arc::new(bytes);
    decode_page_parts_inner(&bytes, Some(Arc::clone(&bytes)), attachments)
}

fn decode_page_parts_inner(
    bytes: &[u8],
    owner: Option<Arc<Vec<u8>>>,
    attachments: Vec<Vec<u8>>,
) -> Result<(String, [u8; 32], Vec<OwnedMutation>), Error> {
    let page = super::Page::decode(bytes)?;
    let section = page.section()?;
    let fingerprint: [u8; 32] = section
        .schema_fingerprint
        .try_into()
        .map_err(|_| Error::Invalid("typed row page has an invalid fingerprint"))?;
    let payload_offset = section.payload.as_ptr() as usize - bytes.as_ptr() as usize;
    let mut reader = Reader::new_with_owner(section.payload, owner, payload_offset);
    let columns = reader.columns()?;
    // Even an empty create occupies a four-byte frame, one-byte operation,
    // four-byte local reference, and the page's already-decoded row layout.
    // Apply both a hard cardinality ceiling and this byte-derived minimum
    // before reserving native `OwnedMutation` storage.
    let remaining = reader.bytes.len() - reader.offset;
    if section.record_count > MAX_RECORDS_PER_PAGE || section.record_count as usize > remaining / 9
    {
        return Err(Error::Invalid(
            "typed row record count exceeds the framed payload",
        ));
    }
    let mut mutations = Vec::with_capacity(section.record_count as usize);
    let mut attachments = attachments.into_iter().map(Some).collect::<Vec<_>>();
    for _ in 0..section.record_count {
        mutations.push(reader.mutation(&mut attachments, &columns)?);
    }
    reader.finish()?;
    if attachments.iter().any(Option::is_some) {
        return Err(Error::Invalid(
            "typed row page contains an unreferenced attachment",
        ));
    }
    Ok((section.schema_key.to_owned(), fingerprint, mutations))
}

fn encode_mutation(
    output: &mut Vec<u8>,
    mutation: &Mutation<'_>,
    columns: &[Arc<str>],
    attachments: &mut Vec<Vec<u8>>,
    layout_validated: bool,
) -> Result<(), Error> {
    let start = output.len();
    output.extend_from_slice(&0_u32.to_le_bytes());
    match mutation {
        Mutation::Create { local_ref, row } => {
            output.push(2);
            output.extend_from_slice(&local_ref.to_le_bytes());
            encode_row(output, row, columns, attachments, layout_validated)?;
        }
        Mutation::Upsert {
            row_pk,
            row,
            effect,
        } => {
            output.push(0);
            encode_key(output, row_pk, attachments)?;
            output.push(match effect {
                ChangeEffect::Content => 0,
                ChangeEffect::FormatOnly => 1,
            });
            encode_row(output, row, columns, attachments, layout_validated)?;
        }
        Mutation::Delete { row_pk } => {
            output.push(1);
            encode_key(output, row_pk, attachments)?;
        }
    }
    let length = u32::try_from(output.len() - start - 4)
        .map_err(|_| Error::Invalid("typed row record exceeds u32 framing"))?;
    output[start..start + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn encode_key(
    output: &mut Vec<u8>,
    values: &[Value],
    attachments: &mut Vec<Vec<u8>>,
) -> Result<(), Error> {
    let count = u32::try_from(values.len())
        .map_err(|_| Error::Invalid("typed row primary key has too many components"))?;
    if count > MAX_KEY_COMPONENTS {
        return Err(Error::Invalid(
            "typed row primary key has too many components",
        ));
    }
    output.extend_from_slice(&count.to_le_bytes());
    for value in values {
        validate_key_value(value)?;
        encode_value(output, value, attachments)?;
    }
    Ok(())
}

fn encode_row(
    output: &mut Vec<u8>,
    row: &Row,
    columns: &[Arc<str>],
    attachments: &mut Vec<Vec<u8>>,
    layout_validated: bool,
) -> Result<(), Error> {
    if !layout_validated
        && (row.len() != columns.len()
            || row
                .keys()
                .zip(columns)
                .any(|(actual, expected)| actual != expected.as_ref()))
    {
        return Err(Error::Invalid(
            "typed rows in one page must have the same canonical columns",
        ));
    }
    for value in row.values() {
        encode_value(output, value, attachments)?;
    }
    Ok(())
}

fn page_columns(mutations: &[Mutation<'_>]) -> Result<Vec<Arc<str>>, Error> {
    let columns = mutations
        .iter()
        .find_map(|mutation| match mutation {
            Mutation::Create { row, .. } | Mutation::Upsert { row, .. } => Some(*row),
            Mutation::Delete { .. } => None,
        })
        .map_or_else(Vec::new, |row| row.shared_keys().cloned().collect());
    if columns.len() > MAX_COLUMNS as usize {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    Ok(columns)
}

fn encode_columns(output: &mut Vec<u8>, columns: &[Arc<str>]) -> Result<(), Error> {
    let count = u32::try_from(columns.len())
        .map_err(|_| Error::Invalid("typed row has too many columns"))?;
    if count > MAX_COLUMNS {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    output.extend_from_slice(&count.to_le_bytes());
    for name in columns {
        encode_text(output, name)?;
    }
    Ok(())
}

fn encode_row_inline(output: &mut Vec<u8>, row: &Row) -> Result<(), Error> {
    let count =
        u32::try_from(row.len()).map_err(|_| Error::Invalid("typed row has too many columns"))?;
    if count > MAX_COLUMNS {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    output.extend_from_slice(&count.to_le_bytes());
    for (name, value) in row {
        encode_text(output, name)?;
        encode_value_inline(output, value)?;
    }
    Ok(())
}

fn encode_value(
    output: &mut Vec<u8>,
    value: &Value,
    attachments: &mut Vec<Vec<u8>>,
) -> Result<(), Error> {
    encode_value_with_mode(output, value, attachments, true)
}

fn encode_value_inline(output: &mut Vec<u8>, value: &Value) -> Result<(), Error> {
    encode_value_with_mode(output, value, &mut Vec::new(), false)
}

fn encode_value_with_mode(
    output: &mut Vec<u8>,
    value: &Value,
    attachments: &mut Vec<Vec<u8>>,
    allow_attachments: bool,
) -> Result<(), Error> {
    match value {
        Value::Null => output.push(0),
        Value::Text(value) => {
            if value.contains('\0') {
                return Err(Error::Invalid("typed row text contains an interior NUL"));
            }
            encode_variable_value(output, 1, value.as_bytes(), attachments, allow_attachments)?;
        }
        Value::Uuid(value) => {
            output.push(2);
            output.extend_from_slice(value.as_bytes());
        }
        Value::Int8(value) => {
            output.push(3);
            output.extend_from_slice(&value.to_be_bytes());
        }
        Value::Float8(value) => {
            if !value.is_finite() {
                return Err(Error::Invalid("typed row float8 must be finite"));
            }
            output.push(4);
            output.extend_from_slice(&(*value + 0.0).to_be_bytes());
        }
        Value::Boolean(value) => {
            output.push(5);
            output.push(u8::from(*value));
        }
        Value::Jsonb(value) => {
            let length = value
                .binary_len()
                .map_err(|error| Error::Invalid(error.0))?;
            if length > MAX_TEXT_BYTES {
                return Err(Error::Invalid("typed row variable value is too large"));
            }
            if allow_attachments && length >= ATTACHMENT_THRESHOLD_BYTES {
                let value = value.binary().map_err(|error| Error::Invalid(error.0))?;
                encode_attachment(output, 6, value.into_owned(), attachments)?;
            } else {
                let checkpoint = output.len();
                output.push(6);
                output.extend_from_slice(
                    &u32::try_from(length)
                        .map_err(|_| Error::Invalid("typed row variable value is too large"))?
                        .to_le_bytes(),
                );
                if let Err(error) = value.append_binary(output) {
                    output.truncate(checkpoint);
                    return Err(Error::Invalid(error.0));
                }
            }
        }
        Value::Timestamptz(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
    Ok(())
}

pub fn encode_key_value_bytes(value: &Value) -> Result<Vec<u8>, Error> {
    validate_key_value(value)?;
    let mut output = Vec::with_capacity(encoded_inline_value_size(value)?);
    encode_value_inline(&mut output, value)?;
    Ok(output)
}

pub(crate) fn encoded_key_value_size(value: &Value) -> Result<usize, Error> {
    validate_key_value(value)?;
    encoded_inline_value_size(value)
}

pub(crate) fn append_key_value_bytes(output: &mut Vec<u8>, value: &Value) -> Result<(), Error> {
    validate_key_value(value)?;
    encode_value_inline(output, value)
}

pub fn decode_key_value_bytes(bytes: &[u8]) -> Result<Value, Error> {
    let mut reader = Reader::new(bytes);
    let value = reader.value(&mut [])?;
    reader.finish()?;
    validate_key_value(&value)?;
    Ok(value)
}

fn validate_key_value(value: &Value) -> Result<(), Error> {
    if matches!(value, Value::Text(_) | Value::Uuid(_) | Value::Int8(_)) {
        Ok(())
    } else {
        Err(Error::Invalid(
            "typed row keys accept only text, uuid, or int8 values",
        ))
    }
}

fn encode_text(output: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    encode_bytes(output, value.as_bytes())
}

fn encode_variable_value(
    output: &mut Vec<u8>,
    inline_tag: u8,
    value: &[u8],
    attachments: &mut Vec<Vec<u8>>,
    allow_attachments: bool,
) -> Result<(), Error> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(Error::Invalid("typed row variable value is too large"));
    }
    if allow_attachments && value.len() >= ATTACHMENT_THRESHOLD_BYTES {
        encode_attachment(output, inline_tag, value.to_vec(), attachments)
    } else {
        output.push(inline_tag);
        encode_bytes(output, value)
    }
}

fn encode_attachment(
    output: &mut Vec<u8>,
    inline_tag: u8,
    value: Vec<u8>,
    attachments: &mut Vec<Vec<u8>>,
) -> Result<(), Error> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(Error::Invalid("typed row variable value is too large"));
    }
    let index = u32::try_from(attachments.len())
        .map_err(|_| Error::Invalid("too many typed row attachments"))?;
    let length = u64::try_from(value.len())
        .map_err(|_| Error::Invalid("typed row attachment exceeds u64"))?;
    attachments.push(value);
    output.push(8);
    output.push(inline_tag);
    output.extend_from_slice(&index.to_le_bytes());
    output.extend_from_slice(&length.to_le_bytes());
    Ok(())
}

fn encode_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), Error> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(Error::Invalid("typed row variable value is too large"));
    }
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| Error::Invalid("typed row variable value exceeds u32 framing"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    owner: Option<Arc<Vec<u8>>>,
    owner_offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self::new_with_owner(bytes, None, 0)
    }

    fn new_with_owner(bytes: &'a [u8], owner: Option<Arc<Vec<u8>>>, owner_offset: usize) -> Self {
        Self {
            bytes,
            offset: 0,
            owner,
            owner_offset,
        }
    }

    fn exact(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(Error::Invalid("truncated typed row page"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.exact(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.exact(4)?.try_into().expect("four-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.exact(8)?.try_into().expect("eight-byte length"),
        ))
    }

    fn value(&mut self, attachments: &mut [Option<Vec<u8>>]) -> Result<Value, Error> {
        Ok(match self.u8()? {
            0 => Value::Null,
            1 => Value::Text(self.text()?),
            2 => Value::Uuid(uuid::Uuid::from_bytes(
                self.exact(16)?.try_into().expect("sixteen-byte UUID"),
            )),
            3 => Value::Int8(i64::from_be_bytes(
                self.exact(8)?.try_into().expect("eight-byte integer"),
            )),
            4 => {
                let value =
                    f64::from_be_bytes(self.exact(8)?.try_into().expect("eight-byte float"));
                if !value.is_finite() || (value == 0.0 && value.is_sign_negative()) {
                    return Err(Error::Invalid("typed row float8 is not canonical"));
                }
                Value::Float8(value)
            }
            5 => match self.u8()? {
                0 => Value::Boolean(false),
                1 => Value::Boolean(true),
                _ => return Err(Error::Invalid("typed row boolean is not canonical")),
            },
            6 => {
                let length = self.u32()? as usize;
                if length > MAX_TEXT_BYTES {
                    return Err(Error::Invalid("typed row variable value is too large"));
                }
                let start = self.offset;
                let value = self.exact(length)?;
                let jsonb = if let Some(owner) = &self.owner {
                    let start = self
                        .owner_offset
                        .checked_add(start)
                        .ok_or(Error::Invalid("typed row shared value offset overflowed"))?;
                    let end = start
                        .checked_add(length)
                        .ok_or(Error::Invalid("typed row shared value range overflowed"))?;
                    lix_schema::Jsonb::from_binary_vec_slice(Arc::clone(owner), start..end)
                } else {
                    lix_schema::Jsonb::from_binary(Arc::from(value))
                };
                Value::Jsonb(jsonb.map_err(|error| Error::Invalid(error.0))?)
            }
            8 => {
                let kind = self.u8()?;
                let index = self.u32()? as usize;
                let length = self.u64()?;
                let bytes = attachments
                    .get_mut(index)
                    .ok_or(Error::Invalid(
                        "typed row attachment index is out of bounds",
                    ))?
                    .take()
                    .ok_or(Error::Invalid(
                        "typed row attachment is referenced more than once",
                    ))?;
                if u64::try_from(bytes.len()).ok() != Some(length) {
                    return Err(Error::Invalid("typed row attachment length mismatch"));
                }
                if bytes.len() > MAX_TEXT_BYTES {
                    return Err(Error::Invalid("typed row variable value is too large"));
                }
                match kind {
                    1 => Value::Text(
                        String::from_utf8(bytes)
                            .map_err(|_| Error::Invalid("typed row text is not UTF-8"))?,
                    ),
                    6 => Value::Jsonb(
                        lix_schema::Jsonb::from_binary_vec(bytes)
                            .map_err(|error| Error::Invalid(error.0))?,
                    ),
                    _ => return Err(Error::Invalid("typed row attachment kind is invalid")),
                }
            }
            7 => Value::Timestamptz(i64::from_be_bytes(
                self.exact(8)?.try_into().expect("eight-byte timestamp"),
            )),
            _ => return Err(Error::Invalid("unknown typed row value")),
        })
    }

    fn text(&mut self) -> Result<String, Error> {
        String::from_utf8(self.bytes_value()?.to_vec())
            .map_err(|_| Error::Invalid("typed row text is not UTF-8"))
    }

    fn bytes_value(&mut self) -> Result<&'a [u8], Error> {
        let length = self.u32()? as usize;
        if length > MAX_TEXT_BYTES {
            return Err(Error::Invalid("typed row variable value is too large"));
        }
        Ok(self.exact(length)?)
    }

    fn key(&mut self, attachments: &mut [Option<Vec<u8>>]) -> Result<Vec<Value>, Error> {
        let count = self.u32()?;
        if count > MAX_KEY_COMPONENTS {
            return Err(Error::Invalid(
                "typed row primary key has too many components",
            ));
        }
        (0..count)
            .map(|_| {
                let value = self.value(attachments)?;
                validate_key_value(&value)?;
                Ok(value)
            })
            .collect()
    }

    fn columns(&mut self) -> Result<Vec<Arc<str>>, Error> {
        let count = self.u32()?;
        if count > MAX_COLUMNS {
            return Err(Error::Invalid("typed row has too many columns"));
        }
        let mut columns: Vec<Arc<str>> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = self.text()?;
            if columns
                .last()
                .is_some_and(|previous| previous.as_ref() >= name.as_str())
            {
                return Err(Error::Invalid("typed row contains a duplicate column"));
            }
            columns.push(name.into());
        }
        Ok(columns)
    }

    fn row(
        &mut self,
        attachments: &mut [Option<Vec<u8>>],
        columns: &[Arc<str>],
    ) -> Result<Row, Error> {
        let mut entries = Vec::with_capacity(columns.len());
        for name in columns {
            entries.push((Arc::clone(name), self.value(attachments)?));
        }
        Row::from_sorted_entries(entries).map_err(Error::Invalid)
    }

    fn inline_row(&mut self, attachments: &mut [Option<Vec<u8>>]) -> Result<Row, Error> {
        let count = self.u32()?;
        if count > MAX_COLUMNS {
            return Err(Error::Invalid("typed row has too many columns"));
        }
        let mut row = Row::new();
        for _ in 0..count {
            let name = self.text()?;
            if row.insert(name, self.value(attachments)?).is_some() {
                return Err(Error::Invalid("typed row contains a duplicate column"));
            }
        }
        Ok(row)
    }

    fn mutation(
        &mut self,
        attachments: &mut [Option<Vec<u8>>],
        columns: &[Arc<str>],
    ) -> Result<OwnedMutation, Error> {
        let record_length = self.u32()? as usize;
        let record_offset = self.offset;
        let record = self.exact(record_length)?;
        let mut reader = Self::new_with_owner(
            record,
            self.owner.clone(),
            self.owner_offset
                .checked_add(record_offset)
                .ok_or(Error::Invalid("typed row record offset overflowed"))?,
        );
        let mutation = match reader.u8()? {
            0 => {
                let row_pk = reader.key(attachments)?;
                let effect = match reader.u8()? {
                    0 => ChangeEffect::Content,
                    1 => ChangeEffect::FormatOnly,
                    _ => return Err(Error::Invalid("unknown typed row change effect")),
                };
                OwnedMutation::Upsert {
                    row_pk,
                    row: reader.row(attachments, columns)?,
                    effect,
                }
            }
            1 => OwnedMutation::Delete {
                row_pk: reader.key(attachments)?,
            },
            2 => OwnedMutation::Create {
                local_ref: reader.u32()?,
                row: reader.row(attachments, columns)?,
            },
            _ => return Err(Error::Invalid("unknown typed row mutation")),
        };
        reader.finish()?;
        Ok(mutation)
    }

    fn finish(&self) -> Result<(), Error> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(Error::Invalid("typed row page has trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChangeEffect, Mutation, decode_key_value_bytes, decode_page, decode_page_parts,
        encode_key_value_bytes, encode_page, encode_page_parts,
    };
    use lix_schema::{Row, Value};

    #[test]
    fn durable_payload_rejects_unbounded_key_count_before_allocation() {
        let mut payload = vec![super::NATIVE_ROW_PAYLOAD_VERSION];
        payload.extend_from_slice(&[0; 32]);
        payload.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(super::decode_native_row_payload(&payload).is_err());
    }

    #[test]
    fn durable_payload_key_count_bounds_are_symmetric() {
        let row = Row::from([("id".to_owned(), Value::Uuid(uuid::Uuid::nil()))]);
        assert!(super::encode_native_row_payload(&[1; 32], &[], &row).is_err());
        assert!(
            super::encode_native_row_payload(
                &[1; 32],
                &vec![Value::Uuid(uuid::Uuid::nil()); super::MAX_KEY_COMPONENTS as usize + 1],
                &row,
            )
            .is_err()
        );

        let mut payload = vec![super::NATIVE_ROW_PAYLOAD_VERSION];
        payload.extend_from_slice(&[0; 32]);
        payload.extend_from_slice(&0_u32.to_be_bytes());
        assert!(super::decode_native_row_payload(&payload).is_err());
    }

    #[test]
    fn durable_payload_accepts_a_maximum_sized_typed_value_with_framing() {
        let row = Row::from([(
            "body".to_owned(),
            Value::Text("x".repeat(super::MAX_TEXT_BYTES)),
        )]);
        let key = vec![Value::Uuid(uuid::Uuid::nil())];
        let (_, attachments) = encode_page_parts(
            "row",
            &[2; 32],
            &[Mutation::Upsert {
                row_pk: &key,
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].len(), super::MAX_TEXT_BYTES);

        let payload = super::encode_native_row_payload(&[2; 32], &key, &row).unwrap();

        assert!(payload.len() > super::MAX_TEXT_BYTES);
        assert_eq!(
            super::decode_native_row_payload(&payload).unwrap(),
            super::NativeRowPayload {
                schema_fingerprint: [2; 32],
                row_pk: key,
                row,
            }
        );
    }

    #[test]
    fn page_encoder_rejects_out_of_bounds_record_counts() {
        assert!(super::finish_page_parts("test", &[1; 32], 0, Vec::new(), Vec::new()).is_err());
        assert!(
            super::finish_page_parts(
                "test",
                &[1; 32],
                super::MAX_RECORDS_PER_PAGE + 1,
                Vec::new(),
                Vec::new(),
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_record_count_that_cannot_fit_in_payload() {
        let row = Row::from([("id".to_owned(), Value::Uuid(uuid::Uuid::nil()))]);
        let (mut page, attachments) = encode_page_parts(
            "test",
            &[3; 32],
            &[Mutation::Upsert {
                row_pk: &[Value::Uuid(uuid::Uuid::nil())],
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        let count_offset =
            page.len() - super::super::TRAILER_BYTES - super::super::DESCRIPTOR_BYTES + 4;
        page[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_page_parts(&page, attachments).is_err());
    }

    #[test]
    fn rejects_unreferenced_attachments() {
        let row = Row::from([("id".to_owned(), Value::Uuid(uuid::Uuid::nil()))]);
        let (page, mut attachments) = encode_page_parts(
            "test",
            &[3; 32],
            &[Mutation::Upsert {
                row_pk: &[Value::Uuid(uuid::Uuid::nil())],
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        attachments.push(b"unused".to_vec());
        assert!(decode_page_parts(&page, attachments).is_err());
    }

    #[test]
    fn rejects_repeated_attachment_references() {
        let row = Row::from([
            ("a".to_owned(), Value::Text("a".repeat(9 * 1024))),
            ("b".to_owned(), Value::Text("b".repeat(9 * 1024))),
        ]);
        let (mut page, attachments) = encode_page_parts(
            "test",
            &[3; 32],
            &[Mutation::Upsert {
                row_pk: &[Value::Text("key".to_owned())],
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        let second_reference = page
            .windows(6)
            .position(|window| window == [8, 1, 1, 0, 0, 0])
            .expect("second text attachment reference");
        page[second_reference + 2..second_reference + 6].fill(0);
        assert!(decode_page_parts(&page, attachments).is_err());
    }

    #[test]
    fn rejects_noncanonical_jsonb_value_bytes() {
        let json = br#"{"b":1,"a":2}"#;
        let mut encoded = vec![6];
        encoded.extend_from_slice(&(json.len() as u32).to_le_bytes());
        encoded.extend_from_slice(json);
        assert!(super::decode_value_bytes(&encoded).is_err());
    }

    #[test]
    fn typed_page_round_trips_native_values_and_fingerprint() {
        let id = uuid::Uuid::nil();
        let row = Row::from([
            ("enabled".to_owned(), Value::Boolean(true)),
            ("id".to_owned(), Value::Uuid(id)),
            (
                "metadata".to_owned(),
                Value::Jsonb(serde_json::json!({"b": 2, "a": 1}).into()),
            ),
            ("title".to_owned(), Value::Text("hello".to_owned())),
        ]);
        let key = vec![Value::Uuid(id)];
        let fingerprint = [7_u8; 32];
        let page = encode_page(
            "note",
            &fingerprint,
            &[Mutation::Upsert {
                row_pk: &key,
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        let (schema_key, actual_fingerprint, mutations) = decode_page(&page).unwrap();
        assert_eq!(schema_key, "note");
        assert_eq!(actual_fingerprint, fingerprint);
        assert_eq!(mutations.len(), 1);
        let super::OwnedMutation::Upsert { row: decoded, .. } = &mutations[0] else {
            panic!("expected decoded upsert");
        };
        let Value::Jsonb(metadata) = decoded.get("metadata").unwrap() else {
            panic!("expected native JSONB metadata");
        };
        assert!(metadata.is_binary());
        assert!(matches!(
            metadata.binary().unwrap(),
            std::borrow::Cow::Borrowed(_)
        ));
        assert_eq!(
            mutations[0],
            super::OwnedMutation::Upsert {
                row_pk: key,
                row,
                effect: ChangeEffect::Content,
            }
        );
    }

    #[test]
    fn typed_key_components_preserve_native_identity_values() {
        let values = [
            Value::Text("hello".to_owned()),
            Value::Int8(-42),
            Value::Uuid(uuid::Uuid::nil()),
        ];
        for value in values {
            let encoded = encode_key_value_bytes(&value).unwrap();
            assert_eq!(decode_key_value_bytes(&encoded).unwrap(), value);
        }
        assert!(encode_key_value_bytes(&Value::Boolean(true)).is_err());
        assert!(encode_key_value_bytes(&Value::Jsonb(serde_json::json!({}).into())).is_err());
    }

    #[test]
    fn merge_value_and_row_codecs_preserve_native_schema_values() {
        let row = Row::from([
            ("count".to_owned(), Value::Int8(-7)),
            (
                "payload".to_owned(),
                Value::Jsonb(serde_json::json!({"answer": 42, "ok": true}).into()),
            ),
        ]);
        let value = Value::Jsonb(serde_json::json!([1, 2, 3]).into());
        assert_eq!(
            super::decode_value_bytes(&super::encode_value_bytes(&value).unwrap()).unwrap(),
            value
        );
        assert_eq!(
            super::decode_row_bytes(&super::encode_row_bytes(&row).unwrap()).unwrap(),
            row
        );
    }

    #[test]
    fn merge_codecs_reject_trailing_bytes() {
        let mut encoded = super::encode_value_bytes(&Value::Boolean(true)).unwrap();
        encoded.push(0);
        assert!(super::decode_value_bytes(&encoded).is_err());
    }

    #[test]
    fn large_typed_values_use_page_local_attachments() {
        let id = uuid::Uuid::nil();
        let title = "x".repeat(super::ATTACHMENT_THRESHOLD_BYTES + 17);
        let metadata =
            serde_json::json!({"payload": "y".repeat(super::ATTACHMENT_THRESHOLD_BYTES + 9)});
        let row = Row::from([
            ("id".to_owned(), Value::Uuid(id)),
            ("title".to_owned(), Value::Text(title)),
            ("metadata".to_owned(), Value::Jsonb(metadata.into())),
        ]);
        let key = vec![Value::Uuid(id)];
        let fingerprint = [9_u8; 32];
        let (page, attachments) = encode_page_parts(
            "note",
            &fingerprint,
            &[Mutation::Upsert {
                row_pk: &key,
                row: &row,
                effect: ChangeEffect::Content,
            }],
        )
        .unwrap();
        assert_eq!(attachments.len(), 2);
        assert!(
            attachments
                .iter()
                .all(|attachment| attachment.len() > 8 * 1024)
        );
        let title_attachment_pointer = attachments
            .iter()
            .find(|attachment| attachment.first() == Some(&b'x'))
            .expect("title attachment")
            .as_ptr();
        let metadata_attachment_pointer = attachments
            .iter()
            .find(|attachment| attachment.first() != Some(&b'x'))
            .expect("metadata attachment")
            .as_ptr();
        let (_, actual_fingerprint, mutations) = decode_page_parts(&page, attachments).unwrap();
        assert_eq!(actual_fingerprint, fingerprint);
        let super::OwnedMutation::Upsert { row: decoded, .. } = &mutations[0] else {
            panic!("expected decoded upsert");
        };
        let Value::Text(decoded_title) = decoded.get("title").unwrap() else {
            panic!("expected decoded title");
        };
        assert_eq!(
            decoded_title.as_ptr(),
            title_attachment_pointer,
            "text attachment allocation must move directly into the typed value"
        );
        let Value::Jsonb(decoded_metadata) = decoded.get("metadata").unwrap() else {
            panic!("expected decoded metadata");
        };
        assert_eq!(
            decoded_metadata.binary().unwrap().as_ptr(),
            metadata_attachment_pointer,
            "JSONB attachment allocation must move directly into the typed value"
        );
        assert_eq!(
            mutations[0],
            super::OwnedMutation::Upsert {
                row_pk: key,
                row,
                effect: ChangeEffect::Content,
            }
        );
        assert!(
            decode_page(&page).is_err(),
            "attachment references cannot be decoded without the page table"
        );
    }
}
