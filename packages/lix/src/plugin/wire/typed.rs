//! Schema v1 typed-row page records.
//!
//! A typed page carries one schema key and its exact 32-byte Schema v1
//! fingerprint in the envelope. Records contain typed primary-key values and
//! typed column values; there is no outer row object to parse or canonicalize.

use std::mem::size_of_val;
use std::ops::Range;
use std::sync::Arc;

use lix_schema::{Row, Value};

use super::{Error as PageError, encode_typed_page};

#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
type NativePayloadOwner = bytes::Bytes;
#[cfg(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2"))]
type NativePayloadOwner = Arc<[u8]>;

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

pub(crate) const NATIVE_ROW_PAYLOAD_VERSION: u8 = 2;
pub(crate) const ENGINE_ROW_PAYLOAD_VERSION: u8 = 3;
/// Protocol-v69 storage payload. The outer durable row authenticates schema
/// key and primary key, so repeating the primary-key values here is both
/// redundant and expensive for long string identities.
pub(crate) const STORAGE_ROW_PAYLOAD_VERSION: u8 = 5;
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

/// Borrowed scalar view used by the SQL projection path. Durable native
/// payloads never contain attachments, so variable-width values can remain
/// slices of the authenticated payload while every value is still validated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum BorrowedNativeValue<'a> {
    Null,
    Text(&'a str),
    Uuid(uuid::Uuid),
    Int8(i64),
    Float8(f64),
    Boolean(bool),
    Jsonb(&'a str),
    Timestamptz(i64),
}

/// Exact durable native-row bytes whose complete wire envelope and canonical
/// scalar encodings have already been validated. The bytes are private so a
/// certified visitor can never be invoked with an unchecked slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedNativePayload {
    bytes: NativePayloadOwner,
    range: Range<usize>,
}

impl ValidatedNativePayload {
    pub(crate) fn try_new(bytes: NativePayloadOwner) -> Result<Self, Error> {
        let length = bytes.len();
        Self::try_new_range(bytes, 0..length)
    }

    pub(crate) fn try_new_range(
        bytes: NativePayloadOwner,
        range: Range<usize>,
    ) -> Result<Self, Error> {
        let payload = bytes.get(range.clone()).ok_or(Error::Invalid(
            "validated native payload range is out of bounds",
        ))?;
        visit_native_row_payload(payload, |_, _| {}, |_, _| {})?;
        Ok(Self { bytes, range })
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes[self.range.clone()]
    }

    pub(crate) fn owner_len(&self) -> usize {
        self.bytes.len()
    }
}

/// Logical scalar kind certified by the native wire validator. `Null` is
/// represented separately because its declared kind comes from Schema v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CertifiedNativeScalarKind {
    Text,
    Uuid,
    Int8,
    Float8,
    Boolean,
    Jsonb,
    Timestamptz,
}

#[derive(Debug, Clone, PartialEq)]
enum CertifiedNativeValue {
    Null,
    Text(Range<usize>),
    Uuid(uuid::Uuid),
    Int8(i64),
    Float8(f64),
    Boolean(bool),
    Jsonb(Range<usize>),
    Timestamptz(i64),
}

impl CertifiedNativeValue {
    fn from_borrowed(owner: &[u8], value: BorrowedNativeValue<'_>) -> Result<Self, Error> {
        Ok(match value {
            BorrowedNativeValue::Null => Self::Null,
            BorrowedNativeValue::Text(value) => {
                Self::Text(certified_subslice_range(owner, value.as_bytes())?)
            }
            BorrowedNativeValue::Uuid(value) => Self::Uuid(value),
            BorrowedNativeValue::Int8(value) => Self::Int8(value),
            BorrowedNativeValue::Float8(value) => Self::Float8(value),
            BorrowedNativeValue::Boolean(value) => Self::Boolean(value),
            BorrowedNativeValue::Jsonb(value) => {
                Self::Jsonb(certified_subslice_range(owner, value.as_bytes())?)
            }
            BorrowedNativeValue::Timestamptz(value) => Self::Timestamptz(value),
        })
    }

    fn kind(&self) -> Option<CertifiedNativeScalarKind> {
        Some(match self {
            Self::Null => return None,
            Self::Text(_) => CertifiedNativeScalarKind::Text,
            Self::Uuid(_) => CertifiedNativeScalarKind::Uuid,
            Self::Int8(_) => CertifiedNativeScalarKind::Int8,
            Self::Float8(_) => CertifiedNativeScalarKind::Float8,
            Self::Boolean(_) => CertifiedNativeScalarKind::Boolean,
            Self::Jsonb(_) => CertifiedNativeScalarKind::Jsonb,
            Self::Timestamptz(_) => CertifiedNativeScalarKind::Timestamptz,
        })
    }

    fn borrow<'a>(&self, owner: &'a [u8]) -> BorrowedNativeValue<'a> {
        match self {
            Self::Null => BorrowedNativeValue::Null,
            Self::Text(range) => {
                let bytes = &owner[range.clone()];
                // SAFETY: this private locator is created only from `&str`
                // returned by the full native wire validator, and `owner` is
                // retained immutably by the certificate.
                BorrowedNativeValue::Text(unsafe { std::str::from_utf8_unchecked(bytes) })
            }
            Self::Uuid(value) => BorrowedNativeValue::Uuid(*value),
            Self::Int8(value) => BorrowedNativeValue::Int8(*value),
            Self::Float8(value) => BorrowedNativeValue::Float8(*value),
            Self::Boolean(value) => BorrowedNativeValue::Boolean(*value),
            Self::Jsonb(range) => {
                let bytes = &owner[range.clone()];
                // SAFETY: identical to the text case; full validation also
                // proves the retained JSONB text is canonical UTF-8.
                BorrowedNativeValue::Jsonb(unsafe { std::str::from_utf8_unchecked(bytes) })
            }
            Self::Timestamptz(value) => BorrowedNativeValue::Timestamptz(*value),
        }
    }

    fn semantic_eq(&self, other: &Self, owner: &[u8]) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Text(left), Self::Text(right)) | (Self::Jsonb(left), Self::Jsonb(right)) => {
                owner[left.clone()] == owner[right.clone()]
            }
            (Self::Uuid(left), Self::Uuid(right)) => left == right,
            (Self::Int8(left), Self::Int8(right))
            | (Self::Timestamptz(left), Self::Timestamptz(right)) => left == right,
            (Self::Float8(left), Self::Float8(right)) => left.to_bits() == right.to_bits(),
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            _ => false,
        }
    }
}

fn certified_subslice_range(owner: &[u8], value: &[u8]) -> Result<Range<usize>, Error> {
    let owner_start = owner.as_ptr() as usize;
    let value_start = value.as_ptr() as usize;
    let start = value_start
        .checked_sub(owner_start)
        .ok_or(Error::Invalid("certified scalar is outside its owner"))?;
    let end = start
        .checked_add(value.len())
        .ok_or(Error::Invalid("certified scalar range overflowed"))?;
    if end > owner.len() {
        return Err(Error::Invalid("certified scalar is outside its owner"));
    }
    Ok(start..end)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CertifiedNativeFieldShape {
    name: Box<str>,
    observed_non_null_kind: Option<CertifiedNativeScalarKind>,
    saw_null: bool,
}

impl CertifiedNativeFieldShape {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn observed_non_null_kind(&self) -> Option<CertifiedNativeScalarKind> {
        self.observed_non_null_kind
    }

    pub(crate) fn saw_null(&self) -> bool {
        self.saw_null
    }
}

/// Projection index derived by fully validating each exact native payload in
/// one immutable segment. This proves wire shape only; storage must separately
/// bind payload keys to authenticated leaf identities before exposing the
/// locator path to SQL.
#[derive(Debug, Clone)]
pub(crate) struct CertifiedNativeProjectionSegment {
    owner: NativePayloadOwner,
    payload_ranges: Box<[Range<u32>]>,
    schema_fingerprint: [u8; 32],
    key_kinds: Box<[CertifiedNativeScalarKind]>,
    fields: Box<[CertifiedNativeFieldShape]>,
    keys: Box<[CertifiedNativeValue]>,
    values: Box<[CertifiedNativeValue]>,
    key_field_equal: Box<[bool]>,
    row_count: usize,
}

impl CertifiedNativeProjectionSegment {
    pub(crate) fn schema_fingerprint(&self) -> [u8; 32] {
        self.schema_fingerprint
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn key_kinds(&self) -> &[CertifiedNativeScalarKind] {
        &self.key_kinds
    }

    pub(crate) fn fields(&self) -> &[CertifiedNativeFieldShape] {
        &self.fields
    }

    pub(crate) fn key_equals_field(&self, key_ordinal: usize, field_ordinal: usize) -> bool {
        self.key_field_equal
            .get(key_ordinal.saturating_mul(self.fields.len()) + field_ordinal)
            .copied()
            .unwrap_or(false)
    }

    pub(crate) fn key_value(
        &self,
        row_ordinal: usize,
        key_ordinal: usize,
    ) -> Option<BorrowedNativeValue<'_>> {
        self.keys
            .get(row_ordinal.checked_mul(self.key_kinds.len())? + key_ordinal)
            .map(|value| value.borrow(&self.owner))
    }

    pub(crate) fn field_value(
        &self,
        row_ordinal: usize,
        field_ordinal: usize,
    ) -> Option<BorrowedNativeValue<'_>> {
        self.values
            .get(row_ordinal.checked_mul(self.fields.len())? + field_ordinal)
            .map(|value| value.borrow(&self.owner))
    }

    fn payload_range(&self, row_ordinal: usize) -> Option<Range<usize>> {
        let range = self.payload_ranges.get(row_ordinal)?;
        Some(
            usize::try_from(range.start).expect("u32 payload offset fits usize")
                ..usize::try_from(range.end).expect("u32 payload offset fits usize"),
        )
    }

    pub(crate) fn payload_bytes(&self, row_ordinal: usize) -> Option<&[u8]> {
        self.owner.get(self.payload_range(row_ordinal)?)
    }

    /// Materializes an owned proof only for callers which miss the direct
    /// certified projection. The normal path retains one owner and compact
    /// ranges instead of one ref-counted owner handle per row.
    pub(crate) fn validated_payload_owned(
        &self,
        row_ordinal: usize,
    ) -> Option<ValidatedNativePayload> {
        Some(ValidatedNativePayload {
            bytes: self.owner.clone(),
            range: self.payload_range(row_ordinal)?,
        })
    }

    pub(crate) fn owner_len(&self) -> usize {
        self.owner.len()
    }

    pub(crate) fn has_same_layout(&self, other: &Self) -> bool {
        self.schema_fingerprint == other.schema_fingerprint
            && self.key_kinds == other.key_kinds
            && self.fields == other.fields
            && self.key_field_equal == other.key_field_equal
    }

    /// Heap bytes unique to the locator/layout proof. The immutable owner is
    /// accounted by the decoded payload index.
    pub(crate) fn locator_resident_bytes(&self) -> usize {
        size_of_val(self.payload_ranges.as_ref())
            .saturating_add(size_of_val(self.key_kinds.as_ref()))
            .saturating_add(size_of_val(self.fields.as_ref()))
            .saturating_add(
                self.fields
                    .iter()
                    .map(|field| field.name.len())
                    .sum::<usize>(),
            )
            .saturating_add(size_of_val(self.keys.as_ref()))
            .saturating_add(size_of_val(self.values.as_ref()))
            .saturating_add(size_of_val(self.key_field_equal.as_ref()))
    }
}

/// Fully validates native payload ranges once and records direct scalar
/// locators. No schema or storage-envelope authority is inferred here.
pub(crate) fn certify_native_projection_segment(
    owner: NativePayloadOwner,
    payload_ranges: Box<[Range<u32>]>,
) -> Result<CertifiedNativeProjectionSegment, Error> {
    use std::cell::Cell;

    let row_capacity = payload_ranges.len();
    let mut schema_fingerprint = None;
    let mut key_kinds = Vec::new();
    let mut fields = Vec::<CertifiedNativeFieldShape>::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut key_field_equal = Vec::new();
    for (row_ordinal, compact_range) in payload_ranges.iter().enumerate() {
        let range = usize::try_from(compact_range.start).expect("u32 payload offset fits usize")
            ..usize::try_from(compact_range.end).expect("u32 payload offset fits usize");
        let payload = owner.get(range).ok_or(Error::Invalid(
            "validated native payload range is out of bounds",
        ))?;
        let key_start = keys.len();
        let value_start = values.len();
        let invalid = Cell::new(None::<&'static str>);
        let fingerprint = visit_native_row_payload(
            payload,
            |index, value| {
                let Ok(value) = CertifiedNativeValue::from_borrowed(&owner, value) else {
                    invalid.set(Some("native key locator is outside its owner"));
                    return;
                };
                let Some(kind) = value.kind() else {
                    invalid.set(Some("native primary key is null"));
                    return;
                };
                if row_ordinal == 0 {
                    key_kinds.push(kind);
                } else if key_kinds.get(index) != Some(&kind) {
                    invalid.set(Some("native primary-key layout differs between rows"));
                }
                keys.push(value);
            },
            |name, value| {
                let field_ordinal = values.len().saturating_sub(value_start);
                let Ok(value) = CertifiedNativeValue::from_borrowed(&owner, value) else {
                    invalid.set(Some("native field locator is outside its owner"));
                    return;
                };
                if row_ordinal == 0 {
                    fields.push(CertifiedNativeFieldShape {
                        name: name.into(),
                        observed_non_null_kind: value.kind(),
                        saw_null: value.kind().is_none(),
                    });
                } else if let Some(field) = fields.get_mut(field_ordinal) {
                    if field.name.as_ref() != name {
                        invalid.set(Some("native field layout differs between rows"));
                    }
                    match value.kind() {
                        Some(kind) => match field.observed_non_null_kind {
                            Some(expected) if expected != kind => {
                                invalid.set(Some("native field scalar kind differs between rows"))
                            }
                            None => field.observed_non_null_kind = Some(kind),
                            _ => {}
                        },
                        None => field.saw_null = true,
                    }
                } else {
                    invalid.set(Some("native field count differs between rows"));
                }
                values.push(value);
            },
        )?;
        if let Some(message) = invalid.get() {
            return Err(Error::Invalid(message));
        }
        let row_key_count = keys.len().saturating_sub(key_start);
        let row_field_count = values.len().saturating_sub(value_start);
        if row_ordinal > 0 && (row_key_count != key_kinds.len() || row_field_count != fields.len())
        {
            return Err(Error::Invalid(
                "native row layout differs between segment members",
            ));
        }
        if row_ordinal == 0 {
            let remaining = row_capacity.saturating_sub(1);
            keys.reserve(remaining.saturating_mul(row_key_count));
            values.reserve(remaining.saturating_mul(row_field_count));
        }
        if let Some(expected) = schema_fingerprint {
            if expected != fingerprint {
                return Err(Error::Invalid(
                    "native schema fingerprint differs between segment members",
                ));
            }
        } else {
            schema_fingerprint = Some(fingerprint);
            key_field_equal.resize(row_key_count.saturating_mul(row_field_count), true);
        }
        for (key_ordinal, key) in keys[key_start..].iter().enumerate() {
            let row_fields = &values[value_start..];
            for (field_ordinal, field) in row_fields.iter().enumerate() {
                if !key.semantic_eq(field, &owner) {
                    key_field_equal[key_ordinal * fields.len() + field_ordinal] = false;
                }
            }
        }
    }
    let row_count = payload_ranges.len();
    let schema_fingerprint = schema_fingerprint.ok_or(Error::Invalid(
        "native projection segment must contain at least one row",
    ))?;
    Ok(CertifiedNativeProjectionSegment {
        owner,
        payload_ranges,
        schema_fingerprint,
        key_kinds: key_kinds.into_boxed_slice(),
        fields: fields.into_boxed_slice(),
        keys: keys.into_boxed_slice(),
        values: values.into_boxed_slice(),
        key_field_equal: key_field_equal.into_boxed_slice(),
        row_count,
    })
}

/// Validates and streams a durable native row without constructing an owned
/// primary-key vector or `lix_schema::Row`. Callbacks borrow directly from the
/// payload and may select only the values needed by their consumer.
pub(crate) fn visit_native_row_payload<'a>(
    bytes: &'a [u8],
    visit_key: impl FnMut(usize, BorrowedNativeValue<'a>),
    visit_field: impl FnMut(&'a str, BorrowedNativeValue<'a>),
) -> Result<[u8; 32], Error> {
    visit_native_row_payload_with_validation(
        bytes,
        NativePayloadValidation::Full,
        visit_key,
        visit_field,
    )
}

/// Streams a payload whose full native wire was validated unchanged by
/// [`ValidatedNativePayload::try_new`]. Schema and storage-envelope binding
/// remain consumer responsibilities and are intentionally not certified here.
pub(crate) fn visit_validated_native_row_payload<'a>(
    payload: &'a ValidatedNativePayload,
    visit_key: impl FnMut(usize, BorrowedNativeValue<'a>),
    visit_field: impl FnMut(&'a str, BorrowedNativeValue<'a>),
) -> Result<[u8; 32], Error> {
    visit_native_row_payload_with_validation(
        payload.as_bytes(),
        NativePayloadValidation::Certified,
        visit_key,
        visit_field,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NativePayloadValidation {
    Full,
    Certified,
}

fn visit_native_row_payload_with_validation<'a>(
    bytes: &'a [u8],
    validation: NativePayloadValidation,
    mut visit_key: impl FnMut(usize, BorrowedNativeValue<'a>),
    mut visit_field: impl FnMut(&'a str, BorrowedNativeValue<'a>),
) -> Result<[u8; 32], Error> {
    if validation == NativePayloadValidation::Full && bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut offset = 0usize;
    let version = take_payload_bytes(bytes, &mut offset, 1)?[0];
    if validation == NativePayloadValidation::Full
        && !matches!(
            version,
            NATIVE_ROW_PAYLOAD_VERSION | STORAGE_ROW_PAYLOAD_VERSION
        )
    {
        return Err(Error::Message(format!(
            "unsupported typed row payload version {version}"
        )));
    }
    let schema_fingerprint = take_payload_bytes(bytes, &mut offset, 32)?
        .try_into()
        .expect("fixed fingerprint width");
    if version == NATIVE_ROW_PAYLOAD_VERSION {
        let key_count = read_payload_u32(bytes, &mut offset)? as usize;
        if validation == NativePayloadValidation::Full
            && (key_count == 0
                || key_count > MAX_KEY_COMPONENTS as usize
                || key_count > bytes.len() / 5)
        {
            return Err(Error::Invalid(
                "typed row payload key count exceeds its bounds",
            ));
        }
        for index in 0..key_count {
            let frame = take_payload_frame(bytes, &mut offset)?;
            let mut reader = Reader::new(frame);
            let value = reader.borrowed_value_with_validation(validation)?;
            if validation == NativePayloadValidation::Full {
                reader.finish()?;
            }
            if validation == NativePayloadValidation::Full
                && !matches!(
                    value,
                    BorrowedNativeValue::Text(_)
                        | BorrowedNativeValue::Uuid(_)
                        | BorrowedNativeValue::Int8(_)
                )
            {
                return Err(Error::Invalid(
                    "typed row keys accept only text, uuid, or int8 values",
                ));
            }
            visit_key(index, value);
        }
    }

    let row_frame = take_payload_frame(bytes, &mut offset)?;
    if validation == NativePayloadValidation::Full && offset != bytes.len() {
        return Err(Error::Invalid("typed row payload has trailing bytes"));
    }
    let mut reader = Reader::new(row_frame);
    let field_count = reader.u32()?;
    if validation == NativePayloadValidation::Full && field_count > MAX_COLUMNS {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    let mut previous_name = None;
    for _ in 0..field_count {
        let name = reader.borrowed_text_with_validation(validation)?;
        if validation == NativePayloadValidation::Full
            && previous_name.is_some_and(|previous| previous >= name)
        {
            return Err(Error::Invalid("typed row contains a duplicate column"));
        }
        let value = reader.borrowed_value_with_validation(validation)?;
        visit_field(name, value);
        previous_name = Some(name);
    }
    if validation == NativePayloadValidation::Full {
        reader.finish()?;
    }
    Ok(schema_fingerprint)
}

/// Encodes the protocol-v69 durable payload used by typed plugin rows.
/// Schema key and primary key remain in the authenticated outer record; the
/// payload contains the fingerprint and complete typed row exactly once.
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
    for value in row_pk {
        validate_key_value(value)?;
    }
    let row_bytes = estimated_inline_row_size(row)?;
    let capacity = 1usize
        .checked_add(32)
        .and_then(|size| size.checked_add(4 + row_bytes))
        .ok_or(Error::Invalid("typed row payload size overflowed"))?;
    if capacity > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(STORAGE_ROW_PAYLOAD_VERSION);
    bytes.extend_from_slice(schema_fingerprint);
    append_framed_encoded(&mut bytes, |bytes| encode_row_inline(bytes, row))?;
    if bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    Ok(bytes)
}

/// Encodes the legacy self-contained v2 payload for temporary in-memory/test
/// carriers that have no separate durable identity envelope. Protocol-v69
/// storage must use [`encode_native_row_payload`] instead.
pub(crate) fn encode_native_row_payload_with_identity(
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
    let row_bytes = estimated_inline_row_size(row)?;
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
        append_framed_encoded(&mut bytes, |bytes| encode_value_inline(bytes, value))?;
    }
    append_framed_encoded(&mut bytes, |bytes| encode_row_inline(bytes, row))?;
    Ok(bytes)
}

/// Encodes the exact row layout certified by the SQL `path`/`value`
/// replacement path without allocating a general-purpose [`Row`] or its
/// repeated column-name owners.
#[cfg(test)]
fn encode_native_path_value_payload(
    schema_fingerprint: &[u8; 32],
    path: &str,
    value: &lix_schema::Jsonb,
) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    append_native_path_value_payload(&mut bytes, schema_fingerprint, path, value)?;
    Ok(bytes)
}

pub(crate) fn append_native_path_value_payload(
    bytes: &mut Vec<u8>,
    schema_fingerprint: &[u8; 32],
    path: &str,
    value: &lix_schema::Jsonb,
) -> Result<(), Error> {
    append_native_path_value_payload_with(bytes, schema_fingerprint, path, |bytes| {
        append_jsonb_inline(bytes, value)
    })
}

/// Appends the certified path/value layout when the SQL parameter already is
/// canonical compact JSON. The canonical validator supplies the same scalar
/// contract as `Jsonb`; retaining the borrowed bytes avoids constructing a
/// serde DOM only to serialize it back into the durable typed payload.
/// Attempts the allocation-free canonical parameter path. `Ok(false)` leaves
/// the destination unchanged and lets SQL fall back to parsing and
/// canonicalizing otherwise-valid JSON input.
pub(crate) fn try_append_native_path_value_payload_from_canonical_json(
    bytes: &mut Vec<u8>,
    schema_fingerprint: &[u8; 32],
    path: &str,
    canonical_json: &[u8],
) -> Result<bool, Error> {
    if lix_schema::validate_canonical_json_text(canonical_json).is_err() {
        return Ok(false);
    }
    append_native_path_value_payload_with(bytes, schema_fingerprint, path, |bytes| {
        append_canonical_json_inline(bytes, canonical_json)
    })?;
    Ok(true)
}

fn append_native_path_value_payload_with(
    bytes: &mut Vec<u8>,
    schema_fingerprint: &[u8; 32],
    path: &str,
    append_value: impl FnOnce(&mut Vec<u8>) -> Result<(), Error>,
) -> Result<(), Error> {
    if path.contains('\0') || path.len() > MAX_TEXT_BYTES {
        return Err(Error::Invalid("typed row text is invalid or too large"));
    }
    let additional = 1usize
        .saturating_add(32)
        .saturating_add(4 + 4 + 4 + 5 + path.len() + 32);
    let checkpoint = bytes.len();
    bytes.reserve(additional);
    let result = (|| {
        bytes.push(STORAGE_ROW_PAYLOAD_VERSION);
        bytes.extend_from_slice(schema_fingerprint);
        append_framed_encoded(bytes, |bytes| {
            bytes.extend_from_slice(&2_u32.to_le_bytes());
            encode_text(bytes, "path")?;
            bytes.push(1);
            encode_bytes(bytes, path.as_bytes())?;
            encode_text(bytes, "value")?;
            append_value(bytes)
        })
    })();
    if let Err(error) = result {
        bytes.truncate(checkpoint);
        return Err(error);
    }
    if bytes.len().saturating_sub(checkpoint) > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        bytes.truncate(checkpoint);
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    Ok(())
}

/// Encodes an engine-owned row whose storage envelope already authenticates
/// schema key and primary key. Built-in schema compilation supplies the
/// fingerprint and canonical column layout during decode, so neither identity
/// nor column names are duplicated in every durable row.
pub(crate) fn encode_engine_row_payload(
    schema: &lix_schema::CompiledSchema,
    row: &Row,
) -> Result<Vec<u8>, Error> {
    schema.validate_complete_row(row).map_err(|error| {
        Error::Message(format!(
            "engine typed row does not satisfy its schema: {error}"
        ))
    })?;
    let column_count = schema.canonical_columns().len();
    if row.len() != column_count || column_count > MAX_COLUMNS as usize {
        return Err(Error::Invalid(
            "engine typed row does not match its canonical column layout",
        ));
    }
    let capacity = schema
        .canonical_columns()
        .try_fold(5usize, |size, column| {
            let value = row.get(column).ok_or(Error::Invalid(
                "engine typed row is missing a canonical column",
            ))?;
            size.checked_add(estimated_inline_value_size(value)?)
                .ok_or(Error::Invalid("typed row payload size overflowed"))
        })?;
    if capacity > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(ENGINE_ROW_PAYLOAD_VERSION);
    bytes.extend_from_slice(
        &u32::try_from(column_count)
            .map_err(|_| Error::Invalid("too many typed columns"))?
            .to_le_bytes(),
    );
    for column in schema.canonical_columns() {
        encode_value_inline(
            &mut bytes,
            row.get(column)
                .expect("engine row layout was checked before encoding"),
        )?;
    }
    Ok(bytes)
}

pub(crate) fn decode_engine_row_payload(
    bytes: &[u8],
    schema: &lix_schema::CompiledSchema,
) -> Result<Row, Error> {
    if bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != ENGINE_ROW_PAYLOAD_VERSION {
        return Err(Error::Message(format!(
            "unsupported engine typed row payload version {version}"
        )));
    }
    let column_count = reader.u32()? as usize;
    let schema_column_count = schema.canonical_columns().len();
    if column_count > schema_column_count {
        return Err(Error::Invalid(
            "engine typed row column count does not match its schema",
        ));
    }
    let mut row = Row::with_capacity(schema_column_count);
    for column in schema.canonical_columns().take(column_count) {
        row.insert(column, reader.value(&mut [])?);
    }
    reader.finish()?;
    schema.materialize_missing_nullable_columns(&mut row);
    schema.validate_complete_row(&row).map_err(|error| {
        Error::Message(format!(
            "decoded engine typed row does not satisfy its schema: {error}"
        ))
    })?;
    Ok(row)
}

/// Validates and streams one compact built-in engine row without constructing
/// an owned [`Row`]. Column names come from the compiled built-in schema; the
/// payload contains only canonical values in that exact order.
pub(crate) fn visit_engine_row_payload<'a>(
    bytes: &'a [u8],
    schema: &lix_schema::CompiledSchema,
    mut visit_field: impl FnMut(&str, BorrowedNativeValue<'a>),
) -> Result<(), Error> {
    if bytes.len() > NATIVE_ROW_PAYLOAD_MAX_BYTES {
        return Err(Error::Invalid("typed row payload exceeds its size limit"));
    }
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != ENGINE_ROW_PAYLOAD_VERSION {
        return Err(Error::Message(format!(
            "unsupported engine typed row payload version {version}"
        )));
    }
    let column_count = reader.u32()? as usize;
    let schema_column_count = schema.canonical_columns().len();
    if column_count > schema_column_count {
        return Err(Error::Invalid(
            "engine typed row column count does not match its schema",
        ));
    }
    for column in schema.canonical_columns().take(column_count) {
        visit_field(column, reader.borrowed_value()?);
    }
    reader.finish()?;
    for column in schema.canonical_columns().skip(column_count) {
        if schema.column_nullable(column) != Some(true) {
            return Err(Error::Invalid(
                "engine typed row column count does not match its schema",
            ));
        }
        visit_field(column, BorrowedNativeValue::Null);
    }
    Ok(())
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

fn estimated_inline_row_size(row: &Row) -> Result<usize, Error> {
    if row.len() > MAX_COLUMNS as usize {
        return Err(Error::Invalid("typed row has too many columns"));
    }
    row.iter().try_fold(4usize, |size, (name, value)| {
        size.checked_add(4 + name.len())
            .and_then(|size| size.checked_add(estimated_inline_value_size(value).ok()?))
            .ok_or(Error::Invalid("typed row payload size overflowed"))
    })
}

fn estimated_inline_value_size(value: &Value) -> Result<usize, Error> {
    let variable = match value {
        Value::Jsonb(value) => Some(value.estimated_binary_size() as usize),
        Value::Text(value) => Some(value.len()),
        _ => None,
    };
    if let Some(length) = variable {
        return 5usize
            .checked_add(length)
            .ok_or(Error::Invalid("typed row payload size overflowed"));
    }
    Ok(match value {
        Value::Null => 1,
        Value::Uuid(_) => 17,
        Value::Int8(_) | Value::Float8(_) | Value::Timestamptz(_) => 9,
        Value::Boolean(_) => 2,
        Value::Text(_) | Value::Jsonb(_) => unreachable!("variable values returned above"),
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
        Value::Jsonb(value) => {
            let mut canonical = Vec::with_capacity(value.estimated_binary_size() as usize);
            value
                .append_canonical_json(&mut canonical)
                .map_err(|error| Error::Invalid(error.0))?;
            1usize
                .checked_add(4 + canonical.len())
                .ok_or(Error::Invalid("typed row payload size overflowed"))?
        }
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
    if !matches!(
        version,
        NATIVE_ROW_PAYLOAD_VERSION | STORAGE_ROW_PAYLOAD_VERSION
    ) {
        return Err(Error::Message(format!(
            "unsupported typed row payload version {version}"
        )));
    }
    let schema_fingerprint: [u8; 32] = take_payload_bytes(bytes, &mut offset, 32)?
        .try_into()
        .expect("fixed fingerprint width");
    let mut row_pk = Vec::new();
    if version == NATIVE_ROW_PAYLOAD_VERSION {
        let key_count = read_payload_u32(bytes, &mut offset)? as usize;
        if key_count == 0 || key_count > MAX_KEY_COMPONENTS as usize || key_count > bytes.len() / 5
        {
            return Err(Error::Invalid(
                "typed row payload key count exceeds its bounds",
            ));
        }
        row_pk.reserve(key_count);
        for _ in 0..key_count {
            row_pk.push(decode_key_value_bytes(take_payload_frame(
                bytes,
                &mut offset,
            )?)?);
        }
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
            let checkpoint = output.len();
            append_jsonb_inline(output, value)?;
            let length = output.len() - checkpoint - 5;
            if allow_attachments && length >= ATTACHMENT_THRESHOLD_BYTES {
                let canonical = output.split_off(checkpoint + 5);
                output.truncate(checkpoint);
                encode_attachment(output, 6, canonical, attachments)?;
            }
        }
        Value::Timestamptz(value) => {
            output.push(7);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
    Ok(())
}

/// Appends tag 6 and canonical compact JSON text, backpatching its u32 length.
/// This deliberately writes into the destination so durable SQL updates do
/// not allocate an intermediary `String` (or JSON DOM) per value.
fn append_jsonb_inline(output: &mut Vec<u8>, value: &lix_schema::Jsonb) -> Result<(), Error> {
    let checkpoint = output.len();
    output.push(6);
    output.extend_from_slice(&0_u32.to_le_bytes());
    if let Err(error) = value.append_canonical_json(output) {
        output.truncate(checkpoint);
        return Err(Error::Invalid(error.0));
    }
    let length = output.len() - checkpoint - 5;
    if length > MAX_TEXT_BYTES {
        output.truncate(checkpoint);
        return Err(Error::Invalid("typed row variable value is too large"));
    }
    let length = u32::try_from(length)
        .map_err(|_| Error::Invalid("typed row variable value is too large"))?;
    output[checkpoint + 1..checkpoint + 5].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn append_canonical_json_inline(output: &mut Vec<u8>, value: &[u8]) -> Result<(), Error> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(Error::Invalid("typed row variable value is too large"));
    }
    output.push(6);
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| Error::Invalid("typed row variable value is too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value);
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
                let start = self.owner_offset + self.offset;
                let value = self.exact(length)?;
                let jsonb = if let Some(owner) = self.owner.as_ref() {
                    lix_schema::Jsonb::from_canonical_text_vec_slice(
                        Arc::clone(owner),
                        start..start + length,
                    )
                } else {
                    lix_schema::Jsonb::from_canonical_text_vec(value.to_vec())
                }
                .map_err(|error| Error::Invalid(error.0))?;
                Value::Jsonb(jsonb)
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
                        lix_schema::Jsonb::from_canonical_text_vec(bytes)
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

    fn borrowed_value(&mut self) -> Result<BorrowedNativeValue<'a>, Error> {
        self.borrowed_value_with_validation(NativePayloadValidation::Full)
    }

    fn borrowed_value_with_validation(
        &mut self,
        validation: NativePayloadValidation,
    ) -> Result<BorrowedNativeValue<'a>, Error> {
        Ok(match self.u8()? {
            0 => BorrowedNativeValue::Null,
            1 => BorrowedNativeValue::Text(self.borrowed_text_with_validation(validation)?),
            2 => BorrowedNativeValue::Uuid(uuid::Uuid::from_bytes(
                self.exact(16)?.try_into().expect("sixteen-byte UUID"),
            )),
            3 => BorrowedNativeValue::Int8(i64::from_be_bytes(
                self.exact(8)?.try_into().expect("eight-byte integer"),
            )),
            4 => {
                let value =
                    f64::from_be_bytes(self.exact(8)?.try_into().expect("eight-byte float"));
                if validation == NativePayloadValidation::Full
                    && (!value.is_finite() || (value == 0.0 && value.is_sign_negative()))
                {
                    return Err(Error::Invalid("typed row float8 is not canonical"));
                }
                BorrowedNativeValue::Float8(value)
            }
            5 => {
                let value = self.u8()?;
                if validation == NativePayloadValidation::Full && value > 1 {
                    return Err(Error::Invalid("typed row boolean is not canonical"));
                }
                BorrowedNativeValue::Boolean(value != 0)
            }
            6 => {
                let length = self.u32()? as usize;
                if validation == NativePayloadValidation::Full && length > MAX_TEXT_BYTES {
                    return Err(Error::Invalid("typed row variable value is too large"));
                }
                let value = self.exact(length)?;
                let value = if validation == NativePayloadValidation::Full {
                    lix_schema::validate_canonical_json_text(value)
                        .map_err(|error| Error::Invalid(error.0))?
                } else {
                    // SAFETY: the opaque payload proof can only be constructed
                    // after this exact range passed canonical UTF-8 validation.
                    unsafe { std::str::from_utf8_unchecked(value) }
                };
                BorrowedNativeValue::Jsonb(value)
            }
            7 => BorrowedNativeValue::Timestamptz(i64::from_be_bytes(
                self.exact(8)?.try_into().expect("eight-byte timestamp"),
            )),
            8 => {
                return Err(Error::Invalid(
                    "durable typed row payload must not contain attachments",
                ));
            }
            _ => return Err(Error::Invalid("unknown typed row value")),
        })
    }

    fn borrowed_text_with_validation(
        &mut self,
        validation: NativePayloadValidation,
    ) -> Result<&'a str, Error> {
        let bytes = self.bytes_value_with_validation(validation)?;
        let value = if validation == NativePayloadValidation::Full {
            std::str::from_utf8(bytes).map_err(|_| Error::Invalid("typed row text is not UTF-8"))?
        } else {
            // SAFETY: `ValidatedNativePayload` proves this exact framed text.
            unsafe { std::str::from_utf8_unchecked(bytes) }
        };
        if validation == NativePayloadValidation::Full && value.contains('\0') {
            return Err(Error::Invalid("typed row text contains an interior NUL"));
        }
        Ok(value)
    }

    fn text(&mut self) -> Result<String, Error> {
        String::from_utf8(self.bytes_value()?.to_vec())
            .map_err(|_| Error::Invalid("typed row text is not UTF-8"))
    }

    fn bytes_value(&mut self) -> Result<&'a [u8], Error> {
        self.bytes_value_with_validation(NativePayloadValidation::Full)
    }

    fn bytes_value_with_validation(
        &mut self,
        validation: NativePayloadValidation,
    ) -> Result<&'a [u8], Error> {
        let length = self.u32()? as usize;
        if validation == NativePayloadValidation::Full && length > MAX_TEXT_BYTES {
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
    use std::ops::Range;

    use super::{
        ChangeEffect, Mutation, decode_key_value_bytes, decode_page, decode_page_parts,
        encode_key_value_bytes, encode_page, encode_page_parts,
    };
    use lix_schema::{CompiledSchema, Row, Schema, Value};

    #[test]
    fn projection_certificate_indexes_uniform_native_rows() {
        let first_value: lix_schema::Jsonb = serde_json::json!({"n": 1}).into();
        let second_value: lix_schema::Jsonb = serde_json::json!({"n": 2}).into();
        let first_row = Row::from([
            ("path".to_owned(), Value::Text("/a".to_owned())),
            ("value".to_owned(), Value::Jsonb(first_value)),
        ]);
        let second_row = Row::from([
            ("path".to_owned(), Value::Text("/b".to_owned())),
            ("value".to_owned(), Value::Jsonb(second_value)),
        ]);
        let first = super::encode_native_row_payload_with_identity(
            &[7; 32],
            &[Value::Text("/a".to_owned())],
            &first_row,
        )
        .expect("first native row encodes");
        let second = super::encode_native_row_payload_with_identity(
            &[7; 32],
            &[Value::Text("/b".to_owned())],
            &second_row,
        )
        .expect("second native row encodes");
        let first_len = first.len();
        let first_range = 0..u32::try_from(first_len).unwrap();
        let second_range =
            u32::try_from(first.len()).unwrap()..u32::try_from(first.len() + second.len()).unwrap();
        let owner = bytes::Bytes::from([first, second].concat());
        let certified = super::certify_native_projection_segment(
            owner.clone(),
            Box::new([first_range, second_range]),
        )
        .expect("uniform native segment certifies");

        assert_eq!(certified.row_count(), 2);
        assert_eq!(certified.schema_fingerprint(), [7; 32]);
        assert_eq!(
            certified.key_kinds(),
            &[super::CertifiedNativeScalarKind::Text]
        );
        assert_eq!(
            certified
                .fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["path", "value"]
        );
        assert!(certified.key_equals_field(0, 0));
        assert!(!certified.key_equals_field(0, 1));
        assert_eq!(
            certified.key_value(1, 0),
            Some(super::BorrowedNativeValue::Text("/b"))
        );
        assert_eq!(
            certified.field_value(0, 1),
            Some(super::BorrowedNativeValue::Jsonb("{\"n\":1}"))
        );
        assert_eq!(certified.payload_bytes(0).unwrap(), &owner[..first_len]);
        let fallback = certified
            .validated_payload_owned(1)
            .expect("certified range materializes a fallback proof");
        let expected_fallback = owner[first_len..].to_vec();
        drop(certified);
        drop(owner);
        assert_eq!(fallback.as_bytes(), expected_fallback);
    }

    #[test]
    fn projection_certificate_rejects_mixed_fingerprints() {
        let value: lix_schema::Jsonb = serde_json::json!(null).into();
        let first = super::encode_native_path_value_payload(&[1; 32], "/a", &value)
            .expect("first native row encodes");
        let second = super::encode_native_path_value_payload(&[2; 32], "/b", &value)
            .expect("second native row encodes");
        let ranges = [
            Range {
                start: 0,
                end: u32::try_from(first.len()).unwrap(),
            },
            Range {
                start: u32::try_from(first.len()).unwrap(),
                end: u32::try_from(first.len() + second.len()).unwrap(),
            },
        ];
        let error = super::certify_native_projection_segment(
            bytes::Bytes::from([first, second].concat()),
            Box::new(ranges),
        )
        .expect_err("mixed fingerprints must not certify");
        assert!(matches!(error, super::Error::Invalid(_)));
    }

    #[test]
    fn certified_path_value_encoder_is_byte_identical_to_general_native_encoder() {
        let path = "/packages/β";
        let value: lix_schema::Jsonb = serde_json::json!({"lane": "scale", "ordinal": 7}).into();
        let row = Row::from([
            ("path", Value::Text(path.to_owned())),
            ("value", Value::Jsonb(value.clone())),
        ]);
        let expected =
            super::encode_native_row_payload(&[9; 32], &[Value::Text(path.to_owned())], &row)
                .expect("general native payload should encode");

        assert_eq!(
            super::encode_native_path_value_payload(&[9; 32], path, &value)
                .expect("certified native payload should encode"),
            expected
        );

        let canonical = value.to_json_string().unwrap();
        let mut direct = b"prefix".to_vec();
        assert!(
            super::try_append_native_path_value_payload_from_canonical_json(
                &mut direct,
                &[9; 32],
                path,
                canonical.as_bytes(),
            )
            .expect("canonical parameter payload should encode")
        );
        assert_eq!(&direct[b"prefix".len()..], expected);
    }

    #[test]
    fn certified_canonical_parameter_path_rejects_without_mutating_output() {
        for rejected in [
            br#" {"a":1}"#.as_slice(),
            br#"{"b":1,"a":2}"#.as_slice(),
            br#"{"a":}"#.as_slice(),
            b"\xff".as_slice(),
        ] {
            let mut output = b"retained".to_vec();
            assert!(
                !super::try_append_native_path_value_payload_from_canonical_json(
                    &mut output,
                    &[3; 32],
                    "/path",
                    rejected,
                )
                .expect("noncanonical input should select the fallback")
            );
            assert_eq!(output, b"retained");
        }

        let mut output = b"retained".to_vec();
        assert!(
            super::try_append_native_path_value_payload_from_canonical_json(
                &mut output,
                &[3; 32],
                "bad\0path",
                b"null",
            )
            .is_err()
        );
        assert_eq!(output, b"retained");
    }

    #[test]
    fn compact_engine_payload_uses_schema_layout_without_identity_duplication() {
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "compact_probe",
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "value", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        }))
        .expect("compact probe schema should decode");
        let compiled = CompiledSchema::compile(&schema).expect("compact probe should compile");
        let id = uuid::Uuid::from_u128(1);
        let key = vec![Value::Uuid(id)];
        let row = Row::from([
            ("id", Value::Uuid(id)),
            ("value", Value::Text("payload".to_owned())),
        ]);

        let compact = super::encode_engine_row_payload(&compiled, &row)
            .expect("compact engine payload should encode");
        let ordinary = super::encode_native_row_payload(&[7; 32], &key, &row)
            .expect("ordinary native payload should encode");

        assert_eq!(compact[0], super::ENGINE_ROW_PAYLOAD_VERSION);
        assert!(compact.len() < ordinary.len());
        assert_eq!(
            super::decode_engine_row_payload(&compact, &compiled)
                .expect("compact engine payload should decode"),
            row
        );
    }

    #[test]
    fn compact_engine_payload_rejects_schema_invalid_values() {
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "compact_probe",
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "value", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        }))
        .expect("compact probe schema should decode");
        let compiled = CompiledSchema::compile(&schema).expect("compact probe should compile");
        let mut corrupt = vec![super::ENGINE_ROW_PAYLOAD_VERSION];
        corrupt.extend_from_slice(&2u32.to_le_bytes());
        super::encode_value_inline(&mut corrupt, &Value::Uuid(uuid::Uuid::from_u128(1)))
            .expect("UUID should encode");
        super::encode_value_inline(&mut corrupt, &Value::Null).expect("NULL should encode");

        assert!(super::decode_engine_row_payload(&corrupt, &compiled).is_err());
    }

    #[test]
    fn compact_engine_payload_materializes_appended_nullable_columns() {
        let historical: Schema = serde_json::from_value(serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "compact_probe",
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "value", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"]
        }))
        .expect("historical compact schema should decode");
        let amended: Schema = serde_json::from_value(serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "compact_probe",
            "columns": [
                {"name": "id", "type": "uuid", "nullable": false},
                {"name": "value", "type": "text", "nullable": false},
                {"name": "profile_uri", "type": "text", "nullable": true}
            ],
            "primary_key": ["id"]
        }))
        .expect("amended compact schema should decode");
        let historical =
            CompiledSchema::compile(&historical).expect("historical schema should compile");
        let amended = CompiledSchema::compile(&amended).expect("amended schema should compile");
        let id = uuid::Uuid::from_u128(1);
        let row = Row::from([
            ("id", Value::Uuid(id)),
            ("value", Value::Text("payload".to_owned())),
        ]);
        let compact = super::encode_engine_row_payload(&historical, &row)
            .expect("historical compact payload should encode");

        let mut expected = row;
        expected.insert("profile_uri", Value::Null);
        assert_eq!(
            super::decode_engine_row_payload(&compact, &amended)
                .expect("nullable amendment should decode historical payload"),
            expected
        );
        let mut visited = Vec::new();
        super::visit_engine_row_payload(&compact, &amended, |name, value| {
            visited.push((name.to_owned(), value));
        })
        .expect("projection visitor should materialize the nullable amendment");
        assert_eq!(
            visited.last(),
            Some(&("profile_uri".to_owned(), super::BorrowedNativeValue::Null))
        );
    }

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
                row_pk: Vec::new(),
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
    fn jsonb_scalar_wire_is_canonical_compact_utf8_text() {
        let encoded = super::encode_value_bytes(&Value::Jsonb(
            serde_json::json!({"b": 1, "a": [true, "β"]}).into(),
        ))
        .unwrap();
        let canonical = r#"{"a":[true,"β"],"b":1}"#.as_bytes();

        assert_eq!(encoded[0], 6);
        assert_eq!(
            u32::from_le_bytes(encoded[1..5].try_into().unwrap()) as usize,
            canonical.len()
        );
        assert_eq!(&encoded[5..], canonical);
        assert!(lix_schema::validate_canonical_json_text(&encoded[5..]).is_ok());
    }

    #[test]
    fn borrowed_native_jsonb_is_validated_canonical_text() {
        let canonical = r#"{"a":2,"z":1}"#;
        let row = Row::from([(
            "json".to_owned(),
            Value::Jsonb(serde_json::from_str(canonical).unwrap()),
        )]);
        let mut payload =
            super::encode_native_row_payload(&[5; 32], &[Value::Text("row".to_owned())], &row)
                .expect("native payload encodes");
        let canonical_offset = payload
            .windows(canonical.len())
            .position(|window| window == canonical.as_bytes())
            .expect("encoded payload contains canonical JSONB text");

        let mut projected = None;
        super::visit_native_row_payload(
            &payload,
            |_, _| {},
            |name, value| {
                if name == "json"
                    && let super::BorrowedNativeValue::Jsonb(value) = value
                {
                    projected = Some(value);
                }
            },
        )
        .expect("canonical JSONB payload visits");
        assert_eq!(projected, Some(canonical));

        let validated = super::ValidatedNativePayload::try_new(bytes::Bytes::from(payload.clone()))
            .expect("full validation constructs an opaque proof");
        let mut certified_projected = None;
        super::visit_validated_native_row_payload(
            &validated,
            |_, _| {},
            |name, value| {
                if name == "json"
                    && let super::BorrowedNativeValue::Jsonb(value) = value
                {
                    certified_projected = Some(value);
                }
            },
        )
        .expect("certified visitor traverses the proven bytes");
        assert_eq!(certified_projected, projected);

        payload[canonical_offset..canonical_offset + canonical.len()]
            .copy_from_slice(br#"{"z":1,"a":2}"#);
        assert!(
            super::visit_native_row_payload(&payload, |_, _| {}, |_, _| {}).is_err(),
            "borrowed JSONB must retain canonical validation"
        );
        assert!(
            super::ValidatedNativePayload::try_new(bytes::Bytes::from(payload)).is_err(),
            "noncanonical bytes cannot construct the visitor proof"
        );
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
        assert_eq!(metadata.to_json_string().unwrap(), r#"{"a":1,"b":2}"#);
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
        let expected_metadata = format!(r#"{{"payload":"{}"}}"#, "y".repeat(8 * 1024 + 9));
        assert!(attachments.iter().any(|attachment| {
            attachment.as_slice() == expected_metadata.as_bytes()
                && lix_schema::validate_canonical_json_text(attachment).is_ok()
        }));
        let title_attachment_pointer = attachments
            .iter()
            .find(|attachment| attachment.first() == Some(&b'x'))
            .expect("title attachment")
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
            decoded_metadata.to_json_string().unwrap(),
            expected_metadata
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
