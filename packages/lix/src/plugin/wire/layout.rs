//! Compiled wire layouts for dense plugin rows.
//!
//! A layout describes a compact row wire shape and how its values map to a
//! canonical JSON snapshot. Parsing resolves and validates every slot reference
//! once. [`Rows`] then reuses one slot buffer while rendering a bounded page.

#![allow(clippy::missing_errors_doc)]

use std::{borrow::Cow, cmp::Ordering, fmt, ops::Range};

use base64::Engine as _;
use serde::Deserialize;
const MAX_WIRE_SLOTS: usize = 256;
const MAX_FIELDS: usize = 4096;
const MAX_OBJECT_DEPTH: usize = 32;

/// Inserts a generated string identity at a compiled object path and returns
/// canonical JSON bytes. Every segment before the leaf must already be an
/// object; the leaf must be absent.
pub fn insert_generated_id(
    snapshot: &[u8],
    path: &[String],
    generated_id: &str,
) -> Result<Vec<u8>, Error> {
    if path.is_empty() {
        return Err(Error::new("generated identity path is empty"));
    }
    if snapshot.first() != Some(&b'{') || snapshot.last() != Some(&b'}') {
        return Err(Error::new(
            "generated identity snapshot must be a canonical JSON object",
        ));
    }

    let insertion = find_identity_insertion(snapshot, 0, snapshot.len(), path, 0)?;
    let mut field = Vec::with_capacity(path.last().unwrap().len() + generated_id.len() + 7);
    write_json_string(&mut field, path.last().unwrap());
    field.push(b':');
    write_json_string(&mut field, generated_id);

    let separator = usize::from(insertion.needs_comma);
    let mut output = Vec::with_capacity(snapshot.len() + field.len() + separator);
    output.extend_from_slice(&snapshot[..insertion.offset]);
    if insertion.comma_before {
        output.push(b',');
    }
    output.extend_from_slice(&field);
    if insertion.comma_after {
        output.push(b',');
    }
    output.extend_from_slice(&snapshot[insertion.offset..]);
    Ok(output)
}

/// Validates that a complete canonical snapshot contains the generated string
/// identity at the compiled path.
pub fn validate_generated_id(
    snapshot: &[u8],
    path: &[String],
    expected: &str,
) -> Result<(), Error> {
    if path.is_empty() {
        return Err(Error::new("generated identity path is empty"));
    }
    if snapshot.first() != Some(&b'{') || snapshot.last() != Some(&b'}') {
        return Err(Error::new(
            "generated identity snapshot must be a canonical JSON object",
        ));
    }
    validate_identity_at(snapshot, 0, snapshot.len(), path, 0, expected)
}

fn validate_identity_at(
    bytes: &[u8],
    object_start: usize,
    object_end: usize,
    path: &[String],
    depth: usize,
    expected: &str,
) -> Result<(), Error> {
    if object_end <= object_start + 1
        || bytes.get(object_start) != Some(&b'{')
        || bytes.get(object_end - 1) != Some(&b'}')
    {
        return Err(Error::new(format!(
            "generated identity parent '{}' is missing or not an object",
            path[..depth].join(".")
        )));
    }
    let target = &path[depth];
    let mut entry_start = object_start + 1;
    while entry_start < object_end - 1 {
        if bytes.get(entry_start) != Some(&b'\"') {
            return Err(Error::new("canonical JSON object has an invalid key"));
        }
        let key_end = json_string_end(bytes, entry_start, object_end)?;
        let key = decoded_json_key(&bytes[entry_start..key_end])?;
        let ordering = key.as_ref().cmp(target.as_str());
        if ordering == Ordering::Greater {
            break;
        }
        if bytes.get(key_end) != Some(&b':') {
            return Err(Error::new("canonical JSON object key has no value"));
        }
        let value_start = key_end + 1;
        let boundary = json_value_boundary(bytes, value_start, object_end)?;
        if ordering == Ordering::Equal {
            if depth + 1 == path.len() {
                let value = decoded_json_key(&bytes[value_start..boundary.delimiter])
                    .map_err(|_| Error::new("generated identity must be a JSON string"))?;
                if value.as_ref() != expected {
                    return Err(Error::new(
                        "generated identity does not match its create context",
                    ));
                }
                return Ok(());
            }
            return validate_identity_at(
                bytes,
                value_start,
                boundary.delimiter,
                path,
                depth + 1,
                expected,
            );
        }
        if !boundary.has_next {
            break;
        }
        entry_start = boundary.delimiter + 1;
    }
    Err(Error::new(format!(
        "generated identity field '{}' is missing",
        target
    )))
}

#[derive(Clone, Copy)]
struct IdentityInsertion {
    offset: usize,
    needs_comma: bool,
    comma_before: bool,
    comma_after: bool,
}

fn find_identity_insertion(
    bytes: &[u8],
    object_start: usize,
    object_end: usize,
    path: &[String],
    depth: usize,
) -> Result<IdentityInsertion, Error> {
    if object_end <= object_start + 1
        || bytes.get(object_start) != Some(&b'{')
        || bytes.get(object_end - 1) != Some(&b'}')
    {
        return Err(Error::new(format!(
            "generated identity parent '{}' is missing or not an object",
            path[..depth].join(".")
        )));
    }

    let target = &path[depth];
    let mut entry_start = object_start + 1;
    if entry_start == object_end - 1 {
        if depth + 1 != path.len() {
            return Err(Error::new(format!(
                "generated identity parent '{}' is missing or not an object",
                target
            )));
        }
        return Ok(IdentityInsertion {
            offset: entry_start,
            needs_comma: false,
            comma_before: false,
            comma_after: false,
        });
    }

    loop {
        if bytes.get(entry_start) != Some(&b'\"') {
            return Err(Error::new("canonical JSON object has an invalid key"));
        }
        let key_end = json_string_end(bytes, entry_start, object_end)?;
        let key = decoded_json_key(&bytes[entry_start..key_end])?;
        let ordering = key.as_ref().cmp(target.as_str());
        if bytes.get(key_end) != Some(&b':') {
            return Err(Error::new("canonical JSON object key has no value"));
        }
        let value_start = key_end + 1;
        let boundary = json_value_boundary(bytes, value_start, object_end)?;

        if depth + 1 == path.len() {
            match ordering {
                Ordering::Equal => {
                    return Err(Error::new(
                        "snapshot already contains its generated identity",
                    ));
                }
                Ordering::Greater => {
                    return Ok(IdentityInsertion {
                        offset: entry_start,
                        needs_comma: true,
                        comma_before: false,
                        comma_after: true,
                    });
                }
                Ordering::Less => {}
            }
        } else {
            match ordering {
                Ordering::Equal => {
                    return find_identity_insertion(
                        bytes,
                        value_start,
                        boundary.delimiter,
                        path,
                        depth + 1,
                    );
                }
                Ordering::Greater => {
                    return Err(Error::new(format!(
                        "generated identity parent '{}' is missing or not an object",
                        target
                    )));
                }
                Ordering::Less => {}
            }
        }

        if boundary.has_next {
            entry_start = boundary.delimiter + 1;
        } else {
            if depth + 1 != path.len() {
                return Err(Error::new(format!(
                    "generated identity parent '{}' is missing or not an object",
                    target
                )));
            }
            return Ok(IdentityInsertion {
                offset: boundary.delimiter,
                needs_comma: true,
                comma_before: true,
                comma_after: false,
            });
        }
    }
}

fn decoded_json_key(encoded: &[u8]) -> Result<Cow<'_, str>, Error> {
    if encoded.first() != Some(&b'\"') || encoded.last() != Some(&b'\"') {
        return Err(Error::new("canonical JSON string is not quoted"));
    }
    let inner = encoded
        .get(1..encoded.len().saturating_sub(1))
        .ok_or_else(|| Error::new("canonical JSON object has an invalid key"))?;
    if inner.contains(&b'\\') {
        serde_json::from_slice::<String>(encoded)
            .map(Cow::Owned)
            .map_err(|error| Error::new(format!("invalid canonical JSON key: {error}")))
    } else {
        std::str::from_utf8(inner)
            .map(Cow::Borrowed)
            .map_err(|error| Error::new(format!("invalid canonical JSON key: {error}")))
    }
}

fn json_string_end(bytes: &[u8], start: usize, limit: usize) -> Result<usize, Error> {
    let mut escaped = false;
    for (offset, byte) in bytes[start + 1..limit].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'\"' {
            return Ok(start + offset + 2);
        }
    }
    Err(Error::new("unterminated canonical JSON string"))
}

struct JsonValueBoundary {
    delimiter: usize,
    has_next: bool,
}

fn json_value_boundary(
    bytes: &[u8],
    start: usize,
    object_end: usize,
) -> Result<JsonValueBoundary, Error> {
    let mut nested = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, byte) in bytes[start..object_end].iter().enumerate() {
        let index = start + offset;
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'\"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'\"' => in_string = true,
            b'[' | b'{' => {
                nested = nested
                    .checked_add(1)
                    .ok_or_else(|| Error::new("canonical JSON nesting overflowed"))?;
            }
            b']' | b'}' if nested > 0 => nested -= 1,
            b',' if nested == 0 => {
                return Ok(JsonValueBoundary {
                    delimiter: index,
                    has_next: true,
                });
            }
            b'}' if nested == 0 && index == object_end - 1 => {
                return Ok(JsonValueBoundary {
                    delimiter: index,
                    has_next: false,
                });
            }
            _ => {}
        }
    }
    Err(Error::new("canonical JSON object has no closing boundary"))
}

/// A parsed and fully validated row layout.
#[derive(Debug)]
pub struct CompiledLayout {
    wire: Vec<WireSlot>,
    local_ref_slot: usize,
    generated_id_path: Vec<String>,
    fields: Vec<FieldPlan>,
}

impl CompiledLayout {
    /// Parses a declarative JSON layout and resolves every slot reference.
    pub fn parse(json: &[u8]) -> Result<Self, Error> {
        let raw: RawLayout = serde_json::from_slice(json)
            .map_err(|error| Error::new(format!("invalid row layout JSON: {error}")))?;
        if raw.wire.is_empty() {
            return Err(Error::new("row layout wire shape is empty"));
        }
        if raw.wire.len() > MAX_WIRE_SLOTS {
            return Err(Error::new("row layout has too many wire slots"));
        }
        let [
            RawValue::GeneratedId {
                slot: local_ref_slot,
            },
        ] = raw.primary_key.as_slice()
        else {
            return Err(Error::new(
                "row layout primary key must be one generated-id value",
            ));
        };
        expect_slot(
            &raw.wire,
            *local_ref_slot,
            SlotKind::CreateRef,
            "primary key",
        )?;

        let mut field_count = 0_usize;
        let fields = compile_fields(&raw.fields, &raw.wire, *local_ref_slot, 1, &mut field_count)?;
        let mut generated_id_paths = Vec::new();
        collect_generated_id_paths(&fields, &mut Vec::new(), &mut generated_id_paths);
        let [generated_id_path] = generated_id_paths.as_slice() else {
            return Err(Error::new(
                "row layout must map its generated primary key to exactly one field",
            ));
        };
        Ok(Self {
            wire: raw.wire,
            local_ref_slot: *local_ref_slot,
            generated_id_path: generated_id_path.clone(),
            fields,
        })
    }

    /// Opens a compact payload containing exactly `row_count` rows.
    pub fn rows<'layout, 'payload>(
        &'layout self,
        payload: &'payload [u8],
        row_count: u32,
    ) -> Result<Rows<'layout, 'payload>, Error> {
        if row_count == 0 {
            return Err(Error::new("row page is empty"));
        }
        Ok(Rows {
            layout: self,
            input: Reader::new(payload),
            remaining: row_count,
            slots: (0..self.wire.len()).map(|_| SlotValue::Empty).collect(),
        })
    }

    /// JSON object path where the host-generated identity must be inserted.
    pub fn generated_id_path(&self) -> &[String] {
        &self.generated_id_path
    }
}

/// Stateful renderer for one compact row page.
///
/// The renderer allocates its slot table once. [`Rows::render_next`] overwrites
/// that table in place and appends one snapshot to the caller-owned output.
#[derive(Debug)]
pub struct Rows<'layout, 'payload> {
    layout: &'layout CompiledLayout,
    input: Reader<'payload>,
    remaining: u32,
    slots: Vec<SlotValue<'payload>>,
}

impl Rows<'_, '_> {
    /// Parses the next row and appends its canonical snapshot to `output`.
    ///
    /// Generated-ID fields are identity metadata and are intentionally omitted
    /// from the snapshot. The returned range identifies the newly appended JSON.
    pub fn render_next(&mut self, output: &mut Vec<u8>) -> Result<Option<RenderedRow>, Error> {
        let Some(local_ref) = self.parse_next()? else {
            return Ok(None);
        };
        let output_start = output.len();
        if let Err(error) = render_object(&self.layout.fields, &self.slots, output) {
            output.truncate(output_start);
            return Err(error);
        }
        Ok(Some(RenderedRow {
            local_ref,
            snapshot: output_start..output.len(),
        }))
    }

    /// Validates and advances one row without materializing its snapshot.
    ///
    /// This uses the same reusable slot table as rendering and is intended for
    /// boundary admission, identity-range validation, and other metadata-only
    /// passes.
    pub fn validate_next(&mut self) -> Result<Option<u64>, Error> {
        self.parse_next()
    }

    /// Verifies that all declared rows were rendered and no payload bytes remain.
    pub fn finish(self) -> Result<(), Error> {
        if self.remaining != 0 {
            return Err(Error::new("row page ended before its declared row count"));
        }
        self.input.finish()
    }

    fn parse_next(&mut self) -> Result<Option<u64>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.parse_slots()?;
        let SlotValue::CreateRef(local_ref) = self.slots[self.layout.local_ref_slot] else {
            unreachable!("compiled local-ref slot has the validated type")
        };
        self.remaining -= 1;
        Ok(Some(local_ref))
    }

    fn parse_slots(&mut self) -> Result<(), Error> {
        for (index, field) in self.layout.wire.iter().copied().enumerate() {
            self.slots[index] = match field {
                WireSlot::CreateRefU32 => SlotValue::CreateRef(u64::from(self.input.read_u32()?)),
                WireSlot::U64 => SlotValue::U64(self.input.read_u64()?),
                WireSlot::U8 => SlotValue::U8(self.input.read_u8()?),
                WireSlot::BytesU32 => {
                    let length = self.input.read_u32()? as usize;
                    SlotValue::Bytes(self.input.read_exact(length)?)
                }
                WireSlot::ListUtf8U16 => {
                    let count = self.input.read_u16()?;
                    let start = self.input.offset;
                    for _ in 0..count {
                        let length = self.input.read_u32()? as usize;
                        std::str::from_utf8(self.input.read_exact(length)?).map_err(|error| {
                            Error::new(format!("row string is not UTF-8: {error}"))
                        })?;
                    }
                    SlotValue::ListUtf8 {
                        count,
                        framed: self.input.consumed_from(start),
                    }
                }
            };
        }
        Ok(())
    }
}

/// Metadata for one rendered row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedRow {
    pub local_ref: u64,
    pub snapshot: Range<usize>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireSlot {
    CreateRefU32,
    U64,
    U8,
    BytesU32,
    ListUtf8U16,
}

impl WireSlot {
    fn kind(self) -> SlotKind {
        match self {
            Self::CreateRefU32 => SlotKind::CreateRef,
            Self::U64 => SlotKind::U64,
            Self::U8 => SlotKind::U8,
            Self::BytesU32 => SlotKind::Bytes,
            Self::ListUtf8U16 => SlotKind::ListUtf8,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotKind {
    CreateRef,
    U64,
    U8,
    Bytes,
    ListUtf8,
}

#[derive(Debug)]
enum FieldPlan {
    Value {
        name: String,
        value: ValuePlan,
    },
    Object {
        name: String,
        fields: Vec<FieldPlan>,
    },
}

impl FieldPlan {
    fn name(&self) -> &str {
        match self {
            Self::Value { name, .. } | Self::Object { name, .. } => name,
        }
    }
}

#[derive(Debug)]
enum ValuePlan {
    /// The value was validated but is identity metadata, not snapshot content.
    GeneratedId,
    HexU64 {
        slot: usize,
        width: u8,
    },
    Enum {
        slot: usize,
        values: Vec<Option<String>>,
    },
    Base64Url {
        slot: usize,
    },
    ListUtf8 {
        slot: usize,
    },
}

fn collect_generated_id_paths(
    fields: &[FieldPlan],
    prefix: &mut Vec<String>,
    output: &mut Vec<Vec<String>>,
) {
    for field in fields {
        prefix.push(field.name().to_owned());
        match field {
            FieldPlan::Value {
                value: ValuePlan::GeneratedId,
                ..
            } => output.push(prefix.clone()),
            FieldPlan::Object { fields, .. } => {
                collect_generated_id_paths(fields, prefix, output);
            }
            FieldPlan::Value { .. } => {}
        }
        prefix.pop();
    }
}

#[derive(Debug)]
enum SlotValue<'a> {
    Empty,
    CreateRef(u64),
    U64(u64),
    U8(u8),
    Bytes(&'a [u8]),
    ListUtf8 { count: u16, framed: &'a [u8] },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLayout {
    wire: Vec<WireSlot>,
    primary_key: Vec<RawValue>,
    fields: Vec<RawField>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawField {
    name: String,
    #[serde(default)]
    value: Option<RawValue>,
    #[serde(default)]
    object: Option<Vec<RawField>>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RawValue {
    GeneratedId {
        slot: usize,
    },
    HexU64 {
        slot: usize,
        width: u8,
    },
    Enum {
        slot: usize,
        values: Vec<Option<String>>,
    },
    Base64Url {
        slot: usize,
    },
    ListUtf8 {
        slot: usize,
    },
}

fn compile_fields(
    raw: &[RawField],
    wire: &[WireSlot],
    local_ref_slot: usize,
    depth: usize,
    field_count: &mut usize,
) -> Result<Vec<FieldPlan>, Error> {
    if raw.is_empty() {
        return Err(Error::new("row object has no fields"));
    }
    if depth > MAX_OBJECT_DEPTH {
        return Err(Error::new("row layout object nesting is too deep"));
    }
    if !raw.windows(2).all(|pair| pair[0].name < pair[1].name)
        || raw.iter().any(|field| field.name.is_empty())
    {
        return Err(Error::new(
            "row object fields must be nonempty and canonically sorted",
        ));
    }
    let mut output = Vec::with_capacity(raw.len());
    for field in raw {
        *field_count = field_count
            .checked_add(1)
            .ok_or_else(|| Error::new("row layout field count overflowed"))?;
        if *field_count > MAX_FIELDS {
            return Err(Error::new("row layout has too many fields"));
        }
        output.push(match (&field.value, &field.object) {
            (Some(value), None) => FieldPlan::Value {
                name: field.name.clone(),
                value: compile_value(value, wire, local_ref_slot)?,
            },
            (None, Some(fields)) => FieldPlan::Object {
                name: field.name.clone(),
                fields: compile_fields(fields, wire, local_ref_slot, depth + 1, field_count)?,
            },
            _ => {
                return Err(Error::new(format!(
                    "row field {:?} must contain exactly one of value or object",
                    field.name
                )));
            }
        });
    }
    Ok(output)
}

fn compile_value(
    raw: &RawValue,
    wire: &[WireSlot],
    local_ref_slot: usize,
) -> Result<ValuePlan, Error> {
    Ok(match raw {
        RawValue::GeneratedId { slot } => {
            expect_slot(wire, *slot, SlotKind::CreateRef, "generated-id value")?;
            if *slot != local_ref_slot {
                return Err(Error::new(
                    "generated-id snapshot field must use the primary-key local reference",
                ));
            }
            ValuePlan::GeneratedId
        }
        RawValue::HexU64 { slot, width } => {
            expect_slot(wire, *slot, SlotKind::U64, "hex-u64 value")?;
            if !(1..=16).contains(width) {
                return Err(Error::new("hex-u64 width is outside 1..=16"));
            }
            ValuePlan::HexU64 {
                slot: *slot,
                width: *width,
            }
        }
        RawValue::Enum { slot, values } => {
            expect_slot(wire, *slot, SlotKind::U8, "enum value")?;
            if values.is_empty() || values.len() > usize::from(u8::MAX) + 1 {
                return Err(Error::new("u8 enum must have 1..=256 values"));
            }
            ValuePlan::Enum {
                slot: *slot,
                values: values.clone(),
            }
        }
        RawValue::Base64Url { slot } => {
            expect_slot(wire, *slot, SlotKind::Bytes, "base64-url value")?;
            ValuePlan::Base64Url { slot: *slot }
        }
        RawValue::ListUtf8 { slot } => {
            expect_slot(wire, *slot, SlotKind::ListUtf8, "list-utf8 value")?;
            ValuePlan::ListUtf8 { slot: *slot }
        }
    })
}

fn expect_slot(
    wire: &[WireSlot],
    slot: usize,
    expected: SlotKind,
    context: &str,
) -> Result<(), Error> {
    let Some(actual) = wire.get(slot).copied().map(WireSlot::kind) else {
        return Err(Error::new(format!(
            "{context} references missing slot {slot}"
        )));
    };
    if actual != expected {
        return Err(Error::new(format!(
            "{context} references slot {slot} with the wrong wire type"
        )));
    }
    Ok(())
}

fn render_object(
    fields: &[FieldPlan],
    slots: &[SlotValue<'_>],
    output: &mut Vec<u8>,
) -> Result<(), Error> {
    output.push(b'{');
    let mut comma = false;
    for field in fields {
        let start = output.len();
        if comma {
            output.push(b',');
        }
        write_json_string(output, field.name());
        output.push(b':');
        let present = match field {
            FieldPlan::Value { value, .. } => render_value(value, slots, output)?,
            FieldPlan::Object { fields, .. } => {
                let object_start = output.len();
                render_object(fields, slots, output)?;
                output.len() > object_start + 2
            }
        };
        if present {
            comma = true;
        } else {
            output.truncate(start);
        }
    }
    output.push(b'}');
    Ok(())
}

fn render_value(
    value: &ValuePlan,
    slots: &[SlotValue<'_>],
    output: &mut Vec<u8>,
) -> Result<bool, Error> {
    match value {
        ValuePlan::GeneratedId => Ok(false),
        ValuePlan::HexU64 { slot, width } => {
            let SlotValue::U64(value) = slots[*slot] else {
                unreachable!("compiled hex-u64 slot has the validated type")
            };
            const HEX: &[u8; 16] = b"0123456789abcdef";
            output.push(b'"');
            for shift in (0..usize::from(*width)).rev() {
                output.push(HEX[((value >> (shift * 4)) & 0x0f) as usize]);
            }
            output.push(b'"');
            Ok(true)
        }
        ValuePlan::Enum { slot, values } => {
            let SlotValue::U8(index) = slots[*slot] else {
                unreachable!("compiled enum slot has the validated type")
            };
            let value = values
                .get(usize::from(index))
                .ok_or_else(|| Error::new("row enum index is out of range"))?;
            let Some(value) = value else {
                return Ok(false);
            };
            write_json_string(output, value);
            Ok(true)
        }
        ValuePlan::Base64Url { slot } => {
            let SlotValue::Bytes(value) = slots[*slot] else {
                unreachable!("compiled base64-url slot has the validated type")
            };
            if value.is_empty() {
                return Ok(false);
            }
            output.push(b'"');
            let encoded_len = base64::encoded_len(value.len(), false)
                .ok_or_else(|| Error::new("base64 output length overflowed"))?;
            let start = output.len();
            output.resize(start + encoded_len, 0);
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode_slice(value, &mut output[start..])
                .map_err(|error| Error::new(format!("failed to encode base64 value: {error}")))?;
            output.push(b'"');
            Ok(true)
        }
        ValuePlan::ListUtf8 { slot } => {
            let SlotValue::ListUtf8 { count, framed } = slots[*slot] else {
                unreachable!("compiled list-utf8 slot has the validated type")
            };
            let mut input = Reader::new(framed);
            output.push(b'[');
            for index in 0..count {
                if index != 0 {
                    output.push(b',');
                }
                let length = input.read_u32()? as usize;
                let value = std::str::from_utf8(input.read_exact(length)?)
                    .expect("row strings were validated while parsing slots");
                write_json_string(output, value);
            }
            input.finish()?;
            output.push(b']');
            Ok(true)
        }
    }
}

fn write_json_string(output: &mut Vec<u8>, value: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    for &byte in value.as_bytes() {
        match byte {
            b'"' => output.extend_from_slice(br#"\""#),
            b'\\' => output.extend_from_slice(br#"\\"#),
            b'\n' => output.extend_from_slice(br#"\n"#),
            b'\r' => output.extend_from_slice(br#"\r"#),
            b'\t' => output.extend_from_slice(br#"\t"#),
            0x08 => output.extend_from_slice(br#"\b"#),
            0x0c => output.extend_from_slice(br#"\f"#),
            0x00..=0x1f => {
                output.extend_from_slice(b"\\u00");
                output.push(HEX[usize::from(byte >> 4)]);
                output.push(HEX[usize::from(byte & 0x0f)]);
            }
            _ => output.push(byte),
        }
    }
    output.push(b'"');
}

#[derive(Clone, Copy, Debug)]
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| Error::new("row payload range overflowed"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::new("row payload ended early"))?;
        self.offset = end;
        Ok(value)
    }

    fn consumed_from(&self, start: usize) -> &'a [u8] {
        &self.bytes[start..self.offset]
    }

    fn read_u8(&mut self) -> Result<u8, Error> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(
            self.read_exact(2)?.try_into().expect("exact u16 width"),
        ))
    }

    fn read_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("exact u32 width"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("exact u64 width"),
        ))
    }

    fn finish(self) -> Result<(), Error> {
        if self.offset != self.bytes.len() {
            return Err(Error::new("row payload has trailing bytes"));
        }
        Ok(())
    }
}

/// Invalid layout or compact row payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Callers render this error through `Display`; only the layout tests need
    /// the raw message for substring assertions.
    #[cfg(test)]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::{CompiledLayout, insert_generated_id, validate_generated_id};

    const ROW_LAYOUT: &[u8] = br#"{
      "wire":["create_ref_u32","u64","u8","bytes_u32","list_utf8_u16"],
      "primary_key":[{"kind":"generated_id","slot":0}],
      "fields":[
        {"name":"cells","value":{"kind":"list_utf8","slot":4}},
        {"name":"id","value":{"kind":"generated_id","slot":0}},
        {"name":"layout","object":[
          {"name":"force_quote","value":{"kind":"base64_url","slot":3}},
          {"name":"terminator","value":{"kind":"enum","slot":2,"values":[null,"","\n","\r\n","\r"]}}
        ]},
        {"name":"order_key","value":{"kind":"hex_u64","slot":1,"width":16}}
      ]
    }"#;

    #[test]
    fn renders_current_compact_row_shape() {
        let layout = CompiledLayout::parse(ROW_LAYOUT).unwrap();
        let mut payload = Vec::new();
        push_row(
            &mut payload,
            7,
            0x21,
            3,
            &[5],
            &["alpha", "b\"eta", "line\n"],
        );
        push_row(&mut payload, 8, 0x41, 0, &[], &[]);

        let mut rows = layout.rows(&payload, 2).unwrap();
        let slot_buffer = rows.slots.as_ptr();
        let mut snapshots = Vec::new();
        let first = rows.render_next(&mut snapshots).unwrap().unwrap();
        assert_eq!(first.local_ref, 7);
        assert_eq!(
            &snapshots[first.snapshot],
            br#"{"cells":["alpha","b\"eta","line\n"],"layout":{"force_quote":"BQ","terminator":"\r\n"},"order_key":"0000000000000021"}"#
        );
        assert_eq!(rows.slots.as_ptr(), slot_buffer);

        let second = rows.render_next(&mut snapshots).unwrap().unwrap();
        assert_eq!(second.local_ref, 8);
        assert_eq!(
            &snapshots[second.snapshot],
            br#"{"cells":[],"order_key":"0000000000000041"}"#
        );
        assert_eq!(rows.slots.as_ptr(), slot_buffer);
        assert!(rows.render_next(&mut snapshots).unwrap().is_none());
        rows.finish().unwrap();
    }

    #[test]
    fn accepts_but_does_not_materialize_a_generated_id_field() {
        let layout = CompiledLayout::parse(
            br#"{
              "wire":["create_ref_u32"],
              "primary_key":[{"kind":"generated_id","slot":0}],
              "fields":[{"name":"id","value":{"kind":"generated_id","slot":0}}]
            }"#,
        )
        .unwrap();
        let mut output = Vec::new();
        let payload = 42_u32.to_le_bytes();
        let mut rows = layout.rows(&payload, 1).unwrap();
        let row = rows.render_next(&mut output).unwrap().unwrap();
        assert_eq!(row.local_ref, 42);
        assert_eq!(&output[row.snapshot], b"{}");
        rows.finish().unwrap();
    }

    #[test]
    fn discovers_and_inserts_a_nested_non_id_primary_key() {
        let layout = CompiledLayout::parse(
            br#"{
              "wire":["create_ref_u32"],
              "primary_key":[{"kind":"generated_id","slot":0}],
              "fields":[{"name":"identity","object":[
                {"name":"stable_key","value":{"kind":"generated_id","slot":0}}
              ]}]
            }"#,
        )
        .unwrap();
        assert_eq!(
            layout.generated_id_path(),
            &["identity".to_owned(), "stable_key".to_owned()]
        );
        let snapshot = insert_generated_id(
            br#"{"identity":{},"value":"kept"}"#,
            layout.generated_id_path(),
            "01920000-0000-7000-8000-000000000001",
        )
        .unwrap();
        assert_eq!(
            snapshot,
            br#"{"identity":{"stable_key":"01920000-0000-7000-8000-000000000001"},"value":"kept"}"#
        );
    }

    #[test]
    fn inserts_generated_identity_in_canonical_key_order() {
        let generated = "01920000-0000-7000-8000-000000000001";
        assert_eq!(
            insert_generated_id(br#"{}"#, &["key".to_owned()], generated).unwrap(),
            br#"{"key":"01920000-0000-7000-8000-000000000001"}"#
        );
        assert_eq!(
            insert_generated_id(br#"{"alpha":1,"omega":2}"#, &["key".to_owned()], generated,)
                .unwrap(),
            br#"{"alpha":1,"key":"01920000-0000-7000-8000-000000000001","omega":2}"#
        );
        assert_eq!(
            insert_generated_id(br#"{"alpha":1}"#, &["zeta".to_owned()], generated).unwrap(),
            br#"{"alpha":1,"zeta":"01920000-0000-7000-8000-000000000001"}"#
        );
    }

    #[test]
    fn inserts_generated_identity_through_escaped_parent_key() {
        let snapshot = insert_generated_id(
            br#"{"a\tb":{"other":true}}"#,
            &["a\tb".to_owned(), "stable\"key".to_owned()],
            "generated",
        )
        .unwrap();
        assert_eq!(
            snapshot,
            br#"{"a\tb":{"other":true,"stable\"key":"generated"}}"#
        );
    }

    #[test]
    fn rejects_existing_or_missing_generated_identity_paths() {
        assert!(
            insert_generated_id(br#"{"key":"existing"}"#, &["key".to_owned()], "new")
                .unwrap_err()
                .message()
                .contains("already contains")
        );
        assert!(
            insert_generated_id(
                br#"{"identity":null}"#,
                &["identity".to_owned(), "key".to_owned()],
                "new",
            )
            .unwrap_err()
            .message()
            .contains("missing or not an object")
        );
        assert!(
            insert_generated_id(
                br#"{"other":{}}"#,
                &["identity".to_owned(), "key".to_owned()],
                "new",
            )
            .unwrap_err()
            .message()
            .contains("missing or not an object")
        );
    }

    #[test]
    fn validates_complete_generated_identity_snapshots() {
        let path = ["identity".to_owned(), "stable_key".to_owned()];
        validate_generated_id(
            br#"{"identity":{"stable_key":"generated"},"value":1}"#,
            &path,
            "generated",
        )
        .unwrap();
        assert!(
            validate_generated_id(
                br#"{"identity":{"stable_key":"wrong"}}"#,
                &path,
                "generated",
            )
            .unwrap_err()
            .message()
            .contains("does not match")
        );
        assert!(
            validate_generated_id(br#"{"identity":{"stable_key":1}}"#, &path, "generated",)
                .unwrap_err()
                .message()
                .contains("JSON string")
        );
    }

    #[test]
    fn validates_rows_without_materializing_snapshots_or_reallocating_slots() {
        let layout = CompiledLayout::parse(ROW_LAYOUT).unwrap();
        let mut payload = Vec::new();
        push_row(&mut payload, 3, 1, 0, &[], &["one"]);
        push_row(&mut payload, 4, 2, 2, &[], &["two"]);
        let mut rows = layout.rows(&payload, 2).unwrap();
        let slot_buffer = rows.slots.as_ptr();
        assert_eq!(rows.validate_next().unwrap(), Some(3));
        assert_eq!(rows.slots.as_ptr(), slot_buffer);
        assert_eq!(rows.validate_next().unwrap(), Some(4));
        assert_eq!(rows.slots.as_ptr(), slot_buffer);
        assert_eq!(rows.validate_next().unwrap(), None);
        rows.finish().unwrap();
    }

    #[test]
    fn rejects_wrong_slot_types_while_compiling() {
        let invalid = ROW_LAYOUT
            .windows(br#""kind":"base64_url","slot":3"#.len())
            .position(|window| window == br#""kind":"base64_url","slot":3"#)
            .expect("layout contains base64 value");
        let mut invalid_layout = ROW_LAYOUT.to_vec();
        let slot = invalid + br#""kind":"base64_url","slot":"#.len();
        invalid_layout[slot] = b'1';
        let error = CompiledLayout::parse(&invalid_layout).unwrap_err();
        assert!(error.message().contains("wrong wire type"));
    }

    #[test]
    fn checks_declared_count_and_trailing_payload() {
        let layout = CompiledLayout::parse(ROW_LAYOUT).unwrap();
        let mut payload = Vec::new();
        push_row(&mut payload, 1, 1, 0, &[], &[]);
        payload.push(0xff);
        let mut rows = layout.rows(&payload, 1).unwrap();
        rows.render_next(&mut Vec::new()).unwrap().unwrap();
        assert!(rows.finish().unwrap_err().message().contains("trailing"));

        let mut payload = Vec::new();
        push_row(&mut payload, 1, 1, 0, &[], &[]);
        let mut rows = layout.rows(&payload, 2).unwrap();
        rows.render_next(&mut Vec::new()).unwrap().unwrap();
        assert!(
            rows.render_next(&mut Vec::new())
                .unwrap_err()
                .message()
                .contains("ended early")
        );
    }

    fn push_row(
        output: &mut Vec<u8>,
        local_ref: u32,
        order: u64,
        terminator: u8,
        bytes: &[u8],
        cells: &[&str],
    ) {
        output.extend_from_slice(&local_ref.to_le_bytes());
        output.extend_from_slice(&order.to_le_bytes());
        output.push(terminator);
        output.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        output.extend_from_slice(bytes);
        output.extend_from_slice(&(cells.len() as u16).to_le_bytes());
        for cell in cells {
            output.extend_from_slice(&(cell.len() as u32).to_le_bytes());
            output.extend_from_slice(cell.as_bytes());
        }
    }
}
