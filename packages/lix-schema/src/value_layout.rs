//! Schema v1 typed-row body encoding: the *value* half of the typed-row layout.
//!
//! Layout (envelope carries `schema_key`, `row_pk`, `schema_fingerprint`):
//!
//! ```text
//! [1B header: version:4 | wide_offsets:1 | reserved:3]
//! [null bitmap, ceil(k_nullable/8) bytes, NULLABLE columns only]
//! [fixed area, canonical schema order, schema-constant offsets; NULL slot zero-filled]
//! [offset array: (nvar-1) x u16 end-offsets, u32 iff wide_offsets]
//! [var area]
//! ```
//!
//! Primary-key columns are elided from the body.
//!
//! # Canonicality
//!
//! The load-bearing property is that **one logical value produces exactly one
//! byte string**. Two encodings of one value would mean two content addresses
//! for one snapshot, which breaks dedup and identity. Each type therefore has
//! exactly one admissible byte image, and every non-canonical spelling is
//! either normalised on the way in or rejected outright:
//!
//! | type | body image | canonicalisation | rejected |
//! |---|---|---|---|
//! | `boolean` | 1 fixed byte | — | any byte other than `0x00`/`0x01` on decode |
//! | `int8` | 8 fixed bytes, big-endian two's complement | — | — |
//! | `float8` | 8 fixed bytes, big-endian IEEE-754 | `-0.0` -> `+0.0` | NaN, +/-Inf |
//! | `uuid` | 16 fixed bytes, RFC 4122 field order | textual spelling parsed away | unparseable text |
//! | `timestamptz` | 8 fixed bytes, signed UTC microseconds, big-endian | input offset converted to UTC | out-of-range RFC 3339 input |
//! | `text` | raw UTF-8, var area | **none** (see below) | interior NUL |
//! | `jsonb` | canonical semantic JSON, UTF-8, var area | keys sorted, integral f64 -> i64, spelling normalised | NUL in string or key, non-finite number |
//!
//! `text` is deliberately **not** Unicode-normalised. Two normalisations of
//! "the same" string are distinct SQL `text` values under PostgreSQL semantics
//! and therefore distinct Lix values with distinct content addresses. This is a
//! decision, not an oversight: normalising would make the encoding non-injective
//! with respect to the value the user stored. `canonicality_text_is_not_unicode_normalised`
//! pins it.

use std::collections::BTreeMap;

use serde_json::Value;

/// Body format version carried in the high nibble of the header byte.
pub const BODY_VERSION: u8 = 1;
const HEADER_WIDE_OFFSETS: u8 = 0b0000_1000;

/// The seven Schema v1 types, in the form the body encoder consumes them.
#[derive(Debug, Clone, PartialEq)]
pub enum BodyValue {
    Null,
    Text(String),
    Uuid(uuid::Uuid),
    Int8(i64),
    Float8(f64),
    Boolean(bool),
    /// JSON `null` is `Jsonb(Value::Null)`; SQL NULL is `BodyValue::Null`.
    Jsonb(Value),
    /// Signed UTC microseconds since the Unix epoch.
    Timestamptz(i64),
}

/// Per-column plan: the two things the body encoder needs from the schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyColumn {
    pub kind: BodyKind,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    Boolean,
    Int8,
    Float8,
    Uuid,
    Text,
    Jsonb,
    Timestamptz,
}

impl BodyKind {
    /// Fixed-area width, or `None` for variable-width types.
    pub const fn fixed_width(self) -> Option<usize> {
        match self {
            Self::Boolean => Some(1),
            Self::Int8 | Self::Float8 | Self::Timestamptz => Some(8),
            Self::Uuid => Some(16),
            Self::Text | Self::Jsonb => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError(pub String);

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(message: impl Into<String>) -> Result<T, EncodeError> {
    Err(EncodeError(message.into()))
}

// ---------------------------------------------------------------------------
// Scalar canonicalisation
// ---------------------------------------------------------------------------

/// Canonical 8-byte image of a `float8`.
///
/// `-0.0` and `+0.0` are the *same* value under IEEE-754 `==` and under Rust's
/// `PartialEq`, but have different bit patterns. Emitting both would give one
/// logical value two content addresses, so the sign of zero is erased here.
/// Non-finite values are rejected: `lix_schema`'s row validation already
/// requires `f64::is_finite`, so NaN payloads and signalling bits cannot reach
/// a conforming writer, and this is the mechanical backstop for the ones that do.
pub fn canonical_float8_bits(value: f64) -> Result<[u8; 8], EncodeError> {
    if !value.is_finite() {
        return err(format!(
            "float8 cannot represent {value}: Schema v1 admits finite values only"
        ));
    }
    // `+ 0.0` maps -0.0 to +0.0 and is the identity on every other finite f64.
    let canonical = value + 0.0;
    Ok(canonical.to_be_bytes())
}

/// Canonical 8-byte image of a `int8`.
///
/// Big-endian two's complement, matching the incumbent `typed_slots`
/// (`i64::from_be_bytes`) so the two representations agree, and so a
/// sign-flipped fixed slot sorts by `memcmp`.
pub const fn canonical_int8_bits(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Canonical 1-byte image of a `boolean`. Only `0x00` and `0x01` are legal.
pub const fn canonical_boolean_byte(value: bool) -> u8 {
    value as u8
}

/// Canonical 16-byte image of a `uuid`: RFC 4122 field order, which is what
/// `Uuid::into_bytes` returns. Every textual spelling (hyphenated, simple,
/// braced, URN, upper or lower case) parses to the same 16 bytes.
pub const fn canonical_uuid_bytes(value: uuid::Uuid) -> [u8; 16] {
    value.into_bytes()
}

/// Canonical bytes of a `text` value: the raw UTF-8, unnormalised.
///
/// `String` is UTF-8 by construction, and `serde_json` rejects invalid UTF-8
/// and unpaired surrogates while parsing, so validity is a type invariant here
/// rather than a check. Interior NUL is rejected to match the JSONB rule and
/// PostgreSQL's own `text` restriction.
pub fn canonical_text_bytes(value: &str) -> Result<&[u8], EncodeError> {
    if value.contains('\0') {
        return err("text cannot contain an interior NUL");
    }
    Ok(value.as_bytes())
}

/// Canonical semantic JSON for a `jsonb` value.
///
/// Mirrors `lix::sql2::udfs::common::parse_jsonb` + `serde_json::to_string`,
/// the function the public write path already routes through: object keys
/// sorted, NUL rejected in keys and strings, and integral non-integer numbers
/// within +/-2^53 folded to integers so `1.0` and `1` are one value.
/// Non-finite numbers cannot occur (JSON has no syntax for them) but the fold
/// guards for them anyway.
pub fn canonical_jsonb_bytes(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let mut normalised = value.clone();
    normalise_jsonb(&mut normalised)?;
    serde_json::to_vec(&normalised).map_err(|error| EncodeError(error.to_string()))
}

fn normalise_jsonb(value: &mut Value) -> Result<(), EncodeError> {
    match value {
        Value::String(text) => reject_nul(text)?,
        Value::Array(values) => {
            for value in values {
                normalise_jsonb(value)?;
            }
        }
        Value::Object(values) => {
            let old = std::mem::take(values);
            let mut entries = old.into_iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (key, mut value) in entries {
                reject_nul(&key)?;
                normalise_jsonb(&mut value)?;
                values.insert(key, value);
            }
        }
        Value::Number(number) if !number.is_i64() && !number.is_u64() => {
            if let Some(number) = number.as_f64() {
                if !number.is_finite() {
                    return err("jsonb cannot represent a non-finite number");
                }
                if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
                    *value = Value::from(number as i64);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn reject_nul(value: &str) -> Result<(), EncodeError> {
    if value.contains('\0') {
        err("jsonb cannot represent the Unicode NUL escape (\\u0000)")
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Body encoding
// ---------------------------------------------------------------------------

/// Encode one row body. `plan` is the non-PK columns in canonical schema order;
/// `values` is positionally aligned with it.
///
/// `output` is cleared first: the encoder owns every byte it emits, so a reused
/// buffer cannot leave stale bytes in a zero-filled NULL fixed slot.
pub fn encode_body(
    plan: &[BodyColumn],
    values: &[BodyValue],
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    output.clear();
    if plan.len() != values.len() {
        return err(format!(
            "plan has {} columns but {} values were supplied",
            plan.len(),
            values.len()
        ));
    }

    // Var-area payloads are materialised first so the offset width is known
    // before the header byte is written.
    let mut var_payloads: Vec<Vec<u8>> = Vec::new();
    for (column, value) in plan.iter().zip(values) {
        check_value_kind(column, value)?;
        if column.kind.fixed_width().is_some() {
            continue;
        }
        var_payloads.push(match value {
            BodyValue::Null => Vec::new(),
            BodyValue::Text(text) => canonical_text_bytes(text)?.to_vec(),
            BodyValue::Jsonb(json) => canonical_jsonb_bytes(json)?,
            _ => unreachable!("check_value_kind admits only Text/Jsonb/Null here"),
        });
    }

    let var_total: usize = var_payloads.iter().map(Vec::len).sum();
    // A single u16 end-offset is elided (the last var column ends at the body
    // end), so only the *interior* boundaries decide the width.
    let wide_offsets = var_total > usize::from(u16::MAX);

    let mut header = BODY_VERSION << 4;
    if wide_offsets {
        header |= HEADER_WIDE_OFFSETS;
    }
    output.push(header);

    // Null bitmap: nullable columns only, in canonical schema order.
    let nullable_count = plan.iter().filter(|column| column.nullable).count();
    let bitmap_bytes = nullable_count.div_ceil(8);
    let bitmap_start = output.len();
    output.resize(bitmap_start + bitmap_bytes, 0);
    let mut nullable_index = 0usize;
    for (column, value) in plan.iter().zip(values) {
        if !column.nullable {
            if matches!(value, BodyValue::Null) {
                return err("NULL supplied for a NOT NULL column");
            }
            continue;
        }
        if matches!(value, BodyValue::Null) {
            output[bitmap_start + nullable_index / 8] |= 1 << (nullable_index % 8);
        }
        nullable_index += 1;
    }

    // Fixed area, canonical schema order, schema-constant offsets.
    for (column, value) in plan.iter().zip(values) {
        let Some(width) = column.kind.fixed_width() else {
            continue;
        };
        match value {
            // Zero-filled: `resize` writes zeroes into freshly grown capacity,
            // and `output` was cleared at entry, so no prior content survives.
            BodyValue::Null => output.resize(output.len() + width, 0),
            BodyValue::Boolean(flag) => output.push(canonical_boolean_byte(*flag)),
            BodyValue::Int8(number) => {
                output.extend_from_slice(&canonical_int8_bits(*number));
            }
            BodyValue::Timestamptz(micros) => {
                output.extend_from_slice(&micros.to_be_bytes());
            }
            BodyValue::Float8(number) => {
                output.extend_from_slice(&canonical_float8_bits(*number)?);
            }
            BodyValue::Uuid(value) => output.extend_from_slice(&canonical_uuid_bytes(*value)),
            _ => unreachable!("check_value_kind admits only fixed kinds here"),
        }
    }

    // Offset array: (nvar - 1) interior end-offsets. The final end is implied.
    if var_payloads.len() > 1 {
        let mut end = 0usize;
        for payload in &var_payloads[..var_payloads.len() - 1] {
            end += payload.len();
            if wide_offsets {
                output.extend_from_slice(&(end as u32).to_le_bytes());
            } else {
                output.extend_from_slice(&(end as u16).to_le_bytes());
            }
        }
    }

    for payload in &var_payloads {
        output.extend_from_slice(payload);
    }
    Ok(())
}

fn check_value_kind(column: &BodyColumn, value: &BodyValue) -> Result<(), EncodeError> {
    let ok = match (column.kind, value) {
        (_, BodyValue::Null) => true,
        (BodyKind::Boolean, BodyValue::Boolean(_))
        | (BodyKind::Int8, BodyValue::Int8(_))
        | (BodyKind::Float8, BodyValue::Float8(_))
        | (BodyKind::Uuid, BodyValue::Uuid(_))
        | (BodyKind::Text, BodyValue::Text(_))
        | (BodyKind::Jsonb, BodyValue::Jsonb(_))
        | (BodyKind::Timestamptz, BodyValue::Timestamptz(_)) => true,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        err(format!(
            "value {value:?} does not match column kind {:?}",
            column.kind
        ))
    }
}

/// Encode a single value on its own, for per-type canonicality assertions.
pub fn encode_one(kind: BodyKind, value: &BodyValue) -> Result<Vec<u8>, EncodeError> {
    let mut output = Vec::new();
    encode_body(
        &[BodyColumn {
            kind,
            nullable: true,
        }],
        std::slice::from_ref(value),
        &mut output,
    )?;
    Ok(output)
}

// ---------------------------------------------------------------------------
// Decoding, for the totality/injectivity round trip
// ---------------------------------------------------------------------------

pub fn decode_body(plan: &[BodyColumn], body: &[u8]) -> Result<Vec<BodyValue>, EncodeError> {
    let Some((&header, rest)) = body.split_first() else {
        return err("body is empty");
    };
    if header >> 4 != BODY_VERSION {
        return err(format!("unsupported body version {}", header >> 4));
    }
    if header & 0b0000_0111 != 0 {
        return err("reserved header bits must be zero");
    }
    let wide_offsets = header & HEADER_WIDE_OFFSETS != 0;

    let nullable_count = plan.iter().filter(|column| column.nullable).count();
    let bitmap_bytes = nullable_count.div_ceil(8);
    if rest.len() < bitmap_bytes {
        return err("body is shorter than its null bitmap");
    }
    let (bitmap, mut cursor) = rest.split_at(bitmap_bytes);

    let mut is_null = Vec::with_capacity(plan.len());
    let mut nullable_index = 0usize;
    for column in plan {
        if column.nullable {
            is_null.push(bitmap[nullable_index / 8] & (1 << (nullable_index % 8)) != 0);
            nullable_index += 1;
        } else {
            is_null.push(false);
        }
    }

    let mut fixed = BTreeMap::new();
    for (index, column) in plan.iter().enumerate() {
        let Some(width) = column.kind.fixed_width() else {
            continue;
        };
        if cursor.len() < width {
            return err("body is shorter than its fixed area");
        }
        let (slot, rest) = cursor.split_at(width);
        cursor = rest;
        if is_null[index] {
            if slot.iter().any(|&byte| byte != 0) {
                return err("NULL fixed slot is not zero-filled");
            }
            fixed.insert(index, BodyValue::Null);
            continue;
        }
        let value = match column.kind {
            BodyKind::Boolean => match slot[0] {
                0 => BodyValue::Boolean(false),
                1 => BodyValue::Boolean(true),
                other => return err(format!("boolean byte {other:#04x} is not 0x00 or 0x01")),
            },
            BodyKind::Int8 => BodyValue::Int8(i64::from_be_bytes(slot.try_into().unwrap())),
            BodyKind::Timestamptz => {
                BodyValue::Timestamptz(i64::from_be_bytes(slot.try_into().unwrap()))
            }
            BodyKind::Float8 => {
                let number = f64::from_be_bytes(slot.try_into().unwrap());
                if !number.is_finite() {
                    return err("float8 slot holds a non-finite value");
                }
                if number == 0.0 && number.is_sign_negative() {
                    return err("float8 slot holds -0.0, which is not canonical");
                }
                BodyValue::Float8(number)
            }
            BodyKind::Uuid => BodyValue::Uuid(uuid::Uuid::from_bytes(slot.try_into().unwrap())),
            BodyKind::Text | BodyKind::Jsonb => unreachable!(),
        };
        fixed.insert(index, value);
    }

    let var_indices = plan
        .iter()
        .enumerate()
        .filter(|(_, column)| column.kind.fixed_width().is_none())
        .map(|(index, _)| index)
        .collect::<Vec<_>>();

    let offset_width = if wide_offsets { 4 } else { 2 };
    let offset_bytes = var_indices.len().saturating_sub(1) * offset_width;
    if cursor.len() < offset_bytes {
        return err("body is shorter than its offset array");
    }
    let (offsets, var_area) = cursor.split_at(offset_bytes);

    let mut ends = Vec::with_capacity(var_indices.len());
    for chunk in offsets.chunks_exact(offset_width) {
        ends.push(if wide_offsets {
            u32::from_le_bytes(chunk.try_into().unwrap()) as usize
        } else {
            u16::from_le_bytes(chunk.try_into().unwrap()) as usize
        });
    }
    if !var_indices.is_empty() {
        ends.push(var_area.len());
    }

    let mut decoded = vec![BodyValue::Null; plan.len()];
    for (index, value) in fixed {
        decoded[index] = value;
    }
    let mut start = 0usize;
    for (slot, &index) in var_indices.iter().enumerate() {
        let end = ends[slot];
        if end < start || end > var_area.len() {
            return err("var-area offsets are not monotonic and in range");
        }
        let payload = &var_area[start..end];
        start = end;
        if is_null[index] {
            if !payload.is_empty() {
                return err("NULL var slot has a non-empty payload");
            }
            continue;
        }
        decoded[index] = match plan[index].kind {
            BodyKind::Text => BodyValue::Text(
                std::str::from_utf8(payload)
                    .map_err(|error| EncodeError(error.to_string()))?
                    .to_owned(),
            ),
            BodyKind::Jsonb => BodyValue::Jsonb(
                serde_json::from_slice(payload).map_err(|error| EncodeError(error.to_string()))?,
            ),
            _ => unreachable!(),
        };
    }
    Ok(decoded)
}
