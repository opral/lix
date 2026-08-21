use std::borrow::Cow;
use std::ops::{Deref, DerefMut, Range};
use std::sync::{Arc, OnceLock};

use serde_json::Value as JsonValue;

const MAX_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 128;

/// A native JSONB value.
///
/// Component ingress retains the validated canonical binary representation
/// and materializes a JSON DOM only when a consumer inspects it. Values built
/// by plugins remain ordinary owned JSON until their first wire encoding.
#[derive(Debug, Clone)]
pub struct Jsonb(JsonbRepr);

#[derive(Debug, Clone)]
enum JsonbRepr {
    Value(JsonValue),
    Binary(BinaryJsonb),
    CanonicalText(CanonicalTextJsonb),
    TextArray(TextArrayJsonb),
}

#[derive(Debug, Clone)]
struct TextArrayJsonb {
    values: Vec<String>,
    binary_len: usize,
    value: OnceLock<JsonValue>,
}

#[derive(Debug, Clone)]
struct BinaryJsonb {
    bytes: BinaryBytes,
    value: OnceLock<JsonValue>,
}

#[derive(Debug, Clone)]
struct CanonicalTextJsonb {
    bytes: BinaryBytes,
    value: OnceLock<JsonValue>,
    binary: OnceLock<Vec<u8>>,
}

#[derive(Debug, Clone)]
enum BinaryBytes {
    Shared(Arc<[u8]>),
    SharedSlice {
        bytes: Arc<[u8]>,
        range: Range<usize>,
    },
    SharedVecSlice {
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    },
    Owned(Vec<u8>),
}

impl Deref for BinaryBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Shared(bytes) => bytes,
            Self::SharedSlice { bytes, range } => &bytes[range.clone()],
            Self::SharedVecSlice { bytes, range } => &bytes[range.clone()],
            Self::Owned(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for BinaryBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq for BinaryBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsonbError(pub &'static str);

impl std::fmt::Display for JsonbError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for JsonbError {}

impl Jsonb {
    pub fn from_value(value: JsonValue) -> Self {
        Self(JsonbRepr::Value(value))
    }

    /// Builds the canonical binary representation of a JSON string array
    /// directly, consuming the strings without first allocating a JSON DOM.
    pub fn from_text_array(values: Vec<String>) -> Result<Self, JsonbError> {
        let size = values.iter().try_fold(5usize, |size, value| {
            if value.contains('\0') {
                return Err(JsonbError("JSONB string contains a NUL"));
            }
            size.checked_add(5 + value.len())
                .ok_or(JsonbError("JSONB array is too large"))
        })?;
        u32::try_from(values.len()).map_err(|_| JsonbError("JSONB array is too large"))?;
        if size > MAX_BYTES {
            return Err(JsonbError("JSONB value is too large"));
        }
        Ok(Self(JsonbRepr::TextArray(TextArrayJsonb {
            values,
            binary_len: size,
            value: OnceLock::new(),
        })))
    }

    /// Validates and retains one canonical binary JSONB value without
    /// constructing a DOM.
    pub fn from_binary(bytes: Arc<[u8]>) -> Result<Self, JsonbError> {
        validate_binary(&bytes)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::Shared(bytes),
            value: OnceLock::new(),
        })))
    }

    /// Retains a validated JSONB value as a range of a shared typed page.
    ///
    /// This is intentionally hidden from the public schema model: it lets the
    /// component decoder keep one page allocation instead of copying every
    /// inline JSONB cell into a separate allocation.
    #[doc(hidden)]
    pub fn from_binary_slice(bytes: Arc<[u8]>, range: Range<usize>) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        validate_binary(value)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::SharedSlice { bytes, range },
            value: OnceLock::new(),
        })))
    }

    /// Retains a validated range of an owned typed-page buffer without
    /// converting that `Vec` into a second page allocation.
    #[doc(hidden)]
    pub fn from_binary_vec_slice(
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        validate_binary(value)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::SharedVecSlice { bytes, range },
            value: OnceLock::new(),
        })))
    }

    /// Validates and consumes canonical binary JSONB bytes without copying the
    /// component attachment allocation.
    pub fn from_binary_vec(bytes: Vec<u8>) -> Result<Self, JsonbError> {
        validate_binary(&bytes)?;
        Ok(Self(JsonbRepr::Binary(BinaryJsonb {
            bytes: BinaryBytes::Owned(bytes),
            value: OnceLock::new(),
        })))
    }

    /// Retains canonical JSON text as a range of an owned typed-page buffer.
    /// Consumers that only forward canonical JSON avoid constructing a DOM.
    #[doc(hidden)]
    pub fn from_canonical_text_vec_slice(
        bytes: Arc<Vec<u8>>,
        range: Range<usize>,
    ) -> Result<Self, JsonbError> {
        let value = bytes
            .get(range.clone())
            .ok_or(JsonbError("JSONB shared range is out of bounds"))?;
        validate_canonical_json_text(value)?;
        Ok(Self(JsonbRepr::CanonicalText(CanonicalTextJsonb {
            bytes: BinaryBytes::SharedVecSlice { bytes, range },
            value: OnceLock::new(),
            binary: OnceLock::new(),
        })))
    }

    /// Validates and consumes canonical JSON text without materializing a DOM.
    #[doc(hidden)]
    pub fn from_canonical_text_vec(bytes: Vec<u8>) -> Result<Self, JsonbError> {
        validate_canonical_json_text(&bytes)?;
        Ok(Self(JsonbRepr::CanonicalText(CanonicalTextJsonb {
            bytes: BinaryBytes::Owned(bytes),
            value: OnceLock::new(),
            binary: OnceLock::new(),
        })))
    }

    pub fn as_value(&self) -> &JsonValue {
        match &self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(binary) => binary
                .value
                .get_or_init(|| decode_binary(&binary.bytes).expect("validated JSONB decodes")),
            JsonbRepr::CanonicalText(text) => text.value.get_or_init(|| {
                serde_json::from_slice(&text.bytes).expect("validated canonical JSON decodes")
            }),
            JsonbRepr::TextArray(array) => array.value.get_or_init(|| {
                JsonValue::Array(
                    array
                        .values
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect(),
                )
            }),
        }
    }

    pub fn into_value(self) -> JsonValue {
        match self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(binary) => binary
                .value
                .into_inner()
                .unwrap_or_else(|| decode_binary(&binary.bytes).expect("validated JSONB decodes")),
            JsonbRepr::CanonicalText(text) => text.value.into_inner().unwrap_or_else(|| {
                serde_json::from_slice(&text.bytes).expect("validated canonical JSON decodes")
            }),
            JsonbRepr::TextArray(array) => array.value.into_inner().unwrap_or_else(|| {
                JsonValue::Array(array.values.into_iter().map(JsonValue::String).collect())
            }),
        }
    }

    pub fn as_value_mut(&mut self) -> &mut JsonValue {
        if !matches!(self.0, JsonbRepr::Value(_)) {
            let value = self.as_value().clone();
            self.0 = JsonbRepr::Value(value);
        }
        match &mut self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(_) | JsonbRepr::CanonicalText(_) | JsonbRepr::TextArray(_) => {
                unreachable!("native JSONB was materialized above")
            }
        }
    }

    pub fn binary(&self) -> Result<Cow<'_, [u8]>, JsonbError> {
        match &self.0 {
            JsonbRepr::Value(value) => encode_binary(value).map(Cow::Owned),
            JsonbRepr::Binary(binary) => Ok(Cow::Borrowed(&binary.bytes)),
            JsonbRepr::CanonicalText(text) => Ok(Cow::Borrowed(text.binary.get_or_init(|| {
                encode_binary(text.value.get_or_init(|| {
                    serde_json::from_slice(&text.bytes).expect("validated canonical JSON decodes")
                }))
                .expect("validated canonical JSON encodes")
            }))),
            JsonbRepr::TextArray(array) => {
                let mut bytes = Vec::with_capacity(array.binary_len);
                append_text_array_binary(&mut bytes, &array.values)?;
                Ok(Cow::Owned(bytes))
            }
        }
    }

    /// Exact canonical binary width without materializing an intermediate
    /// JSONB buffer for native text arrays.
    #[doc(hidden)]
    pub fn binary_len(&self) -> Result<usize, JsonbError> {
        match &self.0 {
            JsonbRepr::Value(value) => usize::try_from(estimated_value_size(value))
                .map_err(|_| JsonbError("JSONB value is too large")),
            JsonbRepr::Binary(binary) => Ok(binary.bytes.len()),
            JsonbRepr::CanonicalText(_) => self.binary().map(|bytes| bytes.len()),
            JsonbRepr::TextArray(array) => Ok(array.binary_len),
        }
    }

    /// Appends canonical binary bytes directly to an existing typed page.
    #[doc(hidden)]
    pub fn append_binary(&self, output: &mut Vec<u8>) -> Result<(), JsonbError> {
        let start = output.len();
        let result = match &self.0 {
            JsonbRepr::Value(value) => encode_node(output, value, 0),
            JsonbRepr::Binary(binary) => {
                output.extend_from_slice(&binary.bytes);
                Ok(())
            }
            JsonbRepr::CanonicalText(_) => {
                output.extend_from_slice(&self.binary()?);
                Ok(())
            }
            JsonbRepr::TextArray(array) => append_text_array_binary(output, &array.values),
        };
        if result.is_err() {
            output.truncate(start);
        }
        result
    }

    pub fn is_valid(&self) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_value_valid(value),
            JsonbRepr::Binary(_) => true,
            JsonbRepr::CanonicalText(_) => true,
            JsonbRepr::TextArray(_) => true,
        }
    }

    pub fn is_binary(&self) -> bool {
        matches!(self.0, JsonbRepr::Binary(_) | JsonbRepr::TextArray(_))
    }

    pub fn estimated_binary_size(&self) -> u64 {
        match &self.0 {
            JsonbRepr::Binary(binary) => binary.bytes.len() as u64,
            JsonbRepr::CanonicalText(text) => text.bytes.len() as u64,
            JsonbRepr::Value(value) => estimated_value_size(value),
            JsonbRepr::TextArray(array) => {
                u64::try_from(text_array_binary_size(&array.values).unwrap_or(usize::MAX))
                    .unwrap_or(u64::MAX)
            }
        }
    }

    /// Renders canonical JSON text without materializing a DOM for values
    /// already held in the native binary representation.
    pub fn to_json_string(&self) -> Result<String, JsonbError> {
        let capacity = match &self.0 {
            JsonbRepr::CanonicalText(text) => text.bytes.len(),
            _ => self.binary_len()?,
        };
        let mut output = Vec::with_capacity(capacity);
        self.append_canonical_json(&mut output)?;
        // SAFETY: the appenders emit ASCII syntax around validated UTF-8.
        Ok(unsafe { String::from_utf8_unchecked(output) })
    }

    /// Appends this value's canonical compact JSON text without constructing
    /// an intermediate string or materializing binary-backed values as a DOM.
    pub fn append_canonical_json(&self, output: &mut Vec<u8>) -> Result<(), JsonbError> {
        let start = output.len();
        let result = match &self.0 {
            JsonbRepr::Value(value) => append_value_json(output, value, 0),
            JsonbRepr::Binary(binary) => {
                let mut reader = Reader {
                    bytes: &binary.bytes,
                    offset: 0,
                };
                reader.write_json(output, 0)
            }
            JsonbRepr::CanonicalText(text) => {
                output.extend_from_slice(&text.bytes);
                Ok(())
            }
            JsonbRepr::TextArray(array) => append_text_array_json(output, &array.values),
        };
        if result.is_err() {
            output.truncate(start);
            return result;
        }
        Ok(())
    }
}

impl From<JsonValue> for Jsonb {
    fn from(value: JsonValue) -> Self {
        Self::from_value(value)
    }
}

impl Deref for Jsonb {
    type Target = JsonValue;

    fn deref(&self) -> &Self::Target {
        self.as_value()
    }
}

impl DerefMut for Jsonb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_value_mut()
    }
}

impl AsRef<JsonValue> for Jsonb {
    fn as_ref(&self) -> &JsonValue {
        self.as_value()
    }
}

impl PartialEq for Jsonb {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (JsonbRepr::Value(left), JsonbRepr::Value(right)) => json_values_eq(left, right),
            (JsonbRepr::CanonicalText(left), JsonbRepr::CanonicalText(right)) => {
                left.bytes == right.bytes
            }
            _ => match (self.binary(), other.binary()) {
                (Ok(left), Ok(right)) => left == right,
                _ => false,
            },
        }
    }
}

// Canonical JSONB equality is reflexive: every constructor validates values
// that can be encoded canonically, and equality compares that canonical form.
impl Eq for Jsonb {}

impl PartialEq<JsonValue> for Jsonb {
    fn eq(&self, other: &JsonValue) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_values_eq(value, other),
            JsonbRepr::Binary(binary) => encode_binary(other)
                .is_ok_and(|encoded| encoded.as_slice() == binary.bytes.as_ref()),
            JsonbRepr::CanonicalText(_) | JsonbRepr::TextArray(_) => self
                .binary()
                .is_ok_and(|left| encode_binary(other).is_ok_and(|right| left == right)),
        }
    }
}

impl PartialEq<Jsonb> for JsonValue {
    fn eq(&self, other: &Jsonb) -> bool {
        other == self
    }
}

fn json_values_eq(left: &JsonValue, right: &JsonValue) -> bool {
    left == right
        || match (encode_binary(left), encode_binary(right)) {
            (Ok(left), Ok(right)) => left == right,
            // `from_value` is intentionally infallible, so invalid JSONB can
            // exist until validation or encoding. Raw equality above preserves
            // reflexivity without letting invalid data equal valid binary data.
            (Ok(_), Err(_)) | (Err(_), Ok(_)) | (Err(_), Err(_)) => false,
        }
}

impl serde::Serialize for Jsonb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(self.as_value(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Jsonb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer).map(Self::from_value)
    }
}

pub fn encode_binary(value: &JsonValue) -> Result<Vec<u8>, JsonbError> {
    let mut bytes = Vec::new();
    encode_node(&mut bytes, value, 0)?;
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    Ok(bytes)
}

fn text_array_binary_size(values: &[String]) -> Result<usize, JsonbError> {
    let size = values.iter().try_fold(5usize, |size, value| {
        size.checked_add(5 + value.len())
            .ok_or(JsonbError("JSONB array is too large"))
    })?;
    if size > MAX_BYTES || u32::try_from(values.len()).is_err() {
        return Err(JsonbError("JSONB array is too large"));
    }
    Ok(size)
}

fn append_text_array_binary(output: &mut Vec<u8>, values: &[String]) -> Result<(), JsonbError> {
    // `TextArrayJsonb` validates all lengths and interior NULs once at
    // construction. Page encoding is its hot path, so do not rescan every
    // string after `binary_len` has already supplied the exact framed width.
    output.push(7);
    output.extend_from_slice(&(values.len() as u32).to_be_bytes());
    for value in values {
        output.push(6);
        output.extend_from_slice(&(value.len() as u32).to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

pub fn validate_binary(bytes: &[u8]) -> Result<(), JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    reader.validate_value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB value has trailing bytes"));
    }
    Ok(())
}

/// Validates canonical compact UTF-8 JSON text using the same normalization
/// rules as native JSONB, without constructing a JSON DOM.
///
/// The returned string borrows the input and is safe to retain as canonical
/// JSON text. Object-key decoding allocates only when a key contains an escape
/// sequence and therefore cannot be compared in its borrowed representation.
pub fn validate_canonical_json_text(bytes: &[u8]) -> Result<&str, JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| JsonbError("JSONB text is not UTF-8"))?;
    let mut reader = CanonicalTextReader {
        text,
        bytes,
        offset: 0,
    };
    reader.value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB text has trailing bytes"));
    }
    Ok(text)
}

struct CanonicalTextReader<'a> {
    text: &'a str,
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CanonicalTextReader<'a> {
    fn value(&mut self, depth: usize) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.peek()? {
            b'n' => self.literal(b"null"),
            b'f' => self.literal(b"false"),
            b't' => self.literal(b"true"),
            b'"' => self.string(false).map(|_| ()),
            b'[' => self.array(depth),
            b'{' => self.object(depth),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(JsonbError("JSONB text value is invalid")),
        }
    }

    fn peek(&self) -> Result<u8, JsonbError> {
        self.bytes
            .get(self.offset)
            .copied()
            .ok_or(JsonbError("JSONB text is truncated"))
    }

    fn byte(&mut self, expected: u8) -> Result<(), JsonbError> {
        if self.peek()? != expected {
            return Err(JsonbError("JSONB text punctuation is invalid"));
        }
        self.offset += 1;
        Ok(())
    }

    fn literal(&mut self, literal: &[u8]) -> Result<(), JsonbError> {
        if self.bytes.get(self.offset..self.offset + literal.len()) != Some(literal) {
            return Err(JsonbError("JSONB text literal is invalid"));
        }
        self.offset += literal.len();
        Ok(())
    }

    fn array(&mut self, depth: usize) -> Result<(), JsonbError> {
        self.byte(b'[')?;
        if self.peek()? == b']' {
            self.offset += 1;
            return Ok(());
        }
        loop {
            self.value(depth + 1)?;
            match self.peek()? {
                b',' => self.offset += 1,
                b']' => {
                    self.offset += 1;
                    return Ok(());
                }
                _ => return Err(JsonbError("JSONB text array is invalid")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<(), JsonbError> {
        self.byte(b'{')?;
        if self.peek()? == b'}' {
            self.offset += 1;
            return Ok(());
        }
        let mut previous: Option<Cow<'a, str>> = None;
        loop {
            let key = self.string(true)?;
            if previous
                .as_deref()
                .is_some_and(|previous| previous >= key.as_ref())
            {
                return Err(JsonbError("JSONB text keys are not canonical"));
            }
            self.byte(b':')?;
            self.value(depth + 1)?;
            previous = Some(key);
            match self.peek()? {
                b',' => self.offset += 1,
                b'}' => {
                    self.offset += 1;
                    return Ok(());
                }
                _ => return Err(JsonbError("JSONB text object is invalid")),
            }
        }
    }

    fn string(&mut self, decode_escapes: bool) -> Result<Cow<'a, str>, JsonbError> {
        self.byte(b'"')?;
        let start = self.offset;
        let mut escaped = false;
        loop {
            match self.peek()? {
                b'"' => {
                    let end = self.offset;
                    self.offset += 1;
                    let encoded = &self.text[start..end];
                    return if escaped && decode_escapes {
                        decode_canonical_string(encoded).map(Cow::Owned)
                    } else {
                        Ok(Cow::Borrowed(encoded))
                    };
                }
                b'\\' => {
                    escaped = true;
                    self.offset += 1;
                    self.validate_escape()?;
                }
                0x00..=0x1f => {
                    return Err(JsonbError("JSONB text string contains a control byte"));
                }
                _ => self.offset += 1,
            }
        }
    }

    fn validate_escape(&mut self) -> Result<(), JsonbError> {
        match self.peek()? {
            b'"' | b'\\' | b'b' | b't' | b'n' | b'f' | b'r' => {
                self.offset += 1;
                Ok(())
            }
            b'u' => {
                let digits = self
                    .bytes
                    .get(self.offset + 1..self.offset + 5)
                    .ok_or(JsonbError("JSONB text escape is truncated"))?;
                let code = canonical_control_escape(digits)?;
                if code == 0 {
                    return Err(JsonbError("JSONB string contains a NUL"));
                }
                if matches!(code, 0x08 | 0x09 | 0x0a | 0x0c | 0x0d) {
                    return Err(JsonbError("JSONB text escape is not canonical"));
                }
                self.offset += 5;
                Ok(())
            }
            _ => Err(JsonbError("JSONB text escape is not canonical")),
        }
    }

    fn number(&mut self) -> Result<(), JsonbError> {
        let start = self.offset;
        while self
            .bytes
            .get(self.offset)
            .is_some_and(|byte| matches!(byte, b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'))
        {
            self.offset += 1;
        }
        let encoded = &self.text[start..self.offset];
        if !encoded
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b'.' | b'e' | b'E'))
            && let Some(canonical) = canonical_integer_text(encoded.as_bytes())
        {
            return canonical
                .then_some(())
                .ok_or(JsonbError("JSONB text number is not canonical"));
        }
        let number = serde_json::from_str::<serde_json::Number>(encoded)
            .map_err(|_| JsonbError("JSONB text number is invalid"))?;
        if !canonical_number_matches(&number, encoded.as_bytes())? {
            return Err(JsonbError("JSONB text number is not canonical"));
        }
        Ok(())
    }
}

/// Recognizes canonical integer spellings that fit serde_json's native
/// signed/unsigned integer representation. `None` leaves oversized values to
/// the existing binary64 round-trip check; accepting those lexically would be
/// incorrect because their canonical spelling may be scientific notation.
fn canonical_integer_text(encoded: &[u8]) -> Option<bool> {
    let (negative, digits) = match encoded.first() {
        Some(b'-') => (true, &encoded[1..]),
        _ => (false, encoded),
    };
    if digits.is_empty()
        || !digits.iter().all(u8::is_ascii_digit)
        || (digits.len() > 1 && digits[0] == b'0')
    {
        return Some(false);
    }
    let mut magnitude = 0_u64;
    for digit in digits {
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add(u64::from(*digit - b'0'))?;
    }
    if negative {
        Some(magnitude != 0 && magnitude <= (i64::MAX as u64) + 1)
    } else {
        Some(true)
    }
}

fn canonical_control_escape(digits: &[u8]) -> Result<u8, JsonbError> {
    if digits.len() != 4 || digits[0] != b'0' || digits[1] != b'0' {
        return Err(JsonbError("JSONB text escape is not canonical"));
    }
    let hex = |digit| match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    };
    let high = hex(digits[2]).ok_or(JsonbError("JSONB text escape is not canonical"))?;
    let low = hex(digits[3]).ok_or(JsonbError("JSONB text escape is not canonical"))?;
    let code = high * 16 + low;
    if code > 0x1f {
        return Err(JsonbError("JSONB text escape is not canonical"));
    }
    Ok(code)
}

fn decode_canonical_string(encoded: &str) -> Result<String, JsonbError> {
    let bytes = encoded.as_bytes();
    let mut output = String::with_capacity(encoded.len());
    let mut offset = 0usize;
    let mut plain_start = 0usize;
    while offset < bytes.len() {
        if bytes[offset] != b'\\' {
            offset += 1;
            continue;
        }
        output.push_str(&encoded[plain_start..offset]);
        offset += 1;
        match bytes[offset] {
            b'"' => output.push('"'),
            b'\\' => output.push('\\'),
            b'b' => output.push('\u{0008}'),
            b't' => output.push('\t'),
            b'n' => output.push('\n'),
            b'f' => output.push('\u{000c}'),
            b'r' => output.push('\r'),
            b'u' => {
                let code = canonical_control_escape(&bytes[offset + 1..offset + 5])?;
                output.push(char::from(code));
                offset += 4;
            }
            _ => return Err(JsonbError("JSONB text escape is not canonical")),
        }
        offset += 1;
        plain_start = offset;
    }
    output.push_str(&encoded[plain_start..]);
    Ok(output)
}

fn canonical_number_matches(
    number: &serde_json::Number,
    encoded: &[u8],
) -> Result<bool, JsonbError> {
    if let Some(number) = number.as_i64() {
        Ok(itoa::Buffer::new().format(number).as_bytes() == encoded)
    } else if let Some(number) = number.as_u64() {
        Ok(itoa::Buffer::new().format(number).as_bytes() == encoded)
    } else {
        let number = number
            .as_f64()
            .filter(|number| number.is_finite())
            .ok_or(JsonbError("JSONB text number is invalid"))?;
        if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
            Ok(itoa::Buffer::new().format(number as i64).as_bytes() == encoded)
        } else {
            Ok(ryu::Buffer::new().format(number + 0.0).as_bytes() == encoded)
        }
    }
}

/// Validates and renders borrowed canonical binary JSONB without constructing
/// a [`Jsonb`], retaining the input, or materializing a JSON DOM.
pub fn binary_to_json_string(bytes: &[u8]) -> Result<String, JsonbError> {
    validate_binary(bytes)?;
    // SAFETY: `validate_binary` proved the complete canonical JSONB envelope.
    unsafe { validated_binary_to_json_string(bytes) }
}

/// Renders binary JSONB after the caller has already validated the complete
/// envelope. This avoids repeating recursive validation at typed wire and SQL
/// boundaries that carry an explicit validation proof.
///
/// # Safety
///
/// `bytes` must have passed [`validate_binary`] unchanged.
#[doc(hidden)]
pub unsafe fn validated_binary_to_json_string(bytes: &[u8]) -> Result<String, JsonbError> {
    let mut output = Vec::with_capacity(bytes.len());
    let mut reader = Reader { bytes, offset: 0 };
    reader.write_json(&mut output, 0)?;
    debug_assert_eq!(
        reader.offset,
        bytes.len(),
        "validated JSONB is fully consumed"
    );
    // SAFETY: JSON punctuation and numeric formatters emit ASCII; every copied
    // string was validated as UTF-8 by `Reader::string`, and the fallback JSON
    // serializer emits UTF-8. The caller's envelope proof excludes trailing
    // bytes that this projection deliberately does not re-scan.
    Ok(unsafe { String::from_utf8_unchecked(output) })
}

pub fn decode_binary(bytes: &[u8]) -> Result<JsonValue, JsonbError> {
    if bytes.len() > MAX_BYTES {
        return Err(JsonbError("JSONB value is too large"));
    }
    let mut reader = Reader { bytes, offset: 0 };
    let value = reader.decode_value(0)?;
    if reader.offset != bytes.len() {
        return Err(JsonbError("JSONB value has trailing bytes"));
    }
    Ok(value)
}

fn json_value_valid(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(value) => !value.contains('\0'),
        JsonValue::Array(values) => values.iter().all(json_value_valid),
        JsonValue::Object(values) => values
            .iter()
            .all(|(key, value)| !key.contains('\0') && json_value_valid(value)),
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => true,
    }
}

fn estimated_value_size(value: &JsonValue) -> u64 {
    match value {
        JsonValue::Null | JsonValue::Bool(_) => 1,
        JsonValue::Number(_) => 9,
        JsonValue::String(value) => 5_u64.saturating_add(value.len() as u64),
        JsonValue::Array(values) => values.iter().fold(5, |size, value| {
            size.saturating_add(estimated_value_size(value))
        }),
        JsonValue::Object(values) => values.iter().fold(5, |size, (key, value)| {
            size.saturating_add(4)
                .saturating_add(key.len() as u64)
                .saturating_add(estimated_value_size(value))
        }),
    }
}

fn encode_node(bytes: &mut Vec<u8>, value: &JsonValue, depth: usize) -> Result<(), JsonbError> {
    if depth > MAX_DEPTH {
        return Err(JsonbError("JSONB value exceeds its nesting limit"));
    }
    match value {
        JsonValue::Null => bytes.push(0),
        JsonValue::Bool(false) => bytes.push(1),
        JsonValue::Bool(true) => bytes.push(2),
        JsonValue::Number(number) => {
            if let Some(number) = number.as_i64() {
                bytes.push(3);
                bytes.extend_from_slice(&number.to_be_bytes());
            } else if let Some(number) = number.as_u64() {
                bytes.push(4);
                bytes.extend_from_slice(&number.to_be_bytes());
            } else {
                let number = number
                    .as_f64()
                    .filter(|number| number.is_finite())
                    .ok_or(JsonbError("JSONB number is not finite"))?;
                if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
                    bytes.push(3);
                    bytes.extend_from_slice(&(number as i64).to_be_bytes());
                } else {
                    bytes.push(5);
                    bytes.extend_from_slice(&(number + 0.0).to_be_bytes());
                }
            }
        }
        JsonValue::String(value) => {
            if value.contains('\0') {
                return Err(JsonbError("JSONB string contains a NUL"));
            }
            bytes.push(6);
            encode_string(bytes, value)?;
        }
        JsonValue::Array(values) => {
            bytes.push(7);
            bytes.extend_from_slice(
                &u32::try_from(values.len())
                    .map_err(|_| JsonbError("JSONB array is too large"))?
                    .to_be_bytes(),
            );
            for value in values {
                encode_node(bytes, value, depth + 1)?;
            }
        }
        JsonValue::Object(values) => {
            bytes.push(8);
            bytes.extend_from_slice(
                &u32::try_from(values.len())
                    .map_err(|_| JsonbError("JSONB object is too large"))?
                    .to_be_bytes(),
            );
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (key, value) in entries {
                if key.contains('\0') {
                    return Err(JsonbError("JSONB key contains a NUL"));
                }
                encode_string(bytes, key)?;
                encode_node(bytes, value, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn encode_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), JsonbError> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| JsonbError("JSONB string is too large"))?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn append_text_array_json(output: &mut Vec<u8>, values: &[String]) -> Result<(), JsonbError> {
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        append_json_string(output, value)?;
    }
    output.push(b']');
    Ok(())
}

fn append_value_json(
    output: &mut Vec<u8>,
    value: &JsonValue,
    depth: usize,
) -> Result<(), JsonbError> {
    if depth > MAX_DEPTH {
        return Err(JsonbError("JSONB value exceeds its nesting limit"));
    }
    match value {
        JsonValue::Null => output.extend_from_slice(b"null"),
        JsonValue::Bool(false) => output.extend_from_slice(b"false"),
        JsonValue::Bool(true) => output.extend_from_slice(b"true"),
        JsonValue::Number(number) => append_canonical_number(output, number)?,
        JsonValue::String(value) => append_json_string(output, value)?,
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                append_value_json(output, value, depth + 1)?;
            }
            output.push(b']');
        }
        JsonValue::Object(values) => {
            output.push(b'{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                append_json_string(output, key)?;
                output.push(b':');
                append_value_json(output, value, depth + 1)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn append_canonical_number(
    output: &mut Vec<u8>,
    number: &serde_json::Number,
) -> Result<(), JsonbError> {
    if let Some(number) = number.as_i64() {
        output.extend_from_slice(itoa::Buffer::new().format(number).as_bytes());
    } else if let Some(number) = number.as_u64() {
        output.extend_from_slice(itoa::Buffer::new().format(number).as_bytes());
    } else {
        let number = number
            .as_f64()
            .filter(|number| number.is_finite())
            .ok_or(JsonbError("JSONB number is not finite"))?;
        if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
            output.extend_from_slice(itoa::Buffer::new().format(number as i64).as_bytes());
        } else {
            output.extend_from_slice(ryu::Buffer::new().format(number + 0.0).as_bytes());
        }
    }
    Ok(())
}

fn append_json_string(output: &mut Vec<u8>, value: &str) -> Result<(), JsonbError> {
    if value.contains('\0') {
        return Err(JsonbError("JSONB string contains a NUL"));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    let mut plain_start = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        let escaped = match byte {
            b'"' => Some(&b"\\\""[..]),
            b'\\' => Some(&b"\\\\"[..]),
            0x08 => Some(&b"\\b"[..]),
            b'\t' => Some(&b"\\t"[..]),
            b'\n' => Some(&b"\\n"[..]),
            0x0c => Some(&b"\\f"[..]),
            b'\r' => Some(&b"\\r"[..]),
            0x00..=0x1f => {
                output.extend_from_slice(&value.as_bytes()[plain_start..index]);
                output.extend_from_slice(b"\\u00");
                output.push(HEX[(byte >> 4) as usize]);
                output.push(HEX[(byte & 0x0f) as usize]);
                plain_start = index + 1;
                None
            }
            _ => None,
        };
        if let Some(escaped) = escaped {
            output.extend_from_slice(&value.as_bytes()[plain_start..index]);
            output.extend_from_slice(escaped);
            plain_start = index + 1;
        }
    }
    output.extend_from_slice(&value.as_bytes()[plain_start..]);
    output.push(b'"');
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), JsonbError> {
        if value
            .as_bytes()
            .iter()
            .all(|byte| *byte >= 0x20 && *byte != b'"' && *byte != b'\\')
        {
            output.push(b'"');
            output.extend_from_slice(value.as_bytes());
            output.push(b'"');
            return Ok(());
        }
        serde_json::to_writer(output, value)
            .map_err(|_| JsonbError("JSONB string cannot be rendered"))
    }

    fn exact(&mut self, length: usize) -> Result<&'a [u8], JsonbError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(JsonbError("JSONB value is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<usize, JsonbError> {
        Ok(u32::from_be_bytes(self.exact(4)?.try_into().expect("four JSONB bytes")) as usize)
    }

    fn string(&mut self) -> Result<&'a str, JsonbError> {
        let length = self.u32()?;
        if length > MAX_BYTES {
            return Err(JsonbError("JSONB string is too large"));
        }
        let value = std::str::from_utf8(self.exact(length)?)
            .map_err(|_| JsonbError("JSONB string is not UTF-8"))?;
        if value.contains('\0') {
            return Err(JsonbError("JSONB string contains a NUL"));
        }
        Ok(value)
    }

    /// Reads a string from a JSONB envelope that has already passed the full
    /// recursive validator.
    unsafe fn validated_string(&mut self) -> Result<&'a str, JsonbError> {
        let length = self.u32()?;
        if length > MAX_BYTES {
            return Err(JsonbError("JSONB string is too large"));
        }
        let value = self.exact(length)?;
        // SAFETY: the caller's validation proof covered UTF-8 and interior-NUL
        // checks for this exact immutable byte range.
        Ok(unsafe { std::str::from_utf8_unchecked(value) })
    }

    fn count(&mut self, divisor: usize, kind: &'static str) -> Result<usize, JsonbError> {
        let count = self.u32()?;
        if count > self.bytes.len().saturating_sub(self.offset) / divisor {
            return Err(JsonbError(kind));
        }
        Ok(count)
    }

    fn validate_value(&mut self, depth: usize) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.exact(1)?[0] {
            0..=2 => {}
            3 => {
                self.exact(8)?;
            }
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value <= i64::MAX as u64 {
                    return Err(JsonbError("JSONB number is not canonical"));
                }
            }
            5 => validate_float(self.exact(8)?)?,
            6 => {
                self.string()?;
            }
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                for _ in 0..count {
                    self.validate_value(depth + 1)?;
                }
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                let mut previous = None;
                for _ in 0..count {
                    let key = self.string()?;
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(JsonbError("JSONB keys are not canonical"));
                    }
                    self.validate_value(depth + 1)?;
                    previous = Some(key);
                }
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        }
        Ok(())
    }

    fn write_json(&mut self, output: &mut Vec<u8>, depth: usize) -> Result<(), JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        match self.exact(1)?[0] {
            0 => output.extend_from_slice(b"null"),
            1 => output.extend_from_slice(b"false"),
            2 => output.extend_from_slice(b"true"),
            3 => {
                let value = i64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
            }
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                output.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
            }
            5 => {
                let bytes = self.exact(8)?;
                validate_float(bytes)?;
                let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                output.extend_from_slice(ryu::Buffer::new().format(number).as_bytes());
            }
            6 => Self::write_string(output, unsafe { self.validated_string()? })?,
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                output.push(b'[');
                for index in 0..count {
                    if index != 0 {
                        output.push(b',');
                    }
                    self.write_json(output, depth + 1)?;
                }
                output.push(b']');
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                output.push(b'{');
                for index in 0..count {
                    if index != 0 {
                        output.push(b',');
                    }
                    Self::write_string(output, unsafe { self.validated_string()? })?;
                    output.push(b':');
                    self.write_json(output, depth + 1)?;
                }
                output.push(b'}');
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        }
        Ok(())
    }

    fn decode_value(&mut self, depth: usize) -> Result<JsonValue, JsonbError> {
        if depth > MAX_DEPTH {
            return Err(JsonbError("JSONB value exceeds its nesting limit"));
        }
        Ok(match self.exact(1)?[0] {
            0 => JsonValue::Null,
            1 => JsonValue::Bool(false),
            2 => JsonValue::Bool(true),
            3 => JsonValue::from(i64::from_be_bytes(
                self.exact(8)?
                    .try_into()
                    .expect("eight JSONB integer bytes"),
            )),
            4 => {
                let value = u64::from_be_bytes(
                    self.exact(8)?
                        .try_into()
                        .expect("eight JSONB integer bytes"),
                );
                if value <= i64::MAX as u64 {
                    return Err(JsonbError("JSONB number is not canonical"));
                }
                JsonValue::from(value)
            }
            5 => {
                let bytes = self.exact(8)?;
                validate_float(bytes)?;
                let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
                JsonValue::Number(
                    serde_json::Number::from_f64(number)
                        .ok_or(JsonbError("JSONB number is invalid"))?,
                )
            }
            6 => JsonValue::String(self.string()?.to_owned()),
            7 => {
                let count = self.count(1, "JSONB array count is invalid")?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.decode_value(depth + 1)?);
                }
                JsonValue::Array(values)
            }
            8 => {
                let count = self.count(5, "JSONB object count is invalid")?;
                let mut values = serde_json::Map::new();
                let mut previous = None;
                for _ in 0..count {
                    let key = self.string()?;
                    if previous.is_some_and(|previous| previous >= key) {
                        return Err(JsonbError("JSONB keys are not canonical"));
                    }
                    values.insert(key.to_owned(), self.decode_value(depth + 1)?);
                    previous = Some(key);
                }
                JsonValue::Object(values)
            }
            _ => return Err(JsonbError("JSONB tag is invalid")),
        })
    }
}

fn validate_float(bytes: &[u8]) -> Result<(), JsonbError> {
    let number = f64::from_be_bytes(bytes.try_into().expect("eight JSONB float bytes"));
    if !number.is_finite()
        || (number == 0.0 && number.is_sign_negative())
        || (number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0)
    {
        return Err(JsonbError("JSONB number is not canonical"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_jsonb_validates_without_materializing_and_decodes_lazily() {
        let value = serde_json::json!({"a": [true, null, 1.5], "z": "text"});
        let bytes: Arc<[u8]> = encode_binary(&value).unwrap().into();
        let jsonb = Jsonb::from_binary(bytes).unwrap();
        assert!(jsonb.is_binary());
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        assert!(jsonb.estimated_binary_size() > 0);
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
        assert_eq!(jsonb.as_value(), &value);
    }

    #[test]
    fn binary_jsonb_renders_without_materializing_a_dom() {
        let value = serde_json::json!({"z": [true, null, "x"], "a": -7});
        let jsonb = Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap();

        assert_eq!(
            jsonb.to_json_string().unwrap(),
            r#"{"a":-7,"z":[true,null,"x"]}"#
        );
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));
    }

    #[test]
    fn borrowed_binary_jsonb_renders_nested_canonical_json() {
        let value = serde_json::json!({
            "array": [null, true, {"escaped": "line\n\"quoted\"", "number": 1.5}],
            "object": {"a": -7, "z": 18_446_744_073_709_551_615_u64}
        });
        let bytes = encode_binary(&value).unwrap();

        assert_eq!(
            binary_to_json_string(&bytes).unwrap(),
            r#"{"array":[null,true,{"escaped":"line\n\"quoted\"","number":1.5}],"object":{"a":-7,"z":18446744073709551615}}"#
        );
    }

    #[test]
    fn canonical_json_append_is_equivalent_across_representations() {
        let value: JsonValue = serde_json::from_str(
            r#"{"z":[1.0,"line\nquoted\"",null],"a":{"β":1.5,"control":"\u000b"}}"#,
        )
        .unwrap();
        let expected = r#"{"a":{"control":"\u000b","β":1.5},"z":[1,"line\nquoted\"",null]}"#;
        let values = [
            Jsonb::from_value(value.clone()),
            Jsonb::from_binary(encode_binary(&value).unwrap().into()).unwrap(),
        ];

        for jsonb in &values {
            let mut output = b"prefix:".to_vec();
            jsonb.append_canonical_json(&mut output).unwrap();
            assert_eq!(&output[b"prefix:".len()..], expected.as_bytes());
            assert_eq!(jsonb.to_json_string().unwrap(), expected);
        }
        assert!(matches!(
            &values[1].0,
            JsonbRepr::Binary(binary) if binary.value.get().is_none()
        ));

        let text_array =
            Jsonb::from_text_array(vec!["alpha".to_owned(), "line\nβ".to_owned()]).unwrap();
        let mut output = Vec::new();
        text_array.append_canonical_json(&mut output).unwrap();
        assert_eq!(output, r#"["alpha","line\nβ"]"#.as_bytes());
        assert!(matches!(
            &text_array.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
    }

    #[test]
    fn canonical_json_append_rolls_back_on_invalid_value() {
        let jsonb = Jsonb::from_value(JsonValue::String("bad\0value".to_owned()));
        let mut output = b"prefix".to_vec();
        assert_eq!(
            jsonb.append_canonical_json(&mut output),
            Err(JsonbError("JSONB string contains a NUL"))
        );
        assert_eq!(output, b"prefix");
    }

    #[test]
    fn canonical_json_text_validator_accepts_renderer_output() {
        let cases = [
            serde_json::json!(null),
            serde_json::json!(false),
            serde_json::json!(18_446_744_073_709_551_615_u64),
            serde_json::from_str::<JsonValue>("1.0").unwrap(),
            serde_json::json!(["quote\"", "slash\\", "line\n", "\u{000b}", "β"]),
            serde_json::json!({"a": {"nested": true}, "z": [1.5, -7]}),
        ];

        for value in cases {
            let jsonb = Jsonb::from_value(value);
            let mut encoded = Vec::new();
            jsonb.append_canonical_json(&mut encoded).unwrap();
            assert_eq!(
                validate_canonical_json_text(&encoded).unwrap().as_bytes(),
                encoded
            );
        }

        assert!(validate_canonical_json_text(br#"{"\n":0,"a":1}"#).is_ok());
    }

    #[test]
    fn canonical_json_text_validator_rejects_noncanonical_text() {
        let cases: &[&[u8]] = &[
            b" null",
            b"null ",
            br#"{"b":1,"a":2}"#,
            br#"{"a":1,"a":2}"#,
            br#"{"a":1,}"#,
            br#"[1,]"#,
            br#""\/""#,
            br#""\u0061""#,
            br#""\u000a""#,
            br#""\u000B""#,
            br#""\u0000""#,
            b"1.0",
            b"-0",
            b"1e0",
            b"01",
            b"truefalse",
            &[b'"', 0xff, b'"'],
        ];
        for encoded in cases {
            assert!(
                validate_canonical_json_text(encoded).is_err(),
                "accepted noncanonical JSON: {:?}",
                String::from_utf8_lossy(encoded)
            );
        }

        assert!(validate_canonical_json_text(br#""\u000b""#).is_ok());
        assert!(validate_canonical_json_text(br#""quote\"slash\\line\n""#).is_ok());
    }

    #[test]
    fn canonical_json_text_integer_fast_path_matches_number_boundaries() {
        for encoded in [
            "0",
            "1",
            "-1",
            "-9223372036854775808",
            "9223372036854775807",
            "18446744073709551615",
            "9007199254740992",
            "9007199254740993",
        ] {
            assert!(
                validate_canonical_json_text(encoded.as_bytes()).is_ok(),
                "rejected canonical integer {encoded}"
            );
        }
        for encoded in [
            "-0",
            "+0",
            "00",
            "01",
            "-01",
            "-",
            "18446744073709551616",
            "-9223372036854775809",
        ] {
            assert!(
                validate_canonical_json_text(encoded.as_bytes()).is_err(),
                "accepted noncanonical integer {encoded}"
            );
        }
    }

    #[test]
    fn canonical_json_text_validator_enforces_depth_limit() {
        let accepted = format!("{}null{}", "[".repeat(MAX_DEPTH), "]".repeat(MAX_DEPTH));
        assert!(validate_canonical_json_text(accepted.as_bytes()).is_ok());

        let rejected = format!(
            "{}null{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert_eq!(
            validate_canonical_json_text(rejected.as_bytes()),
            Err(JsonbError("JSONB value exceeds its nesting limit"))
        );
    }

    #[test]
    fn borrowed_binary_jsonb_rejects_corruption_and_noncanonical_input() {
        let canonical = encode_binary(&serde_json::json!({"a": 1})).unwrap();

        let mut truncated = canonical.clone();
        truncated.pop();
        assert_eq!(
            binary_to_json_string(&truncated),
            Err(JsonbError("JSONB value is truncated"))
        );

        let mut trailing = canonical;
        trailing.push(0);
        assert_eq!(
            binary_to_json_string(&trailing),
            Err(JsonbError("JSONB value has trailing bytes"))
        );

        let mut unordered = vec![8];
        unordered.extend_from_slice(&2_u32.to_be_bytes());
        encode_string(&mut unordered, "z").unwrap();
        unordered.push(0);
        encode_string(&mut unordered, "a").unwrap();
        unordered.push(0);
        assert_eq!(
            binary_to_json_string(&unordered),
            Err(JsonbError("JSONB keys are not canonical"))
        );

        let mut noncanonical_integer = vec![4];
        noncanonical_integer.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            binary_to_json_string(&noncanonical_integer),
            Err(JsonbError("JSONB number is not canonical"))
        );
    }

    #[test]
    fn text_array_builds_canonical_binary_without_materializing_a_dom() {
        let jsonb = Jsonb::from_text_array(vec!["alpha".to_owned(), "βeta".to_owned()]).unwrap();
        assert!(jsonb.is_binary());
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
        validate_binary(&jsonb.binary().unwrap()).unwrap();
        assert!(matches!(
            &jsonb.0,
            JsonbRepr::TextArray(array) if array.value.get().is_none()
        ));
        assert_eq!(jsonb.as_value(), &serde_json::json!(["alpha", "βeta"]));
    }

    #[test]
    fn canonical_text_render_and_equality_do_not_materialize_binary_or_dom() {
        let left = Jsonb::from_canonical_text_vec(br#"{"a":1}"#.to_vec()).unwrap();
        let right = Jsonb::from_canonical_text_vec(br#"{"a":1}"#.to_vec()).unwrap();

        assert_eq!(left.to_json_string().unwrap(), r#"{"a":1}"#);
        assert_eq!(left, right);
        for value in [&left, &right] {
            assert!(matches!(
                &value.0,
                JsonbRepr::CanonicalText(text)
                    if text.value.get().is_none() && text.binary.get().is_none()
            ));
        }
    }

    #[test]
    fn binary_jsonb_can_render_text_larger_than_the_binary_limit() {
        let escaped = "\u{0001}".repeat(2_800_000);
        let binary = encode_binary(&JsonValue::String(escaped)).unwrap();
        assert!(binary.len() < MAX_BYTES);
        let jsonb = Jsonb::from_binary(binary.into()).unwrap();

        let rendered = jsonb.to_json_string().unwrap();
        assert!(rendered.len() > MAX_BYTES);
        assert!(rendered.starts_with("\"\\u0001\\u0001"));
        assert!(rendered.ends_with("\\u0001\""));
    }

    #[test]
    fn text_array_rejects_non_json_strings() {
        assert_eq!(
            Jsonb::from_text_array(vec!["bad\0value".to_owned()]).unwrap_err(),
            JsonbError("JSONB string contains a NUL")
        );
    }

    #[test]
    fn binary_jsonb_rejects_noncanonical_object_order() {
        let mut bytes = vec![8];
        bytes.extend_from_slice(&2_u32.to_be_bytes());
        encode_string(&mut bytes, "z").unwrap();
        bytes.push(0);
        encode_string(&mut bytes, "a").unwrap();
        bytes.push(0);
        assert_eq!(
            validate_binary(&bytes),
            Err(JsonbError("JSONB keys are not canonical"))
        );
    }

    #[test]
    fn binary_jsonb_rejects_small_integer_with_unsigned_tag() {
        let mut bytes = vec![4];
        bytes.extend_from_slice(&1_u64.to_be_bytes());
        assert_eq!(
            validate_binary(&bytes),
            Err(JsonbError("JSONB number is not canonical"))
        );
        assert_eq!(
            decode_binary(&bytes),
            Err(JsonbError("JSONB number is not canonical"))
        );
    }

    #[test]
    fn equality_is_canonical_across_the_full_representation_matrix() {
        let integer: JsonValue = serde_json::from_str("1").unwrap();
        let decimal: JsonValue = serde_json::from_str("1.0").unwrap();
        assert_ne!(
            integer, decimal,
            "the test inputs must retain distinct spellings"
        );

        let values = [
            Jsonb::from_value(integer.clone()),
            Jsonb::from_value(decimal.clone()),
            Jsonb::from_binary(encode_binary(&integer).unwrap().into()).unwrap(),
            Jsonb::from_binary(encode_binary(&decimal).unwrap().into()).unwrap(),
        ];

        for (left_index, left) in values.iter().enumerate() {
            for (right_index, right) in values.iter().enumerate() {
                assert_eq!(
                    left, right,
                    "representation matrix entry ({left_index}, {right_index})"
                );
            }
        }

        for left in &values {
            for middle in &values {
                for right in &values {
                    assert!(left == middle && middle == right && left == right);
                }
            }
        }

        for value in &values {
            assert_eq!(value, &integer);
            assert_eq!(value, &decimal);
            assert_eq!(&integer, value);
            assert_eq!(&decimal, value);
        }

        for binary in &values[2..] {
            assert!(matches!(
                &binary.0,
                JsonbRepr::Binary(binary) if binary.value.get().is_none()
            ));
        }
    }
}
