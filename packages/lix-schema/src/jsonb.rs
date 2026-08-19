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

    pub fn as_value(&self) -> &JsonValue {
        match &self.0 {
            JsonbRepr::Value(value) => value,
            JsonbRepr::Binary(binary) => binary
                .value
                .get_or_init(|| decode_binary(&binary.bytes).expect("validated JSONB decodes")),
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
            JsonbRepr::Binary(_) | JsonbRepr::TextArray(_) => {
                unreachable!("native JSONB was materialized above")
            }
        }
    }

    pub fn binary(&self) -> Result<Cow<'_, [u8]>, JsonbError> {
        match &self.0 {
            JsonbRepr::Value(value) => encode_binary(value).map(Cow::Owned),
            JsonbRepr::Binary(binary) => Ok(Cow::Borrowed(&binary.bytes)),
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
            JsonbRepr::TextArray(array) => append_text_array_binary(output, &array.values),
        };
        if result.is_err() || output.len().saturating_sub(start) > MAX_BYTES {
            output.truncate(start);
            return result.and(Err(JsonbError("JSONB value is too large")));
        }
        result
    }

    pub fn is_valid(&self) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_value_valid(value),
            JsonbRepr::Binary(_) => true,
            JsonbRepr::TextArray(_) => true,
        }
    }

    pub fn is_binary(&self) -> bool {
        matches!(self.0, JsonbRepr::Binary(_) | JsonbRepr::TextArray(_))
    }

    pub fn estimated_binary_size(&self) -> u64 {
        match &self.0 {
            JsonbRepr::Binary(binary) => binary.bytes.len() as u64,
            JsonbRepr::Value(value) => estimated_value_size(value),
            JsonbRepr::TextArray(array) => {
                u64::try_from(text_array_binary_size(&array.values).unwrap_or(usize::MAX))
                    .unwrap_or(u64::MAX)
            }
        }
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
            _ => match (self.binary(), other.binary()) {
                (Ok(left), Ok(right)) => left == right,
                _ => false,
            },
        }
    }
}

impl PartialEq<JsonValue> for Jsonb {
    fn eq(&self, other: &JsonValue) -> bool {
        match &self.0 {
            JsonbRepr::Value(value) => json_values_eq(value, other),
            JsonbRepr::Binary(binary) => encode_binary(other)
                .is_ok_and(|encoded| encoded.as_slice() == binary.bytes.as_ref()),
            JsonbRepr::TextArray(_) => self
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

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
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
