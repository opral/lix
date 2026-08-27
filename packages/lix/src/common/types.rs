use std::borrow::Borrow;
use std::fmt;
use std::ops::{Deref, Range};

/// Immutable, cheaply cloned binary SQL value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Blob(bytes::Bytes);

impl Blob {
    pub fn from_static(bytes: &'static [u8]) -> Self {
        Self(bytes::Bytes::from_static(bytes))
    }

    pub fn into_bytes(self) -> bytes::Bytes {
        self.0
    }

    pub fn as_bytes(&self) -> &bytes::Bytes {
        &self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes.into())
    }
}

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes::Bytes::copy_from_slice(bytes))
    }
}

impl From<bytes::Bytes> for Blob {
    fn from(bytes: bytes::Bytes) -> Self {
        Self(bytes)
    }
}

impl From<Blob> for bytes::Bytes {
    fn from(blob: Blob) -> Self {
        blob.0
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl PartialEq<[u8]> for Blob {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl Deref for Blob {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Immutable UTF-8 backed by a cheaply cloned byte buffer.
///
/// A `SharedStr` retains its source buffer plus a validated byte range. This
/// lets decoders hand out many string views over one arena without allocating
/// one `String` per row. Every public constructor validates UTF-8 before the
/// value can be observed as `str`.
#[derive(Clone)]
pub struct SharedStr {
    bytes: bytes::Bytes,
    range: Range<usize>,
}

impl SharedStr {
    /// Retains a static UTF-8 string without allocating.
    ///
    /// Repeated construction from the same static string produces views over
    /// the same immutable backing buffer, which is useful for batch-wide
    /// engine metadata such as write-surface identifiers.
    pub fn from_static(value: &'static str) -> Self {
        let bytes = bytes::Bytes::from_static(value.as_bytes());
        let len = bytes.len();
        Self {
            bytes,
            range: 0..len,
        }
    }

    /// Retains `bytes` without copying after validating the complete buffer.
    pub fn from_utf8(bytes: bytes::Bytes) -> Result<Self, std::str::Utf8Error> {
        std::str::from_utf8(&bytes)?;
        let len = bytes.len();
        Ok(Self {
            bytes,
            range: 0..len,
        })
    }

    /// Retains bytes whose producer already proved complete-buffer UTF-8.
    ///
    /// # Safety
    ///
    /// `bytes` must contain valid UTF-8. Callers should prefer [`Self::from_utf8`]
    /// unless validity follows directly from construction from `str` slices or
    /// a UTF-8 serializer.
    pub(crate) unsafe fn from_utf8_unchecked(bytes: bytes::Bytes) -> Self {
        debug_assert!(
            std::str::from_utf8(&bytes).is_ok(),
            "SharedStr trusted producer emitted invalid UTF-8"
        );
        let len = bytes.len();
        Self {
            bytes,
            range: 0..len,
        }
    }

    /// Retains one UTF-8 range of an otherwise arbitrary shared buffer.
    #[cfg(test)]
    pub(crate) fn from_utf8_range(bytes: bytes::Bytes, range: Range<usize>) -> Option<Self> {
        let slice = bytes.get(range.clone())?;
        std::str::from_utf8(slice).ok()?;
        Some(Self { bytes, range })
    }

    /// Retains `value` as a zero-copy view when it points inside `bytes`.
    ///
    /// `value` is already valid UTF-8 by type. Pointer containment is checked
    /// before constructing the range, so callers cannot forge an invalid view.
    pub fn from_utf8_slice(bytes: bytes::Bytes, value: &str) -> Option<Self> {
        if value.is_empty() {
            return Some(Self::default());
        }
        let bytes_start = bytes.as_ptr() as usize;
        let bytes_end = bytes_start.checked_add(bytes.len())?;
        let value_start = value.as_ptr() as usize;
        let value_end = value_start.checked_add(value.len())?;
        if value_start < bytes_start || value_end > bytes_end {
            return None;
        }
        Some(Self {
            range: (value_start - bytes_start)..(value_end - bytes_start),
            bytes,
        })
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: all constructors validate exactly `range`; slicing a
        // `SharedStr` below also checks UTF-8 character boundaries via `str`.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[self.range.clone()]) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[self.range.clone()]
    }

    /// Creates another zero-copy view over this value.
    pub fn slice(&self, range: Range<usize>) -> Option<Self> {
        self.as_str().get(range.clone())?;
        let start = self.range.start.checked_add(range.start)?;
        let end = self.range.start.checked_add(range.end)?;
        Some(Self {
            bytes: self.bytes.clone(),
            range: start..end,
        })
    }

    /// Returns the selected bytes without copying.
    pub fn into_bytes(self) -> bytes::Bytes {
        self.bytes.slice(self.range)
    }

    /// Whether two views retain the same complete source buffer.
    ///
    /// Intended for structural assertions and batch diagnostics; it makes no
    /// claim that the selected ranges overlap.
    pub fn shares_buffer_with(&self, other: &Self) -> bool {
        self.bytes.as_ptr() == other.bytes.as_ptr() && self.bytes.len() == other.bytes.len()
    }

    pub(crate) fn retained_buffer_len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn retained_buffer_identity(&self) -> (*const u8, usize) {
        (self.bytes.as_ptr(), self.bytes.len())
    }
}

impl Default for SharedStr {
    fn default() -> Self {
        Self {
            bytes: bytes::Bytes::new(),
            range: 0..0,
        }
    }
}

impl From<String> for SharedStr {
    fn from(value: String) -> Self {
        let bytes = bytes::Bytes::from(value.into_bytes());
        let len = bytes.len();
        Self {
            bytes,
            range: 0..len,
        }
    }
}

impl From<&str> for SharedStr {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl From<Box<str>> for SharedStr {
    fn from(value: Box<str>) -> Self {
        Self::from(value.into_string())
    }
}

impl From<SharedStr> for bytes::Bytes {
    fn from(value: SharedStr) -> Self {
        value.into_bytes()
    }
}

impl From<SharedStr> for String {
    fn from(value: SharedStr) -> Self {
        value.as_str().to_owned()
    }
}

impl Deref for SharedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for SharedStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for SharedStr {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Borrow<str> for SharedStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SharedStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

impl fmt::Display for SharedStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq for SharedStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for SharedStr {}

impl PartialOrd for SharedStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SharedStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for SharedStr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl PartialEq<str> for SharedStr {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SharedStr {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SharedStr {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<SharedStr> for String {
    fn eq(&self, other: &SharedStr) -> bool {
        self == other.as_str()
    }
}

impl serde::Serialize for SharedStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for SharedStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        <String as serde::Deserialize>::deserialize(deserializer).map(Self::from)
    }
}

/// Canonical JSON text carried by a SQL value.
///
/// Every JSON payload the engine accepts is normalized to serde_json's stable
/// compact form at the write boundary, and the row projection decoder hands
/// those exact bytes to Arrow as UTF-8. A JSON result column is therefore
/// already the byte string a caller receives, so `Json` retains the bytes
/// instead of rebuilding a `serde_json::Value` DOM for every row on every scan.
#[derive(Clone)]
pub struct Json(SharedStr);

impl Json {
    /// Retains canonical JSON text without re-validating it.
    ///
    /// Only engine-produced text may take this constructor. Public input is
    /// canonicalized before it reaches storage, so re-parsing on read would
    /// only re-prove what the write boundary already proved.
    pub fn from_canonical_text(text: impl Into<SharedStr>) -> Self {
        Self(text.into())
    }

    /// Parses arbitrary JSON text into canonical form.
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str::<serde_json::Value>(text).map(Self::from)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn as_shared_str(&self) -> &SharedStr {
        &self.0
    }

    pub fn into_shared_str(self) -> SharedStr {
        self.0
    }

    pub fn is_null(&self) -> bool {
        self.0.as_str() == "null"
    }

    /// Returns the decoded string when this payload is a JSON string.
    pub fn as_json_string(&self) -> Option<String> {
        match self.to_value() {
            serde_json::Value::String(value) => Some(value),
            _ => None,
        }
    }

    /// Materializes the DOM for callers that must inspect JSON structure.
    ///
    /// This is the opt-in cost. Row plumbing, the wire encoder, and the JS
    /// bridges all stay on the text representation and never call it.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::from_str(self.0.as_str())
            .expect("canonical JSON text retained by Json is valid JSON")
    }
}

impl From<serde_json::Value> for Json {
    fn from(value: serde_json::Value) -> Self {
        Self(SharedStr::from(value.to_string()))
    }
}

impl From<&serde_json::Value> for Json {
    fn from(value: &serde_json::Value) -> Self {
        Self(SharedStr::from(value.to_string()))
    }
}

impl From<Json> for serde_json::Value {
    fn from(value: Json) -> Self {
        value.to_value()
    }
}

impl PartialEq for Json {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Json {}

impl PartialEq<serde_json::Value> for Json {
    fn eq(&self, other: &serde_json::Value) -> bool {
        &self.to_value() == other
    }
}

impl fmt::Debug for Json {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Json({})", self.0.as_str())
    }
}

impl fmt::Display for Json {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.as_str())
    }
}

impl serde::Serialize for Json {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // `RawValue` emits the retained bytes verbatim, so the wire encoding is
        // identical to what re-serializing the DOM produced.
        serde_json::value::RawValue::from_string(self.0.as_str().to_owned())
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde_json::Value::deserialize(deserializer).map(Self::from)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Jsonb(Json),
    /// PostgreSQL `timestamptz`, represented losslessly as signed UTC
    /// microseconds since the Unix epoch.
    Timestamptz(i64),
    Blob(Blob),
}

/// Stable SQL result type exposed at language and protocol boundaries.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ResultColumnType {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Jsonb,
    Timestamptz,
    Blob,
}

impl ResultColumnType {
    pub(crate) fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Integer(_) => Self::Integer,
            Value::Real(_) => Self::Real,
            Value::Text(_) => Self::Text,
            Value::Jsonb(_) => Self::Jsonb,
            Value::Timestamptz(_) => Self::Timestamptz,
            Value::Blob(_) => Self::Blob,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum NullableKeyFilter<T> {
    #[default]
    Any,
    Null,
    Value(T),
}

impl<T> NullableKeyFilter<T> {
    pub fn as_ref(&self) -> NullableKeyFilter<&T> {
        match self {
            Self::Any => NullableKeyFilter::Any,
            Self::Null => NullableKeyFilter::Null,
            Self::Value(value) => NullableKeyFilter::Value(value),
        }
    }
}

impl<T> NullableKeyFilter<T>
where
    T: Deref,
{
    pub fn as_deref(&self) -> NullableKeyFilter<&T::Target> {
        match self {
            Self::Any => NullableKeyFilter::Any,
            Self::Null => NullableKeyFilter::Null,
            Self::Value(value) => NullableKeyFilter::Value(&**value),
        }
    }
}

impl<T: PartialEq> NullableKeyFilter<T> {
    pub fn matches(&self, candidate: Option<&T>) -> bool {
        match self {
            Self::Any => true,
            Self::Null => candidate.is_none(),
            Self::Value(expected) => candidate == Some(expected),
        }
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SqlQueryResult {
    pub rows: Vec<Vec<Value>>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub column_types: Vec<ResultColumnType>,
    #[serde(default)]
    pub notices: Vec<LixNotice>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LixNotice {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{Json, SharedStr, Value};
    use bytes::Bytes;

    #[test]
    fn json_values_serialize_their_retained_bytes_verbatim() {
        let canonical = r#"{"a":1,"b":[true,null],"c":"x"}"#;
        let value = Value::Jsonb(Json::from_canonical_text(canonical));

        let encoded = serde_json::to_string(&value).expect("value serializes");
        assert_eq!(encoded, format!(r#"{{"Jsonb":{canonical}}}"#));
    }

    #[test]
    fn json_text_matches_what_re_serializing_the_dom_produced() {
        // The read path stops building a DOM, so the retained bytes must equal
        // the bytes the previous `serde_json::Value` round trip emitted.
        let canonical = r#"{"a":1,"b":[true,null],"c":"x"}"#;
        let dom = serde_json::from_str::<serde_json::Value>(canonical).expect("valid JSON");

        assert_eq!(Json::from_canonical_text(canonical).as_str(), canonical);
        assert_eq!(Json::from(dom).as_str(), canonical);
    }

    #[test]
    fn json_deserialization_canonicalizes_noncanonical_input() {
        let decoded = serde_json::from_str::<Json>(r#"{ "b" : 2 , "a" : 1 }"#).expect("decodes");
        assert_eq!(decoded.as_str(), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn cloning_blob_values_shares_the_payload() {
        let value = Value::Blob(vec![7; 1024 * 1024].into());
        let cloned = value.clone();
        let Value::Blob(original) = value else {
            unreachable!("constructed a blob value");
        };
        let Value::Blob(cloned) = cloned else {
            unreachable!("cloned a blob value");
        };
        assert_eq!(original.as_ptr(), cloned.as_ptr());
    }

    #[test]
    fn shared_str_views_retain_one_utf8_buffer() {
        let arena = Bytes::from_static(b"alpha|beta");
        let alpha = SharedStr::from_utf8_range(arena.clone(), 0..5).expect("valid alpha");
        let beta = SharedStr::from_utf8_range(arena, 6..10).expect("valid beta");

        assert_eq!(alpha, "alpha");
        assert_eq!(beta, "beta");
        assert!(alpha.shares_buffer_with(&beta));
        assert_eq!(
            alpha.clone().into_bytes().as_ptr(),
            alpha.as_bytes().as_ptr()
        );
    }

    #[test]
    fn shared_str_static_views_reuse_the_static_buffer() {
        let first = SharedStr::from_static("plugin_reconciliation");
        let second = SharedStr::from_static("plugin_reconciliation");

        assert_eq!(first, "plugin_reconciliation");
        assert!(first.shares_buffer_with(&second));
        assert_eq!(first.as_bytes().as_ptr(), second.as_bytes().as_ptr());
    }

    #[test]
    fn shared_str_rejects_invalid_utf8() {
        assert!(SharedStr::from_utf8(Bytes::from_static(b"\xff")).is_err());
    }
}
