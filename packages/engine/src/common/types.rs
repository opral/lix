use std::ops::Deref;

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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Json(serde_json::Value),
    Blob(Blob),
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
    use super::Value;

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
}
