use std::ops::Deref;

pub(crate) const ENGINE_STORAGE_SCOPE_KEY: &str = "engine";
pub(crate) const STORAGE_SCOPE_KEY_COLUMN: &str = "storage_scope_key";

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Json(serde_json::Value),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NullableKeyFilter<T> {
    Any,
    Null,
    Value(T),
}

impl<T> Default for NullableKeyFilter<T> {
    fn default() -> Self {
        Self::Any
    }
}

impl<T> NullableKeyFilter<T> {
    pub fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    pub fn as_value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Any | Self::Null => None,
        }
    }

    pub fn as_ref(&self) -> NullableKeyFilter<&T> {
        match self {
            Self::Any => NullableKeyFilter::Any,
            Self::Null => NullableKeyFilter::Null,
            Self::Value(value) => NullableKeyFilter::Value(value),
        }
    }

    pub fn from_nullable(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Value(value),
            None => Self::Null,
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
            Self::Value(value) => NullableKeyFilter::Value(value.deref()),
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

pub(crate) fn storage_scope_key_for_file_id(file_id: Option<&str>) -> String {
    match file_id {
        Some(file_id) => format!("file:{file_id}"),
        None => ENGINE_STORAGE_SCOPE_KEY.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Vec<Value>>,
    #[serde(default)]
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct WriteReceipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_commit_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_commit: Option<crate::canonical::CanonicalCommitReceipt>,
}

impl WriteReceipt {
    pub fn is_empty(&self) -> bool {
        self.state_commit_sequence.is_none() && self.canonical_commit.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ExecuteResult {
    #[serde(default)]
    pub statements: Vec<QueryResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_receipt: Option<WriteReceipt>,
}
