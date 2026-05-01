use std::ops::Deref;

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
}

impl WriteReceipt {
    pub fn is_empty(&self) -> bool {
        self.state_commit_sequence.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct ExecuteResult {
    #[serde(default)]
    pub statements: Vec<QueryResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_receipt: Option<WriteReceipt>,
}
