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
