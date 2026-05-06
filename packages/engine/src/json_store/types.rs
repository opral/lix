use std::sync::Arc;

use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedJson(Arc<str>);

impl NormalizedJson {
    pub(crate) fn from_arc_unchecked(normalized: Arc<str>) -> Self {
        Self(normalized)
    }

    pub(crate) fn from_value(value: &serde_json::Value, context: &str) -> Result<Self, LixError> {
        let normalized: Arc<str> = serde_json::to_string(value)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_UNKNOWN,
                    format!("{context} failed to serialize as normalized JSON: {error}"),
                )
            })?
            .into();
        Ok(Self(normalized))
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct JsonRef {
    hash: [u8; 32],
}

impl JsonRef {
    pub(crate) fn from_hash(hash: blake3::Hash) -> Self {
        Self {
            hash: *hash.as_bytes(),
        }
    }

    pub(crate) fn from_hash_bytes(hash: [u8; 32]) -> Self {
        Self { hash }
    }

    pub(crate) fn as_hash_bytes(&self) -> &[u8] {
        &self.hash
    }

    pub(crate) fn to_hex(&self) -> String {
        self.hash.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JsonProjectionPath(String);

impl JsonProjectionPath {
    pub(crate) fn new(pointer: impl Into<String>) -> Self {
        Self(pointer.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonProjection {
    values: Vec<Option<serde_json::Value>>,
}

impl JsonProjection {
    pub(crate) fn new(values: Vec<Option<serde_json::Value>>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[Option<serde_json::Value>] {
        &self.values
    }
}
