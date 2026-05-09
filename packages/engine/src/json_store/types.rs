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

    pub(crate) fn for_content(bytes: &[u8]) -> Self {
        Self::from_hash(blake3::hash(bytes))
    }

    pub(crate) fn as_hash_bytes(&self) -> &[u8] {
        &self.hash
    }

    pub(crate) fn to_hex(&self) -> String {
        self.hash.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NormalizedJsonRef<'a> {
    pub(crate) normalized: &'a str,
}

impl<'a> From<&'a NormalizedJson> for NormalizedJsonRef<'a> {
    fn from(value: &'a NormalizedJson) -> Self {
        Self {
            normalized: value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonWritePlacementRef<'a> {
    CommitPack { commit_id: &'a str, pack_id: u32 },
    Direct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonReadScopeRef<'a> {
    Direct,
    CommitPacks {
        commit_id: &'a str,
        pack_ids: &'a [u32],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JsonLoadRequestRef<'a> {
    pub(crate) refs: &'a [JsonRef],
    pub(crate) scope: JsonReadScopeRef<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JsonProjectionLoadRequestRef<'a> {
    pub(crate) refs: &'a [JsonRef],
    pub(crate) scope: JsonReadScopeRef<'a>,
    pub(crate) paths: &'a [JsonProjectionPath],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JsonLoadBatch {
    values: Vec<Option<Vec<u8>>>,
}

impl JsonLoadBatch {
    pub(crate) fn new(values: Vec<Option<Vec<u8>>>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[Option<Vec<u8>>] {
        &self.values
    }

    pub(crate) fn into_values(self) -> Vec<Option<Vec<u8>>> {
        self.values
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonValueBatch {
    values: Vec<Option<serde_json::Value>>,
}

impl JsonValueBatch {
    pub(crate) fn new(values: Vec<Option<serde_json::Value>>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[Option<serde_json::Value>] {
        &self.values
    }

    pub(crate) fn into_values(self) -> Vec<Option<serde_json::Value>> {
        self.values
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JsonProjectionBatch {
    values: Vec<Option<JsonProjection>>,
}

impl JsonProjectionBatch {
    pub(crate) fn new(values: Vec<Option<JsonProjection>>) -> Self {
        Self { values }
    }

    pub(crate) fn values(&self) -> &[Option<JsonProjection>] {
        &self.values
    }

    pub(crate) fn into_values(self) -> Vec<Option<JsonProjection>> {
        self.values
    }
}
