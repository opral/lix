use std::sync::Arc;

use crate::LixError;
use musli::{Allocator, Decode, Decoder, Encode, Encoder};

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

    pub(crate) fn as_hash_array(&self) -> &[u8; 32] {
        &self.hash
    }

    pub(crate) fn to_hex(&self) -> String {
        self.hash.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl<M> Encode<M> for JsonRef {
    type Encode = [u8; 32];

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        encoder.encode(&self.hash)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.hash.len())
    }

    fn as_encode(&self) -> &Self::Encode {
        &self.hash
    }
}

impl<'de, M, A> Decode<'de, M, A> for JsonRef
where
    A: Allocator,
{
    fn decode<D>(decoder: D) -> Result<Self, D::Error>
    where
        D: Decoder<'de, Mode = M, Allocator = A>,
    {
        Ok(Self::from_hash_bytes(<[u8; 32]>::decode(decoder)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NormalizedJsonRef<'a> {
    normalized: &'a str,
    trusted_json_ref: Option<JsonRef>,
}

impl<'a> NormalizedJsonRef<'a> {
    pub(crate) fn new(normalized: &'a str) -> Self {
        Self {
            normalized,
            trusted_json_ref: None,
        }
    }

    /// Uses a caller-owned invariant that `json_ref` was computed from
    /// `normalized`. This avoids rehashing JSON already normalized by the
    /// transaction staging boundary.
    pub(crate) fn trusted_prehashed(normalized: &'a str, json_ref: JsonRef) -> Self {
        Self {
            normalized,
            trusted_json_ref: Some(json_ref),
        }
    }

    pub(crate) fn normalized(&self) -> &'a str {
        self.normalized
    }

    pub(crate) fn trusted_json_ref(&self) -> Option<JsonRef> {
        self.trusted_json_ref
    }
}

impl<'a> From<&'a NormalizedJson> for NormalizedJsonRef<'a> {
    fn from(value: &'a NormalizedJson) -> Self {
        Self::new(value.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonWritePlacementRef {
    OutOfBand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonReadScopeRef {
    OutOfBand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JsonLoadRequestRef<'a> {
    pub(crate) refs: &'a [JsonRef],
    pub(crate) scope: JsonReadScopeRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JsonProjectionLoadRequestRef<'a> {
    pub(crate) refs: &'a [JsonRef],
    pub(crate) scope: JsonReadScopeRef,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_codec_roundtrips_json_ref() {
        let json_ref = JsonRef::from_hash_bytes([7; 32]);
        let bytes =
            crate::storage_codec::encode("json ref", &json_ref).expect("json ref should encode");

        let decoded: JsonRef =
            crate::storage_codec::decode("json ref", &bytes).expect("json ref should decode");

        assert_eq!(decoded, json_ref);
    }

    #[test]
    fn storage_codec_rejects_wrong_hash_length() {
        let short_hash: &[u8] = &[0; 31];
        let bytes = crate::storage_codec::encode("json ref hash", short_hash)
            .expect("short hash should encode");

        let error = crate::storage_codec::decode::<JsonRef>("json ref", &bytes)
            .expect_err("short hash should reject");

        assert!(error.message.contains("failed to decode json ref"));
    }
}
