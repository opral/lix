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

    pub(crate) fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let mut out = String::with_capacity(self.hash.len() * 2);
        for byte in self.hash {
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        out
    }
}

impl<M> Encode<M> for JsonRef {
    type Encode = [u8; 32];

    fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
    where
        E: Encoder<Mode = M>,
    {
        encoder.encode(self.hash)
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

/// A snapshot or metadata payload slot in tracked-state values and change
/// records.
///
/// Small payloads inline their JSON text directly: measurement on the 10k
/// merge workload showed 99.8% of payloads at 65-128 bytes with ~0.2%
/// content dedup, so the json_store indirection (two 32-byte refs, a store
/// key, a store row, and a point read per materialization) cost more than
/// the payloads themselves. Large payloads keep the content-addressed ref.
/// The threshold is applied deterministically at staging time, so identical
/// content always produces the same variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JsonSlot {
    None,
    Ref(JsonRef),
    Inline(Box<str>),
}

/// Inline threshold in bytes. Payloads at or under this length skip the
/// json_store entirely.
pub(crate) const JSON_INLINE_MAX_BYTES: usize = 256;

impl JsonSlot {
    pub(crate) fn from_json(json: &str) -> Self {
        if json.len() <= JSON_INLINE_MAX_BYTES {
            Self::Inline(json.into())
        } else {
            Self::Ref(JsonRef::for_content(json.as_bytes()))
        }
    }

    pub(crate) fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    pub(crate) fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub(crate) fn as_ref_slot(&self) -> JsonSlotRef<'_> {
        match self {
            Self::None => JsonSlotRef::None,
            Self::Ref(json_ref) => JsonSlotRef::Ref(json_ref),
            Self::Inline(json) => JsonSlotRef::Inline(json),
        }
    }
}

/// Borrowed form of [`JsonSlot`] for zero-copy staging paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonSlotRef<'a> {
    None,
    Ref(&'a JsonRef),
    Inline(&'a str),
}

impl JsonSlotRef<'_> {
    pub(crate) fn to_owned_slot(self) -> JsonSlot {
        match self {
            Self::None => JsonSlot::None,
            Self::Ref(json_ref) => JsonSlot::Ref(*json_ref),
            Self::Inline(json) => JsonSlot::Inline(json.into()),
        }
    }
}

/// Musli codec for [`JsonSlot`]: tag byte 0/1/2, then the payload.
pub(crate) mod json_slot_storage {
    use musli::Context;
    use musli::de::SequenceDecoder;

    use super::{JsonRef, JsonSlot};

    pub(crate) fn decode<'de, D>(decoder: D) -> Result<JsonSlot, D::Error>
    where
        D: musli::Decoder<'de>,
    {
        let cx = decoder.cx();
        decoder.decode_pack(|pack| {
            let tag: u8 = pack.next()?;
            match tag {
                0 => Ok(JsonSlot::None),
                1 => Ok(JsonSlot::Ref(pack.next::<JsonRef>()?)),
                2 => {
                    let bytes: Vec<u8> = pack.next()?;
                    String::from_utf8(bytes).map_or_else(
                        |_| Err(cx.message(format_args!("inline json payload is not UTF-8"))),
                        |json| Ok(JsonSlot::Inline(json.into_boxed_str())),
                    )
                }
                other => Err(cx.message(format_args!("unknown json slot tag {other}"))),
            }
        })
    }
}

/// Encode-only musli codec for borrowed [`JsonSlotRef`] fields.
pub(crate) mod json_slot_storage_ref {
    use musli::en::SequenceEncoder;

    use super::JsonSlotRef;

    pub(crate) fn encode<E>(value: &JsonSlotRef<'_>, encoder: E) -> Result<(), E::Error>
    where
        E: musli::Encoder,
    {
        encoder.encode_pack_fn(|pack| match value {
            JsonSlotRef::None => pack.push(0u8),
            JsonSlotRef::Ref(json_ref) => {
                pack.push(1u8)?;
                pack.push(*json_ref)
            }
            JsonSlotRef::Inline(json) => {
                pack.push(2u8)?;
                pack.push(json.as_bytes())
            }
        })
    }
}
