use crate::json_store::compression::{compress_json_payload, decode_json_zstd_payload};
use crate::json_store::encoded::{EncodedJson, JsonCodec};
use crate::json_store::types::{JsonReadScopeRef, JsonRef};
use crate::storage::{PointReadPlan, StorageRead, StorageSpace};
use crate::storage::{StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpaceId};
use crate::LixError;
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::HashMap;

pub(crate) const JSON_NAMESPACE: &str = "json_store.json";
pub(crate) const JSON_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0002_0001), JSON_NAMESPACE);
const STORED_JSON_MAGIC: &[u8] = b"lix-json:v1";
const STORED_JSON_HEADER_LEN: usize = STORED_JSON_MAGIC.len() + 1 + 8;
const ZSTD_MIN_JSON_BYTES: usize = 16 * 1024;
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;

struct StoredJsonPayload<'a> {
    codec: JsonCodec,
    uncompressed_len: usize,
    data: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonHashCheck {
    /// Hot reads trust the local storage layer and pack directory. Content
    /// hashes are computed at write time; exhaustive verification belongs in
    /// explicit integrity-check/fsck callers rather than every row scan.
    TrustedHotRead,
    Verify,
}

fn raw_json_ref_for_content(json: &str) -> JsonRef {
    JsonRef::from_hash(blake3::hash(json.as_bytes()))
}

#[cfg(test)]
fn encode_json(json: &str) -> Result<EncodedJson<'_>, LixError> {
    encode_json_for_storage(json)
}

fn encode_json_for_storage(json: &str) -> Result<EncodedJson<'_>, LixError> {
    let raw_ref = raw_json_ref_for_content(json);
    encode_json_for_storage_with_ref(json, raw_ref)
}

fn encode_json_for_storage_with_ref(
    json: &str,
    raw_ref: JsonRef,
) -> Result<EncodedJson<'_>, LixError> {
    let raw_data = json.as_bytes();

    if raw_data.len() >= ZSTD_MIN_JSON_BYTES {
        let compressed = compress_json_payload(raw_data)?;
        if raw_data.len().saturating_sub(compressed.len()) >= MIN_ZSTD_SAVINGS_BYTES {
            return Ok(EncodedJson {
                json_ref: raw_ref,
                codec: JsonCodec::Zstd,
                uncompressed_len: json.len(),
                data: Cow::Owned(compressed),
            });
        }
    }

    Ok(EncodedJson {
        json_ref: raw_ref,
        codec: JsonCodec::Raw,
        uncompressed_len: json.len(),
        data: Cow::Borrowed(raw_data),
    })
}

pub(crate) fn encode_json_str(json: &str) -> Result<EncodedJson<'_>, LixError> {
    encode_json_for_storage(json)
}

pub(crate) fn encode_json_str_with_ref(
    json: &str,
    json_ref: JsonRef,
) -> Result<EncodedJson<'_>, LixError> {
    debug_assert_eq!(JsonRef::for_content(json.as_bytes()), json_ref);
    encode_json_for_storage_with_ref(json, json_ref)
}

pub(crate) fn encode_direct_json_payload(encoded_json: &EncodedJson<'_>) -> Vec<u8> {
    encode_stored_json_payload(encoded_json)
}

pub(crate) fn encode_json_str_for_storage_with_ref(
    json: &str,
    json_ref: JsonRef,
) -> Result<(JsonRef, Vec<u8>), LixError> {
    let encoded_json = encode_json_for_storage_with_ref(json, json_ref)?;
    let json_ref = encoded_json.json_ref.clone();
    Ok((json_ref, encode_stored_json_payload(&encoded_json)))
}

async fn load_json_bytes_direct(
    store: &(impl StorageRead + ?Sized),
    json_ref: &JsonRef,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = load_values(store, JSON_SPACE, vec![json_ref.as_hash_bytes().to_vec()])?
        .into_iter()
        .next()
        .flatten();
    let Some(bytes) = result else {
        return Ok(None);
    };
    let stored_payload = decode_stored_json_payload(&bytes)?;
    let _ = store;
    decode_json_payload(json_ref, stored_payload, JsonHashCheck::TrustedHotRead).map(Some)
}

pub(crate) async fn load_json_bytes_many_in_scope(
    store: &(impl StorageRead + ?Sized),
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    load_json_bytes_many_in_scope_with_hash_check(
        store,
        json_refs,
        scope,
        JsonHashCheck::TrustedHotRead,
    )
    .await
}

pub(crate) async fn verify_json_bytes_many_in_scope(
    store: &impl StorageRead,
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    load_json_bytes_many_in_scope_with_hash_check(store, json_refs, scope, JsonHashCheck::Verify)
        .await
}

async fn load_json_bytes_many_in_scope_with_hash_check(
    store: &(impl StorageRead + ?Sized),
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
    hash_check: JsonHashCheck,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if json_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_keys = Vec::new();
    let mut unique_refs = Vec::new();
    let mut key_indexes = HashMap::<[u8; 32], usize>::new();
    let mut requested_indexes = Vec::with_capacity(json_refs.len());
    let mut has_duplicate_refs = false;
    for json_ref in json_refs {
        let hash = *json_ref.as_hash_array();
        let index = match key_indexes.get(&hash) {
            Some(index) => {
                has_duplicate_refs = true;
                *index
            }
            None => {
                let index = unique_keys.len();
                key_indexes.insert(hash, index);
                unique_keys.push(hash.to_vec());
                unique_refs.push(*json_ref);
                index
            }
        };
        requested_indexes.push(index);
    }

    let JsonReadScopeRef::OutOfBand = scope;
    let mut unique_values = vec![None; unique_refs.len()];

    let missing = unique_values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.is_none().then_some(index))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(json_values_in_request_order(
            unique_values,
            requested_indexes,
            has_duplicate_refs,
        ));
    }

    let loaded = load_values(
        store,
        JSON_SPACE,
        missing
            .iter()
            .map(|&index| unique_keys[index].clone())
            .collect(),
    )?;
    if loaded.len() != missing.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "json_store batch load returned {} values for {} requested refs",
                loaded.len(),
                missing.len()
            ),
        ));
    }

    for (index, stored_bytes) in loaded.into_iter().enumerate() {
        let unique_index = missing[index];
        let Some(stored_bytes) = stored_bytes else {
            continue;
        };
        let stored_payload = decode_stored_json_payload(&stored_bytes)?;
        let _ = store;
        unique_values[unique_index] = Some(decode_json_payload(
            &unique_refs[unique_index],
            stored_payload,
            hash_check,
        )?);
    }

    Ok(json_values_in_request_order(
        unique_values,
        requested_indexes,
        has_duplicate_refs,
    ))
}

fn json_values_in_request_order(
    unique_values: Vec<Option<Vec<u8>>>,
    requested_indexes: Vec<usize>,
    has_duplicate_refs: bool,
) -> Vec<Option<Vec<u8>>> {
    if !has_duplicate_refs {
        debug_assert_eq!(requested_indexes.len(), unique_values.len());
        debug_assert!(requested_indexes
            .iter()
            .copied()
            .enumerate()
            .all(|(request_index, unique_index)| request_index == unique_index));
        return unique_values;
    }
    requested_indexes
        .into_iter()
        .map(|index| unique_values[index].clone())
        .collect()
}

fn load_values(
    store: &(impl StorageRead + ?Sized),
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let keys = keys
        .into_iter()
        .map(|key| StorageKey(Bytes::from(key)))
        .collect::<Vec<_>>();
    let result =
        PointReadPlan::new(space, &keys).materialize(store, StorageGetOptions::default())?;
    Ok(result
        .value
        .into_iter()
        .map(|value| match value {
            Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes.to_vec()),
            Some(StorageProjectedValue::KeyOnly) | None => None,
        })
        .collect())
}

fn encode_stored_json_payload(encoded_json: &EncodedJson<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(STORED_JSON_HEADER_LEN + encoded_json.data.as_ref().len());
    out.extend_from_slice(STORED_JSON_MAGIC);
    out.push(json_codec_byte(encoded_json.codec));
    out.extend_from_slice(&(encoded_json.uncompressed_len as u64).to_be_bytes());
    out.extend_from_slice(encoded_json.data.as_ref());
    out
}

fn decode_stored_json_payload(bytes: &[u8]) -> Result<StoredJsonPayload<'_>, LixError> {
    if bytes.len() < STORED_JSON_HEADER_LEN {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON payload is truncated",
        ));
    }
    if &bytes[..STORED_JSON_MAGIC.len()] != STORED_JSON_MAGIC {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON payload has invalid header",
        ));
    }
    let codec = read_json_codec(bytes[STORED_JSON_MAGIC.len()])?;
    let len_start = STORED_JSON_MAGIC.len() + 1;
    let len_end = len_start + 8;
    let uncompressed_len = u64::from_be_bytes(
        bytes[len_start..len_end]
            .try_into()
            .expect("stored JSON length header is fixed size"),
    ) as usize;
    Ok(StoredJsonPayload {
        codec,
        uncompressed_len,
        data: &bytes[len_end..],
    })
}

fn json_codec_byte(codec: JsonCodec) -> u8 {
    match codec {
        JsonCodec::Raw => 0,
        JsonCodec::Zstd => 1,
    }
}

fn read_json_codec(byte: u8) -> Result<JsonCodec, LixError> {
    match byte {
        0 => Ok(JsonCodec::Raw),
        1 => Ok(JsonCodec::Zstd),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("stored JSON payload has unknown codec byte {byte}"),
        )),
    }
}

fn decode_json_payload(
    json_ref: &JsonRef,
    stored_payload: StoredJsonPayload<'_>,
    hash_check: JsonHashCheck,
) -> Result<Vec<u8>, LixError> {
    let data = match stored_payload.codec {
        JsonCodec::Raw => Ok(stored_payload.data.to_vec()),
        JsonCodec::Zstd => decode_json_zstd_payload(
            stored_payload.data,
            stored_payload.uncompressed_len,
            &json_ref.to_hex(),
        ),
    }?;
    if data.len() != stored_payload.uncompressed_len {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "json ref '{}' decoded to {} bytes, expected {}",
                json_ref.to_hex(),
                data.len(),
                stored_payload.uncompressed_len
            ),
        ));
    }
    if hash_check == JsonHashCheck::Verify {
        let actual_hash = blake3::hash(&data);
        if actual_hash.as_bytes() != json_ref.as_hash_bytes() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("json ref '{}' hash mismatch", json_ref.to_hex()),
            ));
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageContext;
    use crate::storage::{
        InMemoryStorageBackend, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
    };

    #[tokio::test]
    async fn json_roundtrips_raw_payload() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let json = "{\"value\":\"small\"}";
        let encoded = encode_json(json).expect("json should encode");
        assert_eq!(encoded.codec, JsonCodec::Raw);

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(encoded.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&encoded)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert_eq!(
            load_json_bytes_direct(&store, &encoded.json_ref)
                .await
                .expect("json should load"),
            Some(json.as_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn json_batch_load_roundtrips_in_request_order() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let first = encode_json("{\"value\":\"first\"}").expect("first json should encode");
        let second = encode_json("{\"value\":\"second\"}").expect("second json should encode");

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(first.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&first)),
            },
        );
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(second.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&second)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let values = load_json_bytes_many_in_scope(
            &store,
            &[second.json_ref, first.json_ref, second.json_ref],
            JsonReadScopeRef::OutOfBand,
        )
        .await
        .expect("json batch should load");

        assert_eq!(
            values,
            vec![
                Some(second.data.as_ref().to_vec()),
                Some(first.data.as_ref().to_vec()),
                Some(second.data.as_ref().to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn verified_batch_load_rejects_hash_mismatch() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let requested_ref = JsonRef::for_content(br#"{"value":"requested"}"#);
        let stored = encode_json("{\"value\":\"different\"}").expect("stored json should encode");

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(requested_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&stored)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let trusted =
            load_json_bytes_many_in_scope(&store, &[requested_ref], JsonReadScopeRef::OutOfBand)
                .await
                .expect("trusted hot read should not hash-check");
        assert_eq!(trusted, vec![Some(stored.data.as_ref().to_vec())]);

        let error =
            verify_json_bytes_many_in_scope(&store, &[requested_ref], JsonReadScopeRef::OutOfBand)
                .await
                .expect_err("verified read should reject mismatched content address");
        assert!(
            error.to_string().contains("hash mismatch"),
            "error should mention hash mismatch: {error}"
        );
    }
}
