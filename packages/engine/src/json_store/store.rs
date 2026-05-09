use crate::json_store::compression::{compress_json_payload, decode_json_zstd_payload};
use crate::json_store::encoded::{EncodedJson, JsonCodec};
use crate::json_store::types::JsonRef;
use crate::storage::{KvGetGroup, KvGetRequest, StorageReader};
use crate::LixError;
use std::borrow::Cow;
use std::collections::BTreeMap;

pub(crate) const JSON_NAMESPACE: &str = "json_store.json";
const STORED_JSON_MAGIC: &[u8] = b"lix-json:v1";
const STORED_JSON_HEADER_LEN: usize = STORED_JSON_MAGIC.len() + 1 + 8;
const ZSTD_MIN_JSON_BYTES: usize = 16 * 1024;
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;

struct StoredJsonPayload<'a> {
    codec: JsonCodec,
    uncompressed_len: usize,
    data: &'a [u8],
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

pub(crate) fn encode_json_bytes_for_storage(bytes: &[u8]) -> Result<(JsonRef, Vec<u8>), LixError> {
    let json = std::str::from_utf8(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("json bytes are invalid UTF-8: {error}"),
        )
    })?;
    let json_ref = JsonRef::from_hash(blake3::hash(bytes));
    encode_json_str_for_storage_with_ref(json, json_ref)
}

pub(crate) fn encode_json_str_for_storage_with_ref(
    json: &str,
    json_ref: JsonRef,
) -> Result<(JsonRef, Vec<u8>), LixError> {
    let encoded_json = encode_json_for_storage_with_ref(json, json_ref)?;
    let json_ref = encoded_json.json_ref.clone();
    Ok((json_ref, encode_stored_json_payload(&encoded_json)))
}

pub(crate) async fn load_json_bytes(
    store: &mut impl StorageReader,
    json_ref: &JsonRef,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: JSON_NAMESPACE.to_string(),
                keys: vec![json_ref.as_hash_bytes().to_vec()],
            }],
        })
        .await?
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.single_value_owned());
    let Some(bytes) = result else {
        return Ok(None);
    };
    let stored_payload = decode_stored_json_payload(&bytes)?;
    decode_json_payload(store, json_ref, stored_payload)
        .await
        .map(Some)
}

pub(crate) async fn load_json_bytes_many(
    store: &mut impl StorageReader,
    json_refs: &[JsonRef],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if json_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_keys = Vec::new();
    let mut unique_refs = Vec::new();
    let mut key_indexes = BTreeMap::<Vec<u8>, usize>::new();
    let mut requested_indexes = Vec::with_capacity(json_refs.len());
    for json_ref in json_refs {
        let key = json_ref.as_hash_bytes().to_vec();
        let index = match key_indexes.get(&key) {
            Some(index) => *index,
            None => {
                let index = unique_keys.len();
                key_indexes.insert(key.clone(), index);
                unique_keys.push(key);
                unique_refs.push(*json_ref);
                index
            }
        };
        requested_indexes.push(index);
    }

    let result = store
        .get_values(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: JSON_NAMESPACE.to_string(),
                keys: unique_keys,
            }],
        })
        .await?;
    let group = result.groups.into_iter().next().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "json_store batch load returned no result group",
        )
    })?;
    if group.len() != unique_refs.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "json_store batch load returned {} values for {} requested refs",
                group.len(),
                unique_refs.len()
            ),
        ));
    }

    let mut unique_values = Vec::with_capacity(unique_refs.len());
    for (index, stored_bytes) in group.values_iter().enumerate() {
        let Some(stored_bytes) = stored_bytes else {
            unique_values.push(None);
            continue;
        };
        let stored_payload = decode_stored_json_payload(stored_bytes)?;
        unique_values.push(Some(
            decode_json_payload(store, &unique_refs[index], stored_payload).await?,
        ));
    }

    Ok(requested_indexes
        .into_iter()
        .map(|index| unique_values[index].clone())
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

async fn decode_json_payload(
    _store: &mut impl StorageReader,
    json_ref: &JsonRef,
    stored_payload: StoredJsonPayload<'_>,
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
    let actual_hash = blake3::hash(&data);
    if actual_hash.as_bytes() != json_ref.as_hash_bytes() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("json ref '{}' hash mismatch", json_ref.to_hex()),
        ));
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::testing::UnitTestBackend;
    use crate::storage::{StorageContext, StorageWriteSet};

    #[tokio::test]
    async fn json_roundtrips_raw_payload() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let json = "{\"value\":\"small\"}";
        let encoded = encode_json(json).expect("json should encode");
        assert_eq!(encoded.codec, JsonCodec::Raw);

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        writes.put(
            JSON_NAMESPACE,
            encoded.json_ref.as_hash_bytes().to_vec(),
            encode_stored_json_payload(&encoded),
        );
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("json should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        assert_eq!(
            load_json_bytes(&mut store, &encoded.json_ref)
                .await
                .expect("json should load"),
            Some(json.as_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn json_batch_load_roundtrips_in_request_order() {
        let storage = StorageContext::new(Arc::new(UnitTestBackend::new()));
        let first = encode_json("{\"value\":\"first\"}").expect("first json should encode");
        let second = encode_json("{\"value\":\"second\"}").expect("second json should encode");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut writes = StorageWriteSet::new();
        writes.put(
            JSON_NAMESPACE,
            first.json_ref.as_hash_bytes().to_vec(),
            encode_stored_json_payload(&first),
        );
        writes.put(
            JSON_NAMESPACE,
            second.json_ref.as_hash_bytes().to_vec(),
            encode_stored_json_payload(&second),
        );
        writes
            .apply(&mut transaction.as_mut())
            .await
            .expect("json should store");
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let mut store = storage.clone();
        let values = load_json_bytes_many(
            &mut store,
            &[second.json_ref, first.json_ref, second.json_ref],
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
}
