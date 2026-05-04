use crate::json_store::compression::{compress_json_payload, decode_json_zstd_payload};
use crate::json_store::encoded::{EncodedJson, JsonCodec};
use crate::json_store::types::JsonRef;
use crate::storage::{
    KvGetGroup, KvGetProjection, KvGetRequest, KvPut, KvWriteBatch, KvWriteGroup, StorageReader,
    StorageWriter,
};
use crate::LixError;
use std::borrow::Cow;

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

pub(crate) async fn persist_json_bytes(
    writer: &mut (impl StorageWriter + ?Sized),
    bytes: &[u8],
) -> Result<JsonRef, LixError> {
    let (json_ref, stored_payload) = encode_json_bytes_for_storage(bytes)?;
    persist_stored_json_payload(writer, &json_ref, &stored_payload).await?;
    Ok(json_ref)
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

async fn persist_encoded_json(
    writer: &mut (impl StorageWriter + ?Sized),
    encoded_json: &EncodedJson<'_>,
) -> Result<(), LixError> {
    let stored_payload = encode_stored_json_payload(encoded_json);
    persist_stored_json_payload(writer, &encoded_json.json_ref, &stored_payload).await
}

pub(crate) async fn persist_stored_json_payload(
    writer: &mut (impl StorageWriter + ?Sized),
    json_ref: &JsonRef,
    stored_payload: &[u8],
) -> Result<(), LixError> {
    writer
        .write_kv_batch(KvWriteBatch {
            groups: vec![KvWriteGroup {
                namespace: JSON_NAMESPACE.to_string(),
                puts: vec![KvPut {
                    key: json_ref.as_hash_bytes().to_vec(),
                    value: stored_payload.to_vec(),
                }],
                deletes: Vec::new(),
            }],
        })
        .await?;
    Ok(())
}

pub(crate) async fn load_json_bytes(
    store: &mut impl StorageReader,
    json_ref: &JsonRef,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = store
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: JSON_NAMESPACE.to_string(),
                keys: vec![json_ref.as_hash_bytes().to_vec()],
            }],
            projection: KvGetProjection::Values,
        })
        .await?
        .groups
        .into_iter()
        .next()
        .map(|mut group| group.pop_value())
        .transpose()?
        .flatten();
    let Some(bytes) = result else {
        return Ok(None);
    };
    let stored_payload = decode_stored_json_payload(&bytes)?;
    decode_json_payload(store, json_ref, stored_payload)
        .await
        .map(Some)
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
    use crate::storage::StorageContext;

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
        persist_encoded_json(&mut transaction.as_mut(), &encoded)
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
}
